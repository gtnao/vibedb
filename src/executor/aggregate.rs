//! Hash-based aggregation executor for GROUP BY and aggregate functions.
//!
//! This module implements a hash aggregation executor that supports:
//! - Multiple grouping columns
//! - Multiple aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - Proper NULL handling according to SQL semantics
//! - Efficient one-pass execution using hash tables

use crate::access::{DataType, Tuple, Value};
use crate::executor::{ColumnInfo, Executor};
use anyhow::{bail, Result};
use std::collections::HashMap;

/// Supported aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    /// COUNT(*) or COUNT(expr) - counts non-NULL values
    Count,
    /// SUM(expr) - sums numeric values, ignoring NULLs
    Sum,
    /// AVG(expr) - average of numeric values, ignoring NULLs
    Avg,
    /// MIN(expr) - minimum value, ignoring NULLs
    Min,
    /// MAX(expr) - maximum value, ignoring NULLs
    Max,
}

impl AggregateFunction {
    /// Returns the name of the aggregate function
    pub fn name(&self) -> &'static str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        }
    }

    /// Returns the output data type for this aggregate function given the input type
    pub fn output_type(&self, input_type: Option<DataType>) -> DataType {
        match self {
            AggregateFunction::Count => DataType::Int32,
            AggregateFunction::Sum => input_type.unwrap_or(DataType::Int32),
            AggregateFunction::Avg => DataType::Int32, // For simplicity, we'll use integer division
            AggregateFunction::Min | AggregateFunction::Max => {
                input_type.unwrap_or(DataType::Int32)
            }
        }
    }
}

/// Specification for an aggregate computation
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// The aggregate function to apply
    pub function: AggregateFunction,
    /// Column index to aggregate (None for COUNT(*))
    pub column_idx: Option<usize>,
    /// Optional alias for the result column
    pub alias: Option<String>,
}

impl AggregateSpec {
    /// Create a new aggregate specification
    pub fn new(function: AggregateFunction, column_idx: Option<usize>) -> Self {
        Self {
            function,
            column_idx,
            alias: None,
        }
    }

    /// Create an aggregate specification with an alias
    pub fn with_alias(
        function: AggregateFunction,
        column_idx: Option<usize>,
        alias: String,
    ) -> Self {
        Self {
            function,
            column_idx,
            alias: Some(alias),
        }
    }
}

/// GROUP BY clause specification
#[derive(Debug, Clone)]
pub struct GroupByClause {
    /// Column indices to group by
    pub column_indices: Vec<usize>,
}

impl GroupByClause {
    /// Create a new GROUP BY clause
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self { column_indices }
    }

    /// Check if this is an empty GROUP BY (aggregate without grouping)
    pub fn is_empty(&self) -> bool {
        self.column_indices.is_empty()
    }
}

/// State for tracking aggregate values during computation
#[derive(Debug, Clone)]
struct AggregateState {
    /// For COUNT
    count: i32,
    /// For SUM
    sum: Option<i32>,
    /// For MIN
    min: Option<Value>,
    /// For MAX
    max: Option<Value>,
    /// For AVG - sum of values
    avg_sum: Option<i32>,
    /// For AVG - count of non-NULL values
    avg_count: i32,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sum: None,
            min: None,
            max: None,
            avg_sum: None,
            avg_count: 0,
        }
    }

    /// Update the state with a new value
    fn update(&mut self, value: &Value, function: AggregateFunction) -> Result<()> {
        match function {
            AggregateFunction::Count => {
                if !matches!(value, Value::Null) {
                    self.count += 1;
                }
            }
            AggregateFunction::Sum => {
                if let Value::Int32(n) = value {
                    self.sum = Some(self.sum.unwrap_or(0) + n);
                }
            }
            AggregateFunction::Avg => {
                if let Value::Int32(n) = value {
                    self.avg_sum = Some(self.avg_sum.unwrap_or(0) + n);
                    self.avg_count += 1;
                }
            }
            AggregateFunction::Min => match (&self.min, value) {
                (None, Value::Null) => {}
                (None, v) => self.min = Some(v.clone()),
                (Some(_current), Value::Null) => {}
                (Some(Value::Int32(current)), Value::Int32(new)) => {
                    if new < current {
                        self.min = Some(Value::Int32(*new));
                    }
                }
                (Some(Value::String(current)), Value::String(new)) => {
                    if new < current {
                        self.min = Some(Value::String(new.clone()));
                    }
                }
                (Some(Value::Boolean(current)), Value::Boolean(new)) => {
                    if !new && *current {
                        // false < true
                        self.min = Some(Value::Boolean(*new));
                    }
                }
                _ => {}
            },
            AggregateFunction::Max => match (&self.max, value) {
                (None, Value::Null) => {}
                (None, v) => self.max = Some(v.clone()),
                (Some(_current), Value::Null) => {}
                (Some(Value::Int32(current)), Value::Int32(new)) => {
                    if new > current {
                        self.max = Some(Value::Int32(*new));
                    }
                }
                (Some(Value::String(current)), Value::String(new)) => {
                    if new > current {
                        self.max = Some(Value::String(new.clone()));
                    }
                }
                (Some(Value::Boolean(current)), Value::Boolean(new)) => {
                    if *new && !current {
                        // true > false
                        self.max = Some(Value::Boolean(*new));
                    }
                }
                _ => {}
            },
        }
        Ok(())
    }

    /// Get the final aggregate value
    fn finalize(&self, function: AggregateFunction) -> Value {
        match function {
            AggregateFunction::Count => Value::Int32(self.count),
            AggregateFunction::Sum => self.sum.map(Value::Int32).unwrap_or(Value::Null),
            AggregateFunction::Avg => {
                if self.avg_count > 0 {
                    Value::Int32(self.avg_sum.unwrap() / self.avg_count)
                } else {
                    Value::Null
                }
            }
            AggregateFunction::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateFunction::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

/// Hash-based aggregation executor
pub struct HashAggregateExecutor {
    /// Child executor providing input tuples
    child: Box<dyn Executor>,
    /// GROUP BY specification
    group_by: GroupByClause,
    /// Aggregate functions to compute
    aggregates: Vec<AggregateSpec>,
    /// Output schema
    output_schema: Vec<ColumnInfo>,
    /// Hash table for grouping: group key -> aggregate states
    groups: HashMap<Vec<Value>, Vec<AggregateState>>,
    /// Iterator over groups for outputting results
    group_iter: Option<std::vec::IntoIter<(Vec<Value>, Vec<AggregateState>)>>,
    /// Flag indicating if we've consumed all input
    consumed_input: bool,
}

impl HashAggregateExecutor {
    /// Create a new hash aggregate executor
    pub fn new(
        child: Box<dyn Executor>,
        group_by: GroupByClause,
        aggregates: Vec<AggregateSpec>,
    ) -> Result<Self> {
        if aggregates.is_empty() {
            bail!("At least one aggregate function must be specified");
        }

        // Build output schema
        let child_schema = child.output_schema();
        let mut output_schema = Vec::new();

        // Add GROUP BY columns to output
        for &idx in &group_by.column_indices {
            if idx >= child_schema.len() {
                bail!("GROUP BY column index {} is out of bounds", idx);
            }
            output_schema.push(child_schema[idx].clone());
        }

        // Add aggregate columns to output
        for agg in &aggregates {
            let input_type = if let Some(idx) = agg.column_idx {
                if idx >= child_schema.len() {
                    bail!("Aggregate column index {} is out of bounds", idx);
                }
                Some(child_schema[idx].data_type)
            } else {
                None // COUNT(*)
            };

            let name = if let Some(alias) = &agg.alias {
                alias.clone()
            } else if let Some(idx) = agg.column_idx {
                format!("{}({})", agg.function.name(), child_schema[idx].name)
            } else {
                format!("{}(*)", agg.function.name())
            };

            output_schema.push(ColumnInfo::new(name, agg.function.output_type(input_type)));
        }

        Ok(Self {
            child,
            group_by,
            aggregates,
            output_schema,
            groups: HashMap::new(),
            group_iter: None,
            consumed_input: false,
        })
    }

    /// Extract group key from a tuple
    fn extract_group_key(&self, values: &[Value]) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.group_by.column_indices.len());
        for &idx in &self.group_by.column_indices {
            if idx >= values.len() {
                bail!("Column index {} is out of bounds", idx);
            }
            key.push(values[idx].clone());
        }
        Ok(key)
    }

    /// Process all input tuples and build groups
    fn consume_input(&mut self) -> Result<()> {
        while let Some(tuple) = self.child.next()? {
            let child_schema = self.child.output_schema();
            let values = crate::access::deserialize_values(
                &tuple.data,
                &child_schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
            )?;

            // Extract group key
            let group_key = if self.group_by.is_empty() {
                vec![] // Single group for aggregate without GROUP BY
            } else {
                self.extract_group_key(&values)?
            };

            // Get or create aggregate states for this group
            let states = self
                .groups
                .entry(group_key)
                .or_insert_with(|| vec![AggregateState::new(); self.aggregates.len()]);

            // Update each aggregate
            for (i, agg) in self.aggregates.iter().enumerate() {
                let value = if let Some(idx) = agg.column_idx {
                    &values[idx]
                } else {
                    // COUNT(*) - use a non-NULL value
                    &Value::Int32(1)
                };
                states[i].update(value, agg.function)?;
            }
        }

        // For empty input with no GROUP BY, we still need to return one row
        if self.groups.is_empty() && self.group_by.is_empty() {
            self.groups
                .insert(vec![], vec![AggregateState::new(); self.aggregates.len()]);
        }

        // Convert HashMap to Vec for iteration
        let groups: Vec<_> = self.groups.drain().collect();
        self.group_iter = Some(groups.into_iter());
        self.consumed_input = true;

        Ok(())
    }
}

impl Executor for HashAggregateExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        // First consume all input if we haven't already
        if !self.consumed_input {
            self.consume_input()?;
        }

        // Return next group
        if let Some(ref mut iter) = self.group_iter {
            if let Some((group_key, states)) = iter.next() {
                // Build output values
                let mut values = Vec::new();

                // Add group key values
                values.extend(group_key);

                // Add aggregate results
                for (i, agg) in self.aggregates.iter().enumerate() {
                    values.push(states[i].finalize(agg.function));
                }

                // Serialize and return
                let schema: Vec<DataType> =
                    self.output_schema.iter().map(|c| c.data_type).collect();
                let data = crate::access::serialize_values(&values, &schema)?;

                // Create a dummy TupleId for the aggregate result
                let tuple_id = crate::access::TupleId::new(crate::storage::page::PageId(0), 0);

                Ok(Some(Tuple::new(tuple_id, data)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ColumnInfo;
    use crate::storage::page::PageId;

    /// Mock executor for testing
    struct MockExecutor {
        tuples: Vec<Tuple>,
        schema: Vec<ColumnInfo>,
        pos: usize,
    }

    impl MockExecutor {
        fn new(schema: Vec<ColumnInfo>, values_list: Vec<Vec<Value>>) -> Result<Self> {
            let mut tuples = Vec::new();
            let schema_types: Vec<DataType> = schema.iter().map(|c| c.data_type).collect();

            for values in values_list {
                let data = crate::access::serialize_values(&values, &schema_types)?;
                let tuple_id = crate::access::TupleId::new(PageId(0), tuples.len() as u16);
                tuples.push(Tuple::new(tuple_id, data));
            }

            Ok(Self {
                tuples,
                schema,
                pos: 0,
            })
        }
    }

    impl Executor for MockExecutor {
        fn init(&mut self) -> Result<()> {
            self.pos = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>> {
            if self.pos < self.tuples.len() {
                let tuple = self.tuples[self.pos].clone();
                self.pos += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }
    }

    #[test]
    fn test_aggregate_function_properties() {
        assert_eq!(AggregateFunction::Count.name(), "COUNT");
        assert_eq!(AggregateFunction::Sum.name(), "SUM");
        assert_eq!(AggregateFunction::Count.output_type(None), DataType::Int32);
        assert_eq!(
            AggregateFunction::Sum.output_type(Some(DataType::Int32)),
            DataType::Int32
        );
    }

    #[test]
    fn test_count_star_no_grouping() -> Result<()> {
        // Create test data
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];
        let values = vec![
            vec![Value::Int32(1), Value::String("Alice".to_string())],
            vec![Value::Int32(2), Value::String("Bob".to_string())],
            vec![Value::Int32(3), Value::Null],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        // COUNT(*) without GROUP BY
        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![AggregateSpec::new(AggregateFunction::Count, None)],
        )?;

        executor.init()?;

        // Should return one row with count = 3
        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![DataType::Int32];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(values, vec![Value::Int32(3)]);

        // No more results
        assert!(executor.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_sum_with_nulls() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let values = vec![
            vec![Value::Int32(10)],
            vec![Value::Null],
            vec![Value::Int32(20)],
            vec![Value::Int32(30)],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![AggregateSpec::new(AggregateFunction::Sum, Some(0))],
        )?;

        executor.init()?;

        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![DataType::Int32];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(values, vec![Value::Int32(60)]); // 10 + 20 + 30, NULL ignored

        Ok(())
    }

    #[test]
    fn test_group_by_single_column() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("dept", DataType::Varchar),
            ColumnInfo::new("salary", DataType::Int32),
        ];
        let values = vec![
            vec![Value::String("Sales".to_string()), Value::Int32(1000)],
            vec![Value::String("IT".to_string()), Value::Int32(2000)],
            vec![Value::String("Sales".to_string()), Value::Int32(1500)],
            vec![Value::String("IT".to_string()), Value::Int32(2500)],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![0]), // GROUP BY dept
            vec![
                AggregateSpec::new(AggregateFunction::Count, None),
                AggregateSpec::new(AggregateFunction::Sum, Some(1)),
            ],
        )?;

        executor.init()?;

        // Collect all results
        let mut results = Vec::new();
        while let Some(tuple) = executor.next()? {
            let schema_types = vec![DataType::Varchar, DataType::Int32, DataType::Int32];
            let values = crate::access::deserialize_values(&tuple.data, &schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 2);

        // Check that we have both groups (order may vary)
        let sales_group = results
            .iter()
            .find(|v| v[0] == Value::String("Sales".to_string()))
            .expect("Should have Sales group");
        assert_eq!(sales_group[1], Value::Int32(2)); // COUNT
        assert_eq!(sales_group[2], Value::Int32(2500)); // SUM

        let it_group = results
            .iter()
            .find(|v| v[0] == Value::String("IT".to_string()))
            .expect("Should have IT group");
        assert_eq!(it_group[1], Value::Int32(2)); // COUNT
        assert_eq!(it_group[2], Value::Int32(4500)); // SUM

        Ok(())
    }

    #[test]
    fn test_min_max_aggregates() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("category", DataType::Varchar),
            ColumnInfo::new("value", DataType::Int32),
        ];
        let values = vec![
            vec![Value::String("A".to_string()), Value::Int32(10)],
            vec![Value::String("A".to_string()), Value::Int32(5)],
            vec![Value::String("A".to_string()), Value::Int32(15)],
            vec![Value::String("B".to_string()), Value::Int32(20)],
            vec![Value::String("B".to_string()), Value::Null],
            vec![Value::String("B".to_string()), Value::Int32(25)],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![0]), // GROUP BY category
            vec![
                AggregateSpec::new(AggregateFunction::Min, Some(1)),
                AggregateSpec::new(AggregateFunction::Max, Some(1)),
            ],
        )?;

        executor.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = executor.next()? {
            let schema_types = vec![DataType::Varchar, DataType::Int32, DataType::Int32];
            let values = crate::access::deserialize_values(&tuple.data, &schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 2);

        let a_group = results
            .iter()
            .find(|v| v[0] == Value::String("A".to_string()))
            .unwrap();
        assert_eq!(a_group[1], Value::Int32(5)); // MIN
        assert_eq!(a_group[2], Value::Int32(15)); // MAX

        let b_group = results
            .iter()
            .find(|v| v[0] == Value::String("B".to_string()))
            .unwrap();
        assert_eq!(b_group[1], Value::Int32(20)); // MIN (NULL ignored)
        assert_eq!(b_group[2], Value::Int32(25)); // MAX

        Ok(())
    }

    #[test]
    fn test_avg_aggregate() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let values = vec![
            vec![Value::Int32(10)],
            vec![Value::Int32(20)],
            vec![Value::Int32(30)],
            vec![Value::Null],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![AggregateSpec::new(AggregateFunction::Avg, Some(0))],
        )?;

        executor.init()?;

        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![DataType::Int32];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(values, vec![Value::Int32(20)]); // (10 + 20 + 30) / 3

        Ok(())
    }

    #[test]
    fn test_multiple_aggregates_one_pass() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let values = vec![
            vec![Value::Int32(10)],
            vec![Value::Int32(20)],
            vec![Value::Int32(30)],
        ];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![
                AggregateSpec::new(AggregateFunction::Count, None),
                AggregateSpec::new(AggregateFunction::Sum, Some(0)),
                AggregateSpec::new(AggregateFunction::Avg, Some(0)),
                AggregateSpec::new(AggregateFunction::Min, Some(0)),
                AggregateSpec::new(AggregateFunction::Max, Some(0)),
            ],
        )?;

        executor.init()?;

        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(
            values,
            vec![
                Value::Int32(3),  // COUNT
                Value::Int32(60), // SUM
                Value::Int32(20), // AVG
                Value::Int32(10), // MIN
                Value::Int32(30), // MAX
            ]
        );

        Ok(())
    }

    #[test]
    fn test_empty_input() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let child = Box::new(MockExecutor::new(schema, vec![])?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![
                AggregateSpec::new(AggregateFunction::Count, None),
                AggregateSpec::new(AggregateFunction::Sum, Some(0)),
            ],
        )?;

        executor.init()?;

        // For empty input with no GROUP BY, should return one row with zero count and NULL sum
        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![DataType::Int32, DataType::Int32];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(values, vec![Value::Int32(0), Value::Null]);

        Ok(())
    }

    #[test]
    fn test_all_null_column() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let values = vec![vec![Value::Null], vec![Value::Null], vec![Value::Null]];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let mut executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![
                AggregateSpec::new(AggregateFunction::Count, Some(0)),
                AggregateSpec::new(AggregateFunction::Sum, Some(0)),
                AggregateSpec::new(AggregateFunction::Avg, Some(0)),
                AggregateSpec::new(AggregateFunction::Min, Some(0)),
                AggregateSpec::new(AggregateFunction::Max, Some(0)),
            ],
        )?;

        executor.init()?;

        let result = executor.next()?.expect("Should have result");
        let schema_types = vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ];
        let values = crate::access::deserialize_values(&result.data, &schema_types)?;
        assert_eq!(
            values,
            vec![
                Value::Int32(0), // COUNT of non-NULL values
                Value::Null,     // SUM
                Value::Null,     // AVG
                Value::Null,     // MIN
                Value::Null,     // MAX
            ]
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_with_alias() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];
        let values = vec![vec![Value::Int32(10)]];

        let child = Box::new(MockExecutor::new(schema, values)?);

        let executor = HashAggregateExecutor::new(
            child,
            GroupByClause::new(vec![]),
            vec![AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(0),
                "total".to_string(),
            )],
        )?;

        // Check output schema has the alias
        assert_eq!(executor.output_schema()[0].name, "total");
        assert_eq!(executor.output_schema()[0].data_type, DataType::Int32);

        Ok(())
    }
}
