//! Sort executor implementation.
//!
//! This executor sorts tuples from a child executor based on one or more
//! sort criteria. It materializes all tuples from the child executor into
//! memory before sorting, then returns them in the sorted order.
//!
//! Supports:
//! - Multi-column sorting (ORDER BY col1 ASC, col2 DESC)
//! - NULL handling (NULLs first or last)
//! - ASC/DESC ordering per column

use crate::access::{deserialize_values, DataType, Tuple, Value};
use crate::executor::{ColumnInfo, Executor};
use anyhow::{bail, Result};
use std::cmp::Ordering;

/// Sort order for a column
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// NULL ordering preference
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    First,
    Last,
}

/// Sort criteria for a single column
#[derive(Debug, Clone)]
pub struct SortCriteria {
    /// Column index to sort by
    pub column_index: usize,
    /// Sort order (ASC/DESC)
    pub order: SortOrder,
    /// NULL ordering (FIRST/LAST)
    pub null_order: NullOrder,
}

impl SortCriteria {
    /// Create new sort criteria with default NULL ordering
    /// (NULLs first for ASC, NULLs last for DESC)
    pub fn new(column_index: usize, order: SortOrder) -> Self {
        let null_order = match order {
            SortOrder::Asc => NullOrder::First,
            SortOrder::Desc => NullOrder::Last,
        };
        Self {
            column_index,
            order,
            null_order,
        }
    }

    /// Create new sort criteria with explicit NULL ordering
    pub fn with_null_order(column_index: usize, order: SortOrder, null_order: NullOrder) -> Self {
        Self {
            column_index,
            order,
            null_order,
        }
    }
}

/// Executor that sorts tuples based on multiple criteria
pub struct SortExecutor {
    /// Child executor that produces tuples
    child: Box<dyn Executor>,
    /// Sort criteria (in order of precedence)
    criteria: Vec<SortCriteria>,
    /// Output schema (same as child's schema)
    output_schema: Vec<ColumnInfo>,
    /// Schema data types for deserialization
    schema_types: Vec<DataType>,
    /// Materialized and sorted tuples
    sorted_tuples: Vec<Tuple>,
    /// Current position in sorted_tuples
    current_position: usize,
    /// Whether the executor has been initialized
    initialized: bool,
}

impl SortExecutor {
    /// Create a new sort executor
    ///
    /// # Arguments
    /// * `child` - The child executor that produces tuples
    /// * `criteria` - Sort criteria in order of precedence
    pub fn new(child: Box<dyn Executor>, criteria: Vec<SortCriteria>) -> Self {
        Self {
            child,
            criteria,
            output_schema: Vec::new(),
            schema_types: Vec::new(),
            sorted_tuples: Vec::new(),
            current_position: 0,
            initialized: false,
        }
    }

    /// Compare two values according to sort order and null handling
    fn compare_values(v1: &Value, v2: &Value, order: SortOrder, null_order: NullOrder) -> Ordering {
        // Handle NULLs first
        match (v1, v2) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => match null_order {
                NullOrder::First => Ordering::Less,
                NullOrder::Last => Ordering::Greater,
            },
            (_, Value::Null) => match null_order {
                NullOrder::First => Ordering::Greater,
                NullOrder::Last => Ordering::Less,
            },
            // Both non-NULL, compare actual values
            (v1, v2) => {
                let cmp = Self::compare_non_null_values(v1, v2);
                match order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                }
            }
        }
    }

    /// Compare two non-NULL values
    fn compare_non_null_values(v1: &Value, v2: &Value) -> Ordering {
        match (v1, v2) {
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
            (Value::Int32(i1), Value::Int32(i2)) => i1.cmp(i2),
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            // Type mismatch - this shouldn't happen in practice
            // but we need to handle it for completeness
            _ => panic!("Cannot compare values of different types"),
        }
    }

    /// Sort the tuples based on the criteria
    fn sort_tuples(&mut self) -> Result<()> {
        // Create a vector of (tuple, values) pairs for efficient sorting
        let mut tuple_values: Vec<(Tuple, Vec<Value>)> = Vec::new();

        for tuple in &self.sorted_tuples {
            let values = deserialize_values(&tuple.data, &self.schema_types)?;
            tuple_values.push((tuple.clone(), values));
        }

        // Sort using all criteria
        tuple_values.sort_by(|a, b| {
            for criteria in &self.criteria {
                // Validate column index
                if criteria.column_index >= a.1.len() {
                    panic!("Sort column index {} out of range", criteria.column_index);
                }

                let v1 = &a.1[criteria.column_index];
                let v2 = &b.1[criteria.column_index];

                let cmp = Self::compare_values(v1, v2, criteria.order, criteria.null_order);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            // All criteria equal
            Ordering::Equal
        });

        // Extract sorted tuples
        self.sorted_tuples = tuple_values.into_iter().map(|(tuple, _)| tuple).collect();

        Ok(())
    }
}

impl Executor for SortExecutor {
    fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Initialize child executor
        self.child.init()?;

        // Copy the child's output schema
        self.output_schema = self.child.output_schema().to_vec();

        // Extract data types for deserialization
        self.schema_types = self.output_schema.iter().map(|col| col.data_type).collect();

        // Validate sort criteria
        for criteria in &self.criteria {
            if criteria.column_index >= self.output_schema.len() {
                bail!(
                    "Sort column index {} is out of range (schema has {} columns)",
                    criteria.column_index,
                    self.output_schema.len()
                );
            }
        }

        // Materialize all tuples from child
        self.sorted_tuples.clear();
        while let Some(tuple) = self.child.next()? {
            self.sorted_tuples.push(tuple);
        }

        // Sort the tuples
        self.sort_tuples()?;

        // Reset position
        self.current_position = 0;

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        if self.current_position >= self.sorted_tuples.len() {
            return Ok(None);
        }

        let tuple = self.sorted_tuples[self.current_position].clone();
        self.current_position += 1;
        Ok(Some(tuple))
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{serialize_values, TupleId};
    use crate::storage::page::PageId;

    /// Mock executor for testing that produces a fixed set of tuples
    struct MockExecutor {
        tuples: Vec<(TupleId, Vec<Value>)>,
        schema: Vec<ColumnInfo>,
        schema_types: Vec<DataType>,
        current: usize,
        initialized: bool,
    }

    impl MockExecutor {
        fn new(tuples: Vec<(TupleId, Vec<Value>)>, schema: Vec<ColumnInfo>) -> Self {
            let schema_types = schema.iter().map(|col| col.data_type).collect();
            Self {
                tuples,
                schema,
                schema_types,
                current: 0,
                initialized: false,
            }
        }
    }

    impl Executor for MockExecutor {
        fn init(&mut self) -> Result<()> {
            self.initialized = true;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>> {
            if !self.initialized {
                bail!("Not initialized");
            }

            if self.current >= self.tuples.len() {
                return Ok(None);
            }

            let (tuple_id, values) = &self.tuples[self.current];
            self.current += 1;

            // Serialize values
            let data = serialize_values(values, &self.schema_types)?;
            Ok(Some(Tuple::new(*tuple_id, data)))
        }

        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }
    }

    #[test]
    fn test_sort_single_column_asc() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("age", DataType::Int32),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(3),
                    Value::String("Charlie".to_string()),
                    Value::Int32(35),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Int32(25),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Int32(30),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema.clone()));

        // Sort by id ASC
        let criteria = vec![SortCriteria::new(0, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        // Verify schema
        assert_eq!(sort.output_schema(), &schema);

        // Get sorted tuples
        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0][0], Value::Int32(1)); // Alice
        assert_eq!(results[1][0], Value::Int32(2)); // Bob
        assert_eq!(results[2][0], Value::Int32(3)); // Charlie

        Ok(())
    }

    #[test]
    fn test_sort_single_column_desc() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Int32(30)],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::Int32(20)],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by value DESC
        let criteria = vec![SortCriteria::new(1, SortOrder::Desc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0][1], Value::Int32(30)); // id=2
        assert_eq!(results[1][1], Value::Int32(20)); // id=3
        assert_eq!(results[2][1], Value::Int32(10)); // id=1

        Ok(())
    }

    #[test]
    fn test_sort_multi_column() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("category", DataType::Varchar),
            ColumnInfo::new("price", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::String("Electronics".to_string()),
                    Value::Int32(100),
                    Value::String("Mouse".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::String("Books".to_string()),
                    Value::Int32(20),
                    Value::String("Novel".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::String("Electronics".to_string()),
                    Value::Int32(200),
                    Value::String("Keyboard".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![
                    Value::String("Books".to_string()),
                    Value::Int32(15),
                    Value::String("Dictionary".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 4),
                vec![
                    Value::String("Electronics".to_string()),
                    Value::Int32(100),
                    Value::String("Cable".to_string()),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by category ASC, then price DESC, then name ASC
        let criteria = vec![
            SortCriteria::new(0, SortOrder::Asc),  // category ASC
            SortCriteria::new(1, SortOrder::Desc), // price DESC
            SortCriteria::new(2, SortOrder::Asc),  // name ASC
        ];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 5);

        // Books category first (ASC)
        assert_eq!(results[0][0], Value::String("Books".to_string()));
        assert_eq!(results[0][1], Value::Int32(20)); // Higher price first (DESC)
        assert_eq!(results[1][0], Value::String("Books".to_string()));
        assert_eq!(results[1][1], Value::Int32(15));

        // Electronics category
        assert_eq!(results[2][0], Value::String("Electronics".to_string()));
        assert_eq!(results[2][1], Value::Int32(200)); // Highest price first
        assert_eq!(results[3][0], Value::String("Electronics".to_string()));
        assert_eq!(results[3][1], Value::Int32(100));
        assert_eq!(results[3][2], Value::String("Cable".to_string())); // Cable before Mouse (ASC)
        assert_eq!(results[4][0], Value::String("Electronics".to_string()));
        assert_eq!(results[4][1], Value::Int32(100));
        assert_eq!(results[4][2], Value::String("Mouse".to_string()));

        Ok(())
    }

    #[test]
    fn test_sort_with_nulls_first() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::String("Alice".to_string())],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Null],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::String("Bob".to_string())],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![Value::Int32(4), Value::Null],
            ),
            (
                TupleId::new(PageId(1), 4),
                vec![Value::Int32(5), Value::String("Charlie".to_string())],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by name ASC with NULLs first (default for ASC)
        let criteria = vec![SortCriteria::new(1, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 5);
        // NULLs come first
        assert_eq!(results[0][1], Value::Null);
        assert_eq!(results[1][1], Value::Null);
        // Then sorted strings
        assert_eq!(results[2][1], Value::String("Alice".to_string()));
        assert_eq!(results[3][1], Value::String("Bob".to_string()));
        assert_eq!(results[4][1], Value::String("Charlie".to_string()));

        Ok(())
    }

    #[test]
    fn test_sort_with_nulls_last() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];

        let tuples = vec![
            (TupleId::new(PageId(1), 0), vec![Value::Int32(30)]),
            (TupleId::new(PageId(1), 1), vec![Value::Null]),
            (TupleId::new(PageId(1), 2), vec![Value::Int32(10)]),
            (TupleId::new(PageId(1), 3), vec![Value::Null]),
            (TupleId::new(PageId(1), 4), vec![Value::Int32(20)]),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by value DESC with NULLs last (default for DESC)
        let criteria = vec![SortCriteria::new(0, SortOrder::Desc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 5);
        // Sorted values in DESC order
        assert_eq!(results[0][0], Value::Int32(30));
        assert_eq!(results[1][0], Value::Int32(20));
        assert_eq!(results[2][0], Value::Int32(10));
        // NULLs come last
        assert_eq!(results[3][0], Value::Null);
        assert_eq!(results[4][0], Value::Null);

        Ok(())
    }

    #[test]
    fn test_sort_with_custom_null_order() -> Result<()> {
        let schema = vec![ColumnInfo::new("value", DataType::Int32)];

        let tuples = vec![
            (TupleId::new(PageId(1), 0), vec![Value::Int32(20)]),
            (TupleId::new(PageId(1), 1), vec![Value::Null]),
            (TupleId::new(PageId(1), 2), vec![Value::Int32(10)]),
            (TupleId::new(PageId(1), 3), vec![Value::Int32(30)]),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by value ASC with NULLs last (opposite of default)
        let criteria = vec![SortCriteria::with_null_order(
            0,
            SortOrder::Asc,
            NullOrder::Last,
        )];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 4);
        // Sorted values in ASC order
        assert_eq!(results[0][0], Value::Int32(10));
        assert_eq!(results[1][0], Value::Int32(20));
        assert_eq!(results[2][0], Value::Int32(30));
        // NULL comes last
        assert_eq!(results[3][0], Value::Null);

        Ok(())
    }

    #[test]
    fn test_sort_empty_input() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));

        let criteria = vec![SortCriteria::new(0, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        // Should return no tuples
        assert!(sort.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_sort_single_tuple() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let tuples = vec![(TupleId::new(PageId(1), 0), vec![Value::Int32(42)])];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        let criteria = vec![SortCriteria::new(0, SortOrder::Desc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        // Should return the single tuple
        let tuple = sort.next()?.expect("Should have one tuple");
        let values = deserialize_values(&tuple.data, &sort.schema_types)?;
        assert_eq!(values[0], Value::Int32(42));

        assert!(sort.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_sort_boolean_values() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("active", DataType::Boolean),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Boolean(true)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Boolean(false)],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::Boolean(true)],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![Value::Int32(4), Value::Boolean(false)],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort by active ASC (false < true)
        let criteria = vec![SortCriteria::new(1, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 4);
        // false values come first
        assert_eq!(results[0][1], Value::Boolean(false));
        assert_eq!(results[1][1], Value::Boolean(false));
        // true values come after
        assert_eq!(results[2][1], Value::Boolean(true));
        assert_eq!(results[3][1], Value::Boolean(true));

        Ok(())
    }

    #[test]
    fn test_sort_not_initialized() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));

        let criteria = vec![SortCriteria::new(0, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);

        // Try to call next() without init()
        let result = sort.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_sort_invalid_column_index() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![(
            TupleId::new(PageId(1), 0),
            vec![Value::Int32(1), Value::String("Test".to_string())],
        )];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Invalid column index
        let criteria = vec![SortCriteria::new(5, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);

        // Should fail during init
        let result = sort.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));

        Ok(())
    }

    #[test]
    fn test_sort_stable_ordering() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("category", DataType::Varchar),
            ColumnInfo::new("id", DataType::Int32),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::String("A".to_string()), Value::Int32(1)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::String("B".to_string()), Value::Int32(2)],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::String("A".to_string()), Value::Int32(3)],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![Value::String("B".to_string()), Value::Int32(4)],
            ),
            (
                TupleId::new(PageId(1), 4),
                vec![Value::String("A".to_string()), Value::Int32(5)],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Sort only by category
        let criteria = vec![SortCriteria::new(0, SortOrder::Asc)];
        let mut sort = SortExecutor::new(mock_executor, criteria);
        sort.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = sort.next()? {
            let values = deserialize_values(&tuple.data, &sort.schema_types)?;
            results.push(values);
        }

        assert_eq!(results.len(), 5);

        // All A's should come before B's
        assert_eq!(results[0][0], Value::String("A".to_string()));
        assert_eq!(results[1][0], Value::String("A".to_string()));
        assert_eq!(results[2][0], Value::String("A".to_string()));
        assert_eq!(results[3][0], Value::String("B".to_string()));
        assert_eq!(results[4][0], Value::String("B".to_string()));

        // Within each category, the original order should be preserved (stable sort)
        assert_eq!(results[0][1], Value::Int32(1)); // First A
        assert_eq!(results[1][1], Value::Int32(3)); // Second A
        assert_eq!(results[2][1], Value::Int32(5)); // Third A
        assert_eq!(results[3][1], Value::Int32(2)); // First B
        assert_eq!(results[4][1], Value::Int32(4)); // Second B

        Ok(())
    }
}
