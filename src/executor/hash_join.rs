//! Hash Join executor implementation.
//!
//! This executor performs an equi-join between two relations using the hash join algorithm.
//! It's more efficient than nested loop join for large datasets, especially when one
//! relation is significantly smaller than the other.
//!
//! Algorithm:
//! 1. Build phase: Create hash table from right relation using join keys
//! 2. Probe phase: For each tuple in left relation, probe the hash table for matches

use crate::access::{deserialize_values, serialize_values, DataType, Tuple, Value};
use crate::executor::{ColumnInfo, Executor};
use crate::expression::{evaluate_expression, Expression, TypeChecker};
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Wrapper type for hash keys that implements Hash and Eq
#[derive(Clone, Debug)]
struct HashKey(Vec<Value>);

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.0 {
            match value {
                Value::Null => {
                    0u8.hash(state);
                }
                Value::Boolean(b) => {
                    1u8.hash(state);
                    b.hash(state);
                }
                Value::Int32(i) => {
                    2u8.hash(state);
                    i.hash(state);
                }
                Value::String(s) => {
                    3u8.hash(state);
                    s.hash(state);
                }
            }
        }
    }
}

impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for HashKey {}

/// Hash join executor that performs equi-joins efficiently
pub struct HashJoinExecutor {
    /// Left child executor
    left_child: Box<dyn Executor>,
    /// Right child executor  
    right_child: Box<dyn Executor>,
    /// Join key expressions for left side
    left_keys: Vec<Expression>,
    /// Join key expressions for right side
    right_keys: Vec<Expression>,
    /// Optional additional join condition (evaluated after hash match)
    additional_condition: Option<Expression>,
    /// Output schema (left schema + right schema)
    output_schema: Vec<ColumnInfo>,
    /// Left schema data types
    left_schema_types: Vec<DataType>,
    /// Right schema data types
    right_schema_types: Vec<DataType>,
    /// Combined schema data types
    combined_schema_types: Vec<DataType>,
    /// Hash table mapping join key values to right tuples
    hash_table: HashMap<HashKey, Vec<(Tuple, Vec<Value>)>>,
    /// Current left tuple being processed
    current_left_tuple: Option<(Tuple, Vec<Value>)>,
    /// Current matches from hash table for current left tuple
    current_matches: Vec<(Tuple, Vec<Value>)>,
    /// Index of current match being returned
    current_match_index: usize,
    /// Whether the executor has been initialized
    initialized: bool,
}

impl HashJoinExecutor {
    /// Create a new hash join executor with equi-join conditions
    ///
    /// # Arguments
    /// * `left_child` - The left child executor
    /// * `right_child` - The right child executor  
    /// * `left_keys` - Join key expressions evaluated on left tuples
    /// * `right_keys` - Join key expressions evaluated on right tuples (must have same count as left_keys)
    /// * `additional_condition` - Optional additional condition evaluated after hash match
    pub fn new(
        left_child: Box<dyn Executor>,
        right_child: Box<dyn Executor>,
        left_keys: Vec<Expression>,
        right_keys: Vec<Expression>,
        additional_condition: Option<Expression>,
    ) -> Result<Self> {
        if left_keys.len() != right_keys.len() {
            bail!(
                "Join key count mismatch: {} left keys vs {} right keys",
                left_keys.len(),
                right_keys.len()
            );
        }

        if left_keys.is_empty() {
            bail!("Hash join requires at least one equi-join key");
        }

        Ok(Self {
            left_child,
            right_child,
            left_keys,
            right_keys,
            additional_condition,
            output_schema: Vec::new(),
            left_schema_types: Vec::new(),
            right_schema_types: Vec::new(),
            combined_schema_types: Vec::new(),
            hash_table: HashMap::new(),
            current_left_tuple: None,
            current_matches: Vec::new(),
            current_match_index: 0,
            initialized: false,
        })
    }

    /// Build the hash table from the right relation
    fn build_hash_table(&mut self) -> Result<()> {
        // Clear any existing hash table
        self.hash_table.clear();

        // Process all tuples from right child
        while let Some(right_tuple) = self.right_child.next()? {
            // Deserialize right tuple values
            let right_values = deserialize_values(&right_tuple.data, &self.right_schema_types)?;

            // Evaluate join keys for right tuple
            let mut key_values = Vec::with_capacity(self.right_keys.len());
            for key_expr in &self.right_keys {
                let key_value = evaluate_expression(key_expr, &right_values)?;
                key_values.push(key_value);
            }

            // Insert into hash table (handle collisions by appending to vector)
            self.hash_table
                .entry(HashKey(key_values))
                .or_default()
                .push((right_tuple, right_values));
        }

        Ok(())
    }

    /// Probe the hash table for matches with the given left tuple
    fn probe_hash_table(&mut self, left_tuple: Tuple, left_values: Vec<Value>) -> Result<()> {
        // Evaluate join keys for left tuple
        let mut key_values = Vec::with_capacity(self.left_keys.len());
        for key_expr in &self.left_keys {
            let key_value = evaluate_expression(key_expr, &left_values)?;
            key_values.push(key_value);
        }

        // Look up matches in hash table
        self.current_matches.clear();
        if let Some(matches) = self.hash_table.get(&HashKey(key_values)) {
            // Found potential matches
            for (right_tuple, right_values) in matches {
                // If there's an additional condition, check it
                if let Some(ref condition) = self.additional_condition {
                    // Combine left and right values for condition evaluation
                    let mut combined_values = left_values.clone();
                    combined_values.extend(right_values.clone());

                    match evaluate_expression(condition, &combined_values)? {
                        Value::Boolean(true) => {
                            // Condition satisfied, add to matches
                            self.current_matches
                                .push((right_tuple.clone(), right_values.clone()));
                        }
                        Value::Boolean(false) | Value::Null => {
                            // Condition not satisfied or NULL, skip
                            continue;
                        }
                        _ => {
                            bail!("Additional join condition did not evaluate to boolean");
                        }
                    }
                } else {
                    // No additional condition, all hash matches are valid
                    self.current_matches
                        .push((right_tuple.clone(), right_values.clone()));
                }
            }
        }

        self.current_left_tuple = Some((left_tuple, left_values));
        self.current_match_index = 0;

        Ok(())
    }
}

impl Executor for HashJoinExecutor {
    fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Initialize both child executors
        self.left_child.init()?;
        self.right_child.init()?;

        // Get schemas from both children
        let left_schema = self.left_child.output_schema();
        let right_schema = self.right_child.output_schema();

        // Extract data types
        self.left_schema_types = left_schema.iter().map(|col| col.data_type).collect();
        self.right_schema_types = right_schema.iter().map(|col| col.data_type).collect();

        // Build combined output schema
        self.output_schema = left_schema
            .iter()
            .cloned()
            .chain(right_schema.iter().cloned())
            .collect();

        // Build combined schema types
        self.combined_schema_types = self
            .left_schema_types
            .iter()
            .cloned()
            .chain(self.right_schema_types.iter().cloned())
            .collect();

        // Type check left join keys
        let left_type_checker = TypeChecker::new(&self.left_schema_types);
        for key_expr in &self.left_keys {
            left_type_checker.check(key_expr)?;
        }

        // Type check right join keys
        let right_type_checker = TypeChecker::new(&self.right_schema_types);
        for key_expr in &self.right_keys {
            right_type_checker.check(key_expr)?;
        }

        // Type check additional condition if present
        if let Some(ref condition) = self.additional_condition {
            let combined_type_checker = TypeChecker::new(&self.combined_schema_types);
            combined_type_checker.check_filter_predicate(condition)?;
        }

        // Build hash table from right relation
        self.build_hash_table()?;

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        loop {
            // If we have matches for current left tuple, return next match
            if let Some((left_tuple, left_values)) = &self.current_left_tuple {
                if self.current_match_index < self.current_matches.len() {
                    let (_right_tuple, right_values) =
                        &self.current_matches[self.current_match_index];
                    self.current_match_index += 1;

                    // Combine left and right values
                    let mut combined_values = left_values.clone();
                    combined_values.extend(right_values.clone());

                    // Serialize combined tuple
                    let combined_data =
                        serialize_values(&combined_values, &self.combined_schema_types)?;

                    // Use left tuple's ID for the combined tuple
                    let combined_tuple = Tuple::new(left_tuple.tuple_id, combined_data);

                    return Ok(Some(combined_tuple));
                }
            }

            // Need to get next left tuple
            match self.left_child.next()? {
                Some(left_tuple) => {
                    // Deserialize left tuple values
                    let left_values =
                        deserialize_values(&left_tuple.data, &self.left_schema_types)?;

                    // Probe hash table for matches
                    self.probe_hash_table(left_tuple, left_values)?;
                }
                None => {
                    // No more left tuples
                    return Ok(None);
                }
            }
        }
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::TupleId;
    use crate::executor::FilterBuilder;
    use crate::storage::page::PageId;

    /// Mock executor for testing
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
            self.current = 0;
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

            let data = serialize_values(values, &self.schema_types)?;
            Ok(Some(Tuple::new(*tuple_id, data)))
        }

        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }
    }

    #[test]
    fn test_hash_join_basic() -> Result<()> {
        // Create left table data (employees)
        let left_schema = vec![
            ColumnInfo::new("emp_id", DataType::Int32),
            ColumnInfo::new("emp_name", DataType::Varchar),
            ColumnInfo::new("dept_id", DataType::Int32),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Int32(10),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Int32(20),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(3),
                    Value::String("Charlie".to_string()),
                    Value::Int32(10),
                ],
            ),
        ];

        // Create right table data (departments)
        let right_schema = vec![
            ColumnInfo::new("dept_id", DataType::Int32),
            ColumnInfo::new("dept_name", DataType::Varchar),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![Value::Int32(10), Value::String("Engineering".to_string())],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![Value::Int32(20), Value::String("Sales".to_string())],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema.clone()));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema.clone()));

        // Create hash join on dept_id
        let left_keys = vec![FilterBuilder::column(2)]; // left.dept_id
        let right_keys = vec![FilterBuilder::column(0)]; // right.dept_id

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        // Verify output schema
        assert_eq!(join.output_schema().len(), 5); // 3 + 2 columns

        // Collect all results
        let mut results = Vec::new();
        while let Some(tuple) = join.next()? {
            let values = deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,
                    DataType::Varchar,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Varchar,
                ],
            )?;
            results.push(values);
        }

        // Should have 3 results (Alice and Charlie in Engineering, Bob in Sales)
        assert_eq!(results.len(), 3);

        // Results may come in any order due to hash table, so check by employee name
        for result in &results {
            match &result[1] {
                Value::String(name) if name == "Alice" => {
                    assert_eq!(result[0], Value::Int32(1));
                    assert_eq!(result[2], Value::Int32(10));
                    assert_eq!(result[3], Value::Int32(10));
                    assert_eq!(result[4], Value::String("Engineering".to_string()));
                }
                Value::String(name) if name == "Bob" => {
                    assert_eq!(result[0], Value::Int32(2));
                    assert_eq!(result[2], Value::Int32(20));
                    assert_eq!(result[3], Value::Int32(20));
                    assert_eq!(result[4], Value::String("Sales".to_string()));
                }
                Value::String(name) if name == "Charlie" => {
                    assert_eq!(result[0], Value::Int32(3));
                    assert_eq!(result[2], Value::Int32(10));
                    assert_eq!(result[3], Value::Int32(10));
                    assert_eq!(result[4], Value::String("Engineering".to_string()));
                }
                _ => panic!("Unexpected employee name"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_hash_join_multiple_keys() -> Result<()> {
        // Test joining on multiple keys
        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("key1", DataType::Int32),
            ColumnInfo::new("key2", DataType::Varchar),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::Int32(10),
                    Value::String("A".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::Int32(20),
                    Value::String("B".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(3),
                    Value::Int32(10),
                    Value::String("A".to_string()),
                ],
            ),
        ];

        let right_schema = vec![
            ColumnInfo::new("key1", DataType::Int32),
            ColumnInfo::new("key2", DataType::Varchar),
            ColumnInfo::new("data", DataType::Varchar),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![
                    Value::Int32(10),
                    Value::String("A".to_string()),
                    Value::String("Match1".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![
                    Value::Int32(20),
                    Value::String("B".to_string()),
                    Value::String("Match2".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(2), 2),
                vec![
                    Value::Int32(10),
                    Value::String("B".to_string()),
                    Value::String("NoMatch".to_string()),
                ],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        // Join on both key1 and key2
        let left_keys = vec![FilterBuilder::column(1), FilterBuilder::column(2)];
        let right_keys = vec![FilterBuilder::column(0), FilterBuilder::column(1)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = join.next()? {
            let values = deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Varchar,
                    DataType::Int32,
                    DataType::Varchar,
                    DataType::Varchar,
                ],
            )?;
            results.push(values);
        }

        // Should have 3 matches (ID 1 and 3 with Match1, ID 2 with Match2)
        assert_eq!(results.len(), 3);

        Ok(())
    }

    #[test]
    fn test_hash_join_with_additional_condition() -> Result<()> {
        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
            ColumnInfo::new("join_key", DataType::Int32),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(100), Value::Int32(1)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Int32(200), Value::Int32(1)],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::Int32(300), Value::Int32(2)],
            ),
        ];

        let right_schema = vec![
            ColumnInfo::new("join_key", DataType::Int32),
            ColumnInfo::new("threshold", DataType::Int32),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![Value::Int32(1), Value::Int32(150)],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![Value::Int32(2), Value::Int32(250)],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        // Hash join on join_key
        let left_keys = vec![FilterBuilder::column(2)];
        let right_keys = vec![FilterBuilder::column(0)];

        // Additional condition: left.value > right.threshold
        let additional_condition = Some(FilterBuilder::gt(
            FilterBuilder::column(1), // left.value
            FilterBuilder::column(4), // right.threshold (3 left cols + 1)
        ));

        let mut join = HashJoinExecutor::new(
            left_executor,
            right_executor,
            left_keys,
            right_keys,
            additional_condition,
        )?;
        join.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = join.next()? {
            let values = deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                ],
            )?;
            results.push(values);
        }

        // Only ID 2 (value 200 > threshold 150) and ID 3 (value 300 > threshold 250) should match
        assert_eq!(results.len(), 2);

        Ok(())
    }

    #[test]
    fn test_hash_join_with_duplicates() -> Result<()> {
        // Test handling of duplicate join keys
        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("join_key", DataType::Int32),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Int32(10)],
            ),
        ];

        let right_schema = vec![
            ColumnInfo::new("join_key", DataType::Int32),
            ColumnInfo::new("data", DataType::Varchar),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![Value::Int32(10), Value::String("A".to_string())],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![Value::Int32(10), Value::String("B".to_string())],
            ),
            (
                TupleId::new(PageId(2), 2),
                vec![Value::Int32(10), Value::String("C".to_string())],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        let left_keys = vec![FilterBuilder::column(1)];
        let right_keys = vec![FilterBuilder::column(0)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = join.next()? {
            let values = deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Varchar,
                ],
            )?;
            results.push(values);
        }

        // Should have 6 results (2 left tuples × 3 right tuples)
        assert_eq!(results.len(), 6);

        Ok(())
    }

    #[test]
    fn test_hash_join_no_matches() -> Result<()> {
        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Int32(20)],
            ),
        ];

        let right_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![Value::Int32(3), Value::Int32(30)],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![Value::Int32(4), Value::Int32(40)],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        let left_keys = vec![FilterBuilder::column(1)]; // left.value
        let right_keys = vec![FilterBuilder::column(1)]; // right.value

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        // Should return no results
        assert!(join.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_hash_join_with_nulls() -> Result<()> {
        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let left_tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Null],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::Int32(30)],
            ),
        ];

        let right_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let right_tuples = vec![
            (
                TupleId::new(PageId(2), 0),
                vec![Value::Int32(4), Value::Int32(10)],
            ),
            (
                TupleId::new(PageId(2), 1),
                vec![Value::Int32(5), Value::Null],
            ),
            (
                TupleId::new(PageId(2), 2),
                vec![Value::Int32(6), Value::Int32(30)],
            ),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        let left_keys = vec![FilterBuilder::column(1)];
        let right_keys = vec![FilterBuilder::column(1)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = join.next()? {
            let values = deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                ],
            )?;
            results.push(values);
        }

        // NULLs should match with each other in hash join (unlike nested loop join)
        // Should have 3 matches: (1,10)-(4,10), (2,NULL)-(5,NULL), (3,30)-(6,30)
        assert_eq!(results.len(), 3);

        Ok(())
    }

    #[test]
    fn test_hash_join_empty_relations() -> Result<()> {
        // Test with empty right relation
        let left_schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let left_tuples = vec![(TupleId::new(PageId(1), 0), vec![Value::Int32(1)])];

        let right_schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let right_tuples = vec![];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        let left_keys = vec![FilterBuilder::column(0)];
        let right_keys = vec![FilterBuilder::column(0)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        assert!(join.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_hash_join_not_initialized() -> Result<()> {
        let left_schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let right_schema = vec![ColumnInfo::new("id", DataType::Int32)];

        let left_executor = Box::new(MockExecutor::new(vec![], left_schema));
        let right_executor = Box::new(MockExecutor::new(vec![], right_schema));

        let left_keys = vec![FilterBuilder::column(0)];
        let right_keys = vec![FilterBuilder::column(0)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;

        // Try to call next() without init()
        let result = join.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_hash_join_mismatched_key_count() -> Result<()> {
        let left_schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let right_schema = vec![ColumnInfo::new("id", DataType::Int32)];

        let left_executor = Box::new(MockExecutor::new(vec![], left_schema));
        let right_executor = Box::new(MockExecutor::new(vec![], right_schema));

        let left_keys = vec![FilterBuilder::column(0)];
        let right_keys = vec![FilterBuilder::column(0), FilterBuilder::column(1)]; // Mismatch!

        let result =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("key count mismatch"));
        }

        Ok(())
    }

    #[test]
    fn test_hash_join_no_keys() -> Result<()> {
        let left_schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let right_schema = vec![ColumnInfo::new("id", DataType::Int32)];

        let left_executor = Box::new(MockExecutor::new(vec![], left_schema));
        let right_executor = Box::new(MockExecutor::new(vec![], right_schema));

        let result = HashJoinExecutor::new(left_executor, right_executor, vec![], vec![], None);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("at least one equi-join key"));
        }

        Ok(())
    }

    #[test]
    fn test_hash_join_performance_comparison() -> Result<()> {
        // This test demonstrates the efficiency of hash join for larger datasets
        let mut left_tuples = Vec::new();
        let mut right_tuples = Vec::new();

        // Create 100 left tuples
        for i in 0..100 {
            left_tuples.push((
                TupleId::new(PageId(1), i as u16),
                vec![Value::Int32(i), Value::Int32(i % 10)], // Join key cycles 0-9
            ));
        }

        // Create 20 right tuples
        for i in 0..20 {
            right_tuples.push((
                TupleId::new(PageId(2), i as u16),
                vec![Value::Int32(i % 10), Value::String(format!("Data{}", i))],
            ));
        }

        let left_schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("join_key", DataType::Int32),
        ];
        let right_schema = vec![
            ColumnInfo::new("join_key", DataType::Int32),
            ColumnInfo::new("data", DataType::Varchar),
        ];

        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema));

        let left_keys = vec![FilterBuilder::column(1)];
        let right_keys = vec![FilterBuilder::column(0)];

        let mut join =
            HashJoinExecutor::new(left_executor, right_executor, left_keys, right_keys, None)?;
        join.init()?;

        let mut count = 0;
        while join.next()?.is_some() {
            count += 1;
        }

        // Each join key 0-9 appears 10 times in left and 2 times in right
        // So we should have 10 * 10 * 2 = 200 results
        assert_eq!(count, 200);

        Ok(())
    }
}
