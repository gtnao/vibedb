//! Filter executor implementation.
//!
//! This executor filters tuples from a child executor based on a predicate function.
//! It implements the volcano-style iterator model, producing one tuple at a time
//! that matches the predicate.

use crate::access::{deserialize_values, DataType, Tuple, Value};
use crate::executor::{ColumnInfo, Executor};
use anyhow::{bail, Result};

/// A predicate function that evaluates a row of values
pub type Predicate = Box<dyn Fn(&[Value]) -> bool + Send>;

/// Executor that filters tuples based on a predicate
pub struct FilterExecutor {
    /// Child executor that produces tuples
    child: Box<dyn Executor>,
    /// Predicate function to evaluate each tuple
    predicate: Predicate,
    /// Output schema (same as child's schema)
    output_schema: Vec<ColumnInfo>,
    /// Schema data types for deserialization
    schema_types: Vec<DataType>,
    /// Whether the executor has been initialized
    initialized: bool,
}

impl FilterExecutor {
    /// Create a new filter executor
    ///
    /// # Arguments
    /// * `child` - The child executor that produces tuples
    /// * `predicate` - The predicate function to filter tuples
    pub fn new(child: Box<dyn Executor>, predicate: Predicate) -> Self {
        Self {
            child,
            predicate,
            output_schema: Vec::new(),
            schema_types: Vec::new(),
            initialized: false,
        }
    }
}

impl Executor for FilterExecutor {
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

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        // Keep getting tuples from child until we find one that matches the predicate
        loop {
            match self.child.next()? {
                Some(tuple) => {
                    // Deserialize the tuple data into values
                    let values = deserialize_values(&tuple.data, &self.schema_types)?;

                    // Evaluate the predicate
                    if (self.predicate)(&values) {
                        // Predicate matches, return this tuple
                        return Ok(Some(tuple));
                    }
                    // Predicate doesn't match, continue to next tuple
                }
                None => {
                    // No more tuples from child
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
    use crate::access::{serialize_values, TableHeap, TupleId, Value};
    use crate::database::Database;
    use crate::executor::{ExecutionContext, SeqScanExecutor};
    use crate::storage::page::PageId;
    use tempfile::tempdir;

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
    fn test_filter_basic() -> Result<()> {
        // Create test data
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("age", DataType::Int32),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Int32(25),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Int32(30),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(3),
                    Value::String("Charlie".to_string()),
                    Value::Int32(35),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema.clone()));

        // Create filter that selects age > 28
        let predicate: Predicate = Box::new(|values| {
            if let Value::Int32(age) = &values[2] {
                *age > 28
            } else {
                false
            }
        });

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Verify schema
        assert_eq!(filter.output_schema(), &schema);

        // Get first matching tuple (Bob, age 30)
        let tuple1 = filter.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(
            &tuple1.data,
            &vec![DataType::Int32, DataType::Varchar, DataType::Int32],
        )?;
        assert_eq!(values1[0], Value::Int32(2));
        assert_eq!(values1[1], Value::String("Bob".to_string()));
        assert_eq!(values1[2], Value::Int32(30));

        // Get second matching tuple (Charlie, age 35)
        let tuple2 = filter.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(
            &tuple2.data,
            &vec![DataType::Int32, DataType::Varchar, DataType::Int32],
        )?;
        assert_eq!(values2[0], Value::Int32(3));
        assert_eq!(values2[1], Value::String("Charlie".to_string()));
        assert_eq!(values2[2], Value::Int32(35));

        // No more matching tuples
        assert!(filter.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_filter_none_match() -> Result<()> {
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
                vec![Value::Int32(2), Value::Int32(20)],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::Int32(30)],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create filter that matches nothing (value > 100)
        let predicate: Predicate = Box::new(|values| {
            if let Value::Int32(val) = &values[1] {
                *val > 100
            } else {
                false
            }
        });

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Should return no tuples
        assert!(filter.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_filter_all_match() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];

        let tuples = vec![
            (TupleId::new(PageId(1), 0), vec![Value::Int32(1)]),
            (TupleId::new(PageId(1), 1), vec![Value::Int32(2)]),
            (TupleId::new(PageId(1), 2), vec![Value::Int32(3)]),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create filter that matches all (always true)
        let predicate: Predicate = Box::new(|_| true);

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Should return all three tuples
        assert!(filter.next()?.is_some());
        assert!(filter.next()?.is_some());
        assert!(filter.next()?.is_some());
        assert!(filter.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_filter_with_null() -> Result<()> {
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
                vec![Value::Int32(3), Value::String("Charlie".to_string())],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create filter that excludes NULL names
        let predicate: Predicate = Box::new(|values| !matches!(&values[1], Value::Null));

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Should return only non-NULL names
        let tuple1 = filter.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values1[0], Value::Int32(1));

        let tuple2 = filter.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values2[0], Value::Int32(3));

        assert!(filter.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_filter_not_initialized() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));
        let predicate: Predicate = Box::new(|_| true);

        let mut filter = FilterExecutor::new(mock_executor, predicate);

        // Try to call next() without init()
        let result = filter.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_filter_with_seq_scan() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("age", DataType::Int32),
            ],
        )?;

        // Insert test data
        let table_info = db.catalog.get_table("users")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];

        heap.insert_values(
            &vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Int32(25),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(2),
                Value::String("Bob".to_string()),
                Value::Int32(30),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(3),
                Value::String("Charlie".to_string()),
                Value::Int32(20),
            ],
            &schema,
        )?;

        // Create execution context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create seq scan
        let seq_scan = Box::new(SeqScanExecutor::new("users".to_string(), context));

        // Create filter for age >= 25
        let predicate: Predicate = Box::new(|values| {
            if let Value::Int32(age) = &values[2] {
                *age >= 25
            } else {
                false
            }
        });

        let mut filter = FilterExecutor::new(seq_scan, predicate);
        filter.init()?;

        // Collect results
        let mut results = Vec::new();
        while let Some(tuple) = filter.next()? {
            let values = deserialize_values(&tuple.data, &schema)?;
            results.push(values);
        }

        // Should have 2 results (Alice and Bob)
        assert_eq!(results.len(), 2);

        // Verify the results contain Alice and Bob
        let has_alice = results
            .iter()
            .any(|values| matches!(&values[1], Value::String(name) if name == "Alice"));
        let has_bob = results
            .iter()
            .any(|values| matches!(&values[1], Value::String(name) if name == "Bob"));

        assert!(has_alice);
        assert!(has_bob);

        Ok(())
    }

    #[test]
    fn test_filter_complex_predicate() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("active", DataType::Boolean),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Boolean(true),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Boolean(false),
                ],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(3),
                    Value::String("Alice".to_string()),
                    Value::Boolean(false),
                ],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![
                    Value::Int32(4),
                    Value::String("Dave".to_string()),
                    Value::Boolean(true),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Complex predicate: name = "Alice" AND active = true
        let predicate: Predicate = Box::new(|values| {
            let name_matches = matches!(&values[1], Value::String(name) if name == "Alice");
            let is_active = matches!(&values[2], Value::Boolean(true));
            name_matches && is_active
        });

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Should return only the first tuple
        let tuple = filter.next()?.expect("Should have one tuple");
        let values = deserialize_values(
            &tuple.data,
            &vec![DataType::Int32, DataType::Varchar, DataType::Boolean],
        )?;
        assert_eq!(values[0], Value::Int32(1));
        assert_eq!(values[1], Value::String("Alice".to_string()));
        assert_eq!(values[2], Value::Boolean(true));

        // No more matching tuples
        assert!(filter.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_filter_string_operations() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("email", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("alice@example.com".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::String("bob@test.com".to_string())],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![
                    Value::Int32(3),
                    Value::String("charlie@example.com".to_string()),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Filter for emails ending with "@example.com"
        let predicate: Predicate = Box::new(|values| {
            if let Value::String(email) = &values[1] {
                email.ends_with("@example.com")
            } else {
                false
            }
        });

        let mut filter = FilterExecutor::new(mock_executor, predicate);
        filter.init()?;

        // Should return two tuples
        let tuple1 = filter.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values1[0], Value::Int32(1));

        let tuple2 = filter.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values2[0], Value::Int32(3));

        assert!(filter.next()?.is_none());

        Ok(())
    }
}
