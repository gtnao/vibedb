//! Projection executor implementation.
//!
//! This executor projects specific columns from a child executor, allowing for
//! column selection and reordering. It implements the volcano-style iterator model,
//! producing tuples with only the specified columns.

use crate::access::{deserialize_values, serialize_values, DataType, Tuple, Value};
use crate::executor::{ColumnInfo, Executor};
use anyhow::{bail, Result};

/// Executor that projects specific columns from child tuples
pub struct ProjectionExecutor {
    /// Child executor that produces tuples
    child: Box<dyn Executor>,
    /// Indices of columns to project from the child's output
    column_indices: Vec<usize>,
    /// Output schema (projected columns)
    output_schema: Vec<ColumnInfo>,
    /// Child's schema data types for deserialization
    child_schema_types: Vec<DataType>,
    /// Output schema data types for serialization
    output_schema_types: Vec<DataType>,
    /// Whether the executor has been initialized
    initialized: bool,
}

impl ProjectionExecutor {
    /// Create a new projection executor
    ///
    /// # Arguments
    /// * `child` - The child executor that produces tuples
    /// * `column_indices` - Indices of columns to project from child's output (0-based)
    ///
    /// # Example
    /// ```ignore
    /// // If child produces columns [id, name, age, email]
    /// // column_indices [2, 0] would produce [age, id]
    /// ```
    pub fn new(child: Box<dyn Executor>, column_indices: Vec<usize>) -> Self {
        Self {
            child,
            column_indices,
            output_schema: Vec::new(),
            child_schema_types: Vec::new(),
            output_schema_types: Vec::new(),
            initialized: false,
        }
    }
}

impl Executor for ProjectionExecutor {
    fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Initialize child executor if not already initialized
        // Note: In some cases the child may already be initialized
        self.child.init()?;

        // Get child's output schema
        let child_schema = self.child.output_schema();

        // Validate column indices
        for &idx in &self.column_indices {
            if idx >= child_schema.len() {
                bail!(
                    "Column index {} is out of bounds for schema with {} columns",
                    idx,
                    child_schema.len()
                );
            }
        }

        // Build projected output schema based on column indices
        self.output_schema = self
            .column_indices
            .iter()
            .map(|&idx| child_schema[idx].clone())
            .collect();

        // Extract data types for deserialization/serialization
        self.child_schema_types = child_schema.iter().map(|col| col.data_type).collect();
        self.output_schema_types = self.output_schema.iter().map(|col| col.data_type).collect();

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        // Get next tuple from child
        match self.child.next()? {
            Some(tuple) => {
                // Deserialize the tuple data into values
                let all_values = deserialize_values(&tuple.data, &self.child_schema_types)?;

                // Project only the specified columns
                let projected_values: Vec<Value> = self
                    .column_indices
                    .iter()
                    .map(|&idx| all_values[idx].clone())
                    .collect();

                // Serialize the projected values
                let projected_data =
                    serialize_values(&projected_values, &self.output_schema_types)?;

                // Return tuple with same ID but projected data
                Ok(Some(Tuple::new(tuple.tuple_id, projected_data)))
            }
            None => Ok(None),
        }
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{TupleId, Value};
    use crate::database::Database;
    use crate::executor::{ExecutionContext, FilterExecutor, SeqScanExecutor};
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
    fn test_projection_basic() -> Result<()> {
        // Create test data with 4 columns
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("age", DataType::Int32),
            ColumnInfo::new("email", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Int32(25),
                    Value::String("alice@example.com".to_string()),
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Int32(30),
                    Value::String("bob@example.com".to_string()),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Project columns [1, 2] (name, age)
        let mut projection = ProjectionExecutor::new(mock_executor, vec![1, 2]);
        projection.init()?;

        // Verify output schema
        let output_schema = projection.output_schema();
        assert_eq!(output_schema.len(), 2);
        assert_eq!(output_schema[0].name, "name");
        assert_eq!(output_schema[0].data_type, DataType::Varchar);
        assert_eq!(output_schema[1].name, "age");
        assert_eq!(output_schema[1].data_type, DataType::Int32);

        // Get first projected tuple
        let tuple1 = projection.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Varchar, DataType::Int32])?;
        assert_eq!(values1[0], Value::String("Alice".to_string()));
        assert_eq!(values1[1], Value::Int32(25));

        // Get second projected tuple
        let tuple2 = projection.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Varchar, DataType::Int32])?;
        assert_eq!(values2[0], Value::String("Bob".to_string()));
        assert_eq!(values2[1], Value::Int32(30));

        // No more tuples
        assert!(projection.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_projection_reordering() -> Result<()> {
        // Test column reordering
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("active", DataType::Boolean),
        ];

        let tuples = vec![(
            TupleId::new(PageId(1), 0),
            vec![
                Value::Int32(100),
                Value::String("Test".to_string()),
                Value::Boolean(true),
            ],
        )];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Project columns [2, 0, 1] (active, id, name) - reordering
        let mut projection = ProjectionExecutor::new(mock_executor, vec![2, 0, 1]);
        projection.init()?;

        // Verify reordered schema
        let output_schema = projection.output_schema();
        assert_eq!(output_schema[0].name, "active");
        assert_eq!(output_schema[1].name, "id");
        assert_eq!(output_schema[2].name, "name");

        // Get projected tuple with reordered columns
        let tuple = projection.next()?.expect("Should have tuple");
        let values = deserialize_values(
            &tuple.data,
            &vec![DataType::Boolean, DataType::Int32, DataType::Varchar],
        )?;
        assert_eq!(values[0], Value::Boolean(true));
        assert_eq!(values[1], Value::Int32(100));
        assert_eq!(values[2], Value::String("Test".to_string()));

        Ok(())
    }

    #[test]
    fn test_projection_single_column() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("value", DataType::Int32),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::Int32(100)],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::Int32(200)],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Project only column [1] (value)
        let mut projection = ProjectionExecutor::new(mock_executor, vec![1]);
        projection.init()?;

        // Verify single column schema
        let output_schema = projection.output_schema();
        assert_eq!(output_schema.len(), 1);
        assert_eq!(output_schema[0].name, "value");

        // Get values
        let tuple1 = projection.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Int32])?;
        assert_eq!(values1[0], Value::Int32(100));

        let tuple2 = projection.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Int32])?;
        assert_eq!(values2[0], Value::Int32(200));

        Ok(())
    }

    #[test]
    fn test_projection_duplicate_columns() -> Result<()> {
        // Test projecting the same column multiple times
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![(
            TupleId::new(PageId(1), 0),
            vec![Value::Int32(1), Value::String("Test".to_string())],
        )];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Project columns [0, 1, 0] (id, name, id)
        let mut projection = ProjectionExecutor::new(mock_executor, vec![0, 1, 0]);
        projection.init()?;

        // Verify schema with duplicate
        let output_schema = projection.output_schema();
        assert_eq!(output_schema.len(), 3);
        assert_eq!(output_schema[0].name, "id");
        assert_eq!(output_schema[1].name, "name");
        assert_eq!(output_schema[2].name, "id");

        // Get projected tuple
        let tuple = projection.next()?.expect("Should have tuple");
        let values = deserialize_values(
            &tuple.data,
            &vec![DataType::Int32, DataType::Varchar, DataType::Int32],
        )?;
        assert_eq!(values[0], Value::Int32(1));
        assert_eq!(values[1], Value::String("Test".to_string()));
        assert_eq!(values[2], Value::Int32(1)); // Duplicated column

        Ok(())
    }

    #[test]
    fn test_projection_with_nulls() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
            ColumnInfo::new("email", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Null,
                ],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![
                    Value::Int32(2),
                    Value::Null,
                    Value::String("bob@example.com".to_string()),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Project columns [0, 2] (id, email)
        let mut projection = ProjectionExecutor::new(mock_executor, vec![0, 2]);
        projection.init()?;

        // First tuple: id=1, email=NULL
        let tuple1 = projection.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values1[0], Value::Int32(1));
        assert_eq!(values1[1], Value::Null);

        // Second tuple: id=2, email="bob@example.com"
        let tuple2 = projection.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Int32, DataType::Varchar])?;
        assert_eq!(values2[0], Value::Int32(2));
        assert_eq!(values2[1], Value::String("bob@example.com".to_string()));

        Ok(())
    }

    #[test]
    fn test_projection_empty_result() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));

        let mut projection = ProjectionExecutor::new(mock_executor, vec![0]);
        projection.init()?;

        // Should return no tuples
        assert!(projection.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_projection_invalid_index() -> Result<()> {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![(
            TupleId::new(PageId(1), 0),
            vec![Value::Int32(1), Value::String("Test".to_string())],
        )];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Try to project column index 2 (out of bounds)
        let mut projection = ProjectionExecutor::new(mock_executor, vec![0, 2]);

        // Should fail during init
        let result = projection.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));

        Ok(())
    }

    #[test]
    fn test_projection_not_initialized() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));

        let mut projection = ProjectionExecutor::new(mock_executor, vec![0]);

        // Try to call next() without init()
        let result = projection.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_projection_with_filter() -> Result<()> {
        // Test projection combined with filter
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
                    Value::Int32(20),
                ],
            ),
        ];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create filter for age >= 25
        use crate::executor::FilterBuilder;
        let filter_expr = FilterBuilder::column_ge_int32(2, 25);
        let filter = Box::new(FilterExecutor::new(mock_executor, filter_expr));

        // Project columns [1, 2] (name, age) from filtered results
        let mut projection = ProjectionExecutor::new(filter, vec![1, 2]);
        projection.init()?;

        // Should get Alice (25) and Bob (30), but not Charlie (20)
        let tuple1 = projection.next()?.expect("Should have first tuple");
        let values1 = deserialize_values(&tuple1.data, &vec![DataType::Varchar, DataType::Int32])?;
        assert_eq!(values1[0], Value::String("Alice".to_string()));
        assert_eq!(values1[1], Value::Int32(25));

        let tuple2 = projection.next()?.expect("Should have second tuple");
        let values2 = deserialize_values(&tuple2.data, &vec![DataType::Varchar, DataType::Int32])?;
        assert_eq!(values2[0], Value::String("Bob".to_string()));
        assert_eq!(values2[1], Value::Int32(30));

        assert!(projection.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_projection_all_columns() -> Result<()> {
        // Test projecting all columns in order
        let schema = vec![
            ColumnInfo::new("a", DataType::Int32),
            ColumnInfo::new("b", DataType::Int32),
            ColumnInfo::new("c", DataType::Int32),
        ];

        let tuples = vec![(
            TupleId::new(PageId(1), 0),
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
        )];

        let mock_executor = Box::new(MockExecutor::new(tuples, schema.clone()));

        // Project all columns [0, 1, 2]
        let mut projection = ProjectionExecutor::new(mock_executor, vec![0, 1, 2]);
        projection.init()?;

        // Output schema should match input
        assert_eq!(projection.output_schema(), &schema);

        // Values should be unchanged
        let tuple = projection.next()?.expect("Should have tuple");
        let values = deserialize_values(
            &tuple.data,
            &vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )?;
        assert_eq!(values[0], Value::Int32(1));
        assert_eq!(values[1], Value::Int32(2));
        assert_eq!(values[2], Value::Int32(3));

        Ok(())
    }

    #[test]
    fn test_projection_with_database() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "employees",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("department", DataType::Varchar),
                ("salary", DataType::Int32),
            ],
        )?;

        // Insert test data
        let table_info = db.catalog.get_table("employees")?.unwrap();
        let mut heap = crate::access::TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![
            DataType::Int32,
            DataType::Varchar,
            DataType::Varchar,
            DataType::Int32,
        ];

        heap.insert_values(
            &vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::String("Engineering".to_string()),
                Value::Int32(100000),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(2),
                Value::String("Bob".to_string()),
                Value::String("Sales".to_string()),
                Value::Int32(80000),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(3),
                Value::String("Charlie".to_string()),
                Value::String("Engineering".to_string()),
                Value::Int32(90000),
            ],
            &schema,
        )?;

        // Create execution context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create seq scan
        let seq_scan = Box::new(SeqScanExecutor::new("employees".to_string(), context));

        // Project columns [1, 3] (name, salary)
        let mut projection = ProjectionExecutor::new(seq_scan, vec![1, 3]);
        projection.init()?;

        // Verify output schema
        let output_schema = projection.output_schema();
        assert_eq!(output_schema.len(), 2);
        assert_eq!(output_schema[0].name, "name");
        assert_eq!(output_schema[1].name, "salary");

        // Collect all results
        let mut results = Vec::new();
        while let Some(tuple) = projection.next()? {
            let values =
                deserialize_values(&tuple.data, &vec![DataType::Varchar, DataType::Int32])?;
            results.push(values);
        }

        // Should have 3 results
        assert_eq!(results.len(), 3);

        // Verify we have the expected names and salaries
        let has_alice = results.iter().any(|values| {
            matches!(&values[0], Value::String(name) if name == "Alice")
                && matches!(&values[1], Value::Int32(100000))
        });
        let has_bob = results.iter().any(|values| {
            matches!(&values[0], Value::String(name) if name == "Bob")
                && matches!(&values[1], Value::Int32(80000))
        });
        let has_charlie = results.iter().any(|values| {
            matches!(&values[0], Value::String(name) if name == "Charlie")
                && matches!(&values[1], Value::Int32(90000))
        });

        assert!(has_alice);
        assert!(has_bob);
        assert!(has_charlie);

        Ok(())
    }
}
