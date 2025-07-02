//! Insert executor implementation.

use crate::access::{btree::BTree, DataType, TableHeap, Tuple, TupleId, Value};
use crate::executor::{ColumnInfo, ExecutionContext, Executor};
use anyhow::{bail, Result};

/// Executor for inserting rows into a table
pub struct InsertExecutor {
    table_name: String,
    values: Vec<Vec<Value>>,
    context: ExecutionContext,
    heap: Option<TableHeap>,
    schema: Vec<DataType>,
    output_schema: Vec<ColumnInfo>,
    current_index: usize,
    initialized: bool,
}

impl InsertExecutor {
    /// Create a new insert executor
    pub fn new(table_name: String, values: Vec<Vec<Value>>, context: ExecutionContext) -> Self {
        // Output schema for INSERT is typically the number of rows affected
        let output_schema = vec![ColumnInfo::new("count", DataType::Int32)];

        Self {
            table_name,
            values,
            context,
            heap: None,
            schema: Vec::new(),
            output_schema,
            current_index: 0,
            initialized: false,
        }
    }
}

impl Executor for InsertExecutor {
    fn init(&mut self) -> Result<()> {
        eprintln!("DEBUG: InsertExecutor::init for table: {}", self.table_name);
        if self.initialized {
            return Ok(());
        }

        // Get table info from catalog
        eprintln!("DEBUG: Getting table info from catalog");
        let table_info = self
            .context
            .catalog
            .get_table(&self.table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", self.table_name))?;
        eprintln!("DEBUG: Got table info, table_id: {:?}", table_info.table_id);

        // Get table schema
        eprintln!("DEBUG: Getting table columns from catalog");
        let columns = self
            .context
            .catalog
            .get_table_columns(table_info.table_id)?;
        eprintln!("DEBUG: Got {} columns", columns.len());

        // Extract schema
        self.schema = columns.iter().map(|col| col.column_type).collect();

        // Validate all rows against schema
        for (i, row) in self.values.iter().enumerate() {
            if row.len() != self.schema.len() {
                bail!(
                    "Row {} has {} values but table has {} columns",
                    i,
                    row.len(),
                    self.schema.len()
                );
            }

            // Validate each value against its column type
            for (j, (value, expected_type)) in row.iter().zip(self.schema.iter()).enumerate() {
                if !value.is_compatible_with(*expected_type) {
                    bail!(
                        "Row {} column {}: value {:?} is not compatible with type {:?}",
                        i,
                        j,
                        value,
                        expected_type
                    );
                }
            }
        }

        // Create table heap
        self.heap = Some(TableHeap::with_first_page(
            (*self.context.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        ));

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        // Insert all rows and return count
        if self.current_index == 0 && !self.values.is_empty() {
            let heap = self
                .heap
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Heap not available"))?;

            let mut insert_count = 0;

            // Get indexes for this table
            let indexes = self
                .context
                .catalog
                .get_table_indexes_by_name(&self.table_name)?;

            for row in &self.values {
                let tuple_id = heap.insert_values(row, &self.schema)?;

                // Update all indexes
                for index_info in &indexes {
                    // Extract key values for this index
                    let mut key_values = Vec::new();
                    for key_col in &index_info.key_columns {
                        // Find the position of this column in the table schema
                        let table_info = self
                            .context
                            .catalog
                            .get_table(&self.table_name)?
                            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

                        if let Some(col_names) = &table_info.column_names {
                            if let Some(pos) = col_names
                                .iter()
                                .position(|name| name == &key_col.column_name)
                            {
                                key_values.push(row[pos].clone());
                            } else {
                                anyhow::bail!("Column {} not found in table", key_col.column_name);
                            }
                        } else {
                            anyhow::bail!("Table has no column names");
                        }
                    }

                    // Open the B+Tree index and insert
                    let mut btree = BTree::open(
                        (*self.context.buffer_pool).clone(),
                        index_info.root_page_id,
                        index_info.key_columns.clone(),
                    )?;

                    btree.insert(&key_values, tuple_id)?;

                    // Update the root page id if it changed
                    let new_root = btree.root_page_id();
                    if new_root != index_info.root_page_id {
                        // Update pg_index table with new root_page_id
                        // Note: This requires a mutable reference to catalog, which we don't have
                        // in the current architecture. For now, we'll log this as a warning.
                        // In a real system, this would be handled through a transaction manager
                        // or by having the catalog handle index updates internally.
                        eprintln!(
                            "Warning: Index {} root page changed from {:?} to {:?}",
                            index_info.index_name, index_info.root_page_id, new_root
                        );

                        // We can't update the catalog here because we only have an Arc<Catalog>
                        // and catalog.update_index_root_page requires &mut self
                        // This is a limitation of the current architecture
                    }
                }

                insert_count += 1;
            }

            self.current_index = 1;

            // Return the count as a tuple
            let count_value = vec![Value::Int32(insert_count)];
            let count_schema = vec![DataType::Int32];
            let data = crate::access::serialize_values(&count_value, &count_schema)?;

            // For INSERT, we don't have a meaningful TupleId for the result
            // Use a dummy TupleId
            let dummy_tid = TupleId::new(crate::storage::page::PageId(0), 0);

            Ok(Some(Tuple::new(dummy_tid, data)))
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
    use crate::database::Database;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_insert_single_row() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create insert executor
        let values = vec![vec![Value::Int32(1), Value::String("Alice".to_string())]];

        let mut executor = InsertExecutor::new("test_table".to_string(), values, context);

        executor.init()?;

        // Execute insert
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;

        assert_eq!(count_values[0], Value::Int32(1));

        // No more results
        assert!(executor.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_insert_multiple_rows() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("active", DataType::Boolean),
            ],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create insert executor with multiple rows
        let values = vec![
            vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Boolean(true),
            ],
            vec![
                Value::Int32(2),
                Value::String("Bob".to_string()),
                Value::Boolean(false),
            ],
            vec![
                Value::Int32(3),
                Value::String("Charlie".to_string()),
                Value::Boolean(true),
            ],
        ];

        let mut executor = InsertExecutor::new("users".to_string(), values, context);

        executor.init()?;

        // Execute insert
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;

        assert_eq!(count_values[0], Value::Int32(3));

        Ok(())
    }

    #[test]
    fn test_insert_schema_mismatch() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Try to insert with wrong number of columns
        let values = vec![
            vec![Value::Int32(1)], // Missing name column
        ];

        let mut executor = InsertExecutor::new("test_table".to_string(), values, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("2 columns"));

        Ok(())
    }

    #[test]
    fn test_insert_type_mismatch() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("active", DataType::Boolean)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Try to insert with wrong type
        let values = vec![vec![
            Value::String("not_an_int".to_string()), // Should be Int32
            Value::Boolean(true),
        ]];

        let mut executor = InsertExecutor::new("test_table".to_string(), values, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not compatible"));

        Ok(())
    }

    #[test]
    fn test_insert_nonexistent_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let values = vec![vec![Value::Int32(1)]];

        let mut executor = InsertExecutor::new("nonexistent".to_string(), values, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_insert_empty_values() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns("test_table", vec![("id", DataType::Int32)])?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Insert with empty values vector
        let values = vec![];

        let mut executor = InsertExecutor::new("test_table".to_string(), values, context);

        executor.init()?;

        // Should return None since there's nothing to insert
        assert!(executor.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_insert_with_nulls() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("notes", DataType::Varchar),
            ],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Insert with NULL values
        let values = vec![
            vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Null,
            ],
            vec![
                Value::Int32(2),
                Value::Null,
                Value::String("Has no name".to_string()),
            ],
        ];

        let mut executor = InsertExecutor::new("test_table".to_string(), values, context);

        executor.init()?;

        // Execute insert
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;

        assert_eq!(count_values[0], Value::Int32(2));

        Ok(())
    }

    #[test]
    fn test_insert_with_index_update() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "users",
            vec![
                ("id", DataType::Int32),
                ("email", DataType::Varchar),
                ("age", DataType::Int32),
            ],
        )?;

        // Create index on email column
        let catalog = &mut *Arc::get_mut(&mut db.catalog).unwrap();
        catalog.create_index("idx_email", "users", &["email"], false)?;

        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Insert data
        let values = vec![
            vec![
                Value::Int32(1),
                Value::String("alice@example.com".to_string()),
                Value::Int32(25),
            ],
            vec![
                Value::Int32(2),
                Value::String("bob@example.com".to_string()),
                Value::Int32(30),
            ],
        ];

        let mut insert = InsertExecutor::new("users".to_string(), values, context.clone());
        insert.init()?;

        // Execute insert
        let result = insert.next()?;
        assert!(result.is_some());

        // Verify index was updated by using IndexScanExecutor
        let index_info = db
            .catalog
            .get_table_indexes_by_name("users")?
            .into_iter()
            .find(|idx| idx.index_name == "idx_email")
            .unwrap();

        let mut index_scan = crate::executor::index_scan::IndexScanExecutor::new(
            "users".to_string(),
            index_info,
            crate::executor::index_scan::IndexScanMode::Exact(vec![Value::String(
                "alice@example.com".to_string(),
            )]),
            db.buffer_pool.clone(),
            Arc::new(context),
        );

        index_scan.init()?;
        let scan_result = index_scan.next()?;
        assert!(scan_result.is_some());
        let tuple = scan_result.unwrap();
        let values = crate::access::deserialize_values(
            &tuple.data,
            &[DataType::Int32, DataType::Varchar, DataType::Int32],
        )?;
        assert_eq!(values[0], Value::Int32(1));
        assert_eq!(values[1], Value::String("alice@example.com".to_string()));
        assert_eq!(values[2], Value::Int32(25));

        Ok(())
    }
}
