//! Delete executor implementation.

use crate::access::{btree::BTree, DataType, TableHeap, TableScanner, Tuple, TupleId, Value};
use crate::executor::{ColumnInfo, ExecutionContext, Executor};
use anyhow::{bail, Result};

/// Executor for deleting rows from a table
pub struct DeleteExecutor {
    table_name: String,
    child: Option<Box<dyn Executor>>,
    context: ExecutionContext,
    heap: Option<TableHeap>,
    schema: Vec<DataType>,
    output_schema: Vec<ColumnInfo>,
    deleted_count: Option<i32>,
    initialized: bool,
}

impl DeleteExecutor {
    /// Create a new delete executor
    pub fn new(
        table_name: String,
        child: Option<Box<dyn Executor>>,
        context: ExecutionContext,
    ) -> Self {
        // Output schema for DELETE is typically the number of rows affected
        let output_schema = vec![ColumnInfo::new("count", DataType::Int32)];

        Self {
            table_name,
            child,
            context,
            heap: None,
            schema: Vec::new(),
            output_schema,
            deleted_count: None,
            initialized: false,
        }
    }

    /// Delete all tuples from the table
    fn delete_all(&mut self) -> Result<i32> {
        let table_info = self
            .context
            .catalog
            .get_table(&self.table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", self.table_name))?;

        let heap = self
            .heap
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Heap not available"))?;

        // Get indexes for this table
        let indexes = self
            .context
            .catalog
            .get_table_indexes_by_name(&self.table_name)?;

        // Create scanner to iterate through all tuples
        let scanner = TableScanner::new(
            (*self.context.buffer_pool).clone(),
            Some(table_info.first_page_id),
            self.schema.clone(),
            None,
        );

        let mut delete_count = 0;
        let mut tuples_to_delete = Vec::new();

        // First, collect all tuples to delete
        for result in scanner {
            let (tuple_id, values) = result?;
            tuples_to_delete.push((tuple_id, values));
        }

        // Now delete them
        for (tuple_id, values) in tuples_to_delete {
            // Delete from indexes first
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
                            key_values.push(values[pos].clone());
                        } else {
                            anyhow::bail!("Column {} not found in table", key_col.column_name);
                        }
                    } else {
                        anyhow::bail!("Table has no column names");
                    }
                }

                // Open the B+Tree index and delete
                let mut btree = BTree::open(
                    (*self.context.buffer_pool).clone(),
                    index_info.root_page_id,
                    index_info.key_columns.clone(),
                )?;

                btree.delete(&key_values, tuple_id)?;

                // Update the root page id if it changed
                let new_root = btree.root_page_id();
                if new_root != index_info.root_page_id {
                    // Same limitation as InsertExecutor - we can't update the catalog here
                    eprintln!(
                        "Warning: Index {} root page changed from {:?} to {:?}",
                        index_info.index_name, index_info.root_page_id, new_root
                    );
                }
            }

            // Delete from heap
            heap.delete(tuple_id)?;
            delete_count += 1;
        }

        Ok(delete_count)
    }

    /// Delete tuples matching the child executor's output
    fn delete_with_condition(&mut self) -> Result<i32> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Child executor not available"))?;

        // Initialize child executor
        child.init()?;

        let heap = self
            .heap
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Heap not available"))?;

        // Get indexes for this table
        let indexes = self
            .context
            .catalog
            .get_table_indexes_by_name(&self.table_name)?;

        let mut delete_count = 0;
        let mut tuples_to_delete = Vec::new();

        // Collect all tuples to delete from child executor
        while let Some(tuple) = child.next()? {
            // Extract tuple_id and values from the tuple
            let values = crate::access::deserialize_values(&tuple.data, &self.schema)?;
            tuples_to_delete.push((tuple.tuple_id, values));
        }

        // Now delete them
        for (tuple_id, values) in tuples_to_delete {
            // Delete from indexes first
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
                            key_values.push(values[pos].clone());
                        } else {
                            anyhow::bail!("Column {} not found in table", key_col.column_name);
                        }
                    } else {
                        anyhow::bail!("Table has no column names");
                    }
                }

                // Open the B+Tree index and delete
                let mut btree = BTree::open(
                    (*self.context.buffer_pool).clone(),
                    index_info.root_page_id,
                    index_info.key_columns.clone(),
                )?;

                btree.delete(&key_values, tuple_id)?;

                // Update the root page id if it changed
                let new_root = btree.root_page_id();
                if new_root != index_info.root_page_id {
                    eprintln!(
                        "Warning: Index {} root page changed from {:?} to {:?}",
                        index_info.index_name, index_info.root_page_id, new_root
                    );
                }
            }

            // Delete from heap
            heap.delete(tuple_id)?;
            delete_count += 1;
        }

        Ok(delete_count)
    }
}

impl Executor for DeleteExecutor {
    fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Get table info from catalog
        let table_info = self
            .context
            .catalog
            .get_table(&self.table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", self.table_name))?;

        // Get table schema
        let columns = self
            .context
            .catalog
            .get_table_columns(table_info.table_id)?;

        // Extract schema
        self.schema = columns.iter().map(|col| col.column_type).collect();

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

        // Delete tuples only once
        if self.deleted_count.is_none() {
            let count = if self.child.is_some() {
                self.delete_with_condition()?
            } else {
                self.delete_all()?
            };

            self.deleted_count = Some(count);

            // Return the count as a tuple
            let count_value = vec![Value::Int32(count)];
            let count_schema = vec![DataType::Int32];
            let data = crate::access::serialize_values(&count_value, &count_schema)?;

            // For DELETE, we don't have a meaningful TupleId for the result
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
    fn test_delete_all_rows() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Insert some data
        let table_info = db.catalog.get_table("test_table")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar];
        heap.insert_values(
            &vec![Value::Int32(1), Value::String("Alice".to_string())],
            &schema,
        )?;
        heap.insert_values(
            &vec![Value::Int32(2), Value::String("Bob".to_string())],
            &schema,
        )?;
        heap.insert_values(
            &vec![Value::Int32(3), Value::String("Charlie".to_string())],
            &schema,
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create delete executor (no child = delete all)
        let mut executor = DeleteExecutor::new("test_table".to_string(), None, context.clone());
        executor.init()?;

        // Execute delete
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(3));

        // No more results
        assert!(executor.next()?.is_none());

        // Verify table is empty
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("test_table".to_string(), context);
        scan.init()?;
        assert!(scan.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_delete_with_child_executor() -> Result<()> {
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

        // Insert some data
        let table_info = db.catalog.get_table("users")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];
        heap.insert_values(
            &vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Boolean(true),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(2),
                Value::String("Bob".to_string()),
                Value::Boolean(false),
            ],
            &schema,
        )?;
        heap.insert_values(
            &vec![
                Value::Int32(3),
                Value::String("Charlie".to_string()),
                Value::Boolean(true),
            ],
            &schema,
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create a mock child executor that returns specific tuples
        // For this test, we'll use a SeqScanExecutor as the child
        // In a real implementation, this would be a FilterExecutor that applies WHERE conditions
        let child_exec = Box::new(crate::executor::seq_scan::SeqScanExecutor::new(
            "users".to_string(),
            context.clone(),
        ));

        // Create delete executor with child
        let mut executor =
            DeleteExecutor::new("users".to_string(), Some(child_exec), context.clone());
        executor.init()?;

        // Execute delete - this will delete all rows since we're using SeqScan as child
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(3));

        // Verify table is empty
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("users".to_string(), context);
        scan.init()?;
        assert!(scan.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_delete_empty_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create empty table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create delete executor
        let mut executor = DeleteExecutor::new("test_table".to_string(), None, context);
        executor.init()?;

        // Execute delete
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(0));

        Ok(())
    }

    #[test]
    fn test_delete_nonexistent_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let mut executor = DeleteExecutor::new("nonexistent".to_string(), None, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_delete_with_index_update() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "products",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("price", DataType::Int32),
            ],
        )?;

        // Create index on name column
        let catalog = &mut *Arc::get_mut(&mut db.catalog).unwrap();
        catalog.create_index("idx_name", "products", &["name"], false)?;

        // Insert some data using InsertExecutor to ensure indexes are updated
        let values = vec![
            vec![
                Value::Int32(1),
                Value::String("Laptop".to_string()),
                Value::Int32(1000),
            ],
            vec![
                Value::Int32(2),
                Value::String("Mouse".to_string()),
                Value::Int32(50),
            ],
        ];

        let mut insert = crate::executor::insert::InsertExecutor::new(
            "products".to_string(),
            values,
            ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone()),
        );
        insert.init()?;
        insert.next()?;

        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Verify index has entries before delete
        let index_info = db
            .catalog
            .get_table_indexes_by_name("products")?
            .into_iter()
            .find(|idx| idx.index_name == "idx_name")
            .unwrap();

        let mut index_scan_before = crate::executor::index_scan::IndexScanExecutor::new(
            "products".to_string(),
            index_info.clone(),
            crate::executor::index_scan::IndexScanMode::Range {
                start: None,
                end: None,
                include_start: true,
                include_end: true,
            },
            db.buffer_pool.clone(),
            Arc::new(context.clone()),
        );
        index_scan_before.init()?;

        // Count entries in index before delete
        let mut count_before = 0;
        while index_scan_before.next()?.is_some() {
            count_before += 1;
        }
        assert_eq!(count_before, 2);

        // Delete all rows
        let mut delete = DeleteExecutor::new("products".to_string(), None, context.clone());
        delete.init()?;

        let result = delete.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(2));

        // Verify index is empty after delete
        let mut index_scan_after = crate::executor::index_scan::IndexScanExecutor::new(
            "products".to_string(),
            index_info,
            crate::executor::index_scan::IndexScanMode::Range {
                start: None,
                end: None,
                include_start: true,
                include_end: true,
            },
            db.buffer_pool.clone(),
            Arc::new(context),
        );
        index_scan_after.init()?;
        assert!(index_scan_after.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_delete_not_initialized() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let mut executor = DeleteExecutor::new("test".to_string(), None, context);

        // Try to call next() without init()
        let result = executor.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_delete_multiple_calls() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns("test_table", vec![("id", DataType::Int32)])?;

        // Insert one row
        let table_info = db.catalog.get_table("test_table")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );
        heap.insert_values(&vec![Value::Int32(1)], &[DataType::Int32])?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create delete executor
        let mut executor = DeleteExecutor::new("test_table".to_string(), None, context);
        executor.init()?;

        // First call returns count
        let result1 = executor.next()?;
        assert!(result1.is_some());

        // Second call returns None (operation already completed)
        let result2 = executor.next()?;
        assert!(result2.is_none());

        // Third call also returns None
        let result3 = executor.next()?;
        assert!(result3.is_none());

        Ok(())
    }
}
