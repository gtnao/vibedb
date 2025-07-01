//! Update executor implementation.

use crate::access::{btree::BTree, DataType, TableHeap, TableScanner, Tuple, TupleId, Value};
use crate::executor::{ColumnInfo, ExecutionContext, Executor};
use anyhow::{bail, Result};
use std::collections::HashMap;

/// Update expression representing a column assignment
#[derive(Debug, Clone)]
pub struct UpdateExpression {
    pub column_name: String,
    pub value: Value,
}

impl UpdateExpression {
    pub fn new(column_name: impl Into<String>, value: Value) -> Self {
        Self {
            column_name: column_name.into(),
            value,
        }
    }
}

/// Executor for updating rows in a table
pub struct UpdateExecutor {
    table_name: String,
    update_expressions: Vec<UpdateExpression>,
    child: Option<Box<dyn Executor>>,
    context: ExecutionContext,
    heap: Option<TableHeap>,
    schema: Vec<DataType>,
    column_names: Vec<String>,
    output_schema: Vec<ColumnInfo>,
    updated_count: Option<i32>,
    initialized: bool,
}

impl UpdateExecutor {
    /// Create a new update executor
    pub fn new(
        table_name: String,
        update_expressions: Vec<UpdateExpression>,
        child: Option<Box<dyn Executor>>,
        context: ExecutionContext,
    ) -> Self {
        // Output schema for UPDATE is typically the number of rows affected
        let output_schema = vec![ColumnInfo::new("count", DataType::Int32)];

        Self {
            table_name,
            update_expressions,
            child,
            context,
            heap: None,
            schema: Vec::new(),
            column_names: Vec::new(),
            output_schema,
            updated_count: None,
            initialized: false,
        }
    }

    /// Update all tuples in the table
    fn update_all(&mut self) -> Result<i32> {
        let table_info = self
            .context
            .catalog
            .get_table(&self.table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", self.table_name))?;

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

        let mut update_count = 0;
        let mut tuples_to_update = Vec::new();

        // First, collect all tuples to update
        for result in scanner {
            let (tuple_id, values) = result?;
            tuples_to_update.push((tuple_id, values));
        }

        // Apply updates to all tuples
        let mut updates_to_apply = Vec::new();
        for (tuple_id, old_values) in &tuples_to_update {
            // Create new values by applying update expressions
            let new_values = self.apply_updates(old_values)?;
            updates_to_apply.push((*tuple_id, old_values.clone(), new_values));
        }

        // Get heap reference
        let heap = self
            .heap
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Heap not available"))?;

        // Now update them
        for (tuple_id, old_values, new_values) in updates_to_apply {
            // Update indexes: delete old entries, insert new ones
            for index_info in &indexes {
                // Extract old key values for this index
                let mut old_key_values = Vec::new();
                let mut new_key_values = Vec::new();

                for key_col in &index_info.key_columns {
                    if let Some(pos) = self
                        .column_names
                        .iter()
                        .position(|name| name == &key_col.column_name)
                    {
                        old_key_values.push(old_values[pos].clone());
                        new_key_values.push(new_values[pos].clone());
                    } else {
                        bail!("Column {} not found in table", key_col.column_name);
                    }
                }

                // Open the B+Tree index
                let mut btree = BTree::open(
                    (*self.context.buffer_pool).clone(),
                    index_info.root_page_id,
                    index_info.key_columns.clone(),
                )?;

                // Delete old entry
                btree.delete(&old_key_values, tuple_id)?;

                // Insert new entry (we'll use the same tuple_id since we're updating in-place)
                btree.insert(&new_key_values, tuple_id)?;

                // Update the root page id if it changed
                let new_root = btree.root_page_id();
                if new_root != index_info.root_page_id {
                    eprintln!(
                        "Warning: Index {} root page changed from {:?} to {:?}",
                        index_info.index_name, index_info.root_page_id, new_root
                    );
                }
            }

            // Update tuple in heap
            let data = crate::access::serialize_values(&new_values, &self.schema)?;
            let new_tuple_id = heap.update(tuple_id, &data)?;

            // If the tuple_id changed (due to delete + insert), we need to update indexes
            if new_tuple_id != tuple_id {
                // Re-update indexes with the new tuple_id
                for index_info in &indexes {
                    // Extract new key values for this index
                    let mut new_key_values = Vec::new();

                    for key_col in &index_info.key_columns {
                        if let Some(pos) = self
                            .column_names
                            .iter()
                            .position(|name| name == &key_col.column_name)
                        {
                            new_key_values.push(new_values[pos].clone());
                        } else {
                            bail!("Column {} not found in table", key_col.column_name);
                        }
                    }

                    // Open the B+Tree index
                    let mut btree = BTree::open(
                        (*self.context.buffer_pool).clone(),
                        index_info.root_page_id,
                        index_info.key_columns.clone(),
                    )?;

                    // Delete the entry with old tuple_id
                    btree.delete(&new_key_values, tuple_id)?;

                    // Insert the entry with new tuple_id
                    btree.insert(&new_key_values, new_tuple_id)?;
                }
            }

            update_count += 1;
        }

        Ok(update_count)
    }

    /// Update tuples matching the child executor's output
    fn update_with_condition(&mut self) -> Result<i32> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Child executor not available"))?;

        // Initialize child executor
        child.init()?;

        // Get indexes for this table
        let indexes = self
            .context
            .catalog
            .get_table_indexes_by_name(&self.table_name)?;

        let mut update_count = 0;
        let mut tuples_to_update = Vec::new();

        // Collect all tuples to update from child executor
        while let Some(tuple) = child.next()? {
            // Extract tuple_id and values from the tuple
            let values = crate::access::deserialize_values(&tuple.data, &self.schema)?;
            tuples_to_update.push((tuple.tuple_id, values));
        }

        // Apply updates to all tuples
        let mut updates_to_apply = Vec::new();
        for (tuple_id, old_values) in &tuples_to_update {
            // Create new values by applying update expressions
            let new_values = self.apply_updates(old_values)?;
            updates_to_apply.push((*tuple_id, old_values.clone(), new_values));
        }

        // Get heap reference
        let heap = self
            .heap
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Heap not available"))?;

        // Now update them
        for (tuple_id, old_values, new_values) in updates_to_apply {
            // Update indexes: delete old entries, insert new ones
            for index_info in &indexes {
                // Extract old and new key values for this index
                let mut old_key_values = Vec::new();
                let mut new_key_values = Vec::new();

                for key_col in &index_info.key_columns {
                    if let Some(pos) = self
                        .column_names
                        .iter()
                        .position(|name| name == &key_col.column_name)
                    {
                        old_key_values.push(old_values[pos].clone());
                        new_key_values.push(new_values[pos].clone());
                    } else {
                        bail!("Column {} not found in table", key_col.column_name);
                    }
                }

                // Open the B+Tree index
                let mut btree = BTree::open(
                    (*self.context.buffer_pool).clone(),
                    index_info.root_page_id,
                    index_info.key_columns.clone(),
                )?;

                // Delete old entry
                btree.delete(&old_key_values, tuple_id)?;

                // Insert new entry
                btree.insert(&new_key_values, tuple_id)?;

                // Update the root page id if it changed
                let new_root = btree.root_page_id();
                if new_root != index_info.root_page_id {
                    eprintln!(
                        "Warning: Index {} root page changed from {:?} to {:?}",
                        index_info.index_name, index_info.root_page_id, new_root
                    );
                }
            }

            // Update tuple in heap
            let data = crate::access::serialize_values(&new_values, &self.schema)?;
            let new_tuple_id = heap.update(tuple_id, &data)?;

            // If the tuple_id changed (due to delete + insert), we need to update indexes
            if new_tuple_id != tuple_id {
                // Re-update indexes with the new tuple_id
                for index_info in &indexes {
                    // Extract new key values for this index
                    let mut new_key_values = Vec::new();

                    for key_col in &index_info.key_columns {
                        if let Some(pos) = self
                            .column_names
                            .iter()
                            .position(|name| name == &key_col.column_name)
                        {
                            new_key_values.push(new_values[pos].clone());
                        } else {
                            bail!("Column {} not found in table", key_col.column_name);
                        }
                    }

                    // Open the B+Tree index
                    let mut btree = BTree::open(
                        (*self.context.buffer_pool).clone(),
                        index_info.root_page_id,
                        index_info.key_columns.clone(),
                    )?;

                    // Delete the entry with old tuple_id
                    btree.delete(&new_key_values, tuple_id)?;

                    // Insert the entry with new tuple_id
                    btree.insert(&new_key_values, new_tuple_id)?;
                }
            }

            update_count += 1;
        }

        Ok(update_count)
    }

    /// Apply update expressions to old values to create new values
    fn apply_updates(&self, old_values: &[Value]) -> Result<Vec<Value>> {
        // Create a map of column positions for quick lookup
        let mut column_positions = HashMap::new();
        for (i, name) in self.column_names.iter().enumerate() {
            column_positions.insert(name.as_str(), i);
        }

        // Start with a copy of old values
        let mut new_values = old_values.to_vec();

        // Apply each update expression
        for expr in &self.update_expressions {
            if let Some(&pos) = column_positions.get(expr.column_name.as_str()) {
                // Validate type compatibility
                if !expr.value.is_compatible_with(self.schema[pos]) {
                    bail!(
                        "Value {:?} is not compatible with column '{}' of type {:?}",
                        expr.value,
                        expr.column_name,
                        self.schema[pos]
                    );
                }
                new_values[pos] = expr.value.clone();
            } else {
                bail!("Column '{}' not found in table", expr.column_name);
            }
        }

        Ok(new_values)
    }
}

impl Executor for UpdateExecutor {
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

        // Extract schema and column names
        self.schema = columns.iter().map(|col| col.column_type).collect();
        self.column_names = table_info
            .column_names
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Table has no column names"))?;

        // Validate update expressions
        for expr in &self.update_expressions {
            if !self.column_names.contains(&expr.column_name) {
                bail!(
                    "Column '{}' not found in table '{}'",
                    expr.column_name,
                    self.table_name
                );
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

        // Update tuples only once
        if self.updated_count.is_none() {
            let count = if self.child.is_some() {
                self.update_with_condition()?
            } else {
                self.update_all()?
            };

            self.updated_count = Some(count);

            // Return the count as a tuple
            let count_value = vec![Value::Int32(count)];
            let count_schema = vec![DataType::Int32];
            let data = crate::access::serialize_values(&count_value, &count_schema)?;

            // For UPDATE, we don't have a meaningful TupleId for the result
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
    fn test_update_all_rows() -> Result<()> {
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

        // Create update executor to change all names to "Updated"
        let update_exprs = vec![UpdateExpression::new(
            "name",
            Value::String("Updated".to_string()),
        )];
        let mut executor = UpdateExecutor::new(
            "test_table".to_string(),
            update_exprs,
            None,
            context.clone(),
        );
        executor.init()?;

        // Execute update
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(3));

        // No more results
        assert!(executor.next()?.is_none());

        // Verify all rows were updated
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("test_table".to_string(), context);
        scan.init()?;

        let mut updated_count = 0;
        while let Some(tuple) = scan.next()? {
            let values = crate::access::deserialize_values(&tuple.data, &schema)?;
            assert_eq!(values[1], Value::String("Updated".to_string()));
            updated_count += 1;
        }
        assert_eq!(updated_count, 3);

        Ok(())
    }

    #[test]
    fn test_update_with_child_executor() -> Result<()> {
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

        // Create a child executor - using SeqScan for simplicity
        // In a real implementation, this would be a FilterExecutor
        let child_exec = Box::new(crate::executor::seq_scan::SeqScanExecutor::new(
            "users".to_string(),
            context.clone(),
        ));

        // Create update executor to set active = false
        let update_exprs = vec![UpdateExpression::new("active", Value::Boolean(false))];
        let mut executor = UpdateExecutor::new(
            "users".to_string(),
            update_exprs,
            Some(child_exec),
            context.clone(),
        );
        executor.init()?;

        // Execute update
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(3));

        // Verify all rows have active = false
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("users".to_string(), context);
        scan.init()?;

        while let Some(tuple) = scan.next()? {
            let values = crate::access::deserialize_values(&tuple.data, &schema)?;
            assert_eq!(values[2], Value::Boolean(false));
        }

        Ok(())
    }

    #[test]
    fn test_update_multiple_columns() -> Result<()> {
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
                ("active", DataType::Boolean),
            ],
        )?;

        // Insert a product
        let table_info = db.catalog.get_table("products")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![
            DataType::Int32,
            DataType::Varchar,
            DataType::Int32,
            DataType::Boolean,
        ];
        heap.insert_values(
            &vec![
                Value::Int32(1),
                Value::String("Laptop".to_string()),
                Value::Int32(1000),
                Value::Boolean(true),
            ],
            &schema,
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Update multiple columns
        let update_exprs = vec![
            UpdateExpression::new("name", Value::String("Gaming Laptop".to_string())),
            UpdateExpression::new("price", Value::Int32(1500)),
            UpdateExpression::new("active", Value::Boolean(false)),
        ];
        let mut executor =
            UpdateExecutor::new("products".to_string(), update_exprs, None, context.clone());
        executor.init()?;

        // Execute update
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(1));

        // Verify the update
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("products".to_string(), context);
        scan.init()?;

        let tuple = scan.next()?.expect("Should have one row");
        let values = crate::access::deserialize_values(&tuple.data, &schema)?;
        assert_eq!(values[0], Value::Int32(1)); // id unchanged
        assert_eq!(values[1], Value::String("Gaming Laptop".to_string()));
        assert_eq!(values[2], Value::Int32(1500));
        assert_eq!(values[3], Value::Boolean(false));

        Ok(())
    }

    #[test]
    fn test_update_empty_table() -> Result<()> {
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

        // Create update executor
        let update_exprs = vec![UpdateExpression::new(
            "name",
            Value::String("Updated".to_string()),
        )];
        let mut executor =
            UpdateExecutor::new("test_table".to_string(), update_exprs, None, context);
        executor.init()?;

        // Execute update
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(0));

        Ok(())
    }

    #[test]
    fn test_update_nonexistent_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let update_exprs = vec![UpdateExpression::new(
            "name",
            Value::String("test".to_string()),
        )];
        let mut executor =
            UpdateExecutor::new("nonexistent".to_string(), update_exprs, None, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_update_nonexistent_column() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns("test_table", vec![("id", DataType::Int32)])?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Try to update non-existent column
        let update_exprs = vec![UpdateExpression::new(
            "nonexistent",
            Value::String("test".to_string()),
        )];
        let mut executor =
            UpdateExecutor::new("test_table".to_string(), update_exprs, None, context);

        let result = executor.init();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_update_type_mismatch() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("active", DataType::Boolean)],
        )?;

        // Insert a row
        let table_info = db.catalog.get_table("test_table")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );
        heap.insert_values(
            &vec![Value::Int32(1), Value::Boolean(true)],
            &[DataType::Int32, DataType::Boolean],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Try to update with wrong type
        let update_exprs = vec![UpdateExpression::new(
            "active",
            Value::String("not_a_boolean".to_string()),
        )];
        let mut executor =
            UpdateExecutor::new("test_table".to_string(), update_exprs, None, context);
        executor.init()?;

        let result = executor.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not compatible"));

        Ok(())
    }

    #[test]
    fn test_update_with_nulls() -> Result<()> {
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

        // Insert data with non-null values
        let table_info = db.catalog.get_table("test_table")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Varchar];
        heap.insert_values(
            &vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::String("Some notes".to_string()),
            ],
            &schema,
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Update notes to NULL
        let update_exprs = vec![UpdateExpression::new("notes", Value::Null)];
        let mut executor = UpdateExecutor::new(
            "test_table".to_string(),
            update_exprs,
            None,
            context.clone(),
        );
        executor.init()?;

        // Execute update
        let result = executor.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(1));

        // Verify the update
        let mut scan =
            crate::executor::seq_scan::SeqScanExecutor::new("test_table".to_string(), context);
        scan.init()?;

        let tuple = scan.next()?.expect("Should have one row");
        let values = crate::access::deserialize_values(&tuple.data, &schema)?;
        assert_eq!(values[0], Value::Int32(1));
        assert_eq!(values[1], Value::String("Alice".to_string()));
        assert_eq!(values[2], Value::Null);

        Ok(())
    }

    #[test]
    fn test_update_with_index_update() -> Result<()> {
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

        // Insert data using InsertExecutor to ensure indexes are updated
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

        let mut insert = crate::executor::insert::InsertExecutor::new(
            "users".to_string(),
            values,
            ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone()),
        );
        insert.init()?;
        insert.next()?;

        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Update email addresses
        let update_exprs = vec![UpdateExpression::new(
            "email",
            Value::String("alice.updated@example.com".to_string()),
        )];

        // Create a mock child executor that returns only the first tuple
        // In a real implementation, this would be a FilterExecutor
        let mut mock_tuples = Vec::new();
        let table_info = db.catalog.get_table("users")?.unwrap();
        let _heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );
        let scanner = TableScanner::new(
            (*db.buffer_pool).clone(),
            Some(table_info.first_page_id),
            vec![DataType::Int32, DataType::Varchar, DataType::Int32],
            None,
        );
        for (i, result) in scanner.enumerate() {
            if i == 0 {
                let (tid, values) = result?;
                let data = crate::access::serialize_values(
                    &values,
                    &[DataType::Int32, DataType::Varchar, DataType::Int32],
                )?;
                mock_tuples.push(Tuple::new(tid, data));
                break;
            }
        }

        // Create a mock executor that returns only the first tuple
        struct MockExecutor {
            tuples: Vec<Tuple>,
            index: usize,
            schema: Vec<ColumnInfo>,
        }
        impl Executor for MockExecutor {
            fn init(&mut self) -> Result<()> {
                Ok(())
            }
            fn next(&mut self) -> Result<Option<Tuple>> {
                if self.index < self.tuples.len() {
                    let tuple = self.tuples[self.index].clone();
                    self.index += 1;
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            fn output_schema(&self) -> &[ColumnInfo] {
                &self.schema
            }
        }

        let child_exec = Box::new(MockExecutor {
            tuples: mock_tuples,
            index: 0,
            schema: vec![
                ColumnInfo::new("id", DataType::Int32),
                ColumnInfo::new("email", DataType::Varchar),
                ColumnInfo::new("age", DataType::Int32),
            ],
        });

        let mut update = UpdateExecutor::new(
            "users".to_string(),
            update_exprs,
            Some(child_exec),
            context.clone(),
        );
        update.init()?;

        // Execute update
        let result = update.next()?.expect("Should return count");
        let count_values = crate::access::deserialize_values(&result.data, &[DataType::Int32])?;
        assert_eq!(count_values[0], Value::Int32(1)); // Only updated one row

        // Verify index was updated by using IndexScanExecutor
        let index_info = db
            .catalog
            .get_table_indexes_by_name("users")?
            .into_iter()
            .find(|idx| idx.index_name == "idx_email")
            .unwrap();

        // Search for updated email
        let mut index_scan = crate::executor::index_scan::IndexScanExecutor::new(
            "users".to_string(),
            index_info.clone(),
            crate::executor::index_scan::IndexScanMode::Exact(vec![Value::String(
                "alice.updated@example.com".to_string(),
            )]),
            db.buffer_pool.clone(),
            Arc::new(context.clone()),
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
        assert_eq!(
            values[1],
            Value::String("alice.updated@example.com".to_string())
        );

        // Verify old email is not in index
        let mut old_index_scan = crate::executor::index_scan::IndexScanExecutor::new(
            "users".to_string(),
            index_info,
            crate::executor::index_scan::IndexScanMode::Exact(vec![Value::String(
                "alice@example.com".to_string(),
            )]),
            db.buffer_pool.clone(),
            Arc::new(context),
        );

        old_index_scan.init()?;
        assert!(old_index_scan.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_update_not_initialized() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let update_exprs = vec![UpdateExpression::new("test", Value::Int32(1))];
        let mut executor = UpdateExecutor::new("test".to_string(), update_exprs, None, context);

        // Try to call next() without init()
        let result = executor.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_update_multiple_calls() -> Result<()> {
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

        // Create update executor
        let update_exprs = vec![UpdateExpression::new("id", Value::Int32(2))];
        let mut executor =
            UpdateExecutor::new("test_table".to_string(), update_exprs, None, context);
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
