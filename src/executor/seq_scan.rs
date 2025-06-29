//! Sequential scan executor implementation.

#[cfg(test)]
use crate::access::TableHeap;
use crate::access::{DataType, TableScanner, Tuple};
use crate::executor::{ColumnInfo, ExecutionContext, Executor};
use anyhow::{Result, bail};

/// Executor for sequential table scans
pub struct SeqScanExecutor {
    table_name: String,
    context: ExecutionContext,
    scanner: Option<TableScanner>,
    output_schema: Vec<ColumnInfo>,
    initialized: bool,
}

impl SeqScanExecutor {
    /// Create a new sequential scan executor
    pub fn new(table_name: String, context: ExecutionContext) -> Self {
        Self {
            table_name,
            context,
            scanner: None,
            output_schema: Vec::new(),
            initialized: false,
        }
    }
}

impl Executor for SeqScanExecutor {
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

        // Check if this is a system table
        let is_system_table = crate::catalog::is_system_table(&self.table_name);

        let schema = if is_system_table {
            // For system tables, get the predefined schema
            let schema = crate::catalog::get_system_table_schema(&self.table_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown system table: {}", self.table_name))?;

            // Build output schema based on system table type
            self.output_schema = match self.table_name.as_str() {
                crate::catalog::CATALOG_TABLE_NAME => vec![
                    ColumnInfo::new("table_id", DataType::Int32),
                    ColumnInfo::new("table_name", DataType::Varchar),
                    ColumnInfo::new("first_page_id", DataType::Int32),
                ],
                crate::catalog::CATALOG_ATTR_TABLE_NAME => vec![
                    ColumnInfo::new("table_id", DataType::Int32),
                    ColumnInfo::new("column_name", DataType::Varchar),
                    ColumnInfo::new("column_type", DataType::Int32),
                    ColumnInfo::new("column_order", DataType::Int32),
                ],
                _ => unreachable!(),
            };

            schema
        } else {
            // For user tables, get schema from catalog
            let columns = self
                .context
                .catalog
                .get_table_columns(table_info.table_id)?;

            // Build output schema
            self.output_schema = columns
                .iter()
                .map(|col| ColumnInfo::new(&col.column_name, col.column_type))
                .collect();

            // Extract just the data types for the scanner
            columns.into_iter().map(|col| col.column_type).collect()
        };

        // Create scanner
        self.scanner = Some(TableScanner::new_with_name(
            (*self.context.buffer_pool).clone(),
            Some(table_info.first_page_id),
            schema,
            self.table_name.clone(),
        ));

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        let scanner = self
            .scanner
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Scanner not available"))?;

        match scanner.next() {
            Some(Ok((tuple_id, values))) => {
                // Serialize values for Tuple
                // Note: In a real implementation, we might want to avoid this serialization
                // and work directly with values, but for now we maintain compatibility
                // with the existing Tuple structure
                let schema: Vec<_> = self.output_schema.iter().map(|col| col.data_type).collect();
                let data = crate::access::serialize_values(&values, &schema)?;
                Ok(Some(Tuple::new(tuple_id, data)))
            }
            Some(Err(e)) => Err(e),
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
    use crate::access::{DataType, Value};
    use crate::database::Database;
    use tempfile::tempdir;

    #[test]
    fn test_seq_scan_empty_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create an empty table
        db.create_table_with_columns(
            "test_table",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create and initialize executor
        let mut executor = SeqScanExecutor::new("test_table".to_string(), context);
        executor.init()?;

        // Verify schema
        let schema = executor.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[0].data_type, DataType::Int32);
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[1].data_type, DataType::Varchar);

        // Should return no tuples
        assert!(executor.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_seq_scan_with_data() -> Result<()> {
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

        // Get table info and insert data
        let table_info = db.catalog.get_table("users")?.unwrap();
        let mut heap = TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];

        // Insert test data
        let values1 = vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::Boolean(true),
        ];
        let values2 = vec![
            Value::Int32(2),
            Value::String("Bob".to_string()),
            Value::Boolean(false),
        ];

        heap.insert_values(&values1, &schema)?;
        heap.insert_values(&values2, &schema)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Create and run scan
        let mut executor = SeqScanExecutor::new("users".to_string(), context);
        executor.init()?;

        // Get first tuple
        let tuple1 = executor.next()?.expect("Should have first tuple");
        let values1_back = crate::access::deserialize_values(&tuple1.data, &schema)?;
        assert_eq!(values1_back[0], Value::Int32(1));
        assert_eq!(values1_back[1], Value::String("Alice".to_string()));
        assert_eq!(values1_back[2], Value::Boolean(true));

        // Get second tuple
        let tuple2 = executor.next()?.expect("Should have second tuple");
        let values2_back = crate::access::deserialize_values(&tuple2.data, &schema)?;
        assert_eq!(values2_back[0], Value::Int32(2));
        assert_eq!(values2_back[1], Value::String("Bob".to_string()));
        assert_eq!(values2_back[2], Value::Boolean(false));

        // No more tuples
        assert!(executor.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_seq_scan_nonexistent_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let mut executor = SeqScanExecutor::new("nonexistent".to_string(), context);
        let result = executor.init();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_seq_scan_not_initialized() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = Database::create(&db_path)?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        let mut executor = SeqScanExecutor::new("test".to_string(), context);

        // Try to call next() without init()
        let result = executor.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_seq_scan_system_tables() -> Result<()> {
        // System tables now use SystemSeqScanExecutor
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create some user tables to make the test more interesting
        db.create_table("users")?;
        db.create_table_with_columns(
            "products",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // System tables now work with regular SeqScanExecutor

        // Scan pg_tables
        let mut executor = SeqScanExecutor::new("pg_tables".to_string(), context.clone());
        executor.init()?;

        // Should have at least 2 system tables + 2 user tables
        let mut count = 0;
        while let Some(_) = executor.next()? {
            count += 1;
        }
        assert!(count >= 4);

        // Scan pg_attribute
        let mut executor2 = SeqScanExecutor::new("pg_attribute".to_string(), context);
        executor2.init()?;

        // Should have multiple attribute entries
        let mut attr_count = 0;
        while let Some(_) = executor2.next()? {
            attr_count += 1;
        }
        assert!(attr_count > 0);

        Ok(())
    }
}
