//! System table scan executor implementation.
//!
//! System tables (pg_tables, pg_attribute) use custom serialization
//! formats that differ from regular user tables. This module provides
//! specialized scanning for these tables.

use crate::access::{DataType, Tuple, TupleId, Value};
use crate::catalog::{CATALOG_ATTR_TABLE_NAME, CATALOG_TABLE_NAME};
use crate::executor::{ColumnInfo, ExecutionContext, Executor};
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::{HeapPage, PageId};
use anyhow::{Result, bail};
use std::sync::Arc;

/// Scanner for system tables with custom deserialization
pub struct SystemTableScanner {
    buffer_pool: Arc<BufferPoolManager>,
    table_name: String,
    current_page_id: Option<PageId>,
    current_slot: u16,
}

impl SystemTableScanner {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>,
        table_name: String,
        first_page_id: Option<PageId>,
    ) -> Self {
        Self {
            buffer_pool,
            table_name,
            current_page_id: first_page_id,
            current_slot: 0,
        }
    }

    fn deserialize_pg_tables(&self, data: &[u8]) -> Result<Vec<Value>> {
        // TableInfo::deserialize logic
        if data.len() < 8 {
            bail!("Invalid table info data: too short");
        }

        let mut offset = 0;

        // Read table_id
        let table_id = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Read table_name length
        let name_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // Read table_name
        if data.len() < offset + name_len + 4 {
            bail!("Invalid table info data: name too long");
        }
        let table_name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Read first_page_id
        let first_page_id = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        Ok(vec![
            Value::Int32(table_id as i32),
            Value::String(table_name),
            Value::Int32(first_page_id as i32),
        ])
    }

    fn deserialize_pg_attribute(&self, data: &[u8]) -> Result<Vec<Value>> {
        // AttributeRow::deserialize logic
        if data.len() < 4 {
            bail!("Invalid attribute row data: too short");
        }

        let table_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        // ColumnInfo::deserialize from remaining data
        let column_data = &data[4..];
        if column_data.len() < 9 {
            bail!("Invalid column info data: too short");
        }

        let mut offset = 0;

        // Read column_name length
        let name_len = u32::from_le_bytes([
            column_data[offset],
            column_data[offset + 1],
            column_data[offset + 2],
            column_data[offset + 3],
        ]) as usize;
        offset += 4;

        // Read column_name
        if column_data.len() < offset + name_len + 5 {
            bail!("Invalid column info data: name too long");
        }
        let column_name = String::from_utf8(column_data[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Read column_type
        let column_type = column_data[offset];
        offset += 1;

        // Read column_order
        let column_order = u32::from_le_bytes([
            column_data[offset],
            column_data[offset + 1],
            column_data[offset + 2],
            column_data[offset + 3],
        ]);

        Ok(vec![
            Value::Int32(table_id as i32),
            Value::String(column_name),
            Value::Int32(column_type as i32),
            Value::Int32(column_order as i32),
        ])
    }

    fn try_next_tuple(&mut self) -> Result<Option<(TupleId, Vec<Value>)>> {
        let page_id = match self.current_page_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let guard = self.buffer_pool.fetch_page(page_id)?;

        // Create a temporary HeapPage view
        // SAFETY: Same as TableHeap::get() - guard ensures page stays in memory
        let page_data = unsafe {
            std::slice::from_raw_parts_mut(guard.as_ptr() as *mut u8, crate::storage::PAGE_SIZE)
        };
        let page_array =
            unsafe { &mut *(page_data.as_mut_ptr() as *mut [u8; crate::storage::PAGE_SIZE]) };
        let heap_page = HeapPage::from_data(page_array);

        let tuple_count = heap_page.get_tuple_count();

        // Try to find a valid tuple starting from current_slot
        while self.current_slot < tuple_count {
            let slot_id = self.current_slot;
            self.current_slot += 1;

            match heap_page.get_tuple(slot_id) {
                Ok(data) => {
                    // Deserialize based on table type
                    let values = match self.table_name.as_str() {
                        CATALOG_TABLE_NAME => self.deserialize_pg_tables(data)?,
                        CATALOG_ATTR_TABLE_NAME => self.deserialize_pg_attribute(data)?,
                        _ => bail!("Unknown system table: {}", self.table_name),
                    };
                    let tuple_id = TupleId::new(page_id, slot_id);
                    return Ok(Some((tuple_id, values)));
                }
                Err(_) => {
                    // Skip deleted or invalid tuples
                    continue;
                }
            }
        }

        // No more valid tuples on this page, try next page
        self.current_page_id = heap_page.get_next_page_id();
        self.current_slot = 0;

        // If we moved to a new page, recurse to try getting a tuple from it
        if self.current_page_id.is_some() {
            self.try_next_tuple()
        } else {
            Ok(None)
        }
    }
}

impl Iterator for SystemTableScanner {
    type Item = Result<(TupleId, Vec<Value>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next_tuple() {
            Ok(Some(tuple)) => Some(Ok(tuple)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Executor for system table sequential scans
pub struct SystemSeqScanExecutor {
    table_name: String,
    context: ExecutionContext,
    scanner: Option<SystemTableScanner>,
    output_schema: Vec<ColumnInfo>,
    initialized: bool,
}

impl SystemSeqScanExecutor {
    /// Create a new system table scan executor
    pub fn new(table_name: String, context: ExecutionContext) -> Self {
        Self {
            table_name,
            context,
            scanner: None,
            output_schema: Vec::new(),
            initialized: false,
        }
    }

    /// Check if this is a system table
    pub fn is_system_table(table_name: &str) -> bool {
        matches!(table_name, CATALOG_TABLE_NAME | CATALOG_ATTR_TABLE_NAME)
    }
}

impl Executor for SystemSeqScanExecutor {
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

        // Define output schema based on table type
        self.output_schema = match self.table_name.as_str() {
            CATALOG_TABLE_NAME => vec![
                ColumnInfo::new("table_id", DataType::Int32),
                ColumnInfo::new("table_name", DataType::Varchar),
                ColumnInfo::new("first_page_id", DataType::Int32),
            ],
            CATALOG_ATTR_TABLE_NAME => vec![
                ColumnInfo::new("table_id", DataType::Int32),
                ColumnInfo::new("column_name", DataType::Varchar),
                ColumnInfo::new("column_type", DataType::Int32),
                ColumnInfo::new("column_order", DataType::Int32),
            ],
            _ => bail!("Unknown system table: {}", self.table_name),
        };

        // Create scanner
        self.scanner = Some(SystemTableScanner::new(
            self.context.buffer_pool.clone(),
            self.table_name.clone(),
            Some(table_info.first_page_id),
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
    use crate::database::Database;
    use tempfile::tempdir;

    #[test]
    fn test_system_scan_pg_tables() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create some user tables
        db.create_table("users")?;
        db.create_table("products")?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Scan pg_tables
        let mut executor = SystemSeqScanExecutor::new(CATALOG_TABLE_NAME.to_string(), context);
        executor.init()?;

        // Verify schema
        let schema = executor.output_schema();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "table_id");
        assert_eq!(schema[1].name, "table_name");
        assert_eq!(schema[2].name, "first_page_id");

        // Collect all tables
        let mut tables = Vec::new();
        while let Some(tuple) = executor.next()? {
            let values = crate::access::deserialize_values(
                &tuple.data,
                &[DataType::Int32, DataType::Varchar, DataType::Int32],
            )?;
            if let Value::String(name) = &values[1] {
                tables.push(name.clone());
            }
        }

        // Should have system tables + user tables
        assert!(tables.contains(&CATALOG_TABLE_NAME.to_string()));
        assert!(tables.contains(&CATALOG_ATTR_TABLE_NAME.to_string()));
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"products".to_string()));

        Ok(())
    }

    #[test]
    fn test_system_scan_pg_attribute() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create a table with columns
        db.create_table_with_columns(
            "test_table",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("active", DataType::Boolean),
            ],
        )?;

        // Create context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Scan pg_attribute
        let mut executor = SystemSeqScanExecutor::new(CATALOG_ATTR_TABLE_NAME.to_string(), context);
        executor.init()?;

        // Verify schema
        let schema = executor.output_schema();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[0].name, "table_id");
        assert_eq!(schema[1].name, "column_name");
        assert_eq!(schema[2].name, "column_type");
        assert_eq!(schema[3].name, "column_order");

        // Count attributes
        let mut attr_count = 0;
        while let Some(_) = executor.next()? {
            attr_count += 1;
        }

        // Should have attributes for system tables + test_table
        assert!(attr_count >= 10); // At least system table columns + 3 test_table columns

        Ok(())
    }

    #[test]
    fn test_is_system_table() {
        assert!(SystemSeqScanExecutor::is_system_table("pg_tables"));
        assert!(SystemSeqScanExecutor::is_system_table("pg_attribute"));
        assert!(!SystemSeqScanExecutor::is_system_table("users"));
        assert!(!SystemSeqScanExecutor::is_system_table("products"));
    }
}
