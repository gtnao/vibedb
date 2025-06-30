//! System table definitions and utilities.

use crate::access::{DataType, Value};
use crate::catalog::column_info::AttributeRow;
use crate::catalog::table_info::{TableId, TableInfo};
use crate::storage::page::PageId;
use anyhow::Result;

pub const CATALOG_TABLE_ID: TableId = TableId(1);
pub const CATALOG_FIRST_PAGE: PageId = PageId(0);
pub const CATALOG_TABLE_NAME: &str = "pg_tables";

pub const CATALOG_ATTR_TABLE_ID: TableId = TableId(2);
pub const CATALOG_ATTR_TABLE_NAME: &str = "pg_attribute";

pub const CATALOG_INDEX_TABLE_ID: TableId = TableId(3);
pub const CATALOG_INDEX_TABLE_NAME: &str = "pg_index";

/// Deserializer for pg_tables
pub fn deserialize_pg_tables(data: &[u8]) -> Result<Vec<Value>> {
    let table_info = TableInfo::deserialize(data)?;
    Ok(table_info.to_values())
}

/// Deserializer for pg_attribute  
pub fn deserialize_pg_attribute(data: &[u8]) -> Result<Vec<Value>> {
    let attr_row = AttributeRow::deserialize(data)?;
    Ok(attr_row.to_values())
}

/// Get schema for pg_tables
pub fn pg_tables_schema() -> Vec<DataType> {
    vec![DataType::Int32, DataType::Varchar, DataType::Int32]
}

/// Get column names for pg_tables
pub fn pg_tables_column_names() -> Vec<String> {
    vec![
        "table_id".to_string(),
        "table_name".to_string(),
        "first_page_id".to_string(),
    ]
}

/// Get schema for pg_attribute
pub fn pg_attribute_schema() -> Vec<DataType> {
    vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
    ]
}

/// Get column names for pg_attribute
pub fn pg_attribute_column_names() -> Vec<String> {
    vec![
        "table_id".to_string(),
        "column_name".to_string(),
        "column_type".to_string(),
        "column_order".to_string(),
    ]
}

/// Get schema for pg_index
pub fn pg_index_schema() -> Vec<DataType> {
    vec![
        DataType::Int32,   // index_id
        DataType::Varchar, // index_name
        DataType::Int32,   // table_id
        DataType::Varchar, // key_column_names (comma-separated)
        DataType::Int32,   // root_page_id (nullable)
        DataType::Boolean, // is_unique
    ]
}

/// Get column names for pg_index
pub fn pg_index_column_names() -> Vec<String> {
    vec![
        "index_id".to_string(),
        "index_name".to_string(),
        "table_id".to_string(),
        "key_column_names".to_string(),
        "root_page_id".to_string(),
        "is_unique".to_string(),
    ]
}
