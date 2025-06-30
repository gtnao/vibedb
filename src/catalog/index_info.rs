use crate::access::value::{DataType, Value};
use crate::catalog::{ColumnInfo, TableId};
use crate::storage::page::PageId;
use anyhow::Result;

/// Information about an index
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    /// Unique identifier for the index
    pub index_id: IndexId,
    /// Name of the index
    pub index_name: String,
    /// Table that this index belongs to
    pub table_id: TableId,
    /// Columns that make up the index key
    pub key_columns: Vec<ColumnInfo>,
    /// Root page of the B+Tree index
    pub root_page_id: Option<PageId>,
    /// Whether this is a unique index
    pub is_unique: bool,
}

/// Unique identifier for an index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexId(pub u32);

impl IndexInfo {
    /// Get the schema for the index_info table
    pub fn index_info_schema() -> Vec<DataType> {
        vec![
            DataType::Int32,   // index_id
            DataType::Varchar, // index_name
            DataType::Int32,   // table_id
            DataType::Varchar, // key_column_names (comma-separated)
            DataType::Int32,   // root_page_id (nullable)
            DataType::Boolean, // is_unique
        ]
    }

    /// Convert IndexInfo to a tuple for storage
    pub fn to_tuple(&self) -> Vec<Value> {
        vec![
            Value::Int32(self.index_id.0 as i32),
            Value::String(self.index_name.clone()),
            Value::Int32(self.table_id.0 as i32),
            Value::String(
                self.key_columns
                    .iter()
                    .map(|c| c.column_name.clone())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            self.root_page_id
                .map(|pid| Value::Int32(pid.0 as i32))
                .unwrap_or(Value::Null),
            Value::Boolean(self.is_unique),
        ]
    }

    /// Create IndexInfo from tuple values (requires column info for key columns)
    pub fn from_tuple_values(values: &[Value], table_columns: &[ColumnInfo]) -> Result<Self> {
        if values.len() < 6 {
            anyhow::bail!("Not enough values for IndexInfo");
        }

        let index_id = match &values[0] {
            Value::Int32(id) => IndexId(*id as u32),
            _ => anyhow::bail!("Invalid index_id type"),
        };

        let index_name = match &values[1] {
            Value::String(name) => name.clone(),
            _ => anyhow::bail!("Invalid index_name type"),
        };

        let table_id = match &values[2] {
            Value::Int32(id) => TableId(*id as u32),
            _ => anyhow::bail!("Invalid table_id type"),
        };

        let key_column_names = match &values[3] {
            Value::String(names) => names.split(',').map(|s| s.to_string()).collect::<Vec<_>>(),
            _ => anyhow::bail!("Invalid key_column_names type"),
        };

        // Find the column info for each key column
        let mut key_columns = Vec::new();
        for col_name in key_column_names {
            let col_info = table_columns
                .iter()
                .find(|c| c.column_name == col_name)
                .ok_or_else(|| anyhow::anyhow!("Column {} not found in table", col_name))?;
            key_columns.push(col_info.clone());
        }

        let root_page_id = match &values[4] {
            Value::Int32(id) => Some(PageId(*id as u32)),
            Value::Null => None,
            _ => anyhow::bail!("Invalid root_page_id type"),
        };

        let is_unique = match &values[5] {
            Value::Boolean(b) => *b,
            _ => anyhow::bail!("Invalid is_unique type"),
        };

        Ok(Self {
            index_id,
            index_name,
            table_id,
            key_columns,
            root_page_id,
            is_unique,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_index_info_schema() {
        let schema = IndexInfo::index_info_schema();
        assert_eq!(schema.len(), 6);
        assert_eq!(schema[0], DataType::Int32);
        assert_eq!(schema[1], DataType::Varchar);
        assert_eq!(schema[2], DataType::Int32);
        assert_eq!(schema[3], DataType::Varchar);
        assert_eq!(schema[4], DataType::Int32);
        assert_eq!(schema[5], DataType::Boolean);
    }

    #[test]
    fn test_index_info_serialization() {
        let index_info = IndexInfo {
            index_id: IndexId(1),
            index_name: "idx_users_email".to_string(),
            table_id: TableId(2),
            key_columns: vec![ColumnInfo {
                column_name: "email".to_string(),
                column_type: DataType::Varchar,
                column_order: 0,
            }],
            root_page_id: Some(PageId(10)),
            is_unique: true,
        };

        let tuple = index_info.to_tuple();
        assert_eq!(tuple.len(), 6);
        assert_eq!(tuple[0], Value::Int32(1));
        assert_eq!(tuple[1], Value::String("idx_users_email".to_string()));
        assert_eq!(tuple[2], Value::Int32(2));
        assert_eq!(tuple[3], Value::String("email".to_string()));
        assert_eq!(tuple[4], Value::Int32(10));
        assert_eq!(tuple[5], Value::Boolean(true));
    }
}
