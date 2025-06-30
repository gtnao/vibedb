//! Table information and metadata structures.

use crate::access::{DataType, Value};
use crate::storage::page::PageId;
use anyhow::{Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(pub u32);

/// Type alias for custom deserializer function
pub type CustomDeserializer = fn(&[u8]) -> Result<Vec<Value>>;

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: TableId,
    pub table_name: String,
    pub first_page_id: PageId,
    /// Schema for this table (None means it should be loaded from pg_attribute)
    pub schema: Option<Vec<DataType>>,
    /// Column names for this table (None means it should be loaded from pg_attribute)
    pub column_names: Option<Vec<String>>,
    /// Custom deserializer for this table (None means use standard schema-based deserialization)
    pub custom_deserializer: Option<CustomDeserializer>,
}

impl TableInfo {
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.table_id.0.to_le_bytes());
        data.extend_from_slice(&(self.table_name.len() as u32).to_le_bytes());
        data.extend_from_slice(self.table_name.as_bytes());
        data.extend_from_slice(&self.first_page_id.0.to_le_bytes());
        data
    }

    /// Convert to Values for scanning
    pub fn to_values(&self) -> Vec<Value> {
        vec![
            Value::Int32(self.table_id.0 as i32),
            Value::String(self.table_name.clone()),
            Value::Int32(self.first_page_id.0 as i32),
        ]
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            bail!("Invalid table info data: too short");
        }

        let mut offset = 0;

        // Read table_id
        let table_id = TableId(u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]));
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
        let first_page_id = PageId(u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]));

        Ok(TableInfo {
            table_id,
            table_name,
            first_page_id,
            schema: None,
            column_names: None,
            custom_deserializer: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_info_serialization() -> Result<()> {
        let info = TableInfo {
            table_id: TableId(42),
            table_name: "test_table".to_string(),
            first_page_id: PageId(123),
            schema: None,
            column_names: None,
            custom_deserializer: None,
        };

        let serialized = info.serialize();
        let deserialized = TableInfo::deserialize(&serialized)?;

        assert_eq!(info.table_id, deserialized.table_id);
        assert_eq!(info.table_name, deserialized.table_name);
        assert_eq!(info.first_page_id, deserialized.first_page_id);

        Ok(())
    }
}
