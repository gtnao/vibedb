//! Column information and metadata structures.

use crate::access::{DataType, Value};
use crate::catalog::table_info::TableId;
use anyhow::{Result, bail};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub column_name: String,
    pub column_type: DataType,
    pub column_order: u32,
}

impl ColumnInfo {
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(self.column_name.len() as u32).to_le_bytes());
        data.extend_from_slice(self.column_name.as_bytes());
        data.push(self.column_type as u8);
        data.extend_from_slice(&self.column_order.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 9 {
            bail!("Invalid column info data: too short");
        }

        let mut offset = 0;

        // Read column_name length
        let name_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // Read column_name
        if data.len() < offset + name_len + 5 {
            bail!("Invalid column info data: name too long");
        }
        let column_name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Read column_type
        let column_type = DataType::from_u8(data[offset])?;
        offset += 1;

        // Read column_order
        let column_order = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        Ok(ColumnInfo {
            column_name,
            column_type,
            column_order,
        })
    }
}

/// Represents a pg_attribute row
pub struct AttributeRow {
    pub table_id: TableId,
    pub column_info: ColumnInfo,
}

impl AttributeRow {
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.table_id.0.to_le_bytes());
        data.extend_from_slice(&self.column_info.serialize());
        data
    }

    /// Convert to Values for scanning
    pub fn to_values(&self) -> Vec<Value> {
        vec![
            Value::Int32(self.table_id.0 as i32),
            Value::String(self.column_info.column_name.clone()),
            Value::Int32(self.column_info.column_type as u8 as i32),
            Value::Int32(self.column_info.column_order as i32),
        ]
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            bail!("Invalid attribute row data: too short");
        }

        let table_id = TableId(u32::from_le_bytes([data[0], data[1], data[2], data[3]]));

        let column_info = ColumnInfo::deserialize(&data[4..])?;

        Ok(AttributeRow {
            table_id,
            column_info,
        })
    }
}
