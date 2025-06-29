use anyhow::{Result, bail};

/// Data types supported by the database
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Boolean = 1,
    Int32 = 2,
    Varchar = 4,
}

impl DataType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(DataType::Boolean),
            2 => Ok(DataType::Int32),
            4 => Ok(DataType::Varchar),
            _ => bail!("Unknown data type: {}", value),
        }
    }
}

/// Values that can be stored in the database
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    String(String),
}

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Int32(_) => Some(DataType::Int32),
            Value::String(_) => Some(DataType::Varchar),
        }
    }

    /// Check if this value is compatible with the given data type
    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Null, _) => true, // NULL is compatible with any type
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Int32(_), DataType::Int32) => true,
            (Value::String(_), DataType::Varchar) => true,
            _ => false,
        }
    }
}

/// Serialize values according to schema
pub fn serialize_values(values: &[Value], schema: &[DataType]) -> Result<Vec<u8>> {
    if values.len() != schema.len() {
        bail!(
            "Value count {} doesn't match schema length {}",
            values.len(),
            schema.len()
        );
    }

    let mut data = Vec::new();

    // NULL bitmap (1 bit per column, rounded up to bytes)
    let null_bitmap_size = schema.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; null_bitmap_size];

    // First pass: build NULL bitmap and validate types
    for (i, (value, expected_type)) in values.iter().zip(schema.iter()).enumerate() {
        match value {
            Value::Null => {
                // Set bit in NULL bitmap
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                null_bitmap[byte_idx] |= 1 << bit_idx;
            }
            _ => {
                // Validate type compatibility
                if !value.is_compatible_with(*expected_type) {
                    bail!(
                        "Value {:?} is not compatible with type {:?}",
                        value,
                        expected_type
                    );
                }
            }
        }
    }

    // Write NULL bitmap
    data.extend_from_slice(&null_bitmap);

    // Second pass: write non-NULL values
    for (value, data_type) in values.iter().zip(schema.iter()) {
        match (value, data_type) {
            (Value::Null, _) => {
                // Skip NULL values - already in bitmap
            }
            (Value::Boolean(b), DataType::Boolean) => {
                data.push(if *b { 1 } else { 0 });
            }
            (Value::Int32(i), DataType::Int32) => {
                data.extend_from_slice(&i.to_le_bytes());
            }
            (Value::String(s), DataType::Varchar) => {
                // Variable length: store length then data
                let bytes = s.as_bytes();
                data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(bytes);
            }
            _ => unreachable!("Type compatibility already checked"),
        }
    }

    Ok(data)
}

/// Deserialize bytes according to schema
pub fn deserialize_values(data: &[u8], schema: &[DataType]) -> Result<Vec<Value>> {
    if schema.is_empty() {
        return Ok(vec![]);
    }

    let null_bitmap_size = schema.len().div_ceil(8);
    if data.len() < null_bitmap_size {
        bail!("Data too short for NULL bitmap");
    }

    let mut offset = 0;
    let null_bitmap = &data[offset..offset + null_bitmap_size];
    offset += null_bitmap_size;

    let mut values = Vec::with_capacity(schema.len());

    for (i, data_type) in schema.iter().enumerate() {
        // Check NULL bitmap
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

        if is_null {
            values.push(Value::Null);
        } else {
            match data_type {
                DataType::Boolean => {
                    if offset >= data.len() {
                        bail!("Invalid boolean value: no data");
                    }
                    values.push(Value::Boolean(data[offset] != 0));
                    offset += 1;
                }
                DataType::Int32 => {
                    if offset + 4 > data.len() {
                        bail!("Invalid int32 value: not enough data");
                    }
                    let value = i32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    values.push(Value::Int32(value));
                    offset += 4;
                }
                DataType::Varchar => {
                    if offset + 4 > data.len() {
                        bail!("Invalid string value: no length");
                    }
                    let len = u32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]) as usize;
                    offset += 4;

                    if offset + len > data.len() {
                        bail!("Invalid string value: string too long");
                    }
                    let s = String::from_utf8(data[offset..offset + len].to_vec())?;
                    values.push(Value::String(s));
                    offset += len;
                }
            }
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_conversion() -> Result<()> {
        assert_eq!(DataType::from_u8(1)?, DataType::Boolean);
        assert_eq!(DataType::from_u8(2)?, DataType::Int32);
        assert_eq!(DataType::from_u8(4)?, DataType::Varchar);
        assert!(DataType::from_u8(99).is_err());
        Ok(())
    }

    #[test]
    fn test_value_compatibility() {
        assert!(Value::Null.is_compatible_with(DataType::Int32));
        assert!(Value::Boolean(true).is_compatible_with(DataType::Boolean));
        assert!(Value::Int32(42).is_compatible_with(DataType::Int32));
        assert!(Value::String("hello".to_string()).is_compatible_with(DataType::Varchar));

        assert!(!Value::Boolean(true).is_compatible_with(DataType::Int32));
        assert!(!Value::Int32(42).is_compatible_with(DataType::Varchar));
    }

    #[test]
    fn test_serialize_deserialize_values() -> Result<()> {
        let values = vec![
            Value::Int32(42),
            Value::String("Hello".to_string()),
            Value::Boolean(true),
            Value::Null,
        ];

        let schema = vec![
            DataType::Int32,
            DataType::Varchar,
            DataType::Boolean,
            DataType::Varchar, // NULL is allowed for any type
        ];

        let serialized = serialize_values(&values, &schema)?;
        let deserialized = deserialize_values(&serialized, &schema)?;

        assert_eq!(values, deserialized);
        Ok(())
    }

    #[test]
    fn test_empty_values() -> Result<()> {
        let values: Vec<Value> = vec![];
        let schema: Vec<DataType> = vec![];
        let serialized = serialize_values(&values, &schema)?;
        let deserialized = deserialize_values(&serialized, &schema)?;
        assert_eq!(values, deserialized);
        Ok(())
    }

    #[test]
    fn test_long_string() -> Result<()> {
        let long_string = "x".repeat(1000);
        let values = vec![Value::String(long_string.clone())];
        let schema = vec![DataType::Varchar];

        let serialized = serialize_values(&values, &schema)?;
        let deserialized = deserialize_values(&serialized, &schema)?;

        assert_eq!(values, deserialized);
        Ok(())
    }

    #[test]
    fn test_null_bitmap() -> Result<()> {
        let values = vec![
            Value::Int32(1),
            Value::Null,
            Value::String("test".to_string()),
            Value::Null,
            Value::Boolean(true),
        ];

        let schema = vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Varchar,
            DataType::Boolean,
            DataType::Boolean,
        ];

        let serialized = serialize_values(&values, &schema)?;
        // NULL bitmap should be 1 byte: 0b00001010 = 10
        assert_eq!(serialized[0], 0b00001010);

        let deserialized = deserialize_values(&serialized, &schema)?;
        assert_eq!(values, deserialized);
        Ok(())
    }
}
