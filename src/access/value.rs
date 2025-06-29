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

/// Serialize a vector of values into bytes
pub fn serialize_values(values: &[Value]) -> Result<Vec<u8>> {
    let mut data = Vec::new();

    // Write number of values
    data.extend_from_slice(&(values.len() as u32).to_le_bytes());

    for value in values {
        match value {
            Value::Null => {
                data.push(0); // Type tag for NULL
            }
            Value::Boolean(b) => {
                data.push(1); // Type tag for Boolean
                data.push(if *b { 1 } else { 0 });
            }
            Value::Int32(i) => {
                data.push(2); // Type tag for Int32
                data.extend_from_slice(&i.to_le_bytes());
            }
            Value::String(s) => {
                data.push(4); // Type tag for String
                let bytes = s.as_bytes();
                data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(bytes);
            }
        }
    }

    Ok(data)
}

/// Deserialize bytes into a vector of values
pub fn deserialize_values(data: &[u8]) -> Result<Vec<Value>> {
    if data.len() < 4 {
        bail!("Invalid value data: too short");
    }

    let mut offset = 0;
    let count = u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]) as usize;
    offset += 4;

    let mut values = Vec::with_capacity(count);

    for _ in 0..count {
        if offset >= data.len() {
            bail!("Invalid value data: unexpected end");
        }

        let type_tag = data[offset];
        offset += 1;

        match type_tag {
            0 => {
                // NULL
                values.push(Value::Null);
            }
            1 => {
                // Boolean
                if offset >= data.len() {
                    bail!("Invalid boolean value: no data");
                }
                values.push(Value::Boolean(data[offset] != 0));
                offset += 1;
            }
            2 => {
                // Int32
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
            4 => {
                // String
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
            _ => bail!("Unknown value type tag: {}", type_tag),
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

        let serialized = serialize_values(&values)?;
        let deserialized = deserialize_values(&serialized)?;

        assert_eq!(values, deserialized);
        Ok(())
    }

    #[test]
    fn test_empty_values() -> Result<()> {
        let values: Vec<Value> = vec![];
        let serialized = serialize_values(&values)?;
        let deserialized = deserialize_values(&serialized)?;
        assert_eq!(values, deserialized);
        Ok(())
    }

    #[test]
    fn test_long_string() -> Result<()> {
        let long_string = "x".repeat(1000);
        let values = vec![Value::String(long_string.clone())];

        let serialized = serialize_values(&values)?;
        let deserialized = deserialize_values(&serialized)?;

        assert_eq!(values, deserialized);
        Ok(())
    }
}
