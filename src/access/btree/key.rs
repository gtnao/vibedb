use crate::access::value::{DataType, Value, deserialize_values, serialize_values};
use anyhow::{Result, bail};
use std::cmp::Ordering;

/// Represents a key in the B+Tree
/// Handles proper comparison semantics for different data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BTreeKey {
    /// The serialized key data
    data: Vec<u8>,
    /// The schema of the key columns
    schema: Vec<DataType>,
}

impl BTreeKey {
    /// Create a new BTreeKey from values and schema
    pub fn from_values(values: &[Value], schema: &[DataType]) -> Result<Self> {
        if values.len() != schema.len() {
            bail!(
                "Value count {} doesn't match schema length {}",
                values.len(),
                schema.len()
            );
        }

        let data = serialize_values(values, schema)?;
        Ok(Self {
            data,
            schema: schema.to_vec(),
        })
    }

    /// Get the serialized key data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the schema
    pub fn schema(&self) -> &[DataType] {
        &self.schema
    }

    /// Deserialize the key back to values
    pub fn to_values(&self) -> Result<Vec<Value>> {
        deserialize_values(&self.data, &self.schema)
    }

    /// Create a BTreeKey from raw serialized data
    pub fn from_data(data: Vec<u8>, schema: Vec<DataType>) -> Self {
        Self { data, schema }
    }

    /// Check if this key matches a prefix (for prefix scans)
    pub fn matches_prefix(&self, prefix: &BTreeKey) -> Result<bool> {
        let self_values = self.to_values()?;
        let prefix_values = prefix.to_values()?;

        if prefix_values.len() > self_values.len() {
            return Ok(false);
        }

        for i in 0..prefix_values.len() {
            if self_values[i] != prefix_values[i] {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Get the length of the serialized key
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the key is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // For proper comparison, we need the same schema
        if self.schema != other.schema {
            // If schemas don't match, fall back to byte comparison
            // This shouldn't happen in a well-formed B+Tree
            return self.data.cmp(&other.data);
        }

        // Deserialize and compare values according to their types
        match (self.to_values(), other.to_values()) {
            (Ok(self_values), Ok(other_values)) => {
                // Compare each value according to its type
                for (i, (v1, v2)) in self_values.iter().zip(other_values.iter()).enumerate() {
                    let ord = match (v1, v2) {
                        // NULL is always the minimum value
                        (Value::Null, Value::Null) => Ordering::Equal,
                        (Value::Null, _) => Ordering::Less,
                        (_, Value::Null) => Ordering::Greater,

                        // Same type comparisons
                        (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
                        (Value::Int32(i1), Value::Int32(i2)) => i1.cmp(i2),
                        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),

                        // Different types - this shouldn't happen with proper schema
                        _ => match (self_values[i].data_type(), other_values[i].data_type()) {
                            (Some(t1), Some(t2)) => (t1 as u8).cmp(&(t2 as u8)),
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Less,
                            (Some(_), None) => Ordering::Greater,
                        },
                    };

                    if ord != Ordering::Equal {
                        return ord;
                    }
                }

                // All values are equal
                Ordering::Equal
            }
            _ => {
                // If deserialization fails, fall back to byte comparison
                self.data.cmp(&other.data)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_key_from_values() {
        let values = vec![Value::Int32(42), Value::String("hello".to_string())];
        let schema = vec![DataType::Int32, DataType::Varchar];

        let key = BTreeKey::from_values(&values, &schema).unwrap();
        assert!(!key.is_empty());
        assert_eq!(key.schema(), &schema);

        let deserialized = key.to_values().unwrap();
        assert_eq!(deserialized, values);
    }

    #[test]
    fn test_btree_key_comparison_int32() {
        let schema = vec![DataType::Int32];

        let key1 = BTreeKey::from_values(&[Value::Int32(1)], &schema).unwrap();
        let key256 = BTreeKey::from_values(&[Value::Int32(256)], &schema).unwrap();
        let key512 = BTreeKey::from_values(&[Value::Int32(512)], &schema).unwrap();

        // Verify correct integer ordering
        assert!(key1 < key256);
        assert!(key256 < key512);
        assert!(key1 < key512);

        // Test equality
        let key1_dup = BTreeKey::from_values(&[Value::Int32(1)], &schema).unwrap();
        assert_eq!(key1, key1_dup);
    }

    #[test]
    fn test_btree_key_comparison_string() {
        let schema = vec![DataType::Varchar];

        let key_a = BTreeKey::from_values(&[Value::String("apple".to_string())], &schema).unwrap();
        let key_b = BTreeKey::from_values(&[Value::String("banana".to_string())], &schema).unwrap();
        let key_c = BTreeKey::from_values(&[Value::String("cherry".to_string())], &schema).unwrap();

        assert!(key_a < key_b);
        assert!(key_b < key_c);
        assert!(key_a < key_c);
    }

    #[test]
    fn test_btree_key_null_handling() {
        let schema = vec![DataType::Int32];

        let key_null = BTreeKey::from_values(&[Value::Null], &schema).unwrap();
        let key_0 = BTreeKey::from_values(&[Value::Int32(0)], &schema).unwrap();
        let key_neg = BTreeKey::from_values(&[Value::Int32(-100)], &schema).unwrap();

        // NULL should be less than any value
        assert!(key_null < key_0);
        assert!(key_null < key_neg);
    }

    #[test]
    fn test_btree_key_composite() {
        let schema = vec![DataType::Varchar, DataType::Int32];

        let key1 =
            BTreeKey::from_values(&[Value::String("a".to_string()), Value::Int32(1)], &schema)
                .unwrap();

        let key2 =
            BTreeKey::from_values(&[Value::String("a".to_string()), Value::Int32(2)], &schema)
                .unwrap();

        let key3 =
            BTreeKey::from_values(&[Value::String("b".to_string()), Value::Int32(1)], &schema)
                .unwrap();

        // First column takes precedence
        assert!(key1 < key2); // Same string, different int
        assert!(key2 < key3); // Different string
    }

    #[test]
    fn test_btree_key_prefix_matching() {
        let schema = vec![DataType::Varchar, DataType::Int32, DataType::Boolean];

        let full_key = BTreeKey::from_values(
            &[
                Value::String("test".to_string()),
                Value::Int32(42),
                Value::Boolean(true),
            ],
            &schema,
        )
        .unwrap();

        // Exact prefix match
        let prefix1 =
            BTreeKey::from_values(&[Value::String("test".to_string())], &[DataType::Varchar])
                .unwrap();
        assert!(full_key.matches_prefix(&prefix1).unwrap());

        // Longer prefix match
        let prefix2 = BTreeKey::from_values(
            &[Value::String("test".to_string()), Value::Int32(42)],
            &[DataType::Varchar, DataType::Int32],
        )
        .unwrap();
        assert!(full_key.matches_prefix(&prefix2).unwrap());

        // Non-matching prefix
        let prefix3 =
            BTreeKey::from_values(&[Value::String("other".to_string())], &[DataType::Varchar])
                .unwrap();
        assert!(!full_key.matches_prefix(&prefix3).unwrap());
    }

    #[test]
    fn test_btree_key_schema_mismatch() {
        let values = vec![Value::Int32(42)];
        let _schema1 = vec![DataType::Int32];
        let schema2 = vec![DataType::Varchar];

        // Should fail with mismatched schema
        let key = BTreeKey::from_values(&values, &schema2);
        assert!(key.is_err());
    }

    #[test]
    fn test_btree_key_serialization_roundtrip() {
        let values = vec![
            Value::Boolean(true),
            Value::Int32(-123),
            Value::String("test string".to_string()),
            Value::Null,
        ];
        let schema = vec![
            DataType::Boolean,
            DataType::Int32,
            DataType::Varchar,
            DataType::Int32, // NULL can be any type
        ];

        let key = BTreeKey::from_values(&values, &schema).unwrap();
        let data = key.data().to_vec();

        // Create from raw data
        let key2 = BTreeKey::from_data(data, schema);
        let values2 = key2.to_values().unwrap();

        assert_eq!(values, values2);
    }
}
