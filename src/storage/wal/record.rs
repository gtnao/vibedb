//! WAL record types and structures.
//!
//! Defines the various types of WAL records used for logging database operations.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::storage::PageId;

/// Simplified TupleId for WAL records (just slot ID).
pub type TupleId = u16;

/// Simplified Value representation for WAL records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
}

/// Log Sequence Number - a unique identifier for WAL records.
/// LSNs are monotonically increasing and used to order log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LSN(pub u64);

impl LSN {
    /// Create a new LSN with value 0.
    pub fn new() -> Self {
        LSN(0)
    }

    /// Get the next LSN.
    pub fn next(&self) -> Self {
        LSN(self.0 + 1)
    }

    /// Check if this is an invalid LSN (0).
    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

impl Default for LSN {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for LSN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LSN({})", self.0)
    }
}

/// WAL record types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalRecordType {
    /// Transaction begin record.
    Begin,
    /// Transaction commit record.
    Commit,
    /// Transaction abort record.
    Abort,
    /// Tuple update record.
    Update,
    /// Tuple insert record.
    Insert,
    /// Tuple delete record.
    Delete,
    /// Checkpoint record.
    Checkpoint,
}

/// WAL record header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalRecordHeader {
    /// Log sequence number of this record.
    pub lsn: LSN,
    /// Previous LSN in the same transaction (0 if first record).
    pub prev_lsn: LSN,
    /// Transaction ID that generated this record.
    pub transaction_id: u64,
    /// Type of the WAL record.
    pub record_type: WalRecordType,
    /// Size of the record payload in bytes.
    pub payload_size: u32,
}

/// WAL record for Begin transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeginRecord {
    /// Transaction ID.
    pub transaction_id: u64,
}

/// WAL record for Commit transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitRecord {
    /// Transaction ID.
    pub transaction_id: u64,
    /// Commit timestamp.
    pub commit_timestamp: u64,
}

/// WAL record for Abort transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AbortRecord {
    /// Transaction ID.
    pub transaction_id: u64,
}

/// WAL record for tuple updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateRecord {
    /// Page ID containing the tuple.
    pub page_id: PageId,
    /// Tuple ID within the page.
    pub tuple_id: TupleId,
    /// Old tuple values (for undo).
    pub old_values: Vec<Value>,
    /// New tuple values.
    pub new_values: Vec<Value>,
}

/// WAL record for tuple inserts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertRecord {
    /// Page ID where tuple is inserted.
    pub page_id: PageId,
    /// Tuple ID of the inserted tuple.
    pub tuple_id: TupleId,
    /// Values of the inserted tuple.
    pub values: Vec<Value>,
}

/// WAL record for tuple deletes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteRecord {
    /// Page ID containing the tuple.
    pub page_id: PageId,
    /// Tuple ID of the deleted tuple.
    pub tuple_id: TupleId,
    /// Values of the deleted tuple (for undo).
    pub values: Vec<Value>,
}

/// WAL record for checkpoints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    /// LSN of the checkpoint.
    pub checkpoint_lsn: LSN,
    /// Active transaction IDs at checkpoint time.
    pub active_transactions: Vec<u64>,
}

/// Complete WAL record with header and payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    /// Record header.
    pub header: WalRecordHeader,
    /// Record payload.
    pub payload: WalRecordPayload,
}

/// WAL record payload variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalRecordPayload {
    /// Begin transaction payload.
    Begin(BeginRecord),
    /// Commit transaction payload.
    Commit(CommitRecord),
    /// Abort transaction payload.
    Abort(AbortRecord),
    /// Update tuple payload.
    Update(UpdateRecord),
    /// Insert tuple payload.
    Insert(InsertRecord),
    /// Delete tuple payload.
    Delete(DeleteRecord),
    /// Checkpoint payload.
    Checkpoint(CheckpointRecord),
}

impl WalRecord {
    /// Create a new Begin transaction record.
    pub fn begin(lsn: LSN, transaction_id: u64) -> Self {
        let payload = WalRecordPayload::Begin(BeginRecord { transaction_id });
        let header = WalRecordHeader {
            lsn,
            prev_lsn: LSN::new(),
            transaction_id,
            record_type: WalRecordType::Begin,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Commit transaction record.
    pub fn commit(lsn: LSN, prev_lsn: LSN, transaction_id: u64, commit_timestamp: u64) -> Self {
        let payload = WalRecordPayload::Commit(CommitRecord {
            transaction_id,
            commit_timestamp,
        });
        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Commit,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Abort transaction record.
    pub fn abort(lsn: LSN, prev_lsn: LSN, transaction_id: u64) -> Self {
        let payload = WalRecordPayload::Abort(AbortRecord { transaction_id });
        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Abort,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Update record.
    pub fn update(
        lsn: LSN,
        prev_lsn: LSN,
        transaction_id: u64,
        page_id: PageId,
        tuple_id: TupleId,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    ) -> Self {
        let payload = WalRecordPayload::Update(UpdateRecord {
            page_id,
            tuple_id,
            old_values,
            new_values,
        });
        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Update,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Insert record.
    pub fn insert(
        lsn: LSN,
        prev_lsn: LSN,
        transaction_id: u64,
        page_id: PageId,
        tuple_id: TupleId,
        values: Vec<Value>,
    ) -> Self {
        let payload = WalRecordPayload::Insert(InsertRecord {
            page_id,
            tuple_id,
            values,
        });
        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Insert,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Delete record.
    pub fn delete(
        lsn: LSN,
        prev_lsn: LSN,
        transaction_id: u64,
        page_id: PageId,
        tuple_id: TupleId,
        values: Vec<Value>,
    ) -> Self {
        let payload = WalRecordPayload::Delete(DeleteRecord {
            page_id,
            tuple_id,
            values,
        });
        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Delete,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Create a new Checkpoint record.
    pub fn checkpoint(lsn: LSN, active_transactions: Vec<u64>) -> Self {
        let payload = WalRecordPayload::Checkpoint(CheckpointRecord {
            checkpoint_lsn: lsn,
            active_transactions,
        });
        let header = WalRecordHeader {
            lsn,
            prev_lsn: LSN::new(),
            transaction_id: 0, // Checkpoint is not associated with a specific transaction
            record_type: WalRecordType::Checkpoint,
            payload_size: bincode::serialize(&payload).unwrap().len() as u32,
        };
        WalRecord { header, payload }
    }

    /// Serialize the WAL record to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize a WAL record from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn() {
        let lsn = LSN::new();
        assert_eq!(lsn.0, 0);
        assert!(lsn.is_invalid());

        let next_lsn = lsn.next();
        assert_eq!(next_lsn.0, 1);
        assert!(!next_lsn.is_invalid());

        assert!(lsn < next_lsn);
    }

    #[test]
    fn test_wal_record_serialization() {
        let lsn = LSN(100);
        let prev_lsn = LSN(99);
        let transaction_id = 42;

        // Test Begin record
        let begin_record = WalRecord::begin(lsn, transaction_id);
        let serialized = begin_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(begin_record, deserialized);

        // Test Commit record
        let commit_record = WalRecord::commit(lsn.next(), lsn, transaction_id, 1234567890);
        let serialized = commit_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(commit_record, deserialized);

        // Test Insert record
        let insert_record = WalRecord::insert(
            lsn,
            prev_lsn,
            transaction_id,
            PageId(1),
            10,
            vec![Value::Integer(42), Value::String("test".to_string())],
        );
        let serialized = insert_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(insert_record, deserialized);

        // Test Update record
        let update_record = WalRecord::update(
            lsn,
            prev_lsn,
            transaction_id,
            PageId(1),
            10,
            vec![Value::Integer(42)],
            vec![Value::Integer(43)],
        );
        let serialized = update_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(update_record, deserialized);

        // Test Delete record
        let delete_record = WalRecord::delete(
            lsn,
            prev_lsn,
            transaction_id,
            PageId(1),
            10,
            vec![Value::Integer(42), Value::String("test".to_string())],
        );
        let serialized = delete_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(delete_record, deserialized);

        // Test Checkpoint record
        let checkpoint_record = WalRecord::checkpoint(lsn, vec![1, 2, 3]);
        let serialized = checkpoint_record.serialize().unwrap();
        let deserialized = WalRecord::deserialize(&serialized).unwrap();
        assert_eq!(checkpoint_record, deserialized);
    }

    #[test]
    fn test_record_payload_size() {
        let lsn = LSN(100);
        let transaction_id = 42;

        let record = WalRecord::begin(lsn, transaction_id);
        let payload_bytes = bincode::serialize(&record.payload).unwrap();
        assert_eq!(record.header.payload_size, payload_bytes.len() as u32);
    }
}
