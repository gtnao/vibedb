//! Recovery-specific log record types.
//!
//! Extends the base WAL records with recovery-specific types such as
//! Compensation Log Records (CLRs) used during the undo phase.

use serde::{Deserialize, Serialize};

use crate::storage::wal::{WalRecord, WalRecordHeader, WalRecordPayload, WalRecordType, LSN};

/// Compensation Log Record (CLR) - written during undo operations.
/// CLRs are redo-only log records that describe the undo of a previous operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClrRecord {
    /// Transaction ID performing the undo.
    pub transaction_id: u64,
    /// LSN of the record being undone.
    pub undo_lsn: LSN,
    /// Next LSN to undo in this transaction (used to skip over CLRs).
    pub undo_next_lsn: LSN,
    /// The actual undo operation (e.g., re-insert for delete, delete for insert).
    pub undo_operation: WalRecordPayload,
}

/// Extended WAL record types for recovery.
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryLogRecord {
    /// Standard WAL record.
    Standard(WalRecord),
    /// Compensation Log Record.
    Clr(ClrWalRecord),
}

/// WAL record wrapper for CLR.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClrWalRecord {
    /// Record header.
    pub header: WalRecordHeader,
    /// CLR payload.
    pub clr: ClrRecord,
}

impl ClrWalRecord {
    /// Create a new CLR record.
    pub fn new(
        lsn: LSN,
        prev_lsn: LSN,
        transaction_id: u64,
        undo_lsn: LSN,
        undo_next_lsn: LSN,
        undo_operation: WalRecordPayload,
    ) -> Self {
        let clr = ClrRecord {
            transaction_id,
            undo_lsn,
            undo_next_lsn,
            undo_operation,
        };

        let header = WalRecordHeader {
            lsn,
            prev_lsn,
            transaction_id,
            record_type: WalRecordType::Update, // CLRs are always redo-only updates
            payload_size: bincode::serialize(&clr).unwrap().len() as u32,
        };

        ClrWalRecord { header, clr }
    }

    /// Check if this CLR is for an insert undo (i.e., delete).
    pub fn is_insert_undo(&self) -> bool {
        matches!(&self.clr.undo_operation, WalRecordPayload::Delete(_))
    }

    /// Check if this CLR is for a delete undo (i.e., insert).
    pub fn is_delete_undo(&self) -> bool {
        matches!(&self.clr.undo_operation, WalRecordPayload::Insert(_))
    }

    /// Check if this CLR is for an update undo.
    pub fn is_update_undo(&self) -> bool {
        matches!(&self.clr.undo_operation, WalRecordPayload::Update(_))
    }
}

impl RecoveryLogRecord {
    /// Get the LSN of this record.
    pub fn lsn(&self) -> LSN {
        match self {
            RecoveryLogRecord::Standard(record) => record.header.lsn,
            RecoveryLogRecord::Clr(clr) => clr.header.lsn,
        }
    }

    /// Get the transaction ID of this record.
    pub fn transaction_id(&self) -> u64 {
        match self {
            RecoveryLogRecord::Standard(record) => record.header.transaction_id,
            RecoveryLogRecord::Clr(clr) => clr.header.transaction_id,
        }
    }

    /// Get the previous LSN in the same transaction.
    pub fn prev_lsn(&self) -> LSN {
        match self {
            RecoveryLogRecord::Standard(record) => record.header.prev_lsn,
            RecoveryLogRecord::Clr(clr) => clr.header.prev_lsn,
        }
    }

    /// Check if this is a CLR.
    pub fn is_clr(&self) -> bool {
        matches!(self, RecoveryLogRecord::Clr(_))
    }

    /// Check if this record needs to be redone based on page LSN.
    pub fn needs_redo(&self, page_lsn: LSN) -> bool {
        self.lsn() > page_lsn
    }

    /// Convert a standard WAL record to a recovery log record.
    pub fn from_wal_record(record: WalRecord) -> Self {
        RecoveryLogRecord::Standard(record)
    }
}

/// Utility to create inverse operations for undo.
pub fn create_undo_operation(original: &WalRecordPayload) -> Option<WalRecordPayload> {
    match original {
        WalRecordPayload::Insert(insert) => {
            // Undo of insert is delete
            Some(WalRecordPayload::Delete(
                crate::storage::wal::DeleteRecord {
                    page_id: insert.page_id,
                    tuple_id: insert.tuple_id,
                    values: insert.values.clone(),
                },
            ))
        }
        WalRecordPayload::Delete(delete) => {
            // Undo of delete is insert
            Some(WalRecordPayload::Insert(
                crate::storage::wal::InsertRecord {
                    page_id: delete.page_id,
                    tuple_id: delete.tuple_id,
                    values: delete.values.clone(),
                },
            ))
        }
        WalRecordPayload::Update(update) => {
            // Undo of update is reverse update
            Some(WalRecordPayload::Update(
                crate::storage::wal::UpdateRecord {
                    page_id: update.page_id,
                    tuple_id: update.tuple_id,
                    old_values: update.new_values.clone(),
                    new_values: update.old_values.clone(),
                },
            ))
        }
        _ => None, // Begin, Commit, Abort, Checkpoint don't need undo
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::{record::Value, DeleteRecord, InsertRecord, UpdateRecord};

    #[test]
    fn test_clr_creation() {
        let lsn = LSN(100);
        let prev_lsn = LSN(99);
        let undo_lsn = LSN(95);
        let undo_next_lsn = LSN(90);
        let transaction_id = 42;

        let undo_op = WalRecordPayload::Delete(DeleteRecord {
            page_id: crate::storage::PageId(1),
            tuple_id: 10,
            values: vec![Value::Integer(42)],
        });

        let clr = ClrWalRecord::new(
            lsn,
            prev_lsn,
            transaction_id,
            undo_lsn,
            undo_next_lsn,
            undo_op,
        );

        assert_eq!(clr.header.lsn, lsn);
        assert_eq!(clr.header.prev_lsn, prev_lsn);
        assert_eq!(clr.header.transaction_id, transaction_id);
        assert_eq!(clr.clr.undo_lsn, undo_lsn);
        assert_eq!(clr.clr.undo_next_lsn, undo_next_lsn);
        assert!(clr.is_insert_undo());
    }

    #[test]
    fn test_create_undo_operation() {
        // Test insert undo
        let insert = WalRecordPayload::Insert(InsertRecord {
            page_id: crate::storage::PageId(1),
            tuple_id: 10,
            values: vec![Value::Integer(42), Value::String("test".to_string())],
        });

        let undo_insert = create_undo_operation(&insert).unwrap();
        match undo_insert {
            WalRecordPayload::Delete(delete) => {
                assert_eq!(delete.page_id, crate::storage::PageId(1));
                assert_eq!(delete.tuple_id, 10);
                assert_eq!(delete.values.len(), 2);
            }
            _ => panic!("Expected delete operation"),
        }

        // Test delete undo
        let delete = WalRecordPayload::Delete(DeleteRecord {
            page_id: crate::storage::PageId(2),
            tuple_id: 20,
            values: vec![Value::Boolean(true)],
        });

        let undo_delete = create_undo_operation(&delete).unwrap();
        match undo_delete {
            WalRecordPayload::Insert(insert) => {
                assert_eq!(insert.page_id, crate::storage::PageId(2));
                assert_eq!(insert.tuple_id, 20);
                assert_eq!(insert.values.len(), 1);
            }
            _ => panic!("Expected insert operation"),
        }

        // Test update undo
        let update = WalRecordPayload::Update(UpdateRecord {
            page_id: crate::storage::PageId(3),
            tuple_id: 30,
            old_values: vec![Value::Integer(100)],
            new_values: vec![Value::Integer(200)],
        });

        let undo_update = create_undo_operation(&update).unwrap();
        match undo_update {
            WalRecordPayload::Update(rev_update) => {
                assert_eq!(rev_update.page_id, crate::storage::PageId(3));
                assert_eq!(rev_update.tuple_id, 30);
                assert_eq!(rev_update.old_values, vec![Value::Integer(200)]);
                assert_eq!(rev_update.new_values, vec![Value::Integer(100)]);
            }
            _ => panic!("Expected update operation"),
        }
    }

    #[test]
    fn test_recovery_log_record() {
        let lsn = LSN(100);
        let transaction_id = 42;

        // Test with standard record
        let wal_record = WalRecord::begin(lsn, transaction_id);
        let recovery_record = RecoveryLogRecord::from_wal_record(wal_record);

        assert_eq!(recovery_record.lsn(), lsn);
        assert_eq!(recovery_record.transaction_id(), transaction_id);
        assert!(!recovery_record.is_clr());

        // Test needs_redo
        assert!(recovery_record.needs_redo(LSN(50))); // Page LSN < Record LSN
        assert!(!recovery_record.needs_redo(LSN(150))); // Page LSN > Record LSN
    }
}
