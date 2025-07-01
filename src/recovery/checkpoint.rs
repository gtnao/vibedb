//! Checkpoint management for recovery.
//!
//! Implements fuzzy checkpointing which allows database operations to continue
//! during checkpoint creation.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::storage::{
    wal::{WalManager, WalRecord, LSN},
    PageId,
};
use crate::transaction::TransactionId;

/// Checkpoint manager handles creating and managing checkpoints.
pub struct CheckpointManager {
    /// WAL manager for writing checkpoint records.
    wal_manager: Arc<WalManager>,
    /// Current checkpoint state.
    state: Arc<RwLock<CheckpointState>>,
}

/// Internal checkpoint state.
#[derive(Debug)]
struct CheckpointState {
    /// Last checkpoint LSN.
    last_checkpoint_lsn: LSN,
    /// Whether a checkpoint is currently in progress.
    checkpoint_in_progress: bool,
}

/// Fuzzy checkpoint data structure.
/// Contains all information needed to recover from a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuzzyCheckpoint {
    /// LSN of this checkpoint.
    pub checkpoint_lsn: LSN,
    /// Transaction table at checkpoint time.
    pub transaction_table: TransactionTable,
    /// Dirty page table at checkpoint time.
    pub dirty_page_table: DirtyPageTable,
    /// LSN where redo pass should start.
    pub redo_lsn: LSN,
}

/// Transaction table entry for recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionTableEntry {
    /// Transaction ID.
    pub transaction_id: TransactionId,
    /// Transaction status.
    pub status: TransactionStatus,
    /// Last LSN written by this transaction.
    pub last_lsn: LSN,
    /// Undo next LSN (for undo processing).
    pub undo_next_lsn: LSN,
}

/// Transaction status during recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    /// Transaction is active.
    Active,
    /// Transaction is committed but not yet flushed.
    Committing,
    /// Transaction is being aborted.
    Aborting,
}

/// Transaction table maps transaction IDs to their recovery information.
pub type TransactionTable = HashMap<TransactionId, TransactionTableEntry>;

/// Dirty page table entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyPageEntry {
    /// Page ID.
    pub page_id: PageId,
    /// Recovery LSN - earliest LSN that might have dirtied this page.
    pub recovery_lsn: LSN,
}

/// Dirty page table tracks which pages might need redo.
pub type DirtyPageTable = HashMap<PageId, DirtyPageEntry>;

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(wal_manager: Arc<WalManager>) -> Self {
        CheckpointManager {
            wal_manager,
            state: Arc::new(RwLock::new(CheckpointState {
                last_checkpoint_lsn: LSN::new(),
                checkpoint_in_progress: false,
            })),
        }
    }

    /// Get the last checkpoint LSN.
    pub fn last_checkpoint_lsn(&self) -> LSN {
        self.state.read().unwrap().last_checkpoint_lsn
    }

    /// Create a fuzzy checkpoint.
    /// This allows other transactions to continue while the checkpoint is created.
    pub fn create_fuzzy_checkpoint(
        &self,
        active_transactions: &HashMap<TransactionId, LSN>,
        dirty_pages: &HashMap<PageId, LSN>,
    ) -> Result<FuzzyCheckpoint, CheckpointError> {
        // Check if checkpoint is already in progress
        {
            let mut state = self.state.write().unwrap();
            if state.checkpoint_in_progress {
                return Err(CheckpointError::CheckpointInProgress);
            }
            state.checkpoint_in_progress = true;
        }

        // Build transaction table
        let mut transaction_table = TransactionTable::new();
        for (txn_id, last_lsn) in active_transactions {
            transaction_table.insert(
                *txn_id,
                TransactionTableEntry {
                    transaction_id: *txn_id,
                    status: TransactionStatus::Active,
                    last_lsn: *last_lsn,
                    undo_next_lsn: *last_lsn, // Will be updated during recovery
                },
            );
        }

        // Build dirty page table and find minimum recovery LSN
        let mut dirty_page_table = DirtyPageTable::new();
        let mut min_recovery_lsn = LSN(u64::MAX);

        for (page_id, recovery_lsn) in dirty_pages {
            dirty_page_table.insert(
                *page_id,
                DirtyPageEntry {
                    page_id: *page_id,
                    recovery_lsn: *recovery_lsn,
                },
            );

            if *recovery_lsn < min_recovery_lsn {
                min_recovery_lsn = *recovery_lsn;
            }
        }

        // If no dirty pages, redo LSN is the next LSN
        let redo_lsn = if min_recovery_lsn == LSN(u64::MAX) {
            self.wal_manager.get_current_lsn()
        } else {
            min_recovery_lsn
        };

        // Write checkpoint record to WAL
        let checkpoint_lsn = self.wal_manager.get_current_lsn();
        let checkpoint_record = WalRecord::checkpoint(
            checkpoint_lsn,
            active_transactions.keys().map(|tid| tid.0).collect(),
        );

        self.wal_manager
            .write_record(&checkpoint_record)
            .map_err(|_| CheckpointError::WalWriteError)?;

        // Create fuzzy checkpoint structure
        let checkpoint = FuzzyCheckpoint {
            checkpoint_lsn,
            transaction_table,
            dirty_page_table,
            redo_lsn,
        };

        // Update state
        {
            let mut state = self.state.write().unwrap();
            state.last_checkpoint_lsn = checkpoint_lsn;
            state.checkpoint_in_progress = false;
        }

        Ok(checkpoint)
    }

    /// Write checkpoint metadata to disk.
    pub fn write_checkpoint_metadata(
        &self,
        checkpoint: &FuzzyCheckpoint,
    ) -> Result<(), CheckpointError> {
        // In a real implementation, this would write to a special checkpoint file
        // For now, we'll just validate the checkpoint
        if checkpoint.checkpoint_lsn.is_invalid() {
            return Err(CheckpointError::InvalidCheckpoint);
        }
        Ok(())
    }

    /// Read the last checkpoint metadata from disk.
    pub fn read_last_checkpoint(&self) -> Result<Option<FuzzyCheckpoint>, CheckpointError> {
        // In a real implementation, this would read from a special checkpoint file
        // For now, return None to indicate no checkpoint found
        Ok(None)
    }
}

/// Errors that can occur during checkpoint operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointError {
    /// A checkpoint is already in progress.
    CheckpointInProgress,
    /// Error writing to WAL.
    WalWriteError,
    /// Invalid checkpoint data.
    InvalidCheckpoint,
    /// I/O error.
    IoError(String),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::CheckpointInProgress => write!(f, "Checkpoint already in progress"),
            CheckpointError::WalWriteError => write!(f, "Failed to write checkpoint to WAL"),
            CheckpointError::InvalidCheckpoint => write!(f, "Invalid checkpoint data"),
            CheckpointError::IoError(msg) => write!(f, "I/O error: {}", msg),
        }
    }
}

impl std::error::Error for CheckpointError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_wal_manager() -> Arc<WalManager> {
        let temp_dir = TempDir::new().unwrap();
        let config = crate::storage::wal::WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_file_size: 10 * 1024 * 1024,
            sync_on_commit: false,
        };
        let manager = Arc::new(WalManager::new(config).unwrap());
        manager.initialize().unwrap();
        manager
    }

    #[test]
    fn test_checkpoint_manager_creation() {
        let wal_manager = create_test_wal_manager();
        let checkpoint_mgr = CheckpointManager::new(wal_manager);

        assert_eq!(checkpoint_mgr.last_checkpoint_lsn(), LSN::new());
    }

    #[test]
    fn test_fuzzy_checkpoint_creation() {
        let wal_manager = create_test_wal_manager();
        let checkpoint_mgr = CheckpointManager::new(wal_manager.clone());

        // Create some active transactions
        let mut active_txns = HashMap::new();
        active_txns.insert(TransactionId::new(1), LSN(10));
        active_txns.insert(TransactionId::new(2), LSN(20));

        // Create some dirty pages
        let mut dirty_pages = HashMap::new();
        dirty_pages.insert(PageId(100), LSN(5));
        dirty_pages.insert(PageId(200), LSN(15));

        // Create checkpoint
        let checkpoint = checkpoint_mgr
            .create_fuzzy_checkpoint(&active_txns, &dirty_pages)
            .unwrap();

        // Verify checkpoint contents
        assert_eq!(checkpoint.transaction_table.len(), 2);
        assert_eq!(checkpoint.dirty_page_table.len(), 2);
        assert_eq!(checkpoint.redo_lsn, LSN(5)); // Minimum recovery LSN

        // Verify transaction table
        let txn1 = checkpoint
            .transaction_table
            .get(&TransactionId::new(1))
            .unwrap();
        assert_eq!(txn1.last_lsn, LSN(10));
        assert_eq!(txn1.status, TransactionStatus::Active);

        // Verify dirty page table
        let page1 = checkpoint.dirty_page_table.get(&PageId(100)).unwrap();
        assert_eq!(page1.recovery_lsn, LSN(5));
    }

    #[test]
    fn test_checkpoint_in_progress_error() {
        let wal_manager = create_test_wal_manager();
        let checkpoint_mgr = CheckpointManager::new(wal_manager);

        // Set checkpoint in progress
        {
            let mut state = checkpoint_mgr.state.write().unwrap();
            state.checkpoint_in_progress = true;
        }

        // Try to create another checkpoint
        let result = checkpoint_mgr.create_fuzzy_checkpoint(&HashMap::new(), &HashMap::new());
        assert!(matches!(result, Err(CheckpointError::CheckpointInProgress)));
    }

    #[test]
    fn test_empty_checkpoint() {
        let wal_manager = create_test_wal_manager();
        let checkpoint_mgr = CheckpointManager::new(wal_manager.clone());

        // Create checkpoint with no active transactions or dirty pages
        let checkpoint = checkpoint_mgr
            .create_fuzzy_checkpoint(&HashMap::new(), &HashMap::new())
            .unwrap();

        assert_eq!(checkpoint.transaction_table.len(), 0);
        assert_eq!(checkpoint.dirty_page_table.len(), 0);
        // Redo LSN should be current LSN when no dirty pages
        // Since this is a new WAL manager, current LSN starts at 0
        assert_eq!(checkpoint.redo_lsn, LSN(0));
    }
}
