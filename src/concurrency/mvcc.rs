//! Multi-Version Concurrency Control (MVCC) implementation.
//!
//! This module provides the core MVCC functionality, coordinating between
//! timestamp ordering, version management, and lock management.

use crate::access::TupleId;
use crate::concurrency::{
    lock::{LockId, LockManager, LockMode},
    timestamp::{Timestamp, TimestampOracle},
    version::{VersionKey, VersionManager},
};
use crate::storage::PageId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Isolation levels supported by the MVCC system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read Committed: Each query sees only committed data at query start.
    ReadCommitted,
    /// Repeatable Read: All queries in a transaction see the same snapshot.
    RepeatableRead,
    /// Serializable: Transactions appear to execute serially.
    Serializable,
}

impl IsolationLevel {
    /// Whether this isolation level requires predicate locks.
    pub fn requires_predicate_locks(&self) -> bool {
        matches!(self, IsolationLevel::Serializable)
    }

    /// Whether this isolation level uses a consistent snapshot.
    pub fn uses_snapshot(&self) -> bool {
        matches!(
            self,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable
        )
    }
}

/// Transaction state in the MVCC system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active.
    Active,
    /// Transaction is preparing to commit.
    Preparing,
    /// Transaction has committed.
    Committed,
    /// Transaction has aborted.
    Aborted,
}

/// Information about an active transaction.
#[derive(Debug)]
pub struct TransactionInfo {
    /// Transaction ID.
    pub tid: u64,
    /// Transaction start timestamp.
    pub start_timestamp: Timestamp,
    /// Transaction commit timestamp (set when committing).
    pub commit_timestamp: Option<Timestamp>,
    /// Isolation level.
    pub isolation_level: IsolationLevel,
    /// Current state.
    pub state: TransactionState,
    /// Versions created by this transaction.
    pub created_versions: Vec<VersionKey>,
    /// Versions deleted by this transaction.
    pub deleted_versions: Vec<VersionKey>,
}

/// MVCC manager that coordinates all components.
pub struct MVCCManager {
    /// Timestamp oracle for generating timestamps.
    timestamp_oracle: TimestampOracle,
    /// Version manager for tuple versions.
    version_manager: Arc<VersionManager>,
    /// Lock manager for concurrency control.
    lock_manager: Arc<LockManager>,
    /// Active transactions.
    transactions: Arc<RwLock<HashMap<u64, TransactionInfo>>>,
    /// Next transaction ID.
    next_tid: Arc<RwLock<u64>>,
}

impl MVCCManager {
    /// Creates a new MVCC manager.
    pub fn new() -> Self {
        Self {
            timestamp_oracle: TimestampOracle::new(),
            version_manager: Arc::new(VersionManager::new()),
            lock_manager: Arc::new(LockManager::new()),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            next_tid: Arc::new(RwLock::new(1)),
        }
    }

    /// Begins a new transaction.
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> u64 {
        let mut next_tid = self.next_tid.write().unwrap();
        let tid = *next_tid;
        *next_tid += 1;
        drop(next_tid);

        let start_timestamp = self.timestamp_oracle.generate();

        let info = TransactionInfo {
            tid,
            start_timestamp,
            commit_timestamp: None,
            isolation_level,
            state: TransactionState::Active,
            created_versions: Vec::new(),
            deleted_versions: Vec::new(),
        };

        self.transactions.write().unwrap().insert(tid, info);

        tid
    }

    /// Reads a tuple in the context of a transaction.
    pub fn read_tuple(
        &self,
        tid: u64,
        page_id: PageId,
        tuple_id: TupleId,
    ) -> Result<Option<Vec<u8>>, String> {
        let transactions = self.transactions.read().unwrap();
        let tx_info = transactions
            .get(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        let key = VersionKey::new(page_id, tuple_id);
        // In MVCC, reads don't need locks - they read from snapshots

        // Determine the snapshot timestamp based on isolation level
        let snapshot_ts = if tx_info.isolation_level.uses_snapshot() {
            tx_info.start_timestamp
        } else {
            // For Read Committed, use current timestamp
            self.timestamp_oracle.current()
        };

        // Get the visible version
        let version_opt = self.version_manager.get_visible_version(&key, snapshot_ts);

        if let Some(version_arc) = version_opt {
            let version = version_arc.read().unwrap();

            // Check if this version was created by our transaction
            if version.created_by(tid) {
                // We can see our own writes
                if version.deleted_by(tid) {
                    Ok(None) // We deleted it
                } else {
                    Ok(Some(version.data.clone()))
                }
            } else {
                // Check if the creator transaction is committed
                let creator_state = self.get_transaction_state(version.creator_tid);

                // Only visible if creator is committed
                if creator_state == Some(TransactionState::Committed) {
                    if version.end_timestamp.is_none()
                        || version.end_timestamp.is_some_and(|end| end > snapshot_ts)
                    {
                        Ok(Some(version.data.clone()))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None) // Creator is not committed, so version is not visible
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Inserts a new tuple in the context of a transaction.
    pub fn insert_tuple(
        &self,
        tid: u64,
        page_id: PageId,
        tuple_id: TupleId,
        data: Vec<u8>,
    ) -> Result<(), String> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions
            .get_mut(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        let key = VersionKey::new(page_id, tuple_id);
        let lock_id = LockId::Tuple(page_id, tuple_id);

        // Acquire exclusive lock for write
        self.lock_manager.acquire_lock(
            tid,
            lock_id,
            LockMode::Exclusive,
            Some(Duration::from_secs(5)),
        )?;

        // Check if tuple already exists
        if self
            .version_manager
            .get_visible_version(&key, self.timestamp_oracle.current())
            .is_some()
        {
            return Err("Tuple already exists".to_string());
        }

        // Insert new version
        let timestamp = self.timestamp_oracle.generate();
        self.version_manager
            .insert_version(key.clone(), timestamp, tid, data);

        // Track this version
        tx_info.created_versions.push(key);

        Ok(())
    }

    /// Updates a tuple in the context of a transaction.
    pub fn update_tuple(
        &self,
        tid: u64,
        page_id: PageId,
        tuple_id: TupleId,
        data: Vec<u8>,
    ) -> Result<(), String> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions
            .get_mut(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        let key = VersionKey::new(page_id, tuple_id);
        let lock_id = LockId::Tuple(page_id, tuple_id);

        // Acquire exclusive lock for write
        self.lock_manager.acquire_lock(
            tid,
            lock_id,
            LockMode::Exclusive,
            Some(Duration::from_secs(5)),
        )?;

        // Check if tuple exists and is visible
        let current_version = self
            .version_manager
            .get_visible_version(&key, self.timestamp_oracle.current())
            .ok_or_else(|| "Tuple not found".to_string())?;

        // For Repeatable Read and Serializable, check for write-write conflicts
        if tx_info.isolation_level != IsolationLevel::ReadCommitted {
            let version = current_version.read().unwrap();
            if version.begin_timestamp > tx_info.start_timestamp && !version.created_by(tid) {
                return Err("Write-write conflict detected".to_string());
            }
        }

        // Mark old version as deleted
        let timestamp = self.timestamp_oracle.generate();
        self.version_manager.delete_version(&key, timestamp, tid)?;

        // Insert new version
        self.version_manager
            .insert_version(key.clone(), timestamp, tid, data);

        // Track these changes
        tx_info.deleted_versions.push(key.clone());
        tx_info.created_versions.push(key);

        Ok(())
    }

    /// Deletes a tuple in the context of a transaction.
    pub fn delete_tuple(&self, tid: u64, page_id: PageId, tuple_id: TupleId) -> Result<(), String> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions
            .get_mut(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        let key = VersionKey::new(page_id, tuple_id);
        let lock_id = LockId::Tuple(page_id, tuple_id);

        // Acquire exclusive lock for write
        self.lock_manager.acquire_lock(
            tid,
            lock_id,
            LockMode::Exclusive,
            Some(Duration::from_secs(5)),
        )?;

        // Check if tuple exists and is visible
        let current_version = self
            .version_manager
            .get_visible_version(&key, self.timestamp_oracle.current())
            .ok_or_else(|| "Tuple not found".to_string())?;

        // For Repeatable Read and Serializable, check for write-write conflicts
        if tx_info.isolation_level != IsolationLevel::ReadCommitted {
            let version = current_version.read().unwrap();
            if version.begin_timestamp > tx_info.start_timestamp && !version.created_by(tid) {
                return Err("Write-write conflict detected".to_string());
            }
        }

        // Mark as deleted
        let timestamp = self.timestamp_oracle.generate();
        self.version_manager.delete_version(&key, timestamp, tid)?;

        // Track this deletion
        tx_info.deleted_versions.push(key);

        Ok(())
    }

    /// Commits a transaction.
    pub fn commit_transaction(&self, tid: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions
            .get_mut(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        // Set state to preparing
        tx_info.state = TransactionState::Preparing;

        // Get commit timestamp
        let commit_timestamp = self.timestamp_oracle.generate();
        tx_info.commit_timestamp = Some(commit_timestamp);

        // For Serializable isolation, we would perform additional validation here
        if tx_info.isolation_level == IsolationLevel::Serializable {
            // TODO: Implement serialization graph testing
        }

        // Commit is successful
        tx_info.state = TransactionState::Committed;

        // Release all locks
        self.lock_manager.release_all_locks(tid);

        Ok(())
    }

    /// Aborts a transaction.
    pub fn abort_transaction(&self, tid: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions
            .get_mut(&tid)
            .ok_or_else(|| "Transaction not found".to_string())?;

        if tx_info.state == TransactionState::Committed {
            return Err("Cannot abort committed transaction".to_string());
        }

        // Set state to aborted
        tx_info.state = TransactionState::Aborted;

        // TODO: Rollback changes by removing versions created by this transaction
        // For now, we rely on visibility rules to hide aborted changes

        // Release all locks
        self.lock_manager.release_all_locks(tid);

        Ok(())
    }

    /// Gets the state of a transaction.
    pub fn get_transaction_state(&self, tid: u64) -> Option<TransactionState> {
        self.transactions
            .read()
            .unwrap()
            .get(&tid)
            .map(|info| info.state)
    }

    /// Performs garbage collection on old versions.
    pub fn garbage_collect(&self) {
        let transactions = self.transactions.read().unwrap();

        // Find the oldest active transaction
        let oldest_active = transactions
            .values()
            .filter(|tx| tx.state == TransactionState::Active)
            .map(|tx| tx.start_timestamp)
            .min();

        if let Some(oldest_ts) = oldest_active {
            self.version_manager.garbage_collect(oldest_ts);
        }

        // Clean up completed transactions
        let completed_tids: Vec<u64> = transactions
            .iter()
            .filter(|(_, tx)| {
                matches!(
                    tx.state,
                    TransactionState::Committed | TransactionState::Aborted
                )
            })
            .map(|(tid, _)| *tid)
            .collect();

        drop(transactions);

        if !completed_tids.is_empty() {
            let mut transactions = self.transactions.write().unwrap();
            for tid in completed_tids {
                transactions.remove(&tid);
            }
        }
    }

    /// Gets statistics about the MVCC system.
    pub fn get_stats(&self) -> MVCCStats {
        let transactions = self.transactions.read().unwrap();

        MVCCStats {
            active_transactions: transactions
                .values()
                .filter(|tx| tx.state == TransactionState::Active)
                .count(),
            total_transactions: transactions.len(),
            version_chains: self.version_manager.chain_count(),
            total_versions: self.version_manager.total_version_count(),
        }
    }
}

impl Default for MVCCManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the MVCC system.
#[derive(Debug)]
pub struct MVCCStats {
    pub active_transactions: usize,
    pub total_transactions: usize,
    pub version_chains: usize,
    pub total_versions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_transaction() {
        let mvcc = MVCCManager::new();

        // Begin transaction
        let tid = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        assert_eq!(
            mvcc.get_transaction_state(tid),
            Some(TransactionState::Active)
        );

        // Insert tuple
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);
        let data = vec![1, 2, 3, 4];

        assert!(mvcc
            .insert_tuple(tid, page_id, tuple_id, data.clone())
            .is_ok());

        // Read tuple (should see our own write)
        let read_result = mvcc.read_tuple(tid, page_id, tuple_id).unwrap();
        assert_eq!(read_result, Some(data));

        // Commit transaction
        assert!(mvcc.commit_transaction(tid).is_ok());
        assert_eq!(
            mvcc.get_transaction_state(tid),
            Some(TransactionState::Committed)
        );
    }

    #[test]
    fn test_isolation_read_committed() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // T1: Insert and commit
        let t1 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        mvcc.insert_tuple(t1, page_id, tuple_id, vec![1, 2, 3])
            .unwrap();
        mvcc.commit_transaction(t1).unwrap();

        // T2: Begin and read (should see T1's write)
        let t2 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        let read1 = mvcc.read_tuple(t2, page_id, tuple_id).unwrap();
        assert_eq!(read1, Some(vec![1, 2, 3]));

        // In MVCC, reads don't hold locks, so no need to release

        // T3: Update the tuple
        let t3 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        mvcc.update_tuple(t3, page_id, tuple_id, vec![4, 5, 6])
            .unwrap();
        mvcc.commit_transaction(t3).unwrap();

        // T2: Read again (Read Committed sees the new value)
        let read2 = mvcc.read_tuple(t2, page_id, tuple_id).unwrap();
        assert_eq!(read2, Some(vec![4, 5, 6]));

        mvcc.commit_transaction(t2).unwrap();
    }

    #[test]
    fn test_isolation_repeatable_read() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // T1: Insert and commit
        let t1 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        mvcc.insert_tuple(t1, page_id, tuple_id, vec![1, 2, 3])
            .unwrap();
        mvcc.commit_transaction(t1).unwrap();

        // T2: Begin and read
        let t2 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        let read1 = mvcc.read_tuple(t2, page_id, tuple_id).unwrap();
        assert_eq!(read1, Some(vec![1, 2, 3]));

        // T3: Update the tuple
        let t3 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        mvcc.update_tuple(t3, page_id, tuple_id, vec![4, 5, 6])
            .unwrap();
        mvcc.commit_transaction(t3).unwrap();

        // T2: Read again (Repeatable Read sees the same value)
        let read2 = mvcc.read_tuple(t2, page_id, tuple_id).unwrap();
        assert_eq!(read2, Some(vec![1, 2, 3])); // Still sees old value

        mvcc.commit_transaction(t2).unwrap();
    }

    #[test]
    fn test_write_write_conflict() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // T1: Insert and commit
        let t1 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        mvcc.insert_tuple(t1, page_id, tuple_id, vec![1, 2, 3])
            .unwrap();
        mvcc.commit_transaction(t1).unwrap();

        // T2 and T3: Both try to update the same tuple
        let t2 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        let t3 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);

        // T2 updates first
        assert!(mvcc
            .update_tuple(t2, page_id, tuple_id, vec![4, 5, 6])
            .is_ok());

        // T3 tries to update (should fail due to lock)
        assert!(mvcc
            .update_tuple(t3, page_id, tuple_id, vec![7, 8, 9])
            .is_err());
    }

    #[test]
    fn test_abort_transaction() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // T1: Insert and abort
        let t1 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        mvcc.insert_tuple(t1, page_id, tuple_id, vec![1, 2, 3])
            .unwrap();
        mvcc.abort_transaction(t1).unwrap();

        // T2: Should not see T1's aborted write
        let t2 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        let read = mvcc.read_tuple(t2, page_id, tuple_id).unwrap();
        assert_eq!(read, None);
        mvcc.commit_transaction(t2).unwrap();
    }

    #[test]
    fn test_delete_tuple() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // T1: Insert and commit
        let t1 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        mvcc.insert_tuple(t1, page_id, tuple_id, vec![1, 2, 3])
            .unwrap();
        mvcc.commit_transaction(t1).unwrap();

        // T2: Delete and commit
        let t2 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        mvcc.delete_tuple(t2, page_id, tuple_id).unwrap();
        mvcc.commit_transaction(t2).unwrap();

        // T3: Should not see deleted tuple
        let t3 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        let read = mvcc.read_tuple(t3, page_id, tuple_id).unwrap();
        assert_eq!(read, None);
        mvcc.commit_transaction(t3).unwrap();
    }

    #[test]
    fn test_garbage_collection() {
        let mvcc = MVCCManager::new();
        let page_id = PageId(1);
        let tuple_id = TupleId::new(PageId(1), 100);

        // Create and delete multiple versions
        for i in 0..5 {
            let tid = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
            if i == 0 {
                mvcc.insert_tuple(tid, page_id, tuple_id, vec![i]).unwrap();
            } else {
                mvcc.update_tuple(tid, page_id, tuple_id, vec![i]).unwrap();
            }
            mvcc.commit_transaction(tid).unwrap();
        }

        // Check stats before GC
        let stats_before = mvcc.get_stats();
        assert_eq!(stats_before.version_chains, 1);
        assert_eq!(stats_before.total_versions, 5);

        // Run GC (should clean up old versions since no active transactions)
        mvcc.garbage_collect();

        // Stats after GC should show cleaned up transactions
        let stats_after = mvcc.get_stats();
        assert_eq!(stats_after.total_transactions, 0);
    }

    #[test]
    fn test_mvcc_stats() {
        let mvcc = MVCCManager::new();

        // Start multiple transactions
        let t1 = mvcc.begin_transaction(IsolationLevel::ReadCommitted);
        let t2 = mvcc.begin_transaction(IsolationLevel::RepeatableRead);
        let _t3 = mvcc.begin_transaction(IsolationLevel::Serializable);

        // Check initial stats
        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 3);
        assert_eq!(stats.total_transactions, 3);

        // Commit one transaction
        mvcc.commit_transaction(t1).unwrap();

        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 2);
        assert_eq!(stats.total_transactions, 3);

        // Abort another
        mvcc.abort_transaction(t2).unwrap();

        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 1);
        assert_eq!(stats.total_transactions, 3);
    }
}
