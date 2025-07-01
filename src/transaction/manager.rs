//! Transaction manager for coordinating transaction lifecycle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::id::{TransactionId, TransactionIdGenerator};
use super::state::{SharedTransactionInfo, TransactionState};

/// Error types for transaction operations.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError {
    /// The transaction was not found.
    NotFound(TransactionId),
    /// The transaction is not in the expected state.
    InvalidState(TransactionId, TransactionState),
    /// General transaction error with a message.
    General(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "Transaction {} not found", id),
            Self::InvalidState(id, state) => {
                write!(f, "Transaction {} is in invalid state: {}", id, state)
            }
            Self::General(msg) => write!(f, "Transaction error: {}", msg),
        }
    }
}

impl std::error::Error for TransactionError {}

/// Result type for transaction operations.
pub type Result<T> = std::result::Result<T, TransactionError>;

/// The transaction manager handles the lifecycle of all transactions.
pub struct TransactionManager {
    /// Generator for unique transaction IDs.
    id_generator: TransactionIdGenerator,
    /// Map of active and recently finished transactions.
    transactions: Arc<RwLock<HashMap<TransactionId, SharedTransactionInfo>>>,
}

impl TransactionManager {
    /// Creates a new transaction manager.
    pub fn new() -> Self {
        Self {
            id_generator: TransactionIdGenerator::new(),
            transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begins a new transaction.
    pub fn begin(&self) -> Result<TransactionId> {
        let id = self.id_generator.next();
        let info = SharedTransactionInfo::new(id);

        let mut transactions = self.transactions.write().unwrap();
        transactions.insert(id, info);

        Ok(id)
    }

    /// Commits a transaction.
    pub fn commit(&self, id: TransactionId) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        let info = transactions
            .get(&id)
            .ok_or(TransactionError::NotFound(id))?;

        info.commit().map_err(TransactionError::General)?;

        Ok(())
    }

    /// Aborts a transaction.
    pub fn abort(&self, id: TransactionId) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        let info = transactions
            .get(&id)
            .ok_or(TransactionError::NotFound(id))?;

        info.abort().map_err(TransactionError::General)?;

        Ok(())
    }

    /// Gets the state of a transaction.
    pub fn get_state(&self, id: TransactionId) -> Result<TransactionState> {
        let transactions = self.transactions.read().unwrap();
        let info = transactions
            .get(&id)
            .ok_or(TransactionError::NotFound(id))?;

        Ok(info.state())
    }

    /// Gets information about a transaction.
    pub fn get_info(&self, id: TransactionId) -> Result<SharedTransactionInfo> {
        let transactions = self.transactions.read().unwrap();
        let info = transactions
            .get(&id)
            .ok_or(TransactionError::NotFound(id))?;

        Ok(info.clone())
    }

    /// Returns a list of all active transactions.
    pub fn active_transactions(&self) -> Vec<TransactionId> {
        let transactions = self.transactions.read().unwrap();
        transactions
            .iter()
            .filter(|(_, info)| info.state() == TransactionState::Active)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Returns the total number of transactions (active and finished).
    pub fn transaction_count(&self) -> usize {
        self.transactions.read().unwrap().len()
    }

    /// Removes finished transactions from the manager.
    /// This is useful for preventing memory growth in long-running systems.
    pub fn cleanup_finished(&self) -> usize {
        let mut transactions = self.transactions.write().unwrap();
        let initial_count = transactions.len();

        transactions.retain(|_, info| info.state() == TransactionState::Active);

        initial_count - transactions.len()
    }

    /// Checks if a transaction exists and is active.
    pub fn is_active(&self, id: TransactionId) -> bool {
        self.get_state(id)
            .map(|state| state == TransactionState::Active)
            .unwrap_or(false)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// A handle to a transaction that automatically aborts on drop if still active.
pub struct Transaction {
    id: TransactionId,
    manager: Arc<TransactionManager>,
    committed: bool,
}

impl Transaction {
    /// Creates a new transaction handle.
    pub fn new(manager: Arc<TransactionManager>) -> Result<Self> {
        let id = manager.begin()?;
        Ok(Self {
            id,
            manager,
            committed: false,
        })
    }

    /// Gets the transaction ID.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Commits the transaction.
    pub fn commit(mut self) -> Result<()> {
        self.manager.commit(self.id)?;
        self.committed = true;
        Ok(())
    }

    /// Aborts the transaction.
    pub fn abort(mut self) -> Result<()> {
        self.manager.abort(self.id)?;
        self.committed = true; // Mark as handled to prevent double abort
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            // Best effort abort - ignore errors
            let _ = self.manager.abort(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_begin() {
        let manager = TransactionManager::new();

        let id1 = manager.begin().unwrap();
        let id2 = manager.begin().unwrap();

        assert_ne!(id1, id2);
        assert_eq!(manager.transaction_count(), 2);
        assert_eq!(manager.active_transactions().len(), 2);
    }

    #[test]
    fn test_transaction_manager_commit() {
        let manager = TransactionManager::new();

        let id = manager.begin().unwrap();
        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Active);

        manager.commit(id).unwrap();
        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Committed);

        // Can't commit again
        assert!(manager.commit(id).is_err());
    }

    #[test]
    fn test_transaction_manager_abort() {
        let manager = TransactionManager::new();

        let id = manager.begin().unwrap();
        manager.abort(id).unwrap();
        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Aborted);

        // Can't abort again
        assert!(manager.abort(id).is_err());
    }

    #[test]
    fn test_transaction_manager_not_found() {
        let manager = TransactionManager::new();
        let fake_id = TransactionId::new(999);

        assert!(matches!(
            manager.get_state(fake_id),
            Err(TransactionError::NotFound(_))
        ));
        assert!(matches!(
            manager.commit(fake_id),
            Err(TransactionError::NotFound(_))
        ));
        assert!(matches!(
            manager.abort(fake_id),
            Err(TransactionError::NotFound(_))
        ));
    }

    #[test]
    fn test_transaction_manager_active_transactions() {
        let manager = TransactionManager::new();

        let id1 = manager.begin().unwrap();
        let id2 = manager.begin().unwrap();
        let id3 = manager.begin().unwrap();

        assert_eq!(manager.active_transactions().len(), 3);

        manager.commit(id1).unwrap();
        assert_eq!(manager.active_transactions().len(), 2);

        manager.abort(id2).unwrap();
        assert_eq!(manager.active_transactions().len(), 1);

        assert!(manager.active_transactions().contains(&id3));
    }

    #[test]
    fn test_transaction_manager_cleanup() {
        let manager = TransactionManager::new();

        let id1 = manager.begin().unwrap();
        let id2 = manager.begin().unwrap();
        let id3 = manager.begin().unwrap();

        manager.commit(id1).unwrap();
        manager.abort(id2).unwrap();

        assert_eq!(manager.transaction_count(), 3);

        let cleaned = manager.cleanup_finished();
        assert_eq!(cleaned, 2);
        assert_eq!(manager.transaction_count(), 1);
        assert!(manager.is_active(id3));
    }

    #[test]
    fn test_transaction_handle() {
        let manager = Arc::new(TransactionManager::new());

        let txn = Transaction::new(Arc::clone(&manager)).unwrap();
        let id = txn.id();

        assert!(manager.is_active(id));

        txn.commit().unwrap();
        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Committed);
    }

    #[test]
    fn test_transaction_handle_auto_abort() {
        let manager = Arc::new(TransactionManager::new());

        let id = {
            let txn = Transaction::new(Arc::clone(&manager)).unwrap();
            txn.id()
            // txn drops here without commit
        };

        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_handle_explicit_abort() {
        let manager = Arc::new(TransactionManager::new());

        let txn = Transaction::new(Arc::clone(&manager)).unwrap();
        let id = txn.id();

        txn.abort().unwrap();
        assert_eq!(manager.get_state(id).unwrap(), TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_manager_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let manager = Arc::new(TransactionManager::new());
        let mut handles = vec![];

        // Spawn threads to create transactions
        for _ in 0..10 {
            let mgr = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..10 {
                    ids.push(mgr.begin().unwrap());
                }
                ids
            });
            handles.push(handle);
        }

        // Collect all transaction IDs
        let mut all_ids = vec![];
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // Verify all IDs are unique
        let mut unique_ids = all_ids.clone();
        unique_ids.sort();
        unique_ids.dedup();

        assert_eq!(all_ids.len(), 100);
        assert_eq!(unique_ids.len(), 100);
        assert_eq!(manager.transaction_count(), 100);
        assert_eq!(manager.active_transactions().len(), 100);
    }
}
