//! Transaction state management.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::id::TransactionId;

/// The possible states of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// The transaction is currently active and can perform operations.
    Active,
    /// The transaction has been successfully committed.
    Committed,
    /// The transaction has been aborted (rolled back).
    Aborted,
}

impl TransactionState {
    /// Returns true if the transaction is active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns true if the transaction is committed.
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed)
    }

    /// Returns true if the transaction is aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self, Self::Aborted)
    }

    /// Returns true if the transaction is finished (committed or aborted).
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "Active"),
            Self::Committed => write!(f, "Committed"),
            Self::Aborted => write!(f, "Aborted"),
        }
    }
}

/// Information about a transaction.
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// The unique identifier of the transaction.
    pub id: TransactionId,
    /// The current state of the transaction.
    pub state: TransactionState,
    /// The timestamp when the transaction started.
    pub start_time: Instant,
    /// The timestamp when the transaction ended (if finished).
    pub end_time: Option<Instant>,
}

impl TransactionInfo {
    /// Creates a new transaction info for an active transaction.
    pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            start_time: Instant::now(),
            end_time: None,
        }
    }

    /// Returns the duration for which the transaction has been running.
    pub fn duration(&self) -> Duration {
        match self.end_time {
            Some(end) => end.duration_since(self.start_time),
            None => self.start_time.elapsed(),
        }
    }

    /// Marks the transaction as committed.
    pub fn commit(&mut self) {
        assert!(
            self.state.is_active(),
            "Can only commit active transactions"
        );
        self.state = TransactionState::Committed;
        self.end_time = Some(Instant::now());
    }

    /// Marks the transaction as aborted.
    pub fn abort(&mut self) {
        assert!(self.state.is_active(), "Can only abort active transactions");
        self.state = TransactionState::Aborted;
        self.end_time = Some(Instant::now());
    }
}

/// A thread-safe wrapper for transaction information.
#[derive(Debug, Clone)]
pub struct SharedTransactionInfo {
    inner: Arc<Mutex<TransactionInfo>>,
}

impl SharedTransactionInfo {
    /// Creates a new shared transaction info.
    pub fn new(id: TransactionId) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionInfo::new(id))),
        }
    }

    /// Gets the transaction ID.
    pub fn id(&self) -> TransactionId {
        self.inner.lock().unwrap().id
    }

    /// Gets the current state of the transaction.
    pub fn state(&self) -> TransactionState {
        self.inner.lock().unwrap().state
    }

    /// Gets a copy of the transaction info.
    pub fn info(&self) -> TransactionInfo {
        self.inner.lock().unwrap().clone()
    }

    /// Commits the transaction.
    pub fn commit(&self) -> Result<(), String> {
        let mut info = self.inner.lock().unwrap();
        if !info.state.is_active() {
            return Err(format!("Transaction {} is not active", info.id));
        }
        info.commit();
        Ok(())
    }

    /// Aborts the transaction.
    pub fn abort(&self) -> Result<(), String> {
        let mut info = self.inner.lock().unwrap();
        if !info.state.is_active() {
            return Err(format!("Transaction {} is not active", info.id));
        }
        info.abort();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state() {
        let active = TransactionState::Active;
        let committed = TransactionState::Committed;
        let aborted = TransactionState::Aborted;

        assert!(active.is_active());
        assert!(!active.is_committed());
        assert!(!active.is_aborted());
        assert!(!active.is_finished());

        assert!(!committed.is_active());
        assert!(committed.is_committed());
        assert!(!committed.is_aborted());
        assert!(committed.is_finished());

        assert!(!aborted.is_active());
        assert!(!aborted.is_committed());
        assert!(aborted.is_aborted());
        assert!(aborted.is_finished());
    }

    #[test]
    fn test_transaction_state_display() {
        assert_eq!(format!("{}", TransactionState::Active), "Active");
        assert_eq!(format!("{}", TransactionState::Committed), "Committed");
        assert_eq!(format!("{}", TransactionState::Aborted), "Aborted");
    }

    #[test]
    fn test_transaction_info_new() {
        let id = TransactionId::new(1);
        let info = TransactionInfo::new(id);

        assert_eq!(info.id, id);
        assert_eq!(info.state, TransactionState::Active);
        assert!(info.end_time.is_none());
    }

    #[test]
    fn test_transaction_info_commit() {
        let id = TransactionId::new(1);
        let mut info = TransactionInfo::new(id);

        info.commit();
        assert_eq!(info.state, TransactionState::Committed);
        assert!(info.end_time.is_some());
    }

    #[test]
    fn test_transaction_info_abort() {
        let id = TransactionId::new(1);
        let mut info = TransactionInfo::new(id);

        info.abort();
        assert_eq!(info.state, TransactionState::Aborted);
        assert!(info.end_time.is_some());
    }

    #[test]
    fn test_transaction_info_duration() {
        let id = TransactionId::new(1);
        let mut info = TransactionInfo::new(id);

        // Sleep for a short time
        std::thread::sleep(Duration::from_millis(10));

        let duration1 = info.duration();
        assert!(duration1 >= Duration::from_millis(10));

        info.commit();
        let duration2 = info.duration();

        // After commit, duration should be fixed
        std::thread::sleep(Duration::from_millis(10));
        let duration3 = info.duration();
        assert_eq!(duration2, duration3);
    }

    #[test]
    fn test_shared_transaction_info() {
        let id = TransactionId::new(1);
        let shared = SharedTransactionInfo::new(id);

        assert_eq!(shared.id(), id);
        assert_eq!(shared.state(), TransactionState::Active);

        // Test commit
        assert!(shared.commit().is_ok());
        assert_eq!(shared.state(), TransactionState::Committed);

        // Can't commit again
        assert!(shared.commit().is_err());
    }

    #[test]
    fn test_shared_transaction_info_abort() {
        let id = TransactionId::new(2);
        let shared = SharedTransactionInfo::new(id);

        assert!(shared.abort().is_ok());
        assert_eq!(shared.state(), TransactionState::Aborted);

        // Can't abort again
        assert!(shared.abort().is_err());
    }

    #[test]
    fn test_shared_transaction_info_thread_safety() {
        use std::thread;

        let id = TransactionId::new(3);
        let shared = SharedTransactionInfo::new(id);

        let shared1 = shared.clone();
        let shared2 = shared.clone();

        let handle1 = thread::spawn(move || {
            // Try to commit from thread 1
            shared1.commit()
        });

        let handle2 = thread::spawn(move || {
            // Try to abort from thread 2
            std::thread::sleep(Duration::from_millis(1));
            shared2.abort()
        });

        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();

        // Only one should succeed
        assert!(result1.is_ok() != result2.is_ok());
        assert!(shared.state().is_finished());
    }
}
