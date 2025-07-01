//! Transaction ID generation and management.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// A unique identifier for a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub u64);

impl TransactionId {
    /// Creates a new transaction ID with the given value.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the inner u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Txn{}", self.0)
    }
}

/// A thread-safe transaction ID generator.
pub struct TransactionIdGenerator {
    next_id: AtomicU64,
}

impl TransactionIdGenerator {
    /// Creates a new transaction ID generator starting from 1.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    /// Generates the next unique transaction ID.
    pub fn next(&self) -> TransactionId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        TransactionId::new(id)
    }

    /// Returns the current value without incrementing.
    pub fn current(&self) -> TransactionId {
        let id = self.next_id.load(Ordering::SeqCst);
        TransactionId::new(id.saturating_sub(1))
    }
}

impl Default for TransactionIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_new() {
        let id = TransactionId::new(42);
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_transaction_id_display() {
        let id = TransactionId::new(123);
        assert_eq!(format!("{}", id), "Txn123");
    }

    #[test]
    fn test_transaction_id_comparison() {
        let id1 = TransactionId::new(1);
        let id2 = TransactionId::new(2);
        let id3 = TransactionId::new(1);

        assert!(id1 < id2);
        assert!(id1 == id3);
        assert!(id2 > id1);
    }

    #[test]
    fn test_transaction_id_generator() {
        let generator = TransactionIdGenerator::new();

        let id1 = generator.next();
        let id2 = generator.next();
        let id3 = generator.next();

        assert_eq!(id1.value(), 1);
        assert_eq!(id2.value(), 2);
        assert_eq!(id3.value(), 3);
    }

    #[test]
    fn test_transaction_id_generator_current() {
        let generator = TransactionIdGenerator::new();

        // Before any IDs are generated, current should be 0
        assert_eq!(generator.current().value(), 0);

        let _id1 = generator.next();
        assert_eq!(generator.current().value(), 1);

        let _id2 = generator.next();
        assert_eq!(generator.current().value(), 2);
    }

    #[test]
    fn test_transaction_id_generator_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let generator = Arc::new(TransactionIdGenerator::new());
        let mut handles = vec![];

        // Spawn 10 threads, each generating 100 IDs
        for _ in 0..10 {
            let gen = Arc::clone(&generator);
            let handle = thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(gen.next());
                }
                ids
            });
            handles.push(handle);
        }

        // Collect all generated IDs
        let mut all_ids = vec![];
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // Verify that all IDs are unique
        let mut unique_ids: Vec<_> = all_ids.iter().map(|id| id.value()).collect();
        unique_ids.sort();
        unique_ids.dedup();

        assert_eq!(all_ids.len(), 1000);
        assert_eq!(unique_ids.len(), 1000);
    }
}
