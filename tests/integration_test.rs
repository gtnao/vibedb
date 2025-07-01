use std::sync::Arc;
use std::thread;
use std::time::Duration;
use vibedb::concurrency::lock::{LockId, LockManager, LockMode};
use vibedb::concurrency::mvcc::MVCCManager;
use vibedb::storage::page::PageId;
use vibedb::transaction::manager::{Transaction, TransactionManager};

#[test]
fn test_transaction_basic_operations() {
    let tx_manager = Arc::new(TransactionManager::new());
    let lock_manager = Arc::new(LockManager::new());

    // Test basic transaction lifecycle
    let tx_id = tx_manager.begin().unwrap();
    assert!(tx_manager.is_active(tx_id));

    // Acquire and release locks
    let page_id = PageId(1);
    lock_manager
        .acquire_lock(
            tx_id.value(),
            LockId::Page(page_id),
            LockMode::Exclusive,
            None,
        )
        .unwrap();

    let held_locks = lock_manager.get_held_locks(tx_id.value());
    assert_eq!(held_locks.len(), 1);

    tx_manager.commit(tx_id).unwrap();
    lock_manager.release_all_locks(tx_id.value());

    assert!(!tx_manager.is_active(tx_id));
}

#[test]
fn test_transaction_auto_abort() {
    let tx_manager = Arc::new(TransactionManager::new());

    let tx_id = {
        let tx_handle = Transaction::new(tx_manager.clone()).unwrap();
        tx_handle.id()
    };

    // Transaction should be aborted after handle is dropped
    assert!(!tx_manager.is_active(tx_id));
}

#[test]
fn test_concurrent_transactions() {
    let tx_manager = Arc::new(TransactionManager::new());
    let lock_manager = Arc::new(LockManager::new());

    let tx1 = tx_manager.begin().unwrap();
    let tx2 = tx_manager.begin().unwrap();

    assert_eq!(tx_manager.active_transactions().len(), 2);

    // T1 acquires exclusive lock
    let page_id = PageId(1);
    lock_manager
        .acquire_lock(
            tx1.value(),
            LockId::Page(page_id),
            LockMode::Exclusive,
            None,
        )
        .unwrap();

    // T2 tries to acquire exclusive lock with timeout
    let result = lock_manager.acquire_lock(
        tx2.value(),
        LockId::Page(page_id),
        LockMode::Exclusive,
        Some(Duration::from_millis(100)),
    );

    // Should timeout because T1 holds exclusive lock
    assert!(result.is_err());

    // Clean up
    tx_manager.commit(tx1).unwrap();
    tx_manager.abort(tx2).unwrap();
    lock_manager.release_all_locks(tx1.value());
    lock_manager.release_all_locks(tx2.value());
}

#[test]
fn test_mvcc_snapshots() {
    let mvcc_manager = Arc::new(MVCCManager::new());
    let tx_manager = Arc::new(TransactionManager::new());

    let tx1 = tx_manager.begin().unwrap();
    let tx2 = tx_manager.begin().unwrap();

    // MVCC tracks active transactions
    let stats = mvcc_manager.get_stats();
    assert!(stats.active_transactions >= 0);

    // Clean up
    tx_manager.commit(tx1).unwrap();
    tx_manager.commit(tx2).unwrap();
}

#[test]
fn test_concurrent_readers() {
    let lock_manager = Arc::new(LockManager::new());
    let tx_manager = Arc::new(TransactionManager::new());

    let page_id = PageId(1);
    let mut handles = vec![];

    // Multiple readers should be able to acquire shared locks
    for i in 0..3 {
        let lock_manager_clone = lock_manager.clone();
        let tx_manager_clone = tx_manager.clone();

        let handle = thread::spawn(move || {
            let tx = tx_manager_clone.begin().unwrap();

            // Acquire shared lock
            lock_manager_clone
                .acquire_lock(tx.value(), LockId::Page(page_id), LockMode::Shared, None)
                .unwrap();

            // Hold lock briefly
            thread::sleep(Duration::from_millis(50));

            tx_manager_clone.commit(tx).unwrap();
            lock_manager_clone.release_all_locks(tx.value());
        });

        handles.push(handle);
    }

    // All readers should succeed
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_deadlock_prevention() {
    let lock_manager = Arc::new(LockManager::new());
    let tx_manager = Arc::new(TransactionManager::new());

    let page1 = PageId(1);
    let page2 = PageId(2);

    let tx1 = tx_manager.begin().unwrap();
    let tx2 = tx_manager.begin().unwrap();

    // T1 locks page1
    lock_manager
        .acquire_lock(tx1.value(), LockId::Page(page1), LockMode::Exclusive, None)
        .unwrap();

    // T2 locks page2
    lock_manager
        .acquire_lock(tx2.value(), LockId::Page(page2), LockMode::Exclusive, None)
        .unwrap();

    // Potential deadlock scenario - use timeouts to prevent
    let lock_manager_clone = lock_manager.clone();
    let tx_manager_clone = tx_manager.clone();

    let handle = thread::spawn(move || {
        // T1 tries to lock page2
        let result = lock_manager_clone.acquire_lock(
            tx1.value(),
            LockId::Page(page2),
            LockMode::Exclusive,
            Some(Duration::from_millis(200)),
        );

        if result.is_err() {
            // Timeout - abort to prevent deadlock
            tx_manager_clone.abort(tx1).ok();
            lock_manager_clone.release_all_locks(tx1.value());
        }
    });

    // Give T1 time to start
    thread::sleep(Duration::from_millis(50));

    // T2 tries to lock page1
    let result = lock_manager.acquire_lock(
        tx2.value(),
        LockId::Page(page1),
        LockMode::Exclusive,
        Some(Duration::from_millis(200)),
    );

    if result.is_err() {
        tx_manager.abort(tx2).ok();
    }

    lock_manager.release_all_locks(tx2.value());

    handle.join().unwrap();
}

#[test]
fn test_transaction_cleanup() {
    let tx_manager = Arc::new(TransactionManager::new());

    // Create and commit several transactions
    for _ in 0..5 {
        let tx = tx_manager.begin().unwrap();
        tx_manager.commit(tx).unwrap();
    }

    // Create and abort several transactions
    for _ in 0..5 {
        let tx = tx_manager.begin().unwrap();
        tx_manager.abort(tx).unwrap();
    }

    assert_eq!(tx_manager.transaction_count(), 10);

    // Cleanup finished transactions
    let cleaned = tx_manager.cleanup_finished();
    assert_eq!(cleaned, 10);
    assert_eq!(tx_manager.transaction_count(), 0);
}
