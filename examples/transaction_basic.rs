use std::sync::Arc;
use vibedb::concurrency::lock::{LockId, LockManager, LockMode};
use vibedb::concurrency::mvcc::MVCCManager;
use vibedb::storage::page::PageId;
use vibedb::transaction::manager::{Transaction, TransactionManager};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Basic Transaction Example ===\n");

    // Initialize components
    let lock_manager = Arc::new(LockManager::new());
    let mvcc_manager = Arc::new(MVCCManager::new());
    let tx_manager = Arc::new(TransactionManager::new());

    // Example 1: Simple transaction with commit
    println!("Example 1: Simple transaction with commit");
    {
        // Begin transaction
        let tx_id = tx_manager.begin()?;
        println!("Started transaction {}", tx_id);

        // Simulate some work with locks
        let page_id = PageId(1);
        lock_manager.acquire_lock(tx_id.0, LockId::Page(page_id), LockMode::Exclusive, None)?;
        println!("Acquired exclusive lock on page {:?}", page_id);

        // Commit transaction
        tx_manager.commit(tx_id)?;
        println!("Transaction {} committed successfully", tx_id);

        // Release locks
        lock_manager.release_all_locks(tx_id.0);
        println!("Released all locks for transaction {}\n", tx_id);
    }

    // Example 2: Transaction with rollback
    println!("Example 2: Transaction with rollback");
    {
        // Begin transaction
        let tx_id = tx_manager.begin()?;
        println!("Started transaction {}", tx_id);

        let page_id = PageId(2);
        lock_manager.acquire_lock(tx_id.0, LockId::Page(page_id), LockMode::Exclusive, None)?;
        println!("Acquired lock on page {:?}", page_id);

        // Abort transaction
        tx_manager.abort(tx_id)?;
        println!("Transaction {} aborted", tx_id);

        // Release locks
        lock_manager.release_all_locks(tx_id.0);
        println!("Released all locks\n");
    }

    // Example 3: Multiple concurrent transactions
    println!("Example 3: Multiple concurrent transactions");
    {
        // Start multiple transactions
        let tx1 = tx_manager.begin()?;
        println!("Started transaction {}", tx1);

        let tx2 = tx_manager.begin()?;
        println!("Started transaction {}", tx2);

        let tx3 = tx_manager.begin()?;
        println!("Started transaction {}", tx3);

        // Show active transactions
        println!(
            "Active transactions: {:?}",
            tx_manager.active_transactions()
        );

        // Commit them in different order
        tx_manager.commit(tx2)?;
        println!("Transaction {} committed", tx2);

        tx_manager.abort(tx1)?;
        println!("Transaction {} aborted", tx1);

        tx_manager.commit(tx3)?;
        println!("Transaction {} committed", tx3);

        // Clean up locks
        lock_manager.release_all_locks(tx1.0);
        lock_manager.release_all_locks(tx2.0);
        lock_manager.release_all_locks(tx3.0);

        println!(
            "Active transactions after cleanup: {:?}\n",
            tx_manager.active_transactions()
        );
    }

    // Example 4: Transaction handle with auto-abort
    println!("Example 4: Transaction handle with auto-abort");
    {
        let tx_manager_arc = tx_manager.clone();

        // This transaction will auto-abort when dropped
        {
            let tx_handle = Transaction::new(tx_manager_arc.clone())?;
            println!("Started transaction {} with handle", tx_handle.id());
            println!("Transaction handle going out of scope...");
            // Transaction auto-aborts here
        }

        // This transaction will be explicitly committed
        let tx_handle = Transaction::new(tx_manager_arc)?;
        let tx_id = tx_handle.id();
        println!("Started transaction {} with handle", tx_id);
        tx_handle.commit()?;
        println!("Transaction {} explicitly committed\n", tx_id);
    }

    // Example 5: Transaction states
    println!("Example 5: Transaction states");
    {
        let tx = tx_manager.begin()?;
        println!("Transaction {} started", tx);

        // Acquire some locks
        lock_manager.acquire_lock(tx.0, LockId::Page(PageId(10)), LockMode::Shared, None)?;
        lock_manager.acquire_lock(tx.0, LockId::Page(PageId(11)), LockMode::Exclusive, None)?;

        // Check held locks
        println!("Transaction {} acquired locks", tx);

        tx_manager.commit(tx)?;
        println!("Transaction {} committed", tx);

        lock_manager.release_all_locks(tx.0);
    }

    // Show final statistics
    println!("\n=== Transaction Statistics ===");
    println!("Total transactions: {}", tx_manager.transaction_count());
    println!(
        "Active transactions: {}",
        tx_manager.active_transactions().len()
    );

    // Clean up finished transactions
    let cleaned = tx_manager.cleanup_finished();
    println!("Cleaned up {} finished transactions", cleaned);

    // Show MVCC statistics
    println!("\n=== MVCC Statistics ===");
    // Note: get_active_transactions is not part of the public API
    println!("MVCC manager initialized");
    println!("Transaction demo completed successfully!");

    Ok(())
}
