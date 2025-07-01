use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use vibedb::concurrency::lock::{LockId, LockManager, LockMode};
use vibedb::concurrency::mvcc::MVCCManager;
use vibedb::concurrency::version::VersionManager;
use vibedb::storage::page::PageId;
use vibedb::transaction::manager::TransactionManager;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== MVCC Demonstration ===\n");

    // Initialize components
    let lock_manager = Arc::new(LockManager::new());
    let mvcc_manager = Arc::new(MVCCManager::new());
    let version_manager = Arc::new(VersionManager::new());
    let tx_manager = Arc::new(TransactionManager::new());

    // Example 1: Basic MVCC - Snapshot Isolation
    println!("Example 1: Snapshot Isolation");
    {
        // Transaction 1 starts
        let tx1 = tx_manager.begin()?;
        println!("T1 ({}): Started", tx1);

        // Get snapshot for T1
        let snapshot1 = mvcc_manager.get_snapshot(tx1);
        println!("T1: Got snapshot {:?}", snapshot1);

        // Transaction 2 starts
        let tx2 = tx_manager.begin()?;
        println!("T2 ({}): Started", tx2);

        // T2 creates a version and commits
        let page_id = PageId(1);
        version_manager.create_version(tx2, page_id, vec![1, 2, 3])?;
        println!("T2: Created version for page {:?}", page_id);

        tx_manager.commit(tx2)?;
        println!("T2: Committed");

        // T1 still sees its original snapshot
        println!("T1: Still working with snapshot {:?}", snapshot1);

        // T3 starts after T2 commits
        let tx3 = tx_manager.begin()?;
        let snapshot3 = mvcc_manager.get_snapshot(tx3);
        println!("T3 ({}): Started with snapshot {:?}", tx3, snapshot3);

        // Clean up
        tx_manager.commit(tx1)?;
        tx_manager.commit(tx3)?;
        println!();
    }

    // Example 2: Write-Write Conflict with Locks
    println!("Example 2: Write-Write Conflict");
    {
        let tx1 = tx_manager.begin()?;
        let tx2 = tx_manager.begin()?;
        println!("Started T1 ({}) and T2 ({})", tx1, tx2);

        let page_id = PageId(2);

        // T1 acquires exclusive lock
        println!("T1: Acquiring exclusive lock on page {:?}", page_id);
        lock_manager.acquire_lock(tx1, LockId::Page(page_id), LockMode::Exclusive, None)?;
        println!("T1: Got exclusive lock");

        // T2 tries to acquire exclusive lock - will timeout
        println!(
            "T2: Attempting to acquire exclusive lock on page {:?}",
            page_id
        );
        let tx2_clone = tx2;
        let lock_manager_clone = lock_manager.clone();
        let page_id_clone = page_id;

        let handle = thread::spawn(move || {
            match lock_manager_clone.acquire_lock(
                tx2_clone,
                LockId::Page(page_id_clone),
                LockMode::Exclusive,
                Some(Duration::from_millis(100)),
            ) {
                Ok(_) => println!("T2: Got exclusive lock"),
                Err(_) => println!("T2: Lock acquisition timed out"),
            }
        });

        // Give T2 time to attempt lock
        thread::sleep(Duration::from_millis(50));

        // T1 commits and releases lock
        version_manager.create_version(tx1, page_id, vec![10, 20, 30])?;
        tx_manager.commit(tx1)?;
        lock_manager.release_all_locks(tx1)?;
        println!("T1: Committed and released locks");

        handle.join().unwrap();
        tx_manager.abort(tx2)?;
        lock_manager.release_all_locks(tx2)?;
        println!();
    }

    // Example 3: Concurrent Readers
    println!("Example 3: Concurrent Readers");
    {
        let page_id = PageId(3);

        // Create initial version
        let tx0 = tx_manager.begin()?;
        version_manager.create_version(tx0, page_id, vec![100, 200, 300])?;
        tx_manager.commit(tx0)?;
        println!("Initial version created and committed");

        // Multiple readers
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];

        for i in 1..=3 {
            let tx_manager_clone = tx_manager.clone();
            let lock_manager_clone = lock_manager.clone();
            let version_manager_clone = version_manager.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let tx = tx_manager_clone.begin()?;
                    println!("Reader {}: Started transaction {}", i, tx);

                    // Acquire shared lock
                    lock_manager_clone.acquire_lock(
                        tx,
                        LockId::Page(page_id),
                        LockMode::Shared,
                        None,
                    )?;
                    println!("Reader {}: Acquired shared lock", i);

                    // Wait for all readers
                    barrier_clone.wait();

                    // Read version
                    match version_manager_clone.get_version(tx, page_id) {
                        Ok(version) => println!("Reader {}: Read version {}", i, version),
                        Err(_) => println!("Reader {}: No visible version", i),
                    }

                    // Simulate work
                    thread::sleep(Duration::from_millis(50));

                    tx_manager_clone.commit(tx)?;
                    lock_manager_clone.release_all_locks(tx)?;
                    println!("Reader {}: Committed", i);
                    Ok(())
                },
            );

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap()?;
        }
        println!();
    }

    // Example 4: Version Chain
    println!("Example 4: Version Chain");
    {
        let page_id = PageId(4);

        // Create multiple versions
        for i in 1..=3 {
            let tx = tx_manager.begin()?;
            version_manager.create_version(tx, page_id, vec![i; 3])?;
            tx_manager.commit(tx)?;
            println!("Created version {} for page {:?}", i, page_id);
        }

        // Show version chain
        let versions = version_manager.get_version_chain(page_id);
        println!(
            "Version chain for page {:?}: {} versions",
            page_id,
            versions.len()
        );

        // Clean old versions
        version_manager.cleanup_old_versions(page_id)?;
        let versions_after = version_manager.get_version_chain(page_id);
        println!("After cleanup: {} versions\n", versions_after.len());
    }

    // Example 5: Deadlock Prevention
    println!("Example 5: Deadlock Prevention");
    {
        let page1 = PageId(5);
        let page2 = PageId(6);

        let tx1 = tx_manager.begin()?;
        let tx2 = tx_manager.begin()?;
        println!("Started T1 ({}) and T2 ({})", tx1, tx2);

        // T1 locks page1
        lock_manager.acquire_lock(tx1, LockId::Page(page1), LockMode::Exclusive, None)?;
        println!("T1: Locked page {:?}", page1);

        // T2 locks page2
        lock_manager.acquire_lock(tx2, LockId::Page(page2), LockMode::Exclusive, None)?;
        println!("T2: Locked page {:?}", page2);

        // Potential deadlock scenario
        let tx_manager_clone1 = tx_manager.clone();
        let lock_manager_clone1 = lock_manager.clone();

        let handle1 = thread::spawn(move || {
            println!("T1: Attempting to lock page {:?}", page2);
            match lock_manager_clone1.acquire_lock(
                tx1,
                LockId::Page(page2),
                LockMode::Exclusive,
                Some(Duration::from_millis(500)),
            ) {
                Ok(_) => {
                    println!("T1: Got lock on page {:?}", page2);
                    lock_manager_clone1.release_all_locks(tx1).ok();
                }
                Err(_) => {
                    println!("T1: Lock timeout - aborting to prevent deadlock");
                    tx_manager_clone1.abort(tx1).ok();
                    lock_manager_clone1.release_all_locks(tx1).ok();
                }
            }
        });

        // Give T1 time to start
        thread::sleep(Duration::from_millis(50));

        println!("T2: Attempting to lock page {:?}", page1);
        match lock_manager.acquire_lock(
            tx2,
            LockId::Page(page1),
            LockMode::Exclusive,
            Some(Duration::from_millis(500)),
        ) {
            Ok(_) => {
                println!("T2: Got lock on page {:?}", page1);
                lock_manager.release_all_locks(tx2)?;
            }
            Err(_) => {
                println!("T2: Lock timeout");
                tx_manager.abort(tx2)?;
                lock_manager.release_all_locks(tx2)?;
            }
        }

        handle1.join().unwrap();
        println!();
    }

    // Example 6: MVCC Garbage Collection
    println!("Example 6: MVCC Garbage Collection");
    {
        // Create many versions
        let page_id = PageId(7);
        println!("Creating multiple versions...");

        for i in 0..5 {
            let tx = tx_manager.begin()?;
            version_manager.create_version(tx, page_id, vec![i; 3])?;
            tx_manager.commit(tx)?;
        }

        let stats_before = mvcc_manager.get_stats();
        println!("Before GC: {} total versions", stats_before.total_versions);

        // Run garbage collection
        println!("Running garbage collection...");
        mvcc_manager.garbage_collect();

        let stats_after = mvcc_manager.get_stats();
        println!("After GC: {} total versions", stats_after.total_versions);
        println!();
    }

    // Show final statistics
    println!("=== MVCC Statistics ===");
    let stats = mvcc_manager.get_stats();
    println!("Active transactions: {}", stats.active_transactions);
    println!("Total transactions: {}", stats.total_transactions);
    println!("Version chains: {}", stats.version_chains);
    println!("Total versions: {}", stats.total_versions);

    Ok(())
}
