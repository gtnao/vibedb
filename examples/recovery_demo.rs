use std::path::Path;
use std::sync::Arc;
use vibedb::recovery::aries::{AriesRecovery, RecoveryError};
use vibedb::recovery::checkpoint::{CheckpointManager, FuzzyCheckpoint};
use vibedb::recovery::log_record::RecoveryLogRecord;
use vibedb::storage::buffer::{BufferPoolManager, LRUReplacer, Replacer};
use vibedb::storage::disk::PageManager;
use vibedb::storage::page::PageId;
use vibedb::storage::wal::manager::{WalConfig, WalManager};
use vibedb::transaction::id::TransactionId;
use vibedb::transaction::manager::TransactionManager;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Recovery Demonstration ===\n");

    // Clean up any existing files
    std::fs::remove_file("recovery_demo.db").ok();
    std::fs::remove_file("recovery_demo.wal").ok();

    // Example 1: Basic WAL and Recovery
    println!("Example 1: Basic WAL and Recovery");
    basic_wal_demo()?;
    println!();

    // Example 2: Checkpoint and Recovery
    println!("Example 2: Checkpoint and Recovery");
    checkpoint_demo()?;
    println!();

    // Example 3: ARIES Recovery Algorithm
    println!("Example 3: ARIES Recovery Algorithm");
    aries_recovery_demo()?;

    // Clean up
    std::fs::remove_file("recovery_demo.db").ok();
    std::fs::remove_file("recovery_demo.wal").ok();

    Ok(())
}

fn basic_wal_demo() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize components
    let wal_config = WalConfig {
        wal_dir: Path::new(".").to_path_buf(),
        max_file_size: 1024 * 1024, // 1MB
        sync_on_commit: true,
    };
    let wal_manager = Arc::new(WalManager::new(wal_config)?);
    let tx_manager = Arc::new(TransactionManager::new());

    // Simulate some transactions
    println!("Writing transactions to WAL...");

    // Transaction 1
    let tx1 = tx_manager.begin()?;
    let log1 = RecoveryLogRecord::Begin { tx_id: tx1 };
    let lsn1 = wal_manager.append(log1.into())?;
    println!("T1: Begin logged at LSN {}", lsn1);

    let update1 = RecoveryLogRecord::Update {
        tx_id: tx1,
        page_id: PageId(1),
        offset: 0,
        old_data: vec![],
        new_data: vec![1, 2, 3, 4, 5],
    };
    let lsn2 = wal_manager.append(update1.into())?;
    println!("T1: Update logged at LSN {}", lsn2);

    let commit1 = RecoveryLogRecord::Commit { tx_id: tx1 };
    let lsn3 = wal_manager.append(commit1.into())?;
    println!("T1: Commit logged at LSN {}", lsn3);
    tx_manager.commit(tx1)?;

    // Transaction 2 (will be incomplete)
    let tx2 = tx_manager.begin()?;
    let log2 = RecoveryLogRecord::Begin { tx_id: tx2 };
    let lsn4 = wal_manager.append(log2.into())?;
    println!("T2: Begin logged at LSN {}", lsn4);

    let update2 = RecoveryLogRecord::Update {
        tx_id: tx2,
        page_id: PageId(2),
        offset: 0,
        old_data: vec![],
        new_data: vec![10, 20, 30, 40, 50],
    };
    let lsn5 = wal_manager.append(update2.into())?;
    println!(
        "T2: Update logged at LSN {} (no commit - simulating crash)",
        lsn5
    );

    // Flush WAL
    wal_manager.flush()?;
    println!("\nWAL flushed. Simulating crash...");

    // Drop WAL manager to simulate crash
    drop(wal_manager);

    // Recovery: Read WAL and show what would be recovered
    println!("\nRecovery phase - reading WAL:");
    let wal_config_recovery = WalConfig {
        wal_dir: Path::new(".").to_path_buf(),
        max_file_size: 1024 * 1024,
        sync_on_commit: true,
    };
    let wal_manager_recovery = WalManager::new(wal_config_recovery)?;

    // In a real system, we would replay the log here
    println!("Would replay:");
    println!("- T1: Complete (Begin, Update, Commit) - REDO");
    println!("- T2: Incomplete (Begin, Update, no Commit) - UNDO");

    Ok(())
}

fn checkpoint_demo() -> Result<(), Box<dyn std::error::Error>> {
    // Clean up
    std::fs::remove_file("recovery_demo.db").ok();
    std::fs::remove_file("recovery_demo.wal").ok();

    // Initialize components
    let disk_manager = PageManager::create(Path::new("recovery_demo.db"))?;
    let replacer: Box<dyn Replacer> = Box::new(LRUReplacer::new(10));
    let buffer_pool = Arc::new(BufferPoolManager::new(disk_manager, replacer, 10));

    let wal_config = WalConfig {
        wal_dir: Path::new(".").to_path_buf(),
        max_file_size: 1024 * 1024,
        sync_on_commit: true,
    };
    let wal_manager = Arc::new(WalManager::new(wal_config)?);
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        wal_manager.clone(),
        buffer_pool.clone(),
    ));
    let tx_manager = Arc::new(TransactionManager::new());

    // Create some transactions
    println!("Creating transactions before checkpoint...");
    for i in 1..=3 {
        let tx = tx_manager.begin()?;
        let log = RecoveryLogRecord::Update {
            tx_id: tx,
            page_id: PageId(i),
            offset: 0,
            old_data: vec![],
            new_data: vec![i as u8; 5],
        };
        wal_manager.append(log.into())?;
        tx_manager.commit(tx)?;
        wal_manager.append(RecoveryLogRecord::Commit { tx_id: tx }.into())?;
        println!("Transaction {} committed", i);
    }

    // Take a checkpoint
    println!("\nTaking fuzzy checkpoint...");
    let checkpoint = checkpoint_manager.take_checkpoint()?;
    match checkpoint {
        FuzzyCheckpoint {
            active_txns,
            dirty_pages,
            checkpoint_lsn,
        } => {
            println!("Checkpoint taken at LSN {}", checkpoint_lsn);
            println!("Active transactions: {:?}", active_txns);
            println!("Dirty pages: {} pages", dirty_pages.len());
        }
    }

    // More transactions after checkpoint
    println!("\nCreating transactions after checkpoint...");
    for i in 4..=6 {
        let tx = tx_manager.begin()?;
        let log = RecoveryLogRecord::Update {
            tx_id: tx,
            page_id: PageId(i),
            offset: 0,
            old_data: vec![],
            new_data: vec![i as u8; 5],
        };
        wal_manager.append(log.into())?;
        tx_manager.commit(tx)?;
        wal_manager.append(RecoveryLogRecord::Commit { tx_id: tx }.into())?;
        println!("Transaction {} committed", i);
    }

    println!("\nRecovery would start from checkpoint, not beginning of log");
    println!("This reduces recovery time significantly!");

    Ok(())
}

fn aries_recovery_demo() -> Result<(), Box<dyn std::error::Error>> {
    // Clean up
    std::fs::remove_file("recovery_demo.db").ok();
    std::fs::remove_file("recovery_demo.wal").ok();

    // Initialize components for creating scenario
    let disk_manager = PageManager::create(Path::new("recovery_demo.db"))?;
    let replacer: Box<dyn Replacer> = Box::new(LRUReplacer::new(10));
    let buffer_pool = Arc::new(BufferPoolManager::new(disk_manager, replacer, 10));

    let wal_config = WalConfig {
        wal_dir: Path::new(".").to_path_buf(),
        max_file_size: 1024 * 1024,
        sync_on_commit: true,
    };
    let wal_manager = Arc::new(WalManager::new(wal_config)?);
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        wal_manager.clone(),
        buffer_pool.clone(),
    ));
    let tx_manager = Arc::new(TransactionManager::new());

    println!("Creating complex scenario for ARIES recovery...");

    // Transaction states:
    // T1: Committed before checkpoint
    // T2: Started before checkpoint, committed after
    // T3: Started after checkpoint, committed
    // T4: Started after checkpoint, not committed

    // T1: Committed before checkpoint
    let tx1 = tx_manager.begin()?;
    wal_manager.append(RecoveryLogRecord::Begin { tx_id: tx1 }.into())?;
    wal_manager.append(
        RecoveryLogRecord::Update {
            tx_id: tx1,
            page_id: PageId(1),
            offset: 0,
            old_data: vec![],
            new_data: vec![1; 5],
        }
        .into(),
    )?;
    tx_manager.commit(tx1)?;
    wal_manager.append(RecoveryLogRecord::Commit { tx_id: tx1 }.into())?;
    println!("T1: Committed before checkpoint");

    // T2: Started before checkpoint
    let tx2 = tx_manager.begin()?;
    wal_manager.append(RecoveryLogRecord::Begin { tx_id: tx2 }.into())?;
    wal_manager.append(
        RecoveryLogRecord::Update {
            tx_id: tx2,
            page_id: PageId(2),
            offset: 0,
            old_data: vec![],
            new_data: vec![2; 5],
        }
        .into(),
    )?;
    println!("T2: Started, not yet committed");

    // Take checkpoint
    println!("\nTaking checkpoint...");
    checkpoint_manager.take_checkpoint()?;

    // T2: Commit after checkpoint
    tx_manager.commit(tx2)?;
    wal_manager.append(RecoveryLogRecord::Commit { tx_id: tx2 }.into())?;
    println!("T2: Committed after checkpoint");

    // T3: Started and committed after checkpoint
    let tx3 = tx_manager.begin()?;
    wal_manager.append(RecoveryLogRecord::Begin { tx_id: tx3 }.into())?;
    wal_manager.append(
        RecoveryLogRecord::Update {
            tx_id: tx3,
            page_id: PageId(3),
            offset: 0,
            old_data: vec![],
            new_data: vec![3; 5],
        }
        .into(),
    )?;
    tx_manager.commit(tx3)?;
    wal_manager.append(RecoveryLogRecord::Commit { tx_id: tx3 }.into())?;
    println!("T3: Started and committed after checkpoint");

    // T4: Started but not committed
    let tx4 = tx_manager.begin()?;
    wal_manager.append(RecoveryLogRecord::Begin { tx_id: tx4 }.into())?;
    wal_manager.append(
        RecoveryLogRecord::Update {
            tx_id: tx4,
            page_id: PageId(4),
            offset: 0,
            old_data: vec![],
            new_data: vec![4; 5],
        }
        .into(),
    )?;
    println!("T4: Started but not committed (simulating crash)");

    // Flush and simulate crash
    wal_manager.flush()?;
    println!("\nCRASH! System going down...");

    // Drop everything to simulate crash
    drop(wal_manager);
    drop(buffer_pool);
    drop(checkpoint_manager);
    drop(tx_manager);

    // Recovery phase
    println!("\n=== ARIES Recovery ===");

    // Re-initialize for recovery
    let disk_manager_recovery = PageManager::open(Path::new("recovery_demo.db"))?;
    let replacer_recovery: Box<dyn Replacer> = Box::new(LRUReplacer::new(10));
    let buffer_pool_recovery = Arc::new(BufferPoolManager::new(
        disk_manager_recovery,
        replacer_recovery,
        10,
    ));

    let wal_config_recovery = WalConfig {
        wal_dir: Path::new(".").to_path_buf(),
        max_file_size: 1024 * 1024,
        sync_on_commit: true,
    };
    let wal_manager_recovery = Arc::new(WalManager::new(wal_config_recovery)?);
    let checkpoint_manager_recovery = Arc::new(CheckpointManager::new(
        wal_manager_recovery.clone(),
        buffer_pool_recovery.clone(),
    ));
    let tx_manager_recovery = Arc::new(TransactionManager::new());

    let recovery_manager = Arc::new(AriesRecovery::new(
        wal_manager_recovery.clone(),
        buffer_pool_recovery.clone(),
        checkpoint_manager_recovery.clone(),
        tx_manager_recovery.clone(),
    ));

    println!("Running ARIES recovery algorithm...");
    match recovery_manager.recover() {
        Ok(()) => {
            println!("\nRecovery completed successfully!");
            println!("Recovery phases executed:");
            println!("1. Analysis: Built transaction table and dirty page table");
            println!("2. Redo: Replayed all updates from checkpoint");
            println!("3. Undo: Rolled back incomplete transactions");
            println!("\nFinal state:");
            println!("- T1: Already committed (no action needed)");
            println!("- T2: Committed (redo applied)");
            println!("- T3: Committed (redo applied)");
            println!("- T4: Uncommitted (undo applied)");
        }
        Err(e) => {
            println!("Recovery failed: {:?}", e);
        }
    }

    Ok(())
}
