# VibeDB Examples

This directory contains example programs demonstrating various features of VibeDB.

## Available Examples

### 1. Transaction Basic (`transaction_basic.rs`)
Demonstrates basic transaction operations including:
- Transaction lifecycle (begin, commit, abort)
- Lock acquisition and release
- Transaction handles with auto-abort
- Transaction state management
- MVCC statistics

Run with: `cargo run --example transaction_basic`

### 2. MVCC Demo (`mvcc_demo.rs`)
Demonstrates Multi-Version Concurrency Control (MVCC) features:
- Snapshot isolation
- Write-write conflict handling
- Concurrent readers with shared locks
- Version chains
- Deadlock prevention
- Garbage collection

Run with: `cargo run --example mvcc_demo`

### 3. Recovery Demo (`recovery_demo.rs`)
Demonstrates recovery and crash resilience features:
- Write-Ahead Logging (WAL)
- Checkpoint creation and recovery
- ARIES recovery algorithm
- Transaction recovery after crash
- Handling of committed and uncommitted transactions

Run with: `cargo run --example recovery_demo`

### 4. B+Tree Index (`btree_index.rs`)
Demonstrates B+Tree index operations:
- Index creation and management
- Key insertion and deletion
- Range queries and prefix scans
- Concurrent access with latching
- Index statistics

Run with: `cargo run --example btree_index`

### 5. Executor Demo (`executor_demo.rs`)
Demonstrates query execution:
- Table scans
- Projections and filters
- Hash joins
- Sorting and limiting
- Query plan construction

Run with: `cargo run --example executor_demo`

### 6. System Tables (`system_tables.rs`)
Demonstrates system catalog usage:
- Creating and managing tables
- Column definitions
- Index management
- Metadata queries

Run with: `cargo run --example system_tables`

## Integration Tests

The `tests/integration_test.rs` file contains integration tests that demonstrate:
- Transaction isolation
- Concurrent access patterns
- Deadlock prevention
- Auto-abort functionality
- Resource cleanup

Run tests with: `cargo test`

## Key Concepts Demonstrated

### Transactions
- ACID properties
- Isolation levels
- Lock management
- Auto-abort on drop

### Concurrency Control
- Two-phase locking (2PL)
- Multi-Version Concurrency Control (MVCC)
- Deadlock prevention with timeouts
- Reader-writer locks

### Recovery
- Write-Ahead Logging (WAL)
- ARIES recovery algorithm
- Fuzzy checkpointing
- Crash recovery

### Storage
- Page-based storage
- Buffer pool management
- B+Tree indexes
- Efficient data structures

## Notes

- Examples use simplified APIs and may not demonstrate all production features
- File cleanup is handled automatically in most examples
- Examples are designed to be self-contained and runnable independently
- Check the source code comments for detailed explanations