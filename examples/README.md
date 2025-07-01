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

### 7. CRUD Operations (`crud_operations.rs`)
Comprehensive demonstration of Create, Read, Update, Delete operations:
- Creating tables with indexes
- INSERT operations with multiple rows
- SELECT queries with various filters
- UPDATE with conditions (deactivating users, changing values)
- DELETE operations with filters
- Transaction rollback demonstration
- Working with indexes for fast lookups

Run with: `cargo run --example crud_operations`

### 8. Query Operators Demo (`query_operators_demo.rs`)
Showcases how query operators work together:
- Sequential table scans
- Filter operations with WHERE-like conditions
- Projection for column selection
- Sorting with single and multiple columns
- Limiting result sets (TOP N queries)
- Complex query pipelines combining multiple operators
- Real-world query patterns

Run with: `cargo run --example query_operators_demo`

### 9. Expression in Action (`expression_in_action.rs`)
Demonstrates the full power of the expression system:
- Comparison operators (=, !=, <, >, <=, >=)
- Logical operators (AND, OR, NOT)
- Arithmetic operations (+, -, *, /)
- String operations with LIKE patterns
- NULL handling (IS NULL, IS NOT NULL)
- Complex nested expressions
- Expression evaluation in filters and calculations
- Real-world expression patterns

Run with: `cargo run --example expression_in_action`

### 10. Filter Executor (`filter_executor.rs`)
Basic filtering demonstration:
- Simple filter conditions
- Expression-based filtering
- Integration with table scans

Run with: `cargo run --example filter_executor`

### 11. Projection Executor (`projection_executor.rs`)
Column projection examples:
- Selecting specific columns
- Reordering columns
- Projection after filtering

Run with: `cargo run --example projection_executor`

### 12. Sort Executor (`sort_executor.rs`)
Sorting functionality:
- Single column sorting
- Multi-column sorting
- Ascending and descending order
- Sorting with NULL values

Run with: `cargo run --example sort_executor`

### 13. Limit Executor (`limit_executor.rs`)
Limiting result sets:
- Basic LIMIT functionality
- TOP N queries
- Combining with sorting

Run with: `cargo run --example limit_executor`

### 14. Expression Demo (`expression_demo.rs`)
Expression evaluation basics:
- Creating expressions
- Evaluating against tuples
- Type checking

Run with: `cargo run --example expression_demo`

### 15. Filter Expressions Advanced (`filter_expressions_advanced.rs`)
Advanced filtering patterns:
- Complex boolean logic
- Nested conditions
- Performance considerations

Run with: `cargo run --example filter_expressions_advanced`

## Integration Tests

The `tests/integration_test.rs` file contains integration tests that demonstrate:
- Transaction isolation
- Concurrent access patterns
- Deadlock prevention
- Auto-abort functionality
- Resource cleanup

Run tests with: `cargo test`

## Quick Start Guide

### Basic CRUD Operations
Start with `crud_operations.rs` to learn how to:
1. Create tables
2. Insert data
3. Query with filters
4. Update records
5. Delete data
6. Use transactions

### Building Complex Queries
Explore `query_operators_demo.rs` to understand:
1. How to chain operators
2. Filter → Sort → Limit pipelines
3. Projection for column selection
4. Multi-column sorting

### Advanced Expressions
Study `expression_in_action.rs` for:
1. Complex WHERE clauses
2. Calculated fields
3. NULL handling
4. Pattern matching

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

### Query Execution
- Volcano-style iterators
- Operator pipelining
- Expression evaluation
- Filter pushdown

### Data Manipulation
- INSERT with batch operations
- UPDATE with conditions
- DELETE with filters
- Complex expressions

## Notes

- Examples use simplified APIs and may not demonstrate all production features
- File cleanup is handled automatically in most examples
- Examples are designed to be self-contained and runnable independently
- Check the source code comments for detailed explanations

## Example Progression

1. **Beginners**: Start with `crud_operations.rs` for basic operations
2. **Intermediate**: Move to `query_operators_demo.rs` for query building
3. **Advanced**: Explore `expression_in_action.rs` for complex expressions
4. **Deep Dive**: Study individual executor examples for specific features

## Common Patterns

### Creating a Filter Pipeline
```rust
let scan = Box::new(SeqScanExecutor::new("table", context.clone()));
let filter = Box::new(FilterExecutor::new(scan, condition, context.clone()));
let sort = Box::new(SortExecutor::new(filter, sort_keys, context.clone()));
let mut limit = LimitExecutor::new(sort, 10, context);
```

### Building Complex Expressions
```rust
let expr = Expression::binary(
    BinaryOperator::And,
    Expression::binary(
        BinaryOperator::GreaterThan,
        Expression::column(ColumnRef::new(0)),
        Expression::literal(Literal::int32(100)),
    ),
    Expression::binary(
        BinaryOperator::Like,
        Expression::column(ColumnRef::new(1)),
        Expression::literal(Literal::varchar("%pattern%")),
    ),
);
```