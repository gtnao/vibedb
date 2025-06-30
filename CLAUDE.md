# TODO: B+Tree Index Implementation

docsにある`btree_*.md`ドキュメントを適宜参考に実装してください。
TransactionやWALは未実装なので無視してください。
都度テストやビルドが通ることを確認して進めること。
**mod.rs**は使ってはダメです。絶対にです。

## Storage Layer (storage/page/)

### BTree Internal Page

- [x] Create btree_internal_page.rs
- [x] Implement BTreeInternalPage struct with header (24 bytes)
- [x] Write unit tests for BTreeInternalPage struct
- [x] Run cargo build, test, fmt, clippy
- [x] Implement slot array management (child_page_id, key_offset, key_length)
- [x] Write unit tests for slot array management
- [x] Run cargo build, test, fmt, clippy
- [x] Implement lower/upper pointer management for dynamic space allocation
- [x] Write unit tests for pointer management
- [x] Run cargo build, test, fmt, clippy
- [x] Implement key insertion with proper slot array ordering
- [x] Write unit tests for key insertion
- [x] Run cargo build, test, fmt, clippy
- [x] Implement key deletion with slot compaction
- [x] Write unit tests for key deletion
- [x] Run cargo build, test, fmt, clippy
- [x] Implement page split logic for internal nodes
- [x] Write unit tests for page split
- [x] Run cargo build, test, fmt, clippy
- [x] Implement page merge logic for internal nodes
- [x] Write unit tests for page merge
- [x] Run cargo build, test, fmt, clippy
- [x] Implement find_child method for navigation
- [x] Write unit tests for find_child
- [x] Run cargo build, test, fmt, clippy
- [x] Implement redistribute_keys for balancing
- [x] Write unit tests for redistribute_keys
- [x] Run cargo build, test, fmt, clippy
- [x] Implement serialization/deserialization for page persistence
- [x] Write unit tests for serialization
- [x] Run cargo build, test, fmt, clippy

### BTree Leaf Page

- [x] Create btree_leaf_page.rs
- [x] Implement BTreeLeafPage struct with header (32 bytes)
- [x] Write unit tests for BTreeLeafPage struct
- [x] Run cargo build, test, fmt, clippy
- [x] Implement slot array management (key_offset, key_length, tuple_id)
- [x] Write unit tests for slot array management
- [x] Run cargo build, test, fmt, clippy
- [x] Implement sibling pointers (next_page_id, prev_page_id)
- [x] Write unit tests for sibling pointers
- [x] Run cargo build, test, fmt, clippy
- [x] Implement key-value insertion with duplicate key support
- [x] Write unit tests for key-value insertion
- [x] Run cargo build, test, fmt, clippy
- [x] Implement key-value deletion
- [x] Write unit tests for key-value deletion
- [x] Run cargo build, test, fmt, clippy
- [x] Implement page split logic for leaf nodes
- [x] Write unit tests for page split
- [x] Run cargo build, test, fmt, clippy
- [x] Implement page merge logic for leaf nodes
- [x] Write unit tests for page merge
- [x] Run cargo build, test, fmt, clippy
- [x] Implement search within leaf page
- [x] Write unit tests for search
- [x] Run cargo build, test, fmt, clippy
- [x] Implement range scan support within page
- [x] Write unit tests for range scan
- [x] Run cargo build, test, fmt, clippy
- [x] Implement serialization/deserialization
- [x] Write unit tests for serialization
- [x] Run cargo build, test, fmt, clippy

## Access Layer (access/)

### BTree Main Module

- [ ] Create btree.rs main module
- [ ] Implement BTree struct with buffer pool integration
- [ ] Write unit tests for BTree struct
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement create method for new B+Tree
- [ ] Write unit tests for create method
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement insert method with split propagation
- [ ] Write unit tests for insert method
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement delete method with merge/redistribute
- [ ] Write unit tests for delete method
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement search method for exact match
- [ ] Write unit tests for search method
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement range_scan method with iterator support
- [ ] Write unit tests for range_scan
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement prefix_scan for composite keys
- [ ] Write unit tests for prefix_scan
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement bulk_load for efficient initial loading
- [ ] Write unit tests for bulk_load
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement root page management and splits
- [ ] Write unit tests for root management
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement height tracking and statistics
- [ ] Write unit tests for statistics
- [ ] Run cargo build, test, fmt, clippy

### BTree Key Module

- [ ] Create btree/key.rs
- [ ] Implement BTreeKey struct for key abstraction
- [ ] Write unit tests for BTreeKey struct
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement composite key support
- [ ] Write unit tests for composite keys
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement NULL value handling (NULL as minimum)
- [ ] Write unit tests for NULL handling
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement type-safe comparison operators
- [ ] Write unit tests for comparisons
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement key serialization format
- [ ] Write unit tests for serialization
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement key deserialization with validation
- [ ] Write unit tests for deserialization
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement from_values constructor
- [ ] Write unit tests for from_values
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement to_values method
- [ ] Write unit tests for to_values
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement partial key matching for prefix scans
- [ ] Write unit tests for partial matching
- [ ] Run cargo build, test, fmt, clippy

### BTree Latch Module

- [ ] Create btree/latch.rs
- [ ] Implement LatchManager for page-level latches
- [ ] Write unit tests for LatchManager
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement Reader-Writer latch support
- [ ] Write unit tests for RW latches
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement latch coupling protocol
- [ ] Write unit tests for latch coupling
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement acquire_shared and acquire_exclusive
- [ ] Write unit tests for acquire methods
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement latch upgrade/downgrade
- [ ] Write unit tests for upgrade/downgrade
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement deadlock detection with Wait-For graph
- [ ] Write unit tests for deadlock detection
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement latch timeout handling
- [ ] Write unit tests for timeout handling
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement latch statistics collection
- [ ] Write unit tests for statistics
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement optimistic latch acquisition
- [ ] Write unit tests for optimistic acquisition
- [ ] Run cargo build, test, fmt, clippy

### BTree Iterator Module

- [ ] Create btree/iterator.rs
- [ ] Implement BTreeIterator for range scans
- [ ] Write unit tests for BTreeIterator
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement forward iteration with latch coupling
- [ ] Write unit tests for forward iteration
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement backward iteration support
- [ ] Write unit tests for backward iteration
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement seek to specific key
- [ ] Write unit tests for seek
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement handling of concurrent modifications
- [ ] Write unit tests for concurrent handling
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement iterator state management
- [ ] Write unit tests for state management
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement proper latch release on drop
- [ ] Write unit tests for drop behavior
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement bounded and unbounded scans
- [ ] Write unit tests for scan bounds
- [ ] Run cargo build, test, fmt, clippy

## Concurrency Control

- [ ] Implement crab latching for search operations
- [ ] Write tests for crab latching
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement pessimistic latching for insert operations
- [ ] Write tests for insert latching
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement pessimistic latching for delete operations
- [ ] Write tests for delete latching
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement safe node detection logic
- [ ] Write tests for safe node detection
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement ancestor latch release optimization
- [ ] Write tests for latch release
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement sibling pointer update protocol
- [ ] Write tests for sibling updates
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement optimistic descent for read-only operations
- [ ] Write tests for optimistic descent
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement version-based validation
- [ ] Write tests for version validation
- [ ] Run cargo build, test, fmt, clippy

## Error Handling

- [ ] Define BTreeError enum with all error types
- [ ] Write unit tests for error types
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement error propagation throughout the stack
- [ ] Write tests for error propagation
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement page corruption detection
- [ ] Write tests for corruption detection
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement basic page repair functionality
- [ ] Write tests for page repair
- [ ] Run cargo build, test, fmt, clippy

## Performance Optimizations

- [ ] Implement page-level statistics tracking
- [ ] Write tests for statistics
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement hot page identification
- [ ] Write tests for hot page detection
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement batch insert optimization
- [ ] Write tests for batch insert
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement key compression (prefix compression)
- [ ] Write tests for compression
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement leaf page scan optimization
- [ ] Write tests for scan optimization
- [ ] Run cargo build, test, fmt, clippy

## Debugging and Diagnostics

- [ ] Implement tree structure validation
- [ ] Write tests for validation
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement page integrity checks
- [ ] Write tests for integrity checks
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement tree visualization/dump functionality
- [ ] Write tests for visualization
- [ ] Run cargo build, test, fmt, clippy
- [ ] Implement operation tracing
- [ ] Write tests for tracing
- [ ] Run cargo build, test, fmt, clippy

## Integration and Final Steps

- [ ] Run ./check_no_mod_rs.sh final check
- [ ] Create integration tests with TableHeap
- [ ] Add concurrent stress tests
- [ ] Add performance benchmarks
- [ ] Write API documentation for all public methods
- [ ] Create usage examples for common scenarios
- [ ] Final cargo build, test, fmt, clippy
- [ ] Commit all changes

Current: Completed btree_leaf_page.rs implementation. Next: Starting btree.rs main module

# 🚨 ABSOLUTE EXECUTION RULES

## MANDATORY

1. **NEVER STOP until ALL tasks are complete** - No confirmations, reports, or consultations needed
2. **Create TODO list at the top of CLAUDE.md and update with ✓ as you progress**
3. **Report progress via Slack MCP tools when available** - Use mcp**slack-message**send_message for updates

## TODO Format (KEEP FLAT - NO NESTING)

'''markdown

# TODO: [Task Name]

- [ ] Specific task 1 description
- [ ] Specific task 2 description
- [ ] Specific task 3 description
- [x] Completed task

Current:

## FORBIDDEN

- "Shall I continue?" "Is this okay?" or ANY confirmation
- "I've completed X" or ANY progress reports **in Claude Code terminal**
- Waiting for user response
- Stopping on errors (find workarounds and continue)

## ONLY EXCEPTION

Fatal system errors only. Otherwise **RUN TO COMPLETION NO MATTER WHAT**.

# vibedb Development Rules

## Coding Rules

1. **Module Management - CRITICAL RULE**

   - **NEVER CREATE mod.rs FILES** - This is the #1 most important rule
   - Define modules directly in parent files
   - Example of CORRECT module structure:
     ```
     src/
       lib.rs          # Contains: pub mod catalog;
       catalog.rs      # Main catalog module
       catalog/        # Submodules directory
         table_info.rs
         column_info.rs
     ```
   - In catalog.rs, declare submodules like:
     ```rust
     pub mod table_info;
     pub mod column_info;
     ```
   - Run `./check_no_mod_rs.sh` before any commit to verify no mod.rs exists

2. **Testing**

   - Write tests in the same file as implementation (except E2E tests)
   - Write tests inside `#[cfg(test)]` module

3. **Quality Assurance**

   - Ensure all following commands pass:
     - `./check_no_mod_rs.sh` (MUST run this first!)
     - `cargo build`
     - `cargo test`
     - `cargo fmt`
     - `cargo clippy`

4. **Version Control**
   - Execute `git commit` and `git push` after all commands pass
