# Executor Layer Design

## Overview

The Executor layer is responsible for executing physical query plans in vibedb. This document describes the design and implementation of the initial executors: SeqScan and Insert.

## Architecture

### Volcano Model

We adopt the Volcano-style iterator model where:
- Each executor implements a `next()` method that produces one tuple at a time
- Executors can be composed into trees for complex queries
- Pull-based execution allows for efficient memory usage

### Core Components

#### 1. Executor Trait

```rust
pub trait Executor: Send {
    /// Initialize the executor
    fn init(&mut self) -> Result<()>;
    
    /// Get the next tuple from the executor
    fn next(&mut self) -> Result<Option<Tuple>>;
    
    /// Get the output schema of this executor
    fn output_schema(&self) -> &[ColumnInfo];
}
```

#### 2. ExecutionContext

Holds references to shared resources:
- Catalog: For table metadata and schema information
- BufferPoolManager: For page access

#### 3. ColumnInfo

Describes a column in the output schema:
```rust
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}
```

## Executor Implementations

### SeqScanExecutor

**Purpose**: Scan all tuples from a table sequentially

**Key Features**:
- Uses TableScanner iterator internally
- Handles multi-page tables via page linking
- Skips deleted tuples automatically
- Returns tuples with proper schema information

**Algorithm**:
1. Initialize with table name
2. Fetch table metadata from catalog
3. Create TableScanner for the table
4. Return tuples one by one via `next()`

### InsertExecutor

**Purpose**: Insert tuples into a table

**Key Features**:
- Validates values against table schema
- Supports batch inserts
- Returns number of inserted rows
- Maintains ACID properties (with buffer pool)

**Algorithm**:
1. Initialize with table name and values to insert
2. Validate schema compatibility
3. Serialize values according to schema
4. Insert into TableHeap
5. Return success/failure status

## Access Layer Enhancement: TableScanner

To support efficient sequential scanning, we add a TableScanner to the access layer:

```rust
pub struct TableScanner {
    heap: TableHeap,
    buffer_pool: BufferPoolManager,
    current_page_id: Option<PageId>,
    current_slot: u16,
    schema: Vec<DataType>,
}

impl Iterator for TableScanner {
    type Item = Result<(TupleId, Vec<Value>)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Iterate through all pages and slots
        // Skip deleted tuples
        // Deserialize according to schema
    }
}
```

## Execution Flow Examples

### Sequential Scan

```rust
// Create executor
let mut executor = SeqScanExecutor::new(
    "users".to_string(),
    execution_context,
);

// Initialize
executor.init()?;

// Fetch all tuples
while let Some(tuple) = executor.next()? {
    // Process tuple
}
```

### Insert Operation

```rust
// Create executor with values
let values = vec![
    vec![Value::Int32(1), Value::String("Alice".to_string())],
    vec![Value::Int32(2), Value::String("Bob".to_string())],
];

let mut executor = InsertExecutor::new(
    "users".to_string(),
    values,
    execution_context,
);

// Execute insert
executor.init()?;
let result = executor.next()?; // Returns number of inserted rows
```

## Testing Strategy

### Unit Tests

1. **SeqScanExecutor Tests**:
   - Empty table scan
   - Single page scan
   - Multi-page scan
   - Scan with deleted tuples
   - Schema validation

2. **InsertExecutor Tests**:
   - Single row insert
   - Batch insert
   - Schema mismatch error
   - Insert into full page (triggers new page allocation)

3. **TableScanner Tests**:
   - Iterator correctness
   - Page boundary handling
   - Deleted tuple skipping

### Integration Tests

1. **Insert then Scan**:
   - Insert data using InsertExecutor
   - Scan using SeqScanExecutor
   - Verify all data is retrieved correctly

2. **Large Dataset Tests**:
   - Insert thousands of rows
   - Verify scan performance
   - Test memory usage stays bounded

## Future Extensions

### Near Term
1. **ProjectionExecutor**: Select specific columns
2. **FilterExecutor**: WHERE clause support
3. **LimitExecutor**: LIMIT/OFFSET support

### Medium Term
1. **HashJoinExecutor**: Join operations
2. **SortExecutor**: ORDER BY support
3. **AggregateExecutor**: GROUP BY and aggregations

### Long Term
1. **IndexScanExecutor**: Use indexes for faster access
2. **ParallelSeqScanExecutor**: Multi-threaded scanning
3. **ExchangeExecutor**: Distributed query execution

## Performance Considerations

1. **Memory Usage**: Volcano model ensures bounded memory usage
2. **I/O Efficiency**: Buffer pool minimizes disk reads
3. **CPU Efficiency**: Avoid unnecessary tuple copies
4. **Scalability**: Design supports parallel execution in future

## Implementation Order

1. Create `executor.rs` with trait and common types
2. Implement `access/scan.rs` with TableScanner
3. Implement `executor/seq_scan.rs`
4. Implement `executor/insert.rs`
5. Write comprehensive tests
6. Performance benchmarks