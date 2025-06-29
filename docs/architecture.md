# vibedb Architecture Documentation

## Overview

vibedb is an educational RDBMS (Relational Database Management System) being developed for learning purposes. It follows the design principles of production databases like PostgreSQL while implementing features incrementally.

## Architecture Overview

```
┌─────────────────────────────────────┐
│       SQL Parser & Planner          │ ← Future implementation
├─────────────────────────────────────┤
│          Executor Layer             │ ← Future implementation
├─────────────────────────────────────┤
│           Database API              │ ← User interface
├─────────────────────────────────────┤
│            Catalog                  │ ← Table metadata (planned)
├─────────────────────────────────────┤
│          Access Layer               │ ← In progress
│   - TableHeap (table abstraction)   │
│   - Tuple (logical row)             │
├─────────────────────────────────────┤
│         Storage Layer               │ ← Implemented
│   - BufferPoolManager               │
│   - PageManager                     │
│   - HeapPage                        │
└─────────────────────────────────────┘
```

## Design Principles

1. **Incremental Implementation**
   - Start with minimal features
   - Each layer can be developed and tested independently

2. **Practical Design**
   - Choose proven designs over academic implementations
   - Reference PostgreSQL and other production databases

3. **Extensibility**
   - Design for future feature additions
   - Maintain stable interfaces

## Layer Details

### Storage Layer

The storage layer manages physical disk I/O and memory caching, providing page-based data management.

#### Components

**PageManager**
- Manages page-level disk I/O operations
- Fixed page size: 8KB (PAGE_SIZE constant)
- Persistence guarantee: fsync on write
- Zero-copy interface using buffers

**BufferPoolManager**
- Caches frequently accessed pages in memory
- Reduces disk I/O through intelligent caching
- Implements LRU (Least Recently Used) replacement policy
- Thread-safe with concurrent access support

**HeapPage**
- PostgreSQL-style page layout for storing tuple data
- Efficient free space management using upper/lower pointers
- Special area for future extensions (currently used for next_page_id)

#### Page Layout

```
┌─────────────────────────────────────────┐
│ Header (20 bytes)                       │
├─────────────────────────────────────────┤
│ page_id (4 bytes)          [0-3]        │
│ reserved (8 bytes)         [4-11]       │
│ lower (2 bytes)            [12-13]      │ ← End of slot array
│ upper (2 bytes)            [14-15]      │ ← Start of tuple data
│ special (4 bytes)          [16-19]      │ ← next_page_id storage
├─────────────────────────────────────────┤
│                                         │
│ Slot Array (grows down ↓)               │
│ ┌─────────────────────────┐             │
│ │ slot[0]: offset, length │ [20-23]    │
│ │ slot[1]: offset, length │ [24-27]    │
│ │ ...                     │             │
│ └─────────────────────────┘             │
│                                         │
│           Free Space                    │
│         (upper - lower)                 │
│                                         │
│ ┌─────────────────────────┐             │
│ │ Tuple Data (grows up ↑) │             │
│ │ tuple[n]                │ [8xxx-8191]│
│ │ ...                     │             │
│ │ tuple[1]                │             │
│ │ tuple[0]                │             │
│ └─────────────────────────┘             │
└─────────────────────────────────────────┘
```

#### Zero-Copy Design

The BufferPoolManager implements a zero-copy design using guard patterns:
- `PageReadGuard`: Read-only access (multiple simultaneous readers)
- `PageWriteGuard`: Write access (exclusive)
- Automatic unpinning via Drop trait

### Access Layer

The access layer abstracts physical page structures into logical table and tuple operations, allowing upper layers to work with data without concern for page boundaries.

#### Components

**TupleId**
- Unique identifier for a tuple: (page_id, slot_id)
- 8 bytes total (4 bytes PageId + 2 bytes slot_id)
- Provides ordering (by page then slot)
- Lightweight to copy

**Tuple**
- Logical representation of a database row
- Currently holds raw byte data
- Future: MVCC information and schema interpretation

**TableHeap**
- Manages tables spanning multiple HeapPages
- Provides tuple operations without page boundary concerns
- Handles page allocation and free space management

#### Key Operations

- `insert(data: &[u8]) -> Result<TupleId>`: Find page with space and insert
- `get(tuple_id: TupleId) -> Result<Option<Tuple>>`: Retrieve tuple by ID
- `update(tuple_id: TupleId, data: &[u8]) -> Result<()>`: Update existing tuple
- `delete(tuple_id: TupleId) -> Result<()>`: Mark tuple as deleted

### Catalog Layer

The catalog layer manages metadata about tables, columns, and other database objects. System catalogs themselves are implemented as tables, providing a self-describing structure.

#### System Tables

**pg_tables** (table information)
| Column | Type | Description |
|--------|------|-------------|
| table_id | u32 | Unique table identifier |
| table_name | text | Table name |
| first_page_id | u32 | First page ID of the table |

**pg_attribute** (column information) - Planned
| Column | Type | Description |
|--------|------|-------------|
| table_id | u32 | Table identifier |
| column_id | u16 | Column position |
| column_name | text | Column name |
| type_id | u32 | Data type identifier |
| nullable | bool | Whether NULL is allowed |

#### Bootstrap Process

The catalog faces a chicken-and-egg problem: reading catalog information requires the catalog table, but the catalog table's information is stored in the catalog. This is solved by:

1. Fixed definition: CATALOG_TABLE_ID = 1, CATALOG_FIRST_PAGE = PageId(0)
2. Bootstrap process writes catalog table's own entry first
3. After bootstrap, catalog can be treated as a normal table

### Type System

vibedb implements a flexible type system supporting multiple data types with efficient storage:

#### Supported Types
- **Boolean**: 1 byte storage
- **Int32**: 4 byte signed integer
- **Varchar**: Variable-length text with 4-byte length prefix

#### Features
- **NULL Handling**: Efficient bitmap storage for NULL values
- **Schema-based Serialization**: No type tags needed in storage
- **Type Safety**: Compile-time type checking through Rust's type system

### Database API

The top-level API provides a simple interface for database operations:

```rust
// Create/open database
let db = Database::create("mydb.db")?;
let db = Database::open("mydb.db")?;

// Table operations
db.create_table("users")?;
let mut table = db.open_table("users")?;
let tables = db.list_tables()?;

// Data operations
let tuple_id = table.insert(b"Hello, World!")?;
let tuple = table.get(tuple_id)?;
table.update(tuple_id, b"Updated data")?;
table.delete(tuple_id)?;

// Persistence
db.flush()?;
```

## Implementation Status

### Completed ✅
- Disk I/O (PageManager)
- Buffer pool (BufferPoolManager)
- Page structure (HeapPage)
- Zero-copy design
- Basic CRUD operations

### In Progress 🚧
- Access layer improvements
- Page linking for tables
- Catalog system design

### Planned ⏳
- Catalog system implementation
- Query execution engine
- Transaction management
- Indexes (B+Tree)
- WAL (Write-Ahead Logging)
- MVCC (Multi-Version Concurrency Control)

## Development Roadmap

### Phase 1: Access Layer Improvements
- Implement next_page_id in Special area
- Separate space calculation responsibilities
- Zero-copy get implementation

### Phase 2: Catalog Implementation
- Basic catalog structure
- Bootstrap process
- Table management operations

### Phase 3: Database Integration
- Unified Database API
- End-to-end testing
- Documentation and examples

### Phase 4: Future Extensions
- Column information (pg_attribute)
- Type system
- Query execution
- Transaction support

## Concurrency Control

The system implements thread-safe operations at multiple levels:
- **PageTable**: DashMap for thread-safe hash map
- **Frames**: RwLock for read multiplexing
- **Replacer**: Mutex for exclusive control
- **Frame internals**: Atomic operations for lock-free access

## Error Handling

Unified error handling using anyhow::Result:
- I/O errors from disk operations
- Invalid buffer sizes
- Page not found errors
- Tuple not found errors
- Insufficient space errors

## Testing Strategy

### Unit Tests
- Component isolation
- Edge case coverage
- Located in the same file as implementation

### Integration Tests
- Multi-table operations
- Persistence verification
- Restart behavior

### Performance Tests
- Large data insertion
- Page linking overhead
- Concurrent access patterns