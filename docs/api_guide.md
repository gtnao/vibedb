# vibedb API Guide

## Overview

vibedb provides a simple API for creating and managing databases with table-based storage.

## Creating a Database

```rust
use vibedb::database::Database;

// Create a new database
let db = Database::create("mydb.db")?;

// Open an existing database
let db = Database::open("mydb.db")?;
```

## Table Operations

### Creating Tables

```rust
// Create a new table
db.create_table("users")?;
```

### Opening Tables

```rust
// Open a table for read/write operations
let mut table = db.open_table("users")?;
```

### Listing Tables

```rust
// Get all table names
let tables = db.list_tables()?;
// Returns: ["pg_tables", "users", ...]
```

## Data Operations

### Inserting Data

```rust
let data = b"Hello, World!";
let tuple_id = table.insert(data)?;
```

### Reading Data

```rust
let tuple = table.get(tuple_id)?;
match tuple {
    Some(t) => println!("Data: {:?}", t.data),
    None => println!("Tuple not found or deleted"),
}
```

### Updating Data

```rust
// Currently implemented as delete + insert
table.update(tuple_id, b"Updated data")?;
```

### Deleting Data

```rust
table.delete(tuple_id)?;
```

## Persistence

Data is automatically persisted through the buffer pool manager. To ensure all data is written to disk:

```rust
db.flush()?;
```

The database also automatically flushes on drop.

## Architecture

```
┌─────────────────────────────────────┐
│           Database API              │
├─────────────────────────────────────┤
│            Catalog                  │ ← Table metadata
├─────────────────────────────────────┤
│          Access Layer               │ ← TableHeap, Tuple
├─────────────────────────────────────┤
│         Storage Layer               │ ← BufferPool, Pages
└─────────────────────────────────────┘
```

## Example

See `examples/basic_usage.rs` for a complete example.