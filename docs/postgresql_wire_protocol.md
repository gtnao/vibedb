# PostgreSQL Wire Protocol Implementation

This document describes the PostgreSQL wire protocol v3 implementation in VibeDB.

## Overview

VibeDB implements a subset of the PostgreSQL wire protocol v3, allowing it to be accessed using standard PostgreSQL clients like `psql`. The implementation is found in the `src/network/` module.

## Architecture

### Module Structure

- `src/network.rs` - Main module declaration and common types
- `src/network/message.rs` - Protocol message types and encoding/decoding
- `src/network/protocol.rs` - Protocol handler and query execution
- `src/network/connection.rs` - Individual connection handling
- `src/network/server.rs` - TCP server implementation

### Key Components

1. **Message Types**: Implements both frontend (client-to-server) and backend (server-to-client) messages
2. **Protocol Handler**: Manages the protocol state machine and executes queries
3. **Connection Handler**: Manages individual client connections asynchronously
4. **Server**: TCP server that accepts connections and spawns handlers

## Supported Features

### Authentication
- Simple authentication (AuthenticationOk)
- No password verification (accepts any credentials)

### Query Execution
- **Simple Query Protocol**: Direct SQL execution with auto-commit
- **Extended Query Protocol**: Parse, Bind, Execute flow for prepared statements

### SQL Support
- `INSERT INTO table VALUES (...)` - Insert literal values
- `SELECT * FROM table` - Full table scans
- CREATE TABLE is NOT supported via wire protocol (use Database API)

### Data Types
Currently maps PostgreSQL types to VibeDB's limited type system:
- Integer types (INT, BIGINT) → Int32
- String types (VARCHAR, TEXT) → Varchar  
- BOOLEAN → Boolean
- Other types are mapped to closest equivalent

## Limitations

1. **No CREATE TABLE**: Tables must be created programmatically through the Database API
2. **Limited Data Types**: Only supports Int32, Varchar, and Boolean
3. **No UPDATE/DELETE**: Not yet implemented in the protocol handler
4. **No WHERE Clauses**: Only full table scans supported
5. **No Transactions**: Simple query protocol auto-commits each statement
6. **No SSL/TLS**: Plain text connections only

## Usage Example

```rust
// Start the server
cargo run --example psql_server

// Connect with psql
psql -h localhost -p 5432 -U postgres testdb

// Run queries
INSERT INTO users VALUES (1, 'Alice', true);
SELECT * FROM users;
```

## Future Enhancements

1. Implement UPDATE and DELETE support
2. Add WHERE clause evaluation
3. Support more data types (dates, timestamps, floats)
4. Implement transaction control (BEGIN/COMMIT/ROLLBACK)
5. Add SSL/TLS support
6. Implement authentication methods (MD5, SCRAM-SHA-256)
7. Support for prepared statement parameters
8. Query result pagination with LIMIT/OFFSET