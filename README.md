# vibedb

A experimental/educational RDBMS implementation in Rust, built entirely through "vibe coding" - where all code is written by Claude Code AI assistant without any manual coding by humans.

## Overview

vibedb is an experimental relational database management system that demonstrates how far we can push AI-assisted development. The entire codebase has been written by Claude Code, with a human providing only high-level guidance and feedback. This project serves as both an educational resource for understanding database internals and an experiment in AI-driven software development.

## Features

### Storage Layer
- Page-based storage with 4KB pages
- Buffer pool manager with LRU eviction policy
- Persistent disk storage with crash recovery

### Access Layer
- Heap file organization with slotted pages
- Multi-page table support with page linking
- Tuple management with unique TupleIds

### Catalog Layer
- System catalogs (pg_tables, pg_attribute)
- Schema management and column definitions
- Self-describing bootstrap process

### Type System
- Support for multiple data types (Boolean, Int32, Varchar)
- NULL handling with efficient bitmap storage
- Schema-based serialization without type tags

### Database API
- High-level database operations
- Table creation with schema definitions
- ACID properties foundation (future work)

## Getting Started

### Prerequisites
- Rust 1.70 or later
- Cargo

### Building
```bash
cargo build --release
```

### Running Tests
```bash
cargo test
```

### Example Usage
```bash
cargo run --example schema_usage
```

This will demonstrate:
- Creating a database
- Defining tables with schemas
- Inserting typed data
- Querying with NULL support

## Architecture

```
vibedb/
├── src/
│   ├── storage/        # Physical storage and buffer management
│   ├── access/         # Table and tuple management
│   ├── catalog/        # System catalogs and metadata
│   └── database.rs     # High-level database API
└── examples/           # Usage examples
```

## Educational Value

This project demonstrates key database concepts:

1. **Storage Management**: How databases organize data on disk using pages
2. **Buffer Pool**: Caching strategies to minimize disk I/O
3. **Access Methods**: How tuples are stored and retrieved efficiently
4. **System Catalogs**: Self-describing metadata management
5. **Type Systems**: Efficient data serialization with schema awareness

## The "Vibe Coding" Experiment

This entire project was created through conversational programming with Claude Code. The human contributor provided only:
- High-level architectural decisions
- Feedback on implementation approaches
- Requests for specific features

All actual code implementation, testing, and debugging was performed by the AI assistant. This demonstrates the potential of AI-powered development tools while also serving as a learning resource for database internals.

## Current Limitations

As an educational/experimental project, vibedb currently lacks:
- SQL query processing
- Transaction support
- Concurrent access control
- Index structures
- Query optimization
- Network protocol

These could be future additions to explore more advanced database concepts.

## Contributing

While this is primarily an AI-driven experiment, suggestions and feedback are welcome! Please open an issue to discuss potential improvements or educational enhancements.

## License

This project is open source and available under the MIT License.

## Acknowledgments

- Built entirely by Claude Code (Anthropic)
- Inspired by PostgreSQL's architecture
- Educational database systems like SimpleDB and BusTub