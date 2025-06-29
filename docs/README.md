# vibedb Documentation

## Overview

This directory contains the technical documentation for vibedb, an educational RDBMS implementation.

## Documentation Structure

- **[architecture.md](architecture.md)** - Complete architectural overview including:
  - System architecture and design principles
  - Storage layer (PageManager, BufferPoolManager, HeapPage)
  - Access layer (TableHeap, Tuple, TupleId)
  - Catalog layer (system tables, bootstrap process)
  - Type system and data types
  - Database API
  - Implementation status and roadmap
  - Concurrency control and error handling

## Quick Links

For general project information, see the [main README](../README.md).

For development rules and conventions, see [CLAUDE.md](../CLAUDE.md).

## Contributing to Documentation

When adding new documentation:
1. Update the main architecture.md file for architectural changes
2. Keep documentation close to the code it describes
3. Use clear diagrams and examples where possible
4. Maintain consistency with existing documentation style