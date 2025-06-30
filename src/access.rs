//! Access layer for tuple-oriented operations.
//!
//! This module provides abstractions for managing tables and tuples:
//!
//! - **TableHeap**: Manages a table that can span multiple pages
//! - **Tuple**: Individual records with unique TupleIds
//! - **Value**: Type-safe representation of column values
//! - **DataType**: Supported data types with efficient serialization
//!
//! The access layer handles the complexity of multi-page tables, page linking,
//! and provides a clean API for higher layers to work with logical records
//! rather than raw bytes.

pub mod btree;
pub mod heap;
pub mod scan;
pub mod tuple;
pub mod value;

pub use heap::TableHeap;
pub use scan::TableScanner;
pub use tuple::{Tuple, TupleId};
pub use value::{DataType, Value, deserialize_values, serialize_values};
