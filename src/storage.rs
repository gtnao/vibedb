//! Storage layer implementation for vibedb.
//!
//! This module provides the foundation for persistent data storage using a page-based
//! architecture. Key components:
//!
//! - **Page**: Fixed-size (4KB) blocks of data, the basic unit of I/O
//! - **PageManager**: Handles reading/writing pages to disk
//! - **BufferPool**: In-memory cache of pages with LRU eviction
//! - **HeapPage**: Slotted page format for storing variable-length tuples
//!
//! The storage layer ensures durability through write-ahead logging (future)
//! and provides the abstraction that higher layers build upon.

pub mod buffer;
pub mod disk;
pub mod error;
pub mod page;
pub mod wal;

pub use buffer::{BufferPoolManager, PageReadGuard, PageWriteGuard};
pub use disk::{PageManager, PAGE_SIZE};
pub use page::{HeapPage, Page, PageId};
