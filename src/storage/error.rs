//! Storage layer error types.

use thiserror::Error;

/// Errors that can occur in the storage layer.
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Tuple not found: slot {slot_id} is empty or deleted")]
    TupleNotFound { slot_id: u16 },

    #[error("Invalid slot ID: {slot_id} (max: {max_slot})")]
    InvalidSlotId { slot_id: u16, max_slot: u16 },

    #[error("Page is full: requires {required} bytes but only {available} available")]
    PageFull { required: usize, available: usize },

    #[error("Buffer pool is full: cannot allocate new frame")]
    BufferPoolFull,

    #[error("Page not found: {0}")]
    PageNotFound(crate::storage::page::PageId),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Other error: {0}")]
    Other(String),
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
