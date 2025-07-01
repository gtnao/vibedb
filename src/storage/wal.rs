//! Write-Ahead Logging (WAL) module.
//!
//! This module implements the write-ahead logging subsystem for VibeDB, providing:
//! - WAL record formatting and serialization
//! - Log buffer management
//! - Log flushing and persistence
//! - Log sequence number (LSN) management
//! - Support for various log record types (insert, update, delete, checkpoint, etc.)

pub mod manager;
pub mod record;

pub use manager::{WalConfig, WalManager};
pub use record::{
    AbortRecord, BeginRecord, CheckpointRecord, CommitRecord, DeleteRecord, InsertRecord,
    UpdateRecord, WalRecord, WalRecordHeader, WalRecordPayload, WalRecordType, LSN,
};
