//! Recovery and crash resilience module.
//!
//! This module handles database recovery operations, including:
//! - Crash recovery using write-ahead logging (WAL)
//! - Checkpoint management
//! - Redo and undo operations
//! - Ensuring durability after system failures

pub mod aries;
pub mod checkpoint;
pub mod log_record;

// Re-export commonly used types
pub use aries::{AriesRecovery, RecoveryError};
pub use checkpoint::{CheckpointManager, FuzzyCheckpoint};
pub use log_record::{ClrRecord, RecoveryLogRecord};
