//! Concurrency control module.
//!
//! This module implements concurrency control mechanisms for VibeDB, including:
//! - Lock management (shared/exclusive locks)
//! - Multi-version concurrency control (MVCC)
//! - Deadlock detection and prevention
//! - Timestamp ordering protocols

pub mod lock;
pub mod mvcc;
pub mod timestamp;
pub mod version;
