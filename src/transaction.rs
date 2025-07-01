//! Transaction management module.
//!
//! This module provides transaction support for VibeDB, including:
//! - Transaction lifecycle management (begin, commit, rollback)
//! - ACID property enforcement
//! - Transaction isolation levels
//! - Transaction state tracking

pub mod id;
pub mod manager;
pub mod state;

// Re-export commonly used types
pub use id::{TransactionId, TransactionIdGenerator};
pub use manager::{Transaction, TransactionError, TransactionManager};
pub use state::{TransactionInfo, TransactionState};
