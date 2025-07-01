//! Executor layer for query execution.
//!
//! This module implements the Volcano-style iterator model for executing
//! physical query plans. Each executor produces tuples one at a time via
//! the `next()` method, allowing for efficient memory usage and composability.

use crate::access::{DataType, Tuple};
use crate::catalog::Catalog;
use crate::storage::buffer::BufferPoolManager;
use anyhow::Result;
use std::sync::Arc;

pub mod delete;
pub mod filter;
pub mod index_scan;
pub mod insert;
pub mod limit;
pub mod projection;
pub mod seq_scan;
pub mod sort;
pub mod update;

// Re-export executors
pub use delete::DeleteExecutor;
pub use filter::FilterExecutor;
pub use index_scan::IndexScanExecutor;
pub use insert::InsertExecutor;
pub use limit::LimitExecutor;
pub use projection::ProjectionExecutor;
pub use seq_scan::SeqScanExecutor;
pub use sort::{NullOrder, SortCriteria, SortExecutor, SortOrder};
pub use update::{UpdateExecutor, UpdateExpression};

/// Trait for all query executors
pub trait Executor: Send {
    /// Initialize the executor. This must be called before `next()`.
    fn init(&mut self) -> Result<()>;

    /// Get the next tuple from the executor.
    /// Returns None when there are no more tuples.
    fn next(&mut self) -> Result<Option<Tuple>>;

    /// Get the output schema of this executor
    fn output_schema(&self) -> &[ColumnInfo];
}

/// Information about a column in the output schema
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnInfo {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Execution context containing shared resources
#[derive(Clone)]
pub struct ExecutionContext {
    pub catalog: Arc<Catalog>,
    pub buffer_pool: Arc<BufferPoolManager>,
}

impl ExecutionContext {
    pub fn new(catalog: Arc<Catalog>, buffer_pool: Arc<BufferPoolManager>) -> Self {
        Self {
            catalog,
            buffer_pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::lru::LruReplacer;
    use crate::storage::disk::PageManager;
    use tempfile::tempdir;

    fn create_test_context() -> Result<ExecutionContext> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = Arc::new(BufferPoolManager::new(page_manager, replacer, 10));
        let catalog = Arc::new(Catalog::initialize((*buffer_pool).clone())?);

        Ok(ExecutionContext::new(catalog, buffer_pool))
    }

    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo::new("id", DataType::Int32);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Int32);

        let col2 = ColumnInfo::new(String::from("name"), DataType::Varchar);
        assert_eq!(col2.name, "name");
        assert_eq!(col2.data_type, DataType::Varchar);
    }

    #[test]
    fn test_execution_context_creation() -> Result<()> {
        let context = create_test_context()?;

        // Verify catalog has system tables
        let tables = context.catalog.list_tables()?;
        assert!(tables.len() >= 2); // At least pg_tables and pg_attribute

        Ok(())
    }
}
