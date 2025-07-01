//! Limit executor implementation.
//!
//! This executor limits the number of tuples returned from a child executor.
//! It supports both LIMIT and OFFSET functionality for pagination.

use crate::access::Tuple;
use crate::executor::{ColumnInfo, Executor};
use anyhow::{bail, Result};

/// Executor that limits the number of tuples returned
pub struct LimitExecutor {
    /// Child executor that produces tuples
    child: Box<dyn Executor>,
    /// Maximum number of tuples to return
    limit: usize,
    /// Number of tuples to skip before returning
    offset: usize,
    /// Number of tuples skipped so far
    skipped: usize,
    /// Number of tuples returned so far
    returned: usize,
    /// Output schema (same as child's schema)
    output_schema: Vec<ColumnInfo>,
    /// Whether the executor has been initialized
    initialized: bool,
}

impl LimitExecutor {
    /// Create a new limit executor with only limit
    ///
    /// # Arguments
    /// * `child` - The child executor that produces tuples
    /// * `limit` - The maximum number of tuples to return
    pub fn new(child: Box<dyn Executor>, limit: usize) -> Self {
        Self::with_offset(child, limit, 0)
    }

    /// Create a new limit executor with limit and offset
    ///
    /// # Arguments
    /// * `child` - The child executor that produces tuples
    /// * `limit` - The maximum number of tuples to return
    /// * `offset` - The number of tuples to skip before returning
    pub fn with_offset(child: Box<dyn Executor>, limit: usize, offset: usize) -> Self {
        Self {
            child,
            limit,
            offset,
            skipped: 0,
            returned: 0,
            output_schema: Vec::new(),
            initialized: false,
        }
    }
}

impl Executor for LimitExecutor {
    fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Initialize child executor
        self.child.init()?;

        // Copy the child's output schema
        self.output_schema = self.child.output_schema().to_vec();

        // Reset counters
        self.skipped = 0;
        self.returned = 0;

        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            bail!("Executor not initialized. Call init() first.");
        }

        // If we've already returned the limit number of tuples, we're done
        if self.returned >= self.limit {
            return Ok(None);
        }

        // Skip offset number of tuples if we haven't already
        while self.skipped < self.offset {
            match self.child.next()? {
                Some(_) => self.skipped += 1,
                None => return Ok(None), // No more tuples to skip
            }
        }

        // Return the next tuple from child
        match self.child.next()? {
            Some(tuple) => {
                self.returned += 1;
                Ok(Some(tuple))
            }
            None => Ok(None),
        }
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{serialize_values, DataType, TupleId, Value};
    use crate::database::Database;
    use crate::executor::{ExecutionContext, SeqScanExecutor};
    use crate::storage::page::PageId;
    use anyhow::bail;
    use tempfile::tempdir;

    /// Mock executor for testing that produces a fixed set of tuples
    struct MockExecutor {
        tuples: Vec<(TupleId, Vec<Value>)>,
        schema: Vec<ColumnInfo>,
        schema_types: Vec<DataType>,
        current: usize,
        initialized: bool,
    }

    impl MockExecutor {
        fn new(tuples: Vec<(TupleId, Vec<Value>)>, schema: Vec<ColumnInfo>) -> Self {
            let schema_types = schema.iter().map(|col| col.data_type).collect();
            Self {
                tuples,
                schema,
                schema_types,
                current: 0,
                initialized: false,
            }
        }
    }

    impl Executor for MockExecutor {
        fn init(&mut self) -> Result<()> {
            self.initialized = true;
            self.current = 0; // Reset current position on init
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>> {
            if !self.initialized {
                bail!("Not initialized");
            }

            if self.current >= self.tuples.len() {
                return Ok(None);
            }

            let (tuple_id, values) = &self.tuples[self.current];
            self.current += 1;

            // Serialize values
            let data = serialize_values(values, &self.schema_types)?;
            Ok(Some(Tuple::new(*tuple_id, data)))
        }

        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }
    }

    fn create_test_data() -> (Vec<(TupleId, Vec<Value>)>, Vec<ColumnInfo>) {
        let schema = vec![
            ColumnInfo::new("id", DataType::Int32),
            ColumnInfo::new("name", DataType::Varchar),
        ];

        let tuples = vec![
            (
                TupleId::new(PageId(1), 0),
                vec![Value::Int32(1), Value::String("Alice".to_string())],
            ),
            (
                TupleId::new(PageId(1), 1),
                vec![Value::Int32(2), Value::String("Bob".to_string())],
            ),
            (
                TupleId::new(PageId(1), 2),
                vec![Value::Int32(3), Value::String("Charlie".to_string())],
            ),
            (
                TupleId::new(PageId(1), 3),
                vec![Value::Int32(4), Value::String("David".to_string())],
            ),
            (
                TupleId::new(PageId(1), 4),
                vec![Value::Int32(5), Value::String("Eve".to_string())],
            ),
        ];

        (tuples, schema)
    }

    #[test]
    fn test_limit_only() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema.clone()));

        // Create limit executor with limit = 3
        let mut limit = LimitExecutor::new(mock_executor, 3);
        limit.init()?;

        // Verify schema
        assert_eq!(limit.output_schema(), &schema);

        // Should return exactly 3 tuples
        assert!(limit.next()?.is_some()); // Alice
        assert!(limit.next()?.is_some()); // Bob
        assert!(limit.next()?.is_some()); // Charlie
        assert!(limit.next()?.is_none()); // No more (limit reached)

        Ok(())
    }

    #[test]
    fn test_limit_with_offset() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create limit executor with limit = 2, offset = 2
        let mut limit = LimitExecutor::with_offset(mock_executor, 2, 2);
        limit.init()?;

        // Should skip 2 tuples and return next 2
        let _tuple1 = limit.next()?.expect("Should have first tuple");
        // This should be Charlie (id=3)

        let _tuple2 = limit.next()?.expect("Should have second tuple");
        // This should be David (id=4)

        // No more tuples (limit reached)
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_limit_zero() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create limit executor with limit = 0
        let mut limit = LimitExecutor::new(mock_executor, 0);
        limit.init()?;

        // Should return no tuples
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_offset_exceeds_data() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Create limit executor with offset = 10 (exceeds data size)
        let mut limit = LimitExecutor::with_offset(mock_executor, 5, 10);
        limit.init()?;

        // Should return no tuples (all skipped)
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_limit_exceeds_data() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples.clone(), schema.clone()));

        // Create limit executor with limit = 10 (exceeds data size)
        let mut limit = LimitExecutor::new(mock_executor, 10);
        limit.init()?;

        // Should return all 5 tuples
        let mut count = 0;
        while limit.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 5);

        Ok(())
    }

    #[test]
    fn test_limit_not_initialized() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        let mut limit = LimitExecutor::new(mock_executor, 5);

        // Try to call next() without init()
        let result = limit.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));

        Ok(())
    }

    #[test]
    fn test_offset_only_first_page() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Simulate pagination: offset = 0, limit = 2 (first page)
        let mut limit = LimitExecutor::with_offset(mock_executor, 2, 0);
        limit.init()?;

        assert!(limit.next()?.is_some()); // Alice
        assert!(limit.next()?.is_some()); // Bob
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_offset_second_page() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Simulate pagination: offset = 2, limit = 2 (second page)
        let mut limit = LimitExecutor::with_offset(mock_executor, 2, 2);
        limit.init()?;

        assert!(limit.next()?.is_some()); // Charlie
        assert!(limit.next()?.is_some()); // David
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_offset_last_page() -> Result<()> {
        let (tuples, schema) = create_test_data();
        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Simulate pagination: offset = 4, limit = 2 (last page with only 1 item)
        let mut limit = LimitExecutor::with_offset(mock_executor, 2, 4);
        limit.init()?;

        assert!(limit.next()?.is_some()); // Eve
        assert!(limit.next()?.is_none()); // No more data

        Ok(())
    }

    #[test]
    fn test_limit_with_empty_child() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];
        let mock_executor = Box::new(MockExecutor::new(vec![], schema));

        let mut limit = LimitExecutor::new(mock_executor, 10);
        limit.init()?;

        // Should return no tuples
        assert!(limit.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_limit_with_seq_scan() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let mut db = Database::create(&db_path)?;

        // Create table
        db.create_table_with_columns(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Varchar)],
        )?;

        // Insert test data
        let table_info = db.catalog.get_table("users")?.unwrap();
        let mut heap = crate::access::TableHeap::with_first_page(
            (*db.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        );

        let schema = vec![DataType::Int32, DataType::Varchar];

        // Insert 10 rows
        for i in 1..=10 {
            heap.insert_values(
                &vec![Value::Int32(i), Value::String(format!("User{}", i))],
                &schema,
            )?;
        }

        // Create execution context
        let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

        // Test LIMIT 5
        let seq_scan = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));
        let mut limit = LimitExecutor::new(seq_scan, 5);
        limit.init()?;

        let mut count = 0;
        while limit.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 5);

        // Test LIMIT 3 OFFSET 5
        let seq_scan = Box::new(SeqScanExecutor::new("users".to_string(), context));
        let mut limit = LimitExecutor::with_offset(seq_scan, 3, 5);
        limit.init()?;

        let mut count = 0;
        while limit.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        Ok(())
    }

    #[test]
    fn test_multiple_init_calls() -> Result<()> {
        let (tuples, schema) = create_test_data();

        // Test that init() can be called multiple times without error
        let mock_executor = Box::new(MockExecutor::new(tuples.clone(), schema.clone()));
        let mut limit = LimitExecutor::with_offset(mock_executor, 2, 1);

        // First init
        limit.init()?;
        assert!(limit.next()?.is_some()); // Bob

        // Second init call should be idempotent (no-op)
        limit.init()?;
        // Continue from where we left off
        assert!(limit.next()?.is_some()); // Charlie
        assert!(limit.next()?.is_none());

        // Test with a fresh executor to verify reset behavior
        let mock_executor2 = Box::new(MockExecutor::new(tuples, schema));
        let mut limit2 = LimitExecutor::with_offset(mock_executor2, 2, 1);
        limit2.init()?;
        assert!(limit2.next()?.is_some()); // Bob
        assert!(limit2.next()?.is_some()); // Charlie
        assert!(limit2.next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_large_limit_and_offset() -> Result<()> {
        let schema = vec![ColumnInfo::new("id", DataType::Int32)];

        // Create 1000 tuples
        let mut tuples = Vec::new();
        for i in 0..1000 {
            tuples.push((TupleId::new(PageId(1), i as u16), vec![Value::Int32(i)]));
        }

        let mock_executor = Box::new(MockExecutor::new(tuples, schema));

        // Test with large offset and limit
        let mut limit = LimitExecutor::with_offset(mock_executor, 100, 500);
        limit.init()?;

        let mut count = 0;
        while limit.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 100);

        Ok(())
    }
}
