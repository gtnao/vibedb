//! Table scanning functionality for sequential access.

use crate::access::tuple::TupleId;
use crate::access::value::{deserialize_values, DataType, Value};
use crate::catalog::CustomDeserializer;
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageId;
use anyhow::Result;

/// Iterator for scanning all tuples in a table
pub struct TableScanner {
    buffer_pool: BufferPoolManager,
    current_page_id: Option<PageId>,
    current_slot: u16,
    schema: Vec<DataType>,
    custom_deserializer: Option<CustomDeserializer>,
}

impl TableScanner {
    /// Create a new table scanner
    pub fn new(
        buffer_pool: BufferPoolManager,
        first_page_id: Option<PageId>,
        schema: Vec<DataType>,
        custom_deserializer: Option<CustomDeserializer>,
    ) -> Self {
        Self {
            buffer_pool,
            current_page_id: first_page_id,
            current_slot: 0,
            schema,
            custom_deserializer,
        }
    }

    /// Try to get the next tuple from the current page
    fn try_next_tuple(&mut self) -> Result<Option<(TupleId, Vec<Value>)>> {
        loop {
            let page_id = match self.current_page_id {
                Some(id) => id,
                None => return Ok(None),
            };

            let guard = self.buffer_pool.fetch_page(page_id)?;

            // Create a temporary HeapPage view
            let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

            let tuple_count = heap_page.get_tuple_count();

            // Try to find a valid tuple starting from current_slot
            while self.current_slot < tuple_count {
                let slot_id = self.current_slot;
                self.current_slot += 1;

                match heap_page.get_tuple(slot_id) {
                    Ok(data) => {
                        // Deserialize the tuple data
                        let values = if let Some(deserializer) = self.custom_deserializer {
                            // Use custom deserializer
                            deserializer(data)?
                        } else {
                            // Use standard schema-based deserialization
                            deserialize_values(data, &self.schema)?
                        };
                        let tuple_id = TupleId::new(page_id, slot_id);
                        return Ok(Some((tuple_id, values)));
                    }
                    Err(_) => {
                        // Skip deleted or invalid tuples
                        continue;
                    }
                }
            }

            // No more valid tuples on this page, try next page
            self.current_page_id = heap_page.get_next_page_id();
            self.current_slot = 0;

            // Continue loop if we have a next page, otherwise return None
            if self.current_page_id.is_none() {
                return Ok(None);
            }
        }
    }
}

impl Iterator for TableScanner {
    type Item = Result<(TupleId, Vec<Value>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next_tuple() {
            Ok(Some(tuple)) => Some(Ok(tuple)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{serialize_values, TableHeap};
    use crate::storage::buffer::lru::LruReplacer;
    use crate::storage::disk::PageManager;
    use tempfile::tempdir;

    fn create_test_scanner() -> Result<(TableScanner, TableHeap)> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let mut heap = TableHeap::new(buffer_pool.clone(), crate::catalog::TableId(1));

        // Insert some test data
        let schema = vec![DataType::Int32, DataType::Varchar];
        let values1 = vec![Value::Int32(1), Value::String("Alice".to_string())];
        let values2 = vec![Value::Int32(2), Value::String("Bob".to_string())];

        let data1 = serialize_values(&values1, &schema)?;
        let data2 = serialize_values(&values2, &schema)?;

        let tid1 = heap.insert(&data1)?;
        heap.insert(&data2)?;

        // Create scanner starting from the first page
        let scanner = TableScanner::new(buffer_pool, Some(tid1.page_id), schema, None);

        Ok((scanner, heap))
    }

    #[test]
    fn test_table_scanner_basic() -> Result<()> {
        let (mut scanner, _heap) = create_test_scanner()?;

        // First tuple
        let result1 = scanner.next().expect("Should have first tuple")?;
        assert_eq!(result1.1[0], Value::Int32(1));
        assert_eq!(result1.1[1], Value::String("Alice".to_string()));

        // Second tuple
        let result2 = scanner.next().expect("Should have second tuple")?;
        assert_eq!(result2.1[0], Value::Int32(2));
        assert_eq!(result2.1[1], Value::String("Bob".to_string()));

        // No more tuples
        assert!(scanner.next().is_none());

        Ok(())
    }

    #[test]
    fn test_table_scanner_empty_table() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        // Create scanner for empty table
        let mut scanner = TableScanner::new(
            buffer_pool,
            None, // No first page
            vec![DataType::Int32],
            None,
        );

        assert!(scanner.next().is_none());

        Ok(())
    }

    #[test]
    fn test_table_scanner_with_deleted_tuples() -> Result<()> {
        let (mut scanner, mut heap) = create_test_scanner()?;

        // Delete the first tuple
        let tid1 = TupleId::new(PageId(0), 0);
        heap.delete(tid1)?;

        // Scanner should skip the deleted tuple
        let result = scanner.next().expect("Should have tuple")?;
        assert_eq!(result.1[0], Value::Int32(2)); // Should get the second tuple
        assert_eq!(result.1[1], Value::String("Bob".to_string()));

        // No more tuples
        assert!(scanner.next().is_none());

        Ok(())
    }

    #[test]
    fn test_table_scanner_multiple_pages() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let mut heap = TableHeap::new(buffer_pool.clone(), crate::catalog::TableId(1));

        // Insert enough data to span multiple pages
        let schema = vec![DataType::Varchar];
        let large_string = "x".repeat(1000); // Large data to fill pages quickly

        let mut first_page_id = None;
        let mut tuple_count = 0;

        for i in 0..20 {
            let value = Value::String(format!("{}-{}", i, large_string));
            let data = serialize_values(&[value], &schema)?;
            let tid = heap.insert(&data)?;

            if first_page_id.is_none() {
                first_page_id = Some(tid.page_id);
            }

            tuple_count += 1;
        }

        // Create scanner and count all tuples
        let mut scanner = TableScanner::new(buffer_pool, first_page_id, schema, None);

        let mut scanned_count = 0;
        while let Some(result) = scanner.next() {
            result?; // Check for errors
            scanned_count += 1;
        }

        assert_eq!(scanned_count, tuple_count);

        Ok(())
    }
}
