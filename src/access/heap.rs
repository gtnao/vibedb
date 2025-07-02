use crate::access::tuple::{Tuple, TupleId};
use crate::access::value::{serialize_values, DataType, Value};
use crate::catalog::TableId;
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::{HeapPage, PageId};
use anyhow::Result;

/// Manages a table that spans multiple heap pages
pub struct TableHeap {
    buffer_pool: BufferPoolManager,
    _table_id: TableId,            // Will be used for multi-table support
    first_page_id: Option<PageId>, // First page of the table
}

impl TableHeap {
    pub fn new(buffer_pool: BufferPoolManager, table_id: TableId) -> Self {
        Self {
            buffer_pool,
            _table_id: table_id,
            first_page_id: None,
        }
    }

    pub fn with_first_page(
        buffer_pool: BufferPoolManager,
        table_id: TableId,
        first_page_id: PageId,
    ) -> Self {
        Self {
            buffer_pool,
            _table_id: table_id,
            first_page_id: Some(first_page_id),
        }
    }

    /// Insert a tuple into the table
    pub fn insert(&mut self, data: &[u8]) -> Result<TupleId> {
        let required_space = HeapPage::required_space_for(data.len());

        // If we have a first page, start from there
        if let Some(mut current_page_id) = self.first_page_id {
            while let Ok(mut guard) = self.buffer_pool.fetch_page_write(current_page_id) {
                let mut heap_page = HeapPage::from_data(&mut guard);

                // Check if there's enough space
                if heap_page.get_free_space() >= required_space {
                    let slot_id = heap_page.insert_tuple(data)?;
                    return Ok(TupleId::new(current_page_id, slot_id));
                }

                // Not enough space, check next page
                match heap_page.get_next_page_id() {
                    Some(next_page_id) => {
                        current_page_id = next_page_id;
                    }
                    None => {
                        // No next page, need to create one
                        let (new_page_id, mut new_guard) = self.buffer_pool.new_page()?;
                        let mut new_heap_page = HeapPage::new(&mut new_guard, new_page_id);
                        let slot_id = new_heap_page.insert_tuple(data)?;

                        // Link the pages (drop not needed, just reassign)
                        let mut prev_page = HeapPage::from_data(&mut guard);
                        prev_page.set_next_page_id(Some(new_page_id));

                        return Ok(TupleId::new(new_page_id, slot_id));
                    }
                }
            }
        }

        // No existing pages, create the first page
        let (new_page_id, mut guard) = self.buffer_pool.new_page()?;
        let mut heap_page = HeapPage::new(&mut guard, new_page_id);
        let slot_id = heap_page.insert_tuple(data)?;

        // Update first_page_id if this is the first page
        if self.first_page_id.is_none() {
            self.first_page_id = Some(new_page_id);
        }

        Ok(TupleId::new(new_page_id, slot_id))
    }

    /// Get a tuple by its ID (zero-copy implementation)
    pub fn get(&self, tuple_id: TupleId) -> Result<Option<Tuple>> {
        let guard = self.buffer_pool.fetch_page(tuple_id.page_id)?;

        // Create a temporary HeapPage view without copying
        let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

        match heap_page.get_tuple(tuple_id.slot_id) {
            Ok(data) => Ok(Some(Tuple::new(tuple_id, data.to_vec()))),
            Err(crate::storage::error::StorageError::TupleNotFound { .. }) => Ok(None),
            Err(crate::storage::error::StorageError::InvalidSlotId { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get the first page ID of the table
    pub fn first_page_id(&self) -> Option<PageId> {
        self.first_page_id
    }

    /// Update a tuple
    pub fn update(&mut self, tuple_id: TupleId, data: &[u8]) -> Result<TupleId> {
        // For simplicity, we'll do delete + insert for now
        // In a real implementation, we'd try in-place update if size permits
        self.delete(tuple_id)?;
        let new_tuple_id = self.insert(data)?;
        Ok(new_tuple_id)
    }

    /// Delete a tuple
    pub fn delete(&mut self, tuple_id: TupleId) -> Result<()> {
        let mut guard = self.buffer_pool.fetch_page_write(tuple_id.page_id)?;
        let mut heap_page = HeapPage::from_data(&mut guard);
        heap_page.delete_tuple(tuple_id.slot_id)?;
        Ok(())
    }

    /// Insert values as a tuple with schema
    pub fn insert_values(&mut self, values: &[Value], schema: &[DataType]) -> Result<TupleId> {
        let data = serialize_values(values, schema)?;
        self.insert(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::PageManager;
    use tempfile::tempdir;

    fn create_test_table_heap() -> Result<TableHeap> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(crate::storage::buffer::lru::LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);
        Ok(TableHeap::new(buffer_pool, crate::catalog::TableId(1)))
    }

    #[test]
    fn test_empty_table() -> Result<()> {
        let table_heap = create_test_table_heap()?;

        // Try to get a tuple from a non-existent page
        let tid = TupleId::new(PageId(0), 0);
        // This should return an error since page doesn't exist
        assert!(table_heap.get(tid).is_err());

        Ok(())
    }

    #[test]
    fn test_insert_and_get() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        // Insert a tuple
        let data = b"Hello, World!";
        let tid = table_heap.insert(data)?;

        // Get the tuple back
        let tuple = table_heap.get(tid)?.expect("Tuple should exist");
        assert_eq!(tuple.data, data);
        assert_eq!(tuple.tuple_id, tid);

        Ok(())
    }

    #[test]
    fn test_multiple_inserts() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        let data1 = b"First tuple";
        let data2 = b"Second tuple";
        let data3 = b"Third tuple";

        let tid1 = table_heap.insert(data1)?;
        let tid2 = table_heap.insert(data2)?;
        let tid3 = table_heap.insert(data3)?;

        // All should be on the same page
        assert_eq!(tid1.page_id, tid2.page_id);
        assert_eq!(tid2.page_id, tid3.page_id);

        // But different slots
        assert_ne!(tid1.slot_id, tid2.slot_id);
        assert_ne!(tid2.slot_id, tid3.slot_id);

        // Verify all data
        assert_eq!(table_heap.get(tid1)?.unwrap().data, data1);
        assert_eq!(table_heap.get(tid2)?.unwrap().data, data2);
        assert_eq!(table_heap.get(tid3)?.unwrap().data, data3);

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        let data = b"To be deleted";
        let tid = table_heap.insert(data)?;

        // Verify it exists
        assert!(table_heap.get(tid)?.is_some());

        // Delete it
        table_heap.delete(tid)?;

        // Verify it's gone
        assert!(table_heap.get(tid)?.is_none());

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        let original = b"Original data";
        let updated = b"Updated data";

        let tid = table_heap.insert(original)?;
        table_heap.update(tid, updated)?;

        // The simple implementation deletes and re-inserts,
        // so we can't check the exact TupleId
        // Just verify the data was updated somewhere
        let mut found = false;
        for slot_id in 0..10 {
            let check_tid = TupleId::new(PageId(0), slot_id);
            if let Some(tuple) = table_heap.get(check_tid)? {
                if tuple.data == updated {
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "Updated data should be found");

        Ok(())
    }

    #[test]
    fn test_page_boundary() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        // Insert large tuples to fill up a page
        let large_data = vec![0xAA; 1000]; // 1KB each
        let mut tids = Vec::new();

        // Insert until we get a tuple on a different page
        let mut first_page_id = None;
        for _ in 0..20 {
            let tid = table_heap.insert(&large_data)?;

            if first_page_id.is_none() {
                first_page_id = Some(tid.page_id);
            } else if tid.page_id != first_page_id.unwrap() {
                // We've crossed a page boundary
                assert_eq!(tid.page_id.0, first_page_id.unwrap().0 + 1);
                break;
            }

            tids.push(tid);
        }

        // Verify all tuples are still accessible
        for tid in tids {
            let tuple = table_heap.get(tid)?.expect("Tuple should exist");
            assert_eq!(tuple.data, large_data);
        }

        Ok(())
    }

    #[test]
    fn test_empty_tuple() -> Result<()> {
        let mut table_heap = create_test_table_heap()?;

        let tid = table_heap.insert(&[])?;
        let tuple = table_heap.get(tid)?.expect("Empty tuple should exist");
        assert_eq!(tuple.data.len(), 0);

        Ok(())
    }
}
