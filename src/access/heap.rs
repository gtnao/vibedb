use crate::access::tuple::{Tuple, TupleId};
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::{HeapPage, PageId};
use anyhow::Result;

pub type TableId = u32;

/// Manages a table that spans multiple heap pages
pub struct TableHeap {
    buffer_pool: BufferPoolManager,
    _table_id: TableId, // Will be used for multi-table support
}

impl TableHeap {
    pub fn new(buffer_pool: BufferPoolManager, table_id: TableId) -> Self {
        Self {
            buffer_pool,
            _table_id: table_id,
        }
    }

    /// Insert a tuple into the table
    pub fn insert(&mut self, data: &[u8]) -> Result<TupleId> {
        // Try existing pages first
        let mut page_id = PageId(0);
        loop {
            // Try to fetch the page
            match self.buffer_pool.fetch_page_write(page_id) {
                Ok(mut guard) => {
                    let mut heap_page = HeapPage::from_data(&mut guard);

                    // Check if there's enough space
                    if heap_page.get_free_space() >= data.len() + 4 {
                        // 4 bytes for slot
                        let slot_id = heap_page.insert_tuple(data)?;
                        return Ok(TupleId::new(page_id, slot_id));
                    }
                    // Not enough space, try next page
                }
                Err(_) => {
                    // Page doesn't exist, create a new one
                    break;
                }
            }

            // Move to next page
            page_id = PageId(page_id.0 + 1);
        }

        // No existing page has space, create a new page
        let (new_page_id, mut guard) = self.buffer_pool.new_page()?;
        let mut heap_page = HeapPage::new(&mut guard, new_page_id);
        let slot_id = heap_page.insert_tuple(data)?;

        Ok(TupleId::new(new_page_id, slot_id))
    }

    /// Get a tuple by its ID
    pub fn get(&self, tuple_id: TupleId) -> Result<Option<Tuple>> {
        let guard = self.buffer_pool.fetch_page(tuple_id.page_id)?;
        let mut data_copy = [0u8; crate::storage::PAGE_SIZE];
        data_copy.copy_from_slice(&guard[..]);
        let heap_page = HeapPage::from_data(&mut data_copy);

        match heap_page.get_tuple(tuple_id.slot_id) {
            Ok(data) => Ok(Some(Tuple::new(tuple_id, data.to_vec()))),
            Err(e) => {
                // Check if it's a "tuple deleted" error or "invalid slot" error
                if e.to_string().contains("deleted") || e.to_string().contains("Invalid slot") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Update a tuple
    pub fn update(&mut self, tuple_id: TupleId, data: &[u8]) -> Result<()> {
        // For simplicity, we'll do delete + insert for now
        // In a real implementation, we'd try in-place update if size permits
        self.delete(tuple_id)?;
        self.insert(data)?;
        Ok(())
    }

    /// Delete a tuple
    pub fn delete(&mut self, tuple_id: TupleId) -> Result<()> {
        let mut guard = self.buffer_pool.fetch_page_write(tuple_id.page_id)?;
        let mut heap_page = HeapPage::from_data(&mut guard);
        heap_page.delete_tuple(tuple_id.slot_id)?;
        Ok(())
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
        Ok(TableHeap::new(buffer_pool, 1))
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
