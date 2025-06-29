//! Utility functions for page operations.

use crate::storage::PAGE_SIZE;
use crate::storage::buffer::PageReadGuard;
use crate::storage::page::HeapPage;

/// Create a temporary HeapPage view from a PageReadGuard.
///
/// # Safety
/// This function creates a temporary view of the page data without copying.
/// The returned HeapPage borrows data from the guard and must not outlive it.
/// The guard ensures the page remains in memory and won't be evicted.
pub fn heap_page_from_guard(guard: &PageReadGuard) -> HeapPage<'_> {
    // SAFETY: This is safe because:
    // 1. The guard ensures the page remains in memory and won't be evicted
    // 2. We're creating a temporary view that doesn't outlive the guard
    // 3. We cast to *mut for the slice API, but only read from it
    // 4. PAGE_SIZE is guaranteed to match the actual page size
    let page_data = unsafe { std::slice::from_raw_parts_mut(guard.as_ptr() as *mut u8, PAGE_SIZE) };
    let page_array = unsafe { &mut *(page_data.as_mut_ptr() as *mut [u8; PAGE_SIZE]) };
    HeapPage::from_data(page_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::BufferPoolManager;
    use crate::storage::buffer::lru::LruReplacer;
    use crate::storage::disk::PageManager;
    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn test_heap_page_from_guard() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        // Create a new page
        let (page_id, mut guard) = buffer_pool.new_page()?;
        let _ = HeapPage::new(&mut guard, page_id);
        drop(guard);

        // Fetch the page and create a view
        let read_guard = buffer_pool.fetch_page(page_id)?;
        let heap_page = heap_page_from_guard(&read_guard);

        // Verify we can read from the heap page
        assert_eq!(heap_page.get_tuple_count(), 0);
        assert!(heap_page.get_free_space() > 0);

        Ok(())
    }
}
