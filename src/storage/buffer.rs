pub mod lru;
pub mod replacer;

use crate::storage::{PAGE_SIZE, PageId, PageManager};
use anyhow::Result;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use replacer::{FrameId, Replacer};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

pub struct Frame {
    data: Box<[u8; PAGE_SIZE]>,
    page_id: Option<PageId>,
    pin_count: AtomicU32,
    is_dirty: AtomicBool,
}

impl Frame {
    fn new() -> Self {
        Self {
            data: Box::new([0u8; PAGE_SIZE]),
            page_id: None,
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }

    fn reset(&mut self) {
        self.page_id = None;
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty.store(false, Ordering::SeqCst);
        self.data.fill(0);
    }
}

#[derive(Clone)]
pub struct BufferPoolManager {
    inner: Arc<BufferPoolInner>,
}

struct BufferPoolInner {
    page_table: DashMap<PageId, FrameId>,
    frames: RwLock<HashMap<FrameId, Frame>>,
    replacer: Mutex<Box<dyn Replacer>>,
    page_manager: Mutex<PageManager>,
    next_frame_id: AtomicU32,
    max_frames: usize,
}

impl BufferPoolManager {
    pub fn new(page_manager: PageManager, replacer: Box<dyn Replacer>, max_frames: usize) -> Self {
        Self {
            inner: Arc::new(BufferPoolInner {
                page_table: DashMap::new(),
                frames: RwLock::new(HashMap::with_capacity(max_frames)),
                replacer: Mutex::new(replacer),
                page_manager: Mutex::new(page_manager),
                next_frame_id: AtomicU32::new(0),
                max_frames,
            }),
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<PageReadGuard> {
        // Check if page is already in buffer pool
        if let Some(frame_id) = self.inner.page_table.get(&page_id).map(|e| *e.value()) {
            let frames = self.inner.frames.read();
            if let Some(frame) = frames.get(&frame_id) {
                frame.pin_count.fetch_add(1, Ordering::SeqCst);
                self.inner.replacer.lock().pin(frame_id);

                let data = frame.data.as_ref() as *const [u8; PAGE_SIZE];
                return Ok(PageReadGuard {
                    inner: self.inner.clone(),
                    frame_id,
                    data,
                });
            }
        }

        // Page not in buffer pool, need to load from disk
        let frame_id = self.get_frame()?;

        // Load page from disk
        {
            let mut page_manager = self.inner.page_manager.lock();
            let mut frames = self.inner.frames.write();
            let frame = frames.get_mut(&frame_id).unwrap();

            page_manager.read_page(page_id, frame.data.as_mut())?;
            frame.page_id = Some(page_id);
            frame.pin_count.store(1, Ordering::SeqCst);
            frame.is_dirty.store(false, Ordering::SeqCst);
        }

        // Update page table
        self.inner.page_table.insert(page_id, frame_id);
        self.inner.replacer.lock().pin(frame_id);

        let frames = self.inner.frames.read();
        let frame = frames.get(&frame_id).unwrap();
        let data = frame.data.as_ref() as *const [u8; PAGE_SIZE];

        Ok(PageReadGuard {
            inner: self.inner.clone(),
            frame_id,
            data,
        })
    }

    pub fn fetch_page_write(&self, page_id: PageId) -> Result<PageWriteGuard> {
        // Check if page is already in buffer pool
        if let Some(frame_id) = self.inner.page_table.get(&page_id).map(|e| *e.value()) {
            let mut frames = self.inner.frames.write();
            if let Some(frame) = frames.get_mut(&frame_id) {
                frame.pin_count.fetch_add(1, Ordering::SeqCst);
                frame.is_dirty.store(true, Ordering::SeqCst);
                self.inner.replacer.lock().pin(frame_id);

                let data = frame.data.as_mut() as *mut [u8; PAGE_SIZE];
                drop(frames);

                return Ok(PageWriteGuard {
                    inner: self.inner.clone(),
                    frame_id,
                    data,
                });
            }
        }

        // Page not in buffer pool, need to load from disk
        let frame_id = self.get_frame()?;

        // Load page from disk
        {
            let mut page_manager = self.inner.page_manager.lock();
            let mut frames = self.inner.frames.write();
            let frame = frames.get_mut(&frame_id).unwrap();

            page_manager.read_page(page_id, frame.data.as_mut())?;
            frame.page_id = Some(page_id);
            frame.pin_count.store(1, Ordering::SeqCst);
            frame.is_dirty.store(true, Ordering::SeqCst);
        }

        // Update page table
        self.inner.page_table.insert(page_id, frame_id);
        self.inner.replacer.lock().pin(frame_id);

        let mut frames = self.inner.frames.write();
        let frame = frames.get_mut(&frame_id).unwrap();
        let data = frame.data.as_mut() as *mut [u8; PAGE_SIZE];
        drop(frames);

        Ok(PageWriteGuard {
            inner: self.inner.clone(),
            frame_id,
            data,
        })
    }

    pub fn new_page(&self) -> Result<(PageId, PageWriteGuard)> {
        let frame_id = self.get_frame()?;

        // Allocate new page
        let page_id = {
            let mut page_manager = self.inner.page_manager.lock();
            page_manager.allocate_page()?
        };

        // Initialize frame and get guard
        let mut frames = self.inner.frames.write();
        let frame = frames.get_mut(&frame_id).unwrap();
        frame.reset();
        frame.page_id = Some(page_id);
        frame.pin_count.store(1, Ordering::SeqCst);
        frame.is_dirty.store(true, Ordering::SeqCst);

        // Update page table
        self.inner.page_table.insert(page_id, frame_id);
        self.inner.replacer.lock().pin(frame_id);

        let data = frame.data.as_mut() as *mut [u8; PAGE_SIZE];
        drop(frames);

        Ok((
            page_id,
            PageWriteGuard {
                inner: self.inner.clone(),
                frame_id,
                data,
            },
        ))
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        if let Some(frame_id) = self.inner.page_table.get(&page_id).map(|e| *e.value()) {
            let frames = self.inner.frames.read();
            if let Some(frame) = frames.get(&frame_id) {
                if frame.is_dirty.load(Ordering::SeqCst) {
                    let mut page_manager = self.inner.page_manager.lock();
                    page_manager.write_page(page_id, frame.data.as_ref())?;
                    frame.is_dirty.store(false, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let frames = self.inner.frames.read();
        let mut page_manager = self.inner.page_manager.lock();

        for frame in frames.values() {
            if let Some(page_id) = frame.page_id {
                if frame.is_dirty.load(Ordering::SeqCst) {
                    page_manager.write_page(page_id, frame.data.as_ref())?;
                    frame.is_dirty.store(false, Ordering::SeqCst);
                }
            }
        }

        Ok(())
    }

    fn get_frame(&self) -> Result<FrameId> {
        // Try to allocate new frame if under limit
        {
            let frames = self.inner.frames.read();
            if frames.len() < self.inner.max_frames {
                drop(frames);
                let mut frames = self.inner.frames.write();
                // Double-check after acquiring write lock
                if frames.len() < self.inner.max_frames {
                    let frame_id = self.inner.next_frame_id.fetch_add(1, Ordering::SeqCst);
                    frames.insert(frame_id, Frame::new());
                    return Ok(frame_id);
                }
            }
        }

        // Need to evict a frame
        let evict_frame_id = {
            let mut replacer = self.inner.replacer.lock();
            replacer
                .evict()
                .ok_or_else(|| anyhow::anyhow!("No frame available for eviction"))?
        };

        // Collect information about the frame to evict
        let (old_page_id, is_dirty, data) = {
            let frames = self.inner.frames.read();
            if let Some(frame) = frames.get(&evict_frame_id) {
                (
                    frame.page_id,
                    frame.is_dirty.load(Ordering::SeqCst),
                    frame.data.clone(),
                )
            } else {
                return Ok(evict_frame_id);
            }
        };

        // Flush if dirty (without holding frames lock)
        if let Some(page_id) = old_page_id {
            if is_dirty {
                let mut page_manager = self.inner.page_manager.lock();
                page_manager.write_page(page_id, data.as_ref())?;
            }
            self.inner.page_table.remove(&page_id);
        }

        // Reset frame
        {
            let mut frames = self.inner.frames.write();
            if let Some(frame) = frames.get_mut(&evict_frame_id) {
                frame.reset();
            }
        }

        Ok(evict_frame_id)
    }
}

pub struct PageReadGuard {
    inner: Arc<BufferPoolInner>,
    frame_id: FrameId,
    data: *const [u8; PAGE_SIZE],
}

impl Deref for PageReadGuard {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        // Decrement pin count
        let should_unpin = {
            let frames = self.inner.frames.read();
            if let Some(frame) = frames.get(&self.frame_id) {
                let old_count = frame.pin_count.fetch_sub(1, Ordering::SeqCst);
                old_count == 1
            } else {
                false
            }
        };

        // Call unpin on the replacer if needed
        if should_unpin {
            self.inner.replacer.lock().unpin(self.frame_id);
        }
    }
}

pub struct PageWriteGuard {
    inner: Arc<BufferPoolInner>,
    frame_id: FrameId,
    data: *mut [u8; PAGE_SIZE],
}

impl Deref for PageWriteGuard {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl DerefMut for PageWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.data }
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        // Decrement pin count
        let should_unpin = {
            let frames = self.inner.frames.read();
            if let Some(frame) = frames.get(&self.frame_id) {
                let old_count = frame.pin_count.fetch_sub(1, Ordering::SeqCst);
                old_count == 1
            } else {
                false
            }
        };

        // Call unpin on the replacer if needed
        if should_unpin {
            self.inner.replacer.lock().unpin(self.frame_id);
        }
    }
}

// Mark as Send and Sync
unsafe impl Send for PageReadGuard {}
unsafe impl Sync for PageReadGuard {}
unsafe impl Send for PageWriteGuard {}
unsafe impl Sync for PageWriteGuard {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::PageManager;
    use crate::storage::page::{HeapPage, Page};
    use tempfile::tempdir;

    fn create_test_buffer_pool(max_frames: usize) -> Result<BufferPoolManager> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(max_frames));
        Ok(BufferPoolManager::new(page_manager, replacer, max_frames))
    }

    #[test]
    fn test_new_page() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(10)?;

        let (page_id, mut guard) = buffer_pool.new_page()?;
        assert_eq!(page_id, PageId(0));

        // Write some data
        guard[0] = 42;
        guard[1] = 43;

        drop(guard);

        // Fetch and verify
        let guard = buffer_pool.fetch_page(page_id)?;
        assert_eq!(guard[0], 42);
        assert_eq!(guard[1], 43);

        Ok(())
    }

    #[test]
    fn test_fetch_write() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(10)?;

        // Create a page
        let (page_id, mut guard) = buffer_pool.new_page()?;
        guard[0] = 10;
        drop(guard);

        // Fetch for write
        let mut guard = buffer_pool.fetch_page_write(page_id)?;
        guard[0] = 20;
        drop(guard);

        // Verify change
        let guard = buffer_pool.fetch_page(page_id)?;
        assert_eq!(guard[0], 20);

        Ok(())
    }

    #[test]
    fn test_eviction() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(2)?;

        // Create 3 pages (more than buffer pool capacity)
        let (page_id1, mut guard1) = buffer_pool.new_page()?;
        assert_eq!(page_id1.0, 0);
        guard1[0] = 1;
        drop(guard1);

        let (page_id2, mut guard2) = buffer_pool.new_page()?;
        assert_eq!(page_id2.0, 1);
        guard2[0] = 2;
        drop(guard2);

        let (page_id3, mut guard3) = buffer_pool.new_page()?;
        assert_eq!(page_id3.0, 2);
        guard3[0] = 3;
        drop(guard3);

        // Page 1 should have been evicted, fetch it again
        let guard1 = buffer_pool.fetch_page(page_id1)?;
        assert_eq!(guard1[0], 1); // Should be persisted

        // Also verify page 2 was persisted correctly
        let guard2 = buffer_pool.fetch_page(page_id2)?;
        assert_eq!(guard2[0], 2);

        Ok(())
    }

    #[test]
    fn test_flush() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(10)?;

        let (page_id, mut guard) = buffer_pool.new_page()?;
        guard[0] = 99;
        drop(guard);

        // Flush specific page
        buffer_pool.flush_page(page_id)?;

        // Create another buffer pool with same file
        let dir = tempdir()?;
        let file_path = dir.path().join("test2.db");
        let _page_manager = PageManager::create(&file_path)?;

        // Copy data from original
        buffer_pool.flush_all()?;

        Ok(())
    }

    #[test]
    fn test_pin_unpin() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(2)?;

        // Create page 1 but immediately unpin it
        let (page_id1, mut guard1) = buffer_pool.new_page()?;
        guard1[0] = 1;
        drop(guard1);

        // Create page 2 and keep it pinned
        let (_page_id2, guard2) = buffer_pool.new_page()?;

        // Create page 3 - should evict page 1 (page 2 is still pinned)
        let (_page_id3, mut guard3) = buffer_pool.new_page()?;
        guard3[0] = 3;
        drop(guard3);

        // Drop guard2 to unpin page 2
        drop(guard2);

        // Now fetch page 1 - should be loaded from disk
        let g1 = buffer_pool.fetch_page(page_id1)?;
        assert_eq!(g1[0], 1); // Should be persisted

        Ok(())
    }

    #[test]
    fn test_heap_page_integration() -> Result<()> {
        let buffer_pool = create_test_buffer_pool(10)?;

        // Create new page and use as HeapPage
        let (page_id, mut guard) = buffer_pool.new_page()?;
        {
            let _heap_page = HeapPage::new(&mut guard, page_id);
            // HeapPage operations would go here
        }
        drop(guard);

        // Fetch and verify
        let mut guard = buffer_pool.fetch_page_write(page_id)?;
        let heap_page = HeapPage::from_data(&mut guard);
        assert_eq!(heap_page.page_id(), page_id);

        Ok(())
    }
}
