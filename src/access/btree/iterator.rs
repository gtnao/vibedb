use crate::access::btree::key::BTreeKey;
use crate::access::value::DataType;
use crate::access::TupleId;
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::btree_leaf_page::BTreeLeafPage;
use crate::storage::page::PageId;
use crate::storage::PAGE_SIZE;
use anyhow::{bail, Result};

/// Direction of iteration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterDirection {
    Forward,
    Backward,
}

/// State of the iterator
#[derive(Debug)]
enum IterState {
    /// Iterator is positioned at a valid entry
    Valid { page_id: PageId, slot_index: usize },
    /// Iterator has exhausted all entries
    Exhausted,
    /// Iterator encountered an error
    #[allow(dead_code)]
    Error(String),
}

/// Iterator for B+Tree range scans
pub struct BTreeIterator {
    /// Buffer pool for page access
    buffer_pool: BufferPoolManager,
    /// Current state
    state: IterState,
    /// Direction of iteration
    direction: IterDirection,
    /// Optional start bound (inclusive)
    start_key: Option<BTreeKey>,
    /// Optional end bound (inclusive)
    end_key: Option<BTreeKey>,
    /// Whether to include the start key
    include_start: bool,
    /// Whether to include the end key
    include_end: bool,
    /// Current page data (cached)
    current_page: Option<Vec<u8>>,
    /// Key schema
    schema: Vec<DataType>,
    /// Statistics
    pages_visited: usize,
    keys_returned: usize,
    /// Version of current page for concurrent modification detection
    current_page_version: Option<u64>,
}

impl BTreeIterator {
    /// Extract version (LSN) from page data
    fn get_page_version(page_data: &[u8]) -> u64 {
        // LSN is at offset 10-17 in the header
        u64::from_le_bytes([
            page_data[10],
            page_data[11],
            page_data[12],
            page_data[13],
            page_data[14],
            page_data[15],
            page_data[16],
            page_data[17],
        ])
    }

    /// Verify page hasn't changed since last access
    fn verify_page_version(&self, page_id: PageId) -> Result<bool> {
        if let Some(expected_version) = self.current_page_version {
            let guard = self.buffer_pool.fetch_page(page_id)?;
            let current_version = Self::get_page_version(&*guard);
            Ok(current_version == expected_version)
        } else {
            // No version tracked yet
            Ok(true)
        }
    }
    /// Create a new iterator for forward scanning
    pub fn new_forward(
        buffer_pool: BufferPoolManager,
        start_page_id: PageId,
        schema: Vec<DataType>,
        start_key: Option<BTreeKey>,
        end_key: Option<BTreeKey>,
        include_start: bool,
        include_end: bool,
    ) -> Result<Self> {
        let mut iter = Self {
            buffer_pool,
            state: IterState::Valid {
                page_id: start_page_id,
                slot_index: 0,
            },
            direction: IterDirection::Forward,
            start_key,
            end_key,
            include_start,
            include_end,
            current_page: None,
            schema,
            pages_visited: 0,
            keys_returned: 0,
            current_page_version: None,
        };

        // Position iterator at the first valid entry
        iter.seek_to_start()?;
        Ok(iter)
    }

    /// Create a new iterator for backward scanning
    pub fn new_backward(
        buffer_pool: BufferPoolManager,
        start_page_id: PageId,
        schema: Vec<DataType>,
        start_key: Option<BTreeKey>,
        end_key: Option<BTreeKey>,
        include_start: bool,
        include_end: bool,
    ) -> Result<Self> {
        let mut iter = Self {
            buffer_pool,
            state: IterState::Valid {
                page_id: start_page_id,
                slot_index: 0,
            },
            direction: IterDirection::Backward,
            start_key,
            end_key,
            include_start,
            include_end,
            current_page: None,
            schema,
            pages_visited: 0,
            keys_returned: 0,
            current_page_version: None,
        };

        // Position iterator at the last valid entry
        iter.seek_to_end()?;
        Ok(iter)
    }

    /// Seek to the first valid entry
    fn seek_to_start(&mut self) -> Result<()> {
        match &self.state {
            IterState::Valid { page_id, .. } => {
                let page_id = *page_id;

                // Load the page
                let guard = self.buffer_pool.fetch_page(page_id)?;
                let mut page_data = [0u8; PAGE_SIZE];
                page_data.copy_from_slice(&*guard);
                self.current_page = Some(page_data.to_vec());
                self.pages_visited += 1;

                // Track page version for concurrent modification detection
                self.current_page_version = Some(Self::get_page_version(&page_data));

                let leaf_page = BTreeLeafPage::from_data(page_id, &mut page_data);

                // Find the first valid slot
                if let Some(ref start_key) = self.start_key {
                    // Find the first key >= start_key
                    for i in 0..leaf_page.slot_count() as usize {
                        if let Some(key_data) = leaf_page.get_key(i) {
                            let key =
                                BTreeKey::from_data(key_data.to_vec(), start_key.schema().to_vec());

                            match key.cmp(start_key) {
                                std::cmp::Ordering::Less => continue,
                                std::cmp::Ordering::Equal => {
                                    if self.include_start {
                                        self.state = IterState::Valid {
                                            page_id,
                                            slot_index: i,
                                        };
                                        return self.check_end_bound();
                                    } else {
                                        continue;
                                    }
                                }
                                std::cmp::Ordering::Greater => {
                                    self.state = IterState::Valid {
                                        page_id,
                                        slot_index: i,
                                    };
                                    return self.check_end_bound();
                                }
                            }
                        }
                    }

                    // No valid key in this page, try next page
                    if let Some(next_page_id) = leaf_page.next_page_id() {
                        self.state = IterState::Valid {
                            page_id: next_page_id,
                            slot_index: 0,
                        };
                        return self.seek_to_start();
                    } else {
                        self.state = IterState::Exhausted;
                    }
                } else {
                    // No start bound, begin at first slot
                    if leaf_page.slot_count() > 0 {
                        self.state = IterState::Valid {
                            page_id,
                            slot_index: 0,
                        };
                        self.check_end_bound()?;
                    } else {
                        // Empty page, try next
                        if let Some(next_page_id) = leaf_page.next_page_id() {
                            self.state = IterState::Valid {
                                page_id: next_page_id,
                                slot_index: 0,
                            };
                            return self.seek_to_start();
                        } else {
                            self.state = IterState::Exhausted;
                        }
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Seek to the last valid entry
    fn seek_to_end(&mut self) -> Result<()> {
        match &self.state {
            IterState::Valid { page_id, .. } => {
                let page_id = *page_id;

                // Load the page
                let guard = self.buffer_pool.fetch_page(page_id)?;
                let mut page_data = [0u8; PAGE_SIZE];
                page_data.copy_from_slice(&*guard);
                self.current_page = Some(page_data.to_vec());
                self.pages_visited += 1;

                // Track page version for concurrent modification detection
                self.current_page_version = Some(Self::get_page_version(&page_data));

                let leaf_page = BTreeLeafPage::from_data(page_id, &mut page_data);

                // Find the last valid slot
                if let Some(ref end_key) = self.end_key {
                    // Find the last key <= end_key
                    for i in (0..leaf_page.slot_count() as usize).rev() {
                        if let Some(key_data) = leaf_page.get_key(i) {
                            let key =
                                BTreeKey::from_data(key_data.to_vec(), end_key.schema().to_vec());

                            match key.cmp(end_key) {
                                std::cmp::Ordering::Greater => continue,
                                std::cmp::Ordering::Equal => {
                                    if self.include_end {
                                        self.state = IterState::Valid {
                                            page_id,
                                            slot_index: i,
                                        };
                                        return self.check_start_bound();
                                    } else {
                                        continue;
                                    }
                                }
                                std::cmp::Ordering::Less => {
                                    self.state = IterState::Valid {
                                        page_id,
                                        slot_index: i,
                                    };
                                    return self.check_start_bound();
                                }
                            }
                        }
                    }

                    // No valid key in this page, try previous page
                    if let Some(prev_page_id) = leaf_page.prev_page_id() {
                        self.state = IterState::Valid {
                            page_id: prev_page_id,
                            slot_index: 0,
                        };
                        return self.seek_to_end();
                    } else {
                        self.state = IterState::Exhausted;
                    }
                } else {
                    // No end bound, begin at last slot
                    let key_count = leaf_page.slot_count() as usize;
                    if key_count > 0 {
                        self.state = IterState::Valid {
                            page_id,
                            slot_index: key_count - 1,
                        };
                        self.check_start_bound()?;
                    } else {
                        // Empty page, try previous
                        if let Some(prev_page_id) = leaf_page.prev_page_id() {
                            self.state = IterState::Valid {
                                page_id: prev_page_id,
                                slot_index: 0,
                            };
                            return self.seek_to_end();
                        } else {
                            self.state = IterState::Exhausted;
                        }
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Check if current position violates end bound
    fn check_end_bound(&mut self) -> Result<()> {
        if self.end_key.is_some() {
            if let Some((key, _)) = self.current_internal()? {
                let end_key = self.end_key.as_ref().unwrap();
                match key.cmp(end_key) {
                    std::cmp::Ordering::Greater => {
                        self.state = IterState::Exhausted;
                    }
                    std::cmp::Ordering::Equal => {
                        if !self.include_end {
                            self.state = IterState::Exhausted;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Check if current position violates start bound
    fn check_start_bound(&mut self) -> Result<()> {
        if self.start_key.is_some() {
            if let Some((key, _)) = self.current_internal()? {
                let start_key = self.start_key.as_ref().unwrap();
                match key.cmp(start_key) {
                    std::cmp::Ordering::Less => {
                        self.state = IterState::Exhausted;
                    }
                    std::cmp::Ordering::Equal => {
                        if !self.include_start {
                            self.state = IterState::Exhausted;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Get current key-value without advancing
    fn current_internal(&mut self) -> Result<Option<(BTreeKey, TupleId)>> {
        match &self.state {
            IterState::Valid {
                page_id,
                slot_index,
            } => {
                // Check if page has been modified since last access
                if self.current_page.is_some() && !self.verify_page_version(*page_id)? {
                    // Page has been modified, need to re-read and re-position
                    bail!("Page modified during iteration");
                }

                // Ensure we have the current page cached
                if self.current_page.is_none() {
                    let guard = self.buffer_pool.fetch_page(*page_id)?;
                    let page_data = guard.to_vec();
                    self.current_page_version = Some(Self::get_page_version(&page_data));
                    self.current_page = Some(page_data);
                    self.pages_visited += 1;
                }

                let page_data = self.current_page.as_ref().unwrap();
                let mut page_data_array = [0u8; PAGE_SIZE];
                page_data_array.copy_from_slice(page_data);
                let leaf_page = BTreeLeafPage::from_data(*page_id, &mut page_data_array);

                if let Some(key_data) = leaf_page.get_key(*slot_index) {
                    if let Some(tuple_id) = leaf_page.get_tuple_id(*slot_index) {
                        let key = BTreeKey::from_data(key_data.to_vec(), self.schema.clone());
                        Ok(Some((key, tuple_id)))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Get current key-value pair
    pub fn current(&mut self) -> Result<Option<(BTreeKey, TupleId)>> {
        self.current_internal()
    }

    /// Move to next entry
    pub fn advance(&mut self) -> Result<Option<(BTreeKey, TupleId)>> {
        match self.direction {
            IterDirection::Forward => self.move_forward(),
            IterDirection::Backward => self.move_backward(),
        }
    }

    /// Move forward to next entry
    fn move_forward(&mut self) -> Result<Option<(BTreeKey, TupleId)>> {
        match &self.state {
            IterState::Valid {
                page_id,
                slot_index,
            } => {
                let current_page_id = *page_id;
                let current_slot = *slot_index;

                // Get the current value before moving
                let current_value = self.current_internal()?;

                // Load page if not cached
                if self.current_page.is_none() {
                    let guard = self.buffer_pool.fetch_page(current_page_id)?;
                    let page_data = guard.to_vec();
                    self.current_page = Some(page_data);
                    self.pages_visited += 1;
                }

                let page_data = self.current_page.as_ref().unwrap();
                let mut page_data_array = [0u8; PAGE_SIZE];
                page_data_array.copy_from_slice(page_data);
                let leaf_page = BTreeLeafPage::from_data(current_page_id, &mut page_data_array);

                // Try next slot
                let next_slot = current_slot + 1;
                if next_slot < leaf_page.slot_count() as usize {
                    self.state = IterState::Valid {
                        page_id: current_page_id,
                        slot_index: next_slot,
                    };
                    self.check_end_bound()?;
                } else {
                    // Move to next page
                    if let Some(next_page_id) = leaf_page.next_page_id() {
                        self.state = IterState::Valid {
                            page_id: next_page_id,
                            slot_index: 0,
                        };
                        self.current_page = None; // Clear cache
                        self.current_page_version = None; // Clear version
                        self.check_end_bound()?;
                    } else {
                        self.state = IterState::Exhausted;
                    }
                }

                if current_value.is_some() {
                    self.keys_returned += 1;
                }
                Ok(current_value)
            }
            IterState::Exhausted => Ok(None),
            IterState::Error(e) => bail!("Iterator in error state: {}", e),
        }
    }

    /// Move backward to previous entry
    fn move_backward(&mut self) -> Result<Option<(BTreeKey, TupleId)>> {
        match &self.state {
            IterState::Valid {
                page_id,
                slot_index,
            } => {
                let current_page_id = *page_id;
                let current_slot = *slot_index;

                // Get the current value before moving
                let current_value = self.current_internal()?;

                // Load page if not cached
                if self.current_page.is_none() {
                    let guard = self.buffer_pool.fetch_page(current_page_id)?;
                    let page_data = guard.to_vec();
                    self.current_page = Some(page_data);
                    self.pages_visited += 1;
                }

                let page_data = self.current_page.as_ref().unwrap();
                let mut page_data_array = [0u8; PAGE_SIZE];
                page_data_array.copy_from_slice(page_data);
                let leaf_page = BTreeLeafPage::from_data(current_page_id, &mut page_data_array);

                // Try previous slot
                if current_slot > 0 {
                    self.state = IterState::Valid {
                        page_id: current_page_id,
                        slot_index: current_slot - 1,
                    };
                    self.check_start_bound()?;
                } else {
                    // Move to previous page
                    if let Some(prev_page_id) = leaf_page.prev_page_id() {
                        // Load previous page to get its key count
                        let prev_guard = self.buffer_pool.fetch_page(prev_page_id)?;
                        let mut prev_data_array = [0u8; PAGE_SIZE];
                        prev_data_array.copy_from_slice(&*prev_guard);
                        let prev_leaf =
                            BTreeLeafPage::from_data(prev_page_id, &mut prev_data_array);
                        let prev_key_count = prev_leaf.slot_count() as usize;

                        if prev_key_count > 0 {
                            self.state = IterState::Valid {
                                page_id: prev_page_id,
                                slot_index: prev_key_count - 1,
                            };
                            self.current_page = Some(prev_data_array.to_vec());
                            self.current_page_version =
                                Some(Self::get_page_version(&prev_data_array));
                            self.pages_visited += 1;
                            self.check_start_bound()?;
                        } else {
                            // Empty page, continue to previous
                            self.state = IterState::Valid {
                                page_id: prev_page_id,
                                slot_index: 0,
                            };
                            self.current_page = None;
                            self.current_page_version = None;
                            return self.move_backward();
                        }
                    } else {
                        self.state = IterState::Exhausted;
                    }
                }

                if current_value.is_some() {
                    self.keys_returned += 1;
                }
                Ok(current_value)
            }
            IterState::Exhausted => Ok(None),
            IterState::Error(e) => bail!("Iterator in error state: {}", e),
        }
    }

    /// Seek to a specific key
    pub fn seek(&mut self, target_key: &BTreeKey) -> Result<()> {
        // For simplicity, we'll restart from the beginning and seek
        // In a production implementation, we'd navigate the tree to find the right page
        match self.direction {
            IterDirection::Forward => {
                self.start_key = Some(target_key.clone());
                self.include_start = true;
                self.seek_to_start()
            }
            IterDirection::Backward => {
                self.end_key = Some(target_key.clone());
                self.include_end = true;
                self.seek_to_end()
            }
        }
    }

    /// Check if iterator is valid
    pub fn is_valid(&self) -> bool {
        matches!(self.state, IterState::Valid { .. })
    }

    /// Get statistics
    pub fn statistics(&self) -> (usize, usize) {
        (self.pages_visited, self.keys_returned)
    }
}

impl Drop for BTreeIterator {
    fn drop(&mut self) {
        // Nothing special to do - page guards are automatically released
        // In a production implementation with latches, we'd release them here
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::value::{DataType, Value};
    use crate::storage::buffer::{lru, BufferPoolManager};
    use crate::storage::disk::PageManager;
    use crate::storage::page::btree_leaf_page::BTreeLeafPage;
    use crate::storage::page::Page;
    use tempfile::TempDir;

    fn create_test_leaf_page(
        buffer_pool: &mut BufferPoolManager,
        keys: Vec<i32>,
        schema: &[DataType],
    ) -> Result<PageId> {
        let (page_id, mut guard) = buffer_pool.new_page()?;
        let mut leaf_page = BTreeLeafPage::new(page_id);

        for (i, key) in keys.iter().enumerate() {
            let values = vec![Value::Int32(*key)];
            let key_data = crate::access::value::serialize_values(&values, schema)?;
            let tuple_id = TupleId::new(PageId(100), i as u16);
            leaf_page
                .insert_key_value(&key_data, tuple_id)
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        guard.copy_from_slice(leaf_page.data());
        Ok(page_id)
    }

    #[test]
    fn test_iterator_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];

        // Create a page first
        let (page_id, mut guard) = buffer_pool.new_page()?;
        let leaf_page = BTreeLeafPage::new(page_id);
        guard.copy_from_slice(leaf_page.data());
        drop(guard);

        let start_key = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
        let end_key = BTreeKey::from_values(&[Value::Int32(20)], &schema)?;

        let iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema,
            Some(start_key),
            Some(end_key),
            true,
            true,
        )?;

        assert!(matches!(iter.direction, IterDirection::Forward));
        Ok(())
    }

    #[test]
    fn test_forward_iteration() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let start_key = BTreeKey::from_values(&[Value::Int32(5)], &schema)?;
        let end_key = BTreeKey::from_values(&[Value::Int32(15)], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            Some(start_key),
            Some(end_key),
            true,
            true,
        )?;

        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![5, 10, 15]);
        Ok(())
    }

    #[test]
    fn test_backward_iteration() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let start_key = BTreeKey::from_values(&[Value::Int32(5)], &schema)?;
        let end_key = BTreeKey::from_values(&[Value::Int32(15)], &schema)?;

        let mut iter = BTreeIterator::new_backward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            Some(start_key),
            Some(end_key),
            true,
            true,
        )?;

        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![15, 10, 5]);
        Ok(())
    }

    #[test]
    fn test_unbounded_scan() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![1, 5, 10, 15, 20]);
        Ok(())
    }

    #[test]
    fn test_exclusive_bounds() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let start_key = BTreeKey::from_values(&[Value::Int32(5)], &schema)?;
        let end_key = BTreeKey::from_values(&[Value::Int32(15)], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            Some(start_key),
            Some(end_key),
            false, // Exclude start
            false, // Exclude end
        )?;

        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![10]); // Only 10 is within (5, 15)
        Ok(())
    }

    #[test]
    fn test_iterator_statistics() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Iterate through all entries
        while iter.advance()?.is_some() {}

        let (pages_visited, keys_returned) = iter.statistics();
        assert_eq!(pages_visited, 1);
        assert_eq!(keys_returned, 3);
        Ok(())
    }

    #[test]
    fn test_empty_page_iteration() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        // Create an empty leaf page
        let (page_id, mut guard) = buffer_pool.new_page()?;
        let leaf_page = BTreeLeafPage::new(page_id);
        guard.copy_from_slice(leaf_page.data());
        drop(guard);

        let schema = vec![DataType::Int32]; // Empty page, but we still need a schema
        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema,
            None,
            None,
            true,
            true,
        )?;

        assert!(iter.advance()?.is_none());
        Ok(())
    }

    #[test]
    fn test_seek_forward() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Seek to 10
        let seek_key = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
        iter.seek(&seek_key)?;

        // Should start from 10
        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![10, 15, 20]);
        Ok(())
    }

    #[test]
    fn test_seek_backward() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let mut iter = BTreeIterator::new_backward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Seek to 10
        let seek_key = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
        iter.seek(&seek_key)?;

        // Should start from 10 and go backward
        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![10, 5, 1]);
        Ok(())
    }

    #[test]
    fn test_seek_non_existent() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Seek to 12 (doesn't exist)
        let seek_key = BTreeKey::from_values(&[Value::Int32(12)], &schema)?;
        iter.seek(&seek_key)?;

        // Should start from 15 (next key after 12)
        let mut results = Vec::new();
        while let Some((key, _tuple_id)) = iter.advance()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                results.push(*v);
            }
        }

        assert_eq!(results, vec![15, 20]);
        Ok(())
    }

    #[test]
    fn test_concurrent_modification_detection() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10], &schema)?;

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Get first value
        let first = iter.current()?;
        assert!(first.is_some());

        // Simulate concurrent modification by updating page version
        // We'll modify the page directly
        {
            let mut guard = buffer_pool.fetch_page_write(page_id)?;
            // Change the LSN (version) at offset 10-17
            guard[10] = 42; // Change version
        }

        // Try to advance - should detect modification
        let result = iter.advance();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Page modified during iteration"));
        }

        Ok(())
    }

    #[test]
    fn test_version_tracking_across_pages() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];

        // Create two linked pages
        let page_id1 = create_test_leaf_page(&mut buffer_pool, vec![1, 2, 3], &schema)?;
        let page_id2 = create_test_leaf_page(&mut buffer_pool, vec![4, 5, 6], &schema)?;

        // Link the pages
        {
            let mut guard1 = buffer_pool.fetch_page_write(page_id1)?;
            let mut leaf1 = BTreeLeafPage::from_data(page_id1, &mut *guard1);
            leaf1.set_next_page_id(Some(page_id2));
            guard1.copy_from_slice(leaf1.data());
        }
        {
            let mut guard2 = buffer_pool.fetch_page_write(page_id2)?;
            let mut leaf2 = BTreeLeafPage::from_data(page_id2, &mut *guard2);
            leaf2.set_prev_page_id(Some(page_id1));
            guard2.copy_from_slice(leaf2.data());
        }

        let mut iter = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id1,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        // Iterate through first page
        let mut count = 0;
        while count < 3 {
            assert!(iter.advance()?.is_some());
            count += 1;
        }

        // When we move to the second page, version tracking should reset
        // This should work fine even though we moved pages
        assert!(iter.advance()?.is_some()); // Should get 4

        Ok(())
    }

    #[test]
    fn test_drop_behavior() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10], &schema)?;

        // Create iterator in a scope
        {
            let mut iter = BTreeIterator::new_forward(
                buffer_pool.clone(),
                page_id,
                schema.clone(),
                None,
                None,
                true,
                true,
            )?;

            // Use the iterator
            assert!(iter.current()?.is_some());
            assert!(iter.advance()?.is_some());

            // Iterator will be dropped here
        }

        // After drop, we should still be able to access the page
        // This verifies that the iterator properly released any resources
        let guard = buffer_pool.fetch_page(page_id)?;
        assert_eq!(guard.len(), PAGE_SIZE);

        Ok(())
    }

    #[test]
    fn test_multiple_iterators() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(lru::LruReplacer::new(10));
        let mut buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);

        let schema = vec![DataType::Int32];
        let page_id = create_test_leaf_page(&mut buffer_pool, vec![1, 5, 10, 15, 20], &schema)?;

        // Create multiple iterators on the same page
        let mut iter1 = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            None,
            None,
            true,
            true,
        )?;

        let mut iter2 = BTreeIterator::new_forward(
            buffer_pool.clone(),
            page_id,
            schema.clone(),
            Some(BTreeKey::from_values(&[Value::Int32(10)], &schema)?),
            None,
            true,
            true,
        )?;

        // Both iterators should work independently
        if let Some((key, _)) = iter1.current()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                assert_eq!(*v, 1);
            }
        }

        if let Some((key, _)) = iter2.current()? {
            let values = key.to_values()?;
            if let Value::Int32(v) = &values[0] {
                assert_eq!(*v, 10);
            }
        }

        // Advance both
        assert!(iter1.advance()?.is_some());
        assert!(iter2.advance()?.is_some());

        // Drop iter1 while iter2 is still active
        drop(iter1);

        // iter2 should still work
        assert!(iter2.advance()?.is_some());

        Ok(())
    }
}
