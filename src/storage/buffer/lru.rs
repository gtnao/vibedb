use super::replacer::{FrameId, Replacer};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct LruReplacer {
    /// Queue of evictable frames (least recently used at front)
    lru_list: VecDeque<FrameId>,
    /// Map to track position in LRU list for O(1) removal
    frame_map: HashMap<FrameId, usize>,
    /// Maximum number of frames
    max_size: usize,
}

impl LruReplacer {
    pub fn new(max_size: usize) -> Self {
        Self {
            lru_list: VecDeque::with_capacity(max_size),
            frame_map: HashMap::with_capacity(max_size),
            max_size,
        }
    }

    fn update_indices(&mut self) {
        // Update all indices in the map after modification
        for (idx, &frame_id) in self.lru_list.iter().enumerate() {
            self.frame_map.insert(frame_id, idx);
        }
    }
}

impl Replacer for LruReplacer {
    fn evict(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.lru_list.pop_front() {
            self.frame_map.remove(&frame_id);
            self.update_indices();
            Some(frame_id)
        } else {
            None
        }
    }

    fn pin(&mut self, frame_id: FrameId) {
        if let Some(&idx) = self.frame_map.get(&frame_id) {
            self.lru_list.remove(idx);
            self.frame_map.remove(&frame_id);
            self.update_indices();
        }
    }

    fn unpin(&mut self, frame_id: FrameId) {
        if !self.frame_map.contains_key(&frame_id) && self.lru_list.len() < self.max_size {
            self.lru_list.push_back(frame_id);
            self.frame_map.insert(frame_id, self.lru_list.len() - 1);
        }
    }

    fn size(&self) -> usize {
        self.lru_list.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lru_operations() {
        let mut replacer = LruReplacer::new(3);

        // Initially empty
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);

        // Unpin frames
        replacer.unpin(1);
        replacer.unpin(2);
        replacer.unpin(3);
        assert_eq!(replacer.size(), 3);

        // Evict in FIFO order (for LRU, first unpinned is first evicted)
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_pin_unpin() {
        let mut replacer = LruReplacer::new(3);

        replacer.unpin(1);
        replacer.unpin(2);
        assert_eq!(replacer.size(), 2);

        // Pin frame 1
        replacer.pin(1);
        assert_eq!(replacer.size(), 1);

        // Only frame 2 should be evictable
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.evict(), None);

        // Unpin frame 1 again
        replacer.unpin(1);
        assert_eq!(replacer.evict(), Some(1));
    }

    #[test]
    fn test_duplicate_unpin() {
        let mut replacer = LruReplacer::new(2);

        replacer.unpin(1);
        assert_eq!(replacer.size(), 1);

        // Duplicate unpin should be ignored
        replacer.unpin(1);
        assert_eq!(replacer.size(), 1);
    }

    #[test]
    fn test_pin_non_existent() {
        let mut replacer = LruReplacer::new(2);

        // Pinning non-existent frame should be safe
        replacer.pin(999);
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_max_size_limit() {
        let mut replacer = LruReplacer::new(2);

        replacer.unpin(1);
        replacer.unpin(2);
        replacer.unpin(3); // Should be ignored due to max size

        assert_eq!(replacer.size(), 2);
    }

    #[test]
    fn test_complex_scenario() {
        let mut replacer = LruReplacer::new(3);

        // Add frames
        replacer.unpin(1);
        replacer.unpin(2);
        replacer.unpin(3);

        // Pin middle frame
        replacer.pin(2);
        assert_eq!(replacer.size(), 2);

        // Evict
        assert_eq!(replacer.evict(), Some(1));

        // Unpin 2 again
        replacer.unpin(2);

        // Add new frame
        replacer.unpin(4);

        // Should evict in order: 3, 2, 4
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.evict(), Some(4));
    }
}
