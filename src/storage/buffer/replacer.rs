use std::fmt::Debug;

pub type FrameId = u32;

pub trait Replacer: Send + Sync + Debug {
    /// Select a frame to evict. Returns None if no frame can be evicted.
    fn evict(&mut self) -> Option<FrameId>;

    /// Mark a frame as pinned (not evictable).
    fn pin(&mut self, frame_id: FrameId);

    /// Mark a frame as unpinned (evictable).
    fn unpin(&mut self, frame_id: FrameId);

    /// Get the number of evictable frames.
    fn size(&self) -> usize;
}
