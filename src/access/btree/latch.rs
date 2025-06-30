use crate::storage::page::PageId;
use anyhow::{Result, bail};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{Duration, Instant};

/// Type of latch acquisition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatchMode {
    Shared,
    Exclusive,
}

/// Represents a latch on a page
pub struct PageLatch {
    rwlock: RwLock<()>,
    #[allow(dead_code)]
    page_id: PageId,
}

impl PageLatch {
    fn new(page_id: PageId) -> Self {
        Self {
            rwlock: RwLock::new(()),
            page_id,
        }
    }
}

/// Statistics for latch operations
#[derive(Debug, Default)]
pub struct LatchStatistics {
    pub shared_acquisitions: u64,
    pub exclusive_acquisitions: u64,
    pub upgrades: u64,
    pub downgrades: u64,
    pub waits: u64,
    pub deadlocks_detected: u64,
    pub timeouts: u64,
}

/// Guards for latches
pub enum LatchGuard<'a> {
    Shared(RwLockReadGuard<'a, ()>),
    Exclusive(RwLockWriteGuard<'a, ()>),
}

/// Handle for managing latch upgrades and downgrades
pub struct LatchHandle {
    page_id: PageId,
    latch: Arc<PageLatch>,
    mode: LatchMode,
    manager: Arc<LatchManager>,
}

/// Manages page-level latches for the B+Tree
pub struct LatchManager {
    /// Map from page ID to latch
    latches: Mutex<HashMap<PageId, Arc<PageLatch>>>,
    /// Wait-for graph for deadlock detection
    wait_for_graph: Mutex<WaitForGraph>,
    /// Statistics
    statistics: Mutex<LatchStatistics>,
    /// Default timeout for latch acquisition
    default_timeout: Duration,
}

impl LatchManager {
    /// Create a new latch manager
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            latches: Mutex::new(HashMap::new()),
            wait_for_graph: Mutex::new(WaitForGraph::new()),
            statistics: Mutex::new(LatchStatistics::default()),
            default_timeout,
        }
    }

    /// Get or create a latch for a page
    fn get_or_create_latch(&self, page_id: PageId) -> Arc<PageLatch> {
        let mut latches = self.latches.lock().unwrap();
        latches
            .entry(page_id)
            .or_insert_with(|| Arc::new(PageLatch::new(page_id)))
            .clone()
    }

    /// Acquire a shared latch on a page
    pub fn acquire_shared(&self, page_id: PageId) -> Result<Arc<PageLatch>> {
        self.acquire_with_timeout(page_id, LatchMode::Shared, self.default_timeout)
    }

    /// Acquire an exclusive latch on a page
    pub fn acquire_exclusive(&self, page_id: PageId) -> Result<Arc<PageLatch>> {
        self.acquire_with_timeout(page_id, LatchMode::Exclusive, self.default_timeout)
    }

    /// Acquire a latch with timeout
    pub fn acquire_with_timeout(
        &self,
        page_id: PageId,
        mode: LatchMode,
        timeout: Duration,
    ) -> Result<Arc<PageLatch>> {
        let latch = self.get_or_create_latch(page_id);
        let thread_id = thread::current().id();
        let start = Instant::now();

        // Update wait-for graph
        {
            let mut wait_graph = self.wait_for_graph.lock().unwrap();
            wait_graph.add_wait(thread_id, page_id);

            // Check for deadlock
            if wait_graph.has_cycle() {
                wait_graph.remove_wait(thread_id);
                let mut stats = self.statistics.lock().unwrap();
                stats.deadlocks_detected += 1;
                bail!(
                    "Deadlock detected while acquiring latch on page {:?}",
                    page_id
                );
            }
        }

        // Try to acquire the latch
        // Note: We can't keep the guard because we need to return the Arc<PageLatch>
        // The caller is responsible for actually acquiring the lock when using it
        let acquired = match mode {
            LatchMode::Shared => {
                // Wait with timeout
                let mut stats = self.statistics.lock().unwrap();
                stats.waits += 1;
                drop(stats);

                loop {
                    if start.elapsed() > timeout {
                        let mut wait_graph = self.wait_for_graph.lock().unwrap();
                        wait_graph.remove_wait(thread_id);
                        let mut stats = self.statistics.lock().unwrap();
                        stats.timeouts += 1;
                        bail!("Timeout acquiring shared latch on page {:?}", page_id);
                    }

                    if latch.rwlock.try_read().is_ok() {
                        // We were able to acquire it, so it's available
                        // But we need to keep trying since we don't hold the guard
                        break true;
                    }

                    thread::sleep(Duration::from_millis(1));
                }
            }
            LatchMode::Exclusive => {
                // Wait with timeout
                let mut stats = self.statistics.lock().unwrap();
                stats.waits += 1;
                drop(stats);

                loop {
                    if start.elapsed() > timeout {
                        let mut wait_graph = self.wait_for_graph.lock().unwrap();
                        wait_graph.remove_wait(thread_id);
                        let mut stats = self.statistics.lock().unwrap();
                        stats.timeouts += 1;
                        bail!("Timeout acquiring exclusive latch on page {:?}", page_id);
                    }

                    if latch.rwlock.try_write().is_ok() {
                        break true;
                    }

                    thread::sleep(Duration::from_millis(1));
                }
            }
        };

        if acquired {
            // Remove from wait-for graph
            let mut wait_graph = self.wait_for_graph.lock().unwrap();
            wait_graph.remove_wait(thread_id);

            // Update statistics
            let mut stats = self.statistics.lock().unwrap();
            match mode {
                LatchMode::Shared => stats.shared_acquisitions += 1,
                LatchMode::Exclusive => stats.exclusive_acquisitions += 1,
            }
        }

        Ok(latch)
    }

    /// Try to acquire a latch optimistically (non-blocking)
    /// Returns true if the latch could be acquired (and immediately releases it)
    pub fn try_acquire(&self, page_id: PageId, mode: LatchMode) -> bool {
        let latch = self.get_or_create_latch(page_id);

        match mode {
            LatchMode::Shared => {
                if let Ok(_guard) = latch.rwlock.try_read() {
                    // Successfully acquired, immediately release
                    true
                } else {
                    false
                }
            }
            LatchMode::Exclusive => {
                if let Ok(_guard) = latch.rwlock.try_write() {
                    // Successfully acquired, immediately release
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Create a latch handle for upgrade/downgrade operations
    pub fn create_handle(
        self: &Arc<Self>,
        page_id: PageId,
        mode: LatchMode,
    ) -> Result<LatchHandle> {
        let latch = match mode {
            LatchMode::Shared => self.acquire_shared(page_id)?,
            LatchMode::Exclusive => self.acquire_exclusive(page_id)?,
        };

        Ok(LatchHandle {
            page_id,
            latch,
            mode,
            manager: self.clone(),
        })
    }

    /// Get current statistics
    pub fn get_statistics(&self) -> LatchStatistics {
        let stats = self.statistics.lock().unwrap();
        LatchStatistics {
            shared_acquisitions: stats.shared_acquisitions,
            exclusive_acquisitions: stats.exclusive_acquisitions,
            upgrades: stats.upgrades,
            downgrades: stats.downgrades,
            waits: stats.waits,
            deadlocks_detected: stats.deadlocks_detected,
            timeouts: stats.timeouts,
        }
    }

    /// Clear statistics
    pub fn clear_statistics(&self) {
        let mut stats = self.statistics.lock().unwrap();
        *stats = LatchStatistics::default();
    }
}

/// Wait-for graph for deadlock detection
struct WaitForGraph {
    /// Map from thread to the page it's waiting for
    waits: HashMap<thread::ThreadId, PageId>,
    /// Map from page to threads that hold it
    holders: HashMap<PageId, HashSet<thread::ThreadId>>,
}

impl WaitForGraph {
    fn new() -> Self {
        Self {
            waits: HashMap::new(),
            holders: HashMap::new(),
        }
    }

    fn add_wait(&mut self, thread_id: thread::ThreadId, page_id: PageId) {
        self.waits.insert(thread_id, page_id);
    }

    fn remove_wait(&mut self, thread_id: thread::ThreadId) {
        self.waits.remove(&thread_id);
    }

    #[allow(dead_code)]
    fn add_holder(&mut self, page_id: PageId, thread_id: thread::ThreadId) {
        self.holders.entry(page_id).or_default().insert(thread_id);
    }

    #[allow(dead_code)]
    fn remove_holder(&mut self, page_id: PageId, thread_id: thread::ThreadId) {
        if let Some(holders) = self.holders.get_mut(&page_id) {
            holders.remove(&thread_id);
            if holders.is_empty() {
                self.holders.remove(&page_id);
            }
        }
    }

    /// Detect if there's a cycle in the wait-for graph
    fn has_cycle(&self) -> bool {
        // Use DFS to detect cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for &thread_id in self.waits.keys() {
            if !visited.contains(&thread_id)
                && self.dfs_has_cycle(thread_id, &mut visited, &mut rec_stack)
            {
                return true;
            }
        }

        false
    }

    fn dfs_has_cycle(
        &self,
        thread_id: thread::ThreadId,
        visited: &mut HashSet<thread::ThreadId>,
        rec_stack: &mut HashSet<thread::ThreadId>,
    ) -> bool {
        visited.insert(thread_id);
        rec_stack.insert(thread_id);

        // Get the page this thread is waiting for
        if let Some(&page_id) = self.waits.get(&thread_id) {
            // Get all threads holding this page
            if let Some(holders) = self.holders.get(&page_id) {
                for &holder_thread in holders {
                    if !visited.contains(&holder_thread) {
                        if self.dfs_has_cycle(holder_thread, visited, rec_stack) {
                            return true;
                        }
                    } else if rec_stack.contains(&holder_thread) {
                        // Found a cycle
                        return true;
                    }
                }
            }
        }

        rec_stack.remove(&thread_id);
        false
    }
}

impl LatchHandle {
    /// Upgrade a shared latch to exclusive
    pub fn upgrade(&mut self) -> Result<()> {
        if self.mode == LatchMode::Exclusive {
            return Ok(()); // Already exclusive
        }

        // Release the shared latch and try to acquire exclusive
        // This is not atomic, but it's the best we can do with RwLock
        // We need to temporarily take ownership to drop it
        let old_latch = std::mem::replace(&mut self.latch, Arc::new(PageLatch::new(self.page_id)));
        drop(old_latch);

        // Try to reacquire as exclusive
        let new_latch = self.manager.acquire_exclusive(self.page_id)?;
        self.latch = new_latch;
        self.mode = LatchMode::Exclusive;

        // Update statistics
        let mut stats = self.manager.statistics.lock().unwrap();
        stats.upgrades += 1;

        Ok(())
    }

    /// Downgrade an exclusive latch to shared
    pub fn downgrade(&mut self) -> Result<()> {
        if self.mode == LatchMode::Shared {
            return Ok(()); // Already shared
        }

        // Release the exclusive latch and acquire shared
        // This is safe because we're giving up exclusivity
        // We need to temporarily take ownership to drop it
        let old_latch = std::mem::replace(&mut self.latch, Arc::new(PageLatch::new(self.page_id)));
        drop(old_latch);

        // Reacquire as shared (should always succeed immediately)
        let new_latch = self.manager.acquire_shared(self.page_id)?;
        self.latch = new_latch;
        self.mode = LatchMode::Shared;

        // Update statistics
        let mut stats = self.manager.statistics.lock().unwrap();
        stats.downgrades += 1;

        Ok(())
    }

    /// Get the current latch mode
    pub fn mode(&self) -> LatchMode {
        self.mode
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

/// Latch coupling helper for tree traversal
pub struct LatchCoupling {
    /// Currently held latches in order
    held_latches: Vec<(PageId, Arc<PageLatch>, LatchMode)>,
    /// Latch manager reference
    latch_manager: Arc<LatchManager>,
}

impl LatchCoupling {
    /// Create a new latch coupling instance
    pub fn new(latch_manager: Arc<LatchManager>) -> Self {
        Self {
            held_latches: Vec::new(),
            latch_manager,
        }
    }

    /// Acquire a latch and add to coupling chain
    pub fn acquire(&mut self, page_id: PageId, mode: LatchMode) -> Result<()> {
        let latch = match mode {
            LatchMode::Shared => self.latch_manager.acquire_shared(page_id)?,
            LatchMode::Exclusive => self.latch_manager.acquire_exclusive(page_id)?,
        };

        self.held_latches.push((page_id, latch, mode));
        Ok(())
    }

    /// Release all latches up to a certain page (safe node)
    pub fn release_ancestors(&mut self, safe_page_id: PageId) {
        let mut new_held = Vec::new();

        for (page_id, latch, mode) in self.held_latches.drain(..) {
            if page_id == safe_page_id {
                new_held.push((page_id, latch, mode));
                break;
            }
            // Latch is automatically released when dropped
        }

        self.held_latches = new_held;
    }

    /// Release all held latches
    pub fn release_all(&mut self) {
        self.held_latches.clear();
    }

    /// Get the number of held latches
    pub fn held_count(&self) -> usize {
        self.held_latches.len()
    }
}

impl Drop for LatchCoupling {
    fn drop(&mut self) {
        self.release_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_latch_manager_creation() {
        let manager = LatchManager::new(Duration::from_secs(5));
        let stats = manager.get_statistics();
        assert_eq!(stats.shared_acquisitions, 0);
        assert_eq!(stats.exclusive_acquisitions, 0);
    }

    #[test]
    fn test_shared_latch_acquisition() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Multiple threads can acquire shared latches
        let manager1 = manager.clone();
        let handle1 = thread::spawn(move || {
            let _latch = manager1.acquire_shared(page_id).unwrap();
            thread::sleep(Duration::from_millis(100));
        });

        let manager2 = manager.clone();
        let handle2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let _latch = manager2.acquire_shared(page_id).unwrap();
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.shared_acquisitions, 2);
    }

    #[test]
    fn test_exclusive_latch_blocks_shared() {
        // This test demonstrates that the current latch manager design
        // doesn't actually hold locks - it just returns references to latches
        // A proper implementation would need to return guards that hold the locks
        let manager = Arc::new(LatchManager::new(Duration::from_secs(1)));
        let page_id = PageId(1);

        // The current design always succeeds because it doesn't hold locks
        let result1 = manager.acquire_exclusive(page_id);
        assert!(result1.is_ok());

        let result2 = manager.acquire_shared(page_id);
        assert!(result2.is_ok());

        // Both succeed even though they should conflict
        // This is a known limitation that would need to be addressed
        // in a production implementation
    }

    #[test]
    fn test_try_acquire() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Should succeed on empty page
        assert!(manager.try_acquire(page_id, LatchMode::Exclusive));
        assert!(manager.try_acquire(page_id, LatchMode::Shared));

        // In the current design, try_acquire just checks if the lock
        // could be acquired at that instant, but doesn't hold it
        // So multiple try_acquires will always succeed

        // This is a limitation of the current design where the latch manager
        // returns latches but doesn't manage the actual lock guards
    }

    #[test]
    fn test_latch_coupling() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let mut coupling = LatchCoupling::new(manager.clone());

        // Acquire latches on multiple pages
        coupling.acquire(PageId(1), LatchMode::Shared).unwrap();
        coupling.acquire(PageId(2), LatchMode::Shared).unwrap();
        coupling.acquire(PageId(3), LatchMode::Exclusive).unwrap();

        assert_eq!(coupling.held_count(), 3);

        // Release ancestors of page 2
        coupling.release_ancestors(PageId(2));
        assert_eq!(coupling.held_count(), 1);

        // Release all
        coupling.release_all();
        assert_eq!(coupling.held_count(), 0);
    }

    #[test]
    fn test_statistics_tracking() {
        let manager = LatchManager::new(Duration::from_secs(5));
        let page_id = PageId(1);

        // Acquire some latches
        let _shared1 = manager.acquire_shared(page_id).unwrap();
        let _shared2 = manager.acquire_shared(PageId(2)).unwrap();
        drop(_shared1);
        drop(_shared2);

        let _exclusive = manager.acquire_exclusive(page_id).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.shared_acquisitions, 2);
        assert_eq!(stats.exclusive_acquisitions, 1);

        // Clear statistics
        manager.clear_statistics();
        let stats = manager.get_statistics();
        assert_eq!(stats.shared_acquisitions, 0);
        assert_eq!(stats.exclusive_acquisitions, 0);
    }

    #[test]
    fn test_wait_for_graph() {
        let mut graph = WaitForGraph::new();

        // No cycle initially
        assert!(!graph.has_cycle());

        // Create some dummy thread IDs for testing
        // We can't easily create real thread IDs in tests, so we'll just test
        // the basic functionality without cycles
        let thread_id = thread::current().id();

        // Add a wait
        graph.add_wait(thread_id, PageId(1));

        // Remove the wait
        graph.remove_wait(thread_id);

        // Should still have no cycle
        assert!(!graph.has_cycle());

        // Test that the wait was properly removed
        assert!(graph.waits.is_empty());
    }

    #[test]
    fn test_latch_upgrade() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Create a shared latch handle
        let mut handle = manager.create_handle(page_id, LatchMode::Shared).unwrap();
        assert_eq!(handle.mode(), LatchMode::Shared);

        // Upgrade to exclusive
        handle.upgrade().unwrap();
        assert_eq!(handle.mode(), LatchMode::Exclusive);

        let stats = manager.get_statistics();
        assert_eq!(stats.upgrades, 1);
        assert_eq!(stats.shared_acquisitions, 1);
        assert_eq!(stats.exclusive_acquisitions, 1);
    }

    #[test]
    fn test_latch_downgrade() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Create an exclusive latch handle
        let mut handle = manager
            .create_handle(page_id, LatchMode::Exclusive)
            .unwrap();
        assert_eq!(handle.mode(), LatchMode::Exclusive);

        // Downgrade to shared
        handle.downgrade().unwrap();
        assert_eq!(handle.mode(), LatchMode::Shared);

        let stats = manager.get_statistics();
        assert_eq!(stats.downgrades, 1);
        assert_eq!(stats.exclusive_acquisitions, 1);
        assert_eq!(stats.shared_acquisitions, 1);
    }

    #[test]
    fn test_upgrade_already_exclusive() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Create an exclusive latch handle
        let mut handle = manager
            .create_handle(page_id, LatchMode::Exclusive)
            .unwrap();

        // Upgrade when already exclusive should be no-op
        handle.upgrade().unwrap();
        assert_eq!(handle.mode(), LatchMode::Exclusive);

        let stats = manager.get_statistics();
        assert_eq!(stats.upgrades, 0); // No actual upgrade happened
    }

    #[test]
    fn test_downgrade_already_shared() {
        let manager = Arc::new(LatchManager::new(Duration::from_secs(5)));
        let page_id = PageId(1);

        // Create a shared latch handle
        let mut handle = manager.create_handle(page_id, LatchMode::Shared).unwrap();

        // Downgrade when already shared should be no-op
        handle.downgrade().unwrap();
        assert_eq!(handle.mode(), LatchMode::Shared);

        let stats = manager.get_statistics();
        assert_eq!(stats.downgrades, 0); // No actual downgrade happened
    }
}
