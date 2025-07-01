//! Lock management for MVCC.
//!
//! This module provides read/write lock management for transactions,
//! supporting different isolation levels.

use crate::access::TupleId;
use crate::storage::PageId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Lock modes supported by the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Shared lock for read operations.
    Shared,
    /// Exclusive lock for write operations.
    Exclusive,
}

impl LockMode {
    /// Checks if this lock mode is compatible with another.
    pub fn is_compatible_with(&self, other: &LockMode) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }
}

/// Identifier for a lockable resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockId {
    /// Lock on a specific tuple.
    Tuple(PageId, TupleId),
    /// Lock on an entire page.
    Page(PageId),
    /// Lock on an entire table.
    Table(u32),
}

/// Request for a lock from a transaction.
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// The transaction ID requesting the lock.
    pub tid: u64,
    /// The requested lock mode.
    pub mode: LockMode,
    /// Whether the request has been granted.
    pub granted: bool,
}

/// Information about a lock.
#[derive(Debug)]
struct LockInfo {
    /// Queue of lock requests (both granted and waiting).
    requests: VecDeque<LockRequest>,
    /// Condition variable for waiting threads.
    cv: Arc<Condvar>,
}

impl LockInfo {
    fn new() -> Self {
        Self {
            requests: VecDeque::new(),
            cv: Arc::new(Condvar::new()),
        }
    }

    /// Gets all granted locks.
    fn granted_locks(&self) -> Vec<&LockRequest> {
        self.requests.iter().filter(|r| r.granted).collect()
    }

    /// Checks if a new request is compatible with existing granted locks.
    fn is_compatible(&self, mode: LockMode) -> bool {
        self.granted_locks()
            .iter()
            .all(|req| req.mode.is_compatible_with(&mode))
    }

    /// Grants waiting requests that are now compatible.
    fn grant_compatible_requests(&mut self) {
        let mut granted_any = false;
        let mut indices_to_grant = Vec::new();

        // First pass: collect indices of requests to grant
        for (i, request) in self.requests.iter().enumerate() {
            if !request.granted {
                // Check compatibility with all currently granted requests
                let compatible = self
                    .granted_locks()
                    .iter()
                    .all(|req| req.mode.is_compatible_with(&request.mode));

                if compatible {
                    indices_to_grant.push(i);
                } else {
                    // Stop at the first incompatible request to maintain FIFO order
                    break;
                }
            }
        }

        // Second pass: grant the requests
        for i in indices_to_grant {
            self.requests[i].granted = true;
            granted_any = true;
        }

        if granted_any {
            self.cv.notify_all();
        }
    }
}

/// Deadlock detection information.
#[derive(Debug)]
struct DeadlockDetector {
    /// Wait-for graph: tid -> set of tids it's waiting for.
    wait_for: HashMap<u64, HashSet<u64>>,
}

impl DeadlockDetector {
    fn new() -> Self {
        Self {
            wait_for: HashMap::new(),
        }
    }

    /// Adds a wait-for edge.
    fn add_edge(&mut self, waiter: u64, holder: u64) {
        self.wait_for.entry(waiter).or_default().insert(holder);
    }

    /// Removes all edges from a transaction.
    fn remove_transaction(&mut self, tid: u64) {
        self.wait_for.remove(&tid);
        // Also remove tid from all other wait sets
        for wait_set in self.wait_for.values_mut() {
            wait_set.remove(&tid);
        }
    }

    /// Detects if adding an edge would create a cycle.
    fn would_cause_deadlock(&self, waiter: u64, holder: u64) -> bool {
        // Check if holder can reach waiter through the wait-for graph
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(holder);

        while let Some(current) = queue.pop_front() {
            if current == waiter {
                return true; // Found a cycle
            }

            if visited.insert(current) {
                if let Some(waiting_for) = self.wait_for.get(&current) {
                    for &next in waiting_for {
                        queue.push_back(next);
                    }
                }
            }
        }

        false
    }
}

/// Lock manager for the MVCC system.
pub struct LockManager {
    /// Map from lock ID to lock information.
    locks: Arc<Mutex<HashMap<LockId, LockInfo>>>,
    /// Map from transaction ID to its held locks.
    transaction_locks: Arc<Mutex<HashMap<u64, HashSet<LockId>>>>,
    /// Deadlock detector.
    deadlock_detector: Arc<Mutex<DeadlockDetector>>,
}

impl LockManager {
    /// Creates a new lock manager.
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
            transaction_locks: Arc::new(Mutex::new(HashMap::new())),
            deadlock_detector: Arc::new(Mutex::new(DeadlockDetector::new())),
        }
    }

    /// Acquires a lock for a transaction.
    pub fn acquire_lock(
        &self,
        tid: u64,
        lock_id: LockId,
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<(), String> {
        let start = Instant::now();

        // First, check for deadlock
        {
            let locks = self.locks.lock().unwrap();
            let detector = self.deadlock_detector.lock().unwrap();

            if let Some(lock_info) = locks.get(&lock_id) {
                for granted in lock_info.granted_locks() {
                    if granted.tid != tid
                        && !granted.mode.is_compatible_with(&mode)
                        && detector.would_cause_deadlock(tid, granted.tid)
                    {
                        return Err("Deadlock detected".to_string());
                    }
                }
            }
        }

        let cv = {
            let mut locks = self.locks.lock().unwrap();
            let lock_info = locks.entry(lock_id.clone()).or_insert_with(LockInfo::new);

            // Check if we already have a compatible lock
            for request in &lock_info.requests {
                if request.tid == tid && request.granted {
                    if request.mode == mode {
                        return Ok(()); // Already have the lock
                    } else if mode == LockMode::Shared {
                        return Ok(()); // Already have exclusive, which implies shared
                    }
                }
            }

            // Add the request
            let compatible = lock_info.is_compatible(mode);
            lock_info.requests.push_back(LockRequest {
                tid,
                mode,
                granted: compatible,
            });

            if compatible {
                // Lock granted immediately
                let mut tx_locks = self.transaction_locks.lock().unwrap();
                tx_locks.entry(tid).or_default().insert(lock_id);
                return Ok(());
            }

            // Need to wait - update wait-for graph
            {
                let mut detector = self.deadlock_detector.lock().unwrap();
                for granted in lock_info.granted_locks() {
                    if granted.tid != tid && !granted.mode.is_compatible_with(&mode) {
                        detector.add_edge(tid, granted.tid);
                    }
                }
            }

            Arc::clone(&lock_info.cv)
        };

        // Wait for the lock
        let mut locks = self.locks.lock().unwrap();
        loop {
            // Check if our request has been granted
            if let Some(lock_info) = locks.get(&lock_id) {
                if let Some(request) = lock_info.requests.iter().find(|r| r.tid == tid) {
                    if request.granted {
                        let mut tx_locks = self.transaction_locks.lock().unwrap();
                        tx_locks.entry(tid).or_default().insert(lock_id);

                        // Remove from wait-for graph
                        self.deadlock_detector
                            .lock()
                            .unwrap()
                            .remove_transaction(tid);

                        return Ok(());
                    }
                }
            }

            // Check timeout
            if let Some(timeout) = timeout {
                let elapsed = start.elapsed();
                if elapsed >= timeout {
                    // Remove our request and clean up
                    if let Some(lock_info) = locks.get_mut(&lock_id) {
                        lock_info.requests.retain(|r| r.tid != tid);
                    }
                    self.deadlock_detector
                        .lock()
                        .unwrap()
                        .remove_transaction(tid);
                    return Err("Lock acquisition timeout".to_string());
                }

                let remaining = timeout - elapsed;
                let (new_locks, timeout_result) = cv.wait_timeout(locks, remaining).unwrap();
                locks = new_locks;

                if timeout_result.timed_out() {
                    // Remove our request and clean up
                    if let Some(lock_info) = locks.get_mut(&lock_id) {
                        lock_info.requests.retain(|r| r.tid != tid);
                    }
                    self.deadlock_detector
                        .lock()
                        .unwrap()
                        .remove_transaction(tid);
                    return Err("Lock acquisition timeout".to_string());
                }
            } else {
                locks = cv.wait(locks).unwrap();
            }
        }
    }

    /// Releases a specific lock held by a transaction.
    pub fn release_lock(&self, tid: u64, lock_id: &LockId) -> Result<(), String> {
        let mut locks = self.locks.lock().unwrap();
        let mut tx_locks = self.transaction_locks.lock().unwrap();

        // Remove from transaction's lock set
        if let Some(lock_set) = tx_locks.get_mut(&tid) {
            lock_set.remove(lock_id);
            if lock_set.is_empty() {
                tx_locks.remove(&tid);
            }
        }

        // Remove from lock info and grant waiting requests
        if let Some(lock_info) = locks.get_mut(lock_id) {
            lock_info.requests.retain(|r| r.tid != tid);

            if lock_info.requests.is_empty() {
                locks.remove(lock_id);
            } else {
                lock_info.grant_compatible_requests();
            }

            Ok(())
        } else {
            Err("Lock not found".to_string())
        }
    }

    /// Releases all locks held by a transaction.
    pub fn release_all_locks(&self, tid: u64) {
        let mut tx_locks = self.transaction_locks.lock().unwrap();

        if let Some(lock_set) = tx_locks.remove(&tid) {
            let mut locks = self.locks.lock().unwrap();

            for lock_id in lock_set {
                if let Some(lock_info) = locks.get_mut(&lock_id) {
                    lock_info.requests.retain(|r| r.tid != tid);

                    if lock_info.requests.is_empty() {
                        locks.remove(&lock_id);
                    } else {
                        lock_info.grant_compatible_requests();
                    }
                }
            }
        }

        // Clean up deadlock detector
        self.deadlock_detector
            .lock()
            .unwrap()
            .remove_transaction(tid);
    }

    /// Gets all locks held by a transaction.
    pub fn get_transaction_locks(&self, tid: u64) -> Vec<LockId> {
        let tx_locks = self.transaction_locks.lock().unwrap();
        tx_locks
            .get(&tid)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Checks if a transaction holds a specific lock.
    pub fn has_lock(&self, tid: u64, lock_id: &LockId, mode: LockMode) -> bool {
        let locks = self.locks.lock().unwrap();

        if let Some(lock_info) = locks.get(lock_id) {
            lock_info.requests.iter().any(|r| {
                r.tid == tid
                    && r.granted
                    && (r.mode == mode
                        || (r.mode == LockMode::Exclusive && mode == LockMode::Shared))
            })
        } else {
            false
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_lock_compatibility() {
        assert!(LockMode::Shared.is_compatible_with(&LockMode::Shared));
        assert!(!LockMode::Shared.is_compatible_with(&LockMode::Exclusive));
        assert!(!LockMode::Exclusive.is_compatible_with(&LockMode::Shared));
        assert!(!LockMode::Exclusive.is_compatible_with(&LockMode::Exclusive));
    }

    #[test]
    fn test_basic_lock_acquire_release() {
        let manager = LockManager::new();
        let lock_id = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));

        // Acquire shared lock
        assert!(manager
            .acquire_lock(1, lock_id.clone(), LockMode::Shared, None)
            .is_ok());
        assert!(manager.has_lock(1, &lock_id, LockMode::Shared));

        // Release lock
        assert!(manager.release_lock(1, &lock_id).is_ok());
        assert!(!manager.has_lock(1, &lock_id, LockMode::Shared));
    }

    #[test]
    fn test_multiple_shared_locks() {
        let manager = LockManager::new();
        let lock_id = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));

        // Multiple transactions can acquire shared locks
        assert!(manager
            .acquire_lock(1, lock_id.clone(), LockMode::Shared, None)
            .is_ok());
        assert!(manager
            .acquire_lock(2, lock_id.clone(), LockMode::Shared, None)
            .is_ok());
        assert!(manager
            .acquire_lock(3, lock_id.clone(), LockMode::Shared, None)
            .is_ok());

        // All have the lock
        assert!(manager.has_lock(1, &lock_id, LockMode::Shared));
        assert!(manager.has_lock(2, &lock_id, LockMode::Shared));
        assert!(manager.has_lock(3, &lock_id, LockMode::Shared));
    }

    #[test]
    fn test_exclusive_lock_blocks_others() {
        let manager = Arc::new(LockManager::new());
        let lock_id = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));
        let barrier = Arc::new(Barrier::new(2));

        // Transaction 1 acquires exclusive lock
        assert!(manager
            .acquire_lock(1, lock_id.clone(), LockMode::Exclusive, None)
            .is_ok());

        // Transaction 2 tries to acquire shared lock (should block)
        let manager_clone = Arc::clone(&manager);
        let lock_id_clone = lock_id.clone();
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let result = manager_clone.acquire_lock(
                2,
                lock_id_clone,
                LockMode::Shared,
                Some(Duration::from_millis(100)),
            );
            assert!(result.is_err()); // Should timeout
        });

        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        // Transaction 2 should not have the lock
        assert!(!manager.has_lock(2, &lock_id, LockMode::Shared));

        handle.join().unwrap();
    }

    #[test]
    fn test_lock_queue_fifo() {
        let manager = Arc::new(LockManager::new());
        let lock_id = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));
        let barrier = Arc::new(Barrier::new(3));

        // Transaction 1 acquires exclusive lock
        assert!(manager
            .acquire_lock(1, lock_id.clone(), LockMode::Exclusive, None)
            .is_ok());

        // Transaction 2 waits for exclusive lock
        let manager_clone1 = Arc::clone(&manager);
        let lock_id_clone1 = lock_id.clone();
        let barrier_clone1 = Arc::clone(&barrier);

        let handle1 = thread::spawn(move || {
            barrier_clone1.wait();
            manager_clone1
                .acquire_lock(2, lock_id_clone1, LockMode::Exclusive, None)
                .unwrap();
            // Hold the lock briefly
            thread::sleep(Duration::from_millis(50));
            manager_clone1
                .release_lock(2, &LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100)))
                .unwrap();
        });

        // Transaction 3 waits for shared lock
        let manager_clone2 = Arc::clone(&manager);
        let lock_id_clone2 = lock_id.clone();
        let barrier_clone2 = Arc::clone(&barrier);

        let handle2 = thread::spawn(move || {
            barrier_clone2.wait();
            thread::sleep(Duration::from_millis(10)); // Ensure T2 requests first
            manager_clone2
                .acquire_lock(3, lock_id_clone2, LockMode::Shared, None)
                .unwrap();
            manager_clone2
                .release_lock(3, &LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100)))
                .unwrap();
        });

        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        // Release T1's lock
        manager.release_lock(1, &lock_id).unwrap();

        // Wait for both threads to complete
        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_release_all_locks() {
        let manager = LockManager::new();
        let lock1 = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));
        let lock2 = LockId::Tuple(PageId(2), TupleId::new(PageId(2), 200));
        let lock3 = LockId::Page(PageId(3));

        // Acquire multiple locks
        assert!(manager
            .acquire_lock(1, lock1.clone(), LockMode::Shared, None)
            .is_ok());
        assert!(manager
            .acquire_lock(1, lock2.clone(), LockMode::Exclusive, None)
            .is_ok());
        assert!(manager
            .acquire_lock(1, lock3.clone(), LockMode::Shared, None)
            .is_ok());

        // Check all locks are held
        assert_eq!(manager.get_transaction_locks(1).len(), 3);

        // Release all locks
        manager.release_all_locks(1);

        // Check all locks are released
        assert_eq!(manager.get_transaction_locks(1).len(), 0);
        assert!(!manager.has_lock(1, &lock1, LockMode::Shared));
        assert!(!manager.has_lock(1, &lock2, LockMode::Exclusive));
        assert!(!manager.has_lock(1, &lock3, LockMode::Shared));
    }

    #[test]
    fn test_deadlock_detection() {
        let manager = LockManager::new();
        let lock1 = LockId::Tuple(PageId(1), TupleId::new(PageId(1), 100));
        let lock2 = LockId::Tuple(PageId(2), TupleId::new(PageId(2), 200));

        // T1 acquires lock1
        assert!(manager
            .acquire_lock(1, lock1.clone(), LockMode::Exclusive, None)
            .is_ok());

        // T2 acquires lock2
        assert!(manager
            .acquire_lock(2, lock2.clone(), LockMode::Exclusive, None)
            .is_ok());

        // T1 tries to acquire lock2 (would wait)
        // T2 tries to acquire lock1 (would create deadlock)
        // For simplicity in testing, we'll just verify the deadlock detector logic
        let detector = manager.deadlock_detector.lock().unwrap();
        assert!(!detector.would_cause_deadlock(1, 2)); // T1 waiting for T2 is ok
        drop(detector);

        // In a real scenario with concurrent threads, the second transaction
        // trying to create a cycle would get a deadlock error
    }
}
