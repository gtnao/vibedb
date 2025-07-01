//! Version management for MVCC.
//!
//! This module provides version chain management for tuples, allowing
//! multiple versions of the same data to exist simultaneously.

use crate::access::TupleId;
use crate::concurrency::timestamp::Timestamp;
use crate::storage::PageId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Represents a version of a tuple.
#[derive(Debug, Clone)]
pub struct Version {
    /// The timestamp when this version was created.
    pub begin_timestamp: Timestamp,
    /// The timestamp when this version was deleted (None if still visible).
    pub end_timestamp: Option<Timestamp>,
    /// The transaction ID that created this version.
    pub creator_tid: u64,
    /// The transaction ID that deleted this version (None if not deleted).
    pub deleter_tid: Option<u64>,
    /// The actual tuple data.
    pub data: Vec<u8>,
    /// Pointer to the next version in the chain (older version).
    pub next: Option<Arc<RwLock<Version>>>,
}

impl Version {
    /// Creates a new version.
    pub fn new(begin_timestamp: Timestamp, creator_tid: u64, data: Vec<u8>) -> Self {
        Self {
            begin_timestamp,
            end_timestamp: None,
            creator_tid,
            deleter_tid: None,
            data,
            next: None,
        }
    }

    /// Marks this version as deleted.
    pub fn mark_deleted(&mut self, timestamp: Timestamp, tid: u64) {
        self.end_timestamp = Some(timestamp);
        self.deleter_tid = Some(tid);
    }

    /// Checks if this version is visible at the given timestamp.
    pub fn is_visible_at(&self, timestamp: Timestamp) -> bool {
        self.begin_timestamp <= timestamp && self.end_timestamp.is_none_or(|end| timestamp < end)
    }

    /// Checks if this version was created by the given transaction.
    pub fn created_by(&self, tid: u64) -> bool {
        self.creator_tid == tid
    }

    /// Checks if this version was deleted by the given transaction.
    pub fn deleted_by(&self, tid: u64) -> bool {
        self.deleter_tid == Some(tid)
    }
}

/// Key for identifying a tuple across versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VersionKey {
    pub page_id: PageId,
    pub tuple_id: TupleId,
}

impl VersionKey {
    pub fn new(page_id: PageId, tuple_id: TupleId) -> Self {
        Self { page_id, tuple_id }
    }
}

/// Manages version chains for all tuples in the system.
#[derive(Debug)]
pub struct VersionManager {
    /// Map from tuple identifier to the head of its version chain.
    /// The head is always the newest version.
    versions: Arc<RwLock<HashMap<VersionKey, Arc<RwLock<Version>>>>>,
}

impl VersionManager {
    /// Creates a new version manager.
    pub fn new() -> Self {
        Self {
            versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Inserts a new version for a tuple.
    pub fn insert_version(
        &self,
        key: VersionKey,
        timestamp: Timestamp,
        tid: u64,
        data: Vec<u8>,
    ) -> Arc<RwLock<Version>> {
        let new_version = Arc::new(RwLock::new(Version::new(timestamp, tid, data)));

        let mut versions = self.versions.write().unwrap();

        // If there's an existing version chain, link it
        if let Some(old_head) = versions.get(&key) {
            new_version.write().unwrap().next = Some(Arc::clone(old_head));
        }

        // Insert the new version as the head
        versions.insert(key, Arc::clone(&new_version));

        new_version
    }

    /// Marks a tuple as deleted by creating a deletion marker.
    pub fn delete_version(
        &self,
        key: &VersionKey,
        timestamp: Timestamp,
        tid: u64,
    ) -> Result<(), String> {
        let versions = self.versions.read().unwrap();

        if let Some(head) = versions.get(key) {
            let mut head_write = head.write().unwrap();
            if head_write.end_timestamp.is_none() {
                head_write.mark_deleted(timestamp, tid);
                Ok(())
            } else {
                Err("Tuple already deleted".to_string())
            }
        } else {
            Err("Tuple not found".to_string())
        }
    }

    /// Gets the version of a tuple visible at the given timestamp.
    pub fn get_visible_version(
        &self,
        key: &VersionKey,
        timestamp: Timestamp,
    ) -> Option<Arc<RwLock<Version>>> {
        let versions = self.versions.read().unwrap();

        if let Some(head) = versions.get(key) {
            let mut current = Some(Arc::clone(head));

            while let Some(version_arc) = current {
                let version = version_arc.read().unwrap();

                if version.is_visible_at(timestamp) {
                    drop(version);
                    return Some(version_arc);
                }

                current = version.next.as_ref().map(Arc::clone);
            }
        }

        None
    }

    /// Gets all versions of a tuple for debugging/testing.
    pub fn get_all_versions(&self, key: &VersionKey) -> Vec<Arc<RwLock<Version>>> {
        let versions = self.versions.read().unwrap();
        let mut result = Vec::new();

        if let Some(head) = versions.get(key) {
            let mut current = Some(Arc::clone(head));

            while let Some(version_arc) = current {
                result.push(Arc::clone(&version_arc));
                let version = version_arc.read().unwrap();
                current = version.next.as_ref().map(Arc::clone);
            }
        }

        result
    }

    /// Garbage collects old versions that are no longer needed.
    pub fn garbage_collect(&self, oldest_active_timestamp: Timestamp) {
        let mut versions = self.versions.write().unwrap();
        let mut keys_to_remove = Vec::new();

        for (key, head) in versions.iter() {
            let head_read = head.read().unwrap();

            // If the head version's end timestamp is before the oldest active timestamp,
            // we can remove the entire chain
            if let Some(end_ts) = head_read.end_timestamp {
                if end_ts < oldest_active_timestamp {
                    keys_to_remove.push(key.clone());
                    continue;
                }
            }

            // TODO: Implement more sophisticated GC that removes intermediate versions
            // while keeping versions that might still be needed
        }

        for key in keys_to_remove {
            versions.remove(&key);
        }
    }

    /// Gets the number of version chains currently stored.
    pub fn chain_count(&self) -> usize {
        self.versions.read().unwrap().len()
    }

    /// Gets the total number of versions across all chains.
    pub fn total_version_count(&self) -> usize {
        let versions = self.versions.read().unwrap();
        let mut count = 0;

        for head in versions.values() {
            let mut current = Some(Arc::clone(head));

            while let Some(version_arc) = current {
                count += 1;
                let version = version_arc.read().unwrap();
                current = version.next.as_ref().map(Arc::clone);
            }
        }

        count
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_visibility() {
        let mut version = Version::new(Timestamp::new(10), 1, vec![1, 2, 3]);

        // Version is visible after its begin timestamp
        assert!(version.is_visible_at(Timestamp::new(10)));
        assert!(version.is_visible_at(Timestamp::new(15)));
        assert!(!version.is_visible_at(Timestamp::new(5)));

        // Mark as deleted
        version.mark_deleted(Timestamp::new(20), 2);

        // Version is not visible after its end timestamp
        assert!(version.is_visible_at(Timestamp::new(15)));
        assert!(!version.is_visible_at(Timestamp::new(20)));
        assert!(!version.is_visible_at(Timestamp::new(25)));
    }

    #[test]
    fn test_version_manager_insert() {
        let manager = VersionManager::new();
        let key = VersionKey::new(PageId(1), TupleId::new(PageId(1), 100));

        // Insert first version
        let _v1 = manager.insert_version(key.clone(), Timestamp::new(10), 1, vec![1, 2, 3]);

        // Insert second version
        let v2 = manager.insert_version(key.clone(), Timestamp::new(20), 2, vec![4, 5, 6]);

        // Check that v2 points to v1
        {
            let v2_read = v2.read().unwrap();
            assert!(v2_read.next.is_some());
            let v1_from_chain = v2_read.next.as_ref().unwrap();
            let v1_read = v1_from_chain.read().unwrap();
            assert_eq!(v1_read.data, vec![1, 2, 3]);
        }

        // Check chain count
        assert_eq!(manager.chain_count(), 1);
        assert_eq!(manager.total_version_count(), 2);
    }

    #[test]
    fn test_version_manager_delete() {
        let manager = VersionManager::new();
        let key = VersionKey::new(PageId(1), TupleId::new(PageId(1), 100));

        // Insert version
        manager.insert_version(key.clone(), Timestamp::new(10), 1, vec![1, 2, 3]);

        // Delete version
        assert!(manager.delete_version(&key, Timestamp::new(20), 2).is_ok());

        // Try to delete again (should fail)
        assert!(manager.delete_version(&key, Timestamp::new(30), 3).is_err());
    }

    #[test]
    fn test_get_visible_version() {
        let manager = VersionManager::new();
        let key = VersionKey::new(PageId(1), TupleId::new(PageId(1), 100));

        // Insert v1 at timestamp 10
        manager.insert_version(key.clone(), Timestamp::new(10), 1, vec![1, 2, 3]);

        // Delete v1 at timestamp 20
        manager.delete_version(&key, Timestamp::new(20), 2).unwrap();

        // Insert v2 at timestamp 30
        manager.insert_version(key.clone(), Timestamp::new(30), 3, vec![4, 5, 6]);

        // Check visibility at different timestamps
        // At timestamp 15: should see v1
        let visible_at_15 = manager
            .get_visible_version(&key, Timestamp::new(15))
            .unwrap();
        assert_eq!(visible_at_15.read().unwrap().data, vec![1, 2, 3]);

        // At timestamp 25: nothing visible (v1 deleted, v2 not yet created)
        assert!(manager
            .get_visible_version(&key, Timestamp::new(25))
            .is_none());

        // At timestamp 35: should see v2
        let visible_at_35 = manager
            .get_visible_version(&key, Timestamp::new(35))
            .unwrap();
        assert_eq!(visible_at_35.read().unwrap().data, vec![4, 5, 6]);
    }

    #[test]
    fn test_get_all_versions() {
        let manager = VersionManager::new();
        let key = VersionKey::new(PageId(1), TupleId::new(PageId(1), 100));

        // Insert multiple versions
        manager.insert_version(key.clone(), Timestamp::new(10), 1, vec![1]);
        manager.insert_version(key.clone(), Timestamp::new(20), 2, vec![2]);
        manager.insert_version(key.clone(), Timestamp::new(30), 3, vec![3]);

        let all_versions = manager.get_all_versions(&key);
        assert_eq!(all_versions.len(), 3);

        // Check that versions are in reverse chronological order
        assert_eq!(all_versions[0].read().unwrap().data, vec![3]);
        assert_eq!(all_versions[1].read().unwrap().data, vec![2]);
        assert_eq!(all_versions[2].read().unwrap().data, vec![1]);
    }

    #[test]
    fn test_garbage_collect() {
        let manager = VersionManager::new();
        let key1 = VersionKey::new(PageId(1), TupleId::new(PageId(1), 100));
        let key2 = VersionKey::new(PageId(1), TupleId::new(PageId(1), 200));

        // Insert and delete v1
        manager.insert_version(key1.clone(), Timestamp::new(10), 1, vec![1]);
        manager
            .delete_version(&key1, Timestamp::new(20), 2)
            .unwrap();

        // Insert v2 (not deleted)
        manager.insert_version(key2.clone(), Timestamp::new(30), 3, vec![2]);

        assert_eq!(manager.chain_count(), 2);

        // GC with timestamp 25 (should remove key1)
        manager.garbage_collect(Timestamp::new(25));

        assert_eq!(manager.chain_count(), 1);
        assert!(manager.get_all_versions(&key1).is_empty());
        assert_eq!(manager.get_all_versions(&key2).len(), 1);
    }
}
