//! Timestamp ordering module for MVCC.
//!
//! This module provides timestamp generation and ordering functionality
//! for multi-version concurrency control.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Timestamp type used throughout the MVCC system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Creates a new timestamp with the given value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value of the timestamp.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Returns the minimum possible timestamp.
    pub fn min() -> Self {
        Self(0)
    }

    /// Returns the maximum possible timestamp.
    pub fn max() -> Self {
        Self(u64::MAX)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timestamp({})", self.0)
    }
}

/// Oracle for generating monotonically increasing timestamps.
#[derive(Debug)]
pub struct TimestampOracle {
    /// The last issued timestamp.
    last_timestamp: Arc<AtomicU64>,
}

impl TimestampOracle {
    /// Creates a new timestamp oracle.
    pub fn new() -> Self {
        // Initialize with current time in microseconds since epoch
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Self {
            last_timestamp: Arc::new(AtomicU64::new(now)),
        }
    }

    /// Creates a new timestamp oracle with a specific starting value.
    pub fn with_start(start: u64) -> Self {
        Self {
            last_timestamp: Arc::new(AtomicU64::new(start)),
        }
    }

    /// Generates a new unique timestamp.
    ///
    /// This method guarantees that each timestamp is strictly greater than
    /// all previously generated timestamps.
    pub fn generate(&self) -> Timestamp {
        // Use fetch_add to atomically increment and get the old value
        // Add 1 to ensure the returned timestamp is unique
        let ts = self.last_timestamp.fetch_add(1, Ordering::SeqCst) + 1;
        Timestamp::new(ts)
    }

    /// Gets the current timestamp without incrementing.
    pub fn current(&self) -> Timestamp {
        let ts = self.last_timestamp.load(Ordering::SeqCst);
        Timestamp::new(ts)
    }

    /// Updates the oracle's timestamp to at least the given value.
    ///
    /// This is useful for synchronizing with external timestamp sources.
    pub fn update(&self, ts: Timestamp) {
        // Use compare_exchange in a loop to ensure we only update if the new value is larger
        let mut current = self.last_timestamp.load(Ordering::SeqCst);
        while ts.value() > current {
            match self.last_timestamp.compare_exchange(
                current,
                ts.value(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TimestampOracle {
    fn clone(&self) -> Self {
        Self {
            last_timestamp: Arc::clone(&self.last_timestamp),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp::new(100);
        let ts2 = Timestamp::new(200);
        let ts3 = Timestamp::new(200);

        assert!(ts1 < ts2);
        assert!(ts2 == ts3);
        assert!(ts1 != ts2);
    }

    #[test]
    fn test_timestamp_min_max() {
        let min = Timestamp::min();
        let max = Timestamp::max();
        let regular = Timestamp::new(1000);

        assert!(min < regular);
        assert!(regular < max);
        assert_eq!(min.value(), 0);
        assert_eq!(max.value(), u64::MAX);
    }

    #[test]
    fn test_oracle_monotonic() {
        let oracle = TimestampOracle::with_start(1000);

        let ts1 = oracle.generate();
        let ts2 = oracle.generate();
        let ts3 = oracle.generate();

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
        assert_eq!(ts1.value(), 1001);
        assert_eq!(ts2.value(), 1002);
        assert_eq!(ts3.value(), 1003);
    }

    #[test]
    fn test_oracle_current() {
        let oracle = TimestampOracle::with_start(5000);

        let current1 = oracle.current();
        assert_eq!(current1.value(), 5000);

        let ts = oracle.generate();
        assert_eq!(ts.value(), 5001);

        let current2 = oracle.current();
        assert_eq!(current2.value(), 5001);
    }

    #[test]
    fn test_oracle_update() {
        let oracle = TimestampOracle::with_start(1000);

        // Update to a larger value
        oracle.update(Timestamp::new(2000));
        let ts1 = oracle.generate();
        assert_eq!(ts1.value(), 2001);

        // Update to a smaller value (should be ignored)
        oracle.update(Timestamp::new(1500));
        let ts2 = oracle.generate();
        assert_eq!(ts2.value(), 2002);
    }

    #[test]
    fn test_oracle_concurrent() {
        let oracle = TimestampOracle::with_start(0);
        let num_threads = 10;
        let timestamps_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let oracle_clone = oracle.clone();
                let barrier_clone = Arc::clone(&barrier);

                thread::spawn(move || {
                    let mut timestamps = Vec::new();

                    // Wait for all threads to be ready
                    barrier_clone.wait();

                    // Generate timestamps
                    for _ in 0..timestamps_per_thread {
                        timestamps.push(oracle_clone.generate());
                    }

                    timestamps
                })
            })
            .collect();

        // Collect all timestamps
        let mut all_timestamps = Vec::new();
        for handle in handles {
            all_timestamps.extend(handle.join().unwrap());
        }

        // Check that all timestamps are unique
        let unique_timestamps: HashSet<_> = all_timestamps.iter().collect();
        assert_eq!(unique_timestamps.len(), all_timestamps.len());

        // Check that we have the expected number of timestamps
        assert_eq!(all_timestamps.len(), num_threads * timestamps_per_thread);
    }

    #[test]
    fn test_oracle_clone() {
        let oracle1 = TimestampOracle::with_start(1000);
        let oracle2 = oracle1.clone();

        let ts1 = oracle1.generate();
        let ts2 = oracle2.generate();

        assert!(ts1 != ts2);
        assert!(ts1.value() == 1001 || ts1.value() == 1002);
        assert!(ts2.value() == 1001 || ts2.value() == 1002);
    }
}
