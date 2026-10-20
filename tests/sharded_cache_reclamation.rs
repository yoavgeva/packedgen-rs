//! Reclamation pacing for a lone writer: `ShardedCache` drops the values it
//! removes within a few hundred writes, not after the thousands of
//! retirements a cache's advances wait for when many threads read it.
//!
//! Keep a single test in this binary: other tests' threads would claim
//! reader shards, and pacing follows the shards in use.

#![allow(missing_docs)]

use std::sync::Arc;

use packedgen::ShardedCacheBuilder;

#[test]
fn a_lone_writers_removed_values_are_dropped_within_a_few_hundred_writes() {
    let value = Arc::new(());
    let cache = ShardedCacheBuilder::new(1_000)
        .shards(1)
        .build::<u64, Arc<()>>();
    for key in 0..100_000 {
        cache.insert(key, Arc::clone(&value));
    }
    // Every count beyond the test's own is a cached entry or a removed one
    // whose value has not been dropped yet.
    let removed_but_held = Arc::strong_count(&value) - 1 - cache.len();
    assert!(
        removed_but_held <= 256,
        "{removed_but_held} removed values are still held"
    );
}
