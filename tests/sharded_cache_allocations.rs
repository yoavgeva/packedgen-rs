//! Allocation contract for `ShardedCache` churn: a warm, full cache reuses
//! the storage of the entries it evicts, so inserts allocate almost nothing.
//!
//! Keep a single test in this binary: `stats_alloc` measures process-wide
//! allocations, so parallel tests would contaminate the measured region.

#![allow(missing_docs)]

use std::alloc::System;
use std::hint::black_box;

use packedgen::{ShardedCache, ShardedCacheBuilder};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const INSERTS: u64 = 100_000;

#[test]
fn warm_churn_reuses_entry_storage() {
    let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(4_096).shards(4).build();
    // Reach the steady state: full shards, queued retirements, and reused
    // storage. Reads promote some keys so eviction takes every path.
    for key in 0..200_000_u64 {
        cache.insert(key, key);
        if key % 3 == 0 {
            black_box(cache.get(&(key / 2)));
        }
    }
    let region = Region::new(GLOBAL);
    for key in 1_000_000..1_000_000 + INSERTS {
        cache.insert(key, key);
        if key % 3 == 0 {
            black_box(cache.get(&(key - 500)));
        }
    }
    let allocations = region.change().allocations;
    assert!(
        allocations as u64 * 100 <= INSERTS,
        "{allocations} allocations for {INSERTS} inserts into a warm cache"
    );
}
