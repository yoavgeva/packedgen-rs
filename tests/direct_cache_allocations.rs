//! Allocation contracts for resident reads, separate from throughput probes.
//!
//! Keep a single test in this binary: `stats_alloc` measures process-wide
//! allocations, so parallel tests would contaminate the measured regions.

#![allow(missing_docs)]

use std::alloc::System;
use std::hint::black_box;
use std::sync::Arc;

use packedgen::{CacheConfig, DirectPackedCache};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const READS: usize = 4_096;

#[test]
fn resident_borrowed_and_arc_reads_do_not_allocate() {
    for key_len in [32, 73] {
        for payload_len in [64, 65_536] {
            let key = vec![1_u8; key_len];
            let missing = vec![2_u8; key_len];
            let cache = DirectPackedCache::try_new(
                CacheConfig::new(u64::MAX)
                    .with_max_entries(16)
                    .with_overlay_capacity(16),
            )
            .unwrap();
            cache
                .insert_discard_with_options(
                    &key,
                    vec![7_u8; payload_len].into_boxed_slice(),
                    payload_len as u64,
                    None,
                )
                .unwrap();

            let guard = cache.pin();
            // Initialize thread-local/snapshot state outside the measurement.
            assert_eq!(guard.get(&key).unwrap().len(), payload_len);
            assert!(guard.get(&missing).is_none());
            for tracked in [true, false] {
                let region = Region::new(GLOBAL);
                for _ in 0..READS {
                    let value = if tracked {
                        guard.get(black_box(&key))
                    } else {
                        guard.get_untracked(black_box(&key))
                    };
                    assert_eq!(black_box(value.unwrap())[0], 7);
                    let absent = if tracked {
                        guard.get(black_box(&missing))
                    } else {
                        guard.get_untracked(black_box(&missing))
                    };
                    assert!(black_box(absent).is_none());
                }
                assert_no_allocations(region.change());
            }
            drop(guard);

            // Positive control: deep owned copies must be visible to this
            // allocator. This deliberately is not the borrowed-read contract.
            drop(cache.get_cloned(&key).unwrap());
            let region = Region::new(GLOBAL);
            for _ in 0..16 {
                drop(black_box(cache.get_cloned(black_box(&key)).unwrap()));
            }
            let copied = region.change();
            assert!(copied.allocations >= 16);
            assert!(copied.bytes_allocated >= 16 * payload_len);

            let shared = DirectPackedCache::try_new(
                CacheConfig::new(u64::MAX)
                    .with_max_entries(16)
                    .with_overlay_capacity(16),
            )
            .unwrap();
            shared
                .insert_discard_with_options(
                    &key,
                    Arc::<[u8]>::from(vec![7_u8; payload_len]),
                    payload_len as u64,
                    None,
                )
                .unwrap();
            let cloned = shared.pin_cloned();
            drop(cloned.get(&key).unwrap());
            assert!(cloned.get(&missing).is_none());
            let region = Region::new(GLOBAL);
            for _ in 0..READS {
                assert_eq!(black_box(cloned.get(black_box(&key)).unwrap())[0], 7);
                assert!(black_box(cloned.get(black_box(&missing))).is_none());
            }
            assert_no_allocations(region.change());
        }
    }
}

fn assert_no_allocations(stats: Stats) {
    assert_eq!(stats.allocations, 0, "{stats:?}");
    assert_eq!(stats.reallocations, 0, "{stats:?}");
    assert_eq!(stats.bytes_allocated, 0, "{stats:?}");
}
