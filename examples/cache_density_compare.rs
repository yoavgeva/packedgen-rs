//! Requested live bytes per entry of a full `DirectPackedCache` and a full
//! `quick_cache` cache holding identical keys and values.
//!
//! Usage: `cache_density_compare [ENTRIES...]` (default 100000 1000000).
//! Each cache is filled with twice its entry limit in distinct keys, so both
//! have evicted and hold their steady-state metadata. Bytes are requested live
//! allocation (`stats_alloc` over the system allocator), not RSS; allocator
//! metadata and fragmentation are excluded for both caches alike. "Overhead"
//! subtracts the key and value bytes every cache must store.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::alloc::System;
use std::hint::black_box;

use packedgen::{CacheConfig, DirectPackedCache, ShardedCache};
use quick_cache::{UnitWeighter, sync::Cache};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn live(region: &Region<'_, System>) -> isize {
    let change = region.change();
    change.bytes_allocated as isize - change.bytes_deallocated as isize
}

fn key<const K: usize>(id: u64) -> [u8; K] {
    let mut key = [0_u8; K];
    let mixed = id.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes();
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = mixed[index % 8] ^ (index as u8);
    }
    key[..8].copy_from_slice(&id.to_le_bytes());
    key
}

fn direct<const K: usize, V: Send + Sync>(entries: usize, value: impl Fn(u64) -> V) -> f64 {
    let region = Region::new(GLOBAL);
    let cache = DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_max_entries(entries))
        .expect("cache constructs");
    for id in 0..(2 * entries) as u64 {
        cache
            .insert_discard_with_options(&key::<K>(id), value(id), 1, None)
            .expect("insert");
    }
    cache.maintain().expect("maintenance");
    assert!(cache.len() <= entries);
    let bytes = live(&region);
    let resident = cache.len();
    drop(black_box(cache));
    bytes as f64 / resident as f64
}

fn sharded<const K: usize, V: Send + Sync>(entries: usize, value: impl Fn(u64) -> V) -> f64 {
    let region = Region::new(GLOBAL);
    let cache = ShardedCache::new(entries);
    for id in 0..(2 * entries) as u64 {
        cache.insert(key::<K>(id), value(id));
    }
    let bytes = live(&region);
    let resident = cache.len();
    drop(black_box(cache));
    bytes as f64 / resident as f64
}

fn quick<const K: usize, V: Clone + Send + Sync>(entries: usize, value: impl Fn(u64) -> V) -> f64 {
    let region = Region::new(GLOBAL);
    let cache: Cache<[u8; K], V, UnitWeighter> = Cache::new(entries);
    for id in 0..(2 * entries) as u64 {
        cache.insert(key::<K>(id), value(id));
    }
    let bytes = live(&region);
    let resident = cache.len();
    drop(black_box(cache));
    bytes as f64 / resident as f64
}

fn row<const K: usize, V: Clone + Send + Sync>(
    entries: usize,
    label: &str,
    value: impl Fn(u64) -> V + Copy,
) {
    let direct = direct::<K, V>(entries, value);
    let sharded = sharded::<K, V>(entries, value);
    let quick = quick::<K, V>(entries, value);
    let payload = (K + size_of::<V>()) as f64;
    println!(
        "| {entries} | {K} | {label} | {direct:.1} | {sharded:.1} | {quick:.1} | {:.1} | {:.1} | {:.1} |",
        direct - payload,
        sharded - payload,
        quick - payload,
    );
}

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|value| value.parse().expect("entry count"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![100_000, 1_000_000]
    } else {
        sizes
    };
    // Start rayon's global pool (used by index rebuilds) outside the regions.
    let _ = direct::<8, u64>(4_096, |id| id);
    println!(
        "| Entries | Key B | Value | Direct B/entry | Sharded B/entry | quick_cache B/entry | Direct overhead | Sharded overhead | quick_cache overhead |"
    );
    println!("| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    for entries in sizes {
        row::<8, u64>(entries, "u64", |id| id);
        row::<16, u64>(entries, "u64", |id| id);
        row::<32, u64>(entries, "u64", |id| id);
        row::<16, [u8; 64]>(entries, "[u8; 64]", |id| [id as u8; 64]);
        row::<32, [u8; 64]>(entries, "[u8; 64]", |id| [id as u8; 64]);
    }
}
