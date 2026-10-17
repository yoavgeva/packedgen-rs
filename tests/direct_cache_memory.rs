//! Requested live bytes of a direct cache, measured via `stats_alloc` over the
//! system allocator (not RSS). Probation adds a FIFO of recently published keys
//! (with key copies) and a two-generation ghost filter. Together they cost
//! about 3.5 to 4.5 B/entry, both when the cache is first filled and after its
//! capacity has turned over.

#![allow(missing_docs, clippy::cast_precision_loss)]

use std::alloc::System;
use std::fmt::Write;
use std::hint::black_box;

use packedgen::{CacheConfig, DirectPackedCache};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// A non-power-of-two size exercises the filter's power-of-two rounding.
const SIZES: [usize; 3] = [65_536, 200_000, 524_288];

// Allocation counters are process-wide: keep every measurement in one test.
#[test]
fn probation_adds_at_most_six_requested_bytes_per_entry() {
    // The first index rebuild starts rayon's global pool for PtrHash builds;
    // that one-time process state must not be charged to a measurement.
    drop(filled_cache(SIZES[0], 2, true));

    let mut table = String::from(
        "| Entries | Inserted | Without probation B/entry | With probation B/entry | Probation B/entry |\n\
         | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let mut overheads = Vec::new();
    for entries in SIZES {
        // Filling holds the most probation records; churn reaches steady state.
        for capacities in [1, 2] {
            let without = live_bytes_after_inserts(entries, capacities, false);
            let with = live_bytes_after_inserts(entries, capacities, true);
            let overhead = (with as f64 - without as f64) / entries as f64;
            overheads.push(overhead);
            writeln!(
                table,
                "| {entries} | {capacities}x | {:.2} | {:.2} | {overhead:.2} |",
                without as f64 / entries as f64,
                with as f64 / entries as f64,
            )
            .unwrap();
        }
    }
    println!("{table}");
    assert!(
        overheads.iter().all(|overhead| *overhead <= 6.0),
        "probation must add at most 6 requested bytes per entry:\n{table}"
    );
}

/// Live bytes held after inserting `capacities` times the capacity in
/// distinct 32-byte keys. Dropping the cache must release them again, which
/// also checks the measurement itself.
fn live_bytes_after_inserts(entries: usize, capacities: u64, probation: bool) -> usize {
    let region = Region::new(GLOBAL);
    let cache = filled_cache(entries, capacities, probation);
    let bytes = live_bytes(&region);
    black_box(&cache);
    drop(cache);
    let leftover = live_bytes(&region);
    assert!(
        leftover <= bytes / 100,
        "dropping the cache must release what it measured: {leftover} of {bytes} bytes remain"
    );
    bytes
}

fn filled_cache(entries: usize, capacities: u64, probation: bool) -> DirectPackedCache<u64> {
    let config = CacheConfig::new(u64::MAX).with_max_entries(entries);
    let cache = DirectPackedCache::try_new(if probation {
        config
    } else {
        config.with_probation(0)
    })
    .unwrap();
    for id in 0..capacities * entries as u64 {
        cache
            .insert_discard_with_options(&key32(id), id, 40, None)
            .unwrap();
        if id.is_multiple_of(4_096) {
            cache.maintain().unwrap();
        }
    }
    cache.maintain().unwrap();
    cache
}

// `stats_alloc` already adds each reallocation's size change to
// `bytes_allocated` or `bytes_deallocated`; adding `bytes_reallocated` again
// would count it twice.
fn live_bytes(region: &Region<'_, System>) -> usize {
    let stats = region.change();
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn key32(value: u64) -> [u8; 32] {
    let mut key = [0; 32];
    for (index, chunk) in key.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&mix(value.wrapping_add((index as u64) << 32)).to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
