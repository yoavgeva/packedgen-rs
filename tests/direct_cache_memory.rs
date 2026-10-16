//! Requested live bytes of a direct cache, measured via `stats_alloc` over the
//! system allocator (not RSS). Probation adds a FIFO of recently published keys
//! (with key copies) and a two-generation ghost filter. The filter and the
//! reserved record buffers cost about 2.4 B/entry before any insert; a full
//! FIFO's key copies bring a filled cache to about 3 to 5.5 B/entry. After
//! churn the cache is smaller with probation, because it evicts recently
//! inserted keys first.

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
    live_bytes_after_inserts(1_024, 1, true);

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
/// distinct 32-byte keys.
fn live_bytes_after_inserts(entries: usize, capacities: u64, probation: bool) -> usize {
    let region = Region::new(GLOBAL);
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
    let stats = region.change();
    let bytes = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
        .saturating_add_signed(stats.bytes_reallocated);
    black_box(&cache);
    drop(cache);
    bytes
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
