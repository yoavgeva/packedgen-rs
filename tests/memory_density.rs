//! Requested live bytes measured via `stats_alloc` over the system allocator, not RSS.
//! This excludes allocator metadata, fragmentation, and other process overhead.
//! `FixedElasticMap` is not denser than hashbrown in the median case. The previous
//! single-size 250K assertion reflected hashbrown's post-resize phase, not a general
//! saving. See docs/DENSITY_SWEEP_2026-10-11.md for the measured size sweep.

#![allow(missing_docs, clippy::cast_precision_loss)]

use std::alloc::System;
use std::fmt::Write;
use std::hash::Hash;
use std::hint::black_box;

use hashbrown::HashMap;
use packedgen::{AtomicPackedGenMap, ElasticConfig, FixedElasticMap, NonMaxU64};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// Eight rounded log-spaced points from 2^16 to 2^20, plus the old test's 250K.
const SIZES: [usize; 9] = [
    65_536, 97_386, 144_715, 215_046, 250_000, 319_557, 474_860, 705_639, 1_048_576,
];

struct Measurement {
    entries: usize,
    atomic_bytes: usize,
    hashbrown32_bytes: usize,
    elastic_bytes: usize,
    hashbrown64_bytes: usize,
}

impl Measurement {
    fn atomic_ratio(&self) -> f64 {
        self.atomic_bytes as f64 / self.hashbrown32_bytes as f64
    }

    fn elastic_ratio(&self) -> f64 {
        self.elastic_bytes as f64 / self.hashbrown64_bytes as f64
    }
}

// Allocation counters are process-wide: keep all measurements in one test and
// finish/drop each constructor before doing any formatting or reporting.
#[test]
fn requested_memory_density_across_sizes() {
    atomic_live_bytes(256);
    hashbrown_live_bytes(256, key32);
    elastic_live_bytes(256);
    hashbrown_live_bytes(256, mix);

    let rows = SIZES.map(|entries| Measurement {
        entries,
        atomic_bytes: atomic_live_bytes(entries),
        hashbrown32_bytes: hashbrown_live_bytes(entries, key32),
        elastic_bytes: elastic_live_bytes(entries),
        hashbrown64_bytes: hashbrown_live_bytes(entries, mix),
    });
    let (atomic_median, atomic_worst) =
        median_and_worst(rows.each_ref().map(Measurement::atomic_ratio));
    let (elastic_median, elastic_worst) =
        median_and_worst(rows.each_ref().map(Measurement::elastic_ratio));
    let mut table = String::from(
        "| Entries | Atomic B/entry | Hashbrown32 B/entry | Atomic/hashbrown | \
         FixedElastic B/entry | Hashbrown64 B/entry | Fixed/hashbrown |\n\
         | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for row in &rows {
        let entries = row.entries as f64;
        writeln!(
            table,
            "| {} | {:.3} | {:.3} | {:.4}x | {:.3} | {:.3} | {:.4}x |",
            row.entries,
            row.atomic_bytes as f64 / entries,
            row.hashbrown32_bytes as f64 / entries,
            row.atomic_ratio(),
            row.elastic_bytes as f64 / entries,
            row.hashbrown64_bytes as f64 / entries,
            row.elastic_ratio(),
        )
        .unwrap();
    }
    writeln!(
        table,
        "Atomic median={atomic_median:.4}x, worst={atomic_worst:.4}x; \
         FixedElastic median={elastic_median:.4}x, worst={elastic_worst:.4}x"
    )
    .unwrap();
    println!("{table}");

    assert!(
        rows.iter()
            .all(|row| row.atomic_bytes as f64 / row.entries as f64 <= 52.0),
        "AtomicPackedGenMap must use <= 52.0 requested B/entry at every size:\n{table}"
    );
    assert!(
        atomic_median <= 0.80,
        "AtomicPackedGenMap median ratio must be <= 0.80:\n{table}"
    );
    assert!(
        atomic_worst <= 1.10,
        "AtomicPackedGenMap worst ratio must be <= 1.10:\n{table}"
    );
    assert!(
        elastic_median <= 1.10,
        "FixedElasticMap median ratio must be <= 1.10:\n{table}"
    );
    assert!(
        elastic_worst <= 1.15,
        "FixedElasticMap worst ratio must be <= 1.15:\n{table}"
    );
}

fn atomic_live_bytes(entries: usize) -> usize {
    live_bytes(|| {
        AtomicPackedGenMap::try_from_entries(
            (0..entries as u64).map(|key| (key32(key), NonMaxU64::new(key).unwrap())),
            0,
        )
        .unwrap()
    })
}

fn elastic_live_bytes(entries: usize) -> usize {
    live_bytes(|| {
        let config = ElasticConfig::new(entries)
            .with_reserve_exponent(6)
            .unwrap();
        let mut map = FixedElasticMap::new(config);
        for key in 0..entries as u64 {
            map.try_insert(mix(key), key).unwrap();
        }
        map
    })
}

fn hashbrown_live_bytes<K: Eq + Hash>(entries: usize, make_key: impl Fn(u64) -> K) -> usize {
    live_bytes(|| {
        let mut map = HashMap::with_capacity(entries);
        for key in 0..entries as u64 {
            map.insert(make_key(key), key);
        }
        map
    })
}

fn live_bytes<T>(build: impl FnOnce() -> T) -> usize {
    let region = Region::new(GLOBAL);
    let map = build();
    let stats = region.change();
    let bytes = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated);
    black_box(&map);
    drop(map);
    bytes
}

fn median_and_worst<const N: usize>(mut ratios: [f64; N]) -> (f64, f64) {
    ratios.sort_unstable_by(f64::total_cmp);
    let median = if N.is_multiple_of(2) {
        f64::midpoint(ratios[N / 2 - 1], ratios[N / 2])
    } else {
        ratios[N / 2]
    };
    (median, ratios[N - 1])
}

fn key32(value: u64) -> [u8; 32] {
    let mut key = [0; 32];
    for (index, chunk) in key.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&mix(value.wrapping_add((index as u64) << 32)).to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
