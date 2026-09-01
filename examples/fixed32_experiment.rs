//! One-shot RAM and latency probe for the fixed-32-byte-key `SoA` experiment.
//!
//! Run with, for example:
//! `cargo run --release --example fixed32_experiment -- 1000000`.

#![allow(clippy::cast_precision_loss)]

use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use packedgen::{Fixed32Load, Fixed32SoaMap, PackedSwissMap, SegmentedLoad, SegmentedSwissMap};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const LOOKUP_ROUNDS: usize = 8;
const REMOVE_STRIDE: usize = 2;

fn main() {
    let entries = std::env::args().nth(1).map_or(1_000_000_usize, |argument| {
        argument.parse().expect("entry count must be an integer")
    });
    assert!(entries > 0, "entry count must not be zero");
    assert!(u32::try_from(entries).is_ok(), "entry count must fit u32");

    let keys: Vec<_> = (0..entries as u64).map(key).collect();
    let misses: Vec<_> = (entries as u64..(entries * 2) as u64).map(key).collect();
    let queries: Vec<_> = (0..entries)
        .map(|index| usize::try_from(mix(index as u64) % entries as u64).unwrap())
        .collect();

    let measurements = [
        measure_fixed(&keys, &misses, &queries, Fixed32Load::Compact),
        measure_fixed(&keys, &misses, &queries, Fixed32Load::Balanced),
        measure_segmented(&keys, &misses, &queries, SegmentedLoad::Compact),
        measure_segmented(&keys, &misses, &queries, SegmentedLoad::Balanced),
        measure_packed(&keys, &misses, &queries),
    ];

    println!("entries={entries}, key=32 bytes, value=u64, lookup_rounds={LOOKUP_ROUNDS}");
    println!(
        "implementation                 B/entry   build ns/op   hit ns/op  miss ns/op remove ns/op"
    );
    for measurement in measurements {
        println!(
            "{:<29} {:>8.2} {:>13.2} {:>11.2} {:>11.2} {:>12.2}",
            measurement.name,
            measurement.bytes as f64 / entries as f64,
            ns_per(measurement.build, entries),
            ns_per(measurement.hits, entries * LOOKUP_ROUNDS),
            ns_per(measurement.misses, entries * LOOKUP_ROUNDS),
            ns_per(measurement.removals, entries.div_ceil(REMOVE_STRIDE)),
        );
    }
}

fn measure_fixed(
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    queries: &[usize],
    load: Fixed32Load,
) -> Measurement {
    let region = Region::new(GLOBAL);
    let started = Instant::now();
    let mut map = Fixed32SoaMap::with_capacity_and_load(keys.len(), load);
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(*key, index as u64).unwrap();
    }
    let build = started.elapsed();
    let bytes = net_live_bytes(region.change());
    let hits = time_hits(keys, queries, |key| map.get(key).copied());
    let misses = time_misses(misses, queries, |key| map.get(key).copied());
    let removals = time_removals(keys, |key| map.remove(key));
    black_box(&map);
    Measurement {
        name: match load {
            Fixed32Load::Compact => "Fixed32SoA compact",
            Fixed32Load::Balanced => "Fixed32SoA balanced",
            Fixed32Load::Fast => "Fixed32SoA fast",
        },
        bytes,
        build,
        hits,
        misses,
        removals,
    }
}

fn measure_segmented(
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    queries: &[usize],
    load: SegmentedLoad,
) -> Measurement {
    let region = Region::new(GLOBAL);
    let started = Instant::now();
    let mut map =
        SegmentedSwissMap::try_with_capacity_key_bytes_and_load(keys.len(), keys.len() * 32, load)
            .unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let build = started.elapsed();
    let bytes = net_live_bytes(region.change());
    let hits = time_hits(keys, queries, |key| map.get(key).copied());
    let misses = time_misses(misses, queries, |key| map.get(key).copied());
    let removals = time_removals(keys, |key| map.remove(key));
    black_box(&map);
    Measurement {
        name: match load {
            SegmentedLoad::Compact => "SegmentedSwiss compact",
            SegmentedLoad::Balanced => "SegmentedSwiss balanced",
            SegmentedLoad::Fast => "SegmentedSwiss fast",
        },
        bytes,
        build,
        hits,
        misses,
        removals,
    }
}

fn measure_packed(keys: &[[u8; 32]], misses: &[[u8; 32]], queries: &[usize]) -> Measurement {
    let region = Region::new(GLOBAL);
    let started = Instant::now();
    let mut map =
        PackedSwissMap::try_with_capacity_and_key_bytes(keys.len(), keys.len() * 32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let build = started.elapsed();
    let bytes = net_live_bytes(region.change());
    let hits = time_hits(keys, queries, |key| map.get(key).copied());
    let misses = time_misses(misses, queries, |key| map.get(key).copied());
    let removals = time_removals(keys, |key| map.remove(key));
    black_box(&map);
    Measurement {
        name: "PackedSwiss",
        bytes,
        build,
        hits,
        misses,
        removals,
    }
}

fn time_hits(
    keys: &[[u8; 32]],
    queries: &[usize],
    mut get: impl FnMut(&[u8; 32]) -> Option<u64>,
) -> Duration {
    let started = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..LOOKUP_ROUNDS {
        for &index in queries {
            checksum ^= get(black_box(&keys[index])).expect("generated key must exist");
        }
    }
    black_box(checksum);
    started.elapsed()
}

fn time_misses(
    misses: &[[u8; 32]],
    queries: &[usize],
    mut get: impl FnMut(&[u8; 32]) -> Option<u64>,
) -> Duration {
    let started = Instant::now();
    for _ in 0..LOOKUP_ROUNDS {
        for &index in queries {
            assert!(get(black_box(&misses[index])).is_none());
        }
    }
    started.elapsed()
}

fn time_removals(keys: &[[u8; 32]], mut remove: impl FnMut(&[u8; 32]) -> Option<u64>) -> Duration {
    let started = Instant::now();
    for (index, key) in keys.iter().enumerate().step_by(REMOVE_STRIDE) {
        assert_eq!(remove(black_box(key)), Some(index as u64));
    }
    started.elapsed()
}

struct Measurement {
    name: &'static str,
    bytes: usize,
    build: Duration,
    hits: Duration,
    misses: Duration,
    removals: Duration,
}

fn ns_per(duration: Duration, operations: usize) -> f64 {
    duration.as_nanos() as f64 / operations as f64
}

fn net_live_bytes(stats: Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn key(index: u64) -> [u8; 32] {
    let mut result = [0_u8; 32];
    let mut state = index;
    for chunk in result.as_chunks_mut::<8>().0 {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    result
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
