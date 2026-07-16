//! Isolated retained-memory and operation-latency probe for hybrid generations.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::alloc::System;
use std::hint::black_box;
use std::time::Instant;

use packedgen::{FrozenPackedMap, HybridFilterMode, HybridPackedMap, PackedSwissMap};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000);
    let changed_percent = argument(&mut arguments, 1).min(100);
    let lookups = argument(&mut arguments, 2_000_000);
    assert!(entries > 0, "entry count must be positive");
    assert!(lookups > 0, "lookup count must be positive");
    let keys: Vec<_> = (0..entries).map(|index| binary_key(index as u64)).collect();
    let misses: Vec<_> = (0..entries)
        .map(|index| binary_key(entries.wrapping_add(index) as u64))
        .collect();

    println!(
        "implementation,entries,changed_percent,live_bytes,bytes_per_entry,\
         base_hit_ns,overlay_hit_ns,miss_ns,update_ns,insert_ns"
    );
    probe_frozen(&keys, &misses, lookups);
    probe_hybrid(
        &keys,
        &misses,
        changed_percent,
        lookups,
        HybridFilterMode::Disabled,
    );
    probe_hybrid(
        &keys,
        &misses,
        changed_percent,
        lookups,
        HybridFilterMode::HalfBytePerEntry,
    );
    probe_hybrid(
        &keys,
        &misses,
        changed_percent,
        lookups,
        HybridFilterMode::OneBytePerEntry,
    );
    probe_swiss(&keys, &misses, changed_percent, lookups);
}

fn probe_frozen(keys: &[[u8; 32]], misses: &[[u8; 32]], lookups: usize) {
    let region = Region::new(GLOBAL);
    let map = FrozenPackedMap::try_from_entries(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key, index as u64)),
    )
    .unwrap();
    let retained = net_live_bytes(region.change());
    let hit_ns = lookup_ns(keys, lookups, |key| map.get(key).copied());
    let miss_ns = lookup_ns(misses, lookups, |key| map.get(key).copied());
    print_row(
        "frozen",
        keys.len(),
        0,
        retained,
        [hit_ns, f64::NAN, miss_ns, f64::NAN, f64::NAN],
    );
    black_box(map);
}

fn probe_hybrid(
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    changed_percent: usize,
    lookups: usize,
    filter_mode: HybridFilterMode,
) {
    let changed = keys.len().saturating_mul(changed_percent) / 100;
    let updates = changed.div_ceil(2);
    let region = Region::new(GLOBAL);
    let base = FrozenPackedMap::try_from_entries(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key, index as u64)),
    )
    .unwrap();
    // Reserve the full churn budget: half is occupied by overlays at the RAM
    // sample and the other half is available for the timed absent inserts.
    let mut map = HybridPackedMap::with_delta_capacity_and_filter(base, changed, filter_mode);

    let started = Instant::now();
    for (index, key) in keys.iter().enumerate().take(updates) {
        let value = (index as u64).wrapping_add(1 << 48);
        map.try_insert(key, value).unwrap();
    }
    let update_ns = nanos_per_operation(started, updates);
    for key in &keys[updates..changed] {
        map.remove(key).unwrap();
    }

    let retained = net_live_bytes(region.change());
    let base_hit_ns = lookup_ns(&keys[changed..], lookups, |key| map.get(key).copied());
    let overlay_hit_ns = lookup_ns(&keys[..updates], lookups, |key| map.get(key).copied());
    let miss_ns = lookup_ns(misses, lookups, |key| map.get(key).copied());
    let stats = map.stats();
    eprintln!(
        "hybrid-components: base_arena_bytes={}, base_slot_bytes={}, \
         base_index_bits_per_entry={:.3}, delta_entries={}, delta_capacity={}, \
         delta_arena_bytes={}, tombstones={}, tombstone_bytes={}, membership_bytes={}",
        stats.base.arena_allocated_bytes,
        stats.base.slot_bytes,
        stats.base.index_bits_per_entry,
        stats.delta.len,
        stats.delta.table_capacity,
        stats.delta.arena_allocated_bytes,
        stats.tombstones,
        stats.tombstone_bytes,
        stats.membership_bytes
    );

    // Memory was sampled above, before these inserts. The full churn budget was
    // reserved up front, matching a service that knows its merge threshold.
    let inserts = updates.max(1_000).min(misses.len());
    let started = Instant::now();
    for (index, key) in misses[..inserts].iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let insert_ns = nanos_per_operation(started, inserts);
    print_row(
        match filter_mode {
            HybridFilterMode::Disabled => "hybrid-unfiltered",
            HybridFilterMode::HalfBytePerEntry => "hybrid-filtered-4bit",
            HybridFilterMode::OneBytePerEntry => "hybrid-filtered-8bit",
        },
        keys.len(),
        changed_percent,
        retained,
        [base_hit_ns, overlay_hit_ns, miss_ns, update_ns, insert_ns],
    );
    black_box(map);
}

fn probe_swiss(keys: &[[u8; 32]], misses: &[[u8; 32]], changed_percent: usize, lookups: usize) {
    let changed = keys.len().saturating_mul(changed_percent) / 100;
    let updates = changed.div_ceil(2);
    let region = Region::new(GLOBAL);
    let mut map =
        PackedSwissMap::try_with_capacity_and_key_bytes(keys.len(), keys.len().saturating_mul(32))
            .unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }

    let started = Instant::now();
    for (index, key) in keys.iter().enumerate().take(updates) {
        let value = (index as u64).wrapping_add(1 << 48);
        map.try_insert(key, value).unwrap();
    }
    let update_ns = nanos_per_operation(started, updates);
    for key in &keys[updates..changed] {
        map.remove(key).unwrap();
    }

    let retained = net_live_bytes(region.change());
    let base_hit_ns = lookup_ns(&keys[changed..], lookups, |key| map.get(key).copied());
    let overlay_hit_ns = lookup_ns(&keys[..updates], lookups, |key| map.get(key).copied());
    let miss_ns = lookup_ns(misses, lookups, |key| map.get(key).copied());
    let inserts = updates.max(1_000).min(misses.len());
    let started = Instant::now();
    for (index, key) in misses[..inserts].iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let insert_ns = nanos_per_operation(started, inserts);
    print_row(
        "packed-swiss",
        keys.len(),
        changed_percent,
        retained,
        [base_hit_ns, overlay_hit_ns, miss_ns, update_ns, insert_ns],
    );
    black_box(map);
}

fn lookup_ns(keys: &[[u8; 32]], lookups: usize, mut get: impl FnMut(&[u8]) -> Option<u64>) -> f64 {
    if keys.is_empty() {
        return f64::NAN;
    }
    run_lookups(keys, lookups.min(100_000), &mut get);
    let mut samples = [0_u128; 5];
    for elapsed in &mut samples {
        let start = Instant::now();
        run_lookups(keys, lookups, &mut get);
        *elapsed = start.elapsed().as_nanos();
    }
    samples.sort_unstable();
    samples[samples.len() / 2] as f64 / lookups as f64
}

fn run_lookups(keys: &[[u8; 32]], lookups: usize, mut get: impl FnMut(&[u8]) -> Option<u64>) {
    let mut checksum = 0_u64;
    for query in 0..lookups {
        let key = &keys[scramble(query) % keys.len()];
        checksum ^= black_box(get(black_box(key))).unwrap_or_default();
    }
    black_box(checksum);
}

fn print_row(
    name: &str,
    entries: usize,
    changed_percent: usize,
    live_bytes: usize,
    latency_ns: [f64; 5],
) {
    println!(
        "{name},{entries},{changed_percent},{live_bytes},{:.3},\
         {:.3},{:.3},{:.3},{:.3},{:.3}",
        live_bytes as f64 / entries as f64,
        latency_ns[0],
        latency_ns[1],
        latency_ns[2],
        latency_ns[3],
        latency_ns[4]
    );
}

fn nanos_per_operation(started: Instant, operations: usize) -> f64 {
    if operations == 0 {
        f64::NAN
    } else {
        started.elapsed().as_nanos() as f64 / operations as f64
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected integer"))
}

fn net_live_bytes(stats: Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn scramble(mut value: usize) -> usize {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb_352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846c_a68b);
    value ^ (value >> 16)
}

fn binary_key(index: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
