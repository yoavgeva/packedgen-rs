//! Warm update-path allocation comparison.

#![allow(missing_docs, clippy::cast_precision_loss)]

use std::alloc::System;
use std::hint::black_box;

use dashmap::DashMap;
use hashbrown::DefaultHashBuilder;
use packedgen::{
    ConcurrentSwissMap, LockFreeAtomicU64GenerationMap, LockFreeBinaryMap, LockFreeGenerationMap,
    NonMaxU64,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let operations = argument(&mut arguments, 1_000_000);
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();

    println!(
        "implementation,entries,operations,allocations,deallocations,bytes_allocated,bytes_per_operation,net_live_bytes"
    );
    measure_atomic_generation(&keys, operations);
    measure_generation(&keys, operations);
    measure_papaya(&keys, operations);
    measure_concurrent_swiss(&keys, operations);
    measure_dashmap(&keys, operations);
}

fn measure_atomic_generation(keys: &[[u8; 32]], operations: usize) {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        keys.iter().enumerate().map(|(index, key)| {
            (
                *key,
                NonMaxU64::new(index as u64).expect("test index is representable"),
            )
        }),
        keys.len(),
    )
    .unwrap();
    for key in keys {
        map.update(key, increment).unwrap();
    }
    let region = Region::new(GLOBAL);
    for operation in 0..operations {
        map.update(&keys[operation % keys.len()], increment)
            .unwrap();
    }
    print_stats(
        "lockfree-packed-generation-atomic-warm",
        keys.len(),
        operations,
        region.change(),
    );
    black_box(map);
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    NonMaxU64::new(value.get() + 1).expect("probe values do not reach the reserved marker")
}

fn measure_generation(keys: &[[u8; 32]], operations: usize) {
    let map = LockFreeGenerationMap::try_from_entries(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (*key, index as u64)),
        keys.len(),
    )
    .unwrap();
    for key in keys {
        map.update(key, |value| value + 1).unwrap();
    }
    let region = Region::new(GLOBAL);
    for operation in 0..operations {
        map.update(&keys[operation % keys.len()], |value| value + 1)
            .unwrap();
    }
    print_stats(
        "lockfree-packed-generation-warm",
        keys.len(),
        operations,
        region.change(),
    );
    black_box(map);
}

fn measure_papaya(keys: &[[u8; 32]], operations: usize) {
    let map = LockFreeBinaryMap::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        map.insert(key, index as u64);
    }
    let region = Region::new(GLOBAL);
    for operation in 0..operations {
        map.update(&keys[operation % keys.len()], |value| value + 1)
            .unwrap();
    }
    print_stats(
        "lockfree-papaya-owned-key",
        keys.len(),
        operations,
        region.change(),
    );
    black_box(map);
}

fn measure_concurrent_swiss(keys: &[[u8; 32]], operations: usize) {
    let map = ConcurrentSwissMap::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let region = Region::new(GLOBAL);
    for operation in 0..operations {
        map.update(&keys[operation % keys.len()], |value| *value += 1)
            .unwrap();
    }
    print_stats(
        "concurrent-segmented-swiss",
        keys.len(),
        operations,
        region.change(),
    );
    black_box(map);
}

fn measure_dashmap(keys: &[[u8; 32]], operations: usize) {
    let map = DashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher(
        keys.len(),
        DefaultHashBuilder::default(),
    );
    for (index, key) in keys.iter().enumerate() {
        map.insert(key.to_vec().into_boxed_slice(), index as u64);
    }
    let region = Region::new(GLOBAL);
    for operation in 0..operations {
        *map.get_mut(keys[operation % keys.len()].as_slice())
            .unwrap() += 1;
    }
    print_stats(
        "dashmap-borrowed-update",
        keys.len(),
        operations,
        region.change(),
    );
    black_box(map);
}

fn print_stats(name: &str, entries: usize, operations: usize, stats: Stats) {
    let bytes_per_operation = stats.bytes_allocated as f64 / operations.max(1) as f64;
    let net_live_bytes = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated);
    println!(
        "{name},{entries},{operations},{},{},{},{bytes_per_operation:.3},{net_live_bytes}",
        stats.allocations, stats.deallocations, stats.bytes_allocated
    );
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be integers")
    })
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
