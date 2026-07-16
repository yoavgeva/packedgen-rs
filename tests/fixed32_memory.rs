#![allow(missing_docs)]

use std::alloc::System;
use std::hint::black_box;

use packedgen::{Fixed32Load, Fixed32SoaMap, PackedSwissMap, SegmentedLoad, SegmentedSwissMap};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const ENTRIES: usize = 250_000;

#[test]
fn fixed_width_soa_retains_less_than_both_packed_swiss_layouts() {
    let keys: Vec<_> = (0..ENTRIES as u64).map(key).collect();
    let fixed = fixed_live_bytes(&keys);
    let segmented = segmented_live_bytes(&keys);
    let packed = packed_live_bytes(&keys);

    assert!(
        fixed < segmented,
        "fixed32={fixed} must beat segmented={segmented}"
    );
    assert!(fixed < packed, "fixed32={fixed} must beat packed={packed}");
}

fn fixed_live_bytes(keys: &[[u8; 32]]) -> usize {
    let region = Region::new(GLOBAL);
    let mut map = Fixed32SoaMap::with_capacity_and_load(keys.len(), Fixed32Load::Balanced);
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(*key, index as u64).unwrap();
    }
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn segmented_live_bytes(keys: &[[u8; 32]]) -> usize {
    let region = Region::new(GLOBAL);
    let mut map = SegmentedSwissMap::try_with_capacity_key_bytes_and_load(
        keys.len(),
        keys.len() * 32,
        SegmentedLoad::Balanced,
    )
    .unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn packed_live_bytes(keys: &[[u8; 32]]) -> usize {
    let region = Region::new(GLOBAL);
    let mut map =
        PackedSwissMap::try_with_capacity_and_key_bytes(keys.len(), keys.len() * 32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn net_live_bytes(stats: Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn key(index: u64) -> [u8; 32] {
    let mut result = [0_u8; 32];
    let mut state = index;
    for chunk in result.chunks_exact_mut(8) {
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
