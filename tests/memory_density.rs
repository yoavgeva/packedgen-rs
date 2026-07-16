#![allow(missing_docs)]

use std::alloc::System;
use std::hint::black_box;

use hashbrown::HashMap;
use packedgen::{ElasticConfig, FixedElasticMap};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const ENTRIES: usize = 250_000;

#[test]
fn high_occupancy_uses_materially_fewer_requested_bytes_than_hashbrown() {
    let elastic_bytes = elastic_live_bytes(ENTRIES, 6);
    let hashbrown_bytes = hashbrown_live_bytes(ENTRIES);

    assert!(
        elastic_bytes * 4 < hashbrown_bytes * 3,
        "expected at least 25% requested-byte saving: elastic={elastic_bytes}, \
         hashbrown={hashbrown_bytes}"
    );
}

fn elastic_live_bytes(entries: usize, exponent: u32) -> usize {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap();
    let mut map = FixedElasticMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(mix(key), key).unwrap();
    }
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn hashbrown_live_bytes(entries: usize) -> usize {
    let region = Region::new(GLOBAL);
    let mut map = HashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(mix(key), key);
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

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
