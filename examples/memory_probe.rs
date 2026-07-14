//! Isolated requested-allocation comparison for `ElasticHash` and `HashBrown`.

use std::alloc::System;
use std::hint::black_box;

use elastichash::{ElasticConfig, FixedElasticMap};
use hashbrown::HashMap;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let implementation = arguments.next().unwrap_or_else(|| "all".to_owned());
    let entries = arguments.next().map_or(1_000_000, |value| {
        value.parse::<usize>().expect("entries must be an integer")
    });

    println!("implementation,entries,live_bytes,bytes_per_entry,allocations");
    match implementation.as_str() {
        "elastic-3" => print_elastic(entries, 3),
        "elastic-6" => print_elastic(entries, 6),
        "hashbrown" => print_hashbrown(entries),
        "elastic-binary-3" => print_elastic_binary(entries, 3),
        "elastic-binary-6" => print_elastic_binary(entries, 6),
        "hashbrown-binary" => print_hashbrown_binary(entries),
        "all" => {
            print_elastic(entries, 3);
            print_elastic(entries, 6);
            print_hashbrown(entries);
            print_elastic_binary(entries, 3);
            print_elastic_binary(entries, 6);
            print_hashbrown_binary(entries);
        }
        _ => panic!(
            "expected elastic-3, elastic-6, hashbrown, elastic-binary-3, \
             elastic-binary-6, hashbrown-binary, or all"
        ),
    }
}

fn print_elastic(entries: usize, exponent: u32) {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap();
    let mut map = FixedElasticMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(mix(key), key).unwrap();
    }
    let stats = region.change();
    print_row(&format!("elastic-2^-{exponent}"), entries, stats);
    black_box(&map);
}

fn print_hashbrown(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut map = HashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(mix(key), key);
    }
    let stats = region.change();
    print_row("hashbrown", entries, stats);
    black_box(&map);
}

fn print_elastic_binary(entries: usize, exponent: u32) {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap();
    let mut map = FixedElasticMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(binary_key(key), key).unwrap();
    }
    let stats = region.change();
    print_row(&format!("elastic-binary32-2^-{exponent}"), entries, stats);
    black_box(&map);
}

fn print_hashbrown_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut map = HashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(binary_key(key), key);
    }
    let stats = region.change();
    print_row("hashbrown-binary32", entries, stats);
    black_box(&map);
}

fn print_row(name: &str, entries: usize, stats: Stats) {
    let live_bytes = net_live_bytes(stats);
    #[allow(clippy::cast_precision_loss)]
    let bytes_per_entry = live_bytes as f64 / entries as f64;
    println!(
        "{name},{entries},{live_bytes},{bytes_per_entry:.3},{}",
        stats.allocations
    );
}

fn net_live_bytes(stats: Stats) -> usize {
    let allocated = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated);
    if stats.bytes_reallocated >= 0 {
        allocated.saturating_add(stats.bytes_reallocated.cast_unsigned())
    } else {
        allocated.saturating_sub(stats.bytes_reallocated.unsigned_abs())
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn binary_key(index: u64) -> Box<[u8]> {
    let mut key = vec![0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key.into_boxed_slice()
}
