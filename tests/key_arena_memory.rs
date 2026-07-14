#![allow(missing_docs)]

use std::alloc::System;
use std::hint::black_box;

use elastichash::{PackedKeyArena, PackedKeyRef};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const ENTRIES: usize = 100_000;
const KEY_BYTES: usize = 32;

#[test]
fn packed_arena_removes_per_key_allocation_and_reduces_live_bytes() {
    let packed = measure_packed();
    let boxed = measure_boxed();

    assert!(
        packed.allocations < 10,
        "packed arena unexpectedly allocated {} times",
        packed.allocations
    );
    assert!(
        boxed.allocations >= ENTRIES,
        "boxed keys unexpectedly allocated only {} times",
        boxed.allocations
    );
    assert!(
        packed.live_bytes * 20 < boxed.live_bytes * 19,
        "expected packed keys to use at least 5% fewer requested live bytes: \
         packed={}, boxed={}",
        packed.live_bytes,
        boxed.live_bytes
    );
}

fn measure_packed() -> Measurement {
    let region = Region::new(GLOBAL);
    let mut arena = PackedKeyArena::with_segment_bytes(ENTRIES * KEY_BYTES).unwrap();
    let mut references = Vec::<PackedKeyRef>::with_capacity(ENTRIES);
    for index in 0..ENTRIES as u64 {
        let key = binary_key(index);
        references.push(arena.insert(&key).unwrap());
    }
    let stats = region.change();
    black_box((&arena, &references));
    Measurement::from(stats)
}

fn measure_boxed() -> Measurement {
    let region = Region::new(GLOBAL);
    let mut keys = Vec::<Box<[u8]>>::with_capacity(ENTRIES);
    for index in 0..ENTRIES as u64 {
        keys.push(binary_key(index).to_vec().into_boxed_slice());
    }
    let stats = region.change();
    black_box(&keys);
    Measurement::from(stats)
}

#[derive(Clone, Copy)]
struct Measurement {
    live_bytes: usize,
    allocations: usize,
}

impl From<Stats> for Measurement {
    fn from(stats: Stats) -> Self {
        let allocated = stats
            .bytes_allocated
            .saturating_sub(stats.bytes_deallocated);
        let live_bytes = if stats.bytes_reallocated >= 0 {
            allocated.saturating_add(stats.bytes_reallocated.cast_unsigned())
        } else {
            allocated.saturating_sub(stats.bytes_reallocated.unsigned_abs())
        };
        Self {
            live_bytes,
            allocations: stats.allocations,
        }
    }
}

fn binary_key(mut state: u64) -> [u8; KEY_BYTES] {
    let mut key = [0_u8; KEY_BYTES];
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
