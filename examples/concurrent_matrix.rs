//! Multi-reader/multi-writer operation comparison for cache-like binary keys.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hashbrown::{DefaultHashBuilder, HashMap};
use packedgen::{
    AtomicGenerationOverlay, ConcurrentSwissMap, LockFreeAtomicU64GenerationMap, LockFreeBinaryMap,
    LockFreeGenerationMap, NonMaxU64, SegmentedLoad,
};
use parking_lot::RwLock;

const SAMPLES: usize = 3;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000);
    let operations = argument(&mut arguments, 500_000);
    let maximum_threads = argument(&mut arguments, 8).max(1);
    let shard_count = argument(&mut arguments, 64).max(1).next_power_of_two();
    let workload_filter = arguments.next();
    assert!(entries > 0);

    let keys: Vec<_> = (0..entries as u64).map(binary_key).collect();
    let miss_count = entries.max(operations);
    let misses: Vec<_> = (entries as u64..entries.saturating_add(miss_count) as u64)
        .map(binary_key)
        .collect();
    let thread_counts = powers_of_two_through(maximum_threads);

    println!("implementation,workload,entries,operations,threads,shards,median_ns,throughput_mops");
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| workload.name() != filter)
        {
            continue;
        }
        let workload_operations = workload.operations(entries, operations);
        for threads in &thread_counts {
            print_measurement::<Concurrent>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<LockFree>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<LockFreeGeneration>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<LockFreeAtomicGeneration>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<LockFreeAtomicPapayaGeneration>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<Dash>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
            print_measurement::<Locked>(
                workload,
                entries,
                workload_operations,
                *threads,
                shard_count,
                &keys,
                &misses,
            );
        }
    }
}

fn print_measurement<M: BenchMap>(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    shards: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let capacity = workload.capacity(entries, operations);
        let map = if workload.prefill() {
            M::build_filled(capacity, shards, keys)
        } else {
            M::build(capacity, shards)
        };
        samples.push(run_parallel(
            &map, workload, operations, threads, keys, misses,
        ));
        black_box(map);
    }
    samples.sort_unstable();
    let median = samples[SAMPLES / 2];
    let throughput = operations as f64 / median.as_secs_f64() / 1_000_000.0;
    println!(
        "{},{},{entries},{operations},{threads},{shards},{},{throughput:.3}",
        M::NAME,
        workload.name(),
        median.as_nanos()
    );
}

fn run_parallel<M: BenchMap>(
    map: &M,
    workload: Workload,
    operations: usize,
    threads: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) -> Duration {
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            let start = &start;
            let done = &done;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    run_operation(map, workload, operation, keys, misses);
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    })
}

fn run_operation<M: BenchMap>(
    map: &M,
    workload: Workload,
    operation: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    let hit_index = mix(operation as u64) as usize % keys.len();
    match workload {
        Workload::ReadHit => {
            black_box(map.get(&keys[hit_index]));
        }
        Workload::ReadMiss => {
            black_box(map.get(&misses[operation % misses.len()]));
        }
        Workload::UpdateHit => {
            black_box(map.update(&keys[hit_index]));
        }
        Workload::UpdateHot => {
            black_box(map.update(&keys[0]));
        }
        Workload::InsertMiss => {
            map.insert(&misses[operation % misses.len()], operation as u64);
        }
        Workload::DeleteHit => {
            black_box(map.remove(&keys[operation]));
        }
        Workload::DeleteMiss => {
            black_box(map.remove(&misses[operation % misses.len()]));
        }
        Workload::CacheMix => {
            let selector = mix((operation as u64) ^ 0xa5a5_5a5a) % 100;
            match selector {
                0..=89 => {
                    black_box(map.get(&keys[hit_index]));
                }
                90..=94 => {
                    black_box(map.get(&misses[operation % misses.len()]));
                }
                95..=97 => {
                    black_box(map.update(&keys[hit_index]));
                }
                98 => {
                    map.insert(&misses[operation % misses.len()], operation as u64);
                }
                _ => {
                    black_box(map.remove(&keys[hit_index]));
                }
            }
        }
    }
}

trait BenchMap: Sync + Sized {
    const NAME: &'static str;

    fn build(capacity: usize, shards: usize) -> Self;
    fn build_filled(capacity: usize, shards: usize, keys: &[[u8; 32]]) -> Self {
        let map = Self::build(capacity, shards);
        for (index, key) in keys.iter().enumerate() {
            map.insert(key, index as u64);
        }
        map
    }
    fn get(&self, key: &[u8]) -> Option<u64>;
    fn insert(&self, key: &[u8], value: u64);
    fn update(&self, key: &[u8]) -> bool;
    fn remove(&self, key: &[u8]) -> bool;
}

struct Concurrent(ConcurrentSwissMap<u64>);

impl BenchMap for Concurrent {
    const NAME: &'static str = "concurrent-segmented-swiss";

    fn build(capacity: usize, shards: usize) -> Self {
        Self(
            ConcurrentSwissMap::try_with_capacity_and_shards(
                capacity,
                shards,
                SegmentedLoad::Balanced,
            )
            .unwrap(),
        )
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get_cloned(key)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.try_insert(key, value).unwrap();
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.update(key, |value| *value += 1).is_some()
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

struct LockFree(LockFreeBinaryMap<u64>);

impl BenchMap for LockFree {
    const NAME: &'static str = "lockfree-papaya-binary";

    fn build(capacity: usize, _shards: usize) -> Self {
        Self(LockFreeBinaryMap::with_capacity(capacity))
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get_cloned(key)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.insert(key, value);
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.update(key, |value| value + 1).is_some()
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

struct LockFreeGeneration(LockFreeGenerationMap<u64>);

impl BenchMap for LockFreeGeneration {
    const NAME: &'static str = "lockfree-packed-generation";

    fn build(capacity: usize, _shards: usize) -> Self {
        Self(
            LockFreeGenerationMap::try_from_entries(
                std::iter::empty::<([u8; 32], u64)>(),
                capacity,
            )
            .unwrap(),
        )
    }

    fn build_filled(capacity: usize, _shards: usize, keys: &[[u8; 32]]) -> Self {
        let overlay_capacity = capacity.saturating_sub(keys.len());
        Self(
            LockFreeGenerationMap::try_from_entries(
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (*key, index as u64)),
                overlay_capacity,
            )
            .unwrap(),
        )
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get_cloned(key)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.insert(key, value);
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.update(key, |value| value + 1).is_some()
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

struct LockFreeAtomicGeneration(LockFreeAtomicU64GenerationMap);

impl BenchMap for LockFreeAtomicGeneration {
    const NAME: &'static str = "lockfree-packed-generation-atomic-compact32";

    fn build(capacity: usize, _shards: usize) -> Self {
        Self(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                capacity,
            )
            .unwrap(),
        )
    }

    fn build_filled(capacity: usize, _shards: usize, keys: &[[u8; 32]]) -> Self {
        let overlay_capacity = capacity.saturating_sub(keys.len());
        Self(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                keys.iter().enumerate().map(|(index, key)| {
                    (
                        *key,
                        NonMaxU64::new(index as u64).expect("benchmark index is representable"),
                    )
                }),
                overlay_capacity,
            )
            .unwrap(),
        )
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get(key).map(NonMaxU64::get)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.insert(
            key,
            NonMaxU64::new(value).expect("benchmark value is representable"),
        );
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0
            .update(key, |value| {
                NonMaxU64::new(value.get() + 1).expect("benchmark value remains representable")
            })
            .is_some()
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

struct LockFreeAtomicPapayaGeneration(LockFreeAtomicU64GenerationMap);

impl BenchMap for LockFreeAtomicPapayaGeneration {
    const NAME: &'static str = "lockfree-packed-generation-atomic-papaya";

    fn build(capacity: usize, _shards: usize) -> Self {
        Self(
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                capacity,
                AtomicGenerationOverlay::Papaya,
            )
            .unwrap(),
        )
    }

    fn build_filled(capacity: usize, _shards: usize, keys: &[[u8; 32]]) -> Self {
        let overlay_capacity = capacity.saturating_sub(keys.len());
        Self(
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                keys.iter().enumerate().map(|(index, key)| {
                    (
                        *key,
                        NonMaxU64::new(index as u64).expect("benchmark index is representable"),
                    )
                }),
                overlay_capacity,
                AtomicGenerationOverlay::Papaya,
            )
            .unwrap(),
        )
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get(key).map(NonMaxU64::get)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.insert(
            key,
            NonMaxU64::new(value).expect("benchmark value is representable"),
        );
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0
            .update(key, |value| {
                NonMaxU64::new(value.get() + 1).expect("benchmark value remains representable")
            })
            .is_some()
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

type DashInner = DashMap<Box<[u8]>, u64, DefaultHashBuilder>;

struct Dash(DashInner);

impl BenchMap for Dash {
    const NAME: &'static str = "dashmap-packed-key";

    fn build(capacity: usize, shards: usize) -> Self {
        Self(DashMap::with_capacity_and_hasher_and_shard_amount(
            capacity,
            DefaultHashBuilder::default(),
            shards,
        ))
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get(key).map(|value| *value)
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.insert(key.into(), value);
    }

    fn update(&self, key: &[u8]) -> bool {
        let Some(mut value) = self.0.get_mut(key) else {
            return false;
        };
        *value += 1;
        true
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.remove(key).is_some()
    }
}

struct Locked(RwLock<HashMap<Box<[u8]>, u64>>);

impl BenchMap for Locked {
    const NAME: &'static str = "single-rwlock-hashbrown";

    fn build(capacity: usize, _shards: usize) -> Self {
        Self(RwLock::new(HashMap::with_capacity(capacity)))
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.read().get(key).copied()
    }

    fn insert(&self, key: &[u8], value: u64) {
        self.0.write().insert(key.into(), value);
    }

    fn update(&self, key: &[u8]) -> bool {
        let mut map = self.0.write();
        let Some(value) = map.get_mut(key) else {
            return false;
        };
        *value += 1;
        true
    }

    fn remove(&self, key: &[u8]) -> bool {
        self.0.write().remove(key).is_some()
    }
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadMiss,
    UpdateHit,
    UpdateHot,
    InsertMiss,
    DeleteHit,
    DeleteMiss,
    CacheMix,
}

impl Workload {
    const ALL: [Self; 8] = [
        Self::ReadHit,
        Self::ReadMiss,
        Self::UpdateHit,
        Self::UpdateHot,
        Self::InsertMiss,
        Self::DeleteHit,
        Self::DeleteMiss,
        Self::CacheMix,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadMiss => "read_miss",
            Self::UpdateHit => "update_hit",
            Self::UpdateHot => "update_hot_key",
            Self::InsertMiss => "insert_miss",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
            Self::CacheMix => "cache_mix_90r5m3u1i1d",
        }
    }

    const fn prefill(self) -> bool {
        !matches!(self, Self::InsertMiss)
    }

    fn operations(self, entries: usize, requested: usize) -> usize {
        if matches!(self, Self::DeleteHit) {
            requested.min(entries)
        } else {
            requested
        }
    }

    fn capacity(self, entries: usize, operations: usize) -> usize {
        match self {
            Self::InsertMiss => operations,
            Self::CacheMix => entries.saturating_add(operations.div_ceil(100)),
            _ => entries,
        }
    }
}

fn powers_of_two_through(maximum: usize) -> Vec<usize> {
    let mut result = Vec::new();
    let mut threads = 1;
    while threads <= maximum {
        result.push(threads);
        let Some(next) = threads.checked_mul(2) else {
            break;
        };
        threads = next;
    }
    if result.last().copied() != Some(maximum) {
        result.push(maximum);
    }
    result
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
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
