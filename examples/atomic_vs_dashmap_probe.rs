//! Warm, alternating-order comparison of atomic generations and `DashMap`.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hashbrown::DefaultHashBuilder;
use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

type Dash = DashMap<Box<[u8]>, u64, DefaultHashBuilder>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let requested_operations = argument(&mut arguments, 300_000).max(1);
    let maximum_threads = argument(&mut arguments, 8).max(1);
    let sample_count = argument(&mut arguments, 15).max(3);
    let shards = argument(&mut arguments, 64).max(2).next_power_of_two();
    let workload_filter = arguments.next();
    let implementations = Implementation::ALL;
    let hash_builder = DefaultHashBuilder::default();
    let generation_hash_builder = GenerationHashBuilder::default();
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let misses = (entries as u64..entries.saturating_add(requested_operations) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();
    let thread_counts = if maximum_threads == 1 {
        vec![1]
    } else {
        vec![1, maximum_threads]
    };

    println!(
        "implementation,workload,entries,operations,threads,shards,samples,median_ns,throughput_mops"
    );
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| workload.name() != filter)
        {
            continue;
        }
        let operations = workload.operations(entries, requested_operations);
        for &threads in &thread_counts {
            let mut measurements = implementations.map(|_| Vec::with_capacity(sample_count));
            for sample in 0..sample_count {
                for offset in 0..implementations.len() {
                    let implementation_index = (sample + offset) % implementations.len();
                    measurements[implementation_index].push(measure(
                        implementations[implementation_index],
                        workload,
                        operations,
                        threads,
                        shards,
                        &keys,
                        &misses,
                        &hash_builder,
                        &generation_hash_builder,
                    ));
                }
            }
            for (implementation, samples) in implementations.into_iter().zip(&mut measurements) {
                samples.sort_unstable();
                let median = samples[samples.len() / 2];
                let throughput = operations as f64 / median.as_secs_f64() / 1_000_000.0;
                println!(
                    "{},{},{entries},{operations},{threads},{shards},{},{},{throughput:.3}",
                    implementation.name(),
                    workload.name(),
                    samples.len(),
                    median.as_nanos(),
                );
            }
        }
    }
}

fn measure(
    implementation: Implementation,
    workload: Workload,
    operations: usize,
    threads: usize,
    shards: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    hash_builder: &DefaultHashBuilder,
    generation_hash_builder: &GenerationHashBuilder,
) -> Duration {
    let map = BenchMap::build(
        implementation,
        workload.new_key_capacity(operations),
        shards,
        keys,
        hash_builder,
        generation_hash_builder,
    );
    for key in keys {
        black_box(map.get(key));
    }
    for key in misses.iter().take(keys.len()) {
        black_box(map.get(key));
    }
    let mixed_reads = match workload {
        Workload::Read95Hit => Some(read_trace(operations, 95, keys, misses)),
        Workload::Read99Hit => Some(read_trace(operations, 99, keys, misses)),
        _ => None,
    };
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    let elapsed = std::thread::scope(|scope| {
        for thread in 0..threads {
            let map = &map;
            let start = &start;
            let done = &done;
            let mixed_reads = mixed_reads.as_deref();
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    run_operation(map, workload, operation, keys, misses, mixed_reads);
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

fn run_operation(
    map: &BenchMap,
    workload: Workload,
    operation: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    mixed_reads: Option<&[&[u8; 32]]>,
) {
    let hit_index = mix(operation as u64) as usize % keys.len();
    match workload {
        Workload::ReadHit => {
            black_box(map.get(&keys[hit_index]));
        }
        Workload::ReadMiss => {
            black_box(map.get(&misses[operation]));
        }
        Workload::Read95Hit | Workload::Read99Hit => {
            black_box(map.get(mixed_reads.unwrap()[operation]));
        }
        Workload::InsertHit => {
            black_box(map.insert(&keys[hit_index], operation as u64));
        }
        Workload::InsertMiss => {
            black_box(map.insert(&misses[operation], operation as u64));
        }
        Workload::UpdateHit => {
            black_box(map.update(&keys[hit_index]));
        }
        Workload::UpdateMiss => {
            black_box(map.update(&misses[operation]));
        }
        Workload::UpdateHot => {
            black_box(map.update(&keys[0]));
        }
        Workload::DeleteHit => {
            black_box(map.remove(&keys[operation]));
        }
        Workload::DeleteMiss => {
            black_box(map.remove(&misses[operation]));
        }
        Workload::CacheMix90 => run_cache_mix_90(map, operation, hit_index, keys, misses),
        Workload::CacheMix95 => run_cache_mix_95(map, operation, hit_index, keys, misses),
    }
}

fn run_cache_mix_90(
    map: &BenchMap,
    operation: usize,
    hit_index: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    match mix((operation as u64) ^ 0xa5a5_5a5a) % 100 {
        0..=89 => black_box(map.get(&keys[hit_index])),
        90..=94 => black_box(map.get(&misses[operation])),
        95..=97 => black_box(map.update(&keys[hit_index]).then_some(0)),
        98 => black_box(map.insert(&misses[operation], operation as u64).map(|_| 0)),
        _ => black_box(map.remove(&keys[hit_index]).map(|_| 0)),
    };
}

fn run_cache_mix_95(
    map: &BenchMap,
    operation: usize,
    hit_index: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    match mix((operation as u64) ^ 0x5a5a_a5a5) % 200 {
        0..=189 => black_box(map.get(&keys[hit_index])),
        190..=193 => black_box(map.get(&misses[operation])),
        194..=197 => black_box(map.update(&keys[hit_index]).then_some(0)),
        198 => black_box(map.insert(&misses[operation], operation as u64).map(|_| 0)),
        _ => black_box(map.remove(&keys[hit_index]).map(|_| 0)),
    };
}

enum BenchMap {
    Atomic(LockFreeAtomicU64GenerationMap),
    Dash(Dash),
}

impl BenchMap {
    fn build(
        implementation: Implementation,
        new_key_capacity: usize,
        shards: usize,
        keys: &[[u8; 32]],
        hash_builder: &DefaultHashBuilder,
        generation_hash_builder: &GenerationHashBuilder,
    ) -> Self {
        if let Some(filter) = implementation.filter() {
            Self::Atomic(
                LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash(
                    keys.iter().enumerate().map(|(index, key)| {
                        (
                            key,
                            NonMaxU64::new(index as u64).expect("benchmark index is representable"),
                        )
                    }),
                    new_key_capacity,
                    AtomicGenerationOverlay::AtomicFixed32,
                    filter,
                    generation_hash_builder.clone(),
                )
                .unwrap(),
            )
        } else {
            let map = DashMap::with_capacity_and_hasher_and_shard_amount(
                keys.len().saturating_add(new_key_capacity),
                hash_builder.clone(),
                shards,
            );
            for (index, key) in keys.iter().enumerate() {
                map.insert(key.as_slice().into(), index as u64);
            }
            Self::Dash(map)
        }
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        match self {
            Self::Atomic(map) => map.get(key).map(NonMaxU64::get),
            Self::Dash(map) => map.get(key).map(|value| *value),
        }
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        match self {
            Self::Atomic(map) => match map.insert(key, non_max(value)) {
                packedgen::InsertOutcome::Inserted => None,
                packedgen::InsertOutcome::Replaced(previous) => Some(previous.get()),
            },
            Self::Dash(map) => map.insert(key.into(), value),
        }
    }

    fn update(&self, key: &[u8]) -> bool {
        match self {
            Self::Atomic(map) => map.update(key, increment).is_some(),
            Self::Dash(map) => {
                let Some(mut value) = map.get_mut(key) else {
                    return false;
                };
                *value += 1;
                true
            }
        }
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        match self {
            Self::Atomic(map) => map.remove(key).map(NonMaxU64::get),
            Self::Dash(map) => map.remove(key).map(|(_, value)| value),
        }
    }
}

#[derive(Clone, Copy)]
enum Implementation {
    AtomicExact,
    AtomicEmbedded,
    AtomicOneByte,
    DashMap,
}

impl Implementation {
    const ALL: [Self; 4] = [
        Self::AtomicExact,
        Self::AtomicEmbedded,
        Self::AtomicOneByte,
        Self::DashMap,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AtomicExact => "atomic-exact-default",
            Self::AtomicEmbedded => "atomic-embedded-fingerprint",
            Self::AtomicOneByte => "atomic-one-byte-filter",
            Self::DashMap => "dashmap-boxed-key",
        }
    }

    const fn filter(self) -> Option<AtomicGenerationBaseFilter> {
        match self {
            Self::AtomicExact => Some(AtomicGenerationBaseFilter::Disabled),
            Self::AtomicEmbedded => Some(AtomicGenerationBaseFilter::EmbeddedFingerprint),
            Self::AtomicOneByte => Some(AtomicGenerationBaseFilter::OneBytePerEntry),
            Self::DashMap => None,
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadMiss,
    Read95Hit,
    Read99Hit,
    InsertHit,
    InsertMiss,
    UpdateHit,
    UpdateMiss,
    UpdateHot,
    DeleteHit,
    DeleteMiss,
    CacheMix90,
    CacheMix95,
}

impl Workload {
    const ALL: [Self; 13] = [
        Self::ReadHit,
        Self::ReadMiss,
        Self::Read95Hit,
        Self::Read99Hit,
        Self::InsertHit,
        Self::InsertMiss,
        Self::UpdateHit,
        Self::UpdateMiss,
        Self::UpdateHot,
        Self::DeleteHit,
        Self::DeleteMiss,
        Self::CacheMix90,
        Self::CacheMix95,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadMiss => "read_miss",
            Self::Read95Hit => "read_95_hit",
            Self::Read99Hit => "read_99_hit",
            Self::InsertHit => "insert_hit",
            Self::InsertMiss => "insert_miss",
            Self::UpdateHit => "update_hit",
            Self::UpdateMiss => "update_miss",
            Self::UpdateHot => "update_hot_key",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
            Self::CacheMix90 => "cache_mix_90rh5rm3u1i1d",
            Self::CacheMix95 => "cache_mix_95rh2rm2u0.5i0.5d",
        }
    }

    const fn operations(self, entries: usize, requested: usize) -> usize {
        if matches!(self, Self::DeleteHit) {
            if requested < entries {
                requested
            } else {
                entries
            }
        } else {
            requested
        }
    }

    const fn new_key_capacity(self, operations: usize) -> usize {
        match self {
            Self::InsertMiss => operations,
            Self::CacheMix90 => operations.div_ceil(100),
            Self::CacheMix95 => operations.div_ceil(200),
            _ => 0,
        }
    }
}

fn read_trace<'a>(
    operations: usize,
    hit_percentage: usize,
    keys: &'a [[u8; 32]],
    misses: &'a [[u8; 32]],
) -> Vec<&'a [u8; 32]> {
    (0..operations)
        .map(|operation| {
            let mixed = mix(operation as u64) as usize;
            if mixed % 100 < hit_percentage {
                &keys[mixed % keys.len()]
            } else {
                &misses[operation]
            }
        })
        .collect()
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("benchmark values remain representable")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = value;
    for chunk in key.as_chunks_mut::<8>().0 {
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
