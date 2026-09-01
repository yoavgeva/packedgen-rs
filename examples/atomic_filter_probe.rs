//! Alternating-order operation probe for frozen-base negative filtering.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let requested_operations = argument(&mut arguments, 500_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let sample_count = argument(&mut arguments, 9).max(3);
    let workload_filter = arguments.next();
    let filters = [
        AtomicGenerationBaseFilter::Disabled,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
        AtomicGenerationBaseFilter::OneBytePerEntry,
    ];
    let writer_hash_builder = GenerationHashBuilder::default();
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let misses = (entries as u64..entries.saturating_add(requested_operations) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();

    println!("filter,workload,entries,operations,threads,samples,median_ns,throughput_mops");
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| workload.name() != filter)
        {
            continue;
        }
        let operations = workload.operations(entries, requested_operations);
        let mut measurements = filters.map(|_| Vec::with_capacity(sample_count));
        for sample in 0..sample_count {
            for offset in 0..filters.len() {
                let filter_index = (sample + offset) % filters.len();
                measurements[filter_index].push(measure(
                    filters[filter_index],
                    workload,
                    operations,
                    threads,
                    &keys,
                    &misses,
                    &writer_hash_builder,
                ));
            }
        }
        for (filter, samples) in filters.into_iter().zip(&mut measurements) {
            samples.sort_unstable();
            let median = samples[samples.len() / 2];
            let throughput = operations as f64 / median.as_secs_f64() / 1_000_000.0;
            println!(
                "{},{},{entries},{operations},{threads},{},{},{throughput:.3}",
                filter_name(filter),
                workload.name(),
                samples.len(),
                median.as_nanos(),
            );
        }
    }
}

fn measure(
    filter: AtomicGenerationBaseFilter,
    workload: Workload,
    operations: usize,
    threads: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    writer_hash_builder: &GenerationHashBuilder,
) -> Duration {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key, value(index as u64))),
        operations,
        AtomicGenerationOverlay::AtomicFixed32,
        filter,
        writer_hash_builder.clone(),
    )
    .unwrap();
    let mixed_reads = match workload {
        Workload::Read95Hit => Some(read_trace(operations, 95, keys, misses)),
        Workload::Read97Hit => Some(read_trace(operations, 97, keys, misses)),
        Workload::Read98Hit => Some(read_trace(operations, 98, keys, misses)),
        Workload::Read99Hit => Some(read_trace(operations, 99, keys, misses)),
        _ => None,
    };
    for key in keys {
        black_box(map.get(key));
    }
    for key in misses.iter().take(keys.len()) {
        black_box(map.get(key));
    }
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    let elapsed = std::thread::scope(|scope| {
        for thread in 0..threads {
            let map = &map;
            let start = &start;
            let done = &done;
            let mixed_reads = &mixed_reads;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    match workload {
                        Workload::ReadHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.get(key).unwrap());
                        }
                        Workload::ReadMiss => {
                            black_box(map.get(&misses[operation]));
                        }
                        Workload::Read95Hit
                        | Workload::Read97Hit
                        | Workload::Read98Hit
                        | Workload::Read99Hit => {
                            black_box(map.get(mixed_reads.as_ref().unwrap()[operation]));
                        }
                        Workload::InsertHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.insert(key, value(operation as u64)));
                        }
                        Workload::InsertMiss => {
                            black_box(map.insert(&misses[operation], value(operation as u64)));
                        }
                        Workload::UpdateHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            map.update(key, increment).unwrap();
                        }
                        Workload::UpdateMiss => {
                            assert_eq!(map.update(&misses[operation], increment), None);
                        }
                        Workload::UpdateHot => {
                            map.update(&keys[0], increment).unwrap();
                        }
                        Workload::UpsertHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.upsert(key, value(operation as u64), increment));
                        }
                        Workload::UpsertMiss => {
                            black_box(map.upsert(
                                &misses[operation],
                                value(operation as u64),
                                increment,
                            ));
                        }
                        Workload::DeleteHit => {
                            black_box(map.remove(&keys[operation]).unwrap());
                        }
                        Workload::DeleteMiss => {
                            assert_eq!(map.remove(&misses[operation]), None);
                        }
                    }
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

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadMiss,
    Read95Hit,
    Read97Hit,
    Read98Hit,
    Read99Hit,
    InsertHit,
    InsertMiss,
    UpdateHit,
    UpdateMiss,
    UpdateHot,
    UpsertHit,
    UpsertMiss,
    DeleteHit,
    DeleteMiss,
}

impl Workload {
    const ALL: [Self; 15] = [
        Self::ReadHit,
        Self::ReadMiss,
        Self::Read95Hit,
        Self::Read97Hit,
        Self::Read98Hit,
        Self::Read99Hit,
        Self::InsertHit,
        Self::InsertMiss,
        Self::UpdateHit,
        Self::UpdateMiss,
        Self::UpdateHot,
        Self::UpsertHit,
        Self::UpsertMiss,
        Self::DeleteHit,
        Self::DeleteMiss,
    ];

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

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadMiss => "read_miss",
            Self::Read95Hit => "read_95_hit",
            Self::Read97Hit => "read_97_hit",
            Self::Read98Hit => "read_98_hit",
            Self::Read99Hit => "read_99_hit",
            Self::InsertHit => "insert_hit",
            Self::InsertMiss => "insert_miss",
            Self::UpdateHit => "update_hit",
            Self::UpdateMiss => "update_miss",
            Self::UpdateHot => "update_hot_key",
            Self::UpsertHit => "upsert_hit",
            Self::UpsertMiss => "upsert_miss",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
        }
    }
}

const fn filter_name(filter: AtomicGenerationBaseFilter) -> &'static str {
    match filter {
        AtomicGenerationBaseFilter::Disabled => "disabled",
        AtomicGenerationBaseFilter::EmbeddedFingerprint => "embedded-fingerprint",
        AtomicGenerationBaseFilter::OneBytePerEntry => "one-byte",
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn value(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("probe values remain representable")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    NonMaxU64::new(value.get() + 1).expect("probe values remain representable")
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
