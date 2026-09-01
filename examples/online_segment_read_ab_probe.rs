//! Same-layout paired read probe for the exclusive and online segment wrappers.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use packedgen::{
    FrozenSegmentCache, GenerationHashBuilder, MutableSegmentCache, OnlineMutableSegmentCache,
    SegmentCacheConfig,
};
use papaya::HashMap as PapayaHashMap;

const VALUE_BYTES: usize = 64;
const TTL: Duration = Duration::from_secs(3_600);

struct PapayaValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: AtomicU32,
    accessed: AtomicBool,
}

type PapayaCache = PapayaHashMap<Box<[u8]>, PapayaValue, DefaultHashBuilder>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 20_000_000).max(1);
    let trials = argument(&mut arguments, 7).max(1);
    let threads = argument(&mut arguments, 16).max(1);
    let delta_capacity = argument(&mut arguments, entries.div_ceil(10).max(1_024)).max(1);

    let keys = (0..entries).map(mixed_key).collect::<Vec<_>>();
    let config = SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1));
    let hash_builder = GenerationHashBuilder::default();
    let exclusive = MutableSegmentCache::try_from_frozen(
        FrozenSegmentCache::try_from_entries_with_hash_builder(
            config,
            keys.iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index), Some(TTL))),
            hash_builder.clone(),
        )
        .unwrap(),
        delta_capacity,
    )
    .unwrap();
    let online = OnlineMutableSegmentCache::try_from_frozen(
        FrozenSegmentCache::try_from_entries_with_hash_builder(
            config,
            keys.iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index), Some(TTL))),
            hash_builder,
        )
        .unwrap(),
        delta_capacity,
    )
    .unwrap();
    let papaya = PapayaCache::with_capacity_and_hasher(entries, DefaultHashBuilder::default());
    {
        let guard = papaya.pin();
        for (index, key) in keys.iter().enumerate() {
            guard.insert(key.clone(), papaya_value(index, key));
        }
    }

    let mut exclusive_seconds = Vec::with_capacity(trials);
    let mut online_seconds = Vec::with_capacity(trials);
    let mut papaya_seconds = Vec::with_capacity(trials);
    for trial in 0..trials {
        match trial % 3 {
            0 => {
                online_seconds.push(measure_online(&online, &keys, operations, threads));
                exclusive_seconds.push(measure_exclusive(&exclusive, &keys, operations, threads));
                papaya_seconds.push(measure_papaya(&papaya, &keys, operations, threads));
            }
            1 => {
                papaya_seconds.push(measure_papaya(&papaya, &keys, operations, threads));
                online_seconds.push(measure_online(&online, &keys, operations, threads));
                exclusive_seconds.push(measure_exclusive(&exclusive, &keys, operations, threads));
            }
            _ => {
                exclusive_seconds.push(measure_exclusive(&exclusive, &keys, operations, threads));
                papaya_seconds.push(measure_papaya(&papaya, &keys, operations, threads));
                online_seconds.push(measure_online(&online, &keys, operations, threads));
            }
        }
    }

    let exclusive_mops = median_mops(&mut exclusive_seconds, operations);
    let online_mops = median_mops(&mut online_seconds, operations);
    let papaya_mops = median_mops(&mut papaya_seconds, operations);
    println!(
        "entries,operations,trials,threads,exclusive_mops,online_mops,papaya_mops,\
         online_vs_exclusive_pct,online_vs_papaya_pct"
    );
    println!(
        "{entries},{operations},{trials},{threads},{exclusive_mops:.3},{online_mops:.3},\
         {papaya_mops:.3},{:.2},{:.2}",
        (online_mops / exclusive_mops - 1.0) * 100.0,
        (online_mops / papaya_mops - 1.0) * 100.0,
    );
}

fn measure_exclusive(
    cache: &MutableSegmentCache,
    keys: &[Box<[u8]>],
    operations: usize,
    threads: usize,
) -> f64 {
    let started = Instant::now();
    let checksum = thread::scope(|scope| {
        let workers = (0..threads)
            .map(|worker| {
                scope.spawn(move || {
                    let guard = cache.pin();
                    let begin = operations.saturating_mul(worker) / threads;
                    let end = operations.saturating_mul(worker + 1) / threads;
                    let mut checksum = 0_u64;
                    for operation in begin..end {
                        let index = mixed_index(operation, keys.len());
                        checksum = checksum.wrapping_add(
                            guard
                                .peek(&keys[index])
                                .map_or(0, |value| u64::from(value[0])),
                        );
                    }
                    checksum
                })
            })
            .collect::<Vec<_>>();
        workers.into_iter().fold(0_u64, |sum, worker| {
            sum.wrapping_add(worker.join().unwrap())
        })
    });
    let elapsed = started.elapsed().as_secs_f64();
    black_box(checksum);
    elapsed
}

fn measure_online(
    cache: &OnlineMutableSegmentCache,
    keys: &[Box<[u8]>],
    operations: usize,
    threads: usize,
) -> f64 {
    let started = Instant::now();
    let checksum = thread::scope(|scope| {
        let workers = (0..threads)
            .map(|worker| {
                scope.spawn(move || {
                    let guard = cache.pin();
                    let begin = operations.saturating_mul(worker) / threads;
                    let end = operations.saturating_mul(worker + 1) / threads;
                    let mut checksum = 0_u64;
                    for operation in begin..end {
                        let index = mixed_index(operation, keys.len());
                        checksum = checksum.wrapping_add(
                            guard
                                .peek(&keys[index])
                                .map_or(0, |value| u64::from(value[0])),
                        );
                    }
                    checksum
                })
            })
            .collect::<Vec<_>>();
        workers.into_iter().fold(0_u64, |sum, worker| {
            sum.wrapping_add(worker.join().unwrap())
        })
    });
    let elapsed = started.elapsed().as_secs_f64();
    black_box(checksum);
    elapsed
}

fn measure_papaya(
    cache: &PapayaCache,
    keys: &[Box<[u8]>],
    operations: usize,
    threads: usize,
) -> f64 {
    let started = Instant::now();
    let checksum = thread::scope(|scope| {
        let workers = (0..threads)
            .map(|worker| {
                scope.spawn(move || {
                    let guard = cache.pin();
                    let begin = operations.saturating_mul(worker) / threads;
                    let end = operations.saturating_mul(worker + 1) / threads;
                    let mut checksum = 0_u64;
                    for operation in begin..end {
                        let index = mixed_index(operation, keys.len());
                        checksum = checksum
                            .wrapping_add(guard.get(&keys[index]).map_or(0, papaya_first_byte));
                    }
                    checksum
                })
            })
            .collect::<Vec<_>>();
        workers.into_iter().fold(0_u64, |sum, worker| {
            sum.wrapping_add(worker.join().unwrap())
        })
    });
    let elapsed = started.elapsed().as_secs_f64();
    black_box(checksum);
    elapsed
}

fn median_mops(samples: &mut [f64], operations: usize) -> f64 {
    samples.sort_by(f64::total_cmp);
    operations as f64 / samples[samples.len() / 2] / 1_000_000.0
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse::<usize>().expect("argument must be an integer")
    })
}

fn mixed_key(index: usize) -> Box<[u8]> {
    let hash = mix(u64::try_from(index).unwrap_or(u64::MAX));
    let length = 8 + index % 17;
    let mut key = Vec::with_capacity(length);
    while key.len() < length {
        key.extend_from_slice(&hash.to_le_bytes());
    }
    key.truncate(length);
    key.into_boxed_slice()
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn papaya_value(index: usize, key: &[u8]) -> PapayaValue {
    PapayaValue {
        value: value(index),
        weight: u32::try_from(VALUE_BYTES + key.len()).unwrap_or(u32::MAX),
        expires_at: AtomicU32::new(u32::MAX),
        accessed: AtomicBool::new(false),
    }
}

fn papaya_first_byte(value: &PapayaValue) -> u64 {
    black_box(value.weight);
    black_box(value.expires_at.load(Ordering::Relaxed));
    black_box(value.accessed.load(Ordering::Relaxed));
    u64::from(value.value[0])
}

fn mixed_index(operation: usize, modulus: usize) -> usize {
    usize::try_from(
        mix(u64::try_from(operation).unwrap_or(u64::MAX)) % u64::try_from(modulus).unwrap(),
    )
    .unwrap()
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
