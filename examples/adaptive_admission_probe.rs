//! Existing, alternating, and sustained-new conditional-admission comparison.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use hashbrown::DefaultHashBuilder;
use packedgen::{CacheAdmissionOutcome, CacheConfig, DirectPackedCache};
use papaya::HashMap as PapayaHashMap;

const VALUE_BYTES: usize = 64;

#[allow(dead_code)]
struct ControlValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: AtomicU32,
    accessed: AtomicBool,
}

type PapayaCache = PapayaHashMap<Box<[u8]>, ControlValue, DefaultHashBuilder>;

#[derive(Clone, Copy)]
enum Strategy {
    DirectOrdinary,
    DirectAdaptive,
    DirectAdaptiveLazy,
    PapayaEager,
    PapayaLazy,
}

#[derive(Clone, Copy)]
enum Pattern {
    Existing,
    Alternating,
    New,
}

impl Pattern {
    fn named(name: &str) -> Self {
        match name {
            "existing" => Self::Existing,
            "alternating" => Self::Alternating,
            "new" => Self::New,
            _ => panic!("expected existing, alternating, or new"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Existing => "existing",
            Self::Alternating => "alternating",
            Self::New => "new",
        }
    }
}

impl Strategy {
    const ALL: [Self; 5] = [
        Self::DirectOrdinary,
        Self::DirectAdaptive,
        Self::DirectAdaptiveLazy,
        Self::PapayaEager,
        Self::PapayaLazy,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::DirectOrdinary => "direct-ordinary",
            Self::DirectAdaptive => "direct-adaptive",
            Self::DirectAdaptiveLazy => "direct-adaptive-lazy",
            Self::PapayaEager => "papaya-try-insert",
            Self::PapayaLazy => "papaya-try-insert-with",
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let operations = argument(&mut arguments, 2_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    let strategy_filter = arguments.next();
    let pattern = Pattern::named(&arguments.next().unwrap_or_else(|| "existing".to_owned()));
    let keys = Arc::new(
        (0..entries)
            .map(mixed_binary_key)
            .collect::<Vec<Box<[u8]>>>(),
    );
    let miss_count = match pattern {
        Pattern::Existing => 0,
        Pattern::Alternating => operations / 2,
        Pattern::New => operations,
    };
    let misses = Arc::new(
        (0..miss_count)
            .map(|index| mixed_binary_key(entries + 1_000_000 + index))
            .collect::<Vec<_>>(),
    );

    println!(
        "strategy,pattern,entries,operations,threads,samples,median_mops,p05_mops,p95_time_over_median_pct"
    );
    let strategies = Strategy::ALL
        .into_iter()
        .filter(|strategy| {
            strategy_filter
                .as_deref()
                .is_none_or(|filter| filter.split(',').any(|name| strategy.name() == name))
        })
        .collect::<Vec<_>>();
    assert!(!strategies.is_empty(), "unknown strategy filter");
    let mut measurements = vec![Vec::with_capacity(samples); strategies.len()];
    for sample in 0..samples {
        for offset in 0..strategies.len() {
            let strategy_index = (sample + offset) % strategies.len();
            measurements[strategy_index].push(measure(
                strategies[strategy_index],
                entries,
                operations,
                threads,
                Arc::clone(&keys),
                Arc::clone(&misses),
                pattern,
            ));
        }
    }
    for (strategy, mut measured) in strategies.into_iter().zip(measurements) {
        measured.sort_by(f64::total_cmp);
        let median = measured[measured.len() / 2];
        let p05 = measured[measured.len() / 20];
        println!(
            "{},{},{entries},{operations},{threads},{samples},{median:.3},{p05:.3},{:.3}",
            strategy.name(),
            pattern.name(),
            (median / p05 - 1.0) * 100.0,
        );
    }
}

#[allow(clippy::too_many_lines)]
fn measure(
    strategy: Strategy,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: Arc<Vec<Box<[u8]>>>,
    misses: Arc<Vec<Box<[u8]>>>,
    pattern: Pattern,
) -> f64 {
    match strategy {
        Strategy::DirectOrdinary | Strategy::DirectAdaptive | Strategy::DirectAdaptiveLazy => {
            let cache = Arc::new(
                DirectPackedCache::try_new(
                    CacheConfig::new(u64::MAX)
                        .with_max_entries(entries + misses.len() + 1)
                        .with_overlay_capacity(entries + misses.len()),
                )
                .unwrap(),
            );
            for (index, key) in keys.iter().enumerate() {
                cache
                    .insert_discard_with_options(key, value(index), charge(key), None)
                    .unwrap();
            }
            cache.maintain().unwrap();
            run_workers(operations, threads, move |begin, end, checksum| {
                let cache = Arc::clone(&cache);
                let keys = Arc::clone(&keys);
                let misses = Arc::clone(&misses);
                move || {
                    let guard = cache.pin();
                    let mut adaptive = guard.adaptive_admission();
                    let mut local = 0_u64;
                    for operation in begin..end {
                        let key = match pattern {
                            Pattern::Alternating if !operation.is_multiple_of(2) => {
                                &misses[operation / 2]
                            }
                            Pattern::Existing | Pattern::Alternating => {
                                &keys[mixed_index(operation, keys.len())]
                            }
                            Pattern::New => &misses[operation],
                        };
                        let outcome = match strategy {
                            Strategy::DirectOrdinary => guard.insert_if_absent_with_options(
                                key,
                                value(operation),
                                charge(key),
                                None,
                            ),
                            Strategy::DirectAdaptive => adaptive.insert_if_absent_with_options(
                                key,
                                value(operation),
                                charge(key),
                                None,
                            ),
                            Strategy::DirectAdaptiveLazy => adaptive
                                .insert_if_absent_with_options_by(
                                    key,
                                    || value(operation),
                                    charge(key),
                                    None,
                                ),
                            Strategy::PapayaEager | Strategy::PapayaLazy => unreachable!(),
                        }
                        .unwrap();
                        local += u64::from(outcome == CacheAdmissionOutcome::Existing);
                    }
                    checksum.fetch_xor(local, Ordering::Relaxed);
                }
            })
        }
        Strategy::PapayaEager | Strategy::PapayaLazy => {
            let cache = Arc::new(PapayaCache::with_capacity_and_hasher(
                entries + misses.len(),
                DefaultHashBuilder::default(),
            ));
            let guard = cache.pin();
            for (index, key) in keys.iter().enumerate() {
                guard.insert(key.clone(), control_value(index, key));
            }
            drop(guard);
            run_workers(operations, threads, move |begin, end, checksum| {
                let cache = Arc::clone(&cache);
                let keys = Arc::clone(&keys);
                let misses = Arc::clone(&misses);
                move || {
                    let guard = cache.pin();
                    let mut local = 0_u64;
                    for operation in begin..end {
                        let key = match pattern {
                            Pattern::Alternating if !operation.is_multiple_of(2) => {
                                &misses[operation / 2]
                            }
                            Pattern::Existing | Pattern::Alternating => {
                                &keys[mixed_index(operation, keys.len())]
                            }
                            Pattern::New => &misses[operation],
                        };
                        let existing = match strategy {
                            Strategy::PapayaEager => guard
                                .try_insert(key.clone(), control_value(operation, key))
                                .is_err(),
                            Strategy::PapayaLazy => guard
                                .try_insert_with(key.clone(), || control_value(operation, key))
                                .is_err(),
                            Strategy::DirectOrdinary
                            | Strategy::DirectAdaptive
                            | Strategy::DirectAdaptiveLazy => unreachable!(),
                        };
                        local += u64::from(existing);
                    }
                    checksum.fetch_xor(local, Ordering::Relaxed);
                }
            })
        }
    }
}

fn run_workers<F, W>(operations: usize, threads: usize, worker: F) -> f64
where
    F: Fn(usize, usize, Arc<AtomicU64>) -> W,
    W: FnOnce() + Send,
{
    let barrier = Arc::new(Barrier::new(threads + 1));
    let checksum = Arc::new(AtomicU64::new(0));
    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            let barrier = Arc::clone(&barrier);
            let work = worker(begin, end, Arc::clone(&checksum));
            workers.push(scope.spawn(move || {
                barrier.wait();
                work();
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        black_box(checksum.load(Ordering::Relaxed));
        operations as f64 / started.elapsed().as_secs_f64() / 1e6
    })
}

fn control_value(index: usize, key: &[u8]) -> ControlValue {
    ControlValue {
        value: value(index),
        weight: u32::try_from(charge(key)).unwrap(),
        expires_at: AtomicU32::new(u32::MAX),
        accessed: AtomicBool::new(false),
    }
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    let mut value = [0_u8; VALUE_BYTES];
    value[..8].copy_from_slice(&(index as u64).to_le_bytes());
    value
}

fn charge(key: &[u8]) -> u64 {
    VALUE_BYTES as u64 + key.len() as u64
}

fn mixed_binary_key(index: usize) -> Box<[u8]> {
    let mut key = [0_u8; 48];
    let mut state = index as u64;
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    let bytes = match index % 100 {
        0..=39 => 8,
        40..=64 => 16,
        65..=79 => 24,
        80..=89 => 32,
        _ => 48,
    };
    key[..bytes].into()
}

fn mixed_index(operation: usize, entries: usize) -> usize {
    usize::try_from(mix(operation as u64) % entries as u64).unwrap()
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}
