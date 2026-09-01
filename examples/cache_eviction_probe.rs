//! Alternating full-cache admission probe with a manual-victim Papaya bound.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use hashbrown::DefaultHashBuilder;
use packedgen::{CacheConfig, DirectPackedCache};
use papaya::HashMap as PapayaHashMap;

#[derive(Clone, Copy)]
enum AdmissionMode {
    Scalar,
    Batch,
    PapayaManualVictim,
}

impl AdmissionMode {
    const fn name(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Batch => "batch",
            Self::PapayaManualVictim => "papaya-manual-victim",
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 20_000).max(1);
    let insertions = argument(&mut arguments, 2_000).max(1);
    let samples = argument(&mut arguments, 11).max(3);
    let eviction_batch = argument(&mut arguments, 64).max(1);
    let admission_batch = argument(&mut arguments, 32).max(1);
    let threads = argument(&mut arguments, 1).clamp(1, entries);
    let key_bytes = argument(&mut arguments, 16).max(1);
    let keys = (0..entries + insertions)
        .map(|index| binary_key(index, key_bytes))
        .collect::<Vec<_>>();
    let mut results = [
        Vec::with_capacity(samples),
        Vec::with_capacity(samples),
        Vec::with_capacity(samples),
    ];
    let mut original_survivors = [
        Vec::with_capacity(samples),
        Vec::with_capacity(samples),
        Vec::with_capacity(samples),
    ];

    for sample in 0..samples {
        let all_modes = [
            AdmissionMode::Scalar,
            AdmissionMode::Batch,
            AdmissionMode::PapayaManualVictim,
        ];
        for offset in 0..all_modes.len() {
            let index = (sample + offset) % all_modes.len();
            let mode = all_modes[index];
            let (mops, survivors) = match mode {
                AdmissionMode::Scalar | AdmissionMode::Batch => measure_direct(
                    mode,
                    &keys,
                    entries,
                    insertions,
                    eviction_batch,
                    admission_batch,
                    threads,
                ),
                AdmissionMode::PapayaManualVictim => {
                    measure_papaya(&keys, entries, insertions, threads)
                }
            };
            results[index].push(mops);
            original_survivors[index].push(survivors);
            if std::env::var_os("PACKED_CACHE_RAW").is_some() {
                eprintln!("raw,{sample},{},{mops}", mode.name());
            }
        }
    }

    println!(
        "mode,entries,insertions,threads,key_bytes,samples,eviction_batch,admission_batch,median_mops,p05_mops,median_vs_scalar,median_original_survivors"
    );
    for (index, mode) in [
        AdmissionMode::Scalar,
        AdmissionMode::Batch,
        AdmissionMode::PapayaManualVictim,
    ]
    .into_iter()
    .enumerate()
    {
        let mut versus_scalar = results[index]
            .iter()
            .zip(&results[0])
            .map(|(candidate, scalar)| candidate / scalar)
            .collect::<Vec<_>>();
        versus_scalar.sort_by(f64::total_cmp);
        let median_versus_scalar = versus_scalar[versus_scalar.len() / 2];
        results[index].sort_by(f64::total_cmp);
        original_survivors[index].sort_unstable();
        let median = results[index][results[index].len() / 2];
        let p05 = results[index][results[index].len() * 5 / 100];
        let median_original_survivors =
            original_survivors[index][original_survivors[index].len() / 2];
        println!(
            "{},{entries},{insertions},{threads},{key_bytes},{samples},{eviction_batch},{admission_batch},{median:.3},{p05:.3},{median_versus_scalar:.3},{median_original_survivors}",
            mode.name()
        );
    }
}

fn measure_direct(
    mode: AdmissionMode,
    keys: &[Box<[u8]>],
    entries: usize,
    insertions: usize,
    eviction_batch: usize,
    admission_batch: usize,
    threads: usize,
) -> (f64, usize) {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(entries)
            .with_overlay_capacity(entries + insertions)
            .with_eviction_batch(eviction_batch),
    )
    .unwrap();
    for (entry, key) in keys.iter().take(entries).enumerate() {
        cache
            .insert_discard_with_options(key, entry as u64, 8, None)
            .unwrap();
    }
    cache.maintain().unwrap();

    let barrier = Arc::new(Barrier::new(threads + 1));
    let elapsed = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = insertions * thread / threads;
            let end = insertions * (thread + 1) / threads;
            let barrier = Arc::clone(&barrier);
            let cache = &cache;
            workers.push(scope.spawn(move || {
                let mut guard = cache.pin();
                barrier.wait();
                match mode {
                    AdmissionMode::Scalar => {
                        for entry in begin..end {
                            guard
                                .insert_if_absent_with_options(
                                    &keys[entries + entry],
                                    (entries + entry) as u64,
                                    8,
                                    None,
                                )
                                .unwrap();
                        }
                    }
                    AdmissionMode::Batch => {
                        for chunk_begin in (begin..end).step_by(admission_batch) {
                            let mut batch = guard.admission_batch();
                            for entry in chunk_begin..(chunk_begin + admission_batch).min(end) {
                                batch
                                    .insert_if_absent_with_options(
                                        &keys[entries + entry],
                                        (entries + entry) as u64,
                                        8,
                                        None,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                    AdmissionMode::PapayaManualVictim => unreachable!(),
                }
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        started.elapsed()
    });
    assert!(cache.len() <= entries);
    black_box(cache.len());
    let survivors = keys
        .iter()
        .take(entries)
        .filter(|key| cache.peek(key).is_some())
        .count();
    (insertions as f64 / elapsed.as_secs_f64() / 1e6, survivors)
}

fn measure_papaya(
    keys: &[Box<[u8]>],
    entries: usize,
    insertions: usize,
    threads: usize,
) -> (f64, usize) {
    let cache = PapayaHashMap::with_capacity_and_hasher(entries, DefaultHashBuilder::default());
    let guard = cache.pin();
    for (entry, key) in keys.iter().take(entries).enumerate() {
        guard.insert(key.clone(), entry as u64);
    }
    drop(guard);

    let barrier = Arc::new(Barrier::new(threads + 1));
    let elapsed = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = insertions * thread / threads;
            let end = insertions * (thread + 1) / threads;
            let resident_begin = entries * thread / threads;
            let resident_end = entries * (thread + 1) / threads;
            let mut resident = keys[resident_begin..resident_end].to_vec();
            let barrier = Arc::clone(&barrier);
            let cache = &cache;
            workers.push(scope.spawn(move || {
                let guard = cache.pin();
                barrier.wait();
                for entry in begin..end {
                    let slot = (entry - begin) % resident.len();
                    let key = &keys[entries + entry];
                    let victim = std::mem::replace(&mut resident[slot], key.clone());
                    guard.remove(&victim);
                    guard.insert(key.clone(), (entries + entry) as u64);
                }
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        started.elapsed()
    });
    let guard = cache.pin();
    assert_eq!(guard.len(), entries);
    black_box(guard.len());
    let survivors = keys
        .iter()
        .take(entries)
        .filter(|key| guard.get(key.as_ref()).is_some())
        .count();
    (insertions as f64 / elapsed.as_secs_f64() / 1e6, survivors)
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}

fn binary_key(index: usize, key_bytes: usize) -> Box<[u8]> {
    let mut state = index as u64;
    let mut key = vec![0_u8; key_bytes];
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    key.into()
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
