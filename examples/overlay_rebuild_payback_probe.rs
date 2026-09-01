//! Measures whether compacting a grown cache overlay earns back its cost.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use packedgen::{CacheConfig, DirectPackedCache};

const VALUE_BYTES: usize = 64;

#[derive(Clone, Copy)]
enum Distribution {
    Uniform,
    Hot,
}

impl Distribution {
    const ALL: [Self; 2] = [Self::Uniform, Self::Hot];

    const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Hot => "hot_90pct_on_1pct",
        }
    }

    fn index(self, operation: usize, entries: usize) -> usize {
        match self {
            Self::Uniform => mixed_index(operation, entries),
            Self::Hot => {
                let hot = entries
                    .div_ceil(100)
                    .clamp(1, entries.saturating_sub(1).max(1));
                if mix(operation as u64 ^ 0x1ae9_523f_184c_7b21).is_multiple_of(10) {
                    hot + mixed_index(operation ^ 0x6d5a_56da, entries - hot)
                } else {
                    mixed_index(operation, hot)
                }
            }
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let base_entries = argument(&mut arguments, 200_000).max(1);
    let growth_entries = argument(&mut arguments, 50_000).max(1);
    let operations = argument(&mut arguments, 10_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 7).max(3);
    let total_entries = base_entries + growth_entries;
    let keys = Arc::new(
        (0..total_entries)
            .map(mixed_binary_key)
            .collect::<Vec<Box<[u8]>>>(),
    );

    println!(
        "distribution,base_entries,growth_entries,operations,threads,samples,before_mops,after_mops,read_gain_pct,rebuild_ms,break_even_million_reads,rebuilt"
    );
    for distribution in Distribution::ALL {
        let mut before_results = Vec::with_capacity(samples);
        let mut after_results = Vec::with_capacity(samples);
        let mut rebuild_results = Vec::with_capacity(samples);
        let mut rebuilt_all = true;
        let warmup_operations = operations.min(1_000_000);
        for _ in 0..samples {
            let cache = Arc::new(build_grown_cache(base_entries, growth_entries, &keys));
            black_box(run_reads(
                &cache,
                &keys,
                warmup_operations,
                threads,
                distribution,
            ));
            let before = run_reads(&cache, &keys, operations, threads, distribution);
            let rebuild_started = Instant::now();
            let rebuilt = cache.maintain().unwrap().rebuilt;
            let rebuild_seconds = rebuild_started.elapsed().as_secs_f64();
            black_box(run_reads(
                &cache,
                &keys,
                warmup_operations,
                threads,
                distribution,
            ));
            let after = run_reads(&cache, &keys, operations, threads, distribution);
            before_results.push(before);
            after_results.push(after);
            rebuild_results.push(rebuild_seconds);
            rebuilt_all &= rebuilt;
        }
        before_results.sort_by(f64::total_cmp);
        after_results.sort_by(f64::total_cmp);
        rebuild_results.sort_by(f64::total_cmp);
        let before = before_results[before_results.len() / 2];
        let after = after_results[after_results.len() / 2];
        let rebuild_seconds = rebuild_results[rebuild_results.len() / 2];
        let saved_seconds_per_operation = 1.0 / (after * 1e6) - 1.0 / (before * 1e6);
        let break_even = if saved_seconds_per_operation < 0.0 {
            rebuild_seconds / -saved_seconds_per_operation / 1e6
        } else {
            f64::INFINITY
        };
        println!(
            "{},{base_entries},{growth_entries},{operations},{threads},{samples},{before:.3},{after:.3},{:.3},{:.3},{break_even:.3},{rebuilt_all}",
            distribution.name(),
            (after / before - 1.0) * 100.0,
            rebuild_seconds * 1e3,
        );
    }
}

fn build_grown_cache(
    base_entries: usize,
    growth_entries: usize,
    keys: &[Box<[u8]>],
) -> DirectPackedCache<[u8; VALUE_BYTES]> {
    // Keep the generation budget below post-build growth so the second
    // maintenance call deterministically has useful packing work to do.
    let overlay_capacity = growth_entries.max(4_096);
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(base_entries + growth_entries + 1)
            .with_overlay_capacity(overlay_capacity),
    )
    .unwrap();
    for (index, key) in keys.iter().take(base_entries).enumerate() {
        cache
            .insert_discard_with_options(key, value(index), charge(key), None)
            .unwrap();
    }
    assert!(cache.maintain().unwrap().rebuilt, "initial base was packed");
    for (index, key) in keys.iter().enumerate().skip(base_entries) {
        cache
            .insert_discard_with_options(key, value(index), charge(key), None)
            .unwrap();
    }
    assert_eq!(cache.len(), base_entries + growth_entries);
    cache
}

fn run_reads(
    cache: &Arc<DirectPackedCache<[u8; VALUE_BYTES]>>,
    keys: &Arc<Vec<Box<[u8]>>>,
    operations: usize,
    threads: usize,
    distribution: Distribution,
) -> f64 {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let checksum = Arc::new(AtomicU64::new(0));
    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            let cache = Arc::clone(cache);
            let keys = Arc::clone(keys);
            let barrier = Arc::clone(&barrier);
            let checksum = Arc::clone(&checksum);
            workers.push(scope.spawn(move || {
                let guard = cache.pin();
                barrier.wait();
                let mut local = 0_u64;
                for operation in begin..end {
                    let key = &keys[distribution.index(operation, keys.len())];
                    local += guard
                        .get_untracked(key)
                        .map_or(0, |value| u64::from(value[0]));
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
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

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn charge(key: &[u8]) -> u64 {
    VALUE_BYTES as u64 + key.len() as u64
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}

fn mixed_binary_key(index: usize) -> Box<[u8]> {
    let mut key = [0_u8; 48];
    let mut state = index as u64;
    for chunk in key.as_chunks_mut::<8>().0 {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key.into()
}

fn mixed_index(operation: usize, len: usize) -> usize {
    let len = u64::try_from(len).expect("benchmark key count fits u64");
    usize::try_from(mix(operation as u64) % len).expect("mixed index fits usize")
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
