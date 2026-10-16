//! Resident-read scaling of the caches' read APIs.
//!
//! Usage: `cache_read_scaling ENTRIES OPERATIONS_PER_THREAD THREADS...`.
//! Every key is resident, so each read is a hit. Direct-cache modes: `get` and
//! `peek` return a `DirectCacheValue`, `guard` reads through a pinned guard,
//! and `cloned` uses `get_cloned`. `packed-get`, `packed-peek`, and
//! `packed-guard` are the same reads on `PackedCache`. One CSV row is printed
//! per mode and thread count; higher Mops/s is better.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

use packedgen::{CacheConfig, DirectPackedCache, PackedCache};

const MODES: [&str; 7] = [
    "get",
    "peek",
    "guard",
    "cloned",
    "packed-get",
    "packed-peek",
    "packed-guard",
];

struct Caches {
    direct: DirectPackedCache<u64>,
    packed: PackedCache<u64>,
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn read_all(caches: &Caches, mode: &str, keys: &[u64]) -> u64 {
    // Chosen once per run so every mode pays the same indirect call per read.
    let read: fn(&Caches, &[u8; 8]) -> u64 = match mode {
        "get" => |caches, key| *caches.direct.get(key).expect("resident"),
        "peek" => |caches, key| *caches.direct.peek(key).expect("resident"),
        "cloned" => |caches, key| caches.direct.get_cloned(key).expect("resident"),
        "packed-get" => |caches, key| *caches.packed.get(key).expect("resident"),
        "packed-peek" => |caches, key| *caches.packed.peek(key).expect("resident"),
        "guard" | "packed-guard" => return read_guarded(caches, mode, keys),
        _ => panic!("unknown mode: {mode}"),
    };
    keys.iter().fold(0_u64, |checksum, &key| {
        checksum.wrapping_add(read(caches, &key.to_le_bytes()))
    })
}

fn read_guarded(caches: &Caches, mode: &str, keys: &[u64]) -> u64 {
    let mut checksum = 0_u64;
    if mode == "guard" {
        let mut guard = caches.direct.pin();
        for (position, &key) in keys.iter().enumerate() {
            checksum = checksum.wrapping_add(*guard.get(&key.to_le_bytes()).expect("resident"));
            if position % 512 == 511 {
                guard.quiesce();
            }
        }
    } else {
        let mut guard = caches.packed.pin();
        for (position, &key) in keys.iter().enumerate() {
            checksum = checksum.wrapping_add(*guard.get(&key.to_le_bytes()).expect("resident"));
            if position % 512 == 511 {
                guard.refresh();
            }
        }
    }
    checksum
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        arguments.len() >= 3,
        "usage: cache_read_scaling ENTRIES OPERATIONS_PER_THREAD THREADS..."
    );
    let entries: u64 = arguments[0].parse().expect("ENTRIES is an integer");
    let operations: u64 = arguments[1].parse().expect("OPERATIONS is an integer");
    let thread_counts: Vec<usize> = arguments[2..]
        .iter()
        .map(|threads| threads.parse().expect("THREADS are integers"))
        .collect();
    let capacity = usize::try_from(entries * 2).expect("capacity fits usize");
    let config = CacheConfig::new(u64::MAX).with_max_entries(capacity);
    let caches = Caches {
        direct: DirectPackedCache::try_new(config.clone()).expect("cache builds"),
        packed: PackedCache::try_new(config).expect("cache builds"),
    };
    for key in 0..entries {
        caches
            .direct
            .insert_discard_with_options(&key.to_le_bytes(), key, 16, None)
            .expect("seed inserted");
        caches
            .packed
            .insert_discard_with_options(&key.to_le_bytes(), key, 16, None)
            .expect("seed inserted");
    }
    caches.direct.maintain().expect("seed maintenance succeeds");
    caches.packed.maintain().expect("seed maintenance succeeds");

    println!("mode,threads,entries,operations_per_thread,mops,checksum");
    for &threads in &thread_counts {
        for mode in MODES {
            let traces: Vec<Vec<u64>> = (0..threads as u64)
                .map(|thread| {
                    (0..operations)
                        .map(|operation| mix((thread << 40) ^ operation) % entries)
                        .collect()
                })
                .collect();
            let start = Barrier::new(threads + 1);
            let (elapsed, checksum) = thread::scope(|scope| {
                let workers: Vec<_> = traces
                    .iter()
                    .map(|trace| {
                        let caches = &caches;
                        let start = &start;
                        scope.spawn(move || {
                            start.wait();
                            read_all(caches, mode, trace)
                        })
                    })
                    .collect();
                start.wait();
                let started = Instant::now();
                let checksum = workers
                    .into_iter()
                    .map(|worker| worker.join().expect("reader completed"))
                    .fold(0_u64, u64::wrapping_add);
                (started.elapsed(), checksum)
            });
            let total = (operations * threads as u64) as f64;
            println!(
                "{mode},{threads},{entries},{operations},{:.3},{}",
                total / elapsed.as_secs_f64() / 1e6,
                black_box(checksum)
            );
        }
    }
}
