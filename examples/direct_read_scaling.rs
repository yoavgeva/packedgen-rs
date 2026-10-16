//! Resident-read scaling of the direct cache's read APIs.
//!
//! Usage: `direct_read_scaling ENTRIES OPERATIONS_PER_THREAD THREADS...`.
//! Every key is resident, so each read is a hit. Modes: `get` and `peek`
//! return an owned `DirectCacheValue`, `guard` reads through a pinned guard,
//! and `cloned` uses `get_cloned`. One CSV row is printed per mode and thread
//! count; higher Mops/s is better.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

use packedgen::{CacheConfig, DirectPackedCache};

const MODES: [&str; 4] = ["get", "peek", "guard", "cloned"];

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn read_all(cache: &DirectPackedCache<u64>, mode: &str, keys: &[u64]) -> u64 {
    let mut checksum = 0_u64;
    match mode {
        "get" => {
            for &key in keys {
                checksum = checksum.wrapping_add(*cache.get(&key.to_le_bytes()).expect("resident"));
            }
        }
        "peek" => {
            for &key in keys {
                checksum =
                    checksum.wrapping_add(*cache.peek(&key.to_le_bytes()).expect("resident"));
            }
        }
        "guard" => {
            let mut guard = cache.pin();
            for (position, &key) in keys.iter().enumerate() {
                checksum = checksum.wrapping_add(*guard.get(&key.to_le_bytes()).expect("resident"));
                if position % 512 == 511 {
                    guard.quiesce();
                }
            }
        }
        "cloned" => {
            for &key in keys {
                checksum =
                    checksum.wrapping_add(cache.get_cloned(&key.to_le_bytes()).expect("resident"));
            }
        }
        _ => panic!("unknown mode: {mode}"),
    }
    checksum
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        arguments.len() >= 3,
        "usage: direct_read_scaling ENTRIES OPERATIONS_PER_THREAD THREADS..."
    );
    let entries: u64 = arguments[0].parse().expect("ENTRIES is an integer");
    let operations: u64 = arguments[1].parse().expect("OPERATIONS is an integer");
    let thread_counts: Vec<usize> = arguments[2..]
        .iter()
        .map(|threads| threads.parse().expect("THREADS are integers"))
        .collect();
    let capacity = usize::try_from(entries * 2).expect("capacity fits usize");
    let cache = DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_max_entries(capacity))
        .expect("cache builds");
    for key in 0..entries {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), key, 16, None)
            .expect("seed inserted");
    }
    cache.maintain().expect("seed maintenance succeeds");

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
                        let cache = &cache;
                        let start = &start;
                        scope.spawn(move || {
                            start.wait();
                            read_all(cache, mode, trace)
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
