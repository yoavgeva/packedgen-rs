#![allow(missing_docs, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use packedgen::{CacheAdmissionOutcome, CacheConfig, CacheWriteOutcome, DirectPackedCache};

const KEY_COUNT: usize = 257;
const TRACE_STEPS: usize = 50_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModelEntry {
    value: u64,
    weight: u64,
}

#[test]
fn direct_cache_matches_reference_model_across_mixed_traces() {
    for seed in [
        0x243f_6a88_85a3_08d3,
        0x1319_8a2e_0370_7344,
        0xa409_3822_299f_31d0,
        0x082e_fa98_ec4e_6c89,
    ] {
        run_differential_trace(seed, TRACE_STEPS);
    }
}

#[test]
fn concurrent_mixed_writes_and_maintenance_preserve_exact_state() {
    run_concurrent_maintenance_trace(8, 128, 20_000);
}

#[test]
#[ignore = "long-running reclamation and rebuild soak; run through scripts/cache_proof.sh --soak"]
fn concurrent_reclamation_and_rebuild_soak() {
    let operations = std::env::var("PACKEDGEN_SOAK_OPERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000_000);
    run_concurrent_maintenance_trace(8, 1_024, operations);
}

fn run_differential_trace(seed: u64, steps: usize) {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(KEY_COUNT * 2)
            .with_overlay_capacity(KEY_COUNT * 2),
    )
    .unwrap();
    let keys = (0..KEY_COUNT).map(binary_key).collect::<Vec<_>>();
    let mut model = HashMap::<Vec<u8>, ModelEntry>::new();
    let mut state = seed;

    for step in 0..steps {
        state = splitmix64(state);
        let key_index = usize::try_from(state % keys.len() as u64).unwrap();
        let key = &keys[key_index];
        let operation = (state >> 32) % 100;
        let value = splitmix64(state ^ step as u64);
        let weight = 1 + (value % 4_096);

        match operation {
            0..=24 => {
                let expected = model.insert(key.clone(), ModelEntry { value, weight });
                let actual = cache
                    .insert_discard_with_options(key, value, weight, None)
                    .unwrap();
                assert_eq!(
                    actual,
                    if expected.is_some() {
                        CacheWriteOutcome::Replaced
                    } else {
                        CacheWriteOutcome::Inserted
                    },
                    "seed={seed:#x} step={step}"
                );
            }
            25..=29 => {
                let expected = model.get_mut(key).is_some_and(|entry| {
                    *entry = ModelEntry { value, weight };
                    true
                });
                assert_eq!(
                    cache
                        .replace_discard_with_options(key, value, weight, None)
                        .unwrap(),
                    expected,
                    "seed={seed:#x} step={step}"
                );
            }
            30..=44 => {
                let expected = if model.contains_key(key) {
                    CacheAdmissionOutcome::Existing
                } else {
                    model.insert(key.clone(), ModelEntry { value, weight });
                    CacheAdmissionOutcome::Inserted
                };
                let actual = cache
                    .insert_if_absent_with_options(key, value, weight, None)
                    .unwrap();
                assert_eq!(actual, expected, "seed={seed:#x} step={step}");
            }
            45..=59 => {
                assert_eq!(
                    cache.get(key).as_deref().copied(),
                    model.get(key).map(|entry| entry.value),
                    "seed={seed:#x} step={step}"
                );
            }
            60..=69 => {
                assert_eq!(
                    cache.peek(key).as_deref().copied(),
                    model.get(key).map(|entry| entry.value),
                    "seed={seed:#x} step={step}"
                );
            }
            70..=79 => {
                assert_eq!(
                    cache.remove(key).as_deref().copied(),
                    model.remove(key).map(|entry| entry.value),
                    "seed={seed:#x} step={step}"
                );
            }
            80..=84 => {
                assert_eq!(
                    cache.remove_discard(key),
                    model.remove(key).is_some(),
                    "seed={seed:#x} step={step}"
                );
            }
            85..=89 => {
                assert_eq!(
                    cache.touch(key, None),
                    model.contains_key(key),
                    "seed={seed:#x} step={step}"
                );
            }
            90..=94 => {
                let existed = model.remove(key).is_some();
                assert_eq!(
                    cache.touch(key, Some(Duration::ZERO)),
                    existed,
                    "seed={seed:#x} step={step}"
                );
                assert!(cache.get(key).is_none(), "seed={seed:#x} step={step}");
            }
            _ => {
                cache.maintain().unwrap();
            }
        }

        assert_eq!(cache.len(), model.len(), "seed={seed:#x} step={step}");
        assert_eq!(
            cache.weight(),
            model.values().map(|entry| entry.weight).sum::<u64>(),
            "seed={seed:#x} step={step}"
        );
    }

    cache.maintain().unwrap();
    for key in &keys {
        assert_eq!(
            cache.peek(key).as_deref().copied(),
            model.get(key).map(|entry| entry.value),
            "seed={seed:#x} final key={key:?}"
        );
    }
}

fn run_concurrent_maintenance_trace(threads: usize, keys_per_thread: usize, operations: usize) {
    let expected_entries = threads * keys_per_thread;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(expected_entries * 2)
                .with_overlay_capacity(expected_entries * 2),
        )
        .unwrap(),
    );
    let start = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    let maintainer = {
        let cache = Arc::clone(&cache);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                cache.maintain().unwrap();
                thread::yield_now();
            }
            cache.maintain().unwrap();
        })
    };

    let mut workers = Vec::with_capacity(threads);
    for worker in 0..threads {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            let keys = (0..keys_per_thread)
                .map(|local| binary_key(worker * keys_per_thread + local))
                .collect::<Vec<_>>();
            start.wait();
            let mut guard = cache.pin();
            for operation in 0..operations {
                let mixed = splitmix64(operation as u64 ^ (worker as u64).rotate_left(19));
                let local = usize::try_from(mixed % keys_per_thread as u64).unwrap();
                let key = &keys[local];
                match operation % 8 {
                    0..=2 => {
                        cache
                            .insert_discard_with_options(
                                key,
                                operation as u64,
                                1 + (operation % 31) as u64,
                                None,
                            )
                            .unwrap();
                    }
                    3 => {
                        cache.remove_discard(key);
                    }
                    4 => {
                        cache
                            .insert_if_absent_with_options(
                                key,
                                operation as u64,
                                1 + (operation % 31) as u64,
                                None,
                            )
                            .unwrap();
                    }
                    5 => {
                        let _ = guard.get_untracked(key);
                    }
                    6 => {
                        let _ = guard.touch(key, None);
                    }
                    _ => guard.refresh(),
                }
            }

            for (local, key) in keys.iter().enumerate() {
                cache
                    .insert_discard_with_options(
                        key,
                        final_value(worker, local),
                        final_weight(local),
                        None,
                    )
                    .unwrap();
            }
        }));
    }

    start.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    stop.store(true, Ordering::Release);
    maintainer.join().unwrap();

    let expected_weight = (0..threads)
        .flat_map(|_| 0..keys_per_thread)
        .map(final_weight)
        .sum::<u64>();
    assert_eq!(cache.len(), expected_entries);
    assert_eq!(cache.weight(), expected_weight);
    for worker in 0..threads {
        for local in 0..keys_per_thread {
            let key = binary_key(worker * keys_per_thread + local);
            assert_eq!(
                cache.peek(&key).as_deref().copied(),
                Some(final_value(worker, local))
            );
        }
    }
}

fn final_value(worker: usize, local: usize) -> u64 {
    ((worker as u64) << 32) | local as u64
}

fn final_weight(local: usize) -> u64 {
    1 + (local % 97) as u64
}

fn binary_key(index: usize) -> Vec<u8> {
    const LENGTHS: [usize; 15] = [8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 47, 48, 49, 64];
    let length = LENGTHS[index % LENGTHS.len()];
    let mut key = vec![0_u8; length];
    key[..8].copy_from_slice(&(index as u64).to_le_bytes());
    let mut state = index as u64;
    for byte in &mut key[8..] {
        state = splitmix64(state);
        *byte = state.to_le_bytes()[0];
    }
    key
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
