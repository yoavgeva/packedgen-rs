//! Point-operation and retained-memory probe for the mutable segment cache.

use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use packedgen::{CacheConfig, DirectPackedCache, MutableSegmentCache, SegmentCacheConfig};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let backend = arguments.next().unwrap_or_else(|| "segment".to_owned());
    let operation = arguments.next().unwrap_or_else(|| "read-hit".to_owned());
    let entries = arguments.next().map_or(1_000_000, |value| {
        value.parse::<usize>().expect("entries must be an integer")
    });
    let operations = arguments.next().map_or(1_000_000, |value| {
        value
            .parse::<usize>()
            .expect("operations must be an integer")
    });
    let delta_capacity = arguments.next().map_or_else(
        || entries.div_ceil(10).max(1_024),
        |value| {
            value
                .parse::<usize>()
                .expect("delta capacity must be an integer")
        },
    );

    match backend.as_str() {
        "segment" => run_segment(&operation, entries, operations, delta_capacity),
        "direct" => run_direct(&operation, entries, operations, delta_capacity),
        _ => panic!("backend must be segment or direct"),
    }
}

#[allow(clippy::too_many_lines)]
fn run_segment(operation: &str, entries: usize, operations: usize, delta_capacity: usize) {
    let region = Region::new(GLOBAL);
    let build_started = Instant::now();
    let mut cache = MutableSegmentCache::try_from_entries(
        SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1)),
        (0..entries).map(|index| {
            (
                mixed_key(index),
                [u8::try_from(index & 255).unwrap(); 64],
                Some(Duration::from_secs(3_600)),
            )
        }),
        delta_capacity,
    )
    .unwrap();
    let build_elapsed = build_started.elapsed();
    let build_allocation = region.change();
    let build_live = live_bytes(build_allocation);

    let prepared = delta_capacity.min(entries);
    match operation {
        "read-updated-hit" | "read-untouched-after-update" => {
            for index in 0..prepared {
                let key = mixed_key_array(index);
                cache
                    .insert(
                        &key[..mixed_key_bytes(index)],
                        [u8::try_from(index.wrapping_add(1) & 255).unwrap(); 64],
                        Some(Duration::from_secs(3_600)),
                    )
                    .unwrap();
            }
        }
        "read-new-hit" | "read-base-after-new" => {
            for index in 0..prepared {
                let new_index = entries.saturating_add(index);
                let key = mixed_key_array(new_index);
                cache
                    .insert(
                        &key[..mixed_key_bytes(new_index)],
                        [u8::try_from(index & 255).unwrap(); 64],
                        Some(Duration::from_secs(3_600)),
                    )
                    .unwrap();
            }
        }
        "read-deleted-miss" => {
            for index in 0..prepared {
                let key = mixed_key_array(index);
                black_box(cache.remove(&key[..mixed_key_bytes(index)]));
            }
        }
        _ => {}
    }

    let started = Instant::now();
    let checksum = match operation {
        "read-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                let value = guard
                    .peek(&key[..mixed_key_bytes(resident)])
                    .expect("resident key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-miss" => {
            let guard = cache.pin();
            let mut misses = 0_u64;
            for index in 0..operations {
                let missing = entries.saturating_add(index);
                let key = mixed_key_array(missing);
                misses = misses.wrapping_add(u64::from(
                    guard.peek(&key[..mixed_key_bytes(missing)]).is_none(),
                ));
            }
            misses
        }
        "read-updated-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % prepared;
                let key = mixed_key_array(resident);
                let value = guard
                    .peek(&key[..mixed_key_bytes(resident)])
                    .expect("updated key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-new-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = entries.saturating_add(index % prepared);
                let key = mixed_key_array(resident);
                let value = guard
                    .peek(&key[..mixed_key_bytes(resident)])
                    .expect("new delta key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-base-after-new" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                let value = guard
                    .peek(&key[..mixed_key_bytes(resident)])
                    .expect("base key must remain live");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-untouched-after-update" => {
            let untouched = entries.saturating_sub(prepared);
            assert_ne!(untouched, 0, "benchmark requires an untouched base key");
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = prepared.saturating_add(index % untouched);
                let key = mixed_key_array(resident);
                let value = guard
                    .peek(&key[..mixed_key_bytes(resident)])
                    .expect("untouched base key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-deleted-miss" => {
            let guard = cache.pin();
            let mut misses = 0_u64;
            for index in 0..operations {
                let resident = index % prepared;
                let key = mixed_key_array(resident);
                misses = misses.wrapping_add(u64::from(
                    guard.peek(&key[..mixed_key_bytes(resident)]).is_none(),
                ));
            }
            misses
        }
        "update-hit" => {
            let mut replaced = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                replaced = replaced.wrapping_add(u64::from(matches!(
                    cache
                        .insert(
                            &key[..mixed_key_bytes(resident)],
                            [u8::try_from(index.wrapping_add(1) & 255).unwrap(); 64],
                            Some(Duration::from_secs(3_600)),
                        )
                        .unwrap(),
                    packedgen::SegmentCacheWriteOutcome::Replaced
                )));
            }
            replaced
        }
        "insert-new" => {
            let mut inserted = 0_u64;
            for index in 0..operations {
                let new_index = entries.saturating_add(index);
                let key = mixed_key_array(new_index);
                inserted = inserted.wrapping_add(u64::from(matches!(
                    cache
                        .insert(
                            &key[..mixed_key_bytes(new_index)],
                            [u8::try_from(index & 255).unwrap(); 64],
                            Some(Duration::from_secs(3_600)),
                        )
                        .unwrap(),
                    packedgen::SegmentCacheWriteOutcome::Inserted
                )));
            }
            inserted
        }
        "delete-hit" => {
            let mut deleted = 0_u64;
            for index in 0..operations.min(entries) {
                let key = mixed_key_array(index);
                deleted =
                    deleted.wrapping_add(u64::from(cache.remove(&key[..mixed_key_bytes(index)])));
            }
            deleted
        }
        "compact" | "compact-new" | "compact-delete" => {
            match operation {
                "compact" => apply_churn(&cache, entries, operations),
                "compact-new" => {
                    for index in 0..operations {
                        let new_index = entries.saturating_add(index);
                        let key = mixed_key_array(new_index);
                        cache
                            .insert(
                                &key[..mixed_key_bytes(new_index)],
                                [u8::try_from(index & 255).unwrap(); 64],
                                Some(Duration::from_secs(3_600)),
                            )
                            .unwrap();
                    }
                }
                "compact-delete" => {
                    for index in 0..operations.min(entries) {
                        let key = mixed_key_array(index);
                        black_box(cache.remove(&key[..mixed_key_bytes(index)]));
                    }
                }
                _ => unreachable!(),
            }
            let precompact_bytes = live_bytes(region.change());
            let stats = cache.compact().unwrap();
            let modeled_peak_bytes = precompact_bytes
                .saturating_add(stats.new_generation_bytes)
                .saturating_add(stats.temporary_build_index_bytes);
            println!(
                "compaction,previous_base,delta_records,compacted_entries,precompact_bytes,new_generation_bytes,temporary_staging_payload_bytes,temporary_build_index_bytes,modeled_peak_bytes"
            );
            println!(
                "compaction,{},{},{},{},{},{},{},{}",
                stats.previous_base_entries,
                stats.delta_records,
                stats.compacted_entries,
                precompact_bytes,
                stats.new_generation_bytes,
                stats.temporary_staging_payload_bytes,
                stats.temporary_build_index_bytes,
                modeled_peak_bytes,
            );
            u64::try_from(stats.compacted_entries).unwrap_or(u64::MAX)
        }
        _ => panic!(
            "operation must be read-hit, read-miss, read-updated-hit, read-new-hit, \
             read-deleted-miss, read-base-after-new, read-untouched-after-update, update-hit, \
             insert-new, delete-hit, compact, compact-new, or compact-delete"
        ),
    };
    let elapsed = started.elapsed();
    let post_allocation = region.change();
    let post_live = live_bytes(post_allocation);
    black_box((checksum, &cache));
    print_result(
        "segment",
        operation,
        entries,
        operations,
        build_live,
        post_live,
        build_elapsed,
        elapsed,
        cache.delta_records(),
        post_allocation.allocations,
    );
}

#[allow(clippy::too_many_lines)]
fn run_direct(operation: &str, entries: usize, operations: usize, delta_capacity: usize) {
    assert_ne!(
        operation, "compact",
        "the direct control has no equivalent exclusive compaction operation"
    );
    let region = Region::new(GLOBAL);
    let build_started = Instant::now();
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(u64::MAX).with_overlay_capacity(delta_capacity),
        (0..entries).map(|index| {
            (
                mixed_key(index),
                [u8::try_from(index & 255).unwrap(); 64],
                u64::try_from(64 + mixed_key_bytes(index)).unwrap(),
                Some(Duration::from_secs(3_600)),
            )
        }),
    )
    .unwrap();
    let build_elapsed = build_started.elapsed();
    let build_allocation = region.change();
    let build_live = live_bytes(build_allocation);

    let prepared = delta_capacity.min(entries);
    match operation {
        "read-updated-hit" | "read-untouched-after-update" => {
            for index in 0..prepared {
                let key = mixed_key_array(index);
                cache
                    .insert_discard_with_options(
                        &key[..mixed_key_bytes(index)],
                        [u8::try_from(index.wrapping_add(1) & 255).unwrap(); 64],
                        u64::try_from(64 + mixed_key_bytes(index)).unwrap(),
                        Some(Duration::from_secs(3_600)),
                    )
                    .unwrap();
            }
        }
        "read-new-hit" | "read-base-after-new" => {
            for index in 0..prepared {
                let new_index = entries.saturating_add(index);
                let key = mixed_key_array(new_index);
                cache
                    .insert_discard_with_options(
                        &key[..mixed_key_bytes(new_index)],
                        [u8::try_from(index & 255).unwrap(); 64],
                        u64::try_from(64 + mixed_key_bytes(new_index)).unwrap(),
                        Some(Duration::from_secs(3_600)),
                    )
                    .unwrap();
            }
        }
        "read-deleted-miss" => {
            for index in 0..prepared {
                let key = mixed_key_array(index);
                black_box(cache.remove_discard(&key[..mixed_key_bytes(index)]));
            }
        }
        _ => {}
    }

    let started = Instant::now();
    let checksum = match operation {
        "read-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                let value = guard
                    .get_untracked(&key[..mixed_key_bytes(resident)])
                    .expect("resident key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-miss" => {
            let guard = cache.pin();
            let mut misses = 0_u64;
            for index in 0..operations {
                let missing = entries.saturating_add(index);
                let key = mixed_key_array(missing);
                misses = misses.wrapping_add(u64::from(
                    guard
                        .get_untracked(&key[..mixed_key_bytes(missing)])
                        .is_none(),
                ));
            }
            misses
        }
        "read-updated-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % prepared;
                let key = mixed_key_array(resident);
                let value = guard
                    .get_untracked(&key[..mixed_key_bytes(resident)])
                    .expect("updated key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-new-hit" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = entries.saturating_add(index % prepared);
                let key = mixed_key_array(resident);
                let value = guard
                    .get_untracked(&key[..mixed_key_bytes(resident)])
                    .expect("new delta key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-base-after-new" => {
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                let value = guard
                    .get_untracked(&key[..mixed_key_bytes(resident)])
                    .expect("base key must remain live");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-untouched-after-update" => {
            let untouched = entries.saturating_sub(prepared);
            assert_ne!(untouched, 0, "benchmark requires an untouched base key");
            let guard = cache.pin();
            let mut checksum = 0_u64;
            for index in 0..operations {
                let resident = prepared.saturating_add(index % untouched);
                let key = mixed_key_array(resident);
                let value = guard
                    .get_untracked(&key[..mixed_key_bytes(resident)])
                    .expect("untouched base key must hit");
                checksum = checksum.wrapping_add(u64::from(value[0]));
            }
            checksum
        }
        "read-deleted-miss" => {
            let guard = cache.pin();
            let mut misses = 0_u64;
            for index in 0..operations {
                let resident = index % prepared;
                let key = mixed_key_array(resident);
                misses = misses.wrapping_add(u64::from(
                    guard
                        .get_untracked(&key[..mixed_key_bytes(resident)])
                        .is_none(),
                ));
            }
            misses
        }
        "update-hit" => {
            let mut replaced = 0_u64;
            for index in 0..operations {
                let resident = index % entries;
                let key = mixed_key_array(resident);
                replaced = replaced.wrapping_add(u64::from(matches!(
                    cache
                        .insert_discard_with_options(
                            &key[..mixed_key_bytes(resident)],
                            [u8::try_from(index.wrapping_add(1) & 255).unwrap(); 64],
                            u64::try_from(64 + mixed_key_bytes(resident)).unwrap(),
                            Some(Duration::from_secs(3_600)),
                        )
                        .unwrap(),
                    packedgen::CacheWriteOutcome::Replaced
                )));
            }
            replaced
        }
        "insert-new" => {
            let mut inserted = 0_u64;
            for index in 0..operations {
                let new_index = entries.saturating_add(index);
                let key = mixed_key_array(new_index);
                inserted = inserted.wrapping_add(u64::from(matches!(
                    cache
                        .insert_discard_with_options(
                            &key[..mixed_key_bytes(new_index)],
                            [u8::try_from(index & 255).unwrap(); 64],
                            u64::try_from(64 + mixed_key_bytes(new_index)).unwrap(),
                            Some(Duration::from_secs(3_600)),
                        )
                        .unwrap(),
                    packedgen::CacheWriteOutcome::Inserted
                )));
            }
            inserted
        }
        "delete-hit" => {
            let mut deleted = 0_u64;
            for index in 0..operations.min(entries) {
                let key = mixed_key_array(index);
                deleted = deleted.wrapping_add(u64::from(
                    cache.remove_discard(&key[..mixed_key_bytes(index)]),
                ));
            }
            deleted
        }
        _ => panic!("unsupported direct operation"),
    };
    let elapsed = started.elapsed();
    let post_allocation = region.change();
    let post_live = live_bytes(post_allocation);
    black_box((checksum, &cache));
    print_result(
        "direct",
        operation,
        entries,
        operations,
        build_live,
        post_live,
        build_elapsed,
        elapsed,
        0,
        post_allocation.allocations,
    );
}

fn apply_churn(cache: &MutableSegmentCache, entries: usize, operations: usize) {
    let third = operations / 3;
    for index in 0..third {
        let resident = index % entries;
        let key = mixed_key_array(resident);
        cache
            .insert(
                &key[..mixed_key_bytes(resident)],
                [u8::try_from(index.wrapping_add(1) & 255).unwrap(); 64],
                Some(Duration::from_secs(3_600)),
            )
            .unwrap();
    }
    for index in 0..third {
        let new_index = entries.saturating_add(index);
        let key = mixed_key_array(new_index);
        cache
            .insert(
                &key[..mixed_key_bytes(new_index)],
                [u8::try_from(index & 255).unwrap(); 64],
                Some(Duration::from_secs(3_600)),
            )
            .unwrap();
    }
    for index in third..third.saturating_mul(2).min(entries) {
        let key = mixed_key_array(index);
        black_box(cache.remove(&key[..mixed_key_bytes(index)]));
    }
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    backend: &str,
    operation: &str,
    entries: usize,
    operations: usize,
    build_live: usize,
    post_live: usize,
    build_elapsed: Duration,
    elapsed: Duration,
    delta_records: usize,
    allocations: usize,
) {
    #[allow(clippy::cast_precision_loss)]
    let build_bpe = build_live as f64 / entries as f64;
    #[allow(clippy::cast_precision_loss)]
    let post_bpe = post_live as f64 / entries as f64;
    #[allow(clippy::cast_precision_loss)]
    let operations_f64 = operations as f64;
    let seconds = elapsed.as_secs_f64();
    println!(
        "backend,operation,entries,operations,build_bytes,build_bpe,post_bytes,post_bpe,delta_records,build_ms,mops_per_second,ns_per_operation,allocations"
    );
    println!(
        "{backend},{operation},{entries},{operations},{build_live},{build_bpe:.3},{post_live},{post_bpe:.3},{delta_records},{:.3},{:.3},{:.3},{allocations}",
        build_elapsed.as_secs_f64() * 1_000.0,
        operations_f64 / seconds / 1_000_000.0,
        seconds * 1_000_000_000.0 / operations_f64,
    );
}

fn live_bytes(stats: stats_alloc::Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn mixed_key(index: usize) -> Box<[u8]> {
    let key = mixed_key_array(index);
    key[..mixed_key_bytes(index)].into()
}

fn mixed_key_array(index: usize) -> [u8; 48] {
    let mut key = [0_u8; 48];
    let mut state = u64::try_from(index).expect("entry index fits u64");
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mixed_key_bytes(index: usize) -> usize {
    match index % 100 {
        0..=39 => 8,
        40..=64 => 16,
        65..=79 => 24,
        80..=89 => 32,
        _ => 48,
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
