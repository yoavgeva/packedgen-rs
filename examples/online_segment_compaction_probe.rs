//! Online segment compaction under concurrent read/write traffic.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::alloc::System;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{OnlineMutableSegmentCache, SegmentCacheConfig};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const VALUE_BYTES: usize = 64;
const TTL: Duration = Duration::from_secs(3_600);

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let prepared_writes = argument(&mut arguments, entries.div_ceil(10)).max(1);
    let threads = argument(&mut arguments, 16);
    let delta_capacity =
        argument(&mut arguments, prepared_writes.saturating_mul(2).max(1_024)).max(1);
    let preparation = arguments.next().unwrap_or_else(|| "update".to_owned());
    let post_read_operations = argument(&mut arguments, 0);
    assert!(
        matches!(
            preparation.as_str(),
            "update" | "delete" | "insert" | "localized-update" | "localized-delete"
        ),
        "preparation must be update, delete, insert, localized-update, or localized-delete"
    );

    let prepared_key_count = if matches!(
        preparation.as_str(),
        "insert" | "localized-update" | "localized-delete"
    ) {
        entries.saturating_add(prepared_writes)
    } else {
        entries
    };
    let keys = Arc::new((0..prepared_key_count).map(key).collect::<Vec<_>>());
    let region = Region::new(GLOBAL);
    let cache = Arc::new(
        OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1)),
            keys.iter()
                .take(entries)
                .enumerate()
                .map(|(index, key)| (key.as_slice(), value(index), Some(TTL))),
            delta_capacity,
        )
        .unwrap(),
    );
    let build_bytes = live_bytes(region.change());
    if matches!(
        preparation.as_str(),
        "localized-update" | "localized-delete"
    ) {
        let writer = cache.pin_writer_batch();
        for operation in 0..prepared_writes {
            let inserted = entries.saturating_add(operation);
            writer
                .insert(&keys[inserted], value(inserted), Some(TTL))
                .unwrap();
        }
        drop(writer);
        cache.compact().unwrap();
    }
    {
        let writer = cache.pin_writer_batch();
        for operation in 0..prepared_writes {
            if preparation == "localized-delete" {
                let inserted = entries.saturating_add(operation);
                black_box(writer.remove(&keys[inserted]));
            } else if preparation == "localized-update" {
                let inserted = entries.saturating_add(operation);
                writer
                    .insert(&keys[inserted], value(operation + 1), Some(TTL))
                    .unwrap();
            } else if preparation == "delete" {
                let resident = operation % entries;
                black_box(writer.remove(&keys[resident]));
            } else if preparation == "insert" {
                let inserted = entries.saturating_add(operation);
                writer
                    .insert(&keys[inserted], value(inserted), Some(TTL))
                    .unwrap();
            } else {
                let resident = mixed_index(operation, entries);
                writer
                    .insert(&keys[resident], value(operation + 1), Some(TTL))
                    .unwrap();
            }
        }
    }
    let precompact_bytes = live_bytes(region.change());
    let compaction_threshold_records = cache.compaction_threshold_records();
    let compaction_recommended = cache.compaction_recommended();

    let active = Arc::new(AtomicBool::new(true));
    let ready = Arc::new(Barrier::new(threads + 1));
    let operations = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let workers = (0..threads)
        .map(|worker| {
            let cache = Arc::clone(&cache);
            let keys = Arc::clone(&keys);
            let active = Arc::clone(&active);
            let ready = Arc::clone(&ready);
            let operations = Arc::clone(&operations);
            let writes = Arc::clone(&writes);
            thread::spawn(move || {
                ready.wait();
                let mut operation = worker.wrapping_mul(0x9e37_79b9);
                let mut local_operations = 0_u64;
                let mut local_writes = 0_u64;
                let mut checksum = 0_u64;
                while active.load(Ordering::Acquire) {
                    let guard = cache.pin();
                    for _ in 0..256 {
                        let resident = mixed_index(operation, keys.len());
                        if operation % 20 == 0 {
                            guard
                                .insert(&keys[resident], value(operation), Some(TTL))
                                .unwrap();
                            local_writes += 1;
                        } else {
                            checksum = checksum.wrapping_add(
                                guard
                                    .peek(&keys[resident])
                                    .map_or(0, |value| u64::from(value[0])),
                            );
                        }
                        operation = operation.wrapping_add(1);
                        local_operations += 1;
                    }
                }
                operations.fetch_add(local_operations, Ordering::Relaxed);
                writes.fetch_add(local_writes, Ordering::Relaxed);
                checksum
            })
        })
        .collect::<Vec<_>>();

    ready.wait();
    let compact_started = Instant::now();
    let stats = cache.compact().unwrap();
    let compact_elapsed = compact_started.elapsed();
    active.store(false, Ordering::Release);
    let checksum = workers
        .into_iter()
        .fold(0_u64, |checksum, worker| checksum ^ worker.join().unwrap());
    let point_operations = operations.load(Ordering::Relaxed);
    let point_writes = writes.load(Ordering::Relaxed);
    let postcompact_bytes = live_bytes(region.change());
    let modeled_peak_bytes = precompact_bytes
        .saturating_add(stats.newly_allocated_generation_bytes)
        .saturating_add(stats.temporary_build_index_bytes);
    let post_read_started = Instant::now();
    let mut post_read_checksum = 0_u64;
    if post_read_operations != 0 {
        let guard = cache.pin();
        let post_read_entries = if matches!(preparation.as_str(), "insert" | "localized-update") {
            entries.saturating_add(prepared_writes)
        } else {
            entries
        };
        for operation in 0..post_read_operations {
            let resident = mixed_index(operation, post_read_entries);
            let lookup = keys[resident].as_slice();
            post_read_checksum = post_read_checksum
                .wrapping_add(guard.peek(lookup).map_or(0, |value| u64::from(value[0])));
        }
    }
    let post_read_elapsed = post_read_started.elapsed();
    let post_read_mops = if post_read_operations == 0 {
        0.0
    } else {
        post_read_operations as f64 / post_read_elapsed.as_secs_f64() / 1_000_000.0
    };
    black_box((checksum, post_read_checksum, &cache));

    println!(
        "preparation,entries,prepared_writes,threads,compaction_threshold_records,\
         compaction_recommended,build_bytes,precompact_bytes,postcompact_bytes,\
         modeled_peak_bytes,previous_entries,compacted_delta_records,compacted_entries,\
         new_generation_bytes,newly_allocated_generation_bytes,reused_entries,\
         reused_record_bytes,copied_record_bytes,record_segments,\
         temporary_build_index_bytes,writer_redirect_ms,\
         background_build_ms,base_publish_ms,total_compaction_ms,point_operations,\
         point_writes,point_mops_during_compaction,post_read_operations,post_read_mops"
    );
    println!(
        "{preparation},{entries},{prepared_writes},{threads},{compaction_threshold_records},\
         {compaction_recommended},{build_bytes},{precompact_bytes},\
         {postcompact_bytes},{modeled_peak_bytes},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},\
         {:.3},{point_operations},{point_writes},{:.3},{post_read_operations},{post_read_mops:.3}",
        stats.previous_entries,
        stats.compacted_delta_records,
        stats.compacted_entries,
        stats.new_generation_bytes,
        stats.newly_allocated_generation_bytes,
        stats.reused_entries,
        stats.reused_record_bytes,
        stats.copied_record_bytes,
        stats.record_segments,
        stats.temporary_build_index_bytes,
        stats.writer_redirect.as_secs_f64() * 1_000.0,
        stats.background_build.as_secs_f64() * 1_000.0,
        stats.base_publish.as_secs_f64() * 1_000.0,
        compact_elapsed.as_secs_f64() * 1_000.0,
        point_operations as f64 / compact_elapsed.as_secs_f64() / 1_000_000.0,
    );
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse::<usize>().expect("argument must be an integer")
    })
}

fn key(index: usize) -> Vec<u8> {
    let bytes = u64::try_from(index).unwrap().to_le_bytes();
    let length = 8 + index % 17;
    let mut key = Vec::with_capacity(length);
    while key.len() < length {
        key.extend_from_slice(&bytes);
    }
    key.truncate(length);
    key
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn mixed_index(operation: usize, modulus: usize) -> usize {
    let mut value = u64::try_from(operation).unwrap_or(u64::MAX);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    usize::try_from(value % u64::try_from(modulus).unwrap()).unwrap()
}

fn live_bytes(stats: stats_alloc::Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}
