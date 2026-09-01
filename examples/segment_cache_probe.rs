//! Requested-memory and read-cost probe for the packed-segment layout.

use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use packedgen::{CacheConfig, DirectPackedCache, FrozenSegmentCache, SegmentCacheConfig};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = arguments.next().map_or(1_000_000, |value| {
        value.parse::<usize>().expect("entries must be an integer")
    });
    let lookups = arguments.next().map_or(10_000_000, |value| {
        value.parse::<usize>().expect("lookups must be an integer")
    });
    let expiry = arguments.next().is_none_or(|value| match value.as_str() {
        "ttl" => true,
        "no-ttl" => false,
        _ => panic!("expiry mode must be ttl or no-ttl"),
    });
    let backend = arguments.next().unwrap_or_else(|| "segment".to_owned());
    match backend.as_str() {
        "segment" => run_segment(entries, lookups, expiry, 7, false),
        "segment6" => run_segment(entries, lookups, expiry, 6, false),
        "packed" => run_segment(entries, lookups, expiry, 7, true),
        "packed6" => run_segment(entries, lookups, expiry, 6, true),
        "direct" => run_direct(entries, lookups, expiry),
        _ => panic!("backend must be segment, segment6, packed, packed6, or direct"),
    }
}

fn run_segment(entries: usize, lookups: usize, expiry: bool, occupied: u8, packed: bool) {
    let mut config = if expiry {
        SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1))
    } else {
        SegmentCacheConfig::without_expiry()
    }
    .with_target_bucket_occupancy(occupied);
    if packed {
        config = config.with_packed_bucket_index();
    } else {
        config = config.with_atomic_word_index();
    }
    let ttl = expiry.then_some(Duration::from_secs(3_600));

    let region = Region::new(GLOBAL);
    let cache = FrozenSegmentCache::try_from_entries(
        config,
        (0..entries).map(|index| {
            (
                mixed_key(index),
                [u8::try_from(index & 255).unwrap(); 64],
                ttl,
            )
        }),
    )
    .unwrap();
    let allocation = region.change();
    let live_bytes = allocation
        .bytes_allocated
        .saturating_sub(allocation.bytes_deallocated);
    let layout = cache.stats();

    #[allow(clippy::cast_precision_loss)]
    let requested_bpe = live_bytes as f64 / entries as f64;
    #[allow(clippy::cast_precision_loss)]
    let modeled_bpe = layout.modeled_retained_bytes() as f64 / entries as f64;
    #[allow(clippy::cast_precision_loss)]
    let overhead_bpe = layout.modeled_overhead_bytes() as f64 / entries as f64;
    println!(
        "backend,entries,requested_bytes,requested_bpe,modeled_bpe,overhead_bpe,index_bpe,record_header_bpe,load_bps,allocations"
    );
    #[allow(clippy::cast_precision_loss)]
    let index_bpe = layout.index_bytes as f64 / entries as f64;
    #[allow(clippy::cast_precision_loss)]
    let header_bpe = layout.record_header_bytes as f64 / entries as f64;
    let backend = match (packed, occupied) {
        (false, 7) => "segment",
        (false, _) => "segment6",
        (true, 7) => "packed",
        (true, _) => "packed6",
    };
    println!(
        "{backend},{entries},{live_bytes},{requested_bpe:.3},{modeled_bpe:.3},{overhead_bpe:.3},{index_bpe:.3},{header_bpe:.3},{},{}",
        layout.index_load_bps(),
        allocation.allocations,
    );

    let hit_started = Instant::now();
    let mut hit_checksum = 0_u64;
    for operation in 0..lookups {
        let index = operation % entries;
        let key = mixed_key_array(index);
        let value = cache
            .peek(&key[..mixed_key_bytes(index)])
            .expect("resident key must hit");
        hit_checksum = hit_checksum.wrapping_add(u64::from(value[0]));
    }
    let hit_elapsed = hit_started.elapsed();

    let miss_started = Instant::now();
    let mut misses = 0_usize;
    for operation in 0..lookups {
        let index = entries.saturating_add(operation % entries);
        let key = mixed_key_array(index);
        misses += usize::from(cache.peek(&key[..mixed_key_bytes(index)]).is_none());
    }
    let miss_elapsed = miss_started.elapsed();
    black_box((hit_checksum, misses, &cache));

    println!("operation,mops_per_second,ns_per_operation");
    print_rate("read_hit", lookups, hit_elapsed);
    print_rate("read_miss", lookups, miss_elapsed);
}

fn run_direct(entries: usize, lookups: usize, expiry: bool) {
    let ttl = expiry.then_some(Duration::from_secs(3_600));
    let region = Region::new(GLOBAL);
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(entries.saturating_mul(2).max(1))
            .with_overlay_capacity(entries.max(1)),
    )
    .unwrap();
    let guard = cache.pin();
    for index in 0..entries {
        guard
            .insert_if_absent_with_options(
                &mixed_key(index),
                [u8::try_from(index & 255).unwrap(); 64],
                64 + u64::try_from(mixed_key_bytes(index)).unwrap(),
                ttl,
            )
            .unwrap();
    }
    drop(guard);
    let allocation = region.change();
    let live_bytes = allocation
        .bytes_allocated
        .saturating_sub(allocation.bytes_deallocated);
    #[allow(clippy::cast_precision_loss)]
    let requested_bpe = live_bytes as f64 / entries as f64;
    let logical_bytes = entries.saturating_mul(828) / 10;
    #[allow(clippy::cast_precision_loss)]
    let overhead_bpe = live_bytes.saturating_sub(logical_bytes) as f64 / entries as f64;
    println!(
        "backend,entries,requested_bytes,requested_bpe,modeled_bpe,overhead_bpe,index_bpe,record_header_bpe,load_bps,allocations"
    );
    println!(
        "direct,{entries},{live_bytes},{requested_bpe:.3},{requested_bpe:.3},{overhead_bpe:.3},-,-,-,{}",
        allocation.allocations,
    );

    let guard = cache.pin();
    let hit_started = Instant::now();
    let mut hit_checksum = 0_u64;
    for operation in 0..lookups {
        let index = operation % entries;
        let key = mixed_key_array(index);
        let value = guard
            .get_untracked(&key[..mixed_key_bytes(index)])
            .expect("resident key must hit");
        hit_checksum = hit_checksum.wrapping_add(u64::from(value[0]));
    }
    let hit_elapsed = hit_started.elapsed();

    let miss_started = Instant::now();
    let mut misses = 0_usize;
    for operation in 0..lookups {
        let index = entries.saturating_add(operation % entries);
        let key = mixed_key_array(index);
        misses += usize::from(
            guard
                .get_untracked(&key[..mixed_key_bytes(index)])
                .is_none(),
        );
    }
    let miss_elapsed = miss_started.elapsed();
    black_box((hit_checksum, misses, &cache));

    println!("operation,mops_per_second,ns_per_operation");
    print_rate("read_hit", lookups, hit_elapsed);
    print_rate("read_miss", lookups, miss_elapsed);
}

fn print_rate(name: &str, operations: usize, elapsed: Duration) {
    #[allow(clippy::cast_precision_loss)]
    let operations = operations as f64;
    let seconds = elapsed.as_secs_f64();
    println!(
        "{name},{:.3},{:.3}",
        operations / seconds / 1_000_000.0,
        seconds * 1_000_000_000.0 / operations,
    );
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
