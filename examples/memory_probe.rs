//! Isolated requested-allocation comparison for `PackedGen` and `HashBrown`.

#![allow(clippy::too_many_lines)]

use std::alloc::System;
use std::hint::black_box;

use dashmap::DashMap;
use flurry::HashMap as FlurryHashMap;
use hashbrown::{DefaultHashBuilder, HashMap};
use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, BucketPackedMap, ConcurrentSwissMap,
    ElasticConfig, FixedElasticMap, FrozenPackedMap, LockFreeAtomicU64GenerationMap,
    LockFreeBinaryMap, LockFreeGenerationMap, LockFreeHybridMap, NonMaxU64, PackedBinaryMap,
    PackedKeyArena, PackedKeyRef, PackedSwissMap, RouteCacheBudget, SegmentedLoad,
    SegmentedSwissMap,
};
use parking_lot::RwLock;
use scc::HashMap as SccHashMap;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let implementation = arguments.next().unwrap_or_else(|| "all".to_owned());
    let entries = arguments.next().map_or(1_000_000, |value| {
        value.parse::<usize>().expect("entries must be an integer")
    });
    let key_bytes = arguments.next().map_or(32, |value| {
        value
            .parse::<usize>()
            .expect("key bytes must be an integer")
    });

    println!("implementation,entries,live_bytes,bytes_per_entry,allocations");
    match implementation.as_str() {
        "elastic-3" => print_elastic(entries, 3),
        "elastic-6" => print_elastic(entries, 6),
        "hashbrown" => print_hashbrown(entries),
        "elastic-binary-3" => print_elastic_binary(entries, 3),
        "elastic-binary-6" => print_elastic_binary(entries, 6),
        "hashbrown-binary" => print_hashbrown_binary(entries),
        "packed-binary-3" => print_packed_binary(entries, 3),
        "packed-binary-6" => print_packed_binary(entries, 6),
        "packed-binary-read-6" => print_packed_binary_read_optimized(entries, 6),
        "frozen-binary" => print_frozen_binary(entries),
        "bucket-binary" => print_bucket_binary(entries),
        "swiss-binary" => print_swiss_binary(entries),
        "segmented-swiss-binary" => print_segmented_swiss_binary(entries),
        "concurrent-swiss-binary" => print_concurrent_swiss_binary(entries),
        "dashmap-binary" => print_dashmap_binary(entries),
        "dashmap-delete-half-binary" => print_dashmap_after_half_delete(entries),
        "scc-binary" => print_scc_binary(entries),
        "flurry-binary" => print_flurry_binary(entries),
        "rwlock-hashbrown-binary" => print_rwlock_hashbrown_binary(entries),
        "lockfree-binary" => print_lockfree_binary(entries),
        "lockfree-hybrid-binary" => print_lockfree_hybrid_binary(entries),
        "lockfree-hybrid-churn-binary" => print_lockfree_hybrid_churn_binary(entries),
        "lockfree-generation-binary" => print_lockfree_generation(entries, false),
        "lockfree-generation-churn-binary" => print_lockfree_generation(entries, true),
        "lockfree-atomic-generation-binary" => print_lockfree_atomic_generation(entries, false),
        "lockfree-atomic-generation-delete-half-binary" => {
            print_lockfree_atomic_after_half_delete(entries, false);
        }
        "lockfree-atomic-generation-delete-half-rebuild-binary" => {
            print_lockfree_atomic_after_half_delete(entries, true);
        }
        "lockfree-atomic-generation-filter-binary" => {
            print_lockfree_atomic_accelerated_generation(
                entries,
                AtomicGenerationBaseFilter::OneBytePerEntry,
                "filter1b",
            );
        }
        "lockfree-atomic-generation-fingerprint-binary" => {
            print_lockfree_atomic_accelerated_generation(
                entries,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
                "fingerprint",
            );
        }
        "lockfree-atomic-generation-churn-binary" => {
            print_lockfree_atomic_generation(entries, true);
        }
        "lockfree-atomic-overlay-compact-binary" => {
            print_lockfree_atomic_overlay(entries, key_bytes, compact_overlay_mode(key_bytes));
        }
        "lockfree-atomic-overlay-atomic-binary" => {
            print_lockfree_atomic_overlay(
                entries,
                key_bytes,
                AtomicGenerationOverlay::AtomicFixed32,
            );
        }
        "lockfree-atomic-overlay-papaya-binary" => {
            print_lockfree_atomic_overlay(entries, key_bytes, AtomicGenerationOverlay::Papaya);
        }
        "lockfree-atomic-overlay-arcswap-binary" => {
            print_lockfree_atomic_overlay(
                entries,
                key_bytes,
                AtomicGenerationOverlay::ArcSwapFixed32,
            );
        }
        "arena" => {
            print_packed_arena(entries);
            print_boxed_keys(entries);
        }
        "sweep" => print_sweep(),
        "packed-sweep" => print_packed_sweep(),
        "all" => print_all(entries),
        _ => panic!(
            "expected elastic-3, elastic-6, hashbrown, elastic-binary-3, \
             elastic-binary-6, hashbrown-binary, packed-binary-3, packed-binary-6, \
             packed-binary-read-6, \
             frozen-binary, bucket-binary, swiss-binary, segmented-swiss-binary, \
             concurrent-swiss-binary, dashmap-binary, dashmap-delete-half-binary, scc-binary, \
             flurry-binary, rwlock-hashbrown-binary, lockfree-binary, \
             lockfree-hybrid-binary, lockfree-hybrid-churn-binary, \
             lockfree-generation-binary, lockfree-generation-churn-binary, \
             lockfree-atomic-generation-binary, lockfree-atomic-generation-churn-binary, \
             lockfree-atomic-generation-delete-half-binary, \
             lockfree-atomic-generation-delete-half-rebuild-binary, \
             lockfree-atomic-generation-filter-binary, \
             lockfree-atomic-generation-fingerprint-binary, \
             lockfree-atomic-overlay-compact-binary, lockfree-atomic-overlay-papaya-binary, \
             lockfree-atomic-overlay-atomic-binary, \
             lockfree-atomic-overlay-arcswap-binary, \
             arena, sweep, \
             packed-sweep, or all"
        ),
    }
}

fn print_all(entries: usize) {
    print_elastic(entries, 3);
    print_elastic(entries, 6);
    print_hashbrown(entries);
    print_elastic_binary(entries, 3);
    print_elastic_binary(entries, 6);
    print_hashbrown_binary(entries);
    print_packed_binary(entries, 3);
    print_packed_binary(entries, 6);
    print_frozen_binary(entries);
    print_bucket_binary(entries);
    print_swiss_binary(entries);
    print_segmented_swiss_binary(entries);
    print_concurrent_swiss_binary(entries);
    print_dashmap_binary(entries);
    print_scc_binary(entries);
    print_flurry_binary(entries);
    print_rwlock_hashbrown_binary(entries);
    print_lockfree_binary(entries);
    print_lockfree_hybrid_binary(entries);
    print_lockfree_hybrid_churn_binary(entries);
    print_lockfree_generation(entries, false);
    print_lockfree_generation(entries, true);
    print_lockfree_atomic_generation(entries, false);
    print_lockfree_atomic_accelerated_generation(
        entries,
        AtomicGenerationBaseFilter::OneBytePerEntry,
        "filter1b",
    );
    print_lockfree_atomic_accelerated_generation(
        entries,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
        "fingerprint",
    );
    print_lockfree_atomic_generation(entries, true);
    print_packed_arena(entries);
    print_boxed_keys(entries);
}

fn print_elastic(entries: usize, exponent: u32) {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap();
    let mut map = FixedElasticMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(mix(key), key).unwrap();
    }
    let stats = region.change();
    print_row(&format!("elastic-2^-{exponent}"), entries, stats);
    black_box(&map);
}

fn print_hashbrown(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut map = HashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(mix(key), key);
    }
    let stats = region.change();
    print_row("hashbrown", entries, stats);
    black_box(&map);
}

fn print_elastic_binary(entries: usize, exponent: u32) {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap();
    let mut map = FixedElasticMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(binary_key(key), key).unwrap();
    }
    let stats = region.change();
    print_row(&format!("elastic-binary32-2^-{exponent}"), entries, stats);
    black_box(&map);
}

fn print_hashbrown_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut map = HashMap::with_capacity(entries);
    for key in 0..entries as u64 {
        map.insert(binary_key(key), key);
    }
    let stats = region.change();
    print_row("hashbrown-binary32", entries, stats);
    black_box(&map);
}

fn print_packed_binary(entries: usize, exponent: u32) {
    print_packed_binary_with_budget(entries, exponent, RouteCacheBudget::Adaptive, "");
}

fn print_packed_binary_read_optimized(entries: usize, exponent: u32) {
    print_packed_binary_with_budget(
        entries,
        exponent,
        RouteCacheBudget::ReadOptimized,
        "-read-optimized",
    );
}

fn print_packed_binary_with_budget(
    entries: usize,
    exponent: u32,
    budget: RouteCacheBudget,
    suffix: &str,
) {
    let region = Region::new(GLOBAL);
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(exponent)
        .unwrap()
        .with_route_cache_budget(budget);
    let mut map = PackedBinaryMap::new(config);
    for key in 0..entries as u64 {
        map.try_insert(&binary_key_array(key), key).unwrap();
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "packed-components: core_capacity={}, negative_filter_bytes={}, route_cache_bytes={}, route_cache_entries={}, arena_allocated_bytes={}",
        components.core_capacity,
        components.negative_filter_bytes,
        components.route_cache_bytes,
        components.route_cache_entries,
        components.arena_allocated_bytes
    );
    print_row(
        &format!("packed-elastic-binary32-2^-{exponent}{suffix}"),
        entries,
        stats,
    );
    black_box(&map);
}

fn print_frozen_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let map = FrozenPackedMap::try_from_entries((0..entries).map(|index| {
        let value = u64::try_from(index).expect("entry index must fit u64");
        (binary_key_array(value), value)
    }))
    .unwrap();
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "frozen-components: arena_allocated_bytes={}, slot_bytes={}, index_bits_per_entry={:.3}",
        components.arena_allocated_bytes, components.slot_bytes, components.index_bits_per_entry
    );
    print_row("frozen-ptrhash-binary32", entries, stats);
    black_box(&map);
}

fn print_bucket_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut map = BucketPackedMap::new(entries);
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key_array(value), value).unwrap();
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "bucket-components: bucket_bytes={}, entry_bytes={}, overflow_routes={}, \
         overflow_bytes={}, arena_allocated_bytes={}",
        components.bucket_bytes,
        components.entry_bytes,
        components.overflow_routes,
        components.overflow_bytes,
        components.arena_allocated_bytes
    );
    print_row("cacheline-bucket-binary32", entries, stats);
    black_box(&map);
}

fn print_swiss_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let key_bytes = entries.checked_mul(32).expect("key byte count overflow");
    let mut map = PackedSwissMap::try_with_capacity_and_key_bytes(entries, key_bytes).unwrap();
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key_array(value), value).unwrap();
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "swiss-components: table_capacity={}, arena_allocated_bytes={}",
        components.table_capacity, components.arena_allocated_bytes
    );
    print_row("packed-swiss-binary32", entries, stats);
    black_box(&map);
}

fn print_segmented_swiss_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let key_bytes = entries.checked_mul(32).expect("key byte count overflow");
    let mut map = SegmentedSwissMap::try_with_capacity_and_key_bytes(entries, key_bytes).unwrap();
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key_array(value), value).unwrap();
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "segmented-swiss-components: segments={}, table_capacity={}, route_buckets={}, \
         arena_allocated_bytes={}",
        components.segments,
        components.table_capacity,
        components.route_buckets,
        components.arena_allocated_bytes
    );
    print_row("segmented-packed-swiss-binary32", entries, stats);
    black_box(&map);
}

fn print_concurrent_swiss_binary(entries: usize) {
    const SHARDS: usize = 64;

    let region = Region::new(GLOBAL);
    let map =
        ConcurrentSwissMap::try_with_capacity_and_shards(entries, SHARDS, SegmentedLoad::Balanced)
            .unwrap();
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key_array(value), value).unwrap();
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "concurrent-swiss-components: shards={}, segments={}, table_capacity={}, \
         route_buckets={}, arena_allocated_bytes={}",
        components.shards,
        components.segments,
        components.table_capacity,
        components.route_buckets,
        components.arena_allocated_bytes
    );
    print_row("concurrent-segmented-swiss-binary32", entries, stats);
    black_box(&map);
}

fn print_dashmap_binary(entries: usize) {
    const SHARDS: usize = 64;

    let region = Region::new(GLOBAL);
    let map =
        DashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher_and_shard_amount(
            entries,
            DefaultHashBuilder::default(),
            SHARDS,
        );
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert(binary_key(value), value);
    }
    let stats = region.change();
    print_row("dashmap-boxed-binary32", entries, stats);
    black_box(&map);
}

fn print_dashmap_after_half_delete(entries: usize) {
    const SHARDS: usize = 64;

    let region = Region::new(GLOBAL);
    let map =
        DashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher_and_shard_amount(
            entries,
            DefaultHashBuilder::default(),
            SHARDS,
        );
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert(binary_key(value), value);
    }
    for index in (0..entries).step_by(2) {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.remove(binary_key_array(value).as_slice()).unwrap();
    }
    let stats = region.change();
    print_row(
        "dashmap-boxed-binary32-after-50pct-delete",
        map.len(),
        stats,
    );
    black_box(&map);
}

fn print_scc_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let map = SccHashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher(
        entries,
        DefaultHashBuilder::default(),
    );
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert_sync(binary_key(value), value).unwrap();
    }
    let stats = region.change();
    print_row("scc-boxed-binary32", entries, stats);
    black_box(&map);
}

fn print_flurry_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let map = FlurryHashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher(
        entries,
        DefaultHashBuilder::default(),
    );
    let guard = map.guard();
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert(binary_key(value), value, &guard);
    }
    drop(guard);
    let stats = region.change();
    print_row("flurry-boxed-binary32", entries, stats);
    black_box(&map);
}

fn print_rwlock_hashbrown_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut inner = HashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher(
        entries,
        DefaultHashBuilder::default(),
    );
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        inner.insert(binary_key(value), value);
    }
    let map = RwLock::new(inner);
    let stats = region.change();
    print_row("rwlock-hashbrown-boxed-binary32", entries, stats);
    black_box(&map);
}

fn print_lockfree_binary(entries: usize) {
    let region = Region::new(GLOBAL);
    let map = LockFreeBinaryMap::with_capacity(entries);
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert(&binary_key_array(value), value);
    }
    let stats = region.change();
    print_row("lockfree-papaya-boxed-binary32", entries, stats);
    black_box(&map);
}

fn print_lockfree_hybrid_binary(entries: usize) {
    print_lockfree_hybrid(entries, false);
}

fn print_lockfree_hybrid_churn_binary(entries: usize) {
    print_lockfree_hybrid(entries, true);
}

fn print_lockfree_hybrid(entries: usize, with_churn: bool) {
    let region = Region::new(GLOBAL);
    let map = LockFreeHybridMap::try_from_entries(
        (0..entries).map(|index| {
            let value = u64::try_from(index).expect("entry index must fit u64");
            (binary_key_array(value), value)
        }),
        entries.div_ceil(100),
    )
    .unwrap();
    if with_churn {
        for index in 0..entries.div_ceil(100) {
            let value = u64::try_from(index).expect("entry index must fit u64");
            map.update(&binary_key_array(value), |current| current + 1)
                .unwrap();
        }
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "lockfree-hybrid-components: base_entries={}, overlay_records={}, \
         arena_allocated_bytes={}, slot_bytes={}, index_bits_per_entry={:.3}",
        components.base.len,
        components.overlay_records,
        components.base.arena_allocated_bytes,
        components.base.slot_bytes,
        components.base.index_bits_per_entry
    );
    let name = if with_churn {
        "lockfree-hybrid-frozen-1pct-overlay-binary32"
    } else {
        "lockfree-hybrid-frozen-binary32"
    };
    print_row(name, entries, stats);
    black_box(&map);
}

fn print_lockfree_generation(entries: usize, with_churn: bool) {
    let region = Region::new(GLOBAL);
    let map = LockFreeGenerationMap::try_from_entries(
        (0..entries).map(|index| {
            let value = u64::try_from(index).expect("entry index must fit u64");
            (binary_key_array(value), value)
        }),
        entries.div_ceil(100),
    )
    .unwrap();
    if with_churn {
        for index in 0..entries.div_ceil(100) {
            let value = u64::try_from(index).expect("entry index must fit u64");
            map.update(&binary_key_array(value), |current| current + 1)
                .unwrap();
        }
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "lockfree-generation-components: base_entries={}, overlay_records={}, \
         layer_depth={}, arena_allocated_bytes={}, slot_bytes={}",
        components.current.base.len,
        components.current.overlay_records,
        components.layer_depth,
        components.current.base.arena_allocated_bytes,
        components.current.base.slot_bytes,
    );
    let name = if with_churn {
        "lockfree-generation-frozen-1pct-overlay-binary32"
    } else {
        "lockfree-generation-frozen-binary32"
    };
    print_row(name, entries, stats);
    black_box(&map);
}

fn print_lockfree_atomic_generation(entries: usize, with_churn: bool) {
    let region = Region::new(GLOBAL);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..entries).map(|index| {
            let value = u64::try_from(index).expect("entry index must fit u64");
            (
                binary_key_array(value),
                NonMaxU64::new(value).expect("entry index must not be u64::MAX"),
            )
        }),
        entries.div_ceil(100),
    )
    .unwrap();
    if with_churn {
        for index in 0..entries.div_ceil(100) {
            let value = u64::try_from(index).expect("entry index must fit u64");
            map.update(&binary_key_array(value), |current| {
                NonMaxU64::new(current.get() + 1).expect("probe values remain representable")
            })
            .unwrap();
        }
    }
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "lockfree-atomic-generation-components: base_entries={}, overlay_records={}, \
         layer_depth={}, arena_allocated_bytes={}, slot_bytes={}",
        components.current.base.len,
        components.current.overlay_records,
        components.layer_depth,
        components.current.base.arena_allocated_bytes,
        components.current.base.slot_bytes,
    );
    let name = if with_churn {
        "lockfree-atomic-generation-frozen-1pct-overlay-binary32"
    } else {
        "lockfree-atomic-generation-frozen-binary32"
    };
    print_row(name, entries, stats);
    black_box(&map);
}

fn print_lockfree_atomic_after_half_delete(entries: usize, rebuild: bool) {
    let region = Region::new(GLOBAL);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..entries).map(|index| {
            let value = u64::try_from(index).expect("entry index must fit u64");
            (
                binary_key_array(value),
                NonMaxU64::new(value).expect("entry index must not be u64::MAX"),
            )
        }),
        entries.div_ceil(100),
    )
    .unwrap();
    for index in (0..entries).step_by(2) {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.remove(&binary_key_array(value)).unwrap();
    }
    if rebuild {
        map.rebuild(0).unwrap();
    }
    let stats = region.change();
    let name = if rebuild {
        "lockfree-atomic-generation-after-50pct-delete-rebuild"
    } else {
        "lockfree-atomic-generation-after-50pct-delete"
    };
    print_row(name, map.len(), stats);
    black_box(&map);
}

fn print_lockfree_atomic_accelerated_generation(
    entries: usize,
    policy: AtomicGenerationBaseFilter,
    policy_name: &str,
) {
    let region = Region::new(GLOBAL);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        (0..entries).map(|index| {
            let value = u64::try_from(index).expect("entry index must fit u64");
            (
                binary_key_array(value),
                NonMaxU64::new(value).expect("entry index must not be u64::MAX"),
            )
        }),
        entries.div_ceil(100),
        AtomicGenerationOverlay::AtomicFixed32,
        policy,
    )
    .unwrap();
    let stats = region.change();
    let components = map.stats();
    eprintln!(
        "lockfree-atomic-{policy_name}-components: base_entries={}, filter_bytes={}",
        components.current.base.len, components.base_filter_bytes,
    );
    print_row(
        &format!("lockfree-atomic-generation-{policy_name}-frozen-binary32"),
        entries,
        stats,
    );
    black_box(&map);
}

fn print_lockfree_atomic_overlay(
    entries: usize,
    key_bytes: usize,
    overlay: AtomicGenerationOverlay,
) {
    assert!((8..=128).contains(&key_bytes));
    let region = Region::new(GLOBAL);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        entries,
        overlay,
    )
    .unwrap();
    for index in 0..entries as u64 {
        let key = sized_binary_key_array(index);
        map.insert(&key[..key_bytes], NonMaxU64::new(index).unwrap());
    }
    let stats = region.change();
    let strategy = match overlay {
        AtomicGenerationOverlay::AtomicFixed32 => "atomic32",
        AtomicGenerationOverlay::CompactFixed32 | AtomicGenerationOverlay::CompactSized { .. } => {
            "inline-sized"
        }
        AtomicGenerationOverlay::ArcSwapFixed32 => "arcswap32",
        AtomicGenerationOverlay::Papaya => "boxed-papaya",
    };
    print_row(
        &format!("lockfree-atomic-overlay-{strategy}-binary{key_bytes}"),
        entries,
        stats,
    );
    black_box(&map);
}

fn compact_overlay_mode(key_bytes: usize) -> AtomicGenerationOverlay {
    if key_bytes == 32 {
        AtomicGenerationOverlay::CompactFixed32
    } else {
        AtomicGenerationOverlay::CompactSized {
            key_bytes: u8::try_from(key_bytes).expect("probe key size fits u8"),
        }
    }
}

fn print_packed_arena(entries: usize) {
    let region = Region::new(GLOBAL);
    let key_bytes = entries.checked_mul(32).expect("key byte count overflow");
    let mut arena = PackedKeyArena::with_segment_bytes(key_bytes).unwrap();
    let mut references = Vec::<PackedKeyRef>::with_capacity(entries);
    for key in 0..entries as u64 {
        references.push(arena.insert(&binary_key_array(key)).unwrap());
    }
    let stats = region.change();
    print_row("packed-key-arena32", entries, stats);
    black_box((&arena, &references));
}

fn print_boxed_keys(entries: usize) {
    let region = Region::new(GLOBAL);
    let mut keys = Vec::<Box<[u8]>>::with_capacity(entries);
    for key in 0..entries as u64 {
        keys.push(binary_key(key));
    }
    let stats = region.change();
    print_row("boxed-key-vector32", entries, stats);
    black_box(&keys);
}

fn print_sweep() {
    for entries in [
        100_000, 250_000, 450_000, 458_752, 500_000, 900_000, 917_504, 1_000_000,
    ] {
        print_elastic(entries, 6);
        print_hashbrown(entries);
    }
}

fn print_packed_sweep() {
    for entries in [
        100_000, 250_000, 450_000, 458_752, 500_000, 900_000, 917_504, 1_000_000,
    ] {
        print_packed_binary(entries, 6);
        print_hashbrown_binary(entries);
    }
}

fn print_row(name: &str, entries: usize, stats: Stats) {
    let live_bytes = net_live_bytes(stats);
    #[allow(clippy::cast_precision_loss)]
    let bytes_per_entry = live_bytes as f64 / entries as f64;
    println!(
        "{name},{entries},{live_bytes},{bytes_per_entry:.3},{}",
        stats.allocations
    );
}

fn net_live_bytes(stats: Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn binary_key(index: u64) -> Box<[u8]> {
    binary_key_array(index).to_vec().into_boxed_slice()
}

fn binary_key_array(index: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn sized_binary_key_array(index: u64) -> [u8; 128] {
    let mut key = [0_u8; 128];
    let mut state = index;
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}
