//! Same-requested-RAM working-set and successful-lookup comparison.
//!
//! This is a system-oriented probe, not a substitute for the pinned benchmark
//! protocol. It counts requested live allocation, excluding allocator metadata,
//! fragmentation, process overhead, and RSS effects.

use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use hashbrown::HashMap;
use packedgen::{
    ElasticConfig, FrozenPackedMap, PackedBinaryMap, RouteCacheBudget, SegmentedLoad,
    SegmentedSwissMap,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let budget_mib = arguments.next().map_or(64, |value| {
        value
            .parse::<usize>()
            .expect("budget MiB must be an integer")
    });
    let lookups = arguments.next().map_or(1_000_000, |value| {
        value.parse::<usize>().expect("lookups must be an integer")
    });
    let samples = arguments.next().map_or(5, |value| {
        value.parse::<usize>().expect("samples must be an integer")
    });
    assert!(budget_mib > 0, "budget MiB must be positive");
    assert!(lookups > 0, "lookups must be positive");
    assert!(samples > 0, "samples must be positive");
    let budget = budget_mib
        .checked_mul(1024 * 1024)
        .expect("byte budget overflow");

    let packed_entries = max_entries_within_budget(budget, measure_packed_bytes);
    let frozen_entries = max_entries_within_budget(budget, measure_frozen_bytes);
    let hashbrown_entries = max_entries_within_budget(budget, measure_hashbrown_bytes);
    let segmented_compact_entries =
        max_entries_within_budget(budget, measure_segmented_compact_bytes);
    let segmented_balanced_entries =
        max_entries_within_budget(budget, measure_segmented_balanced_bytes);
    let segmented_fast_entries = max_entries_within_budget(budget, measure_segmented_fast_bytes);
    assert!(
        packed_entries > 0
            && frozen_entries > 0
            && hashbrown_entries > 0
            && segmented_compact_entries > 0
            && segmented_balanced_entries > 0
            && segmented_fast_entries > 0,
        "budget must fit at least one entry in every implementation"
    );
    let packed = run_packed(packed_entries, lookups, samples);
    let frozen = run_frozen(frozen_entries, lookups, samples);
    let hashbrown = run_hashbrown(hashbrown_entries, lookups, samples);
    let segmented_compact = run_segmented(
        segmented_compact_entries,
        lookups,
        samples,
        SegmentedLoad::Compact,
    );
    let segmented_balanced = run_segmented(
        segmented_balanced_entries,
        lookups,
        samples,
        SegmentedLoad::Balanced,
    );
    let segmented_fast = run_segmented(
        segmented_fast_entries,
        lookups,
        samples,
        SegmentedLoad::Fast,
    );

    println!(
        "implementation,budget_bytes,entries,live_bytes,budget_utilization,lookups,samples,median_ns_per_lookup,lookups_per_second"
    );
    print_row(
        "packed-elastic-binary32-2^-6",
        budget,
        lookups,
        samples,
        packed,
    );
    print_row("frozen-ptrhash-binary32", budget, lookups, samples, frozen);
    print_row("hashbrown-binary32", budget, lookups, samples, hashbrown);
    print_row(
        "segmented-swiss-compact-binary32",
        budget,
        lookups,
        samples,
        segmented_compact,
    );
    print_row(
        "segmented-swiss-balanced-binary32",
        budget,
        lookups,
        samples,
        segmented_balanced,
    );
    print_row(
        "segmented-swiss-fast-binary32",
        budget,
        lookups,
        samples,
        segmented_fast,
    );
}

fn max_entries_within_budget(budget: usize, measure: fn(usize) -> usize) -> usize {
    let mut within = 0_usize;
    let mut beyond = 1_usize;
    while measure(beyond) <= budget {
        within = beyond;
        beyond = beyond.checked_mul(2).expect("entry search overflow");
    }
    while beyond - within > 1 {
        let candidate = within + (beyond - within) / 2;
        if measure(candidate) <= budget {
            within = candidate;
        } else {
            beyond = candidate;
        }
    }
    within
}

fn measure_packed_bytes(entries: usize) -> usize {
    let region = Region::new(GLOBAL);
    let map = build_packed(entries);
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn measure_hashbrown_bytes(entries: usize) -> usize {
    let region = Region::new(GLOBAL);
    let map = build_hashbrown(entries);
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn measure_frozen_bytes(entries: usize) -> usize {
    let region = Region::new(GLOBAL);
    let map = build_frozen(entries);
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn measure_segmented_compact_bytes(entries: usize) -> usize {
    measure_segmented_bytes(entries, SegmentedLoad::Compact)
}

fn measure_segmented_balanced_bytes(entries: usize) -> usize {
    measure_segmented_bytes(entries, SegmentedLoad::Balanced)
}

fn measure_segmented_fast_bytes(entries: usize) -> usize {
    measure_segmented_bytes(entries, SegmentedLoad::Fast)
}

fn measure_segmented_bytes(entries: usize, load: SegmentedLoad) -> usize {
    let region = Region::new(GLOBAL);
    let map = build_segmented(entries, load);
    let bytes = net_live_bytes(region.change());
    black_box(&map);
    bytes
}

fn run_packed(entries: usize, lookups: usize, samples: usize) -> ProbeResult {
    let region = Region::new(GLOBAL);
    let map = build_packed(entries);
    let live_bytes = net_live_bytes(region.change());
    let elapsed = median_hit_time(entries, lookups, samples, |key| map.get(key).copied());
    black_box(&map);
    ProbeResult {
        entries,
        live_bytes,
        elapsed,
    }
}

fn run_hashbrown(entries: usize, lookups: usize, samples: usize) -> ProbeResult {
    let region = Region::new(GLOBAL);
    let map = build_hashbrown(entries);
    let live_bytes = net_live_bytes(region.change());
    let elapsed = median_hit_time(entries, lookups, samples, |key| map.get(key).copied());
    black_box(&map);
    ProbeResult {
        entries,
        live_bytes,
        elapsed,
    }
}

fn run_frozen(entries: usize, lookups: usize, samples: usize) -> ProbeResult {
    let region = Region::new(GLOBAL);
    let map = build_frozen(entries);
    let live_bytes = net_live_bytes(region.change());
    let elapsed = median_hit_time(entries, lookups, samples, |key| map.get(key).copied());
    black_box(&map);
    ProbeResult {
        entries,
        live_bytes,
        elapsed,
    }
}

fn run_segmented(
    entries: usize,
    lookups: usize,
    samples: usize,
    load: SegmentedLoad,
) -> ProbeResult {
    let region = Region::new(GLOBAL);
    let map = build_segmented(entries, load);
    let live_bytes = net_live_bytes(region.change());
    let elapsed = median_hit_time(entries, lookups, samples, |key| map.get(key).copied());
    black_box(&map);
    ProbeResult {
        entries,
        live_bytes,
        elapsed,
    }
}

fn build_packed(entries: usize) -> PackedBinaryMap<u64> {
    let config = ElasticConfig::new(entries)
        .with_reserve_exponent(6)
        .expect("reserve exponent is valid")
        .with_route_cache_budget(RouteCacheBudget::ReadOptimized);
    let mut map = PackedBinaryMap::new(config);
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key(value), value)
            .expect("fixed packed epoch must fit configured entries");
    }
    map
}

fn build_hashbrown(entries: usize) -> HashMap<Box<[u8]>, u64> {
    let mut map = HashMap::with_capacity(entries);
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.insert(binary_key(value).to_vec().into_boxed_slice(), value);
    }
    map
}

fn build_frozen(entries: usize) -> FrozenPackedMap<u64> {
    FrozenPackedMap::try_from_entries((0..entries).map(|index| {
        let value = u64::try_from(index).expect("entry index must fit u64");
        (binary_key(value), value)
    }))
    .expect("unique generated keys must build a frozen map")
}

fn build_segmented(entries: usize, load: SegmentedLoad) -> SegmentedSwissMap<u64> {
    let key_bytes = entries.checked_mul(32).expect("key byte count overflow");
    let mut map = SegmentedSwissMap::try_with_capacity_key_bytes_and_load(entries, key_bytes, load)
        .expect("segmented SwissTable allocation must fit");
    for index in 0..entries {
        let value = u64::try_from(index).expect("entry index must fit u64");
        map.try_insert(&binary_key(value), value)
            .expect("segmented SwissTable insertion must succeed");
    }
    map
}

fn median_hit_time(
    entries: usize,
    lookups: usize,
    samples: usize,
    mut get: impl FnMut(&[u8]) -> Option<u64>,
) -> Duration {
    assert!(
        entries > 0,
        "positive RAM budget must fit at least one entry"
    );
    run_hits(entries, lookups.min(100_000), &mut get);
    let mut elapsed = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        run_hits(entries, lookups, &mut get);
        elapsed.push(started.elapsed());
    }
    elapsed.sort_unstable();
    elapsed[elapsed.len() / 2]
}

fn run_hits(entries: usize, lookups: usize, get: &mut impl FnMut(&[u8]) -> Option<u64>) {
    let mut checksum = 0_u64;
    for query in 0..lookups {
        let query = u64::try_from(query).expect("lookup index must fit u64");
        let entry_count = u64::try_from(entries).expect("entry count must fit u64");
        let index = mix(query) % entry_count;
        checksum ^= get(&binary_key(index)).expect("generated hit key must exist");
    }
    black_box(checksum);
}

#[derive(Clone, Copy)]
struct ProbeResult {
    entries: usize,
    live_bytes: usize,
    elapsed: Duration,
}

fn print_row(name: &str, budget: usize, lookups: usize, samples: usize, result: ProbeResult) {
    #[allow(clippy::cast_precision_loss)]
    let utilization = result.live_bytes as f64 / budget as f64;
    #[allow(clippy::cast_precision_loss)]
    let nanoseconds = result.elapsed.as_nanos() as f64 / lookups as f64;
    #[allow(clippy::cast_precision_loss)]
    let throughput = lookups as f64 / result.elapsed.as_secs_f64();
    println!(
        "{name},{budget},{},{},{utilization:.6},{lookups},{samples},{nanoseconds:.3},{throughput:.0}",
        result.entries, result.live_bytes
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

fn binary_key(index: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}
