//! Unified hit, miss, update, delete, and insert comparison.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hint::black_box;
use std::time::Instant;

use hashbrown::HashMap;
use packedgen::{
    BucketPackedMap, ElasticConfig, Fixed32SoaMap, FrozenPackedMap, HybridPackedMap, InsertOutcome,
    PackedBinaryMap, PackedSwissMap, RouteCacheBudget, SegmentedSwissMap,
};

const SAMPLES: usize = 5;

#[allow(clippy::too_many_lines)]
fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000);
    let lookups = argument(&mut arguments, 1_000_000);
    let batch = argument(&mut arguments, (entries / 100).max(1));
    assert!(entries >= batch * SAMPLES * 2);

    let keys: Vec<_> = (0..entries as u64).map(binary_key).collect();
    let misses: Vec<_> = (entries as u64..entries.saturating_mul(2) as u64)
        .map(binary_key)
        .collect();
    let final_capacity = entries.saturating_add(batch.saturating_mul(SAMPLES));

    let mut results = Vec::new();

    results.push(probe_mutable(
        "hashbrown-inline32",
        build_hashbrown(&keys, final_capacity),
        build_hashbrown(&keys, final_capacity),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| map.insert(*key, value).is_some(),
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "packed-swiss",
        build_packed_swiss(&keys, final_capacity),
        build_packed_swiss(&keys, final_capacity),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "segmented-swiss-balanced",
        build_segmented_swiss(&keys, final_capacity),
        build_segmented_swiss(&keys, final_capacity),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "packed-elastic-adaptive",
        build_packed_elastic(&keys, final_capacity, RouteCacheBudget::Adaptive),
        build_packed_elastic(&keys, final_capacity, RouteCacheBudget::Adaptive),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "packed-elastic-read",
        build_packed_elastic(&keys, final_capacity, RouteCacheBudget::ReadOptimized),
        build_packed_elastic(&keys, final_capacity, RouteCacheBudget::ReadOptimized),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "fixed32-soa-balanced",
        build_fixed32(&keys, final_capacity),
        build_fixed32(&keys, final_capacity),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(*key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "cacheline-bucket",
        build_bucket(&keys, final_capacity),
        build_bucket(&keys, final_capacity),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    results.push(probe_mutable(
        "hybrid-filtered-8bit",
        build_hybrid(&keys, batch * SAMPLES * 2),
        build_hybrid(&keys, batch * SAMPLES),
        &keys,
        &misses,
        lookups,
        batch,
        |map, key| map.get(key).copied(),
        |map, key, value| {
            matches!(
                map.try_insert(key, value).unwrap(),
                InsertOutcome::Replaced(_)
            )
        },
        |map, key| map.remove(key).is_some(),
    ));

    let frozen = FrozenPackedMap::try_from_entries(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key, index as u64)),
    )
    .unwrap();
    results.push(OperationMeasurement {
        name: "frozen-ptrhash",
        read_hit_ns: lookup_ns(&keys, lookups, |key| frozen.get(key).copied(), true),
        read_miss_ns: lookup_ns(&misses, lookups, |key| frozen.get(key).copied(), false),
        update_hit_ns: None,
        delete_hit_ns: None,
        delete_miss_ns: None,
        insert_miss_ns: None,
    });

    println!(
        "implementation,entries,lookups,mutation_batch,read_hit_ns,read_miss_ns,\
         update_hit_ns,delete_hit_ns,delete_miss_ns,insert_miss_ns"
    );
    for result in results {
        println!(
            "{},{entries},{lookups},{batch},{:.3},{:.3},{},{},{},{}",
            result.name,
            result.read_hit_ns,
            result.read_miss_ns,
            optional(result.update_hit_ns),
            optional(result.delete_hit_ns),
            optional(result.delete_miss_ns),
            optional(result.insert_miss_ns),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn probe_mutable<M>(
    name: &'static str,
    mut map: M,
    mut insert_map: M,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    lookups: usize,
    batch: usize,
    get: impl Fn(&M, &[u8; 32]) -> Option<u64>,
    insert: impl Fn(&mut M, &[u8; 32], u64) -> bool,
    remove: impl Fn(&mut M, &[u8; 32]) -> bool,
) -> OperationMeasurement {
    let read_hit_ns = lookup_ns(keys, lookups, |key| get(&map, key), true);
    let read_miss_ns = lookup_ns(misses, lookups, |key| get(&map, key), false);

    let update_hit_ns = mutation_ns(batch, |sample, operation| {
        let index = sample * batch + operation;
        insert(
            &mut map,
            &keys[index],
            (sample as u64) << 48 | operation as u64,
        )
    });
    let delete_miss_ns = mutation_ns(batch, |sample, operation| {
        let index = sample * batch + operation;
        !remove(&mut map, &misses[index])
    });
    let delete_hit_ns = mutation_ns(batch, |sample, operation| {
        let index = (SAMPLES + sample) * batch + operation;
        remove(&mut map, &keys[index])
    });
    let insert_miss_ns = mutation_ns(batch, |sample, operation| {
        let index = sample * batch + operation;
        !insert(
            &mut insert_map,
            &misses[index],
            (sample as u64) << 48 | operation as u64,
        )
    });

    OperationMeasurement {
        name,
        read_hit_ns,
        read_miss_ns,
        update_hit_ns: Some(update_hit_ns),
        delete_hit_ns: Some(delete_hit_ns),
        delete_miss_ns: Some(delete_miss_ns),
        insert_miss_ns: Some(insert_miss_ns),
    }
}

fn lookup_ns(
    keys: &[[u8; 32]],
    operations: usize,
    mut get: impl FnMut(&[u8; 32]) -> Option<u64>,
    expect_hit: bool,
) -> f64 {
    let mut samples = [0_u128; SAMPLES];
    for elapsed in &mut samples {
        let started = Instant::now();
        let mut observed = 0_usize;
        let mut checksum = 0_u64;
        for operation in 0..operations {
            let index = scramble(operation) % keys.len();
            if let Some(value) = black_box(get(black_box(&keys[index]))) {
                observed += 1;
                checksum ^= value;
            }
        }
        black_box(checksum);
        let duration = started.elapsed().as_nanos();
        assert_eq!(observed, usize::from(expect_hit) * operations);
        *elapsed = duration;
    }
    median_ns(samples, operations)
}

fn mutation_ns(operations: usize, mut operation: impl FnMut(usize, usize) -> bool) -> f64 {
    let mut samples = [0_u128; SAMPLES];
    for (sample, elapsed) in samples.iter_mut().enumerate() {
        let started = Instant::now();
        let mut expected = 0_usize;
        for index in 0..operations {
            expected += usize::from(black_box(operation(sample, index)));
        }
        let duration = started.elapsed().as_nanos();
        assert_eq!(expected, operations);
        *elapsed = duration;
    }
    median_ns(samples, operations)
}

fn median_ns(mut samples: [u128; SAMPLES], operations: usize) -> f64 {
    samples.sort_unstable();
    samples[SAMPLES / 2] as f64 / operations as f64
}

fn build_hashbrown(keys: &[[u8; 32]], capacity: usize) -> HashMap<[u8; 32], u64> {
    let mut map = HashMap::with_capacity(capacity);
    for (index, key) in keys.iter().enumerate() {
        map.insert(*key, index as u64);
    }
    map
}

fn build_packed_swiss(keys: &[[u8; 32]], capacity: usize) -> PackedSwissMap<u64> {
    let mut map = PackedSwissMap::try_with_capacity_and_key_bytes(capacity, capacity * 32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    map
}

fn build_segmented_swiss(keys: &[[u8; 32]], capacity: usize) -> SegmentedSwissMap<u64> {
    let mut map =
        SegmentedSwissMap::try_with_capacity_and_key_bytes(capacity, capacity * 32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    map
}

fn build_packed_elastic(
    keys: &[[u8; 32]],
    capacity: usize,
    route_budget: RouteCacheBudget,
) -> PackedBinaryMap<u64> {
    let config = ElasticConfig::new(capacity)
        .with_reserve_exponent(6)
        .unwrap()
        .with_route_cache_budget(route_budget);
    let mut map = PackedBinaryMap::new(config);
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    map
}

fn build_fixed32(keys: &[[u8; 32]], capacity: usize) -> Fixed32SoaMap<u64> {
    let mut map = Fixed32SoaMap::with_capacity(capacity);
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(*key, index as u64).unwrap();
    }
    map
}

fn build_bucket(keys: &[[u8; 32]], capacity: usize) -> BucketPackedMap<u64> {
    let mut map = BucketPackedMap::new(capacity);
    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index as u64).unwrap();
    }
    map
}

fn build_hybrid(keys: &[[u8; 32]], delta_capacity: usize) -> HybridPackedMap<u64> {
    let base = FrozenPackedMap::try_from_entries(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key, index as u64)),
    )
    .unwrap();
    HybridPackedMap::with_delta_capacity(base, delta_capacity)
}

fn optional(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.3}"))
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected integer"))
}

fn scramble(mut value: usize) -> usize {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb_352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846c_a68b);
    value ^ (value >> 16)
}

fn binary_key(index: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct OperationMeasurement {
    name: &'static str,
    read_hit_ns: f64,
    read_miss_ns: f64,
    update_hit_ns: Option<f64>,
    delete_hit_ns: Option<f64>,
    delete_miss_ns: Option<f64>,
    insert_miss_ns: Option<f64>,
}
