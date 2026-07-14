#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use elastichash::{ElasticConfig, FixedElasticMap, PackedBinaryMap};
use hashbrown::HashMap;

mod support;

use support::{binary_key, scramble};

const LOOKUP_ENTRIES: usize = 1 << 17;
const INSERT_ENTRIES: usize = 1 << 14;
const BINARY_KEY_BYTES: usize = 32;

fn missing_lookups(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("missing_lookup_u64");
    group.throughput(Throughput::Elements(1));

    for exponent in [3, 6] {
        let config = ElasticConfig::new(LOOKUP_ENTRIES)
            .with_reserve_exponent(exponent)
            .unwrap();
        let mut map = FixedElasticMap::new(config);
        for key in 0..LOOKUP_ENTRIES as u64 {
            map.try_insert(scramble(key), key).unwrap();
        }
        let mut cursor = LOOKUP_ENTRIES as u64;
        group.bench_with_input(
            BenchmarkId::new("elastic", format!("reserve_2^-{exponent}")),
            &exponent,
            |bencher, _| {
                bencher.iter(|| {
                    cursor = cursor.wrapping_add(1);
                    map.get(black_box(&scramble(cursor)))
                });
            },
        );
    }

    let mut map = HashMap::with_capacity(LOOKUP_ENTRIES);
    for key in 0..LOOKUP_ENTRIES as u64 {
        map.insert(scramble(key), key);
    }
    let mut cursor = LOOKUP_ENTRIES as u64;
    group.bench_function("hashbrown", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1);
            map.get(black_box(&scramble(cursor)))
        });
    });
    group.finish();
}

fn binary_key_insertions(criterion: &mut Criterion) {
    let entries = 1 << 14;
    let corpus: Vec<Box<[u8]>> = (0..entries as u64)
        .map(|index| binary_key(index, BINARY_KEY_BYTES))
        .collect();
    let mut group = criterion.benchmark_group("bulk_insert_binary_32");
    group.throughput(Throughput::Elements(entries as u64));

    for exponent in [3, 6] {
        let config = ElasticConfig::new(entries)
            .with_reserve_exponent(exponent)
            .unwrap();
        group.bench_with_input(
            BenchmarkId::new("packed-elastic", format!("reserve_2^-{exponent}")),
            &config,
            |bencher, config| {
                bencher.iter_batched(
                    || PackedBinaryMap::new(*config),
                    |mut map| {
                        for (index, key) in corpus.iter().enumerate() {
                            black_box(map.try_insert(key, index as u64).unwrap());
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.bench_function("hashbrown", |bencher| {
        bencher.iter_batched(
            || HashMap::with_capacity(entries),
            |mut map| {
                for (index, key) in corpus.iter().enumerate() {
                    black_box(map.insert(key.clone(), index as u64));
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bulk_insertions(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("bulk_insert_u64");
    group.throughput(Throughput::Elements(INSERT_ENTRIES as u64));

    for exponent in [3, 6] {
        let config = ElasticConfig::new(INSERT_ENTRIES)
            .with_reserve_exponent(exponent)
            .unwrap();
        group.bench_with_input(
            BenchmarkId::new("elastic", format!("reserve_2^-{exponent}")),
            &config,
            |bencher, config| {
                bencher.iter_batched(
                    || FixedElasticMap::new(*config),
                    |mut map| {
                        for key in 0..INSERT_ENTRIES as u64 {
                            black_box(map.try_insert(scramble(key), key).unwrap());
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.bench_function("hashbrown", |bencher| {
        bencher.iter_batched(
            || HashMap::with_capacity(INSERT_ENTRIES),
            |mut map| {
                for key in 0..INSERT_ENTRIES as u64 {
                    black_box(map.insert(scramble(key), key));
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn binary_key_lookups(criterion: &mut Criterion) {
    let entries = 1 << 15;
    let corpus: Vec<Box<[u8]>> = (0..entries as u64)
        .map(|index| binary_key(index, BINARY_KEY_BYTES))
        .collect();
    let mut group = criterion.benchmark_group("successful_lookup_binary_32");
    group.throughput(Throughput::Elements(1));

    for exponent in [3, 6] {
        let config = ElasticConfig::new(entries)
            .with_reserve_exponent(exponent)
            .unwrap();
        let mut map = FixedElasticMap::new(config);
        for (index, key) in corpus.iter().enumerate() {
            map.try_insert(key.clone(), index as u64).unwrap();
        }
        let mut cursor = 0_usize;
        group.bench_with_input(
            BenchmarkId::new("elastic", format!("reserve_2^-{exponent}")),
            &exponent,
            |bencher, _| {
                bencher.iter(|| {
                    cursor = cursor.wrapping_add(1) % entries;
                    map.get(black_box(corpus[cursor].as_ref()))
                });
            },
        );

        let mut packed = PackedBinaryMap::new(config);
        for (index, key) in corpus.iter().enumerate() {
            packed.try_insert(key, index as u64).unwrap();
        }
        let mut cursor = 0_usize;
        group.bench_with_input(
            BenchmarkId::new("packed-elastic", format!("reserve_2^-{exponent}")),
            &exponent,
            |bencher, _| {
                bencher.iter(|| {
                    cursor = cursor.wrapping_add(1) % entries;
                    packed.get(black_box(corpus[cursor].as_ref()))
                });
            },
        );
    }

    let mut map = HashMap::with_capacity(entries);
    for (index, key) in corpus.iter().enumerate() {
        map.insert(key.clone(), index as u64);
    }
    let mut cursor = 0_usize;
    group.bench_function("hashbrown", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1) % entries;
            map.get(black_box(corpus[cursor].as_ref()))
        });
    });
    group.finish();
}

fn binary_key_missing_lookups(criterion: &mut Criterion) {
    let entries = 1 << 15;
    let corpus: Vec<Box<[u8]>> = (0..entries as u64)
        .map(|index| binary_key(index, BINARY_KEY_BYTES))
        .collect();
    let misses: Vec<Box<[u8]>> = (entries as u64..(entries * 2) as u64)
        .map(|index| binary_key(index, BINARY_KEY_BYTES))
        .collect();
    let mut group = criterion.benchmark_group("missing_lookup_binary_32");
    group.throughput(Throughput::Elements(1));

    for exponent in [3, 6] {
        let config = ElasticConfig::new(entries)
            .with_reserve_exponent(exponent)
            .unwrap();
        let mut packed = PackedBinaryMap::new(config);
        for (index, key) in corpus.iter().enumerate() {
            packed.try_insert(key, index as u64).unwrap();
        }
        let mut cursor = 0_usize;
        group.bench_with_input(
            BenchmarkId::new("packed-elastic", format!("reserve_2^-{exponent}")),
            &exponent,
            |bencher, _| {
                bencher.iter(|| {
                    cursor = cursor.wrapping_add(1) % entries;
                    packed.get(black_box(misses[cursor].as_ref()))
                });
            },
        );
    }

    let mut map = HashMap::with_capacity(entries);
    for (index, key) in corpus.iter().enumerate() {
        map.insert(key.clone(), index as u64);
    }
    let mut cursor = 0_usize;
    group.bench_function("hashbrown", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1) % entries;
            map.get(black_box(misses[cursor].as_ref()))
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    missing_lookups,
    bulk_insertions,
    binary_key_lookups,
    binary_key_missing_lookups,
    binary_key_insertions
);
criterion_main!(benches);
