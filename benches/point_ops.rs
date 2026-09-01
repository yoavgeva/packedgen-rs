#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hashbrown::HashMap;
use packedgen::{ElasticConfig, FixedElasticMap};

mod support;

use support::{LARGE_ENTRIES as ENTRIES, scramble};

fn point_lookups(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("successful_lookup_u64");
    group.throughput(Throughput::Elements(1));

    for reserve_exponent in [3, 6] {
        let config = ElasticConfig::new(ENTRIES)
            .with_reserve_exponent(reserve_exponent)
            .unwrap();
        let mut map = FixedElasticMap::new(config);
        for key in 0..ENTRIES as u64 {
            map.try_insert(scramble(key), key).unwrap();
        }

        let mut cursor = 0_u64;
        group.bench_with_input(
            BenchmarkId::new("elastic", format!("reserve_2^-{reserve_exponent}")),
            &reserve_exponent,
            |bencher, _| {
                bencher.iter(|| {
                    cursor = cursor.wrapping_add(1);
                    map.get(black_box(&scramble(cursor % ENTRIES as u64)))
                });
            },
        );
    }

    let mut baseline = HashMap::with_capacity(ENTRIES);
    for key in 0..ENTRIES as u64 {
        baseline.insert(scramble(key), key);
    }
    let mut cursor = 0_u64;
    group.bench_function("hashbrown", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1);
            baseline.get(black_box(&scramble(cursor % ENTRIES as u64)))
        });
    });

    group.finish();
}

criterion_group!(benches, point_lookups);
criterion_main!(benches);
