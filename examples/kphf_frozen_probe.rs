//! Paired exact frozen-map comparison for `PtrHash` and cache-line k-PtrHash.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use packedgen::{KPhfAtomicU64Map, NonMaxU64, PtrHashAtomicU64Map};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(2);
    let operations = argument(&mut arguments, 3_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    let build_samples = argument(&mut arguments, 3).max(1);
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let misses = (entries as u64..entries.saturating_mul(2) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();

    let mut build_times = [
        Vec::with_capacity(build_samples),
        Vec::with_capacity(build_samples),
    ];
    for sample in 0..build_samples {
        for offset in 0..Backend::ALL.len() {
            let index = (sample + offset) % Backend::ALL.len();
            let started = Instant::now();
            black_box(build_map(Backend::ALL[index], &keys));
            build_times[index].push(started.elapsed());
        }
    }

    let ptrhash = build_ptrhash(&keys);
    let kphf = build_kphf(&keys);
    let maps = [MapRef::PtrHash(&ptrhash), MapRef::KPhf(&kphf)];

    for (index, key) in keys.iter().enumerate() {
        for map in maps {
            assert_eq!(map.get(key), Some(index as u64));
        }
    }
    for key in misses.iter().take(10_000) {
        for map in maps {
            assert_eq!(map.get(key), None);
        }
    }

    let mut hit_times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
    let mut miss_times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
    for sample in 0..samples {
        for offset in 0..Backend::ALL.len() {
            let index = (sample + offset) % Backend::ALL.len();
            hit_times[index].push(measure(maps[index], &keys, operations, threads));
            miss_times[index].push(measure(maps[index], &misses, operations, threads));
        }
    }
    let hit_changes = paired_changes(&hit_times);
    let miss_changes = paired_changes(&miss_times);

    println!(
        "backend,entries,operations,threads,samples,build_samples,build_ns_per_entry,bytes_per_entry,index_bits_per_entry,load_factor,bumped_pct,hit_mops,hit_vs_ptr_pct,miss_mops,miss_vs_ptr_pct"
    );
    for index in 0..Backend::ALL.len() {
        build_times[index].sort_unstable();
        hit_times[index].sort_unstable();
        miss_times[index].sort_unstable();
        let build = median(&build_times[index]);
        let hit = median(&hit_times[index]);
        let miss = median(&miss_times[index]);
        println!(
            "{},{entries},{operations},{threads},{samples},{build_samples},{:.3},{:.3},{:.3},{:.6},{:.4},{:.3},{:.3},{:.3},{:.3}",
            Backend::ALL[index].name(),
            ns_per_entry(build, entries),
            maps[index].retained_bytes() as f64 / entries as f64,
            maps[index].index_bits_per_entry(),
            maps[index].load_factor(),
            maps[index].bumped_entries() as f64 * 100.0 / entries as f64,
            throughput(operations, hit),
            hit_changes[index],
            throughput(operations, miss),
            miss_changes[index],
        );
    }
}

fn build_map(backend: Backend, keys: &[[u8; 32]]) -> usize {
    match backend {
        Backend::PtrHash => black_box(build_ptrhash(keys)).stats().len,
        Backend::KPhf => black_box(build_kphf(keys)).stats().len,
    }
}

fn build_ptrhash(keys: &[[u8; 32]]) -> PtrHashAtomicU64Map {
    PtrHashAtomicU64Map::try_from_entries(keys.iter().enumerate().map(|(index, key)| {
        (
            key,
            NonMaxU64::new(index as u64).expect("benchmark index is representable"),
        )
    }))
    .unwrap()
}

fn build_kphf(keys: &[[u8; 32]]) -> KPhfAtomicU64Map {
    KPhfAtomicU64Map::try_from_entries(keys.iter().enumerate().map(|(index, key)| {
        (
            key,
            NonMaxU64::new(index as u64).expect("benchmark index is representable"),
        )
    }))
    .unwrap()
}

#[derive(Clone, Copy)]
enum MapRef<'a> {
    PtrHash(&'a PtrHashAtomicU64Map),
    KPhf(&'a KPhfAtomicU64Map),
}

impl MapRef<'_> {
    fn get(self, key: &[u8]) -> Option<u64> {
        match self {
            Self::PtrHash(map) => map.get(key).map(NonMaxU64::get),
            Self::KPhf(map) => map.get(key).map(NonMaxU64::get),
        }
    }

    fn retained_bytes(self) -> usize {
        match self {
            Self::PtrHash(map) => {
                let stats = map.stats();
                stats
                    .arena_allocated_bytes
                    .saturating_add(stats.slot_bytes)
                    .saturating_add(index_bytes(stats.len, stats.index_bits_per_entry))
            }
            Self::KPhf(map) => {
                let stats = map.stats();
                stats
                    .arena_allocated_bytes
                    .saturating_add(stats.slot_bytes)
                    .saturating_add(index_bytes(stats.len, stats.index_bits_per_entry))
            }
        }
    }

    fn index_bits_per_entry(self) -> f64 {
        match self {
            Self::PtrHash(map) => map.stats().index_bits_per_entry,
            Self::KPhf(map) => map.stats().index_bits_per_entry,
        }
    }

    fn load_factor(self) -> f64 {
        match self {
            Self::PtrHash(_) => 1.0,
            Self::KPhf(map) => map.stats().load_factor,
        }
    }

    fn bumped_entries(self) -> usize {
        match self {
            Self::PtrHash(_) => 0,
            Self::KPhf(map) => map.stats().bumped_entries,
        }
    }
}

fn measure(map: MapRef<'_>, keys: &[[u8; 32]], operations: usize, threads: usize) -> Duration {
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let start = &start;
            let done = &done;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    let index = mix(operation as u64) as usize % keys.len();
                    black_box(map.get(&keys[index]));
                }
                done.wait();
            });
        }
        let started = Instant::now();
        start.wait();
        done.wait();
        started.elapsed()
    })
}

fn paired_changes(measurements: &[Vec<Duration>; 2]) -> [f64; 2] {
    let mut changes = measurements[0]
        .iter()
        .zip(&measurements[1])
        .map(|(control, challenger)| {
            (control.as_secs_f64() / challenger.as_secs_f64() - 1.0) * 100.0
        })
        .collect::<Vec<_>>();
    changes.sort_unstable_by(f64::total_cmp);
    [0.0, changes[changes.len() / 2]]
}

fn index_bytes(entries: usize, bits_per_entry: f64) -> usize {
    ((entries as f64 * bits_per_entry) / 8.0).ceil() as usize
}

fn median(samples: &[Duration]) -> Duration {
    samples[samples.len() / 2]
}

fn throughput(operations: usize, duration: Duration) -> f64 {
    operations as f64 / duration.as_secs_f64() / 1_000_000.0
}

fn ns_per_entry(duration: Duration, entries: usize) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / entries as f64
}

#[derive(Clone, Copy)]
enum Backend {
    PtrHash,
    KPhf,
}

impl Backend {
    const ALL: [Self; 2] = [Self::PtrHash, Self::KPhf];

    const fn name(self) -> &'static str {
        match self {
            Self::PtrHash => "ptrhash-atomic",
            Self::KPhf => "kphf8-cacheline-soa",
        }
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = value;
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
