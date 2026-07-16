//! Isolated cost of the current writer-hash plus frozen-digest schedule.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hash::BuildHasher;
use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use ptr_hash::hash::{Gx128, KeyHasher, Xxh3_128};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let keys_count = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 30_000_000).max(1);
    let threads = argument(&mut arguments, 1).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    let keys = (0..keys_count as u64).map(binary_key).collect::<Vec<_>>();
    let writer = DefaultHashBuilder::default();
    let mut measurements = Workload::ALL.map(|_| Vec::with_capacity(samples));

    for sample in 0..samples {
        for offset in 0..Workload::ALL.len() {
            let index = (sample + offset) % Workload::ALL.len();
            measurements[index].push(measure(
                Workload::ALL[index],
                &keys,
                operations,
                threads,
                &writer,
            ));
        }
    }

    println!("workload,keys,operations,threads,samples,median_ns_per_op,throughput_mops");
    for (workload, samples) in Workload::ALL.into_iter().zip(&mut measurements) {
        samples.sort_unstable();
        let median = samples[samples.len() / 2];
        println!(
            "{},{keys_count},{operations},{threads},{},{:.3},{:.3}",
            workload.name(),
            samples.len(),
            median.as_secs_f64() * 1e9 / operations as f64,
            operations as f64 / median.as_secs_f64() / 1e6,
        );
    }
}

fn measure(
    workload: Workload,
    keys: &[[u8; 32]],
    operations: usize,
    threads: usize,
    writer: &DefaultHashBuilder,
) -> Duration {
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
                    let key = &keys[mix(operation as u64) as usize % keys.len()];
                    match workload {
                        Workload::Writer64 => {
                            black_box(writer.hash_one(key));
                        }
                        Workload::Frozen128 => {
                            black_box(<Gx128 as KeyHasher<[u8]>>::hash(key, 0));
                        }
                        Workload::Both => {
                            black_box(writer.hash_one(key));
                            black_box(<Gx128 as KeyHasher<[u8]>>::hash(key, 0));
                        }
                        Workload::SharedGxLow => {
                            let digest = <Gx128 as KeyHasher<[u8]>>::hash(key, 0);
                            black_box((digest as u64) & 4_095);
                            black_box(digest);
                        }
                        Workload::SharedXxhLow => {
                            let digest = <Xxh3_128 as KeyHasher<[u8]>>::hash(key, 0);
                            black_box((digest as u64) & 4_095);
                            black_box(digest);
                        }
                    }
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

#[derive(Clone, Copy)]
enum Workload {
    Writer64,
    Frozen128,
    Both,
    SharedGxLow,
    SharedXxhLow,
}

impl Workload {
    const ALL: [Self; 5] = [
        Self::Writer64,
        Self::Frozen128,
        Self::Both,
        Self::SharedGxLow,
        Self::SharedXxhLow,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Writer64 => "writer_hash64",
            Self::Frozen128 => "frozen_gx128",
            Self::Both => "current_both",
            Self::SharedGxLow => "shared_gx128_low64",
            Self::SharedXxhLow => "shared_xxh3_128_low64",
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
