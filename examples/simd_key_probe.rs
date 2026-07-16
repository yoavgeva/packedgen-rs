//! Exact 32-byte key comparison: native, scalar-word, and portable SIMD paths.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use wide::u64x4;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let operations = argument(&mut arguments, 2_000_000).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    let workload_filter = arguments.next();
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let mut misses_first = keys.clone();
    let mut misses_last = keys.clone();
    for key in &mut misses_first {
        key[0] ^= 0xff;
    }
    for key in &mut misses_last {
        key[31] ^= 0xff;
    }

    println!(
        "strategy,workload,entries,operations,samples,median_ns,ns_per_compare,throughput_mops,paired_vs_native_pct"
    );
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| filter != "all" && filter != workload.name())
        {
            continue;
        }
        let mut measurements = vec![Vec::with_capacity(samples); Strategy::ALL.len()];
        for sample in 0..samples {
            for offset in 0..Strategy::ALL.len() {
                let index = (sample + offset) % Strategy::ALL.len();
                measurements[index].push(measure(
                    Strategy::ALL[index],
                    workload,
                    &keys,
                    &misses_first,
                    &misses_last,
                    operations,
                ));
            }
        }
        for samples in &mut measurements {
            samples.sort_unstable();
        }
        let native = median(&measurements[0]);
        for (strategy, samples) in Strategy::ALL.into_iter().zip(measurements) {
            let duration = median(&samples);
            let ns = duration.as_secs_f64() * 1e9 / operations as f64;
            println!(
                "{},{},{entries},{operations},{},{},{ns:.3},{:.3},{:.3}",
                strategy.name(),
                workload.name(),
                samples.len(),
                duration.as_nanos(),
                1_000.0 / ns,
                (native.as_secs_f64() / duration.as_secs_f64() - 1.0) * 100.0,
            );
        }
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    Native,
    U128Pairs,
    U64Xor,
    WideU64x4,
}

impl Strategy {
    const ALL: [Self; 4] = [Self::Native, Self::U128Pairs, Self::U64Xor, Self::WideU64x4];

    const fn name(self) -> &'static str {
        match self {
            Self::Native => "native-array-equality",
            Self::U128Pairs => "scalar-u128-pairs",
            Self::U64Xor => "scalar-u64-xor",
            Self::WideU64x4 => "wide-u64x4",
        }
    }

    fn compare(self, left: &[u8; 32], right: &[u8; 32]) -> bool {
        match self {
            Self::Native => left == right,
            Self::U128Pairs => exact_u128_pairs(left, right),
            Self::U64Xor => exact_u64_xor(left, right),
            Self::WideU64x4 => exact_wide(left, right),
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Hit,
    MissFirst,
    MissLast,
    Mix95,
}

impl Workload {
    const ALL: [Self; 4] = [Self::Hit, Self::MissFirst, Self::MissLast, Self::Mix95];

    const fn name(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::MissFirst => "miss_first_byte",
            Self::MissLast => "miss_last_byte",
            Self::Mix95 => "mix_95pct_hit",
        }
    }
}

fn measure(
    strategy: Strategy,
    workload: Workload,
    keys: &[[u8; 32]],
    misses_first: &[[u8; 32]],
    misses_last: &[[u8; 32]],
    operations: usize,
) -> Duration {
    let started = Instant::now();
    for operation in 0..operations {
        let index = mix(operation as u64) as usize % keys.len();
        let query = match workload {
            Workload::Hit => &keys[index],
            Workload::MissFirst => &misses_first[index],
            Workload::MissLast => &misses_last[index],
            Workload::Mix95 => {
                if mix((operation as u64) ^ 0xa076_1d64_78bd_642f).is_multiple_of(20) {
                    &misses_first[index]
                } else {
                    &keys[index]
                }
            }
        };
        black_box(strategy.compare(&keys[index], query));
    }
    started.elapsed()
}

#[inline]
fn exact_u128_pairs(left: &[u8; 32], right: &[u8; 32]) -> bool {
    word_u128(left, 0) == word_u128(right, 0) && word_u128(left, 1) == word_u128(right, 1)
}

#[inline]
fn exact_u64_xor(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let left = key_words(left);
    let right = key_words(right);
    (left[0] ^ right[0]) | (left[1] ^ right[1]) | (left[2] ^ right[2]) | (left[3] ^ right[3]) == 0
}

#[inline]
fn exact_wide(left: &[u8; 32], right: &[u8; 32]) -> bool {
    u64x4::from(key_words(left))
        .cmp_eq(u64x4::from(key_words(right)))
        .to_array()
        == [u64::MAX; 4]
}

#[inline]
fn word_u128(key: &[u8; 32], word: usize) -> u128 {
    let begin = word * 16;
    u128::from_ne_bytes(
        key[begin..begin + 16]
            .try_into()
            .expect("32-byte key contains two exact u128 words"),
    )
}

#[inline]
fn key_words(key: &[u8; 32]) -> [u64; 4] {
    std::array::from_fn(|word| {
        let begin = word * 8;
        u64::from_ne_bytes(
            key[begin..begin + 8]
                .try_into()
                .expect("32-byte key contains four exact u64 words"),
        )
    })
}

fn median(samples: &[Duration]) -> Duration {
    samples[samples.len() / 2]
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
