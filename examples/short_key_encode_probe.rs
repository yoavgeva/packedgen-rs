//! Isolates safe non-boundary key materialization from hashing and table work.
//!
//! The production adaptive overlay stores keys in the smallest 8-byte size
//! class. Non-boundary keys therefore have a fixed full-word prefix followed
//! by a zero-padded 0-7 byte tail and an in-band length byte. This probe
//! compares the current whole-slice copy with safe tail-specialized forms.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::{Duration, Instant};

const KEY_POOL: usize = 4_096;

#[derive(Clone, Copy)]
enum Algorithm {
    WholeSlice,
    TailMatch,
    TailMatchOutlined,
    TailMatchCold,
    TailMatchShared,
    TailMatchOutParam,
    TailLoop,
}

impl Algorithm {
    const ALL: [Self; 7] = [
        Self::WholeSlice,
        Self::TailMatch,
        Self::TailMatchOutlined,
        Self::TailMatchCold,
        Self::TailMatchShared,
        Self::TailMatchOutParam,
        Self::TailLoop,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::WholeSlice => "whole_slice",
            Self::TailMatch => "fixed_prefix_tail_match",
            Self::TailMatchOutlined => "fixed_prefix_tail_match_outlined",
            Self::TailMatchCold => "fixed_prefix_tail_match_cold",
            Self::TailMatchShared => "fixed_prefix_shared_tail_match",
            Self::TailMatchOutParam => "fixed_prefix_tail_match_out_param",
            Self::TailLoop => "fixed_prefix_tail_loop",
        }
    }
}

fn encode_whole_slice<const N: usize>(key: &[u8]) -> [u8; N] {
    debug_assert!(key.len() < N);
    let mut encoded = [0_u8; N];
    encoded[..key.len()].copy_from_slice(key);
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

fn encode_tail_match<const N: usize>(key: &[u8]) -> [u8; N] {
    debug_assert!(N >= 8);
    debug_assert!(key.len() < N);
    let prefix = N - 8;
    debug_assert!(key.len() >= prefix);
    let mut encoded = [0_u8; N];
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    let tail = &key[prefix..];
    match tail.len() {
        0 => {}
        1 => encoded[prefix] = tail[0],
        2 => encoded[prefix..prefix + 2].copy_from_slice(tail),
        3 => encoded[prefix..prefix + 3].copy_from_slice(tail),
        4 => encoded[prefix..prefix + 4].copy_from_slice(tail),
        5 => encoded[prefix..prefix + 5].copy_from_slice(tail),
        6 => encoded[prefix..prefix + 6].copy_from_slice(tail),
        7 => encoded[prefix..prefix + 7].copy_from_slice(tail),
        _ => unreachable!("a short size-class tail is at most seven bytes"),
    }
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

#[inline(never)]
fn encode_tail_match_outlined<const N: usize>(key: &[u8]) -> [u8; N] {
    encode_tail_match(key)
}

#[cold]
#[inline(never)]
fn encode_tail_match_cold<const N: usize>(key: &[u8]) -> [u8; N] {
    encode_tail_match(key)
}

#[cold]
#[inline(never)]
fn encode_shared_tail(tail: &[u8], key_bytes: usize) -> [u8; 8] {
    let mut encoded = [0_u8; 8];
    match tail.len() {
        0 => {}
        1 => encoded[0] = tail[0],
        2 => encoded[..2].copy_from_slice(tail),
        3 => encoded[..3].copy_from_slice(tail),
        4 => encoded[..4].copy_from_slice(tail),
        5 => encoded[..5].copy_from_slice(tail),
        6 => encoded[..6].copy_from_slice(tail),
        7 => encoded[..7].copy_from_slice(tail),
        _ => unreachable!("a short size-class tail is at most seven bytes"),
    }
    encoded[7] = u8::try_from(key_bytes).expect("probe key length fits u8");
    encoded
}

fn encode_tail_match_shared<const N: usize>(key: &[u8]) -> [u8; N] {
    let prefix = N - 8;
    let mut encoded = [0_u8; N];
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    encoded[prefix..].copy_from_slice(&encode_shared_tail(&key[prefix..], key.len()));
    encoded
}

#[inline(never)]
fn fill_tail_match(encoded: &mut [u8], key: &[u8]) {
    let prefix = encoded.len() - 8;
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    let tail = &key[prefix..];
    match tail.len() {
        0 => {}
        1 => encoded[prefix] = tail[0],
        2 => encoded[prefix..prefix + 2].copy_from_slice(tail),
        3 => encoded[prefix..prefix + 3].copy_from_slice(tail),
        4 => encoded[prefix..prefix + 4].copy_from_slice(tail),
        5 => encoded[prefix..prefix + 5].copy_from_slice(tail),
        6 => encoded[prefix..prefix + 6].copy_from_slice(tail),
        7 => encoded[prefix..prefix + 7].copy_from_slice(tail),
        _ => unreachable!("a short size-class tail is at most seven bytes"),
    }
    encoded[encoded.len() - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
}

fn encode_tail_match_out_param<const N: usize>(key: &[u8]) -> [u8; N] {
    let mut encoded = [0_u8; N];
    fill_tail_match(&mut encoded, key);
    encoded
}

fn encode_tail_loop<const N: usize>(key: &[u8]) -> [u8; N] {
    debug_assert!(N >= 8);
    debug_assert!(key.len() < N);
    let prefix = N - 8;
    debug_assert!(key.len() >= prefix);
    let mut encoded = [0_u8; N];
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    for (offset, byte) in key[prefix..].iter().copied().enumerate() {
        encoded[prefix + offset] = byte;
    }
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

fn encode<const N: usize>(algorithm: Algorithm, key: &[u8]) -> [u8; N] {
    match algorithm {
        Algorithm::WholeSlice => encode_whole_slice(key),
        Algorithm::TailMatch => encode_tail_match(key),
        Algorithm::TailMatchOutlined => encode_tail_match_outlined(key),
        Algorithm::TailMatchCold => encode_tail_match_cold(key),
        Algorithm::TailMatchShared => encode_tail_match_shared(key),
        Algorithm::TailMatchOutParam => encode_tail_match_out_param(key),
        Algorithm::TailLoop => encode_tail_loop(key),
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn keys(key_bytes: usize) -> Vec<Box<[u8]>> {
    (0..KEY_POOL)
        .map(|index| {
            let mut key = vec![0_u8; key_bytes];
            let mut state = mix(index as u64);
            for chunk in key.chunks_mut(8) {
                state = mix(state);
                let word = state.to_le_bytes();
                chunk.copy_from_slice(&word[..chunk.len()]);
            }
            key.into_boxed_slice()
        })
        .collect()
}

fn duration<const N: usize>(
    algorithm: Algorithm,
    keys: &[Box<[u8]>],
    operations: usize,
) -> Duration {
    let mut checksum = 0_u64;
    let started = Instant::now();
    for operation in 0..operations {
        let key = black_box(&keys[operation & (KEY_POOL - 1)]);
        let encoded = black_box(encode::<N>(algorithm, black_box(key)));
        checksum = checksum.wrapping_add(u64::from(encoded[operation % N]));
    }
    black_box(checksum);
    started.elapsed()
}

fn percentile(mut values: Vec<f64>, numerator: usize, denominator: usize) -> f64 {
    values.sort_by(f64::total_cmp);
    let index = values.len().saturating_sub(1).saturating_mul(numerator) / denominator;
    values[index]
}

fn run_class<const N: usize>(key_bytes: usize, operations: usize, samples: usize) {
    assert!(N.is_multiple_of(8));
    assert!((N - 8..N).contains(&key_bytes));
    let keys = keys(key_bytes);
    for key in &keys {
        let expected = encode_whole_slice::<N>(key);
        for algorithm in Algorithm::ALL {
            assert_eq!(encode::<N>(algorithm, key), expected);
        }
    }
    for algorithm in Algorithm::ALL {
        let _ = duration::<N>(algorithm, &keys, operations / 20);
    }

    let mut results = vec![Vec::with_capacity(samples); Algorithm::ALL.len()];
    for sample in 0..samples {
        for step in 0..Algorithm::ALL.len() {
            let index = (sample + step) % Algorithm::ALL.len();
            let elapsed = duration::<N>(Algorithm::ALL[index], &keys, operations);
            results[index].push(operations as f64 / elapsed.as_secs_f64() / 1_000_000.0);
        }
    }

    println!("key_bytes={key_bytes} class_bytes={N} operations={operations} samples={samples}");
    for (algorithm, values) in Algorithm::ALL.into_iter().zip(results) {
        let median = percentile(values.clone(), 1, 2);
        let p05 = percentile(values, 1, 20);
        println!(
            "algorithm={} median_mops={median:.3} p05_mops={p05:.3}",
            algorithm.name()
        );
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let key_bytes = arguments
        .next()
        .map_or(13, |value| value.parse().expect("key bytes is an integer"));
    let operations = arguments.next().map_or(20_000_000, |value| {
        value.parse().expect("operations is an integer")
    });
    let samples = arguments
        .next()
        .map_or(15, |value| value.parse().expect("samples is an integer"));
    match key_bytes {
        1..=7 => run_class::<8>(key_bytes, operations, samples),
        8..=15 => run_class::<16>(key_bytes, operations, samples),
        16..=23 => run_class::<24>(key_bytes, operations, samples),
        24..=31 => run_class::<32>(key_bytes, operations, samples),
        _ => panic!("probe accepts non-boundary key widths from 1 through 31"),
    }
}
