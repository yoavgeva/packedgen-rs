//! Compares variable-key routing with fixed encoded-key routing primitives.

use std::hash::{BuildHasher, RandomState as StdRandomState};
use std::hint::black_box;
use std::time::Instant;

use rapidhash::fast::RandomState as RapidRandomState;
use rapidhash::v3::{RapidSecrets, rapidhash_v3_nano_inline};

const KEY_COUNT: usize = 4_096;

fn encode<const N: usize>(key: &[u8]) -> [u8; N] {
    debug_assert!(key.len() < N);
    let mut encoded = [0_u8; N];
    encoded[..key.len()].copy_from_slice(key);
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

fn keys(width: usize) -> Vec<Box<[u8]>> {
    (0..KEY_COUNT)
        .map(|index| {
            let mut key = vec![0_u8; width];
            let mixed = u64::try_from(index)
                .unwrap()
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .to_le_bytes();
            for (offset, byte) in key.iter_mut().enumerate() {
                *byte = mixed[offset & 7] ^ u8::try_from(offset).unwrap().wrapping_mul(37);
            }
            key.into_boxed_slice()
        })
        .collect()
}

fn measure(mut operation: impl FnMut(usize) -> u64, operations: u32, samples: usize) -> f64 {
    let mut rates = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let mut checksum = 0_u64;
        for operation_index in 0..operations {
            checksum ^= black_box(operation(usize::try_from(operation_index).unwrap()));
        }
        black_box(checksum);
        rates.push(f64::from(operations) / start.elapsed().as_secs_f64() / 1_000_000.0);
    }
    rates.sort_by(f64::total_cmp);
    rates[rates.len() / 2]
}

fn run<const N: usize>(width: usize, operations: u32, samples: usize) {
    let keys = keys(width);
    let rapid = RapidRandomState::default();
    let secrets = RapidSecrets::random();
    let std = StdRandomState::new();

    let current_route = measure(
        |operation| rapid.hash_one(black_box(&keys[operation & (KEY_COUNT - 1)])),
        operations,
        samples,
    );
    let nano_raw = measure(
        |operation| {
            rapidhash_v3_nano_inline::<false, false>(
                black_box(&keys[operation & (KEY_COUNT - 1)]),
                &secrets,
            )
        },
        operations,
        samples,
    );
    let current_plus_encode = measure(
        |operation| {
            let key = black_box(&keys[operation & (KEY_COUNT - 1)]);
            let hash = rapid.hash_one(key);
            let encoded = encode::<N>(key);
            hash ^ u64::from(black_box(encoded)[N - 1])
        },
        operations,
        samples,
    );
    let nano_plus_encode = measure(
        |operation| {
            let key = black_box(&keys[operation & (KEY_COUNT - 1)]);
            let hash = rapidhash_v3_nano_inline::<false, false>(key, &secrets);
            let encoded = encode::<N>(key);
            hash ^ u64::from(black_box(encoded)[N - 1])
        },
        operations,
        samples,
    );
    let encoded_current = measure(
        |operation| {
            let encoded = encode::<N>(black_box(&keys[operation & (KEY_COUNT - 1)]));
            rapid.hash_one(black_box(encoded))
        },
        operations,
        samples,
    );
    let encoded_nano = measure(
        |operation| {
            let encoded = encode::<N>(black_box(&keys[operation & (KEY_COUNT - 1)]));
            rapidhash_v3_nano_inline::<false, false>(black_box(&encoded), &secrets)
        },
        operations,
        samples,
    );
    let encoded_std = measure(
        |operation| {
            let encoded = encode::<N>(black_box(&keys[operation & (KEY_COUNT - 1)]));
            std.hash_one(black_box(encoded))
        },
        operations,
        samples,
    );

    println!(
        "{width},{N},{current_route:.3},{nano_raw:.3},{current_plus_encode:.3},{nano_plus_encode:.3},{encoded_current:.3},{encoded_nano:.3},{encoded_std:.3}"
    );
}

fn main() {
    let operations = std::env::args()
        .nth(1)
        .map_or(10_000_000, |value| value.parse().unwrap());
    let samples = std::env::args()
        .nth(2)
        .map_or(11, |value| value.parse().unwrap());
    println!(
        "width,class,current_route_mops,nano_raw_mops,current_plus_encode_mops,nano_plus_encode_mops,encoded_current_mops,encoded_nano_mops,encoded_std_mops"
    );
    run::<8>(7, operations, samples);
    run::<16>(13, operations, samples);
    run::<24>(21, operations, samples);
    run::<32>(29, operations, samples);
}
