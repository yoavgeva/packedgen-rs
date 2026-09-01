//! One-shot frozen-generation rebuild and process-peak-memory probe.

#![allow(
    missing_docs,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::hint::black_box;
use std::time::Instant;

use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, FrozenIndexBackend, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(2);
    let backend = match arguments.next().as_deref() {
        None | Some("ptrhash") => FrozenIndexBackend::PtrHash,
        #[cfg(feature = "phast")]
        Some("phast-plus") => FrozenIndexBackend::PhastPlus,
        #[cfg(not(feature = "phast"))]
        Some("phast-plus") => panic!("phast-plus requires the phast feature"),
        Some(name) => panic!("unknown backend: {name}"),
    };
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let map =
        LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash_and_index(
            keys.iter().enumerate().map(|(index, key)| {
                (
                    key,
                    NonMaxU64::new(index as u64).expect("benchmark index is representable"),
                )
            }),
            0,
            AtomicGenerationOverlay::AtomicFixed32,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
            GenerationHashBuilder::default(),
            backend,
        )
        .unwrap();
    let started = Instant::now();
    let rebuild = map.rebuild(0).unwrap();
    let elapsed = started.elapsed();

    println!("entries,rebuild_ns_per_entry,background_build_ns_per_entry");
    println!(
        "{entries},{:.3},{:.3}",
        elapsed.as_secs_f64() * 1_000_000_000.0 / entries as f64,
        rebuild.background_build.as_secs_f64() * 1_000_000_000.0 / entries as f64,
    );
    black_box((keys, map));
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
