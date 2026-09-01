//! One-shot frozen-generation construction and retained-memory probe.

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
    let started = Instant::now();
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
    let elapsed = started.elapsed();
    let stats = map.stats();
    let base = stats.current.base;
    let index_bytes = ((base.index_bits_per_entry * base.len as f64) / 8.0).ceil() as usize;
    let retained_bytes = base
        .arena_allocated_bytes
        .saturating_add(base.slot_bytes)
        .saturating_add(index_bytes)
        .saturating_add(stats.base_filter_bytes);

    println!("entries,build_ns_per_entry,base_bytes_per_entry,index_bits_per_entry");
    println!(
        "{entries},{:.3},{:.3},{:.3}",
        elapsed.as_secs_f64() * 1_000_000_000.0 / entries as f64,
        retained_bytes as f64 / entries as f64,
        base.index_bits_per_entry,
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
    for chunk in key.as_chunks_mut::<8>().0 {
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
