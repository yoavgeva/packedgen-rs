//! Paired construction, rebuild, and retained-base comparison for frozen indexes.

#![allow(
    missing_docs,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, FrozenIndexBackend, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

type AtomicMap = LockFreeAtomicU64GenerationMap;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(2);
    let samples = argument(&mut arguments, 9).max(3);
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let writer_hash_builder = GenerationHashBuilder::default();

    let mut build_times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
    let mut rebuild_times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];

    for sample in 0..samples {
        for offset in 0..Backend::ALL.len() {
            let index = (sample + offset) % Backend::ALL.len();
            let backend = Backend::ALL[index];

            let started = Instant::now();
            let map = build_map(backend.index(), &keys, &writer_hash_builder);
            build_times[index].push(started.elapsed());
            black_box(map.stats());

            let map = build_map(backend.index(), &keys, &writer_hash_builder);
            let rebuild = map.rebuild(0).unwrap();
            rebuild_times[index].push(rebuild.background_build);
            black_box(map);
        }
    }

    println!(
        "backend,entries,samples,build_ns_per_entry,rebuild_ns_per_entry,base_bytes_per_entry,index_bits_per_entry,filter_bytes_per_entry"
    );
    for (index, backend) in Backend::ALL.into_iter().enumerate() {
        build_times[index].sort_unstable();
        rebuild_times[index].sort_unstable();
        let build = median(&build_times[index]);
        let rebuild = median(&rebuild_times[index]);
        let map = build_map(backend.index(), &keys, &writer_hash_builder);
        let stats = map.stats();
        let base = stats.current.base;
        let index_bytes = ((base.index_bits_per_entry * base.len as f64) / 8.0).ceil() as usize;
        let retained_bytes = base
            .arena_allocated_bytes
            .saturating_add(base.slot_bytes)
            .saturating_add(index_bytes)
            .saturating_add(stats.base_filter_bytes);

        println!(
            "{},{entries},{samples},{:.3},{:.3},{:.3},{:.3},{:.3}",
            backend.name(),
            ns_per_entry(build, entries),
            ns_per_entry(rebuild, entries),
            retained_bytes as f64 / entries as f64,
            base.index_bits_per_entry,
            stats.base_filter_bytes as f64 / entries as f64,
        );
    }
}

fn build_map(
    index_backend: FrozenIndexBackend,
    keys: &[[u8; 32]],
    writer_hash_builder: &GenerationHashBuilder,
) -> AtomicMap {
    AtomicMap::try_from_entries_with_options_and_writer_hash_and_index(
        keys.iter().enumerate().map(|(index, key)| {
            (
                key,
                NonMaxU64::new(index as u64).expect("benchmark index is representable"),
            )
        }),
        0,
        AtomicGenerationOverlay::AtomicFixed32,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
        writer_hash_builder.clone(),
        index_backend,
    )
    .unwrap()
}

fn median(samples: &[Duration]) -> Duration {
    samples[samples.len() / 2]
}

fn ns_per_entry(duration: Duration, entries: usize) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / entries as f64
}

#[derive(Clone, Copy)]
enum Backend {
    PtrHash,
    PhastPlus,
}

impl Backend {
    const ALL: [Self; 2] = [Self::PtrHash, Self::PhastPlus];

    const fn name(self) -> &'static str {
        match self {
            Self::PtrHash => "ptrhash",
            Self::PhastPlus => "phast-plus",
        }
    }

    const fn index(self) -> FrozenIndexBackend {
        match self {
            Self::PtrHash => FrozenIndexBackend::PtrHash,
            Self::PhastPlus => FrozenIndexBackend::PhastPlus,
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
