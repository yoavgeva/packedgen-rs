//! Focused alternating-order probe for atomic generation overlay strategies.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, LockFreeAtomicU64GenerationMap, NonMaxU64,
};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let operations = argument(&mut arguments, 500_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    let key_bytes = argument(&mut arguments, 32).clamp(8, 128);
    let workload_filter = arguments.next();
    let strategy_group = arguments.next();
    let mut modes = vec![compact_mode(key_bytes), AtomicGenerationOverlay::Papaya];
    if key_bytes == 32 {
        modes.insert(0, AtomicGenerationOverlay::AtomicFixed32);
        modes.push(AtomicGenerationOverlay::ArcSwapFixed32);
    }
    if strategy_group.as_deref() == Some("atomic-only") {
        modes.retain(|mode| matches!(mode, AtomicGenerationOverlay::AtomicFixed32));
    }
    let keys = (0..entries as u64)
        .map(|value| binary_key(value, key_bytes))
        .collect::<Vec<_>>();
    let inserts = (entries as u64..entries.saturating_add(operations) as u64)
        .map(|value| binary_key(value, key_bytes))
        .collect::<Vec<_>>();

    println!(
        "strategy,workload,key_bytes,entries,operations,threads,samples,median_ns,throughput_mops"
    );
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| workload.name() != filter)
        {
            continue;
        }
        let workload_operations = workload.operations(entries, operations);
        let mut measurements = (0..modes.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        for sample in 0..samples {
            for offset in 0..modes.len() {
                let mode_index = (sample + offset) % modes.len();
                let duration = measure(
                    modes[mode_index],
                    workload,
                    entries,
                    workload_operations,
                    threads,
                    &keys,
                    &inserts,
                );
                measurements[mode_index].push(duration);
            }
        }
        for (mode, samples) in modes.iter().copied().zip(&mut measurements) {
            samples.sort_unstable();
            let median = samples[samples.len() / 2];
            let throughput = workload_operations as f64 / median.as_secs_f64() / 1_000_000.0;
            println!(
                "{},{},{key_bytes},{entries},{workload_operations},{threads},{},{},{throughput:.3}",
                mode_name(mode),
                workload.name(),
                samples.len(),
                median.as_nanos(),
            );
        }
    }
}

fn measure(
    mode: AtomicGenerationOverlay,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) -> Duration {
    let map = match workload {
        Workload::InsertMiss => LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
            std::iter::empty::<([u8; 32], NonMaxU64)>(),
            operations,
            mode,
        ),
        Workload::InsertMissOverflow => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                entries.div_ceil(100),
                mode,
            )
        }
        Workload::InsertMissWithBase => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (key.as_ref(), value(index as u64))),
                operations,
                mode,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
            )
        }
        Workload::OverlayReadHit | Workload::OverlayUpdateHit => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                entries,
                mode,
            )
        }
        Workload::ReadHit
        | Workload::ReadMiss
        | Workload::InsertHit
        | Workload::UpdateHit
        | Workload::UpdateMiss
        | Workload::UpdateHot
        | Workload::DeleteHit
        | Workload::DeleteMiss => LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            keys.iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index as u64))),
            entries.div_ceil(100),
            mode,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
        ),
    }
    .unwrap();
    if matches!(
        workload,
        Workload::OverlayReadHit | Workload::OverlayUpdateHit
    ) {
        for (index, key) in keys.iter().enumerate() {
            map.insert(key, value(index as u64));
        }
    }
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    let elapsed = std::thread::scope(|scope| {
        for thread in 0..threads {
            let map = &map;
            let start = &start;
            let done = &done;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    match workload {
                        Workload::ReadHit | Workload::OverlayReadHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.get(key.as_ref()).unwrap());
                        }
                        Workload::ReadMiss => {
                            black_box(map.get(inserts[operation].as_ref()));
                        }
                        Workload::InsertHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.insert(key.as_ref(), value(operation as u64)));
                        }
                        Workload::InsertMiss
                        | Workload::InsertMissWithBase
                        | Workload::InsertMissOverflow => {
                            map.insert(inserts[operation].as_ref(), value(operation as u64));
                        }
                        Workload::UpdateHit | Workload::OverlayUpdateHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            map.update(key.as_ref(), increment).unwrap();
                        }
                        Workload::UpdateMiss => {
                            assert_eq!(map.update(inserts[operation].as_ref(), increment), None);
                        }
                        Workload::UpdateHot => {
                            map.update(keys[0].as_ref(), increment).unwrap();
                        }
                        Workload::DeleteHit => {
                            black_box(map.remove(keys[operation].as_ref()).unwrap());
                        }
                        Workload::DeleteMiss => {
                            assert_eq!(map.remove(inserts[operation].as_ref()), None);
                        }
                    }
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadMiss,
    InsertHit,
    InsertMiss,
    InsertMissWithBase,
    InsertMissOverflow,
    OverlayReadHit,
    OverlayUpdateHit,
    UpdateHit,
    UpdateMiss,
    UpdateHot,
    DeleteHit,
    DeleteMiss,
}

impl Workload {
    const ALL: [Self; 13] = [
        Self::ReadHit,
        Self::ReadMiss,
        Self::InsertHit,
        Self::InsertMiss,
        Self::InsertMissWithBase,
        Self::InsertMissOverflow,
        Self::OverlayReadHit,
        Self::OverlayUpdateHit,
        Self::UpdateHit,
        Self::UpdateMiss,
        Self::UpdateHot,
        Self::DeleteHit,
        Self::DeleteMiss,
    ];

    const fn operations(self, entries: usize, requested: usize) -> usize {
        if matches!(self, Self::DeleteHit) {
            if requested < entries {
                requested
            } else {
                entries
            }
        } else {
            requested
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadMiss => "read_miss",
            Self::InsertHit => "insert_hit",
            Self::InsertMiss => "insert_miss",
            Self::InsertMissWithBase => "insert_miss_with_base",
            Self::InsertMissOverflow => "insert_miss_overflow",
            Self::OverlayReadHit => "overlay_read_hit",
            Self::OverlayUpdateHit => "overlay_update_hit",
            Self::UpdateHit => "update_hit",
            Self::UpdateMiss => "update_miss",
            Self::UpdateHot => "update_hot_key",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
        }
    }
}

const fn mode_name(mode: AtomicGenerationOverlay) -> &'static str {
    match mode {
        AtomicGenerationOverlay::AtomicFixed32 => "atomic-bucket32",
        AtomicGenerationOverlay::CompactFixed32 | AtomicGenerationOverlay::CompactSized { .. } => {
            "inline-sized-papaya"
        }
        AtomicGenerationOverlay::Papaya => "boxed-key-papaya",
        AtomicGenerationOverlay::ArcSwapFixed32 => "dense-arcswap32",
    }
}

fn compact_mode(key_bytes: usize) -> AtomicGenerationOverlay {
    if key_bytes == 32 {
        AtomicGenerationOverlay::CompactFixed32
    } else {
        AtomicGenerationOverlay::CompactSized {
            key_bytes: u8::try_from(key_bytes).expect("probe key size fits u8"),
        }
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn value(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("probe values remain representable")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    NonMaxU64::new(value.get() + 1).expect("probe values remain representable")
}

fn binary_key(value: u64, key_bytes: usize) -> Box<[u8]> {
    let mut key = vec![0_u8; key_bytes];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    key.into_boxed_slice()
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
