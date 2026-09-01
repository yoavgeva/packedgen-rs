//! Measures adaptive metadata scanning and one recommended rebuild.

#![allow(missing_docs, clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::Instant;

use packedgen::{
    AdaptiveRebuildPolicy, AtomicGenerationOverlay, LockFreeAtomicU64GenerationMap, NonMaxU64,
};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(64);
    let samples = argument(&mut arguments, 11).max(3);
    let capacity = entries.saturating_mul(2);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        capacity,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .expect("adaptive probe map builds");
    for index in 0..entries {
        let value = u64::try_from(index).expect("probe index fits u64");
        map.insert(
            &binary_key(value, selected_key_bytes(value)),
            NonMaxU64::new(value).expect("probe value is not reserved"),
        );
    }

    let mut scans = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        black_box(map.adaptive_overlay_stats().expect("adaptive statistics"));
        scans.push(started.elapsed());
    }
    scans.sort_unstable();
    let median_scan = scans[scans.len() / 2];

    let policy = AdaptiveRebuildPolicy {
        max_slot_utilization_bps: 0,
        ..AdaptiveRebuildPolicy::default()
    };
    let started = Instant::now();
    let rebuild = map
        .rebuild_adaptive_if_needed(policy)
        .expect("adaptive rebuild succeeds")
        .expect("forced policy recommends rebuild");
    let total_rebuild = started.elapsed();

    println!(
        "entries,scan_samples,median_scan_ns,scan_ns_per_record,total_maintenance_ns,writer_redirect_ns,background_build_ns,base_publish_ns"
    );
    println!(
        "{entries},{samples},{},{:.3},{},{},{},{}",
        median_scan.as_nanos(),
        median_scan.as_secs_f64() * 1_000_000_000.0 / entries as f64,
        total_rebuild.as_nanos(),
        rebuild.writer_redirect.as_nanos(),
        rebuild.background_build.as_nanos(),
        rebuild.base_publish.as_nanos(),
    );
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn selected_key_bytes(value: u64) -> usize {
    match value % 100 {
        0..=39 => 8,
        40..=64 => 16,
        65..=79 => 24,
        80..=89 => 32,
        _ => 48,
    }
}

fn binary_key(value: u64, key_bytes: usize) -> Vec<u8> {
    let mut key = vec![0_u8; key_bytes];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
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
