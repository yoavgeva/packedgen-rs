//! Rebuild handoff timing under continuous prepared update batches.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::trivially_copy_pass_by_ref
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use packedgen::{AtomicPreparedKey, LockFreeAtomicU64GenerationMap, NonMaxU64};

const BATCH: usize = 16;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let hot_entries = argument(&mut arguments, entries.div_ceil(100)).clamp(1, entries);
    let writers = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    let mut redirects = Vec::with_capacity(samples);
    let mut builds = Vec::with_capacity(samples);
    let mut publishes = Vec::with_capacity(samples);
    let mut operation_counts = Vec::with_capacity(samples);

    for _ in 0..samples {
        let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            keys.iter()
                .enumerate()
                .map(|(index, key)| (key, non_max(index as u64))),
            hot_entries,
        )
        .unwrap();
        let prepared = keys[..hot_entries]
            .iter()
            .map(|key| map.prepare_key(key))
            .collect::<Vec<_>>();
        let running = AtomicBool::new(true);
        let operations = AtomicU64::new(0);
        let start = Barrier::new(writers + 1);

        let rebuilt = std::thread::scope(|scope| {
            for writer in 0..writers {
                let map = &map;
                let keys = &keys;
                let prepared = &prepared;
                let running = &running;
                let operations = &operations;
                let start = &start;
                scope.spawn(move || {
                    let mut batch_keys = [&[][..]; BATCH];
                    let mut batch_prepared = [AtomicPreparedKey::fallback(); BATCH];
                    let mut updated = [None; BATCH];
                    let mut operation = writer * BATCH;
                    start.wait();
                    while running.load(Ordering::Acquire) {
                        for offset in 0..BATCH {
                            let index = mix((operation + offset) as u64) as usize % hot_entries;
                            batch_keys[offset] = &keys[index];
                            batch_prepared[offset] = prepared[index];
                        }
                        map.update_prepared_batch(
                            &batch_keys,
                            &batch_prepared,
                            &mut updated,
                            increment,
                        );
                        debug_assert!(updated.iter().all(Option::is_some));
                        operations.fetch_add(BATCH as u64, Ordering::Relaxed);
                        operation = operation.wrapping_add(writers * BATCH);
                    }
                    black_box(updated);
                });
            }

            start.wait();
            let warm_operations =
                u64::try_from(writers * BATCH * 32).expect("warm operation count fits u64");
            while operations.load(Ordering::Acquire) < warm_operations {
                std::thread::yield_now();
            }
            let rebuilt = map.rebuild(hot_entries).unwrap();
            running.store(false, Ordering::Release);
            rebuilt
        });

        redirects.push(rebuilt.writer_redirect);
        builds.push(rebuilt.background_build);
        publishes.push(rebuilt.base_publish);
        operation_counts.push(operations.load(Ordering::Relaxed));
    }

    redirects.sort_unstable();
    builds.sort_unstable();
    publishes.sort_unstable();
    operation_counts.sort_unstable();
    println!(
        "entries,hot_entries,writers,batch,samples,writer_redirect_us,background_build_ms,base_publish_us,writer_operations"
    );
    println!(
        "{entries},{hot_entries},{writers},{BATCH},{samples},{:.3},{:.3},{:.3},{}",
        micros(median(&redirects)),
        millis(median(&builds)),
        micros(median(&publishes)),
        operation_counts[operation_counts.len() / 2],
    );
}

fn median(values: &[Duration]) -> Duration {
    values[values.len() / 2]
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("probe value must not use deletion marker")
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
