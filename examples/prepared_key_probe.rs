//! Preparation cost and amortization for exact atomic prepared-key handles.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hint::black_box;
use std::mem::size_of_val;
use std::time::{Duration, Instant};

use packedgen::{LockFreeAtomicU64GenerationMap, NonMaxU64};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let hot_entries = argument(&mut arguments, entries.div_ceil(100)).clamp(1, entries);
    let operations = argument(&mut arguments, 5_000_000).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        keys.iter().enumerate().map(|(index, key)| {
            (
                key,
                NonMaxU64::new(index as u64).expect("benchmark value is representable"),
            )
        }),
        0,
    )
    .unwrap();

    let handles = keys[..hot_entries]
        .iter()
        .map(|key| map.prepare_key(key))
        .collect::<Vec<_>>();
    assert!(handles.iter().all(|handle| handle.has_direct_slot()));

    let mut preparation_times = Vec::with_capacity(samples);
    let mut batch_preparation_times = Vec::with_capacity(samples);
    let mut normal_times = Vec::with_capacity(samples);
    let mut handle_read_times = Vec::with_capacity(samples);
    for sample in 0..samples {
        let started = Instant::now();
        let prepared = keys[..hot_entries]
            .iter()
            .map(|key| map.prepare_key(key))
            .collect::<Vec<_>>();
        preparation_times.push(started.elapsed());
        black_box(prepared);

        let mut prepared = vec![packedgen::AtomicPreparedKey::fallback(); hot_entries];
        let started = Instant::now();
        map.prepare_key_batch(&keys[..hot_entries], &mut prepared);
        batch_preparation_times.push(started.elapsed());
        black_box(prepared);

        if sample % 2 == 0 {
            normal_times.push(measure_normal(&map, &keys, hot_entries, operations));
            handle_read_times.push(measure_prepared(
                &map,
                &keys,
                &handles,
                hot_entries,
                operations,
            ));
        } else {
            handle_read_times.push(measure_prepared(
                &map,
                &keys,
                &handles,
                hot_entries,
                operations,
            ));
            normal_times.push(measure_normal(&map, &keys, hot_entries, operations));
        }
    }

    preparation_times.sort_unstable();
    batch_preparation_times.sort_unstable();
    normal_times.sort_unstable();
    handle_read_times.sort_unstable();
    let preparation_ns = ns_per_operation(median(&preparation_times), hot_entries);
    let batch_preparation_ns = ns_per_operation(median(&batch_preparation_times), hot_entries);
    let normal_ns = ns_per_operation(median(&normal_times), operations);
    let handle_read_ns = ns_per_operation(median(&handle_read_times), operations);
    let saved_ns = normal_ns - handle_read_ns;
    let break_even_reads = if saved_ns > 0.0 {
        preparation_ns / saved_ns
    } else {
        f64::INFINITY
    };
    let handle_bytes = size_of_val(handles.as_slice());

    println!(
        "entries,hot_entries,operations,samples,handle_bytes,bytes_per_total_key,prepare_ns_per_handle,batch_prepare_ns_per_handle,batch_prepare_speedup_pct,normal_ns_per_read,prepared_ns_per_read,speedup_pct,break_even_reads"
    );
    println!(
        "{entries},{hot_entries},{operations},{samples},{handle_bytes},{:.3},{preparation_ns:.3},{batch_preparation_ns:.3},{:.3},{normal_ns:.3},{handle_read_ns:.3},{:.3},{break_even_reads:.3}",
        handle_bytes as f64 / entries as f64,
        (preparation_ns / batch_preparation_ns - 1.0) * 100.0,
        (normal_ns / handle_read_ns - 1.0) * 100.0,
    );
}

fn measure_normal(
    map: &LockFreeAtomicU64GenerationMap,
    keys: &[[u8; 32]],
    hot_entries: usize,
    operations: usize,
) -> Duration {
    let started = Instant::now();
    for operation in 0..operations {
        let index = mix(operation as u64) as usize % hot_entries;
        black_box(map.get(&keys[index]));
    }
    started.elapsed()
}

fn measure_prepared(
    map: &LockFreeAtomicU64GenerationMap,
    keys: &[[u8; 32]],
    handles: &[packedgen::AtomicPreparedKey],
    hot_entries: usize,
    operations: usize,
) -> Duration {
    let started = Instant::now();
    for operation in 0..operations {
        let index = mix(operation as u64) as usize % hot_entries;
        black_box(map.get_prepared(&keys[index], &handles[index]));
    }
    started.elapsed()
}

fn median(samples: &[Duration]) -> Duration {
    samples[samples.len() / 2]
}

fn ns_per_operation(duration: Duration, operations: usize) -> f64 {
    duration.as_secs_f64() * 1e9 / operations as f64
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
