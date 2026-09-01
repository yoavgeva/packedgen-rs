//! Allocation-free prepared-read batch-size comparison.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::trivially_copy_pass_by_ref
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hashbrown::DefaultHashBuilder;
use packedgen::{AtomicPreparedKey, LockFreeAtomicU64GenerationMap, NonMaxU64};
use papaya::HashMap as PapayaHashMap;

const BATCH_SIZES: [usize; 8] = [2, 4, 8, 16, 32, 64, 128, 256];
const ACCESS_PATTERN: usize = 65_536;

type Atomic = LockFreeAtomicU64GenerationMap;
type Dash = DashMap<[u8; 32], u64, DefaultHashBuilder>;
type Papaya = PapayaHashMap<Box<[u8]>, u64, DefaultHashBuilder>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let hot_entries = argument(&mut arguments, entries.div_ceil(100)).clamp(1, entries);
    let operations = argument(&mut arguments, 2_000_000).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    let operation = Operation::parse(arguments.next().as_deref());
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let atomic = Atomic::try_from_entries(
        keys.iter().enumerate().map(|(index, key)| {
            (
                key,
                NonMaxU64::new(index as u64).expect("benchmark value is representable"),
            )
        }),
        0,
    )
    .unwrap();
    let dash = Dash::with_capacity_and_hasher(entries, DefaultHashBuilder::default());
    let papaya = Papaya::with_capacity_and_hasher(entries, DefaultHashBuilder::default());
    let papaya_guard = papaya.guard();
    for (index, key) in keys.iter().enumerate() {
        dash.insert(*key, index as u64);
        papaya.insert(key.as_slice().into(), index as u64, &papaya_guard);
    }
    drop(papaya_guard);

    let handles = keys[..hot_entries]
        .iter()
        .map(|key| atomic.prepare_key(key))
        .collect::<Vec<_>>();
    let pattern_len = operations.min(ACCESS_PATTERN);
    let access_indices = (0..pattern_len)
        .map(|operation| mix(operation as u64) as usize % hot_entries)
        .collect::<Vec<_>>();
    let access_keys = access_indices
        .iter()
        .map(|&index| keys[index].as_slice())
        .collect::<Vec<_>>();
    let access_handles = access_indices
        .iter()
        .map(|&index| handles[index])
        .collect::<Vec<_>>();

    let strategies = Strategy::all();
    let mut measurements = vec![Vec::with_capacity(samples); strategies.len()];
    for sample in 0..samples {
        for offset in 0..strategies.len() {
            let index = (sample + offset) % strategies.len();
            measurements[index].push(measure(
                strategies[index],
                operation,
                &atomic,
                &dash,
                &papaya,
                &access_keys,
                &access_handles,
                operations,
            ));
        }
    }

    for samples in &mut measurements {
        samples.sort_unstable();
    }
    let scalar_ns = ns_per_operation(
        median(&measurements[Strategy::Prepared.index()]),
        operations,
    );
    let dash_ns = ns_per_operation(median(&measurements[Strategy::Dash.index()]), operations);
    let papaya_ns = ns_per_operation(median(&measurements[Strategy::Papaya.index()]), operations);

    println!(
        "operation,strategy,batch_size,entries,hot_entries,operations,samples,median_ns,ns_per_operation,throughput_mops,vs_scalar_prepared_pct,vs_dash_pct,vs_papaya_pct"
    );
    for (strategy, samples) in strategies.into_iter().zip(measurements) {
        let duration = median(&samples);
        let ns = ns_per_operation(duration, operations);
        println!(
            "{},{},{},{entries},{hot_entries},{operations},{},{},{ns:.3},{:.3},{:.3},{:.3},{:.3}",
            operation.name(),
            strategy.name(),
            strategy.batch_size(),
            samples.len(),
            duration.as_nanos(),
            1_000.0 / ns,
            (scalar_ns / ns - 1.0) * 100.0,
            (dash_ns / ns - 1.0) * 100.0,
            (papaya_ns / ns - 1.0) * 100.0,
        );
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Read,
    Update,
    Replace,
    Insert,
}

impl Operation {
    fn parse(value: Option<&str>) -> Self {
        match value {
            None | Some("read") => Self::Read,
            Some("update") => Self::Update,
            Some("replace") => Self::Replace,
            Some("insert") => Self::Insert,
            Some(value) => {
                panic!("operation must be read, update, replace, or insert, got {value}")
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Update => "update",
            Self::Replace => "replace",
            Self::Insert => "insert",
        }
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    Normal,
    Prepared,
    Batch(usize),
    Dash,
    Papaya,
}

impl Strategy {
    const fn all() -> [Self; 12] {
        [
            Self::Normal,
            Self::Prepared,
            Self::Batch(BATCH_SIZES[0]),
            Self::Batch(BATCH_SIZES[1]),
            Self::Batch(BATCH_SIZES[2]),
            Self::Batch(BATCH_SIZES[3]),
            Self::Batch(BATCH_SIZES[4]),
            Self::Batch(BATCH_SIZES[5]),
            Self::Batch(BATCH_SIZES[6]),
            Self::Batch(BATCH_SIZES[7]),
            Self::Dash,
            Self::Papaya,
        ]
    }

    const fn index(self) -> usize {
        match self {
            Self::Normal => 0,
            Self::Prepared => 1,
            Self::Batch(size) => {
                let mut index = 0;
                while BATCH_SIZES[index] != size {
                    index += 1;
                }
                index + 2
            }
            Self::Dash => 10,
            Self::Papaya => 11,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Normal => "atomic-normal",
            Self::Prepared => "atomic-prepared-scalar",
            Self::Batch(_) => "atomic-prepared-batch",
            Self::Dash => "dashmap",
            Self::Papaya => "papaya-0.2.4-direct",
        }
    }

    const fn batch_size(self) -> usize {
        match self {
            Self::Batch(size) => size,
            Self::Normal | Self::Prepared | Self::Dash | Self::Papaya => 1,
        }
    }
}

fn measure(
    strategy: Strategy,
    operation_kind: Operation,
    atomic: &Atomic,
    dash: &Dash,
    papaya: &Papaya,
    access_keys: &[&[u8]],
    access_handles: &[AtomicPreparedKey],
    operations: usize,
) -> Duration {
    let mut values = [None; BATCH_SIZES[BATCH_SIZES.len() - 1]];
    let mut inserted = [non_max(0); BATCH_SIZES[BATCH_SIZES.len() - 1]];
    let papaya_guard = papaya.guard();
    let started = Instant::now();
    match strategy {
        Strategy::Normal => {
            for operation in 0..operations {
                let index = operation % access_keys.len();
                match operation_kind {
                    Operation::Read => {
                        black_box(atomic.get(access_keys[index]));
                    }
                    Operation::Update => {
                        black_box(atomic.update(access_keys[index], increment));
                    }
                    Operation::Replace => {
                        let replacement = non_max(operation as u64);
                        black_box(atomic.update(access_keys[index], |_| replacement));
                    }
                    Operation::Insert => {
                        black_box(atomic.insert(access_keys[index], non_max(operation as u64)));
                    }
                }
            }
        }
        Strategy::Prepared => {
            for operation in 0..operations {
                let index = operation % access_keys.len();
                match operation_kind {
                    Operation::Read => {
                        black_box(atomic.get_prepared(access_keys[index], &access_handles[index]));
                    }
                    Operation::Update => {
                        black_box(atomic.update_prepared(
                            access_keys[index],
                            &access_handles[index],
                            increment,
                        ));
                    }
                    Operation::Replace => {
                        let replacement = non_max(operation as u64);
                        black_box(atomic.update_prepared(
                            access_keys[index],
                            &access_handles[index],
                            |_| replacement,
                        ));
                    }
                    Operation::Insert => {
                        black_box(atomic.insert_prepared(
                            access_keys[index],
                            &access_handles[index],
                            non_max(operation as u64),
                        ));
                    }
                }
            }
        }
        Strategy::Batch(batch_size) => {
            let mut operation = 0;
            while operation < operations {
                let pattern_index = operation % access_keys.len();
                let count = batch_size
                    .min(operations - operation)
                    .min(access_keys.len() - pattern_index);
                for (offset, inserted) in inserted[..count].iter_mut().enumerate() {
                    *inserted = non_max((operation + offset) as u64);
                }
                match operation_kind {
                    Operation::Read => atomic.get_prepared_batch(
                        &access_keys[pattern_index..pattern_index + count],
                        &access_handles[pattern_index..pattern_index + count],
                        &mut values[..count],
                    ),
                    Operation::Update => atomic.update_prepared_batch(
                        &access_keys[pattern_index..pattern_index + count],
                        &access_handles[pattern_index..pattern_index + count],
                        &mut values[..count],
                        increment,
                    ),
                    Operation::Replace => atomic.replace_prepared_batch(
                        &access_keys[pattern_index..pattern_index + count],
                        &access_handles[pattern_index..pattern_index + count],
                        &inserted[..count],
                        &mut values[..count],
                    ),
                    Operation::Insert => atomic.insert_prepared_batch(
                        &access_keys[pattern_index..pattern_index + count],
                        &access_handles[pattern_index..pattern_index + count],
                        &inserted[..count],
                        &mut values[..count],
                    ),
                }
                black_box(&values[..count]);
                operation += count;
            }
        }
        Strategy::Dash => {
            for operation in 0..operations {
                let index = operation % access_keys.len();
                match operation_kind {
                    Operation::Read => {
                        black_box(dash.get(access_keys[index]).map(|value| *value));
                    }
                    Operation::Update => {
                        let mut value = dash
                            .get_mut(access_keys[index])
                            .expect("benchmark key must remain present");
                        *value += 1;
                        black_box(value);
                    }
                    Operation::Replace | Operation::Insert => {
                        let key: [u8; 32] = access_keys[index]
                            .try_into()
                            .expect("benchmark keys are exactly 32 bytes");
                        black_box(dash.insert(key, operation as u64));
                    }
                }
            }
        }
        Strategy::Papaya => {
            for operation in 0..operations {
                let index = operation % access_keys.len();
                match operation_kind {
                    Operation::Read => {
                        black_box(papaya.get(access_keys[index], &papaya_guard).copied());
                    }
                    Operation::Update => {
                        black_box(papaya.update(
                            access_keys[index].into(),
                            |value| value + 1,
                            &papaya_guard,
                        ));
                    }
                    Operation::Replace => {
                        black_box(papaya.update(
                            access_keys[index].into(),
                            |_| operation as u64,
                            &papaya_guard,
                        ));
                    }
                    Operation::Insert => {
                        black_box(papaya.insert(
                            access_keys[index].into(),
                            operation as u64,
                            &papaya_guard,
                        ));
                    }
                }
            }
        }
    }
    started.elapsed()
}

fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("benchmark value must not use deletion marker")
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
