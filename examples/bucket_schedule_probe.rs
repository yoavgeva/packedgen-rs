//! Placement and atomic-throughput census for cheaper four-bucket schedules.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::{black_box, spin_loop};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const TARGET_NUMERATOR: usize = 23;
const TARGET_DENOMINATOR: usize = 20;
const BUCKET_SLOTS: usize = 8;
const CHOICES: usize = 4;
const BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const WRITING: u8 = 1;

#[derive(Clone, Copy)]
enum Schedule {
    Independent,
    IndependentPairStep,
    FixedStep,
    StaggeredStep,
}

impl Schedule {
    const ALL: [Self; 4] = [
        Self::Independent,
        Self::IndependentPairStep,
        Self::FixedStep,
        Self::StaggeredStep,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Independent => "independent-four-reductions",
            Self::IndependentPairStep => "independent-pair-step-two-reductions",
            Self::FixedStep => "fixed-step-two-reductions",
            Self::StaggeredStep => "staggered-step-two-reductions",
        }
    }

    fn choices(self, hash: u64, buckets: usize) -> [usize; CHOICES] {
        match self {
            Self::Independent => independent_choices(hash, buckets),
            Self::IndependentPairStep => independent_pair_step_choices(hash, buckets),
            Self::FixedStep => stepped_choices(hash, buckets, false),
            Self::StaggeredStep => stepped_choices(hash, buckets, true),
        }
    }
}

struct Table {
    controls: Box<[AtomicU64]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
    schedule: Schedule,
}

impl Table {
    fn with_capacity(capacity: usize, schedule: Schedule) -> Self {
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots.div_ceil(BUCKET_SLOTS).max(CHOICES);
        Self {
            controls: (0..buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            keys: (0..buckets * BUCKET_SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
            schedule,
        }
    }

    fn insert(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in self.schedule.choices(hash, self.buckets) {
            loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                if matching_bytes(control, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(control, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * BUCKET_SLOTS + offset].load(Ordering::Relaxed) == encoded
                    {
                        return false;
                    }
                }
                let Some(offset) = first_offset(control, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                if self.controls[bucket]
                    .compare_exchange(
                        control,
                        control | (u64::from(WRITING) << shift),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue;
                }
                self.keys[bucket * BUCKET_SLOTS + offset].store(encoded, Ordering::Relaxed);
                let byte_mask = u64::from(u8::MAX) << shift;
                self.controls[bucket].store(
                    (control & !byte_mask) | (u64::from(tag) << shift),
                    Ordering::Release,
                );
                return true;
            }
        }
        false
    }

    fn contains(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in self.schedule.choices(hash, self.buckets) {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let mut candidates = matching_bytes(control, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * BUCKET_SLOTS + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if first_offset(control, 0).is_some() {
                return false;
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            let control = self.controls[bucket].load(Ordering::Acquire);
            for offset in 0..BUCKET_SLOTS {
                if control_byte(control, offset) >= 2 {
                    let encoded = self.keys[bucket * BUCKET_SLOTS + offset].load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
        }
        keys.into_boxed_slice()
    }

    fn requested_bytes(&self) -> usize {
        self.controls.len() * size_of::<AtomicU64>() + self.keys.len() * size_of::<AtomicU64>()
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    insert: f64,
    hit: f64,
    miss: f64,
    overflow: usize,
    bytes_per_placed: f64,
    average_probes: f64,
    first_choice_percent: f64,
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 5_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    println!(
        "schedule,entries,operations,threads,samples,insert_median_mops,insert_p05_mops,hit_median_mops,hit_p05_mops,miss_median_mops,miss_p05_mops,overflow_median,primary_bytes_per_placed,average_probes,first_choice_percent"
    );
    let mut measurements: [Vec<Measurement>; Schedule::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Schedule::ALL.len() {
            let index = (sample + offset) % Schedule::ALL.len();
            measurements[index].push(measure(entries, operations, threads, Schedule::ALL[index]));
        }
    }
    for (schedule, measured) in Schedule::ALL.into_iter().zip(measurements) {
        let (insert, insert_p05) = metric(&measured, |measurement| measurement.insert);
        let (hit, hit_p05) = metric(&measured, |measurement| measurement.hit);
        let (miss, miss_p05) = metric(&measured, |measurement| measurement.miss);
        let mut overflows = measured
            .iter()
            .map(|measurement| measurement.overflow)
            .collect::<Vec<_>>();
        overflows.sort_unstable();
        let mean = |field: fn(&Measurement) -> f64| {
            measured.iter().map(field).sum::<f64>() / measured.len() as f64
        };
        println!(
            "{},{entries},{operations},{threads},{samples},{insert:.3},{insert_p05:.3},{hit:.3},{hit_p05:.3},{miss:.3},{miss_p05:.3},{},{:.3},{:.4},{:.4}",
            schedule.name(),
            overflows[overflows.len() / 2],
            mean(|measurement| measurement.bytes_per_placed),
            mean(|measurement| measurement.average_probes),
            mean(|measurement| measurement.first_choice_percent),
        );
    }
}

fn measure(entries: usize, operations: usize, threads: usize, schedule: Schedule) -> Measurement {
    let table = Arc::new(Table::with_capacity(entries, schedule));
    let started = Instant::now();
    run_workers(entries, threads, {
        let table = Arc::clone(&table);
        move |index| u64::from(table.insert(index as u64))
    });
    let insert = entries as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;
    let placed = table.placed_keys();
    let overflow = entries - placed.len();
    let bytes_per_placed = table.requested_bytes() as f64 / placed.len() as f64;
    let (average_probes, first_choice_percent) = placement_census(entries, schedule);

    let started = Instant::now();
    run_workers(operations, threads, {
        let table = Arc::clone(&table);
        let placed = &placed;
        move |operation| {
            let index = mix(operation as u64) as usize % placed.len();
            u64::from(table.contains(placed[index]))
        }
    });
    let hit = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, move |operation| {
        u64::from(table.contains(entries.wrapping_add(operation) as u64))
    });
    let miss = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    Measurement {
        insert,
        hit,
        miss,
        overflow,
        bytes_per_placed,
        average_probes,
        first_choice_percent,
    }
}

fn placement_census(entries: usize, schedule: Schedule) -> (f64, f64) {
    let target_slots = entries
        .saturating_mul(TARGET_NUMERATOR)
        .div_ceil(TARGET_DENOMINATOR);
    let buckets = target_slots.div_ceil(BUCKET_SLOTS).max(CHOICES);
    let mut occupied = vec![0_u8; buckets];
    let mut probes = 0_usize;
    let mut first_choice = 0_usize;
    for key in 0..entries {
        let hash = mix(key as u64 + 1);
        for (attempt, bucket) in schedule.choices(hash, buckets).into_iter().enumerate() {
            probes += 1;
            if usize::from(occupied[bucket]) < BUCKET_SLOTS {
                occupied[bucket] += 1;
                first_choice += usize::from(attempt == 0);
                break;
            }
        }
    }
    (
        probes as f64 / entries as f64,
        first_choice as f64 * 100.0 / entries as f64,
    )
}

fn run_workers(work: usize, threads: usize, operation: impl Fn(usize) -> u64 + Sync) {
    let barrier = Barrier::new(threads + 1);
    let checksum = AtomicU64::new(0);
    thread::scope(|scope| {
        for worker in 0..threads {
            let begin = work * worker / threads;
            let end = work * (worker + 1) / threads;
            let barrier = &barrier;
            let checksum = &checksum;
            let operation = &operation;
            scope.spawn(move || {
                barrier.wait();
                let mut local = 0_u64;
                for index in begin..end {
                    local ^= operation(index);
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
            });
        }
        barrier.wait();
    });
    black_box(checksum.load(Ordering::Relaxed));
}

fn independent_choices(hash: u64, buckets: usize) -> [usize; CHOICES] {
    let variants = [
        hash,
        hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15,
        hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93,
        hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f,
    ];
    let mut choices = [0_usize; CHOICES];
    for (attempt, variant) in variants.into_iter().enumerate() {
        let mut candidate = reduce(variant, buckets);
        while choices[..attempt].contains(&candidate) {
            candidate = next_index(candidate, buckets);
        }
        choices[attempt] = candidate;
    }
    choices
}

fn independent_pair_step_choices(hash: u64, buckets: usize) -> [usize; CHOICES] {
    let primary = reduce(hash, buckets);
    let secondary_hash = hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15;
    let mut secondary = reduce(secondary_hash, buckets);
    if secondary == primary {
        secondary = next_index(secondary, buckets);
    }
    let step = if secondary >= primary {
        secondary - primary
    } else {
        buckets - (primary - secondary)
    };
    let mut choices = [primary, secondary, 0, 0];
    for attempt in 2..CHOICES {
        let mut candidate = add_wrapped(choices[attempt - 1], step, buckets);
        while choices[..attempt].contains(&candidate) {
            candidate = next_index(candidate, buckets);
        }
        choices[attempt] = candidate;
    }
    choices
}

fn stepped_choices(hash: u64, buckets: usize, staggered: bool) -> [usize; CHOICES] {
    let mut choices = [0_usize; CHOICES];
    choices[0] = reduce(hash, buckets);
    let mut step = reduce(hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15, buckets - 1) + 1;
    for attempt in 1..CHOICES {
        let mut candidate = add_wrapped(choices[attempt - 1], step, buckets);
        while choices[..attempt].contains(&candidate) {
            candidate = next_index(candidate, buckets);
        }
        choices[attempt] = candidate;
        if staggered {
            step = next_index(step, buckets);
            if step == 0 {
                step = 1;
            }
        }
    }
    choices
}

fn add_wrapped(index: usize, offset: usize, upper: usize) -> usize {
    let remaining = upper - index;
    if offset >= remaining {
        offset - remaining
    } else {
        index + offset
    }
}

fn matching_bytes(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * BYTE_ONES);
    !((different & BYTE_LOW_BITS).wrapping_add(BYTE_LOW_BITS) | different | BYTE_LOW_BITS)
        & BYTE_HIGH_BITS
}

fn first_offset(control: u64, state: u8) -> Option<usize> {
    let matches = matching_bytes(control, state);
    (matches != 0).then(|| matches.trailing_zeros() as usize / u8::BITS as usize)
}

fn take_offset(matches: &mut u64) -> Option<usize> {
    if *matches == 0 {
        return None;
    }
    let offset = matches.trailing_zeros() as usize / u8::BITS as usize;
    *matches &= matches.wrapping_sub(1);
    Some(offset)
}

fn control_byte(control: u64, offset: usize) -> u8 {
    (control >> (offset * u8::BITS as usize)) as u8
}

fn slot_tag(hash: u64) -> u8 {
    u8::try_from(hash % 254).expect("tag fits u8") + 2
}

fn reduce(hash: u64, upper: usize) -> usize {
    ((u128::from(hash) * upper as u128) >> 64) as usize
}

const fn next_index(index: usize, upper: usize) -> usize {
    let next = index + 1;
    if next == upper { 0 } else { next }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn metric(measurements: &[Measurement], field: impl Fn(&Measurement) -> f64) -> (f64, f64) {
    let mut values = measurements.iter().map(field).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    (values[values.len() / 2], values[values.len() / 20])
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}
