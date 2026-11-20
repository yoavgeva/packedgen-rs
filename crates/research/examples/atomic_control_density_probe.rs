//! Atomic throughput and RAM screen for byte and nibble bucket controls.

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
const WRITING: u8 = 1;

#[derive(Clone, Copy)]
enum Design {
    Byte8x4,
    Nibble16x3,
    Nibble16x4,
}

impl Design {
    const ALL: [Self; 3] = [Self::Byte8x4, Self::Nibble16x3, Self::Nibble16x4];

    const fn name(self) -> &'static str {
        match self {
            Self::Byte8x4 => "byte-control-8-slots-4-choices",
            Self::Nibble16x3 => "nibble-control-16-slots-3-choices",
            Self::Nibble16x4 => "nibble-control-16-slots-4-choices",
        }
    }

    fn measure(self, entries: usize, operations: usize, threads: usize) -> Measurement {
        match self {
            Self::Byte8x4 => measure::<8, 8, 4>(entries, operations, threads),
            Self::Nibble16x3 => measure::<16, 4, 3>(entries, operations, threads),
            Self::Nibble16x4 => measure::<16, 4, 4>(entries, operations, threads),
        }
    }
}

struct Table<const SLOTS: usize, const LANE_BITS: usize, const CHOICES: usize> {
    controls: Box<[AtomicU64]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
}

impl<const SLOTS: usize, const LANE_BITS: usize, const CHOICES: usize>
    Table<SLOTS, LANE_BITS, CHOICES>
{
    fn with_capacity(capacity: usize) -> Self {
        assert_eq!(SLOTS * LANE_BITS, u64::BITS as usize);
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots.div_ceil(SLOTS).max(CHOICES);
        Self {
            controls: (0..buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            keys: (0..buckets * SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
        }
    }

    fn insert(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag::<LANE_BITS>(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                let (empty, writing) = frontier::<SLOTS, LANE_BITS>(control);
                let stop = writing.or(empty).unwrap_or(SLOTS);
                let mut candidates = matching_lanes::<LANE_BITS>(control, tag);
                while let Some(offset) = take_offset::<LANE_BITS>(&mut candidates) {
                    if offset >= stop {
                        break;
                    }
                    if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                        return false;
                    }
                }
                if writing.is_some() {
                    spin_loop();
                    continue;
                }
                let Some(offset) = empty else {
                    break;
                };
                let shift = offset * LANE_BITS;
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
                self.keys[bucket * SLOTS + offset].store(encoded, Ordering::Relaxed);
                let lane_mask = lane_mask::<LANE_BITS>() << shift;
                self.controls[bucket].store(
                    (control & !lane_mask) | (u64::from(tag) << shift),
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
        let tag = slot_tag::<LANE_BITS>(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let (empty, writing) = frontier::<SLOTS, LANE_BITS>(control);
            let stop = writing.or(empty).unwrap_or(SLOTS);
            let mut candidates = matching_lanes::<LANE_BITS>(control, tag);
            while let Some(offset) = take_offset::<LANE_BITS>(&mut candidates) {
                if offset >= stop {
                    break;
                }
                if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if stop < SLOTS {
                return false;
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let (empty, writing) = frontier::<SLOTS, LANE_BITS>(control);
            assert!(writing.is_none());
            let occupied = empty.unwrap_or(SLOTS);
            for offset in 0..occupied {
                let encoded = self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed);
                keys.push(encoded.wrapping_sub(1));
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
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 5_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 15).max(3);
    println!(
        "design,entries,operations,threads,samples,insert_median_mops,insert_p05_mops,hit_median_mops,hit_p05_mops,miss_median_mops,miss_p05_mops,overflow_median,primary_bytes_per_placed"
    );
    let mut measurements: [Vec<Measurement>; Design::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Design::ALL.len() {
            let index = (sample + offset) % Design::ALL.len();
            measurements[index].push(Design::ALL[index].measure(entries, operations, threads));
        }
    }
    for (design, measured) in Design::ALL.into_iter().zip(measurements) {
        let (insert, insert_p05) = metric(&measured, |measurement| measurement.insert);
        let (hit, hit_p05) = metric(&measured, |measurement| measurement.hit);
        let (miss, miss_p05) = metric(&measured, |measurement| measurement.miss);
        let mut overflows = measured
            .iter()
            .map(|measurement| measurement.overflow)
            .collect::<Vec<_>>();
        overflows.sort_unstable();
        let bytes_per_placed = measured
            .iter()
            .map(|measurement| measurement.bytes_per_placed)
            .sum::<f64>()
            / measured.len() as f64;
        println!(
            "{},{entries},{operations},{threads},{samples},{insert:.3},{insert_p05:.3},{hit:.3},{hit_p05:.3},{miss:.3},{miss_p05:.3},{},{bytes_per_placed:.3}",
            design.name(),
            overflows[overflows.len() / 2],
        );
    }
}

fn measure<const SLOTS: usize, const LANE_BITS: usize, const CHOICES: usize>(
    entries: usize,
    operations: usize,
    threads: usize,
) -> Measurement {
    let table = Arc::new(Table::<SLOTS, LANE_BITS, CHOICES>::with_capacity(entries));
    let started = Instant::now();
    run_workers(entries, threads, {
        let table = Arc::clone(&table);
        move |index| u64::from(table.insert(index as u64))
    });
    let insert = entries as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;
    let placed = table.placed_keys();
    let overflow = entries - placed.len();
    let bytes_per_placed = table.requested_bytes() as f64 / placed.len() as f64;

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
    }
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

fn bucket_choices<const CHOICES: usize>(hash: u64, buckets: usize) -> [usize; CHOICES] {
    let mut choices = [0_usize; CHOICES];
    for attempt in 0..CHOICES {
        let variant = match attempt {
            0 => hash,
            1 => hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15,
            2 => hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93,
            3 => hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f,
            _ => unreachable!("probe uses at most four choices"),
        };
        let mut candidate = reduce(variant, buckets);
        while choices[..attempt].contains(&candidate) {
            candidate = next_index(candidate, buckets);
        }
        choices[attempt] = candidate;
    }
    choices
}

fn frontier<const SLOTS: usize, const LANE_BITS: usize>(
    control: u64,
) -> (Option<usize>, Option<usize>) {
    let empty = first_offset::<LANE_BITS>(control, 0);
    let frontier = empty.unwrap_or(SLOTS);
    let writing = frontier
        .checked_sub(1)
        .filter(|offset| control_lane::<LANE_BITS>(control, *offset) == WRITING);
    (empty, writing)
}

fn matching_lanes<const LANE_BITS: usize>(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * lane_ones::<LANE_BITS>());
    !((different & lane_low_bits::<LANE_BITS>()).wrapping_add(lane_low_bits::<LANE_BITS>())
        | different
        | lane_low_bits::<LANE_BITS>())
        & lane_high_bits::<LANE_BITS>()
}

fn first_offset<const LANE_BITS: usize>(control: u64, state: u8) -> Option<usize> {
    let matches = matching_lanes::<LANE_BITS>(control, state);
    (matches != 0).then(|| matches.trailing_zeros() as usize / LANE_BITS)
}

fn take_offset<const LANE_BITS: usize>(matches: &mut u64) -> Option<usize> {
    if *matches == 0 {
        return None;
    }
    let offset = matches.trailing_zeros() as usize / LANE_BITS;
    *matches &= matches.wrapping_sub(1);
    Some(offset)
}

fn control_lane<const LANE_BITS: usize>(control: u64, offset: usize) -> u8 {
    ((control >> (offset * LANE_BITS)) & lane_mask::<LANE_BITS>()) as u8
}

fn slot_tag<const LANE_BITS: usize>(hash: u64) -> u8 {
    let states = lane_mask::<LANE_BITS>() - 1;
    u8::try_from(hash % states).expect("tag fits its control lane") + 2
}

const fn lane_mask<const LANE_BITS: usize>() -> u64 {
    (1_u64 << LANE_BITS) - 1
}

const fn lane_ones<const LANE_BITS: usize>() -> u64 {
    match LANE_BITS {
        4 => 0x1111_1111_1111_1111,
        8 => 0x0101_0101_0101_0101,
        _ => panic!("probe supports four- or eight-bit lanes"),
    }
}

const fn lane_low_bits<const LANE_BITS: usize>() -> u64 {
    match LANE_BITS {
        4 => 0x7777_7777_7777_7777,
        8 => 0x7f7f_7f7f_7f7f_7f7f,
        _ => panic!("probe supports four- or eight-bit lanes"),
    }
}

const fn lane_high_bits<const LANE_BITS: usize>() -> u64 {
    match LANE_BITS {
        4 => 0x8888_8888_8888_8888,
        8 => 0x8080_8080_8080_8080,
        _ => panic!("probe supports four- or eight-bit lanes"),
    }
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
