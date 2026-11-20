//! Atomic throughput comparison for four- and eight-slot bucket controls.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::{black_box, spin_loop};
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const TARGET_NUMERATOR: usize = 23;
const TARGET_DENOMINATOR: usize = 20;
const BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const WRITING: u8 = 1;

trait AtomicControl: Send + Sync + 'static {
    fn empty() -> Self;
    fn load_acquire(&self) -> u64;
    fn compare_exchange_relaxed(&self, current: u64, new: u64) -> bool;
    fn store_release(&self, value: u64);
}

impl AtomicControl for AtomicU32 {
    fn empty() -> Self {
        Self::new(0)
    }

    fn load_acquire(&self) -> u64 {
        u64::from(self.load(Ordering::Acquire))
    }

    fn compare_exchange_relaxed(&self, current: u64, new: u64) -> bool {
        self.compare_exchange(
            current as u32,
            new as u32,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
        .is_ok()
    }

    fn store_release(&self, value: u64) {
        self.store(value as u32, Ordering::Release);
    }
}

impl AtomicControl for AtomicU64 {
    fn empty() -> Self {
        Self::new(0)
    }

    fn load_acquire(&self) -> u64 {
        self.load(Ordering::Acquire)
    }

    fn compare_exchange_relaxed(&self, current: u64, new: u64) -> bool {
        self.compare_exchange(current, new, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    fn store_release(&self, value: u64) {
        self.store(value, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
enum Geometry {
    FourBySix,
    EightByFour,
}

impl Geometry {
    const ALL: [Self; 2] = [Self::FourBySix, Self::EightByFour];

    const fn name(self) -> &'static str {
        match self {
            Self::FourBySix => "atomic-u32-4-slots-6-choices",
            Self::EightByFour => "atomic-u64-8-slots-4-choices",
        }
    }

    fn measure(self, entries: usize, operations: usize, threads: usize) -> Measurement {
        match self {
            Self::FourBySix => measure::<AtomicU32, 4>(entries, operations, threads, 6),
            Self::EightByFour => measure::<AtomicU64, 8>(entries, operations, threads, 4),
        }
    }
}

struct Table<A, const SLOTS: usize> {
    controls: Box<[A]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
    choices: usize,
}

impl<A: AtomicControl, const SLOTS: usize> Table<A, SLOTS> {
    fn with_capacity(capacity: usize, choices: usize) -> Self {
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots.div_ceil(SLOTS).max(choices);
        Self {
            controls: (0..buckets)
                .map(|_| A::empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            keys: (0..buckets * SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
            choices,
        }
    }

    fn insert(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        let buckets = bucket_choices(hash, self.buckets);
        for bucket in buckets.into_iter().take(self.choices) {
            loop {
                let control = self.controls[bucket].load_acquire();
                if matching_bytes::<SLOTS>(control, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes::<SLOTS>(control, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                        return false;
                    }
                }
                let Some(offset) = first_offset::<SLOTS>(control, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                if !self.controls[bucket]
                    .compare_exchange_relaxed(control, control | (u64::from(WRITING) << shift))
                {
                    continue;
                }
                self.keys[bucket * SLOTS + offset].store(encoded, Ordering::Relaxed);
                let byte_mask = u64::from(u8::MAX) << shift;
                self.controls[bucket]
                    .store_release((control & !byte_mask) | (u64::from(tag) << shift));
                return true;
            }
        }
        false
    }

    fn contains(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        let buckets = bucket_choices(hash, self.buckets);
        for bucket in buckets.into_iter().take(self.choices) {
            let control = self.controls[bucket].load_acquire();
            let mut candidates = matching_bytes::<SLOTS>(control, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if first_offset::<SLOTS>(control, 0).is_some() {
                return false;
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            let control = self.controls[bucket].load_acquire();
            for offset in 0..SLOTS {
                if control_byte(control, offset) >= 2 {
                    let encoded = self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
        }
        keys.into_boxed_slice()
    }

    fn requested_bytes(&self) -> usize {
        self.controls.len() * size_of::<A>() + self.keys.len() * size_of::<AtomicU64>()
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
    let samples = argument(&mut arguments, 9).max(3);
    println!(
        "geometry,entries,operations,threads,samples,insert_median_mops,insert_p05_mops,hit_median_mops,hit_p05_mops,miss_median_mops,miss_p05_mops,overflow_median,primary_bytes_per_placed"
    );
    let mut measurements: [Vec<Measurement>; Geometry::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Geometry::ALL.len() {
            let index = (sample + offset) % Geometry::ALL.len();
            measurements[index].push(Geometry::ALL[index].measure(entries, operations, threads));
        }
    }
    for (geometry, measured) in Geometry::ALL.into_iter().zip(measurements) {
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
            geometry.name(),
            overflows[overflows.len() / 2],
        );
    }
}

fn measure<A: AtomicControl, const SLOTS: usize>(
    entries: usize,
    operations: usize,
    threads: usize,
    choices: usize,
) -> Measurement {
    let table = Arc::new(Table::<A, SLOTS>::with_capacity(entries, choices));
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

fn bucket_choices(hash: u64, buckets: usize) -> [usize; 8] {
    let mut choices = [0_usize; 8];
    for attempt in 0..choices.len() {
        let variant = match attempt {
            0 => hash,
            1 => hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15,
            2 => hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93,
            3 => hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f,
            _ => mix(hash ^ (attempt as u64).wrapping_mul(0xe703_7ed1_a0b4_28db)),
        };
        let mut candidate = reduce(variant, buckets);
        while choices[..attempt].contains(&candidate) {
            candidate = if candidate + 1 == buckets {
                0
            } else {
                candidate + 1
            };
        }
        choices[attempt] = candidate;
    }
    choices
}

fn matching_bytes<const SLOTS: usize>(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * BYTE_ONES);
    let matches =
        !((different & BYTE_LOW_BITS).wrapping_add(BYTE_LOW_BITS) | different | BYTE_LOW_BITS)
            & BYTE_HIGH_BITS;
    matches & valid_high_bits::<SLOTS>()
}

const fn valid_high_bits<const SLOTS: usize>() -> u64 {
    BYTE_HIGH_BITS >> ((8 - SLOTS) * u8::BITS as usize)
}

fn first_offset<const SLOTS: usize>(control: u64, state: u8) -> Option<usize> {
    let matches = matching_bytes::<SLOTS>(control, state);
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
