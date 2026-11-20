//! Atomic throughput and RAM screen for eight- and sixteen-slot byte buckets.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::{black_box, spin_loop};
use std::mem::size_of;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const TARGET_NUMERATOR: usize = 23;
const TARGET_DENOMINATOR: usize = 20;
const SLOTS_PER_CONTROL: usize = 8;
const BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const WRITING: u8 = 1;

trait TailControl: Send + Sync + 'static {
    fn empty() -> Self;
    fn load_acquire(&self) -> u64;
    fn compare_exchange_relaxed(&self, current: u64, new: u64) -> bool;
    fn store_release(&self, value: u64);
}

impl TailControl for AtomicU16 {
    fn empty() -> Self {
        Self::new(0)
    }

    fn load_acquire(&self) -> u64 {
        u64::from(self.load(Ordering::Acquire))
    }

    fn compare_exchange_relaxed(&self, current: u64, new: u64) -> bool {
        self.compare_exchange(
            current as u16,
            new as u16,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
        .is_ok()
    }

    fn store_release(&self, value: u64) {
        self.store(value as u16, Ordering::Release);
    }
}

impl TailControl for AtomicU32 {
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

#[derive(Clone, Copy)]
enum Design {
    Byte8x4,
    Byte10x4,
    Byte10x4Grouped,
    Byte12x4,
    Byte16x3,
    Byte16x3Unrolled,
    Byte16x4,
}

impl Design {
    const ALL: [Self; 7] = [
        Self::Byte8x4,
        Self::Byte10x4,
        Self::Byte10x4Grouped,
        Self::Byte12x4,
        Self::Byte16x3,
        Self::Byte16x3Unrolled,
        Self::Byte16x4,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Byte8x4 => "byte-control-8-slots-4-choices",
            Self::Byte10x4 => "byte-control-10-slots-4-choices-split",
            Self::Byte10x4Grouped => "byte-control-10-slots-4-choices-grouped",
            Self::Byte12x4 => "byte-control-12-slots-4-choices-split",
            Self::Byte16x3 => "byte-control-16-slots-3-choices",
            Self::Byte16x3Unrolled => "byte-control-16-slots-3-choices-unrolled",
            Self::Byte16x4 => "byte-control-16-slots-4-choices",
        }
    }

    fn measure(self, entries: usize, operations: usize, threads: usize) -> Measurement {
        match self {
            Self::Byte8x4 => measure::<8, 4>(entries, operations, threads),
            Self::Byte10x4 => measure_split::<AtomicU16, 2, 10, 4>(entries, operations, threads),
            Self::Byte10x4Grouped => measure_grouped10(entries, operations, threads),
            Self::Byte12x4 => measure_split::<AtomicU32, 4, 12, 4>(entries, operations, threads),
            Self::Byte16x3 => measure::<16, 3>(entries, operations, threads),
            Self::Byte16x3Unrolled => measure_wide(entries, operations, threads),
            Self::Byte16x4 => measure::<16, 4>(entries, operations, threads),
        }
    }
}

struct Table<const SLOTS: usize, const CHOICES: usize> {
    controls: Box<[AtomicU64]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
}

impl<const SLOTS: usize, const CHOICES: usize> Table<SLOTS, CHOICES> {
    const CONTROL_WORDS: usize = SLOTS / SLOTS_PER_CONTROL;

    fn with_capacity(capacity: usize) -> Self {
        assert!(SLOTS == 8 || SLOTS == 16);
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots.div_ceil(SLOTS).max(CHOICES);
        Self {
            controls: (0..buckets * Self::CONTROL_WORDS)
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
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            'retry_bucket: loop {
                for word_index in 0..Self::CONTROL_WORDS {
                    let control_index = bucket * Self::CONTROL_WORDS + word_index;
                    let control = self.controls[control_index].load(Ordering::Acquire);
                    if matching_bytes(control, WRITING) != 0 {
                        spin_loop();
                        continue 'retry_bucket;
                    }
                    let mut candidates = matching_bytes(control, tag);
                    while let Some(word_offset) = take_offset(&mut candidates) {
                        let offset = word_index * SLOTS_PER_CONTROL + word_offset;
                        if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                            return false;
                        }
                    }
                    let Some(word_offset) = first_offset(control, 0) else {
                        continue;
                    };
                    let shift = word_offset * u8::BITS as usize;
                    if self.controls[control_index]
                        .compare_exchange(
                            control,
                            control | (u64::from(WRITING) << shift),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        continue 'retry_bucket;
                    }
                    let offset = word_index * SLOTS_PER_CONTROL + word_offset;
                    self.keys[bucket * SLOTS + offset].store(encoded, Ordering::Relaxed);
                    let byte_mask = u64::from(u8::MAX) << shift;
                    self.controls[control_index].store(
                        (control & !byte_mask) | (u64::from(tag) << shift),
                        Ordering::Release,
                    );
                    return true;
                }
                break;
            }
        }
        false
    }

    fn contains(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            for word_index in 0..Self::CONTROL_WORDS {
                let control = self.controls[bucket * Self::CONTROL_WORDS + word_index]
                    .load(Ordering::Acquire);
                let mut candidates = matching_bytes(control, tag);
                while let Some(word_offset) = take_offset(&mut candidates) {
                    let offset = word_index * SLOTS_PER_CONTROL + word_offset;
                    if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                        return true;
                    }
                }
                if first_offset(control, 0).is_some() {
                    return false;
                }
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            for word_index in 0..Self::CONTROL_WORDS {
                let control = self.controls[bucket * Self::CONTROL_WORDS + word_index]
                    .load(Ordering::Acquire);
                for word_offset in 0..SLOTS_PER_CONTROL {
                    if control_byte(control, word_offset) >= 2 {
                        let offset = word_index * SLOTS_PER_CONTROL + word_offset;
                        let encoded = self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed);
                        keys.push(encoded.wrapping_sub(1));
                    }
                }
            }
        }
        keys.into_boxed_slice()
    }

    fn requested_bytes(&self) -> usize {
        self.controls.len() * size_of::<AtomicU64>() + self.keys.len() * size_of::<AtomicU64>()
    }
}

impl Table<16, 3> {
    fn insert_unrolled(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<3>(hash, self.buckets) {
            'retry_bucket: loop {
                let first_index = bucket * 2;
                let first = self.controls[first_index].load(Ordering::Acquire);
                if matching_bytes(first, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(first, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * 16 + offset].load(Ordering::Relaxed) == encoded {
                        return false;
                    }
                }
                if let Some(offset) = first_offset(first, 0) {
                    let shift = offset * u8::BITS as usize;
                    if self.controls[first_index]
                        .compare_exchange(
                            first,
                            first | (u64::from(WRITING) << shift),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        continue 'retry_bucket;
                    }
                    self.keys[bucket * 16 + offset].store(encoded, Ordering::Relaxed);
                    let byte_mask = u64::from(u8::MAX) << shift;
                    self.controls[first_index].store(
                        (first & !byte_mask) | (u64::from(tag) << shift),
                        Ordering::Release,
                    );
                    return true;
                }

                let second_index = first_index + 1;
                let second = self.controls[second_index].load(Ordering::Acquire);
                if matching_bytes(second, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(second, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * 16 + SLOTS_PER_CONTROL + offset].load(Ordering::Relaxed)
                        == encoded
                    {
                        return false;
                    }
                }
                let Some(offset) = first_offset(second, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                if self.controls[second_index]
                    .compare_exchange(
                        second,
                        second | (u64::from(WRITING) << shift),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue 'retry_bucket;
                }
                self.keys[bucket * 16 + SLOTS_PER_CONTROL + offset]
                    .store(encoded, Ordering::Relaxed);
                let byte_mask = u64::from(u8::MAX) << shift;
                self.controls[second_index].store(
                    (second & !byte_mask) | (u64::from(tag) << shift),
                    Ordering::Release,
                );
                return true;
            }
        }
        false
    }

    fn contains_unrolled(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<3>(hash, self.buckets) {
            let first_index = bucket * 2;
            let first = self.controls[first_index].load(Ordering::Acquire);
            let mut candidates = matching_bytes(first, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * 16 + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if first_offset(first, 0).is_some() {
                return false;
            }

            let second = self.controls[first_index + 1].load(Ordering::Acquire);
            let mut candidates = matching_bytes(second, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * 16 + SLOTS_PER_CONTROL + offset].load(Ordering::Relaxed)
                    == encoded
                {
                    return true;
                }
            }
            if first_offset(second, 0).is_some() {
                return false;
            }
        }
        false
    }
}

struct SplitTable<T, const TAIL: usize, const SLOTS: usize, const CHOICES: usize> {
    first_controls: Box<[AtomicU64]>,
    tail_controls: Box<[T]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
}

impl<T: TailControl, const TAIL: usize, const SLOTS: usize, const CHOICES: usize>
    SplitTable<T, TAIL, SLOTS, CHOICES>
{
    fn with_capacity(capacity: usize) -> Self {
        assert_eq!(SLOTS, SLOTS_PER_CONTROL + TAIL);
        assert!(TAIL == 2 || TAIL == 4);
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots.div_ceil(SLOTS).max(CHOICES);
        Self {
            first_controls: (0..buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            tail_controls: (0..buckets)
                .map(|_| T::empty())
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
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            'retry_bucket: loop {
                let first = self.first_controls[bucket].load(Ordering::Acquire);
                if matching_bytes(first, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(first, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                        return false;
                    }
                }
                if let Some(offset) = first_offset(first, 0) {
                    let shift = offset * u8::BITS as usize;
                    if self.first_controls[bucket]
                        .compare_exchange(
                            first,
                            first | (u64::from(WRITING) << shift),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        continue 'retry_bucket;
                    }
                    self.keys[bucket * SLOTS + offset].store(encoded, Ordering::Relaxed);
                    let byte_mask = u64::from(u8::MAX) << shift;
                    self.first_controls[bucket].store(
                        (first & !byte_mask) | (u64::from(tag) << shift),
                        Ordering::Release,
                    );
                    return true;
                }

                let tail = self.tail_controls[bucket].load_acquire();
                if matching_tail::<TAIL>(tail, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_tail::<TAIL>(tail, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * SLOTS + SLOTS_PER_CONTROL + offset]
                        .load(Ordering::Relaxed)
                        == encoded
                    {
                        return false;
                    }
                }
                let Some(offset) = first_tail_offset::<TAIL>(tail, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                if !self.tail_controls[bucket]
                    .compare_exchange_relaxed(tail, tail | (u64::from(WRITING) << shift))
                {
                    continue 'retry_bucket;
                }
                self.keys[bucket * SLOTS + SLOTS_PER_CONTROL + offset]
                    .store(encoded, Ordering::Relaxed);
                let byte_mask = u64::from(u8::MAX) << shift;
                self.tail_controls[bucket]
                    .store_release((tail & !byte_mask) | (u64::from(tag) << shift));
                return true;
            }
        }
        false
    }

    fn contains(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<CHOICES>(hash, self.buckets) {
            let first = self.first_controls[bucket].load(Ordering::Acquire);
            let mut candidates = matching_bytes(first, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if first_offset(first, 0).is_some() {
                return false;
            }

            let tail = self.tail_controls[bucket].load_acquire();
            let mut candidates = matching_tail::<TAIL>(tail, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * SLOTS + SLOTS_PER_CONTROL + offset].load(Ordering::Relaxed)
                    == encoded
                {
                    return true;
                }
            }
            if first_tail_offset::<TAIL>(tail, 0).is_some() {
                return false;
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            let first = self.first_controls[bucket].load(Ordering::Acquire);
            for offset in 0..SLOTS_PER_CONTROL {
                if control_byte(first, offset) >= 2 {
                    let encoded = self.keys[bucket * SLOTS + offset].load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
            let tail = self.tail_controls[bucket].load_acquire();
            for offset in 0..TAIL {
                if control_byte(tail, offset) >= 2 {
                    let encoded = self.keys[bucket * SLOTS + SLOTS_PER_CONTROL + offset]
                        .load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
        }
        keys.into_boxed_slice()
    }

    fn requested_bytes(&self) -> usize {
        self.first_controls.len() * size_of::<AtomicU64>()
            + self.tail_controls.len() * size_of::<T>()
            + self.keys.len() * size_of::<AtomicU64>()
    }
}

struct Control10Group {
    first: [AtomicU64; 4],
    tail: [AtomicU16; 4],
}

impl Control10Group {
    fn new() -> Self {
        Self {
            first: std::array::from_fn(|_| AtomicU64::new(0)),
            tail: std::array::from_fn(|_| AtomicU16::new(0)),
        }
    }
}

struct GroupedTable10 {
    controls: Box<[Control10Group]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
}

impl GroupedTable10 {
    const SLOTS: usize = 10;
    const CHOICES: usize = 4;

    fn with_capacity(capacity: usize) -> Self {
        assert_eq!(size_of::<Control10Group>(), 40);
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = target_slots
            .div_ceil(Self::SLOTS)
            .max(Self::CHOICES)
            .next_multiple_of(4);
        Self {
            controls: (0..buckets / 4)
                .map(|_| Control10Group::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            keys: (0..buckets * Self::SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
        }
    }

    fn bucket_controls(&self, bucket: usize) -> (&AtomicU64, &AtomicU16) {
        let group = &self.controls[bucket / 4];
        let offset = bucket % 4;
        (&group.first[offset], &group.tail[offset])
    }

    fn insert(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = slot_tag(hash);
        for bucket in bucket_choices::<4>(hash, self.buckets) {
            let (first_control, tail_control) = self.bucket_controls(bucket);
            'retry_bucket: loop {
                let first = first_control.load(Ordering::Acquire);
                if matching_bytes(first, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(first, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * Self::SLOTS + offset].load(Ordering::Relaxed) == encoded {
                        return false;
                    }
                }
                if let Some(offset) = first_offset(first, 0) {
                    let shift = offset * u8::BITS as usize;
                    if first_control
                        .compare_exchange(
                            first,
                            first | (u64::from(WRITING) << shift),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        continue 'retry_bucket;
                    }
                    self.keys[bucket * Self::SLOTS + offset].store(encoded, Ordering::Relaxed);
                    let byte_mask = u64::from(u8::MAX) << shift;
                    first_control.store(
                        (first & !byte_mask) | (u64::from(tag) << shift),
                        Ordering::Release,
                    );
                    return true;
                }

                let tail = tail_control.load(Ordering::Acquire);
                if matching_tail::<2>(u64::from(tail), WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_tail::<2>(u64::from(tail), tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * Self::SLOTS + SLOTS_PER_CONTROL + offset]
                        .load(Ordering::Relaxed)
                        == encoded
                    {
                        return false;
                    }
                }
                let Some(offset) = first_tail_offset::<2>(u64::from(tail), 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                let writing = tail | ((u16::from(WRITING)) << shift);
                if tail_control
                    .compare_exchange(tail, writing, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    continue 'retry_bucket;
                }
                self.keys[bucket * Self::SLOTS + SLOTS_PER_CONTROL + offset]
                    .store(encoded, Ordering::Relaxed);
                let byte_mask = u16::from(u8::MAX) << shift;
                tail_control.store(
                    (tail & !byte_mask) | (u16::from(tag) << shift),
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
        for bucket in bucket_choices::<4>(hash, self.buckets) {
            let (first_control, tail_control) = self.bucket_controls(bucket);
            let first = first_control.load(Ordering::Acquire);
            let mut candidates = matching_bytes(first, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * Self::SLOTS + offset].load(Ordering::Relaxed) == encoded {
                    return true;
                }
            }
            if first_offset(first, 0).is_some() {
                return false;
            }

            let tail = u64::from(tail_control.load(Ordering::Acquire));
            let mut candidates = matching_tail::<2>(tail, tag);
            while let Some(offset) = take_offset(&mut candidates) {
                if self.keys[bucket * Self::SLOTS + SLOTS_PER_CONTROL + offset]
                    .load(Ordering::Relaxed)
                    == encoded
                {
                    return true;
                }
            }
            if first_tail_offset::<2>(tail, 0).is_some() {
                return false;
            }
        }
        false
    }

    fn placed_keys(&self) -> Box<[u64]> {
        let mut keys = Vec::new();
        for bucket in 0..self.buckets {
            let (first_control, tail_control) = self.bucket_controls(bucket);
            let first = first_control.load(Ordering::Acquire);
            for offset in 0..SLOTS_PER_CONTROL {
                if control_byte(first, offset) >= 2 {
                    let encoded = self.keys[bucket * Self::SLOTS + offset].load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
            let tail = u64::from(tail_control.load(Ordering::Acquire));
            for offset in 0..2 {
                if control_byte(tail, offset) >= 2 {
                    let encoded = self.keys[bucket * Self::SLOTS + SLOTS_PER_CONTROL + offset]
                        .load(Ordering::Relaxed);
                    keys.push(encoded.wrapping_sub(1));
                }
            }
        }
        keys.into_boxed_slice()
    }

    fn requested_bytes(&self) -> usize {
        self.controls.len() * size_of::<Control10Group>() + self.keys.len() * size_of::<AtomicU64>()
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

fn measure<const SLOTS: usize, const CHOICES: usize>(
    entries: usize,
    operations: usize,
    threads: usize,
) -> Measurement {
    let table = Arc::new(Table::<SLOTS, CHOICES>::with_capacity(entries));
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

fn measure_wide(entries: usize, operations: usize, threads: usize) -> Measurement {
    let table = Arc::new(Table::<16, 3>::with_capacity(entries));
    let started = Instant::now();
    run_workers(entries, threads, {
        let table = Arc::clone(&table);
        move |index| u64::from(table.insert_unrolled(index as u64))
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
            u64::from(table.contains_unrolled(placed[index]))
        }
    });
    let hit = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, move |operation| {
        u64::from(table.contains_unrolled(entries.wrapping_add(operation) as u64))
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

fn measure_split<T: TailControl, const TAIL: usize, const SLOTS: usize, const CHOICES: usize>(
    entries: usize,
    operations: usize,
    threads: usize,
) -> Measurement {
    let table = Arc::new(SplitTable::<T, TAIL, SLOTS, CHOICES>::with_capacity(
        entries,
    ));
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

fn measure_grouped10(entries: usize, operations: usize, threads: usize) -> Measurement {
    let table = Arc::new(GroupedTable10::with_capacity(entries));
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

fn matching_bytes(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * BYTE_ONES);
    !((different & BYTE_LOW_BITS).wrapping_add(BYTE_LOW_BITS) | different | BYTE_LOW_BITS)
        & BYTE_HIGH_BITS
}

fn matching_tail<const TAIL: usize>(control: u64, state: u8) -> u64 {
    matching_bytes(control, state) & (BYTE_HIGH_BITS >> ((8 - TAIL) * u8::BITS as usize))
}

fn first_offset(control: u64, state: u8) -> Option<usize> {
    let matches = matching_bytes(control, state);
    (matches != 0).then(|| matches.trailing_zeros() as usize / u8::BITS as usize)
}

fn first_tail_offset<const TAIL: usize>(control: u64, state: u8) -> Option<usize> {
    let matches = matching_tail::<TAIL>(control, state);
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
    2 + (hash as u8 % 254)
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn reduce(value: u64, buckets: usize) -> usize {
    ((u128::from(value) * buckets as u128) >> u64::BITS) as usize
}

fn next_index(index: usize, length: usize) -> usize {
    if index + 1 == length { 0 } else { index + 1 }
}

fn metric(measured: &[Measurement], select: impl Fn(&Measurement) -> f64) -> (f64, f64) {
    let mut values = measured.iter().map(select).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let median = values[values.len() / 2];
    let p05_index = (values.len() - 1) / 20;
    (median, values[p05_index])
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("argument is a usize"))
}
