//! Structural comparison of serialized and tag-reserved bucket publication.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::{black_box, spin_loop};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const BUCKET_SLOTS: usize = 8;
// Keep this primary-table protocol probe below the point where a production
// table would spill into its separate overflow path. Dense behavior belongs in
// a follow-up that models that path rather than silently dropping entries.
const TARGET_NUMERATOR: usize = 4;
const TARGET_DENOMINATOR: usize = 1;
const BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const SERIAL_WRITING: u8 = 1;
const RESERVED_WRITING_BIT: u8 = 128;
const ORDERED_CLAIMED: u8 = 1;
const ORDERED_READY: u8 = 2;
const ORDERED_TOMBSTONE: u8 = 3;

#[derive(Clone, Copy)]
enum Protocol {
    Serialized,
    TagReserved,
    OrderedReady,
}

impl Protocol {
    const ALL: [Self; 3] = [Self::Serialized, Self::TagReserved, Self::OrderedReady];

    const fn name(self) -> &'static str {
        match self {
            Self::Serialized => "serialized-writing-byte",
            Self::TagReserved => "tag-reserved-writing-byte",
            Self::OrderedReady => "ordered-ready-writing-byte",
        }
    }

    fn published_tag(self, hash: u64) -> u8 {
        match self {
            Self::Serialized => u8::try_from(hash % 254).expect("tag fits u8") + 2,
            Self::TagReserved => u8::try_from(hash % 126).expect("tag fits u8") + 2,
            Self::OrderedReady => u8::try_from(hash % 252).expect("tag fits u8") + 4,
        }
    }

    fn writing_tag(self, published: u8) -> u8 {
        match self {
            Self::Serialized => SERIAL_WRITING,
            Self::TagReserved => published | RESERVED_WRITING_BIT,
            Self::OrderedReady => ORDERED_CLAIMED,
        }
    }
}

struct Table {
    protocol: Protocol,
    controls: Box<[AtomicU64]>,
    keys: Box<[AtomicU64]>,
    buckets: usize,
}

impl Table {
    fn with_capacity(capacity: usize, protocol: Protocol) -> Self {
        let slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = slots.div_ceil(BUCKET_SLOTS).max(1);
        Self {
            protocol,
            controls: (0..buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            keys: (0..buckets * BUCKET_SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
        }
    }

    fn insert(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let published = self.protocol.published_tag(hash);
        if matches!(self.protocol, Protocol::OrderedReady) {
            return self.insert_ordered_ready(encoded, hash, published);
        }
        let writing = self.protocol.writing_tag(published);
        for bucket in bucket_choices(hash, self.buckets) {
            loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                if matches!(self.protocol, Protocol::Serialized)
                    && matching_bytes(control, SERIAL_WRITING) != 0
                {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(control, published);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * BUCKET_SLOTS + offset].load(Ordering::Relaxed) == encoded
                    {
                        return false;
                    }
                }
                if matches!(self.protocol, Protocol::TagReserved)
                    && matching_bytes(control, writing) != 0
                {
                    spin_loop();
                    continue;
                }
                let Some(offset) = first_offset(control, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                let claimed = control | (u64::from(writing) << shift);
                if self.controls[bucket]
                    .compare_exchange(control, claimed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                }
                self.keys[bucket * BUCKET_SLOTS + offset].store(encoded, Ordering::Relaxed);
                match self.protocol {
                    Protocol::Serialized => {
                        let byte_mask = u64::from(u8::MAX) << shift;
                        let published_control =
                            (control & !byte_mask) | (u64::from(published) << shift);
                        self.controls[bucket].store(published_control, Ordering::Release);
                    }
                    Protocol::TagReserved => {
                        self.controls[bucket]
                            .fetch_xor(u64::from(RESERVED_WRITING_BIT) << shift, Ordering::Release);
                    }
                    Protocol::OrderedReady => unreachable!("handled before the serialized path"),
                }
                return true;
            }
        }
        panic!("four bucket choices exhausted at the configured headroom");
    }

    fn insert_ordered_ready(&self, encoded: u64, hash: u64, published: u8) -> bool {
        for bucket in bucket_choices(hash, self.buckets) {
            loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                let mut candidates = matching_bytes(control, published);
                while let Some(offset) = take_offset(&mut candidates) {
                    if self.keys[bucket * BUCKET_SLOTS + offset].load(Ordering::Relaxed) == encoded
                    {
                        return false;
                    }
                }
                let Some(offset) = first_offset(control, 0) else {
                    if ordered_in_progress(control) != 0 {
                        spin_loop();
                        continue;
                    }
                    break;
                };
                let shift = offset * u8::BITS as usize;
                let claimed = control | (u64::from(ORDERED_CLAIMED) << shift);
                if self.controls[bucket]
                    .compare_exchange(control, claimed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                }
                self.keys[bucket * BUCKET_SLOTS + offset].store(encoded, Ordering::Relaxed);
                self.controls[bucket].fetch_xor(
                    u64::from(ORDERED_CLAIMED ^ ORDERED_READY) << shift,
                    Ordering::Release,
                );

                let earlier = (1_u64 << shift).wrapping_sub(1);
                loop {
                    let ready_control = self.controls[bucket].load(Ordering::Acquire);
                    if ordered_in_progress(ready_control) & earlier != 0 {
                        spin_loop();
                        continue;
                    }
                    let mut duplicates = matching_bytes(ready_control, published);
                    let mut duplicate = false;
                    while let Some(candidate_offset) = take_offset(&mut duplicates) {
                        if self.keys[bucket * BUCKET_SLOTS + candidate_offset]
                            .load(Ordering::Relaxed)
                            == encoded
                        {
                            duplicate = true;
                            break;
                        }
                    }
                    let state = if duplicate {
                        ORDERED_TOMBSTONE
                    } else {
                        published
                    };
                    self.controls[bucket]
                        .fetch_xor(u64::from(ORDERED_READY ^ state) << shift, Ordering::Release);
                    return !duplicate;
                }
            }
        }
        panic!("four bucket choices exhausted at the configured headroom");
    }

    fn contains(&self, key: u64) -> bool {
        let encoded = key.wrapping_add(1);
        let hash = mix(encoded);
        let tag = self.protocol.published_tag(hash);
        for bucket in bucket_choices(hash, self.buckets) {
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
}

#[derive(Clone, Copy)]
struct Measurement {
    insert: f64,
    contended_insert: f64,
    hit: f64,
    miss: f64,
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let operations = argument(&mut arguments, 2_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    prove_same_key();

    println!(
        "protocol,entries,operations,threads,samples,insert_median_mops,insert_p05_mops,contended_insert_median_mops,contended_insert_p05_mops,hit_median_mops,hit_p05_mops,miss_median_mops,miss_p05_mops"
    );
    let mut measurements: [Vec<Measurement>; Protocol::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Protocol::ALL.len() {
            let protocol_index = (sample + offset) % Protocol::ALL.len();
            measurements[protocol_index].push(measure(
                Protocol::ALL[protocol_index],
                entries,
                operations,
                threads,
            ));
        }
    }
    for (protocol, measured) in Protocol::ALL.into_iter().zip(measurements) {
        let (insert, insert_p05) = median_and_p05(
            measured
                .iter()
                .map(|measurement| measurement.insert)
                .collect(),
        );
        let (contended_insert, contended_insert_p05) = median_and_p05(
            measured
                .iter()
                .map(|measurement| measurement.contended_insert)
                .collect(),
        );
        let (hit, hit_p05) =
            median_and_p05(measured.iter().map(|measurement| measurement.hit).collect());
        let (miss, miss_p05) = median_and_p05(
            measured
                .iter()
                .map(|measurement| measurement.miss)
                .collect(),
        );
        println!(
            "{},{entries},{operations},{threads},{samples},{insert:.3},{insert_p05:.3},{contended_insert:.3},{contended_insert_p05:.3},{hit:.3},{hit_p05:.3},{miss:.3},{miss_p05:.3}",
            protocol.name()
        );
    }
}

fn measure(protocol: Protocol, entries: usize, operations: usize, threads: usize) -> Measurement {
    let table = Arc::new(Table::with_capacity(entries, protocol));
    let started = Instant::now();
    run_workers(entries, threads, {
        let table = Arc::clone(&table);
        move |index| {
            assert!(table.insert(index as u64));
            index as u64
        }
    });
    let insert_mops = entries as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;
    for key in [0, entries / 3, entries.saturating_sub(1)] {
        assert!(table.contains(key as u64));
    }

    let contended_insert_mops = if (4..=BUCKET_SLOTS).contains(&threads) {
        let contended = Arc::new(Table::with_capacity(entries, protocol));
        let contended_keys = hot_primary_keys(entries, threads, contended.buckets);
        let started = Instant::now();
        run_workers(entries, threads, {
            let table = Arc::clone(&contended);
            move |index| {
                assert!(table.insert(contended_keys[index]));
                contended_keys[index]
            }
        });
        entries as f64 / started.elapsed().as_secs_f64() / 1_000_000.0
    } else {
        insert_mops
    };

    let started = Instant::now();
    run_workers(operations, threads, {
        let table = Arc::clone(&table);
        move |operation| {
            let key = mix(operation as u64) as usize % entries;
            u64::from(table.contains(key as u64))
        }
    });
    let hit_mops = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, move |operation| {
        let key = entries.wrapping_add(operation);
        u64::from(table.contains(key as u64))
    });
    let miss_mops = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    Measurement {
        insert: insert_mops,
        contended_insert: contended_insert_mops,
        hit: hit_mops,
        miss: miss_mops,
    }
}

fn hot_primary_keys(entries: usize, contenders: usize, buckets: usize) -> Box<[u64]> {
    let groups = entries.div_ceil(contenders);
    assert!(groups <= buckets);
    let mut keys = vec![0_u64; entries];
    let mut counts = vec![0_usize; groups];
    let mut filled = 0;
    let mut candidate = entries as u64;
    while filled < entries {
        let bucket = reduce(mix(candidate.wrapping_add(1)), buckets);
        if bucket < groups && counts[bucket] < contenders {
            let index = counts[bucket] * groups + bucket;
            if index < entries {
                keys[index] = candidate;
                counts[bucket] += 1;
                filled += 1;
            }
        }
        candidate = candidate.wrapping_add(1);
    }
    keys.into_boxed_slice()
}

fn prove_same_key() {
    for protocol in Protocol::ALL {
        let table = Arc::new(Table::with_capacity(64, protocol));
        let winners = Arc::new(AtomicUsize::new(0));
        thread::scope(|scope| {
            for _ in 0..16 {
                let table = Arc::clone(&table);
                let winners = Arc::clone(&winners);
                scope.spawn(move || {
                    if table.insert(42) {
                        winners.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(winners.load(Ordering::Relaxed), 1);
        assert!(table.contains(42));
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

fn bucket_choices(hash: u64, buckets: usize) -> [usize; 4] {
    let mut choices = [0; 4];
    let variants = [
        hash,
        hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15,
        hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93,
        hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f,
    ];
    for index in 0..choices.len() {
        let mut candidate = reduce(variants[index], buckets);
        while choices[..index].contains(&candidate) {
            candidate = if candidate + 1 == buckets {
                0
            } else {
                candidate + 1
            };
        }
        choices[index] = candidate;
    }
    choices
}

fn matching_bytes(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * BYTE_ONES);
    !((different & BYTE_LOW_BITS).wrapping_add(BYTE_LOW_BITS) | different | BYTE_LOW_BITS)
        & BYTE_HIGH_BITS
}

fn ordered_in_progress(control: u64) -> u64 {
    matching_bytes(control, ORDERED_CLAIMED) | matching_bytes(control, ORDERED_READY)
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

fn median_and_p05(mut values: Vec<f64>) -> (f64, f64) {
    values.sort_by(f64::total_cmp);
    (values[values.len() / 2], values[values.len() / 20])
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}
