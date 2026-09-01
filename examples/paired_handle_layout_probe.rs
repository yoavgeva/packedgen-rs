//! Locality screen for colocated 64-bit pointers versus paired 32-bit handles.

#![allow(
    unsafe_code,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::mem::{size_of, size_of_val};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const KEY_WORDS: usize = 4;
const KEY_DWORDS: usize = KEY_WORDS * 2;
const BLOCK_BITS: usize = 8;
const BLOCK_SIZE: usize = 1 << BLOCK_BITS;
const SLOT_MASK: u32 = (BLOCK_SIZE as u32) - 1;

#[repr(C)]
struct Entry {
    metadata: AtomicU64,
    value: [u64; 7],
}

#[repr(C)]
struct PointerSlot {
    key: [u64; KEY_WORDS],
    handle: AtomicU64,
}

#[repr(C)]
struct CompactPair {
    keys: [[u64; KEY_WORDS]; 2],
    handles: [AtomicU32; 2],
}

#[repr(C)]
struct AdjacentCompactPair {
    first_key: [u64; KEY_WORDS],
    handles: [AtomicU32; 2],
    second_key: [u64; KEY_WORDS],
}

#[repr(C)]
struct CompactSlot32 {
    key: [AtomicU32; KEY_DWORDS],
    handle: AtomicU32,
}

struct Fixture {
    _blocks: Vec<Box<[Entry]>>,
    directory: Box<[usize]>,
    pointer_slots: Box<[PointerSlot]>,
    compact_pairs: Box<[CompactPair]>,
    adjacent_pairs: Box<[AdjacentCompactPair]>,
    compact_slots: Box<[CompactSlot32]>,
    entries: usize,
}

impl Fixture {
    fn new(entries: usize) -> Self {
        let mut blocks = Vec::with_capacity(entries.div_ceil(BLOCK_SIZE));
        let mut directory = Vec::with_capacity(entries.div_ceil(BLOCK_SIZE));
        let mut raw_handles = Vec::with_capacity(entries);
        let mut compact_handles = Vec::with_capacity(entries);
        for block_id in 0..entries.div_ceil(BLOCK_SIZE) {
            let begin = block_id * BLOCK_SIZE;
            let len = (entries - begin).min(BLOCK_SIZE);
            let block = (0..len)
                .map(|offset| Entry {
                    metadata: AtomicU64::new((begin + offset) as u64),
                    value: std::array::from_fn(|word| mix((begin + offset) as u64 ^ word as u64)),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let start = block.as_ptr().expose_provenance();
            directory.push(start);
            for offset in 0..len {
                raw_handles.push(start + offset * size_of::<Entry>());
                let block_id = u32::try_from(block_id).expect("probe block id fits u32");
                let offset = u32::try_from(offset).expect("block offset fits u32");
                compact_handles.push((block_id << BLOCK_BITS) | offset);
            }
            blocks.push(block);
        }

        let pointer_slots = (0..entries)
            .map(|index| PointerSlot {
                key: key(index),
                handle: AtomicU64::new(raw_handles[index] as u64),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let compact_pairs = (0..entries.div_ceil(2))
            .map(|pair| {
                let first = pair * 2;
                let second = (first + 1).min(entries - 1);
                CompactPair {
                    keys: [key(first), key(second)],
                    handles: [
                        AtomicU32::new(compact_handles[first]),
                        AtomicU32::new(compact_handles[second]),
                    ],
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let compact_slots = (0..entries)
            .map(|index| {
                let key = key(index);
                CompactSlot32 {
                    key: std::array::from_fn(|dword| {
                        let word = key[dword / 2];
                        let value = if dword & 1 == 0 {
                            word as u32
                        } else {
                            (word >> u32::BITS) as u32
                        };
                        AtomicU32::new(value)
                    }),
                    handle: AtomicU32::new(compact_handles[index]),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let adjacent_pairs = (0..entries.div_ceil(2))
            .map(|pair| {
                let first = pair * 2;
                let second = (first + 1).min(entries - 1);
                AdjacentCompactPair {
                    first_key: key(first),
                    handles: [
                        AtomicU32::new(compact_handles[first]),
                        AtomicU32::new(compact_handles[second]),
                    ],
                    second_key: key(second),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            _blocks: blocks,
            directory: directory.into_boxed_slice(),
            pointer_slots,
            compact_pairs,
            adjacent_pairs,
            compact_slots,
            entries,
        }
    }

    fn pointer_entry(&self, index: usize, expected: &[u64; KEY_WORDS]) -> Option<&Entry> {
        let slot = &self.pointer_slots[index];
        if slot.key != *expected {
            return None;
        }
        let address = slot.handle.load(Ordering::Acquire) as usize & !3;
        // SAFETY: every handle points into a block owned for the fixture's
        // complete lifetime.
        Some(unsafe { &*std::ptr::with_exposed_provenance::<Entry>(address) })
    }

    fn compact_entry(&self, index: usize, expected: &[u64; KEY_WORDS]) -> Option<&Entry> {
        let pair = &self.compact_pairs[index / 2];
        let lane = index & 1;
        if pair.keys[lane] != *expected {
            return None;
        }
        let handle = pair.handles[lane].load(Ordering::Acquire);
        let block_id = (handle >> BLOCK_BITS) as usize;
        let offset = (handle & SLOT_MASK) as usize;
        let start = self.directory[block_id];
        // SAFETY: compact handles are constructed from in-range block slots
        // and the directory's blocks stay live for the fixture's lifetime.
        Some(unsafe { &*std::ptr::with_exposed_provenance::<Entry>(start).add(offset) })
    }

    fn compact_slot_entry(&self, index: usize, expected: &[u64; KEY_WORDS]) -> Option<&Entry> {
        let slot = &self.compact_slots[index];
        for (word, expected) in expected.iter().copied().enumerate() {
            let lower = u64::from(slot.key[word * 2].load(Ordering::Relaxed));
            let upper = u64::from(slot.key[word * 2 + 1].load(Ordering::Relaxed));
            if lower | (upper << u32::BITS) != expected {
                return None;
            }
        }
        let handle = slot.handle.load(Ordering::Acquire);
        let block_id = (handle >> BLOCK_BITS) as usize;
        let offset = (handle & SLOT_MASK) as usize;
        let start = self.directory[block_id];
        // SAFETY: compact handles are constructed from live in-range entries.
        Some(unsafe { &*std::ptr::with_exposed_provenance::<Entry>(start).add(offset) })
    }

    fn adjacent_pair_entry(&self, index: usize, expected: &[u64; KEY_WORDS]) -> Option<&Entry> {
        let pair = &self.adjacent_pairs[index / 2];
        let lane = index & 1;
        let stored = if lane == 0 {
            &pair.first_key
        } else {
            &pair.second_key
        };
        if stored != expected {
            return None;
        }
        let handle = pair.handles[lane].load(Ordering::Acquire);
        let block_id = (handle >> BLOCK_BITS) as usize;
        let offset = (handle & SLOT_MASK) as usize;
        let start = self.directory[block_id];
        // SAFETY: compact handles are constructed from live in-range entries.
        Some(unsafe { &*std::ptr::with_exposed_provenance::<Entry>(start).add(offset) })
    }
}

#[derive(Clone, Copy)]
enum Design {
    Pointer64,
    Paired32,
    AdjacentPaired32,
    Colocated32,
}

impl Design {
    const ALL: [Self; 4] = [
        Self::Pointer64,
        Self::Paired32,
        Self::AdjacentPaired32,
        Self::Colocated32,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Pointer64 => "colocated-pointer-64",
            Self::Paired32 => "paired-block-slot-32",
            Self::AdjacentPaired32 => "adjacent-paired-block-slot-32",
            Self::Colocated32 => "colocated-dword-block-slot-32",
        }
    }

    fn entry<'a>(
        self,
        fixture: &'a Fixture,
        index: usize,
        expected: &[u64; KEY_WORDS],
    ) -> Option<&'a Entry> {
        match self {
            Self::Pointer64 => fixture.pointer_entry(index, expected),
            Self::Paired32 => fixture.compact_entry(index, expected),
            Self::AdjacentPaired32 => fixture.adjacent_pair_entry(index, expected),
            Self::Colocated32 => fixture.compact_slot_entry(index, expected),
        }
    }

    fn resident_index_bytes(self, fixture: &Fixture) -> usize {
        match self {
            Self::Pointer64 => size_of_val(&*fixture.pointer_slots),
            Self::Paired32 => {
                size_of_val(&*fixture.compact_pairs) + size_of_val(&*fixture.directory)
            }
            Self::AdjacentPaired32 => {
                size_of_val(&*fixture.adjacent_pairs) + size_of_val(&*fixture.directory)
            }
            Self::Colocated32 => {
                size_of_val(&*fixture.compact_slots) + size_of_val(&*fixture.directory)
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    hit: f64,
    miss: f64,
    update: f64,
    hot_hit: f64,
    hot_update: f64,
}

fn main() {
    assert_eq!(size_of::<PointerSlot>(), 40);
    assert_eq!(size_of::<CompactPair>(), 72);
    assert_eq!(size_of::<AdjacentCompactPair>(), 72);
    assert_eq!(size_of::<CompactSlot32>(), 36);
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(2);
    let operations = argument(&mut arguments, 10_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 21).max(3);
    let fixture = Arc::new(Fixture::new(entries));
    println!(
        "design,entries,operations,threads,samples,hit_median_mops,hit_p05_mops,miss_median_mops,miss_p05_mops,update_median_mops,update_p05_mops,hot_hit_median_mops,hot_hit_p05_mops,hot_update_median_mops,hot_update_p05_mops,index_and_directory_bytes_per_entry"
    );
    let mut measurements: [Vec<Measurement>; Design::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Design::ALL.len() {
            let index = (sample + offset) % Design::ALL.len();
            measurements[index].push(measure(
                Arc::clone(&fixture),
                operations,
                threads,
                Design::ALL[index],
            ));
        }
    }
    for (design, measured) in Design::ALL.into_iter().zip(measurements) {
        let (hit, hit_p05) = metric(&measured, |measurement| measurement.hit);
        let (miss, miss_p05) = metric(&measured, |measurement| measurement.miss);
        let (update, update_p05) = metric(&measured, |measurement| measurement.update);
        let (hot_hit, hot_hit_p05) = metric(&measured, |measurement| measurement.hot_hit);
        let (hot_update, hot_update_p05) = metric(&measured, |measurement| measurement.hot_update);
        let bytes = design.resident_index_bytes(&fixture) as f64 / entries as f64;
        println!(
            "{},{entries},{operations},{threads},{samples},{hit:.3},{hit_p05:.3},{miss:.3},{miss_p05:.3},{update:.3},{update_p05:.3},{hot_hit:.3},{hot_hit_p05:.3},{hot_update:.3},{hot_update_p05:.3},{bytes:.3}",
            design.name(),
        );
    }
}

fn measure(
    fixture: Arc<Fixture>,
    operations: usize,
    threads: usize,
    design: Design,
) -> Measurement {
    let hit = run_timed(operations, threads, {
        let fixture = Arc::clone(&fixture);
        move |operation| {
            let index = reduce(mix(operation as u64), fixture.entries);
            let expected = key(index);
            let entry = design
                .entry(&fixture, index, &expected)
                .expect("hit key matches");
            entry.metadata.load(Ordering::Relaxed) ^ entry.value[operation % entry.value.len()]
        }
    });
    let miss = run_timed(operations, threads, {
        let fixture = Arc::clone(&fixture);
        move |operation| {
            let index = reduce(mix(operation as u64), fixture.entries);
            let expected = key(index + fixture.entries);
            u64::from(design.entry(&fixture, index, &expected).is_some())
        }
    });
    let update = run_timed(operations, threads, {
        let fixture = Arc::clone(&fixture);
        move |operation| {
            let index = reduce(mix(operation as u64), fixture.entries);
            let expected = key(index);
            let entry = design
                .entry(&fixture, index, &expected)
                .expect("update key matches");
            entry
                .metadata
                .fetch_xor(1_u64 << (operation & 31), Ordering::Relaxed)
        }
    });
    let hot_hit = run_timed(operations, threads, {
        let fixture = Arc::clone(&fixture);
        move |operation| {
            let index = hot_index(operation, fixture.entries);
            let expected = key(index);
            let entry = design
                .entry(&fixture, index, &expected)
                .expect("hot hit key matches");
            entry.metadata.load(Ordering::Relaxed) ^ entry.value[operation % entry.value.len()]
        }
    });
    let hot_update = run_timed(operations, threads, move |operation| {
        let index = hot_index(operation, fixture.entries);
        let expected = key(index);
        let entry = design
            .entry(&fixture, index, &expected)
            .expect("hot update key matches");
        entry
            .metadata
            .fetch_xor(1_u64 << (operation & 31), Ordering::Relaxed)
    });
    Measurement {
        hit,
        miss,
        update,
        hot_hit,
        hot_update,
    }
}

fn run_timed(work: usize, threads: usize, operation: impl Fn(usize) -> u64 + Sync) -> f64 {
    let barrier = Barrier::new(threads + 1);
    let checksum = AtomicU64::new(0);
    let started = Instant::now();
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
    work as f64 / started.elapsed().as_secs_f64() / 1_000_000.0
}

fn key(index: usize) -> [u64; KEY_WORDS] {
    std::array::from_fn(|word| mix(index as u64 ^ (word as u64).wrapping_mul(0x9e37_79b9)))
}

fn hot_index(operation: usize, entries: usize) -> usize {
    let hot = entries.div_ceil(100).clamp(1, entries);
    if mix(operation as u64 ^ 0x1ae9_523f_184c_7b21).is_multiple_of(10) {
        reduce(mix(operation as u64 ^ 0x6d5a_56da), entries)
    } else {
        reduce(mix(operation as u64), hot)
    }
}

fn reduce(hash: u64, upper: usize) -> usize {
    usize::try_from((u128::from(hash) * upper as u128) >> 64)
        .expect("reduced index is below upper bound")
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
