//! Cost floor for raw-pointer versus 32-bit block/slot arena handles.

#![allow(
    unsafe_code,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const BLOCK_BITS: usize = 8;
const BLOCK_SIZE: usize = 1 << BLOCK_BITS;
const SLOT_MASK: u32 = (BLOCK_SIZE as u32) - 1;

#[repr(C, align(8))]
struct Entry {
    metadata: AtomicU64,
    value: [u64; 7],
}

impl Entry {
    fn new(index: usize) -> Self {
        Self {
            metadata: AtomicU64::new(index as u64),
            value: std::array::from_fn(|word| mix(index as u64 ^ word as u64)),
        }
    }
}

struct Arena {
    _blocks: Vec<Box<[Entry]>>,
    directory: Box<[usize]>,
    raw_handles: Box<[usize]>,
    compact_handles: Box<[u32]>,
}

impl Arena {
    fn new(entries: usize) -> Self {
        let mut blocks = Vec::with_capacity(entries.div_ceil(BLOCK_SIZE));
        let mut directory = Vec::with_capacity(entries.div_ceil(BLOCK_SIZE));
        let mut raw_handles = Vec::with_capacity(entries);
        let mut compact_handles = Vec::with_capacity(entries);
        for block_id in 0..entries.div_ceil(BLOCK_SIZE) {
            let begin = block_id * BLOCK_SIZE;
            let len = (entries - begin).min(BLOCK_SIZE);
            let block = (0..len)
                .map(|offset| Entry::new(begin + offset))
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
        Self {
            _blocks: blocks,
            directory: directory.into_boxed_slice(),
            raw_handles: raw_handles.into_boxed_slice(),
            compact_handles: compact_handles.into_boxed_slice(),
        }
    }

    fn raw_entry(&self, index: usize) -> &Entry {
        let address = self.raw_handles[index] & !3;
        // SAFETY: handles were derived from live entries in `_blocks`, whose
        // boxes stay owned by this arena for the entire measurement.
        unsafe { &*std::ptr::with_exposed_provenance::<Entry>(address) }
    }

    fn compact_entry(&self, index: usize) -> &Entry {
        let handle = self.compact_handles[index];
        let block_id = (handle >> BLOCK_BITS) as usize;
        let offset = (handle & SLOT_MASK) as usize;
        let start = self.directory[block_id];
        // SAFETY: each directory address owns `BLOCK_SIZE` entries except the
        // final block, and every published compact handle was constructed
        // from an in-range entry in that block.
        unsafe { &*std::ptr::with_exposed_provenance::<Entry>(start).add(offset) }
    }
}

#[derive(Clone, Copy)]
enum Design {
    RawPointer,
    Compact32,
}

impl Design {
    const ALL: [Self; 2] = [Self::RawPointer, Self::Compact32];

    const fn name(self) -> &'static str {
        match self {
            Self::RawPointer => "raw-pointer-64",
            Self::Compact32 => "block-slot-32",
        }
    }

    fn entry(self, arena: &Arena, index: usize) -> &Entry {
        match self {
            Self::RawPointer => arena.raw_entry(index),
            Self::Compact32 => arena.compact_entry(index),
        }
    }

    const fn resident_handle_bytes(self, entries: usize, blocks: usize) -> usize {
        match self {
            Self::RawPointer => entries * size_of::<usize>(),
            Self::Compact32 => entries * size_of::<u32>() + blocks * size_of::<usize>(),
        }
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    read: f64,
    update: f64,
    hot_read: f64,
    hot_update: f64,
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 20_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 21).max(3);
    let arena = Arc::new(Arena::new(entries));
    println!(
        "design,entries,operations,threads,samples,read_median_mops,read_p05_mops,update_median_mops,update_p05_mops,hot_read_median_mops,hot_read_p05_mops,hot_update_median_mops,hot_update_p05_mops,handle_and_directory_bytes_per_entry"
    );
    let mut measurements: [Vec<Measurement>; Design::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Design::ALL.len() {
            let index = (sample + offset) % Design::ALL.len();
            measurements[index].push(measure(
                Arc::clone(&arena),
                operations,
                threads,
                Design::ALL[index],
            ));
        }
    }
    let blocks = entries.div_ceil(BLOCK_SIZE);
    for (design, measured) in Design::ALL.into_iter().zip(measurements) {
        let (read, read_p05) = metric(&measured, |measurement| measurement.read);
        let (update, update_p05) = metric(&measured, |measurement| measurement.update);
        let (hot_read, hot_read_p05) = metric(&measured, |measurement| measurement.hot_read);
        let (hot_update, hot_update_p05) = metric(&measured, |measurement| measurement.hot_update);
        let bytes = design.resident_handle_bytes(entries, blocks) as f64 / entries as f64;
        println!(
            "{},{entries},{operations},{threads},{samples},{read:.3},{read_p05:.3},{update:.3},{update_p05:.3},{hot_read:.3},{hot_read_p05:.3},{hot_update:.3},{hot_update_p05:.3},{bytes:.3}",
            design.name(),
        );
    }
}

fn measure(arena: Arc<Arena>, operations: usize, threads: usize, design: Design) -> Measurement {
    let started = Instant::now();
    run_workers(operations, threads, {
        let arena = Arc::clone(&arena);
        move |operation| {
            let index = reduce(mix(operation as u64), arena.raw_handles.len());
            let entry = design.entry(&arena, index);
            entry.metadata.load(Ordering::Relaxed) ^ entry.value[operation % entry.value.len()]
        }
    });
    let read = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, {
        let arena = Arc::clone(&arena);
        move |operation| {
            let index = reduce(mix(operation as u64), arena.raw_handles.len());
            let entry = design.entry(&arena, index);
            entry
                .metadata
                .fetch_xor(1_u64 << (operation & 31), Ordering::Relaxed)
        }
    });
    let update = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, {
        let arena = Arc::clone(&arena);
        move |operation| {
            let index = hot_index(operation, arena.raw_handles.len());
            let entry = design.entry(&arena, index);
            entry.metadata.load(Ordering::Relaxed) ^ entry.value[operation % entry.value.len()]
        }
    });
    let hot_read = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    let started = Instant::now();
    run_workers(operations, threads, move |operation| {
        let index = hot_index(operation, arena.raw_handles.len());
        let entry = design.entry(&arena, index);
        entry
            .metadata
            .fetch_xor(1_u64 << (operation & 31), Ordering::Relaxed)
    });
    let hot_update = operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0;

    Measurement {
        read,
        update,
        hot_read,
        hot_update,
    }
}

fn hot_index(operation: usize, entries: usize) -> usize {
    let hot = entries.div_ceil(100).clamp(1, entries);
    if mix(operation as u64 ^ 0x1ae9_523f_184c_7b21).is_multiple_of(10) {
        reduce(mix(operation as u64 ^ 0x6d5a_56da), entries)
    } else {
        reduce(mix(operation as u64), hot)
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
