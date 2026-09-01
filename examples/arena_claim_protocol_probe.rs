//! Cost floor for mutex-owned and lifetime-protected atomic arena claims.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use parking_lot::Mutex;

const BLOCK_SLOTS: usize = 256;
const BITMAP_WORDS: usize = BLOCK_SLOTS / u64::BITS as usize;
const CLOSED_BIT: usize = 1_usize << (usize::BITS - 1);

struct MutexBlock {
    state: Mutex<PlainState>,
}

struct PlainState {
    occupied: [u64; BITMAP_WORDS],
    live: usize,
}

impl MutexBlock {
    fn new() -> Self {
        Self {
            state: Mutex::new(PlainState {
                occupied: [0; BITMAP_WORDS],
                live: 0,
            }),
        }
    }

    fn claim(&self) -> usize {
        let mut state = self.state.lock();
        for word_index in 0..state.occupied.len() {
            let vacant = !state.occupied[word_index];
            if vacant == 0 {
                continue;
            }
            let bit = vacant.trailing_zeros() as usize;
            state.occupied[word_index] |= 1_u64 << bit;
            state.live += 1;
            return word_index * u64::BITS as usize + bit;
        }
        panic!("preallocated mutex block exhausted");
    }
}

struct AtomicBlock {
    occupied: [AtomicU64; BITMAP_WORDS],
    live: AtomicUsize,
    fast_users: AtomicUsize,
}

struct PinnedAtomicBlock {
    occupied: [AtomicU64; BITMAP_WORDS],
    live: AtomicUsize,
}

struct PinnedClosableBlock {
    occupied: [AtomicU64; BITMAP_WORDS],
    state: AtomicUsize,
}

impl PinnedClosableBlock {
    fn new() -> Self {
        Self {
            occupied: std::array::from_fn(|_| AtomicU64::new(0)),
            state: AtomicUsize::new(0),
        }
    }

    fn claim(&self) -> usize {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            assert_eq!(state & CLOSED_BIT, 0, "preallocated block closed");
            state = match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => observed,
            };
        }
        for (word_index, word) in self.occupied.iter().enumerate() {
            let occupied = word.load(Ordering::Relaxed);
            let vacant = !occupied;
            if vacant == 0 {
                continue;
            }
            let bit = vacant.trailing_zeros() as usize;
            let previous = word.fetch_or(1_u64 << bit, Ordering::Relaxed);
            assert_eq!(previous & (1_u64 << bit), 0);
            return word_index * u64::BITS as usize + bit;
        }
        self.state.fetch_sub(1, Ordering::Release);
        panic!("preallocated closable block exhausted");
    }
}

impl PinnedAtomicBlock {
    fn new() -> Self {
        Self {
            occupied: std::array::from_fn(|_| AtomicU64::new(0)),
            live: AtomicUsize::new(0),
        }
    }

    fn claim(&self) -> usize {
        // A guard-owned strong block reference supplies lifetime protection,
        // leaving only the state that must synchronize with reclamation.
        self.live.fetch_add(1, Ordering::Relaxed);
        for (word_index, word) in self.occupied.iter().enumerate() {
            let occupied = word.load(Ordering::Relaxed);
            let vacant = !occupied;
            if vacant == 0 {
                continue;
            }
            let bit = vacant.trailing_zeros() as usize;
            let previous = word.fetch_or(1_u64 << bit, Ordering::Relaxed);
            assert_eq!(previous & (1_u64 << bit), 0);
            return word_index * u64::BITS as usize + bit;
        }
        self.live.fetch_sub(1, Ordering::Relaxed);
        panic!("preallocated pinned atomic block exhausted");
    }
}

impl AtomicBlock {
    fn new() -> Self {
        Self {
            occupied: std::array::from_fn(|_| AtomicU64::new(0)),
            live: AtomicUsize::new(0),
            fast_users: AtomicUsize::new(0),
        }
    }

    fn claim(&self) -> usize {
        // The lifetime count is the minimum safe protocol required before an
        // allocator dereferences a current block that reclamation may detach.
        self.fast_users.fetch_add(1, Ordering::Acquire);
        self.live.fetch_add(1, Ordering::Relaxed);
        for (word_index, word) in self.occupied.iter().enumerate() {
            let occupied = word.load(Ordering::Relaxed);
            let vacant = !occupied;
            if vacant == 0 {
                continue;
            }
            let bit = vacant.trailing_zeros() as usize;
            let previous = word.fetch_or(1_u64 << bit, Ordering::Relaxed);
            assert_eq!(previous & (1_u64 << bit), 0);
            self.fast_users.fetch_sub(1, Ordering::Release);
            return word_index * u64::BITS as usize + bit;
        }
        self.live.fetch_sub(1, Ordering::Relaxed);
        self.fast_users.fetch_sub(1, Ordering::Release);
        panic!("preallocated atomic block exhausted");
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Mutex,
    AtomicLifetime,
    PinnedAtomic,
    PinnedClosable,
}

impl Protocol {
    const ALL: [Self; 4] = [
        Self::Mutex,
        Self::AtomicLifetime,
        Self::PinnedAtomic,
        Self::PinnedClosable,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Mutex => "partition-mutex-plain-bitmap",
            Self::AtomicLifetime => "owner-atomic-bitmap-lifetime",
            Self::PinnedAtomic => "guard-pinned-atomic-bitmap",
            Self::PinnedClosable => "guard-pinned-closable-bitmap",
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let operations = argument(&mut arguments, 5_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 21).max(3);
    println!("protocol,operations,threads,samples,median_mops,p05_mops,block_state_bytes");
    let mut measured: [Vec<f64>; Protocol::ALL.len()] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for sample in 0..samples {
        for offset in 0..Protocol::ALL.len() {
            let index = (sample + offset) % Protocol::ALL.len();
            measured[index].push(measure(Protocol::ALL[index], operations, threads));
        }
    }
    for (protocol, values) in Protocol::ALL.into_iter().zip(measured) {
        let (median, p05) = median_and_p05(values);
        let bytes = match protocol {
            Protocol::Mutex => size_of::<MutexBlock>(),
            Protocol::AtomicLifetime => size_of::<AtomicBlock>(),
            Protocol::PinnedAtomic => size_of::<PinnedAtomicBlock>(),
            Protocol::PinnedClosable => size_of::<PinnedClosableBlock>(),
        };
        println!(
            "{},{operations},{threads},{samples},{median:.3},{p05:.3},{bytes}",
            protocol.name()
        );
    }
}

fn measure(protocol: Protocol, operations: usize, threads: usize) -> f64 {
    let blocks_per_thread = operations.div_ceil(threads).div_ceil(BLOCK_SLOTS);
    let blocks = match protocol {
        Protocol::Mutex => Blocks::Mutex(Arc::new(
            (0..threads * blocks_per_thread)
                .map(|_| MutexBlock::new())
                .collect::<Box<[_]>>(),
        )),
        Protocol::AtomicLifetime => Blocks::Atomic(Arc::new(
            (0..threads * blocks_per_thread)
                .map(|_| AtomicBlock::new())
                .collect::<Box<[_]>>(),
        )),
        Protocol::PinnedAtomic => Blocks::PinnedAtomic(Arc::new(
            (0..threads * blocks_per_thread)
                .map(|_| PinnedAtomicBlock::new())
                .collect::<Box<[_]>>(),
        )),
        Protocol::PinnedClosable => Blocks::PinnedClosable(Arc::new(
            (0..threads * blocks_per_thread)
                .map(|_| PinnedClosableBlock::new())
                .collect::<Box<[_]>>(),
        )),
    };
    let barrier = Barrier::new(threads + 1);
    let checksum = AtomicUsize::new(0);
    let started = thread::scope(|scope| {
        for worker in 0..threads {
            let begin = operations * worker / threads;
            let end = operations * (worker + 1) / threads;
            let local_work = end - begin;
            let blocks = &blocks;
            let barrier = &barrier;
            let checksum = &checksum;
            scope.spawn(move || {
                barrier.wait();
                let mut local = 0;
                for operation in 0..local_work {
                    let block = worker * blocks_per_thread + operation / BLOCK_SLOTS;
                    local ^= blocks.claim(block);
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
            });
        }
        barrier.wait();
        Instant::now()
    });
    black_box(checksum.load(Ordering::Relaxed));
    operations as f64 / started.elapsed().as_secs_f64() / 1_000_000.0
}

enum Blocks {
    Mutex(Arc<Box<[MutexBlock]>>),
    Atomic(Arc<Box<[AtomicBlock]>>),
    PinnedAtomic(Arc<Box<[PinnedAtomicBlock]>>),
    PinnedClosable(Arc<Box<[PinnedClosableBlock]>>),
}

impl Blocks {
    fn claim(&self, block: usize) -> usize {
        match self {
            Self::Mutex(blocks) => blocks[block].claim(),
            Self::Atomic(blocks) => blocks[block].claim(),
            Self::PinnedAtomic(blocks) => blocks[block].claim(),
            Self::PinnedClosable(blocks) => blocks[block].claim(),
        }
    }
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
