//! Alternating-order exact frozen-map comparison for `PtrHash` and `PHast`.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::mem::size_of_val;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use packedgen::{PackedKeyArena, PackedKeyRef};
use ph::GetSize;
use ph::phast::{Function, Function2};
use ph::seeds::Bits8;
#[cfg(feature = "gxhash")]
use ptr_hash::hash::Gx128;
use ptr_hash::hash::KeyHasher;
#[cfg(not(feature = "gxhash"))]
use ptr_hash::hash::Xxh3_128;
use ptr_hash::{DefaultPtrHash, PtrHashParams};

type PtrIndex = DefaultPtrHash<DigestHasher, Digest>;
type RegularIndex = Function<Bits8>;
type PlusIndex = Function2<Bits8>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 300_000).max(2);
    let operations = argument(&mut arguments, 3_000_000).max(1);
    let threads = argument(&mut arguments, 1).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    let build_samples = argument(&mut arguments, 3).max(1);

    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let misses = (entries as u64..entries.saturating_mul(2) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();

    let mut build_times = (0..Strategy::ALL.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    for sample in 0..build_samples {
        for offset in 0..Strategy::ALL.len() {
            let strategy_index = (sample + offset) % Strategy::ALL.len();
            let strategy = Strategy::ALL[strategy_index];
            let started = Instant::now();
            black_box(build_map(strategy, &keys));
            build_times[strategy_index].push(started.elapsed());
        }
    }

    let ptr = PtrFrozenMap::build(
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key.as_ref(), index as u64)),
    );
    let regular = PhastFrozenMap::build(
        PhastKind::Regular,
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key.as_ref(), index as u64)),
    );
    let plus = PhastFrozenMap::build(
        PhastKind::Plus,
        keys.iter()
            .enumerate()
            .map(|(index, key)| (key.as_ref(), index as u64)),
    );
    let maps = [
        MapRef::PtrHash(&ptr),
        MapRef::Phast(&regular),
        MapRef::Phast(&plus),
    ];

    for (index, key) in keys.iter().enumerate() {
        for map in maps {
            assert_eq!(map.get(key), Some(index as u64));
        }
    }
    for key in misses.iter().take(10_000) {
        for map in maps {
            assert_eq!(map.get(key), None);
        }
    }

    let mut hit_times = (0..Strategy::ALL.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    let mut miss_times = (0..Strategy::ALL.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    for sample in 0..samples {
        for offset in 0..Strategy::ALL.len() {
            let strategy_index = (sample + offset) % Strategy::ALL.len();
            hit_times[strategy_index].push(measure_lookup(
                maps[strategy_index],
                &keys,
                operations,
                threads,
            ));
            miss_times[strategy_index].push(measure_lookup(
                maps[strategy_index],
                &misses,
                operations,
                threads,
            ));
        }
    }

    println!(
        "strategy,entries,operations,threads,samples,build_samples,build_ns_per_entry,bytes_per_entry,index_bits_per_entry,hit_mops,miss_mops"
    );
    for strategy_index in 0..Strategy::ALL.len() {
        build_times[strategy_index].sort_unstable();
        hit_times[strategy_index].sort_unstable();
        miss_times[strategy_index].sort_unstable();
        let build = build_times[strategy_index][build_times[strategy_index].len() / 2];
        let hit = hit_times[strategy_index][hit_times[strategy_index].len() / 2];
        let miss = miss_times[strategy_index][miss_times[strategy_index].len() / 2];
        let map = maps[strategy_index];
        println!(
            "{},{entries},{operations},{threads},{samples},{build_samples},{:.3},{:.3},{:.3},{:.3},{:.3}",
            Strategy::ALL[strategy_index].name(),
            build.as_secs_f64() * 1_000_000_000.0 / entries as f64,
            map.retained_bytes() as f64 / entries as f64,
            map.index_bits_per_entry(),
            operations as f64 / hit.as_secs_f64() / 1_000_000.0,
            operations as f64 / miss.as_secs_f64() / 1_000_000.0,
        );
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    PtrHash,
    Phast,
    PhastPlus,
}

impl Strategy {
    const ALL: [Self; 3] = [Self::PtrHash, Self::Phast, Self::PhastPlus];

    const fn name(self) -> &'static str {
        match self {
            Self::PtrHash => "ptrhash",
            Self::Phast => "phast",
            Self::PhastPlus => "phast-plus",
        }
    }
}

fn build_map(strategy: Strategy, keys: &[Box<[u8]>]) -> usize {
    match strategy {
        Strategy::PtrHash => {
            let map = PtrFrozenMap::build(
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (key.as_ref(), index as u64)),
            );
            black_box(&map);
            map.entries.len()
        }
        Strategy::Phast => {
            let map = PhastFrozenMap::build(
                PhastKind::Regular,
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (key.as_ref(), index as u64)),
            );
            black_box(&map);
            map.entries.len()
        }
        Strategy::PhastPlus => {
            let map = PhastFrozenMap::build(
                PhastKind::Plus,
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (key.as_ref(), index as u64)),
            );
            black_box(&map);
            map.entries.len()
        }
    }
}

#[derive(Clone, Copy)]
enum MapRef<'a> {
    PtrHash(&'a PtrFrozenMap),
    Phast(&'a PhastFrozenMap),
}

impl MapRef<'_> {
    fn get(self, key: &[u8]) -> Option<u64> {
        match self {
            Self::PtrHash(map) => map.get(key),
            Self::Phast(map) => map.get(key),
        }
    }

    fn retained_bytes(self) -> usize {
        match self {
            Self::PtrHash(map) => map.retained_bytes(),
            Self::Phast(map) => map.retained_bytes(),
        }
    }

    fn index_bits_per_entry(self) -> f64 {
        match self {
            Self::PtrHash(map) => map.index_bits_per_entry,
            Self::Phast(map) => map.index_bits_per_entry(),
        }
    }
}

struct PtrFrozenMap {
    index: PtrIndex,
    arena: PackedKeyArena,
    entries: Box<[PhastEntry]>,
    index_bits_per_entry: f64,
}

impl PtrFrozenMap {
    fn build<'a>(entries: impl IntoIterator<Item = (&'a [u8], u64)>) -> Self {
        let iterator = entries.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut arena = PackedKeyArena::new();
        let mut staged = Vec::with_capacity(lower);
        let mut digests = Vec::with_capacity(lower);
        for (key, value) in iterator {
            let digest = key_digest(key);
            let key = arena.insert(key).unwrap();
            staged.push((key, value));
            digests.push(digest);
        }
        let index = PtrIndex::try_new(&digests, PtrHashParams::default()).unwrap();
        let (pilot_bits, remap_bits) = index.bits_per_element();
        let mut indexed = staged
            .into_iter()
            .zip(digests)
            .map(|((key, value), digest)| (index.index(&digest), key, value))
            .collect::<Vec<_>>();
        indexed.sort_unstable_by_key(|(slot, _, _)| *slot);
        let entries = indexed
            .into_iter()
            .map(|(_, key, value)| PhastEntry { key, value })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            index,
            arena,
            entries,
            index_bits_per_entry: pilot_bits + remap_bits,
        }
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        let digest = key_digest(key);
        let entry = self.entries.get(self.index.index(&digest))?;
        (self.arena.get(entry.key) == Some(key)).then_some(entry.value)
    }

    fn retained_bytes(&self) -> usize {
        self.arena.allocated_bytes()
            + size_of_val(self.entries.as_ref())
            + ((self.index_bits_per_entry * self.entries.len() as f64) / 8.0).ceil() as usize
    }
}

struct PhastFrozenMap {
    index: PhastIndex,
    arena: PackedKeyArena,
    entries: Box<[PhastEntry]>,
    index_bytes: usize,
}

struct PhastEntry {
    key: PackedKeyRef,
    value: u64,
}

impl PhastFrozenMap {
    fn build<'a>(kind: PhastKind, entries: impl IntoIterator<Item = (&'a [u8], u64)>) -> Self {
        let iterator = entries.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut arena = PackedKeyArena::new();
        let mut staged = Vec::with_capacity(lower);
        let mut digests = Vec::with_capacity(lower);
        for (key, value) in iterator {
            let digest = key_digest(key);
            let key = arena.insert(key).unwrap();
            staged.push((key, value, digest));
            digests.push(digest);
        }
        let index = PhastIndex::build(kind, digests);
        let index_bytes = index.size_bytes();
        let mut indexed = staged
            .into_iter()
            .map(|(key, value, digest)| (index.get(&digest), key, value))
            .collect::<Vec<_>>();
        indexed.sort_unstable_by_key(|(slot, _, _)| *slot);
        assert!(
            indexed
                .iter()
                .enumerate()
                .all(|(expected, (actual, _, _))| expected == *actual),
            "PHast construction must assign every member one dense slot"
        );
        let entries = indexed
            .into_iter()
            .map(|(_, key, value)| PhastEntry { key, value })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            index,
            arena,
            entries,
            index_bytes,
        }
    }

    fn get(&self, key: &[u8]) -> Option<u64> {
        let digest = key_digest(key);
        let slot = self.index.get(&digest);
        let entry = self.entries.get(slot)?;
        (self.arena.get(entry.key) == Some(key)).then_some(entry.value)
    }

    fn retained_bytes(&self) -> usize {
        self.arena.allocated_bytes() + size_of_val(self.entries.as_ref()) + self.index_bytes
    }

    fn index_bits_per_entry(&self) -> f64 {
        self.index_bytes as f64 * 8.0 / self.entries.len() as f64
    }
}

#[derive(Clone, Copy)]
enum PhastKind {
    Regular,
    Plus,
}

enum PhastIndex {
    Regular(RegularIndex),
    Plus(PlusIndex),
}

impl PhastIndex {
    fn build(kind: PhastKind, digests: Vec<Digest>) -> Self {
        match kind {
            PhastKind::Regular => Self::Regular(RegularIndex::from_vec_st(digests)),
            PhastKind::Plus => Self::Plus(PlusIndex::from_vec_st(digests)),
        }
    }

    fn get(&self, digest: &Digest) -> usize {
        match self {
            Self::Regular(index) => index.get(digest),
            Self::Plus(index) => index.get(digest),
        }
    }

    fn size_bytes(&self) -> usize {
        match self {
            Self::Regular(index) => index.size_bytes(),
            Self::Plus(index) => index.size_bytes(),
        }
    }
}

#[derive(Clone, Copy, Hash)]
struct Digest(u128);

#[derive(Clone)]
struct DigestHasher;

impl KeyHasher<Digest> for DigestHasher {
    type H = u128;

    fn hash(digest: &Digest, seed: u64) -> Self::H {
        let seed = u128::from(seed);
        digest.0 ^ seed ^ (seed << 64)
    }
}

fn key_digest(key: &[u8]) -> Digest {
    #[cfg(feature = "gxhash")]
    let digest = <Gx128 as KeyHasher<[u8]>>::hash(key, 0);
    #[cfg(not(feature = "gxhash"))]
    let digest = <Xxh3_128 as KeyHasher<[u8]>>::hash(key, 0);
    Digest(digest)
}

fn measure_lookup(
    map: MapRef<'_>,
    keys: &[Box<[u8]>],
    operations: usize,
    threads: usize,
) -> Duration {
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let start = &start;
            let done = &done;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                for operation in begin..end {
                    let index = mix(operation as u64) as usize % keys.len();
                    black_box(map.get(&keys[index]));
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    })
}

fn binary_key(value: u64) -> Box<[u8]> {
    let mut key = vec![0_u8; 32];
    key[0..8].copy_from_slice(&value.to_le_bytes());
    key[8..16].copy_from_slice(&value.rotate_left(17).to_le_bytes());
    key[16..24].copy_from_slice(&value.wrapping_mul(0x9e37_79b9).to_le_bytes());
    key[24..32].copy_from_slice(&(value ^ 0xa5a5_a5a5_a5a5_a5a5).to_le_bytes());
    key.into_boxed_slice()
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}
