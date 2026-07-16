//! Warm concurrent-map scaling comparison using the same keys, values, and hasher.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flurry::HashMap as FlurryHashMap;
use hashbrown::{DefaultHashBuilder, HashMap as HashBrownMap};
#[cfg(feature = "prepared-keys")]
use packedgen::AtomicPreparedKey;
use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, FrozenIndexBackend, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};
use papaya::HashMap as PapayaHashMap;
use parking_lot::RwLock;
use scc::HashMap as SccHashMap;

const GUARD_BATCH_OPERATIONS: usize = 4_096;
#[cfg(feature = "prepared-keys")]
const PREPARED_OPERATION_BATCH: usize = 16;

type Key = Box<[u8]>;
type Atomic = LockFreeAtomicU64GenerationMap;
type Dash = DashMap<Key, u64, DefaultHashBuilder>;
type Papaya = PapayaHashMap<Key, u64, DefaultHashBuilder>;
type Scc = SccHashMap<Key, u64, DefaultHashBuilder>;
type Flurry = FlurryHashMap<Key, u64, DefaultHashBuilder>;
type Locked = RwLock<HashBrownMap<Key, u64, DefaultHashBuilder>>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let requested_operations = argument(&mut arguments, 300_000).max(1);
    let default_threads =
        std::thread::available_parallelism().map_or(8, |count| count.get().min(16));
    let maximum_threads = argument(&mut arguments, default_threads).max(1);
    let sample_count = argument(&mut arguments, 7).max(3);
    let shards = argument(&mut arguments, 64).max(2).next_power_of_two();
    let workload_filter = arguments.next();
    let implementation_filter = arguments.next();
    let thread_filter = arguments.next().map(|value| {
        value
            .parse::<usize>()
            .expect("thread filter must be a positive integer")
            .max(1)
    });
    let hash_builder = DefaultHashBuilder::default();
    #[cfg(feature = "shared-gx")]
    let generation_hash_builder = GenerationHashBuilder::with_seed(0);
    #[cfg(not(feature = "shared-gx"))]
    let generation_hash_builder = GenerationHashBuilder::default();
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let misses = (entries as u64..entries.saturating_add(requested_operations) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();

    eprintln!("guard_batch_operations={GUARD_BATCH_OPERATIONS}; threads are unpinned OS threads");
    println!(
        "implementation,workload,entries,operations,threads,shards,samples,median_ns,throughput_mops,paired_vs_first_pct"
    );
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| filter != "all" && workload.name() != filter)
        {
            continue;
        }
        let operations = workload.operations(entries, requested_operations);
        for threads in thread_counts(maximum_threads) {
            if thread_filter.is_some_and(|filter| threads != filter) {
                continue;
            }
            let implementations = Implementation::ALL
                .into_iter()
                .filter(|implementation| {
                    implementation_filter
                        .as_deref()
                        .is_none_or(|filter| implementation.matches_filter(filter))
                })
                .collect::<Vec<_>>();
            let mut measurements = vec![Vec::with_capacity(sample_count); implementations.len()];
            for sample in 0..sample_count {
                for offset in 0..implementations.len() {
                    let implementation_index = (sample + offset) % implementations.len();
                    measurements[implementation_index].push(measure(
                        implementations[implementation_index],
                        workload,
                        operations,
                        threads,
                        shards,
                        &keys,
                        &misses,
                        &hash_builder,
                        &generation_hash_builder,
                    ));
                }
            }
            let paired_changes = paired_changes(&measurements);
            for ((implementation, samples), paired_change) in implementations
                .into_iter()
                .zip(&mut measurements)
                .zip(paired_changes)
            {
                samples.sort_unstable();
                let median = samples[samples.len() / 2];
                let throughput = operations as f64 / median.as_secs_f64() / 1_000_000.0;
                println!(
                    "{},{},{entries},{operations},{threads},{shards},{},{},{throughput:.3},{paired_change:.3}",
                    implementation.name(),
                    workload.name(),
                    samples.len(),
                    median.as_nanos(),
                );
            }
        }
    }
}

fn paired_changes(measurements: &[Vec<Duration>]) -> Vec<f64> {
    let Some(control) = measurements.first() else {
        return Vec::new();
    };
    measurements
        .iter()
        .map(|samples| {
            let mut changes = control
                .iter()
                .zip(samples)
                .map(|(control, sample)| {
                    (control.as_secs_f64() / sample.as_secs_f64() - 1.0) * 100.0
                })
                .collect::<Vec<_>>();
            changes.sort_unstable_by(f64::total_cmp);
            changes[changes.len() / 2]
        })
        .collect()
}

fn measure(
    implementation: Implementation,
    workload: Workload,
    operations: usize,
    threads: usize,
    shards: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
    hash_builder: &DefaultHashBuilder,
    generation_hash_builder: &GenerationHashBuilder,
) -> Duration {
    let map = BenchMap::build(
        implementation,
        workload.new_key_capacity(operations),
        shards,
        keys,
        hash_builder,
        generation_hash_builder,
    );
    map.warm(keys, misses);
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    let elapsed = std::thread::scope(|scope| {
        for thread in 0..threads {
            let map = &map;
            let start = &start;
            let done = &done;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                map.run(workload, begin, end, keys, misses);
                done.wait();
            });
        }
        let started = Instant::now();
        start.wait();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

enum BenchMap {
    Atomic(Box<Atomic>),
    #[cfg(feature = "prepared-keys")]
    AtomicPrepared(Box<PreparedAtomic>),
    #[cfg(feature = "prepared-keys")]
    AtomicPreparedBatch(Box<PreparedAtomic>),
    Dash(Box<Dash>),
    Papaya(Box<Papaya>),
    Scc(Box<Scc>),
    Flurry(Box<Flurry>),
    RwLockHashBrown(Box<Locked>),
}

impl BenchMap {
    fn build(
        implementation: Implementation,
        new_key_capacity: usize,
        shards: usize,
        keys: &[[u8; 32]],
        hash_builder: &DefaultHashBuilder,
        generation_hash_builder: &GenerationHashBuilder,
    ) -> Self {
        let capacity = keys.len().saturating_add(new_key_capacity);
        match implementation {
            Implementation::AtomicPtrHash => Self::Atomic(Box::new(build_atomic(
                FrozenIndexBackend::PtrHash,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
                new_key_capacity,
                keys,
                generation_hash_builder,
            ))),
            #[cfg(feature = "prepared-keys")]
            Implementation::AtomicPrepared => Self::AtomicPrepared(Box::new(
                build_prepared_atomic(new_key_capacity, keys, generation_hash_builder),
            )),
            #[cfg(feature = "prepared-keys")]
            Implementation::AtomicPreparedBatch => Self::AtomicPreparedBatch(Box::new(
                build_prepared_atomic(new_key_capacity, keys, generation_hash_builder),
            )),
            #[cfg(feature = "phast")]
            Implementation::AtomicPhastPlus => Self::Atomic(Box::new(build_atomic(
                FrozenIndexBackend::PhastPlus,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
                new_key_capacity,
                keys,
                generation_hash_builder,
            ))),
            #[cfg(feature = "phast")]
            Implementation::AtomicPhastPlusOneByte => Self::Atomic(Box::new(build_atomic(
                FrozenIndexBackend::PhastPlus,
                AtomicGenerationBaseFilter::OneBytePerEntry,
                new_key_capacity,
                keys,
                generation_hash_builder,
            ))),
            Implementation::Dash => {
                let map = Dash::with_capacity_and_hasher_and_shard_amount(
                    capacity,
                    hash_builder.clone(),
                    shards,
                );
                for (index, key) in keys.iter().enumerate() {
                    map.insert(key.as_slice().into(), index as u64);
                }
                Self::Dash(Box::new(map))
            }
            Implementation::Papaya => {
                let map = Papaya::with_capacity_and_hasher(capacity, hash_builder.clone());
                let guard = map.guard();
                for (index, key) in keys.iter().enumerate() {
                    map.insert(key.as_slice().into(), index as u64, &guard);
                }
                drop(guard);
                Self::Papaya(Box::new(map))
            }
            Implementation::Scc => {
                let map = Scc::with_capacity_and_hasher(capacity, hash_builder.clone());
                for (index, key) in keys.iter().enumerate() {
                    map.insert_sync(key.as_slice().into(), index as u64)
                        .unwrap();
                }
                Self::Scc(Box::new(map))
            }
            Implementation::Flurry => {
                let map = Flurry::with_capacity_and_hasher(capacity, hash_builder.clone());
                let guard = map.guard();
                for (index, key) in keys.iter().enumerate() {
                    map.insert(key.as_slice().into(), index as u64, &guard);
                }
                drop(guard);
                Self::Flurry(Box::new(map))
            }
            Implementation::RwLockHashBrown => {
                let mut map =
                    HashBrownMap::with_capacity_and_hasher(capacity, hash_builder.clone());
                for (index, key) in keys.iter().enumerate() {
                    map.insert(key.as_slice().into(), index as u64);
                }
                Self::RwLockHashBrown(Box::new(RwLock::new(map)))
            }
        }
    }

    fn warm(&self, keys: &[[u8; 32]], misses: &[[u8; 32]]) {
        let operations = keys.len().min(10_000);
        match self {
            Self::Atomic(map) => warm(&AtomicOps(map), operations, keys, misses),
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPrepared(map) => {
                warm(&PreparedAtomicOps(map), operations, keys, misses);
            }
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPreparedBatch(map) => {
                warm(&PreparedAtomicOps(map), operations, keys, misses);
            }
            Self::Dash(map) => warm(&DashOps(map), operations, keys, misses),
            Self::Papaya(map) => {
                let ops = PapayaOps {
                    map,
                    guard: map.guard(),
                };
                warm(&ops, operations, keys, misses);
            }
            Self::Scc(map) => warm(&SccOps(map), operations, keys, misses),
            Self::Flurry(map) => {
                let ops = FlurryOps {
                    map,
                    guard: map.guard(),
                };
                warm(&ops, operations, keys, misses);
            }
            Self::RwLockHashBrown(map) => {
                warm(&RwLockHashBrownOps(map), operations, keys, misses);
            }
        }
    }

    fn run(
        &self,
        workload: Workload,
        begin: usize,
        end: usize,
        keys: &[[u8; 32]],
        misses: &[[u8; 32]],
    ) {
        match self {
            Self::Atomic(map) => run_range(&AtomicOps(map), workload, begin, end, keys, misses),
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPrepared(map) => {
                run_range(&PreparedAtomicOps(map), workload, begin, end, keys, misses);
            }
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPreparedBatch(map) => {
                run_prepared_batch_range(map, workload, begin, end, keys, misses);
            }
            Self::Dash(map) => run_range(&DashOps(map), workload, begin, end, keys, misses),
            Self::Papaya(map) => run_guard_batches(begin, end, |batch_begin, batch_end| {
                let ops = PapayaOps {
                    map,
                    guard: map.guard(),
                };
                run_range(&ops, workload, batch_begin, batch_end, keys, misses);
            }),
            Self::Scc(map) => run_range(&SccOps(map), workload, begin, end, keys, misses),
            Self::Flurry(map) => run_guard_batches(begin, end, |batch_begin, batch_end| {
                let ops = FlurryOps {
                    map,
                    guard: map.guard(),
                };
                run_range(&ops, workload, batch_begin, batch_end, keys, misses);
            }),
            Self::RwLockHashBrown(map) => {
                run_range(&RwLockHashBrownOps(map), workload, begin, end, keys, misses);
            }
        }
    }
}

fn build_atomic(
    index_backend: FrozenIndexBackend,
    base_filter: AtomicGenerationBaseFilter,
    new_key_capacity: usize,
    keys: &[[u8; 32]],
    hash_builder: &GenerationHashBuilder,
) -> Atomic {
    Atomic::try_from_entries_with_options_and_writer_hash_and_index(
        keys.iter().enumerate().map(|(index, key)| {
            (
                key,
                NonMaxU64::new(index as u64).expect("benchmark index is representable"),
            )
        }),
        new_key_capacity,
        AtomicGenerationOverlay::AtomicFixed32,
        base_filter,
        hash_builder.clone(),
        index_backend,
    )
    .unwrap()
}

#[cfg(feature = "prepared-keys")]
fn build_prepared_atomic(
    new_key_capacity: usize,
    keys: &[[u8; 32]],
    generation_hash_builder: &GenerationHashBuilder,
) -> PreparedAtomic {
    let map = build_atomic(
        FrozenIndexBackend::PtrHash,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
        new_key_capacity,
        keys,
        generation_hash_builder,
    );
    let hot_entries = keys.len().div_ceil(100).max(1);
    let prepared = keys[..hot_entries]
        .iter()
        .map(|key| map.prepare_key(key))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    PreparedAtomic { map, prepared }
}

trait MapOps {
    fn get(&self, key: &[u8]) -> Option<u64>;
    fn get_indexed(&self, key: &[u8], _index: usize) -> Option<u64> {
        self.get(key)
    }
    fn insert(&self, key: &[u8], value: u64) -> Option<u64>;
    fn insert_indexed(&self, key: &[u8], _index: usize, value: u64) -> Option<u64> {
        self.insert(key, value)
    }
    fn update(&self, key: &[u8]) -> bool;
    fn update_indexed(&self, key: &[u8], _index: usize) -> bool {
        self.update(key)
    }
    fn remove(&self, key: &[u8]) -> Option<u64>;
}

struct AtomicOps<'a>(&'a Atomic);

impl MapOps for AtomicOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get(key).map(NonMaxU64::get)
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        match self.0.insert(key, non_max(value)) {
            packedgen::InsertOutcome::Inserted => None,
            packedgen::InsertOutcome::Replaced(previous) => Some(previous.get()),
        }
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.update(key, increment).is_some()
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.remove(key).map(NonMaxU64::get)
    }
}

#[cfg(feature = "prepared-keys")]
struct PreparedAtomic {
    map: Atomic,
    prepared: Box<[AtomicPreparedKey]>,
}

#[cfg(feature = "prepared-keys")]
struct PreparedAtomicOps<'a>(&'a PreparedAtomic);

#[cfg(feature = "prepared-keys")]
impl MapOps for PreparedAtomicOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.map.get(key).map(NonMaxU64::get)
    }

    fn get_indexed(&self, key: &[u8], index: usize) -> Option<u64> {
        self.0.prepared.get(index).map_or_else(
            || self.get(key),
            |prepared| self.0.map.get_prepared(key, prepared).map(NonMaxU64::get),
        )
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        match self.0.map.insert(key, non_max(value)) {
            packedgen::InsertOutcome::Inserted => None,
            packedgen::InsertOutcome::Replaced(previous) => Some(previous.get()),
        }
    }

    fn insert_indexed(&self, key: &[u8], index: usize, value: u64) -> Option<u64> {
        self.0.prepared.get(index).map_or_else(
            || self.insert(key, value),
            |prepared| match self.0.map.insert_prepared(key, prepared, non_max(value)) {
                packedgen::InsertOutcome::Inserted => None,
                packedgen::InsertOutcome::Replaced(previous) => Some(previous.get()),
            },
        )
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.map.update(key, increment).is_some()
    }

    fn update_indexed(&self, key: &[u8], index: usize) -> bool {
        self.0.prepared.get(index).map_or_else(
            || self.update(key),
            |prepared| {
                self.0
                    .map
                    .update_prepared(key, prepared, increment)
                    .is_some()
            },
        )
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.map.remove(key).map(NonMaxU64::get)
    }
}

struct DashOps<'a>(&'a Dash);

impl MapOps for DashOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.get(key).map(|value| *value)
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.0.insert(key.into(), value)
    }

    fn update(&self, key: &[u8]) -> bool {
        let Some(mut value) = self.0.get_mut(key) else {
            return false;
        };
        *value += 1;
        true
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.remove(key).map(|(_, value)| value)
    }
}

struct PapayaOps<'a> {
    map: &'a Papaya,
    guard: papaya::LocalGuard<'a>,
}

impl MapOps for PapayaOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.map.get(key, &self.guard).copied()
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.map.insert(key.into(), value, &self.guard).copied()
    }

    fn update(&self, key: &[u8]) -> bool {
        self.map
            .update(key.into(), |value| value + 1, &self.guard)
            .is_some()
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.map.remove(key, &self.guard).copied()
    }
}

struct SccOps<'a>(&'a Scc);

impl MapOps for SccOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.read_sync(key, |_, value| *value)
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.0.upsert_sync(key.into(), value)
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0
            .update_sync(key, |_, value| {
                *value += 1;
            })
            .is_some()
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.remove_sync(key).map(|(_, value)| value)
    }
}

struct FlurryOps<'a> {
    map: &'a Flurry,
    guard: flurry::Guard<'a>,
}

impl MapOps for FlurryOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.map.get(key, &self.guard).copied()
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.map.insert(key.into(), value, &self.guard).copied()
    }

    fn update(&self, key: &[u8]) -> bool {
        self.map
            .compute_if_present(key, |_, value| Some(value + 1), &self.guard)
            .is_some()
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.map.remove(key, &self.guard).copied()
    }
}

struct RwLockHashBrownOps<'a>(&'a Locked);

impl MapOps for RwLockHashBrownOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.read().get(key).copied()
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.0.write().insert(key.into(), value)
    }

    fn update(&self, key: &[u8]) -> bool {
        let mut map = self.0.write();
        let Some(value) = map.get_mut(key) else {
            return false;
        };
        *value += 1;
        true
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.write().remove(key)
    }
}

fn warm(map: &impl MapOps, operations: usize, keys: &[[u8; 32]], misses: &[[u8; 32]]) {
    for operation in 0..operations {
        let index = operation % keys.len();
        black_box(map.get_indexed(&keys[index], index));
        black_box(map.get(&misses[operation % misses.len()]));
    }
}

fn run_guard_batches(begin: usize, end: usize, mut run: impl FnMut(usize, usize)) {
    let mut batch_begin = begin;
    while batch_begin < end {
        let batch_end = batch_begin.saturating_add(GUARD_BATCH_OPERATIONS).min(end);
        run(batch_begin, batch_end);
        batch_begin = batch_end;
    }
}

fn run_range(
    map: &impl MapOps,
    workload: Workload,
    begin: usize,
    end: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    for operation in begin..end {
        let hit_index = mix(operation as u64) as usize % keys.len();
        match workload {
            Workload::ReadHit => {
                black_box(map.get_indexed(&keys[hit_index], hit_index));
            }
            Workload::ReadHotSet => {
                let hot_entries = keys.len().div_ceil(100).max(1);
                let index = if mix((operation as u64) ^ 0xd6e8_feb8_6659_fd93) % 10 < 9 {
                    hit_index % hot_entries
                } else {
                    hit_index
                };
                black_box(map.get_indexed(&keys[index], index));
            }
            Workload::ReadMiss => {
                black_box(map.get(&misses[operation]));
            }
            Workload::InsertHit => {
                black_box(map.insert_indexed(&keys[hit_index], hit_index, operation as u64));
            }
            Workload::InsertMiss => {
                black_box(map.insert(&misses[operation], operation as u64));
            }
            Workload::InsertHotSet => {
                let hot_entries = keys.len().div_ceil(100).max(1);
                let index = if mix((operation as u64) ^ 0x94d0_49bb_1331_11eb) % 10 < 9 {
                    hit_index % hot_entries
                } else {
                    hit_index
                };
                black_box(map.insert_indexed(&keys[index], index, operation as u64));
            }
            Workload::UpdateHit => {
                black_box(map.update_indexed(&keys[hit_index], hit_index).then_some(0));
            }
            Workload::UpdateMiss => {
                black_box(map.update(&misses[operation]).then_some(0));
            }
            Workload::UpdateHotSet => {
                let hot_entries = keys.len().div_ceil(100).max(1);
                let index = if mix((operation as u64) ^ 0x517c_c1b7_2722_0a95) % 10 < 9 {
                    hit_index % hot_entries
                } else {
                    hit_index
                };
                black_box(map.update_indexed(&keys[index], index).then_some(0));
            }
            Workload::UpdateHot => {
                black_box(map.update(&keys[0]).then_some(0));
            }
            Workload::DeleteHit => {
                black_box(map.remove(&keys[operation]));
            }
            Workload::DeleteMiss => {
                black_box(map.remove(&misses[operation]));
            }
            Workload::CacheMix95 => run_cache_mix_95(map, operation, hit_index, keys, misses),
        }
    }
}

#[cfg(feature = "prepared-keys")]
fn run_prepared_batch_range(
    map: &PreparedAtomic,
    workload: Workload,
    begin: usize,
    end: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    if !matches!(
        workload,
        Workload::ReadHotSet | Workload::UpdateHotSet | Workload::InsertHotSet
    ) {
        run_range(&PreparedAtomicOps(map), workload, begin, end, keys, misses);
        return;
    }

    let fallback = AtomicPreparedKey::fallback();
    let mut batch_keys = [&[][..]; PREPARED_OPERATION_BATCH];
    let mut batch_prepared = [fallback; PREPARED_OPERATION_BATCH];
    let mut batch_values = [None; PREPARED_OPERATION_BATCH];
    let mut batch_inserted = [non_max(0); PREPARED_OPERATION_BATCH];
    let hot_seed = match workload {
        Workload::ReadHotSet => 0xd6e8_feb8_6659_fd93,
        Workload::InsertHotSet => 0x94d0_49bb_1331_11eb,
        Workload::UpdateHotSet => 0x517c_c1b7_2722_0a95,
        _ => unreachable!("only prepared hot-set workloads reach the batch loop"),
    };
    let mut operation = begin;
    while operation < end {
        let count = PREPARED_OPERATION_BATCH.min(end - operation);
        for offset in 0..count {
            let current = operation + offset;
            let hit_index = mix(current as u64) as usize % keys.len();
            let hot_entries = keys.len().div_ceil(100).max(1);
            let index = if mix((current as u64) ^ hot_seed) % 10 < 9 {
                hit_index % hot_entries
            } else {
                hit_index
            };
            batch_keys[offset] = &keys[index];
            batch_prepared[offset] = map.prepared.get(index).copied().unwrap_or(fallback);
            batch_inserted[offset] = non_max(current as u64);
        }
        match workload {
            Workload::ReadHotSet => map.map.get_prepared_batch(
                &batch_keys[..count],
                &batch_prepared[..count],
                &mut batch_values[..count],
            ),
            Workload::UpdateHotSet => map.map.update_prepared_batch(
                &batch_keys[..count],
                &batch_prepared[..count],
                &mut batch_values[..count],
                increment,
            ),
            Workload::InsertHotSet => map.map.insert_prepared_batch(
                &batch_keys[..count],
                &batch_prepared[..count],
                &batch_inserted[..count],
                &mut batch_values[..count],
            ),
            _ => unreachable!("only prepared hot-set workloads reach the batch operation"),
        }
        black_box(&batch_values[..count]);
        operation += count;
    }
}

fn run_cache_mix_95(
    map: &impl MapOps,
    operation: usize,
    hit_index: usize,
    keys: &[[u8; 32]],
    misses: &[[u8; 32]],
) {
    match mix((operation as u64) ^ 0x5a5a_a5a5) % 200 {
        0..=189 => black_box(map.get_indexed(&keys[hit_index], hit_index)),
        190..=193 => black_box(map.get(&misses[operation])),
        194..=197 => black_box(map.update_indexed(&keys[hit_index], hit_index).then_some(0)),
        198 => black_box(map.insert(&misses[operation], operation as u64).map(|_| 0)),
        _ => black_box(map.remove(&keys[hit_index]).map(|_| 0)),
    };
}

#[derive(Clone, Copy)]
enum Implementation {
    AtomicPtrHash,
    #[cfg(feature = "prepared-keys")]
    AtomicPrepared,
    #[cfg(feature = "prepared-keys")]
    AtomicPreparedBatch,
    #[cfg(feature = "phast")]
    AtomicPhastPlus,
    #[cfg(feature = "phast")]
    AtomicPhastPlusOneByte,
    Dash,
    Papaya,
    Scc,
    Flurry,
    RwLockHashBrown,
}

impl Implementation {
    #[cfg(all(not(feature = "phast"), not(feature = "prepared-keys")))]
    const ALL: [Self; 6] = [
        Self::AtomicPtrHash,
        Self::Dash,
        Self::Papaya,
        Self::Scc,
        Self::Flurry,
        Self::RwLockHashBrown,
    ];

    #[cfg(all(not(feature = "phast"), feature = "prepared-keys"))]
    const ALL: [Self; 8] = [
        Self::AtomicPtrHash,
        Self::AtomicPrepared,
        Self::AtomicPreparedBatch,
        Self::Dash,
        Self::Papaya,
        Self::Scc,
        Self::Flurry,
        Self::RwLockHashBrown,
    ];

    #[cfg(all(feature = "phast", not(feature = "prepared-keys")))]
    const ALL: [Self; 8] = [
        Self::AtomicPtrHash,
        Self::AtomicPhastPlus,
        Self::AtomicPhastPlusOneByte,
        Self::Dash,
        Self::Papaya,
        Self::Scc,
        Self::Flurry,
        Self::RwLockHashBrown,
    ];

    #[cfg(all(feature = "phast", feature = "prepared-keys"))]
    const ALL: [Self; 10] = [
        Self::AtomicPtrHash,
        Self::AtomicPrepared,
        Self::AtomicPreparedBatch,
        Self::AtomicPhastPlus,
        Self::AtomicPhastPlusOneByte,
        Self::Dash,
        Self::Papaya,
        Self::Scc,
        Self::Flurry,
        Self::RwLockHashBrown,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AtomicPtrHash => "packedgen-atomic-ptrhash",
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPrepared => "packedgen-atomic-prepared-hotset",
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPreparedBatch => "packedgen-atomic-prepared-batch16-hotset",
            #[cfg(feature = "phast")]
            Self::AtomicPhastPlus => "packedgen-atomic-phast-plus",
            #[cfg(feature = "phast")]
            Self::AtomicPhastPlusOneByte => "packedgen-atomic-phast-plus-one-byte-filter",
            Self::Dash => "dashmap-6.2.1",
            Self::Papaya => "papaya-0.2.4",
            Self::Scc => "scc-3.8.5",
            Self::Flurry => "flurry-0.5.2",
            Self::RwLockHashBrown => "rwlock-hashbrown-0.17.1",
        }
    }

    fn matches_filter(self, filter: &str) -> bool {
        if self.name() == filter {
            return true;
        }
        match filter {
            "atomic-indexes" => self.is_atomic_index(),
            #[cfg(feature = "prepared-keys")]
            "prepared-compare" => {
                matches!(
                    self,
                    Self::AtomicPtrHash
                        | Self::AtomicPrepared
                        | Self::AtomicPreparedBatch
                        | Self::Dash
                )
            }
            #[cfg(feature = "prepared-keys")]
            "prepared-dash" => {
                matches!(
                    self,
                    Self::AtomicPrepared | Self::AtomicPreparedBatch | Self::Dash
                )
            }
            #[cfg(feature = "phast")]
            "ptr-phast" => matches!(self, Self::AtomicPtrHash | Self::AtomicPhastPlus),
            #[cfg(feature = "phast")]
            "ptr-phast-one" => {
                matches!(self, Self::AtomicPtrHash | Self::AtomicPhastPlusOneByte)
            }
            _ => false,
        }
    }

    const fn is_atomic_index(self) -> bool {
        match self {
            Self::AtomicPtrHash => true,
            #[cfg(feature = "prepared-keys")]
            Self::AtomicPrepared | Self::AtomicPreparedBatch => true,
            #[cfg(feature = "phast")]
            Self::AtomicPhastPlus | Self::AtomicPhastPlusOneByte => true,
            Self::Dash | Self::Papaya | Self::Scc | Self::Flurry | Self::RwLockHashBrown => false,
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadHotSet,
    ReadMiss,
    InsertHit,
    InsertMiss,
    InsertHotSet,
    UpdateHit,
    UpdateMiss,
    UpdateHotSet,
    UpdateHot,
    DeleteHit,
    DeleteMiss,
    CacheMix95,
}

impl Workload {
    const ALL: [Self; 13] = [
        Self::ReadHit,
        Self::ReadHotSet,
        Self::ReadMiss,
        Self::InsertHit,
        Self::InsertMiss,
        Self::InsertHotSet,
        Self::UpdateHit,
        Self::UpdateMiss,
        Self::UpdateHotSet,
        Self::UpdateHot,
        Self::DeleteHit,
        Self::DeleteMiss,
        Self::CacheMix95,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadHotSet => "read_hotset_90pct_on_1pct",
            Self::ReadMiss => "read_miss",
            Self::InsertHit => "insert_hit",
            Self::InsertMiss => "insert_miss",
            Self::InsertHotSet => "insert_hotset_90pct_on_1pct",
            Self::UpdateHit => "update_hit",
            Self::UpdateMiss => "update_miss",
            Self::UpdateHotSet => "update_hotset_90pct_on_1pct",
            Self::UpdateHot => "update_hot_key",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
            Self::CacheMix95 => "cache_mix_95rh2rm2u0.5i0.5d",
        }
    }

    const fn operations(self, entries: usize, requested: usize) -> usize {
        if matches!(self, Self::DeleteHit) {
            if requested < entries {
                requested
            } else {
                entries
            }
        } else {
            requested
        }
    }

    const fn new_key_capacity(self, operations: usize) -> usize {
        match self {
            Self::InsertMiss => operations,
            Self::CacheMix95 => operations.div_ceil(200),
            _ => 0,
        }
    }
}

fn thread_counts(maximum: usize) -> Vec<usize> {
    let mut counts = vec![1];
    let mut count = 2;
    while count < maximum {
        counts.push(count);
        count = count.saturating_mul(2);
    }
    if maximum > 8 {
        counts.push(maximum.min(12));
    }
    if maximum > 1 {
        counts.push(maximum);
    }
    counts.sort_unstable();
    counts.dedup();
    counts
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("benchmark values remain representable")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
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
