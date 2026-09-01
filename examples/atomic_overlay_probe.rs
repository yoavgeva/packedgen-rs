//! Focused alternating-order probe for atomic generation overlay strategies.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flurry::HashMap as FlurryHashMap;
use hashbrown::{DefaultHashBuilder, HashMap as HashBrownMap};
use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, LockFreeAtomicU64GenerationMap, NonMaxU64,
};
use papaya::HashMap as PapayaHashMap;
use parking_lot::RwLock;
use scc::HashMap as SccHashMap;

type ExternalDash = DashMap<Box<[u8]>, u64, DefaultHashBuilder>;
type ExternalPapaya = PapayaHashMap<Box<[u8]>, u64, DefaultHashBuilder>;
type ExternalScc = SccHashMap<Box<[u8]>, u64, DefaultHashBuilder>;
type ExternalFlurry = FlurryHashMap<Box<[u8]>, u64, DefaultHashBuilder>;
type ExternalRwLock = RwLock<HashBrownMap<Box<[u8]>, u64, DefaultHashBuilder>>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let operations = argument(&mut arguments, 500_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 9).max(3);
    let requested_key_bytes = argument(&mut arguments, 32);
    let mixed_keys = requested_key_bytes == 0;
    let key_bytes = requested_key_bytes.clamp(8, 128);
    let reported_key_bytes = usize::from(!mixed_keys) * key_bytes;
    let workload_filter = arguments.next();
    let strategy_group = arguments.next();
    let mut modes = if mixed_keys {
        vec![
            Strategy::Overlay(AtomicGenerationOverlay::AtomicAdaptive),
            Strategy::Overlay(AtomicGenerationOverlay::AtomicUpTo32),
            Strategy::Overlay(AtomicGenerationOverlay::Papaya),
            Strategy::DirectPapaya,
            Strategy::Scc,
            Strategy::Flurry,
            Strategy::DashMap,
            Strategy::RwLockHashBrown,
        ]
    } else {
        let mut modes = vec![
            Strategy::Overlay(AtomicGenerationOverlay::AtomicAdaptive),
            Strategy::Overlay(compact_mode(key_bytes)),
            Strategy::Overlay(AtomicGenerationOverlay::Papaya),
            Strategy::DirectPapaya,
            Strategy::Scc,
            Strategy::DashMap,
        ];
        if key_bytes <= 32 {
            modes.insert(0, Strategy::Overlay(AtomicGenerationOverlay::AtomicUpTo32));
        }
        if key_bytes <= 16 {
            modes.insert(0, Strategy::Overlay(AtomicGenerationOverlay::AtomicUpTo16));
        }
        if key_bytes <= 8 {
            modes.insert(0, Strategy::Overlay(AtomicGenerationOverlay::AtomicUpTo8));
        }
        if key_bytes == 32 {
            modes.insert(0, Strategy::Overlay(AtomicGenerationOverlay::AtomicFixed32));
            modes.push(Strategy::Overlay(AtomicGenerationOverlay::ArcSwapFixed32));
        }
        modes
    };
    if strategy_group.as_deref() == Some("atomic-only") {
        modes.retain(|mode| {
            matches!(
                mode,
                Strategy::Overlay(AtomicGenerationOverlay::AtomicFixed32)
            )
        });
    } else if strategy_group.as_deref() == Some("adaptive-leading") {
        modes.retain(|mode| {
            matches!(
                mode,
                Strategy::Overlay(AtomicGenerationOverlay::AtomicAdaptive)
                    | Strategy::DirectPapaya
                    | Strategy::Scc
                    | Strategy::DashMap
            )
        });
    } else if strategy_group.as_deref() == Some("adaptive-atomic-papaya") {
        modes.retain(|mode| {
            matches!(
                mode,
                Strategy::Overlay(
                    AtomicGenerationOverlay::AtomicAdaptive
                        | AtomicGenerationOverlay::AtomicUpTo8
                        | AtomicGenerationOverlay::AtomicUpTo16
                        | AtomicGenerationOverlay::AtomicUpTo32
                ) | Strategy::DirectPapaya
            )
        });
    } else if strategy_group.as_deref() == Some("adaptive-only") {
        modes.retain(|mode| {
            matches!(
                mode,
                Strategy::Overlay(AtomicGenerationOverlay::AtomicAdaptive)
            )
        });
    }
    let keys = (0..entries as u64)
        .map(|value| binary_key(value, selected_key_bytes(value, key_bytes, mixed_keys)))
        .collect::<Vec<_>>();
    let inserts = (entries as u64..entries.saturating_add(operations) as u64)
        .map(|value| binary_key(value, selected_key_bytes(value, key_bytes, mixed_keys)))
        .collect::<Vec<_>>();

    println!(
        "strategy,workload,key_bytes,entries,operations,threads,samples,median_ns,p95_sample_ns,p95_over_median_pct,throughput_mops"
    );
    for workload in Workload::ALL {
        if workload_filter
            .as_deref()
            .is_some_and(|filter| workload.name() != filter)
        {
            continue;
        }
        let workload_operations = workload.operations(entries, operations);
        let mut measurements = (0..modes.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        for sample in 0..samples {
            for offset in 0..modes.len() {
                let mode_index = (sample + offset) % modes.len();
                let duration = measure_strategy(
                    modes[mode_index],
                    workload,
                    entries,
                    workload_operations,
                    threads,
                    &keys,
                    &inserts,
                );
                measurements[mode_index].push(duration);
            }
        }
        for (mode, samples) in modes.iter().copied().zip(&mut measurements) {
            samples.sort_unstable();
            let median = samples[samples.len() / 2];
            let p95_index = samples.len().saturating_mul(95).div_ceil(100) - 1;
            let p95 = samples[p95_index];
            let p95_over_median = (p95.as_secs_f64() / median.as_secs_f64() - 1.0) * 100.0;
            let throughput = workload_operations as f64 / median.as_secs_f64() / 1_000_000.0;
            println!(
                "{},{},{reported_key_bytes},{entries},{workload_operations},{threads},{},{},{},{p95_over_median:.3},{throughput:.3}",
                mode.name(),
                workload.name(),
                samples.len(),
                median.as_nanos(),
                p95.as_nanos(),
            );
        }
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    Overlay(AtomicGenerationOverlay),
    DirectPapaya,
    Scc,
    Flurry,
    DashMap,
    RwLockHashBrown,
}

impl Strategy {
    const fn name(self) -> &'static str {
        match self {
            Self::Overlay(mode) => mode_name(mode),
            Self::DirectPapaya => "papaya-0.2.4-direct",
            Self::Scc => "scc-3.8.5",
            Self::Flurry => "flurry-0.5.2",
            Self::DashMap => "dashmap-6.2.1",
            Self::RwLockHashBrown => "rwlock-hashbrown-0.17.1",
        }
    }
}

fn measure_strategy(
    strategy: Strategy,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) -> Duration {
    match strategy {
        Strategy::Overlay(mode) => {
            measure(mode, workload, entries, operations, threads, keys, inserts)
        }
        Strategy::DirectPapaya => measure_external::<PapayaExternal>(
            workload, entries, operations, threads, keys, inserts,
        ),
        Strategy::Scc => {
            measure_external::<SccExternal>(workload, entries, operations, threads, keys, inserts)
        }
        Strategy::Flurry => measure_external::<FlurryExternal>(
            workload, entries, operations, threads, keys, inserts,
        ),
        Strategy::DashMap => {
            measure_external::<DashExternal>(workload, entries, operations, threads, keys, inserts)
        }
        Strategy::RwLockHashBrown => measure_external::<RwLockExternal>(
            workload, entries, operations, threads, keys, inserts,
        ),
    }
}

fn measure(
    mode: AtomicGenerationOverlay,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) -> Duration {
    let map = match workload {
        Workload::InsertMiss | Workload::InsertMissLearned => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                operations.saturating_add(
                    usize::from(matches!(workload, Workload::InsertMissLearned))
                        * keys.len().min(4_096),
                ),
                mode,
            )
        }
        Workload::InsertMissOverflow => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                entries.div_ceil(100),
                mode,
            )
        }
        Workload::InsertMissWithBase => {
            LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
                keys.iter()
                    .enumerate()
                    .map(|(index, key)| (key.as_ref(), value(index as u64))),
                operations,
                mode,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
            )
        }
        Workload::OverlayReadHit
        | Workload::OverlayReadHotSet
        | Workload::OverlayUpdateHit
        | Workload::OverlayUpdateHotSet
        | Workload::Churn50
        | Workload::CacheMix90
        | Workload::CacheMix95 => {
            let capacity = match workload {
                Workload::CacheMix90 => entries.saturating_add(operations.div_ceil(100)),
                Workload::CacheMix95 => entries.saturating_add(operations.div_ceil(200)),
                Workload::Churn50 => entries.saturating_add(operations.div_ceil(2)),
                _ => entries,
            };
            LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
                std::iter::empty::<([u8; 32], NonMaxU64)>(),
                capacity,
                mode,
            )
        }
        Workload::ReadHit
        | Workload::ReadMiss
        | Workload::InsertHit
        | Workload::UpdateHit
        | Workload::UpdateMiss
        | Workload::UpdateHot
        | Workload::DeleteHit
        | Workload::DeleteMiss => LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            keys.iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index as u64))),
            entries.div_ceil(100),
            mode,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
        ),
    }
    .unwrap();
    if matches!(workload, Workload::InsertMissLearned) {
        for (index, key) in keys.iter().take(4_096).enumerate() {
            map.insert(key, value(index as u64));
        }
    }
    if matches!(
        workload,
        Workload::OverlayReadHit
            | Workload::OverlayReadHotSet
            | Workload::OverlayUpdateHit
            | Workload::OverlayUpdateHotSet
            | Workload::Churn50
            | Workload::CacheMix90
            | Workload::CacheMix95
    ) {
        for (index, key) in keys.iter().enumerate() {
            map.insert(key, value(index as u64));
        }
    }
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
                for operation in begin..end {
                    match workload {
                        Workload::ReadHit | Workload::OverlayReadHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.get(key.as_ref()).unwrap());
                        }
                        Workload::OverlayReadHotSet => {
                            let index = hot_set_index(operation, keys.len());
                            black_box(map.get(keys[index].as_ref()).unwrap());
                        }
                        Workload::ReadMiss => {
                            black_box(map.get(inserts[operation].as_ref()));
                        }
                        Workload::InsertHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.insert(key.as_ref(), value(operation as u64)));
                        }
                        Workload::InsertMiss
                        | Workload::InsertMissLearned
                        | Workload::InsertMissWithBase
                        | Workload::InsertMissOverflow => {
                            map.insert(inserts[operation].as_ref(), value(operation as u64));
                        }
                        Workload::UpdateHit | Workload::OverlayUpdateHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            map.update(key.as_ref(), increment).unwrap();
                        }
                        Workload::OverlayUpdateHotSet => {
                            let index = hot_set_index(operation, keys.len());
                            map.update(keys[index].as_ref(), increment).unwrap();
                        }
                        Workload::UpdateMiss => {
                            assert_eq!(map.update(inserts[operation].as_ref(), increment), None);
                        }
                        Workload::UpdateHot => {
                            map.update(keys[0].as_ref(), increment).unwrap();
                        }
                        Workload::DeleteHit => {
                            black_box(map.remove(keys[operation].as_ref()).unwrap());
                        }
                        Workload::DeleteMiss => {
                            assert_eq!(map.remove(inserts[operation].as_ref()), None);
                        }
                        Workload::Churn50 => {
                            let index = operation / 2;
                            if operation & 1 == 0 {
                                black_box(map.remove(keys[index].as_ref()).unwrap());
                            } else {
                                black_box(
                                    map.insert(inserts[index].as_ref(), value(operation as u64)),
                                );
                            }
                        }
                        Workload::CacheMix90 => {
                            run_atomic_cache_mix(map, operation, 90, keys, inserts);
                        }
                        Workload::CacheMix95 => {
                            run_atomic_cache_mix(map, operation, 95, keys, inserts);
                        }
                    }
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

trait ExternalMap {
    type Map: Sync;
    type Ops<'a>: ExternalOps
    where
        Self: 'a;

    fn build(capacity: usize) -> Self::Map;
    fn ops(map: &Self::Map) -> Self::Ops<'_>;
}

trait ExternalOps {
    fn get(&self, key: &[u8]) -> Option<u64>;
    fn insert(&self, key: &[u8], value: u64) -> Option<u64>;
    fn update(&self, key: &[u8]) -> bool;
    fn remove(&self, key: &[u8]) -> Option<u64>;
}

struct DashExternal;
struct PapayaExternal;
struct SccExternal;
struct FlurryExternal;
struct RwLockExternal;

fn measure_external<M: ExternalMap>(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) -> Duration {
    let seed = keys.len().min(4_096);
    let capacity = match workload {
        Workload::InsertMiss => operations,
        Workload::InsertMissLearned => operations.saturating_add(seed),
        Workload::InsertMissOverflow => entries.div_ceil(100),
        Workload::InsertMissWithBase => entries.saturating_add(operations),
        Workload::CacheMix90 => entries.saturating_add(operations.div_ceil(100)),
        Workload::CacheMix95 => entries.saturating_add(operations.div_ceil(200)),
        Workload::Churn50 => entries.saturating_add(operations.div_ceil(2)),
        _ => entries,
    };
    let map = M::build(capacity);
    {
        let ops = M::ops(&map);
        if !matches!(
            workload,
            Workload::InsertMiss | Workload::InsertMissLearned | Workload::InsertMissOverflow
        ) {
            for (index, key) in keys.iter().enumerate() {
                ops.insert(key, index as u64);
            }
        } else if matches!(workload, Workload::InsertMissLearned) {
            for (index, key) in keys.iter().take(seed).enumerate() {
                ops.insert(key, index as u64);
            }
        }
    }

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
                let ops = M::ops(map);
                start.wait();
                for operation in begin..end {
                    run_external_operation(&ops, workload, operation, keys, inserts);
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

fn run_external_operation(
    map: &impl ExternalOps,
    workload: Workload,
    operation: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) {
    let key = &keys[mix(operation as u64) as usize % keys.len()];
    match workload {
        Workload::ReadHit | Workload::OverlayReadHit => {
            black_box(map.get(key).unwrap());
        }
        Workload::OverlayReadHotSet => {
            black_box(
                map.get(&keys[hot_set_index(operation, keys.len())])
                    .unwrap(),
            );
        }
        Workload::ReadMiss => {
            black_box(map.get(&inserts[operation]));
        }
        Workload::InsertHit => {
            black_box(map.insert(key, operation as u64));
        }
        Workload::InsertMiss
        | Workload::InsertMissLearned
        | Workload::InsertMissWithBase
        | Workload::InsertMissOverflow => {
            black_box(map.insert(&inserts[operation], operation as u64));
        }
        Workload::UpdateHit | Workload::OverlayUpdateHit => {
            assert!(map.update(key));
        }
        Workload::OverlayUpdateHotSet => {
            assert!(map.update(&keys[hot_set_index(operation, keys.len())]));
        }
        Workload::UpdateMiss => {
            assert!(!map.update(&inserts[operation]));
        }
        Workload::UpdateHot => {
            assert!(map.update(&keys[0]));
        }
        Workload::DeleteHit => {
            black_box(map.remove(&keys[operation]).unwrap());
        }
        Workload::DeleteMiss => {
            assert_eq!(map.remove(&inserts[operation]), None);
        }
        Workload::Churn50 => {
            let index = operation / 2;
            if operation & 1 == 0 {
                black_box(map.remove(&keys[index]).unwrap());
            } else {
                black_box(map.insert(&inserts[index], operation as u64));
            }
        }
        Workload::CacheMix90 => run_external_cache_mix(map, operation, 90, keys, inserts),
        Workload::CacheMix95 => run_external_cache_mix(map, operation, 95, keys, inserts),
    }
}

fn run_external_cache_mix(
    map: &impl ExternalOps,
    operation: usize,
    hit_percentage: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) {
    let hit = &keys[mix(operation as u64) as usize % keys.len()];
    let roll = mix((operation as u64) ^ 0xa5a5_5a5a);
    let divisor = if hit_percentage == 90 { 100 } else { 200 };
    match (hit_percentage, roll % divisor) {
        (90, 0..=89) | (95, 0..=189) => {
            black_box(map.get(hit));
        }
        (90, 90..=94) | (95, 190..=193) => {
            black_box(map.get(&inserts[operation]));
        }
        (90, 95..=97) | (95, 194..=197) => {
            black_box(map.update(hit));
        }
        (90, 98) | (95, 198) => {
            black_box(map.insert(&inserts[operation], operation as u64));
        }
        (90 | 95, _) => {
            black_box(map.remove(hit));
        }
        _ => unreachable!("cache mix supports 90% or 95% read hits"),
    }
}

fn hot_set_index(operation: usize, entries: usize) -> usize {
    let uniform = mix(operation as u64) as usize % entries;
    if mix((operation as u64) ^ 0xd6e8_feb8_6659_fd93) % 10 < 9 {
        uniform % entries.div_ceil(100).max(1)
    } else {
        uniform
    }
}

struct DashOps<'a>(&'a ExternalDash);

impl ExternalMap for DashExternal {
    type Map = ExternalDash;
    type Ops<'a> = DashOps<'a>;

    fn build(capacity: usize) -> Self::Map {
        ExternalDash::with_capacity_and_hasher_and_shard_amount(
            capacity,
            DefaultHashBuilder::default(),
            64,
        )
    }

    fn ops(map: &Self::Map) -> Self::Ops<'_> {
        DashOps(map)
    }
}

impl ExternalOps for DashOps<'_> {
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
    map: &'a ExternalPapaya,
    guard: papaya::LocalGuard<'a>,
}

impl ExternalMap for PapayaExternal {
    type Map = ExternalPapaya;
    type Ops<'a> = PapayaOps<'a>;

    fn build(capacity: usize) -> Self::Map {
        ExternalPapaya::with_capacity_and_hasher(capacity, DefaultHashBuilder::default())
    }

    fn ops(map: &Self::Map) -> Self::Ops<'_> {
        PapayaOps {
            map,
            guard: map.guard(),
        }
    }
}

impl ExternalOps for PapayaOps<'_> {
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

struct SccOps<'a>(&'a ExternalScc);

impl ExternalMap for SccExternal {
    type Map = ExternalScc;
    type Ops<'a> = SccOps<'a>;

    fn build(capacity: usize) -> Self::Map {
        ExternalScc::with_capacity_and_hasher(capacity, DefaultHashBuilder::default())
    }

    fn ops(map: &Self::Map) -> Self::Ops<'_> {
        SccOps(map)
    }
}

impl ExternalOps for SccOps<'_> {
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.0.read_sync(key, |_, value| *value)
    }

    fn insert(&self, key: &[u8], value: u64) -> Option<u64> {
        self.0.upsert_sync(key.into(), value)
    }

    fn update(&self, key: &[u8]) -> bool {
        self.0.update_sync(key, |_, value| *value += 1).is_some()
    }

    fn remove(&self, key: &[u8]) -> Option<u64> {
        self.0.remove_sync(key).map(|(_, value)| value)
    }
}

struct FlurryOps<'a> {
    map: &'a ExternalFlurry,
    guard: flurry::Guard<'a>,
}

impl ExternalMap for FlurryExternal {
    type Map = ExternalFlurry;
    type Ops<'a> = FlurryOps<'a>;

    fn build(capacity: usize) -> Self::Map {
        ExternalFlurry::with_capacity_and_hasher(capacity, DefaultHashBuilder::default())
    }

    fn ops(map: &Self::Map) -> Self::Ops<'_> {
        FlurryOps {
            map,
            guard: map.guard(),
        }
    }
}

impl ExternalOps for FlurryOps<'_> {
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

struct RwLockOps<'a>(&'a ExternalRwLock);

impl ExternalMap for RwLockExternal {
    type Map = ExternalRwLock;
    type Ops<'a> = RwLockOps<'a>;

    fn build(capacity: usize) -> Self::Map {
        RwLock::new(HashBrownMap::with_capacity_and_hasher(
            capacity,
            DefaultHashBuilder::default(),
        ))
    }

    fn ops(map: &Self::Map) -> Self::Ops<'_> {
        RwLockOps(map)
    }
}

impl ExternalOps for RwLockOps<'_> {
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

#[allow(dead_code)]
fn measure_dashmap(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) -> Duration {
    const SHARDS: usize = 64;
    let seed = keys.len().min(4_096);
    let capacity = match workload {
        Workload::InsertMiss => operations,
        Workload::InsertMissLearned => operations.saturating_add(seed),
        Workload::InsertMissOverflow => entries.div_ceil(100),
        Workload::InsertMissWithBase => entries.saturating_add(operations),
        Workload::CacheMix90 => entries.saturating_add(operations.div_ceil(100)),
        Workload::CacheMix95 => entries.saturating_add(operations.div_ceil(200)),
        _ => entries,
    };
    let map =
        DashMap::<Box<[u8]>, u64, DefaultHashBuilder>::with_capacity_and_hasher_and_shard_amount(
            capacity,
            DefaultHashBuilder::default(),
            SHARDS,
        );
    if !matches!(
        workload,
        Workload::InsertMiss | Workload::InsertMissLearned | Workload::InsertMissOverflow
    ) {
        for (index, key) in keys.iter().enumerate() {
            map.insert(key.clone(), index as u64);
        }
    } else if matches!(workload, Workload::InsertMissLearned) {
        for (index, key) in keys.iter().take(seed).enumerate() {
            map.insert(key.clone(), index as u64);
        }
    }

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
                for operation in begin..end {
                    match workload {
                        Workload::ReadHit | Workload::OverlayReadHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(*map.get(key.as_ref()).unwrap());
                        }
                        Workload::OverlayReadHotSet => {
                            let index = hot_set_index(operation, keys.len());
                            black_box(*map.get(keys[index].as_ref()).unwrap());
                        }
                        Workload::ReadMiss => {
                            black_box(map.get(inserts[operation].as_ref()));
                        }
                        Workload::InsertHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            black_box(map.insert(key.clone(), operation as u64));
                        }
                        Workload::InsertMiss
                        | Workload::InsertMissLearned
                        | Workload::InsertMissWithBase
                        | Workload::InsertMissOverflow => {
                            map.insert(inserts[operation].clone(), operation as u64);
                        }
                        Workload::UpdateHit | Workload::OverlayUpdateHit => {
                            let key = &keys[mix(operation as u64) as usize % keys.len()];
                            *map.get_mut(key.as_ref()).unwrap() += 1;
                        }
                        Workload::OverlayUpdateHotSet => {
                            let index = hot_set_index(operation, keys.len());
                            *map.get_mut(keys[index].as_ref()).unwrap() += 1;
                        }
                        Workload::UpdateMiss => {
                            assert!(map.get_mut(inserts[operation].as_ref()).is_none());
                        }
                        Workload::UpdateHot => {
                            *map.get_mut(keys[0].as_ref()).unwrap() += 1;
                        }
                        Workload::DeleteHit => {
                            black_box(map.remove(keys[operation].as_ref()).unwrap());
                        }
                        Workload::DeleteMiss => {
                            assert!(map.remove(inserts[operation].as_ref()).is_none());
                        }
                        Workload::Churn50 => {
                            let index = operation / 2;
                            if operation & 1 == 0 {
                                black_box(map.remove(keys[index].as_ref()).unwrap());
                            } else {
                                black_box(map.insert(inserts[index].clone(), operation as u64));
                            }
                        }
                        Workload::CacheMix90 => {
                            run_dash_cache_mix(map, operation, 90, keys, inserts);
                        }
                        Workload::CacheMix95 => {
                            run_dash_cache_mix(map, operation, 95, keys, inserts);
                        }
                    }
                }
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

fn run_atomic_cache_mix(
    map: &LockFreeAtomicU64GenerationMap,
    operation: usize,
    hit_percentage: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) {
    let hit = &keys[mix(operation as u64) as usize % keys.len()];
    let roll = mix((operation as u64) ^ 0xa5a5_5a5a);
    if hit_percentage == 90 {
        match roll % 100 {
            0..=89 => {
                black_box(map.get(hit));
            }
            90..=94 => {
                black_box(map.get(&inserts[operation]));
            }
            95..=97 => {
                black_box(map.update(hit, increment));
            }
            98 => {
                black_box(map.insert(&inserts[operation], value(operation as u64)));
            }
            _ => {
                black_box(map.remove(hit));
            }
        }
    } else {
        match roll % 200 {
            0..=189 => {
                black_box(map.get(hit));
            }
            190..=193 => {
                black_box(map.get(&inserts[operation]));
            }
            194..=197 => {
                black_box(map.update(hit, increment));
            }
            198 => {
                black_box(map.insert(&inserts[operation], value(operation as u64)));
            }
            _ => {
                black_box(map.remove(hit));
            }
        }
    }
}

#[allow(dead_code)]
fn run_dash_cache_mix(
    map: &DashMap<Box<[u8]>, u64, DefaultHashBuilder>,
    operation: usize,
    hit_percentage: usize,
    keys: &[Box<[u8]>],
    inserts: &[Box<[u8]>],
) {
    let hit = &keys[mix(operation as u64) as usize % keys.len()];
    let roll = mix((operation as u64) ^ 0xa5a5_5a5a);
    let update = || {
        if let Some(mut current) = map.get_mut(hit.as_ref()) {
            *current += 1;
        }
    };
    if hit_percentage == 90 {
        match roll % 100 {
            0..=89 => {
                black_box(map.get(hit.as_ref()));
            }
            90..=94 => {
                black_box(map.get(inserts[operation].as_ref()));
            }
            95..=97 => {
                update();
            }
            98 => {
                black_box(map.insert(inserts[operation].clone(), operation as u64));
            }
            _ => {
                black_box(map.remove(hit.as_ref()));
            }
        }
    } else {
        match roll % 200 {
            0..=189 => {
                black_box(map.get(hit.as_ref()));
            }
            190..=193 => {
                black_box(map.get(inserts[operation].as_ref()));
            }
            194..=197 => {
                update();
            }
            198 => {
                black_box(map.insert(inserts[operation].clone(), operation as u64));
            }
            _ => {
                black_box(map.remove(hit.as_ref()));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    ReadHit,
    ReadMiss,
    InsertHit,
    InsertMiss,
    InsertMissLearned,
    InsertMissWithBase,
    InsertMissOverflow,
    OverlayReadHit,
    OverlayReadHotSet,
    OverlayUpdateHit,
    OverlayUpdateHotSet,
    UpdateHit,
    UpdateMiss,
    UpdateHot,
    DeleteHit,
    DeleteMiss,
    Churn50,
    CacheMix90,
    CacheMix95,
}

impl Workload {
    const ALL: [Self; 19] = [
        Self::ReadHit,
        Self::ReadMiss,
        Self::InsertHit,
        Self::InsertMiss,
        Self::InsertMissLearned,
        Self::InsertMissWithBase,
        Self::InsertMissOverflow,
        Self::OverlayReadHit,
        Self::OverlayReadHotSet,
        Self::OverlayUpdateHit,
        Self::OverlayUpdateHotSet,
        Self::UpdateHit,
        Self::UpdateMiss,
        Self::UpdateHot,
        Self::DeleteHit,
        Self::DeleteMiss,
        Self::Churn50,
        Self::CacheMix90,
        Self::CacheMix95,
    ];

    const fn operations(self, entries: usize, requested: usize) -> usize {
        if matches!(self, Self::DeleteHit) {
            if requested < entries {
                requested
            } else {
                entries
            }
        } else if matches!(self, Self::Churn50) {
            let maximum = entries.saturating_mul(2);
            (if requested < maximum {
                requested
            } else {
                maximum
            }) & !1
        } else {
            requested
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ReadHit => "read_hit",
            Self::ReadMiss => "read_miss",
            Self::InsertHit => "insert_hit",
            Self::InsertMiss => "insert_miss",
            Self::InsertMissLearned => "insert_miss_learned",
            Self::InsertMissWithBase => "insert_miss_with_base",
            Self::InsertMissOverflow => "insert_miss_overflow",
            Self::OverlayReadHit => "overlay_read_hit",
            Self::OverlayReadHotSet => "overlay_read_hotset_90pct_on_1pct",
            Self::OverlayUpdateHit => "overlay_update_hit",
            Self::OverlayUpdateHotSet => "overlay_update_hotset_90pct_on_1pct",
            Self::UpdateHit => "update_hit",
            Self::UpdateMiss => "update_miss",
            Self::UpdateHot => "update_hot_key",
            Self::DeleteHit => "delete_hit",
            Self::DeleteMiss => "delete_miss",
            Self::Churn50 => "churn_50delete_50insert",
            Self::CacheMix90 => "cache_mix_90rh5rm3u1i1d",
            Self::CacheMix95 => "cache_mix_95rh2rm2u0.5i0.5d",
        }
    }
}

const fn mode_name(mode: AtomicGenerationOverlay) -> &'static str {
    match mode {
        AtomicGenerationOverlay::AtomicFixed32 => "atomic-bucket32",
        AtomicGenerationOverlay::AtomicUpTo8 => "atomic-up-to8",
        AtomicGenerationOverlay::AtomicUpTo16 => "atomic-up-to16",
        AtomicGenerationOverlay::AtomicAdaptive => "atomic-adaptive",
        AtomicGenerationOverlay::AtomicUpTo32 => "atomic-up-to32",
        AtomicGenerationOverlay::CompactFixed32 | AtomicGenerationOverlay::CompactSized { .. } => {
            "inline-sized-papaya"
        }
        AtomicGenerationOverlay::Papaya => "boxed-key-papaya",
        AtomicGenerationOverlay::ArcSwapFixed32 => "dense-arcswap32",
    }
}

fn compact_mode(key_bytes: usize) -> AtomicGenerationOverlay {
    if key_bytes == 32 {
        AtomicGenerationOverlay::CompactFixed32
    } else {
        AtomicGenerationOverlay::CompactSized {
            key_bytes: u8::try_from(key_bytes).expect("probe key size fits u8"),
        }
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be positive integers")
    })
}

fn value(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("probe values remain representable")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    NonMaxU64::new(value.get() + 1).expect("probe values remain representable")
}

fn binary_key(value: u64, key_bytes: usize) -> Box<[u8]> {
    let mut key = vec![0_u8; key_bytes];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    key.into_boxed_slice()
}

fn selected_key_bytes(value: u64, fixed: usize, mixed: bool) -> usize {
    if !mixed {
        return fixed;
    }
    match value % 100 {
        0..=39 => 8,
        40..=64 => 16,
        65..=79 => 24,
        80..=89 => 32,
        _ => 48,
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
