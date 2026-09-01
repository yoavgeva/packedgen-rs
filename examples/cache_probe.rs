//! Alternating full-cache comparison against a Papaya control.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

#[cfg(all(feature = "allocation-diagnostics", not(feature = "jemalloc-probe")))]
use std::alloc::System;
use std::hint::black_box;
use std::ops::Index;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use packedgen::{CacheAdmissionOutcome, CacheConfig, DirectPackedCache, PackedCache};
use papaya::HashMap as PapayaHashMap;
use parking_lot::Mutex;
#[cfg(all(feature = "allocation-diagnostics", not(feature = "jemalloc-probe")))]
use stats_alloc::INSTRUMENTED_SYSTEM;
#[cfg(feature = "allocation-diagnostics")]
use stats_alloc::{Region, StatsAlloc};
#[cfg(feature = "jemalloc-probe")]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc-probe", not(feature = "allocation-diagnostics")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(all(feature = "allocation-diagnostics", not(feature = "jemalloc-probe")))]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[cfg(all(feature = "allocation-diagnostics", feature = "jemalloc-probe"))]
#[global_allocator]
static GLOBAL: StatsAlloc<Jemalloc> = StatsAlloc::new(Jemalloc);

const VALUE_BYTES: usize = 64;
const PIPELINE_MIX_SPAN: usize = 3_200;
const PIPELINE_HIT_READ_END: usize = 3_040;
const PIPELINE_MISS_END: usize = 3_104;
const PIPELINE_REPLACE_END: usize = 3_168;
const PIPELINE_INSERT_END: usize = 3_184;

const fn pipeline_mix_uses_auxiliary_key(operation: usize) -> bool {
    let phase = operation % PIPELINE_MIX_SPAN;
    (phase >= PIPELINE_HIT_READ_END && phase < PIPELINE_MISS_END)
        || (phase >= PIPELINE_REPLACE_END && phase < PIPELINE_INSERT_END)
}

struct ControlValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: AtomicU32,
    accessed: AtomicBool,
}

type PapayaArcCache = PapayaHashMap<Box<[u8]>, Arc<ControlValue>, DefaultHashBuilder>;
type PapayaInlineCache = PapayaHashMap<Box<[u8]>, ControlValue, DefaultHashBuilder>;

#[derive(Clone, Copy, Debug)]
struct Sample {
    mops: f64,
    read_hits: u64,
    read_operations: u64,
    final_entries: usize,
    rebuilds: u64,
}

struct OperationKeys {
    keys: Vec<Box<[u8]>>,
    operation_to_key: Option<SparseOperationIndex>,
    operation_modulus: Option<usize>,
}

struct SparseOperationIndex {
    occupied: Vec<u64>,
    rank_prefix: Vec<u32>,
}

impl SparseOperationIndex {
    fn new(operations: usize) -> Self {
        Self::from_predicate(operations, |operation| {
            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
            (190..=193).contains(&roll) || roll == 198
        })
    }

    fn pipeline_mix(operations: usize) -> Self {
        Self::from_predicate(operations, pipeline_mix_uses_auxiliary_key)
    }

    fn from_predicate(operations: usize, uses_key: impl Fn(usize) -> bool) -> Self {
        let mut occupied = vec![0_u64; operations.div_ceil(u64::BITS as usize)];
        for operation in 0..operations {
            if uses_key(operation) {
                occupied[operation / u64::BITS as usize] |= 1_u64 << (operation % 64);
            }
        }
        let mut rank_prefix = Vec::with_capacity(occupied.len());
        let mut count = 0_u32;
        for word in &occupied {
            rank_prefix.push(count);
            count = count
                .checked_add(word.count_ones())
                .expect("mixed trace key count fits u32");
        }
        Self {
            occupied,
            rank_prefix,
        }
    }

    fn key_count(&self) -> usize {
        self.occupied
            .last()
            .map_or(0, |word| {
                self.rank_prefix[self.rank_prefix.len() - 1] + word.count_ones()
            })
            .try_into()
            .expect("mixed trace key count fits usize")
    }

    fn rank(&self, operation: usize) -> usize {
        let word_index = operation / u64::BITS as usize;
        let bit = operation % u64::BITS as usize;
        let word = self.occupied[word_index];
        assert_ne!(
            word & (1_u64 << bit),
            0,
            "operation uses a compact trace key"
        );
        let below = word & (1_u64 << bit).wrapping_sub(1);
        usize::try_from(self.rank_prefix[word_index] + below.count_ones())
            .expect("mixed trace key rank fits usize")
    }
}

impl OperationKeys {
    fn dense(entries: usize, count: usize) -> Self {
        Self {
            keys: (0..count)
                .map(|index| mixed_binary_key(entries + 1_000_000 + index))
                .collect(),
            operation_to_key: None,
            operation_modulus: None,
        }
    }

    fn cyclic(entries: usize, count: usize) -> Self {
        Self {
            keys: (0..count)
                .map(|index| mixed_binary_key(entries + 1_000_000 + index))
                .collect(),
            operation_to_key: None,
            operation_modulus: Some(count),
        }
    }

    fn sparse_mixed(entries: usize, operations: usize) -> Self {
        let operation_to_key = SparseOperationIndex::new(operations);
        let key_count = operation_to_key.key_count();
        Self {
            keys: (0..key_count.max(1))
                .map(|index| mixed_binary_key(entries + 1_000_000 + index))
                .collect(),
            operation_to_key: Some(operation_to_key),
            operation_modulus: None,
        }
    }

    fn sparse_pipeline_mix(entries: usize, operations: usize) -> Self {
        let operation_to_key = SparseOperationIndex::pipeline_mix(operations);
        let key_count = operation_to_key.key_count();
        Self {
            keys: (0..key_count.max(1))
                .map(|index| mixed_binary_key(entries + 1_000_000 + index))
                .collect(),
            operation_to_key: Some(operation_to_key),
            operation_modulus: None,
        }
    }
}

impl Index<usize> for OperationKeys {
    type Output = Box<[u8]>;

    fn index(&self, operation: usize) -> &Self::Output {
        let key_index = self.operation_to_key.as_ref().map_or_else(
            || {
                self.operation_modulus
                    .map_or(operation, |modulus| operation % modulus)
            },
            |index| index.rank(operation),
        );
        &self.keys[key_index]
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    Packed,
    Direct,
    DirectAsync,
    PapayaInline,
    PapayaArc,
}

impl Strategy {
    const fn name(self) -> &'static str {
        match self {
            Self::Packed => "packed-cache",
            Self::Direct => "direct-packed-cache",
            Self::DirectAsync => "direct-packed-cache-async-1pct",
            Self::PapayaInline => "papaya-cache-inline-pinned-compact-control",
            Self::PapayaArc => "papaya-cache-arc-pinned-compact-control",
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Read,
    ReadHot,
    ReadPrepared,
    ReadPreparedHot,
    Peek,
    ReadTracked,
    ReadMiss,
    ReadMixed,
    Replace,
    ReplaceBatch,
    ReplacePreparedHot,
    ReplacePreparedHotBatch,
    ReplacePreparedHotBulk64,
    Insert,
    InsertExisting,
    InsertBatch,
    InsertUntracked,
    RemoveHit,
    RemoveHitBatch,
    RemoveHitBatchUntracked,
    RemoveHitFrozen,
    RemoveHitFrozenBatch,
    RemoveHitFrozenBatchPrepared,
    RemoveHitFrozenBatchUntracked,
    RemoveMiss,
    Touch,
    TouchPreparedHot,
    Cache95,
    Cache95Tracked,
    Cache95PreparedHotTracked,
    Cache95PreparedHotPipeline,
    Cache95PreparedHotPipelineBulk64,
    Cache95Pressure,
    Cache95HotPressure,
}

impl Workload {
    const ALL: [Self; 33] = [
        Self::Read,
        Self::ReadHot,
        Self::ReadPrepared,
        Self::ReadPreparedHot,
        Self::Peek,
        Self::ReadTracked,
        Self::ReadMiss,
        Self::Replace,
        Self::ReplaceBatch,
        Self::ReplacePreparedHot,
        Self::ReplacePreparedHotBatch,
        Self::ReplacePreparedHotBulk64,
        Self::Insert,
        Self::InsertExisting,
        Self::InsertBatch,
        Self::InsertUntracked,
        Self::RemoveHit,
        Self::RemoveHitBatch,
        Self::RemoveHitBatchUntracked,
        Self::RemoveHitFrozen,
        Self::RemoveHitFrozenBatch,
        Self::RemoveHitFrozenBatchPrepared,
        Self::RemoveHitFrozenBatchUntracked,
        Self::RemoveMiss,
        Self::Touch,
        Self::TouchPreparedHot,
        Self::Cache95,
        Self::Cache95Tracked,
        Self::Cache95PreparedHotTracked,
        Self::Cache95PreparedHotPipeline,
        Self::Cache95PreparedHotPipelineBulk64,
        Self::Cache95Pressure,
        Self::Cache95HotPressure,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Read => "read_hit",
            Self::ReadHot => "read_hit_hot_90pct_on_1pct",
            Self::ReadPrepared => "read_hit_prepared",
            Self::ReadPreparedHot => "read_hit_prepared_hot_90pct_on_1pct",
            Self::Peek => "read_peek",
            Self::ReadTracked => "read_hit_tracked",
            Self::ReadMiss => "read_miss",
            Self::ReadMixed => "read_mix_configured_miss",
            Self::Replace => "replace_hit",
            Self::ReplaceBatch => "replace_hit_batch",
            Self::ReplacePreparedHot => "replace_hit_prepared_hot_90pct_on_1pct",
            Self::ReplacePreparedHotBatch => "replace_hit_prepared_hot_batch_90pct_on_1pct",
            Self::ReplacePreparedHotBulk64 => "replace_hit_prepared_hot_bulk_64_90pct_on_1pct",
            Self::Insert => "insert_miss",
            Self::InsertExisting => "insert_existing",
            Self::InsertBatch => "insert_miss_batch_32",
            Self::InsertUntracked => "insert_miss_untracked",
            Self::RemoveHit => "remove_hit",
            Self::RemoveHitBatch => "remove_hit_batch",
            Self::RemoveHitBatchUntracked => "remove_hit_batch_untracked",
            Self::RemoveHitFrozen => "remove_hit_frozen",
            Self::RemoveHitFrozenBatch => "remove_hit_frozen_batch",
            Self::RemoveHitFrozenBatchPrepared => "remove_hit_frozen_batch_prepared",
            Self::RemoveHitFrozenBatchUntracked => "remove_hit_frozen_batch_untracked",
            Self::RemoveMiss => "remove_miss",
            Self::Touch => "touch_hit",
            Self::TouchPreparedHot => "touch_hit_prepared_hot_90pct_on_1pct",
            Self::Cache95 => "cache_mix_95",
            Self::Cache95Tracked => "cache_mix_95_tracked",
            Self::Cache95PreparedHotTracked => "cache_mix_95_prepared_hot_tracked",
            Self::Cache95PreparedHotPipeline => "cache_mix_95_prepared_hot_pipeline",
            Self::Cache95PreparedHotPipelineBulk64 => "cache_mix_95_prepared_hot_pipeline_bulk_64",
            Self::Cache95Pressure => "cache_mix_95_pressure",
            Self::Cache95HotPressure => "cache_mix_95_hot_pressure",
        }
    }

    fn named(name: &str) -> Option<Self> {
        if name == Self::ReadMixed.name() {
            return Some(Self::ReadMixed);
        }
        Self::ALL
            .into_iter()
            .find(|workload| workload.name() == name)
    }

    const fn is_pressure(self) -> bool {
        matches!(self, Self::Cache95Pressure | Self::Cache95HotPressure)
    }

    const fn supports_sparse_insert_trace(self) -> bool {
        matches!(
            self,
            Self::Cache95
                | Self::Cache95Tracked
                | Self::Cache95PreparedHotTracked
                | Self::Cache95PreparedHotPipeline
                | Self::Cache95PreparedHotPipelineBulk64
        )
    }

    fn pressure_hit_index(self, operation: usize, entries: usize) -> usize {
        if !matches!(self, Self::Cache95HotPressure) || entries < 2 {
            return mixed_index(operation, entries);
        }
        let hot = entries.div_ceil(5);
        if mix(operation as u64 ^ 0x61c8_8646_80b5_83eb).is_multiple_of(5) {
            hot + mixed_index(operation ^ 0x5a5a_3c3c, entries - hot)
        } else {
            mixed_index(operation, hot)
        }
    }

    fn hit_index(self, operation: usize, entries: usize) -> usize {
        let entries = if matches!(self, Self::ReadMixed) {
            entries / 2
        } else {
            entries
        };
        if !matches!(
            self,
            Self::ReadHot
                | Self::ReadPreparedHot
                | Self::ReplacePreparedHot
                | Self::ReplacePreparedHotBatch
                | Self::ReplacePreparedHotBulk64
                | Self::TouchPreparedHot
                | Self::Cache95PreparedHotTracked
                | Self::Cache95PreparedHotPipeline
                | Self::Cache95PreparedHotPipelineBulk64
        ) || entries < 2
        {
            return mixed_index(operation, entries);
        }
        let hot = entries.div_ceil(100).clamp(1, entries - 1);
        if mix(operation as u64 ^ 0x1ae9_523f_184c_7b21).is_multiple_of(10) {
            hot + mixed_index(operation ^ 0x6d5a_56da, entries - hot)
        } else {
            mixed_index(operation, hot)
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let operations = argument(&mut arguments, 1_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 9).max(1);
    let workload_filter = arguments.next();
    let strategy_filter = arguments.next();
    if let Some(bytes) = configured_key_bytes() {
        eprintln!("Cache probe fixed key width: {bytes} bytes");
    }
    if configured_variable_keys() {
        assert!(
            configured_key_bytes().is_none(),
            "PACKED_CACHE_KEY_BYTES and PACKED_CACHE_KEY_SHAPE=variable are mutually exclusive"
        );
        eprintln!("Cache probe key shape: variable Redis-like binary keys");
    }
    if std::env::var_os("PACKED_CACHE_UNBOUNDED").is_some() {
        eprintln!("Packed/Direct capacity mode: unbounded entry count and weight");
    } else if let Ok(value) = std::env::var("PACKED_CACHE_WEIGHT_LIMIT_PER_ENTRY") {
        eprintln!("Packed/Direct weight budget: {value} bytes per maximum entry");
    }
    if std::env::var_os("PACKED_CACHE_REPLACEMENT_BATCH_SIZE").is_some() {
        eprintln!(
            "Direct prepared replacement batch size: {}",
            replacement_batch_size()
        );
    }
    let workloads = workload_filter.map_or_else(
        || Workload::ALL.to_vec(),
        |name| vec![Workload::named(&name).expect("unknown workload name")],
    );
    let key_count = if workloads.len() == 1 && matches!(workloads[0], Workload::ReadMixed) {
        entries.saturating_mul(2)
    } else if workloads.iter().any(|workload| {
        matches!(
            workload,
            Workload::RemoveHit
                | Workload::RemoveHitBatch
                | Workload::RemoveHitBatchUntracked
                | Workload::RemoveHitFrozen
                | Workload::RemoveHitFrozenBatch
                | Workload::RemoveHitFrozenBatchPrepared
                | Workload::RemoveHitFrozenBatchUntracked
        )
    }) {
        entries.max(operations)
    } else {
        entries
    };
    let keys = Arc::new(
        (0..key_count)
            .map(mixed_binary_key)
            .collect::<Vec<Box<[u8]>>>(),
    );
    let cyclic_miss_trace =
        workloads.len() == 1 && matches!(workloads[0], Workload::ReadMiss | Workload::RemoveMiss);
    let insert_count = if cyclic_miss_trace {
        entries
    } else if workloads.iter().any(|workload| {
        !matches!(
            workload,
            Workload::Read
                | Workload::ReadHot
                | Workload::ReadPrepared
                | Workload::ReadPreparedHot
                | Workload::Peek
                | Workload::ReadTracked
                | Workload::ReadMixed
                | Workload::Replace
                | Workload::ReplaceBatch
                | Workload::ReplacePreparedHot
                | Workload::ReplacePreparedHotBatch
                | Workload::ReplacePreparedHotBulk64
                | Workload::InsertExisting
                | Workload::RemoveHit
                | Workload::RemoveHitBatch
                | Workload::RemoveHitBatchUntracked
                | Workload::RemoveHitFrozen
                | Workload::RemoveHitFrozenBatch
                | Workload::RemoveHitFrozenBatchPrepared
                | Workload::RemoveHitFrozenBatchUntracked
                | Workload::Touch
                | Workload::TouchPreparedHot
        )
    }) {
        operations
    } else {
        1
    };
    let inserts = Arc::new(
        if workloads.len() == 1
            && matches!(
                workloads[0],
                Workload::Cache95PreparedHotPipeline | Workload::Cache95PreparedHotPipelineBulk64
            )
        {
            OperationKeys::sparse_pipeline_mix(entries, operations)
        } else if workloads.len() == 1 && workloads[0].supports_sparse_insert_trace() {
            OperationKeys::sparse_mixed(entries, operations)
        } else if cyclic_miss_trace {
            OperationKeys::cyclic(entries, insert_count)
        } else {
            OperationKeys::dense(entries, insert_count)
        },
    );

    println!(
        "strategy,workload,entries,operations,threads,samples,median_mops,p05_mops,p95_time_over_median_pct,median_read_hit_pct,median_final_entries"
    );
    let include_async = std::env::var_os("PACKED_CACHE_INCLUDE_ASYNC").is_some();
    for workload in workloads {
        let mut packed = Vec::with_capacity(samples);
        let mut direct = Vec::with_capacity(samples);
        let mut direct_async = Vec::with_capacity(samples);
        let mut papaya_inline = Vec::with_capacity(samples);
        let mut papaya_arc = Vec::with_capacity(samples);
        for sample in 0..samples {
            let all_strategies = [
                Strategy::Packed,
                Strategy::Direct,
                Strategy::PapayaInline,
                Strategy::PapayaArc,
            ];
            let all_strategies_with_async = [
                Strategy::Packed,
                Strategy::Direct,
                Strategy::DirectAsync,
                Strategy::PapayaInline,
                Strategy::PapayaArc,
            ];
            let pressure_strategies = [
                Strategy::Direct,
                Strategy::DirectAsync,
                Strategy::PapayaInline,
                Strategy::PapayaArc,
            ];
            let strategies = if workload.is_pressure() {
                pressure_strategies.as_slice()
            } else if include_async {
                all_strategies_with_async.as_slice()
            } else {
                all_strategies.as_slice()
            }
            .iter()
            .copied()
            .filter(|strategy| {
                strategy_filter
                    .as_deref()
                    .is_none_or(|filter| filter.split(',').any(|name| strategy.name() == name))
            })
            .collect::<Vec<_>>();
            assert!(!strategies.is_empty(), "unknown strategy filter");
            for offset in 0..strategies.len() {
                let strategy = strategies[(sample + offset) % strategies.len()];
                let measured = measure(
                    strategy,
                    workload,
                    entries,
                    operations,
                    threads,
                    Arc::clone(&keys),
                    Arc::clone(&inserts),
                );
                match strategy {
                    Strategy::Packed => packed.push(measured),
                    Strategy::Direct => direct.push(measured),
                    Strategy::DirectAsync => direct_async.push(measured),
                    Strategy::PapayaInline => papaya_inline.push(measured),
                    Strategy::PapayaArc => papaya_arc.push(measured),
                }
            }
        }
        if !packed.is_empty() {
            print_row(
                Strategy::Packed,
                workload,
                entries,
                operations,
                threads,
                &mut packed,
            );
        }
        if !direct.is_empty() {
            print_row(
                Strategy::Direct,
                workload,
                entries,
                operations,
                threads,
                &mut direct,
            );
        }
        if !direct_async.is_empty() {
            print_row(
                Strategy::DirectAsync,
                workload,
                entries,
                operations,
                threads,
                &mut direct_async,
            );
        }
        if !papaya_inline.is_empty() {
            print_row(
                Strategy::PapayaInline,
                workload,
                entries,
                operations,
                threads,
                &mut papaya_inline,
            );
        }
        if !papaya_arc.is_empty() {
            print_row(
                Strategy::PapayaArc,
                workload,
                entries,
                operations,
                threads,
                &mut papaya_arc,
            );
        }
    }
}

fn measure(
    strategy: Strategy,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: Arc<Vec<Box<[u8]>>>,
    inserts: Arc<OperationKeys>,
) -> Sample {
    let unbounded = std::env::var_os("PACKED_CACHE_UNBOUNDED").is_some();
    let capacity = match workload {
        Workload::Insert
        | Workload::InsertBatch
        | Workload::InsertUntracked
        | Workload::RemoveHit
        | Workload::RemoveHitBatch
        | Workload::RemoveHitBatchUntracked
        | Workload::RemoveHitFrozen
        | Workload::RemoveHitFrozenBatch
        | Workload::RemoveHitFrozenBatchPrepared
        | Workload::RemoveHitFrozenBatchUntracked => operations + 4_096,
        Workload::Cache95
        | Workload::Cache95Tracked
        | Workload::Cache95PreparedHotTracked
        | Workload::Cache95PreparedHotPipeline
        | Workload::Cache95PreparedHotPipelineBulk64 => entries + operations.div_ceil(200),
        Workload::Cache95Pressure | Workload::Cache95HotPressure => {
            entries + operations.div_ceil(100)
        }
        Workload::Read
        | Workload::ReadHot
        | Workload::ReadPrepared
        | Workload::ReadPreparedHot
        | Workload::Peek
        | Workload::ReadTracked
        | Workload::ReadMiss
        | Workload::ReadMixed
        | Workload::Replace
        | Workload::ReplaceBatch
        | Workload::ReplacePreparedHot
        | Workload::ReplacePreparedHotBatch
        | Workload::ReplacePreparedHotBulk64
        | Workload::InsertExisting
        | Workload::RemoveMiss
        | Workload::Touch
        | Workload::TouchPreparedHot => entries,
    };
    let max_entries = if workload.is_pressure() {
        entries
    } else {
        capacity.saturating_add(1)
    };
    let seed = match workload {
        Workload::Insert | Workload::InsertBatch | Workload::InsertUntracked => entries.min(4_096),
        Workload::RemoveHit
        | Workload::RemoveHitBatch
        | Workload::RemoveHitBatchUntracked
        | Workload::RemoveHitFrozen
        | Workload::RemoveHitFrozenBatch
        | Workload::RemoveHitFrozenBatchPrepared
        | Workload::RemoveHitFrozenBatchUntracked => operations,
        Workload::Read
        | Workload::ReadHot
        | Workload::ReadPrepared
        | Workload::ReadPreparedHot
        | Workload::Peek
        | Workload::ReadTracked
        | Workload::ReadMiss
        | Workload::ReadMixed
        | Workload::Replace
        | Workload::ReplaceBatch
        | Workload::ReplacePreparedHot
        | Workload::ReplacePreparedHotBatch
        | Workload::ReplacePreparedHotBulk64
        | Workload::InsertExisting
        | Workload::RemoveMiss
        | Workload::Touch
        | Workload::TouchPreparedHot
        | Workload::Cache95
        | Workload::Cache95Tracked
        | Workload::Cache95PreparedHotTracked
        | Workload::Cache95PreparedHotPipeline
        | Workload::Cache95PreparedHotPipelineBulk64
        | Workload::Cache95Pressure
        | Workload::Cache95HotPressure => entries,
    };
    match strategy {
        Strategy::Packed => {
            let mut config = CacheConfig::new(benchmark_max_weight(max_entries, unbounded))
                .with_overlay_capacity(capacity.max(1));
            if !unbounded {
                config = config.with_max_entries(max_entries);
            }
            let cache = Arc::new(PackedCache::try_new(config).unwrap());
            for (index, key) in keys.iter().take(seed).enumerate() {
                cache
                    .insert_with_options(key, value(index), charge(key), None)
                    .unwrap();
            }
            if !matches!(
                workload,
                Workload::Insert
                    | Workload::InsertBatch
                    | Workload::InsertUntracked
                    | Workload::RemoveHit
                    | Workload::RemoveHitBatch
                    | Workload::RemoveHitBatchUntracked
            ) {
                cache.maintain().unwrap();
            }
            measure_packed(cache, workload, operations, threads, keys, inserts)
        }
        Strategy::Direct | Strategy::DirectAsync => {
            let mut config = CacheConfig::new(benchmark_max_weight(max_entries, unbounded))
                .with_overlay_capacity(capacity.max(1));
            if !unbounded {
                config = config.with_max_entries(max_entries);
            }
            if matches!(strategy, Strategy::DirectAsync) {
                config = config.with_async_eviction(10_100);
            }
            let cache = Arc::new(DirectPackedCache::try_new(config).unwrap());
            for (index, key) in keys.iter().take(seed).enumerate() {
                cache
                    .insert_discard_with_options(key, value(index), charge(key), None)
                    .unwrap();
            }
            if !matches!(
                workload,
                Workload::Insert
                    | Workload::InsertBatch
                    | Workload::InsertUntracked
                    | Workload::RemoveHit
                    | Workload::RemoveHitBatch
                    | Workload::RemoveHitBatchUntracked
            ) {
                cache.maintain().unwrap();
            }
            let maintenance_interval = std::env::var("PACKED_CACHE_MAINTENANCE_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map_or(Duration::from_secs(60), Duration::from_millis);
            let maintenance = matches!(strategy, Strategy::DirectAsync)
                .then(|| cache.spawn_maintenance(maintenance_interval));
            let measured = measure_direct(cache, workload, operations, threads, keys, inserts);
            drop(maintenance);
            measured
        }
        Strategy::PapayaInline => {
            let cache = Arc::new(PapayaInlineCache::with_capacity_and_hasher(
                capacity,
                DefaultHashBuilder::default(),
            ));
            let guard = cache.pin();
            for (index, key) in keys.iter().take(seed).enumerate() {
                guard.insert(key.clone(), ControlValue::new(index, key));
            }
            drop(guard);
            measure_papaya(cache, workload, entries, operations, threads, keys, inserts)
        }
        Strategy::PapayaArc => {
            let cache = Arc::new(PapayaArcCache::with_capacity_and_hasher(
                capacity,
                DefaultHashBuilder::default(),
            ));
            let guard = cache.pin();
            for (index, key) in keys.iter().take(seed).enumerate() {
                guard.insert(key.clone(), Arc::new(ControlValue::new(index, key)));
            }
            drop(guard);
            measure_papaya(cache, workload, entries, operations, threads, keys, inserts)
        }
    }
}

fn measure_packed(
    cache: Arc<PackedCache<[u8; VALUE_BYTES]>>,
    workload: Workload,
    operations: usize,
    threads: usize,
    keys: Arc<Vec<Box<[u8]>>>,
    inserts: Arc<OperationKeys>,
) -> Sample {
    let read_miss_percent = configured_read_miss_percent();
    let refresh_interval = std::env::var("PACKED_CACHE_REFRESH_INTERVAL")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|interval| *interval != 0)
        .unwrap_or(16_384);
    let observed = Arc::clone(&cache);
    let mut sample = run_workers(
        operations,
        threads,
        move |begin, end, checksum, read_hits, read_operations| {
            let cache = Arc::clone(&cache);
            let keys = Arc::clone(&keys);
            let inserts = Arc::clone(&inserts);
            move || {
                let mut guard = cache.pin();
                let mut local = 0_u64;
                let mut local_read_hits = 0_u64;
                let mut local_read_operations = 0_u64;
                for operation in begin..end {
                    if operation.is_multiple_of(refresh_interval) {
                        guard.refresh();
                    }
                    let hit_index = workload.hit_index(operation, keys.len());
                    let hit = &keys[hit_index];
                    match workload {
                        Workload::Read
                        | Workload::ReadHot
                        | Workload::ReadPrepared
                        | Workload::ReadPreparedHot => {
                            local += guard
                                .get_untracked(hit)
                                .map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::Peek => {
                            local += guard.peek(hit).map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::ReadTracked => {
                            local += guard.get(hit).map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::ReadMiss => {
                            local += u64::from(guard.get_untracked(&inserts[operation]).is_some());
                        }
                        Workload::ReadMixed => {
                            local_read_operations += 1;
                            let miss = usize::from(
                                mix(operation as u64 ^ 0x7d18_65a3_f42c_91e7) % 100
                                    < read_miss_percent,
                            );
                            local_read_hits += u64::from(miss == 0);
                            let resident = keys.len() / 2;
                            let miss_index = resident + operation % resident;
                            let selected = &keys[hit_index + miss * (miss_index - hit_index)];
                            local += guard
                                .get_untracked(selected)
                                .map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::Replace
                        | Workload::ReplaceBatch
                        | Workload::ReplacePreparedHot
                        | Workload::ReplacePreparedHotBatch
                        | Workload::ReplacePreparedHotBulk64 => {
                            local += u64::from(
                                cache
                                    .replace_discard_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap(),
                            );
                        }
                        Workload::Insert | Workload::InsertBatch => {
                            let key = &inserts[operation];
                            guard
                                .insert_if_absent_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    None,
                                )
                                .unwrap();
                        }
                        Workload::InsertExisting => {
                            local += u64::from(
                                guard
                                    .insert_if_absent_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap()
                                    == CacheAdmissionOutcome::Existing,
                            );
                        }
                        Workload::InsertUntracked => {
                            let key = &inserts[operation];
                            cache
                                .insert_if_absent_untracked_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    None,
                                )
                                .unwrap();
                        }
                        Workload::RemoveHit
                        | Workload::RemoveHitBatch
                        | Workload::RemoveHitBatchUntracked
                        | Workload::RemoveHitFrozen
                        | Workload::RemoveHitFrozenBatch
                        | Workload::RemoveHitFrozenBatchPrepared
                        | Workload::RemoveHitFrozenBatchUntracked => {
                            local += u64::from(cache.remove_discard(&keys[operation]));
                        }
                        Workload::RemoveMiss => {
                            local += u64::from(cache.remove_discard(&inserts[operation]));
                        }
                        Workload::Touch | Workload::TouchPreparedHot => {
                            local += u64::from(guard.touch(hit, None));
                        }
                        Workload::Cache95 => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                local += guard
                                    .get_untracked(hit)
                                    .map_or(0, |value| u64::from(value[0]));
                            } else if roll <= 193 {
                                local +=
                                    u64::from(guard.get_untracked(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else if roll == 198 {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                cache.remove_discard(hit);
                            }
                        }
                        Workload::Cache95Tracked | Workload::Cache95PreparedHotTracked => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                local += guard.get(hit).map_or(0, |value| u64::from(value[0]));
                            } else if roll <= 193 {
                                local += u64::from(guard.get(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else if roll == 198 {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                cache.remove_discard(hit);
                            }
                        }
                        Workload::Cache95PreparedHotPipeline
                        | Workload::Cache95PreparedHotPipelineBulk64 => {
                            let phase = operation % PIPELINE_MIX_SPAN;
                            if phase < PIPELINE_HIT_READ_END {
                                let found = guard.get(hit);
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                            } else if phase < PIPELINE_MISS_END {
                                let found = guard.get(&inserts[operation]);
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                            } else if phase < PIPELINE_REPLACE_END {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else if phase < PIPELINE_INSERT_END {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                let victim = &keys[pipeline_delete_index(operation, keys.len())];
                                cache.remove_discard(victim);
                            }
                        }
                        Workload::Cache95Pressure | Workload::Cache95HotPressure => {
                            let hit = &keys[workload.pressure_hit_index(operation, keys.len())];
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
                            if roll < 95 {
                                local_read_operations += 1;
                                let found = guard.get_untracked(hit);
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                            } else if roll < 97 {
                                local +=
                                    u64::from(guard.get_untracked(&inserts[operation]).is_some());
                            } else if roll < 99 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
                read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
            }
        },
    );
    sample.final_entries = observed.len();
    if std::env::var_os("PACKED_CACHE_RAW").is_some() {
        sample.rebuilds = observed.stats().rebuilds;
    }
    sample
}

fn measure_direct(
    cache: Arc<DirectPackedCache<[u8; VALUE_BYTES]>>,
    workload: Workload,
    operations: usize,
    threads: usize,
    keys: Arc<Vec<Box<[u8]>>>,
    inserts: Arc<OperationKeys>,
) -> Sample {
    let read_miss_percent = configured_read_miss_percent();
    #[cfg(all(feature = "allocation-diagnostics", not(feature = "jemalloc-probe")))]
    let allocation_region = Region::new(GLOBAL);
    #[cfg(all(feature = "allocation-diagnostics", feature = "jemalloc-probe"))]
    let allocation_region = Region::new(&GLOBAL);
    #[cfg(feature = "prepared-keys")]
    let disable_mix_growth =
        std::env::var_os("PACKED_CACHE_DIAGNOSTIC_DISABLE_MIX_GROWTH").is_some();
    #[cfg(not(feature = "prepared-keys"))]
    let disable_mix_growth = false;
    #[cfg(feature = "prepared-keys")]
    let disable_mix_hit_mutations =
        std::env::var_os("PACKED_CACHE_DIAGNOSTIC_DISABLE_MIX_HIT_MUTATIONS").is_some();
    #[cfg(feature = "prepared-keys")]
    let disable_mix_replacements = disable_mix_hit_mutations
        || std::env::var_os("PACKED_CACHE_DIAGNOSTIC_DISABLE_MIX_REPLACEMENTS").is_some();
    #[cfg(feature = "prepared-keys")]
    let disable_mix_deletes = disable_mix_hit_mutations
        || std::env::var_os("PACKED_CACHE_DIAGNOSTIC_DISABLE_MIX_DELETES").is_some();
    let refresh_interval = std::env::var("PACKED_CACHE_REFRESH_INTERVAL")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|interval| *interval != 0)
        .unwrap_or(16_384);
    #[cfg(feature = "prepared-keys")]
    let replacement_batch_size = replacement_batch_size();
    #[cfg(feature = "prepared-keys")]
    let prepare_removal_inside = std::env::var_os("PACKED_CACHE_PREPARE_REMOVAL_INSIDE").is_some();
    #[cfg(feature = "prepared-keys")]
    let prepared = (std::env::var_os("PACKED_CACHE_DISABLE_PREPARED").is_none()
        && !(prepare_removal_inside && matches!(workload, Workload::RemoveHitFrozenBatchPrepared))
        && matches!(
            workload,
            Workload::ReadPrepared
                | Workload::ReadPreparedHot
                | Workload::ReplacePreparedHot
                | Workload::ReplacePreparedHotBatch
                | Workload::ReplacePreparedHotBulk64
                | Workload::RemoveHitFrozenBatchPrepared
                | Workload::TouchPreparedHot
                | Workload::Cache95PreparedHotTracked
                | Workload::Cache95PreparedHotPipeline
                | Workload::Cache95PreparedHotPipelineBulk64
        ))
    .then(|| {
        let prepared_entries = if matches!(
            workload,
            Workload::ReadPreparedHot
                | Workload::ReplacePreparedHot
                | Workload::ReplacePreparedHotBatch
                | Workload::ReplacePreparedHotBulk64
                | Workload::TouchPreparedHot
                | Workload::Cache95PreparedHotTracked
                | Workload::Cache95PreparedHotPipeline
                | Workload::Cache95PreparedHotPipelineBulk64
        ) {
            if std::env::var_os("PACKED_CACHE_PREPARE_ALL").is_some() {
                keys.len()
            } else {
                keys.len().div_ceil(100)
            }
        } else {
            keys.len()
        };
        Arc::new(
            keys.iter()
                .take(prepared_entries)
                .map(|key| cache.prepare_key(key))
                .collect::<Vec<_>>(),
        )
    });
    #[cfg(not(feature = "prepared-keys"))]
    assert!(
        !matches!(
            workload,
            Workload::ReadPrepared
                | Workload::ReadPreparedHot
                | Workload::ReplacePreparedHot
                | Workload::ReplacePreparedHotBatch
                | Workload::ReplacePreparedHotBulk64
                | Workload::RemoveHitFrozenBatchPrepared
                | Workload::TouchPreparedHot
                | Workload::Cache95PreparedHotTracked
                | Workload::Cache95PreparedHotPipeline
                | Workload::Cache95PreparedHotPipelineBulk64
        ),
        "read_hit_prepared requires the prepared-keys feature"
    );
    #[cfg(feature = "prepared-keys")]
    let prepared_mutations = std::env::var_os("PACKED_CACHE_DISABLE_PREPARED_MUTATIONS").is_none();
    let observed = Arc::clone(&cache);
    let mut sample = run_workers(
        operations,
        threads,
        move |begin, end, checksum, read_hits, read_operations| {
            let cache = Arc::clone(&cache);
            let keys = Arc::clone(&keys);
            let inserts = Arc::clone(&inserts);
            #[cfg(feature = "prepared-keys")]
            let prepared = prepared.clone();
            move || {
                let mut guard = cache.pin();
                let mut local = 0_u64;
                let mut local_read_hits = 0_u64;
                let mut local_read_operations = 0_u64;
                if matches!(workload, Workload::InsertBatch) {
                    for chunk_begin in (begin..end).step_by(32) {
                        if chunk_begin.is_multiple_of(refresh_interval) {
                            guard.refresh();
                        }
                        let mut batch = guard.bulk_admission_batch();
                        let chunk_end = (chunk_begin + 32).min(end);
                        for operation in chunk_begin..chunk_end {
                            let key = &inserts[operation];
                            batch
                                .insert_if_absent_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    None,
                                )
                                .unwrap();
                        }
                    }
                    checksum.fetch_xor(local, Ordering::Relaxed);
                    read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                    read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                    return;
                }
                if matches!(
                    workload,
                    Workload::Cache95PreparedHotPipeline
                        | Workload::Cache95PreparedHotPipelineBulk64
                ) {
                    #[cfg(feature = "prepared-keys")]
                    {
                        let prepared = prepared
                            .as_deref()
                            .expect("prepared pipeline mix owns hot handles");
                        let bulk = matches!(workload, Workload::Cache95PreparedHotPipelineBulk64);
                        let mut pipeline = guard.prepared_replacement_batch();
                        let mut batch_keys = Vec::<&[u8]>::with_capacity(64);
                        let mut batch_prepared = Vec::with_capacity(64);
                        let mut batch_replacements = Vec::with_capacity(64);
                        let mut cold_operations = Vec::with_capacity(8);
                        let mut replaced = [false; 64];
                        let mut operation = begin;
                        while operation < end {
                            let phase = operation % PIPELINE_MIX_SPAN;
                            if phase < PIPELINE_HIT_READ_END {
                                let hit_index = workload.hit_index(operation, keys.len());
                                let hit = &keys[hit_index];
                                let found = prepared.get(hit_index).map_or_else(
                                    || pipeline.get(hit),
                                    |prepared| pipeline.get_prepared(hit, prepared),
                                );
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                                operation += 1;
                                continue;
                            }
                            if phase < PIPELINE_MISS_END {
                                let found = pipeline.get(&inserts[operation]);
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                                operation += 1;
                                continue;
                            }
                            if phase < PIPELINE_REPLACE_END {
                                let count = (PIPELINE_REPLACE_END - phase).min(end - operation);
                                if bulk {
                                    let replacement_end = operation + count;
                                    for batch_begin in
                                        (operation..replacement_end).step_by(replacement_batch_size)
                                    {
                                        batch_keys.clear();
                                        batch_prepared.clear();
                                        batch_replacements.clear();
                                        cold_operations.clear();
                                        let batch_end = (batch_begin + replacement_batch_size)
                                            .min(replacement_end);
                                        for replacement in batch_begin..batch_end {
                                            let hit_index =
                                                workload.hit_index(replacement, keys.len());
                                            let hit = &keys[hit_index];
                                            if let Some(prepared) = prepared.get(hit_index) {
                                                batch_keys.push(hit);
                                                batch_prepared.push(*prepared);
                                                batch_replacements.push((
                                                    value(replacement),
                                                    charge(hit),
                                                    None,
                                                ));
                                            } else {
                                                cold_operations.push(replacement);
                                            }
                                        }
                                        let prepared_count = batch_keys.len();
                                        if prepared_count != 0 {
                                            pipeline
                                                .replace_discard_prepared_batch_with_options(
                                                    &batch_keys,
                                                    &batch_prepared,
                                                    batch_replacements.drain(..),
                                                    &mut replaced[..prepared_count],
                                                )
                                                .unwrap();
                                            local += replaced[..prepared_count]
                                                .iter()
                                                .map(|replaced| u64::from(*replaced))
                                                .sum::<u64>();
                                        }
                                        batch_keys.clear();
                                        batch_replacements.clear();
                                        for replacement in cold_operations.iter().copied() {
                                            let hit =
                                                &keys[workload.hit_index(replacement, keys.len())];
                                            batch_keys.push(hit);
                                            batch_replacements.push((
                                                value(replacement),
                                                charge(hit),
                                                None,
                                            ));
                                        }
                                        let cold_count = batch_keys.len();
                                        if cold_count != 0 {
                                            pipeline
                                                .replace_discard_batch_with_options(
                                                    &batch_keys,
                                                    batch_replacements.drain(..),
                                                    &mut replaced[..cold_count],
                                                )
                                                .unwrap();
                                            local += replaced[..cold_count]
                                                .iter()
                                                .map(|replaced| u64::from(*replaced))
                                                .sum::<u64>();
                                        }
                                    }
                                } else {
                                    for replacement in operation..operation + count {
                                        let hit_index = workload.hit_index(replacement, keys.len());
                                        let hit = &keys[hit_index];
                                        let outcome =
                                            if let Some(prepared) = prepared.get(hit_index) {
                                                pipeline.replace_discard_prepared_with_options(
                                                    hit,
                                                    prepared,
                                                    value(replacement),
                                                    charge(hit),
                                                    None,
                                                )
                                            } else {
                                                pipeline.replace_discard_with_options(
                                                    hit,
                                                    value(replacement),
                                                    charge(hit),
                                                    None,
                                                )
                                            };
                                        local += u64::from(outcome.unwrap());
                                    }
                                }
                                operation += count;
                                continue;
                            }
                            if phase < PIPELINE_INSERT_END {
                                let key = &inserts[operation];
                                pipeline
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                                operation += 1;
                                continue;
                            }

                            let hit_index = pipeline_delete_index(operation, keys.len());
                            let hit = &keys[hit_index];
                            if !prepared.get(hit_index).is_some_and(|prepared| {
                                pipeline.remove_discard_prepared(hit, prepared)
                            }) {
                                let _ = pipeline.remove_discard(hit);
                            }
                            operation += 1;
                        }
                        checksum.fetch_xor(local, Ordering::Relaxed);
                        read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                        read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                        return;
                    }
                    #[cfg(not(feature = "prepared-keys"))]
                    unreachable!("prepared workload was rejected before worker start");
                }
                if matches!(workload, Workload::ReplacePreparedHotBulk64) {
                    #[cfg(feature = "prepared-keys")]
                    {
                        let prepared = prepared
                            .as_deref()
                            .expect("prepared replacement batch owns hot handles");
                        let mut batch = guard.prepared_replacement_batch();
                        let mut batch_keys = Vec::<&[u8]>::with_capacity(64);
                        let mut batch_prepared = Vec::with_capacity(64);
                        let mut batch_replacements = Vec::with_capacity(64);
                        let mut cold_operations = Vec::with_capacity(8);
                        let mut replaced = [false; 64];
                        for chunk_begin in (begin..end).step_by(replacement_batch_size) {
                            batch_keys.clear();
                            batch_prepared.clear();
                            batch_replacements.clear();
                            cold_operations.clear();
                            let chunk_end = (chunk_begin + replacement_batch_size).min(end);
                            for operation in chunk_begin..chunk_end {
                                let hit_index = workload.hit_index(operation, keys.len());
                                let hit = &keys[hit_index];
                                if let Some(prepared) = prepared.get(hit_index) {
                                    batch_keys.push(hit);
                                    batch_prepared.push(*prepared);
                                    batch_replacements.push((value(operation), charge(hit), None));
                                } else {
                                    cold_operations.push(operation);
                                }
                            }
                            let prepared_count = batch_keys.len();
                            if prepared_count != 0 {
                                batch
                                    .replace_discard_prepared_batch_with_options(
                                        &batch_keys,
                                        &batch_prepared,
                                        batch_replacements.drain(..),
                                        &mut replaced[..prepared_count],
                                    )
                                    .unwrap();
                                local += replaced[..prepared_count]
                                    .iter()
                                    .map(|replaced| u64::from(*replaced))
                                    .sum::<u64>();
                            }
                            batch_keys.clear();
                            batch_replacements.clear();
                            for operation in cold_operations.iter().copied() {
                                let hit = &keys[workload.hit_index(operation, keys.len())];
                                batch_keys.push(hit);
                                batch_replacements.push((value(operation), charge(hit), None));
                            }
                            let cold_count = batch_keys.len();
                            if cold_count != 0 {
                                batch
                                    .replace_discard_batch_with_options(
                                        &batch_keys,
                                        batch_replacements.drain(..),
                                        &mut replaced[..cold_count],
                                    )
                                    .unwrap();
                                local += replaced[..cold_count]
                                    .iter()
                                    .map(|replaced| u64::from(*replaced))
                                    .sum::<u64>();
                            }
                        }
                        checksum.fetch_xor(local, Ordering::Relaxed);
                        read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                        read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                        return;
                    }
                    #[cfg(not(feature = "prepared-keys"))]
                    unreachable!("prepared workload was rejected before worker start");
                }
                if matches!(
                    workload,
                    Workload::ReplaceBatch | Workload::ReplacePreparedHotBatch
                ) {
                    #[cfg(feature = "prepared-keys")]
                    {
                        let prepared = prepared.as_deref();
                        let mut batch = guard.replacement_batch();
                        for operation in begin..end {
                            let hit_index = workload.hit_index(operation, keys.len());
                            let hit = &keys[hit_index];
                            let replaced = if let Some(prepared) =
                                prepared.and_then(|prepared| prepared.get(hit_index))
                            {
                                batch
                                    .replace_discard_prepared_with_options(
                                        hit,
                                        prepared,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap()
                            } else {
                                batch
                                    .replace_discard_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap()
                            };
                            local += u64::from(replaced);
                        }
                        checksum.fetch_xor(local, Ordering::Relaxed);
                        read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                        read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                        return;
                    }
                    #[cfg(not(feature = "prepared-keys"))]
                    {
                        debug_assert!(matches!(workload, Workload::ReplaceBatch));
                        let mut batch = guard.replacement_batch();
                        for operation in begin..end {
                            let hit = &keys[workload.hit_index(operation, keys.len())];
                            local += u64::from(
                                batch
                                    .replace_discard_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap(),
                            );
                        }
                        checksum.fetch_xor(local, Ordering::Relaxed);
                        read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                        read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                        return;
                    }
                }
                if matches!(
                    workload,
                    Workload::RemoveHitBatch
                        | Workload::RemoveHitBatchUntracked
                        | Workload::RemoveHitFrozenBatch
                        | Workload::RemoveHitFrozenBatchPrepared
                        | Workload::RemoveHitFrozenBatchUntracked
                ) {
                    if matches!(
                        workload,
                        Workload::RemoveHitBatchUntracked | Workload::RemoveHitFrozenBatchUntracked
                    ) {
                        let mut batch = guard.removal_batch_untracked();
                        for operation in begin..end {
                            local += u64::from(batch.remove_discard(&keys[operation]));
                        }
                    } else if matches!(workload, Workload::RemoveHitFrozenBatchPrepared) {
                        #[cfg(feature = "prepared-keys")]
                        {
                            if prepare_removal_inside {
                                let mut prepared =
                                    vec![packedgen::AtomicPreparedKey::fallback(); end - begin];
                                cache.prepare_key_batch(&keys[begin..end], &mut prepared);
                                let mut batch = guard.removal_batch();
                                for operation in begin..end {
                                    local += u64::from(batch.remove_discard_prepared(
                                        &keys[operation],
                                        &prepared[operation - begin],
                                    ));
                                }
                            } else {
                                let prepared = prepared
                                    .as_ref()
                                    .expect("prepared removal workload builds exact handles");
                                let mut batch = guard.removal_batch();
                                for operation in begin..end {
                                    local += u64::from(batch.remove_discard_prepared(
                                        &keys[operation],
                                        &prepared[operation],
                                    ));
                                }
                            }
                        }
                        #[cfg(not(feature = "prepared-keys"))]
                        unreachable!("prepared workload was rejected before worker start");
                    } else {
                        let mut batch = guard.removal_batch();
                        for operation in begin..end {
                            local += u64::from(batch.remove_discard(&keys[operation]));
                        }
                    }
                    checksum.fetch_xor(local, Ordering::Relaxed);
                    read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                    read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
                    return;
                }
                for operation in begin..end {
                    if operation.is_multiple_of(refresh_interval) {
                        guard.refresh();
                    }
                    let hit_index = workload.hit_index(operation, keys.len());
                    let hit = &keys[hit_index];
                    match workload {
                        Workload::Read | Workload::ReadHot => {
                            local += guard
                                .get_untracked(hit)
                                .map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::ReadPrepared => {
                            #[cfg(feature = "prepared-keys")]
                            {
                                let value = prepared.as_ref().map_or_else(
                                    || guard.get_untracked(hit),
                                    |prepared| {
                                        guard.get_prepared_untracked(hit, &prepared[hit_index])
                                    },
                                );
                                local += value.map_or(0, |value| u64::from(value[0]));
                            }
                            #[cfg(not(feature = "prepared-keys"))]
                            unreachable!("prepared workload was rejected before worker start");
                        }
                        Workload::ReadPreparedHot => {
                            #[cfg(feature = "prepared-keys")]
                            {
                                let value = prepared.as_ref().map_or_else(
                                    || guard.get_untracked(hit),
                                    |prepared| {
                                        if let Some(prepared) = prepared.get(hit_index) {
                                            guard.get_prepared_untracked(hit, prepared)
                                        } else {
                                            guard.get_untracked(hit)
                                        }
                                    },
                                );
                                local += value.map_or(0, |value| u64::from(value[0]));
                            }
                            #[cfg(not(feature = "prepared-keys"))]
                            unreachable!("prepared workload was rejected before worker start");
                        }
                        Workload::Peek => {
                            local += guard.peek(hit).map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::ReadTracked => {
                            local += guard.get(hit).map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::ReadMiss => {
                            local += u64::from(guard.get_untracked(&inserts[operation]).is_some());
                        }
                        Workload::ReadMixed => {
                            local_read_operations += 1;
                            let miss = usize::from(
                                mix(operation as u64 ^ 0x7d18_65a3_f42c_91e7) % 100
                                    < read_miss_percent,
                            );
                            local_read_hits += u64::from(miss == 0);
                            let resident = keys.len() / 2;
                            let miss_index = resident + operation % resident;
                            let selected = &keys[hit_index + miss * (miss_index - hit_index)];
                            local += guard
                                .get_untracked(selected)
                                .map_or(0, |value| u64::from(value[0]));
                        }
                        Workload::Replace => {
                            local += u64::from(
                                cache
                                    .replace_discard_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap(),
                            );
                        }
                        Workload::ReplacePreparedHot => {
                            #[cfg(feature = "prepared-keys")]
                            {
                                let outcome = prepared
                                    .as_ref()
                                    .and_then(|prepared| prepared.get(hit_index))
                                    .map_or_else(
                                        || {
                                            cache.replace_discard_with_options(
                                                hit,
                                                value(operation),
                                                charge(hit),
                                                None,
                                            )
                                        },
                                        |prepared| {
                                            guard.replace_discard_prepared_with_options(
                                                hit,
                                                prepared,
                                                value(operation),
                                                charge(hit),
                                                None,
                                            )
                                        },
                                    );
                                local += u64::from(outcome.unwrap());
                            }
                            #[cfg(not(feature = "prepared-keys"))]
                            unreachable!("prepared workload was rejected before worker start");
                        }
                        Workload::Insert => {
                            let key = &inserts[operation];
                            guard
                                .insert_if_absent_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    None,
                                )
                                .unwrap();
                        }
                        Workload::InsertExisting => {
                            local += u64::from(
                                guard
                                    .insert_if_absent_with_options(
                                        hit,
                                        value(operation),
                                        charge(hit),
                                        None,
                                    )
                                    .unwrap()
                                    == CacheAdmissionOutcome::Existing,
                            );
                        }
                        Workload::InsertBatch => unreachable!(),
                        Workload::InsertUntracked => {
                            let key = &inserts[operation];
                            cache
                                .insert_if_absent_untracked_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    None,
                                )
                                .unwrap();
                        }
                        Workload::RemoveHit | Workload::RemoveHitFrozen => {
                            local += u64::from(guard.remove_discard(&keys[operation]));
                        }
                        Workload::RemoveHitBatch
                        | Workload::RemoveHitBatchUntracked
                        | Workload::RemoveHitFrozenBatch
                        | Workload::RemoveHitFrozenBatchPrepared
                        | Workload::RemoveHitFrozenBatchUntracked => {
                            unreachable!("batched removal uses its dedicated worker loop")
                        }
                        Workload::RemoveMiss => {
                            local += u64::from(guard.remove_discard(&inserts[operation]));
                        }
                        Workload::Touch => {
                            local += u64::from(guard.touch(hit, None));
                        }
                        Workload::TouchPreparedHot => {
                            #[cfg(feature = "prepared-keys")]
                            {
                                local += u64::from(
                                    prepared
                                        .as_ref()
                                        .and_then(|prepared| prepared.get(hit_index))
                                        .map_or_else(
                                            || guard.touch(hit, None),
                                            |prepared| guard.touch_prepared(hit, prepared, None),
                                        ),
                                );
                            }
                            #[cfg(not(feature = "prepared-keys"))]
                            unreachable!("prepared workload was rejected before worker start");
                        }
                        Workload::Cache95 => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                local += guard
                                    .get_untracked(hit)
                                    .map_or(0, |value| u64::from(value[0]));
                            } else if roll <= 193 {
                                local +=
                                    u64::from(guard.get_untracked(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else if roll == 198 {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                guard.remove_discard(hit);
                            }
                        }
                        Workload::ReplaceBatch
                        | Workload::ReplacePreparedHotBatch
                        | Workload::ReplacePreparedHotBulk64 => {
                            unreachable!("batched replacement uses its dedicated worker loop")
                        }
                        Workload::Cache95Tracked => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                local += guard.get(hit).map_or(0, |value| u64::from(value[0]));
                            } else if roll <= 193 {
                                local += u64::from(guard.get(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else if roll == 198 {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                guard.remove_discard(hit);
                            }
                        }
                        Workload::Cache95PreparedHotTracked => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                #[cfg(feature = "prepared-keys")]
                                {
                                    let value = prepared.as_ref().map_or_else(
                                        || guard.get(hit),
                                        |prepared| {
                                            if let Some(prepared) = prepared.get(hit_index) {
                                                guard.get_prepared(hit, prepared)
                                            } else {
                                                guard.get(hit)
                                            }
                                        },
                                    );
                                    local += value.map_or(0, |value| u64::from(value[0]));
                                }
                                #[cfg(not(feature = "prepared-keys"))]
                                unreachable!("prepared workload was rejected before worker start");
                            } else if roll <= 193 {
                                local += u64::from(guard.get(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                #[cfg(feature = "prepared-keys")]
                                {
                                    if disable_mix_replacements {
                                        continue;
                                    }
                                    let outcome = if prepared_mutations {
                                        prepared.as_ref().and_then(|prepared| {
                                            prepared.get(hit_index).map(|prepared| {
                                                guard.replace_discard_prepared_with_options(
                                                    hit,
                                                    prepared,
                                                    value(operation),
                                                    charge(hit),
                                                    None,
                                                )
                                            })
                                        })
                                    } else {
                                        None
                                    }
                                    .unwrap_or_else(|| {
                                        cache.replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                    });
                                    local += u64::from(outcome.unwrap());
                                }
                                #[cfg(not(feature = "prepared-keys"))]
                                unreachable!("prepared workload was rejected before worker start");
                            } else if roll == 198 {
                                if disable_mix_growth {
                                    continue;
                                }
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            } else {
                                #[cfg(feature = "prepared-keys")]
                                {
                                    if disable_mix_deletes {
                                        continue;
                                    }
                                    if !prepared_mutations
                                        || !prepared.as_ref().is_some_and(|prepared| {
                                            prepared.get(hit_index).is_some_and(|prepared| {
                                                guard.remove_discard_prepared(hit, prepared)
                                            })
                                        })
                                    {
                                        guard.remove_discard(hit);
                                    }
                                }
                                #[cfg(not(feature = "prepared-keys"))]
                                unreachable!("prepared workload was rejected before worker start");
                            }
                        }
                        Workload::Cache95PreparedHotPipeline
                        | Workload::Cache95PreparedHotPipelineBulk64 => {
                            unreachable!("pipeline mix uses its dedicated worker loop")
                        }
                        Workload::Cache95Pressure | Workload::Cache95HotPressure => {
                            let hit = &keys[workload.pressure_hit_index(operation, keys.len())];
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
                            if roll < 95 {
                                local_read_operations += 1;
                                let found = guard.get_untracked(hit);
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |value| u64::from(value[0]));
                            } else if roll < 97 {
                                local +=
                                    u64::from(guard.get_untracked(&inserts[operation]).is_some());
                            } else if roll < 99 {
                                local += u64::from(
                                    cache
                                        .replace_discard_with_options(
                                            hit,
                                            value(operation),
                                            charge(hit),
                                            None,
                                        )
                                        .unwrap(),
                                );
                            } else {
                                let key = &inserts[operation];
                                guard
                                    .insert_if_absent_with_options(
                                        key,
                                        value(operation),
                                        charge(key),
                                        None,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
                read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
            }
        },
    );
    sample.final_entries = observed.len();
    if std::env::var_os("PACKED_CACHE_RAW").is_some() {
        sample.rebuilds = observed.stats().rebuilds;
    }
    #[cfg(feature = "allocation-diagnostics")]
    if std::env::var_os("PACKED_CACHE_ALLOCATION_STATS").is_some() {
        let allocations = allocation_region.change();
        eprintln!(
            "allocations={},deallocations={},reallocations={},bytes_allocated={},bytes_deallocated={},bytes_reallocated={},net_live_bytes={}",
            allocations.allocations,
            allocations.deallocations,
            allocations.reallocations,
            allocations.bytes_allocated,
            allocations.bytes_deallocated,
            allocations.bytes_reallocated,
            allocations
                .bytes_allocated
                .saturating_sub(allocations.bytes_deallocated)
        );
    }
    #[cfg(feature = "cache-diagnostics")]
    if std::env::var_os("PACKED_CACHE_RECLAMATION_STATS").is_some() {
        eprintln!("reclamation={:?}", observed.reclamation_stats());
    }
    sample
}

fn measure_papaya<C: PapayaControl + 'static>(
    cache: Arc<PapayaHashMap<Box<[u8]>, C, DefaultHashBuilder>>,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    keys: Arc<Vec<Box<[u8]>>>,
    inserts: Arc<OperationKeys>,
) -> Sample {
    let read_miss_percent = configured_read_miss_percent();
    let resident_keys = workload.is_pressure().then(|| {
        Arc::new(Mutex::new(
            keys.iter().take(entries).cloned().collect::<Vec<_>>(),
        ))
    });
    let observed = Arc::clone(&cache);
    let mut sample = run_workers(
        operations,
        threads,
        move |begin, end, checksum, read_hits, read_operations| {
            let cache = Arc::clone(&cache);
            let keys = Arc::clone(&keys);
            let inserts = Arc::clone(&inserts);
            let resident_keys = resident_keys.clone();
            move || {
                let guard = cache.pin();
                let mut local = 0_u64;
                let mut local_read_hits = 0_u64;
                let mut local_read_operations = 0_u64;
                for operation in begin..end {
                    let hit_index = workload.hit_index(operation, keys.len());
                    let hit = &keys[hit_index];
                    match workload {
                        Workload::Read
                        | Workload::ReadHot
                        | Workload::ReadPrepared
                        | Workload::ReadPreparedHot
                        | Workload::ReadTracked => {
                            local += guard.get(hit).map_or(0, |entry| entry.read(operation));
                        }
                        Workload::Peek => {
                            local += guard.get(hit).map_or(0, PapayaControl::first_byte);
                        }
                        Workload::ReadMiss => {
                            local += u64::from(guard.get(&inserts[operation]).is_some());
                        }
                        Workload::ReadMixed => {
                            local_read_operations += 1;
                            let miss = usize::from(
                                mix(operation as u64 ^ 0x7d18_65a3_f42c_91e7) % 100
                                    < read_miss_percent,
                            );
                            local_read_hits += u64::from(miss == 0);
                            let resident = keys.len() / 2;
                            let miss_index = resident + operation % resident;
                            let selected = &keys[hit_index + miss * (miss_index - hit_index)];
                            local += guard.get(selected).map_or(0, |entry| entry.read(operation));
                        }
                        Workload::Replace
                        | Workload::ReplaceBatch
                        | Workload::ReplacePreparedHot
                        | Workload::ReplacePreparedHotBatch
                        | Workload::ReplacePreparedHotBulk64 => {
                            local += u64::from(
                                guard
                                    .update(hit.clone(), |_| C::new(operation, hit))
                                    .is_some(),
                            );
                        }
                        Workload::Insert | Workload::InsertBatch | Workload::InsertUntracked => {
                            let key = &inserts[operation];
                            guard.insert(key.clone(), C::new(operation, key));
                        }
                        Workload::InsertExisting => {
                            local += u64::from(
                                guard
                                    .try_insert(hit.clone(), C::new(operation, hit))
                                    .is_err(),
                            );
                        }
                        Workload::RemoveHit
                        | Workload::RemoveHitBatch
                        | Workload::RemoveHitBatchUntracked
                        | Workload::RemoveHitFrozen
                        | Workload::RemoveHitFrozenBatch
                        | Workload::RemoveHitFrozenBatchPrepared
                        | Workload::RemoveHitFrozenBatchUntracked => {
                            local += u64::from(guard.remove(&keys[operation]).is_some());
                        }
                        Workload::RemoveMiss => {
                            local += u64::from(guard.remove(&inserts[operation]).is_some());
                        }
                        Workload::Touch | Workload::TouchPreparedHot => {
                            local += guard.get(hit).map_or(0, PapayaControl::touch);
                        }
                        Workload::Cache95
                        | Workload::Cache95Tracked
                        | Workload::Cache95PreparedHotTracked => {
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 200;
                            if roll <= 189 {
                                local += guard.get(hit).map_or(0, |entry| entry.read(operation));
                            } else if roll <= 193 {
                                local += u64::from(guard.get(&inserts[operation]).is_some());
                            } else if roll <= 197 {
                                local += u64::from(
                                    guard
                                        .update(hit.clone(), |_| C::new(operation, hit))
                                        .is_some(),
                                );
                            } else if roll == 198 {
                                let key = &inserts[operation];
                                guard.insert(key.clone(), C::new(operation, key));
                            } else {
                                guard.remove(hit);
                            }
                        }
                        Workload::Cache95PreparedHotPipeline
                        | Workload::Cache95PreparedHotPipelineBulk64 => {
                            let phase = operation % PIPELINE_MIX_SPAN;
                            if phase < PIPELINE_HIT_READ_END {
                                let found = guard.get(hit);
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |entry| entry.read(operation));
                            } else if phase < PIPELINE_MISS_END {
                                let found = guard.get(&inserts[operation]);
                                local_read_operations += 1;
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |entry| entry.read(operation));
                            } else if phase < PIPELINE_REPLACE_END {
                                local += u64::from(
                                    guard
                                        .update(hit.clone(), |_| C::new(operation, hit))
                                        .is_some(),
                                );
                            } else if phase < PIPELINE_INSERT_END {
                                let key = &inserts[operation];
                                guard.insert(key.clone(), C::new(operation, key));
                            } else {
                                let victim = &keys[pipeline_delete_index(operation, keys.len())];
                                guard.remove(victim);
                            }
                        }
                        Workload::Cache95Pressure | Workload::Cache95HotPressure => {
                            let hit = &keys[workload.pressure_hit_index(operation, keys.len())];
                            let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
                            if roll < 95 {
                                local_read_operations += 1;
                                let found = guard.get(hit);
                                local_read_hits += u64::from(found.is_some());
                                local += found.map_or(0, |entry| entry.read(operation));
                            } else if roll < 97 {
                                local += u64::from(guard.get(&inserts[operation]).is_some());
                            } else if roll < 99 {
                                local += u64::from(
                                    guard
                                        .update(hit.clone(), |_| C::new(operation, hit))
                                        .is_some(),
                                );
                            } else {
                                let key = &inserts[operation];
                                let victim = {
                                    let mut resident = resident_keys
                                        .as_ref()
                                        .expect("pressure workload owns resident keys")
                                        .lock();
                                    let slot = mixed_index(operation, resident.len());
                                    std::mem::replace(&mut resident[slot], key.clone())
                                };
                                guard.remove(&victim);
                                guard.insert(key.clone(), C::new(operation, key));
                            }
                        }
                    }
                }
                checksum.fetch_xor(local, Ordering::Relaxed);
                read_hits.fetch_add(local_read_hits, Ordering::Relaxed);
                read_operations.fetch_add(local_read_operations, Ordering::Relaxed);
            }
        },
    );
    sample.final_entries = observed.len();
    sample
}

fn run_workers<F, W>(operations: usize, threads: usize, worker: F) -> Sample
where
    F: Fn(usize, usize, Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) -> W,
    W: FnOnce() + Send,
{
    let barrier = Arc::new(Barrier::new(threads + 1));
    let checksum = Arc::new(AtomicU64::new(0));
    let read_hits = Arc::new(AtomicU64::new(0));
    let read_operations = Arc::new(AtomicU64::new(0));
    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            let barrier = Arc::clone(&barrier);
            let work = worker(
                begin,
                end,
                Arc::clone(&checksum),
                Arc::clone(&read_hits),
                Arc::clone(&read_operations),
            );
            workers.push(scope.spawn(move || {
                barrier.wait();
                work();
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        black_box(checksum.load(Ordering::Relaxed));
        Sample {
            mops: operations as f64 / started.elapsed().as_secs_f64() / 1e6,
            read_hits: read_hits.load(Ordering::Relaxed),
            read_operations: read_operations.load(Ordering::Relaxed),
            final_entries: 0,
            rebuilds: 0,
        }
    })
}

fn print_row(
    strategy: Strategy,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    values: &mut [Sample],
) {
    values.sort_by(|left, right| f64::total_cmp(&left.mops, &right.mops));
    if std::env::var_os("PACKED_CACHE_RAW").is_some() {
        let rebuilds = values
            .iter()
            .map(|sample| sample.rebuilds)
            .collect::<Vec<_>>();
        eprintln!(
            "raw,{},{},rebuilds={rebuilds:?},{values:?}",
            strategy.name(),
            workload.name()
        );
    }
    let median = values[values.len() / 2].mops;
    let p05 = values[values.len() * 5 / 100].mops;
    let mut hit_rates = values
        .iter()
        .filter(|sample| sample.read_operations != 0)
        .map(|sample| sample.read_hits as f64 / sample.read_operations as f64 * 100.0)
        .collect::<Vec<_>>();
    hit_rates.sort_by(f64::total_cmp);
    let hit_rate = hit_rates
        .get(hit_rates.len() / 2)
        .map_or_else(|| "na".to_owned(), |rate| format!("{rate:.3}"));
    let mut final_entries = values
        .iter()
        .map(|sample| sample.final_entries)
        .collect::<Vec<_>>();
    final_entries.sort_unstable();
    let median_final_entries = final_entries[final_entries.len() / 2];
    println!(
        "{},{},{entries},{operations},{threads},{},{median:.3},{p05:.3},{:.3},{hit_rate},{median_final_entries}",
        strategy.name(),
        workload.name(),
        values.len(),
        (median / p05 - 1.0) * 100.0
    );
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn mixed_index(operation: usize, len: usize) -> usize {
    let len = u64::try_from(len).expect("benchmark key count fits u64");
    usize::try_from(mix(operation as u64) % len).expect("index is below the original usize length")
}

fn pipeline_delete_index(operation: usize, len: usize) -> usize {
    mixed_index(operation ^ 0x4d59_5df4, len)
}

fn replacement_batch_size() -> usize {
    std::env::var("PACKED_CACHE_REPLACEMENT_BATCH_SIZE").map_or(64, |value| {
        let size = value
            .parse::<usize>()
            .expect("PACKED_CACHE_REPLACEMENT_BATCH_SIZE must be an integer");
        assert!(
            (1..=64).contains(&size),
            "PACKED_CACHE_REPLACEMENT_BATCH_SIZE must be between 1 and 64"
        );
        size
    })
}

trait PapayaControl: Send + Sync + Sized {
    fn new(index: usize, key: &[u8]) -> Self;
    fn inner(&self) -> &ControlValue;

    fn first_byte(&self) -> u64 {
        u64::from(self.inner().value[0])
    }

    fn read(&self, _access: usize) -> u64 {
        let entry = self.inner();
        entry.accessed.store(true, Ordering::Relaxed);
        black_box(entry.weight);
        black_box(entry.expires_at.load(Ordering::Relaxed));
        u64::from(entry.value[0])
    }

    fn touch(&self) -> u64 {
        self.inner().expires_at.store(u32::MAX, Ordering::Relaxed);
        1
    }
}

impl ControlValue {
    fn new(index: usize, key: &[u8]) -> Self {
        Self {
            value: value(index),
            weight: u32::try_from(charge(key)).unwrap(),
            expires_at: AtomicU32::new(u32::MAX),
            accessed: AtomicBool::new(index.is_multiple_of(16)),
        }
    }
}

impl PapayaControl for ControlValue {
    fn new(index: usize, key: &[u8]) -> Self {
        Self::new(index, key)
    }

    fn inner(&self) -> &ControlValue {
        self
    }
}

impl PapayaControl for Arc<ControlValue> {
    fn new(index: usize, key: &[u8]) -> Self {
        Arc::new(ControlValue::new(index, key))
    }

    fn inner(&self) -> &ControlValue {
        self
    }
}

fn charge(key: &[u8]) -> u64 {
    VALUE_BYTES as u64 + key.len() as u64
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}

fn configured_read_miss_percent() -> u64 {
    std::env::var("PACKED_CACHE_READ_MISS_PERCENT").map_or(20, |value| {
        let percent = value
            .parse::<u64>()
            .expect("PACKED_CACHE_READ_MISS_PERCENT must be an integer from 0 to 100");
        assert!(
            percent <= 100,
            "PACKED_CACHE_READ_MISS_PERCENT must be at most 100"
        );
        percent
    })
}

fn benchmark_max_weight(max_entries: usize, unbounded: bool) -> u64 {
    if unbounded {
        return u64::MAX;
    }
    std::env::var("PACKED_CACHE_WEIGHT_LIMIT_PER_ENTRY").map_or(u64::MAX, |value| {
        let per_entry = value
            .parse::<u64>()
            .expect("PACKED_CACHE_WEIGHT_LIMIT_PER_ENTRY must be a positive integer");
        assert_ne!(
            per_entry, 0,
            "PACKED_CACHE_WEIGHT_LIMIT_PER_ENTRY must be positive"
        );
        u64::try_from(max_entries)
            .unwrap_or(u64::MAX)
            .saturating_mul(per_entry)
    })
}

fn mixed_binary_key(index: usize) -> Box<[u8]> {
    let mut key = [0_u8; 64];
    let mut state = index as u64;
    for chunk in key.as_chunks_mut::<8>().0 {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    let bytes = configured_key_bytes().unwrap_or_else(|| {
        if configured_variable_keys() {
            match index % 100 {
                0..=39 => 7,
                40..=64 => 13,
                65..=79 => 21,
                80..=89 => 29,
                90..=94 => 37,
                95..=97 => 48,
                _ => 61,
            }
        } else {
            match index % 100 {
                0..=39 => 8,
                40..=64 => 16,
                65..=79 => 24,
                80..=89 => 32,
                _ => 48,
            }
        }
    });
    key[..bytes].into()
}

fn configured_key_bytes() -> Option<usize> {
    static KEY_BYTES: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *KEY_BYTES.get_or_init(|| {
        std::env::var("PACKED_CACHE_KEY_BYTES").ok().map(|value| {
            let bytes = value
                .parse::<usize>()
                .expect("PACKED_CACHE_KEY_BYTES must be an integer from 1 through 64");
            assert!(
                (1..=64).contains(&bytes),
                "PACKED_CACHE_KEY_BYTES must be from 1 through 64"
            );
            bytes
        })
    })
}

fn configured_variable_keys() -> bool {
    static VARIABLE_KEYS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VARIABLE_KEYS.get_or_init(|| {
        std::env::var("PACKED_CACHE_KEY_SHAPE").is_ok_and(|value| match value.as_str() {
            "boundary" => false,
            "variable" => true,
            _ => panic!("PACKED_CACHE_KEY_SHAPE must be boundary or variable"),
        })
    })
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
