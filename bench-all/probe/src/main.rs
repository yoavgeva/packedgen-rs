//! Fresh-process bounded-cache comparison with explicit payload ownership.
//!
//! Usage: `bounded_cache_probe BACKEND WORKLOAD ENTRIES OPS WORKERS VALUE_BYTES
//! SEED [LATENCY_STRIDE]`. BACKEND is `direct-owned`, `direct-borrowed`,
//! `direct-guarded-owned`, `direct-guarded-borrowed`, `direct-background`,
//! `sharded`, `sharded-batch`, or `quickcache`. `sharded-batch` writes through
//! `insert_many`, 64 writes at a time, flushing before every read or removal. The direct backends run full maintenance on each worker every
//! 4096 writes, inside the timing; `direct-background` instead starts the
//! cache's maintenance thread (a 10 ms interval) and reads owned values.
//! The guarded insertion variants require `cold-fill` (unique new keys).
//! WORKLOAD is `resident-hit`, `read-miss`, `cold-fill`,
//! `turnover`, `replace`, or `delete-reinsert`. `LATENCY_STRIDE` defaults to zero
//! (uninstrumented throughput); otherwise every stride-th operation is timed,
//! and the row ends with their p99, p99.9, p99.99, p99.999, and maximum
//! latencies. Each invocation runs exactly one cache.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{
    CacheAdmissionOutcome, CacheConfig, CacheWriteOutcome, DirectCacheGuard, DirectPackedCache,
    ShardedCache,
};
use quick_cache::{Weighter, sync::Cache};

type Key = [u8; 32];
type Value = Arc<[u8]>;
type QuickCache = Cache<Key, Value, PayloadWeighter>;
const REFRESH_INTERVAL: usize = 512;

#[derive(Clone)]
struct PayloadWeighter;

impl Weighter<Key, Value> for PayloadWeighter {
    fn weight(&self, _key: &Key, value: &Value) -> u64 {
        u64::try_from(32 + value.len()).expect("payload charge fits u64")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    ResidentHit,
    ReadMiss,
    ColdFill,
    Turnover,
    Replace,
    DeleteReinsert,
}

impl Workload {
    fn parse(value: &str) -> Self {
        match value {
            "resident-hit" => Self::ResidentHit,
            "read-miss" => Self::ReadMiss,
            "cold-fill" => Self::ColdFill,
            "turnover" => Self::Turnover,
            "replace" => Self::Replace,
            "delete-reinsert" => Self::DeleteReinsert,
            _ => panic!("unknown workload: {value}"),
        }
    }

    fn is_resident(self) -> bool {
        matches!(
            self,
            Self::ResidentHit | Self::ReadMiss | Self::Replace | Self::DeleteReinsert
        )
    }
}

#[derive(Clone, Debug)]
struct Config {
    workload: Workload,
    entries: usize,
    operations: usize,
    workers: usize,
    value_bytes: usize,
    seed: u64,
    latency_stride: usize,
}

impl Config {
    fn charge(&self) -> u64 {
        u64::try_from(32 + self.value_bytes).expect("charge fits u64")
    }

    fn capacity_entries(&self) -> usize {
        // Resident controls need spare capacity for QuickCache's randomized
        // per-shard imbalance. A 2x budget lost two seeds in the first long
        // run; 4x provides more room, and the full-seed assertion still rejects
        // any unsuitable hash draw. Pressure tests use the exact budget.
        if self.workload.is_resident() {
            self.entries
                .checked_mul(4)
                .expect("resident capacity fits usize")
        } else {
            self.entries
        }
    }

    fn weight_limit(&self) -> u64 {
        u64::try_from(self.capacity_entries())
            .expect("entry capacity fits u64")
            .checked_mul(self.charge())
            .expect("weight capacity fits u64")
    }

    fn validate(&self) {
        assert!(
            self.entries >= 128 && self.operations > 0 && self.workers > 0,
            "use at least 128 entries so QuickCache's per-shard hot target can hold a payload"
        );
        assert!(self.workers <= self.entries && self.workers <= self.operations);
        assert!(self.value_bytes >= 16 && u32::try_from(self.charge()).is_ok());
        assert!(
            self.latency_stride == 0 || self.latency_stride % 2 == 1,
            "latency stride must be odd to cover every 512-operation refresh phase"
        );
        let _ = self.weight_limit();
        self.entries
            .checked_mul(2)
            .and_then(|entries| entries.checked_add(self.operations))
            .expect("trace key ids fit usize");
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Read,
    Insert,
    Replace,
    DeleteReinsert,
}

#[derive(Clone, Copy, Debug)]
struct Operation {
    key: Key,
    action: Action,
    version: u64,
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key(id: u64) -> Key {
    let mut result = [0; 32];
    result[..8].copy_from_slice(&id.to_le_bytes());
    for lane in 1..4 {
        let word = mix(id.wrapping_add(lane as u64));
        result[lane * 8..(lane + 1) * 8].copy_from_slice(&word.to_le_bytes());
    }
    result
}

fn payload(key: &Key, bytes: usize, version: u64) -> Value {
    // Identical materialization for both caches, included in write timings.
    let mut value = vec![0; bytes];
    value[..8].copy_from_slice(&key[..8]);
    value[8..16].copy_from_slice(&version.to_le_bytes());
    value.into()
}

fn worker_trace(config: &Config, worker: usize) -> Vec<Operation> {
    let (begin, end) = worker_range(config.operations, config.workers, worker);
    let (resident_begin, resident_end) = worker_range(config.entries, config.workers, worker);
    (begin..end)
        .map(|operation| {
            let random = mix(config.seed ^ operation as u64);
            let resident = resident_begin
                + usize::try_from(random % (resident_end - resident_begin) as u64)
                    .expect("resident id fits usize");
            let (id, action) = match config.workload {
                Workload::ResidentHit => (resident, Action::Read),
                Workload::ReadMiss => (config.entries + operation, Action::Read),
                Workload::ColdFill => (config.entries + operation, Action::Insert),
                Workload::Turnover if random % 100 < 95 => {
                    // 80% of reads target a stable hot tenth; the rest cover a
                    // working set twice the soft capacity. Policy remains visible.
                    let id = if random.rotate_left(17) % 100 < 80 {
                        usize::try_from(mix(random) % config.entries.div_ceil(10) as u64)
                            .expect("hot id fits usize")
                    } else {
                        usize::try_from(mix(random) % (2 * config.entries) as u64)
                            .expect("working-set id fits usize")
                    };
                    (id, Action::Read)
                }
                Workload::Turnover => (2 * config.entries + operation, Action::Insert),
                Workload::Replace => (resident, Action::Replace),
                Workload::DeleteReinsert => (resident, Action::DeleteReinsert),
            };
            Operation {
                key: key(id as u64),
                action,
                version: operation as u64 + 1,
            }
        })
        .collect()
}

fn worker_range(total: usize, workers: usize, worker: usize) -> (usize, usize) {
    let per_worker = total / workers;
    let remainder = total % workers;
    let begin = per_worker * worker + worker.min(remainder);
    (begin, begin + per_worker + usize::from(worker < remainder))
}

#[derive(Default)]
struct Sample {
    reads: u64,
    hits: u64,
    inserts: u64,
    replacements: u64,
    removed: u64,
    writes: u64,
    checksum: u64,
    latencies: Vec<Duration>,
}

impl Sample {
    fn observe(&mut self, value: Option<&Value>, expected_key: &Key) {
        self.reads += 1;
        if let Some(value) = value {
            // Check the key/value association in each returned value, rather
            // than accepting throughput from an incorrect cache adapter.
            assert_eq!(&value[..8], &expected_key[..8]);
            self.hits += 1;
            self.checksum = self.checksum.wrapping_add(u64::from(value[8]));
            black_box(value);
        }
    }

    fn merge(&mut self, other: Self) {
        self.reads += other.reads;
        self.hits += other.hits;
        self.inserts += other.inserts;
        self.replacements += other.replacements;
        self.removed += other.removed;
        self.writes += other.writes;
        self.checksum = self.checksum.wrapping_add(other.checksum);
        self.latencies.extend(other.latencies);
    }
}

trait Session {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample);
    fn quiesce(&mut self) {}
}

struct DirectSession<'a, const OWNED: bool, const GUARDED: bool> {
    cache: &'a DirectPackedCache<Value>,
    guard: DirectCacheGuard<'a, Value>,
    writes_since_maintenance: usize,
    inline_maintenance: bool,
}

impl<const OWNED: bool, const GUARDED: bool> DirectSession<'_, OWNED, GUARDED> {
    fn publish(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        let value = payload(&operation.key, config.value_bytes, operation.version);
        sample.writes += 1;
        if matches!(operation.action, Action::Replace) {
            let replaced = self
                .cache
                .replace_discard_with_options(&operation.key, value, config.charge(), None)
                .expect("valid payload charge");
            sample.replacements += u64::from(replaced);
        } else if GUARDED {
            // Cold-fill traces contain unique keys, so conditional publication
            // has exactly the same semantics as upsert here. Do not extend
            // this experiment to racing turnover writes without a new control.
            let outcome = self
                .guard
                .insert_if_absent_with_options(&operation.key, value, config.charge(), None)
                .expect("valid payload charge");
            if outcome == CacheAdmissionOutcome::Inserted {
                sample.inserts += 1;
            }
        } else {
            let outcome = self
                .cache
                .insert_discard_with_options(&operation.key, value, config.charge(), None)
                .expect("valid payload charge");
            match outcome {
                CacheWriteOutcome::Inserted => sample.inserts += 1,
                CacheWriteOutcome::Replaced => sample.replacements += 1,
            }
        }
        self.writes_since_maintenance += 1;
        if self.inline_maintenance && self.writes_since_maintenance == 4096 {
            self.guard.quiesce();
            self.cache
                .maintain()
                .expect("periodic maintenance succeeds");
            self.writes_since_maintenance = 0;
        }
    }
}

impl<const OWNED: bool, const GUARDED: bool> Session for DirectSession<'_, OWNED, GUARDED> {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        match operation.action {
            Action::Read => {
                let hit = if OWNED {
                    let value = self.guard.get(&operation.key).cloned();
                    sample.observe(value.as_ref(), &operation.key);
                    value.is_some()
                } else {
                    let value = self.guard.get(&operation.key);
                    sample.observe(value, &operation.key);
                    value.is_some()
                };
                if !hit && config.workload == Workload::Turnover {
                    self.publish(operation, config, sample);
                }
            }
            Action::Insert | Action::Replace | Action::DeleteReinsert => {
                if matches!(operation.action, Action::DeleteReinsert) {
                    sample.removed += u64::from(self.cache.remove_discard(&operation.key));
                }
                self.publish(operation, config, sample);
            }
        }
    }

    fn quiesce(&mut self) {
        self.guard.quiesce();
    }
}

struct ShardedSession<'a>(&'a ShardedCache<Key, Value>);

impl Session for ShardedSession<'_> {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        match operation.action {
            Action::Read => {
                let value = self.0.get(&operation.key);
                sample.observe(value.as_ref(), &operation.key);
                if value.is_none() && config.workload == Workload::Turnover {
                    self.0.insert(
                        operation.key,
                        payload(&operation.key, config.value_bytes, operation.version),
                    );
                    sample.writes += 1;
                }
            }
            Action::Insert | Action::Replace | Action::DeleteReinsert => {
                if matches!(operation.action, Action::DeleteReinsert) {
                    sample.removed += u64::from(self.0.remove(&operation.key));
                }
                let value = payload(&operation.key, config.value_bytes, operation.version);
                sample.writes += 1;
                self.0.insert(operation.key, value);
                if matches!(operation.action, Action::Replace) {
                    sample.replacements += 1;
                } else {
                    sample.inserts += 1;
                }
            }
        }
    }
}

/// Writes per `insert_many` batch in the `sharded-batch` backend.
const WRITE_BATCH: usize = 64;

struct ShardedBatchSession<'a> {
    cache: &'a ShardedCache<Key, Value>,
    pending: Vec<(Key, Value)>,
}

impl ShardedBatchSession<'_> {
    fn flush(&mut self) {
        if !self.pending.is_empty() {
            self.cache.insert_many(self.pending.drain(..));
        }
    }
}

impl Session for ShardedBatchSession<'_> {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        match operation.action {
            Action::Read => {
                self.flush();
                let value = self.cache.get(&operation.key);
                sample.observe(value.as_ref(), &operation.key);
                if value.is_none() && config.workload == Workload::Turnover {
                    self.pending.push((
                        operation.key,
                        payload(&operation.key, config.value_bytes, operation.version),
                    ));
                    sample.writes += 1;
                }
            }
            Action::Insert | Action::Replace | Action::DeleteReinsert => {
                if matches!(operation.action, Action::DeleteReinsert) {
                    self.flush();
                    sample.removed += u64::from(self.cache.remove(&operation.key));
                }
                let value = payload(&operation.key, config.value_bytes, operation.version);
                sample.writes += 1;
                self.pending.push((operation.key, value));
                if matches!(operation.action, Action::Replace) {
                    sample.replacements += 1;
                } else {
                    sample.inserts += 1;
                }
                if self.pending.len() == WRITE_BATCH {
                    self.flush();
                }
            }
        }
    }

    fn quiesce(&mut self) {
        self.flush();
    }
}

fn sharded_run(config: &Config, batched: bool) -> (Sample, Duration, usize, u64, Duration) {
    let cache = ShardedCache::new(config.capacity_entries());
    for id in 0..seed_count(config) {
        let key = key(id as u64);
        cache.insert(key, payload(&key, config.value_bytes, 0));
    }
    if config.workload.is_resident() {
        assert_eq!(
            cache.len(),
            config.entries,
            "resident control requires every seed to remain resident"
        );
    }
    let (sample, elapsed) = if batched {
        parallel_run(config, || ShardedBatchSession {
            cache: &cache,
            pending: Vec::with_capacity(WRITE_BATCH),
        })
    } else {
        parallel_run(config, || ShardedSession(&cache))
    };
    let entries = cache.len();
    let weight = entries as u64 * config.charge();
    validate_result(config, &sample, entries, weight);
    validate_resident_values(config, |key| cache.get(key));
    (sample, elapsed, entries, weight, Duration::ZERO)
}

struct QuickSession<'a>(&'a QuickCache);

impl Session for QuickSession<'_> {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        match operation.action {
            Action::Read => {
                let value = self.0.get(&operation.key);
                sample.observe(value.as_ref(), &operation.key);
                if value.is_none() && config.workload == Workload::Turnover {
                    self.0.insert(
                        operation.key,
                        payload(&operation.key, config.value_bytes, operation.version),
                    );
                    sample.writes += 1;
                }
            }
            Action::Insert | Action::Replace | Action::DeleteReinsert => {
                if matches!(operation.action, Action::DeleteReinsert) {
                    sample.removed += u64::from(self.0.remove(&operation.key).is_some());
                }
                let value = payload(&operation.key, config.value_bytes, operation.version);
                sample.writes += 1;
                // The trace's replacement/delete keys have disjoint worker
                // ownership and spare capacity, verified before/after timing.
                if matches!(operation.action, Action::Replace) {
                    self.0
                        .replace(operation.key, value, false)
                        .expect("resident replacement succeeds");
                    sample.replacements += 1;
                } else {
                    self.0.insert(operation.key, value);
                    sample.inserts += 1;
                }
            }
        }
    }
}


trait Generic: Sync {
    fn get(&self, key: &Key) -> Option<Value>;
    fn insert(&self, key: Key, value: Value);
    fn remove(&self, key: &Key) -> bool;
    fn len(&self) -> usize;
    fn flush(&self) {}
}

impl Generic for moka::sync::Cache<Key, Value> {
    fn get(&self, key: &Key) -> Option<Value> { moka::sync::Cache::get(self, key) }
    fn insert(&self, key: Key, value: Value) { moka::sync::Cache::insert(self, key, value) }
    fn remove(&self, key: &Key) -> bool { moka::sync::Cache::remove(self, key).is_some() }
    fn len(&self) -> usize { self.run_pending_tasks(); self.entry_count() as usize }
    fn flush(&self) { self.run_pending_tasks() }
}

impl Generic for mini_moka::sync::Cache<Key, Value> {
    fn get(&self, key: &Key) -> Option<Value> { mini_moka::sync::Cache::get(self, key) }
    fn insert(&self, key: Key, value: Value) { mini_moka::sync::Cache::insert(self, key, value) }
    fn remove(&self, key: &Key) -> bool {
        let had = self.contains_key(key);
        self.invalidate(key);
        had
    }
    fn len(&self) -> usize { self.entry_count() as usize }
}

impl Generic for stretto::Cache<Key, Value> {
    fn get(&self, key: &Key) -> Option<Value> { stretto::Cache::get(self, key).map(|v| v.value().clone()) }
    fn insert(&self, key: Key, value: Value) { stretto::Cache::insert(self, key, value, 1); }
    fn remove(&self, key: &Key) -> bool {
        let had = stretto::Cache::get(self, key).is_some();
        stretto::Cache::remove(self, key);
        had
    }
    fn len(&self) -> usize { stretto::Cache::len(self) }
    fn flush(&self) { let _ = self.wait(); }
}

impl Generic for tinyufo::TinyUfo<Key, Value> {
    fn get(&self, key: &Key) -> Option<Value> { tinyufo::TinyUfo::get(self, key) }
    fn insert(&self, key: Key, value: Value) { self.put(key, value, 1); }
    fn remove(&self, key: &Key) -> bool { tinyufo::TinyUfo::remove(self, key).is_some() }
    fn len(&self) -> usize { 0 }
}

impl Generic for scc::HashCache<Key, Value> {
    fn get(&self, key: &Key) -> Option<Value> { self.get_sync(key).map(|entry| entry.get().clone()) }
    fn insert(&self, key: Key, value: Value) {
        if let Err((key, value)) = self.put_sync(key, value) {
            if let Some(mut entry) = self.get_sync(&key) {
                *entry.get_mut() = value;
            }
        }
    }
    fn remove(&self, key: &Key) -> bool { self.remove_sync(key).is_some() }
    fn len(&self) -> usize { scc::HashCache::len(self) }
}

struct GenericSession<'a, C: Generic>(&'a C);

impl<C: Generic> Session for GenericSession<'_, C> {
    fn apply(&mut self, operation: &Operation, config: &Config, sample: &mut Sample) {
        match operation.action {
            Action::Read => {
                let value = self.0.get(&operation.key);
                sample.observe(value.as_ref(), &operation.key);
                if value.is_none() && config.workload == Workload::Turnover {
                    self.0.insert(
                        operation.key,
                        payload(&operation.key, config.value_bytes, operation.version),
                    );
                    sample.writes += 1;
                }
            }
            Action::Insert | Action::Replace | Action::DeleteReinsert => {
                if matches!(operation.action, Action::DeleteReinsert) {
                    sample.removed += u64::from(self.0.remove(&operation.key));
                }
                let value = payload(&operation.key, config.value_bytes, operation.version);
                sample.writes += 1;
                self.0.insert(operation.key, value);
                if matches!(operation.action, Action::Replace) {
                    sample.replacements += 1;
                } else {
                    sample.inserts += 1;
                }
            }
        }
    }
}

fn generic_run<C: Generic>(config: &Config, cache: C) -> (Sample, Duration, usize, u64, Duration) {
    for id in 0..seed_count(config) {
        let key = key(id as u64);
        cache.insert(key, payload(&key, config.value_bytes, 0));
    }
    cache.flush();
    if config.workload.is_resident() {
        let present = (0..config.entries)
            .filter(|id| cache.get(&key(*id as u64)).is_some())
            .count();
        if present != config.entries {
            eprintln!("warning: {present} of {} seeds resident", config.entries);
        }
    }
    let (sample, elapsed) = parallel_run(config, || GenericSession(&cache));
    let entries = cache.len();
    if config.workload == Workload::ResidentHit && sample.hits != config.operations as u64 {
        eprintln!("warning: {} of {} reads hit", sample.hits, config.operations);
    }
    (sample, elapsed, entries, entries as u64 * config.charge(), Duration::ZERO)
}

fn run_worker(mut session: impl Session, trace: &[Operation], config: &Config) -> Sample {
    let mut result = Sample::default();
    if config.latency_stride != 0 {
        result
            .latencies
            .reserve(trace.len().div_ceil(config.latency_stride));
    }
    for (position, operation) in trace.iter().enumerate() {
        let sample_latency = config.latency_stride != 0 && position % config.latency_stride == 0;
        let started = sample_latency.then(Instant::now);
        session.apply(operation, config, &mut result);
        if (position + 1) % REFRESH_INTERVAL == 0 {
            session.quiesce();
        }
        if let Some(started) = started {
            result.latencies.push(started.elapsed());
        }
    }
    session.quiesce();
    result
}

fn parallel_run<'a, S: Session + 'a>(
    config: &Config,
    create_session: impl Fn() -> S + Sync,
) -> (Sample, Duration) {
    // Trace construction and payload-independent memory are identical and
    // outside the timing region for every implementation.
    let traces: Vec<_> = (0..config.workers)
        .map(|worker| worker_trace(config, worker))
        .collect();
    let ready = Barrier::new(config.workers + 1);
    let start = Barrier::new(config.workers + 1);
    thread::scope(|scope| {
        let workers: Vec<_> = traces
            .iter()
            .map(|trace| {
                let create_session = &create_session;
                let ready = &ready;
                let start = &start;
                scope.spawn(move || {
                    let session = create_session();
                    ready.wait();
                    start.wait();
                    run_worker(session, trace, config)
                })
            })
            .collect();
        ready.wait();
        let started = Instant::now();
        start.wait();
        // Join without an end barrier so an adapter assertion failure cannot
        // strand the main thread waiting for the failed worker.
        let samples: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("cache worker completed"))
            .collect();
        let elapsed = started.elapsed();
        let mut result = Sample::default();
        for sample in samples {
            result.merge(sample);
        }
        (result, elapsed)
    })
}

fn seed_count(config: &Config) -> usize {
    if config.workload == Workload::ColdFill {
        0
    } else {
        config.entries
    }
}

fn direct_run<const OWNED: bool>(config: &Config) -> (Sample, Duration, usize, u64, Duration) {
    direct_run_with_admission::<OWNED, false>(config, false)
}

fn direct_background_run(config: &Config) -> (Sample, Duration, usize, u64, Duration) {
    direct_run_with_admission::<true, false>(config, true)
}

fn direct_guarded_run<const OWNED: bool>(
    config: &Config,
) -> (Sample, Duration, usize, u64, Duration) {
    assert_eq!(
        config.workload,
        Workload::ColdFill,
        "guarded insertion experiment requires cold-fill"
    );
    direct_run_with_admission::<OWNED, true>(config, false)
}

fn direct_run_with_admission<const OWNED: bool, const GUARDED: bool>(
    config: &Config,
    background: bool,
) -> (Sample, Duration, usize, u64, Duration) {
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(config.weight_limit())
                .with_max_entries(config.capacity_entries())
                .with_overlay_capacity(config.capacity_entries().max(64)),
        )
        .expect("cache constructs"),
    );
    for id in 0..seed_count(config) {
        let key = key(id as u64);
        cache
            .insert_discard_with_options(
                &key,
                payload(&key, config.value_bytes, 0),
                config.charge(),
                None,
            )
            .expect("seed inserted");
    }
    cache.maintain().expect("seed maintenance succeeds");
    let maintenance = background.then(|| cache.spawn_maintenance(Duration::from_millis(10)));
    let (sample, elapsed) = parallel_run(config, || DirectSession::<OWNED, GUARDED> {
        cache: &cache,
        guard: cache.pin(),
        writes_since_maintenance: 0,
        inline_maintenance: !background,
    });
    drop(maintenance);
    validate_result(config, &sample, cache.len(), cache.weight());
    let maintenance_started = Instant::now();
    cache.maintain().expect("final maintenance succeeds");
    let final_maintenance = maintenance_started.elapsed();
    let entries = cache.len();
    let weight = cache.weight();
    validate_result(config, &sample, entries, weight);
    let guard = cache.pin();
    validate_resident_values(config, |key| guard.peek(key).cloned());
    (sample, elapsed, entries, weight, final_maintenance)
}

fn quick_run(config: &Config) -> (Sample, Duration, usize, u64, Duration) {
    let cache = QuickCache::with_weighter(
        config.capacity_entries(),
        config.weight_limit(),
        PayloadWeighter,
    );
    for id in 0..seed_count(config) {
        let key = key(id as u64);
        cache.insert(key, payload(&key, config.value_bytes, 0));
    }
    if config.workload.is_resident() {
        assert_eq!(
            cache.len(),
            config.entries,
            "resident control requires every seed to remain resident"
        );
    }
    let (sample, elapsed) = parallel_run(config, || QuickSession(&cache));
    let entries = cache.len();
    let weight = cache.weight();
    validate_result(config, &sample, entries, weight);
    validate_resident_values(config, |key| cache.peek(key));
    (sample, elapsed, entries, weight, Duration::ZERO)
}

fn validate_result(config: &Config, sample: &Sample, entries: usize, weight: u64) {
    assert!(
        entries <= config.capacity_entries(),
        "final entry bound exceeded"
    );
    assert!(
        weight <= config.weight_limit(),
        "final weight bound exceeded"
    );
    assert_eq!(
        weight,
        entries as u64 * config.charge(),
        "logical charge disagrees with residency"
    );
    assert!(
        entries > 0,
        "cache must retain at least one record for this comparison"
    );
    match config.workload {
        Workload::ResidentHit => assert_eq!(sample.hits, config.operations as u64),
        Workload::ReadMiss => assert_eq!(sample.hits, 0),
        Workload::ColdFill => assert_eq!(sample.inserts, config.operations as u64),
        Workload::Replace => assert_eq!(sample.replacements, config.operations as u64),
        Workload::DeleteReinsert => {
            assert_eq!(sample.removed, config.operations as u64);
            assert_eq!(sample.inserts, config.operations as u64);
            assert_eq!(entries, config.entries);
        }
        Workload::Turnover => {}
    }
}

fn validate_resident_values(config: &Config, mut get: impl FnMut(&Key) -> Option<Value>) {
    if !config.workload.is_resident() {
        return;
    }
    let mut versions = vec![0_u64; config.entries];
    if matches!(
        config.workload,
        Workload::Replace | Workload::DeleteReinsert
    ) {
        for worker in 0..config.workers {
            for operation in worker_trace(config, worker) {
                let id = u64::from_le_bytes(operation.key[..8].try_into().expect("key id"));
                versions[usize::try_from(id).expect("resident id fits usize")] = operation.version;
            }
        }
    }
    for (id, version) in versions.into_iter().enumerate() {
        let key = key(id as u64);
        let value = get(&key).expect("all resident control keys must remain present");
        assert_eq!(value.len(), config.value_bytes);
        assert_eq!(&value[..8], &key[..8]);
        assert_eq!(
            &value[8..16],
            &version.to_le_bytes(),
            "final version must match the latest write"
        );
    }
}

fn parse<T: std::str::FromStr>(args: &[String], position: usize, label: &str) -> T {
    args.get(position)
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse()
        .unwrap_or_else(|_| panic!("invalid {label}"))
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let backend = args.get(1).expect("missing backend");
    let config = Config {
        workload: Workload::parse(args.get(2).expect("missing workload")),
        entries: parse(&args, 3, "entries"),
        operations: parse(&args, 4, "operations"),
        workers: parse(&args, 5, "workers"),
        value_bytes: parse(&args, 6, "value bytes"),
        seed: parse(&args, 7, "seed"),
        latency_stride: args.get(8).map_or(0, |_| parse(&args, 8, "latency stride")),
    };
    config.validate();
    let (mut sample, elapsed, entries, weight, final_maintenance) = match backend.as_str() {
        "direct-owned" => direct_run::<true>(&config),
        "direct-borrowed" => direct_run::<false>(&config),
        "direct-guarded-owned" => direct_guarded_run::<true>(&config),
        "direct-guarded-borrowed" => direct_guarded_run::<false>(&config),
        "direct-background" => direct_background_run(&config),
        "quickcache" => quick_run(&config),
        "sharded" => sharded_run(&config, false),
        "sharded-batch" => sharded_run(&config, true),
        "moka" => generic_run(&config, moka::sync::Cache::<Key, Value>::new(config.capacity_entries() as u64)),
        "mini-moka" => generic_run(&config, mini_moka::sync::Cache::<Key, Value>::new(config.capacity_entries() as u64)),
        "stretto" => generic_run(&config, stretto::Cache::<Key, Value>::new(config.capacity_entries() * 10, config.capacity_entries() as i64).unwrap()),
        "tinyufo" => generic_run(&config, tinyufo::TinyUfo::<Key, Value>::new(config.capacity_entries(), config.capacity_entries())),
        "scc" => generic_run(&config, scc::HashCache::<Key, Value>::with_capacity(0, config.capacity_entries())),
        _ => panic!("unknown backend: {backend}"),
    };
    sample.latencies.sort_unstable();
    let quantile = |per_hundred_thousand: usize| {
        sample
            .latencies
            .get(
                sample
                    .latencies
                    .len()
                    .saturating_mul(per_hundred_thousand)
                    .div_ceil(100_000)
                    .saturating_sub(1),
            )
            .map_or_else(|| "na".to_owned(), |value| value.as_nanos().to_string())
    };
    println!(
        "backend,workload,entries_limit,weight_limit,operations,workers,value_bytes,seed,latency_stride,mops,read_hits,reads,write_attempts,removed,final_entries,final_weight,latency_samples,p99_ns,final_maintenance_ns,checksum,p999_ns,p9999_ns,p99999_ns,max_ns"
    );
    println!(
        "{backend},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        args[2],
        config.capacity_entries(),
        config.weight_limit(),
        config.operations,
        config.workers,
        config.value_bytes,
        config.seed,
        config.latency_stride,
        config.operations as f64 / elapsed.as_secs_f64() / 1_000_000.0,
        sample.hits,
        sample.reads,
        sample.writes,
        sample.removed,
        entries,
        weight,
        sample.latencies.len(),
        quantile(99_000),
        final_maintenance.as_nanos(),
        black_box(sample.checksum),
        quantile(99_900),
        quantile(99_990),
        quantile(99_999),
        quantile(100_000),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(workload: Workload) -> Config {
        Config {
            workload,
            entries: 128,
            operations: 1_003,
            workers: 3,
            value_bytes: 64,
            seed: 17,
            latency_stride: 0,
        }
    }

    #[test]
    fn cold_fill_has_a_fixed_budget_and_distinct_new_keys_across_workers() {
        let config = config(Workload::ColdFill);
        assert_eq!(seed_count(&config), 0);
        assert_eq!(config.capacity_entries(), 128);
        let mut keys: Vec<_> = (0..config.workers)
            .flat_map(|worker| worker_trace(&config, worker))
            .map(|operation| operation.key)
            .collect();
        assert_eq!(keys.len(), config.operations);
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), config.operations);
    }

    #[test]
    fn delete_reinsert_workers_never_share_a_resident_key() {
        let config = config(Workload::DeleteReinsert);
        let traces: Vec<_> = (0..config.workers)
            .map(|worker| worker_trace(&config, worker))
            .collect();
        for first in 0..config.workers {
            for second in first + 1..config.workers {
                assert!(
                    !traces[first]
                        .iter()
                        .any(|left| traces[second].iter().any(|right| left.key == right.key))
                );
            }
        }
    }

    #[test]
    fn adapters_preserve_resident_values_and_bound_turnover() {
        for workload in [
            Workload::ResidentHit,
            Workload::ReadMiss,
            Workload::ColdFill,
            Workload::Turnover,
            Workload::Replace,
            Workload::DeleteReinsert,
        ] {
            let config = config(workload);
            config.validate();
            let _ = direct_run::<true>(&config);
            let _ = direct_run::<false>(&config);
            let _ = quick_run(&config);
        }
    }

    #[test]
    fn guarded_cold_fill_preserves_the_budget_across_refresh_and_maintenance() {
        for value_bytes in [64, 65_536] {
            let mut config = config(Workload::ColdFill);
            config.operations = 13_003;
            config.value_bytes = value_bytes;
            let (sample, _, entries, weight, _) = direct_guarded_run::<true>(&config);
            assert_eq!(sample.inserts, config.operations as u64);
            assert_eq!(sample.replacements, 0);
            assert_eq!(sample.writes, config.operations as u64);
            assert!(entries <= config.capacity_entries());
            assert_eq!(weight, entries as u64 * config.charge());
            let _ = direct_guarded_run::<false>(&config);
        }
    }

    #[test]
    fn guarded_cold_fill_uses_the_worker_guard_and_flushes_insert_statistics() {
        let config = config(Workload::ColdFill);
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(config.weight_limit()).with_max_entries(config.capacity_entries()),
        )
        .expect("cache constructs");
        let mut session = DirectSession::<true, true> {
            cache: &cache,
            guard: cache.pin(),
            writes_since_maintenance: 0,
            inline_maintenance: true,
        };
        let mut sample = Sample::default();
        session.apply(&worker_trace(&config, 0)[0], &config, &mut sample);
        assert_eq!(sample.inserts, 1);
        assert_eq!(
            cache.stats().inserts,
            0,
            "guard buffers insertion statistics"
        );
        session.quiesce();
        assert_eq!(cache.stats().inserts, 1);
    }

    #[test]
    #[should_panic(expected = "guarded insertion experiment requires cold-fill")]
    fn guarded_insertion_cannot_silently_change_turnover_upsert_semantics() {
        let _ = direct_guarded_run::<true>(&config(Workload::Turnover));
    }

    #[test]
    #[should_panic(expected = "latency stride must be odd")]
    fn latency_sampling_rejects_refresh_phase_aliasing() {
        let mut config = config(Workload::ResidentHit);
        config.latency_stride = 128;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "final version must match")]
    fn resident_validation_rejects_stale_replacement_values() {
        let config = config(Workload::Replace);
        validate_resident_values(&config, |key| Some(payload(key, config.value_bytes, 0)));
    }

    #[test]
    fn resident_controls_preserve_the_full_seed_across_randomized_shards() {
        let mut config = config(Workload::ResidentHit);
        config.entries = 1024;
        config.workers = 2;
        assert_eq!(config.capacity_entries(), 4096);
        for _ in 0..32 {
            let _ = quick_run(&config);
        }
    }
}
