//! Deterministic single-threaded hit-ratio simulator.
//!
//! Usage: `hit_ratio_sim TRACE ENTRIES OPERATIONS SEED [POLICY...]`.
//!
//! TRACE is `turnover`, `zipf:ALPHA`, `scan-zipf:ALPHA`, `shift:ALPHA`, `loop`,
//! `file:PATH` (one key per line), or `arc:PATH` (an ARC-paper `.lis` trace,
//! whose `START COUNT ...` lines each read `COUNT` consecutive blocks). For
//! file traces, `OPERATIONS` of zero replays the whole file. POLICY defaults to every policy: `packed`, `packed-doorkeeper`,
//! `packed-adaptive`, `packed-tiered`, `sharded` (`ShardedCache`), `quick`,
//! `quick-sync` (`QuickCache`'s sharded cache), `lru`, `opt`, and the reference
//! models `model-clock`, `model-s3clock`, `model-s3fifo`, `model-quick`
//! (`QuickCache` 0.7's policy), and `model-tinylfu` (W-TinyLFU). One CSV row is
//! printed per policy. A packed policy accepts `/batch=N`, `/probation=BPS`,
//! `/maint=N`, and `/overlay=N` (see `packed_config`); `sharded` accepts
//! `/probation=BPS` and `/shards=N`; a model accepts the
//! options listed at `ClockModel::parse`.
//!
//! Reads are look-aside, as in `bounded_cache_probe`: a miss admits the key.
//! Writes admit unconditionally. Every record has the same charge, so each
//! policy holds at most `ENTRIES` records. `turnover` replays the probe's
//! one-worker trace, including its preloaded keys. `opt` is Belady's offline
//! policy with bypass: it may decline any admission, including writes, so its
//! hit count bounds every online policy on the same trace.
//!
//! Hit ratios count reads only. `steady_*` columns cover the second half of the
//! trace, after cold-start effects.

#![allow(clippy::cast_precision_loss)]

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::ops::Range;

use packedgen::{
    CacheConfig, DirectCacheGuard, DirectPackedCache, ShardedCache, ShardedCacheBuilder,
};
use quick_cache::Weighter;
use quick_cache::unsync::{Cache as QuickCache, DefaultLifecycle};

type Key = [u8; 32];
const VALUE_BYTES: u64 = 64;
const CHARGE: u64 = 32 + VALUE_BYTES;
const REFRESH_INTERVAL: usize = 512;
const MAINTENANCE_INTERVAL: usize = 4096;
const OCCUPANCY_STRIDE: usize = 64;
const ALL_POLICIES: [&str; 14] = [
    "packed",
    "packed-doorkeeper",
    "packed-adaptive",
    "packed-tiered",
    "sharded",
    "quick",
    "quick-sync",
    "lru",
    "opt",
    "model-clock",
    "model-s3clock",
    "model-s3fifo",
    "model-quick",
    "model-tinylfu",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Request {
    id: u64,
    write: bool,
}

struct Trace {
    preload: Range<u64>,
    requests: Vec<Request>,
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// The probe's key layout, so index hashing sees the same bytes.
fn key(id: u64) -> Key {
    let mut result = [0; 32];
    result[..8].copy_from_slice(&id.to_le_bytes());
    for lane in 1..4_u64 {
        let word = mix(id.wrapping_add(lane));
        let lane = usize::try_from(lane).expect("lane fits usize");
        result[lane * 8..(lane + 1) * 8].copy_from_slice(&word.to_le_bytes());
    }
    result
}

/// Per-request randomness for the synthetic traces. Mixing the seed first
/// keeps nearby seeds from permuting the same draws, which `seed ^ operation`
/// alone would do; `turnover` keeps that form to match the probe exactly.
fn draw(seed: u64, operation: u64) -> u64 {
    mix(mix(seed) ^ operation)
}

fn unit(random: u64) -> f64 {
    (random >> 11) as f64 / (1_u64 << 53) as f64
}

fn read(id: u64) -> Request {
    Request { id, write: false }
}

/// `bounded_cache_probe`'s one-worker turnover trace: 95% reads, 80% of them
/// on a hot tenth, the rest over twice the capacity; 5% unique scan writes.
fn turnover(entries: u64, operations: u64, seed: u64) -> Trace {
    let requests = (0..operations)
        .map(|operation| {
            let random = mix(seed ^ operation);
            if random % 100 < 95 {
                let id = if random.rotate_left(17) % 100 < 80 {
                    mix(random) % entries.div_ceil(10)
                } else {
                    mix(random) % (2 * entries)
                };
                read(id)
            } else {
                Request {
                    id: 2 * entries + operation,
                    write: true,
                }
            }
        })
        .collect();
    Trace {
        preload: 0..entries,
        requests,
    }
}

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(universe: u64, alpha: f64) -> Self {
        let mut total = 0.0;
        let mut cdf: Vec<f64> = (1..=universe)
            .map(|rank| {
                total += (rank as f64).powf(-alpha);
                total
            })
            .collect();
        for value in &mut cdf {
            *value /= total;
        }
        Self { cdf }
    }

    fn sample(&self, random: u64) -> u64 {
        let target = unit(random);
        let rank = self
            .cdf
            .partition_point(|&value| value < target)
            .min(self.cdf.len() - 1);
        u64::try_from(rank).expect("rank fits u64")
    }
}

/// Zipf reads over ten times the capacity.
fn zipf(entries: u64, operations: u64, seed: u64, alpha: f64) -> Trace {
    let distribution = Zipf::new(10 * entries, alpha);
    let requests = (0..operations)
        .map(|operation| read(distribution.sample(draw(seed, operation))))
        .collect();
    Trace {
        preload: 0..0,
        requests,
    }
}

/// Zipf reads interrupted every four capacities by a scan of half a capacity
/// of keys that are never seen again.
fn scan_zipf(entries: u64, operations: u64, seed: u64, alpha: f64) -> Trace {
    let universe = 10 * entries;
    let distribution = Zipf::new(universe, alpha);
    let period = 4 * entries;
    let scan = entries / 2;
    let requests = (0..operations)
        .map(|operation| {
            if operation % period < scan {
                read(universe + operation)
            } else {
                read(distribution.sample(draw(seed, operation)))
            }
        })
        .collect();
    Trace {
        preload: 0..0,
        requests,
    }
}

/// Zipf reads whose popular keys are replaced by unrelated keys halfway.
fn shift(entries: u64, operations: u64, seed: u64, alpha: f64) -> Trace {
    let universe = 10 * entries;
    let distribution = Zipf::new(universe, alpha);
    let requests = (0..operations)
        .map(|operation| {
            let rank = distribution.sample(draw(seed, operation));
            read(if operation < operations / 2 {
                rank
            } else {
                universe + rank
            })
        })
        .collect();
    Trace {
        preload: 0..0,
        requests,
    }
}

/// A cyclic scan over 1.25 capacities, which defeats recency-only policies.
fn cyclic(entries: u64, operations: u64) -> Trace {
    let length = entries + entries / 4;
    Trace {
        preload: 0..0,
        requests: (0..operations)
            .map(|operation| read(operation % length))
            .collect(),
    }
}

fn file(path: &str, operations: u64) -> Trace {
    let text = std::fs::read_to_string(path).expect("trace file is readable UTF-8");
    let limit = if operations == 0 {
        usize::MAX
    } else {
        usize::try_from(operations).expect("operation count fits usize")
    };
    let requests = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(limit)
        .map(|line| {
            let line = line.trim();
            read(line.parse().unwrap_or_else(|_| {
                line.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                    (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
                })
            }))
        })
        .collect();
    Trace {
        preload: 0..0,
        requests,
    }
}

/// Expands an ARC-paper trace (Megiddo and Modha, FAST 2003): each line is
/// `START COUNT IGNORED REQUEST` and reads blocks `START..START + COUNT`.
fn arc(path: &str, operations: u64) -> Trace {
    let text = std::fs::read_to_string(path).expect("trace file is readable UTF-8");
    let limit = if operations == 0 {
        usize::MAX
    } else {
        usize::try_from(operations).expect("operation count fits usize")
    };
    let mut requests = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_ascii_whitespace();
        let (Some(start), Some(count)) = (fields.next(), fields.next()) else {
            continue;
        };
        let start: u64 = start.parse().expect("ARC start block is an integer");
        let count: u64 = count.parse().expect("ARC block count is an integer");
        requests.extend((start..start + count).map(read));
        if requests.len() >= limit {
            requests.truncate(limit);
            break;
        }
    }
    Trace {
        preload: 0..0,
        requests,
    }
}

fn build_trace(name: &str, entries: u64, operations: u64, seed: u64) -> Trace {
    let alpha = |spec: &str| -> f64 { spec.parse().expect("ALPHA is a number") };
    match name.split_once(':') {
        None if name == "turnover" => turnover(entries, operations, seed),
        None if name == "loop" => cyclic(entries, operations),
        Some(("zipf", spec)) => zipf(entries, operations, seed, alpha(spec)),
        Some(("scan-zipf", spec)) => scan_zipf(entries, operations, seed, alpha(spec)),
        Some(("shift", spec)) => shift(entries, operations, seed, alpha(spec)),
        Some(("file", path)) => file(path, operations),
        Some(("arc", path)) => arc(path, operations),
        _ => panic!("unknown trace: {name}"),
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Outcome {
    reads: u64,
    hits: u64,
    steady_reads: u64,
    steady_hits: u64,
    resident_sum: u64,
    resident_samples: u64,
    steady_resident_sum: u64,
    steady_resident_samples: u64,
    rejected: u64,
}

impl Outcome {
    fn hit_ratio(&self) -> f64 {
        ratio(self.hits, self.reads)
    }

    fn steady_hit_ratio(&self) -> f64 {
        ratio(self.steady_hits, self.steady_reads)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

trait Session {
    /// Looks up `id`, admitting it on a miss. Returns whether it hit.
    fn read(&mut self, id: u64) -> bool;
    fn write(&mut self, id: u64);
    fn resident(&self) -> usize;
    fn after_request(&mut self, _position: usize) {}
}

fn replay(trace: &Trace, session: &mut impl Session) -> Outcome {
    for id in trace.preload.clone() {
        session.write(id);
    }
    let mut outcome = Outcome::default();
    let steady_begin = trace.requests.len() / 2;
    for (position, request) in trace.requests.iter().enumerate() {
        let steady = position >= steady_begin;
        if request.write {
            session.write(request.id);
        } else {
            let hit = session.read(request.id);
            outcome.reads += 1;
            outcome.hits += u64::from(hit);
            if steady {
                outcome.steady_reads += 1;
                outcome.steady_hits += u64::from(hit);
            }
        }
        session.after_request(position);
        if position.is_multiple_of(OCCUPANCY_STRIDE) {
            let resident = u64::try_from(session.resident()).expect("length fits u64");
            outcome.resident_sum += resident;
            outcome.resident_samples += 1;
            if steady {
                outcome.steady_resident_sum += resident;
                outcome.steady_resident_samples += 1;
            }
        }
    }
    outcome
}

/// `POLICY[/batch=N][/probation=BPS][/maint=N][/overlay=N]`: the eviction
/// batch (victim window), the probationary share, writes between `maintain()`
/// calls (zero never maintains), and the index overlay capacity (default: the
/// cache's own). Returns the config and maintenance interval.
fn packed_config(policy: &str, entries: usize) -> (CacheConfig, usize) {
    let mut base = CacheConfig::new(u64::try_from(entries).expect("entries fit u64") * CHARGE)
        .with_max_entries(entries);
    let mut maintenance_interval = MAINTENANCE_INTERVAL;
    let mut parts = policy.split('/');
    let policy = parts.next().expect("split yields a policy name");
    for part in parts {
        let (option, value) = part.split_once('=').expect("options are NAME=VALUE");
        let value: usize = value.parse().expect("option values are integers");
        match option {
            "batch" => base = base.with_eviction_batch(value),
            "probation" => {
                base = base.with_probation(u16::try_from(value).expect("share fits u16"));
            }
            "maint" => maintenance_interval = value,
            "overlay" => base = base.with_overlay_capacity(value),
            _ => panic!("unknown packed option: {option}"),
        }
    }
    let config = match policy {
        "packed" => base,
        "packed-doorkeeper" => base.with_admission_doorkeeper(entries),
        "packed-adaptive" => base
            .with_admission_doorkeeper(entries)
            .with_adaptive_frequency_admission(2, 6_000),
        "packed-tiered" => base
            .with_admission_doorkeeper(entries)
            .with_tiered_frequency_admission(2, 6_000, 8, 7_500),
        _ => panic!("unknown packed policy: {policy}"),
    };
    (config, maintenance_interval)
}

struct PackedSession<'cache> {
    cache: &'cache DirectPackedCache<u64>,
    guard: DirectCacheGuard<'cache, u64>,
    maintenance_interval: usize,
    writes_since_maintenance: usize,
}

impl PackedSession<'_> {
    fn after_write(&mut self) {
        self.writes_since_maintenance += 1;
        if self.writes_since_maintenance == self.maintenance_interval {
            self.guard.quiesce();
            self.cache.maintain().expect("maintenance succeeds");
            self.writes_since_maintenance = 0;
        }
    }
}

impl Session for PackedSession<'_> {
    fn read(&mut self, id: u64) -> bool {
        let key = key(id);
        if let Some(&value) = self.guard.get(&key) {
            assert_eq!(value, id, "cache returned another key's value");
            return true;
        }
        // A look-aside fill goes through admission; an upsert would bypass it.
        self.cache
            .insert_if_absent_with_options(&key, id, CHARGE, None)
            .expect("valid charge");
        self.after_write();
        false
    }

    fn write(&mut self, id: u64) {
        self.cache
            .insert_discard_with_options(&key(id), id, CHARGE, None)
            .expect("valid charge");
        self.after_write();
    }

    fn resident(&self) -> usize {
        self.cache.len()
    }

    fn after_request(&mut self, position: usize) {
        if (position + 1).is_multiple_of(REFRESH_INTERVAL) {
            self.guard.quiesce();
        }
    }
}

fn run_packed(policy: &str, trace: &Trace, entries: usize) -> Outcome {
    let (config, maintenance_interval) = packed_config(policy, entries);
    let cache = DirectPackedCache::try_new(config).expect("cache builds");
    let mut session = PackedSession {
        cache: &cache,
        guard: cache.pin(),
        maintenance_interval,
        writes_since_maintenance: 0,
    };
    // Match the probe: preloaded keys are maintained before the measured run.
    for id in trace.preload.clone() {
        session.write(id);
    }
    session.guard.quiesce();
    cache.maintain().expect("seed maintenance succeeds");
    session.writes_since_maintenance = 0;
    let measured = Trace {
        preload: 0..0,
        requests: trace.requests.clone(),
    };
    let mut outcome = replay(&measured, &mut session);
    session.guard.quiesce();
    drop(session);
    outcome.rejected = cache.stats().rejected;
    outcome
}

#[derive(Clone)]
struct ConstantWeighter;

impl Weighter<Key, u64> for ConstantWeighter {
    fn weight(&self, _key: &Key, _value: &u64) -> u64 {
        CHARGE
    }
}

type Quick = QuickCache<
    Key,
    u64,
    ConstantWeighter,
    BuildHasherDefault<DefaultHasher>,
    DefaultLifecycle<Key, u64>,
>;

struct QuickSession(Quick);

impl Session for QuickSession {
    fn read(&mut self, id: u64) -> bool {
        let key = key(id);
        if let Some(&value) = self.0.get(&key) {
            assert_eq!(value, id, "cache returned another key's value");
            true
        } else {
            self.0.insert(key, id);
            false
        }
    }

    fn write(&mut self, id: u64) {
        self.0.insert(key(id), id);
    }

    fn resident(&self) -> usize {
        self.0.len()
    }
}

struct ShardedSession(ShardedCache<Key, u64>);

impl Session for ShardedSession {
    fn read(&mut self, id: u64) -> bool {
        let key = key(id);
        if let Some(value) = self.0.get(&key) {
            assert_eq!(value, id, "cache returned another key's value");
            true
        } else {
            self.0.insert(key, id);
            false
        }
    }

    fn write(&mut self, id: u64) {
        self.0.insert(key(id), id);
    }

    fn resident(&self) -> usize {
        self.0.len()
    }
}

struct QuickSyncSession(quick_cache::sync::Cache<Key, u64, ConstantWeighter>);

impl Session for QuickSyncSession {
    fn read(&mut self, id: u64) -> bool {
        let key = key(id);
        if let Some(value) = self.0.get(&key) {
            assert_eq!(value, id, "cache returned another key's value");
            true
        } else {
            self.0.insert(key, id);
            false
        }
    }

    fn write(&mut self, id: u64) {
        self.0.insert(key(id), id);
    }

    fn resident(&self) -> usize {
        self.0.len()
    }
}

fn run_quick(trace: &Trace, entries: usize) -> Outcome {
    let cache = Quick::with(
        entries,
        u64::try_from(entries).expect("entries fit u64") * CHARGE,
        ConstantWeighter,
        BuildHasherDefault::default(),
        DefaultLifecycle::default(),
    );
    replay(trace, &mut QuickSession(cache))
}

const NIL: usize = usize::MAX;

/// Exact LRU over an index-linked list; `head` is most recent.
struct Lru {
    capacity: usize,
    slots: HashMap<u64, usize>,
    ids: Vec<u64>,
    previous: Vec<usize>,
    next: Vec<usize>,
    head: usize,
    tail: usize,
}

impl Lru {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LRU capacity must be positive");
        Self {
            capacity,
            slots: HashMap::with_capacity(capacity),
            ids: Vec::with_capacity(capacity),
            previous: Vec::with_capacity(capacity),
            next: Vec::with_capacity(capacity),
            head: NIL,
            tail: NIL,
        }
    }

    fn unlink(&mut self, slot: usize) {
        let (previous, next) = (self.previous[slot], self.next[slot]);
        if previous == NIL {
            self.head = next;
        } else {
            self.next[previous] = next;
        }
        if next == NIL {
            self.tail = previous;
        } else {
            self.previous[next] = previous;
        }
    }

    fn push_front(&mut self, slot: usize) {
        self.previous[slot] = NIL;
        self.next[slot] = self.head;
        if self.head == NIL {
            self.tail = slot;
        } else {
            self.previous[self.head] = slot;
        }
        self.head = slot;
    }

    fn touch(&mut self, id: u64) -> bool {
        if let Some(&slot) = self.slots.get(&id) {
            self.unlink(slot);
            self.push_front(slot);
            return true;
        }
        let slot = if self.ids.len() < self.capacity {
            self.ids.push(id);
            self.previous.push(NIL);
            self.next.push(NIL);
            self.ids.len() - 1
        } else {
            let victim = self.tail;
            self.unlink(victim);
            self.slots.remove(&self.ids[victim]);
            self.ids[victim] = id;
            victim
        };
        self.slots.insert(id, slot);
        self.push_front(slot);
        false
    }
}

impl Session for Lru {
    fn read(&mut self, id: u64) -> bool {
        self.touch(id)
    }

    fn write(&mut self, id: u64) {
        self.touch(id);
    }

    fn resident(&self) -> usize {
        self.slots.len()
    }
}

/// Belady's policy with bypass: evict the resident used furthest in the
/// future, or decline admission when the candidate is used later than all.
struct Opt {
    capacity: usize,
    /// Next request position of each resident id.
    resident: HashMap<u64, usize>,
    /// Max-heap of `(next use, id)`; stale entries are skipped lazily.
    heap: BinaryHeap<(usize, u64)>,
    next_use: Vec<usize>,
    position: usize,
}

impl Opt {
    fn new(trace: &Trace, capacity: usize) -> Self {
        let mut next_use = vec![usize::MAX; trace.requests.len()];
        let mut upcoming = HashMap::new();
        for (position, request) in trace.requests.iter().enumerate().rev() {
            if let Some(next) = upcoming.insert(request.id, position) {
                next_use[position] = next;
            }
        }
        let mut opt = Self {
            capacity,
            resident: HashMap::with_capacity(capacity),
            heap: BinaryHeap::with_capacity(capacity),
            next_use,
            position: 0,
        };
        for id in trace.preload.clone() {
            let first = upcoming.get(&id).copied().unwrap_or(usize::MAX);
            opt.admit(id, first);
        }
        opt
    }

    fn admit(&mut self, id: u64, next: usize) {
        if self.resident.len() == self.capacity {
            let victim = loop {
                let (victim_next, victim) = *self.heap.peek().expect("full cache has residents");
                if self.resident.get(&victim) == Some(&victim_next) {
                    break (victim_next, victim);
                }
                self.heap.pop();
            };
            if next >= victim.0 {
                return;
            }
            self.heap.pop();
            self.resident.remove(&victim.1);
        }
        self.resident.insert(id, next);
        self.heap.push((next, id));
    }

    fn access(&mut self, id: u64) -> bool {
        let next = self.next_use[self.position];
        self.position += 1;
        if let Some(entry) = self.resident.get_mut(&id) {
            *entry = next;
            self.heap.push((next, id));
            return true;
        }
        self.admit(id, next);
        false
    }
}

impl Session for Opt {
    fn read(&mut self, id: u64) -> bool {
        self.access(id)
    }

    fn write(&mut self, id: u64) {
        self.access(id);
    }

    fn resident(&self) -> usize {
        self.resident.len()
    }
}

const EMPTY_SLOT: u64 = u64::MAX;

#[derive(Clone, Copy)]
struct ModelEntry {
    frequency: u8,
    /// Main-area slot, or `None` while the key is in the small FIFO.
    slot: Option<usize>,
}

/// CLOCK, optionally behind S3-FIFO's small FIFO and ghost FIFO.
///
/// With a small FIFO, new keys enter it; a key not hit while there is evicted
/// and remembered as a ghost, and a returning ghost goes straight to main.
/// Main is a CLOCK over slots reused from a free list, so the hand visits keys
/// in an order unrelated to insertion, like a sweep over an index.
struct ClockModel {
    capacity: usize,
    small_capacity: usize,
    max_frequency: u8,
    ghost_capacity: usize,
    /// Only every `sample_every`-th hit raises a frequency, like the packed cache's
    /// sampled access marks.
    sample_every: u64,
    hits: u64,
    resident: HashMap<u64, ModelEntry>,
    slots: Vec<u64>,
    free_slots: Vec<usize>,
    hand: usize,
    small: VecDeque<u64>,
    ghost: VecDeque<u64>,
    ghost_counts: HashMap<u64, u32>,
    /// `TinyLFU`: a small-FIFO key leaving unread duels the next main victim
    /// and replaces it if the sketch has seen it more often.
    sketch: Option<ModelSketch>,
    /// Only every `sketch_sample_every`-th hit is recorded in the sketch;
    /// misses always are.
    sketch_sample_every: u64,
    sketch_hits: u64,
    climber: Option<ModelClimber>,
    /// `QuickCache` keeps one fewer mark when a key leaves the small FIFO.
    keep_on_promotion: bool,
    /// Bounds how many main keys one eviction inspects.
    victim_window: Option<usize>,
    /// Replaces the ghost FIFO with the packed cache's two rotating
    /// generations, which remember between one and two generations of keys.
    ghost_generations: Option<GhostGenerations>,
}

/// A ring of generations: remembers between `generations - 1` and
/// `generations` full generations of keys.
struct GhostGenerations {
    sets: Vec<HashSet<u64>>,
    current: usize,
    rotate_every: usize,
}

impl GhostGenerations {
    fn contains(&self, id: u64) -> bool {
        self.sets.iter().any(|set| set.contains(&id))
    }

    fn remember(&mut self, id: u64) {
        if self.sets[self.current].len() >= self.rotate_every {
            self.current = (self.current + 1) % self.sets.len();
            self.sets[self.current].clear();
        }
        self.sets[self.current].insert(id);
    }
}

/// Count-min sketch of 4-bit counters with two hashes, halved every
/// `10 * capacity` increments as in `TinyLFU`.
struct ModelSketch {
    counters: Vec<u8>,
    mask: usize,
    increments: u64,
    reset_every: u64,
}

impl ModelSketch {
    fn new(capacity: usize) -> Self {
        let counters = (capacity * 4).next_power_of_two();
        Self {
            counters: vec![0; counters],
            mask: counters - 1,
            increments: 0,
            reset_every: u64::try_from(capacity * 10).expect("capacity fits u64"),
        }
    }

    fn slots(&self, id: u64) -> [usize; 2] {
        let first = mix(id ^ 0x5851_f42d_4c95_7f2d);
        [first, mix(first)].map(|hash| usize::try_from(hash).unwrap_or(usize::MAX) & self.mask)
    }

    fn increment(&mut self, id: u64) {
        for slot in self.slots(id) {
            self.counters[slot] = (self.counters[slot] + 1).min(15);
        }
        self.increments += 1;
        if self.increments.is_multiple_of(self.reset_every) {
            for counter in &mut self.counters {
                *counter /= 2;
            }
        }
    }

    fn estimate(&self, id: u64) -> u8 {
        self.slots(id)
            .into_iter()
            .map(|slot| self.counters[slot])
            .min()
            .expect("two slots")
    }
}

/// Caffeine's hill climber: every `10 * capacity` requests, move the small
/// FIFO's size in the direction that last raised the hit rate.
struct ModelClimber {
    capacity: usize,
    sample: u64,
    hits: u64,
    requests: u64,
    previous_hit_rate: f64,
    step: f64,
}

impl ModelClimber {
    const STEP_SHARE: f64 = 0.0625;
    const DECAY: f64 = 0.98;
    const RESTART_THRESHOLD: f64 = 0.05;

    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            sample: u64::try_from(capacity * 10).expect("capacity fits u64"),
            hits: 0,
            requests: 0,
            previous_hit_rate: 0.0,
            step: -Self::STEP_SHARE * capacity as f64,
        }
    }

    /// Records one request; returns a size adjustment at the end of a sample.
    fn record(&mut self, hit: bool) -> Option<f64> {
        self.requests += 1;
        self.hits += u64::from(hit);
        if self.requests < self.sample {
            return None;
        }
        let hit_rate = self.hits as f64 / self.requests as f64;
        let change = hit_rate - self.previous_hit_rate;
        let amount = if change >= 0.0 { self.step } else { -self.step };
        self.step = if change.abs() >= Self::RESTART_THRESHOLD {
            Self::STEP_SHARE * self.capacity as f64 * amount.signum()
        } else {
            Self::DECAY * amount
        };
        self.previous_hit_rate = hit_rate;
        self.hits = 0;
        self.requests = 0;
        Some(amount)
    }
}

impl ClockModel {
    /// `NAME[/OPTION=N]...`. `small=PCT` or `smallbps=BPS` sizes the small
    /// FIFO and `ghost=PCT` the ghost FIFO, as shares of capacity. `ghostgens=N`
    /// with `ghostgenbps=BPS` (or `ghostgen=PCT`) replaces the ghost FIFO with
    /// N rotating generations of that size each. `maxf=N` caps the marks a key
    /// collects, `keep=1` keeps all but one when it leaves the small FIFO, and
    /// `window=N` bounds the main keys one eviction inspects. `sample=N` keeps
    /// every Nth hit's mark. For `model-tinylfu`, `fsample=N` records every
    /// Nth hit in the sketch and `climb=1` enables the hill climber.
    fn parse(name: &str, capacity: usize) -> Self {
        let mut parts = name.split('/');
        let (mut small_bps, mut max_frequency, tinylfu) = match parts.next() {
            Some("model-clock") => (0, 1, false),
            Some("model-s3fifo") => (1_000, 3, false),
            Some("model-s3clock") => (1_000, 1, false),
            Some("model-tinylfu") => (100, 1, true),
            // QuickCache 0.7's defaults: 3% cold, ghosts for half the capacity,
            // and hot marks up to two.
            Some("model-quick") => (300, 2, false),
            _ => panic!("unknown model: {name}"),
        };
        let quick = name.starts_with("model-quick");
        let mut ghost_percent = if tinylfu {
            0
        } else if quick {
            50
        } else {
            100 - small_bps / 100
        };
        let mut keep = quick;
        let mut ghost_generation_bps = None;
        let mut ghost_generation_count = 2;
        let mut victim_window = None;
        let mut sample_every = 1;
        let mut sketch_sample_every = 1;
        let mut climb = false;
        for part in parts {
            let (option, value) = part.split_once('=').expect("model options are NAME=VALUE");
            let value: usize = value.parse().expect("model option values are integers");
            match option {
                "small" => small_bps = value * 100,
                "smallbps" => small_bps = value,
                "ghost" => ghost_percent = value,
                "sample" => sample_every = u64::try_from(value).expect("sample fits u64"),
                "fsample" => sketch_sample_every = u64::try_from(value).expect("fits u64"),
                "climb" => climb = value != 0,
                "maxf" => max_frequency = u8::try_from(value).expect("maxf fits u8"),
                "keep" => keep = value != 0,
                "ghostgen" => ghost_generation_bps = Some(value * 100),
                "ghostgenbps" => ghost_generation_bps = Some(value),
                "ghostgens" => ghost_generation_count = value,
                "window" => victim_window = Some(value.max(1)),
                _ => panic!("unknown model option: {option}"),
            }
        }
        let small = (capacity * small_bps / 10_000).max(usize::from(small_bps != 0));
        let mut model = Self::new(capacity, small, max_frequency);
        model.ghost_capacity = capacity * ghost_percent / 100;
        model.sample_every = sample_every.max(1);
        model.keep_on_promotion = keep;
        model.victim_window = victim_window;
        model.ghost_generations = ghost_generation_bps.map(|bps| GhostGenerations {
            sets: (0..ghost_generation_count)
                .map(|_| HashSet::new())
                .collect(),
            current: 0,
            rotate_every: (capacity * bps / 10_000).max(1),
        });
        if tinylfu {
            model.sketch = Some(ModelSketch::new(capacity));
            model.sketch_sample_every = sketch_sample_every.max(1);
        }
        if climb {
            model.climber = Some(ModelClimber::new(capacity));
        }
        model
    }

    fn new(capacity: usize, small_capacity: usize, max_frequency: u8) -> Self {
        assert!(small_capacity < capacity && max_frequency > 0);
        Self {
            capacity,
            small_capacity,
            max_frequency,
            ghost_capacity: capacity - small_capacity,
            sample_every: 1,
            hits: 0,
            resident: HashMap::with_capacity(capacity),
            slots: Vec::with_capacity(capacity),
            free_slots: Vec::new(),
            hand: 0,
            small: VecDeque::with_capacity(small_capacity),
            ghost: VecDeque::new(),
            ghost_counts: HashMap::new(),
            sketch: None,
            sketch_sample_every: 1,
            sketch_hits: 0,
            climber: None,
            keep_on_promotion: false,
            victim_window: None,
            ghost_generations: None,
        }
    }

    fn take_ghost(&mut self, id: u64) -> bool {
        if let Some(generations) = &self.ghost_generations {
            return generations.contains(id);
        }
        let Some(count) = self.ghost_counts.get_mut(&id) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            self.ghost_counts.remove(&id);
        }
        // The stale FIFO position expires harmlessly: its count is gone.
        true
    }

    fn remember_ghost(&mut self, id: u64) {
        if let Some(generations) = &mut self.ghost_generations {
            generations.remember(id);
            return;
        }
        *self.ghost_counts.entry(id).or_default() += 1;
        self.ghost.push_back(id);
        while self.ghost.len() > self.ghost_capacity {
            let old = self.ghost.pop_front().expect("ghost FIFO is non-empty");
            if let Some(count) = self.ghost_counts.get_mut(&old) {
                *count -= 1;
                if *count == 0 {
                    self.ghost_counts.remove(&old);
                }
            }
        }
    }

    fn place_in_main(&mut self, id: u64) {
        let keep = self.keep_on_promotion;
        let slot = if let Some(slot) = self.free_slots.pop() {
            self.slots[slot] = id;
            slot
        } else {
            self.slots.push(id);
            self.slots.len() - 1
        };
        let entry = self.resident.get_mut(&id).expect("placed key is resident");
        entry.frequency = if keep {
            entry.frequency.saturating_sub(1)
        } else {
            0
        };
        entry.slot = Some(slot);
    }

    fn evict_one(&mut self) {
        loop {
            let main_len = self.slots.len() - self.free_slots.len();
            if self.small_capacity != 0
                && (self.small.len() >= self.small_capacity || main_len == 0)
            {
                let id = self.small.pop_front().expect("small FIFO is non-empty");
                if self.resident[&id].frequency > 0 {
                    self.place_in_main(id);
                    continue;
                }
                if self.sketch.is_some() && main_len != 0 {
                    let slot = self.next_main_victim();
                    let victim = self.slots[slot];
                    let sketch = self.sketch.as_ref().expect("checked above");
                    if sketch.estimate(id) > sketch.estimate(victim) {
                        // Pass the freed slot so its new key gets a full turn.
                        self.hand = (self.hand + 1) % self.slots.len();
                        self.resident.remove(&victim);
                        self.slots[slot] = EMPTY_SLOT;
                        self.free_slots.push(slot);
                        self.place_in_main(id);
                        return;
                    }
                }
                self.resident.remove(&id);
                self.remember_ghost(id);
                return;
            }
            if let Some(window) = self.victim_window {
                self.evict_from_window(window);
                return;
            }
            let id = self.slots[self.hand];
            let slot = self.hand;
            self.hand = (self.hand + 1) % self.slots.len();
            if id == EMPTY_SLOT {
                continue;
            }
            let entry = self.resident.get_mut(&id).expect("slot key is resident");
            if entry.frequency > 0 {
                entry.frequency -= 1;
                continue;
            }
            self.resident.remove(&id);
            self.slots[slot] = EMPTY_SLOT;
            self.free_slots.push(slot);
            return;
        }
    }

    /// The packed cache's bounded sweep: the hand inspects `window` keys,
    /// taking one mark from each, and evicts the first unmarked one, or the
    /// least-marked one if every inspected key was marked.
    fn evict_from_window(&mut self, window: usize) {
        let mut chosen: Option<(u8, usize)> = None;
        let mut inspected = 0;
        while inspected < window {
            let slot = self.hand;
            let id = self.slots[slot];
            self.hand = (self.hand + 1) % self.slots.len();
            if id == EMPTY_SLOT {
                continue;
            }
            inspected += 1;
            let entry = self.resident.get_mut(&id).expect("slot key is resident");
            let marks = entry.frequency;
            entry.frequency = entry.frequency.saturating_sub(1);
            if chosen.is_none_or(|(best, _)| marks < best) {
                chosen = Some((marks, slot));
            }
            if marks == 0 {
                break;
            }
        }
        let (_, slot) = chosen.expect("main holds a key");
        let id = self.slots[slot];
        self.resident.remove(&id);
        self.slots[slot] = EMPTY_SLOT;
        self.free_slots.push(slot);
    }

    /// Advances the CLOCK hand to the next unmarked main key, clearing marks
    /// on the way, without evicting it.
    fn next_main_victim(&mut self) -> usize {
        loop {
            let id = self.slots[self.hand];
            if id != EMPTY_SLOT {
                let entry = self.resident.get_mut(&id).expect("slot key is resident");
                if entry.frequency == 0 {
                    return self.hand;
                }
                entry.frequency -= 1;
            }
            self.hand = (self.hand + 1) % self.slots.len();
        }
    }

    fn access(&mut self, id: u64) -> bool {
        let hit = self.lookup_or_admit(id);
        if let Some(sketch) = &mut self.sketch {
            if hit {
                self.sketch_hits += 1;
                if self.sketch_hits.is_multiple_of(self.sketch_sample_every) {
                    sketch.increment(id);
                }
            } else {
                sketch.increment(id);
            }
        }
        if let Some(adjustment) = self
            .climber
            .as_mut()
            .and_then(|climber| climber.record(hit))
        {
            let limit = self.capacity * 4 / 5;
            let step = adjustment.round().abs();
            // A step is a few percent of the capacity, so it fits usize.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let step = step as usize;
            self.small_capacity = if adjustment < 0.0 {
                self.small_capacity.saturating_sub(step).max(1)
            } else {
                (self.small_capacity + step).min(limit)
            };
        }
        hit
    }

    fn lookup_or_admit(&mut self, id: u64) -> bool {
        if let Some(entry) = self.resident.get_mut(&id) {
            self.hits += 1;
            if self.hits.is_multiple_of(self.sample_every) {
                entry.frequency = (entry.frequency + 1).min(self.max_frequency);
            }
            return true;
        }
        if self.resident.len() == self.capacity {
            self.evict_one();
        }
        self.resident.insert(
            id,
            ModelEntry {
                frequency: 0,
                slot: None,
            },
        );
        if self.small_capacity == 0 || self.take_ghost(id) {
            self.place_in_main(id);
        } else {
            self.small.push_back(id);
            // Before the cache first fills, the small FIFO's overflow moves to
            // main, as QuickCache admits straight to hot while hot has room and
            // the packed cache drops probation records beyond its limit.
            if self.resident.len() < self.capacity && self.small.len() > self.small_capacity {
                let oldest = self.small.pop_front().expect("small FIFO is non-empty");
                self.place_in_main(oldest);
            }
        }
        false
    }
}

impl Session for ClockModel {
    fn read(&mut self, id: u64) -> bool {
        self.access(id)
    }

    fn write(&mut self, id: u64) {
        self.access(id);
    }

    fn resident(&self) -> usize {
        self.resident.len()
    }
}

fn run_policy(policy: &str, trace: &Trace, entries: usize) -> Outcome {
    match policy {
        "quick" => run_quick(trace, entries),
        "quick-sync" => replay(
            trace,
            &mut QuickSyncSession(quick_cache::sync::Cache::with_weighter(
                entries,
                u64::try_from(entries).expect("entries fit u64") * CHARGE,
                ConstantWeighter,
            )),
        ),
        sharded if sharded.starts_with("sharded") => {
            let builder = ShardedCacheBuilder::new(entries);
            let builder = match sharded
                .split('/')
                .find_map(|option| option.strip_prefix("probation="))
            {
                Some(bps) => {
                    builder.probation_bps(bps.parse().expect("probation= takes basis points"))
                }
                None => builder,
            };
            let builder = match sharded
                .split('/')
                .find_map(|option| option.strip_prefix("shards="))
            {
                Some(shards) => builder.shards(shards.parse().expect("shards= takes a count")),
                None => builder,
            };
            replay(trace, &mut ShardedSession(builder.build()))
        }
        "lru" => replay(trace, &mut Lru::new(entries)),
        model if model.starts_with("model-") => {
            replay(trace, &mut ClockModel::parse(model, entries))
        }
        "opt" => {
            let mut opt = Opt::new(trace, entries);
            let measured = Trace {
                preload: 0..0,
                requests: trace.requests.clone(),
            };
            replay(&measured, &mut opt)
        }
        packed => run_packed(packed, trace, entries),
    }
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        arguments.len() >= 4,
        "usage: hit_ratio_sim TRACE ENTRIES OPERATIONS SEED [POLICY...]"
    );
    let entries: u64 = arguments[1].parse().expect("ENTRIES is an integer");
    let operations: u64 = arguments[2].parse().expect("OPERATIONS is an integer");
    let seed: u64 = arguments[3].parse().expect("SEED is an integer");
    assert!(entries >= 64, "use at least 64 entries");
    let trace = build_trace(&arguments[0], entries, operations, seed);
    let capacity = usize::try_from(entries).expect("entries fit usize");
    let policies: Vec<&str> = if arguments.len() > 4 {
        arguments[4..].iter().map(String::as_str).collect()
    } else {
        ALL_POLICIES.to_vec()
    };
    println!(
        "trace,entries,operations,seed,policy,reads,hit_ratio,miss_ratio,\
         steady_hit_ratio,mean_resident,steady_mean_resident,rejected"
    );
    for policy in policies {
        let outcome = run_policy(policy, &trace, capacity);
        println!(
            "{},{entries},{},{seed},{policy},{},{:.5},{:.5},{:.5},{:.1},{:.1},{}",
            arguments[0],
            trace.requests.len(),
            outcome.reads,
            outcome.hit_ratio(),
            1.0 - outcome.hit_ratio(),
            outcome.steady_hit_ratio(),
            ratio(outcome.resident_sum, outcome.resident_samples),
            ratio(outcome.steady_resident_sum, outcome.steady_resident_samples),
            outcome.rejected,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reads(ids: &[u64]) -> Trace {
        Trace {
            preload: 0..0,
            requests: ids.iter().copied().map(read).collect(),
        }
    }

    #[test]
    fn arc_lines_expand_into_consecutive_block_reads() {
        let path =
            std::env::temp_dir().join(format!("hit_ratio_sim_arc_{}.lis", std::process::id()));
        std::fs::write(&path, "10 3 0 0\n7 1 0 1\n").unwrap();
        let trace = arc(path.to_str().unwrap(), 0);
        std::fs::remove_file(&path).unwrap();
        let ids: Vec<u64> = trace.requests.iter().map(|request| request.id).collect();
        assert_eq!(ids, [10, 11, 12, 7]);
        assert!(trace.requests.iter().all(|request| !request.write));
    }

    #[test]
    fn lru_evicts_the_least_recent_key() {
        let outcome = replay(&reads(&[1, 2, 1, 3, 1, 2]), &mut Lru::new(2));
        // 1 miss, 2 miss, 1 hit, 3 evicts 2, 1 hit, 2 miss.
        assert_eq!((outcome.reads, outcome.hits), (6, 2));
    }

    #[test]
    fn opt_evicts_the_key_used_furthest_ahead() {
        let trace = reads(&[1, 2, 3, 1, 2, 1, 3]);
        let outcome = run_policy("opt", &trace, 2);
        // 3 bypasses (used after 1 and 2), then 1 and 2 hit, 1 hits, 3 misses.
        assert_eq!(outcome.hits, 3);
    }

    #[test]
    fn a_cyclic_scan_defeats_lru_but_not_opt() {
        let trace = cyclic(64, 2_000);
        assert_eq!(run_policy("lru", &trace, 64).hits, 0);
        assert_eq!(run_policy("model-clock", &trace, 64).hits, 0);
        assert!(run_policy("opt", &trace, 64).hits > 1_000);
    }

    #[test]
    fn opt_bounds_every_online_policy() {
        let traces = [
            turnover(256, 20_000, 7),
            zipf(256, 20_000, 7, 0.9),
            scan_zipf(256, 20_000, 7, 0.9),
            shift(256, 20_000, 7, 0.9),
        ];
        for trace in &traces {
            let optimal = run_policy("opt", trace, 256).hits;
            for policy in ALL_POLICIES {
                let outcome = run_policy(policy, trace, 256);
                assert!(
                    outcome.hits <= optimal,
                    "{policy} hit {} times, above the offline bound {optimal}",
                    outcome.hits
                );
                assert!(outcome.hits > 0, "{policy} never hit");
            }
        }
    }

    #[test]
    fn packed_default_beats_exact_lru_and_tracks_quickcache() {
        let entries = 4_096;
        for trace_name in ["turnover", "zipf:0.9", "scan-zipf:0.9", "shift:0.9"] {
            let trace = build_trace(trace_name, 4_096, 150_000, 3);
            let packed = run_policy("packed", &trace, entries).hit_ratio();
            // On `shift` recency wins: exact LRU is within 1.1 points of
            // QuickCache there, and only a ghost of a full capacity, which
            // costs ten points on DS1, kept the packed cache above it.
            if trace_name != "shift:0.9" {
                let lru = run_policy("lru", &trace, entries).hit_ratio();
                assert!(
                    packed > lru,
                    "{trace_name}: packed hit ratio {packed:.4} is not above exact LRU {lru:.4}"
                );
            }
            // Measured at 0.997-1.033; the original sampled CLOCK was 1.25.
            let quick = run_policy("quick", &trace, entries).hit_ratio();
            assert!(
                1.0 - packed <= 1.06 * (1.0 - quick),
                "{trace_name}: packed misses {:.4} exceed QuickCache's {:.4} by over 6%",
                1.0 - packed,
                1.0 - quick
            );
        }
    }

    #[test]
    fn replays_are_reproducible() {
        let trace = turnover(256, 20_000, 11);
        for policy in ALL_POLICIES {
            let first = run_policy(policy, &trace, 256);
            let second = run_policy(policy, &trace, 256);
            if policy.starts_with("packed")
                || policy.starts_with("sharded")
                || policy == "quick-sync"
            {
                // Each cache seeds its index hasher randomly, which changes
                // the victim scan order; the policy itself must stay stable.
                assert!(
                    first.hits.abs_diff(second.hits) * 100 <= first.reads,
                    "{policy} varied by more than 1% of reads: {first:?} vs {second:?}"
                );
            } else {
                assert_eq!(first, second, "{policy} changed between identical replays");
            }
        }
    }
}
