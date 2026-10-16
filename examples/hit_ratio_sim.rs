//! Deterministic single-threaded hit-ratio simulator.
//!
//! Usage: `hit_ratio_sim TRACE ENTRIES OPERATIONS SEED [POLICY...]`.
//!
//! TRACE is `turnover`, `zipf:ALPHA`, `scan-zipf:ALPHA`, `shift:ALPHA`, `loop`,
//! or `file:PATH` (one key per line; `OPERATIONS` of zero replays the whole
//! file). POLICY defaults to every policy: `packed`, `packed-doorkeeper`,
//! `packed-adaptive`, `packed-tiered`, `quick`, `lru`, `opt`, and the reference
//! models `model-clock`, `model-s3clock`, and `model-s3fifo`. One CSV row is
//! printed per policy. A packed policy accepts `/batch=N`, `/probation=BPS`,
//! and `/maint=N` (see `packed_config`); a model accepts `/small=PCT`, `/ghost=PCT`, and `/sample=N` (see
//! `ClockModel::parse`).
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

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::ops::Range;

use packedgen::{CacheConfig, DirectCacheGuard, DirectPackedCache};
use quick_cache::Weighter;
use quick_cache::unsync::{Cache as QuickCache, DefaultLifecycle};

type Key = [u8; 32];
const VALUE_BYTES: u64 = 64;
const CHARGE: u64 = 32 + VALUE_BYTES;
const REFRESH_INTERVAL: usize = 512;
const MAINTENANCE_INTERVAL: usize = 4096;
const OCCUPANCY_STRIDE: usize = 64;
const ALL_POLICIES: [&str; 10] = [
    "packed",
    "packed-doorkeeper",
    "packed-adaptive",
    "packed-tiered",
    "quick",
    "lru",
    "opt",
    "model-clock",
    "model-s3clock",
    "model-s3fifo",
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

fn build_trace(name: &str, entries: u64, operations: u64, seed: u64) -> Trace {
    let alpha = |spec: &str| -> f64 { spec.parse().expect("ALPHA is a number") };
    match name.split_once(':') {
        None if name == "turnover" => turnover(entries, operations, seed),
        None if name == "loop" => cyclic(entries, operations),
        Some(("zipf", spec)) => zipf(entries, operations, seed, alpha(spec)),
        Some(("scan-zipf", spec)) => scan_zipf(entries, operations, seed, alpha(spec)),
        Some(("shift", spec)) => shift(entries, operations, seed, alpha(spec)),
        Some(("file", path)) => file(path, operations),
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

/// `POLICY[/batch=N][/probation=BPS][/maint=N]`: the eviction batch (victim
/// window), the probationary share, and writes between `maintain()` calls
/// (zero never maintains). Returns the config and maintenance interval.
fn packed_config(policy: &str, entries: usize) -> (CacheConfig, usize) {
    let mut base = CacheConfig::new(u64::try_from(entries).expect("entries fit u64") * CHARGE)
        .with_max_entries(entries)
        .with_overlay_capacity(entries.max(64));
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
}

impl ClockModel {
    /// `NAME[/small=PCT][/ghost=PCT][/sample=N]`: small FIFO and ghost FIFO
    /// sizes are percentages of capacity; `sample` keeps every Nth hit.
    fn parse(name: &str, capacity: usize) -> Self {
        let mut parts = name.split('/');
        let (mut small_percent, max_frequency) = match parts.next() {
            Some("model-clock") => (0, 1),
            Some("model-s3fifo") => (10, 3),
            Some("model-s3clock") => (10, 1),
            _ => panic!("unknown model: {name}"),
        };
        let mut ghost_percent = 100 - small_percent;
        let mut sample_every = 1;
        for part in parts {
            let (option, value) = part.split_once('=').expect("model options are NAME=VALUE");
            let value: usize = value.parse().expect("model option values are integers");
            match option {
                "small" => small_percent = value,
                "ghost" => ghost_percent = value,
                "sample" => sample_every = u64::try_from(value).expect("sample fits u64"),
                _ => panic!("unknown model option: {option}"),
            }
        }
        let mut model = Self::new(capacity, capacity * small_percent / 100, max_frequency);
        model.ghost_capacity = capacity * ghost_percent / 100;
        model.sample_every = sample_every.max(1);
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
        }
    }

    fn take_ghost(&mut self, id: u64) -> bool {
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
        let slot = if let Some(slot) = self.free_slots.pop() {
            self.slots[slot] = id;
            slot
        } else {
            self.slots.push(id);
            self.slots.len() - 1
        };
        let entry = self.resident.get_mut(&id).expect("placed key is resident");
        entry.frequency = 0;
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
                self.resident.remove(&id);
                self.remember_ghost(id);
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

    fn access(&mut self, id: u64) -> bool {
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
            let lru = run_policy("lru", &trace, entries).hit_ratio();
            assert!(
                packed > lru,
                "{trace_name}: packed hit ratio {packed:.4} is not above exact LRU {lru:.4}"
            );
            if trace_name == "shift:0.9" {
                continue;
            }
            // Measured at 1.003-1.030; the original sampled CLOCK was 1.25.
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
            if policy.starts_with("packed") {
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
