//! A sharded concurrent cache with lock-free reads.
//!
//! Each shard is a fixed table of 64-byte buckets. A bucket holds six slots:
//! one control word with a tag byte per slot and an overflow count, and six
//! atomic handles to entries in an epoch-reclaimed arena. Keys live in the
//! entries, not in the table, and an entry never changes after publication,
//! so a reader compares keys only in memory that its epoch pin keeps alive.
//! Writers can therefore reuse a freed slot immediately: a reader racing the
//! reuse sees an old or a new handle, both live for its epoch, and checks the
//! key in that entry. Tables are sized once for the shard's capacity and never
//! rebuilt.
//!
//! Writers take their shard's mutex. A shard at its limit evicts in the
//! S3-FIFO style: new keys wait in a small probationary FIFO and leave unless
//! read; keys the ghost filter remembers go straight to the main area, which
//! a CLOCK hand sweeps over the table's own slots.

use std::borrow::Borrow;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use hashbrown::DefaultHashBuilder;
use parking_lot::Mutex;

use crate::NonMaxU64;
use crate::cache_arena::{
    DIRECT_MAX_WEIGHT, DirectArenaEntry, DirectArenaPin, DirectHandle, DirectValueArena,
    RemovedDirectHandle,
};

const SLOTS: usize = 6;
const TAG_MASK: u64 = 0xff;
const OVERFLOW_SHIFT: u32 = 48;
const OVERFLOW_MAX: u64 = 0xff;
/// Table slots per entry of capacity: about 80% load when the shard is full.
const SLOTS_PER_ENTRY_NUMERATOR: usize = 5;
const SLOTS_PER_ENTRY_DENOMINATOR: usize = 4;
/// Shards hold at least this many entries, so small caches use fewer shards.
const MIN_SHARD_CAPACITY: usize = 64;
/// Main-queue records encode a slot in 24 bits.
const MAX_SHARD_SLOTS: usize = 1 << 24;
/// Stale main-queue records examined per cleanup step.
const MAIN_CLEANUP_CHUNK: usize = 64;
const NEVER_EXPIRES: u64 = u64::MAX;
/// Probation's default share of each shard, in basis points.
const DEFAULT_PROBATION_BPS: u16 = 200;
/// The ghost holds this many generations of evicted probationary keys.
const GHOST_GENERATIONS: usize = 4;
/// Each ghost generation holds this fraction of the shard's capacity.
const GHOST_GENERATION_DIVISOR: usize = 8;
const GHOST_BITS_PER_KEY: usize = 32;
const GHOST_KEY_BITS: u32 = 5;

/// Charges each entry against a cache's weight limit.
pub trait Weighter<K, V> {
    /// The weight of one entry. Weights above `u32::MAX >> 1` are never
    /// admitted.
    fn weight(&self, key: &K, value: &V) -> u64;
}

impl<K, V, F> Weighter<K, V> for F
where
    F: Fn(&K, &V) -> u64,
{
    fn weight(&self, key: &K, value: &V) -> u64 {
        self(key, value)
    }
}

/// Weighs every entry as one.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnitWeighter;

impl<K, V> Weighter<K, V> for UnitWeighter {
    fn weight(&self, _key: &K, _value: &V) -> u64 {
        1
    }
}

/// Configures a [`ShardedCache`].
///
/// ```
/// use packedgen::{ShardedCache, ShardedCacheBuilder};
///
/// let cache: ShardedCache<u64, String> = ShardedCache::new(10_000);
/// cache.insert(1, "one".to_owned());
/// assert_eq!(cache.get(&1).as_deref(), Some("one"));
///
/// // Weighted limits, and borrowed reads that pin once for many lookups.
/// let weighted = ShardedCacheBuilder::new(10_000)
///     .max_weight(1 << 20)
///     .weighter(|_key: &u64, value: &String| value.len() as u64)
///     .build();
/// weighted.insert(2, "two".to_owned());
/// let guard = weighted.pin();
/// assert_eq!(guard.get(&2).map(String::as_str), Some("two"));
/// ```
#[derive(Clone, Debug)]
pub struct ShardedCacheBuilder<W = UnitWeighter, S = DefaultHashBuilder> {
    max_entries: usize,
    max_weight: Option<u64>,
    probation_bps: u16,
    shards: Option<usize>,
    weighter: W,
    hash_builder: S,
}

impl ShardedCacheBuilder {
    /// Starts a cache holding at most `max_entries` entries.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            max_weight: None,
            probation_bps: DEFAULT_PROBATION_BPS,
            shards: None,
            weighter: UnitWeighter,
            hash_builder: DefaultHashBuilder::default(),
        }
    }
}

impl<W, S> ShardedCacheBuilder<W, S> {
    /// Also limits the summed weight of the entries; see [`Self::weighter`].
    #[must_use]
    pub fn max_weight(mut self, max_weight: u64) -> Self {
        self.max_weight = Some(max_weight);
        self
    }

    /// Sets the probationary FIFO's share of each shard in basis points
    /// (default 200).
    #[must_use]
    pub fn probation_bps(mut self, probation_bps: u16) -> Self {
        self.probation_bps = probation_bps;
        self
    }

    /// Sets the shard count, rounded up to a power of two. By default it is
    /// four per available thread, and fewer for small caches.
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = Some(shards.max(1).next_power_of_two());
        self
    }

    /// Weighs entries with `weighter`.
    #[must_use]
    pub fn weighter<W2>(self, weighter: W2) -> ShardedCacheBuilder<W2, S> {
        ShardedCacheBuilder {
            max_entries: self.max_entries,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            weighter,
            hash_builder: self.hash_builder,
        }
    }

    /// Hashes keys with `hash_builder`.
    #[must_use]
    pub fn hasher<S2>(self, hash_builder: S2) -> ShardedCacheBuilder<W, S2> {
        ShardedCacheBuilder {
            max_entries: self.max_entries,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            weighter: self.weighter,
            hash_builder,
        }
    }

    /// Builds the cache.
    ///
    /// # Panics
    ///
    /// Panics if one shard would need 2^32 or more table slots.
    #[must_use]
    pub fn build<K, V>(self) -> ShardedCache<K, V, W, S>
    where
        K: Hash + Eq,
        W: Weighter<K, V>,
        S: BuildHasher,
    {
        let max_entries = self.max_entries.max(1);
        let shards = self.shards.unwrap_or_else(|| {
            let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
            (parallelism * 4).next_power_of_two().min(
                (max_entries / MIN_SHARD_CAPACITY)
                    .max(1)
                    .next_power_of_two(),
            )
        });
        // Main-queue records hold a slot in 24 bits.
        let mut shards = shards;
        while (max_entries / shards).saturating_mul(SLOTS_PER_ENTRY_NUMERATOR)
            / SLOTS_PER_ENTRY_DENOMINATOR
            >= MAX_SHARD_SLOTS - SLOTS
        {
            shards *= 2;
        }
        let max_weight = self.max_weight.unwrap_or(u64::MAX);
        let shard_count = u64::try_from(shards).expect("the shard count fits u64");
        let base = max_entries / shards;
        let extra = max_entries % shards;
        ShardedCache {
            shards: (0..shards)
                .map(|shard| {
                    let index = u64::try_from(shard).expect("a shard index fits u64");
                    let weight_limit = if max_weight == u64::MAX {
                        u64::MAX
                    } else {
                        max_weight / shard_count + u64::from(index < max_weight % shard_count)
                    };
                    Shard::new(
                        base + usize::from(shard < extra),
                        weight_limit,
                        self.probation_bps,
                    )
                })
                .collect(),
            shard_shift: u64::BITS - shards.trailing_zeros(),
            arena: DirectValueArena::new_stashing(64),
            weighter: self.weighter,
            hash_builder: self.hash_builder,
            capacity: max_entries,
            max_weight,
        }
    }
}

/// A bounded concurrent cache with lock-free reads and sharded writers.
///
/// `get` clones the value, so `V` is typically small or an `Arc`; [`Self::pin`]
/// borrows values instead. Limits apply per shard: each holds its share of
/// the entry and weight limits, as in other sharded caches, so a cache can
/// evict a little before it is full overall.
pub struct ShardedCache<K, V, W = UnitWeighter, S = DefaultHashBuilder> {
    shards: Box<[Shard]>,
    shard_shift: u32,
    arena: DirectValueArena<(K, V)>,
    weighter: W,
    hash_builder: S,
    capacity: usize,
    max_weight: u64,
}

#[repr(C, align(64))]
struct Bucket {
    /// Tag bytes for the six slots (zero marks an empty slot), then the
    /// count of keys that probed past this bucket.
    control: AtomicU64,
    handles: [AtomicU64; SLOTS],
    /// A byte per slot counting its placements, so a main-queue record of
    /// a slot can tell its key from a later one. Only writers use it.
    generations: AtomicU64,
}

/// Readers load only `buckets`; the counters and the writer lock change on
/// every write, so each sits on its own cache line.
struct Shard {
    buckets: Box<[Bucket]>,
    weight_limit: u64,
    counters: ShardCounters,
    writer: ShardLock,
}

#[repr(align(128))]
struct ShardCounters {
    len: AtomicUsize,
    weight: AtomicU64,
}

#[repr(align(128))]
struct ShardLock(Mutex<ShardWriter>);

struct ShardWriter {
    len: usize,
    capacity: usize,
    weight: u64,
    weight_limit: u64,
    /// The CLOCK hand: the next slot the main-area sweep examines.
    hand: usize,
    /// Probationary keys in insertion order, as (slot record, key hash).
    /// A key moved to its other bucket leaves probation for the main area.
    probation: VecDeque<(u32, u64)>,
    probation_target: usize,
    /// Past this many records the oldest is dropped, leaving its key to the
    /// main sweep, so records never copy more than a share of the keys.
    probation_limit: usize,
    /// Main-area keys in the order they entered it, as `slot << 8 |
    /// generation`. Stale records (removed, replaced by another key, or
    /// moved) are skipped when popped.
    main: VecDeque<u32>,
    ghost: Ghost,
}

impl ShardWriter {
    /// Appends a slot record to the main area's queue. Past its quarter of
    /// slack, stale records are dropped from the front in order-preserving
    /// chunks, so no write holds the shard lock for a whole-queue pass; only
    /// a chunk with nothing stale falls back to compacting the whole queue.
    fn enter_main(&mut self, shard: &Shard, record: u32) {
        if self.main.len() >= self.main_limit() {
            let mut kept = [0_u32; MAIN_CLEANUP_CHUNK];
            let mut live = 0;
            let mut examined = 0;
            while examined < MAIN_CLEANUP_CHUNK {
                let Some(front) = self.main.pop_front() else {
                    break;
                };
                examined += 1;
                if shard.live_record(front).is_some() {
                    kept[live] = front;
                    live += 1;
                }
            }
            for front in kept[..live].iter().rev() {
                self.main.push_front(*front);
            }
            if live == examined {
                self.main
                    .retain(|record| shard.live_record(*record).is_some());
            }
        }
        self.main.push_back(record);
    }

    fn main_limit(&self) -> usize {
        self.capacity + self.capacity / 4 + 16
    }

    fn over_limit(&self, incoming_weight: u64) -> bool {
        self.len >= self.capacity || self.weight.saturating_add(incoming_weight) > self.weight_limit
    }

    fn publish(&self, shard: &Shard) {
        shard.counters.len.store(self.len, Ordering::Relaxed);
        shard.counters.weight.store(self.weight, Ordering::Relaxed);
    }
}

impl<K: Hash + Eq, V> ShardedCache<K, V> {
    /// Creates a cache holding at most `max_entries` entries.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        ShardedCacheBuilder::new(max_entries).build()
    }
}

impl<K: Hash + Eq, V, W: Weighter<K, V>, S: BuildHasher> ShardedCache<K, V, W, S> {
    /// The configured entry limit.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The configured weight limit; `u64::MAX` when weight is unlimited.
    #[must_use]
    pub fn max_weight(&self) -> u64 {
        self.max_weight
    }

    /// The number of resident entries; a weak snapshot under concurrency.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.counters.len.load(Ordering::Relaxed))
            .sum()
    }

    /// The summed weight of resident entries; a weak snapshot.
    #[must_use]
    pub fn weight(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.counters.weight.load(Ordering::Relaxed))
            .sum()
    }

    /// Whether the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a clone of the value cached for `key`.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        let pin = self.arena.pin();
        self.lookup(&pin, key).cloned()
    }

    /// Pins the cache for a run of borrowed reads.
    ///
    /// Values a guard returns stay valid until it is refreshed or dropped,
    /// even if they are replaced or evicted meanwhile. Entries retired while
    /// a guard is held are not reclaimed until it refreshes, so long-lived
    /// guards should call [`ShardedCacheGuard::refresh`] periodically.
    #[must_use]
    pub fn pin(&self) -> ShardedCacheGuard<'_, K, V, W, S> {
        ShardedCacheGuard {
            cache: self,
            pin: self.arena.pin(),
        }
    }

    #[allow(
        unsafe_code,
        reason = "dereferences handles the table publishes under the caller's pin"
    )]
    fn lookup<'pin, Q>(&self, pin: &'pin DirectArenaPin<'_, (K, V)>, key: &Q) -> Option<&'pin V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let (_, handle) = self.shard(hash).find(hash, |handle| {
            // SAFETY: `find` passes handles published in this cache's table
            // while `pin` was held.
            unsafe { entry(pin, handle) }.value().0.borrow() == key
        })?;
        // SAFETY: as above.
        let entry = unsafe { entry(pin, handle) };
        entry.mark_accessed();
        Some(&entry.value().1)
    }

    /// Inserts or replaces the value for `key`, evicting from the key's shard
    /// while it is over its limits.
    ///
    /// A value heavier than its shard's share of the weight limit, or than
    /// `u32::MAX >> 1`, is not cached; any previous value for `key` is then
    /// removed, so a reader never sees a value the caller replaced. Returns
    /// whether the value was cached.
    ///
    /// # Panics
    ///
    /// Only on a bug: a placement that fails after freeing one of the key's
    /// two buckets.
    #[allow(
        unsafe_code,
        reason = "publishes, replaces, and retires arena handles under the shard lock"
    )]
    pub fn insert(&self, key: K, value: V) -> bool {
        let hash = self.hash_builder.hash_one(&key);
        let shard = self.shard(hash);
        let weight = self.weighter.weight(&key, &value);
        let Some(compact_weight) = u32::try_from(weight).ok().filter(|weight| {
            *weight <= DIRECT_MAX_WEIGHT && u64::from(*weight) <= shard.weight_limit
        }) else {
            self.remove(&key);
            return false;
        };
        let pin = self.arena.pin();
        let handle = self
            .arena
            .allocate_recycled((key, value), compact_weight, NEVER_EXPIRES);
        // SAFETY: the handle is this arena's own unpublished allocation.
        let key = &unsafe { entry(&pin, handle) }.value().0;
        let same_key = |current| {
            // SAFETY: published in this table while `pin` is held.
            unsafe { entry(&pin, current) }.value().0 == *key
        };
        let mut retired = Retired::new();
        {
            let mut writer = shard.writer.0.lock();
            if let Some((slot, _)) = shard.find(hash, same_key) {
                let previous = handle_from_raw(
                    shard.bucket(slot).handles[slot % SLOTS]
                        .swap(handle.index_value().get(), Ordering::AcqRel),
                );
                // SAFETY: the swap unpublished `previous` while `pin` is held.
                writer.weight = writer.weight - unsafe { entry(&pin, previous) }.weight() + weight;
                retired.push(previous);
                while writer.weight > writer.weight_limit {
                    let Some(victim) = self.evict(shard, &mut writer, &pin, Some(handle)) else {
                        break;
                    };
                    retired.push(victim);
                }
            } else {
                while writer.over_limit(weight) {
                    let Some(victim) = self.evict(shard, &mut writer, &pin, None) else {
                        break;
                    };
                    retired.push(victim);
                }
                let slot = if let Some(slot) = shard.place(hash, handle) {
                    slot
                } else {
                    // Both candidate buckets are full: move a resident key to
                    // its other bucket, or else evict one of them.
                    if !self.relocate_candidate(shard, &mut writer, &pin, hash) {
                        #[cfg(test)]
                        WINDOW_EVICTIONS.with(|count| count.set(count.get() + 1));
                        retired.push(self.evict_candidate(shard, &mut writer, &pin, hash));
                    }
                    let placed = shard.place(hash, handle);
                    assert!(
                        placed.is_some(),
                        "relocation or eviction frees a candidate slot"
                    );
                    placed.unwrap_or_default()
                };
                writer.len += 1;
                writer.weight += weight;
                let record = shard.record(slot);
                if writer.ghost.contains(hash) {
                    // Remembered from a recent eviction: straight to main.
                    writer.enter_main(shard, record);
                } else {
                    if writer.probation.len() >= writer.probation_limit {
                        // An overflowing record's key joins the main area.
                        let (overflow, _) = writer
                            .probation
                            .pop_front()
                            .expect("probation is at its limit");
                        writer.enter_main(shard, overflow);
                    }
                    writer.probation.push_back((record, hash));
                }
            }
            writer.publish(shard);
        }
        retired.retire(&pin);
        true
    }

    /// Removes `key`, returning whether it was present.
    #[allow(
        unsafe_code,
        reason = "dereferences and retires handles removed under the shard lock"
    )]
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let shard = self.shard(hash);
        let pin = self.arena.pin();
        let removed = {
            let mut writer = shard.writer.0.lock();
            let found = shard.find(hash, |handle| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, handle) }.value().0.borrow() == key
            });
            found.map(|(slot, _)| {
                let removed = shard.clear(slot, shard.home(hash));
                writer.len -= 1;
                // SAFETY: the clear unpublished `removed` while `pin` is held.
                writer.weight -= unsafe { entry(&pin, removed) }.weight();
                writer.publish(shard);
                removed
            })
        };
        let mut retired = Retired::new();
        if let Some(handle) = removed {
            retired.push(handle);
        }
        retired.retire(&pin);
        removed.is_some()
    }

    fn shard(&self, hash: u64) -> &Shard {
        // `checked_shr` keeps the single-shard shift of 64 in range.
        let shard = hash.checked_shr(self.shard_shift).unwrap_or(0);
        &self.shards[usize::try_from(shard).expect("a shard index fits usize")]
    }

    /// Removes one entry from a shard over its limits: an unread
    /// probationary key first, otherwise the next unread key under the CLOCK
    /// hand. Never removes `protected`.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn evict(
        &self,
        shard: &Shard,
        writer: &mut ShardWriter,
        pin: &DirectArenaPin<'_, (K, V)>,
        protected: Option<DirectHandle>,
    ) -> Option<DirectHandle> {
        while writer.probation.len() > writer.probation_target {
            let (record, hash) = writer
                .probation
                .pop_front()
                .expect("probation is above its target");
            let Some(slot) = shard.live_record(record) else {
                // Removed, or moved to its other bucket, since enrolling.
                continue;
            };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            if protected.is_some_and(|protected| protected.index_value().get() == raw) {
                continue;
            }
            let handle = handle_from_raw(raw);
            // SAFETY: the shard lock keeps this published handle in the table.
            if unsafe { entry(pin, handle) }.take_accessed() {
                // Read during probation: it joins the main area.
                writer.enter_main(shard, record);
                continue;
            }
            let removed = shard.clear(slot, shard.home(hash));
            writer.ghost.remember(hash);
            writer.len -= 1;
            // SAFETY: the clear unpublished `removed` while `pin` is held.
            writer.weight -= unsafe { entry(pin, removed) }.weight();
            return Some(removed);
        }
        // The main area is a CLOCK in the order keys entered it: a read key
        // loses one access mark and goes to the back.
        let mut budget = 3 * writer.main.len();
        while budget > 0 {
            budget -= 1;
            let Some(record) = writer.main.pop_front() else {
                break;
            };
            let Some(slot) = shard.live_record(record) else {
                continue;
            };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            let handle = handle_from_raw(raw);
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, handle) };
            if protected.is_some_and(|protected| protected.index_value().get() == raw)
                || entry.take_accessed()
            {
                writer.main.push_back(record);
                continue;
            }
            let hash = self.hash_builder.hash_one(&entry.value().0);
            let removed = shard.clear(slot, shard.home(hash));
            writer.len -= 1;
            writer.weight -= entry.weight();
            return Some(removed);
        }
        // Keys the queue does not hold, or readers re-marking every queued
        // key, fall back to a CLOCK over the table's slots.
        let slots = shard.buckets.len() * SLOTS;
        // Each pass takes one access mark from every entry, and entries hold
        // at most two, so the third pass finds an unread victim unless
        // readers keep marking; it then evicts regardless.
        for step in 0..3 * slots {
            let slot = writer.hand;
            writer.hand = if slot + 1 == slots { 0 } else { slot + 1 };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            if raw == 0 || protected.is_some_and(|protected| protected.index_value().get() == raw) {
                continue;
            }
            let handle = handle_from_raw(raw);
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, handle) };
            if entry.take_accessed() && step < 2 * slots {
                continue;
            }
            let hash = self.hash_builder.hash_one(&entry.value().0);
            let removed = shard.clear(slot, shard.home(hash));
            writer.len -= 1;
            writer.weight -= entry.weight();
            return Some(removed);
        }
        None
    }

    /// Frees a slot in the two full buckets `hash` may occupy by moving one
    /// resident key to its own other bucket. The key is published at its new
    /// slot before its old slot is cleared, so readers always find it.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn relocate_candidate(
        &self,
        shard: &Shard,
        writer: &mut ShardWriter,
        pin: &DirectArenaPin<'_, (K, V)>,
        hash: u64,
    ) -> bool {
        let (home, alternate) = shard.candidates(hash);
        for bucket in [home, alternate] {
            for slot in bucket * SLOTS..(bucket + 1) * SLOTS {
                let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
                let handle = handle_from_raw(raw);
                // SAFETY: the shard lock keeps this published handle in the table.
                let resident = unsafe { entry(pin, handle) };
                let resident_hash = self.hash_builder.hash_one(&resident.value().0);
                let (resident_home, resident_alternate) = shard.candidates(resident_hash);
                let other = if bucket == resident_home {
                    resident_alternate
                } else {
                    resident_home
                };
                if other == home || other == alternate {
                    continue;
                }
                if let Some(moved_to) = shard.place_at(other, resident_home, resident_hash, handle)
                {
                    let moved = shard.clear(slot, resident_home);
                    debug_assert_eq!(moved.index_value(), handle.index_value());
                    // Its old record no longer matches; a probationary key
                    // gains a main record it does not need, which only
                    // gives it a second life after probation.
                    writer.enter_main(shard, shard.record(moved_to));
                    return true;
                }
            }
        }
        false
    }

    /// Removes the first unread entry, sweeping access marks, from the two
    /// buckets `hash` may occupy. Used only when both are full.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn evict_candidate(
        &self,
        shard: &Shard,
        writer: &mut ShardWriter,
        pin: &DirectArenaPin<'_, (K, V)>,
        hash: u64,
    ) -> DirectHandle {
        let (home, alternate) = shard.candidates(hash);
        let slots = [home, alternate].map(|bucket| bucket * SLOTS);
        // Entries hold at most two access marks, so a third pass finds an
        // unread one unless readers keep marking; then it takes the first.
        for pass in 0..3 {
            for slot in slots.iter().flat_map(|first| *first..*first + SLOTS) {
                let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
                debug_assert_ne!(raw, 0, "both candidate buckets are full");
                // SAFETY: the shard lock keeps this published handle in the table.
                let entry = unsafe { entry(pin, handle_from_raw(raw)) };
                if entry.take_accessed() && pass < 2 {
                    continue;
                }
                let victim_home = shard.home(self.hash_builder.hash_one(&entry.value().0));
                writer.len -= 1;
                writer.weight -= entry.weight();
                return shard.clear(slot, victim_home);
            }
        }
        unreachable!("the third pass evicts its first slot")
    }
}

/// A pinned read session over a [`ShardedCache`]; see [`ShardedCache::pin`].
pub struct ShardedCacheGuard<'cache, K, V, W = UnitWeighter, S = DefaultHashBuilder> {
    cache: &'cache ShardedCache<K, V, W, S>,
    pin: DirectArenaPin<'cache, (K, V)>,
}

impl<K: Hash + Eq, V, W: Weighter<K, V>, S: BuildHasher> ShardedCacheGuard<'_, K, V, W, S> {
    /// Borrows the value cached for `key` until this guard refreshes.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.lookup(&self.pin, key)
    }

    /// Lets entries retired since the guard pinned be reclaimed.
    pub fn refresh(&mut self) {
        self.pin.refresh();
    }
}

impl<K, V, W, S> Drop for ShardedCache<K, V, W, S> {
    #[allow(
        unsafe_code,
        reason = "exclusive drop retires every handle still published in the tables"
    )]
    fn drop(&mut self) {
        let pin = self.arena.pin();
        for shard in &self.shards {
            for bucket in &shard.buckets {
                for handle in &bucket.handles {
                    let raw = handle.load(Ordering::Relaxed);
                    if raw != 0 {
                        // SAFETY: exclusive drop makes every published handle
                        // unreachable, and each slot holds a handle once.
                        unsafe {
                            pin.retire(RemovedDirectHandle::from_exact_removal(handle_from_raw(
                                raw,
                            )));
                        }
                    }
                }
            }
        }
    }
}

impl Shard {
    fn new(capacity: usize, weight_limit: u64, probation_bps: u16) -> Self {
        let probation_target = capacity * usize::from(probation_bps) / 10_000;
        let slots = (capacity.max(1) * SLOTS_PER_ENTRY_NUMERATOR)
            .div_ceil(SLOTS_PER_ENTRY_DENOMINATOR)
            .max(SLOTS * 2);
        let buckets = slots.div_ceil(SLOTS);
        assert!(
            u32::try_from(buckets * SLOTS).is_ok(),
            "a shard holds fewer than 2^32 slots"
        );
        Self {
            buckets: (0..buckets)
                .map(|_| Bucket {
                    control: AtomicU64::new(0),
                    handles: std::array::from_fn(|_| AtomicU64::new(0)),
                    generations: AtomicU64::new(0),
                })
                .collect(),
            counters: ShardCounters {
                len: AtomicUsize::new(0),
                weight: AtomicU64::new(0),
            },
            weight_limit,
            writer: ShardLock(Mutex::new(ShardWriter {
                len: 0,
                capacity,
                weight: 0,
                weight_limit,
                hand: 0,
                probation: VecDeque::new(),
                probation_target,
                probation_limit: probation_target + probation_target / 4 + 4,
                main: VecDeque::with_capacity(capacity + capacity / 4 + 17),
                ghost: Ghost::new(capacity.div_ceil(GHOST_GENERATION_DIVISOR)),
            })),
        }
    }

    fn bucket(&self, slot: usize) -> &Bucket {
        &self.buckets[slot / SLOTS]
    }

    /// Multiply-shift range reduction of 32 bits: the table need not be a
    /// power of two.
    fn reduce(&self, bits: u64) -> usize {
        let scaled = ((bits & 0xffff_ffff) * self.buckets.len() as u64) >> 32;
        usize::try_from(scaled).expect("a bucket index fits usize")
    }

    /// One multiply spreads every hash bit into the high half that picks
    /// the home: for some seeds the fast hasher's low half alone clusters.
    fn home(&self, hash: u64) -> usize {
        self.reduce(hash.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32)
    }

    fn next(&self, bucket: usize) -> usize {
        if bucket + 1 == self.buckets.len() {
            0
        } else {
            bucket + 1
        }
    }

    /// The two buckets a key may live in: its home and an alternate, so a
    /// full region never chains into the next and displacement cannot grow.
    /// The alternate comes from a full avalanche of the key hash: a fast
    /// hasher's bit halves can correlate for some seeds, and correlated
    /// choices left one seed in forty evicting from full pairs on 2.4% of
    /// inserts. Lookups reach the alternate only after the home misses, so
    /// the avalanche stays off the common path.
    fn candidates(&self, hash: u64) -> (usize, usize) {
        let home = self.home(hash);
        let alternate = self.reduce(fmix64(hash) >> 32);
        (
            home,
            if alternate == home {
                self.next(home)
            } else {
                alternate
            },
        )
    }

    /// Finds the slot whose handle `matches` in the key's home bucket, then
    /// in its alternate if any key from that home lives in its alternate.
    /// Lock-free.
    fn find(
        &self,
        hash: u64,
        mut matches: impl FnMut(DirectHandle) -> bool,
    ) -> Option<(usize, DirectHandle)> {
        let tag = tag(hash);
        let home = self.home(hash);
        let control = self.buckets[home].control.load(Ordering::Acquire);
        if let Some(found) = self.find_in(home, control, tag, &mut matches) {
            return Some(found);
        }
        if control >> OVERFLOW_SHIFT & OVERFLOW_MAX == 0 {
            return None;
        }
        let (_, alternate) = self.candidates(hash);
        let control = self.buckets[alternate].control.load(Ordering::Acquire);
        self.find_in(alternate, control, tag, &mut matches)
    }

    fn find_in(
        &self,
        bucket: usize,
        control: u64,
        tag: u8,
        matches: &mut impl FnMut(DirectHandle) -> bool,
    ) -> Option<(usize, DirectHandle)> {
        let current = &self.buckets[bucket];
        let mut candidates = zero_tag_bytes(control ^ (BYTE_ONES * u64::from(tag)));
        while candidates != 0 {
            let slot = candidates.trailing_zeros() as usize / 8;
            candidates &= candidates - 1;
            // Acquire pairs with the release store of a replacement handle,
            // which does not touch the control word.
            let raw = current.handles[slot].load(Ordering::Acquire);
            if raw != 0 {
                let handle = handle_from_raw(raw);
                if matches(handle) {
                    return Some((bucket * SLOTS + slot, handle));
                }
            }
        }
        None
    }

    /// Publishes `handle` in a free slot of the key's home bucket, or else of
    /// its alternate, counting it in the home's overflow first. Returns
    /// `None` when both are full. Requires the shard lock.
    fn place(&self, hash: u64, handle: DirectHandle) -> Option<usize> {
        let (home, alternate) = self.candidates(hash);
        if let Some(slot) = self.place_in(home, hash, handle) {
            return Some(slot);
        }
        let alternate_control = self.buckets[alternate].control.load(Ordering::Relaxed);
        if zero_tag_bytes(alternate_control) == 0 {
            return None;
        }
        let home_bucket = &self.buckets[home];
        let control = home_bucket.control.load(Ordering::Relaxed);
        if control >> OVERFLOW_SHIFT & OVERFLOW_MAX < OVERFLOW_MAX {
            // A saturated count stays saturated: lookups past it are only
            // slower, never wrong. Six keys per bucket keep it far below.
            home_bucket
                .control
                .store(control + (1 << OVERFLOW_SHIFT), Ordering::Release);
        }
        self.place_in(alternate, hash, handle)
    }

    /// A main-queue record of `slot`'s current key.
    fn record(&self, slot: usize) -> u32 {
        let generation =
            (self.bucket(slot).generations.load(Ordering::Relaxed) >> ((slot % SLOTS) * 8)) & 0xff;
        let slot = u32::try_from(slot).expect("shard slots fit 24 bits");
        (slot << 8) | u32::try_from(generation).expect("a generation is one byte")
    }

    /// The slot a main-queue record names, if it still holds the key the
    /// record was made for.
    fn live_record(&self, record: u32) -> Option<usize> {
        let slot = usize::try_from(record >> 8).expect("a slot fits usize");
        (slot < self.buckets.len() * SLOTS
            && self.record(slot) == record
            && self.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed) != 0)
            .then_some(slot)
    }

    /// Publishes `handle` in `bucket`, one of the key's candidates, counting
    /// it in its home's overflow when `bucket` is the alternate. Returns the
    /// slot, or `None` when `bucket` is full. Requires the shard lock.
    fn place_at(
        &self,
        bucket: usize,
        home: usize,
        hash: u64,
        handle: DirectHandle,
    ) -> Option<usize> {
        if zero_tag_bytes(self.buckets[bucket].control.load(Ordering::Relaxed)) == 0 {
            return None;
        }
        if bucket != home {
            let home_bucket = &self.buckets[home];
            let control = home_bucket.control.load(Ordering::Relaxed);
            if control >> OVERFLOW_SHIFT & OVERFLOW_MAX < OVERFLOW_MAX {
                home_bucket
                    .control
                    .store(control + (1 << OVERFLOW_SHIFT), Ordering::Release);
            }
        }
        self.place_in(bucket, hash, handle)
    }

    fn place_in(&self, bucket: usize, hash: u64, handle: DirectHandle) -> Option<usize> {
        let current = &self.buckets[bucket];
        let control = current.control.load(Ordering::Relaxed);
        let empty = zero_tag_bytes(control);
        if empty == 0 {
            return None;
        }
        // The lowest flagged byte of `zero_tag_bytes` is always exact.
        let slot = empty.trailing_zeros() as usize / 8;
        current.handles[slot].store(handle.index_value().get(), Ordering::Release);
        current.control.store(
            control | (u64::from(tag(hash)) << (slot * 8)),
            Ordering::Release,
        );
        let generations = current.generations.load(Ordering::Relaxed);
        let bumped = ((generations >> (slot * 8)) + 1) & 0xff;
        current.generations.store(
            (generations & !(0xff << (slot * 8))) | (bumped << (slot * 8)),
            Ordering::Relaxed,
        );
        Some(bucket * SLOTS + slot)
    }

    /// Unpublishes `slot`, whose key's home bucket is `home`, releasing the
    /// home's overflow count if the key lived in its alternate. Returns the
    /// handle swapped out. Requires the shard lock.
    fn clear(&self, slot: usize, home: usize) -> DirectHandle {
        let bucket = slot / SLOTS;
        let current = &self.buckets[bucket];
        let control = current.control.load(Ordering::Relaxed);
        current.control.store(
            control & !(TAG_MASK << ((slot % SLOTS) * 8)),
            Ordering::Release,
        );
        let removed = handle_from_raw(current.handles[slot % SLOTS].swap(0, Ordering::AcqRel));
        if bucket != home {
            let home_bucket = &self.buckets[home];
            let control = home_bucket.control.load(Ordering::Relaxed);
            let overflow = control >> OVERFLOW_SHIFT & OVERFLOW_MAX;
            debug_assert_ne!(overflow, 0, "a key in its alternate is counted at home");
            if overflow < OVERFLOW_MAX {
                home_bucket
                    .control
                    .store(control - (1 << OVERFLOW_SHIFT), Ordering::Release);
            }
        }
        removed
    }
}

#[cfg(test)]
thread_local! {
    static WINDOW_EVICTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// `MurmurHash3`'s 64-bit finalizer: every output bit depends on every input bit.
const fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^ (value >> 33)
}

const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
/// High bits of the six tag bytes.
const TAG_HIGH_BITS: u64 = 0x0000_8080_8080_8080;

/// Flags tag bytes of `word` that are zero in their high bit. The lowest flag
/// is exact; a borrow can also flag a 0x01 byte above a zero byte, which
/// callers tolerate: a candidate tag is checked against the entry's key, and
/// only the lowest empty slot is used.
const fn zero_tag_bytes(word: u64) -> u64 {
    word.wrapping_sub(BYTE_ONES) & !word & TAG_HIGH_BITS
}

/// A slot tag: eight hash bits other than the shard and home-bucket bits,
/// never zero.
fn tag(hash: u64) -> u8 {
    (hash >> 32).to_le_bytes()[0].max(1)
}

fn handle_from_raw(raw: u64) -> DirectHandle {
    #[allow(
        unsafe_code,
        reason = "tables store only the index values of this cache's arena handles"
    )]
    // SAFETY: every nonzero slot value is an unchanged `index_value` of a
    // handle allocated by this cache's arena.
    unsafe {
        DirectHandle::from_index_value(NonMaxU64::new(raw).expect("a handle is never u64::MAX"))
    }
}

/// Dereferences a handle published in this cache's table, or allocated and
/// not yet published, while `pin` is held.
///
/// # Safety
///
/// `handle` must belong to the arena that created `pin` and stay allocated
/// for the pin's epoch: it was in a table slot while `pin` was held, or it is
/// the caller's own unpublished allocation.
#[allow(unsafe_code, reason = "centralizes the table-handle and pin proof")]
unsafe fn entry<'pin, T>(
    pin: &'pin DirectArenaPin<'_, T>,
    handle: DirectHandle,
) -> &'pin DirectArenaEntry<T> {
    // SAFETY: the caller proves the handle stays allocated for this epoch.
    unsafe { pin.protect(handle) }.entry()
}

/// Handles unpublished under a shard lock, retired after it is released.
struct Retired {
    handles: [Option<DirectHandle>; 4],
    len: usize,
    more: Vec<DirectHandle>,
}

impl Retired {
    const fn new() -> Self {
        Self {
            handles: [None; 4],
            len: 0,
            more: Vec::new(),
        }
    }

    fn push(&mut self, handle: DirectHandle) {
        if self.len < self.handles.len() {
            self.handles[self.len] = Some(handle);
            self.len += 1;
        } else {
            self.more.push(handle);
        }
    }

    #[allow(
        unsafe_code,
        reason = "retires handles this operation unpublished while `pin` was held"
    )]
    fn retire<T>(self, pin: &DirectArenaPin<'_, T>) {
        for handle in self.handles.into_iter().flatten().chain(self.more) {
            // SAFETY: the shard writer cleared or replaced the only slot
            // holding this handle while `pin` was held, so this is its single
            // retirement.
            unsafe { pin.retire(RemovedDirectHandle::from_exact_removal(handle)) };
        }
    }
}

/// Approximate set of recently evicted probationary keys, in a ring of
/// generations; guarded by the shard lock. A key's word in every generation
/// sits on one cache line, so a lookup costs one miss.
struct Ghost {
    /// `words[word * GHOST_GENERATIONS + generation]`.
    words: Box<[u64]>,
    current: usize,
    remembered: usize,
    rotate_every: usize,
}

impl Ghost {
    fn new(keys_per_generation: usize) -> Self {
        let rotate_every = keys_per_generation.max(8);
        let words = (rotate_every * GHOST_BITS_PER_KEY).div_ceil(64);
        Self {
            words: vec![0; words * GHOST_GENERATIONS].into_boxed_slice(),
            current: 0,
            remembered: 0,
            rotate_every,
        }
    }

    fn location(&self, hash: u64) -> (usize, u64) {
        let low = hash & 0xffff_ffff;
        let mixed = low.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let words = (self.words.len() / GHOST_GENERATIONS) as u64;
        let word = usize::try_from(((mixed >> 32) * words) >> 32).expect("a ghost word fits usize");
        let mask =
            (0..GHOST_KEY_BITS).fold(0_u64, |mask, lane| mask | 1 << ((mixed >> (lane * 6)) & 63));
        (word * GHOST_GENERATIONS, mask)
    }

    fn contains(&self, hash: u64) -> bool {
        let (base, mask) = self.location(hash);
        self.words[base..base + GHOST_GENERATIONS]
            .iter()
            .any(|word| word & mask == mask)
    }

    fn remember(&mut self, hash: u64) {
        let (base, mask) = self.location(hash);
        self.words[base + self.current] |= mask;
        self.remembered += 1;
        if self.remembered == self.rotate_every {
            self.current = (self.current + 1) % GHOST_GENERATIONS;
            for word in self
                .words
                .iter_mut()
                .skip(self.current)
                .step_by(GHOST_GENERATIONS)
            {
                *word = 0;
            }
            self.remembered = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;
    use std::sync::Arc;
    use std::thread;

    /// Interpreted and sanitized runs keep the same races, fewer of them.
    const STRESS_ROUNDS: u64 = if cfg!(miri) { 300 } else { 40_000 };

    #[test]
    fn inserts_reads_replaces_and_removes() {
        let cache = ShardedCache::new(1_000);
        assert_eq!(cache.get(&1_u64), None);
        cache.insert(1_u64, "one".to_owned());
        cache.insert(2, "two".to_owned());
        assert_eq!(cache.get(&1).as_deref(), Some("one"));
        cache.insert(1, "uno".to_owned());
        assert_eq!(cache.get(&1).as_deref(), Some("uno"));
        assert_eq!(cache.len(), 2);
        assert!(cache.remove(&1));
        assert!(!cache.remove(&1));
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2).as_deref(), Some("two"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_guard_keeps_borrowed_values_alive_across_replacement_and_eviction() {
        let cache = ShardedCache::new(8);
        cache.insert(1_u64, "first".to_owned());
        let mut guard = cache.pin();
        let first = guard.get(&1).expect("resident");
        cache.insert(1, "second".to_owned());
        for key in 100..1_000_u64 {
            cache.insert(key, key.to_string());
        }
        assert_eq!(first, "first", "the guard keeps a replaced value alive");
        guard.refresh();
        assert_ne!(guard.get(&1).map(String::as_str), Some("first"));
    }

    #[test]
    fn weights_bound_each_shard_and_oversized_values_replace_nothing() {
        struct Length;
        impl Weighter<u64, String> for Length {
            fn weight(&self, _key: &u64, value: &String) -> u64 {
                value.len() as u64
            }
        }
        let cache = ShardedCacheBuilder::new(1_000)
            .max_weight(400)
            .shards(4)
            .weighter(Length)
            .build();
        for key in 0..1_000_u64 {
            assert!(cache.insert(key, "x".repeat(10)));
            assert!(cache.weight() <= 400);
        }
        assert!(cache.len() <= 40);
        // Growing a resident value evicts others in its shard.
        let resident = (0..1_000_u64).find(|key| cache.get(key).is_some()).unwrap();
        assert!(cache.insert(resident, "y".repeat(90)));
        assert_eq!(cache.get(&resident).map(|value| value.len()), Some(90));
        assert!(cache.weight() <= 400);
        // A value heavier than a shard's share is refused and drops the old one.
        assert!(!cache.insert(resident, "z".repeat(101)));
        assert_eq!(cache.get(&resident), None);
        assert_eq!(
            cache.weight(),
            (0..1_000_u64)
                .filter_map(|key| cache.get(&key))
                .map(|value| value.len() as u64)
                .sum::<u64>()
        );
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "reads entries the table publishes under the test's pin"
    )]
    #[cfg_attr(miri, ignore = "a statistical churn of up to a million inserts")]
    fn live_keys_stay_in_their_two_candidate_buckets_under_long_churn() {
        // Linear probing with holes clustered under this churn until keys
        // lived hundreds of buckets from home and every probe scanned the
        // table.
        let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(800).shards(1).build();
        let shard = &cache.shards[0];
        for key in 0..1_000_000_u64 {
            cache.insert(key, key);
            if key % 5 == 0 {
                let _ = cache.get(&(key / 3));
            }
        }
        let pin = cache.arena.pin();
        for (bucket, current) in shard.buckets.iter().enumerate() {
            for handle in &current.handles {
                let raw = handle.load(Ordering::Relaxed);
                if raw == 0 {
                    continue;
                }
                // SAFETY: the table publishes this handle while `pin` is held.
                let key = unsafe { entry(&pin, handle_from_raw(raw)) }.value().0;
                let (home, alternate) = shard.candidates(cache.hash_builder.hash_one(key));
                assert!(
                    bucket == home || bucket == alternate,
                    "key {key} lives in bucket {bucket}, outside {home} and {alternate}"
                );
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "a statistical churn of up to a million inserts")]
    fn full_candidate_buckets_rarely_force_an_eviction() {
        // Evicting from a key's two buckets overrides the cache's policy, so
        // relocation must make room in nearly every case.
        let cache = ShardedCacheBuilder::new(10_000).shards(1).build();
        let before = WINDOW_EVICTIONS.with(std::cell::Cell::get);
        for key in 0..200_000_u64 {
            cache.insert(key, key);
            if key % 4 == 0 {
                let _ = cache.get(&(key / 2));
            }
        }
        let forced = WINDOW_EVICTIONS.with(std::cell::Cell::get) - before;
        assert!(
            forced * 1_000 <= 200_000,
            "{forced} of 200000 inserts evicted from a full pair"
        );
    }

    #[test]
    fn unread_main_area_keys_leave_in_insertion_order() {
        // CLOCK over the table's own slots evicted in hash order; on
        // recency-heavy traces (ARC's P3) insertion order read 1.8 points more.
        let cache = ShardedCacheBuilder::new(256).shards(1).build();
        for key in 0..256_u64 {
            cache.insert(key, key);
        }
        // Read the keys still in probation, so later inserts promote them
        // and evictions fall through to the main area.
        for key in 0..256_u64 {
            if cache.shards[0]
                .writer
                .0
                .lock()
                .probation
                .iter()
                .any(|(_, hash)| *hash == cache.hash_builder.hash_one(key))
            {
                let _ = cache.get(&key);
            }
        }
        for key in 1_000..1_064_u64 {
            cache.insert(key, key);
            let _ = cache.get(&key);
        }
        let evicted: Vec<u64> = (0..256_u64)
            .filter(|key| cache.get(key).is_none())
            .collect();
        assert!(
            evicted.len() >= 32,
            "only {} original keys were evicted",
            evicted.len()
        );
        // A key moved to its other bucket rejoins the queue's back, and a
        // rare eviction from a full bucket pair takes any key; hash order
        // takes about half from the newer half.
        let newer = evicted.iter().filter(|key| **key >= 128).count();
        assert!(
            newer * 10 <= evicted.len(),
            "{newer} of {} evictions came from the newer half: {evicted:?}",
            evicted.len()
        );
    }

    #[test]
    fn borrowed_keys_find_owned_keys() {
        let cache: ShardedCache<String, u32> = ShardedCache::new(16);
        cache.insert("alpha".to_owned(), 1);
        assert_eq!(cache.get("alpha"), Some(1));
        assert!(cache.remove("alpha"));
    }

    #[test]
    fn never_exceeds_its_capacity_and_keeps_recent_keys_findable() {
        let cache = ShardedCache::new(500);
        for key in 0..20_000_u64 {
            cache.insert(key, key * 3);
            assert_eq!(
                cache.get(&key),
                Some(key * 3),
                "a just-inserted key is resident"
            );
            assert!(cache.len() <= 500);
        }
        let resident = (0..20_000_u64)
            .filter(|key| cache.get(key) == Some(key * 3))
            .count();
        assert!(resident >= 400, "only {resident} of 500 entries resident");
        assert!(resident <= 500);
    }

    /// A hasher that sends every key to one shard and one home bucket, so
    /// keys chain through overflow.
    #[derive(Clone, Default)]
    struct Colliding;

    struct CollidingHasher(u64);

    impl Hasher for CollidingHasher {
        fn finish(&self) -> u64 {
            // Same shard, home bucket and tag for every key.
            0
        }
        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 = self.0.rotate_left(5) ^ u64::from(*byte);
            }
        }
    }

    impl BuildHasher for Colliding {
        type Hasher = CollidingHasher;
        fn build_hasher(&self) -> CollidingHasher {
            CollidingHasher(0)
        }
    }

    #[test]
    fn colliding_keys_fill_their_two_buckets_and_unlink_cleanly() {
        // Every key shares one home bucket, so keys beyond its six slots
        // live in the alternate and count in the home's overflow.
        let cache = ShardedCacheBuilder::new(40).hasher(Colliding).build();
        for key in 0..12_u64 {
            cache.insert(key, key);
        }
        for key in 0..12_u64 {
            assert_eq!(cache.get(&key), Some(key));
        }
        let shard = &cache.shards[0];
        let overflow = |bucket: usize| {
            shard.buckets[bucket].control.load(Ordering::Relaxed) >> OVERFLOW_SHIFT & OVERFLOW_MAX
        };
        let (home, _) = shard.candidates(0);
        assert_eq!(overflow(home), 6);
        for key in (0..12_u64).step_by(2) {
            assert!(cache.remove(&key));
        }
        for key in 0..12_u64 {
            assert_eq!(cache.get(&key), (key % 2 == 1).then_some(key));
        }
        // Freed slots are reused; a thirteenth colliding key evicts one.
        for key in 100..107_u64 {
            cache.insert(key, key);
        }
        assert_eq!(cache.len(), 12);
        assert_eq!(cache.get(&106), Some(106));
        let resident: Vec<u64> = (0..12_u64)
            .chain(100..107)
            .filter(|key| cache.get(key).is_some())
            .collect();
        assert_eq!(resident.len(), 12);
        for key in resident {
            assert!(cache.remove(&key));
        }
        assert!(cache.is_empty());
        assert!(
            shard
                .buckets
                .iter()
                .all(|bucket| bucket.control.load(Ordering::Relaxed) == 0),
            "removing every key restores empty buckets and zero overflow"
        );
    }

    #[test]
    fn unread_probationary_keys_leave_before_read_ones() {
        let cache = ShardedCache::new(1_000);
        // Per-shard limits leave some room unused, so the working set is
        // whatever stayed resident after one pass.
        for key in 0..1_000_u64 {
            cache.insert(key, key);
        }
        let working_set: Vec<u64> = (0..1_000_u64)
            .filter(|key| cache.get(key).is_some())
            .collect();
        assert!(
            working_set.len() >= 900,
            "{} keys resident",
            working_set.len()
        );
        // A scan of one-hit keys displaces itself, not the read working set.
        for key in 10_000..20_000_u64 {
            cache.insert(key, key);
        }
        let survivors = working_set
            .iter()
            .filter(|key| cache.get(*key).is_some())
            .count();
        assert!(
            survivors * 10 >= working_set.len() * 8,
            "only {survivors} of {} read keys survived a scan",
            working_set.len()
        );
    }

    #[test]
    fn drop_releases_every_value() {
        let value = Arc::new(());
        {
            let cache = ShardedCache::new(100);
            for key in 0..1_000_u64 {
                cache.insert(key, Arc::clone(&value));
            }
            cache.remove(&999);
        }
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    // A contended parking_lot mutex parks through a futex syscall whose
    // variadic argument type current Miri rejects; ASan and TSan run these.
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn racing_replacements_removals_and_evictions_retire_every_value_once() {
        static LIVE: AtomicUsize = AtomicUsize::new(0);

        struct Tracked(u64);

        impl Tracked {
            fn new(key: u64) -> Self {
                LIVE.fetch_add(1, Ordering::Relaxed);
                Self(key)
            }
        }

        impl Clone for Tracked {
            fn clone(&self) -> Self {
                Self::new(self.0)
            }
        }

        impl Drop for Tracked {
            fn drop(&mut self) {
                let previous = LIVE.fetch_sub(1, Ordering::Relaxed);
                assert_ne!(previous, 0, "a value was dropped twice");
            }
        }

        {
            // Few keys and a small capacity keep replacements, removals, and
            // evictions colliding on the same slots.
            let cache = Arc::new(ShardedCache::<u64, Tracked>::new(64));
            let threads: Vec<_> = (0..8_u64)
                .map(|thread| {
                    let cache = Arc::clone(&cache);
                    thread::spawn(move || {
                        let mut state = thread * 7 + 1;
                        for round in 0..STRESS_ROUNDS {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            let key = state % 96;
                            match round % 3 {
                                0 => {
                                    cache.remove(&key);
                                }
                                1 => {
                                    if let Some(value) = cache.get(&key) {
                                        assert_eq!(value.0, key);
                                    }
                                }
                                _ => {
                                    cache.insert(key, Tracked::new(key));
                                }
                            }
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().expect("worker completes");
            }
            assert!(cache.len() <= 64);
        }
        assert_eq!(
            LIVE.load(Ordering::Relaxed),
            0,
            "every value was dropped once"
        );
    }

    #[test]
    // A contended parking_lot mutex parks through a futex syscall whose
    // variadic argument type current Miri rejects; ASan and TSan run these.
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn concurrent_readers_and_writers_see_only_their_own_values() {
        let cache = Arc::new(ShardedCache::new(2_000));
        let threads: Vec<_> = (0..8_u64)
            .map(|thread| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    let mut state = thread + 1;
                    for round in 0..STRESS_ROUNDS {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let key = state % 5_000;
                        match round % 4 {
                            0 => {
                                cache.insert(key, (key, round));
                            }
                            1 => {
                                cache.remove(&key);
                            }
                            _ => {
                                if let Some((stored, _)) = cache.get(&key) {
                                    assert_eq!(stored, key, "a read returned another key's value");
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker completes");
        }
        assert!(cache.len() <= 2_000);
        for key in 0..5_000_u64 {
            if let Some((stored, _)) = cache.get(&key) {
                assert_eq!(stored, key);
            }
        }
    }
}
