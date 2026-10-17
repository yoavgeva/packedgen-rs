//! A sharded concurrent cache with lock-free reads.
//!
//! Each shard is a fixed table of 64-byte buckets. A bucket holds six slots:
//! one control word with a tag byte per slot and an overflow count, and six
//! atomic handles to entries in the shard's epoch-reclaimed slab (see
//! `sharded_arena`). Keys live in the entries, not in the table, and an entry
//! never changes after publication, so a reader compares keys only in memory
//! that its epoch pin keeps alive. Writers can therefore reuse a freed table
//! slot immediately: a reader racing the reuse sees an old or a new handle,
//! both live for its epoch, and checks the key in that entry. Tables are sized
//! once for the shard's capacity and never rebuilt.
//!
//! Writers take their shard's mutex. A shard at its limit evicts in the
//! S3-FIFO style: new keys wait in a small probationary FIFO and leave unless
//! read; keys the ghost filter remembers go straight to the main area, a
//! CLOCK queue in the order keys entered it. Expired entries leave first
//! wherever eviction meets them.

use std::borrow::Borrow;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::cache_arena::direct_arena_thread;
use crate::sharded_arena::{
    EpochPin, Epochs, MAX_EXPIRY_TICK, MAX_WEIGHT, Reclaimed, RetiredSlots, Slot, SlotStore,
    drop_value, fill,
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
/// Deadlines are 100 ms ticks since the cache was built, kept in the entry's
/// 31-bit expiry field: about 6.8 years of range.
const EXPIRY_TICK_NANOS: u128 = 100_000_000;
/// Hit and miss counts spread over this many cache lines, by thread.
const STAT_STRIPES: usize = 32;

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

/// Why an entry left a [`ShardedCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RemovalCause {
    /// Evicted to keep its shard within the entry or weight limit.
    Size,
    /// Its time to live ran out. Expired entries leave when eviction, a
    /// replacement, or a removal reaches them.
    Expired,
    /// Replaced by a later insert for the same key.
    Replaced,
    /// Removed by [`ShardedCache::remove`] or [`ShardedCache::clear`].
    Explicit,
}

/// Hears about entries leaving a [`ShardedCache`].
///
/// The cache calls the listener on the removing thread, after releasing the
/// shard lock and before the entry can be reclaimed. Another thread may have
/// inserted the key again by then. The listener may use the cache, except
/// for loading the removed key through
/// [`ShardedCache::get_or_insert_with`]: the removal can come from that
/// key's own load on this thread, which the call would wait for. Dropping
/// the cache calls no listener.
pub trait RemovalListener<K, V> {
    /// Called once for each entry that leaves the cache.
    fn on_removal(&self, key: &K, value: &V, cause: RemovalCause);
}

impl<K, V, F> RemovalListener<K, V> for F
where
    F: Fn(&K, &V, RemovalCause),
{
    fn on_removal(&self, key: &K, value: &V, cause: RemovalCause) {
        self(key, value, cause);
    }
}

/// Ignores removals.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoListener;

impl<K, V> RemovalListener<K, V> for NoListener {
    fn on_removal(&self, _key: &K, _value: &V, _cause: RemovalCause) {}
}

/// Counts since a [`ShardedCache`] was built; see [`ShardedCache::stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShardedCacheStats {
    /// Reads that found a live value. Counted only with
    /// [`ShardedCacheBuilder::record_stats`].
    pub hits: u64,
    /// Reads that found none. Counted only with
    /// [`ShardedCacheBuilder::record_stats`].
    pub misses: u64,
    /// Entries evicted to stay within the limits.
    pub evictions: u64,
    /// Expired entries removed from the table.
    pub expirations: u64,
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
///
/// // Expiring entries, loaded once however many callers miss together.
/// let sessions: ShardedCache<u64, String> = ShardedCacheBuilder::new(10_000)
///     .time_to_live(std::time::Duration::from_secs(60))
///     .record_stats()
///     .build();
/// let name = sessions.get_or_insert_with(&3, || "three".to_owned());
/// assert_eq!(name, "three");
/// assert_eq!(sessions.stats().misses, 1);
/// ```
#[derive(Clone, Debug)]
pub struct ShardedCacheBuilder<W = UnitWeighter, S = DefaultHashBuilder, L = NoListener> {
    max_entries: usize,
    max_weight: Option<u64>,
    probation_bps: u16,
    shards: Option<usize>,
    time_to_live: Option<Duration>,
    record_stats: bool,
    weighter: W,
    hash_builder: S,
    listener: L,
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
            time_to_live: None,
            record_stats: false,
            weighter: UnitWeighter,
            hash_builder: DefaultHashBuilder::default(),
            listener: NoListener,
        }
    }
}

impl<W, S, L> ShardedCacheBuilder<W, S, L> {
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

    /// Expires entries `time_to_live` after [`ShardedCache::insert`] stores
    /// them. Deadlines are kept in 100 ms ticks, so an entry stays readable
    /// for its time to live and at most one tick more. The tick clock spans
    /// about 6.8 years from the cache's construction; after that, every
    /// entry with a deadline reads as expired.
    #[must_use]
    pub fn time_to_live(mut self, time_to_live: Duration) -> Self {
        self.time_to_live = Some(time_to_live);
        self
    }

    /// Counts hits and misses for [`ShardedCache::stats`]. Each counted read
    /// adds an uncontended atomic increment.
    #[must_use]
    pub fn record_stats(mut self) -> Self {
        self.record_stats = true;
        self
    }

    /// Weighs entries with `weighter`.
    #[must_use]
    pub fn weighter<W2>(self, weighter: W2) -> ShardedCacheBuilder<W2, S, L> {
        ShardedCacheBuilder {
            max_entries: self.max_entries,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            record_stats: self.record_stats,
            weighter,
            hash_builder: self.hash_builder,
            listener: self.listener,
        }
    }

    /// Hashes keys with `hash_builder`.
    #[must_use]
    pub fn hasher<S2>(self, hash_builder: S2) -> ShardedCacheBuilder<W, S2, L> {
        ShardedCacheBuilder {
            max_entries: self.max_entries,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            record_stats: self.record_stats,
            weighter: self.weighter,
            hash_builder,
            listener: self.listener,
        }
    }

    /// Calls `listener` for every entry that leaves the cache; see
    /// [`RemovalListener`].
    #[must_use]
    pub fn removal_listener<L2>(self, listener: L2) -> ShardedCacheBuilder<W, S, L2> {
        ShardedCacheBuilder {
            max_entries: self.max_entries,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            record_stats: self.record_stats,
            weighter: self.weighter,
            hash_builder: self.hash_builder,
            listener,
        }
    }

    /// Builds the cache.
    ///
    /// # Panics
    ///
    /// Panics if one shard would need 2^32 or more table slots.
    #[must_use]
    pub fn build<K, V>(self) -> ShardedCache<K, V, W, S, L>
    where
        K: Hash + Eq,
        W: Weighter<K, V>,
        S: BuildHasher,
        L: RemovalListener<K, V>,
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
            epochs: Epochs::new(),
            retired: RetiredSlots::new(),
            slots: SlotStore::new(),
            weighter: self.weighter,
            hash_builder: self.hash_builder,
            listener: self.listener,
            flights: (0..shards)
                .map(|_| FlightLane(Mutex::new(Vec::new())))
                .collect(),
            stats: self
                .record_stats
                .then(|| (0..STAT_STRIPES).map(|_| StatStripe::default()).collect()),
            started: Instant::now(),
            time_to_live: self.time_to_live,
            capacity: max_entries,
            max_weight,
            #[cfg(test)]
            clock_skew_nanos: AtomicU64::new(0),
        }
    }
}

/// A bounded concurrent cache with lock-free reads and sharded writers.
///
/// `get` clones the value, so `V` is typically small or an `Arc`; [`Self::pin`]
/// borrows values instead. Limits apply per shard: each holds its share of
/// the entry and weight limits, as in other sharded caches, so a cache can
/// evict a little before it is full overall.
pub struct ShardedCache<K, V, W = UnitWeighter, S = DefaultHashBuilder, L = NoListener> {
    shards: Box<[Shard<(K, V)>]>,
    shard_shift: u32,
    epochs: Epochs,
    /// Unpublished entries awaiting reclamation. Declared before `slots`:
    /// it drops the queued values, and `slots` then frees the chunks.
    retired: RetiredSlots<(K, V)>,
    slots: SlotStore<(K, V)>,
    weighter: W,
    hash_builder: S,
    listener: L,
    /// Loads in progress for `get_or_try_insert_with`, a lane per shard.
    flights: Box<[FlightLane<K, V>]>,
    stats: Option<Box<[StatStripe]>>,
    started: Instant,
    time_to_live: Option<Duration>,
    capacity: usize,
    max_weight: u64,
    #[cfg(test)]
    clock_skew_nanos: AtomicU64,
}

/// One shard's loads in progress.
#[repr(align(128))]
struct FlightLane<K, V>(Mutex<Vec<Arc<Flight<K, V>>>>);

/// A load of one missing key that later callers wait for.
struct Flight<K, V> {
    hash: u64,
    key: K,
    state: Mutex<FlightState<V>>,
    landed: Condvar,
}

enum FlightState<V> {
    Loading,
    Loaded(V),
    /// The loader failed or panicked; waiters look again.
    Abandoned,
}

/// Unlists a leader's flight when dropped and hands `loaded` to its waiters,
/// or abandons the load if there is none, as when the loader fails or panics.
struct FlightLanding<'lane, K, V> {
    lane: &'lane FlightLane<K, V>,
    flight: Arc<Flight<K, V>>,
    loaded: Option<V>,
}

impl<K, V> Drop for FlightLanding<'_, K, V> {
    fn drop(&mut self) {
        {
            let mut flights = self.lane.0.lock();
            if let Some(index) = flights
                .iter()
                .position(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                flights.swap_remove(index);
            }
        }
        // Waiters join under the lane lock, so once the flight is unlisted
        // the count only falls; alone, the leader has nobody to tell.
        if Arc::strong_count(&self.flight) > 1 {
            *self.flight.state.lock() = self
                .loaded
                .take()
                .map_or(FlightState::Abandoned, FlightState::Loaded);
            self.flight.landed.notify_all();
        }
    }
}

#[derive(Default)]
#[repr(align(128))]
struct StatStripe {
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Counts a read on the calling thread's stripe. Out of line, so caches that
/// do not record statistics keep one predictable branch on their read path.
#[cold]
#[inline(never)]
fn count_read(stripes: &[StatStripe], hit: bool) {
    let stripe = &stripes[direct_arena_thread() & (STAT_STRIPES - 1)];
    if hit {
        stripe.hits.fetch_add(1, Ordering::Relaxed);
    } else {
        stripe.misses.fetch_add(1, Ordering::Relaxed);
    }
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

/// Readers load only `buckets`, and the limits never change, so this line
/// stays in every core's cache. The writer lock and the state it guards
/// change on every write, so they sit on cache lines of their own.
struct Shard<T> {
    buckets: Box<[Bucket]>,
    limits: ShardLimits,
    writer: ShardLock,
    /// Tables publish handles to `Slot<T>`s; the cache's `RetiredSlots` and
    /// `SlotStore` carry the ownership bounds.
    _entries: PhantomData<fn() -> T>,
}

struct ShardLimits {
    capacity: usize,
    weight: u64,
    probation_target: usize,
    /// Past this many records the oldest is dropped, leaving its key to the
    /// main sweep, so records never copy more than a share of the keys.
    probation_limit: usize,
}

impl ShardLimits {
    #[inline]
    fn main_limit(&self) -> usize {
        self.capacity + self.capacity / 4 + 16
    }
}

/// The writer lock, between the counts writers keep under it. The counts are
/// atomics so that `len`, `weight`, and `stats` read them without the lock.
///
/// Writers' cores take turns with a shard, and each line of its state a write
/// touches moves to the writer's core while it holds the lock. So the counts
/// every write changes share the lock word's line with the probation queue,
/// and the ghost and the main queue fill the next line.
#[repr(C, align(128))]
struct ShardLock {
    len: AtomicUsize,
    weight: AtomicU64,
    evictions: AtomicU64,
    writer: Mutex<ShardWriter>,
    expirations: AtomicU64,
}

impl ShardLock {
    #[inline]
    fn lock<'shard>(&'shard self, limits: &'shard ShardLimits) -> LockedShard<'shard> {
        LockedShard {
            counts: self,
            limits,
            writer: self.writer.lock(),
        }
    }
}

#[repr(C)]
struct ShardWriter {
    /// Probationary keys in insertion order, as (slot record, key hash).
    /// A key moved to its other bucket leaves probation for the main area.
    probation: VecDeque<(u32, u64)>,
    ghost: Ghost,
    /// Main-area keys in the order they entered it, as `slot << 8 |
    /// generation`. Stale records (removed, replaced by another key, or
    /// moved) are skipped when popped.
    main: VecDeque<u32>,
    /// The CLOCK hand: the next slot the main-area sweep examines.
    hand: usize,
}

/// A shard's writer state under its lock, with the counts kept beside it.
struct LockedShard<'shard> {
    counts: &'shard ShardLock,
    limits: &'shard ShardLimits,
    writer: MutexGuard<'shard, ShardWriter>,
}

impl Deref for LockedShard<'_> {
    type Target = ShardWriter;

    #[inline]
    fn deref(&self) -> &ShardWriter {
        &self.writer
    }
}

impl DerefMut for LockedShard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut ShardWriter {
        &mut self.writer
    }
}

/// Adds one to a count. Counts change only under the shard lock, so a load
/// and a store suffice.
#[inline]
fn increment(count: &AtomicU64) {
    count.store(count.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
}

impl LockedShard<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.counts.len.load(Ordering::Relaxed)
    }

    #[inline]
    fn weight(&self) -> u64 {
        self.counts.weight.load(Ordering::Relaxed)
    }

    #[inline]
    fn over_limit(&self, incoming_weight: u64) -> bool {
        self.len() >= self.limits.capacity
            || self.weight().saturating_add(incoming_weight) > self.limits.weight
    }

    #[inline]
    fn set_len(&mut self, len: usize) {
        self.counts.len.store(len, Ordering::Relaxed);
    }

    #[inline]
    fn set_weight(&mut self, weight: u64) {
        self.counts.weight.store(weight, Ordering::Relaxed);
    }

    /// Accounts for an entry published in the table.
    #[inline]
    fn entered(&mut self, weight: u64) {
        self.set_len(self.len() + 1);
        self.set_weight(self.weight() + weight);
    }

    /// Accounts for an entry cleared from the table.
    #[inline]
    fn left(&mut self, weight: u64) {
        self.set_len(self.len() - 1);
        self.set_weight(self.weight() - weight);
    }

    #[inline]
    fn count_expiration(&mut self) {
        increment(&self.counts.expirations);
    }

    /// Accounts for an entry that eviction cleared from the table, and says
    /// why it left.
    #[inline]
    fn evicted(&mut self, weight: u64, expired: bool) -> RemovalCause {
        self.left(weight);
        if expired {
            self.count_expiration();
            RemovalCause::Expired
        } else {
            increment(&self.counts.evictions);
            RemovalCause::Size
        }
    }

    /// Appends a slot record to the main area's queue. Past its quarter of
    /// slack, stale records are dropped from the front in order-preserving
    /// chunks, so no write holds the shard lock for a whole-queue pass; only
    /// a chunk with nothing stale falls back to compacting the whole queue.
    fn enter_main<T>(&mut self, shard: &Shard<T>, record: u32) {
        if self.main.len() >= self.limits.main_limit() {
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
}

impl<K: Hash + Eq, V> ShardedCache<K, V> {
    /// Creates a cache holding at most `max_entries` entries.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        ShardedCacheBuilder::new(max_entries).build()
    }
}

impl<K, V, W, S, L> ShardedCache<K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
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

    /// The number of resident entries, including expired entries eviction
    /// has not reached yet; a weak snapshot under concurrency.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.writer.len.load(Ordering::Relaxed))
            .sum()
    }

    /// The summed weight of resident entries; a weak snapshot.
    #[must_use]
    pub fn weight(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.writer.weight.load(Ordering::Relaxed))
            .sum()
    }

    /// Whether the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Counts since the cache was built; weak snapshots under concurrency.
    #[must_use]
    pub fn stats(&self) -> ShardedCacheStats {
        let mut stats = ShardedCacheStats::default();
        for stripe in self.stats.iter().flat_map(|stripes| stripes.iter()) {
            stats.hits += stripe.hits.load(Ordering::Relaxed);
            stats.misses += stripe.misses.load(Ordering::Relaxed);
        }
        for shard in &self.shards {
            stats.evictions += shard.writer.evictions.load(Ordering::Relaxed);
            stats.expirations += shard.writer.expirations.load(Ordering::Relaxed);
        }
        stats
    }

    /// Returns a clone of the value cached for `key`.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        let pin = self.epochs.pin();
        let value = self
            .lookup(&pin, key, self.hash_builder.hash_one(key))
            .cloned();
        self.count(value.is_some());
        value
    }

    /// Like [`Self::get`], but neither marks the entry as read for eviction
    /// nor counts in the statistics.
    pub fn peek<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        let pin = self.epochs.pin();
        self.live_entry(&pin, key, self.hash_builder.hash_one(key))
            .map(|entry| entry.value().1.clone())
    }

    /// Whether `key` has a live value; like [`Self::peek`], not a read.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let pin = self.epochs.pin();
        self.live_entry(&pin, key, self.hash_builder.hash_one(key))
            .is_some()
    }

    /// Returns the value cached for `key`, or loads it with `load` and
    /// caches it; see [`Self::get_or_try_insert_with`].
    pub fn get_or_insert_with<Q>(&self, key: &Q, load: impl FnOnce() -> V) -> V
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let Ok(value) = self.get_or_try_insert_with(key, || Ok::<V, Infallible>(load()));
        value
    }

    /// Returns the value cached for `key`, or loads it with `load` and
    /// caches it.
    ///
    /// Callers that miss the same key while it loads wait for that load and
    /// share its value, so a key is loaded once however many callers miss
    /// it together. If the load fails or panics, its caller gets the error
    /// or the panic, and one waiter loads again with its own `load`. A
    /// `load` that reads this key through the cache deadlocks.
    ///
    /// # Errors
    ///
    /// Returns the error from this caller's `load`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from this caller's `load`.
    pub fn get_or_try_insert_with<Q, E>(
        &self,
        key: &Q,
        load: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let hash = self.hash_builder.hash_one(key);
        let cached = self.lookup(&self.epochs.pin(), key, hash).cloned();
        self.count(cached.is_some());
        if let Some(value) = cached {
            return Ok(value);
        }
        let lane = &self.flights[self.shard_index(hash)];
        let mut load = Some(load);
        loop {
            let joined = {
                let mut flights = lane.0.lock();
                if let Some(flight) = flights
                    .iter()
                    .find(|flight| flight.hash == hash && flight.key.borrow() == key)
                {
                    Arc::clone(flight)
                } else {
                    // A load that landed after this caller missed has
                    // inserted its value; flights unlist only after that.
                    if let Some(value) = self.lookup(&self.epochs.pin(), key, hash).cloned() {
                        return Ok(value);
                    }
                    let flight = Arc::new(Flight {
                        hash,
                        key: key.to_owned(),
                        state: Mutex::new(FlightState::Loading),
                        landed: Condvar::new(),
                    });
                    flights.push(Arc::clone(&flight));
                    drop(flights);
                    let mut landing = FlightLanding {
                        lane,
                        flight,
                        loaded: None,
                    };
                    let load = load.take().expect("a caller leads at most one load");
                    let value = load()?;
                    self.insert(key.to_owned(), value.clone());
                    landing.loaded = Some(value.clone());
                    return Ok(value);
                }
            };
            let mut state = joined.state.lock();
            while matches!(*state, FlightState::Loading) {
                joined.landed.wait(&mut state);
            }
            if let FlightState::Loaded(value) = &*state {
                return Ok(value.clone());
            }
        }
    }

    /// Pins the cache for a run of borrowed reads.
    ///
    /// Values a guard returns stay valid until it is refreshed or dropped,
    /// even if they are replaced or evicted meanwhile. Entries retired while
    /// a guard is held are not reclaimed until it refreshes, so long-lived
    /// guards should call [`ShardedCacheGuard::refresh`] periodically.
    #[must_use]
    pub fn pin(&self) -> ShardedCacheGuard<'_, K, V, W, S, L> {
        ShardedCacheGuard {
            cache: self,
            pin: self.epochs.pin(),
        }
    }

    /// Finds `key`'s live value and marks it read.
    fn lookup<'pin, Q>(&self, pin: &'pin EpochPin<'_>, key: &Q, hash: u64) -> Option<&'pin V>
    where
        K: Borrow<Q> + 'pin,
        V: 'pin,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.find_entry(pin, key, hash)?;
        // Entries without a deadline never read the clock.
        entry
            .mark_accessed_if_live(|| self.now())
            .then(|| &entry.value().1)
    }

    /// Finds `key`'s live entry without marking it read.
    fn live_entry<'pin, Q>(
        &self,
        pin: &'pin EpochPin<'_>,
        key: &Q,
        hash: u64,
    ) -> Option<Entry<'pin, (K, V)>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.find_entry(pin, key, hash)?;
        (!self.expired(entry, &mut None)).then_some(entry)
    }

    #[allow(
        unsafe_code,
        reason = "dereferences handles the table publishes under the caller's pin"
    )]
    fn find_entry<'pin, Q>(
        &self,
        pin: &'pin EpochPin<'_>,
        key: &Q,
        hash: u64,
    ) -> Option<Entry<'pin, (K, V)>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (_, handle) = self.shard(hash).find(hash, |handle| {
            // SAFETY: `find` passes handles published in this cache's table
            // while `pin` was held.
            unsafe { entry(pin, handle) }.value().0.borrow() == key
        })?;
        // SAFETY: as above.
        Some(unsafe { entry(pin, handle) })
    }

    fn count(&self, hit: bool) {
        if let Some(stripes) = &self.stats {
            count_read(stripes, hit);
        }
    }

    fn elapsed_nanos(&self) -> u128 {
        let elapsed = self.started.elapsed().as_nanos();
        #[cfg(test)]
        let elapsed = elapsed + u128::from(self.clock_skew_nanos.load(Ordering::Relaxed));
        elapsed
    }

    /// The current tick; it saturates after about 6.8 years. Out of line:
    /// reads of entries without a deadline never call it.
    #[cold]
    #[inline(never)]
    fn now(&self) -> u64 {
        u64::try_from(self.elapsed_nanos() / EXPIRY_TICK_NANOS)
            .unwrap_or(u64::MAX)
            .min(MAX_EXPIRY_TICK)
    }

    /// The first tick at which an entry stored now with `ttl` reads as
    /// expired: rounding the deadline up keeps it readable for all of `ttl`.
    fn deadline(&self, ttl: Duration) -> u64 {
        if ttl.is_zero() {
            return 0;
        }
        let ticks = (self.elapsed_nanos() + ttl.as_nanos()).div_ceil(EXPIRY_TICK_NANOS);
        u64::try_from(ticks)
            .unwrap_or(u64::MAX)
            .min(MAX_EXPIRY_TICK)
    }

    /// Whether `entry` has expired, reading the clock at most once per
    /// operation through `now`.
    fn expired(&self, entry: Entry<'_, (K, V)>, now: &mut Option<u64>) -> bool {
        let expires_at = entry.expires_at();
        expires_at != NEVER_EXPIRES && expires_at <= *now.get_or_insert_with(|| self.now())
    }

    /// Inserts or replaces the value for `key`, evicting from the key's shard
    /// while it is over its limits. The entry expires after the builder's
    /// [`time_to_live`](ShardedCacheBuilder::time_to_live), if any.
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
    pub fn insert(&self, key: K, value: V) -> bool {
        self.insert_expiring(key, value, self.time_to_live)
    }

    /// Like [`Self::insert`], with an entry that expires after `ttl` instead
    /// of the builder's time to live. A zero `ttl` is never readable.
    ///
    /// # Panics
    ///
    /// As for [`Self::insert`].
    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> bool {
        self.insert_expiring(key, value, Some(ttl))
    }

    #[allow(
        unsafe_code,
        reason = "fills, publishes, and unpublishes slab slots under the shard lock"
    )]
    fn insert_expiring(&self, key: K, value: V, ttl: Option<Duration>) -> bool {
        let hash = self.hash_builder.hash_one(&key);
        let shard = self.shard(hash);
        let weight = self.weighter.weight(&key, &value);
        let Some(compact_weight) = u32::try_from(weight)
            .ok()
            .filter(|weight| *weight <= MAX_WEIGHT && u64::from(*weight) <= shard.limits.weight)
        else {
            self.remove(&key);
            return false;
        };
        let expires_at = ttl.map_or(NEVER_EXPIRES, |ttl| self.deadline(ttl));
        let mut now = None;
        let pin = self.epochs.pin();
        let slot = self.slots.take();
        // SAFETY: `take` handed this vacant slot to this caller alone, and
        // nothing can reach it before it is published below.
        unsafe { fill(slot, (key, value), compact_weight, expires_at) };
        let handle = Handle(slot);
        // SAFETY: the caller's own filled slot, published below or never.
        let key = &unsafe { entry(&pin, handle) }.value().0;
        let mut removed = Removed::new();
        {
            let mut writer = shard.lock();
            let found = shard.find(hash, |current| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, current) }.value().0 == *key
            });
            if let Some((index, _)) = found {
                let previous = Handle::from_raw(
                    shard.bucket(index).handles[index % SLOTS].swap(handle.raw(), Ordering::AcqRel),
                );
                // SAFETY: the swap unpublished `previous` while `pin` is held.
                let previous_entry = unsafe { entry(&pin, previous) };
                let reweighed = writer.weight() - previous_entry.weight() + weight;
                writer.set_weight(reweighed);
                let cause = if self.expired(previous_entry, &mut now) {
                    writer.count_expiration();
                    RemovalCause::Expired
                } else {
                    RemovalCause::Replaced
                };
                removed.push(previous, cause);
                while writer.weight() > shard.limits.weight {
                    let Some((victim, cause)) =
                        self.evict(shard, &mut writer, &pin, Some(handle), &mut now)
                    else {
                        break;
                    };
                    removed.push(victim, cause);
                }
            } else {
                while writer.over_limit(weight) {
                    let Some((victim, cause)) =
                        self.evict(shard, &mut writer, &pin, None, &mut now)
                    else {
                        break;
                    };
                    removed.push(victim, cause);
                }
                let index = if let Some(index) = shard.place(hash, handle) {
                    index
                } else {
                    // Both candidate buckets are full: move a resident key to
                    // its other bucket, or else evict one of them.
                    if !self.relocate_candidate(shard, &mut writer, &pin, hash) {
                        #[cfg(test)]
                        WINDOW_EVICTIONS.with(|count| count.set(count.get() + 1));
                        let (victim, cause) =
                            self.evict_candidate(shard, &mut writer, &pin, hash, &mut now);
                        removed.push(victim, cause);
                    }
                    let placed = shard.place(hash, handle);
                    assert!(
                        placed.is_some(),
                        "relocation or eviction frees a candidate slot"
                    );
                    placed.unwrap_or_default()
                };
                writer.entered(weight);
                let record = shard.record(index);
                if writer.ghost.contains(hash) {
                    // Remembered from a recent eviction: straight to main.
                    writer.enter_main(shard, record);
                } else {
                    if writer.probation.len() >= shard.limits.probation_limit {
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
        }
        self.after_write(&removed, pin);
        true
    }

    /// Ends a write after its shard lock, still pinned: tells the listener
    /// what left, queues the unpublished entries for reclamation, drops the
    /// values of entries aged enough to reclaim, unpins, and advances the
    /// epoch generation if enough retirements are pending.
    fn after_write(&self, removed: &Removed<(K, V)>, pin: EpochPin<'_>) {
        if removed.len == 0 {
            return;
        }
        removed.notify(&self.listener, &pin);
        let mut reclaimed = Reclaimed::new();
        let mut foreign = Reclaimed::new();
        let advance = self.retired.retire(
            &self.epochs,
            removed.iter().map(|(handle, _)| handle.0),
            &mut reclaimed,
            &mut foreign,
        );
        reclaimed.finish(&self.slots);
        foreign.finish(&self.slots);
        drop(pin);
        if advance {
            self.epochs.try_advance();
        }
    }

    /// Removes `key`, returning whether it held a live value. An expired
    /// entry is removed too, but reported absent.
    #[allow(
        unsafe_code,
        reason = "dereferences handles unpublished under the shard lock"
    )]
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let shard = self.shard(hash);
        let pin = self.epochs.pin();
        let mut removed = Removed::new();
        let mut live = false;
        {
            let mut writer = shard.lock();
            let found = shard.find(hash, |handle| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, handle) }.value().0.borrow() == key
            });
            if let Some((index, _)) = found {
                let handle = shard.clear(index, shard.home(hash));
                // SAFETY: the clear unpublished `handle` while `pin` is held.
                let removed_entry = unsafe { entry(&pin, handle) };
                writer.left(removed_entry.weight());
                live = !self.expired(removed_entry, &mut None);
                let cause = if live {
                    RemovalCause::Explicit
                } else {
                    writer.count_expiration();
                    RemovalCause::Expired
                };
                removed.push(handle, cause);
            }
        }
        self.after_write(&removed, pin);
        live
    }

    /// Removes every entry. Inserts racing the sweep may survive it.
    pub fn clear(&self) {
        for shard in &self.shards {
            let pin = self.epochs.pin();
            let mut removed = Removed::new();
            {
                let mut writer = shard.lock();
                for bucket in &shard.buckets {
                    if bucket.control.load(Ordering::Relaxed) == 0 {
                        continue;
                    }
                    // Tags first, as `Shard::clear` does, then the handles.
                    bucket.control.store(0, Ordering::Release);
                    for handle in &bucket.handles {
                        let raw = handle.swap(0, Ordering::AcqRel);
                        if raw != 0 {
                            removed.push(Handle::from_raw(raw), RemovalCause::Explicit);
                        }
                    }
                }
                writer.set_len(0);
                writer.set_weight(0);
                writer.hand = 0;
                writer.probation.clear();
                writer.main.clear();
                writer.ghost.clear();
            }
            self.after_write(&removed, pin);
        }
    }

    /// Drops the values of removed entries that no reader can still reach.
    ///
    /// Writes reclaim a few removed entries each, in the shard they write, so
    /// after a burst of writes some removed values wait for later writes to
    /// their shards. Call this to drop them promptly. Readers holding a
    /// [`ShardedCacheGuard`] or a racing operation's pin can delay it.
    pub fn maintain(&self) {
        // Three advances age every entry removed before this call.
        for _ in 0..3 {
            self.epochs.try_advance();
        }
        loop {
            let mut reclaimed = Reclaimed::new();
            let more = self
                .retired
                .reclaim_all(self.epochs.generation(), &mut reclaimed);
            reclaimed.finish(&self.slots);
            if !more {
                break;
            }
        }
    }

    fn shard_index(&self, hash: u64) -> usize {
        // `checked_shr` keeps the single-shard shift of 64 in range.
        let shard = hash.checked_shr(self.shard_shift).unwrap_or(0);
        usize::try_from(shard).expect("a shard index fits usize")
    }

    fn shard(&self, hash: u64) -> &Shard<(K, V)> {
        &self.shards[self.shard_index(hash)]
    }

    /// Removes one entry from a shard over its limits: an unread or expired
    /// probationary key first, otherwise the next unread or expired key
    /// under the CLOCK. Never removes `protected`.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn evict(
        &self,
        shard: &Shard<(K, V)>,
        writer: &mut LockedShard<'_>,
        pin: &EpochPin<'_>,
        protected: Option<Handle<(K, V)>>,
        now: &mut Option<u64>,
    ) -> Option<(Handle<(K, V)>, RemovalCause)> {
        while writer.probation.len() > shard.limits.probation_target {
            let (record, hash) = writer
                .probation
                .pop_front()
                .expect("probation is above its target");
            let Some(slot) = shard.live_record(record) else {
                // Removed, or moved to its other bucket, since enrolling.
                continue;
            };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            if protected.is_some_and(|protected| protected.raw() == raw) {
                continue;
            }
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, Handle::<(K, V)>::from_raw(raw)) };
            let expired = self.expired(entry, now);
            if !expired && entry.take_accessed() {
                // Read during probation: it joins the main area.
                writer.enter_main(shard, record);
                continue;
            }
            let removed = shard.clear(slot, shard.home(hash));
            if !expired {
                writer.ghost.remember(hash);
            }
            return Some((removed, writer.evicted(entry.weight(), expired)));
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
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, Handle::<(K, V)>::from_raw(raw)) };
            let expired = self.expired(entry, now);
            if protected.is_some_and(|protected| protected.raw() == raw)
                || (!expired && entry.take_accessed())
            {
                writer.main.push_back(record);
                continue;
            }
            let hash = self.hash_builder.hash_one(&entry.value().0);
            let removed = shard.clear(slot, shard.home(hash));
            return Some((removed, writer.evicted(entry.weight(), expired)));
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
            if raw == 0 || protected.is_some_and(|protected| protected.raw() == raw) {
                continue;
            }
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, Handle::<(K, V)>::from_raw(raw)) };
            let expired = self.expired(entry, now);
            if !expired && entry.take_accessed() && step < 2 * slots {
                continue;
            }
            let hash = self.hash_builder.hash_one(&entry.value().0);
            let removed = shard.clear(slot, shard.home(hash));
            return Some((removed, writer.evicted(entry.weight(), expired)));
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
        shard: &Shard<(K, V)>,
        writer: &mut LockedShard<'_>,
        pin: &EpochPin<'_>,
        hash: u64,
    ) -> bool {
        let (home, alternate) = shard.candidates(hash);
        for bucket in [home, alternate] {
            for slot in bucket * SLOTS..(bucket + 1) * SLOTS {
                let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
                let handle = Handle::<(K, V)>::from_raw(raw);
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
                    debug_assert_eq!(moved.raw(), handle.raw());
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

    /// Removes an expired entry, or else the first unread one, sweeping
    /// access marks, from the two buckets `hash` may occupy. Used only when
    /// both are full.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn evict_candidate(
        &self,
        shard: &Shard<(K, V)>,
        writer: &mut LockedShard<'_>,
        pin: &EpochPin<'_>,
        hash: u64,
        now: &mut Option<u64>,
    ) -> (Handle<(K, V)>, RemovalCause) {
        let (home, alternate) = shard.candidates(hash);
        let slots = [home, alternate].map(|bucket| bucket * SLOTS);
        // The first pass takes an expired entry. Entries hold at most two
        // access marks, so the next two passes find an unread one unless
        // readers keep marking; the last then takes the first entry.
        for pass in 0..4 {
            for slot in slots.iter().flat_map(|first| *first..*first + SLOTS) {
                let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
                debug_assert_ne!(raw, 0, "both candidate buckets are full");
                // SAFETY: the shard lock keeps this published handle in the table.
                let entry = unsafe { entry(pin, Handle::<(K, V)>::from_raw(raw)) };
                let expired = self.expired(entry, now);
                let keep = match pass {
                    0 => !expired,
                    1 | 2 => entry.take_accessed(),
                    _ => false,
                };
                if keep {
                    continue;
                }
                let victim_home = shard.home(self.hash_builder.hash_one(&entry.value().0));
                let cause = writer.evicted(entry.weight(), expired);
                return (shard.clear(slot, victim_home), cause);
            }
        }
        unreachable!("the last pass evicts its first slot")
    }
}

/// A pinned read session over a [`ShardedCache`]; see [`ShardedCache::pin`].
pub struct ShardedCacheGuard<'cache, K, V, W = UnitWeighter, S = DefaultHashBuilder, L = NoListener>
{
    cache: &'cache ShardedCache<K, V, W, S, L>,
    pin: EpochPin<'cache>,
}

impl<K, V, W, S, L> ShardedCacheGuard<'_, K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    /// Borrows the value cached for `key` until this guard refreshes.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let value = self
            .cache
            .lookup(&self.pin, key, self.cache.hash_builder.hash_one(key));
        self.cache.count(value.is_some());
        value
    }

    /// Lets entries retired since the guard pinned be reclaimed.
    pub fn refresh(&mut self) {
        self.pin.refresh();
    }
}

impl<K, V, W, S, L> Drop for ShardedCache<K, V, W, S, L> {
    #[allow(
        unsafe_code,
        reason = "exclusive drop destroys every value still published in the tables"
    )]
    fn drop(&mut self) {
        // Exclusive access: no pin exists, so every published value can be
        // dropped now; each slab drops its retired values and frees its
        // chunks when the shards drop.
        for shard in &self.shards {
            for bucket in &shard.buckets {
                for handle in &bucket.handles {
                    let raw = handle.load(Ordering::Relaxed);
                    if raw != 0 {
                        // SAFETY: a published slot holds an initialized value,
                        // each slot is published once, and nothing reaches it
                        // after this exclusive drop.
                        unsafe { drop_value(Handle::<(K, V)>::from_raw(raw).0) };
                    }
                }
            }
        }
    }
}

impl<T> Shard<T> {
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
            limits: ShardLimits {
                capacity,
                weight: weight_limit,
                probation_target,
                probation_limit: probation_target + probation_target / 4 + 4,
            },
            writer: ShardLock {
                len: AtomicUsize::new(0),
                weight: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
                writer: Mutex::new(ShardWriter {
                    probation: VecDeque::new(),
                    ghost: Ghost::new(capacity.div_ceil(GHOST_GENERATION_DIVISOR)),
                    main: VecDeque::with_capacity(capacity + capacity / 4 + 17),
                    hand: 0,
                }),
                expirations: AtomicU64::new(0),
            },
            _entries: PhantomData,
        }
    }

    #[inline]
    fn lock(&self) -> LockedShard<'_> {
        self.writer.lock(&self.limits)
    }

    #[inline]
    fn bucket(&self, slot: usize) -> &Bucket {
        &self.buckets[slot / SLOTS]
    }

    /// Multiply-shift range reduction of 32 bits: the table need not be a
    /// power of two.
    #[inline]
    fn reduce(&self, bits: u64) -> usize {
        let scaled = ((bits & 0xffff_ffff) * self.buckets.len() as u64) >> 32;
        usize::try_from(scaled).expect("a bucket index fits usize")
    }

    /// One multiply spreads every hash bit into the high half that picks
    /// the home: for some seeds the fast hasher's low half alone clusters.
    #[inline]
    fn home(&self, hash: u64) -> usize {
        self.reduce(hash.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32)
    }

    #[inline]
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
    #[inline]
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
    #[inline]
    fn find(
        &self,
        hash: u64,
        mut matches: impl FnMut(Handle<T>) -> bool,
    ) -> Option<(usize, Handle<T>)> {
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

    #[inline]
    fn find_in(
        &self,
        bucket: usize,
        control: u64,
        tag: u8,
        matches: &mut impl FnMut(Handle<T>) -> bool,
    ) -> Option<(usize, Handle<T>)> {
        let current = &self.buckets[bucket];
        let mut candidates = zero_tag_bytes(control ^ (BYTE_ONES * u64::from(tag)));
        while candidates != 0 {
            let slot = candidates.trailing_zeros() as usize / 8;
            candidates &= candidates - 1;
            // Acquire pairs with the release store of a replacement handle,
            // which does not touch the control word.
            let raw = current.handles[slot].load(Ordering::Acquire);
            if raw != 0 {
                let handle = Handle::from_raw(raw);
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
    #[inline]
    fn place(&self, hash: u64, handle: Handle<T>) -> Option<usize> {
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
    #[inline]
    fn record(&self, slot: usize) -> u32 {
        let generation =
            (self.bucket(slot).generations.load(Ordering::Relaxed) >> ((slot % SLOTS) * 8)) & 0xff;
        let slot = u32::try_from(slot).expect("shard slots fit 24 bits");
        (slot << 8) | u32::try_from(generation).expect("a generation is one byte")
    }

    /// The slot a main-queue record names, if it still holds the key the
    /// record was made for.
    #[inline]
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
    #[inline]
    fn place_at(&self, bucket: usize, home: usize, hash: u64, handle: Handle<T>) -> Option<usize> {
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

    #[inline]
    fn place_in(&self, bucket: usize, hash: u64, handle: Handle<T>) -> Option<usize> {
        let current = &self.buckets[bucket];
        let control = current.control.load(Ordering::Relaxed);
        let empty = zero_tag_bytes(control);
        if empty == 0 {
            return None;
        }
        // The lowest flagged byte of `zero_tag_bytes` is always exact.
        let slot = empty.trailing_zeros() as usize / 8;
        current.handles[slot].store(handle.raw(), Ordering::Release);
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
    #[inline]
    fn clear(&self, slot: usize, home: usize) -> Handle<T> {
        let bucket = slot / SLOTS;
        let current = &self.buckets[bucket];
        let control = current.control.load(Ordering::Relaxed);
        current.control.store(
            control & !(TAG_MASK << ((slot % SLOTS) * 8)),
            Ordering::Release,
        );
        let removed = Handle::from_raw(current.handles[slot % SLOTS].swap(0, Ordering::AcqRel));
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
#[inline]
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
#[inline]
const fn zero_tag_bytes(word: u64) -> u64 {
    word.wrapping_sub(BYTE_ONES) & !word & TAG_HIGH_BITS
}

/// A slot tag: eight hash bits other than the shard and home-bucket bits,
/// never zero.
#[inline]
fn tag(hash: u64) -> u8 {
    (hash >> 32).to_le_bytes()[0].max(1)
}

/// A table handle: the address of an entry's slot, stored in the table as a
/// nonzero `u64`.
struct Handle<T>(NonNull<Slot<T>>);

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Handle<T> {}

impl<T> Handle<T> {
    #[inline]
    fn raw(self) -> u64 {
        // Targets are 64-bit, so an address fits.
        self.0.as_ptr().expose_provenance() as u64
    }

    #[inline]
    fn from_raw(raw: u64) -> Self {
        let address = usize::try_from(raw).expect("a handle fits usize");
        Self(NonNull::new(ptr::with_exposed_provenance_mut(address)).expect("a handle is nonzero"))
    }
}

/// An entry reached through its handle for the life of an epoch pin.
struct Entry<'pin, T> {
    slot: &'pin Slot<T>,
}

impl<T> Clone for Entry<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Entry<'_, T> {}

impl<'pin, T> Entry<'pin, T> {
    #[inline]
    #[allow(
        unsafe_code,
        reason = "relies on `entry`'s proof that the value outlives the pin"
    )]
    fn value(self) -> &'pin T {
        // SAFETY: `entry` required the slot's value to stay initialized and
        // unmodified for the pin's life.
        unsafe { self.slot.value() }
    }

    #[inline]
    fn weight(self) -> u64 {
        self.slot.weight()
    }

    #[inline]
    fn expires_at(self) -> u64 {
        self.slot.expires_at()
    }

    #[inline]
    fn mark_accessed_if_live(self, now: impl FnOnce() -> u64) -> bool {
        self.slot.mark_accessed_if_live(now)
    }

    #[inline]
    fn take_accessed(self) -> bool {
        self.slot.take_accessed()
    }
}

/// Reaches the entry behind `handle` for the life of `pin`.
///
/// # Safety
///
/// `handle` must name a slot of this cache whose value stays initialized
/// while `pin` is held: published in a table while `pin` was held, or
/// unpublished under a shard lock while `pin` was held, or filled by the
/// caller under the shard lock that still guards it.
#[inline]
#[allow(unsafe_code, reason = "centralizes the table-handle and pin proof")]
unsafe fn entry<'pin, T>(_pin: &'pin EpochPin<'_>, handle: Handle<T>) -> Entry<'pin, T> {
    // SAFETY: the caller proves the slot and its value outlive the pin.
    let slot = unsafe { handle.0.as_ref() };
    Entry { slot }
}

/// Entries one operation unpublished under a shard lock, with why each left.
/// They are queued for reclamation under that lock, and the removal listener
/// hears about them after it is released, while the operation's pin keeps
/// them readable.
struct Removed<T> {
    first: [Option<(Handle<T>, RemovalCause)>; 4],
    len: usize,
    more: Vec<(Handle<T>, RemovalCause)>,
}

impl<T> Removed<T> {
    const fn new() -> Self {
        Self {
            first: [None; 4],
            len: 0,
            more: Vec::new(),
        }
    }

    fn push(&mut self, handle: Handle<T>, cause: RemovalCause) {
        if self.len < self.first.len() {
            self.first[self.len] = Some((handle, cause));
            self.len += 1;
        } else {
            self.more.push((handle, cause));
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Handle<T>, RemovalCause)> + '_ {
        self.first[..self.len]
            .iter()
            .flatten()
            .copied()
            .chain(self.more.iter().copied())
    }
}

impl<K, V> Removed<(K, V)> {
    /// Tells `listener` about each entry.
    #[allow(
        unsafe_code,
        reason = "reads entries this operation unpublished while `pin` was held"
    )]
    fn notify(&self, listener: &impl RemovalListener<K, V>, pin: &EpochPin<'_>) {
        for (handle, cause) in self.iter() {
            // SAFETY: unpublished while `pin` was held, and `pin` still
            // blocks the reclamation of every entry retired since.
            let (key, value) = unsafe { entry(pin, handle) }.value();
            listener.on_removal(key, value, cause);
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
    /// Counts fit 32 bits, as shards hold fewer than 2^24 slots; the ghost
    /// then shares one cache line with the main queue.
    remembered: u32,
    rotate_every: u32,
}

impl Ghost {
    fn new(keys_per_generation: usize) -> Self {
        let rotate_every = keys_per_generation.max(8);
        let words = (rotate_every * GHOST_BITS_PER_KEY).div_ceil(64);
        Self {
            words: vec![0; words * GHOST_GENERATIONS].into_boxed_slice(),
            current: 0,
            remembered: 0,
            rotate_every: u32::try_from(rotate_every).expect("a shard's ghost generation fits u32"),
        }
    }

    #[inline]
    fn location(&self, hash: u64) -> (usize, u64) {
        let low = hash & 0xffff_ffff;
        let mixed = low.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let words = (self.words.len() / GHOST_GENERATIONS) as u64;
        let word = usize::try_from(((mixed >> 32) * words) >> 32).expect("a ghost word fits usize");
        let mask =
            (0..GHOST_KEY_BITS).fold(0_u64, |mask, lane| mask | 1 << ((mixed >> (lane * 6)) & 63));
        (word * GHOST_GENERATIONS, mask)
    }

    #[inline]
    fn contains(&self, hash: u64) -> bool {
        let (base, mask) = self.location(hash);
        self.words[base..base + GHOST_GENERATIONS]
            .iter()
            .any(|word| word & mask == mask)
    }

    #[inline]
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

    fn clear(&mut self) {
        self.words.fill(0);
        self.current = 0;
        self.remembered = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{BuildHasherDefault, Hasher};
    use std::sync::Arc;
    use std::thread;

    /// Interpreted and sanitized runs keep the same races, fewer of them.
    const STRESS_ROUNDS: u64 = if cfg!(miri) { 300 } else { 40_000 };
    /// Miri takes tens of milliseconds per insert, so interpreted runs churn
    /// a few capacities' worth of keys instead of many.
    const CHURN_SCALE: u64 = if cfg!(miri) { 1 } else { 10 };
    /// Times to live are hours long, so even interpreted runs, whose clocks
    /// advance with the work they do, expire entries only when a test moves
    /// the clock.
    const HOUR: Duration = Duration::from_secs(3_600);
    const MINUTE: Duration = Duration::from_secs(60);

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
        let pin = cache.epochs.pin();
        for (bucket, current) in shard.buckets.iter().enumerate() {
            for handle in &current.handles {
                let raw = handle.load(Ordering::Relaxed);
                if raw == 0 {
                    continue;
                }
                // SAFETY: the table publishes this handle while `pin` is held.
                let key = unsafe { entry(&pin, Handle::<(u64, u64)>::from_raw(raw)) }
                    .value()
                    .0;
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
        let keys = 2_000 * CHURN_SCALE;
        for key in 0..keys {
            cache.insert(key, key * 3);
            assert_eq!(
                cache.get(&key),
                Some(key * 3),
                "a just-inserted key is resident"
            );
            assert!(cache.len() <= 500);
        }
        let resident = (0..keys)
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
        let capacity = 100 * CHURN_SCALE;
        // One shard: with several, a hash seed that loads one shard heavily
        // evicts during the first pass, which is not what this test probes.
        let cache =
            ShardedCacheBuilder::new(usize::try_from(capacity).expect("a test capacity fits"))
                .shards(1)
                .build();
        for key in 0..capacity {
            cache.insert(key, key);
        }
        let working_set: Vec<u64> = (0..capacity)
            .filter(|key| cache.get(key).is_some())
            .collect();
        // A full bucket pair can force an eviction before the shard fills.
        assert!(
            working_set.len() as u64 * 100 >= capacity * 99,
            "only {} keys resident after the first pass",
            working_set.len()
        );
        // A scan of one-hit keys displaces itself, not the read working set.
        for key in capacity * 10..capacity * 20 {
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
    fn racing_replacements_removals_loads_clears_and_evictions_retire_every_value_once() {
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
                            if thread == 0 && round % 1_009 == 0 {
                                cache.clear();
                            }
                            match round % 4 {
                                0 => {
                                    cache.remove(&key);
                                }
                                1 => {
                                    if let Some(value) = cache.get(&key) {
                                        assert_eq!(value.0, key);
                                    }
                                }
                                2 => {
                                    let value =
                                        cache.get_or_insert_with(&key, || Tracked::new(key));
                                    assert_eq!(value.0, key);
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

    impl<K, V, W, S: BuildHasher, L> ShardedCache<K, V, W, S, L> {
        /// Moves this cache's expiry clock forward.
        fn advance_clock(&self, by: Duration) {
            self.clock_skew_nanos.fetch_add(
                u64::try_from(by.as_nanos()).expect("test skews fit u64"),
                Ordering::Relaxed,
            );
        }
    }

    /// Whether `key`'s entry carries an access mark.
    fn is_marked(cache: &ShardedCache<u64, u64>, key: u64) -> bool {
        let pin = cache.epochs.pin();
        cache
            .live_entry(&pin, &key, cache.hash_builder.hash_one(key))
            .is_some_and(|entry| entry.slot.accessed())
    }

    /// Waits until exactly `waiters` callers besides the leader wait on the
    /// load of `key`.
    fn wait_for_waiters(cache: &ShardedCache<u64, u64>, key: u64, waiters: usize) {
        let lane = &cache.flights[cache.shard_index(cache.hash_builder.hash_one(key))];
        for _ in 0..10_000 {
            let joined = lane
                .0
                .lock()
                .iter()
                .find(|flight| flight.key == key)
                .map(|flight| Arc::strong_count(flight) - 2);
            if joined == Some(waiters) {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("{waiters} callers never waited on key {key}");
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn get_or_insert_with_loads_a_missing_key_once_across_racing_threads() {
        let cache = Arc::new(ShardedCache::<u64, u64>::new(1_000));
        let loads = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let loads = Arc::clone(&loads);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    cache.get_or_insert_with(&7, || {
                        loads.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(20));
                        70
                    })
                })
            })
            .collect();
        for thread in threads {
            assert_eq!(thread.join().expect("caller completes"), 70);
        }
        assert_eq!(loads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.get(&7), Some(70));
        assert_eq!(
            cache.get_or_insert_with(&7, || unreachable!("a cached key is not loaded")),
            70
        );
        assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn a_failed_or_panicking_load_hands_the_key_to_a_waiter() {
        for panics in [false, true] {
            let cache = Arc::new(ShardedCache::<u64, u64>::new(1_000));
            let (release, released) = std::sync::mpsc::channel::<()>();
            let leader = {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    cache.get_or_try_insert_with(&5, || {
                        released.recv().expect("the test releases the loader");
                        assert!(!panics, "the loader panics");
                        Err("the loader fails")
                    })
                })
            };
            wait_for_waiters(&cache, 5, 0);
            let waiter = {
                let cache = Arc::clone(&cache);
                thread::spawn(move || cache.get_or_try_insert_with(&5, || Ok::<_, &str>(50)))
            };
            wait_for_waiters(&cache, 5, 1);
            release.send(()).expect("the leader is loading");
            let led = leader.join();
            if panics {
                assert!(led.is_err(), "the leader's panic propagates");
            } else {
                assert_eq!(led.expect("the leader returns"), Err("the loader fails"));
            }
            assert_eq!(waiter.join().expect("the waiter completes"), Ok(50));
            assert_eq!(cache.get(&5), Some(50));
            assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
        }
    }

    #[test]
    fn expired_entries_read_as_missing() {
        let cache = ShardedCacheBuilder::new(100)
            .time_to_live(10 * HOUR)
            .build::<u64, u64>();
        cache.insert(1, 10);
        cache.insert_with_ttl(2, 20, HOUR);
        cache.insert_with_ttl(3, 30, Duration::ZERO);
        assert_eq!(cache.get(&3), None, "a zero time to live is never readable");
        assert_eq!((cache.get(&1), cache.get(&2)), (Some(10), Some(20)));
        cache.advance_clock(HOUR + MINUTE);
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.pin().get(&2), None);
        assert_eq!(cache.peek(&2), None);
        assert!(!cache.contains_key(&2));
        assert_eq!(cache.get(&1), Some(10));
        // Replacing a value restarts its time to live.
        cache.insert(1, 11);
        cache.advance_clock(9 * HOUR);
        assert_eq!(cache.get(&1), Some(11));
        cache.advance_clock(HOUR + MINUTE);
        assert_eq!(cache.get(&1), None);
        assert!(!cache.remove(&1), "an expired key was already absent");
    }

    #[test]
    fn expired_entries_leave_before_read_live_ones() {
        // Read marks would keep expired entries in the CLOCK for two more
        // sweeps; expiry overrides them, so read live keys outlast them. A
        // fixed hasher keeps the run, including its rare full-pair
        // evictions, the same every time.
        let cache = ShardedCacheBuilder::new(64)
            .shards(1)
            .hasher(BuildHasherDefault::<DefaultHasher>::default())
            .build::<u64, u64>();
        for key in 0..64_u64 {
            if key < 32 {
                cache.insert(key, key);
            } else {
                cache.insert_with_ttl(key, key, HOUR);
            }
        }
        for _ in 0..2 {
            for key in 0..64 {
                assert_eq!(cache.get(&key), Some(key));
            }
        }
        cache.advance_clock(2 * HOUR);
        for key in 100..132_u64 {
            cache.insert(key, key);
            assert_eq!(cache.get(&key), Some(key));
        }
        let evicted: Vec<u64> = (0..32).filter(|key| !cache.contains_key(key)).collect();
        assert!(
            evicted.is_empty(),
            "read live keys were evicted: {evicted:?}"
        );
        let stats = cache.stats();
        assert_eq!(stats.evictions + stats.expirations, 32);
        // Further churn reaches every expired entry still in the table.
        for key in 200..264_u64 {
            cache.insert(key, key);
            assert_eq!(cache.get(&key), Some(key));
        }
        assert_eq!(cache.stats().expirations, 32);
    }

    #[test]
    fn stats_count_hits_misses_evictions_and_expirations() {
        let cache = ShardedCacheBuilder::new(64)
            .shards(1)
            .record_stats()
            .build::<u64, u64>();
        for key in 0..64 {
            cache.insert(key, key);
        }
        let _ = (cache.get(&0), cache.get(&1), cache.get(&1_000));
        let _ = (
            cache.pin().get(&2).copied(),
            cache.pin().get(&2_000).copied(),
        );
        let _ = (cache.peek(&3), cache.peek(&3_000), cache.contains_key(&4));
        cache.get_or_insert_with(&5, || unreachable!("key 5 is cached"));
        cache.get_or_insert_with(&3_001, || 1);
        for key in 64..96 {
            cache.insert(key, key);
        }
        cache.insert_with_ttl(200, 200, HOUR);
        cache.advance_clock(2 * HOUR);
        assert!(!cache.remove(&200));
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (4, 3));
        assert_eq!((stats.evictions, stats.expirations), (34, 1));

        let uncounted = ShardedCacheBuilder::new(8).shards(1).build::<u64, u64>();
        for key in 0..10 {
            uncounted.insert(key, key);
            let _ = uncounted.get(&key);
        }
        let stats = uncounted.stats();
        assert_eq!((stats.hits, stats.misses, stats.evictions), (0, 0, 2));
    }

    #[test]
    fn the_removal_listener_hears_each_cause() {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let cache = {
            let heard = Arc::clone(&heard);
            ShardedCacheBuilder::new(8)
                .shards(1)
                .removal_listener(move |key: &u64, value: &u64, cause| {
                    heard.lock().push((*key, *value, cause));
                })
                .build::<u64, u64>()
        };
        for key in 0..8 {
            cache.insert(key, key * 10);
        }
        cache.insert(3, 31);
        assert!(cache.remove(&4));
        cache.insert_with_ttl(9, 90, HOUR);
        cache.advance_clock(2 * HOUR);
        assert!(!cache.remove(&9));
        cache.insert(10, 100);
        cache.insert(11, 110);
        {
            let heard = heard.lock();
            assert_eq!(
                heard[..3],
                [
                    (3, 30, RemovalCause::Replaced),
                    (4, 40, RemovalCause::Explicit),
                    (9, 90, RemovalCause::Expired),
                ]
            );
            assert_eq!(heard.len(), 4);
            assert_eq!(heard[3].2, RemovalCause::Size);
            assert!(heard[3].0 < 10, "the newest key stays");
        }
        cache.clear();
        let heard = heard.lock();
        assert_eq!(heard.len(), 12);
        assert!(
            heard[4..]
                .iter()
                .all(|(.., cause)| *cause == RemovalCause::Explicit)
        );
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "reads entries the table publishes under the test's pin"
    )]
    fn clear_removes_every_entry_and_the_cache_refills_cleanly() {
        let value = Arc::new(());
        let capacity = 100 * CHURN_SCALE;
        let cache =
            ShardedCacheBuilder::new(usize::try_from(capacity).expect("a test capacity fits"))
                .shards(2)
                .build::<u64, Arc<()>>();
        for key in 0..2 * capacity {
            cache.insert(key, Arc::clone(&value));
        }
        cache.clear();
        assert_eq!((cache.len(), cache.weight()), (0, 0));
        assert!((0..2 * capacity).all(|key| !cache.contains_key(&key)));
        let refill = 5 * capacity;
        for key in refill..refill + 2 * capacity {
            cache.insert(key, Arc::clone(&value));
        }
        assert!(cache.len() as u64 <= capacity);
        let pin = cache.epochs.pin();
        let mut resident = 0;
        for shard in &cache.shards {
            for current in &shard.buckets {
                for handle in &current.handles {
                    let raw = handle.load(Ordering::Relaxed);
                    if raw != 0 {
                        // SAFETY: the table publishes this handle while
                        // `pin` is held.
                        let key = unsafe { entry(&pin, Handle::<(u64, Arc<()>)>::from_raw(raw)) }
                            .value()
                            .0;
                        assert!(key >= refill, "cleared key {key} is still published");
                        assert!(cache.contains_key(&key), "key {key} is not findable");
                        resident += 1;
                    }
                }
            }
        }
        assert_eq!(resident, cache.len());
        drop(pin);
        drop(cache);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn peek_and_contains_key_leave_no_access_mark() {
        let cache = ShardedCache::new(100);
        cache.insert(1_u64, 1_u64);
        assert_eq!(cache.peek(&1), Some(1));
        assert!(cache.contains_key(&1));
        assert!(!is_marked(&cache, 1));
        assert_eq!(cache.get(&1), Some(1));
        assert!(is_marked(&cache, 1));
    }

    #[test]
    fn maintain_drops_removed_values() {
        let value = Arc::new(());
        let cache = ShardedCacheBuilder::new(64)
            .shards(1)
            .build::<u64, Arc<()>>();
        for key in 0..1_000 {
            cache.insert(key, Arc::clone(&value));
        }
        cache.clear();
        assert!(
            Arc::strong_count(&value) > 1,
            "a write reclaims only entries aged past three advances"
        );
        cache.maintain();
        assert_eq!(
            Arc::strong_count(&value),
            1,
            "maintain dropped every removed value"
        );
    }

    #[test]
    // Threads take turns, so no parking_lot lock is ever contended.
    fn a_thread_that_exits_returns_its_vacant_slots() {
        let cache = Arc::new(ShardedCacheBuilder::new(64).shards(1).build::<u64, u64>());
        {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for key in 0..2_000 {
                    cache.insert(key, key);
                }
                cache.clear();
                cache.maintain();
            })
            .join()
            .expect("the writer completes");
        }
        assert!(
            cache.slots.depot_len() > 0,
            "the exited thread's stack returned to the depot"
        );
    }

    #[test]
    // Threads take turns, so no parking_lot lock is ever contended.
    fn dropping_a_cache_leaves_other_threads_stacks_harmless() {
        let first = Arc::new(ShardedCache::<u64, u64>::new(64));
        let (go, wait) = std::sync::mpsc::channel::<()>();
        let (done, finished) = std::sync::mpsc::channel::<()>();
        let worker = {
            let first = Arc::clone(&first);
            thread::spawn(move || {
                for key in 0..500 {
                    first.insert(key, key);
                }
                first.clear();
                first.maintain();
                drop(first);
                done.send(()).expect("the test waits");
                // The first cache is gone; this thread's stack for it holds
                // freed slots that must never be touched again.
                wait.recv().expect("the test releases the worker");
                let second = ShardedCache::<u64, u64>::new(64);
                for key in 0..500 {
                    second.insert(key, key + 1);
                }
                assert_eq!(second.get(&499), Some(500));
            })
        };
        finished.recv().expect("the worker used the first cache");
        drop(first);
        go.send(()).expect("the worker waits");
        worker.join().expect("the worker completes");
    }

    #[test]
    fn keys_evicted_from_probation_return_to_the_main_area() {
        let cache = ShardedCacheBuilder::new(256).shards(1).build::<u64, u64>();
        let in_probation = |key: u64| {
            let hash = cache.hash_builder.hash_one(key);
            cache.shards[0]
                .lock()
                .probation
                .iter()
                .any(|(_, probationary)| *probationary == hash)
        };
        for key in 0..1_000 {
            cache.insert(key, key);
        }
        // Unread, the late keys left probation as later keys arrived, and
        // the ghost remembers them.
        let evicted = (900..990)
            .filter(|key| !cache.contains_key(key))
            .collect::<Vec<_>>();
        assert!(
            evicted.len() > 80,
            "{} of 90 late keys were evicted",
            evicted.len()
        );
        for &key in &evicted {
            cache.insert(key, key);
            assert!(!in_probation(key), "remembered key {key} entered probation");
        }
        // A key the ghost never saw starts in probation.
        cache.insert(5_000, 5_000);
        assert!(in_probation(5_000));
    }

    #[test]
    fn a_writes_shard_state_spans_two_cache_lines() {
        // Writers' cores take turns holding a shard's lock, and each line of
        // shard state a write touches moves to its core while it holds the
        // lock. Replacing needs the lock word and the counts; an insert that
        // evicts from probation needs the probation queue, then the ghost.
        let shard = Shard::<(u64, u64)>::new(64, u64::MAX, DEFAULT_PROBATION_BPS);
        let base = std::ptr::addr_of!(shard.writer) as usize;
        let line = |field: usize, size: usize| {
            let first = (field - base) / 64;
            assert_eq!(first, (field + size - 1 - base) / 64, "a field spans lines");
            first
        };
        let lock = &shard.writer;
        assert_eq!(line(std::ptr::addr_of!(lock.len) as usize, 8), 0);
        assert_eq!(line(std::ptr::addr_of!(lock.weight) as usize, 8), 0);
        assert_eq!(line(std::ptr::addr_of!(lock.evictions) as usize, 8), 0);
        let mutex = std::ptr::addr_of!(lock.writer) as usize;
        let data = lock.writer.data_ptr() as usize;
        assert!(mutex < data, "the lock word precedes the state it guards");
        assert_eq!(line(mutex, data - mutex), 0);
        let writer = lock.writer.lock();
        let probation = std::ptr::addr_of!(writer.probation) as usize;
        assert_eq!(line(probation, std::mem::size_of_val(&writer.probation)), 0);
        let ghost = std::ptr::addr_of!(writer.ghost) as usize;
        assert_eq!(line(ghost, std::mem::size_of_val(&writer.ghost)), 1);
        let main = std::ptr::addr_of!(writer.main) as usize;
        assert_eq!(line(main, std::mem::size_of_val(&writer.main)), 1);
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn other_writers_drain_the_retired_queue_of_an_idle_thread() {
        let cache = Arc::new(ShardedCacheBuilder::new(256).shards(4).build::<u64, u64>());
        let idle_queue = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for key in 0..5_000 {
                    cache.insert(key, key);
                }
                crate::sharded_arena::retired_queue_index()
            })
            .join()
            .expect("the idle thread's writes complete")
        };
        if idle_queue == crate::sharded_arena::retired_queue_index() {
            // Thread indices 64 apart share a queue; nothing to compare.
            return;
        }
        assert!(
            cache.retired.queued(idle_queue) > 0,
            "the idle thread left retirements"
        );
        // Only this thread's periodic foreign drains reach the idle queue.
        for key in 10_000..10_000 + 40_000 * CHURN_SCALE {
            cache.insert(key, key);
        }
        assert_eq!(
            cache.retired.queued(idle_queue),
            0,
            "the idle thread's queue was not drained"
        );
    }
}
