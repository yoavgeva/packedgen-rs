//! A sharded concurrent cache with lock-free reads.
//!
//! Each shard is a fixed table of 64-byte buckets. A bucket holds ten slots:
//! a tag byte per slot and an overflow count in two words, and ten atomic
//! four-byte handles to entries in the cache's epoch-reclaimed slab (see
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
//! CLOCK queue in the order keys entered it. Each shard sizes its probation
//! to the keys it sees return. Expired entries leave first wherever eviction
//! meets them.

use std::borrow::Borrow;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::cache_arena::direct_arena_thread;
use crate::sharded_arena::{
    EpochPin, Epochs, MAX_EXPIRY_TICK, MAX_WEIGHT, OVERFLOW_HANDLE, RECLAIM_BATCH, Reclaimed,
    RetiredSlots, Slot, SlotDecoder, SlotStore, advise_huge_pages, drop_value, fill, prefetch,
    prefetch_write,
};
#[cfg(test)]
use crate::sharded_arena::{PREFETCHED, RETIRE_LOCKS, STACK_BORROWS, STACK_SEARCHES};

const SLOTS: usize = 10;
/// Slots whose tags fill a bucket's tag word; the rest are in its meta word.
const WORD_SLOTS: usize = 8;
const TAG_MASK: u64 = 0xff;
/// The meta word's count of keys that probed past the bucket.
const OVERFLOW_SHIFT: u32 = 16;
const OVERFLOW_MAX: u64 = 0xff;
/// The meta word marks a key living in its alternate bucket at this bit
/// plus its slot.
const DISPLACED_SHIFT: usize = 24;
/// The meta word holds the placement counts of the slots past `WORD_SLOTS`
/// from this bit.
const META_GENERATION_SHIFT: usize = 40;
/// Meta bits a clear keeps: placement counts.
const META_GENERATIONS: u64 = 0xffff << META_GENERATION_SHIFT;
/// Table slots per entry of capacity: about 80% load when the shard is full.
const SLOTS_PER_ENTRY_NUMERATOR: usize = 5;
const SLOTS_PER_ENTRY_DENOMINATOR: usize = 4;
/// Shards hold at least this many entries, so small caches use fewer shards.
const MIN_SHARD_CAPACITY: usize = 64;
/// Shards past four per thread hold at least this many entries, enough that
/// splitting the cache further leaves hit ratios unchanged.
const WIDE_SHARD_CAPACITY: usize = 256;
/// Main-queue records encode a slot in 24 bits.
const MAX_SHARD_SLOTS: usize = 1 << 24;
/// Slab region slots beyond the entry limit, for entries removed but not
/// yet reclaimed: well past what reclamation trails by on one thread, about
/// a hundred while few threads have claimed reader shards.
const REGION_SPARE: usize = 1_024;
/// More region slots per available thread, for each thread's stack of
/// vacant slots and its share of the reclamation backlog.
const REGION_SPARE_PER_THREAD: usize = 256;
/// Main-queue records compaction examines per append.
const MAIN_CLEANUP_CHUNK: usize = 64;
const NEVER_EXPIRES: u64 = u64::MAX;
/// Probation's default starting share of each shard, in basis points. Each
/// shard then adapts it (see [`Filters`]).
const DEFAULT_PROBATION_BPS: u16 = 25;
/// Probation never takes more than this share of its shard, in basis points.
const MAX_PROBATION_BPS: usize = 2_500;
/// Entries a returning key the ghost's current generation remembers adds to
/// the probation target.
const PROBATION_GROWTH: usize = 1;
/// Entries a returning key the main filter remembers takes from it.
const PROBATION_SHRINKAGE: usize = 2;
/// The ghost holds this many generations of evicted probationary keys, and
/// a key's word in each fills one cache line.
const GHOST_GENERATIONS: usize = GENERATION_COUNT as usize;
/// A key remembered in the ghost adds this to its count, which keeps the
/// current generation's index below it.
const GENERATION_COUNT: u32 = 8;
/// Ghost lines one remembered key clears ahead of the next generation.
const GHOST_CLEARED_LINES: usize = 32;
/// Each ghost generation, and the main filter, holds this fraction of the
/// shard's capacity.
const GHOST_GENERATION_DIVISOR: usize = 16;
const GHOST_BITS_PER_KEY: usize = 32;
const GHOST_KEY_BITS: u32 = 5;
/// Deadlines are 100 ms ticks since the cache was built, kept in the entry's
/// 31-bit expiry field: about 6.8 years of range.
const EXPIRY_TICK_NANOS: u128 = 100_000_000;
/// Hit and miss counts spread over this many cache lines, by thread.
const STAT_STRIPES: usize = 32;
/// Inserts an `insert_many` batch prefetches for and runs under one pin.
const INSERT_MANY_CHUNK: usize = 32;

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
    /// Replaced by a later insert for the same key, including one whose
    /// value is too heavy to cache, which leaves the key absent.
    Replaced,
    /// Removed by [`ShardedCache::remove`], [`ShardedCache::remove_if`],
    /// [`ShardedCache::retain`], [`ShardedCache::drain`], or
    /// [`ShardedCache::clear`].
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

/// Bytes a [`ShardedCache`]'s own structures use; see
/// [`ShardedCache::memory_used`]. Memory the keys and values own elsewhere,
/// such as a `String`'s buffer, is not counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShardedCacheMemory {
    /// The bucket table, allocated in full when the cache is built.
    pub table: usize,
    /// Entry slots handed out so far, holding live entries, removed ones
    /// awaiting reclamation, or nothing while they wait for reuse. Slots
    /// never handed out are not counted: a large slab region's untouched
    /// pages take no memory.
    pub entries: usize,
    /// Each shard's policy state: its filters of evicted keys, its main
    /// queue's ring, and its probation queue.
    pub policy: usize,
    /// Everything else: the epochs' reader counters, the queues of removed
    /// entries awaiting reclamation, the shards' locks and counters, and the
    /// lanes of loads in progress. The counters and queues take about 33 KB
    /// whatever the capacity (41 KB on Linux, which adds announcement
    /// lines), and each shard about half a kilobyte more.
    pub bookkeeping: usize,
}

impl ShardedCacheMemory {
    /// The sum of the parts.
    #[must_use]
    pub fn total(&self) -> usize {
        self.table + self.entries + self.policy + self.bookkeeping
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
    max_capacity: Option<usize>,
    max_weight: Option<u64>,
    probation_bps: u16,
    shards: Option<usize>,
    time_to_live: Option<Duration>,
    time_to_idle: Option<Duration>,
    record_stats: bool,
    weighter: W,
    hash_builder: S,
    listener: L,
}

impl ShardedCacheBuilder {
    /// Starts a cache holding at most `max_entries` entries. Its table is
    /// allocated when the cache is built, about 8 bytes per entry, so a
    /// limit no allocation can hold aborts the process as any failed
    /// allocation does; there is no unbounded setting.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            max_capacity: None,
            max_weight: None,
            probation_bps: DEFAULT_PROBATION_BPS,
            shards: None,
            time_to_live: None,
            time_to_idle: None,
            record_stats: false,
            weighter: UnitWeighter,
            hash_builder: DefaultHashBuilder::default(),
            listener: NoListener,
        }
    }
}

impl<W, S, L> ShardedCacheBuilder<W, S, L> {
    /// Also limits the summed weight of the entries; see [`Self::weighter`].
    ///
    /// Each shard holds an equal share of the limit, so a value heavier than
    /// `max_weight` divided by the shard count is never cached: with 256
    /// shards and a 1-GiB limit, one over 4 MiB. Fewer shards, set with
    /// [`Self::shards`], let heavier values in.
    #[must_use]
    pub fn max_weight(mut self, max_weight: u64) -> Self {
        self.max_weight = Some(max_weight);
        self
    }

    /// Lets [`ShardedCache::set_capacity`] raise the entry limit up to
    /// `max_capacity`. Tables are never rebuilt, so the table, the slab
    /// region, and the shard count are sized for `max_capacity` from the
    /// start: about 8 bytes of table per reserved entry beyond the entry
    /// limit. Without it, a cache can shrink and grow back up to the
    /// capacity it was built with.
    #[must_use]
    pub fn max_capacity(mut self, max_capacity: usize) -> Self {
        self.max_capacity = Some(max_capacity);
        self
    }

    /// Sets the probationary FIFO's starting share of each shard in basis
    /// points (default 25). Each shard then adapts the share, between one
    /// entry and a quarter of the shard, to the keys it sees return.
    #[must_use]
    pub fn probation_bps(mut self, probation_bps: u16) -> Self {
        self.probation_bps = probation_bps;
        self
    }

    /// Sets the shard count, rounded up to a power of two and capped so each
    /// shard holds at least one entry. By default it is four per available
    /// thread, fewer for small caches, and up to sixteen per thread for
    /// caches large enough to give each shard at least 256 entries.
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        // Above the largest power of two, `build` caps the count anyway.
        self.shards = Some(
            shards
                .max(1)
                .checked_next_power_of_two()
                .unwrap_or(1 << (usize::BITS - 1)),
        );
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

    /// Expires entries `time_to_idle` after they were last written or read.
    ///
    /// Reads that mark entries, such as [`ShardedCache::get`] and
    /// [`ShardedCacheGuard::get`], move a live entry's deadline later, to
    /// `time_to_idle` after the read; [`ShardedCache::peek`],
    /// [`ShardedCache::contains_key`], and iteration do not. A read never
    /// moves a deadline earlier, so an entry inserted with
    /// [`ShardedCache::insert_with_ttl`] expires at the later of its own
    /// deadline and `time_to_idle` after its last read. Deadlines are kept
    /// in 100 ms ticks, as for [`Self::time_to_live`].
    #[must_use]
    pub fn time_to_idle(mut self, time_to_idle: Duration) -> Self {
        self.time_to_idle = Some(time_to_idle);
        self
    }

    /// Counts hits and misses for [`ShardedCache::stats`]. Each counted read
    /// adds an uncontended atomic increment. [`ShardedCache::get`],
    /// [`ShardedCacheGuard::get`], and each call of the `get_or_insert` and
    /// `get_value_or_guard` families count one read; [`ShardedCache::peek`],
    /// [`ShardedCache::contains_key`], and iteration do not.
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
            max_capacity: self.max_capacity,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            time_to_idle: self.time_to_idle,
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
            max_capacity: self.max_capacity,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            time_to_idle: self.time_to_idle,
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
            max_capacity: self.max_capacity,
            max_weight: self.max_weight,
            probation_bps: self.probation_bps,
            shards: self.shards,
            time_to_live: self.time_to_live,
            time_to_idle: self.time_to_idle,
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
    /// Panics if one shard would need 2^32 or more table slots, or if both
    /// a time to live and a time to idle are set: an entry keeps one
    /// deadline.
    #[must_use]
    pub fn build<K, V>(self) -> ShardedCache<K, V, W, S, L>
    where
        K: Hash + Eq,
        W: Weighter<K, V>,
        S: BuildHasher,
        L: RemovalListener<K, V>,
    {
        assert!(
            self.time_to_live.is_none() || self.time_to_idle.is_none(),
            "a cache expires entries after a time to live or a time to idle, not both"
        );
        let max_entries = self.max_entries.max(1);
        // Tables, the region, and the shard count are sized for the reserve.
        let reserve = self.max_capacity.unwrap_or(0).max(max_entries);
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        let shards = self
            .shards
            .unwrap_or_else(|| default_shards(reserve, parallelism));
        // Every shard holds an entry: a shard with none would admit one.
        let most = 1 << reserve.ilog2();
        // Main-queue records hold a slot in 24 bits.
        let mut shards = shards.min(most);
        while (reserve / shards).saturating_mul(SLOTS_PER_ENTRY_NUMERATOR)
            / SLOTS_PER_ENTRY_DENOMINATOR
            >= MAX_SHARD_SLOTS - SLOTS
        {
            shards *= 2;
        }
        let max_entries = max_entries.max(shards);
        let max_weight = self.max_weight.unwrap_or(u64::MAX);
        let shard_reserve = reserve.div_ceil(shards);
        // Shards share one bucket count, sized for the largest share.
        let shard_slots = (shard_reserve * SLOTS_PER_ENTRY_NUMERATOR)
            .div_ceil(SLOTS_PER_ENTRY_DENOMINATOR)
            .max(SLOTS * 2);
        let shard_buckets = shard_slots.div_ceil(SLOTS);
        let epochs = Epochs::new();
        assert!(
            u32::try_from(shard_buckets * SLOTS).is_ok(),
            "a shard holds fewer than 2^32 slots"
        );
        ShardedCache {
            shards: (0..shards)
                .map(|index| {
                    Shard::new(
                        share(max_entries, shards, index),
                        weight_share(max_weight, shards, index),
                        self.probation_bps,
                    )
                })
                .collect(),
            table: empty_table(shards * shard_buckets),
            shard_buckets,
            shard_shift: u64::BITS - shards.trailing_zeros(),
            retired: RetiredSlots::new(epochs.announcers()),
            epochs,
            slots: SlotStore::new(
                reserve
                    .saturating_add(REGION_SPARE)
                    .saturating_add(REGION_SPARE_PER_THREAD.saturating_mul(parallelism)),
            ),
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
            time_to_idle: self.time_to_idle,
            capacity: AtomicUsize::new(max_entries),
            max_capacity: reserve,
            max_weight: AtomicU64::new(max_weight),
            resizing: Mutex::new(()),
            #[cfg(test)]
            clock_skew_nanos: AtomicU64::new(0),
        }
    }
}

/// Shard `index`'s share of `total` over `shards` shards; the first shards
/// take the remainder.
fn share(total: usize, shards: usize, index: usize) -> usize {
    total / shards + usize::from(index < total % shards)
}

/// Shard `index`'s share of the weight limit `total`; `u64::MAX`, no limit,
/// for each shard when there is none.
fn weight_share(total: u64, shards: usize, index: usize) -> u64 {
    if total == u64::MAX {
        return u64::MAX;
    }
    let shards = u64::try_from(shards).expect("the shard count fits u64");
    let index = u64::try_from(index).expect("a shard index fits u64");
    total / shards + u64::from(index < total % shards)
}

/// The default shard count for `max_entries` on `parallelism` threads: four
/// shards per thread, fewer for small caches, and up to sixteen per thread
/// for caches large enough to give each at least `WIDE_SHARD_CAPACITY`
/// entries. Writers of different keys then rarely wait on one shard's lock.
fn default_shards(max_entries: usize, parallelism: usize) -> usize {
    let narrow = (parallelism * 4).next_power_of_two().min(
        (max_entries / MIN_SHARD_CAPACITY)
            .max(1)
            .next_power_of_two(),
    );
    let wide_limit = max_entries / WIDE_SHARD_CAPACITY;
    if wide_limit == 0 {
        return narrow;
    }
    let wide = (parallelism * 16)
        .next_power_of_two()
        .min(1 << wide_limit.ilog2());
    narrow.max(wide)
}

/// A bounded concurrent cache with lock-free reads and sharded writers.
///
/// `get` clones the value, so `V` is typically small or an `Arc`; [`Self::pin`]
/// borrows values instead. Limits apply per shard: each holds its share of
/// the entry and weight limits, as in other sharded caches, so a cache can
/// evict a little before it is full overall.
///
/// Keys and values must support concurrent sharing to share the cache:
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<packedgen::ShardedCache<u64, std::cell::Cell<u8>>>();
/// ```
///
/// They must also support transfer between threads:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<packedgen::ShardedCache<u64, std::rc::Rc<u8>>>();
/// ```
///
/// Such a cache is both:
///
/// ```
/// fn assert_send_sync<T: Send + Sync>() {}
/// assert_send_sync::<packedgen::ShardedCache<u64, String>>();
/// ```
///
/// A pin stays on the thread that took it:
///
/// ```compile_fail
/// fn assert_send<T: Send>(_: &T) {}
/// let cache: packedgen::ShardedCache<u64, u64> = packedgen::ShardedCache::new(10);
/// assert_send(&cache.pin());
/// ```
pub struct ShardedCache<K, V, W = UnitWeighter, S = DefaultHashBuilder, L = NoListener> {
    shards: Box<[Shard<(K, V)>]>,
    /// Every shard's buckets, `shard_buckets` per shard in shard order.
    table: Box<[Bucket]>,
    shard_buckets: usize,
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
    time_to_idle: Option<Duration>,
    capacity: AtomicUsize,
    /// The most `set_capacity` allows; tables are sized for it.
    max_capacity: usize,
    max_weight: AtomicU64,
    /// Held by `set_capacity` and `set_max_weight`, so each leaves every
    /// shard with its own shares.
    resizing: Mutex<()>,
    #[cfg(test)]
    clock_skew_nanos: AtomicU64,
}

/// A shard with its buckets: everything a write to it touches.
struct ShardView<'cache, T> {
    state: &'cache Shard<T>,
    buckets: &'cache [Bucket],
    slots: SlotDecoder<'cache, T>,
}

impl<T> Clone for ShardView<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ShardView<'_, T> {}

impl<T> Deref for ShardView<'_, T> {
    type Target = Shard<T>;

    #[inline]
    fn deref(&self) -> &Shard<T> {
        self.state
    }
}

/// Prints the cache's counts and limits, not its entries, so a type holding
/// a cache can derive `Debug` whatever its keys and values are.
impl<K, V, W, S, L> std::fmt::Debug for ShardedCache<K, V, W, S, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let max_weight = self.max_weight.load(Ordering::Relaxed);
        f.debug_struct("ShardedCache")
            .field(
                "len",
                &self
                    .shards
                    .iter()
                    .map(|shard| shard.writer.len.load(Ordering::Relaxed))
                    .sum::<usize>(),
            )
            .field(
                "weight",
                &self
                    .shards
                    .iter()
                    .map(|shard| shard.writer.weight.load(Ordering::Relaxed))
                    .sum::<u64>(),
            )
            .field("capacity", &self.capacity.load(Ordering::Relaxed))
            .field(
                "max_weight",
                &(max_weight != u64::MAX).then_some(max_weight),
            )
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

/// One shard's loads in progress. Listed and unlisted only under the shard
/// lock, which every write to the shard holds while it supersedes the loads
/// of its key.
#[repr(align(128))]
struct FlightLane<K, V>(Mutex<Vec<Arc<Flight<K, V>>>>);

/// A load of one missing key that later callers wait for.
struct Flight<K, V> {
    hash: u64,
    key: K,
    progress: Mutex<FlightProgress<V>>,
    /// Blocking waiters wait on it; asynchronous ones leave their wakers.
    landed: Condvar,
    /// Set under the shard lock by the write that unlists the flight: the
    /// insert of its value, or a write to the key while it loads, which
    /// supersedes the load. The load may have read its source before that
    /// write, so its value then goes to its callers but is not cached.
    unlisted: AtomicBool,
}

/// A flight's outcome so far, and the wakers of its asynchronous waiters.
/// Waiters check the state and leave their wakers under one lock, so none
/// can miss the landing.
struct FlightProgress<V> {
    state: FlightState<V>,
    /// By the index each waiter holds; a waiter dropped before the landing
    /// clears its own.
    wakers: Vec<Option<Waker>>,
    /// Indices of cleared wakers, which later waiters reuse: waiters that
    /// give up and retry during a long load take no more room than those
    /// waiting at once.
    vacant: Vec<usize>,
}

enum FlightState<V> {
    Loading,
    Loaded(V),
    /// The loader failed or panicked, or its future was dropped; waiters
    /// look again.
    Abandoned,
}

impl<K, V: Clone> Flight<K, V> {
    fn new(hash: u64, key: K) -> Self {
        Self {
            hash,
            key,
            progress: Mutex::new(FlightProgress {
                state: FlightState::Loading,
                wakers: Vec::new(),
                vacant: Vec::new(),
            }),
            landed: Condvar::new(),
            unlisted: AtomicBool::new(false),
        }
    }

    /// Blocks until the flight lands, and returns its value, or `None` if
    /// the load was abandoned.
    fn wait(&self) -> Option<V> {
        match self.wait_until(None) {
            Landed::Loaded(value) => Some(value),
            Landed::Abandoned | Landed::TimedOut => None,
        }
    }

    /// Blocks until the flight lands or `deadline` passes. A deadline
    /// already passed returns before parking, so a zero timeout never
    /// waits.
    fn wait_until(&self, deadline: Option<Instant>) -> Landed<V> {
        let mut progress = self.progress.lock();
        loop {
            match &progress.state {
                FlightState::Loaded(value) => return Landed::Loaded(value.clone()),
                FlightState::Abandoned => return Landed::Abandoned,
                FlightState::Loading => {}
            }
            match deadline {
                None => self.landed.wait(&mut progress),
                Some(deadline) if Instant::now() >= deadline => return Landed::TimedOut,
                Some(deadline) => {
                    self.landed.wait_until(&mut progress, deadline);
                }
            }
        }
    }
}

/// How a wait for a flight ended.
enum Landed<V> {
    Loaded(V),
    Abandoned,
    TimedOut,
}

/// What [`ShardedCache::get_value_or_guard`] found.
pub enum GuardResult<'cache, K, V, W = UnitWeighter, S = DefaultHashBuilder, L = NoListener> {
    /// The key's live value, or the value another caller's load produced
    /// while this one waited.
    Value(V),
    /// The key had no value and no load in progress: this caller loads it,
    /// and inserts it through the guard while other callers wait.
    Guard(PlaceholderGuard<'cache, K, V, W, S, L>),
    /// Another caller was still loading the key when the timeout passed.
    Timeout,
}

/// The right to load one missing key; see
/// [`ShardedCache::get_value_or_guard`].
///
/// Other callers that miss the key, through `get_value_or_guard` or the
/// `get_or_insert` methods, wait until the guard inserts or is dropped. A
/// guard dropped without inserting abandons the load, and one waiter takes
/// it over. A write to the key meanwhile supersedes the guard: its insert
/// then caches nothing, and callers that miss the key after the write load
/// it again.
pub struct PlaceholderGuard<'cache, K, V, W = UnitWeighter, S = DefaultHashBuilder, L = NoListener>
{
    cache: &'cache ShardedCache<K, V, W, S, L>,
    landing: FlightLanding<'cache, K, V>,
    key: K,
}

impl<K, V, W, S, L> PlaceholderGuard<'_, K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    /// The key this guard loads.
    #[must_use]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Inserts `value` for the key, as [`ShardedCache::insert`] would, and
    /// hands it to the callers waiting. Returns whether it was cached: not
    /// if a write to the key since the guard was handed out (an insert, a
    /// removal, a clear, or a drain) superseded the load, as for
    /// [`ShardedCache::get_or_try_insert_with`].
    ///
    /// # Panics
    ///
    /// As for [`ShardedCache::insert`].
    pub fn insert(self, value: V) -> bool
    where
        V: Clone,
    {
        let Self {
            cache,
            mut landing,
            key,
        } = self;
        let cached = cache.insert_loaded(&landing.flight, key, value.clone());
        landing.loaded = Some(value);
        cached
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug, W, S, L> std::fmt::Debug
    for GuardResult<'_, K, V, W, S, L>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(value) => f.debug_tuple("Value").field(value).finish(),
            Self::Guard(guard) => f.debug_tuple("Guard").field(guard).finish(),
            Self::Timeout => f.write_str("Timeout"),
        }
    }
}

impl<K: std::fmt::Debug, V, W, S, L> std::fmt::Debug for PlaceholderGuard<'_, K, V, W, S, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaceholderGuard")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

/// Waits for a flight to land without blocking its thread, and yields its
/// value, or `None` if the load was abandoned.
struct FlightWait<'flight, K, V> {
    flight: &'flight Flight<K, V>,
    /// This waiter's index among the flight's wakers, once it left one.
    waker: Option<usize>,
}

impl<K, V: Clone> Future for FlightWait<'_, K, V> {
    type Output = Option<V>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<V>> {
        let flight = self.flight;
        let mut progress = flight.progress.lock();
        match &progress.state {
            FlightState::Loaded(value) => return Poll::Ready(Some(value.clone())),
            FlightState::Abandoned => return Poll::Ready(None),
            FlightState::Loading => {}
        }
        let waker = context.waker();
        if let Some(index) = self.waker {
            let left = &mut progress.wakers[index];
            if !left.as_ref().is_some_and(|left| left.will_wake(waker)) {
                *left = Some(waker.clone());
            }
        } else {
            let index = if let Some(index) = progress.vacant.pop() {
                progress.wakers[index] = Some(waker.clone());
                index
            } else {
                progress.wakers.push(Some(waker.clone()));
                progress.wakers.len() - 1
            };
            drop(progress);
            self.waker = Some(index);
        }
        Poll::Pending
    }
}

impl<K, V> Drop for FlightWait<'_, K, V> {
    fn drop(&mut self) {
        if let Some(index) = self.waker {
            // The landing took the wakers if it came first.
            let mut progress = self.flight.progress.lock();
            if let Some(left) = progress.wakers.get_mut(index) {
                *left = None;
                progress.vacant.push(index);
            }
        }
    }
}

/// Where an inserted value comes from.
enum Origin<'flight, K, V> {
    /// A caller's write, expiring after this time to live if any.
    Write(Option<Duration>),
    /// A load's value, with the builder's time to live, cached unless a
    /// write superseded the load.
    Load(&'flight Flight<K, V>),
}

impl<K, V> Clone for Origin<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for Origin<'_, K, V> {}

/// What a caller that missed a key does next.
enum Missed<K, V> {
    /// A load landed after the miss and inserted this value.
    Found(V),
    /// Wait for this flight's load.
    Wait(Arc<Flight<K, V>>),
    /// Load the key: this caller listed this flight.
    Lead(Arc<Flight<K, V>>),
}

/// Unlists a leader's flight when dropped and hands `loaded` to its waiters,
/// or abandons the load if there is none, as when the loader fails or panics.
struct FlightLanding<'lane, K, V> {
    shard: &'lane Shard<(K, V)>,
    lane: &'lane FlightLane<K, V>,
    flight: Arc<Flight<K, V>>,
    loaded: Option<V>,
}

impl<K, V> Drop for FlightLanding<'_, K, V> {
    fn drop(&mut self) {
        // The insert of its value, or a write that superseded it, has
        // unlisted the flight already, and with the release of the flag,
        // every waiter that joined it is counted below.
        if !self.flight.unlisted.load(Ordering::Acquire) {
            let mut writer = self.shard.lock();
            let mut flights = self.lane.0.lock();
            if let Some(index) = flights
                .iter()
                .position(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                flights.swap_remove(index);
                writer.unlisted();
            }
        }
        // Waiters join under the lane lock, so once the flight is unlisted
        // the count only falls; alone, the leader has nobody to tell.
        if Arc::strong_count(&self.flight) > 1 {
            let wakers = {
                let mut progress = self.flight.progress.lock();
                progress.state = self
                    .loaded
                    .take()
                    .map_or(FlightState::Abandoned, FlightState::Loaded);
                std::mem::take(&mut progress.wakers)
            };
            self.flight.landed.notify_all();
            for waker in wakers.into_iter().flatten() {
                waker.wake();
            }
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

/// Ten slots in one cache line. Writers store to the tag and meta words
/// with release, as readers acquire both, and only under the shard lock.
#[repr(C, align(64))]
struct Bucket {
    /// Tag bytes for slots 0 to 7; zero marks an empty slot.
    tags: AtomicU64,
    /// Tag bytes for slots 8 and 9, then the count of keys that probed past
    /// this bucket. Writers also keep here a bit per slot set while its key
    /// lives in its alternate bucket, and slots 8 and 9's placement counts.
    meta: AtomicU64,
    handles: [AtomicU32; SLOTS],
    /// A byte per slot 0 to 7 counting its placements, so a main-queue
    /// record of a slot can tell its key from a later one. Only writers use
    /// it.
    generations: AtomicU64,
}

impl Bucket {
    /// Puts `handle` in `slot` and returns the handle it held. Requires the
    /// shard lock, under which writers make every change to a handle: the
    /// load sees the current handle, and the lock orders the entry it names
    /// before this writer. Readers acquire the entry `handle` names through
    /// the release store. Readers still holding the old handle are covered
    /// by the writer's pin, as for any unpublication, so a swap's
    /// read-modify-write would buy nothing.
    #[inline]
    fn exchange(&self, slot: usize, handle: u32) -> u32 {
        let current = &self.handles[slot];
        let previous = current.load(Ordering::Relaxed);
        current.store(handle, Ordering::Release);
        previous
    }

    /// The word holding `slot`'s tag, and the tag's shift in it.
    #[inline]
    fn tag_word(&self, slot: usize) -> (&AtomicU64, usize) {
        if slot < WORD_SLOTS {
            (&self.tags, slot * 8)
        } else {
            (&self.meta, (slot - WORD_SLOTS) * 8)
        }
    }

    /// `slot`'s tag. Requires the shard lock.
    #[inline]
    fn tag(&self, slot: usize) -> u8 {
        let (word, shift) = self.tag_word(slot);
        (word.load(Ordering::Relaxed) >> shift).to_le_bytes()[0]
    }

    /// `slot`'s placement count. Requires the shard lock.
    #[inline]
    fn generation(&self, slot: usize) -> u8 {
        let word = if slot < WORD_SLOTS {
            self.generations.load(Ordering::Relaxed) >> (slot * 8)
        } else {
            self.meta.load(Ordering::Relaxed) >> (META_GENERATION_SHIFT + (slot - WORD_SLOTS) * 8)
        };
        word.to_le_bytes()[0]
    }

    /// Whether the key in `slot` lives in its alternate bucket.
    #[inline]
    fn is_displaced(&self, slot: usize) -> bool {
        self.meta.load(Ordering::Relaxed) >> (DISPLACED_SHIFT + slot) & 1 != 0
    }

    /// The lowest empty slot. Requires the shard lock.
    #[inline]
    fn empty_slot(&self) -> Option<usize> {
        // The lowest flag of `zero_tag_bytes` is always exact.
        let empty = zero_tag_bytes(self.tags.load(Ordering::Relaxed));
        if empty != 0 {
            return Some(empty.trailing_zeros() as usize / 8);
        }
        let empty = zero_tag_bytes(self.meta.load(Ordering::Relaxed)) & META_TAG_HIGH_BITS;
        (empty != 0).then(|| WORD_SLOTS + empty.trailing_zeros() as usize / 8)
    }

    /// Adds `delta` to the count of keys that probed past this bucket. A
    /// saturated count stays saturated: lookups past it are only slower,
    /// never wrong. Requires the shard lock.
    #[inline]
    fn add_overflow(&self, delta: i8) {
        let meta = self.meta.load(Ordering::Relaxed);
        let count = overflow(meta);
        debug_assert!(
            delta > 0 || count != 0,
            "a key in its alternate is counted at home"
        );
        if count < OVERFLOW_MAX {
            let count = count.wrapping_add_signed(i64::from(delta)) & OVERFLOW_MAX;
            self.meta.store(
                meta & !(OVERFLOW_MAX << OVERFLOW_SHIFT) | count << OVERFLOW_SHIFT,
                Ordering::Release,
            );
        }
    }
}

/// A shard's writer state. Its buckets are its stretch of the cache's table,
/// which readers find from the hash without loading the shard. The limits
/// change only when the cache is resized, so their line stays in every
/// core's cache; the writer lock and the state it guards change on every
/// write, so they sit on cache lines of their own.
struct Shard<T> {
    limits: ShardLimits,
    writer: ShardLock,
    /// Tables publish handles to `Slot<T>`s; the cache's `RetiredSlots` and
    /// `SlotStore` carry the ownership bounds.
    _entries: PhantomData<fn() -> T>,
}

/// A shard's limits. Resizes store them under the shard lock, where writers
/// read them; atomics only so that resizes can store them through a shared
/// shard.
struct ShardLimits {
    capacity: AtomicUsize,
    weight: AtomicU64,
    /// The largest probation target the shard adapts to.
    probation_max: AtomicUsize,
    /// The loads listed in the shard's flight lane. Every write reads it
    /// under the shard lock, to supersede its key's load if one is listed,
    /// so it sits on this line, which inserts read anyway and which
    /// changes only as a resize or a load starts or ends.
    loads: AtomicUsize,
}

impl ShardLimits {
    fn new(capacity: usize, weight: u64) -> Self {
        let limits = Self {
            capacity: AtomicUsize::new(0),
            weight: AtomicU64::new(weight),
            probation_max: AtomicUsize::new(0),
            loads: AtomicUsize::new(0),
        };
        limits.set_capacity(capacity);
        limits
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    #[inline]
    fn weight(&self) -> u64 {
        self.weight.load(Ordering::Relaxed)
    }

    #[inline]
    fn probation_max(&self) -> usize {
        self.probation_max.load(Ordering::Relaxed)
    }

    /// Requires the shard lock.
    #[inline]
    fn loads(&self) -> usize {
        self.loads.load(Ordering::Relaxed)
    }

    /// Requires the shard lock.
    fn set_loads(&self, loads: usize) {
        self.loads.store(loads, Ordering::Relaxed);
    }

    fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
        self.probation_max.store(
            (capacity * MAX_PROBATION_BPS / 10_000).max(1),
            Ordering::Relaxed,
        );
    }

    #[inline]
    fn main_limit(&self) -> usize {
        let capacity = self.capacity();
        capacity + capacity / 4 + 16
    }
}

/// The writer lock, between the counts writers keep under it. The counts are
/// atomics so that `len`, `weight`, and `stats` read them without the lock.
///
/// Writers' cores take turns with a shard, and each line of its state a write
/// touches moves to the writer's core while it holds the lock. So the counts
/// every write changes share the lock word's line with the probation queue,
/// and the filters and the main queue fill the next line.
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
        #[cfg(test)]
        SHARD_LOCKS.with(|count| count.set(count.get() + 1));
        LockedShard {
            counts: self,
            limits,
            writer: self.writer.lock(),
        }
    }
}

#[repr(C)]
struct ShardWriter {
    /// Probationary keys in insertion order. A key moved to its other bucket
    /// leaves probation for the main area.
    probation: VecDeque<Probationary>,
    filters: Filters,
    main: MainQueue,
    /// The CLOCK hand: the next slot the main-area sweep examines.
    hand: usize,
}

/// A probationary key: its slot record, its hash, and the handle it was
/// published with, whose entry evictions prefetch alongside its bucket.
#[derive(Clone, Copy)]
struct Probationary {
    hash: u64,
    record: u32,
    handle: u32,
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

    /// The loads listed in the shard's flight lane.
    #[inline]
    fn loads(&self) -> usize {
        self.limits.loads()
    }

    /// Accounts for a load listed in the shard's flight lane.
    fn listed(&mut self) {
        self.limits.set_loads(self.loads() + 1);
    }

    /// Accounts for a load unlisted from the shard's flight lane.
    fn unlisted(&mut self) {
        self.limits.set_loads(self.loads() - 1);
    }

    #[inline]
    fn weight(&self) -> u64 {
        self.counts.weight.load(Ordering::Relaxed)
    }

    #[inline]
    fn over_limit(&self, incoming_weight: u64) -> bool {
        self.len() >= self.limits.capacity()
            || self.weight().saturating_add(incoming_weight) > self.limits.weight()
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

    /// Enrolls a key just placed at `record`: in the main area if the
    /// filters remember it from a recent eviction, otherwise on probation.
    #[allow(
        clippy::inline_always,
        reason = "part of the insert body, which inlines into each caller"
    )]
    #[inline(always)]
    fn enroll<T>(&mut self, shard: ShardView<'_, T>, hash: u64, record: u32, handle: u32) {
        if self.filters.admit(hash, shard.limits.probation_max()) {
            // Remembered from a recent eviction: straight to main.
            self.enter_main(shard, record);
        } else {
            if self.probation.len() >= self.filters.probation_record_limit() {
                // An overflowing record's key joins the main area.
                let overflow = self
                    .probation
                    .pop_front()
                    .expect("probation is at its limit");
                self.enter_main(shard, overflow.record);
            }
            self.probation.push_back(Probationary {
                hash,
                record,
                handle,
            });
        }
    }

    /// Appends a slot record to the main area's queue. Within a sixteenth
    /// of its limit, each append first compacts a chunk of the queue, so no
    /// write holds the shard lock for a whole-queue pass. A walk over the
    /// queue takes at most one append per chunk, and the remaining room
    /// lasts two walks, which drop every record stale when the first began:
    /// at least a sixth of the shard, as live records name distinct keys.
    fn enter_main<T>(&mut self, shard: ShardView<'_, T>, record: u32) {
        let limit = self.limits.main_limit();
        if self.main.span() >= limit - limit / 16 {
            self.main.compact(
                MAIN_CLEANUP_CHUNK,
                |record| shard.prefetch_record(record),
                |record| {
                    #[cfg(test)]
                    MAIN_RECORDS_EXAMINED.with(|count| count.set(count.get() + 1));
                    shard.is_live(record)
                },
            );
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
    /// The entry limit.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// The most entries [`Self::set_capacity`] allows: the builder's
    /// [`max_capacity`](ShardedCacheBuilder::max_capacity), or else the
    /// capacity the cache was built with.
    #[must_use]
    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// The weight limit; `u64::MAX` when weight is unlimited.
    #[must_use]
    pub fn max_weight(&self) -> u64 {
        self.max_weight.load(Ordering::Relaxed)
    }

    /// Sets the entry limit, and returns the limit set: `max_entries`,
    /// raised to one entry per shard or lowered to [`Self::max_capacity`].
    ///
    /// Each shard over its new share evicts down to it before this returns,
    /// as inserts would evict, and the removal listener hears
    /// [`RemovalCause::Size`] for each. A shard whose share changes starts
    /// its filters of evicted keys afresh, sized for the new share.
    pub fn set_capacity(&self, max_entries: usize) -> usize {
        let _resizing = self.resizing.lock();
        let max_entries = max_entries.clamp(self.shards.len(), self.max_capacity);
        self.capacity.store(max_entries, Ordering::Relaxed);
        self.apply_limits();
        max_entries
    }

    /// Sets the weight limit; `u64::MAX` removes it. Each shard over its new
    /// share evicts down to it before this returns, as for
    /// [`Self::set_capacity`]; values heavier than a shard's share are
    /// refused from then on.
    pub fn set_max_weight(&self, max_weight: u64) {
        let _resizing = self.resizing.lock();
        self.max_weight.store(max_weight, Ordering::Relaxed);
        self.apply_limits();
    }

    /// Gives each shard its share of the current limits, under its lock,
    /// and evicts from it until it is within them.
    fn apply_limits(&self) {
        let shards = self.shards.len();
        let capacity = self.capacity.load(Ordering::Relaxed);
        let max_weight = self.max_weight.load(Ordering::Relaxed);
        for index in 0..shards {
            let shard = self.view(index);
            let pin = self.epochs.pin();
            let mut removed = Removed::new();
            let mut now = None;
            {
                let mut writer = shard.lock();
                let capacity = share(capacity, shards, index);
                if capacity != shard.limits.capacity() {
                    shard.limits.set_capacity(capacity);
                    writer.main.grow(shard.limits.main_limit() + 1);
                    let target = writer
                        .filters
                        .probation_target()
                        .clamp(1, shard.limits.probation_max());
                    writer.filters = Filters::new(capacity, target);
                }
                shard
                    .limits
                    .weight
                    .store(weight_share(max_weight, shards, index), Ordering::Relaxed);
                while writer.len() > capacity || writer.weight() > shard.limits.weight() {
                    let Some((victim, cause)) =
                        self.evict(shard, &mut writer, &pin, None, &mut now)
                    else {
                        break;
                    };
                    removed.push(victim, cause);
                }
            }
            self.after_write(&removed, pin);
        }
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
        // Hashed before pinning: the pin's atomics hold back the loads after
        // them, so hashing first overlaps it with the previous read's end.
        let hash = self.hash(key);
        let pin = self.epochs.pin();
        let value = self.lookup(&pin, key, hash).cloned();
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
        let hash = self.hash(key);
        let pin = self.epochs.pin();
        self.live_entry(&pin, key, hash)
            .map(|entry| entry.value().1.clone())
    }

    /// Whether `key` has a live value; like [`Self::peek`], not a read.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(key);
        let pin = self.epochs.pin();
        self.live_entry(&pin, key, hash).is_some()
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
    /// it together, through this method or
    /// [`Self::get_or_try_insert_async`]. If the load fails or panics, its
    /// caller gets the error or the panic, and one waiter loads again with
    /// its own `load`. A `load` that reads this key through the cache
    /// deadlocks.
    ///
    /// A write to the key while it loads (an insert, a removal, a clear, or
    /// a drain) supersedes the load, which may have read its source before
    /// the write: its value still goes to the callers sharing it, but is
    /// not cached, and callers that miss the key afterwards load it again.
    /// So removing a key after updating its source leaves no older value
    /// cached.
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
        let hash = self.hash(key);
        let cached = self.lookup(&self.epochs.pin(), key, hash).cloned();
        self.count(cached.is_some());
        if let Some(value) = cached {
            return Ok(value);
        }
        let lane = &self.flights[self.shard_index(hash)];
        let mut load = Some(load);
        loop {
            match self.join_or_lead(lane, key, hash) {
                Missed::Found(value) => return Ok(value),
                Missed::Wait(flight) => {
                    if let Some(value) = flight.wait() {
                        return Ok(value);
                    }
                }
                Missed::Lead(flight) => {
                    let mut landing = self.landing(flight);
                    let load = load.take().expect("a caller leads at most one load");
                    let value = load()?;
                    self.insert_loaded(&landing.flight, key.to_owned(), value.clone());
                    landing.loaded = Some(value.clone());
                    return Ok(value);
                }
            }
        }
    }

    /// Returns the value cached for `key`, or awaits `load` and caches its
    /// value; see [`Self::get_or_try_insert_async`].
    pub async fn get_or_insert_async<Q>(&self, key: &Q, load: impl Future<Output = V>) -> V
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let Ok(value) = self
            .get_or_try_insert_async(key, async { Ok::<V, Infallible>(load.await) })
            .await;
        value
    }

    /// Returns the value cached for `key`, or awaits `load` and caches its
    /// value, waiting without blocking the thread.
    ///
    /// Works with any executor. Callers that miss the same key while it
    /// loads, through this method or [`Self::get_or_try_insert_with`], wait
    /// for that load and share its value. If the load fails, or its future
    /// is dropped before it completes, one waiter loads again with its own
    /// `load`. A write to the key while it loads supersedes the load, as for
    /// [`Self::get_or_try_insert_with`]. The future is `Send` when the
    /// cache, the key, and `load` are.
    ///
    /// # Errors
    ///
    /// Returns the error from this caller's `load`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from this caller's `load`.
    pub async fn get_or_try_insert_async<Q, E>(
        &self,
        key: &Q,
        load: impl Future<Output = Result<V, E>>,
    ) -> Result<V, E>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let hash = self.hash(key);
        let cached = self.lookup(&self.epochs.pin(), key, hash).cloned();
        self.count(cached.is_some());
        if let Some(value) = cached {
            return Ok(value);
        }
        let lane = &self.flights[self.shard_index(hash)];
        let mut load = Some(load);
        loop {
            match self.join_or_lead(lane, key, hash) {
                Missed::Found(value) => return Ok(value),
                Missed::Wait(flight) => {
                    let wait = FlightWait {
                        flight: &flight,
                        waker: None,
                    };
                    if let Some(value) = wait.await {
                        return Ok(value);
                    }
                }
                Missed::Lead(flight) => {
                    // Dropped with the future, the landing abandons the load.
                    let mut landing = self.landing(flight);
                    let load = load.take().expect("a caller leads at most one load");
                    let value = load.await?;
                    self.insert_loaded(&landing.flight, key.to_owned(), value.clone());
                    landing.loaded = Some(value.clone());
                    return Ok(value);
                }
            }
        }
    }

    /// Returns the value cached for `key`, or else a guard through which
    /// this caller inserts it while other callers wait; see
    /// [`PlaceholderGuard`].
    ///
    /// A caller that finds another's guard or load in progress waits for
    /// it, up to `timeout`, and gets its value, or the guard if it was
    /// abandoned. A `None` timeout waits as long as it takes; a zero timeout
    /// never waits. A caller holding the key's guard that asks again
    /// without a timeout deadlocks.
    pub fn get_value_or_guard<Q>(
        &self,
        key: &Q,
        timeout: Option<Duration>,
    ) -> GuardResult<'_, K, V, W, S, L>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let hash = self.hash(key);
        let cached = self.lookup(&self.epochs.pin(), key, hash).cloned();
        self.count(cached.is_some());
        if let Some(value) = cached {
            return GuardResult::Value(value);
        }
        // A timeout past the clock's range waits as long as it takes.
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let lane = &self.flights[self.shard_index(hash)];
        loop {
            match self.join_or_lead(lane, key, hash) {
                Missed::Found(value) => return GuardResult::Value(value),
                Missed::Wait(flight) => match flight.wait_until(deadline) {
                    Landed::Loaded(value) => return GuardResult::Value(value),
                    Landed::Abandoned => {}
                    Landed::TimedOut => return GuardResult::Timeout,
                },
                Missed::Lead(flight) => {
                    return GuardResult::Guard(PlaceholderGuard {
                        cache: self,
                        landing: self.landing(flight),
                        key: key.to_owned(),
                    });
                }
            }
        }
    }

    /// Returns the value cached for `key`, or else a guard through which
    /// this caller inserts it, as [`Self::get_value_or_guard`] does
    /// without a timeout, waiting without blocking the thread. The future
    /// and the guard are `Send` when the cache and the key are.
    ///
    /// # Errors
    ///
    /// Returns the guard when this caller is to load the key.
    pub async fn get_value_or_guard_async<Q>(
        &self,
        key: &Q,
    ) -> Result<V, PlaceholderGuard<'_, K, V, W, S, L>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let hash = self.hash(key);
        let cached = self.lookup(&self.epochs.pin(), key, hash).cloned();
        self.count(cached.is_some());
        if let Some(value) = cached {
            return Ok(value);
        }
        let lane = &self.flights[self.shard_index(hash)];
        loop {
            match self.join_or_lead(lane, key, hash) {
                Missed::Found(value) => return Ok(value),
                Missed::Wait(flight) => {
                    let wait = FlightWait {
                        flight: &flight,
                        waker: None,
                    };
                    if let Some(value) = wait.await {
                        return Ok(value);
                    }
                }
                Missed::Lead(flight) => {
                    return Err(PlaceholderGuard {
                        cache: self,
                        landing: self.landing(flight),
                        key: key.to_owned(),
                    });
                }
            }
        }
    }

    /// Joins the flight loading `key`, or lists a new one for this caller to
    /// lead. Under the shard lock, as writes supersede loads: a write to the
    /// key either precedes the listing, and this caller finds its value or
    /// loads after it, or supersedes the new load.
    fn join_or_lead<Q>(&self, lane: &FlightLane<K, V>, key: &Q, hash: u64) -> Missed<K, V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Clone,
    {
        let shard = self.shard(hash);
        let pin = self.epochs.pin();
        let found = {
            let mut writer = shard.lock();
            let mut flights = lane.0.lock();
            if let Some(flight) = flights
                .iter()
                .find(|flight| flight.hash == hash && flight.key.borrow() == key)
            {
                return Missed::Wait(Arc::clone(flight));
            }
            // A load that landed after this caller missed has inserted its
            // value, and unlisted its flight under this lock.
            if let Some(value) = self.lookup(&pin, key, hash) {
                value
            } else {
                let flight = Arc::new(Flight::new(hash, key.to_owned()));
                flights.push(Arc::clone(&flight));
                writer.listed();
                return Missed::Lead(flight);
            }
        };
        // Cloned outside the locks, under the pin.
        Missed::Found(found.clone())
    }

    /// The landing of `flight`, which this caller leads.
    fn landing(&self, flight: Arc<Flight<K, V>>) -> FlightLanding<'_, K, V> {
        let index = self.shard_index(flight.hash);
        FlightLanding {
            shard: &self.shards[index],
            lane: &self.flights[index],
            flight,
            loaded: None,
        }
    }

    /// Unlists the loads of `key` listed in its shard's lane, as a write to
    /// the key does, superseding any still in progress: their values go to
    /// their callers but are not cached. The insert of a load's own value
    /// unlists it the same way. Requires the shard lock; it takes no guard,
    /// so that writers need not keep theirs in memory to call it.
    #[cold]
    #[inline(never)]
    fn supersede_loads<Q>(&self, hash: u64, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.unlist_loads(self.shard_index(hash), |flight| {
            flight.hash == hash && flight.key.borrow() == key
        });
    }

    /// Unlists and supersedes the loads in shard `index`'s lane that
    /// `picks` returns true for. Requires that shard's lock.
    fn unlist_loads(&self, index: usize, mut picks: impl FnMut(&Flight<K, V>) -> bool) {
        let mut flights = self.flights[index].0.lock();
        let listed = flights.len();
        flights.retain(|flight| {
            let picked = picks(flight);
            if picked {
                flight.unlisted.store(true, Ordering::Release);
            }
            !picked
        });
        let limits = &self.shards[index].limits;
        limits.set_loads(limits.loads() - (listed - flights.len()));
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

    /// Finds `key`'s live value and marks it read. Always inlined: in a
    /// program that also loads through the cache, the compiler kept it out
    /// of line, and `get` called it.
    #[allow(
        clippy::inline_always,
        reason = "out of line once loaders call it too, it left `get` a call"
    )]
    #[inline(always)]
    fn lookup<'pin, Q>(&self, pin: &'pin EpochPin<'_>, key: &Q, hash: u64) -> Option<&'pin V>
    where
        K: Borrow<Q> + 'pin,
        V: 'pin,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.find_entry(pin, key, hash)?;
        // Entries without a deadline never read the clock.
        entry
            .mark_accessed_if_live(|| self.read_tick(entry))
            .then(|| &entry.value().1)
    }

    /// Finds `key`'s live entry without marking it read; always inlined, as
    /// [`Self::lookup`], for `peek` and `contains_key`.
    #[allow(
        clippy::inline_always,
        reason = "out of line once loaders call the lookup too, it left `peek` a call"
    )]
    #[inline(always)]
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

    /// The lookup every read shares; always inlined, as [`Self::lookup`].
    #[allow(
        unsafe_code,
        reason = "dereferences handles the table publishes under the caller's pin"
    )]
    #[allow(
        clippy::inline_always,
        reason = "out of line once loaders call it too, it left `get` a call"
    )]
    #[inline(always)]
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
        let found = self.shard(hash).find_in_region(hash, |handle| {
            // SAFETY: `find_in_region` passes handles published in this
            // cache's table while `pin` was held.
            unsafe { entry(pin, handle) }.value().0.borrow() == key
        });
        match found {
            // SAFETY: as above.
            Ok(found) => found.map(|(_, handle)| unsafe { entry(pin, handle) }),
            Err(Overflowed) => self.find_entry_decoding(pin, key, hash),
        }
    }

    /// [`Self::find_entry`] for a key whose lookup met an overflow handle.
    #[allow(
        unsafe_code,
        reason = "dereferences handles the table publishes under the caller's pin"
    )]
    #[cold]
    #[inline(never)]
    fn find_entry_decoding<'pin, Q>(
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

    /// Hashes `key` with the cache's hasher, its steps written out:
    /// `BuildHasher::hash_one` is a generic default method the compiler may
    /// keep out of line, and built with one codegen unit it did, costing
    /// resident hits 23% on Apple Silicon.
    #[allow(
        clippy::inline_always,
        clippy::manual_hash_one,
        reason = "`hash_one` and a hinted helper alike can stay out of line"
    )]
    #[inline(always)]
    fn hash<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        let mut state = self.hash_builder.build_hasher();
        key.hash(&mut state);
        state.finish()
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
        tick_at(self.elapsed_nanos())
    }

    /// The current tick for a marking read of `entry`, which has a
    /// deadline. With a time to idle, it also moves the deadline of an
    /// entry live now to the time to idle from now. Out of line, as
    /// [`Self::now`].
    #[cold]
    #[inline(never)]
    fn read_tick(&self, entry: Entry<'_, (K, V)>) -> u64 {
        let elapsed = self.elapsed_nanos();
        let now = tick_at(elapsed);
        if let Some(idle) = self.time_to_idle {
            entry.extend_deadline(deadline_at(elapsed, idle), now);
        }
        now
    }

    /// The first tick at which an entry stored now with `ttl` reads as
    /// expired.
    fn deadline(&self, ttl: Duration) -> u64 {
        deadline_at(self.elapsed_nanos(), ttl)
    }

    /// How long a new entry lives without its own time to live: the
    /// builder's time to live or time to idle, if either.
    #[inline]
    fn lifetime(&self) -> Option<Duration> {
        self.time_to_live.or(self.time_to_idle)
    }

    /// Whether `entry` has expired, reading the clock at most once per
    /// operation through `now`.
    fn expired(&self, entry: Entry<'_, (K, V)>, now: &mut Option<u64>) -> bool {
        let expires_at = entry.expires_at();
        expires_at != NEVER_EXPIRES && expires_at <= *now.get_or_insert_with(|| self.now())
    }

    /// Inserts or replaces the value for `key`, evicting from the key's shard
    /// while it is over its limits. The entry expires after the builder's
    /// [`time_to_live`](ShardedCacheBuilder::time_to_live) or
    /// [`time_to_idle`](ShardedCacheBuilder::time_to_idle), if either.
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
        self.insert_expiring(key, value, self.lifetime())
    }

    /// Like [`Self::insert`], with an entry that expires after `ttl` instead
    /// of the builder's time to live. A zero `ttl` is never readable. With a
    /// time to idle, reads can extend the entry past `ttl`; see
    /// [`ShardedCacheBuilder::time_to_idle`].
    ///
    /// # Panics
    ///
    /// As for [`Self::insert`].
    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> bool {
        self.insert_expiring(key, value, Some(ttl))
    }

    /// Inserts each pair in order, as [`Self::insert`] would one at a time,
    /// and returns how many values were cached. A batch takes one epoch pin,
    /// refreshed between chunks of inserts, instead of one per insert, and
    /// starts loading every key's home bucket before inserting the chunk.
    ///
    /// # Panics
    ///
    /// As for [`Self::insert`].
    pub fn insert_many<I>(&self, entries: I) -> usize
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut entries = entries.into_iter();
        let Some(first) = entries.next() else {
            return 0;
        };
        let Some(second) = entries.next() else {
            // Nothing to share: a chunk's buffer and pin refresh would cost
            // a lone insert more than they save.
            let (key, value) = first;
            return usize::from(self.insert(key, value));
        };
        let mut entries = [first, second].into_iter().chain(entries).peekable();
        let mut chunk = Vec::with_capacity(INSERT_MANY_CHUNK);
        let mut cached = 0;
        let mut pinned: Option<EpochPin<'_>> = None;
        while entries.peek().is_some() {
            chunk.extend(
                entries
                    .by_ref()
                    .take(INSERT_MANY_CHUNK)
                    .map(|(key, value)| (self.hash(&key), key, value)),
            );
            for &(hash, ..) in &chunk {
                let shard = self.shard(hash);
                prefetch(&raw const shard.buckets[shard.home(hash)]);
            }
            // The first chunk hashes before pinning, as an insert does. A
            // pin held past two advances would hold back every removal
            // since, the batch's own included, so later chunks refresh it.
            let pin = if let Some(pin) = &mut pinned {
                pin.refresh();
                pin
            } else {
                pinned.insert(self.epochs.pin())
            };
            let mut advance = false;
            let mut removed = Removed::new();
            for (hash, key, value) in chunk.drain(..) {
                cached += usize::from(self.insert_pinned(
                    key,
                    value,
                    Origin::Write(self.lifetime()),
                    hash,
                    pin,
                    &mut removed,
                ));
                // While a backlog is due, a retirement reclaims twice what it
                // queues, within one reclamation batch: retiring more at
                // once would leave the backlog for good.
                if removed.len >= RECLAIM_BATCH / 2 {
                    advance |= self.retire_removed(&removed, pin);
                    removed = Removed::new();
                }
            }
            if self.retire_removed(&removed, pin) || advance {
                self.epochs.try_advance();
            }
        }
        cached
    }

    fn insert_expiring(&self, key: K, value: V, ttl: Option<Duration>) -> bool {
        let hash = self.hash(&key);
        let pin = self.epochs.pin();
        let mut removed = Removed::new();
        let inserted = self.insert_pinned(key, value, Origin::Write(ttl), hash, &pin, &mut removed);
        self.after_write(&removed, pin);
        inserted
    }

    /// Inserts the value `flight` loaded, as [`Self::insert`] would, unless
    /// a write to the key superseded the load. Returns whether it was
    /// cached.
    fn insert_loaded(&self, flight: &Flight<K, V>, key: K, value: V) -> bool {
        let pin = self.epochs.pin();
        let mut removed = Removed::new();
        let inserted = self.insert_pinned(
            key,
            value,
            Origin::Load(flight),
            flight.hash,
            &pin,
            &mut removed,
        );
        self.after_write(&removed, pin);
        inserted
    }

    /// Inserts under `pin`, pushing what the insert unpublished to `removed`
    /// for the caller to retire while still pinned.
    #[allow(
        clippy::inline_always,
        reason = "`insert`'s body; out of line, the key and value passed through memory"
    )]
    #[allow(
        unsafe_code,
        reason = "fills, publishes, and unpublishes slab slots under the shard lock"
    )]
    #[inline(always)]
    fn insert_pinned(
        &self,
        key: K,
        value: V,
        origin: Origin<'_, K, V>,
        hash: u64,
        pin: &EpochPin<'_>,
        removed: &mut Removed<(K, V)>,
    ) -> bool {
        let (ttl, landing) = match origin {
            Origin::Write(ttl) => (ttl, None),
            Origin::Load(flight) => (self.lifetime(), Some(flight)),
        };
        let shard = self.shard(hash);
        let weight = self.weighter.weight(&key, &value);
        let Some(compact_weight) = u32::try_from(weight)
            .ok()
            .filter(|weight| *weight <= MAX_WEIGHT && u64::from(*weight) <= shard.limits.weight())
        else {
            // A load has no value it replaces: one written since it began
            // superseded it.
            if landing.is_none() {
                self.remove_as(&key, RemovalCause::Replaced);
            }
            return false;
        };
        let expires_at = ttl.map_or(NEVER_EXPIRES, |ttl| self.deadline(ttl));
        let mut now = None;
        let (slot, raw) = self.slots.take();
        // SAFETY: `take` handed this vacant slot to this caller alone, and
        // nothing can reach it before it is published below.
        unsafe { fill(slot, (key, value), compact_weight, expires_at) };
        let handle = Handle { raw, slot };
        // SAFETY: the caller's own filled slot, published below or never.
        let key = &unsafe { entry(pin, handle) }.value().0;
        {
            let mut writer = shard.lock();
            if !self.supersede_for_write(writer.loads(), hash, key, landing) {
                drop(writer);
                // SAFETY: the caller's own slot, filled above and never
                // published.
                unsafe { drop_value(slot) };
                self.slots.give(&[slot]);
                return false;
            }
            let found = shard.find(hash, |current| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(pin, current) }.value().0 == *key
            });
            if let Some((index, _)) = found {
                let previous =
                    shard.handle(shard.bucket(index).exchange(index % SLOTS, handle.raw));
                // SAFETY: the exchange unpublished `previous` while `pin` is
                // held.
                let previous_entry = unsafe { entry(pin, previous) };
                let reweighed = writer.weight() - previous_entry.weight() + weight;
                writer.set_weight(reweighed);
                let cause = if self.expired(previous_entry, &mut now) {
                    writer.count_expiration();
                    RemovalCause::Expired
                } else {
                    RemovalCause::Replaced
                };
                removed.push(previous, cause);
                while writer.weight() > shard.limits.weight() {
                    let Some((victim, cause)) =
                        self.evict(shard, &mut writer, pin, Some(handle), &mut now)
                    else {
                        break;
                    };
                    removed.push(victim, cause);
                }
            } else {
                if writer.over_limit(weight) {
                    shard.prefetch_eviction(&writer, hash);
                }
                while writer.over_limit(weight) {
                    let Some((victim, cause)) = self.evict(shard, &mut writer, pin, None, &mut now)
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
                    if shard.relocate_candidate(&mut writer, hash) {
                        #[cfg(test)]
                        RELOCATIONS.with(|count| count.set(count.get() + 1));
                    } else {
                        #[cfg(test)]
                        WINDOW_EVICTIONS.with(|count| count.set(count.get() + 1));
                        let (victim, cause) =
                            self.evict_candidate(shard, &mut writer, pin, hash, &mut now);
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
                writer.enroll(shard, hash, shard.record(index), handle.raw);
            }
        }
        true
    }

    /// Supersedes the loads of `key`, which a write is about to store under
    /// the shard lock, given the `loads` the shard lists. Returns false if
    /// the write lands `landing`'s load and a write superseded it first.
    #[allow(
        clippy::inline_always,
        reason = "part of the insert body, which inlines into each caller"
    )]
    #[inline(always)]
    fn supersede_for_write(
        &self,
        loads: usize,
        hash: u64,
        key: &K,
        landing: Option<&Flight<K, V>>,
    ) -> bool {
        if let Some(flight) = landing {
            // Unlisted before its own insert: a write superseded it.
            if flight.unlisted.load(Ordering::Relaxed) {
                return false;
            }
            self.supersede_loads(hash, key);
        } else if loads != 0 {
            self.supersede_loads(hash, key);
        }
        true
    }

    /// Ends a write after its shard lock, still pinned: retires what it
    /// removed, unpins, and advances the epoch generation if enough
    /// retirements are pending.
    fn after_write(&self, removed: &Removed<(K, V)>, pin: EpochPin<'_>) {
        let advance = self.retire_removed(removed, &pin);
        pin.unpin();
        if advance {
            self.epochs.try_advance();
        }
    }

    /// Queues the unpublished entries for reclamation, drops the values of
    /// entries aged enough to reclaim, and then tells the listener what
    /// left, so a panicking listener leaves nothing unqueued. Requires the
    /// pin taken before the entries were unpublished, which keeps them until
    /// the listener is done. Returns whether enough retirements are pending
    /// to try advancing.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    fn retire_removed(&self, removed: &Removed<(K, V)>, pin: &EpochPin<'_>) -> bool {
        if removed.len == 0 {
            return false;
        }
        let mut reclaimed = Reclaimed::new();
        // Most writes remove one entry. Retiring it through a one-slot
        // iterator lets the compiler fold away `retire`'s loops, which the
        // chain over every removed entry kept: single-thread upserts of
        // `u64`s ran 7 to 13% faster.
        let advance = if removed.len == 1 {
            let slot = removed.first[0].map(|(handle, _)| handle.slot);
            self.retired
                .retire(pin, slot.into_iter(), &mut reclaimed, &self.slots)
        } else {
            #[cfg(test)]
            LOOPED_RETIREMENTS.with(|count| count.set(count.get() + 1));
            self.retired.retire_many(
                pin,
                removed.iter().map(|(handle, _)| handle.slot),
                &mut reclaimed,
                &self.slots,
            )
        };
        reclaimed.finish(&self.slots);
        removed.notify(&self.listener, pin);
        advance
    }

    /// Removes `key`, returning whether it held a live value. An expired
    /// entry is removed too, but reported absent.
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove_as(key, RemovalCause::Explicit)
    }

    /// Removes `key` as [`Self::remove`] does, telling the listener `cause`
    /// for a live value. One body out of line, as `remove`'s was, which
    /// `remove` and writes too heavy to cache share.
    #[allow(
        unsafe_code,
        reason = "dereferences handles unpublished under the shard lock"
    )]
    fn remove_as<Q>(&self, key: &Q, cause: RemovalCause) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(key);
        let shard = self.shard(hash);
        let pin = self.epochs.pin();
        let mut removed = Removed::new();
        let mut live = false;
        {
            let mut writer = shard.lock();
            if writer.loads() != 0 {
                self.supersede_loads(hash, key);
            }
            let found = shard.find(hash, |handle| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, handle) }.value().0.borrow() == key
            });
            if let Some((index, _)) = found {
                let handle = shard.clear(index);
                // SAFETY: the clear unpublished `handle` while `pin` is held.
                let removed_entry = unsafe { entry(&pin, handle) };
                writer.left(removed_entry.weight());
                live = !self.expired(removed_entry, &mut None);
                let cause = if live {
                    cause
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

    /// Removes `key` if `remove` returns true for its live value, deciding
    /// and removing under the shard lock, so no write to the key lands in
    /// between. Returns whether it removed a value. An expired entry is
    /// removed without being offered, as [`Self::remove`] removes it.
    ///
    /// `remove` runs under the shard lock: it must not write to the cache.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published, then unpublished, under the shard lock"
    )]
    pub fn remove_if<Q>(&self, key: &Q, remove: impl FnOnce(&V) -> bool) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(key);
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
            if let Some((index, handle)) = found {
                // SAFETY: as above; the lock keeps it published until the
                // clear below, which `pin` outlives.
                let current = unsafe { entry(&pin, handle) };
                let expired = self.expired(current, &mut None);
                if expired || remove(&current.value().1) {
                    let handle = shard.clear(index);
                    writer.left(current.weight());
                    let cause = if expired {
                        writer.count_expiration();
                        RemovalCause::Expired
                    } else {
                        live = true;
                        RemovalCause::Explicit
                    };
                    removed.push(handle, cause);
                }
            }
        }
        self.after_write(&removed, pin);
        live
    }

    /// Replaces the value of `key` if it holds a live one, as
    /// [`Self::insert`] would, and returns the pair otherwise, leaving the
    /// cache as it was. The new entry expires after the builder's time to
    /// live or time to idle, if either.
    ///
    /// # Errors
    ///
    /// Returns the pair if `key` holds no live value, or if the value is
    /// too heavy to cache; the latter removes the old value, as
    /// [`Self::insert`] does.
    #[allow(
        unsafe_code,
        reason = "fills a slot it owns, and swaps handles, under the shard lock"
    )]
    pub fn replace(&self, key: K, value: V) -> Result<(), (K, V)> {
        let hash = self.hash(&key);
        let shard = self.shard(hash);
        let weight = self.weighter.weight(&key, &value);
        let Some(compact_weight) = u32::try_from(weight)
            .ok()
            .filter(|weight| *weight <= MAX_WEIGHT && u64::from(*weight) <= shard.limits.weight())
        else {
            self.remove_as(&key, RemovalCause::Replaced);
            return Err((key, value));
        };
        let expires_at = self
            .lifetime()
            .map_or(NEVER_EXPIRES, |ttl| self.deadline(ttl));
        let pin = self.epochs.pin();
        let mut removed = Removed::new();
        let mut now = None;
        // Taken before locking, as inserts take theirs; filled only once a
        // live value is found, and otherwise given back unwritten.
        let (slot, raw) = self.slots.take();
        {
            let mut writer = shard.lock();
            let found = shard.find(hash, |current| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, current) }.value().0 == key
            });
            let Some((index, _)) = found.filter(|(_, current)| {
                // SAFETY: as above.
                !self.expired(unsafe { entry(&pin, *current) }, &mut now)
            }) else {
                drop(writer);
                self.slots.give(&[slot]);
                return Err((key, value));
            };
            // SAFETY: `take` handed this vacant slot to this caller alone,
            // and nothing reaches it before the exchange publishes it.
            unsafe { fill(slot, (key, value), compact_weight, expires_at) };
            let handle = Handle { raw, slot };
            let previous = shard.handle(shard.bucket(index).exchange(index % SLOTS, raw));
            // SAFETY: the exchange unpublished `previous` while `pin` is held.
            let previous_entry = unsafe { entry(&pin, previous) };
            writer.set_weight(writer.weight() - previous_entry.weight() + weight);
            removed.push(previous, RemovalCause::Replaced);
            while writer.weight() > shard.limits.weight() {
                let Some((victim, cause)) =
                    self.evict(shard, &mut writer, &pin, Some(handle), &mut now)
                else {
                    break;
                };
                removed.push(victim, cause);
            }
        }
        self.after_write(&removed, pin);
        Ok(())
    }

    /// Removes every entry. Inserts racing the sweep may survive it.
    pub fn clear(&self) {
        for index in 0..self.shards.len() {
            let pin = self.epochs.pin();
            let mut removed = Removed::new();
            self.empty_shard(index, &pin, &mut removed, false);
            self.after_write(&removed, pin);
        }
    }

    /// Removes the entries shard by shard, and yields clones of the live
    /// ones.
    ///
    /// The drain empties each shard under its lock when it reaches it, then
    /// yields that shard's live entries; expired ones leave without being
    /// yielded. So the loop body may write to the cache, entries written to
    /// shards already emptied stay, and dropping the iterator stops the
    /// drain once the shard it reached is emptied. The removal listener
    /// hears [`RemovalCause::Explicit`] for each drained entry.
    pub fn drain(&self) -> impl Iterator<Item = (K, V)> + '_
    where
        K: Clone,
        V: Clone,
    {
        Drain {
            cache: self,
            pin: self.epochs.pin(),
            removed: Removed::new(),
            yielded: 0,
            next_shard: 0,
        }
    }

    /// Unpublishes every entry of shard `index` under its lock and resets
    /// its policy state, pushing each entry to `removed` as `Explicit`, or,
    /// with `expiring`, as `Expired` if it has expired. Requires `pin` to
    /// be held until `removed` is retired.
    #[allow(
        unsafe_code,
        reason = "reads entries unpublished under the shard lock while `pin` is held"
    )]
    fn empty_shard(
        &self,
        index: usize,
        pin: &EpochPin<'_>,
        removed: &mut Removed<(K, V)>,
        expiring: bool,
    ) {
        let shard = self.view(index);
        let mut now = None;
        let mut writer = shard.lock();
        if writer.loads() != 0 {
            self.unlist_loads(index, |_| true);
        }
        for bucket in shard.buckets {
            let meta = bucket.meta.load(Ordering::Relaxed);
            if bucket.tags.load(Ordering::Relaxed) == 0 && meta & !META_GENERATIONS == 0 {
                continue;
            }
            // Tags first, as `ShardView::clear` does, then the handles.
            bucket.tags.store(0, Ordering::Release);
            bucket
                .meta
                .store(meta & META_GENERATIONS, Ordering::Release);
            for slot in 0..SLOTS {
                let raw = bucket.exchange(slot, 0);
                if raw != 0 {
                    let handle = shard.handle(raw);
                    // SAFETY: the exchange unpublished `handle` while `pin`
                    // is held.
                    let cause = if expiring && self.expired(unsafe { entry(pin, handle) }, &mut now)
                    {
                        writer.count_expiration();
                        RemovalCause::Expired
                    } else {
                        RemovalCause::Explicit
                    };
                    removed.push(handle, cause);
                }
            }
        }
        writer.set_len(0);
        writer.set_weight(0);
        writer.hand = 0;
        writer.probation.clear();
        writer.main.clear();
        writer.filters.clear();
    }

    /// Bytes the cache's own structures use: the table, the entry slots
    /// handed out so far, the shards' policy state, and the bookkeeping
    /// around them. Memory the keys and values own elsewhere is not
    /// counted. Takes each shard's lock, each retired queue's, and each load
    /// lane's in turn.
    pub fn memory_used(&self) -> ShardedCacheMemory {
        let policy = self
            .shards
            .iter()
            .map(|shard| {
                let writer = shard.lock();
                writer.filters.lines.len() * size_of::<FilterLine>()
                    + writer.main.records.len() * size_of::<u32>()
                    + writer.probation.capacity() * size_of::<Probationary>()
            })
            .sum();
        let flights = self
            .flights
            .iter()
            .map(|lane| lane.0.lock().capacity() * size_of::<Arc<Flight<K, V>>>())
            .sum::<usize>();
        ShardedCacheMemory {
            table: self.table.len() * size_of::<Bucket>(),
            entries: self.slots.bytes_used(),
            policy,
            bookkeeping: size_of_val(&*self.shards)
                + self.epochs.bytes_used()
                + self.retired.bytes_used()
                + size_of_val(&*self.flights)
                + flights
                + self.stats.as_deref().map_or(0, size_of_val),
        }
    }

    /// Iterates over clones of the live entries, shard by shard.
    ///
    /// A shard's entries are those it held at one moment while the
    /// iteration reached it, so an entry written meanwhile may or may not
    /// appear, and none appears twice. Iterating is not reading: entries are
    /// neither marked as read nor counted in the statistics. The iterator
    /// pins the cache, refreshing the pin between shards; see
    /// [`ShardedCacheGuard::iter`] to borrow the entries instead.
    #[allow(
        unsafe_code,
        reason = "reads entries copied from the table under the iterator's pin"
    )]
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_
    where
        K: Clone,
        V: Clone,
    {
        let mut pin = self.epochs.pin();
        let mut handles = Vec::new();
        let mut next_shard = 0;
        let mut now = None;
        std::iter::from_fn(move || {
            loop {
                if let Some(raw) = handles.pop() {
                    // SAFETY: copied from the table, under its shard's lock,
                    // while `pin` was held, and `pin` has not refreshed since.
                    let entry = unsafe { entry(&pin, self.published(raw)) };
                    if !self.expired(entry, &mut now) {
                        let (key, value) = entry.value();
                        return Some((key.clone(), value.clone()));
                    }
                    continue;
                }
                if next_shard == self.shards.len() {
                    return None;
                }
                // The previous shard's entries were cloned already.
                pin.refresh();
                self.copy_shard(next_shard, &mut handles);
                next_shard += 1;
            }
        })
    }

    /// Keeps only the entries for which `keep` returns true, and removes
    /// expired entries without offering them.
    ///
    /// Each shard's entries are copied under its lock and offered after it
    /// is released, so `keep` may use the cache, writes included. An entry
    /// replaced or removed after it was offered is left alone, and entries
    /// written meanwhile may or may not be offered. The removal listener
    /// hears [`RemovalCause::Explicit`] for each rejected entry.
    #[allow(
        unsafe_code,
        reason = "reads entries copied from the table, and unpublishes them, under the call's pin"
    )]
    pub fn retain(&self, mut keep: impl FnMut(&K, &V) -> bool) {
        let mut handles = Vec::new();
        let mut doomed = Vec::new();
        for index in 0..self.shards.len() {
            let pin = self.epochs.pin();
            self.copy_shard(index, &mut handles);
            let mut now = None;
            for raw in handles.drain(..) {
                // SAFETY: copied from the table, under its shard's lock,
                // while `pin` was held.
                let entry = unsafe { entry(&pin, self.published(raw)) };
                let (key, value) = entry.value();
                if self.expired(entry, &mut now) || !keep(key, value) {
                    doomed.push((self.hash(key), raw));
                }
            }
            if doomed.is_empty() {
                continue;
            }
            let shard = self.view(index);
            let mut removed = Removed::new();
            {
                let mut writer = shard.lock();
                for (hash, raw) in doomed.drain(..) {
                    // Still published, though perhaps moved to its other
                    // bucket; `pin` keeps its handle from naming another
                    // entry meanwhile.
                    let Some((slot, _)) = shard.find(hash, |current| current.raw == raw) else {
                        continue;
                    };
                    let handle = shard.clear(slot);
                    // SAFETY: the clear unpublished `handle` while `pin` is
                    // held.
                    let removed_entry = unsafe { entry(&pin, handle) };
                    writer.left(removed_entry.weight());
                    let cause = if self.expired(removed_entry, &mut now) {
                        writer.count_expiration();
                        RemovalCause::Expired
                    } else {
                        RemovalCause::Explicit
                    };
                    removed.push(handle, cause);
                }
            }
            self.after_write(&removed, pin);
        }
    }

    /// Copies the handles shard `index` publishes to `handles`, under its
    /// lock. Copied while a pin is held, each names an entry readable for
    /// that pin's life.
    fn copy_shard(&self, index: usize, handles: &mut Vec<u32>) {
        let shard = self.view(index);
        let _writer = shard.lock();
        handles.extend(
            shard
                .buckets
                .iter()
                .flat_map(|bucket| &bucket.handles)
                .map(|handle| handle.load(Ordering::Relaxed))
                .filter(|raw| *raw != 0),
        );
    }

    /// The handle `raw`, published in this cache's table.
    #[inline]
    fn published(&self, raw: u32) -> Handle<(K, V)> {
        Handle {
            raw,
            slot: self.slots.slot(raw),
        }
    }

    /// Drops the values of removed entries that no reader can still reach.
    ///
    /// A write drops about as many removed values as it removes, from those
    /// its thread removed earlier, so after a burst of writes some removed
    /// values wait for later writes on their threads. Call this to drop them
    /// promptly. Readers holding a [`ShardedCacheGuard`] or a racing
    /// operation's pin can delay it.
    pub fn maintain(&self) {
        // Three advances age every entry removed before this call.
        for _ in 0..3 {
            self.epochs.advance_evicting();
        }
        self.retired
            .reclaim_all(&self.epochs, self.epochs.generation(), &self.slots);
    }

    /// A multiply spreads every hash bit into the top bits that pick the
    /// shard. Under some seeds the fast hasher's hashes of consecutive
    /// integer keys share their top bits, which then took them all to one
    /// shard. The constant differs from `home`'s, so a shard's keys still
    /// spread over its buckets.
    fn shard_index(&self, hash: u64) -> usize {
        let spread = hash.wrapping_mul(0xd6e8_feb8_6659_fd93);
        // `checked_shr` keeps the single-shard shift of 64 in range.
        let shard = spread.checked_shr(self.shard_shift).unwrap_or(0);
        usize::try_from(shard).expect("a shard index fits usize")
    }

    /// The key's shard, found from the hash without loading the shard.
    #[inline]
    fn shard(&self, hash: u64) -> ShardView<'_, (K, V)> {
        self.view(self.shard_index(hash))
    }

    #[inline]
    fn view(&self, index: usize) -> ShardView<'_, (K, V)> {
        let first = index * self.shard_buckets;
        ShardView {
            state: &self.shards[index],
            buckets: &self.table[first..first + self.shard_buckets],
            slots: self.slots.decoder(),
        }
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
        shard: ShardView<'_, (K, V)>,
        writer: &mut LockedShard<'_>,
        pin: &EpochPin<'_>,
        protected: Option<Handle<(K, V)>>,
        now: &mut Option<u64>,
    ) -> Option<(Handle<(K, V)>, RemovalCause)> {
        while writer.probation.len() > writer.filters.probation_target() {
            let Probationary { record, hash, .. } = writer
                .probation
                .pop_front()
                .expect("probation is above its target");
            let Some(slot) = shard.live_record(record) else {
                // Removed, or moved to its other bucket, since enrolling.
                continue;
            };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            if protected.is_some_and(|protected| protected.raw == raw) {
                // The entry being written keeps its only record: it joins
                // the main area, as the main area keeps it below.
                writer.enter_main(shard, record);
                continue;
            }
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, shard.handle(raw)) };
            let expired = self.expired(entry, now);
            if !expired && entry.take_accessed() {
                // Read during probation: it joins the main area.
                writer.enter_main(shard, record);
                continue;
            }
            let removed = shard.clear(slot);
            if !expired {
                writer.filters.remember_probationary(hash);
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
            let entry = unsafe { entry(pin, shard.handle(raw)) };
            let expired = self.expired(entry, now);
            if protected.is_some_and(|protected| protected.raw == raw)
                || (!expired && entry.take_accessed())
            {
                writer.main.push_back(record);
                continue;
            }
            let hash = self.hash(&entry.value().0);
            let removed = shard.clear(slot);
            if !expired {
                writer.filters.remember_main(hash);
            }
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
            if raw == 0 || protected.is_some_and(|protected| protected.raw == raw) {
                continue;
            }
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, shard.handle(raw)) };
            let expired = self.expired(entry, now);
            if !expired && entry.take_accessed() && step < 2 * slots {
                continue;
            }
            let hash = self.hash(&entry.value().0);
            let removed = shard.clear(slot);
            if !expired {
                writer.filters.remember_main(hash);
            }
            return Some((removed, writer.evicted(entry.weight(), expired)));
        }
        None
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
        shard: ShardView<'_, (K, V)>,
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
                let entry = unsafe { entry(pin, shard.handle(raw)) };
                let expired = self.expired(entry, now);
                let keep = match pass {
                    0 => !expired,
                    1 | 2 => entry.take_accessed(),
                    _ => false,
                };
                if keep {
                    continue;
                }
                let cause = writer.evicted(entry.weight(), expired);
                return (shard.clear(slot), cause);
            }
        }
        unreachable!("the last pass evicts its first slot")
    }
}

/// Empties a cache shard by shard; see [`ShardedCache::drain`].
struct Drain<'cache, K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    cache: &'cache ShardedCache<K, V, W, S, L>,
    pin: EpochPin<'cache>,
    /// The entries the shard last reached held, unpublished under `pin`.
    removed: Removed<(K, V)>,
    /// How many of them the drain has passed.
    yielded: usize,
    next_shard: usize,
}

impl<K, V, W, S, L> Drain<'_, K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    /// Retires the shard's entries, telling the listener, and refreshes the
    /// pin: the drain has cloned every one it yields.
    fn finish_shard(&mut self) {
        let removed = std::mem::replace(&mut self.removed, Removed::new());
        let advance = self.cache.retire_removed(&removed, &self.pin);
        self.pin.refresh();
        if advance {
            self.cache.epochs.try_advance();
        }
    }
}

impl<K, V, W, S, L> Iterator for Drain<'_, K, V, W, S, L>
where
    K: Hash + Eq + Clone,
    V: Clone,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    type Item = (K, V);

    #[allow(
        unsafe_code,
        reason = "reads entries the drain unpublished under its pin"
    )]
    fn next(&mut self) -> Option<(K, V)> {
        loop {
            if let Some((handle, cause)) = self.removed.get(self.yielded) {
                self.yielded += 1;
                if cause == RemovalCause::Explicit {
                    // SAFETY: unpublished by `empty_shard` while `pin` was
                    // held, and `pin` has not refreshed since.
                    let (key, value) = unsafe { entry(&self.pin, handle) }.value();
                    return Some((key.clone(), value.clone()));
                }
                continue;
            }
            self.finish_shard();
            if self.next_shard == self.cache.shards.len() {
                return None;
            }
            self.cache
                .empty_shard(self.next_shard, &self.pin, &mut self.removed, true);
            self.next_shard += 1;
            self.yielded = 0;
        }
    }
}

impl<K, V, W, S, L> Drop for Drain<'_, K, V, W, S, L>
where
    K: Hash + Eq,
    W: Weighter<K, V>,
    S: BuildHasher,
    L: RemovalListener<K, V>,
{
    fn drop(&mut self) {
        self.finish_shard();
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
        let value = self.cache.lookup(&self.pin, key, self.cache.hash(key));
        self.cache.count(value.is_some());
        value
    }

    /// Iterates over the live entries, borrowed until this guard refreshes,
    /// shard by shard; see [`ShardedCache::iter`] for what it visits.
    #[allow(
        unsafe_code,
        reason = "reads entries copied from the table under the guard's pin"
    )]
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        let cache = self.cache;
        let pin = &self.pin;
        let mut handles = Vec::new();
        let mut next_shard = 0;
        let mut now = None;
        std::iter::from_fn(move || {
            loop {
                if let Some(raw) = handles.pop() {
                    // SAFETY: copied from the table, under its shard's lock,
                    // while `pin` was held; the borrow of the guard keeps it
                    // from refreshing.
                    let entry = unsafe { entry(pin, cache.published(raw)) };
                    if !cache.expired(entry, &mut now) {
                        let (key, value) = entry.value();
                        return Some((key, value));
                    }
                    continue;
                }
                if next_shard == cache.shards.len() {
                    return None;
                }
                cache.copy_shard(next_shard, &mut handles);
                next_shard += 1;
            }
        })
    }

    /// Lets entries retired since the guard pinned be reclaimed.
    pub fn refresh(&mut self) {
        self.pin.refresh();
    }
}

impl<K, V, W, S, L> std::fmt::Debug for ShardedCacheGuard<'_, K, V, W, S, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedCacheGuard").finish_non_exhaustive()
    }
}

impl<K, V, W, S, L> Drop for ShardedCache<K, V, W, S, L> {
    #[allow(
        unsafe_code,
        reason = "exclusive drop destroys every value still published in the tables"
    )]
    fn drop(&mut self) {
        // Exclusive access: no pin exists, so every published value can be
        // dropped now; the retired queues drop theirs, and the slot store
        // frees the chunks, when their fields drop.
        for bucket in &self.table {
            for handle in &bucket.handles {
                let raw = handle.load(Ordering::Relaxed);
                if raw != 0 {
                    // SAFETY: a published slot holds an initialized value,
                    // each slot is published once, and nothing reaches it
                    // after this exclusive drop.
                    unsafe { drop_value(self.slots.slot(raw)) };
                }
            }
        }
    }
}

impl<T> Shard<T> {
    /// A shard holding `capacity` entries.
    fn new(capacity: usize, weight_limit: u64, probation_bps: u16) -> Self {
        let limits = ShardLimits::new(capacity, weight_limit);
        let probation_target =
            (capacity * usize::from(probation_bps) / 10_000).clamp(1, limits.probation_max());
        let main = MainQueue::with_capacity(limits.main_limit() + 1);
        Self {
            limits,
            writer: ShardLock {
                len: AtomicUsize::new(0),
                weight: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
                writer: Mutex::new(ShardWriter {
                    probation: VecDeque::new(),
                    filters: Filters::new(capacity, probation_target),
                    main,
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
}

impl<T> ShardView<'_, T> {
    /// Starts loading the lines an insert of `hash` that evicts the next
    /// probationary victim touches: the victim's bucket, entry, and ghost
    /// line, and the new key's home bucket and ghost line. Each was
    /// most likely last written on another core, and fetched together their
    /// misses overlap instead of following one another under the lock.
    /// Lines the insert writes are fetched for writing. Inserts call it only
    /// once a new key's lookup has missed in a full shard, so writes that
    /// evict nothing, replacements included, fetch nothing extra.
    #[inline]
    fn prefetch_eviction(&self, writer: &ShardWriter, hash: u64) {
        #[cfg(test)]
        EVICTION_PREFETCHES.with(|count| count.set(count.get() + 1));
        if let Some(next) = writer.probation.front() {
            let slot = usize::try_from(next.record >> 8).expect("a slot fits usize");
            prefetch_write(self.buckets.as_ptr().wrapping_add(slot / SLOTS));
            // The record may be stale; only a region handle decodes without
            // reading anything.
            if next.handle & OVERFLOW_HANDLE == 0 {
                prefetch(self.slots.slot(next.handle).as_ptr());
            }
            prefetch_write(writer.filters.ghost_line(next.hash));
        }
        prefetch_write(self.buckets.as_ptr().wrapping_add(self.home(hash)));
        prefetch(writer.filters.ghost_line(hash));
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

    /// How far past its home a key with `tag` has its alternate: one to
    /// one less than the bucket count, so the two always differ.
    #[inline]
    fn offset(&self, tag: u8) -> usize {
        // Multiples of 2^32 / phi spread the tags evenly over the range.
        let spread = (u64::from(tag) * 0x9e37_79b9) & 0xffff_ffff;
        let scaled = (spread * (self.buckets.len() as u64 - 1)) >> 32;
        1 + usize::try_from(scaled).expect("a bucket offset fits usize")
    }

    #[inline]
    fn alternate(&self, home: usize, tag: u8) -> usize {
        let alternate = home + self.offset(tag);
        if alternate >= self.buckets.len() {
            alternate - self.buckets.len()
        } else {
            alternate
        }
    }

    /// The home of a key with `tag` that lives in its alternate, `bucket`.
    #[inline]
    fn displaced_home(&self, bucket: usize, tag: u8) -> usize {
        let offset = self.offset(tag);
        if bucket >= offset {
            bucket - offset
        } else {
            bucket + self.buckets.len() - offset
        }
    }

    /// The two buckets a key may live in: its home and an alternate, so a
    /// full region never chains into the next and displacement cannot grow.
    /// The alternate lies its tag's offset past the home, so a resident's
    /// other bucket follows from its slot without reading its key.
    /// Lookups reach the alternate only after the home misses.
    #[inline]
    fn candidates(&self, hash: u64) -> (usize, usize) {
        let home = self.home(hash);
        (home, self.alternate(home, tag(hash)))
    }

    /// Finds the slot whose handle `matches` in the key's home bucket, then
    /// in its alternate if any key from that home lives in its alternate.
    /// Lock-free; decodes overflow handles, for writers.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    fn find(
        &self,
        hash: u64,
        matches: impl FnMut(Handle<T>) -> bool,
    ) -> Option<(usize, Handle<T>)> {
        match self.search::<true>(hash, matches) {
            Ok(found) => found,
            Err(Overflowed) => unreachable!("writers decode overflow handles"),
        }
    }

    /// As [`Self::find`], but gives up on reaching an overflow handle with
    /// the key's tag; the caller then searches again with `find`. Readers
    /// use it: decoding an overflow handle within the lookup, though no
    /// such handle exists while the region has room, made the compiler lay
    /// out `get` so that resident hits at 1,000,000 entries ran 23% slower
    /// on Apple Silicon.
    #[inline]
    fn find_in_region(
        &self,
        hash: u64,
        matches: impl FnMut(Handle<T>) -> bool,
    ) -> Result<Option<Found<T>>, Overflowed> {
        self.search::<false>(hash, matches)
    }

    #[allow(
        clippy::inline_always,
        reason = "out of line, its call passed the view and the found entry through memory"
    )]
    #[inline(always)]
    fn search<const DECODE_OVERFLOW: bool>(
        &self,
        hash: u64,
        mut matches: impl FnMut(Handle<T>) -> bool,
    ) -> Result<Option<Found<T>>, Overflowed> {
        let tag = tag(hash);
        let home = self.home(hash);
        let (found, meta) = self.find_in::<DECODE_OVERFLOW>(home, tag, &mut matches)?;
        if found.is_some() || overflow(meta) == 0 {
            return Ok(found);
        }
        #[cfg(test)]
        if let Some(between) = BETWEEN_BUCKETS.take() {
            between();
        }
        Ok(self
            .find_in::<DECODE_OVERFLOW>(self.alternate(home, tag), tag, &mut matches)?
            .0)
    }

    /// Finds the slot whose handle `matches` among `bucket`'s slots with
    /// `tag`, and returns the meta word if it loaded it. The meta word,
    /// with slots 8 and 9's tags and the overflow count, is acquired only
    /// after slots 0 to 7 miss: a hit there waits on one acquiring load
    /// before its handle's, not two.
    #[allow(
        clippy::inline_always,
        reason = "out of line, its call passed the view and the found entry through memory"
    )]
    #[inline(always)]
    fn find_in<const DECODE_OVERFLOW: bool>(
        &self,
        bucket: usize,
        tag: u8,
        matches: &mut impl FnMut(Handle<T>) -> bool,
    ) -> Result<(Option<Found<T>>, u64), Overflowed> {
        let current = &self.buckets[bucket];
        let pattern = BYTE_ONES * u64::from(tag);
        let tags = current.tags.load(Ordering::Acquire);
        let candidates = zero_tag_bytes(tags ^ pattern);
        if let Some(found) = self.find_among::<DECODE_OVERFLOW>(bucket, candidates, 0, matches)? {
            return Ok((Some(found), 0));
        }
        let meta = current.meta.load(Ordering::Acquire);
        let candidates = zero_tag_bytes(meta ^ pattern) & META_TAG_HIGH_BITS;
        Ok((
            self.find_among::<DECODE_OVERFLOW>(bucket, candidates, WORD_SLOTS, matches)?,
            meta,
        ))
    }

    /// Finds the slot whose handle `matches` among `candidates`, tag-byte
    /// flags for slots from `first` on.
    #[allow(
        clippy::inline_always,
        reason = "out of line, its call passed the view and the found entry through memory"
    )]
    #[inline(always)]
    fn find_among<const DECODE_OVERFLOW: bool>(
        &self,
        bucket: usize,
        mut candidates: u64,
        first: usize,
        matches: &mut impl FnMut(Handle<T>) -> bool,
    ) -> Result<Option<Found<T>>, Overflowed> {
        let current = &self.buckets[bucket];
        while candidates != 0 {
            let slot = first + candidates.trailing_zeros() as usize / 8;
            candidates &= candidates - 1;
            // Acquire pairs with the release store of a replacement handle,
            // which does not touch the tag words.
            let raw = current.handles[slot].load(Ordering::Acquire);
            if !DECODE_OVERFLOW && raw & OVERFLOW_HANDLE != 0 {
                return Err(Overflowed);
            }
            if raw != 0 {
                let handle = self.handle(raw);
                if matches(handle) {
                    return Ok(Some((bucket * SLOTS + slot, handle)));
                }
            }
        }
        Ok(None)
    }

    /// The handle `raw`, published in this shard's table.
    #[inline]
    fn handle(&self, raw: u32) -> Handle<T> {
        Handle {
            raw,
            slot: self.slots.slot(raw),
        }
    }

    /// Publishes `handle` in a free slot of the key's home bucket, or else of
    /// its alternate, counting it in the home's overflow first. Returns
    /// `None` when both are full. Requires the shard lock.
    #[inline]
    fn place(&self, hash: u64, handle: Handle<T>) -> Option<usize> {
        let (home, alternate) = self.candidates(hash);
        if let Some(slot) = self.place_in(home, tag(hash), handle.raw, false) {
            return Some(slot);
        }
        self.buckets[alternate].empty_slot()?;
        // Counted before lookups can need it. Ten keys per bucket keep the
        // count far below saturation.
        self.buckets[home].add_overflow(1);
        self.place_in(alternate, tag(hash), handle.raw, true)
    }

    /// A main-queue record of `slot`'s current key.
    #[inline]
    fn record(&self, slot: usize) -> u32 {
        let generation = self.bucket(slot).generation(slot % SLOTS);
        let slot = u32::try_from(slot).expect("shard slots fit 24 bits");
        (slot << 8) | u32::from(generation)
    }

    /// The slot a main-queue record names, if it still holds the key the
    /// record was made for.
    #[inline]
    fn live_record(&self, record: u32) -> Option<usize> {
        let slot = usize::try_from(record >> 8).expect("a slot fits usize");
        self.is_live(record).then_some(slot)
    }

    /// Whether `record`'s slot still holds the key the record was made for,
    /// tested without branching on the answer.
    #[inline]
    fn is_live(&self, record: u32) -> bool {
        let slot = usize::try_from(record >> 8).expect("a slot fits usize");
        if slot >= self.buckets.len() * SLOTS {
            return false;
        }
        let bucket = self.bucket(slot);
        (u32::from(bucket.generation(slot % SLOTS)) == record & 0xff)
            & (bucket.handles[slot % SLOTS].load(Ordering::Relaxed) != 0)
    }

    /// Prefetches the bucket of `record`'s slot.
    #[inline]
    fn prefetch_record(&self, record: u32) {
        let slot = usize::try_from(record >> 8).expect("a slot fits usize");
        prefetch(self.buckets.as_ptr().wrapping_add(slot / SLOTS));
    }

    /// Moves the key in `slot`, its home, to `alternate`, its alternate
    /// bucket, if that has room, and returns its new slot. The key is
    /// counted in its home's overflow and published in the alternate before
    /// its old slot is cleared, so a lookup that misses it at home finds it
    /// there. Requires the shard lock.
    #[inline]
    fn relocate(&self, slot: usize, alternate: usize) -> Option<usize> {
        self.buckets[alternate].empty_slot()?;
        let current = self.bucket(slot);
        let raw = current.handles[slot % SLOTS].load(Ordering::Relaxed);
        current.add_overflow(1);
        let tag = current.tag(slot % SLOTS);
        let moved_to = self.place_in(alternate, tag, raw, true)?;
        let moved = self.clear(slot);
        debug_assert_eq!(moved.raw, raw);
        Some(moved_to)
    }

    /// Frees a slot in the two full buckets `hash` may occupy by moving one
    /// resident key from its home to its alternate, which follows from its
    /// slot: no resident's entry is read. Requires the shard lock.
    ///
    /// Keys never move back home. A lookup searches the home before the
    /// alternate, so a key published at home after the lookup searched
    /// there, and cleared from its alternate before the lookup arrived,
    /// would be missed though resident throughout.
    fn relocate_candidate(self, writer: &mut LockedShard<'_>, hash: u64) -> bool {
        let (home, alternate) = self.candidates(hash);
        for slot in [home, alternate]
            .into_iter()
            .flat_map(|bucket| bucket * SLOTS..(bucket + 1) * SLOTS)
        {
            let bucket = self.bucket(slot);
            if bucket.is_displaced(slot % SLOTS) {
                continue;
            }
            let other = self.alternate(slot / SLOTS, bucket.tag(slot % SLOTS));
            if other == home || other == alternate {
                continue;
            }
            if let Some(moved_to) = self.relocate(slot, other) {
                // Its old record no longer matches; a probationary key
                // gains a main record it does not need, which only gives it
                // a second life after probation.
                writer.enter_main(self, self.record(moved_to));
                return true;
            }
        }
        false
    }

    /// Publishes `handle`, of a key with `tag`, in a free slot of `bucket`,
    /// marking whether that is the key's alternate. Returns the slot, or
    /// `None` when `bucket` is full. Requires the shard lock.
    #[inline]
    fn place_in(&self, bucket: usize, tag: u8, handle: u32, displaced: bool) -> Option<usize> {
        let current = &self.buckets[bucket];
        let slot = current.empty_slot()?;
        // The handle first: a reader that sees the tag acquires it.
        current.handles[slot].store(handle, Ordering::Release);
        let generation = u64::from(current.generation(slot).wrapping_add(1));
        let flag = 1 << (DISPLACED_SHIFT + slot);
        let meta = current.meta.load(Ordering::Relaxed);
        let mut placed = meta & !flag | if displaced { flag } else { 0 };
        if slot < WORD_SLOTS {
            let tags = current.tags.load(Ordering::Relaxed);
            current
                .tags
                .store(tags | u64::from(tag) << (slot * 8), Ordering::Release);
            let generations = current.generations.load(Ordering::Relaxed);
            current.generations.store(
                generations & !(0xff << (slot * 8)) | generation << (slot * 8),
                Ordering::Relaxed,
            );
        } else {
            let shift = (slot - WORD_SLOTS) * 8;
            let counts = META_GENERATION_SHIFT + shift;
            placed = placed & !(0xff << counts) | generation << counts | u64::from(tag) << shift;
        }
        if placed != meta {
            current.meta.store(placed, Ordering::Release);
        }
        Some(bucket * SLOTS + slot)
    }

    /// Unpublishes `slot`, releasing its key's count in its home's overflow
    /// if the key lived in its alternate. Returns the handle it held.
    /// Requires the shard lock.
    #[inline]
    fn clear(&self, slot: usize) -> Handle<T> {
        let bucket = slot / SLOTS;
        let current = &self.buckets[bucket];
        let tag = current.tag(slot % SLOTS);
        let (word, shift) = current.tag_word(slot % SLOTS);
        word.store(
            word.load(Ordering::Relaxed) & !(TAG_MASK << shift),
            Ordering::Release,
        );
        let removed = self.handle(current.exchange(slot % SLOTS, 0));
        if current.is_displaced(slot % SLOTS) {
            self.buckets[self.displaced_home(bucket, tag)].add_overflow(-1);
        }
        removed
    }
}

#[cfg(test)]
thread_local! {
    static WINDOW_EVICTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static RELOCATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MAIN_RECORDS_EXAMINED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Shard locks this thread has taken.
    static SHARD_LOCKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Eviction prefetches this thread has issued.
    static EVICTION_PREFETCHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Retirements this thread queued through the loop over any number of
    /// removed entries.
    static LOOPED_RETIREMENTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Runs once, when a search on this thread has missed in a key's home
    /// and is about to search its alternate.
    static BETWEEN_BUCKETS: std::cell::Cell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::Cell::new(None) };
}

const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
/// High bits of a word's bytes.
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
/// High bits of the meta word's two tag bytes.
const META_TAG_HIGH_BITS: u64 = 0x8080;

/// Flags bytes of `word` that are zero in their high bit. The lowest flag is
/// exact; a borrow can also flag a 0x01 byte above a zero byte, which callers
/// tolerate: a candidate tag is checked against the entry's key, and only the
/// lowest empty slot is used.
#[inline]
const fn zero_tag_bytes(word: u64) -> u64 {
    word.wrapping_sub(BYTE_ONES) & !word & BYTE_HIGH_BITS
}

/// A meta word's count of keys that probed past its bucket.
#[inline]
const fn overflow(meta: u64) -> u64 {
    meta >> OVERFLOW_SHIFT & OVERFLOW_MAX
}

/// A table of `buckets` empty buckets. Zeroed memory is an empty table, so
/// a large one comes straight from the kernel, untouched until written,
/// and is advised for huge pages before its first write.
#[allow(unsafe_code, reason = "an all-zero bucket is empty")]
fn empty_table(buckets: usize) -> Box<[Bucket]> {
    let layout = std::alloc::Layout::array::<Bucket>(buckets).expect("the table fits isize");
    assert_ne!(layout.size(), 0, "a table has buckets");
    // SAFETY: the layout has a nonzero size.
    let table = unsafe { std::alloc::alloc_zeroed(layout) };
    if table.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    advise_huge_pages(table, layout.size());
    // SAFETY: the global allocator gave `table` the layout a boxed slice of
    // `buckets` buckets uses. A bucket is atomics and arrays of them, valid
    // as all-zero bits, and zero tags and handles mark every slot empty.
    unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(table.cast(), buckets)) }
}

/// The tick `elapsed` nanoseconds after a cache was built; it saturates
/// after about 6.8 years.
fn tick_at(elapsed: u128) -> u64 {
    u64::try_from(elapsed / EXPIRY_TICK_NANOS)
        .unwrap_or(u64::MAX)
        .min(MAX_EXPIRY_TICK)
}

/// The first tick at which an entry stored `elapsed` nanoseconds after its
/// cache was built, with `ttl`, reads as expired: rounding the deadline up
/// keeps it readable for all of `ttl`.
fn deadline_at(elapsed: u128, ttl: Duration) -> u64 {
    if ttl.is_zero() {
        return 0;
    }
    u64::try_from((elapsed + ttl.as_nanos()).div_ceil(EXPIRY_TICK_NANOS))
        .unwrap_or(u64::MAX)
        .min(MAX_EXPIRY_TICK)
}

/// A slot tag: eight hash bits other than the shard and home-bucket bits,
/// never zero.
#[inline]
fn tag(hash: u64) -> u8 {
    (hash >> 32).to_le_bytes()[0].max(1)
}

/// A lookup reached an overflow handle with its key's tag.
struct Overflowed;

/// A found key's slot index and handle.
type Found<T> = (usize, Handle<T>);

/// An entry's slot with the nonzero four-byte handle the table publishes
/// for it (see `SlotStore`).
struct Handle<T> {
    raw: u32,
    slot: NonNull<Slot<T>>,
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Handle<T> {}

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

    #[inline]
    fn extend_deadline(self, deadline: u64, now: u64) {
        self.slot.extend_deadline(deadline, now);
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
    let slot = unsafe { handle.slot.as_ref() };
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

    /// The `index`th entry pushed.
    fn get(&self, index: usize) -> Option<(Handle<T>, RemovalCause)> {
        if index < self.first.len() {
            self.first[index]
        } else {
            self.more.get(index - self.first.len()).copied()
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
        // A loop per slice: with a listener that does nothing, the compiler
        // proves each finite and drops it, as it did not one loop over the
        // chain of both.
        let tell = |&(handle, cause): &(Handle<(K, V)>, RemovalCause)| {
            // SAFETY: unpublished while `pin` was held, and `pin` still
            // blocks the reclamation of every entry retired since.
            let (key, value) = unsafe { entry(pin, handle) }.value();
            listener.on_removal(key, value, cause);
        };
        for removed in self.first[..self.len].iter().flatten() {
            tell(removed);
        }
        for removed in &self.more {
            tell(removed);
        }
    }
}

/// A shard's filters of recently evicted keys, and the probation target
/// they size; guarded by the shard lock.
///
/// - The ghost remembers keys evicted from probation unread, in eight
///   generations of a sixteenth of the shard: seven sixteenths to half of
///   the shard. A missed key it remembers enters the main area directly.
/// - The main filter remembers keys evicted from the main area, a sixteenth
///   of the shard at most, and forgets them all once it holds that many.
///
/// The ghost's current generation and the main filter each hold up to the
/// same number of their queue's most recent evictions. A missed key one of
/// them remembers would most likely have hit had that queue been a little
/// longer, so it moves the probation target toward the queue: one entry for
/// probation, two for the main area, between one entry and a quarter of the
/// shard.
///
/// A key's words in every ghost generation fill one cache line, which also
/// tells whether its current generation remembers it. The main filter is
/// read only while it remembers a key, so an insert that evicts only from
/// probation touches no filter line but the ghost's.
struct Filters {
    /// The ghost's lines, then the main filter's.
    lines: Box<[FilterLine]>,
    /// The words of its lines the main filter uses.
    main_words: u32,
    /// Keys remembered in the ghost's current generation, times the number
    /// of generations, plus the current generation's index. Counts fit 32
    /// bits, as shards hold fewer than 2^24 slots, and the filters then
    /// share one cache line with the main queue.
    ghost_remembered: u32,
    /// Keys remembered in the main filter.
    main_remembered: u32,
    probation_target: u32,
}

/// One cache line of filter words: a key's ghost word in every generation,
/// or eight words of the main filter.
#[repr(C, align(64))]
struct FilterLine([u64; GHOST_GENERATIONS]);

/// Each filter word holds about this many keys.
const KEYS_PER_FILTER_WORD: usize = 64 / GHOST_BITS_PER_KEY;

impl Filters {
    fn new(capacity: usize, probation_target: usize) -> Self {
        let generation = capacity.div_ceil(GHOST_GENERATION_DIVISOR);
        let ghost_lines = generation.max(8).div_ceil(KEYS_PER_FILTER_WORD);
        let main_words = generation.div_ceil(KEYS_PER_FILTER_WORD).max(1);
        let main_lines = main_words.div_ceil(GHOST_GENERATIONS);
        Self {
            lines: (0..ghost_lines + main_lines)
                .map(|_| FilterLine([0; GHOST_GENERATIONS]))
                .collect(),
            main_words: u32::try_from(main_words).expect("a shard's main filter fits u32 words"),
            ghost_remembered: 0,
            main_remembered: 0,
            probation_target: u32::try_from(probation_target)
                .expect("a shard's probation target fits u32"),
        }
    }

    /// Keys probation holds before it evicts.
    #[inline]
    fn probation_target(&self) -> usize {
        self.probation_target as usize
    }

    /// Where the main filter's lines begin: the ghost's line count, which
    /// the lines' length holds, so no field must.
    #[inline]
    fn main_start(&self) -> usize {
        self.lines.len() - (self.main_words as usize).div_ceil(GHOST_GENERATIONS)
    }

    /// The ghost's current generation.
    #[inline]
    fn current(&self) -> usize {
        self.ghost_remembered as usize % GHOST_GENERATIONS
    }

    /// Past this many records the oldest is dropped, leaving its key to the
    /// main sweep, so records never copy more than a share of the keys.
    #[inline]
    fn probation_record_limit(&self) -> usize {
        let target = self.probation_target();
        target + target / 4 + 4
    }

    /// The bits that place `hash` in every filter.
    #[inline]
    fn mixed(hash: u64) -> u64 {
        (hash & 0xffff_ffff).wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }

    #[inline]
    fn mask(mixed: u64) -> u64 {
        (0..GHOST_KEY_BITS).fold(0_u64, |mask, lane| mask | 1 << ((mixed >> (lane * 6)) & 63))
    }

    /// `n` scaled by the fraction `mixed`'s high half picks.
    #[inline]
    fn pick(mixed: u64, n: usize) -> usize {
        usize::try_from(((mixed >> 32) * n as u64) >> 32).expect("a filter index fits usize")
    }

    /// The line of `hash`'s ghost words.
    #[inline]
    fn ghost_line(&self, hash: u64) -> *const FilterLine {
        let line = Self::pick(Self::mixed(hash), self.main_start());
        std::ptr::from_ref(&self.lines[line])
    }

    /// The main filter's word for `mixed`: its line and its index there.
    #[inline]
    fn main_word(&self, mixed: u64) -> (usize, usize) {
        let word = Self::pick(mixed, self.main_words as usize);
        (
            self.main_start() + word / GHOST_GENERATIONS,
            word % GHOST_GENERATIONS,
        )
    }

    /// Whether the ghost remembers `hash`, a missed key the shard now
    /// inserts. Moves the probation target if the ghost's current
    /// generation or the main filter remembers it.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    fn admit(&mut self, hash: u64, max: usize) -> bool {
        let mixed = Self::mixed(hash);
        let mask = Self::mask(mixed);
        let words = &self.lines[Self::pick(mixed, self.main_start())].0;
        let remembered = words.iter().any(|word| word & mask == mask);
        let target = self.probation_target();
        let target = if words[self.current()] & mask == mask {
            (target + PROBATION_GROWTH).min(max)
        } else if self.main_remembered != 0 && {
            let (line, word) = self.main_word(mixed);
            self.lines[line].0[word] & mask == mask
        } {
            target.saturating_sub(PROBATION_SHRINKAGE).max(1)
        } else {
            return remembered;
        };
        self.probation_target = u32::try_from(target).expect("a probation target fits u32");
        remembered
    }

    /// Remembers a key evicted from probation unread, in the ghost.
    #[inline]
    fn remember_probationary(&mut self, hash: u64) {
        let mixed = Self::mixed(hash);
        let main_start = self.main_start();
        let line = Self::pick(mixed, main_start);
        let current = self.current();
        self.lines[line].0[current] |= Self::mask(mixed);
        self.ghost_remembered += GENERATION_COUNT;
        // The generation's last 64th clears the next one's word, a bounded
        // number of lines per key, so no write clears the whole ghost at
        // once. The oldest generation fades over that 64th instead of
        // vanishing at its end.
        let remembered = self.ghost_remembered as usize / GHOST_GENERATIONS;
        let generation = main_start * KEYS_PER_FILTER_WORD;
        let next = (current + 1) % GHOST_GENERATIONS;
        let left = generation - remembered;
        if left * GHOST_CLEARED_LINES < main_start {
            let first = main_start.saturating_sub((left + 1) * GHOST_CLEARED_LINES);
            for line in &mut self.lines[first..main_start - left * GHOST_CLEARED_LINES] {
                line.0[next] = 0;
            }
        }
        if remembered == generation {
            self.ghost_remembered = u32::try_from(next).expect("a generation index fits u32");
        }
    }

    /// Remembers a key evicted from the main area, in the main filter.
    #[inline]
    fn remember_main(&mut self, hash: u64) {
        let mixed = Self::mixed(hash);
        let (line, word) = self.main_word(mixed);
        self.lines[line].0[word] |= Self::mask(mixed);
        self.main_remembered += 1;
        if self.main_remembered as usize == self.main_words as usize * KEYS_PER_FILTER_WORD {
            let main_start = self.main_start();
            for line in &mut self.lines[main_start..] {
                line.0 = [0; GHOST_GENERATIONS];
            }
            self.main_remembered = 0;
        }
    }

    /// Forgets every key; the probation target stays.
    fn clear(&mut self) {
        for line in &mut self.lines {
            line.0 = [0; GHOST_GENERATIONS];
        }
        self.ghost_remembered = 0;
        self.main_remembered = 0;
    }
}

/// Main-area keys in the order they entered it, as `slot << 8 |
/// generation`, in a ring sized for the queue's limit. Stale records
/// (removed, replaced by another key, or moved) are skipped when popped.
///
/// Compaction walks the queue from its front a chunk at a time and drops
/// stale records. The live records it passes close up behind the front, so
/// a gap of dropped records' slots trails them. The gap closes when pops
/// reach it, or frees the ring's slots once the walk reaches the back.
struct MainQueue {
    records: Box<[u32]>,
    head: u32,
    len: u32,
    /// Records compaction has passed, which precede the gap.
    cursor: u32,
    /// Slots between the first `cursor` records and the rest.
    gap: u32,
}

impl MainQueue {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            records: vec![0; capacity.max(1)].into_boxed_slice(),
            head: 0,
            len: 0,
            cursor: 0,
            gap: 0,
        }
    }

    /// Widens the ring to `capacity` slots if it is narrower, keeping the
    /// records, and the gap between them, in place past the head.
    fn grow(&mut self, capacity: usize) {
        if capacity <= self.records.len() {
            return;
        }
        let mut records = vec![0; capacity].into_boxed_slice();
        for (offset, record) in records[..self.span()].iter_mut().enumerate() {
            *record = self.records[self.index(offset)];
        }
        self.records = records;
        self.head = 0;
    }

    #[inline]
    fn len(&self) -> usize {
        self.len as usize
    }

    /// Slots the records and the gap between them take.
    #[inline]
    fn span(&self) -> usize {
        (self.len + self.gap) as usize
    }

    /// The ring slot `offset` slots past the head.
    #[inline]
    fn index(&self, offset: usize) -> usize {
        let index = self.head as usize + offset;
        if index >= self.records.len() {
            index - self.records.len()
        } else {
            index
        }
    }

    /// Appends `record`. The queue never outgrows its ring: `enter_main`
    /// compacts it early enough, and evictions pop before they push. A
    /// full ring would drop its oldest record.
    #[inline]
    fn push_back(&mut self, record: u32) {
        debug_assert!(self.span() < self.records.len(), "the main queue is full");
        if self.span() == self.records.len() {
            self.pop_front();
        }
        let index = self.index(self.span());
        self.records[index] = record;
        self.len += 1;
    }

    #[inline]
    fn pop_front(&mut self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        let record = self.records[self.head as usize];
        self.head = u32::try_from(self.index(1)).expect("a main queue index fits u32");
        self.len -= 1;
        self.cursor = self.cursor.saturating_sub(1);
        self.settle();
        Some(record)
    }

    /// Examines up to `budget` records where compaction left off, dropping
    /// those `keep` rejects, and stops early once it passes the back.
    fn compact(
        &mut self,
        budget: usize,
        mut fetch: impl FnMut(u32),
        mut keep: impl FnMut(u32) -> bool,
    ) {
        // Fetching what each record names first overlaps the misses that
        // checking them one by one would wait for in turn.
        let next = (self.cursor + self.gap) as usize;
        for offset in 0..budget.min((self.len - self.cursor) as usize) {
            fetch(self.records[self.index(next + offset)]);
        }
        for _ in 0..budget {
            if self.len == 0 {
                return;
            }
            let record = self.records[self.index((self.cursor + self.gap) as usize)];
            // Without branches on the outcome, a stale record mispredicts
            // nothing: a dropped record's copy is overwritten by the next.
            let kept = keep(record);
            let index = self.index(self.cursor as usize);
            self.records[index] = record;
            self.cursor += u32::from(kept);
            self.gap += u32::from(!kept);
            self.len -= u32::from(!kept);
            if self.settle() {
                return;
            }
        }
    }

    /// Closes a gap that reached an end of the queue: at the front the head
    /// skips it, and at the back its slots are free and the next walk starts
    /// over. Returns whether the walk passed the back.
    #[inline]
    fn settle(&mut self) -> bool {
        if self.cursor == 0 {
            self.head =
                u32::try_from(self.index(self.gap as usize)).expect("a main queue index fits u32");
            self.gap = 0;
        }
        if self.cursor == self.len {
            self.cursor = 0;
            self.gap = 0;
            return true;
        }
        false
    }

    fn clear(&mut self) {
        self.head = 0;
        self.len = 0;
        self.cursor = 0;
        self.gap = 0;
    }

    /// The records, front first.
    #[cfg(test)]
    fn records(&self) -> Vec<u32> {
        (0..self.len)
            .map(|offset| {
                let gap = if offset < self.cursor { 0 } else { self.gap };
                self.records[self.index((offset + gap) as usize)]
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{BuildHasherDefault, Hasher};
    use std::sync::Arc;
    use std::task::Wake;
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
        let shard = cache.view(0);
        for key in 0..1_000_000_u64 {
            cache.insert(key, key);
            if key % 5 == 0 {
                let _ = cache.get(&(key / 3));
            }
        }
        let pin = cache.epochs.pin();
        let mut displaced_from = vec![0_u64; shard.buckets.len()];
        for (bucket, current) in shard.buckets.iter().enumerate() {
            for (slot, handle) in current.handles.iter().enumerate() {
                let raw = handle.load(Ordering::Relaxed);
                if raw == 0 {
                    continue;
                }
                // SAFETY: the table publishes this handle while `pin` is held.
                let key = unsafe { entry(&pin, shard.handle(raw)) }.value().0;
                let (home, alternate) = shard.candidates(cache.hash(&key));
                assert!(
                    bucket == home || bucket == alternate,
                    "key {key} lives in bucket {bucket}, outside {home} and {alternate}"
                );
                // Relocation and clears find a key's other bucket from its
                // slot alone.
                assert_eq!(current.is_displaced(slot), bucket != home);
                let tag = current.tag(slot);
                if bucket == home {
                    assert_eq!(shard.alternate(bucket, tag), alternate);
                } else {
                    assert_eq!(shard.displaced_home(bucket, tag), home);
                }
                displaced_from[home] += u64::from(bucket != home);
            }
        }
        for (bucket, displaced) in shard.buckets.iter().zip(displaced_from) {
            let overflow = overflow(bucket.meta.load(Ordering::Relaxed));
            assert_eq!(
                overflow, displaced,
                "a home counts its keys in their alternate"
            );
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
                .any(|probationary| probationary.hash == cache.hash(&key))
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

    /// A hasher whose hash is the `u64` key itself, so tests can choose keys
    /// by their candidate buckets.
    #[derive(Clone, Default)]
    struct Identity;

    struct IdentityHasher(u64);

    impl Hasher for IdentityHasher {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, _: &[u8]) {
            unreachable!("only `u64` keys");
        }
        fn write_u64(&mut self, key: u64) {
            self.0 = key;
        }
    }

    impl BuildHasher for Identity {
        type Hasher = IdentityHasher;
        fn build_hasher(&self) -> IdentityHasher {
            IdentityHasher(0)
        }
    }

    #[test]
    fn keys_whose_hashes_share_their_top_bits_spread_over_the_shards() {
        // For about one process seed in a hundred, the fast hasher's hashes
        // of consecutive integer keys differ only below their top bits, as
        // identity hashes do. Shards picked by the top bits then took every
        // key into one or two shards, which kept a quarter or half of the
        // capacity.
        let cache = ShardedCacheBuilder::new(1_000)
            .shards(4)
            .hasher(Identity)
            .build::<u64, u64>();
        for key in 0..1_000_u64 {
            cache.insert(key, key);
        }
        assert!(cache.len() >= 900, "{} of 1,000 keys kept", cache.len());
        let hits = (0..1_000_u64)
            .filter(|key| cache.get(key) == Some(*key))
            .count();
        assert_eq!(hits, cache.len());
    }

    #[test]
    fn readers_find_a_key_that_moves_while_they_search_its_buckets() {
        // A key moving back home was published there after a reader missed
        // it at home, and cleared from its alternate before the reader
        // looked there: the reader reported a resident key absent.
        let cache = Arc::new(
            ShardedCacheBuilder::new(64)
                .shards(1)
                .hasher(Identity)
                .build::<u64, u64>(),
        );
        let shard = cache.view(0);
        let (home, alternate, other, spare) = (0, 1, 2, 3);
        assert!(shard.buckets.len() > spare);
        let mut unused = (1..).map(|index: u64| index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut keys = |count: usize, wanted: &dyn Fn((usize, usize)) -> bool| -> Vec<u64> {
            unused
                .by_ref()
                .filter(|&key| wanted(shard.candidates(key)))
                .take(count)
                .collect()
        };
        let at_home = keys(SLOTS, &|(first, second)| {
            first == home && second != alternate
        });
        let moving = keys(1, &|buckets| buckets == (home, alternate))[0];
        let at_alternate = keys(SLOTS - 1, &|buckets| buckets == (alternate, spare));
        let in_other = keys(SLOTS, &|(first, _)| first == other);
        let arriving = keys(1, &|buckets| buckets == (alternate, other))[0];
        for &key in &at_home {
            cache.insert(key, key);
        }
        // Its home is full, so it takes its alternate's first slot.
        cache.insert(moving, moving);
        for &key in at_alternate.iter().chain(&in_other) {
            cache.insert(key, key);
        }
        assert!(cache.remove(&at_home[0]));
        assert!(shard.buckets[alternate].is_displaced(0));
        assert!(shard.buckets[home].empty_slot().is_some());
        assert!(shard.buckets[alternate].empty_slot().is_none());
        assert!(shard.buckets[other].empty_slot().is_none());

        // Both of the arriving key's buckets are full, so its insert moves
        // a resident while the reader is between the moving key's buckets.
        let writer = Arc::clone(&cache);
        BETWEEN_BUCKETS.set(Some(Box::new(move || {
            writer.insert(arriving, arriving);
        })));
        assert_eq!(cache.get(&moving), Some(moving));
        assert!(
            BETWEEN_BUCKETS.take().is_none(),
            "the read searched both buckets"
        );
        for key in at_home[1..]
            .iter()
            .chain([&moving, &arriving])
            .chain(&at_alternate)
            .chain(&in_other)
        {
            assert_eq!(cache.get(key), Some(*key));
        }
    }

    /// A hasher that notes when its thread hashes under an epoch pin.
    #[derive(Clone, Default)]
    struct PinWatching;

    struct PinWatchingHasher(DefaultHasher);

    thread_local! {
        static HASHED_UNDER_PIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    impl Hasher for PinWatchingHasher {
        fn finish(&self) -> u64 {
            self.0.finish()
        }
        fn write(&mut self, bytes: &[u8]) {
            if crate::sharded_arena::pins_held() != 0 {
                HASHED_UNDER_PIN.set(true);
            }
            self.0.write(bytes);
        }
    }

    impl BuildHasher for PinWatching {
        type Hasher = PinWatchingHasher;
        fn build_hasher(&self) -> PinWatchingHasher {
            PinWatchingHasher(DefaultHasher::new())
        }
    }

    /// A hasher whose one-shot `hash_one` must not be called.
    #[derive(Clone, Default)]
    struct NoOneShot;

    impl BuildHasher for NoOneShot {
        type Hasher = DefaultHasher;
        fn build_hasher(&self) -> DefaultHasher {
            DefaultHasher::new()
        }
        fn hash_one<T: Hash>(&self, _: T) -> u64 {
            panic!("the cache hashed through `hash_one`");
        }
    }

    #[test]
    fn operations_hash_keys_through_build_hasher() {
        // `hash_one` is a generic default method that the compiler may keep
        // out of line. Built with one codegen unit, it was, and resident
        // hits at 1,000,000 entries ran 23% slower on Apple Silicon than
        // with the hasher's steps written out.
        let cache = ShardedCacheBuilder::new(100).hasher(NoOneShot).build();
        assert!(cache.insert(1_u64, 1_u64));
        assert!(cache.insert_with_ttl(2, 2, Duration::from_secs(60)));
        assert_eq!(cache.get(&1), Some(1));
        assert_eq!(cache.peek(&2), Some(2));
        assert!(cache.contains_key(&1));
        assert_eq!(cache.pin().get(&1), Some(&1));
        assert_eq!(cache.get_or_insert_with(&3, || 3), 3);
        assert_eq!(
            cache.get_or_try_insert_with(&4, || Ok::<_, Infallible>(4)),
            Ok(4)
        );
        assert!(cache.remove(&1));
        for key in 10..1_000 {
            cache.insert(key, key);
        }
    }

    /// A deterministic hasher that counts the keys its thread hashes.
    #[derive(Clone, Default)]
    struct Counting;

    thread_local! {
        static KEYS_HASHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    impl BuildHasher for Counting {
        type Hasher = DefaultHasher;
        fn build_hasher(&self) -> DefaultHasher {
            KEYS_HASHED.set(KEYS_HASHED.get() + 1);
            DefaultHasher::new()
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "a thousand inserts counting hash calls")]
    fn relocation_moves_residents_without_rehashing_their_keys() {
        // Both candidate buckets were full on 6% of inserts into a full
        // shard. Rehashing a resident to find its other bucket read its
        // entry, a cache miss apiece, before the move could start.
        let cache = ShardedCacheBuilder::new(1_000)
            .shards(1)
            .hasher(Counting)
            .build();
        let relocations = RELOCATIONS.with(std::cell::Cell::get);
        KEYS_HASHED.set(0);
        for key in 0..1_000_u64 {
            cache.insert(key, key);
        }
        assert!(
            RELOCATIONS.with(std::cell::Cell::get) > relocations,
            "filling the shard relocates keys"
        );
        assert_eq!(KEYS_HASHED.get(), 1_000, "each insert hashes only its key");
    }

    #[test]
    fn reads_hash_their_keys_before_pinning() {
        // A pin's atomics hold back the loads after them, the key's
        // included: a read that hashed under its pin waited for the
        // previous read to finish instead of overlapping it.
        let cache = ShardedCacheBuilder::new(100).hasher(PinWatching).build();
        cache.insert(1_u64, 1_u64);
        HASHED_UNDER_PIN.set(false);
        assert_eq!(cache.get(&1), Some(1));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.peek(&1), Some(1));
        assert!(cache.contains_key(&1));
        assert_eq!(cache.get_or_insert_with(&1, || 0), 1);
        assert!(!HASHED_UNDER_PIN.get());
    }

    #[test]
    fn colliding_keys_fill_their_two_buckets_and_unlink_cleanly() {
        // Every key shares one home bucket, so keys beyond its ten slots
        // live in the alternate and count in the home's overflow.
        let cache = ShardedCacheBuilder::new(40).hasher(Colliding).build();
        let pair = 2 * SLOTS as u64;
        for key in 0..pair {
            cache.insert(key, key);
        }
        for key in 0..pair {
            assert_eq!(cache.get(&key), Some(key));
        }
        let shard = cache.view(0);
        let (home, _) = shard.candidates(0);
        assert_eq!(
            overflow(shard.buckets[home].meta.load(Ordering::Relaxed)),
            SLOTS as u64
        );
        for key in (0..pair).step_by(2) {
            assert!(cache.remove(&key));
        }
        for key in 0..pair {
            assert_eq!(cache.get(&key), (key % 2 == 1).then_some(key));
        }
        // Freed slots are reused; one more colliding key evicts one.
        let last = 100 + SLOTS as u64;
        for key in 100..=last {
            cache.insert(key, key);
        }
        assert_eq!(cache.len(), 2 * SLOTS);
        assert_eq!(cache.get(&last), Some(last));
        let resident: Vec<u64> = (0..pair)
            .chain(100..=last)
            .filter(|key| cache.get(key).is_some())
            .collect();
        assert_eq!(resident.len(), 2 * SLOTS);
        for key in resident {
            assert!(cache.remove(&key));
        }
        assert!(cache.is_empty());
        assert!(
            shard.buckets.iter().all(|bucket| {
                let meta = bucket.meta.load(Ordering::Relaxed);
                bucket.tags.load(Ordering::Relaxed) == 0
                    && zero_tag_bytes(meta) & META_TAG_HIGH_BITS == META_TAG_HIGH_BITS
                    && overflow(meta) == 0
            }),
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
            .live_entry(&pin, &key, cache.hash(&key))
            .is_some_and(|entry| entry.slot.accessed())
    }

    /// Waits until exactly `waiters` callers besides the leader wait on the
    /// load of `key`.
    fn wait_for_waiters(cache: &ShardedCache<u64, u64>, key: u64, waiters: usize) {
        let lane = &cache.flights[cache.shard_index(cache.hash(&key))];
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

    /// Runs `future` on this thread, parking it while the future waits.
    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            thread::park();
        }
    }

    /// A waker that records being woken.
    #[derive(Default)]
    struct WakeFlag(std::sync::atomic::AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    impl WakeFlag {
        fn take(&self) -> bool {
            self.0.swap(false, Ordering::Relaxed)
        }
    }

    /// Pending once, then ready.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn async_loads_are_send_and_hit_without_loading() {
        fn assert_send<T: Send>(_: &T) {}
        let cache = ShardedCache::<u64, String>::new(100);
        let load = cache.get_or_insert_async(&1, async { "one".to_owned() });
        assert_send(&load);
        let try_load =
            cache.get_or_try_insert_async(&2, async { Ok::<_, std::io::Error>("two".to_owned()) });
        assert_send(&try_load);
        assert_eq!(block_on(load), "one");
        assert_eq!(block_on(try_load).expect("the load succeeds"), "two");
        let hit = cache.get_or_insert_async(&1, async { unreachable!("a hit never loads") });
        assert_eq!(block_on(hit), "one");
    }

    #[test]
    fn a_dropped_async_load_wakes_and_hands_the_key_to_a_waiter() {
        // One thread: the leader is polled into its load and dropped there,
        // as a cancelled task would be.
        let cache = ShardedCache::<u64, u64>::new(100);
        let flag = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut context = Context::from_waker(&waker);
        let mut leader = Box::pin(cache.get_or_insert_async(&3, std::future::pending()));
        assert!(leader.as_mut().poll(&mut context).is_pending());
        let mut waiter = Box::pin(cache.get_or_insert_async(&3, async { 30 }));
        assert!(waiter.as_mut().poll(&mut context).is_pending(), "it waits");
        let mut dropped = Box::pin(cache.get_or_insert_async(&3, async { 31 }));
        assert!(dropped.as_mut().poll(&mut context).is_pending());
        drop(dropped);
        assert!(!flag.take());
        drop(leader);
        assert!(flag.take(), "abandoning the load wakes its waiter");
        assert_eq!(waiter.as_mut().poll(&mut context), Poll::Ready(30));
        assert_eq!(cache.get(&3), Some(30));
        assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
    }

    #[test]
    fn a_load_takes_its_shard_lock_twice() {
        // Once to list its flight, and once to insert its value, which
        // unlists the flight; the landing then has nothing left to do.
        let cache = ShardedCache::<u64, u64>::new(100);
        let locks = || SHARD_LOCKS.with(std::cell::Cell::get);
        let before = locks();
        assert_eq!(cache.get_or_insert_with(&1, || 10), 10);
        assert_eq!(locks() - before, 2);
        let before = locks();
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&2, None) else {
            panic!("a missing key hands its caller the guard");
        };
        assert!(guard.insert(20));
        assert_eq!(locks() - before, 2);
        // A failed load has no insert: its landing unlists the flight.
        let before = locks();
        assert_eq!(cache.get_or_try_insert_with(&3, || Err(())), Err(()));
        assert_eq!(locks() - before, 2);
        assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
    }

    #[test]
    fn writes_that_evict_nothing_prefetch_no_victim() {
        let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(64).shards(1).build();
        let mut next = 0;
        while cache.len() < 64 {
            cache.insert(next, next);
            next += 1;
        }
        let resident: Vec<u64> = (0..next).filter(|key| cache.peek(key).is_some()).collect();
        let prefetches = || EVICTION_PREFETCHES.with(std::cell::Cell::get);
        let before = prefetches();
        for &key in &resident {
            assert!(cache.insert(key, key + 1), "a replacement");
        }
        assert_eq!(prefetches(), before, "replacing a key evicts nothing");
        assert!(cache.insert(next, next));
        assert_eq!(prefetches(), before + 1, "a new key evicts");
        assert_eq!(cache.len(), 64);
    }

    #[test]
    fn writes_that_remove_one_entry_retire_it_without_the_loop() {
        let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(64).shards(1).build();
        let looped = || LOOPED_RETIREMENTS.with(std::cell::Cell::get);
        for key in 0..32 {
            cache.insert(key, key);
        }
        let before = looped();
        for key in 0..32 {
            assert!(cache.insert(key, key + 1), "a replacement");
        }
        assert!(cache.remove(&0));
        assert!(cache.remove_if(&1, |_| true));
        assert!(cache.replace(2, 3).is_ok());
        assert_eq!(looped(), before, "one removed entry went through the loop");
        cache.clear();
        assert_eq!(looped(), before + 1, "a clear's many entries take the loop");
    }

    #[test]
    fn writes_reach_the_threads_slot_stack_without_searching() {
        // A thread of its own, whose first slot stack is this cache's.
        thread::spawn(|| {
            let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(64).shards(1).build();
            let searches = || STACK_SEARCHES.with(std::cell::Cell::get);
            for key in 0..32 {
                cache.insert(key, key);
            }
            let before = searches();
            for round in 1..=8 {
                for key in 0..32 {
                    assert!(cache.insert(key, key + round), "a replacement");
                }
            }
            // Only refilling an empty stack searches, once per batch of
            // slots; taking a slot and giving one back do not.
            assert!(
                searches() - before <= 256 / 16,
                "{} searches in 256 writes",
                searches() - before
            );
        })
        .join()
        .expect("the writer finishes");
    }

    #[test]
    fn a_single_cache_threads_writes_skip_the_shared_stack_list() {
        thread::spawn(|| {
            let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(64).shards(1).build();
            let borrows = || STACK_BORROWS.with(std::cell::Cell::get);
            let before = borrows();
            for round in 0..=8 {
                for key in 0..32 {
                    cache.insert(key, key + round);
                }
            }
            assert_eq!(
                borrows(),
                before,
                "a write borrowed the list of slot stacks"
            );
        })
        .join()
        .expect("the writer finishes");
    }

    #[test]
    fn async_waiters_dropped_mid_load_free_their_waker_slots() {
        // Waiters that give up and retry while one load runs, as under an
        // executor's timeout, the oldest dropping first.
        let cache = ShardedCache::<u64, u64>::new(100);
        let flag = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut context = Context::from_waker(&waker);
        let mut leader = Box::pin(cache.get_or_insert_async(&5, std::future::pending()));
        assert!(leader.as_mut().poll(&mut context).is_pending());
        let mut waiting = VecDeque::new();
        for _ in 0..1_000 {
            let mut waiter = Box::pin(cache.get_or_insert_async(&5, async { 50 }));
            assert!(waiter.as_mut().poll(&mut context).is_pending());
            waiting.push_back(waiter);
            if waiting.len() > 2 {
                waiting.pop_front();
            }
        }
        let lane = &cache.flights[cache.shard_index(cache.hash(&5))];
        let wakers = lane.0.lock()[0].progress.lock().wakers.len();
        assert!(
            wakers <= 3,
            "{wakers} waker slots for three waiters at most"
        );
        drop(leader);
        for mut waiter in waiting {
            assert!(matches!(
                waiter.as_mut().poll(&mut context),
                Poll::Ready(50)
            ));
        }
    }

    #[test]
    fn a_failed_async_load_returns_its_error_and_hands_the_key_on() {
        let cache = ShardedCache::<u64, u64>::new(100);
        let flag = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut context = Context::from_waker(&waker);
        let mut leader = Box::pin(cache.get_or_try_insert_async(&4, async {
            YieldOnce(false).await;
            Err("the load fails")
        }));
        assert!(leader.as_mut().poll(&mut context).is_pending());
        let mut waiter = Box::pin(cache.get_or_try_insert_async(&4, async { Ok::<_, &str>(40) }));
        assert!(waiter.as_mut().poll(&mut context).is_pending());
        flag.take();
        assert_eq!(
            leader.as_mut().poll(&mut context),
            Poll::Ready(Err("the load fails"))
        );
        assert!(flag.take(), "the failure wakes the waiter");
        assert_eq!(waiter.as_mut().poll(&mut context), Poll::Ready(Ok(40)));
        assert_eq!(cache.get(&4), Some(40));
    }

    #[test]
    fn a_placeholder_guard_inserts_and_others_time_out_meanwhile() {
        let cache = ShardedCache::<u64, u64>::new(100);
        cache.insert(1, 10);
        assert!(matches!(
            cache.get_value_or_guard(&1, None),
            GuardResult::Value(10)
        ));
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&2, None) else {
            panic!("a missing key hands its caller the guard");
        };
        assert_eq!(*guard.key(), 2);
        // A zero timeout never waits: the guard is held on this thread.
        assert!(matches!(
            cache.get_value_or_guard(&2, Some(Duration::ZERO)),
            GuardResult::Timeout
        ));
        assert!(guard.insert(20));
        assert_eq!(cache.get(&2), Some(20));
        assert!(matches!(
            cache.get_value_or_guard(&2, Some(Duration::ZERO)),
            GuardResult::Value(20)
        ));
        assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
    }

    #[test]
    fn a_dropped_placeholder_guard_hands_the_key_to_a_waiter() {
        let cache = ShardedCache::<u64, u64>::new(100);
        let flag = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut context = Context::from_waker(&waker);
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&3, None) else {
            panic!("a missing key hands its caller the guard");
        };
        let mut waiter = Box::pin(cache.get_value_or_guard_async(&3));
        assert!(waiter.as_mut().poll(&mut context).is_pending(), "it waits");
        drop(guard);
        assert!(flag.take(), "abandoning the guard wakes the waiter");
        let Poll::Ready(Err(next)) = waiter.as_mut().poll(&mut context) else {
            panic!("the waiter takes the guard");
        };
        assert!(next.insert(30));
        let mut hit = Box::pin(cache.get_value_or_guard_async(&3));
        assert!(matches!(
            hit.as_mut().poll(&mut context),
            Poll::Ready(Ok(30))
        ));
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn callers_wait_for_a_placeholder_guard_or_their_timeout() {
        let cache = Arc::new(ShardedCache::<u64, u64>::new(100));
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&4, None) else {
            panic!("a missing key hands its caller the guard");
        };
        let started = Instant::now();
        assert!(matches!(
            cache.get_value_or_guard(&4, Some(Duration::from_millis(30))),
            GuardResult::Timeout
        ));
        assert!(started.elapsed() >= Duration::from_millis(30), "it waited");
        let loader = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || cache.get_or_insert_with(&4, || unreachable!("the guard loads")))
        };
        let waiter = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || match cache.get_value_or_guard(&4, None) {
                GuardResult::Value(value) => value,
                _ => panic!("the guard's value"),
            })
        };
        wait_for_waiters(&cache, 4, 2);
        assert!(guard.insert(40));
        assert_eq!(loader.join().expect("the loader completes"), 40);
        assert_eq!(waiter.join().expect("the waiter completes"), 40);
    }

    #[test]
    fn a_write_while_a_key_loads_keeps_the_load_from_caching_over_it() {
        // A source updated and the key removed while its old value loads,
        // as an invalidation does: the load is stale and must not land.
        let cache = ShardedCache::<u64, u64>::new(100);
        let GuardResult::Guard(stale) = cache.get_value_or_guard(&1, None) else {
            panic!("a missing key hands its caller the guard");
        };
        assert!(!cache.remove(&1), "nothing was cached to remove");
        let GuardResult::Guard(fresh) = cache.get_value_or_guard(&1, Some(Duration::ZERO)) else {
            panic!("a caller after the removal loads again instead of waiting");
        };
        assert!(!stale.insert(10), "the load the removal superseded");
        assert_eq!(cache.get(&1), None);
        assert!(fresh.insert(11));
        assert_eq!(cache.get(&1), Some(11));

        // An insert while the key loads wins over the loaded value.
        let GuardResult::Guard(stale) = cache.get_value_or_guard(&2, None) else {
            panic!("a missing key hands its caller the guard");
        };
        assert!(cache.insert(2, 21));
        assert!(!stale.insert(20));
        assert_eq!(cache.get(&2), Some(21));

        // Loaders get their value, but do not cache it over the write.
        assert_eq!(
            cache.get_or_insert_with(&3, || {
                cache.remove(&3);
                30
            }),
            30
        );
        assert_eq!(cache.get(&3), None);
        assert_eq!(
            block_on(cache.get_or_insert_async(&4, async {
                cache.insert_many([(4, 41), (5, 51)]);
                40
            })),
            40
        );
        assert_eq!(cache.get(&4), Some(41));

        // Clearing or draining the cache supersedes every load.
        let GuardResult::Guard(stale) = cache.get_value_or_guard(&6, None) else {
            panic!("a missing key hands its caller the guard");
        };
        cache.clear();
        assert!(!stale.insert(60));
        let GuardResult::Guard(stale) = cache.get_value_or_guard(&7, None) else {
            panic!("a missing key hands its caller the guard");
        };
        cache.drain().for_each(drop);
        assert!(!stale.insert(70));
        assert!(cache.is_empty());
        assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn loads_racing_invalidations_never_leave_a_stale_value_cached() {
        // A writer updates a source, then invalidates the key by removing
        // it or writing the new version through; loaders read the source on
        // a miss. Once a version is invalidated, the key holds it or
        // nothing: never a version loaded before it.
        for write_through in [false, true] {
            let cache = ShardedCache::<u64, u64>::new(64);
            let source = Mutex::new(0_u64);
            let done = AtomicBool::new(false);
            thread::scope(|scope| {
                scope.spawn(|| {
                    // Stops the loaders even if an assertion fails.
                    struct Done<'flag>(&'flag AtomicBool);
                    impl Drop for Done<'_> {
                        fn drop(&mut self) {
                            self.0.store(true, Ordering::Relaxed);
                        }
                    }
                    let _done = Done(&done);
                    for version in 1..=500 {
                        *source.lock() = version;
                        if write_through {
                            cache.insert(0, version);
                        } else {
                            cache.remove(&0);
                        }
                        for _ in 0..20 {
                            if let Some(cached) = cache.peek(&0) {
                                assert_eq!(cached, version, "write-through {write_through}");
                            }
                            thread::yield_now();
                        }
                    }
                });
                for loader in 0..3 {
                    let (cache, source, done) = (&cache, &source, &done);
                    scope.spawn(move || {
                        let load = || {
                            let version = *source.lock();
                            thread::yield_now();
                            version
                        };
                        let mut round = 0_u64;
                        while !done.load(Ordering::Relaxed) {
                            round += 1;
                            if loader == 0 {
                                if let GuardResult::Guard(guard) =
                                    cache.get_value_or_guard(&0, None)
                                {
                                    guard.insert(load());
                                }
                            } else if round.is_multiple_of(2) {
                                cache.get_or_insert_with(&0, load);
                            } else {
                                block_on(cache.get_or_insert_async(&0, async { load() }));
                            }
                        }
                    });
                }
            });
            assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
            assert!(cache.shards.iter().all(|shard| shard.lock().loads() == 0));
        }
    }

    #[test]
    fn caches_guards_and_results_print_for_debugging() {
        // A struct holding a cache derives `Debug` whatever its entries are.
        #[derive(Debug)]
        #[allow(dead_code, reason = "printed, not read")]
        struct State {
            cache: ShardedCache<u64, std::cell::RefCell<()>>,
        }
        let cache = ShardedCacheBuilder::new(100).shards(1).build::<u64, u64>();
        cache.insert(1, 10);
        assert_eq!(
            format!("{cache:?}"),
            "ShardedCache { len: 1, weight: 1, capacity: 100, max_weight: None, shards: 1, .. }"
        );
        assert_eq!(format!("{:?}", cache.pin()), "ShardedCacheGuard { .. }");
        assert_eq!(
            format!("{:?}", cache.get_value_or_guard(&1, None)),
            "Value(10)"
        );
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&2, None) else {
            panic!("a missing key hands its caller the guard");
        };
        assert_eq!(format!("{guard:?}"), "PlaceholderGuard { key: 2, .. }");
        assert_eq!(
            format!("{:?}", cache.get_value_or_guard(&2, Some(Duration::ZERO))),
            "Timeout"
        );
        drop(guard);
        let state = State {
            cache: ShardedCache::new(10),
        };
        assert!(format!("{state:?}").starts_with("State { cache: ShardedCache {"));
    }

    #[test]
    fn superseded_loads_give_back_their_slots_and_drop_their_values() {
        static LIVE: AtomicUsize = AtomicUsize::new(0);

        struct Tracked;

        impl Tracked {
            fn new() -> Self {
                LIVE.fetch_add(1, Ordering::Relaxed);
                Self
            }
        }

        impl Clone for Tracked {
            fn clone(&self) -> Self {
                Self::new()
            }
        }

        impl Drop for Tracked {
            fn drop(&mut self) {
                LIVE.fetch_sub(1, Ordering::Relaxed);
            }
        }

        let cache = ShardedCache::<u64, Tracked>::new(64);
        let rounds = if cfg!(miri) { 200 } else { 20_000 };
        for round in 0..rounds {
            let key = round % 8;
            drop(cache.get_or_insert_with(&key, || {
                cache.remove(&key);
                Tracked::new()
            }));
        }
        assert!(cache.is_empty(), "every load was superseded");
        let entries = cache.memory_used().entries;
        assert!(
            entries < size_of::<Slot<(u64, Tracked)>>() * 4_096,
            "{entries} bytes of slots for loads that cached nothing"
        );
        cache.maintain();
        assert_eq!(LIVE.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn placeholder_guards_and_their_futures_are_send() {
        fn assert_send<T: Send>(_: &T) {}
        let cache = ShardedCache::<u64, String>::new(100);
        let waiting = cache.get_value_or_guard_async(&1);
        assert_send(&waiting);
        let Err(guard) = block_on(waiting) else {
            panic!("a missing key hands its caller the guard");
        };
        assert_send(&guard);
        assert!(guard.insert("one".to_owned()));
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn async_and_blocking_callers_that_miss_together_load_once() {
        for async_leader in [true, false] {
            let cache = Arc::new(ShardedCache::<u64, u64>::new(1_000));
            let loads = Arc::new(AtomicUsize::new(0));
            let (release, released) = std::sync::mpsc::channel::<()>();
            let leader = {
                let cache = Arc::clone(&cache);
                let loads = Arc::clone(&loads);
                thread::spawn(move || {
                    let load = move || {
                        loads.fetch_add(1, Ordering::Relaxed);
                        released.recv().expect("the test releases the loader");
                        70
                    };
                    if async_leader {
                        block_on(cache.get_or_insert_async(&7, async move { load() }))
                    } else {
                        cache.get_or_insert_with(&7, load)
                    }
                })
            };
            wait_for_waiters(&cache, 7, 0);
            let waiters: Vec<_> = (0..6)
                .map(|index| {
                    let cache = Arc::clone(&cache);
                    let loads = Arc::clone(&loads);
                    thread::spawn(move || {
                        let load = move || {
                            loads.fetch_add(1, Ordering::Relaxed);
                            71
                        };
                        if index % 2 == 0 {
                            block_on(cache.get_or_insert_async(&7, async move { load() }))
                        } else {
                            cache.get_or_insert_with(&7, load)
                        }
                    })
                })
                .collect();
            wait_for_waiters(&cache, 7, 6);
            release.send(()).expect("the leader is loading");
            assert_eq!(leader.join().expect("the leader completes"), 70);
            for waiter in waiters {
                assert_eq!(waiter.join().expect("a waiter completes"), 70);
            }
            assert_eq!(loads.load(Ordering::Relaxed), 1, "one load");
            assert!(cache.flights.iter().all(|lane| lane.0.lock().is_empty()));
        }
    }

    #[test]
    fn reads_keep_an_entry_for_its_time_to_idle_and_peeks_do_not() {
        let cache = ShardedCacheBuilder::new(100)
            .time_to_idle(HOUR)
            .build::<u64, u64>();
        cache.insert(1, 10);
        cache.insert(2, 20);
        assert_eq!(cache.insert_many([(4, 40), (5, 50)]), 2);
        cache.insert_with_ttl(3, 30, 3 * HOUR);
        cache.advance_clock(59 * MINUTE);
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.pin().get(&4), Some(&40));
        assert_eq!(cache.get_or_insert_with(&5, || 0), 50);
        assert_eq!(cache.get(&3), Some(30), "its own deadline is later");
        assert!(cache.contains_key(&2) && cache.peek(&2) == Some(20));
        assert_eq!(cache.iter().count(), 5);
        cache.advance_clock(2 * MINUTE);
        assert_eq!(cache.peek(&2), None, "unread for its time to idle");
        for (key, value) in [(1, 10), (4, 40), (5, 50), (3, 30)] {
            assert_eq!(
                cache.peek(&key),
                Some(value),
                "read within its time to idle"
            );
        }
        // A read moves a deadline later, never earlier: key 3 keeps its own
        // until reads come within the time to idle of it.
        cache.advance_clock(HOUR);
        assert_eq!((cache.get(&1), cache.get(&4)), (None, None));
        assert_eq!(cache.get(&3), Some(30));
        cache.advance_clock(59 * MINUTE);
        assert_eq!(cache.get(&3), Some(30));
        cache.advance_clock(59 * MINUTE);
        assert_eq!(cache.get(&3), Some(30), "the last read extended it");
        cache.advance_clock(HOUR + MINUTE);
        assert_eq!(cache.get(&3), None);
        assert_eq!(
            cache.stats().expirations,
            0,
            "expired entries stay until reached"
        );
    }

    #[test]
    #[should_panic(expected = "a time to live or a time to idle")]
    fn a_cache_expires_after_a_time_to_live_or_a_time_to_idle_not_both() {
        let _ = ShardedCacheBuilder::new(100)
            .time_to_live(HOUR)
            .time_to_idle(HOUR)
            .build::<u64, u64>();
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
    fn iteration_visits_each_live_entry_once_without_reading_it() {
        let cache = ShardedCacheBuilder::new(2_000)
            .shards(4)
            .build::<u64, u64>();
        for key in 0..500_u64 {
            cache.insert(key, key * 10);
        }
        cache.insert_with_ttl(1_000, 1, HOUR);
        cache.advance_clock(HOUR + MINUTE);
        let expected: Vec<(u64, u64)> = (0..500).map(|key| (key, key * 10)).collect();
        let mut cloned: Vec<(u64, u64)> = cache.iter().collect();
        cloned.sort_unstable();
        assert_eq!(cloned, expected, "live entries, each once, none expired");
        let guard = cache.pin();
        let mut borrowed: Vec<(u64, u64)> =
            guard.iter().map(|(key, value)| (*key, *value)).collect();
        borrowed.sort_unstable();
        assert_eq!(borrowed, expected);
        drop(guard);
        assert!(
            (0..500).all(|key| !is_marked(&cache, key)),
            "iteration is not a read"
        );
        assert_eq!(cache.stats().hits + cache.stats().misses, 0);
    }

    /// Keys below 10,000 hold ten times the key; the iteration tests add keys
    /// above it holding one more.
    fn consistent(key: u64, value: u64) -> bool {
        value == key * 10 || key >= 10_000 && value == key * 10 + 1
    }

    /// A two-shard cache of keys below 200, each holding ten times the key.
    fn iteration_cache() -> ShardedCache<u64, u64> {
        let cache = ShardedCacheBuilder::new(1_000)
            .shards(2)
            .build::<u64, u64>();
        for key in 0..200_u64 {
            cache.insert(key, key * 10);
        }
        cache
    }

    #[test]
    fn iterating_while_the_loop_body_writes_neither_blocks_nor_tears() {
        // Each shard is copied under its lock and visited after, so writes
        // from the loop body never wait on the iteration.
        let cache = iteration_cache();
        let guard = cache.pin();
        for (key, value) in guard.iter() {
            assert!(consistent(*key, *value), "{key} held {value}");
            if *key < 10_000 {
                cache.insert(key + 10_000, (key + 10_000) * 10 + 1);
                cache.remove(&(key + 1));
            }
        }
        drop(guard);
        for (key, value) in cache.iter() {
            cache.insert(key, value);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn iterating_while_another_thread_writes_yields_consistent_entries() {
        let cache = iteration_cache();
        let writer = {
            let cache = &cache;
            move || {
                for round in 0..STRESS_ROUNDS / 10 {
                    let key = round % 300;
                    cache.insert(key, key * 10);
                    cache.remove(&((round * 7) % 300));
                }
            }
        };
        thread::scope(|scope| {
            scope.spawn(writer);
            for _ in 0..10 {
                for (key, value) in cache.iter() {
                    assert!(consistent(key, value), "{key} held {value}");
                }
            }
        });
    }

    #[test]
    fn retain_removes_rejected_and_expired_entries_and_reports_why() {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let cache = ShardedCacheBuilder::new(2_000)
            .shards(4)
            .removal_listener({
                let heard = Arc::clone(&heard);
                move |key: &u64, _: &u64, cause| heard.lock().push((*key, cause))
            })
            .build::<u64, u64>();
        for key in 0..300_u64 {
            cache.insert(key, key);
        }
        cache.insert_with_ttl(1_000, 1, HOUR);
        cache.advance_clock(HOUR + MINUTE);
        let mut tested = Vec::new();
        cache.retain(|key, value| {
            assert_eq!(key, value);
            tested.push(*key);
            key % 3 != 0
        });
        tested.sort_unstable();
        assert_eq!(
            tested,
            (0..300).collect::<Vec<_>>(),
            "expired entries are not offered"
        );
        assert_eq!(cache.len(), 200);
        assert!((0..300).all(|key| cache.contains_key(&key) == (key % 3 != 0)));
        let mut heard = heard.lock().clone();
        heard.sort_unstable_by_key(|(key, _)| *key);
        let mut expected: Vec<(u64, RemovalCause)> = (0..300)
            .filter(|key| key % 3 == 0)
            .map(|key| (key, RemovalCause::Explicit))
            .collect();
        expected.push((1_000, RemovalCause::Expired));
        assert_eq!(heard, expected);
        assert_eq!(cache.stats().expirations, 1);
    }

    #[test]
    fn retain_tests_outside_every_lock_and_spares_entries_replaced_meanwhile() {
        // The predicate may write to the cache. A key it replaces was
        // tested by its old value, so the new one stays.
        let cache = ShardedCacheBuilder::new(1_000)
            .shards(2)
            .build::<u64, u64>();
        for key in 0..100_u64 {
            cache.insert(key, key);
        }
        cache.retain(|key, _| {
            cache.insert(*key, key + 1_000);
            cache.insert(key + 5_000, *key);
            false
        });
        for key in 0..100 {
            assert_eq!(cache.get(&key), Some(key + 1_000), "replaced key {key}");
        }
    }

    /// Each removal a listener heard, with its cause.
    type Heard = Arc<Mutex<Vec<(u64, RemovalCause)>>>;

    /// A cache whose removal listener records each key and cause.
    fn heard_cache(
        capacity: usize,
    ) -> (
        ShardedCache<u64, u64, UnitWeighter, DefaultHashBuilder, impl RemovalListener<u64, u64>>,
        Heard,
    ) {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let cache = ShardedCacheBuilder::new(capacity)
            .removal_listener({
                let heard = Arc::clone(&heard);
                move |key: &u64, _: &u64, cause| heard.lock().push((*key, cause))
            })
            .build::<u64, u64>();
        (cache, heard)
    }

    #[test]
    fn a_write_too_heavy_to_cache_reports_the_value_it_displaced_as_replaced() {
        let heard: Heard = Arc::new(Mutex::new(Vec::new()));
        let cache = ShardedCacheBuilder::new(16)
            .max_weight(16)
            .shards(1)
            .weighter(|_: &u64, value: &u64| *value)
            .removal_listener({
                let heard = Arc::clone(&heard);
                move |key: &u64, _: &u64, cause| heard.lock().push((*key, cause))
            })
            .build::<u64, u64>();
        cache.insert(1, 1);
        cache.insert(2, 2);
        // Each new value outweighs the whole cache.
        assert!(!cache.insert(1, 100));
        assert_eq!(cache.replace(2, 100), Err((2, 100)));
        assert_eq!(
            *heard.lock(),
            [(1, RemovalCause::Replaced), (2, RemovalCause::Replaced)]
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn remove_if_decides_and_removes_under_one_lock() {
        let (cache, heard) = heard_cache(100);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert_with_ttl(3, 30, HOUR);
        assert!(!cache.remove_if(&1, |value| *value == 11));
        assert_eq!(
            cache.get(&1),
            Some(10),
            "a rejected removal leaves the value"
        );
        assert!(cache.remove_if(&1, |value| *value == 10));
        assert_eq!(cache.get(&1), None);
        assert!(!cache.remove_if(&9, |_| unreachable!("an absent key is not offered")));
        cache.advance_clock(HOUR + MINUTE);
        assert!(!cache.remove_if(&3, |_| unreachable!("an expired value is not offered")));
        assert!(!cache.contains_key(&3));
        assert_eq!(cache.len(), 1);
        assert_eq!(
            *heard.lock(),
            [(1, RemovalCause::Explicit), (3, RemovalCause::Expired)]
        );
        assert_eq!(cache.stats().expirations, 1);
    }

    #[test]
    fn replace_writes_only_a_live_key_and_returns_the_pair_otherwise() {
        let (cache, heard) = heard_cache(100);
        assert_eq!(cache.replace(1, 10), Err((1, 10)), "absent");
        assert!(cache.is_empty());
        // A failed replace gives its slot back: slots are reused last in,
        // first out, so the next one taken is the one it borrowed.
        let (slot, _) = cache.slots.take();
        cache.slots.give(&[slot]);
        assert_eq!(cache.replace(7, 70), Err((7, 70)));
        assert_eq!(cache.slots.take().0, slot, "the slot came back");
        cache.insert(1, 10);
        assert_eq!(cache.replace(1, 11), Ok(()));
        assert_eq!(cache.get(&1), Some(11));
        cache.insert_with_ttl(2, 20, HOUR);
        cache.advance_clock(HOUR + MINUTE);
        assert_eq!(cache.replace(2, 21), Err((2, 21)), "expired");
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.len(), 2, "the expired entry waits for eviction");
        assert_eq!(*heard.lock(), [(1, RemovalCause::Replaced)]);
        // Replacing restarts the time to live, as inserting does.
        let expiring = ShardedCacheBuilder::new(100)
            .time_to_live(HOUR)
            .build::<u64, u64>();
        expiring.insert(1, 10);
        expiring.advance_clock(59 * MINUTE);
        assert_eq!(expiring.replace(1, 11), Ok(()));
        expiring.advance_clock(59 * MINUTE);
        assert_eq!(expiring.get(&1), Some(11));
    }

    #[test]
    fn replace_reweighs_and_a_value_too_heavy_removes_the_old_one() {
        let cache = ShardedCacheBuilder::new(100)
            .max_weight(100)
            .shards(1)
            .weighter(|_: &u64, value: &u64| *value)
            .build::<u64, u64>();
        for key in 0..4 {
            cache.insert(key, 20);
        }
        assert_eq!(cache.weight(), 80);
        assert_eq!(cache.replace(0, 50), Ok(()));
        assert!(cache.weight() <= 100, "evicted down to the limit");
        assert_eq!(
            cache.get(&0),
            Some(50),
            "the replacement is never evicted for itself"
        );
        assert_eq!(cache.replace(0, 101), Err((0, 101)));
        assert_eq!(
            cache.get(&0),
            None,
            "too heavy: the old value goes, as with insert"
        );
    }

    #[test]
    fn a_replacement_that_evicts_leaves_its_key_evictable() {
        // Probation holds the last five keys. Replacing the oldest of them
        // with a heavier value evicts, and the eviction pops the replaced
        // key's own record from probation.
        let cache = ShardedCacheBuilder::new(64)
            .max_weight(64)
            .shards(1)
            .weighter(|_: &u64, value: &u64| *value)
            .build::<u64, u64>();
        for key in 0..40 {
            cache.insert(key, 1);
        }
        cache.insert(35, 30);
        assert_eq!(cache.get(&35), Some(30));
        // Clear the access mark that read set, as an eviction sweep would.
        let _ = cache
            .live_entry(&cache.epochs.pin(), &35, cache.hash(&35))
            .map(|entry| (entry.take_accessed(), entry.take_accessed()));
        // Keys read during probation join the main area and push it to
        // evict: the cold keys already there leave, and so should key 35.
        for key in 1_000..3_000 {
            cache.insert(key, 1);
            cache.get(&key);
        }
        let survivors: Vec<u64> = (0..40).filter(|key| cache.contains_key(key)).collect();
        assert_eq!(survivors, Vec::<u64>::new(), "cold keys outlived the churn");
    }

    #[test]
    fn zero_sized_and_over_aligned_entries_round_trip() {
        // Handles count eight-byte words from the region's base, so slots
        // of any size and alignment must keep them exact.
        #[repr(align(128))]
        #[derive(Clone, Debug, PartialEq)]
        struct Wide([u64; 3]);

        let units: ShardedCache<(), ()> = ShardedCache::new(4);
        assert!(units.insert((), ()));
        assert_eq!(units.get(&()), Some(()));
        assert!(units.remove(&()));
        assert_eq!(units.get(&()), None);
        let cache = ShardedCacheBuilder::new(512).shards(2).build::<u64, Wide>();
        let keys = if cfg!(miri) { 300 } else { 2_000 };
        for key in 0..keys {
            cache.insert(key, Wide([key, key + 1, key + 2]));
        }
        let guard = cache.pin();
        let mut found = 0;
        for key in 0..keys {
            if let Some(value) = guard.get(&key) {
                assert_eq!(*value, Wide([key, key + 1, key + 2]));
                assert_eq!(std::ptr::from_ref(value).addr() % 128, 0);
                found += 1;
            }
        }
        assert_eq!(found, cache.len());
    }

    #[test]
    fn panics_in_hash_eq_weigher_and_clone_leave_the_cache_whole() {
        // Each kind of user code panics now and then, some of it under the
        // shard lock: the cache may leak what the panicking operation held,
        // but stays usable, within its limits, and right about every key.
        thread_local! {
            static ARMED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        }
        fn tripped(kind: u64) -> bool {
            ARMED.with(|armed| armed.get() == kind && armed.replace(0) == kind)
        }

        #[derive(Clone, Copy, Debug)]
        struct Key(u64);
        impl Hash for Key {
            fn hash<H: Hasher>(&self, state: &mut H) {
                assert!(!tripped(1), "a panicking hash");
                self.0.hash(state);
            }
        }
        impl PartialEq for Key {
            fn eq(&self, other: &Self) -> bool {
                assert!(!tripped(2), "a panicking comparison");
                self.0 == other.0
            }
        }
        impl Eq for Key {}

        struct Value(u64);
        impl Clone for Value {
            fn clone(&self) -> Self {
                assert!(!tripped(3), "a panicking clone");
                Self(self.0)
            }
        }

        let cache = ShardedCacheBuilder::new(64)
            .shards(2)
            .weighter(|_key: &Key, value: &Value| {
                assert!(!tripped(4), "a panicking weigher");
                1 + value.0 % 2
            })
            .max_weight(100)
            .build::<Key, Value>();
        let rounds = if cfg!(miri) { 300 } else { 20_000 };
        let mut random = 0x1234_5678_u64;
        for round in 0..rounds {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key = Key(random % 200);
            if round % 7 == 0 {
                ARMED.with(|armed| armed.set(1 + (random >> 20) % 4));
            }
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match random % 9 {
                0..=3 => {
                    cache.insert(key, Value(round));
                }
                4 => {
                    cache
                        .insert_many((0..3).map(|step| (Key((random + step) % 200), Value(round))));
                }
                5 => {
                    cache.remove(&key);
                }
                6 => {
                    let _ = cache.get(&key);
                }
                7 => {
                    let _ = cache.get_or_insert_with(&key, || Value(round));
                }
                _ => {
                    let _ = cache.replace(key, Value(round));
                }
            }));
            ARMED.with(|armed| armed.set(0));
        }
        for key in 0..200 {
            cache.insert(Key(key), Value(key));
        }
        assert!(cache.len() <= 64 && cache.weight() <= 100);
        let mut found = 0;
        for key in 0..200 {
            if let Some(value) = cache.get(&Key(key)) {
                assert_eq!(value.0, key);
                found += 1;
            }
        }
        assert_eq!(found, cache.len());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn a_panicking_listener_still_drops_removed_values() {
        struct Counted(Arc<AtomicUsize>);
        impl Clone for Counted {
            fn clone(&self) -> Self {
                Counted(Arc::clone(&self.0))
            }
        }
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        {
            let cache = ShardedCacheBuilder::new(64)
                .shards(1)
                .removal_listener(|_: &u64, _: &Counted, _| panic!("listener"))
                .build::<u64, Counted>();
            for key in 0..64 {
                cache.insert(key, Counted(Arc::clone(&dropped)));
            }
            // The 65th entry evicts one, and the listener panics on it.
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cache.insert(1_000, Counted(Arc::clone(&dropped)))
            }));
            assert!(panicked.is_err());
        }
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            65,
            "every value dropped once"
        );
    }

    #[test]
    fn drain_yields_live_entries_and_leaves_the_cache_empty() {
        let (cache, heard) = heard_cache(2_000);
        for key in 0..300_u64 {
            cache.insert(key, key * 10);
        }
        cache.insert_with_ttl(1_000, 1, HOUR);
        cache.advance_clock(HOUR + MINUTE);
        let mut drained: Vec<(u64, u64)> = cache.drain().collect();
        drained.sort_unstable();
        assert_eq!(
            drained,
            (0..300).map(|key| (key, key * 10)).collect::<Vec<_>>()
        );
        assert_eq!((cache.len(), cache.weight()), (0, 0));
        assert!((0..300).all(|key| !cache.contains_key(&key)));
        let mut heard = heard.lock().clone();
        heard.sort_unstable_by_key(|(key, _)| *key);
        let mut expected: Vec<(u64, RemovalCause)> =
            (0..300).map(|key| (key, RemovalCause::Explicit)).collect();
        expected.push((1_000, RemovalCause::Expired));
        assert_eq!(heard, expected);
        assert_eq!(cache.stats().expirations, 1);
        for key in 0..300_u64 {
            cache.insert(key, key);
        }
        assert_eq!(cache.len(), 300, "a drained cache refills");
    }

    #[test]
    fn a_dropped_drain_stops_after_the_shard_it_reached() {
        let heard = Arc::new(AtomicUsize::new(0));
        let cache = ShardedCacheBuilder::new(2_000)
            .shards(4)
            .removal_listener({
                let heard = Arc::clone(&heard);
                move |_: &u64, _: &u64, _| {
                    heard.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build::<u64, u64>();
        for key in 0..400_u64 {
            cache.insert(key, key);
        }
        let first_shard = cache.shards[0].writer.len.load(Ordering::Relaxed);
        let mut drain = cache.drain();
        let first = drain.next().expect("a drained entry");
        // The loop body may write: each shard is emptied under its lock and
        // its entries yielded after it is released. The key goes back to
        // the shard just emptied.
        cache.insert(first.0, first.1);
        drop(drain);
        assert_eq!(
            heard.load(Ordering::Relaxed),
            first_shard,
            "dropping retires the whole shard it emptied"
        );
        let left = cache.len();
        assert_eq!(left, 400 - first_shard + 1, "only the first shard drained");
        assert_eq!(cache.drain().count(), left);
        assert!(cache.is_empty());
    }

    #[test]
    fn memory_used_counts_the_table_the_slots_handed_out_and_the_policy() {
        let cache = ShardedCacheBuilder::new(1_000)
            .shards(2)
            .build::<u64, u64>();
        let empty = cache.memory_used();
        assert_eq!(empty.table, cache.table.len() * size_of::<Bucket>());
        assert_eq!(empty.entries, 0, "no slot handed out yet");
        assert!(empty.policy > 0);
        assert!(empty.bookkeeping > 0);
        assert_eq!(
            empty.total(),
            empty.table + empty.entries + empty.policy + empty.bookkeeping
        );
        for key in 0..500_u64 {
            cache.insert(key, key);
        }
        let filled = cache.memory_used();
        let slot = size_of::<Slot<(u64, u64)>>();
        assert!(
            filled.entries >= 500 * slot,
            "{} bytes for 500 entries",
            filled.entries
        );
        assert_eq!(filled.table, empty.table);
        // Removed slots are reused, so refilling takes no new ones.
        cache.clear();
        cache.maintain();
        for key in 1_000..1_500_u64 {
            cache.insert(key, key);
        }
        assert_eq!(cache.memory_used().entries, filled.entries);
    }

    #[test]
    fn set_capacity_evicts_at_once_and_grows_back_within_the_reserve() {
        // At full size the scenario takes minutes under Miri, which runs the
        // tenth-size one alone.
        if !cfg!(miri) {
            resize_scenario(1_000);
        }
        resize_scenario(100);
    }

    /// Builds a cache of `built` entries with room for four times as many,
    /// shrinks it to a fifth, grows it past the reserve, and shrinks it to
    /// one entry per shard.
    fn resize_scenario(built: usize) {
        let (reserve, shrunk) = (4 * built, built / 5);
        let key = |count: usize| count as u64;
        let heard = Arc::new(Mutex::new(Vec::new()));
        let cache = ShardedCacheBuilder::new(built)
            .max_capacity(reserve)
            .shards(4)
            .removal_listener({
                let heard = Arc::clone(&heard);
                move |_: &u64, _: &u64, cause| heard.lock().push(cause)
            })
            .build::<u64, u64>();
        assert_eq!((cache.capacity(), cache.max_capacity()), (built, reserve));
        for key in 0..key(built) {
            cache.insert(key, key);
        }
        let full = cache.len();
        heard.lock().clear();
        assert_eq!(cache.set_capacity(shrunk), shrunk);
        assert_eq!(cache.capacity(), shrunk);
        assert!(
            cache.len() <= shrunk,
            "{} entries in a cache of {shrunk}",
            cache.len()
        );
        let evicted = heard.lock().clone();
        assert_eq!(evicted.len(), full - cache.len());
        assert!(evicted.iter().all(|cause| *cause == RemovalCause::Size));
        for key in key(built)..key(built + 2 * shrunk) {
            cache.insert(key, key);
        }
        assert!(cache.len() <= shrunk);
        assert_eq!(
            cache.set_capacity(10 * built),
            reserve,
            "capped at the reserve"
        );
        let grown = key(2 * built)..key(2 * built + reserve);
        for key in grown.clone() {
            cache.insert(key, key);
        }
        assert!(
            (reserve / 2..=reserve).contains(&cache.len()),
            "{} entries after growing to {reserve}",
            cache.len()
        );
        assert!(grown.filter(|key| cache.get(key) == Some(*key)).count() >= reserve / 2);
        assert_eq!(cache.set_capacity(1), 4, "one entry per shard");
        assert!(cache.len() <= 4);
    }

    #[test]
    fn a_cache_reserves_its_built_capacity_and_its_weight_limit_resizes() {
        let cache = ShardedCacheBuilder::new(1_000)
            .max_weight(1_000)
            .weighter(|_: &u64, value: &u64| *value)
            .shards(4)
            .build::<u64, u64>();
        assert_eq!(cache.max_capacity(), 1_000);
        assert_eq!(cache.set_capacity(2_000), 1_000);
        for key in 0..100_u64 {
            cache.insert(key, 10);
        }
        assert!(cache.weight() > 300);
        cache.set_max_weight(300);
        assert_eq!(cache.max_weight(), 300);
        assert!(cache.weight() <= 300, "weight {}", cache.weight());
        assert!(
            !cache.insert(1_000, 200),
            "heavier than a shard's new share"
        );
        cache.set_max_weight(u64::MAX);
        assert!(cache.insert(1_000, 200));
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn resizing_while_other_threads_write_and_read_keeps_the_last_limit() {
        let cache = ShardedCacheBuilder::new(2_000)
            .max_capacity(4_000)
            .build::<u64, u64>();
        thread::scope(|scope| {
            for thread in 0..4_u64 {
                let cache = &cache;
                scope.spawn(move || {
                    for round in 0..STRESS_ROUNDS / 4 {
                        let key = (thread << 32) | (round % 5_000);
                        cache.insert(key, key);
                        if let Some(value) = cache.get(&(key ^ 1)) {
                            assert_eq!(value, key ^ 1);
                        }
                    }
                });
            }
            for round in 0..20 {
                cache.set_capacity(if round % 2 == 0 { 300 } else { 4_000 });
                thread::yield_now();
            }
        });
        assert_eq!(cache.set_capacity(300), 300);
        assert!(
            cache.len() <= 300,
            "{} entries in a cache of 300",
            cache.len()
        );
        for key in 0..1_000_u64 {
            cache.insert(key, key);
        }
        assert!(cache.len() <= 300);
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
        for current in &cache.table {
            for handle in &current.handles {
                let raw = handle.load(Ordering::Relaxed);
                if raw != 0 {
                    let handle = Handle {
                        raw,
                        slot: cache.slots.slot(raw),
                    };
                    // SAFETY: the table publishes this handle while
                    // `pin` is held.
                    let key = unsafe { entry(&pin, handle) }.value().0;
                    assert!(key >= refill, "cleared key {key} is still published");
                    assert!(cache.contains_key(&key), "key {key} is not findable");
                    resident += 1;
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
    fn a_capacity_reserve_costs_its_table_alone() {
        let (built, reserve) = (100, 1_000);
        let plain = ShardedCacheBuilder::new(built)
            .shards(2)
            .build::<u64, u64>();
        let reserved = ShardedCacheBuilder::new(built)
            .max_capacity(reserve)
            .shards(2)
            .build::<u64, u64>();
        let (bare, roomy) = (plain.memory_used(), reserved.memory_used());
        assert_eq!(
            roomy.policy, bare.policy,
            "the policy is sized for the capacity"
        );
        // About 8 bytes of table per reserved entry.
        let (table, entries) = (roomy.table - bare.table, reserve - built);
        assert!(
            2 * table < 17 * entries,
            "{table} bytes of table for {entries} reserved entries"
        );
        // Raised to the reserve, the cache fills its main area, whose queue
        // grows with it.
        reserved.set_capacity(reserve);
        for key in 0..2 * reserve as u64 {
            reserved.insert(key, key);
            reserved.get(&key);
        }
        assert!(
            reserved.len() > reserve * 9 / 10,
            "{} entries",
            reserved.len()
        );
        assert!(reserved.memory_used().policy > bare.policy);
    }

    #[test]
    fn an_oversized_shard_count_is_capped_at_one_entry_per_shard() {
        let cache = ShardedCacheBuilder::new(64)
            .shards(usize::MAX)
            .build::<u64, u64>();
        assert_eq!(cache.shards.len(), 64);
        cache.insert(1, 1);
        assert_eq!(cache.get(&1), Some(1));
    }

    #[test]
    fn memory_used_counts_what_every_cache_has_whatever_its_capacity() {
        let cache = ShardedCacheBuilder::new(64).shards(1).build::<u64, u64>();
        // Three epochs of 64 reader counters and 64 retired-slot queues,
        // each counter and queue on its own 128-byte line.
        let fixed = (3 * 64 + 64) * 128;
        let memory = cache.memory_used();
        assert!(
            memory.total() >= fixed,
            "{} bytes reported, {fixed} in counters and queues alone",
            memory.total()
        );
    }

    #[test]
    fn the_table_holds_ten_slots_per_cache_line() {
        // Four-byte handles fit ten slots in a 64-byte bucket, where
        // eight-byte addresses fit six: 8 bytes of table per entry at 80%
        // load instead of 13.3.
        assert_eq!(std::mem::size_of::<Bucket>(), 64);
        assert_eq!(SLOTS, 10);
        // Small enough to stay quick under Miri.
        let cache = ShardedCacheBuilder::new(10_000)
            .shards(1)
            .build::<u64, u64>();
        let bytes = cache.table.len() * std::mem::size_of::<Bucket>();
        assert!(
            bytes <= 8 * 10_000 + 64,
            "{bytes} table bytes for 10,000 entries"
        );
    }

    #[test]
    fn the_slab_region_spares_a_lone_writers_backlog_and_each_threads_stack() {
        // Advances over the reader shards in use keep a lone writer's
        // removed entries to about a hundred, so the spare past the entry
        // limit is 1,024 slots and 256 per available thread.
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        let cache = ShardedCacheBuilder::new(1_000).build::<u64, u64>();
        assert_eq!(cache.slots.region_slots(), 1_000 + 1_024 + 256 * threads);
    }

    #[test]
    #[cfg_attr(miri, ignore = "twenty thousand inserts past the slab region")]
    fn memory_a_guard_held_is_freed_after_it_drops() {
        let cache = ShardedCacheBuilder::new(256).shards(1).build::<u64, u64>();
        let guard = cache.pin();
        for key in 0..20_000 {
            cache.insert(key, key);
        }
        assert!(
            cache.slots.overflow_chunks() > 0,
            "a guard keeps every removed entry, past the slab region"
        );
        let in_overflow = cache
            .table
            .iter()
            .flat_map(|bucket| &bucket.handles)
            .filter(|handle| handle.load(Ordering::Relaxed) & OVERFLOW_HANDLE != 0)
            .count();
        assert!(in_overflow > 0, "cached entries live in overflow slots");
        let found = (0..20_000_u64)
            .filter(|key| {
                let value = cache.get(key);
                assert!(value.is_none_or(|value| value == *key));
                value.is_some()
            })
            .count();
        assert_eq!(found, cache.len(), "every cached entry reads back");
        drop(guard);
        cache.clear();
        cache.maintain();
        assert_eq!(
            cache.slots.overflow_chunks(),
            0,
            "the slots the guard held stayed allocated"
        );
        for key in 0..20_000 {
            cache.insert(key, key);
        }
        assert_eq!(
            cache.slots.overflow_chunks(),
            0,
            "writes took overflow slots while region slots were vacant"
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
    fn large_caches_split_into_more_shards_of_at_least_256_entries() {
        // Small caches keep four shards per thread, fewer when small.
        assert_eq!(default_shards(1_000, 16), 16);
        assert_eq!(default_shards(15_000, 16), 64);
        // Larger ones split further, so writers rarely meet on a shard lock.
        assert_eq!(default_shards(50_000, 16), 128);
        assert_eq!(default_shards(200_000, 16), 256);
        assert_eq!(default_shards(1_000_000, 16), 256);
        assert_eq!(default_shards(1_000_000, 64), 1_024);
        for entries in [32_768, 50_000, 65_536, 200_000, 1 << 20] {
            for parallelism in [1, 4, 16, 64] {
                let shards = default_shards(entries, parallelism);
                assert!(shards.is_power_of_two());
                assert!(
                    shards <= (parallelism * 4).next_power_of_two() || entries / shards >= 256,
                    "{shards} shards for {entries} entries on {parallelism} threads"
                );
            }
        }
    }

    #[test]
    fn shards_share_one_bucket_table() {
        // A read finds its shard's buckets from the hash alone, without
        // loading the shard.
        let cache = ShardedCacheBuilder::new(10_000)
            .shards(8)
            .build::<u64, u64>();
        let per_shard = cache.shard_buckets;
        assert_eq!(cache.table.len(), 8 * per_shard);
        for index in 0..8 {
            let view = cache.view(index);
            assert_eq!(
                view.buckets.as_ptr(),
                cache.table[index * per_shard..].as_ptr()
            );
            assert_eq!(view.buckets.len(), per_shard);
        }
    }

    #[test]
    fn more_shards_than_entries_still_bound_the_cache() {
        let cache = ShardedCacheBuilder::new(100)
            .shards(1024)
            .build::<u64, u64>();
        // Under Miri, 2,000 keys: they still reach about 880 of 1,024
        // shards, so shards that each admitted an entry would hold far over
        // 100.
        let keys = if cfg!(miri) { 2_000 } else { 10_000 };
        for key in 0..keys {
            cache.insert(key, key);
        }
        assert!(
            cache.len() <= 100,
            "{} entries in a cache of 100",
            cache.len()
        );
    }

    #[test]
    fn probation_records_name_the_entries_evictions_prefetch() {
        let cache = ShardedCacheBuilder::new(256).shards(1).build::<u64, u64>();
        for key in 0..1_000 {
            cache.insert(key, key);
        }
        let shard = cache.view(0);
        let writer = shard.lock();
        assert!(!writer.probation.is_empty());
        for probationary in &writer.probation {
            if let Some(slot) = shard.live_record(probationary.record) {
                let published = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
                assert_eq!(probationary.handle, published);
            }
        }
    }

    #[test]
    fn a_full_shards_insert_prefetches_the_lines_its_eviction_writes() {
        // One shard at its limit with every key unread, so the next insert
        // evicts the probation front: its bucket loses the handle, and the
        // ghost remembers its hash. The new key lands in its home bucket
        // after the ghost is checked for it.
        let cache = ShardedCacheBuilder::new(256).shards(1).build::<u64, u64>();
        for key in 0..1_000 {
            cache.insert(key, key);
        }
        let shard = cache.view(0);
        // The next insert must evict the probation front. A missed key the
        // ghost's current generation seems to remember (one fill in 400)
        // grows the probation target and skips probation, so the insert
        // would evict from the main area; a relocated front key (one in 750)
        // leaves a stale record, which eviction skips. Later keys restore
        // both.
        let mut key = 1_000;
        while {
            let writer = shard.lock();
            writer.probation.len() <= writer.filters.probation_target()
                || writer
                    .probation
                    .front()
                    .is_none_or(|front| shard.live_record(front.record).is_none())
        } {
            cache.insert(key, key);
            key += 1;
        }
        let fresh = cache.hash(&5_000_u64);
        let (victim, lines) = {
            let writer = shard.lock();
            let front = *writer.probation.front().expect("probation holds a key");
            let slot = shard
                .live_record(front.record)
                .expect("the front key is live");
            let ghost = |hash: u64| writer.filters.ghost_line(hash) as usize;
            (
                front.hash,
                [
                    (
                        "the victim's bucket",
                        std::ptr::from_ref(shard.bucket(slot)) as usize,
                    ),
                    ("the victim's ghost line", ghost(front.hash)),
                    ("the new key's ghost line", ghost(fresh)),
                    (
                        "the new key's home bucket",
                        std::ptr::from_ref(&shard.buckets[shard.home(fresh)]) as usize,
                    ),
                ],
            )
        };
        PREFETCHED.with(|prefetched| prefetched.borrow_mut().clear());
        cache.insert(5_000, 5_000);
        let prefetched = PREFETCHED.with(std::cell::RefCell::take);
        for (name, line) in lines {
            assert!(prefetched.contains(&line), "{name} was not prefetched");
        }
        assert!(shard.lock().filters.admit(victim, usize::MAX));
    }

    /// The kernel's flags for the mapping that holds `address`.
    #[cfg(all(target_os = "linux", not(miri)))]
    fn mapping_flags(address: usize) -> Vec<String> {
        let smaps = std::fs::read_to_string("/proc/self/smaps").expect("smaps is readable");
        let mut inside = false;
        for line in smaps.lines() {
            let first = line.split(' ').next().unwrap_or_default();
            if let Some((start, end)) = first.split_once('-')
                && let (Ok(start), Ok(end)) = (
                    usize::from_str_radix(start, 16),
                    usize::from_str_radix(end, 16),
                )
            {
                inside = (start..end).contains(&address);
            } else if inside && let Some(flags) = line.strip_prefix("VmFlags:") {
                return flags.split_whitespace().map(str::to_owned).collect();
            }
        }
        panic!("no mapping holds {address:#x}");
    }

    #[test]
    #[cfg(all(target_os = "linux", not(miri)))]
    fn large_tables_and_slab_regions_ask_linux_for_huge_pages() {
        // Hosts that enable transparent huge pages only where asked, the
        // default on many distributions, backed a million-entry cache's 8 MB
        // table and its slab region with 4 KB pages.
        let cache = ShardedCacheBuilder::new(1_000_000).build::<u64, u64>();
        let table = cache.table.as_ptr() as usize + size_of_val(&*cache.table) / 2;
        let (region, length) = cache.slots.region_span();
        for (name, address) in [("table", table), ("slab region", region + length / 2)] {
            let flags = mapping_flags(address);
            assert!(
                flags.iter().any(|flag| flag == "hg"),
                "the {name} is not advised for huge pages: {flags:?}"
            );
        }
    }

    #[test]
    fn insert_many_leaves_the_cache_as_inserting_one_at_a_time_does() {
        // Batches share a pin and retire their removals together; the
        // entries, replacements, and evictions are those of single inserts.
        let build = || {
            ShardedCacheBuilder::new(500)
                .shards(1)
                .hasher(BuildHasherDefault::<DefaultHasher>::default())
                .build::<u64, u64>()
        };
        let (one, many) = (build(), build());
        // Under Miri, a fifth of the inserts over fewer keys: still more
        // keys than the shard holds, and repeats that replace and re-admit.
        let (inserts, keys) = if cfg!(miri) {
            (1_000, 700)
        } else {
            (5_000, 1_700)
        };
        let pairs: Vec<(u64, u64)> = (0..inserts).map(|index| (index % keys, index)).collect();
        for &(key, value) in &pairs {
            one.insert(key, value);
        }
        assert_eq!(many.insert_many(pairs.iter().copied()), pairs.len());
        assert_eq!(many.len(), one.len());
        assert_eq!(many.stats().evictions, one.stats().evictions);
        for key in 0..keys {
            assert_eq!(many.peek(&key), one.peek(&key), "key {key}");
        }
        assert_eq!(many.insert_many(std::iter::empty()), 0);
        assert_eq!(many.insert_many([(9_999, 1)]), 1);
        assert_eq!(many.peek(&9_999), Some(1));
        // A value over the weight limit is refused and removes the key.
        let weighted = ShardedCacheBuilder::new(100)
            .max_weight(100)
            .weighter(|_: &u64, value: &u64| *value)
            .build::<u64, u64>();
        assert_eq!(weighted.insert_many([(1, 1), (2, 2), (1, 1_000)]), 2);
        assert_eq!(weighted.peek(&1), None);
        assert_eq!(weighted.peek(&2), Some(2));
    }

    #[test]
    fn insert_many_pins_once_and_prefetches_each_home_bucket_first() {
        // A pin per insert holds back the loads after it; a batch takes one
        // and starts loading every key's home bucket before inserting.
        let cache = ShardedCacheBuilder::new(1_000).build::<u64, u64>();
        let homes: Vec<usize> = (0..64_u64)
            .map(|key| {
                let hash = cache.hash(&key);
                let shard = cache.shard(hash);
                std::ptr::from_ref(&shard.buckets[shard.home(hash)]) as usize
            })
            .collect();
        let pins = crate::sharded_arena::pins_taken();
        PREFETCHED.with(|prefetched| prefetched.borrow_mut().clear());
        assert_eq!(cache.insert_many((0..64).map(|key| (key, key))), 64);
        assert_eq!(crate::sharded_arena::pins_taken() - pins, 1);
        let prefetched = PREFETCHED.with(std::cell::RefCell::take);
        assert_eq!(prefetched, homes, "an empty cache prefetches nothing else");
        for key in 0..64 {
            assert_eq!(cache.get(&key), Some(key));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "fifty thousand inserts")]
    fn a_long_insert_many_lets_its_removed_entries_be_reclaimed() {
        // One pin held for a whole batch would block every advance past
        // its epoch, so each removed entry would wait for the batch's end.
        let queued = |batched: bool| {
            let cache = ShardedCacheBuilder::new(500).shards(1).build::<u64, u64>();
            if batched {
                cache.insert_many((0..50_000).map(|key| (key, key)));
            } else {
                for key in 0..50_000 {
                    cache.insert(key, key);
                }
            }
            cache.retired.queued(crate::sharded_arena::retiring_index())
        };
        let (single, batched) = (queued(false), queued(true));
        assert!(
            batched <= 2 * single.max(INSERT_MANY_CHUNK),
            "{batched} removed entries await reclamation, against {single} for single inserts"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "forty thousand inserts")]
    fn insert_many_drains_a_reclamation_backlog_as_inserts_do() {
        // A pinned reader holds back every removal's reclamation. Once it
        // leaves, each write reclaims one more entry than it retires until
        // the backlog is gone; batches that retire a full reclamation batch
        // at a time never would, and the slab region would run out.
        let queued = |batched: bool| {
            let cache = ShardedCacheBuilder::new(500).shards(1).build::<u64, u64>();
            {
                let _reader = cache.pin();
                for key in 0..5_000 {
                    cache.insert(key, key);
                }
            }
            let backlog = cache.retired.queued(crate::sharded_arena::retiring_index());
            let keys = 5_000..20_000;
            if batched {
                cache.insert_many(keys.map(|key| (key, key)));
            } else {
                for key in keys {
                    cache.insert(key, key);
                }
            }
            let left = cache.retired.queued(crate::sharded_arena::retiring_index());
            (backlog, left)
        };
        let ((backlog, single), (_, batched)) = (queued(false), queued(true));
        assert!(backlog > 4_000, "the reader held back {backlog} removals");
        assert!(
            batched <= 2 * single.max(INSERT_MANY_CHUNK),
            "{batched} of {backlog} held-back removals await reclamation after a batch, \
             against {single} after single inserts"
        );
    }

    #[test]
    fn keys_evicted_from_probation_return_to_the_main_area() {
        let cache = ShardedCacheBuilder::new(256).shards(1).build::<u64, u64>();
        let in_probation = |key: u64| {
            let hash = cache.hash(&key);
            cache.shards[0]
                .lock()
                .probation
                .iter()
                .any(|probationary| probationary.hash == hash)
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
        // Keys the ghost never saw start in probation, but for the rare one
        // its words remember by chance: the filter's false positives, which
        // failed a lone key here on 1 hash seed in 300.
        let fresh = (5_000..5_020)
            .filter(|&key| {
                cache.insert(key, key);
                in_probation(key)
            })
            .count();
        assert!(fresh >= 17, "{fresh} of 20 fresh keys started in probation");
    }

    #[test]
    #[cfg_attr(miri, ignore = "a churn of a hundred thousand inserts")]
    fn main_queue_cleanup_examines_a_bounded_chunk_per_write() {
        // A full shard taking only new keys keeps its oldest main keys, so
        // the queue's front stays live while relocations append records
        // that go stale behind it. Compacting the whole queue whenever the
        // front had nothing stale held one write for 16 µs. Relocating only
        // away from home halved relocations: over 300 hash seeds, the first
        // compaction came after 73,000 inserts at the median and 113,000 at
        // most.
        let cache = ShardedCacheBuilder::new(1_000).shards(1).build();
        for key in 0..300_000_u64 {
            let before = MAIN_RECORDS_EXAMINED.with(std::cell::Cell::get);
            cache.insert(key, key);
            let examined = MAIN_RECORDS_EXAMINED.with(std::cell::Cell::get) - before;
            // Unread, an insert moves at most two keys to the main area: a
            // relocated one and probation's overflow.
            assert!(
                examined <= 2 * MAIN_CLEANUP_CHUNK as u64,
                "insert {key} examined {examined} main records"
            );
        }
        assert!(
            MAIN_RECORDS_EXAMINED.with(std::cell::Cell::get) > 0,
            "relocations' records made the queue compact"
        );
    }

    #[test]
    fn the_main_queue_matches_a_deque_around_its_ring() {
        // The model compacts by removing records where its walk stands.
        let mut queue = MainQueue::with_capacity(7);
        let mut model = VecDeque::new();
        let mut cursor = 0;
        let mut state = 0x2545_f491_u32;
        for step in 0..if cfg!(miri) { 2_000 } else { 20_000_u32 } {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            match state % 4 {
                0 | 1 if queue.span() < 7 => {
                    queue.push_back(step);
                    model.push_back(step);
                }
                2 => {
                    let budget = (state >> 8) as usize % 4;
                    queue.compact(budget, |_| {}, |record| record % 3 != 0);
                    for _ in 0..budget {
                        if model.is_empty() {
                            break;
                        }
                        if model[cursor] % 3 == 0 {
                            model.remove(cursor);
                        } else {
                            cursor += 1;
                        }
                        if cursor == model.len() {
                            cursor = 0;
                            break;
                        }
                    }
                }
                _ => {
                    assert_eq!(queue.pop_front(), model.pop_front());
                    cursor = cursor.saturating_sub(1);
                }
            }
            assert_eq!(queue.records(), Vec::from(model.clone()));
            assert_eq!(queue.cursor as usize, cursor);
        }
        while let Some(record) = model.pop_front() {
            assert_eq!(queue.pop_front(), Some(record));
        }
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn filters_size_probation_from_recent_evictions_and_forget_older_ones() {
        // A 1,024-entry shard: ghost generations, and the main filter, hold
        // 64 keys each.
        let mut filters = Filters::new(1_024, 8);
        let hash = |key: u64| BuildHasherDefault::<DefaultHasher>::default().hash_one(key);
        filters.remember_probationary(hash(1));
        filters.remember_main(hash(2));
        // Each filter answers only for its own keys.
        assert!(filters.admit(hash(1), 100));
        assert_eq!(filters.probation_target(), 8 + PROBATION_GROWTH);
        assert!(!filters.admit(hash(2), 100));
        assert_eq!(
            filters.probation_target(),
            8 + PROBATION_GROWTH - PROBATION_SHRINKAGE
        );
        assert!(!filters.admit(hash(3), 100));
        assert_eq!(filters.probation_target(), 7);
        // Once later evictions fill its generation, the ghost still admits
        // the key, but no longer counts it as recent.
        for key in 100..164 {
            filters.remember_probationary(hash(key));
        }
        assert!(filters.admit(hash(1), 100));
        assert_eq!(filters.probation_target(), 7);
        // Eight generations later it is forgotten.
        for key in 200..200 + 7 * 64 {
            filters.remember_probationary(hash(key));
        }
        assert!(!filters.admit(hash(1), 100));
        // The main filter forgets everything once it holds 64 keys.
        for key in 1_000..1_062 {
            filters.remember_main(hash(key));
        }
        assert!(!filters.admit(hash(2), 100));
        assert_eq!(filters.probation_target(), 5);
        filters.remember_main(hash(1_062));
        assert!(!filters.admit(hash(2), 100));
        assert_eq!(filters.probation_target(), 5);
    }

    #[test]
    #[cfg_attr(miri, ignore = "thousands of remembers over 512 ghost lines")]
    fn remembering_a_key_clears_a_bounded_number_of_ghost_lines() {
        // A 16,384-entry shard: 512 ghost lines, 1,024 keys a generation.
        // Rotating cleared the next generation's word in every line at
        // once, 25 µs under the shard lock for a 1,000,000-entry shard.
        let mut filters = Filters::new(16_384, 8);
        let lines = filters.main_start();
        assert_eq!(lines, 512);
        let hash = |key: u64| BuildHasherDefault::<DefaultHasher>::default().hash_one(key);
        for line in &mut filters.lines[..lines] {
            line.0 = [u64::MAX; GHOST_GENERATIONS];
        }
        let cleared = |filters: &Filters, generation: usize| {
            filters.lines[..lines]
                .iter()
                .filter(|line| line.0[generation] == 0)
                .count()
        };
        for key in 0..3 * 1_024 {
            let next = (filters.current() + 1) % GHOST_GENERATIONS;
            let before = cleared(&filters, next);
            filters.remember_probationary(hash(key));
            let after = cleared(&filters, next);
            assert!(
                after - before <= GHOST_CLEARED_LINES,
                "key {key} cleared {}",
                after - before
            );
            if filters.current() == next {
                // A new generation starts empty.
                assert_eq!(after, lines, "key {key} rotated into a remembered word");
            }
        }
    }

    #[test]
    fn a_keys_ghost_words_fill_one_cache_line() {
        assert_eq!(std::mem::size_of::<FilterLine>(), 64);
        assert_eq!(std::mem::align_of::<FilterLine>(), 64);
    }

    #[test]
    fn a_key_back_within_the_ghosts_current_generation_grows_probation() {
        // One 1,024-entry shard, whose ghost generations hold 64 keys each.
        let cache = ShardedCacheBuilder::new(1_024)
            .shards(1)
            .build::<u64, u64>();
        let target = || cache.view(0).lock().filters.probation_target();
        for key in 0..1_024 {
            cache.insert(key, key);
        }
        // The next insert evicts the first key from probation, and the
        // ghost's first generation starts with it.
        let mut key = 1_024;
        cache.insert(key, key);
        let evicted = (0..key)
            .find(|old| !cache.contains_key(old))
            .expect("a full shard evicts");
        // Forty more evictions keep it within that generation.
        for _ in 0..40 {
            key += 1;
            cache.insert(key, key);
        }
        let before = target();
        cache.insert(evicted, evicted);
        assert_eq!(target(), before + PROBATION_GROWTH);
    }

    type FixedHashCache = ShardedCache<u64, u64, UnitWeighter, BuildHasherDefault<DefaultHasher>>;

    /// A one-shard cache whose hashes, and so whose evictions, repeat from
    /// run to run.
    fn fixed_hash_cache(probation_bps: u16) -> FixedHashCache {
        ShardedCacheBuilder::new(1_000)
            .shards(1)
            .probation_bps(probation_bps)
            .hasher(BuildHasherDefault::<DefaultHasher>::default())
            .build()
    }

    /// A look-aside read: a miss inserts the key. Returns whether it hit.
    fn read_through(cache: &FixedHashCache, key: u64) -> bool {
        let hit = cache.get(&key).is_some();
        if !hit {
            cache.insert(key, key);
        }
        hit
    }

    #[test]
    #[cfg_attr(miri, ignore = "a hit-ratio run of tens of thousands of requests")]
    fn probation_grows_to_hold_keys_read_again_soon_after_arriving() {
        // Every key is read twice, 40 new keys apart. A probation of 20 keys
        // evicts each before its second read, and the ghost then admits it to
        // the main area, where nothing reads it again.
        const GAP: u64 = 40;
        let cache = fixed_hash_cache(200);
        let steps = 20_000;
        let mut late_hits = 0;
        for step in 0..steps {
            read_through(&cache, step);
            if step >= GAP && read_through(&cache, step - GAP) && step >= steps / 2 {
                late_hits += 1;
            }
        }
        let late_reads = steps / 2;
        assert!(
            late_hits * 10 > late_reads * 9,
            "{late_hits} of {late_reads} second reads hit"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "a hit-ratio run of tens of thousands of requests")]
    fn probation_shrinks_until_a_hot_loop_fits_the_main_area() {
        // 960 keys read in a loop fit a 1,000-entry shard, but not beside a
        // 10% probation; one-time keys arrive between them.
        const HOT: u64 = 960;
        let cache = fixed_hash_cache(1_000);
        let rounds = 40;
        let mut late_hits = 0;
        let mut fresh = HOT;
        for round in 0..rounds {
            for key in 0..HOT {
                if read_through(&cache, key) && round >= rounds / 2 {
                    late_hits += 1;
                }
                if key % 10 == 0 {
                    read_through(&cache, fresh);
                    fresh += 1;
                }
            }
        }
        let late_reads = HOT * rounds / 2;
        assert!(
            late_hits * 10 > late_reads * 9,
            "{late_hits} of {late_reads} hot reads hit"
        );
    }

    #[test]
    fn a_writes_shard_state_spans_two_cache_lines() {
        // Writers' cores take turns holding a shard's lock, and each line of
        // shard state a write touches moves to its core while it holds the
        // lock. Replacing needs the lock word and the counts; an insert that
        // evicts from probation needs the probation queue, then the filters.
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
        let filters = std::ptr::addr_of!(writer.filters) as usize;
        assert_eq!(line(filters, std::mem::size_of_val(&writer.filters)), 1);
        let main = std::ptr::addr_of!(writer.main) as usize;
        assert_eq!(line(main, std::mem::size_of_val(&writer.main)), 1);
    }

    #[test]
    fn announced_writes_retire_without_a_lock() {
        thread::spawn(|| {
            let cache: ShardedCache<u64, u64> = ShardedCacheBuilder::new(64).shards(1).build();
            for key in 0..32 {
                cache.insert(key, key);
            }
            if crate::sharded_arena::announced_slot().is_none() {
                // Every announcer slot is held: this thread pins through
                // the shared counters, whose queues lock.
                return;
            }
            let locks = || RETIRE_LOCKS.with(std::cell::Cell::get);
            let before = locks();
            for round in 1..=8 {
                for key in 0..32 {
                    cache.insert(key, key + round);
                }
            }
            assert_eq!(
                locks(),
                before,
                "an announced write locked its retired queue"
            );
        })
        .join()
        .expect("the writer finishes");
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn other_writers_drain_the_lane_of_an_idle_live_thread() {
        let cache = Arc::new(ShardedCacheBuilder::new(256).shards(4).build::<u64, u64>());
        let (release, parked) = std::sync::mpsc::channel::<()>();
        let (report, reported) = std::sync::mpsc::channel();
        let writer = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for key in 0..5_000 {
                    cache.insert(key, key);
                }
                report
                    .send(crate::sharded_arena::announced_slot())
                    .expect("the main thread listens");
                // Alive and unpinned, retiring nothing.
                let _ = parked.recv();
            })
        };
        if let Some(lane) = reported.recv().expect("the writer reports its slot") {
            assert!(
                cache.retired.lane_queued(lane) > 0,
                "the idle thread left retirements"
            );
            for key in 10_000..10_000 + 40_000 * CHURN_SCALE {
                cache.insert(key, key);
            }
            assert_eq!(
                cache.retired.lane_queued(lane),
                0,
                "the idle thread's lane was not drained"
            );
        }
        drop(release);
        writer.join().expect("the writer exits");
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn a_pinned_threads_lane_is_drained_only_after_it_unpins() {
        let cache = Arc::new(ShardedCacheBuilder::new(256).shards(4).build::<u64, u64>());
        let (to_writer, from_main) = std::sync::mpsc::channel::<()>();
        let (to_main, from_writer) = std::sync::mpsc::channel();
        let writer = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for key in 0..5_000 {
                    cache.insert(key, key);
                }
                let step = |slot| {
                    to_main.send(slot).expect("the main thread listens");
                    from_main.recv().is_ok()
                };
                if !step(crate::sharded_arena::announced_slot()) {
                    return;
                }
                let guard = cache.pin();
                step(None);
                drop(guard);
                step(None);
            })
        };
        let step = || {
            to_writer.send(()).expect("the writer listens");
            from_writer.recv().expect("the writer answers")
        };
        let Some(lane) = from_writer.recv().expect("the writer reports its slot") else {
            drop(to_writer);
            writer.join().expect("the writer exits");
            return;
        };
        // Age everything the writer retired, then let it pin.
        for _ in 0..4 {
            assert!(cache.epochs.advance_evicting(), "nothing else is pinned");
        }
        step();
        let queued = cache.retired.lane_queued(lane);
        assert!(queued > 0, "the writer left retirements");
        // Every lane and queue is visited once per 128 foreign drains, one
        // per 16 of this thread's retirements.
        for key in 10_000..10_000 + 4 * 128 * 16 {
            cache.insert(key, key);
        }
        assert_eq!(
            cache.retired.lane_queued(lane),
            queued,
            "a drainer took slots from a pinned thread's lane"
        );
        step();
        for key in 20_000..20_000 + 40_000 * CHURN_SCALE {
            cache.insert(key, key);
        }
        assert_eq!(
            cache.retired.lane_queued(lane),
            0,
            "the unpinned thread's lane was not drained"
        );
        drop(to_writer);
        writer.join().expect("the writer exits");
    }

    #[test]
    #[cfg_attr(miri, ignore = "parking_lot's futex wait fails Miri's variadic check")]
    fn other_writers_drain_the_retired_queue_of_an_idle_thread() {
        let cache = Arc::new(ShardedCacheBuilder::new(256).shards(4).build::<u64, u64>());
        // Claim this thread's announcer slot first: one the idle thread frees
        // when it exits would pass its lane to this thread, as the lane of a
        // slot belongs to whichever thread holds the slot.
        drop(cache.pin());
        let idle_queue = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for key in 0..5_000 {
                    cache.insert(key, key);
                }
                crate::sharded_arena::retiring_index()
            })
            .join()
            .expect("the idle thread's writes complete")
        };
        if idle_queue == crate::sharded_arena::retiring_index() {
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
