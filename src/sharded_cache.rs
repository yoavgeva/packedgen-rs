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
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::cache_arena::direct_arena_thread;
#[cfg(test)]
use crate::sharded_arena::PREFETCHED;
use crate::sharded_arena::{
    EpochPin, Epochs, MAX_EXPIRY_TICK, MAX_WEIGHT, OVERFLOW_HANDLE, Reclaimed, RetiredSlots, Slot,
    SlotDecoder, SlotStore, advise_huge_pages, drop_value, fill, prefetch, prefetch_write,
};

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
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        let shards = self
            .shards
            .unwrap_or_else(|| default_shards(max_entries, parallelism));
        // Every shard holds an entry: a shard with none would admit one.
        let most = 1 << max_entries.ilog2();
        // Main-queue records hold a slot in 24 bits.
        let mut shards = shards.min(most);
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
        // Shards share one bucket count, sized for the largest share.
        let shard_slots = ((base + usize::from(extra != 0)) * SLOTS_PER_ENTRY_NUMERATOR)
            .div_ceil(SLOTS_PER_ENTRY_DENOMINATOR)
            .max(SLOTS * 2);
        let shard_buckets = shard_slots.div_ceil(SLOTS);
        assert!(
            u32::try_from(shard_buckets * SLOTS).is_ok(),
            "a shard holds fewer than 2^32 slots"
        );
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
            table: empty_table(shards * shard_buckets),
            shard_buckets,
            shard_shift: u64::BITS - shards.trailing_zeros(),
            epochs: Epochs::new(),
            retired: RetiredSlots::new(),
            slots: SlotStore::new(
                max_entries
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
            capacity: max_entries,
            max_weight,
            #[cfg(test)]
            clock_skew_nanos: AtomicU64::new(0),
        }
    }
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
    capacity: usize,
    max_weight: u64,
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
/// never change, so their line stays in every core's cache; the writer lock
/// and the state it guards change on every write, so they sit on cache lines
/// of their own.
struct Shard<T> {
    limits: ShardLimits,
    writer: ShardLock,
    /// Tables publish handles to `Slot<T>`s; the cache's `RetiredSlots` and
    /// `SlotStore` carry the ownership bounds.
    _entries: PhantomData<fn() -> T>,
}

struct ShardLimits {
    capacity: usize,
    weight: u64,
    /// The largest probation target the shard adapts to.
    probation_max: usize,
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
        let hash = self.hash(key);
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
            let mut removed = Removed::new();
            let mut advance = false;
            for (hash, key, value) in chunk.drain(..) {
                cached += usize::from(self.insert_pinned(
                    key,
                    value,
                    self.time_to_live,
                    hash,
                    pin,
                    &mut removed,
                ));
                if removed.len == removed.first.len() {
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
        let inserted = self.insert_pinned(key, value, ttl, hash, &pin, &mut removed);
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
        ttl: Option<Duration>,
        hash: u64,
        pin: &EpochPin<'_>,
        removed: &mut Removed<(K, V)>,
    ) -> bool {
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
        let (slot, raw) = self.slots.take();
        // SAFETY: `take` handed this vacant slot to this caller alone, and
        // nothing can reach it before it is published below.
        unsafe { fill(slot, (key, value), compact_weight, expires_at) };
        let handle = Handle { raw, slot };
        // SAFETY: the caller's own filled slot, published below or never.
        let key = &unsafe { entry(pin, handle) }.value().0;
        {
            let mut writer = shard.lock();
            if writer.over_limit(weight) {
                shard.prefetch_eviction(&writer, hash);
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
                while writer.weight() > shard.limits.weight {
                    let Some((victim, cause)) =
                        self.evict(shard, &mut writer, pin, Some(handle), &mut now)
                    else {
                        break;
                    };
                    removed.push(victim, cause);
                }
            } else {
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
                let record = shard.record(index);
                if writer.filters.admit(hash, shard.limits.probation_max) {
                    // Remembered from a recent eviction: straight to main.
                    writer.enter_main(shard, record);
                } else {
                    if writer.probation.len() >= writer.filters.probation_record_limit() {
                        // An overflowing record's key joins the main area.
                        let overflow = writer
                            .probation
                            .pop_front()
                            .expect("probation is at its limit");
                        writer.enter_main(shard, overflow.record);
                    }
                    writer.probation.push_back(Probationary {
                        record,
                        hash,
                        handle: handle.raw,
                    });
                }
            }
        }
        true
    }

    /// Ends a write after its shard lock, still pinned: retires what it
    /// removed, unpins, and advances the epoch generation if enough
    /// retirements are pending.
    fn after_write(&self, removed: &Removed<(K, V)>, pin: EpochPin<'_>) {
        let advance = self.retire_removed(removed, &pin);
        drop(pin);
        if advance {
            self.epochs.try_advance();
        }
    }

    /// Tells the listener what left, queues the unpublished entries for
    /// reclamation, and drops the values of entries aged enough to reclaim.
    /// Requires the pin taken before the entries were unpublished. Returns
    /// whether enough retirements are pending to try advancing.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    fn retire_removed(&self, removed: &Removed<(K, V)>, pin: &EpochPin<'_>) -> bool {
        if removed.len == 0 {
            return false;
        }
        removed.notify(&self.listener, pin);
        let mut reclaimed = Reclaimed::new();
        let mut foreign = Reclaimed::new();
        let advance = self.retired.retire(
            pin,
            removed.iter().map(|(handle, _)| handle.slot),
            &mut reclaimed,
            &mut foreign,
        );
        reclaimed.finish(&self.slots);
        foreign.finish(&self.slots);
        advance
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
            if let Some((index, _)) = found {
                let handle = shard.clear(index);
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
        for index in 0..self.shards.len() {
            let shard = self.view(index);
            let pin = self.epochs.pin();
            let mut removed = Removed::new();
            {
                let mut writer = shard.lock();
                for bucket in shard.buckets {
                    let meta = bucket.meta.load(Ordering::Relaxed);
                    if bucket.tags.load(Ordering::Relaxed) == 0 && meta & !META_GENERATIONS == 0 {
                        continue;
                    }
                    // Tags first, as `ShardView::clear` does, then the
                    // handles.
                    bucket.tags.store(0, Ordering::Release);
                    bucket
                        .meta
                        .store(meta & META_GENERATIONS, Ordering::Release);
                    for slot in 0..SLOTS {
                        let raw = bucket.exchange(slot, 0);
                        if raw != 0 {
                            removed.push(shard.handle(raw), RemovalCause::Explicit);
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
            self.after_write(&removed, pin);
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
            self.epochs.try_advance();
        }
        self.retired
            .reclaim_all(self.epochs.generation(), &self.slots);
    }

    fn shard_index(&self, hash: u64) -> usize {
        // `checked_shr` keeps the single-shard shift of 64 in range.
        let shard = hash.checked_shr(self.shard_shift).unwrap_or(0);
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
    fn new(capacity: usize, weight_limit: u64, probation_bps: u16) -> Self {
        let probation_max = (capacity * MAX_PROBATION_BPS / 10_000).max(1);
        let probation_target =
            (capacity * usize::from(probation_bps) / 10_000).clamp(1, probation_max);
        Self {
            limits: ShardLimits {
                capacity,
                weight: weight_limit,
                probation_max,
            },
            writer: ShardLock {
                len: AtomicUsize::new(0),
                weight: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
                writer: Mutex::new(ShardWriter {
                    probation: VecDeque::new(),
                    filters: Filters::new(capacity, probation_target),
                    main: MainQueue::with_capacity(capacity + capacity / 4 + 17),
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
    /// Lines the insert writes are fetched for writing. Writers call it only
    /// for a full shard, so writes that evict nothing fetch nothing extra.
    #[inline]
    fn prefetch_eviction(&self, writer: &ShardWriter, hash: u64) {
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
        for key in 0..10_000 {
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
        let pairs: Vec<(u64, u64)> = (0..5_000).map(|index| (index % 1_700, index)).collect();
        for &(key, value) in &pairs {
            one.insert(key, value);
        }
        assert_eq!(many.insert_many(pairs.iter().copied()), pairs.len());
        assert_eq!(many.len(), one.len());
        assert_eq!(many.stats().evictions, one.stats().evictions);
        for key in 0..1_700 {
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
            cache
                .retired
                .queued(crate::sharded_arena::retired_queue_index())
        };
        let (single, batched) = (queued(false), queued(true));
        assert!(
            batched <= 2 * single.max(INSERT_MANY_CHUNK),
            "{batched} removed entries await reclamation, against {single} for single inserts"
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
        // A key the ghost never saw starts in probation.
        cache.insert(5_000, 5_000);
        assert!(in_probation(5_000));
    }

    #[test]
    #[cfg_attr(miri, ignore = "a churn of a hundred thousand inserts")]
    fn main_queue_cleanup_examines_a_bounded_chunk_per_write() {
        // A full shard taking only new keys keeps its oldest main keys, so
        // the queue's front stays live while relocations append records
        // that go stale behind it. Compacting the whole queue whenever the
        // front had nothing stale held one write for 16 µs.
        let cache = ShardedCacheBuilder::new(1_000).shards(1).build();
        for key in 0..100_000_u64 {
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
