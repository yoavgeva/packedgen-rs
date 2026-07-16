use core::fmt;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicUsize, Ordering};

use hashbrown::DefaultHashBuilder;
use parking_lot::RwLock;

use crate::{ArenaError, InsertOutcome, KeyCompactionStats, SegmentedLoad, SegmentedSwissMap};

const MIN_DEFAULT_SHARDS: usize = 16;
const MAX_DEFAULT_SHARDS: usize = 256;
const KEY_SEGMENT_BYTES_PER_SHARD: usize = 8 * 1024;

/// A compact binary-key map with independent read/write-locked shards.
///
/// Unlike putting one map behind one lock, unrelated keys can be mutated in
/// parallel. Each shard owns both its Swiss tables and packed-key arena, so
/// insertion has no hidden global allocation lock. Operations on one key are
/// linearizable because routing always selects the same shard.
///
/// This is a lock-based concurrent map, not a lock-free ETS replacement.
/// [`Self::get_cloned`] gives ETS-like copy-out semantics; [`Self::with_value`]
/// avoids a clone while keeping the shard read-locked for the closure.
pub struct ConcurrentSwissMap<V> {
    shards: Box<[CacheAlignedShard<V>]>,
    route_hasher: DefaultHashBuilder,
    shard_shift: u32,
}

impl<V> ConcurrentSwissMap<V> {
    /// Creates an empty map with a hardware-sensitive default shard count.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a map sized for `entry_capacity` live records.
    ///
    /// The default shard count is four times the available hardware threads,
    /// rounded to a power of two and bounded to 16..=256. Use
    /// [`Self::try_with_capacity_and_shards`] for reproducible deployments and
    /// benchmarks.
    ///
    /// # Panics
    ///
    /// Panics only if the internally selected power-of-two shard count is
    /// rejected, which indicates an internal invariant error.
    #[must_use]
    pub fn with_capacity(entry_capacity: usize) -> Self {
        Self::try_with_capacity_and_shards(
            entry_capacity,
            default_shard_count(),
            SegmentedLoad::Balanced,
        )
        .expect("the internally selected shard count is valid")
    }

    /// Creates a map with explicit concurrency and RAM/latency policy.
    ///
    /// `shard_count` must be a non-zero power of two. Capacity is divided
    /// across shards without dropping the remainder.
    ///
    /// # Errors
    ///
    /// Returns [`ConcurrentConfigError`] for zero or non-power-of-two shard
    /// counts.
    ///
    /// # Panics
    ///
    /// Panics only if the internal 8-KiB packed-key segment size is no longer
    /// representable, which indicates an internal invariant error.
    pub fn try_with_capacity_and_shards(
        entry_capacity: usize,
        shard_count: usize,
        load: SegmentedLoad,
    ) -> Result<Self, ConcurrentConfigError> {
        if shard_count == 0 {
            return Err(ConcurrentConfigError::ZeroShards);
        }
        if !shard_count.is_power_of_two() {
            return Err(ConcurrentConfigError::ShardCountNotPowerOfTwo(shard_count));
        }

        let route_hasher = DefaultHashBuilder::default();
        let mut shards = Vec::with_capacity(shard_count);
        for shard_index in 0..shard_count {
            let capacity = divided_capacity(entry_capacity, shard_count, shard_index);
            let map = SegmentedSwissMap::try_with_capacity_key_bytes_load_and_hasher(
                capacity,
                KEY_SEGMENT_BYTES_PER_SHARD,
                load,
                route_hasher.clone(),
            )
            .expect("the fixed concurrent key-segment size is representable");
            shards.push(CacheAlignedShard {
                map: RwLock::new(map),
                len: AtomicUsize::new(0),
            });
        }

        Ok(Self {
            shards: shards.into_boxed_slice(),
            route_hasher,
            shard_shift: usize::BITS - shard_count.trailing_zeros(),
        })
    }

    /// Inserts a key or atomically replaces its existing value.
    ///
    /// # Errors
    ///
    /// Returns an arena error when the key is too long or key storage cannot
    /// be allocated.
    pub fn try_insert(&self, key: &[u8], value: V) -> Result<InsertOutcome<V>, ArenaError> {
        let hash = self.route_hasher.hash_one(key);
        let shard = self.shard(hash);
        let mut map = shard.map.write();
        let outcome = map.try_insert_prehashed(hash, key, value)?;
        if matches!(outcome, InsertOutcome::Inserted) {
            shard.len.fetch_add(1, Ordering::Release);
        }
        Ok(outcome)
    }

    /// Inserts only when `key` is absent, matching ETS `insert_new` semantics.
    ///
    /// The existence check and insertion happen under one shard write lock.
    /// Returns `true` when inserted and `false` when the key already existed.
    ///
    /// # Errors
    ///
    /// Returns an arena error when the key is too long or key storage cannot
    /// be allocated.
    pub fn try_insert_new(&self, key: &[u8], value: V) -> Result<bool, ArenaError> {
        let hash = self.route_hasher.hash_one(key);
        let shard = self.shard(hash);
        let mut map = shard.map.write();
        if map.get_prehashed(hash, key).is_some() {
            return Ok(false);
        }
        let outcome = map.try_insert_prehashed(hash, key, value)?;
        debug_assert!(matches!(outcome, InsertOutcome::Inserted));
        shard.len.fetch_add(1, Ordering::Release);
        Ok(true)
    }

    /// Clones a value out of the map, matching ETS lookup ownership semantics.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value(key, |value| value.cloned())
    }

    /// Runs `read` with an optional value while holding its shard read lock.
    ///
    /// This avoids cloning large values. The closure must be short and must
    /// not call a write operation on this map: re-entering the same shard for
    /// writing can deadlock.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        let hash = self.route_hasher.hash_one(key);
        let map = self.shard(hash).map.read();
        read(map.get_prehashed(hash, key))
    }

    /// Returns whether `key` is present.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.with_value(key, |value| value.is_some())
    }

    /// Atomically updates an existing value and returns the closure result.
    ///
    /// Returns `None` without running `update` when the key is absent. Keep the
    /// closure short and do not re-enter this map from it.
    pub fn update<R>(&self, key: &[u8], update: impl FnOnce(&mut V) -> R) -> Option<R> {
        let hash = self.route_hasher.hash_one(key);
        let mut map = self.shard(hash).map.write();
        map.get_mut_prehashed(hash, key).map(update)
    }

    /// Atomically inserts a missing value or updates an existing value.
    ///
    /// The returned outcome distinguishes which path ran and carries the
    /// update closure's return value. Keep `update` short and do not re-enter
    /// this map from it.
    ///
    /// # Errors
    ///
    /// Returns an arena error when the insertion path cannot store the key.
    pub fn try_upsert_with<R>(
        &self,
        key: &[u8],
        insert_value: V,
        update: impl FnOnce(&mut V) -> R,
    ) -> Result<UpsertOutcome<R>, ArenaError> {
        let hash = self.route_hasher.hash_one(key);
        let shard = self.shard(hash);
        let mut map = shard.map.write();
        if let Some(value) = map.get_mut_prehashed(hash, key) {
            return Ok(UpsertOutcome::Updated(update(value)));
        }

        let outcome = map.try_insert_prehashed(hash, key, insert_value)?;
        debug_assert!(matches!(outcome, InsertOutcome::Inserted));
        shard.len.fetch_add(1, Ordering::Release);
        Ok(UpsertOutcome::Inserted)
    }

    /// Removes `key` and returns its value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<V> {
        let hash = self.route_hasher.hash_one(key);
        let shard = self.shard(hash);
        let mut map = shard.map.write();
        let removed = map.remove_prehashed(hash, key)?;
        shard.len.fetch_sub(1, Ordering::Release);
        Some(removed)
    }

    /// Atomically removes `key` only when `predicate` accepts its value.
    ///
    /// This supports race-free lazy expiry: a concurrent refresh cannot occur
    /// between the predicate check and deletion. Keep the predicate short and
    /// do not re-enter this map from it.
    #[must_use]
    pub fn remove_if(&self, key: &[u8], predicate: impl FnOnce(&V) -> bool) -> Option<V> {
        let hash = self.route_hasher.hash_one(key);
        let shard = self.shard(hash);
        let mut map = shard.map.write();
        if !map.get_prehashed(hash, key).is_some_and(predicate) {
            return None;
        }
        let removed = map.remove_prehashed(hash, key);
        debug_assert!(removed.is_some());
        if removed.is_some() {
            shard.len.fetch_sub(1, Ordering::Release);
        }
        removed
    }

    /// Removes all records atomically with respect to map operations.
    ///
    /// All shard locks are acquired in index order before any shard is
    /// cleared. This operation is intentionally expensive.
    pub fn clear(&self) {
        let mut maps: Vec<_> = self.shards.iter().map(|shard| shard.map.write()).collect();
        for (shard, map) in self.shards.iter().zip(&mut maps) {
            map.clear();
            shard.len.store(0, Ordering::Release);
        }
    }

    /// Compacts deleted key bytes one shard at a time.
    ///
    /// Other shards remain available while one shard is rewritten. A caller
    /// can therefore schedule this away from request latency without stopping
    /// every reader and writer at once.
    ///
    /// # Errors
    ///
    /// Returns the first staging allocation or arena error. Shards compacted
    /// before that error stay compacted; the failing shard is unchanged.
    ///
    /// # Panics
    ///
    /// Panics only if a live entry has an invalid packed-key reference, which
    /// indicates internal invariant corruption.
    pub fn compact_key_arenas(&self) -> Result<KeyCompactionStats, ArenaError> {
        let mut total = KeyCompactionStats::default();
        for shard in &self.shards {
            let compacted = shard.map.write().compact_keys()?;
            total.live_entries += compacted.live_entries;
            total.copied_key_bytes += compacted.copied_key_bytes;
            total.discarded_key_bytes += compacted.discarded_key_bytes;
            total.reclaimed_allocated_bytes += compacted.reclaimed_allocated_bytes;
        }
        Ok(total)
    }

    /// Number of completed live insertions minus removals.
    ///
    /// The value is exact when no operation is concurrently in flight and is
    /// weakly consistent during concurrent mutation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.len.load(Ordering::Acquire))
            .sum()
    }

    /// Returns whether the observed live count is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.len.load(Ordering::Acquire) == 0)
    }

    /// Number of independently writable shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Captures an exact, stop-the-writers structural snapshot.
    ///
    /// Read locks are held on every shard while statistics are aggregated.
    /// Normal point operations never acquire more than one shard lock.
    #[must_use]
    pub fn stats(&self) -> ConcurrentMapStats {
        let maps: Vec<_> = self.shards.iter().map(|shard| shard.map.read()).collect();
        let mut stats = ConcurrentMapStats {
            len: 0,
            shards: maps.len(),
            segments: 0,
            table_capacity: 0,
            route_buckets: 0,
            arena_allocated_bytes: 0,
            arena_key_bytes: 0,
            live_key_bytes: 0,
        };
        for map in &maps {
            let shard_stats = map.stats();
            stats.len += shard_stats.len;
            stats.segments += shard_stats.segments;
            stats.table_capacity += shard_stats.table_capacity;
            stats.route_buckets += shard_stats.route_buckets;
            stats.arena_allocated_bytes += shard_stats.arena_allocated_bytes;
            stats.arena_key_bytes += shard_stats.arena_key_bytes;
            stats.live_key_bytes += shard_stats.live_key_bytes;
        }
        stats
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn shard(&self, hash: u64) -> &CacheAlignedShard<V> {
        if self.shards.len() == 1 {
            return &self.shards[0];
        }
        // Match DashMap's routing geometry: use bits below SwissTable's top
        // seven-bit SIMD tag, preserving both the tag and low bucket bits for
        // the inner table.
        let index = ((hash as usize) << 7) >> self.shard_shift;
        &self.shards[index]
    }
}

impl<V> Default for ConcurrentSwissMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(align(64))]
struct CacheAlignedShard<V> {
    map: RwLock<SegmentedSwissMap<V>>,
    len: AtomicUsize,
}

/// Result of an atomic [`ConcurrentSwissMap::try_upsert_with`] operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpsertOutcome<R> {
    /// The key was absent and the supplied value was inserted.
    Inserted,
    /// The key existed and the update closure ran.
    Updated(R),
}

/// Invalid concurrent-map construction parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcurrentConfigError {
    /// At least one shard is required.
    ZeroShards,
    /// Fast stable routing requires a power-of-two shard count.
    ShardCountNotPowerOfTwo(usize),
}

impl fmt::Display for ConcurrentConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroShards => formatter.write_str("concurrent map requires at least one shard"),
            Self::ShardCountNotPowerOfTwo(shards) => {
                write!(
                    formatter,
                    "concurrent map shard count {shards} is not a power of two"
                )
            }
        }
    }
}

impl std::error::Error for ConcurrentConfigError {}

/// Observable retained state for a [`ConcurrentSwissMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConcurrentMapStats {
    /// Live entries across all shards.
    pub len: usize,
    /// Independently lockable shards.
    pub shards: usize,
    /// Swiss allocation segments across all shards.
    pub segments: usize,
    /// Sum of entry capacity before a shard segment grows.
    pub table_capacity: usize,
    /// Sum of planned Swiss route buckets.
    pub route_buckets: usize,
    /// Packed-arena allocation bytes across all shards.
    pub arena_allocated_bytes: usize,
    /// Appended key bytes, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to live entries.
    pub live_key_bytes: usize,
}

impl ConcurrentMapStats {
    /// Bytes retained for removed keys until [`ConcurrentSwissMap::clear`].
    #[must_use]
    pub const fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}

fn default_shard_count() -> usize {
    std::thread::available_parallelism()
        .map_or(MIN_DEFAULT_SHARDS, |threads| {
            threads.get().saturating_mul(4).next_power_of_two()
        })
        .clamp(MIN_DEFAULT_SHARDS, MAX_DEFAULT_SHARDS)
}

fn divided_capacity(total: usize, shards: usize, index: usize) -> usize {
    total / shards + usize::from(index < total % shards)
}
