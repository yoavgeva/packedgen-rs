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
    DirectArenaEntry, DirectArenaPin, DirectHandle, DirectValueArena, RemovedDirectHandle,
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
const NEVER_EXPIRES: u64 = u64::MAX;
/// Probation's default share of each shard, in basis points.
const DEFAULT_PROBATION_BPS: u16 = 200;
/// The ghost holds this many generations of evicted probationary keys.
const GHOST_GENERATIONS: usize = 4;
/// Each ghost generation holds this fraction of the shard's capacity.
const GHOST_GENERATION_DIVISOR: usize = 8;
const GHOST_BITS_PER_KEY: usize = 32;
const GHOST_KEY_BITS: u32 = 5;

/// A bounded concurrent cache with lock-free reads and sharded writers.
///
/// `get` clones the value, so `V` is typically small or an `Arc`.
pub struct ShardedCache<K, V, S = DefaultHashBuilder> {
    shards: Box<[Shard]>,
    shard_shift: u32,
    arena: DirectValueArena<(K, V)>,
    hash_builder: S,
    capacity: usize,
}

#[repr(C, align(64))]
struct Bucket {
    /// Tag bytes for the six slots (zero marks an empty slot), then the
    /// count of keys that probed past this bucket.
    control: AtomicU64,
    handles: [AtomicU64; SLOTS],
}

struct Shard {
    buckets: Box<[Bucket]>,
    len: AtomicUsize,
    writer: Mutex<ShardWriter>,
}

struct ShardWriter {
    len: usize,
    capacity: usize,
    /// The CLOCK hand: the next slot the main-area sweep examines.
    hand: usize,
    /// Probationary keys in insertion order: (handle, slot, hash low bits).
    probation: VecDeque<(u64, u32, u32)>,
    probation_target: usize,
    /// Past this many records the oldest is dropped, leaving its key to the
    /// main sweep, so records never copy more than a share of the keys.
    probation_limit: usize,
    ghost: Ghost,
}

impl<K: Hash + Eq, V> ShardedCache<K, V> {
    /// Creates a cache holding at most `max_entries` entries.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self::with_hasher(max_entries, DefaultHashBuilder::default())
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> ShardedCache<K, V, S> {
    /// Creates a cache with an explicit hasher.
    #[must_use]
    pub fn with_hasher(max_entries: usize, hash_builder: S) -> Self {
        Self::with_probation_and_hasher(max_entries, DEFAULT_PROBATION_BPS, hash_builder)
    }

    /// Creates a cache whose probationary FIFO targets `probation_bps`
    /// basis points of each shard.
    ///
    /// # Panics
    ///
    /// Panics if one shard would need 2^32 or more table slots.
    #[must_use]
    pub fn with_probation_and_hasher(
        max_entries: usize,
        probation_bps: u16,
        hash_builder: S,
    ) -> Self {
        let max_entries = max_entries.max(1);
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        let shards = (parallelism * 4)
            .next_power_of_two()
            .min(
                (max_entries / MIN_SHARD_CAPACITY)
                    .max(1)
                    .next_power_of_two(),
            )
            .max(1);
        let base = max_entries / shards;
        let extra = max_entries % shards;
        Self {
            shards: (0..shards)
                .map(|shard| Shard::new(base + usize::from(shard < extra), probation_bps))
                .collect(),
            shard_shift: u64::BITS - shards.trailing_zeros(),
            arena: DirectValueArena::new(64),
            hash_builder,
            capacity: max_entries,
        }
    }

    /// The configured entry limit.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The number of resident entries; a weak snapshot under concurrency.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.len.load(Ordering::Relaxed))
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
    pub fn pin(&self) -> ShardedCacheGuard<'_, K, V, S> {
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
    /// when it is full.
    #[allow(
        unsafe_code,
        reason = "publishes, replaces, and retires arena handles under the shard lock"
    )]
    pub fn insert(&self, key: K, value: V) {
        let hash = self.hash_builder.hash_one(&key);
        let shard = self.shard(hash);
        let pin = self.arena.pin();
        let handle = self.arena.allocate_boxed((key, value), 1, NEVER_EXPIRES);
        // SAFETY: the handle is this arena's own unpublished allocation.
        let key = &unsafe { entry(&pin, handle) }.value().0;
        let same_key = |current| {
            // SAFETY: published in this table while `pin` is held.
            unsafe { entry(&pin, current) }.value().0 == *key
        };
        let mut retired = Retired::new();
        {
            let mut writer = shard.writer.lock();
            if let Some((slot, _)) = shard.find(hash, same_key) {
                let previous = shard.bucket(slot).handles[slot % SLOTS]
                    .swap(handle.index_value().get(), Ordering::AcqRel);
                retired.push(handle_from_raw(previous));
            } else {
                while writer.len >= writer.capacity {
                    let Some(victim) = self.evict(shard, &mut writer, &pin) else {
                        break;
                    };
                    retired.push(victim);
                }
                let slot = shard.place(hash, handle);
                writer.len += 1;
                shard.len.store(writer.len, Ordering::Relaxed);
                if !writer.ghost.contains(hash) {
                    if writer.probation.len() >= writer.probation_limit {
                        writer.probation.pop_front();
                    }
                    writer.probation.push_back((
                        handle.index_value().get(),
                        // `Shard::new` bounds every shard below 2^32 slots.
                        u32::try_from(slot).unwrap_or(u32::MAX),
                        low_hash_bits(hash),
                    ));
                }
            }
        }
        retired.retire(&pin);
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
            let mut writer = shard.writer.lock();
            let found = shard.find(hash, |handle| {
                // SAFETY: published in this table while `pin` is held.
                unsafe { entry(&pin, handle) }.value().0.borrow() == key
            });
            found.map(|(slot, _)| {
                let removed = shard.clear(slot, shard.home(hash));
                writer.len -= 1;
                shard.len.store(writer.len, Ordering::Relaxed);
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

    /// Removes one entry from a full shard: an unread probationary key
    /// first, otherwise the next unread key under the CLOCK hand.
    #[allow(
        unsafe_code,
        reason = "dereferences handles published in this shard while `pin` is held"
    )]
    fn evict(
        &self,
        shard: &Shard,
        writer: &mut ShardWriter,
        pin: &DirectArenaPin<'_, (K, V)>,
    ) -> Option<DirectHandle> {
        while writer.probation.len() > writer.probation_target {
            let (raw, slot, low_hash) = writer
                .probation
                .pop_front()
                .expect("probation is above its target");
            let slot = slot as usize;
            if shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed) != raw {
                // Removed or replaced since it was enrolled.
                continue;
            }
            let handle = handle_from_raw(raw);
            // SAFETY: the shard lock keeps this published handle in the table.
            if unsafe { entry(pin, handle) }.take_accessed() {
                // Read during probation: it stays, under the main sweep.
                continue;
            }
            let removed = shard.clear(slot, shard.home(u64::from(low_hash)));
            writer.ghost.remember(u64::from(low_hash));
            writer.len -= 1;
            shard.len.store(writer.len, Ordering::Relaxed);
            return Some(removed);
        }
        let slots = shard.buckets.len() * SLOTS;
        // Each pass takes one access mark from every entry, and entries hold
        // at most two, so three passes always find a victim.
        for _ in 0..3 * slots {
            let slot = writer.hand;
            writer.hand = if slot + 1 == slots { 0 } else { slot + 1 };
            let raw = shard.bucket(slot).handles[slot % SLOTS].load(Ordering::Relaxed);
            if raw == 0 {
                continue;
            }
            let handle = handle_from_raw(raw);
            // SAFETY: the shard lock keeps this published handle in the table.
            let entry = unsafe { entry(pin, handle) };
            if entry.take_accessed() {
                continue;
            }
            let hash = self.hash_builder.hash_one(&entry.value().0);
            let removed = shard.clear(slot, shard.home(hash));
            writer.len -= 1;
            shard.len.store(writer.len, Ordering::Relaxed);
            return Some(removed);
        }
        None
    }
}

/// A pinned read session over a [`ShardedCache`]; see [`ShardedCache::pin`].
pub struct ShardedCacheGuard<'cache, K, V, S = DefaultHashBuilder> {
    cache: &'cache ShardedCache<K, V, S>,
    pin: DirectArenaPin<'cache, (K, V)>,
}

impl<K: Hash + Eq, V, S: BuildHasher> ShardedCacheGuard<'_, K, V, S> {
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

impl<K, V, S> Drop for ShardedCache<K, V, S> {
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
    fn new(capacity: usize, probation_bps: u16) -> Self {
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
                })
                .collect(),
            len: AtomicUsize::new(0),
            writer: Mutex::new(ShardWriter {
                len: 0,
                capacity,
                hand: 0,
                probation: VecDeque::new(),
                probation_target,
                probation_limit: probation_target + probation_target / 4 + 4,
                ghost: Ghost::new(capacity.div_ceil(GHOST_GENERATION_DIVISOR)),
            }),
        }
    }

    fn bucket(&self, slot: usize) -> &Bucket {
        &self.buckets[slot / SLOTS]
    }

    fn home(&self, hash: u64) -> usize {
        // Multiply-shift range reduction on the low 32 bits: the table need
        // not be a power of two, and probation records keep these bits.
        let scaled = ((hash & 0xffff_ffff) * self.buckets.len() as u64) >> 32;
        usize::try_from(scaled).expect("a bucket index fits usize")
    }

    fn next(&self, bucket: usize) -> usize {
        if bucket + 1 == self.buckets.len() {
            0
        } else {
            bucket + 1
        }
    }

    /// Finds the slot whose handle `matches`, scanning from the key's home
    /// bucket while earlier keys probed past it. Lock-free.
    fn find(
        &self,
        hash: u64,
        mut matches: impl FnMut(DirectHandle) -> bool,
    ) -> Option<(usize, DirectHandle)> {
        let tag = tag(hash);
        let mut bucket = self.home(hash);
        for _ in 0..self.buckets.len() {
            let current = &self.buckets[bucket];
            let control = current.control.load(Ordering::Acquire);
            let mut candidates = zero_tag_bytes(control ^ (BYTE_ONES * u64::from(tag)));
            while candidates != 0 {
                let slot = candidates.trailing_zeros() as usize / 8;
                candidates &= candidates - 1;
                // Acquire pairs with the release store of a replacement
                // handle, which does not touch the control word.
                let raw = current.handles[slot].load(Ordering::Acquire);
                if raw != 0 {
                    let handle = handle_from_raw(raw);
                    if matches(handle) {
                        return Some((bucket * SLOTS + slot, handle));
                    }
                }
            }
            if control >> OVERFLOW_SHIFT & OVERFLOW_MAX == 0 {
                return None;
            }
            bucket = self.next(bucket);
        }
        None
    }

    /// Publishes `handle` in the first free slot from the key's home bucket,
    /// counting the overflow on every full bucket it passes first. Requires
    /// the shard lock and a free slot.
    fn place(&self, hash: u64, handle: DirectHandle) -> usize {
        let mut bucket = self.home(hash);
        loop {
            let current = &self.buckets[bucket];
            let control = current.control.load(Ordering::Relaxed);
            let empty = zero_tag_bytes(control);
            if empty != 0 {
                // The lowest flagged byte of `zero_tag_bytes` is always exact.
                let slot = empty.trailing_zeros() as usize / 8;
                current.handles[slot].store(handle.index_value().get(), Ordering::Release);
                current.control.store(
                    control | u64::from(tag(hash)) << (slot * 8),
                    Ordering::Release,
                );
                return bucket * SLOTS + slot;
            }
            let overflow = control >> OVERFLOW_SHIFT & OVERFLOW_MAX;
            if overflow < OVERFLOW_MAX {
                // A saturated count stays saturated: lookups past it are
                // only slower, never wrong.
                current
                    .control
                    .store(control + (1 << OVERFLOW_SHIFT), Ordering::Release);
            }
            bucket = self.next(bucket);
        }
    }

    /// Unpublishes `slot`, whose key's home bucket is `home`, and releases
    /// the overflow counts its placement added. Returns the handle swapped
    /// out, which a lock-free replacement may have changed since the caller
    /// read it. Requires the shard lock.
    fn clear(&self, slot: usize, home: usize) -> DirectHandle {
        let bucket = slot / SLOTS;
        let current = &self.buckets[bucket];
        let control = current.control.load(Ordering::Relaxed);
        current.control.store(
            control & !(TAG_MASK << ((slot % SLOTS) * 8)),
            Ordering::Release,
        );
        let removed = handle_from_raw(current.handles[slot % SLOTS].swap(0, Ordering::AcqRel));
        let mut passed = home;
        while passed != bucket {
            let passed_bucket = &self.buckets[passed];
            let control = passed_bucket.control.load(Ordering::Relaxed);
            let overflow = control >> OVERFLOW_SHIFT & OVERFLOW_MAX;
            debug_assert_ne!(
                overflow, 0,
                "a displaced key counted every bucket it passed"
            );
            if overflow < OVERFLOW_MAX {
                passed_bucket
                    .control
                    .store(control - (1 << OVERFLOW_SHIFT), Ordering::Release);
            }
            passed = self.next(passed);
        }
        removed
    }
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

/// The low 32 hash bits, which choose the home bucket and ghost position.
fn low_hash_bits(hash: u64) -> u32 {
    let bytes = hash.to_le_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
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
    fn colliding_keys_chain_through_overflow_and_unlink_cleanly() {
        let cache = ShardedCache::with_hasher(40, Colliding);
        for key in 0..40_u64 {
            cache.insert(key, key);
        }
        for key in 0..40_u64 {
            assert_eq!(cache.get(&key), Some(key));
        }
        for key in (0..40_u64).step_by(2) {
            assert!(cache.remove(&key));
        }
        for key in 0..40_u64 {
            assert_eq!(cache.get(&key), (key % 2 == 1).then_some(key));
        }
        // Freed slots are reused without disturbing the displaced survivors.
        for key in 100..120_u64 {
            cache.insert(key, key);
        }
        for key in (1..40_u64).step_by(2).chain(100..120) {
            assert_eq!(cache.get(&key), Some(key));
        }
        let shard = &cache.shards[0];
        let overflow: u64 = shard
            .buckets
            .iter()
            .map(|bucket| bucket.control.load(Ordering::Relaxed) >> OVERFLOW_SHIFT & OVERFLOW_MAX)
            .sum();
        for key in (1..40_u64).step_by(2).chain(100..120) {
            assert!(cache.remove(&key));
        }
        assert!(cache.is_empty());
        assert!(overflow > 0, "the fixture must displace keys");
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
                                _ => cache.insert(key, Tracked::new(key)),
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
                            0 => cache.insert(key, (key, round)),
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
