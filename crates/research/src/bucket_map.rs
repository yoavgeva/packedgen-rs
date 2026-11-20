use core::fmt;
use std::hash::{BuildHasher, Hash};
use std::mem;

use opthash::DefaultHashBuilder;
use wide::u8x16;

use crate::{ArenaError, CapacityError, InsertOutcome, PackedKeyArena, PackedKeyRef};

const BUCKET_SLOTS: usize = 12;
const LIVE_ENTRIES_PER_BUCKET: usize = 11;
const MAX_RELOCATIONS: usize = 64;

/// Fully dynamic binary-key map using two cache-line-sized route buckets.
///
/// Buckets retain only fingerprints and `u32` indexes. Keys and values live in
/// a dense entry array, while original key bytes share a packed arena. Every
/// candidate is verified against those bytes, so fingerprints affect speed but
/// never exact semantics.
pub struct BucketPackedMap<V> {
    buckets: Box<[Bucket]>,
    entries: Vec<Entry<V>>,
    overflow: Vec<OverflowRoute>,
    arena: PackedKeyArena,
    hash_builder: DefaultHashBuilder,
    live_limit: usize,
    live_key_bytes: usize,
    relocations: usize,
}

impl<V> BucketPackedMap<V> {
    /// Constructs a fixed-capacity dynamic map.
    ///
    /// # Panics
    ///
    /// Panics when capacity is zero, exceeds the `u32` route-index domain, or
    /// initial allocation fails. Use [`Self::try_new`] for a typed failure.
    #[must_use]
    pub fn new(live_capacity: usize) -> Self {
        Self::try_new(live_capacity)
            .unwrap_or_else(|error| panic!("bucket map construction failed: {error}"))
    }

    /// Fallibly constructs a fixed-capacity dynamic map.
    ///
    /// # Errors
    ///
    /// Returns [`BucketBuildError::InvalidCapacity`] for zero or more than
    /// `u32::MAX` entries and [`BucketBuildError::AllocationFailed`] when the
    /// bucket or dense-entry reservation fails.
    pub fn try_new(live_capacity: usize) -> Result<Self, BucketBuildError> {
        if live_capacity == 0 || u32::try_from(live_capacity).is_err() {
            return Err(BucketBuildError::InvalidCapacity(live_capacity));
        }
        let bucket_count = live_capacity.div_ceil(LIVE_ENTRIES_PER_BUCKET);
        let mut buckets = Vec::new();
        buckets
            .try_reserve_exact(bucket_count)
            .map_err(|_| BucketBuildError::AllocationFailed)?;
        buckets.resize(bucket_count, Bucket::EMPTY);
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(live_capacity)
            .map_err(|_| BucketBuildError::AllocationFailed)?;
        Ok(Self {
            buckets: buckets.into_boxed_slice(),
            entries,
            overflow: Vec::new(),
            arena: PackedKeyArena::new(),
            hash_builder: DefaultHashBuilder::default(),
            live_limit: live_capacity,
            live_key_bytes: 0,
            relocations: 0,
        })
    }

    /// Inserts a new key or replaces the value of an existing key.
    ///
    /// # Errors
    ///
    /// Returns a capacity, packed-key, or overflow-reservation failure without
    /// changing the key set.
    ///
    /// # Panics
    ///
    /// Panics only if an internal live route no longer resolves to its dense
    /// entry, which indicates map invariant corruption.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, BucketMapError> {
        let hash = self.hash_key(key);
        if let Some(location) = self.locate(key, hash) {
            let entry = self
                .entries
                .get_mut(location.entry_index())
                .expect("live route index must resolve");
            return Ok(InsertOutcome::Replaced(mem::replace(
                &mut entry.value,
                value,
            )));
        }
        if self.entries.len() >= self.live_limit {
            return Err(BucketMapError::Capacity(CapacityError::new(
                self.live_limit,
            )));
        }

        let [first, second] = self.bucket_pair(hash);
        if self.buckets[first].is_full() && self.buckets[second].is_full() {
            self.overflow
                .try_reserve(1)
                .map_err(|_| BucketMapError::AllocationFailed)?;
        }
        let key_ref = self.arena.insert(key).map_err(BucketMapError::Arena)?;
        let entry_index = u32::try_from(self.entries.len()).expect("capacity fits u32");
        self.entries.push(Entry {
            key: key_ref,
            value,
        });
        self.live_key_bytes += key.len();
        self.place_route(Route {
            tag: fingerprint(hash),
            entry_index,
            hash,
        });
        Ok(InsertOutcome::Inserted)
    }

    /// Returns the value for `key` after exact original-byte verification.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let location = self.locate(key, self.hash_key(key))?;
        self.entries
            .get(location.entry_index())
            .map(|entry| &entry.value)
    }

    /// Returns a mutable value for `key`.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let location = self.locate(key, self.hash_key(key))?;
        self.entries
            .get_mut(location.entry_index())
            .map(|entry| &mut entry.value)
    }

    /// Returns whether `key` is live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Removes `key` and returns its value.
    ///
    /// The last dense entry moves into the removed slot and its route is
    /// repaired. Removed key bytes remain in the append-only arena.
    ///
    /// # Panics
    ///
    /// Panics only if an internal dense-entry route invariant is corrupted.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let location = self.locate(key, self.hash_key(key))?;
        let removed_index = location.entry_index();
        let previous_last = self.entries.len() - 1;
        self.remove_route(location);
        let removed = self.entries.swap_remove(removed_index);
        self.live_key_bytes = self.live_key_bytes.saturating_sub(removed.key.len());
        if removed_index != previous_last {
            self.repoint_route(
                u32::try_from(previous_last).expect("entry index fits u32"),
                u32::try_from(removed_index).expect("entry index fits u32"),
            );
        }
        Some(removed.value)
    }

    /// Removes all keys and releases packed key segments.
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = Bucket::EMPTY;
        }
        self.entries.clear();
        self.overflow.clear();
        self.arena.clear();
        self.live_key_bytes = 0;
        self.relocations = 0;
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no entries are live.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Captures dense-entry, bucket, overflow, and packed-key memory state.
    #[must_use]
    pub fn stats(&self) -> BucketMapStats {
        BucketMapStats {
            len: self.entries.len(),
            live_limit: self.live_limit,
            buckets: self.buckets.len(),
            bucket_bytes: size_of_val(self.buckets.as_ref()),
            entry_bytes: self.entries.capacity() * size_of::<Entry<V>>(),
            overflow_routes: self.overflow.len(),
            overflow_bytes: self.overflow.capacity() * size_of::<OverflowRoute>(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            live_key_bytes: self.live_key_bytes,
            relocations: self.relocations,
        }
    }

    #[inline]
    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hash_builder.hash_one(key)
    }

    #[inline]
    fn locate(&self, key: &[u8], hash: u64) -> Option<RouteLocation> {
        let tag = fingerprint(hash);
        let first = self.primary_bucket(hash);
        if let Some(location) = self.locate_in_bucket(first, tag, key) {
            return Some(location);
        }
        if self.buckets.len() > 1 {
            let second = self.secondary_bucket(hash, first);
            if let Some(location) = self.locate_in_bucket(second, tag, key) {
                return Some(location);
            }
        }
        self.overflow
            .iter()
            .enumerate()
            .find(|(_, route)| route.tag == tag && self.entry_matches(route.entry_index, key))
            .map(|(slot, route)| RouteLocation::Overflow {
                slot,
                entry_index: route.entry_index,
            })
    }

    #[inline]
    fn locate_in_bucket(&self, bucket: usize, tag: u8, key: &[u8]) -> Option<RouteLocation> {
        let mut matches = self.buckets[bucket].matching_mask(tag);
        while matches != 0 {
            let slot = matches.trailing_zeros() as usize;
            matches &= matches - 1;
            let entry_index = self.buckets[bucket].indices[slot];
            if self.entry_matches(entry_index, key) {
                return Some(RouteLocation::Bucket {
                    bucket,
                    slot,
                    entry_index,
                });
            }
        }
        None
    }

    #[inline]
    fn entry_matches(&self, entry_index: u32, key: &[u8]) -> bool {
        self.entries
            .get(entry_index as usize)
            .and_then(|entry| self.arena.get(entry.key))
            == Some(key)
    }

    fn place_route(&mut self, mut route: Route) {
        let [first, second] = self.bucket_pair(route.hash);
        if self.buckets[first].try_insert(route.tag, route.entry_index) {
            return;
        }
        if second != first && self.buckets[second].try_insert(route.tag, route.entry_index) {
            return;
        }

        let mut current = if mix(route.hash) & 1 == 0 {
            first
        } else {
            second
        };
        for step in 0..MAX_RELOCATIONS {
            let victim = usize::try_from(
                mix(route.hash ^ u64::try_from(step).expect("step fits u64"))
                    % u64::try_from(BUCKET_SLOTS).expect("bucket size fits u64"),
            )
            .expect("victim slot fits usize");
            let displaced_index = mem::replace(
                &mut self.buckets[current].indices[victim],
                route.entry_index,
            );
            let displaced_tag = self.buckets[current].replace_tag(victim, route.tag);
            self.relocations += 1;

            let displaced_hash = self.hash_entry(displaced_index);
            route = Route {
                tag: displaced_tag,
                entry_index: displaced_index,
                hash: displaced_hash,
            };
            let [left, right] = self.bucket_pair(displaced_hash);
            current = if current == left { right } else { left };
            if self.buckets[current].try_insert(route.tag, route.entry_index) {
                return;
            }
        }
        self.overflow.push(OverflowRoute {
            tag: route.tag,
            entry_index: route.entry_index,
        });
    }

    fn hash_entry(&self, entry_index: u32) -> u64 {
        let entry = self
            .entries
            .get(entry_index as usize)
            .expect("relocated entry index must resolve");
        let key = self
            .arena
            .get(entry.key)
            .expect("relocated packed key must resolve");
        self.hash_key(key)
    }

    fn remove_route(&mut self, location: RouteLocation) {
        match location {
            RouteLocation::Bucket { bucket, slot, .. } => {
                self.buckets[bucket].replace_tag(slot, 0);
                self.buckets[bucket].indices[slot] = 0;
            }
            RouteLocation::Overflow { slot, .. } => {
                self.overflow.swap_remove(slot);
            }
        }
    }

    fn repoint_route(&mut self, previous: u32, replacement: u32) {
        let hash = self.hash_entry(replacement);
        let [first, second] = self.bucket_pair(hash);
        for bucket_index in [first, second] {
            if let Some(index) = self.buckets[bucket_index]
                .indices
                .iter_mut()
                .find(|index| **index == previous)
            {
                *index = replacement;
                return;
            }
            if first == second {
                break;
            }
        }
        if let Some(route) = self
            .overflow
            .iter_mut()
            .find(|route| route.entry_index == previous)
        {
            route.entry_index = replacement;
            return;
        }
        panic!("moved dense entry must retain exactly one route");
    }

    fn bucket_pair(&self, hash: u64) -> [usize; 2] {
        let first = self.primary_bucket(hash);
        if self.buckets.len() == 1 {
            return [first, first];
        }
        [first, self.secondary_bucket(hash, first)]
    }

    #[inline]
    fn primary_bucket(&self, hash: u64) -> usize {
        reduce(hash, self.buckets.len())
    }

    #[inline]
    fn secondary_bucket(&self, hash: u64, first: usize) -> usize {
        let mut second = reduce(mix(hash), self.buckets.len() - 1);
        if second >= first {
            second += 1;
        }
        second
    }
}

struct Entry<V> {
    key: PackedKeyRef,
    value: V,
}

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Bucket {
    tags: [u8; 16],
    indices: [u32; BUCKET_SLOTS],
}

impl Bucket {
    const EMPTY: Self = Self {
        tags: [0; 16],
        indices: [0; BUCKET_SLOTS],
    };

    fn is_full(&self) -> bool {
        self.empty_slot().is_none()
    }

    fn try_insert(&mut self, tag: u8, entry_index: u32) -> bool {
        let Some(slot) = self.empty_slot() else {
            return false;
        };
        self.indices[slot] = entry_index;
        self.replace_tag(slot, tag);
        true
    }

    #[inline]
    fn matching_mask(&self, tag: u8) -> u16 {
        u16::try_from(u8x16::new(self.tags).cmp_eq(u8x16::splat(tag)).move_mask())
            .expect("16-lane mask fits u16")
            & 0x0fff
    }

    fn empty_slot(&self) -> Option<usize> {
        let mask = u16::try_from(u8x16::new(self.tags).cmp_eq(u8x16::splat(0)).move_mask())
            .expect("16-lane mask fits u16")
            & 0x0fff;
        (mask != 0).then(|| mask.trailing_zeros() as usize)
    }

    fn replace_tag(&mut self, slot: usize, replacement: u8) -> u8 {
        mem::replace(&mut self.tags[slot], replacement)
    }
}

#[derive(Clone, Copy)]
struct Route {
    tag: u8,
    entry_index: u32,
    hash: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct OverflowRoute {
    entry_index: u32,
    tag: u8,
}

#[derive(Clone, Copy)]
enum RouteLocation {
    Bucket {
        bucket: usize,
        slot: usize,
        entry_index: u32,
    },
    Overflow {
        slot: usize,
        entry_index: u32,
    },
}

impl RouteLocation {
    #[inline]
    fn entry_index(self) -> usize {
        match self {
            Self::Bucket { entry_index, .. } | Self::Overflow { entry_index, .. } => {
                entry_index as usize
            }
        }
    }
}

#[inline]
fn fingerprint(hash: u64) -> u8 {
    let candidate = (hash >> 56) as u8;
    candidate.max(1)
}

#[inline]
fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize fits u128");
    usize::try_from((u128::from(hash) * upper) >> 64).expect("reduced hash is below upper")
}

#[inline]
fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Observable retained state for a [`BucketPackedMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketMapStats {
    /// Live entries.
    pub len: usize,
    /// Fixed live-entry limit.
    pub live_limit: usize,
    /// Cache-line route buckets.
    pub buckets: usize,
    /// Requested route-bucket bytes.
    pub bucket_bytes: usize,
    /// Requested dense-entry capacity bytes.
    pub entry_bytes: usize,
    /// Routes that exhausted bounded two-bucket relocation.
    pub overflow_routes: usize,
    /// Requested overflow-vector capacity bytes.
    pub overflow_bytes: usize,
    /// Requested packed-key arena capacity bytes.
    pub arena_allocated_bytes: usize,
    /// Appended key bytes, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to live entries.
    pub live_key_bytes: usize,
    /// Cumulative entry routes moved during insertion.
    pub relocations: usize,
}

impl BucketMapStats {
    /// Requested bytes retained by the route and dense-entry indexes.
    #[must_use]
    pub const fn index_bytes(self) -> usize {
        self.bucket_bytes
            .saturating_add(self.entry_bytes)
            .saturating_add(self.overflow_bytes)
    }

    /// Bytes retained for removed keys until arena compaction is added.
    #[must_use]
    pub const fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}

/// Failure while constructing a dynamic bucket map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketBuildError {
    /// Capacity must be in `1..=u32::MAX`.
    InvalidCapacity(usize),
    /// Initial bucket or dense-entry allocation failed.
    AllocationFailed,
}

impl fmt::Display for BucketBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity(capacity) => {
                write!(
                    formatter,
                    "bucket map capacity must fit u32 and be positive: {capacity}"
                )
            }
            Self::AllocationFailed => formatter.write_str("bucket map allocation failed"),
        }
    }
}

impl std::error::Error for BucketBuildError {}

/// Failure while inserting into a dynamic bucket map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketMapError {
    /// The fixed live-entry limit was reached.
    Capacity(CapacityError),
    /// Key packing or arena allocation failed.
    Arena(ArenaError),
    /// Overflow-route storage could not be reserved before relocation.
    AllocationFailed,
}

impl fmt::Display for BucketMapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity(error) => error.fmt(formatter),
            Self::Arena(error) => error.fmt(formatter),
            Self::AllocationFailed => formatter.write_str("bucket overflow allocation failed"),
        }
    }
}

impl std::error::Error for BucketMapError {}

#[cfg(test)]
mod tests {
    use super::{Bucket, OverflowRoute};

    #[test]
    fn route_bucket_is_exactly_one_cache_line() {
        assert_eq!(size_of::<Bucket>(), 64);
        assert_eq!(align_of::<Bucket>(), 64);
        assert_eq!(size_of::<OverflowRoute>(), 8);
    }
}
