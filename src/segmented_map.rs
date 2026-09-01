use std::hash::{BuildHasher, Hash};
use std::mem;

use hashbrown::{DefaultHashBuilder, HashTable};

use crate::{ArenaError, InsertOutcome, PackedKeyArena, PackedKeyRef};

const PLANNED_LOAD_DENOMINATOR: usize = 100;
const MIN_SWISS_BUCKETS: usize = 16;

/// Packed-key map split across power-of-two `SwissTable` segments.
///
/// A normal Swiss table rounds its whole allocation to one power of two. This
/// map decomposes the required bucket count into multiple power-of-two tables,
/// avoiding a global two-times allocation cliff while retaining Swiss probing
/// inside each segment. Original keys share one packed arena and every match is
/// verified against the exact bytes.
pub struct SegmentedSwissMap<V, S = DefaultHashBuilder> {
    segments: Vec<Segment<Entry<V>>>,
    arena: PackedKeyArena,
    hash_builder: S,
    route_buckets: usize,
    len: usize,
    live_key_bytes: usize,
}

impl<V> SegmentedSwissMap<V> {
    /// Creates an empty map without allocating table entries.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a segmented map for an expected live-entry count.
    #[must_use]
    pub fn with_capacity(entry_capacity: usize) -> Self {
        Self::with_capacity_and_load(entry_capacity, SegmentedLoad::Balanced)
    }

    /// Creates a segmented map using an explicit RAM/latency policy.
    #[must_use]
    pub fn with_capacity_and_load(entry_capacity: usize, load: SegmentedLoad) -> Self {
        let route_buckets = planned_route_buckets(entry_capacity, load);
        Self {
            segments: make_segments(entry_capacity, route_buckets),
            arena: PackedKeyArena::new(),
            hash_builder: DefaultHashBuilder::default(),
            route_buckets,
            len: 0,
            live_key_bytes: 0,
        }
    }

    /// Creates a segmented map with a contiguous initial packed-key arena.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] when `key_byte_capacity`
    /// cannot fit the arena's packed 32-bit segment-offset representation.
    pub fn try_with_capacity_and_key_bytes(
        entry_capacity: usize,
        key_byte_capacity: usize,
    ) -> Result<Self, ArenaError> {
        Self::try_with_capacity_key_bytes_and_load(
            entry_capacity,
            key_byte_capacity,
            SegmentedLoad::Balanced,
        )
    }

    /// Creates a segmented map with explicit load policy and key-byte budget.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] when `key_byte_capacity`
    /// cannot fit the arena's packed 32-bit segment-offset representation.
    pub fn try_with_capacity_key_bytes_and_load(
        entry_capacity: usize,
        key_byte_capacity: usize,
        load: SegmentedLoad,
    ) -> Result<Self, ArenaError> {
        let route_buckets = planned_route_buckets(entry_capacity, load);
        Ok(Self {
            segments: make_segments(entry_capacity, route_buckets),
            arena: PackedKeyArena::with_segment_bytes(key_byte_capacity.max(1))?,
            hash_builder: DefaultHashBuilder::default(),
            route_buckets,
            len: 0,
            live_key_bytes: 0,
        })
    }
}

impl<V, S> SegmentedSwissMap<V, S>
where
    S: BuildHasher,
{
    pub(crate) fn try_with_capacity_key_bytes_load_and_hasher(
        entry_capacity: usize,
        key_segment_bytes: usize,
        load: SegmentedLoad,
        hash_builder: S,
    ) -> Result<Self, ArenaError> {
        let route_buckets = planned_route_buckets(entry_capacity, load);
        Ok(Self {
            segments: make_segments(entry_capacity, route_buckets),
            arena: PackedKeyArena::with_segment_bytes(key_segment_bytes.max(1))?,
            hash_builder,
            route_buckets,
            len: 0,
            live_key_bytes: 0,
        })
    }

    /// Inserts `key`, returning the previous value when it already existed.
    ///
    /// # Errors
    ///
    /// Returns an arena error when the key is too long or packed-key storage
    /// cannot be allocated.
    ///
    /// # Panics
    ///
    /// Panics only if a live entry no longer resolves in its owning arena,
    /// which indicates internal invariant corruption.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, ArenaError> {
        let hash = self.hash_key(key);
        self.try_insert_prehashed(hash, key, value)
    }

    pub(crate) fn try_insert_prehashed(
        &mut self,
        hash: u64,
        key: &[u8],
        value: V,
    ) -> Result<InsertOutcome<V>, ArenaError> {
        let segment_index = self.segment_index(hash);
        let arena = &self.arena;
        if let Some(entry) = self.segments[segment_index]
            .table
            .find_mut(hash, |entry| arena.get(entry.key) == Some(key))
        {
            return Ok(InsertOutcome::Replaced(mem::replace(
                &mut entry.value,
                value,
            )));
        }

        let key_ref = self.arena.insert(key)?;
        self.live_key_bytes += key.len();
        let arena = &self.arena;
        let hash_builder = &self.hash_builder;
        self.segments[segment_index].table.insert_unique(
            hash,
            Entry {
                key: key_ref,
                value,
            },
            |entry| {
                let bytes = arena
                    .get(entry.key)
                    .expect("live segmented entry must resolve in its arena");
                hash_builder.hash_one(bytes)
            },
        );
        self.len += 1;
        Ok(InsertOutcome::Inserted)
    }

    /// Returns the value for `key` after exact original-byte verification.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = self.hash_key(key);
        self.get_prehashed(hash, key)
    }

    #[inline]
    pub(crate) fn get_prehashed(&self, hash: u64, key: &[u8]) -> Option<&V> {
        let segment = &self.segments[self.segment_index(hash)];
        segment
            .table
            .find(hash, |entry| self.arena.get(entry.key) == Some(key))
            .map(|entry| &entry.value)
    }

    /// Returns a mutable value for `key`.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let hash = self.hash_key(key);
        self.get_mut_prehashed(hash, key)
    }

    pub(crate) fn get_mut_prehashed(&mut self, hash: u64, key: &[u8]) -> Option<&mut V> {
        let segment_index = self.segment_index(hash);
        let arena = &self.arena;
        self.segments[segment_index]
            .table
            .find_mut(hash, |entry| arena.get(entry.key) == Some(key))
            .map(|entry| &mut entry.value)
    }

    /// Returns whether `key` is present.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Removes `key` and returns its value.
    ///
    /// Removed bytes remain in the append-only arena until [`Self::clear`].
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let hash = self.hash_key(key);
        self.remove_prehashed(hash, key)
    }

    pub(crate) fn remove_prehashed(&mut self, hash: u64, key: &[u8]) -> Option<V> {
        let segment_index = self.segment_index(hash);
        let arena = &self.arena;
        let occupied = self.segments[segment_index]
            .table
            .find_entry(hash, |entry| arena.get(entry.key) == Some(key))
            .ok()?;
        let (entry, _) = occupied.remove();
        self.len -= 1;
        self.live_key_bytes = self.live_key_bytes.saturating_sub(entry.key.len());
        Some(entry.value)
    }

    /// Removes all entries and releases packed-key segments.
    pub fn clear(&mut self) {
        for segment in &mut self.segments {
            segment.table.clear();
        }
        self.arena.clear();
        self.len = 0;
        self.live_key_bytes = 0;
    }

    /// Rewrites live keys into a fresh arena without rebuilding Swiss tables.
    ///
    /// This reclaims bytes retained by prior deletions while preserving table
    /// capacity and every value. Peak memory temporarily includes both the old
    /// and new arenas plus one packed reference per live entry.
    ///
    /// # Errors
    ///
    /// Returns an arena error without changing the map if staging allocation
    /// or key copying fails.
    ///
    /// # Panics
    ///
    /// Panics only if a live table entry no longer resolves in its arena,
    /// which indicates internal invariant corruption.
    pub fn compact_keys(&mut self) -> Result<KeyCompactionStats, ArenaError> {
        let discarded_key_bytes = self.arena.key_bytes().saturating_sub(self.live_key_bytes);
        if discarded_key_bytes == 0 && self.arena.len() == self.len {
            return Ok(KeyCompactionStats {
                live_entries: self.len,
                copied_key_bytes: 0,
                discarded_key_bytes: 0,
                reclaimed_allocated_bytes: 0,
            });
        }

        let before_allocated_bytes = self.arena.allocated_bytes();
        let mut compacted = self.arena.empty_like();
        let mut replacement_refs = Vec::new();
        replacement_refs
            .try_reserve_exact(self.len)
            .map_err(|_| ArenaError::AllocationFailed)?;
        for segment in &self.segments {
            for entry in &segment.table {
                let key = self
                    .arena
                    .get(entry.key)
                    .expect("live segmented entry must resolve during compaction");
                replacement_refs.push(compacted.insert(key)?);
            }
        }
        debug_assert_eq!(replacement_refs.len(), self.len);

        for (entry, replacement) in self
            .segments
            .iter_mut()
            .flat_map(|segment| segment.table.iter_mut())
            .zip(replacement_refs)
        {
            entry.key = replacement;
        }
        let after_allocated_bytes = compacted.allocated_bytes();
        self.arena = compacted;

        Ok(KeyCompactionStats {
            live_entries: self.len,
            copied_key_bytes: self.live_key_bytes,
            discarded_key_bytes,
            reclaimed_allocated_bytes: before_allocated_bytes.saturating_sub(after_allocated_bytes),
        })
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the map has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.segments.iter().all(|segment| segment.table.is_empty())
    }

    /// Captures segment and packed-arena state.
    #[must_use]
    pub fn stats(&self) -> SegmentedMapStats {
        SegmentedMapStats {
            len: self.len(),
            segments: self.segments.len(),
            table_capacity: self
                .segments
                .iter()
                .map(|segment| segment.table.capacity())
                .sum(),
            route_buckets: self.route_buckets,
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            live_key_bytes: self.live_key_bytes,
        }
    }

    #[inline]
    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hash_builder.hash_one(key)
    }

    #[inline]
    fn segment_index(&self, hash: u64) -> usize {
        if self.segments.len() == 1 {
            return 0;
        }
        let route = reduce(hash, self.route_buckets);
        self.segments
            .iter()
            .position(|segment| route < segment.route_end)
            .unwrap_or(self.segments.len() - 1)
    }
}

impl<V> Default for SegmentedSwissMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

struct Entry<V> {
    key: PackedKeyRef,
    value: V,
}

struct Segment<T> {
    table: HashTable<T>,
    route_end: usize,
}

fn make_segments<V>(entry_capacity: usize, route_buckets: usize) -> Vec<Segment<Entry<V>>> {
    if route_buckets < MIN_SWISS_BUCKETS {
        return vec![Segment {
            table: HashTable::with_capacity(entry_capacity),
            route_end: route_buckets,
        }];
    }

    let mut segments = Vec::new();
    let mut remaining = route_buckets;
    let mut route_end = 0_usize;
    while remaining != 0 {
        let buckets = highest_power_of_two(remaining);
        debug_assert!(buckets >= MIN_SWISS_BUCKETS);
        let capacity = buckets - buckets / 8;
        route_end += buckets;
        segments.push(Segment {
            table: HashTable::with_capacity(capacity),
            route_end,
        });
        remaining -= buckets;
    }
    segments
}

fn planned_route_buckets(entry_capacity: usize, load: SegmentedLoad) -> usize {
    if entry_capacity == 0 {
        return 1;
    }
    let planned_capacity = entry_capacity
        .saturating_mul(PLANNED_LOAD_DENOMINATOR)
        .div_ceil(load.numerator());
    let buckets = planned_capacity.saturating_mul(8).div_ceil(7);
    buckets.div_ceil(MIN_SWISS_BUCKETS) * MIN_SWISS_BUCKETS
}

/// Planned segment occupancy, trading route bytes against absent-key latency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SegmentedLoad {
    /// About 97% of advertised table capacity: lowest RAM, slower misses.
    Compact,
    /// About 90% of advertised table capacity: default RAM/latency compromise.
    #[default]
    Balanced,
    /// About 75% of advertised table capacity: faster probes, more route bytes.
    Fast,
}

impl SegmentedLoad {
    const fn numerator(self) -> usize {
        match self {
            Self::Compact => 97,
            Self::Balanced => 90,
            Self::Fast => 75,
        }
    }
}

fn highest_power_of_two(value: usize) -> usize {
    1_usize << (usize::BITS - 1 - value.leading_zeros())
}

#[inline]
fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize fits u128");
    usize::try_from((u128::from(hash) * upper) >> 64).expect("reduced hash is below upper")
}

/// Observable retained state for a [`SegmentedSwissMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentedMapStats {
    /// Live entries.
    pub len: usize,
    /// Number of independently allocated Swiss segments.
    pub segments: usize,
    /// Sum of entry capacities before any segment grows again.
    pub table_capacity: usize,
    /// Sum of planned Swiss bucket counts used for routing.
    pub route_buckets: usize,
    /// Requested packed-arena allocation bytes.
    pub arena_allocated_bytes: usize,
    /// Appended key bytes, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to live entries.
    pub live_key_bytes: usize,
}

impl SegmentedMapStats {
    /// Bytes retained for removed keys until the map is cleared.
    #[must_use]
    pub const fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}

/// Result of rewriting live packed keys into a fresh arena.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KeyCompactionStats {
    /// Entries whose packed references remain valid after the rewrite.
    pub live_entries: usize,
    /// Live key bytes copied; zero when no compaction was needed.
    pub copied_key_bytes: usize,
    /// Dead logical key bytes discarded.
    pub discarded_key_bytes: usize,
    /// Requested arena bytes released after staging completed.
    pub reclaimed_allocated_bytes: usize,
}
