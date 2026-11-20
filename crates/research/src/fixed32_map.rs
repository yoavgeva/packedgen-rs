use std::fmt;
use std::hash::BuildHasher;
use std::mem::{self, size_of};

use hashbrown::{DefaultHashBuilder, HashTable};

use crate::InsertOutcome;

const PLANNED_LOAD_DENOMINATOR: usize = 100;
const MIN_SWISS_BUCKETS: usize = 16;

/// A compact map specialized for exactly 32-byte keys.
///
/// Keys and values live in separate dense vectors.  The Swiss tables contain
/// only a four-byte index into those vectors, rather than repeating a key
/// reference and value in every table bucket.  Splitting the index across
/// independently-sized hash ranges also avoids the global power-of-two
/// allocation cliff of one large Swiss table.
///
/// This specialization is useful when keys are stable identifiers such as
/// SHA-256 digests. Removal keeps the arrays dense by repairing the directory
/// index of the record moved by `swap_remove`.
pub struct Fixed32SoaMap<V, S = DefaultHashBuilder> {
    segments: Vec<Segment>,
    keys: Vec<[u8; 32]>,
    values: Vec<V>,
    hash_builder: S,
    route_buckets: usize,
}

impl<V> Fixed32SoaMap<V> {
    /// Creates an empty map without allocating entries.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a map for `entry_capacity` entries using the balanced policy.
    #[must_use]
    pub fn with_capacity(entry_capacity: usize) -> Self {
        Self::with_capacity_and_load(entry_capacity, Fixed32Load::Balanced)
    }

    /// Creates a map with an explicit directory occupancy policy.
    #[must_use]
    pub fn with_capacity_and_load(entry_capacity: usize, load: Fixed32Load) -> Self {
        Self::with_capacity_load_and_hasher(entry_capacity, load, DefaultHashBuilder::default())
    }
}

impl<V, S> Fixed32SoaMap<V, S>
where
    S: BuildHasher,
{
    /// Creates a map with caller-selected occupancy and hash builder.
    #[must_use]
    pub fn with_capacity_load_and_hasher(
        entry_capacity: usize,
        load: Fixed32Load,
        hash_builder: S,
    ) -> Self {
        let route_buckets = planned_route_buckets(entry_capacity, load);
        Self {
            segments: make_segments(entry_capacity, route_buckets),
            keys: Vec::with_capacity(entry_capacity),
            values: Vec::with_capacity(entry_capacity),
            hash_builder,
            route_buckets,
        }
    }

    /// Inserts a key, returning the old value when the key already exists.
    ///
    /// # Errors
    ///
    /// Returns [`Fixed32CapacityError`] if the dense index would no longer fit
    /// in the four-byte directory representation.
    pub fn try_insert(
        &mut self,
        key: [u8; 32],
        value: V,
    ) -> Result<InsertOutcome<V>, Fixed32CapacityError> {
        let hash = self.hash_key(&key);
        let segment_index = self.segment_index(hash);
        let keys = &self.keys;
        if let Some(&dense_index) = self.segments[segment_index]
            .table
            .find(hash, |&dense_index| keys[dense_index as usize] == key)
        {
            return Ok(InsertOutcome::Replaced(mem::replace(
                &mut self.values[dense_index as usize],
                value,
            )));
        }

        let dense_index = u32::try_from(self.keys.len()).map_err(|_| Fixed32CapacityError)?;
        self.keys.push(key);
        self.values.push(value);

        let keys = &self.keys;
        let hash_builder = &self.hash_builder;
        self.segments[segment_index]
            .table
            .insert_unique(hash, dense_index, |&stored_index| {
                hash_builder.hash_one(keys[stored_index as usize].as_slice())
            });
        Ok(InsertOutcome::Inserted)
    }

    /// Returns the value associated with `key`.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8; 32]) -> Option<&V> {
        let hash = self.hash_key(key);
        let segment = &self.segments[self.segment_index(hash)];
        segment
            .table
            .find(hash, |&dense_index| self.keys[dense_index as usize] == *key)
            .map(|&dense_index| &self.values[dense_index as usize])
    }

    /// Returns a mutable value associated with `key`.
    pub fn get_mut(&mut self, key: &[u8; 32]) -> Option<&mut V> {
        let hash = self.hash_key(key);
        let segment_index = self.segment_index(hash);
        let keys = &self.keys;
        let dense_index = self.segments[segment_index]
            .table
            .find(hash, |&dense_index| keys[dense_index as usize] == *key)
            .copied()?;
        self.values.get_mut(dense_index as usize)
    }

    /// Returns whether `key` exists.
    #[must_use]
    pub fn contains_key(&self, key: &[u8; 32]) -> bool {
        self.get(key).is_some()
    }

    /// Removes `key` and returns its value.
    ///
    /// The last dense record moves into the removed record's position. Its
    /// four-byte directory index is repaired before this method returns.
    ///
    /// # Panics
    ///
    /// Panics only if an internal live directory route no longer resolves to
    /// its dense record, which indicates map invariant corruption.
    pub fn remove(&mut self, key: &[u8; 32]) -> Option<V> {
        let hash = self.hash_key(key);
        let segment_index = self.segment_index(hash);
        let keys = &self.keys;
        let occupied = self.segments[segment_index]
            .table
            .find_entry(hash, |&dense_index| keys[dense_index as usize] == *key)
            .ok()?;
        let (removed_dense_index, _) = occupied.remove();
        let removed_index = removed_dense_index as usize;
        let previous_last = self.keys.len() - 1;

        self.keys.swap_remove(removed_index);
        let removed_value = self.values.swap_remove(removed_index);

        if removed_index != previous_last {
            let moved_key = self.keys[removed_index];
            let moved_hash = self.hash_key(&moved_key);
            let moved_segment = self.segment_index(moved_hash);
            let old_index = u32::try_from(previous_last).expect("dense index was already u32");
            let route = self.segments[moved_segment]
                .table
                .find_mut(moved_hash, |&dense_index| dense_index == old_index)
                .expect("moved dense record must retain a directory route");
            *route = removed_dense_index;
        }

        Some(removed_value)
    }

    /// Removes every entry while retaining allocated capacity for reuse.
    pub fn clear(&mut self) {
        for segment in &mut self.segments {
            segment.table.clear();
        }
        self.keys.clear();
        self.values.clear();
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns whether the map has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Captures directory and dense-array state.
    #[must_use]
    pub fn stats(&self) -> Fixed32MapStats {
        let directory_capacity = self
            .segments
            .iter()
            .map(|segment| segment.table.capacity())
            .sum();
        let directory_estimated_bytes = self
            .segments
            .iter()
            .map(|segment| estimated_table_bytes(segment.table.capacity()))
            .sum();
        Fixed32MapStats {
            len: self.len(),
            segments: self.segments.len(),
            directory_capacity,
            route_buckets: self.route_buckets,
            key_capacity: self.keys.capacity(),
            value_capacity: self.values.capacity(),
            directory_estimated_bytes,
            dense_estimated_bytes: self
                .keys
                .capacity()
                .saturating_mul(size_of::<[u8; 32]>())
                .saturating_add(self.values.capacity().saturating_mul(size_of::<V>())),
        }
    }

    #[inline]
    fn hash_key(&self, key: &[u8; 32]) -> u64 {
        self.hash_builder.hash_one(key.as_slice())
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

impl<V> Default for Fixed32SoaMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

struct Segment {
    table: HashTable<u32>,
    route_end: usize,
}

fn make_segments(entry_capacity: usize, route_buckets: usize) -> Vec<Segment> {
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

fn planned_route_buckets(entry_capacity: usize, load: Fixed32Load) -> usize {
    if entry_capacity == 0 {
        return 1;
    }
    let planned_capacity = entry_capacity
        .saturating_mul(PLANNED_LOAD_DENOMINATOR)
        .div_ceil(load.numerator());
    let buckets = planned_capacity.saturating_mul(8).div_ceil(7);
    buckets.div_ceil(MIN_SWISS_BUCKETS) * MIN_SWISS_BUCKETS
}

/// Planned occupancy of the compact four-byte directory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Fixed32Load {
    /// About 97% of advertised capacity: least RAM and longest miss probes.
    Compact,
    /// About 90% of advertised capacity: default RAM/latency compromise.
    #[default]
    Balanced,
    /// About 75% of advertised capacity: shorter probes and a larger directory.
    Fast,
}

impl Fixed32Load {
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

fn estimated_table_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        capacity + 1
    } else {
        capacity.saturating_mul(8) / 7
    };
    buckets
        .saturating_mul(size_of::<u32>() + 1)
        .saturating_add(16)
}

/// Returned when a compact directory index would exceed four bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fixed32CapacityError;

impl fmt::Display for Fixed32CapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Fixed32SoaMap cannot index more than u32::MAX entries")
    }
}

impl std::error::Error for Fixed32CapacityError {}

/// Observable retained state for a [`Fixed32SoaMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fixed32MapStats {
    /// Live entries.
    pub len: usize,
    /// Number of independently allocated Swiss directory ranges.
    pub segments: usize,
    /// Sum of directory entry capacities.
    pub directory_capacity: usize,
    /// Planned hash-range buckets used for routing.
    pub route_buckets: usize,
    /// Allocated slots in the dense key vector.
    pub key_capacity: usize,
    /// Allocated slots in the dense value vector.
    pub value_capacity: usize,
    /// Approximate directory allocation, including control bytes.
    pub directory_estimated_bytes: usize,
    /// Exact requested bytes for the dense key/value vector capacities.
    pub dense_estimated_bytes: usize,
}

impl Fixed32MapStats {
    /// Estimated requested heap bytes retained by the map.
    #[must_use]
    pub const fn estimated_heap_bytes(self) -> usize {
        self.directory_estimated_bytes
            .saturating_add(self.dense_estimated_bytes)
    }
}
