use std::hash::{BuildHasher, Hash};
use std::mem;

use hashbrown::{DefaultHashBuilder, HashTable};

use crate::{ArenaError, InsertOutcome, PackedKeyArena, PackedKeyRef};

/// Dynamic `SwissTable` whose original binary keys share a packed byte arena.
///
/// The table stores only an eight-byte [`PackedKeyRef`] beside each value.
/// Queries retain exact byte semantics: `SwissTable` fingerprints only select
/// candidates, and each candidate is verified against the original bytes.
pub struct PackedSwissMap<V, S = DefaultHashBuilder> {
    table: HashTable<Entry<V>>,
    arena: PackedKeyArena,
    hash_builder: S,
    live_key_bytes: usize,
}

impl<V> PackedSwissMap<V> {
    /// Creates an empty map without allocating.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a map sized for at least `entry_capacity` entries.
    #[must_use]
    pub fn with_capacity(entry_capacity: usize) -> Self {
        Self {
            table: HashTable::with_capacity(entry_capacity),
            arena: PackedKeyArena::new(),
            hash_builder: DefaultHashBuilder::default(),
            live_key_bytes: 0,
        }
    }

    /// Creates a map with entry capacity and one contiguous initial key arena.
    ///
    /// This is the most memory-efficient constructor when the caller can
    /// estimate the total encoded key bytes. A zero estimate uses one byte so
    /// the arena remains valid for empty-key workloads.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] when `key_byte_capacity`
    /// cannot fit the arena's packed 32-bit segment-offset representation.
    pub fn try_with_capacity_and_key_bytes(
        entry_capacity: usize,
        key_byte_capacity: usize,
    ) -> Result<Self, ArenaError> {
        Ok(Self {
            table: HashTable::with_capacity(entry_capacity),
            arena: PackedKeyArena::with_segment_bytes(key_byte_capacity.max(1))?,
            hash_builder: DefaultHashBuilder::default(),
            live_key_bytes: 0,
        })
    }
}

impl<V, S> PackedSwissMap<V, S>
where
    S: BuildHasher,
{
    /// Creates a map with caller-selected entry capacity and hash builder.
    #[must_use]
    pub fn with_capacity_and_hasher(entry_capacity: usize, hash_builder: S) -> Self {
        Self {
            table: HashTable::with_capacity(entry_capacity),
            arena: PackedKeyArena::new(),
            hash_builder,
            live_key_bytes: 0,
        }
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
    /// Panics only if a live table entry no longer resolves in its owning
    /// arena, which indicates internal invariant corruption.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, ArenaError> {
        let hash = self.hash_key(key);
        let arena = &self.arena;
        if let Some(entry) = self
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
        self.table.insert_unique(
            hash,
            Entry {
                key: key_ref,
                value,
            },
            |entry| {
                let bytes = arena
                    .get(entry.key)
                    .expect("live SwissTable entry must resolve in its arena");
                hash_builder.hash_one(bytes)
            },
        );
        Ok(InsertOutcome::Inserted)
    }

    /// Returns the value for `key` after exact original-byte verification.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = self.hash_key(key);
        self.table
            .find(hash, |entry| self.arena.get(entry.key) == Some(key))
            .map(|entry| &entry.value)
    }

    /// Returns a mutable value for `key`.
    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let hash = self.hash_key(key);
        let arena = &self.arena;
        self.table
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
        let arena = &self.arena;
        let occupied = self
            .table
            .find_entry(hash, |entry| arena.get(entry.key) == Some(key))
            .ok()?;
        let (entry, _) = occupied.remove();
        self.live_key_bytes = self.live_key_bytes.saturating_sub(entry.key.len());
        Some(entry.value)
    }

    /// Removes all entries and releases packed key segments.
    pub fn clear(&mut self) {
        self.table.clear();
        self.arena.clear();
        self.live_key_bytes = 0;
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns whether the map has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Captures observable table and packed-arena state.
    #[must_use]
    pub fn stats(&self) -> SwissMapStats {
        SwissMapStats {
            len: self.table.len(),
            table_capacity: self.table.capacity(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            live_key_bytes: self.live_key_bytes,
        }
    }

    #[inline]
    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hash_builder.hash_one(key)
    }
}

impl<V> Default for PackedSwissMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

struct Entry<V> {
    key: PackedKeyRef,
    value: V,
}

/// Observable retained state for a [`PackedSwissMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SwissMapStats {
    /// Live entries.
    pub len: usize,
    /// Entries that fit before the next `SwissTable` growth.
    pub table_capacity: usize,
    /// Requested packed-arena allocation bytes.
    pub arena_allocated_bytes: usize,
    /// Appended key bytes, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to live entries.
    pub live_key_bytes: usize,
}

impl SwissMapStats {
    /// Bytes retained for removed keys until the map is cleared.
    #[must_use]
    pub const fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}
