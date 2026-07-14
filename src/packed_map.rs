use core::fmt;
use std::mem;

use opthash::{ElasticHashMap, EpochSnapshot, Equivalent, ReserveFraction};

use crate::filter::NegativeLookupFilter;
use crate::{
    ArenaError, CapacityError, ElasticConfig, InsertOutcome, PackedKeyArena, PackedKeyRef,
};

/// Binary-key elastic map backed by a segmented packed-key arena.
///
/// Stored table keys are eight-byte [`PackedKeyRef`] values. Lookups hash the
/// caller's bytes once, then compare candidate references against bytes in the
/// arena through the owned core's prehashed API.
pub struct PackedBinaryMap<V> {
    inner: ElasticHashMap<PackedKeyRef, V>,
    arena: PackedKeyArena,
    negative_filter: NegativeLookupFilter,
    live_limit: usize,
    live_key_bytes: usize,
    deletes_since_rebuild: usize,
}

impl<V> PackedBinaryMap<V> {
    /// Constructs a fixed-epoch binary map using the default key-segment size.
    #[must_use]
    pub fn new(config: ElasticConfig) -> Self {
        Self {
            inner: ElasticHashMap::with_capacity_and_reserve(
                config.live_capacity(),
                config.reserve(),
            ),
            arena: PackedKeyArena::new(),
            negative_filter: NegativeLookupFilter::new(config.live_capacity()),
            live_limit: config.live_capacity(),
            live_key_bytes: 0,
            deletes_since_rebuild: 0,
        }
    }

    /// Constructs a fixed-epoch map with explicit key-arena allocation
    /// granularity.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] for an invalid segment size.
    pub fn with_key_segment_bytes(
        config: ElasticConfig,
        segment_bytes: usize,
    ) -> Result<Self, ArenaError> {
        Ok(Self {
            inner: ElasticHashMap::with_capacity_and_reserve(
                config.live_capacity(),
                config.reserve(),
            ),
            arena: PackedKeyArena::with_segment_bytes(segment_bytes)?,
            negative_filter: NegativeLookupFilter::new(config.live_capacity()),
            live_limit: config.live_capacity(),
            live_key_bytes: 0,
            deletes_since_rebuild: 0,
        })
    }

    /// Inserts or replaces a binary key.
    ///
    /// Replacements do not append duplicate key bytes to the arena.
    ///
    /// # Errors
    ///
    /// Returns a capacity error for a new key at the fixed live limit, or an
    /// arena error when the key is too large or allocation fails.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, PackedMapError> {
        let hash = self.inner.hash_key(key);
        if self.negative_filter.may_contain_hash(hash) {
            let query = PackedQuery {
                arena: &self.arena,
                bytes: key,
            };
            if let Some(previous) = self.inner.get_mut_prehashed(hash, &query) {
                return Ok(InsertOutcome::Replaced(mem::replace(previous, value)));
            }
        }

        if self.inner.len() >= self.live_limit {
            return Err(PackedMapError::Capacity(CapacityError::new(
                self.live_limit,
            )));
        }

        let key_ref = self.arena.insert(key).map_err(PackedMapError::Arena)?;
        self.negative_filter.insert_hash(hash);
        if let Err((key_ref, value)) = self
            .inner
            .try_insert_unique_prehashed_in_place(hash, key_ref, value)
        {
            self.rebuild_core();
            if self
                .inner
                .try_insert_unique_prehashed_in_place(hash, key_ref, value)
                .is_err()
            {
                return Err(PackedMapError::Capacity(CapacityError::new(
                    self.live_limit,
                )));
            }
        }
        self.live_key_bytes += key.len();
        Ok(InsertOutcome::Inserted)
    }

    /// Returns a shared value reference for a binary key.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = self.inner.hash_key(key);
        if !self.negative_filter.may_contain_hash(hash) {
            return None;
        }
        self.inner.get_prehashed(
            hash,
            &PackedQuery {
                arena: &self.arena,
                bytes: key,
            },
        )
    }

    /// Returns whether a binary key is live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Removes a key and returns its value.
    ///
    /// Key bytes remain in the current append-only arena until a future
    /// compacting generation rebuild.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let hash = self.inner.hash_key(key);
        if !self.negative_filter.may_contain_hash(hash) {
            return None;
        }
        let removed = self.inner.remove_prehashed_deferred(
            hash,
            &PackedQuery {
                arena: &self.arena,
                bytes: key,
            },
        );
        let (key_ref, value) = removed?;
        self.live_key_bytes = self.live_key_bytes.saturating_sub(key_ref.len());
        self.deletes_since_rebuild += 1;
        if self.deletes_since_rebuild >= self.rebuild_delete_threshold() {
            self.rebuild_core();
        }
        Some(value)
    }

    /// Removes every entry and releases packed key segments.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.arena.clear();
        self.negative_filter.clear();
        self.live_key_bytes = 0;
        self.deletes_since_rebuild = 0;
    }

    /// Number of live keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether no live keys exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Captures occupancy, epoch, filter, and packed-key memory state.
    #[must_use]
    pub fn stats(&self) -> PackedMapStats {
        PackedMapStats {
            len: self.inner.len(),
            live_limit: self.live_limit,
            core_capacity: self.inner.capacity(),
            reserve: self.inner.reserve_fraction(),
            epoch: self.inner.epoch(),
            negative_filter_bytes: self.negative_filter.bytes(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            live_key_bytes: self.live_key_bytes,
            deletes_since_rebuild: self.deletes_since_rebuild,
        }
    }

    fn rebuild_delete_threshold(&self) -> usize {
        (self.live_limit / 4).max(1)
    }

    fn rebuild_core(&mut self) {
        let replacement = ElasticHashMap::with_capacity_and_reserve(
            self.live_limit,
            self.inner.reserve_fraction(),
        );
        let old = mem::replace(&mut self.inner, replacement);
        for (key_ref, value) in old {
            let bytes = self
                .arena
                .get(key_ref)
                .expect("live packed key reference must resolve");
            let hash = self.inner.hash_key(bytes);
            self.inner
                .try_insert_unique_prehashed_in_place(hash, key_ref, value)
                .unwrap_or_else(|_| panic!("fresh elastic epoch must fit every live entry"));
        }
        self.deletes_since_rebuild = 0;
        self.rebuild_negative_filter();
    }

    fn rebuild_negative_filter(&mut self) {
        let mut replacement = NegativeLookupFilter::new(self.live_limit);
        for (key_ref, _) in &self.inner {
            if let Some(bytes) = self.arena.get(*key_ref) {
                replacement.insert_hash(self.inner.hash_key(bytes));
            }
        }
        self.negative_filter = replacement;
    }
}

struct PackedQuery<'a> {
    arena: &'a PackedKeyArena,
    bytes: &'a [u8],
}

impl Equivalent<PackedKeyRef> for PackedQuery<'_> {
    fn equivalent(&self, key: &PackedKeyRef) -> bool {
        self.arena.get(*key) == Some(self.bytes)
    }
}

/// Failed packed binary-map insertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackedMapError {
    /// The fixed epoch reached its configured live-key limit.
    Capacity(CapacityError),
    /// Key packing or arena allocation failed.
    Arena(ArenaError),
}

impl fmt::Display for PackedMapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity(error) => error.fmt(formatter),
            Self::Arena(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for PackedMapError {}

/// Observable state for a [`PackedBinaryMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackedMapStats {
    /// Current live keys.
    pub len: usize,
    /// Application-enforced live-key limit.
    pub live_limit: usize,
    /// Core capacity before automatic growth.
    pub core_capacity: usize,
    /// Exact empty-slot reserve.
    pub reserve: ReserveFraction,
    /// Core allocation-epoch state.
    pub epoch: EpochSnapshot,
    /// Requested bytes in the definite-negative filter.
    pub negative_filter_bytes: usize,
    /// Requested capacity held by key-arena segments and their directory.
    pub arena_allocated_bytes: usize,
    /// All key bytes appended in this generation, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to currently live entries.
    pub live_key_bytes: usize,
    /// Deletes accumulated since the last byte-aware table rebuild.
    pub deletes_since_rebuild: usize,
}

impl PackedMapStats {
    /// Bytes retained for removed keys until compaction.
    #[must_use]
    pub fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}
