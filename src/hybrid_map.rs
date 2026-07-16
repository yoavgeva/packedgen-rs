use core::fmt;
use std::hash::BuildHasher;

use hashbrown::DefaultHashBuilder;

use crate::{
    ArenaError, FrozenBuildError, FrozenMapStats, FrozenPackedMap, InsertOutcome, PackedSwissMap,
    SwissMapStats,
};

/// Mutable overlay on an immutable, memory-dense frozen generation.
///
/// Reads first consult a small dynamic delta, then the frozen base. Deletions
/// from the base are recorded by dense-slot bit rather than by copying their
/// keys. This layout targets read-mostly maps that receive a bounded amount of
/// churn between periodic generation rebuilds.
pub struct HybridPackedMap<V> {
    base: FrozenPackedMap<V>,
    delta: PackedSwissMap<V>,
    tombstones: Box<[u64]>,
    membership: HybridMembership,
    tombstone_count: usize,
    len: usize,
}

impl<V> HybridPackedMap<V> {
    /// Creates a hybrid map around an already-built frozen generation.
    #[must_use]
    pub fn from_frozen(base: FrozenPackedMap<V>) -> Self {
        Self::with_delta_capacity(base, 0)
    }

    /// Creates a hybrid map and reserves the expected mutable delta entries.
    #[must_use]
    pub fn with_delta_capacity(base: FrozenPackedMap<V>, delta_capacity: usize) -> Self {
        Self::with_delta_capacity_and_filter(
            base,
            delta_capacity,
            HybridFilterMode::OneBytePerEntry,
        )
    }

    /// Creates a hybrid map with explicit delta and negative-filter policy.
    #[must_use]
    pub fn with_delta_capacity_and_filter(
        base: FrozenPackedMap<V>,
        delta_capacity: usize,
        filter_mode: HybridFilterMode,
    ) -> Self {
        let len = base.len();
        let tombstone_words = len.div_ceil(u64::BITS as usize);
        let mut membership = HybridMembership::new(len.saturating_add(delta_capacity), filter_mode);
        base.for_each_key(|key| membership.insert(key));
        Self {
            base,
            delta: PackedSwissMap::with_capacity(delta_capacity),
            tombstones: vec![0; tombstone_words].into_boxed_slice(),
            membership,
            tombstone_count: 0,
            len,
        }
    }

    /// Builds a frozen base from a unique set of binary-key entries.
    ///
    /// # Errors
    ///
    /// Returns the same construction errors as [`FrozenPackedMap`]. Duplicate
    /// keys are rejected because the frozen perfect hash requires a set.
    pub fn try_from_entries<I, K>(entries: I) -> Result<Self, HybridBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        FrozenPackedMap::try_from_entries(entries)
            .map(Self::from_frozen)
            .map_err(HybridBuildError::Frozen)
    }

    /// Returns the current value for `key`.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        if let Some(value) = self.delta.get(key) {
            return Some(value);
        }
        if !self.membership.may_contain(key) {
            return None;
        }
        let (slot, value) = self.base.get_indexed(key)?;
        (!self.is_tombstoned(slot)).then_some(value)
    }

    /// Returns whether `key` is currently live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Number of live entries across the frozen base and mutable delta.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the logical map contains no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the immutable generation.
    #[must_use]
    pub const fn frozen(&self) -> &FrozenPackedMap<V> {
        &self.base
    }

    /// Captures retained state for both generations and the deletion bitset.
    #[must_use]
    pub fn stats(&self) -> HybridMapStats {
        HybridMapStats {
            len: self.len,
            base: self.base.stats(),
            delta: self.delta.stats(),
            tombstones: self.tombstone_count,
            tombstone_bytes: self.tombstones.len() * size_of::<u64>(),
            membership_bytes: self.membership.bytes(),
        }
    }

    #[inline]
    fn is_tombstoned(&self, slot: usize) -> bool {
        let word = slot / u64::BITS as usize;
        let bit = slot % u64::BITS as usize;
        self.tombstones
            .get(word)
            .is_some_and(|bits| bits & (1_u64 << bit) != 0)
    }

    fn set_tombstone(&mut self, slot: usize) {
        let word = slot / u64::BITS as usize;
        let bit = slot % u64::BITS as usize;
        let mask = 1_u64 << bit;
        let bits = &mut self.tombstones[word];
        if *bits & mask == 0 {
            *bits |= mask;
            self.tombstone_count += 1;
        }
    }
}

impl<V: Clone> HybridPackedMap<V> {
    /// Inserts or replaces a binary key in the mutable generation.
    ///
    /// Values inherited from the immutable base are cloned when returned as a
    /// replacement. This is the only mutation-time cost imposed by immutable
    /// ownership; lookups do not require `V: Clone`.
    ///
    /// # Errors
    ///
    /// Returns an arena error when a new delta key cannot be packed.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, ArenaError> {
        let may_exist_in_base = self.membership.may_contain(key);
        if let InsertOutcome::Replaced(previous) = self.delta.try_insert(key, value)? {
            return Ok(InsertOutcome::Replaced(previous));
        }

        self.membership.insert(key);
        let inherited = may_exist_in_base
            .then(|| self.base.get_indexed(key))
            .flatten()
            .filter(|(slot, _)| !self.is_tombstoned(*slot))
            .map(|(_, value)| value.clone());
        if let Some(previous) = inherited {
            Ok(InsertOutcome::Replaced(previous))
        } else {
            self.len += 1;
            Ok(InsertOutcome::Inserted)
        }
    }

    /// Removes `key`, returning its current value when present.
    ///
    /// Removing an overlaid base key permanently sets that base slot's
    /// tombstone for this generation, so the older value cannot reappear.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        if let Some(value) = self.delta.remove(key) {
            if let Some((slot, _)) = self.base.get_indexed(key) {
                self.set_tombstone(slot);
            }
            self.len -= 1;
            return Some(value);
        }

        let (slot, value) = self.base.get_indexed(key)?;
        if self.is_tombstoned(slot) {
            return None;
        }
        let value = value.clone();
        self.set_tombstone(slot);
        self.len -= 1;
        Some(value)
    }
}

/// Retained-memory components for a [`HybridPackedMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HybridMapStats {
    /// Logical live entries across both generations.
    pub len: usize,
    /// Immutable base-generation components.
    pub base: FrozenMapStats,
    /// Mutable delta components.
    pub delta: SwissMapStats,
    /// Deleted base entries.
    pub tombstones: usize,
    /// Bytes reserved by the dense deletion bitset.
    pub tombstone_bytes: usize,
    /// Bytes retained by the stable base-plus-delta negative filter.
    pub membership_bytes: usize,
}

struct HybridMembership {
    words: Box<[u64]>,
    hash_builder: DefaultHashBuilder,
}

impl HybridMembership {
    fn new(capacity: usize, mode: HybridFilterMode) -> Self {
        let words = match mode {
            HybridFilterMode::Disabled => 0,
            HybridFilterMode::HalfBytePerEntry => capacity.div_ceil(16),
            HybridFilterMode::OneBytePerEntry => capacity.div_ceil(8),
        };
        Self {
            words: vec![0; words].into_boxed_slice(),
            hash_builder: DefaultHashBuilder::default(),
        }
    }

    fn insert(&mut self, key: &[u8]) {
        let Some((word, mask)) = self.location(key) else {
            return;
        };
        self.words[word] |= mask;
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        let Some((word, mask)) = self.location(key) else {
            return true;
        };
        self.words[word] & mask == mask
    }

    fn bytes(&self) -> usize {
        size_of_val(self.words.as_ref())
    }

    fn location(&self, key: &[u8]) -> Option<(usize, u64)> {
        if self.words.is_empty() {
            return None;
        }
        let hash = self.hash_builder.hash_one(key);
        let word = reduce(hash, self.words.len());
        let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
        let mut second = (hash >> 32) as u32 & 63;
        if second == first {
            second = (second + 1) & 63;
        }
        Some((word, (1_u64 << first) | (1_u64 << second)))
    }
}

/// Stable negative-filter policy for a hybrid generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HybridFilterMode {
    /// Retain no negative filter. Saves about one byte per planned entry but
    /// every delta miss must continue into the frozen generation.
    Disabled,
    /// Retain four filter bits per planned entry. This is denser than the
    /// default but admits more false positives into the frozen generation.
    HalfBytePerEntry,
    /// Retain eight filter bits per planned base-plus-delta entry.
    #[default]
    OneBytePerEntry,
}

fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize fits u128");
    usize::try_from((u128::from(hash) * upper) >> 64).expect("reduced hash is below upper")
}

/// Failure while building the immutable base of a hybrid map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridBuildError {
    /// Frozen-generation construction failed.
    Frozen(FrozenBuildError),
}

impl fmt::Display for HybridBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frozen(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HybridBuildError {}
