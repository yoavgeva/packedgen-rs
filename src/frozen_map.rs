use core::fmt;

#[cfg(feature = "gxhash")]
use ptr_hash::hash::Gx128;
use ptr_hash::hash::KeyHasher;
#[cfg(not(feature = "gxhash"))]
use ptr_hash::hash::Xxh3_128;
use ptr_hash::{DefaultPtrHash, PtrHashParams};

use crate::{ArenaError, PackedKeyArena, PackedKeyRef};

type FrozenIndex = DefaultPtrHash<DigestHasher, Digest>;

#[derive(Clone, Copy, Hash)]
struct Digest(u128);

#[derive(Clone)]
struct DigestHasher;

impl KeyHasher<Digest> for DigestHasher {
    type H = u128;

    fn hash(digest: &Digest, seed: u64) -> Self::H {
        let seed = u128::from(seed);
        digest.0 ^ seed ^ (seed << 64)
    }
}

/// Immutable exact binary-key map backed by a minimal perfect hash function.
///
/// Construction assigns each member key a unique dense slot. Lookups for
/// arbitrary keys still compare the original bytes at that slot, because a
/// perfect hash function is collision-free only for the construction set.
pub struct FrozenPackedMap<V> {
    index: Option<FrozenIndex>,
    arena: PackedKeyArena,
    entries: Box<[FrozenEntry<V>]>,
    index_bits_per_entry: f64,
}

struct FrozenEntry<V> {
    key: PackedKeyRef,
    value: V,
}

impl<V> FrozenPackedMap<V> {
    /// Builds an immutable map from a unique set of binary-key entries.
    ///
    /// Input order does not affect lookup semantics. The retained table is
    /// arranged in perfect-hash order and owns packed copies of every key.
    ///
    /// # Errors
    ///
    /// Returns [`FrozenBuildError::Arena`] when a key cannot be packed, or
    /// [`FrozenBuildError::IndexConstructionFailed`] when the perfect-hash
    /// builder cannot produce a collision-free index. Duplicate keys are one
    /// cause of index-construction failure.
    pub fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        let iterator = entries.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut arena = PackedKeyArena::new();
        let mut staged = Vec::new();
        staged
            .try_reserve(lower)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut hashes = Vec::new();
        hashes
            .try_reserve(lower)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;

        for (key, value) in iterator {
            let key = key.as_ref();
            let key_ref = arena.insert(key).map_err(FrozenBuildError::Arena)?;
            hashes.push(key_digest(key));
            staged.push((key_ref, value));
        }

        if staged.is_empty() {
            return Ok(Self {
                index: None,
                arena,
                entries: Box::new([]),
                index_bits_per_entry: 0.0,
            });
        }

        let index = FrozenIndex::try_new(&hashes, PtrHashParams::default())
            .ok_or(FrozenBuildError::IndexConstructionFailed)?;
        let (pilot_bits, remap_bits) = index.bits_per_element();
        let mut indexed = Vec::new();
        indexed
            .try_reserve_exact(staged.len())
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        indexed.extend(
            staged
                .into_iter()
                .zip(hashes)
                .map(|((key_ref, value), hash)| (index.index(&hash), key_ref, value)),
        );
        indexed.sort_unstable_by_key(|(slot, _, _)| *slot);
        if indexed
            .iter()
            .enumerate()
            .any(|(expected, (actual, _, _))| expected != *actual)
        {
            return Err(FrozenBuildError::IndexConstructionFailed);
        }
        let entries = indexed
            .into_iter()
            .map(|(_, key, value)| FrozenEntry { key, value })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            index: Some(index),
            arena,
            entries,
            index_bits_per_entry: pilot_bits + remap_bits,
        })
    }

    /// Returns the value for `key`, verifying its original packed bytes.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let index = self.index.as_ref()?;
        let slot = index.index(&key_digest(key));
        let entry = self.entries.get(slot)?;
        (self.arena.get(entry.key) == Some(key)).then_some(&entry.value)
    }

    /// Returns whether `key` belongs to this frozen generation.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Number of entries in the frozen generation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the frozen generation contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Captures retained packed-key, slot, and perfect-index density.
    #[must_use]
    pub fn stats(&self) -> FrozenMapStats {
        FrozenMapStats {
            len: self.entries.len(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            slot_bytes: size_of_val(self.entries.as_ref()),
            index_bits_per_entry: self.index_bits_per_entry,
        }
    }
}

fn key_digest(key: &[u8]) -> Digest {
    #[cfg(feature = "gxhash")]
    let digest = <Gx128 as KeyHasher<[u8]>>::hash(key, 0);
    #[cfg(not(feature = "gxhash"))]
    let digest = <Xxh3_128 as KeyHasher<[u8]>>::hash(key, 0);
    Digest(digest)
}

/// Retained-memory components for a [`FrozenPackedMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrozenMapStats {
    /// Frozen entries.
    pub len: usize,
    /// Requested capacity held by packed-key arena segments.
    pub arena_allocated_bytes: usize,
    /// Logical bytes occupied by member keys.
    pub arena_key_bytes: usize,
    /// Dense key-reference and value slots, excluding heap allocator metadata.
    pub slot_bytes: usize,
    /// Perfect-index pilot and remap metadata reported by `PtrHash`.
    pub index_bits_per_entry: f64,
}

/// Failure while building a frozen exact map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenBuildError {
    /// Packed-key storage rejected a key or allocation.
    Arena(ArenaError),
    /// Temporary construction storage could not be reserved.
    AllocationFailed,
    /// The perfect-hash index could not be constructed for the input set.
    IndexConstructionFailed,
}

impl fmt::Display for FrozenBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arena(error) => error.fmt(formatter),
            Self::AllocationFailed => formatter.write_str("frozen-map allocation failed"),
            Self::IndexConstructionFailed => {
                formatter.write_str("frozen perfect-hash construction failed")
            }
        }
    }
}

impl std::error::Error for FrozenBuildError {}
