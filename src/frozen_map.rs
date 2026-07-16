use core::fmt;

#[cfg(feature = "phast")]
use ph::GetSize;
#[cfg(feature = "phast")]
use ph::phast::Function2;
#[cfg(feature = "phast")]
use ph::seeds::Bits8;
#[cfg(feature = "gxhash")]
use ptr_hash::hash::Gx128;
use ptr_hash::hash::KeyHasher;
#[cfg(not(feature = "gxhash"))]
use ptr_hash::hash::Xxh3_128;
use ptr_hash::{DefaultPtrHash, PtrHashParams};

use crate::{ArenaError, PackedKeyArena, PackedKeyRef};

type PtrFrozenIndex = DefaultPtrHash<DigestHasher, Digest>;
const LINEAR_INDEX_MAX_ENTRIES: usize = 8;

#[cfg(not(feature = "phast"))]
type FrozenIndex = PtrFrozenIndex;
#[cfg(feature = "phast")]
enum FrozenIndex {
    PtrHash(PtrFrozenIndex),
    PhastPlus(Function2<Bits8>),
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Digest(pub(crate) u128);

#[derive(Clone)]
struct DigestHasher;

impl KeyHasher<Digest> for DigestHasher {
    type H = u128;

    fn hash(digest: &Digest, seed: u64) -> Self::H {
        let seed = u128::from(seed);
        digest.0 ^ seed ^ (seed << 64)
    }
}

/// Perfect-hash implementation used by a frozen generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FrozenIndexBackend {
    /// `PtrHash`'s cache-efficient minimal perfect hash.
    #[default]
    PtrHash,
    /// Experimental `PHast`+ index from the 2025 `PHast` paper.
    #[cfg(feature = "phast")]
    PhastPlus,
}

/// Immutable exact binary-key map backed by a minimal perfect hash function.
///
/// Construction assigns each member key a unique dense slot. Lookups for
/// arbitrary keys still compare the original bytes at that slot, because a
/// perfect hash function is collision-free only for the construction set.
/// `PtrHash` is the default index; the optional `phast` feature makes the
/// compact `PHast`+ backend available for explicit workload measurement.
pub struct FrozenPackedMap<V> {
    index: Option<FrozenIndex>,
    arena: PackedKeyArena,
    entries: Box<[FrozenEntry<V>]>,
    index_bits_per_entry: f64,
    embedded_key_tag: bool,
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
        Self::try_from_entries_impl(entries, false, FrozenIndexBackend::PtrHash, key_digest)
    }

    /// Builds with an explicit perfect-hash backend.
    ///
    /// This is exposed for controlled workload experiments. PtrHash remains
    /// the default because backend tradeoffs depend on hit ratio and map size.
    #[doc(hidden)]
    pub fn try_from_entries_with_index_backend<I, K>(
        entries: I,
        backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_impl(entries, false, backend, key_digest)
    }

    pub(crate) fn try_from_entries_with_index_backend_and_digest<I, K>(
        entries: I,
        backend: FrozenIndexBackend,
        digest: impl FnMut(&[u8]) -> Digest,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_impl(entries, false, backend, digest)
    }

    pub(crate) fn try_from_entries_with_embedded_key_tag_index_and_digest<I, K>(
        entries: I,
        backend: FrozenIndexBackend,
        digest: impl FnMut(&[u8]) -> Digest,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_impl(entries, true, backend, digest)
    }

    fn try_from_entries_impl<I, K>(
        entries: I,
        request_embedded_key_tag: bool,
        backend: FrozenIndexBackend,
        mut digest: impl FnMut(&[u8]) -> Digest,
    ) -> Result<Self, FrozenBuildError>
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

        let mut embedded_key_tag = request_embedded_key_tag;
        for (key, value) in iterator {
            let key = key.as_ref();
            embedded_key_tag &= u8::try_from(key.len()).is_ok();
            let key_ref = arena.insert(key).map_err(FrozenBuildError::Arena)?;
            hashes.push(digest(key));
            staged.push((key_ref, value));
        }

        if staged.is_empty() {
            return Ok(Self {
                index: None,
                arena,
                entries: Box::new([]),
                index_bits_per_entry: 0.0,
                embedded_key_tag,
            });
        }

        if staged.len() <= LINEAR_INDEX_MAX_ENTRIES {
            for left in 0..staged.len() {
                for right in left + 1..staged.len() {
                    if arena.get(staged[left].0) == arena.get(staged[right].0) {
                        return Err(FrozenBuildError::IndexConstructionFailed);
                    }
                }
            }
            let entries = staged
                .into_iter()
                .zip(hashes)
                .map(|((key, value), digest)| FrozenEntry {
                    key: if embedded_key_tag {
                        key.with_embedded_tag(digest_tag(digest))
                    } else {
                        key
                    },
                    value,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            return Ok(Self {
                index: None,
                arena,
                entries,
                index_bits_per_entry: 0.0,
                embedded_key_tag,
            });
        }

        let (index, index_bits_per_entry) = build_frozen_index(&hashes, backend)?;
        let mut indexed = Vec::new();
        indexed
            .try_reserve_exact(staged.len())
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        indexed.extend(
            staged
                .into_iter()
                .zip(hashes)
                .map(|((key_ref, value), hash)| {
                    (frozen_index_slot(&index, &hash), key_ref, value, hash)
                }),
        );
        indexed.sort_unstable_by_key(|(slot, _, _, _)| *slot);
        if indexed
            .iter()
            .enumerate()
            .any(|(expected, (actual, _, _, _))| expected != *actual)
        {
            return Err(FrozenBuildError::IndexConstructionFailed);
        }
        let entries = indexed
            .into_iter()
            .map(|(_, key, value, digest)| FrozenEntry {
                key: if embedded_key_tag {
                    key.with_embedded_tag(digest_tag(digest))
                } else {
                    key
                },
                value,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            index: Some(index),
            arena,
            entries,
            index_bits_per_entry,
            embedded_key_tag,
        })
    }

    /// Returns the value for `key`, verifying its original packed bytes.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_indexed(key).map(|(_, value)| value)
    }

    /// Returns the dense perfect-hash slot and value after exact verification.
    ///
    /// This stays crate-private because dense slots are an implementation
    /// detail, but lets generation overlays represent deletions with one bit
    /// per frozen entry instead of retaining another copy of each deleted key.
    pub(crate) fn get_indexed(&self, key: &[u8]) -> Option<(usize, &V)> {
        self.get_indexed_with_digest(key, key_digest(key))
    }

    pub(crate) fn get_indexed_with_digest(
        &self,
        key: &[u8],
        digest: Digest,
    ) -> Option<(usize, &V)> {
        let Some(index) = self.index.as_ref() else {
            return self.entries.iter().enumerate().find_map(|(slot, entry)| {
                (self.key_bytes(entry.key) == Some(key)).then_some((slot, &entry.value))
            });
        };
        let slot = frozen_index_slot(index, &digest);
        let entry = self.entries.get(slot)?;
        let stored_key = if self.embedded_key_tag {
            if entry.key.embedded_tag() != digest_tag(digest) {
                return None;
            }
            self.arena.get(entry.key.without_embedded_tag())
        } else {
            self.arena.get(entry.key)
        };
        (stored_key == Some(key)).then_some((slot, &entry.value))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn slot_matches(&self, slot: usize, key: &[u8]) -> bool {
        self.entries
            .get(slot)
            .is_some_and(|entry| self.key_bytes(entry.key) == Some(key))
    }

    pub(crate) fn for_each_key(&self, mut visit: impl FnMut(&[u8])) {
        self.for_each_entry(|key, _| visit(key));
    }

    pub(crate) fn for_each_entry(&self, mut visit: impl FnMut(&[u8], &V)) {
        self.for_each_indexed_entry(|_, key, value| visit(key, value));
    }

    pub(crate) fn for_each_indexed_entry(&self, mut visit: impl FnMut(usize, &[u8], &V)) {
        for (slot, entry) in self.entries.iter().enumerate() {
            let key = self
                .key_bytes(entry.key)
                .expect("frozen entry key must resolve in its arena");
            visit(slot, key, &entry.value);
        }
    }

    /// Looks up a fixed batch of keys without allocating temporary storage.
    ///
    /// Perfect hashing gives each construction-set key a dense slot. For a
    /// batch, digest and slot computation is independent, so resolving slots
    /// in ascending order keeps the compact entry array's cache lines hot
    /// while preserving the caller's result order.  Exact key-byte checks are
    /// still performed for every candidate, so arbitrary misses and digest
    /// collisions retain ordinary map semantics.
    #[must_use]
    pub fn get_many<const N: usize>(&self, keys: [&[u8]; N]) -> [Option<&V>; N] {
        let Some(index) = self.index.as_ref() else {
            return keys.map(|key| self.get(key));
        };

        let digests = keys.map(key_digest);
        let slots = digests.map(|digest| frozen_index_slot(index, &digest));
        let mut order: [usize; N] = core::array::from_fn(|position| position);

        // N is a caller-selected compile-time batch size.  Insertion sorting
        // avoids a heap allocation and is efficient for the small batches used
        // by storage-engine read paths (typically 8-64 keys).
        for position in 1..N {
            let mut cursor = position;
            while cursor > 0 && slots[order[cursor]] < slots[order[cursor - 1]] {
                order.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }

        let mut results = [None; N];
        for input in order {
            let slot = slots[input];
            let Some(entry) = self.entries.get(slot) else {
                continue;
            };
            let stored_key = if self.embedded_key_tag {
                if entry.key.embedded_tag() != digest_tag(digests[input]) {
                    continue;
                }
                self.arena.get(entry.key.without_embedded_tag())
            } else {
                self.arena.get(entry.key)
            };
            if stored_key == Some(keys[input]) {
                results[input] = Some(&entry.value);
            }
        }
        results
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

    fn key_bytes(&self, key: PackedKeyRef) -> Option<&[u8]> {
        self.arena.get(if self.embedded_key_tag {
            key.without_embedded_tag()
        } else {
            key
        })
    }
}

pub(crate) fn key_digest(key: &[u8]) -> Digest {
    #[cfg(feature = "gxhash")]
    let digest = <Gx128 as KeyHasher<[u8]>>::hash(key, 0);
    #[cfg(not(feature = "gxhash"))]
    let digest = <Xxh3_128 as KeyHasher<[u8]>>::hash(key, 0);
    Digest(digest)
}

fn digest_tag(digest: Digest) -> u16 {
    let folded = digest.0 ^ (digest.0 >> 64);
    u16::try_from(folded & 0x1ff).expect("nine digest bits fit u16")
}

#[cfg(not(feature = "phast"))]
fn frozen_index_slot(index: &FrozenIndex, digest: &Digest) -> usize {
    index.index(digest)
}

#[cfg(feature = "phast")]
fn frozen_index_slot(index: &FrozenIndex, digest: &Digest) -> usize {
    match index {
        FrozenIndex::PtrHash(index) => index.index(digest),
        FrozenIndex::PhastPlus(index) => index.get(digest),
    }
}

#[cfg(not(feature = "phast"))]
fn build_frozen_index(
    hashes: &[Digest],
    _backend: FrozenIndexBackend,
) -> Result<(FrozenIndex, f64), FrozenBuildError> {
    let index = FrozenIndex::try_new(hashes, PtrHashParams::default())
        .ok_or(FrozenBuildError::IndexConstructionFailed)?;
    let (pilot_bits, remap_bits) = index.bits_per_element();
    Ok((index, pilot_bits + remap_bits))
}

#[cfg(feature = "phast")]
#[allow(clippy::cast_precision_loss)]
fn build_frozen_index(
    hashes: &[Digest],
    backend: FrozenIndexBackend,
) -> Result<(FrozenIndex, f64), FrozenBuildError> {
    match backend {
        FrozenIndexBackend::PtrHash => {
            let index = PtrFrozenIndex::try_new(hashes, PtrHashParams::default())
                .ok_or(FrozenBuildError::IndexConstructionFailed)?;
            let (pilot_bits, remap_bits) = index.bits_per_element();
            Ok((FrozenIndex::PtrHash(index), pilot_bits + remap_bits))
        }
        FrozenIndexBackend::PhastPlus => {
            let mut unique = hashes.to_vec();
            unique.sort_unstable();
            if unique.windows(2).any(|window| window[0] == window[1]) {
                return Err(FrozenBuildError::IndexConstructionFailed);
            }
            let index = Function2::from_vec_st(unique);
            let bits = index.size_bytes() as f64 * 8.0 / hashes.len() as f64;
            Ok((FrozenIndex::PhastPlus(index), bits))
        }
    }
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
