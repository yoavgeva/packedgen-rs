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
    #[cfg(not(feature = "miss-optimized-frozen"))]
    entries: Box<[FrozenEntry<V>]>,
    #[cfg(feature = "miss-optimized-frozen")]
    tags: Box<[u8]>,
    #[cfg(feature = "miss-optimized-frozen")]
    locators: Box<[CompactFrozenLocator]>,
    #[cfg(feature = "miss-optimized-frozen")]
    routes: Box<[u32]>,
    #[cfg(feature = "miss-optimized-frozen")]
    offsets: Box<[u16]>,
    #[cfg(feature = "miss-optimized-frozen")]
    values: Box<[V]>,
    index_bits_per_entry: f64,
    embedded_key_tag: bool,
}

#[cfg(not(feature = "miss-optimized-frozen"))]
struct FrozenEntry<V> {
    key: PackedKeyRef,
    value: V,
}

#[cfg(feature = "miss-optimized-frozen")]
#[derive(Clone, Copy)]
#[repr(transparent)]
struct CompactFrozenKeyRef([u8; 6]);

#[cfg(feature = "miss-optimized-frozen")]
#[derive(Clone, Copy)]
#[repr(transparent)]
struct CompactFrozenLocator([u8; 5]);

#[cfg(feature = "miss-optimized-frozen")]
struct SplitCompactEntries<V> {
    tags: Box<[u8]>,
    locators: Box<[CompactFrozenLocator]>,
    routes: Box<[u32]>,
    offsets: Box<[u16]>,
    values: Box<[V]>,
}

#[cfg(feature = "miss-optimized-frozen")]
impl CompactFrozenLocator {
    const SEGMENT_MASK: u64 = (1_u64 << 15) - 1;

    fn new(reference: PackedKeyRef) -> Self {
        let reference = reference.without_embedded_tag();
        let offset = if reference.is_empty() {
            // Any position in the segment resolves the same empty slice.
            // Normalization covers an empty key appended at offset 64 KiB.
            0
        } else {
            reference.offset()
        };
        let offset =
            u16::try_from(offset).expect("64-KiB frozen arena offsets fit compact locators");
        let length = u8::try_from(reference.len()).expect("tagged frozen key length fits u8");
        let raw =
            u64::from(offset) | (u64::from(reference.segment()) << 16) | (u64::from(length) << 31);
        let raw = raw.to_le_bytes();
        Self([raw[0], raw[1], raw[2], raw[3], raw[4]])
    }

    fn expand(self) -> PackedKeyRef {
        let raw = u64::from_le_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], 0, 0, 0,
        ]);
        let offset = raw & 0xffff;
        let segment = (raw >> 16) & Self::SEGMENT_MASK;
        let length = raw >> 31;
        PackedKeyRef::from_raw(offset | (segment << 32) | (length << 47))
    }
}

#[cfg(feature = "miss-optimized-frozen")]
impl CompactFrozenKeyRef {
    const SEGMENT_MASK: u32 = (1_u32 << 15) - 1;

    fn new(reference: PackedKeyRef, embedded_key_tag: bool) -> Self {
        let untagged = if embedded_key_tag {
            reference.without_embedded_tag()
        } else {
            reference
        };
        let offset = if untagged.is_empty() {
            // Any position in the segment resolves the same empty slice.
            // Normalization covers an empty key appended at offset 64 KiB.
            0
        } else {
            reference.offset()
        };
        let offset =
            u16::try_from(offset).expect("64-KiB frozen arena offsets fit compact references");

        let length_and_tag =
            u32::try_from(reference.raw() >> 47).expect("17-bit frozen length and tag fit u32");
        let route = u32::from(reference.segment()) | (length_and_tag << 15);
        let route = route.to_le_bytes();
        let offset = offset.to_le_bytes();
        Self([route[0], route[1], route[2], route[3], offset[0], offset[1]])
    }

    fn route(self) -> u32 {
        u32::from_le_bytes([self.0[0], self.0[1], self.0[2], self.0[3]])
    }

    fn offset(self) -> u16 {
        u16::from_le_bytes([self.0[4], self.0[5]])
    }

    #[cfg(test)]
    fn expand(self) -> PackedKeyRef {
        Self::expand_parts(self.route(), self.offset())
    }

    fn expand_parts(route: u32, offset: u16) -> PackedKeyRef {
        let segment = route & Self::SEGMENT_MASK;
        let length_and_tag = route >> 15;
        PackedKeyRef::from_raw(
            u64::from(offset) | (u64::from(segment) << 32) | (u64::from(length_and_tag) << 47),
        )
    }

    #[cfg(test)]
    fn embedded_tag(self) -> u16 {
        Self::embedded_tag_from_route(self.route())
    }

    #[cfg(test)]
    fn embedded_tag_from_route(route: u32) -> u16 {
        u16::try_from(route >> 23).expect("nine tag bits fit u16")
    }

    #[cfg(test)]
    fn without_embedded_tag(self) -> PackedKeyRef {
        self.expand().without_embedded_tag()
    }
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

    // Both statically selected storage layouts intentionally share one
    // construction algorithm. Keeping the cfg arms adjacent makes their
    // permutation and exactness invariants auditable without runtime dispatch.
    #[allow(clippy::too_many_lines)]
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
                #[cfg(not(feature = "miss-optimized-frozen"))]
                entries: Box::new([]),
                #[cfg(feature = "miss-optimized-frozen")]
                tags: Box::new([]),
                #[cfg(feature = "miss-optimized-frozen")]
                locators: Box::new([]),
                #[cfg(feature = "miss-optimized-frozen")]
                routes: Box::new([]),
                #[cfg(feature = "miss-optimized-frozen")]
                offsets: Box::new([]),
                #[cfg(feature = "miss-optimized-frozen")]
                values: Box::new([]),
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
            let staged = staged
                .into_iter()
                .zip(hashes)
                .map(|((key, value), digest)| {
                    let key = if embedded_key_tag {
                        key.with_embedded_tag(digest_tag(digest))
                    } else {
                        key
                    };
                    (key, value)
                })
                .collect::<Vec<_>>();
            #[cfg(not(feature = "miss-optimized-frozen"))]
            let entries = staged
                .into_iter()
                .map(|(key, value)| FrozenEntry { key, value })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            #[cfg(feature = "miss-optimized-frozen")]
            let split = Self::split_compact_entries(staged, embedded_key_tag);
            return Ok(Self {
                index: None,
                arena,
                #[cfg(not(feature = "miss-optimized-frozen"))]
                entries,
                #[cfg(feature = "miss-optimized-frozen")]
                tags: split.tags,
                #[cfg(feature = "miss-optimized-frozen")]
                locators: split.locators,
                #[cfg(feature = "miss-optimized-frozen")]
                routes: split.routes,
                #[cfg(feature = "miss-optimized-frozen")]
                offsets: split.offsets,
                #[cfg(feature = "miss-optimized-frozen")]
                values: split.values,
                index_bits_per_entry: 0.0,
                embedded_key_tag,
            });
        }

        let (index, index_bits_per_entry) = build_frozen_index(&hashes, backend)?;
        // A checked 32-bit slot halves this transient permutation buffer on
        // 64-bit hosts without changing the public map or retained layout.
        let mut destinations = Vec::new();
        destinations
            .try_reserve_exact(staged.len())
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut occupied = Vec::new();
        occupied
            .try_reserve_exact(staged.len())
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        occupied.resize(staged.len(), false);
        for ((key, _), digest) in staged.iter_mut().zip(&hashes) {
            let slot = frozen_index_slot(&index, digest);
            if slot >= occupied.len() || occupied[slot] {
                return Err(FrozenBuildError::IndexConstructionFailed);
            }
            occupied[slot] = true;
            destinations
                .push(u32::try_from(slot).map_err(|_| FrozenBuildError::IndexConstructionFailed)?);
            if embedded_key_tag {
                *key = key.with_embedded_tag(digest_tag(*digest));
            }
        }
        drop(occupied);
        drop(hashes);

        // The perfect hash maps the construction set onto a dense permutation.
        // Move each staged value to that slot in place instead of retaining a
        // second full tuple array and sorting it by destination.
        for source in 0..destinations.len() {
            while usize::try_from(destinations[source]).expect("u32 slot fits usize") != source {
                let destination =
                    usize::try_from(destinations[source]).expect("u32 slot fits usize");
                staged.swap(source, destination);
                destinations.swap(source, destination);
            }
        }
        #[cfg(not(feature = "miss-optimized-frozen"))]
        let entries = staged
            .into_iter()
            .map(|(key, value)| FrozenEntry { key, value })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        #[cfg(feature = "miss-optimized-frozen")]
        let split = Self::split_compact_entries(staged, embedded_key_tag);

        Ok(Self {
            index: Some(index),
            arena,
            #[cfg(not(feature = "miss-optimized-frozen"))]
            entries,
            #[cfg(feature = "miss-optimized-frozen")]
            tags: split.tags,
            #[cfg(feature = "miss-optimized-frozen")]
            locators: split.locators,
            #[cfg(feature = "miss-optimized-frozen")]
            routes: split.routes,
            #[cfg(feature = "miss-optimized-frozen")]
            offsets: split.offsets,
            #[cfg(feature = "miss-optimized-frozen")]
            values: split.values,
            index_bits_per_entry,
            embedded_key_tag,
        })
    }

    #[cfg(feature = "miss-optimized-frozen")]
    fn split_compact_entries(
        staged: Vec<(PackedKeyRef, V)>,
        embedded_key_tag: bool,
    ) -> SplitCompactEntries<V> {
        let mut tags = Vec::new();
        let mut locators = Vec::new();
        let mut routes = Vec::new();
        let mut offsets = Vec::new();
        let mut values = Vec::with_capacity(staged.len());
        if embedded_key_tag {
            tags.reserve_exact(staged.len());
            locators.reserve_exact(staged.len());
            for (key, value) in staged {
                tags.push(digest_tag8_from_reference(key));
                locators.push(CompactFrozenLocator::new(key));
                values.push(value);
            }
        } else {
            routes.reserve_exact(staged.len());
            offsets.reserve_exact(staged.len());
            for (key, value) in staged {
                let compact = CompactFrozenKeyRef::new(key, false);
                routes.push(compact.route());
                offsets.push(compact.offset());
                values.push(value);
            }
        }
        SplitCompactEntries {
            tags: tags.into_boxed_slice(),
            locators: locators.into_boxed_slice(),
            routes: routes.into_boxed_slice(),
            offsets: offsets.into_boxed_slice(),
            values: values.into_boxed_slice(),
        }
    }

    #[cfg(feature = "miss-optimized-frozen")]
    fn compact_reference(&self, slot: usize) -> Option<PackedKeyRef> {
        if self.embedded_key_tag {
            self.locators
                .get(slot)
                .copied()
                .map(CompactFrozenLocator::expand)
        } else {
            let route = *self.routes.get(slot)?;
            let offset = *self.offsets.get(slot)?;
            Some(CompactFrozenKeyRef::expand_parts(route, offset))
        }
    }

    /// Returns the value for `key`, verifying its original packed bytes.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_indexed(key).map(|(_, value)| value)
    }

    /// Returns the dense perfect-hash slot and value after exact verification.
    ///
    /// This stays crate-private because dense slots are an implementation
    /// detail, but lets generation overlays represent deletions with one bit
    /// per frozen entry instead of retaining another copy of each deleted key.
    #[inline]
    pub(crate) fn get_indexed(&self, key: &[u8]) -> Option<(usize, &V)> {
        self.get_indexed_with_digest(key, key_digest(key))
    }

    #[cfg(not(feature = "miss-optimized-frozen"))]
    // Upstream PtrHash leaves its small `index` wrapper uninlined. Keeping the
    // complete verified lookup in one caller recovers the safe-release cost.
    #[allow(clippy::inline_always)]
    #[inline(always)]
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

    #[cfg(feature = "miss-optimized-frozen")]
    // See the non-split layout above. This is a code-generation constraint,
    // not a safety dependency; exact key verification remains unchanged.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn get_indexed_with_digest(
        &self,
        key: &[u8],
        digest: Digest,
    ) -> Option<(usize, &V)> {
        let Some(index) = self.index.as_ref() else {
            return (0..self.values.len()).find_map(|slot| {
                (self
                    .compact_reference(slot)
                    .and_then(|stored| self.key_bytes(stored))
                    == Some(key))
                .then(|| (slot, &self.values[slot]))
            });
        };
        let slot = frozen_index_slot(index, &digest);
        let stored_key = if self.embedded_key_tag {
            if *self.tags.get(slot)? != digest_tag8(digest) {
                return None;
            }
            self.arena.get(self.locators.get(slot)?.expand())
        } else {
            let route = *self.routes.get(slot)?;
            let offset = *self.offsets.get(slot)?;
            self.arena
                .get(CompactFrozenKeyRef::expand_parts(route, offset))
        };
        (stored_key == Some(key)).then(|| (slot, &self.values[slot]))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn slot_matches(&self, slot: usize, key: &[u8]) -> bool {
        #[cfg(not(feature = "miss-optimized-frozen"))]
        return self
            .entries
            .get(slot)
            .is_some_and(|entry| self.key_bytes(entry.key) == Some(key));
        #[cfg(feature = "miss-optimized-frozen")]
        return self
            .compact_reference(slot)
            .is_some_and(|stored| self.key_bytes(stored) == Some(key));
    }

    pub(crate) fn for_each_key(&self, mut visit: impl FnMut(&[u8])) {
        self.for_each_entry(|key, _| visit(key));
    }

    pub(crate) fn for_each_entry(&self, mut visit: impl FnMut(&[u8], &V)) {
        self.for_each_indexed_entry(|_, key, value| visit(key, value));
    }

    pub(crate) fn for_each_indexed_entry(&self, mut visit: impl FnMut(usize, &[u8], &V)) {
        #[cfg(not(feature = "miss-optimized-frozen"))]
        for (slot, entry) in self.entries.iter().enumerate() {
            let key = self
                .key_bytes(entry.key)
                .expect("frozen entry key must resolve in its arena");
            visit(slot, key, &entry.value);
        }
        #[cfg(feature = "miss-optimized-frozen")]
        for (slot, value) in self.values.iter().enumerate() {
            let key = self
                .compact_reference(slot)
                .and_then(|stored| self.key_bytes(stored))
                .expect("frozen entry key must resolve in its arena");
            visit(slot, key, value);
        }
    }

    pub(crate) fn indexed_entry(&self, slot: usize) -> Option<(&[u8], &V)> {
        #[cfg(not(feature = "miss-optimized-frozen"))]
        {
            let entry = self.entries.get(slot)?;
            let key = self
                .key_bytes(entry.key)
                .expect("frozen entry key must resolve in its arena");
            Some((key, &entry.value))
        }
        #[cfg(feature = "miss-optimized-frozen")]
        {
            let stored = self.compact_reference(slot)?;
            let key = self
                .key_bytes(stored)
                .expect("frozen entry key must resolve in its arena");
            Some((key, &self.values[slot]))
        }
    }

    pub(crate) fn indexed_value(&self, slot: usize) -> Option<&V> {
        #[cfg(not(feature = "miss-optimized-frozen"))]
        {
            self.entries.get(slot).map(|entry| &entry.value)
        }
        #[cfg(feature = "miss-optimized-frozen")]
        {
            self.values.get(slot)
        }
    }

    /// Looks up a fixed batch of keys without allocating temporary storage.
    ///
    /// Perfect hashing gives each construction-set key a dense slot. For a
    /// batch, digest and slot computation is independent, so it is completed
    /// first to expose instruction-level parallelism. Candidates are then
    /// verified in input order: sorting random perfect-hash slots costs more
    /// than the locality it creates at storage-sized batches. Exact key-byte
    /// checks still preserve arbitrary-miss and digest-collision semantics.
    #[must_use]
    pub fn get_many<const N: usize>(&self, keys: [&[u8]; N]) -> [Option<&V>; N] {
        let Some(index) = self.index.as_ref() else {
            return keys.map(|key| self.get(key));
        };

        let digests = keys.map(key_digest);
        #[cfg(not(feature = "phast"))]
        let slots = digests.map(|digest| frozen_index_slot(index, &digest));
        #[cfg(feature = "phast")]
        let slots = digests.map(|digest| frozen_index_slot(index, &digest));

        let mut results = [None; N];
        #[cfg(not(feature = "miss-optimized-frozen"))]
        for input in 0..N {
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
        #[cfg(feature = "miss-optimized-frozen")]
        for input in 0..N {
            let slot = slots[input];
            let stored_key = if self.embedded_key_tag {
                if self.tags.get(slot).copied() != Some(digest_tag8(digests[input])) {
                    continue;
                }
                let Some(locator) = self.locators.get(slot).copied() else {
                    continue;
                };
                self.arena.get(locator.expand())
            } else {
                let Some(route) = self.routes.get(slot).copied() else {
                    continue;
                };
                let Some(offset) = self.offsets.get(slot).copied() else {
                    continue;
                };
                self.arena
                    .get(CompactFrozenKeyRef::expand_parts(route, offset))
            };
            if stored_key == Some(keys[input]) {
                results[input] = Some(&self.values[slot]);
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
        #[cfg(not(feature = "miss-optimized-frozen"))]
        {
            self.entries.len()
        }
        #[cfg(feature = "miss-optimized-frozen")]
        {
            if self.embedded_key_tag {
                debug_assert_eq!(self.tags.len(), self.values.len());
                debug_assert_eq!(self.locators.len(), self.values.len());
                debug_assert!(self.routes.is_empty());
                debug_assert!(self.offsets.is_empty());
            } else {
                debug_assert!(self.tags.is_empty());
                debug_assert!(self.locators.is_empty());
                debug_assert_eq!(self.routes.len(), self.values.len());
                debug_assert_eq!(self.offsets.len(), self.values.len());
            }
            self.values.len()
        }
    }

    /// Returns whether the frozen generation contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        #[cfg(not(feature = "miss-optimized-frozen"))]
        {
            self.entries.is_empty()
        }
        #[cfg(feature = "miss-optimized-frozen")]
        {
            debug_assert_eq!(self.len() == 0, self.values.is_empty());
            self.values.is_empty()
        }
    }

    /// Captures retained packed-key, slot, and perfect-index density.
    #[must_use]
    pub fn stats(&self) -> FrozenMapStats {
        FrozenMapStats {
            len: self.len(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            #[cfg(not(feature = "miss-optimized-frozen"))]
            slot_bytes: size_of_val(self.entries.as_ref()),
            #[cfg(feature = "miss-optimized-frozen")]
            slot_bytes: size_of_val(self.tags.as_ref())
                + size_of_val(self.locators.as_ref())
                + size_of_val(self.routes.as_ref())
                + size_of_val(self.offsets.as_ref())
                + size_of_val(self.values.as_ref()),
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

#[cfg(feature = "miss-optimized-frozen")]
fn digest_tag8(digest: Digest) -> u8 {
    u8::try_from(digest_tag(digest) & 0xff).expect("eight digest bits fit u8")
}

#[cfg(feature = "miss-optimized-frozen")]
fn digest_tag8_from_reference(reference: PackedKeyRef) -> u8 {
    u8::try_from((reference.raw() >> 55) & 0xff).expect("eight embedded tag bits fit u8")
}

#[cfg(not(feature = "phast"))]
// This wrapper otherwise blocks the generic PtrHash index from folding into
// the caller on the measured release toolchain.
#[allow(clippy::inline_always)]
#[inline(always)]
fn frozen_index_slot(index: &FrozenIndex, digest: &Digest) -> usize {
    index.index(digest)
}

#[cfg(feature = "phast")]
// Keep backend dispatch and the selected index calculation in one hot block.
#[allow(clippy::inline_always)]
#[inline(always)]
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

#[cfg(all(test, feature = "miss-optimized-frozen"))]
mod compact_reference_tests {
    use super::{
        CompactFrozenKeyRef, CompactFrozenLocator, PackedKeyArena, digest_tag8_from_reference,
    };

    #[test]
    fn compact_reference_preserves_full_segments_empty_keys_and_tags() {
        assert_eq!(size_of::<CompactFrozenKeyRef>(), 6);

        let mut arena = PackedKeyArena::new();
        let full_key = vec![0x5a; 1 << 16];
        let full = arena.insert(&full_key).unwrap();
        let empty_at_end = arena.insert(b"").unwrap();
        let next_segment = arena.insert(b"member").unwrap();

        let compact_full = CompactFrozenKeyRef::new(full, false);
        assert_eq!(arena.get(compact_full.expand()), Some(full_key.as_slice()));

        let tagged_empty = empty_at_end.with_embedded_tag(0x1ab);
        let compact_empty = CompactFrozenKeyRef::new(tagged_empty, true);
        assert_eq!(compact_empty.embedded_tag(), 0x1ab);
        assert_eq!(
            arena.get(compact_empty.without_embedded_tag()),
            Some(b"".as_slice())
        );

        let tagged_member = next_segment.with_embedded_tag(0x155);
        assert_eq!(digest_tag8_from_reference(tagged_member), 0x55);
        let compact_member = CompactFrozenKeyRef::new(tagged_member, true);
        assert_eq!(compact_member.embedded_tag(), 0x155);
        assert_eq!(
            arena.get(compact_member.without_embedded_tag()),
            Some(b"member".as_slice())
        );

        assert_eq!(size_of::<CompactFrozenLocator>(), 5);
        assert_eq!(
            arena.get(CompactFrozenLocator::new(tagged_empty).expand()),
            Some(b"".as_slice())
        );
        assert_eq!(
            arena.get(CompactFrozenLocator::new(tagged_member).expand()),
            Some(b"member".as_slice())
        );
    }
}
