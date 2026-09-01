//! Experimental non-minimal k-perfect frozen map.
//!
//! The construction is independently implemented from the algorithm described
//! by Koerkamp, Hermann, Sanders, and Walzer (2026). It intentionally uses only
//! safe Rust so the query path can be evaluated without importing the paper's
//! current nightly reference implementation.

use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "gxhash")]
use ptr_hash::hash::Gx128;
use ptr_hash::hash::KeyHasher;
#[cfg(not(feature = "gxhash"))]
use ptr_hash::hash::Xxh3_128;
use wide::u64x4;

use crate::atomic_value::{AtomicU64FrozenMap, NonMaxU64};
use crate::{
    AtomicGenerationBaseFilter, FrozenBuildError, FrozenIndexBackend, FrozenMapStats,
    PackedKeyArena, PackedKeyRef,
};

const BIN_KEYS: usize = 8;
const ALL_BIN_SLOTS: u8 = u8::MAX;
const BIN_PADDING: usize = 128;
const PILOT_SHIFT_MASK: usize = BIN_PADDING - 1;
const PILOT_MIX: u64 = 0x517c_c1b7_2722_0a95;
const TARGET_BITS_PER_KEY: f64 = 0.89;
const EMPTY_VALUE: u64 = u64::MAX;

/// Experimental atomic frozen map using cache-line k-perfect bins.
#[doc(hidden)]
pub struct KPhfAtomicU64Map {
    index: KPtrIndex,
    arena: PackedKeyArena,
    keys: Box<[KeyBin]>,
    values: Box<[ValueBin]>,
    len: usize,
    embedded_key_tag: bool,
}

#[repr(align(64))]
struct KeyBin([PackedKeyRef; BIN_KEYS]);

#[repr(align(64))]
struct ValueBin([AtomicU64; BIN_KEYS]);

impl KeyBin {
    const fn empty() -> Self {
        Self([PackedKeyRef::EMPTY_SLOT; BIN_KEYS])
    }

    #[inline]
    fn matching_tag_mask(&self, tag: u16) -> u8 {
        let words = self.0.map(PackedKeyRef::raw);
        let tag_mask = u64x4::splat(PackedKeyRef::embedded_tag_word_mask());
        let expected = u64x4::splat(PackedKeyRef::embedded_tag_word(tag));
        let first =
            (u64x4::from([words[0], words[1], words[2], words[3]]) & tag_mask).cmp_eq(expected);
        let second =
            (u64x4::from([words[4], words[5], words[6], words[7]]) & tag_mask).cmp_eq(expected);
        lane_mask(first.to_array()) | (lane_mask(second.to_array()) << 4)
    }
}

impl ValueBin {
    fn empty() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(EMPTY_VALUE)))
    }
}

impl KPhfAtomicU64Map {
    /// Builds an exact static map from unique binary keys.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map error for duplicate digests, arena/allocation
    /// failure, or a k-PHF construction that cannot satisfy the bin bound.
    pub fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        let iterator = entries.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut arena = PackedKeyArena::new();
        let mut staged = Vec::new();
        staged
            .try_reserve(lower)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut digests = Vec::new();
        digests
            .try_reserve(lower)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut embedded_key_tag = true;

        for (key, value) in iterator {
            let key = key.as_ref();
            embedded_key_tag &= u8::try_from(key.len()).is_ok();
            let key_ref = arena.insert(key).map_err(FrozenBuildError::Arena)?;
            let digest = key_digest(key);
            digests.push(digest);
            staged.push((key_ref, value, digest));
        }

        let mut unique = digests.clone();
        unique.sort_unstable();
        if unique.windows(2).any(|window| window[0] == window[1]) {
            return Err(FrozenBuildError::IndexConstructionFailed);
        }

        let index = KPtrIndex::try_new(&digests, 1.0, TARGET_BITS_PER_KEY)
            .ok_or(FrozenBuildError::IndexConstructionFailed)?;
        let mut keys = (0..index.num_bins())
            .map(|_| KeyBin::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut values = (0..index.num_bins())
            .map(|_| ValueBin::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        for (key_ref, value, digest) in staged {
            let bin = index.get(digest);
            let slot = keys[bin]
                .0
                .iter()
                .position(|key| key.is_empty_slot())
                .ok_or(FrozenBuildError::IndexConstructionFailed)?;
            keys[bin].0[slot] = if embedded_key_tag {
                key_ref.with_embedded_tag(digest_tag(digest))
            } else {
                key_ref
            };
            values[bin].0[slot] = AtomicU64::new(value.get());
        }

        Ok(Self {
            index,
            arena,
            keys,
            values,
            len: digests.len(),
            embedded_key_tag,
        })
    }

    /// Returns the current value after exact key-byte verification.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<NonMaxU64> {
        if self.len == 0 {
            return None;
        }
        let digest = key_digest(key);
        let bin = self.index.get(digest);
        let mut candidates = if self.embedded_key_tag {
            self.keys[bin].matching_tag_mask(digest_tag(digest))
        } else {
            ALL_BIN_SLOTS
        };
        while candidates != 0 {
            let slot = candidates.trailing_zeros() as usize;
            candidates &= candidates - 1;
            let key_ref = self.keys[bin].0[slot];
            if key_ref.is_empty_slot() {
                continue;
            }
            let stored = if self.embedded_key_tag {
                self.arena.get(key_ref.without_embedded_tag())
            } else {
                self.arena.get(key_ref)
            };
            if stored == Some(key) {
                return NonMaxU64::new(self.values[bin].0[slot].load(Ordering::Acquire));
            }
        }
        None
    }

    /// Number of exact entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the map has no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Captures packed-key, bin, and k-PHF density.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn stats(&self) -> KPhfFrozenStats {
        let index_bits = self.index.bits_used();
        KPhfFrozenStats {
            len: self.len,
            bins: self.keys.len(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            slot_bytes: size_of_val(self.keys.as_ref())
                .saturating_add(size_of_val(self.values.as_ref())),
            index_bits_per_entry: if self.len == 0 {
                0.0
            } else {
                index_bits as f64 / self.len as f64
            },
            load_factor: if self.keys.is_empty() {
                0.0
            } else {
                self.len as f64 / (self.keys.len() * BIN_KEYS) as f64
            },
            bumped_entries: self.index.num_bumped(),
        }
    }
}

/// Experimental exact atomic PtrHash control with the same embedded tag.
#[doc(hidden)]
pub struct PtrHashAtomicU64Map {
    inner: AtomicU64FrozenMap,
}

impl PtrHashAtomicU64Map {
    /// Builds the `PtrHash` control.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error.
    pub fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: AtomicU64FrozenMap::try_from_entries_with_policy_and_index(
                entries,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
                FrozenIndexBackend::PtrHash,
            )?,
        })
    }

    /// Returns the current exact value.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.inner.with_value(key, copy_optional_non_max)
    }

    /// Captures the existing frozen-map components.
    #[must_use]
    pub fn stats(&self) -> FrozenMapStats {
        self.inner.stats()
    }
}

/// Retained-memory components for [`KPhfAtomicU64Map`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KPhfFrozenStats {
    /// Exact entries.
    pub len: usize,
    /// Cache-line bins including fallback and bounded padding.
    pub bins: usize,
    /// Packed arena allocation.
    pub arena_allocated_bytes: usize,
    /// Logical key bytes.
    pub arena_key_bytes: usize,
    /// Aligned key-reference and atomic-value bins.
    pub slot_bytes: usize,
    /// k-PHF pilot bits per exact entry.
    pub index_bits_per_entry: f64,
    /// Exact entries divided by total bin slots.
    pub load_factor: f64,
    /// Entries routed through recursive fallback indexes.
    pub bumped_entries: usize,
}

#[derive(Clone)]
struct KPtrIndex {
    entries: usize,
    primary_bins: usize,
    buckets: usize,
    seeds: Box<[u8]>,
    fallback: Option<Box<Self>>,
    salt: u64,
}

impl KPtrIndex {
    fn try_new(digests: &[u64], alpha: f64, bits_per_key: f64) -> Option<Self> {
        if digests.is_empty() {
            return Some(Self {
                entries: 0,
                primary_bins: 0,
                buckets: 0,
                seeds: Box::new([]),
                fallback: None,
                salt: 0,
            });
        }
        for attempt in 0..10_u64 {
            let salt = mix64(
                u64::try_from(digests.len()).ok()?
                    ^ attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    ^ bits_per_key.to_bits(),
            );
            if let Some(index) = Self::try_build(digests, alpha, bits_per_key, salt) {
                return Some(index);
            }
        }
        None
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::too_many_lines
    )]
    fn try_build(digests: &[u64], alpha: f64, bits_per_key: f64, salt: u64) -> Option<Self> {
        let entries = digests.len();
        let slots = (entries as f64 / alpha).ceil() as usize;
        let primary_bins = slots.div_ceil(BIN_KEYS).checked_add(BIN_PADDING)?;
        let target_bits = (entries as f64 * bits_per_key).ceil() as usize;
        let buckets = target_bits.div_ceil(8).max(1);

        let routed = digests
            .iter()
            .map(|digest| (hash_digest(*digest, salt), *digest))
            .collect::<Vec<_>>();
        let mut bucket_sizes = vec![0_usize; buckets + 1];
        for (hash, _) in &routed {
            bucket_sizes[to_bucket(*hash, buckets)] += 1;
        }
        let mut bucket_starts = bucket_sizes;
        let mut sum = 0;
        for start in &mut bucket_starts {
            let current = sum;
            sum += *start;
            *start = current;
        }
        let mut bucketed = vec![(0_u64, 0_u64); entries];
        for routed_key in routed {
            let bucket = to_bucket(routed_key.0, buckets);
            bucketed[bucket_starts[bucket]] = routed_key;
            bucket_starts[bucket] += 1;
        }
        for bucket in (1..=buckets).rev() {
            bucket_starts[bucket] = bucket_starts[bucket - 1];
        }
        bucket_starts[0] = 0;

        let mut order = (0..buckets).collect::<Vec<_>>();
        order.sort_unstable_by_key(|bucket| {
            core::cmp::Reverse(bucket_starts[*bucket + 1] - bucket_starts[*bucket])
        });
        let mut occupancy = vec![0_u8; primary_bins];
        let mut seeds = vec![255_u8; buckets];
        let mut bumped = Vec::new();
        let weights: [i64; BIN_KEYS] = std::array::from_fn(|index| 1_i64 << index);

        for (position, bucket) in order.into_iter().enumerate() {
            let members = &bucketed[bucket_starts[bucket]..bucket_starts[bucket + 1]];
            if members.is_empty() {
                continue;
            }
            let mut best = None;
            for seed in 1_u16..=u16::from(u8::MAX) {
                let mut bins = members
                    .iter()
                    .map(|(hash, _)| to_bin(*hash, u64::from(seed), primary_bins))
                    .collect::<Vec<_>>();
                bins.sort_unstable();
                let mut deltas = [0_i64; BIN_KEYS + 1];
                let mut valid = true;
                let mut cursor = 0;
                while cursor < bins.len() {
                    let bin = bins[cursor];
                    let mut end = cursor + 1;
                    while end < bins.len() && bins[end] == bin {
                        end += 1;
                    }
                    let count = end - cursor;
                    let occupied = usize::from(occupancy[bin]);
                    if occupied + count > BIN_KEYS {
                        valid = false;
                        break;
                    }
                    deltas[occupied] += 1;
                    deltas[occupied + count] -= 1;
                    cursor = end;
                }
                if !valid {
                    continue;
                }
                for index in 1..=BIN_KEYS {
                    deltas[index] += deltas[index - 1];
                }
                let score = deltas[..BIN_KEYS]
                    .iter()
                    .zip(weights)
                    .map(|(count, weight)| count * weight)
                    .sum::<i64>();
                if best.is_none_or(|(best_score, _)| score < best_score) {
                    best = Some((score, u8::try_from(seed).ok()?));
                }
            }

            let Some((_, seed)) = best else {
                if position == 0 {
                    return None;
                }
                seeds[bucket] = 0;
                bumped.extend(members.iter().map(|(_, digest)| *digest));
                continue;
            };
            seeds[bucket] = seed;
            for (hash, _) in members {
                let bin = to_bin(*hash, u64::from(seed), primary_bins);
                occupancy[bin] += 1;
            }
        }

        if bumped.len() == entries {
            return None;
        }
        let fallback = if bumped.is_empty() {
            None
        } else {
            Some(Box::new(Self::try_new(&bumped, 0.5, bits_per_key * 2.0)?))
        };
        Some(Self {
            entries,
            primary_bins,
            buckets,
            seeds: seeds.into_boxed_slice(),
            fallback,
            salt,
        })
    }

    #[inline]
    fn get(&self, digest: u64) -> usize {
        if self.entries == 0 {
            return 0;
        }
        let hash = hash_digest(digest, self.salt);
        let bucket = to_bucket(hash, self.buckets);
        let seed = self.seeds[bucket];
        if seed == 0 {
            self.primary_bins
                + self
                    .fallback
                    .as_ref()
                    .expect("zero pilot requires a fallback index")
                    .get(digest)
        } else {
            to_bin(hash, u64::from(seed), self.primary_bins)
        }
    }

    fn num_bins(&self) -> usize {
        self.primary_bins
            .saturating_add(self.fallback.as_ref().map_or(0, |index| index.num_bins()))
    }

    fn bits_used(&self) -> usize {
        self.seeds
            .len()
            .saturating_mul(8)
            .saturating_add(self.fallback.as_ref().map_or(0, |index| index.bits_used()))
    }

    fn num_bumped(&self) -> usize {
        self.fallback.as_ref().map_or(0, |index| index.entries)
    }
}

#[inline]
fn hash_digest(digest: u64, salt: u64) -> u64 {
    digest ^ salt
}

#[inline]
fn to_bucket(hash: u64, buckets: usize) -> usize {
    let square = multiply_high(hash, hash);
    let fourth = multiply_high(square, square);
    usize::try_from(multiply_high(
        fourth,
        u64::try_from(buckets).expect("bucket count fits u64"),
    ))
    .expect("reduced bucket fits usize")
}

#[inline]
fn to_bin(hash: u64, seed: u64, bins: usize) -> usize {
    let base_bins = bins - BIN_PADDING;
    let mixed = (hash ^ (seed >> 7)).wrapping_mul(PILOT_MIX);
    let base = usize::try_from(multiply_high(
        mixed,
        u64::try_from(base_bins).expect("bin count fits u64"),
    ))
    .expect("reduced bin fits usize");
    base + (usize::try_from(seed).expect("pilot fits usize") & PILOT_SHIFT_MASK)
}

#[inline]
fn multiply_high(left: u64, right: u64) -> u64 {
    ((u128::from(left) * u128::from(right)) >> 64) as u64
}

#[inline]
fn key_digest(key: &[u8]) -> u64 {
    #[cfg(feature = "gxhash")]
    let digest = <Gx128 as KeyHasher<[u8]>>::hash(key, 0);
    #[cfg(not(feature = "gxhash"))]
    let digest = <Xxh3_128 as KeyHasher<[u8]>>::hash(key, 0);
    let folded = digest ^ (digest >> 64);
    u64::try_from(folded & u128::from(u64::MAX)).expect("masked digest fits u64")
}

#[inline]
fn digest_tag(digest: u64) -> u16 {
    u16::try_from(digest & 0x1ff).expect("nine digest bits fit u16")
}

fn copy_optional_non_max(value: Option<&NonMaxU64>) -> Option<NonMaxU64> {
    value.copied()
}

fn lane_mask(lanes: [u64; 4]) -> u8 {
    u8::try_from(
        (lanes[0] >> 63)
            | ((lanes[1] >> 63) << 1)
            | ((lanes[2] >> 63) << 2)
            | ((lanes[3] >> 63) << 3),
    )
    .expect("four SIMD lanes fit u8")
}

#[inline]
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_line_bins_are_exactly_one_line_each() {
        assert_eq!(size_of::<KeyBin>(), 64);
        assert_eq!(align_of::<KeyBin>(), 64);
        assert_eq!(size_of::<ValueBin>(), 64);
        assert_eq!(align_of::<ValueBin>(), 64);
    }

    #[test]
    fn exact_hits_and_misses_round_trip() {
        let map = KPhfAtomicU64Map::try_from_entries(
            (0..10_000_u64).map(|value| (binary_key(value), NonMaxU64::new(value).unwrap())),
        )
        .unwrap();
        for value in 0..10_000_u64 {
            assert_eq!(
                map.get(&binary_key(value)),
                Some(NonMaxU64::new(value).unwrap())
            );
        }
        for value in 10_000..11_000_u64 {
            assert_eq!(map.get(&binary_key(value)), None);
        }
        assert_eq!(map.len(), 10_000);
        assert!(map.stats().load_factor > 0.8);
    }

    fn binary_key(value: u64) -> [u8; 32] {
        let mut key = [0_u8; 32];
        let mut state = value;
        for chunk in key.as_chunks_mut::<8>().0 {
            state = mix64(state);
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        key
    }
}
