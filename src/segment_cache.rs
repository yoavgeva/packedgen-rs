//! Experimental packed-segment cache layout.
//!
//! The table deliberately separates exact records from the lookup index. Its
//! default 64-byte bucket combines one atomic eight-byte control word with
//! eight immutable seven-byte fingerprint/location references. Keys and values
//! are stored exactly once in an immutable byte arena and are verified after a
//! fingerprint match. Online admission compaction may retain that arena as one
//! of several reference-counted payload segments without widening the index.
//!
//! This first stage is immutable after construction. Reads are lock-free and
//! safe to share between threads. A mutable cache can later publish small
//! atomic delta generations and fold them into this dense representation.

use std::fmt;
use std::hash::BuildHasher;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::GenerationHashBuilder;

const BUCKET_SLOTS: usize = 8;
const DEFAULT_TARGET_OCCUPIED_SLOTS: u8 = 7;
const LOCATION_BITS: u32 = 40;
const TAG_BITS: u32 = 23;
const TAG_SHIFT: u32 = LOCATION_BITS;
const LOCATION_MASK: u64 = (1_u64 << LOCATION_BITS) - 1;
const RECORD_SEGMENT_OFFSET_BITS: u32 = 32;
const RECORD_SEGMENT_OFFSET_MASK: usize = u32::MAX as usize;
const MAX_RECORD_SEGMENTS: usize = 1 << (LOCATION_BITS - RECORD_SEGMENT_OFFSET_BITS);
#[cfg(not(test))]
const TARGET_RECORD_SEGMENT_BYTES: usize = 96 * 1024 * 1024;
#[cfg(test)]
const TARGET_RECORD_SEGMENT_BYTES: usize = 4 * 1024;
const TAG_MASK: u64 = (1_u64 << TAG_BITS) - 1;
const ACCESSED_BIT: u64 = 1_u64 << 63;
const NEVER_EXPIRES: u16 = u16::MAX;
const MAX_EXPIRY_TICK: u16 = u16::MAX - 1;
const PACKED_REFERENCE_BYTES: usize = 7;
const NEGATIVE_FILTER_PREFIX_BUCKETS: usize = 2;
const BYTE_LOW_BITS: u64 = 0x0101_0101_0101_0101;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_LOW_SEVEN_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const PACKED_CONTROL_TAG_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;

const _: () = assert!(size_of::<AtomicU64>() == size_of::<u64>());
const _: () = assert!(align_of::<AtomicU64>() == align_of::<u64>());

#[derive(Clone, Copy)]
pub(crate) enum SegmentRemainingTtl {
    Never,
    Finite(Duration),
}

pub(crate) struct SegmentBaseRecord<'a> {
    pub(crate) slot: usize,
    pub(crate) location: usize,
    pub(crate) key_offset: usize,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
    pub(crate) remaining_ttl: Option<SegmentRemainingTtl>,
}

#[derive(Clone, Copy)]
pub(crate) struct SegmentRecordLocation {
    pub(crate) location: usize,
    pub(crate) header_bytes: usize,
}

impl SegmentRecordLocation {
    pub(crate) const fn segment(self) -> usize {
        self.location >> RECORD_SEGMENT_OFFSET_BITS
    }
}

pub(crate) type SegmentEntryVisitor<'a> = dyn FnMut(SegmentBaseRecord<'_>) + 'a;

/// Expiration encoding retained beside each packed record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentExpiry {
    /// Store no per-record expiration bytes.
    None,
    /// Store a 16-bit deadline relative to table construction.
    ///
    /// The largest finite deadline is 65,534 ticks. For example, one-second
    /// ticks cover about 18.2 hours and one-minute ticks cover about 45.5 days.
    Relative16 {
        /// Duration represented by one expiration tick.
        tick: Duration,
    },
}

impl SegmentExpiry {
    const fn bytes_per_record(self) -> usize {
        match self {
            Self::None => 0,
            Self::Relative16 { .. } => size_of::<u16>(),
        }
    }
}

/// Construction options for [`FrozenSegmentCache`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentCacheConfig {
    expiry: SegmentExpiry,
    target_occupied_slots: u8,
    packed_bucket_index: bool,
    negative_filter_bits_per_entry: u8,
}

impl SegmentCacheConfig {
    /// Creates a layout without per-entry expiration.
    #[must_use]
    pub const fn without_expiry() -> Self {
        Self {
            expiry: SegmentExpiry::None,
            target_occupied_slots: DEFAULT_TARGET_OCCUPIED_SLOTS,
            packed_bucket_index: true,
            negative_filter_bits_per_entry: 0,
        }
    }

    /// Creates a layout with a 16-bit relative expiration deadline.
    #[must_use]
    pub const fn with_relative_expiry(tick: Duration) -> Self {
        Self {
            expiry: SegmentExpiry::Relative16 { tick },
            target_occupied_slots: DEFAULT_TARGET_OCCUPIED_SLOTS,
            packed_bucket_index: true,
            negative_filter_bits_per_entry: 0,
        }
    }

    /// Sets the target number of occupied slots per eight-slot bucket.
    ///
    /// Lower occupancy spends more index bytes to shorten collision chains.
    /// Values from one through seven are valid; seven is the dense default.
    #[must_use]
    pub const fn with_target_bucket_occupancy(mut self, occupied: u8) -> Self {
        self.target_occupied_slots = occupied;
        self
    }

    /// Uses one control word and eight immutable seven-byte references per bucket.
    ///
    /// This retains the same 64-byte bucket size while allowing a lookup to
    /// filter all eight slots with one atomic control-word load. The packed
    /// references are suitable for immutable generations, not in-place updates.
    #[must_use]
    pub const fn with_packed_bucket_index(mut self) -> Self {
        self.packed_bucket_index = true;
        self
    }

    /// Uses the original independently atomic eight-byte word per slot.
    ///
    /// This exists as an experimental control. It supports future per-slot CAS
    /// mutation but costs more atomic loads during bucket scans.
    #[must_use]
    pub const fn with_atomic_word_index(mut self) -> Self {
        self.packed_bucket_index = false;
        self
    }

    /// Adds an immutable blocked Bloom filter for entries displaced beyond
    /// the unfiltered lookup prefix.
    ///
    /// Zero disables the filter. The filter is consulted only after the first
    /// two buckets miss, and positive answers still probe the exact index.
    #[must_use]
    pub const fn with_negative_filter_bits_per_entry(mut self, bits: u8) -> Self {
        self.negative_filter_bits_per_entry = bits;
        self
    }

    /// Returns the configured expiration representation.
    #[must_use]
    pub const fn expiry(self) -> SegmentExpiry {
        self.expiry
    }

    /// Returns the target number of occupied slots per eight-slot bucket.
    #[must_use]
    pub const fn target_bucket_occupancy(self) -> u8 {
        self.target_occupied_slots
    }

    /// Returns the requested immutable negative-filter density.
    #[must_use]
    pub const fn negative_filter_bits_per_entry(self) -> u8 {
        self.negative_filter_bits_per_entry
    }
}

impl Default for SegmentCacheConfig {
    fn default() -> Self {
        Self::without_expiry()
    }
}

/// Failure while constructing a packed segment cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentCacheBuildError {
    /// A retained allocation could not be reserved.
    AllocationFailed,
    /// The supplied exact key already exists.
    DuplicateKey,
    /// Entry or byte-count arithmetic overflowed `usize`.
    CapacityOverflow,
    /// The packed arena cannot be addressed by the 40-bit location field.
    ArenaTooLarge,
    /// A TTL was supplied to a cache configured without expiration bytes.
    ExpiryDisabled,
    /// A zero-duration expiration tick cannot encode deadlines.
    ZeroExpiryTick,
    /// A TTL cannot be represented by the 16-bit relative deadline.
    ExpiryOutOfRange,
    /// Target bucket occupancy must leave at least one empty slot.
    InvalidBucketOccupancy,
}

impl fmt::Display for SegmentCacheBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AllocationFailed => "packed segment allocation failed",
            Self::DuplicateKey => "packed segment input contains a duplicate key",
            Self::CapacityOverflow => "packed segment capacity arithmetic overflowed",
            Self::ArenaTooLarge => "packed segment arena exceeds its 40-bit address space",
            Self::ExpiryDisabled => "a TTL requires the relative-expiry layout",
            Self::ZeroExpiryTick => "relative-expiry tick must be nonzero",
            Self::ExpiryOutOfRange => "TTL exceeds the 16-bit relative-expiry horizon",
            Self::InvalidBucketOccupancy => "target bucket occupancy must be between one and seven",
        })
    }
}

impl std::error::Error for SegmentCacheBuildError {}

/// Retained layout accounting for [`FrozenSegmentCache`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentCacheStats {
    /// Number of exact records.
    pub entries: usize,
    /// Number of eight-slot lookup buckets.
    pub buckets: usize,
    /// Total physical index slots.
    pub index_slots: usize,
    /// Bytes occupied by the exact index and optional negative filter.
    pub index_bytes: usize,
    /// Bytes occupied by retained packed record segments, including headers.
    pub record_bytes: usize,
    /// Exact retained key bytes.
    pub logical_key_bytes: usize,
    /// Exact retained value bytes.
    pub logical_value_bytes: usize,
    /// Varint length and optional expiration bytes retained in records.
    pub record_header_bytes: usize,
}

impl SegmentCacheStats {
    /// Index plus record bytes, excluding fixed-size table fields and allocator metadata.
    #[must_use]
    pub const fn modeled_retained_bytes(self) -> usize {
        self.index_bytes + self.record_bytes
    }

    /// Retained bytes beyond exact key and value payloads.
    #[must_use]
    pub const fn modeled_overhead_bytes(self) -> usize {
        self.modeled_retained_bytes()
            .saturating_sub(self.logical_key_bytes + self.logical_value_bytes)
    }

    /// Physical slot utilization in basis points.
    #[must_use]
    pub fn index_load_bps(self) -> u16 {
        if self.index_slots == 0 {
            return 0;
        }
        let basis_points = self.entries.saturating_mul(10_000) / self.index_slots;
        u16::try_from(basis_points).unwrap_or(10_000)
    }
}

/// Immutable exact byte cache using packed records and an atomic compact index.
///
/// The current type proves steady-state density and lock-free shared reads. It
/// does not yet expose insertion, replacement, deletion, compaction, or bounded
/// eviction after construction.
pub struct FrozenSegmentCache {
    index: Box<[AtomicU64]>,
    packed_index: Box<[AtomicU64]>,
    negative_filter: Option<Box<[u64]>>,
    records: Arc<[u8]>,
    second_record_segment: Option<Arc<[u8]>>,
    additional_record_segments: Box<[Arc<[u8]>]>,
    hash_builder: GenerationHashBuilder,
    config: SegmentCacheConfig,
    started: Instant,
    stats: SegmentCacheStats,
}

impl FrozenSegmentCache {
    /// Packs an exact-size iterator of unique binary key/value records.
    ///
    /// Each tuple is `(key, value, ttl)`. TTL begins at construction time. A
    /// finite TTL requires [`SegmentExpiry::Relative16`].
    ///
    /// # Errors
    ///
    /// Returns [`SegmentCacheBuildError`] when capacity arithmetic or an
    /// allocation fails, keys are duplicated, or expiration configuration and
    /// supplied TTLs cannot be represented by the selected layout.
    pub fn try_from_entries<I, K, V>(
        config: SegmentCacheConfig,
        entries: I,
    ) -> Result<Self, SegmentCacheBuildError>
    where
        I: IntoIterator<Item = (K, V, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let iterator = entries.into_iter();
        let mut builder = SegmentCacheBuilder::try_new(config, iterator.len())?;
        for (key, value, ttl) in iterator {
            builder.push(key.as_ref(), value.as_ref(), ttl)?;
        }
        builder.finish()
    }

    /// Builds a segment with a caller-supplied hash state.
    ///
    /// This is an internal benchmarking hook for constructing byte-identical
    /// table geometry across competing wrappers. Production callers should use
    /// [`Self::try_from_entries`], which creates a fresh randomized state.
    #[doc(hidden)]
    pub fn try_from_entries_with_hash_builder<I, K, V>(
        config: SegmentCacheConfig,
        entries: I,
        hash_builder: GenerationHashBuilder,
    ) -> Result<Self, SegmentCacheBuildError>
    where
        I: IntoIterator<Item = (K, V, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let iterator = entries.into_iter();
        let mut builder = SegmentCacheBuilder::try_new_with_record_capacity_and_hash_builder(
            config,
            iterator.len(),
            0,
            hash_builder,
        )?;
        for (key, value, ttl) in iterator {
            builder.push(key.as_ref(), value.as_ref(), ttl)?;
        }
        builder.finish()
    }

    /// Returns a live value and records one access bit in its index word.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.get_at_tick_inner(key, self.current_tick(), true)
    }

    /// Returns a live value without modifying eviction-access state.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<&[u8]> {
        self.get_at_tick_inner(key, self.current_tick(), false)
    }

    /// Deterministic lookup at a caller-supplied relative expiration tick.
    ///
    /// This is useful for replay engines and expiration tests. It also records
    /// the access bit on a live hit.
    #[must_use]
    pub fn get_at_tick(&self, key: &[u8], now_tick: u64) -> Option<&[u8]> {
        self.get_at_tick_inner(key, now_tick, true)
    }

    pub(crate) fn key_hash(&self, key: &[u8]) -> u64 {
        self.hash_builder.hash_one(key)
    }

    pub(crate) fn hash_builder(&self) -> &GenerationHashBuilder {
        &self.hash_builder
    }

    pub(crate) fn probe_hashed(&self, key: &[u8], hash: u64) -> Option<SegmentBaseRecord<'_>> {
        self.probe_hashed_at_tick(key, hash, self.current_tick())
    }

    pub(crate) fn probe_hashed_at_tick(
        &self,
        key: &[u8],
        hash: u64,
        now_tick: u64,
    ) -> Option<SegmentBaseRecord<'_>> {
        let (slot, record) = self.find_record(key, hash)?;
        Some(SegmentBaseRecord {
            slot,
            location: record.location,
            key_offset: record.key_offset,
            key: record.key,
            value: record.value,
            remaining_ttl: remaining_ttl(self.config.expiry, record.expires_at, now_tick),
        })
    }

    pub(crate) fn mark_slot_accessed(&self, slot: usize) {
        self.mark_accessed(slot);
    }

    pub(crate) fn key_at_location(&self, location: usize) -> Option<&[u8]> {
        self.record_at_location(location).map(|record| record.key)
    }

    pub(crate) fn key_at_physical_slot(&self, slot: usize) -> Option<&[u8]> {
        self.record_for_physical_slot(slot).map(|record| record.key)
    }

    pub(crate) fn key_at_physical_slot_span(
        &self,
        slot: usize,
        key_offset: usize,
        key_len: usize,
    ) -> Option<&[u8]> {
        let location = self.location_for_physical_slot(slot)?;
        self.key_at_record_span(location, key_offset, key_len)
    }

    pub(crate) fn key_at_record_span(
        &self,
        record_location: usize,
        key_offset: usize,
        key_len: usize,
    ) -> Option<&[u8]> {
        let (records, local_location) = self.record_segment_at(record_location)?;
        let begin = local_location.checked_add(key_offset)?;
        records.get(begin..begin.checked_add(key_len)?)
    }

    /// Clears and returns the sampled-access bit for an exact key.
    #[must_use]
    pub fn take_accessed(&self, key: &[u8]) -> bool {
        let hash = self.hash_builder.hash_one(key);
        let Some((slot, _)) = self.find_record(key, hash) else {
            return false;
        };
        self.clear_accessed(slot)
    }

    /// Returns the number of exact records.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.stats.entries
    }

    /// Returns whether no records are retained.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns modeled retained-layout accounting.
    #[must_use]
    pub const fn stats(&self) -> SegmentCacheStats {
        self.stats
    }

    /// Returns the configured expiration encoding.
    #[must_use]
    pub const fn expiry(&self) -> SegmentExpiry {
        self.config.expiry
    }

    /// Returns the complete construction configuration.
    #[must_use]
    pub const fn config(&self) -> SegmentCacheConfig {
        self.config
    }

    #[allow(
        unsafe_code,
        reason = "reads checked immutable seven-byte packed references"
    )]
    pub(crate) fn for_each_physical_entry_at_tick(
        &self,
        now_tick: u64,
        visit: &mut SegmentEntryVisitor<'_>,
    ) {
        if self.config.packed_bucket_index {
            for bucket in 0..self.stats.buckets {
                let controls = self.packed_index[bucket].load(Ordering::Acquire);
                let begin = bucket * BUCKET_SLOTS;
                for offset in 0..BUCKET_SLOTS {
                    let control = u8::try_from(
                        (controls >> (offset * u8::BITS as usize)) & u64::from(u8::MAX),
                    )
                    .expect("masked packed control fits u8");
                    if control.trailing_zeros() >= 7 {
                        continue;
                    }
                    let slot = begin + offset;
                    // SAFETY: every occupied physical slot has one complete
                    // immutable seven-byte reference in the packed index.
                    let (_, location) = unsafe {
                        packed_reference_at_unchecked(&self.packed_index, self.stats.buckets, slot)
                    };
                    let Some(record) = self.record_at_location(location) else {
                        continue;
                    };
                    visit(SegmentBaseRecord {
                        slot,
                        location: record.location,
                        key_offset: record.key_offset,
                        key: record.key,
                        value: record.value,
                        remaining_ttl: remaining_ttl(
                            self.config.expiry,
                            record.expires_at,
                            now_tick,
                        ),
                    });
                }
            }
            return;
        }
        for slot in 0..self.stats.index_slots {
            let Some(record) = self.record_for_physical_slot(slot) else {
                continue;
            };
            visit(SegmentBaseRecord {
                slot,
                location: record.location,
                key_offset: record.key_offset,
                key: record.key,
                value: record.value,
                remaining_ttl: remaining_ttl(self.config.expiry, record.expires_at, now_tick),
            });
        }
    }

    fn record_for_physical_slot(&self, slot: usize) -> Option<RecordRef<'_>> {
        let location = self.location_for_physical_slot(slot)?;
        self.record_at_location(location)
    }

    fn location_for_physical_slot(&self, slot: usize) -> Option<usize> {
        if slot >= self.stats.index_slots {
            return None;
        }
        if !self.config.packed_bucket_index {
            let word = self.index[slot].load(Ordering::Acquire);
            if word == 0 {
                return None;
            }
            return Some(index_location(word));
        }
        let bucket = slot / BUCKET_SLOTS;
        let offset = slot % BUCKET_SLOTS;
        let controls = self.packed_index[bucket].load(Ordering::Acquire);
        let control = u8::try_from((controls >> (offset * 8)) & u64::from(u8::MAX))
            .expect("masked control fits u8");
        if control.trailing_zeros() >= 7 {
            return None;
        }
        packed_reference_at(&self.packed_index, self.stats.buckets, slot)
            .map(|(_, location)| location)
    }

    pub(crate) fn current_tick(&self) -> u64 {
        let SegmentExpiry::Relative16 { tick } = self.config.expiry else {
            return 0;
        };
        let tick_nanos = tick.as_nanos();
        debug_assert_ne!(tick_nanos, 0);
        let ticks = self.started.elapsed().as_nanos() / tick_nanos;
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }

    pub(crate) fn temporary_build_index_bytes(&self) -> usize {
        if self.config.packed_bucket_index {
            self.stats.buckets * size_of::<u64>()
        } else {
            self.stats.index_bytes
        }
    }

    fn get_at_tick_inner(&self, key: &[u8], now_tick: u64, record_access: bool) -> Option<&[u8]> {
        let hash = self.hash_builder.hash_one(key);
        self.get_at_tick_hashed_inner(key, hash, now_tick, record_access)
    }

    fn get_at_tick_hashed_inner(
        &self,
        key: &[u8],
        hash: u64,
        now_tick: u64,
        record_access: bool,
    ) -> Option<&[u8]> {
        let (slot, record) = self.find_record(key, hash)?;
        if record.expires_at != NEVER_EXPIRES && u64::from(record.expires_at) <= now_tick {
            return None;
        }
        if record_access {
            self.mark_accessed(slot);
        }
        Some(record.value)
    }

    #[inline]
    fn find_record(&self, key: &[u8], hash: u64) -> Option<(usize, RecordRef<'_>)> {
        if self.config.packed_bucket_index {
            return self.find_record_packed(key, hash);
        }
        let tag = hash_tag(hash);
        let mut bucket = reduce_hash(hash, self.stats.buckets);
        for probe in 0..self.stats.buckets {
            let begin = bucket * BUCKET_SLOTS;
            for offset in 0..BUCKET_SLOTS {
                let slot = begin + offset;
                let word = self.index[slot].load(Ordering::Acquire);
                if word == 0 {
                    return None;
                }
                if index_tag(word) != tag {
                    continue;
                }
                let record = self.record_at_location(index_location(word))?;
                if record.key == key {
                    return Some((slot, record));
                }
            }
            if probe + 1 == NEGATIVE_FILTER_PREFIX_BUCKETS
                && !negative_filter_may_contain(self.negative_filter.as_deref(), hash)
            {
                return None;
            }
            bucket += 1;
            if bucket == self.stats.buckets {
                bucket = 0;
            }
        }
        None
    }

    #[inline]
    #[allow(
        unsafe_code,
        reason = "reads checked immutable seven-byte packed references"
    )]
    fn find_record_packed(&self, key: &[u8], hash: u64) -> Option<(usize, RecordRef<'_>)> {
        let tag = hash_tag(hash);
        let control_tag = packed_control_tag(tag);
        let secondary_tag = packed_secondary_tag(tag);
        let repeated_tag = u64::from(control_tag) * BYTE_LOW_BITS;
        let mut bucket = reduce_hash(hash, self.stats.buckets);
        for probe in 0..self.stats.buckets {
            let controls = self.packed_index[bucket].load(Ordering::Acquire);
            let begin = bucket * BUCKET_SLOTS;
            let tags = controls & PACKED_CONTROL_TAG_BITS;
            let mut candidates = zero_byte_high_bits(tags ^ repeated_tag);
            while candidates != 0 {
                let offset = candidates.trailing_zeros() as usize / u8::BITS as usize;
                candidates &= candidates - 1;
                let slot = begin + offset;
                // SAFETY: `slot` names an occupied candidate in this bucket,
                // and every physical slot has one seven-byte reference.
                let (candidate_secondary, location) = unsafe {
                    packed_reference_at_unchecked(&self.packed_index, self.stats.buckets, slot)
                };
                if candidate_secondary != secondary_tag {
                    continue;
                }
                let record = self.record_at_location(location)?;
                if record.key == key {
                    return Some((slot, record));
                }
            }
            if zero_byte_high_bits(tags) != 0 {
                return None;
            }
            if probe + 1 == NEGATIVE_FILTER_PREFIX_BUCKETS
                && !negative_filter_may_contain(self.negative_filter.as_deref(), hash)
            {
                return None;
            }
            bucket += 1;
            if bucket == self.stats.buckets {
                bucket = 0;
            }
        }
        None
    }

    fn mark_accessed(&self, slot: usize) {
        if self.config.packed_bucket_index {
            let bucket = slot / BUCKET_SLOTS;
            let bit = 1_u64 << ((slot % BUCKET_SLOTS) * 8 + 7);
            self.packed_index[bucket].fetch_or(bit, Ordering::Relaxed);
        } else {
            self.index[slot].fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        }
    }

    fn clear_accessed(&self, slot: usize) -> bool {
        if self.config.packed_bucket_index {
            let bucket = slot / BUCKET_SLOTS;
            let bit = 1_u64 << ((slot % BUCKET_SLOTS) * 8 + 7);
            self.packed_index[bucket].fetch_and(!bit, Ordering::Relaxed) & bit != 0
        } else {
            self.index[slot].fetch_and(!ACCESSED_BIT, Ordering::Relaxed) & ACCESSED_BIT != 0
        }
    }

    #[inline]
    fn record_at_location(&self, location: usize) -> Option<RecordRef<'_>> {
        let (records, local_location) = self.record_segment_at(location)?;
        let mut record = record_at(records, self.config.expiry, local_location)?;
        record.location = location;
        Some(record)
    }

    #[inline]
    fn record_segment_at(&self, location: usize) -> Option<(&[u8], usize)> {
        let segment = location >> RECORD_SEGMENT_OFFSET_BITS;
        let offset = location & RECORD_SEGMENT_OFFSET_MASK;
        if segment == 0 {
            return Some((&self.records, offset));
        }
        if segment == 1 {
            return self
                .second_record_segment
                .as_deref()
                .map(|records| (records, offset));
        }
        self.additional_record_segments
            .get(segment.checked_sub(2)?)
            .map(|records| (records.as_ref(), offset))
    }

    pub(crate) fn record_segment_count(&self) -> usize {
        1 + usize::from(self.second_record_segment.is_some())
            + self.additional_record_segments.len()
    }

    pub(crate) fn record_segment(&self, segment: usize) -> Option<Arc<[u8]>> {
        if segment == 0 {
            return Some(Arc::clone(&self.records));
        }
        if segment == 1 {
            return self.second_record_segment.as_ref().map(Arc::clone);
        }
        self.additional_record_segments
            .get(segment.checked_sub(2)?)
            .map(Arc::clone)
    }

    pub(crate) fn record_segment_bytes(&self, segment: usize) -> Option<usize> {
        self.record_segment(segment).map(|records| records.len())
    }
}

fn zero_byte_high_bits(value: u64) -> u64 {
    !((value & BYTE_LOW_SEVEN_BITS).wrapping_add(BYTE_LOW_SEVEN_BITS) | value | BYTE_LOW_SEVEN_BITS)
        & BYTE_HIGH_BITS
}

fn negative_filter_words(
    entries: usize,
    bits_per_entry: u8,
) -> Result<Option<Vec<u64>>, SegmentCacheBuildError> {
    if entries == 0 || bits_per_entry == 0 {
        return Ok(None);
    }
    let bits = entries
        .checked_mul(usize::from(bits_per_entry))
        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
    let word_count = bits.div_ceil(u64::BITS as usize).max(1);
    let mut words = Vec::new();
    words
        .try_reserve_exact(word_count)
        .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
    words.resize(word_count, 0_u64);
    Ok(Some(words))
}

#[inline]
fn mark_negative_filter(filter: &mut Option<Vec<u64>>, hash: u64) {
    let Some(words) = filter.as_deref_mut() else {
        return;
    };
    let (word, mask) = negative_filter_location(words.len(), hash);
    words[word] |= mask;
}

#[inline]
fn negative_filter_may_contain(filter: Option<&[u64]>, hash: u64) -> bool {
    let Some(words) = filter else {
        return true;
    };
    let (word, mask) = negative_filter_location(words.len(), hash);
    words[word] & mask == mask
}

#[inline]
fn negative_filter_location(word_count: usize, hash: u64) -> (usize, u64) {
    let mixed = hash.rotate_left(27).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (hash >> 29);
    let word = reduce_hash(mixed, word_count);
    let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
    let mut mask = 1_u64 << first;
    let mut second = u32::try_from((mixed >> 21) & 63).expect("six hash bits fit u32");
    if second == first {
        second = (second + 1) & 63;
    }
    mask |= 1_u64 << second;
    let mut third = u32::try_from((mixed >> 42) & 63).expect("six hash bits fit u32");
    while mask & (1_u64 << third) != 0 {
        third = (third + 1) & 63;
    }
    mask |= 1_u64 << third;
    (word, mask)
}

pub(crate) struct SegmentCacheBuilder {
    config: SegmentCacheConfig,
    expected_entries: usize,
    inserted_entries: usize,
    bucket_count: usize,
    slot_count: usize,
    raw_index: Vec<u64>,
    negative_filter: Option<Vec<u64>>,
    records: Vec<u8>,
    hash_builder: GenerationHashBuilder,
    logical_key_bytes: usize,
    logical_value_bytes: usize,
    started: Instant,
}

pub(crate) struct SharedSegmentCacheBuild {
    pub(crate) cache: FrozenSegmentCache,
    pub(crate) reused_entries: usize,
    pub(crate) reused_record_bytes: usize,
    pub(crate) copied_record_bytes: usize,
    pub(crate) newly_allocated_bytes: usize,
}

/// Builds a new compact index while retaining immutable payload segments from
/// a predecessor. The 40-bit record location is split into an eight-bit local
/// segment ID and a 32-bit byte offset; the seven-byte packed index therefore
/// remains unchanged.
pub(crate) struct SharedSegmentCacheBuilder<'source> {
    source: &'source FrozenSegmentCache,
    config: SegmentCacheConfig,
    expected_entries: usize,
    inserted_entries: usize,
    bucket_count: usize,
    slot_count: usize,
    raw_index: Vec<u64>,
    negative_filter: Option<Vec<u64>>,
    copied_records: Vec<u8>,
    retained_segments: Vec<Arc<[u8]>>,
    source_segment_remap: Vec<Option<usize>>,
    copied_segment: usize,
    hash_builder: GenerationHashBuilder,
    logical_key_bytes: usize,
    logical_value_bytes: usize,
    record_header_bytes: usize,
    reused_entries: usize,
    reused_record_bytes: usize,
    source_now_tick: u64,
}

impl<'source> SharedSegmentCacheBuilder<'source> {
    pub(crate) fn try_new(
        source: &'source FrozenSegmentCache,
        entry_count: usize,
        copied_record_capacity: usize,
        used_source_segments: &[bool],
        hash_builder: GenerationHashBuilder,
        source_now_tick: u64,
    ) -> Result<Self, SegmentCacheBuildError> {
        validate_config(source.config)?;
        if used_source_segments.len() != source.record_segment_count() {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let target_occupied_slots = usize::from(source.config.target_occupied_slots);
        let bucket_count = entry_count.div_ceil(target_occupied_slots).max(1);
        let slot_count = bucket_count
            .checked_mul(BUCKET_SLOTS)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let mut raw_index = Vec::new();
        raw_index
            .try_reserve_exact(slot_count)
            .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
        raw_index.resize(slot_count, 0);
        let negative_filter =
            negative_filter_words(entry_count, source.config.negative_filter_bits_per_entry)?;

        let retained_count = used_source_segments.iter().filter(|used| **used).count();
        let segment_count = retained_count + usize::from(copied_record_capacity != 0);
        if segment_count.max(1) > MAX_RECORD_SEGMENTS {
            return Err(SegmentCacheBuildError::ArenaTooLarge);
        }
        let mut retained_segments = Vec::new();
        retained_segments
            .try_reserve_exact(retained_count)
            .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
        let mut source_segment_remap = vec![None; used_source_segments.len()];
        for (source_segment, used) in used_source_segments.iter().copied().enumerate() {
            if !used {
                continue;
            }
            let new_segment = retained_segments.len();
            source_segment_remap[source_segment] = Some(new_segment);
            retained_segments.push(
                source
                    .record_segment(source_segment)
                    .ok_or(SegmentCacheBuildError::CapacityOverflow)?,
            );
        }
        let copied_segment = retained_segments.len();
        let mut copied_records = Vec::new();
        copied_records
            .try_reserve_exact(copied_record_capacity)
            .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;

        Ok(Self {
            source,
            config: source.config,
            expected_entries: entry_count,
            inserted_entries: 0,
            bucket_count,
            slot_count,
            raw_index,
            negative_filter,
            copied_records,
            retained_segments,
            source_segment_remap,
            copied_segment,
            hash_builder,
            logical_key_bytes: 0,
            logical_value_bytes: 0,
            record_header_bytes: 0,
            reused_entries: 0,
            reused_record_bytes: 0,
            source_now_tick,
        })
    }

    pub(crate) fn push_reused(
        &mut self,
        key: &[u8],
        value: &[u8],
        source: SegmentRecordLocation,
    ) -> Result<(), SegmentCacheBuildError> {
        self.ensure_room()?;
        let source_segment = source.location >> RECORD_SEGMENT_OFFSET_BITS;
        let offset = source.location & RECORD_SEGMENT_OFFSET_MASK;
        let segment = self
            .source_segment_remap
            .get(source_segment)
            .copied()
            .flatten()
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        debug_assert!(
            self.source
                .key_at_location(source.location)
                .is_some_and(|stored| stored == key)
        );
        let location = segmented_location(segment, offset)?;
        self.push_index(key, location)?;
        self.account_record(key.len(), value.len(), source.header_bytes)?;
        self.reused_entries += 1;
        self.reused_record_bytes = self
            .reused_record_bytes
            .checked_add(source.header_bytes)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        Ok(())
    }

    pub(crate) fn push_copied(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), SegmentCacheBuildError> {
        self.ensure_room()?;
        let offset = self.copied_records.len();
        if offset > RECORD_SEGMENT_OFFSET_MASK {
            return Err(SegmentCacheBuildError::ArenaTooLarge);
        }
        let before = self.copied_records.len();
        append_record_at_tick(
            &mut self.copied_records,
            self.config.expiry,
            key,
            value,
            ttl,
            self.source_now_tick,
            RECORD_SEGMENT_OFFSET_MASK,
        )?;
        let encoded_bytes = self.copied_records.len() - before;
        let header_bytes = encoded_bytes
            .checked_sub(key.len() + value.len())
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let location = segmented_location(self.copied_segment, offset)?;
        self.push_index(key, location)?;
        self.account_record(key.len(), value.len(), header_bytes)
    }

    pub(crate) fn finish(mut self) -> Result<SharedSegmentCacheBuild, SegmentCacheBuildError> {
        if self.inserted_entries != self.expected_entries {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        self.copied_records.shrink_to_fit();
        let copied_record_bytes = self.copied_records.len();
        if copied_record_bytes != 0 || self.retained_segments.is_empty() {
            debug_assert_eq!(self.copied_segment, self.retained_segments.len());
            self.retained_segments
                .push(Arc::from(self.copied_records.into_boxed_slice()));
        }
        let record_bytes = self
            .retained_segments
            .iter()
            .try_fold(0_usize, |total, records| {
                total
                    .checked_add(records.len())
                    .ok_or(SegmentCacheBuildError::CapacityOverflow)
            })?;
        let exact_index_bytes = self
            .slot_count
            .checked_mul(size_of::<AtomicU64>())
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let negative_filter_bytes = self
            .negative_filter
            .as_ref()
            .map_or(0, |words| words.len() * size_of::<u64>());
        let index_bytes = exact_index_bytes
            .checked_add(negative_filter_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let (index, packed_index) = if self.config.packed_bucket_index {
            (
                Vec::new().into_boxed_slice(),
                finish_packed_index(self.raw_index, self.bucket_count)?,
            )
        } else {
            (
                self.raw_index
                    .into_iter()
                    .map(AtomicU64::new)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                Vec::new().into_boxed_slice(),
            )
        };
        let mut segments = self.retained_segments.into_iter();
        let records = segments
            .next()
            .unwrap_or_else(|| Arc::from(Box::<[u8]>::default()));
        let second_record_segment = segments.next();
        let additional_record_segments = segments.collect::<Vec<_>>().into_boxed_slice();
        let negative_filter = self.negative_filter.map(Vec::into_boxed_slice);
        let newly_allocated_bytes = index_bytes
            .checked_add(copied_record_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let cache = FrozenSegmentCache {
            index,
            packed_index,
            negative_filter,
            records,
            second_record_segment,
            additional_record_segments,
            hash_builder: self.hash_builder,
            config: self.config,
            started: self.source.started,
            stats: SegmentCacheStats {
                entries: self.expected_entries,
                buckets: self.bucket_count,
                index_slots: self.slot_count,
                index_bytes,
                record_bytes,
                logical_key_bytes: self.logical_key_bytes,
                logical_value_bytes: self.logical_value_bytes,
                record_header_bytes: self.record_header_bytes,
            },
        };
        Ok(SharedSegmentCacheBuild {
            cache,
            reused_entries: self.reused_entries,
            reused_record_bytes: self.reused_record_bytes,
            copied_record_bytes,
            newly_allocated_bytes,
        })
    }

    fn ensure_room(&self) -> Result<(), SegmentCacheBuildError> {
        if self.inserted_entries >= self.expected_entries {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        Ok(())
    }

    fn push_index(&mut self, key: &[u8], location: usize) -> Result<(), SegmentCacheBuildError> {
        let hash = self.hash_builder.hash_one(key);
        let slot = find_empty_build_slot(&self.raw_index, hash)?;
        self.raw_index[slot] = encode_index_word(hash, location)?;
        if bucket_displacement(
            reduce_hash(hash, self.bucket_count),
            slot / BUCKET_SLOTS,
            self.bucket_count,
        ) >= NEGATIVE_FILTER_PREFIX_BUCKETS
        {
            mark_negative_filter(&mut self.negative_filter, hash);
        }
        self.inserted_entries += 1;
        Ok(())
    }

    fn account_record(
        &mut self,
        key_bytes: usize,
        value_bytes: usize,
        header_bytes: usize,
    ) -> Result<(), SegmentCacheBuildError> {
        self.logical_key_bytes = self
            .logical_key_bytes
            .checked_add(key_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        self.logical_value_bytes = self
            .logical_value_bytes
            .checked_add(value_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        self.record_header_bytes = self
            .record_header_bytes
            .checked_add(header_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        Ok(())
    }
}

impl SegmentCacheBuilder {
    pub(crate) fn try_new(
        config: SegmentCacheConfig,
        entry_count: usize,
    ) -> Result<Self, SegmentCacheBuildError> {
        Self::try_new_with_record_capacity(config, entry_count, 0)
    }

    pub(crate) fn try_new_with_record_capacity(
        config: SegmentCacheConfig,
        entry_count: usize,
        record_capacity: usize,
    ) -> Result<Self, SegmentCacheBuildError> {
        Self::try_new_with_record_capacity_and_hash_builder(
            config,
            entry_count,
            record_capacity,
            GenerationHashBuilder::default(),
        )
    }

    pub(crate) fn try_new_with_record_capacity_and_hash_builder(
        config: SegmentCacheConfig,
        entry_count: usize,
        record_capacity: usize,
        hash_builder: GenerationHashBuilder,
    ) -> Result<Self, SegmentCacheBuildError> {
        validate_config(config)?;
        let target_occupied_slots = usize::from(config.target_occupied_slots);
        let bucket_count = entry_count.div_ceil(target_occupied_slots).max(1);
        let slot_count = bucket_count
            .checked_mul(BUCKET_SLOTS)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let mut raw_index = Vec::new();
        raw_index
            .try_reserve_exact(slot_count)
            .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
        raw_index.resize(slot_count, 0_u64);
        let negative_filter =
            negative_filter_words(entry_count, config.negative_filter_bits_per_entry)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(record_capacity)
            .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
        Ok(Self {
            config,
            expected_entries: entry_count,
            inserted_entries: 0,
            bucket_count,
            slot_count,
            raw_index,
            negative_filter,
            records,
            hash_builder,
            logical_key_bytes: 0,
            logical_value_bytes: 0,
            started: Instant::now(),
        })
    }

    pub(crate) fn encoded_record_bytes(
        config: SegmentCacheConfig,
        key_bytes: usize,
        value_bytes: usize,
    ) -> Result<usize, SegmentCacheBuildError> {
        varint_bytes(key_bytes)
            .checked_add(varint_bytes(value_bytes))
            .and_then(|size| size.checked_add(config.expiry.bytes_per_record()))
            .and_then(|size| size.checked_add(key_bytes))
            .and_then(|size| size.checked_add(value_bytes))
            .ok_or(SegmentCacheBuildError::CapacityOverflow)
    }

    pub(crate) fn push(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), SegmentCacheBuildError> {
        if self.inserted_entries >= self.expected_entries {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let hash = self.hash_builder.hash_one(key);
        let slot = find_build_slot(
            &self.raw_index,
            &self.records,
            self.config.expiry,
            key,
            hash,
        )?;
        let location = self.records.len();
        append_record(&mut self.records, self.config.expiry, key, value, ttl)?;
        self.raw_index[slot] = encode_index_word(hash, location)?;
        if bucket_displacement(
            reduce_hash(hash, self.bucket_count),
            slot / BUCKET_SLOTS,
            self.bucket_count,
        ) >= NEGATIVE_FILTER_PREFIX_BUCKETS
        {
            mark_negative_filter(&mut self.negative_filter, hash);
        }
        self.logical_key_bytes = self
            .logical_key_bytes
            .checked_add(key.len())
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        self.logical_value_bytes = self
            .logical_value_bytes
            .checked_add(value.len())
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        self.inserted_entries += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<FrozenSegmentCache, SegmentCacheBuildError> {
        if self.inserted_entries != self.expected_entries {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let record_bytes = self.records.len();
        let logical_bytes = self
            .logical_key_bytes
            .checked_add(self.logical_value_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let record_header_bytes = record_bytes
            .checked_sub(logical_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let exact_index_bytes = self
            .slot_count
            .checked_mul(size_of::<AtomicU64>())
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let negative_filter_bytes = self
            .negative_filter
            .as_ref()
            .map_or(0, |words| words.len() * size_of::<u64>());
        let index_bytes = exact_index_bytes
            .checked_add(negative_filter_bytes)
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        let record_segments = split_record_segments(&mut self.raw_index, self.records)?;
        let (index, packed_index) = if self.config.packed_bucket_index {
            (
                Vec::new().into_boxed_slice(),
                finish_packed_index(self.raw_index, self.bucket_count)?,
            )
        } else {
            (
                self.raw_index
                    .into_iter()
                    .map(AtomicU64::new)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                Vec::new().into_boxed_slice(),
            )
        };
        let mut record_segments = record_segments.into_iter();
        let records = record_segments
            .next()
            .unwrap_or_else(|| Arc::from(Box::<[u8]>::default()));
        let second_record_segment = record_segments.next();
        let additional_record_segments = record_segments.collect::<Vec<_>>().into_boxed_slice();
        let negative_filter = self.negative_filter.map(Vec::into_boxed_slice);
        Ok(FrozenSegmentCache {
            index,
            packed_index,
            negative_filter,
            records,
            second_record_segment,
            additional_record_segments,
            hash_builder: self.hash_builder,
            config: self.config,
            started: self.started,
            stats: SegmentCacheStats {
                entries: self.expected_entries,
                buckets: self.bucket_count,
                index_slots: self.slot_count,
                index_bytes,
                record_bytes,
                logical_key_bytes: self.logical_key_bytes,
                logical_value_bytes: self.logical_value_bytes,
                record_header_bytes,
            },
        })
    }
}

fn split_record_segments(
    index: &mut [u64],
    mut records: Vec<u8>,
) -> Result<Vec<Arc<[u8]>>, SegmentCacheBuildError> {
    if records.len() <= TARGET_RECORD_SEGMENT_BYTES {
        records.shrink_to_fit();
        return Ok(vec![Arc::from(records.into_boxed_slice())]);
    }

    let mut boundaries = vec![0_usize];
    loop {
        let target = boundaries
            .last()
            .copied()
            .and_then(|boundary| boundary.checked_add(TARGET_RECORD_SEGMENT_BYTES))
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        if target >= records.len() {
            break;
        }
        let Some(boundary) = index
            .iter()
            .copied()
            .filter(|word| *word != 0)
            .map(index_location)
            .filter(|location| *location >= target)
            .min()
        else {
            break;
        };
        if boundary == *boundaries.last().expect("record boundaries start at zero") {
            return Err(SegmentCacheBuildError::ArenaTooLarge);
        }
        boundaries.push(boundary);
        if boundaries.len() > MAX_RECORD_SEGMENTS {
            return Err(SegmentCacheBuildError::ArenaTooLarge);
        }
    }

    for word in index.iter_mut().filter(|word| **word != 0) {
        let location = index_location(*word);
        let segment = boundaries.partition_point(|boundary| *boundary <= location) - 1;
        let offset = location - boundaries[segment];
        let tag = index_tag(*word);
        *word = (tag << TAG_SHIFT) | encoded_location(segmented_location(segment, offset)?)?;
    }

    let mut tails = Vec::with_capacity(boundaries.len().saturating_sub(1));
    for boundary in boundaries.iter().copied().skip(1).rev() {
        tails.push(records.split_off(boundary));
    }
    records.shrink_to_fit();
    let mut segments = Vec::with_capacity(boundaries.len());
    segments.push(Arc::from(records.into_boxed_slice()));
    for mut tail in tails.into_iter().rev() {
        tail.shrink_to_fit();
        segments.push(Arc::from(tail.into_boxed_slice()));
    }
    Ok(segments)
}

struct RecordRef<'a> {
    location: usize,
    key_offset: usize,
    key: &'a [u8],
    value: &'a [u8],
    expires_at: u16,
}

fn validate_config(config: SegmentCacheConfig) -> Result<(), SegmentCacheBuildError> {
    if !(1..=7).contains(&config.target_occupied_slots) {
        return Err(SegmentCacheBuildError::InvalidBucketOccupancy);
    }
    if let SegmentExpiry::Relative16 { tick } = config.expiry
        && tick.is_zero()
    {
        return Err(SegmentCacheBuildError::ZeroExpiryTick);
    }
    Ok(())
}

fn find_build_slot(
    index: &[u64],
    records: &[u8],
    expiry: SegmentExpiry,
    key: &[u8],
    hash: u64,
) -> Result<usize, SegmentCacheBuildError> {
    let buckets = index.len() / BUCKET_SLOTS;
    let tag = hash_tag(hash);
    let mut bucket = reduce_hash(hash, buckets);
    for _ in 0..buckets {
        let begin = bucket * BUCKET_SLOTS;
        for offset in 0..BUCKET_SLOTS {
            let slot = begin + offset;
            let word = index[slot];
            if word == 0 {
                return Ok(slot);
            }
            if index_tag(word) == tag
                && record_at(records, expiry, index_location(word))
                    .is_some_and(|record| record.key == key)
            {
                return Err(SegmentCacheBuildError::DuplicateKey);
            }
        }
        bucket += 1;
        if bucket == buckets {
            bucket = 0;
        }
    }
    Err(SegmentCacheBuildError::CapacityOverflow)
}

fn find_empty_build_slot(index: &[u64], hash: u64) -> Result<usize, SegmentCacheBuildError> {
    let buckets = index.len() / BUCKET_SLOTS;
    let mut bucket = reduce_hash(hash, buckets);
    for _ in 0..buckets {
        let begin = bucket * BUCKET_SLOTS;
        for offset in 0..BUCKET_SLOTS {
            let slot = begin + offset;
            if index[slot] == 0 {
                return Ok(slot);
            }
        }
        bucket += 1;
        if bucket == buckets {
            bucket = 0;
        }
    }
    Err(SegmentCacheBuildError::CapacityOverflow)
}

fn append_record(
    records: &mut Vec<u8>,
    expiry: SegmentExpiry,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> Result<(), SegmentCacheBuildError> {
    append_record_at_tick(
        records,
        expiry,
        key,
        value,
        ttl,
        0,
        usize::try_from(LOCATION_MASK).unwrap_or(usize::MAX),
    )
}

#[allow(clippy::too_many_arguments)]
fn append_record_at_tick(
    records: &mut Vec<u8>,
    expiry: SegmentExpiry,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
    now_tick: u64,
    location_limit: usize,
) -> Result<(), SegmentCacheBuildError> {
    let expires_at = encode_expiry_at_tick(expiry, ttl, now_tick)?;
    let required = varint_bytes(key.len())
        .checked_add(varint_bytes(value.len()))
        .and_then(|size| size.checked_add(expiry.bytes_per_record()))
        .and_then(|size| size.checked_add(key.len()))
        .and_then(|size| size.checked_add(value.len()))
        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
    let final_size = records
        .len()
        .checked_add(required)
        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
    if final_size >= location_limit {
        return Err(SegmentCacheBuildError::ArenaTooLarge);
    }
    records
        .try_reserve(required)
        .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
    write_varint(records, key.len());
    write_varint(records, value.len());
    if matches!(expiry, SegmentExpiry::Relative16 { .. }) {
        records.extend_from_slice(&expires_at.to_le_bytes());
    }
    records.extend_from_slice(key);
    records.extend_from_slice(value);
    Ok(())
}

fn encode_expiry_at_tick(
    expiry: SegmentExpiry,
    ttl: Option<Duration>,
    now_tick: u64,
) -> Result<u16, SegmentCacheBuildError> {
    match (expiry, ttl) {
        (SegmentExpiry::None | SegmentExpiry::Relative16 { .. }, None) => Ok(NEVER_EXPIRES),
        (SegmentExpiry::None, Some(_)) => Err(SegmentCacheBuildError::ExpiryDisabled),
        (SegmentExpiry::Relative16 { tick }, Some(ttl)) => {
            let tick_nanos = tick.as_nanos();
            if tick_nanos == 0 {
                return Err(SegmentCacheBuildError::ZeroExpiryTick);
            }
            let ticks = ttl.as_nanos().div_ceil(tick_nanos);
            let deadline = u128::from(now_tick)
                .checked_add(ticks)
                .ok_or(SegmentCacheBuildError::ExpiryOutOfRange)?;
            if deadline > u128::from(MAX_EXPIRY_TICK) {
                return Err(SegmentCacheBuildError::ExpiryOutOfRange);
            }
            u16::try_from(deadline).map_err(|_| SegmentCacheBuildError::ExpiryOutOfRange)
        }
    }
}

fn remaining_ttl(
    expiry: SegmentExpiry,
    expires_at: u16,
    now_tick: u64,
) -> Option<SegmentRemainingTtl> {
    if expires_at == NEVER_EXPIRES {
        return Some(SegmentRemainingTtl::Never);
    }
    if u64::from(expires_at) <= now_tick {
        return None;
    }
    let SegmentExpiry::Relative16 { tick } = expiry else {
        return Some(SegmentRemainingTtl::Never);
    };
    let remaining = u64::from(expires_at) - now_tick;
    let remaining = u32::try_from(remaining).ok()?;
    tick.checked_mul(remaining).map(SegmentRemainingTtl::Finite)
}

#[inline]
fn record_at(records: &[u8], expiry: SegmentExpiry, location: usize) -> Option<RecordRef<'_>> {
    let key_header = *records.get(location)?;
    let value_header = *records.get(location.checked_add(1)?)?;
    if key_header < 0x80 && value_header < 0x80 {
        return record_at_lengths(
            records,
            expiry,
            location,
            location.checked_add(2)?,
            usize::from(key_header),
            usize::from(value_header),
        );
    }

    let mut cursor = location;
    let key_len = read_varint(records, &mut cursor)?;
    let value_len = read_varint(records, &mut cursor)?;
    record_at_lengths(records, expiry, location, cursor, key_len, value_len)
}

#[inline]
fn record_at_lengths(
    records: &[u8],
    expiry: SegmentExpiry,
    location: usize,
    mut cursor: usize,
    key_len: usize,
    value_len: usize,
) -> Option<RecordRef<'_>> {
    let expires_at = match expiry {
        SegmentExpiry::None => NEVER_EXPIRES,
        SegmentExpiry::Relative16 { .. } => {
            let bytes = records.get(cursor..cursor.checked_add(size_of::<u16>())?)?;
            cursor += size_of::<u16>();
            u16::from_le_bytes(bytes.try_into().ok()?)
        }
    };
    let key_end = cursor.checked_add(key_len)?;
    let value_end = key_end.checked_add(value_len)?;
    Some(RecordRef {
        location,
        key_offset: cursor - location,
        key: records.get(cursor..key_end)?,
        value: records.get(key_end..value_end)?,
        expires_at,
    })
}

fn encode_index_word(hash: u64, location: usize) -> Result<u64, SegmentCacheBuildError> {
    Ok((hash_tag(hash) << TAG_SHIFT) | encoded_location(location)?)
}

fn encoded_location(location: usize) -> Result<u64, SegmentCacheBuildError> {
    let location = u64::try_from(location).map_err(|_| SegmentCacheBuildError::ArenaTooLarge)?;
    location
        .checked_add(1)
        .filter(|encoded| *encoded < LOCATION_MASK)
        .ok_or(SegmentCacheBuildError::ArenaTooLarge)
}

fn segmented_location(segment: usize, offset: usize) -> Result<usize, SegmentCacheBuildError> {
    if segment >= MAX_RECORD_SEGMENTS || offset > RECORD_SEGMENT_OFFSET_MASK {
        return Err(SegmentCacheBuildError::ArenaTooLarge);
    }
    segment
        .checked_shl(RECORD_SEGMENT_OFFSET_BITS)
        .and_then(|prefix| prefix.checked_add(offset))
        .ok_or(SegmentCacheBuildError::ArenaTooLarge)
}

#[allow(
    unsafe_code,
    reason = "compacts an exclusively owned build allocation in place"
)]
fn finish_packed_index(
    mut index: Vec<u64>,
    buckets: usize,
) -> Result<Box<[AtomicU64]>, SegmentCacheBuildError> {
    let expected_slots = buckets
        .checked_mul(BUCKET_SLOTS)
        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
    if index.len() != expected_slots {
        return Err(SegmentCacheBuildError::CapacityOverflow);
    }
    let mut controls = Vec::new();
    controls
        .try_reserve_exact(buckets)
        .map_err(|_| SegmentCacheBuildError::AllocationFailed)?;
    controls.resize(buckets, 0_u64);
    let controls_bytes = buckets
        .checked_mul(size_of::<u64>())
        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;

    for slot in (0..index.len()).rev() {
        let word = index[slot];
        let bucket = slot / BUCKET_SLOTS;
        let offset = slot % BUCKET_SLOTS;
        let reference_begin = controls_bytes
            .checked_add(
                slot.checked_mul(PACKED_REFERENCE_BYTES)
                    .ok_or(SegmentCacheBuildError::CapacityOverflow)?,
            )
            .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
        // SAFETY: the final reference lies inside the same allocation and
        // starts after this source word. Descending slot order therefore never
        // overwrites a source word that has not already been copied locally.
        let reference = unsafe {
            std::slice::from_raw_parts_mut(
                index.as_mut_ptr().cast::<u8>().add(reference_begin),
                PACKED_REFERENCE_BYTES,
            )
        };
        if word == 0 {
            reference.fill(0);
            continue;
        }
        let tag = index_tag(word);
        controls[bucket] |= u64::from(packed_control_tag(tag)) << (offset * 8);
        reference[..2].copy_from_slice(&packed_secondary_tag(tag).to_le_bytes());
        let location = word & LOCATION_MASK;
        for byte in 0..5 {
            reference[2 + byte] = u8::try_from((location >> (byte * 8)) & u64::from(u8::MAX))
                .expect("masked location byte fits u8");
        }
    }

    // SAFETY: controls occupy the now-obsolete first eighth of the build
    // words and do not overlap the packed reference region that follows.
    unsafe {
        ptr::copy_nonoverlapping(
            controls.as_ptr().cast::<u8>(),
            index.as_mut_ptr().cast::<u8>(),
            controls_bytes,
        );
    }
    Ok(atomic_index_from_words(index.into_boxed_slice()))
}

#[allow(
    unsafe_code,
    reason = "converts layout-compatible uniquely owned word storage"
)]
fn atomic_index_from_words(index: Box<[u64]>) -> Box<[AtomicU64]> {
    let len = index.len();
    let words = Box::into_raw(index);
    let pointer = words.cast::<u64>().cast::<AtomicU64>();
    let atomics = ptr::slice_from_raw_parts_mut(pointer, len);
    // SAFETY: `AtomicU64` and `u64` have identical size/alignment, every bit
    // pattern is valid, and the boxed allocation is uniquely owned.
    unsafe { Box::from_raw(atomics) }
}

#[allow(
    unsafe_code,
    reason = "calls the unchecked reader after a complete bounds check"
)]
fn packed_reference_at(index: &[AtomicU64], buckets: usize, slot: usize) -> Option<(u16, usize)> {
    let byte_offset = buckets
        .checked_mul(size_of::<AtomicU64>())?
        .checked_add(slot.checked_mul(PACKED_REFERENCE_BYTES)?)?;
    if byte_offset.checked_add(PACKED_REFERENCE_BYTES)? > size_of_val(index) {
        return None;
    }
    // SAFETY: the bounds check covers the seven-byte immutable reference.
    Some(unsafe { packed_reference_at_unchecked(index, buckets, slot) })
}

#[inline]
#[allow(unsafe_code, reason = "decodes one caller-validated packed reference")]
unsafe fn packed_reference_at_unchecked(
    index: &[AtomicU64],
    buckets: usize,
    slot: usize,
) -> (u16, usize) {
    let byte_offset = buckets * size_of::<AtomicU64>() + slot * PACKED_REFERENCE_BYTES;
    // SAFETY: callers prove that the complete reference is inside `index`.
    let reference = unsafe { index.as_ptr().cast::<u8>().add(byte_offset) };
    // SAFETY: the first two and next four bytes lie inside the reference and
    // unaligned reads are permitted for this packed representation.
    let secondary = u16::from_le(unsafe { ptr::read_unaligned(reference.cast::<u16>()) });
    // SAFETY: bytes two through five are within the same checked seven-byte
    // packed reference; this representation explicitly permits unaligned data.
    let low_location = u64::from(u32::from_le(unsafe {
        ptr::read_unaligned(reference.add(2).cast::<u32>())
    }));
    // SAFETY: byte six is the final byte in the seven-byte reference.
    let encoded_location = low_location | (u64::from(unsafe { *reference.add(6) }) << 32);
    debug_assert_ne!(encoded_location, 0);
    let location = usize::try_from(encoded_location - 1).expect("40-bit location fits usize");
    (secondary, location)
}

const fn hash_tag(hash: u64) -> u64 {
    (hash >> (64 - TAG_BITS)) & TAG_MASK
}

fn packed_control_tag(tag: u64) -> u8 {
    u8::try_from((tag & 0x7f) % 127 + 1).expect("packed control tag is in 1..=127")
}

fn packed_secondary_tag(tag: u64) -> u16 {
    u16::try_from(tag >> 7).expect("remaining 16 fingerprint bits fit u16")
}

const fn index_tag(word: u64) -> u64 {
    (word >> TAG_SHIFT) & TAG_MASK
}

fn index_location(word: u64) -> usize {
    usize::try_from((word & LOCATION_MASK) - 1).expect("40-bit location fits usize")
}

fn reduce_hash(hash: u64, buckets: usize) -> usize {
    let reduced =
        (u128::from(hash) * u128::try_from(buckets).expect("usize bucket count fits u128")) >> 64;
    usize::try_from(reduced).expect("reduced hash is below bucket count")
}

fn bucket_displacement(primary: usize, current: usize, buckets: usize) -> usize {
    if current >= primary {
        current - primary
    } else {
        buckets - primary + current
    }
}

fn varint_bytes(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn write_varint(output: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        output.push(u8::try_from(value & 0x7f).expect("masked varint byte fits u8") | 0x80);
        value >>= 7;
    }
    output.push(u8::try_from(value).expect("final varint byte fits u8"));
}

fn read_varint(input: &[u8], cursor: &mut usize) -> Option<usize> {
    let mut value = 0_usize;
    let mut shift = 0_u32;
    loop {
        let byte = *input.get(*cursor)?;
        *cursor = cursor.checked_add(1)?;
        let payload = usize::from(byte & 0x7f);
        value = value.checked_add(payload.checked_shl(shift)?)?;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift = shift.checked_add(7)?;
        if shift >= usize::BITS {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{
        BYTE_HIGH_BITS, FrozenSegmentCache, SegmentCacheBuildError, SegmentCacheConfig,
        SegmentExpiry, finish_packed_index, zero_byte_high_bits,
    };
    use std::time::Duration;

    #[test]
    fn packed_zero_byte_mask_is_exact() {
        for byte in u8::MIN..=u8::MAX {
            let value = u64::from_le_bytes([byte; 8]);
            let expected = if byte == 0 { BYTE_HIGH_BITS } else { 0 };
            assert_eq!(zero_byte_high_bits(value), expected);
        }
    }

    #[test]
    fn packed_index_rejects_mismatched_build_extent_before_raw_compaction() {
        assert_eq!(
            finish_packed_index(vec![0; 7], 1).unwrap_err(),
            SegmentCacheBuildError::CapacityOverflow
        );
        assert_eq!(
            finish_packed_index(vec![0; 9], 1).unwrap_err(),
            SegmentCacheBuildError::CapacityOverflow
        );
        assert_eq!(
            finish_packed_index(Vec::new(), usize::MAX).unwrap_err(),
            SegmentCacheBuildError::CapacityOverflow
        );
    }

    #[test]
    fn exact_binary_keys_and_values_round_trip() {
        let entries = (0_u16..2_000).map(|value| {
            let key = [value.to_le_bytes().as_slice(), &[0, 255]].concat();
            let payload = vec![u8::try_from(value & 255).unwrap(); usize::from(value % 257)];
            (key, payload, None)
        });
        let cache =
            FrozenSegmentCache::try_from_entries(SegmentCacheConfig::without_expiry(), entries)
                .unwrap();

        for value in 0_u16..2_000 {
            let key = [value.to_le_bytes().as_slice(), &[0, 255]].concat();
            assert_eq!(
                cache.peek(&key),
                Some(vec![u8::try_from(value & 255).unwrap(); usize::from(value % 257)].as_slice())
            );
        }
        assert!(cache.peek(b"absent").is_none());
    }

    #[test]
    fn duplicate_keys_are_rejected_after_exact_verification() {
        let error = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [
                (b"same".as_slice(), b"one".as_slice(), None),
                (b"same", b"two", None),
            ],
        )
        .err()
        .unwrap();
        assert_eq!(error, SegmentCacheBuildError::DuplicateKey);
    }

    #[test]
    fn relative_expiry_is_compact_and_exact() {
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1)),
            [
                (
                    b"short".as_slice(),
                    b"a".as_slice(),
                    Some(Duration::from_secs(2)),
                ),
                (b"forever".as_slice(), b"b".as_slice(), None),
            ],
        )
        .unwrap();

        assert_eq!(cache.get_at_tick(b"short", 1), Some(b"a".as_slice()));
        assert!(cache.get_at_tick(b"short", 2).is_none());
        assert_eq!(
            cache.get_at_tick(b"forever", u64::MAX),
            Some(b"b".as_slice())
        );
        assert!(matches!(cache.expiry(), SegmentExpiry::Relative16 { .. }));
    }

    #[test]
    fn access_bit_does_not_change_exact_location_or_value() {
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [(b"key".as_slice(), b"value".as_slice(), None)],
        )
        .unwrap();

        assert!(!cache.take_accessed(b"key"));
        assert_eq!(cache.get(b"key"), Some(b"value".as_slice()));
        assert!(cache.take_accessed(b"key"));
        assert!(!cache.take_accessed(b"key"));
        assert_eq!(cache.peek(b"key"), Some(b"value".as_slice()));
        assert!(!cache.take_accessed(b"key"));
    }

    #[test]
    fn packed_bucket_index_preserves_exactness_and_access_bits() {
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry().with_packed_bucket_index(),
            (0_usize..10_000).map(|index| {
                (
                    mixed_key(index),
                    u64::try_from(index).unwrap().to_le_bytes(),
                    None,
                )
            }),
        )
        .unwrap();

        for index in 0_usize..10_000 {
            let key = mixed_key(index);
            assert_eq!(
                cache.get(&key),
                Some(u64::try_from(index).unwrap().to_le_bytes().as_slice())
            );
            assert!(cache.take_accessed(&key));
            assert!(!cache.take_accessed(&key));
        }
        assert!(cache.peek(b"packed bucket miss").is_none());
    }

    #[test]
    fn immutable_negative_filter_has_no_false_negatives() {
        let entries = 10_000_usize;
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry().with_negative_filter_bits_per_entry(1),
            (0..entries).map(|index| {
                (
                    mixed_key(index),
                    u64::try_from(index).unwrap().to_le_bytes(),
                    None,
                )
            }),
        )
        .unwrap();

        for index in 0..entries {
            assert_eq!(
                cache.peek(&mixed_key(index)),
                Some(u64::try_from(index).unwrap().to_le_bytes().as_slice())
            );
        }
        for index in entries..entries * 2 {
            assert!(cache.peek(&mixed_key(index)).is_none());
        }
        assert_eq!(cache.config().negative_filter_bits_per_entry(), 1);
    }

    #[test]
    fn shared_reads_are_lock_free_and_exact() {
        let cache = Arc::new(
            FrozenSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                (0_usize..10_000).map(|value| {
                    let value = u64::try_from(value).unwrap();
                    (
                        value.to_le_bytes(),
                        value.wrapping_mul(3).to_le_bytes(),
                        None,
                    )
                }),
            )
            .unwrap(),
        );
        let joins = (0..8)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for value in (u64::try_from(worker).unwrap()..10_000).step_by(8) {
                        assert_eq!(
                            cache.get(&value.to_le_bytes()),
                            Some(value.wrapping_mul(3).to_le_bytes().as_slice())
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for join in joins {
            join.join().unwrap();
        }
    }

    #[test]
    fn modeled_mixed_layout_hits_the_density_budget() {
        let entries = 10_000_usize;
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1)),
            (0..entries).map(|index| {
                let key = mixed_key(index);
                (key, [0_u8; 64], Some(Duration::from_secs(3_600)))
            }),
        )
        .unwrap();
        let stats = cache.stats();
        #[allow(clippy::cast_precision_loss)]
        let overhead = stats.modeled_overhead_bytes() as f64 / entries as f64;

        assert_eq!(stats.logical_key_bytes, entries * 188 / 10);
        assert_eq!(stats.logical_value_bytes, entries * 64);
        assert_eq!(stats.record_header_bytes, entries * 4);
        assert_eq!(stats.index_load_bps(), 8_747);
        assert!(
            overhead < 13.15,
            "modeled overhead was {overhead:.3} B/entry"
        );
    }

    #[test]
    fn lower_bucket_occupancy_spends_memory_for_headroom() {
        let entries = 10_000_usize;
        let cache = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1))
                .with_target_bucket_occupancy(6),
            (0..entries).map(|index| {
                let key = mixed_key(index);
                (key, [0_u8; 64], Some(Duration::from_secs(3_600)))
            }),
        )
        .unwrap();
        let stats = cache.stats();
        #[allow(clippy::cast_precision_loss)]
        let overhead = stats.modeled_overhead_bytes() as f64 / entries as f64;

        assert_eq!(cache.stats().index_load_bps(), 7_498);
        assert!(
            overhead < 14.68,
            "six-of-eight overhead was {overhead:.3} B/entry"
        );
        for index in 0..entries {
            assert_eq!(cache.peek(&mixed_key(index)), Some([0_u8; 64].as_slice()));
        }
        assert!(cache.peek(b"six-of-eight miss").is_none());
    }

    #[test]
    fn invalid_bucket_occupancy_is_rejected() {
        for occupied in [0, 8] {
            let error = FrozenSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry().with_target_bucket_occupancy(occupied),
                std::iter::empty::<(&[u8], &[u8], Option<Duration>)>(),
            )
            .err()
            .unwrap();
            assert_eq!(error, SegmentCacheBuildError::InvalidBucketOccupancy);
        }
    }

    fn mixed_key(index: usize) -> Box<[u8]> {
        let bytes = match index % 100 {
            0..=39 => 8,
            40..=64 => 16,
            65..=79 => 24,
            80..=89 => 32,
            _ => 48,
        };
        let mut key = [0_u8; 48];
        let mut state = u64::try_from(index).unwrap();
        for chunk in key.chunks_exact_mut(8) {
            state = state
                .wrapping_add(0x9e37_79b9_7f4a_7c15)
                .rotate_left(17)
                .wrapping_mul(0xbf58_476d_1ce4_e5b9);
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        key[..bytes].into()
    }
}
