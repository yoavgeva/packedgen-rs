//! Mutable delta layered over a compact immutable segment-cache generation.

use std::fmt;
use std::hash::BuildHasher;
use std::hint::spin_loop;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption, Guard};
use parking_lot::Mutex;

use crate::cache_value::{
    CacheValueArena, CacheValueHandle, CacheValuePin, CacheValueRef, LiveCacheValueHandle,
    MAX_CACHE_VALUE_BYTES, RemovedCacheValueHandle,
};
use crate::segment_cache::{
    SegmentCacheBuilder, SegmentRecordLocation, SegmentRemainingTtl, SharedSegmentCacheBuilder,
};
use crate::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, FrozenBuildError, FrozenSegmentCache,
    InsertOutcome, LockFreeAtomicU64GenerationMap, NonMaxU64, SegmentCacheBuildError,
    SegmentCacheConfig, SegmentExpiry,
};

const NEVER_EXPIRES: u64 = u64::MAX;
const MAX_RELATIVE_TICKS: u64 = u16::MAX as u64 - 1;
const DELTA_FILTER_BITS_PER_KEY: usize = 8;
const PACKED_BASE_BUCKET_SLOTS: usize = 8;
const PACKED_BASE_TARGET_SLOTS: usize = 7;
const PACKED_BASE_WRITING: u8 = u8::MAX;
const BYTE_LOW_BITS: u64 = 0x0101_0101_0101_0101;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const IMMEDIATE_HANDLE_TAG_MASK: u64 = 0b11;
const TOMBSTONE_HANDLE_TAG: u64 = 0b10;
const TOMBSTONE_LOCATION_BITS: u32 = 40;
const TOMBSTONE_LOCATION_MASK: u64 = (1_u64 << TOMBSTONE_LOCATION_BITS) - 1;
const TOMBSTONE_KEY_OFFSET_BITS: u32 = 5;
const TOMBSTONE_KEY_OFFSET_MASK: u64 = (1_u64 << TOMBSTONE_KEY_OFFSET_BITS) - 1;
const TOMBSTONE_METADATA_SHIFT: u32 =
    IMMEDIATE_HANDLE_TAG_MASK.count_ones() + TOMBSTONE_LOCATION_BITS;

/// Decodes an unchanged handle loaded from a segment-cache index or packed
/// override slot. Keeping this private prevents unrelated safe crate code from
/// manufacturing a pointer-bearing cache value handle.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes reconstruction of segment-index value handles"
)]
fn cache_value_handle_from_index(value: NonMaxU64) -> CacheValueHandle {
    // SAFETY: callers pass values loaded unchanged from an index/override whose
    // writers publish only arena handles or validated immediate tombstones.
    unsafe { CacheValueHandle::from_index_value(value) }
}

/// Dereferences a value handle owned by this module's segment cache while
/// `pin` protects that cache's value arena.
///
/// # Safety
///
/// `handle` must be allocated by or loaded unchanged from the segment
/// generation that created `pin`, and it must remain live for the guard.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes the segment cache's same-arena pin/handle proof"
)]
unsafe fn protected_cache_value<'pin>(
    pin: &'pin CacheValuePin<'_>,
    handle: LiveCacheValueHandle,
) -> CacheValueRef<'pin> {
    // SAFETY: this private adapter is called only with handles allocated by or
    // loaded unchanged from indices owned by the same `SegmentDelta` arena as
    // `pin`. Exact removal keeps the allocation live through this guard.
    unsafe { pin.protect(handle) }.value()
}

/// Converts the live value displaced by an exact index mutation into its
/// unique reclamation capability.
///
/// # Safety
///
/// The caller must have made `handle` unreachable from every segment index and
/// must own its sole retirement responsibility.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes exact segment-index retirement ownership"
)]
unsafe fn removed_cache_value_after_exact_transfer(
    handle: LiveCacheValueHandle,
) -> RemovedCacheValueHandle {
    // SAFETY: callers use the helper only after exact replacement/removal, or
    // during exclusive generation teardown when no index can expose the value.
    unsafe { RemovedCacheValueHandle::from_exact_removal(handle) }
}
const TOMBSTONE_KEY_LENGTH_BITS: u32 =
    u64::BITS - TOMBSTONE_METADATA_SHIFT - TOMBSTONE_KEY_OFFSET_BITS;
const TOMBSTONE_MAX_DIRECT_KEY_LEN: usize = (1_usize << TOMBSTONE_KEY_LENGTH_BITS) - 2;
const ONLINE_WRITER_STRIPES: usize = 64;
const ONLINE_WRITER_CLOSED: usize = 1_usize << (usize::BITS - 1);
const ONLINE_WRITER_DIRECT_BASE: usize = 1_usize << (usize::BITS - 2);
const ONLINE_WRITER_COUNT_MASK: usize = !(ONLINE_WRITER_CLOSED | ONLINE_WRITER_DIRECT_BASE);
const MIN_ONLINE_DENSITY_COMPACTION_RECORDS: usize = 1_024;
const MAX_SHARED_RECORD_SEGMENTS: usize = 2;
#[cfg(not(test))]
const MAX_SHARED_PAYLOAD_WASTE_BYTES: usize = 64 * 1024;
#[cfg(test)]
const MAX_SHARED_PAYLOAD_WASTE_BYTES: usize = 64;
type OnlineSegmentEntryVisitor<'visit> =
    dyn FnMut(&[u8], &[u8], Option<Duration>, Option<SegmentRecordLocation>) + 'visit;

struct DeltaMembership {
    // This is only a negative-routing filter: stale zeroes cause an exact
    // fallback probe, and stale ones only cause extra work. Publication of the
    // referenced table entry carries the synchronization needed by readers.
    words: Box<[AtomicU64]>,
    word_mask: usize,
}

struct LazyDeltaMembership {
    words: OnceLock<Box<LazyDeltaMembershipWords>>,
    word_mask: usize,
}

struct LazyDeltaMembershipWords {
    words: Box<[AtomicU64]>,
}

const _: () = assert!(size_of::<LazyDeltaMembership>() <= size_of::<DeltaMembership>());

struct BaseChangeBitmap {
    // Like `DeltaMembership`, this bitmap is a routing hint rather than the
    // authority for record visibility. Exact index/control loads synchronize
    // publication, so relaxed bit updates are sufficient here.
    words: OnceLock<Box<[AtomicU64]>>,
}

struct PackedBaseOverrideIndex {
    capacity: usize,
    table: OnceLock<PackedBaseOverrideTable>,
}

struct PackedBaseOverrideTable {
    controls: Box<[AtomicU64]>,
    handles: Box<[AtomicU64]>,
    live_base_slots: OnceLock<Box<[AtomicU32]>>,
    bucket_count: usize,
    len: AtomicUsize,
}

enum PackedBaseInsert {
    Inserted,
    Replaced(CacheValueHandle),
    Full(CacheValueHandle),
}

impl PackedBaseOverrideIndex {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            table: OnceLock::new(),
        }
    }

    fn insert(&self, location: u32, key_hash: u64, handle: CacheValueHandle) -> PackedBaseInsert {
        self.table
            .get_or_init(|| PackedBaseOverrideTable::new(self.capacity))
            .insert(location, key_hash, handle)
    }

    fn get(
        &self,
        key: &[u8],
        key_hash: u64,
        base: &FrozenSegmentCache,
    ) -> Option<CacheValueHandle> {
        self.table.get()?.get(key, key_hash, base)
    }

    fn len(&self) -> usize {
        self.table
            .get()
            .map_or(0, |table| table.len.load(Ordering::Relaxed))
    }

    fn scan(&self, visit: &mut dyn FnMut(CacheValueHandle)) {
        let Some(table) = self.table.get() else {
            return;
        };
        for bucket in 0..table.controls.len() {
            let control = table.controls[bucket].load(Ordering::Acquire);
            for offset in 0..PACKED_BASE_BUCKET_SLOTS {
                let tag = packed_base_control_byte(control, offset);
                if tag != 0 && tag != PACKED_BASE_WRITING {
                    let raw = table.handles[bucket * PACKED_BASE_BUCKET_SLOTS + offset]
                        .load(Ordering::Acquire);
                    visit(cache_value_handle_from_raw(raw));
                }
            }
        }
    }
}

impl PackedBaseOverrideTable {
    fn new(capacity: usize) -> Self {
        let target_buckets = capacity.max(1).div_ceil(PACKED_BASE_TARGET_SLOTS).max(2);
        let target_buckets = next_prime(target_buckets);
        let slot_count = target_buckets
            .checked_mul(PACKED_BASE_BUCKET_SLOTS)
            .expect("base-override slot capacity overflow");
        Self {
            controls: (0..target_buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            handles: (0..slot_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            live_base_slots: OnceLock::new(),
            bucket_count: target_buckets,
            len: AtomicUsize::new(0),
        }
    }

    fn insert(&self, base_slot: u32, key_hash: u64, handle: CacheValueHandle) -> PackedBaseInsert {
        if is_tombstone_handle(handle) {
            self.insert_tombstone_entry_swar(base_slot, key_hash, handle)
        } else {
            self.insert_entry::<true>(base_slot, key_hash, handle)
        }
    }

    fn insert_entry<const LIVE: bool>(
        &self,
        base_slot: u32,
        key_hash: u64,
        handle: CacheValueHandle,
    ) -> PackedBaseInsert {
        debug_assert_eq!(LIVE, !is_tombstone_handle(handle));
        let tag = packed_base_tag(key_hash);
        let raw = handle.index_value().get();
        let primary = reduce_to(key_hash, self.bucket_count);
        let stride = reduce_to(key_hash.rotate_left(29), self.bucket_count - 1) + 1;
        let live_base_slots = if LIVE {
            Some(self.live_base_slots.get_or_init(|| {
                (0..self.handles.len())
                    .map(|_| AtomicU32::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            }))
        } else {
            None
        };

        'retry: loop {
            let mut bucket = primary;
            for _ in 0..self.bucket_count {
                let begin = bucket * PACKED_BASE_BUCKET_SLOTS;
                let control = self.controls[bucket].load(Ordering::Acquire);
                let mut empty = None;
                for offset in 0..PACKED_BASE_BUCKET_SLOTS {
                    let current_tag = packed_base_control_byte(control, offset);
                    if current_tag == PACKED_BASE_WRITING {
                        std::hint::spin_loop();
                        continue 'retry;
                    }
                    if current_tag == 0 {
                        empty.get_or_insert(offset);
                        continue;
                    }
                    if current_tag != tag {
                        continue;
                    }
                    let index = begin + offset;
                    let current = self.handles[index].load(Ordering::Acquire);
                    let current_handle = cache_value_handle_from_raw(current);
                    let matches_base = if is_tombstone_handle(current_handle) {
                        tombstone_base_slot(current_handle) == base_slot as usize
                    } else {
                        self.live_base_slots
                            .get()
                            .expect("a published live override initialized its slots")[index]
                            .load(Ordering::Relaxed)
                            == base_slot
                    };
                    if matches_base {
                        if let Some(slots) = live_base_slots {
                            slots[index].store(base_slot, Ordering::Relaxed);
                        }
                        match self.handles[index].compare_exchange(
                            current,
                            raw,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(previous) => {
                                return PackedBaseInsert::Replaced(cache_value_handle_from_raw(
                                    previous,
                                ));
                            }
                            Err(_) => continue 'retry,
                        }
                    }
                }
                if let Some(offset) = empty {
                    let shift = offset * u8::BITS as usize;
                    let writing = u64::from(PACKED_BASE_WRITING) << shift;
                    let claimed = control | writing;
                    if self.controls[bucket]
                        .compare_exchange(control, claimed, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue 'retry;
                    }
                    if let Some(slots) = live_base_slots {
                        slots[begin + offset].store(base_slot, Ordering::Relaxed);
                    }
                    self.handles[begin + offset].store(raw, Ordering::Relaxed);
                    let published =
                        (claimed & !(u64::from(u8::MAX) << shift)) | (u64::from(tag) << shift);
                    self.controls[bucket].store(published, Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return PackedBaseInsert::Inserted;
                }
                bucket += stride;
                if bucket >= self.bucket_count {
                    bucket -= self.bucket_count;
                }
            }
            return PackedBaseInsert::Full(handle);
        }
    }

    fn insert_tombstone_entry_swar(
        &self,
        base_slot: u32,
        key_hash: u64,
        handle: CacheValueHandle,
    ) -> PackedBaseInsert {
        debug_assert!(is_tombstone_handle(handle));
        let tag = packed_base_tag(key_hash);
        let raw = handle.index_value().get();
        let primary = reduce_to(key_hash, self.bucket_count);
        let stride = reduce_to(key_hash.rotate_left(29), self.bucket_count - 1) + 1;

        'retry: loop {
            let mut bucket = primary;
            for _ in 0..self.bucket_count {
                let begin = bucket * PACKED_BASE_BUCKET_SLOTS;
                let control = self.controls[bucket].load(Ordering::Acquire);
                let mut writing = matching_control_bytes(control, PACKED_BASE_WRITING);
                while writing != 0 {
                    let offset = writing.trailing_zeros() as usize / u8::BITS as usize;
                    writing &= writing - 1;
                    if packed_base_control_byte(control, offset) == PACKED_BASE_WRITING {
                        std::hint::spin_loop();
                        continue 'retry;
                    }
                }
                let mut candidates = matching_control_bytes(control, tag);
                while candidates != 0 {
                    let offset = candidates.trailing_zeros() as usize / u8::BITS as usize;
                    candidates &= candidates - 1;
                    if packed_base_control_byte(control, offset) != tag {
                        continue;
                    }
                    let index = begin + offset;
                    let current = self.handles[index].load(Ordering::Acquire);
                    let current_handle = cache_value_handle_from_raw(current);
                    let matches_base = if is_tombstone_handle(current_handle) {
                        tombstone_base_slot(current_handle) == base_slot as usize
                    } else {
                        self.live_base_slots
                            .get()
                            .expect("a published live override initialized its slots")[index]
                            .load(Ordering::Relaxed)
                            == base_slot
                    };
                    if matches_base {
                        match self.handles[index].compare_exchange(
                            current,
                            raw,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(previous) => {
                                return PackedBaseInsert::Replaced(cache_value_handle_from_raw(
                                    previous,
                                ));
                            }
                            Err(_) => continue 'retry,
                        }
                    }
                }
                let empty = zero_control_bytes(control);
                if empty != 0 {
                    let offset = empty.trailing_zeros() as usize / u8::BITS as usize;
                    debug_assert_eq!(packed_base_control_byte(control, offset), 0);
                    let shift = offset * u8::BITS as usize;
                    let writing = u64::from(PACKED_BASE_WRITING) << shift;
                    let claimed = control | writing;
                    if self.controls[bucket]
                        .compare_exchange(control, claimed, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue 'retry;
                    }
                    self.handles[begin + offset].store(raw, Ordering::Relaxed);
                    let published =
                        (claimed & !(u64::from(u8::MAX) << shift)) | (u64::from(tag) << shift);
                    self.controls[bucket].store(published, Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return PackedBaseInsert::Inserted;
                }
                bucket += stride;
                if bucket >= self.bucket_count {
                    bucket -= self.bucket_count;
                }
            }
            return PackedBaseInsert::Full(handle);
        }
    }

    fn get(
        &self,
        key: &[u8],
        key_hash: u64,
        base: &FrozenSegmentCache,
    ) -> Option<CacheValueHandle> {
        let tag = packed_base_tag(key_hash);
        let primary = reduce_to(key_hash, self.bucket_count);
        let stride = reduce_to(key_hash.rotate_left(29), self.bucket_count - 1) + 1;

        let mut bucket = primary;
        for _ in 0..self.bucket_count {
            let begin = bucket * PACKED_BASE_BUCKET_SLOTS;
            let control = self.controls[bucket].load(Ordering::Acquire);
            let mut candidates = matching_control_bytes(control, tag);
            while candidates != 0 {
                let offset = candidates.trailing_zeros() as usize / u8::BITS as usize;
                candidates &= candidates - 1;
                if packed_base_control_byte(control, offset) != tag {
                    continue;
                }
                let index = begin + offset;
                let raw = self.handles[index].load(Ordering::Acquire);
                let handle = cache_value_handle_from_raw(raw);
                let stored_key = if is_tombstone_handle(handle) {
                    tombstone_base_key(handle, base)
                } else {
                    let slot = usize::try_from(
                        self.live_base_slots
                            .get()
                            .expect("a published live override initialized its slots")[index]
                            .load(Ordering::Relaxed),
                    )
                    .expect("u32 base slot fits usize");
                    base.key_at_physical_slot(slot)
                };
                if stored_key.is_some_and(|stored| stored == key) {
                    return Some(handle);
                }
            }
            if zero_control_bytes(control) != 0 {
                return None;
            }
            bucket += stride;
            if bucket >= self.bucket_count {
                bucket -= self.bucket_count;
            }
        }
        None
    }
}

impl BaseChangeBitmap {
    fn try_new(slots: usize) -> Result<Self, FrozenBuildError> {
        let bitmap = Self {
            words: OnceLock::new(),
        };
        if slots != 0 {
            bitmap.initialize(slots)?;
        }
        Ok(bitmap)
    }

    fn initialize(&self, slots: usize) -> Result<(), FrozenBuildError> {
        if self.words.get().is_some() {
            return Ok(());
        }
        let word_count = slots.div_ceil(u64::BITS as usize);
        let mut words = Vec::new();
        words
            .try_reserve_exact(word_count)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        words.resize_with(word_count, || AtomicU64::new(0));
        let _ = self.words.set(words.into_boxed_slice());
        Ok(())
    }

    fn mark(&self, slot: usize) {
        let word = &self
            .words
            .get()
            .expect("base-change bitmap is initialized before direct writes")
            [slot / u64::BITS as usize];
        let mask = 1_u64 << (slot % u64::BITS as usize);
        if word.load(Ordering::Relaxed) & mask == 0 {
            word.fetch_or(mask, Ordering::Relaxed);
        }
    }

    fn contains(&self, slot: usize) -> bool {
        self.words.get().is_some_and(|words| {
            words[slot / u64::BITS as usize].load(Ordering::Relaxed)
                & (1_u64 << (slot % u64::BITS as usize))
                != 0
        })
    }

    fn bytes(&self) -> usize {
        self.words
            .get()
            .map_or(0, |words| words.len() * size_of::<AtomicU64>())
    }
}

impl DeltaMembership {
    fn try_new(capacity: usize) -> Result<Self, FrozenBuildError> {
        let desired_bits = capacity
            .checked_mul(DELTA_FILTER_BITS_PER_KEY)
            .ok_or(FrozenBuildError::AllocationFailed)?
            .max(u64::BITS as usize);
        let bits = desired_bits
            .checked_next_power_of_two()
            .ok_or(FrozenBuildError::AllocationFailed)?;
        let word_count = bits / u64::BITS as usize;
        let mut words = Vec::new();
        words
            .try_reserve_exact(word_count)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        words.resize_with(word_count, || AtomicU64::new(0));
        Ok(Self {
            words: words.into_boxed_slice(),
            word_mask: word_count - 1,
        })
    }

    fn mark(&self, hash: u64) {
        let (word, mask) = self.location(hash);
        let word = &self.words[word];
        if word.load(Ordering::Relaxed) & mask != mask {
            word.fetch_or(mask, Ordering::Relaxed);
        }
    }

    fn may_contain(&self, hash: u64) -> bool {
        let (word, mask) = self.location(hash);
        self.words[word].load(Ordering::Relaxed) & mask == mask
    }

    fn bytes(&self) -> usize {
        self.words.len() * size_of::<AtomicU64>()
    }

    fn location(&self, hash: u64) -> (usize, u64) {
        let mixed = hash.rotate_left(29).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (hash >> 23);
        let word = usize::try_from((hash >> 6) & u64::try_from(self.word_mask).unwrap_or(u64::MAX))
            .expect("masked membership word fits usize");
        let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
        let mut second = u32::try_from(mixed & 63).expect("six hash bits fit u32");
        if second == first {
            second = (second + 1) & 63;
        }
        (word, (1_u64 << first) | (1_u64 << second))
    }
}

impl LazyDeltaMembership {
    fn try_new(capacity: usize) -> Result<Self, FrozenBuildError> {
        let desired_bits = capacity
            .checked_mul(DELTA_FILTER_BITS_PER_KEY)
            .ok_or(FrozenBuildError::AllocationFailed)?
            .max(u64::BITS as usize);
        let bits = desired_bits
            .checked_next_power_of_two()
            .ok_or(FrozenBuildError::AllocationFailed)?;
        let word_count = bits / u64::BITS as usize;
        Ok(Self {
            words: OnceLock::new(),
            word_mask: word_count - 1,
        })
    }

    fn mark(&self, hash: u64) {
        let (word, mask) = self.location(hash);
        let word = &self
            .words
            .get_or_init(|| {
                Box::new(LazyDeltaMembershipWords {
                    words: (0..=self.word_mask)
                        .map(|_| AtomicU64::new(0))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                })
            })
            .words[word];
        if word.load(Ordering::Relaxed) & mask != mask {
            word.fetch_or(mask, Ordering::Relaxed);
        }
    }

    fn may_contain(&self, hash: u64) -> bool {
        let (word, mask) = self.location(hash);
        self.words
            .get()
            .is_some_and(|words| words.words[word].load(Ordering::Relaxed) & mask == mask)
    }

    fn bytes(&self) -> usize {
        self.words
            .get()
            .map_or(0, |words| words.words.len() * size_of::<AtomicU64>())
    }

    fn location(&self, hash: u64) -> (usize, u64) {
        let mixed = hash.rotate_left(29).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (hash >> 23);
        let word = usize::try_from((hash >> 6) & u64::try_from(self.word_mask).unwrap_or(u64::MAX))
            .expect("masked membership word fits usize");
        let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
        let mut second = u32::try_from(mixed & 63).expect("six hash bits fit u32");
        if second == first {
            second = (second + 1) & 63;
        }
        (word, (1_u64 << first) | (1_u64 << second))
    }
}

struct SegmentDelta {
    packed_base: PackedBaseOverrideIndex,
    base_index: LockFreeAtomicU64GenerationMap,
    new_index: LockFreeAtomicU64GenerationMap,
    arena: Arc<CacheValueArena>,
    base_changes: BaseChangeBitmap,
    base_membership: DeltaMembership,
    base_dirty: AtomicBool,
    base_index_dirty: AtomicBool,
    new_membership: LazyDeltaMembership,
    new_dirty: AtomicBool,
}

#[derive(Clone, Copy)]
struct BasePosition {
    slot: usize,
    key_offset: usize,
    key_len: usize,
}

enum DeltaTarget {
    Base(BasePosition),
    New,
}

impl SegmentDelta {
    fn try_new(
        capacity: usize,
        base_slots: usize,
        arena: Arc<CacheValueArena>,
    ) -> Result<Self, FrozenBuildError> {
        let base_index = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            std::iter::empty::<(Box<[u8]>, NonMaxU64)>(),
            capacity.max(1),
            AtomicGenerationOverlay::AtomicAdaptive,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
        )?;
        let new_index = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            std::iter::empty::<(Box<[u8]>, NonMaxU64)>(),
            capacity.max(1),
            AtomicGenerationOverlay::AtomicAdaptive,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
        )?;
        Ok(Self {
            packed_base: PackedBaseOverrideIndex::new(capacity.max(1)),
            base_index,
            new_index,
            arena,
            base_changes: BaseChangeBitmap::try_new(base_slots)?,
            base_membership: DeltaMembership::try_new(capacity.max(1))?,
            base_dirty: AtomicBool::new(false),
            base_index_dirty: AtomicBool::new(false),
            new_membership: LazyDeltaMembership::try_new(capacity.max(1))?,
            new_dirty: AtomicBool::new(false),
        })
    }

    fn records(&self) -> usize {
        self.packed_base
            .len()
            .saturating_add(self.base_index.len())
            .saturating_add(self.new_index.len())
    }

    fn base_handle(
        &self,
        key: &[u8],
        key_hash: u64,
        base: &FrozenSegmentCache,
        slot: usize,
    ) -> Option<CacheValueHandle> {
        let packed = self.packed_base.get(key, key_hash, base);
        packed.or_else(|| {
            self.base_index
                .get_protected(&slot_key(slot))
                .map(cache_value_handle_from_index)
        })
    }

    #[inline]
    fn mark_base_change(&self, slot: usize, key_hash: u64) {
        self.base_changes.mark(slot);
        self.base_membership.mark(key_hash);
        publish_dirty_once(&self.base_dirty);
    }

    #[inline]
    fn mark_new_route(&self, key_hash: u64) {
        self.new_membership.mark(key_hash);
        publish_dirty_once(&self.new_dirty);
    }
}

#[inline]
fn publish_dirty_once(dirty: &AtomicBool) {
    // These flags are monotonic. Releasing the cache line for every mutation
    // only makes writers fight over an already-true value. Membership and
    // change bitmaps are routing hints; exact index publication remains the
    // authority when a writer observes that another writer already set this.
    if !dirty.load(Ordering::Relaxed) {
        dirty.store(true, Ordering::Release);
    }
}

impl Drop for SegmentDelta {
    fn drop(&mut self) {
        let pin = self.arena.pin();
        self.packed_base
            .scan(&mut |handle| retire_delta_handle(handle, &pin));
        self.base_index.scan_entries(&mut |_, raw| {
            retire_delta_handle(cache_value_handle_from_index(raw), &pin);
        });
        self.new_index.scan_entries(&mut |_, raw| {
            retire_delta_handle(cache_value_handle_from_index(raw), &pin);
        });
    }
}

/// Failure while constructing or compacting a [`MutableSegmentCache`].
#[derive(Debug)]
pub enum MutableSegmentCacheBuildError {
    /// Building the compact segment generation failed.
    Segment(SegmentCacheBuildError),
    /// Building the lock-free delta index failed.
    Delta(FrozenBuildError),
}

impl fmt::Display for MutableSegmentCacheBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Segment(error) => error.fmt(formatter),
            Self::Delta(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MutableSegmentCacheBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Segment(error) => Some(error),
            Self::Delta(error) => Some(error),
        }
    }
}

impl From<SegmentCacheBuildError> for MutableSegmentCacheBuildError {
    fn from(error: SegmentCacheBuildError) -> Self {
        Self::Segment(error)
    }
}

impl From<FrozenBuildError> for MutableSegmentCacheBuildError {
    fn from(error: FrozenBuildError) -> Self {
        Self::Delta(error)
    }
}

/// Logical result of inserting a value into [`MutableSegmentCache`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentCacheWriteOutcome {
    /// The key was logically absent before this write.
    Inserted,
    /// The write replaced a live value from either layer.
    Replaced,
}

/// Result of exclusively folding the mutable delta into a packed generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentCacheCompactionStats {
    /// Live entries in the old packed generation before delta shadowing.
    pub previous_base_entries: usize,
    /// Unique physical keys retained by the delta, including tombstones.
    pub delta_records: usize,
    /// Live entries published into the new packed generation.
    pub compacted_entries: usize,
    /// Index, headers, keys, and values retained by the new generation.
    pub new_generation_bytes: usize,
    /// Additional payload bytes copied before building the new generation.
    ///
    /// Streaming two-pass compaction keeps this at zero.
    pub temporary_staging_payload_bytes: usize,
    /// Temporary raw-index bytes retained while packing the final index.
    pub temporary_build_index_bytes: usize,
}

/// Multi-writer cache delta over one compact immutable generation.
///
/// Point reads and writes do not acquire a global lock. New values live in an
/// epoch-reclaimed arena. Changes to packed-base keys are routed by physical
/// slot, so their original variable-length keys are not duplicated; genuinely
/// new keys use a separate adaptive atomic index. Deletes are tombstones so an
/// older packed value cannot reappear. [`Self::compact`] currently requires
/// exclusive access; online generation publication is the next stage.
pub struct MutableSegmentCache {
    base: FrozenSegmentCache,
    delta: SegmentDelta,
    delta_capacity: usize,
    started: Instant,
}

impl MutableSegmentCache {
    /// Adds an empty lock-free delta to an existing packed generation.
    ///
    /// # Errors
    ///
    /// Returns a frozen-index allocation or construction failure.
    pub fn try_from_frozen(
        base: FrozenSegmentCache,
        delta_capacity: usize,
    ) -> Result<Self, FrozenBuildError> {
        let base_slots = base.stats().index_slots;
        Ok(Self {
            base,
            delta: SegmentDelta::try_new(
                delta_capacity,
                base_slots,
                Arc::new(CacheValueArena::new()),
            )?,
            delta_capacity: delta_capacity.max(1),
            started: Instant::now(),
        })
    }

    /// Builds a packed base and attaches an empty lock-free delta.
    ///
    /// # Errors
    ///
    /// Returns an error when either the packed base or delta index cannot be
    /// constructed.
    pub fn try_from_entries<I, K, V>(
        config: SegmentCacheConfig,
        entries: I,
        delta_capacity: usize,
    ) -> Result<Self, MutableSegmentCacheBuildError>
    where
        I: IntoIterator<Item = (K, V, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let base = FrozenSegmentCache::try_from_entries(config, entries)?;
        Ok(Self::try_from_frozen(base, delta_capacity)?)
    }

    /// Pins the delta arena once for a sequence of reads.
    pub fn pin(&self) -> MutableSegmentCacheGuard<'_> {
        MutableSegmentCacheGuard {
            cache: self,
            arena: self.delta.arena.pin(),
            base_now_tick: self.base.current_tick(),
            delta_now_tick: self.current_tick(),
        }
    }

    /// Clones the latest live value through the layered read path.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.pin().get(key).map(Into::into)
    }

    /// Inserts or replaces an exact binary value.
    ///
    /// # Errors
    ///
    /// Returns an expiration-layout error when `ttl` is unsupported or lies
    /// beyond the compact generation's relative-TTL horizon, or a capacity
    /// error when one value exceeds four GiB.
    pub fn insert(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let value = value.as_ref();
        if value.len() > MAX_CACHE_VALUE_BYTES {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let now_tick = self.current_tick();
        let expires_at = encode_delta_deadline(self.base.expiry(), ttl, now_tick)?;
        let key_hash = self.base.key_hash(key);
        let base = self.base.probe_hashed(key, key_hash).map(|record| {
            (
                BasePosition {
                    slot: record.slot,
                    key_offset: record.key_offset,
                    key_len: record.key.len(),
                },
                record.remaining_ttl.is_some(),
            )
        });
        let pin = self.delta.arena.pin();
        let handle = self.delta.arena.allocate(value, expires_at);
        let replaced = if let Some((position, base_live)) = base {
            self.delta.mark_base_change(position.slot, key_hash);
            publish_base_delta_record(
                &self.delta,
                position,
                key_hash,
                handle.encoded(),
                &pin,
                now_tick,
                base_live,
            )
        } else {
            self.delta.mark_new_route(key_hash);
            publish_delta_record(
                &self.delta.new_index,
                key,
                handle.encoded(),
                &pin,
                now_tick,
                false,
            )
        };
        Ok(if replaced {
            SegmentCacheWriteOutcome::Replaced
        } else {
            SegmentCacheWriteOutcome::Inserted
        })
    }

    /// Deletes a live key by publishing a compact delta tombstone.
    ///
    /// Returns `false` for a key already absent at this operation's read point.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> bool {
        let now_tick = self.current_tick();
        let key_hash = self.base.key_hash(key);
        let pin = self.delta.arena.pin();
        let base = self.base.probe_hashed(key, key_hash);
        let (target, observed_live) = if let Some(base) = base {
            let observed_live = if self.delta.base_changes.contains(base.slot) {
                self.delta
                    .base_handle(key, key_hash, &self.base, base.slot)
                    .map_or(base.remaining_ttl.is_some(), |handle| {
                        delta_handle_is_live(handle, &pin, now_tick)
                    })
            } else {
                base.remaining_ttl.is_some()
            };
            (
                DeltaTarget::Base(BasePosition {
                    slot: base.slot,
                    key_offset: base.key_offset,
                    key_len: base.key.len(),
                }),
                observed_live,
            )
        } else {
            let observed_live = self.delta.new_dirty.load(Ordering::Acquire)
                && self.delta.new_membership.may_contain(key_hash)
                && self.delta.new_index.get_protected(key).is_some_and(|raw| {
                    delta_handle_is_live(cache_value_handle_from_index(raw), &pin, now_tick)
                });
            (DeltaTarget::New, observed_live)
        };
        if !observed_live {
            return false;
        }
        let tombstone = match target {
            DeltaTarget::Base(position) => tombstone_handle_for_base(position),
            DeltaTarget::New => tombstone_handle(0),
        };
        match target {
            DeltaTarget::Base(position) => {
                self.delta.mark_base_change(position.slot, key_hash);
                publish_base_delta_record(
                    &self.delta,
                    position,
                    key_hash,
                    tombstone,
                    &pin,
                    now_tick,
                    observed_live,
                )
            }
            DeltaTarget::New => {
                self.delta.mark_new_route(key_hash);
                publish_delta_record(&self.delta.new_index, key, tombstone, &pin, now_tick, false)
            }
        }
    }

    /// Returns the immutable packed-generation population.
    #[must_use]
    pub fn base_len(&self) -> usize {
        self.base.len()
    }

    /// Returns unique keys retained in the delta, including tombstones.
    #[must_use]
    pub fn delta_records(&self) -> usize {
        self.delta.records()
    }

    /// Returns bytes retained by the monotonic delta-membership filter.
    #[must_use]
    pub fn delta_filter_bytes(&self) -> usize {
        self.delta
            .new_membership
            .bytes()
            .saturating_add(self.delta.base_membership.bytes())
            .saturating_add(self.delta.base_changes.bytes())
    }

    /// Returns the immutable packed generation.
    #[must_use]
    pub const fn frozen(&self) -> &FrozenSegmentCache {
        &self.base
    }

    /// Exclusively folds all live logical records into a new packed generation.
    ///
    /// This stage intentionally requires `&mut self`: point operations are
    /// concurrent, but online publication during compaction is not implemented
    /// yet. TTLs retain their remaining duration rather than restarting.
    ///
    /// # Errors
    ///
    /// Returns an error if staging the new packed generation or replacement
    /// delta fails.
    #[allow(clippy::too_many_lines)]
    pub fn compact(
        &mut self,
    ) -> Result<SegmentCacheCompactionStats, MutableSegmentCacheBuildError> {
        let previous_base_entries = self.base.len();
        let delta_records = self.delta.records();
        let base_now_tick = self.base.current_tick();
        let delta_now_tick = self.current_tick();
        let pin = self.delta.arena.pin();
        let mut compacted_entries = 0_usize;
        let mut record_capacity = 0_usize;
        let mut sizing_error = None;
        self.for_each_compacted_entry(base_now_tick, delta_now_tick, &pin, |key, value, _| {
            compacted_entries = compacted_entries.saturating_add(1);
            if sizing_error.is_none() {
                sizing_error = SegmentCacheBuilder::encoded_record_bytes(
                    self.base.config(),
                    key.len(),
                    value.len(),
                )
                .and_then(|bytes| {
                    record_capacity
                        .checked_add(bytes)
                        .ok_or(SegmentCacheBuildError::CapacityOverflow)
                })
                .map(|capacity| record_capacity = capacity)
                .err();
            }
        });
        if let Some(error) = sizing_error {
            return Err(error.into());
        }

        let mut builder = SegmentCacheBuilder::try_new_with_record_capacity(
            self.base.config(),
            compacted_entries,
            record_capacity,
        )?;
        let mut build_error = None;
        self.for_each_compacted_entry(base_now_tick, delta_now_tick, &pin, |key, value, ttl| {
            if build_error.is_none() {
                build_error = builder.push(key, value, ttl).err();
            }
        });
        drop(pin);
        if let Some(error) = build_error {
            return Err(error.into());
        }
        let new_base = builder.finish()?;
        let new_generation_bytes = new_base.stats().modeled_retained_bytes();
        let temporary_build_index_bytes = new_base.temporary_build_index_bytes();
        let new_delta = SegmentDelta::try_new(
            self.delta_capacity,
            new_base.stats().index_slots,
            Arc::clone(&self.delta.arena),
        )?;
        self.base = new_base;
        self.delta = new_delta;
        self.started = Instant::now();

        Ok(SegmentCacheCompactionStats {
            previous_base_entries,
            delta_records,
            compacted_entries,
            new_generation_bytes,
            temporary_staging_payload_bytes: 0,
            temporary_build_index_bytes,
        })
    }

    #[allow(
        unsafe_code,
        reason = "the delta indices and value-arena pin belong to this exact segment generation"
    )]
    fn for_each_compacted_entry(
        &self,
        base_now_tick: u64,
        delta_now_tick: u64,
        pin: &CacheValuePin<'_>,
        mut visit: impl FnMut(&[u8], &[u8], Option<Duration>),
    ) {
        self.base
            .for_each_physical_entry_at_tick(base_now_tick, &mut |base| {
                if self.delta.base_changes.contains(base.slot)
                    && let Some(handle) = self.delta.base_handle(
                        base.key,
                        self.base.key_hash(base.key),
                        &self.base,
                        base.slot,
                    )
                {
                    if let Some(handle) = handle.live() {
                        // SAFETY: the unchanged delta handle and pin belong to this generation.
                        let record = unsafe { protected_cache_value(pin, handle) };
                        if let Some(remaining) = remaining_delta_ttl(
                            self.base.expiry(),
                            record.expires_at(),
                            delta_now_tick,
                        ) {
                            visit(base.key, record.value(), ttl_from_remaining(remaining));
                        }
                    }
                    return;
                }
                if let Some(remaining) = base.remaining_ttl {
                    visit(base.key, base.value, ttl_from_remaining(remaining));
                }
            });
        self.delta.new_index.scan_entries(&mut |key, raw| {
            let handle = cache_value_handle_from_index(raw);
            let Some(handle) = handle.live() else {
                return;
            };
            // SAFETY: the scanned delta handle and pin belong to this generation.
            let record = unsafe { protected_cache_value(pin, handle) };
            if let Some(remaining) =
                remaining_delta_ttl(self.base.expiry(), record.expires_at(), delta_now_tick)
            {
                visit(key, record.value(), ttl_from_remaining(remaining));
            }
        });
    }

    fn current_tick(&self) -> u64 {
        let SegmentExpiry::Relative16 { tick } = self.base.expiry() else {
            return 0;
        };
        let ticks = self.started.elapsed().as_nanos() / tick.as_nanos();
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }
}

/// Reusable epoch guard for lock-free reads from [`MutableSegmentCache`].
///
/// Keep guards scoped to a request or batch because a live guard delays arena
/// reclamation of replaced delta records. Expiration uses the time snapshot
/// captured by [`MutableSegmentCache::pin`], so one batch sees a stable TTL
/// view and avoids a monotonic-clock read on every finite-TTL hit.
#[must_use]
pub struct MutableSegmentCacheGuard<'cache> {
    cache: &'cache MutableSegmentCache,
    arena: CacheValuePin<'cache>,
    base_now_tick: u64,
    delta_now_tick: u64,
}

impl MutableSegmentCacheGuard<'_> {
    /// Returns the latest live value and records sampled access state.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.lookup(key, true)
    }

    /// Returns the latest live value without changing access state.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<&[u8]> {
        self.lookup(key, false)
    }

    /// Returns whether the latest layered value is live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.peek(key).is_some()
    }

    #[inline]
    fn lookup(&self, key: &[u8], record_access: bool) -> Option<&[u8]> {
        let key_hash = self.cache.base.key_hash(key);
        if self.cache.delta.base_dirty.load(Ordering::Acquire)
            && self.cache.delta.base_membership.may_contain(key_hash)
            && let Some(handle) = self
                .cache
                .delta
                .packed_base
                .get(key, key_hash, &self.cache.base)
        {
            return self.read_delta_handle(handle, record_access);
        }
        if self.cache.delta.new_dirty.load(Ordering::Acquire)
            && self.cache.delta.new_membership.may_contain(key_hash)
            && let Some(raw) = self.cache.delta.new_index.get_protected(key)
        {
            return self.read_delta(raw, record_access);
        }
        if let Some(base) = self
            .cache
            .base
            .probe_hashed_at_tick(key, key_hash, self.base_now_tick)
        {
            if self.cache.delta.base_changes.contains(base.slot)
                && let Some(handle) =
                    self.cache
                        .delta
                        .base_handle(key, key_hash, &self.cache.base, base.slot)
            {
                return self.read_delta_handle(handle, record_access);
            }
            base.remaining_ttl?;
            if record_access {
                self.cache.base.mark_slot_accessed(base.slot);
            }
            return Some(base.value);
        }
        None
    }

    fn read_delta(&self, raw: NonMaxU64, record_access: bool) -> Option<&[u8]> {
        self.read_delta_handle(cache_value_handle_from_index(raw), record_access)
    }

    #[allow(
        unsafe_code,
        reason = "the decoded delta handle and arena pin belong to this exact cache guard"
    )]
    fn read_delta_handle(&self, handle: CacheValueHandle, record_access: bool) -> Option<&[u8]> {
        let handle = handle.live()?;
        // SAFETY: callers supply handles loaded from this guard's delta indices.
        let entry = unsafe { protected_cache_value(&self.arena, handle) };
        if !entry.is_live(self.delta_now_tick) {
            return None;
        }
        if record_access {
            entry.mark_accessed();
        }
        Some(entry.value())
    }
}

/// Timing and retained-layout result of an online packed-generation rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineSegmentCacheCompactionStats {
    /// Logical records in the stable predecessor before packing.
    pub previous_entries: usize,
    /// Unique records retained by the stable predecessor delta.
    pub compacted_delta_records: usize,
    /// Live records published into the new packed base.
    pub compacted_entries: usize,
    /// Bytes retained by the newly published packed base.
    pub new_generation_bytes: usize,
    /// Bytes newly allocated for the replacement index and copied records.
    ///
    /// This excludes immutable predecessor payload segments shared with the
    /// replacement and is therefore the correct generation term for peak-RAM
    /// modeling.
    pub newly_allocated_generation_bytes: usize,
    /// Unchanged records whose packed bytes were shared with the predecessor.
    pub reused_entries: usize,
    /// Live packed record bytes referenced from predecessor payload segments.
    pub reused_record_bytes: usize,
    /// Packed record bytes copied from delta/new records into this generation.
    pub copied_record_bytes: usize,
    /// Immutable payload segments referenced by the replacement generation.
    pub record_segments: usize,
    /// Temporary raw-index bytes used while packing the final index.
    pub temporary_build_index_bytes: usize,
    /// Time spent redirecting and draining predecessor writer stripes.
    pub writer_redirect: Duration,
    /// Time spent constructing the replacement packed base.
    pub background_build: Duration,
    /// Time spent installing direct-base metadata and publishing the base.
    pub base_publish: Duration,
}

/// Multi-writer segment cache with online packed-generation publication.
///
/// A rebuild first redirects each writer stripe into a fresh overlay. Once the
/// predecessor is stable it is packed in the calling thread while reads and
/// writes continue. The successor overlay is never replayed or replaced, so a
/// write racing the build cannot be lost. Point operations do not acquire the
/// rebuild mutex; only concurrent rebuild calls are serialized.
pub struct OnlineMutableSegmentCache {
    current: ArcSwap<OnlineSegmentGeneration>,
    rebuild_gate: Mutex<()>,
    arena: Arc<CacheValueArena>,
    delta_capacity: usize,
    route_hash: crate::generation_hash::GenerationHashBuilder,
    generation: AtomicU64,
}

impl OnlineMutableSegmentCache {
    /// Adds an online mutable layer to an existing packed generation.
    ///
    /// # Errors
    ///
    /// Returns a frozen-index allocation or construction failure.
    pub fn try_from_frozen(
        base: FrozenSegmentCache,
        delta_capacity: usize,
    ) -> Result<Self, FrozenBuildError> {
        let route_hash = base.hash_builder().clone();
        let arena = Arc::new(CacheValueArena::new());
        let generation =
            OnlineSegmentGeneration::with_frozen(base, delta_capacity.max(1), Arc::clone(&arena))?;
        Ok(Self {
            current: ArcSwap::from(generation),
            rebuild_gate: Mutex::new(()),
            arena,
            delta_capacity: delta_capacity.max(1),
            route_hash,
            generation: AtomicU64::new(0),
        })
    }

    /// Builds a packed base and attaches an online mutable layer.
    ///
    /// # Errors
    ///
    /// Returns an error when either the packed base or delta cannot be built.
    pub fn try_from_entries<I, K, V>(
        config: SegmentCacheConfig,
        entries: I,
        delta_capacity: usize,
    ) -> Result<Self, MutableSegmentCacheBuildError>
    where
        I: IntoIterator<Item = (K, V, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let base = FrozenSegmentCache::try_from_entries(config, entries)?;
        Ok(Self::try_from_frozen(base, delta_capacity)?)
    }

    /// Pins one generation and one epoch guard for a read batch.
    pub fn pin(&self) -> OnlineMutableSegmentCacheGuard<'_> {
        loop {
            let generation = self.current.load();
            if let Some(read_epoch) = try_acquire_online_read(&generation) {
                let base = generation.base.load_full();
                let stable_ticks = match base.as_ref() {
                    OnlineSegmentBase::Frozen(base) => {
                        Some((base.current_tick(), generation.current_tick(base.expiry())))
                    }
                    OnlineSegmentBase::Previous { .. } => None,
                };
                let frozen = match base.as_ref() {
                    OnlineSegmentBase::Frozen(base) => Some(OnlineFrozenRead {
                        base: Arc::clone(base),
                        delta: Arc::clone(&generation.delta),
                    }),
                    OnlineSegmentBase::Previous { .. } => None,
                };
                return OnlineMutableSegmentCacheGuard {
                    cache: self,
                    generation,
                    base,
                    arena: self.arena.pin(),
                    stable_ticks,
                    frozen,
                    read_epoch,
                };
            }
            spin_loop();
        }
    }

    /// Pins one writer generation for a mutation batch without retaining an
    /// epoch value guard between operations.
    ///
    /// This removes the per-mutation writer-stripe reservation while allowing
    /// replaced values to be reclaimed between calls. A retained batch delays
    /// writer redirection during compaction.
    pub fn pin_writer_batch(&self) -> OnlineMutableSegmentWriterGuard<'_> {
        loop {
            let generation = self.current.load();
            if let Some(read_epoch) = try_acquire_online_read(&generation) {
                let base = generation.base.load_full();
                return OnlineMutableSegmentWriterGuard {
                    cache: self,
                    generation,
                    base,
                    read_epoch,
                };
            }
            spin_loop();
        }
    }

    /// Clones the latest live value through the online layered read path.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.pin().get(key).map(Into::into)
    }

    /// Inserts or replaces an exact binary value.
    ///
    /// # Errors
    ///
    /// Returns an expiration-layout or value-capacity error.
    pub fn insert(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let value = value.as_ref();
        if value.len() > MAX_CACHE_VALUE_BYTES {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let route_hash = self.route_hash.hash_one(key);
        let writer = self.pin_writer(route_hash);
        writer.insert(key, value, ttl, route_hash)
    }

    /// Deletes a live key without blocking an online rebuild.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> bool {
        let route_hash = self.route_hash.hash_one(key);
        self.pin_writer(route_hash).remove(key, route_hash)
    }

    /// Returns the number of successfully published packed generations.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Returns unique records in the active overlay, including tombstones.
    #[must_use]
    pub fn delta_records(&self) -> usize {
        self.current.load().delta.records()
    }

    /// Returns bytes retained by active delta routing filters.
    #[must_use]
    pub fn delta_filter_bytes(&self) -> usize {
        let generation = self.current.load();
        generation
            .delta
            .new_membership
            .bytes()
            .saturating_add(generation.delta.base_membership.bytes())
            .saturating_add(generation.delta.base_changes.bytes())
    }

    /// Returns the unique-overlay record count that recommends compaction.
    ///
    /// The adaptive threshold is the lower of 75% of the configured delta
    /// budget and roughly 12.5% of the current packed-base population. Small
    /// and initially empty tables use the delta budget to avoid rebuild loops.
    #[must_use]
    pub fn compaction_threshold_records(&self) -> usize {
        let generation = self.current.load();
        let base = generation.base.load();
        online_compaction_threshold(base.frozen_len(), self.delta_capacity)
    }

    /// Returns whether unique overlay churn has crossed the adaptive threshold.
    #[must_use]
    pub fn compaction_recommended(&self) -> bool {
        let generation = self.current.load();
        let base = generation.base.load();
        generation.delta.records()
            >= online_compaction_threshold(base.frozen_len(), self.delta_capacity)
    }

    /// Compacts only when unique overlay churn crosses the adaptive threshold.
    ///
    /// The recommendation is checked again while holding the rebuild mutex, so
    /// concurrent maintenance callers cannot queue redundant compactions.
    /// Point reads and writes continue through the online rebuild protocol.
    ///
    /// # Errors
    ///
    /// Returns a packed-generation or direct-routing allocation failure.
    pub fn compact_if_recommended(
        &self,
    ) -> Result<Option<OnlineSegmentCacheCompactionStats>, MutableSegmentCacheBuildError> {
        let _one_rebuild = self.rebuild_gate.lock();
        let generation = self.current.load();
        let base = generation.base.load();
        if generation.delta.records()
            < online_compaction_threshold(base.frozen_len(), self.delta_capacity)
        {
            return Ok(None);
        }
        drop(base);
        drop(generation);
        self.compact_locked().map(Some)
    }

    /// Redirects writers, packs the stable predecessor, and publishes it.
    ///
    /// Rebuilds selectively reuse immutable predecessor payload segments when
    /// doing so retains at most a small fixed amount of dead payload. Live
    /// records from dirtier segments are copied, and output is capped at two
    /// payload segments; unsuitable generations use a flat full-copy build.
    ///
    /// Reads and writes continue throughout construction. A reader guard that
    /// began before publication keeps its exact old base snapshot until drop.
    ///
    /// # Errors
    ///
    /// Returns a packed-generation or direct-routing allocation failure. If
    /// construction fails, the successor remains correct by retaining its
    /// predecessor as a logical base; a later call can retry the rebuild.
    #[allow(clippy::too_many_lines)]
    pub fn compact(
        &self,
    ) -> Result<OnlineSegmentCacheCompactionStats, MutableSegmentCacheBuildError> {
        let _one_rebuild = self.rebuild_gate.lock();
        self.compact_locked()
    }

    #[allow(clippy::too_many_lines)]
    fn compact_locked(
        &self,
    ) -> Result<OnlineSegmentCacheCompactionStats, MutableSegmentCacheBuildError> {
        let previous = self.current.load_full();
        let previous_base = previous.base.load_full();
        let next = OnlineSegmentGeneration::with_previous(
            Arc::clone(&previous),
            Arc::clone(&previous_base),
            self.delta_capacity,
            Arc::clone(&self.arena),
        )?;

        let redirect_started = Instant::now();
        self.current.store(Arc::clone(&next));
        previous.close_read_batches();
        previous.close_writer_stripes();
        next.write_predecessor.store(None);
        next.write_predecessor_active
            .store(false, Ordering::Release);
        let writer_redirect = redirect_started.elapsed();
        let previous_delta_records = previous.delta.records();

        let scan_times = OnlineScanTimes::capture(&previous, &previous_base);
        let pin = self.arena.pin();
        let build_started = Instant::now();
        let direct_source = match previous_base.as_ref() {
            OnlineSegmentBase::Frozen(frozen) => Some(Arc::clone(frozen)),
            OnlineSegmentBase::Previous { .. } => None,
        };
        let mut previous_entries = 0_usize;
        let mut record_capacity = 0_usize;
        let mut copied_record_capacity = 0_usize;
        let mut reusable_entries = 0_usize;
        let mut live_source_segment_bytes =
            direct_source.as_ref().map_or_else(Vec::new, |source| {
                vec![0_usize; source.record_segment_count()]
            });
        let mut sizing_error = None;
        previous.for_each_compacted_entry(
            &previous_base,
            &scan_times,
            &pin,
            &mut |key, value, _, source| {
                previous_entries = previous_entries.saturating_add(1);
                if sizing_error.is_none() {
                    let record_bytes = source.map_or_else(
                        || {
                            SegmentCacheBuilder::encoded_record_bytes(
                                previous_base.config(),
                                key.len(),
                                value.len(),
                            )
                        },
                        |source| {
                            source
                                .header_bytes
                                .checked_add(key.len())
                                .and_then(|bytes| bytes.checked_add(value.len()))
                                .ok_or(SegmentCacheBuildError::CapacityOverflow)
                        },
                    );
                    sizing_error = record_bytes
                        .and_then(|bytes| {
                            record_capacity = record_capacity
                                .checked_add(bytes)
                                .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
                            if let Some(source) = source {
                                reusable_entries = reusable_entries.saturating_add(1);
                                if live_source_segment_bytes.len() > 1 {
                                    let segment = source.segment();
                                    live_source_segment_bytes[segment] = live_source_segment_bytes
                                        [segment]
                                        .checked_add(bytes)
                                        .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
                                }
                            } else {
                                copied_record_capacity = copied_record_capacity
                                    .checked_add(bytes)
                                    .ok_or(SegmentCacheBuildError::CapacityOverflow)?;
                            }
                            Ok(())
                        })
                        .err();
                }
            },
        );
        if let Some(error) = sizing_error {
            return Err(error.into());
        }
        if live_source_segment_bytes.len() == 1 {
            live_source_segment_bytes[0] = record_capacity.saturating_sub(copied_record_capacity);
        }
        let shared_plan = direct_source.as_ref().and_then(|source| {
            select_shared_record_segments(
                source,
                &live_source_segment_bytes,
                record_capacity,
                copied_record_capacity,
            )
        });
        let mut shared_build = None;
        if let (Some(source), Some((used_source_segments, planned_copied_bytes))) =
            (&direct_source, shared_plan)
        {
            copied_record_capacity = planned_copied_bytes;
            let mut builder = SharedSegmentCacheBuilder::try_new(
                source,
                previous_entries,
                copied_record_capacity,
                &used_source_segments,
                self.route_hash.clone(),
                scan_times
                    .frozen_tick
                    .expect("a direct frozen scan captures its current tick"),
            )?;
            let mut build_error = None;
            previous.for_each_compacted_entry(
                &previous_base,
                &scan_times,
                &pin,
                &mut |key, value, ttl, reusable| {
                    if build_error.is_some() {
                        return;
                    }
                    build_error = if let Some(location) = reusable
                        && used_source_segments[location.segment()]
                    {
                        builder.push_reused(key, value, location).err()
                    } else {
                        builder.push_copied(key, value, ttl).err()
                    };
                },
            );
            match build_error.map_or_else(|| builder.finish(), Err) {
                Ok(built) => shared_build = Some(built),
                Err(
                    SegmentCacheBuildError::ExpiryOutOfRange
                    | SegmentCacheBuildError::ArenaTooLarge,
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }

        let (
            new_base,
            newly_allocated_generation_bytes,
            reused_entries,
            reused_record_bytes,
            copied_record_bytes,
        ) = if let Some(built) = shared_build {
            (
                built.cache,
                built.newly_allocated_bytes,
                built.reused_entries,
                built.reused_record_bytes,
                built.copied_record_bytes,
            )
        } else {
            let mut builder = SegmentCacheBuilder::try_new_with_record_capacity_and_hash_builder(
                previous_base.config(),
                previous_entries,
                record_capacity,
                self.route_hash.clone(),
            )?;
            let mut build_error = None;
            previous.for_each_compacted_entry(
                &previous_base,
                &scan_times,
                &pin,
                &mut |key, value, ttl, _| {
                    if build_error.is_none() {
                        build_error = builder.push(key, value, ttl).err();
                    }
                },
            );
            if let Some(error) = build_error {
                return Err(error.into());
            }
            let base = builder.finish()?;
            let allocated = base.stats().modeled_retained_bytes();
            let copied = base.stats().record_bytes;
            (base, allocated, 0, 0, copied)
        };
        drop(pin);
        let background_build = build_started.elapsed();
        let new_generation_bytes = new_base.stats().modeled_retained_bytes();
        let temporary_build_index_bytes = new_base.temporary_build_index_bytes();
        let record_segments = new_base.record_segment_count();

        let publish_started = Instant::now();
        next.delta
            .base_changes
            .initialize(new_base.stats().index_slots)?;
        next.base
            .store(Arc::new(OnlineSegmentBase::Frozen(Arc::new(new_base))));
        let previous_read_epoch = next.active_read_epoch.fetch_xor(1, Ordering::AcqRel);
        next.close_read_epoch(previous_read_epoch);
        next.enable_direct_base_stripes();
        let base_publish = publish_started.elapsed();
        self.generation.fetch_add(1, Ordering::AcqRel);

        Ok(OnlineSegmentCacheCompactionStats {
            previous_entries,
            compacted_delta_records: previous_delta_records,
            compacted_entries: previous_entries,
            new_generation_bytes,
            newly_allocated_generation_bytes,
            reused_entries,
            reused_record_bytes,
            copied_record_bytes,
            record_segments,
            temporary_build_index_bytes,
            writer_redirect,
            background_build,
            base_publish,
        })
    }

    fn pin_writer(&self, route_hash: u64) -> OnlineSegmentWriter {
        let stripe = online_writer_stripe(route_hash);
        loop {
            let generation = self.current.load();
            if generation.write_predecessor_active.load(Ordering::Acquire)
                && let Some(previous) = generation.write_predecessor.load_full()
                && let Some(writer) = OnlineSegmentWriter::try_pin_arc(previous, stripe)
            {
                return writer;
            }
            if let Some(writer) = OnlineSegmentWriter::try_pin_guard(generation, stripe) {
                return writer;
            }
            spin_loop();
        }
    }
}

/// Reusable zero-copy read guard for [`OnlineMutableSegmentCache`].
#[must_use]
pub struct OnlineMutableSegmentCacheGuard<'cache> {
    cache: &'cache OnlineMutableSegmentCache,
    generation: Guard<Arc<OnlineSegmentGeneration>>,
    base: Arc<OnlineSegmentBase>,
    arena: CacheValuePin<'cache>,
    stable_ticks: Option<(u64, u64)>,
    frozen: Option<OnlineFrozenRead>,
    read_epoch: usize,
}

struct OnlineFrozenRead {
    base: Arc<FrozenSegmentCache>,
    delta: Arc<SegmentDelta>,
}

impl Drop for OnlineMutableSegmentCacheGuard<'_> {
    fn drop(&mut self) {
        let previous =
            self.generation.read_batches[self.read_epoch].fetch_sub(1, Ordering::Release);
        debug_assert_ne!(previous & ONLINE_WRITER_COUNT_MASK, 0);
    }
}

/// Reusable generation guard for mutation-heavy worker batches.
#[must_use]
pub struct OnlineMutableSegmentWriterGuard<'cache> {
    cache: &'cache OnlineMutableSegmentCache,
    generation: Guard<Arc<OnlineSegmentGeneration>>,
    base: Arc<OnlineSegmentBase>,
    read_epoch: usize,
}

impl OnlineMutableSegmentWriterGuard<'_> {
    /// Inserts or replaces while using a short-lived epoch value pin.
    ///
    /// # Errors
    ///
    /// Returns an expiration-layout or value-capacity error.
    pub fn insert(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let value = value.as_ref();
        if value.len() > MAX_CACHE_VALUE_BYTES {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let route_hash = self.cache.route_hash.hash_one(key);
        let stripe = online_writer_stripe(route_hash);
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & ONLINE_WRITER_CLOSED, 0);
        let pin = self.cache.arena.pin();
        self.generation.insert_routed(
            &self.base,
            &pin,
            key,
            value,
            ttl,
            route_hash,
            state & ONLINE_WRITER_DIRECT_BASE != 0,
        )
    }

    /// Deletes a live key while using a short-lived epoch value pin.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> bool {
        let route_hash = self.cache.route_hash.hash_one(key);
        let stripe = online_writer_stripe(route_hash);
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & ONLINE_WRITER_CLOSED, 0);
        let direct_base = state & ONLINE_WRITER_DIRECT_BASE != 0;
        let active_batches = self.generation.read_batches[self.read_epoch].load(Ordering::Relaxed)
            & ONLINE_WRITER_COUNT_MASK;
        if active_batches == 1
            && let Some(removed) = self.generation.try_remove_unchanged_base_without_pin(
                &self.base,
                key,
                route_hash,
                direct_base,
            )
        {
            return removed;
        }
        let pin = self.cache.arena.pin();
        self.generation
            .remove_routed(&self.base, &pin, key, route_hash, direct_base)
    }
}

impl Drop for OnlineMutableSegmentWriterGuard<'_> {
    fn drop(&mut self) {
        let previous =
            self.generation.read_batches[self.read_epoch].fetch_sub(1, Ordering::Release);
        debug_assert_ne!(previous & ONLINE_WRITER_COUNT_MASK, 0);
    }
}

impl OnlineMutableSegmentCacheGuard<'_> {
    /// Returns the latest live value and records sampled access state.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.lookup(key, true)
    }

    /// Returns the latest live value without changing sampled access state.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<&[u8]> {
        self.lookup(key, false)
    }

    /// Returns whether the latest layered value is live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.peek(key).is_some()
    }

    /// Inserts or replaces through this guard's pinned writer generation.
    ///
    /// This amortizes generation-handoff protection across a request or worker
    /// batch. A retained guard can delay writer redirection during compaction.
    ///
    /// # Errors
    ///
    /// Returns an expiration-layout or value-capacity error.
    pub fn insert(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let value = value.as_ref();
        if value.len() > MAX_CACHE_VALUE_BYTES {
            return Err(SegmentCacheBuildError::CapacityOverflow);
        }
        let route_hash = self.cache.route_hash.hash_one(key);
        let stripe = online_writer_stripe(route_hash);
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & ONLINE_WRITER_CLOSED, 0);
        self.generation.insert_routed(
            &self.base,
            &self.arena,
            key,
            value,
            ttl,
            route_hash,
            state & ONLINE_WRITER_DIRECT_BASE != 0,
        )
    }

    /// Deletes a live key through this guard's pinned writer generation.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> bool {
        let route_hash = self.cache.route_hash.hash_one(key);
        let stripe = online_writer_stripe(route_hash);
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & ONLINE_WRITER_CLOSED, 0);
        self.generation.remove_routed(
            &self.base,
            &self.arena,
            key,
            route_hash,
            state & ONLINE_WRITER_DIRECT_BASE != 0,
        )
    }

    #[inline]
    fn lookup(&self, key: &[u8], record_access: bool) -> Option<&[u8]> {
        let route_hash = self.cache.route_hash.hash_one(key);
        if let Some(frozen) = &self.frozen {
            let (base_tick, delta_tick) = self
                .stable_ticks
                .expect("a frozen read guard captures stable ticks");
            return OnlineSegmentGeneration::lookup_frozen(
                &frozen.delta,
                &frozen.base,
                &self.arena,
                key,
                route_hash,
                record_access,
                base_tick,
                delta_tick,
            );
        }
        self.generation.lookup(
            &self.base,
            &self.arena,
            key,
            route_hash,
            record_access,
            None,
        )
    }
}

enum OnlineSegmentBase {
    Frozen(Arc<FrozenSegmentCache>),
    Previous {
        generation: Arc<OnlineSegmentGeneration>,
        base: Arc<OnlineSegmentBase>,
    },
}

impl OnlineSegmentBase {
    fn config(&self) -> SegmentCacheConfig {
        match self {
            Self::Frozen(base) => base.config(),
            Self::Previous { base, .. } => base.config(),
        }
    }

    fn expiry(&self) -> SegmentExpiry {
        match self {
            Self::Frozen(base) => base.expiry(),
            Self::Previous { base, .. } => base.expiry(),
        }
    }

    fn frozen_len(&self) -> usize {
        match self {
            Self::Frozen(base) => base.len(),
            Self::Previous { base, .. } => base.frozen_len(),
        }
    }
}

struct OnlineSegmentGeneration {
    base: ArcSwap<OnlineSegmentBase>,
    delta: Arc<SegmentDelta>,
    started: Instant,
    write_predecessor: ArcSwapOption<OnlineSegmentGeneration>,
    write_predecessor_active: AtomicBool,
    writer_stripes: Box<[AtomicUsize]>,
    read_batches: [AtomicUsize; 2],
    active_read_epoch: AtomicUsize,
    new_may_shadow_base: AtomicBool,
}

struct OnlineScanTimes {
    delta_tick: u64,
    frozen_tick: Option<u64>,
    previous: Option<Box<Self>>,
}

enum OnlinePinnedGeneration {
    Guard(Guard<Arc<OnlineSegmentGeneration>>),
    Arc(Arc<OnlineSegmentGeneration>),
}

struct OnlineSegmentWriter {
    generation: OnlinePinnedGeneration,
    stripe: usize,
    direct_base: bool,
    released: bool,
}

impl OnlineSegmentGeneration {
    fn with_frozen(
        base: FrozenSegmentCache,
        delta_capacity: usize,
        arena: Arc<CacheValueArena>,
    ) -> Result<Arc<Self>, FrozenBuildError> {
        let base_slots = base.stats().index_slots;
        Ok(Arc::new(Self {
            base: ArcSwap::from_pointee(OnlineSegmentBase::Frozen(Arc::new(base))),
            delta: Arc::new(SegmentDelta::try_new(delta_capacity, base_slots, arena)?),
            started: Instant::now(),
            write_predecessor: ArcSwapOption::empty(),
            write_predecessor_active: AtomicBool::new(false),
            writer_stripes: online_writer_stripes(true),
            read_batches: [AtomicUsize::new(0), AtomicUsize::new(0)],
            active_read_epoch: AtomicUsize::new(0),
            new_may_shadow_base: AtomicBool::new(false),
        }))
    }

    fn with_previous(
        previous: Arc<Self>,
        previous_base: Arc<OnlineSegmentBase>,
        delta_capacity: usize,
        arena: Arc<CacheValueArena>,
    ) -> Result<Arc<Self>, FrozenBuildError> {
        Ok(Arc::new(Self {
            base: ArcSwap::from_pointee(OnlineSegmentBase::Previous {
                generation: Arc::clone(&previous),
                base: previous_base,
            }),
            delta: Arc::new(SegmentDelta::try_new(delta_capacity, 0, arena)?),
            started: Instant::now(),
            write_predecessor: ArcSwapOption::from(Some(previous)),
            write_predecessor_active: AtomicBool::new(true),
            writer_stripes: online_writer_stripes(false),
            read_batches: [AtomicUsize::new(0), AtomicUsize::new(0)],
            active_read_epoch: AtomicUsize::new(0),
            new_may_shadow_base: AtomicBool::new(false),
        }))
    }

    fn current_tick(&self, expiry: SegmentExpiry) -> u64 {
        let SegmentExpiry::Relative16 { tick } = expiry else {
            return 0;
        };
        let ticks = self.started.elapsed().as_nanos() / tick.as_nanos();
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }

    fn lookup<'a>(
        &'a self,
        base: &'a OnlineSegmentBase,
        pin: &'a CacheValuePin<'_>,
        key: &[u8],
        route_hash: u64,
        record_access: bool,
        stable_ticks: Option<(u64, u64)>,
    ) -> Option<&'a [u8]> {
        let delta_tick = stable_ticks.map_or_else(
            || self.current_tick(base.expiry()),
            |(_, delta_tick)| delta_tick,
        );
        match base {
            OnlineSegmentBase::Frozen(frozen) => Self::lookup_frozen(
                &self.delta,
                frozen,
                pin,
                key,
                route_hash,
                record_access,
                stable_ticks.map_or_else(|| frozen.current_tick(), |(base_tick, _)| base_tick),
                delta_tick,
            ),
            OnlineSegmentBase::Previous { generation, base } => {
                if self.delta.new_dirty.load(Ordering::Acquire)
                    && self.delta.new_membership.may_contain(route_hash)
                    && let Some(raw) = self.delta.new_index.get_protected(key)
                {
                    return read_online_delta_handle(
                        cache_value_handle_from_index(raw),
                        pin,
                        delta_tick,
                        record_access,
                    );
                }
                generation.lookup(base, pin, key, route_hash, record_access, None)
            }
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn lookup_frozen<'a>(
        delta: &'a SegmentDelta,
        frozen: &'a FrozenSegmentCache,
        pin: &'a CacheValuePin<'_>,
        key: &[u8],
        key_hash: u64,
        record_access: bool,
        base_tick: u64,
        delta_tick: u64,
    ) -> Option<&'a [u8]> {
        if delta.base_dirty.load(Ordering::Acquire)
            && delta.base_membership.may_contain(key_hash)
            && let Some(handle) = delta.packed_base.get(key, key_hash, frozen)
        {
            return read_online_delta_handle(handle, pin, delta_tick, record_access);
        }
        if delta.base_index_dirty.load(Ordering::Acquire)
            && let Some(record) = frozen.probe_hashed_at_tick(key, key_hash, base_tick)
            && delta.base_changes.contains(record.slot)
            && let Some(raw) = delta.base_index.get_protected(&slot_key(record.slot))
        {
            return read_online_delta_handle(
                cache_value_handle_from_index(raw),
                pin,
                delta_tick,
                record_access,
            );
        }
        if delta.new_dirty.load(Ordering::Acquire)
            && delta.new_membership.may_contain(key_hash)
            && let Some(raw) = delta.new_index.get_protected(key)
        {
            return read_online_delta_handle(
                cache_value_handle_from_index(raw),
                pin,
                delta_tick,
                record_access,
            );
        }
        if let Some(record) = frozen.probe_hashed_at_tick(key, key_hash, base_tick) {
            if delta.base_changes.contains(record.slot)
                && let Some(handle) = delta.base_handle(key, key_hash, frozen, record.slot)
            {
                return read_online_delta_handle(handle, pin, delta_tick, record_access);
            }
            record.remaining_ttl?;
            if record_access {
                frozen.mark_slot_accessed(record.slot);
            }
            return Some(record.value);
        }
        None
    }

    fn lookup_base_live(
        base: &OnlineSegmentBase,
        pin: &CacheValuePin<'_>,
        key: &[u8],
        route_hash: u64,
    ) -> bool {
        match base {
            OnlineSegmentBase::Frozen(frozen) => frozen
                .probe_hashed_at_tick(key, route_hash, frozen.current_tick())
                .is_some_and(|record| record.remaining_ttl.is_some()),
            OnlineSegmentBase::Previous { generation, base } => generation
                .lookup(base, pin, key, route_hash, false, None)
                .is_some(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_routed(
        &self,
        base: &OnlineSegmentBase,
        pin: &CacheValuePin<'_>,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
        route_hash: u64,
        direct_base: bool,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let now_tick = self.current_tick(base.expiry());
        let expires_at = encode_delta_deadline(base.expiry(), ttl, now_tick)?;
        let handle = self.delta.arena.allocate(value, expires_at);
        let replaced = if direct_base
            && let OnlineSegmentBase::Frozen(frozen) = base
            && let Some(record) = frozen.probe_hashed(key, route_hash)
        {
            let logical_live = if self.new_may_shadow_base.load(Ordering::Acquire) {
                self.delta
                    .new_index
                    .get_protected(key)
                    .map_or(record.remaining_ttl.is_some(), |raw| {
                        delta_handle_is_live(cache_value_handle_from_index(raw), pin, now_tick)
                    })
            } else {
                record.remaining_ttl.is_some()
            };
            self.delta.mark_base_change(record.slot, route_hash);
            publish_base_delta_record(
                &self.delta,
                BasePosition {
                    slot: record.slot,
                    key_offset: record.key_offset,
                    key_len: record.key.len(),
                },
                route_hash,
                handle.encoded(),
                pin,
                now_tick,
                logical_live,
            )
        } else {
            let base_live = Self::lookup_base_live(base, pin, key, route_hash);
            if base_live {
                self.new_may_shadow_base.store(true, Ordering::Release);
            }
            self.delta.mark_new_route(route_hash);
            publish_delta_record(
                &self.delta.new_index,
                key,
                handle.encoded(),
                pin,
                now_tick,
                base_live,
            )
        };
        Ok(if replaced {
            SegmentCacheWriteOutcome::Replaced
        } else {
            SegmentCacheWriteOutcome::Inserted
        })
    }

    fn remove_routed(
        &self,
        base: &OnlineSegmentBase,
        pin: &CacheValuePin<'_>,
        key: &[u8],
        route_hash: u64,
        direct_base: bool,
    ) -> bool {
        let now_tick = self.current_tick(base.expiry());
        if direct_base
            && let OnlineSegmentBase::Frozen(frozen) = base
            && let Some(record) = frozen.probe_hashed(key, route_hash)
        {
            let observed_live = self.direct_logical_live(
                frozen,
                record.slot,
                record.remaining_ttl.is_some(),
                key,
                route_hash,
                pin,
                now_tick,
            );
            if !observed_live {
                return false;
            }
            self.delta.mark_base_change(record.slot, route_hash);
            return publish_base_delta_record(
                &self.delta,
                BasePosition {
                    slot: record.slot,
                    key_offset: record.key_offset,
                    key_len: record.key.len(),
                },
                route_hash,
                tombstone_handle_for_base(BasePosition {
                    slot: record.slot,
                    key_offset: record.key_offset,
                    key_len: record.key.len(),
                }),
                pin,
                now_tick,
                observed_live,
            );
        }
        let base_live = Self::lookup_base_live(base, pin, key, route_hash);
        let observed_live = self
            .delta
            .new_index
            .get_protected(key)
            .map_or(base_live, |raw| {
                delta_handle_is_live(cache_value_handle_from_index(raw), pin, now_tick)
            });
        if !observed_live {
            return false;
        }
        if base_live {
            self.new_may_shadow_base.store(true, Ordering::Release);
        }
        self.delta.mark_new_route(route_hash);
        publish_delta_record(
            &self.delta.new_index,
            key,
            tombstone_handle(0),
            pin,
            now_tick,
            observed_live,
        )
    }

    fn try_remove_unchanged_base_without_pin(
        &self,
        base: &OnlineSegmentBase,
        key: &[u8],
        route_hash: u64,
        direct_base: bool,
    ) -> Option<bool> {
        if !direct_base || self.new_may_shadow_base.load(Ordering::Acquire) {
            return None;
        }
        let OnlineSegmentBase::Frozen(frozen) = base else {
            return None;
        };
        let record = frozen.probe_hashed(key, route_hash)?;
        if self.delta.base_changes.contains(record.slot) {
            return None;
        }
        if record.remaining_ttl.is_none() {
            return Some(false);
        }
        let position = BasePosition {
            slot: record.slot,
            key_offset: record.key_offset,
            key_len: record.key.len(),
        };
        self.delta.mark_base_change(record.slot, route_hash);
        Some(publish_base_tombstone_record(
            &self.delta,
            position,
            route_hash,
            tombstone_handle_for_base(position),
            self.current_tick(base.expiry()),
            true,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn direct_logical_live(
        &self,
        frozen: &FrozenSegmentCache,
        slot: usize,
        base_live: bool,
        key: &[u8],
        key_hash: u64,
        pin: &CacheValuePin<'_>,
        now_tick: u64,
    ) -> bool {
        if self.delta.base_changes.contains(slot)
            && let Some(handle) = self.delta.base_handle(key, key_hash, frozen, slot)
        {
            return delta_handle_is_live(handle, pin, now_tick);
        }
        if self.new_may_shadow_base.load(Ordering::Acquire)
            && self.delta.new_membership.may_contain(key_hash)
            && let Some(raw) = self.delta.new_index.get_protected(key)
        {
            return delta_handle_is_live(cache_value_handle_from_index(raw), pin, now_tick);
        }
        base_live
    }

    fn close_writer_stripes(&self) {
        for counter in &self.writer_stripes {
            let mut spins = 0_u32;
            let mut state = counter.load(Ordering::Acquire);
            loop {
                if state & ONLINE_WRITER_CLOSED != 0 {
                    break;
                }
                if state & ONLINE_WRITER_COUNT_MASK == 0 {
                    match counter.compare_exchange_weak(
                        state,
                        state | ONLINE_WRITER_CLOSED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(observed) => state = observed,
                    }
                } else if spins < 64 {
                    spin_loop();
                    spins += 1;
                    state = counter.load(Ordering::Acquire);
                } else {
                    thread::yield_now();
                    state = counter.load(Ordering::Acquire);
                }
            }
        }
    }

    fn enable_direct_base_stripes(&self) {
        for counter in &self.writer_stripes {
            let mut spins = 0_u32;
            let mut state = counter.load(Ordering::Acquire);
            loop {
                debug_assert_eq!(state & ONLINE_WRITER_CLOSED, 0);
                if state & ONLINE_WRITER_DIRECT_BASE != 0 {
                    break;
                }
                if state & ONLINE_WRITER_COUNT_MASK == 0 {
                    match counter.compare_exchange_weak(
                        state,
                        state | ONLINE_WRITER_DIRECT_BASE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(observed) => state = observed,
                    }
                } else if spins < 64 {
                    spin_loop();
                    spins += 1;
                    state = counter.load(Ordering::Acquire);
                } else {
                    thread::yield_now();
                    state = counter.load(Ordering::Acquire);
                }
            }
        }
    }

    fn close_read_batches(&self) {
        self.close_read_epoch(0);
        self.close_read_epoch(1);
    }

    fn close_read_epoch(&self, epoch: usize) {
        let previous = self.read_batches[epoch].fetch_or(ONLINE_WRITER_CLOSED, Ordering::AcqRel);
        if previous & ONLINE_WRITER_COUNT_MASK == 0 {
            return;
        }
        let mut spins = 0_u32;
        while self.read_batches[epoch].load(Ordering::Acquire) & ONLINE_WRITER_COUNT_MASK != 0 {
            if spins < 64 {
                spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
            }
        }
    }

    fn for_each_compacted_entry(
        &self,
        base: &OnlineSegmentBase,
        times: &OnlineScanTimes,
        pin: &CacheValuePin<'_>,
        visit: &mut OnlineSegmentEntryVisitor<'_>,
    ) {
        match base {
            OnlineSegmentBase::Frozen(frozen) => {
                let frozen_tick = times
                    .frozen_tick
                    .expect("a frozen scan snapshot contains a frozen tick");
                let new_may_shadow_base = self.new_may_shadow_base.load(Ordering::Acquire);
                frozen.for_each_physical_entry_at_tick(frozen_tick, &mut |record| {
                    // Every direct mutation marks its physical slot before
                    // publication. A live unmarked slot is therefore exact
                    // unless a predecessor-layer new record may shadow it.
                    if !new_may_shadow_base
                        && !self.delta.base_changes.contains(record.slot)
                        && let Some(remaining) = record.remaining_ttl
                    {
                        visit(
                            record.key,
                            record.value,
                            ttl_from_remaining(remaining),
                            Some(SegmentRecordLocation {
                                location: record.location,
                                header_bytes: record.key_offset,
                            }),
                        );
                        return;
                    }
                    let key_hash = frozen.key_hash(record.key);
                    if let Some(handle) =
                        self.frozen_delta_handle(record.key, key_hash, frozen, record.slot)
                    {
                        visit_live_online_handle(
                            handle,
                            pin,
                            frozen.expiry(),
                            times.delta_tick,
                            record.key,
                            visit,
                        );
                    } else if let Some(remaining) = record.remaining_ttl {
                        visit(
                            record.key,
                            record.value,
                            ttl_from_remaining(remaining),
                            Some(SegmentRecordLocation {
                                location: record.location,
                                header_bytes: record.key_offset,
                            }),
                        );
                    }
                });
            }
            OnlineSegmentBase::Previous { generation, base } => {
                generation.for_each_compacted_entry(
                    base,
                    times
                        .previous
                        .as_deref()
                        .expect("a predecessor scan snapshot contains predecessor times"),
                    pin,
                    &mut |key, value, ttl, source| {
                        if let Some(raw) = self.delta.new_index.get_protected(key) {
                            visit_live_online_handle(
                                cache_value_handle_from_index(raw),
                                pin,
                                base.expiry(),
                                times.delta_tick,
                                key,
                                visit,
                            );
                        } else {
                            visit(key, value, ttl, source);
                        }
                    },
                );
            }
        }

        self.delta.new_index.scan_entries(&mut |key, raw| {
            if Self::base_mentions_key(base, pin, key) {
                return;
            }
            visit_live_online_handle(
                cache_value_handle_from_index(raw),
                pin,
                base.expiry(),
                times.delta_tick,
                key,
                visit,
            );
        });
    }

    fn frozen_delta_handle(
        &self,
        key: &[u8],
        key_hash: u64,
        frozen: &FrozenSegmentCache,
        slot: usize,
    ) -> Option<CacheValueHandle> {
        self.delta
            .packed_base
            .get(key, key_hash, frozen)
            .or_else(|| {
                (self.delta.base_index_dirty.load(Ordering::Acquire)
                    && self.delta.base_changes.contains(slot))
                .then(|| {
                    self.delta
                        .base_index
                        .get_protected(&slot_key(slot))
                        .map(cache_value_handle_from_index)
                })
                .flatten()
            })
            .or_else(|| {
                self.delta
                    .new_index
                    .get_protected(key)
                    .map(cache_value_handle_from_index)
            })
            .or_else(|| {
                self.delta
                    .base_changes
                    .contains(slot)
                    .then(|| self.delta.base_handle(key, key_hash, frozen, slot))
                    .flatten()
            })
    }

    fn base_mentions_key(base: &OnlineSegmentBase, pin: &CacheValuePin<'_>, key: &[u8]) -> bool {
        match base {
            OnlineSegmentBase::Frozen(frozen) => {
                frozen.probe_hashed(key, frozen.key_hash(key)).is_some()
            }
            OnlineSegmentBase::Previous { generation, base } => {
                generation.contains_logical_exact(base, pin, key)
            }
        }
    }

    fn contains_logical_exact(
        &self,
        base: &OnlineSegmentBase,
        pin: &CacheValuePin<'_>,
        key: &[u8],
    ) -> bool {
        let delta_tick = self.current_tick(base.expiry());
        match base {
            OnlineSegmentBase::Frozen(frozen) => {
                let key_hash = frozen.key_hash(key);
                if let Some(handle) = self.delta.packed_base.get(key, key_hash, frozen) {
                    return delta_handle_is_live(handle, pin, delta_tick);
                }
                if self.delta.base_index_dirty.load(Ordering::Acquire)
                    && let Some(record) =
                        frozen.probe_hashed_at_tick(key, key_hash, frozen.current_tick())
                    && self.delta.base_changes.contains(record.slot)
                    && let Some(raw) = self.delta.base_index.get_protected(&slot_key(record.slot))
                {
                    return delta_handle_is_live(
                        cache_value_handle_from_index(raw),
                        pin,
                        delta_tick,
                    );
                }
                if let Some(raw) = self.delta.new_index.get_protected(key) {
                    return delta_handle_is_live(
                        cache_value_handle_from_index(raw),
                        pin,
                        delta_tick,
                    );
                }
                if let Some(record) =
                    frozen.probe_hashed_at_tick(key, key_hash, frozen.current_tick())
                {
                    if self.delta.base_changes.contains(record.slot)
                        && let Some(handle) =
                            self.delta.base_handle(key, key_hash, frozen, record.slot)
                    {
                        return delta_handle_is_live(handle, pin, delta_tick);
                    }
                    return record.remaining_ttl.is_some();
                }
                false
            }
            OnlineSegmentBase::Previous { generation, base } => {
                if let Some(raw) = self.delta.new_index.get_protected(key) {
                    return delta_handle_is_live(
                        cache_value_handle_from_index(raw),
                        pin,
                        delta_tick,
                    );
                }
                generation.contains_logical_exact(base, pin, key)
            }
        }
    }
}

impl OnlineScanTimes {
    fn capture(generation: &OnlineSegmentGeneration, base: &OnlineSegmentBase) -> Self {
        match base {
            OnlineSegmentBase::Frozen(frozen) => Self {
                delta_tick: generation.current_tick(frozen.expiry()),
                frozen_tick: Some(frozen.current_tick()),
                previous: None,
            },
            OnlineSegmentBase::Previous {
                generation: previous,
                base,
            } => Self {
                delta_tick: generation.current_tick(base.expiry()),
                frozen_tick: None,
                previous: Some(Box::new(Self::capture(previous, base))),
            },
        }
    }
}

impl Deref for OnlinePinnedGeneration {
    type Target = OnlineSegmentGeneration;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Guard(generation) => generation,
            Self::Arc(generation) => generation,
        }
    }
}

impl Deref for OnlineSegmentWriter {
    type Target = OnlineSegmentGeneration;

    fn deref(&self) -> &Self::Target {
        &self.generation
    }
}

impl OnlineSegmentWriter {
    fn try_pin_arc(generation: Arc<OnlineSegmentGeneration>, stripe: usize) -> Option<Self> {
        Self::try_pin(OnlinePinnedGeneration::Arc(generation), stripe)
    }

    fn try_pin_guard(
        generation: Guard<Arc<OnlineSegmentGeneration>>,
        stripe: usize,
    ) -> Option<Self> {
        Self::try_pin(OnlinePinnedGeneration::Guard(generation), stripe)
    }

    fn try_pin(generation: OnlinePinnedGeneration, stripe: usize) -> Option<Self> {
        let state = try_acquire_online_writer(&generation.writer_stripes[stripe])?;
        Some(Self {
            generation,
            stripe,
            direct_base: state & ONLINE_WRITER_DIRECT_BASE != 0,
            released: false,
        })
    }

    fn insert(
        mut self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
        route_hash: u64,
    ) -> Result<SegmentCacheWriteOutcome, SegmentCacheBuildError> {
        let base = self.base.load();
        let pin = self.delta.arena.pin();
        let outcome =
            self.insert_routed(&base, &pin, key, value, ttl, route_hash, self.direct_base);
        drop(pin);
        drop(base);
        self.release();
        outcome
    }

    fn remove(mut self, key: &[u8], route_hash: u64) -> bool {
        let base = self.base.load();
        if let Some(removed) =
            self.try_remove_unchanged_base_without_pin(&base, key, route_hash, self.direct_base)
        {
            drop(base);
            self.release();
            return removed;
        }
        let pin = self.delta.arena.pin();
        let removed = self.remove_routed(&base, &pin, key, route_hash, self.direct_base);
        drop(pin);
        drop(base);
        self.release();
        removed
    }

    fn release(&mut self) {
        let previous = self.generation.writer_stripes[self.stripe].fetch_sub(1, Ordering::Release);
        debug_assert_eq!(previous & ONLINE_WRITER_CLOSED, 0);
        debug_assert_ne!(previous & ONLINE_WRITER_COUNT_MASK, 0);
        self.released = true;
    }
}

impl Drop for OnlineSegmentWriter {
    fn drop(&mut self) {
        if !self.released {
            let previous =
                self.generation.writer_stripes[self.stripe].fetch_sub(1, Ordering::Release);
            debug_assert_eq!(previous & ONLINE_WRITER_CLOSED, 0);
            debug_assert_ne!(previous & ONLINE_WRITER_COUNT_MASK, 0);
        }
    }
}

fn online_writer_stripes(direct_base: bool) -> Box<[AtomicUsize]> {
    (0..ONLINE_WRITER_STRIPES)
        .map(|_| AtomicUsize::new(usize::from(direct_base) * ONLINE_WRITER_DIRECT_BASE))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn online_writer_stripe(route_hash: u64) -> usize {
    usize::try_from(
        route_hash & u64::try_from(ONLINE_WRITER_STRIPES - 1).expect("stripe mask fits u64"),
    )
    .expect("masked writer stripe fits usize")
}

fn online_compaction_threshold(base_entries: usize, delta_capacity: usize) -> usize {
    let capacity_threshold = delta_capacity.saturating_mul(3).div_ceil(4).max(1);
    if base_entries == 0 {
        return capacity_threshold;
    }
    let density_threshold = base_entries
        .div_ceil(8)
        .max(MIN_ONLINE_DENSITY_COMPACTION_RECORDS)
        .min(delta_capacity);
    capacity_threshold.min(density_threshold).max(1)
}

fn select_shared_record_segments(
    source: &FrozenSegmentCache,
    live_segment_bytes: &[usize],
    final_live_record_bytes: usize,
    initially_copied_bytes: usize,
) -> Option<(Vec<bool>, usize)> {
    if live_segment_bytes.len() != source.record_segment_count() {
        return None;
    }
    let waste_budget = MAX_SHARED_PAYLOAD_WASTE_BYTES.min(final_live_record_bytes);
    let live_segments = live_segment_bytes
        .iter()
        .filter(|bytes| **bytes != 0)
        .count();
    let max_reused_segments =
        if initially_copied_bytes != 0 || live_segments > MAX_SHARED_RECORD_SEGMENTS {
            MAX_SHARED_RECORD_SEGMENTS - 1
        } else {
            MAX_SHARED_RECORD_SEGMENTS
        };
    let mut candidates = live_segment_bytes
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, live)| *live != 0)
        .map(|(segment, live)| {
            let retained = source.record_segment_bytes(segment)?;
            Some((segment, live, retained.saturating_sub(live), retained))
        })
        .collect::<Option<Vec<_>>>()?;
    candidates.sort_unstable_by(|left, right| {
        let left_ratio = (left.2 as u128) * (right.3 as u128);
        let right_ratio = (right.2 as u128) * (left.3 as u128);
        left_ratio
            .cmp(&right_ratio)
            .then_with(|| right.1.cmp(&left.1))
    });

    let mut used = vec![false; live_segment_bytes.len()];
    let mut retained_waste = 0_usize;
    let mut reused_segments = 0_usize;
    for (segment, _, waste, _) in candidates {
        if reused_segments == max_reused_segments
            || retained_waste.saturating_add(waste) > waste_budget
        {
            continue;
        }
        used[segment] = true;
        retained_waste = retained_waste.saturating_add(waste);
        reused_segments += 1;
    }
    if reused_segments == 0 {
        return None;
    }

    let copied_record_bytes = live_segment_bytes
        .iter()
        .copied()
        .enumerate()
        .filter(|(segment, _)| !used[*segment])
        .try_fold(initially_copied_bytes, |total, (_, bytes)| {
            total.checked_add(bytes)
        })?;
    let output_segments = reused_segments + usize::from(copied_record_bytes != 0);
    (output_segments <= MAX_SHARED_RECORD_SEGMENTS).then_some((used, copied_record_bytes))
}

fn try_acquire_online_writer(counter: &AtomicUsize) -> Option<usize> {
    let mut state = counter.load(Ordering::Acquire);
    loop {
        if state & ONLINE_WRITER_CLOSED != 0
            || state & ONLINE_WRITER_COUNT_MASK == ONLINE_WRITER_COUNT_MASK
        {
            return None;
        }
        match counter.compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(previous) => return Some(previous),
            Err(observed) => state = observed,
        }
    }
}

fn try_acquire_online_read(generation: &OnlineSegmentGeneration) -> Option<usize> {
    let epoch = generation.active_read_epoch.load(Ordering::Acquire) & 1;
    try_acquire_online_writer(&generation.read_batches[epoch])?;
    if generation.active_read_epoch.load(Ordering::Acquire) & 1 == epoch {
        return Some(epoch);
    }
    let previous = generation.read_batches[epoch].fetch_sub(1, Ordering::Release);
    debug_assert_ne!(previous & ONLINE_WRITER_COUNT_MASK, 0);
    None
}

#[allow(
    unsafe_code,
    reason = "the online generation supplies a same-arena delta handle and pin"
)]
fn read_online_delta_handle<'a>(
    handle: CacheValueHandle,
    pin: &'a CacheValuePin<'_>,
    now_tick: u64,
    record_access: bool,
) -> Option<&'a [u8]> {
    let handle = handle.live()?;
    // SAFETY: callers pair an unchanged online-delta handle with its generation pin.
    let entry = unsafe { protected_cache_value(pin, handle) };
    if !entry.is_live(now_tick) {
        return None;
    }
    if record_access {
        entry.mark_accessed();
    }
    Some(entry.value())
}

#[allow(
    unsafe_code,
    reason = "the online visitor receives a same-arena delta handle and pin"
)]
fn visit_live_online_handle(
    handle: CacheValueHandle,
    pin: &CacheValuePin<'_>,
    expiry: SegmentExpiry,
    now_tick: u64,
    key: &[u8],
    visit: &mut OnlineSegmentEntryVisitor<'_>,
) {
    let Some(handle) = handle.live() else {
        return;
    };
    // SAFETY: callers pair an unchanged online-delta handle with its generation pin.
    let record = unsafe { protected_cache_value(pin, handle) };
    if let Some(remaining) = remaining_delta_ttl(expiry, record.expires_at(), now_tick) {
        visit(key, record.value(), ttl_from_remaining(remaining), None);
    }
}

fn slot_key(slot: usize) -> [u8; 8] {
    u64::try_from(slot)
        .expect("packed base slot fits u64")
        .to_le_bytes()
}

fn packed_base_tag(hash: u64) -> u8 {
    u8::try_from(hash % u64::from(PACKED_BASE_WRITING - 1)).expect("reduced hash tag fits u8") + 1
}

fn packed_base_control_byte(control: u64, offset: usize) -> u8 {
    u8::try_from((control >> (offset * u8::BITS as usize)) & u64::from(u8::MAX))
        .expect("masked base-override control fits u8")
}

fn matching_control_bytes(control: u64, tag: u8) -> u64 {
    zero_control_bytes(control ^ (u64::from(tag) * BYTE_LOW_BITS))
}

fn zero_control_bytes(value: u64) -> u64 {
    value.wrapping_sub(BYTE_LOW_BITS) & !value & BYTE_HIGH_BITS
}

fn reduce_to(hash: u64, buckets: usize) -> usize {
    let reduced =
        (u128::from(hash) * u128::try_from(buckets).expect("bucket count fits u128")) >> 64;
    usize::try_from(reduced).expect("reduced hash is below the bucket count")
}

fn next_prime(minimum: usize) -> usize {
    if minimum <= 2 {
        return 2;
    }
    let mut candidate = minimum | 1;
    loop {
        if is_prime(candidate) {
            return candidate;
        }
        candidate = candidate
            .checked_add(2)
            .expect("base-override bucket count overflow");
    }
}

fn is_prime(candidate: usize) -> bool {
    if candidate.is_multiple_of(2) {
        return candidate == 2;
    }
    let mut divisor = 3_usize;
    while divisor <= candidate / divisor {
        if candidate.is_multiple_of(divisor) {
            return false;
        }
        divisor += 2;
    }
    true
}

fn cache_value_handle_from_raw(raw: u64) -> CacheValueHandle {
    cache_value_handle_from_index(
        NonMaxU64::new(raw).expect("a published base override contains a non-null arena pointer"),
    )
}

fn tombstone_handle(base_slot: usize) -> CacheValueHandle {
    let raw = u64::try_from(base_slot)
        .expect("packed base slot fits u64")
        .checked_add(1)
        .filter(|encoded| *encoded <= TOMBSTONE_LOCATION_MASK)
        .map(|encoded| encoded - 1)
        .expect("packed base slot fits tombstone location bits")
        .checked_shl(IMMEDIATE_HANDLE_TAG_MASK.count_ones())
        .and_then(|location| location.checked_add(TOMBSTONE_HANDLE_TAG))
        .expect("packed base slot fits tagged tombstone handle");
    CacheValueHandle::from_immediate_value(
        NonMaxU64::new(raw).expect("tagged tombstone does not use the reserved handle"),
    )
}

fn tombstone_handle_for_base(position: BasePosition) -> CacheValueHandle {
    let fallback = tombstone_handle(position.slot);
    if position.key_offset > usize::try_from(TOMBSTONE_KEY_OFFSET_MASK).unwrap_or(usize::MAX)
        || position.key_len > TOMBSTONE_MAX_DIRECT_KEY_LEN
    {
        return fallback;
    }
    let encoded_len = u64::try_from(position.key_len + 1).expect("direct key length fits u64");
    let metadata = (encoded_len << TOMBSTONE_KEY_OFFSET_BITS)
        | u64::try_from(position.key_offset).expect("direct key offset fits u64");
    let raw = fallback.index_value().get() | (metadata << TOMBSTONE_METADATA_SHIFT);
    CacheValueHandle::from_immediate_value(
        NonMaxU64::new(raw).expect("tagged direct tombstone does not use the reserved handle"),
    )
}

fn is_tombstone_handle(handle: CacheValueHandle) -> bool {
    handle.index_value().get() & IMMEDIATE_HANDLE_TAG_MASK == TOMBSTONE_HANDLE_TAG
}

fn tombstone_base_slot(handle: CacheValueHandle) -> usize {
    usize::try_from(
        (handle.index_value().get() >> IMMEDIATE_HANDLE_TAG_MASK.count_ones())
            & TOMBSTONE_LOCATION_MASK,
    )
    .expect("encoded packed base slot fits usize")
}

fn tombstone_base_key(handle: CacheValueHandle, base: &FrozenSegmentCache) -> Option<&[u8]> {
    let raw = handle.index_value().get();
    let metadata = raw >> TOMBSTONE_METADATA_SHIFT;
    if metadata == 0 {
        return base.key_at_physical_slot(tombstone_base_slot(handle));
    }
    let key_offset = usize::try_from(metadata & TOMBSTONE_KEY_OFFSET_MASK).ok()?;
    let encoded_len = usize::try_from(metadata >> TOMBSTONE_KEY_OFFSET_BITS).ok()?;
    let key_len = encoded_len.checked_sub(1)?;
    base.key_at_physical_slot_span(tombstone_base_slot(handle), key_offset, key_len)
}

#[allow(
    unsafe_code,
    reason = "publication helpers pair displaced handles with their exact delta-arena pin"
)]
fn delta_handle_is_live(handle: CacheValueHandle, pin: &CacheValuePin<'_>, now_tick: u64) -> bool {
    handle.live().is_some_and(|handle| {
        // SAFETY: the displaced handle and pin belong to the same delta arena.
        unsafe { protected_cache_value(pin, handle) }.is_live(now_tick)
    })
}

#[allow(
    unsafe_code,
    reason = "publication helpers retire exact displaced handles through their delta-arena pin"
)]
fn retire_delta_handle(handle: CacheValueHandle, pin: &CacheValuePin<'_>) {
    if let Some(handle) = handle.live() {
        // SAFETY: an exact publication replacement transferred this handle once.
        unsafe { pin.retire(removed_cache_value_after_exact_transfer(handle)) };
    }
}

fn publish_base_delta_record(
    delta: &SegmentDelta,
    position: BasePosition,
    key_hash: u64,
    handle: CacheValueHandle,
    pin: &CacheValuePin<'_>,
    now_tick: u64,
    base_live: bool,
) -> bool {
    if let Ok(slot_u32) = u32::try_from(position.slot) {
        match delta.packed_base.insert(slot_u32, key_hash, handle) {
            PackedBaseInsert::Inserted => return base_live,
            PackedBaseInsert::Replaced(previous) => {
                let was_live = delta_handle_is_live(previous, pin, now_tick);
                retire_delta_handle(previous, pin);
                return was_live;
            }
            PackedBaseInsert::Full(unpublished) => {
                publish_dirty_once(&delta.base_index_dirty);
                return publish_delta_record(
                    &delta.base_index,
                    &slot_key(position.slot),
                    unpublished,
                    pin,
                    now_tick,
                    base_live,
                );
            }
        }
    }
    publish_dirty_once(&delta.base_index_dirty);
    publish_delta_record(
        &delta.base_index,
        &slot_key(position.slot),
        handle,
        pin,
        now_tick,
        base_live,
    )
}

fn publish_base_tombstone_record(
    delta: &SegmentDelta,
    position: BasePosition,
    key_hash: u64,
    handle: CacheValueHandle,
    now_tick: u64,
    base_live: bool,
) -> bool {
    debug_assert!(is_tombstone_handle(handle));
    if let Ok(slot_u32) = u32::try_from(position.slot) {
        match delta.packed_base.insert(slot_u32, key_hash, handle) {
            PackedBaseInsert::Inserted => return base_live,
            PackedBaseInsert::Replaced(previous) => {
                return retire_tombstone_displaced_handle(delta, previous, now_tick);
            }
            PackedBaseInsert::Full(unpublished) => {
                publish_dirty_once(&delta.base_index_dirty);
                return publish_delta_tombstone_record(
                    &delta.base_index,
                    &slot_key(position.slot),
                    unpublished,
                    delta,
                    now_tick,
                    base_live,
                );
            }
        }
    }
    publish_dirty_once(&delta.base_index_dirty);
    publish_delta_tombstone_record(
        &delta.base_index,
        &slot_key(position.slot),
        handle,
        delta,
        now_tick,
        base_live,
    )
}

fn publish_delta_tombstone_record(
    index: &LockFreeAtomicU64GenerationMap,
    key: &[u8],
    handle: CacheValueHandle,
    delta: &SegmentDelta,
    now_tick: u64,
    base_live: bool,
) -> bool {
    debug_assert!(is_tombstone_handle(handle));
    match index.insert(key, handle.index_value()) {
        InsertOutcome::Inserted => base_live,
        InsertOutcome::Replaced(raw) => {
            retire_tombstone_displaced_handle(delta, cache_value_handle_from_index(raw), now_tick)
        }
    }
}

#[allow(
    unsafe_code,
    reason = "the exact tombstone replacement pairs its displaced handle with the delta arena"
)]
fn retire_tombstone_displaced_handle(
    delta: &SegmentDelta,
    previous: CacheValueHandle,
    now_tick: u64,
) -> bool {
    let Some(previous) = previous.live() else {
        return false;
    };
    // A successful replacement gives this writer sole retirement ownership of
    // the now-unreachable handle. It therefore remains allocated while the
    // epoch pin is acquired, after which preceding readers are protected.
    let pin = delta.arena.pin();
    // SAFETY: exact replacement keeps this delta-arena handle live through `pin`.
    let was_live = unsafe { protected_cache_value(&pin, previous) }.is_live(now_tick);
    // SAFETY: exact replacement transferred the displaced handle once.
    unsafe { pin.retire(removed_cache_value_after_exact_transfer(previous)) };
    was_live
}

fn publish_delta_record(
    index: &LockFreeAtomicU64GenerationMap,
    key: &[u8],
    handle: CacheValueHandle,
    pin: &CacheValuePin<'_>,
    now_tick: u64,
    base_live: bool,
) -> bool {
    match index.insert(key, handle.index_value()) {
        InsertOutcome::Inserted => base_live,
        InsertOutcome::Replaced(raw) => {
            let previous = cache_value_handle_from_index(raw);
            let was_live = delta_handle_is_live(previous, pin, now_tick);
            retire_delta_handle(previous, pin);
            was_live
        }
    }
}

fn encode_delta_deadline(
    expiry: SegmentExpiry,
    ttl: Option<Duration>,
    now_tick: u64,
) -> Result<u64, SegmentCacheBuildError> {
    match (expiry, ttl) {
        (SegmentExpiry::None | SegmentExpiry::Relative16 { .. }, None) => Ok(NEVER_EXPIRES),
        (SegmentExpiry::None, Some(_)) => Err(SegmentCacheBuildError::ExpiryDisabled),
        (SegmentExpiry::Relative16 { tick }, Some(ttl)) => {
            let ticks = ttl.as_nanos().div_ceil(tick.as_nanos());
            if ticks > u128::from(MAX_RELATIVE_TICKS) {
                return Err(SegmentCacheBuildError::ExpiryOutOfRange);
            }
            Ok(now_tick
                .saturating_add(u64::try_from(ticks).unwrap_or(MAX_RELATIVE_TICKS))
                .min(NEVER_EXPIRES - 1))
        }
    }
}

fn remaining_delta_ttl(
    expiry: SegmentExpiry,
    expires_at: u64,
    now_tick: u64,
) -> Option<SegmentRemainingTtl> {
    if expires_at == NEVER_EXPIRES {
        return Some(SegmentRemainingTtl::Never);
    }
    if expires_at <= now_tick {
        return None;
    }
    let SegmentExpiry::Relative16 { tick } = expiry else {
        return Some(SegmentRemainingTtl::Never);
    };
    let remaining = expires_at - now_tick;
    let remaining = u32::try_from(remaining).ok()?;
    tick.checked_mul(remaining).map(SegmentRemainingTtl::Finite)
}

fn ttl_from_remaining(remaining: SegmentRemainingTtl) -> Option<Duration> {
    match remaining {
        SegmentRemainingTtl::Never => None,
        SegmentRemainingTtl::Finite(ttl) => Some(ttl),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::hash::BuildHasher;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::{
        BasePosition, MAX_SHARED_RECORD_SEGMENTS, MutableSegmentCache, NEVER_EXPIRES,
        OnlineMutableSegmentCache, OnlineSegmentBase, OnlineSegmentGeneration,
        SegmentCacheWriteOutcome, TOMBSTONE_MAX_DIRECT_KEY_LEN, TOMBSTONE_METADATA_SHIFT, is_prime,
        next_prime, publish_delta_record, tombstone_base_key, tombstone_base_slot,
        tombstone_handle_for_base,
    };
    use crate::{FrozenSegmentCache, SegmentCacheConfig, SegmentExpiry};
    use std::time::Duration;

    #[test]
    fn insert_replace_delete_and_reinsert_shadow_the_base() {
        let cache = MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [
                (b"base".as_slice(), b"old".as_slice(), None),
                (b"stay".as_slice(), b"yes".as_slice(), None),
            ],
            64,
        )
        .unwrap();

        assert_eq!(cache.pin().peek(b"base"), Some(b"old".as_slice()));
        assert_eq!(
            cache.insert(b"base", b"new", None).unwrap(),
            SegmentCacheWriteOutcome::Replaced
        );
        assert_eq!(
            cache.insert(b"fresh", b"value", None).unwrap(),
            SegmentCacheWriteOutcome::Inserted
        );
        assert_eq!(cache.pin().peek(b"base"), Some(b"new".as_slice()));
        assert!(cache.remove(b"base"));
        assert!(cache.pin().peek(b"base").is_none());
        assert!(!cache.remove(b"base"));
        assert_eq!(
            cache.insert(b"base", b"again", None).unwrap(),
            SegmentCacheWriteOutcome::Inserted
        );
        assert_eq!(cache.pin().peek(b"base"), Some(b"again".as_slice()));
        assert_eq!(cache.pin().peek(b"stay"), Some(b"yes".as_slice()));
    }

    #[test]
    fn compaction_folds_tombstones_and_updates_without_restarting_ttl() {
        let mut cache = MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1)),
            [
                (b"replace".as_slice(), b"old".as_slice(), None),
                (b"delete".as_slice(), b"gone".as_slice(), None),
            ],
            64,
        )
        .unwrap();
        cache
            .insert(b"replace", b"new", Some(Duration::from_secs(60)))
            .unwrap();
        cache
            .insert(b"insert", b"fresh", Some(Duration::from_secs(60)))
            .unwrap();
        assert!(cache.remove(b"delete"));

        let stats = cache.compact().unwrap();
        assert_eq!(stats.previous_base_entries, 2);
        assert_eq!(stats.delta_records, 3);
        assert_eq!(stats.compacted_entries, 2);
        assert_eq!(cache.delta_records(), 0);
        assert_eq!(cache.pin().peek(b"replace"), Some(b"new".as_slice()));
        assert_eq!(cache.pin().peek(b"insert"), Some(b"fresh".as_slice()));
        assert!(cache.pin().peek(b"delete").is_none());
    }

    #[test]
    fn pinned_guard_uses_a_stable_expiration_snapshot() {
        let cache = MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_millis(1)),
            [(
                b"base".as_slice(),
                b"old".as_slice(),
                Some(Duration::from_millis(100)),
            )],
            64,
        )
        .unwrap();
        cache
            .insert(b"delta", b"new", Some(Duration::from_millis(100)))
            .unwrap();

        let snapshot = cache.pin();
        thread::sleep(Duration::from_millis(150));
        assert_eq!(snapshot.peek(b"base"), Some(b"old".as_slice()));
        assert_eq!(snapshot.peek(b"delta"), Some(b"new".as_slice()));

        let current = cache.pin();
        assert!(current.peek(b"base").is_none());
        assert!(current.peek(b"delta").is_none());
    }

    #[test]
    fn distinct_multiwriters_publish_exact_values_without_a_global_lock() {
        let cache = Arc::new(
            MutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                std::iter::empty::<(&[u8], &[u8], Option<Duration>)>(),
                16_384,
            )
            .unwrap(),
        );
        let joins = (0_u64..8)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for index in 0_u64..2_000 {
                        let key = worker.wrapping_mul(2_000).wrapping_add(index).to_le_bytes();
                        cache.insert(&key, key, None).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for join in joins {
            join.join().unwrap();
        }
        let guard = cache.pin();
        for value in 0_u64..16_000 {
            let key = value.to_le_bytes();
            assert_eq!(guard.peek(&key), Some(key.as_slice()));
        }
    }

    #[test]
    fn contended_same_base_key_keeps_one_exact_latest_record() {
        let cache = Arc::new(
            MutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                [(b"shared".as_slice(), b"initial".as_slice(), None)],
                64,
            )
            .unwrap(),
        );
        let joins = (0_u64..8)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for sequence in 0_u64..2_000 {
                        let value = worker.wrapping_shl(32) | sequence;
                        cache.insert(b"shared", value.to_le_bytes(), None).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for join in joins {
            join.join().unwrap();
        }

        let final_value: [u8; 8] = cache.pin().peek(b"shared").unwrap().try_into().unwrap();
        let final_value = u64::from_le_bytes(final_value);
        assert!(final_value >> 32 < 8);
        assert!((final_value & u64::from(u32::MAX)) < 2_000);
        assert_eq!(cache.delta_records(), 1);
    }

    #[test]
    fn mixed_operations_and_compaction_match_hash_map() {
        let entries = 512_usize;
        let mut model = HashMap::<Vec<u8>, Vec<u8>>::new();
        let initial = (0..entries)
            .map(|index| {
                let key = variable_key(index);
                let value = value_for(index, 0);
                model.insert(key.clone(), value.clone());
                (key, value, None)
            })
            .collect::<Vec<_>>();
        let mut cache = MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            initial,
            2_048,
        )
        .unwrap();
        let mut random = 0x8f31_5d97_a6c4_e2b1_u64;

        for step in 0..20_000_usize {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key_index = usize::try_from(random % 1_024).unwrap();
            let key = variable_key(key_index);
            match (random >> 32) % 4 {
                0 | 1 => {
                    let value = value_for(key_index, step);
                    let expected_replaced = model.insert(key.clone(), value.clone()).is_some();
                    let outcome = cache.insert(&key, &value, None).unwrap();
                    assert_eq!(
                        outcome == SegmentCacheWriteOutcome::Replaced,
                        expected_replaced
                    );
                }
                2 => assert_eq!(cache.remove(&key), model.remove(&key).is_some()),
                _ => assert_eq!(cache.pin().peek(&key), model.get(&key).map(Vec::as_slice)),
            }

            if step == 9_999 {
                cache.compact().unwrap();
            }
        }

        for index in 0..1_024 {
            let key = variable_key(index);
            assert_eq!(cache.pin().peek(&key), model.get(&key).map(Vec::as_slice));
        }
    }

    #[test]
    fn packed_base_saturation_falls_back_without_losing_exact_values() {
        const KEYS: usize = 512;
        let mut cache = MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            (0..KEYS).map(|key| (u64::try_from(key).unwrap().to_le_bytes(), [0_u8; 8], None)),
            1,
        )
        .unwrap();

        for key in 0..u64::try_from(KEYS).unwrap() {
            cache
                .insert(&key.to_le_bytes(), key.to_be_bytes(), None)
                .unwrap();
        }
        for key in 0..u64::try_from(KEYS).unwrap() {
            assert_eq!(
                cache.pin().peek(&key.to_le_bytes()),
                Some(key.to_be_bytes().as_slice())
            );
        }

        cache.compact().unwrap();
        for key in 0..u64::try_from(KEYS).unwrap() {
            assert_eq!(
                cache.pin().peek(&key.to_le_bytes()),
                Some(key.to_be_bytes().as_slice())
            );
        }
    }

    #[test]
    fn packed_base_bucket_geometry_rounds_up_to_a_prime() {
        for minimum in [
            0,
            1,
            2,
            3,
            4,
            17,
            100_000_usize.div_ceil(7),
            1_000_000_usize.div_ceil(7),
        ] {
            let buckets = next_prime(minimum);
            assert!(buckets >= minimum);
            assert!(is_prime(buckets));
        }
    }

    #[test]
    fn tombstones_inline_common_base_key_spans_and_fallback_exactly() {
        let base = FrozenSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [(b"direct-key".as_slice(), b"value".as_slice(), None)],
        )
        .unwrap();
        let hash = base.key_hash(b"direct-key");
        let record = base.probe_hashed(b"direct-key", hash).unwrap();
        let position = BasePosition {
            slot: record.slot,
            key_offset: record.key_offset,
            key_len: record.key.len(),
        };
        let direct = tombstone_handle_for_base(position);
        assert_ne!(direct.index_value().get() >> TOMBSTONE_METADATA_SHIFT, 0);
        assert_eq!(tombstone_base_slot(direct), record.slot);
        assert_eq!(
            tombstone_base_key(direct, &base),
            Some(b"direct-key".as_slice())
        );

        let fallback = tombstone_handle_for_base(BasePosition {
            key_len: TOMBSTONE_MAX_DIRECT_KEY_LEN + 1,
            ..position
        });
        assert_eq!(fallback.index_value().get() >> TOMBSTONE_METADATA_SHIFT, 0);
        assert_eq!(tombstone_base_slot(fallback), record.slot);
        assert_eq!(
            tombstone_base_key(fallback, &base),
            Some(b"direct-key".as_slice())
        );
    }

    #[test]
    fn online_compaction_preserves_updates_deletes_and_new_keys() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [
                (b"replace".as_slice(), b"old".as_slice(), None),
                (b"delete".as_slice(), b"gone".as_slice(), None),
                (b"stay".as_slice(), b"yes".as_slice(), None),
            ],
            64,
        )
        .unwrap();
        assert_eq!(
            cache.insert(b"replace", b"new", None).unwrap(),
            SegmentCacheWriteOutcome::Replaced
        );
        assert!(cache.remove(b"delete"));
        assert_eq!(
            cache.insert(b"fresh", b"value", None).unwrap(),
            SegmentCacheWriteOutcome::Inserted
        );

        let stats = cache.compact().unwrap();
        assert_eq!(stats.previous_entries, 3);
        assert_eq!(stats.compacted_entries, 3);
        assert_eq!(cache.generation(), 1);
        assert_eq!(cache.delta_records(), 0);
        let guard = cache.pin();
        assert_eq!(guard.peek(b"replace"), Some(b"new".as_slice()));
        assert!(guard.peek(b"delete").is_none());
        assert_eq!(guard.peek(b"stay"), Some(b"yes".as_slice()));
        assert_eq!(guard.peek(b"fresh"), Some(b"value".as_slice()));

        drop(guard);
        assert_eq!(
            cache.insert(b"replace", b"newer", None).unwrap(),
            SegmentCacheWriteOutcome::Replaced
        );
        assert!(cache.remove(b"fresh"));
        cache.compact().unwrap();
        let guard = cache.pin();
        assert_eq!(guard.peek(b"replace"), Some(b"newer".as_slice()));
        assert!(guard.peek(b"fresh").is_none());
        assert_eq!(guard.peek(b"stay"), Some(b"yes".as_slice()));
    }

    #[test]
    fn online_append_only_compaction_shares_unchanged_payload_segments() {
        const BASE: usize = 128;
        const BATCH: usize = 64;
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry().with_negative_filter_bits_per_entry(1),
            (0..BASE).map(|key| {
                let key = u64::try_from(key).unwrap();
                (key.to_le_bytes(), key.to_be_bytes(), None)
            }),
            256,
        )
        .unwrap();

        for key in BASE..BASE + BATCH {
            let key = u64::try_from(key).unwrap();
            assert_eq!(
                cache
                    .insert(&key.to_le_bytes(), key.to_be_bytes(), None)
                    .unwrap(),
                SegmentCacheWriteOutcome::Inserted
            );
        }
        let first = cache.compact().unwrap();
        assert_eq!(first.reused_entries, BASE);
        assert!(first.reused_record_bytes > 0);
        assert!(first.copied_record_bytes > 0);
        assert_eq!(first.record_segments, 2);
        assert!(first.newly_allocated_generation_bytes < first.new_generation_bytes);

        for key in BASE + BATCH..BASE + BATCH * 2 {
            let key = u64::try_from(key).unwrap();
            cache
                .insert(&key.to_le_bytes(), key.to_be_bytes(), None)
                .unwrap();
        }
        let second = cache.compact().unwrap();
        assert_eq!(second.reused_entries, BASE);
        assert_eq!(second.record_segments, 2);

        let guard = cache.pin();
        for key in 0..BASE + BATCH * 2 {
            let key = u64::try_from(key).unwrap();
            assert_eq!(
                guard.peek(&key.to_le_bytes()),
                Some(key.to_be_bytes().as_slice())
            );
        }
    }

    #[test]
    fn online_update_compaction_falls_back_to_flat_payload() {
        const ENTRIES: usize = 128;
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            (0..ENTRIES).map(|key| {
                let key = u64::try_from(key).unwrap();
                (key.to_le_bytes(), key.to_be_bytes(), None)
            }),
            ENTRIES,
        )
        .unwrap();
        for key in 0..4_u64 {
            cache
                .insert(&key.to_le_bytes(), b"replacement", None)
                .unwrap();
        }

        let stats = cache.compact().unwrap();
        assert_eq!(stats.reused_entries, 0);
        assert_eq!(stats.reused_record_bytes, 0);
        assert_eq!(stats.record_segments, 1);
        assert_eq!(
            stats.newly_allocated_generation_bytes,
            stats.new_generation_bytes
        );
        assert_eq!(
            cache.get_cloned(&0_u64.to_le_bytes()).as_deref(),
            Some(b"replacement".as_slice())
        );
    }

    #[test]
    fn localized_heavy_updates_reuse_only_clean_payload_segments() {
        const ENTRIES: usize = 256;
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            (0..ENTRIES).map(|key| {
                let key = u64::try_from(key).unwrap();
                (
                    key.to_le_bytes(),
                    [u8::try_from(key & 255).unwrap(); 32],
                    None,
                )
            }),
            ENTRIES,
        )
        .unwrap();

        let keys_in_first_segment = {
            let generation = cache.current.load();
            let base = generation.base.load();
            let OnlineSegmentBase::Frozen(frozen) = base.as_ref() else {
                panic!("initial generation is frozen");
            };
            assert!(frozen.record_segment_count() >= 2);
            (0..ENTRIES)
                .filter(|key| {
                    let key = u64::try_from(*key).unwrap().to_le_bytes();
                    frozen
                        .probe_hashed(&key, cache.route_hash.hash_one(key))
                        .is_some_and(|record| record.location >> 32 == 0)
                })
                .collect::<Vec<_>>()
        };
        assert!(keys_in_first_segment.len() > ENTRIES / 100);
        for key in &keys_in_first_segment {
            let key = u64::try_from(*key).unwrap();
            cache.insert(&key.to_le_bytes(), [7_u8; 32], None).unwrap();
        }

        let stats = cache.compact().unwrap();
        assert!(stats.reused_entries > 0);
        assert!(stats.reused_entries < ENTRIES);
        assert!(stats.copied_record_bytes > 0);
        assert!(stats.record_segments <= MAX_SHARED_RECORD_SEGMENTS);
        for key in keys_in_first_segment {
            assert_eq!(
                cache
                    .get_cloned(&u64::try_from(key).unwrap().to_le_bytes())
                    .as_deref(),
                Some([7_u8; 32].as_slice())
            );
        }
    }

    #[test]
    fn segmented_base_fallback_beats_an_older_new_index_record() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [(b"base".as_slice(), b"value".as_slice(), None)],
            64,
        )
        .unwrap();
        cache.insert(b"segmented", b"packed", None).unwrap();
        assert_eq!(cache.compact().unwrap().record_segments, 2);

        let generation = cache.current.load_full();
        let pin = cache.arena.pin();
        let stale = cache.arena.allocate(b"stale", NEVER_EXPIRES);
        generation
            .delta
            .new_membership
            .mark(cache.route_hash.hash_one(b"segmented"));
        generation.delta.new_dirty.store(true, Ordering::Release);
        assert!(publish_delta_record(
            &generation.delta.new_index,
            b"segmented",
            stale.encoded(),
            &pin,
            generation.current_tick(SegmentExpiry::None),
            true,
        ));
        drop(pin);

        cache.insert(b"segmented", b"latest", None).unwrap();
        assert_eq!(
            cache.get_cloned(b"segmented").as_deref(),
            Some(b"latest".as_slice())
        );
    }

    #[test]
    fn compaction_does_not_bypass_a_new_index_shadow() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [(b"base".as_slice(), b"old".as_slice(), None)],
            64,
        )
        .unwrap();
        let generation = cache.current.load_full();
        let pin = cache.arena.pin();
        let shadow = cache.arena.allocate(b"shadow", NEVER_EXPIRES);
        generation
            .delta
            .new_membership
            .mark(cache.route_hash.hash_one(b"base"));
        generation.delta.new_dirty.store(true, Ordering::Release);
        generation
            .new_may_shadow_base
            .store(true, Ordering::Release);
        assert!(publish_delta_record(
            &generation.delta.new_index,
            b"base",
            shadow.encoded(),
            &pin,
            generation.current_tick(SegmentExpiry::None),
            true,
        ));
        drop(pin);
        drop(generation);

        assert_eq!(
            cache.get_cloned(b"base").as_deref(),
            Some(b"shadow".as_slice())
        );
        cache.compact().unwrap();
        assert_eq!(
            cache.get_cloned(b"base").as_deref(),
            Some(b"shadow".as_slice())
        );
    }

    #[test]
    fn online_adaptive_compaction_reclaims_delete_churn_at_its_threshold() {
        const ENTRIES: usize = 100;
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            (0..ENTRIES).map(|key| {
                let key = u64::try_from(key).unwrap();
                (key.to_le_bytes(), key.to_le_bytes(), None)
            }),
            ENTRIES,
        )
        .unwrap();
        assert_eq!(cache.compaction_threshold_records(), 75);
        assert!(!cache.compaction_recommended());
        assert!(cache.compact_if_recommended().unwrap().is_none());

        let writer = cache.pin_writer_batch();
        for key in 0..74_u64 {
            assert!(writer.remove(&key.to_le_bytes()));
        }
        assert!(!cache.compaction_recommended());
        assert!(writer.remove(&74_u64.to_le_bytes()));
        drop(writer);
        assert!(cache.compaction_recommended());

        let stats = cache.compact_if_recommended().unwrap().unwrap();
        assert_eq!(stats.compacted_delta_records, 75);
        assert_eq!(stats.compacted_entries, 25);
        assert_eq!(cache.delta_records(), 0);
        assert!(!cache.compaction_recommended());
        assert!(cache.compact_if_recommended().unwrap().is_none());
    }

    #[test]
    fn online_pinless_delete_linearizes_against_a_racing_update() {
        const KEYS: usize = 256;
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            (0..KEYS).map(|key| {
                (
                    u64::try_from(key).unwrap().to_le_bytes(),
                    b"base".as_slice(),
                    None,
                )
            }),
            KEYS,
        )
        .unwrap();

        for key in 0..KEYS {
            let key = u64::try_from(key).unwrap().to_le_bytes();
            let barrier = std::sync::Barrier::new(2);
            let (inserted, removed) = thread::scope(|scope| {
                let insert = scope.spawn(|| {
                    barrier.wait();
                    cache.insert(&key, b"updated", None).unwrap()
                });
                let remove = scope.spawn(|| {
                    barrier.wait();
                    cache.remove(&key)
                });
                (insert.join().unwrap(), remove.join().unwrap())
            });
            assert!(removed);
            match inserted {
                SegmentCacheWriteOutcome::Inserted => {
                    assert_eq!(
                        cache.get_cloned(&key).as_deref(),
                        Some(b"updated".as_slice())
                    );
                }
                SegmentCacheWriteOutcome::Replaced => assert!(cache.get_cloned(&key).is_none()),
            }
        }
        assert_eq!(cache.delta_records(), KEYS);
    }

    #[test]
    fn online_lazy_new_membership_initializes_once_under_concurrent_inserts() {
        const WORKERS: usize = 8;
        const KEYS_PER_WORKER: usize = 8;
        const CAPACITY: usize = WORKERS * KEYS_PER_WORKER;
        let cache = Arc::new(
            OnlineMutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                std::iter::empty::<([u8; 8], [u8; 8], Option<Duration>)>(),
                CAPACITY,
            )
            .unwrap(),
        );
        let initial_filter_bytes = cache.delta_filter_bytes();
        assert_eq!(initial_filter_bytes, 72);
        let start = Arc::new(std::sync::Barrier::new(WORKERS));
        let joins = (0..WORKERS)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for local in 0..KEYS_PER_WORKER {
                        let key = u64::try_from(worker * KEYS_PER_WORKER + local)
                            .unwrap()
                            .to_le_bytes();
                        cache.insert(&key, key, None).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(cache.delta_filter_bytes(), initial_filter_bytes + 64);
        assert_eq!(cache.delta_records(), CAPACITY);
        let guard = cache.pin();
        for key in 0..CAPACITY {
            let key = u64::try_from(key).unwrap().to_le_bytes();
            assert_eq!(guard.peek(&key), Some(key.as_slice()));
        }
    }

    #[test]
    fn online_generations_keep_one_shared_route_hash() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [(b"key".as_slice(), b"value".as_slice(), None)],
            64,
        )
        .unwrap();
        let expected = cache.route_hash.hash_one(b"key");
        let generation = cache.current.load();
        let base_guard = generation.base.load();
        let OnlineSegmentBase::Frozen(base) = base_guard.as_ref() else {
            panic!("initial online generation is frozen");
        };
        assert_eq!(base.key_hash(b"key"), expected);
        drop(base_guard);
        drop(generation);

        cache.compact().unwrap();
        let generation = cache.current.load();
        let base_guard = generation.base.load();
        let OnlineSegmentBase::Frozen(base) = base_guard.as_ref() else {
            panic!("compacted online generation is frozen");
        };
        assert_eq!(base.key_hash(b"key"), expected);
    }

    #[test]
    fn online_compaction_redirects_concurrent_distinct_writers_without_loss() {
        const WORKERS: usize = 8;
        const KEYS_PER_WORKER: usize = 64;
        const ROUNDS: usize = 2_000;
        let cache = Arc::new(
            OnlineMutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                (0..16_384_usize).map(|key| {
                    (
                        u64::try_from(key).unwrap().to_le_bytes(),
                        0_u64.to_le_bytes(),
                        None,
                    )
                }),
                8_192,
            )
            .unwrap(),
        );
        let start = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        let joins = (0..WORKERS)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for round in 1..=ROUNDS {
                        for local in 0..KEYS_PER_WORKER {
                            let key = worker * KEYS_PER_WORKER + local;
                            cache
                                .insert(
                                    &u64::try_from(key).unwrap().to_le_bytes(),
                                    u64::try_from(round).unwrap().to_le_bytes(),
                                    None,
                                )
                                .unwrap();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        cache.compact().unwrap();
        for join in joins {
            join.join().unwrap();
        }

        let guard = cache.pin();
        for key in 0..WORKERS * KEYS_PER_WORKER {
            assert_eq!(
                guard.peek(&u64::try_from(key).unwrap().to_le_bytes()),
                Some(u64::try_from(ROUNDS).unwrap().to_le_bytes().as_slice())
            );
        }
    }

    #[test]
    fn online_publication_waits_for_a_prepublication_read_batch() {
        let cache = Arc::new(
            OnlineMutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                (0..32_768_usize).map(|key| {
                    (
                        u64::try_from(key).unwrap().to_le_bytes(),
                        0_u64.to_le_bytes(),
                        None,
                    )
                }),
                4_096,
            )
            .unwrap(),
        );
        let rebuilding = Arc::clone(&cache);
        let join = thread::spawn(move || rebuilding.compact().unwrap());

        loop {
            let current = cache.current.load();
            if matches!(
                current.base.load().as_ref(),
                OnlineSegmentBase::Previous { .. }
            ) {
                break;
            }
            thread::yield_now();
        }
        let old_snapshot = cache.pin();
        old_snapshot
            .insert(b"during-build", b"visible", None)
            .unwrap();
        assert_eq!(
            old_snapshot.peek(b"during-build"),
            Some(b"visible".as_slice())
        );
        while cache
            .current
            .load()
            .active_read_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            thread::yield_now();
        }
        let postpublication_snapshot = cache.pin();
        assert!(postpublication_snapshot.frozen.is_some());
        assert_eq!(
            postpublication_snapshot.peek(b"during-build"),
            Some(b"visible".as_slice())
        );
        drop(old_snapshot);
        join.join().unwrap();

        cache.insert(b"during-build", b"direct", None).unwrap();
        assert_eq!(
            postpublication_snapshot.peek(b"during-build"),
            Some(b"direct".as_slice())
        );
    }

    #[test]
    fn online_guards_and_compaction_do_not_restart_ttl() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_millis(1)),
            [(
                b"base".as_slice(),
                b"value".as_slice(),
                Some(Duration::from_millis(300)),
            )],
            64,
        )
        .unwrap();
        cache
            .insert(b"delta", b"value", Some(Duration::from_millis(300)))
            .unwrap();
        let snapshot = cache.pin();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(snapshot.peek(b"base"), Some(b"value".as_slice()));
        assert_eq!(snapshot.peek(b"delta"), Some(b"value".as_slice()));
        drop(snapshot);
        cache.compact().unwrap();
        thread::sleep(Duration::from_millis(230));

        let current = cache.pin();
        assert!(current.peek(b"base").is_none());
        assert!(current.peek(b"delta").is_none());
    }

    #[test]
    fn repeated_online_compaction_survives_update_delete_churn() {
        const WORKERS: usize = 4;
        const KEYS_PER_WORKER: usize = 128;
        const ROUNDS: usize = 4_000;
        let cache = Arc::new(
            OnlineMutableSegmentCache::try_from_entries(
                SegmentCacheConfig::without_expiry(),
                (0..4_096_usize).map(|key| {
                    (
                        u64::try_from(key).unwrap().to_le_bytes(),
                        0_u64.to_le_bytes(),
                        None,
                    )
                }),
                8_192,
            )
            .unwrap(),
        );
        let start = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        let joins = (0..WORKERS)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for round in 0..ROUNDS {
                        let key = worker * KEYS_PER_WORKER + round % KEYS_PER_WORKER;
                        let key = u64::try_from(key).unwrap().to_le_bytes();
                        if round % 7 == 0 {
                            let _ = cache.remove(&key);
                        } else {
                            cache
                                .insert(&key, u64::try_from(round).unwrap().to_le_bytes(), None)
                                .unwrap();
                        }
                    }
                    for local in 0..KEYS_PER_WORKER {
                        let key = worker * KEYS_PER_WORKER + local;
                        cache
                            .insert(
                                &u64::try_from(key).unwrap().to_le_bytes(),
                                u64::try_from(ROUNDS + worker).unwrap().to_le_bytes(),
                                None,
                            )
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for _ in 0..5 {
            cache.compact().unwrap();
        }
        for join in joins {
            join.join().unwrap();
        }
        cache.compact().unwrap();

        let guard = cache.pin();
        for worker in 0..WORKERS {
            for local in 0..KEYS_PER_WORKER {
                let key = worker * KEYS_PER_WORKER + local;
                assert_eq!(
                    guard.peek(&u64::try_from(key).unwrap().to_le_bytes()),
                    Some(
                        u64::try_from(ROUNDS + worker)
                            .unwrap()
                            .to_le_bytes()
                            .as_slice()
                    )
                );
            }
        }
    }

    #[test]
    fn online_compaction_flattens_a_predecessor_backed_retry_layer() {
        let cache = OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::without_expiry(),
            [
                (b"replace".as_slice(), b"old".as_slice(), None),
                (b"delete".as_slice(), b"gone".as_slice(), None),
            ],
            64,
        )
        .unwrap();
        cache.insert(b"before", b"one", None).unwrap();

        // This is the correct state retained when packing fails after writer
        // redirect: a writable successor whose base is the stable predecessor.
        let previous = cache.current.load_full();
        let previous_base = previous.base.load_full();
        let retry_layer = OnlineSegmentGeneration::with_previous(
            Arc::clone(&previous),
            previous_base,
            cache.delta_capacity,
            Arc::clone(&cache.arena),
        )
        .unwrap();
        cache.current.store(Arc::clone(&retry_layer));
        previous.close_read_batches();
        previous.close_writer_stripes();
        retry_layer.write_predecessor.store(None);
        retry_layer
            .write_predecessor_active
            .store(false, std::sync::atomic::Ordering::Release);

        cache.insert(b"replace", b"new", None).unwrap();
        cache.insert(b"after", b"two", None).unwrap();
        assert!(cache.remove(b"delete"));
        cache.compact().unwrap();

        let guard = cache.pin();
        assert_eq!(guard.peek(b"replace"), Some(b"new".as_slice()));
        assert_eq!(guard.peek(b"before"), Some(b"one".as_slice()));
        assert_eq!(guard.peek(b"after"), Some(b"two".as_slice()));
        assert!(guard.peek(b"delete").is_none());
        assert!(matches!(guard.base.as_ref(), OnlineSegmentBase::Frozen(_)));
    }

    fn variable_key(index: usize) -> Vec<u8> {
        let prefix = index % 23;
        let mut key = vec![u8::try_from(prefix).unwrap(); prefix];
        key.extend_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
        key
    }

    fn value_for(index: usize, version: usize) -> Vec<u8> {
        let mut value = vec![u8::try_from(version & 255).unwrap(); (index % 31) + 1];
        value.extend_from_slice(&u64::try_from(version).unwrap().to_le_bytes());
        value
    }
}
