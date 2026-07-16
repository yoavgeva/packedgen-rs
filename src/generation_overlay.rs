use std::hash::BuildHasher;
use std::hash::{Hash, Hasher};
use std::hint::spin_loop;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwapOption;
use hashbrown::DefaultHashBuilder;
use papaya::HashMap;

use crate::generation_hash::{GenerationHashBuilder, GenerationKeyHash};
use crate::overlay_cell::StableCell;

const COMPACT_KEY_BYTES: usize = 32;
const TARGET_NUMERATOR: usize = 5;
const TARGET_DENOMINATOR: usize = 4;
const MAX_SHARDS: usize = 64;
const TARGET_ENTRIES_PER_SHARD: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationOverlayMode {
    Papaya,
    AtomicFixed32,
    CompactFixed32,
    CompactSized(u8),
    ArcSwapFixed32,
}

pub(crate) enum OverlayInsert<R> {
    Inserted,
    Occupied(R),
}

/// Stable-cell overlay with optional exact-width inline-key front tables.
///
/// The default atomic path stores exact 32-byte keys and values in preallocated
/// lock-free buckets, with Papaya overflow for saturated buckets and other key
/// widths. Explicit common-width Papaya classes and the atomic-pointer table
/// remain available as measured controls.
pub(crate) struct GenerationOverlay<C> {
    atomic_fixed32: Option<AtomicFixed32Overlay<C>>,
    fixed32: Option<HashMap<InlineKey32, C, DefaultHashBuilder>>,
    inline_variable: Option<InlineVariableOverlay<C>>,
    arc_swap_fixed32: Option<CompactOverlay<C>>,
    fallback: HashMap<Box<[u8]>, OverflowCell<C>, DefaultHashBuilder>,
}

impl<C: StableCell> GenerationOverlay<C> {
    pub(crate) fn with_capacity(
        capacity: usize,
        mode: GenerationOverlayMode,
        atomic_hash_builder: GenerationHashBuilder,
    ) -> Self {
        match mode {
            GenerationOverlayMode::Papaya => Self {
                atomic_fixed32: None,
                fixed32: None,
                inline_variable: None,
                arc_swap_fixed32: None,
                fallback: HashMap::with_capacity_and_hasher(
                    capacity,
                    DefaultHashBuilder::default(),
                ),
            },
            GenerationOverlayMode::AtomicFixed32 => Self {
                atomic_fixed32: Some(AtomicFixed32Overlay::with_capacity(
                    capacity,
                    atomic_hash_builder,
                )),
                fixed32: None,
                inline_variable: None,
                arc_swap_fixed32: None,
                fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
            },
            GenerationOverlayMode::CompactFixed32 => Self {
                atomic_fixed32: None,
                fixed32: Some(HashMap::with_capacity_and_hasher(
                    capacity,
                    DefaultHashBuilder::default(),
                )),
                inline_variable: None,
                arc_swap_fixed32: None,
                fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
            },
            GenerationOverlayMode::CompactSized(key_bytes) => {
                let class = InlineClass::for_key_bytes(usize::from(key_bytes));
                Self {
                    atomic_fixed32: None,
                    fixed32: None,
                    inline_variable: class
                        .map(|class| InlineVariableOverlay::with_capacity(class, capacity)),
                    arc_swap_fixed32: None,
                    fallback: HashMap::with_capacity_and_hasher(
                        usize::from(class.is_none()) * capacity,
                        DefaultHashBuilder::default(),
                    ),
                }
            }
            GenerationOverlayMode::ArcSwapFixed32 => Self {
                atomic_fixed32: None,
                fixed32: None,
                inline_variable: None,
                arc_swap_fixed32: Some(CompactOverlay::with_capacity(capacity)),
                fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
            },
        }
    }

    pub(crate) fn with_cell<R>(&self, key: &[u8], read: impl FnOnce(Option<&C>) -> R) -> R {
        self.with_cell_with_hash(key, None, read)
    }

    pub(crate) fn with_cell_prehashed<R>(
        &self,
        key: &[u8],
        key_hash: u64,
        read: impl FnOnce(Option<&C>) -> R,
    ) -> R {
        self.with_cell_with_hash(key, Some(key_hash), read)
    }

    fn with_cell_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        read: impl FnOnce(Option<&C>) -> R,
    ) -> R {
        let mut read = Some(read);
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && let Some(result) = atomic_fixed32.with_cell_hashed(
                key,
                key_hash.unwrap_or_else(|| atomic_fixed32.hash(key)),
                |cell| read.take().expect("overlay read callback runs once")(Some(cell)),
            )
        {
            return result;
        }
        if let Some(fixed32) = &self.fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            let fixed32 = fixed32.pin();
            let fixed_key = InlineKey32::from_slice(key);
            return read.take().expect("overlay read callback runs once")(fixed32.get(&fixed_key));
        }
        if let Some(inline_variable) = &self.inline_variable
            && inline_variable.supports(key)
        {
            return inline_variable
                .with_cell(key, read.take().expect("overlay read callback runs once"));
        }
        if let Some(compact) = &self.arc_swap_fixed32
            && let Some(result) = compact.with_cell(key, |cell| {
                read.take().expect("overlay read callback runs once")(Some(cell))
            })
        {
            return result;
        }

        let fallback = self.fallback.pin();
        read.take().expect("overlay read callback runs once")(
            fallback.get(key).map(OverflowCell::cell),
        )
    }

    pub(crate) fn insert_or_visit_prehashed<R>(
        &self,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        self.insert_or_visit_with_hash(key, Some(key_hash), cell, occupied)
    }

    fn insert_or_visit_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut occupied = Some(occupied);
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            return match atomic_fixed32.insert_or_return_hashed(
                key,
                key_hash.unwrap_or_else(|| atomic_fixed32.hash(key)),
                cell,
            ) {
                AtomicFixedInsert::Inserted => OverlayInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    atomic_fixed32.cell(index),
                )),
                AtomicFixedInsert::Full(cell) => self.insert_fallback(
                    key,
                    OverflowCell::Direct(cell),
                    occupied
                        .take()
                        .expect("overlay occupied callback runs once"),
                ),
            };
        }
        if let Some(fixed32) = &self.fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            let fixed32 = fixed32.pin();
            return match fixed32.try_insert(InlineKey32::from_slice(key), cell) {
                Ok(_) => OverlayInsert::Inserted,
                Err(error) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    error.current
                )),
            };
        }
        if let Some(inline_variable) = &self.inline_variable
            && inline_variable.supports(key)
        {
            return inline_variable.insert_or_visit(
                key,
                cell,
                occupied
                    .take()
                    .expect("overlay occupied callback runs once"),
            );
        }
        if let Some(compact) = &self.arc_swap_fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            return match compact.insert_or_return(key, cell) {
                CompactInsert::Inserted => OverlayInsert::Inserted,
                CompactInsert::Occupied(entry) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    &entry.cell
                )),
                CompactInsert::Full(entry) => self.insert_fallback(
                    key,
                    OverflowCell::Compact(entry),
                    occupied
                        .take()
                        .expect("overlay occupied callback runs once"),
                ),
            };
        }

        self.insert_fallback(
            key,
            OverflowCell::Direct(cell),
            occupied
                .take()
                .expect("overlay occupied callback runs once"),
        )
    }

    /// Probes before inserting while retaining the same pinned table view.
    ///
    /// Papaya's preliminary read helps stagger contended inserts, but entering
    /// a guard once for the read and again for the insert adds avoidable work.
    /// This preserves the read-first behavior and reuses one guard.
    pub(crate) fn probe_or_insert_prehashed<R>(
        &self,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        self.probe_or_insert_with_hash(key, Some(key_hash), cell, occupied)
    }

    fn probe_or_insert_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut occupied = Some(occupied);
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            return match atomic_fixed32.insert_or_return_hashed(
                key,
                key_hash.unwrap_or_else(|| atomic_fixed32.hash(key)),
                cell,
            ) {
                AtomicFixedInsert::Inserted => OverlayInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    atomic_fixed32.cell(index),
                )),
                AtomicFixedInsert::Full(cell) => self.probe_or_insert_fallback(
                    key,
                    OverflowCell::Direct(cell),
                    occupied
                        .take()
                        .expect("overlay occupied callback runs once"),
                ),
            };
        }
        if let Some(fixed32) = &self.fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            let fixed32 = fixed32.pin();
            let fixed_key = InlineKey32::from_slice(key);
            if let Some(current) = fixed32.get(&fixed_key) {
                return OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    current
                ));
            }
            return match fixed32.try_insert(fixed_key, cell) {
                Ok(_) => OverlayInsert::Inserted,
                Err(error) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    error.current
                )),
            };
        }
        if let Some(inline_variable) = &self.inline_variable
            && inline_variable.supports(key)
        {
            return inline_variable.probe_or_insert(
                key,
                cell,
                occupied
                    .take()
                    .expect("overlay occupied callback runs once"),
            );
        }
        if let Some(compact) = &self.arc_swap_fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            if let Some(result) = compact.with_cell(key, |current| {
                occupied
                    .take()
                    .expect("overlay occupied callback runs once")(current)
            }) {
                return OverlayInsert::Occupied(result);
            }
            return match compact.insert_or_return(key, cell) {
                CompactInsert::Inserted => OverlayInsert::Inserted,
                CompactInsert::Occupied(entry) => OverlayInsert::Occupied(occupied
                    .take()
                    .expect("overlay occupied callback runs once")(
                    &entry.cell
                )),
                CompactInsert::Full(entry) => self.probe_or_insert_fallback(
                    key,
                    OverflowCell::Compact(entry),
                    occupied
                        .take()
                        .expect("overlay occupied callback runs once"),
                ),
            };
        }

        self.probe_or_insert_fallback(
            key,
            OverflowCell::Direct(cell),
            occupied
                .take()
                .expect("overlay occupied callback runs once"),
        )
    }

    pub(crate) fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        if let Some(atomic_fixed32) = &self.atomic_fixed32 {
            atomic_fixed32.for_each(visit);
        }
        if let Some(fixed32) = &self.fixed32 {
            let fixed32 = fixed32.pin();
            for (key, cell) in &fixed32 {
                visit(&key.0, cell);
            }
        }
        if let Some(inline_variable) = &self.inline_variable {
            inline_variable.for_each(visit);
        }
        if let Some(compact) = &self.arc_swap_fixed32 {
            compact.for_each(visit);
        }
        let fallback = self.fallback.pin();
        for (key, cell) in &fallback {
            visit(key, cell.cell());
        }
    }

    fn insert_fallback<R>(
        &self,
        key: &[u8],
        cell: OverflowCell<C>,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let fallback = self.fallback.pin();
        match fallback.try_insert(key.into(), cell) {
            Ok(_) => OverlayInsert::Inserted,
            Err(error) => OverlayInsert::Occupied(occupied(error.current.cell())),
        }
    }

    fn probe_or_insert_fallback<R>(
        &self,
        key: &[u8],
        cell: OverflowCell<C>,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let fallback = self.fallback.pin();
        let mut occupied = Some(occupied);
        if let Some(current) = fallback.get(key) {
            return OverlayInsert::Occupied(occupied
                .take()
                .expect("overlay occupied callback runs once")(
                current.cell()
            ));
        }
        match fallback.try_insert(key.into(), cell) {
            Ok(_) => OverlayInsert::Inserted,
            Err(error) => OverlayInsert::Occupied(occupied
                .take()
                .expect("overlay occupied callback runs once")(
                error.current.cell()
            )),
        }
    }
}

const ATOMIC_SLOT_EMPTY: u8 = 0;
const ATOMIC_SLOT_WRITING: u8 = 1;
const ATOMIC_BUCKET_SLOTS: usize = 8;
const ATOMIC_BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const ATOMIC_BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const ATOMIC_BYTE_ONES: u64 = 0x0101_0101_0101_0101;

/// Fixed-capacity bucket overlay for exact 32-byte keys.
///
/// Every slot is fully initialized, so publication needs no unsafe code. A
/// writer claims one byte in an atomic bucket control word, stores four key
/// words and initializes the stable cell, then publishes a hash tag with
/// release ordering. Readers acquire one control word before reading the
/// immutable colocated key and cell.
struct AtomicFixed32Overlay<C> {
    controls: Box<[AtomicU64]>,
    slots: Box<[AtomicFixed32Slot<C>]>,
    bucket_count: usize,
    hash_builder: GenerationHashBuilder,
}

struct AtomicFixed32Slot<C> {
    key_words: [AtomicU64; 4],
    cell: C,
}

impl<C: StableCell> AtomicFixed32Overlay<C> {
    fn with_capacity(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let bucket_count = target_slots.div_ceil(ATOMIC_BUCKET_SLOTS);
        let slots = bucket_count.saturating_mul(ATOMIC_BUCKET_SLOTS);
        Self {
            controls: (0..bucket_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            slots: (0..slots)
                .map(|_| AtomicFixed32Slot {
                    key_words: std::array::from_fn(|_| AtomicU64::new(0)),
                    cell: C::deleted(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_count,
            hash_builder,
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        GenerationKeyHash::new(&self.hash_builder, key).route()
    }

    fn with_cell_hashed<R>(&self, key: &[u8], hash: u64, read: impl FnOnce(&C) -> R) -> Option<R> {
        if key.len() != COMPACT_KEY_BYTES || self.controls.is_empty() {
            return None;
        }
        let tag = atomic_slot_tag(hash);
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        let primary_scan = self.scan_bucket_read(primary, tag, key);
        if let Some(index) = primary_scan.found {
            return Some(read(&self.slots[index].cell));
        }
        if primary_scan.empty.is_none() && secondary != primary {
            let secondary_scan = self.scan_bucket_read(secondary, tag, key);
            if let Some(index) = secondary_scan.found {
                return Some(read(&self.slots[index].cell));
            }
            if secondary_scan.empty.is_none()
                && tertiary != primary
                && tertiary != secondary
                && let Some(index) = self.scan_bucket_read(tertiary, tag, key).found
            {
                return Some(read(&self.slots[index].cell));
            }
        }
        None
    }

    fn insert_or_return_hashed(&self, key: &[u8], hash: u64, cell: C) -> AtomicFixedInsert<C> {
        debug_assert_eq!(key.len(), COMPACT_KEY_BYTES);
        if self.controls.is_empty() {
            return AtomicFixedInsert::Full(cell);
        }
        let tag = atomic_slot_tag(hash);
        let mut cell = Some(cell);
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        loop {
            let primary_scan = self.scan_bucket_write(primary, tag, key);
            if let Some(index) = primary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = primary_scan.empty {
                if self.claim_and_publish(index, tag, key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let secondary_scan = if secondary == primary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(secondary, tag, key)
            };
            if let Some(index) = secondary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = secondary_scan.empty {
                if self.claim_and_publish(index, tag, key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let tertiary_scan = if tertiary == primary || tertiary == secondary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(tertiary, tag, key)
            };
            if let Some(index) = tertiary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            let Some(index) = tertiary_scan.empty else {
                return AtomicFixedInsert::Full(
                    cell.take()
                        .expect("full atomic buckets retain candidate cell"),
                );
            };
            if self.claim_and_publish(index, tag, key, &mut cell) {
                return AtomicFixedInsert::Inserted;
            }
        }
    }

    fn cell(&self, index: usize) -> &C {
        &self.slots[index].cell
    }

    fn scan_bucket_read(&self, bucket: usize, tag: u8, key: &[u8]) -> BucketScan {
        let begin = bucket * ATOMIC_BUCKET_SLOTS;
        let control = self.controls[bucket].load(Ordering::Acquire);
        let stop = first_control_offset(control, ATOMIC_SLOT_EMPTY)
            .into_iter()
            .chain(first_control_offset(control, ATOMIC_SLOT_WRITING))
            .min()
            .unwrap_or(ATOMIC_BUCKET_SLOTS);
        let mut candidates = matching_control_bytes(control, tag);
        while let Some(offset) = take_control_offset(&mut candidates) {
            if offset >= stop {
                break;
            }
            let index = begin + offset;
            if self.key_matches(index, key) {
                return BucketScan {
                    found: Some(index),
                    empty: None,
                };
            }
        }
        BucketScan {
            found: None,
            // A read may linearize before an in-progress insertion publishes,
            // so an unpublished writing slot is an available stopping point.
            empty: (stop < ATOMIC_BUCKET_SLOTS).then_some(begin + stop),
        }
    }

    fn scan_bucket_write(&self, bucket: usize, tag: u8, key: &[u8]) -> BucketScan {
        let begin = bucket * ATOMIC_BUCKET_SLOTS;
        loop {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let writing = first_control_offset(control, ATOMIC_SLOT_WRITING);
            let empty = first_control_offset(control, ATOMIC_SLOT_EMPTY);
            let stop = empty
                .into_iter()
                .chain(writing)
                .min()
                .unwrap_or(ATOMIC_BUCKET_SLOTS);
            let mut candidates = matching_control_bytes(control, tag);
            while let Some(offset) = take_control_offset(&mut candidates) {
                if offset >= stop {
                    break;
                }
                let index = begin + offset;
                if self.key_matches(index, key) {
                    return BucketScan {
                        found: Some(index),
                        empty: None,
                    };
                }
            }
            if writing.is_some() {
                spin_loop();
                continue;
            }
            return BucketScan {
                found: None,
                empty: empty.map(|offset| begin + offset),
            };
        }
    }

    fn claim_and_publish(&self, index: usize, tag: u8, key: &[u8], cell: &mut Option<C>) -> bool {
        let bucket = index / ATOMIC_BUCKET_SLOTS;
        let offset = index % ATOMIC_BUCKET_SLOTS;
        let shift = offset * 8;
        let writing = u64::from(ATOMIC_SLOT_WRITING) << shift;
        let control = self.controls[bucket].load(Ordering::Relaxed);
        if control_byte(control, offset) != ATOMIC_SLOT_EMPTY
            || self.controls[bucket]
                .compare_exchange(
                    control,
                    control | writing,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return false;
        }
        self.write_key(index, key);
        self.slots[index].cell.initialize_from(
            cell.take()
                .expect("claimed atomic slot owns candidate cell"),
        );
        let published = u64::from(tag - ATOMIC_SLOT_WRITING) << shift;
        self.controls[bucket].fetch_add(published, Ordering::Release);
        true
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        for bucket in 0..self.bucket_count {
            let control = loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                if first_control_offset(control, ATOMIC_SLOT_WRITING).is_none() {
                    break control;
                }
                spin_loop();
            };
            let begin = bucket * ATOMIC_BUCKET_SLOTS;
            let occupied =
                first_control_offset(control, ATOMIC_SLOT_EMPTY).unwrap_or(ATOMIC_BUCKET_SLOTS);
            for offset in 0..occupied {
                let index = begin + offset;
                let key = self.load_key(index);
                visit(&key, &self.slots[index].cell);
            }
        }
    }

    fn key_matches(&self, index: usize, key: &[u8]) -> bool {
        (0..4).all(|word| {
            self.slots[index].key_words[word].load(Ordering::Relaxed) == key_word(key, word)
        })
    }

    fn write_key(&self, index: usize, key: &[u8]) {
        for word in 0..4 {
            self.slots[index].key_words[word].store(key_word(key, word), Ordering::Relaxed);
        }
    }

    fn load_key(&self, index: usize) -> [u8; COMPACT_KEY_BYTES] {
        let mut key = [0_u8; COMPACT_KEY_BYTES];
        for word in 0..4 {
            let begin = word * size_of::<u64>();
            let end = begin + size_of::<u64>();
            key[begin..end].copy_from_slice(
                &self.slots[index].key_words[word]
                    .load(Ordering::Relaxed)
                    .to_ne_bytes(),
            );
        }
        key
    }
}

#[derive(Default)]
struct BucketScan {
    found: Option<usize>,
    empty: Option<usize>,
}

enum AtomicFixedInsert<C> {
    Inserted,
    Occupied(usize),
    Full(C),
}

fn key_word(key: &[u8], word: usize) -> u64 {
    let begin = word * size_of::<u64>();
    let end = begin + size_of::<u64>();
    u64::from_ne_bytes(
        key[begin..end]
            .try_into()
            .expect("fixed32 key word is exactly eight bytes"),
    )
}

fn atomic_slot_tag(hash: u64) -> u8 {
    u8::try_from(hash % 254).expect("hash remainder fits u8") + 2
}

fn control_byte(control: u64, offset: usize) -> u8 {
    u8::try_from((control >> (offset * 8)) & u64::from(u8::MAX))
        .expect("masked control byte fits u8")
}

fn matching_control_bytes(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * ATOMIC_BYTE_ONES);
    !((different & ATOMIC_BYTE_LOW_BITS).wrapping_add(ATOMIC_BYTE_LOW_BITS)
        | different
        | ATOMIC_BYTE_LOW_BITS)
        & ATOMIC_BYTE_HIGH_BITS
}

fn first_control_offset(control: u64, state: u8) -> Option<usize> {
    let matches = matching_control_bytes(control, state);
    (matches != 0)
        .then(|| usize::try_from(matches.trailing_zeros()).expect("u64 bit offset fits usize") / 8)
}

fn take_control_offset(matches: &mut u64) -> Option<usize> {
    if *matches == 0 {
        return None;
    }
    let offset = usize::try_from(matches.trailing_zeros()).expect("u64 bit offset fits usize") / 8;
    *matches &= matches.wrapping_sub(1);
    Some(offset)
}

fn atomic_bucket_choices(hash: u64, buckets: usize) -> (usize, usize, usize) {
    let primary = start_index(hash, buckets);
    if buckets == 1 {
        return (primary, primary, primary);
    }
    let secondary_hash = hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15;
    let mut secondary = start_index(secondary_hash, buckets);
    if secondary == primary {
        secondary = next_index(secondary, buckets);
    }
    if buckets == 2 {
        return (primary, secondary, primary);
    }
    let tertiary_hash = hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93;
    let mut tertiary = start_index(tertiary_hash, buckets);
    while tertiary == primary || tertiary == secondary {
        tertiary = next_index(tertiary, buckets);
    }
    (primary, secondary, tertiary)
}

#[derive(Eq, PartialEq)]
struct InlineKey32([u8; COMPACT_KEY_BYTES]);

impl InlineKey32 {
    fn from_slice(key: &[u8]) -> Self {
        Self(
            key.try_into()
                .expect("inline overlay receives exactly 32-byte keys"),
        )
    }
}

impl Hash for InlineKey32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0);
    }
}

struct InlineVariableOverlay<C> {
    active: InlineClass,
    up_to_8: InlineMap<C, 8>,
    up_to_16: InlineMap<C, 16>,
    up_to_24: InlineMap<C, 24>,
    up_to_31: InlineMap<C, 31>,
    up_to_40: InlineMap<C, 40>,
    up_to_48: InlineMap<C, 48>,
    up_to_56: InlineMap<C, 56>,
    up_to_64: InlineMap<C, 64>,
}

impl<C> InlineVariableOverlay<C> {
    fn with_capacity(active: InlineClass, capacity: usize) -> Self {
        Self {
            active,
            up_to_8: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo8, capacity)),
            up_to_16: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo16, capacity)),
            up_to_24: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo24, capacity)),
            up_to_31: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo31, capacity)),
            up_to_40: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo40, capacity)),
            up_to_48: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo48, capacity)),
            up_to_56: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo56, capacity)),
            up_to_64: InlineMap::with_capacity(active.capacity_for(InlineClass::UpTo64, capacity)),
        }
    }

    fn supports(&self, key: &[u8]) -> bool {
        self.active.contains(key.len())
    }

    fn with_cell<R>(&self, key: &[u8], read: impl FnOnce(Option<&C>) -> R) -> R {
        match key.len() {
            0..=8 => self.up_to_8.with_cell(key, read),
            9..=16 => self.up_to_16.with_cell(key, read),
            17..=24 => self.up_to_24.with_cell(key, read),
            25..=31 => self.up_to_31.with_cell(key, read),
            33..=40 => self.up_to_40.with_cell(key, read),
            41..=48 => self.up_to_48.with_cell(key, read),
            49..=56 => self.up_to_56.with_cell(key, read),
            57..=64 => self.up_to_64.with_cell(key, read),
            32 | 65.. => unreachable!("caller validates inline variable key length"),
        }
    }

    fn insert_or_visit<R>(
        &self,
        key: &[u8],
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        match key.len() {
            0..=8 => self.up_to_8.insert_or_visit(key, cell, occupied),
            9..=16 => self.up_to_16.insert_or_visit(key, cell, occupied),
            17..=24 => self.up_to_24.insert_or_visit(key, cell, occupied),
            25..=31 => self.up_to_31.insert_or_visit(key, cell, occupied),
            33..=40 => self.up_to_40.insert_or_visit(key, cell, occupied),
            41..=48 => self.up_to_48.insert_or_visit(key, cell, occupied),
            49..=56 => self.up_to_56.insert_or_visit(key, cell, occupied),
            57..=64 => self.up_to_64.insert_or_visit(key, cell, occupied),
            32 | 65.. => unreachable!("caller validates inline variable key length"),
        }
    }

    fn probe_or_insert<R>(
        &self,
        key: &[u8],
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        match key.len() {
            0..=8 => self.up_to_8.probe_or_insert(key, cell, occupied),
            9..=16 => self.up_to_16.probe_or_insert(key, cell, occupied),
            17..=24 => self.up_to_24.probe_or_insert(key, cell, occupied),
            25..=31 => self.up_to_31.probe_or_insert(key, cell, occupied),
            33..=40 => self.up_to_40.probe_or_insert(key, cell, occupied),
            41..=48 => self.up_to_48.probe_or_insert(key, cell, occupied),
            49..=56 => self.up_to_56.probe_or_insert(key, cell, occupied),
            57..=64 => self.up_to_64.probe_or_insert(key, cell, occupied),
            32 | 65.. => unreachable!("caller validates inline variable key length"),
        }
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        self.up_to_8.for_each(visit);
        self.up_to_16.for_each(visit);
        self.up_to_24.for_each(visit);
        self.up_to_31.for_each(visit);
        self.up_to_40.for_each(visit);
        self.up_to_48.for_each(visit);
        self.up_to_56.for_each(visit);
        self.up_to_64.for_each(visit);
    }
}

struct InlineMap<C, const N: usize> {
    map: HashMap<InlineKey<N>, C, DefaultHashBuilder>,
}

impl<C, const N: usize> InlineMap<C, N> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity_and_hasher(capacity, DefaultHashBuilder::default()),
        }
    }

    fn with_cell<R>(&self, key: &[u8], read: impl FnOnce(Option<&C>) -> R) -> R {
        let map = self.map.pin();
        read(map.get(&InlineKey::from_slice(key)))
    }

    fn insert_or_visit<R>(
        &self,
        key: &[u8],
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let map = self.map.pin();
        match map.try_insert(InlineKey::from_slice(key), cell) {
            Ok(_) => OverlayInsert::Inserted,
            Err(error) => OverlayInsert::Occupied(occupied(error.current)),
        }
    }

    fn probe_or_insert<R>(
        &self,
        key: &[u8],
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let map = self.map.pin();
        let inline_key = InlineKey::from_slice(key);
        let mut occupied = Some(occupied);
        if let Some(current) = map.get(&inline_key) {
            return OverlayInsert::Occupied(occupied
                .take()
                .expect("overlay occupied callback runs once")(
                current
            ));
        }
        match map.try_insert(inline_key, cell) {
            Ok(_) => OverlayInsert::Inserted,
            Err(error) => OverlayInsert::Occupied(occupied
                .take()
                .expect("overlay occupied callback runs once")(
                error.current
            )),
        }
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        let map = self.map.pin();
        for (key, cell) in &map {
            visit(key.as_slice(), cell);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum InlineClass {
    UpTo8,
    UpTo16,
    UpTo24,
    UpTo31,
    UpTo40,
    UpTo48,
    UpTo56,
    UpTo64,
}

impl InlineClass {
    const fn for_key_bytes(key_bytes: usize) -> Option<Self> {
        match key_bytes {
            8 => Some(Self::UpTo8),
            16 => Some(Self::UpTo16),
            24 => Some(Self::UpTo24),
            31 => Some(Self::UpTo31),
            40 => Some(Self::UpTo40),
            48 => Some(Self::UpTo48),
            56 => Some(Self::UpTo56),
            64 => Some(Self::UpTo64),
            _ => None,
        }
    }

    fn contains(self, key_bytes: usize) -> bool {
        match self {
            Self::UpTo8 => key_bytes == 8,
            Self::UpTo16 => key_bytes == 16,
            Self::UpTo24 => key_bytes == 24,
            Self::UpTo31 => key_bytes == 31,
            Self::UpTo40 => key_bytes == 40,
            Self::UpTo48 => key_bytes == 48,
            Self::UpTo56 => key_bytes == 56,
            Self::UpTo64 => key_bytes == 64,
        }
    }

    fn capacity_for(self, class: Self, capacity: usize) -> usize {
        if self == class { capacity } else { 0 }
    }
}

#[derive(Eq, PartialEq)]
struct InlineKey<const N: usize>([u8; N]);

impl<const N: usize> InlineKey<N> {
    fn from_slice(key: &[u8]) -> Self {
        Self(
            key.try_into()
                .expect("configured inline key receives its exact key width"),
        )
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> Hash for InlineKey<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0);
    }
}

enum OverflowCell<C> {
    Direct(C),
    Compact(Arc<CompactEntry<C>>),
}

impl<C> OverflowCell<C> {
    fn cell(&self) -> &C {
        match self {
            Self::Direct(cell) => cell,
            Self::Compact(entry) => &entry.cell,
        }
    }
}

struct CompactOverlay<C> {
    shards: Box<[CompactShard<C>]>,
    hash_builder: DefaultHashBuilder,
}

impl<C> CompactOverlay<C> {
    fn with_capacity(capacity: usize) -> Self {
        let shard_count = capacity
            .div_ceil(TARGET_ENTRIES_PER_SHARD)
            .max(1)
            .next_power_of_two()
            .min(MAX_SHARDS);
        let target_slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR)
            .max(shard_count);
        let slots_per_shard = target_slots.div_ceil(shard_count);
        let shards = (0..shard_count)
            .map(|_| CompactShard::with_slots(slots_per_shard))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            hash_builder: DefaultHashBuilder::default(),
        }
    }

    fn with_cell<R>(&self, key: &[u8], read: impl FnOnce(&C) -> R) -> Option<R> {
        if key.len() != COMPACT_KEY_BYTES {
            return None;
        }
        let hash = self.hash_builder.hash_one(key);
        self.shard(hash).with_cell(hash, key, read)
    }

    fn insert_or_return(&self, key: &[u8], cell: C) -> CompactInsert<C> {
        debug_assert_eq!(key.len(), COMPACT_KEY_BYTES);
        let hash = self.hash_builder.hash_one(key);
        self.shard(hash).insert_or_return(hash, key, cell)
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        for shard in &self.shards {
            shard.for_each(visit);
        }
    }

    fn shard(&self, hash: u64) -> &CompactShard<C> {
        let mixed = hash ^ hash.rotate_right(32);
        let index = usize::try_from(mixed).unwrap_or(usize::MAX) % self.shards.len();
        &self.shards[index]
    }
}

struct CompactShard<C> {
    slots: Box<[ArcSwapOption<CompactEntry<C>>]>,
}

impl<C> CompactShard<C> {
    fn with_slots(slots: usize) -> Self {
        Self {
            slots: (0..slots)
                .map(|_| ArcSwapOption::empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn with_cell<R>(&self, hash: u64, key: &[u8], read: impl FnOnce(&C) -> R) -> Option<R> {
        let mut index = start_index(hash, self.slots.len());
        for _ in 0..self.slots.len() {
            let entry = self.slots[index].load();
            let entry = entry.as_ref()?;
            if entry.key == key {
                return Some(read(&entry.cell));
            }
            index = next_index(index, self.slots.len());
        }
        None
    }

    fn insert_or_return(&self, hash: u64, key: &[u8], cell: C) -> CompactInsert<C> {
        let mut candidate = Some(Arc::new(CompactEntry {
            key: key
                .try_into()
                .expect("compact overlay receives exactly 32-byte keys"),
            cell,
        }));
        let mut index = start_index(hash, self.slots.len());
        for _ in 0..self.slots.len() {
            let slot = &self.slots[index];
            let current = slot.load();
            if let Some(entry) = current.as_ref() {
                if entry.key == key {
                    return CompactInsert::Occupied(Arc::clone(entry));
                }
            } else {
                let proposed = Arc::clone(candidate.as_ref().expect("candidate remains owned"));
                let previous = slot.compare_and_swap(&None::<Arc<CompactEntry<C>>>, Some(proposed));
                if previous.is_none() {
                    return CompactInsert::Inserted;
                }
                if previous.as_ref().is_some_and(|entry| entry.key == key) {
                    return CompactInsert::Occupied(Arc::clone(
                        previous.as_ref().expect("checked occupied entry"),
                    ));
                }
            }
            index = next_index(index, self.slots.len());
        }
        CompactInsert::Full(candidate.take().expect("full shard returns candidate"))
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        for slot in &self.slots {
            let entry = slot.load();
            if let Some(entry) = entry.as_ref() {
                visit(&entry.key, &entry.cell);
            }
        }
    }
}

struct CompactEntry<C> {
    key: [u8; COMPACT_KEY_BYTES],
    cell: C,
}

enum CompactInsert<C> {
    Inserted,
    Occupied(Arc<CompactEntry<C>>),
    Full(Arc<CompactEntry<C>>),
}

fn start_index(hash: u64, slots: usize) -> usize {
    usize::try_from(hash).unwrap_or(usize::MAX) % slots
}

const fn next_index(index: usize, slots: usize) -> usize {
    let next = index + 1;
    if next == slots { 0 } else { next }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay_cell::OverlayCell;

    #[test]
    fn whole_word_control_match_returns_exact_offsets() {
        let states = [0_u8, 1, 2, 3, 254, 255, 2, 0];
        let control = states
            .into_iter()
            .enumerate()
            .fold(0_u64, |word, (offset, state)| {
                word | (u64::from(state) << (offset * 8))
            });

        for state in [0_u8, 1, 2, 3, 4, 254, 255] {
            let expected = states
                .iter()
                .enumerate()
                .filter_map(|(offset, candidate)| (*candidate == state).then_some(offset))
                .collect::<Vec<_>>();
            let mut matches = matching_control_bytes(control, state);
            let mut actual = Vec::new();
            while let Some(offset) = take_control_offset(&mut matches) {
                actual.push(offset);
            }
            assert_eq!(actual, expected);
            assert_eq!(
                first_control_offset(control, state),
                expected.first().copied()
            );
        }
    }

    #[test]
    fn read_scan_does_not_wait_for_unrelated_writing_slot() {
        let overlay = AtomicFixed32Overlay::<OverlayCell<u64>>::with_capacity(
            ATOMIC_BUCKET_SLOTS,
            GenerationHashBuilder::default(),
        );
        let key = [3_u8; COMPACT_KEY_BYTES];
        let missing = [4_u8; COMPACT_KEY_BYTES];
        let tag = 42;
        let mut cell = Some(OverlayCell::present(7));
        assert!(overlay.claim_and_publish(0, tag, &key, &mut cell));

        overlay.controls[0].fetch_or(u64::from(ATOMIC_SLOT_WRITING) << 8, Ordering::Relaxed);

        assert_eq!(overlay.scan_bucket_read(0, tag, &key).found, Some(0));
        assert_eq!(overlay.scan_bucket_read(0, tag, &missing).empty, Some(1));
        assert_eq!(overlay.scan_bucket_write(0, tag, &key).found, Some(0));
    }
}
