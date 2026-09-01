use std::hash::BuildHasher;
use std::hash::{Hash, Hasher};
use std::hint::spin_loop;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use hashbrown::DefaultHashBuilder;
use papaya::HashMap;

use crate::dynamic_entry::DynamicEntrySlot;
use crate::generation_hash::{GenerationHashBuilder, GenerationKeyHash};
use crate::overlay_cell::StableCell;

const COMPACT_KEY_BYTES: usize = 32;
const TARGET_NUMERATOR: usize = 5;
const TARGET_DENOMINATOR: usize = 4;
const ATOMIC_TARGET_NUMERATOR: usize = 23;
const ATOMIC_TARGET_DENOMINATOR: usize = 20;
const MAX_SHARDS: usize = 64;
const TARGET_ENTRIES_PER_SHARD: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationOverlayMode {
    Papaya,
    AtomicFixed32,
    AtomicUpTo8,
    AtomicUpTo16,
    AtomicAdaptive,
    AtomicUpTo32,
    CompactFixed32,
    CompactSized(u8),
    ArcSwapFixed32,
}

pub(crate) enum OverlayInsert<R> {
    Inserted,
    Occupied(R),
}

/// Stack-only padded key used by the explicit bulk-admission path.
///
/// Boundary widths and keys outside the compact adaptive classes retain the
/// ordinary encoder. Keeping this representation out of the scalar API lets
/// the bulk loader use a branch-specialized short tail without changing read
/// lookup code or adding resident cache metadata.
#[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
pub(crate) enum AtomicEncodedAdmissionKey {
    UpTo8([u8; 8]),
    UpTo16([u8; 16]),
    UpTo24([u8; 24]),
    UpTo32([u8; 32]),
}

#[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
impl AtomicEncodedAdmissionKey {
    pub(crate) fn prepare(key: &[u8]) -> Option<Self> {
        match key.len() {
            0..=7 => Some(Self::UpTo8(encode_admission_key(key))),
            9..=15 => Some(Self::UpTo16(encode_admission_key(key))),
            17..=23 => Some(Self::UpTo24(encode_admission_key(key))),
            25..=31 => Some(Self::UpTo32(encode_admission_key(key))),
            8 | 16 | 24 | 32 | 33.. => None,
        }
    }
}

#[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
fn encode_admission_key<const N: usize>(key: &[u8]) -> [u8; N] {
    debug_assert!(key.len() < N);
    debug_assert!(key.len() >= N.saturating_sub(8));
    let prefix = N - 8;
    let mut encoded = [0_u8; N];
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    let tail = &key[prefix..];
    match tail.len() {
        0 => {}
        1 => encoded[prefix] = tail[0],
        2 => encoded[prefix..prefix + 2].copy_from_slice(tail),
        3 => encoded[prefix..prefix + 3].copy_from_slice(tail),
        4 => encoded[prefix..prefix + 4].copy_from_slice(tail),
        5 => encoded[prefix..prefix + 5].copy_from_slice(tail),
        6 => encoded[prefix..prefix + 6].copy_from_slice(tail),
        7 => encoded[prefix..prefix + 7].copy_from_slice(tail),
        _ => unreachable!("short admission key tail is at most seven bytes"),
    }
    encoded[N - 1] = u8::try_from(key.len()).expect("short admission key length fits u8");
    encoded
}

#[cfg(feature = "prepared-keys")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AtomicOverlayPreparedSlot {
    pub(crate) owner_id: u64,
    pub(crate) class: u8,
    pub(crate) slot: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdaptiveOverlaySnapshot {
    pub(crate) capacity: usize,
    pub(crate) sample_target: usize,
    pub(crate) sampled: usize,
    pub(crate) sample_key_classes: [usize; ADAPTIVE_DISTRIBUTION_CLASSES],
    pub(crate) learned_insertions: usize,
    pub(crate) learned_key_classes: [usize; ADAPTIVE_DISTRIBUTION_CLASSES],
    pub(crate) planned_atomic_capacities: [usize; ADAPTIVE_KEY_CLASSES],
    pub(crate) short_key_fallback_insertions: usize,
    pub(crate) ready: bool,
}

fn take_occupied<T>(occupied: &mut Option<T>) -> T {
    occupied
        .take()
        .expect("overlay occupied callback runs once")
}

/// Stable-cell overlay with optional exact-width inline-key front tables.
///
/// The atomic paths store exact or bounded variable-width keys in elastic
/// lock-free bucket tables. Adaptive mode adds native stable cells for its
/// learning sample and arbitrary-length residual keys; Papaya remains only as
/// an emergency saturation spill. Explicit common-width Papaya classes and the
/// atomic-pointer table remain available as measured controls.
pub(crate) struct GenerationOverlay<C> {
    adaptive: Option<AdaptiveAtomicOverlay<C>>,
    atomic_fixed8: Option<AtomicFixed8Overlay<C>>,
    atomic_overflow8: Option<AtomicOverflow8<C>>,
    atomic_fixed16: Option<AtomicFixed16Overlay<C>>,
    atomic_overflow16: Option<AtomicOverflow16<C>>,
    atomic_fixed32: Option<AtomicFixed32Overlay<C>>,
    atomic_overflow32: Option<AtomicOverflow32<C>>,
    fixed32: Option<HashMap<InlineKey32, C, DefaultHashBuilder>>,
    inline_variable: Option<InlineVariableOverlay<C>>,
    arc_swap_fixed32: Option<CompactOverlay<C>>,
    fallback: HashMap<Box<[u8]>, OverflowCell<C>, DefaultHashBuilder>,
}

impl<C: StableCell> GenerationOverlay<C> {
    pub(crate) fn fallback_len(&self) -> usize {
        self.fallback.len()
            + self
                .adaptive
                .as_ref()
                .map_or(0, |adaptive| adaptive.sample_fallback.len())
            + self
                .adaptive
                .as_ref()
                .and_then(AdaptiveAtomicOverlay::dynamic_fallback)
                .map_or(0, AtomicDynamicFallback::len)
    }

    pub(crate) fn adaptive_snapshot(&self) -> Option<AdaptiveOverlaySnapshot> {
        let adaptive = self.adaptive.as_ref()?;
        let mut physical_key_classes = [0_usize; ADAPTIVE_DISTRIBUTION_CLASSES];
        if let Some(tables) = adaptive.tables() {
            let mut count = |key: &[u8], _: &C| {
                let class = adaptive_key_class(key.len()).unwrap_or(ADAPTIVE_KEY_CLASSES);
                physical_key_classes[class] = physical_key_classes[class].saturating_add(1);
            };
            tables.fixed8.for_each(&mut count);
            Self::for_each_atomic_overflow(&tables.overflow8, &mut count);
            tables.fixed16.for_each(&mut count);
            Self::for_each_atomic_overflow(&tables.overflow16, &mut count);
            tables.fixed24.for_each(&mut count);
            Self::for_each_atomic_overflow(&tables.overflow24, &mut count);
            tables.fixed32.for_each(&mut count);
            Self::for_each_atomic_overflow(&tables.overflow32, &mut count);
            tables.fixed48.for_each(&mut count);
            Self::for_each_atomic_overflow(&tables.overflow48, &mut count);
        }
        let mut fallback_short_keys = 0_usize;
        adaptive.sample_fallback.for_each(&mut |key, _| {
            let class = adaptive_key_class(key.len()).unwrap_or(ADAPTIVE_KEY_CLASSES);
            physical_key_classes[class] = physical_key_classes[class].saturating_add(1);
            if class < ADAPTIVE_KEY_CLASSES {
                fallback_short_keys = fallback_short_keys.saturating_add(1);
            }
        });
        if let Some(fallback) = adaptive.dynamic_fallback() {
            fallback.for_each(&mut |key, _| {
                let class = adaptive_key_class(key.len()).unwrap_or(ADAPTIVE_KEY_CLASSES);
                physical_key_classes[class] = physical_key_classes[class].saturating_add(1);
                if class < ADAPTIVE_KEY_CLASSES {
                    fallback_short_keys = fallback_short_keys.saturating_add(1);
                }
            });
        }
        let fallback = self.fallback.pin();
        for (key, _) in &fallback {
            let class = adaptive_key_class(key.len()).unwrap_or(ADAPTIVE_KEY_CLASSES);
            physical_key_classes[class] = physical_key_classes[class].saturating_add(1);
            if class < ADAPTIVE_KEY_CLASSES {
                fallback_short_keys = fallback_short_keys.saturating_add(1);
            }
        }
        Some(adaptive.snapshot(physical_key_classes, fallback_short_keys))
    }

    pub(crate) fn adaptive_capacity_pressure(&self, maximum_bps: u16) -> bool {
        self.adaptive
            .as_ref()
            .is_some_and(|adaptive| adaptive.capacity_pressure(maximum_bps, &self.fallback))
    }

    fn atomic8_parts(&self) -> (&AtomicFixed8Overlay<C>, &AtomicOverflow8<C>) {
        (
            self.atomic_fixed8
                .as_ref()
                .expect("atomic fixed8 mode owns a primary table"),
            self.atomic_overflow8
                .as_ref()
                .expect("atomic fixed8 mode owns a compact overflow tier"),
        )
    }

    fn atomic16_parts(&self) -> (&AtomicFixed16Overlay<C>, &AtomicOverflow16<C>) {
        (
            self.atomic_fixed16
                .as_ref()
                .expect("atomic fixed16 mode owns a primary table"),
            self.atomic_overflow16
                .as_ref()
                .expect("atomic fixed16 mode owns a compact overflow tier"),
        )
    }

    fn atomic32_parts(&self) -> (&AtomicFixed32Overlay<C>, &AtomicOverflow32<C>) {
        (
            self.atomic_fixed32
                .as_ref()
                .expect("atomic fixed32 mode owns a primary table"),
            self.atomic_overflow32
                .as_ref()
                .expect("atomic fixed32 mode owns a compact overflow tier"),
        )
    }

    pub(crate) fn with_capacity(
        capacity: usize,
        mode: GenerationOverlayMode,
        atomic_hash_builder: GenerationHashBuilder,
    ) -> Self {
        match mode {
            GenerationOverlayMode::Papaya => Self {
                adaptive: None,
                atomic_fixed8: None,
                atomic_overflow8: None,
                atomic_fixed16: None,
                atomic_overflow16: None,
                atomic_fixed32: None,
                atomic_overflow32: None,
                fixed32: None,
                inline_variable: None,
                arc_swap_fixed32: None,
                fallback: HashMap::with_capacity_and_hasher(
                    capacity,
                    DefaultHashBuilder::default(),
                ),
            },
            GenerationOverlayMode::AtomicFixed32 => {
                Self::with_atomic32(capacity, atomic_hash_builder, AtomicKeyMode::Exact)
            }
            GenerationOverlayMode::AtomicUpTo8 => Self::with_atomic8(capacity, atomic_hash_builder),
            GenerationOverlayMode::AtomicUpTo16 => {
                Self::with_atomic16(capacity, atomic_hash_builder)
            }
            GenerationOverlayMode::AtomicAdaptive => {
                Self::with_atomic_adaptive(capacity, atomic_hash_builder)
            }
            GenerationOverlayMode::AtomicUpTo32 => {
                Self::with_atomic32(capacity, atomic_hash_builder, AtomicKeyMode::UpTo)
            }
            GenerationOverlayMode::CompactFixed32 => Self {
                adaptive: None,
                atomic_fixed8: None,
                atomic_overflow8: None,
                atomic_fixed16: None,
                atomic_overflow16: None,
                atomic_fixed32: None,
                atomic_overflow32: None,
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
                    adaptive: None,
                    atomic_fixed8: None,
                    atomic_overflow8: None,
                    atomic_fixed16: None,
                    atomic_overflow16: None,
                    atomic_fixed32: None,
                    atomic_overflow32: None,
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
                adaptive: None,
                atomic_fixed8: None,
                atomic_overflow8: None,
                atomic_fixed16: None,
                atomic_overflow16: None,
                atomic_fixed32: None,
                atomic_overflow32: None,
                fixed32: None,
                inline_variable: None,
                arc_swap_fixed32: Some(CompactOverlay::with_capacity(capacity)),
                fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
            },
        }
    }

    fn with_atomic8(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        Self {
            adaptive: None,
            atomic_fixed8: Some(AtomicFixed8Overlay::with_capacity_mode(
                capacity,
                hash_builder.clone(),
                AtomicKeyMode::UpTo,
            )),
            atomic_overflow8: Some(AtomicOverflow8::new(
                capacity,
                hash_builder,
                AtomicKeyMode::UpTo,
            )),
            atomic_fixed16: None,
            atomic_overflow16: None,
            atomic_fixed32: None,
            atomic_overflow32: None,
            fixed32: None,
            inline_variable: None,
            arc_swap_fixed32: None,
            fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
        }
    }

    fn with_atomic16(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        Self {
            adaptive: None,
            atomic_fixed8: None,
            atomic_overflow8: None,
            atomic_fixed16: Some(AtomicFixed16Overlay::with_capacity_mode(
                capacity,
                hash_builder.clone(),
                AtomicKeyMode::UpTo,
            )),
            atomic_overflow16: Some(AtomicOverflow16::new(
                capacity,
                hash_builder,
                AtomicKeyMode::UpTo,
            )),
            atomic_fixed32: None,
            atomic_overflow32: None,
            fixed32: None,
            inline_variable: None,
            arc_swap_fixed32: None,
            fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
        }
    }

    fn with_atomic32(
        capacity: usize,
        hash_builder: GenerationHashBuilder,
        key_mode: AtomicKeyMode,
    ) -> Self {
        Self {
            adaptive: None,
            atomic_fixed8: None,
            atomic_overflow8: None,
            atomic_fixed16: None,
            atomic_overflow16: None,
            atomic_fixed32: Some(AtomicFixed32Overlay::with_capacity_mode(
                capacity,
                hash_builder.clone(),
                key_mode,
            )),
            atomic_overflow32: Some(AtomicOverflow32::new(capacity, hash_builder, key_mode)),
            fixed32: None,
            inline_variable: None,
            arc_swap_fixed32: None,
            fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
        }
    }

    fn with_atomic_adaptive(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        Self {
            adaptive: Some(AdaptiveAtomicOverlay::new(capacity, hash_builder)),
            atomic_fixed8: None,
            atomic_overflow8: None,
            atomic_fixed16: None,
            atomic_overflow16: None,
            atomic_fixed32: None,
            atomic_overflow32: None,
            fixed32: None,
            inline_variable: None,
            arc_swap_fixed32: None,
            fallback: HashMap::with_hasher(DefaultHashBuilder::default()),
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

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn prepare_atomic(
        &self,
        key: &[u8],
        key_hash: u64,
    ) -> Option<AtomicOverlayPreparedSlot> {
        let adaptive = self.adaptive.as_ref()?;
        if let Some(slot) = adaptive
            .sample_fallback
            .prepared_index_hashed(key, key_hash)
        {
            return Some(AtomicOverlayPreparedSlot {
                owner_id: adaptive.owner_id(),
                class: u8::try_from(ADAPTIVE_KEY_CLASSES).expect("adaptive sample class fits u8"),
                slot,
            });
        }
        let tables = adaptive.tables()?;
        let class = adaptive_key_class(key.len())?;
        let slot = match class {
            0 => tables.fixed8.prepared_index_hashed(key, key_hash),
            1 => tables.fixed16.prepared_index_hashed(key, key_hash),
            2 => tables.fixed24.prepared_index_hashed(key, key_hash),
            3 => tables.fixed32.prepared_index_hashed(key, key_hash),
            4 => tables.fixed48.prepared_index_hashed(key, key_hash),
            _ => unreachable!("adaptive key class is bounded"),
        }?;
        Some(AtomicOverlayPreparedSlot {
            owner_id: adaptive.owner_id(),
            class: u8::try_from(class).expect("adaptive key class fits u8"),
            slot,
        })
    }

    #[cfg(feature = "prepared-keys")]
    #[inline]
    pub(crate) fn with_prepared_atomic<R>(
        &self,
        key: &[u8],
        prepared: AtomicOverlayPreparedSlot,
        read: impl FnOnce(&C) -> R,
    ) -> Option<R> {
        let adaptive = self.adaptive.as_ref()?;
        if adaptive.owner_id() != prepared.owner_id {
            return None;
        }
        if usize::from(prepared.class) == ADAPTIVE_KEY_CLASSES {
            return adaptive
                .sample_fallback
                .with_prepared_cell(prepared.slot, key, read);
        }
        let tables = adaptive.tables()?;
        match usize::from(prepared.class) {
            0 => tables.fixed8.with_prepared_cell(prepared.slot, key, read),
            1 => tables.fixed16.with_prepared_cell(prepared.slot, key, read),
            2 => tables.fixed24.with_prepared_cell(prepared.slot, key, read),
            3 => tables.fixed32.with_prepared_cell(prepared.slot, key, read),
            4 => tables.fixed48.with_prepared_cell(prepared.slot, key, read),
            _ => None,
        }
    }

    fn with_cell_atomic_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        &self,
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: Option<u64>,
        read: impl FnOnce(Option<&C>) -> R,
    ) -> R {
        let key_hash = key_hash.unwrap_or_else(|| primary.hash(key));
        let mut read = Some(read);
        let primary_probe = primary.probe_cell_hashed(key, key_hash, |cell| {
            read.take().expect("overlay read callback runs once")(Some(cell))
        });
        if let Some(result) = primary_probe.found {
            return result;
        }
        if primary_probe.stops_overflow {
            return read.take().expect("overlay read callback runs once")(None);
        }
        for segment in overflow.segments() {
            let table = segment.load();
            let Some(table) = table.as_ref() else {
                return read.take().expect("overlay read callback runs once")(None);
            };
            let probe = table.probe_cell_hashed(key, key_hash, |cell| {
                read.take().expect("overlay read callback runs once")(Some(cell))
            });
            if let Some(result) = probe.found {
                return result;
            }
            if probe.stops_overflow {
                return read.take().expect("overlay read callback runs once")(None);
            }
        }
        let fallback = self.fallback.pin();
        read.take().expect("overlay read callback runs once")(
            fallback.get(key).map(OverflowCell::cell),
        )
    }

    fn with_cell_adaptive_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: u64,
        read: &mut Option<impl FnOnce(Option<&C>) -> R>,
    ) -> Option<R> {
        let primary_probe =
            primary.probe_cell_hashed(key, key_hash, |cell| take_occupied(read)(Some(cell)));
        if let Some(hit) = primary_probe.found {
            return Some(hit);
        }
        if primary_probe.stops_overflow {
            return None;
        }
        for segment in overflow.segments() {
            let table = segment.load();
            let table = table.as_ref()?;
            let probe =
                table.probe_cell_hashed(key, key_hash, |cell| take_occupied(read)(Some(cell)));
            if let Some(hit) = probe.found {
                return Some(hit);
            }
            if probe.stops_overflow {
                return None;
            }
        }
        None
    }

    fn with_cell_adaptive<R>(
        &self,
        adaptive: &AdaptiveAtomicOverlay<C>,
        key: &[u8],
        key_hash: Option<u64>,
        read: impl FnOnce(Option<&C>) -> R,
    ) -> R {
        let mut read = Some(read);
        let key_hash = key_hash.unwrap_or_else(|| adaptive.hash(key));
        let Some(tables) = adaptive.tables() else {
            let sample = adaptive
                .sample_fallback
                .probe_cell_hashed(key, key_hash, |cell| take_occupied(&mut read)(Some(cell)));
            if let Some(hit) = sample.found {
                return hit;
            }
            let fallback = self.fallback.pin();
            return take_occupied(&mut read)(fallback.get(key).map(OverflowCell::cell));
        };
        let hit = match adaptive_key_class(key.len()) {
            Some(0) => Self::with_cell_adaptive_primary(
                &tables.fixed8,
                &tables.overflow8,
                key,
                key_hash,
                &mut read,
            ),
            Some(1) => Self::with_cell_adaptive_primary(
                &tables.fixed16,
                &tables.overflow16,
                key,
                key_hash,
                &mut read,
            ),
            Some(2) => Self::with_cell_adaptive_primary(
                &tables.fixed24,
                &tables.overflow24,
                key,
                key_hash,
                &mut read,
            ),
            Some(3) => Self::with_cell_adaptive_primary(
                &tables.fixed32,
                &tables.overflow32,
                key,
                key_hash,
                &mut read,
            ),
            Some(4) => Self::with_cell_adaptive_primary(
                &tables.fixed48,
                &tables.overflow48,
                key,
                key_hash,
                &mut read,
            ),
            Some(_) => unreachable!("adaptive key class is bounded"),
            None => None,
        };
        if let Some(hit) = hit {
            return hit;
        }
        let sample_may_contain = adaptive.sample_may_contain(key_hash);
        let fallback_may_contain = adaptive.fallback_may_contain(key_hash);
        if sample_may_contain || key.len() > 48 || fallback_may_contain {
            if sample_may_contain {
                let sample = adaptive
                    .sample_fallback
                    .probe_cell_hashed(key, key_hash, |cell| take_occupied(&mut read)(Some(cell)));
                if let Some(hit) = sample.found {
                    return hit;
                }
                let fallback = self.fallback.pin();
                if let Some(cell) = fallback.get(key) {
                    return take_occupied(&mut read)(Some(cell.cell()));
                }
            }
            if (key.len() > 48 || fallback_may_contain)
                && let Some(fallback) = adaptive.dynamic_fallback()
            {
                let probe = fallback
                    .probe_cell_hashed(key, key_hash, |cell| take_occupied(&mut read)(Some(cell)));
                if let Some(hit) = probe.found {
                    return hit;
                }
            }
            return take_occupied(&mut read)(None);
        }
        take_occupied(&mut read)(None)
    }

    fn with_cell_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        read: impl FnOnce(Option<&C>) -> R,
    ) -> R {
        let mut read = Some(read);
        if let Some(adaptive) = &self.adaptive {
            return self.with_cell_adaptive(adaptive, key, key_hash, take_occupied(&mut read));
        }
        if let Some(atomic_fixed8) = &self.atomic_fixed8
            && atomic_fixed8.supports(key)
        {
            return self.with_cell_atomic_primary(
                atomic_fixed8,
                self.atomic_overflow8
                    .as_ref()
                    .expect("atomic fixed8 mode owns a compact overflow tier"),
                key,
                key_hash,
                read.take().expect("overlay read callback runs once"),
            );
        }
        if let Some(atomic_fixed16) = &self.atomic_fixed16
            && atomic_fixed16.supports(key)
        {
            return self.with_cell_atomic_primary(
                atomic_fixed16,
                self.atomic_overflow16
                    .as_ref()
                    .expect("atomic fixed16 mode owns a compact overflow tier"),
                key,
                key_hash,
                read.take().expect("overlay read callback runs once"),
            );
        }
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && atomic_fixed32.supports(key)
        {
            return self.with_cell_atomic_primary(
                atomic_fixed32,
                self.atomic_overflow32
                    .as_ref()
                    .expect("atomic fixed32 mode owns a compact overflow tier"),
                key,
                key_hash,
                read.take().expect("overlay read callback runs once"),
            );
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

    fn insert_adaptive_atomic_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: &mut Option<impl FnOnce(&C) -> R>,
    ) -> AdaptiveAtomicInsert<C, R> {
        let mut cell = match primary.insert_or_return_hashed(key, key_hash, cell) {
            AtomicFixedInsert::Inserted => return AdaptiveAtomicInsert::Inserted,
            AtomicFixedInsert::Occupied(index) => {
                return AdaptiveAtomicInsert::Occupied(take_occupied(occupied)(
                    primary.cell(index),
                ));
            }
            AtomicFixedInsert::Full(cell) => cell,
        };
        for (segment_index, segment) in overflow.segments().iter().enumerate() {
            let table = segment
                .load_full()
                .unwrap_or_else(|| overflow.table(segment_index));
            match table.insert_or_return_hashed(key, key_hash, cell) {
                AtomicFixedInsert::Inserted => return AdaptiveAtomicInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => {
                    return AdaptiveAtomicInsert::Occupied(take_occupied(occupied)(
                        table.cell(index),
                    ));
                }
                AtomicFixedInsert::Full(returned) => cell = returned,
            }
        }
        AdaptiveAtomicInsert::Full(cell)
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    fn insert_adaptive_encoded_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        encoded: &[u8; KEY_BYTES],
        key_hash: u64,
        cell: C,
        occupied: &mut Option<impl FnOnce(&C) -> R>,
    ) -> AdaptiveAtomicInsert<C, R> {
        let mut cell =
            match primary.insert_or_return_encoded_hashed(key.len(), encoded, key_hash, cell) {
                AtomicFixedInsert::Inserted => return AdaptiveAtomicInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => {
                    return AdaptiveAtomicInsert::Occupied(take_occupied(occupied)(
                        primary.cell(index),
                    ));
                }
                AtomicFixedInsert::Full(cell) => cell,
            };
        for (segment_index, segment) in overflow.segments().iter().enumerate() {
            let table = segment
                .load_full()
                .unwrap_or_else(|| overflow.table(segment_index));
            match table.insert_or_return_encoded_hashed(key.len(), encoded, key_hash, cell) {
                AtomicFixedInsert::Inserted => return AdaptiveAtomicInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => {
                    return AdaptiveAtomicInsert::Occupied(take_occupied(occupied)(
                        table.cell(index),
                    ));
                }
                AtomicFixedInsert::Full(returned) => cell = returned,
            }
        }
        AdaptiveAtomicInsert::Full(cell)
    }

    #[inline(never)]
    fn probe_adaptive_sample<R>(
        &self,
        adaptive: &AdaptiveAtomicOverlay<C>,
        key: &[u8],
        key_hash: u64,
        occupied: &mut Option<impl FnOnce(&C) -> R>,
    ) -> Option<OverlayInsert<R>> {
        let sample = adaptive
            .sample_fallback
            .probe_cell_hashed(key, key_hash, |cell| take_occupied(occupied)(cell));
        if let Some(result) = sample.found {
            return Some(OverlayInsert::Occupied(result));
        }
        let fallback = self.fallback.pin();
        fallback
            .get(key)
            .map(|current| OverlayInsert::Occupied(take_occupied(occupied)(current.cell())))
    }

    fn insert_or_visit_adaptive_ready<R>(
        &self,
        ready: (&AdaptiveAtomicOverlay<C>, &AdaptiveAtomicTables<C>),
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
        _probe_fallback: bool,
    ) -> OverlayInsert<R> {
        let (adaptive, tables) = ready;
        let Some(class) = adaptive_key_class(key.len()) else {
            adaptive.mark_fallback(key_hash);
            return self.insert_adaptive_fallback(adaptive, key, key_hash, cell, occupied);
        };

        let mut occupied = Some(occupied);
        if adaptive.sample_may_contain(key_hash)
            && let Some(result) = self.probe_adaptive_sample(adaptive, key, key_hash, &mut occupied)
        {
            return result;
        }

        let inserted = match class {
            0 => Self::insert_adaptive_atomic_primary(
                &tables.fixed8,
                &tables.overflow8,
                key,
                key_hash,
                cell,
                &mut occupied,
            ),
            1 => Self::insert_adaptive_atomic_primary(
                &tables.fixed16,
                &tables.overflow16,
                key,
                key_hash,
                cell,
                &mut occupied,
            ),
            2 => Self::insert_adaptive_atomic_primary(
                &tables.fixed24,
                &tables.overflow24,
                key,
                key_hash,
                cell,
                &mut occupied,
            ),
            3 => Self::insert_adaptive_atomic_primary(
                &tables.fixed32,
                &tables.overflow32,
                key,
                key_hash,
                cell,
                &mut occupied,
            ),
            4 => Self::insert_adaptive_atomic_primary(
                &tables.fixed48,
                &tables.overflow48,
                key,
                key_hash,
                cell,
                &mut occupied,
            ),
            _ => unreachable!("adaptive key class is bounded"),
        };
        match inserted {
            AdaptiveAtomicInsert::Inserted => OverlayInsert::Inserted,
            AdaptiveAtomicInsert::Occupied(result) => OverlayInsert::Occupied(result),
            AdaptiveAtomicInsert::Full(cell) => {
                adaptive.mark_fallback(key_hash);
                self.insert_adaptive_fallback(
                    adaptive,
                    key,
                    key_hash,
                    cell,
                    take_occupied(&mut occupied),
                )
            }
        }
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    fn insert_or_visit_adaptive_ready_encoded<R>(
        &self,
        ready: (&AdaptiveAtomicOverlay<C>, &AdaptiveAtomicTables<C>),
        key: &[u8],
        encoded: &AtomicEncodedAdmissionKey,
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let (adaptive, tables) = ready;
        let mut occupied = Some(occupied);
        if adaptive.sample_may_contain(key_hash)
            && let Some(result) = self.probe_adaptive_sample(adaptive, key, key_hash, &mut occupied)
        {
            return result;
        }

        let inserted = match encoded {
            AtomicEncodedAdmissionKey::UpTo8(encoded) => Self::insert_adaptive_encoded_primary(
                &tables.fixed8,
                &tables.overflow8,
                key,
                encoded,
                key_hash,
                cell,
                &mut occupied,
            ),
            AtomicEncodedAdmissionKey::UpTo16(encoded) => Self::insert_adaptive_encoded_primary(
                &tables.fixed16,
                &tables.overflow16,
                key,
                encoded,
                key_hash,
                cell,
                &mut occupied,
            ),
            AtomicEncodedAdmissionKey::UpTo24(encoded) => Self::insert_adaptive_encoded_primary(
                &tables.fixed24,
                &tables.overflow24,
                key,
                encoded,
                key_hash,
                cell,
                &mut occupied,
            ),
            AtomicEncodedAdmissionKey::UpTo32(encoded) => Self::insert_adaptive_encoded_primary(
                &tables.fixed32,
                &tables.overflow32,
                key,
                encoded,
                key_hash,
                cell,
                &mut occupied,
            ),
        };
        match inserted {
            AdaptiveAtomicInsert::Inserted => OverlayInsert::Inserted,
            AdaptiveAtomicInsert::Occupied(result) => OverlayInsert::Occupied(result),
            AdaptiveAtomicInsert::Full(cell) => {
                adaptive.mark_fallback(key_hash);
                self.insert_adaptive_fallback(
                    adaptive,
                    key,
                    key_hash,
                    cell,
                    take_occupied(&mut occupied),
                )
            }
        }
    }

    fn insert_or_visit_adaptive<R>(
        &self,
        adaptive: &AdaptiveAtomicOverlay<C>,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
        probe_fallback: bool,
    ) -> OverlayInsert<R> {
        let key_hash = key_hash.unwrap_or_else(|| adaptive.hash(key));
        if let Some(tables) = adaptive.tables() {
            return self.insert_or_visit_adaptive_ready(
                (adaptive, tables),
                key,
                key_hash,
                cell,
                occupied,
                probe_fallback,
            );
        }
        let Some(sample_guard) = adaptive.pin_sampling() else {
            let tables = adaptive.wait_for_tables();
            return self.insert_or_visit_adaptive_ready(
                (adaptive, tables),
                key,
                key_hash,
                cell,
                occupied,
                probe_fallback,
            );
        };

        adaptive.mark_sample(key_hash);
        let mut occupied = Some(occupied);
        let result = match adaptive
            .sample_fallback
            .insert_or_return_hashed(key, key_hash, cell)
        {
            AtomicDynamicInsert::Inserted => OverlayInsert::Inserted,
            AtomicDynamicInsert::Occupied(index) => {
                adaptive.sample_fallback.with_entry(index, |_, cell| {
                    OverlayInsert::Occupied(take_occupied(&mut occupied)(cell))
                })
            }
            AtomicDynamicInsert::Full(cell) => {
                adaptive.mark_fallback(key_hash);
                adaptive.mark_papaya(key_hash);
                if probe_fallback {
                    self.probe_or_insert_fallback(
                        key,
                        OverflowCell::Direct(cell),
                        take_occupied(&mut occupied),
                    )
                } else {
                    self.insert_fallback(
                        key,
                        OverflowCell::Direct(cell),
                        take_occupied(&mut occupied),
                    )
                }
            }
        };
        let inserted = matches!(&result, OverlayInsert::Inserted);
        let should_initialize = inserted && adaptive.observe_inserted(key.len());
        drop(sample_guard);
        if should_initialize {
            adaptive.try_initialize(&self.fallback);
        }
        result
    }

    fn insert_atomic_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        &self,
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let key_hash = key_hash.unwrap_or_else(|| primary.hash(key));
        match primary.insert_or_return_hashed(key, key_hash, cell) {
            AtomicFixedInsert::Inserted => OverlayInsert::Inserted,
            AtomicFixedInsert::Occupied(index) => {
                OverlayInsert::Occupied(occupied(primary.cell(index)))
            }
            AtomicFixedInsert::Full(cell) => {
                self.insert_atomic_overflow(overflow, key, key_hash, cell, occupied)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn insert_or_visit_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut occupied = Some(occupied);
        if let Some(adaptive) = &self.adaptive {
            let result = self.insert_or_visit_adaptive(
                adaptive,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
                false,
            );
            return adaptive.observe_physical_insertion(key, result);
        }
        if let Some(atomic_fixed8) = &self.atomic_fixed8
            && atomic_fixed8.supports(key)
        {
            let (primary, overflow) = self.atomic8_parts();
            return self.insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
        }
        if let Some(atomic_fixed16) = &self.atomic_fixed16
            && atomic_fixed16.supports(key)
        {
            let (primary, overflow) = self.atomic16_parts();
            return self.insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
        }
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && atomic_fixed32.supports(key)
        {
            let (primary, overflow) = self.atomic32_parts();
            return self.insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
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

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    pub(crate) fn probe_or_insert_encoded_prehashed<R>(
        &self,
        key: &[u8],
        encoded: &AtomicEncodedAdmissionKey,
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let Some(adaptive) = &self.adaptive else {
            return self.probe_or_insert_prehashed(key, key_hash, cell, occupied);
        };
        let Some(tables) = adaptive.tables() else {
            return self.probe_or_insert_prehashed(key, key_hash, cell, occupied);
        };
        let result = self.insert_or_visit_adaptive_ready_encoded(
            (adaptive, tables),
            key,
            encoded,
            key_hash,
            cell,
            occupied,
        );
        adaptive.observe_physical_insertion(key, result)
    }

    fn probe_or_insert_atomic_primary<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        &self,
        primary: &AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>,
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let key_hash = key_hash.unwrap_or_else(|| primary.hash(key));
        match primary.insert_or_return_hashed(key, key_hash, cell) {
            AtomicFixedInsert::Inserted => OverlayInsert::Inserted,
            AtomicFixedInsert::Occupied(index) => {
                OverlayInsert::Occupied(occupied(primary.cell(index)))
            }
            AtomicFixedInsert::Full(cell) => {
                self.probe_or_insert_atomic_overflow(overflow, key, key_hash, cell, occupied)
            }
        }
    }

    fn probe_or_insert_with_hash<R>(
        &self,
        key: &[u8],
        key_hash: Option<u64>,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut occupied = Some(occupied);
        if let Some(adaptive) = &self.adaptive {
            let result = self.insert_or_visit_adaptive(
                adaptive,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
                true,
            );
            return adaptive.observe_physical_insertion(key, result);
        }
        if let Some(atomic_fixed8) = &self.atomic_fixed8
            && atomic_fixed8.supports(key)
        {
            let (primary, overflow) = self.atomic8_parts();
            return self.probe_or_insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
        }
        if let Some(atomic_fixed16) = &self.atomic_fixed16
            && atomic_fixed16.supports(key)
        {
            let (primary, overflow) = self.atomic16_parts();
            return self.probe_or_insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
        }
        if let Some(atomic_fixed32) = &self.atomic_fixed32
            && atomic_fixed32.supports(key)
        {
            let (primary, overflow) = self.atomic32_parts();
            return self.probe_or_insert_atomic_primary(
                primary,
                overflow,
                key,
                key_hash,
                cell,
                take_occupied(&mut occupied),
            );
        }
        if let Some(fixed32) = &self.fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            let fixed32 = fixed32.pin();
            let fixed_key = InlineKey32::from_slice(key);
            if let Some(current) = fixed32.get(&fixed_key) {
                return OverlayInsert::Occupied(take_occupied(&mut occupied)(current));
            }
            return match fixed32.try_insert(fixed_key, cell) {
                Ok(_) => OverlayInsert::Inserted,
                Err(error) => OverlayInsert::Occupied(take_occupied(&mut occupied)(error.current)),
            };
        }
        if let Some(inline_variable) = &self.inline_variable
            && inline_variable.supports(key)
        {
            return inline_variable.probe_or_insert(key, cell, take_occupied(&mut occupied));
        }
        if let Some(compact) = &self.arc_swap_fixed32
            && key.len() == COMPACT_KEY_BYTES
        {
            if let Some(result) =
                compact.with_cell(key, |current| take_occupied(&mut occupied)(current))
            {
                return OverlayInsert::Occupied(result);
            }
            return match compact.insert_or_return(key, cell) {
                CompactInsert::Inserted => OverlayInsert::Inserted,
                CompactInsert::Occupied(entry) => {
                    OverlayInsert::Occupied(take_occupied(&mut occupied)(&entry.cell))
                }
                CompactInsert::Full(entry) => self.probe_or_insert_fallback(
                    key,
                    OverflowCell::Compact(entry),
                    take_occupied(&mut occupied),
                ),
            };
        }

        self.probe_or_insert_fallback(
            key,
            OverflowCell::Direct(cell),
            take_occupied(&mut occupied),
        )
    }

    pub(crate) fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        if let Some(adaptive) = &self.adaptive
            && let Some(tables) = adaptive.tables()
        {
            tables.fixed8.for_each(visit);
            Self::for_each_atomic_overflow(&tables.overflow8, visit);
            tables.fixed16.for_each(visit);
            Self::for_each_atomic_overflow(&tables.overflow16, visit);
            tables.fixed24.for_each(visit);
            Self::for_each_atomic_overflow(&tables.overflow24, visit);
            tables.fixed32.for_each(visit);
            Self::for_each_atomic_overflow(&tables.overflow32, visit);
            tables.fixed48.for_each(visit);
            Self::for_each_atomic_overflow(&tables.overflow48, visit);
        }
        if let Some(atomic_fixed8) = &self.atomic_fixed8 {
            atomic_fixed8.for_each(visit);
        }
        if let Some(overflow8) = &self.atomic_overflow8 {
            for segment in overflow8.segments() {
                let table = segment.load();
                if let Some(table) = table.as_ref() {
                    table.for_each(visit);
                }
            }
        }
        if let Some(atomic_fixed16) = &self.atomic_fixed16 {
            atomic_fixed16.for_each(visit);
        }
        if let Some(overflow16) = &self.atomic_overflow16 {
            for segment in overflow16.segments() {
                let table = segment.load();
                if let Some(table) = table.as_ref() {
                    table.for_each(visit);
                }
            }
        }
        if let Some(atomic_fixed32) = &self.atomic_fixed32 {
            atomic_fixed32.for_each(visit);
        }
        if let Some(overflow32) = &self.atomic_overflow32 {
            for segment in overflow32.segments() {
                let table = segment.load();
                if let Some(table) = table.as_ref() {
                    table.for_each(visit);
                }
            }
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
        if let Some(adaptive) = &self.adaptive {
            adaptive.sample_fallback.for_each(visit);
        }
        if let Some(fallback) = self
            .adaptive
            .as_ref()
            .and_then(AdaptiveAtomicOverlay::dynamic_fallback)
        {
            fallback.for_each(visit);
        }
        let fallback = self.fallback.pin();
        for (key, cell) in &fallback {
            visit(key, cell.cell());
        }
    }

    /// Samples one entry directly from an inline atomic table.
    ///
    /// This deliberately excludes the generic Papaya fallback: indexing an
    /// iterator there would turn a constant-time cache sample into a scan.
    /// Callers retain their aggregate scan as the uncommon coverage fallback.
    pub(crate) fn sample_atomic(
        &self,
        seed: u64,
        visit: &mut dyn FnMut(&[u8], &C) -> bool,
    ) -> bool {
        if let Some(adaptive) = &self.adaptive
            && let Some(tables) = adaptive.tables()
            && tables.sample(seed, visit)
        {
            return true;
        }

        let mut attempt = 0_u64;
        if let Some(table) = &self.atomic_fixed8 {
            if table.sample(mix_sample_seed(seed, attempt), visit) {
                return true;
            }
            attempt += 1;
        }
        if let Some(overflow) = &self.atomic_overflow8
            && Self::sample_atomic_overflow(overflow, seed, &mut attempt, visit)
        {
            return true;
        }
        if let Some(table) = &self.atomic_fixed16 {
            if table.sample(mix_sample_seed(seed, attempt), visit) {
                return true;
            }
            attempt += 1;
        }
        if let Some(overflow) = &self.atomic_overflow16
            && Self::sample_atomic_overflow(overflow, seed, &mut attempt, visit)
        {
            return true;
        }
        if let Some(table) = &self.atomic_fixed32
            && table.sample(mix_sample_seed(seed, attempt), visit)
        {
            return true;
        }
        false
    }

    /// Samples distinct live candidates from the generic fallback.
    ///
    /// The adaptive native tables start at reduced buckets and visit only
    /// enough physical buckets to fill the requested window. The bounded
    /// learning sample, dynamic residual, and emergency Papaya overflow receive
    /// population-weighted shares of every request.
    pub(crate) fn sample_fallback(
        &self,
        seed: u64,
        limit: usize,
        visit: &mut dyn FnMut(&[u8], &C) -> bool,
    ) -> usize {
        if limit == 0 {
            return 0;
        }
        let fallback = self.fallback.pin();
        let papaya_len = fallback.len();
        let adaptive = self.adaptive.as_ref();
        let sample = adaptive.map(|adaptive| &adaptive.sample_fallback);
        let sample_len = sample.map_or(0, AtomicDynamicFallback::len);
        let dynamic = adaptive.and_then(AdaptiveAtomicOverlay::dynamic_fallback);
        let dynamic_len = dynamic.map_or(0, AtomicDynamicFallback::len);
        let physical = papaya_len
            .saturating_add(sample_len)
            .saturating_add(dynamic_len);
        if physical == 0 {
            return 0;
        }
        let sample_limit = weighted_sample_limit(seed, sample_len, physical, limit);
        let mut sampled = sample.map_or(0, |sample| {
            sample.sample_entries(seed.rotate_left(7), sample_limit, visit)
        });
        let remaining = limit.saturating_sub(sampled);
        let remaining_physical = physical.saturating_sub(sample_len);
        let dynamic_limit = weighted_sample_limit(
            seed.rotate_left(17),
            dynamic_len,
            remaining_physical,
            remaining,
        );
        sampled += dynamic.map_or(0, |dynamic| {
            dynamic.sample_entries(seed.rotate_left(11), dynamic_limit, visit)
        });
        let papaya_limit = limit.saturating_sub(sampled);
        if papaya_limit == 0 || papaya_len == 0 {
            return sampled;
        }
        let start = reduce_sample(seed, papaya_len);
        for (key, cell) in (&fallback).into_iter().skip(start) {
            if visit(key, cell.cell()) {
                sampled += 1;
                if sampled == limit {
                    return sampled;
                }
            }
        }
        for (key, cell) in (&fallback).into_iter().take(start) {
            if visit(key, cell.cell()) {
                sampled += 1;
                if sampled == limit {
                    break;
                }
            }
        }
        sampled
    }

    fn sample_atomic_overflow<const KEY_BYTES: usize, const KEY_WORDS: usize>(
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        seed: u64,
        attempt: &mut u64,
        visit: &mut dyn FnMut(&[u8], &C) -> bool,
    ) -> bool {
        for segment in overflow.segments() {
            let table = segment.load();
            if let Some(table) = table.as_ref()
                && table.sample(mix_sample_seed(seed, *attempt), visit)
            {
                return true;
            }
            *attempt += 1;
        }
        false
    }

    fn for_each_atomic_overflow<const KEY_BYTES: usize, const KEY_WORDS: usize>(
        overflow: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        visit: &mut dyn FnMut(&[u8], &C),
    ) {
        for segment in overflow.segments() {
            let table = segment.load();
            if let Some(table) = table.as_ref() {
                table.for_each(visit);
            }
        }
    }

    fn insert_adaptive_fallback<R>(
        &self,
        adaptive: &AdaptiveAtomicOverlay<C>,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut occupied = Some(occupied);
        if adaptive.sample_may_contain(key_hash) {
            let sample = adaptive
                .sample_fallback
                .probe_cell_hashed(key, key_hash, |cell| take_occupied(&mut occupied)(cell));
            if let Some(result) = sample.found {
                return OverlayInsert::Occupied(result);
            }
            let fallback = self.fallback.pin();
            if let Some(current) = fallback.get(key) {
                return OverlayInsert::Occupied(take_occupied(&mut occupied)(current.cell()));
            }
        }

        let dynamic = adaptive
            .dynamic_fallback()
            .expect("adaptive tables publish their dynamic fallback first");
        match dynamic.insert_or_return_hashed(key, key_hash, cell) {
            AtomicDynamicInsert::Inserted => OverlayInsert::Inserted,
            AtomicDynamicInsert::Occupied(index) => dynamic.with_entry(index, |_, cell| {
                OverlayInsert::Occupied(take_occupied(&mut occupied)(cell))
            }),
            AtomicDynamicInsert::Full(cell) => {
                adaptive.mark_papaya(key_hash);
                self.insert_fallback(
                    key,
                    OverflowCell::Direct(cell),
                    take_occupied(&mut occupied),
                )
            }
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

    fn insert_atomic_overflow<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        &self,
        overflow32: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut cell = Some(cell);
        let mut occupied = Some(occupied);
        for (segment_index, segment) in overflow32.segments().iter().enumerate() {
            let table = segment.load();
            if let Some(table) = table.as_ref() {
                match table.insert_or_return_hashed(
                    key,
                    key_hash,
                    cell.take().expect("overflow cell remains available"),
                ) {
                    AtomicFixedInsert::Inserted => return OverlayInsert::Inserted,
                    AtomicFixedInsert::Occupied(index) => {
                        return OverlayInsert::Occupied(occupied
                            .take()
                            .expect("overlay occupied callback runs once")(
                            table.cell(index)
                        ));
                    }
                    AtomicFixedInsert::Full(returned) => cell = Some(returned),
                }
                continue;
            }
            drop(table);
            let table = overflow32.table(segment_index);
            match table.insert_or_return_hashed(
                key,
                key_hash,
                cell.take().expect("overflow cell remains available"),
            ) {
                AtomicFixedInsert::Inserted => return OverlayInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => {
                    return OverlayInsert::Occupied(occupied
                        .take()
                        .expect("overlay occupied callback runs once")(
                        table.cell(index)
                    ));
                }
                AtomicFixedInsert::Full(returned) => {
                    cell = Some(returned);
                }
            }
        }
        self.insert_fallback(
            key,
            OverflowCell::Direct(cell.expect("overflow cell remains available")),
            occupied
                .take()
                .expect("overlay occupied callback runs once"),
        )
    }

    fn probe_or_insert_atomic_overflow<R, const KEY_BYTES: usize, const KEY_WORDS: usize>(
        &self,
        overflow32: &AtomicOverflow<C, KEY_BYTES, KEY_WORDS>,
        key: &[u8],
        key_hash: u64,
        cell: C,
        occupied: impl FnOnce(&C) -> R,
    ) -> OverlayInsert<R> {
        let mut cell = Some(cell);
        let mut occupied = Some(occupied);
        for (segment_index, segment) in overflow32.segments().iter().enumerate() {
            let table = segment.load();
            if let Some(table) = table.as_ref() {
                match table.insert_or_return_hashed(
                    key,
                    key_hash,
                    cell.take().expect("overflow cell remains available"),
                ) {
                    AtomicFixedInsert::Inserted => return OverlayInsert::Inserted,
                    AtomicFixedInsert::Occupied(index) => {
                        return OverlayInsert::Occupied(occupied
                            .take()
                            .expect("overlay occupied callback runs once")(
                            table.cell(index)
                        ));
                    }
                    AtomicFixedInsert::Full(returned) => cell = Some(returned),
                }
                continue;
            }
            drop(table);
            let table = overflow32.table(segment_index);
            match table.insert_or_return_hashed(
                key,
                key_hash,
                cell.take().expect("overflow cell remains available"),
            ) {
                AtomicFixedInsert::Inserted => return OverlayInsert::Inserted,
                AtomicFixedInsert::Occupied(index) => {
                    return OverlayInsert::Occupied(occupied
                        .take()
                        .expect("overlay occupied callback runs once")(
                        table.cell(index)
                    ));
                }
                AtomicFixedInsert::Full(returned) => {
                    cell = Some(returned);
                }
            }
        }
        self.probe_or_insert_fallback(
            key,
            OverflowCell::Direct(cell.expect("overflow cell remains available")),
            occupied
                .take()
                .expect("overlay occupied callback runs once"),
        )
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
const ATOMIC_OVERFLOW_CAPACITY_DIVISOR: usize = 32;
const ATOMIC_OVERFLOW_FIRST_SEGMENT_NUMERATOR: usize = 4;
const ATOMIC_OVERFLOW_FIRST_SEGMENT_DENOMINATOR: usize = 5;
const ADAPTIVE_KEY_CLASSES: usize = 5;
const ADAPTIVE_DISTRIBUTION_CLASSES: usize = ADAPTIVE_KEY_CLASSES + 1;
const ADAPTIVE_SAMPLE_MIN: usize = 64;
const ADAPTIVE_SAMPLE_MAX: usize = 4_096;
const ADAPTIVE_INSERTION_SHARDS: usize = 64;
const ADAPTIVE_GATE_CLOSED: usize = 1 << (usize::BITS - 1);
const ADAPTIVE_GATE_WRITERS: usize = ADAPTIVE_GATE_CLOSED - 1;

fn adaptive_sample_target(capacity: usize) -> usize {
    let scaled_target = capacity.div_ceil(64).max(ADAPTIVE_SAMPLE_MIN);
    capacity.min(ADAPTIVE_SAMPLE_MAX).min(scaled_target).max(1)
}

#[derive(Clone, Copy)]
enum AtomicKeyMode {
    Exact,
    UpTo,
}

/// One-time learned allocation across bounded inline key classes.
///
/// New maps initially retain a small exact sample in a native append-only
/// stable-cell table. Once the sample target is reached, one writer builds
/// proportional fixed tables while other writers continue using that sample.
/// Publication closes and drains the sampling path before new writers can use
/// the tables, so the same key can never be published in both destinations
/// concurrently. The sample remains readable and directly preparable after
/// publication; only emergency saturation spills to Papaya.
struct AdaptiveAtomicOverlay<C> {
    capacity: usize,
    sample_target: usize,
    hash_builder: GenerationHashBuilder,
    sample_fallback: AtomicDynamicFallback<C>,
    tables: OnceLock<AdaptiveAtomicTables<C>>,
    dynamic_fallback: OnceLock<AtomicDynamicFallback<C>>,
    sample_gate: AtomicUsize,
    building: AtomicBool,
    sampled: AtomicUsize,
    physical_insertions: OnceLock<Box<[AdaptiveInsertionCount]>>,
    physical_baseline: OnceLock<usize>,
    class_counts: [AtomicUsize; ADAPTIVE_KEY_CLASSES],
    sample_filter: Box<[AtomicU64]>,
    fallback_filter: Box<[AtomicU64]>,
}

#[repr(align(64))]
struct AdaptiveInsertionCount(AtomicUsize);

impl AdaptiveInsertionCount {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }
}

struct AdaptiveAtomicTables<C> {
    capacities: [usize; ADAPTIVE_KEY_CLASSES],
    fixed8: AtomicFixed8Overlay<C>,
    overflow8: AtomicOverflow8<C>,
    fixed16: AtomicFixed16Overlay<C>,
    overflow16: AtomicOverflow16<C>,
    fixed24: AtomicFixed24Overlay<C>,
    overflow24: AtomicOverflow24<C>,
    fixed32: AtomicFixed32Overlay<C>,
    overflow32: AtomicOverflow32<C>,
    fixed48: AtomicFixed48Overlay<C>,
    overflow48: AtomicOverflow48<C>,
}

enum AdaptiveAtomicInsert<C, R> {
    Inserted,
    Occupied(R),
    Full(C),
}

struct AdaptiveSamplingGuard<'a, C> {
    overlay: &'a AdaptiveAtomicOverlay<C>,
}

impl<C: StableCell> AdaptiveAtomicOverlay<C> {
    fn new(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        let sample_target = adaptive_sample_target(capacity);
        // Only sampled/fallback keys set this filter. Bounding it by the
        // sample target keeps a fixed 64 filter bits per sampled key once the
        // 4,096-key learning cap applies, instead of scaling a mostly empty
        // filter with the full configured overlay capacity.
        let fallback_filter_words = capacity.div_ceil(64).min(sample_target);
        Self {
            capacity,
            sample_target,
            hash_builder,
            sample_fallback: AtomicDynamicFallback::with_capacity(sample_target),
            tables: OnceLock::new(),
            dynamic_fallback: OnceLock::new(),
            sample_gate: AtomicUsize::new(0),
            building: AtomicBool::new(false),
            sampled: AtomicUsize::new(0),
            physical_insertions: OnceLock::new(),
            physical_baseline: OnceLock::new(),
            class_counts: std::array::from_fn(|_| AtomicUsize::new(0)),
            sample_filter: (0..fallback_filter_words)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            fallback_filter: (0..fallback_filter_words)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        GenerationKeyHash::new(&self.hash_builder, key).route()
    }

    fn tables(&self) -> Option<&AdaptiveAtomicTables<C>> {
        self.tables.get()
    }

    fn dynamic_fallback(&self) -> Option<&AtomicDynamicFallback<C>> {
        self.dynamic_fallback.get()
    }

    #[cfg(feature = "prepared-keys")]
    #[inline]
    fn owner_id(&self) -> u64 {
        u64::try_from(std::ptr::from_ref(self) as usize).expect("pointer address fits u64")
    }

    fn snapshot(
        &self,
        physical_key_classes: [usize; ADAPTIVE_DISTRIBUTION_CLASSES],
        fallback_short_keys: usize,
    ) -> AdaptiveOverlaySnapshot {
        let sampled = self.sampled.load(Ordering::Acquire);
        let short_sample: [usize; ADAPTIVE_KEY_CLASSES] =
            std::array::from_fn(|class| self.class_counts[class].load(Ordering::Relaxed));
        let short_sample_total = short_sample.iter().copied().sum::<usize>();
        let mut sample_key_classes = [0; ADAPTIVE_DISTRIBUTION_CLASSES];
        sample_key_classes[..ADAPTIVE_KEY_CLASSES].copy_from_slice(&short_sample);
        sample_key_classes[ADAPTIVE_KEY_CLASSES] = sampled.saturating_sub(short_sample_total);
        let learned_key_classes = std::array::from_fn(|class| {
            physical_key_classes[class].saturating_sub(sample_key_classes[class])
        });
        let learned_insertions = learned_key_classes.iter().copied().sum();
        let sampled_short_keys = sample_key_classes[..ADAPTIVE_KEY_CLASSES]
            .iter()
            .copied()
            .sum::<usize>();
        let short_key_fallback_insertions = fallback_short_keys.saturating_sub(sampled_short_keys);
        let tables = self.tables();
        AdaptiveOverlaySnapshot {
            capacity: self.capacity,
            sample_target: self.sample_target,
            sampled,
            sample_key_classes,
            learned_insertions,
            learned_key_classes,
            planned_atomic_capacities: tables
                .map_or([0; ADAPTIVE_KEY_CLASSES], |tables| tables.capacities),
            short_key_fallback_insertions,
            ready: tables.is_some(),
        }
    }

    fn try_pin_sampling(&self) -> bool {
        let mut current = self.sample_gate.load(Ordering::Acquire);
        loop {
            if current & ADAPTIVE_GATE_CLOSED != 0 {
                return false;
            }
            assert!(
                current & ADAPTIVE_GATE_WRITERS < ADAPTIVE_GATE_WRITERS,
                "adaptive sampling writer count overflow"
            );
            match self.sample_gate.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn pin_sampling(&self) -> Option<AdaptiveSamplingGuard<'_, C>> {
        self.try_pin_sampling()
            .then(|| AdaptiveSamplingGuard { overlay: self })
    }

    fn observe_inserted(&self, key_bytes: usize) -> bool {
        if let Some(class) = adaptive_key_class(key_bytes) {
            self.class_counts[class].fetch_add(1, Ordering::Relaxed);
        }
        let sampled = self.sampled.fetch_add(1, Ordering::Release) + 1;
        sampled >= self.sample_target
    }

    fn observe_physical_insertion<R>(
        &self,
        key: &[u8],
        result: OverlayInsert<R>,
    ) -> OverlayInsert<R> {
        if matches!(&result, OverlayInsert::Inserted)
            && let Some(counters) = self.physical_insertions.get()
        {
            let first = key.first().copied().unwrap_or(0) as usize;
            let last = key.last().copied().unwrap_or(0) as usize;
            let shard = (key.len() ^ first ^ last.rotate_left(3)) & (ADAPTIVE_INSERTION_SHARDS - 1);
            counters[shard].0.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn capacity_pressure(
        &self,
        maximum_bps: u16,
        fallback: &HashMap<Box<[u8]>, OverflowCell<C>, DefaultHashBuilder>,
    ) -> bool {
        // Only the wake-driven async-maintenance path asks for this cheap
        // certificate. Keep ordinary foreground insertions free of a counter
        // RMW until that path is actually active. Publishing the counters
        // before taking the one-time baseline makes racing insertions visible;
        // a record observed by both is a safe overcount that can only request
        // maintenance early.
        let counters = self.physical_insertions.get_or_init(|| {
            (0..ADAPTIVE_INSERTION_SHARDS)
                .map(|_| AdaptiveInsertionCount::new())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
        let baseline = *self
            .physical_baseline
            .get_or_init(|| self.physical_len(fallback));
        let occupied = baseline.saturating_add(
            counters
                .iter()
                .map(|counter| counter.0.load(Ordering::Relaxed))
                .sum::<usize>(),
        );
        (occupied as u128).saturating_mul(10_000)
            >= (self.capacity as u128).saturating_mul(u128::from(maximum_bps))
    }

    fn physical_len(
        &self,
        fallback: &HashMap<Box<[u8]>, OverflowCell<C>, DefaultHashBuilder>,
    ) -> usize {
        fallback
            .len()
            .saturating_add(self.sample_fallback.len())
            .saturating_add(
                self.dynamic_fallback()
                    .map_or(0, AtomicDynamicFallback::len),
            )
            .saturating_add(self.tables().map_or(0, AdaptiveAtomicTables::physical_len))
    }

    fn try_initialize(
        &self,
        _fallback: &HashMap<Box<[u8]>, OverflowCell<C>, DefaultHashBuilder>,
    ) -> bool {
        if self.tables.get().is_some()
            || self.sampled.load(Ordering::Acquire) < self.sample_target
            || self.building.swap(true, Ordering::AcqRel)
        {
            return false;
        }

        let previous = self
            .sample_gate
            .fetch_or(ADAPTIVE_GATE_CLOSED, Ordering::AcqRel);
        debug_assert_eq!(previous & ADAPTIVE_GATE_CLOSED, 0);
        while self.sample_gate.load(Ordering::Acquire) & ADAPTIVE_GATE_WRITERS != 0 {
            spin_loop();
        }
        let sampled = self.sampled.load(Ordering::Acquire);
        let counts = std::array::from_fn(|class| self.class_counts[class].load(Ordering::Relaxed));
        let capacities = adaptive_class_capacities(self.capacity, sampled, counts);
        let atomic_capacity = capacities.iter().copied().sum::<usize>();
        let dynamic_fallback_capacity = self
            .capacity
            .saturating_sub(sampled)
            .saturating_sub(atomic_capacity);
        self.dynamic_fallback
            .set(AtomicDynamicFallback::with_capacity(
                dynamic_fallback_capacity,
            ))
            .unwrap_or_else(|_| unreachable!("one adaptive builder publishes the fallback"));
        let tables = AdaptiveAtomicTables::new(capacities, self.hash_builder.clone());
        self.tables
            .set(tables)
            .unwrap_or_else(|_| unreachable!("one adaptive builder publishes tables"));
        true
    }

    fn wait_for_tables(&self) -> &AdaptiveAtomicTables<C> {
        loop {
            if let Some(tables) = self.tables.get() {
                return tables;
            }
            spin_loop();
        }
    }

    fn mark_fallback(&self, hash: u64) {
        let Some((word, mask)) = adaptive_filter_location(hash, self.fallback_filter.len()) else {
            return;
        };
        // The filter is a monotonic routing hint, not the publication edge for
        // the fallback entry. The fallback map provides its own synchronization.
        self.fallback_filter[word].fetch_or(mask, Ordering::Relaxed);
    }

    fn mark_sample(&self, hash: u64) {
        let Some((word, mask)) = adaptive_filter_location(hash, self.sample_filter.len()) else {
            return;
        };
        self.sample_filter[word].fetch_or(mask, Ordering::Relaxed);
    }

    fn sample_may_contain(&self, hash: u64) -> bool {
        let Some((word, mask)) = adaptive_filter_location(hash, self.sample_filter.len()) else {
            return true;
        };
        self.sample_filter[word].load(Ordering::Relaxed) & mask == mask
    }

    fn fallback_may_contain(&self, hash: u64) -> bool {
        let Some((word, mask)) = adaptive_filter_location(hash, self.fallback_filter.len()) else {
            return true;
        };
        self.fallback_filter[word].load(Ordering::Relaxed) & mask == mask
    }

    fn mark_papaya(&self, hash: u64) {
        self.mark_sample(hash);
    }

    #[cfg(test)]
    fn papaya_may_contain(&self, hash: u64) -> bool {
        self.sample_may_contain(hash)
    }
}

impl<C> Drop for AdaptiveSamplingGuard<'_, C> {
    fn drop(&mut self) {
        let previous = self.overlay.sample_gate.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & ADAPTIVE_GATE_WRITERS > 0);
    }
}

impl<C: StableCell> AdaptiveAtomicTables<C> {
    fn new(capacities: [usize; ADAPTIVE_KEY_CLASSES], hash: GenerationHashBuilder) -> Self {
        Self {
            capacities,
            fixed8: AtomicFixed8Overlay::with_capacity_mode(
                capacities[0],
                hash.clone(),
                AtomicKeyMode::UpTo,
            ),
            overflow8: AtomicOverflow8::new(capacities[0], hash.clone(), AtomicKeyMode::UpTo),
            fixed16: AtomicFixed16Overlay::with_capacity_mode(
                capacities[1],
                hash.clone(),
                AtomicKeyMode::UpTo,
            ),
            overflow16: AtomicOverflow16::new(capacities[1], hash.clone(), AtomicKeyMode::UpTo),
            fixed24: AtomicFixed24Overlay::with_capacity_mode(
                capacities[2],
                hash.clone(),
                AtomicKeyMode::UpTo,
            ),
            overflow24: AtomicOverflow24::new(capacities[2], hash.clone(), AtomicKeyMode::UpTo),
            fixed32: AtomicFixed32Overlay::with_capacity_mode(
                capacities[3],
                hash.clone(),
                AtomicKeyMode::UpTo,
            ),
            fixed48: AtomicFixed48Overlay::with_capacity_mode(
                capacities[4],
                hash.clone(),
                AtomicKeyMode::Exact,
            ),
            overflow48: AtomicOverflow48::new(capacities[4], hash.clone(), AtomicKeyMode::Exact),
            overflow32: AtomicOverflow32::new(capacities[3], hash, AtomicKeyMode::UpTo),
        }
    }

    fn physical_len(&self) -> usize {
        self.fixed8
            .physical_len()
            .saturating_add(self.overflow8.physical_len())
            .saturating_add(self.fixed16.physical_len())
            .saturating_add(self.overflow16.physical_len())
            .saturating_add(self.fixed24.physical_len())
            .saturating_add(self.overflow24.physical_len())
            .saturating_add(self.fixed32.physical_len())
            .saturating_add(self.overflow32.physical_len())
            .saturating_add(self.fixed48.physical_len())
            .saturating_add(self.overflow48.physical_len())
    }

    fn sampling_class(&self, seed: u64, step: usize) -> Option<usize> {
        debug_assert!(step < ADAPTIVE_KEY_CLASSES);
        let start = usize::try_from(seed % ADAPTIVE_KEY_CLASSES as u64)
            .expect("adaptive sample class fits usize");
        let class = (start + step) % ADAPTIVE_KEY_CLASSES;
        (self.capacities[class] != 0).then_some(class)
    }

    fn sample(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &C) -> bool) -> bool {
        for step in 0..ADAPTIVE_KEY_CLASSES {
            let Some(class) = self.sampling_class(seed, step) else {
                continue;
            };
            let class_seed = mix_sample_seed(seed, step as u64);
            let sampled = match class {
                0 => self.fixed8.sample(class_seed, visit),
                1 => self.fixed16.sample(class_seed, visit),
                2 => self.fixed24.sample(class_seed, visit),
                3 => self.fixed32.sample(class_seed, visit),
                4 => self.fixed48.sample(class_seed, visit),
                _ => unreachable!("adaptive class is bounded"),
            };
            if sampled {
                return true;
            }
        }

        let mut attempt = ADAPTIVE_KEY_CLASSES as u64;
        for step in 0..ADAPTIVE_KEY_CLASSES {
            let Some(class) = self.sampling_class(seed, step) else {
                continue;
            };
            let sampled = match class {
                0 => GenerationOverlay::sample_atomic_overflow(
                    &self.overflow8,
                    seed,
                    &mut attempt,
                    visit,
                ),
                1 => GenerationOverlay::sample_atomic_overflow(
                    &self.overflow16,
                    seed,
                    &mut attempt,
                    visit,
                ),
                2 => GenerationOverlay::sample_atomic_overflow(
                    &self.overflow24,
                    seed,
                    &mut attempt,
                    visit,
                ),
                3 => GenerationOverlay::sample_atomic_overflow(
                    &self.overflow32,
                    seed,
                    &mut attempt,
                    visit,
                ),
                4 => GenerationOverlay::sample_atomic_overflow(
                    &self.overflow48,
                    seed,
                    &mut attempt,
                    visit,
                ),
                _ => unreachable!("adaptive class is bounded"),
            };
            if sampled {
                return true;
            }
        }
        false
    }
}

fn adaptive_key_class(key_bytes: usize) -> Option<usize> {
    match key_bytes {
        0..=8 => Some(0),
        9..=16 => Some(1),
        17..=24 => Some(2),
        25..=32 => Some(3),
        48 => Some(4),
        _ => None,
    }
}

fn adaptive_class_capacities(
    capacity: usize,
    sampled: usize,
    counts: [usize; ADAPTIVE_KEY_CLASSES],
) -> [usize; ADAPTIVE_KEY_CLASSES] {
    let mut capacities = [0; ADAPTIVE_KEY_CLASSES];
    let short_keys = counts.iter().copied().sum::<usize>();
    if sampled == 0 || short_keys == 0 {
        return capacities;
    }
    let remaining = capacity.saturating_sub(sampled);
    let atomic_budget = scale_usize(remaining, short_keys, sampled);
    let mut assigned = 0;
    for (class, count) in counts.iter().copied().enumerate() {
        capacities[class] = scale_usize(atomic_budget, count, short_keys);
        assigned += capacities[class];
    }
    let largest = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| *count)
        .map_or(0, |(class, _)| class);
    capacities[largest] += atomic_budget.saturating_sub(assigned);
    capacities
}

fn scale_usize(value: usize, numerator: usize, denominator: usize) -> usize {
    if denominator == 0 {
        return 0;
    }
    usize::try_from(
        (value as u128)
            .saturating_mul(numerator as u128)
            .checked_div(denominator as u128)
            .unwrap_or(0),
    )
    .unwrap_or(usize::MAX)
}

fn adaptive_filter_location(hash: u64, words: usize) -> Option<(usize, u64)> {
    if words == 0 {
        return None;
    }
    let word =
        usize::try_from((u128::from(hash) * u128::try_from(words).expect("usize fits u128")) >> 64)
            .expect("reduced adaptive filter hash is below word count");
    let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
    let mut second = (hash >> 32) as u32 & 63;
    if second == first {
        second = (second + 1) & 63;
    }
    Some((word, (1_u64 << first) | (1_u64 << second)))
}

/// Append-only arbitrary-length table for the adaptive generation fallback.
///
/// Deletion changes the stable cell instead of removing the physical key, so
/// every published entry remains valid until the containing generation is
/// dropped. Atomic bucket controls can therefore publish stable entry pointers
/// without an epoch collector, while direct bucket access makes bounded victim
/// sampling independent of the table population.
struct AtomicDynamicFallback<C> {
    controls: Box<[AtomicU64]>,
    slots: Box<[DynamicEntrySlot<C>]>,
    bucket_count: usize,
    len_shards: Box<[AtomicDynamicLen]>,
}

#[repr(align(64))]
struct AtomicDynamicLen(AtomicUsize);

enum AtomicDynamicInsert<C> {
    Inserted,
    Occupied(usize),
    Full(C),
}

impl<C: StableCell> AtomicDynamicFallback<C> {
    fn with_capacity(capacity: usize) -> Self {
        let target_slots = capacity
            .saturating_mul(ATOMIC_TARGET_NUMERATOR)
            .div_ceil(ATOMIC_TARGET_DENOMINATOR);
        let bucket_count = target_slots.div_ceil(ATOMIC_BUCKET_SLOTS);
        let slots = bucket_count.saturating_mul(ATOMIC_BUCKET_SLOTS);
        let len_shards = if capacity == 0 {
            0
        } else {
            capacity
                .div_ceil(TARGET_ENTRIES_PER_SHARD)
                .clamp(1, 64)
                .checked_next_power_of_two()
                .unwrap_or(64)
        };
        Self {
            controls: (0..bucket_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            slots: (0..slots)
                .map(|_| DynamicEntrySlot::empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_count,
            len_shards: (0..len_shards)
                .map(|_| AtomicDynamicLen(AtomicUsize::new(0)))
                .collect(),
        }
    }

    fn len(&self) -> usize {
        self.len_shards
            .iter()
            .map(|counter| counter.0.load(Ordering::Relaxed))
            .sum()
    }

    fn probe_cell_hashed<R>(
        &self,
        key: &[u8],
        hash: u64,
        read: impl FnOnce(&C) -> R,
    ) -> AtomicCellProbe<R> {
        if self.controls.is_empty() {
            return AtomicCellProbe::miss(false);
        }
        let tag = atomic_slot_tag(hash);
        let primary = atomic_primary_bucket(hash, self.bucket_count);
        let primary_scan = self.scan_bucket_read(primary, tag, key);
        if let Some(index) = primary_scan.found {
            return AtomicCellProbe::found(self.with_entry(index, |_, cell| read(cell)));
        }
        if primary_scan.empty.is_some() || self.bucket_count == 1 {
            return AtomicCellProbe::miss(primary_scan.empty.is_some());
        }
        let secondary = atomic_secondary_bucket(hash, self.bucket_count, primary);
        let secondary_scan = self.scan_bucket_read(secondary, tag, key);
        if let Some(index) = secondary_scan.found {
            return AtomicCellProbe::found(self.with_entry(index, |_, cell| read(cell)));
        }
        if secondary_scan.empty.is_some() || self.bucket_count == 2 {
            return AtomicCellProbe::miss(secondary_scan.empty.is_some());
        }
        let tertiary = atomic_tertiary_bucket(hash, self.bucket_count, primary, secondary);
        let tertiary_scan = self.scan_bucket_read(tertiary, tag, key);
        if let Some(index) = tertiary_scan.found {
            return AtomicCellProbe::found(self.with_entry(index, |_, cell| read(cell)));
        }
        if tertiary_scan.empty.is_some() {
            return AtomicCellProbe::miss(true);
        }
        let Some(fourth) =
            atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
        else {
            return AtomicCellProbe::miss(false);
        };
        let fourth_scan = self.scan_bucket_read(fourth, tag, key);
        if let Some(index) = fourth_scan.found {
            return AtomicCellProbe::found(self.with_entry(index, |_, cell| read(cell)));
        }
        AtomicCellProbe::miss(fourth_scan.empty.is_some())
    }

    #[cfg(feature = "prepared-keys")]
    fn prepared_index_hashed(&self, key: &[u8], hash: u64) -> Option<usize> {
        if self.controls.is_empty() {
            return None;
        }
        let tag = atomic_slot_tag(hash);
        let primary = atomic_primary_bucket(hash, self.bucket_count);
        let primary_scan = self.scan_bucket_read(primary, tag, key);
        if primary_scan.found.is_some() || primary_scan.empty.is_some() || self.bucket_count == 1 {
            return primary_scan.found;
        }
        let secondary = atomic_secondary_bucket(hash, self.bucket_count, primary);
        let secondary_scan = self.scan_bucket_read(secondary, tag, key);
        if secondary_scan.found.is_some()
            || secondary_scan.empty.is_some()
            || self.bucket_count == 2
        {
            return secondary_scan.found;
        }
        let tertiary = atomic_tertiary_bucket(hash, self.bucket_count, primary, secondary);
        let tertiary_scan = self.scan_bucket_read(tertiary, tag, key);
        if tertiary_scan.found.is_some() || tertiary_scan.empty.is_some() {
            return tertiary_scan.found;
        }
        let fourth = atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)?;
        self.scan_bucket_read(fourth, tag, key).found
    }

    #[cfg(feature = "prepared-keys")]
    #[inline]
    fn with_prepared_cell<R>(
        &self,
        index: usize,
        key: &[u8],
        read: impl FnOnce(&C) -> R,
    ) -> Option<R> {
        let entry = self.slots.get(index)?.get()?;
        (entry.key() == key).then(|| read(entry.cell()))
    }

    fn insert_or_return_hashed(&self, key: &[u8], hash: u64, cell: C) -> AtomicDynamicInsert<C> {
        if self.controls.is_empty() {
            return AtomicDynamicInsert::Full(cell);
        }
        let tag = atomic_slot_tag(hash);
        let mut cell = Some(cell);
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        let mut observed_control = 0;
        loop {
            let primary_scan = self.scan_bucket_write(primary, tag, key, &mut observed_control);
            if let Some(index) = primary_scan.found {
                return AtomicDynamicInsert::Occupied(index);
            }
            if let Some(index) = primary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                    return AtomicDynamicInsert::Inserted;
                }
                continue;
            }
            let secondary_scan = if secondary == primary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(secondary, tag, key, &mut observed_control)
            };
            if let Some(index) = secondary_scan.found {
                return AtomicDynamicInsert::Occupied(index);
            }
            if let Some(index) = secondary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                    return AtomicDynamicInsert::Inserted;
                }
                continue;
            }
            let tertiary_scan = if tertiary == primary || tertiary == secondary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(tertiary, tag, key, &mut observed_control)
            };
            if let Some(index) = tertiary_scan.found {
                return AtomicDynamicInsert::Occupied(index);
            }
            let Some(index) = tertiary_scan.empty else {
                if let Some(fourth) =
                    atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
                {
                    let fourth_scan =
                        self.scan_bucket_write(fourth, tag, key, &mut observed_control);
                    if let Some(index) = fourth_scan.found {
                        return AtomicDynamicInsert::Occupied(index);
                    }
                    if let Some(index) = fourth_scan.empty {
                        if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                            return AtomicDynamicInsert::Inserted;
                        }
                        continue;
                    }
                }
                return AtomicDynamicInsert::Full(
                    cell.take()
                        .expect("full dynamic buckets retain candidate cell"),
                );
            };
            if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                return AtomicDynamicInsert::Inserted;
            }
        }
    }

    fn with_entry<R>(&self, index: usize, read: impl FnOnce(&[u8], &C) -> R) -> R {
        let entry = self.slots[index]
            .get()
            .expect("published dynamic control owns an entry");
        read(entry.key(), entry.cell())
    }

    fn scan_bucket_read(&self, bucket: usize, tag: u8, key: &[u8]) -> BucketScan {
        let begin = bucket * ATOMIC_BUCKET_SLOTS;
        let control = self.controls[bucket].load(Ordering::Acquire);
        let (empty, writing) = atomic_bucket_frontier(control);
        let stop = writing.or(empty).unwrap_or(ATOMIC_BUCKET_SLOTS);
        let mut candidates = matching_control_bytes(control, tag);
        while let Some(offset) = take_control_offset(&mut candidates) {
            if offset >= stop {
                break;
            }
            let index = begin + offset;
            if self.with_entry(index, |entry_key, _| entry_key == key) {
                return BucketScan {
                    found: Some(index),
                    empty: None,
                };
            }
        }
        BucketScan {
            found: None,
            empty: (stop < ATOMIC_BUCKET_SLOTS).then_some(begin + stop),
        }
    }

    fn scan_bucket_write(
        &self,
        bucket: usize,
        tag: u8,
        key: &[u8],
        observed_control: &mut u64,
    ) -> BucketScan {
        let begin = bucket * ATOMIC_BUCKET_SLOTS;
        loop {
            let control = self.controls[bucket].load(Ordering::Acquire);
            *observed_control = control;
            if control == 0 {
                return BucketScan {
                    found: None,
                    empty: Some(begin),
                };
            }
            let (empty, writing) = atomic_bucket_frontier(control);
            let stop = writing.or(empty).unwrap_or(ATOMIC_BUCKET_SLOTS);
            let mut candidates = matching_control_bytes(control, tag);
            while let Some(offset) = take_control_offset(&mut candidates) {
                if offset >= stop {
                    break;
                }
                let index = begin + offset;
                if self.with_entry(index, |entry_key, _| entry_key == key) {
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

    fn claim_and_publish(
        &self,
        index: usize,
        control: u64,
        tag: u8,
        key: &[u8],
        cell: &mut Option<C>,
    ) -> bool {
        let bucket = index / ATOMIC_BUCKET_SLOTS;
        let offset = index % ATOMIC_BUCKET_SLOTS;
        let shift = offset * 8;
        let writing = u64::from(ATOMIC_SLOT_WRITING) << shift;
        debug_assert_eq!(control_byte(control, offset), ATOMIC_SLOT_EMPTY);
        if self.controls[bucket]
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
        if self.slots[index]
            .set(
                key,
                cell.take()
                    .expect("claimed dynamic slot owns candidate cell"),
            )
            .is_err()
        {
            unreachable!("one bucket claimant initializes each dynamic slot");
        }
        self.len_shards[bucket & (self.len_shards.len() - 1)]
            .0
            .fetch_add(1, Ordering::Relaxed);
        let byte_mask = u64::from(u8::MAX) << shift;
        let published = (control & !byte_mask) | (u64::from(tag) << shift);
        self.controls[bucket].store(published, Ordering::Release);
        true
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        for bucket in 0..self.bucket_count {
            let occupied = self.occupied_in_bucket(bucket);
            let begin = bucket * ATOMIC_BUCKET_SLOTS;
            for offset in 0..occupied {
                self.with_entry(begin + offset, |key, cell| visit(key, cell));
            }
        }
    }

    fn sample_entries(
        &self,
        seed: u64,
        limit: usize,
        visit: &mut dyn FnMut(&[u8], &C) -> bool,
    ) -> usize {
        if limit == 0 || self.bucket_count == 0 {
            return 0;
        }
        let start = reduce_sample(seed, self.bucket_count);
        let mut sampled = 0;
        for step in 0..self.bucket_count {
            let bucket = (start + step) % self.bucket_count;
            let occupied = self.occupied_in_bucket(bucket);
            if occupied == 0 {
                continue;
            }
            let first = reduce_sample(mix_sample_seed(seed, step as u64), occupied);
            let begin = bucket * ATOMIC_BUCKET_SLOTS;
            for candidate in 0..occupied {
                let offset = (first + candidate) % occupied;
                if self.with_entry(begin + offset, &mut *visit) {
                    sampled += 1;
                    if sampled == limit {
                        return sampled;
                    }
                }
            }
        }
        sampled
    }

    fn occupied_in_bucket(&self, bucket: usize) -> usize {
        loop {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let (empty, writing) = atomic_bucket_frontier(control);
            if writing.is_none() {
                return empty.unwrap_or(ATOMIC_BUCKET_SLOTS);
            }
            spin_loop();
        }
    }
}

/// Lazily allocated atomic segments for packed 32-byte key representations
/// that exhaust all primary bucket choices.
struct AtomicOverflow<C, const KEY_BYTES: usize, const KEY_WORDS: usize> {
    tables: [ArcSwapOption<AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>>; 2],
    capacities: [usize; 2],
    hash_builder: GenerationHashBuilder,
    key_mode: AtomicKeyMode,
}

type AtomicOverflow8<C> = AtomicOverflow<C, 8, 1>;
type AtomicOverflow16<C> = AtomicOverflow<C, 16, 2>;
type AtomicOverflow24<C> = AtomicOverflow<C, 24, 3>;
type AtomicOverflow32<C> = AtomicOverflow<C, 32, 4>;
type AtomicOverflow48<C> = AtomicOverflow<C, 48, 6>;

impl<C: StableCell, const KEY_BYTES: usize, const KEY_WORDS: usize>
    AtomicOverflow<C, KEY_BYTES, KEY_WORDS>
{
    fn new(
        primary_capacity: usize,
        hash_builder: GenerationHashBuilder,
        key_mode: AtomicKeyMode,
    ) -> Self {
        let capacity = primary_capacity
            .div_ceil(ATOMIC_OVERFLOW_CAPACITY_DIVISOR)
            .max(32);
        let first_capacity = capacity
            .saturating_mul(ATOMIC_OVERFLOW_FIRST_SEGMENT_NUMERATOR)
            .div_ceil(ATOMIC_OVERFLOW_FIRST_SEGMENT_DENOMINATOR);
        debug_assert!(first_capacity < capacity);
        Self {
            tables: std::array::from_fn(|_| ArcSwapOption::empty()),
            capacities: [first_capacity, capacity - first_capacity],
            hash_builder,
            key_mode,
        }
    }

    fn segments(&self) -> &[ArcSwapOption<AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>>] {
        &self.tables
    }

    fn physical_len(&self) -> usize {
        self.tables.iter().fold(0, |total, segment| {
            total.saturating_add(
                segment
                    .load_full()
                    .as_deref()
                    .map_or(0, AtomicFixedOverlay::physical_len),
            )
        })
    }

    fn table(&self, segment: usize) -> Arc<AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>> {
        if let Some(table) = self.tables[segment].load_full() {
            return table;
        }
        let candidate = Arc::new(AtomicFixedOverlay::with_capacity_mode(
            self.capacities[segment],
            self.hash_builder.clone(),
            self.key_mode,
        ));
        let previous = self.tables[segment].compare_and_swap(
            &None::<Arc<AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>>>,
            Some(Arc::clone(&candidate)),
        );
        previous.as_ref().map_or(candidate, Arc::clone)
    }
}

/// Fixed-capacity bucket overlay for 32-byte physical key representations.
///
/// Every slot is fully initialized, so publication needs no unsafe code. A
/// writer claims one byte in an atomic bucket control word, stores four key
/// words and initializes the stable cell, then publishes a hash tag with
/// release ordering. Readers acquire one control word before reading the
/// immutable colocated key and cell.
struct AtomicFixedOverlay<C, const KEY_BYTES: usize, const KEY_WORDS: usize> {
    controls: Box<[AtomicU64]>,
    slots: Box<[AtomicFixedSlot<C, KEY_WORDS>]>,
    bucket_count: usize,
    hash_builder: GenerationHashBuilder,
    key_mode: AtomicKeyMode,
}

type AtomicFixed8Overlay<C> = AtomicFixedOverlay<C, 8, 1>;
type AtomicFixed16Overlay<C> = AtomicFixedOverlay<C, 16, 2>;
type AtomicFixed24Overlay<C> = AtomicFixedOverlay<C, 24, 3>;
type AtomicFixed32Overlay<C> = AtomicFixedOverlay<C, 32, 4>;
type AtomicFixed48Overlay<C> = AtomicFixedOverlay<C, 48, 6>;

struct AtomicFixedSlot<C, const KEY_WORDS: usize> {
    key_words: [AtomicU64; KEY_WORDS],
    cell: C,
}

impl<C: StableCell, const KEY_BYTES: usize, const KEY_WORDS: usize>
    AtomicFixedOverlay<C, KEY_BYTES, KEY_WORDS>
{
    #[cfg(test)]
    fn with_capacity(capacity: usize, hash_builder: GenerationHashBuilder) -> Self {
        Self::with_capacity_mode(capacity, hash_builder, AtomicKeyMode::Exact)
    }

    fn with_capacity_mode(
        capacity: usize,
        hash_builder: GenerationHashBuilder,
        key_mode: AtomicKeyMode,
    ) -> Self {
        debug_assert_eq!(KEY_BYTES, KEY_WORDS * size_of::<u64>());
        let target_slots = capacity
            .saturating_mul(ATOMIC_TARGET_NUMERATOR)
            .div_ceil(ATOMIC_TARGET_DENOMINATOR);
        let bucket_count = target_slots.div_ceil(ATOMIC_BUCKET_SLOTS);
        let slots = bucket_count.saturating_mul(ATOMIC_BUCKET_SLOTS);
        Self {
            controls: (0..bucket_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            slots: (0..slots)
                .map(|_| AtomicFixedSlot {
                    key_words: std::array::from_fn(|_| AtomicU64::new(0)),
                    cell: C::deleted(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_count,
            hash_builder,
            key_mode,
        }
    }

    fn supports(&self, key: &[u8]) -> bool {
        match self.key_mode {
            AtomicKeyMode::Exact => key.len() == KEY_BYTES,
            AtomicKeyMode::UpTo => key.len() <= KEY_BYTES,
        }
    }

    fn physical_len(&self) -> usize {
        (0..self.bucket_count)
            .map(|bucket| self.occupied_in_bucket(bucket))
            .sum()
    }

    fn occupied_in_bucket(&self, bucket: usize) -> usize {
        loop {
            let control = self.controls[bucket].load(Ordering::Acquire);
            let (empty, writing) = atomic_bucket_frontier(control);
            if writing.is_none() {
                return empty.unwrap_or(ATOMIC_BUCKET_SLOTS);
            }
            spin_loop();
        }
    }

    fn encode_key(&self, key: &[u8]) -> Option<[u8; KEY_BYTES]> {
        if !self.supports(key) {
            return None;
        }
        match self.key_mode {
            AtomicKeyMode::UpTo if key.len() < KEY_BYTES => {
                let mut encoded = [0_u8; KEY_BYTES];
                encoded[..key.len()].copy_from_slice(key);
                encoded[KEY_BYTES - 1] =
                    u8::try_from(key.len()).expect("short atomic key length fits u8");
                Some(encoded)
            }
            AtomicKeyMode::Exact | AtomicKeyMode::UpTo => key.try_into().ok(),
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        GenerationKeyHash::new(&self.hash_builder, key).route()
    }

    fn slot_tag(&self, hash: u64, key_bytes: usize) -> u8 {
        match self.key_mode {
            AtomicKeyMode::UpTo if key_bytes == KEY_BYTES => {
                u8::try_from(hash % 128).expect("hash remainder fits u8") + 128
            }
            AtomicKeyMode::UpTo => u8::try_from(hash % 126).expect("hash remainder fits u8") + 2,
            AtomicKeyMode::Exact => atomic_slot_tag(hash),
        }
    }

    fn probe_cell_hashed<R>(
        &self,
        key: &[u8],
        hash: u64,
        read: impl FnOnce(&C) -> R,
    ) -> AtomicCellProbe<R> {
        let tag = self.slot_tag(hash, key.len());
        let Some(key) = self.encode_key(key) else {
            return AtomicCellProbe::miss(false);
        };
        if self.controls.is_empty() {
            return AtomicCellProbe::miss(false);
        }
        let primary = atomic_primary_bucket(hash, self.bucket_count);
        let primary_scan = self.scan_bucket_read(primary, tag, &key);
        if let Some(index) = primary_scan.found {
            return AtomicCellProbe::found(read(&self.slots[index].cell));
        }
        if primary_scan.empty.is_some() || self.bucket_count == 1 {
            return AtomicCellProbe::miss(primary_scan.empty.is_some());
        }
        let secondary = atomic_secondary_bucket(hash, self.bucket_count, primary);
        let secondary_scan = self.scan_bucket_read(secondary, tag, &key);
        if let Some(index) = secondary_scan.found {
            return AtomicCellProbe::found(read(&self.slots[index].cell));
        }
        if secondary_scan.empty.is_some() || self.bucket_count == 2 {
            return AtomicCellProbe::miss(secondary_scan.empty.is_some());
        }
        let tertiary = atomic_tertiary_bucket(hash, self.bucket_count, primary, secondary);
        let tertiary_scan = self.scan_bucket_read(tertiary, tag, &key);
        if let Some(index) = tertiary_scan.found {
            return AtomicCellProbe::found(read(&self.slots[index].cell));
        }
        if tertiary_scan.empty.is_some() {
            return AtomicCellProbe::miss(true);
        }
        let Some(fourth) =
            atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
        else {
            return AtomicCellProbe::miss(false);
        };
        let fourth_scan = self.scan_bucket_read(fourth, tag, &key);
        if let Some(index) = fourth_scan.found {
            return AtomicCellProbe::found(read(&self.slots[index].cell));
        }
        AtomicCellProbe::miss(fourth_scan.empty.is_some())
    }

    #[cfg(feature = "prepared-keys")]
    fn prepared_index_hashed(&self, key: &[u8], hash: u64) -> Option<usize> {
        let tag = self.slot_tag(hash, key.len());
        let key = self.encode_key(key)?;
        if self.controls.is_empty() {
            return None;
        }
        let primary = atomic_primary_bucket(hash, self.bucket_count);
        let primary_scan = self.scan_bucket_read(primary, tag, &key);
        if primary_scan.found.is_some() || primary_scan.empty.is_some() || self.bucket_count == 1 {
            return primary_scan.found;
        }
        let secondary = atomic_secondary_bucket(hash, self.bucket_count, primary);
        let secondary_scan = self.scan_bucket_read(secondary, tag, &key);
        if secondary_scan.found.is_some()
            || secondary_scan.empty.is_some()
            || self.bucket_count == 2
        {
            return secondary_scan.found;
        }
        let tertiary = atomic_tertiary_bucket(hash, self.bucket_count, primary, secondary);
        let tertiary_scan = self.scan_bucket_read(tertiary, tag, &key);
        if tertiary_scan.found.is_some() || tertiary_scan.empty.is_some() {
            return tertiary_scan.found;
        }
        let fourth = atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)?;
        self.scan_bucket_read(fourth, tag, &key).found
    }

    #[cfg(feature = "prepared-keys")]
    fn with_prepared_cell<R>(
        &self,
        index: usize,
        key: &[u8],
        read: impl FnOnce(&C) -> R,
    ) -> Option<R> {
        let key = self.encode_key(key)?;
        let slot = self.slots.get(index)?;
        let bucket = index / ATOMIC_BUCKET_SLOTS;
        let offset = index % ATOMIC_BUCKET_SLOTS;
        let control = self.controls.get(bucket)?.load(Ordering::Acquire);
        if control_byte(control, offset) < 2 || !self.key_matches(index, &key) {
            return None;
        }
        Some(read(&slot.cell))
    }

    #[cfg(test)]
    fn definitely_no_overflow_hashed(&self, key: &[u8], hash: u64) -> bool {
        if !self.supports(key) || self.controls.is_empty() {
            return false;
        }
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        if self.bucket_has_stopping_slot(primary) {
            return true;
        }
        if secondary == primary || self.bucket_has_stopping_slot(secondary) {
            return secondary != primary;
        }
        if tertiary == primary || tertiary == secondary {
            return false;
        }
        if self.bucket_has_stopping_slot(tertiary) {
            return true;
        }
        atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
            .is_some_and(|fourth| self.bucket_has_stopping_slot(fourth))
    }

    #[cfg(test)]
    fn bucket_has_stopping_slot(&self, bucket: usize) -> bool {
        let control = self.controls[bucket].load(Ordering::Acquire);
        first_control_offset(control, ATOMIC_SLOT_EMPTY).is_some()
            || first_control_offset(control, ATOMIC_SLOT_WRITING).is_some()
    }

    fn insert_or_return_hashed(&self, key: &[u8], hash: u64, cell: C) -> AtomicFixedInsert<C> {
        let tag = self.slot_tag(hash, key.len());
        let Some(key) = self.encode_key(key) else {
            return AtomicFixedInsert::Full(cell);
        };
        if self.controls.is_empty() {
            return AtomicFixedInsert::Full(cell);
        }
        let mut cell = Some(cell);
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        let mut observed_control = 0;
        loop {
            let primary_scan = self.scan_bucket_write(primary, tag, &key, &mut observed_control);
            if let Some(index) = primary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = primary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, &key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let secondary_scan = if secondary == primary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(secondary, tag, &key, &mut observed_control)
            };
            if let Some(index) = secondary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = secondary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, &key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let tertiary_scan = if tertiary == primary || tertiary == secondary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(tertiary, tag, &key, &mut observed_control)
            };
            if let Some(index) = tertiary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            let Some(index) = tertiary_scan.empty else {
                if let Some(fourth) =
                    atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
                {
                    let fourth_scan =
                        self.scan_bucket_write(fourth, tag, &key, &mut observed_control);
                    if let Some(index) = fourth_scan.found {
                        return AtomicFixedInsert::Occupied(index);
                    }
                    if let Some(index) = fourth_scan.empty {
                        if self.claim_and_publish(index, observed_control, tag, &key, &mut cell) {
                            return AtomicFixedInsert::Inserted;
                        }
                        continue;
                    }
                }
                return AtomicFixedInsert::Full(
                    cell.take()
                        .expect("full atomic buckets retain candidate cell"),
                );
            };
            if self.claim_and_publish(index, observed_control, tag, &key, &mut cell) {
                return AtomicFixedInsert::Inserted;
            }
        }
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    fn insert_or_return_encoded_hashed(
        &self,
        key_bytes: usize,
        key: &[u8; KEY_BYTES],
        hash: u64,
        cell: C,
    ) -> AtomicFixedInsert<C> {
        debug_assert!(matches!(self.key_mode, AtomicKeyMode::UpTo));
        debug_assert!(key_bytes < KEY_BYTES);
        let tag = self.slot_tag(hash, key_bytes);
        self.insert_encoded_hashed(key, hash, tag, cell)
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    #[inline]
    fn insert_encoded_hashed(
        &self,
        key: &[u8; KEY_BYTES],
        hash: u64,
        tag: u8,
        cell: C,
    ) -> AtomicFixedInsert<C> {
        if self.controls.is_empty() {
            return AtomicFixedInsert::Full(cell);
        }
        let mut cell = Some(cell);
        let (primary, secondary, tertiary) = atomic_bucket_choices(hash, self.bucket_count);
        let mut observed_control = 0;
        loop {
            let primary_scan = self.scan_bucket_write(primary, tag, key, &mut observed_control);
            if let Some(index) = primary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = primary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let secondary_scan = if secondary == primary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(secondary, tag, key, &mut observed_control)
            };
            if let Some(index) = secondary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            if let Some(index) = secondary_scan.empty {
                if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                    return AtomicFixedInsert::Inserted;
                }
                continue;
            }
            let tertiary_scan = if tertiary == primary || tertiary == secondary {
                BucketScan::default()
            } else {
                self.scan_bucket_write(tertiary, tag, key, &mut observed_control)
            };
            if let Some(index) = tertiary_scan.found {
                return AtomicFixedInsert::Occupied(index);
            }
            let Some(index) = tertiary_scan.empty else {
                if let Some(fourth) =
                    atomic_fourth_bucket(hash, self.bucket_count, primary, secondary, tertiary)
                {
                    let fourth_scan =
                        self.scan_bucket_write(fourth, tag, key, &mut observed_control);
                    if let Some(index) = fourth_scan.found {
                        return AtomicFixedInsert::Occupied(index);
                    }
                    if let Some(index) = fourth_scan.empty {
                        if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
                            return AtomicFixedInsert::Inserted;
                        }
                        continue;
                    }
                }
                return AtomicFixedInsert::Full(
                    cell.take()
                        .expect("full atomic buckets retain candidate cell"),
                );
            };
            if self.claim_and_publish(index, observed_control, tag, key, &mut cell) {
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
        let (empty, writing) = atomic_bucket_frontier(control);
        let stop = writing.or(empty).unwrap_or(ATOMIC_BUCKET_SLOTS);
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

    #[inline]
    fn scan_bucket_write(
        &self,
        bucket: usize,
        tag: u8,
        key: &[u8],
        observed_control: &mut u64,
    ) -> BucketScan {
        let begin = bucket * ATOMIC_BUCKET_SLOTS;
        loop {
            let control = self.controls[bucket].load(Ordering::Acquire);
            *observed_control = control;
            let (empty, writing) = atomic_bucket_frontier(control);
            let stop = writing.or(empty).unwrap_or(ATOMIC_BUCKET_SLOTS);
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

    fn claim_and_publish(
        &self,
        index: usize,
        control: u64,
        tag: u8,
        key: &[u8],
        cell: &mut Option<C>,
    ) -> bool {
        let bucket = index / ATOMIC_BUCKET_SLOTS;
        let offset = index % ATOMIC_BUCKET_SLOTS;
        let shift = offset * 8;
        let writing = u64::from(ATOMIC_SLOT_WRITING) << shift;
        debug_assert_eq!(control_byte(control, offset), ATOMIC_SLOT_EMPTY);
        if self.controls[bucket]
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
        // A writing byte makes every other bucket writer wait, so the
        // successful claimant exclusively owns this control word until it
        // publishes. Readers never modify controls and occupied slots are not
        // cleared. Replacing our marker therefore needs only a release store,
        // not a second read-modify-write.
        let byte_mask = u64::from(u8::MAX) << shift;
        let published = (control & !byte_mask) | (u64::from(tag) << shift);
        self.controls[bucket].store(published, Ordering::Release);
        true
    }

    fn for_each(&self, visit: &mut dyn FnMut(&[u8], &C)) {
        for bucket in 0..self.bucket_count {
            let (control, occupied) = loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                let (empty, writing) = atomic_bucket_frontier(control);
                if writing.is_none() {
                    break (control, empty.unwrap_or(ATOMIC_BUCKET_SLOTS));
                }
                spin_loop();
            };
            let begin = bucket * ATOMIC_BUCKET_SLOTS;
            for offset in 0..occupied {
                let index = begin + offset;
                let key = self.load_key(index);
                match self.key_mode {
                    AtomicKeyMode::UpTo if control_byte(control, offset) < 128 => {
                        let length = usize::from(key[KEY_BYTES - 1]);
                        debug_assert!(length < KEY_BYTES);
                        visit(&key[..length], &self.slots[index].cell);
                    }
                    AtomicKeyMode::Exact | AtomicKeyMode::UpTo => {
                        visit(&key, &self.slots[index].cell);
                    }
                }
            }
        }
    }

    fn sample(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &C) -> bool) -> bool {
        if self.bucket_count == 0 {
            return false;
        }
        // Four independent bucket probes make an empty result unlikely at the
        // target load without turning sampling into a table scan.
        for attempt in 0..4_u64 {
            let mixed = mix_sample_seed(seed, attempt);
            let bucket = reduce_sample(mixed, self.bucket_count);
            let (control, occupied) = loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                let (empty, writing) = atomic_bucket_frontier(control);
                if writing.is_none() {
                    break (control, empty.unwrap_or(ATOMIC_BUCKET_SLOTS));
                }
                spin_loop();
            };
            if occupied == 0 {
                continue;
            }
            let offset = reduce_sample(mixed.rotate_left(29), occupied);
            let index = bucket * ATOMIC_BUCKET_SLOTS + offset;
            let key = self.load_key(index);
            let accepted = match self.key_mode {
                AtomicKeyMode::UpTo if control_byte(control, offset) < 128 => {
                    let length = usize::from(key[KEY_BYTES - 1]);
                    debug_assert!(length < KEY_BYTES);
                    visit(&key[..length], &self.slots[index].cell)
                }
                AtomicKeyMode::Exact | AtomicKeyMode::UpTo => visit(&key, &self.slots[index].cell),
            };
            if accepted {
                return true;
            }
        }
        false
    }

    fn key_matches(&self, index: usize, key: &[u8]) -> bool {
        (0..KEY_WORDS).all(|word| {
            self.slots[index].key_words[word].load(Ordering::Relaxed) == key_word(key, word)
        })
    }

    fn write_key(&self, index: usize, key: &[u8]) {
        for word in 0..KEY_WORDS {
            self.slots[index].key_words[word].store(key_word(key, word), Ordering::Relaxed);
        }
    }

    fn load_key(&self, index: usize) -> [u8; KEY_BYTES] {
        let mut key = [0_u8; KEY_BYTES];
        for word in 0..KEY_WORDS {
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

struct AtomicCellProbe<R> {
    found: Option<R>,
    stops_overflow: bool,
}

impl<R> AtomicCellProbe<R> {
    fn found(value: R) -> Self {
        Self {
            found: Some(value),
            stops_overflow: false,
        }
    }

    const fn miss(stops_overflow: bool) -> Self {
        Self {
            found: None,
            stops_overflow,
        }
    }
}

enum AtomicFixedInsert<C> {
    Inserted,
    Occupied(usize),
    Full(C),
}

fn mix_sample_seed(seed: u64, attempt: u64) -> u64 {
    let mut value = seed.wrapping_add(attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn reduce_sample(hash: u64, upper: usize) -> usize {
    usize::try_from((u128::from(hash) * upper as u128) >> 64)
        .expect("reduced sample hash is below usize upper bound")
}

fn weighted_sample_limit(seed: u64, selected: usize, total: usize, limit: usize) -> usize {
    debug_assert!(selected <= total);
    if selected == 0 || total == 0 || limit == 0 {
        return 0;
    }
    let numerator = selected as u128 * limit as u128;
    let total_u128 = total as u128;
    let floor = usize::try_from(numerator / total_u128).unwrap_or(limit);
    let remainder =
        usize::try_from(numerator % total_u128).expect("sample remainder is below total");
    let rounded = floor
        + usize::from(
            remainder > 0 && reduce_sample(mix_sample_seed(seed, 0x44d7_13c9), total) < remainder,
        );
    rounded.min(limit)
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

fn atomic_bucket_frontier(control: u64) -> (Option<usize>, Option<usize>) {
    let empty = first_control_offset(control, ATOMIC_SLOT_EMPTY);
    let frontier = empty.unwrap_or(ATOMIC_BUCKET_SLOTS);
    // Controls are append-only and every claimant takes the first empty slot.
    // A visible writer is therefore the byte immediately before the new first
    // empty slot (or the final byte when it claims the last slot). Writers
    // serialize on this marker, so a bucket cannot contain a second one.
    let writing = frontier
        .checked_sub(1)
        .filter(|offset| control_byte(control, *offset) == ATOMIC_SLOT_WRITING);
    (empty, writing)
}

#[inline]
fn take_control_offset(matches: &mut u64) -> Option<usize> {
    if *matches == 0 {
        return None;
    }
    let offset = usize::try_from(matches.trailing_zeros()).expect("u64 bit offset fits usize") / 8;
    *matches &= matches.wrapping_sub(1);
    Some(offset)
}

fn atomic_bucket_choices(hash: u64, buckets: usize) -> (usize, usize, usize) {
    let primary = atomic_primary_bucket(hash, buckets);
    if buckets == 1 {
        return (primary, primary, primary);
    }
    let secondary = atomic_secondary_bucket(hash, buckets, primary);
    if buckets == 2 {
        return (primary, secondary, primary);
    }
    let tertiary = atomic_tertiary_bucket(hash, buckets, primary, secondary);
    (primary, secondary, tertiary)
}

fn atomic_primary_bucket(hash: u64, buckets: usize) -> usize {
    start_index(hash, buckets)
}

fn atomic_secondary_bucket(hash: u64, buckets: usize, primary: usize) -> usize {
    let secondary_hash = hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15;
    let secondary = start_index(secondary_hash, buckets);
    if secondary == primary {
        next_index(secondary, buckets)
    } else {
        secondary
    }
}

fn atomic_tertiary_bucket(hash: u64, buckets: usize, primary: usize, secondary: usize) -> usize {
    let tertiary_hash = hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93;
    let mut tertiary = start_index(tertiary_hash, buckets);
    while tertiary == primary || tertiary == secondary {
        tertiary = next_index(tertiary, buckets);
    }
    tertiary
}

fn atomic_fourth_bucket(
    hash: u64,
    buckets: usize,
    primary: usize,
    secondary: usize,
    tertiary: usize,
) -> Option<usize> {
    if buckets <= 3 {
        return None;
    }
    let fourth_hash = hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f;
    let mut fourth = start_index(fourth_hash, buckets);
    while fourth == primary || fourth == secondary || fourth == tertiary {
        fourth = next_index(fourth, buckets);
    }
    Some(fourth)
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
    debug_assert_ne!(slots, 0);
    usize::try_from((u128::from(hash) * slots as u128) >> 64)
        .expect("reduced bucket index is below usize upper bound")
}

const fn next_index(index: usize, slots: usize) -> usize {
    let next = index + 1;
    if next == slots { 0 } else { next }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay_cell::OverlayCell;

    fn copied_u64(value: Option<&u64>) -> Option<u64> {
        value.copied()
    }

    #[test]
    fn atomic_slot_tags_exclude_reserved_controls_with_near_uniform_mapping() {
        let mut counts = [0_u8; 256];
        for byte in 0_u64..=u64::from(u8::MAX) {
            let tag = atomic_slot_tag(byte);
            assert!(tag >= 2);
            counts[usize::from(tag)] += 1;
        }
        assert_eq!(counts.iter().filter(|count| **count != 0).count(), 254);
        assert_eq!(counts.into_iter().max(), Some(2));
    }

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
    fn bucket_frontier_derives_the_single_append_writer() {
        for occupied in 0..ATOMIC_BUCKET_SLOTS {
            let occupied_control = (0..occupied).fold(0_u64, |control, offset| {
                let tag = 2_u8 + u8::try_from(offset).expect("bucket offset fits u8");
                control | (u64::from(tag) << (offset * 8))
            });
            assert_eq!(
                atomic_bucket_frontier(occupied_control),
                (Some(occupied), None)
            );

            let writing_control =
                occupied_control | (u64::from(ATOMIC_SLOT_WRITING) << (occupied * 8));
            let empty = (occupied + 1 < ATOMIC_BUCKET_SLOTS).then_some(occupied + 1);
            assert_eq!(
                atomic_bucket_frontier(writing_control),
                (empty, Some(occupied))
            );
        }

        let full = (0..ATOMIC_BUCKET_SLOTS).fold(0_u64, |control, offset| {
            let tag = 2_u8 + u8::try_from(offset).expect("bucket offset fits u8");
            control | (u64::from(tag) << (offset * 8))
        });
        assert_eq!(atomic_bucket_frontier(full), (None, None));
    }

    #[test]
    fn fourth_bucket_is_distinct_when_available() {
        for buckets in 4..128 {
            for hash in [0, 1, u64::MAX, 0x9e37_79b9_7f4a_7c15] {
                let (primary, secondary, tertiary) = atomic_bucket_choices(hash, buckets);
                let fourth = atomic_fourth_bucket(hash, buckets, primary, secondary, tertiary)
                    .expect("four or more buckets support a fourth choice");
                assert!(![primary, secondary, tertiary].contains(&fourth));
                assert!(fourth < buckets);
            }
        }
    }

    #[test]
    fn stopping_slot_proves_fallback_absence() {
        let overlay = AtomicFixed32Overlay::<OverlayCell<u64>>::with_capacity(
            ATOMIC_BUCKET_SLOTS,
            GenerationHashBuilder::default(),
        );
        let key = [9_u8; COMPACT_KEY_BYTES];
        let hash = overlay.hash(&key);
        assert!(overlay.definitely_no_overflow_hashed(&key, hash));
        let probe = overlay.probe_cell_hashed(&key, hash, |_| 0_u64);
        assert!(probe.found.is_none());
        assert!(probe.stops_overflow);

        for control in &overlay.controls {
            control.store(u64::MAX, Ordering::Relaxed);
        }
        assert!(!overlay.definitely_no_overflow_hashed(&key, hash));
        let probe = overlay.probe_cell_hashed(&key, hash, |_| 0_u64);
        assert!(probe.found.is_none());
        assert!(!probe.stops_overflow);
    }

    #[test]
    fn elastic_overflow_segments_allocate_in_order() {
        let overflow = AtomicOverflow32::<OverlayCell<u64>>::new(
            100_000,
            GenerationHashBuilder::default(),
            AtomicKeyMode::Exact,
        );
        assert_eq!(overflow.capacities, [2_500, 625]);
        assert!(overflow.tables.iter().all(|table| table.load().is_none()));

        drop(overflow.table(0));
        assert!(overflow.tables[0].load().is_some());
        assert!(overflow.tables[1].load().is_none());

        drop(overflow.table(1));
        assert!(overflow.tables.iter().all(|table| table.load().is_some()));
    }

    #[test]
    fn dynamic_fallback_round_trips_and_samples_distinct_variable_keys() {
        let table = AtomicDynamicFallback::<OverlayCell<u64>>::with_capacity(512);
        let hash_builder = GenerationHashBuilder::default();
        let keys = (0..300_u64)
            .map(|index| {
                let mut key = vec![0_u8; 49 + usize::try_from(index % 79).unwrap()];
                key[..8].copy_from_slice(&index.to_le_bytes());
                for (offset, byte) in key.iter_mut().enumerate().skip(8) {
                    *byte = index
                        .wrapping_add(u64::try_from(offset).unwrap())
                        .to_le_bytes()[0];
                }
                key
            })
            .collect::<Vec<_>>();

        for (value, key) in keys.iter().enumerate() {
            let hash = GenerationKeyHash::new(&hash_builder, key).route();
            assert!(matches!(
                table.insert_or_return_hashed(
                    key,
                    hash,
                    OverlayCell::present(u64::try_from(value).unwrap())
                ),
                AtomicDynamicInsert::Inserted
            ));
        }
        assert_eq!(table.len(), keys.len());

        for (value, key) in keys.iter().enumerate() {
            let hash = GenerationKeyHash::new(&hash_builder, key).route();
            let found = table
                .probe_cell_hashed(key, hash, |cell| cell.with_value(copied_u64))
                .found
                .flatten();
            assert_eq!(found, Some(u64::try_from(value).unwrap()));
        }

        let duplicate = &keys[117];
        let hash = GenerationKeyHash::new(&hash_builder, duplicate).route();
        let AtomicDynamicInsert::Occupied(index) =
            table.insert_or_return_hashed(duplicate, hash, OverlayCell::present(u64::MAX))
        else {
            panic!("duplicate dynamic key must find its published entry");
        };
        assert_eq!(
            table.with_entry(index, |_, cell| cell.with_value(copied_u64)),
            Some(117)
        );

        let mut sampled = std::collections::HashSet::new();
        assert_eq!(
            table.sample_entries(0x243f_6a88_85a3_08d3, 128, &mut |key, cell| {
                assert!(cell.with_value(|value| value.is_some()));
                assert!(sampled.insert(key.to_vec()));
                true
            }),
            128
        );
    }

    #[test]
    fn dynamic_fallback_concurrent_equal_key_has_one_physical_winner() {
        let table = AtomicDynamicFallback::<OverlayCell<u64>>::with_capacity(64);
        let hash_builder = GenerationHashBuilder::default();
        let key = vec![7_u8; 73];
        let hash = GenerationKeyHash::new(&hash_builder, &key).route();
        let winners = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for writer in 0..8_u64 {
                let table = &table;
                let key = &key;
                let winners = &winners;
                scope.spawn(move || {
                    if matches!(
                        table.insert_or_return_hashed(key, hash, OverlayCell::present(writer)),
                        AtomicDynamicInsert::Inserted
                    ) {
                        winners.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        assert_eq!(winners.load(Ordering::Relaxed), 1);
        assert_eq!(table.len(), 1);
        assert!(
            table
                .probe_cell_hashed(&key, hash, |cell| cell.with_value(|value| value.is_some()))
                .found
                .unwrap()
        );
    }

    #[test]
    fn adaptive_fallback_filter_is_bounded_by_sample_working_set() {
        let large = AdaptiveAtomicOverlay::<OverlayCell<u64>>::new(
            1_000_000,
            GenerationHashBuilder::default(),
        );
        assert_eq!(large.sample_target, ADAPTIVE_SAMPLE_MAX);
        assert_eq!(large.fallback_filter.len(), ADAPTIVE_SAMPLE_MAX);
        assert!(large.fallback_filter.len() < large.capacity.div_ceil(64));

        for hash in [0, 1, 42, u64::MAX, 0x9e37_79b9_7f4a_7c15] {
            large.mark_fallback(hash);
            large.mark_papaya(hash);
            assert!(large.fallback_may_contain(hash));
            assert!(large.papaya_may_contain(hash));
        }

        let small =
            AdaptiveAtomicOverlay::<OverlayCell<u64>>::new(1_000, GenerationHashBuilder::default());
        assert_eq!(small.fallback_filter.len(), 1_000_usize.div_ceil(64));
    }

    #[test]
    fn adaptive_sample_and_dynamic_fallback_hints_remain_independent() {
        let sample =
            AdaptiveAtomicOverlay::<OverlayCell<u64>>::new(1_000, GenerationHashBuilder::default());
        sample.mark_sample(42);
        assert!(sample.sample_may_contain(42));
        assert!(!sample.fallback_may_contain(42));

        let fallback =
            AdaptiveAtomicOverlay::<OverlayCell<u64>>::new(1_000, GenerationHashBuilder::default());
        fallback.mark_fallback(42);
        assert!(!fallback.sample_may_contain(42));
        assert!(fallback.fallback_may_contain(42));
    }

    #[test]
    fn adaptive_pressure_counting_arms_lazily_from_a_physical_baseline() {
        const CAPACITY: usize = 128;
        let hash_builder = GenerationHashBuilder::default();
        let overlay = GenerationOverlay::<OverlayCell<u64>>::with_capacity(
            CAPACITY,
            GenerationOverlayMode::AtomicAdaptive,
            hash_builder.clone(),
        );
        let adaptive = overlay.adaptive.as_ref().unwrap();

        for index in 0..64_u64 {
            let key = index.to_le_bytes();
            let hash = GenerationKeyHash::new(&hash_builder, &key).route();
            assert!(matches!(
                overlay.insert_or_visit_prehashed(&key, hash, OverlayCell::present(index), |_| ()),
                OverlayInsert::Inserted
            ));
        }
        assert!(adaptive.physical_insertions.get().is_none());
        assert!(!overlay.adaptive_capacity_pressure(7_500));
        assert_eq!(adaptive.physical_baseline.get(), Some(&64));
        assert!(adaptive.physical_insertions.get().is_some());

        for index in 64..96_u64 {
            let key = index.to_le_bytes();
            let hash = GenerationKeyHash::new(&hash_builder, &key).route();
            assert!(matches!(
                overlay.insert_or_visit_prehashed(&key, hash, OverlayCell::present(index), |_| ()),
                OverlayInsert::Inserted
            ));
        }
        assert!(overlay.adaptive_capacity_pressure(7_500));
    }

    #[test]
    fn adaptive_native_sampling_visits_only_allocated_key_classes() {
        let tables = AdaptiveAtomicTables::<OverlayCell<u64>>::new(
            [0, 128, 0, 64, 0],
            GenerationHashBuilder::default(),
        );

        assert_eq!(
            (0..ADAPTIVE_KEY_CLASSES)
                .filter_map(|step| tables.sampling_class(0, step))
                .collect::<Vec<_>>(),
            [1, 3]
        );
        assert_eq!(
            (0..ADAPTIVE_KEY_CLASSES)
                .filter_map(|step| tables.sampling_class(2, step))
                .collect::<Vec<_>>(),
            [3, 1]
        );
    }

    #[test]
    fn adaptive_filter_reduction_stays_in_range_for_power_of_two_and_odd_sizes() {
        assert_eq!(adaptive_filter_location(42, 0), None);
        for words in [1, 3, 16, ADAPTIVE_SAMPLE_MAX] {
            for hash in [0, 1, 42, u64::MAX, 0x9e37_79b9_7f4a_7c15] {
                let (word, bits) = adaptive_filter_location(hash, words).unwrap();
                assert!(word < words);
                assert_eq!(bits.count_ones(), 2);
            }
        }
    }

    #[test]
    fn fallback_sample_split_preserves_small_populations_over_time() {
        assert_eq!(weighted_sample_limit(7, 0, 1_000, 64), 0);
        assert_eq!(weighted_sample_limit(7, 1_000, 1_000, 64), 64);

        let selected = (0..10_000_u64)
            .map(|seed| weighted_sample_limit(seed, ADAPTIVE_SAMPLE_MAX, 1_000_000, 1))
            .sum::<usize>();
        assert!(
            (30..=55).contains(&selected),
            "a 0.4096% population should receive about 41 of 10,000 single samples, got {selected}"
        );
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
        assert!(overlay.claim_and_publish(0, 0, tag, &key, &mut cell));

        overlay.controls[0].fetch_or(u64::from(ATOMIC_SLOT_WRITING) << 8, Ordering::Relaxed);

        assert_eq!(overlay.scan_bucket_read(0, tag, &key).found, Some(0));
        assert_eq!(overlay.scan_bucket_read(0, tag, &missing).empty, Some(1));
        let mut observed_control = 0;
        assert_eq!(
            overlay
                .scan_bucket_write(0, tag, &key, &mut observed_control)
                .found,
            Some(0)
        );
    }

    #[test]
    fn empty_fixed_bucket_write_scan_returns_first_slot_and_exact_snapshot() {
        let overlay = AtomicFixed32Overlay::<OverlayCell<u64>>::with_capacity(
            ATOMIC_BUCKET_SLOTS,
            GenerationHashBuilder::default(),
        );
        let key = [5_u8; COMPACT_KEY_BYTES];
        let mut observed_control = u64::MAX;

        let scan = overlay.scan_bucket_write(0, 42, &key, &mut observed_control);

        assert_eq!(scan.found, None);
        assert_eq!(scan.empty, Some(0));
        assert_eq!(observed_control, 0);
    }

    #[test]
    fn short_fixed_key_equality_rejects_prefix_tail_and_length_changes() {
        let overlay = AtomicFixed32Overlay::<OverlayCell<u64>>::with_capacity_mode(
            ATOMIC_BUCKET_SLOTS,
            GenerationHashBuilder::default(),
            AtomicKeyMode::UpTo,
        );
        let key = vec![3_u8; 29];
        let encoded = overlay.encode_key(&key).expect("29-byte key is supported");
        overlay.write_key(0, &encoded);
        assert!(overlay.key_matches(0, &encoded));

        let mut prefix = key.clone();
        prefix[0] ^= 1;
        let prefix = overlay.encode_key(&prefix).unwrap();
        assert!(!overlay.key_matches(0, &prefix));

        let mut tail = key.clone();
        tail[28] ^= 1;
        let tail = overlay.encode_key(&tail).unwrap();
        assert!(!overlay.key_matches(0, &tail));

        let shorter = overlay.encode_key(&key[..28]).unwrap();
        assert!(!overlay.key_matches(0, &shorter));
    }

    #[test]
    fn fixed_key_word_encoding_preserves_every_atomic_key_class() {
        fn key<const N: usize>() -> [u8; N] {
            std::array::from_fn(|index| u8::try_from(index).expect("atomic key index fits u8"))
        }

        fn verify<const N: usize>(key: [u8; N]) {
            assert_eq!(N % size_of::<u64>(), 0);
            for word in 0..N / size_of::<u64>() {
                let begin = word * size_of::<u64>();
                let expected =
                    u64::from_ne_bytes(key[begin..begin + size_of::<u64>()].try_into().unwrap());
                assert_eq!(key_word(&key, word), expected);
            }
        }

        verify(key::<8>());
        verify(key::<16>());
        verify(key::<24>());
        verify(key::<32>());
        verify(key::<48>());
    }
}
