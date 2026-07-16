use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption, Guard};
use parking_lot::Mutex;

#[cfg(feature = "prepared-keys")]
use crate::atomic_value::AtomicPreparedSlot;
use crate::atomic_value::{AtomicU64Cell, AtomicU64FrozenMap, DirectMutation, NonMaxU64};
use crate::frozen_map::Digest;
#[cfg(feature = "prepared-keys")]
use crate::frozen_map::key_digest;
use crate::generation_hash::{GenerationHashBuilder, GenerationKeyHash};
use crate::generation_overlay::{GenerationOverlay, GenerationOverlayMode, OverlayInsert};
use crate::overlay_cell::{GenerationCell, OverlayCell};
use crate::{
    FrozenBuildError, FrozenIndexBackend, FrozenMapStats, FrozenPackedMap, InsertOutcome,
    LockFreeHybridMap, LockFreeHybridStats,
};

trait GenerationFrozen<V>: Sized {
    const DIRECT_MUTATION: bool;
    const DEFAULT_BASE_FILTER: AtomicGenerationBaseFilter;

    fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>;

    fn try_from_entries_with_policy<I, K>(
        entries: I,
        _policy: AtomicGenerationBaseFilter,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries(entries)
    }

    fn try_from_entries_with_policy_and_index<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        _index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy(entries, policy)
    }

    fn try_from_entries_with_policy_index_and_hash<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        _hash_builder: &GenerationHashBuilder,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy_and_index(entries, policy, index_backend)
    }

    fn len(&self) -> usize;

    fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R;

    fn with_value_hashed<R>(
        &self,
        key: &[u8],
        _digest: Option<Digest>,
        read: impl FnOnce(Option<&V>) -> R,
    ) -> R {
        self.with_value(key, read)
    }

    fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &V));

    fn stats(&self) -> FrozenMapStats;

    #[cfg(feature = "prepared-keys")]
    fn prepare_slot(&self, _key: &[u8], _digest: Digest) -> Option<AtomicPreparedSlot> {
        None
    }

    #[cfg(feature = "prepared-keys")]
    fn with_prepared_value<R>(
        &self,
        _key: &[u8],
        _prepared: AtomicPreparedSlot,
        _read: impl FnOnce(Option<&V>) -> R,
    ) -> DirectMutation<R> {
        DirectMutation::NotMember
    }

    #[cfg(feature = "prepared-keys")]
    fn update_prepared(
        &self,
        _key: &[u8],
        _prepared: AtomicPreparedSlot,
        _update: &impl Fn(&V) -> V,
    ) -> DirectMutation<Option<V>> {
        DirectMutation::NotMember
    }

    #[cfg(feature = "prepared-keys")]
    fn insert_prepared(
        &self,
        _key: &[u8],
        _prepared: AtomicPreparedSlot,
        _value: &V,
    ) -> DirectMutation<InsertOutcome<V>> {
        DirectMutation::NotMember
    }

    #[cfg(feature = "prepared-keys")]
    fn remove_prepared(
        &self,
        _key: &[u8],
        _prepared: AtomicPreparedSlot,
    ) -> DirectMutation<Option<V>> {
        DirectMutation::NotMember
    }

    fn direct_insert(
        &self,
        _key: &[u8],
        _digest: Option<Digest>,
        _value: &V,
    ) -> Option<InsertOutcome<V>> {
        None
    }

    fn direct_insert_new(&self, _key: &[u8], _digest: Option<Digest>, _value: &V) -> Option<bool> {
        None
    }

    fn direct_update(
        &self,
        _key: &[u8],
        _digest: Option<Digest>,
        _update: &impl Fn(&V) -> V,
    ) -> DirectMutation<Option<V>> {
        DirectMutation::NotMember
    }

    fn direct_upsert(
        &self,
        _key: &[u8],
        _digest: Option<Digest>,
        _insert_value: &V,
        _update: &impl Fn(&V) -> V,
    ) -> Option<(V, bool)> {
        None
    }

    fn direct_remove_if(
        &self,
        _key: &[u8],
        _digest: Option<Digest>,
        _predicate: &impl Fn(&V) -> bool,
    ) -> DirectMutation<Option<V>> {
        DirectMutation::NotMember
    }
}

impl<V> GenerationFrozen<V> for FrozenPackedMap<V> {
    const DIRECT_MUTATION: bool = false;
    const DEFAULT_BASE_FILTER: AtomicGenerationBaseFilter = AtomicGenerationBaseFilter::Disabled;

    fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries(entries)
    }

    fn try_from_entries_with_policy_and_index<I, K>(
        entries: I,
        _policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_index_backend(entries, index_backend)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        read(self.get(key))
    }

    fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &V)) {
        self.for_each_entry(visit);
    }

    fn stats(&self) -> FrozenMapStats {
        self.stats()
    }
}

impl GenerationFrozen<NonMaxU64> for AtomicU64FrozenMap {
    const DIRECT_MUTATION: bool = true;
    const DEFAULT_BASE_FILTER: AtomicGenerationBaseFilter = AtomicGenerationBaseFilter::Disabled;

    fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries(entries)
    }

    fn try_from_entries_with_policy<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy(entries, policy)
    }

    fn try_from_entries_with_policy_and_index<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy_and_index(entries, policy, index_backend)
    }

    fn try_from_entries_with_policy_index_and_hash<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        hash_builder: &GenerationHashBuilder,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy_index_and_hash(
            entries,
            policy,
            index_backend,
            hash_builder,
        )
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&NonMaxU64>) -> R) -> R {
        self.with_value(key, read)
    }

    fn with_value_hashed<R>(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        read: impl FnOnce(Option<&NonMaxU64>) -> R,
    ) -> R {
        self.with_value_hashed(key, digest, read)
    }

    fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &NonMaxU64)) {
        self.for_each_entry(visit);
    }

    fn stats(&self) -> FrozenMapStats {
        self.stats()
    }

    #[cfg(feature = "prepared-keys")]
    fn prepare_slot(&self, key: &[u8], digest: Digest) -> Option<AtomicPreparedSlot> {
        self.prepare_slot(key, digest)
    }

    #[cfg(feature = "prepared-keys")]
    fn with_prepared_value<R>(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        read: impl FnOnce(Option<&NonMaxU64>) -> R,
    ) -> DirectMutation<R> {
        self.with_prepared_value(key, prepared, read)
    }

    #[cfg(feature = "prepared-keys")]
    fn update_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        self.update_prepared(key, prepared, update)
    }

    #[cfg(feature = "prepared-keys")]
    fn insert_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: &NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        self.insert_prepared(key, prepared, *value)
    }

    #[cfg(feature = "prepared-keys")]
    fn remove_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
    ) -> DirectMutation<Option<NonMaxU64>> {
        self.remove_prepared(key, prepared)
    }

    fn direct_insert(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: &NonMaxU64,
    ) -> Option<InsertOutcome<NonMaxU64>> {
        self.insert_hashed(key, digest, *value)
    }

    fn direct_insert_new(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: &NonMaxU64,
    ) -> Option<bool> {
        self.insert_new_hashed(key, digest, *value)
    }

    fn direct_update(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        self.update_hashed(key, digest, update)
    }

    fn direct_upsert(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        insert_value: &NonMaxU64,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<(NonMaxU64, bool)> {
        self.upsert_hashed(key, digest, *insert_value, update)
    }

    fn direct_remove_if(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        predicate: &impl Fn(&NonMaxU64) -> bool,
    ) -> DirectMutation<Option<NonMaxU64>> {
        self.remove_if_hashed(key, digest, predicate)
    }
}

/// Layered packed generations with lock-free point reads and writes.
///
/// Values in the writable overlay use stable atomically replaced `Arc` cells.
/// This supports arbitrary cloneable values and avoids reallocating an owned
/// key on repeated mutations. Use [`LockFreeAtomicU64GenerationMap`] when the
/// value domain fits [`NonMaxU64`] and allocation-free mutations are important.
pub struct LockFreeGenerationMap<V> {
    inner: GenerationMapCore<V, OverlayCell<V>, FrozenPackedMap<V>>,
}

impl<V> LockFreeGenerationMap<V> {
    /// Wraps an existing packed-base/lock-free-overlay generation.
    #[must_use]
    pub fn from_hybrid(initial: LockFreeHybridMap<V>) -> Self {
        Self {
            inner: GenerationMapCore::from_hybrid(initial),
        }
    }

    /// Builds generation zero from unique binary-key entries.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map build error for duplicate keys, allocation failure,
    /// or perfect-index construction failure.
    pub fn try_from_entries<I, K>(
        entries: I,
        overlay_capacity: usize,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: GenerationMapCore::try_from_entries(entries, overlay_capacity)?,
        })
    }

    /// Clones the latest value through a lock-free layered read.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.inner.get_cloned(key)
    }

    /// Runs `read` against one safely pinned generation without a lock.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        self.inner.with_value(key, read)
    }

    /// Returns whether the latest generation contains `key`.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    /// Inserts or replaces a value through the lock-free writer path.
    pub fn insert(&self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        self.inner.insert(key, value)
    }

    /// Inserts only when the logical key is absent.
    pub fn insert_new(&self, key: &[u8], value: V) -> bool
    where
        V: Clone,
    {
        self.inner.insert_new(key, value)
    }

    /// Atomically transforms an existing value with a retry-safe function.
    pub fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        self.inner.update(key, update)
    }

    /// Atomically updates an existing value or inserts a default.
    pub fn upsert(&self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        self.inner.upsert(key, insert_value, update)
    }

    /// Removes and clones the latest logical value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.inner.remove(key)
    }

    /// Removes a value accepted by a retry-safe predicate.
    #[must_use]
    pub fn remove_if(&self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        self.inner.remove_if(key, predicate)
    }

    /// Redirects writer stripes and packs the stable old generation.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error. The redirected layer remains
    /// usable and retains its previous-generation base if packing fails.
    pub fn rebuild(&self, overlay_capacity: usize) -> Result<GenerationRebuild, FrozenBuildError>
    where
        V: Clone,
    {
        self.inner.rebuild(overlay_capacity)
    }

    /// Returns the current logical entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether the current logical generation is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of successfully packed generation publications.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.generation()
    }

    /// Captures current packed-base, overlay, and layer populations.
    #[must_use]
    pub fn stats(&self) -> GenerationMapStats {
        self.inner.stats()
    }
}

/// A packed-generation map with allocation-free atomic `u64`-class mutations.
///
/// The restricted [`NonMaxU64`] value domain lets each frozen or overlay cell
/// encode both its value and deletion marker in one [`AtomicU64`]. Existing
/// frozen keys update, reinsert, and delete directly in their perfect-hash
/// slots without allocating. Only keys absent from the frozen generation need
/// an owned overlay record.
pub struct LockFreeAtomicU64GenerationMap {
    inner: GenerationMapCore<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
}

/// Prepared routing and frozen-slot metadata for one repeatedly accessed key.
///
/// The handle never weakens exact key semantics. Callers still supply the key
/// bytes, which are verified before a dense slot is used. Rebuilds and
/// overlay-shadowed stripes automatically fall back to the ordinary lookup.
#[cfg(feature = "prepared-keys")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AtomicPreparedKey {
    route_hash: u64,
    base_id: u64,
    slot: u64,
}

#[cfg(feature = "prepared-keys")]
impl AtomicPreparedKey {
    const NO_SLOT: u64 = u64::MAX;

    /// Creates an exact ordinary-lookup marker for a mixed prepared batch.
    ///
    /// This is useful when only part of a caller-owned batch has prepared
    /// handles. Operations using this marker skip the direct-slot attempt and
    /// perform a normal exact lookup.
    #[must_use]
    pub const fn fallback() -> Self {
        Self {
            route_hash: 0,
            base_id: 0,
            slot: Self::NO_SLOT,
        }
    }

    fn new(route_hash: u64, prepared: Option<AtomicPreparedSlot>) -> Self {
        let (base_id, slot) = prepared.map_or((0, Self::NO_SLOT), |prepared| {
            (
                prepared.base_id,
                u64::try_from(prepared.slot).unwrap_or(Self::NO_SLOT),
            )
        });
        Self {
            route_hash,
            base_id,
            slot,
        }
    }

    fn prepared_slot(self) -> Option<AtomicPreparedSlot> {
        (self.base_id != 0 && self.slot != Self::NO_SLOT).then(|| AtomicPreparedSlot {
            base_id: self.base_id,
            slot: usize::try_from(self.slot).expect("prepared slot originated as usize"),
        })
    }

    /// Returns whether preparation captured a candidate direct frozen slot.
    ///
    /// Validity is checked on every operation because rebuilds can make a
    /// previously captured slot stale. A `false` handle remains safe to use,
    /// but takes the ordinary lookup path until prepared again.
    #[must_use]
    pub const fn has_direct_slot(self) -> bool {
        self.base_id != 0 && self.slot != Self::NO_SLOT
    }
}

/// Mutable-overlay implementation used by [`LockFreeAtomicU64GenerationMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicGenerationOverlay {
    /// Fixed-capacity, allocation-free atomic slots for 32-byte keys with a
    /// dynamic Papaya overflow map.
    AtomicFixed32,
    /// Lock-free nodes with inline 32-byte keys, with a dynamic boxed-key
    /// fallback for other key sizes.
    CompactFixed32,
    /// Preallocates one exact inline width for 8, 16, 24, 31, 40, 48, 56, or
    /// 64-byte keys. Other widths select the boxed-key fallback; 32-byte
    /// workloads should use `CompactFixed32`.
    CompactSized {
        /// Expected binary-key length used to select the preallocated class.
        key_bytes: u8,
    },
    /// Experimental dense atomic-pointer slots for 32-byte keys. This saves
    /// memory but is retained mainly as a measured performance control.
    ArcSwapFixed32,
    /// Papaya's fully dynamic lock-free table, retained as a control and as a
    /// useful choice when keys are rarely 32 bytes long.
    Papaya,
}

/// Definite-negative policy for the immutable atomic generation.
///
/// The filter never replaces the exact perfect-hash lookup. A negative answer
/// only skips that lookup; a positive answer is always verified against the
/// frozen map. Bits are not cleared after direct deletes, so the shortcut
/// remains safe for the lifetime of a generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AtomicGenerationBaseFilter {
    /// Retains no extra membership data.
    #[default]
    Disabled,
    /// Embeds a nine-bit digest tag into each short packed-key reference.
    ///
    /// This adds no retained bytes and rejects most misses after perfect-hash
    /// routing but before loading the member key bytes. If any frozen key is
    /// longer than 255 bytes, the generation safely falls back to exact-only
    /// references.
    EmbeddedFingerprint,
    /// Retains a two-probe blocked filter using about one byte per frozen key.
    OneBytePerEntry,
}

impl From<AtomicGenerationOverlay> for GenerationOverlayMode {
    fn from(value: AtomicGenerationOverlay) -> Self {
        match value {
            AtomicGenerationOverlay::AtomicFixed32 => Self::AtomicFixed32,
            AtomicGenerationOverlay::CompactFixed32 => Self::CompactFixed32,
            AtomicGenerationOverlay::CompactSized { key_bytes } => Self::CompactSized(key_bytes),
            AtomicGenerationOverlay::ArcSwapFixed32 => Self::ArcSwapFixed32,
            AtomicGenerationOverlay::Papaya => Self::Papaya,
        }
    }
}

impl LockFreeAtomicU64GenerationMap {
    /// Wraps an existing hybrid whose values use the atomic value domain.
    #[must_use]
    pub fn from_hybrid(initial: LockFreeHybridMap<NonMaxU64>) -> Self {
        Self {
            inner: GenerationMapCore::from_hybrid(initial),
        }
    }

    /// Builds generation zero from unique binary-key entries.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map build error for duplicate keys, allocation failure,
    /// or perfect-index construction failure.
    pub fn try_from_entries<I, K>(
        entries: I,
        overlay_capacity: usize,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: GenerationMapCore::try_from_entries(entries, overlay_capacity)?,
        })
    }

    /// Builds generation zero with an explicit mutable-overlay strategy.
    ///
    /// This constructor primarily supports workload measurement. The ordinary
    /// constructor selects [`AtomicGenerationOverlay::AtomicFixed32`].
    ///
    /// # Errors
    ///
    /// Returns a frozen-map build error for duplicate keys, allocation failure,
    /// or perfect-index construction failure.
    pub fn try_from_entries_with_overlay<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay: AtomicGenerationOverlay,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: GenerationMapCore::try_from_entries_with_modes(
                entries,
                overlay_capacity,
                overlay.into(),
                AtomicGenerationBaseFilter::Disabled,
            )?,
        })
    }

    /// Builds generation zero with explicit overlay and frozen-base filter
    /// strategies.
    ///
    /// This constructor is intended for workload-specific measurement. The
    /// ordinary atomic constructors select
    /// [`AtomicGenerationBaseFilter::Disabled`] for hit-heavy workloads.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map build error for duplicate keys, allocation failure,
    /// or perfect-index construction failure.
    pub fn try_from_entries_with_options<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay: AtomicGenerationOverlay,
        base_filter: AtomicGenerationBaseFilter,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: GenerationMapCore::try_from_entries_with_modes(
                entries,
                overlay_capacity,
                overlay.into(),
                base_filter,
            )?,
        })
    }

    /// Builds a map with a caller-supplied writer-routing hash state.
    ///
    /// This is exposed for controlled paired measurements where every strategy
    /// must route keys through identical writer stripes. Normal applications
    /// should use [`Self::try_from_entries_with_options`].
    #[doc(hidden)]
    pub fn try_from_entries_with_options_and_writer_hash<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay: AtomicGenerationOverlay,
        base_filter: AtomicGenerationBaseFilter,
        writer_hash_builder: GenerationHashBuilder,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_options_and_writer_hash_and_index(
            entries,
            overlay_capacity,
            overlay,
            base_filter,
            writer_hash_builder,
            FrozenIndexBackend::PtrHash,
        )
    }

    /// Builds with explicit writer routing and frozen perfect-hash backends.
    ///
    /// This exists for paired backend experiments. Normal applications should
    /// keep using [`Self::try_from_entries_with_options`].
    #[doc(hidden)]
    pub fn try_from_entries_with_options_and_writer_hash_and_index<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay: AtomicGenerationOverlay,
        base_filter: AtomicGenerationBaseFilter,
        writer_hash_builder: GenerationHashBuilder,
        frozen_index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Ok(Self {
            inner: GenerationMapCore::try_from_entries_with_modes_writer_hash_and_index(
                entries,
                overlay_capacity,
                overlay.into(),
                base_filter,
                writer_hash_builder,
                frozen_index_backend,
            )?,
        })
    }

    /// Returns the latest value through a lock-free layered read.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.inner.get_cloned(key)
    }

    /// Prepares a compact exact handle for repeated reads of `key`.
    ///
    /// Preparation performs the normal routing and frozen lookup once. The
    /// returned handle remains safe across mutations and rebuilds; stale slot
    /// metadata simply falls back to [`Self::get`].
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn prepare_key(&self, key: &[u8]) -> AtomicPreparedKey {
        self.inner.prepare_key(key)
    }

    /// Prepares or refreshes a batch of exact handles without allocating.
    ///
    /// The slices must have equal lengths. This is intended for refreshing a
    /// caller-owned hot set after [`Self::rebuild`] publishes a new frozen
    /// generation.
    ///
    /// # Panics
    ///
    /// Panics when `keys` and `prepared` do not have equal lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn prepare_key_batch<K>(&self, keys: &[K], prepared: &mut [AtomicPreparedKey])
    where
        K: AsRef<[u8]>,
    {
        self.inner.prepare_key_batch(keys, prepared);
    }

    /// Reads with a previously prepared key handle.
    ///
    /// The supplied bytes are always checked before a cached dense slot is
    /// trusted. Handles from another key, map, or generation remain exact and
    /// fall back to the ordinary lookup path.
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn get_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<NonMaxU64> {
        self.inner.get_prepared(key, prepared)
    }

    /// Reads a batch of prepared keys into caller-provided storage.
    ///
    /// The three slices must have equal lengths. The fast path pins one
    /// generation for the entire batch, verifies every original key, and
    /// allocates nothing. If a rebuild publishes while the batch is running,
    /// every result is transparently refreshed through the new generation
    /// before this method returns.
    ///
    /// Invalid, stale, cross-map, overlay, or wrong-key handles retain exact
    /// semantics by taking the ordinary lookup path for that item.
    ///
    /// # Panics
    ///
    /// Panics when `keys`, `prepared`, and `values` do not have equal lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn get_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        self.inner.get_prepared_batch(keys, prepared, values);
    }

    /// Atomically updates a key through a prepared direct slot when valid.
    ///
    /// A stale, cross-map, overlay, or wrong-key handle falls back to the
    /// ordinary writer path before invoking `update`.
    #[cfg(feature = "prepared-keys")]
    pub fn update_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<NonMaxU64> {
        self.inner.update_prepared(key, prepared, update)
    }

    /// Atomically updates a batch of prepared keys without allocating.
    ///
    /// By default one generation snapshot is shared while every item pins its
    /// writer stripe. With `prepared-batch-gate`, one of 16 sharded generation
    /// gates is pinned once for the whole batch instead. A concurrent rebuild
    /// or invalid prepared handle falls back to the ordinary exact writer route
    /// for that item.
    ///
    /// `update` may be invoked repeatedly after compare-and-swap races and must
    /// therefore be pure and free of external side effects.
    ///
    /// # Panics
    ///
    /// Panics when `keys`, `prepared`, and `updated` do not have equal lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn update_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        updated: &mut [Option<NonMaxU64>],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) where
        K: AsRef<[u8]>,
    {
        self.inner
            .update_prepared_batch(keys, prepared, updated, update);
    }

    /// Inserts or replaces through a prepared direct slot when valid.
    ///
    /// Invalid handles fall back to the ordinary exact writer path. Length
    /// accounting remains correct when the prepared slot was deleted.
    #[cfg(feature = "prepared-keys")]
    pub fn insert_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: NonMaxU64,
    ) -> InsertOutcome<NonMaxU64> {
        self.inner.insert_prepared(key, prepared, value)
    }

    /// Inserts or replaces a batch through prepared direct slots.
    ///
    /// `previous` receives `None` for a newly inserted value and `Some(old)`
    /// for a replacement. The operation allocates no batch storage internally;
    /// keys that require the overlay retain the ordinary insertion behavior.
    /// The optional `prepared-batch-gate` feature amortizes writer pinning once
    /// per batch using 16 cache-line-separated generation gates.
    ///
    /// # Panics
    ///
    /// Panics unless `keys`, `prepared`, `values`, and `previous` have equal
    /// lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn insert_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &[NonMaxU64],
        previous: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        self.inner
            .insert_prepared_batch(keys, prepared, values, previous);
    }

    /// Removes through a prepared direct slot when valid.
    ///
    /// Invalid handles fall back to the ordinary exact writer path.
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn remove_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<NonMaxU64> {
        self.inner.remove_prepared(key, prepared)
    }

    /// Runs `read` against one safely pinned generation without a lock.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&NonMaxU64>) -> R) -> R {
        self.inner.with_value(key, read)
    }

    /// Returns whether the latest generation contains `key`.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    /// Inserts or replaces a value through the lock-free writer path.
    pub fn insert(&self, key: &[u8], value: NonMaxU64) -> InsertOutcome<NonMaxU64> {
        self.inner.insert(key, value)
    }

    /// Inserts only when the logical key is absent.
    pub fn insert_new(&self, key: &[u8], value: NonMaxU64) -> bool {
        self.inner.insert_new(key, value)
    }

    /// Atomically transforms an existing value without allocating.
    pub fn update(
        &self,
        key: &[u8],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<NonMaxU64> {
        self.inner.update(key, update)
    }

    /// Atomically updates an existing value or inserts a default.
    pub fn upsert(
        &self,
        key: &[u8],
        insert_value: NonMaxU64,
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> NonMaxU64 {
        self.inner.upsert(key, insert_value, update)
    }

    /// Removes the latest logical value without allocating for a frozen member.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.inner.remove(key)
    }

    /// Removes a value accepted by a retry-safe predicate.
    #[must_use]
    pub fn remove_if(
        &self,
        key: &[u8],
        predicate: impl Fn(&NonMaxU64) -> bool,
    ) -> Option<NonMaxU64> {
        self.inner.remove_if(key, predicate)
    }

    /// Redirects writer stripes and packs the stable old generation.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error. The redirected layer remains
    /// usable and retains its previous-generation base if packing fails.
    pub fn rebuild(&self, overlay_capacity: usize) -> Result<GenerationRebuild, FrozenBuildError> {
        self.inner.rebuild(overlay_capacity)
    }

    /// Returns the current logical entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether the current logical generation is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of successfully packed generation publications.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.generation()
    }

    /// Captures current packed-base, overlay, and layer populations.
    #[must_use]
    pub fn stats(&self) -> GenerationMapStats {
        self.inner.stats()
    }
}

/// Layered packed generations with lock-free point reads and writes.
///
/// The active layer contains a lock-free overlay and an atomically replaceable
/// base. Rebuild first publishes a fresh overlay whose base points at the old
/// generation. A striped atomic handoff keeps every equal key on one layer:
/// writers continue using an open predecessor stripe until rebuild closes that
/// stripe at zero active writers, after which they use the new overlay. The old
/// generation is immutable once all stripes close and can be packed while all
/// point operations continue. Replacing the old base with an exactly equivalent
/// frozen base needs no delta replay and no writer gate.
///
/// Rebuild calls are serialized because they are aggregate maintenance
/// operations. Point operations never acquire that mutex.
struct GenerationMapCore<V, C, B> {
    current: ArcSwap<GenerationLayer<V, C, B>>,
    rebuild_gate: Mutex<()>,
    writer_hash_builder: GenerationHashBuilder,
    overlay_mode: GenerationOverlayMode,
    base_filter: AtomicGenerationBaseFilter,
    frozen_index_backend: FrozenIndexBackend,
    generation: AtomicU64,
}

impl<V, C, B> GenerationMapCore<V, C, B>
where
    C: GenerationCell<V>,
    B: GenerationFrozen<V>,
{
    /// Wraps an existing packed-base/lock-free-overlay generation.
    ///
    /// The supplied hybrid becomes a stable base under a new writable layer.
    #[must_use]
    pub fn from_hybrid(initial: LockFreeHybridMap<V>) -> Self {
        let len = initial.len();
        let overlay_mode = Self::default_overlay_mode();
        let writer_hash_builder = GenerationHashBuilder::default();
        Self::from_layer(
            GenerationLayer::with_base(
                GenerationBase::StableHybrid(Arc::new(initial)),
                len,
                0,
                overlay_mode,
                writer_hash_builder.clone(),
            ),
            overlay_mode,
            writer_hash_builder,
            B::DEFAULT_BASE_FILTER,
            FrozenIndexBackend::PtrHash,
        )
    }

    /// Builds generation zero from unique binary-key entries.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map build error for duplicate keys, allocation
    /// failure, or perfect-index construction failure.
    pub fn try_from_entries<I, K>(
        entries: I,
        overlay_capacity: usize,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_modes(
            entries,
            overlay_capacity,
            Self::default_overlay_mode(),
            B::DEFAULT_BASE_FILTER,
        )
    }

    fn try_from_entries_with_modes<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay_mode: GenerationOverlayMode,
        base_filter: AtomicGenerationBaseFilter,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_modes_and_writer_hash(
            entries,
            overlay_capacity,
            overlay_mode,
            base_filter,
            GenerationHashBuilder::default(),
        )
    }

    fn try_from_entries_with_modes_and_writer_hash<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay_mode: GenerationOverlayMode,
        base_filter: AtomicGenerationBaseFilter,
        writer_hash_builder: GenerationHashBuilder,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_modes_writer_hash_and_index(
            entries,
            overlay_capacity,
            overlay_mode,
            base_filter,
            writer_hash_builder,
            FrozenIndexBackend::PtrHash,
        )
    }

    fn try_from_entries_with_modes_writer_hash_and_index<I, K>(
        entries: I,
        overlay_capacity: usize,
        overlay_mode: GenerationOverlayMode,
        base_filter: AtomicGenerationBaseFilter,
        writer_hash_builder: GenerationHashBuilder,
        frozen_index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        let frozen = B::try_from_entries_with_policy_index_and_hash(
            entries,
            base_filter,
            frozen_index_backend,
            &writer_hash_builder,
        )?;
        let len = frozen.len();
        Ok(Self::from_layer(
            GenerationLayer::with_base(
                GenerationBase::frozen(frozen, base_filter, &writer_hash_builder),
                len,
                overlay_capacity,
                overlay_mode,
                writer_hash_builder.clone(),
            ),
            overlay_mode,
            writer_hash_builder,
            base_filter,
            frozen_index_backend,
        ))
    }

    /// Clones the latest value through a lock-free layered read.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value(key, |value| value.cloned())
    }

    /// Runs `read` against one safely pinned generation without a lock.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        let generation = self.current.load();
        if B::DIRECT_MUTATION {
            let (stripe, key_hash) = self.writer_route(key);
            generation.with_value_in_stripe(key, key_hash, stripe, read)
        } else {
            generation.with_value(key, read)
        }
    }

    /// Returns whether the latest generation contains `key`.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.with_value(key, |value| value.is_some())
    }

    /// Inserts or replaces a value through the lock-free writer path.
    pub fn insert(&self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        self.pin_writer(key).insert(key, value)
    }

    /// Inserts only when the logical key is absent.
    pub fn insert_new(&self, key: &[u8], value: V) -> bool
    where
        V: Clone,
    {
        self.pin_writer(key).insert_new(key, value)
    }

    /// Atomically transforms an existing value with a retry-safe function.
    ///
    /// `update` may run repeatedly after a Papaya compare-and-swap race and
    /// must be pure and free of external side effects.
    pub fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        self.pin_writer(key).update(key, update)
    }

    /// Atomically updates an existing value or inserts a default.
    ///
    /// `update` has the same retry-safe requirement as [`Self::update`].
    pub fn upsert(&self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        self.pin_writer(key).upsert(key, insert_value, update)
    }

    /// Removes and clones the latest logical value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.pin_writer(key).remove(key)
    }

    /// Removes a value accepted by a retry-safe predicate.
    #[must_use]
    pub fn remove_if(&self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        self.pin_writer(key).remove_if(key, predicate)
    }

    /// Redirects writer stripes to a new overlay and packs the stable old layer.
    ///
    /// Point readers and writers continue throughout construction and stripe
    /// closure. There is no changed-key replay: the new overlay remains active
    /// while its base changes from the stable previous generation to an exactly
    /// equivalent packed map. Stripe closure can starve maintenance under an
    /// uninterrupted stream of writers to one stripe, but it does not make
    /// those point writers wait for rebuild.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error. The redirected layer remains
    /// correct and continues to reference the previous generation if packing
    /// fails; a later rebuild can compact the resulting extra layer.
    ///
    /// # Panics
    ///
    /// Panics only if a packed base entry has an invalid key reference, which
    /// indicates internal invariant corruption.
    pub fn rebuild(&self, overlay_capacity: usize) -> Result<GenerationRebuild, FrozenBuildError>
    where
        V: Clone,
    {
        let _one_rebuild = self.rebuild_gate.lock();
        let previous = self.current.load_full();
        let previous_stats = previous.stats();
        let next = GenerationLayer::with_base(
            GenerationBase::Previous(Arc::clone(&previous)),
            previous.len(),
            overlay_capacity,
            self.overlay_mode,
            self.writer_hash_builder.clone(),
        );

        let redirect_started = Instant::now();
        self.current.store(Arc::clone(&next));
        #[cfg(feature = "prepared-batch-gate")]
        previous.close_prepared_batch_writers();
        previous.close_writer_stripes();
        next.initial_len.store(
            isize::try_from(previous.len()).unwrap_or(isize::MAX),
            Ordering::Release,
        );
        next.write_predecessor.store(None);
        next.write_predecessor_active
            .store(false, Ordering::Release);
        let writer_redirect = redirect_started.elapsed();

        let build_started = Instant::now();
        let frozen = previous.try_build_frozen(
            self.base_filter,
            self.frozen_index_backend,
            &self.writer_hash_builder,
        )?;
        let background_build = build_started.elapsed();
        let entries = frozen.len();

        let publish_started = Instant::now();
        next.base.store(Arc::new(GenerationBase::frozen(
            frozen,
            self.base_filter,
            &self.writer_hash_builder,
        )));
        if B::DIRECT_MUTATION {
            next.enable_direct_base_stripes();
        }
        let base_publish = publish_started.elapsed();
        let from_generation = self.generation.fetch_add(1, Ordering::AcqRel);

        Ok(GenerationRebuild {
            from_generation,
            to_generation: from_generation.wrapping_add(1),
            entries,
            compacted_overlay_records: previous_stats.current.overlay_records,
            previous_layer_depth: previous_stats.layer_depth,
            writer_redirect,
            background_build,
            base_publish,
        })
    }

    /// Returns the current logical entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.current.load().len()
    }

    /// Returns whether the current logical generation is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.current.load().is_empty()
    }

    /// Returns the number of successfully packed generation publications.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Captures current packed-base, overlay, and layer populations.
    #[must_use]
    pub fn stats(&self) -> GenerationMapStats {
        let generation = self.current.load();
        let layer = generation.stats();
        GenerationMapStats {
            generation: self.generation(),
            current: layer.current,
            layer_depth: layer.layer_depth,
            active_writers: generation.active_writers(),
            base_filter_bytes: layer.base_filter_bytes,
        }
    }

    fn from_layer(
        initial: Arc<GenerationLayer<V, C, B>>,
        overlay_mode: GenerationOverlayMode,
        writer_hash_builder: GenerationHashBuilder,
        base_filter: AtomicGenerationBaseFilter,
        frozen_index_backend: FrozenIndexBackend,
    ) -> Self {
        Self {
            current: ArcSwap::from(initial),
            rebuild_gate: Mutex::new(()),
            writer_hash_builder,
            overlay_mode,
            base_filter,
            frozen_index_backend,
            generation: AtomicU64::new(0),
        }
    }

    const fn default_overlay_mode() -> GenerationOverlayMode {
        if C::COMPACT_FIXED32_OVERLAY {
            GenerationOverlayMode::AtomicFixed32
        } else {
            GenerationOverlayMode::Papaya
        }
    }

    fn pin_writer(&self, key: &[u8]) -> GenerationWriter<V, C, B> {
        let (stripe, key_hash) = self.writer_route(key);
        loop {
            let generation = self.current.load();
            // An open predecessor stripe remains the single writer destination
            // for this key until maintenance closes it at count zero. Equal
            // keys always hash to the same stripe, preventing cross-layer CAS
            // updates from observing the same old value.
            if generation.write_predecessor_active.load(Ordering::Acquire)
                && let Some(previous) = generation.write_predecessor.load_full()
                && let Some(writer) = GenerationWriter::try_pin_arc(previous, stripe, key_hash)
            {
                return writer;
            }
            if let Some(writer) = GenerationWriter::try_pin_guard(generation, stripe, key_hash) {
                return writer;
            }
            spin_loop();
        }
    }

    fn writer_route(&self, key: &[u8]) -> (usize, GenerationKeyHash) {
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let key_hash = GenerationKeyHash::new(&self.writer_hash_builder, key);
        let stripe = usize::try_from(key_hash.route() & stripe_mask)
            .expect("masked writer stripe fits usize");
        (stripe, key_hash)
    }
}

#[cfg(feature = "prepared-keys")]
impl GenerationMapCore<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    fn prepare_key(&self, key: &[u8]) -> AtomicPreparedKey {
        let generation = self.current.load();
        self.prepare_key_in_generation(&generation, key)
    }

    fn prepare_key_batch<K>(&self, keys: &[K], prepared: &mut [AtomicPreparedKey])
    where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        let generation = self.current.load();
        for (key, prepared) in keys.iter().zip(prepared.iter_mut()) {
            *prepared = self.prepare_key_in_generation(&generation, key.as_ref());
        }

        let current = self.current.load();
        if Arc::ptr_eq(&generation, &current) {
            return;
        }
        drop(current);
        drop(generation);

        for (key, prepared) in keys.iter().zip(prepared) {
            *prepared = self.prepare_key(key.as_ref());
        }
    }

    fn prepare_key_in_generation(
        &self,
        generation: &GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
    ) -> AtomicPreparedKey {
        let key_hash = GenerationKeyHash::new(&self.writer_hash_builder, key);
        let route_hash = key_hash.route();
        let digest = key_hash.frozen().unwrap_or_else(|| key_digest(key));
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        let prepared = generation.prepare_slot(key, route_hash, digest, stripe);
        AtomicPreparedKey::new(route_hash, prepared)
    }

    fn get_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<NonMaxU64> {
        let prepared = *prepared;
        let generation = self.current.load();
        self.get_prepared_in_generation(&generation, key, prepared)
    }

    fn get_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            values.len(),
            "prepared output batch length mismatch"
        );

        let generation = self.current.load();
        for ((key, prepared), value) in keys.iter().zip(prepared).zip(values.iter_mut()) {
            *value = self.get_prepared_in_generation(&generation, key.as_ref(), *prepared);
        }

        let current = self.current.load();
        if Arc::ptr_eq(&generation, &current) {
            return;
        }
        drop(current);
        drop(generation);

        for (key, value) in keys.iter().zip(values) {
            *value = self.get_cloned(key.as_ref());
        }
    }

    fn get_prepared_in_generation(
        &self,
        generation: &GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        prepared: AtomicPreparedKey,
    ) -> Option<NonMaxU64> {
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = usize::try_from(prepared.route_hash & stripe_mask)
            .expect("masked writer stripe fits usize");
        if let Some(slot) = prepared.prepared_slot()
            && let DirectMutation::Handled(value) =
                generation.with_prepared_value(key, slot, stripe, copy_optional_non_max)
        {
            return value;
        }

        let key_hash = GenerationKeyHash::new(&self.writer_hash_builder, key);
        let stripe = usize::try_from(key_hash.route() & stripe_mask)
            .expect("masked writer stripe fits usize");
        generation.with_value_in_stripe(key, key_hash, stripe, copy_optional_non_max)
    }

    fn update_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<NonMaxU64> {
        if let Some(slot) = prepared.prepared_slot() {
            let writer = self.pin_prepared_writer(prepared.route_hash);
            if writer.direct_base && !writer.overlay_may_shadow_base {
                match writer
                    .generation
                    .base
                    .load()
                    .update_prepared(key, slot, &update)
                {
                    DirectMutation::Handled(updated) => return updated,
                    DirectMutation::NotMember => {}
                }
            }
            drop(writer);
        }
        self.update(key, update)
    }

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn update_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        updated: &mut [Option<NonMaxU64>],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            updated.len(),
            "prepared update output batch length mismatch"
        );

        let generation = self.current.load();
        let predecessor = generation
            .write_predecessor_active
            .load(Ordering::Acquire)
            .then(|| generation.write_predecessor.load_full())
            .flatten();
        for ((key, prepared), updated) in keys.iter().zip(prepared).zip(updated.iter_mut()) {
            let key = key.as_ref();
            match Self::update_prepared_in_snapshot(
                &generation,
                predecessor.as_deref(),
                key,
                *prepared,
                &update,
            ) {
                DirectMutation::Handled(value) => *updated = value,
                DirectMutation::NotMember => *updated = self.update(key, &update),
            }
        }
    }

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn update_prepared_in_snapshot(
        generation: &GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        predecessor: Option<&GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some(slot) = prepared.prepared_slot() else {
            return DirectMutation::NotMember;
        };
        let Some(writer) =
            AtomicPreparedBorrowedWriter::try_pin(generation, predecessor, prepared.route_hash)
        else {
            return DirectMutation::NotMember;
        };
        if !writer.direct_base || writer.overlay_may_shadow_base {
            return DirectMutation::NotMember;
        }
        writer
            .generation
            .base
            .load()
            .update_prepared(key, slot, update)
    }

    fn insert_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: NonMaxU64,
    ) -> InsertOutcome<NonMaxU64> {
        if let Some(slot) = prepared.prepared_slot() {
            let writer = self.pin_prepared_writer(prepared.route_hash);
            if writer.direct_base && !writer.overlay_may_shadow_base {
                match writer
                    .generation
                    .base
                    .load()
                    .insert_prepared(key, slot, &value)
                {
                    DirectMutation::Handled(outcome) => {
                        if matches!(outcome, InsertOutcome::Inserted) {
                            writer.generation.adjust_len(writer.stripe, 1);
                        }
                        return outcome;
                    }
                    DirectMutation::NotMember => {}
                }
            }
            drop(writer);
        }
        self.insert(key, value)
    }

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn insert_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &[NonMaxU64],
        previous: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            values.len(),
            "prepared value batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            previous.len(),
            "prepared insert output batch length mismatch"
        );

        let generation = self.current.load();
        let predecessor = generation
            .write_predecessor_active
            .load(Ordering::Acquire)
            .then(|| generation.write_predecessor.load_full())
            .flatten();
        for (((key, prepared), value), previous) in keys
            .iter()
            .zip(prepared)
            .zip(values)
            .zip(previous.iter_mut())
        {
            let key = key.as_ref();
            let outcome = match Self::insert_prepared_in_snapshot(
                &generation,
                predecessor.as_deref(),
                key,
                *prepared,
                *value,
            ) {
                DirectMutation::Handled(outcome) => outcome,
                DirectMutation::NotMember => self.insert(key, *value),
            };
            *previous = match outcome {
                InsertOutcome::Inserted => None,
                InsertOutcome::Replaced(value) => Some(value),
            };
        }
    }

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn insert_prepared_in_snapshot(
        generation: &GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        predecessor: Option<&GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        value: NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        let Some(slot) = prepared.prepared_slot() else {
            return DirectMutation::NotMember;
        };
        let Some(writer) =
            AtomicPreparedBorrowedWriter::try_pin(generation, predecessor, prepared.route_hash)
        else {
            return DirectMutation::NotMember;
        };
        if !writer.direct_base || writer.overlay_may_shadow_base {
            return DirectMutation::NotMember;
        }
        match writer
            .generation
            .base
            .load()
            .insert_prepared(key, slot, &value)
        {
            DirectMutation::Handled(outcome) => {
                if matches!(outcome, InsertOutcome::Inserted) {
                    writer.generation.adjust_len(writer.stripe, 1);
                }
                DirectMutation::Handled(outcome)
            }
            DirectMutation::NotMember => DirectMutation::NotMember,
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn update_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        updated: &mut [Option<NonMaxU64>],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            updated.len(),
            "prepared update output batch length mismatch"
        );
        if keys.is_empty() {
            return;
        }

        let route_hash = prepared
            .iter()
            .copied()
            .find(|prepared| prepared.has_direct_slot())
            .map_or(0, |prepared| prepared.route_hash);
        let writer = self.pin_prepared_batch_writer(route_hash);
        for ((key, prepared), updated) in keys.iter().zip(prepared).zip(updated.iter_mut()) {
            let key = key.as_ref();
            match Self::update_prepared_in_batch_writer(&writer, key, *prepared, &update) {
                DirectMutation::Handled(value) => *updated = value,
                DirectMutation::NotMember => *updated = self.update(key, &update),
            }
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn update_prepared_in_batch_writer(
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        prepared: AtomicPreparedKey,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some((slot, _stripe)) = Self::prepared_batch_direct_slot(writer, prepared) else {
            return DirectMutation::NotMember;
        };
        writer
            .generation
            .base
            .load()
            .update_prepared(key, slot, update)
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn insert_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &[NonMaxU64],
        previous: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            values.len(),
            "prepared value batch length mismatch"
        );
        assert_eq!(
            keys.len(),
            previous.len(),
            "prepared insert output batch length mismatch"
        );
        if keys.is_empty() {
            return;
        }

        let route_hash = prepared
            .iter()
            .copied()
            .find(|prepared| prepared.has_direct_slot())
            .map_or(0, |prepared| prepared.route_hash);
        let writer = self.pin_prepared_batch_writer(route_hash);
        for (((key, prepared), value), previous) in keys
            .iter()
            .zip(prepared)
            .zip(values)
            .zip(previous.iter_mut())
        {
            let key = key.as_ref();
            let outcome =
                match Self::insert_prepared_in_batch_writer(&writer, key, *prepared, *value) {
                    DirectMutation::Handled(outcome) => outcome,
                    DirectMutation::NotMember => self.insert(key, *value),
                };
            *previous = match outcome {
                InsertOutcome::Inserted => None,
                InsertOutcome::Replaced(value) => Some(value),
            };
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn insert_prepared_in_batch_writer(
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        prepared: AtomicPreparedKey,
        value: NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        let Some((slot, stripe)) = Self::prepared_batch_direct_slot(writer, prepared) else {
            return DirectMutation::NotMember;
        };
        match writer
            .generation
            .base
            .load()
            .insert_prepared(key, slot, &value)
        {
            DirectMutation::Handled(outcome) => {
                if matches!(outcome, InsertOutcome::Inserted) {
                    writer.generation.adjust_len(stripe, 1);
                }
                DirectMutation::Handled(outcome)
            }
            DirectMutation::NotMember => DirectMutation::NotMember,
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn prepared_batch_direct_slot(
        writer: &AtomicPreparedBatchWriter,
        prepared: AtomicPreparedKey,
    ) -> Option<(AtomicPreparedSlot, usize)> {
        let slot = prepared.prepared_slot()?;
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = usize::try_from(prepared.route_hash & stripe_mask)
            .expect("masked writer stripe fits usize");
        let state = writer.generation.writer_stripes[stripe].load(Ordering::Acquire);
        (state & WRITER_STRIPE_CLOSED == 0
            && state & WRITER_STRIPE_DIRECT_BASE != 0
            && state & WRITER_STRIPE_OVERLAY_BASE == 0)
            .then_some((slot, stripe))
    }

    fn remove_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<NonMaxU64> {
        if let Some(slot) = prepared.prepared_slot() {
            let writer = self.pin_prepared_writer(prepared.route_hash);
            if writer.direct_base && !writer.overlay_may_shadow_base {
                match writer.generation.base.load().remove_prepared(key, slot) {
                    DirectMutation::Handled(removed) => {
                        if removed.is_some() {
                            writer.generation.adjust_len(writer.stripe, -1);
                        }
                        return removed;
                    }
                    DirectMutation::NotMember => {}
                }
            }
            drop(writer);
        }
        self.remove(key)
    }

    fn pin_prepared_writer(&self, route_hash: u64) -> AtomicPreparedWriter {
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        loop {
            let generation = self.current.load();
            if generation.write_predecessor_active.load(Ordering::Acquire)
                && let Some(previous) = generation.write_predecessor.load_full()
                && let Some(writer) = AtomicPreparedWriter::try_pin_arc(previous, stripe)
            {
                return writer;
            }
            if let Some(writer) = AtomicPreparedWriter::try_pin_guard(generation, stripe) {
                return writer;
            }
            spin_loop();
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn pin_prepared_batch_writer(&self, route_hash: u64) -> AtomicPreparedBatchWriter {
        loop {
            let generation = self.current.load();
            if generation.write_predecessor_active.load(Ordering::Acquire)
                && let Some(previous) = generation.write_predecessor.load_full()
                && let Some(writer) = AtomicPreparedBatchWriter::try_pin_arc(previous, route_hash)
            {
                return writer;
            }
            if let Some(writer) = AtomicPreparedBatchWriter::try_pin_guard(generation, route_hash) {
                return writer;
            }
            spin_loop();
        }
    }
}

#[cfg(feature = "prepared-keys")]
fn copy_optional_non_max(value: Option<&NonMaxU64>) -> Option<NonMaxU64> {
    value.copied()
}

#[cfg(feature = "prepared-keys")]
struct AtomicPreparedWriter {
    generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    stripe: usize,
    direct_base: bool,
    overlay_may_shadow_base: bool,
}

#[cfg(feature = "prepared-keys")]
impl AtomicPreparedWriter {
    fn try_pin_arc(
        generation: Arc<GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        stripe: usize,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Arc(generation), stripe)
    }

    fn try_pin_guard(
        generation: Guard<Arc<GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>>,
        stripe: usize,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Guard(generation), stripe)
    }

    fn try_pin(
        generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        stripe: usize,
    ) -> Option<Self> {
        let counter = &generation.writer_stripes[stripe];
        let mut state = counter.load(Ordering::Acquire);
        loop {
            if state & WRITER_STRIPE_CLOSED != 0 {
                return None;
            }
            assert!(
                state & WRITER_STRIPE_COUNT_MASK < WRITER_STRIPE_COUNT_MASK,
                "writer stripe counter overflow"
            );
            match counter.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Self {
                        generation,
                        stripe,
                        direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
                        overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
                    });
                }
                Err(observed) => state = observed,
            }
        }
    }
}

#[cfg(feature = "prepared-keys")]
impl Drop for AtomicPreparedWriter {
    fn drop(&mut self) {
        let previous = self.generation.writer_stripes[self.stripe].fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

#[cfg(all(feature = "prepared-keys", not(feature = "prepared-batch-gate")))]
struct AtomicPreparedBorrowedWriter<'a> {
    generation: &'a GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    stripe: usize,
    direct_base: bool,
    overlay_may_shadow_base: bool,
}

#[cfg(all(feature = "prepared-keys", not(feature = "prepared-batch-gate")))]
impl<'a> AtomicPreparedBorrowedWriter<'a> {
    fn try_pin(
        generation: &'a GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        predecessor: Option<&'a GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        route_hash: u64,
    ) -> Option<Self> {
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        predecessor
            .and_then(|predecessor| Self::try_pin_generation(predecessor, stripe))
            .or_else(|| Self::try_pin_generation(generation, stripe))
    }

    fn try_pin_generation(
        generation: &'a GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        stripe: usize,
    ) -> Option<Self> {
        let counter = &generation.writer_stripes[stripe];
        let mut state = counter.load(Ordering::Acquire);
        loop {
            if state & WRITER_STRIPE_CLOSED != 0 {
                return None;
            }
            assert!(
                state & WRITER_STRIPE_COUNT_MASK < WRITER_STRIPE_COUNT_MASK,
                "writer stripe counter overflow"
            );
            match counter.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Self {
                        generation,
                        stripe,
                        direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
                        overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
                    });
                }
                Err(observed) => state = observed,
            }
        }
    }
}

#[cfg(all(feature = "prepared-keys", not(feature = "prepared-batch-gate")))]
impl Drop for AtomicPreparedBorrowedWriter<'_> {
    fn drop(&mut self) {
        let previous = self.generation.writer_stripes[self.stripe].fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

#[cfg(feature = "prepared-batch-gate")]
struct AtomicPreparedBatchWriter {
    generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    gate: usize,
}

#[cfg(feature = "prepared-batch-gate")]
impl AtomicPreparedBatchWriter {
    fn try_pin_arc(
        generation: Arc<GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        route_hash: u64,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Arc(generation), route_hash)
    }

    fn try_pin_guard(
        generation: Guard<Arc<GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>>,
        route_hash: u64,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Guard(generation), route_hash)
    }

    fn try_pin(
        generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        route_hash: u64,
    ) -> Option<Self> {
        let gate_mask =
            u64::try_from(PREPARED_BATCH_WRITER_GATES - 1).expect("batch gate mask fits u64");
        let gate = usize::try_from(route_hash & gate_mask)
            .expect("masked prepared batch writer gate fits usize");
        let counter = &generation.prepared_batch_writers[gate].0;
        let mut state = counter.load(Ordering::Acquire);
        loop {
            if state & WRITER_STRIPE_CLOSED != 0 {
                return None;
            }
            assert!(
                state & WRITER_STRIPE_COUNT_MASK < WRITER_STRIPE_COUNT_MASK,
                "prepared batch writer counter overflow"
            );
            match counter.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Self { generation, gate }),
                Err(observed) => state = observed,
            }
        }
    }
}

#[cfg(feature = "prepared-batch-gate")]
impl Drop for AtomicPreparedBatchWriter {
    fn drop(&mut self) {
        let previous = self.generation.prepared_batch_writers[self.gate]
            .0
            .fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

struct GenerationWriter<V, C, B> {
    generation: PinnedGeneration<V, C, B>,
    route: GenerationWriteRoute,
}

#[derive(Clone, Copy)]
struct GenerationWriteRoute {
    stripe: usize,
    direct_base: bool,
    overlay_may_shadow_base: bool,
    key_hash: GenerationKeyHash,
}

impl<V, C, B> GenerationWriter<V, C, B> {
    fn try_pin_arc(
        generation: Arc<GenerationLayer<V, C, B>>,
        stripe: usize,
        key_hash: GenerationKeyHash,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Arc(generation), stripe, key_hash)
    }

    fn try_pin_guard(
        generation: Guard<Arc<GenerationLayer<V, C, B>>>,
        stripe: usize,
        key_hash: GenerationKeyHash,
    ) -> Option<Self> {
        Self::try_pin(PinnedGeneration::Guard(generation), stripe, key_hash)
    }

    fn try_pin(
        generation: PinnedGeneration<V, C, B>,
        stripe: usize,
        key_hash: GenerationKeyHash,
    ) -> Option<Self> {
        let counter = &generation.writer_stripes[stripe];
        let mut state = counter.load(Ordering::Acquire);
        loop {
            if state & WRITER_STRIPE_CLOSED != 0 {
                return None;
            }
            assert!(
                state & WRITER_STRIPE_COUNT_MASK < WRITER_STRIPE_COUNT_MASK,
                "writer stripe counter overflow"
            );
            match counter.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Self {
                        generation,
                        route: GenerationWriteRoute {
                            stripe,
                            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
                            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
                            key_hash,
                        },
                    });
                }
                Err(observed) => state = observed,
            }
        }
    }
}

impl<V, C, B> GenerationWriter<V, C, B>
where
    C: GenerationCell<V>,
    B: GenerationFrozen<V>,
{
    fn insert(&self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        self.generation.insert(key, value, self.route)
    }

    fn insert_new(&self, key: &[u8], value: V) -> bool
    where
        V: Clone,
    {
        self.generation.insert_new(key, value, self.route)
    }

    fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        self.generation.update(key, update, self.route)
    }

    fn upsert(&self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        self.generation
            .upsert(key, insert_value, update, self.route)
    }

    fn remove(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.remove_if(key, |_| true)
    }

    fn remove_if(&self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        self.generation.remove_if(key, predicate, self.route)
    }
}

enum PinnedGeneration<V, C, B> {
    Guard(Guard<Arc<GenerationLayer<V, C, B>>>),
    Arc(Arc<GenerationLayer<V, C, B>>),
}

impl<V, C, B> Deref for PinnedGeneration<V, C, B> {
    type Target = GenerationLayer<V, C, B>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Guard(generation) => generation,
            Self::Arc(generation) => generation,
        }
    }
}

impl<V, C, B> Deref for GenerationWriter<V, C, B> {
    type Target = GenerationLayer<V, C, B>;

    fn deref(&self) -> &Self::Target {
        &self.generation
    }
}

impl<V, C, B> Drop for GenerationWriter<V, C, B> {
    fn drop(&mut self) {
        let previous =
            self.generation.writer_stripes[self.route.stripe].fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

struct GenerationLayer<V, C, B> {
    base: ArcSwap<GenerationBase<V, C, B>>,
    write_predecessor: ArcSwapOption<GenerationLayer<V, C, B>>,
    write_predecessor_active: AtomicBool,
    overlay: GenerationOverlay<C>,
    initial_len: AtomicIsize,
    len_deltas: Box<[PaddedAtomicIsize]>,
    writer_stripes: Box<[AtomicUsize]>,
    #[cfg(feature = "prepared-batch-gate")]
    prepared_batch_writers: Box<[PaddedAtomicUsize]>,
}

impl<V, C, B> GenerationLayer<V, C, B>
where
    C: GenerationCell<V>,
    B: GenerationFrozen<V>,
{
    fn with_base(
        base: GenerationBase<V, C, B>,
        len: usize,
        overlay_capacity: usize,
        overlay_mode: GenerationOverlayMode,
        overlay_hash_builder: GenerationHashBuilder,
    ) -> Arc<Self> {
        let (write_predecessor, write_predecessor_active) = match &base {
            GenerationBase::Previous(previous) => (Some(Arc::clone(previous)), true),
            GenerationBase::Frozen { .. } | GenerationBase::StableHybrid(_) => (None, false),
        };
        let direct_base = matches!(&base, GenerationBase::Frozen { .. }) && B::DIRECT_MUTATION;
        Arc::new(Self {
            base: ArcSwap::from_pointee(base),
            write_predecessor: ArcSwapOption::from(write_predecessor),
            write_predecessor_active: AtomicBool::new(write_predecessor_active),
            overlay: GenerationOverlay::with_capacity(
                overlay_capacity,
                overlay_mode,
                overlay_hash_builder,
            ),
            initial_len: AtomicIsize::new(isize::try_from(len).unwrap_or(isize::MAX)),
            len_deltas: (0..LEN_STRIPES)
                .map(|_| PaddedAtomicIsize(AtomicIsize::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            writer_stripes: (0..WRITER_STRIPES)
                .map(|_| AtomicUsize::new(usize::from(direct_base) * WRITER_STRIPE_DIRECT_BASE))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            #[cfg(feature = "prepared-batch-gate")]
            prepared_batch_writers: (0..PREPARED_BATCH_WRITER_GATES)
                .map(|_| PaddedAtomicUsize(AtomicUsize::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        let mut read = Some(read);
        let overlay_value = self.overlay.with_cell(key, |cell| {
            cell.map(|cell| {
                cell.with_value(read.take().expect("generation read callback runs once"))
            })
        });
        match overlay_value {
            Some(value) => value,
            None => self.base.load().with_value(
                key,
                read.take().expect("generation read callback runs once"),
            ),
        }
    }

    fn with_value_in_stripe<R>(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        stripe: usize,
        read: impl FnOnce(Option<&V>) -> R,
    ) -> R {
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        let mut read = Some(read);
        if state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0 {
            let base_value = self.base.load().with_present_value(key, key_hash, |value| {
                read.take().expect("generation read callback runs once")(Some(value))
            });
            if let Some(value) = base_value {
                return value;
            }
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| match cell {
                    Some(cell) => {
                        cell.with_value(read.take().expect("generation read callback runs once"))
                    }
                    None => read.take().expect("generation read callback runs once")(None),
                });
        }

        let overlay_value = self
            .overlay
            .with_cell_prehashed(key, key_hash.route(), |cell| {
                cell.map(|cell| {
                    cell.with_value(read.take().expect("generation read callback runs once"))
                })
            });
        match overlay_value {
            Some(value) => value,
            None => self.base.load().with_value_hashed(
                key,
                key_hash,
                read.take().expect("generation read callback runs once"),
            ),
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn prepare_slot(
        &self,
        key: &[u8],
        route_hash: u64,
        digest: Digest,
        stripe: usize,
    ) -> Option<AtomicPreparedSlot> {
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        if state & WRITER_STRIPE_DIRECT_BASE == 0 || state & WRITER_STRIPE_OVERLAY_BASE != 0 {
            return None;
        }
        self.base.load().prepare_slot(key, route_hash, digest)
    }

    #[cfg(feature = "prepared-keys")]
    fn with_prepared_value<R>(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        stripe: usize,
        read: impl FnOnce(Option<&V>) -> R,
    ) -> DirectMutation<R> {
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        if state & WRITER_STRIPE_DIRECT_BASE == 0 || state & WRITER_STRIPE_OVERLAY_BASE != 0 {
            return DirectMutation::NotMember;
        }
        self.base.load().with_prepared_value(key, prepared, read)
    }

    fn insert(&self, key: &[u8], value: V, route: GenerationWriteRoute) -> InsertOutcome<V>
    where
        V: Clone,
    {
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some(outcome) = self.base.load().direct_insert(key, route.key_hash, &value)
        {
            if matches!(outcome, InsertOutcome::Inserted) {
                self.adjust_len(route.stripe, 1);
            }
            return outcome;
        }
        if direct_first {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.replace(value),
            ) {
                OverlayInsert::Inserted => {
                    self.adjust_len(route.stripe, 1);
                    InsertOutcome::Inserted
                }
                OverlayInsert::Occupied(previous) => {
                    if let Some(previous) = previous {
                        InsertOutcome::Replaced(previous)
                    } else {
                        self.adjust_len(route.stripe, 1);
                        InsertOutcome::Inserted
                    }
                }
            };
        }
        if let Some(previous) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.replace(value.clone()))
                })
        {
            return if let Some(previous) = previous {
                InsertOutcome::Replaced(previous)
            } else {
                self.adjust_len(route.stripe, 1);
                InsertOutcome::Inserted
            };
        }
        if route.direct_base
            && !direct_first
            && let Some(outcome) = self.base.load().direct_insert(key, route.key_hash, &value)
        {
            if matches!(outcome, InsertOutcome::Inserted) {
                self.adjust_len(route.stripe, 1);
            }
            return outcome;
        }
        let base_value = if route.direct_base {
            None
        } else {
            self.base.load().get_cloned_hashed(key, route.key_hash)
        };
        if base_value.is_some() {
            self.mark_overlay_may_shadow_base(route.stripe);
        }
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(value.clone()),
            |current| current.replace(value),
        ) {
            OverlayInsert::Inserted => base_value.map_or_else(
                || {
                    self.adjust_len(route.stripe, 1);
                    InsertOutcome::Inserted
                },
                InsertOutcome::Replaced,
            ),
            OverlayInsert::Occupied(previous) => {
                if let Some(previous) = previous {
                    InsertOutcome::Replaced(previous)
                } else {
                    self.adjust_len(route.stripe, 1);
                    InsertOutcome::Inserted
                }
            }
        }
    }

    fn insert_new(&self, key: &[u8], value: V, route: GenerationWriteRoute) -> bool
    where
        V: Clone,
    {
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some(inserted) = self
                .base
                .load()
                .direct_insert_new(key, route.key_hash, &value)
        {
            if inserted {
                self.adjust_len(route.stripe, 1);
            }
            return inserted;
        }
        if direct_first {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.insert_new(value),
            ) {
                OverlayInsert::Inserted => {
                    self.adjust_len(route.stripe, 1);
                    true
                }
                OverlayInsert::Occupied(inserted) => {
                    if inserted {
                        self.adjust_len(route.stripe, 1);
                    }
                    inserted
                }
            };
        }
        if let Some(inserted) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.insert_new(value.clone()))
                })
        {
            if inserted {
                self.adjust_len(route.stripe, 1);
            }
            return inserted;
        }
        if route.direct_base
            && !direct_first
            && let Some(inserted) = self
                .base
                .load()
                .direct_insert_new(key, route.key_hash, &value)
        {
            if inserted {
                self.adjust_len(route.stripe, 1);
            }
            return inserted;
        }
        if !route.direct_base && self.base.load().contains_key_hashed(key, route.key_hash) {
            return false;
        }
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(value.clone()),
            |current| current.insert_new(value),
        ) {
            OverlayInsert::Inserted => {
                self.adjust_len(route.stripe, 1);
                true
            }
            OverlayInsert::Occupied(inserted) => {
                if inserted {
                    self.adjust_len(route.stripe, 1);
                }
                inserted
            }
        }
    }

    fn update(&self, key: &[u8], update: impl Fn(&V) -> V, route: GenerationWriteRoute) -> Option<V>
    where
        V: Clone,
    {
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first {
            match self.base.load().direct_update(key, route.key_hash, &update) {
                DirectMutation::Handled(updated) => return updated,
                DirectMutation::NotMember => {}
            }
        }
        if let Some(updated) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.update(&update))
                })
        {
            return updated;
        }
        if route.direct_base && !direct_first {
            return match self.base.load().direct_update(key, route.key_hash, &update) {
                DirectMutation::Handled(updated) => updated,
                DirectMutation::NotMember => None,
            };
        }
        let base_value = self.base.load().get_cloned_hashed(key, route.key_hash)?;
        let next = update(&base_value);
        self.mark_overlay_may_shadow_base(route.stripe);
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(next.clone()),
            |current| current.update(&update),
        ) {
            OverlayInsert::Inserted => Some(next),
            OverlayInsert::Occupied(updated) => updated,
        }
    }

    fn upsert(
        &self,
        key: &[u8],
        insert_value: V,
        update: impl Fn(&V) -> V,
        route: GenerationWriteRoute,
    ) -> V
    where
        V: Clone,
    {
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some((value, became_live)) =
                self.base
                    .load()
                    .direct_upsert(key, route.key_hash, &insert_value, &update)
        {
            if became_live {
                self.adjust_len(route.stripe, 1);
            }
            return value;
        }
        if direct_first {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(insert_value.clone()),
                |current| current.upsert(&insert_value, &update),
            ) {
                OverlayInsert::Inserted => {
                    self.adjust_len(route.stripe, 1);
                    insert_value
                }
                OverlayInsert::Occupied((value, became_live)) => {
                    if became_live {
                        self.adjust_len(route.stripe, 1);
                    }
                    value
                }
            };
        }
        if let Some((value, became_live)) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.upsert(&insert_value, &update))
                })
        {
            if became_live {
                self.adjust_len(route.stripe, 1);
            }
            return value;
        }
        if route.direct_base
            && !direct_first
            && let Some((value, became_live)) =
                self.base
                    .load()
                    .direct_upsert(key, route.key_hash, &insert_value, &update)
        {
            if became_live {
                self.adjust_len(route.stripe, 1);
            }
            return value;
        }
        let base_value = if route.direct_base {
            None
        } else {
            self.base.load().get_cloned_hashed(key, route.key_hash)
        };
        if base_value.is_some() {
            self.mark_overlay_may_shadow_base(route.stripe);
        }
        let next = base_value
            .as_ref()
            .map_or_else(|| insert_value.clone(), &update);
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(next.clone()),
            |current| current.upsert(&insert_value, &update),
        ) {
            OverlayInsert::Inserted => {
                if base_value.is_none() {
                    self.adjust_len(route.stripe, 1);
                }
                next
            }
            OverlayInsert::Occupied((value, became_live)) => {
                if became_live {
                    self.adjust_len(route.stripe, 1);
                }
                value
            }
        }
    }

    fn remove_if(
        &self,
        key: &[u8],
        predicate: impl Fn(&V) -> bool,
        route: GenerationWriteRoute,
    ) -> Option<V>
    where
        V: Clone,
    {
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first {
            match self
                .base
                .load()
                .direct_remove_if(key, route.key_hash, &predicate)
            {
                DirectMutation::Handled(removed) => {
                    if removed.is_some() {
                        self.adjust_len(route.stripe, -1);
                    }
                    return removed;
                }
                DirectMutation::NotMember => {}
            }
        }
        if let Some(removed) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.remove_if(&predicate))
                })
        {
            if removed.is_some() {
                self.adjust_len(route.stripe, -1);
            }
            return removed;
        }
        if route.direct_base && !direct_first {
            let removed = match self
                .base
                .load()
                .direct_remove_if(key, route.key_hash, &predicate)
            {
                DirectMutation::Handled(removed) => removed,
                DirectMutation::NotMember => None,
            };
            if removed.is_some() {
                self.adjust_len(route.stripe, -1);
            }
            return removed;
        }
        let base_value = self
            .base
            .load()
            .get_cloned_hashed(key, route.key_hash)
            .filter(|value| predicate(value))?;
        self.mark_overlay_may_shadow_base(route.stripe);
        let removed = match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::deleted(),
            |current| current.remove_if(&predicate),
        ) {
            OverlayInsert::Inserted => Some(base_value),
            OverlayInsert::Occupied(removed) => removed,
        };
        if removed.is_some() {
            self.adjust_len(route.stripe, -1);
        }
        removed
    }

    fn len(&self) -> usize {
        let len = self
            .len_deltas
            .iter()
            .fold(self.initial_len.load(Ordering::Acquire), |len, delta| {
                len.saturating_add(delta.0.load(Ordering::Acquire))
            });
        usize::try_from(len.max(0)).unwrap_or(usize::MAX)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn stats(&self) -> LayerStats {
        let base = self.base.load().stats();
        let mut present = 0;
        let mut deleted = 0;
        self.overlay.for_each(&mut |_, value| {
            value.with_value(|value| {
                if value.is_some() {
                    present += 1;
                } else {
                    deleted += 1;
                }
            });
        });
        LayerStats {
            current: LockFreeHybridStats {
                len: self.len(),
                base: base.frozen,
                overlay_records: base.overlay_present + base.overlay_deleted + present + deleted,
                overlay_present: base.overlay_present + present,
                overlay_deleted: base.overlay_deleted + deleted,
            },
            layer_depth: base.layer_depth + 1,
            base_filter_bytes: base.base_filter_bytes,
        }
    }

    fn try_build_frozen(
        &self,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        hash_builder: &GenerationHashBuilder,
    ) -> Result<B, FrozenBuildError>
    where
        V: Clone,
    {
        let mut entries = Vec::<(Box<[u8]>, V)>::new();
        entries
            .try_reserve(self.len())
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        self.for_each_entry_hashed(hash_builder, &mut |key, value| {
            entries.push((key.into(), value.clone()));
        });
        B::try_from_entries_with_policy_index_and_hash(entries, policy, index_backend, hash_builder)
    }

    fn for_each_entry_hashed(
        &self,
        hash_builder: &GenerationHashBuilder,
        visit: &mut dyn FnMut(&[u8], &V),
    ) {
        let base = self.base.load();
        base.for_each_entry_hashed(hash_builder, &mut |key, base_value| {
            self.overlay.with_cell(key, |cell| match cell {
                Some(cell) => cell.with_value(|value| {
                    if let Some(value) = value {
                        visit(key, value);
                    }
                }),
                None => visit(key, base_value),
            });
        });
        self.overlay.for_each(&mut |key, value| {
            if base.contains_key_hashed(key, GenerationKeyHash::new(hash_builder, key)) {
                return;
            }
            value.with_value(|value| {
                if let Some(value) = value {
                    visit(key, value);
                }
            });
        });
    }

    fn close_writer_stripes(&self) {
        for counter in &self.writer_stripes {
            let mut spins = 0_u32;
            let mut state = counter.load(Ordering::Acquire);
            loop {
                if state & WRITER_STRIPE_CLOSED != 0 {
                    break;
                }
                if state & WRITER_STRIPE_COUNT_MASK == 0 {
                    match counter.compare_exchange_weak(
                        state,
                        WRITER_STRIPE_CLOSED,
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

    #[cfg(feature = "prepared-batch-gate")]
    fn close_prepared_batch_writers(&self) {
        for gate in &self.prepared_batch_writers {
            let previous = gate.0.fetch_or(WRITER_STRIPE_CLOSED, Ordering::AcqRel);
            if previous & WRITER_STRIPE_COUNT_MASK == 0 {
                continue;
            }

            let mut spins = 0_u32;
            while gate.0.load(Ordering::Acquire) & WRITER_STRIPE_COUNT_MASK != 0 {
                if spins < 64 {
                    spin_loop();
                    spins += 1;
                } else {
                    thread::yield_now();
                }
            }
        }
    }

    fn enable_direct_base_stripes(&self) {
        for counter in &self.writer_stripes {
            let mut spins = 0_u32;
            let mut state = counter.load(Ordering::Acquire);
            loop {
                debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
                if state & WRITER_STRIPE_DIRECT_BASE != 0 {
                    break;
                }
                if state & WRITER_STRIPE_COUNT_MASK == 0 {
                    match counter.compare_exchange_weak(
                        state,
                        state | WRITER_STRIPE_DIRECT_BASE,
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

    fn active_writers(&self) -> usize {
        let current = self
            .writer_stripes
            .iter()
            .map(|counter| counter.load(Ordering::Acquire) & WRITER_STRIPE_COUNT_MASK)
            .sum::<usize>();
        self.write_predecessor
            .load()
            .as_ref()
            .map_or(current, |previous| {
                current.saturating_add(previous.active_writers())
            })
    }

    fn adjust_len(&self, stripe: usize, amount: isize) {
        self.len_deltas[stripe & (LEN_STRIPES - 1)]
            .0
            .fetch_add(amount, Ordering::Relaxed);
    }

    fn mark_overlay_may_shadow_base(&self, stripe: usize) {
        self.writer_stripes[stripe].fetch_or(WRITER_STRIPE_OVERLAY_BASE, Ordering::Release);
    }
}

#[repr(align(64))]
struct PaddedAtomicIsize(AtomicIsize);

const WRITER_STRIPES: usize = 4_096;
#[cfg(feature = "prepared-batch-gate")]
const PREPARED_BATCH_WRITER_GATES: usize = 16;
const LEN_STRIPES: usize = 64;
const WRITER_STRIPE_CLOSED: usize = 1 << (usize::BITS - 1);
const WRITER_STRIPE_DIRECT_BASE: usize = WRITER_STRIPE_CLOSED >> 1;
const WRITER_STRIPE_OVERLAY_BASE: usize = WRITER_STRIPE_DIRECT_BASE >> 1;
const WRITER_STRIPE_COUNT_MASK: usize = WRITER_STRIPE_OVERLAY_BASE - 1;

#[cfg(feature = "prepared-batch-gate")]
#[repr(align(64))]
struct PaddedAtomicUsize(AtomicUsize);

enum BaseMembershipFilter {
    Disabled,
    OneByte(Box<[u64]>),
}

impl BaseMembershipFilter {
    fn new(entries: usize, mode: AtomicGenerationBaseFilter) -> Self {
        match mode {
            AtomicGenerationBaseFilter::Disabled
            | AtomicGenerationBaseFilter::EmbeddedFingerprint => Self::Disabled,
            AtomicGenerationBaseFilter::OneBytePerEntry => {
                Self::OneByte(vec![0; entries.div_ceil(8)].into_boxed_slice())
            }
        }
    }

    fn insert_hash(&mut self, hash: u64) {
        let Self::OneByte(words) = self else {
            return;
        };
        let Some((word, mask)) = membership_location(hash, words.len()) else {
            return;
        };
        words[word] |= mask;
    }

    fn may_contain_hash(&self, hash: u64) -> bool {
        let Self::OneByte(words) = self else {
            return true;
        };
        let Some((word, mask)) = membership_location(hash, words.len()) else {
            return true;
        };
        words[word] & mask == mask
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Disabled => 0,
            Self::OneByte(words) => size_of_val(words.as_ref()),
        }
    }
}

fn membership_location(hash: u64, words: usize) -> Option<(usize, u64)> {
    if words == 0 {
        return None;
    }
    let upper = u128::try_from(words).expect("usize fits u128");
    let word = usize::try_from((u128::from(hash) * upper) >> 64)
        .expect("reduced membership hash is below word count");
    let first = u32::try_from(hash & 63).expect("six hash bits fit u32");
    let mut second = (hash >> 32) as u32 & 63;
    if second == first {
        second = (second + 1) & 63;
    }
    Some((word, (1_u64 << first) | (1_u64 << second)))
}

enum GenerationBase<V, C, B> {
    Frozen {
        map: Arc<B>,
        membership: BaseMembershipFilter,
    },
    StableHybrid(Arc<LockFreeHybridMap<V>>),
    Previous(Arc<GenerationLayer<V, C, B>>),
}

impl<V, C, B> GenerationBase<V, C, B>
where
    C: GenerationCell<V>,
    B: GenerationFrozen<V>,
{
    fn frozen(
        map: B,
        filter: AtomicGenerationBaseFilter,
        hash_builder: &GenerationHashBuilder,
    ) -> Self {
        let mut membership = BaseMembershipFilter::new(map.len(), filter);
        map.for_each_entry(&mut |key, _| {
            membership.insert_hash(GenerationKeyHash::new(hash_builder, key).route());
        });
        Self::Frozen {
            map: Arc::new(map),
            membership,
        }
    }

    fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        match self {
            Self::Frozen { map, .. } => map.with_value(key, read),
            Self::StableHybrid(map) => map.with_value(key, read),
            Self::Previous(map) => map.with_value(key, read),
        }
    }

    fn with_value_hashed<R>(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        read: impl FnOnce(Option<&V>) -> R,
    ) -> R {
        match self {
            Self::Frozen { map, .. } => map.with_value_hashed(key, key_hash.frozen(), read),
            Self::StableHybrid(map) => map.with_value(key, read),
            Self::Previous(map) => {
                let stripe_mask =
                    u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
                let stripe = usize::try_from(key_hash.route() & stripe_mask)
                    .expect("masked writer stripe fits usize");
                map.with_value_in_stripe(key, key_hash, stripe, read)
            }
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn prepare_slot(
        &self,
        key: &[u8],
        route_hash: u64,
        digest: Digest,
    ) -> Option<AtomicPreparedSlot> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(route_hash)
                .then(|| map.prepare_slot(key, digest))
                .flatten(),
            Self::StableHybrid(_) | Self::Previous(_) => None,
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn with_prepared_value<R>(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        read: impl FnOnce(Option<&V>) -> R,
    ) -> DirectMutation<R> {
        match self {
            Self::Frozen { map, .. } => map.with_prepared_value(key, prepared, read),
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn update_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        update: &impl Fn(&V) -> V,
    ) -> DirectMutation<Option<V>> {
        match self {
            Self::Frozen { map, .. } => map.update_prepared(key, prepared, update),
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn insert_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: &V,
    ) -> DirectMutation<InsertOutcome<V>> {
        match self {
            Self::Frozen { map, .. } => map.insert_prepared(key, prepared, value),
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn remove_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
    ) -> DirectMutation<Option<V>> {
        match self {
            Self::Frozen { map, .. } => map.remove_prepared(key, prepared),
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    fn with_present_value<R>(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        read: impl FnOnce(&V) -> R,
    ) -> Option<R> {
        match self {
            Self::Frozen { map, membership } => {
                if !membership.may_contain_hash(key_hash.route()) {
                    return None;
                }
                map.with_value_hashed(key, key_hash.frozen(), |value| value.map(read))
            }
            Self::StableHybrid(map) => map.with_value(key, |value| value.map(read)),
            Self::Previous(map) => map.with_value(key, |value| value.map(read)),
        }
    }

    fn get_cloned_hashed(&self, key: &[u8], key_hash: GenerationKeyHash) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value_hashed(key, key_hash, |value| value.cloned())
    }

    fn contains_key_hashed(&self, key: &[u8], key_hash: GenerationKeyHash) -> bool {
        self.with_value_hashed(key, key_hash, |value| value.is_some())
    }

    fn direct_insert(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        value: &V,
    ) -> Option<InsertOutcome<V>> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(key_hash.route())
                .then(|| map.direct_insert(key, key_hash.frozen(), value))
                .flatten(),
            Self::StableHybrid(_) | Self::Previous(_) => None,
        }
    }

    fn direct_insert_new(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        value: &V,
    ) -> Option<bool> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(key_hash.route())
                .then(|| map.direct_insert_new(key, key_hash.frozen(), value))
                .flatten(),
            Self::StableHybrid(_) | Self::Previous(_) => None,
        }
    }

    fn direct_update(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        update: &impl Fn(&V) -> V,
    ) -> DirectMutation<Option<V>> {
        match self {
            Self::Frozen { map, membership } => {
                if membership.may_contain_hash(key_hash.route()) {
                    map.direct_update(key, key_hash.frozen(), update)
                } else {
                    DirectMutation::NotMember
                }
            }
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    fn direct_upsert(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        insert_value: &V,
        update: &impl Fn(&V) -> V,
    ) -> Option<(V, bool)> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(key_hash.route())
                .then(|| map.direct_upsert(key, key_hash.frozen(), insert_value, update))
                .flatten(),
            Self::StableHybrid(_) | Self::Previous(_) => None,
        }
    }

    fn direct_remove_if(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        predicate: &impl Fn(&V) -> bool,
    ) -> DirectMutation<Option<V>> {
        match self {
            Self::Frozen { map, membership } => {
                if membership.may_contain_hash(key_hash.route()) {
                    map.direct_remove_if(key, key_hash.frozen(), predicate)
                } else {
                    DirectMutation::NotMember
                }
            }
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    fn for_each_entry_hashed(
        &self,
        hash_builder: &GenerationHashBuilder,
        visit: &mut dyn FnMut(&[u8], &V),
    ) {
        match self {
            Self::Frozen { map, .. } => map.for_each_entry(visit),
            Self::StableHybrid(map) => map.for_each_entry(visit),
            Self::Previous(map) => map.for_each_entry_hashed(hash_builder, visit),
        }
    }

    fn stats(&self) -> LayerBaseStats {
        match self {
            Self::Frozen { map, membership } => LayerBaseStats {
                frozen: map.stats(),
                overlay_present: 0,
                overlay_deleted: 0,
                layer_depth: 0,
                base_filter_bytes: membership.bytes(),
            },
            Self::StableHybrid(map) => {
                let stats = map.stats();
                LayerBaseStats {
                    frozen: stats.base,
                    overlay_present: stats.overlay_present,
                    overlay_deleted: stats.overlay_deleted,
                    layer_depth: 0,
                    base_filter_bytes: 0,
                }
            }
            Self::Previous(map) => {
                let stats = map.stats();
                LayerBaseStats {
                    frozen: stats.current.base,
                    overlay_present: stats.current.overlay_present,
                    overlay_deleted: stats.current.overlay_deleted,
                    layer_depth: stats.layer_depth,
                    base_filter_bytes: stats.base_filter_bytes,
                }
            }
        }
    }
}

struct LayerBaseStats {
    frozen: FrozenMapStats,
    overlay_present: usize,
    overlay_deleted: usize,
    layer_depth: usize,
    base_filter_bytes: usize,
}

struct LayerStats {
    current: LockFreeHybridStats,
    layer_depth: usize,
    base_filter_bytes: usize,
}

/// Successful immutable-generation publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationRebuild {
    /// Generation replaced by the rebuild.
    pub from_generation: u64,
    /// Newly packed generation.
    pub to_generation: u64,
    /// Logical entries packed into the new base.
    pub entries: usize,
    /// Overlay records compacted from all previous layers.
    pub compacted_overlay_records: usize,
    /// Layer depth immediately before writer redirection.
    pub previous_layer_depth: usize,
    /// Time to publish the new layer and atomically close predecessor stripes.
    ///
    /// Point writers continue against each open predecessor stripe and move to
    /// the new layer after their stripe closes.
    pub writer_redirect: Duration,
    /// Packed/perfect-hash build time during which readers and writers ran.
    pub background_build: Duration,
    /// Time to replace the equivalent previous-generation base with packed data.
    pub base_publish: Duration,
}

/// Observable state of a [`LockFreeGenerationMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GenerationMapStats {
    /// Number of successfully packed generations.
    pub generation: u64,
    /// Aggregate packed-base and overlay populations across active layers.
    pub current: LockFreeHybridStats,
    /// Active lookup depth, including the writable top layer.
    pub layer_depth: usize,
    /// Writers currently pinned across the active striped handoff route.
    pub active_writers: usize,
    /// Bytes retained by definite-negative filters across active frozen bases.
    pub base_filter_bytes: usize,
}
