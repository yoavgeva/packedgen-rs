#[cfg(feature = "prepared-keys")]
use std::cell::Cell;
use std::cell::RefCell;
use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::cache::Cache as ArcSwapCache;
use arc_swap::{ArcSwap, ArcSwapOption, Guard};
use parking_lot::Mutex;

#[cfg(feature = "prepared-keys")]
use crate::atomic_value::AtomicPreparedSlot;
use crate::atomic_value::{
    AtomicFrozenProbe, AtomicU64Cell, AtomicU64FrozenMap, DirectMutation, NonMaxU64,
};
use crate::frozen_map::Digest;
#[cfg(feature = "prepared-keys")]
use crate::frozen_map::key_digest;
use crate::generation_hash::{GenerationHashBuilder, GenerationKeyHash};
#[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
use crate::generation_overlay::AtomicEncodedAdmissionKey;
#[cfg(feature = "prepared-keys")]
use crate::generation_overlay::AtomicOverlayPreparedSlot;
use crate::generation_overlay::{
    AdaptiveOverlaySnapshot, GenerationOverlay, GenerationOverlayMode, OverlayInsert,
};
use crate::overlay_cell::{GenerationCell, OverlayCell, StableCell};
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

    fn sample_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V) -> bool) -> bool;

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
    fn replace_prepared(
        &self,
        _key: &[u8],
        _prepared: AtomicPreparedSlot,
        _value: &V,
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

    fn sample_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V) -> bool) -> bool {
        if self.is_empty() {
            return false;
        }
        let slot =
            usize::try_from(seed % self.len() as u64).expect("frozen sample slot fits usize");
        let (key, value) = self
            .indexed_entry(slot)
            .expect("sample slot is below frozen length");
        visit(key, value)
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

    fn sample_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &NonMaxU64) -> bool) -> bool {
        self.sample_entry(seed, visit)
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
    fn replace_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: &NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        self.replace_prepared(key, prepared, *value)
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

pub(crate) struct AtomicReadGuard<'map> {
    map: &'map LockFreeAtomicU64GenerationMap,
    generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    stable_base: Option<Arc<GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>>,
}

type AtomicGenerationLayer = GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>;
type AtomicGenerationBase = GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>;
type AtomicCurrentCache<'map> =
    ArcSwapCache<&'map ArcSwap<AtomicGenerationLayer>, Arc<AtomicGenerationLayer>>;

struct AtomicReadCacheState<'map> {
    current: AtomicCurrentCache<'map>,
    base_generation: Arc<AtomicGenerationLayer>,
    base: Arc<AtomicGenerationBase>,
}

pub(crate) struct AtomicReadCache<'map> {
    map: &'map LockFreeAtomicU64GenerationMap,
    state: RefCell<AtomicReadCacheState<'map>>,
}

impl AtomicReadCache<'_> {
    pub(crate) fn get_protected(&self, key: &[u8]) -> Option<NonMaxU64> {
        #[cfg(feature = "shared-gx")]
        let key_hash = GenerationKeyHash::new(&self.map.inner.writer_hash_builder, key);
        #[cfg(feature = "shared-gx")]
        let route_hash = key_hash.route();
        #[cfg(not(feature = "shared-gx"))]
        let route_hash = GenerationKeyHash::route_for(&self.map.inner.writer_hash_builder, key);
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        let mut state = self.state.borrow_mut();
        let AtomicReadCacheState {
            current,
            base_generation,
            base,
        } = &mut *state;
        let generation = current.load();
        if !Arc::ptr_eq(base_generation, generation) {
            *base_generation = Arc::clone(generation);
            *base = generation.base.load_full();
        } else if matches!(base.as_ref(), GenerationBase::Previous(_))
            && generation.writer_stripes[stripe].load(Ordering::Acquire) & WRITER_STRIPE_DIRECT_BASE
                != 0
        {
            // A generation changes its base at most once: an immutable
            // predecessor is replaced by an equivalent frozen map before any
            // stripe can enable direct frozen-cell mutation. Observing that
            // enable bit therefore proves the cached predecessor must be
            // refreshed before this read can observe later direct updates.
            *base = generation.base.load_full();
        }
        #[cfg(feature = "shared-gx")]
        {
            generation.get_atomic_protected_with_base(base, key, key_hash, stripe)
        }
        #[cfg(not(feature = "shared-gx"))]
        generation.get_atomic_protected_with_base_route(
            base,
            key,
            route_hash,
            stripe,
            &self.map.inner.writer_hash_builder,
        )
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn get_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
    ) -> Option<NonMaxU64> {
        let prepared = *prepared;
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let prepared_stripe = usize::try_from(prepared.route_hash() & stripe_mask)
            .expect("masked writer stripe fits usize");
        let mut cache = self.state.borrow_mut();
        let AtomicReadCacheState {
            current,
            base_generation,
            base,
        } = &mut *cache;
        let generation = current.load();
        if !Arc::ptr_eq(base_generation, generation) {
            *base_generation = Arc::clone(generation);
            *base = generation.base.load_full();
        }

        if let Some(slot) = prepared.prepared_overlay_slot()
            && let Some(value) =
                generation
                    .overlay
                    .with_prepared_atomic(key, slot, AtomicU64Cell::get_protected)
        {
            return value;
        }

        let prepared_stripe_state =
            generation.writer_stripes[prepared_stripe].load(Ordering::Acquire);
        if matches!(base.as_ref(), GenerationBase::Previous(_))
            && prepared_stripe_state & WRITER_STRIPE_DIRECT_BASE != 0
        {
            *base = generation.base.load_full();
        }
        if let Some(slot) = prepared.prepared_slot()
            && prepared_stripe_state & WRITER_STRIPE_DIRECT_BASE != 0
            && prepared_stripe_state & WRITER_STRIPE_OVERLAY_BASE == 0
            && let DirectMutation::Handled(value) =
                base.with_prepared_value(key, slot, copy_optional_non_max)
        {
            return value;
        }

        #[cfg(feature = "shared-gx")]
        let key_hash = GenerationKeyHash::new(&self.map.inner.writer_hash_builder, key);
        #[cfg(feature = "shared-gx")]
        let route_hash = key_hash.route();
        #[cfg(not(feature = "shared-gx"))]
        let route_hash = GenerationKeyHash::route_for(&self.map.inner.writer_hash_builder, key);
        let key_stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        if key_stripe != prepared_stripe
            && matches!(base.as_ref(), GenerationBase::Previous(_))
            && generation.writer_stripes[key_stripe].load(Ordering::Acquire)
                & WRITER_STRIPE_DIRECT_BASE
                != 0
        {
            *base = generation.base.load_full();
        }
        #[cfg(feature = "shared-gx")]
        {
            generation.get_atomic_protected_with_base(base, key, key_hash, key_stripe)
        }
        #[cfg(not(feature = "shared-gx"))]
        generation.get_atomic_protected_with_base_route(
            base,
            key,
            route_hash,
            key_stripe,
            &self.map.inner.writer_hash_builder,
        )
    }

    pub(crate) fn refresh(&self) {
        let mut state = self.state.borrow_mut();
        let AtomicReadCacheState {
            current,
            base_generation,
            base,
        } = &mut *state;
        let generation = current.load();
        *base_generation = Arc::clone(generation);
        *base = generation.base.load_full();
    }
}

impl AtomicReadGuard<'_> {
    #[cfg(test)]
    pub(crate) fn get_protected(&self, key: &[u8]) -> Option<NonMaxU64> {
        let (stripe, key_hash) = self.map.inner.writer_route(key);
        self.stable_base.as_ref().map_or_else(
            || self.generation.get_atomic_protected(key, key_hash, stripe),
            |base| {
                self.generation
                    .get_atomic_protected_with_base(base, key, key_hash, stripe)
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn refresh(&mut self) {
        *self = self.map.read_guard();
    }

    pub(crate) fn get_or_insert(&self, key: &[u8], value: NonMaxU64) -> NonMaxU64 {
        let (current, deferred_stripe) = self.get_or_insert_deferred_len(key, value);
        if let Some(stripe) = deferred_stripe {
            self.generation.adjust_len(stripe, 1);
        }
        current
    }

    pub(crate) fn get_or_insert_deferred_len(
        &self,
        key: &[u8],
        value: NonMaxU64,
    ) -> (NonMaxU64, Option<usize>) {
        if self.stable_base.is_none() {
            return (self.map.get_or_insert(key, value), None);
        }
        #[cfg(not(feature = "shared-gx"))]
        if self.generation.base_is_empty {
            // This stable generation cannot consume the independent frozen
            // digest, so compute only the mutable writer route.
            return self.get_or_insert_deferred_len_route_only(key, value);
        }
        let (stripe, key_hash) = self.map.inner.writer_route(key);
        // This guard owns one `read_batches` reservation. Rebuild closes and
        // drains those reservations before it can close writer stripes, so a
        // stable guarded write does not need a second per-operation writer
        // reservation merely to keep this generation open.
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
        let route = GenerationWriteRoute {
            stripe,
            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
            key_hash,
        };
        let (current, inserted) = self.generation.get_or_insert_atomic_with_base(
            self.stable_base
                .as_ref()
                .expect("stable guarded writes retain a base"),
            key,
            value,
            route,
        );
        (current, inserted.then_some(stripe))
    }

    #[cfg(not(feature = "shared-gx"))]
    pub(crate) fn uses_route_only_admission(&self) -> bool {
        self.stable_base.is_some() && self.generation.base_is_empty
    }

    #[cfg(not(feature = "shared-gx"))]
    pub(crate) fn get_or_insert_deferred_len_route_only(
        &self,
        key: &[u8],
        value: NonMaxU64,
    ) -> (NonMaxU64, Option<usize>) {
        debug_assert!(self.uses_route_only_admission());
        // A mutable-only generation needs just the routing lane used by the
        // writer stripe and overlay. Defer the independent frozen-map digest
        // lane until a generation actually has a frozen base.
        let route_hash = GenerationKeyHash::route_for(&self.map.inner.writer_hash_builder, key);
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
        let (current, inserted) = self
            .generation
            .get_or_insert_atomic_empty_base(key, value, route_hash);
        (current, inserted.then_some(stripe))
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    pub(crate) fn get_or_insert_deferred_len_route_only_encoded(
        &self,
        key: &[u8],
        value: NonMaxU64,
    ) -> (NonMaxU64, Option<usize>) {
        debug_assert!(self.uses_route_only_admission());
        let route_hash = GenerationKeyHash::route_for(&self.map.inner.writer_hash_builder, key);
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe =
            usize::try_from(route_hash & stripe_mask).expect("masked writer stripe fits usize");
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
        let encoded = AtomicEncodedAdmissionKey::prepare(key)
            .expect("encoded bulk admission receives a non-boundary short key");
        let (current, inserted) = self
            .generation
            .get_or_insert_atomic_empty_base_encoded(key, &encoded, value, route_hash);
        (current, inserted.then_some(stripe))
    }

    pub(crate) fn flush_deferred_insert_len(&self, stripe: usize, amount: u16) {
        debug_assert!(self.stable_base.is_some());
        self.generation.adjust_len(
            stripe,
            isize::try_from(amount).expect("deferred insert count fits isize"),
        );
    }

    #[cfg(test)]
    pub(crate) fn remove(&self, key: &[u8]) -> Option<NonMaxU64> {
        let (removed, deferred_stripe) = self.remove_deferred_len(key);
        if let Some(stripe) = deferred_stripe {
            self.generation.adjust_len(stripe, -1);
        }
        removed
    }

    pub(crate) fn remove_deferred_len(&self, key: &[u8]) -> (Option<NonMaxU64>, Option<usize>) {
        if self.stable_base.is_none() {
            return (self.map.remove(key), None);
        }
        let (stripe, key_hash) = self.map.inner.writer_route(key);
        // As with guarded conditional admission, the read-batch reservation
        // keeps this generation and its writer stripes open through removal.
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
        let route = GenerationWriteRoute {
            stripe,
            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
            key_hash,
        };
        let removed = self.generation.remove_atomic_with_base(
            self.stable_base
                .as_ref()
                .expect("stable guarded writes retain a base"),
            key,
            route,
        );
        let deferred_stripe = removed.map(|_| stripe);
        (removed, deferred_stripe)
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn remove_prepared_deferred_len(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
    ) -> (Option<NonMaxU64>, Option<usize>) {
        if self.stable_base.is_none() {
            return (self.map.remove_prepared(key, prepared), None);
        }
        let Some(slot) = prepared.prepared_slot() else {
            return self.remove_deferred_len(key);
        };
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = usize::try_from(prepared.route_hash() & stripe_mask)
            .expect("masked writer stripe fits usize");
        let state = self.generation.writer_stripes[stripe].load(Ordering::Acquire);
        debug_assert_eq!(state & WRITER_STRIPE_CLOSED, 0);
        if state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0 {
            match self
                .stable_base
                .as_ref()
                .expect("stable guarded writes retain a base")
                .remove_prepared(key, slot)
            {
                DirectMutation::Handled(removed) => {
                    let deferred_stripe = removed.map(|_| stripe);
                    return (removed, deferred_stripe);
                }
                DirectMutation::NotMember => {}
            }
        }
        self.remove_deferred_len(key)
    }

    pub(crate) fn flush_deferred_remove_len(&self, stripe: usize, amount: u16) {
        debug_assert!(self.stable_base.is_some());
        self.generation.adjust_len(
            stripe,
            -isize::try_from(amount).expect("deferred removal count fits isize"),
        );
    }
}

impl Drop for AtomicReadGuard<'_> {
    fn drop(&mut self) {
        let previous = self.generation.read_batches.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

/// Reusable writer-generation guard for a sequence of atomic map operations.
///
/// The guard amortizes generation handoff protection across many keys. Keep it
/// scoped to one request or worker batch: a live guard can delay an online
/// rebuild from closing its generation, in the same way that a retained vacant
/// entry delays one writer stripe.
#[cfg(feature = "prepared-batch-gate")]
#[must_use]
pub struct AtomicOperationGuard<'map> {
    map: &'map LockFreeAtomicU64GenerationMap,
    writer: AtomicPreparedBatchWriter,
}

#[cfg(feature = "prepared-batch-gate")]
impl AtomicOperationGuard<'_> {
    /// Reads through the pinned generation.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.map.inner.get_in_batch(&self.writer, key)
    }

    /// Inserts or replaces a value without a per-call writer pin.
    #[must_use]
    pub fn insert(&self, key: &[u8], value: NonMaxU64) -> InsertOutcome<NonMaxU64> {
        self.map.inner.insert_in_batch(&self.writer, key, value)
    }

    /// Inserts only while the logical key is absent.
    #[must_use]
    pub fn insert_new(&self, key: &[u8], value: NonMaxU64) -> bool {
        self.map.inner.insert_new_in_batch(&self.writer, key, value)
    }

    /// Returns the existing value or inserts `value` through the pinned
    /// generation without pinning and releasing a writer stripe per call.
    #[must_use]
    pub fn get_or_insert(&self, key: &[u8], value: NonMaxU64) -> NonMaxU64 {
        self.map
            .inner
            .get_or_insert_in_batch(&self.writer, key, value)
    }

    /// Atomically transforms an existing value without a per-call writer pin.
    pub fn update(
        &self,
        key: &[u8],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<NonMaxU64> {
        self.map.inner.update_in_batch(&self.writer, key, update)
    }

    /// Atomically updates an existing value or inserts `insert_value`.
    pub fn upsert(
        &self,
        key: &[u8],
        insert_value: NonMaxU64,
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> NonMaxU64 {
        self.map
            .inner
            .upsert_in_batch(&self.writer, key, insert_value, update)
    }

    /// Removes and returns the latest logical value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.remove_if(key, |_| true)
    }

    /// Removes a value accepted by a retry-safe predicate.
    #[must_use]
    pub fn remove_if(
        &self,
        key: &[u8],
        predicate: impl Fn(&NonMaxU64) -> bool,
    ) -> Option<NonMaxU64> {
        self.map
            .inner
            .remove_if_in_batch(&self.writer, key, predicate)
    }
}

/// Result of one exact atomic-map entry probe.
///
/// Unlike a lock-based entry guard, an occupied value is a snapshot and may be
/// changed immediately by another writer. A vacant handle pins only the key's
/// writer stripe until it is consumed or dropped.
#[must_use]
pub enum AtomicEntry<'map, 'key> {
    /// The key was live when probed.
    Occupied(NonMaxU64),
    /// The key was absent and can be inserted without hashing or probing the
    /// frozen base a second time.
    Vacant(AtomicVacantEntry<'map, 'key>),
}

/// One-shot proof that an atomic-map key was absent during an exact probe.
///
/// The handle carries the precomputed route and keeps the relevant writer
/// stripe open across a concurrent rebuild. Dropping it without inserting is
/// harmless. Handles should be short-lived because a retained handle can delay
/// rebuild progress for its stripe.
#[must_use]
pub struct AtomicVacantEntry<'map, 'key> {
    _map: &'map LockFreeAtomicU64GenerationMap,
    key: &'key [u8],
    writer: GenerationWriter<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    target: AtomicVacantTarget,
}

enum AtomicVacantTarget {
    FrozenSlot {
        map: Arc<AtomicU64FrozenMap>,
        slot: usize,
    },
    Overlay {
        mark_base_shadow: bool,
    },
}

enum AtomicEntryProbe {
    Occupied(NonMaxU64),
    Vacant(AtomicVacantTarget),
}

enum AtomicBaseProbe {
    NotMember,
    Member {
        map: Arc<AtomicU64FrozenMap>,
        slot: usize,
        value: Option<NonMaxU64>,
    },
    Logical(Option<NonMaxU64>),
}

enum AtomicOverlayProbe {
    NotMember,
    Member(Option<NonMaxU64>),
}

impl AtomicVacantEntry<'_, '_> {
    /// Returns the exact key bytes associated with this vacancy proof.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        self.key
    }

    /// Inserts or replaces after the earlier miss without repeating its hash
    /// or frozen-base lookup.
    ///
    /// Another writer can win the key between the probe and this call. In that
    /// case this operation replaces its value and returns
    /// [`InsertOutcome::Replaced`].
    #[must_use]
    pub fn insert(self, value: NonMaxU64) -> InsertOutcome<NonMaxU64> {
        self.writer.insert_vacant(self.key, value, self.target)
    }

    /// Inserts only if the key is still absent.
    ///
    /// Returns `false` when another writer made the key live after the entry
    /// probe. The carried hash and frozen-base proof are still reused.
    #[must_use]
    pub fn insert_new(self, value: NonMaxU64) -> bool {
        self.writer.insert_new_vacant(self.key, value, self.target)
    }
}

/// Prepared routing and native-slot metadata for one repeatedly accessed key.
///
/// The handle never weakens exact key semantics. Callers still supply the key
/// bytes, which are verified before a frozen or native-overlay slot is used.
/// Rebuilds and overlay-shadowed frozen stripes automatically fall back to the
/// ordinary lookup.
#[cfg(feature = "prepared-keys")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AtomicPreparedKey {
    owner_id: u64,
    route_and_slot: u64,
}

#[cfg(feature = "prepared-keys")]
impl AtomicPreparedKey {
    const STRIPE_BITS: u32 = WRITER_STRIPES.trailing_zeros();
    const TARGET_BITS: u32 = 3;
    const SLOT_BITS: u32 = u64::BITS - Self::STRIPE_BITS - Self::TARGET_BITS;
    const SLOT_MASK: u64 = (1_u64 << Self::SLOT_BITS) - 1;
    const TARGET_MASK: u64 = (1_u64 << Self::TARGET_BITS) - 1;
    const STRIPE_SHIFT: u32 = Self::SLOT_BITS + Self::TARGET_BITS;

    /// Creates an exact ordinary-lookup marker for a mixed prepared batch.
    ///
    /// This is useful when only part of a caller-owned batch has prepared
    /// handles. Operations using this marker skip the direct-slot attempt and
    /// perform a normal exact lookup.
    #[must_use]
    pub const fn fallback() -> Self {
        Self {
            owner_id: 0,
            route_and_slot: 0,
        }
    }

    fn new(route_hash: u64, prepared: Option<AtomicPreparedSlot>) -> Self {
        let Some(prepared) = prepared else {
            return Self::fallback();
        };
        let Ok(slot) = u64::try_from(prepared.slot) else {
            return Self::fallback();
        };
        if prepared.base_id == 0 || slot > Self::SLOT_MASK {
            return Self::fallback();
        }
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = route_hash & stripe_mask;
        Self {
            owner_id: prepared.base_id,
            route_and_slot: stripe << Self::STRIPE_SHIFT | slot,
        }
    }

    fn new_overlay(route_hash: u64, prepared: AtomicOverlayPreparedSlot) -> Self {
        let Ok(slot) = u64::try_from(prepared.slot) else {
            return Self::fallback();
        };
        if prepared.owner_id == 0 || slot > Self::SLOT_MASK {
            return Self::fallback();
        }
        let target = u64::from(prepared.class) + 1;
        if target > Self::TARGET_MASK {
            return Self::fallback();
        }
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = route_hash & stripe_mask;
        Self {
            owner_id: prepared.owner_id,
            route_and_slot: stripe << Self::STRIPE_SHIFT | target << Self::SLOT_BITS | slot,
        }
    }

    const fn target(self) -> u64 {
        self.route_and_slot >> Self::SLOT_BITS & Self::TARGET_MASK
    }

    fn prepared_slot(self) -> Option<AtomicPreparedSlot> {
        (self.owner_id != 0 && self.target() == 0).then(|| AtomicPreparedSlot {
            base_id: self.owner_id,
            slot: usize::try_from(self.route_and_slot & Self::SLOT_MASK)
                .expect("prepared slot originated as usize"),
        })
    }

    #[inline]
    fn prepared_overlay_slot(self) -> Option<AtomicOverlayPreparedSlot> {
        let target = self.target();
        (self.owner_id != 0 && target != 0).then(|| AtomicOverlayPreparedSlot {
            owner_id: self.owner_id,
            class: u8::try_from(target - 1).expect("prepared overlay class fits u8"),
            slot: usize::try_from(self.route_and_slot & Self::SLOT_MASK)
                .expect("prepared slot originated as usize"),
        })
    }

    fn route_hash(self) -> u64 {
        self.route_and_slot >> Self::STRIPE_SHIFT
    }

    /// Returns whether preparation captured a candidate direct frozen slot.
    ///
    /// Validity is checked on every operation because rebuilds can make a
    /// previously captured slot stale. A `false` handle remains safe to use,
    /// but takes the ordinary lookup path until prepared again.
    #[must_use]
    pub const fn has_direct_slot(self) -> bool {
        self.owner_id != 0 && self.target() == 0
    }

    /// Returns whether preparation captured either a frozen or native-overlay slot.
    ///
    /// Native overlay slots accelerate reads but intentionally remain outside
    /// [`Self::has_direct_slot`], which continues to identify slots that also
    /// support the frozen-base prepared mutation path.
    #[must_use]
    pub const fn has_native_slot(self) -> bool {
        self.owner_id != 0
    }
}

/// Mutable-overlay implementation used by [`LockFreeAtomicU64GenerationMap`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicGenerationOverlay {
    /// Elastic allocation-free atomic slots for 32-byte keys with a lazily
    /// published packed overflow tier and residual dynamic Papaya fallback.
    AtomicFixed32,
    /// Elastic allocation-free atomic slots for variable keys from zero
    /// through 8 bytes. This uses one atomic key word per slot and is the
    /// densest choice for integer-sized cache keys.
    AtomicUpTo8,
    /// Elastic allocation-free atomic slots for variable keys from zero
    /// through 16 bytes. This uses two atomic key words per slot and is the
    /// compact choice for small cache keys.
    AtomicUpTo16,
    /// Learns the observed key-length distribution before publishing
    /// proportionally sized 8, 16, 24, and 32-byte atomic tables plus an exact
    /// inline 48-byte concurrent table. Other widths remain exact in the
    /// dynamic fallback.
    AtomicAdaptive,
    /// Elastic allocation-free atomic slots for variable keys from zero
    /// through 32 bytes. Short-key length is encoded into the 32nd key byte;
    /// disjoint control tags distinguish short encodings from full-width keys,
    /// so this has the same slot size and exact semantics as `AtomicFixed32`.
    AtomicUpTo32,
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

/// Lifecycle state of an adaptive mutable overlay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdaptiveOverlayPhase {
    /// Exact keys are still being sampled in the dynamic fallback.
    Sampling,
    /// The learned atomic key-class tables have been published.
    Ready,
}

/// Thresholds used to recommend adaptive-generation maintenance.
///
/// Ratios use basis points: `10_000` is 100%, `1_500` is 15%.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveRebuildPolicy {
    /// Recommend rebuild after this share of non-reclaimable overlay slots has
    /// been consumed.
    pub max_slot_utilization_bps: u16,
    /// Minimum learned insertions before drift and spill checks activate.
    pub min_learned_insertions: usize,
    /// Maximum total-variation distance from the sampled key-length mix.
    pub max_distribution_drift_bps: u16,
    /// Maximum share of learned short keys that spilled into the dynamic
    /// fallback after their planned atomic class filled.
    pub max_short_key_spill_bps: u16,
}

impl Default for AdaptiveRebuildPolicy {
    fn default() -> Self {
        Self {
            max_slot_utilization_bps: 7_500,
            min_learned_insertions: 256,
            max_distribution_drift_bps: 1_500,
            max_short_key_spill_bps: 500,
        }
    }
}

/// Reasons an adaptive generation should be rebuilt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdaptiveRebuildRecommendation {
    /// Non-reclaimable overlay slots crossed the configured utilization.
    pub capacity_pressure: bool,
    /// Learned key lengths moved too far from the initial sample.
    pub distribution_drift: bool,
    /// Too many short keys spilled out of their packed atomic class.
    pub short_key_spill: bool,
}

impl AdaptiveRebuildRecommendation {
    /// Returns whether any maintenance reason is active.
    #[must_use]
    pub const fn is_recommended(self) -> bool {
        self.capacity_pressure || self.distribution_drift || self.short_key_spill
    }
}

/// Maintenance snapshot describing the current adaptive mutable overlay.
///
/// Key-class arrays use `0..=8`, `9..=16`, `17..=24`, `25..=32`, exact
/// `48`, and residual-width buckets. Planned inline capacities omit the final
/// dynamic bucket. The snapshot scans immutable key metadata, so collecting
/// it is proportional to occupied overlay slots but adds no work to foreground
/// mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveOverlayStats {
    /// Sampling or learned-table state.
    pub phase: AdaptiveOverlayPhase,
    /// Configured physical-record budget for this generation.
    pub capacity: usize,
    /// Exact insertion sample requested before learning.
    pub sample_target: usize,
    /// Unique physical records observed during sampling.
    pub sampled_records: usize,
    /// Sample key-length counts in the six documented buckets.
    pub sample_key_classes: [usize; 6],
    /// Unique physical records inserted after table publication.
    pub learned_insertions: usize,
    /// Learned key-length counts in the six documented buckets.
    pub learned_key_classes: [usize; 6],
    /// Slot budgets learned for four packed atomic classes and exact-width 48.
    pub planned_atomic_capacities: [usize; 5],
    /// Learned inline keys forced into the dynamic fallback by class pressure.
    pub short_key_fallback_insertions: usize,
    /// Sampling plus learned physical records; deletes do not reduce this.
    pub occupied_slots: usize,
    /// Occupied slots divided by capacity, in basis points.
    pub slot_utilization_bps: u16,
    /// Total-variation distance between sampled and learned distributions.
    pub distribution_drift_bps: u16,
    /// Inline-key fallback insertions divided by learned inline-key insertions.
    pub short_key_spill_bps: u16,
}

impl AdaptiveOverlayStats {
    /// Evaluates this snapshot against a maintenance policy.
    #[must_use]
    pub fn recommendation(self, policy: AdaptiveRebuildPolicy) -> AdaptiveRebuildRecommendation {
        let enough_observations = self.learned_insertions >= policy.min_learned_insertions;
        AdaptiveRebuildRecommendation {
            capacity_pressure: self.slot_utilization_bps >= policy.max_slot_utilization_bps,
            distribution_drift: enough_observations
                && self.distribution_drift_bps >= policy.max_distribution_drift_bps,
            short_key_spill: enough_observations
                && self.short_key_spill_bps >= policy.max_short_key_spill_bps,
        }
    }
}

impl AdaptiveOverlayStats {
    fn from_snapshot(snapshot: AdaptiveOverlaySnapshot) -> Self {
        let occupied_slots = snapshot.sampled.saturating_add(snapshot.learned_insertions);
        let learned_short = snapshot.learned_key_classes[..5]
            .iter()
            .copied()
            .sum::<usize>();
        Self {
            phase: if snapshot.ready {
                AdaptiveOverlayPhase::Ready
            } else {
                AdaptiveOverlayPhase::Sampling
            },
            capacity: snapshot.capacity,
            sample_target: snapshot.sample_target,
            sampled_records: snapshot.sampled,
            sample_key_classes: snapshot.sample_key_classes,
            learned_insertions: snapshot.learned_insertions,
            learned_key_classes: snapshot.learned_key_classes,
            planned_atomic_capacities: snapshot.planned_atomic_capacities,
            short_key_fallback_insertions: snapshot.short_key_fallback_insertions,
            occupied_slots,
            slot_utilization_bps: ratio_bps(occupied_slots, snapshot.capacity),
            distribution_drift_bps: distribution_drift_bps(
                snapshot.sample_key_classes,
                snapshot.learned_key_classes,
            ),
            short_key_spill_bps: ratio_bps(snapshot.short_key_fallback_insertions, learned_short),
        }
    }
}

fn ratio_bps(numerator: usize, denominator: usize) -> u16 {
    if denominator == 0 {
        return 0;
    }
    let scaled = (numerator as u128)
        .saturating_mul(10_000)
        .checked_div(denominator as u128)
        .unwrap_or(0)
        .min(u128::from(u16::MAX));
    u16::try_from(scaled).unwrap_or(u16::MAX)
}

fn distribution_drift_bps<const CLASSES: usize>(
    sample: [usize; CLASSES],
    learned: [usize; CLASSES],
) -> u16 {
    let sample_total = sample.iter().copied().sum::<usize>();
    let learned_total = learned.iter().copied().sum::<usize>();
    if sample_total == 0 || learned_total == 0 {
        return 0;
    }
    let denominator = (sample_total as u128).saturating_mul(learned_total as u128);
    let distance =
        sample
            .into_iter()
            .zip(learned)
            .fold(0_u128, |sum, (sample_count, learned_count)| {
                let sampled = (sample_count as u128).saturating_mul(learned_total as u128);
                let observed = (learned_count as u128).saturating_mul(sample_total as u128);
                sum.saturating_add(sampled.abs_diff(observed))
            });
    let basis_points = distance
        .saturating_mul(5_000)
        .checked_div(denominator)
        .unwrap_or(0)
        .min(u128::from(u16::MAX));
    u16::try_from(basis_points).unwrap_or(u16::MAX)
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
            AtomicGenerationOverlay::AtomicUpTo8 => Self::AtomicUpTo8,
            AtomicGenerationOverlay::AtomicUpTo16 => Self::AtomicUpTo16,
            AtomicGenerationOverlay::AtomicAdaptive => Self::AtomicAdaptive,
            AtomicGenerationOverlay::AtomicUpTo32 => Self::AtomicUpTo32,
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
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.inner.get_cloned(key)
    }

    /// Loads a pointer-class value with the ordering required by an external
    /// epoch collector whose guard was entered before this call.
    #[doc(hidden)]
    #[must_use]
    pub fn get_protected(&self, key: &[u8]) -> Option<NonMaxU64> {
        self.inner.get_atomic_protected(key)
    }

    pub(crate) fn read_guard(&self) -> AtomicReadGuard<'_> {
        loop {
            let generation = self.inner.current.load();
            if try_acquire_writer_stripe(&generation.read_batches).is_some() {
                let base = generation.base.load_full();
                let stable_base =
                    (!matches!(base.as_ref(), GenerationBase::Previous(_))).then_some(base);
                return AtomicReadGuard {
                    map: self,
                    generation: PinnedGeneration::Guard(generation),
                    stable_base,
                };
            }
            spin_loop();
        }
    }

    pub(crate) fn read_cache(&self) -> AtomicReadCache<'_> {
        let mut current = ArcSwapCache::new(&self.inner.current);
        let base_generation = Arc::clone(current.load());
        let base = base_generation.base.load_full();
        AtomicReadCache {
            map: self,
            state: RefCell::new(AtomicReadCacheState {
                current,
                base_generation,
                base,
            }),
        }
    }

    /// Probes a key once and returns either its current value or a one-shot
    /// vacant handle for a following insertion.
    ///
    /// This is intended for cache-style miss-then-insert flows. The vacant
    /// path reuses the route hash and exact absence proof, while retaining the
    /// same lock-free race semantics as [`Self::insert`] and
    /// [`Self::insert_new`].
    pub fn entry<'map, 'key>(&'map self, key: &'key [u8]) -> AtomicEntry<'map, 'key> {
        let writer = self.inner.pin_writer(key);
        match writer.probe_entry(key) {
            AtomicEntryProbe::Occupied(value) => AtomicEntry::Occupied(value),
            AtomicEntryProbe::Vacant(target) => AtomicEntry::Vacant(AtomicVacantEntry {
                _map: self,
                key,
                writer,
                target,
            }),
        }
    }

    /// Returns the existing value or atomically inserts `value` when absent.
    ///
    /// This is the fused cache-miss path: it hashes and probes the key once,
    /// pins one writer stripe, and publishes both insertion and length change
    /// through that writer. If another writer wins the race, its value is
    /// returned without replacing it.
    #[must_use]
    pub fn get_or_insert(&self, key: &[u8], value: NonMaxU64) -> NonMaxU64 {
        self.inner.pin_writer(key).get_or_insert(key, value)
    }

    /// Pins one generation for a short sequence of atomic operations.
    ///
    /// This adds 16 cache-line-separated generation gates (about 1 KiB per
    /// live generation) through the `prepared-batch-gate` feature. Reusing one
    /// guard removes the per-operation writer pin/release pair; successful
    /// insertions still publish exact length changes.
    #[cfg(feature = "prepared-batch-gate")]
    pub fn operation_guard(&self) -> AtomicOperationGuard<'_> {
        AtomicOperationGuard {
            map: self,
            writer: self.inner.pin_prepared_batch_writer(0),
        }
    }

    /// Performs a conditional-insert batch under one short-lived operation
    /// guard.
    ///
    /// The three slices must have equal lengths. `results` receives the
    /// existing or newly inserted value for each key.
    ///
    /// # Panics
    ///
    /// Panics when `keys`, `values`, and `results` have different lengths.
    #[cfg(feature = "prepared-batch-gate")]
    pub fn get_or_insert_batch<K>(
        &self,
        keys: &[K],
        values: &[NonMaxU64],
        results: &mut [NonMaxU64],
    ) where
        K: AsRef<[u8]>,
    {
        assert_eq!(keys.len(), values.len(), "operation batch value mismatch");
        assert_eq!(keys.len(), results.len(), "operation batch output mismatch");
        if keys.is_empty() {
            return;
        }
        let guard = self.operation_guard();
        for ((key, value), result) in keys.iter().zip(values).zip(results) {
            *result = guard.get_or_insert(key.as_ref(), *value);
        }
    }

    /// Prepares a compact exact handle for repeated reads of `key`.
    ///
    /// Preparation performs the normal routing and captures an exact frozen or
    /// native-overlay slot when one is stable. The returned handle remains safe
    /// across mutations and rebuilds; stale slot metadata simply falls back to
    /// [`Self::get`]. Native-overlay slots currently accelerate reads, while
    /// prepared mutations continue through the ordinary exact writer route.
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
    /// Invalid, stale, cross-map, or wrong-key handles retain exact semantics
    /// by taking the ordinary lookup path for that item. Valid native-overlay
    /// handles use their exact stable slot directly.
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

    /// Atomically replaces existing prepared keys with caller-provided values.
    ///
    /// Unlike [`Self::insert_prepared_batch`], absent keys remain absent.
    /// `previous` receives the exact old value for each successful replacement
    /// and `None` for each absent key. Invalid or stale prepared handles fall
    /// back to the ordinary exact update path.
    ///
    /// # Panics
    ///
    /// Panics unless `keys`, `prepared`, `values`, and `previous` have equal
    /// lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn replace_prepared_batch<K>(
        &self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        values: &[NonMaxU64],
        previous: &mut [Option<NonMaxU64>],
    ) where
        K: AsRef<[u8]>,
    {
        self.inner
            .replace_prepared_batch(keys, prepared, values, previous);
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

    /// Rebuilds the adaptive generation only when its lock-free counters cross
    /// the supplied maintenance policy.
    ///
    /// The current generation's capacity is reused, so callers do not need to
    /// predict the maximum cache population. This is intended for a background
    /// maintenance worker; it may construct a frozen generation before
    /// returning. Concurrent calls are serialized and recheck the policy after
    /// acquiring the rebuild gate.
    ///
    /// Non-adaptive overlays and adaptive generations below every threshold
    /// return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error. The redirected generation
    /// remains correct and usable if packing fails.
    pub fn rebuild_adaptive_if_needed(
        &self,
        policy: AdaptiveRebuildPolicy,
    ) -> Result<Option<GenerationRebuild>, FrozenBuildError> {
        self.inner.rebuild_adaptive_if_needed(policy)
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

    /// Returns adaptive learning and capacity statistics for the active layer.
    ///
    /// This is `None` for every non-adaptive overlay strategy. Collection scans
    /// immutable overlay key metadata without acquiring the rebuild mutex and
    /// should normally run on a maintenance worker.
    #[must_use]
    pub fn adaptive_overlay_stats(&self) -> Option<AdaptiveOverlayStats> {
        self.inner.adaptive_overlay_stats()
    }

    pub(crate) fn adaptive_capacity_pressure(&self, maximum_bps: u16) -> bool {
        self.inner.adaptive_capacity_pressure(maximum_bps)
    }

    pub(crate) fn scan_entries(&self, visit: &mut dyn FnMut(&[u8], NonMaxU64)) {
        self.inner
            .scan_entries(&mut |key, value| visit(key, *value));
    }

    pub(crate) fn fallback_len(&self) -> usize {
        self.inner.fallback_len()
    }

    pub(crate) fn sample_atomic_entry(
        &self,
        seed: u64,
        visit: &mut dyn FnMut(&[u8], NonMaxU64),
    ) -> bool {
        self.inner
            .sample_atomic_entry(seed, &mut |key, value| visit(key, *value))
    }

    pub(crate) fn sample_base_entry(
        &self,
        seed: u64,
        visit: &mut dyn FnMut(&[u8], NonMaxU64),
    ) -> bool {
        self.inner
            .sample_base_entry(seed, &mut |key, value| visit(key, *value))
    }

    pub(crate) fn sample_fallback_entries(
        &self,
        seed: u64,
        limit: usize,
        visit: &mut dyn FnMut(&[u8], NonMaxU64),
    ) -> usize {
        self.inner
            .sample_fallback_entries(seed, limit, &mut |key, value| visit(key, *value))
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
    #[inline]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value(key, |value| value.cloned())
    }

    /// Runs `read` against one safely pinned generation without a lock.
    #[inline]
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
        self.rebuild_locked(overlay_capacity)
    }

    fn rebuild_adaptive_if_needed(
        &self,
        policy: AdaptiveRebuildPolicy,
    ) -> Result<Option<GenerationRebuild>, FrozenBuildError>
    where
        V: Clone,
    {
        let Some(initial) = self.adaptive_overlay_stats() else {
            return Ok(None);
        };
        if !initial.recommendation(policy).is_recommended() {
            return Ok(None);
        }

        let _one_rebuild = self.rebuild_gate.lock();
        let Some(current) = self.adaptive_overlay_stats() else {
            return Ok(None);
        };
        if !current.recommendation(policy).is_recommended() {
            return Ok(None);
        }
        self.rebuild_locked(current.capacity).map(Some)
    }

    fn rebuild_locked(&self, overlay_capacity: usize) -> Result<GenerationRebuild, FrozenBuildError>
    where
        V: Clone,
    {
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
        previous.close_read_batches();
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

    fn adaptive_overlay_stats(&self) -> Option<AdaptiveOverlayStats> {
        self.current
            .load()
            .overlay
            .adaptive_snapshot()
            .map(AdaptiveOverlayStats::from_snapshot)
    }

    fn adaptive_capacity_pressure(&self, maximum_bps: u16) -> bool {
        self.current
            .load()
            .overlay
            .adaptive_capacity_pressure(maximum_bps)
    }

    fn scan_entries(&self, visit: &mut dyn FnMut(&[u8], &V)) {
        self.current
            .load()
            .for_each_entry_hashed(&self.writer_hash_builder, visit);
    }

    fn sample_atomic_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V)) -> bool {
        self.current.load().sample_atomic_entry(seed, visit)
    }

    fn sample_base_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V)) -> bool {
        self.current.load().sample_base_entry(seed, visit)
    }

    fn sample_fallback_entries(
        &self,
        seed: u64,
        limit: usize,
        visit: &mut dyn FnMut(&[u8], &V),
    ) -> usize {
        self.current
            .load()
            .sample_fallback_entries(seed, limit, visit)
    }

    fn fallback_len(&self) -> usize {
        self.current.load().fallback_len()
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

impl GenerationMapCore<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    fn get_atomic_protected(&self, key: &[u8]) -> Option<NonMaxU64> {
        let generation = self.current.load();
        let (stripe, key_hash) = self.writer_route(key);
        generation.get_atomic_protected(key, key_hash, stripe)
    }
}

#[cfg(feature = "prepared-keys")]
impl GenerationMapCore<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    #[cfg(feature = "prepared-batch-gate")]
    fn batch_route(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
    ) -> Option<GenerationWriteRoute> {
        let key_hash = GenerationKeyHash::new(&self.writer_hash_builder, key);
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        let stripe = usize::try_from(key_hash.route() & stripe_mask)
            .expect("masked writer stripe fits usize");
        let state = writer.generation.writer_stripes[stripe].load(Ordering::Acquire);
        (state & WRITER_STRIPE_CLOSED == 0).then_some(GenerationWriteRoute {
            stripe,
            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
            key_hash,
        })
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn get_in_batch(&self, writer: &AtomicPreparedBatchWriter, key: &[u8]) -> Option<NonMaxU64> {
        let Some(route) = self.batch_route(writer, key) else {
            return self.get_cloned(key);
        };
        writer.generation.with_value_in_stripe(
            key,
            route.key_hash,
            route.stripe,
            copy_optional_non_max,
        )
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn insert_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        value: NonMaxU64,
    ) -> InsertOutcome<NonMaxU64> {
        let Some(route) = self.batch_route(writer, key) else {
            return self.insert(key, value);
        };
        let outcome = writer.generation.insert(key, value, route);
        if matches!(outcome, InsertOutcome::Inserted) {
            writer.generation.adjust_len(route.stripe, 1);
        }
        outcome
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn insert_new_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        value: NonMaxU64,
    ) -> bool {
        let Some(route) = self.batch_route(writer, key) else {
            return self.insert_new(key, value);
        };
        let inserted = writer.generation.insert_new(key, value, route);
        if inserted {
            writer.generation.adjust_len(route.stripe, 1);
        }
        inserted
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn get_or_insert_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        value: NonMaxU64,
    ) -> NonMaxU64 {
        let Some(route) = self.batch_route(writer, key) else {
            return self.pin_writer(key).get_or_insert(key, value);
        };
        let (current, inserted) = writer.generation.get_or_insert_atomic(key, value, route);
        if inserted {
            writer.generation.adjust_len(route.stripe, 1);
        }
        current
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn update_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<NonMaxU64> {
        let Some(route) = self.batch_route(writer, key) else {
            return self.update(key, update);
        };
        writer.generation.update(key, update, route)
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn upsert_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        insert_value: NonMaxU64,
        update: impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> NonMaxU64 {
        let Some(route) = self.batch_route(writer, key) else {
            return self.upsert(key, insert_value, update);
        };
        let (value, inserted) = writer.generation.upsert(key, insert_value, update, route);
        if inserted {
            writer.generation.adjust_len(route.stripe, 1);
        }
        value
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn remove_if_in_batch(
        &self,
        writer: &AtomicPreparedBatchWriter,
        key: &[u8],
        predicate: impl Fn(&NonMaxU64) -> bool,
    ) -> Option<NonMaxU64> {
        let Some(route) = self.batch_route(writer, key) else {
            return self.remove_if(key, predicate);
        };
        let removed = writer.generation.remove_if(key, predicate, route);
        if removed.is_some() {
            writer.generation.adjust_len(route.stripe, -1);
        }
        removed
    }

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
        if let Some(prepared) = generation.overlay.prepare_atomic(key, route_hash) {
            return AtomicPreparedKey::new_overlay(route_hash, prepared);
        }
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
        let base = generation.base.load();
        let stripe_mask = u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
        for ((key, prepared), value) in keys.iter().zip(prepared).zip(values.iter_mut()) {
            let key = key.as_ref();
            let stripe = usize::try_from(prepared.route_hash() & stripe_mask)
                .expect("masked writer stripe fits usize");
            let state = generation.writer_stripes[stripe].load(Ordering::Acquire);
            if let Some(slot) = prepared.prepared_slot()
                && state & WRITER_STRIPE_DIRECT_BASE != 0
                && state & WRITER_STRIPE_OVERLAY_BASE == 0
                && let DirectMutation::Handled(prepared_value) =
                    base.with_prepared_value(key, slot, copy_optional_non_max)
            {
                *value = prepared_value;
            } else {
                *value = self.get_prepared_in_generation(&generation, key, *prepared);
            }
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
        let stripe = usize::try_from(prepared.route_hash() & stripe_mask)
            .expect("masked writer stripe fits usize");
        if let Some(slot) = prepared.prepared_overlay_slot()
            && let Some(value) =
                generation
                    .overlay
                    .with_prepared_atomic(key, slot, AtomicU64Cell::get_protected)
        {
            return value;
        }
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
            let writer = self.pin_prepared_writer(prepared.route_hash());
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
            AtomicPreparedBorrowedWriter::try_pin(generation, predecessor, prepared.route_hash())
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

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn replace_prepared_batch<K>(
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
            "prepared replacement output batch length mismatch"
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
            match Self::replace_prepared_in_snapshot(
                &generation,
                predecessor.as_deref(),
                key,
                *prepared,
                *value,
            ) {
                DirectMutation::Handled(value) => *previous = value,
                DirectMutation::NotMember => {
                    let displaced = Cell::new(None);
                    let updated = self.update(key, |current| {
                        displaced.set(Some(*current));
                        *value
                    });
                    *previous = updated.and(displaced.get());
                }
            }
        }
    }

    #[cfg(not(feature = "prepared-batch-gate"))]
    fn replace_prepared_in_snapshot(
        generation: &GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        predecessor: Option<&GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        value: NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some(slot) = prepared.prepared_slot() else {
            return DirectMutation::NotMember;
        };
        let Some(writer) =
            AtomicPreparedBorrowedWriter::try_pin(generation, predecessor, prepared.route_hash())
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
            .replace_prepared(key, slot, &value)
    }

    fn insert_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: NonMaxU64,
    ) -> InsertOutcome<NonMaxU64> {
        if let Some(slot) = prepared.prepared_slot() {
            let mut writer = self.pin_prepared_writer(prepared.route_hash());
            if writer.direct_base && !writer.overlay_may_shadow_base {
                match writer
                    .generation
                    .base
                    .load()
                    .insert_prepared(key, slot, &value)
                {
                    DirectMutation::Handled(outcome) => {
                        writer.release_with_len(isize::from(matches!(
                            outcome,
                            InsertOutcome::Inserted
                        )));
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
        let Some(mut writer) =
            AtomicPreparedBorrowedWriter::try_pin(generation, predecessor, prepared.route_hash())
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
                writer.release_with_len(isize::from(matches!(outcome, InsertOutcome::Inserted)));
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
            .map_or(0, AtomicPreparedKey::route_hash);
        let writer = self.pin_prepared_batch_writer(route_hash);
        let base = writer.generation.base.load();
        for ((key, prepared), updated) in keys.iter().zip(prepared).zip(updated.iter_mut()) {
            let key = key.as_ref();
            match Self::update_prepared_in_batch_writer(&writer, &base, key, *prepared, &update) {
                DirectMutation::Handled(value) => *updated = value,
                DirectMutation::NotMember => *updated = self.update(key, &update),
            }
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn update_prepared_in_batch_writer(
        writer: &AtomicPreparedBatchWriter,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some((slot, _stripe)) = Self::prepared_batch_direct_slot(writer, prepared) else {
            return DirectMutation::NotMember;
        };
        base.update_prepared(key, slot, update)
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn replace_prepared_batch<K>(
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
            "prepared replacement output batch length mismatch"
        );
        if keys.is_empty() {
            return;
        }

        let route_hash = prepared
            .iter()
            .copied()
            .find(|prepared| prepared.has_direct_slot())
            .map_or(0, AtomicPreparedKey::route_hash);
        let writer = self.pin_prepared_batch_writer(route_hash);
        let base = writer.generation.base.load();
        for (((key, prepared), value), previous) in keys
            .iter()
            .zip(prepared)
            .zip(values)
            .zip(previous.iter_mut())
        {
            let key = key.as_ref();
            match Self::replace_prepared_in_batch_writer(&writer, &base, key, *prepared, *value) {
                DirectMutation::Handled(value) => *previous = value,
                DirectMutation::NotMember => {
                    let displaced = Cell::new(None);
                    let updated = self.update(key, |current| {
                        displaced.set(Some(*current));
                        *value
                    });
                    *previous = updated.and(displaced.get());
                }
            }
        }
    }

    #[cfg(feature = "prepared-batch-gate")]
    fn replace_prepared_in_batch_writer(
        writer: &AtomicPreparedBatchWriter,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        value: NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some((slot, _stripe)) = Self::prepared_batch_direct_slot(writer, prepared) else {
            return DirectMutation::NotMember;
        };
        base.replace_prepared(key, slot, &value)
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
            .map_or(0, AtomicPreparedKey::route_hash);
        let writer = self.pin_prepared_batch_writer(route_hash);
        let base = writer.generation.base.load();
        for (((key, prepared), value), previous) in keys
            .iter()
            .zip(prepared)
            .zip(values)
            .zip(previous.iter_mut())
        {
            let key = key.as_ref();
            let outcome =
                match Self::insert_prepared_in_batch_writer(&writer, &base, key, *prepared, *value)
                {
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
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        prepared: AtomicPreparedKey,
        value: NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        let Some((slot, stripe)) = Self::prepared_batch_direct_slot(writer, prepared) else {
            return DirectMutation::NotMember;
        };
        match base.insert_prepared(key, slot, &value) {
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
        let stripe = usize::try_from(prepared.route_hash() & stripe_mask)
            .expect("masked writer stripe fits usize");
        let state = writer.generation.writer_stripes[stripe].load(Ordering::Acquire);
        (state & WRITER_STRIPE_CLOSED == 0
            && state & WRITER_STRIPE_DIRECT_BASE != 0
            && state & WRITER_STRIPE_OVERLAY_BASE == 0)
            .then_some((slot, stripe))
    }

    fn remove_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<NonMaxU64> {
        if let Some(slot) = prepared.prepared_slot() {
            let mut writer = self.pin_prepared_writer(prepared.route_hash());
            if writer.direct_base && !writer.overlay_may_shadow_base {
                match writer.generation.base.load().remove_prepared(key, slot) {
                    DirectMutation::Handled(removed) => {
                        writer.release_with_len(if removed.is_some() { -1 } else { 0 });
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

fn copy_optional_non_max(value: Option<&NonMaxU64>) -> Option<NonMaxU64> {
    value.copied()
}

#[cfg(feature = "prepared-keys")]
struct AtomicPreparedWriter {
    generation: PinnedGeneration<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
    stripe: usize,
    direct_base: bool,
    overlay_may_shadow_base: bool,
    released: bool,
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
        let state = try_acquire_writer_stripe(counter)?;
        Some(Self {
            generation,
            stripe,
            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
            released: false,
        })
    }

    fn release_with_len(&mut self, amount: isize) {
        release_writer_stripe(
            &self.generation.writer_stripes[self.stripe],
            amount,
            &mut self.released,
        );
    }
}

#[cfg(feature = "prepared-keys")]
impl Drop for AtomicPreparedWriter {
    fn drop(&mut self) {
        if self.released {
            return;
        }
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
    released: bool,
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
        let state = try_acquire_writer_stripe(counter)?;
        Some(Self {
            generation,
            stripe,
            direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
            overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
            released: false,
        })
    }

    fn release_with_len(&mut self, amount: isize) {
        release_writer_stripe(
            &self.generation.writer_stripes[self.stripe],
            amount,
            &mut self.released,
        );
    }
}

#[cfg(all(feature = "prepared-keys", not(feature = "prepared-batch-gate")))]
impl Drop for AtomicPreparedBorrowedWriter<'_> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
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
        try_acquire_writer_stripe(counter)?;
        Some(Self { generation, gate })
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
    released: bool,
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
        let state = try_acquire_writer_stripe(counter)?;
        Some(Self {
            generation,
            route: GenerationWriteRoute {
                stripe,
                direct_base: state & WRITER_STRIPE_DIRECT_BASE != 0,
                overlay_may_shadow_base: state & WRITER_STRIPE_OVERLAY_BASE != 0,
                key_hash,
            },
            released: false,
        })
    }
}

impl<V, C, B> GenerationWriter<V, C, B>
where
    C: GenerationCell<V>,
    B: GenerationFrozen<V>,
{
    fn insert(mut self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        let outcome = self.generation.insert(key, value, self.route);
        self.release_with_len(isize::from(matches!(outcome, InsertOutcome::Inserted)));
        outcome
    }

    fn insert_new(mut self, key: &[u8], value: V) -> bool
    where
        V: Clone,
    {
        let inserted = self.generation.insert_new(key, value, self.route);
        self.release_with_len(isize::from(inserted));
        inserted
    }

    fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        self.generation.update(key, update, self.route)
    }

    fn upsert(mut self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        let (value, inserted) = self
            .generation
            .upsert(key, insert_value, update, self.route);
        self.release_with_len(isize::from(inserted));
        value
    }

    fn remove(self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.remove_if(key, |_| true)
    }

    fn remove_if(mut self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        let removed = self.generation.remove_if(key, predicate, self.route);
        self.release_with_len(if removed.is_some() { -1 } else { 0 });
        removed
    }

    fn release_with_len(&mut self, amount: isize) {
        release_writer_stripe(
            &self.generation.writer_stripes[self.route.stripe],
            amount,
            &mut self.released,
        );
    }
}

impl GenerationWriter<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    fn probe_entry(&self, key: &[u8]) -> AtomicEntryProbe {
        self.generation.probe_atomic_entry(key, self.route)
    }

    fn get_or_insert(mut self, key: &[u8], value: NonMaxU64) -> NonMaxU64 {
        let (current, inserted) = self.generation.get_or_insert_atomic(key, value, self.route);
        self.release_with_len(isize::from(inserted));
        current
    }

    fn insert_vacant(
        mut self,
        key: &[u8],
        value: NonMaxU64,
        target: AtomicVacantTarget,
    ) -> InsertOutcome<NonMaxU64> {
        let outcome = match target {
            AtomicVacantTarget::FrozenSlot { map, slot } => map.insert_slot(slot, value),
            AtomicVacantTarget::Overlay { mark_base_shadow } => {
                if mark_base_shadow {
                    self.mark_overlay_may_shadow_base(self.route.stripe);
                } else {
                    self.mark_overlay_touched();
                }
                match self.overlay.insert_or_visit_prehashed(
                    key,
                    self.route.key_hash.route(),
                    AtomicU64Cell::present(value),
                    |current| current.replace(value),
                ) {
                    OverlayInsert::Inserted => InsertOutcome::Inserted,
                    OverlayInsert::Occupied(previous) => {
                        previous.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                    }
                }
            }
        };
        self.release_with_len(isize::from(matches!(outcome, InsertOutcome::Inserted)));
        outcome
    }

    fn insert_new_vacant(
        mut self,
        key: &[u8],
        value: NonMaxU64,
        target: AtomicVacantTarget,
    ) -> bool {
        let inserted = match target {
            AtomicVacantTarget::FrozenSlot { map, slot } => map.insert_new_slot(slot, value),
            AtomicVacantTarget::Overlay { mark_base_shadow } => {
                if mark_base_shadow {
                    self.mark_overlay_may_shadow_base(self.route.stripe);
                } else {
                    self.mark_overlay_touched();
                }
                match self.overlay.insert_or_visit_prehashed(
                    key,
                    self.route.key_hash.route(),
                    AtomicU64Cell::present(value),
                    |current| current.insert_new(value),
                ) {
                    OverlayInsert::Inserted => true,
                    OverlayInsert::Occupied(inserted) => inserted,
                }
            }
        };
        self.release_with_len(isize::from(inserted));
        inserted
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
        if self.released {
            return;
        }
        let previous =
            self.generation.writer_stripes[self.route.stripe].fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
    }
}

struct GenerationLayer<V, C, B> {
    base: ArcSwap<GenerationBase<V, C, B>>,
    base_is_empty: bool,
    write_predecessor: ArcSwapOption<GenerationLayer<V, C, B>>,
    write_predecessor_active: AtomicBool,
    overlay: GenerationOverlay<C>,
    overlay_touched: AtomicBool,
    initial_len: AtomicIsize,
    writer_stripes: Box<[AtomicUsize]>,
    read_batches: AtomicUsize,
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
        let base_is_empty = matches!(
            &base,
            GenerationBase::Frozen { map, .. } if map.len() == 0
        );
        let (write_predecessor, write_predecessor_active) = match &base {
            GenerationBase::Previous(previous) => (Some(Arc::clone(previous)), true),
            GenerationBase::Frozen { .. } | GenerationBase::StableHybrid(_) => (None, false),
        };
        let direct_base = matches!(&base, GenerationBase::Frozen { .. }) && B::DIRECT_MUTATION;
        Arc::new(Self {
            base: ArcSwap::from_pointee(base),
            base_is_empty,
            write_predecessor: ArcSwapOption::from(write_predecessor),
            write_predecessor_active: AtomicBool::new(write_predecessor_active),
            overlay: GenerationOverlay::with_capacity(
                overlay_capacity,
                overlay_mode,
                overlay_hash_builder,
            ),
            overlay_touched: AtomicBool::new(false),
            initial_len: AtomicIsize::new(isize::try_from(len).unwrap_or(isize::MAX)),
            writer_stripes: (0..WRITER_STRIPES)
                .map(|_| {
                    AtomicUsize::new(
                        (usize::from(direct_base) * WRITER_STRIPE_DIRECT_BASE)
                            | WRITER_STRIPE_LEN_ZERO,
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            read_batches: AtomicUsize::new(0),
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
        if self.base_is_empty {
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| match cell {
                    Some(cell) => cell.with_value(read),
                    None => read(None),
                });
        }
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        let mut read = Some(read);
        if state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0 {
            let base_value = self.base.load().with_present_value(key, key_hash, |value| {
                read.take().expect("generation read callback runs once")(Some(value))
            });
            if let Some(value) = base_value {
                return value;
            }
            if !self.overlay_touched.load(Ordering::Acquire) {
                return read.take().expect("generation read callback runs once")(None);
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
        if self.base_is_empty {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.replace(value),
            ) {
                OverlayInsert::Inserted => InsertOutcome::Inserted,
                OverlayInsert::Occupied(previous) => {
                    previous.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                }
            };
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some(outcome) = self.base.load().direct_insert(key, route.key_hash, &value)
        {
            return outcome;
        }
        if direct_first {
            self.mark_overlay_touched();
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.replace(value),
            ) {
                OverlayInsert::Inserted => InsertOutcome::Inserted,
                OverlayInsert::Occupied(previous) => {
                    if let Some(previous) = previous {
                        InsertOutcome::Replaced(previous)
                    } else {
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
                InsertOutcome::Inserted
            };
        }
        if route.direct_base
            && !direct_first
            && let Some(outcome) = self.base.load().direct_insert(key, route.key_hash, &value)
        {
            return outcome;
        }
        let base_value = if route.direct_base {
            None
        } else {
            self.base.load().get_cloned_hashed(key, route.key_hash)
        };
        if base_value.is_some() {
            self.mark_overlay_may_shadow_base(route.stripe);
        } else {
            self.mark_overlay_touched();
        }
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(value.clone()),
            |current| current.replace(value),
        ) {
            OverlayInsert::Inserted => {
                base_value.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
            }
            OverlayInsert::Occupied(previous) => {
                if let Some(previous) = previous {
                    InsertOutcome::Replaced(previous)
                } else {
                    InsertOutcome::Inserted
                }
            }
        }
    }

    fn insert_new(&self, key: &[u8], value: V, route: GenerationWriteRoute) -> bool
    where
        V: Clone,
    {
        if self.base_is_empty {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.insert_new(value),
            ) {
                OverlayInsert::Inserted => true,
                OverlayInsert::Occupied(inserted) => inserted,
            };
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some(inserted) = self
                .base
                .load()
                .direct_insert_new(key, route.key_hash, &value)
        {
            return inserted;
        }
        if direct_first {
            self.mark_overlay_touched();
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(value.clone()),
                |current| current.insert_new(value),
            ) {
                OverlayInsert::Inserted => true,
                OverlayInsert::Occupied(inserted) => inserted,
            };
        }
        if let Some(inserted) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.insert_new(value.clone()))
                })
        {
            return inserted;
        }
        if route.direct_base
            && !direct_first
            && let Some(inserted) = self
                .base
                .load()
                .direct_insert_new(key, route.key_hash, &value)
        {
            return inserted;
        }
        if !route.direct_base && self.base.load().contains_key_hashed(key, route.key_hash) {
            return false;
        }
        self.mark_overlay_touched();
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::present(value.clone()),
            |current| current.insert_new(value),
        ) {
            OverlayInsert::Inserted => true,
            OverlayInsert::Occupied(inserted) => inserted,
        }
    }

    fn update(&self, key: &[u8], update: impl Fn(&V) -> V, route: GenerationWriteRoute) -> Option<V>
    where
        V: Clone,
    {
        if self.base_is_empty {
            return self
                .overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.and_then(|cell| cell.update(&update))
                });
        }
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
    ) -> (V, bool)
    where
        V: Clone,
    {
        if self.base_is_empty {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(insert_value.clone()),
                |current| current.upsert(&insert_value, &update),
            ) {
                OverlayInsert::Inserted => (insert_value, true),
                OverlayInsert::Occupied(result) => result,
            };
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some((value, became_live)) =
                self.base
                    .load()
                    .direct_upsert(key, route.key_hash, &insert_value, &update)
        {
            return (value, became_live);
        }
        if direct_first {
            self.mark_overlay_touched();
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                C::present(insert_value.clone()),
                |current| current.upsert(&insert_value, &update),
            ) {
                OverlayInsert::Inserted => (insert_value, true),
                OverlayInsert::Occupied(result) => result,
            };
        }
        if let Some((value, became_live)) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.upsert(&insert_value, &update))
                })
        {
            return (value, became_live);
        }
        if route.direct_base
            && !direct_first
            && let Some((value, became_live)) =
                self.base
                    .load()
                    .direct_upsert(key, route.key_hash, &insert_value, &update)
        {
            return (value, became_live);
        }
        let base_value = if route.direct_base {
            None
        } else {
            self.base.load().get_cloned_hashed(key, route.key_hash)
        };
        if base_value.is_some() {
            self.mark_overlay_may_shadow_base(route.stripe);
        } else {
            self.mark_overlay_touched();
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
            OverlayInsert::Inserted => (next, base_value.is_none()),
            OverlayInsert::Occupied(result) => result,
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
        if self.base_is_empty {
            return self
                .overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.and_then(|cell| cell.remove_if(&predicate))
                });
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first {
            match self
                .base
                .load()
                .direct_remove_if(key, route.key_hash, &predicate)
            {
                DirectMutation::Handled(removed) => return removed,
                DirectMutation::NotMember => {}
            }
        }
        if let Some(removed) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.remove_if(&predicate))
                })
        {
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
            return removed;
        }
        let base_value = self
            .base
            .load()
            .get_cloned_hashed(key, route.key_hash)
            .filter(|value| predicate(value))?;
        self.mark_overlay_may_shadow_base(route.stripe);
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            C::deleted(),
            |current| current.remove_if(&predicate),
        ) {
            OverlayInsert::Inserted => Some(base_value),
            OverlayInsert::Occupied(removed) => removed,
        }
    }

    fn len(&self) -> usize {
        let delta = self.writer_stripes.iter().fold(0_i128, |delta, stripe| {
            delta + writer_stripe_len_delta(stripe.load(Ordering::Acquire)) as i128
        });
        let len = self.initial_len.load(Ordering::Acquire) as i128 + delta;
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
        // Stage key bytes contiguously. Rebuild used to allocate one temporary
        // Box per live key, only for the frozen builder to copy those bytes
        // into its packed arena and immediately free every Box. Keeping the
        // temporary ownership in one byte buffer removes O(n) allocator calls
        // while retaining the same fallible construction boundary.
        let expected_entries = self.len();
        // End offsets are sufficient because keys are appended without gaps;
        // this keeps staging metadata to one machine word plus the value.
        let mut entries = Vec::<(usize, V)>::new();
        entries
            .try_reserve(expected_entries)
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut key_bytes = Vec::<u8>::new();
        key_bytes
            .try_reserve(expected_entries.saturating_mul(16))
            .map_err(|_| FrozenBuildError::AllocationFailed)?;
        let mut allocation_failed = false;
        self.for_each_entry_hashed(hash_builder, &mut |key, value| {
            if allocation_failed {
                return;
            }
            if key_bytes.try_reserve(key.len()).is_err() {
                allocation_failed = true;
                return;
            }
            key_bytes.extend_from_slice(key);
            entries.push((key_bytes.len(), value.clone()));
        });
        if allocation_failed {
            return Err(FrozenBuildError::AllocationFailed);
        }
        let mut key_begin = 0;
        let entries = entries.into_iter().map(|(key_end, value)| {
            let key = &key_bytes[key_begin..key_end];
            key_begin = key_end;
            (key, value)
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

    fn sample_atomic_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V)) -> bool {
        self.overlay.sample_atomic(seed, &mut |key, cell| {
            cell.with_value(|value| {
                value.is_some_and(|value| {
                    visit(key, value);
                    true
                })
            })
        })
    }

    fn sample_base_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V)) -> bool {
        let base = self.base.load();
        base.sample_entry(seed, &mut |key, base_value| {
            self.overlay.with_cell(key, |cell| {
                if let Some(cell) = cell {
                    cell.with_value(|value| {
                        if let Some(value) = value {
                            visit(key, value);
                            true
                        } else {
                            false
                        }
                    })
                } else {
                    visit(key, base_value);
                    true
                }
            })
        })
    }

    fn sample_fallback_entries(
        &self,
        seed: u64,
        limit: usize,
        visit: &mut dyn FnMut(&[u8], &V),
    ) -> usize {
        self.overlay.sample_fallback(seed, limit, &mut |key, cell| {
            cell.with_value(|value| {
                value.is_some_and(|value| {
                    visit(key, value);
                    true
                })
            })
        })
    }

    fn fallback_len(&self) -> usize {
        self.overlay.fallback_len()
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
                        state | WRITER_STRIPE_CLOSED,
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
        let previous = self
            .read_batches
            .fetch_or(WRITER_STRIPE_CLOSED, Ordering::AcqRel);
        if previous & WRITER_STRIPE_COUNT_MASK == 0 {
            return;
        }

        let mut spins = 0_u32;
        while self.read_batches.load(Ordering::Acquire) & WRITER_STRIPE_COUNT_MASK != 0 {
            if spins < 64 {
                spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
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
        if amount == 0 {
            return;
        }
        let counter = &self.writer_stripes[stripe];
        if usize::BITS >= 64 {
            let magnitude = amount.unsigned_abs();
            let adjustment = WRITER_STRIPE_LEN_UNIT
                .checked_mul(magnitude)
                .expect("writer stripe length adjustment fits usize");
            let previous = if amount > 0 {
                counter.fetch_add(adjustment, Ordering::Relaxed)
            } else {
                counter.fetch_sub(adjustment, Ordering::Relaxed)
            };
            let raw = (previous & WRITER_STRIPE_LEN_MASK) >> WRITER_STRIPE_LEN_SHIFT;
            debug_assert!(amount < 0 || raw <= WRITER_STRIPE_LEN_VALUE_MASK - magnitude);
            debug_assert!(amount > 0 || raw >= magnitude);
            return;
        }
        let mut state = counter.load(Ordering::Relaxed);
        loop {
            let next_delta = writer_stripe_len_delta(state)
                .checked_add(amount)
                .expect("writer stripe length delta overflow");
            let next = (state & !WRITER_STRIPE_LEN_MASK) | writer_stripe_len_bits(next_delta);
            match counter.compare_exchange_weak(state, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => state = observed,
            }
        }
    }

    fn mark_overlay_may_shadow_base(&self, stripe: usize) {
        self.mark_overlay_touched();
        self.writer_stripes[stripe].fetch_or(WRITER_STRIPE_OVERLAY_BASE, Ordering::Release);
    }

    fn mark_overlay_touched(&self) {
        self.overlay_touched.store(true, Ordering::Release);
    }
}

impl GenerationLayer<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    fn get_atomic_protected(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        stripe: usize,
    ) -> Option<NonMaxU64> {
        if self.base_is_empty {
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| {
                    cell.and_then(AtomicU64Cell::get_protected)
                });
        }
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        let direct_first =
            state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0;
        if direct_first && let Some(value) = self.base.load().get_atomic_protected(key, key_hash) {
            return Some(value);
        }
        if direct_first {
            if !self.overlay_touched.load(Ordering::Acquire) {
                return None;
            }
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| {
                    cell.and_then(AtomicU64Cell::get_protected)
                });
        }
        if let Some(value) = self
            .overlay
            .with_cell_prehashed(key, key_hash.route(), |cell| {
                cell.map(AtomicU64Cell::get_protected)
            })
        {
            return value;
        }
        self.base.load().get_atomic_protected(key, key_hash)
    }

    #[cfg(any(test, feature = "shared-gx"))]
    fn get_atomic_protected_with_base(
        &self,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        key_hash: GenerationKeyHash,
        stripe: usize,
    ) -> Option<NonMaxU64> {
        if self.base_is_empty {
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| {
                    cell.and_then(AtomicU64Cell::get_protected)
                });
        }
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        let direct_first =
            state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0;
        if direct_first && matches!(base, GenerationBase::Previous(_)) {
            // Direct mutation is enabled only after the authoritative base has
            // changed from Previous to Frozen. A cached reader can observe the
            // enable bit immediately after its own earlier revalidation, so a
            // predecessor passed here must never service that direct read.
            return self.get_atomic_protected(key, key_hash, stripe);
        }
        if direct_first && let Some(value) = base.get_atomic_protected(key, key_hash) {
            return Some(value);
        }
        if direct_first {
            if !self.overlay_touched.load(Ordering::Acquire) {
                return None;
            }
            return self
                .overlay
                .with_cell_prehashed(key, key_hash.route(), |cell| {
                    cell.and_then(AtomicU64Cell::get_protected)
                });
        }
        if let Some(value) = self
            .overlay
            .with_cell_prehashed(key, key_hash.route(), |cell| {
                cell.map(AtomicU64Cell::get_protected)
            })
        {
            return value;
        }
        base.get_atomic_protected(key, key_hash)
    }

    #[cfg(not(feature = "shared-gx"))]
    fn get_atomic_protected_with_base_route(
        &self,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        route_hash: u64,
        stripe: usize,
        hash_builder: &GenerationHashBuilder,
    ) -> Option<NonMaxU64> {
        if self.base_is_empty {
            return self.overlay.with_cell_prehashed(key, route_hash, |cell| {
                cell.and_then(AtomicU64Cell::get_protected)
            });
        }
        let state = self.writer_stripes[stripe].load(Ordering::Acquire);
        let direct_first =
            state & WRITER_STRIPE_DIRECT_BASE != 0 && state & WRITER_STRIPE_OVERLAY_BASE == 0;
        if direct_first && matches!(base, GenerationBase::Previous(_)) {
            let key_hash = GenerationKeyHash::from_verified_route(hash_builder, key, route_hash);
            return self.get_atomic_protected(key, key_hash, stripe);
        }
        if direct_first {
            let key_hash = GenerationKeyHash::from_verified_route(hash_builder, key, route_hash);
            if let Some(value) = base.get_atomic_protected(key, key_hash) {
                return Some(value);
            }
            if !self.overlay_touched.load(Ordering::Acquire) {
                return None;
            }
            return self.overlay.with_cell_prehashed(key, route_hash, |cell| {
                cell.and_then(AtomicU64Cell::get_protected)
            });
        }
        if let Some(value) = self.overlay.with_cell_prehashed(key, route_hash, |cell| {
            cell.map(AtomicU64Cell::get_protected)
        }) {
            return value;
        }
        let key_hash = GenerationKeyHash::from_verified_route(hash_builder, key, route_hash);
        base.get_atomic_protected(key, key_hash)
    }

    fn get_or_insert_atomic(
        &self,
        key: &[u8],
        value: NonMaxU64,
        route: GenerationWriteRoute,
    ) -> (NonMaxU64, bool) {
        let base = self.base.load();
        self.get_or_insert_atomic_with_base(&base, key, value, route)
    }

    fn get_or_insert_atomic_with_base(
        &self,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        value: NonMaxU64,
        route: GenerationWriteRoute,
    ) -> (NonMaxU64, bool) {
        if self.base_is_empty {
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                AtomicU64Cell::present(value),
                |current| current.get_or_insert(value),
            ) {
                OverlayInsert::Inserted => (value, true),
                OverlayInsert::Occupied(result) => result,
            };
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first
            && let Some(result) = base.direct_get_or_insert_atomic(key, route.key_hash, value)
        {
            return result;
        }
        if direct_first {
            self.mark_overlay_touched();
            return match self.overlay.probe_or_insert_prehashed(
                key,
                route.key_hash.route(),
                AtomicU64Cell::present(value),
                |current| current.get_or_insert(value),
            ) {
                OverlayInsert::Inserted => (value, true),
                OverlayInsert::Occupied(result) => result,
            };
        }
        if let Some(result) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(|cell| cell.get_or_insert(value))
                })
        {
            return result;
        }
        if route.direct_base
            && let Some(result) = base.direct_get_or_insert_atomic(key, route.key_hash, value)
        {
            return result;
        }
        if !route.direct_base
            && let Some(current) = base.get_cloned_hashed(key, route.key_hash)
        {
            return (current, false);
        }
        self.mark_overlay_touched();
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            AtomicU64Cell::present(value),
            |current| current.get_or_insert(value),
        ) {
            OverlayInsert::Inserted => (value, true),
            OverlayInsert::Occupied(result) => result,
        }
    }

    #[cfg(not(feature = "shared-gx"))]
    fn get_or_insert_atomic_empty_base(
        &self,
        key: &[u8],
        value: NonMaxU64,
        route_hash: u64,
    ) -> (NonMaxU64, bool) {
        debug_assert!(self.base_is_empty);
        match self.overlay.probe_or_insert_prehashed(
            key,
            route_hash,
            AtomicU64Cell::present(value),
            |current| current.get_or_insert(value),
        ) {
            OverlayInsert::Inserted => (value, true),
            OverlayInsert::Occupied(result) => result,
        }
    }

    #[cfg(all(feature = "operation-batch", not(feature = "shared-gx")))]
    fn get_or_insert_atomic_empty_base_encoded(
        &self,
        key: &[u8],
        encoded: &AtomicEncodedAdmissionKey,
        value: NonMaxU64,
        route_hash: u64,
    ) -> (NonMaxU64, bool) {
        debug_assert!(self.base_is_empty);
        match self.overlay.probe_or_insert_encoded_prehashed(
            key,
            encoded,
            route_hash,
            AtomicU64Cell::present(value),
            |current| current.get_or_insert(value),
        ) {
            OverlayInsert::Inserted => (value, true),
            OverlayInsert::Occupied(result) => result,
        }
    }

    fn probe_atomic_entry(&self, key: &[u8], route: GenerationWriteRoute) -> AtomicEntryProbe {
        if self.base_is_empty {
            return match self.probe_atomic_overlay(key, route.key_hash.route()) {
                AtomicOverlayProbe::Member(Some(value)) => AtomicEntryProbe::Occupied(value),
                AtomicOverlayProbe::NotMember | AtomicOverlayProbe::Member(None) => {
                    AtomicEntryProbe::Vacant(AtomicVacantTarget::Overlay {
                        mark_base_shadow: false,
                    })
                }
            };
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first {
            match self.base.load().probe_atomic(key, route.key_hash) {
                AtomicBaseProbe::Member {
                    value: Some(value), ..
                } => return AtomicEntryProbe::Occupied(value),
                AtomicBaseProbe::Member {
                    map,
                    slot,
                    value: None,
                } => {
                    return AtomicEntryProbe::Vacant(AtomicVacantTarget::FrozenSlot { map, slot });
                }
                AtomicBaseProbe::Logical(Some(value)) => {
                    return AtomicEntryProbe::Occupied(value);
                }
                AtomicBaseProbe::NotMember | AtomicBaseProbe::Logical(None) => {}
            }
            return match self.probe_atomic_overlay(key, route.key_hash.route()) {
                AtomicOverlayProbe::Member(Some(value)) => AtomicEntryProbe::Occupied(value),
                AtomicOverlayProbe::NotMember | AtomicOverlayProbe::Member(None) => {
                    AtomicEntryProbe::Vacant(AtomicVacantTarget::Overlay {
                        mark_base_shadow: false,
                    })
                }
            };
        }

        match self.probe_atomic_overlay(key, route.key_hash.route()) {
            AtomicOverlayProbe::Member(Some(value)) => return AtomicEntryProbe::Occupied(value),
            AtomicOverlayProbe::Member(None) => {
                return AtomicEntryProbe::Vacant(AtomicVacantTarget::Overlay {
                    mark_base_shadow: false,
                });
            }
            AtomicOverlayProbe::NotMember => {}
        }

        match self.base.load().probe_atomic(key, route.key_hash) {
            AtomicBaseProbe::Member {
                value: Some(value), ..
            }
            | AtomicBaseProbe::Logical(Some(value)) => AtomicEntryProbe::Occupied(value),
            AtomicBaseProbe::Member {
                map,
                slot,
                value: None,
            } if route.direct_base => {
                AtomicEntryProbe::Vacant(AtomicVacantTarget::FrozenSlot { map, slot })
            }
            AtomicBaseProbe::Member { value: None, .. } => {
                AtomicEntryProbe::Vacant(AtomicVacantTarget::Overlay {
                    mark_base_shadow: true,
                })
            }
            AtomicBaseProbe::NotMember | AtomicBaseProbe::Logical(None) => {
                AtomicEntryProbe::Vacant(AtomicVacantTarget::Overlay {
                    mark_base_shadow: false,
                })
            }
        }
    }

    fn probe_atomic_overlay(&self, key: &[u8], route_hash: u64) -> AtomicOverlayProbe {
        self.overlay
            .with_cell_prehashed(key, route_hash, |cell| match cell {
                Some(cell) => AtomicOverlayProbe::Member(cell.with_value(copy_optional_non_max)),
                None => AtomicOverlayProbe::NotMember,
            })
    }

    fn remove_atomic_with_base(
        &self,
        base: &GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap>,
        key: &[u8],
        route: GenerationWriteRoute,
    ) -> Option<NonMaxU64> {
        if self.base_is_empty {
            return self.remove_atomic_empty_base(key, route.key_hash.route());
        }
        let direct_first = route.direct_base && !route.overlay_may_shadow_base;
        if direct_first {
            match base.direct_remove_atomic(key, route.key_hash) {
                DirectMutation::Handled(removed) => return removed,
                DirectMutation::NotMember => {}
            }
        }
        if let Some(removed) =
            self.overlay
                .with_cell_prehashed(key, route.key_hash.route(), |cell| {
                    cell.map(AtomicU64Cell::remove)
                })
        {
            return removed;
        }
        if route.direct_base && !direct_first {
            return match base.direct_remove_atomic(key, route.key_hash) {
                DirectMutation::Handled(removed) => removed,
                DirectMutation::NotMember => None,
            };
        }
        let base_value = base.get_cloned_hashed(key, route.key_hash)?;
        self.mark_overlay_may_shadow_base(route.stripe);
        match self.overlay.insert_or_visit_prehashed(
            key,
            route.key_hash.route(),
            AtomicU64Cell::deleted(),
            AtomicU64Cell::remove,
        ) {
            OverlayInsert::Inserted => Some(base_value),
            OverlayInsert::Occupied(removed) => removed,
        }
    }

    fn remove_atomic_empty_base(&self, key: &[u8], route_hash: u64) -> Option<NonMaxU64> {
        debug_assert!(self.base_is_empty);
        self.overlay
            .with_cell_prehashed(key, route_hash, |cell| cell.and_then(AtomicU64Cell::remove))
    }
}

pub(crate) const WRITER_STRIPES: usize = 4_096;
#[cfg(feature = "prepared-batch-gate")]
const PREPARED_BATCH_WRITER_GATES: usize = 16;
const WRITER_STRIPE_CLOSED: usize = 1 << (usize::BITS - 1);
const WRITER_STRIPE_DIRECT_BASE: usize = WRITER_STRIPE_CLOSED >> 1;
const WRITER_STRIPE_OVERLAY_BASE: usize = WRITER_STRIPE_DIRECT_BASE >> 1;
const WRITER_STRIPE_COUNT_BITS: u32 = if usize::BITS >= 64 { 16 } else { 8 };
const WRITER_STRIPE_COUNT_MASK: usize = (1 << WRITER_STRIPE_COUNT_BITS) - 1;
const WRITER_STRIPE_LEN_SHIFT: u32 = WRITER_STRIPE_COUNT_BITS;
const WRITER_STRIPE_LEN_BITS: u32 = usize::BITS - 3 - WRITER_STRIPE_COUNT_BITS;
const WRITER_STRIPE_LEN_VALUE_MASK: usize = (1 << WRITER_STRIPE_LEN_BITS) - 1;
const WRITER_STRIPE_LEN_MASK: usize = WRITER_STRIPE_LEN_VALUE_MASK << WRITER_STRIPE_LEN_SHIFT;
const WRITER_STRIPE_LEN_BIAS: usize = 1 << (WRITER_STRIPE_LEN_BITS - 1);
const WRITER_STRIPE_LEN_UNIT: usize = 1 << WRITER_STRIPE_LEN_SHIFT;
const WRITER_STRIPE_LEN_ZERO: usize = WRITER_STRIPE_LEN_BIAS << WRITER_STRIPE_LEN_SHIFT;

#[inline]
fn try_acquire_writer_stripe(counter: &AtomicUsize) -> Option<usize> {
    let mut state = counter.load(Ordering::Relaxed);
    loop {
        if state & WRITER_STRIPE_CLOSED != 0 {
            return None;
        }
        assert_ne!(
            state & WRITER_STRIPE_COUNT_MASK,
            WRITER_STRIPE_COUNT_MASK,
            "writer stripe counter overflow"
        );
        match counter.compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => return Some(state),
            Err(observed) => state = observed,
        }
    }
}

fn release_writer_stripe(counter: &AtomicUsize, amount: isize, released: &mut bool) {
    if amount == 0 {
        let previous = counter.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
        *released = true;
        return;
    }

    if usize::BITS >= 64 {
        let previous = match amount {
            1 => counter.fetch_add(WRITER_STRIPE_LEN_UNIT - 1, Ordering::Release),
            -1 => counter.fetch_sub(WRITER_STRIPE_LEN_UNIT + 1, Ordering::Release),
            _ => panic!("writer stripe release length must be one entry"),
        };
        debug_assert!(previous & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(previous & WRITER_STRIPE_COUNT_MASK > 0);
        let raw = (previous & WRITER_STRIPE_LEN_MASK) >> WRITER_STRIPE_LEN_SHIFT;
        debug_assert!(amount != 1 || raw < WRITER_STRIPE_LEN_VALUE_MASK);
        debug_assert!(amount != -1 || raw > 0);
        *released = true;
        return;
    }

    let mut state = counter.load(Ordering::Relaxed);
    loop {
        debug_assert!(state & WRITER_STRIPE_CLOSED == 0);
        debug_assert!(state & WRITER_STRIPE_COUNT_MASK > 0);
        let next_delta = writer_stripe_len_delta(state)
            .checked_add(amount)
            .expect("writer stripe length delta overflow");
        let next = ((state & !WRITER_STRIPE_LEN_MASK) | writer_stripe_len_bits(next_delta)) - 1;
        match counter.compare_exchange_weak(state, next, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => {
                *released = true;
                return;
            }
            Err(observed) => state = observed,
        }
    }
}

fn writer_stripe_len_delta(state: usize) -> isize {
    let raw = (state & WRITER_STRIPE_LEN_MASK) >> WRITER_STRIPE_LEN_SHIFT;
    isize::try_from(raw).expect("writer stripe length field fits isize")
        - isize::try_from(WRITER_STRIPE_LEN_BIAS).expect("writer stripe bias fits isize")
}

fn writer_stripe_len_bits(delta: isize) -> usize {
    let raw = isize::try_from(WRITER_STRIPE_LEN_BIAS)
        .expect("writer stripe bias fits isize")
        .checked_add(delta)
        .and_then(|raw| usize::try_from(raw).ok())
        .expect("writer stripe length delta exceeds packed range");
    assert!(
        raw <= WRITER_STRIPE_LEN_VALUE_MASK
            && writer_stripe_len_delta(raw << WRITER_STRIPE_LEN_SHIFT) == delta,
        "writer stripe length delta exceeds packed range"
    );
    raw << WRITER_STRIPE_LEN_SHIFT
}

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
    fn replace_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: &V,
    ) -> DirectMutation<Option<V>> {
        match self {
            Self::Frozen { map, .. } => map.replace_prepared(key, prepared, value),
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
                if map.len() == 0 {
                    return None;
                }
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

    fn sample_entry(&self, seed: u64, visit: &mut dyn FnMut(&[u8], &V) -> bool) -> bool {
        match self {
            Self::Frozen { map, .. } => map.sample_entry(seed, visit),
            Self::StableHybrid(_) | Self::Previous(_) => false,
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

impl GenerationBase<NonMaxU64, AtomicU64Cell, AtomicU64FrozenMap> {
    fn get_atomic_protected(&self, key: &[u8], key_hash: GenerationKeyHash) -> Option<NonMaxU64> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(key_hash.route())
                .then(|| map.get_hashed_protected(key, key_hash.frozen()))
                .flatten(),
            Self::StableHybrid(map) => map.with_value(key, copy_optional_non_max),
            Self::Previous(map) => {
                let stripe_mask =
                    u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
                let stripe = usize::try_from(key_hash.route() & stripe_mask)
                    .expect("masked writer stripe fits usize");
                map.get_atomic_protected(key, key_hash, stripe)
            }
        }
    }

    fn direct_get_or_insert_atomic(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
        value: NonMaxU64,
    ) -> Option<(NonMaxU64, bool)> {
        match self {
            Self::Frozen { map, membership } => membership
                .may_contain_hash(key_hash.route())
                .then(|| map.get_or_insert_hashed(key, key_hash.frozen(), value))
                .flatten(),
            Self::StableHybrid(_) | Self::Previous(_) => None,
        }
    }

    fn direct_remove_atomic(
        &self,
        key: &[u8],
        key_hash: GenerationKeyHash,
    ) -> DirectMutation<Option<NonMaxU64>> {
        match self {
            Self::Frozen { map, membership } => {
                if membership.may_contain_hash(key_hash.route()) {
                    map.remove_hashed(key, key_hash.frozen())
                } else {
                    DirectMutation::NotMember
                }
            }
            Self::StableHybrid(_) | Self::Previous(_) => DirectMutation::NotMember,
        }
    }

    fn probe_atomic(&self, key: &[u8], key_hash: GenerationKeyHash) -> AtomicBaseProbe {
        match self {
            Self::Frozen { map, membership } => {
                if !membership.may_contain_hash(key_hash.route()) {
                    return AtomicBaseProbe::NotMember;
                }
                match map.probe_hashed(key, key_hash.frozen()) {
                    AtomicFrozenProbe::NotMember => AtomicBaseProbe::NotMember,
                    AtomicFrozenProbe::Member { slot, value } => AtomicBaseProbe::Member {
                        map: Arc::clone(map),
                        slot,
                        value,
                    },
                }
            }
            Self::StableHybrid(map) => {
                AtomicBaseProbe::Logical(map.with_value(key, copy_optional_non_max))
            }
            Self::Previous(map) => {
                let stripe_mask =
                    u64::try_from(WRITER_STRIPES - 1).expect("writer stripe mask fits u64");
                let stripe = usize::try_from(key_hash.route() & stripe_mask)
                    .expect("masked writer stripe fits usize");
                AtomicBaseProbe::Logical(map.with_value_in_stripe(
                    key,
                    key_hash,
                    stripe,
                    copy_optional_non_max,
                ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_cache_revalidates_across_mutations_and_rebuild() {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            (0..64_u64).map(|value| (value.to_le_bytes(), NonMaxU64::new(value + 1).unwrap())),
            128,
        )
        .unwrap();
        let cached = map.read_cache();
        let existing = 7_u64.to_le_bytes();
        let inserted = 1_001_u64.to_le_bytes();
        let inserted_value = NonMaxU64::new(1_002).unwrap();

        assert_eq!(cached.get_protected(&existing), map.get(&existing));
        assert_eq!(
            map.insert(&inserted, inserted_value),
            InsertOutcome::Inserted
        );
        assert_eq!(cached.get_protected(&inserted), Some(inserted_value));

        map.rebuild(128).unwrap();
        cached.refresh();
        assert_eq!(cached.get_protected(&existing), map.get(&existing));
        assert_eq!(cached.get_protected(&inserted), Some(inserted_value));

        let existing_value = map.get(&existing);
        assert_eq!(map.remove(&existing), existing_value);
        assert_eq!(cached.get_protected(&existing), None);
    }

    #[test]
    fn read_cache_replaces_a_transitional_base_before_direct_updates() {
        let map = Arc::new(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                std::iter::once((b"counter".as_slice(), NonMaxU64::new(1).unwrap())),
                64,
            )
            .unwrap(),
        );
        let cached = map.read_cache();
        let old_generation = map.inner.current.load_full();
        let blocker = map.read_guard();
        let rebuilding = Arc::clone(&map);
        let join = std::thread::spawn(move || rebuilding.rebuild(64).unwrap());

        while Arc::ptr_eq(&old_generation, &map.inner.current.load_full()) {
            std::thread::yield_now();
        }
        assert!(matches!(
            map.inner.current.load().base.load().as_ref(),
            GenerationBase::Previous(_)
        ));
        assert_eq!(cached.get_protected(b"counter").unwrap().get(), 1);

        drop(blocker);
        join.join().unwrap();
        assert_eq!(
            map.update(b"counter", |_| NonMaxU64::new(2).unwrap()),
            Some(NonMaxU64::new(2).unwrap())
        );
        assert_eq!(cached.get_protected(b"counter").unwrap().get(), 2);
    }

    #[test]
    fn protected_read_rejects_a_stale_predecessor_after_direct_base_enable() {
        let map = Arc::new(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                std::iter::once((b"counter".as_slice(), NonMaxU64::new(1).unwrap())),
                64,
            )
            .unwrap(),
        );
        let old_generation = map.inner.current.load_full();
        let blocker = map.read_guard();
        let rebuilding = Arc::clone(&map);
        let join = std::thread::spawn(move || rebuilding.rebuild(64).unwrap());

        let generation = loop {
            let generation = map.inner.current.load_full();
            if !Arc::ptr_eq(&old_generation, &generation) {
                break generation;
            }
            std::thread::yield_now();
        };
        let stale_base = generation.base.load_full();
        assert!(matches!(stale_base.as_ref(), GenerationBase::Previous(_)));

        drop(blocker);
        join.join().unwrap();
        assert_eq!(
            map.update(b"counter", |_| NonMaxU64::new(2).unwrap()),
            Some(NonMaxU64::new(2).unwrap())
        );
        let (stripe, key_hash) = map.inner.writer_route(b"counter");
        assert_eq!(
            generation
                .get_atomic_protected_with_base(&stale_base, b"counter", key_hash, stripe)
                .unwrap()
                .get(),
            2
        );
    }

    #[cfg(feature = "prepared-keys")]
    #[test]
    fn prepared_read_cache_falls_back_exactly_across_rebuild_publication() {
        let map = Arc::new(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                std::iter::once((b"counter".as_slice(), NonMaxU64::new(1).unwrap())),
                64,
            )
            .unwrap(),
        );
        let prepared = map.prepare_key(b"counter");
        let cached = map.read_cache();
        assert_eq!(cached.get_prepared(b"counter", &prepared).unwrap().get(), 1);

        let old_generation = map.inner.current.load_full();
        let blocker = map.read_guard();
        let rebuilding = Arc::clone(&map);
        let join = std::thread::spawn(move || rebuilding.rebuild(64).unwrap());
        while Arc::ptr_eq(&old_generation, &map.inner.current.load_full()) {
            std::thread::yield_now();
        }
        assert_eq!(cached.get_prepared(b"counter", &prepared).unwrap().get(), 1);

        drop(blocker);
        join.join().unwrap();
        assert_eq!(
            map.update(b"counter", |_| NonMaxU64::new(2).unwrap()),
            Some(NonMaxU64::new(2).unwrap())
        );
        assert_eq!(cached.get_prepared(b"counter", &prepared).unwrap().get(), 2);
        let refreshed = map.prepare_key(b"counter");
        assert_eq!(
            cached.get_prepared(b"counter", &refreshed).unwrap().get(),
            2
        );
    }

    #[cfg(feature = "prepared-keys")]
    #[test]
    fn prepared_read_cache_recomputes_a_wrong_handles_writer_stripe() {
        let map = Arc::new(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                (0..128_u64).map(|value| (value.to_le_bytes(), NonMaxU64::new(value + 1).unwrap())),
                256,
            )
            .unwrap(),
        );
        let source = 0_u64.to_le_bytes();
        let source_stripe = map.inner.writer_route(&source).0;
        let target = (1..128_u64)
            .map(u64::to_le_bytes)
            .find(|key| map.inner.writer_route(key).0 != source_stripe)
            .expect("test keys cover more than one writer stripe");
        let prepared_for_source = map.prepare_key(&source);
        let cached = map.read_cache();

        let old_generation = map.inner.current.load_full();
        let blocker = map.read_guard();
        let rebuilding = Arc::clone(&map);
        let join = std::thread::spawn(move || rebuilding.rebuild(256).unwrap());
        while Arc::ptr_eq(&old_generation, &map.inner.current.load_full()) {
            std::thread::yield_now();
        }

        let updated = NonMaxU64::new(10_000).unwrap();
        assert_eq!(map.update(&target, |_| updated), Some(updated));
        drop(blocker);
        join.join().unwrap();

        assert_eq!(
            cached.get_prepared(&target, &prepared_for_source),
            Some(updated)
        );
        assert_eq!(
            cached.get_prepared(&target, &prepared_for_source),
            map.get(&target)
        );
    }

    #[test]
    fn read_cache_never_moves_backward_across_repeated_rebuilds() {
        const UPDATES: usize = 100_000;
        const REBUILDS: usize = 8;

        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            std::iter::once((b"counter".as_slice(), NonMaxU64::new(0).unwrap())),
            64,
        )
        .unwrap();
        let start = std::sync::Barrier::new(3);
        let updating = AtomicBool::new(true);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                for _ in 0..UPDATES {
                    map.update(b"counter", |value| NonMaxU64::new(value.get() + 1).unwrap())
                        .unwrap();
                }
                updating.store(false, Ordering::Release);
            });

            scope.spawn(|| {
                let cached = map.read_cache();
                start.wait();
                let mut previous = 0;
                while updating.load(Ordering::Acquire) {
                    let current = cached.get_protected(b"counter").unwrap().get();
                    assert!(current >= previous);
                    previous = current;
                }
                assert!(cached.get_protected(b"counter").unwrap().get() >= previous);
            });

            start.wait();
            for _ in 0..REBUILDS {
                map.rebuild(64).unwrap();
            }
        });

        assert_eq!(map.get(b"counter").unwrap().get(), UPDATES as u64);
    }

    #[test]
    fn read_guard_delays_cutover_and_refreshes_to_the_next_generation() {
        let map = Arc::new(
            LockFreeAtomicU64GenerationMap::try_from_entries(
                (0..64_u64).map(|value| (value.to_le_bytes(), NonMaxU64::new(value + 1).unwrap())),
                128,
            )
            .unwrap(),
        );
        let key = 7_u64.to_le_bytes();
        let mut guard = map.read_guard();
        assert_eq!(guard.get_protected(&key), map.get(&key));

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let rebuilding = Arc::clone(&map);
        let join = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = rebuilding.rebuild(128);
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        let inserted_key = 1_001_u64.to_le_bytes();
        let inserted_value = NonMaxU64::new(1_002).unwrap();
        assert_eq!(
            guard.get_or_insert(&inserted_key, inserted_value),
            inserted_value
        );
        let removed_key = 8_u64.to_le_bytes();
        let removed_value = NonMaxU64::new(9).unwrap();
        assert_eq!(guard.remove(&removed_key), Some(removed_value));
        guard.refresh();
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        join.join().unwrap();

        assert_eq!(guard.get_protected(&key), map.get(&key));
        assert_eq!(guard.get_protected(&inserted_key), Some(inserted_value));
        assert_eq!(guard.get_protected(&removed_key), None);
        assert_eq!(map.len(), 64);
    }

    #[test]
    fn frozen_misses_skip_an_untouched_overlay_until_the_first_overlay_write() {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            (0..64_u64).map(|value| (value.to_le_bytes(), NonMaxU64::new(value + 1).unwrap())),
            128,
        )
        .unwrap();
        let missing = 1_001_u64.to_le_bytes();
        let inserted = NonMaxU64::new(1_002).unwrap();

        assert!(
            !map.inner
                .current
                .load()
                .overlay_touched
                .load(Ordering::Acquire)
        );
        assert_eq!(map.get_protected(&missing), None);
        assert!(
            !map.inner
                .current
                .load()
                .overlay_touched
                .load(Ordering::Acquire)
        );

        assert_eq!(map.get_or_insert(&missing, inserted), inserted);
        assert!(
            map.inner
                .current
                .load()
                .overlay_touched
                .load(Ordering::Acquire)
        );
        assert_eq!(map.get_protected(&missing), Some(inserted));

        map.rebuild(128).unwrap();
        assert!(
            !map.inner
                .current
                .load()
                .overlay_touched
                .load(Ordering::Acquire)
        );
        assert_eq!(map.get_protected(&missing), Some(inserted));
    }

    #[test]
    fn guarded_insert_length_can_be_published_after_the_index_value() {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            std::iter::empty::<([u8; 8], NonMaxU64)>(),
            128,
        )
        .unwrap();
        let guard = map.read_guard();
        let key = 7_u64.to_le_bytes();
        let value = NonMaxU64::new(11).unwrap();

        let (current, stripe) = guard.get_or_insert_deferred_len(&key, value);
        assert_eq!(current, value);
        let stripe = stripe.expect("the first guarded insertion is deferred");
        assert_eq!(guard.get_protected(&key), Some(value));
        assert_eq!(map.len(), 0);

        let (current, duplicate_stripe) = guard.get_or_insert_deferred_len(&key, value);
        assert_eq!(current, value);
        assert_eq!(duplicate_stripe, None);

        guard.flush_deferred_insert_len(stripe, 1);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn guarded_removal_length_can_be_published_after_key_absence() {
        let key = 7_u64.to_le_bytes();
        let value = NonMaxU64::new(11).unwrap();
        let map = LockFreeAtomicU64GenerationMap::try_from_entries([(key, value)], 128).unwrap();
        let guard = map.read_guard();

        let (removed, stripe) = guard.remove_deferred_len(&key);
        assert_eq!(removed, Some(value));
        let stripe = stripe.expect("the guarded removal length is deferred");
        assert_eq!(guard.get_protected(&key), None);
        assert_eq!(map.len(), 1);

        let (removed, duplicate_stripe) = guard.remove_deferred_len(&key);
        assert_eq!(removed, None);
        assert_eq!(duplicate_stripe, None);

        guard.flush_deferred_remove_len(stripe, 1);
        assert_eq!(map.len(), 0);
    }

    #[cfg(feature = "prepared-keys")]
    #[test]
    fn prepared_guarded_removal_falls_back_exactly_after_rebuild() {
        let first = 7_u64.to_le_bytes();
        let second = 9_u64.to_le_bytes();
        let first_value = NonMaxU64::new(11).unwrap();
        let second_value = NonMaxU64::new(13).unwrap();
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            [(first, first_value), (second, second_value)],
            128,
        )
        .unwrap();
        let stale = map.prepare_key(&first);
        map.rebuild(128).unwrap();
        let guard = map.read_guard();

        let (removed, first_stripe) = guard.remove_prepared_deferred_len(&first, &stale);
        assert_eq!(removed, Some(first_value));
        let first_stripe = first_stripe.expect("stale prepared removal still defers length");
        assert_eq!(guard.get_protected(&first), None);
        assert_eq!(guard.get_protected(&second), Some(second_value));
        assert_eq!(map.len(), 2);

        let (removed, second_stripe) = guard.remove_prepared_deferred_len(&second, &stale);
        assert_eq!(removed, Some(second_value));
        assert!(second_stripe.is_some());
        guard.flush_deferred_remove_len(first_stripe, 2);
        assert_eq!(map.len(), 0);
    }

    #[cfg(not(feature = "shared-gx"))]
    #[test]
    fn mutable_only_guarded_insert_automatically_uses_route_hash_and_preserves_length() {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            std::iter::empty::<([u8; 8], NonMaxU64)>(),
            128,
        )
        .unwrap();
        let guard = map.read_guard();
        let key = 19_u64.to_le_bytes();
        let value = NonMaxU64::new(23).unwrap();

        assert!(guard.uses_route_only_admission());
        let (current, stripe) = guard.get_or_insert_deferred_len(&key, value);
        assert_eq!(current, value);
        let stripe = stripe.expect("the first route-only insertion is deferred");
        assert_eq!(map.len(), 0);

        let (current, duplicate_stripe) = guard.get_or_insert_deferred_len(&key, value);
        assert_eq!(current, value);
        assert_eq!(duplicate_stripe, None);

        guard.flush_deferred_insert_len(stripe, 1);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&key), Some(value));
    }

    #[test]
    fn deferred_insert_length_balances_a_removal_before_flush() {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries(
            std::iter::empty::<([u8; 8], NonMaxU64)>(),
            128,
        )
        .unwrap();
        let guard = map.read_guard();
        let retained_key = 13_u64.to_le_bytes();
        let retained_value = NonMaxU64::new(17).unwrap();
        let (_, retained_stripe) = guard.get_or_insert_deferred_len(&retained_key, retained_value);
        let retained_stripe = retained_stripe.unwrap();
        let removed_key = (14_u64..=u64::MAX)
            .map(u64::to_le_bytes)
            .find(|key| map.inner.writer_route(key).0 != retained_stripe)
            .unwrap();
        let removed_value = NonMaxU64::new(19).unwrap();
        let (_, removed_stripe) = guard.get_or_insert_deferred_len(&removed_key, removed_value);
        assert_ne!(removed_stripe, Some(retained_stripe));

        assert_eq!(guard.remove(&removed_key), Some(removed_value));
        assert_eq!(guard.get_protected(&removed_key), None);
        guard.flush_deferred_insert_len(retained_stripe, 2);
        assert_eq!(map.len(), 1);
        assert_eq!(guard.get_protected(&retained_key), Some(retained_value));
    }

    #[test]
    fn checked_writer_acquire_counts_open_stripes_and_never_publishes_overflow() {
        let counter = AtomicUsize::new(WRITER_STRIPE_LEN_ZERO);

        assert_eq!(
            try_acquire_writer_stripe(&counter),
            Some(WRITER_STRIPE_LEN_ZERO)
        );
        assert_eq!(counter.load(Ordering::Relaxed), WRITER_STRIPE_LEN_ZERO + 1);

        let mut released = false;
        release_writer_stripe(&counter, 0, &mut released);
        assert!(released);
        assert_eq!(counter.load(Ordering::Relaxed), WRITER_STRIPE_LEN_ZERO);

        let closed = WRITER_STRIPE_LEN_ZERO | WRITER_STRIPE_CLOSED;
        counter.store(closed, Ordering::Relaxed);
        assert_eq!(try_acquire_writer_stripe(&counter), None);
        assert_eq!(counter.load(Ordering::Relaxed), closed);

        let full = WRITER_STRIPE_LEN_ZERO | WRITER_STRIPE_COUNT_MASK;
        counter.store(full, Ordering::Relaxed);
        assert!(
            std::panic::catch_unwind(|| try_acquire_writer_stripe(&counter)).is_err(),
            "a full stripe must reject another guard"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed),
            full,
            "overflow rejection must never expose a zero active count or alter packed length bits"
        );
    }

    #[test]
    fn writer_stripe_length_delta_round_trips_without_touching_route_state() {
        let maximum =
            isize::try_from(WRITER_STRIPE_LEN_BIAS - 1).expect("packed positive delta fits isize");
        let minimum =
            -isize::try_from(WRITER_STRIPE_LEN_BIAS).expect("packed negative delta fits isize");
        let route_state =
            WRITER_STRIPE_DIRECT_BASE | WRITER_STRIPE_OVERLAY_BASE | WRITER_STRIPE_COUNT_MASK;

        for delta in [minimum, -1, 0, 1, maximum] {
            let state = route_state | writer_stripe_len_bits(delta);
            assert_eq!(writer_stripe_len_delta(state), delta);
            assert_eq!(
                state
                    & (WRITER_STRIPE_CLOSED
                        | WRITER_STRIPE_DIRECT_BASE
                        | WRITER_STRIPE_OVERLAY_BASE
                        | WRITER_STRIPE_COUNT_MASK),
                route_state
            );
        }
    }
}
