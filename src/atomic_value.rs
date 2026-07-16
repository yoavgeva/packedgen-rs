use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frozen_map::{Digest, key_digest};
use crate::generation_hash::{GenerationHashBuilder, GenerationKeyHash};
use crate::overlay_cell::{GenerationCell, StableCell};
use crate::{
    AtomicGenerationBaseFilter, FrozenBuildError, FrozenIndexBackend, FrozenMapStats,
    FrozenPackedMap, InsertOutcome,
};

const DELETED: u64 = u64::MAX;
#[cfg(feature = "prepared-keys")]
static NEXT_PREPARED_BASE_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) enum DirectMutation<T> {
    NotMember,
    Handled(T),
}

/// A `u64` value that can be stored directly in an atomic generation cell.
///
/// All `u64` values except [`u64::MAX`] are representable. The excluded bit
/// pattern is used internally as the deletion marker, allowing reads, updates,
/// inserts, and deletes to remain allocation-free after a key reaches the
/// mutable overlay.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct NonMaxU64(u64);

impl NonMaxU64 {
    /// Creates a value unless `value` is the reserved deletion bit pattern.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        if value == DELETED {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns the stored integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u32> for NonMaxU64 {
    fn from(value: u32) -> Self {
        Self(u64::from(value))
    }
}

impl TryFrom<u64> for NonMaxU64 {
    type Error = NonMaxU64Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(NonMaxU64Error)
    }
}

/// Error returned when attempting to use `u64::MAX` as a [`NonMaxU64`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonMaxU64Error;

impl fmt::Display for NonMaxU64Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("u64::MAX is reserved as the atomic deletion marker")
    }
}

impl std::error::Error for NonMaxU64Error {}

pub(crate) struct AtomicU64Cell {
    value: AtomicU64,
}

pub(crate) struct AtomicU64FrozenMap {
    keys: FrozenPackedMap<()>,
    values: Box<[AtomicU64]>,
    #[cfg(feature = "prepared-keys")]
    base_id: u64,
}

#[cfg(feature = "prepared-keys")]
#[derive(Clone, Copy)]
pub(crate) struct AtomicPreparedSlot {
    pub(crate) base_id: u64,
    pub(crate) slot: usize,
}

impl AtomicU64FrozenMap {
    pub(crate) fn try_from_entries<I, K>(entries: I) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy(entries, AtomicGenerationBaseFilter::Disabled)
    }

    pub(crate) fn try_from_entries_with_policy<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        Self::try_from_entries_with_policy_and_index(entries, policy, FrozenIndexBackend::PtrHash)
    }

    pub(crate) fn try_from_entries_with_policy_and_index<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        let staged = entries.into_iter().collect::<Vec<_>>();
        Self::try_from_staged(&staged, policy, index_backend, key_digest)
    }

    pub(crate) fn try_from_entries_with_policy_index_and_hash<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        hash_builder: &GenerationHashBuilder,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        let staged = entries.into_iter().collect::<Vec<_>>();
        Self::try_from_staged(&staged, policy, index_backend, |key| {
            GenerationKeyHash::new(hash_builder, key)
                .frozen()
                .unwrap_or_else(|| key_digest(key))
        })
    }

    fn try_from_staged<K>(
        staged: &[(K, NonMaxU64)],
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        mut digest: impl FnMut(&[u8]) -> Digest,
    ) -> Result<Self, FrozenBuildError>
    where
        K: AsRef<[u8]>,
    {
        let key_entries = || staged.iter().map(|(key, _)| (key.as_ref(), ()));
        let keys = match policy {
            AtomicGenerationBaseFilter::EmbeddedFingerprint => {
                FrozenPackedMap::try_from_entries_with_embedded_key_tag_index_and_digest(
                    key_entries(),
                    index_backend,
                    &mut digest,
                )?
            }
            AtomicGenerationBaseFilter::Disabled | AtomicGenerationBaseFilter::OneBytePerEntry => {
                FrozenPackedMap::try_from_entries_with_index_backend_and_digest(
                    key_entries(),
                    index_backend,
                    &mut digest,
                )?
            }
        };
        let mut values = (0..staged.len())
            .map(|_| AtomicU64::new(DELETED))
            .collect::<Vec<_>>();
        for (key, value) in staged {
            let (slot, ()) = keys
                .get_indexed_with_digest(key.as_ref(), digest(key.as_ref()))
                .expect("every staged atomic key must resolve after construction");
            values[slot] = AtomicU64::new(value.get());
        }
        Ok(Self {
            keys,
            values: values.into_boxed_slice(),
            #[cfg(feature = "prepared-keys")]
            base_id: NEXT_PREPARED_BASE_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    pub(crate) fn with_value<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(Option<&NonMaxU64>) -> R,
    ) -> R {
        self.with_value_hashed(key, None, read)
    }

    pub(crate) fn with_value_hashed<R>(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        read: impl FnOnce(Option<&NonMaxU64>) -> R,
    ) -> R {
        let value = self
            .slot_hashed(key, digest)
            .and_then(|slot| NonMaxU64::new(self.values[slot].load(Ordering::Acquire)));
        read(value.as_ref())
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn prepare_slot(&self, key: &[u8], digest: Digest) -> Option<AtomicPreparedSlot> {
        self.keys
            .get_indexed_with_digest(key, digest)
            .map(|(slot, ())| AtomicPreparedSlot {
                base_id: self.base_id,
                slot,
            })
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn with_prepared_value<R>(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        read: impl FnOnce(Option<&NonMaxU64>) -> R,
    ) -> DirectMutation<R> {
        if prepared.base_id != self.base_id || !self.keys.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        let value = NonMaxU64::new(self.values[prepared.slot].load(Ordering::Acquire));
        DirectMutation::Handled(read(value.as_ref()))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn update_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.keys.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        DirectMutation::Handled(update_atomic(&self.values[prepared.slot], update))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn insert_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.keys.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        let previous = self.values[prepared.slot].swap(value.get(), Ordering::AcqRel);
        DirectMutation::Handled(
            NonMaxU64::new(previous).map_or(InsertOutcome::Inserted, InsertOutcome::Replaced),
        )
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn remove_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
    ) -> DirectMutation<Option<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.keys.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        DirectMutation::Handled(NonMaxU64::new(
            self.values[prepared.slot].swap(DELETED, Ordering::AcqRel),
        ))
    }

    pub(crate) fn insert_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: NonMaxU64,
    ) -> Option<InsertOutcome<NonMaxU64>> {
        let slot = self.slot_hashed(key, digest)?;
        let previous = self.values[slot].swap(value.get(), Ordering::AcqRel);
        Some(NonMaxU64::new(previous).map_or(InsertOutcome::Inserted, InsertOutcome::Replaced))
    }

    pub(crate) fn insert_new_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: NonMaxU64,
    ) -> Option<bool> {
        let slot = self.slot_hashed(key, digest)?;
        Some(
            self.values[slot]
                .compare_exchange(DELETED, value.get(), Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
        )
    }

    pub(crate) fn update_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some(slot) = self.slot_hashed(key, digest) else {
            return DirectMutation::NotMember;
        };
        DirectMutation::Handled(update_atomic(&self.values[slot], update))
    }

    pub(crate) fn upsert_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        insert_value: NonMaxU64,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<(NonMaxU64, bool)> {
        let slot = self.slot_hashed(key, digest)?;
        Some(upsert_atomic(&self.values[slot], insert_value, update))
    }

    pub(crate) fn remove_if_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        predicate: &impl Fn(&NonMaxU64) -> bool,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some(slot) = self.slot_hashed(key, digest) else {
            return DirectMutation::NotMember;
        };
        DirectMutation::Handled(remove_atomic(&self.values[slot], predicate))
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &NonMaxU64)) {
        self.keys.for_each_indexed_entry(|slot, key, ()| {
            if let Some(value) = NonMaxU64::new(self.values[slot].load(Ordering::Acquire)) {
                visit(key, &value);
            }
        });
    }

    pub(crate) fn stats(&self) -> FrozenMapStats {
        let mut stats = self.keys.stats();
        stats.slot_bytes = stats
            .slot_bytes
            .saturating_add(size_of_val(self.values.as_ref()));
        stats
    }

    fn slot_hashed(&self, key: &[u8], digest: Option<Digest>) -> Option<usize> {
        digest
            .map_or_else(
                || self.keys.get_indexed(key),
                |digest| self.keys.get_indexed_with_digest(key, digest),
            )
            .map(|(slot, ())| slot)
    }
}

impl StableCell for AtomicU64Cell {
    fn deleted() -> Self {
        Self {
            value: AtomicU64::new(DELETED),
        }
    }

    fn initialize_from(&self, source: Self) {
        self.value
            .store(source.value.into_inner(), Ordering::Relaxed);
    }
}

impl GenerationCell<NonMaxU64> for AtomicU64Cell {
    const COMPACT_FIXED32_OVERLAY: bool = true;

    fn present(value: NonMaxU64) -> Self {
        Self {
            value: AtomicU64::new(value.get()),
        }
    }

    fn with_value<R>(&self, read: impl FnOnce(Option<&NonMaxU64>) -> R) -> R {
        let value = NonMaxU64::new(self.value.load(Ordering::Acquire));
        read(value.as_ref())
    }

    fn replace(&self, value: NonMaxU64) -> Option<NonMaxU64> {
        NonMaxU64::new(self.value.swap(value.get(), Ordering::AcqRel))
    }

    fn insert_new(&self, value: NonMaxU64) -> bool {
        self.value
            .compare_exchange(DELETED, value.get(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn update(&self, update: &impl Fn(&NonMaxU64) -> NonMaxU64) -> Option<NonMaxU64> {
        update_atomic(&self.value, update)
    }

    fn upsert(
        &self,
        insert_value: &NonMaxU64,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> (NonMaxU64, bool) {
        upsert_atomic(&self.value, *insert_value, update)
    }

    fn remove_if(&self, predicate: &impl Fn(&NonMaxU64) -> bool) -> Option<NonMaxU64> {
        remove_atomic(&self.value, predicate)
    }
}

fn update_atomic(
    value: &AtomicU64,
    update: &impl Fn(&NonMaxU64) -> NonMaxU64,
) -> Option<NonMaxU64> {
    let mut current = value.load(Ordering::Acquire);
    loop {
        let current_value = NonMaxU64::new(current)?;
        let next = update(&current_value);
        match value.compare_exchange_weak(current, next.get(), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return Some(next),
            Err(observed) => current = observed,
        }
    }
}

fn upsert_atomic(
    value: &AtomicU64,
    insert_value: NonMaxU64,
    update: &impl Fn(&NonMaxU64) -> NonMaxU64,
) -> (NonMaxU64, bool) {
    let mut current = value.load(Ordering::Acquire);
    loop {
        let became_live = current == DELETED;
        let next = NonMaxU64::new(current).map_or(insert_value, |value| update(&value));
        match value.compare_exchange_weak(current, next.get(), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return (next, became_live),
            Err(observed) => current = observed,
        }
    }
}

fn remove_atomic(value: &AtomicU64, predicate: &impl Fn(&NonMaxU64) -> bool) -> Option<NonMaxU64> {
    let mut current = value.load(Ordering::Acquire);
    loop {
        let current_value = NonMaxU64::new(current)?;
        if !predicate(&current_value) {
            return None;
        }
        match value.compare_exchange_weak(current, DELETED, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Some(current_value),
            Err(observed) => current = observed,
        }
    }
}
