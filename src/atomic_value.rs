use std::fmt;
use std::hint::spin_loop;
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

pub(crate) enum AtomicFrozenProbe {
    NotMember,
    Member {
        slot: usize,
        value: Option<NonMaxU64>,
    },
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
    entries: FrozenPackedMap<AtomicU64>,
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
        Self::try_from_entries_with_digest(entries, policy, index_backend, key_digest)
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
        Self::try_from_entries_with_digest(entries, policy, index_backend, |key| {
            GenerationKeyHash::new(hash_builder, key)
                .frozen()
                .unwrap_or_else(|| key_digest(key))
        })
    }

    fn try_from_entries_with_digest<I, K>(
        entries: I,
        policy: AtomicGenerationBaseFilter,
        index_backend: FrozenIndexBackend,
        mut digest: impl FnMut(&[u8]) -> Digest,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, NonMaxU64)>,
        K: AsRef<[u8]>,
    {
        let atomic_entries = entries
            .into_iter()
            .map(|(key, value)| (key, AtomicU64::new(value.get())));
        let entries = match policy {
            AtomicGenerationBaseFilter::EmbeddedFingerprint => {
                FrozenPackedMap::try_from_entries_with_embedded_key_tag_index_and_digest(
                    atomic_entries,
                    index_backend,
                    &mut digest,
                )?
            }
            AtomicGenerationBaseFilter::Disabled | AtomicGenerationBaseFilter::OneBytePerEntry => {
                FrozenPackedMap::try_from_entries_with_index_backend_and_digest(
                    atomic_entries,
                    index_backend,
                    &mut digest,
                )?
            }
        };
        Ok(Self {
            entries,
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
            .and_then(|slot| NonMaxU64::new(self.value(slot).load(Ordering::Acquire)));
        read(value.as_ref())
    }

    pub(crate) fn get_hashed_protected(
        &self,
        key: &[u8],
        digest: Option<Digest>,
    ) -> Option<NonMaxU64> {
        let slot = self.slot_hashed(key, digest)?;
        NonMaxU64::new(self.value(slot).load(Ordering::SeqCst))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn prepare_slot(&self, key: &[u8], digest: Digest) -> Option<AtomicPreparedSlot> {
        self.entries
            .get_indexed_with_digest(key, digest)
            .map(|(slot, _)| AtomicPreparedSlot {
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
        if prepared.base_id != self.base_id || !self.entries.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        let value = NonMaxU64::new(self.value(prepared.slot).load(Ordering::Acquire));
        DirectMutation::Handled(read(value.as_ref()))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn update_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.entries.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        DirectMutation::Handled(update_atomic(self.value(prepared.slot), update))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn replace_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: NonMaxU64,
    ) -> DirectMutation<Option<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.entries.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        DirectMutation::Handled(replace_existing_atomic(self.value(prepared.slot), value))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn insert_prepared(
        &self,
        key: &[u8],
        prepared: AtomicPreparedSlot,
        value: NonMaxU64,
    ) -> DirectMutation<InsertOutcome<NonMaxU64>> {
        if prepared.base_id != self.base_id || !self.entries.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        let previous = self
            .value(prepared.slot)
            .swap(value.get(), Ordering::AcqRel);
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
        if prepared.base_id != self.base_id || !self.entries.slot_matches(prepared.slot, key) {
            return DirectMutation::NotMember;
        }
        DirectMutation::Handled(NonMaxU64::new(
            self.value(prepared.slot).swap(DELETED, Ordering::AcqRel),
        ))
    }

    pub(crate) fn insert_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: NonMaxU64,
    ) -> Option<InsertOutcome<NonMaxU64>> {
        let slot = self.slot_hashed(key, digest)?;
        let previous = self.value(slot).swap(value.get(), Ordering::AcqRel);
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
            self.value(slot)
                .compare_exchange(DELETED, value.get(), Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
        )
    }

    pub(crate) fn get_or_insert_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        value: NonMaxU64,
    ) -> Option<(NonMaxU64, bool)> {
        let slot = self.slot_hashed(key, digest)?;
        Some(self.get_or_insert_slot(slot, value))
    }

    pub(crate) fn probe_hashed(&self, key: &[u8], digest: Option<Digest>) -> AtomicFrozenProbe {
        let Some(slot) = self.slot_hashed(key, digest) else {
            return AtomicFrozenProbe::NotMember;
        };
        AtomicFrozenProbe::Member {
            slot,
            value: NonMaxU64::new(self.value(slot).load(Ordering::Acquire)),
        }
    }

    pub(crate) fn insert_slot(&self, slot: usize, value: NonMaxU64) -> InsertOutcome<NonMaxU64> {
        let previous = self.value(slot).swap(value.get(), Ordering::AcqRel);
        NonMaxU64::new(previous).map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
    }

    pub(crate) fn insert_new_slot(&self, slot: usize, value: NonMaxU64) -> bool {
        self.value(slot)
            .compare_exchange(DELETED, value.get(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn get_or_insert_slot(&self, slot: usize, value: NonMaxU64) -> (NonMaxU64, bool) {
        get_or_insert_atomic(self.value(slot), value)
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
        DirectMutation::Handled(update_atomic(self.value(slot), update))
    }

    pub(crate) fn upsert_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
        insert_value: NonMaxU64,
        update: &impl Fn(&NonMaxU64) -> NonMaxU64,
    ) -> Option<(NonMaxU64, bool)> {
        let slot = self.slot_hashed(key, digest)?;
        Some(upsert_atomic(self.value(slot), insert_value, update))
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
        DirectMutation::Handled(remove_atomic(self.value(slot), predicate))
    }

    pub(crate) fn remove_hashed(
        &self,
        key: &[u8],
        digest: Option<Digest>,
    ) -> DirectMutation<Option<NonMaxU64>> {
        let Some(slot) = self.slot_hashed(key, digest) else {
            return DirectMutation::NotMember;
        };
        DirectMutation::Handled(NonMaxU64::new(
            self.value(slot).swap(DELETED, Ordering::AcqRel),
        ))
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &NonMaxU64)) {
        self.entries.for_each_entry(|key, atomic| {
            if let Some(value) = NonMaxU64::new(atomic.load(Ordering::Acquire)) {
                visit(key, &value);
            }
        });
    }

    pub(crate) fn sample_entry(
        &self,
        seed: u64,
        visit: &mut dyn FnMut(&[u8], &NonMaxU64) -> bool,
    ) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        let start = usize::try_from(seed % self.entries.len() as u64)
            .expect("frozen sample slot fits usize");
        for attempt in 0..8 {
            let slot = (start + attempt) % self.entries.len();
            let (key, atomic) = self
                .entries
                .indexed_entry(slot)
                .expect("sample slot is below frozen length");
            let Some(value) = NonMaxU64::new(atomic.load(Ordering::Acquire)) else {
                continue;
            };
            if visit(key, &value) {
                return true;
            }
        }
        false
    }

    pub(crate) fn stats(&self) -> FrozenMapStats {
        self.entries.stats()
    }

    fn slot_hashed(&self, key: &[u8], digest: Option<Digest>) -> Option<usize> {
        digest
            .map_or_else(
                || self.entries.get_indexed(key),
                |digest| self.entries.get_indexed_with_digest(key, digest),
            )
            .map(|(slot, _)| slot)
    }

    fn value(&self, slot: usize) -> &AtomicU64 {
        self.entries
            .indexed_value(slot)
            .expect("atomic frozen slot is in bounds")
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

impl AtomicU64Cell {
    pub(crate) fn get_protected(&self) -> Option<NonMaxU64> {
        NonMaxU64::new(self.value.load(Ordering::SeqCst))
    }

    pub(crate) fn get_or_insert(&self, value: NonMaxU64) -> (NonMaxU64, bool) {
        get_or_insert_atomic(&self.value, value)
    }

    pub(crate) fn remove(&self) -> Option<NonMaxU64> {
        NonMaxU64::new(self.value.swap(DELETED, Ordering::AcqRel))
    }
}

fn get_or_insert_atomic(value: &AtomicU64, insert_value: NonMaxU64) -> (NonMaxU64, bool) {
    match value.compare_exchange(
        DELETED,
        insert_value.get(),
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => (insert_value, true),
        Err(current) => (
            NonMaxU64::new(current).expect("deleted compare-exchange observed a live value"),
            false,
        ),
    }
}

#[cfg(feature = "prepared-keys")]
fn replace_existing_atomic(value: &AtomicU64, replacement: NonMaxU64) -> Option<NonMaxU64> {
    let mut current = value.load(Ordering::Acquire);
    loop {
        let previous = NonMaxU64::new(current)?;
        match value.compare_exchange_weak(
            current,
            replacement.get(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(previous),
            Err(observed) => current = observed,
        }
    }
}

fn update_atomic(
    value: &AtomicU64,
    update: &impl Fn(&NonMaxU64) -> NonMaxU64,
) -> Option<NonMaxU64> {
    let mut current = value.load(Ordering::Acquire);
    let mut backoff = 1;
    loop {
        let current_value = NonMaxU64::new(current)?;
        let next = update(&current_value);
        match value.compare_exchange_weak(current, next.get(), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return Some(next),
            Err(observed) => {
                current = observed;
                // Let the current cache-line owner complete under contention.
                // The cap preserves distributed-update throughput while
                // preventing a hot key from becoming an unbounded CAS storm.
                for _ in 0..backoff {
                    spin_loop();
                }
                backoff = (backoff * 2).min(8);
            }
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
