use std::sync::atomic::{AtomicIsize, Ordering};

use hashbrown::DefaultHashBuilder;
use papaya::HashMap;

use crate::overlay_cell::OverlayCell;
use crate::{FrozenBuildError, FrozenMapStats, FrozenPackedMap, InsertOutcome};

/// Packed immutable base with a lock-free mutable overlay.
///
/// Reads first pin the small Papaya overlay. An overlay value wins, an overlay
/// deletion hides the base, and an overlay miss falls through to the immutable
/// packed generation. Neither step takes a lock. Writes use Papaya's atomic
/// compare-and-swap operations, so many writers can update the overlay.
///
/// This layout targets read-mostly tables with bounded churn. Overlay records
/// own boxed keys, so callers should rebuild a new packed generation before the
/// overlay becomes a large fraction of the base.
pub struct LockFreeHybridMap<V> {
    base: FrozenPackedMap<V>,
    overlay: HashMap<Box<[u8]>, OverlayCell<V>, DefaultHashBuilder>,
    logical_len: AtomicIsize,
}

impl<V> LockFreeHybridMap<V> {
    /// Creates a lock-free overlay around an immutable packed generation.
    #[must_use]
    pub fn from_frozen(base: FrozenPackedMap<V>) -> Self {
        Self::with_overlay_capacity(base, 0)
    }

    /// Creates a hybrid map with reserved mutable-overlay capacity.
    #[must_use]
    pub fn with_overlay_capacity(base: FrozenPackedMap<V>, overlay_capacity: usize) -> Self {
        let logical_len = isize::try_from(base.len()).unwrap_or(isize::MAX);
        Self {
            base,
            overlay: HashMap::with_capacity_and_hasher(
                overlay_capacity,
                DefaultHashBuilder::default(),
            ),
            logical_len: AtomicIsize::new(logical_len),
        }
    }

    /// Builds the immutable base from unique binary-key entries.
    ///
    /// # Errors
    ///
    /// Returns a frozen-map construction error for duplicate keys, allocation
    /// failure, or a perfect-index construction failure.
    pub fn try_from_entries<I, K>(
        entries: I,
        overlay_capacity: usize,
    ) -> Result<Self, FrozenBuildError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
    {
        FrozenPackedMap::try_from_entries(entries)
            .map(|base| Self::with_overlay_capacity(base, overlay_capacity))
    }

    /// Clones the latest logical value out through the lock-free read path.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value(key, |value| value.cloned())
    }

    /// Runs `read` with the latest logical value without acquiring a lock.
    ///
    /// Overlay reclamation remains pinned for the closure. The immutable base
    /// requires no guard.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        let overlay = self.overlay.pin();
        match overlay.get(key) {
            Some(cell) => cell.with_value(read),
            None => read(self.base.get(key)),
        }
    }

    /// Returns whether the logical key is present.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.with_value(key, |value| value.is_some())
    }

    /// Atomically inserts or replaces a logical value.
    pub fn insert(&self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        let overlay = self.overlay.pin();
        if let Some(cell) = overlay.get(key) {
            return if let Some(previous) = cell.replace(value) {
                InsertOutcome::Replaced(previous)
            } else {
                self.adjust_len(1);
                InsertOutcome::Inserted
            };
        }
        let base_value = self.base.get(key).cloned();
        match overlay.try_insert(key.into(), OverlayCell::present(value.clone())) {
            Ok(_) => base_value.map_or_else(
                || {
                    self.adjust_len(1);
                    InsertOutcome::Inserted
                },
                InsertOutcome::Replaced,
            ),
            Err(error) => {
                if let Some(previous) = error.current.replace(value) {
                    InsertOutcome::Replaced(previous)
                } else {
                    self.adjust_len(1);
                    InsertOutcome::Inserted
                }
            }
        }
    }

    /// Inserts only when the logical key is absent.
    ///
    /// Exactly one concurrent inserter can change a deleted cell back to live or
    /// publish a new overlay record.
    pub fn insert_new(&self, key: &[u8], value: V) -> bool
    where
        V: Clone,
    {
        let overlay = self.overlay.pin();
        if let Some(cell) = overlay.get(key) {
            let inserted = cell.insert_new(value);
            if inserted {
                self.adjust_len(1);
            }
            return inserted;
        }
        if self.base.contains_key(key) {
            return false;
        }
        match overlay.try_insert(key.into(), OverlayCell::present(value.clone())) {
            Ok(_) => {
                self.adjust_len(1);
                true
            }
            Err(error) => {
                let inserted = error.current.insert_new(value);
                if inserted {
                    self.adjust_len(1);
                }
                inserted
            }
        }
    }

    /// Atomically transforms an existing logical value.
    ///
    /// `update` may be retried after a CAS race and must be pure. Base values
    /// are promoted into the overlay on their first update.
    pub fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        let overlay = self.overlay.pin();
        if let Some(cell) = overlay.get(key) {
            return cell.update(&update);
        }
        let base_value = self.base.get(key)?;
        let next = update(base_value);
        match overlay.try_insert(key.into(), OverlayCell::present(next.clone())) {
            Ok(_) => Some(next),
            Err(error) => error.current.update(&update),
        }
    }

    /// Atomically updates a logical value or inserts the supplied default.
    ///
    /// `update` may be retried and must be pure.
    pub fn upsert(&self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        let overlay = self.overlay.pin();
        if let Some(cell) = overlay.get(key) {
            let (value, became_live) = cell.upsert(&insert_value, &update);
            if became_live {
                self.adjust_len(1);
            }
            return value;
        }
        let base_value = self.base.get(key);
        let next = base_value.map_or_else(|| insert_value.clone(), &update);
        match overlay.try_insert(key.into(), OverlayCell::present(next.clone())) {
            Ok(_) => {
                if base_value.is_none() {
                    self.adjust_len(1);
                }
                next
            }
            Err(error) => {
                let (value, became_live) = error.current.upsert(&insert_value, &update);
                if became_live {
                    self.adjust_len(1);
                }
                value
            }
        }
    }

    /// Atomically removes a logical value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.remove_if(key, |_| true)
    }

    /// Atomically removes a logical value accepted by a retry-safe predicate.
    ///
    /// The predicate may run repeatedly after CAS races and must be pure.
    #[must_use]
    pub fn remove_if(&self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        let overlay = self.overlay.pin();
        if let Some(cell) = overlay.get(key) {
            let removed = cell.remove_if(&predicate);
            if removed.is_some() {
                self.adjust_len(-1);
            }
            return removed;
        }
        let base_value = self
            .base
            .get(key)
            .filter(|value| predicate(value))
            .cloned()?;
        let removed = match overlay.try_insert(key.into(), OverlayCell::deleted()) {
            Ok(_) => Some(base_value),
            Err(error) => error.current.remove_if(&predicate),
        };
        if removed.is_some() {
            self.adjust_len(-1);
        }
        removed
    }

    /// Returns the logical base-plus-overlay entry count.
    ///
    /// The result is exact after completed operations and weakly consistent
    /// while concurrent writers are between CAS publication and accounting.
    #[must_use]
    pub fn len(&self) -> usize {
        usize::try_from(self.logical_len.load(Ordering::Acquire).max(0)).unwrap_or(usize::MAX)
    }

    /// Returns whether the observed logical map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the immutable packed generation.
    #[must_use]
    pub const fn frozen(&self) -> &FrozenPackedMap<V> {
        &self.base
    }

    /// Captures base and overlay population statistics.
    #[must_use]
    pub fn stats(&self) -> LockFreeHybridStats {
        let overlay = self.overlay.pin();
        let mut present = 0;
        let mut deleted = 0;
        for (_, value) in &overlay {
            value.with_value(|value| {
                if value.is_some() {
                    present += 1;
                } else {
                    deleted += 1;
                }
            });
        }
        LockFreeHybridStats {
            len: self.len(),
            base: self.base.stats(),
            overlay_records: present + deleted,
            overlay_present: present,
            overlay_deleted: deleted,
        }
    }

    pub(crate) fn for_each_entry(&self, visit: &mut dyn FnMut(&[u8], &V)) {
        let overlay = self.overlay.pin();
        self.base
            .for_each_entry(|key, base_value| match overlay.get(key) {
                Some(cell) => cell.with_value(|value| {
                    if let Some(value) = value {
                        visit(key, value);
                    }
                }),
                None => visit(key, base_value),
            });
        for (key, value) in &overlay {
            if self.base.contains_key(key) {
                continue;
            }
            value.with_value(|value| {
                if let Some(value) = value {
                    visit(key, value);
                }
            });
        }
    }

    fn adjust_len(&self, amount: isize) {
        self.logical_len.fetch_add(amount, Ordering::AcqRel);
    }
}

/// Observable generation populations for [`LockFreeHybridMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LockFreeHybridStats {
    /// Logical live records.
    pub len: usize,
    /// Immutable packed-generation memory components.
    pub base: FrozenMapStats,
    /// Total lock-free overlay records, including deletion markers.
    pub overlay_records: usize,
    /// Overlay records containing current values.
    pub overlay_present: usize,
    /// Overlay records hiding immutable base values.
    pub overlay_deleted: usize,
}
