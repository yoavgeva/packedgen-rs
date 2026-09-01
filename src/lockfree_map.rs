use hashbrown::DefaultHashBuilder;
use papaya::HashMap;

use crate::InsertOutcome;

/// Lock-free concurrent map for arbitrary owned binary keys.
///
/// Point reads pin Papaya's epoch collector instead of taking a shard lock.
/// Returned references remain protected only for the duration of the internal
/// pin, so the default API clones values out like ETS lookup. The
/// [`Self::with_value`] closure avoids that clone while keeping reclamation
/// pinned; unlike a lock guard, the pin cannot deadlock writers.
///
/// This backend establishes the repository's lock-free correctness and speed
/// baseline. It owns one boxed allocation per key and is therefore expected to
/// use more RAM than the packed sharded backend.
pub struct LockFreeBinaryMap<V> {
    inner: HashMap<Box<[u8]>, V, DefaultHashBuilder>,
}

impl<V> LockFreeBinaryMap<V> {
    /// Creates an empty map without initial table allocation.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: HashMap::with_hasher(DefaultHashBuilder::default()),
        }
    }

    /// Creates a map with estimated capacity for `entry_capacity` records.
    #[must_use]
    pub fn with_capacity(entry_capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity_and_hasher(entry_capacity, DefaultHashBuilder::default()),
        }
    }

    /// Inserts a key or atomically replaces its value.
    ///
    /// The previous value is cloned while reclamation is pinned because a
    /// removed concurrent record cannot be moved out safely.
    pub fn insert(&self, key: &[u8], value: V) -> InsertOutcome<V>
    where
        V: Clone,
    {
        let map = self.inner.pin();
        map.insert(key.into(), value)
            .map_or(InsertOutcome::Inserted, |previous| {
                InsertOutcome::Replaced(previous.clone())
            })
    }

    /// Inserts only if `key` is absent.
    ///
    /// Returns `true` for the single successful inserter and `false` when an
    /// equal key already exists.
    pub fn insert_new(&self, key: &[u8], value: V) -> bool {
        self.inner.pin().try_insert(key.into(), value).is_ok()
    }

    /// Clones the latest value out under an epoch pin.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.with_value(key, |value| value.cloned())
    }

    /// Runs `read` with the latest value while reclamation is pinned.
    ///
    /// The read path is lock-free. Keeping this closure short allows retired
    /// records to be reclaimed promptly, but it cannot block writers.
    pub fn with_value<R>(&self, key: &[u8], read: impl FnOnce(Option<&V>) -> R) -> R {
        let map = self.inner.pin();
        read(map.get(key))
    }

    /// Returns whether `key` is currently present.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let map = self.inner.pin();
        map.contains_key(key)
    }

    /// Atomically replaces an existing value using a retry-safe function.
    ///
    /// `update` may be called more than once when another writer wins a
    /// compare-and-swap race. It must therefore be pure and free of external
    /// side effects. The returned value is the newly published value.
    pub fn update(&self, key: &[u8], update: impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        let map = self.inner.pin();
        map.update(key.into(), update).cloned()
    }

    /// Atomically updates an existing value or inserts `insert_value`.
    ///
    /// `update` has the same retry-safe requirement as [`Self::update`].
    pub fn upsert(&self, key: &[u8], insert_value: V, update: impl Fn(&V) -> V) -> V
    where
        V: Clone,
    {
        let map = self.inner.pin();
        map.update_or_insert(key.into(), update, insert_value)
            .clone()
    }

    /// Atomically removes `key` and clones its previous value.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.inner.pin().remove(key).cloned()
    }

    /// Removes a value only when a retry-safe predicate accepts it.
    ///
    /// The predicate may run repeatedly under concurrent updates. Returning
    /// `Some` guarantees the cloned value corresponds to the removed record.
    #[must_use]
    pub fn remove_if(&self, key: &[u8], predicate: impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        let map = self.inner.pin();
        loop {
            match map.remove_if(key, |_, value| predicate(value)) {
                Ok(Some((_, value))) => return Some(value.clone()),
                Ok(None) => return None,
                Err((_, value)) if !predicate(value) => return None,
                Err(_) => {}
            }
        }
    }

    /// Removes every entry.
    ///
    /// Aggregate operations may wait for an in-progress table resize and are
    /// not part of the lock-free point-operation guarantee.
    pub fn clear(&self) {
        self.inner.pin().clear();
    }

    /// Returns Papaya's current concurrent entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether the map currently contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<V> Default for LockFreeBinaryMap<V> {
    fn default() -> Self {
        Self::new()
    }
}
