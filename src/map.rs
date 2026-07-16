use core::fmt;
use std::borrow::Borrow;
use std::hash::Hash;

use opthash::{ElasticHashMap, EpochSnapshot, ReserveFraction};

use crate::ElasticConfig;

/// A fixed-epoch elastic map that rejects growth past an explicit live limit.
///
/// This type is intentionally single-writer and does not perform internal
/// synchronization. Put it behind an application shard, not a global mutex.
pub struct FixedElasticMap<K: Eq + Hash, V> {
    inner: ElasticHashMap<K, V>,
    live_limit: usize,
}

impl<K, V> FixedElasticMap<K, V>
where
    K: Eq + Hash,
{
    /// Allocates a new map for the configured fixed epoch.
    #[must_use]
    pub fn new(config: ElasticConfig) -> Self {
        Self {
            inner: ElasticHashMap::with_capacity_and_reserve(
                config.live_capacity(),
                config.reserve(),
            ),
            live_limit: config.live_capacity(),
        }
    }

    /// Inserts a new key or replaces its existing value.
    ///
    /// An update is allowed when the table is at its live limit. An absent key
    /// is rejected, preserving the current capacity epoch.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityError`] when an absent key would exceed the configured
    /// live-entry limit.
    pub fn try_insert(&mut self, key: K, value: V) -> Result<InsertOutcome<V>, CapacityError> {
        // Hash once and reuse the result across the service filter and core.
        // This removes a second hash from the capacity/replacement path while
        // retaining the single-pass core insertion path for ordinary writes.
        let hash = self.inner.hash_key(&key);
        if self.inner.len() >= self.live_limit {
            if !self.inner.may_contain_prehashed(hash) {
                return Err(CapacityError {
                    live_limit: self.live_limit,
                });
            }
            if let Some(previous) = self.inner.get_mut_prehashed(hash, &key) {
                return Ok(InsertOutcome::Replaced(core::mem::replace(previous, value)));
            }
            return Err(CapacityError {
                live_limit: self.live_limit,
            });
        }

        let outcome = match self.inner.insert(key, value) {
            Some(previous) => InsertOutcome::Replaced(previous),
            None => InsertOutcome::Inserted,
        };
        Ok(outcome)
    }

    /// Looks up a value using an equivalent borrowed key.
    #[must_use]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.inner.hash_key(key);
        if !self.inner.may_contain_prehashed(hash) {
            return None;
        }
        self.inner.get_prehashed(hash, key)
    }

    /// Returns whether an equivalent borrowed key is present.
    #[must_use]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.inner.hash_key(key);
        self.inner.may_contain_prehashed(hash) && self.inner.contains_prehashed(hash, key)
    }

    /// Removes a key and returns its value.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.inner.hash_key(key);
        if !self.inner.may_contain_prehashed(hash) {
            return None;
        }
        self.inner
            .remove_prehashed(hash, key)
            .map(|(_, value)| value)
    }

    /// Removes every entry while retaining the allocation.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` when the map contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Captures observable lifecycle and occupancy information.
    #[must_use]
    pub fn stats(&self) -> MapStats {
        MapStats {
            len: self.inner.len(),
            live_limit: self.live_limit,
            core_capacity: self.inner.capacity(),
            reserve: self.inner.reserve_fraction(),
            epoch: self.inner.epoch(),
            negative_filter_bytes: self.inner.membership_filter_bytes(),
        }
    }
}

/// Result of a successful insertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertOutcome<V> {
    /// A previously absent key was inserted.
    Inserted,
    /// An existing value was replaced and returned.
    Replaced(V),
}

/// An insertion would cross the configured fixed-epoch capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityError {
    live_limit: usize,
}

impl CapacityError {
    pub(crate) const fn new(live_limit: usize) -> Self {
        Self { live_limit }
    }

    /// Configured maximum live entries for the current epoch.
    #[must_use]
    pub const fn live_limit(self) -> usize {
        self.live_limit
    }
}

impl fmt::Display for CapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "elastic hash epoch reached its live-entry limit ({})",
            self.live_limit
        )
    }
}

impl std::error::Error for CapacityError {}

/// Observable state for metrics and rebuild policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MapStats {
    /// Current number of live entries.
    pub len: usize,
    /// Application-enforced fixed-epoch limit.
    pub live_limit: usize,
    /// Core capacity before an automatic resize would occur.
    pub core_capacity: usize,
    /// Exact empty-slot reserve selected for the core.
    pub reserve: ReserveFraction,
    /// Core allocation-epoch lifecycle state.
    pub epoch: EpochSnapshot,
    /// Requested bytes in the core definite-negative membership filter.
    pub negative_filter_bytes: usize,
}

impl MapStats {
    /// Fraction of the application live-entry limit currently occupied.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn occupancy(self) -> f64 {
        // Metrics need a ratio, not integer-exact identity. Capacities above
        // f64's exact integer range are not practically allocatable tables.
        self.len as f64 / self.live_limit as f64
    }
}
