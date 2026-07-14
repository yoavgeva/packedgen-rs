use core::fmt;
use std::borrow::Borrow;
use std::hash::Hash;

use opthash::{ElasticHashMap, EpochSnapshot, ReserveFraction};

use crate::{ElasticConfig, filter::NegativeLookupFilter};

/// A fixed-epoch elastic map that rejects growth past an explicit live limit.
///
/// This type is intentionally single-writer and does not perform internal
/// synchronization. Put it behind an application shard, not a global mutex.
pub struct FixedElasticMap<K: Eq + Hash, V> {
    inner: ElasticHashMap<K, V>,
    live_limit: usize,
    negative_filter: NegativeLookupFilter,
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
            negative_filter: NegativeLookupFilter::new(config.live_capacity()),
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
        if self.inner.len() >= self.live_limit && !self.inner.contains_key(&key) {
            return Err(CapacityError {
                live_limit: self.live_limit,
            });
        }

        let generation = self.inner.epoch().generation;
        self.negative_filter.insert(&key);
        let outcome = match self.inner.insert(key, value) {
            Some(previous) => InsertOutcome::Replaced(previous),
            None => InsertOutcome::Inserted,
        };
        if self.inner.epoch().generation != generation {
            self.rebuild_negative_filter();
        }
        Ok(outcome)
    }

    /// Looks up a value using an equivalent borrowed key.
    #[must_use]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if !self.negative_filter.may_contain(key) {
            return None;
        }
        self.inner.get(key)
    }

    /// Returns whether an equivalent borrowed key is present.
    #[must_use]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.negative_filter.may_contain(key) && self.inner.contains_key(key)
    }

    /// Removes a key and returns its value.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if !self.negative_filter.may_contain(key) {
            return None;
        }
        let generation = self.inner.epoch().generation;
        let removed = self.inner.remove(key);
        if self.inner.epoch().generation != generation {
            self.rebuild_negative_filter();
        }
        removed
    }

    /// Removes every entry while retaining the allocation.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.negative_filter.clear();
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
            negative_filter_bytes: self.negative_filter.bytes(),
        }
    }

    fn rebuild_negative_filter(&mut self) {
        let mut replacement = NegativeLookupFilter::new(self.live_limit);
        for (key, _) in &self.inner {
            replacement.insert(key);
        }
        self.negative_filter = replacement;
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
    /// Requested bytes in the service-layer definite-negative filter.
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
