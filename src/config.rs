use core::fmt;
use std::num::NonZeroUsize;

use opthash::ReserveFraction;

/// Construction settings for a fixed elastic-hash epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ElasticConfig {
    live_capacity: NonZeroUsize,
    reserve: ReserveFraction,
    route_cache_budget: RouteCacheBudget,
}

impl ElasticConfig {
    /// Creates a configuration with the conservative `1/8` reserve.
    ///
    /// # Panics
    ///
    /// Panics if `live_capacity` is zero. Use [`Self::try_new`] when capacity
    /// comes from untrusted configuration.
    #[must_use]
    pub fn new(live_capacity: usize) -> Self {
        Self::try_new(live_capacity).expect("elastic hash capacity must be positive")
    }

    /// Creates a validated configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroCapacity`] when `live_capacity` is zero.
    pub fn try_new(live_capacity: usize) -> Result<Self, ConfigError> {
        let live_capacity = NonZeroUsize::new(live_capacity).ok_or(ConfigError::ZeroCapacity)?;
        Ok(Self {
            live_capacity,
            reserve: ReserveFraction::DEFAULT,
            route_cache_budget: RouteCacheBudget::Adaptive,
        })
    }

    /// Selects the exact reserve `delta = 2^-exponent`.
    ///
    /// Exponent 6 means a `1/64` reserve and a target occupancy near 98.4%.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidReserveExponent`] for exponent zero.
    pub fn with_reserve_exponent(mut self, exponent: u32) -> Result<Self, ConfigError> {
        self.reserve = ReserveFraction::from_exponent(exponent)
            .map_err(|_| ConfigError::InvalidReserveExponent(exponent))?;
        Ok(self)
    }

    /// Maximum number of live entries admitted in this application epoch.
    #[must_use]
    pub const fn live_capacity(self) -> usize {
        self.live_capacity.get()
    }

    /// Exact fraction of physical slots reserved as empty headroom.
    #[must_use]
    pub const fn reserve(self) -> ReserveFraction {
        self.reserve
    }

    /// Selects the packed-map direct-routing memory budget.
    #[must_use]
    pub const fn with_route_cache_budget(mut self, budget: RouteCacheBudget) -> Self {
        self.route_cache_budget = budget;
        self
    }

    pub(crate) fn route_cache_slots(self) -> usize {
        match self.route_cache_budget {
            RouteCacheBudget::Compact => 0,
            RouteCacheBudget::Adaptive if self.live_capacity() < (1 << 17) => {
                self.live_capacity().saturating_mul(3).div_ceil(4)
            }
            RouteCacheBudget::ReadOptimized | RouteCacheBudget::Adaptive => {
                self.live_capacity().saturating_mul(2)
            }
        }
    }
}

/// Memory policy for the packed map's advisory direct-routing cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RouteCacheBudget {
    /// Use a smaller cache below 128K entries and a dense cache for large maps.
    #[default]
    Adaptive,
    /// Disable direct routes; retain the packed arena and exact elastic lookup.
    Compact,
    /// Allocate two route slots per configured entry for lookup-heavy indexes.
    ReadOptimized,
}

/// Invalid elastic-hash configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// A table cannot admit zero live entries.
    ZeroCapacity,
    /// Reserve fractions use the exact form `2^-d`, with `d > 0`.
    InvalidReserveExponent(u32),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => formatter.write_str("live capacity must be positive"),
            Self::InvalidReserveExponent(value) => {
                write!(formatter, "reserve exponent must be positive, got {value}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::{ElasticConfig, RouteCacheBudget};

    #[test]
    fn route_cache_budgets_are_bounded_and_adaptive() {
        assert_eq!(ElasticConfig::new(100_000).route_cache_slots(), 75_000);
        assert_eq!(ElasticConfig::new(250_000).route_cache_slots(), 500_000);
        assert_eq!(
            ElasticConfig::new(250_000)
                .with_route_cache_budget(RouteCacheBudget::Compact)
                .route_cache_slots(),
            0
        );
        assert_eq!(
            ElasticConfig::new(10_000)
                .with_route_cache_budget(RouteCacheBudget::ReadOptimized)
                .route_cache_slots(),
            20_000
        );
    }
}
