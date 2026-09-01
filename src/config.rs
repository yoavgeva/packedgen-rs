use core::fmt;
use std::num::NonZeroUsize;

use opthash::ReserveFraction;

/// Construction settings for a fixed elastic-hash epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ElasticConfig {
    live_capacity: NonZeroUsize,
    reserve: ReserveFraction,
    route_cache_budget: RouteCacheBudget,
    maintenance_mode: MaintenanceMode,
    maintenance_soft_percent: u8,
    maintenance_hard_percent: u8,
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
            maintenance_mode: MaintenanceMode::Synchronous,
            maintenance_soft_percent: 25,
            maintenance_hard_percent: 50,
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

    /// Selects whether delete-threshold maintenance runs inline or is deferred
    /// until the owner explicitly requests it.
    #[must_use]
    pub const fn with_maintenance_mode(mut self, mode: MaintenanceMode) -> Self {
        self.maintenance_mode = mode;
        self
    }

    pub(crate) const fn maintenance_mode(self) -> MaintenanceMode {
        self.maintenance_mode
    }

    /// Sets the soft maintenance signal and forced-maintenance ceiling as
    /// percentages of configured live capacity.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidMaintenanceThresholds`] unless
    /// `1 <= soft_percent <= hard_percent <= 100`.
    pub fn with_maintenance_threshold_percents(
        mut self,
        soft_percent: u8,
        hard_percent: u8,
    ) -> Result<Self, ConfigError> {
        if soft_percent == 0 || soft_percent > hard_percent || hard_percent > 100 {
            return Err(ConfigError::InvalidMaintenanceThresholds {
                soft_percent,
                hard_percent,
            });
        }
        self.maintenance_soft_percent = soft_percent;
        self.maintenance_hard_percent = hard_percent;
        Ok(self)
    }

    /// Configured `(soft, hard)` delete-pressure percentages.
    #[must_use]
    pub const fn maintenance_threshold_percents(self) -> (u8, u8) {
        (self.maintenance_soft_percent, self.maintenance_hard_percent)
    }

    pub(crate) fn route_cache_slots(self) -> usize {
        match self.route_cache_budget {
            RouteCacheBudget::Compact => 0,
            // Four-way buckets retain most routes at one physical slot per
            // entry. This is the default RAM/performance balance.
            RouteCacheBudget::Adaptive => self.live_capacity(),
            // Keep the old denser route directory available for latency-first
            // callers; it now benefits from four-way buckets as well.
            RouteCacheBudget::ReadOptimized => self.live_capacity().saturating_mul(2),
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

/// Policy for tombstone cleanup and packed-key compaction after deletions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MaintenanceMode {
    /// Rebuild synchronously when the configured delete threshold is crossed.
    #[default]
    Synchronous,
    /// Mark maintenance due and let the single writer choose when to rebuild.
    /// An insertion may still force maintenance if accumulated tombstones leave
    /// no physical slot for a new key. Reaching one deletion per two configured
    /// entries also forces maintenance to bound ignored tombstones and dead key
    /// bytes.
    Deferred,
}

/// Invalid elastic-hash configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// A table cannot admit zero live entries.
    ZeroCapacity,
    /// Reserve fractions use the exact form `2^-d`, with `d > 0`.
    InvalidReserveExponent(u32),
    /// Maintenance percentages must satisfy `1 <= soft <= hard <= 100`.
    InvalidMaintenanceThresholds {
        /// Rejected soft percentage.
        soft_percent: u8,
        /// Rejected hard percentage.
        hard_percent: u8,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => formatter.write_str("live capacity must be positive"),
            Self::InvalidReserveExponent(value) => {
                write!(formatter, "reserve exponent must be positive, got {value}")
            }
            Self::InvalidMaintenanceThresholds {
                soft_percent,
                hard_percent,
            } => write!(
                formatter,
                "maintenance thresholds require 1 <= soft <= hard <= 100, got {soft_percent}/{hard_percent}"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::{ElasticConfig, MaintenanceMode, RouteCacheBudget};

    #[test]
    fn route_cache_budgets_are_bounded_and_adaptive() {
        assert_eq!(ElasticConfig::new(100_000).route_cache_slots(), 100_000);
        assert_eq!(ElasticConfig::new(250_000).route_cache_slots(), 250_000);
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

    #[test]
    fn synchronous_maintenance_is_the_safe_default() {
        assert_eq!(
            ElasticConfig::new(1).maintenance_mode(),
            MaintenanceMode::Synchronous
        );
        assert_eq!(
            ElasticConfig::new(1)
                .with_maintenance_mode(MaintenanceMode::Deferred)
                .maintenance_mode(),
            MaintenanceMode::Deferred
        );
    }

    #[test]
    fn maintenance_thresholds_are_validated() {
        let config = ElasticConfig::new(10)
            .with_maintenance_threshold_percents(20, 30)
            .unwrap();
        assert_eq!(config.maintenance_threshold_percents(), (20, 30));
        for invalid in [(0, 50), (51, 50), (25, 101)] {
            assert!(
                ElasticConfig::new(10)
                    .with_maintenance_threshold_percents(invalid.0, invalid.1)
                    .is_err()
            );
        }
    }
}
