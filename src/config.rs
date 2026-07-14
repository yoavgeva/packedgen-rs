use core::fmt;
use std::num::NonZeroUsize;

use opthash::ReserveFraction;

/// Construction settings for a fixed elastic-hash epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ElasticConfig {
    live_capacity: NonZeroUsize,
    reserve: ReserveFraction,
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
