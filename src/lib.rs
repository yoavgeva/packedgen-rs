//! Service-oriented elastic hashing.
//!
//! The initial API makes allocation epochs explicit and prevents automatic
//! growth from hiding the high-occupancy behavior an application intends to
//! measure. Concurrency and background rebuilding are planned, not yet
//! implemented.

mod config;
mod filter;
mod map;

pub use config::{ConfigError, ElasticConfig};
pub use map::{CapacityError, FixedElasticMap, InsertOutcome, MapStats};
pub use opthash::{EpochSnapshot, EpochTransition, ReserveFraction};
