//! Service-oriented elastic hashing.
//!
//! The initial API makes allocation epochs explicit and prevents automatic
//! growth from hiding the high-occupancy behavior an application intends to
//! measure. Concurrency and background rebuilding are planned, not yet
//! implemented.

mod arena;
mod config;
mod filter;
mod map;
mod packed_map;
mod route_cache;

pub use arena::{ArenaError, PackedKeyArena, PackedKeyRef};
pub use config::{ConfigError, ElasticConfig, MaintenanceMode, RouteCacheBudget};
pub use map::{CapacityError, FixedElasticMap, InsertOutcome, MapStats};
pub use opthash::{EpochSnapshot, EpochTransition, ReserveFraction};
pub use packed_map::{
    MaintenanceError, MaintenanceProgress, PackedBinaryMap, PackedBuildError, PackedGeneration,
    PackedLoadError, PackedMaintenancePlan, PackedMapError, PackedMapStats,
};
