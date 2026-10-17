//! Packed, lock-free generational hash maps.
//!
//! [`PackedGenMap`] combines immutable packed generations with a lock-free
//! mutable overlay and exact online rebuild publication. [`AtomicPackedGenMap`]
//! specializes the value domain to [`NonMaxU64`], enabling allocation-free
//! updates, replacements, reinsertion, and deletion in frozen slots.
//!
//! The crate also retains paper-derived Elastic Hashing and SwissTable-based
//! comparison backends. They are explicit alternatives, not the placement
//! algorithm behind the primary `PackedGen` maps.

#[cfg(not(target_pointer_width = "64"))]
compile_error!("packedgen currently supports only 64-bit targets");

mod arena;
mod atomic_value;
mod bucket_map;
mod cache;
mod cache_arena;
mod cache_value;
mod concurrent_map;
mod config;
mod direct_epoch_protocol;
mod dynamic_entry;
mod fixed32_map;
mod frozen_map;
mod generation_hash;
mod generation_map;
mod generation_overlay;
mod hybrid_map;
#[cfg(feature = "kphf")]
mod kphf_map;
mod lockfree_hybrid;
mod lockfree_map;
mod map;
mod mutable_segment_cache;
mod overlay_cell;
mod packed_map;
mod route_cache;
mod segment_cache;
mod segmented_map;
mod swiss_map;

pub use arena::{ArenaError, PackedKeyArena, PackedKeyRef};
pub use atomic_value::{NonMaxU64, NonMaxU64Error};
pub use bucket_map::{BucketBuildError, BucketMapError, BucketMapStats, BucketPackedMap};
#[cfg(feature = "prepared-keys")]
pub use cache::DirectCachePreparedReplacementBatch;
#[cfg(feature = "cache-production-diagnostics")]
pub use cache::DirectCacheProductionStats;
#[cfg(feature = "cache-diagnostics")]
pub use cache::DirectCacheReclamationStats;
pub use cache::{
    CacheAdmissionOutcome, CacheBuildError, CacheConfig, CacheConfigError, CacheGuard,
    CacheInsertError, CacheInsertOutcome, CacheMaintenance, CacheMaintenanceResult, CacheStats,
    CacheValue, CacheWriteOutcome, DirectAdaptiveAdmission, DirectCacheAdmissionBatch,
    DirectCacheBulkAdmissionBatch, DirectCacheGuard, DirectCacheRemovalBatch,
    DirectCacheReplacementBatch, DirectCacheUntrackedRemovalBatch,
    DirectCacheUntrackedReplacementBatch, DirectCacheValue, DirectClonedCacheGuard,
    DirectPackedCache, DirectShareableCacheGuard, PackedCache,
};
pub use concurrent_map::{
    ConcurrentConfigError, ConcurrentMapStats, ConcurrentSwissMap, UpsertOutcome,
};
pub use config::{ConfigError, ElasticConfig, MaintenanceMode, RouteCacheBudget};
pub use fixed32_map::{Fixed32CapacityError, Fixed32Load, Fixed32MapStats, Fixed32SoaMap};
pub use frozen_map::{FrozenBuildError, FrozenIndexBackend, FrozenMapStats, FrozenPackedMap};
#[doc(hidden)]
pub use generation_hash::GenerationHashBuilder;
#[cfg(feature = "prepared-batch-gate")]
pub use generation_map::AtomicOperationGuard;
#[cfg(feature = "prepared-keys")]
pub use generation_map::AtomicPreparedKey;
pub use generation_map::{
    AdaptiveOverlayPhase, AdaptiveOverlayStats, AdaptiveRebuildPolicy,
    AdaptiveRebuildRecommendation, AtomicEntry, AtomicGenerationBaseFilter,
    AtomicGenerationOverlay, AtomicVacantEntry, GenerationMapStats, GenerationRebuild,
    LockFreeAtomicU64GenerationMap, LockFreeGenerationMap,
};
pub use hybrid_map::{HybridBuildError, HybridFilterMode, HybridMapStats, HybridPackedMap};
#[cfg(feature = "kphf")]
#[doc(hidden)]
pub use kphf_map::{KPhfAtomicU64Map, KPhfFrozenStats, PtrHashAtomicU64Map};
pub use lockfree_hybrid::{LockFreeHybridMap, LockFreeHybridStats};
pub use lockfree_map::LockFreeBinaryMap;
pub use map::{CapacityError, FixedElasticMap, InsertOutcome, MapStats};
pub use mutable_segment_cache::{
    MutableSegmentCache, MutableSegmentCacheBuildError, MutableSegmentCacheGuard,
    OnlineMutableSegmentCache, OnlineMutableSegmentCacheGuard, OnlineMutableSegmentWriterGuard,
    OnlineSegmentCacheCompactionStats, SegmentCacheCompactionStats, SegmentCacheWriteOutcome,
};
pub use opthash::{EpochSnapshot, EpochTransition, ReserveFraction};
pub use packed_map::{
    MaintenanceError, MaintenanceProgress, PackedBinaryMap, PackedBuildError, PackedGeneration,
    PackedLoadError, PackedMaintenancePlan, PackedMapError, PackedMapStats,
};
pub use segment_cache::{
    FrozenSegmentCache, SegmentCacheBuildError, SegmentCacheConfig, SegmentCacheStats,
    SegmentExpiry,
};
pub use segmented_map::{KeyCompactionStats, SegmentedLoad, SegmentedMapStats, SegmentedSwissMap};
pub use swiss_map::{PackedSwissMap, SwissMapStats};

/// Primary packed-generation map for arbitrary cloneable values.
pub type PackedGenMap<V> = LockFreeGenerationMap<V>;

/// Primary packed-generation map with allocation-free atomic `u64`-class values.
pub type AtomicPackedGenMap = LockFreeAtomicU64GenerationMap;

/// Prepared exact handle for explicitly identified hot atomic keys.
#[cfg(feature = "prepared-keys")]
pub type PreparedKey = AtomicPreparedKey;
