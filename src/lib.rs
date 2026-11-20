//! A memory-dense concurrent cache.
//!
//! [`ShardedCache`] is a bounded concurrent cache for any `K: Hash + Eq`:
//! lock-free reads, per-shard writer locks, S3-FIFO eviction with
//! frequency-sketch admission, times to live, loading a missing key once,
//! and about 30 bytes of overhead per entry. Configure it with
//! [`ShardedCacheBuilder`].

#[cfg(not(target_pointer_width = "64"))]
compile_error!("packedgen currently supports only 64-bit targets");

mod direct_epoch_protocol;
mod sharded_arena;
mod sharded_cache;
mod thread_index;

pub use sharded_cache::{
    GuardResult, NoListener, PlaceholderGuard, RemovalCause, RemovalListener, ShardedCache,
    ShardedCacheBuilder, ShardedCacheGuard, ShardedCacheMemory, ShardedCacheStats, UnitWeighter,
    Weighter,
};

/// The hasher [`ShardedCache`] uses unless its builder is given another:
/// foldhash's fast hasher, seeded at random per cache.
pub type DefaultHashBuilder = foldhash::fast::RandomState;

/// Compiles and runs the README's example as a doctest, so it cannot drift
/// from the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
