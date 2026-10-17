use std::cell::{Cell, OnceCell};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use parking_lot::Mutex;

#[cfg(feature = "prepared-keys")]
use crate::AtomicPreparedKey;
use crate::cache_arena::{
    ArenaAllocationError, ArenaEntry, ArenaHandle, ArenaPin, ArenaValue, DIRECT_MAX_EXPIRY_TICK,
    DIRECT_MAX_WEIGHT, DirectArenaEntry, DirectArenaMutationPin, DirectArenaPin,
    DirectArenaReservation, DirectArenaValue, DirectHandle, DirectValueArena, MAX_EXPIRY_TICK,
    RemovedDirectHandle, ValueArena, direct_arena_thread,
};
use crate::generation_hash::GenerationKeyHash;
use crate::{
    AdaptiveRebuildPolicy, AtomicEntry, AtomicGenerationBaseFilter, AtomicGenerationOverlay,
    FrozenBuildError, FrozenIndexBackend, GenerationHashBuilder, InsertOutcome,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

const NEVER_EXPIRES: u64 = u64::MAX;
const DEFAULT_OVERLAY_CAPACITY: usize = 65_536;
/// The smallest default overlay of an entry-limited direct cache.
const MIN_DIRECT_DEFAULT_OVERLAY_CAPACITY: usize = 64;
const DEFAULT_ARENA_PARTITIONS: usize = 64;
const DEFAULT_EVICTION_BATCH: usize = 64;
const DEFAULT_PROBATION_BPS: u16 = 200;
const MAX_PROBATION_BPS: u16 = 5_000;
const MAX_EXPIRATION_BATCHES: usize = 16;
const COUNTER_SHARDS: usize = 64;
const DIRECT_RESERVOIR_KEY_CHUNK: usize = 64;

#[cfg(test)]
thread_local! {
    // Keep operation counts isolated from parallel unit tests.
    static DIRECT_WEIGHT_SCANS: AtomicUsize = const { AtomicUsize::new(0) };
    static DIRECT_VICTIM_INDEX_PROBES: AtomicUsize = const { AtomicUsize::new(0) };
}

/// Decodes an unchanged value loaded from the direct cache's authoritative
/// index. Keeping this boundary private to the cache module prevents unrelated
/// safe crate code from forging a dereferenceable arena handle.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes reconstruction of index-owned direct handles"
)]
fn published_direct_handle_from_index(value: NonMaxU64) -> DirectHandle {
    // SAFETY: every caller passes the direct index's unchanged stored value;
    // only `DirectHandle::index_value` is published into that value domain.
    unsafe { DirectHandle::from_index_value(value) }
}

/// Dereferences a handle owned by this module's direct cache while `pin`
/// protects the same cache arena.
///
/// # Safety
///
/// `handle` must be allocated by or loaded unchanged from the
/// `DirectPackedCache` that created `pin`, and it must remain live for the
/// pin's reader epoch.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes the direct cache's same-arena pin/handle proof"
)]
unsafe fn protected_direct_entry<'pin, V>(
    pin: &'pin DirectArenaPin<'_, V>,
    handle: DirectHandle,
) -> &'pin DirectArenaEntry<V> {
    // SAFETY: this private adapter is called only with handles allocated by or
    // loaded unchanged from the `DirectPackedCache` that created `pin`. Exact
    // removal retains the allocation until the pin's reader epoch ends.
    unsafe { pin.protect(handle) }.entry()
}

/// Converts the old value returned by an exact direct-index mutation into the
/// unique capability that may be queued for reclamation.
///
/// Keep every call immediately adjacent to the successful removal/replacement
/// that transferred ownership. This private boundary prevents ordinary read
/// handles from reaching the retirement API.
///
/// # Safety
///
/// The caller must have made `handle` unreachable from this cache's index and
/// must own its sole retirement responsibility.
#[inline]
#[allow(
    unsafe_code,
    reason = "centralizes the exact index-removal ownership proof"
)]
unsafe fn removed_after_exact_index_transfer(handle: DirectHandle) -> RemovedDirectHandle {
    // SAFETY: callers in this module invoke the helper only with the exact old
    // value returned by the index operation that made that publication
    // unreachable, or while exclusively tearing down the complete cache.
    unsafe { RemovedDirectHandle::from_exact_removal(handle) }
}
const ACCESS_SAMPLE_MASK: u64 = 15;
const HEAVY_ACCESS_SAMPLE_MASK: u64 = 0;
const HEAVY_ACCESS_WEIGHT: u64 = 4_096;
const FREQUENCY_REUSE_MIN_LOOKUPS: u64 = 1_024;
const FREQUENCY_REUSE_WINDOW: u64 = 16_384;
const FREQUENCY_REUSE_PUBLISH_INTERVAL: u64 = 4_096;
const MUTATION_STRIPES: usize = 2_048;
const EXPIRY_TICK_NANOS: u128 = 100_000_000;
const DIRECT_LENGTH_IMMEDIATE_RECHECK: u8 = 64;
const DIRECT_ADAPTIVE_EXISTING_THRESHOLD: u8 = 2;
const DIRECT_FALLBACK_REFILL_MULTIPLIER: usize = 16;
const DIRECT_ADMISSION_RECLAIM_INTERVAL: usize = 512;
const DIRECT_DOORKEEPER_CLEAR_WORDS: usize = 16;
const DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL: u64 = 16;
const DIRECT_ASYNC_VICTIM_SAMPLE_MAX: usize = 1_024;
/// Victim candidates sampled per requested removal. Sorting a window by its
/// access marks only protects read entries while it holds several candidates
/// per removal; an equal-sized window evicts every candidate it samples.
const DIRECT_VICTIM_CANDIDATES_PER_REMOVAL: usize = 4;
/// Sampling rounds one eviction batch may take to find unread entries. On the
/// ARC paper's traces a CLOCK model limited to four candidates per removal
/// lost up to 2.4 points of hit rate against an unbounded hand, and at most
/// 0.2 with sixty-four, which sixteen rounds of four allow.
const DIRECT_VICTIM_SWEEP_ROUNDS: usize = 16;
const DIRECT_DOORKEEPER_GROUP_LOCK: u64 = 1_u64 << 63;
const DIRECT_DOORKEEPER_EPOCH_MASK: u64 = DIRECT_DOORKEEPER_GROUP_LOCK - 1;
const DIRECT_DOORKEEPER_INVALID_EPOCH: u64 = DIRECT_DOORKEEPER_EPOCH_MASK;

fn doorkeeper_hashes_from_key_hash(key_hash: GenerationKeyHash) -> [u64; 2] {
    doorkeeper_hashes_from_route(key_hash.route())
}

fn doorkeeper_hashes_from_route(route: u64) -> [u64; 2] {
    [route, doorkeeper_mix64(route ^ 0xd6e8_feb8_6659_fd93)]
}

/// Configuration for [`PackedCache`].
#[derive(Clone, Debug)]
pub struct CacheConfig {
    max_weight: u64,
    max_entries: Option<usize>,
    /// `None` uses the cache type's default; see [`Self::with_overlay_capacity`].
    overlay_capacity: Option<usize>,
    arena_partitions: usize,
    eviction_batch: usize,
    admission_doorkeeper_entries: Option<usize>,
    frequency_admission_max_gate: Option<u8>,
    frequency_admission_min_hit_rate_bps: Option<u16>,
    frequency_admission_high_reuse: Option<(u8, u16)>,
    default_ttl: Option<Duration>,
    rebuild_policy: AdaptiveRebuildPolicy,
    async_hard_limit_bps: Option<u16>,
    probation_bps: u16,
}

impl CacheConfig {
    /// Creates a cache with a soft maximum caller-accounted weight.
    ///
    /// The default item charge is the shallow value size plus key bytes.
    /// Heap-owning values should be inserted with an explicit charge through
    /// [`PackedCache::insert_with_options`].
    #[must_use]
    pub const fn new(max_weight: u64) -> Self {
        Self {
            max_weight,
            max_entries: None,
            overlay_capacity: None,
            arena_partitions: DEFAULT_ARENA_PARTITIONS,
            eviction_batch: DEFAULT_EVICTION_BATCH,
            admission_doorkeeper_entries: None,
            frequency_admission_max_gate: None,
            frequency_admission_min_hit_rate_bps: None,
            frequency_admission_high_reuse: None,
            default_ttl: None,
            rebuild_policy: AdaptiveRebuildPolicy {
                max_slot_utilization_bps: 7_500,
                min_learned_insertions: 256,
                max_distribution_drift_bps: 1_500,
                max_short_key_spill_bps: 500,
            },
            async_hard_limit_bps: None,
            probation_bps: DEFAULT_PROBATION_BPS,
        }
    }

    /// Adds a maximum live-entry count in addition to the weight limit.
    #[must_use]
    pub const fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = Some(max_entries);
        self
    }

    /// Sets the initial adaptive index overlay budget.
    ///
    /// The default is 65,536. A [`DirectPackedCache`] with an entry limit
    /// defaults to that limit instead, between 64 and 65,536: an overlay much
    /// larger than the cache fills with evicted keys' slots between rebuilds,
    /// which leaves sampled eviction too few live candidates.
    #[must_use]
    pub const fn with_overlay_capacity(mut self, overlay_capacity: usize) -> Self {
        self.overlay_capacity = Some(overlay_capacity);
        self
    }

    const fn packed_overlay_capacity(&self) -> usize {
        match self.overlay_capacity {
            Some(capacity) => capacity,
            None => DEFAULT_OVERLAY_CAPACITY,
        }
    }

    fn direct_overlay_capacity(&self) -> usize {
        match (self.overlay_capacity, self.max_entries) {
            (Some(capacity), _) => capacity,
            (None, Some(entries)) => entries.clamp(
                MIN_DIRECT_DEFAULT_OVERLAY_CAPACITY,
                DEFAULT_OVERLAY_CAPACITY,
            ),
            (None, None) => DEFAULT_OVERLAY_CAPACITY,
        }
    }

    /// Sets the number of independently allocated value-arena partitions.
    #[must_use]
    pub const fn with_arena_partitions(mut self, partitions: usize) -> Self {
        self.arena_partitions = partitions;
        self
    }

    /// Sets the approximate-LRU victim batch size.
    #[must_use]
    pub const fn with_eviction_batch(mut self, eviction_batch: usize) -> Self {
        self.eviction_batch = eviction_batch;
        self
    }

    /// Enables scan-resistant second-sighting admission for the direct cache.
    ///
    /// The doorkeeper retains two rotating approximate membership generations
    /// at 16 bits per expected entry per generation (four nominal bytes per
    /// expected entry total). Initial fill is admitted normally. After the
    /// cache first reaches `expected_entries`, a previously unseen key is
    /// rejected once; a repeated miss is admitted. False positives can admit
    /// a one-off key, but never change key/value correctness.
    #[must_use]
    pub const fn with_admission_doorkeeper(mut self, expected_entries: usize) -> Self {
        self.admission_doorkeeper_entries = Some(expected_entries);
        self
    }

    /// Enables frequency-aware admission and victim ranking for the doorkeeper.
    ///
    /// Two saturating counters are sampled per admission attempt. Ordinary
    /// cache reads are unchanged. `maximum_victim_frequency` caps how many
    /// sightings a candidate may need before admission. The prototype sketch
    /// adds four to eight bytes per expected entry after power-of-two rounding
    /// and has no effect unless [`Self::with_admission_doorkeeper`] is also
    /// enabled.
    #[must_use]
    pub const fn with_frequency_admission(mut self, maximum_victim_frequency: u8) -> Self {
        self.frequency_admission_max_gate = Some(maximum_victim_frequency);
        self.frequency_admission_min_hit_rate_bps = None;
        self.frequency_admission_high_reuse = None;
        self
    }

    /// Enables frequency admission only after the cache demonstrates reuse.
    ///
    /// Before the cache records 1,024 recent lookups, or while its rolling hit
    /// rate remains below `minimum_hit_rate_bps`, candidates use ordinary
    /// second-sighting admission. This avoids starving version-like workloads
    /// with little reusable history while allowing a cache to recover from an
    /// early cold-fill phase. The rolling estimate retains at most 16,384
    /// lookups. Borrowed guards publish observations when refreshed or dropped.
    /// `6_000` represents a 60% minimum hit rate.
    #[must_use]
    pub const fn with_adaptive_frequency_admission(
        mut self,
        maximum_victim_frequency: u8,
        minimum_hit_rate_bps: u16,
    ) -> Self {
        self.frequency_admission_max_gate = Some(maximum_victim_frequency);
        self.frequency_admission_min_hit_rate_bps = Some(minimum_hit_rate_bps);
        self.frequency_admission_high_reuse = None;
        self
    }

    /// Adds a stricter frequency tier after the cache demonstrates high reuse.
    ///
    /// The base gate becomes active at `minimum_hit_rate_bps`. The high gate
    /// is used only after at least 1,024 recent lookups and a rolling hit rate of
    /// `high_reuse_hit_rate_bps`. This preserves ordinary pressure behavior
    /// while allowing high-reuse caches to protect proven residents more
    /// aggressively. The tier adds no sketch counters or per-entry metadata.
    #[must_use]
    pub const fn with_tiered_frequency_admission(
        mut self,
        base_maximum_victim_frequency: u8,
        minimum_hit_rate_bps: u16,
        high_reuse_maximum_victim_frequency: u8,
        high_reuse_hit_rate_bps: u16,
    ) -> Self {
        self.frequency_admission_max_gate = Some(base_maximum_victim_frequency);
        self.frequency_admission_min_hit_rate_bps = Some(minimum_hit_rate_bps);
        self.frequency_admission_high_reuse =
            Some((high_reuse_maximum_victim_frequency, high_reuse_hit_rate_bps));
        self
    }

    /// Sets the TTL used by ordinary [`PackedCache::insert`] calls.
    #[must_use]
    pub const fn with_default_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Sets the adaptive index maintenance thresholds.
    #[must_use]
    pub const fn with_rebuild_policy(mut self, policy: AdaptiveRebuildPolicy) -> Self {
        self.rebuild_policy = policy;
        self
    }

    /// Enables asynchronous direct-cache eviction with a bounded hard limit.
    ///
    /// `hard_limit_bps` is relative to the configured soft limits: `10_100`
    /// sets writer backpressure at 1% temporary overshoot. Concurrent writers
    /// can each have one admission in flight before helping drain capacity
    /// synchronously. Call [`DirectPackedCache::spawn_maintenance`] to drain
    /// ordinary soft-limit debt on a background thread. Strict synchronous
    /// eviction remains the default, and `PackedCache` currently ignores this
    /// direct-cache-only option.
    #[must_use]
    pub const fn with_async_eviction(mut self, hard_limit_bps: u16) -> Self {
        self.async_hard_limit_bps = Some(hard_limit_bps);
        self
    }

    /// Sets the direct cache's probationary share, in basis points of the
    /// live population. The default is `200` (2%); `0` disables probation.
    ///
    /// Newly published keys wait in a small FIFO. While it holds more than
    /// this share, eviction takes its oldest key first: a key read since
    /// publication stays resident under the ordinary approximate-LRU sweep,
    /// and an unread key is evicted and remembered in a ghost filter of about
    /// two bytes per expected entry. A remembered key that returns skips
    /// probation. One-hit keys therefore leave within a short window instead
    /// of displacing reused keys for a full sweep. Shares above 50% are
    /// clamped to 50%. `PackedCache` currently ignores this direct-cache
    /// option.
    #[must_use]
    pub const fn with_probation(mut self, share_bps: u16) -> Self {
        self.probation_bps = if share_bps > MAX_PROBATION_BPS {
            MAX_PROBATION_BPS
        } else {
            share_bps
        };
        self
    }

    fn frequency_sketch_max_gate(&self) -> Option<u8> {
        self.frequency_admission_max_gate.map(|base| {
            self.frequency_admission_high_reuse
                .map_or(base, |(high, _)| base.max(high))
        })
    }
}

/// Invalid cache configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheConfigError {
    /// A cache must have a nonzero weight budget.
    ZeroWeight,
    /// An explicitly configured entry limit must be nonzero.
    ZeroEntries,
    /// The adaptive overlay budget must be nonzero.
    ZeroOverlayCapacity,
    /// The arena must have at least one partition.
    ZeroArenaPartitions,
    /// Eviction must retain at least one candidate per scan.
    ZeroEvictionBatch,
    /// A configured admission doorkeeper needs a nonzero entry estimate.
    ZeroAdmissionDoorkeeperEntries,
    /// Async eviction needs a hard limit strictly above the soft target.
    InvalidAsyncHardLimit,
    /// Adaptive frequency admission needs a hit-rate threshold at most 100%.
    InvalidFrequencyHitRate,
    /// A high-reuse tier cannot be weaker or activate before its base tier.
    InvalidFrequencyTier,
}

impl fmt::Display for CacheConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroWeight => "cache weight limit must be nonzero",
            Self::ZeroEntries => "cache entry limit must be nonzero",
            Self::ZeroOverlayCapacity => "cache overlay capacity must be nonzero",
            Self::ZeroArenaPartitions => "cache arena partition count must be nonzero",
            Self::ZeroEvictionBatch => "cache eviction batch must be nonzero",
            Self::ZeroAdmissionDoorkeeperEntries => {
                "cache admission doorkeeper expected entries must be nonzero"
            }
            Self::InvalidAsyncHardLimit => {
                "async eviction hard limit must exceed 10,000 basis points"
            }
            Self::InvalidFrequencyHitRate => {
                "adaptive frequency admission hit rate must not exceed 10,000 basis points"
            }
            Self::InvalidFrequencyTier => {
                "high-reuse frequency admission must not weaken or precede its base tier"
            }
        })
    }
}

impl std::error::Error for CacheConfigError {}

/// Error constructing a [`PackedCache`].
#[derive(Debug)]
pub enum CacheBuildError {
    /// Cache configuration is invalid.
    Config(CacheConfigError),
    /// The initial packed index could not be built.
    Index(FrozenBuildError),
    /// One of the preloaded entries cannot be represented by the cache.
    InitialItem(CacheInsertError),
    /// The preload contains more entries than the configured limit.
    InitialEntries {
        /// Number of entries supplied by the preload.
        entries: usize,
        /// Configured entry limit.
        maximum: usize,
    },
    /// The preload's caller-accounted weight exceeds the configured limit.
    InitialWeight {
        /// Total weight supplied by the preload.
        weight: u64,
        /// Configured weight limit.
        maximum: u64,
    },
    /// The preload's total caller-accounted weight cannot fit in a `u64`.
    InitialWeightOverflow,
}

impl fmt::Display for CacheBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::InitialItem(error) => error.fmt(formatter),
            Self::InitialEntries { entries, maximum } => write!(
                formatter,
                "initial entry count {entries} exceeds cache limit {maximum}"
            ),
            Self::InitialWeight { weight, maximum } => write!(
                formatter,
                "initial weight {weight} exceeds cache budget {maximum}"
            ),
            Self::InitialWeightOverflow => {
                formatter.write_str("initial cache weight exceeds u64 accounting")
            }
        }
    }
}

impl std::error::Error for CacheBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Index(error) => Some(error),
            Self::InitialItem(error) => Some(error),
            Self::InitialEntries { .. }
            | Self::InitialWeight { .. }
            | Self::InitialWeightOverflow => None,
        }
    }
}

impl From<FrozenBuildError> for CacheBuildError {
    fn from(value: FrozenBuildError) -> Self {
        Self::Index(value)
    }
}

/// Error inserting a cache value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheInsertError {
    /// The individual item charge exceeds the entire cache budget.
    ItemTooHeavy {
        /// Supplied item charge.
        weight: u64,
        /// Configured cache budget.
        maximum: u64,
    },
    /// One entry cannot account four GiB or more in `PackedCache`, or two GiB
    /// or more in `DirectPackedCache`, whose weight word also holds an access
    /// mark.
    WeightNotCompact {
        /// Supplied item charge.
        weight: u64,
    },
    /// A value-arena partition exhausted its 24-bit slot space.
    ArenaFull,
}

impl fmt::Display for CacheInsertError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ItemTooHeavy { weight, maximum } => {
                write!(
                    formatter,
                    "item weight {weight} exceeds cache budget {maximum}"
                )
            }
            Self::WeightNotCompact { weight } => {
                write!(
                    formatter,
                    "item weight {weight} exceeds compact u32 accounting"
                )
            }
            Self::ArenaFull => formatter.write_str("cache value arena is full"),
        }
    }
}

impl std::error::Error for CacheInsertError {}

/// An epoch guard keeping a cache value stable after lookup or removal.
///
/// This guard is thread-local and is intentionally not `Send`. Use or drop it
/// on the thread that created it. It borrows its cache, so reads on many
/// threads share no reference count, and it cannot outlive the cache:
///
/// ```compile_fail,E0505
/// use packedgen::{CacheConfig, PackedCache};
///
/// let cache = PackedCache::try_new(CacheConfig::new(1_024)).unwrap();
/// cache.insert_discard_with_options(b"key", 7_u64, 16, None).unwrap();
/// let value = cache.get(b"key").unwrap();
/// drop(cache);
/// assert_eq!(*value, 7);
/// ```
pub struct CacheValue<'cache, V> {
    entry: ArenaValue<'cache, ArenaEntry<V>>,
}

impl<V> Deref for CacheValue<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.entry.value()
    }
}

impl<V> AsRef<V> for CacheValue<'_, V> {
    fn as_ref(&self) -> &V {
        self
    }
}

impl<V: fmt::Debug> fmt::Debug for CacheValue<'_, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}

/// Result of a successful cache insertion.
#[derive(Debug)]
pub enum CacheInsertOutcome<'cache, V> {
    /// The key was absent.
    Inserted,
    /// The previous value was replaced and remains safely owned by this result.
    Replaced(CacheValue<'cache, V>),
}

/// Result of a cache insertion that does not retain the previous value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheWriteOutcome {
    /// The key was absent.
    Inserted,
    /// The key's previous value was replaced and retired.
    Replaced,
}

/// Result of inserting a cache-miss value without replacing a racing winner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheAdmissionOutcome {
    /// This caller inserted the value.
    Inserted,
    /// Another value already occupied the key.
    Existing,
    /// Admission policy declined this absent candidate.
    Rejected,
}

/// Observable cache counters and current capacity use.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    /// Live arena entries.
    pub entries: usize,
    /// Current caller-accounted live weight.
    pub weight: u64,
    /// Configured maximum weight.
    pub max_weight: u64,
    /// Successful reads.
    pub hits: u64,
    /// Missing or expired reads.
    pub misses: u64,
    /// Newly inserted keys.
    pub inserts: u64,
    /// Replaced keys.
    pub replacements: u64,
    /// Explicit removals.
    pub removals: u64,
    /// Capacity-driven removals.
    pub evictions: u64,
    /// TTL-driven removals.
    pub expirations: u64,
    /// Insertions rejected by size limits or the admission policy.
    pub rejected: u64,
    /// Successful adaptive index rebuilds.
    pub rebuilds: u64,
    /// Failed background maintenance attempts.
    pub maintenance_errors: u64,
}

/// Exact reclamation state for memory and stalled-reader diagnostics.
///
/// This is available only with the `cache-diagnostics` feature. Capturing it
/// locks every retirement queue, so it must not be used on a production hot
/// path.
#[cfg(feature = "cache-diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectCacheReclamationStats {
    /// Epoch currently accepting newly pinned readers and retirements.
    pub current_epoch: usize,
    /// Number of active guards in each of the three reclamation epochs.
    pub readers: [usize; 3],
    /// Exact number of removed values still awaiting a safe epoch.
    pub retired_values: usize,
    /// Exact caller-accounted weight still awaiting a safe epoch.
    pub retired_bytes: u64,
    /// Retired values published to the threshold counters in 64-value groups.
    pub published_retired_values: usize,
    /// Empty value allocations retained in the prepared replacement recycler.
    pub recyclable_allocations: usize,
    /// Foreground admissions that synchronously enforced the async hard limit.
    /// Zero unless `cache-pressure-timing` is enabled.
    pub foreground_capacity_enforcements: u64,
    /// Total nanoseconds spent in foreground hard-limit enforcement.
    pub foreground_capacity_enforcement_ns: u64,
    /// Longest foreground hard-limit enforcement in nanoseconds.
    pub max_foreground_capacity_enforcement_ns: u64,
    /// Background-worker passes that drained soft-limit capacity debt.
    /// Zero unless `cache-pressure-timing` is enabled.
    pub background_capacity_drains: u64,
    /// Total nanoseconds spent draining capacity on the background worker.
    pub background_capacity_drain_ns: u64,
    /// Longest background capacity drain in nanoseconds.
    pub max_background_capacity_drain_ns: u64,
    /// Native candidate collections performed after the victim reservoir missed.
    /// Zero unless `cache-pressure-timing` is enabled.
    pub victim_collections: u64,
    /// Total nanoseconds spent collecting native victim candidates.
    pub victim_collection_ns: u64,
    /// Longest native victim-candidate collection in nanoseconds.
    pub max_victim_collection_ns: u64,
}

/// Feature-gated production telemetry for [`DirectPackedCache`].
///
/// This snapshot is available with `cache-production-diagnostics`. Enabling
/// that feature adds relaxed atomic accounting to mutation, admission, and
/// maintenance paths. The default build carries neither the counters nor their
/// hot-path cost. Nanosecond timing and its p99 histogram additionally require
/// `cache-pressure-timing`.
#[cfg(feature = "cache-production-diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectCacheProductionStats {
    /// Current live entry count tracked by production diagnostics.
    pub current_entries: usize,
    /// Current caller-accounted live weight tracked by production diagnostics.
    pub current_weight: u64,
    /// Highest live entry count observed after a capacity mutation.
    pub peak_entries: usize,
    /// Highest live weight observed after a capacity mutation.
    pub peak_weight: u64,
    /// Highest soft-limit overshoot, in basis points of the limiting capacity.
    pub peak_soft_limit_overshoot_bps: u64,
    /// Current entries above the configured soft entry limit.
    pub current_entry_debt: usize,
    /// Current weight above the configured soft weight limit.
    pub current_weight_debt: u64,
    /// Highest observed entry debt above the soft limit.
    pub peak_entry_debt: usize,
    /// Highest observed weight debt above the soft limit.
    pub peak_weight_debt: u64,
    /// Exact values currently waiting for a safe reclamation epoch.
    pub retired_entries: usize,
    /// Exact caller-accounted weight currently waiting for reclamation.
    pub retired_bytes: u64,
    /// Highest concurrently retired entry count observed at retirement time.
    pub peak_retired_entries: usize,
    /// Highest concurrently retired caller-accounted weight observed.
    pub peak_retired_bytes: u64,
    /// Foreground requests that synchronously enforced the async hard limit.
    pub foreground_hard_limit_enforcements: u64,
    /// Total foreground hard-limit enforcement time in nanoseconds.
    pub foreground_hard_limit_total_ns: u64,
    /// Approximate p99 foreground enforcement time in nanoseconds.
    pub foreground_hard_limit_p99_ns: u64,
    /// Longest foreground hard-limit enforcement time in nanoseconds.
    pub foreground_hard_limit_max_ns: u64,
    /// Background passes that drained soft-limit capacity debt.
    pub background_drains: u64,
    /// Total background drain time in nanoseconds.
    pub background_drain_total_ns: u64,
    /// Longest background drain time in nanoseconds.
    pub background_drain_max_ns: u64,
    /// Entry debt present at the start of all background drains.
    pub background_entry_debt_total: u64,
    /// Weight debt present at the start of all background drains.
    pub background_weight_debt_total: u64,
    /// Eviction batches that examined one or more victim candidates.
    pub victim_batches: u64,
    /// Candidate records examined by eviction batches.
    pub victims_examined: u64,
    /// Candidate records successfully removed by eviction batches.
    pub victims_removed: u64,
    /// Largest candidate count examined by one eviction batch.
    pub max_victims_examined_per_batch: u64,
    /// Native candidate collections performed after the reservoir missed.
    pub native_victim_collections: u64,
    /// Total native victim-collection time in nanoseconds.
    pub native_victim_collection_total_ns: u64,
    /// Longest native victim-collection time in nanoseconds.
    pub native_victim_collection_max_ns: u64,
    /// Candidates rejected because their weight exceeded the cache budget.
    pub rejected_item_too_heavy: u64,
    /// Candidates rejected because their weight did not fit compact metadata.
    pub rejected_weight_not_compact: u64,
    /// Absent candidates rejected on their first doorkeeper observation.
    pub rejected_doorkeeper_first_sighting: u64,
    /// Repeated candidates rejected by the frequency comparison gate.
    pub rejected_frequency: u64,
    /// Candidates accepted by the doorkeeper after it became warm.
    pub doorkeeper_admissions: u64,
    /// Doorkeeper filter rotations completed.
    pub doorkeeper_rotations: u64,
    /// Whether reuse-sensitive frequency admission is currently active.
    pub frequency_gate_active: bool,
    /// Changes between active and inactive frequency-gate states.
    pub frequency_gate_transitions: u64,
    /// Transitions that activated reuse-sensitive frequency admission.
    pub frequency_gate_activations: u64,
    /// Failed compare-and-swap attempts in the rolling reuse estimator.
    pub rolling_estimator_cas_retries: u64,
    /// Pressure notifications that unparked a registered maintenance worker.
    pub maintenance_worker_wakeups: u64,
    /// Wake-driven or periodic maintenance loop iterations.
    pub maintenance_worker_runs: u64,
    /// Failed maintenance or rebuild attempts observed by the worker.
    pub maintenance_worker_failures: u64,
    /// Whether capacity work is currently queued for the worker.
    pub maintenance_pending: bool,
    /// Value-allocation requests served by the direct arena.
    pub arena_allocation_requests: u64,
    /// Out-of-line boxed value allocations requested by prepared paths.
    pub boxed_allocation_requests: u64,
    /// Prepared boxed allocations reused from a reclamation pool.
    pub recycled_box_reuses: u64,
    /// Segmented arena blocks currently allocated.
    pub arena_blocks: usize,
    /// Bytes reserved by currently allocated segmented arena blocks.
    pub arena_allocated_bytes: usize,
    /// Occupied segmented-arena slots that are not retired.
    pub arena_active_allocations: usize,
    /// Bytes occupied by active segmented-arena slots.
    pub arena_active_bytes: usize,
    /// Immediately reusable vacant segmented-arena slots.
    pub arena_reusable_allocations: usize,
    /// Bytes represented by immediately reusable arena slots.
    pub arena_reusable_bytes: usize,
    /// Segmented arena blocks allocated over the cache lifetime.
    pub arena_block_growths: u64,
    /// Empty segmented arena blocks returned over the cache lifetime.
    pub arena_block_releases: u64,
}

/// Work completed by one [`PackedCache::maintain`] call.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheMaintenanceResult {
    /// Expired entries removed.
    pub expired: usize,
    /// Capacity victims removed.
    pub evicted: usize,
    /// Whether the adaptive index published a rebuilt generation.
    pub rebuilt: bool,
}

#[derive(Default)]
// Isolate hot shards on Apple cache lines and paired Intel prefetch lines.
#[repr(align(128))]
struct CacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    replacements: AtomicU64,
    removals: AtomicU64,
    evictions: AtomicU64,
    expirations: AtomicU64,
    rejected: AtomicU64,
    rebuilds: AtomicU64,
    maintenance_errors: AtomicU64,
}

#[derive(Default)]
#[repr(align(128))]
struct DirectFrequencyReuseShard {
    pending: AtomicU64,
}

#[derive(Default)]
#[repr(align(128))]
struct DirectDoorkeeperObservationShard {
    pending: AtomicU64,
}

struct DirectDoorkeeperObservations {
    published: AtomicU64,
    shards: Box<[DirectDoorkeeperObservationShard]>,
}

impl DirectDoorkeeperObservations {
    #[cfg(test)]
    fn load(&self, ordering: Ordering) -> u64 {
        self.published.load(ordering)
    }

    #[cfg(test)]
    fn store(&self, value: u64, ordering: Ordering) {
        self.published.store(value, ordering);
    }
}

#[cfg(feature = "cache-production-diagnostics")]
#[repr(align(128))]
struct DirectCacheDiagnosticCounters {
    current_entries: AtomicUsize,
    current_weight: AtomicU64,
    peak_entries: AtomicUsize,
    peak_weight: AtomicU64,
    peak_soft_limit_overshoot_bps: AtomicU64,
    peak_entry_debt: AtomicUsize,
    peak_weight_debt: AtomicU64,
    foreground_capacity_enforcements: AtomicU64,
    foreground_capacity_enforcement_ns: AtomicU64,
    max_foreground_capacity_enforcement_ns: AtomicU64,
    foreground_capacity_enforcement_histogram: [AtomicU64; u64::BITS as usize],
    background_capacity_drains: AtomicU64,
    background_capacity_drain_ns: AtomicU64,
    max_background_capacity_drain_ns: AtomicU64,
    background_entry_debt_total: AtomicU64,
    background_weight_debt_total: AtomicU64,
    victim_collections: AtomicU64,
    victim_collection_ns: AtomicU64,
    max_victim_collection_ns: AtomicU64,
    victim_batches: AtomicU64,
    victims_examined: AtomicU64,
    victims_removed: AtomicU64,
    max_victims_examined_per_batch: AtomicU64,
    rejected_item_too_heavy: AtomicU64,
    rejected_weight_not_compact: AtomicU64,
    rejected_doorkeeper_first_sighting: AtomicU64,
    rejected_frequency: AtomicU64,
    doorkeeper_admissions: AtomicU64,
    frequency_gate_state: AtomicU8,
    frequency_gate_transitions: AtomicU64,
    frequency_gate_activations: AtomicU64,
    rolling_estimator_cas_retries: AtomicU64,
    maintenance_worker_wakeups: AtomicU64,
    maintenance_worker_runs: AtomicU64,
}

#[cfg(feature = "cache-diagnostics")]
#[derive(Default)]
struct DirectPressureTimingSnapshot {
    foreground_capacity_enforcements: u64,
    foreground_capacity_enforcement_ns: u64,
    max_foreground_capacity_enforcement_ns: u64,
    background_capacity_drains: u64,
    background_capacity_drain_ns: u64,
    max_background_capacity_drain_ns: u64,
    victim_collections: u64,
    victim_collection_ns: u64,
    max_victim_collection_ns: u64,
}

#[cfg(feature = "cache-production-diagnostics")]
impl DirectCacheDiagnosticCounters {
    fn new(entries: usize, weight: u64) -> Self {
        Self {
            current_entries: AtomicUsize::new(entries),
            current_weight: AtomicU64::new(weight),
            peak_entries: AtomicUsize::new(entries),
            peak_weight: AtomicU64::new(weight),
            peak_soft_limit_overshoot_bps: AtomicU64::new(0),
            peak_entry_debt: AtomicUsize::new(0),
            peak_weight_debt: AtomicU64::new(0),
            foreground_capacity_enforcements: AtomicU64::new(0),
            foreground_capacity_enforcement_ns: AtomicU64::new(0),
            max_foreground_capacity_enforcement_ns: AtomicU64::new(0),
            foreground_capacity_enforcement_histogram: std::array::from_fn(|_| AtomicU64::new(0)),
            background_capacity_drains: AtomicU64::new(0),
            background_capacity_drain_ns: AtomicU64::new(0),
            max_background_capacity_drain_ns: AtomicU64::new(0),
            background_entry_debt_total: AtomicU64::new(0),
            background_weight_debt_total: AtomicU64::new(0),
            victim_collections: AtomicU64::new(0),
            victim_collection_ns: AtomicU64::new(0),
            max_victim_collection_ns: AtomicU64::new(0),
            victim_batches: AtomicU64::new(0),
            victims_examined: AtomicU64::new(0),
            victims_removed: AtomicU64::new(0),
            max_victims_examined_per_batch: AtomicU64::new(0),
            rejected_item_too_heavy: AtomicU64::new(0),
            rejected_weight_not_compact: AtomicU64::new(0),
            rejected_doorkeeper_first_sighting: AtomicU64::new(0),
            rejected_frequency: AtomicU64::new(0),
            doorkeeper_admissions: AtomicU64::new(0),
            frequency_gate_state: AtomicU8::new(0),
            frequency_gate_transitions: AtomicU64::new(0),
            frequency_gate_activations: AtomicU64::new(0),
            rolling_estimator_cas_retries: AtomicU64::new(0),
            maintenance_worker_wakeups: AtomicU64::new(0),
            maintenance_worker_runs: AtomicU64::new(0),
        }
    }

    fn note_capacity(
        &self,
        entries: usize,
        weight: u64,
        max_entries: Option<usize>,
        max_weight: u64,
    ) {
        self.peak_entries.fetch_max(entries, Ordering::Relaxed);
        self.peak_weight.fetch_max(weight, Ordering::Relaxed);
        let entry_debt = max_entries.map_or(0, |maximum| entries.saturating_sub(maximum));
        let weight_debt = if max_weight == u64::MAX {
            0
        } else {
            weight.saturating_sub(max_weight)
        };
        self.peak_entry_debt
            .fetch_max(entry_debt, Ordering::Relaxed);
        self.peak_weight_debt
            .fetch_max(weight_debt, Ordering::Relaxed);
        let entry_overshoot = max_entries.map_or(0, |maximum| {
            u64::try_from(entry_debt)
                .unwrap_or(u64::MAX)
                .saturating_mul(10_000)
                / u64::try_from(maximum).unwrap_or(u64::MAX).max(1)
        });
        let weight_overshoot = if max_weight == u64::MAX {
            0
        } else {
            weight_debt.saturating_mul(10_000) / max_weight.max(1)
        };
        self.peak_soft_limit_overshoot_bps
            .fetch_max(entry_overshoot.max(weight_overshoot), Ordering::Relaxed);
    }

    fn add_capacity(&self, weight: u64, max_entries: Option<usize>, max_weight: u64) {
        let entries = self
            .current_entries
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let weight = self
            .current_weight
            .fetch_add(weight, Ordering::Relaxed)
            .wrapping_add(weight);
        // An exact index publication can race the corresponding capacity
        // bookkeeping on another thread. The wrapping totals converge once
        // both operations finish; never mistake that short negative delta for
        // a physical peak.
        if entries <= usize::try_from(isize::MAX).expect("isize maximum fits usize")
            && weight <= u64::try_from(i64::MAX).expect("i64 maximum fits u64")
        {
            self.note_capacity(entries, weight, max_entries, max_weight);
        }
    }

    fn remove_capacity(&self, weight: u64) {
        self.current_entries.fetch_sub(1, Ordering::Relaxed);
        self.current_weight.fetch_sub(weight, Ordering::Relaxed);
    }

    fn replace_capacity(&self, previous: u64, replacement: u64, config: &CacheConfig) {
        let weight = if replacement >= previous {
            self.current_weight
                .fetch_add(replacement - previous, Ordering::Relaxed)
                .wrapping_add(replacement - previous)
        } else {
            self.current_weight
                .fetch_sub(previous - replacement, Ordering::Relaxed)
                .wrapping_sub(previous - replacement)
        };
        let entries = self.current_entries.load(Ordering::Relaxed);
        if entries <= usize::try_from(isize::MAX).expect("isize maximum fits usize")
            && weight <= u64::try_from(i64::MAX).expect("i64 maximum fits u64")
        {
            self.note_capacity(entries, weight, config.max_entries, config.max_weight);
        }
    }

    fn note_frequency_gate(&self, active: bool) {
        let next = if active { 2 } else { 1 };
        let previous = self.frequency_gate_state.swap(next, Ordering::Relaxed);
        if active && previous != next {
            self.frequency_gate_activations
                .fetch_add(1, Ordering::Relaxed);
        }
        if previous != 0 && previous != next {
            self.frequency_gate_transitions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_victim_batch(&self, examined: usize, removed: usize) {
        if examined == 0 {
            return;
        }
        self.victim_batches.fetch_add(1, Ordering::Relaxed);
        self.victims_examined.fetch_add(
            u64::try_from(examined).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.victims_removed.fetch_add(
            u64::try_from(removed).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.max_victims_examined_per_batch.fetch_max(
            u64::try_from(examined).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    #[cfg(feature = "cache-pressure-timing")]
    fn record_foreground_capacity_enforcement(&self, started: Instant) {
        let elapsed_ns = record_diagnostic_duration(
            &self.foreground_capacity_enforcements,
            &self.foreground_capacity_enforcement_ns,
            &self.max_foreground_capacity_enforcement_ns,
            started,
        );
        let bucket = usize::try_from(u64::BITS - 1 - elapsed_ns.leading_zeros())
            .expect("u32 histogram index fits usize");
        self.foreground_capacity_enforcement_histogram[bucket].fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "cache-pressure-timing")]
    fn record_background_capacity_drain(&self, started: Instant) {
        record_diagnostic_duration(
            &self.background_capacity_drains,
            &self.background_capacity_drain_ns,
            &self.max_background_capacity_drain_ns,
            started,
        );
    }

    #[cfg(feature = "cache-pressure-timing")]
    fn record_victim_collection(&self, started: Instant) {
        record_diagnostic_duration(
            &self.victim_collections,
            &self.victim_collection_ns,
            &self.max_victim_collection_ns,
            started,
        );
    }
}

#[cfg(feature = "cache-pressure-timing")]
fn record_diagnostic_duration(
    count: &AtomicU64,
    total_ns: &AtomicU64,
    max_ns: &AtomicU64,
    started: Instant,
) -> u64 {
    let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    count.fetch_add(1, Ordering::Relaxed);
    total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
    elapsed_ns
}

#[cfg(feature = "cache-production-diagnostics")]
fn diagnostic_histogram_percentile(
    histogram: &[AtomicU64; u64::BITS as usize],
    count: u64,
    percentile: u64,
) -> u64 {
    if count == 0 {
        return 0;
    }
    let target = count.saturating_mul(percentile).div_ceil(100).max(1);
    let mut cumulative = 0_u64;
    for (bucket, samples) in histogram.iter().enumerate() {
        cumulative = cumulative.saturating_add(samples.load(Ordering::Relaxed));
        if cumulative >= target {
            let upper_bit = u32::try_from(bucket + 1).expect("histogram bucket fits u32");
            return 1_u64
                .checked_shl(upper_bit)
                .map_or(u64::MAX, |upper| upper - 1);
        }
    }
    u64::MAX
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Victim {
    expired: bool,
    accessed: bool,
    handle: ArenaHandle,
}

impl Ord for Victim {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (
            u8::from(!self.expired),
            u8::from(self.accessed),
            self.handle,
        )
            .cmp(&(
                u8::from(!other.expired),
                u8::from(other.accessed),
                other.handle,
            ))
    }
}

impl PartialOrd for Victim {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// A memory-bounded concurrent cache for arbitrary values and binary keys.
///
/// `PackedGen` stores only a generational `u64` handle in the compact adaptive
/// index. Values live in a segmented arena whose point reads are lock-free and
/// return an owned [`CacheValue`]. Arena allocation and recycling are sharded;
/// a 31-bit generation prevents a stale index handle from observing a reused
/// slot. Capacity maintenance uses weakly consistent index scans and exact
/// handle-matched removals, avoiding a second retained copy of every key.
///
/// Values must support concurrent sharing to share the cache:
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<packedgen::PackedCache<std::cell::Cell<u8>>>();
/// ```
///
/// Values must also support transfer between threads:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<packedgen::PackedCache<std::rc::Rc<u8>>>();
/// ```
pub struct PackedCache<V> {
    index: LockFreeAtomicU64GenerationMap,
    arena: ValueArena<V>,
    config: CacheConfig,
    started: Instant,
    maintenance_gate: Mutex<()>,
    counters: Box<[CacheCounters]>,
    mutation_stripes: Box<[Mutex<()>]>,
}

/// Experimental direct-pointer variant of [`PackedCache`].
///
/// The adaptive index stores an epoch-protected entry pointer directly,
/// removing the separate generational slot arrays. Reads remain lock-free;
/// exact atomic replacements/removals return the protected old pointer that
/// must be retired, so those common writer paths remain lock-free. Operations
/// requiring multi-step TTL or upsert coordination use striped serialization.
pub struct DirectPackedCache<V> {
    index: LockFreeAtomicU64GenerationMap,
    arena: DirectValueArena<V>,
    config: CacheConfig,
    started: Instant,
    maintenance_gate: Mutex<()>,
    full_maintenance_gate: Mutex<()>,
    counters: Box<[CacheCounters]>,
    capacity: Box<[DirectCapacityCounters]>,
    hard_capacity: Option<DirectHardCapacity>,
    entry_pressure: AtomicU64,
    weight_pressure: AtomicU64,
    maintenance_pending: AtomicBool,
    maintenance_worker: Arc<ArcSwapOption<Thread>>,
    victim_scan_cursor: AtomicU64,
    victim_reservoir: Mutex<Vec<DirectVictimReservoirBatch>>,
    probation: Option<DirectProbation>,
    admission_doorkeeper: Option<DirectAdmissionDoorkeeper>,
    frequency_reuse: AtomicU64,
    frequency_reuse_shards: Option<Box<[DirectFrequencyReuseShard]>>,
    frequency_reuse_publishing: AtomicBool,
    expiration_possible: AtomicBool,
    mutation_stripes: Box<[Mutex<()>]>,
    maintenance_completions: AtomicU64,
    #[cfg(feature = "cache-production-diagnostics")]
    diagnostics: Box<DirectCacheDiagnosticCounters>,
}

/// Reusable thread-local guard for borrowed direct-cache reads.
///
/// The guard caches the index publication handles used by ordinary reads.
/// Every lookup still validates the authoritative generation pointer, and a
/// rebuild transition refreshes the cached base before direct frozen-cell
/// mutations can become visible. [`Self::refresh`] also releases obsolete
/// publication snapshots along with the arena reclamation interval.
pub struct DirectCacheGuard<'cache, V> {
    cache: &'cache DirectPackedCache<V>,
    arena: OnceCell<DirectArenaPin<'cache, V>>,
    lookup_index: crate::generation_map::AtomicReadCache<'cache>,
    index: OnceCell<crate::generation_map::AtomicReadGuard<'cache>>,
    accesses: Cell<u64>,
    pending_hits: Cell<u64>,
    pending_misses: Cell<u64>,
    pending_inserts: Cell<u64>,
    remove_precheck: Cell<bool>,
    admissions_since_refresh: usize,
    pending_lengths: Option<DirectPendingLengths>,
    counter_shard: usize,
}

/// Reusable read session that clones values under short reclamation pins.
///
/// Unlike [`DirectCacheGuard`], this session never retains an arena reader
/// epoch between calls. It is intended for cheaply shareable values such as
/// `Arc<T>` when bounded retirement memory matters more than the absolute
/// minimum borrowed-read overhead.
pub struct DirectClonedCacheGuard<'cache, V> {
    cache: &'cache DirectPackedCache<V>,
    lookup_index: crate::generation_map::AtomicReadCache<'cache>,
    accesses: Cell<u64>,
    pending_hits: Cell<u64>,
    pending_misses: Cell<u64>,
    counter_shard: usize,
}

/// Cloned/shareable read session with an automatically bounded reader epoch.
///
/// Values are cloned before this session may refresh its borrowed guard, so
/// returned values never borrow the cache. At most `refresh_interval` lookups
/// share one arena epoch.
pub struct DirectShareableCacheGuard<'cache, V> {
    guard: DirectCacheGuard<'cache, V>,
    refresh_interval: usize,
    lookups: usize,
}

/// Short-lived conditional-admission batch for [`DirectPackedCache`].
///
/// Successful admissions update accounting immediately, but capacity
/// enforcement is coalesced across an adaptive internal window and at final
/// drop. Reclamation refreshes are accumulated on the parent guard across
/// short batch scopes and run periodically during long pipelines. This
/// amortizes limit checks and epoch advancement while bounding both temporary
/// capacity debt and retired arena state.
#[must_use = "dropping the admission batch enforces the configured cache limits"]
pub struct DirectCacheAdmissionBatch<'guard, 'cache, V> {
    guard: &'guard mut DirectCacheGuard<'cache, V>,
    reservation: DirectArenaReservation<'cache, V>,
    #[cfg(not(feature = "shared-gx"))]
    route_only_admission: bool,
    protected: Cell<Option<DirectHandle>>,
    pending_entries: usize,
    pending_weight: u64,
    entry_limit: usize,
    weight_limit: u64,
}

/// Bulk-loader conditional-admission scope for [`DirectPackedCache`].
///
/// This is separate from [`DirectCacheAdmissionBatch`] so ordinary cache
/// admission keeps its exact compiled path. On caches without an entry limit,
/// successful index-length increments are coalesced until this scope is
/// dropped; key visibility and all other accounting remain immediate.
#[must_use = "dropping the bulk admission batch publishes its exact logical length"]
pub struct DirectCacheBulkAdmissionBatch<'guard, 'cache, V> {
    batch: DirectCacheAdmissionBatch<'guard, 'cache, V>,
    pending_lengths: DirectPendingLengths,
    capacity_limited: bool,
}

/// Short-lived successful-removal batch for [`DirectPackedCache`].
///
/// Exact index removal and capacity accounting remain immediate. Retired value
/// handles are published to the arena collector in groups of 64 and logical
/// index-length decrements are coalesced until refresh or drop. This amortizes
/// shared bookkeeping for delete-heavy request or pipeline processing.
#[must_use = "dropping the removal batch publishes pending retirement and length accounting"]
pub struct DirectCacheRemovalBatch<'guard, 'cache, V, const RECORD_STATS: bool = true> {
    guard: &'guard mut DirectCacheGuard<'cache, V>,
    mutation: Option<DirectArenaMutationPin<'cache, V>>,
    pending_retirements: Vec<RemovedDirectHandle>,
    pending_lengths: DirectPendingLengths,
    removals_since_refresh: usize,
}

/// Removal batch that leaves the cache's removal statistic unchanged.
pub type DirectCacheUntrackedRemovalBatch<'guard, 'cache, V> =
    DirectCacheRemovalBatch<'guard, 'cache, V, false>;

/// Short-lived replacement batch for [`DirectPackedCache`].
///
/// Index publication and capacity accounting remain immediate. Retired old
/// values are published to the epoch collector in groups of 64, and operation
/// statistics are aggregated until refresh or drop.
#[must_use = "dropping the replacement batch publishes pending retirement and statistics"]
pub struct DirectCacheReplacementBatch<'guard, 'cache, V, const RECORD_STATS: bool = true> {
    guard: &'guard mut DirectCacheGuard<'cache, V>,
    pending_retirements: Vec<RemovedDirectHandle>,
    replacements_since_refresh: usize,
}

/// Replacement batch that leaves the cache's replacement statistic unchanged.
pub type DirectCacheUntrackedReplacementBatch<'guard, 'cache, V> =
    DirectCacheReplacementBatch<'guard, 'cache, V, false>;

/// Prepared replacement pipeline with reusable exact-update scratch storage.
///
/// This separate scope leaves [`DirectCacheReplacementBatch`] unchanged for
/// ordinary and scalar callers. Prepared batches share one generation
/// snapshot for up to 64 replacements while retaining exact absent-key and
/// stale-handle fallback semantics. Its first bulk replacement lazily creates
/// a sharded, bounded recyclable-box pool in the arena owner; cache entries do
/// not carry recycler metadata.
#[cfg(feature = "prepared-keys")]
#[must_use = "dropping the prepared replacement batch publishes pending retirement and statistics"]
pub struct DirectCachePreparedReplacementBatch<'guard, 'cache, V> {
    batch: DirectCacheReplacementBatch<'guard, 'cache, V>,
    prepared_values: Vec<NonMaxU64>,
    prepared_previous: Vec<Option<NonMaxU64>>,
}

/// Caller-owned adaptive state for repeated conditional cache admissions.
///
/// This session adds no state to the cache or its ordinary read guard. Keep it
/// across a run of related admissions so it can learn whether exact existing
/// keys or genuinely new keys are currently dominant.
pub struct DirectAdaptiveAdmission<'guard, 'cache, V> {
    guard: &'guard DirectCacheGuard<'cache, V>,
    existing_streak: u8,
}

struct DirectPendingLengths {
    stripe: Cell<usize>,
    count: Cell<u16>,
    immediate_budget: Cell<u8>,
}

#[derive(Clone, Copy)]
struct DirectAdmissionContext<'guard, 'cache> {
    index: &'guard crate::generation_map::AtomicReadGuard<'cache>,
    pending_lengths: Option<&'guard DirectPendingLengths>,
    deferred_limit: Option<&'guard Cell<Option<DirectHandle>>>,
}

impl DirectPendingLengths {
    fn new() -> Self {
        Self {
            stripe: Cell::new(0),
            count: Cell::new(0),
            immediate_budget: Cell::new(0),
        }
    }

    fn use_immediate(&self) -> bool {
        let budget = self.immediate_budget.get();
        if budget == 0 {
            return false;
        }
        self.immediate_budget.set(budget - 1);
        true
    }

    fn record(&self, index: &crate::generation_map::AtomicReadGuard<'_>, stripe: usize) {
        let current = self.count.get();
        if current == u16::MAX {
            index.flush_deferred_insert_len(self.stripe.get(), current);
            self.stripe.set(stripe);
            self.count.set(1);
            return;
        }
        if current == 0 {
            self.stripe.set(stripe);
        }
        self.count.set(current + 1);
    }

    fn record_removal(&self, index: &crate::generation_map::AtomicReadGuard<'_>, stripe: usize) {
        let current = self.count.get();
        if current == u16::MAX {
            index.flush_deferred_remove_len(self.stripe.get(), current);
            self.stripe.set(stripe);
            self.count.set(1);
            return;
        }
        if current == 0 {
            self.stripe.set(stripe);
        }
        self.count.set(current + 1);
    }

    fn flush(&self, index: &crate::generation_map::AtomicReadGuard<'_>) {
        let amount = self.count.replace(0);
        if amount != 0 {
            index.flush_deferred_insert_len(self.stripe.get(), amount);
            self.immediate_budget.set(if amount == 1 {
                DIRECT_LENGTH_IMMEDIATE_RECHECK
            } else {
                0
            });
        }
    }

    fn flush_removals(&self, index: &crate::generation_map::AtomicReadGuard<'_>) {
        let amount = self.count.replace(0);
        if amount != 0 {
            index.flush_deferred_remove_len(self.stripe.get(), amount);
        }
    }
}

/// Epoch guard for a value returned by [`DirectPackedCache`].
///
/// The guard borrows its cache, so reads on many threads share no reference
/// count. It cannot outlive the cache:
///
/// ```compile_fail,E0505
/// use packedgen::{CacheConfig, DirectPackedCache};
///
/// let cache = DirectPackedCache::try_new(CacheConfig::new(1_024)).unwrap();
/// cache.insert_discard_with_options(b"key", 7_u64, 16, None).unwrap();
/// let value = cache.get(b"key").unwrap();
/// drop(cache);
/// assert_eq!(*value, 7);
/// ```
pub struct DirectCacheValue<'cache, V> {
    entry: DirectArenaValue<'cache, V>,
}

impl<V> Deref for DirectCacheValue<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.entry.value()
    }
}

impl<V> AsRef<V> for DirectCacheValue<'_, V> {
    fn as_ref(&self) -> &V {
        self
    }
}

impl<V: fmt::Debug> fmt::Debug for DirectCacheValue<'_, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}

#[derive(Default)]
#[repr(align(128))]
struct DirectCapacityCounters {
    entries: AtomicU64,
    weight: AtomicU64,
}

/// Exact committed-plus-reserved capacity for asynchronous eviction.
///
/// The ordinary capacity counters stay sharded because every mutation updates
/// them.  An asynchronous cache additionally uses this pair to reserve room
/// before publishing a value, preventing concurrent writers from crossing the
/// configured hard ceiling while background maintenance drains toward the
/// soft limit.
struct DirectHardCapacity {
    entries: AtomicU64,
    weight: AtomicU64,
    max_entries: u64,
    max_weight: u64,
}

struct DirectHardCapacityReservation<'cache> {
    capacity: Option<&'cache DirectHardCapacity>,
    entries: u64,
    weight: u64,
}

#[cfg(feature = "prepared-keys")]
enum DirectReplacementCapacityReservation<'cache> {
    Full(DirectHardCapacityReservation<'cache>),
    Delta(DirectHardCapacityReservation<'cache>),
}

// Diagnostic builds pair the removal's diagnostic debit with the hard-counter
// release and the next successful reservation's acquire. A reservation that
// reuses released capacity therefore observes the debit before its diagnostic
// add; non-diagnostic builds retain Relaxed ordering.
#[inline]
fn hard_capacity_release_ordering() -> Ordering {
    #[cfg(feature = "cache-production-diagnostics")]
    {
        Ordering::Release
    }
    #[cfg(not(feature = "cache-production-diagnostics"))]
    {
        Ordering::Relaxed
    }
}

#[inline]
fn hard_capacity_reservation_success_ordering() -> Ordering {
    #[cfg(feature = "cache-production-diagnostics")]
    {
        Ordering::Acquire
    }
    #[cfg(not(feature = "cache-production-diagnostics"))]
    {
        Ordering::Relaxed
    }
}

impl DirectHardCapacity {
    fn new(config: &CacheConfig, entries: usize, weight: u64) -> Option<Self> {
        let bps = config.async_hard_limit_bps?;
        Some(Self {
            entries: AtomicU64::new(if config.max_entries.is_some() {
                u64::try_from(entries).unwrap_or(u64::MAX)
            } else {
                0
            }),
            weight: AtomicU64::new(if config.max_weight == u64::MAX {
                0
            } else {
                weight
            }),
            max_entries: config.max_entries.map_or(u64::MAX, |maximum| {
                u64::try_from(scale_hard_limit_usize(maximum, bps)).unwrap_or(u64::MAX)
            }),
            max_weight: if config.max_weight == u64::MAX {
                u64::MAX
            } else {
                scale_hard_limit_u64(config.max_weight, bps)
            },
        })
    }

    fn try_reserve(&self, entries: u64, weight: u64) -> Option<(u64, u64)> {
        let reserved_entries = if self.max_entries == u64::MAX {
            0
        } else {
            if !try_add_bounded(&self.entries, entries, self.max_entries) {
                return None;
            }
            entries
        };
        let reserved_weight = if self.max_weight == u64::MAX {
            0
        } else if try_add_bounded(&self.weight, weight, self.max_weight) {
            weight
        } else {
            if reserved_entries != 0 {
                self.entries
                    .fetch_sub(reserved_entries, hard_capacity_release_ordering());
            }
            return None;
        };
        Some((reserved_entries, reserved_weight))
    }

    fn release(&self, entries: u64, weight: u64) {
        let ordering = hard_capacity_release_ordering();
        if entries != 0 {
            let previous = self.entries.fetch_sub(entries, ordering);
            debug_assert!(previous >= entries, "hard entry capacity underflow");
        }
        if weight != 0 {
            let previous = self.weight.fetch_sub(weight, ordering);
            debug_assert!(previous >= weight, "hard weight capacity underflow");
        }
    }
}

impl DirectHardCapacityReservation<'_> {
    fn commit_add(mut self) {
        self.entries = 0;
        self.weight = 0;
    }

    fn commit_replace(mut self, previous_weight: u64) {
        if let Some(capacity) = self.capacity {
            capacity.release(
                self.entries,
                if capacity.max_weight == u64::MAX {
                    0
                } else {
                    previous_weight
                },
            );
        }
        self.entries = 0;
        self.weight = 0;
    }

    fn commit_delta_replace(mut self, previous_weight: u64, replacement_weight: u64) {
        if let Some(capacity) = self.capacity
            && previous_weight > replacement_weight
        {
            capacity.release(
                0,
                if capacity.max_weight == u64::MAX {
                    0
                } else {
                    previous_weight - replacement_weight
                },
            );
        }
        self.entries = 0;
        self.weight = 0;
    }
}

impl Drop for DirectHardCapacityReservation<'_> {
    fn drop(&mut self) {
        if let Some(capacity) = self.capacity {
            capacity.release(self.entries, self.weight);
        }
    }
}

#[cfg(feature = "prepared-keys")]
impl DirectReplacementCapacityReservation<'_> {
    fn commit(self, previous_weight: u64, replacement_weight: u64) {
        match self {
            Self::Full(reservation) => reservation.commit_replace(previous_weight),
            Self::Delta(reservation) => {
                reservation.commit_delta_replace(previous_weight, replacement_weight);
            }
        }
    }
}

struct DirectFrequencySketch {
    counters: Box<[AtomicU8]>,
    mask: usize,
    observations: AtomicU64,
    reset_every: u64,
    epoch: AtomicU8,
    under_pressure: AtomicBool,
    victim_gate: AtomicU8,
    maximum_victim_frequency: u8,
}

impl DirectFrequencySketch {
    const MAX_FREQUENCY: u8 = 15;
    const FREQUENCY_MASK: u8 = 0x0f;
    const EPOCH_MASK: u8 = 0x0f;
    const EPOCH_SHIFT: u32 = 4;

    fn new(expected_entries: usize, maximum_victim_frequency: u8) -> Self {
        let counter_count = expected_entries
            .max(1)
            .saturating_mul(4)
            .next_power_of_two();
        Self {
            counters: std::iter::repeat_with(|| AtomicU8::new(0))
                .take(counter_count)
                .collect(),
            mask: counter_count - 1,
            observations: AtomicU64::new(0),
            reset_every: u64::try_from(expected_entries.max(1).saturating_mul(10))
                .unwrap_or(u64::MAX),
            epoch: AtomicU8::new(0),
            under_pressure: AtomicBool::new(false),
            victim_gate: AtomicU8::new(0),
            maximum_victim_frequency: maximum_victim_frequency.min(Self::MAX_FREQUENCY),
        }
    }

    fn observe(&self, hashes: [u64; 2]) -> u8 {
        let observation = self
            .observations
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if observation.is_multiple_of(self.reset_every) {
            // `fetch_update` is deprecated in favor of `try_update`, which is
            // newer than the crate's 1.88 MSRV.
            #[allow(deprecated)]
            self.epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    Some(epoch.wrapping_add(1) & Self::EPOCH_MASK)
                })
                .unwrap_or_else(|_| unreachable!("epoch update always succeeds"));
            #[allow(deprecated)]
            let _ = self
                .victim_gate
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value / 2)
                });
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        hashes
            .into_iter()
            .map(|hash| self.observe_counter(hash, epoch))
            .min()
            .unwrap_or(0)
    }

    fn observe_resident(&self, hashes: [u64; 2]) -> u8 {
        // Admissions advance the sketch's aging clock. Resident hits only
        // refresh their two distributed counters, avoiding one globally
        // contended fetch-add on every sampled read at high core counts.
        let epoch = self.epoch.load(Ordering::Acquire);
        hashes
            .into_iter()
            .map(|hash| self.observe_counter(hash, epoch))
            .min()
            .unwrap_or(0)
    }

    fn estimate(&self, hashes: [u64; 2]) -> u8 {
        let epoch = self.epoch.load(Ordering::Acquire);
        hashes
            .into_iter()
            .map(|hash| {
                let encoded = self.counters
                    [usize::try_from(hash).unwrap_or(usize::MAX) & self.mask]
                    .load(Ordering::Relaxed);
                Self::aged_frequency(encoded, epoch)
            })
            .min()
            .unwrap_or(0)
    }

    fn observe_counter(&self, hash: u64, mut epoch: u8) -> u8 {
        let counter = &self.counters[usize::try_from(hash).unwrap_or(usize::MAX) & self.mask];
        let mut encoded = counter.load(Ordering::Relaxed);
        loop {
            let frequency = Self::aged_frequency(encoded, epoch);
            if encoded >> Self::EPOCH_SHIFT == epoch && frequency == Self::MAX_FREQUENCY {
                return frequency;
            }
            let frequency = frequency.saturating_add(1).min(Self::MAX_FREQUENCY);
            let replacement = epoch << Self::EPOCH_SHIFT | frequency;
            match counter.compare_exchange_weak(
                encoded,
                replacement,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return frequency,
                Err(current) => {
                    encoded = current;
                    // A reset can advance while this counter is contended.
                    // Refreshing here prevents a retry from interpreting a
                    // concurrently published newer epoch as ancient state.
                    epoch = self.epoch.load(Ordering::Acquire);
                }
            }
        }
    }

    fn aged_frequency(encoded: u8, epoch: u8) -> u8 {
        let stored_epoch = encoded >> Self::EPOCH_SHIFT;
        // This is an approximate sketch: an untouched counter can alias after
        // sixteen complete aging windows. Such a stale collision may affect
        // admission policy but cannot change key/value correctness.
        let age = epoch.wrapping_sub(stored_epoch) & Self::EPOCH_MASK;
        let frequency = encoded & Self::FREQUENCY_MASK;
        if age >= 4 { 0 } else { frequency >> age }
    }

    fn admits(&self, frequency: u8) -> bool {
        frequency > self.victim_gate.load(Ordering::Relaxed)
    }

    fn note_victim(&self, frequency: u8, maximum_victim_frequency: u8) {
        self.under_pressure.store(true, Ordering::Relaxed);
        self.victim_gate.store(
            frequency.min(maximum_victim_frequency.min(self.maximum_victim_frequency)),
            Ordering::Relaxed,
        );
    }

    fn is_under_pressure(&self) -> bool {
        self.under_pressure.load(Ordering::Relaxed)
    }
}

struct DirectAdmissionDoorkeeper {
    filters: [Box<[AtomicU64]>; 2],
    filter_epochs: [Box<[AtomicU64]>; 2],
    generation: AtomicU64,
    observations: Box<DirectDoorkeeperObservations>,
    rotate_every: u64,
    frequency: Option<DirectFrequencySketch>,
    warmed: AtomicBool,
    admitted: AtomicUsize,
    expected_entries: usize,
    #[cfg(feature = "cache-production-diagnostics")]
    rotations: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectAdmissionDecision {
    Admit,
    RejectFirstSighting,
    RejectFrequency,
}

impl DirectAdmissionDoorkeeper {
    const BITS_PER_ENTRY: usize = 16;

    #[cfg(test)]
    fn hashes(key: &[u8]) -> [u64; 2] {
        let first = rapidhash::v3::rapidhash_v3(key);
        [first, doorkeeper_mix64(first ^ 0xd6e8_feb8_6659_fd93)]
    }

    fn blocked_location(hashes: [u64; 2], words: usize) -> (usize, u64) {
        debug_assert!(words.is_power_of_two());
        let word_mask = u64::try_from(words - 1).unwrap_or(u64::MAX);
        let word = usize::try_from((hashes[0] >> 6) & word_mask).unwrap_or(usize::MAX);
        let first_bit = usize::try_from(hashes[0] & 63).expect("six bits fit usize");
        let mut second_bit = usize::try_from(hashes[1] & 63).expect("six bits fit usize");
        if second_bit == first_bit {
            second_bit = (second_bit + 1) & 63;
        }
        (word, (1_u64 << first_bit) | (1_u64 << second_bit))
    }

    fn new(expected_entries: usize, frequency_max_gate: Option<u8>) -> Self {
        let requested_bits = expected_entries.max(1).saturating_mul(Self::BITS_PER_ENTRY);
        let words = requested_bits.div_ceil(64).next_power_of_two();
        let make_filter = || {
            std::iter::repeat_with(|| AtomicU64::new(0))
                .take(words)
                .collect::<Box<[_]>>()
        };
        let groups = words.div_ceil(DIRECT_DOORKEEPER_CLEAR_WORDS);
        let make_epochs = |initial| {
            std::iter::repeat_with(|| AtomicU64::new(initial))
                .take(groups)
                .collect::<Box<[_]>>()
        };
        let requested_observation_shards = expected_entries.max(1).div_ceil(1_024);
        let observation_shards = requested_observation_shards
            .checked_next_power_of_two()
            .unwrap_or(COUNTER_SHARDS)
            .min(COUNTER_SHARDS);
        Self {
            filters: [make_filter(), make_filter()],
            filter_epochs: [make_epochs(0), make_epochs(DIRECT_DOORKEEPER_INVALID_EPOCH)],
            generation: AtomicU64::new(0),
            observations: Box::new(DirectDoorkeeperObservations {
                published: AtomicU64::new(0),
                shards: std::iter::repeat_with(DirectDoorkeeperObservationShard::default)
                    .take(observation_shards)
                    .collect(),
            }),
            rotate_every: u64::try_from(expected_entries.max(1)).unwrap_or(u64::MAX),
            frequency: frequency_max_gate
                .map(|maximum| DirectFrequencySketch::new(expected_entries, maximum)),
            warmed: AtomicBool::new(false),
            admitted: AtomicUsize::new(0),
            expected_entries,
            #[cfg(feature = "cache-production-diagnostics")]
            rotations: AtomicU64::new(0),
        }
    }

    fn observe_hashed(&self, hashes: [u64; 2], observe_frequency: bool) -> (bool, Option<u8>) {
        self.record_observation(hashes[0]);
        loop {
            let generation = self.generation.load(Ordering::Acquire);
            let current = usize::try_from(generation & 1).expect("one bit fits usize");
            let previous = 1 - current;
            let (word, mask) = Self::blocked_location(hashes, self.filters[current].len());
            let seen = self.filter_contains(current, word, mask, generation)
                || (generation != 0 && self.filter_contains(previous, word, mask, generation - 1));
            if !self.prepare_filter_group(current, word, generation) {
                continue;
            }
            self.filters[current][word].fetch_or(mask, Ordering::Relaxed);
            let frequency = (seen && observe_frequency)
                .then(|| self.frequency.as_ref().map(|sketch| sketch.observe(hashes)))
                .flatten();
            return (seen, frequency);
        }
    }

    fn record_observation(&self, route: u64) {
        let shard =
            usize::try_from(route).unwrap_or(usize::MAX) & (self.observations.shards.len() - 1);
        let local = self.observations.shards[shard]
            .pending
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if !local.is_multiple_of(DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL) {
            return;
        }
        let previous = self.observations.published.fetch_add(
            DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL,
            Ordering::Relaxed,
        );
        let next = previous.wrapping_add(DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL);
        let rotations = if next < previous {
            1
        } else {
            next / self.rotate_every - previous / self.rotate_every
        };
        if rotations != 0 {
            // `try_update` is newer than the crate's 1.88 MSRV.
            #[allow(deprecated)]
            self.generation
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                    Some(generation.wrapping_add(rotations) & DIRECT_DOORKEEPER_EPOCH_MASK)
                })
                .unwrap_or_else(|_| unreachable!("generation update always succeeds"));
            #[cfg(feature = "cache-production-diagnostics")]
            self.rotations.fetch_add(rotations, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    fn prime_rotation_on_next_observation(&self, key: &[u8], next_total: u64) {
        let route = Self::hashes(key)[0];
        let shard =
            usize::try_from(route).unwrap_or(usize::MAX) & (self.observations.shards.len() - 1);
        self.observations.shards[shard].pending.store(
            DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL - 1,
            Ordering::Relaxed,
        );
        self.observations.store(
            next_total.saturating_sub(DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL),
            Ordering::Relaxed,
        );
    }

    #[cfg(test)]
    fn observe(&self, key: &[u8], observe_frequency: bool) -> (bool, Option<u8>) {
        self.observe_hashed(Self::hashes(key), observe_frequency)
    }

    fn filter_contains(&self, filter: usize, word: usize, mask: u64, generation: u64) -> bool {
        let group = word / DIRECT_DOORKEEPER_CLEAR_WORDS;
        self.filter_epochs[filter][group].load(Ordering::Acquire) == generation
            && self.filters[filter][word].load(Ordering::Relaxed) & mask == mask
    }

    fn prepare_filter_group(&self, filter: usize, word: usize, generation: u64) -> bool {
        let group = word / DIRECT_DOORKEEPER_CLEAR_WORDS;
        let epoch = &self.filter_epochs[filter][group];
        let mut observed = epoch.load(Ordering::Acquire);
        loop {
            if observed == generation {
                return true;
            }
            if observed & DIRECT_DOORKEEPER_GROUP_LOCK != 0 {
                spin_loop();
                observed = epoch.load(Ordering::Acquire);
                continue;
            }
            match epoch.compare_exchange_weak(
                observed,
                DIRECT_DOORKEEPER_GROUP_LOCK | generation,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(previous) => {
                    if self.generation.load(Ordering::Acquire) != generation {
                        epoch.store(previous, Ordering::Release);
                        return false;
                    }
                    let begin = group * DIRECT_DOORKEEPER_CLEAR_WORDS;
                    let end =
                        (begin + DIRECT_DOORKEEPER_CLEAR_WORDS).min(self.filters[filter].len());
                    for stale in &self.filters[filter][begin..end] {
                        stale.store(0, Ordering::Relaxed);
                    }
                    epoch.store(generation, Ordering::Release);
                    return true;
                }
                Err(current) => observed = current,
            }
        }
    }

    fn admission_decision_hashed(
        &self,
        hashes: [u64; 2],
        enforce_frequency: bool,
    ) -> DirectAdmissionDecision {
        let (seen, frequency) = self.observe_hashed(hashes, enforce_frequency);
        if !self.warmed.load(Ordering::Acquire) {
            return DirectAdmissionDecision::Admit;
        }
        if !seen {
            return DirectAdmissionDecision::RejectFirstSighting;
        }
        if enforce_frequency
            && frequency.is_some_and(|frequency| {
                self.frequency
                    .as_ref()
                    .is_some_and(|sketch| !sketch.admits(frequency))
            })
        {
            DirectAdmissionDecision::RejectFrequency
        } else {
            DirectAdmissionDecision::Admit
        }
    }

    #[cfg(feature = "cache-production-diagnostics")]
    fn rotations(&self) -> u64 {
        self.rotations.load(Ordering::Relaxed)
    }

    fn victim_frequency_hashed(&self, hashes: [u64; 2]) -> u8 {
        self.frequency
            .as_ref()
            .map_or(0, |sketch| sketch.estimate(hashes))
    }

    fn note_victim_frequency(&self, frequency: u8, maximum_victim_frequency: u8) {
        if let Some(sketch) = &self.frequency {
            sketch.note_victim(frequency, maximum_victim_frequency);
        }
    }

    fn note_access_hashed(&self, hashes: [u64; 2]) {
        if let Some(sketch) = &self.frequency {
            debug_assert!(sketch.is_under_pressure());
            sketch.observe_resident(hashes);
        }
    }

    fn tracks_resident_frequency(&self) -> bool {
        self.frequency
            .as_ref()
            .is_some_and(DirectFrequencySketch::is_under_pressure)
    }

    #[cfg(test)]
    fn note_access(&self, key: &[u8]) {
        if self.tracks_resident_frequency() {
            self.note_access_hashed(Self::hashes(key));
        }
    }

    fn protects_resident(&self, frequency: u8) -> bool {
        self.frequency
            .as_ref()
            .is_some_and(|sketch| sketch.admits(frequency))
    }

    fn note_population(&self, entries: usize) {
        self.admitted.store(entries, Ordering::Relaxed);
        if entries >= self.expected_entries {
            self.warmed.store(true, Ordering::Release);
        }
    }

    fn note_admission(&self) {
        let admitted = self
            .admitted
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if admitted >= self.expected_entries {
            self.warmed.store(true, Ordering::Release);
        }
    }
}

struct DirectVictim {
    expired: bool,
    accessed: bool,
    frequency: u8,
    handle: DirectHandle,
    key: DirectVictimKey,
}

#[derive(Clone, Copy)]
struct DirectVictimKey {
    key_start: usize,
    key_len: usize,
}

/// Outcome of validating and removing one eviction victim.
enum DirectVictimRemoval {
    /// Absent, replaced, protected, or kept because it was read.
    Kept,
    /// Removed from the index; the caller retires it.
    Removed {
        handle: DirectHandle,
        expired: bool,
        weight: u64,
    },
}

struct DirectVictimBatch {
    victims: Vec<DirectVictim>,
    key_bytes: Vec<u8>,
    next: usize,
}

impl DirectVictimBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            victims: Vec::with_capacity(capacity),
            key_bytes: Vec::new(),
            next: 0,
        }
    }

    fn push(
        &mut self,
        expired: bool,
        accessed: bool,
        frequency: u8,
        handle: DirectHandle,
        key: &[u8],
    ) {
        if self.victims.is_empty() {
            self.key_bytes
                .reserve(key.len().saturating_mul(self.victims.capacity()));
        }
        let key_start = self.key_bytes.len();
        self.key_bytes.extend_from_slice(key);
        self.victims.push(DirectVictim {
            expired,
            accessed,
            frequency,
            handle,
            key: DirectVictimKey {
                key_start,
                key_len: key.len(),
            },
        });
    }

    /// Merges repeated samples of one entry in O(n log n). A repeated sample
    /// reads the access mark after the earlier sample already took it.
    fn merge_repeated_samples(&mut self) {
        self.victims.sort_unstable_by_key(|victim| victim.handle);
        self.victims.dedup_by(|repeated, kept| {
            if repeated.handle != kept.handle {
                return false;
            }
            kept.expired |= repeated.expired;
            kept.accessed |= repeated.accessed;
            kept.frequency = kept.frequency.max(repeated.frequency);
            true
        });
    }

    /// Candidates this batch may evict without taking a second chance.
    fn eligible(&self) -> usize {
        self.victims
            .iter()
            .filter(|victim| victim.expired || !victim.accessed)
            .count()
    }

    /// Adds a later sampling round, merging entries sampled in both.
    fn absorb(&mut self, round: Self) {
        let key_offset = self.key_bytes.len();
        self.key_bytes.extend_from_slice(&round.key_bytes);
        self.victims
            .extend(round.victims.into_iter().map(|mut victim| {
                victim.key.key_start += key_offset;
                victim
            }));
        self.merge_repeated_samples();
    }

    fn key(&self, victim: &DirectVictim) -> &[u8] {
        self.key_slice(victim.key)
    }

    fn key_slice(&self, key: DirectVictimKey) -> &[u8] {
        &self.key_bytes[key.key_start..key.key_start + key.key_len]
    }

    fn is_empty(&self) -> bool {
        self.victims.is_empty()
    }

    fn len(&self) -> usize {
        self.victims.len()
    }

    fn append_reservoir_batches(self, reservoir: &mut Vec<DirectVictimReservoirBatch>) {
        let remaining = &self.victims[self.next..];
        // Sampling already cleared `accessed`. Saving that survivor would let
        // the reservoir evict it without a new CLOCK round, consuming its
        // second chance too cheaply. Expiration remains unconditional.
        let eligible = |victim: &&DirectVictim| victim.expired || !victim.accessed;
        let eligible_count = remaining.iter().filter(eligible).count();
        reservoir.reserve(eligible_count.div_ceil(DIRECT_RESERVOIR_KEY_CHUNK));
        let mut end = remaining.len();
        while end != 0 {
            let mut start = end;
            let mut candidate_count = 0;
            while start != 0 && candidate_count < DIRECT_RESERVOIR_KEY_CHUNK {
                start -= 1;
                let victim = &remaining[start];
                candidate_count += usize::from(victim.expired || !victim.accessed);
            }
            if candidate_count == 0 {
                break;
            }
            let candidates = remaining[start..end]
                .iter()
                .filter(|victim| victim.expired || !victim.accessed);
            let key_capacity = candidates.clone().fold(0_usize, |total, victim| {
                total.saturating_add(victim.key.key_len)
            });
            let mut key_ends = Vec::with_capacity(candidate_count);
            let mut key_bytes = Vec::with_capacity(key_capacity);
            for victim in candidates {
                let key = self.key_slice(victim.key);
                key_bytes.extend_from_slice(key);
                key_ends.push(key_bytes.len());
            }
            reservoir.push(DirectVictimReservoirBatch {
                key_ends,
                key_bytes,
                next: 0,
            });
            end = start;
        }
    }
}

struct DirectVictimReservoirBatch {
    key_ends: Vec<usize>,
    key_bytes: Vec<u8>,
    next: usize,
}

impl DirectVictimReservoirBatch {
    fn key(&self, index: usize) -> &[u8] {
        let start = index
            .checked_sub(1)
            .map_or(0, |previous| self.key_ends[previous]);
        &self.key_bytes[start..self.key_ends[index]]
    }

    fn exhausted(&self) -> bool {
        self.next == self.key_ends.len()
    }
}

const DIRECT_PROBATION_MAX_SHARDS: usize = 16;
/// Round-robin pops across shards approximate one FIFO only when each shard
/// holds many records, so small caches use fewer shards.
const DIRECT_PROBATION_MIN_SHARD_RECORDS: usize = 64;
/// Ghost filter bits per remembered key in each generation. A false positive
/// lets a one-hit key skip probation; at 16 bits and three set bits about 2%
/// of new keys did, costing up to a point of hit rate on scan-heavy traces.
const DIRECT_GHOST_BITS_PER_KEY: usize = 32;
/// Bits set per remembered key, all within one 64-bit word.
const DIRECT_GHOST_KEY_BITS: u32 = 5;
/// The ghost remembers between three and four generations of evicted keys.
const DIRECT_GHOST_GENERATIONS: usize = 4;
/// Each generation holds an eighth of the capacity, so evicted keys are
/// remembered for three eighths to half of it. A ghost of a full capacity
/// readmitted loop-like keys straight to the main area and lost ten points of
/// hit rate on the ARC paper's DS1 trace at 4,000,000 entries.
const DIRECT_GHOST_GENERATION_DIVISOR: usize = 8;

/// Probationary FIFO and ghost history for newly published direct-cache keys.
///
/// Records copy the key beside its published handle and are validated against
/// the index when popped, so removals and replacements leave only harmless
/// stale records. The FIFO is capped near its share of the population; past
/// the cap the oldest record is dropped, leaving that key to the ordinary
/// sweep, so the cache never retains a second copy of every key.
struct DirectProbation {
    shards: Box<[Mutex<DirectProbationShard>]>,
    len: AtomicUsize,
    next_shard: AtomicUsize,
    share_bps: u16,
    record_limit: AtomicUsize,
    ghost: DirectGhostFilter,
}

#[derive(Default)]
struct DirectProbationShard {
    records: VecDeque<(DirectHandle, usize)>,
    key_bytes: VecDeque<u8>,
}

impl DirectProbation {
    fn for_config(config: &CacheConfig) -> Option<Self> {
        (config.probation_bps != 0).then(|| {
            Self::new(
                config.probation_bps,
                config
                    .max_entries
                    .unwrap_or_else(|| config.direct_overlay_capacity()),
            )
        })
    }

    fn new(share_bps: u16, expected_entries: usize) -> Self {
        let expected_target = expected_entries.saturating_mul(usize::from(share_bps)) / 10_000;
        let shards = (expected_target / DIRECT_PROBATION_MIN_SHARD_RECORDS)
            .next_power_of_two()
            .clamp(1, DIRECT_PROBATION_MAX_SHARDS);
        // Reserving each shard's share of the record limit up front avoids the
        // doubling slack of growing to it one record at a time.
        let shard_records = Self::record_limit(expected_target, shards).div_ceil(shards);
        let probation = Self {
            shards: std::iter::repeat_with(|| {
                Mutex::new(DirectProbationShard {
                    records: VecDeque::with_capacity(shard_records),
                    key_bytes: VecDeque::new(),
                })
            })
            .take(shards)
            .collect(),
            len: AtomicUsize::new(0),
            next_shard: AtomicUsize::new(0),
            share_bps,
            record_limit: AtomicUsize::new(0),
            ghost: DirectGhostFilter::new(
                expected_entries.div_ceil(DIRECT_GHOST_GENERATION_DIVISOR),
            ),
        };
        probation.note_population(expected_entries);
        probation
    }

    /// The FIFO length above which eviction takes probationary keys first.
    fn target(&self, population: usize) -> usize {
        population.saturating_mul(usize::from(self.share_bps)) / 10_000
    }

    /// A quarter of the target plus slack per shard absorbs bursts between
    /// evictions; each record copies its key, so the cap bounds that memory.
    fn record_limit(target: usize, shards: usize) -> usize {
        target.saturating_add(target / 4).saturating_add(shards * 4)
    }

    fn note_population(&self, population: usize) {
        let limit = Self::record_limit(self.target(population), self.shards.len());
        self.record_limit.store(limit, Ordering::Relaxed);
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn enroll(&self, route: u64, key: &[u8], handle: DirectHandle) {
        if self.ghost.contains(route) {
            return;
        }
        let shard_index = usize::try_from(route >> 60).expect("four bits fit usize");
        let mut shard = self.shards[shard_index % self.shards.len()].lock();
        shard.records.push_back((handle, key.len()));
        shard.key_bytes.extend(key);
        if self.len.fetch_add(1, Ordering::Relaxed) >= self.record_limit.load(Ordering::Relaxed) {
            let (_, dropped_len) = shard
                .records
                .pop_front()
                .expect("the shard holds the record just enrolled");
            shard.key_bytes.drain(..dropped_len);
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Pops an approximately oldest record, copying its key into `key`.
    fn pop(&self, key: &mut Vec<u8>) -> Option<DirectHandle> {
        let start = self.next_shard.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.shards.len() {
            let mut shard = self.shards[start.wrapping_add(offset) % self.shards.len()].lock();
            if let Some((handle, key_len)) = shard.records.pop_front() {
                // Copy whole slices: extending from the drain iterator copies
                // one byte at a time while the shard lock is held.
                key.clear();
                let (front, back) = shard.key_bytes.as_slices();
                let from_front = key_len.min(front.len());
                key.extend_from_slice(&front[..from_front]);
                key.extend_from_slice(&back[..key_len - from_front]);
                shard.key_bytes.drain(..key_len);
                drop(shard);
                self.len.fetch_sub(1, Ordering::Relaxed);
                return Some(handle);
            }
        }
        None
    }
}

/// Approximate set of keys recently evicted from probation, in a ring of
/// generations.
///
/// Each generation holds up to `rotate_every` keys before the oldest one is
/// cleared, so a key is remembered for between three and four generations. A
/// false positive lets a new key skip probation, which is the cache's
/// behavior without probation; it never affects key/value correctness.
struct DirectGhostFilter {
    generations: [Box<[AtomicU64]>; DIRECT_GHOST_GENERATIONS],
    current: AtomicUsize,
    remembered: AtomicUsize,
    rotate_every: usize,
}

impl DirectGhostFilter {
    fn new(keys_per_generation: usize) -> Self {
        let rotate_every = keys_per_generation.max(64);
        // Exact sizing: the word is chosen by multiply-shift, not a mask, so
        // there is no power-of-two rounding of up to 2x.
        let words = rotate_every
            .saturating_mul(DIRECT_GHOST_BITS_PER_KEY)
            .div_ceil(64);
        Self {
            generations: std::array::from_fn(|_| {
                std::iter::repeat_with(|| AtomicU64::new(0))
                    .take(words)
                    .collect::<Box<[_]>>()
            }),
            current: AtomicUsize::new(0),
            remembered: AtomicUsize::new(0),
            rotate_every,
        }
    }

    /// One word from the route and five bits from an independent mix, which
    /// keeps false positives near 0.05% per full generation at 32 bits a key,
    /// about 0.2% across all generations.
    fn location(&self, route: u64) -> (usize, u64) {
        let words = u128::try_from(self.generations[0].len()).expect("usize fits u128");
        let word = usize::try_from((u128::from(route) * words) >> 64)
            .expect("the scaled word index is below the word count");
        let bits = doorkeeper_mix64(route ^ 0x9e37_79b9_7f4a_7c15);
        let mask = (0..DIRECT_GHOST_KEY_BITS).fold(0_u64, |mask, lane| {
            mask | 1_u64 << ((bits >> (lane * 6)) & 63)
        });
        (word, mask)
    }

    fn contains(&self, route: u64) -> bool {
        let (word, mask) = self.location(route);
        self.generations
            .iter()
            .any(|generation| generation[word].load(Ordering::Relaxed) & mask == mask)
    }

    fn remember(&self, route: u64) {
        let (word, mask) = self.location(route);
        let current = self.current.load(Ordering::Relaxed);
        self.generations[current][word].fetch_or(mask, Ordering::Relaxed);
        if self.remembered.fetch_add(1, Ordering::Relaxed) + 1 == self.rotate_every {
            // Exactly one caller observes the boundary. Racing callers may
            // land in either generation, which only shifts how long they are
            // remembered.
            let oldest = (current + 1) % DIRECT_GHOST_GENERATIONS;
            for word in &self.generations[oldest] {
                word.store(0, Ordering::Relaxed);
            }
            self.current.store(oldest, Ordering::Relaxed);
            self.remembered.store(0, Ordering::Relaxed);
        }
    }
}

struct UnpublishedDirectEntries<'arena, V> {
    arena: &'arena DirectValueArena<V>,
    handles: Vec<DirectHandle>,
    published: bool,
}

impl<V> Drop for UnpublishedDirectEntries<'_, V> {
    #[allow(
        unsafe_code,
        reason = "the guard exclusively owns candidates until publication succeeds"
    )]
    fn drop(&mut self) {
        if !self.published {
            for handle in self.handles.drain(..) {
                // SAFETY: the guard owns this candidate and `published` is false.
                unsafe { self.arena.drop_unpublished(handle) };
            }
        }
    }
}

/// Reusable thread-local cache read guard.
///
/// Pin once for a worker loop or request batch and call [`Self::refresh`]
/// periodically during long-running work so retired values can be reclaimed.
/// Borrowed values cannot outlive this guard.
pub struct CacheGuard<'cache, V> {
    cache: &'cache PackedCache<V>,
    arena: ArenaPin<'cache, V>,
    accesses: Cell<u64>,
    pending_hits: Cell<u64>,
    pending_misses: Cell<u64>,
    pending_inserts: Cell<u64>,
    counter_shard: usize,
}

impl<V> CacheGuard<'_, V> {
    /// Reads and records access without allocating an owned value guard.
    ///
    /// Hit and miss totals are buffered locally and published on
    /// [`Self::refresh`] or guard drop.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let value = self.lookup(key, true);
        if value.is_some() {
            self.pending_hits
                .set(self.pending_hits.get().wrapping_add(1));
        } else {
            self.pending_misses
                .set(self.pending_misses.get().wrapping_add(1));
        }
        value
    }

    /// Reads and records eviction access without updating hit/miss counters.
    ///
    /// This is the lowest-overhead read path for callers that do not require
    /// exact global read statistics.
    #[must_use]
    pub fn get_untracked(&self, key: &[u8]) -> Option<&V> {
        self.lookup(key, true)
    }

    /// Inserts a miss value while buffering successful-insert statistics.
    ///
    /// The total is published on [`Self::refresh`] or guard drop.
    ///
    /// # Errors
    ///
    /// Rejects a value whose charge exceeds the cache or compact entry limit,
    /// or an insertion whose arena partition exhausted its slot domain.
    pub fn insert_if_absent_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let outcome = self
            .cache
            .insert_if_absent_inner(key, value, weight, ttl, false)?;
        if outcome == CacheAdmissionOutcome::Inserted {
            self.pending_inserts
                .set(self.pending_inserts.get().wrapping_add(1));
        }
        Ok(outcome)
    }

    /// Reads without updating access or hit/miss counters.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<&V> {
        self.lookup(key, false)
    }

    /// Replaces a live value's expiration deadline through this pinned guard.
    pub fn touch(&self, key: &[u8], ttl: Option<Duration>) -> bool {
        let mutation = self.cache.mutation_stripe(key).lock();
        let Some(handle) = self.cache.index.get(key).map(ArenaHandle::from_index_value) else {
            return false;
        };
        let Some(entry) = self.arena.get(handle) else {
            return false;
        };
        let now = self.cache.now();
        if PackedCache::is_expired(entry, now) {
            drop(mutation);
            self.cache.expire_handle(key, handle);
            return false;
        }
        entry.set_expires_at(PackedCache::<V>::deadline_at(ttl, now));
        self.cache.arena.mark_accessed(handle);
        true
    }

    /// Releases prior protections and begins a fresh reclamation interval.
    ///
    /// Any references returned before this call must no longer be used.
    pub fn refresh(&mut self) {
        self.flush_stats();
        self.arena.refresh();
    }

    fn lookup(&self, key: &[u8], record_access: bool) -> Option<&V> {
        for _ in 0..16 {
            let handle = ArenaHandle::from_index_value(self.cache.index.get(key)?);
            let Some(entry) = self.arena.get(handle) else {
                continue;
            };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                continue;
            }
            if record_access && self.sample_access() {
                self.cache.arena.mark_accessed(handle);
            }
            return Some(entry.value());
        }
        None
    }

    fn sample_access(&self) -> bool {
        let accesses = self.accesses.get();
        self.accesses.set(accesses.wrapping_add(1));
        accesses & ACCESS_SAMPLE_MASK == 0
    }

    fn flush_stats(&self) {
        let hits = self.pending_hits.replace(0);
        let misses = self.pending_misses.replace(0);
        let inserts = self.pending_inserts.replace(0);
        let counters = &self.cache.counters[self.counter_shard];
        if hits != 0 {
            counters.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses != 0 {
            counters.misses.fetch_add(misses, Ordering::Relaxed);
        }
        if inserts != 0 {
            counters.inserts.fetch_add(inserts, Ordering::Relaxed);
        }
    }
}

impl<V> Drop for CacheGuard<'_, V> {
    fn drop(&mut self) {
        self.flush_stats();
    }
}

impl<V> PackedCache<V> {
    /// Builds an empty cache using the adaptive mixed-key index.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration or initial-index build error.
    pub fn try_new(config: CacheConfig) -> Result<Self, CacheBuildError> {
        if config.max_weight == 0 {
            return Err(CacheBuildError::Config(CacheConfigError::ZeroWeight));
        }
        if config.max_entries == Some(0) {
            return Err(CacheBuildError::Config(CacheConfigError::ZeroEntries));
        }
        if config.packed_overlay_capacity() == 0 {
            return Err(CacheBuildError::Config(
                CacheConfigError::ZeroOverlayCapacity,
            ));
        }
        if config.arena_partitions == 0 {
            return Err(CacheBuildError::Config(
                CacheConfigError::ZeroArenaPartitions,
            ));
        }
        if config.eviction_batch == 0 {
            return Err(CacheBuildError::Config(CacheConfigError::ZeroEvictionBatch));
        }
        if config.admission_doorkeeper_entries == Some(0) {
            return Err(CacheBuildError::Config(
                CacheConfigError::ZeroAdmissionDoorkeeperEntries,
            ));
        }
        if config
            .frequency_admission_min_hit_rate_bps
            .is_some_and(|minimum| minimum > 10_000)
            || config
                .frequency_admission_high_reuse
                .is_some_and(|(_, minimum)| minimum > 10_000)
        {
            return Err(CacheBuildError::Config(
                CacheConfigError::InvalidFrequencyHitRate,
            ));
        }
        if config
            .frequency_admission_high_reuse
            .is_some_and(|(high_gate, high_hit_rate)| {
                high_gate < config.frequency_admission_max_gate.unwrap_or(0)
                    || high_hit_rate < config.frequency_admission_min_hit_rate_bps.unwrap_or(0)
            })
        {
            return Err(CacheBuildError::Config(
                CacheConfigError::InvalidFrequencyTier,
            ));
        }
        let index = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            std::iter::empty::<(Box<[u8]>, NonMaxU64)>(),
            config.packed_overlay_capacity(),
            AtomicGenerationOverlay::AtomicAdaptive,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
        )?;
        Ok(Self {
            index,
            arena: ValueArena::new(config.arena_partitions),
            config,
            started: Instant::now(),
            maintenance_gate: Mutex::new(()),
            counters: std::iter::repeat_with(CacheCounters::default)
                .take(COUNTER_SHARDS)
                .collect(),
            mutation_stripes: std::iter::repeat_with(|| Mutex::new(()))
                .take(MUTATION_STRIPES)
                .collect(),
        })
    }

    /// Pins a reusable thread-local read guard.
    #[must_use]
    pub fn pin(&self) -> CacheGuard<'_, V> {
        let arena = self.arena.pin();
        let counter_shard = arena.thread_id() & (COUNTER_SHARDS - 1);
        CacheGuard {
            cache: self,
            arena,
            accesses: Cell::new(0),
            pending_hits: Cell::new(0),
            pending_misses: Cell::new(0),
            pending_inserts: Cell::new(0),
            counter_shard,
        }
    }

    /// Reads and records access to a live value.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<CacheValue<'_, V>> {
        let value = self.lookup(key, true);
        let counters = &self.counters[direct_arena_thread() & (COUNTER_SHARDS - 1)];
        if value.is_some() {
            counters.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.misses.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    /// Reads a live value without changing its approximate-LRU timestamp or
    /// hit/miss counters.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<CacheValue<'_, V>> {
        self.lookup(key, false)
    }

    /// Inserts with the configured default TTL and a shallow estimated charge.
    ///
    /// The estimate is `key.len() + size_of::<V>()`; callers storing heap-owned
    /// data should use [`Self::insert_with_options`] with a complete charge.
    ///
    /// # Errors
    ///
    /// Rejects a value whose estimated charge exceeds the cache budget or an
    /// insertion whose arena partition exhausted its handle slot domain.
    pub fn insert(
        &self,
        key: &[u8],
        value: V,
    ) -> Result<CacheInsertOutcome<'_, V>, CacheInsertError> {
        let weight = (key.len() as u64).saturating_add(std::mem::size_of::<V>() as u64);
        self.insert_with_options(key, value, weight, self.config.default_ttl)
    }

    /// Inserts while immediately retiring any previous value.
    ///
    /// This is the preferred cache/ETS-style write when the caller does not
    /// consume the old value.
    ///
    /// # Errors
    ///
    /// Rejects a value whose charge exceeds the cache or compact entry limit,
    /// or an insertion whose arena partition exhausted its slot domain.
    pub fn insert_discard(
        &self,
        key: &[u8],
        value: V,
    ) -> Result<CacheWriteOutcome, CacheInsertError> {
        let weight = (key.len() as u64).saturating_add(std::mem::size_of::<V>() as u64);
        self.insert_discard_with_options(key, value, weight, self.config.default_ttl)
    }

    /// Inserts with explicit charge and TTL while retiring the old value.
    ///
    /// # Errors
    ///
    /// Rejects a value whose charge exceeds the cache or compact entry limit,
    /// or an insertion whose arena partition exhausted its slot domain.
    #[allow(
        unsafe_code,
        reason = "crosses the arena mutation boundary while holding the same-key stripe"
    )]
    pub fn insert_discard_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheWriteOutcome, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let entry = ArenaEntry::new(value, compact_weight, self.deadline(ttl));
        let (outcome, protected) = {
            let _mutation = self.mutation_stripe(key).lock();
            // SAFETY: `_mutation` holds the stripe selected from this exact
            // key for the complete index lookup and arena mutation.
            unsafe { self.insert_discard_locked(key, entry) }?
        };
        self.enforce_limits(protected);
        Ok(outcome)
    }

    /// Inserts a miss value only while the key remains absent.
    ///
    /// This fuses the exact vacancy proof with publication and does not replace
    /// a racing cache-fill winner.
    ///
    /// # Errors
    ///
    /// Rejects a value whose charge exceeds the cache or compact entry limit,
    /// or an insertion whose arena partition exhausted its slot domain.
    pub fn insert_if_absent_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.insert_if_absent_inner(key, value, weight, ttl, true)
    }

    /// Inserts a miss value without updating insertion statistics.
    ///
    /// This preserves the same atomic winner semantics as
    /// [`Self::insert_if_absent_with_options`].
    ///
    /// # Errors
    ///
    /// Rejects a value whose charge exceeds the cache or compact entry limit,
    /// or an insertion whose arena partition exhausted its slot domain.
    pub fn insert_if_absent_untracked_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.insert_if_absent_inner(key, value, weight, ttl, false)
    }

    /// Replaces a live value without inserting an absent key.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[allow(
        unsafe_code,
        reason = "crosses the arena mutation boundary while holding the same-key stripe"
    )]
    pub fn replace_discard_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let entry = ArenaEntry::new(value, compact_weight, self.deadline(ttl));
        let mutation = self.mutation_stripe(key).lock();
        let Some(raw) = self.index.get(key) else {
            return Ok(false);
        };
        let handle = ArenaHandle::from_index_value(raw);
        // SAFETY: `mutation` holds the stripe selected from `key`, and `handle`
        // is the unchanged value loaded from that key's authoritative index.
        if unsafe { self.arena.replace_discard(handle, entry) }.is_err() {
            debug_assert!(
                false,
                "indexed cache entry remains live under its mutation stripe"
            );
            return Ok(false);
        }
        self.counters_for(key)
            .replacements
            .fetch_add(1, Ordering::Relaxed);
        drop(mutation);
        self.enforce_limits(handle);
        Ok(true)
    }

    #[allow(
        unsafe_code,
        reason = "retires an allocation that lost publication and was never externally reachable"
    )]
    fn insert_if_absent_inner(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
        record_stats: bool,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let entry = ArenaEntry::new(value, compact_weight, self.deadline(ttl));
        let inserted = self
            .arena
            .allocate_entry(entry, key_partition_hash(key))
            .map_err(|ArenaAllocationError::Full| CacheInsertError::ArenaFull)?;
        let published = self.index.get_or_insert(key, inserted.index_value());
        if published != inserted.index_value() {
            // SAFETY: `inserted` lost the atomic publication race, so its
            // handle was never stored in the index or exposed to another path.
            unsafe { self.arena.remove_discard(inserted) };
            return Ok(CacheAdmissionOutcome::Existing);
        }
        if record_stats {
            self.counters_for(key)
                .inserts
                .fetch_add(1, Ordering::Relaxed);
        }
        self.enforce_limits(inserted);
        Ok(CacheAdmissionOutcome::Inserted)
    }

    /// Inserts with an explicit total charge and TTL.
    ///
    /// `weight` should include value-owned heap memory when capacity must track
    /// real application bytes. `None` disables expiration for this item.
    ///
    /// # Errors
    ///
    /// Rejects a value whose supplied charge exceeds the cache budget or an
    /// insertion whose arena partition exhausted its handle slot domain.
    #[allow(
        unsafe_code,
        reason = "crosses the arena mutation boundary while holding the same-key stripe"
    )]
    pub fn insert_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheInsertOutcome<'_, V>, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let entry = ArenaEntry::new(value, compact_weight, self.deadline(ttl));
        let (outcome, protected) = {
            let _mutation = self.mutation_stripe(key).lock();
            // SAFETY: `_mutation` holds the stripe selected from this exact
            // key for the complete index lookup and arena mutation.
            unsafe { self.insert_locked(key, entry) }?
        };
        self.enforce_limits(protected);
        Ok(outcome)
    }

    /// Inserts or replaces while the matching cache mutation stripe is held.
    ///
    /// # Safety
    ///
    /// The caller must hold `self.mutation_stripe(key)` for this operation.
    #[allow(
        unsafe_code,
        reason = "performs arena mutation under its caller's same-key stripe proof"
    )]
    unsafe fn insert_discard_locked(
        &self,
        key: &[u8],
        mut entry: Box<ArenaEntry<V>>,
    ) -> Result<(CacheWriteOutcome, ArenaHandle), CacheInsertError> {
        if let Some(raw) = self.index.get(key) {
            let handle = ArenaHandle::from_index_value(raw);
            // SAFETY: the caller holds this key's mutation stripe, and the
            // handle is the unchanged authoritative index value for `key`.
            match unsafe { self.arena.replace_discard(handle, entry) } {
                Ok(()) => {
                    self.counters_for(key)
                        .replacements
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok((CacheWriteOutcome::Replaced, handle));
                }
                Err(returned) => entry = returned,
            }
        }
        match self.index.entry(key) {
            AtomicEntry::Occupied(raw) => {
                let handle = ArenaHandle::from_index_value(raw);
                // SAFETY: the caller holds this key's mutation stripe, and the
                // occupied entry is the authoritative index value for `key`.
                match unsafe { self.arena.replace_discard(handle, entry) } {
                    Ok(()) => {
                        self.counters_for(key)
                            .replacements
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok((CacheWriteOutcome::Replaced, handle));
                    }
                    Err(returned) => entry = returned,
                }
            }
            AtomicEntry::Vacant(vacant) => {
                let handle = self
                    .arena
                    .allocate_entry(entry, key_partition_hash(key))
                    .map_err(|ArenaAllocationError::Full| CacheInsertError::ArenaFull)?;
                // SAFETY: the caller of `insert_discard_locked` holds this
                // exact key's mutation stripe.
                let outcome =
                    unsafe { self.finish_discard(key, &vacant.insert(handle.index_value())) };
                return Ok((outcome, handle));
            }
        }
        let handle = self
            .arena
            .allocate_entry(entry, key_partition_hash(key))
            .map_err(|ArenaAllocationError::Full| CacheInsertError::ArenaFull)?;
        // SAFETY: the caller of `insert_discard_locked` holds this exact key's
        // mutation stripe.
        let outcome =
            unsafe { self.finish_discard(key, &self.index.insert(key, handle.index_value())) };
        Ok((outcome, handle))
    }

    /// Completes index publication while its same-key mutation stripe is held.
    ///
    /// # Safety
    ///
    /// The caller must hold `self.mutation_stripe(key)` and `indexed` must be
    /// the outcome of the index mutation performed while holding that stripe.
    #[allow(
        unsafe_code,
        reason = "retires the replaced arena allocation under its same-key stripe"
    )]
    unsafe fn finish_discard(
        &self,
        key: &[u8],
        indexed: &InsertOutcome<NonMaxU64>,
    ) -> CacheWriteOutcome {
        match indexed {
            InsertOutcome::Inserted => {
                self.counters_for(key)
                    .inserts
                    .fetch_add(1, Ordering::Relaxed);
                CacheWriteOutcome::Inserted
            }
            InsertOutcome::Replaced(previous) => {
                self.counters_for(key)
                    .replacements
                    .fetch_add(1, Ordering::Relaxed);
                // SAFETY: the caller holds the key's mutation stripe, and
                // `previous` was the authoritative value replaced under it.
                unsafe {
                    self.arena
                        .remove_discard(ArenaHandle::from_index_value(*previous));
                }
                CacheWriteOutcome::Replaced
            }
        }
    }

    fn validate_weight(&self, key: &[u8], weight: u64) -> Result<u32, CacheInsertError> {
        if weight > self.config.max_weight {
            self.counters_for(key)
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(CacheInsertError::ItemTooHeavy {
                weight,
                maximum: self.config.max_weight,
            });
        }
        u32::try_from(weight).map_err(|_| {
            self.counters_for(key)
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            CacheInsertError::WeightNotCompact { weight }
        })
    }

    /// Inserts or replaces while the matching cache mutation stripe is held.
    ///
    /// # Safety
    ///
    /// The caller must hold `self.mutation_stripe(key)` for this operation.
    #[allow(
        unsafe_code,
        reason = "performs arena mutation under its caller's same-key stripe proof"
    )]
    unsafe fn insert_locked(
        &self,
        key: &[u8],
        mut entry: Box<ArenaEntry<V>>,
    ) -> Result<(CacheInsertOutcome<'_, V>, ArenaHandle), CacheInsertError> {
        if let Some(raw) = self.index.get(key) {
            let handle = ArenaHandle::from_index_value(raw);
            // SAFETY: the caller holds this key's mutation stripe, and the
            // handle is the unchanged authoritative index value for `key`.
            match unsafe { self.arena.replace(handle, entry) } {
                Ok(previous) => {
                    self.counters_for(key)
                        .replacements
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok((
                        CacheInsertOutcome::Replaced(CacheValue { entry: previous }),
                        handle,
                    ));
                }
                Err(returned) => entry = returned,
            }
        }
        match self.index.entry(key) {
            AtomicEntry::Occupied(raw) => {
                let handle = ArenaHandle::from_index_value(raw);
                // SAFETY: the caller holds this key's mutation stripe, and the
                // occupied entry is the authoritative index value for `key`.
                match unsafe { self.arena.replace(handle, entry) } {
                    Ok(previous) => {
                        self.counters_for(key)
                            .replacements
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok((
                            CacheInsertOutcome::Replaced(CacheValue { entry: previous }),
                            handle,
                        ));
                    }
                    Err(returned) => entry = returned,
                }
            }
            AtomicEntry::Vacant(vacant) => {
                let handle = self
                    .arena
                    .allocate_entry(entry, key_partition_hash(key))
                    .map_err(|ArenaAllocationError::Full| CacheInsertError::ArenaFull)?;
                // SAFETY: the caller of `insert_locked` holds this exact key's
                // mutation stripe.
                let outcome =
                    unsafe { self.finish_insert(key, &vacant.insert(handle.index_value())) };
                return Ok((outcome, handle));
            }
        }

        let handle = self
            .arena
            .allocate_entry(entry, key_partition_hash(key))
            .map_err(|ArenaAllocationError::Full| CacheInsertError::ArenaFull)?;
        // SAFETY: the caller of `insert_locked` holds this exact key's mutation
        // stripe.
        let outcome =
            unsafe { self.finish_insert(key, &self.index.insert(key, handle.index_value())) };
        Ok((outcome, handle))
    }

    /// Completes index publication while its same-key mutation stripe is held.
    ///
    /// # Safety
    ///
    /// The caller must hold `self.mutation_stripe(key)` and `indexed` must be
    /// the outcome of the index mutation performed while holding that stripe.
    #[allow(
        unsafe_code,
        reason = "removes the replaced arena allocation under its same-key stripe"
    )]
    unsafe fn finish_insert(
        &self,
        key: &[u8],
        indexed: &InsertOutcome<NonMaxU64>,
    ) -> CacheInsertOutcome<'_, V> {
        match indexed {
            InsertOutcome::Inserted => {
                self.counters_for(key)
                    .inserts
                    .fetch_add(1, Ordering::Relaxed);
                CacheInsertOutcome::Inserted
            }
            InsertOutcome::Replaced(previous) => {
                self.counters_for(key)
                    .replacements
                    .fetch_add(1, Ordering::Relaxed);
                // SAFETY: the caller holds the key's mutation stripe, and
                // `previous` was the authoritative value replaced under it.
                let previous =
                    unsafe { self.arena.remove(ArenaHandle::from_index_value(*previous)) };
                previous.map_or(CacheInsertOutcome::Inserted, |entry| {
                    CacheInsertOutcome::Replaced(CacheValue { entry })
                })
            }
        }
    }

    /// Removes and returns a value when present.
    #[allow(
        unsafe_code,
        reason = "removes the authoritative arena allocation under its same-key stripe"
    )]
    pub fn remove(&self, key: &[u8]) -> Option<CacheValue<'_, V>> {
        let _mutation = self.mutation_stripe(key).lock();
        let handle = ArenaHandle::from_index_value(self.index.remove(key)?);
        // SAFETY: `_mutation` holds the stripe for `key`, and `handle` was just
        // removed from that key's authoritative index entry.
        let entry = unsafe { self.arena.remove(handle) }?;
        self.counters_for(key)
            .removals
            .fetch_add(1, Ordering::Relaxed);
        Some(CacheValue { entry })
    }

    /// Removes a key without retaining its previous value.
    #[allow(
        unsafe_code,
        reason = "retires the authoritative arena allocation under its same-key stripe"
    )]
    pub fn remove_discard(&self, key: &[u8]) -> bool {
        if self.index.get(key).is_none() {
            return false;
        }
        let _mutation = self.mutation_stripe(key).lock();
        let Some(raw) = self.index.remove(key) else {
            return false;
        };
        // SAFETY: `_mutation` holds the stripe for `key`, and `raw` was just
        // removed from that key's authoritative index entry.
        let removed = unsafe {
            self.arena
                .remove_discard(ArenaHandle::from_index_value(raw))
        };
        if removed {
            self.counters_for(key)
                .removals
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Replaces the expiration deadline of a currently live value.
    ///
    /// `None` makes the item persistent until eviction or explicit removal.
    #[allow(
        unsafe_code,
        reason = "updates arena expiry while holding the same-key mutation stripe"
    )]
    pub fn touch(&self, key: &[u8], ttl: Option<Duration>) -> bool {
        let mutation = self.mutation_stripe(key).lock();
        let Some(handle) = self.index.get(key).map(ArenaHandle::from_index_value) else {
            return false;
        };
        let now = self.now();
        // SAFETY: `mutation` holds the stripe selected from `key`, and `handle`
        // is the unchanged authoritative index value loaded under that stripe.
        match unsafe {
            self.arena
                .update_expiry(handle, now, Self::deadline_at(ttl, now))
        } {
            Some(true) => true,
            Some(false) => {
                drop(mutation);
                self.expire_handle(key, handle);
                false
            }
            None => false,
        }
    }

    /// Returns the current live arena entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    /// Returns whether the cache currently has no arena entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the current caller-accounted live weight.
    #[must_use]
    pub fn weight(&self) -> u64 {
        self.arena.weight()
    }

    /// Captures capacity state and cumulative operation counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.counters.iter().fold(
            CacheStats {
                entries: self.len(),
                weight: self.weight(),
                max_weight: self.config.max_weight,
                ..CacheStats::default()
            },
            |mut total, counters| {
                total.hits += counters.hits.load(Ordering::Relaxed);
                total.misses += counters.misses.load(Ordering::Relaxed);
                total.inserts += counters.inserts.load(Ordering::Relaxed);
                total.replacements += counters.replacements.load(Ordering::Relaxed);
                total.removals += counters.removals.load(Ordering::Relaxed);
                total.evictions += counters.evictions.load(Ordering::Relaxed);
                total.expirations += counters.expirations.load(Ordering::Relaxed);
                total.rejected += counters.rejected.load(Ordering::Relaxed);
                total.rebuilds += counters.rebuilds.load(Ordering::Relaxed);
                total.maintenance_errors += counters.maintenance_errors.load(Ordering::Relaxed);
                total
            },
        )
    }

    /// Purges expired entries, enforces limits, and conditionally rebuilds the
    /// adaptive index.
    ///
    /// # Errors
    ///
    /// Returns an index construction error if adaptive rebuild fails. Point
    /// operations remain usable after such a failure.
    pub fn maintain(&self) -> Result<CacheMaintenanceResult, FrozenBuildError> {
        let _maintenance = self.maintenance_gate.lock();
        let now = self.now();
        let expired = self.purge_expired_locked(now);
        let evicted = self.evict_to_limits_locked(now, None);
        let rebuilt = self
            .index
            .try_rebuild_adaptive_if_needed(self.config.rebuild_policy)?
            .is_some();
        if rebuilt {
            self.maintenance_counters()
                .rebuilds
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(CacheMaintenanceResult {
            expired,
            evicted,
            rebuilt,
        })
    }

    /// Starts periodic maintenance on a dedicated thread.
    ///
    /// Dropping the returned handle requests shutdown and joins the worker.
    #[must_use]
    pub fn spawn_maintenance(self: &Arc<Self>, interval: Duration) -> CacheMaintenance
    where
        V: Send + Sync + 'static,
    {
        let interval = interval.max(Duration::from_millis(1));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let cache = Arc::clone(self);
        let join = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                if cache.maintain().is_err() {
                    cache
                        .maintenance_counters()
                        .maintenance_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                thread::park_timeout(interval);
            }
        });
        let worker = join.thread().clone();
        CacheMaintenance {
            stop,
            worker,
            join: Some(join),
            registration: None,
            registered_worker: None,
        }
    }

    fn lookup(&self, key: &[u8], record_access: bool) -> Option<CacheValue<'_, V>> {
        // A replacement publishes its new handle before retiring the old arena
        // slot. Readers that caught the old handle retry; the wider bound keeps
        // rapid same-key replacement from surfacing a transient false miss.
        for _ in 0..16 {
            let handle = ArenaHandle::from_index_value(self.index.get(key)?);
            let Some(entry) = self.arena.get(handle) else {
                continue;
            };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES {
                let now = self.now();
                if expires_at <= now {
                    self.expire_handle(key, handle);
                    continue;
                }
                if record_access {
                    self.arena.mark_accessed(handle);
                }
            } else if record_access
                && self.counters_for(key).hits.load(Ordering::Relaxed) & ACCESS_SAMPLE_MASK == 0
            {
                self.arena.mark_accessed(handle);
            }
            return Some(CacheValue { entry });
        }
        None
    }

    #[allow(
        unsafe_code,
        reason = "retires an expired allocation under its same-key stripe"
    )]
    fn expire_handle(&self, key: &[u8], handle: ArenaHandle) -> bool {
        let _mutation = self.mutation_stripe(key).lock();
        let removed = self
            .index
            .remove_if(key, |current| *current == handle.index_value());
        if removed.is_none() {
            return false;
        }
        // SAFETY: `_mutation` holds the stripe for `key`, and `remove_if`
        // confirmed and removed this exact authoritative handle.
        let removed = unsafe { self.arena.remove_discard(handle) };
        if removed {
            self.counters_for(key)
                .expirations
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    fn enforce_limits(&self, protected: ArenaHandle) {
        if !self.over_limit() {
            return;
        }
        let _maintenance = self.maintenance_gate.lock();
        self.evict_to_limits_locked(self.now(), Some(protected));
    }

    fn purge_expired_locked(&self, now: u64) -> usize {
        let mut removed = 0;
        for _ in 0..MAX_EXPIRATION_BATCHES {
            let batch = self.remove_victim_batch(now, true, None);
            removed += batch;
            if batch < self.config.eviction_batch {
                break;
            }
        }
        removed
    }

    fn evict_to_limits_locked(&self, now: u64, protected: Option<ArenaHandle>) -> usize {
        let mut removed = 0;
        while self.over_limit() {
            let batch = self.remove_victim_batch(now, false, protected);
            if batch == 0 {
                break;
            }
            removed += batch;
        }
        removed
    }

    #[allow(
        unsafe_code,
        reason = "retires selected allocations under their same-key stripes"
    )]
    fn remove_victim_batch(
        &self,
        now: u64,
        expired_only: bool,
        protected: Option<ArenaHandle>,
    ) -> usize {
        let mut victims = BinaryHeap::with_capacity(self.config.eviction_batch + 1);
        self.index.scan_entries(&mut |_, raw| {
            let handle = ArenaHandle::from_index_value(raw);
            if Some(handle) == protected {
                return;
            }
            let Some(entry) = self.arena.get(handle) else {
                return;
            };
            let expired = Self::is_expired(&entry, now);
            if expired_only && !expired {
                return;
            }
            victims.push(Victim {
                expired,
                accessed: self.arena.take_accessed(handle),
                handle,
            });
            if victims.len() > self.config.eviction_batch {
                victims.pop();
            }
        });
        if victims.is_empty() {
            return 0;
        }
        let selected = victims
            .into_sorted_vec()
            .into_iter()
            .map(|victim| victim.handle)
            .collect::<Vec<_>>();
        let mut keys = Vec::<(ArenaHandle, Box<[u8]>)>::with_capacity(selected.len());
        self.index.scan_entries(&mut |key, raw| {
            let handle = ArenaHandle::from_index_value(raw);
            if selected.contains(&handle) && !keys.iter().any(|(seen, _)| *seen == handle) {
                keys.push((handle, key.into()));
            }
        });

        let mut removed = 0;
        for handle in selected {
            let Some(position) = keys.iter().position(|(seen, _)| *seen == handle) else {
                continue;
            };
            let (_, key) = keys.swap_remove(position);
            let _mutation = self.mutation_stripe(&key).lock();
            if self
                .index
                .remove_if(&key, |current| *current == handle.index_value())
                .is_none()
            {
                continue;
            }
            let was_expired = self
                .arena
                .get(handle)
                .is_some_and(|entry| Self::is_expired(&entry, now));
            // SAFETY: `_mutation` holds the stripe for `key`, and `remove_if`
            // confirmed and removed this exact authoritative handle.
            if !unsafe { self.arena.remove_discard(handle) } {
                continue;
            }
            if was_expired {
                self.counters_for(&key)
                    .expirations
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters_for(&key)
                    .evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            removed += 1;
            if !expired_only && !self.over_limit() {
                break;
            }
        }
        removed
    }

    fn over_limit(&self) -> bool {
        (self.config.max_weight != u64::MAX && self.weight() > self.config.max_weight)
            || self
                .config
                .max_entries
                .is_some_and(|maximum| self.len() > maximum)
    }

    fn is_expired(entry: &ArenaEntry<V>, now: u64) -> bool {
        let expires_at = entry.expires_at();
        expires_at != NEVER_EXPIRES && expires_at <= now
    }

    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos() / EXPIRY_TICK_NANOS)
            .unwrap_or(MAX_EXPIRY_TICK)
            .min(MAX_EXPIRY_TICK)
    }

    fn deadline_at(ttl: Option<Duration>, now: u64) -> u64 {
        ttl.map_or(NEVER_EXPIRES, |ttl| {
            let nanos = ttl.as_nanos();
            let ticks = if nanos == 0 {
                0
            } else {
                // `now` floors elapsed time, so cover the unknown tick phase.
                nanos.div_ceil(EXPIRY_TICK_NANOS).saturating_add(1)
            };
            let ttl = u64::try_from(ticks).unwrap_or(MAX_EXPIRY_TICK);
            now.saturating_add(ttl).min(MAX_EXPIRY_TICK)
        })
    }

    fn deadline(&self, ttl: Option<Duration>) -> u64 {
        ttl.map_or(NEVER_EXPIRES, |ttl| {
            Self::deadline_at(Some(ttl), self.now())
        })
    }

    fn counters_for(&self, key: &[u8]) -> &CacheCounters {
        &self.counters[key_counter_hash(key) & (COUNTER_SHARDS - 1)]
    }

    fn maintenance_counters(&self) -> &CacheCounters {
        &self.counters[0]
    }

    fn mutation_stripe(&self, key: &[u8]) -> &Mutex<()> {
        &self.mutation_stripes[key_counter_hash(key) & (MUTATION_STRIPES - 1)]
    }
}

impl<'cache, V> DirectCacheGuard<'cache, V> {
    /// Reads and records a direct-cache hit or miss.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let value = self.lookup(key, true);
        if value.is_some() {
            self.pending_hits
                .set(self.pending_hits.get().wrapping_add(1));
        } else {
            self.pending_misses
                .set(self.pending_misses.get().wrapping_add(1));
        }
        value
    }

    /// Reads through an exact prepared hot-key handle and records a hit or miss.
    ///
    /// The original key bytes are always verified. Stale, wrong-key,
    /// cross-cache, overlay, and post-rebuild handles transparently use the
    /// ordinary exact lookup path.
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn get_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<&V> {
        let value = self.lookup_prepared(key, prepared, true);
        if value.is_some() {
            self.pending_hits
                .set(self.pending_hits.get().wrapping_add(1));
        } else {
            self.pending_misses
                .set(self.pending_misses.get().wrapping_add(1));
        }
        value
    }

    /// Reads through an exact prepared hot-key handle without global read statistics.
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn get_prepared_untracked(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<&V> {
        self.lookup_prepared(key, prepared, true)
    }

    /// Reads while retaining CLOCK access but not global hit/miss statistics.
    #[must_use]
    pub fn get_untracked(&self, key: &[u8]) -> Option<&V> {
        self.lookup(key, true)
    }

    /// Reads without changing CLOCK state or global statistics.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<&V> {
        self.lookup(key, false)
    }

    /// Inserts only if absent and buffers a successful-insert statistic.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn insert_if_absent_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let index = self.index.get_or_init(|| self.cache.index.read_guard());
        let outcome = self.cache.insert_if_absent_inner(
            key,
            value,
            weight,
            ttl,
            false,
            Some(DirectAdmissionContext {
                index,
                pending_lengths: self.pending_lengths.as_ref(),
                deferred_limit: None,
            }),
        )?;
        if outcome == CacheAdmissionOutcome::Inserted {
            self.pending_inserts
                .set(self.pending_inserts.get().wrapping_add(1));
        }
        Ok(outcome)
    }

    /// Starts a caller-owned adaptive conditional-admission session.
    ///
    /// The session learns existing-key and new-key phases without adding state
    /// to this ordinary read guard. Keep it across a run of related cache-fill
    /// attempts to amortize the first exact occupancy observation.
    pub fn adaptive_admission(&self) -> DirectAdaptiveAdmission<'_, 'cache, V> {
        DirectAdaptiveAdmission {
            guard: self,
            existing_streak: 0,
        }
    }

    /// Starts a conditional-admission batch.
    ///
    /// Capacity accounting remains immediately visible, while eviction is
    /// coalesced across a window derived from configured entry/weight limits.
    /// Long pipelines self-flush and periodically advance reclamation; repeated
    /// short batches share that reclamation cadence through this guard.
    /// Dropping the returned batch enforces any remaining capacity debt.
    pub fn admission_batch(&mut self) -> DirectCacheAdmissionBatch<'_, 'cache, V> {
        #[cfg(not(feature = "shared-gx"))]
        let route_only_admission = self
            .index
            .get_or_init(|| self.cache.index.read_guard())
            .uses_route_only_admission();
        #[cfg(feature = "shared-gx")]
        self.index.get_or_init(|| self.cache.index.read_guard());
        let entry_limit = self.cache.admission_batch_entry_limit();
        let weight_limit = self.cache.admission_batch_weight_limit();
        let reservation = self.cache.arena.reservation();
        DirectCacheAdmissionBatch {
            guard: self,
            reservation,
            #[cfg(not(feature = "shared-gx"))]
            route_only_admission,
            protected: Cell::new(None),
            pending_entries: 0,
            pending_weight: 0,
            entry_limit,
            weight_limit,
        }
    }

    /// Starts a conditional-admission scope optimized for bulk loading.
    ///
    /// On a cache without an entry-count limit, [`DirectPackedCache::len`] may
    /// omit successful admissions until this scope is dropped. Exact key
    /// lookup, duplicate prevention, value weight, and final length are
    /// unchanged. Capacity-bounded caches retain their ordinary accounting.
    pub fn bulk_admission_batch(&mut self) -> DirectCacheBulkAdmissionBatch<'_, 'cache, V> {
        let capacity_limited =
            self.cache.config.max_entries.is_some() || self.cache.config.max_weight != u64::MAX;
        DirectCacheBulkAdmissionBatch {
            batch: self.admission_batch(),
            pending_lengths: DirectPendingLengths::new(),
            capacity_limited,
        }
    }

    /// Starts a delete-heavy scope that batches value retirement.
    ///
    /// Key absence, capacity, and weight remain visible immediately. Removal
    /// statistics publish at each automatic refresh and when the scope drops.
    pub fn removal_batch(&mut self) -> DirectCacheRemovalBatch<'_, 'cache, V> {
        DirectCacheRemovalBatch {
            guard: self,
            mutation: None,
            pending_retirements: Vec::with_capacity(
                crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            ),
            pending_lengths: DirectPendingLengths::new(),
            removals_since_refresh: 0,
        }
    }

    /// Starts a delete-heavy scope that omits removal statistics.
    ///
    /// Key visibility, capacity, weight, and reclamation are identical to
    /// [`Self::removal_batch`]. Use this when request metrics are collected by
    /// a surrounding cache or service layer.
    pub fn removal_batch_untracked(&mut self) -> DirectCacheUntrackedRemovalBatch<'_, 'cache, V> {
        DirectCacheRemovalBatch {
            guard: self,
            mutation: None,
            pending_retirements: Vec::with_capacity(
                crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            ),
            pending_lengths: DirectPendingLengths::new(),
            removals_since_refresh: 0,
        }
    }

    /// Starts a replacement-heavy scope that batches old-value retirement.
    ///
    /// New values and capacity changes publish immediately. Replacement
    /// statistics publish at each automatic refresh and when the scope drops.
    pub fn replacement_batch(&mut self) -> DirectCacheReplacementBatch<'_, 'cache, V> {
        DirectCacheReplacementBatch {
            guard: self,
            pending_retirements: Vec::with_capacity(
                crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            ),
            replacements_since_refresh: 0,
        }
    }

    /// Starts a replacement-heavy scope that omits replacement statistics.
    pub fn replacement_batch_untracked(
        &mut self,
    ) -> DirectCacheUntrackedReplacementBatch<'_, 'cache, V> {
        DirectCacheReplacementBatch {
            guard: self,
            pending_retirements: Vec::with_capacity(
                crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            ),
            replacements_since_refresh: 0,
        }
    }

    /// Starts a prepared replacement pipeline with reusable 64-item scratch.
    #[cfg(feature = "prepared-keys")]
    pub fn prepared_replacement_batch(
        &mut self,
    ) -> DirectCachePreparedReplacementBatch<'_, 'cache, V> {
        DirectCachePreparedReplacementBatch {
            batch: self.replacement_batch(),
            prepared_values: Vec::new(),
            prepared_previous: Vec::new(),
        }
    }

    /// Replaces a live value's expiration deadline through this pinned guard.
    #[allow(
        unsafe_code,
        reason = "the lookup index and arena pin belong to this exact cache guard"
    )]
    pub fn touch(&self, key: &[u8], ttl: Option<Duration>) -> bool {
        let arena = self.arena();
        if ttl.is_none() {
            let Some(raw) = self.lookup_index.get_protected(key) else {
                return false;
            };
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(arena, handle) };
            let expires_at = entry.expires_at();
            if expires_at == NEVER_EXPIRES {
                entry.mark_accessed();
                return true;
            }
            if expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                return false;
            }
        }
        self.cache.touch_pinned_locked(arena, key, ttl)
    }

    /// Replaces expiration through an exact prepared hot-key handle.
    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared lookup index and arena pin belong to this exact cache guard"
    )]
    pub fn touch_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        ttl: Option<Duration>,
    ) -> bool {
        let arena = self.arena();
        if ttl.is_none() {
            let Some(raw) = self.lookup_index.get_prepared(key, prepared) else {
                return false;
            };
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged prepared-index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(arena, handle) };
            let expires_at = entry.expires_at();
            if expires_at == NEVER_EXPIRES {
                entry.mark_accessed();
                return true;
            }
            if expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                return false;
            }
        }
        self.cache
            .touch_prepared_pinned_locked(arena, key, prepared, ttl)
    }

    /// Removes a value without retaining it, reusing this guard's epoch and
    /// statistics buffers.
    #[cold]
    pub fn remove_discard(&self, key: &[u8]) -> bool {
        if self.remove_precheck.get() && self.lookup_index.get_protected(key).is_none() {
            return false;
        }
        let removed = self.cache.remove_discard(key);
        self.remove_precheck.set(!removed);
        removed
    }

    /// Removes through an exact prepared hot-key handle without retaining the value.
    #[cfg(feature = "prepared-keys")]
    pub fn remove_discard_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> bool {
        if self.remove_precheck.get() && self.lookup_index.get_prepared(key, prepared).is_none() {
            return false;
        }
        let removed = self.cache.remove_discard_prepared(key, prepared);
        self.remove_precheck.set(!removed);
        removed
    }

    /// Inserts or replaces through an exact prepared hot-key handle.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[cfg(feature = "prepared-keys")]
    pub fn insert_discard_prepared_with_options(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheWriteOutcome, CacheInsertError> {
        let Some(arena) = self.arena.get() else {
            return self
                .cache
                .insert_discard_prepared_with_options(key, prepared, value, weight, ttl);
        };
        let (result, replacement) = self
            .cache
            .insert_discard_prepared_pinned(arena, key, prepared, value, weight, ttl)?;
        self.cache.enforce_limits(replacement);
        Ok(result)
    }

    /// Replaces a live value without inserting an absent key, reusing this
    /// guard's arena pin when reads have already initialized it.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn replace_discard_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let Some(arena) = self.arena.get() else {
            return self
                .cache
                .replace_discard_with_options(key, value, weight, ttl);
        };
        let (replaced, replacement) = self
            .cache
            .replace_discard_pinned(arena, key, value, weight, ttl)?;
        if let Some(replacement) = replacement {
            self.cache.enforce_limits(replacement);
        }
        Ok(replaced)
    }

    /// Replaces a live value through an exact prepared hot-key handle without
    /// inserting an absent key.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[cfg(feature = "prepared-keys")]
    pub fn replace_discard_prepared_with_options(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let Some(arena) = self.arena.get() else {
            return self
                .cache
                .replace_discard_prepared_with_options(key, prepared, value, weight, ttl);
        };
        let (replaced, replacement) = self
            .cache
            .replace_discard_prepared_pinned(arena, key, prepared, value, weight, ttl)?;
        if let Some(replacement) = replacement {
            self.cache.enforce_limits(replacement);
        }
        Ok(replaced)
    }

    /// Starts fresh index and reclamation intervals and flushes buffered state.
    ///
    /// Any references returned before this call must no longer be used.
    pub fn refresh(&mut self) {
        self.flush_lengths();
        self.flush_stats();
        self.lookup_index.refresh();
        if let Some(arena) = self.arena.get_mut() {
            arena.refresh();
        }
        // Release the guarded-admission generation without immediately
        // reserving its successor. A caller commonly refreshes immediately
        // before maintenance; eagerly repinning here would make that rebuild
        // wait for this same idle guard. The next guarded mutation lazily pins
        // the then-current generation through `get_or_init`.
        self.index.take();
        self.admissions_since_refresh = 0;
    }

    /// Publishes buffered state and releases every reader/writer epoch.
    ///
    /// Any references returned before this call must no longer be used. The
    /// next read or guarded mutation lazily pins the then-current epochs.
    pub fn quiesce(&mut self) {
        self.flush_lengths();
        self.flush_stats();
        self.lookup_index.refresh();
        self.arena.take();
        self.index.take();
        self.admissions_since_refresh = 0;
    }

    fn refresh_guarded_batch(&mut self) {
        self.refresh();
        self.index.get_or_init(|| self.cache.index.read_guard());
    }

    #[allow(
        unsafe_code,
        reason = "the lookup index and arena pin belong to this exact cache guard"
    )]
    fn lookup(&self, key: &[u8], record_access: bool) -> Option<&V> {
        let arena = self.arena();
        for _ in 0..16 {
            let raw = self.lookup_index.get_protected(key)?;
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(arena, handle) };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                continue;
            }
            if record_access {
                // Every hit marks: probation and the sweep both need it, and
                // an already-set mark costs only a load of the metadata word
                // just read for expiry. Only the frequency sketch is sampled.
                entry.mark_accessed();
                if self.sample_access(entry.weight()) {
                    self.cache.note_frequency_access(key);
                }
            }
            return Some(entry.value());
        }
        None
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared lookup index and arena pin belong to this exact cache guard"
    )]
    fn lookup_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        record_access: bool,
    ) -> Option<&V> {
        let arena = self.arena();
        for _ in 0..16 {
            let raw = self.lookup_index.get_prepared(key, prepared)?;
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged prepared-index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(arena, handle) };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                continue;
            }
            if record_access {
                // Every hit marks: probation and the sweep both need it, and
                // an already-set mark costs only a load of the metadata word
                // just read for expiry. Only the frequency sketch is sampled.
                entry.mark_accessed();
                if self.sample_access(entry.weight()) {
                    self.cache.note_frequency_access(key);
                }
            }
            return Some(entry.value());
        }
        None
    }

    fn arena(&self) -> &DirectArenaPin<'cache, V> {
        self.arena.get_or_init(|| self.cache.arena.pin())
    }

    fn sample_access(&self, weight: u64) -> bool {
        let accesses = self.accesses.get();
        self.accesses.set(accesses.wrapping_add(1));
        accesses & self.cache.access_sample_mask(weight) == 0
    }

    fn flush_stats(&self) {
        let hits = self.pending_hits.replace(0);
        let misses = self.pending_misses.replace(0);
        let inserts = self.pending_inserts.replace(0);
        let counters = &self.cache.counters[self.counter_shard];
        if hits != 0 {
            counters.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses != 0 {
            counters.misses.fetch_add(misses, Ordering::Relaxed);
        }
        self.cache
            .record_frequency_reuse_shard(self.counter_shard, hits, misses);
        if inserts != 0 {
            counters.inserts.fetch_add(inserts, Ordering::Relaxed);
        }
    }

    fn flush_lengths(&self) {
        if let (Some(pending), Some(index)) = (&self.pending_lengths, self.index.get()) {
            pending.flush(index);
        }
    }
}

impl<V> DirectClonedCacheGuard<'_, V> {
    /// Clones a live value while recording access and buffering statistics.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let value = self.lookup(key, true);
        if value.is_some() {
            self.pending_hits
                .set(self.pending_hits.get().wrapping_add(1));
        } else {
            self.pending_misses
                .set(self.pending_misses.get().wrapping_add(1));
        }
        value
    }

    /// Clones a live value without recording access or operation counters.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.lookup(key, false)
    }

    /// Publishes buffered counters and adopts the current index generation.
    pub fn refresh(&mut self) {
        self.flush_stats();
        self.lookup_index.refresh();
    }

    #[allow(
        unsafe_code,
        reason = "clones only while the exact index handle is protected by a short cache pin"
    )]
    fn lookup(&self, key: &[u8], record_access: bool) -> Option<V>
    where
        V: Clone,
    {
        for _ in 0..16 {
            let pin = self.cache.arena.pin();
            let raw = self.lookup_index.get_protected(key)?;
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and short pin belong to this cache.
            let entry = unsafe { protected_direct_entry(&pin, handle) };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.cache.now() {
                self.cache.expire_handle(key, handle);
                continue;
            }
            if record_access {
                // Every hit marks: probation and the sweep both need it, and
                // an already-set mark costs only a load of the metadata word
                // just read for expiry. Only the frequency sketch is sampled.
                entry.mark_accessed();
                if self.sample_access(entry.weight()) {
                    self.cache.note_frequency_access(key);
                }
            }
            return Some(entry.value().clone());
        }
        None
    }

    fn sample_access(&self, weight: u64) -> bool {
        let accesses = self.accesses.get();
        self.accesses.set(accesses.wrapping_add(1));
        accesses & self.cache.access_sample_mask(weight) == 0
    }

    fn flush_stats(&self) {
        let hits = self.pending_hits.replace(0);
        let misses = self.pending_misses.replace(0);
        let counters = &self.cache.counters[self.counter_shard];
        if hits != 0 {
            counters.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses != 0 {
            counters.misses.fetch_add(misses, Ordering::Relaxed);
        }
        self.cache
            .record_frequency_reuse_shard(self.counter_shard, hits, misses);
    }
}

impl<V> Drop for DirectClonedCacheGuard<'_, V> {
    fn drop(&mut self) {
        self.flush_stats();
    }
}

impl<V> DirectShareableCacheGuard<'_, V> {
    /// Clones a live value and advances the reader epoch at the configured bound.
    #[must_use]
    pub fn get(&mut self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let value = self.guard.get(key).cloned();
        self.note_lookup();
        value
    }

    /// Clones a live value without access/statistics accounting.
    #[must_use]
    pub fn peek(&mut self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let value = self.guard.peek(key).cloned();
        self.note_lookup();
        value
    }

    /// Immediately publishes buffered state and starts a fresh reader epoch.
    pub fn refresh(&mut self) {
        self.guard.quiesce();
        self.lookups = 0;
    }

    fn note_lookup(&mut self) {
        self.lookups += 1;
        if self.lookups == self.refresh_interval {
            self.refresh();
        }
    }
}

impl<V> DirectAdaptiveAdmission<'_, '_, V> {
    /// Inserts only if absent while adapting to repeated existing-key phases.
    ///
    /// Two consecutive occupied admissions enable a lock-free exact precheck.
    /// The first absent precheck disables it again before using the ordinary
    /// atomic conditional-insert path. Requiring two observations prevents an
    /// alternating existing/new-key trace from repeatedly mispredicting its
    /// next admission.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn insert_if_absent_with_options(
        &mut self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.guard.cache.validate_weight(key, weight)?;
        if self.precheck_existing(key) {
            return Ok(CacheAdmissionOutcome::Existing);
        }
        let outcome = self
            .guard
            .insert_if_absent_with_options(key, value, weight, ttl)?;
        self.observe(outcome);
        Ok(outcome)
    }

    /// Lazily constructs a value while adapting to existing-key phases.
    ///
    /// `make_value` is not called when the learned exact precheck observes an
    /// existing key. A racing winner after an absent precheck is still handled
    /// by the ordinary atomic conditional insertion.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn insert_if_absent_with_options_by<F>(
        &mut self,
        key: &[u8],
        make_value: F,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError>
    where
        F: FnOnce() -> V,
    {
        self.guard.cache.validate_weight(key, weight)?;
        if self.precheck_existing(key) {
            return Ok(CacheAdmissionOutcome::Existing);
        }
        let outcome = self
            .guard
            .insert_if_absent_with_options(key, make_value(), weight, ttl)?;
        self.observe(outcome);
        Ok(outcome)
    }

    fn precheck_existing(&mut self, key: &[u8]) -> bool {
        if self.existing_streak < DIRECT_ADAPTIVE_EXISTING_THRESHOLD {
            return false;
        }
        if self.guard.lookup_index.get_protected(key).is_some() {
            return true;
        }
        self.existing_streak = 0;
        false
    }

    fn observe(&mut self, outcome: CacheAdmissionOutcome) {
        self.existing_streak = if outcome == CacheAdmissionOutcome::Existing {
            self.existing_streak
                .saturating_add(1)
                .min(DIRECT_ADAPTIVE_EXISTING_THRESHOLD)
        } else {
            0
        };
    }
}

impl<V> DirectCacheAdmissionBatch<'_, '_, V> {
    /// Inserts only while the exact key remains absent.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    ///
    /// # Panics
    ///
    /// Panics only if the admission batch's internal generation reservation
    /// invariant is violated.
    #[allow(
        clippy::too_many_lines,
        unsafe_code,
        reason = "keeps every feature-gated publication branch beside the batch's unpublished ownership transfer"
    )]
    pub fn insert_if_absent_with_options(
        &mut self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let cache = self.guard.cache;
        let compact_weight = cache.validate_weight(key, weight)?;
        let admission_hash = match cache.admission_precheck(key) {
            Ok(hash) => hash,
            Err(outcome) => return Ok(outcome),
        };
        let Some(hard_capacity) = cache.reserve_admission_capacity(key, compact_weight) else {
            return Ok(CacheAdmissionOutcome::Existing);
        };
        let inserted = self
            .reservation
            .allocate(value, compact_weight, cache.deadline(ttl));
        let context = DirectAdmissionContext {
            index: self
                .guard
                .index
                .get()
                .expect("admission batch owns a generation reservation"),
            pending_lengths: self.guard.pending_lengths.as_ref(),
            deferred_limit: Some(&self.protected),
        };
        let published = if let Some(pending) = context.pending_lengths {
            if pending.use_immediate() {
                admission_hash.map_or_else(
                    || context.index.get_or_insert(key, inserted.index_value()),
                    |key_hash| {
                        context
                            .index
                            .get_or_insert_hashed(key, inserted.index_value(), key_hash)
                    },
                )
            } else {
                #[cfg(not(feature = "shared-gx"))]
                let (published, deferred_stripe) = if let Some(key_hash) = admission_hash {
                    context.index.get_or_insert_deferred_len_hashed(
                        key,
                        inserted.index_value(),
                        key_hash,
                    )
                } else if self.route_only_admission {
                    context
                        .index
                        .get_or_insert_deferred_len_route_only(key, inserted.index_value())
                } else {
                    context
                        .index
                        .get_or_insert_deferred_len(key, inserted.index_value())
                };
                #[cfg(feature = "shared-gx")]
                let (published, deferred_stripe) = admission_hash.map_or_else(
                    || {
                        context
                            .index
                            .get_or_insert_deferred_len(key, inserted.index_value())
                    },
                    |key_hash| {
                        context.index.get_or_insert_deferred_len_hashed(
                            key,
                            inserted.index_value(),
                            key_hash,
                        )
                    },
                );
                if let Some(stripe) = deferred_stripe {
                    pending.record(context.index, stripe);
                }
                published
            }
        } else {
            #[cfg(not(feature = "shared-gx"))]
            {
                if let Some(key_hash) = admission_hash {
                    context
                        .index
                        .get_or_insert_hashed(key, inserted.index_value(), key_hash)
                } else if self.route_only_admission {
                    let (published, inserted_stripe) = context
                        .index
                        .get_or_insert_deferred_len_route_only(key, inserted.index_value());
                    if let Some(stripe) = inserted_stripe {
                        context.index.flush_deferred_insert_len(stripe, 1);
                    }
                    published
                } else {
                    context.index.get_or_insert(key, inserted.index_value())
                }
            }
            #[cfg(feature = "shared-gx")]
            admission_hash.map_or_else(
                || context.index.get_or_insert(key, inserted.index_value()),
                |key_hash| {
                    context
                        .index
                        .get_or_insert_hashed(key, inserted.index_value(), key_hash)
                },
            )
        };
        let outcome = if published == inserted.index_value() {
            cache.add_capacity(key, compact_weight);
            cache.enroll_probation(key, inserted);
            hard_capacity.commit_add();
            cache.note_admission_population();
            context
                .deferred_limit
                .expect("admission batch defers its capacity limit")
                .set(Some(inserted));
            CacheAdmissionOutcome::Inserted
        } else {
            // SAFETY: failed publication leaves this cache's candidate unreachable.
            unsafe { cache.arena.drop_unpublished(inserted) };
            CacheAdmissionOutcome::Existing
        };
        if outcome == CacheAdmissionOutcome::Inserted {
            self.guard
                .pending_inserts
                .set(self.guard.pending_inserts.get().wrapping_add(1));
            self.record_admission(weight);
        }
        Ok(outcome)
    }

    fn record_admission(&mut self, weight: u64) {
        self.pending_entries = self.pending_entries.saturating_add(1);
        self.pending_weight = self.pending_weight.saturating_add(weight);
        self.guard.admissions_since_refresh = self.guard.admissions_since_refresh.saturating_add(1);
        if self.pending_entries >= self.entry_limit || self.pending_weight >= self.weight_limit {
            self.enforce_pending();
        }
        if self.guard.admissions_since_refresh >= DIRECT_ADMISSION_RECLAIM_INTERVAL {
            self.guard.refresh_guarded_batch();
        }
    }

    fn enforce_pending(&mut self) {
        if let Some(protected) = self.protected.take() {
            self.guard.cache.enforce_batched_limits(protected);
        }
        self.pending_entries = 0;
        self.pending_weight = 0;
    }
}

impl<V> DirectCacheBulkAdmissionBatch<'_, '_, V> {
    /// Inserts only while the exact key remains absent.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    ///
    /// # Panics
    ///
    /// Panics only if the bulk admission batch's internal generation
    /// reservation invariant is violated.
    #[allow(
        clippy::too_many_lines,
        unsafe_code,
        reason = "keeps every feature-gated publication branch beside the bulk batch's unpublished ownership transfer"
    )]
    pub fn insert_if_absent_with_options(
        &mut self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let Self {
            batch,
            pending_lengths,
            capacity_limited,
        } = self;
        let cache = batch.guard.cache;
        let compact_weight = cache.validate_weight(key, weight)?;
        let admission_hash = match cache.admission_precheck(key) {
            Ok(hash) => hash,
            Err(outcome) => return Ok(outcome),
        };
        let Some(hard_capacity) = cache.reserve_admission_capacity(key, compact_weight) else {
            return Ok(CacheAdmissionOutcome::Existing);
        };
        let inserted = batch
            .reservation
            .allocate(value, compact_weight, cache.deadline(ttl));
        let pending = batch
            .guard
            .pending_lengths
            .as_ref()
            .unwrap_or(pending_lengths);
        let context = DirectAdmissionContext {
            index: batch
                .guard
                .index
                .get()
                .expect("bulk admission batch owns a generation reservation"),
            pending_lengths: Some(pending),
            deferred_limit: Some(&batch.protected),
        };
        let published = if pending.use_immediate() {
            admission_hash.map_or_else(
                || context.index.get_or_insert(key, inserted.index_value()),
                |key_hash| {
                    context
                        .index
                        .get_or_insert_hashed(key, inserted.index_value(), key_hash)
                },
            )
        } else {
            #[cfg(not(feature = "shared-gx"))]
            let (published, deferred_stripe) = if let Some(key_hash) = admission_hash {
                context.index.get_or_insert_deferred_len_hashed(
                    key,
                    inserted.index_value(),
                    key_hash,
                )
            } else if batch.route_only_admission {
                #[cfg(feature = "operation-batch")]
                {
                    if matches!(key.len(), 0..=7 | 9..=15 | 17..=23 | 25..=31) {
                        context.index.get_or_insert_deferred_len_route_only_encoded(
                            key,
                            inserted.index_value(),
                        )
                    } else {
                        context
                            .index
                            .get_or_insert_deferred_len_route_only(key, inserted.index_value())
                    }
                }
                #[cfg(not(feature = "operation-batch"))]
                {
                    context
                        .index
                        .get_or_insert_deferred_len_route_only(key, inserted.index_value())
                }
            } else {
                context
                    .index
                    .get_or_insert_deferred_len(key, inserted.index_value())
            };
            #[cfg(feature = "shared-gx")]
            let (published, deferred_stripe) = admission_hash.map_or_else(
                || {
                    context
                        .index
                        .get_or_insert_deferred_len(key, inserted.index_value())
                },
                |key_hash| {
                    context.index.get_or_insert_deferred_len_hashed(
                        key,
                        inserted.index_value(),
                        key_hash,
                    )
                },
            );
            if let Some(stripe) = deferred_stripe {
                pending.record(context.index, stripe);
            }
            published
        };
        let outcome = if published == inserted.index_value() {
            if *capacity_limited {
                cache.add_capacity(key, compact_weight);
                cache.enroll_probation(key, inserted);
                batch.protected.set(Some(inserted));
            }
            hard_capacity.commit_add();
            cache.note_admission_population();
            CacheAdmissionOutcome::Inserted
        } else {
            // SAFETY: failed publication leaves this cache's candidate unreachable.
            unsafe { cache.arena.drop_unpublished(inserted) };
            CacheAdmissionOutcome::Existing
        };
        if outcome == CacheAdmissionOutcome::Inserted {
            batch
                .guard
                .pending_inserts
                .set(batch.guard.pending_inserts.get().wrapping_add(1));
            if *capacity_limited {
                Self::record_admission(batch, pending_lengths, weight);
            } else {
                Self::record_unbounded_admission(batch, pending_lengths);
            }
        }
        Ok(outcome)
    }

    fn record_admission(
        batch: &mut DirectCacheAdmissionBatch<'_, '_, V>,
        pending_lengths: &DirectPendingLengths,
        weight: u64,
    ) {
        batch.pending_entries = batch.pending_entries.saturating_add(1);
        batch.pending_weight = batch.pending_weight.saturating_add(weight);
        batch.guard.admissions_since_refresh =
            batch.guard.admissions_since_refresh.saturating_add(1);
        if batch.pending_entries >= batch.entry_limit || batch.pending_weight >= batch.weight_limit
        {
            batch.enforce_pending();
        }
        if batch.guard.admissions_since_refresh >= DIRECT_ADMISSION_RECLAIM_INTERVAL {
            if batch.guard.pending_lengths.is_none()
                && let Some(index) = batch.guard.index.get()
            {
                pending_lengths.flush(index);
            }
            batch.guard.refresh_guarded_batch();
        }
    }

    fn record_unbounded_admission(
        batch: &mut DirectCacheAdmissionBatch<'_, '_, V>,
        pending_lengths: &DirectPendingLengths,
    ) {
        batch.guard.admissions_since_refresh =
            batch.guard.admissions_since_refresh.saturating_add(1);
        if batch.guard.admissions_since_refresh >= DIRECT_ADMISSION_RECLAIM_INTERVAL {
            if let Some(index) = batch.guard.index.get() {
                pending_lengths.flush(index);
            }
            batch.guard.refresh_guarded_batch();
        }
    }
}

impl<V, const RECORD_STATS: bool> DirectCacheRemovalBatch<'_, '_, V, RECORD_STATS> {
    /// Removes a value without retaining it.
    ///
    /// The standard batch records removals at refresh or drop. A batch created
    /// by [`DirectCacheGuard::removal_batch_untracked`] omits that statistic.
    pub fn remove_discard(&mut self, key: &[u8]) -> bool {
        if !self.remove_discard_inner(key) {
            return false;
        }
        self.record_removal();
        true
    }

    /// Removes through a prepared exact hot-key handle without retaining the value.
    ///
    /// Invalid or rebuild-stale handles fall back to the ordinary exact batch
    /// route. Successful removal has the same deferred retirement, length, and
    /// optional-statistics behavior as [`Self::remove_discard`].
    #[cfg(feature = "prepared-keys")]
    pub fn remove_discard_prepared(&mut self, key: &[u8], prepared: &AtomicPreparedKey) -> bool {
        let mutation = self
            .mutation
            .get_or_insert_with(|| self.guard.cache.arena.mutation_pin());
        let index = self
            .guard
            .index
            .get_or_init(|| self.guard.cache.index.read_guard());
        let removed = self.guard.cache.remove_discard_prepared_guarded_deferred(
            mutation,
            index,
            key,
            prepared,
            &mut self.pending_retirements,
            &self.pending_lengths,
        );
        self.guard.remove_precheck.set(!removed);
        if !removed {
            if self.pending_retirements.is_empty() {
                self.mutation = None;
            }
            return false;
        }
        self.publish_full_retirement_batch();
        self.record_removal();
        true
    }

    fn remove_discard_inner(&mut self, key: &[u8]) -> bool {
        if self.guard.remove_precheck.get() && self.guard.lookup_index.get_protected(key).is_none()
        {
            return false;
        }
        let mutation = self
            .mutation
            .get_or_insert_with(|| self.guard.cache.arena.mutation_pin());
        let index = self
            .guard
            .index
            .get_or_init(|| self.guard.cache.index.read_guard());
        let removed = self.guard.cache.remove_discard_guarded_deferred(
            mutation,
            index,
            key,
            &mut self.pending_retirements,
            &self.pending_lengths,
        );
        self.guard.remove_precheck.set(!removed);
        if removed {
            self.publish_full_retirement_batch();
        } else if self.pending_retirements.is_empty() {
            self.mutation = None;
        }
        removed
    }

    #[allow(
        unsafe_code,
        reason = "publishes one full batch of exact removals through its mutation pin"
    )]
    fn publish_full_retirement_batch(&mut self) {
        if self.pending_retirements.len() != crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH {
            return;
        }
        let mutation = self
            .mutation
            .as_ref()
            .unwrap_or_else(|| std::process::abort());
        // SAFETY: the mutation pin was acquired before every exact removal in
        // this group and remains active through its queue publication.
        unsafe { mutation.retire_batch(&mut self.pending_retirements) };
        self.mutation = None;
    }

    fn record_removal(&mut self) {
        self.removals_since_refresh = self.removals_since_refresh.saturating_add(1);
        if self.removals_since_refresh >= DIRECT_ADMISSION_RECLAIM_INTERVAL {
            self.flush();
            self.guard.refresh();
            self.removals_since_refresh = 0;
        }
    }

    #[allow(
        unsafe_code,
        reason = "the batch contains only exact removals from this cache index"
    )]
    fn flush(&mut self) {
        if let Some(index) = self.guard.index.get() {
            self.pending_lengths.flush_removals(index);
        }
        if !self.pending_retirements.is_empty() {
            let mutation = self
                .mutation
                .as_ref()
                .unwrap_or_else(|| std::process::abort());
            // SAFETY: every exact removal occurred while this mutation pin was
            // active, and it remains active through queue publication.
            unsafe { mutation.retire_batch(&mut self.pending_retirements) };
        }
        self.mutation = None;
        if RECORD_STATS && self.removals_since_refresh != 0 {
            self.guard.cache.counters[self.guard.counter_shard]
                .removals
                .fetch_add(self.removals_since_refresh as u64, Ordering::Relaxed);
        }
    }
}

impl<V, const RECORD_STATS: bool> DirectCacheReplacementBatch<'_, '_, V, RECORD_STATS> {
    /// Replaces a live value without inserting an absent key.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn replace_discard_with_options(
        &mut self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let cache = self.guard.cache;
        let arena = self.guard.arena();
        let (replaced, replacement) = cache.replace_discard_pinned_deferred(
            arena,
            key,
            value,
            weight,
            ttl,
            &mut self.pending_retirements,
        )?;
        if let Some(replacement) = replacement {
            cache.enforce_limits(replacement);
        }
        if replaced {
            self.record_replacement();
        }
        Ok(replaced)
    }

    /// Replaces a live value through an exact prepared hot-key handle.
    ///
    /// Wrong or stale handles fall back to the ordinary exact route. An absent
    /// key remains absent without allocating a replacement.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[cfg(feature = "prepared-keys")]
    pub fn replace_discard_prepared_with_options(
        &mut self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let cache = self.guard.cache;
        let arena = self.guard.arena();
        let (replaced, replacement) = cache.replace_discard_prepared_pinned_deferred(
            arena,
            key,
            prepared,
            value,
            weight,
            ttl,
            &mut self.pending_retirements,
        )?;
        if let Some(replacement) = replacement {
            cache.enforce_limits(replacement);
        }
        if replaced {
            self.record_replacement();
        }
        Ok(replaced)
    }
}

#[cfg(feature = "prepared-keys")]
impl<V> DirectCachePreparedReplacementBatch<'_, '_, V> {
    /// Reads and records a cache hit or miss while retaining this pipeline.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.batch.guard.get(key)
    }

    /// Reads through an exact prepared handle while retaining this pipeline.
    #[must_use]
    pub fn get_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> Option<&V> {
        self.batch.guard.get_prepared(key, prepared)
    }

    /// Inserts only if absent while retaining this pipeline.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn insert_if_absent_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.batch
            .guard
            .insert_if_absent_with_options(key, value, weight, ttl)
    }

    /// Removes a value without retaining it while retaining this pipeline.
    #[must_use]
    pub fn remove_discard(&self, key: &[u8]) -> bool {
        self.batch.guard.remove_discard(key)
    }

    /// Removes through an exact prepared handle while retaining this pipeline.
    #[must_use]
    pub fn remove_discard_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> bool {
        self.batch.guard.remove_discard_prepared(key, prepared)
    }

    /// Replaces one existing key through the ordinary exact route.
    ///
    /// This is useful for cold-key fallbacks inside a mixed prepared pipeline.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn replace_discard_with_options(
        &mut self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        self.batch
            .replace_discard_with_options(key, value, weight, ttl)
    }

    /// Replaces one existing key through an exact prepared handle.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn replace_discard_prepared_with_options(
        &mut self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        self.batch
            .replace_discard_prepared_with_options(key, prepared, value, weight, ttl)
    }

    /// Replaces up to 64 existing keys through the ordinary exact route.
    ///
    /// This is the cold-key companion to the prepared batch operation. It
    /// reserves recyclable value storage once, then preserves ordinary exact
    /// update semantics for every key. Absent keys remain absent and
    /// `replaced` receives one result per input key.
    ///
    /// # Errors
    ///
    /// Rejects a replacement whose charge exceeds the configured or compact
    /// limit. No index changes occur unless every charge validates.
    ///
    /// # Panics
    ///
    /// Panics unless the key, replacement, and result lengths match, or when
    /// more than 64 items are supplied.
    #[inline(never)]
    #[allow(
        clippy::too_many_lines,
        unsafe_code,
        reason = "the batch pairs this cache's candidates, exact removals, and arena pin"
    )]
    pub fn replace_discard_batch_with_options<K, I>(
        &mut self,
        keys: &[K],
        replacements: I,
        replaced: &mut [bool],
    ) -> Result<(), CacheInsertError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (V, u64, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
    {
        let replacements = replacements.into_iter();
        let count = replacements.len();
        assert_eq!(keys.len(), count, "cache replacement batch length mismatch");
        assert_eq!(
            keys.len(),
            replaced.len(),
            "replacement result batch length mismatch"
        );
        assert!(
            count <= crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            "cache replacement batches contain at most 64 items"
        );

        let cache = self.batch.guard.cache;
        self.prepared_values.clear();
        self.prepared_previous.clear();
        self.prepared_values.reserve(count);
        self.prepared_previous.resize(count, None);
        let mut reservation = cache.arena.recycled_boxed_reservation(count);
        let mut hard_capacity = cache
            .hard_capacity
            .as_ref()
            .map(|_| Vec::with_capacity(count));

        for (key, (value, weight, ttl)) in keys.iter().zip(replacements) {
            let compact_weight = match cache.validate_weight(key.as_ref(), weight) {
                Ok(weight) => weight,
                Err(error) => {
                    for raw in self.prepared_values.drain(..) {
                        // SAFETY: validation failed before these candidates were published.
                        unsafe {
                            cache
                                .arena
                                .drop_unpublished(published_direct_handle_from_index(raw));
                        }
                    }
                    self.prepared_previous.clear();
                    return Err(error);
                }
            };
            if let Some(reservations) = &mut hard_capacity {
                reservations.push(
                    cache
                        .try_reserve_publish_capacity(compact_weight)
                        .map(DirectReplacementCapacityReservation::Full),
                );
            }
            let candidate = reservation.allocate(value, compact_weight, cache.deadline(ttl));
            self.prepared_values.push(candidate.index_value());
        }

        let arena = self.batch.guard.arena();
        for (index, key) in keys.iter().enumerate() {
            let candidate = self.prepared_values[index];
            if hard_capacity
                .as_ref()
                .is_some_and(|reservations| reservations[index].is_none())
            {
                let candidate = published_direct_handle_from_index(candidate);
                // SAFETY: this batch candidate belongs to this cache and pin.
                let replacement_weight =
                    unsafe { protected_direct_entry(arena, candidate) }.weight();
                if let Some((previous, _previous_weight, reservation)) = cache
                    .publish_replacement_at_hard_capacity(
                        arena,
                        key.as_ref(),
                        candidate,
                        replacement_weight,
                    )
                {
                    self.prepared_previous[index] = Some(previous.index_value());
                    hard_capacity
                        .as_mut()
                        .expect("hard-capacity slow path has reservation storage")[index] =
                        Some(DirectReplacementCapacityReservation::Delta(reservation));
                }
                continue;
            }
            let displaced = Cell::new(None);
            let updated = cache.index.update(key.as_ref(), |current| {
                displaced.set(Some(*current));
                candidate
            });
            self.prepared_previous[index] = updated.and(displaced.get());
            if self.prepared_previous[index].is_none()
                && let Some(reservations) = &mut hard_capacity
            {
                drop(reservations[index].take());
            }
        }

        let mut successes = 0;
        let mut protected = None;
        for index in 0..count {
            let candidate = published_direct_handle_from_index(self.prepared_values[index]);
            let Some(previous) = self.prepared_previous[index] else {
                if let Some(reservations) = &mut hard_capacity {
                    drop(reservations[index].take());
                }
                // SAFETY: the failed exact update left this candidate unpublished.
                unsafe { cache.arena.drop_unpublished(candidate) };
                replaced[index] = false;
                continue;
            };
            let previous = published_direct_handle_from_index(previous);
            // SAFETY: both handles and the pin belong to this cache; the old
            // handle remains epoch-protected and the candidate is still live.
            let previous_weight = unsafe { protected_direct_entry(arena, previous) }.weight();
            // SAFETY: the published candidate belongs to this cache and pin.
            let replacement_weight = unsafe { protected_direct_entry(arena, candidate) }.weight();
            cache.replace_capacity(keys[index].as_ref(), previous_weight, replacement_weight);
            if let Some(reservations) = &mut hard_capacity {
                reservations[index]
                    .take()
                    .expect("each replacement owns one hard-capacity reservation")
                    .commit(previous_weight, replacement_weight);
            }
            // SAFETY: the successful exact replacement displaced this handle once.
            self.batch
                .pending_retirements
                .push(unsafe { removed_after_exact_index_transfer(previous) });
            if self.batch.pending_retirements.len()
                == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH
            {
                // SAFETY: every token came from an exact replacement in this cache.
                unsafe {
                    arena.retire_batch(&mut self.batch.pending_retirements);
                }
            }
            protected = Some(candidate);
            replaced[index] = true;
            successes += 1;
        }
        if let Some(protected) = protected {
            cache.enforce_batched_limits(protected);
        }
        self.prepared_values.clear();
        self.prepared_previous.clear();
        self.batch.record_replacements(successes);
        Ok(())
    }

    /// Replaces up to 64 existing prepared keys through one generation snapshot.
    ///
    /// Each replacement item is `(value, weight, ttl)`. Absent keys remain
    /// absent, and their unpublished values are reclaimed before this method
    /// returns. Wrong, stale, cross-cache, or overlay handles use the ordinary
    /// exact update path. `replaced` receives one result for every input key.
    /// Capacity accounting and reclamation are exact when the call returns.
    ///
    /// # Errors
    ///
    /// Rejects a replacement whose charge exceeds the configured or compact
    /// limit. No index changes occur unless every charge validates.
    ///
    /// # Panics
    ///
    /// Panics unless the key, prepared-handle, replacement, and result lengths
    /// match, or when more than 64 items are supplied.
    #[allow(
        clippy::too_many_lines,
        unsafe_code,
        reason = "the prepared batch pairs this cache's candidates, exact removals, and arena pin"
    )]
    pub fn replace_discard_prepared_batch_with_options<K, I>(
        &mut self,
        keys: &[K],
        prepared: &[AtomicPreparedKey],
        replacements: I,
        replaced: &mut [bool],
    ) -> Result<(), CacheInsertError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (V, u64, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
    {
        let replacements = replacements.into_iter();
        let count = replacements.len();
        assert_eq!(
            keys.len(),
            prepared.len(),
            "prepared key batch length mismatch"
        );
        assert_eq!(keys.len(), count, "cache replacement batch length mismatch");
        assert_eq!(
            keys.len(),
            replaced.len(),
            "replacement result batch length mismatch"
        );
        assert!(
            count <= crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH,
            "cache replacement batches contain at most 64 items"
        );

        let cache = self.batch.guard.cache;
        self.prepared_values.clear();
        self.prepared_previous.clear();
        self.prepared_values.reserve(count);
        self.prepared_previous.resize(count, None);
        let mut reservation = cache.arena.recycled_boxed_reservation(count);
        let mut hard_capacity = cache
            .hard_capacity
            .as_ref()
            .map(|_| Vec::with_capacity(count));

        for (key, (value, weight, ttl)) in keys.iter().zip(replacements) {
            let compact_weight = match cache.validate_weight(key.as_ref(), weight) {
                Ok(weight) => weight,
                Err(error) => {
                    for raw in self.prepared_values.drain(..) {
                        // SAFETY: validation failed before these candidates were published.
                        unsafe {
                            cache
                                .arena
                                .drop_unpublished(published_direct_handle_from_index(raw));
                        }
                    }
                    self.prepared_previous.clear();
                    return Err(error);
                }
            };
            if let Some(reservations) = &mut hard_capacity {
                reservations.push(
                    cache
                        .try_reserve_publish_capacity(compact_weight)
                        .map(DirectReplacementCapacityReservation::Full),
                );
            }
            let candidate = reservation.allocate(value, compact_weight, cache.deadline(ttl));
            self.prepared_values.push(candidate.index_value());
        }

        let arena = self.batch.guard.arena();
        if hard_capacity
            .as_ref()
            .is_none_or(|reservations| reservations.iter().all(Option::is_some))
        {
            cache.index.replace_prepared_batch(
                keys,
                prepared,
                &self.prepared_values,
                &mut self.prepared_previous,
            );
        } else {
            for (index, (key, prepared)) in keys.iter().zip(prepared).enumerate() {
                let candidate = self.prepared_values[index];
                if hard_capacity
                    .as_ref()
                    .is_some_and(|reservations| reservations[index].is_none())
                {
                    let candidate = published_direct_handle_from_index(candidate);
                    // SAFETY: this batch candidate belongs to this cache and pin.
                    let replacement_weight =
                        unsafe { protected_direct_entry(arena, candidate) }.weight();
                    if let Some((previous, _previous_weight, reservation)) = cache
                        .publish_replacement_at_hard_capacity(
                            arena,
                            key.as_ref(),
                            candidate,
                            replacement_weight,
                        )
                    {
                        self.prepared_previous[index] = Some(previous.index_value());
                        hard_capacity
                            .as_mut()
                            .expect("hard-capacity slow path has reservation storage")[index] =
                            Some(DirectReplacementCapacityReservation::Delta(reservation));
                    }
                    continue;
                }
                let displaced = Cell::new(None);
                let updated = cache
                    .index
                    .update_prepared(key.as_ref(), prepared, |current| {
                        displaced.set(Some(*current));
                        candidate
                    });
                self.prepared_previous[index] = updated.and(displaced.get());
                if self.prepared_previous[index].is_none()
                    && let Some(reservations) = &mut hard_capacity
                {
                    drop(reservations[index].take());
                }
            }
        }

        let mut successes = 0;
        let mut protected = None;
        for index in 0..count {
            let candidate = published_direct_handle_from_index(self.prepared_values[index]);
            let Some(previous) = self.prepared_previous[index] else {
                if let Some(reservations) = &mut hard_capacity {
                    drop(reservations[index].take());
                }
                // SAFETY: the failed prepared update left this candidate unpublished.
                unsafe { cache.arena.drop_unpublished(candidate) };
                replaced[index] = false;
                continue;
            };
            let previous = published_direct_handle_from_index(previous);
            // SAFETY: both handles and the pin belong to this cache; the old
            // handle remains epoch-protected and the candidate is still live.
            let previous_weight = unsafe { protected_direct_entry(arena, previous) }.weight();
            // SAFETY: the published candidate belongs to this cache and pin.
            let replacement_weight = unsafe { protected_direct_entry(arena, candidate) }.weight();
            cache.replace_capacity(keys[index].as_ref(), previous_weight, replacement_weight);
            if let Some(reservations) = &mut hard_capacity {
                reservations[index]
                    .take()
                    .expect("each replacement owns one hard-capacity reservation")
                    .commit(previous_weight, replacement_weight);
            }
            // SAFETY: the successful prepared replacement displaced this handle once.
            self.batch
                .pending_retirements
                .push(unsafe { removed_after_exact_index_transfer(previous) });
            if self.batch.pending_retirements.len()
                == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH
            {
                // SAFETY: every token came from an exact replacement in this cache.
                unsafe {
                    arena.retire_batch(&mut self.batch.pending_retirements);
                }
            }
            protected = Some(candidate);
            replaced[index] = true;
            successes += 1;
        }
        if let Some(protected) = protected {
            cache.enforce_batched_limits(protected);
        }
        self.prepared_values.clear();
        self.prepared_previous.clear();
        self.batch.record_replacements(successes);
        Ok(())
    }
}

impl<V, const RECORD_STATS: bool> DirectCacheReplacementBatch<'_, '_, V, RECORD_STATS> {
    fn record_replacement(&mut self) {
        self.record_replacements(1);
    }

    fn record_replacements(&mut self, replacements: usize) {
        self.replacements_since_refresh =
            self.replacements_since_refresh.saturating_add(replacements);
        if self.replacements_since_refresh >= DIRECT_ADMISSION_RECLAIM_INTERVAL {
            self.flush();
            self.guard.refresh();
            self.replacements_since_refresh = 0;
        }
    }

    #[allow(
        unsafe_code,
        reason = "the batch contains only exact replacements from this cache index"
    )]
    fn flush(&mut self) {
        // SAFETY: every token came from an exact replacement in this cache batch.
        unsafe {
            self.guard
                .arena()
                .retire_batch(&mut self.pending_retirements);
        }
        if RECORD_STATS && self.replacements_since_refresh != 0 {
            self.guard.cache.counters[self.guard.counter_shard]
                .replacements
                .fetch_add(self.replacements_since_refresh as u64, Ordering::Relaxed);
        }
    }
}

impl<V> Drop for DirectCacheAdmissionBatch<'_, '_, V> {
    fn drop(&mut self) {
        self.enforce_pending();
    }
}

impl<V> Drop for DirectCacheBulkAdmissionBatch<'_, '_, V> {
    fn drop(&mut self) {
        if let Some(index) = self.batch.guard.index.get() {
            self.pending_lengths.flush(index);
        }
    }
}

impl<V, const RECORD_STATS: bool> Drop for DirectCacheRemovalBatch<'_, '_, V, RECORD_STATS> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<V, const RECORD_STATS: bool> Drop for DirectCacheReplacementBatch<'_, '_, V, RECORD_STATS> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<V> Drop for DirectCacheGuard<'_, V> {
    fn drop(&mut self) {
        self.flush_lengths();
        self.flush_stats();
    }
}

impl<V> DirectPackedCache<V> {
    /// Builds an empty direct-pointer cache.
    ///
    /// # Errors
    ///
    /// Returns an invalid configuration or adaptive-index construction error.
    pub fn try_new(config: CacheConfig) -> Result<Self, CacheBuildError> {
        validate_direct_config(&config).map_err(CacheBuildError::Config)?;
        let hash_builder = GenerationHashBuilder::default();
        let index =
            LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash_and_index(
            std::iter::empty::<(Box<[u8]>, NonMaxU64)>(),
            config.direct_overlay_capacity(),
            AtomicGenerationOverlay::AtomicAdaptive,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
            hash_builder,
            FrozenIndexBackend::PtrHash,
        )?;
        let admission_doorkeeper = config.admission_doorkeeper_entries.map(|entries| {
            DirectAdmissionDoorkeeper::new(entries, config.frequency_sketch_max_gate())
        });
        let frequency_reuse_shards = config.frequency_admission_min_hit_rate_bps.map(|_| {
            std::iter::repeat_with(DirectFrequencyReuseShard::default)
                .take(COUNTER_SHARDS)
                .collect()
        });
        let hard_capacity = DirectHardCapacity::new(&config, 0, 0);
        let probation = DirectProbation::for_config(&config);
        Ok(Self {
            index,
            arena: DirectValueArena::new(config.arena_partitions),
            config,
            started: Instant::now(),
            maintenance_gate: Mutex::new(()),
            full_maintenance_gate: Mutex::new(()),
            counters: std::iter::repeat_with(CacheCounters::default)
                .take(COUNTER_SHARDS)
                .collect(),
            capacity: std::iter::repeat_with(DirectCapacityCounters::default)
                .take(COUNTER_SHARDS)
                .collect(),
            hard_capacity,
            entry_pressure: AtomicU64::new(0),
            weight_pressure: AtomicU64::new(0),
            maintenance_pending: AtomicBool::new(false),
            maintenance_worker: Arc::new(ArcSwapOption::empty()),
            victim_scan_cursor: AtomicU64::new(0),
            victim_reservoir: Mutex::new(Vec::new()),
            probation,
            admission_doorkeeper,
            frequency_reuse: AtomicU64::new(0),
            frequency_reuse_shards,
            frequency_reuse_publishing: AtomicBool::new(false),
            expiration_possible: AtomicBool::new(false),
            mutation_stripes: std::iter::repeat_with(|| Mutex::new(()))
                .take(MUTATION_STRIPES)
                .collect(),
            maintenance_completions: AtomicU64::new(0),
            #[cfg(feature = "cache-production-diagnostics")]
            diagnostics: Box::new(DirectCacheDiagnosticCounters::new(0, 0)),
        })
    }

    /// Bulk-loads unique binary keys directly into the compact frozen layer.
    ///
    /// Each tuple contains `(key, value, accounted_weight, ttl)`. This path is
    /// intended for snapshot restore and warm cache construction: unlike
    /// repeated inserts followed by [`Self::maintain`], it never retains a
    /// full-size writable layer alongside the completed frozen generation.
    /// Initial entries do not increment operation counters. The iterator must
    /// report an exact length so the constructor can retain only one compact
    /// cleanup handle per unpublished value while it builds the index.
    ///
    /// # Errors
    ///
    /// Returns an invalid configuration, an unrepresentable item charge, an
    /// initial capacity violation, or a frozen-index construction error. Keys
    /// must be unique. All values allocated before an error are reclaimed.
    #[allow(
        clippy::too_many_lines,
        reason = "keeps staged cleanup and every fallible bulk-load phase in one auditable transaction"
    )]
    pub fn try_from_entries_with_options<I, K>(
        config: CacheConfig,
        entries: I,
    ) -> Result<Self, CacheBuildError>
    where
        I: IntoIterator<Item = (K, V, u64, Option<Duration>)>,
        I::IntoIter: ExactSizeIterator,
        K: AsRef<[u8]>,
    {
        validate_direct_config(&config).map_err(CacheBuildError::Config)?;

        let started = Instant::now();
        let arena = DirectValueArena::new(config.arena_partitions);
        let admission_doorkeeper = config.admission_doorkeeper_entries.map(|entries| {
            DirectAdmissionDoorkeeper::new(entries, config.frequency_sketch_max_gate())
        });
        let iterator = entries.into_iter();
        let entry_count = iterator.len();
        if let Some(maximum) = config.max_entries
            && entry_count > maximum
        {
            return Err(CacheBuildError::InitialEntries {
                entries: entry_count,
                maximum,
            });
        }
        let mut staged = UnpublishedDirectEntries {
            arena: &arena,
            handles: Vec::new(),
            published: false,
        };
        staged
            .handles
            .try_reserve_exact(entry_count)
            .map_err(|_| CacheBuildError::Index(FrozenBuildError::AllocationFailed))?;

        let mut entry_shards = [0_u64; COUNTER_SHARDS];
        let mut weight_shards = [0_u64; COUNTER_SHARDS];
        let mut total_weight = 0_u64;
        let mut expiration_possible = false;
        let mut initial_error = None;
        let hash_builder = GenerationHashBuilder::default();
        let admission_hash_builder = hash_builder.clone();
        let indexed_entries = iterator.map(|(key, value, weight, ttl)| {
            let compact_weight = compact_initial_weight(&config, weight, &mut initial_error);
            total_weight =
                checked_initial_weight(&config, total_weight, weight, &mut initial_error);
            let key_bytes = key.as_ref();
            if let Some(doorkeeper) = &admission_doorkeeper {
                let key_hash = GenerationKeyHash::new(&admission_hash_builder, key_bytes);
                doorkeeper.observe_hashed(doorkeeper_hashes_from_key_hash(key_hash), false);
            }
            let shard = key_counter_hash(key_bytes) & (COUNTER_SHARDS - 1);
            expiration_possible |= ttl.is_some();
            let handle = arena.allocate(value, compact_weight, Self::deadline_at(ttl, 0));
            staged.handles.push(handle);
            entry_shards[shard] += 1;
            weight_shards[shard] = weight_shards[shard].saturating_add(weight);
            (key, handle.index_value())
        });

        let index_result =
            LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash_and_index(
                indexed_entries,
                config.direct_overlay_capacity(),
                AtomicGenerationOverlay::AtomicAdaptive,
                AtomicGenerationBaseFilter::EmbeddedFingerprint,
                hash_builder,
                FrozenIndexBackend::PtrHash,
            );
        if let Some(error) = initial_error {
            drop(index_result);
            return Err(error);
        }
        let index = index_result?;

        let counters = std::iter::repeat_with(CacheCounters::default)
            .take(COUNTER_SHARDS)
            .collect();
        let (capacity, entry_pressure, weight_pressure) =
            initialized_direct_capacity(&config, &entry_shards, &weight_shards);
        let maintenance_worker = Arc::new(ArcSwapOption::empty());
        let frequency_reuse_shards = config.frequency_admission_min_hit_rate_bps.map(|_| {
            std::iter::repeat_with(DirectFrequencyReuseShard::default)
                .take(COUNTER_SHARDS)
                .collect()
        });
        let mutation_stripes = std::iter::repeat_with(|| Mutex::new(()))
            .take(MUTATION_STRIPES)
            .collect();
        staged.published = true;
        drop(staged);
        if let Some(doorkeeper) = &admission_doorkeeper {
            doorkeeper.note_population(entry_count);
        }
        let hard_capacity = DirectHardCapacity::new(&config, entry_count, total_weight);
        let probation = DirectProbation::for_config(&config);
        Ok(Self {
            index,
            arena,
            config,
            started,
            maintenance_gate: Mutex::new(()),
            full_maintenance_gate: Mutex::new(()),
            counters,
            capacity,
            hard_capacity,
            entry_pressure: AtomicU64::new(entry_pressure),
            weight_pressure: AtomicU64::new(weight_pressure),
            maintenance_pending: AtomicBool::new(false),
            maintenance_worker,
            victim_scan_cursor: AtomicU64::new(0),
            victim_reservoir: Mutex::new(Vec::new()),
            probation,
            admission_doorkeeper,
            frequency_reuse: AtomicU64::new(0),
            frequency_reuse_shards,
            frequency_reuse_publishing: AtomicBool::new(false),
            expiration_possible: AtomicBool::new(expiration_possible),
            mutation_stripes,
            maintenance_completions: AtomicU64::new(0),
            #[cfg(feature = "cache-production-diagnostics")]
            diagnostics: Box::new(DirectCacheDiagnosticCounters::new(
                entry_count,
                total_weight,
            )),
        })
    }

    /// Pins a reusable direct-cache read guard.
    #[must_use]
    pub fn pin(&self) -> DirectCacheGuard<'_, V> {
        let counter_shard = DirectValueArena::<V>::thread_id() & (COUNTER_SHARDS - 1);
        DirectCacheGuard {
            cache: self,
            arena: OnceCell::new(),
            lookup_index: self.index.read_cache(),
            index: OnceCell::new(),
            accesses: Cell::new(0),
            pending_hits: Cell::new(0),
            pending_misses: Cell::new(0),
            pending_inserts: Cell::new(0),
            remove_precheck: Cell::new(true),
            admissions_since_refresh: 0,
            pending_lengths: self.config.max_entries.map(|_| DirectPendingLengths::new()),
            counter_shard,
        }
    }

    /// Starts a reusable cloned-read session without retaining an arena epoch.
    #[must_use]
    pub fn pin_cloned(&self) -> DirectClonedCacheGuard<'_, V> {
        let counter_shard = DirectValueArena::<V>::thread_id() & (COUNTER_SHARDS - 1);
        DirectClonedCacheGuard {
            cache: self,
            lookup_index: self.index.read_cache(),
            accesses: Cell::new(0),
            pending_hits: Cell::new(0),
            pending_misses: Cell::new(0),
            counter_shard,
        }
    }

    /// Starts a cloned/shareable session with bounded epoch retention.
    ///
    /// # Panics
    ///
    /// Panics when `refresh_interval` is zero.
    #[must_use]
    pub fn pin_shareable(&self, refresh_interval: usize) -> DirectShareableCacheGuard<'_, V> {
        assert!(
            refresh_interval != 0,
            "shareable refresh interval is non-zero"
        );
        DirectShareableCacheGuard {
            guard: self.pin(),
            refresh_interval,
            lookups: 0,
        }
    }

    /// Prepares an exact handle for a repeatedly accessed cache key.
    ///
    /// Preparation is optional and never changes ordinary cache semantics.
    /// The handle remains safe after mutation or rebuild; when its dense slot
    /// is no longer usable, prepared reads transparently fall back to the
    /// ordinary adaptive lookup.
    #[cfg(feature = "prepared-keys")]
    #[must_use]
    pub fn prepare_key(&self, key: &[u8]) -> AtomicPreparedKey {
        self.index.prepare_key(key)
    }

    /// Prepares or refreshes a caller-owned batch of exact hot-key handles.
    ///
    /// The slices must have equal lengths. Batch preparation shares one
    /// generation snapshot and allocates no temporary storage.
    ///
    /// # Panics
    ///
    /// Panics when `keys` and `prepared` have different lengths.
    #[cfg(feature = "prepared-keys")]
    pub fn prepare_key_batch<K>(&self, keys: &[K], prepared: &mut [AtomicPreparedKey])
    where
        K: AsRef<[u8]>,
    {
        self.index.prepare_key_batch(keys, prepared);
    }

    /// Reads and records access to a live value.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<DirectCacheValue<'_, V>> {
        let value = self.lookup(key, true);
        let counters = &self.counters[direct_arena_thread() & (COUNTER_SHARDS - 1)];
        if value.is_some() {
            counters.hits.fetch_add(1, Ordering::Relaxed);
            self.record_frequency_reuse(1, 0);
        } else {
            counters.misses.fetch_add(1, Ordering::Relaxed);
            self.record_frequency_reuse(0, 1);
        }
        value
    }

    /// Reads by cloning the resident value under a short reclamation pin.
    ///
    /// This is the preferred shareable-read path for values such as `Arc<T>`:
    /// it returns an ordinary owned value without keeping a cache epoch active.
    /// For deeply owned values, clone cost remains the caller's tradeoff.
    #[must_use]
    pub fn get_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let value = self.lookup_cloned(key, true);
        let counters = &self.counters[direct_arena_thread() & (COUNTER_SHARDS - 1)];
        if value.is_some() {
            counters.hits.fetch_add(1, Ordering::Relaxed);
            self.record_frequency_reuse(1, 0);
        } else {
            counters.misses.fetch_add(1, Ordering::Relaxed);
            self.record_frequency_reuse(0, 1);
        }
        value
    }

    /// Reads without changing access or operation counters.
    #[must_use]
    pub fn peek(&self, key: &[u8]) -> Option<DirectCacheValue<'_, V>> {
        self.lookup(key, false)
    }

    /// Clones a live value without recording access or operation counters.
    #[must_use]
    pub fn peek_cloned(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.lookup_cloned(key, false)
    }

    /// Inserts or replaces a value while retiring any previous value.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[allow(
        unsafe_code,
        reason = "the mutation stripe pairs exact index results with this cache arena pin"
    )]
    pub fn insert_discard_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheWriteOutcome, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            return Ok(self.insert_discard_at_hard_capacity(key, value, compact_weight, ttl));
        };
        let _reclaim_deferral = self.arena.defer_reclamation();
        let mutation = self.mutation_stripe(key).lock();
        let pin = self.arena.pin();
        // The entry handle carries the writer route and exact vacancy proof
        // into the new-key insertion, avoiding the old read-then-rehash path.
        // Occupied keys still use the allocator that wins replacement churn.
        let (replacement, outcome) = match self.index.entry(key) {
            AtomicEntry::Occupied(_) => {
                let replacement =
                    self.arena
                        .allocate_boxed(value, compact_weight, self.deadline(ttl));
                let outcome = self.index.insert(key, replacement.index_value());
                (replacement, outcome)
            }
            AtomicEntry::Vacant(vacant) => {
                let replacement = self
                    .arena
                    .allocate(value, compact_weight, self.deadline(ttl));
                let outcome = vacant.insert(replacement.index_value());
                (replacement, outcome)
            }
        };
        let result = match outcome {
            InsertOutcome::Inserted => {
                self.add_capacity(key, compact_weight);
                self.enroll_probation(key, replacement);
                hard_capacity.commit_add();
                self.counters_for(key)
                    .inserts
                    .fetch_add(1, Ordering::Relaxed);
                CacheWriteOutcome::Inserted
            }
            InsertOutcome::Replaced(previous) => {
                // A concurrent lock-free writer may have replaced the value.
                let previous = published_direct_handle_from_index(previous);
                // SAFETY: the displaced index handle and pin belong to this cache.
                let previous_weight = unsafe { protected_direct_entry(&pin, previous) }.weight();
                self.replace_capacity(key, previous_weight, u64::from(compact_weight));
                hard_capacity.commit_replace(previous_weight);
                self.counters_for(key)
                    .replacements
                    .fetch_add(1, Ordering::Relaxed);
                // SAFETY: the exact replacement transferred this cache handle once.
                unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
                CacheWriteOutcome::Replaced
            }
        };
        drop(pin);
        drop(mutation);
        self.enforce_limits(replacement);
        Ok(result)
    }

    #[allow(
        unsafe_code,
        reason = "the slow hard-cap path conditionally publishes and retires exact cache handles"
    )]
    fn insert_discard_at_hard_capacity(
        &self,
        key: &[u8],
        value: V,
        compact_weight: u32,
        ttl: Option<Duration>,
    ) -> CacheWriteOutcome {
        let pin = self.arena.pin();
        let (result, replacement) =
            self.insert_discard_pinned_at_hard_capacity(&pin, key, value, compact_weight, ttl);
        drop(pin);
        self.enforce_limits(replacement);
        result
    }

    #[allow(
        unsafe_code,
        reason = "the slow hard-cap path conditionally publishes and retires exact cache handles"
    )]
    fn insert_discard_pinned_at_hard_capacity(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        value: V,
        compact_weight: u32,
        ttl: Option<Duration>,
    ) -> (CacheWriteOutcome, DirectHandle) {
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let result = loop {
            if self.index.get_protected(key).is_some() {
                let Some((previous, previous_weight, hard_capacity)) = self
                    .publish_replacement_at_hard_capacity(
                        pin,
                        key,
                        replacement,
                        u64::from(compact_weight),
                    )
                else {
                    continue;
                };
                self.replace_capacity(key, previous_weight, u64::from(compact_weight));
                hard_capacity.commit_delta_replace(previous_weight, u64::from(compact_weight));
                self.counters_for(key)
                    .replacements
                    .fetch_add(1, Ordering::Relaxed);
                // SAFETY: the exact conditional replacement transferred this handle once.
                unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
                break CacheWriteOutcome::Replaced;
            }

            let hard_capacity = self.reserve_publish_capacity(compact_weight);
            match self.index.insert(key, replacement.index_value()) {
                InsertOutcome::Inserted => {
                    self.add_capacity(key, compact_weight);
                    self.enroll_probation(key, replacement);
                    hard_capacity.commit_add();
                    self.counters_for(key)
                        .inserts
                        .fetch_add(1, Ordering::Relaxed);
                    break CacheWriteOutcome::Inserted;
                }
                InsertOutcome::Replaced(previous) => {
                    let previous = published_direct_handle_from_index(previous);
                    // SAFETY: the displaced index handle and pin belong to this cache.
                    let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
                    self.replace_capacity(key, previous_weight, u64::from(compact_weight));
                    hard_capacity.commit_replace(previous_weight);
                    self.counters_for(key)
                        .replacements
                        .fetch_add(1, Ordering::Relaxed);
                    // SAFETY: the exact replacement transferred this cache handle once.
                    unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
                    break CacheWriteOutcome::Replaced;
                }
            }
        };
        (result, replacement)
    }

    /// Inserts or replaces through an exact prepared hot-key handle.
    ///
    /// A stale, wrong-key, cross-cache, overlay, or post-rebuild handle falls
    /// back to the ordinary exact writer route. The handle remains useful
    /// after replacement because the dense index slot itself does not move.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[cfg(feature = "prepared-keys")]
    pub fn insert_discard_prepared_with_options(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheWriteOutcome, CacheInsertError> {
        let pin = self.arena.pin();
        let (result, replacement) =
            self.insert_discard_prepared_pinned(&pin, key, prepared, value, weight, ttl)?;
        drop(pin);
        self.enforce_limits(replacement);
        Ok(result)
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared mutation pairs exact index results with this cache arena pin"
    )]
    fn insert_discard_prepared_pinned(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<(CacheWriteOutcome, DirectHandle), CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            return Ok(self.insert_discard_pinned_at_hard_capacity(
                pin,
                key,
                value,
                compact_weight,
                ttl,
            ));
        };
        // Prepared admission targets an already-hot key, so use the allocator
        // that wins replacement churn and let the single prepared writer probe
        // decide whether this is instead a deleted-key reinsertion. That rare
        // reinsertion remains exact and is reclaimed normally; maintenance can
        // later fold it back into the dense population.
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let outcome = self
            .index
            .insert_prepared(key, prepared, replacement.index_value());
        let result = match outcome {
            InsertOutcome::Inserted => {
                self.add_capacity(key, compact_weight);
                self.enroll_probation(key, replacement);
                hard_capacity.commit_add();
                self.counters_for(key)
                    .inserts
                    .fetch_add(1, Ordering::Relaxed);
                CacheWriteOutcome::Inserted
            }
            InsertOutcome::Replaced(previous) => {
                let previous = published_direct_handle_from_index(previous);
                // SAFETY: the displaced prepared-index handle and pin belong to this cache.
                let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
                self.replace_capacity(key, previous_weight, u64::from(compact_weight));
                hard_capacity.commit_replace(previous_weight);
                self.counters_for(key)
                    .replacements
                    .fetch_add(1, Ordering::Relaxed);
                // SAFETY: the exact prepared replacement transferred this handle once.
                unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
                CacheWriteOutcome::Replaced
            }
        };
        Ok((result, replacement))
    }

    /// Inserts only while the exact key remains absent.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    ///
    pub fn insert_if_absent_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.insert_if_absent_inner(key, value, weight, ttl, true, None)
    }

    /// Inserts only while absent without updating insertion statistics.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn insert_if_absent_untracked_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        self.insert_if_absent_inner(key, value, weight, ttl, false, None)
    }

    #[allow(
        unsafe_code,
        reason = "the slow hard-cap replacement keeps its candidate unpublished until an exact conditional update"
    )]
    fn prepare_replacement_at_hard_capacity<'cache>(
        &'cache self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        value: V,
        compact_weight: u32,
        ttl: Option<Duration>,
    ) -> Option<(
        DirectHandle,
        DirectHandle,
        u64,
        DirectHardCapacityReservation<'cache>,
    )> {
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let Some((previous, previous_weight, hard_capacity)) = self
            .publish_replacement_at_hard_capacity(pin, key, replacement, u64::from(compact_weight))
        else {
            // SAFETY: no exact conditional update published this candidate.
            unsafe { self.arena.drop_unpublished(replacement) };
            return None;
        };
        Some((replacement, previous, previous_weight, hard_capacity))
    }

    /// Replaces a live value without inserting an absent key.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    pub fn replace_discard_with_options(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let pin = self.arena.pin();
        let (replaced, replacement) = self.replace_discard_pinned(&pin, key, value, weight, ttl)?;
        drop(pin);
        if let Some(replacement) = replacement {
            self.enforce_limits(replacement);
        }
        Ok(replaced)
    }

    #[allow(
        unsafe_code,
        reason = "the exact replacement pairs this cache's candidate, old handle, and arena pin"
    )]
    fn replace_discard_pinned(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<(bool, Option<DirectHandle>), CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            let Some((replacement, previous, previous_weight, hard_capacity)) =
                self.prepare_replacement_at_hard_capacity(pin, key, value, compact_weight, ttl)
            else {
                return Ok((false, None));
            };
            self.replace_capacity(key, previous_weight, u64::from(compact_weight));
            hard_capacity.commit_delta_replace(previous_weight, u64::from(compact_weight));
            self.counters_for(key)
                .replacements
                .fetch_add(1, Ordering::Relaxed);
            // SAFETY: the exact conditional update transferred this cache handle once.
            unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
            return Ok((true, Some(replacement)));
        };
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let previous = Cell::new(None);
        if self
            .index
            .update(key, |current| {
                previous.set(Some(*current));
                replacement.index_value()
            })
            .is_none()
        {
            // SAFETY: a failed exact update leaves this cache candidate unpublished.
            unsafe { self.arena.drop_unpublished(replacement) };
            return Ok((false, None));
        }
        let previous = published_direct_handle_from_index(
            previous
                .get()
                .expect("successful cache replacement observed a previous handle"),
        );
        // SAFETY: the displaced index handle and pin belong to this cache.
        let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
        self.replace_capacity(key, previous_weight, u64::from(compact_weight));
        hard_capacity.commit_replace(previous_weight);
        self.counters_for(key)
            .replacements
            .fetch_add(1, Ordering::Relaxed);
        // SAFETY: the exact update transferred this cache handle once.
        unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
        Ok((true, Some(replacement)))
    }

    #[allow(
        unsafe_code,
        reason = "the exact replacement pairs this cache's candidate, old handle, and retirement batch"
    )]
    fn replace_discard_pinned_deferred(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
        pending_retirements: &mut Vec<RemovedDirectHandle>,
    ) -> Result<(bool, Option<DirectHandle>), CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            let Some((replacement, previous, previous_weight, hard_capacity)) =
                self.prepare_replacement_at_hard_capacity(pin, key, value, compact_weight, ttl)
            else {
                return Ok((false, None));
            };
            self.replace_capacity(key, previous_weight, u64::from(compact_weight));
            hard_capacity.commit_delta_replace(previous_weight, u64::from(compact_weight));
            // SAFETY: the exact conditional update transferred this cache handle once.
            pending_retirements.push(unsafe { removed_after_exact_index_transfer(previous) });
            if pending_retirements.len() == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH {
                // SAFETY: every token came from an exact replacement in this cache.
                unsafe { pin.retire_batch(pending_retirements) };
            }
            return Ok((true, Some(replacement)));
        };
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let previous = Cell::new(None);
        if self
            .index
            .update(key, |current| {
                previous.set(Some(*current));
                replacement.index_value()
            })
            .is_none()
        {
            // SAFETY: a failed exact update leaves this cache candidate unpublished.
            unsafe { self.arena.drop_unpublished(replacement) };
            return Ok((false, None));
        }
        let previous = published_direct_handle_from_index(
            previous
                .get()
                .expect("successful deferred replacement observed a previous handle"),
        );
        // SAFETY: the displaced index handle and pin belong to this cache.
        let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
        self.replace_capacity(key, previous_weight, u64::from(compact_weight));
        hard_capacity.commit_replace(previous_weight);
        // SAFETY: the successful exact replacement displaced this handle once.
        pending_retirements.push(unsafe { removed_after_exact_index_transfer(previous) });
        if pending_retirements.len() == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH {
            // SAFETY: every token came from an exact replacement in this cache.
            unsafe { pin.retire_batch(pending_retirements) };
        }
        Ok((true, Some(replacement)))
    }

    /// Replaces a live value through an exact prepared hot-key handle.
    ///
    /// Invalid handles fall back to the ordinary exact writer route, while an
    /// absent key remains absent.
    ///
    /// # Errors
    ///
    /// Rejects values whose charge exceeds the configured or compact limit.
    #[cfg(feature = "prepared-keys")]
    pub fn replace_discard_prepared_with_options(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheInsertError> {
        let pin = self.arena.pin();
        let (replaced, replacement) =
            self.replace_discard_prepared_pinned(&pin, key, prepared, value, weight, ttl)?;
        drop(pin);
        if let Some(replacement) = replacement {
            self.enforce_limits(replacement);
        }
        Ok(replaced)
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared replacement pairs this cache's candidate, old handle, and arena pin"
    )]
    fn replace_discard_prepared_pinned(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
    ) -> Result<(bool, Option<DirectHandle>), CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        // A prepared membership check is one verified dense-slot load. Avoid
        // allocating a replacement when a delete has already made a hot key
        // absent; a racing insertion after this check linearizes afterward.
        if self.index.get_prepared(key, prepared).is_none() {
            return Ok((false, None));
        }
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            let Some((replacement, previous, previous_weight, hard_capacity)) =
                self.prepare_replacement_at_hard_capacity(pin, key, value, compact_weight, ttl)
            else {
                return Ok((false, None));
            };
            self.replace_capacity(key, previous_weight, u64::from(compact_weight));
            hard_capacity.commit_delta_replace(previous_weight, u64::from(compact_weight));
            self.counters_for(key)
                .replacements
                .fetch_add(1, Ordering::Relaxed);
            // SAFETY: the exact conditional update transferred this cache handle once.
            unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
            return Ok((true, Some(replacement)));
        };
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let previous = Cell::new(None);
        if self
            .index
            .update_prepared(key, prepared, |current| {
                previous.set(Some(*current));
                replacement.index_value()
            })
            .is_none()
        {
            // SAFETY: a failed prepared update leaves this cache candidate unpublished.
            unsafe { self.arena.drop_unpublished(replacement) };
            return Ok((false, None));
        }
        let previous = published_direct_handle_from_index(
            previous
                .get()
                .expect("successful prepared replacement observed a previous handle"),
        );
        // SAFETY: the displaced prepared-index handle and pin belong to this cache.
        let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
        self.replace_capacity(key, previous_weight, u64::from(compact_weight));
        hard_capacity.commit_replace(previous_weight);
        self.counters_for(key)
            .replacements
            .fetch_add(1, Ordering::Relaxed);
        // SAFETY: the exact prepared update transferred this handle once.
        unsafe { pin.retire(removed_after_exact_index_transfer(previous)) };
        Ok((true, Some(replacement)))
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        clippy::too_many_arguments,
        unsafe_code,
        reason = "the prepared replacement pairs this cache's exact old handles with its retirement batch"
    )]
    fn replace_discard_prepared_pinned_deferred(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        value: V,
        weight: u64,
        ttl: Option<Duration>,
        pending_retirements: &mut Vec<RemovedDirectHandle>,
    ) -> Result<(bool, Option<DirectHandle>), CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        if self.index.get_prepared(key, prepared).is_none() {
            return Ok((false, None));
        }
        let Some(hard_capacity) = self.try_reserve_publish_capacity(compact_weight) else {
            let Some((replacement, previous, previous_weight, hard_capacity)) =
                self.prepare_replacement_at_hard_capacity(pin, key, value, compact_weight, ttl)
            else {
                return Ok((false, None));
            };
            self.replace_capacity(key, previous_weight, u64::from(compact_weight));
            hard_capacity.commit_delta_replace(previous_weight, u64::from(compact_weight));
            // SAFETY: the exact conditional update transferred this cache handle once.
            pending_retirements.push(unsafe { removed_after_exact_index_transfer(previous) });
            if pending_retirements.len() == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH {
                // SAFETY: every token came from an exact replacement in this cache.
                unsafe { pin.retire_batch(pending_retirements) };
            }
            return Ok((true, Some(replacement)));
        };
        let replacement = self
            .arena
            .allocate_boxed(value, compact_weight, self.deadline(ttl));
        let previous = Cell::new(None);
        if self
            .index
            .update_prepared(key, prepared, |current| {
                previous.set(Some(*current));
                replacement.index_value()
            })
            .is_none()
        {
            // SAFETY: a failed prepared update leaves this cache candidate unpublished.
            unsafe { self.arena.drop_unpublished(replacement) };
            return Ok((false, None));
        }
        let previous = published_direct_handle_from_index(
            previous
                .get()
                .expect("successful deferred prepared replacement observed a previous handle"),
        );
        // SAFETY: the displaced prepared-index handle and pin belong to this cache.
        let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
        self.replace_capacity(key, previous_weight, u64::from(compact_weight));
        hard_capacity.commit_replace(previous_weight);
        // SAFETY: the successful prepared replacement displaced this handle once.
        pending_retirements.push(unsafe { removed_after_exact_index_transfer(previous) });
        if pending_retirements.len() == crate::cache_arena::DIRECT_RECLAIM_PUBLISH_BATCH {
            // SAFETY: every token came from an exact replacement in this cache.
            unsafe { pin.retire_batch(pending_retirements) };
        }
        Ok((true, Some(replacement)))
    }

    #[allow(
        unsafe_code,
        reason = "failed publication leaves this cache's candidate uniquely unpublished"
    )]
    fn insert_if_absent_inner(
        &self,
        key: &[u8],
        value: V,
        weight: u64,
        ttl: Option<Duration>,
        record_stats: bool,
        admission: Option<DirectAdmissionContext<'_, '_>>,
    ) -> Result<CacheAdmissionOutcome, CacheInsertError> {
        let compact_weight = self.validate_weight(key, weight)?;
        let admission_hash = match self.admission_precheck(key) {
            Ok(hash) => hash,
            Err(outcome) => return Ok(outcome),
        };
        let Some(hard_capacity) = self.reserve_admission_capacity(key, compact_weight) else {
            return Ok(CacheAdmissionOutcome::Existing);
        };
        let inserted = self
            .arena
            .allocate(value, compact_weight, self.deadline(ttl));
        let published = admission.as_ref().map_or_else(
            || {
                admission_hash.map_or_else(
                    || self.index.get_or_insert(key, inserted.index_value()),
                    |key_hash| {
                        self.index
                            .get_or_insert_hashed(key, inserted.index_value(), key_hash)
                    },
                )
            },
            |context| {
                if let Some(pending) = context.pending_lengths {
                    if pending.use_immediate() {
                        return admission_hash.map_or_else(
                            || context.index.get_or_insert(key, inserted.index_value()),
                            |key_hash| {
                                context.index.get_or_insert_hashed(
                                    key,
                                    inserted.index_value(),
                                    key_hash,
                                )
                            },
                        );
                    }
                    let (published, deferred_stripe) = admission_hash.map_or_else(
                        || {
                            context
                                .index
                                .get_or_insert_deferred_len(key, inserted.index_value())
                        },
                        |key_hash| {
                            context.index.get_or_insert_deferred_len_hashed(
                                key,
                                inserted.index_value(),
                                key_hash,
                            )
                        },
                    );
                    if let Some(stripe) = deferred_stripe {
                        pending.record(context.index, stripe);
                    }
                    published
                } else {
                    admission_hash.map_or_else(
                        || context.index.get_or_insert(key, inserted.index_value()),
                        |key_hash| {
                            context.index.get_or_insert_hashed(
                                key,
                                inserted.index_value(),
                                key_hash,
                            )
                        },
                    )
                }
            },
        );
        if published != inserted.index_value() {
            // SAFETY: another value won publication, so this candidate is unreachable.
            unsafe { self.arena.drop_unpublished(inserted) };
            return Ok(CacheAdmissionOutcome::Existing);
        }
        self.add_capacity(key, compact_weight);
        self.enroll_probation(key, inserted);
        hard_capacity.commit_add();
        self.note_admission_population();
        if record_stats {
            self.counters_for(key)
                .inserts
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Some(protected) = admission.and_then(|context| context.deferred_limit) {
            protected.set(Some(inserted));
        } else {
            self.enforce_limits(inserted);
        }
        Ok(CacheAdmissionOutcome::Inserted)
    }

    /// Removes and returns a protected value.
    #[allow(
        unsafe_code,
        reason = "the exact removal and arena pin belong to this cache"
    )]
    pub fn remove(&self, key: &[u8]) -> Option<DirectCacheValue<'_, V>> {
        let pin = self.arena.pin();
        let handle = published_direct_handle_from_index(self.index.remove(key)?);
        // SAFETY: the exact removed handle and pin belong to this cache.
        let weight = unsafe { protected_direct_entry(&pin, handle) }.weight();
        self.remove_capacity(key, weight);
        self.counters_for(key)
            .removals
            .fetch_add(1, Ordering::Relaxed);
        Some(DirectCacheValue {
            // SAFETY: exact removal transferred this handle once; the same pin
            // retains protection while retirement is queued.
            entry: unsafe { pin.into_retired(removed_after_exact_index_transfer(handle)) },
        })
    }

    /// Removes without retaining the previous value.
    #[allow(
        unsafe_code,
        reason = "the exact removal transfers one handle from this cache to its arena"
    )]
    pub fn remove_discard(&self, key: &[u8]) -> bool {
        // Delay the mutation pin until the index has found a live value, but
        // acquire it before the retry-safe predicate permits exact removal.
        // This keeps delete misses on their previous pin-free path.
        let mutation = OnceCell::new();
        let Some(raw) = self.index.remove_if(key, |_| {
            mutation.get_or_init(|| self.arena.mutation_pin());
            true
        }) else {
            return false;
        };
        let mutation = mutation
            .into_inner()
            .unwrap_or_else(|| std::process::abort());
        let handle = published_direct_handle_from_index(raw);
        // SAFETY: exact removal uniquely owns this still-allocated handle; the
        // mutation pin prevents reclamation until it has been queued below.
        let removed = unsafe { removed_after_exact_index_transfer(handle) };
        // SAFETY: the exact removed token remains uniquely allocated here.
        let weight = unsafe { DirectValueArena::<V>::removed_weight(&removed) };
        self.remove_capacity(key, weight);
        self.counters_for(key)
            .removals
            .fetch_add(1, Ordering::Relaxed);
        // SAFETY: exact removal occurred while `mutation` was active, and the
        // same mutation pin remains active through this queue publication.
        unsafe { mutation.retire(removed) };
        true
    }

    #[allow(
        unsafe_code,
        reason = "the guarded exact removal transfers this cache handle into its retirement batch"
    )]
    fn remove_discard_guarded_deferred(
        &self,
        _mutation: &DirectArenaMutationPin<'_, V>,
        index: &crate::generation_map::AtomicReadGuard<'_>,
        key: &[u8],
        pending_retirements: &mut Vec<RemovedDirectHandle>,
        pending_lengths: &DirectPendingLengths,
    ) -> bool {
        let (removed, deferred_stripe) = index.remove_deferred_len(key);
        let Some(raw) = removed else {
            return false;
        };
        if let Some(stripe) = deferred_stripe {
            pending_lengths.record_removal(index, stripe);
        }
        // SAFETY: exact guarded removal transferred this handle once.
        let handle =
            unsafe { removed_after_exact_index_transfer(published_direct_handle_from_index(raw)) };
        // SAFETY: exact removal uniquely owns this still-allocated handle; the
        // mutation pin was active before removal and remains so through queuing.
        let weight = unsafe { DirectValueArena::<V>::removed_weight(&handle) };
        let shard = key_counter_hash(key) & (COUNTER_SHARDS - 1);
        self.remove_capacity_from_shard(shard, weight);
        pending_retirements.push(handle);
        true
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared exact removal transfers this cache handle into its retirement batch"
    )]
    fn remove_discard_prepared_guarded_deferred(
        &self,
        _mutation: &DirectArenaMutationPin<'_, V>,
        index: &crate::generation_map::AtomicReadGuard<'_>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        pending_retirements: &mut Vec<RemovedDirectHandle>,
        pending_lengths: &DirectPendingLengths,
    ) -> bool {
        let (removed, deferred_stripe) = index.remove_prepared_deferred_len(key, prepared);
        let Some(raw) = removed else {
            return false;
        };
        if let Some(stripe) = deferred_stripe {
            pending_lengths.record_removal(index, stripe);
        }
        // SAFETY: exact prepared removal transferred this handle once.
        let handle =
            unsafe { removed_after_exact_index_transfer(published_direct_handle_from_index(raw)) };
        // SAFETY: exact removal uniquely owns this still-allocated handle; the
        // mutation pin was active before removal and remains so through queuing.
        let weight = unsafe { DirectValueArena::<V>::removed_weight(&handle) };
        let shard = key_counter_hash(key) & (COUNTER_SHARDS - 1);
        self.remove_capacity_from_shard(shard, weight);
        pending_retirements.push(handle);
        true
    }

    /// Removes through an exact prepared hot-key handle without retaining the value.
    ///
    /// Invalid handles fall back to the ordinary exact removal route.
    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared exact removal and arena pin belong to this cache"
    )]
    pub fn remove_discard_prepared(&self, key: &[u8], prepared: &AtomicPreparedKey) -> bool {
        let pin = self.arena.pin();
        let Some(raw) = self.index.remove_prepared(key, prepared) else {
            return false;
        };
        let handle = published_direct_handle_from_index(raw);
        // SAFETY: the exact removed prepared handle and pin belong to this cache.
        let weight = unsafe { protected_direct_entry(&pin, handle) }.weight();
        self.remove_capacity(key, weight);
        self.counters_for(key)
            .removals
            .fetch_add(1, Ordering::Relaxed);
        // SAFETY: exact prepared removal transferred this handle once.
        unsafe { pin.retire(removed_after_exact_index_transfer(handle)) };
        true
    }

    /// Replaces a live value's expiration deadline.
    pub fn touch(&self, key: &[u8], ttl: Option<Duration>) -> bool {
        let pin = self.arena.pin();
        self.touch_pinned(&pin, key, ttl)
    }

    /// Replaces expiration through an exact prepared hot-key handle.
    ///
    /// Invalid handles fall back to the ordinary exact lookup route.
    #[cfg(feature = "prepared-keys")]
    pub fn touch_prepared(
        &self,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        ttl: Option<Duration>,
    ) -> bool {
        let pin = self.arena.pin();
        self.touch_prepared_pinned(&pin, key, prepared, ttl)
    }

    /// Returns the current live entry count.
    ///
    /// Caches without an entry limit reuse the index's exact logical length
    /// instead of maintaining a duplicate foreground counter.
    /// Under concurrent mutation this is a weak snapshot: publication and
    /// sharded bookkeeping can be observed out of order, with the aggregate
    /// temporarily wrapping modulo `u64` until mutations quiesce. It is not a
    /// linearizable observation; callers must not use it as hard-capacity
    /// reservation state.
    #[must_use]
    pub fn len(&self) -> usize {
        if self.config.max_entries.is_none() {
            return self.index.len();
        }
        usize::try_from(
            self.capacity
                .iter()
                .map(|counter| counter.entries.load(Ordering::Relaxed))
                .fold(0_u64, u64::wrapping_add),
        )
        .unwrap_or(usize::MAX)
    }

    /// Returns whether the cache has no live entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the caller-accounted live weight.
    ///
    /// When the weight limit is disabled with `u64::MAX`, this derives the
    /// total from live entry metadata. That keeps an unused capacity dimension
    /// off the mutation path at the cost of an O(n) observation.
    /// Under concurrent mutation a bounded-weight result is a weak snapshot:
    /// publication and sharded bookkeeping can be observed out of order, with
    /// the aggregate temporarily wrapping modulo `u64` until mutations
    /// quiesce. It is not linearizable; callers must not use it as
    /// hard-capacity reservation state.
    #[must_use]
    #[allow(
        unsafe_code,
        reason = "the scan and arena pin belong to this exact cache"
    )]
    pub fn weight(&self) -> u64 {
        if self.config.max_weight == u64::MAX {
            #[cfg(test)]
            DIRECT_WEIGHT_SCANS.with(|scans| {
                scans.fetch_add(1, Ordering::Relaxed);
            });
            let pin = self.arena.pin();
            let mut weight = 0_u64;
            self.index.scan_entries(&mut |_, raw| {
                weight = weight.saturating_add(
                    // SAFETY: scan values come unchanged from this cache index.
                    unsafe {
                        protected_direct_entry(&pin, published_direct_handle_from_index(raw))
                    }
                    .weight(),
                );
            });
            return weight;
        }
        self.capacity
            .iter()
            .map(|counter| counter.weight.load(Ordering::Relaxed))
            .fold(0_u64, u64::wrapping_add)
    }

    /// Captures capacity state and cumulative operation counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.counters.iter().fold(
            CacheStats {
                entries: self.len(),
                weight: self.weight(),
                max_weight: self.config.max_weight,
                ..CacheStats::default()
            },
            |mut total, counters| {
                total.hits += counters.hits.load(Ordering::Relaxed);
                total.misses += counters.misses.load(Ordering::Relaxed);
                total.inserts += counters.inserts.load(Ordering::Relaxed);
                total.replacements += counters.replacements.load(Ordering::Relaxed);
                total.removals += counters.removals.load(Ordering::Relaxed);
                total.evictions += counters.evictions.load(Ordering::Relaxed);
                total.expirations += counters.expirations.load(Ordering::Relaxed);
                total.rejected += counters.rejected.load(Ordering::Relaxed);
                total.rebuilds += counters.rebuilds.load(Ordering::Relaxed);
                total.maintenance_errors += counters.maintenance_errors.load(Ordering::Relaxed);
                total
            },
        )
    }

    /// Captures exact epoch-retirement and replacement-recycler state.
    ///
    /// This diagnostic locks every retirement and active recycler shard. It is
    /// intended for benchmarks and production investigations, not hot paths.
    #[cfg(feature = "cache-diagnostics")]
    #[must_use]
    pub fn reclamation_stats(&self) -> DirectCacheReclamationStats {
        let snapshot = self.arena.reclamation_snapshot();
        let pressure = self.pressure_timing_snapshot();
        DirectCacheReclamationStats {
            current_epoch: snapshot.current_epoch,
            readers: snapshot.readers,
            retired_values: snapshot.retired_values,
            retired_bytes: snapshot.retired_bytes,
            published_retired_values: snapshot.published_retired_values,
            recyclable_allocations: snapshot.recyclable_allocations,
            foreground_capacity_enforcements: pressure.foreground_capacity_enforcements,
            foreground_capacity_enforcement_ns: pressure.foreground_capacity_enforcement_ns,
            max_foreground_capacity_enforcement_ns: pressure.max_foreground_capacity_enforcement_ns,
            background_capacity_drains: pressure.background_capacity_drains,
            background_capacity_drain_ns: pressure.background_capacity_drain_ns,
            max_background_capacity_drain_ns: pressure.max_background_capacity_drain_ns,
            victim_collections: pressure.victim_collections,
            victim_collection_ns: pressure.victim_collection_ns,
            max_victim_collection_ns: pressure.max_victim_collection_ns,
        }
    }

    /// Captures feature-gated capacity, policy, maintenance, and arena telemetry.
    ///
    /// The snapshot takes the same cold retirement and arena locks as
    /// [`Self::reclamation_stats`]. Do not call it from a request hot path.
    #[cfg(feature = "cache-production-diagnostics")]
    #[must_use]
    #[allow(
        clippy::too_many_lines,
        reason = "one explicit snapshot initializer keeps every public counter auditable"
    )]
    pub fn production_diagnostics(&self) -> DirectCacheProductionStats {
        let arena = self.arena.production_snapshot();
        let current_entries = self.len();
        let current_weight = self.weight();
        let current_entry_debt = self
            .config
            .max_entries
            .map_or(0, |maximum| current_entries.saturating_sub(maximum));
        let current_weight_debt = if self.config.max_weight == u64::MAX {
            0
        } else {
            current_weight.saturating_sub(self.config.max_weight)
        };
        let current_entry_overshoot = self.config.max_entries.map_or(0, |maximum| {
            u64::try_from(current_entry_debt)
                .unwrap_or(u64::MAX)
                .saturating_mul(10_000)
                / u64::try_from(maximum).unwrap_or(u64::MAX).max(1)
        });
        let current_weight_overshoot = if self.config.max_weight == u64::MAX {
            0
        } else {
            current_weight_debt.saturating_mul(10_000) / self.config.max_weight.max(1)
        };
        let foreground_count = self
            .diagnostics
            .foreground_capacity_enforcements
            .load(Ordering::Relaxed);
        let foreground_max = self
            .diagnostics
            .max_foreground_capacity_enforcement_ns
            .load(Ordering::Relaxed);
        let maintenance_worker_failures = self.counters.iter().fold(0_u64, |total, counters| {
            total.saturating_add(counters.maintenance_errors.load(Ordering::Relaxed))
        });
        DirectCacheProductionStats {
            current_entries,
            current_weight,
            peak_entries: self
                .diagnostics
                .peak_entries
                .load(Ordering::Relaxed)
                .max(current_entries),
            peak_weight: self
                .diagnostics
                .peak_weight
                .load(Ordering::Relaxed)
                .max(current_weight),
            peak_soft_limit_overshoot_bps: self
                .diagnostics
                .peak_soft_limit_overshoot_bps
                .load(Ordering::Relaxed)
                .max(current_entry_overshoot.max(current_weight_overshoot)),
            current_entry_debt,
            current_weight_debt,
            peak_entry_debt: self
                .diagnostics
                .peak_entry_debt
                .load(Ordering::Relaxed)
                .max(current_entry_debt),
            peak_weight_debt: self
                .diagnostics
                .peak_weight_debt
                .load(Ordering::Relaxed)
                .max(current_weight_debt),
            retired_entries: arena.retired_values,
            retired_bytes: arena.retired_bytes,
            peak_retired_entries: arena.peak_retired_values,
            peak_retired_bytes: arena.peak_retired_bytes,
            foreground_hard_limit_enforcements: foreground_count,
            foreground_hard_limit_total_ns: self
                .diagnostics
                .foreground_capacity_enforcement_ns
                .load(Ordering::Relaxed),
            foreground_hard_limit_p99_ns: diagnostic_histogram_percentile(
                &self.diagnostics.foreground_capacity_enforcement_histogram,
                foreground_count,
                99,
            )
            .min(foreground_max),
            foreground_hard_limit_max_ns: foreground_max,
            background_drains: self
                .diagnostics
                .background_capacity_drains
                .load(Ordering::Relaxed),
            background_drain_total_ns: self
                .diagnostics
                .background_capacity_drain_ns
                .load(Ordering::Relaxed),
            background_drain_max_ns: self
                .diagnostics
                .max_background_capacity_drain_ns
                .load(Ordering::Relaxed),
            background_entry_debt_total: self
                .diagnostics
                .background_entry_debt_total
                .load(Ordering::Relaxed),
            background_weight_debt_total: self
                .diagnostics
                .background_weight_debt_total
                .load(Ordering::Relaxed),
            victim_batches: self.diagnostics.victim_batches.load(Ordering::Relaxed),
            victims_examined: self.diagnostics.victims_examined.load(Ordering::Relaxed),
            victims_removed: self.diagnostics.victims_removed.load(Ordering::Relaxed),
            max_victims_examined_per_batch: self
                .diagnostics
                .max_victims_examined_per_batch
                .load(Ordering::Relaxed),
            native_victim_collections: self.diagnostics.victim_collections.load(Ordering::Relaxed),
            native_victim_collection_total_ns: self
                .diagnostics
                .victim_collection_ns
                .load(Ordering::Relaxed),
            native_victim_collection_max_ns: self
                .diagnostics
                .max_victim_collection_ns
                .load(Ordering::Relaxed),
            rejected_item_too_heavy: self
                .diagnostics
                .rejected_item_too_heavy
                .load(Ordering::Relaxed),
            rejected_weight_not_compact: self
                .diagnostics
                .rejected_weight_not_compact
                .load(Ordering::Relaxed),
            rejected_doorkeeper_first_sighting: self
                .diagnostics
                .rejected_doorkeeper_first_sighting
                .load(Ordering::Relaxed),
            rejected_frequency: self.diagnostics.rejected_frequency.load(Ordering::Relaxed),
            doorkeeper_admissions: self
                .diagnostics
                .doorkeeper_admissions
                .load(Ordering::Relaxed),
            doorkeeper_rotations: self
                .admission_doorkeeper
                .as_ref()
                .map_or(0, DirectAdmissionDoorkeeper::rotations),
            frequency_gate_active: self
                .diagnostics
                .frequency_gate_state
                .load(Ordering::Relaxed)
                == 2,
            frequency_gate_transitions: self
                .diagnostics
                .frequency_gate_transitions
                .load(Ordering::Relaxed),
            frequency_gate_activations: self
                .diagnostics
                .frequency_gate_activations
                .load(Ordering::Relaxed),
            rolling_estimator_cas_retries: self
                .diagnostics
                .rolling_estimator_cas_retries
                .load(Ordering::Relaxed),
            maintenance_worker_wakeups: self
                .diagnostics
                .maintenance_worker_wakeups
                .load(Ordering::Relaxed),
            maintenance_worker_runs: self
                .diagnostics
                .maintenance_worker_runs
                .load(Ordering::Relaxed),
            maintenance_worker_failures,
            maintenance_pending: self.maintenance_pending.load(Ordering::Acquire),
            arena_allocation_requests: arena.allocation_requests,
            boxed_allocation_requests: arena.boxed_allocation_requests,
            recycled_box_reuses: arena.recycled_box_reuses,
            arena_blocks: arena.blocks,
            arena_allocated_bytes: arena.allocated_bytes,
            arena_active_allocations: arena.active_allocations,
            arena_active_bytes: arena.active_bytes,
            arena_reusable_allocations: arena.reusable_allocations,
            arena_reusable_bytes: arena.reusable_bytes,
            arena_block_growths: arena.block_growths,
            arena_block_releases: arena.block_releases,
        }
    }

    #[cfg(all(feature = "cache-diagnostics", feature = "cache-pressure-timing"))]
    fn pressure_timing_snapshot(&self) -> DirectPressureTimingSnapshot {
        DirectPressureTimingSnapshot {
            foreground_capacity_enforcements: self
                .diagnostics
                .foreground_capacity_enforcements
                .load(Ordering::Relaxed),
            foreground_capacity_enforcement_ns: self
                .diagnostics
                .foreground_capacity_enforcement_ns
                .load(Ordering::Relaxed),
            max_foreground_capacity_enforcement_ns: self
                .diagnostics
                .max_foreground_capacity_enforcement_ns
                .load(Ordering::Relaxed),
            background_capacity_drains: self
                .diagnostics
                .background_capacity_drains
                .load(Ordering::Relaxed),
            background_capacity_drain_ns: self
                .diagnostics
                .background_capacity_drain_ns
                .load(Ordering::Relaxed),
            max_background_capacity_drain_ns: self
                .diagnostics
                .max_background_capacity_drain_ns
                .load(Ordering::Relaxed),
            victim_collections: self.diagnostics.victim_collections.load(Ordering::Relaxed),
            victim_collection_ns: self
                .diagnostics
                .victim_collection_ns
                .load(Ordering::Relaxed),
            max_victim_collection_ns: self
                .diagnostics
                .max_victim_collection_ns
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(all(feature = "cache-diagnostics", not(feature = "cache-pressure-timing")))]
    #[allow(
        clippy::unused_self,
        reason = "keeps the no-timing snapshot signature identical to the timed cfg"
    )]
    fn pressure_timing_snapshot(&self) -> DirectPressureTimingSnapshot {
        DirectPressureTimingSnapshot::default()
    }

    /// Purges expired entries, enforces limits, and rebuilds adaptively.
    ///
    /// Rebuild is deferred while guarded mutations retain generation
    /// reservations. Refresh, quiesce, or drop those guards to allow a later
    /// cycle to rebuild. Ordinary borrowed reads do not retain reservations.
    ///
    /// # Errors
    ///
    /// Returns an index construction error if adaptive rebuild fails.
    pub fn maintain(&self) -> Result<CacheMaintenanceResult, FrozenBuildError> {
        let completion = self.maintenance_completions.load(Ordering::Acquire);
        self.maintain_after_completion(completion)
    }

    fn maintain_after_completion(
        &self,
        observed_completion: u64,
    ) -> Result<CacheMaintenanceResult, FrozenBuildError> {
        // Full cycles coalesce independently of capacity eviction. A rebuild
        // defers if guarded writers retain generation reservations.
        let _reclaim_deferral = self.arena.defer_reclamation();
        let _full_maintenance = self.full_maintenance_gate.lock();
        if self.maintenance_completions.load(Ordering::Acquire) != observed_completion {
            return Ok(CacheMaintenanceResult::default());
        }
        let (expired, evicted) = {
            let _reclaim_deferral = self.arena.defer_reclamation();
            let _capacity = self.maintenance_gate.lock();
            let now = self.now();
            let expired = if self.expiration_possible.load(Ordering::Acquire) {
                self.purge_expired_locked(now)
            } else {
                0
            };
            let evicted = self.evict_to_limits(now, None, None);
            (expired, evicted)
        };
        let rebuilt = self
            .index
            .try_rebuild_adaptive_if_needed(self.config.rebuild_policy)?
            .is_some();
        if rebuilt {
            self.maintenance_counters()
                .rebuilds
                .fetch_add(1, Ordering::Relaxed);
        }
        self.arena.reclaim_retired();
        self.index.reclaim_retired_generations();
        self.maintenance_completions
            .store(observed_completion.wrapping_add(1), Ordering::Release);
        Ok(CacheMaintenanceResult {
            expired,
            evicted,
            rebuilt,
        })
    }

    /// Starts wake-driven capacity eviction plus periodic full maintenance.
    ///
    /// In async-eviction mode, admissions above the soft target unpark this
    /// worker immediately. Adaptive rebuild and expiration maintenance still
    /// run no more often than `interval`. Dropping the returned handle requests
    /// shutdown and joins the worker. Starting a replacement after shutdown is
    /// supported; if multiple workers overlap, only the newest receives
    /// pressure wakeups.
    #[must_use]
    pub fn spawn_maintenance(self: &Arc<Self>, interval: Duration) -> CacheMaintenance
    where
        V: Send + Sync + 'static,
    {
        let interval = interval.max(Duration::from_millis(1));
        self.index.defer_retired_generation_drops();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let cache = Arc::clone(self);
        let join = thread::spawn(move || {
            let mut next_full = Instant::now() + interval;
            while !worker_stop.load(Ordering::Acquire) {
                #[cfg(feature = "cache-production-diagnostics")]
                cache
                    .diagnostics
                    .maintenance_worker_runs
                    .fetch_add(1, Ordering::Relaxed);
                let removed = cache.drain_capacity();
                if cache.rebuild_capacity_pressure().is_err() {
                    cache
                        .maintenance_counters()
                        .maintenance_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                cache.index.reclaim_retired_generations();
                let now = Instant::now();
                if now >= next_full {
                    if cache.maintain().is_err() {
                        cache
                            .maintenance_counters()
                            .maintenance_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    next_full = now + interval;
                }
                if removed == 0 || !cache.over_limit() {
                    cache.maintenance_pending.store(false, Ordering::Release);
                    if cache.over_limit() && !cache.maintenance_pending.swap(true, Ordering::AcqRel)
                    {
                        continue;
                    }
                    thread::park_timeout(next_full.saturating_duration_since(Instant::now()));
                }
            }
        });
        let worker = join.thread().clone();
        let registered_worker = Arc::new(worker.clone());
        self.maintenance_worker
            .store(Some(Arc::clone(&registered_worker)));
        CacheMaintenance {
            stop,
            worker,
            join: Some(join),
            registration: Some(Arc::clone(&self.maintenance_worker)),
            registered_worker: Some(registered_worker),
        }
    }

    #[allow(
        unsafe_code,
        reason = "each lookup pairs this cache's unchanged index handle with its arena pin"
    )]
    fn lookup(&self, key: &[u8], record_access: bool) -> Option<DirectCacheValue<'_, V>> {
        for _ in 0..16 {
            let pin = self.arena.pin();
            let raw = self.index.get_protected(key)?;
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(&pin, handle) };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.now() {
                self.expire_handle(key, handle);
                continue;
            }
            if record_access {
                entry.mark_accessed();
                if self.counters_for(key).hits.load(Ordering::Relaxed)
                    & self.access_sample_mask(entry.weight())
                    == 0
                {
                    self.note_frequency_access(key);
                }
            }
            return Some(DirectCacheValue {
                // SAFETY: the still-published handle belongs to this cache pin.
                entry: unsafe { pin.into_protected(handle) },
            });
        }
        None
    }

    #[allow(
        unsafe_code,
        reason = "clones only while the exact index handle is protected by this cache pin"
    )]
    fn lookup_cloned(&self, key: &[u8], record_access: bool) -> Option<V>
    where
        V: Clone,
    {
        for _ in 0..16 {
            let pin = self.arena.pin();
            let raw = self.index.get_protected(key)?;
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(&pin, handle) };
            let expires_at = entry.expires_at();
            if expires_at != NEVER_EXPIRES && expires_at <= self.now() {
                self.expire_handle(key, handle);
                continue;
            }
            if record_access {
                entry.mark_accessed();
                if self.counters_for(key).hits.load(Ordering::Relaxed)
                    & self.access_sample_mask(entry.weight())
                    == 0
                {
                    self.note_frequency_access(key);
                }
            }
            return Some(entry.value().clone());
        }
        None
    }

    #[allow(
        unsafe_code,
        reason = "the lookup handle and supplied pin belong to this cache"
    )]
    fn touch_pinned(&self, pin: &DirectArenaPin<'_, V>, key: &[u8], ttl: Option<Duration>) -> bool {
        if ttl.is_none() {
            let Some(raw) = self.index.get_protected(key) else {
                return false;
            };
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(pin, handle) };
            let expires_at = entry.expires_at();
            if expires_at == NEVER_EXPIRES {
                entry.mark_accessed();
                return true;
            }
            if expires_at <= self.now() {
                self.expire_handle(key, handle);
                return false;
            }
        }
        self.touch_pinned_locked(pin, key, ttl)
    }

    #[allow(
        unsafe_code,
        reason = "the mutation stripe keeps this cache's index handle paired with its pin"
    )]
    fn touch_pinned_locked(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        ttl: Option<Duration>,
    ) -> bool {
        let _reclaim_deferral = self.arena.defer_reclamation();
        let mutation = self.mutation_stripe(key).lock();
        let Some(raw) = self.index.get_protected(key) else {
            return false;
        };
        let handle = published_direct_handle_from_index(raw);
        // SAFETY: the unchanged index handle and pin belong to this cache.
        let entry = unsafe { protected_direct_entry(pin, handle) };
        let expires_at = entry.expires_at();
        if expires_at != NEVER_EXPIRES && expires_at <= self.now() {
            drop(mutation);
            self.expire_handle(key, handle);
            return false;
        }
        self.note_expiration(ttl);
        let deadline = ttl.map_or(NEVER_EXPIRES, |ttl| {
            Self::deadline_at(Some(ttl), self.now())
        });
        if deadline != expires_at {
            entry.set_expires_at(deadline);
        }
        entry.mark_accessed();
        true
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the prepared lookup handle and supplied pin belong to this cache"
    )]
    fn touch_prepared_pinned(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        ttl: Option<Duration>,
    ) -> bool {
        if ttl.is_none() {
            let Some(raw) = self.index.get_prepared(key, prepared) else {
                return false;
            };
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the unchanged prepared-index handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(pin, handle) };
            let expires_at = entry.expires_at();
            if expires_at == NEVER_EXPIRES {
                entry.mark_accessed();
                return true;
            }
            if expires_at <= self.now() {
                self.expire_handle(key, handle);
                return false;
            }
        }
        self.touch_prepared_pinned_locked(pin, key, prepared, ttl)
    }

    #[cfg(feature = "prepared-keys")]
    #[allow(
        unsafe_code,
        reason = "the mutation stripe keeps this cache's prepared handle paired with its pin"
    )]
    fn touch_prepared_pinned_locked(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        prepared: &AtomicPreparedKey,
        ttl: Option<Duration>,
    ) -> bool {
        let _reclaim_deferral = self.arena.defer_reclamation();
        let mutation = self.mutation_stripe(key).lock();
        let Some(raw) = self.index.get_prepared(key, prepared) else {
            return false;
        };
        let handle = published_direct_handle_from_index(raw);
        // SAFETY: the unchanged prepared-index handle and pin belong to this cache.
        let entry = unsafe { protected_direct_entry(pin, handle) };
        let expires_at = entry.expires_at();
        if expires_at != NEVER_EXPIRES && expires_at <= self.now() {
            drop(mutation);
            self.expire_handle(key, handle);
            return false;
        }
        self.note_expiration(ttl);
        let deadline = ttl.map_or(NEVER_EXPIRES, |ttl| {
            Self::deadline_at(Some(ttl), self.now())
        });
        if deadline != expires_at {
            entry.set_expires_at(deadline);
        }
        entry.mark_accessed();
        true
    }

    #[allow(
        unsafe_code,
        reason = "the exact expiration removal and arena pin belong to this cache"
    )]
    fn expire_handle(&self, key: &[u8], expected: DirectHandle) -> bool {
        let _reclaim_deferral = self.arena.defer_reclamation();
        let _mutation = self.mutation_stripe(key).lock();
        let pin = self.arena.pin();
        let Some(raw) = self.index.get_protected(key) else {
            return false;
        };
        if raw != expected.index_value() {
            return false;
        }
        // SAFETY: equality with the unchanged index value proves this cache handle.
        let entry = unsafe { protected_direct_entry(&pin, expected) };
        if !Self::is_expired(entry, self.now()) {
            return false;
        }
        let weight = entry.weight();
        if self
            .index
            .remove_if(key, |current| *current == raw)
            .is_none()
        {
            return false;
        }
        self.remove_capacity(key, weight);
        self.counters_for(key)
            .expirations
            .fetch_add(1, Ordering::Relaxed);
        // SAFETY: exact conditional removal transferred this handle once.
        unsafe { pin.retire(removed_after_exact_index_transfer(expected)) };
        true
    }

    fn admission_precheck(
        &self,
        key: &[u8],
    ) -> Result<Option<GenerationKeyHash>, CacheAdmissionOutcome> {
        let Some(doorkeeper) = self.admission_doorkeeper.as_ref() else {
            return Ok(None);
        };
        let key_hash = self.index.key_hash(key);
        let decision = doorkeeper.admission_decision_hashed(
            doorkeeper_hashes_from_key_hash(key_hash),
            self.frequency_admission_active(),
        );
        if decision == DirectAdmissionDecision::Admit {
            #[cfg(feature = "cache-production-diagnostics")]
            self.diagnostics
                .doorkeeper_admissions
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Some(key_hash));
        }
        if self.index.get_hashed(key, key_hash).is_some() {
            return Err(CacheAdmissionOutcome::Existing);
        }
        #[cfg(feature = "cache-production-diagnostics")]
        match decision {
            DirectAdmissionDecision::RejectFirstSighting => {
                self.diagnostics
                    .rejected_doorkeeper_first_sighting
                    .fetch_add(1, Ordering::Relaxed);
            }
            DirectAdmissionDecision::RejectFrequency => {
                self.diagnostics
                    .rejected_frequency
                    .fetch_add(1, Ordering::Relaxed);
            }
            DirectAdmissionDecision::Admit => unreachable!("admitted candidates returned above"),
        }
        self.counters_for(key)
            .rejected
            .fetch_add(1, Ordering::Relaxed);
        Err(CacheAdmissionOutcome::Rejected)
    }

    fn note_admission_population(&self) {
        if let Some(doorkeeper) = &self.admission_doorkeeper {
            doorkeeper.note_admission();
        }
    }

    fn frequency_admission_active(&self) -> bool {
        let active =
            if let Some(minimum_hit_rate) = self.config.frequency_admission_min_hit_rate_bps {
                self.frequency_hit_rate_at_least(minimum_hit_rate)
            } else {
                true
            };
        #[cfg(feature = "cache-production-diagnostics")]
        self.diagnostics.note_frequency_gate(active);
        active
    }

    fn frequency_victim_gate(&self) -> u8 {
        let base = self.config.frequency_admission_max_gate.unwrap_or(0);
        self.config
            .frequency_admission_high_reuse
            .map_or(base, |(high, minimum_hit_rate)| {
                if self.frequency_hit_rate_at_least(minimum_hit_rate) {
                    high
                } else {
                    base
                }
            })
    }

    fn frequency_hit_rate_at_least(&self, minimum_hit_rate: u16) -> bool {
        if minimum_hit_rate == 0 {
            return true;
        }
        let mut state = self.frequency_reuse.load(Ordering::Relaxed);
        if unpack_frequency_reuse(state).1 < FREQUENCY_REUSE_MIN_LOOKUPS {
            self.publish_frequency_reuse();
            state = self.frequency_reuse.load(Ordering::Relaxed);
        }
        let (hits, lookups) = unpack_frequency_reuse(state);
        lookups >= FREQUENCY_REUSE_MIN_LOOKUPS
            && u128::from(hits) * 10_000 >= u128::from(lookups) * u128::from(minimum_hit_rate)
    }

    fn record_frequency_reuse(&self, hits: u64, misses: u64) {
        let shard = DirectValueArena::<V>::thread_id() & (COUNTER_SHARDS - 1);
        self.record_frequency_reuse_shard(shard, hits, misses);
    }

    fn record_frequency_reuse_shard(&self, shard: usize, hits: u64, misses: u64) {
        let Some(shards) = &self.frequency_reuse_shards else {
            return;
        };
        let lookups = hits.saturating_add(misses);
        if lookups == 0 {
            return;
        }
        let sample_lookups = lookups.min(FREQUENCY_REUSE_WINDOW);
        let sample_hits = if lookups <= FREQUENCY_REUSE_WINDOW {
            hits.min(lookups)
        } else {
            u64::try_from(
                u128::from(hits.min(lookups)) * u128::from(sample_lookups) / u128::from(lookups),
            )
            .unwrap_or(sample_lookups)
        };
        let previous = shards[shard & (COUNTER_SHARDS - 1)].pending.fetch_add(
            pack_frequency_reuse(sample_hits, sample_lookups),
            Ordering::Relaxed,
        );
        let previous_lookups = unpack_frequency_reuse(previous).1;
        if previous_lookups / FREQUENCY_REUSE_PUBLISH_INTERVAL
            != previous_lookups.saturating_add(sample_lookups) / FREQUENCY_REUSE_PUBLISH_INTERVAL
        {
            self.publish_frequency_reuse();
        }
    }

    fn publish_frequency_reuse(&self) {
        let Some(shards) = &self.frequency_reuse_shards else {
            return;
        };
        if self
            .frequency_reuse_publishing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let (hits, lookups) = shards.iter().fold((0_u64, 0_u64), |totals, shard| {
            let (hits, lookups) = unpack_frequency_reuse(shard.pending.swap(0, Ordering::Relaxed));
            (
                totals.0.saturating_add(hits),
                totals.1.saturating_add(lookups),
            )
        });
        if lookups != 0 {
            self.merge_frequency_reuse(hits, lookups);
        }
        self.frequency_reuse_publishing
            .store(false, Ordering::Release);
    }

    fn merge_frequency_reuse(&self, hits: u64, lookups: u64) {
        debug_assert!(hits <= lookups);
        let sample_lookups = lookups.min(FREQUENCY_REUSE_WINDOW);
        let sample_hits = if lookups <= FREQUENCY_REUSE_WINDOW {
            hits
        } else {
            u64::try_from(u128::from(hits) * u128::from(sample_lookups) / u128::from(lookups))
                .unwrap_or(sample_lookups)
        };
        let mut previous = self.frequency_reuse.load(Ordering::Relaxed);
        loop {
            let (mut previous_hits, mut previous_lookups) = unpack_frequency_reuse(previous);
            while previous_lookups.saturating_add(sample_lookups) > FREQUENCY_REUSE_WINDOW
                && previous_lookups != 0
            {
                previous_hits /= 2;
                previous_lookups /= 2;
            }
            let replacement = pack_frequency_reuse(
                previous_hits.saturating_add(sample_hits),
                previous_lookups.saturating_add(sample_lookups),
            );
            match self.frequency_reuse.compare_exchange_weak(
                previous,
                replacement,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => {
                    #[cfg(feature = "cache-production-diagnostics")]
                    self.diagnostics
                        .rolling_estimator_cas_retries
                        .fetch_add(1, Ordering::Relaxed);
                    previous = current;
                }
            }
        }
    }

    fn validate_weight(&self, key: &[u8], weight: u64) -> Result<u32, CacheInsertError> {
        if weight > self.config.max_weight {
            #[cfg(feature = "cache-production-diagnostics")]
            self.diagnostics
                .rejected_item_too_heavy
                .fetch_add(1, Ordering::Relaxed);
            self.counters_for(key)
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(CacheInsertError::ItemTooHeavy {
                weight,
                maximum: self.config.max_weight,
            });
        }
        direct_compact_weight(weight).ok_or_else(|| {
            #[cfg(feature = "cache-production-diagnostics")]
            self.diagnostics
                .rejected_weight_not_compact
                .fetch_add(1, Ordering::Relaxed);
            self.counters_for(key)
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            CacheInsertError::WeightNotCompact { weight }
        })
    }

    fn reserve_publish_capacity(&self, weight: u32) -> DirectHardCapacityReservation<'_> {
        self.reserve_capacity(1, u64::from(weight), None, None)
    }

    fn reserve_admission_capacity(
        &self,
        key: &[u8],
        weight: u32,
    ) -> Option<DirectHardCapacityReservation<'_>> {
        if let Some(reservation) = self.try_reserve_publish_capacity(weight) {
            return Some(reservation);
        }
        // The presence read is itself a valid conditional-admission
        // linearization point. Avoid evicting another resident merely to
        // rediscover this key during publication at the hard ceiling.
        if self.index.get_protected(key).is_some() {
            return None;
        }
        Some(self.reserve_publish_capacity(weight))
    }

    fn try_reserve_publish_capacity(
        &self,
        weight: u32,
    ) -> Option<DirectHardCapacityReservation<'_>> {
        self.try_reserve_capacity(1, u64::from(weight))
    }

    fn reserve_replacement_capacity(
        &self,
        key: &[u8],
        previous_weight: u64,
        replacement_weight: u64,
        protected: DirectHandle,
    ) -> DirectHardCapacityReservation<'_> {
        self.reserve_capacity(
            0,
            replacement_weight.saturating_sub(previous_weight),
            Some(protected),
            Some(key),
        )
    }

    fn try_reserve_capacity(
        &self,
        entries: u64,
        weight: u64,
    ) -> Option<DirectHardCapacityReservation<'_>> {
        let Some(capacity) = &self.hard_capacity else {
            return Some(DirectHardCapacityReservation {
                capacity: None,
                entries: 0,
                weight: 0,
            });
        };
        capacity
            .try_reserve(entries, weight)
            .map(|(entries, weight)| DirectHardCapacityReservation {
                capacity: Some(capacity),
                entries,
                weight,
            })
    }

    fn reserve_capacity(
        &self,
        entries: u64,
        weight: u64,
        protected: Option<DirectHandle>,
        protected_key: Option<&[u8]>,
    ) -> DirectHardCapacityReservation<'_> {
        if let Some(reservation) = self.try_reserve_capacity(entries, weight) {
            return reservation;
        }
        #[cfg(feature = "cache-pressure-timing")]
        let started = Instant::now();
        #[cfg(all(
            feature = "cache-production-diagnostics",
            not(feature = "cache-pressure-timing")
        ))]
        self.diagnostics
            .foreground_capacity_enforcements
            .fetch_add(1, Ordering::Relaxed);
        let mut spins = 0_u32;
        loop {
            if let Some(reservation) = self.try_reserve_capacity(entries, weight) {
                #[cfg(feature = "cache-pressure-timing")]
                self.diagnostics
                    .record_foreground_capacity_enforcement(started);
                return reservation;
            }

            let _reclaim_deferral = self.arena.defer_reclamation();

            if let Some(_maintenance) = self.maintenance_gate.try_lock() {
                if let Some(reservation) = self.try_reserve_capacity(entries, weight) {
                    #[cfg(feature = "cache-pressure-timing")]
                    self.diagnostics
                        .record_foreground_capacity_enforcement(started);
                    return reservation;
                }
                let removal_target = self.hard_capacity_reservation_target(entries, weight);
                let removed = self.remove_victim_batch(
                    self.now(),
                    false,
                    protected,
                    protected_key,
                    Some(removal_target),
                    true,
                );
                spins = 0;
                if removed == 0 {
                    thread::yield_now();
                }
            } else if spins < 64 {
                spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
            }
        }
    }

    fn hard_capacity_reservation_target(
        &self,
        incoming_entries: u64,
        incoming_weight: u64,
    ) -> usize {
        let Some(capacity) = &self.hard_capacity else {
            return 1;
        };
        let entry_target = if capacity.max_entries == u64::MAX || incoming_entries == 0 {
            0
        } else {
            capacity
                .entries
                .load(Ordering::Relaxed)
                .saturating_add(incoming_entries)
                .saturating_sub(capacity.max_entries)
                .saturating_add(self.config.max_entries.map_or(0, |maximum| {
                    u64::try_from(maximum.div_ceil(256).max(1)).unwrap_or(u64::MAX)
                }))
        };
        let weight_deficit = if capacity.max_weight == u64::MAX {
            0
        } else {
            capacity
                .weight
                .load(Ordering::Relaxed)
                .saturating_add(incoming_weight)
                .saturating_sub(capacity.max_weight)
        };
        let weight_target = if self.config.max_weight == u64::MAX {
            0
        } else {
            let average_weight = self
                .weight()
                .div_ceil(u64::try_from(self.len().max(1)).unwrap_or(u64::MAX))
                .max(1);
            weight_deficit
                .saturating_add(self.config.max_weight.div_ceil(256).max(1))
                .div_ceil(average_weight)
        };
        usize::try_from(entry_target.max(weight_target))
            .unwrap_or(usize::MAX)
            .clamp(1, self.config.eviction_batch)
    }

    #[allow(
        unsafe_code,
        reason = "the exact expected index handle remains protected by the caller's arena pin"
    )]
    fn publish_replacement_at_hard_capacity<'cache>(
        &'cache self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        replacement: DirectHandle,
        replacement_weight: u64,
    ) -> Option<(DirectHandle, u64, DirectHardCapacityReservation<'cache>)> {
        loop {
            let raw = self.index.get_protected(key)?;
            let previous = published_direct_handle_from_index(raw);
            // SAFETY: the exact index handle and the caller's pin belong to this cache.
            let previous_weight = unsafe { protected_direct_entry(pin, previous) }.weight();
            let hard_capacity = self.reserve_replacement_capacity(
                key,
                previous_weight,
                replacement_weight,
                previous,
            );
            let published = self.index.update(key, |current| {
                if *current == raw {
                    replacement.index_value()
                } else {
                    *current
                }
            });
            if published == Some(replacement.index_value()) {
                return Some((previous, previous_weight, hard_capacity));
            }
        }
    }

    fn enroll_probation(&self, key: &[u8], handle: DirectHandle) {
        if let Some(probation) = &self.probation {
            probation.enroll(self.index.route_hash(key), key, handle);
        }
    }

    fn add_capacity(&self, key: &[u8], weight: u32) {
        #[cfg(feature = "cache-production-diagnostics")]
        self.diagnostics.add_capacity(
            u64::from(weight),
            self.config.max_entries,
            self.config.max_weight,
        );
        let counters = self.capacity_for(key);
        if let Some(share) = self.entry_fair_share() {
            let previous_entries = counters.entries.fetch_add(1, Ordering::Relaxed);
            if previous_entries == share {
                self.entry_pressure.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(share) = self.weight_fair_share() {
            let weight = u64::from(weight);
            let previous_weight = counters.weight.fetch_add(weight, Ordering::Relaxed);
            let after_weight = previous_weight.wrapping_add(weight);
            Self::update_capacity_pressure(
                &self.weight_pressure,
                previous_weight,
                after_weight,
                share,
            );
        }
    }

    fn remove_capacity(&self, key: &[u8], weight: u64) {
        let shard = key_counter_hash(key) & (COUNTER_SHARDS - 1);
        self.remove_capacity_from_shard(shard, weight);
    }

    fn remove_capacity_from_shard(&self, shard: usize, weight: u64) {
        #[cfg(feature = "cache-production-diagnostics")]
        {
            // Debit diagnostics before releasing the hard reservation. A
            // concurrent admission can reuse that reservation immediately;
            // publishing its diagnostic increment before this removal would
            // report a false peak even though the hard bound held.
            self.diagnostics.remove_capacity(weight);
        }
        if let Some(capacity) = &self.hard_capacity {
            capacity.release(
                u64::from(capacity.max_entries != u64::MAX),
                if capacity.max_weight == u64::MAX {
                    0
                } else {
                    weight
                },
            );
        }
        let counters = &self.capacity[shard];
        if let Some(share) = self.entry_fair_share() {
            let previous_entries = counters.entries.fetch_sub(1, Ordering::Relaxed);
            if previous_entries == share + 1 {
                self.entry_pressure.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if let Some(share) = self.weight_fair_share() {
            let previous_weight = counters.weight.fetch_sub(weight, Ordering::Relaxed);
            let after_weight = previous_weight.wrapping_sub(weight);
            Self::update_capacity_pressure(
                &self.weight_pressure,
                previous_weight,
                after_weight,
                share,
            );
        }
    }

    fn replace_capacity(&self, key: &[u8], previous: u64, replacement: u64) {
        #[cfg(feature = "cache-production-diagnostics")]
        self.diagnostics
            .replace_capacity(previous, replacement, &self.config);
        let Some(share) = self.weight_fair_share() else {
            return;
        };
        if previous == replacement {
            return;
        }
        let total = &self.capacity_for(key).weight;
        let (before, after) = if replacement >= previous {
            let delta = replacement - previous;
            let before = total.fetch_add(delta, Ordering::Relaxed);
            (before, before.wrapping_add(delta))
        } else {
            let delta = previous - replacement;
            let before = total.fetch_sub(delta, Ordering::Relaxed);
            (before, before.wrapping_sub(delta))
        };
        Self::update_capacity_pressure(&self.weight_pressure, before, after, share);
    }

    #[inline]
    fn update_capacity_pressure(pressure: &AtomicU64, before: u64, after: u64, share: u64) {
        // Publication and capacity bookkeeping are independent. A removal can
        // therefore be observed before its matching insertion, briefly
        // wrapping a shard total. Apply the transition to the same modulo-
        // u64 before/after states so inverse deltas telescope without
        // saturating or hiding an accounting error.
        if before <= share && after > share {
            pressure.fetch_add(1, Ordering::Relaxed);
        } else if before > share && after <= share {
            pressure.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn enforce_limits(&self, protected: DirectHandle) {
        if !self.over_limit() {
            return;
        }
        self.enforce_limits_after_pressure(protected, None);
    }

    fn enforce_batched_limits(&self, protected: DirectHandle) {
        if !self.over_limit() {
            return;
        }
        let removal_target = self.batched_eviction_target();
        self.enforce_limits_after_pressure(protected, removal_target);
    }

    fn enforce_limits_after_pressure(
        &self,
        protected: DirectHandle,
        removal_target: Option<usize>,
    ) {
        let async_hard_limit = self.config.async_hard_limit_bps.is_some();
        if async_hard_limit {
            if !self.maintenance_pending.swap(true, Ordering::AcqRel)
                && let Some(worker) = self.maintenance_worker.load_full()
            {
                #[cfg(feature = "cache-production-diagnostics")]
                self.diagnostics
                    .maintenance_worker_wakeups
                    .fetch_add(1, Ordering::Relaxed);
                worker.unpark();
            }
            if !self.over_hard_limit() {
                return;
            }
        }
        #[cfg(feature = "cache-pressure-timing")]
        let started = Instant::now();
        if async_hard_limit {
            let mut spins = 0_u32;
            while self.over_hard_limit() {
                let _reclaim_deferral = self.arena.defer_reclamation();
                if let Some(_maintenance) = self.maintenance_gate.try_lock() {
                    if self.over_hard_limit() {
                        self.evict_to_hard_limit_locked(self.now(), Some(protected));
                    }
                    break;
                }
                if spins < 64 {
                    spin_loop();
                    spins += 1;
                } else {
                    thread::yield_now();
                }
            }
        } else {
            // Writers at the limit evict concurrently rather than queueing on
            // the capacity gate: every removal is an exact conditional index
            // transfer under the key's mutation stripe, so racing evictors can
            // only overshoot the limit by their in-flight batches.
            let _reclaim_deferral = self.arena.defer_reclamation();
            self.evict_to_limits(self.now(), Some(protected), removal_target);
        }
        #[cfg(feature = "cache-pressure-timing")]
        self.diagnostics
            .record_foreground_capacity_enforcement(started);
        #[cfg(all(
            feature = "cache-production-diagnostics",
            not(feature = "cache-pressure-timing")
        ))]
        self.diagnostics
            .foreground_capacity_enforcements
            .fetch_add(1, Ordering::Relaxed);
    }

    fn drain_capacity(&self) -> usize {
        if !self.over_limit() {
            return 0;
        }
        #[cfg(feature = "cache-production-diagnostics")]
        {
            let entries = self.len();
            let weight = self.weight();
            let entry_debt = self
                .config
                .max_entries
                .map_or(0, |maximum| entries.saturating_sub(maximum));
            let weight_debt = if self.config.max_weight == u64::MAX {
                0
            } else {
                weight.saturating_sub(self.config.max_weight)
            };
            self.diagnostics.background_entry_debt_total.fetch_add(
                u64::try_from(entry_debt).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.diagnostics
                .background_weight_debt_total
                .fetch_add(weight_debt, Ordering::Relaxed);
        }
        #[cfg(feature = "cache-pressure-timing")]
        let started = Instant::now();
        let _reclaim_deferral = self.arena.defer_reclamation();
        let _maintenance = self.maintenance_gate.lock();
        let removed = self.evict_to_limits(self.now(), None, None);
        #[cfg(feature = "cache-pressure-timing")]
        self.diagnostics.record_background_capacity_drain(started);
        #[cfg(all(
            feature = "cache-production-diagnostics",
            not(feature = "cache-pressure-timing")
        ))]
        self.diagnostics
            .background_capacity_drains
            .fetch_add(1, Ordering::Relaxed);
        removed
    }

    fn evict_to_hard_limit_locked(&self, now: u64, protected: Option<DirectHandle>) -> usize {
        let mut removed = 0;
        while self.over_hard_limit() {
            let removal_target = self.hard_limit_eviction_target();
            let batch =
                self.remove_victim_batch(now, false, protected, None, Some(removal_target), false);
            if batch == 0 {
                break;
            }
            removed += batch;
        }
        removed
    }

    fn rebuild_capacity_pressure(&self) -> Result<bool, FrozenBuildError> {
        let maximum_bps = self.config.rebuild_policy.max_slot_utilization_bps;
        if !self.index.adaptive_capacity_pressure(maximum_bps) {
            return Ok(false);
        }
        // Do not hold `maintenance_gate` while rebuild closes read batches.
        // A writer at the async hard limit may need that gate to evict before
        // it reaches its next guard refresh; holding both would deadlock the
        // rebuild waiting for the writer's old read-batch reservation.
        let rebuilt = self
            .index
            .try_rebuild_adaptive_if_needed(self.config.rebuild_policy)?
            .is_some();
        if rebuilt {
            self.maintenance_counters()
                .rebuilds
                .fetch_add(1, Ordering::Relaxed);
            self.arena.reclaim_retired();
        }
        Ok(rebuilt)
    }

    fn purge_expired_locked(&self, now: u64) -> usize {
        let mut removed = 0;
        for _ in 0..MAX_EXPIRATION_BATCHES {
            let batch = self.remove_victim_batch(now, true, None, None, None, false);
            removed += batch;
            if batch < self.config.eviction_batch {
                break;
            }
        }
        removed
    }

    /// Evicts until the cache is within its limits. Callers may run this
    /// concurrently with each other and with maintenance.
    fn evict_to_limits(
        &self,
        now: u64,
        protected: Option<DirectHandle>,
        first_removal_target: Option<usize>,
    ) -> usize {
        let mut removed = 0;
        let mut removal_target = first_removal_target;
        while self.over_limit() {
            let batch =
                self.remove_victim_batch(now, false, protected, None, removal_target.take(), false);
            if batch == 0 {
                break;
            }
            removed += batch;
        }
        removed
    }

    #[allow(
        clippy::too_many_lines,
        unsafe_code,
        reason = "keeps the probation, reservoir, and sweep stages in eviction order; victim validation pairs this cache's exact index handles with its arena pin"
    )]
    fn remove_victim_batch(
        &self,
        now: u64,
        expired_only: bool,
        protected: Option<DirectHandle>,
        protected_key: Option<&[u8]>,
        requested_removal_target: Option<usize>,
        force: bool,
    ) -> usize {
        let pin = self.arena.pin();
        let removal_target = if expired_only {
            self.config.eviction_batch
        } else {
            requested_removal_target.unwrap_or_else(|| self.proactive_eviction_target())
        };
        let mut victim_sample_window =
            self.victim_sample_window(expired_only, requested_removal_target.is_none());
        if !expired_only {
            victim_sample_window = victim_sample_window
                .max(removal_target.saturating_mul(DIRECT_VICTIM_CANDIDATES_PER_REMOVAL));
        }
        if !expired_only
            && protected_key.is_none()
            && let Some(probation) = &self.probation
        {
            // Unread probationary keys go first; when probation is within its
            // share, eviction falls through to the reservoir and sweep.
            let removed =
                self.remove_probation_victims(probation, &pin, now, protected, removal_target);
            if removed != 0 || (!self.over_limit() && !force) {
                return removed;
            }
        }
        if !expired_only {
            // Reservoir entries survive across batches. The rare logical-key
            // protection path skips them, keeping ordinary reservoir eviction
            // free of a per-victim key comparison.
            let reservoir_result = if protected_key.is_some() {
                (0, 0)
            } else {
                self.remove_reservoir_victims(&pin, now, protected, removal_target)
            };
            let removed = reservoir_result.0;
            #[cfg(feature = "cache-production-diagnostics")]
            self.diagnostics
                .record_victim_batch(reservoir_result.1, removed);
            if removed != 0 || (!self.over_limit() && !force) {
                return removed;
            }
        }
        #[cfg(feature = "cache-pressure-timing")]
        let collection_started = Instant::now();
        let mut batch = self.collect_victim_candidates(
            &pin,
            now,
            expired_only,
            protected,
            victim_sample_window,
        );
        if !expired_only {
            // Like a CLOCK hand, keep sweeping past read entries instead of
            // evicting them. Each round takes one access mark from every
            // sampled entry, so even a fully read cache converges; the cap
            // bounds one batch's latency.
            for _ in 1..DIRECT_VICTIM_SWEEP_ROUNDS {
                if batch.eligible() >= removal_target {
                    break;
                }
                let round = self.collect_victim_candidates(
                    &pin,
                    now,
                    expired_only,
                    protected,
                    victim_sample_window,
                );
                if round.is_empty() {
                    break;
                }
                batch.absorb(round);
            }
        }
        #[cfg(feature = "cache-pressure-timing")]
        self.diagnostics
            .record_victim_collection(collection_started);
        #[cfg(all(
            feature = "cache-production-diagnostics",
            not(feature = "cache-pressure-timing")
        ))]
        self.diagnostics
            .victim_collections
            .fetch_add(1, Ordering::Relaxed);
        if batch.is_empty() {
            return 0;
        }
        #[cfg(feature = "cache-production-diagnostics")]
        let examined = batch.victims.len();
        batch.victims.sort_unstable_by_key(|victim| {
            (
                u8::from(!victim.expired),
                u8::from(victim.accessed),
                victim.frequency,
            )
        });
        let mut removed = 0;
        while batch.next < batch.victims.len() {
            let victim_index = batch.next;
            batch.next += 1;
            let victim = &batch.victims[victim_index];
            let handle = victim.handle;
            let key = batch.key(victim);
            if protected_key.is_some_and(|protected_key| protected_key == key) {
                continue;
            }
            let _reclaim_deferral = self.arena.defer_reclamation();
            let _mutation = self.mutation_stripe(key).lock();
            if self
                .victim_index_remove_if(key, |current| *current == handle.index_value())
                .is_none()
            {
                continue;
            }
            // SAFETY: the removed victim handle stays allocated until `pin` ends.
            let entry = unsafe { protected_direct_entry(&pin, handle) };
            let weight = entry.weight();
            let was_expired = Self::is_expired(entry, now);
            self.remove_capacity(key, weight);
            // SAFETY: exact conditional removal transferred this victim once.
            unsafe { pin.retire(removed_after_exact_index_transfer(handle)) };
            if was_expired {
                self.counters_for(key)
                    .expirations
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.note_victim_frequency(victim.frequency);
                self.counters_for(key)
                    .evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            removed += 1;
            if !expired_only && removed == removal_target {
                batch.append_reservoir_batches(&mut self.victim_reservoir.lock());
                break;
            }
        }
        #[cfg(feature = "cache-production-diagnostics")]
        self.diagnostics.record_victim_batch(examined, removed);
        removed
    }

    fn proactive_eviction_target(&self) -> usize {
        let population = self.len().max(1);
        let entry_target = self
            .config
            .max_entries
            .map_or(1, |capacity| capacity.div_ceil(1_024));
        let weight_target = if self.config.max_weight == u64::MAX {
            1
        } else {
            let current_weight = self.weight();
            let average_weight = current_weight
                .div_ceil(u64::try_from(population).unwrap_or(u64::MAX))
                .max(1);
            let requested_headroom = self.config.max_weight.div_ceil(1_024).max(1);
            let weight_to_remove = current_weight
                .saturating_sub(self.config.max_weight)
                .saturating_add(requested_headroom);
            usize::try_from(weight_to_remove.div_ceil(average_weight)).unwrap_or(usize::MAX)
        };
        entry_target
            .max(weight_target)
            .clamp(1, self.config.eviction_batch)
    }

    fn hard_limit_eviction_target(&self) -> usize {
        let population = self.len().max(1);
        let entry_target = self.config.max_entries.map_or(0, |capacity| {
            self.len()
                .saturating_sub(scale_hard_limit_usize(
                    capacity,
                    self.config.async_hard_limit_bps.unwrap_or(10_000),
                ))
                .saturating_add(capacity.div_ceil(1_024).max(1))
        });
        let weight_target = if self.config.max_weight == u64::MAX {
            0
        } else {
            let hard_weight = scale_hard_limit_u64(
                self.config.max_weight,
                self.config.async_hard_limit_bps.unwrap_or(10_000),
            );
            let current_weight = self.weight();
            let average_weight = current_weight
                .div_ceil(u64::try_from(population).unwrap_or(u64::MAX))
                .max(1);
            usize::try_from(
                current_weight
                    .saturating_sub(hard_weight)
                    .saturating_add(self.config.max_weight.div_ceil(1_024).max(1))
                    .div_ceil(average_weight),
            )
            .unwrap_or(usize::MAX)
        };
        entry_target
            .max(weight_target)
            .clamp(1, self.config.eviction_batch)
    }

    fn batched_eviction_target(&self) -> Option<usize> {
        if self.config.max_weight != u64::MAX {
            return None;
        }
        let capacity = self.config.max_entries?;
        let headroom = self.proactive_eviction_target();
        if headroom == self.config.eviction_batch {
            return None;
        }
        let overage = self.len().saturating_sub(capacity);
        (overage > 1).then(|| {
            headroom
                .saturating_add(overage - 1)
                .min(self.config.eviction_batch)
        })
    }

    fn admission_batch_entry_limit(&self) -> usize {
        self.config.max_entries.map_or(usize::MAX, |capacity| {
            let headroom = capacity
                .div_ceil(1_024)
                .clamp(1, self.config.eviction_batch);
            headroom
                .saturating_mul(2)
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
                .min(self.config.eviction_batch.saturating_mul(2))
                .min(capacity.max(1))
        })
    }

    fn admission_batch_weight_limit(&self) -> u64 {
        if self.config.max_weight == u64::MAX {
            u64::MAX
        } else {
            self.config.max_weight.div_ceil(1_024).max(1)
        }
    }

    #[allow(
        unsafe_code,
        reason = "all sampled index handles and the arena pin belong to this cache"
    )]
    fn collect_victim_candidates(
        &self,
        pin: &DirectArenaPin<'_, V>,
        now: u64,
        expired_only: bool,
        protected: Option<DirectHandle>,
        requested_window: usize,
    ) -> DirectVictimBatch {
        let population = if self.config.max_weight == u64::MAX {
            self.config.max_entries.unwrap_or_else(|| self.len())
        } else {
            self.len()
        }
        .max(1);
        let window = requested_window.min(population);
        let (sampling_window, fallback_population, fallback_dominates) =
            self.victim_sampling_plan(population, window, expired_only);
        let mut victims = DirectVictimBatch::with_capacity(sampling_window);
        let cursor = self
            .victim_scan_cursor
            .fetch_add(sampling_window as u64, Ordering::Relaxed);
        // Redis-style bounded candidate sampling is the normal capacity path.
        // A logical scan remains the fallback when every physical sampler is
        // empty; expiration maintenance scans because it must find deadlines
        // rather than merely any eviction candidate.
        let sample_round = cursor / sampling_window.max(1) as u64;
        if !expired_only {
            let fallback_samples = if fallback_dominates {
                fallback_population
                    .saturating_mul(sampling_window)
                    .div_ceil(population)
                    .min(sampling_window)
            } else if sample_round.is_multiple_of(4) {
                window.min(8)
            } else {
                0
            };
            self.index.sample_fallback_entries(
                cursor.rotate_left(17),
                fallback_samples,
                &mut |key, scanned| {
                    let handle = published_direct_handle_from_index(scanned);
                    // One fallback traversal visits each live Papaya entry at
                    // most once, so checking the growing candidate vector for
                    // duplicate handles only adds quadratic refill work.
                    if Some(handle) == protected {
                        return;
                    }
                    // SAFETY: sampled handle and pin belong to this cache.
                    let entry = unsafe { protected_direct_entry(pin, handle) };
                    victims.push(
                        Self::is_expired(entry, now),
                        entry.take_accessed(),
                        self.victim_frequency(key, expired_only),
                        handle,
                        key,
                    );
                },
            );
            let indexed_samples = sampling_window.saturating_sub(victims.len());
            for offset in 0..indexed_samples {
                let mut collect = |key: &[u8], scanned| {
                    let handle = published_direct_handle_from_index(scanned);
                    if Some(handle) == protected {
                        return;
                    }
                    // SAFETY: sampled handle and pin belong to this cache.
                    let entry = unsafe { protected_direct_entry(pin, handle) };
                    victims.push(
                        Self::is_expired(entry, now),
                        entry.take_accessed(),
                        self.victim_frequency(key, expired_only),
                        handle,
                        key,
                    );
                };
                let overlay_turn = offset % 8 == 7;
                let sampled = if overlay_turn {
                    let seed = cursor / 8 + (offset / 8) as u64;
                    self.index.sample_atomic_entry(seed, &mut collect)
                } else {
                    let base_start = cursor - cursor / 8;
                    let seed = base_start + (offset - offset / 8) as u64;
                    self.index.sample_base_entry(seed, &mut collect)
                };
                if !sampled {
                    let seed = cursor.wrapping_add(offset as u64).rotate_left(23);
                    if overlay_turn {
                        self.index.sample_base_entry(seed, &mut collect);
                    } else {
                        self.index.sample_atomic_entry(seed, &mut collect);
                    }
                }
            }
        }
        if !victims.is_empty() {
            victims.merge_repeated_samples();
            return victims;
        }

        self.scan_victim_candidates(
            pin,
            now,
            expired_only,
            protected,
            population,
            window,
            cursor,
            &mut victims,
        );
        victims
    }

    fn victim_frequency(&self, key: &[u8], expired_only: bool) -> u8 {
        if expired_only {
            return 0;
        }
        self.admission_doorkeeper.as_ref().map_or(0, |doorkeeper| {
            doorkeeper
                .victim_frequency_hashed(doorkeeper_hashes_from_route(self.index.route_hash(key)))
        })
    }

    fn note_victim_frequency(&self, frequency: u8) {
        if let Some(doorkeeper) = &self.admission_doorkeeper {
            doorkeeper.note_victim_frequency(frequency, self.frequency_victim_gate());
        }
    }

    fn note_frequency_access(&self, key: &[u8]) {
        if let Some(doorkeeper) = &self.admission_doorkeeper
            && doorkeeper.tracks_resident_frequency()
        {
            doorkeeper.note_access_hashed(doorkeeper_hashes_from_route(self.index.route_hash(key)));
        }
    }

    fn access_sample_mask(&self, weight: u64) -> u64 {
        if weight >= HEAVY_ACCESS_WEIGHT
            && self
                .admission_doorkeeper
                .as_ref()
                .is_some_and(DirectAdmissionDoorkeeper::tracks_resident_frequency)
        {
            HEAVY_ACCESS_SAMPLE_MASK
        } else {
            ACCESS_SAMPLE_MASK
        }
    }

    fn victim_sample_window(&self, expired_only: bool, background: bool) -> usize {
        let configured = self.config.eviction_batch;
        if expired_only
            || !background
            || self.config.async_hard_limit_bps.is_none()
            || self.config.max_weight == u64::MAX
            || configured >= DIRECT_ASYNC_VICTIM_SAMPLE_MAX
        {
            return configured;
        }
        configured
            .saturating_mul(4)
            .min(DIRECT_ASYNC_VICTIM_SAMPLE_MAX)
    }

    fn frequency_protects_resident(&self, frequency: u8) -> bool {
        self.admission_doorkeeper
            .as_ref()
            .is_some_and(|doorkeeper| doorkeeper.protects_resident(frequency))
    }

    fn victim_sampling_plan(
        &self,
        population: usize,
        window: usize,
        expired_only: bool,
    ) -> (usize, usize, bool) {
        if expired_only {
            return (window, 0, false);
        }
        let fallback_population = self.index.fallback_len().min(population);
        let fallback_dominates = fallback_population >= population.div_ceil(2);
        let sampling_window = if fallback_dominates {
            window
                .saturating_mul(DIRECT_FALLBACK_REFILL_MULTIPLIER)
                .min(population)
                .min(window.max(4_096))
        } else {
            window
        };
        (sampling_window, fallback_population, fallback_dominates)
    }

    #[allow(
        clippy::too_many_arguments,
        unsafe_code,
        reason = "all scanned index handles and the arena pin belong to this cache"
    )]
    fn scan_victim_candidates(
        &self,
        pin: &DirectArenaPin<'_, V>,
        now: u64,
        expired_only: bool,
        protected: Option<DirectHandle>,
        population: usize,
        window: usize,
        cursor: u64,
        victims: &mut DirectVictimBatch,
    ) {
        let start = usize::try_from(cursor % population as u64)
            .expect("victim cursor is below the cache population");
        let mut ordinal = 0_usize;
        self.index.scan_entries(&mut |key, scanned| {
            let handle = published_direct_handle_from_index(scanned);
            if Some(handle) == protected {
                return;
            }
            // SAFETY: the scanned handle and pin belong to this cache.
            let entry = unsafe { protected_direct_entry(pin, handle) };
            let expired = Self::is_expired(entry, now);
            let selected = if expired_only {
                expired && victims.len() < window
            } else {
                let distance = if ordinal >= start {
                    ordinal - start
                } else {
                    population - start + ordinal
                };
                ordinal += 1;
                distance < window
            };
            if selected {
                victims.push(
                    expired,
                    entry.take_accessed(),
                    self.victim_frequency(key, expired_only),
                    handle,
                    key,
                );
            }
        });
    }

    /// Removes `key` if it still publishes an unprotected entry (the
    /// `expected` one, when given) that is expired, or unread and not
    /// protected by frequency. Validation and removal are one conditional
    /// index operation; a read entry loses one access mark and stays.
    #[allow(
        unsafe_code,
        reason = "the predicate dereferences a handle the index publishes under this cache's pin"
    )]
    fn remove_unread_victim(
        &self,
        pin: &DirectArenaPin<'_, V>,
        key: &[u8],
        expected: Option<NonMaxU64>,
        protected: Option<DirectHandle>,
        now: u64,
        protects_resident: bool,
    ) -> DirectVictimRemoval {
        // The predicate may run again after a lost race; the last evaluation
        // describes the value that was, or failed to be, removed.
        let retained = Cell::new(None);
        let expired = Cell::new(false);
        let removed = self.victim_index_remove_if(key, |current| {
            retained.set(None);
            let handle = published_direct_handle_from_index(*current);
            if expected.is_some_and(|expected| *current != expected) || Some(handle) == protected {
                return false;
            }
            // SAFETY: the index publishes this handle while `pin` is held.
            let entry = unsafe { protected_direct_entry(pin, handle) };
            expired.set(Self::is_expired(entry, now));
            if !expired.get() && (entry.accessed() || protects_resident) {
                retained.set(Some(handle));
                return false;
            }
            true
        });
        if let Some(raw) = removed {
            let handle = published_direct_handle_from_index(raw);
            // SAFETY: the removed handle stays allocated until `pin` ends.
            let weight = unsafe { protected_direct_entry(pin, handle) }.weight();
            return DirectVictimRemoval::Removed {
                handle,
                expired: expired.get(),
                weight,
            };
        }
        if let Some(handle) = retained.get() {
            // SAFETY: the index published this handle under `pin` above.
            unsafe { protected_direct_entry(pin, handle) }.take_accessed();
        }
        DirectVictimRemoval::Kept
    }

    #[allow(
        unsafe_code,
        reason = "probation validation pairs this cache's exact index handles with its arena pin"
    )]
    fn remove_probation_victims(
        &self,
        probation: &DirectProbation,
        pin: &DirectArenaPin<'_, V>,
        now: u64,
        protected: Option<DirectHandle>,
        removal_target: usize,
    ) -> usize {
        let population = self.len();
        probation.note_population(population);
        let target = probation.target(population);
        let mut removed = 0;
        #[cfg(feature = "cache-production-diagnostics")]
        let mut examined = 0;
        let mut key = Vec::new();
        while removed < removal_target && probation.len() > target {
            let Some(handle) = probation.pop(&mut key) else {
                break;
            };
            #[cfg(feature = "cache-production-diagnostics")]
            {
                examined += 1;
            }
            if Some(handle) == protected {
                continue;
            }
            let frequency = self.victim_frequency(&key, false);
            let _reclaim_deferral = self.arena.defer_reclamation();
            let _mutation = self.mutation_stripe(&key).lock();
            // A removed, replaced, or still-publishing key is no longer
            // probationary; one read during probation keeps it for the sweep.
            let DirectVictimRemoval::Removed {
                expired, weight, ..
            } = self.remove_unread_victim(
                pin,
                &key,
                Some(handle.index_value()),
                protected,
                now,
                self.frequency_protects_resident(frequency),
            )
            else {
                continue;
            };
            self.remove_capacity(&key, weight);
            // SAFETY: exact conditional removal transferred this victim once.
            unsafe { pin.retire(removed_after_exact_index_transfer(handle)) };
            if expired {
                self.counters_for(&key)
                    .expirations
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                probation.ghost.remember(self.index.route_hash(&key));
                self.note_victim_frequency(frequency);
                self.counters_for(&key)
                    .evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            removed += 1;
        }
        #[cfg(feature = "cache-production-diagnostics")]
        self.diagnostics.record_victim_batch(examined, removed);
        removed
    }

    #[allow(
        unsafe_code,
        reason = "reservoir validation pairs this cache's exact index handles with its arena pin"
    )]
    fn remove_reservoir_victims(
        &self,
        pin: &DirectArenaPin<'_, V>,
        now: u64,
        protected: Option<DirectHandle>,
        removal_target: usize,
    ) -> (usize, usize) {
        let mut removed = 0;
        let mut examined = 0;
        let mut reservoir = std::mem::take(&mut *self.victim_reservoir.lock());
        while removed < removal_target {
            while reservoir
                .last()
                .is_some_and(DirectVictimReservoirBatch::exhausted)
            {
                reservoir.pop();
            }
            let Some(batch) = reservoir.last_mut() else {
                break;
            };
            let victim_index = batch.next;
            batch.next += 1;
            examined += 1;
            let key = batch.key(victim_index);
            let frequency = self.victim_frequency(key, false);
            let _reclaim_deferral = self.arena.defer_reclamation();
            let _mutation = self.mutation_stripe(key).lock();
            let DirectVictimRemoval::Removed {
                handle,
                expired,
                weight,
            } = self.remove_unread_victim(
                pin,
                key,
                None,
                protected,
                now,
                self.frequency_protects_resident(frequency),
            )
            else {
                continue;
            };
            self.remove_capacity(key, weight);
            // SAFETY: exact conditional removal transferred this victim once.
            unsafe { pin.retire(removed_after_exact_index_transfer(handle)) };
            if expired {
                self.counters_for(key)
                    .expirations
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.note_victim_frequency(frequency);
                self.counters_for(key)
                    .evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            removed += 1;
        }
        while reservoir
            .last()
            .is_some_and(DirectVictimReservoirBatch::exhausted)
        {
            reservoir.pop();
        }
        self.restore_victim_reservoir(reservoir);
        (removed, examined)
    }

    fn restore_victim_reservoir(&self, mut reservoir: Vec<DirectVictimReservoirBatch>) {
        let mut stored = self.victim_reservoir.lock();
        if stored.is_empty() {
            // Returning the Vec itself preserves the outer allocation. All
            // ordinary reservoir consumers are serialized by maintenance_gate,
            // so this is the normal path; extending the empty replacement Vec
            // instead would allocate and copy the batch headers on every drain.
            *stored = reservoir;
        } else {
            // Preserve any independently supplied hints and their existing
            // stack order rather than relying on the serialization for safety.
            stored.append(&mut reservoir);
        }
    }

    fn over_limit(&self) -> bool {
        (self.weight_pressure.load(Ordering::Relaxed) != 0
            && self.weight() > self.config.max_weight)
            || self.config.max_entries.is_some_and(|maximum| {
                self.entry_pressure.load(Ordering::Relaxed) != 0 && self.len() > maximum
            })
    }

    fn over_hard_limit(&self) -> bool {
        let Some(bps) = self.config.async_hard_limit_bps else {
            return self.over_limit();
        };
        (self.config.max_weight != u64::MAX
            && self.weight() > scale_hard_limit_u64(self.config.max_weight, bps))
            || self
                .config
                .max_entries
                .is_some_and(|maximum| self.len() > scale_hard_limit_usize(maximum, bps))
    }

    fn entry_fair_share(&self) -> Option<u64> {
        self.config
            .max_entries
            .map(|maximum| u64::try_from(maximum).unwrap_or(u64::MAX) / COUNTER_SHARDS as u64)
    }

    fn weight_fair_share(&self) -> Option<u64> {
        (self.config.max_weight != u64::MAX)
            .then_some(self.config.max_weight / COUNTER_SHARDS as u64)
    }

    fn is_expired(entry: &DirectArenaEntry<V>, now: u64) -> bool {
        let expires_at = entry.expires_at();
        expires_at != NEVER_EXPIRES && expires_at <= now
    }

    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos() / EXPIRY_TICK_NANOS)
            .unwrap_or(DIRECT_MAX_EXPIRY_TICK)
            .min(DIRECT_MAX_EXPIRY_TICK)
    }

    fn deadline_at(ttl: Option<Duration>, now: u64) -> u64 {
        ttl.map_or(NEVER_EXPIRES, |ttl| {
            let nanos = ttl.as_nanos();
            let ticks = if nanos == 0 {
                0
            } else {
                // `now` floors elapsed time, so cover the unknown tick phase.
                nanos.div_ceil(EXPIRY_TICK_NANOS).saturating_add(1)
            };
            let ttl = u64::try_from(ticks).unwrap_or(DIRECT_MAX_EXPIRY_TICK);
            now.saturating_add(ttl).min(DIRECT_MAX_EXPIRY_TICK)
        })
    }

    fn deadline(&self, ttl: Option<Duration>) -> u64 {
        self.note_expiration(ttl);
        ttl.map_or(NEVER_EXPIRES, |ttl| {
            Self::deadline_at(Some(ttl), self.now())
        })
    }

    #[inline]
    fn note_expiration(&self, ttl: Option<Duration>) {
        if ttl.is_some() {
            // The marker is deliberately monotonic. A release before index
            // publication lets maintenance safely skip the full expiration
            // scan until the cache has ever accepted a TTL-capable path.
            self.expiration_possible.store(true, Ordering::Release);
        }
    }

    fn counters_for(&self, key: &[u8]) -> &CacheCounters {
        &self.counters[key_counter_hash(key) & (COUNTER_SHARDS - 1)]
    }

    fn capacity_for(&self, key: &[u8]) -> &DirectCapacityCounters {
        &self.capacity[key_counter_hash(key) & (COUNTER_SHARDS - 1)]
    }

    fn maintenance_counters(&self) -> &CacheCounters {
        &self.counters[0]
    }

    /// Conditional index removal of an eviction victim.
    fn victim_index_remove_if(
        &self,
        key: &[u8],
        predicate: impl Fn(&NonMaxU64) -> bool,
    ) -> Option<NonMaxU64> {
        #[cfg(test)]
        DIRECT_VICTIM_INDEX_PROBES.with(|probes| {
            probes.fetch_add(1, Ordering::Relaxed);
        });
        self.index.remove_if(key, predicate)
    }

    fn mutation_stripe(&self, key: &[u8]) -> &Mutex<()> {
        &self.mutation_stripes[key_counter_hash(key) & (MUTATION_STRIPES - 1)]
    }
}

impl<V> Drop for DirectPackedCache<V> {
    #[allow(
        unsafe_code,
        reason = "exclusive cache drop transfers every remaining index handle to its arena"
    )]
    fn drop(&mut self) {
        let pin = self.arena.pin();
        self.index.scan_entries(&mut |_, raw| {
            // SAFETY: exclusive drop makes each scanned handle unreachable and
            // every handle belongs to this cache's arena pin.
            unsafe {
                pin.retire(removed_after_exact_index_transfer(
                    published_direct_handle_from_index(raw),
                ));
            }
        });
    }
}

fn key_partition_hash(key: &[u8]) -> u64 {
    let mut hash = (key.len() as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for &byte in key.iter().take(8) {
        hash = hash.rotate_left(7) ^ u64::from(byte);
    }
    for &byte in key.iter().rev().take(8) {
        hash = hash.rotate_left(11) ^ u64::from(byte);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^ (hash >> 27)
}

const fn doorkeeper_mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key_counter_hash(key: &[u8]) -> usize {
    let first = key.first().copied().unwrap_or(0) as usize;
    let last = key.last().copied().unwrap_or(0) as usize;
    first ^ last.rotate_left(3) ^ key.len().rotate_left(5)
}

fn validate_direct_config(config: &CacheConfig) -> Result<(), CacheConfigError> {
    if config.max_weight == 0 {
        return Err(CacheConfigError::ZeroWeight);
    }
    if config.max_entries == Some(0) {
        return Err(CacheConfigError::ZeroEntries);
    }
    if config.direct_overlay_capacity() == 0 {
        return Err(CacheConfigError::ZeroOverlayCapacity);
    }
    if config.arena_partitions == 0 {
        return Err(CacheConfigError::ZeroArenaPartitions);
    }
    if config.eviction_batch == 0 {
        return Err(CacheConfigError::ZeroEvictionBatch);
    }
    if config.admission_doorkeeper_entries == Some(0) {
        return Err(CacheConfigError::ZeroAdmissionDoorkeeperEntries);
    }
    if config
        .frequency_admission_min_hit_rate_bps
        .is_some_and(|minimum| minimum > 10_000)
        || config
            .frequency_admission_high_reuse
            .is_some_and(|(_, minimum)| minimum > 10_000)
    {
        return Err(CacheConfigError::InvalidFrequencyHitRate);
    }
    if config
        .frequency_admission_high_reuse
        .is_some_and(|(high_gate, high_hit_rate)| {
            high_gate < config.frequency_admission_max_gate.unwrap_or(0)
                || high_hit_rate < config.frequency_admission_min_hit_rate_bps.unwrap_or(0)
        })
    {
        return Err(CacheConfigError::InvalidFrequencyTier);
    }
    if config
        .async_hard_limit_bps
        .is_some_and(|hard_limit| hard_limit <= 10_000)
    {
        return Err(CacheConfigError::InvalidAsyncHardLimit);
    }
    Ok(())
}

fn initialized_direct_capacity(
    config: &CacheConfig,
    entry_shards: &[u64; COUNTER_SHARDS],
    weight_shards: &[u64; COUNTER_SHARDS],
) -> (Box<[DirectCapacityCounters]>, u64, u64) {
    let capacity = (0..COUNTER_SHARDS)
        .map(|shard| DirectCapacityCounters {
            entries: AtomicU64::new(if config.max_entries.is_some() {
                entry_shards[shard]
            } else {
                0
            }),
            weight: AtomicU64::new(if config.max_weight == u64::MAX {
                0
            } else {
                weight_shards[shard]
            }),
        })
        .collect();
    let entry_pressure = config.max_entries.map_or(0, |maximum| {
        let share = u64::try_from(maximum).unwrap_or(u64::MAX) / COUNTER_SHARDS as u64;
        entry_shards
            .iter()
            .filter(|&&entries| entries > share)
            .count() as u64
    });
    let weight_pressure = if config.max_weight == u64::MAX {
        0
    } else {
        let share = config.max_weight / COUNTER_SHARDS as u64;
        weight_shards
            .iter()
            .filter(|&&weight| weight > share)
            .count() as u64
    };
    (capacity, entry_pressure, weight_pressure)
}

/// A direct entry's weight shares its word with an access mark.
fn direct_compact_weight(weight: u64) -> Option<u32> {
    u32::try_from(weight)
        .ok()
        .filter(|weight| *weight <= DIRECT_MAX_WEIGHT)
}

fn compact_initial_weight(
    config: &CacheConfig,
    weight: u64,
    error: &mut Option<CacheBuildError>,
) -> u32 {
    if weight > config.max_weight {
        record_initial_error(
            error,
            CacheBuildError::InitialItem(CacheInsertError::ItemTooHeavy {
                weight,
                maximum: config.max_weight,
            }),
        );
    }
    if let Some(weight) = direct_compact_weight(weight) {
        weight
    } else {
        record_initial_error(
            error,
            CacheBuildError::InitialItem(CacheInsertError::WeightNotCompact { weight }),
        );
        0
    }
}

fn checked_initial_weight(
    config: &CacheConfig,
    current: u64,
    weight: u64,
    error: &mut Option<CacheBuildError>,
) -> u64 {
    let Some(total) = current.checked_add(weight) else {
        record_initial_error(error, CacheBuildError::InitialWeightOverflow);
        return u64::MAX;
    };
    if total > config.max_weight {
        record_initial_error(
            error,
            CacheBuildError::InitialWeight {
                weight: total,
                maximum: config.max_weight,
            },
        );
    }
    total
}

fn record_initial_error(target: &mut Option<CacheBuildError>, error: CacheBuildError) {
    if target.is_none() {
        *target = Some(error);
    }
}

fn scale_hard_limit_u64(limit: u64, bps: u16) -> u64 {
    u64::try_from(
        u128::from(limit)
            .saturating_mul(u128::from(bps))
            .div_ceil(10_000),
    )
    .unwrap_or(u64::MAX)
}

fn scale_hard_limit_usize(limit: usize, bps: u16) -> usize {
    usize::try_from(
        (limit as u128)
            .saturating_mul(u128::from(bps))
            .div_ceil(10_000),
    )
    .unwrap_or(usize::MAX)
}

fn try_add_bounded(counter: &AtomicU64, addition: u64, maximum: u64) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    let success_ordering = hard_capacity_reservation_success_ordering();
    loop {
        let Some(replacement) = current.checked_add(addition) else {
            return false;
        };
        if replacement > maximum {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            replacement,
            success_ordering,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn pack_frequency_reuse(hits: u64, lookups: u64) -> u64 {
    debug_assert!(hits <= lookups);
    let hits = hits.min(u64::from(u32::MAX));
    let lookups = lookups.min(u64::from(u32::MAX));
    hits << 32 | lookups
}

fn unpack_frequency_reuse(state: u64) -> (u64, u64) {
    (state >> 32, state & u64::from(u32::MAX))
}

/// Join handle for a cache maintenance worker.
pub struct CacheMaintenance {
    stop: Arc<AtomicBool>,
    worker: Thread,
    join: Option<JoinHandle<()>>,
    registration: Option<Arc<ArcSwapOption<Thread>>>,
    registered_worker: Option<Arc<Thread>>,
}

impl CacheMaintenance {
    /// Requests shutdown and waits for the worker to finish.
    pub fn shutdown(mut self) {
        self.stop();
        self.join();
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.worker.unpark();
    }

    fn join(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        if let (Some(registration), Some(registered_worker)) =
            (&self.registration, &self.registered_worker)
        {
            let current = registration.load_full();
            if current
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, registered_worker))
            {
                registration.compare_and_swap(&current, None);
            }
        }
    }
}

impl Drop for CacheMaintenance {
    fn drop(&mut self) {
        self.stop();
        self.join();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn one_thread_routes_distinct_key_hits_to_one_shard() {
        let config = CacheConfig::new(u64::MAX).with_overlay_capacity(128);
        let packed = PackedCache::try_new(config.clone()).unwrap();
        let direct = DirectPackedCache::try_new(config).unwrap();
        for key in 0_u64..64 {
            packed
                .insert_with_options(&key.to_le_bytes(), key, 1, None)
                .unwrap();
            direct
                .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                .unwrap();
            assert!(packed.get(&key.to_le_bytes()).is_some());
            assert!(direct.get(&key.to_le_bytes()).is_some());
        }
        for counters in [&packed.counters, &direct.counters] {
            let populated = counters
                .iter()
                .filter(|counter| counter.hits.load(Ordering::Relaxed) != 0)
                .count();
            assert_eq!(
                populated, 1,
                "distinct keys must not select distinct hit shards"
            );
            assert_eq!(
                counters
                    .iter()
                    .map(|counter| counter.hits.load(Ordering::Relaxed))
                    .sum::<u64>(),
                64
            );
        }
    }

    #[test]
    fn entry_only_hard_reservations_do_not_scan_weights() {
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(2_000)
                .with_overlay_capacity(4_096)
                .with_async_eviction(10_100),
        )
        .unwrap();
        for key in 0_u64..2_020 {
            cache
                .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                .unwrap();
        }
        let before = DIRECT_WEIGHT_SCANS.with(|scans| scans.load(Ordering::Relaxed));
        for key in 2_020_u64..2_520 {
            cache
                .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                .unwrap();
        }
        assert_eq!(
            DIRECT_WEIGHT_SCANS.with(|scans| scans.load(Ordering::Relaxed)),
            before
        );
        assert!(cache.len() <= 2_020);
    }

    #[test]
    fn hot_shards_have_128_byte_alignment() {
        assert!(std::mem::align_of::<CacheCounters>() >= 128);
        assert!(std::mem::align_of::<DirectFrequencyReuseShard>() >= 128);
        assert!(std::mem::align_of::<DirectDoorkeeperObservationShard>() >= 128);
        #[cfg(feature = "cache-diagnostics")]
        assert!(std::mem::align_of::<DirectCacheDiagnosticCounters>() >= 128);
        assert!(std::mem::align_of::<DirectCapacityCounters>() >= 128);
    }
    use super::*;

    #[test]
    fn doorkeeper_observation_updates_one_block() {
        let doorkeeper = DirectAdmissionDoorkeeper::new(1_024, None);
        let bit_mask = doorkeeper.filters[0].len() * 64 - 1;
        let key = (0_u64..1_024)
            .map(u64::to_le_bytes)
            .find(|key| {
                let hashes = DirectAdmissionDoorkeeper::hashes(key);
                let first = usize::try_from(hashes[0]).unwrap_or(usize::MAX) & bit_mask;
                let second = usize::try_from(hashes[1]).unwrap_or(usize::MAX) & bit_mask;
                first / 64 != second / 64
            })
            .expect("the test range contains hashes in distinct words");

        assert_eq!(doorkeeper.observe(&key, false), (false, None));

        let current = usize::try_from(doorkeeper.generation.load(Ordering::Relaxed) & 1)
            .expect("one bit fits usize");
        let changed = doorkeeper.filters[current]
            .iter()
            .filter(|word| word.load(Ordering::Relaxed) != 0)
            .count();
        assert_eq!(changed, 1, "one observation must update one atomic word");
        let set_bits = doorkeeper.filters[current]
            .iter()
            .map(|word| word.load(Ordering::Relaxed).count_ones())
            .sum::<u32>();
        assert_eq!(set_bits, 2, "both membership bits share the same word");
        assert_eq!(doorkeeper.observe(&key, false), (true, None));
    }

    #[test]
    fn doorkeeper_observations_publish_in_sharded_batches() {
        let doorkeeper = DirectAdmissionDoorkeeper::new(50_000, None);
        for _ in 0..15 {
            doorkeeper.observe(b"one observation shard", false);
        }
        assert_eq!(
            doorkeeper.observations.load(Ordering::Relaxed),
            0,
            "local observations must avoid the shared rotation counter"
        );

        doorkeeper.observe(b"one observation shard", false);
        assert_eq!(
            doorkeeper.observations.load(Ordering::Relaxed),
            16,
            "one bounded local batch publishes rotation progress"
        );
    }

    #[test]
    fn stale_synchronous_maintenance_request_reuses_a_completed_boundary() {
        let cache =
            DirectPackedCache::<u64>::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(64))
                .unwrap();
        assert_eq!(cache.maintenance_completions.load(Ordering::Relaxed), 0);

        assert_eq!(cache.maintain().unwrap(), CacheMaintenanceResult::default());
        assert_eq!(cache.maintenance_completions.load(Ordering::Relaxed), 1);

        assert_eq!(
            cache.maintain_after_completion(0).unwrap(),
            CacheMaintenanceResult::default()
        );
        assert_eq!(
            cache.maintenance_completions.load(Ordering::Relaxed),
            1,
            "a boundary completed after this request began must satisfy it"
        );

        assert_eq!(cache.maintain().unwrap(), CacheMaintenanceResult::default());
        assert_eq!(cache.maintenance_completions.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn doorkeeper_rotation_bounds_foreground_clear_work() {
        let doorkeeper = DirectAdmissionDoorkeeper::new(1_024, None);
        for word in &doorkeeper.filters[1] {
            word.store(u64::MAX, Ordering::Relaxed);
        }
        doorkeeper.prime_rotation_on_next_observation(b"rotate", doorkeeper.rotate_every);

        assert_eq!(doorkeeper.observe(b"rotate", false), (false, None));

        let cleared = doorkeeper.filters[1]
            .iter()
            .filter(|word| word.load(Ordering::Relaxed) != u64::MAX)
            .count();
        assert!(
            cleared <= DIRECT_DOORKEEPER_CLEAR_WORDS,
            "one admission cleared {cleared} words instead of a bounded group"
        );
    }

    #[test]
    fn doorkeeper_lazy_rotation_keeps_exactly_one_previous_generation() {
        let doorkeeper = DirectAdmissionDoorkeeper::new(1_024, None);
        let recurring = b"recurring";
        let old = b"only-generation-zero";
        assert_eq!(doorkeeper.observe(recurring, false), (false, None));
        assert_eq!(doorkeeper.observe(recurring, false), (true, None));
        assert_eq!(doorkeeper.observe(old, false), (false, None));
        assert_eq!(doorkeeper.observe(old, false), (true, None));

        doorkeeper.prime_rotation_on_next_observation(b"rotate-once", doorkeeper.rotate_every);
        assert_eq!(doorkeeper.observe(b"rotate-once", false), (false, None));
        assert_eq!(
            doorkeeper.observe(recurring, false),
            (true, None),
            "the immediately previous generation must remain visible"
        );

        doorkeeper.prime_rotation_on_next_observation(b"rotate-twice", doorkeeper.rotate_every * 2);
        assert_eq!(doorkeeper.observe(b"rotate-twice", false), (false, None));
        assert_eq!(
            doorkeeper.observe(recurring, false),
            (true, None),
            "membership refreshed in generation one must remain visible"
        );
        assert_eq!(doorkeeper.observe(old, false), (false, None));
    }

    #[test]
    fn concurrent_doorkeeper_rotations_leave_no_group_locked() {
        const THREADS: usize = 8;
        const OBSERVATIONS: usize = 4_096;
        let doorkeeper = Arc::new(DirectAdmissionDoorkeeper::new(64, None));

        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let doorkeeper = Arc::clone(&doorkeeper);
                scope.spawn(move || {
                    for observation in 0..OBSERVATIONS {
                        let key = ((thread as u64) << 32 | observation as u64).to_le_bytes();
                        doorkeeper.observe(&key, false);
                    }
                });
            }
        });

        let expected_rotations = u64::try_from(THREADS * OBSERVATIONS / 64).unwrap();
        let maximum_unpublished = u64::try_from(
            doorkeeper.observations.shards.len()
                * usize::try_from(DIRECT_DOORKEEPER_OBSERVATION_PUBLISH_INTERVAL - 1).unwrap(),
        )
        .unwrap();
        let maximum_rotation_lag = maximum_unpublished.div_ceil(doorkeeper.rotate_every);
        let generation = doorkeeper.generation.load(Ordering::Relaxed);
        assert!(generation <= expected_rotations);
        assert!(generation >= expected_rotations.saturating_sub(maximum_rotation_lag));
        assert!(
            doorkeeper
                .filter_epochs
                .iter()
                .flatten()
                .all(|epoch| { epoch.load(Ordering::Acquire) & DIRECT_DOORKEEPER_GROUP_LOCK == 0 })
        );
    }

    #[test]
    fn resident_frequency_access_starts_only_after_capacity_pressure() {
        let doorkeeper = DirectAdmissionDoorkeeper::new(1_024, Some(8));
        let key = b"resident";
        let hashes = DirectAdmissionDoorkeeper::hashes(key);

        doorkeeper.note_access(key);
        assert_eq!(doorkeeper.frequency.as_ref().unwrap().estimate(hashes), 0);

        doorkeeper.note_victim_frequency(0, 8);
        let aging_observations = doorkeeper
            .frequency
            .as_ref()
            .unwrap()
            .observations
            .load(Ordering::Relaxed);
        doorkeeper.note_access(key);
        assert_eq!(doorkeeper.frequency.as_ref().unwrap().estimate(hashes), 1);
        assert_eq!(
            doorkeeper
                .frequency
                .as_ref()
                .unwrap()
                .observations
                .load(Ordering::Relaxed),
            aging_observations,
            "resident hits must not contend on the global aging clock"
        );
    }

    #[test]
    fn direct_guard_access_sampling_preserves_schedule_across_counter_wrap() {
        let cache = DirectPackedCache::<u64>::try_new(CacheConfig::new(16)).unwrap();
        let guard = cache.pin();
        let sampled = (0..300)
            .filter(|_| guard.sample_access(1))
            .collect::<Vec<_>>();
        let expected = (0..300).filter(|index| index % 16 == 0).collect::<Vec<_>>();

        assert_eq!(sampled, expected);
    }

    #[test]
    fn packed_owned_reads_do_not_share_one_reference_count() {
        let cache = PackedCache::<u64>::try_new(CacheConfig::new(u64::MAX)).unwrap();
        for id in 0..4_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        let baseline = cache.arena.collector_references();

        let got = cache.get(&0_u64.to_le_bytes()).unwrap();
        let peeked = cache.peek(&1_u64.to_le_bytes()).unwrap();
        let removed = cache.remove(&2_u64.to_le_bytes()).unwrap();
        let CacheInsertOutcome::Replaced(replaced) =
            cache.insert(&3_u64.to_le_bytes(), 30).unwrap()
        else {
            panic!("key 3 was resident");
        };
        assert_eq!(cache.arena.collector_references(), baseline);
        assert_eq!((*got, *peeked, *removed, *replaced), (0, 1, 2, 3));
    }

    #[test]
    fn direct_owned_reads_do_not_share_one_reference_count() {
        let cache = DirectPackedCache::<u64>::try_new(CacheConfig::new(u64::MAX)).unwrap();
        for id in 0..3_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        let baseline = cache.arena.owner_references();

        // Every reader thread incremented and decremented this one count on
        // each owned read, so owned reads lost throughput as threads grew.
        let got = cache.get(&0_u64.to_le_bytes()).unwrap();
        let peeked = cache.peek(&1_u64.to_le_bytes()).unwrap();
        let removed = cache.remove(&2_u64.to_le_bytes()).unwrap();
        assert_eq!(cache.arena.owner_references(), baseline);
        assert_eq!((*got, *peeked, *removed), (0, 1, 2));
    }

    #[test]
    fn direct_probation_pops_whole_keys_in_order_across_ring_wraparound() {
        let probation = DirectProbation::new(10_000, 64);
        assert_eq!(probation.shards.len(), 1);
        // Handles are only stored and compared here, never dereferenced.
        let handle = |id: u64| published_direct_handle_from_index(NonMaxU64::new(id).unwrap());
        let key =
            |id: u64| -> Vec<u8> { (0..=id % 37).map(|byte| byte as u8 ^ id as u8).collect() };
        let mut expected = VecDeque::new();
        let mut popped = Vec::new();
        let mut wrapped = false;
        for id in 0..2_000_u64 {
            probation.enroll(0, &key(id), handle(id));
            expected.push_back(id);
            // Past the record limit the oldest record is dropped on enrollment.
            while expected.len() > probation.len() {
                expected.pop_front();
            }
            if id % 3 != 0 {
                wrapped |= !probation.shards[0]
                    .lock()
                    .key_bytes
                    .as_slices()
                    .1
                    .is_empty();
                let id = expected.pop_front().unwrap();
                assert_eq!(probation.pop(&mut popped), Some(handle(id)));
                assert_eq!(popped, key(id));
            }
        }
        assert!(wrapped, "the key ring must wrap for this test to cover it");
        while let Some(id) = expected.pop_front() {
            assert_eq!(probation.pop(&mut popped), Some(handle(id)));
            assert_eq!(popped, key(id));
        }
        assert_eq!(probation.pop(&mut popped), None);
    }

    #[test]
    fn direct_eviction_probes_the_index_about_once_per_victim() {
        let probation = probation_test_cache();
        let sweep_only = DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(1_024)
                .with_probation(0),
        )
        .expect("cache constructs");
        for (name, cache) in [("probation", probation), ("sweep", sweep_only)] {
            for id in 0..1_024_u64 {
                cache
                    .insert_discard_with_options(&id.to_le_bytes(), id, 1, None)
                    .expect("seed admission succeeds");
            }
            let probes =
                || DIRECT_VICTIM_INDEX_PROBES.with(|probes| probes.load(Ordering::Relaxed));
            let probes_before = probes();
            let evictions_before = cache.stats().evictions;
            for id in 1_024..4_096_u64 {
                cache
                    .insert_discard_with_options(&id.to_le_bytes(), id, 1, None)
                    .expect("admission succeeds");
            }
            let evicted = cache.stats().evictions - evictions_before;
            let probed = probes() - probes_before;
            assert!(evicted >= 3_000, "{name}: only {evicted} evictions");
            // Validation and removal share one conditional index operation.
            assert!(
                probed as u64 <= evicted + evicted / 8,
                "{name}: {probed} index probes for {evicted} evictions"
            );
        }
    }

    fn probation_test_cache() -> DirectPackedCache<u64> {
        DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_max_entries(1_024))
            .expect("cache constructs")
    }

    #[test]
    fn direct_one_shot_inserts_do_not_flush_reused_entries() {
        let cache = probation_test_cache();
        // 999 is coprime with the 1-in-16 access sample, so sixteen read
        // rounds mark every reused key exactly once.
        for id in 0..999_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        {
            let guard = cache.pin();
            for _ in 0..16 {
                for id in 0..999_u64 {
                    assert_eq!(guard.get(&id.to_le_bytes()), Some(&id));
                }
            }
        }
        for id in 1_000_000..1_004_096_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }

        let resident = (0..999_u64)
            .filter(|id| cache.peek(&id.to_le_bytes()).is_some())
            .count();
        // The approximate ghost filter lets about 1% of one-shot keys skip
        // probation and displace a reused key; 1,000 runs kept 944 to 983.
        assert!(
            resident >= 900,
            "only {resident} of 999 reused entries survived a one-shot scan"
        );
    }

    #[test]
    fn direct_large_cache_sweep_evicts_unread_entries_before_read_ones() {
        // From 65,536 entries the proactive removal target reaches the
        // default 64-entry victim window, so a sweep that evicts its whole
        // window cannot prefer unread entries at all.
        const CAPACITY: u64 = 131_072;
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(usize::try_from(CAPACITY).unwrap())
                .with_probation(0),
        )
        .unwrap();
        for id in 0..CAPACITY {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        cache.maintain().unwrap();
        {
            let guard = cache.pin();
            for id in (0..CAPACITY).step_by(2) {
                assert_eq!(guard.get(&id.to_le_bytes()), Some(&id));
            }
        }
        for id in CAPACITY..CAPACITY + CAPACITY / 4 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
            if id.is_multiple_of(4_096) {
                cache.maintain().unwrap();
            }
        }

        let read_lost = (0..CAPACITY)
            .step_by(2)
            .filter(|id| cache.peek(&id.to_le_bytes()).is_none())
            .count();
        assert!(
            read_lost * 100 <= usize::try_from(CAPACITY).unwrap() / 2,
            "the sweep evicted {read_lost} read entries while unread ones remained"
        );
    }

    #[test]
    fn direct_default_overlay_follows_the_entry_limit() {
        // An overlay far larger than the cache fills with evicted keys' slots
        // between rebuilds, so sampled eviction finds few live candidates:
        // on the ARC paper's OLTP trace at 1,000 entries the hit rate fell
        // from 35.3% to 26.1%, below exact LRU's 32.8%.
        let overlay_capacity = |config: CacheConfig| {
            DirectPackedCache::<u64>::try_new(config)
                .unwrap()
                .index
                .adaptive_overlay_stats()
                .expect("the direct index is adaptive")
                .capacity
        };
        let limited = CacheConfig::new(u64::MAX).with_max_entries(1_000);
        assert_eq!(overlay_capacity(limited.clone()), 1_000);
        assert_eq!(
            overlay_capacity(limited.with_overlay_capacity(4_096)),
            4_096
        );
        assert_eq!(
            overlay_capacity(CacheConfig::new(u64::MAX).with_max_entries(8)),
            64
        );
        assert_eq!(
            overlay_capacity(CacheConfig::new(u64::MAX).with_max_entries(1 << 20)),
            DEFAULT_OVERLAY_CAPACITY
        );
        assert_eq!(
            overlay_capacity(CacheConfig::new(u64::MAX)),
            DEFAULT_OVERLAY_CAPACITY
        );
    }

    #[test]
    fn direct_sweep_keeps_sampling_past_read_entries() {
        // Nine in ten entries are read, so a fixed window of four candidates
        // per removal finds too few unread ones and evicts read entries. A
        // CLOCK hand instead keeps sweeping until it finds unread entries.
        const CAPACITY: u64 = 131_072;
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(usize::try_from(CAPACITY).unwrap())
                .with_probation(0),
        )
        .unwrap();
        for id in 0..CAPACITY {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        cache.maintain().unwrap();
        {
            let guard = cache.pin();
            for id in (0..CAPACITY).filter(|id| id % 10 != 0) {
                assert_eq!(guard.get(&id.to_le_bytes()), Some(&id));
            }
        }
        for id in CAPACITY..CAPACITY + CAPACITY / 20 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }

        let read_lost = (0..CAPACITY)
            .filter(|id| id % 10 != 0 && cache.peek(&id.to_le_bytes()).is_none())
            .count();
        assert!(
            read_lost * 1_000 <= usize::try_from(CAPACITY).unwrap(),
            "the sweep evicted {read_lost} read entries while unread ones remained"
        );
    }

    #[test]
    fn direct_single_read_during_probation_keeps_a_key_resident() {
        let cache = probation_test_cache();
        for id in 0..1_024_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        let read_once = u64::MAX.to_le_bytes();
        cache
            .insert_discard_with_options(&read_once, 0, 16, None)
            .unwrap();
        {
            let guard = cache.pin();
            // Any read but the guard's first used to fall outside the sample.
            assert_eq!(guard.get(&1_023_u64.to_le_bytes()), Some(&1_023));
            assert_eq!(guard.get(&read_once), Some(&0));
        }
        for id in 10_000..10_300_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }

        assert!(
            cache.peek(&read_once).is_some(),
            "a key read during probation must stay resident"
        );
    }

    #[test]
    fn direct_ghost_remembers_between_three_eighths_and_half_the_capacity() {
        // Remembering a full capacity of evictions sends loop-like keys
        // straight back to the main area: on DS1 at 4,000,000 entries that
        // cost about ten points of hit rate against a half-capacity ghost.
        let capacity = 8_192_u64;
        let probation = DirectProbation::new(DEFAULT_PROBATION_BPS, 8_192);
        let ghost = &probation.ghost;
        let route = |id: u64| doorkeeper_mix64(id ^ 0x2545_f491_4f6c_dd1d);
        for remembered in 1..=2 * capacity {
            ghost.remember(route(remembered - 1));
            if remembered % 512 != 0 || remembered < capacity / 2 {
                continue;
            }
            let recent = (remembered - capacity * 3 / 8..remembered)
                .filter(|id| ghost.contains(route(*id)))
                .count();
            assert_eq!(
                recent,
                usize::try_from(capacity * 3 / 8).unwrap(),
                "the newest three eighths are always remembered (after {remembered})"
            );
            let stale = (0..remembered - capacity / 2)
                .filter(|id| ghost.contains(route(*id)))
                .count();
            // False positives across the four generations add up to about 0.2%.
            assert!(
                stale * 200 <= usize::try_from(remembered - capacity / 2).unwrap(),
                "{stale} keys evicted more than half a capacity ago are still \
                 remembered after {remembered}"
            );
        }
    }

    #[test]
    fn direct_probation_ghost_returns_a_key_to_the_main_area() {
        let cache = probation_test_cache();
        for id in 0..1_024_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        let returning = u64::MAX.to_le_bytes();
        cache
            .insert_discard_with_options(&returning, 0, 16, None)
            .unwrap();
        for id in 10_000..10_300_u64 {
            cache
                .insert_discard_with_options(&id.to_le_bytes(), id, 16, None)
                .unwrap();
        }
        assert!(
            cache.peek(&returning).is_none(),
            "an unread probationary key is evicted within its probation window"
        );

        // Free one slot so the returning insert cannot itself pop a record.
        assert!(cache.remove_discard(&10_299_u64.to_le_bytes()));
        let probation = cache
            .probation
            .as_ref()
            .expect("probation is on by default");
        let enrolled = probation.len();
        cache
            .insert_discard_with_options(&returning, 1, 16, None)
            .unwrap();

        assert_eq!(cache.peek(&returning).map(|value| *value), Some(1));
        assert_eq!(
            probation.len(),
            enrolled,
            "a key remembered by the ghost filter must skip probation"
        );
    }

    #[test]
    fn direct_guard_samples_heavy_values_more_often_only_under_pressure() {
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(16)
                .with_admission_doorkeeper(16)
                .with_frequency_admission(8),
        )
        .unwrap();
        let guard = cache.pin();
        let before_pressure = (0..64).filter(|_| guard.sample_access(4_096)).count();
        cache.note_victim_frequency(0);
        let under_pressure = (0..64).filter(|_| guard.sample_access(4_096)).count();

        assert_eq!(before_pressure, 4);
        assert_eq!(under_pressure, 64);
    }

    #[test]
    fn direct_cloned_read_releases_reclamation_without_borrowing_the_cache() {
        let cache = DirectPackedCache::try_new(CacheConfig::new(64)).expect("cache constructs");
        cache
            .insert_if_absent_with_options(b"key", String::from("value"), 5, None)
            .expect("admission succeeds");

        let cloned = cache.get_cloned(b"key").expect("value is resident");
        assert!(cache.remove_discard(b"key"));
        cache.arena.reclaim_retired();

        assert_eq!(cloned, "value");
        assert!(cache.peek(b"key").is_none());
    }

    #[test]
    fn direct_cloned_guard_keeps_no_arena_epoch_between_reads() {
        let cache = DirectPackedCache::try_new(CacheConfig::new(64)).expect("cache constructs");
        cache
            .insert_if_absent_with_options(b"key", Arc::new(String::from("value")), 5, None)
            .expect("admission succeeds");
        let guard = cache.pin_cloned();

        let cloned = guard.get(b"key").expect("value is resident");
        assert!(cache.remove_discard(b"key"));
        cache.arena.reclaim_retired();

        assert_eq!(&**cloned, "value");
        #[cfg(feature = "cache-diagnostics")]
        assert_eq!(cache.arena.reclamation_snapshot().retired_values, 0);
    }

    #[test]
    fn direct_shareable_guard_automatically_bounds_its_reader_epoch() {
        let cache = DirectPackedCache::try_new(CacheConfig::new(64)).expect("cache constructs");
        cache
            .insert_if_absent_with_options(b"key", Arc::new(String::from("value")), 5, None)
            .expect("admission succeeds");
        let mut guard = cache.pin_shareable(1);

        let cloned = guard.get(b"key").expect("value is resident");
        assert!(cache.remove_discard(b"key"));
        cache.arena.reclaim_retired();

        assert_eq!(&**cloned, "value");
        #[cfg(feature = "cache-diagnostics")]
        assert_eq!(cache.arena.reclamation_snapshot().retired_values, 0);
    }

    #[test]
    fn async_background_eviction_expands_sampling_without_expanding_foreground_work() {
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(1_000_000)
                .with_max_entries(50_000)
                .with_eviction_batch(256)
                .with_async_eviction(11_000),
        )
        .unwrap();

        assert_eq!(cache.victim_sample_window(false, true), 1_024);
        assert_eq!(cache.victim_sample_window(false, false), 256);
        assert_eq!(cache.victim_sample_window(true, true), 256);
    }

    #[test]
    fn accessed_victim_survivors_are_not_saved_in_the_reservoir() {
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(16)
                .with_overlay_capacity(64)
                .with_eviction_batch(8),
        )
        .expect("cache constructs");
        for (key, value) in [(b"hot".as_slice(), 1_u64), (b"cold", 2), (b"expired", 3)] {
            assert_eq!(
                cache
                    .insert_if_absent_with_options(key, value, 1, None)
                    .expect("admission succeeds"),
                CacheAdmissionOutcome::Inserted
            );
        }
        let handle = |key: &[u8]| {
            published_direct_handle_from_index(
                cache
                    .index
                    .get_protected(key)
                    .expect("inserted key is published"),
            )
        };
        let mut victims = DirectVictimBatch::with_capacity(3);
        victims.push(false, true, 0, handle(b"hot"), b"hot");
        victims.push(false, false, 0, handle(b"cold"), b"cold");
        victims.push(true, true, 0, handle(b"expired"), b"expired");

        let mut reservoir = Vec::new();
        victims.append_reservoir_batches(&mut reservoir);
        let keys = reservoir
            .iter()
            .flat_map(|batch| (0..batch.key_ends.len()).map(|index| batch.key(index).to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(keys, [b"cold".to_vec(), b"expired".to_vec()]);
    }

    #[test]
    fn filtered_reservoir_chunks_preserve_victim_order() {
        let cache = DirectPackedCache::try_new(CacheConfig::new(1)).expect("cache constructs");
        cache
            .insert_if_absent_with_options(b"handle", 1_u64, 1, None)
            .expect("admission succeeds");
        let handle = published_direct_handle_from_index(
            cache
                .index
                .get_protected(b"handle")
                .expect("inserted key is published"),
        );
        let mut victims = DirectVictimBatch::with_capacity(140);
        for index in 0_u16..140 {
            victims.push(
                false,
                index.is_multiple_of(3),
                0,
                handle,
                &index.to_le_bytes(),
            );
        }
        victims.next = 5;

        let mut reservoir = Vec::new();
        victims.append_reservoir_batches(&mut reservoir);
        let keys = reservoir
            .iter()
            .rev()
            .flat_map(|batch| (0..batch.key_ends.len()).map(|index| batch.key(index).to_vec()))
            .collect::<Vec<_>>();
        let expected = (5_u16..140)
            .filter(|index| !index.is_multiple_of(3))
            .map(|index| index.to_le_bytes().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(keys, expected);
    }

    #[test]
    fn reservoir_eviction_reuses_the_outer_batch_allocation() {
        let cache = DirectPackedCache::try_new(CacheConfig::new(16)).expect("cache constructs");
        let mut victims = DirectVictimBatch::with_capacity(4);
        for id in 0_u64..4 {
            let key = id.to_le_bytes();
            cache
                .insert_if_absent_with_options(&key, id, 1, None)
                .expect("admission succeeds");
            let handle = published_direct_handle_from_index(
                cache.index.get_protected(&key).expect("key is published"),
            );
            victims.push(false, false, 0, handle, &key);
        }
        let mut reservoir = Vec::with_capacity(32);
        victims.append_reservoir_batches(&mut reservoir);
        let allocation = reservoir.as_ptr();
        let capacity = reservoir.capacity();
        *cache.victim_reservoir.lock() = reservoir;
        let _maintenance = cache.maintenance_gate.lock();
        let pin = cache.arena.pin();

        assert_eq!(
            cache.remove_reservoir_victims(&pin, cache.now(), None, 1),
            (1, 1)
        );
        assert_eq!(cache.len(), 3);
        let reservoir = cache.victim_reservoir.lock();
        assert_eq!(
            reservoir.capacity(),
            capacity,
            "eviction must retain batch capacity"
        );
        assert_eq!(
            reservoir.as_ptr(),
            allocation,
            "eviction must return the same allocation"
        );
        assert_eq!(reservoir[0].next, 1);
        assert_eq!(reservoir[0].key(1), &1_u64.to_le_bytes());
        drop(reservoir);
        assert_eq!(
            cache.remove_reservoir_victims(&pin, cache.now(), None, 3),
            (3, 3)
        );
        let reservoir = cache.victim_reservoir.lock();
        assert!(reservoir.is_empty());
        assert_eq!(reservoir.capacity(), capacity);
        assert_eq!(reservoir.as_ptr(), allocation);
    }

    #[test]
    fn reservoir_restoration_preserves_independently_supplied_hints() {
        let cache =
            DirectPackedCache::<u64>::try_new(CacheConfig::new(16)).expect("cache constructs");
        let batch = |key: &[u8]| DirectVictimReservoirBatch {
            key_ends: vec![key.len()],
            key_bytes: key.to_vec(),
            next: 0,
        };
        cache.victim_reservoir.lock().push(batch(b"newer"));
        cache.restore_victim_reservoir(vec![batch(b"older")]);
        let reservoir = cache.victim_reservoir.lock();
        assert_eq!(reservoir.len(), 2);
        assert_eq!(reservoir[0].key(0), b"newer");
        assert_eq!(reservoir[1].key(0), b"older");
    }

    #[test]
    fn frequency_aging_is_lazy_and_touches_only_observed_counters() {
        let sketch = DirectFrequencySketch::new(32, 8);
        for counter in &sketch.counters {
            counter.store(DirectFrequencySketch::MAX_FREQUENCY, Ordering::Relaxed);
        }
        sketch.observations.store(319, Ordering::Relaxed);

        sketch.observe([64, 65]);

        let changed = sketch
            .counters
            .iter()
            .filter(|counter| {
                counter.load(Ordering::Relaxed) != DirectFrequencySketch::MAX_FREQUENCY
            })
            .count();
        assert_eq!(changed, 2, "the reset boundary must not sweep the sketch");
        assert_eq!(sketch.estimate([0, 1]), 7);
    }

    #[test]
    fn resident_frequency_refreshes_counters_without_global_contention() {
        let sketch = DirectFrequencySketch::new(32, 8);
        let hashes = [64, 65];

        sketch.observe_resident(hashes);

        assert_eq!(sketch.estimate(hashes), 1);
        assert_eq!(sketch.observations.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn lazy_frequency_aging_matches_eager_halving_before_epoch_wrap() {
        let sketch = DirectFrequencySketch::new(32, 8);
        for counter in &sketch.counters {
            counter.store(DirectFrequencySketch::MAX_FREQUENCY, Ordering::Relaxed);
        }
        sketch.victim_gate.store(8, Ordering::Relaxed);

        for reset in 1..=3_u64 {
            sketch
                .observations
                .store(reset * 320 - 1, Ordering::Relaxed);
            sketch.observe([64, 65]);
        }

        assert_eq!(sketch.estimate([0, 1]), 1);
        assert_eq!(sketch.victim_gate.load(Ordering::Relaxed), 1);
        assert_eq!(sketch.counters[0].load(Ordering::Relaxed), 15);
    }

    #[test]
    fn tiered_frequency_gate_keeps_the_base_until_high_reuse() {
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(16)
                .with_max_entries(16)
                .with_overlay_capacity(64)
                .with_admission_doorkeeper(16)
                .with_tiered_frequency_admission(2, 6_000, 4, 7_500),
        )
        .expect("cache constructs");
        cache
            .frequency_reuse
            .store(pack_frequency_reuse(700, 1_024), Ordering::Relaxed);
        assert_eq!(cache.frequency_victim_gate(), 2);

        cache
            .frequency_reuse
            .store(pack_frequency_reuse(800, 1_024), Ordering::Relaxed);
        assert_eq!(cache.frequency_victim_gate(), 4);
    }

    #[test]
    fn adaptive_frequency_gate_follows_recent_reuse_after_an_early_cold_phase() {
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(16)
                .with_max_entries(16)
                .with_overlay_capacity(64)
                .with_admission_doorkeeper(16)
                .with_adaptive_frequency_admission(2, 6_000),
        )
        .expect("cache constructs");

        cache.record_frequency_reuse(0, 100_000);
        assert!(!cache.frequency_admission_active());

        for _ in 0..64 {
            cache.record_frequency_reuse(512, 0);
        }

        assert!(
            cache.frequency_admission_active(),
            "a bounded adaptive estimate must recover from an early cold-fill phase"
        );
    }

    #[test]
    fn adaptive_frequency_gate_turns_off_after_reuse_disappears() {
        let cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(16)
                .with_max_entries(16)
                .with_overlay_capacity(64)
                .with_admission_doorkeeper(16)
                .with_adaptive_frequency_admission(2, 6_000),
        )
        .expect("cache constructs");

        cache.record_frequency_reuse(100_000, 0);
        assert!(cache.frequency_admission_active());

        for _ in 0..64 {
            cache.record_frequency_reuse(0, 512);
        }

        assert!(!cache.frequency_admission_active());
    }

    #[test]
    fn rolling_frequency_reuse_stays_bounded_during_concurrent_updates() {
        let cache = Arc::new(
            DirectPackedCache::<u64>::try_new(
                CacheConfig::new(16)
                    .with_max_entries(16)
                    .with_overlay_capacity(64)
                    .with_admission_doorkeeper(16)
                    .with_adaptive_frequency_admission(2, 5_000),
            )
            .expect("cache constructs"),
        );

        thread::scope(|scope| {
            for thread_index in 0..8 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        if thread_index % 2 == 0 {
                            cache.record_frequency_reuse_shard(thread_index, 512, 0);
                        } else {
                            cache.record_frequency_reuse_shard(thread_index, 0, 512);
                        }
                    }
                });
            }
        });
        cache.publish_frequency_reuse();

        let (hits, lookups) = unpack_frequency_reuse(cache.frequency_reuse.load(Ordering::Relaxed));
        assert!(hits <= lookups);
        assert!(lookups <= FREQUENCY_REUSE_WINDOW);
        assert!(lookups >= FREQUENCY_REUSE_MIN_LOOKUPS);
        #[cfg(feature = "cache-production-diagnostics")]
        assert!(
            cache
                .diagnostics
                .rolling_estimator_cas_retries
                .load(Ordering::Relaxed)
                <= 8,
            "independent writer shards must not contend on the rolling summary"
        );
    }

    #[test]
    fn direct_maintenance_skips_expiration_scan_when_ttl_was_never_used() {
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(1_024)
                .with_overlay_capacity(1_024)
                .with_eviction_batch(32),
        )
        .expect("cache constructs");
        for key in 0_u64..16 {
            assert_eq!(
                cache
                    .insert_if_absent_with_options(&key.to_le_bytes(), key, 1, None)
                    .expect("admission succeeds"),
                CacheAdmissionOutcome::Inserted
            );
        }

        let cursor_before = cache.victim_scan_cursor.load(Ordering::Relaxed);
        let result = cache.maintain().expect("maintenance succeeds");

        assert_eq!(result.expired, 0);
        assert_eq!(
            cache.victim_scan_cursor.load(Ordering::Relaxed),
            cursor_before,
            "a never-expiring cache must not scan for expiration"
        );
    }

    #[test]
    fn direct_maintenance_keeps_expiration_scan_after_ttl_admission() {
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(16)
                .with_overlay_capacity(64)
                .with_eviction_batch(8),
        )
        .expect("cache constructs");
        cache
            .insert_if_absent_with_options(b"expiring", 1_u64, 1, Some(Duration::ZERO))
            .expect("admission succeeds");

        let result = cache.maintain().expect("maintenance succeeds");

        assert_eq!(result.expired, 1);
        assert!(cache.peek(b"expiring").is_none());
    }

    #[test]
    fn direct_guard_pinned_before_first_ttl_admission_observes_expiration() {
        let cache = DirectPackedCache::try_new(
            CacheConfig::new(16)
                .with_overlay_capacity(64)
                .with_eviction_batch(8),
        )
        .expect("cache constructs");
        let guard = cache.pin();

        cache
            .insert_if_absent_with_options(b"expiring", 1_u64, 1, Some(Duration::ZERO))
            .expect("admission succeeds");

        assert!(guard.peek(b"expiring").is_none());
    }

    #[test]
    fn direct_bulk_load_with_ttl_enables_expiration_maintenance() {
        let cache = DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(16)
                .with_overlay_capacity(64)
                .with_eviction_batch(8),
            [(b"expiring", 1_u64, 1, Some(Duration::ZERO))],
        )
        .expect("cache constructs");

        let result = cache.maintain().expect("maintenance succeeds");

        assert_eq!(result.expired, 1);
        assert!(cache.peek(b"expiring").is_none());
    }

    #[test]
    fn contended_async_writer_reserves_before_publication_and_stops_waiting_below_the_hard_limit() {
        const CAPACITY: usize = 128;
        let cache = Arc::new(
            DirectPackedCache::try_new(
                CacheConfig::new(u64::MAX)
                    .with_max_entries(CAPACITY)
                    .with_overlay_capacity(512)
                    .with_async_eviction(11_000),
            )
            .expect("cache constructs"),
        );
        for key in 0_u64..141 {
            cache
                .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                .expect("admission succeeds");
        }
        assert_eq!(cache.len(), 141);

        let maintenance = cache.maintenance_gate.lock();
        let writer_cache = Arc::clone(&cache);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let writer = thread::spawn(move || {
            started_tx.send(()).expect("receiver remains live");
            writer_cache
                .insert_discard_with_options(&141_u64.to_le_bytes(), 141, 1, None)
                .expect("admission succeeds");
            finished_tx.send(()).expect("receiver remains live");
        });
        started_rx.recv().expect("writer starts");
        for _ in 0..1_024 {
            thread::yield_now();
        }
        assert_eq!(
            cache.len(),
            141,
            "writer must reserve capacity before publishing"
        );
        assert!(
            matches!(
                finished_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "writer must wait while the exact hard ceiling is full"
        );

        assert!(cache.remove_discard(&0_u64.to_le_bytes()));
        finished_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("writer may return below the hard ceiling while maintenance owns the gate");
        writer.join().expect("writer remains healthy");
        assert_eq!(cache.len(), 141);
        drop(maintenance);
    }

    #[test]
    fn strict_writers_evict_without_waiting_for_another_evictor() {
        const CAPACITY: usize = 128;
        let cache = Arc::new(
            DirectPackedCache::try_new(
                CacheConfig::new(u64::MAX)
                    .with_max_entries(CAPACITY)
                    .with_overlay_capacity(512),
            )
            .expect("cache constructs"),
        );
        for key in 0..CAPACITY as u64 {
            cache
                .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                .expect("admission succeeds");
        }

        // Another thread holding the capacity gate (maintenance, or a writer
        // already evicting) must not stall every other writer at the limit.
        let maintenance = cache.maintenance_gate.lock();
        let writer_cache = Arc::clone(&cache);
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let writer = thread::spawn(move || {
            for key in CAPACITY as u64..CAPACITY as u64 + 8 {
                writer_cache
                    .insert_discard_with_options(&key.to_le_bytes(), key, 1, None)
                    .expect("admission succeeds");
            }
            finished_tx.send(()).expect("receiver remains live");
        });
        let finished = finished_rx.recv_timeout(Duration::from_secs(5));
        drop(maintenance);
        writer.join().expect("writer remains healthy");
        assert!(
            finished.is_ok(),
            "a writer at the entry limit waited for the capacity gate"
        );
        assert!(cache.len() <= CAPACITY);
    }

    #[test]
    fn full_maintenance_defers_rebuild_while_guarded_writers_hold_reservations() {
        let cache = Arc::new(
            DirectPackedCache::<u64>::try_new(
                CacheConfig::new(u64::MAX)
                    .with_max_entries(64)
                    .with_overlay_capacity(64)
                    .with_rebuild_policy(AdaptiveRebuildPolicy {
                        max_slot_utilization_bps: 1,
                        min_learned_insertions: 1,
                        max_distribution_drift_bps: 1,
                        max_short_key_spill_bps: 1,
                    }),
            )
            .expect("cache constructs"),
        );
        for value in 0..64_u64 {
            cache
                .insert_discard_with_options(&value.to_le_bytes(), value, 1, None)
                .expect("seed admission succeeds");
        }
        assert!(
            cache
                .index
                .adaptive_overlay_stats()
                .expect("adaptive index")
                .recommendation(cache.config.rebuild_policy)
                .is_recommended(),
            "the fixture must force an adaptive rebuild"
        );

        // Guarded admissions retain this exact generation reservation until
        // refresh. Maintenance must defer rebuild without publishing a redirect.
        let mut guard = cache.pin();
        assert_eq!(
            guard
                .insert_if_absent_with_options(&0_u64.to_le_bytes(), 0, 1, None)
                .expect("existing-key admission succeeds"),
            CacheAdmissionOutcome::Existing
        );
        let maintaining = Arc::clone(&cache);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let maintenance = thread::spawn(move || {
            done_tx
                .send(maintaining.maintain())
                .expect("receiver remains live");
        });
        let initial_depth = cache.index.stats().layer_depth;
        let result = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("maintenance defers while the guarded writer remains live")
            .expect("maintenance succeeds");
        maintenance
            .join()
            .expect("maintenance thread remains healthy");
        assert!(!result.rebuilt);
        assert_eq!(cache.index.stats().layer_depth, initial_depth);
        let capacity_gate_available = cache.maintenance_gate.try_lock().is_some();
        let admission = if capacity_gate_available {
            Some(guard.insert_if_absent_with_options(&64_u64.to_le_bytes(), 64, 1, None))
        } else {
            None
        };
        guard.quiesce();
        drop(guard);
        assert!(cache.maintain().expect("deferred rebuild succeeds").rebuilt);
        assert_eq!(cache.index.stats().layer_depth, 1);
        assert!(
            capacity_gate_available,
            "a guarded writer must be able to enforce capacity before releasing its generation reservation"
        );
        assert_eq!(admission, Some(Ok(CacheAdmissionOutcome::Inserted)));
        assert_eq!(cache.len(), 64);
        assert_eq!(cache.pin().get(&64_u64.to_le_bytes()), Some(&64));
    }

    #[test]
    fn replacement_capacity_uses_the_published_delta_after_reordered_bookkeeping() {
        let cache =
            DirectPackedCache::<u64>::try_new(CacheConfig::new(128).with_overlay_capacity(64))
                .expect("cache constructs");
        let key = b"reordered-replacement";
        let counters = cache.capacity_for(key);

        // A newer replacement can publish before an older replacement updates
        // the sharded total. This snapshot is therefore valid while the
        // 120 -> 100 bookkeeping delta is still pending: the aggregate is
        // 100, not the displaced value's 120.
        counters.weight.store(100, Ordering::Relaxed);
        cache.replace_capacity(key, 120, 100);
        assert_eq!(counters.weight.load(Ordering::Relaxed), 80);

        // The delayed inverse delta restores the exact aggregate.
        cache.replace_capacity(key, 100, 120);
        assert_eq!(counters.weight.load(Ordering::Relaxed), 100);

        counters.weight.store(3, Ordering::Relaxed);
        cache.weight_pressure.store(1, Ordering::Relaxed);
        cache.replace_capacity(key, 4, 3);
        assert_eq!(counters.weight.load(Ordering::Relaxed), 2);
        assert_eq!(cache.weight_pressure.load(Ordering::Relaxed), 0);

        counters.weight.store(2, Ordering::Relaxed);
        cache.weight_pressure.store(0, Ordering::Relaxed);
        cache.replace_capacity(key, 4, 5);
        assert_eq!(counters.weight.load(Ordering::Relaxed), 3);
        assert_eq!(cache.weight_pressure.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn weight_pressure_transitions_follow_wrapped_reordered_deltas() {
        let weight_cache =
            DirectPackedCache::<u64>::try_new(CacheConfig::new(128).with_overlay_capacity(64))
                .expect("weight cache constructs");
        let weight_key = b"reordered-weight";
        let weight_shard = key_counter_hash(weight_key) & (COUNTER_SHARDS - 1);

        weight_cache.remove_capacity_from_shard(weight_shard, 100);
        weight_cache.add_capacity(weight_key, 120);
        assert_eq!(
            weight_cache.capacity[weight_shard]
                .weight
                .load(Ordering::Relaxed),
            20
        );
        assert_eq!(weight_cache.weight_pressure.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn reordered_weight_deltas_telescope_across_all_bookkeeping_orders() {
        let operations = [
            (100_u64, 0_u64, 0_u8), // insert 100
            (100, 120, 1),          // replace 100 with 120
            (120, 0, 2),            // remove 120
            (20, 0, 0),             // insert 20
        ];
        let key = b"reordered-permutations";
        let shard = key_counter_hash(key) & (COUNTER_SHARDS - 1);

        for first in 0..4 {
            for second in 0..4 {
                if second == first {
                    continue;
                }
                for third in 0..4 {
                    if third == first || third == second {
                        continue;
                    }
                    let fourth = 6 - first - second - third;
                    let order = [first, second, third, fourth];
                    let cache = DirectPackedCache::<u64>::try_new(
                        CacheConfig::new(128).with_overlay_capacity(64),
                    )
                    .expect("cache constructs");

                    for operation in order {
                        let (weight, replacement, kind) = operations[operation];
                        match kind {
                            0 => cache.add_capacity(key, u32::try_from(weight).unwrap()),
                            1 => cache.replace_capacity(key, weight, replacement),
                            2 => cache.remove_capacity_from_shard(shard, weight),
                            _ => unreachable!("operation kind is bounded by the table"),
                        }
                    }

                    assert_eq!(
                        cache.capacity[shard].weight.load(Ordering::Relaxed),
                        20,
                        "weight total must converge for bookkeeping order {order:?}"
                    );
                    assert_eq!(
                        cache.weight_pressure.load(Ordering::Relaxed),
                        1,
                        "pressure must converge for bookkeeping order {order:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn capacity_observations_wrap_across_shard_sums() {
        let weight_cache =
            DirectPackedCache::<u64>::try_new(CacheConfig::new(128).with_overlay_capacity(64))
                .expect("weight cache constructs");
        weight_cache.capacity[0]
            .weight
            .store(u64::MAX, Ordering::Relaxed);
        weight_cache.capacity[1].weight.store(2, Ordering::Relaxed);
        assert_eq!(weight_cache.weight(), 1);

        let entry_cache = DirectPackedCache::<u64>::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(64)
                .with_overlay_capacity(64),
        )
        .expect("entry cache constructs");
        entry_cache.capacity[0]
            .entries
            .store(u64::MAX, Ordering::Relaxed);
        entry_cache.capacity[1].entries.store(2, Ordering::Relaxed);
        assert_eq!(entry_cache.len(), 1);
    }

    #[cfg(feature = "cache-production-diagnostics")]
    #[test]
    fn hard_capacity_release_acquire_orders_diagnostic_reuse() {
        use loom::sync::Arc;
        use loom::sync::atomic::{AtomicU64, Ordering as LoomOrdering};
        use loom::thread;

        fn run_model(
            diagnostics_before_release: bool,
            release_ordering: LoomOrdering,
            reservation_ordering: LoomOrdering,
        ) {
            let mut builder = loom::model::Builder::new();
            builder.preemption_bound = Some(3);
            builder.check(move || {
                let hard_capacity = Arc::new(AtomicU64::new(1));
                let diagnostics = Arc::new(AtomicU64::new(1));
                let peak = Arc::new(AtomicU64::new(1));

                let remover = {
                    let hard_capacity = Arc::clone(&hard_capacity);
                    let diagnostics = Arc::clone(&diagnostics);
                    thread::spawn(move || {
                        if diagnostics_before_release {
                            diagnostics.fetch_sub(1, LoomOrdering::Relaxed);
                            hard_capacity.fetch_sub(1, release_ordering);
                        } else {
                            hard_capacity.fetch_sub(1, release_ordering);
                            diagnostics.fetch_sub(1, LoomOrdering::Relaxed);
                        }
                    })
                };
                let adder = {
                    let hard_capacity = Arc::clone(&hard_capacity);
                    let diagnostics = Arc::clone(&diagnostics);
                    let peak = Arc::clone(&peak);
                    thread::spawn(move || {
                        let current = hard_capacity.load(LoomOrdering::Relaxed);
                        if current < 1
                            && hard_capacity
                                .compare_exchange(
                                    current,
                                    current + 1,
                                    reservation_ordering,
                                    LoomOrdering::Relaxed,
                                )
                                .is_ok()
                        {
                            let observed = diagnostics
                                .fetch_add(1, LoomOrdering::Relaxed)
                                .wrapping_add(1);
                            peak.store(observed, LoomOrdering::Relaxed);
                        }
                    })
                };

                remover.join().expect("modeled removal remains healthy");
                adder.join().expect("modeled admission remains healthy");
                assert_eq!(
                    peak.load(LoomOrdering::Relaxed),
                    1,
                    "an admission reusing released capacity must not observe a stale diagnostic count"
                );
            });
        }

        let old_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_model(false, LoomOrdering::Relaxed, LoomOrdering::Relaxed);
        }));
        let old_panic = old_panic.expect_err(
            "the old release-before-diagnostic Relaxed model must expose the stale peak",
        );
        let old_message = old_panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| old_panic.downcast_ref::<&'static str>().copied());
        assert!(
            old_message.is_some_and(|message| {
                message.contains(
                    "an admission reusing released capacity must not observe a stale diagnostic count",
                )
            }),
            "the negative-control panic must be the stale-diagnostic assertion, got {old_message:?}"
        );

        run_model(
            true,
            hard_capacity_release_ordering(),
            hard_capacity_reservation_success_ordering(),
        );
    }
}
