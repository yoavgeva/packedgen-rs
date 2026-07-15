use core::fmt;
use std::mem;

use opthash::{ElasticHashMap, EpochSnapshot, Equivalent, ReserveFraction};

use crate::filter::NegativeLookupFilter;
use crate::route_cache::RouteCache;
use crate::{
    ArenaError, CapacityError, ElasticConfig, InsertOutcome, MaintenanceMode, PackedKeyArena,
    PackedKeyRef,
};

/// Binary-key elastic map backed by a segmented packed-key arena.
///
/// Stored table keys are eight-byte [`PackedKeyRef`] values. Lookups hash the
/// caller's bytes once, then compare candidate references against bytes in the
/// arena through the owned core's prehashed API.
pub struct PackedBinaryMap<V> {
    inner: ElasticHashMap<PackedKeyRef, V>,
    arena: PackedKeyArena,
    negative_filter: NegativeLookupFilter,
    route_cache: RouteCache,
    live_limit: usize,
    live_key_bytes: usize,
    deletes_since_rebuild: usize,
    maintenance_mode: MaintenanceMode,
    maintenance_runs: usize,
    compaction_failures: usize,
    arena_allocated_bytes_reclaimed: usize,
    structural_revision: u64,
}

impl<V> PackedBinaryMap<V> {
    /// Constructs a fixed-epoch binary map using the default key-segment size.
    #[must_use]
    pub fn new(config: ElasticConfig) -> Self {
        Self {
            inner: ElasticHashMap::with_capacity_and_reserve(
                config.live_capacity(),
                config.reserve(),
            ),
            arena: PackedKeyArena::new(),
            negative_filter: NegativeLookupFilter::new(config.live_capacity()),
            route_cache: RouteCache::new(config.live_capacity(), config.route_cache_slots()),
            live_limit: config.live_capacity(),
            live_key_bytes: 0,
            deletes_since_rebuild: 0,
            maintenance_mode: config.maintenance_mode(),
            maintenance_runs: 0,
            compaction_failures: 0,
            arena_allocated_bytes_reclaimed: 0,
            structural_revision: 0,
        }
    }

    /// Constructs a fixed-epoch map with explicit key-arena allocation
    /// granularity.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] for an invalid segment size.
    pub fn with_key_segment_bytes(
        config: ElasticConfig,
        segment_bytes: usize,
    ) -> Result<Self, ArenaError> {
        Ok(Self {
            inner: ElasticHashMap::with_capacity_and_reserve(
                config.live_capacity(),
                config.reserve(),
            ),
            arena: PackedKeyArena::with_segment_bytes(segment_bytes)?,
            negative_filter: NegativeLookupFilter::new(config.live_capacity()),
            route_cache: RouteCache::new(config.live_capacity(), config.route_cache_slots()),
            live_limit: config.live_capacity(),
            live_key_bytes: 0,
            deletes_since_rebuild: 0,
            maintenance_mode: config.maintenance_mode(),
            maintenance_runs: 0,
            compaction_failures: 0,
            arena_allocated_bytes_reclaimed: 0,
            structural_revision: 0,
        })
    }

    /// Inserts or replaces a binary key.
    ///
    /// Replacements do not append duplicate key bytes to the arena.
    ///
    /// # Errors
    ///
    /// Returns a capacity error for a new key at the fixed live limit, or an
    /// arena error when the key is too large or allocation fails.
    pub fn try_insert(&mut self, key: &[u8], value: V) -> Result<InsertOutcome<V>, PackedMapError> {
        let hash = self.inner.hash_key(key);
        if self.negative_filter.may_contain_hash(hash) {
            let query = PackedQuery {
                arena: &self.arena,
                bytes: key,
            };
            if let Some(previous) = self.inner.get_mut_prehashed(hash, &query) {
                return Ok(InsertOutcome::Replaced(mem::replace(previous, value)));
            }
        }

        if self.inner.len() >= self.live_limit {
            return Err(PackedMapError::Capacity(CapacityError::new(
                self.live_limit,
            )));
        }

        let key_ref = self.arena.insert(key).map_err(PackedMapError::Arena)?;
        self.negative_filter.insert_hash(hash);
        let location = match self
            .inner
            .try_insert_unique_prehashed_in_place_with_location(hash, key_ref, value)
        {
            Ok((location, _)) => location,
            Err((_key_ref, value)) => {
                self.rebuild_core();
                let key_ref = self.arena.insert(key).map_err(PackedMapError::Arena)?;
                let Ok((location, _)) = self
                    .inner
                    .try_insert_unique_prehashed_in_place_with_location(hash, key_ref, value)
                else {
                    return Err(PackedMapError::Capacity(CapacityError::new(
                        self.live_limit,
                    )));
                };
                location
            }
        };
        self.route_cache.insert(hash, location);
        self.live_key_bytes += key.len();
        self.structural_revision = self.structural_revision.wrapping_add(1);
        Ok(InsertOutcome::Inserted)
    }

    /// Returns a shared value reference for a binary key.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = self.inner.hash_key(key);
        let query = PackedQuery {
            arena: &self.arena,
            bytes: key,
        };
        for location in self.route_cache.candidates(hash) {
            if let Some(value) = self.inner.get_prehashed_at(location, hash, &query) {
                return Some(value);
            }
        }
        if self.route_cache.definitely_absent(hash) || !self.negative_filter.may_contain_hash(hash)
        {
            return None;
        }
        self.inner.get_prehashed(hash, &query)
    }

    /// Looks up a fixed batch of binary keys in a memory-latency-friendly
    /// order.
    ///
    /// Hashes and route candidates are prepared first, then each cache way is
    /// resolved across the whole batch before exact fallbacks run. This keeps
    /// independent table and arena reads in flight together on large indexes.
    #[must_use]
    pub fn get_many<const N: usize>(&self, keys: [&[u8]; N]) -> [Option<&V>; N] {
        let hashes = keys.map(|key| self.inner.hash_key(key));
        let candidates: [[Option<opthash::PrehashedLocation>; 2]; N] =
            core::array::from_fn(|index| {
                let mut routes = self.route_cache.candidates(hashes[index]);
                [routes.next(), routes.next()]
            });
        let mut results = [None; N];

        for way in [0, 1] {
            for index in 0..N {
                if results[index].is_some() {
                    continue;
                }
                let Some(location) = candidates[index][way] else {
                    continue;
                };
                let query = PackedQuery {
                    arena: &self.arena,
                    bytes: keys[index],
                };
                results[index] = self.inner.get_prehashed_at(location, hashes[index], &query);
            }
        }

        for index in 0..N {
            if results[index].is_some()
                || self.route_cache.definitely_absent(hashes[index])
                || !self.negative_filter.may_contain_hash(hashes[index])
            {
                continue;
            }
            results[index] = self.inner.get_prehashed(
                hashes[index],
                &PackedQuery {
                    arena: &self.arena,
                    bytes: keys[index],
                },
            );
        }
        results
    }

    /// Returns whether a binary key is live.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Removes a key and returns its value.
    ///
    /// Key bytes remain in the current append-only arena until a future
    /// compacting generation rebuild.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let hash = self.inner.hash_key(key);
        if !self.negative_filter.may_contain_hash(hash) {
            return None;
        }
        let removed = self.inner.remove_prehashed_deferred(
            hash,
            &PackedQuery {
                arena: &self.arena,
                bytes: key,
            },
        );
        let (key_ref, value) = removed?;
        self.structural_revision = self.structural_revision.wrapping_add(1);
        self.live_key_bytes = self.live_key_bytes.saturating_sub(key_ref.len());
        self.deletes_since_rebuild += 1;
        if self.maintenance_mode == MaintenanceMode::Synchronous && self.maintenance_due() {
            self.rebuild_core();
        }
        Some(value)
    }

    /// Returns whether delete churn has crossed the maintenance threshold.
    #[must_use]
    pub fn maintenance_due(&self) -> bool {
        self.deletes_since_rebuild >= self.rebuild_delete_threshold()
    }

    /// Rebuilds the fixed epoch and compacts dead key bytes when any deletes
    /// have accumulated. Returns whether maintenance work was performed.
    ///
    /// Deferred-mode owners can call this at a controlled single-writer
    /// boundary to keep rebuild latency off the request that crossed the
    /// threshold.
    pub fn maintain(&mut self) -> bool {
        if self.deletes_since_rebuild == 0 {
            return false;
        }
        self.rebuild_core();
        true
    }

    /// Starts a staged maintenance plan for the current structural revision.
    ///
    /// This allocates and captures compact references, but key-byte copying is
    /// left to [`prepare_maintenance_step`](Self::prepare_maintenance_step).
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::AllocationFailed`] if the reference snapshot
    /// cannot be allocated.
    pub fn try_begin_maintenance(&self) -> Result<PackedMaintenancePlan, ArenaError> {
        let mut pending_refs = Vec::new();
        pending_refs
            .try_reserve_exact(self.inner.len())
            .map_err(|_| ArenaError::AllocationFailed)?;
        pending_refs.extend(self.inner.iter().map(|(key_ref, _)| *key_ref));
        let mut remapped_refs = Vec::new();
        remapped_refs
            .try_reserve_exact(self.inner.len())
            .map_err(|_| ArenaError::AllocationFailed)?;
        Ok(PackedMaintenancePlan {
            source_revision: self.structural_revision,
            pending_refs,
            next: 0,
            replacement_arena: self.arena.empty_like(),
            remapped_refs,
        })
    }

    /// Returns whether structural mutation invalidated a staged plan.
    #[must_use]
    pub fn maintenance_plan_is_stale(&self, plan: &PackedMaintenancePlan) -> bool {
        plan.source_revision != self.structural_revision
    }

    /// Replaces a stale or unwanted plan with a fresh snapshot.
    ///
    /// The replacement is fully allocated before the old plan is discarded, so
    /// allocation failure leaves the caller's existing plan intact.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::AllocationFailed`] if the new reference snapshot
    /// cannot be allocated.
    pub fn try_restart_maintenance(
        &self,
        plan: &mut PackedMaintenancePlan,
    ) -> Result<(), ArenaError> {
        let replacement = self.try_begin_maintenance()?;
        *plan = replacement;
        Ok(())
    }

    /// Copies at most `max_entries` live keys into a staged compact arena.
    ///
    /// A structural mutation after the plan began returns
    /// [`MaintenanceError::StalePlan`]. A zero budget performs no copying.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenanceError::StalePlan`] after structural mutation or
    /// [`MaintenanceError::Arena`] when staged key storage cannot grow.
    pub fn prepare_maintenance_step(
        &self,
        plan: &mut PackedMaintenancePlan,
        max_entries: usize,
    ) -> Result<MaintenanceProgress, MaintenanceError> {
        self.validate_maintenance_plan(plan)?;
        let end = plan
            .next
            .saturating_add(max_entries)
            .min(plan.pending_refs.len());
        for old_ref in &plan.pending_refs[plan.next..end] {
            let bytes = self
                .arena
                .get(*old_ref)
                .ok_or(MaintenanceError::StalePlan)?;
            let new_ref = plan
                .replacement_arena
                .insert(bytes)
                .map_err(MaintenanceError::Arena)?;
            plan.remapped_refs.push((*old_ref, new_ref));
        }
        let copied = end - plan.next;
        plan.next = end;
        Ok(MaintenanceProgress {
            copied,
            remaining: plan.pending_refs.len() - plan.next,
        })
    }

    /// Finishes a fully prepared plan and atomically replaces the writer's
    /// table and key arena.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenanceError::PlanNotReady`] when keys remain or
    /// [`MaintenanceError::StalePlan`] after structural mutation.
    pub fn finish_maintenance(
        &mut self,
        plan: &mut PackedMaintenancePlan,
    ) -> Result<(), MaintenanceError> {
        self.validate_maintenance_plan(plan)?;
        if plan.next != plan.pending_refs.len() {
            return Err(MaintenanceError::PlanNotReady {
                remaining: plan.pending_refs.len() - plan.next,
            });
        }
        plan.remapped_refs
            .sort_unstable_by_key(|(old_ref, _)| *old_ref);
        let empty_arena = plan.replacement_arena.empty_like();
        let replacement_arena = mem::replace(&mut plan.replacement_arena, empty_arena);
        let remapped_refs = mem::take(&mut plan.remapped_refs);
        self.rebuild_core_with_compaction(Some((replacement_arena, remapped_refs)));
        Ok(())
    }

    /// Removes every entry and releases packed key segments.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.arena.clear();
        self.negative_filter.clear();
        self.route_cache.clear();
        self.live_key_bytes = 0;
        self.deletes_since_rebuild = 0;
        self.maintenance_runs = 0;
        self.compaction_failures = 0;
        self.arena_allocated_bytes_reclaimed = 0;
        self.structural_revision = self.structural_revision.wrapping_add(1);
    }

    /// Number of live keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether no live keys exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Captures occupancy, epoch, filter, and packed-key memory state.
    #[must_use]
    pub fn stats(&self) -> PackedMapStats {
        PackedMapStats {
            len: self.inner.len(),
            live_limit: self.live_limit,
            core_capacity: self.inner.capacity(),
            reserve: self.inner.reserve_fraction(),
            epoch: self.inner.epoch(),
            negative_filter_bytes: self.negative_filter.bytes(),
            route_cache_bytes: self.route_cache.bytes(),
            route_cache_entries: self.route_cache.cached(),
            route_cache_overflows: self.route_cache.overflowed(),
            arena_allocated_bytes: self.arena.allocated_bytes(),
            arena_key_bytes: self.arena.key_bytes(),
            live_key_bytes: self.live_key_bytes,
            deletes_since_rebuild: self.deletes_since_rebuild,
            maintenance_runs: self.maintenance_runs,
            compaction_failures: self.compaction_failures,
            arena_allocated_bytes_reclaimed: self.arena_allocated_bytes_reclaimed,
        }
    }

    fn rebuild_delete_threshold(&self) -> usize {
        (self.live_limit / 4).max(1)
    }

    fn rebuild_core(&mut self) {
        let compaction = if let Ok(compaction) = self.build_compacted_arena() {
            Some(compaction)
        } else {
            self.compaction_failures += 1;
            None
        };
        self.rebuild_core_with_compaction(compaction);
    }

    fn rebuild_core_with_compaction(
        &mut self,
        compaction: Option<(PackedKeyArena, Vec<(PackedKeyRef, PackedKeyRef)>)>,
    ) {
        let allocated_before = self.arena.allocated_bytes();
        let replacement = ElasticHashMap::with_capacity_and_reserve(
            self.live_limit,
            self.inner.reserve_fraction(),
        );
        let old = mem::replace(&mut self.inner, replacement);
        let remapped_refs = compaction.map(|(replacement_arena, remapped_refs)| {
            drop(mem::replace(&mut self.arena, replacement_arena));
            remapped_refs
        });
        let mut replacement_filter = NegativeLookupFilter::new(self.live_limit);
        self.route_cache.clear();
        for (key_ref, value) in old {
            let rebuilt_ref = remapped_refs.as_ref().map_or(key_ref, |references| {
                references
                    .binary_search_by_key(&key_ref, |(old_ref, _)| *old_ref)
                    .map(|index| references[index].1)
                    .expect("every live packed key must have a compacted reference")
            });
            let bytes = self
                .arena
                .get(rebuilt_ref)
                .expect("live packed key reference must resolve");
            let hash = self.inner.hash_key(bytes);
            let (location, _) = self
                .inner
                .try_insert_unique_prehashed_in_place_with_location(hash, rebuilt_ref, value)
                .unwrap_or_else(|_| panic!("fresh elastic epoch must fit every live entry"));
            replacement_filter.insert_hash(hash);
            self.route_cache.insert(hash, location);
        }
        self.deletes_since_rebuild = 0;
        self.negative_filter = replacement_filter;
        self.maintenance_runs += 1;
        self.arena_allocated_bytes_reclaimed +=
            allocated_before.saturating_sub(self.arena.allocated_bytes());
        self.structural_revision = self.structural_revision.wrapping_add(1);
    }

    fn build_compacted_arena(
        &self,
    ) -> Result<(PackedKeyArena, Vec<(PackedKeyRef, PackedKeyRef)>), ArenaError> {
        let mut replacement = self.arena.empty_like();
        let mut references = Vec::new();
        references
            .try_reserve_exact(self.inner.len())
            .map_err(|_| ArenaError::AllocationFailed)?;
        for (old_ref, _) in &self.inner {
            let bytes = self
                .arena
                .get(*old_ref)
                .expect("live packed key reference must resolve");
            references.push((*old_ref, replacement.insert(bytes)?));
        }
        references.sort_unstable_by_key(|(old_ref, _)| *old_ref);
        Ok((replacement, references))
    }

    fn validate_maintenance_plan(
        &self,
        plan: &PackedMaintenancePlan,
    ) -> Result<(), MaintenanceError> {
        if self.maintenance_plan_is_stale(plan) {
            return Err(MaintenanceError::StalePlan);
        }
        Ok(())
    }
}

/// Opaque staged key-compaction work for a [`PackedBinaryMap`].
pub struct PackedMaintenancePlan {
    source_revision: u64,
    pending_refs: Vec<PackedKeyRef>,
    next: usize,
    replacement_arena: PackedKeyArena,
    remapped_refs: Vec<(PackedKeyRef, PackedKeyRef)>,
}

impl PackedMaintenancePlan {
    /// Keys still waiting to be copied before cutover.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.pending_refs.len() - self.next
    }

    /// Returns whether every captured key has been copied.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.remaining() == 0
    }
}

/// Result of one bounded maintenance-preparation step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceProgress {
    /// Keys copied by this step.
    pub copied: usize,
    /// Keys still waiting to be copied.
    pub remaining: usize,
}

impl MaintenanceProgress {
    /// Returns whether the plan is ready for final cutover.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        self.remaining == 0
    }
}

/// Failure while preparing or finishing staged maintenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceError {
    /// Key-arena allocation failed while staging compacted bytes.
    Arena(ArenaError),
    /// The map was structurally mutated after the plan began.
    StalePlan,
    /// Cutover was requested before all live keys were copied.
    PlanNotReady {
        /// Keys still waiting to be copied.
        remaining: usize,
    },
}

impl fmt::Display for MaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arena(error) => error.fmt(formatter),
            Self::StalePlan => formatter.write_str("maintenance plan is stale"),
            Self::PlanNotReady { remaining } => {
                write!(formatter, "maintenance plan has {remaining} keys remaining")
            }
        }
    }
}

impl std::error::Error for MaintenanceError {}

struct PackedQuery<'a> {
    arena: &'a PackedKeyArena,
    bytes: &'a [u8],
}

impl Equivalent<PackedKeyRef> for PackedQuery<'_> {
    fn equivalent(&self, key: &PackedKeyRef) -> bool {
        self.arena.get(*key) == Some(self.bytes)
    }
}

/// Failed packed binary-map insertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackedMapError {
    /// The fixed epoch reached its configured live-key limit.
    Capacity(CapacityError),
    /// Key packing or arena allocation failed.
    Arena(ArenaError),
}

impl fmt::Display for PackedMapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity(error) => error.fmt(formatter),
            Self::Arena(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for PackedMapError {}

/// Observable state for a [`PackedBinaryMap`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackedMapStats {
    /// Current live keys.
    pub len: usize,
    /// Application-enforced live-key limit.
    pub live_limit: usize,
    /// Core capacity before automatic growth.
    pub core_capacity: usize,
    /// Exact empty-slot reserve.
    pub reserve: ReserveFraction,
    /// Core allocation-epoch state.
    pub epoch: EpochSnapshot,
    /// Requested bytes in the definite-negative filter.
    pub negative_filter_bytes: usize,
    /// Requested bytes in the best-effort direct-location accelerator.
    pub route_cache_bytes: usize,
    /// Live routes retained by the accelerator.
    pub route_cache_entries: usize,
    /// Routes that fell back to the exact schedule because their bucket filled.
    pub route_cache_overflows: usize,
    /// Requested capacity held by key-arena segments and their directory.
    pub arena_allocated_bytes: usize,
    /// All key bytes appended in this generation, including removed keys.
    pub arena_key_bytes: usize,
    /// Key bytes belonging to currently live entries.
    pub live_key_bytes: usize,
    /// Deletes accumulated since the last byte-aware table rebuild.
    pub deletes_since_rebuild: usize,
    /// Completed table-maintenance runs since construction or clear.
    pub maintenance_runs: usize,
    /// Compaction staging attempts that failed and fell back to table-only rebuild.
    pub compaction_failures: usize,
    /// Cumulative requested arena capacity released by successful compactions.
    pub arena_allocated_bytes_reclaimed: usize,
}

impl PackedMapStats {
    /// Bytes retained for removed keys until compaction.
    #[must_use]
    pub fn dead_key_bytes(self) -> usize {
        self.arena_key_bytes.saturating_sub(self.live_key_bytes)
    }
}
