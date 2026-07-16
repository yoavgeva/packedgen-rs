use opthash::PrehashedLocation;

/// Four entries keep one route-cache bucket to a single 16-byte SIMD lane.
pub(crate) const WAYS: usize = 4;
// Four roots/children cap the near-saturation insertion tail without giving
// up the adaptive cache's >96% route coverage. Larger searches gained only a
// few points of coverage but created multi-microsecond final-percent inserts.
const MAX_RELOCATION_BUCKETS: usize = 4;
const NO_PARENT: usize = usize::MAX;

/// Best-effort two-choice accelerator for stable Elastic locations.
///
/// Each non-zero `u32` stores both a hash tag and a capacity-aware location.
/// Elastic's level capacities halve, so `(level, slot)` can be mapped into a
/// dense code bounded by twice the next power of two above the live capacity.
/// This leaves substantially more tag bits than storing level and slot as two
/// independent fixed-width fields.
///
/// A missing, collided, stale, or unrepresentable entry always falls back to
/// the exact Elastic schedule. Cache contents therefore affect speed only.
pub(crate) struct RouteCache {
    entries: Box<[u32]>,
    overflow_buckets: Box<[u64]>,
    bucket_count: usize,
    bucket_mask: usize,
    theoretical_slots: u64,
    location_bits: u32,
    tag_bits: u32,
    tag_mask: u32,
    sparse_budget: bool,
    cached: usize,
    overflowed: usize,
}

impl RouteCache {
    pub(crate) fn new(live_capacity: usize, route_slots: usize) -> Self {
        Self::try_new(live_capacity, route_slots).expect("route-cache allocation failed")
    }

    pub(crate) fn try_new(live_capacity: usize, route_slots: usize) -> Result<Self, ()> {
        let layout = PackedLayout::for_capacity(live_capacity);
        let route_slots = route_slots.min(live_capacity.saturating_mul(2));
        let bucket_count = if layout.is_some() {
            route_slots.div_ceil(WAYS)
        } else {
            0
        };
        let slots = bucket_count.saturating_mul(WAYS);

        let mut entries = Vec::new();
        entries.try_reserve_exact(slots).map_err(|_| ())?;
        entries.resize(slots, 0);
        let mut overflow_buckets = Vec::new();
        overflow_buckets
            .try_reserve_exact(bucket_count.div_ceil(64))
            .map_err(|_| ())?;
        overflow_buckets.resize(bucket_count.div_ceil(64), 0);

        let layout = layout.unwrap_or(PackedLayout::DISABLED);
        Ok(Self {
            entries: entries.into_boxed_slice(),
            overflow_buckets: overflow_buckets.into_boxed_slice(),
            bucket_count,
            bucket_mask: if bucket_count.is_power_of_two() {
                bucket_count.saturating_sub(1)
            } else {
                usize::MAX
            },
            theoretical_slots: layout.theoretical_slots,
            location_bits: layout.location_bits,
            tag_bits: layout.tag_bits,
            tag_mask: layout.tag_mask,
            sparse_budget: route_slots >= live_capacity.saturating_mul(2),
            cached: 0,
            overflowed: 0,
        })
    }

    pub(crate) fn insert(&mut self, hash: u64, location: PrehashedLocation) {
        let Some((first, second, tag)) = self.bucket_pair(hash) else {
            self.overflowed += 1;
            return;
        };
        let Some(entry) = self.compact_location(location, tag) else {
            self.overflowed += 1;
            self.mark_pair_overflow(first, second);
            return;
        };

        let first_state = self.bucket_state(first);
        if self.sparse_budget
            && let Some(index) = first_state.free
        {
            self.entries[index] = entry;
            self.cached += 1;
            return;
        }
        let second_state = if first == second {
            first_state
        } else {
            self.bucket_state(second)
        };
        let target = match (first_state.free, second_state.free) {
            (Some(index), None) | (None, Some(index)) => Some(index),
            // Prefer the primary when both have room. This keeps the common
            // hit in the first 16-byte lane; two-choice relocation still
            // supplies the high-occupancy coverage.
            (Some(first_free), Some(_)) => Some(first_free),
            (None, None) => None,
        };
        if let Some(index) = target {
            self.entries[index] = entry;
            self.cached += 1;
            return;
        }

        if self.relocate_and_insert(entry, first, second) {
            self.cached += 1;
        } else {
            self.overflowed += 1;
            self.mark_pair_overflow(first, second);
        }
    }

    #[inline]
    pub(crate) fn candidates(&self, hash: u64) -> RouteCandidates<'_> {
        let Some((first, tag)) = self.primary_and_tag(hash) else {
            return RouteCandidates {
                cache: self,
                tag: 0,
                first_bucket: 0,
                current_start: 0,
                lane: 0,
                active: false,
                second_pending: false,
            };
        };
        RouteCandidates {
            cache: self,
            tag,
            first_bucket: first,
            current_start: first * WAYS,
            lane: 0,
            active: true,
            second_pending: true,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.fill(0);
        self.overflow_buckets.fill(0);
        self.cached = 0;
        self.overflowed = 0;
    }

    pub(crate) fn bytes(&self) -> usize {
        size_of_val(self.entries.as_ref()) + size_of_val(self.overflow_buckets.as_ref())
    }

    pub(crate) fn cached(&self) -> usize {
        self.cached
    }

    pub(crate) fn overflowed(&self) -> usize {
        self.overflowed
    }

    /// Returns true only when every key assigned to both candidate buckets was
    /// cached and none of their retained tags matches the query.
    #[inline]
    pub(crate) fn definitely_absent(&self, hash: u64) -> bool {
        let Some((first, second, tag)) = self.bucket_pair(hash) else {
            return false;
        };
        if self.bucket_overflowed(first) || self.bucket_overflowed(second) {
            return false;
        }
        !self.bucket_has_tag(first, tag) && (first == second || !self.bucket_has_tag(second, tag))
    }

    /// Reuses the work of an exhausted [`Self::candidates`] iterator.
    ///
    /// The caller must have verified every yielded candidate. When neither
    /// bucket overflowed, those buckets contained every assigned live route,
    /// so exhausting their matching tags proves the key absent.
    #[inline]
    pub(crate) fn miss_after_candidates_is_definite(&self, hash: u64) -> bool {
        let Some((first, second, _)) = self.bucket_pair(hash) else {
            return false;
        };
        !self.bucket_overflowed(first) && !self.bucket_overflowed(second)
    }

    fn relocate_and_insert(&mut self, entry: u32, first: usize, second: usize) -> bool {
        // Search a bounded cuckoo path before moving anything. Failed searches
        // leave every old route intact, which makes overflow accounting exact.
        let mut nodes = [RelocationNode::EMPTY; MAX_RELOCATION_BUCKETS];
        nodes[0] = RelocationNode::root(first);
        let mut queued = 1;
        if second != first {
            nodes[1] = RelocationNode::root(second);
            queued += 1;
        }
        let mut cursor = 0;
        while cursor < queued {
            let bucket = nodes[cursor].bucket;
            for lane in 0..WAYS {
                let resident = self.entries[bucket * WAYS + lane];
                debug_assert_ne!(resident, 0);
                let alternate = self.alternate_bucket(bucket, self.entry_tag(resident));
                if alternate == bucket
                    || nodes[..queued].iter().any(|node| node.bucket == alternate)
                {
                    continue;
                }

                if let Some(free) = self.bucket_state(alternate).free {
                    self.apply_relocation_path(entry, free, cursor, lane, &nodes);
                    return true;
                }
                if queued == nodes.len() {
                    continue;
                }
                nodes[queued] = RelocationNode {
                    bucket: alternate,
                    parent: cursor,
                    via_lane: lane,
                };
                queued += 1;
            }
            cursor += 1;
        }
        false
    }

    fn apply_relocation_path(
        &mut self,
        entry: u32,
        mut free: usize,
        mut node: usize,
        mut lane: usize,
        nodes: &[RelocationNode; MAX_RELOCATION_BUCKETS],
    ) {
        loop {
            let source = nodes[node].bucket * WAYS + lane;
            self.entries[free] = self.entries[source];
            free = source;
            if nodes[node].parent == NO_PARENT {
                self.entries[free] = entry;
                return;
            }
            lane = nodes[node].via_lane;
            node = nodes[node].parent;
        }
    }

    #[inline]
    fn bucket_pair(&self, hash: u64) -> Option<(usize, usize, u32)> {
        let (first, tag) = self.primary_and_tag(hash)?;
        Some((first, self.alternate_bucket(first, tag), tag))
    }

    #[inline]
    fn primary_and_tag(&self, hash: u64) -> Option<(usize, u32)> {
        if self.bucket_count == 0 {
            return None;
        }
        let tag = self.tag(hash);
        // Keep the primary independent of the low bits retained as the tag.
        // `reduce_nbits` scales the shortened high part without a division.
        let bucket_hash = hash >> self.tag_bits;
        let first = if self.bucket_mask == usize::MAX {
            reduce_nbits(bucket_hash, self.bucket_count, u64::BITS - self.tag_bits)
        } else {
            let masked =
                bucket_hash & u64::try_from(self.bucket_mask).expect("bucket mask fits in u64");
            usize::try_from(masked).expect("masked bucket index fits in usize")
        };
        Some((first, tag))
    }

    #[inline]
    fn alternate_bucket(&self, bucket: usize, tag: u32) -> usize {
        // The tag is already uniformly distributed. Multiplication by an odd
        // golden-ratio constant is a bijection before reduction and is much
        // cheaper on the lookup path than a full hash finalizer.
        let mixed = u64::from(tag).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        if self.bucket_mask != usize::MAX {
            let masked = mixed & u64::try_from(self.bucket_mask).expect("bucket mask fits in u64");
            return bucket ^ usize::try_from(masked).expect("masked bucket index fits in usize");
        }
        let reflected_around = reduce(mixed, self.bucket_count);
        if reflected_around >= bucket {
            reflected_around - bucket
        } else {
            self.bucket_count - (bucket - reflected_around)
        }
    }

    #[inline]
    fn tag(&self, hash: u64) -> u32 {
        let candidate =
            u32::try_from(hash & u64::from(self.tag_mask)).expect("masked route tag fits in u32");
        if candidate == 0 {
            self.tag_mask
        } else {
            candidate
        }
    }

    fn compact_location(&self, location: PrehashedLocation, tag: u32) -> Option<u32> {
        let bits = location.bits();
        let level = u32::try_from(bits >> 32).ok()?;
        if level >= self.location_bits {
            return None;
        }
        let slot = bits & u64::from(u32::MAX);
        let offset = self.theoretical_slots - (self.theoretical_slots >> level);
        let end = if level + 1 == self.location_bits {
            self.theoretical_slots
        } else {
            self.theoretical_slots - (self.theoretical_slots >> (level + 1))
        };
        if slot >= end - offset {
            return None;
        }
        let code = u32::try_from(offset + slot).ok()?;
        Some((code << self.tag_bits) | tag)
    }

    #[inline]
    fn expand_location(&self, entry: u32) -> PrehashedLocation {
        let code = u64::from(entry >> self.tag_bits);
        let remaining = self.theoretical_slots - code;
        let remaining_bits = if remaining <= 1 {
            0
        } else {
            u64::BITS - (remaining - 1).leading_zeros()
        };
        let level = (self.location_bits - remaining_bits).min(self.location_bits - 1);
        let offset = self.theoretical_slots - (self.theoretical_slots >> level);
        PrehashedLocation::from_bits((u64::from(level) << 32) | (code - offset))
    }

    #[inline]
    fn entry_tag(&self, entry: u32) -> u32 {
        entry & self.tag_mask
    }

    fn bucket_state(&self, bucket: usize) -> BucketState {
        let start = bucket * WAYS;
        let mut free = None;
        for index in start..start + WAYS {
            if self.entries[index] == 0 {
                free = free.or(Some(index));
            }
        }
        BucketState { free }
    }

    #[inline]
    fn bucket_entries(&self, bucket: usize) -> &[u32] {
        let start = bucket * WAYS;
        &self.entries[start..start + WAYS]
    }

    #[inline]
    fn bucket_has_tag(&self, bucket: usize, tag: u32) -> bool {
        let entries = self.bucket_entries(bucket);
        self.entry_tag(entries[0]) == tag
            || self.entry_tag(entries[1]) == tag
            || self.entry_tag(entries[2]) == tag
            || self.entry_tag(entries[3]) == tag
    }

    fn mark_pair_overflow(&mut self, first: usize, second: usize) {
        self.mark_overflow(first);
        self.mark_overflow(second);
    }

    fn mark_overflow(&mut self, bucket: usize) {
        self.overflow_buckets[bucket / 64] |= 1_u64 << (bucket % 64);
    }

    #[inline]
    fn bucket_overflowed(&self, bucket: usize) -> bool {
        self.overflow_buckets[bucket / 64] & (1_u64 << (bucket % 64)) != 0
    }
}

#[derive(Clone, Copy)]
struct BucketState {
    free: Option<usize>,
}

#[derive(Clone, Copy)]
struct RelocationNode {
    bucket: usize,
    parent: usize,
    via_lane: usize,
}

impl RelocationNode {
    const EMPTY: Self = Self {
        bucket: usize::MAX,
        parent: NO_PARENT,
        via_lane: 0,
    };

    const fn root(bucket: usize) -> Self {
        Self {
            bucket,
            parent: NO_PARENT,
            via_lane: 0,
        }
    }
}

struct PackedLayout {
    theoretical_slots: u64,
    location_bits: u32,
    tag_bits: u32,
    tag_mask: u32,
}

impl PackedLayout {
    const DISABLED: Self = Self {
        theoretical_slots: 0,
        location_bits: 0,
        tag_bits: 0,
        tag_mask: 0,
    };

    fn for_capacity(live_capacity: usize) -> Option<Self> {
        let level_zero_bound = live_capacity.max(8).checked_next_power_of_two()?;
        let theoretical_slots = u64::try_from(level_zero_bound).ok()?.checked_mul(2)?;
        let location_bits = theoretical_slots.trailing_zeros();
        let tag_bits = u32::BITS.checked_sub(location_bits)?;
        if tag_bits == 0 {
            return None;
        }
        Some(Self {
            theoretical_slots,
            location_bits,
            tag_bits,
            tag_mask: u32::MAX >> location_bits,
        })
    }
}

pub(crate) struct RouteCandidates<'a> {
    cache: &'a RouteCache,
    tag: u32,
    first_bucket: usize,
    current_start: usize,
    lane: usize,
    active: bool,
    second_pending: bool,
}

impl Iterator for RouteCandidates<'_> {
    type Item = PrehashedLocation;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.active {
            let index = self.current_start + self.lane;
            self.lane += 1;
            let entry = self.cache.entries[index];
            let matches = entry != 0 && self.cache.entry_tag(entry) == self.tag;
            if self.lane == WAYS {
                self.lane = 0;
                if self.second_pending {
                    self.second_pending = false;
                    let second = self.cache.alternate_bucket(self.first_bucket, self.tag);
                    if second == self.first_bucket {
                        self.active = false;
                    } else {
                        self.current_start = second * WAYS;
                    }
                } else {
                    self.active = false;
                }
            }
            if matches {
                return Some(self.cache.expand_location(entry));
            }
        }
        None
    }
}

fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize always fits in u128");
    usize::try_from((u128::from(hash) * upper) >> 64)
        .expect("reduced hash is below the original usize upper bound")
}

fn reduce_nbits(value: u64, upper: usize, bits: u32) -> usize {
    let upper = u128::try_from(upper).expect("usize always fits in u128");
    usize::try_from((u128::from(value) * upper) >> bits)
        .expect("reduced hash is below the original usize upper bound")
}

#[cfg(test)]
#[inline]
fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{RouteCache, mix};
    use opthash::PrehashedLocation;

    #[test]
    fn locations_round_trip_and_wrong_tags_do_not_match() {
        let location = PrehashedLocation::from_bits((3_u64 << 32) | 1);
        let mut cache = RouteCache::new(16, 32);
        cache.insert(7, location);

        assert_eq!(cache.candidates(7).collect::<Vec<_>>(), [location]);
        assert!(cache.candidates(1_u64 << 63).next().is_none());
        assert!(cache.definitely_absent(1_u64 << 63));
        assert!(cache.miss_after_candidates_is_definite(7));
    }

    #[test]
    fn capacity_aware_locations_cover_each_geometric_level() {
        for capacity in [1, 15, 16, 17, 1_000_000] {
            let cache = RouteCache::new(capacity, capacity);
            for level in 0..cache.location_bits {
                let offset = cache.theoretical_slots - (cache.theoretical_slots >> level);
                let end = if level + 1 == cache.location_bits {
                    cache.theoretical_slots
                } else {
                    cache.theoretical_slots - (cache.theoretical_slots >> (level + 1))
                };
                for slot in [0, end - offset - 1] {
                    let location = PrehashedLocation::from_bits((u64::from(level) << 32) | slot);
                    let compact = cache.compact_location(location, 1).unwrap();
                    assert_eq!(cache.expand_location(compact), location);
                }
            }
        }
    }

    #[test]
    fn disabled_cache_never_claims_definite_absence() {
        let cache = RouteCache::new(16, 0);

        assert!(cache.candidates(7).next().is_none());
        assert!(!cache.definitely_absent(7));
        assert!(!cache.miss_after_candidates_is_definite(7));
    }

    #[test]
    fn largest_packed_layout_and_unpacked_fallback_are_safe() {
        if usize::BITS < 64 {
            return;
        }

        let large_capacity = usize::try_from(1_u64 << 30).unwrap();
        let large = RouteCache::new(large_capacity, 0);
        assert_eq!(large.location_bits, 31);
        assert_eq!(large.tag_bits, 1);
        let last = PrehashedLocation::from_bits((30_u64 << 32) | 1);
        let packed = large.compact_location(last, 1).unwrap();
        assert_eq!(large.expand_location(packed), last);

        let unpackable_capacity = usize::try_from(1_u64 << 31).unwrap();
        let mut fallback = RouteCache::new(unpackable_capacity, 1);
        fallback.insert(7, PrehashedLocation::from_bits(0));
        assert_eq!(fallback.bytes(), 0);
        assert_eq!(fallback.cached(), 0);
        assert_eq!(fallback.overflowed(), 1);
        assert!(fallback.candidates(7).next().is_none());
        assert!(!fallback.definitely_absent(7));
    }

    #[test]
    fn unrepresentable_location_disables_both_negative_proofs() {
        let mut cache = RouteCache::new(4, 8);
        let hash = 7;
        let unrepresentable = PrehashedLocation::from_bits(u64::from(cache.location_bits) << 32);
        let (first, second, _) = cache.bucket_pair(hash).unwrap();

        cache.insert(hash, unrepresentable);

        assert_eq!(cache.cached(), 0);
        assert_eq!(cache.overflowed(), 1);
        assert!(cache.bucket_overflowed(first));
        assert!(cache.bucket_overflowed(second));
        assert!(!cache.definitely_absent(hash));
    }

    #[test]
    fn full_pair_overflows_without_false_negative_answers() {
        let mut cache = RouteCache::new(4, 4);
        let mut inserted = Vec::new();
        for index in 0..5_u64 {
            let hash = mix(index);
            let location = PrehashedLocation::from_bits(index);
            cache.insert(hash, location);
            inserted.push((hash, location));
        }
        assert_eq!(cache.cached(), 4);
        assert_eq!(cache.overflowed(), 1);
        for (hash, location) in inserted {
            let retained = cache
                .candidates(hash)
                .any(|candidate| candidate == location);
            assert!(retained || !cache.definitely_absent(hash));
        }
    }

    #[test]
    fn two_choice_cache_retains_more_than_ninety_five_percent_at_one_x_budget() {
        let capacity = 100_000;
        let mut cache = RouteCache::new(capacity, capacity);
        for index in 0..capacity as u64 {
            let hash = mix(index);
            cache.insert(hash, PrehashedLocation::from_bits(index));
        }

        assert!(
            cache.cached() > capacity * 95 / 100,
            "cached={}",
            cache.cached()
        );
        assert_eq!(cache.cached() + cache.overflowed(), capacity);
        assert_eq!(cache.bytes(), capacity * 4 + 3_128);
    }

    #[test]
    fn clear_resets_routes_overflow_proofs_and_counters() {
        let mut cache = RouteCache::new(4, 4);
        for index in 0..5_u64 {
            cache.insert(mix(index), PrehashedLocation::from_bits(index));
        }
        cache.clear();

        assert_eq!(cache.cached(), 0);
        assert_eq!(cache.overflowed(), 0);
        assert!(cache.entries.iter().all(|entry| *entry == 0));
        assert!(cache.overflow_buckets.iter().all(|word| *word == 0));
    }
}
