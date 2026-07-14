use opthash::PrehashedLocation;

const WAYS: usize = 2;
const SLOT_BITS: u32 = 27;
const SLOT_MASK: u64 = (1_u64 << SLOT_BITS) - 1;
const MAX_LEVEL: u64 = (1_u64 << (u32::BITS - SLOT_BITS)) - 1;

/// Best-effort set-associative accelerator for stable elastic locations.
///
/// A missing, collided, stale, or unrepresentable entry always falls back to
/// the exact elastic schedule. Cache contents therefore affect speed only.
pub(crate) struct RouteCache {
    tags: Box<[u8]>,
    locations: Box<[u32]>,
    overflow_buckets: Box<[u64]>,
    bucket_count: usize,
    bucket_mask: usize,
    cached: usize,
    overflowed: usize,
}

impl RouteCache {
    /// Two ways and one bucket per live entry gives two cache slots per
    /// configured entry. The low-occupancy buckets keep insertion and hit
    /// scans short; overflow safely falls back to the exact schedule.
    pub(crate) fn new(live_capacity: usize) -> Self {
        let bucket_count = live_capacity;
        let slots = bucket_count.saturating_mul(WAYS);
        Self {
            tags: vec![0; slots].into_boxed_slice(),
            locations: vec![0; slots].into_boxed_slice(),
            overflow_buckets: vec![0; bucket_count.div_ceil(64)].into_boxed_slice(),
            bucket_count,
            bucket_mask: if bucket_count.is_power_of_two() {
                bucket_count - 1
            } else {
                usize::MAX
            },
            cached: 0,
            overflowed: 0,
        }
    }

    pub(crate) fn insert(&mut self, hash: u64, location: PrehashedLocation) {
        let Some(compact) = compact_location(location) else {
            self.overflowed += 1;
            return;
        };
        let Some(start) = self.bucket_start(hash) else {
            self.overflowed += 1;
            return;
        };
        let tag = tag(hash);
        for index in start..start + WAYS {
            if self.tags[index] == 0 {
                self.locations[index] = compact;
                self.tags[index] = tag;
                self.cached += 1;
                return;
            }
        }
        self.overflowed += 1;
        self.overflow_buckets[start / WAYS / 64] |= 1_u64 << (start / WAYS % 64);
    }

    pub(crate) fn candidates(&self, hash: u64) -> RouteCandidates<'_> {
        let start = self.bucket_start(hash).unwrap_or(0);
        RouteCandidates {
            cache: self,
            tag: tag(hash),
            next: start,
            end: start + usize::from(self.bucket_count != 0) * WAYS,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.tags.fill(0);
        self.overflow_buckets.fill(0);
        self.cached = 0;
        self.overflowed = 0;
    }

    pub(crate) fn bytes(&self) -> usize {
        self.tags.len()
            + size_of_val(self.locations.as_ref())
            + size_of_val(self.overflow_buckets.as_ref())
    }

    pub(crate) fn cached(&self) -> usize {
        self.cached
    }

    pub(crate) fn overflowed(&self) -> usize {
        self.overflowed
    }

    /// Returns true only when every key assigned to this bucket was cached and
    /// none of its retained tags matches the query.
    pub(crate) fn definitely_absent(&self, hash: u64) -> bool {
        let Some(start) = self.bucket_start(hash) else {
            return true;
        };
        let bucket = start / WAYS;
        if self.overflow_buckets[bucket / 64] & (1_u64 << (bucket % 64)) != 0 {
            return false;
        }
        let tag = tag(hash);
        self.tags[start..start + WAYS]
            .iter()
            .all(|candidate| *candidate != tag)
    }

    fn bucket_start(&self, hash: u64) -> Option<usize> {
        if self.bucket_count == 0 {
            return None;
        }
        let bucket = if self.bucket_mask == usize::MAX {
            reduce(hash.rotate_right(7), self.bucket_count)
        } else {
            ((hash >> 7) as usize) & self.bucket_mask
        };
        Some(bucket * WAYS)
    }
}

pub(crate) struct RouteCandidates<'a> {
    cache: &'a RouteCache,
    tag: u8,
    next: usize,
    end: usize,
}

impl Iterator for RouteCandidates<'_> {
    type Item = PrehashedLocation;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.end {
            let index = self.next;
            self.next += 1;
            if self.cache.tags[index] == self.tag {
                return Some(expand_location(self.cache.locations[index]));
            }
        }
        None
    }
}

fn compact_location(location: PrehashedLocation) -> Option<u32> {
    let bits = location.bits();
    let level = bits >> 32;
    let slot = bits & u64::from(u32::MAX);
    if level > MAX_LEVEL || slot > SLOT_MASK {
        return None;
    }
    u32::try_from((level << SLOT_BITS) | slot).ok()
}

fn expand_location(compact: u32) -> PrehashedLocation {
    let compact = u64::from(compact);
    let level = compact >> SLOT_BITS;
    let slot = compact & SLOT_MASK;
    PrehashedLocation::from_bits((level << 32) | slot)
}

fn tag(hash: u64) -> u8 {
    u8::try_from(hash & 0x7f).expect("masked route tag fits u8") + 1
}

fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize always fits in u128");
    usize::try_from((u128::from(hash) * upper) >> 64)
        .expect("reduced hash is below the original usize upper bound")
}

#[cfg(test)]
mod tests {
    use super::{RouteCache, expand_location};
    use opthash::PrehashedLocation;

    #[test]
    fn locations_round_trip_and_wrong_tags_do_not_match() {
        let location = PrehashedLocation::from_bits((3_u64 << 32) | 0x2a);
        let mut cache = RouteCache::new(16);
        cache.insert(7, location);

        assert_eq!(cache.candidates(7).collect::<Vec<_>>(), [location]);
        assert!(cache.candidates(1_u64 << 63).next().is_none());
        assert!(cache.definitely_absent(1_u64 << 63));
        assert_eq!(expand_location(0).bits(), 0);
    }

    #[test]
    fn full_bucket_overflows_without_evicting_existing_routes() {
        let mut cache = RouteCache::new(4);
        for slot in 0..3_u64 {
            cache.insert(0, PrehashedLocation::from_bits(slot));
        }
        assert_eq!(cache.cached(), 2);
        assert_eq!(cache.overflowed(), 1);
        assert_eq!(cache.candidates(0).count(), 2);
        assert!(!cache.definitely_absent(1));
    }
}
