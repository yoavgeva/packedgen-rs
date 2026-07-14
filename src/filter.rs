use std::hash::{BuildHasher, Hash};

use opthash::DefaultHashBuilder;

/// An epoch-stable definite-negative filter.
///
/// Bits are set on insertion and deliberately not cleared on deletion. This
/// guarantees no false negatives while a table generation is live. A rebuild
/// or `clear` resets accumulated false positives.
pub(crate) struct NegativeLookupFilter {
    words: Box<[u64]>,
    hash_builder: DefaultHashBuilder,
}

impl NegativeLookupFilter {
    /// Eight filter bits per configured live entry, rounded to whole words.
    pub(crate) fn new(live_capacity: usize) -> Self {
        let words = live_capacity.div_ceil(8).max(1);
        Self {
            words: vec![0; words].into_boxed_slice(),
            hash_builder: DefaultHashBuilder::default(),
        }
    }

    #[inline]
    pub(crate) fn insert<Q: Hash + ?Sized>(&mut self, key: &Q) {
        let (word, mask) = self.location(key);
        self.words[word] |= mask;
    }

    #[inline]
    pub(crate) fn may_contain<Q: Hash + ?Sized>(&self, key: &Q) -> bool {
        let (word, mask) = self.location(key);
        self.words[word] & mask == mask
    }

    pub(crate) fn clear(&mut self) {
        self.words.fill(0);
    }

    pub(crate) fn bytes(&self) -> usize {
        size_of_val(self.words.as_ref())
    }

    #[inline]
    fn location<Q: Hash + ?Sized>(&self, key: &Q) -> (usize, u64) {
        let mixed = mix(self.hash_builder.hash_one(key));
        let word = reduce(mixed, self.words.len());
        let first = (mixed & 63) as u32;
        let distance = 1 + ((mixed >> 6) % 63) as u32;
        let second = (first + distance) & 63;
        (word, (1_u64 << first) | (1_u64 << second))
    }
}

#[inline]
fn reduce(hash: u64, upper: usize) -> usize {
    let upper = u128::try_from(upper).expect("usize always fits in u128");
    let reduced = (u128::from(hash) * upper) >> 64;
    usize::try_from(reduced).expect("reduced hash is below the original usize upper bound")
}

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
    use super::NegativeLookupFilter;

    #[test]
    fn inserted_keys_are_never_rejected() {
        let mut filter = NegativeLookupFilter::new(10_000);
        for key in 0..10_000_u64 {
            filter.insert(&key);
        }
        for key in 0..10_000_u64 {
            assert!(filter.may_contain(&key));
        }
    }

    #[test]
    fn clear_forgets_previous_epoch() {
        let mut filter = NegativeLookupFilter::new(128);
        filter.insert(&42_u64);
        assert!(filter.may_contain(&42_u64));
        filter.clear();
        assert!(!filter.may_contain(&42_u64));
    }

    #[test]
    fn uses_eight_bits_per_entry_at_word_boundaries() {
        let filter = NegativeLookupFilter::new(1_024);
        assert_eq!(filter.bytes(), 1_024);
    }

    #[test]
    fn full_epoch_rejects_most_absent_keys() {
        let mut filter = NegativeLookupFilter::new(100_000);
        for key in 0..100_000_u64 {
            filter.insert(&key);
        }

        let false_positives = (100_000..200_000_u64)
            .filter(|key| filter.may_contain(key))
            .count();
        assert!(
            false_positives < 10_000,
            "unexpected false-positive rate: {false_positives}/100000"
        );
    }
}
