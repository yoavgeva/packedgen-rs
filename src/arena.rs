use core::fmt;

const OFFSET_BITS: u32 = 32;
const SEGMENT_BITS: u32 = 15;
const LENGTH_BITS: u32 = 17;
const SEGMENT_SHIFT: u32 = OFFSET_BITS;
const LENGTH_SHIFT: u32 = OFFSET_BITS + SEGMENT_BITS;
const SEGMENT_MASK: u64 = (1_u64 << SEGMENT_BITS) - 1;
const LENGTH_MASK: u64 = (1_u64 << LENGTH_BITS) - 1;
const MAX_SEGMENTS: usize = 1 << SEGMENT_BITS;

/// Largest key accepted by the packed arena: exactly 64 KiB.
pub const MAX_PACKED_KEY_BYTES: usize = 1 << 16;

/// Default allocation granularity for packed key bytes.
pub const DEFAULT_KEY_SEGMENT_BYTES: usize = 1 << 20;

/// An eight-byte reference to immutable bytes in a [`PackedKeyArena`].
///
/// The packed fields are a 32-bit offset, 15-bit segment index, and 17-bit
/// length. References are meaningful only with the arena that created them.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct PackedKeyRef(u64);

impl PackedKeyRef {
    /// Byte offset within the referenced segment.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn offset(self) -> u32 {
        // The packed representation intentionally stores the offset in the
        // low 32 bits, so truncation is the decoding operation.
        self.0 as u32
    }

    /// Segment index within the originating arena.
    #[must_use]
    pub const fn segment(self) -> u16 {
        ((self.0 >> SEGMENT_SHIFT) & SEGMENT_MASK) as u16
    }

    /// Key length in bytes.
    #[must_use]
    pub const fn len(self) -> usize {
        ((self.0 >> LENGTH_SHIFT) & LENGTH_MASK) as usize
    }

    /// Returns whether this reference points to an empty key.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len() == 0
    }

    fn encode(segment: usize, offset: usize, length: usize) -> Self {
        debug_assert!(segment < MAX_SEGMENTS);
        debug_assert!(u32::try_from(offset).is_ok());
        debug_assert!(length <= MAX_PACKED_KEY_BYTES);
        Self(
            u64::try_from(offset).expect("validated key offset")
                | (u64::try_from(segment).expect("validated segment") << SEGMENT_SHIFT)
                | (u64::try_from(length).expect("validated key length") << LENGTH_SHIFT),
        )
    }
}

/// Append-only storage for variable-length binary keys.
///
/// Keys share a small number of fixed-capacity byte segments. Inserting a key
/// performs no allocation while the active segment has room. Segment vectors
/// may move, but packed references remain valid because they contain indices
/// and offsets rather than pointers.
#[derive(Debug)]
pub struct PackedKeyArena {
    segments: Vec<Vec<u8>>,
    segment_bytes: usize,
    keys: usize,
    key_bytes: usize,
}

impl PackedKeyArena {
    /// Creates an empty arena using one-MiB segments.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            segments: Vec::new(),
            segment_bytes: DEFAULT_KEY_SEGMENT_BYTES,
            keys: 0,
            key_bytes: 0,
        }
    }

    /// Creates an empty arena with a chosen segment allocation size.
    ///
    /// A key larger than this setting receives a dedicated segment large enough
    /// to hold that key.
    ///
    /// # Errors
    ///
    /// Returns [`ArenaError::InvalidSegmentSize`] for zero or sizes that cannot
    /// be represented by the packed 32-bit offset.
    pub fn with_segment_bytes(segment_bytes: usize) -> Result<Self, ArenaError> {
        if segment_bytes == 0 || u32::try_from(segment_bytes).is_err() {
            return Err(ArenaError::InvalidSegmentSize(segment_bytes));
        }
        Ok(Self {
            segments: Vec::new(),
            segment_bytes,
            keys: 0,
            key_bytes: 0,
        })
    }

    /// Appends a key and returns its compact reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the key exceeds 64 KiB, the segment-index space is
    /// exhausted, or allocation fails.
    pub fn insert(&mut self, key: &[u8]) -> Result<PackedKeyRef, ArenaError> {
        if key.len() > MAX_PACKED_KEY_BYTES {
            return Err(ArenaError::KeyTooLong {
                length: key.len(),
                maximum: MAX_PACKED_KEY_BYTES,
            });
        }

        let needs_segment = self
            .segments
            .last()
            .is_none_or(|segment| segment.capacity() - segment.len() < key.len());
        if needs_segment {
            self.allocate_segment(key.len())?;
        }

        let segment_index = self
            .segments
            .len()
            .checked_sub(1)
            .ok_or(ArenaError::AllocationFailed)?;
        let segment = self
            .segments
            .get_mut(segment_index)
            .ok_or(ArenaError::AllocationFailed)?;
        let offset = segment.len();
        segment.extend_from_slice(key);
        self.keys += 1;
        self.key_bytes += key.len();
        Ok(PackedKeyRef::encode(segment_index, offset, key.len()))
    }

    /// Resolves a reference produced by this arena.
    #[must_use]
    pub fn get(&self, key_ref: PackedKeyRef) -> Option<&[u8]> {
        let segment = self.segments.get(usize::from(key_ref.segment()))?;
        let start = usize::try_from(key_ref.offset()).ok()?;
        let end = start.checked_add(key_ref.len())?;
        segment.get(start..end)
    }

    /// Number of keys appended, including duplicate and empty keys.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.keys
    }

    /// Returns whether no keys have been appended.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.keys == 0
    }

    /// Sum of logical key bytes, excluding segment headroom.
    #[must_use]
    pub const fn key_bytes(&self) -> usize {
        self.key_bytes
    }

    /// Number of allocated byte segments.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Requested capacity of byte segments plus the outer segment directory.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.segments
            .iter()
            .map(Vec::capacity)
            .sum::<usize>()
            .saturating_add(
                self.segments
                    .capacity()
                    .saturating_mul(size_of::<Vec<u8>>()),
            )
    }

    /// Drops every segment and invalidates all previously returned references.
    pub fn clear(&mut self) {
        self.segments.clear();
        self.keys = 0;
        self.key_bytes = 0;
    }

    fn allocate_segment(&mut self, minimum: usize) -> Result<(), ArenaError> {
        if self.segments.len() >= MAX_SEGMENTS {
            return Err(ArenaError::TooManySegments);
        }
        self.segments
            .try_reserve(1)
            .map_err(|_| ArenaError::AllocationFailed)?;
        let capacity = self.segment_bytes.max(minimum);
        let mut segment = Vec::new();
        segment
            .try_reserve_exact(capacity)
            .map_err(|_| ArenaError::AllocationFailed)?;
        self.segments.push(segment);
        Ok(())
    }
}

impl Default for PackedKeyArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Failure while configuring or appending to a packed key arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArenaError {
    /// Segment capacity must be positive and fit the 32-bit packed offset.
    InvalidSegmentSize(usize),
    /// A key exceeded the supported binary-key limit.
    KeyTooLong {
        /// Attempted key size.
        length: usize,
        /// Maximum supported key size.
        maximum: usize,
    },
    /// The 15-bit segment index was exhausted.
    TooManySegments,
    /// The allocator rejected a fallible reservation.
    AllocationFailed,
}

impl fmt::Display for ArenaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSegmentSize(size) => {
                write!(formatter, "key-arena segment size is invalid: {size}")
            }
            Self::KeyTooLong { length, maximum } => {
                write!(formatter, "key length {length} exceeds maximum {maximum}")
            }
            Self::TooManySegments => formatter.write_str("packed key segment limit reached"),
            Self::AllocationFailed => formatter.write_str("packed key arena allocation failed"),
        }
    }
}

impl std::error::Error for ArenaError {}

#[cfg(test)]
mod tests {
    use super::{ArenaError, MAX_PACKED_KEY_BYTES, PackedKeyArena, PackedKeyRef};

    #[test]
    fn reference_is_exactly_eight_bytes() {
        assert_eq!(size_of::<PackedKeyRef>(), 8);
    }

    #[test]
    fn round_trips_binary_and_empty_keys() {
        let mut arena = PackedKeyArena::with_segment_bytes(64).unwrap();
        let empty = arena.insert(b"").unwrap();
        let binary = arena.insert(&[0, 255, 1, 128]).unwrap();

        assert!(empty.is_empty());
        assert_eq!(arena.get(empty), Some(b"".as_slice()));
        assert_eq!(arena.get(binary), Some([0, 255, 1, 128].as_slice()));
        assert_eq!(arena.len(), 2);
        assert_eq!(arena.key_bytes(), 4);
    }

    #[test]
    fn starts_new_segment_without_invalidating_old_reference() {
        let mut arena = PackedKeyArena::with_segment_bytes(8).unwrap();
        let first = arena.insert(b"12345678").unwrap();
        let second = arena.insert(b"abcdefgh").unwrap();

        assert_eq!(arena.segment_count(), 2);
        assert_eq!(arena.get(first), Some(b"12345678".as_slice()));
        assert_eq!(arena.get(second), Some(b"abcdefgh".as_slice()));
        assert_eq!(first.segment(), 0);
        assert_eq!(second.segment(), 1);
    }

    #[test]
    fn accepts_exactly_64_kib_and_rejects_larger_key() {
        let mut arena = PackedKeyArena::new();
        let maximum = vec![7_u8; MAX_PACKED_KEY_BYTES];
        let key_ref = arena.insert(&maximum).unwrap();
        assert_eq!(key_ref.len(), MAX_PACKED_KEY_BYTES);
        assert_eq!(arena.get(key_ref), Some(maximum.as_slice()));

        let error = arena
            .insert(&vec![0_u8; MAX_PACKED_KEY_BYTES + 1])
            .unwrap_err();
        assert_eq!(
            error,
            ArenaError::KeyTooLong {
                length: MAX_PACKED_KEY_BYTES + 1,
                maximum: MAX_PACKED_KEY_BYTES,
            }
        );
    }

    #[test]
    fn clear_invalidates_references_and_resets_accounting() {
        let mut arena = PackedKeyArena::new();
        let key_ref = arena.insert(b"key").unwrap();
        arena.clear();

        assert!(arena.is_empty());
        assert_eq!(arena.key_bytes(), 0);
        assert_eq!(arena.get(key_ref), None);
    }
}
