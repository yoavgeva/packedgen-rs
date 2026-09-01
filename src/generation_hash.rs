use std::hash::BuildHasher;

#[cfg(feature = "shared-gx")]
use gxhash::GxHasher;
#[cfg(not(feature = "shared-gx"))]
use rapidhash::fast::RandomState as RapidRandomState;
#[cfg(feature = "shared-gx")]
use std::hash::Hash;

use crate::frozen_map::Digest;

#[cfg(feature = "shared-gx")]
/// Hash state shared by atomic writer routing and frozen-key indexing.
pub type GenerationHashBuilder = gxhash::GxBuildHasher;
#[cfg(not(feature = "shared-gx"))]
/// Portable two-lane hash state shared by routing and frozen indexing.
///
/// Independent randomized Rapidhash lanes provide the full 128-bit digest
/// expected by the frozen perfect hash without paying for a second hashing
/// algorithm on every lookup. Ordinary overlay hash tables use the route lane.
#[derive(Clone, Debug, Default)]
pub struct GenerationHashBuilder {
    route: RapidRandomState,
    digest: RapidRandomState,
}

#[cfg(not(feature = "shared-gx"))]
impl BuildHasher for GenerationHashBuilder {
    type Hasher = <RapidRandomState as BuildHasher>::Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.route.build_hasher()
    }
}

#[cfg(feature = "shared-gx")]
#[derive(Clone, Copy)]
pub(crate) struct GenerationKeyHash {
    digest: Digest,
}

#[cfg(not(feature = "shared-gx"))]
#[derive(Clone, Copy)]
pub(crate) struct GenerationKeyHash {
    digest: Digest,
}

impl GenerationKeyHash {
    pub(crate) fn new(builder: &GenerationHashBuilder, key: &[u8]) -> Self {
        #[cfg(feature = "shared-gx")]
        {
            let mut hasher: GxHasher = builder.build_hasher();
            key.hash(&mut hasher);
            Self {
                digest: Digest(hasher.finish_u128()),
            }
        }

        #[cfg(not(feature = "shared-gx"))]
        {
            let route = builder.route.hash_one(key);
            let upper = builder.digest.hash_one(key);
            Self {
                digest: Digest(u128::from(route) | (u128::from(upper) << 64)),
            }
        }
    }

    /// Computes only the lane used for writer stripes and mutable overlays.
    ///
    /// Mutable-only generations do not need the second frozen-map digest
    /// lane. Readers can use this route immediately and defer the remaining
    /// hash work until an exact frozen-base lookup is actually required.
    #[cfg(not(feature = "shared-gx"))]
    pub(crate) fn route_for(builder: &GenerationHashBuilder, key: &[u8]) -> u64 {
        builder.route.hash_one(key)
    }

    /// Completes a full frozen-map digest after `route` was already computed.
    #[cfg(not(feature = "shared-gx"))]
    pub(crate) fn from_verified_route(
        builder: &GenerationHashBuilder,
        key: &[u8],
        route: u64,
    ) -> Self {
        let upper = builder.digest.hash_one(key);
        Self {
            digest: Digest(u128::from(route) | (u128::from(upper) << 64)),
        }
    }

    pub(crate) fn route(self) -> u64 {
        #[cfg(feature = "shared-gx")]
        {
            u64::try_from(self.digest.0 & u128::from(u64::MAX))
                .expect("masked digest half fits u64")
        }

        #[cfg(not(feature = "shared-gx"))]
        {
            u64::try_from(self.digest.0 & u128::from(u64::MAX))
                .expect("masked digest half fits u64")
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(crate) const fn frozen(self) -> Option<Digest> {
        #[cfg(feature = "shared-gx")]
        {
            Some(self.digest)
        }

        #[cfg(not(feature = "shared-gx"))]
        {
            Some(self.digest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GenerationKeyHash;

    #[test]
    fn carried_hash_context_fits_one_digest() {
        #[cfg(feature = "shared-gx")]
        assert_eq!(
            std::mem::size_of::<GenerationKeyHash>(),
            std::mem::size_of::<u128>()
        );
        #[cfg(not(feature = "shared-gx"))]
        assert_eq!(
            std::mem::size_of::<GenerationKeyHash>(),
            std::mem::size_of::<u128>()
        );
    }
}
