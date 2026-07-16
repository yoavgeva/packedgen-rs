use std::hash::BuildHasher;

#[cfg(feature = "shared-gx")]
use gxhash::GxHasher;
#[cfg(not(feature = "shared-gx"))]
use hashbrown::DefaultHashBuilder;
#[cfg(feature = "shared-gx")]
use std::hash::Hash;

use crate::frozen_map::Digest;

#[cfg(feature = "shared-gx")]
/// Hash state shared by atomic writer routing and frozen-key indexing.
pub type GenerationHashBuilder = gxhash::GxBuildHasher;
#[cfg(not(feature = "shared-gx"))]
/// Default split-schedule hash state used by atomic writer routing.
pub type GenerationHashBuilder = DefaultHashBuilder;

#[cfg(feature = "shared-gx")]
#[derive(Clone, Copy)]
pub(crate) struct GenerationKeyHash {
    digest: Digest,
}

#[cfg(not(feature = "shared-gx"))]
#[derive(Clone, Copy)]
pub(crate) struct GenerationKeyHash {
    route: u64,
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
            Self {
                route: builder.hash_one(key),
            }
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
            self.route
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
            let _ = self.route;
            None
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
            std::mem::size_of::<u64>()
        );
    }
}
