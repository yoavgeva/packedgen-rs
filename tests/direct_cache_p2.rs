//! Regression coverage for bounded-cache correctness and counter sharding.

use std::sync::{Arc, Barrier};
use std::thread;

use packedgen::{CacheConfig, DirectPackedCache, PackedCache};

#[test]
fn shared_key_read_counters_aggregate_exactly() {
    let config = CacheConfig::new(u64::MAX).with_overlay_capacity(128);
    let direct = Arc::new(DirectPackedCache::try_new(config.clone()).unwrap());
    let packed = Arc::new(PackedCache::try_new(config).unwrap());
    direct
        .insert_discard_with_options(b"hit", 1_u64, 1, None)
        .unwrap();
    packed.insert_with_options(b"hit", 1_u64, 1, None).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    thread::scope(|scope| {
        for _ in 0..8 {
            let direct = Arc::clone(&direct);
            let packed = Arc::clone(&packed);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                for read in 0..10_000 {
                    let hit = read % 2 == 0;
                    let key = if hit {
                        b"hit".as_slice()
                    } else {
                        b"miss".as_slice()
                    };
                    assert_eq!(direct.get(key).is_some(), hit);
                    assert_eq!(packed.get(key).is_some(), hit);
                }
            });
        }
    });
    for stats in [direct.stats(), packed.stats()] {
        assert_eq!(stats.hits, 40_000);
        assert_eq!(stats.misses, 40_000);
    }
}
