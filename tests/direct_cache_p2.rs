//! Regression coverage for bounded-cache correctness and counter sharding.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

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

fn observe_ttl_lifetimes(
    mut insert: impl FnMut(&[u8], Duration),
    mut present: impl FnMut(&[u8]) -> bool,
) {
    let ttl = Duration::from_millis(150);
    let mut inserted = Vec::new();
    for key in 0_u64..20 {
        let key = key.to_le_bytes();
        let started = Instant::now();
        insert(&key, ttl);
        inserted.push((key, started, false));
        thread::sleep(Duration::from_millis(7));
    }
    let poll_started = Instant::now();
    while inserted.iter().any(|(_, _, missing)| !missing) {
        assert!(
            poll_started.elapsed() < Duration::from_secs(3),
            "entries must eventually expire"
        );
        for (key, started, missing) in &mut inserted {
            if !*missing && !present(key) {
                assert!(
                    started.elapsed() >= ttl,
                    "entry expired before its TTL: {:?}",
                    started.elapsed()
                );
                *missing = true;
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    insert(b"zero", Duration::ZERO);
    assert!(!present(b"zero"), "zero TTL must expire immediately");
}

#[test]
fn direct_ttl_never_expires_early() {
    let cache =
        DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(128)).unwrap();
    observe_ttl_lifetimes(
        |key, ttl| {
            cache
                .insert_discard_with_options(key, 1_u64, 1, Some(ttl))
                .unwrap();
        },
        |key| cache.get(key).is_some(),
    );
}

#[test]
fn packed_ttl_never_expires_early() {
    let cache =
        PackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(128)).unwrap();
    observe_ttl_lifetimes(
        |key, ttl| {
            cache.insert_with_options(key, 1_u64, 1, Some(ttl)).unwrap();
        },
        |key| cache.get(key).is_some(),
    );
}
