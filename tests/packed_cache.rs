//! Correctness and concurrency coverage for the arbitrary-value cache layer.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{
    CacheAdmissionOutcome, CacheConfig, CacheInsertError, CacheInsertOutcome, CacheWriteOutcome,
    PackedCache,
};

fn cache<V>(max_weight: u64) -> PackedCache<V> {
    PackedCache::try_new(
        CacheConfig::new(max_weight)
            .with_overlay_capacity(1_024)
            .with_arena_partitions(4)
            .with_eviction_batch(8),
    )
    .unwrap()
}

#[derive(Debug, Eq, PartialEq)]
struct NotClone(String);

#[test]
fn stores_arbitrary_non_clone_values() {
    let cache = cache(1_024);
    assert!(matches!(
        cache.insert(b"name", NotClone("first".into())).unwrap(),
        CacheInsertOutcome::Inserted
    ));
    let first = cache.get(b"name").unwrap();
    assert_eq!(&*first, &NotClone("first".into()));

    let outcome = cache.insert(b"name", NotClone("second".into())).unwrap();
    let CacheInsertOutcome::Replaced(previous) = outcome else {
        panic!("existing key must be replaced");
    };
    assert_eq!(&*previous, &NotClone("first".into()));
    assert_eq!(&*first, &NotClone("first".into()));
    assert_eq!(&*cache.get(b"name").unwrap(), &NotClone("second".into()));

    let removed = cache.remove(b"name").unwrap();
    assert_eq!(&*removed, &NotClone("second".into()));
    assert!(cache.get(b"name").is_none());
}

#[test]
fn expiration_is_lazy_and_exact() {
    let cache = cache(1_024);
    cache
        .insert_with_options(b"expired", 7_u64, 1, Some(Duration::ZERO))
        .unwrap();
    cache.insert_with_options(b"live", 9_u64, 1, None).unwrap();

    assert!(cache.get(b"expired").is_none());
    assert_eq!(*cache.get(b"live").unwrap(), 9);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.stats().expirations, 1);
}

#[test]
fn touch_changes_expiration() {
    let cache = cache(1_024);
    cache.insert_with_options(b"key", 11_u64, 1, None).unwrap();
    assert!(cache.touch(b"key", Some(Duration::ZERO)));
    assert!(cache.get(b"key").is_none());
    assert!(!cache.touch(b"missing", None));
}

#[test]
fn replace_only_keeps_absent_keys_absent() {
    let cache = cache(1_024);
    assert!(
        !cache
            .replace_discard_with_options(b"key", 7_u64, 1, None)
            .unwrap()
    );
    assert!(cache.is_empty());

    cache.insert_with_options(b"key", 9_u64, 3, None).unwrap();
    assert!(
        cache
            .replace_discard_with_options(b"key", 11_u64, 5, None)
            .unwrap()
    );
    assert_eq!(*cache.get(b"key").unwrap(), 11);
    assert_eq!(cache.weight(), 5);
}

#[test]
fn pinned_touch_changes_expiration() {
    let cache = cache(1_024);
    cache.insert_with_options(b"key", 11_u64, 1, None).unwrap();
    let guard = cache.pin();
    assert!(guard.touch(b"key", Some(Duration::ZERO)));
    assert!(guard.get(b"key").is_none());
    assert!(!guard.touch(b"missing", None));
}

#[test]
fn pinned_read_statistics_flush_without_counting_untracked_reads() {
    let cache = cache(1_024);
    cache.insert_with_options(b"key", 11_u64, 1, None).unwrap();
    {
        let mut guard = cache.pin();
        assert_eq!(*guard.get(b"key").unwrap(), 11);
        assert!(guard.get(b"missing").is_none());
        assert_eq!(cache.stats().hits, 0);
        assert_eq!(cache.stats().misses, 0);
        guard.refresh();
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
        assert!(guard.get_untracked(b"key").is_some());
        assert!(guard.get_untracked(b"missing").is_none());
    }
    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().misses, 1);
}

#[test]
fn pinned_admission_statistics_flush_without_counting_untracked_admission() {
    let cache = cache(1_024);
    {
        let mut guard = cache.pin();
        assert_eq!(
            guard
                .insert_if_absent_with_options(b"tracked", 1_u64, 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
        assert_eq!(cache.stats().inserts, 0);
        guard.refresh();
        assert_eq!(cache.stats().inserts, 1);
        assert_eq!(
            cache
                .insert_if_absent_untracked_with_options(b"untracked", 2, 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    assert_eq!(cache.stats().inserts, 1);
}

#[test]
fn weight_pressure_evicts_the_older_entry() {
    let cache = cache(2);
    cache.insert_with_options(b"a", 1_u64, 1, None).unwrap();
    cache.insert_with_options(b"b", 2_u64, 1, None).unwrap();
    assert_eq!(*cache.get(b"a").unwrap(), 1);
    cache.insert_with_options(b"c", 3_u64, 1, None).unwrap();

    assert_eq!(cache.weight(), 2);
    assert_eq!(cache.len(), 2);
    assert_eq!(*cache.get(b"a").unwrap(), 1);
    assert!(cache.get(b"b").is_none());
    assert_eq!(*cache.get(b"c").unwrap(), 3);
    assert_eq!(cache.stats().evictions, 1);
}

#[test]
fn held_value_survives_capacity_eviction() {
    let cache = cache(1);
    cache.insert_with_options(b"old", 7_u64, 1, None).unwrap();
    let held = cache.get(b"old").unwrap();
    cache.insert_with_options(b"new", 9_u64, 1, None).unwrap();
    assert!(cache.get(b"old").is_none());
    assert_eq!(*held, 7);
    assert_eq!(*cache.get(b"new").unwrap(), 9);
}

#[test]
fn entry_limit_is_enforced_alongside_weight() {
    let cache = PackedCache::try_new(
        CacheConfig::new(1_000)
            .with_max_entries(2)
            .with_overlay_capacity(128),
    )
    .unwrap();
    for index in 0..3_u64 {
        cache
            .insert_with_options(&index.to_le_bytes(), index, 1, None)
            .unwrap();
    }
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.stats().evictions, 1);
}

#[test]
fn rejects_an_item_larger_than_the_cache() {
    let cache = cache::<u64>(10);
    assert!(matches!(
        cache.insert_with_options(b"large", 1, 11, None),
        Err(CacheInsertError::ItemTooHeavy {
            weight: 11,
            maximum: 10,
        })
    ));
    assert!(cache.is_empty());
    assert_eq!(cache.stats().rejected, 1);
}

#[test]
fn rejects_weight_that_does_not_fit_compact_accounting() {
    let cache = cache::<u64>(u64::MAX);
    let weight = u64::from(u32::MAX) + 1;
    assert!(matches!(
        cache.insert_with_options(b"large", 1, weight, None),
        Err(CacheInsertError::WeightNotCompact { weight })
            if weight == u64::from(u32::MAX) + 1
    ));
    assert!(cache.is_empty());
    assert_eq!(cache.stats().rejected, 1);
}

#[test]
fn pinned_value_survives_replacement_until_refresh() {
    let cache = cache(1_024);
    cache.insert_with_options(b"key", 1_u64, 1, None).unwrap();
    let mut guard = cache.pin();
    let held = guard.get(b"key").unwrap();
    assert_eq!(
        cache
            .insert_discard_with_options(b"key", 2, 1, None)
            .unwrap(),
        CacheWriteOutcome::Replaced
    );
    assert_eq!(*held, 1);
    assert_eq!(*guard.get(b"key").unwrap(), 2);
    guard.refresh();
    assert_eq!(*guard.get(b"key").unwrap(), 2);
}

#[test]
fn owned_value_keeps_reclamation_state_alive_after_cache_drop() {
    let held = {
        let cache = cache(1_024);
        cache.insert_with_options(b"key", 41_u64, 1, None).unwrap();
        cache.get(b"key").unwrap()
    };
    assert_eq!(*held, 41);
}

#[test]
fn discard_remove_reports_presence() {
    let cache = cache(1_024);
    cache.insert_with_options(b"key", 1_u64, 1, None).unwrap();
    assert!(cache.remove_discard(b"key"));
    assert!(!cache.remove_discard(b"key"));
    assert_eq!(cache.stats().removals, 1);
}

#[test]
fn concurrent_admission_keeps_exactly_one_winner() {
    let cache = Arc::new(cache(1_024));
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for value in 0..8_u64 {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            cache
                .insert_if_absent_with_options(b"shared", value, 1, None)
                .unwrap()
        }));
    }
    barrier.wait();
    let inserted = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|outcome| *outcome == CacheAdmissionOutcome::Inserted)
        .count();
    assert_eq!(inserted, 1);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 1);
    assert!(*cache.get(b"shared").unwrap() < 8);
}

#[test]
fn maintenance_purges_expired_items() {
    let cache = cache(1_024);
    for index in 0..20_u64 {
        cache
            .insert_with_options(&index.to_le_bytes(), index, 1, Some(Duration::ZERO))
            .unwrap();
    }
    let result = cache.maintain().unwrap();
    assert_eq!(result.expired, 20);
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.stats().expirations, 20);
}

#[test]
fn concurrent_replacement_and_reads_keep_values_owned() {
    let cache = Arc::new(cache(1_000_000));
    cache
        .insert_with_options(b"shared", 0_u64, 1, None)
        .unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for worker in 0..8_u64 {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            for operation in 0..5_000_u64 {
                let value = worker * 5_000 + operation + 1;
                cache
                    .insert_with_options(b"shared", value, 1, None)
                    .unwrap();
                let held = cache.get(b"shared").unwrap();
                assert!(*held <= 40_000);
            }
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 1);
    assert!(cache.get(b"shared").is_some());
}

#[test]
fn concurrent_capacity_pressure_remains_bounded() {
    let cache = Arc::new(
        PackedCache::try_new(
            CacheConfig::new(1_000_000)
                .with_max_entries(128)
                .with_overlay_capacity(2_048)
                .with_eviction_batch(16),
        )
        .unwrap(),
    );
    let mut workers = Vec::new();
    for worker in 0..4_u64 {
        let cache = Arc::clone(&cache);
        workers.push(thread::spawn(move || {
            for operation in 0..250_u64 {
                let key = (worker * 250 + operation).to_le_bytes();
                cache.insert_with_options(&key, operation, 1, None).unwrap();
                let _ = cache.get(&key);
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(cache.len() <= 128);
    assert_eq!(cache.weight(), cache.len() as u64);
    assert!(cache.stats().evictions >= 872);
}

#[test]
fn background_worker_can_be_started_and_stopped() {
    let cache = Arc::new(cache(1_024));
    cache
        .insert_with_options(b"expired", 1_u64, 1, Some(Duration::ZERO))
        .unwrap();
    let maintenance = cache.spawn_maintenance(Duration::from_millis(1));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !cache.is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    maintenance.shutdown();
    assert!(cache.is_empty());
}
