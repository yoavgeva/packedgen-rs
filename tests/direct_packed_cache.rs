#![allow(missing_docs)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{
    CacheAdmissionOutcome, CacheBuildError, CacheConfig, CacheConfigError, CacheWriteOutcome,
    DirectPackedCache, FrozenBuildError,
};

fn cache(max_weight: u64, max_entries: usize) -> DirectPackedCache<Vec<u8>> {
    DirectPackedCache::try_new(
        CacheConfig::new(max_weight)
            .with_max_entries(max_entries)
            .with_overlay_capacity(max_entries.max(1)),
    )
    .unwrap()
}

#[test]
fn exact_binary_crud_and_accounting() {
    let cache = cache(1_024, 16);
    let key = [0, 255, 0, 7];

    assert_eq!(
        cache
            .insert_discard_with_options(&key, vec![1, 2, 3], 11, None)
            .unwrap(),
        CacheWriteOutcome::Inserted
    );
    assert_eq!(cache.get(&key).unwrap().as_slice(), [1, 2, 3]);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 11);
    assert!(
        !cache
            .replace_discard_with_options(b"missing", vec![0], 1, None)
            .unwrap()
    );
    assert!(
        cache
            .replace_discard_with_options(&key, vec![6, 7], 9, None)
            .unwrap()
    );
    assert_eq!(cache.peek(&key).unwrap().as_slice(), [6, 7]);
    assert_eq!(cache.weight(), 9);
    assert_eq!(
        cache
            .insert_discard_with_options(&key, vec![9, 8], 7, None)
            .unwrap(),
        CacheWriteOutcome::Replaced
    );
    assert_eq!(cache.peek(&key).unwrap().as_slice(), [9, 8]);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 7);
    assert_eq!(cache.remove(&key).unwrap().as_slice(), [9, 8]);
    assert!(cache.is_empty());
    assert_eq!(cache.weight(), 0);

    cache
        .insert_discard_with_options(&key, vec![4, 5], 13, None)
        .unwrap();
    let guard = cache.pin();
    assert!(guard.remove_discard(&key));
    assert!(!guard.remove_discard(&key));
    assert!(cache.is_empty());
    assert_eq!(cache.weight(), 0);
}

#[test]
fn guarded_reads_flush_exact_hit_and_miss_counts() {
    let cache = cache(1_024, 16);
    cache
        .insert_discard_with_options(b"present", vec![1], 1, None)
        .unwrap();

    let guard = cache.pin();
    assert!(guard.get(b"present").is_some());
    assert!(guard.get(b"missing").is_none());
    drop(guard);

    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().misses, 1);
}

#[test]
fn admission_batch_enforces_limits_when_it_leaves_scope() {
    let cache = cache(u64::MAX, 2);
    let mut guard = cache.pin();

    {
        let mut batch = guard.admission_batch();
        for key in [b"one".as_slice(), b"two", b"three"] {
            assert_eq!(
                batch
                    .insert_if_absent_with_options(key, vec![1; 8], 8, None)
                    .unwrap(),
                CacheAdmissionOutcome::Inserted
            );
        }
        assert_eq!(cache.len(), 3);
    }

    assert!(cache.len() <= 2);
    assert!(guard.get_untracked(b"three").is_some());
}

#[test]
fn admission_batch_preserves_conditional_insert_semantics() {
    let cache = cache(u64::MAX, 8);
    let mut guard = cache.pin();
    let mut batch = guard.admission_batch();

    assert_eq!(
        batch
            .insert_if_absent_with_options(b"same", vec![1], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert_eq!(
        batch
            .insert_if_absent_with_options(b"same", vec![2], 2, None)
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );
    drop(batch);

    assert_eq!(guard.get_untracked(b"same").unwrap().as_slice(), [1]);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 1);
}

#[test]
fn doorkeeper_rejects_one_off_pressure_and_admits_a_repeat() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(2)
            .with_overlay_capacity(16)
            .with_admission_doorkeeper(2),
    )
    .unwrap();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        assert_eq!(
            cache
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }

    assert_eq!(
        cache
            .insert_if_absent_with_options(b"scan", vec![2], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert!(cache.peek(b"scan").is_none());
    assert_eq!(cache.len(), 2);

    assert_eq!(
        cache
            .insert_if_absent_with_options(b"scan", vec![3], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert!(cache.peek(b"scan").is_some());
    assert_eq!(cache.len(), 2);
}

#[test]
fn frequency_admission_requires_more_evidence_than_frequent_victims() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(2)
            .with_overlay_capacity(32)
            .with_eviction_batch(2)
            .with_admission_doorkeeper(2)
            .with_frequency_admission(5),
    )
    .unwrap();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        assert_eq!(
            cache
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    for _ in 0..4 {
        for key in [b"a".as_slice(), b"b".as_slice()] {
            assert_eq!(
                cache
                    .insert_if_absent_with_options(key, vec![2], 1, None)
                    .unwrap(),
                CacheAdmissionOutcome::Existing
            );
        }
    }
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![3], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![4], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );

    for attempt in 1..=5 {
        assert_eq!(
            cache
                .insert_if_absent_with_options(b"d", vec![attempt], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Rejected,
            "candidate attempt {attempt}"
        );
    }
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"d", vec![6], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert!(cache.len() <= 2);
}

#[test]
fn frequency_admission_preserves_a_sampled_hot_resident_during_churn() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(8)
            .with_overlay_capacity(64)
            .with_eviction_batch(8)
            .with_admission_doorkeeper(8)
            .with_frequency_admission(2),
    )
    .unwrap();
    let resident_keys = [
        b"hot".as_slice(),
        b"cold-1".as_slice(),
        b"cold-2".as_slice(),
        b"cold-3".as_slice(),
        b"cold-4".as_slice(),
        b"cold-5".as_slice(),
        b"cold-6".as_slice(),
        b"cold-7".as_slice(),
    ];
    for key in resident_keys {
        assert_eq!(
            cache
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }

    let admit = |key: &[u8]| {
        for attempt in 0..8 {
            match cache
                .insert_if_absent_with_options(key, vec![attempt], 1, None)
                .unwrap()
            {
                CacheAdmissionOutcome::Inserted | CacheAdmissionOutcome::Existing => return,
                CacheAdmissionOutcome::Rejected => {}
            }
        }
        panic!("candidate was never admitted");
    };

    admit(b"first");
    let hot = resident_keys
        .into_iter()
        .find(|key| cache.peek(key).is_some())
        .unwrap();
    let guard = cache.pin();
    for _ in 0..64 {
        assert!(guard.get(hot).is_some());
    }
    admit(b"clear-clock");
    assert!(guard.get(hot).is_some());

    for candidate in 0_u8..16 {
        admit(&[b'x', candidate]);
        assert!(
            guard.get(hot).is_some(),
            "hot resident lost after candidate {candidate}"
        );
    }
}

#[test]
fn adaptive_frequency_admission_falls_back_for_low_reuse_traffic() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(2)
            .with_overlay_capacity(32)
            .with_eviction_batch(2)
            .with_admission_doorkeeper(2)
            .with_adaptive_frequency_admission(5, 2_500),
    )
    .unwrap();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        cache
            .insert_if_absent_with_options(key, vec![1], 1, None)
            .unwrap();
    }
    for _ in 0..4 {
        for key in [b"a".as_slice(), b"b".as_slice()] {
            assert_eq!(
                cache
                    .insert_if_absent_with_options(key, vec![2], 1, None)
                    .unwrap(),
                CacheAdmissionOutcome::Existing
            );
        }
    }
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![3], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![4], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    for _ in 0..1_024 {
        assert!(cache.get(b"d").is_none());
    }
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"d", vec![5], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"d", vec![6], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
}

#[test]
fn adaptive_frequency_admission_activates_after_reuse() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(2)
            .with_overlay_capacity(32)
            .with_eviction_batch(2)
            .with_admission_doorkeeper(2)
            .with_adaptive_frequency_admission(5, 2_500),
    )
    .unwrap();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        cache
            .insert_if_absent_with_options(key, vec![1], 1, None)
            .unwrap();
    }
    let mut guard = cache.pin();
    for _ in 0..1_024 {
        assert!(guard.get(b"a").is_some());
    }
    guard.refresh();
    for _ in 0..4 {
        for key in [b"a".as_slice(), b"b".as_slice()] {
            assert_eq!(
                cache
                    .insert_if_absent_with_options(key, vec![2], 1, None)
                    .unwrap(),
                CacheAdmissionOutcome::Existing
            );
        }
    }
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![3], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"c", vec![4], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"d", vec![5], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        cache
            .insert_if_absent_with_options(b"d", vec![6], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
}

#[test]
fn adaptive_frequency_admission_rejects_an_invalid_hit_rate() {
    let result = DirectPackedCache::<Vec<u8>>::try_new(
        CacheConfig::new(1_024)
            .with_admission_doorkeeper(16)
            .with_adaptive_frequency_admission(1, 10_001),
    );
    assert!(matches!(
        result,
        Err(CacheBuildError::Config(
            CacheConfigError::InvalidFrequencyHitRate
        ))
    ));
}

#[test]
fn tiered_frequency_admission_rejects_a_weaker_or_earlier_high_tier() {
    for config in [
        CacheConfig::new(1_024)
            .with_admission_doorkeeper(16)
            .with_tiered_frequency_admission(2, 6_000, 1, 7_500),
        CacheConfig::new(1_024)
            .with_admission_doorkeeper(16)
            .with_tiered_frequency_admission(2, 6_000, 8, 5_999),
    ] {
        assert!(matches!(
            DirectPackedCache::<Vec<u8>>::try_new(config),
            Err(CacheBuildError::Config(
                CacheConfigError::InvalidFrequencyTier
            ))
        ));
    }
}

#[test]
fn tiered_frequency_admission_rejects_an_invalid_high_hit_rate() {
    let result = DirectPackedCache::<Vec<u8>>::try_new(
        CacheConfig::new(1_024)
            .with_admission_doorkeeper(16)
            .with_tiered_frequency_admission(2, 6_000, 8, 10_001),
    );
    assert!(matches!(
        result,
        Err(CacheBuildError::Config(
            CacheConfigError::InvalidFrequencyHitRate
        ))
    ));
}

#[test]
fn doorkeeper_applies_to_guarded_admission_batches() {
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(2)
            .with_overlay_capacity(16)
            .with_admission_doorkeeper(2),
    )
    .unwrap();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        cache
            .insert_if_absent_with_options(key, vec![1], 1, None)
            .unwrap();
    }
    let mut guard = cache.pin();
    let mut batch = guard.admission_batch();
    assert_eq!(
        batch
            .insert_if_absent_with_options(b"scan", vec![2], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Rejected
    );
    assert_eq!(
        batch
            .insert_if_absent_with_options(b"scan", vec![3], 1, None)
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    drop(batch);
    assert!(cache.peek(b"scan").is_some());
    assert!(cache.len() <= 2);
}

#[test]
fn doorkeeper_requires_a_nonzero_population_hint() {
    let result =
        DirectPackedCache::<Vec<u8>>::try_new(CacheConfig::new(1_024).with_admission_doorkeeper(0));
    assert!(matches!(
        result,
        Err(CacheBuildError::Config(
            CacheConfigError::ZeroAdmissionDoorkeeperEntries
        ))
    ));
}

#[test]
fn concurrent_doorkeeper_rotation_preserves_capacity_and_repeat_admission() {
    const CAPACITY: usize = 128;
    const THREADS: usize = 4;
    const KEYS: usize = 512;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(CAPACITY)
                .with_overlay_capacity(4_096)
                .with_admission_doorkeeper(CAPACITY),
        )
        .unwrap(),
    );
    for key in 0_u64..u64::try_from(CAPACITY).unwrap() {
        cache
            .insert_if_absent_with_options(&key.to_le_bytes(), vec![0], 1, None)
            .unwrap();
    }

    let mut joins = Vec::new();
    for worker in 0..THREADS {
        let cache = Arc::clone(&cache);
        joins.push(thread::spawn(move || {
            for round in 0..2 {
                for index in 0..KEYS {
                    let key = (u64::try_from(worker).unwrap() << 48
                        | u64::try_from(index).unwrap()
                        | 1_u64 << 32)
                        .to_le_bytes();
                    let outcome = cache
                        .insert_if_absent_with_options(
                            &key,
                            vec![u8::try_from(round).unwrap()],
                            1,
                            None,
                        )
                        .unwrap();
                    assert!(matches!(
                        outcome,
                        CacheAdmissionOutcome::Inserted
                            | CacheAdmissionOutcome::Existing
                            | CacheAdmissionOutcome::Rejected
                    ));
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }

    cache.maintain().unwrap();
    assert!(cache.len() <= CAPACITY);
    assert!(cache.stats().rejected > 0);
    assert!(cache.stats().evictions > 0);
}

#[test]
fn unbounded_admission_batch_publishes_length_immediately() {
    let cache =
        DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(128)).unwrap();
    let mut guard = cache.pin();
    let mut batch = guard.admission_batch();

    for key in [b"one".as_slice(), b"two", b"three"] {
        assert_eq!(
            batch
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    assert_eq!(cache.len(), 3);
    assert_eq!(
        batch
            .insert_if_absent_with_options(b"two", vec![2], 2, None)
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );

    drop(batch);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.weight(), 3);
}

#[test]
fn unbounded_bulk_admission_publishes_exact_length_at_scope_exit() {
    let cache =
        DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(128)).unwrap();
    let mut guard = cache.pin();
    let mut batch = guard.bulk_admission_batch();

    for key in [b"one".as_slice(), b"two", b"three"] {
        assert_eq!(
            batch
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
        assert!(cache.get(key).is_some());
    }
    assert_eq!(cache.len(), 0);
    assert_eq!(
        batch
            .insert_if_absent_with_options(b"two", vec![2], 2, None)
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );

    drop(batch);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.weight(), 3);
}

#[cfg(feature = "operation-batch")]
#[test]
fn bulk_admission_preserves_every_short_key_width_after_adaptive_learning() {
    const WARM_KEYS: usize = 96;
    let cache = DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(4_096))
        .unwrap();
    let mut guard = cache.pin();
    let mut batch = guard.bulk_admission_batch();

    for index in 0..WARM_KEYS {
        let mut key = [0_u8; 7];
        key[0] = 0xf0;
        key[1..].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes()[..6]);
        assert_eq!(
            batch
                .insert_if_absent_with_options(&key, vec![0xff], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }

    let keys = (0..=32)
        .map(|bytes| {
            let mut key = vec![u8::try_from(bytes).unwrap(); bytes];
            if let Some(first) = key.first_mut() {
                *first ^= 0x80;
            }
            key
        })
        .collect::<Vec<_>>();
    for (bytes, key) in keys.iter().enumerate() {
        assert_eq!(
            batch
                .insert_if_absent_with_options(key, vec![u8::try_from(bytes).unwrap()], 1, None,)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
        assert_eq!(
            cache.get(key).unwrap().as_slice(),
            [u8::try_from(bytes).unwrap()]
        );
        assert_eq!(
            batch
                .insert_if_absent_with_options(key, vec![0xee], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Existing
        );
    }

    drop(batch);
    assert_eq!(cache.len(), WARM_KEYS + keys.len());
    assert_eq!(cache.weight(), (WARM_KEYS + keys.len()) as u64);
}

#[test]
fn long_unbounded_bulk_admission_flushes_before_guard_refresh() {
    const ADMISSIONS: usize = 600;
    const REFRESH_INTERVAL: usize = 512;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX).with_overlay_capacity(ADMISSIONS + 1_024),
    )
    .unwrap();
    let mut guard = cache.pin();
    let mut batch = guard.bulk_admission_batch();
    for key in 0..ADMISSIONS as u64 {
        assert_eq!(
            batch
                .insert_if_absent_with_options(&key.to_le_bytes(), vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    assert_eq!(cache.len(), REFRESH_INTERVAL);
    drop(batch);
    assert_eq!(cache.len(), ADMISSIONS);
}

#[test]
fn bounded_bulk_admission_keeps_public_capacity_immediate() {
    let cache = cache(u64::MAX, 8);
    let mut guard = cache.pin();
    let mut batch = guard.bulk_admission_batch();
    for (index, key) in [b"one".as_slice(), b"two", b"three"]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            batch
                .insert_if_absent_with_options(key, vec![1], 1, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
        assert_eq!(cache.len(), index + 1);
    }
    drop(batch);
    assert_eq!(cache.len(), 3);
}

#[test]
fn adaptive_admission_skips_existing_value_construction_and_relearns_after_a_miss() {
    let cache = cache(u64::MAX, 8);
    let guard = cache.pin();
    let mut admission = guard.adaptive_admission();
    let constructions = AtomicUsize::new(0);

    assert_eq!(
        admission
            .insert_if_absent_with_options_by(
                b"same",
                || {
                    constructions.fetch_add(1, Ordering::Relaxed);
                    vec![1]
                },
                1,
                None,
            )
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert_eq!(
        admission
            .insert_if_absent_with_options(b"same", vec![2], 2, None)
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );
    assert_eq!(
        admission
            .insert_if_absent_with_options(b"same", vec![3], 3, None)
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );
    assert_eq!(
        admission
            .insert_if_absent_with_options_by(
                b"same",
                || {
                    constructions.fetch_add(1, Ordering::Relaxed);
                    vec![4]
                },
                3,
                None,
            )
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );
    assert_eq!(constructions.load(Ordering::Relaxed), 1);

    assert_eq!(
        admission
            .insert_if_absent_with_options_by(
                b"new",
                || {
                    constructions.fetch_add(1, Ordering::Relaxed);
                    vec![5]
                },
                4,
                None,
            )
            .unwrap(),
        CacheAdmissionOutcome::Inserted
    );
    assert_eq!(constructions.load(Ordering::Relaxed), 2);
    for expected_constructions in [3, 4] {
        assert_eq!(
            admission
                .insert_if_absent_with_options_by(
                    b"new",
                    || {
                        constructions.fetch_add(1, Ordering::Relaxed);
                        vec![6]
                    },
                    4,
                    None,
                )
                .unwrap(),
            CacheAdmissionOutcome::Existing
        );
        assert_eq!(
            constructions.load(Ordering::Relaxed),
            expected_constructions
        );
    }
    assert_eq!(
        admission
            .insert_if_absent_with_options_by(
                b"new",
                || {
                    constructions.fetch_add(1, Ordering::Relaxed);
                    vec![7]
                },
                4,
                None,
            )
            .unwrap(),
        CacheAdmissionOutcome::Existing
    );
    assert_eq!(constructions.load(Ordering::Relaxed), 4);
    assert_eq!(guard.get_untracked(b"same").unwrap().as_slice(), [1]);
    assert_eq!(guard.get_untracked(b"new").unwrap().as_slice(), [5]);
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.weight(), 5);
}

#[test]
fn concurrent_admission_batches_restore_the_shared_capacity_limit() {
    const THREADS: usize = 8;
    const ADMISSIONS: usize = 512;
    const CAPACITY: usize = 257;

    let cache = Arc::new(cache(u64::MAX, CAPACITY));
    let start = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for writer in 0..THREADS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let mut guard = cache.pin();
            start.wait();
            for chunk in (0..ADMISSIONS).step_by(32) {
                let mut batch = guard.admission_batch();
                for admission in chunk..(chunk + 32).min(ADMISSIONS) {
                    let key = ((writer as u64) << 32 | admission as u64).to_le_bytes();
                    batch
                        .insert_if_absent_with_options(&key, vec![1], 1, None)
                        .unwrap();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }

    assert!(cache.len() <= CAPACITY);
    assert_eq!(cache.weight(), cache.len() as u64);
}

#[test]
fn concurrent_unbounded_bulk_admission_flushes_exact_total_length() {
    const THREADS: usize = 8;
    const ADMISSIONS: usize = 512;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX).with_overlay_capacity(THREADS * ADMISSIONS + 1_024),
        )
        .unwrap(),
    );
    let start = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for writer in 0..THREADS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let mut guard = cache.pin();
            let mut batch = guard.bulk_admission_batch();
            start.wait();
            for admission in 0..ADMISSIONS {
                let key = ((writer as u64) << 32 | admission as u64).to_le_bytes();
                assert_eq!(
                    batch
                        .insert_if_absent_with_options(&key, vec![1], 1, None)
                        .unwrap(),
                    CacheAdmissionOutcome::Inserted
                );
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }

    assert_eq!(cache.len(), THREADS * ADMISSIONS);
    assert_eq!(cache.weight(), (THREADS * ADMISSIONS) as u64);
}

#[test]
fn concurrent_unbounded_removal_batches_flush_exact_total_length() {
    const THREADS: usize = 8;
    const REMOVALS: usize = 600;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX).with_overlay_capacity(THREADS * REMOVALS + 1_024),
        )
        .unwrap(),
    );
    for worker in 0..THREADS {
        for removal in 0..REMOVALS {
            let key = ((worker as u64) << 32 | removal as u64).to_le_bytes();
            cache
                .insert_discard_with_options(&key, vec![1], 1, None)
                .unwrap();
        }
    }
    cache.maintain().unwrap();

    let start = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for worker in 0..THREADS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let mut guard = cache.pin();
            let mut batch = guard.removal_batch_untracked();
            start.wait();
            for removal in 0..REMOVALS {
                let key = ((worker as u64) << 32 | removal as u64).to_le_bytes();
                assert!(batch.remove_discard(&key));
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }

    assert!(cache.is_empty());
}

#[test]
fn long_admission_batch_self_flushes_before_scope_exit() {
    const CAPACITY: usize = 16;
    let cache = cache(u64::MAX, CAPACITY);
    for key in 0_u8..u8::try_from(CAPACITY).unwrap() {
        cache
            .insert_discard_with_options(&[key], vec![0], 1, None)
            .unwrap();
    }
    let mut guard = cache.pin();
    let mut batch = guard.admission_batch();
    for key in 0_u8..64 {
        batch
            .insert_if_absent_with_options(&[128 + key], vec![1], 1, None)
            .unwrap();
        assert!(cache.len() <= CAPACITY + 1);
    }
    drop(batch);
    assert!(cache.len() <= CAPACITY);
}

#[test]
fn disabled_capacity_dimensions_derive_exact_quiescent_totals() {
    let entry_only = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(8)
            .with_overlay_capacity(16),
    )
    .unwrap();
    entry_only
        .insert_discard_with_options(b"a", vec![1], 3, None)
        .unwrap();
    entry_only
        .insert_discard_with_options(b"b", vec![2], 5, None)
        .unwrap();
    assert_eq!(entry_only.len(), 2);
    assert_eq!(entry_only.weight(), 8);
    entry_only.remove_discard(b"a");
    assert_eq!(entry_only.len(), 1);
    assert_eq!(entry_only.weight(), 5);

    let weight_only =
        DirectPackedCache::try_new(CacheConfig::new(32).with_overlay_capacity(16)).unwrap();
    weight_only
        .insert_discard_with_options(b"a", vec![1], 7, None)
        .unwrap();
    weight_only
        .insert_discard_with_options(b"b", vec![2], 11, None)
        .unwrap();
    assert_eq!(weight_only.len(), 2);
    assert_eq!(weight_only.weight(), 18);
    weight_only.remove_discard(b"b");
    assert_eq!(weight_only.len(), 1);
    assert_eq!(weight_only.weight(), 7);
}

#[test]
fn guarded_admission_batches_keep_public_accounting_and_rebuild_exact() {
    let cache = cache(1_000_000, 1_024);
    let mut guard = cache.pin();

    for value in 0..512_u64 {
        assert_eq!(
            guard
                .insert_if_absent_with_options(
                    &value.to_le_bytes(),
                    value.to_le_bytes().to_vec(),
                    8,
                    None,
                )
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    for value in (0..512_u64).step_by(2) {
        assert!(guard.remove_discard(&value.to_le_bytes()));
    }

    assert_eq!(cache.len(), 256);
    assert_eq!(cache.weight(), 256 * 8);
    guard.refresh();
    drop(guard);
    cache.maintain().unwrap();
    assert_eq!(cache.len(), 256);
    assert_eq!(cache.weight(), 256 * 8);
    for value in 0..512_u64 {
        assert_eq!(
            cache.peek(&value.to_le_bytes()).is_some(),
            !value.is_multiple_of(2)
        );
    }
}

#[test]
fn refreshed_guard_does_not_block_adaptive_maintenance() {
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(512)
                .with_overlay_capacity(256),
        )
        .unwrap(),
    );
    let mut guard = cache.pin();
    for value in 0..192_u64 {
        assert_eq!(
            guard
                .insert_if_absent_with_options(
                    &value.to_le_bytes(),
                    value.to_le_bytes().to_vec(),
                    8,
                    None,
                )
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    for value in 0..100_u64 {
        assert!(guard.remove_discard(&value.to_le_bytes()));
    }
    guard.refresh();

    let rebuilding = Arc::clone(&cache);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let rebuild = thread::spawn(move || {
        done_tx
            .send(rebuilding.maintain())
            .expect("receiver remains live");
    });
    let result = done_rx
        .recv_timeout(Duration::from_millis(250))
        .expect("refresh must release the old guarded-admission generation");
    assert!(result.unwrap().rebuilt);
    rebuild.join().unwrap();
    drop(guard);
}

#[test]
fn alternating_live_arenas_and_dropped_owner_never_reuse_stale_slots() {
    let left = cache(u64::MAX, 8);
    let right = cache(u64::MAX, 8);
    for operation in 0_usize..4_096 {
        let target = if operation.is_multiple_of(2) {
            &left
        } else {
            &right
        };
        target
            .insert_discard_with_options(
                b"shared",
                vec![u8::try_from(operation & 255).unwrap(); 8],
                8,
                None,
            )
            .unwrap();
        assert_eq!(target.peek(b"shared").unwrap().len(), 8);
    }
    assert_eq!(left.len(), 1);
    assert_eq!(right.len(), 1);

    drop(left);
    for operation in 0_usize..2_048 {
        right
            .insert_discard_with_options(
                b"shared",
                vec![u8::try_from(operation & 255).unwrap(); 16],
                16,
                None,
            )
            .unwrap();
        assert_eq!(right.peek(b"shared").unwrap().len(), 16);
    }
    assert_eq!(right.len(), 1);
    assert_eq!(right.weight(), 16);
}

#[test]
fn bulk_load_builds_the_frozen_cache_with_exact_accounting() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(100)
            .with_max_entries(4)
            .with_overlay_capacity(8),
        [
            (Box::<[u8]>::from([0, 255, 0]), vec![1, 2], 7, None),
            (
                Box::<[u8]>::from(*b"second"),
                vec![3, 4, 5],
                11,
                Some(Duration::from_secs(60)),
            ),
        ],
    )
    .unwrap();

    assert_eq!(cache.len(), 2);
    assert_eq!(cache.weight(), 18);
    assert_eq!(cache.peek(&[0, 255, 0]).unwrap().as_slice(), [1, 2]);
    assert_eq!(cache.peek(b"second").unwrap().as_slice(), [3, 4, 5]);
    assert_eq!(cache.stats().inserts, 0);
    assert!(!cache.maintain().unwrap().rebuilt);

    assert_eq!(
        cache
            .insert_discard_with_options(b"second", vec![9], 3, None)
            .unwrap(),
        CacheWriteOutcome::Replaced
    );
    assert_eq!(cache.weight(), 10);
    assert!(cache.remove_discard(&[0, 255, 0]));
    assert_eq!(cache.len(), 1);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_cache_reads_preserve_exact_keys_and_mutations() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(1_024)
            .with_max_entries(16)
            .with_overlay_capacity(16),
        [
            (b"alpha".as_slice(), vec![1, 2], 2, None),
            (b"bravo".as_slice(), vec![3, 4], 2, None),
        ],
    )
    .unwrap();
    let alpha = cache.prepare_key(b"alpha");
    let mut prepared = [packedgen::AtomicPreparedKey::fallback(); 2];
    cache.prepare_key_batch(&[b"alpha".as_slice(), b"bravo"], &mut prepared);
    let guard = cache.pin();

    assert_eq!(
        guard.get_prepared_untracked(b"alpha", &alpha).unwrap(),
        &[1, 2]
    );
    assert_eq!(
        guard.get_prepared_untracked(b"bravo", &alpha).unwrap(),
        &[3, 4]
    );
    assert_eq!(
        guard
            .get_prepared_untracked(b"bravo", &prepared[1])
            .unwrap(),
        &[3, 4]
    );
    assert!(guard.get_prepared_untracked(b"missing", &alpha).is_none());
    assert!(
        !guard
            .replace_discard_prepared_with_options(b"missing", &alpha, vec![0], 1, None)
            .unwrap()
    );

    assert!(
        guard
            .replace_discard_prepared_with_options(b"alpha", &alpha, vec![9, 8, 7], 3, None)
            .unwrap()
    );
    assert_eq!(guard.get_prepared(b"alpha", &alpha).unwrap(), &[9, 8, 7]);
    assert!(guard.touch_prepared(b"alpha", &alpha, Some(Duration::from_secs(60))));
    assert!(cache.touch_prepared(b"alpha", &alpha, None));

    assert!(
        cache
            .replace_discard_prepared_with_options(b"bravo", &alpha, vec![5, 6, 7, 8], 4, None)
            .unwrap()
    );
    assert_eq!(
        guard.get_prepared_untracked(b"bravo", &alpha).unwrap(),
        &[5, 6, 7, 8]
    );

    assert!(guard.remove_discard_prepared(b"alpha", &alpha));
    assert!(guard.get_prepared_untracked(b"alpha", &alpha).is_none());
    assert!(
        !guard
            .replace_discard_prepared_with_options(b"alpha", &alpha, vec![0], 1, None)
            .unwrap()
    );
    assert_eq!(
        cache
            .insert_discard_prepared_with_options(b"alpha", &alpha, vec![6], 1, None)
            .unwrap(),
        CacheWriteOutcome::Inserted
    );
    assert_eq!(
        guard.get_prepared_untracked(b"alpha", &alpha).unwrap(),
        &[6]
    );
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.weight(), 5);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_removal_batch_preserves_exact_fallback_and_accounting() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(1_024)
            .with_max_entries(16)
            .with_overlay_capacity(16),
        [
            (b"alpha".as_slice(), vec![1], 1, None),
            (b"bravo".as_slice(), vec![2, 2], 2, None),
            (b"charlie".as_slice(), vec![3, 3, 3], 3, None),
        ],
    )
    .unwrap();
    let alpha = cache.prepare_key(b"alpha");
    let mut guard = cache.pin();
    {
        let mut batch = guard.removal_batch();
        assert!(batch.remove_discard_prepared(b"alpha", &alpha));
        assert!(!batch.remove_discard_prepared(b"alpha", &alpha));
        assert!(batch.remove_discard_prepared(b"bravo", &alpha));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.weight(), 3);
        assert_eq!(cache.stats().removals, 0);
    }

    assert_eq!(cache.stats().removals, 2);
    assert_eq!(cache.peek(b"charlie").unwrap().as_slice(), [3, 3, 3]);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn replacement_batch_preserves_exact_fallback_capacity_and_statistics() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(1_024)
            .with_max_entries(16)
            .with_overlay_capacity(16),
        [
            (b"alpha".as_slice(), vec![1], 1, None),
            (b"bravo".as_slice(), vec![2, 2], 2, None),
            (b"charlie".as_slice(), vec![3, 3, 3], 3, None),
        ],
    )
    .unwrap();
    let alpha = cache.prepare_key(b"alpha");
    let mut guard = cache.pin();
    {
        let mut batch = guard.replacement_batch();
        assert!(
            batch
                .replace_discard_prepared_with_options(b"alpha", &alpha, vec![9, 9], 2, None)
                .unwrap()
        );
        assert!(
            batch
                .replace_discard_prepared_with_options(b"bravo", &alpha, vec![8], 1, None)
                .unwrap()
        );
        assert!(
            batch
                .replace_discard_with_options(b"charlie", vec![7, 7, 7, 7], 4, None)
                .unwrap()
        );
        assert!(
            !batch
                .replace_discard_prepared_with_options(b"missing", &alpha, vec![0], 1, None)
                .unwrap()
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.weight(), 7);
        assert_eq!(cache.stats().replacements, 0);
    }

    assert_eq!(cache.stats().replacements, 3);
    assert_eq!(cache.peek(b"alpha").unwrap().as_slice(), [9, 9]);
    assert_eq!(cache.peek(b"bravo").unwrap().as_slice(), [8]);
    assert_eq!(cache.peek(b"charlie").unwrap().as_slice(), [7, 7, 7, 7]);

    {
        let mut batch = guard.replacement_batch_untracked();
        assert!(
            batch
                .replace_discard_prepared_with_options(b"alpha", &alpha, vec![6], 1, None)
                .unwrap()
        );
    }
    assert_eq!(cache.stats().replacements, 3);
    assert_eq!(cache.weight(), 6);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_replacement_batch_is_exact_and_validates_before_publication() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(16)
            .with_max_entries(8)
            .with_overlay_capacity(8),
        [
            (b"alpha".as_slice(), vec![1], 1, None),
            (b"bravo".as_slice(), vec![2, 2], 2, None),
            (b"charlie".as_slice(), vec![3, 3, 3], 3, None),
        ],
    )
    .unwrap();
    let keys = [
        b"alpha".as_slice(),
        b"bravo".as_slice(),
        b"charlie".as_slice(),
        b"missing".as_slice(),
    ];
    let alpha = cache.prepare_key(keys[0]);
    let prepared = [
        alpha,
        alpha,
        packedgen::AtomicPreparedKey::fallback(),
        alpha,
    ];
    let mut guard = cache.pin();
    {
        let mut batch = guard.prepared_replacement_batch();
        let mut replaced = [false; 4];
        batch
            .replace_discard_prepared_batch_with_options(
                &keys,
                &prepared,
                [
                    (vec![9, 9], 2, None),
                    (vec![8], 1, None),
                    (vec![7, 7, 7, 7], 4, None),
                    (vec![0], 1, None),
                ],
                &mut replaced,
            )
            .unwrap();
        assert_eq!(replaced, [true, true, true, false]);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.weight(), 7);
        assert_eq!(cache.stats().replacements, 0);
    }
    assert_eq!(cache.stats().replacements, 3);
    assert_eq!(cache.peek(keys[0]).unwrap().as_slice(), [9, 9]);
    assert_eq!(cache.peek(keys[1]).unwrap().as_slice(), [8]);
    assert_eq!(cache.peek(keys[2]).unwrap().as_slice(), [7, 7, 7, 7]);
    assert!(cache.peek(keys[3]).is_none());

    let before = cache.peek(keys[0]).unwrap().to_vec();
    let mut batch = guard.prepared_replacement_batch();
    let mut replaced = [false; 2];
    assert!(
        batch
            .replace_discard_prepared_batch_with_options(
                &keys[..2],
                &prepared[..2],
                [(vec![6], 1, None), (vec![5], 17, None)],
                &mut replaced,
            )
            .is_err()
    );
    drop(batch);
    assert_eq!(cache.peek(keys[0]).unwrap().as_slice(), before);
    assert_eq!(cache.stats().replacements, 3);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_replacement_batch_accounts_before_capacity_eviction() {
    let keys = [
        b"alpha".as_slice(),
        b"bravo".as_slice(),
        b"charlie".as_slice(),
        b"delta".as_slice(),
    ];
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(4)
            .with_max_entries(4)
            .with_overlay_capacity(8),
        keys.iter().map(|key| (*key, vec![1], 1, None)),
    )
    .unwrap();
    let prepared = keys.map(|key| cache.prepare_key(key));
    let mut guard = cache.pin();
    {
        let mut batch = guard.prepared_replacement_batch();
        let mut replaced = [false; 4];
        batch
            .replace_discard_prepared_batch_with_options(
                &keys,
                &prepared,
                [
                    (vec![2, 2], 2, None),
                    (vec![3, 3], 2, None),
                    (vec![4, 4], 2, None),
                    (vec![5, 5], 2, None),
                ],
                &mut replaced,
            )
            .unwrap();
        assert_eq!(replaced, [true; 4]);
    }

    let surviving = keys.iter().filter(|key| cache.peek(key).is_some()).count();
    assert_eq!(cache.weight(), (surviving * 2) as u64);
    assert!(cache.weight() <= 4);
    assert_eq!(cache.len(), surviving);
    assert_eq!(cache.stats().replacements, 4);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_replacement_pipeline_supports_mixed_cache_operations() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(16)
            .with_max_entries(8)
            .with_overlay_capacity(8),
        [
            (b"alpha".as_slice(), vec![1], 1, None),
            (b"bravo".as_slice(), vec![2], 1, None),
        ],
    )
    .unwrap();
    let alpha = cache.prepare_key(b"alpha");
    let mut guard = cache.pin();
    {
        let mut pipeline = guard.prepared_replacement_batch();
        assert_eq!(pipeline.get_prepared(b"alpha", &alpha).unwrap(), &[1]);
        assert!(pipeline.get(b"missing").is_none());
        assert_eq!(
            pipeline
                .insert_if_absent_with_options(b"charlie", vec![3, 3], 2, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
        assert!(
            pipeline
                .replace_discard_prepared_with_options(b"alpha", &alpha, vec![4, 4, 4], 3, None,)
                .unwrap()
        );
        assert!(pipeline.remove_discard(b"bravo"));
    }
    drop(guard);

    assert_eq!(cache.peek(b"alpha").unwrap().as_slice(), [4, 4, 4]);
    assert_eq!(cache.peek(b"charlie").unwrap().as_slice(), [3, 3]);
    assert!(cache.peek(b"bravo").is_none());
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.weight(), 5);
    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().misses, 1);
    assert_eq!(cache.stats().inserts, 1);
    assert_eq!(cache.stats().replacements, 1);
    assert_eq!(cache.stats().removals, 1);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_replacement_pipeline_recycles_and_drops_values_exactly_once() {
    struct CountedValue(Arc<AtomicUsize>);

    impl Drop for CountedValue {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    const ROUNDS: usize = 600;
    let drops = Arc::new(AtomicUsize::new(0));
    let keys = [
        b"alpha".as_slice(),
        b"bravo".as_slice(),
        b"missing".as_slice(),
    ];
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(u64::MAX)
            .with_max_entries(8)
            .with_overlay_capacity(8),
        keys[..2]
            .iter()
            .map(|key| (*key, CountedValue(Arc::clone(&drops)), 1, None)),
    )
    .unwrap();
    let prepared = keys.map(|key| cache.prepare_key(key));
    let mut guard = cache.pin();
    {
        let mut pipeline = guard.prepared_replacement_batch();
        let mut replaced = [false; 3];
        for round in 0..ROUNDS {
            let replacements = (0..keys.len()).map(|_| (CountedValue(Arc::clone(&drops)), 1, None));
            if round.is_multiple_of(2) {
                pipeline
                    .replace_discard_prepared_batch_with_options(
                        &keys,
                        &prepared,
                        replacements,
                        &mut replaced,
                    )
                    .unwrap();
            } else {
                pipeline
                    .replace_discard_batch_with_options(&keys, replacements, &mut replaced)
                    .unwrap();
            }
            assert_eq!(replaced, [true, true, false]);
        }
    }
    drop(guard);

    assert_eq!(cache.len(), 2);
    assert!(cache.peek(b"missing").is_none());
    assert_eq!(cache.stats().replacements, (ROUNDS * 2) as u64);
    drop(cache);
    assert_eq!(drops.load(Ordering::Relaxed), 2 + ROUNDS * keys.len());
}

#[cfg(all(feature = "prepared-keys", feature = "cache-diagnostics"))]
#[test]
fn reclamation_diagnostics_reach_zero_after_maintenance() {
    let keys = (0..16_u8).map(|key| [key; 8]).collect::<Vec<_>>();
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(u64::MAX)
            .with_max_entries(32)
            .with_overlay_capacity(32),
        keys.iter().map(|key| (*key, vec![0; 16], 16, None)),
    )
    .unwrap();
    let prepared = keys
        .iter()
        .map(|key| cache.prepare_key(key))
        .collect::<Vec<_>>();
    let key_refs = keys.iter().map(<[u8; 8]>::as_slice).collect::<Vec<_>>();
    let mut replaced = vec![false; keys.len()];
    let mut guard = cache.pin();
    {
        let mut pipeline = guard.prepared_replacement_batch();
        for round in 0..300_u16 {
            pipeline
                .replace_discard_prepared_batch_with_options(
                    &key_refs,
                    &prepared,
                    (0..keys.len()).map(|_| (round.to_le_bytes().repeat(8), 16, None)),
                    &mut replaced,
                )
                .unwrap();
            assert!(replaced.iter().all(|replaced| *replaced));
        }
    }
    drop(guard);

    let before = cache.reclamation_stats();
    assert_eq!(before.readers, [0; 3]);
    assert!(before.retired_values > 0);
    assert!(before.recyclable_allocations > 0);
    cache.maintain().unwrap();
    let after = cache.reclamation_stats();
    assert_eq!(after.readers, [0; 3]);
    assert_eq!(after.retired_values, 0);
    assert_eq!(after.published_retired_values, 0);
    assert!(after.recyclable_allocations > 0);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn long_replacement_batch_refreshes_and_publishes_exact_statistics() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(1_024)
            .with_max_entries(16)
            .with_overlay_capacity(16),
        [(b"alpha".as_slice(), vec![0], 1, None)],
    )
    .unwrap();
    let alpha = cache.prepare_key(b"alpha");
    let mut guard = cache.pin();
    {
        let mut batch = guard.replacement_batch();
        for value in 0..600_u16 {
            assert!(
                batch
                    .replace_discard_prepared_with_options(
                        b"alpha",
                        &alpha,
                        value.to_le_bytes().to_vec(),
                        2,
                        None,
                    )
                    .unwrap()
            );
        }
        assert_eq!(cache.stats().replacements, 512);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.weight(), 2);
    }

    assert_eq!(cache.stats().replacements, 600);
    assert_eq!(
        cache.peek(b"alpha").unwrap().as_slice(),
        599_u16.to_le_bytes()
    );
}

#[test]
fn bulk_load_rejects_capacity_violations() {
    let entries = [(b"a", vec![1], 6, None), (b"b", vec![2], 5, None)];
    let result = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(10)
            .with_max_entries(2)
            .with_overlay_capacity(4),
        entries,
    );
    assert!(matches!(
        result,
        Err(CacheBuildError::InitialWeight {
            weight: 11,
            maximum: 10
        })
    ));

    let entries = [(b"a", vec![1], 1, None), (b"b", vec![2], 1, None)];
    let result = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(10)
            .with_max_entries(1)
            .with_overlay_capacity(4),
        entries,
    );
    assert!(matches!(
        result,
        Err(CacheBuildError::InitialEntries {
            entries: 2,
            maximum: 1
        })
    ));
}

#[test]
fn failed_duplicate_bulk_load_reclaims_values() {
    struct DropValue(Arc<AtomicUsize>);

    impl Drop for DropValue {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let entries = (0..9).map(|_| (b"duplicate", DropValue(Arc::clone(&drops)), 1, None));
    let result = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(16)
            .with_max_entries(16)
            .with_overlay_capacity(16),
        entries,
    );
    assert!(matches!(
        result,
        Err(CacheBuildError::Index(
            FrozenBuildError::IndexConstructionFailed
        ))
    ));
    assert_eq!(drops.load(Ordering::Relaxed), 9);
}

#[test]
fn bulk_loaded_expired_entry_is_removed_on_read() {
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(10)
            .with_max_entries(1)
            .with_overlay_capacity(4),
        [(b"expired", vec![1], 1, Some(Duration::ZERO))],
    )
    .unwrap();
    assert!(cache.get(b"expired").is_none());
    assert!(cache.is_empty());
    assert_eq!(cache.weight(), 0);
}

#[test]
fn protected_value_survives_replacement_and_cache_drop() {
    let cache = Arc::new(cache(1_024, 16));
    cache
        .insert_discard_with_options(b"key", vec![1, 2, 3], 3, None)
        .unwrap();
    let held = cache.get(b"key").unwrap();
    cache
        .insert_discard_with_options(b"key", vec![4, 5, 6], 3, None)
        .unwrap();
    assert_eq!(held.as_slice(), [1, 2, 3]);
    drop(cache);
    assert_eq!(held.as_slice(), [1, 2, 3]);
}

#[test]
fn protected_value_survives_discard_removal_and_forced_reclamation() {
    let cache = Arc::new(cache(1_024, 16));
    cache
        .insert_discard_with_options(b"key", vec![1, 2, 3], 3, None)
        .unwrap();
    let held = cache.get(b"key").unwrap();

    assert!(cache.remove_discard(b"key"));
    assert!(cache.get(b"key").is_none());
    cache.maintain().unwrap();
    assert_eq!(held.as_slice(), [1, 2, 3]);

    drop(cache);
    assert_eq!(held.as_slice(), [1, 2, 3]);
}

#[test]
fn removal_batch_keeps_accounting_and_partial_retirement_exact() {
    const ENTRIES: usize = 600;
    let cache = Arc::new(cache(u64::MAX, ENTRIES + 1));
    for key in 0..ENTRIES {
        cache
            .insert_discard_with_options(
                &key.to_le_bytes(),
                vec![u8::try_from(key & 255).unwrap()],
                1,
                None,
            )
            .unwrap();
    }
    cache.maintain().unwrap();
    let held = cache.get(&0_usize.to_le_bytes()).unwrap();

    let mut guard = cache.pin();
    {
        let mut batch = guard.removal_batch();
        for key in 0..ENTRIES {
            assert!(batch.remove_discard(&key.to_le_bytes()));
        }
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.weight(), 0);
        assert_eq!(cache.stats().removals, 512);
    }
    cache.maintain().unwrap();
    assert_eq!(held.as_slice(), [0]);
    assert_eq!(cache.stats().removals, ENTRIES as u64);
}

#[test]
fn untracked_removal_batch_keeps_capacity_without_recording_statistics() {
    const ENTRIES: usize = 70;
    let cache = cache(u64::MAX, ENTRIES + 1);
    for key in 0..ENTRIES {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1], 1, None)
            .unwrap();
    }
    cache.maintain().unwrap();

    let mut guard = cache.pin();
    {
        let mut batch = guard.removal_batch_untracked();
        for key in 0..ENTRIES {
            assert!(batch.remove_discard(&key.to_le_bytes()));
        }
    }

    assert!(cache.is_empty());
    assert_eq!(cache.weight(), 0);
    assert_eq!(cache.stats().removals, 0);
}

#[test]
fn unbounded_removal_batch_publishes_index_length_on_drop() {
    const ENTRIES: usize = 600;
    let cache =
        DirectPackedCache::try_new(CacheConfig::new(u64::MAX).with_overlay_capacity(ENTRIES + 1))
            .unwrap();
    for key in 0..ENTRIES {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1], 1, None)
            .unwrap();
    }
    cache.maintain().unwrap();

    let mut guard = cache.pin();
    {
        let mut batch = guard.removal_batch_untracked();
        for key in 0..ENTRIES {
            assert!(batch.remove_discard(&key.to_le_bytes()));
        }
        assert_eq!(cache.len(), ENTRIES - 512);
    }

    assert!(cache.is_empty());
}

#[test]
fn ttl_touch_and_capacity_eviction_remain_bounded() {
    let cache = cache(1_024, 2);
    cache
        .insert_discard_with_options(b"a", vec![1], 1, None)
        .unwrap();
    cache
        .insert_discard_with_options(b"b", vec![2], 1, None)
        .unwrap();
    cache
        .insert_discard_with_options(b"c", vec![3], 1, None)
        .unwrap();
    assert_eq!(cache.len(), 2);
    assert!(cache.get(b"c").is_some());

    let guard = cache.pin();
    assert!(guard.touch(b"c", Some(Duration::ZERO)));
    assert!(guard.get(b"c").is_none());
    assert!(cache.len() <= 1);
}

#[test]
fn weighted_eviction_creates_bounded_headroom_for_uniform_turnover() {
    const CAPACITY: u64 = 4_096;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(CAPACITY)
            .with_overlay_capacity(usize::try_from(CAPACITY).unwrap() * 2)
            .with_eviction_batch(64),
    )
    .unwrap();
    for key in 0_u64..CAPACITY {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }

    let admitted = CAPACITY.to_le_bytes();
    cache
        .insert_discard_with_options(&admitted, vec![1; 8], 1, None)
        .unwrap();

    assert!(cache.peek(&admitted).is_some());
    assert!(cache.weight() <= CAPACITY - 4);
    assert!(cache.weight() >= CAPACITY - 64);
}

#[test]
fn victim_reservoir_tolerates_stale_keys_under_repeated_pressure() {
    const CAPACITY: usize = 64;
    let cache = cache(u64::MAX, CAPACITY);
    for key in 0_u64..CAPACITY as u64 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }

    // The first over-capacity insertion fills the victim reservoir. Explicit
    // removals and replacements then make some of those copied keys stale.
    cache
        .insert_discard_with_options(&64_u64.to_le_bytes(), vec![1; 8], 1, None)
        .unwrap();
    for key in 1_u64..32 {
        cache.remove_discard(&key.to_le_bytes());
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![2; 8], 1, None)
            .unwrap();
    }

    for key in 65_u64..2_065 {
        let encoded = key.to_le_bytes();
        cache
            .insert_discard_with_options(&encoded, vec![3; 8], 1, None)
            .unwrap();
        assert!(cache.peek(&encoded).is_some());
        assert!(cache.len() <= CAPACITY);
    }

    let original_survivors = (0_u64..=64)
        .filter(|key| cache.peek(&key.to_le_bytes()).is_some())
        .count();
    assert!(original_survivors <= CAPACITY / 4);
    assert!(cache.stats().evictions > 0);
}

#[test]
fn concurrent_replacement_and_eviction_preserve_exact_capacity_accounting() {
    const WRITERS: usize = 8;
    const KEY_SPACE: usize = 512;
    const OPERATIONS: usize = 4_000;
    const MAX_ENTRIES: usize = 96;
    const MAX_WEIGHT: u64 = 512;

    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(MAX_WEIGHT)
                .with_max_entries(MAX_ENTRIES)
                .with_overlay_capacity(KEY_SPACE * 2)
                .with_eviction_batch(64),
        )
        .unwrap(),
    );
    for key in 0..MAX_ENTRIES {
        let weight = 1 + key % 11;
        cache
            .insert_discard_with_options(
                &key.to_le_bytes(),
                vec![u8::try_from(key & 255).unwrap(); weight],
                weight as u64,
                None,
            )
            .unwrap();
    }

    let start = Arc::new(Barrier::new(WRITERS));
    let mut joins = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            for operation in 0..OPERATIONS {
                let key = (operation.wrapping_mul(131) + writer.wrapping_mul(17)) % KEY_SPACE;
                let weight = 1 + (operation + writer * 3) % 11;
                cache
                    .insert_discard_with_options(
                        &key.to_le_bytes(),
                        vec![u8::try_from((operation + writer) & 255).unwrap(); weight],
                        weight as u64,
                        None,
                    )
                    .unwrap();
                if operation.is_multiple_of(257) {
                    cache.maintain().unwrap();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    cache.maintain().unwrap();

    let mut observed_entries = 0;
    let mut observed_weight = 0_u64;
    for key in 0..KEY_SPACE {
        if let Some(value) = cache.peek(&key.to_le_bytes()) {
            observed_entries += 1;
            observed_weight += value.len() as u64;
        }
    }
    assert_eq!(cache.len(), observed_entries);
    assert_eq!(cache.weight(), observed_weight);
    assert!(observed_entries <= MAX_ENTRIES);
    assert!(observed_weight <= MAX_WEIGHT);
    assert!(cache.stats().evictions > 0);
}

#[test]
fn long_key_fallback_turns_over_without_exceeding_capacity() {
    const CAPACITY: usize = 256;
    let cache = cache(u64::MAX, CAPACITY);
    for key in 0..CAPACITY {
        cache
            .insert_discard_with_options(&long_key(key), vec![0; 8], 1, None)
            .unwrap();
    }

    for key in CAPACITY..CAPACITY * 9 {
        let encoded = long_key(key);
        cache
            .insert_discard_with_options(&encoded, vec![1; 8], 1, None)
            .unwrap();
        assert!(cache.len() <= CAPACITY);
    }

    let original_survivors = (0..CAPACITY)
        .filter(|key| cache.peek(&long_key(*key)).is_some())
        .count();
    assert!(original_survivors < CAPACITY / 2);
    assert!(cache.stats().evictions >= CAPACITY as u64);
}

#[test]
fn mixed_long_key_reservoir_preserves_variable_key_boundaries() {
    const CAPACITY: usize = 256;
    let cache = cache(u64::MAX, CAPACITY);
    for key in 0..CAPACITY {
        cache
            .insert_discard_with_options(&mixed_long_key(key), vec![0; 8], 1, None)
            .unwrap();
    }

    for key in CAPACITY..CAPACITY * 9 {
        let encoded = mixed_long_key(key);
        cache
            .insert_discard_with_options(&encoded, vec![1; 8], 1, None)
            .unwrap();
        assert!(cache.peek(&encoded).is_some());
        assert!(cache.len() <= CAPACITY);
    }

    let original_survivors = (0..CAPACITY)
        .filter(|key| cache.peek(&mixed_long_key(*key)).is_some())
        .count();
    assert!(original_survivors < CAPACITY / 2);
    assert!(cache.stats().evictions >= CAPACITY as u64);
}

#[test]
fn frozen_base_sampling_turns_over_the_packed_generation() {
    const CAPACITY: usize = 1_024;
    let cache = cache(u64::MAX, CAPACITY);
    for key in 0_u64..CAPACITY as u64 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }
    assert!(cache.maintain().unwrap().rebuilt);

    for key in CAPACITY as u64..(CAPACITY * 2) as u64 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1; 8], 1, None)
            .unwrap();
    }

    let original_survivors = (0_u64..CAPACITY as u64)
        .filter(|key| cache.peek(&key.to_le_bytes()).is_some())
        .count();
    assert!(original_survivors < CAPACITY / 2);
    assert!(cache.len() <= CAPACITY);
}

#[test]
fn async_eviction_allows_bounded_overshoot_and_enforces_the_hard_limit() {
    const CAPACITY: usize = 128;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(CAPACITY)
            .with_overlay_capacity(512)
            .with_async_eviction(11_000),
    )
    .unwrap();
    for key in 0_u64..140 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }
    assert!(cache.len() > CAPACITY);
    assert!(cache.len() <= 141);

    for key in 140_u64..142 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1; 8], 1, None)
            .unwrap();
    }
    assert_eq!(cache.len(), 140);
}

#[cfg(feature = "cache-pressure-timing")]
#[test]
fn diagnostics_attribute_foreground_hard_limit_eviction() {
    const CAPACITY: usize = 128;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(CAPACITY)
            .with_overlay_capacity(512)
            .with_async_eviction(11_000),
    )
    .unwrap();

    for key in 0_u64..142 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }

    let diagnostics = cache.reclamation_stats();
    assert!(diagnostics.foreground_capacity_enforcements > 0);
    assert!(diagnostics.foreground_capacity_enforcement_ns > 0);
    assert!(diagnostics.max_foreground_capacity_enforcement_ns > 0);
    assert!(diagnostics.victim_collections > 0);
    assert!(diagnostics.victim_collection_ns > 0);
    assert!(diagnostics.max_victim_collection_ns > 0);
    assert_eq!(diagnostics.background_capacity_drains, 0);
}

#[cfg(all(feature = "cache-diagnostics", not(feature = "cache-pressure-timing")))]
#[test]
fn pressure_timing_is_opt_in() {
    const CAPACITY: usize = 128;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(u64::MAX)
            .with_max_entries(CAPACITY)
            .with_overlay_capacity(512)
            .with_async_eviction(11_000),
    )
    .unwrap();
    for key in 0_u64..142 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }

    let diagnostics = cache.reclamation_stats();
    assert_eq!(diagnostics.foreground_capacity_enforcements, 0);
    assert_eq!(diagnostics.foreground_capacity_enforcement_ns, 0);
    assert_eq!(diagnostics.victim_collections, 0);
    assert_eq!(diagnostics.victim_collection_ns, 0);
}

#[test]
fn async_eviction_requires_positive_slack() {
    let result =
        DirectPackedCache::<Vec<u8>>::try_new(CacheConfig::new(1).with_async_eviction(10_000));
    assert!(matches!(
        result,
        Err(CacheBuildError::Config(
            CacheConfigError::InvalidAsyncHardLimit
        ))
    ));
}

#[test]
fn async_eviction_enforces_the_weight_hard_limit() {
    const CAPACITY: u64 = 128;
    let cache = DirectPackedCache::try_new(
        CacheConfig::new(CAPACITY)
            .with_overlay_capacity(512)
            .with_async_eviction(11_000),
    )
    .unwrap();
    for key in 0_u64..140 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }
    assert!(cache.weight() > CAPACITY);
    assert!(cache.weight() <= 141);

    for key in 140_u64..142 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1; 8], 1, None)
            .unwrap();
    }
    assert_eq!(cache.weight(), 140);
}

#[test]
fn async_worker_wakes_to_drain_soft_limit_debt() {
    const CAPACITY: usize = 256;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(CAPACITY)
                .with_overlay_capacity(1_024)
                .with_async_eviction(12_000),
        )
        .unwrap(),
    );
    let maintenance = cache.spawn_maintenance(Duration::from_secs(60));
    for key in 0_u64..300 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while cache.len() > CAPACITY && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(cache.len() <= CAPACITY);
    maintenance.shutdown();
}

#[cfg(feature = "cache-pressure-timing")]
#[test]
fn diagnostics_attribute_background_capacity_drain() {
    const CAPACITY: usize = 256;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(CAPACITY)
                .with_overlay_capacity(1_024)
                .with_async_eviction(20_000),
        )
        .unwrap(),
    );
    let maintenance = cache.spawn_maintenance(Duration::from_secs(60));
    for key in 0_u64..300 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while cache.len() > CAPACITY && Instant::now() < deadline {
        thread::yield_now();
    }
    maintenance.shutdown();

    let diagnostics = cache.reclamation_stats();
    assert!(diagnostics.background_capacity_drains > 0);
    assert!(diagnostics.background_capacity_drain_ns > 0);
    assert!(diagnostics.max_background_capacity_drain_ns > 0);
}

#[test]
fn async_worker_can_be_restarted() {
    const CAPACITY: usize = 64;
    let cache = Arc::new(
        DirectPackedCache::try_new(
            CacheConfig::new(u64::MAX)
                .with_max_entries(CAPACITY)
                .with_overlay_capacity(256)
                .with_async_eviction(20_000),
        )
        .unwrap(),
    );
    for key in 0_u64..CAPACITY as u64 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![0; 8], 1, None)
            .unwrap();
    }

    cache.spawn_maintenance(Duration::from_secs(60)).shutdown();
    let maintenance = cache.spawn_maintenance(Duration::from_secs(60));
    thread::sleep(Duration::from_millis(10));
    for key in CAPACITY as u64..CAPACITY as u64 + 16 {
        cache
            .insert_discard_with_options(&key.to_le_bytes(), vec![1; 8], 1, None)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while cache.len() > CAPACITY && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(cache.len() <= CAPACITY);
    maintenance.shutdown();
}

#[test]
fn pressure_certificate_preserves_skewed_entry_and_weight_limits() {
    let entry_limited = cache(u64::MAX, 128);
    for middle in 0..129_u8 {
        entry_limited
            .insert_discard_with_options(&[7, middle, 9], vec![middle], 1, None)
            .unwrap();
    }
    assert!(entry_limited.len() <= 128);

    let weight_limited = cache(128, 1_024);
    for middle in 0..129_u8 {
        weight_limited
            .insert_discard_with_options(&[11, middle, 13], vec![middle], 1, None)
            .unwrap();
    }
    assert!(weight_limited.weight() <= 128);
}

#[test]
fn concurrent_admission_has_one_winner() {
    const WRITERS: usize = 16;
    let cache = Arc::new(cache(1_024, 32));
    let start = Arc::new(Barrier::new(WRITERS));
    let winners = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::new();
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        let winners = Arc::clone(&winners);
        joins.push(thread::spawn(move || {
            start.wait();
            let guard = cache.pin();
            if guard
                .insert_if_absent_with_options(
                    b"shared",
                    vec![u8::try_from(writer).unwrap()],
                    1,
                    None,
                )
                .unwrap()
                == CacheAdmissionOutcome::Inserted
            {
                winners.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert_eq!(winners.load(Ordering::Relaxed), 1);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 1);
}

#[test]
fn guarded_remove_and_concurrent_readmission_keep_exact_accounting() {
    const THREADS: usize = 8;
    const OPERATIONS: usize = 2_000;
    let cache = Arc::new(cache(u64::MAX, 16));
    cache
        .insert_discard_with_options(b"shared", vec![0], 1, None)
        .unwrap();
    let start = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::new();
    for worker in 0..THREADS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let mut guard = cache.pin();
            start.wait();
            for operation in 0..OPERATIONS {
                guard.remove_discard(b"shared");
                guard
                    .insert_if_absent_with_options(
                        b"shared",
                        vec![u8::try_from(worker).unwrap()],
                        1,
                        None,
                    )
                    .unwrap();
                if operation.is_multiple_of(256) {
                    guard.refresh();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert!(cache.peek(b"shared").is_some());
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 1);
}

#[test]
fn concurrent_replacement_and_pinned_reads_stay_valid() {
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const OPERATIONS: usize = 20_000;
    let cache = Arc::new(cache(u64::MAX, 64));
    cache
        .insert_discard_with_options(b"hot", vec![0; 32], 32, None)
        .unwrap();
    let start = Arc::new(Barrier::new(WRITERS + READERS));
    let mut joins = Vec::new();
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            for operation in 0..OPERATIONS {
                cache
                    .insert_discard_with_options(
                        b"hot",
                        vec![u8::try_from((writer + operation) & 255).unwrap(); 32],
                        32,
                        None,
                    )
                    .unwrap();
            }
        }));
    }
    for _ in 0..READERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            let mut guard = cache.pin();
            for operation in 0..OPERATIONS {
                assert_eq!(guard.get_untracked(b"hot").unwrap().len(), 32);
                assert!(guard.touch(b"hot", None));
                if operation.is_multiple_of(1_024) {
                    guard.refresh();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 32);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn concurrent_prepared_replacement_reads_and_touch_stay_valid() {
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const OPERATIONS: usize = 10_000;
    let cache = Arc::new(
        DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(u64::MAX)
                .with_max_entries(64)
                .with_overlay_capacity(64),
            [(b"hot".as_slice(), vec![0; 32], 32, None)],
        )
        .unwrap(),
    );
    let prepared = cache.prepare_key(b"hot");
    let start = Arc::new(Barrier::new(WRITERS + READERS));
    let mut joins = Vec::new();
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            for operation in 0..OPERATIONS {
                cache
                    .replace_discard_prepared_with_options(
                        b"hot",
                        &prepared,
                        vec![u8::try_from((writer + operation) & 255).unwrap(); 32],
                        32,
                        None,
                    )
                    .unwrap();
            }
        }));
    }
    for _ in 0..READERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            let mut guard = cache.pin();
            for operation in 0..OPERATIONS {
                assert_eq!(
                    guard
                        .get_prepared_untracked(b"hot", &prepared)
                        .unwrap()
                        .len(),
                    32
                );
                assert!(guard.touch_prepared(b"hot", &prepared, None));
                if operation.is_multiple_of(1_024) {
                    guard.refresh();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.weight(), 32);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn concurrent_prepared_replacement_batches_recycle_without_lost_values() {
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const KEYS: usize = 16;
    const ROUNDS: usize = 1_000;
    let keys = Arc::new(
        (0..KEYS)
            .map(|key| key.to_le_bytes().to_vec().into_boxed_slice())
            .collect::<Vec<_>>(),
    );
    let cache = Arc::new(
        DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(u64::MAX)
                .with_max_entries(KEYS * 2)
                .with_overlay_capacity(KEYS * 2),
            keys.iter().map(|key| (key.as_ref(), vec![0; 32], 32, None)),
        )
        .unwrap(),
    );
    let prepared = Arc::new(
        keys.iter()
            .map(|key| cache.prepare_key(key))
            .collect::<Vec<_>>(),
    );
    let start = Arc::new(Barrier::new(WRITERS + READERS));
    let mut joins = Vec::new();
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let keys = Arc::clone(&keys);
        let prepared = Arc::clone(&prepared);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let key_refs = keys.iter().map(AsRef::as_ref).collect::<Vec<&[u8]>>();
            let mut replaced = [false; KEYS];
            let mut guard = cache.pin();
            let mut pipeline = guard.prepared_replacement_batch();
            start.wait();
            for round in 0..ROUNDS {
                pipeline
                    .replace_discard_prepared_batch_with_options(
                        &key_refs,
                        &prepared,
                        (0..KEYS).map(|key| {
                            (
                                vec![u8::try_from((writer + round + key) & 255).unwrap(); 32],
                                32,
                                None,
                            )
                        }),
                        &mut replaced,
                    )
                    .unwrap();
                assert_eq!(replaced, [true; KEYS]);
            }
        }));
    }
    for reader in 0..READERS {
        let cache = Arc::clone(&cache);
        let keys = Arc::clone(&keys);
        let prepared = Arc::clone(&prepared);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let mut guard = cache.pin();
            start.wait();
            for round in 0..ROUNDS * 4 {
                let key = (reader + round) % KEYS;
                assert_eq!(
                    guard
                        .get_prepared_untracked(&keys[key], &prepared[key])
                        .unwrap()
                        .len(),
                    32
                );
                if round.is_multiple_of(256) {
                    guard.refresh();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }

    assert_eq!(cache.len(), KEYS);
    assert_eq!(cache.weight(), (KEYS * 32) as u64);
    assert_eq!(cache.stats().replacements, (WRITERS * KEYS * ROUNDS) as u64);
}

fn long_key(index: usize) -> [u8; 64] {
    let mut key = [0_u8; 64];
    let mut state = index as u64;
    for chunk in key.chunks_exact_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15).rotate_left(17);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mixed_long_key(index: usize) -> Vec<u8> {
    const LENGTHS: [usize; 5] = [49, 57, 64, 80, 127];
    let mut key = vec![0_u8; LENGTHS[index % LENGTHS.len()]];
    let mut state = index as u64;
    for byte in &mut key {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15).rotate_left(17);
        *byte = state.to_le_bytes()[0];
    }
    key[..8].copy_from_slice(&(index as u64).to_le_bytes());
    key
}

#[cfg(miri)]
#[test]
fn miri_conditional_block_and_boxed_reclaimers_coexist() {
    let cache = cache(u64::MAX, 32);
    let guard = cache.pin();
    for key in 0_u8..16 {
        assert_eq!(
            guard
                .insert_if_absent_with_options(&[key], vec![key; 8], 8, None)
                .unwrap(),
            CacheAdmissionOutcome::Inserted
        );
    }
    drop(guard);

    cache
        .insert_discard_with_options(&[0], vec![99; 16], 16, None)
        .unwrap();
    let held = cache.get(&[0]).unwrap();
    let guard = cache.pin();
    for key in 1_u8..16 {
        assert!(guard.remove_discard(&[key]));
    }
    drop(guard);
    drop(cache);
    assert_eq!(held.as_slice(), [99; 16]);
}

#[cfg(miri)]
#[test]
fn miri_concurrent_replacement_and_reclamation_smoke() {
    const WRITERS: usize = 2;
    const READERS: usize = 2;
    const OPERATIONS: usize = 64;
    let cache = Arc::new(cache(u64::MAX, 8));
    for key in 0..WRITERS {
        cache
            .insert_discard_with_options(&[key as u8], vec![0; 8], 8, None)
            .unwrap();
    }
    let start = Arc::new(Barrier::new(WRITERS + READERS));
    let mut joins = Vec::new();
    for writer in 0..WRITERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            for operation in 0..OPERATIONS {
                cache
                    .insert_discard_with_options(
                        &[writer as u8],
                        vec![u8::try_from(operation).unwrap(); 8],
                        8,
                        None,
                    )
                    .unwrap();
            }
        }));
    }
    for _ in 0..READERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            start.wait();
            let mut guard = cache.pin();
            for operation in 0..OPERATIONS {
                for key in 0..WRITERS {
                    assert_eq!(guard.get_untracked(&[key as u8]).unwrap().len(), 8);
                }
                if operation.is_multiple_of(8) {
                    guard.refresh();
                }
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert_eq!(cache.len(), WRITERS);
    assert_eq!(cache.weight(), (WRITERS * 8) as u64);
}
