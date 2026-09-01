#![allow(missing_docs)]

#[cfg(feature = "prepared-keys")]
use std::mem::size_of;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

#[cfg(feature = "prepared-keys")]
use packedgen::AtomicPreparedKey;
#[cfg(feature = "phast")]
use packedgen::FrozenIndexBackend;
#[cfg(any(feature = "phast", feature = "shared-gx"))]
use packedgen::GenerationHashBuilder;
use packedgen::{
    AdaptiveOverlayPhase, AdaptiveRebuildPolicy, AtomicEntry, AtomicGenerationBaseFilter,
    AtomicGenerationOverlay, InsertOutcome, LockFreeAtomicU64GenerationMap, NonMaxU64,
    NonMaxU64Error,
};

#[test]
fn value_domain_reserves_only_the_deletion_marker() {
    assert_eq!(
        AtomicGenerationBaseFilter::default(),
        AtomicGenerationBaseFilter::Disabled
    );
    assert_eq!(NonMaxU64::new(0).unwrap().get(), 0);
    assert_eq!(NonMaxU64::new(u64::MAX - 1).unwrap().get(), u64::MAX - 1);
    assert_eq!(NonMaxU64::new(u64::MAX), None);
    assert_eq!(NonMaxU64::try_from(u64::MAX), Err(NonMaxU64Error));
}

#[test]
fn protected_pointer_class_reads_match_normal_reads_across_layers() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        (0..64_u64).map(|value| (value.to_le_bytes(), non_max(value + 1))),
        128,
        AtomicGenerationOverlay::AtomicAdaptive,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
    )
    .unwrap();
    let overlay_key = 1_000_u64.to_le_bytes();
    let removed_key = 7_u64.to_le_bytes();

    assert!(map.insert_new(&overlay_key, non_max(1_001)));
    assert_eq!(map.remove(&removed_key), Some(non_max(8)));
    for key in [0_u64.to_le_bytes(), overlay_key, removed_key] {
        assert_eq!(map.get_protected(&key), map.get(&key));
    }

    map.rebuild(128).unwrap();
    for key in [0_u64.to_le_bytes(), overlay_key, removed_key] {
        assert_eq!(map.get_protected(&key), map.get(&key));
    }
}

#[test]
fn atomic_cells_preserve_crud_and_rebuild_semantics() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..128_u64).map(|value| (value.to_le_bytes(), non_max(value))),
        32,
    )
    .unwrap();

    assert_eq!(map.get(&7_u64.to_le_bytes()), Some(non_max(7)));
    assert_eq!(
        map.insert(&7_u64.to_le_bytes(), non_max(70)),
        InsertOutcome::Replaced(non_max(7))
    );
    assert_eq!(map.remove(&8_u64.to_le_bytes()), Some(non_max(8)));
    assert!(map.insert_new(&200_u64.to_le_bytes(), non_max(200)));
    assert_eq!(
        map.upsert(&201_u64.to_le_bytes(), non_max(201), increment),
        non_max(201)
    );
    assert_eq!(
        map.upsert(&201_u64.to_le_bytes(), non_max(999), increment),
        non_max(202)
    );
    assert_eq!(
        map.update(&9_u64.to_le_bytes(), increment),
        Some(non_max(10))
    );
    assert_eq!(map.len(), 129);
    assert_eq!(map.stats().current.overlay_records, 2);

    let rebuilt = map.rebuild(32).unwrap();
    assert_eq!(rebuilt.entries, 129);
    assert_eq!(map.get(&7_u64.to_le_bytes()), Some(non_max(70)));
    assert_eq!(map.get(&8_u64.to_le_bytes()), None);
    assert_eq!(map.get(&9_u64.to_le_bytes()), Some(non_max(10)));
    assert_eq!(map.get(&200_u64.to_le_bytes()), Some(non_max(200)));
    assert_eq!(map.get(&201_u64.to_le_bytes()), Some(non_max(202)));
    assert_eq!(map.stats().current.overlay_records, 0);
}

#[test]
fn entry_reuses_a_miss_for_new_and_deleted_keys() {
    let deleted_base_key = binary_key(7);
    let new_key = binary_key(10_000);
    let overlay_key = binary_key(10_001);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..128_u64).map(|value| (binary_key(value), non_max(value))),
        16,
    )
    .unwrap();

    assert!(matches!(
        map.entry(&binary_key(8)),
        AtomicEntry::Occupied(value) if value == non_max(8)
    ));

    let AtomicEntry::Vacant(vacant) = map.entry(&new_key) else {
        panic!("new key must be vacant");
    };
    assert_eq!(vacant.key(), new_key);
    assert!(vacant.insert_new(non_max(10_000)));

    assert_eq!(map.remove(&deleted_base_key), Some(non_max(7)));
    let records_before = map.stats().current.overlay_records;
    let AtomicEntry::Vacant(vacant) = map.entry(&deleted_base_key) else {
        panic!("deleted frozen key must be vacant");
    };
    assert!(vacant.insert_new(non_max(700)));
    assert_eq!(map.stats().current.overlay_records, records_before);

    assert!(map.insert_new(&overlay_key, non_max(1)));
    assert_eq!(map.remove(&overlay_key), Some(non_max(1)));
    let AtomicEntry::Vacant(vacant) = map.entry(&overlay_key) else {
        panic!("deleted overlay key must be vacant");
    };
    assert_eq!(vacant.insert(non_max(2)), InsertOutcome::Inserted);

    assert_eq!(map.get(&new_key), Some(non_max(10_000)));
    assert_eq!(map.get(&deleted_base_key), Some(non_max(700)));
    assert_eq!(map.get(&overlay_key), Some(non_max(2)));
    assert_eq!(map.len(), 130);
}

#[test]
fn vacant_entry_loses_insert_new_race_without_overwriting() {
    let key = binary_key(50_000);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        8,
    )
    .unwrap();
    let AtomicEntry::Vacant(vacant) = map.entry(&key) else {
        panic!("new key must be vacant");
    };

    assert!(map.insert_new(&key, non_max(11)));
    assert!(!vacant.insert_new(non_max(22)));
    assert_eq!(map.get(&key), Some(non_max(11)));
    assert_eq!(map.len(), 1);
    assert_eq!(map.stats().current.overlay_records, 1);
}

#[test]
fn get_or_insert_fuses_hit_new_and_deleted_paths() {
    let base_key = binary_key(7);
    let new_key = binary_key(50_000);
    let overlay_key = binary_key(50_001);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..128_u64).map(|value| (binary_key(value), non_max(value))),
        8,
    )
    .unwrap();

    assert_eq!(map.get_or_insert(&base_key, non_max(700)), non_max(7));
    assert_eq!(map.len(), 128);
    assert_eq!(map.get_or_insert(&new_key, non_max(500)), non_max(500));
    assert_eq!(map.get_or_insert(&new_key, non_max(501)), non_max(500));

    assert_eq!(map.remove(&base_key), Some(non_max(7)));
    let records_before = map.stats().current.overlay_records;
    assert_eq!(map.get_or_insert(&base_key, non_max(700)), non_max(700));
    assert_eq!(map.stats().current.overlay_records, records_before);

    assert_eq!(map.get_or_insert(&overlay_key, non_max(800)), non_max(800));
    assert_eq!(map.remove(&overlay_key), Some(non_max(800)));
    assert_eq!(map.get_or_insert(&overlay_key, non_max(801)), non_max(801));

    assert_eq!(map.len(), 130);
    assert_eq!(map.get(&base_key), Some(non_max(700)));
    assert_eq!(map.get(&new_key), Some(non_max(500)));
    assert_eq!(map.get(&overlay_key), Some(non_max(801)));
}

#[test]
fn concurrent_get_or_insert_returns_one_winner_without_overwriting() {
    const THREADS: usize = 16;
    let key = binary_key(70_000);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        THREADS,
    )
    .unwrap();
    let start = Barrier::new(THREADS);
    let mut observed = Vec::with_capacity(THREADS);

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(THREADS);
        for thread in 0..THREADS {
            let map = &map;
            let start = &start;
            handles.push(scope.spawn(move || {
                start.wait();
                map.get_or_insert(&key, non_max(thread as u64 + 1))
            }));
        }
        for handle in handles {
            observed.push(handle.join().unwrap());
        }
    });

    let winner = map.get(&key).unwrap();
    assert!(observed.iter().all(|value| *value == winner));
    assert_eq!(map.len(), 1);
    assert_eq!(map.stats().current.overlay_records, 1);
}

#[cfg(feature = "prepared-batch-gate")]
#[test]
fn operation_guard_preserves_atomic_crud_and_length() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..32_u64).map(|value| (binary_key(value), non_max(value))),
        16,
    )
    .unwrap();
    let guard = map.operation_guard();
    let existing = binary_key(7);
    let inserted = binary_key(1_000);
    let upserted = binary_key(1_001);

    assert_eq!(guard.get(&existing), Some(non_max(7)));
    assert_eq!(
        guard.insert(&existing, non_max(70)),
        InsertOutcome::Replaced(non_max(7))
    );
    assert!(guard.insert_new(&inserted, non_max(1_000)));
    assert!(!guard.insert_new(&inserted, non_max(9_000)));
    assert_eq!(
        guard.get_or_insert(&inserted, non_max(9_001)),
        non_max(1_000)
    );
    assert_eq!(guard.update(&existing, increment), Some(non_max(71)));
    assert_eq!(
        guard.upsert(&upserted, non_max(1_001), increment),
        non_max(1_001)
    );
    assert_eq!(guard.remove_if(&existing, |_| false), None);
    assert_eq!(guard.remove(&existing), Some(non_max(71)));
    assert_eq!(map.len(), 33);
    drop(guard);

    assert_eq!(map.get(&existing), None);
    assert_eq!(map.get(&inserted), Some(non_max(1_000)));
    assert_eq!(map.get(&upserted), Some(non_max(1_001)));
    map.rebuild(16).unwrap();
    assert_eq!(map.len(), 33);

    let batch_keys = [binary_key(2_000), inserted, binary_key(2_001)];
    let batch_values = [non_max(2_000), non_max(9_000), non_max(2_001)];
    let mut batch_results = [non_max(0); 3];
    map.get_or_insert_batch(&batch_keys, &batch_values, &mut batch_results);
    assert_eq!(
        batch_results,
        [non_max(2_000), non_max(1_000), non_max(2_001)]
    );
    assert_eq!(map.len(), 35);
}

#[cfg(feature = "prepared-batch-gate")]
#[test]
fn operation_guards_publish_concurrent_distinct_keys_exactly() {
    const THREADS: usize = 8;
    const KEYS_PER_THREAD: usize = 256;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        THREADS * KEYS_PER_THREAD,
    )
    .unwrap();
    let start = Barrier::new(THREADS);

    thread::scope(|scope| {
        for worker in 0..THREADS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                let guard = map.operation_guard();
                start.wait();
                for offset in 0..KEYS_PER_THREAD {
                    let value = u64::try_from(worker * KEYS_PER_THREAD + offset).unwrap();
                    assert_eq!(
                        guard.get_or_insert(&binary_key(value), non_max(value)),
                        non_max(value)
                    );
                }
            });
        }
    });

    assert_eq!(map.len(), THREADS * KEYS_PER_THREAD);
    for value in 0..u64::try_from(THREADS * KEYS_PER_THREAD).unwrap() {
        assert_eq!(map.get(&binary_key(value)), Some(non_max(value)));
    }
}

#[cfg(feature = "prepared-batch-gate")]
#[test]
fn operation_guard_remains_exact_during_rebuild_handoff() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..1_024_u64).map(|value| (binary_key(value), non_max(value))),
        512,
    )
    .unwrap();
    let guard = map.operation_guard();
    let rebuild_started = AtomicBool::new(false);

    thread::scope(|scope| {
        let map = &map;
        let rebuild_started = &rebuild_started;
        let rebuild = scope.spawn(move || {
            rebuild_started.store(true, Ordering::Release);
            map.rebuild(512).unwrap();
        });
        while !rebuild_started.load(Ordering::Acquire) {
            thread::yield_now();
        }
        for value in 2_000..2_256_u64 {
            assert_eq!(
                guard.get_or_insert(&binary_key(value), non_max(value)),
                non_max(value)
            );
        }
        drop(guard);
        rebuild.join().unwrap();
    });

    assert_eq!(map.len(), 1_280);
    for value in 2_000..2_256_u64 {
        assert_eq!(map.get(&binary_key(value)), Some(non_max(value)));
    }
}

#[test]
fn vacant_entry_remains_exact_when_rebuild_redirects_writers() {
    let key = binary_key(80_000);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..1_024_u64).map(|value| (binary_key(value), non_max(value))),
        64,
    )
    .unwrap();
    let AtomicEntry::Vacant(vacant) = map.entry(&key) else {
        panic!("new key must be vacant");
    };
    let rebuild_started = AtomicBool::new(false);

    thread::scope(|scope| {
        let rebuild_started = &rebuild_started;
        let map = &map;
        scope.spawn(move || {
            rebuild_started.store(true, Ordering::Release);
            map.rebuild(64).unwrap();
        });

        while !rebuild_started.load(Ordering::Acquire) {
            thread::yield_now();
        }
        assert!(vacant.insert_new(non_max(80_000)));
    });

    assert_eq!(map.get(&key), Some(non_max(80_000)));
    assert_eq!(map.len(), 1_025);
    map.rebuild(64).unwrap();
    assert_eq!(map.get(&key), Some(non_max(80_000)));
    assert_eq!(map.stats().current.overlay_records, 0);
}

#[cfg(feature = "phast")]
#[test]
fn phast_plus_backend_preserves_exact_crud_and_rebuild_semantics() {
    const ENTRIES: u64 = 1_024;
    let map =
        LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash_and_index(
            (0..ENTRIES).map(|value| (binary_key(value), non_max(value))),
            128,
            AtomicGenerationOverlay::AtomicFixed32,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
            GenerationHashBuilder::default(),
            FrozenIndexBackend::PhastPlus,
        )
        .unwrap();

    for value in 0..ENTRIES {
        assert_eq!(map.get(&binary_key(value)), Some(non_max(value)));
    }
    for value in ENTRIES..ENTRIES + 256 {
        assert_eq!(map.get(&binary_key(value)), None);
    }

    assert_eq!(map.update(&binary_key(7), increment), Some(non_max(8)));
    assert_eq!(map.remove(&binary_key(8)), Some(non_max(8)));
    assert!(map.insert_new(&binary_key(ENTRIES + 1), non_max(20_000)));

    let rebuilt = map.rebuild(128).unwrap();
    assert_eq!(rebuilt.entries, usize::try_from(ENTRIES).unwrap());
    assert_eq!(map.get(&binary_key(7)), Some(non_max(8)));
    assert_eq!(map.get(&binary_key(8)), None);
    assert_eq!(map.get(&binary_key(ENTRIES + 1)), Some(non_max(20_000)));
    assert_eq!(map.stats().current.overlay_records, 0);
}

#[test]
fn base_filter_has_no_false_negatives_and_survives_direct_deletes() {
    const ENTRIES: usize = 4_096;
    for filter in [
        AtomicGenerationBaseFilter::Disabled,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
        AtomicGenerationBaseFilter::OneBytePerEntry,
    ] {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
            (0..ENTRIES as u64).map(|value| (binary_key(value), non_max(value))),
            128,
            AtomicGenerationOverlay::CompactFixed32,
            filter,
        )
        .unwrap();

        for value in 0..ENTRIES as u64 {
            assert_eq!(map.get(&binary_key(value)), Some(non_max(value)));
        }
        for value in 0..512_u64 {
            let key = binary_key(value);
            assert_eq!(map.remove(&key), Some(non_max(value)));
            assert_eq!(map.get(&key), None);
            assert!(map.insert_new(&key, non_max(value + 10_000)));
            assert_eq!(map.get(&key), Some(non_max(value + 10_000)));
        }
        for value in ENTRIES as u64..ENTRIES as u64 + 512 {
            let key = binary_key(value);
            assert_eq!(map.get(&key), None);
            assert_eq!(map.update(&key, increment), None);
            assert_eq!(map.remove(&key), None);
        }
        assert_eq!(
            map.stats().base_filter_bytes,
            match filter {
                AtomicGenerationBaseFilter::Disabled
                | AtomicGenerationBaseFilter::EmbeddedFingerprint => 0,
                AtomicGenerationBaseFilter::OneBytePerEntry => ENTRIES,
            }
        );

        map.rebuild(128).unwrap();
        assert_eq!(map.len(), ENTRIES);
        for value in 0..ENTRIES as u64 {
            let expected = if value < 512 { value + 10_000 } else { value };
            assert_eq!(map.get(&binary_key(value)), Some(non_max(expected)));
        }
    }
}

#[test]
fn embedded_fingerprint_falls_back_exactly_for_long_keys() {
    let short = vec![7_u8; 32];
    let long = vec![9_u8; 300];
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        [(&short, non_max(7)), (&long, non_max(9))],
        4,
        AtomicGenerationOverlay::CompactFixed32,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
    )
    .unwrap();

    assert_eq!(map.get(&short), Some(non_max(7)));
    assert_eq!(map.get(&long), Some(non_max(9)));
    assert_eq!(map.get(&vec![8_u8; 300]), None);
    assert_eq!(map.update(&long, increment), Some(non_max(10)));
    map.rebuild(4).unwrap();
    assert_eq!(map.get(&long), Some(non_max(10)));
    assert_eq!(map.stats().base_filter_bytes, 0);
}

#[cfg(feature = "shared-gx")]
#[test]
fn randomized_two_key_generations_use_the_exact_linear_fallback() {
    let short = vec![7_u8; 32];
    let long = vec![9_u8; 300];
    for _ in 0..256 {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash(
            [(&short, non_max(7)), (&long, non_max(9))],
            0,
            AtomicGenerationOverlay::AtomicFixed32,
            AtomicGenerationBaseFilter::EmbeddedFingerprint,
            GenerationHashBuilder::default(),
        )
        .unwrap();
        assert_eq!(map.get(&short), Some(non_max(7)));
        assert_eq!(map.get(&long), Some(non_max(9)));
        assert_eq!(map.get(&vec![8_u8; 300]), None);
    }
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_keys_remain_exact_across_keys_maps_mutations_and_rebuilds() {
    assert_eq!(size_of::<AtomicPreparedKey>(), 16);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..32_u64).map(|value| (binary_key(value), non_max(value))),
        8,
    )
    .unwrap();
    let key = binary_key(7);
    let prepared = map.prepare_key(&key);
    assert!(prepared.has_direct_slot());
    assert_eq!(map.get_prepared(&key, &prepared), Some(non_max(7)));
    assert_eq!(
        map.update_prepared(&key, &prepared, increment),
        Some(non_max(8))
    );

    let different = binary_key(8);
    assert_eq!(map.get_prepared(&different, &prepared), Some(non_max(8)));
    assert_eq!(
        map.update_prepared(&different, &prepared, increment),
        Some(non_max(9))
    );
    assert_eq!(
        map.insert_prepared(&different, &prepared, non_max(80)),
        InsertOutcome::Replaced(non_max(9))
    );

    let other = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..32_u64).map(|value| (binary_key(value), non_max(value + 100))),
        0,
    )
    .unwrap();
    assert_eq!(other.get_prepared(&key, &prepared), Some(non_max(107)));

    assert_eq!(map.remove_prepared(&key, &prepared), Some(non_max(8)));
    assert_eq!(map.len(), 31);
    assert_eq!(map.get_prepared(&key, &prepared), None);
    assert_eq!(
        map.insert_prepared(&key, &prepared, non_max(70)),
        InsertOutcome::Inserted
    );
    assert_eq!(map.len(), 32);
    assert_eq!(map.get_prepared(&key, &prepared), Some(non_max(70)));

    map.rebuild(8).unwrap();
    assert_eq!(map.get_prepared(&key, &prepared), Some(non_max(70)));
    assert_eq!(
        map.update_prepared(&key, &prepared, increment),
        Some(non_max(71))
    );
    let refreshed = map.prepare_key(&key);
    assert!(refreshed.has_direct_slot());
    assert_ne!(prepared, refreshed);

    let overlay_key = binary_key(1_000);
    assert!(map.insert_new(&overlay_key, non_max(1_000)));
    let overlay_prepared = map.prepare_key(&overlay_key);
    assert!(!overlay_prepared.has_direct_slot());
    assert_eq!(
        map.get_prepared(&overlay_key, &overlay_prepared),
        Some(non_max(1_000))
    );
    assert_eq!(
        map.update_prepared(&overlay_key, &overlay_prepared, increment),
        Some(non_max(1_001))
    );
}

#[cfg(feature = "prepared-keys")]
#[test]
fn adaptive_learning_sample_exposes_exact_native_prepared_reads() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        8_192,
        AtomicGenerationOverlay::AtomicAdaptive,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
    )
    .unwrap();
    let keys = (0..256_u64).map(binary_key).collect::<Vec<_>>();
    for (value, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(value as u64 + 1)));
    }

    let prepared = map.prepare_key(&keys[0]);
    assert!(prepared.has_native_slot());
    assert!(!prepared.has_direct_slot());
    assert_eq!(map.get_prepared(&keys[0], &prepared), Some(non_max(1)));
    assert_eq!(
        map.get_prepared(&keys[1], &prepared),
        Some(non_max(2)),
        "a wrong native handle must fall back to an exact lookup"
    );
    assert_eq!(map.remove_prepared(&keys[0], &prepared), Some(non_max(1)));
    assert_eq!(map.get_prepared(&keys[0], &prepared), None);
    assert_eq!(
        map.insert_prepared(&keys[0], &prepared, non_max(999)),
        InsertOutcome::Inserted
    );
    assert_eq!(map.get_prepared(&keys[0], &prepared), Some(non_max(999)));
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_batches_preserve_exact_fallbacks_and_stale_handle_semantics() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..64_u64).map(|value| (binary_key(value), non_max(value + 10))),
        8,
    )
    .unwrap();
    let other = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..64_u64).map(|value| (binary_key(value), non_max(value + 1_000))),
        0,
    )
    .unwrap();

    let keys = [binary_key(3), binary_key(4), binary_key(5), binary_key(6)];
    let prepared = [
        map.prepare_key(&keys[0]),
        map.prepare_key(&binary_key(40)),
        other.prepare_key(&keys[2]),
        map.prepare_key(&keys[3]),
    ];
    assert_eq!(map.remove(&keys[3]), Some(non_max(16)));

    let mut values = [Some(non_max(1)); 4];
    map.get_prepared_batch(&keys, &prepared, &mut values);
    assert_eq!(
        values,
        [
            Some(non_max(13)),
            Some(non_max(14)),
            Some(non_max(15)),
            None
        ]
    );

    assert_eq!(map.insert(&keys[3], non_max(600)), InsertOutcome::Inserted);
    map.rebuild(8).unwrap();
    map.get_prepared_batch(&keys, &prepared, &mut values);
    assert_eq!(
        values,
        [
            Some(non_max(13)),
            Some(non_max(14)),
            Some(non_max(15)),
            Some(non_max(600))
        ]
    );
    let mut refreshed = [AtomicPreparedKey::fallback(); 4];
    map.prepare_key_batch(&keys, &mut refreshed);
    assert!(refreshed.iter().all(|handle| handle.has_direct_slot()));
    assert_ne!(prepared, refreshed);
    map.get_prepared_batch(&keys, &refreshed, &mut values);
    assert_eq!(
        values,
        [
            Some(non_max(13)),
            Some(non_max(14)),
            Some(non_max(15)),
            Some(non_max(600))
        ]
    );

    let overlay_key = binary_key(10_000);
    assert!(map.insert_new(&overlay_key, non_max(10_000)));
    let overlay_keys = [overlay_key];
    let overlay_prepared = [map.prepare_key(&overlay_key)];
    assert!(!overlay_prepared[0].has_direct_slot());
    let mut overlay_values = [None];
    map.get_prepared_batch(&overlay_keys, &overlay_prepared, &mut overlay_values);
    assert_eq!(overlay_values, [Some(non_max(10_000))]);
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_batches_remain_exact_during_repeated_rebuilds() {
    const KEYS: usize = 1_024;
    const READERS: usize = 4;
    const ROUNDS: usize = 200;
    const BATCH: usize = 64;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..KEYS as u64).map(|value| (binary_key(value), non_max(value))),
        64,
    )
    .unwrap();
    let keys = (0..KEYS as u64).map(binary_key).collect::<Vec<_>>();
    let prepared = keys
        .iter()
        .map(|key| map.prepare_key(key))
        .collect::<Vec<_>>();
    let start = Barrier::new(READERS + 1);

    thread::scope(|scope| {
        for reader in 0..READERS {
            let map = &map;
            let keys = &keys;
            let prepared = &prepared;
            let start = &start;
            scope.spawn(move || {
                let mut values = [None; BATCH];
                start.wait();
                for round in 0..ROUNDS {
                    let first = (round * BATCH + reader * 17) % (KEYS - BATCH);
                    map.get_prepared_batch(
                        &keys[first..first + BATCH],
                        &prepared[first..first + BATCH],
                        &mut values,
                    );
                    for (offset, value) in values.iter().enumerate() {
                        assert_eq!(*value, Some(non_max((first + offset) as u64)));
                    }
                }
            });
        }

        start.wait();
        for _ in 0..8 {
            map.rebuild(64).unwrap();
        }
    });
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_write_batches_preserve_crud_fallbacks_and_length() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..64_u64).map(|value| (binary_key(value), non_max(value + 10))),
        8,
    )
    .unwrap();
    let other = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..64_u64).map(|value| (binary_key(value), non_max(value + 1_000))),
        0,
    )
    .unwrap();
    let overlay_key = binary_key(10_000);
    assert!(map.insert_new(&overlay_key, non_max(10_000)));

    let keys = [
        binary_key(3),
        binary_key(4),
        binary_key(5),
        binary_key(6),
        overlay_key,
    ];
    let prepared = [
        map.prepare_key(&keys[0]),
        map.prepare_key(&binary_key(40)),
        other.prepare_key(&keys[2]),
        map.prepare_key(&keys[3]),
        AtomicPreparedKey::fallback(),
    ];
    assert_eq!(map.remove(&keys[3]), Some(non_max(16)));
    assert_eq!(map.len(), 64);

    let mut updated = [Some(non_max(1)); 5];
    map.update_prepared_batch(&keys, &prepared, &mut updated, increment);
    assert_eq!(
        updated,
        [
            Some(non_max(14)),
            Some(non_max(15)),
            Some(non_max(16)),
            None,
            Some(non_max(10_001))
        ]
    );

    let replacements = [
        non_max(130),
        non_max(140),
        non_max(150),
        non_max(160),
        non_max(20_000),
    ];
    let mut replaced = [Some(non_max(1)); 5];
    map.replace_prepared_batch(&keys, &prepared, &replacements, &mut replaced);
    assert_eq!(
        replaced,
        [
            Some(non_max(14)),
            Some(non_max(15)),
            Some(non_max(16)),
            None,
            Some(non_max(10_001))
        ]
    );
    assert_eq!(map.get(&keys[3]), None);
    assert_eq!(map.len(), 64);

    let inserted = [
        non_max(30),
        non_max(40),
        non_max(50),
        non_max(60),
        non_max(20_000),
    ];
    let mut previous = [None; 5];
    map.insert_prepared_batch(&keys, &prepared, &inserted, &mut previous);
    assert_eq!(
        previous,
        [
            Some(non_max(130)),
            Some(non_max(140)),
            Some(non_max(150)),
            None,
            Some(non_max(20_000))
        ]
    );
    assert_eq!(map.len(), 65);

    map.rebuild(8).unwrap();
    map.update_prepared_batch(&keys, &prepared, &mut updated, increment);
    assert_eq!(
        updated,
        [
            Some(non_max(31)),
            Some(non_max(41)),
            Some(non_max(51)),
            Some(non_max(61)),
            Some(non_max(20_001))
        ]
    );
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_update_batches_remain_ordered_during_repeated_rebuilds() {
    const KEYS: usize = 512;
    const WRITERS: usize = 4;
    const ROUNDS: usize = 100;
    const BATCH: usize = 16;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..KEYS as u64).map(|value| (binary_key(value), non_max(value))),
        64,
    )
    .unwrap();
    let keys = (0..KEYS as u64).map(binary_key).collect::<Vec<_>>();
    let prepared = keys
        .iter()
        .map(|key| map.prepare_key(key))
        .collect::<Vec<_>>();
    let start = Barrier::new(WRITERS + 1);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let keys = &keys;
            let prepared = &prepared;
            let start = &start;
            scope.spawn(move || {
                let first = writer * KEYS / WRITERS;
                let end = (writer + 1) * KEYS / WRITERS;
                let mut updated = [None; BATCH];
                start.wait();
                for _ in 0..ROUNDS {
                    for batch_first in (first..end).step_by(BATCH) {
                        let batch_end = (batch_first + BATCH).min(end);
                        map.update_prepared_batch(
                            &keys[batch_first..batch_end],
                            &prepared[batch_first..batch_end],
                            &mut updated[..batch_end - batch_first],
                            increment,
                        );
                        assert!(
                            updated[..batch_end - batch_first]
                                .iter()
                                .all(Option::is_some)
                        );
                    }
                }
            });
        }

        start.wait();
        for _ in 0..8 {
            map.rebuild(64).unwrap();
        }
    });

    for value in 0..KEYS as u64 {
        assert_eq!(
            map.get(&binary_key(value)),
            Some(non_max(value + ROUNDS as u64))
        );
    }
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_replacement_batches_return_exact_old_values_during_rebuilds() {
    const KEYS: usize = 512;
    const WRITERS: usize = 4;
    const ROUNDS: usize = 100;
    const BATCH: usize = 16;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..KEYS as u64).map(|value| (binary_key(value), non_max(value))),
        64,
    )
    .unwrap();
    let keys = (0..KEYS as u64).map(binary_key).collect::<Vec<_>>();
    let prepared = keys
        .iter()
        .map(|key| map.prepare_key(key))
        .collect::<Vec<_>>();
    let start = Barrier::new(WRITERS + 1);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let keys = &keys;
            let prepared = &prepared;
            let start = &start;
            scope.spawn(move || {
                let first = writer * KEYS / WRITERS;
                let end = (writer + 1) * KEYS / WRITERS;
                let mut values = [non_max(0); BATCH];
                let mut previous = [None; BATCH];
                start.wait();
                for round in 0..ROUNDS {
                    for batch_first in (first..end).step_by(BATCH) {
                        let batch_end = (batch_first + BATCH).min(end);
                        let count = batch_end - batch_first;
                        for (offset, value) in values[..count].iter_mut().enumerate() {
                            *value = non_max((batch_first + offset + round + 1) as u64);
                        }
                        map.replace_prepared_batch(
                            &keys[batch_first..batch_end],
                            &prepared[batch_first..batch_end],
                            &values[..count],
                            &mut previous[..count],
                        );
                        for (offset, old) in previous[..count].iter().enumerate() {
                            assert_eq!(*old, Some(non_max((batch_first + offset + round) as u64)));
                        }
                    }
                }
            });
        }

        start.wait();
        for _ in 0..8 {
            map.rebuild(64).unwrap();
        }
    });

    for value in 0..KEYS as u64 {
        assert_eq!(
            map.get(&binary_key(value)),
            Some(non_max(value + ROUNDS as u64))
        );
    }
}

#[cfg(feature = "prepared-keys")]
#[test]
fn prepared_updates_remain_ordered_during_repeated_rebuilds() {
    const KEYS: usize = 512;
    const WRITERS: usize = 4;
    const ROUNDS: usize = 100;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..KEYS as u64).map(|value| (binary_key(value), non_max(value))),
        64,
    )
    .unwrap();
    let prepared = (0..KEYS as u64)
        .map(|value| map.prepare_key(&binary_key(value)))
        .collect::<Vec<_>>();
    let start = Barrier::new(WRITERS + 1);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let prepared = &prepared;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..ROUNDS {
                    for index in (writer..KEYS).step_by(WRITERS) {
                        let key = binary_key(index as u64);
                        assert!(
                            map.update_prepared(&key, &prepared[index], increment)
                                .is_some()
                        );
                    }
                }
            });
        }
        start.wait();
        for _ in 0..4 {
            map.rebuild(64).unwrap();
        }
    });

    for value in 0..KEYS as u64 {
        assert_eq!(
            map.get(&binary_key(value)),
            Some(non_max(value + ROUNDS as u64))
        );
    }
}

#[test]
fn direct_base_deletes_scale_accounting_without_tombstone_records() {
    const ENTRIES: usize = 100_000;
    const WRITERS: usize = 8;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..ENTRIES as u64).map(|value| (value.to_le_bytes(), non_max(value))),
        0,
    )
    .unwrap();
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            scope.spawn(move || {
                for value in (writer..ENTRIES).step_by(WRITERS) {
                    assert_eq!(
                        map.remove(&(value as u64).to_le_bytes()),
                        Some(non_max(value as u64))
                    );
                }
            });
        }
    });

    assert!(map.is_empty());
    assert_eq!(map.len(), 0);
    assert_eq!(map.stats().current.overlay_records, 0);
}

#[test]
fn direct_and_overlay_mutations_converge_during_rebuild() {
    const KEYS: usize = 512;
    const WRITERS: usize = 4;
    const ROUNDS: usize = 200;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        (0..KEYS as u64).map(|value| (value.to_le_bytes(), non_max(value))),
        128,
    )
    .unwrap();
    let start = Barrier::new(WRITERS + 1);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for round in 0..ROUNDS {
                    for value in (writer..KEYS).step_by(WRITERS) {
                        let key = (value as u64).to_le_bytes();
                        if round % 2 == 0 {
                            assert_eq!(map.remove(&key), Some(non_max(value as u64)));
                        } else {
                            assert!(map.insert_new(&key, non_max(value as u64)));
                        }
                    }
                }
            });
        }
        start.wait();
        for _ in 0..4 {
            map.rebuild(128).unwrap();
        }
    });

    assert_eq!(map.len(), KEYS);
    for value in 0..KEYS as u64 {
        assert_eq!(map.get(&value.to_le_bytes()), Some(non_max(value)));
    }
    assert_eq!(map.stats().layer_depth, 1);
}

#[test]
fn concurrent_atomic_updates_are_not_lost_across_cutovers() {
    const WRITERS: usize = 8;
    const UPDATES: usize = 20_000;
    const REBUILDS: usize = 4;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        std::iter::once((b"counter".as_slice(), non_max(0))),
        64,
        AtomicGenerationOverlay::CompactFixed32,
        AtomicGenerationBaseFilter::EmbeddedFingerprint,
    )
    .unwrap();
    let start = Barrier::new(WRITERS + 1);
    thread::scope(|scope| {
        for _ in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..UPDATES {
                    map.update(b"counter", increment).unwrap();
                }
            });
        }
        start.wait();
        for _ in 0..REBUILDS {
            map.rebuild(64).unwrap();
        }
    });

    assert_eq!(
        map.get(b"counter").map(NonMaxU64::get),
        Some((WRITERS * UPDATES) as u64)
    );
    assert_eq!(map.generation(), REBUILDS as u64);
}

#[test]
fn clean_and_shadowed_read_routes_never_move_backward_across_cutovers() {
    const UPDATES: usize = 500_000;
    const REBUILDS: usize = 16;

    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_options(
        std::iter::once((b"counter".as_slice(), non_max(0))),
        64,
        AtomicGenerationOverlay::CompactFixed32,
        AtomicGenerationBaseFilter::OneBytePerEntry,
    )
    .unwrap();
    let start = Barrier::new(3);
    let updating = AtomicBool::new(true);
    thread::scope(|scope| {
        let map_ref = &map;
        let start_ref = &start;
        let updating_ref = &updating;
        scope.spawn(move || {
            start_ref.wait();
            for _ in 0..UPDATES {
                map_ref.update(b"counter", increment).unwrap();
            }
            updating_ref.store(false, Ordering::Release);
        });

        let map_ref = &map;
        let start_ref = &start;
        let updating_ref = &updating;
        scope.spawn(move || {
            start_ref.wait();
            let mut previous = 0;
            while updating_ref.load(Ordering::Acquire) {
                let current = map_ref.get(b"counter").unwrap().get();
                assert!(current >= previous);
                previous = current;
            }
        });

        start.wait();
        for _ in 0..REBUILDS {
            map.rebuild(64).unwrap();
        }
    });

    assert_eq!(map.get(b"counter").unwrap().get(), UPDATES as u64);
}

#[test]
fn compact_overlay_publishes_one_copy_of_a_racing_new_key() {
    const WRITERS: usize = 16;
    let key = binary_key(42);
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        WRITERS,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);
    let inserted = std::sync::atomic::AtomicUsize::new(0);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            let inserted = &inserted;
            scope.spawn(move || {
                start.wait();
                if map.insert_new(&key, non_max(writer as u64)) {
                    inserted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(inserted.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(map.len(), 1);
    assert!(map.get(&key).is_some());
    assert_eq!(map.stats().current.overlay_records, 1);
}

#[test]
fn atomic_bucket_publishes_adjacent_slots_without_lost_entries() {
    const WRITERS: usize = 8;
    const ROUNDS: usize = 64;
    for _ in 0..ROUNDS {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
            std::iter::empty::<([u8; 32], NonMaxU64)>(),
            1,
            AtomicGenerationOverlay::AtomicFixed32,
        )
        .unwrap();
        let start = Barrier::new(WRITERS);

        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let map = &map;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let key = binary_key(writer as u64);
                    assert!(map.insert_new(&key, non_max(writer as u64)));
                });
            }
        });

        assert_eq!(map.len(), WRITERS);
        assert_eq!(map.stats().current.overlay_records, WRITERS);
        for writer in 0..WRITERS {
            assert_eq!(
                map.get(&binary_key(writer as u64)),
                Some(non_max(writer as u64))
            );
        }
    }
}

#[test]
fn atomic_overflow_initialization_is_exact_under_race() {
    const WRITERS: usize = 64;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        1,
        AtomicGenerationOverlay::AtomicFixed32,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                assert!(map.insert_new(&binary_key(writer as u64), non_max(writer as u64)));
            });
        }
    });

    assert_eq!(map.len(), WRITERS);
    for writer in 0..WRITERS {
        assert_eq!(
            map.get(&binary_key(writer as u64)),
            Some(non_max(writer as u64))
        );
    }
}

#[test]
fn compact_overlay_overflow_and_variable_keys_remain_exact() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<([u8; 32], NonMaxU64)>(),
        1,
    )
    .unwrap();

    for value in 0..256_u64 {
        assert!(map.insert_new(&binary_key(value), non_max(value)));
    }
    for value in 0..64_u64 {
        let key = format!("variable-key-{value}");
        assert!(map.insert_new(key.as_bytes(), non_max(1_000 + value)));
    }
    assert_eq!(map.len(), 320);

    map.rebuild(1).unwrap();
    for value in 0..256_u64 {
        assert_eq!(map.get(&binary_key(value)), Some(non_max(value)));
    }
    for value in 0..64_u64 {
        let key = format!("variable-key-{value}");
        assert_eq!(map.get(key.as_bytes()), Some(non_max(1_000 + value)));
    }
    assert_eq!(map.stats().current.overlay_records, 0);
}

#[test]
fn compact_and_papaya_overlay_modes_have_matching_semantics() {
    for overlay in [
        AtomicGenerationOverlay::AtomicFixed32,
        AtomicGenerationOverlay::CompactFixed32,
        AtomicGenerationOverlay::ArcSwapFixed32,
        AtomicGenerationOverlay::Papaya,
    ] {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
            std::iter::once((binary_key(1), non_max(1))),
            8,
            overlay,
        )
        .unwrap();
        let compact = binary_key(2);
        assert!(map.insert_new(&compact, non_max(2)));
        assert_eq!(
            map.insert(&compact, non_max(3)),
            InsertOutcome::Replaced(non_max(2))
        );
        assert!(map.insert_new(b"short", non_max(4)));
        assert_eq!(map.update(b"short", increment), Some(non_max(5)));
        assert_eq!(map.remove(&compact), Some(non_max(3)));
        assert_eq!(map.len(), 2);
        map.rebuild(8).unwrap();
        assert_eq!(map.get(&binary_key(1)), Some(non_max(1)));
        assert_eq!(map.get(&compact), None);
        assert_eq!(map.get(b"short"), Some(non_max(5)));
    }
}

#[test]
fn inline_size_classes_preserve_every_boundary_and_fallback() {
    const LENGTHS: [usize; 19] = [
        0, 1, 8, 9, 16, 17, 24, 25, 31, 32, 33, 40, 41, 48, 49, 56, 57, 64, 65,
    ];
    let map = LockFreeAtomicU64GenerationMap::try_from_entries(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        LENGTHS.len(),
    )
    .unwrap();

    for (index, length) in LENGTHS.into_iter().enumerate() {
        assert!(map.insert_new(&sized_key(length), non_max(index as u64)));
    }
    for (index, length) in LENGTHS.into_iter().enumerate() {
        let key = sized_key(length);
        assert_eq!(map.get(&key), Some(non_max(index as u64)));
        assert_eq!(map.update(&key, increment), Some(non_max(index as u64 + 1)));
    }
    assert_eq!(map.len(), LENGTHS.len());

    map.rebuild(LENGTHS.len()).unwrap();
    for (index, length) in LENGTHS.into_iter().enumerate() {
        let key = sized_key(length);
        assert_eq!(map.get(&key), Some(non_max(index as u64 + 1)));
        assert_eq!(map.remove(&key), Some(non_max(index as u64 + 1)));
    }
    assert!(map.is_empty());
}

#[test]
fn configured_inline_classes_round_trip_without_boxed_key_semantics() {
    for length in [8_usize, 16, 24, 31, 40, 48, 56, 64] {
        let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
            std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
            4,
            AtomicGenerationOverlay::CompactSized {
                key_bytes: u8::try_from(length).unwrap(),
            },
        )
        .unwrap();
        let key = sized_key(length);
        assert!(map.insert_new(&key, non_max(length as u64)));
        assert_eq!(map.get(&key), Some(non_max(length as u64)));
        assert_eq!(
            map.update(&key, increment),
            Some(non_max(length as u64 + 1))
        );
        map.rebuild(4).unwrap();
        assert_eq!(map.remove(&key), Some(non_max(length as u64 + 1)));
        assert!(map.is_empty());
    }
}

#[test]
fn atomic_adaptive_mixed_lengths_remain_exact_across_learning_and_rebuild() {
    const LENGTHS: [usize; 14] = [1, 8, 9, 16, 17, 24, 25, 32, 33, 48, 49, 64, 97, 128];
    const ENTRIES: usize = 512;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        ENTRIES,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();
    let keys = (0..ENTRIES)
        .map(|index| adaptive_test_key(index, LENGTHS[index % LENGTHS.len()]))
        .collect::<Vec<_>>();

    for (index, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(index as u64)));
        assert!(!map.insert_new(key, non_max(10_000 + index as u64)));
    }
    assert_eq!(map.len(), ENTRIES);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(non_max(index as u64)));
        assert_eq!(map.update(key, increment), Some(non_max(index as u64 + 1)));
    }

    map.rebuild(ENTRIES).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(non_max(index as u64 + 1)));
        assert_eq!(map.remove(key), Some(non_max(index as u64 + 1)));
    }
    assert!(map.is_empty());
}

#[test]
fn atomic_adaptive_exact_48_class_round_trips_without_fallback_spill() {
    const ENTRIES: usize = 512;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        ENTRIES,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();

    for index in 0..ENTRIES {
        assert!(map.insert_new(&adaptive_test_key(index, 48), non_max(index as u64 + 1)));
    }
    let stats = map.adaptive_overlay_stats().unwrap();
    assert_eq!(stats.sample_key_classes, [0, 0, 0, 0, 64, 0]);
    assert_eq!(stats.planned_atomic_capacities, [0, 0, 0, 0, 448]);
    assert_eq!(stats.learned_key_classes, [0, 0, 0, 0, 448, 0]);
    assert_eq!(stats.short_key_fallback_insertions, 0);

    for index in 0..ENTRIES {
        let key = adaptive_test_key(index, 48);
        assert_eq!(map.get(&key), Some(non_max(index as u64 + 1)));
        assert_eq!(map.update(&key, increment), Some(non_max(index as u64 + 2)));
    }
    map.rebuild(ENTRIES).unwrap();
    for index in 0..ENTRIES {
        assert_eq!(
            map.remove(&adaptive_test_key(index, 48)),
            Some(non_max(index as u64 + 2))
        );
    }
    assert!(map.is_empty());
}

#[test]
fn atomic_adaptive_learning_handoff_has_one_winner_per_key() {
    const CAPACITY: usize = 4_096;
    const PREFIX: usize = 63;
    const DISTINCT_RACING: usize = 32;
    const WRITERS: usize = DISTINCT_RACING * 2;
    const LENGTHS: [usize; 4] = [8, 16, 64, 97];
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        CAPACITY,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();
    for index in 0..PREFIX {
        let key = adaptive_test_key(index, LENGTHS[index % LENGTHS.len()]);
        assert!(map.insert_new(&key, non_max(index as u64)));
    }

    let start = Barrier::new(WRITERS);
    let winners = AtomicUsize::new(0);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            let winners = &winners;
            scope.spawn(move || {
                let key_index = PREFIX + writer % DISTINCT_RACING;
                let key = adaptive_test_key(key_index, LENGTHS[key_index % LENGTHS.len()]);
                start.wait();
                if map.insert_new(&key, non_max(key_index as u64)) {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(winners.load(Ordering::Relaxed), DISTINCT_RACING);
    assert_eq!(map.len(), PREFIX + DISTINCT_RACING);
    for index in 0..PREFIX + DISTINCT_RACING {
        let key = adaptive_test_key(index, LENGTHS[index % LENGTHS.len()]);
        assert_eq!(map.get(&key), Some(non_max(index as u64)));
    }
    let adaptive = map.adaptive_overlay_stats().unwrap();
    assert_eq!(
        adaptive.sample_key_classes.iter().sum::<usize>(),
        adaptive.sampled_records
    );
    assert_eq!(
        adaptive.learned_key_classes.iter().sum::<usize>(),
        adaptive.learned_insertions
    );
    assert_eq!(adaptive.occupied_slots, PREFIX + DISTINCT_RACING);
}

#[test]
fn atomic_adaptive_stats_detect_distribution_drift_and_rebuild_exactly() {
    const CAPACITY: usize = 1_024;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        CAPACITY,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();

    for index in 0..64 {
        assert!(map.insert_new(&adaptive_test_key(index, 8), non_max(index as u64 + 1)));
    }
    let learned = map.adaptive_overlay_stats().unwrap();
    assert_eq!(learned.phase, AdaptiveOverlayPhase::Ready);
    assert_eq!(learned.sampled_records, 64);
    assert_eq!(learned.sample_key_classes, [64, 0, 0, 0, 0, 0]);
    assert_eq!(learned.planned_atomic_capacities, [960, 0, 0, 0, 0]);

    for index in 64..320 {
        assert!(map.insert_new(&adaptive_test_key(index, 32), non_max(index as u64 + 1)));
    }
    let drifted = map.adaptive_overlay_stats().unwrap();
    assert_eq!(drifted.learned_key_classes, [0, 0, 0, 256, 0, 0]);
    assert_eq!(drifted.distribution_drift_bps, 10_000);
    assert!(drifted.short_key_spill_bps >= 8_000);
    let recommendation = drifted.recommendation(AdaptiveRebuildPolicy::default());
    assert!(!recommendation.capacity_pressure);
    assert!(recommendation.distribution_drift);
    assert!(recommendation.short_key_spill);

    let rebuilt = map
        .rebuild_adaptive_if_needed(AdaptiveRebuildPolicy::default())
        .unwrap()
        .expect("drift recommends one rebuild");
    assert_eq!(rebuilt.entries, 320);
    assert_eq!(map.generation(), 1);
    let reset = map.adaptive_overlay_stats().unwrap();
    assert_eq!(reset.phase, AdaptiveOverlayPhase::Sampling);
    assert_eq!(reset.capacity, CAPACITY);
    assert_eq!(reset.occupied_slots, 0);
    for index in 0..320 {
        let length = if index < 64 { 8 } else { 32 };
        assert_eq!(
            map.get(&adaptive_test_key(index, length)),
            Some(non_max(index as u64 + 1))
        );
    }
}

#[test]
fn atomic_adaptive_capacity_counts_deleted_slots_until_maintenance() {
    const CAPACITY: usize = 256;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        CAPACITY,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();
    let keys = (0..192)
        .map(|index| adaptive_test_key(index, 8))
        .collect::<Vec<_>>();
    for (index, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(index as u64 + 1)));
    }
    for key in &keys[..100] {
        assert!(map.remove(key).is_some());
    }

    let before = map.adaptive_overlay_stats().unwrap();
    assert_eq!(before.occupied_slots, 192);
    assert_eq!(before.slot_utilization_bps, 7_500);
    assert!(
        before
            .recommendation(AdaptiveRebuildPolicy::default())
            .capacity_pressure
    );
    let rebuilt = map
        .rebuild_adaptive_if_needed(AdaptiveRebuildPolicy::default())
        .unwrap()
        .expect("deleted physical slots recommend rebuild");
    assert_eq!(rebuilt.entries, 92);
    assert_eq!(map.len(), 92);
    assert_eq!(map.adaptive_overlay_stats().unwrap().occupied_slots, 0);
}

#[test]
fn atomic_adaptive_concurrent_maintenance_publishes_only_once() {
    const WORKERS: usize = 8;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        256,
        AtomicGenerationOverlay::AtomicAdaptive,
    )
    .unwrap();
    for index in 0..192 {
        assert!(map.insert_new(&adaptive_test_key(index, 8), non_max(index as u64 + 1)));
    }

    let barrier = Barrier::new(WORKERS);
    let rebuilt = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..WORKERS {
            scope.spawn(|| {
                barrier.wait();
                if map
                    .rebuild_adaptive_if_needed(AdaptiveRebuildPolicy::default())
                    .unwrap()
                    .is_some()
                {
                    rebuilt.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(rebuilt.load(Ordering::Relaxed), 1);
    assert_eq!(map.generation(), 1);
    assert_eq!(map.len(), 192);
}

#[test]
fn adaptive_maintenance_is_a_noop_for_other_overlay_modes() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<([u8; 8], NonMaxU64)>(),
        8,
        AtomicGenerationOverlay::Papaya,
    )
    .unwrap();
    assert_eq!(map.adaptive_overlay_stats(), None);
    assert_eq!(
        map.rebuild_adaptive_if_needed(AdaptiveRebuildPolicy::default())
            .unwrap(),
        None
    );
}

#[test]
fn atomic_tiny_keys_preserve_lengths_aliases_and_fallback() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        24,
        AtomicGenerationOverlay::AtomicUpTo8,
    )
    .unwrap();
    let mut keys = (0..=8)
        .map(|length| {
            (0..length)
                .map(|offset| u8::try_from((length * 23 + offset) % 251).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    keys.push(vec![3; 9]);
    keys.push(vec![1]);
    let mut full_width_alias = vec![0; 8];
    full_width_alias[0] = 1;
    full_width_alias[7] = 1;
    keys.push(full_width_alias);

    for (index, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(index as u64)));
    }
    map.rebuild(24).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(non_max(index as u64)));
        assert_eq!(map.update(key, increment), Some(non_max(index as u64 + 1)));
    }
    map.rebuild(24).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.remove(key), Some(non_max(index as u64 + 1)));
    }
    assert!(map.is_empty());
}

#[test]
fn atomic_tiny_overflow_initialization_is_exact_under_race() {
    const WRITERS: usize = 64;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        1,
        AtomicGenerationOverlay::AtomicUpTo8,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                let key = writer.to_le_bytes();
                assert!(map.insert_new(&key, non_max(writer as u64)));
            });
        }
    });

    assert_eq!(map.len(), WRITERS);
    for writer in 0..WRITERS {
        assert_eq!(map.get(&writer.to_le_bytes()), Some(non_max(writer as u64)));
    }
}

#[test]
fn atomic_small_keys_preserve_lengths_aliases_and_fallback() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        32,
        AtomicGenerationOverlay::AtomicUpTo16,
    )
    .unwrap();
    let mut keys = (0..=16)
        .map(|length| {
            (0..length)
                .map(|offset| u8::try_from((length * 19 + offset) % 251).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    keys.push(vec![3; 17]);
    keys.push(vec![1]);
    let mut full_width_alias = vec![0; 16];
    full_width_alias[0] = 1;
    full_width_alias[15] = 1;
    keys.push(full_width_alias);

    for (index, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(index as u64)));
    }
    map.rebuild(32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(non_max(index as u64)));
        assert_eq!(map.update(key, increment), Some(non_max(index as u64 + 1)));
    }
    map.rebuild(32).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.remove(key), Some(non_max(index as u64 + 1)));
    }
    assert!(map.is_empty());
}

#[test]
fn atomic_small_overflow_initialization_is_exact_under_race() {
    const WRITERS: usize = 64;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        1,
        AtomicGenerationOverlay::AtomicUpTo16,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                let key = writer.to_le_bytes();
                assert!(map.insert_new(&key, non_max(writer as u64)));
            });
        }
    });

    assert_eq!(map.len(), WRITERS);
    for writer in 0..WRITERS {
        assert_eq!(map.get(&writer.to_le_bytes()), Some(non_max(writer as u64)));
    }
}

#[test]
fn atomic_short_keys_preserve_lengths_fallback_and_rebuilds() {
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        64,
        AtomicGenerationOverlay::AtomicUpTo32,
    )
    .unwrap();
    let mut keys = (0..=31)
        .map(|length| {
            (0..length)
                .map(|offset| u8::try_from((length * 17 + offset) % 251).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    keys.extend([vec![1], vec![1, 0], vec![1, 0, 0], vec![1; 32], vec![2; 33]]);
    let mut full_width_alias = vec![0; 32];
    full_width_alias[0] = 1;
    full_width_alias[31] = 1;
    keys.push(full_width_alias);

    for (index, key) in keys.iter().enumerate() {
        assert!(map.insert_new(key, non_max(index as u64)));
        assert!(!map.insert_new(key, non_max(10_000 + index as u64)));
    }
    assert_eq!(map.len(), keys.len());
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(non_max(index as u64)));
    }

    map.rebuild(64).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.update(key, increment), Some(non_max(index as u64 + 1)));
    }
    map.rebuild(64).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(map.remove(key), Some(non_max(index as u64 + 1)));
    }
    assert!(map.is_empty());
}

#[test]
fn atomic_short_overflow_initialization_is_exact_under_race() {
    const WRITERS: usize = 64;
    let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
        std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
        1,
        AtomicGenerationOverlay::AtomicUpTo32,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                let key = writer.to_le_bytes();
                assert!(map.insert_new(&key, non_max(writer as u64)));
            });
        }
    });

    assert_eq!(map.len(), WRITERS);
    for writer in 0..WRITERS {
        assert_eq!(map.get(&writer.to_le_bytes()), Some(non_max(writer as u64)));
    }
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).unwrap()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    for (word, chunk) in key.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        chunk.copy_from_slice(&value.wrapping_add(word as u64).to_le_bytes());
    }
    key
}

fn sized_key(length: usize) -> Vec<u8> {
    let length_byte = u8::try_from(length).expect("test key length fits u8");
    (0..length)
        .map(|index| {
            length_byte
                .wrapping_add(u8::try_from(index).expect("test key index fits u8"))
                .wrapping_add(1)
        })
        .collect()
}

fn adaptive_test_key(index: usize, length: usize) -> Vec<u8> {
    let mut key = vec![0; length];
    let index = u64::try_from(index).expect("test key index fits u64");
    for (word, chunk) in key.chunks_mut(8).enumerate() {
        let bytes = index
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(word as u64)
            .to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    key
}
