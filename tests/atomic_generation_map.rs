#![allow(missing_docs)]

#[cfg(feature = "prepared-keys")]
use std::mem::size_of;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[cfg(feature = "prepared-keys")]
use packedgen::AtomicPreparedKey;
#[cfg(feature = "phast")]
use packedgen::FrozenIndexBackend;
#[cfg(any(feature = "phast", feature = "shared-gx"))]
use packedgen::GenerationHashBuilder;
use packedgen::{
    AtomicGenerationBaseFilter, AtomicGenerationOverlay, InsertOutcome,
    LockFreeAtomicU64GenerationMap, NonMaxU64, NonMaxU64Error,
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
    assert_eq!(size_of::<AtomicPreparedKey>(), 24);
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
            Some(non_max(14)),
            Some(non_max(15)),
            Some(non_max(16)),
            None,
            Some(non_max(10_001))
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

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).unwrap()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn increment(value: &NonMaxU64) -> NonMaxU64 {
    non_max(value.get() + 1)
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    for (word, chunk) in key.chunks_exact_mut(8).enumerate() {
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
