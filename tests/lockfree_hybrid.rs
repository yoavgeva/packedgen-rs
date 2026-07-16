#![allow(missing_docs)]

use std::collections::HashMap;
use std::sync::Barrier;
use std::thread;

use packedgen::{InsertOutcome, LockFreeHybridMap};

#[test]
fn base_overlay_and_deletion_precedence_are_exact() {
    let map = build_base(64, 32);
    assert_eq!(map.get_cloned(&binary_key(7)), Some(7));
    assert_eq!(map.insert(&binary_key(7), 70), InsertOutcome::Replaced(7));
    assert_eq!(map.get_cloned(&binary_key(7)), Some(70));
    assert_eq!(map.remove(&binary_key(7)), Some(70));
    assert_eq!(map.get_cloned(&binary_key(7)), None);
    assert_eq!(map.remove(&binary_key(7)), None);

    assert!(map.insert_new(&binary_key(7), 700));
    assert!(!map.insert_new(&binary_key(7), 701));
    assert_eq!(map.remove(&binary_key(7)), Some(700));
    assert_eq!(map.get_cloned(&binary_key(7)), None);
    assert_eq!(map.len(), 63);

    let stats = map.stats();
    assert_eq!(stats.base.len, 64);
    assert_eq!(stats.overlay_records, 1);
    assert_eq!(stats.overlay_deleted, 1);
}

#[test]
fn mixed_operations_match_standard_hash_map() {
    const BASE: usize = 1_024;
    const DOMAIN: usize = 2_048;

    let map = build_base(BASE, DOMAIN);
    let mut reference: HashMap<Vec<u8>, u64> = (0..BASE)
        .map(|index| (binary_key(index as u64).to_vec(), index as u64))
        .collect();
    let mut state = 0x0123_4567_89ab_cdef_u64;
    for step in 0..50_000_u64 {
        state = mix(state);
        let key = binary_key(state % DOMAIN as u64);
        match state % 7 {
            0 => assert_eq!(map.get_cloned(&key), reference.get(key.as_slice()).copied()),
            1 => assert_eq!(map.remove(&key), reference.remove(key.as_slice())),
            2 => {
                let expected = reference.insert(key.to_vec(), step);
                let actual = map.insert(&key, step);
                assert_insert(actual, expected);
            }
            3 => {
                let expected = match reference.entry(key.to_vec()) {
                    std::collections::hash_map::Entry::Occupied(_) => false,
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(step);
                        true
                    }
                };
                assert_eq!(map.insert_new(&key, step), expected);
            }
            4 => {
                let expected = reference.get_mut(key.as_slice()).map(|value| {
                    *value = value.wrapping_add(1);
                    *value
                });
                assert_eq!(map.update(&key, |value| value.wrapping_add(1)), expected);
            }
            5 => {
                let expected = reference
                    .entry(key.to_vec())
                    .and_modify(|value| *value = value.wrapping_mul(3))
                    .or_insert(step);
                assert_eq!(
                    map.upsert(&key, step, |value| value.wrapping_mul(3)),
                    *expected
                );
            }
            _ => {
                let expected = reference
                    .get(key.as_slice())
                    .copied()
                    .filter(|value| value % 2 == 0);
                if expected.is_some() {
                    reference.remove(key.as_slice());
                }
                assert_eq!(map.remove_if(&key, |value| value % 2 == 0), expected);
            }
        }
    }

    assert_eq!(map.len(), reference.len());
    for index in 0..DOMAIN as u64 {
        let key = binary_key(index);
        assert_eq!(map.get_cloned(&key), reference.get(key.as_slice()).copied());
    }
}

#[test]
fn concurrent_cas_updates_promote_base_without_lost_writes() {
    const WRITERS: usize = 8;
    const UPDATES: usize = 10_000;

    let map = LockFreeHybridMap::try_from_entries([(b"counter".as_slice(), 0_u64)], 8).unwrap();
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for _ in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..UPDATES {
                    map.update(b"counter", |value| value + 1).unwrap();
                }
            });
        }
    });
    assert_eq!(map.get_cloned(b"counter"), Some((WRITERS * UPDATES) as u64));
    assert_eq!(map.len(), 1);
}

#[test]
fn contended_insert_remove_accounting_converges_to_logical_state() {
    const WRITERS: usize = 8;
    const OPERATIONS: usize = 20_000;

    let map =
        LockFreeHybridMap::try_from_entries(std::iter::empty::<([u8; 32], u64)>(), 32).unwrap();
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for operation in 0..OPERATIONS {
                    if (writer + operation) % 2 == 0 {
                        map.insert(b"same-key", operation as u64);
                    } else {
                        let _ = map.remove(b"same-key");
                    }
                }
            });
        }
    });
    assert_eq!(map.len(), usize::from(map.contains_key(b"same-key")));
}

#[test]
fn readers_and_writers_overlap_on_packed_base() {
    const KEYS: usize = 2_048;
    const READERS: usize = 4;
    const WRITERS: usize = 4;
    const OPERATIONS: usize = 20_000;

    let map = build_base(KEYS, KEYS);
    let start = Barrier::new(READERS + WRITERS);
    thread::scope(|scope| {
        for reader in 0..READERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for operation in 0..OPERATIONS {
                    let index = (reader * 97 + operation * 17) % KEYS;
                    assert!(map.get_cloned(&binary_key(index as u64)).is_some());
                }
            });
        }
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for operation in 0..OPERATIONS {
                    let index = (writer * 193 + operation * 29) % KEYS;
                    map.update(&binary_key(index as u64), |value| value + 1)
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(map.len(), KEYS);
}

fn build_base(entries: usize, overlay_capacity: usize) -> LockFreeHybridMap<u64> {
    LockFreeHybridMap::try_from_entries(
        (0..entries).map(|index| (binary_key(index as u64), index as u64)),
        overlay_capacity,
    )
    .unwrap()
}

fn assert_insert(actual: InsertOutcome<u64>, expected: Option<u64>) {
    match (actual, expected) {
        (InsertOutcome::Inserted, None) => {}
        (InsertOutcome::Replaced(actual), Some(expected)) => assert_eq!(actual, expected),
        pair => panic!("insert mismatch: {pair:?}"),
    }
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
