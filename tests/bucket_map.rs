#![allow(missing_docs)]

use std::collections::HashMap;

use packedgen::{BucketMapError, BucketPackedMap, InsertOutcome};

#[test]
fn binary_crud_replacement_and_dense_repair() {
    let mut map = BucketPackedMap::new(64);
    for index in 0..64_u64 {
        assert_eq!(
            map.try_insert(&binary_key(index), index),
            Ok(InsertOutcome::Inserted)
        );
    }
    assert_eq!(
        map.try_insert(&binary_key(7), 70),
        Ok(InsertOutcome::Replaced(7))
    );
    for index in (0..64_u64).step_by(2) {
        assert_eq!(
            map.remove(&binary_key(index)),
            Some(if index == 7 { 70 } else { index })
        );
    }
    for index in 0..64_u64 {
        let expected = (index % 2 == 1).then_some(if index == 7 { &70 } else { &index });
        assert_eq!(map.get(&binary_key(index)), expected);
    }
    assert_eq!(map.len(), 32);
    assert!(map.stats().dead_key_bytes() > 0);
}

#[test]
fn fixed_capacity_rejects_new_keys_but_replaces() {
    let mut map = BucketPackedMap::new(2);
    map.try_insert(b"a", 1).unwrap();
    map.try_insert(b"b", 2).unwrap();
    assert!(matches!(
        map.try_insert(b"c", 3),
        Err(BucketMapError::Capacity(error)) if error.live_limit() == 2
    ));
    assert_eq!(map.try_insert(b"a", 4), Ok(InsertOutcome::Replaced(1)));
}

#[test]
fn mixed_operations_match_standard_hash_map() {
    let mut map = BucketPackedMap::new(2_000);
    let mut reference = HashMap::<Vec<u8>, u64>::new();
    let mut state = 0x1234_5678_9abc_def0_u64;
    for step in 0..50_000_u64 {
        state = mix(state);
        let key = binary_key(state % 1_500);
        match state % 4 {
            0 | 1 => {
                let value = mix(state ^ step);
                let expected = reference.insert(key.to_vec(), value);
                let actual = map.try_insert(&key, value).unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                );
            }
            2 => assert_eq!(map.get(&key), reference.get(key.as_slice())),
            _ => assert_eq!(map.remove(&key), reference.remove(key.as_slice())),
        }
        assert_eq!(map.len(), reference.len());
    }
}

#[test]
fn high_occupancy_round_trips_and_bounds_overflow() {
    let entries = 100_000;
    let mut map = BucketPackedMap::new(entries);
    for index in 0..entries as u64 {
        map.try_insert(&binary_key(index), index).unwrap();
    }
    for index in 0..entries as u64 {
        assert_eq!(map.get(&binary_key(index)), Some(&index));
    }
    let stats = map.stats();
    assert_eq!(stats.len, entries);
    assert!(
        stats.overflow_routes < entries / 1_000,
        "unexpected overflow routes: {}",
        stats.overflow_routes
    );
}

#[test]
fn clear_starts_an_empty_reusable_map() {
    let mut map = BucketPackedMap::new(8);
    map.try_insert(b"key", 1).unwrap();
    map.clear();
    assert!(map.is_empty());
    assert_eq!(map.get(b"key"), None);
    assert_eq!(map.stats().arena_key_bytes, 0);
    map.try_insert(b"next", 2).unwrap();
    assert_eq!(map.get(b"next"), Some(&2));
}

fn binary_key(index: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
