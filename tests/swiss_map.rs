#![allow(missing_docs)]

use std::collections::HashMap;

use packedgen::{ArenaError, InsertOutcome, PackedSwissMap};

#[test]
fn binary_crud_and_replacement_are_exact() {
    let mut map = PackedSwissMap::with_capacity(4);
    assert_eq!(map.try_insert(b"alpha", 1), Ok(InsertOutcome::Inserted));
    assert_eq!(
        map.try_insert(&[0, 1, 0, 2], 2),
        Ok(InsertOutcome::Inserted)
    );
    assert_eq!(map.try_insert(b"alpha", 3), Ok(InsertOutcome::Replaced(1)));
    assert_eq!(map.get(b"alpha"), Some(&3));
    assert_eq!(map.get(&[0, 1, 0, 2]), Some(&2));
    assert_eq!(map.get(&[0, 1, 0, 3]), None);
    *map.get_mut(b"alpha").unwrap() = 4;
    assert_eq!(map.remove(b"alpha"), Some(4));
    assert_eq!(map.remove(b"alpha"), None);
    assert_eq!(map.len(), 1);
}

#[test]
fn grows_and_matches_standard_hash_map_under_churn() {
    let mut packed = PackedSwissMap::with_capacity(1);
    let mut reference = HashMap::<Vec<u8>, u64>::new();
    let mut state = 0x1234_5678_9abc_def0_u64;
    for step in 0..50_000_u64 {
        state = mix(state);
        let key = binary_key(state % 8_192);
        match state % 5 {
            0 => assert_eq!(packed.remove(&key), reference.remove(key.as_slice())),
            1 => assert_eq!(
                packed.get(&key).copied(),
                reference.get(key.as_slice()).copied()
            ),
            _ => {
                let expected = reference.insert(key.clone(), step);
                let outcome = packed.try_insert(&key, step).unwrap();
                let previous = match outcome {
                    InsertOutcome::Inserted => None,
                    InsertOutcome::Replaced(previous) => Some(previous),
                };
                assert_eq!(previous, expected);
            }
        }
    }
    assert_eq!(packed.len(), reference.len());
    for (key, value) in reference {
        assert_eq!(packed.get(&key), Some(&value));
    }
}

#[test]
fn empty_and_long_key_boundaries_work() {
    let mut map = PackedSwissMap::new();
    assert_eq!(map.try_insert(b"", 1), Ok(InsertOutcome::Inserted));
    let maximum = vec![7_u8; 1 << 16];
    assert_eq!(map.try_insert(&maximum, 2), Ok(InsertOutcome::Inserted));
    assert_eq!(map.get(b""), Some(&1));
    assert_eq!(map.get(&maximum), Some(&2));
    let too_long = vec![8_u8; (1 << 16) + 1];
    assert!(matches!(
        map.try_insert(&too_long, 3),
        Err(ArenaError::KeyTooLong { .. })
    ));
}

#[test]
fn configured_key_arena_and_clear_are_reusable() {
    let mut map = PackedSwissMap::try_with_capacity_and_key_bytes(128, 4_096).unwrap();
    for index in 0..128_u64 {
        map.try_insert(&binary_key(index), index).unwrap();
    }
    let stats = map.stats();
    assert_eq!(stats.len, 128);
    assert!(stats.arena_allocated_bytes >= 4_096);
    map.clear();
    assert!(map.is_empty());
    assert_eq!(map.stats().arena_key_bytes, 0);
    map.try_insert(b"reused", 9).unwrap();
    assert_eq!(map.get(b"reused"), Some(&9));
}

fn binary_key(value: u64) -> Vec<u8> {
    let mut key = vec![0_u8; 32];
    key[..8].copy_from_slice(&value.to_le_bytes());
    key[8..16].copy_from_slice(&mix(value).to_le_bytes());
    key[16..24].copy_from_slice(&mix(value ^ 0xa5a5).to_le_bytes());
    key[24..].copy_from_slice(&mix(value ^ 0x5a5a).to_le_bytes());
    key
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
