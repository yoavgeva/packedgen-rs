#![allow(missing_docs)]

use std::collections::HashMap;

use packedgen::{InsertOutcome, SegmentedLoad, SegmentedSwissMap};

#[test]
fn capacity_is_decomposed_without_a_global_power_of_two_cliff() {
    let map = SegmentedSwissMap::<u64>::with_capacity(1_000_000);
    let stats = map.stats();
    assert!(stats.segments > 1);
    assert!(stats.table_capacity >= 1_000_000);
    assert!(stats.table_capacity < 1_120_000);
}

#[test]
fn load_policies_form_an_ordered_capacity_frontier() {
    let compact =
        SegmentedSwissMap::<u64>::with_capacity_and_load(1_000_000, SegmentedLoad::Compact);
    let balanced = SegmentedSwissMap::<u64>::with_capacity(1_000_000);
    let fast = SegmentedSwissMap::<u64>::with_capacity_and_load(1_000_000, SegmentedLoad::Fast);
    assert!(compact.stats().table_capacity < balanced.stats().table_capacity);
    assert!(balanced.stats().table_capacity < fast.stats().table_capacity);
}

#[test]
fn exact_crud_crosses_all_segments() {
    let mut map = SegmentedSwissMap::try_with_capacity_and_key_bytes(100_000, 3_200_000).unwrap();
    for index in 0..100_000_u64 {
        assert_eq!(
            map.try_insert(&binary_key(index), index),
            Ok(InsertOutcome::Inserted)
        );
    }
    assert_eq!(map.len(), 100_000);
    for index in 0..100_000_u64 {
        assert_eq!(map.get(&binary_key(index)), Some(&index));
    }
    for index in (0..100_000_u64).step_by(3) {
        assert_eq!(map.remove(&binary_key(index)), Some(index));
    }
    for index in 0..100_000_u64 {
        assert_eq!(map.contains_key(&binary_key(index)), index % 3 != 0);
    }
}

#[test]
fn mixed_operations_match_standard_hash_map() {
    let mut packed = SegmentedSwissMap::with_capacity(16_384);
    let mut reference = HashMap::<Vec<u8>, u64>::new();
    let mut state = 0x0123_4567_89ab_cdef_u64;
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
                let expected = reference.insert(key.to_vec(), step);
                let actual = packed.try_insert(&key, step).unwrap();
                match (actual, expected) {
                    (InsertOutcome::Inserted, None) => {}
                    (InsertOutcome::Replaced(actual), Some(expected)) => {
                        assert_eq!(actual, expected);
                    }
                    pair => panic!("insert mismatch: {pair:?}"),
                }
            }
        }
    }
    assert_eq!(packed.len(), reference.len());
}

#[test]
fn zero_capacity_grows_and_clear_reuses_table() {
    let mut map = SegmentedSwissMap::new();
    map.try_insert(b"", 1).unwrap();
    map.try_insert(b"second", 2).unwrap();
    assert_eq!(map.get(b""), Some(&1));
    map.clear();
    assert!(map.is_empty());
    map.try_insert(b"again", 3).unwrap();
    assert_eq!(map.get(b"again"), Some(&3));
}

#[test]
fn compaction_discards_deleted_key_bytes_without_rebuilding_tables() {
    let mut map = SegmentedSwissMap::try_with_capacity_key_bytes_and_load(
        1_024,
        256,
        SegmentedLoad::Balanced,
    )
    .unwrap();
    for index in 0..1_024_u64 {
        map.try_insert(&binary_key(index), index).unwrap();
    }
    for index in (0..1_024_u64).step_by(2) {
        map.remove(&binary_key(index)).unwrap();
    }
    let table_capacity = map.stats().table_capacity;
    assert_eq!(map.stats().dead_key_bytes(), 512 * 32);

    let compacted = map.compact_keys().unwrap();
    assert_eq!(compacted.live_entries, 512);
    assert_eq!(compacted.copied_key_bytes, 512 * 32);
    assert_eq!(compacted.discarded_key_bytes, 512 * 32);
    assert_eq!(map.stats().dead_key_bytes(), 0);
    assert_eq!(map.stats().table_capacity, table_capacity);
    for index in 0..1_024_u64 {
        assert_eq!(
            map.get(&binary_key(index)).copied(),
            (index % 2 == 1).then_some(index)
        );
    }
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
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
