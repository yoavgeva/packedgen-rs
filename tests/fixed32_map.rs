#![allow(missing_docs)]

use packedgen::{Fixed32Load, Fixed32SoaMap, InsertOutcome};
use std::collections::HashMap;

#[test]
fn fixed_keys_insert_replace_lookup_and_clear() {
    let mut map = Fixed32SoaMap::with_capacity_and_load(4_096, Fixed32Load::Compact);
    for index in 0..4_096_u64 {
        assert_eq!(
            map.try_insert(key(index), index).unwrap(),
            InsertOutcome::Inserted
        );
    }

    for index in 0..4_096_u64 {
        assert_eq!(map.get(&key(index)), Some(&index));
    }
    assert_eq!(map.get(&key(9_999)), None);
    assert_eq!(
        map.try_insert(key(17), 99).unwrap(),
        InsertOutcome::Replaced(17)
    );
    *map.get_mut(&key(18)).unwrap() = 100;
    assert_eq!(map.get(&key(17)), Some(&99));
    assert_eq!(map.get(&key(18)), Some(&100));

    let stats = map.stats();
    assert_eq!(stats.len, 4_096);
    assert_eq!(stats.key_capacity, 4_096);
    assert_eq!(stats.value_capacity, 4_096);
    assert!(stats.estimated_heap_bytes() > 4_096 * 40);

    map.clear();
    assert!(map.is_empty());
    assert_eq!(map.get(&key(17)), None);
}

#[test]
fn all_load_policies_preserve_exact_keys() {
    for load in [
        Fixed32Load::Compact,
        Fixed32Load::Balanced,
        Fixed32Load::Fast,
    ] {
        let mut map = Fixed32SoaMap::with_capacity_and_load(10_000, load);
        for index in 0..10_000_u64 {
            map.try_insert(key(index), index).unwrap();
        }
        for index in 0..10_000_u64 {
            assert!(map.contains_key(&key(index)));
        }
        assert!(!map.contains_key(&key(u64::MAX)));
    }
}

#[test]
fn remove_repairs_moved_dense_routes() {
    let mut map = Fixed32SoaMap::with_capacity(1_024);
    for index in 0..1_024_u64 {
        map.try_insert(key(index), index).unwrap();
    }

    for index in (0..1_024_u64).step_by(3) {
        assert_eq!(map.remove(&key(index)), Some(index));
        assert_eq!(map.remove(&key(index)), None);
    }

    for index in 0..1_024_u64 {
        let expected = (index % 3 != 0).then_some(&index);
        assert_eq!(map.get(&key(index)), expected);
    }
    assert_eq!(map.len(), 1_024 - 342);
}

#[test]
fn mixed_operations_match_standard_hash_map() {
    let mut map = Fixed32SoaMap::with_capacity(2_000);
    let mut reference = HashMap::new();
    let mut state = 0x1234_5678_9abc_def0_u64;

    for step in 0..20_000_u64 {
        state = mix(state);
        let index = state % 1_500;
        let candidate = key(index);
        match state % 4 {
            0 | 1 => {
                let value = mix(state ^ step);
                let expected = reference.insert(candidate, value);
                let actual = map.try_insert(candidate, value).unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                );
            }
            2 => assert_eq!(map.get(&candidate), reference.get(&candidate)),
            _ => assert_eq!(map.remove(&candidate), reference.remove(&candidate)),
        }
        assert_eq!(map.len(), reference.len());
    }
}

fn key(index: u64) -> [u8; 32] {
    let mut result = [0_u8; 32];
    let mut state = index;
    for chunk in result.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    result
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
