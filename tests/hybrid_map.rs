#![allow(missing_docs)]

use std::collections::HashMap;

use packedgen::{FrozenPackedMap, HybridFilterMode, HybridPackedMap, InsertOutcome};

#[test]
fn base_delta_and_tombstone_precedence_is_exact() {
    let mut map =
        HybridPackedMap::try_from_entries([(b"base".as_slice(), 1_u64), (b"remove".as_slice(), 2)])
            .unwrap();

    assert_eq!(map.try_insert(b"base", 10), Ok(InsertOutcome::Replaced(1)));
    assert_eq!(map.try_insert(b"new", 3), Ok(InsertOutcome::Inserted));
    assert_eq!(map.get(b"base"), Some(&10));
    assert_eq!(map.len(), 3);

    assert_eq!(map.remove(b"base"), Some(10));
    assert_eq!(
        map.get(b"base"),
        None,
        "the old base value must not reappear"
    );
    assert_eq!(map.remove(b"remove"), Some(2));
    assert_eq!(map.remove(b"remove"), None);
    assert_eq!(map.remove(b"new"), Some(3));
    assert!(map.is_empty());

    let stats = map.stats();
    assert_eq!(stats.tombstones, 2);
    assert_eq!(stats.tombstone_bytes, 8);
}

#[test]
fn reinserting_a_deleted_base_key_remains_deleted_after_overlay_removal() {
    let mut map = HybridPackedMap::try_from_entries([(b"key".as_slice(), 1_u64)]).unwrap();
    assert_eq!(map.remove(b"key"), Some(1));
    assert_eq!(map.try_insert(b"key", 2), Ok(InsertOutcome::Inserted));
    assert_eq!(map.try_insert(b"key", 3), Ok(InsertOutcome::Replaced(2)));
    assert_eq!(map.remove(b"key"), Some(3));
    assert_eq!(map.get(b"key"), None);
    assert!(map.is_empty());
}

#[test]
fn binary_empty_and_unknown_keys_are_distinguished() {
    let mut map =
        HybridPackedMap::try_from_entries([(b"".as_slice(), 1_u64), (&[0, 255, 0, 128], 2)])
            .unwrap();
    assert_eq!(map.get(b""), Some(&1));
    assert_eq!(map.get(&[0, 255, 0, 128]), Some(&2));
    assert_eq!(map.get(&[0, 255, 0, 129]), None);
    assert_eq!(map.remove(b""), Some(1));
    assert_eq!(map.try_insert(b"", 4), Ok(InsertOutcome::Inserted));
    assert_eq!(map.get(b""), Some(&4));
}

#[test]
fn randomized_churn_matches_standard_hash_map() {
    let initial: Vec<_> = (0..2_048_u64)
        .map(|index| (binary_key(index), index))
        .collect();
    let mut hybrid = HybridPackedMap::try_from_entries(
        initial.iter().map(|(key, value)| (key.as_slice(), *value)),
    )
    .unwrap();
    let mut reference: HashMap<Vec<u8>, u64> = initial.into_iter().collect();
    let mut state = 0x1234_5678_9abc_def0_u64;

    for step in 0..50_000_u64 {
        state = mix(state);
        let key = binary_key(state % 4_096);
        match state % 5 {
            0 => assert_eq!(hybrid.remove(&key), reference.remove(key.as_slice())),
            1 => assert_eq!(
                hybrid.get(&key).copied(),
                reference.get(key.as_slice()).copied()
            ),
            _ => {
                let expected = reference.insert(key.clone(), step);
                let actual = match hybrid.try_insert(&key, step).unwrap() {
                    InsertOutcome::Inserted => None,
                    InsertOutcome::Replaced(previous) => Some(previous),
                };
                assert_eq!(actual, expected);
            }
        }
        assert_eq!(hybrid.len(), reference.len());
    }

    for index in 0..4_096_u64 {
        let key = binary_key(index);
        assert_eq!(hybrid.get(&key), reference.get(key.as_slice()));
    }
}

#[test]
fn membership_filter_is_optional_and_preserves_semantics() {
    for mode in [
        HybridFilterMode::Disabled,
        HybridFilterMode::HalfBytePerEntry,
        HybridFilterMode::OneBytePerEntry,
    ] {
        let base =
            FrozenPackedMap::try_from_entries((0..128_u64).map(|index| (binary_key(index), index)))
                .unwrap();
        let mut map = HybridPackedMap::with_delta_capacity_and_filter(base, 16, mode);
        for index in 0..128_u64 {
            assert_eq!(map.get(&binary_key(index)), Some(&index));
        }
        assert_eq!(map.get(&binary_key(999)), None);
        assert_eq!(
            map.try_insert(&binary_key(999), 999),
            Ok(InsertOutcome::Inserted)
        );
        assert_eq!(map.get(&binary_key(999)), Some(&999));

        match mode {
            HybridFilterMode::Disabled => assert_eq!(map.stats().membership_bytes, 0),
            HybridFilterMode::HalfBytePerEntry => {
                assert_eq!(map.stats().membership_bytes, 72);
            }
            HybridFilterMode::OneBytePerEntry => {
                assert_eq!(map.stats().membership_bytes, 144);
            }
        }
    }
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
