#![allow(missing_docs)]

use packedgen::{FrozenBuildError, FrozenPackedMap};

#[test]
fn singleton_map_rejects_arbitrary_misses_without_index_remapping() {
    let map = FrozenPackedMap::try_from_entries([(b"member".as_slice(), 7_u64)]).unwrap();

    assert_eq!(map.get(b"member"), Some(&7));
    assert_eq!(map.get(b"not-a-member"), None);
    assert_eq!(
        map.get_many([b"not-a-member".as_slice(), b"member".as_slice()]),
        [None, Some(&7)]
    );
}

#[test]
fn exact_hits_and_unknown_keys_are_distinguished() {
    let entries: Vec<(Vec<u8>, u64)> = (0..1_000_u64)
        .map(|index| (binary_key(index), index))
        .collect();
    let map = FrozenPackedMap::try_from_entries(
        entries.iter().map(|(key, value)| (key.as_slice(), *value)),
    )
    .unwrap();

    assert_eq!(map.len(), entries.len());
    for (key, value) in &entries {
        assert_eq!(map.get(key), Some(value));
    }
    for index in 1_000..2_000_u64 {
        assert_eq!(map.get(&binary_key(index)), None);
    }
    let stats = map.stats();
    assert_eq!(stats.len, 1_000);
    assert_eq!(stats.arena_key_bytes, 32_000);
    assert!(stats.index_bits_per_entry > 0.0);
}

#[test]
fn empty_and_binary_keys_round_trip() {
    let map =
        FrozenPackedMap::try_from_entries([(b"".as_slice(), 1_u64), (&[0, 255, 128, 1], 2_u64)])
            .unwrap();

    assert_eq!(map.get(b""), Some(&1));
    assert_eq!(map.get(&[0, 255, 128, 1]), Some(&2));
    assert_eq!(map.get(b"absent"), None);
}

#[test]
fn duplicate_keys_are_rejected() {
    let error = FrozenPackedMap::try_from_entries([(b"same".as_slice(), 1), (b"same", 2)])
        .err()
        .unwrap();
    assert_eq!(error, FrozenBuildError::IndexConstructionFailed);
}

#[test]
fn empty_map_has_exact_empty_semantics() {
    let map = FrozenPackedMap::<u64>::try_from_entries(std::iter::empty::<(&[u8], u64)>()).unwrap();
    assert!(map.is_empty());
    assert_eq!(map.get(b"anything"), None);
    assert!(map.stats().index_bits_per_entry.abs() < f64::EPSILON);
}

#[test]
fn batched_lookup_preserves_order_and_exact_miss_semantics() {
    let entries: Vec<(Vec<u8>, u64)> = (0..32_u64)
        .map(|index| (binary_key(index), index))
        .collect();
    let map = FrozenPackedMap::try_from_entries(
        entries.iter().map(|(key, value)| (key.as_slice(), *value)),
    )
    .unwrap();
    let absent = binary_key(10_000);
    let results = map.get_many([
        entries[19].0.as_slice(),
        absent.as_slice(),
        entries[0].0.as_slice(),
        entries[31].0.as_slice(),
    ]);

    assert_eq!(
        results.map(Option::<&u64>::copied),
        [Some(19), None, Some(0), Some(31)]
    );
}

#[test]
fn batched_lookup_on_empty_map_returns_all_misses() {
    let map = FrozenPackedMap::<u64>::try_from_entries(std::iter::empty::<(&[u8], u64)>()).unwrap();
    let results = map.get_many([b"one".as_slice(), b"two".as_slice()]);
    assert_eq!(results, [None, None]);
}

fn binary_key(index: u64) -> Vec<u8> {
    let mut key = vec![0_u8; 32];
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
