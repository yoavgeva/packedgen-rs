#![allow(missing_docs)]

use std::collections::HashMap;

use elastichash::{
    ElasticConfig, InsertOutcome, PackedBinaryMap, PackedMapError, RouteCacheBudget,
};

#[test]
fn binary_crud_and_replacement_reuse_packed_key() {
    let config = ElasticConfig::new(16).with_reserve_exponent(6).unwrap();
    let mut map = PackedBinaryMap::with_key_segment_bytes(config, 128).unwrap();

    assert_eq!(map.try_insert(b"key", 1), Ok(InsertOutcome::Inserted));
    let bytes_after_insert = map.stats().arena_key_bytes;
    assert_eq!(map.try_insert(b"key", 2), Ok(InsertOutcome::Replaced(1)));
    assert_eq!(map.stats().arena_key_bytes, bytes_after_insert);
    assert_eq!(map.get(b"key"), Some(&2));
    assert_eq!(map.remove(b"key"), Some(2));
    assert_eq!(map.get(b"key"), None);
    assert_eq!(map.stats().dead_key_bytes(), 3);
}

#[test]
fn empty_and_non_utf8_keys_round_trip() {
    let mut map = PackedBinaryMap::new(ElasticConfig::new(8));
    assert_eq!(map.try_insert(b"", 1), Ok(InsertOutcome::Inserted));
    assert_eq!(
        map.try_insert(&[0, 255, 128, 1], 2),
        Ok(InsertOutcome::Inserted)
    );
    assert_eq!(map.get(b""), Some(&1));
    assert_eq!(map.get(&[0, 255, 128, 1]), Some(&2));
}

#[test]
fn fixed_capacity_rejects_new_key_but_allows_replacement() {
    let mut map = PackedBinaryMap::new(ElasticConfig::new(2));
    map.try_insert(b"a", 1).unwrap();
    map.try_insert(b"b", 2).unwrap();

    let error = map.try_insert(b"c", 3).unwrap_err();
    assert!(matches!(
        error,
        PackedMapError::Capacity(capacity) if capacity.live_limit() == 2
    ));
    assert_eq!(map.try_insert(b"a", 4), Ok(InsertOutcome::Replaced(1)));
}

#[test]
fn mixed_operations_match_standard_hash_map() {
    let config = ElasticConfig::new(2_000).with_reserve_exponent(6).unwrap();
    let mut packed = PackedBinaryMap::new(config);
    let mut reference = HashMap::<Vec<u8>, u64>::new();

    let mut state = 0x1234_5678_9abc_def0_u64;
    for step in 0..20_000_u64 {
        state = mix(state);
        let key = (state % 1_500).to_le_bytes();
        match state % 4 {
            0 | 1 => {
                let value = mix(state ^ step);
                let expected = reference.insert(key.to_vec(), value);
                let actual = packed.try_insert(&key, value).unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                );
            }
            2 => assert_eq!(packed.get(&key), reference.get(key.as_slice())),
            _ => assert_eq!(packed.remove(&key), reference.remove(key.as_slice())),
        }
        assert_eq!(packed.len(), reference.len());
    }
}

#[test]
fn byte_aware_rebuild_preserves_survivors_after_heavy_deletes() {
    let capacity = 1_024;
    let mut map = PackedBinaryMap::new(
        ElasticConfig::new(capacity)
            .with_reserve_exponent(6)
            .unwrap(),
    );
    let keys: Vec<Vec<u8>> = (0..capacity)
        .map(|index| format!("packed-key-{index:08}").into_bytes())
        .collect();

    for (index, key) in keys.iter().enumerate() {
        map.try_insert(key, index).unwrap();
    }
    for key in &keys[..capacity / 2] {
        assert!(map.remove(key).is_some());
    }

    assert_eq!(map.stats().deletes_since_rebuild, 0);
    for (index, key) in keys.iter().enumerate().skip(capacity / 2) {
        assert_eq!(map.get(key), Some(&index));
    }
}

#[test]
fn routing_accelerator_is_bounded_and_caches_most_routes() {
    let capacity = 10_000;
    let mut map = PackedBinaryMap::new(
        ElasticConfig::new(capacity).with_route_cache_budget(RouteCacheBudget::ReadOptimized),
    );
    for index in 0..capacity {
        map.try_insert(format!("route-{index}").as_bytes(), index)
            .unwrap();
    }

    let stats = map.stats();
    assert_eq!(
        stats.route_cache_bytes,
        capacity * 10 + capacity.div_ceil(64) * 8
    );
    assert!(stats.route_cache_entries > capacity * 4 / 5, "{stats:?}");
    assert_eq!(
        stats.route_cache_entries + stats.route_cache_overflows,
        capacity
    );
}

#[test]
fn batched_lookup_preserves_order_across_hits_and_misses() {
    for budget in [RouteCacheBudget::Compact, RouteCacheBudget::ReadOptimized] {
        let mut map = PackedBinaryMap::new(ElasticConfig::new(16).with_route_cache_budget(budget));
        map.try_insert(b"alpha", 1).unwrap();
        map.try_insert(b"beta", 2).unwrap();
        map.try_insert(b"gamma", 3).unwrap();

        assert_eq!(map.get(b"alpha"), Some(&1));
        assert_eq!(
            map.get_many([b"gamma".as_slice(), b"missing", b"alpha", b"beta"]),
            [Some(&3), None, Some(&1), Some(&2)]
        );
        assert_eq!(map.remove(b"gamma"), Some(3));
        assert_eq!(
            map.get_many([b"gamma".as_slice(), b"alpha", b"missing"]),
            [None, Some(&1), None]
        );
        assert_eq!(map.get_many::<0>([]), []);
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
