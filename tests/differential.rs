#![allow(missing_docs)]

use std::collections::HashMap;

use elastichash::{ElasticConfig, FixedElasticMap, InsertOutcome};

#[test]
fn matches_std_map_for_mixed_operations() {
    let config = ElasticConfig::new(2_048).with_reserve_exponent(6).unwrap();
    let mut elastic = FixedElasticMap::new(config);
    let mut standard = HashMap::new();

    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    for step in 0..20_000_u64 {
        state = splitmix64(state);
        let key = state % 1_500;

        match state % 4 {
            0 | 1 => {
                let value = splitmix64(state ^ step);
                let expected = standard.insert(key, value);
                let actual = elastic.try_insert(key, value).unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(InsertOutcome::Inserted, InsertOutcome::Replaced)
                );
            }
            2 => assert_eq!(elastic.get(&key), standard.get(&key)),
            _ => assert_eq!(elastic.remove(&key), standard.remove(&key)),
        }

        assert_eq!(elastic.len(), standard.len());
    }
}

#[test]
fn fixed_epoch_rejects_only_absent_keys_at_limit() {
    let mut map = FixedElasticMap::new(ElasticConfig::new(4).with_reserve_exponent(6).unwrap());

    for key in 0..4 {
        assert_eq!(map.try_insert(key, key), Ok(InsertOutcome::Inserted));
    }

    assert_eq!(map.try_insert(9, 9).unwrap_err().live_limit(), 4);
    assert_eq!(map.try_insert(2, 200), Ok(InsertOutcome::Replaced(2)));
    assert_eq!(map.get(&2), Some(&200));
}

#[test]
fn borrowed_binary_lookup_does_not_allocate_a_key() {
    let mut map = FixedElasticMap::new(ElasticConfig::new(8));
    map.try_insert(b"flow:123".to_vec(), 7).unwrap();

    assert_eq!(map.get(b"flow:123".as_slice()), Some(&7));
    assert!(map.contains_key(b"flow:123".as_slice()));
}

#[test]
fn stats_expose_reserve_and_delete_lifecycle() {
    let config = ElasticConfig::new(128).with_reserve_exponent(6).unwrap();
    let mut map = FixedElasticMap::new(config);
    map.try_insert(1_u64, 10_u64).unwrap();
    map.remove(&1);

    let stats = map.stats();
    assert_eq!(stats.reserve.exponent(), 6);
    assert!(stats.epoch.had_delete);
    assert!(stats.occupancy().abs() < f64::EPSILON);
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
