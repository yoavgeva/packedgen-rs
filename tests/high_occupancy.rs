#![allow(missing_docs)]

use std::collections::HashMap;

use elastichash::{ElasticConfig, FixedElasticMap};

const ENTRIES: usize = 200_000;

#[test]
fn one_over_64_reserve_survives_full_epoch_and_churn() {
    let config = ElasticConfig::new(ENTRIES)
        .with_reserve_exponent(6)
        .unwrap();
    let mut elastic = FixedElasticMap::new(config);
    let mut reference = HashMap::with_capacity(ENTRIES);

    for key in 0..ENTRIES as u64 {
        let value = mix(key);
        elastic.try_insert(mix(key), value).unwrap();
        reference.insert(mix(key), value);
    }

    assert_eq!(elastic.len(), ENTRIES);
    assert!((elastic.stats().occupancy() - 1.0).abs() < f64::EPSILON);
    assert_eq!(
        elastic.try_insert(u64::MAX, 1).unwrap_err().live_limit(),
        ENTRIES
    );

    for key in (0..ENTRIES as u64).step_by(3) {
        assert_eq!(elastic.remove(&mix(key)), reference.remove(&mix(key)));
    }
    for key in ENTRIES as u64..=ENTRIES as u64 + ENTRIES as u64 / 3 {
        let value = mix(key ^ 0xa5a5_a5a5_a5a5_a5a5);
        elastic.try_insert(mix(key), value).unwrap();
        reference.insert(mix(key), value);
    }

    assert_eq!(elastic.len(), reference.len());
    for (key, value) in &reference {
        assert_eq!(elastic.get(key), Some(value));
    }
}

#[test]
fn repeated_replacements_do_not_cross_epoch_limit() {
    let mut map =
        FixedElasticMap::new(ElasticConfig::new(10_000).with_reserve_exponent(6).unwrap());
    for key in 0..10_000_u64 {
        map.try_insert(key, 0).unwrap();
    }
    let generation = map.stats().epoch.generation;

    for round in 1..=10_u64 {
        for key in 0..10_000_u64 {
            map.try_insert(key, round).unwrap();
        }
    }

    assert_eq!(map.len(), 10_000);
    assert_eq!(map.stats().epoch.generation, generation);
    assert_eq!(map.get(&9_999), Some(&10));
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
