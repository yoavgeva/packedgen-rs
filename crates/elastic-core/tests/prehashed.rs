use opthash::{ElasticHashMap, Equivalent, FunnelHashMap};

#[derive(Debug, Eq, Hash, PartialEq)]
struct StoredKey(u64);

struct Query(u64);

impl Equivalent<StoredKey> for Query {
    fn equivalent(&self, key: &StoredKey) -> bool {
        self.0 == key.0
    }
}

#[test]
fn prehashed_api_accepts_non_hashing_equivalence_query() {
    check_elastic();
    check_funnel();
}

fn check_elastic() {
    let mut map = ElasticHashMap::<StoredKey, u64>::with_capacity(32);
    let hash = map.hash_key(&42_u64);
    map.insert_unique_prehashed(hash, StoredKey(42), 7);

    assert_eq!(map.get_prehashed(hash, &Query(42)), Some(&7));
    assert!(map.contains_prehashed(hash, &Query(42)));
    *map.get_mut_prehashed(hash, &Query(42)).unwrap() = 8;
    assert_eq!(
        map.remove_prehashed(hash, &Query(42)),
        Some((StoredKey(42), 8))
    );
}

fn check_funnel() {
    let mut map = FunnelHashMap::<StoredKey, u64>::with_capacity(32);
    let hash = map.hash_key(&42_u64);
    map.insert_unique_prehashed(hash, StoredKey(42), 7);

    assert_eq!(map.get_prehashed(hash, &Query(42)), Some(&7));
    assert!(map.contains_prehashed(hash, &Query(42)));
    *map.get_mut_prehashed(hash, &Query(42)).unwrap() = 8;
    assert_eq!(
        map.remove_prehashed(hash, &Query(42)),
        Some((StoredKey(42), 8))
    );
}
