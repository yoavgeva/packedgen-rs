#![allow(missing_docs)]

use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use packedgen::{
    ConcurrentConfigError, ConcurrentSwissMap, InsertOutcome, SegmentedLoad, UpsertOutcome,
};

#[test]
fn shard_configuration_is_explicit_and_validated() {
    assert!(matches!(
        ConcurrentSwissMap::<u64>::try_with_capacity_and_shards(100, 0, SegmentedLoad::Balanced),
        Err(ConcurrentConfigError::ZeroShards)
    ));
    assert!(matches!(
        ConcurrentSwissMap::<u64>::try_with_capacity_and_shards(100, 3, SegmentedLoad::Balanced),
        Err(ConcurrentConfigError::ShardCountNotPowerOfTwo(3))
    ));

    let map =
        ConcurrentSwissMap::<u64>::try_with_capacity_and_shards(100, 8, SegmentedLoad::Balanced)
            .unwrap();
    assert_eq!(map.shard_count(), 8);
    assert!(map.stats().table_capacity >= 100);
}

#[test]
fn ets_style_atomic_operations_have_exact_semantics() {
    let map =
        ConcurrentSwissMap::try_with_capacity_and_shards(32, 4, SegmentedLoad::Balanced).unwrap();

    assert_eq!(map.try_insert(b"key", 10), Ok(InsertOutcome::Inserted));
    assert_eq!(map.try_insert(b"key", 11), Ok(InsertOutcome::Replaced(10)));
    assert_eq!(map.get_cloned(b"key"), Some(11));
    assert!(!map.try_insert_new(b"key", 99).unwrap());
    assert!(map.try_insert_new(b"new", 20).unwrap());
    assert_eq!(map.len(), 2);

    assert_eq!(map.update(b"key", |value| *value += 4), Some(()));
    assert_eq!(map.update(b"missing", |value| *value += 1), None);
    assert_eq!(map.get_cloned(b"key"), Some(15));

    assert_eq!(
        map.try_upsert_with(b"key", 1, |value| {
            *value *= 2;
            *value
        }),
        Ok(UpsertOutcome::Updated(30))
    );
    assert_eq!(
        map.try_upsert_with(b"third", 3, |value| *value += 1),
        Ok(UpsertOutcome::Inserted)
    );

    assert_eq!(map.remove_if(b"key", |value| *value < 30), None);
    assert_eq!(map.remove_if(b"key", |value| *value == 30), Some(30));
    assert_eq!(map.remove(b"new"), Some(20));
    assert_eq!(map.remove(b"new"), None);
    assert_eq!(map.get_cloned(b"third"), Some(3));

    let stats = map.stats();
    assert_eq!(stats.len, 1);
    assert!(stats.dead_key_bytes() >= 6);
    map.clear();
    assert!(map.is_empty());
    assert_eq!(map.stats().arena_key_bytes, 0);
}

#[test]
fn many_writers_insert_without_a_global_writer() {
    const WRITERS: usize = 8;
    const INSERTS_PER_WRITER: usize = 2_000;

    let map = ConcurrentSwissMap::try_with_capacity_and_shards(
        WRITERS * INSERTS_PER_WRITER,
        32,
        SegmentedLoad::Balanced,
    )
    .unwrap();
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for offset in 0..INSERTS_PER_WRITER {
                    let index = writer * INSERTS_PER_WRITER + offset;
                    assert_eq!(
                        map.try_insert(&binary_key(index as u64), index as u64),
                        Ok(InsertOutcome::Inserted)
                    );
                }
            });
        }
    });

    assert_eq!(map.len(), WRITERS * INSERTS_PER_WRITER);
    for index in 0..WRITERS * INSERTS_PER_WRITER {
        assert_eq!(
            map.get_cloned(&binary_key(index as u64)),
            Some(index as u64)
        );
    }
}

#[test]
fn contended_updates_do_not_lose_increments() {
    const WRITERS: usize = 8;
    const UPDATES_PER_WRITER: usize = 5_000;

    let map = ConcurrentSwissMap::with_capacity(1);
    map.try_insert(b"counter", 0_u64).unwrap();
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for _ in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..UPDATES_PER_WRITER {
                    assert_eq!(map.update(b"counter", |value| *value += 1), Some(()));
                }
            });
        }
    });

    assert_eq!(
        map.get_cloned(b"counter"),
        Some((WRITERS * UPDATES_PER_WRITER) as u64)
    );
}

#[test]
fn insert_new_has_exactly_one_winner() {
    const WRITERS: usize = 16;

    let map = ConcurrentSwissMap::with_capacity(1);
    let start = Barrier::new(WRITERS);
    let winners = AtomicUsize::new(0);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            let winners = &winners;
            scope.spawn(move || {
                start.wait();
                if map.try_insert_new(b"winner", writer).unwrap() {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(winners.load(Ordering::Relaxed), 1);
    assert!(map.get_cloned(b"winner").is_some());
    assert_eq!(map.len(), 1);
}

#[test]
fn readers_and_writers_share_the_table_safely() {
    const KEYS: usize = 1_024;
    const READERS: usize = 4;
    const WRITERS: usize = 4;
    const OPERATIONS: usize = 10_000;

    let map = ConcurrentSwissMap::with_capacity(KEYS);
    for index in 0..KEYS {
        map.try_insert(&binary_key(index as u64), 0_u64).unwrap();
    }

    let start = Barrier::new(READERS + WRITERS);
    thread::scope(|scope| {
        for reader in 0..READERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for operation in 0..OPERATIONS {
                    let index = (reader * 97 + operation * 17) % KEYS;
                    assert!(map.get_cloned(&binary_key(index as u64)).is_some());
                }
            });
        }
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for operation in 0..OPERATIONS {
                    let index = (writer * 193 + operation * 29) % KEYS;
                    map.update(&binary_key(index as u64), |value| *value += 1)
                        .unwrap();
                }
            });
        }
    });

    let total: u64 = (0..KEYS)
        .map(|index| map.get_cloned(&binary_key(index as u64)).unwrap())
        .sum();
    assert_eq!(total, (WRITERS * OPERATIONS) as u64);
    assert_eq!(map.len(), KEYS);
}

#[test]
fn shard_by_shard_compaction_reclaims_delete_churn() {
    const KEYS: usize = 4_096;

    let map = ConcurrentSwissMap::try_with_capacity_and_shards(KEYS, 32, SegmentedLoad::Balanced)
        .unwrap();
    for index in 0..KEYS {
        map.try_insert(&binary_key(index as u64), index as u64)
            .unwrap();
    }
    for index in (0..KEYS).step_by(2) {
        map.remove(&binary_key(index as u64)).unwrap();
    }
    assert_eq!(map.stats().dead_key_bytes(), KEYS / 2 * 32);

    let compacted = map.compact_key_arenas().unwrap();
    assert_eq!(compacted.live_entries, KEYS / 2);
    assert_eq!(compacted.discarded_key_bytes, KEYS / 2 * 32);
    assert_eq!(map.stats().dead_key_bytes(), 0);
    assert_eq!(map.len(), KEYS / 2);
    for index in 0..KEYS {
        assert_eq!(
            map.get_cloned(&binary_key(index as u64)),
            (index % 2 == 1).then_some(index as u64)
        );
    }
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    let mut state = value;
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
