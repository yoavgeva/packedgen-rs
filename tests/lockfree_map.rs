#![allow(missing_docs)]

use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use packedgen::{InsertOutcome, LockFreeBinaryMap};

#[test]
fn exact_binary_crud_and_atomic_operations() {
    let map = LockFreeBinaryMap::with_capacity(8);
    assert_eq!(map.insert(b"key", 10), InsertOutcome::Inserted);
    assert_eq!(map.insert(b"key", 11), InsertOutcome::Replaced(10));
    assert_eq!(map.get_cloned(b"key"), Some(11));
    assert!(!map.insert_new(b"key", 99));
    assert!(map.insert_new(b"new", 20));
    assert_eq!(map.update(b"key", |value| value + 1), Some(12));
    assert_eq!(map.update(b"missing", |value| value + 1), None);
    assert_eq!(map.upsert(b"key", 0, |value| value * 2), 24);
    assert_eq!(map.upsert(b"third", 3, |value| value + 1), 3);
    assert_eq!(map.remove_if(b"key", |value| *value < 24), None);
    assert_eq!(map.remove_if(b"key", |value| *value == 24), Some(24));
    assert_eq!(map.remove(b"new"), Some(20));
    assert_eq!(map.len(), 1);
    map.clear();
    assert!(map.is_empty());
}

#[test]
fn concurrent_writers_insert_without_locks_in_the_api() {
    const WRITERS: usize = 8;
    const INSERTS: usize = 4_000;

    let map = LockFreeBinaryMap::with_capacity(WRITERS * INSERTS);
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for offset in 0..INSERTS {
                    let index = writer * INSERTS + offset;
                    assert_eq!(
                        map.insert(&binary_key(index as u64), index as u64),
                        InsertOutcome::Inserted
                    );
                }
            });
        }
    });
    assert_eq!(map.len(), WRITERS * INSERTS);
}

#[test]
fn cas_updates_do_not_lose_increments() {
    const WRITERS: usize = 8;
    const UPDATES: usize = 10_000;

    let map = LockFreeBinaryMap::with_capacity(1);
    map.insert(b"counter", 0_u64);
    let start = Barrier::new(WRITERS);
    thread::scope(|scope| {
        for _ in 0..WRITERS {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..UPDATES {
                    map.update(b"counter", |value| value + 1).unwrap();
                }
            });
        }
    });
    assert_eq!(map.get_cloned(b"counter"), Some((WRITERS * UPDATES) as u64));
}

#[test]
fn insert_new_has_one_winner() {
    const WRITERS: usize = 16;

    let map = LockFreeBinaryMap::with_capacity(1);
    let start = Barrier::new(WRITERS);
    let winners = AtomicUsize::new(0);
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let start = &start;
            let winners = &winners;
            scope.spawn(move || {
                start.wait();
                if map.insert_new(b"winner", writer) {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(winners.load(Ordering::Relaxed), 1);
}

#[test]
fn lock_free_readers_overlap_cas_writers() {
    const KEYS: usize = 2_048;
    const READERS: usize = 4;
    const WRITERS: usize = 4;
    const OPERATIONS: usize = 20_000;

    let map = LockFreeBinaryMap::with_capacity(KEYS);
    for index in 0..KEYS {
        map.insert(&binary_key(index as u64), 0_u64);
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
                    map.update(&binary_key(index as u64), |value| value + 1)
                        .unwrap();
                }
            });
        }
    });
    let total: u64 = (0..KEYS)
        .map(|index| map.get_cloned(&binary_key(index as u64)).unwrap())
        .sum();
    assert_eq!(total, (WRITERS * OPERATIONS) as u64);
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
