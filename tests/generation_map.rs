#![allow(missing_docs)]

use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use packedgen::{InsertOutcome, LockFreeGenerationMap};

#[test]
fn point_operations_survive_generation_publication() {
    let map = build_generation(1_024, 128);
    assert_eq!(map.get_cloned(&binary_key(7)), Some(7));
    assert_eq!(map.insert(&binary_key(7), 70), InsertOutcome::Replaced(7));
    assert_eq!(map.remove(&binary_key(8)), Some(8));
    assert!(map.insert_new(&binary_key(2_000), 2_000));
    assert_eq!(map.update(&binary_key(9), |value| value + 1), Some(10));

    let rebuilt = map.rebuild(128).unwrap();
    assert_eq!(rebuilt.from_generation, 0);
    assert_eq!(rebuilt.to_generation, 1);
    assert_eq!(rebuilt.entries, 1_024);
    assert_eq!(rebuilt.compacted_overlay_records, 4);
    assert_eq!(rebuilt.previous_layer_depth, 1);
    assert_eq!(map.generation(), 1);
    assert_eq!(map.get_cloned(&binary_key(7)), Some(70));
    assert_eq!(map.get_cloned(&binary_key(8)), None);
    assert_eq!(map.get_cloned(&binary_key(9)), Some(10));
    assert_eq!(map.get_cloned(&binary_key(2_000)), Some(2_000));
    assert_eq!(map.stats().current.overlay_records, 0);
    assert_eq!(map.stats().layer_depth, 1);
}

#[test]
fn deleted_base_values_never_resurrect_across_rebuilds() {
    let map = build_generation(128, 16);
    let key = binary_key(42);
    assert_eq!(map.remove(&key), Some(42));
    for generation in 1..=4 {
        let rebuilt = map.rebuild(16).unwrap();
        assert_eq!(rebuilt.to_generation, generation);
        assert_eq!(map.get_cloned(&key), None);
    }
}

#[test]
fn concurrent_updates_are_not_lost_during_repeated_cutovers() {
    const BASE: usize = 10_000;
    const WRITERS: usize = 4;
    const UPDATES: usize = 5_000;
    const REBUILDS: usize = 6;

    let map = LockFreeGenerationMap::try_from_entries(
        std::iter::once((b"counter".to_vec(), 0_u64))
            .chain((0..BASE).map(|index| (binary_key(index as u64).to_vec(), index as u64))),
        1_024,
    )
    .unwrap();
    let start = Barrier::new(WRITERS + 1);
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
        start.wait();
        for _ in 0..REBUILDS {
            map.rebuild(1_024).unwrap();
        }
    });

    assert_eq!(map.get_cloned(b"counter"), Some((WRITERS * UPDATES) as u64));
    assert_eq!(map.generation(), REBUILDS as u64);
}

#[test]
fn readers_and_writers_make_progress_during_background_build() {
    const BASE: usize = 200_000;

    let map = build_generation(BASE, 1_024);
    map.insert(b"counter", 0);
    let start = Barrier::new(3);
    let rebuilding = AtomicBool::new(true);
    let reads = AtomicUsize::new(0);
    let writes = AtomicUsize::new(0);
    let maximum_layer_depth = AtomicUsize::new(0);
    thread::scope(|scope| {
        let map_ref = &map;
        let start_ref = &start;
        let rebuilding_ref = &rebuilding;
        let reads_ref = &reads;
        let maximum_layer_depth_ref = &maximum_layer_depth;
        scope.spawn(move || {
            start_ref.wait();
            while rebuilding_ref.load(Ordering::Acquire) {
                assert_eq!(map_ref.get_cloned(&binary_key(77)), Some(77));
                reads_ref.fetch_add(1, Ordering::Relaxed);
                maximum_layer_depth_ref.fetch_max(map_ref.stats().layer_depth, Ordering::Relaxed);
            }
        });
        let map_ref = &map;
        let start_ref = &start;
        let rebuilding_ref = &rebuilding;
        let writes_ref = &writes;
        scope.spawn(move || {
            start_ref.wait();
            while rebuilding_ref.load(Ordering::Acquire) {
                map_ref.update(b"counter", |value| value + 1).unwrap();
                writes_ref.fetch_add(1, Ordering::Relaxed);
            }
        });

        start.wait();
        let rebuilt = map.rebuild(1_024).unwrap();
        assert!(rebuilt.background_build > std::time::Duration::ZERO);
        rebuilding.store(false, Ordering::Release);
    });
    assert!(reads.load(Ordering::Relaxed) > 0);
    assert!(writes.load(Ordering::Relaxed) > 0);
    assert!(maximum_layer_depth.load(Ordering::Relaxed) >= 2);
    assert_eq!(map.stats().layer_depth, 1);
    assert_eq!(
        map.get_cloned(b"counter"),
        Some(writes.load(Ordering::Relaxed) as u64)
    );
}

#[test]
fn inserts_and_deletes_converge_across_layered_cutover() {
    const BASE: usize = 100_000;
    const WRITERS: usize = 4;
    const KEYS_PER_WRITER: usize = 64;

    let map = build_generation(BASE, 1_024);
    let running = AtomicBool::new(true);
    let start = Barrier::new(WRITERS + 1);
    let expected = (0..WRITERS * KEYS_PER_WRITER)
        .map(|_| AtomicBool::new(true))
        .collect::<Vec<_>>();
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let map = &map;
            let running = &running;
            let start = &start;
            let expected = &expected;
            scope.spawn(move || {
                let first = writer * KEYS_PER_WRITER;
                let mut operation = 0;
                start.wait();
                while running.load(Ordering::Acquire) {
                    let index = first + operation % KEYS_PER_WRITER;
                    let key = binary_key(index as u64);
                    if expected[index].load(Ordering::Relaxed) {
                        assert!(map.remove(&key).is_some());
                        expected[index].store(false, Ordering::Release);
                    } else {
                        assert!(map.insert_new(&key, index as u64));
                        expected[index].store(true, Ordering::Release);
                    }
                    operation += 1;
                }
            });
        }

        start.wait();
        for _ in 0..4 {
            map.rebuild(1_024).unwrap();
        }
        running.store(false, Ordering::Release);
    });

    for (index, present) in expected.iter().enumerate() {
        assert_eq!(
            map.contains_key(&binary_key(index as u64)),
            present.load(Ordering::Acquire)
        );
    }
}

fn build_generation(entries: usize, overlay_capacity: usize) -> LockFreeGenerationMap<u64> {
    LockFreeGenerationMap::try_from_entries(
        (0..entries).map(|index| (binary_key(index as u64), index as u64)),
        overlay_capacity,
    )
    .unwrap()
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
