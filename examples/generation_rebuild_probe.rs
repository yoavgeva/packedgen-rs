//! Packed lock-free generation rebuild and striped-handoff probe.

#![allow(missing_docs, clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use packedgen::LockFreeGenerationMap;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 1_000_000);
    let churn_percent = argument(&mut arguments, 1).min(100);
    let writer_threads = argument(&mut arguments, 0);
    let churn = entries.saturating_mul(churn_percent).div_ceil(100);
    let writer_working_set = argument(&mut arguments, churn.max(1)).min(entries).max(1);

    let map = LockFreeGenerationMap::try_from_entries(
        (0..entries).map(|index| {
            let value = index as u64;
            (binary_key(value), value)
        }),
        churn.max(1),
    )
    .unwrap();

    let updates = churn / 2;
    let deletes = churn / 4;
    let inserts = churn.saturating_sub(updates).saturating_sub(deletes);
    for index in 0..updates {
        map.update(&binary_key(index as u64), |value| value + 1)
            .unwrap();
    }
    for index in updates..updates + deletes {
        map.remove(&binary_key(index as u64)).unwrap();
    }
    for offset in 0..inserts {
        let value = (entries + offset) as u64;
        map.insert(&binary_key(value), value);
    }

    let before = map.stats();
    let started = Instant::now();
    let writer_operations = AtomicU64::new(0);
    let rebuilt = if writer_threads == 0 {
        map.rebuild(churn.max(1)).unwrap()
    } else {
        let running = AtomicBool::new(true);
        let start = Barrier::new(writer_threads + 1);
        thread::scope(|scope| {
            for writer in 0..writer_threads {
                let map = &map;
                let running = &running;
                let start = &start;
                let writer_operations = &writer_operations;
                scope.spawn(move || {
                    let mut operation = writer;
                    start.wait();
                    while running.load(Ordering::Acquire) {
                        let key_index = operation % writer_working_set;
                        let _ = map
                            .update(&binary_key(key_index as u64), |value| value.wrapping_add(1));
                        writer_operations.fetch_add(1, Ordering::Relaxed);
                        operation = operation.wrapping_add(writer_threads);
                    }
                });
            }
            start.wait();
            let rebuilt = map.rebuild(churn.max(1)).unwrap();
            running.store(false, Ordering::Release);
            rebuilt
        })
    };
    let elapsed = started.elapsed();
    let after = map.stats();
    if writer_threads == 0 {
        assert_eq!(after.current.overlay_records, 0);
    }
    assert_eq!(rebuilt.entries, map.len());

    println!(
        "entries,churn_percent,overlay_records,logical_entries,writer_threads,writer_working_set,writer_operations,total_ms,writer_redirect_us,background_build_ms,base_publish_us,compacted_overlay_records,layer_depth,published_generation"
    );
    println!(
        "{entries},{churn_percent},{},{},{writer_threads},{writer_working_set},{},{:.3},{:.3},{:.3},{:.3},{},{},{}",
        before.current.overlay_records,
        rebuilt.entries,
        writer_operations.load(Ordering::Relaxed),
        elapsed.as_secs_f64() * 1_000.0,
        rebuilt.writer_redirect.as_secs_f64() * 1_000_000.0,
        rebuilt.background_build.as_secs_f64() * 1_000.0,
        rebuilt.base_publish.as_secs_f64() * 1_000_000.0,
        rebuilt.compacted_overlay_records,
        after.layer_depth,
        after.generation
    );
    black_box(map);
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("arguments must be integers")
    })
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
