//! Allocation and RSS stability across repeated full key turnover.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines, missing_docs)]

#[cfg(not(feature = "jemalloc-probe"))]
use std::alloc::System;
use std::hint::black_box;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use packedgen::{CacheConfig, DirectPackedCache};
use papaya::HashMap as PapayaHashMap;
#[cfg(not(feature = "jemalloc-probe"))]
use stats_alloc::INSTRUMENTED_SYSTEM;
use stats_alloc::{Region, StatsAlloc};
#[cfg(feature = "jemalloc-probe")]
use tikv_jemallocator::Jemalloc;

#[cfg(not(feature = "jemalloc-probe"))]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[cfg(feature = "jemalloc-probe")]
static INSTRUMENTED_JEMALLOC: StatsAlloc<Jemalloc> = StatsAlloc::new(Jemalloc);
#[cfg(feature = "jemalloc-probe")]
#[global_allocator]
static GLOBAL: &StatsAlloc<Jemalloc> = &INSTRUMENTED_JEMALLOC;

const VALUE_BYTES: usize = 64;
const REFRESH_INTERVAL: usize = 16_384;

struct PapayaControlValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: u32,
    accessed: bool,
}

type PapayaCache = PapayaHashMap<Box<[u8]>, PapayaControlValue, DefaultHashBuilder>;

#[derive(Clone, Copy)]
struct CycleResult {
    mops: f64,
    maintenance_ms: f64,
    rebuilt: bool,
}

#[derive(Clone, Copy)]
struct MemorySnapshot {
    live_bytes: usize,
    live_allocations: usize,
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let implementation = arguments.next().unwrap_or_else(|| "direct".to_owned());
    let entries = argument(&mut arguments, 200_000).max(1);
    let requested_cycles = argument(&mut arguments, 50);
    let threads = argument(&mut arguments, 8).max(1);
    let duration = arguments
        .next()
        .map(|value| Duration::from_secs(value.parse().expect("expected seconds as an integer")));
    let cycles = if duration.is_some() && requested_cycles == 0 {
        usize::MAX
    } else {
        requested_cycles.max(1)
    };
    let maximum_growth_bps = std::env::var("PACKEDGEN_SOAK_MAX_GROWTH_BPS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("expected growth basis points")
        });
    let report_every = std::env::var("PACKEDGEN_SOAK_REPORT_EVERY")
        .map_or(1, |value| {
            value.parse::<usize>().expect("expected a cycle count")
        })
        .max(1);

    println!(
        "implementation,cycle,elapsed_seconds,entries,threads,turnover_mops,maintenance_ms,rebuilt,live_bytes,bytes_per_entry,live_allocations,rss_bytes"
    );
    match implementation.as_str() {
        "direct" => run_direct(
            entries,
            cycles,
            threads,
            duration,
            maximum_growth_bps,
            report_every,
        ),
        "papaya-inline" => run_papaya(
            entries,
            cycles,
            threads,
            duration,
            maximum_growth_bps,
            report_every,
        ),
        _ => panic!("implementation must be direct or papaya-inline"),
    }
}

fn run_direct(
    entries: usize,
    cycles: usize,
    threads: usize,
    duration: Option<Duration>,
    maximum_growth_bps: Option<usize>,
    report_every: usize,
) {
    let run_started = Instant::now();
    let region = Region::new(GLOBAL);
    let records = (0..entries).map(|index| {
        let key = mixed_binary_key(index);
        let weight = charge(&key);
        (key, value(index), weight, None::<Duration>)
    });
    let cache = Arc::new(
        DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(u64::MAX)
                .with_max_entries(entries)
                .with_overlay_capacity(entries),
            records,
        )
        .unwrap(),
    );
    let baseline = memory_snapshot(&region);
    print_cycle(
        "direct-packed-cache",
        0,
        run_started.elapsed(),
        entries,
        threads,
        None,
        baseline,
    );

    for cycle in 0..cycles {
        if cycle > 0 && duration.is_some_and(|limit| run_started.elapsed() >= limit) {
            break;
        }
        let started = Instant::now();
        parallel_ranges(entries, threads, |begin, end| {
            let cache = Arc::clone(&cache);
            move || {
                let mut guard = cache.pin();
                for index in begin..end {
                    if index != begin && index.is_multiple_of(REFRESH_INTERVAL) {
                        guard.refresh();
                    }
                    let old = mixed_binary_key(cycle * entries + index);
                    let new = mixed_binary_key((cycle + 1) * entries + index);
                    assert!(cache.remove_discard(&old));
                    cache
                        .insert_discard_with_options(&new, value(cycle + index), charge(&new), None)
                        .unwrap();
                }
            }
        });
        let turnover = started.elapsed();
        let maintenance_started = Instant::now();
        let maintenance = cache.maintain().unwrap();
        settle_direct(&cache);
        let result = CycleResult {
            mops: entries as f64 / turnover.as_secs_f64() / 1e6,
            maintenance_ms: maintenance_started.elapsed().as_secs_f64() * 1_000.0,
            rebuilt: maintenance.rebuilt,
        };
        assert_eq!(cache.len(), entries);
        let snapshot = memory_snapshot(&region);
        assert_growth_bound(baseline, snapshot, maximum_growth_bps);
        let elapsed = run_started.elapsed();
        if (cycle + 1).is_multiple_of(report_every)
            || duration.is_some_and(|limit| elapsed >= limit)
        {
            print_cycle(
                "direct-packed-cache",
                cycle + 1,
                elapsed,
                entries,
                threads,
                Some(result),
                snapshot,
            );
        }
    }
    black_box(cache);
}

fn run_papaya(
    entries: usize,
    cycles: usize,
    threads: usize,
    duration: Option<Duration>,
    maximum_growth_bps: Option<usize>,
    report_every: usize,
) {
    let run_started = Instant::now();
    let region = Region::new(GLOBAL);
    let cache = Arc::new(PapayaCache::with_capacity_and_hasher(
        entries,
        DefaultHashBuilder::default(),
    ));
    {
        let guard = cache.pin();
        for index in 0..entries {
            let key = mixed_binary_key(index);
            guard.insert(key.clone(), PapayaControlValue::new(index, &key));
        }
    }
    let baseline = memory_snapshot(&region);
    print_cycle(
        "papaya-inline-compact",
        0,
        run_started.elapsed(),
        entries,
        threads,
        None,
        baseline,
    );

    for cycle in 0..cycles {
        if cycle > 0 && duration.is_some_and(|limit| run_started.elapsed() >= limit) {
            break;
        }
        let started = Instant::now();
        parallel_ranges(entries, threads, |begin, end| {
            let cache = Arc::clone(&cache);
            move || {
                for block_begin in (begin..end).step_by(REFRESH_INTERVAL) {
                    let block_end = (block_begin + REFRESH_INTERVAL).min(end);
                    let guard = cache.pin();
                    for index in block_begin..block_end {
                        let old = mixed_binary_key(cycle * entries + index);
                        let new = mixed_binary_key((cycle + 1) * entries + index);
                        assert!(guard.remove(&old).is_some());
                        guard.insert(new.clone(), PapayaControlValue::new(cycle + index, &new));
                    }
                }
            }
        });
        let turnover = started.elapsed();
        let maintenance_started = Instant::now();
        settle_papaya(&cache);
        let result = CycleResult {
            mops: entries as f64 / turnover.as_secs_f64() / 1e6,
            maintenance_ms: maintenance_started.elapsed().as_secs_f64() * 1_000.0,
            rebuilt: false,
        };
        assert_eq!(cache.len(), entries);
        let snapshot = memory_snapshot(&region);
        assert_growth_bound(baseline, snapshot, maximum_growth_bps);
        let elapsed = run_started.elapsed();
        if (cycle + 1).is_multiple_of(report_every)
            || duration.is_some_and(|limit| elapsed >= limit)
        {
            print_cycle(
                "papaya-inline-compact",
                cycle + 1,
                elapsed,
                entries,
                threads,
                Some(result),
                snapshot,
            );
        }
    }
    let guard = cache.pin();
    let checksum = guard.iter().fold(0_u64, |sum, (_, entry)| {
        sum.wrapping_add(u64::from(entry.value[0]))
            .wrapping_add(u64::from(entry.weight))
            .wrapping_add(u64::from(entry.expires_at))
            .wrapping_add(u64::from(entry.accessed))
    });
    drop(guard);
    black_box((cache, checksum));
}

fn parallel_ranges<F, W>(entries: usize, threads: usize, worker: F)
where
    F: Fn(usize, usize) -> W,
    W: FnOnce() + Send,
{
    let barrier = Arc::new(Barrier::new(threads + 1));
    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let begin = entries * thread / threads;
            let end = entries * (thread + 1) / threads;
            let barrier = Arc::clone(&barrier);
            let work = worker(begin, end);
            workers.push(scope.spawn(move || {
                barrier.wait();
                work();
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
    });
}

fn settle_direct(cache: &DirectPackedCache<[u8; VALUE_BYTES]>) {
    for _ in 0..8 {
        let mut guard = cache.pin();
        guard.refresh();
        drop(guard);
        thread::yield_now();
    }
}

fn settle_papaya(cache: &PapayaCache) {
    for _ in 0..8 {
        drop(cache.pin());
        thread::yield_now();
    }
}

fn memory_snapshot<T: std::alloc::GlobalAlloc>(region: &Region<'_, T>) -> MemorySnapshot {
    let stats = region.change();
    MemorySnapshot {
        live_bytes: stats
            .bytes_allocated
            .saturating_sub(stats.bytes_deallocated),
        live_allocations: stats.allocations.saturating_sub(stats.deallocations),
    }
}

fn print_cycle(
    implementation: &str,
    cycle: usize,
    elapsed: Duration,
    entries: usize,
    threads: usize,
    result: Option<CycleResult>,
    snapshot: MemorySnapshot,
) {
    let (mops, maintenance_ms, rebuilt) = result.map_or_else(
        || ("na".to_owned(), "na".to_owned(), "false"),
        |result| {
            (
                format!("{:.3}", result.mops),
                format!("{:.3}", result.maintenance_ms),
                if result.rebuilt { "true" } else { "false" },
            )
        },
    );
    println!(
        "{implementation},{cycle},{:.3},{entries},{threads},{mops},{maintenance_ms},{rebuilt},{},{:.3},{},{}",
        elapsed.as_secs_f64(),
        snapshot.live_bytes,
        snapshot.live_bytes as f64 / entries as f64,
        snapshot.live_allocations,
        process_rss_bytes(std::process::id()).unwrap_or(0),
    );
}

fn assert_growth_bound(
    baseline: MemorySnapshot,
    current: MemorySnapshot,
    maximum_growth_bps: Option<usize>,
) {
    let Some(maximum_growth_bps) = maximum_growth_bps else {
        return;
    };
    let allowed_growth = baseline
        .live_bytes
        .saturating_mul(maximum_growth_bps)
        .div_ceil(10_000);
    let limit = baseline.live_bytes.saturating_add(allowed_growth);
    assert!(
        current.live_bytes <= limit,
        "live allocation growth exceeded the configured bound: baseline={} current={} limit={} growth_bps={maximum_growth_bps}",
        baseline.live_bytes,
        current.live_bytes,
        limit,
    );
}

impl PapayaControlValue {
    fn new(index: usize, key: &[u8]) -> Self {
        Self {
            value: value(index),
            weight: u32::try_from(charge(key)).unwrap(),
            expires_at: u32::MAX,
            accessed: false,
        }
    }
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kib = std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(kib.saturating_mul(1_024))
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn charge(key: &[u8]) -> u64 {
    VALUE_BYTES as u64 + key.len() as u64
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}

fn mixed_binary_key(index: usize) -> Box<[u8]> {
    let mut key = [0_u8; 48];
    let mut state = index as u64;
    for chunk in key.as_chunks_mut::<8>().0 {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    let bytes = match index % 100 {
        0..=39 => 8,
        40..=64 => 16,
        65..=79 => 24,
        80..=89 => 32,
        _ => 48,
    };
    key[..bytes].into()
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
