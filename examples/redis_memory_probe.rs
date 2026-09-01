//! Memory comparison for ordinary Redis-style binary records with per-key TTL.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

#[cfg(not(feature = "jemalloc-probe"))]
use std::alloc::System;
use std::hint::black_box;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{CacheConfig, DirectPackedCache};
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

const DEFAULT_ENTRIES: usize = 1_000_000;
const DEFAULT_VALUE_BYTES: usize = 64;
const DEFAULT_TTL_SECONDS: usize = 3_600;
const LOAD_PIPELINE: usize = 256;
const DEFAULT_OPERATIONS: usize = 10_000_000;
const DEFAULT_THREADS: usize = 8;
const DEFAULT_PIPELINE: usize = 32;
const DEFAULT_BUDGET_MIB: usize = 128;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let implementation = arguments.next().unwrap_or_else(|| "packedgen".to_owned());
    let entries = argument(&mut arguments, DEFAULT_ENTRIES);
    let value_bytes = argument(&mut arguments, DEFAULT_VALUE_BYTES);
    let ttl_seconds = u64::try_from(argument(&mut arguments, DEFAULT_TTL_SECONDS)).unwrap();
    let operations = argument(&mut arguments, DEFAULT_OPERATIONS);
    let threads = argument(&mut arguments, DEFAULT_THREADS).max(1);
    let catalog = argument(&mut arguments, entries.saturating_mul(2)).max(entries);
    let pipeline = argument(&mut arguments, DEFAULT_PIPELINE).max(1);
    let budget_mib = argument(&mut arguments, DEFAULT_BUDGET_MIB).max(1);
    let profile = arguments.next().unwrap_or_else(|| "mixed".to_owned());
    assert!(
        matches!(
            profile.as_str(),
            "mixed"
                | "read-hit"
                | "read-miss"
                | "update-hit"
                | "insert-new"
                | "delete-hit"
                | "delete-miss"
        ),
        "unknown workload profile"
    );
    match implementation.as_str() {
        "packedgen" => {
            print_memory_header();
            dispatch_packed(entries, value_bytes, ttl_seconds);
        }
        "redis" => {
            print_memory_header();
            run_redis(entries, value_bytes, ttl_seconds);
        }
        "packedgen-workload" => {
            print_workload_header();
            dispatch_packed_workload(
                entries,
                value_bytes,
                ttl_seconds,
                operations,
                threads,
                catalog,
                &profile,
            );
        }
        "redis-workload" => {
            print_workload_header();
            run_redis_workload(
                entries,
                value_bytes,
                ttl_seconds,
                operations,
                threads,
                catalog,
                pipeline,
                budget_mib,
                &profile,
            );
        }
        _ => {
            panic!("implementation must be packedgen, redis, packedgen-workload, or redis-workload")
        }
    }
}

fn print_memory_header() {
    println!(
        "implementation,version,entries,value_bytes,ttl_seconds,live_bytes,bytes_per_entry,allocations,rss_delta_bytes,rss_total_bytes,dataset_bytes"
    );
}

fn print_workload_header() {
    println!(
        "implementation,version,profile,capacity,initial_entries,catalog,operations,threads,pipeline,value_bytes,ttl_seconds,mops,read_hit_pct,final_entries,live_bytes,rss_delta_bytes,evictions"
    );
}

fn dispatch_packed(entries: usize, value_bytes: usize, ttl_seconds: u64) {
    match value_bytes {
        16 => run_packed::<16>(entries, ttl_seconds),
        64 => run_packed::<64>(entries, ttl_seconds),
        256 => run_packed::<256>(entries, ttl_seconds),
        1_024 => run_packed::<1_024>(entries, ttl_seconds),
        _ => panic!("PackedGen value size must be 16, 64, 256, or 1024"),
    }
}

fn run_packed<const VALUE_BYTES: usize>(entries: usize, ttl_seconds: u64) {
    let rss_before = process_rss_bytes(std::process::id()).unwrap_or(0);
    let region = Region::new(GLOBAL);
    let ttl = (ttl_seconds != 0).then(|| Duration::from_secs(ttl_seconds));
    let records = (0..entries).map(|index| {
        let key = MixedBinaryKey::new(index);
        let weight = u64::try_from(key.as_ref().len() + VALUE_BYTES).unwrap();
        (
            key,
            [u8::try_from(index & 255).unwrap(); VALUE_BYTES],
            weight,
            ttl,
        )
    });
    let cache = DirectPackedCache::try_from_entries_with_options(
        CacheConfig::new(u64::MAX)
            .with_max_entries(entries.saturating_mul(2).max(1))
            .with_overlay_capacity(entries.clamp(1, 65_536)),
        records,
    )
    .unwrap();
    let stats = region.change();
    let live_bytes = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated);
    let live_allocations = stats.allocations.saturating_sub(stats.deallocations);
    let rss_total = process_rss_bytes(std::process::id()).unwrap_or(0);
    println!(
        "packedgen-direct,0.1.0,{entries},{VALUE_BYTES},{ttl_seconds},{live_bytes},{:.3},{live_allocations},{},{rss_total},{live_bytes}",
        live_bytes as f64 / entries.max(1) as f64,
        rss_total.saturating_sub(rss_before),
    );
    black_box(cache.len());
}

fn dispatch_packed_workload(
    capacity: usize,
    value_bytes: usize,
    ttl_seconds: u64,
    operations: usize,
    threads: usize,
    catalog: usize,
    profile: &str,
) {
    match value_bytes {
        16 => {
            run_packed_workload::<16>(capacity, ttl_seconds, operations, threads, catalog, profile);
        }
        64 => {
            run_packed_workload::<64>(capacity, ttl_seconds, operations, threads, catalog, profile);
        }
        256 => {
            run_packed_workload::<256>(
                capacity,
                ttl_seconds,
                operations,
                threads,
                catalog,
                profile,
            );
        }
        1_024 => {
            run_packed_workload::<1_024>(
                capacity,
                ttl_seconds,
                operations,
                threads,
                catalog,
                profile,
            );
        }
        _ => panic!("PackedGen value size must be 16, 64, 256, or 1024"),
    }
}

fn run_packed_workload<const VALUE_BYTES: usize>(
    capacity: usize,
    ttl_seconds: u64,
    operations: usize,
    threads: usize,
    catalog: usize,
    profile: &str,
) {
    if profile == "delete-hit" {
        assert!(
            operations <= capacity,
            "delete-hit needs one live key per operation"
        );
    }
    let rss_before = process_rss_bytes(std::process::id()).unwrap_or(0);
    let region = Region::new(GLOBAL);
    let ttl = (ttl_seconds != 0).then(|| Duration::from_secs(ttl_seconds));
    let records = (0..capacity).map(|index| {
        let key = MixedBinaryKey::new(index);
        let weight = u64::try_from(key.as_ref().len() + VALUE_BYTES).unwrap();
        (
            key,
            [u8::try_from(index & 255).unwrap(); VALUE_BYTES],
            weight,
            ttl,
        )
    });
    let config = CacheConfig::new(u64::MAX)
        .with_max_entries(capacity)
        .with_overlay_capacity(capacity.clamp(1, 65_536))
        .with_async_eviction(10_100);
    let cache =
        Arc::new(DirectPackedCache::try_from_entries_with_options(config, records).unwrap());
    raw_allocation("packed-bulk-load", region.change());
    let maintenance = cache.spawn_maintenance(Duration::from_secs(60));
    let refresh_interval = std::env::var("PACKED_CACHE_REFRESH_INTERVAL")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16_384)
        .max(1);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let read_hits = Arc::new(AtomicU64::new(0));
    let read_operations = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let workload_started = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for worker in 0..threads {
            let begin = operations * worker / threads;
            let end = operations * (worker + 1) / threads;
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            let read_hits = Arc::clone(&read_hits);
            let read_operations = Arc::clone(&read_operations);
            let checksum = Arc::clone(&checksum);
            workers.push(scope.spawn(move || {
                let mut guard = cache.pin();
                let mut local_hits = 0_u64;
                let mut local_reads = 0_u64;
                let mut local_checksum = 0_u64;
                barrier.wait();
                for operation in begin..end {
                    if operation.is_multiple_of(refresh_interval) {
                        guard.refresh();
                    }
                    if profile != "mixed" {
                        let key_index = match profile {
                            "read-hit" | "update-hit" | "delete-hit" => operation % capacity,
                            "read-miss" | "delete-miss" => {
                                catalog.saturating_add(2_000_000).saturating_add(operation)
                            }
                            "insert-new" => catalog.saturating_add(operation),
                            _ => unreachable!(),
                        };
                        let (key, key_bytes) = mixed_binary_key_array(key_index);
                        match profile {
                            "read-hit" | "read-miss" => {
                                let found = guard.get_untracked(&key[..key_bytes]);
                                local_hits += u64::from(found.is_some());
                                local_reads += 1;
                                local_checksum ^= found.map_or(0, |value| u64::from(value[0]));
                            }
                            "update-hit" | "insert-new" => {
                                cache
                                    .insert_discard_with_options(
                                        &key[..key_bytes],
                                        [u8::try_from(operation & 255).unwrap(); VALUE_BYTES],
                                        u64::try_from(key_bytes + VALUE_BYTES).unwrap(),
                                        ttl,
                                    )
                                    .unwrap();
                            }
                            "delete-hit" | "delete-miss" => {
                                cache.remove_discard(&key[..key_bytes]);
                            }
                            _ => unreachable!(),
                        }
                        continue;
                    }
                    let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
                    if roll < 95 {
                        let key_index = if roll < 90 {
                            workload_hit_index(operation, catalog)
                        } else {
                            catalog.saturating_add(1_000_000).saturating_add(operation)
                        };
                        let (key, key_bytes) = mixed_binary_key_array(key_index);
                        let found = guard.get_untracked(&key[..key_bytes]);
                        local_hits += u64::from(found.is_some());
                        local_reads += 1;
                        local_checksum ^=
                            found.map_or(0, |value| u64::from(value[operation % VALUE_BYTES]));
                    } else if roll < 99 {
                        let key_index = if roll < 97 {
                            workload_hit_index(operation, catalog)
                        } else {
                            catalog.saturating_add(operation)
                        };
                        let (key, key_bytes) = mixed_binary_key_array(key_index);
                        cache
                            .insert_discard_with_options(
                                &key[..key_bytes],
                                [u8::try_from(operation & 255).unwrap(); VALUE_BYTES],
                                u64::try_from(key_bytes + VALUE_BYTES).unwrap(),
                                ttl,
                            )
                            .unwrap();
                    } else {
                        let key_index = if mix(operation as u64 ^ 0xd311_e7e5) & 1 == 0 {
                            workload_hit_index(operation, catalog)
                        } else {
                            catalog.saturating_add(2_000_000).saturating_add(operation)
                        };
                        let (key, key_bytes) = mixed_binary_key_array(key_index);
                        cache.remove_discard(&key[..key_bytes]);
                    }
                }
                read_hits.fetch_add(local_hits, Ordering::Relaxed);
                read_operations.fetch_add(local_reads, Ordering::Relaxed);
                checksum.fetch_xor(local_checksum, Ordering::Relaxed);
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        started
    });
    black_box(checksum.load(Ordering::Relaxed));
    raw_allocation("packed-after-workers", region.change());
    drop(maintenance);
    raw_allocation("packed-after-worker-drop", region.change());
    if matches!(profile, "mixed" | "insert-new") {
        let final_maintenance_started = Instant::now();
        let final_maintenance = cache.maintain().unwrap();
        let final_maintenance_elapsed = final_maintenance_started.elapsed();
        if std::env::var_os("PACKED_CACHE_RAW").is_some() {
            eprintln!("packed-final-maintain,{final_maintenance:?},{final_maintenance_elapsed:?}");
        }
        raw_allocation("packed-final-maintain", region.change());
    }
    let elapsed = workload_started.elapsed();
    let hits = read_hits.load(Ordering::Relaxed);
    let reads = read_operations.load(Ordering::Relaxed);
    let final_entries = cache.len();
    let evictions = cache.stats().evictions;
    let stats = region.change();
    let live_bytes = stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated);
    let rss_total = process_rss_bytes(std::process::id()).unwrap_or(0);
    println!(
        "packedgen-direct-bulk-async,0.1.0,{profile},{capacity},{capacity},{catalog},{operations},{threads},1,{VALUE_BYTES},{ttl_seconds},{:.3},{:.3},{final_entries},{live_bytes},{},{evictions}",
        operations as f64 / elapsed.as_secs_f64() / 1e6,
        hits as f64 / reads.max(1) as f64 * 100.0,
        rss_total.saturating_sub(rss_before),
    );
}

fn run_redis(entries: usize, value_bytes: usize, ttl_seconds: u64) {
    let mut server = RedisServer::spawn();
    let mut connection = server.connect();
    let version = info_value(&mut connection, "server", "redis_version")
        .unwrap_or_else(|| "unknown".to_owned());
    let before = RedisMemory::read(&mut connection);
    let rss_before = process_rss_bytes(server.id()).unwrap_or(before.used_memory_rss);
    let value = vec![0x5a; value_bytes];
    let ttl = ttl_seconds.to_string();
    let mut commands = Vec::with_capacity(LOAD_PIPELINE * (value_bytes + 96));
    let mut queued = 0;
    for index in 0..entries {
        let key = mixed_binary_key(index);
        if ttl_seconds == 0 {
            encode_command(&mut commands, &[b"SET", &key, &value]);
        } else {
            encode_command(
                &mut commands,
                &[b"SET", &key, &value, b"EX", ttl.as_bytes()],
            );
        }
        queued += 1;
        if queued == LOAD_PIPELINE {
            connection.pipeline(&commands, queued).unwrap();
            commands.clear();
            queued = 0;
        }
    }
    if queued != 0 {
        connection.pipeline(&commands, queued).unwrap();
    }
    let stored = connection.integer(&[b"DBSIZE"]).unwrap();
    assert_eq!(usize::try_from(stored).unwrap(), entries);
    thread::sleep(Duration::from_millis(100));
    let after = RedisMemory::read(&mut connection);
    let rss_total = process_rss_bytes(server.id()).unwrap_or(after.used_memory_rss);
    let live_bytes = after.used_memory.saturating_sub(before.used_memory);
    let dataset_bytes = after.dataset_bytes.saturating_sub(before.dataset_bytes);
    println!(
        "redis,{version},{entries},{value_bytes},{ttl_seconds},{live_bytes},{:.3},na,{},{rss_total},{dataset_bytes}",
        live_bytes as f64 / entries.max(1) as f64,
        rss_total.saturating_sub(rss_before),
    );
    connection.command(&[b"SHUTDOWN", b"NOSAVE"]).ok();
    server.wait();
}

#[allow(clippy::too_many_arguments)]
fn run_redis_workload(
    capacity: usize,
    value_bytes: usize,
    ttl_seconds: u64,
    operations: usize,
    threads: usize,
    catalog: usize,
    pipeline: usize,
    budget_mib: usize,
    profile: &str,
) {
    if profile == "delete-hit" {
        assert!(
            operations <= capacity,
            "delete-hit needs one live key per operation"
        );
    }
    let mut server = RedisServer::spawn();
    let mut control = server.connect();
    let version =
        info_value(&mut control, "server", "redis_version").unwrap_or_else(|| "unknown".to_owned());
    let before = RedisMemory::read(&mut control);
    let rss_before = process_rss_bytes(server.id()).unwrap_or(before.used_memory_rss);
    control
        .command(&[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lfu"])
        .unwrap();
    let budget_bytes = u64::try_from(budget_mib)
        .unwrap()
        .saturating_mul(1024 * 1024);
    let maxmemory = before.used_memory.saturating_add(budget_bytes).to_string();
    control
        .command(&[b"CONFIG", b"SET", b"maxmemory", maxmemory.as_bytes()])
        .unwrap();
    load_redis_records(&mut control, capacity, value_bytes, ttl_seconds);
    let prefilled = usize::try_from(control.integer(&[b"DBSIZE"]).unwrap()).unwrap();

    let barrier = Arc::new(Barrier::new(threads + 1));
    let read_hits = Arc::new(AtomicU64::new(0));
    let read_operations = Arc::new(AtomicU64::new(0));
    let port = server.port();
    let elapsed = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for worker in 0..threads {
            let begin = operations * worker / threads;
            let end = operations * (worker + 1) / threads;
            let barrier = Arc::clone(&barrier);
            let read_hits = Arc::clone(&read_hits);
            let read_operations = Arc::clone(&read_operations);
            workers.push(scope.spawn(move || {
                let mut connection = RedisConnection::connect(port).unwrap();
                let value = vec![u8::try_from(worker & 255).unwrap(); value_bytes];
                let ttl = ttl_seconds.to_string();
                let mut encoded = Vec::with_capacity(pipeline * (value_bytes + 96));
                let mut reads = Vec::with_capacity(pipeline);
                let mut local_hits = 0_u64;
                let mut local_reads = 0_u64;
                barrier.wait();
                let mut operation = begin;
                while operation < end {
                    encoded.clear();
                    reads.clear();
                    let batch_end = end.min(operation.saturating_add(pipeline));
                    while operation < batch_end {
                        if profile != "mixed" {
                            let key_index = match profile {
                                "read-hit" | "update-hit" | "delete-hit" => operation % capacity,
                                "read-miss" | "delete-miss" => {
                                    catalog.saturating_add(2_000_000).saturating_add(operation)
                                }
                                "insert-new" => catalog.saturating_add(operation),
                                _ => unreachable!(),
                            };
                            let (key, key_bytes) = mixed_binary_key_array(key_index);
                            match profile {
                                "read-hit" | "read-miss" => {
                                    encode_command(&mut encoded, &[b"GET", &key[..key_bytes]]);
                                    reads.push(true);
                                }
                                "update-hit" | "insert-new" => {
                                    if ttl_seconds == 0 {
                                        encode_command(
                                            &mut encoded,
                                            &[b"SET", &key[..key_bytes], &value],
                                        );
                                    } else {
                                        encode_command(
                                            &mut encoded,
                                            &[
                                                b"SET",
                                                &key[..key_bytes],
                                                &value,
                                                b"EX",
                                                ttl.as_bytes(),
                                            ],
                                        );
                                    }
                                    reads.push(false);
                                }
                                "delete-hit" | "delete-miss" => {
                                    encode_command(&mut encoded, &[b"DEL", &key[..key_bytes]]);
                                    reads.push(false);
                                }
                                _ => unreachable!(),
                            }
                            operation += 1;
                            continue;
                        }
                        let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
                        if roll < 95 {
                            let key_index = if roll < 90 {
                                workload_hit_index(operation, catalog)
                            } else {
                                catalog.saturating_add(1_000_000).saturating_add(operation)
                            };
                            let (key, key_bytes) = mixed_binary_key_array(key_index);
                            encode_command(&mut encoded, &[b"GET", &key[..key_bytes]]);
                            reads.push(true);
                        } else if roll < 99 {
                            let key_index = if roll < 97 {
                                workload_hit_index(operation, catalog)
                            } else {
                                catalog.saturating_add(operation)
                            };
                            let (key, key_bytes) = mixed_binary_key_array(key_index);
                            if ttl_seconds == 0 {
                                encode_command(&mut encoded, &[b"SET", &key[..key_bytes], &value]);
                            } else {
                                encode_command(
                                    &mut encoded,
                                    &[b"SET", &key[..key_bytes], &value, b"EX", ttl.as_bytes()],
                                );
                            }
                            reads.push(false);
                        } else {
                            let key_index = if mix(operation as u64 ^ 0xd311_e7e5) & 1 == 0 {
                                workload_hit_index(operation, catalog)
                            } else {
                                catalog.saturating_add(2_000_000).saturating_add(operation)
                            };
                            let (key, key_bytes) = mixed_binary_key_array(key_index);
                            encode_command(&mut encoded, &[b"DEL", &key[..key_bytes]]);
                            reads.push(false);
                        }
                        operation += 1;
                    }
                    let (hits, read_count) = connection.mixed_pipeline(&encoded, &reads).unwrap();
                    local_hits += hits;
                    local_reads += read_count;
                }
                read_hits.fetch_add(local_hits, Ordering::Relaxed);
                read_operations.fetch_add(local_reads, Ordering::Relaxed);
            }));
        }
        barrier.wait();
        let started = Instant::now();
        for worker in workers {
            worker.join().unwrap();
        }
        started.elapsed()
    });
    let after = RedisMemory::read(&mut control);
    let final_entries = control.integer(&[b"DBSIZE"]).unwrap();
    let evictions = info_value(&mut control, "stats", "evicted_keys")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let hits = read_hits.load(Ordering::Relaxed);
    let reads = read_operations.load(Ordering::Relaxed);
    let rss_total = process_rss_bytes(server.id()).unwrap_or(after.used_memory_rss);
    println!(
        "redis-allkeys-lfu,{version},{profile},{capacity},{prefilled},{catalog},{operations},{threads},{pipeline},{value_bytes},{ttl_seconds},{:.3},{:.3},{final_entries},{},{},{evictions}",
        operations as f64 / elapsed.as_secs_f64() / 1e6,
        hits as f64 / reads.max(1) as f64 * 100.0,
        after.used_memory.saturating_sub(before.used_memory),
        rss_total.saturating_sub(rss_before),
    );
    control.command(&[b"SHUTDOWN", b"NOSAVE"]).ok();
    server.wait();
}

fn load_redis_records(
    connection: &mut RedisConnection,
    entries: usize,
    value_bytes: usize,
    ttl_seconds: u64,
) {
    let value = vec![0x5a; value_bytes];
    let ttl = ttl_seconds.to_string();
    let mut commands = Vec::with_capacity(LOAD_PIPELINE * (value_bytes + 96));
    let mut queued = 0;
    for index in 0..entries {
        let (key, key_bytes) = mixed_binary_key_array(index);
        if ttl_seconds == 0 {
            encode_command(&mut commands, &[b"SET", &key[..key_bytes], &value]);
        } else {
            encode_command(
                &mut commands,
                &[b"SET", &key[..key_bytes], &value, b"EX", ttl.as_bytes()],
            );
        }
        queued += 1;
        if queued == LOAD_PIPELINE {
            connection.pipeline(&commands, queued).unwrap();
            commands.clear();
            queued = 0;
        }
    }
    if queued != 0 {
        connection.pipeline(&commands, queued).unwrap();
    }
}

struct RedisServer {
    child: Option<Child>,
    port: u16,
}

impl RedisServer {
    fn spawn() -> Self {
        let port = available_port();
        let executable = std::env::var_os("REDIS_SERVER").unwrap_or_else(|| "redis-server".into());
        let child = Command::new(executable)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--protected-mode",
                "no",
                "--save",
                "",
                "--appendonly",
                "no",
                "--daemonize",
                "no",
                "--maxmemory",
                "0",
                "--loglevel",
                "warning",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start redis-server");
        let server = Self {
            child: Some(child),
            port,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Ok(mut connection) = RedisConnection::connect(port)
                && connection.command(&[b"PING"]).is_ok()
            {
                return server;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("redis-server did not become ready");
    }

    fn connect(&self) -> RedisConnection {
        RedisConnection::connect(self.port).expect("failed to connect to redis-server")
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("server remains owned").id()
    }

    const fn port(&self) -> u16 {
        self.port
    }

    fn wait(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct RedisConnection {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl RedisConnection {
    fn connect(port: u16) -> io::Result<Self> {
        let writer = TcpStream::connect(("127.0.0.1", port))?;
        writer.set_nodelay(true)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Self { writer, reader })
    }

    fn command(&mut self, parts: &[&[u8]]) -> io::Result<RedisReply> {
        let mut encoded = Vec::with_capacity(128);
        encode_command(&mut encoded, parts);
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        read_reply(&mut self.reader)
    }

    fn integer(&mut self, parts: &[&[u8]]) -> io::Result<i64> {
        match self.command(parts)? {
            RedisReply::Integer(value) => Ok(value),
            reply => Err(io::Error::other(format!(
                "expected integer reply, got {reply:?}"
            ))),
        }
    }

    fn pipeline(&mut self, encoded: &[u8], replies: usize) -> io::Result<()> {
        self.writer.write_all(encoded)?;
        self.writer.flush()?;
        for _ in 0..replies {
            match read_reply(&mut self.reader)? {
                RedisReply::Simple => {}
                reply => {
                    return Err(io::Error::other(format!(
                        "expected simple pipeline reply, got {reply:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn mixed_pipeline(&mut self, encoded: &[u8], reads: &[bool]) -> io::Result<(u64, u64)> {
        self.writer.write_all(encoded)?;
        self.writer.flush()?;
        let mut hits = 0_u64;
        let mut read_count = 0_u64;
        for is_read in reads {
            let reply = read_reply(&mut self.reader)?;
            if *is_read {
                read_count += 1;
                match reply {
                    RedisReply::Bulk(_) => hits += 1,
                    RedisReply::Null => {}
                    other => {
                        return Err(io::Error::other(format!(
                            "expected read reply, got {other:?}"
                        )));
                    }
                }
            } else if !matches!(reply, RedisReply::Simple | RedisReply::Integer(_)) {
                return Err(io::Error::other(format!(
                    "expected write reply, got {reply:?}"
                )));
            }
        }
        Ok((hits, read_count))
    }
}

#[derive(Debug)]
enum RedisReply {
    Simple,
    Bulk(Vec<u8>),
    Integer(i64),
    Null,
}

fn read_reply(reader: &mut BufReader<TcpStream>) -> io::Result<RedisReply> {
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => {
            read_line(reader)?;
            Ok(RedisReply::Simple)
        }
        b':' => {
            let line = read_line(reader)?;
            let value = std::str::from_utf8(&line)
                .map_err(io::Error::other)?
                .parse()
                .map_err(io::Error::other)?;
            Ok(RedisReply::Integer(value))
        }
        b'$' => {
            let line = read_line(reader)?;
            let length = std::str::from_utf8(&line)
                .map_err(io::Error::other)?
                .parse::<isize>()
                .map_err(io::Error::other)?;
            if length < 0 {
                return Ok(RedisReply::Null);
            }
            let mut value = vec![0; usize::try_from(length).unwrap()];
            reader.read_exact(&mut value)?;
            let mut ending = [0; 2];
            reader.read_exact(&mut ending)?;
            if ending != *b"\r\n" {
                return Err(io::Error::other("invalid bulk reply ending"));
            }
            Ok(RedisReply::Bulk(value))
        }
        b'-' => Err(io::Error::other(
            String::from_utf8_lossy(&read_line(reader)?).into_owned(),
        )),
        other => Err(io::Error::other(format!(
            "unsupported Redis reply prefix {other}"
        ))),
    }
}

fn read_line(reader: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
        Ok(line)
    } else {
        Err(io::Error::other("invalid Redis line ending"))
    }
}

fn encode_command(output: &mut Vec<u8>, parts: &[&[u8]]) {
    write!(output, "*{}\r\n", parts.len()).unwrap();
    for part in parts {
        write!(output, "${}\r\n", part.len()).unwrap();
        output.extend_from_slice(part);
        output.extend_from_slice(b"\r\n");
    }
}

#[derive(Default)]
struct RedisMemory {
    used_memory: u64,
    used_memory_rss: u64,
    dataset_bytes: u64,
}

impl RedisMemory {
    fn read(connection: &mut RedisConnection) -> Self {
        let info = match connection.command(&[b"INFO", b"MEMORY"]).unwrap() {
            RedisReply::Bulk(info) => info,
            reply => panic!("expected INFO bulk reply, got {reply:?}"),
        };
        Self {
            used_memory: parse_info_u64(&info, "used_memory").unwrap(),
            used_memory_rss: parse_info_u64(&info, "used_memory_rss").unwrap(),
            dataset_bytes: parse_info_u64(&info, "used_memory_dataset").unwrap(),
        }
    }
}

fn info_value(connection: &mut RedisConnection, section: &str, field: &str) -> Option<String> {
    let reply = connection.command(&[b"INFO", section.as_bytes()]).ok()?;
    let RedisReply::Bulk(info) = reply else {
        return None;
    };
    parse_info(&info, field).map(ToOwned::to_owned)
}

fn parse_info_u64(info: &[u8], field: &str) -> Option<u64> {
    parse_info(info, field)?.parse().ok()
}

fn parse_info<'a>(info: &'a [u8], field: &str) -> Option<&'a str> {
    std::str::from_utf8(info).ok()?.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name == field).then_some(value.trim_end_matches('\r'))
    })
}

fn available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

fn raw_allocation(label: &str, stats: stats_alloc::Stats) {
    if std::env::var_os("PACKED_CACHE_RAW").is_some() {
        eprintln!(
            "{label},live_bytes={},live_allocations={}",
            stats
                .bytes_allocated
                .saturating_sub(stats.bytes_deallocated),
            stats.allocations.saturating_sub(stats.deallocations),
        );
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}

fn mixed_binary_key(index: usize) -> Box<[u8]> {
    let (key, bytes) = mixed_binary_key_array(index);
    key[..bytes].into()
}

struct MixedBinaryKey {
    bytes: [u8; 48],
    len: usize,
}

impl MixedBinaryKey {
    fn new(index: usize) -> Self {
        let (bytes, len) = mixed_binary_key_array(index);
        Self { bytes, len }
    }
}

impl AsRef<[u8]> for MixedBinaryKey {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn mixed_binary_key_array(index: usize) -> ([u8; 48], usize) {
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
    (key, bytes)
}

fn workload_hit_index(operation: usize, catalog: usize) -> usize {
    if catalog < 2 {
        return 0;
    }
    let hot = catalog.div_ceil(5).min(catalog - 1);
    if mix(operation as u64 ^ 0x61c8_8646_80b5_83eb).is_multiple_of(5) {
        hot + mixed_index(operation ^ 0x5a5a_3c3c, catalog - hot)
    } else {
        mixed_index(operation, hot)
    }
}

fn mixed_index(operation: usize, len: usize) -> usize {
    usize::try_from(mix(operation as u64) % len as u64).unwrap()
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
