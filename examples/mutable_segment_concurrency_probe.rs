//! Multicore mutable-cache comparison against `DirectPackedCache` and Papaya.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::alloc::System;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use packedgen::{
    CacheConfig, DirectPackedCache, MutableSegmentCache, OnlineMutableSegmentCache,
    SegmentCacheConfig,
};
use papaya::HashMap as PapayaHashMap;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const VALUE_BYTES: usize = 64;
const TTL: Duration = Duration::from_secs(3_600);

struct PapayaValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: AtomicU32,
    accessed: AtomicBool,
}

type PapayaCache = PapayaHashMap<Box<[u8]>, PapayaValue, DefaultHashBuilder>;

#[derive(Clone, Copy)]
enum Workload {
    ReadPristine,
    ReadMiss,
    ReadUpdated,
    ReadNew,
    ReadDeleted,
    ReadUntouchedAfterUpdate,
    Update,
    Insert,
    Delete,
    Read95Update5,
    Read80Update20,
}

impl Workload {
    const fn name(self) -> &'static str {
        match self {
            Self::ReadPristine => "read-pristine",
            Self::ReadMiss => "read-miss",
            Self::ReadUpdated => "read-updated",
            Self::ReadNew => "read-new",
            Self::ReadDeleted => "read-deleted",
            Self::ReadUntouchedAfterUpdate => "read-untouched-after-update",
            Self::Update => "update",
            Self::Insert => "insert",
            Self::Delete => "delete",
            Self::Read95Update5 => "read95-update5",
            Self::Read80Update20 => "read80-update20",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "read-pristine" => Self::ReadPristine,
            "read-miss" => Self::ReadMiss,
            "read-updated" => Self::ReadUpdated,
            "read-new" => Self::ReadNew,
            "read-deleted" => Self::ReadDeleted,
            "read-untouched-after-update" => Self::ReadUntouchedAfterUpdate,
            "update" => Self::Update,
            "insert" => Self::Insert,
            "delete" => Self::Delete,
            "read95-update5" => Self::Read95Update5,
            "read80-update20" => Self::Read80Update20,
            _ => panic!("unknown workload"),
        }
    }

    const fn prepares_updates(self) -> bool {
        matches!(self, Self::ReadUpdated | Self::ReadUntouchedAfterUpdate)
    }

    const fn prepares_new(self) -> bool {
        matches!(self, Self::ReadNew)
    }

    const fn prepares_deletes(self) -> bool {
        matches!(self, Self::ReadDeleted)
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let backend = arguments.next().unwrap_or_else(|| "segment".to_owned());
    let workload = Workload::parse(
        &arguments
            .next()
            .unwrap_or_else(|| "read95-update5".to_owned()),
    );
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let operations = argument(&mut arguments, 5_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let delta_capacity = argument(&mut arguments, entries.div_ceil(10).max(1_024)).max(1);
    let target_occupied_slots = u8::try_from(argument(&mut arguments, 7)).unwrap();
    let negative_filter_bits = u8::try_from(argument(&mut arguments, 0)).unwrap();
    assert!(
        (1..=7).contains(&target_occupied_slots),
        "target bucket occupancy must be between one and seven"
    );
    let working = delta_capacity.min(entries);
    assert!(
        !matches!(workload, Workload::Insert | Workload::Delete)
            || operations <= delta_capacity.min(entries),
        "insert/delete throughput uses one distinct key per operation; increase capacity or lower operations"
    );
    assert!(
        !matches!(workload, Workload::ReadUntouchedAfterUpdate) || working < entries,
        "untouched-read workload requires delta capacity below base entries"
    );

    let base_keys = Arc::new((0..entries).map(mixed_key).collect::<Vec<_>>());
    let delta_keys = Arc::new(
        (0..delta_capacity)
            .map(|index| mixed_key(entries.saturating_add(index)))
            .collect::<Vec<_>>(),
    );

    match backend.as_str() {
        "segment" => run_segment(
            workload,
            entries,
            operations,
            threads,
            delta_capacity,
            working,
            &base_keys,
            &delta_keys,
            target_occupied_slots,
            negative_filter_bits,
        ),
        "online" => run_online(
            workload,
            entries,
            operations,
            threads,
            delta_capacity,
            working,
            &base_keys,
            &delta_keys,
            true,
            target_occupied_slots,
            negative_filter_bits,
        ),
        "online-unbatched" => run_online(
            workload,
            entries,
            operations,
            threads,
            delta_capacity,
            working,
            &base_keys,
            &delta_keys,
            false,
            target_occupied_slots,
            negative_filter_bits,
        ),
        "direct" => run_direct(
            workload,
            entries,
            operations,
            threads,
            delta_capacity,
            working,
            &base_keys,
            &delta_keys,
        ),
        "papaya" => run_papaya(
            workload,
            entries,
            operations,
            threads,
            delta_capacity,
            working,
            &base_keys,
            &delta_keys,
        ),
        _ => panic!("backend must be segment, online, online-unbatched, direct, or papaya"),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_online(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    delta_capacity: usize,
    working: usize,
    base_keys: &Arc<Vec<Box<[u8]>>>,
    delta_keys: &Arc<Vec<Box<[u8]>>>,
    batched: bool,
    target_occupied_slots: u8,
    negative_filter_bits: u8,
) {
    let region = Region::new(GLOBAL);
    let cache = Arc::new(
        OnlineMutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1))
                .with_target_bucket_occupancy(target_occupied_slots)
                .with_negative_filter_bits_per_entry(negative_filter_bits),
            base_keys
                .iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index), Some(TTL))),
            delta_capacity,
        )
        .unwrap(),
    );
    let build_bytes = live_bytes(region.change());
    prepare_online(&cache, workload, working, base_keys, delta_keys);
    let prepared_stats = region.change();
    let prepared_bytes = live_bytes(prepared_stats);

    let (elapsed, checksum) = run_parallel(operations, threads, |begin, end| {
        if batched {
            online_batched_worker(
                &cache, workload, begin, end, entries, working, base_keys, delta_keys,
            )
        } else {
            online_unbatched_worker(
                &cache, workload, begin, end, entries, working, base_keys, delta_keys,
            )
        }
    });
    let post_stats = region.change();
    let post_bytes = live_bytes(post_stats);
    black_box((checksum, &cache));
    let backend = match (batched, target_occupied_slots, negative_filter_bits) {
        (true, 7, 0) => "online-batch4096".to_owned(),
        (false, 7, 0) => "online-unbatched".to_owned(),
        (true, occupied, 0) => format!("online-batch4096-occ{occupied}"),
        (false, occupied, 0) => format!("online-unbatched-occ{occupied}"),
        (true, occupied, bits) => format!("online-batch4096-occ{occupied}-nf{bits}"),
        (false, occupied, bits) => format!("online-unbatched-occ{occupied}-nf{bits}"),
    };
    print_result(
        &backend,
        workload,
        entries,
        operations,
        threads,
        build_bytes,
        prepared_bytes,
        post_bytes,
        operation_stats(prepared_stats, post_stats),
        elapsed,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_segment(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    delta_capacity: usize,
    working: usize,
    base_keys: &Arc<Vec<Box<[u8]>>>,
    delta_keys: &Arc<Vec<Box<[u8]>>>,
    target_occupied_slots: u8,
    negative_filter_bits: u8,
) {
    let region = Region::new(GLOBAL);
    let cache = Arc::new(
        MutableSegmentCache::try_from_entries(
            SegmentCacheConfig::with_relative_expiry(Duration::from_secs(1))
                .with_target_bucket_occupancy(target_occupied_slots)
                .with_negative_filter_bits_per_entry(negative_filter_bits),
            base_keys
                .iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index), Some(TTL))),
            delta_capacity,
        )
        .unwrap(),
    );
    let build_bytes = live_bytes(region.change());
    prepare_segment(&cache, workload, working, base_keys, delta_keys);
    let prepared_stats = region.change();
    let prepared_bytes = live_bytes(prepared_stats);

    let (elapsed, checksum) = run_parallel(operations, threads, |begin, end| {
        segment_worker(
            &cache, workload, begin, end, entries, working, base_keys, delta_keys,
        )
    });
    let post_stats = region.change();
    let post_bytes = live_bytes(post_stats);
    black_box((checksum, &cache));
    let backend = if target_occupied_slots == 7 && negative_filter_bits == 0 {
        "segment".to_owned()
    } else if negative_filter_bits == 0 {
        format!("segment-occ{target_occupied_slots}")
    } else {
        format!("segment-occ{target_occupied_slots}-nf{negative_filter_bits}")
    };
    print_result(
        &backend,
        workload,
        entries,
        operations,
        threads,
        build_bytes,
        prepared_bytes,
        post_bytes,
        operation_stats(prepared_stats, post_stats),
        elapsed,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_direct(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    delta_capacity: usize,
    working: usize,
    base_keys: &Arc<Vec<Box<[u8]>>>,
    delta_keys: &Arc<Vec<Box<[u8]>>>,
) {
    let region = Region::new(GLOBAL);
    let cache = Arc::new(
        DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(u64::MAX).with_overlay_capacity(delta_capacity),
            base_keys
                .iter()
                .enumerate()
                .map(|(index, key)| (key.as_ref(), value(index), charge(key), Some(TTL))),
        )
        .unwrap(),
    );
    let build_bytes = live_bytes(region.change());
    prepare_direct(&cache, workload, working, base_keys, delta_keys);
    let prepared_stats = region.change();
    let prepared_bytes = live_bytes(prepared_stats);

    let (elapsed, checksum) = run_parallel(operations, threads, |begin, end| {
        direct_worker(
            &cache, workload, begin, end, entries, working, base_keys, delta_keys,
        )
    });
    let post_stats = region.change();
    let post_bytes = live_bytes(post_stats);
    black_box((checksum, &cache));
    print_result(
        "direct",
        workload,
        entries,
        operations,
        threads,
        build_bytes,
        prepared_bytes,
        post_bytes,
        operation_stats(prepared_stats, post_stats),
        elapsed,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_papaya(
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    delta_capacity: usize,
    working: usize,
    base_keys: &Arc<Vec<Box<[u8]>>>,
    delta_keys: &Arc<Vec<Box<[u8]>>>,
) {
    let region = Region::new(GLOBAL);
    let cache = Arc::new(PapayaCache::with_capacity_and_hasher(
        entries.saturating_add(delta_capacity),
        DefaultHashBuilder::default(),
    ));
    {
        let guard = cache.pin();
        for (index, key) in base_keys.iter().enumerate() {
            guard.insert(key.clone(), papaya_value(index, key));
        }
    }
    let build_bytes = live_bytes(region.change());
    prepare_papaya(&cache, workload, working, base_keys, delta_keys);
    let prepared_stats = region.change();
    let prepared_bytes = live_bytes(prepared_stats);

    let (elapsed, checksum) = run_parallel(operations, threads, |begin, end| {
        papaya_worker(
            &cache, workload, begin, end, entries, working, base_keys, delta_keys,
        )
    });
    let post_stats = region.change();
    let post_bytes = live_bytes(post_stats);
    black_box((checksum, &cache));
    print_result(
        "papaya-inline-control",
        workload,
        entries,
        operations,
        threads,
        build_bytes,
        prepared_bytes,
        post_bytes,
        operation_stats(prepared_stats, post_stats),
        elapsed,
    );
}

#[allow(clippy::too_many_arguments)]
fn segment_worker(
    cache: &MutableSegmentCache,
    workload: Workload,
    begin: usize,
    end: usize,
    entries: usize,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) -> u64 {
    match workload {
        Workload::Update => (begin..end).fold(0_u64, |checksum, operation| {
            let key = &base_keys[mixed_index(operation, working)];
            checksum.wrapping_add(u64::from(
                cache.insert(key, value(operation), Some(TTL)).is_ok(),
            ))
        }),
        Workload::Insert => (begin..end).fold(0_u64, |checksum, operation| {
            checksum.wrapping_add(u64::from(
                cache
                    .insert(&delta_keys[operation], value(operation), Some(TTL))
                    .is_ok(),
            ))
        }),
        Workload::Delete => (begin..end).fold(0_u64, |checksum, operation| {
            checksum.wrapping_add(u64::from(cache.remove(&base_keys[operation])))
        }),
        Workload::Read95Update5 | Workload::Read80Update20 => {
            let mut checksum = 0_u64;
            for chunk in (begin..end).step_by(4_096) {
                let guard = cache.pin();
                for operation in chunk..end.min(chunk + 4_096) {
                    let key = &base_keys[mixed_index(operation, working)];
                    if is_read(workload, operation) {
                        checksum = checksum
                            .wrapping_add(guard.peek(key).map_or(0, |value| u64::from(value[0])));
                    } else {
                        checksum = checksum.wrapping_add(u64::from(
                            cache.insert(key, value(operation), Some(TTL)).is_ok(),
                        ));
                    }
                }
            }
            checksum
        }
        _ => {
            let guard = cache.pin();
            (begin..end).fold(0_u64, |checksum, operation| {
                let result = match workload {
                    Workload::ReadPristine => {
                        let key = &base_keys[mixed_index(operation, entries)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadMiss => {
                        let key = &delta_keys[mixed_index(operation, delta_keys.len())];
                        u64::from(guard.peek(key).is_none())
                    }
                    Workload::ReadUpdated => {
                        let key = &base_keys[mixed_index(operation, working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadNew => {
                        let key = &delta_keys[mixed_index(operation, working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadDeleted => {
                        let key = &base_keys[mixed_index(operation, working)];
                        u64::from(guard.peek(key).is_none())
                    }
                    Workload::ReadUntouchedAfterUpdate => {
                        let key = &base_keys[working + mixed_index(operation, entries - working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    _ => unreachable!("write workloads use their dedicated path"),
                };
                checksum.wrapping_add(result)
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn online_unbatched_worker(
    cache: &OnlineMutableSegmentCache,
    workload: Workload,
    begin: usize,
    end: usize,
    entries: usize,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) -> u64 {
    match workload {
        Workload::Update => (begin..end).fold(0_u64, |checksum, operation| {
            let key = &base_keys[mixed_index(operation, working)];
            checksum.wrapping_add(u64::from(
                cache.insert(key, value(operation), Some(TTL)).is_ok(),
            ))
        }),
        Workload::Insert => (begin..end).fold(0_u64, |checksum, operation| {
            checksum.wrapping_add(u64::from(
                cache
                    .insert(&delta_keys[operation], value(operation), Some(TTL))
                    .is_ok(),
            ))
        }),
        Workload::Delete => (begin..end).fold(0_u64, |checksum, operation| {
            checksum.wrapping_add(u64::from(cache.remove(&base_keys[operation])))
        }),
        Workload::Read95Update5 | Workload::Read80Update20 => {
            let mut checksum = 0_u64;
            for chunk in (begin..end).step_by(4_096) {
                let guard = cache.pin();
                for operation in chunk..end.min(chunk + 4_096) {
                    let key = &base_keys[mixed_index(operation, working)];
                    if is_read(workload, operation) {
                        checksum = checksum
                            .wrapping_add(guard.peek(key).map_or(0, |value| u64::from(value[0])));
                    } else {
                        checksum = checksum.wrapping_add(u64::from(
                            cache.insert(key, value(operation), Some(TTL)).is_ok(),
                        ));
                    }
                }
            }
            checksum
        }
        _ => {
            let guard = cache.pin();
            (begin..end).fold(0_u64, |checksum, operation| {
                let result = match workload {
                    Workload::ReadPristine => {
                        let key = &base_keys[mixed_index(operation, entries)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadMiss => {
                        let key = &delta_keys[mixed_index(operation, delta_keys.len())];
                        u64::from(guard.peek(key).is_none())
                    }
                    Workload::ReadUpdated => {
                        let key = &base_keys[mixed_index(operation, working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadNew => {
                        let key = &delta_keys[mixed_index(operation, working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadDeleted => {
                        let key = &base_keys[mixed_index(operation, working)];
                        u64::from(guard.peek(key).is_none())
                    }
                    Workload::ReadUntouchedAfterUpdate => {
                        let key = &base_keys[working + mixed_index(operation, entries - working)];
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    }
                    _ => unreachable!("write workloads use their dedicated path"),
                };
                checksum.wrapping_add(result)
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn online_batched_worker(
    cache: &OnlineMutableSegmentCache,
    workload: Workload,
    begin: usize,
    end: usize,
    entries: usize,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) -> u64 {
    if matches!(
        workload,
        Workload::Update | Workload::Insert | Workload::Delete
    ) {
        let mut checksum = 0_u64;
        for chunk in (begin..end).step_by(4_096) {
            let guard = cache.pin_writer_batch();
            for operation in chunk..end.min(chunk + 4_096) {
                let result = match workload {
                    Workload::Update => {
                        let key = &base_keys[mixed_index(operation, working)];
                        u64::from(guard.insert(key, value(operation), Some(TTL)).is_ok())
                    }
                    Workload::Insert => u64::from(
                        guard
                            .insert(&delta_keys[operation], value(operation), Some(TTL))
                            .is_ok(),
                    ),
                    Workload::Delete => u64::from(guard.remove(&base_keys[operation])),
                    _ => unreachable!(),
                };
                checksum = checksum.wrapping_add(result);
            }
        }
        return checksum;
    }
    if !matches!(workload, Workload::Read95Update5 | Workload::Read80Update20) {
        return online_unbatched_worker(
            cache, workload, begin, end, entries, working, base_keys, delta_keys,
        );
    }
    let mut checksum = 0_u64;
    for chunk in (begin..end).step_by(4_096) {
        let guard = cache.pin();
        for operation in chunk..end.min(chunk + 4_096) {
            let result = match workload {
                Workload::Update => {
                    let key = &base_keys[mixed_index(operation, working)];
                    u64::from(guard.insert(key, value(operation), Some(TTL)).is_ok())
                }
                Workload::Insert => u64::from(
                    guard
                        .insert(&delta_keys[operation], value(operation), Some(TTL))
                        .is_ok(),
                ),
                Workload::Delete => u64::from(guard.remove(&base_keys[operation])),
                Workload::Read95Update5 | Workload::Read80Update20 => {
                    let key = &base_keys[mixed_index(operation, working)];
                    if is_read(workload, operation) {
                        guard.peek(key).map_or(0, |value| u64::from(value[0]))
                    } else {
                        u64::from(guard.insert(key, value(operation), Some(TTL)).is_ok())
                    }
                }
                Workload::ReadPristine => {
                    let key = &base_keys[mixed_index(operation, entries)];
                    guard.peek(key).map_or(0, |value| u64::from(value[0]))
                }
                Workload::ReadMiss => {
                    let key = &delta_keys[mixed_index(operation, delta_keys.len())];
                    u64::from(guard.peek(key).is_none())
                }
                Workload::ReadUpdated => {
                    let key = &base_keys[mixed_index(operation, working)];
                    guard.peek(key).map_or(0, |value| u64::from(value[0]))
                }
                Workload::ReadNew => {
                    let key = &delta_keys[mixed_index(operation, working)];
                    guard.peek(key).map_or(0, |value| u64::from(value[0]))
                }
                Workload::ReadDeleted => {
                    let key = &base_keys[mixed_index(operation, working)];
                    u64::from(guard.peek(key).is_none())
                }
                Workload::ReadUntouchedAfterUpdate => {
                    let key = &base_keys[working + mixed_index(operation, entries - working)];
                    guard.peek(key).map_or(0, |value| u64::from(value[0]))
                }
            };
            checksum = checksum.wrapping_add(result);
        }
    }
    checksum
}

#[allow(clippy::too_many_arguments)]
fn direct_worker(
    cache: &DirectPackedCache<[u8; VALUE_BYTES]>,
    workload: Workload,
    begin: usize,
    end: usize,
    entries: usize,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) -> u64 {
    match workload {
        Workload::Update => (begin..end).fold(0_u64, |checksum, operation| {
            let key = &base_keys[mixed_index(operation, working)];
            checksum.wrapping_add(u64::from(
                cache
                    .insert_discard_with_options(key, value(operation), charge(key), Some(TTL))
                    .is_ok(),
            ))
        }),
        Workload::Insert => (begin..end).fold(0_u64, |checksum, operation| {
            let key = &delta_keys[operation];
            checksum.wrapping_add(u64::from(
                cache
                    .insert_discard_with_options(key, value(operation), charge(key), Some(TTL))
                    .is_ok(),
            ))
        }),
        Workload::Delete => (begin..end).fold(0_u64, |checksum, operation| {
            checksum.wrapping_add(u64::from(cache.remove_discard(&base_keys[operation])))
        }),
        Workload::Read95Update5 | Workload::Read80Update20 => {
            let mut checksum = 0_u64;
            for chunk in (begin..end).step_by(4_096) {
                let guard = cache.pin();
                for operation in chunk..end.min(chunk + 4_096) {
                    let key = &base_keys[mixed_index(operation, working)];
                    if is_read(workload, operation) {
                        checksum = checksum.wrapping_add(
                            guard
                                .get_untracked(key)
                                .map_or(0, |value| u64::from(value[0])),
                        );
                    } else {
                        checksum = checksum.wrapping_add(u64::from(
                            cache
                                .insert_discard_with_options(
                                    key,
                                    value(operation),
                                    charge(key),
                                    Some(TTL),
                                )
                                .is_ok(),
                        ));
                    }
                }
            }
            checksum
        }
        _ => {
            let guard = cache.pin();
            (begin..end).fold(0_u64, |checksum, operation| {
                let result = match workload {
                    Workload::ReadPristine => {
                        let key = &base_keys[mixed_index(operation, entries)];
                        guard
                            .get_untracked(key)
                            .map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadMiss => {
                        let key = &delta_keys[mixed_index(operation, delta_keys.len())];
                        u64::from(guard.get_untracked(key).is_none())
                    }
                    Workload::ReadUpdated => {
                        let key = &base_keys[mixed_index(operation, working)];
                        guard
                            .get_untracked(key)
                            .map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadNew => {
                        let key = &delta_keys[mixed_index(operation, working)];
                        guard
                            .get_untracked(key)
                            .map_or(0, |value| u64::from(value[0]))
                    }
                    Workload::ReadDeleted => {
                        let key = &base_keys[mixed_index(operation, working)];
                        u64::from(guard.get_untracked(key).is_none())
                    }
                    Workload::ReadUntouchedAfterUpdate => {
                        let key = &base_keys[working + mixed_index(operation, entries - working)];
                        guard
                            .get_untracked(key)
                            .map_or(0, |value| u64::from(value[0]))
                    }
                    _ => unreachable!("write workloads use their dedicated path"),
                };
                checksum.wrapping_add(result)
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn papaya_worker(
    cache: &PapayaCache,
    workload: Workload,
    begin: usize,
    end: usize,
    entries: usize,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) -> u64 {
    match workload {
        Workload::Update | Workload::Insert | Workload::Delete => {
            let mut checksum = 0_u64;
            for chunk in (begin..end).step_by(4_096) {
                let guard = cache.pin();
                for operation in chunk..end.min(chunk + 4_096) {
                    checksum = checksum.wrapping_add(match workload {
                        Workload::Update => {
                            let key = &base_keys[mixed_index(operation, working)];
                            u64::from(
                                guard
                                    .update(key.clone(), |_| papaya_value(operation, key))
                                    .is_some(),
                            )
                        }
                        Workload::Insert => {
                            let key = &delta_keys[operation];
                            u64::from(
                                guard
                                    .insert(key.clone(), papaya_value(operation, key))
                                    .is_none(),
                            )
                        }
                        Workload::Delete => {
                            u64::from(guard.remove(&base_keys[operation]).is_some())
                        }
                        _ => unreachable!(),
                    });
                }
            }
            checksum
        }
        Workload::Read95Update5 | Workload::Read80Update20 => {
            let mut checksum = 0_u64;
            for chunk in (begin..end).step_by(4_096) {
                let guard = cache.pin();
                for operation in chunk..end.min(chunk + 4_096) {
                    let key = &base_keys[mixed_index(operation, working)];
                    if is_read(workload, operation) {
                        checksum =
                            checksum.wrapping_add(guard.get(key).map_or(0, papaya_first_byte));
                    } else {
                        checksum = checksum.wrapping_add(u64::from(
                            guard
                                .update(key.clone(), |_| papaya_value(operation, key))
                                .is_some(),
                        ));
                    }
                }
            }
            checksum
        }
        _ => {
            let guard = cache.pin();
            (begin..end).fold(0_u64, |checksum, operation| {
                let result = match workload {
                    Workload::ReadPristine => {
                        let key = &base_keys[mixed_index(operation, entries)];
                        guard.get(key).map_or(0, papaya_first_byte)
                    }
                    Workload::ReadMiss => {
                        let key = &delta_keys[mixed_index(operation, delta_keys.len())];
                        u64::from(guard.get(key).is_none())
                    }
                    Workload::ReadUpdated => {
                        let key = &base_keys[mixed_index(operation, working)];
                        guard.get(key).map_or(0, papaya_first_byte)
                    }
                    Workload::ReadNew => {
                        let key = &delta_keys[mixed_index(operation, working)];
                        guard.get(key).map_or(0, papaya_first_byte)
                    }
                    Workload::ReadDeleted => {
                        let key = &base_keys[mixed_index(operation, working)];
                        u64::from(guard.get(key).is_none())
                    }
                    Workload::ReadUntouchedAfterUpdate => {
                        let key = &base_keys[working + mixed_index(operation, entries - working)];
                        guard.get(key).map_or(0, papaya_first_byte)
                    }
                    _ => unreachable!("write workloads use their dedicated path"),
                };
                checksum.wrapping_add(result)
            })
        }
    }
}

fn prepare_segment(
    cache: &MutableSegmentCache,
    workload: Workload,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) {
    if workload.prepares_updates() {
        for (index, key) in base_keys.iter().take(working).enumerate() {
            cache.insert(key, value(index + 1), Some(TTL)).unwrap();
        }
    } else if workload.prepares_new() {
        for (index, key) in delta_keys.iter().take(working).enumerate() {
            cache.insert(key, value(index), Some(TTL)).unwrap();
        }
    } else if workload.prepares_deletes() {
        for key in base_keys.iter().take(working) {
            black_box(cache.remove(key));
        }
    }
}

fn prepare_online(
    cache: &OnlineMutableSegmentCache,
    workload: Workload,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) {
    if workload.prepares_updates() {
        for (index, key) in base_keys.iter().take(working).enumerate() {
            cache.insert(key, value(index + 1), Some(TTL)).unwrap();
        }
    } else if workload.prepares_new() {
        for (index, key) in delta_keys.iter().take(working).enumerate() {
            cache.insert(key, value(index), Some(TTL)).unwrap();
        }
    } else if workload.prepares_deletes() {
        for key in base_keys.iter().take(working) {
            black_box(cache.remove(key));
        }
    }
}

fn prepare_direct(
    cache: &DirectPackedCache<[u8; VALUE_BYTES]>,
    workload: Workload,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) {
    if workload.prepares_updates() {
        for (index, key) in base_keys.iter().take(working).enumerate() {
            cache
                .insert_discard_with_options(key, value(index + 1), charge(key), Some(TTL))
                .unwrap();
        }
    } else if workload.prepares_new() {
        for (index, key) in delta_keys.iter().take(working).enumerate() {
            cache
                .insert_discard_with_options(key, value(index), charge(key), Some(TTL))
                .unwrap();
        }
    } else if workload.prepares_deletes() {
        for key in base_keys.iter().take(working) {
            black_box(cache.remove_discard(key));
        }
    }
}

fn prepare_papaya(
    cache: &PapayaCache,
    workload: Workload,
    working: usize,
    base_keys: &[Box<[u8]>],
    delta_keys: &[Box<[u8]>],
) {
    let guard = cache.pin();
    if workload.prepares_updates() {
        for (index, key) in base_keys.iter().take(working).enumerate() {
            guard.update(key.clone(), |_| papaya_value(index + 1, key));
        }
    } else if workload.prepares_new() {
        for (index, key) in delta_keys.iter().take(working).enumerate() {
            guard.insert(key.clone(), papaya_value(index, key));
        }
    } else if workload.prepares_deletes() {
        for key in base_keys.iter().take(working) {
            guard.remove(key);
        }
    }
}

fn run_parallel(
    operations: usize,
    threads: usize,
    work: impl Fn(usize, usize) -> u64 + Sync,
) -> (Duration, u64) {
    let ready = Arc::new(Barrier::new(threads + 1));
    let start = Arc::new(Barrier::new(threads + 1));
    let done = Arc::new(Barrier::new(threads + 1));
    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for worker in 0..threads {
            let begin = operations * worker / threads;
            let end = operations * (worker + 1) / threads;
            let ready = Arc::clone(&ready);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let work = &work;
            workers.push(scope.spawn(move || {
                ready.wait();
                start.wait();
                let checksum = work(begin, end);
                done.wait();
                checksum
            }));
        }
        ready.wait();
        let started = Instant::now();
        start.wait();
        done.wait();
        let elapsed = started.elapsed();
        let checksum = workers
            .into_iter()
            .fold(0_u64, |checksum, worker| checksum ^ worker.join().unwrap());
        (elapsed, checksum)
    })
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    backend: &str,
    workload: Workload,
    entries: usize,
    operations: usize,
    threads: usize,
    build_bytes: usize,
    prepared_bytes: usize,
    post_bytes: usize,
    operation_stats: Stats,
    elapsed: Duration,
) {
    let seconds = elapsed.as_secs_f64();
    println!(
        "backend,workload,entries,operations,threads,build_bytes,build_bpe,prepared_bytes,prepared_bpe,post_bytes,post_bpe,mops_per_second,ns_per_operation,operation_allocations,operation_deallocations,operation_bytes_allocated,operation_bytes_deallocated,operation_net_live_bytes"
    );
    println!(
        "{backend},{},{entries},{operations},{threads},{build_bytes},{:.3},{prepared_bytes},{:.3},{post_bytes},{:.3},{:.3},{:.3},{},{},{},{},{}",
        workload.name(),
        build_bytes as f64 / entries as f64,
        prepared_bytes as f64 / entries as f64,
        post_bytes as f64 / entries as f64,
        operations as f64 / seconds / 1_000_000.0,
        seconds * 1_000_000_000.0 / operations as f64,
        operation_stats.allocations,
        operation_stats.deallocations,
        operation_stats.bytes_allocated,
        operation_stats.bytes_deallocated,
        live_bytes(operation_stats),
    );
}

fn is_read(workload: Workload, operation: usize) -> bool {
    let roll = mix(operation as u64 ^ 0xa5a5_5a5a) % 100;
    match workload {
        Workload::Read95Update5 => roll < 95,
        Workload::Read80Update20 => roll < 80,
        _ => unreachable!("read ratio is defined only for mixed workloads"),
    }
}

fn papaya_value(index: usize, key: &[u8]) -> PapayaValue {
    PapayaValue {
        value: value(index),
        weight: u32::try_from(charge(key)).unwrap_or(u32::MAX),
        expires_at: AtomicU32::new(u32::MAX),
        accessed: AtomicBool::new(false),
    }
}

fn papaya_first_byte(value: &PapayaValue) -> u64 {
    black_box(value.weight);
    black_box(value.expires_at.load(Ordering::Relaxed));
    black_box(value.accessed.load(Ordering::Relaxed));
    u64::from(value.value[0])
}

fn value(index: usize) -> [u8; VALUE_BYTES] {
    [u8::try_from(index & 255).unwrap(); VALUE_BYTES]
}

fn charge(key: &[u8]) -> u64 {
    u64::try_from(VALUE_BYTES + key.len()).unwrap()
}

fn mixed_index(operation: usize, len: usize) -> usize {
    let len = u64::try_from(len).expect("key count fits u64");
    usize::try_from(mix(operation as u64) % len).expect("mixed index is in range")
}

fn mixed_key(index: usize) -> Box<[u8]> {
    let mut key = [0_u8; 48];
    let mut state = u64::try_from(index).expect("entry index fits u64");
    for chunk in key.chunks_exact_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key[..mixed_key_bytes(index)].into()
}

const fn mixed_key_bytes(index: usize) -> usize {
    [8, 16, 24, 32, 48][index % 5]
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn live_bytes(stats: stats_alloc::Stats) -> usize {
    stats
        .bytes_allocated
        .saturating_sub(stats.bytes_deallocated)
}

fn operation_stats(before: Stats, after: Stats) -> Stats {
    Stats {
        allocations: after.allocations.saturating_sub(before.allocations),
        deallocations: after.deallocations.saturating_sub(before.deallocations),
        reallocations: after.reallocations.saturating_sub(before.reallocations),
        bytes_allocated: after.bytes_allocated.saturating_sub(before.bytes_allocated),
        bytes_deallocated: after
            .bytes_deallocated
            .saturating_sub(before.bytes_deallocated),
        bytes_reallocated: after
            .bytes_reallocated
            .saturating_sub(before.bytes_reallocated),
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |value| value.parse().expect("expected an integer"))
}
