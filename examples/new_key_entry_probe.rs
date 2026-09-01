//! New-key insertion comparison for `PackedGen`, `DashMap`, and `Papaya` entry APIs.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry as DashEntry;
use hashbrown::DefaultHashBuilder;
use packedgen::{
    AtomicEntry, AtomicGenerationBaseFilter, AtomicGenerationOverlay, GenerationHashBuilder,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

type Dash = DashMap<Box<[u8]>, u64, DefaultHashBuilder>;
type Papaya = papaya::HashMap<Box<[u8]>, u64, DefaultHashBuilder>;

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 100_000).max(1);
    let operations = argument(&mut arguments, 300_000).max(1);
    let maximum_threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 11).max(3);
    let shards = argument(&mut arguments, 64).max(2).next_power_of_two();
    let keys = (0..entries as u64).map(binary_key).collect::<Vec<_>>();
    let new_keys = (entries as u64..entries.saturating_add(operations) as u64)
        .map(binary_key)
        .collect::<Vec<_>>();
    let thread_counts = if maximum_threads == 1 {
        vec![1]
    } else {
        vec![1, maximum_threads]
    };

    eprintln!("Papaya reuses one guard per worker; the pure-insert trace retires no records");
    println!("implementation,entries,operations,threads,shards,samples,median_ns,throughput_mops");
    for threads in thread_counts {
        let implementations = Implementation::ALL;
        let mut measurements = implementations.map(|_| Vec::with_capacity(samples));
        for sample in 0..samples {
            for offset in 0..implementations.len() {
                let implementation_index = (sample + offset) % implementations.len();
                measurements[implementation_index].push(measure(
                    implementations[implementation_index],
                    operations,
                    threads,
                    shards,
                    &keys,
                    &new_keys,
                ));
            }
        }
        for (implementation, measurements) in implementations.into_iter().zip(&mut measurements) {
            measurements.sort_unstable();
            let median = measurements[measurements.len() / 2];
            let throughput = operations as f64 / median.as_secs_f64() / 1_000_000.0;
            println!(
                "{},{entries},{operations},{threads},{shards},{},{},{throughput:.3}",
                implementation.name(),
                measurements.len(),
                median.as_nanos(),
            );
        }
    }
}

fn measure(
    implementation: Implementation,
    operations: usize,
    threads: usize,
    shards: usize,
    keys: &[[u8; 32]],
    new_keys: &[[u8; 32]],
) -> Duration {
    let map = BenchMap::build(implementation, operations, shards, keys);
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    let elapsed = std::thread::scope(|scope| {
        for thread in 0..threads {
            let map = &map;
            let start = &start;
            let done = &done;
            let begin = operations * thread / threads;
            let end = operations * (thread + 1) / threads;
            scope.spawn(move || {
                start.wait();
                map.run_range(implementation, begin, end, new_keys);
                done.wait();
            });
        }
        start.wait();
        let started = Instant::now();
        done.wait();
        started.elapsed()
    });
    black_box(map);
    elapsed
}

enum BenchMap {
    Atomic(LockFreeAtomicU64GenerationMap),
    Dash(Dash),
    Papaya(Box<Papaya>),
}

impl BenchMap {
    fn build(
        implementation: Implementation,
        new_key_capacity: usize,
        shards: usize,
        keys: &[[u8; 32]],
    ) -> Self {
        if let Some(filter) = implementation.atomic_filter() {
            Self::Atomic(
                LockFreeAtomicU64GenerationMap::try_from_entries_with_options_and_writer_hash(
                    keys.iter().enumerate().map(|(index, key)| {
                        (
                            key,
                            NonMaxU64::new(index as u64).expect("benchmark index is representable"),
                        )
                    }),
                    new_key_capacity,
                    AtomicGenerationOverlay::AtomicFixed32,
                    filter,
                    GenerationHashBuilder::default(),
                )
                .unwrap(),
            )
        } else if implementation.is_dash() {
            let map = DashMap::with_capacity_and_hasher_and_shard_amount(
                keys.len().saturating_add(new_key_capacity),
                DefaultHashBuilder::default(),
                shards,
            );
            for (index, key) in keys.iter().enumerate() {
                map.insert(key.as_slice().into(), index as u64);
            }
            Self::Dash(map)
        } else {
            debug_assert!(implementation.is_papaya());
            let map = Papaya::with_capacity_and_hasher(
                keys.len().saturating_add(new_key_capacity),
                DefaultHashBuilder::default(),
            );
            let guard = map.guard();
            for (index, key) in keys.iter().enumerate() {
                map.insert(key.as_slice().into(), index as u64, &guard);
            }
            drop(guard);
            Self::Papaya(Box::new(map))
        }
    }

    fn run_range(
        &self,
        implementation: Implementation,
        begin: usize,
        end: usize,
        keys: &[[u8; 32]],
    ) {
        #[cfg(feature = "prepared-batch-gate")]
        if implementation.is_atomic_guard()
            && let Self::Atomic(map) = self
        {
            let guard = map.operation_guard();
            for (offset, key) in keys[begin..end].iter().enumerate() {
                let value = u64::try_from(begin + offset).expect("operation index fits u64");
                black_box(guard.get_or_insert(key, non_max(value)));
            }
            return;
        }

        if let Self::Papaya(map) = self {
            let guard = map.guard();
            for (offset, key) in keys[begin..end].iter().enumerate() {
                let value = u64::try_from(begin + offset).expect("operation index fits u64");
                let inserted = match implementation {
                    Implementation::PapayaInsert => {
                        map.insert(key.as_slice().into(), value, &guard).is_none()
                    }
                    Implementation::PapayaGetInsert => {
                        map.get(key.as_slice(), &guard).is_none()
                            && map.insert(key.as_slice().into(), value, &guard).is_none()
                    }
                    Implementation::PapayaGetOrInsert => {
                        black_box(map.get_or_insert(key.as_slice().into(), value, &guard));
                        true
                    }
                    _ => unreachable!("Papaya map requires a Papaya implementation"),
                };
                black_box(inserted);
            }
            return;
        }

        for (offset, key) in keys[begin..end].iter().enumerate() {
            let value = u64::try_from(begin + offset).expect("operation index fits u64");
            black_box(self.insert_new(implementation, key, value));
        }
    }

    fn insert_new(&self, implementation: Implementation, key: &[u8], value: u64) -> bool {
        match (self, implementation) {
            (
                Self::Atomic(map),
                Implementation::AtomicInsertExact
                | Implementation::AtomicInsertEmbedded
                | Implementation::AtomicInsertOneByte,
            ) => map.insert_new(key, non_max(value)),
            (
                Self::Atomic(map),
                Implementation::AtomicGetInsertExact | Implementation::AtomicGetInsertEmbedded,
            ) => map.get(key).is_none() && map.insert_new(key, non_max(value)),
            (
                Self::Atomic(map),
                Implementation::AtomicEntryExact
                | Implementation::AtomicEntryEmbedded
                | Implementation::AtomicEntryOneByte,
            ) => match map.entry(key) {
                AtomicEntry::Occupied(_) => false,
                AtomicEntry::Vacant(vacant) => vacant.insert_new(non_max(value)),
            },
            (
                Self::Atomic(map),
                Implementation::AtomicGetOrInsertExact
                | Implementation::AtomicGetOrInsertEmbedded
                | Implementation::AtomicGetOrInsertOneByte,
            ) => {
                black_box(map.get_or_insert(key, non_max(value)));
                true
            }
            (Self::Dash(map), Implementation::DashInsert) => {
                map.insert(key.into(), value).is_none()
            }
            (Self::Dash(map), Implementation::DashGetInsert) => {
                map.get(key).is_none() && map.insert(key.into(), value).is_none()
            }
            (Self::Dash(map), Implementation::DashEntry) => match map.entry(key.into()) {
                DashEntry::Occupied(_) => false,
                DashEntry::Vacant(vacant) => {
                    vacant.insert(value);
                    true
                }
            },
            (Self::Papaya(_), _) => unreachable!("Papaya operations require a worker guard"),
            _ => unreachable!("benchmark implementation and map must match"),
        }
    }
}

#[derive(Clone, Copy)]
enum Implementation {
    AtomicInsertExact,
    AtomicInsertEmbedded,
    AtomicInsertOneByte,
    AtomicGetInsertExact,
    AtomicGetInsertEmbedded,
    AtomicEntryExact,
    AtomicEntryEmbedded,
    AtomicEntryOneByte,
    AtomicGetOrInsertExact,
    AtomicGetOrInsertEmbedded,
    AtomicGetOrInsertOneByte,
    #[cfg(feature = "prepared-batch-gate")]
    AtomicGuardGetOrInsertExact,
    #[cfg(feature = "prepared-batch-gate")]
    AtomicGuardGetOrInsertEmbedded,
    #[cfg(feature = "prepared-batch-gate")]
    AtomicGuardGetOrInsertOneByte,
    DashInsert,
    DashGetInsert,
    DashEntry,
    PapayaInsert,
    PapayaGetInsert,
    PapayaGetOrInsert,
}

impl Implementation {
    #[cfg(not(feature = "prepared-batch-gate"))]
    const ALL: [Self; 17] = [
        Self::AtomicInsertExact,
        Self::AtomicInsertEmbedded,
        Self::AtomicInsertOneByte,
        Self::AtomicGetInsertExact,
        Self::AtomicGetInsertEmbedded,
        Self::AtomicEntryExact,
        Self::AtomicEntryEmbedded,
        Self::AtomicEntryOneByte,
        Self::AtomicGetOrInsertExact,
        Self::AtomicGetOrInsertEmbedded,
        Self::AtomicGetOrInsertOneByte,
        Self::DashInsert,
        Self::DashGetInsert,
        Self::DashEntry,
        Self::PapayaInsert,
        Self::PapayaGetInsert,
        Self::PapayaGetOrInsert,
    ];

    #[cfg(feature = "prepared-batch-gate")]
    const ALL: [Self; 20] = [
        Self::AtomicInsertExact,
        Self::AtomicInsertEmbedded,
        Self::AtomicInsertOneByte,
        Self::AtomicGetInsertExact,
        Self::AtomicGetInsertEmbedded,
        Self::AtomicEntryExact,
        Self::AtomicEntryEmbedded,
        Self::AtomicEntryOneByte,
        Self::AtomicGetOrInsertExact,
        Self::AtomicGetOrInsertEmbedded,
        Self::AtomicGetOrInsertOneByte,
        Self::AtomicGuardGetOrInsertExact,
        Self::AtomicGuardGetOrInsertEmbedded,
        Self::AtomicGuardGetOrInsertOneByte,
        Self::DashInsert,
        Self::DashGetInsert,
        Self::DashEntry,
        Self::PapayaInsert,
        Self::PapayaGetInsert,
        Self::PapayaGetOrInsert,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AtomicInsertExact => "atomic-insert-new-exact",
            Self::AtomicInsertEmbedded => "atomic-insert-new-embedded",
            Self::AtomicInsertOneByte => "atomic-insert-new-one-byte",
            Self::AtomicGetInsertExact => "atomic-get-insert-new-exact",
            Self::AtomicGetInsertEmbedded => "atomic-get-insert-new-embedded",
            Self::AtomicEntryExact => "atomic-entry-insert-new-exact",
            Self::AtomicEntryEmbedded => "atomic-entry-insert-new-embedded",
            Self::AtomicEntryOneByte => "atomic-entry-insert-new-one-byte",
            Self::AtomicGetOrInsertExact => "atomic-get-or-insert-exact",
            Self::AtomicGetOrInsertEmbedded => "atomic-get-or-insert-embedded",
            Self::AtomicGetOrInsertOneByte => "atomic-get-or-insert-one-byte",
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertExact => "atomic-guard-get-or-insert-exact",
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertEmbedded => "atomic-guard-get-or-insert-embedded",
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertOneByte => "atomic-guard-get-or-insert-one-byte",
            Self::DashInsert => "dashmap-insert",
            Self::DashGetInsert => "dashmap-get-then-insert",
            Self::DashEntry => "dashmap-entry",
            Self::PapayaInsert => "papaya-insert",
            Self::PapayaGetInsert => "papaya-get-then-insert",
            Self::PapayaGetOrInsert => "papaya-get-or-insert",
        }
    }

    const fn atomic_filter(self) -> Option<AtomicGenerationBaseFilter> {
        match self {
            Self::AtomicInsertExact
            | Self::AtomicGetInsertExact
            | Self::AtomicEntryExact
            | Self::AtomicGetOrInsertExact => Some(AtomicGenerationBaseFilter::Disabled),
            Self::AtomicInsertEmbedded
            | Self::AtomicGetInsertEmbedded
            | Self::AtomicEntryEmbedded
            | Self::AtomicGetOrInsertEmbedded => {
                Some(AtomicGenerationBaseFilter::EmbeddedFingerprint)
            }
            Self::AtomicInsertOneByte
            | Self::AtomicEntryOneByte
            | Self::AtomicGetOrInsertOneByte => Some(AtomicGenerationBaseFilter::OneBytePerEntry),
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertExact => Some(AtomicGenerationBaseFilter::Disabled),
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertEmbedded => {
                Some(AtomicGenerationBaseFilter::EmbeddedFingerprint)
            }
            #[cfg(feature = "prepared-batch-gate")]
            Self::AtomicGuardGetOrInsertOneByte => {
                Some(AtomicGenerationBaseFilter::OneBytePerEntry)
            }
            Self::DashInsert
            | Self::DashGetInsert
            | Self::DashEntry
            | Self::PapayaInsert
            | Self::PapayaGetInsert
            | Self::PapayaGetOrInsert => None,
        }
    }

    const fn is_dash(self) -> bool {
        matches!(
            self,
            Self::DashInsert | Self::DashGetInsert | Self::DashEntry
        )
    }

    const fn is_papaya(self) -> bool {
        matches!(
            self,
            Self::PapayaInsert | Self::PapayaGetInsert | Self::PapayaGetOrInsert
        )
    }

    #[cfg(feature = "prepared-batch-gate")]
    const fn is_atomic_guard(self) -> bool {
        matches!(
            self,
            Self::AtomicGuardGetOrInsertExact
                | Self::AtomicGuardGetOrInsertEmbedded
                | Self::AtomicGuardGetOrInsertOneByte
        )
    }
}

fn non_max(value: u64) -> NonMaxU64 {
    NonMaxU64::new(value).expect("benchmark value is representable")
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments
        .next()
        .map_or(default, |argument| argument.parse().unwrap())
}

fn binary_key(value: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    key[..8].copy_from_slice(&value.to_le_bytes());
    key[8..16].copy_from_slice(&value.rotate_left(17).to_le_bytes());
    key[16..24].copy_from_slice(&value.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    key[24..].copy_from_slice(&(value ^ 0xa5a5_5a5a_d3c3_b4b4).to_le_bytes());
    key
}
