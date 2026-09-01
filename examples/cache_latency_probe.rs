//! Read tail latency while concurrent writers mutate the cache.

#![allow(
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    missing_docs
)]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::DefaultHashBuilder;
use packedgen::{CacheConfig, DirectPackedCache};
use papaya::HashMap as PapayaHashMap;

const VALUE_BYTES: usize = 64;
const REFRESH_INTERVAL: usize = 16_384;
const NEW_KEY_OFFSET: usize = 1_000_000_000;

struct Corpus {
    hits: Vec<Box<[u8]>>,
    misses: Vec<Box<[u8]>>,
}

struct ReaderResult {
    latencies_ns: Vec<u64>,
    hits: u64,
    checksum: u64,
}

#[derive(Debug)]
struct Measurement {
    read_mops: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
    read_hit_pct: f64,
    final_entries: usize,
    rebuilds: u64,
}

trait Backend: Send + Sync + 'static {
    const NAME: &'static str;

    fn read_range(&self, corpus: &Corpus, begin: usize, end: usize) -> ReaderResult;
    fn write_range(&self, corpus: &Corpus, begin: usize, end: usize);
    fn maintain(&self) -> bool;
    fn len(&self) -> usize;
}

struct DirectBackend(DirectPackedCache<[u8; VALUE_BYTES]>);

struct PapayaControlValue {
    value: [u8; VALUE_BYTES],
    weight: u32,
    expires_at: AtomicU32,
    accessed: AtomicBool,
}

type PapayaCache = PapayaHashMap<Box<[u8]>, PapayaControlValue, DefaultHashBuilder>;

struct PapayaBackend(PapayaCache);

fn main() {
    let mut arguments = std::env::args().skip(1);
    let entries = argument(&mut arguments, 200_000).max(1);
    let read_operations = argument(&mut arguments, 1_000_000).max(1);
    let write_operations = argument(&mut arguments, 200_000).max(1);
    let reader_threads = argument(&mut arguments, 8).max(1);
    let writer_threads = argument(&mut arguments, 2).max(1);
    let samples = argument(&mut arguments, 5).max(3);

    let corpus = Arc::new(Corpus {
        hits: (0..entries).map(mixed_binary_key).collect(),
        misses: (0..entries)
            .map(|index| mixed_binary_key(NEW_KEY_OFFSET / 2 + index))
            .collect(),
    });
    let new_entries = (0..write_operations)
        .filter(|operation| mutation_kind(*operation) >= 90)
        .count();
    let final_capacity = entries + new_entries;
    let mut direct = Vec::with_capacity(samples);
    let mut papaya = Vec::with_capacity(samples);

    for sample in 0..samples {
        if sample.is_multiple_of(2) {
            direct.push(run_once(
                &Arc::new(build_direct(&corpus, final_capacity, new_entries)),
                &corpus,
                read_operations,
                write_operations,
                reader_threads,
                writer_threads,
            ));
            papaya.push(run_once(
                &Arc::new(build_papaya(&corpus, final_capacity)),
                &corpus,
                read_operations,
                write_operations,
                reader_threads,
                writer_threads,
            ));
        } else {
            papaya.push(run_once(
                &Arc::new(build_papaya(&corpus, final_capacity)),
                &corpus,
                read_operations,
                write_operations,
                reader_threads,
                writer_threads,
            ));
            direct.push(run_once(
                &Arc::new(build_direct(&corpus, final_capacity, new_entries)),
                &corpus,
                read_operations,
                write_operations,
                reader_threads,
                writer_threads,
            ));
        }
    }

    println!(
        "strategy,entries,read_operations,write_operations,readers,writers,samples,read_mops,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,read_hit_pct,final_entries,median_rebuilds"
    );
    print_summary::<DirectBackend>(
        entries,
        read_operations,
        write_operations,
        reader_threads,
        writer_threads,
        &mut direct,
    );
    print_summary::<PapayaBackend>(
        entries,
        read_operations,
        write_operations,
        reader_threads,
        writer_threads,
        &mut papaya,
    );
}

fn build_direct(corpus: &Corpus, capacity: usize, new_entries: usize) -> DirectBackend {
    let records = corpus
        .hits
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), value(index), charge(key), None::<Duration>));
    DirectBackend(
        DirectPackedCache::try_from_entries_with_options(
            CacheConfig::new(u64::MAX)
                .with_max_entries(capacity)
                .with_overlay_capacity(new_entries.max(1)),
            records,
        )
        .unwrap(),
    )
}

fn build_papaya(corpus: &Corpus, capacity: usize) -> PapayaBackend {
    let cache = PapayaCache::with_capacity_and_hasher(capacity, DefaultHashBuilder::default());
    let guard = cache.pin();
    for (index, key) in corpus.hits.iter().enumerate() {
        guard.insert(key.clone(), PapayaControlValue::new(index, key));
    }
    drop(guard);
    PapayaBackend(cache)
}

fn run_once<B: Backend>(
    backend: &Arc<B>,
    corpus: &Arc<Corpus>,
    read_operations: usize,
    write_operations: usize,
    reader_threads: usize,
    writer_threads: usize,
) -> Measurement {
    let barrier = Arc::new(Barrier::new(reader_threads + writer_threads + 2));
    let stop = Arc::new(AtomicBool::new(false));
    let rebuilds = Arc::new(AtomicU64::new(0));

    let (mut latencies, hits, elapsed) = thread::scope(|scope| {
        let mut readers = Vec::with_capacity(reader_threads);
        for reader in 0..reader_threads {
            let begin = read_operations * reader / reader_threads;
            let end = read_operations * (reader + 1) / reader_threads;
            let backend = Arc::clone(backend);
            let corpus = Arc::clone(corpus);
            let barrier = Arc::clone(&barrier);
            readers.push(scope.spawn(move || {
                barrier.wait();
                backend.read_range(&corpus, begin, end)
            }));
        }

        let mut writers = Vec::with_capacity(writer_threads);
        for writer in 0..writer_threads {
            let begin = write_operations * writer / writer_threads;
            let end = write_operations * (writer + 1) / writer_threads;
            let backend = Arc::clone(backend);
            let corpus = Arc::clone(corpus);
            let barrier = Arc::clone(&barrier);
            writers.push(scope.spawn(move || {
                barrier.wait();
                backend.write_range(&corpus, begin, end);
            }));
        }

        let maintenance = {
            let backend = Arc::clone(backend);
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let rebuilds = Arc::clone(&rebuilds);
            scope.spawn(move || {
                barrier.wait();
                while !stop.load(Ordering::Acquire) {
                    if backend.maintain() {
                        rebuilds.fetch_add(1, Ordering::Relaxed);
                    }
                    thread::park_timeout(Duration::from_millis(1));
                }
            })
        };

        barrier.wait();
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(read_operations);
        let mut hits = 0_u64;
        let mut checksum = 0_u64;
        for reader in readers {
            let result = reader.join().unwrap();
            latencies.extend(result.latencies_ns);
            hits += result.hits;
            checksum ^= result.checksum;
        }
        let elapsed = started.elapsed();
        black_box(checksum);
        for writer in writers {
            writer.join().unwrap();
        }
        stop.store(true, Ordering::Release);
        maintenance.thread().unpark();
        maintenance.join().unwrap();
        (latencies, hits, elapsed)
    });

    latencies.sort_unstable();
    let final_entries = backend.len();
    let expected_entries = corpus.hits.len()
        + (0..write_operations)
            .filter(|operation| mutation_kind(*operation) >= 90)
            .count();
    assert_eq!(final_entries, expected_entries);
    Measurement {
        read_mops: read_operations as f64 / elapsed.as_secs_f64() / 1e6,
        p50_ns: percentile(&latencies, 500),
        p95_ns: percentile(&latencies, 950),
        p99_ns: percentile(&latencies, 990),
        p999_ns: percentile(&latencies, 999),
        max_ns: *latencies.last().unwrap(),
        read_hit_pct: hits as f64 / read_operations as f64 * 100.0,
        final_entries,
        rebuilds: rebuilds.load(Ordering::Relaxed),
    }
}

impl Backend for DirectBackend {
    const NAME: &'static str = "direct-packed-cache";

    fn read_range(&self, corpus: &Corpus, begin: usize, end: usize) -> ReaderResult {
        let mut guard = self.0.pin();
        let mut latencies_ns = Vec::with_capacity(end - begin);
        let mut hits = 0_u64;
        let mut checksum = 0_u64;
        for operation in begin..end {
            if operation != begin && operation.is_multiple_of(REFRESH_INTERVAL) {
                guard.refresh();
            }
            let key = read_key(corpus, operation);
            let started = Instant::now();
            let found = guard.get(key);
            latencies_ns.push(elapsed_ns(started));
            if let Some(value) = found {
                hits += 1;
                checksum ^= u64::from(value[0]);
            }
        }
        ReaderResult {
            latencies_ns,
            hits,
            checksum,
        }
    }

    fn write_range(&self, corpus: &Corpus, begin: usize, end: usize) {
        let mut guard = self.0.pin();
        for operation in begin..end {
            if operation != begin && operation.is_multiple_of(REFRESH_INTERVAL) {
                guard.refresh();
            }
            let kind = mutation_kind(operation);
            if kind < 80 {
                let key = write_hit_key(corpus, operation);
                self.0
                    .insert_discard_with_options(key, value(operation), charge(key), None)
                    .unwrap();
            } else if kind < 90 {
                let key = write_hit_key(corpus, operation);
                self.0.remove_discard(key);
                self.0
                    .insert_discard_with_options(key, value(operation), charge(key), None)
                    .unwrap();
            } else {
                let key = mixed_binary_key(NEW_KEY_OFFSET + operation);
                self.0
                    .insert_discard_with_options(&key, value(operation), charge(&key), None)
                    .unwrap();
            }
        }
    }

    fn maintain(&self) -> bool {
        self.0.maintain().unwrap().rebuilt
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl Backend for PapayaBackend {
    const NAME: &'static str = "papaya-inline-compact-control";

    fn read_range(&self, corpus: &Corpus, begin: usize, end: usize) -> ReaderResult {
        let mut latencies_ns = Vec::with_capacity(end - begin);
        let mut hits = 0_u64;
        let mut checksum = 0_u64;
        for block_begin in (begin..end).step_by(REFRESH_INTERVAL) {
            let block_end = (block_begin + REFRESH_INTERVAL).min(end);
            let guard = self.0.pin();
            for operation in block_begin..block_end {
                let key = read_key(corpus, operation);
                let started = Instant::now();
                let found = guard.get(key);
                latencies_ns.push(elapsed_ns(started));
                if let Some(entry) = found {
                    if operation.is_multiple_of(16) {
                        entry.accessed.store(true, Ordering::Relaxed);
                    }
                    black_box(entry.expires_at.load(Ordering::Relaxed));
                    black_box(entry.weight);
                    hits += 1;
                    checksum ^= u64::from(entry.value[0]);
                }
            }
        }
        ReaderResult {
            latencies_ns,
            hits,
            checksum,
        }
    }

    fn write_range(&self, corpus: &Corpus, begin: usize, end: usize) {
        for block_begin in (begin..end).step_by(REFRESH_INTERVAL) {
            let block_end = (block_begin + REFRESH_INTERVAL).min(end);
            let guard = self.0.pin();
            for operation in block_begin..block_end {
                let kind = mutation_kind(operation);
                if kind < 80 {
                    let key = write_hit_key(corpus, operation);
                    guard.update(key.into(), |_| PapayaControlValue::new(operation, key));
                } else if kind < 90 {
                    let key = write_hit_key(corpus, operation);
                    guard.remove(key);
                    guard.insert(key.into(), PapayaControlValue::new(operation, key));
                } else {
                    let key = mixed_binary_key(NEW_KEY_OFFSET + operation);
                    let entry = PapayaControlValue::new(operation, &key);
                    guard.insert(key, entry);
                }
            }
        }
    }

    fn maintain(&self) -> bool {
        false
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl PapayaControlValue {
    fn new(index: usize, key: &[u8]) -> Self {
        Self {
            value: value(index),
            weight: u32::try_from(charge(key)).unwrap(),
            expires_at: AtomicU32::new(u32::MAX),
            accessed: AtomicBool::new(false),
        }
    }
}

fn print_summary<B: Backend>(
    entries: usize,
    read_operations: usize,
    write_operations: usize,
    readers: usize,
    writers: usize,
    measurements: &mut [Measurement],
) {
    if std::env::var_os("PACKED_CACHE_RAW").is_some() {
        eprintln!("raw,{},{measurements:?}", B::NAME);
    }
    let samples = measurements.len();
    println!(
        "{},{entries},{read_operations},{write_operations},{readers},{writers},{samples},{:.3},{},{},{},{},{},{:.3},{},{}",
        B::NAME,
        median_by(measurements, |measurement| measurement.read_mops),
        median_by(measurements, |measurement| measurement.p50_ns),
        median_by(measurements, |measurement| measurement.p95_ns),
        median_by(measurements, |measurement| measurement.p99_ns),
        median_by(measurements, |measurement| measurement.p999_ns),
        median_by(measurements, |measurement| measurement.max_ns),
        median_by(measurements, |measurement| measurement.read_hit_pct),
        median_by(measurements, |measurement| measurement.final_entries),
        median_by(measurements, |measurement| measurement.rebuilds),
    );
}

fn median_by<T: Copy + PartialOrd>(
    measurements: &[Measurement],
    value: impl Fn(&Measurement) -> T,
) -> T {
    let mut values = measurements.iter().map(value).collect::<Vec<_>>();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap());
    values[values.len() / 2]
}

fn percentile(sorted: &[u64], per_thousand: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(per_thousand)
        .div_ceil(1_000)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn read_key(corpus: &Corpus, operation: usize) -> &[u8] {
    if operation.is_multiple_of(20) {
        &corpus.misses[mixed_index(operation, corpus.misses.len())]
    } else {
        &corpus.hits[mixed_index(operation, corpus.hits.len())]
    }
}

fn write_hit_key(corpus: &Corpus, operation: usize) -> &[u8] {
    &corpus.hits[mixed_index(operation ^ 0x5a5a_3c3c, corpus.hits.len())]
}

fn mutation_kind(operation: usize) -> u64 {
    mix(operation as u64 ^ 0xa5a5_5a5a_1234_5678) % 100
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
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

fn mixed_index(operation: usize, len: usize) -> usize {
    let len = u64::try_from(len).expect("benchmark key count fits u64");
    usize::try_from(mix(operation as u64) % len).expect("index fits usize")
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
