//! Throughput of `ShardedCache` and `quick_cache` with `u64` keys and values,
//! each built with its defaults, where fixed per-operation costs dominate.
//!
//! Usage: `small_entry_probe BACKEND WORKLOAD ENTRIES OPS WORKERS SEED
//! [ZIPF_EXPONENT]`. BACKEND is `sharded` or `quickcache`. WORKLOAD is:
//!
//! - `zipf`: look-aside reads of Zipf-distributed keys over ten times the
//!   capacity, inserting each missed key; `ZIPF_EXPONENT` defaults to 1.0. A
//!   warm-up of four times the capacity in operations runs untimed first.
//! - `zipf-read`: as `zipf` through its warm-up, then reads alone.
//! - `churn`: inserts of distinct new keys into a full cache.
//! - `hit`: reads of resident keys in a cache with room to spare.
//! - `upsert`: inserts of resident keys into an exactly full cache.
//!
//! Each worker's keys are drawn before timing starts. The row reports
//! million operations per second and the hit ratio of the timed reads.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use packedgen::ShardedCache;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Zipf,
    ZipfRead,
    Churn,
    Hit,
    Upsert,
}

trait Cache: Sync {
    fn get(&self, key: u64) -> Option<u64>;
    fn insert(&self, key: u64, value: u64);
}

impl Cache for ShardedCache<u64, u64> {
    #[inline]
    fn get(&self, key: u64) -> Option<u64> {
        ShardedCache::get(self, &key)
    }

    #[inline]
    fn insert(&self, key: u64, value: u64) {
        ShardedCache::insert(self, key, value);
    }
}

impl Cache for quick_cache::sync::Cache<u64, u64> {
    #[inline]
    fn get(&self, key: u64) -> Option<u64> {
        quick_cache::sync::Cache::get(self, &key)
    }

    #[inline]
    fn insert(&self, key: u64, value: u64) {
        quick_cache::sync::Cache::insert(self, key, value);
    }
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn value_of(key: u64) -> u64 {
    key ^ 0x5555_5555_5555_5555
}

/// Draws Zipf-distributed ranks over `keys` by inverting the CDF.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(keys: usize, exponent: f64) -> Self {
        let mut total = 0.0;
        let mut cdf = Vec::with_capacity(keys);
        for rank in 1..=keys {
            total += 1.0 / (rank as f64).powf(exponent);
            cdf.push(total);
        }
        for weight in &mut cdf {
            *weight /= total;
        }
        Self { cdf }
    }

    fn rank(&self, random: u64) -> usize {
        let unit = (random >> 11) as f64 / (1_u64 << 53) as f64;
        self.cdf.partition_point(|&weight| weight < unit)
    }
}

struct Config {
    workload: Workload,
    entries: usize,
    operations: usize,
    workers: usize,
    seed: u64,
    exponent: f64,
}

/// The keys of one worker's timed run, after its untimed warm-up keys.
fn trace(config: &Config, worker: usize, zipf: Option<&Zipf>) -> (Vec<u64>, Vec<u64>) {
    let per_worker = config.operations / config.workers;
    let stream = config.seed ^ mix(worker as u64 + 1);
    match config.workload {
        Workload::Zipf | Workload::ZipfRead => {
            let zipf = zipf.expect("zipf workloads draw from a distribution");
            let warmup = 4 * config.entries / config.workers;
            let mut keys = (0..warmup + per_worker).map(|index| {
                let rank = zipf.rank(mix(stream ^ index as u64));
                mix(rank as u64 ^ config.seed)
            });
            let warm = keys.by_ref().take(warmup).collect();
            (warm, keys.collect())
        }
        Workload::Churn => {
            let first = config.entries + worker * per_worker;
            let keys = (first..first + per_worker)
                .map(|id| mix(id as u64))
                .collect();
            (Vec::new(), keys)
        }
        Workload::Hit | Workload::Upsert => {
            let keys = (0..per_worker)
                .map(|index| mix(mix(stream ^ index as u64) % config.entries as u64))
                .collect();
            (Vec::new(), keys)
        }
    }
}

fn run<C: Cache>(cache: &C, config: &Config) -> (Duration, u64, u64) {
    for id in 0..config.entries {
        if !matches!(config.workload, Workload::Zipf | Workload::ZipfRead) {
            let key = mix(id as u64);
            cache.insert(key, value_of(key));
        }
    }
    let zipf = matches!(config.workload, Workload::Zipf | Workload::ZipfRead)
        .then(|| Zipf::new(10 * config.entries, config.exponent));
    let traces: Vec<_> = (0..config.workers)
        .map(|worker| trace(config, worker, zipf.as_ref()))
        .collect();
    let warmed = Barrier::new(config.workers + 1);
    let start = Barrier::new(config.workers + 1);
    thread::scope(|scope| {
        let workers: Vec<_> = traces
            .iter()
            .map(|(warm, timed)| {
                let (warmed, start) = (&warmed, &start);
                scope.spawn(move || {
                    for &key in warm {
                        if cache.get(key).is_none() {
                            cache.insert(key, value_of(key));
                        }
                    }
                    warmed.wait();
                    start.wait();
                    let mut hits = 0_u64;
                    let mut reads = 0_u64;
                    for &key in timed {
                        match config.workload {
                            Workload::Zipf | Workload::Hit => {
                                reads += 1;
                                match cache.get(key) {
                                    Some(value) => {
                                        assert_eq!(value, value_of(key));
                                        hits += 1;
                                    }
                                    None => cache.insert(key, value_of(key)),
                                }
                            }
                            Workload::ZipfRead => {
                                reads += 1;
                                if let Some(value) = cache.get(key) {
                                    assert_eq!(value, value_of(key));
                                    hits += 1;
                                }
                            }
                            Workload::Churn | Workload::Upsert => {
                                cache.insert(key, value_of(key));
                            }
                        }
                    }
                    black_box((hits, reads))
                })
            })
            .collect();
        warmed.wait();
        let started = Instant::now();
        start.wait();
        let (hits, reads) = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker completes"))
            .fold((0, 0), |(hits, reads), (h, r)| (hits + h, reads + r));
        (started.elapsed(), hits, reads)
    })
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        arguments.len() >= 6,
        "usage: small_entry_probe BACKEND WORKLOAD ENTRIES OPS WORKERS SEED [ZIPF_EXPONENT]"
    );
    let backend = arguments[0].as_str();
    let workload = match arguments[1].as_str() {
        "zipf" => Workload::Zipf,
        "zipf-read" => Workload::ZipfRead,
        "churn" => Workload::Churn,
        "hit" => Workload::Hit,
        "upsert" => Workload::Upsert,
        other => panic!("unknown workload: {other}"),
    };
    let config = Config {
        workload,
        entries: arguments[2].parse().expect("ENTRIES is a count"),
        operations: arguments[3].parse().expect("OPS is a count"),
        workers: arguments[4].parse().expect("WORKERS is a count"),
        seed: arguments[5].parse().expect("SEED is a u64"),
        exponent: arguments
            .get(6)
            .map_or(1.0, |e| e.parse().expect("ZIPF_EXPONENT is a float")),
    };
    // Hits run with room to spare; every other workload fills the cache.
    let capacity = if workload == Workload::Hit {
        2 * config.entries
    } else {
        config.entries
    };
    let (elapsed, hits, reads) = match backend {
        "sharded" => run(&ShardedCache::<u64, u64>::new(capacity), &config),
        "quickcache" => run(
            &quick_cache::sync::Cache::<u64, u64>::new(capacity),
            &config,
        ),
        other => panic!("unknown backend: {other}"),
    };
    let done = (config.operations / config.workers * config.workers) as f64;
    let hit_ratio = if reads == 0 {
        0.0
    } else {
        hits as f64 / reads as f64
    };
    println!(
        "{backend},{},{},{},{},{:.2},{:.4}",
        arguments[1],
        config.entries,
        config.operations,
        config.workers,
        done / elapsed.as_secs_f64() / 1e6,
        hit_ratio
    );
}
