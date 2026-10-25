//! Every `ShardedCache` operation from eight threads at once: writes,
//! batches, replacements, removals, the three loaders, placeholder guards,
//! pinned and cloned reads, iteration, `retain`, partial drains, clears,
//! resizes, and `maintain`. Each value names its key, and each read checks
//! it; afterwards the entry and weight bounds hold, the live values are the
//! resident ones, and a clear leaves none.
//!
//! It runs for `PACKEDGEN_EVERY_OPERATION_SECONDS` per seed, one second by
//! default. Keep a single test in this binary: it counts every live value.

#![allow(missing_docs)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use packedgen::{GuardResult, RemovalCause, ShardedCacheBuilder};

static LIVE: AtomicU64 = AtomicU64::new(0);

/// A value that names its key and counts itself among the live values.
struct Tracked {
    key: u64,
    id: u64,
}

impl Tracked {
    fn new(key: u64, id: u64) -> Self {
        LIVE.fetch_add(1, Ordering::Relaxed);
        Self { key, id }
    }
}

impl Clone for Tracked {
    fn clone(&self) -> Self {
        Self::new(self.key, self.id)
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park_timeout(Duration::from_millis(1));
    }
}

const KEYS: u64 = 3_000;

#[allow(clippy::too_many_lines, reason = "one match arm per operation")]
fn run(seed: u64, duration: Duration) {
    let misplaced = Arc::new(AtomicBool::new(false));
    let cache = {
        let misplaced = Arc::clone(&misplaced);
        ShardedCacheBuilder::new(512)
            .max_capacity(2_048)
            .shards(8)
            .weighter(|_key: &u64, value: &Tracked| 1 + value.id % 3)
            .max_weight(1_500)
            .removal_listener(move |key: &u64, value: &Tracked, _cause: RemovalCause| {
                if *key != value.key {
                    misplaced.store(true, Ordering::Relaxed);
                }
            })
            .build::<u64, Tracked>()
    };
    let next_id = AtomicU64::new(1);
    let until = Instant::now() + duration;
    thread::scope(|scope| {
        for worker in 0..8_u64 {
            let (cache, next_id, misplaced) = (&cache, &next_id, &misplaced);
            scope.spawn(move || {
                let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9) ^ ((worker + 1) * 0x1234_5677));
                let check = |key: u64, value: &Tracked| {
                    if value.key != key {
                        misplaced.store(true, Ordering::Relaxed);
                    }
                };
                while Instant::now() < until {
                    for _ in 0..64 {
                        let key = rng.below(KEYS);
                        let id = next_id.fetch_add(1, Ordering::Relaxed);
                        match rng.below(100) {
                            0..=19 => {
                                cache.insert(key, Tracked::new(key, id));
                            }
                            20..=22 => {
                                let ttl = Duration::from_millis(rng.below(3));
                                cache.insert_with_ttl(key, Tracked::new(key, id), ttl);
                            }
                            23..=25 => {
                                cache.insert_many((0..4).map(|step| {
                                    let key = (key + step * 7) % KEYS;
                                    (key, Tracked::new(key, id))
                                }));
                            }
                            26..=30 => {
                                let _ = cache.replace(key, Tracked::new(key, id));
                            }
                            31..=36 => {
                                cache.remove(&key);
                            }
                            37..=39 => {
                                cache.remove_if(&key, |value| value.id % 2 == 0);
                            }
                            40..=59 => {
                                if let Some(value) = cache.get(&key) {
                                    check(key, &value);
                                }
                            }
                            60..=64 => {
                                if let Some(value) = cache.peek(&key) {
                                    check(key, &value);
                                }
                                let _ = cache.contains_key(&key);
                            }
                            65..=72 => {
                                check(
                                    key,
                                    &cache.get_or_insert_with(&key, || Tracked::new(key, id)),
                                );
                            }
                            73..=74 => {
                                let _ = cache.get_or_try_insert_with(&key, || {
                                    if id % 2 == 0 {
                                        Ok(Tracked::new(key, id))
                                    } else {
                                        Err(())
                                    }
                                });
                            }
                            75..=79 => {
                                match cache
                                    .get_value_or_guard(&key, Some(Duration::from_micros(200)))
                                {
                                    GuardResult::Value(value) => check(key, &value),
                                    GuardResult::Guard(guard) => {
                                        if id % 3 != 0 {
                                            guard.insert(Tracked::new(key, id));
                                        }
                                    }
                                    GuardResult::Timeout => {}
                                }
                            }
                            80..=83 => {
                                let load = cache
                                    .get_or_insert_async(&key, async { Tracked::new(key, id) });
                                check(key, &block_on(load));
                            }
                            84..=85 => match block_on(cache.get_value_or_guard_async(&key)) {
                                Ok(value) => check(key, &value),
                                Err(guard) => {
                                    guard.insert(Tracked::new(key, id));
                                }
                            },
                            86..=89 => {
                                let guard = cache.pin();
                                if let Some(value) = guard.get(&key) {
                                    check(key, value);
                                }
                                for (key, value) in guard.iter().take(8) {
                                    check(*key, value);
                                }
                            }
                            90..=92 => {
                                for (key, value) in cache.iter().take(16) {
                                    check(key, &value);
                                }
                            }
                            93 => cache.retain(|key, value| {
                                check(*key, value);
                                value.id % 5 != 0
                            }),
                            94 => {
                                for (key, value) in cache.drain().take(3) {
                                    check(key, &value);
                                }
                            }
                            95 => {
                                if rng.below(20) == 0 {
                                    cache.clear();
                                }
                            }
                            96 => {
                                if rng.below(10) == 0 {
                                    let capacity = 64 + rng.below(1_984);
                                    cache.set_capacity(usize::try_from(capacity).expect("fits"));
                                }
                            }
                            97 => {
                                if rng.below(10) == 0 {
                                    cache.set_max_weight(200 + rng.below(3_000));
                                }
                            }
                            _ => cache.maintain(),
                        }
                    }
                }
            });
        }
    });
    assert!(
        !misplaced.load(Ordering::Relaxed),
        "a value under another key (seed {seed})"
    );
    assert!(cache.len() <= cache.capacity());
    assert!(cache.weight() <= cache.max_weight());
    cache.maintain();
    let resident = u64::try_from(cache.len()).expect("fits");
    assert_eq!(
        LIVE.load(Ordering::Relaxed),
        resident,
        "live values beyond the resident ones (seed {seed})"
    );
    cache.clear();
    cache.maintain();
    assert_eq!(
        LIVE.load(Ordering::Relaxed),
        0,
        "values outlived a clear (seed {seed})"
    );
}

#[test]
#[cfg_attr(miri, ignore = "eight threads for a second each seed")]
fn every_operation_from_eight_threads_keeps_values_whole_and_dropped_once() {
    let seconds = std::env::var("PACKEDGEN_EVERY_OPERATION_SECONDS")
        .ok()
        .and_then(|seconds| seconds.parse().ok())
        .unwrap_or(1);
    for seed in 1..=2 {
        run(seed, Duration::from_secs(seconds));
    }
}
