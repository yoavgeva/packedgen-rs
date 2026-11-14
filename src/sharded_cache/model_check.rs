//! Random operation sequences checked against an exact model of the cache.
//!
//! The removal listener says which entries left and why, so a model that
//! applies each operation's intent and then the removals heard knows exactly
//! what the cache holds: every read, return value, length, weight, and
//! statistic is checked against it after every operation, and every shard's
//! table against its entries.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use parking_lot::Mutex;

use super::{
    GuardResult, MAX_WEIGHT, OVERFLOW_MAX, RELOCATIONS, RemovalCause, RemovalListener, SLOTS,
    ShardedCache, ShardedCacheBuilder, WINDOW_EVICTIONS, Weighter, entry, overflow, tag,
};

/// Deadlines in the model are an hour from a write or a read.
const HOUR: Duration = Duration::from_secs(3_600);
/// The model's clock moves in jumps of 25 minutes, so it is never within
/// ten minutes of a deadline an hour from a whole jump, far past what the
/// real clock adds in a run.
const JUMP_MINUTES: u64 = 25;
const HOUR_MINUTES: u64 = 60;

thread_local! {
    /// `Versioned` values alive on this thread: built or cloned, not dropped.
    static VERSIONED_ALIVE: Cell<i64> = const { Cell::new(0) };
}

/// A value whose version no other write reuses, with its weight. Counts its
/// instances, so a value the cache leaks or drops twice shows.
#[derive(Debug)]
struct Versioned {
    version: u64,
    weight: u64,
}

impl Versioned {
    fn new(version: u64, weight: u64) -> Self {
        VERSIONED_ALIVE.set(VERSIONED_ALIVE.get() + 1);
        Self { version, weight }
    }
}

impl Clone for Versioned {
    fn clone(&self) -> Self {
        Self::new(self.version, self.weight)
    }
}

impl Drop for Versioned {
    fn drop(&mut self) {
        VERSIONED_ALIVE.set(VERSIONED_ALIVE.get() - 1);
    }
}

/// xorshift64*, seeded per run.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn pick<T: Copy>(&mut self, choices: &[T]) -> T {
        choices[usize::try_from(self.below(choices.len() as u64)).expect("an index fits")]
    }
}

/// How the cache under test expires entries written without their own time
/// to live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lifetime {
    Forever,
    ToLive,
    ToIdle,
}

/// A time to live a write asks for.
#[derive(Clone, Copy, Debug)]
enum Ttl {
    /// The cache's own lifetime.
    Default,
    Hour,
    /// Never readable.
    Zero,
}

#[derive(Clone, Copy, Debug)]
struct Modeled {
    version: u64,
    weight: u64,
    /// Written with a zero time to live: never readable.
    stillborn: bool,
    /// The model minute from which it reads as expired, if it expires.
    deadline: Option<u64>,
}

/// What a set of runs exercised, so a test can tell it reached each path.
#[derive(Debug, Default)]
struct Exercised {
    evictions: u64,
    expirations: u64,
    refusals: u64,
    loads_cached: u64,
    replacements: u64,
    resizes: u64,
    pair_evictions: u64,
    relocations: u64,
}

struct Model {
    entries: HashMap<u64, Modeled>,
    /// Minutes the cache's clock was moved.
    now: u64,
    lifetime: Lifetime,
    next_version: u64,
    evictions: u64,
    expirations: u64,
    hits: u64,
    misses: u64,
}

impl Model {
    fn expired(&self, modeled: &Modeled) -> bool {
        modeled.stillborn
            || modeled
                .deadline
                .is_some_and(|deadline| deadline <= self.now)
    }

    fn live(&self, key: u64) -> Option<Modeled> {
        self.entries
            .get(&key)
            .copied()
            .filter(|modeled| !self.expired(modeled))
    }

    /// A marking read of `key`: counted, and under a time to idle, a live
    /// entry's deadline moves to an hour from now if that is later.
    fn read(&mut self, key: u64) -> Option<Modeled> {
        let live = self.live(key);
        if live.is_some() {
            self.hits += 1;
            if self.lifetime == Lifetime::ToIdle {
                let idle = self.now + HOUR_MINUTES;
                let deadline = &mut self.entries.get_mut(&key).expect("live").deadline;
                *deadline = deadline.map(|deadline| deadline.max(idle));
            }
        } else {
            self.misses += 1;
        }
        live
    }

    fn version(&mut self) -> u64 {
        self.next_version += 1;
        self.next_version
    }

    fn written(&self, version: u64, weight: u64, ttl: Ttl) -> Modeled {
        Modeled {
            version,
            weight,
            stillborn: matches!(ttl, Ttl::Zero),
            deadline: match (ttl, self.lifetime) {
                (Ttl::Default, Lifetime::Forever) => None,
                _ => Some(self.now + HOUR_MINUTES),
            },
        }
    }

    /// Takes one removal the listener heard out of the model, checking that
    /// the model held that version and that the cause fits it. `explicit_expired`
    /// allows an explicit removal of an expired entry, as `clear` reports.
    fn remove_heard(
        &mut self,
        (key, version, cause): (u64, u64, RemovalCause),
        explicit_expired: bool,
    ) {
        let modeled = self
            .entries
            .remove(&key)
            .unwrap_or_else(|| panic!("heard {key} v{version} leave ({cause:?}), not modeled"));
        assert_eq!(
            modeled.version, version,
            "heard {key} v{version} leave ({cause:?}), modeled v{}",
            modeled.version
        );
        let expired = self.expired(&modeled);
        match cause {
            RemovalCause::Size => {
                assert!(!expired, "{key} v{version} evicted for size, but expired");
                self.evictions += 1;
            }
            RemovalCause::Replaced => {
                assert!(!expired, "{key} v{version} replaced, but expired");
            }
            RemovalCause::Expired => {
                assert!(expired, "{key} v{version} expired early");
                self.expirations += 1;
            }
            RemovalCause::Explicit => {
                assert!(
                    !expired || explicit_expired,
                    "{key} v{version} removed explicitly, but expired"
                );
            }
        }
    }
}

type Heard = Arc<Mutex<Vec<(u64, u64, RemovalCause)>>>;

struct Listener(Heard);

impl RemovalListener<u64, Versioned> for Listener {
    fn on_removal(&self, key: &u64, value: &Versioned, cause: RemovalCause) {
        self.0.lock().push((*key, value.version, cause));
    }
}

struct ByWeight;

impl Weighter<u64, Versioned> for ByWeight {
    fn weight(&self, _key: &u64, value: &Versioned) -> u64 {
        value.weight
    }
}

/// Sends every key to one shard, one home bucket, and one tag, so a key
/// pair's two buckets hold all the cache can.
#[derive(Clone, Default)]
struct Colliding;

struct CollidingHasher;

impl Hasher for CollidingHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

impl BuildHasher for Colliding {
    type Hasher = CollidingHasher;
    fn build_hasher(&self) -> CollidingHasher {
        CollidingHasher
    }
}

/// Keys that share a few hash patterns: shards, home buckets, and tags
/// collide often but not always.
#[derive(Clone, Default)]
struct Clustered;

struct ClusteredHasher(u64);

impl Hasher for ClusteredHasher {
    fn finish(&self) -> u64 {
        // Eight patterns, each a well-mixed hash.
        let mut mixed = (self.0 % 8).wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only `u64` keys");
    }
    fn write_u64(&mut self, key: u64) {
        self.0 = key;
    }
}

impl BuildHasher for Clustered {
    type Hasher = ClusteredHasher;
    fn build_hasher(&self) -> ClusteredHasher {
        ClusteredHasher(0)
    }
}

type Cache<S> = ShardedCache<u64, Versioned, ByWeight, S, Listener>;

/// Checks every shard's table against its entries and counts: each key
/// lives once, in its own shard, in one of its two candidate buckets, under
/// its tag, flagged as displaced exactly when outside its home; every home
/// counts its displaced keys unless the count saturated; and the shard's
/// length and weight are its entries' count and summed weight, within its
/// limits.
#[allow(
    unsafe_code,
    reason = "reads entries the table publishes under the check's pin"
)]
fn check_tables<S: BuildHasher>(cache: &Cache<S>) {
    let pin = cache.epochs.pin();
    for index in 0..cache.shards.len() {
        let shard = cache.view(index);
        let writer = shard.lock();
        let mut displaced_from = vec![0_u64; shard.buckets.len()];
        let mut keys = HashSet::new();
        let mut weight = 0;
        for (bucket, current) in shard.buckets.iter().enumerate() {
            for slot in 0..SLOTS {
                let raw = current.handles[slot].load(Ordering::Relaxed);
                if raw == 0 {
                    assert_eq!(current.tag(slot), 0, "an empty slot keeps a tag");
                    continue;
                }
                // SAFETY: the table publishes this handle while `pin` is held.
                let found = unsafe { entry(&pin, shard.handle(raw)) };
                let key = found.value().0;
                let hash = cache.hash(&key);
                assert_eq!(cache.shard_index(hash), index, "{key} in another shard");
                assert_eq!(current.tag(slot), tag(hash), "{key} under another tag");
                let (home, alternate) = shard.candidates(hash);
                assert!(
                    bucket == home || bucket == alternate,
                    "{key} in bucket {bucket}, outside {home} and {alternate}"
                );
                assert_eq!(current.is_displaced(slot), bucket != home);
                displaced_from[home] += u64::from(bucket != home);
                assert!(keys.insert(key), "{key} published twice");
                weight += found.weight();
            }
        }
        for (bucket, (current, displaced)) in shard.buckets.iter().zip(displaced_from).enumerate() {
            let counted = overflow(current.meta.load(Ordering::Relaxed));
            assert!(
                counted == displaced || counted == OVERFLOW_MAX,
                "bucket {bucket} counts {counted} displaced keys of {displaced}"
            );
        }
        assert_eq!(writer.len(), keys.len(), "shard {index}'s length");
        assert_eq!(writer.weight(), weight, "shard {index}'s weight");
        assert!(
            keys.len() <= shard.limits.capacity(),
            "shard {index} holds {} of {}",
            keys.len(),
            shard.limits.capacity()
        );
        assert!(weight <= shard.limits.weight(), "shard {index} overweight");
    }
}

/// One run's cache and what it checks against.
struct Run<'cache, S> {
    cache: &'cache Cache<S>,
    heard: Heard,
    model: Model,
    rng: Rng,
    keys: u64,
    /// The weight a heavy value takes: past some shard's share, if limited.
    heavy: u64,
    exercised: Exercised,
}

impl<S: BuildHasher> Run<'_, S> {
    /// Takes the removals heard since the last call out of the model.
    fn settle(&mut self, explicit_expired: bool) {
        let heard = std::mem::take(&mut *self.heard.lock());
        for removal in heard {
            self.model.remove_heard(removal, explicit_expired);
        }
    }

    fn weight(&mut self) -> u64 {
        match self.rng.below(20) {
            0 => 0,
            1 => self.heavy,
            2 => u64::from(MAX_WEIGHT) + 1,
            _ => 1 + self.rng.below(4),
        }
    }

    /// Whether a value of `weight` fits `key`'s shard.
    fn fits(&self, key: u64, weight: u64) -> bool {
        weight <= u64::from(MAX_WEIGHT)
            && weight <= self.cache.shard(self.cache.hash(&key)).limits.weight()
    }

    fn insert(&mut self, key: u64) {
        let (version, weight) = (self.model.version(), self.weight());
        let ttl = self
            .rng
            .pick(&[Ttl::Default, Ttl::Default, Ttl::Hour, Ttl::Zero]);
        let value = Versioned::new(version, weight);
        let cached = match ttl {
            Ttl::Default => self.cache.insert(key, value),
            Ttl::Hour => self.cache.insert_with_ttl(key, value, HOUR),
            Ttl::Zero => self.cache.insert_with_ttl(key, value, Duration::ZERO),
        };
        assert_eq!(cached, self.fits(key, weight), "insert of weight {weight}");
        self.exercised.refusals += u64::from(!cached);
        self.settle(false);
        if cached {
            let written = self.model.written(version, weight, ttl);
            assert!(
                self.model.entries.insert(key, written).is_none(),
                "{key}'s previous value was not heard leaving"
            );
        } else {
            assert!(
                !self.model.entries.contains_key(&key),
                "a refused write left {key}'s previous value"
            );
        }
    }

    fn insert_many(&mut self) {
        let count = 2 + self.rng.below(40);
        // Distinct keys, in the order drawn.
        let mut batch = Vec::new();
        let mut drawn = HashSet::new();
        while (batch.len() as u64) < count.min(self.keys) {
            let key = self.rng.below(self.keys);
            if drawn.insert(key) {
                batch.push((key, (self.model.version(), 1 + self.rng.below(2))));
            }
        }
        let entries: Vec<(u64, Versioned)> = batch
            .iter()
            .map(|(key, (version, weight))| (*key, Versioned::new(*version, *weight)))
            .collect();
        let fitting = batch
            .iter()
            .filter(|(key, (_, weight))| self.fits(*key, *weight))
            .count();
        assert_eq!(self.cache.insert_many(entries), fitting);
        // Each batch key's previous value leaves once: replaced, removed by
        // a write too heavy to cache, or evicted by an earlier insert.
        let mut before: HashMap<u64, Modeled> = batch
            .iter()
            .filter_map(|(key, _)| Some((*key, self.model.entries.remove(key)?)))
            .collect();
        for (key, (version, weight)) in &batch {
            if self.fits(*key, *weight) {
                let written = self.model.written(*version, *weight, Ttl::Default);
                self.model.entries.insert(*key, written);
            }
        }
        let heard = std::mem::take(&mut *self.heard.lock());
        for (key, version, cause) in heard {
            if let Some(previous) = before.remove(&key) {
                assert_eq!(previous.version, version, "{key} v{version} left");
                let expired = self.model.expired(&previous);
                match cause {
                    RemovalCause::Expired => {
                        assert!(expired, "{key} v{version} expired early");
                        self.model.expirations += 1;
                    }
                    RemovalCause::Size => {
                        assert!(!expired, "{key} v{version} evicted, but expired");
                        self.model.evictions += 1;
                    }
                    RemovalCause::Replaced => assert!(!expired, "{key} v{version} expired"),
                    RemovalCause::Explicit => panic!("{key} v{version} left explicitly"),
                }
            } else {
                // Others' values, and the batch's own, evicted by a later
                // insert of it.
                self.model.remove_heard((key, version, cause), false);
            }
        }
        assert!(
            before.is_empty(),
            "previous values never heard leaving: {before:?}"
        );
    }

    fn get(&mut self, key: u64) {
        let found = self.cache.get(&key).map(|value| value.version);
        let modeled = self.model.read(key);
        assert_eq!(found, modeled.map(|modeled| modeled.version), "get({key})");
    }

    fn peek(&mut self, key: u64) {
        let live = self.model.live(key).map(|modeled| modeled.version);
        assert_eq!(
            self.cache.peek(&key).map(|value| value.version),
            live,
            "peek({key})"
        );
        assert_eq!(self.cache.contains_key(&key), live.is_some());
    }

    fn remove(&mut self, key: u64) {
        let live = self.model.live(key).is_some();
        assert_eq!(self.cache.remove(&key), live, "remove({key})");
        self.settle(false);
        assert!(!self.model.entries.contains_key(&key));
    }

    fn remove_if(&mut self, key: u64) {
        let live = self.model.live(key);
        let offered = Cell::new(None);
        let removed = self.cache.remove_if(&key, |value| {
            offered.set(Some(value.version));
            value.version % 2 == 0
        });
        assert_eq!(offered.get(), live.map(|modeled| modeled.version));
        assert_eq!(
            removed,
            live.is_some_and(|modeled| modeled.version % 2 == 0)
        );
        self.settle(false);
        if live.is_none() {
            assert!(!self.model.entries.contains_key(&key));
        }
    }

    fn replace(&mut self, key: u64) {
        let (version, weight) = (self.model.version(), self.weight());
        let live = self.model.live(key).is_some();
        let fits = self.fits(key, weight);
        match self.cache.replace(key, Versioned::new(version, weight)) {
            Ok(()) => {
                assert!(live && fits, "replaced {key}, live {live}, fits {fits}");
                self.exercised.replacements += 1;
                self.settle(false);
                let written = self.model.written(version, weight, Ttl::Default);
                assert!(self.model.entries.insert(key, written).is_none());
            }
            Err((returned, value)) => {
                assert!(
                    !live || !fits,
                    "refused to replace live {key} of weight {weight}"
                );
                assert_eq!((returned, value.version), (key, version));
                self.settle(false);
                if !fits {
                    assert!(!self.model.entries.contains_key(&key));
                }
            }
        }
    }

    /// The value a load caches: as an insert's, but a load too heavy to
    /// cache leaves an expired entry it would have replaced.
    fn loaded(&mut self, key: u64, version: u64, weight: u64, cached: bool) {
        assert_eq!(cached, self.fits(key, weight));
        self.exercised.loads_cached += u64::from(cached);
        self.settle(false);
        if cached {
            let written = self.model.written(version, weight, Ttl::Default);
            assert!(self.model.entries.insert(key, written).is_none());
        }
    }

    fn get_or_insert_with(&mut self, key: u64) {
        let (version, weight) = (self.model.version(), self.weight());
        let loads = Cell::new(0);
        let got = self.cache.get_or_insert_with(&key, || {
            loads.set(loads.get() + 1);
            Versioned::new(version, weight)
        });
        if let Some(modeled) = self.model.read(key) {
            assert_eq!((got.version, loads.get()), (modeled.version, 0));
            assert!(self.heard.lock().is_empty());
        } else {
            assert_eq!((got.version, loads.get()), (version, 1));
            let cached = self
                .cache
                .peek(&key)
                .is_some_and(|value| value.version == version);
            self.loaded(key, version, weight, cached);
        }
    }

    fn get_or_fail(&mut self, key: u64) {
        let got = self
            .cache
            .get_or_try_insert_with(&key, || Err::<Versioned, _>(key));
        let modeled = self.model.read(key);
        match got {
            Ok(value) => assert_eq!(Some(value.version), modeled.map(|modeled| modeled.version)),
            Err(failed) => assert_eq!((failed, modeled.map(|_| ())), (key, None)),
        }
        assert!(self.heard.lock().is_empty());
    }

    fn get_value_or_guard(&mut self, key: u64) {
        let (version, weight) = (self.model.version(), self.weight());
        let insert = self.rng.below(4) != 0;
        let cache = self.cache;
        let result = cache.get_value_or_guard(&key, Some(Duration::ZERO));
        let modeled = self.model.read(key);
        match result {
            GuardResult::Value(value) => {
                assert_eq!(Some(value.version), modeled.map(|modeled| modeled.version));
            }
            GuardResult::Guard(guard) => {
                assert!(modeled.is_none(), "a guard for live {key}");
                assert_eq!(*guard.key(), key);
                if insert {
                    let cached = guard.insert(Versioned::new(version, weight));
                    self.loaded(key, version, weight, cached);
                }
            }
            GuardResult::Timeout => panic!("no other load of {key} to wait for"),
        }
        assert!(self.heard.lock().is_empty());
    }

    /// Reads through a guard, writes `key` and churns its shard, then checks
    /// the value borrowed before the writes.
    fn pinned_read(&mut self, key: u64) {
        let cache = self.cache;
        let guard = cache.pin();
        let borrowed = guard.get(&key);
        let modeled = self.model.read(key);
        assert_eq!(
            borrowed.map(|value| value.version),
            modeled.map(|modeled| modeled.version)
        );
        let kept = borrowed.map(|value| (value.version, value.weight));
        self.insert(key);
        for _ in 0..self.rng.below(8) {
            let other = self.rng.below(self.keys);
            self.insert(other);
        }
        assert_eq!(borrowed.map(|value| (value.version, value.weight)), kept);
    }

    fn retain(&mut self) {
        let salt = self.rng.below(3);
        let mut offered = HashMap::new();
        self.cache.retain(|key, value| {
            assert!(
                offered.insert(*key, value.version).is_none(),
                "{key} offered twice"
            );
            !(value.version + salt).is_multiple_of(3)
        });
        let live: HashMap<u64, u64> = self
            .model
            .entries
            .keys()
            .filter_map(|key| Some((*key, self.model.live(*key)?.version)))
            .collect();
        assert_eq!(offered, live, "retain offers each live entry once");
        self.settle(false);
        for (key, version) in &offered {
            let kept = !(version + salt).is_multiple_of(3);
            assert_eq!(self.model.entries.contains_key(key), kept, "{key}");
        }
        assert!(
            self.model
                .entries
                .keys()
                .all(|key| offered.contains_key(key))
        );
    }

    fn drain(&mut self) {
        let take = self.rng.pick(&[0, 1, 5, usize::MAX]);
        let mut drained = HashMap::new();
        {
            let mut drain = self.cache.drain();
            for (key, value) in drain.by_ref().take(take) {
                assert!(
                    drained.insert(key, value.version).is_none(),
                    "{key} drained twice"
                );
                assert_eq!(
                    self.model.live(key).map(|modeled| modeled.version),
                    Some(value.version),
                    "drained {key}"
                );
            }
        }
        self.settle(false);
        for key in drained.keys() {
            assert!(!self.model.entries.contains_key(key));
        }
        if take == usize::MAX {
            assert!(self.model.entries.is_empty());
        }
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.settle(true);
        assert!(self.model.entries.is_empty());
    }

    fn resize(&mut self) {
        let cache = self.cache;
        self.exercised.resizes += 1;
        if self.rng.below(2) == 0 {
            let wanted =
                usize::try_from(self.rng.below(2 * cache.max_capacity() as u64 + 2)).expect("fits");
            let set = cache.set_capacity(wanted);
            assert_eq!(set, wanted.clamp(cache.shards.len(), cache.max_capacity()));
            assert_eq!(set, cache.capacity());
        } else if cache.max_weight() != u64::MAX {
            let wanted = cache.shards.len() as u64 * (1 + self.rng.below(12));
            cache.set_max_weight(wanted);
            self.heavy = wanted;
        }
        self.settle(false);
    }

    fn iterate(&mut self) {
        let live: HashMap<u64, u64> = self
            .model
            .entries
            .keys()
            .filter_map(|key| Some((*key, self.model.live(*key)?.version)))
            .collect();
        let mut seen = HashMap::new();
        for (key, value) in self.cache.iter() {
            assert!(seen.insert(key, value.version).is_none(), "{key} twice");
        }
        assert_eq!(seen, live);
        let guard = self.cache.pin();
        let borrowed: HashMap<u64, u64> = guard
            .iter()
            .map(|(key, value)| (*key, value.version))
            .collect();
        assert_eq!(borrowed, live);
    }

    fn jump(&mut self) {
        self.cache.clock_skew_nanos.fetch_add(
            u64::try_from(Duration::from_secs(60 * JUMP_MINUTES).as_nanos()).expect("fits"),
            Ordering::Relaxed,
        );
        self.model.now += JUMP_MINUTES;
    }

    /// Checks the cache's counts and tables against the model.
    fn check(&mut self) {
        assert!(
            self.heard.lock().is_empty(),
            "removals heard after an operation"
        );
        check_tables(self.cache);
        assert_eq!(self.cache.len(), self.model.entries.len());
        assert_eq!(
            self.cache.weight(),
            self.model
                .entries
                .values()
                .map(|modeled| modeled.weight)
                .sum::<u64>()
        );
        let stats = self.cache.stats();
        assert_eq!(stats.evictions, self.model.evictions, "evictions");
        assert_eq!(stats.expirations, self.model.expirations, "expirations");
        assert_eq!(
            (stats.hits, stats.misses),
            (self.model.hits, self.model.misses)
        );
    }

    fn step(&mut self) {
        let key = self.rng.below(self.keys);
        match self.rng.below(100) {
            0..30 => self.insert(key),
            30..45 => self.get(key),
            45..50 => self.peek(key),
            50..56 => self.remove(key),
            56..60 => self.remove_if(key),
            60..65 => self.replace(key),
            65..72 => self.get_or_insert_with(key),
            72..74 => self.get_or_fail(key),
            74..79 => self.get_value_or_guard(key),
            79..82 => self.pinned_read(key),
            82..85 => self.insert_many(),
            85 => self.retain(),
            86 => self.drain(),
            87 => self.clear(),
            88..91 => self.resize(),
            91..93 => self.iterate(),
            93..95 => self.jump(),
            _ => {
                if self.rng.below(4) == 0 {
                    self.cache.maintain();
                }
                for key in 0..self.keys {
                    self.peek(key);
                }
            }
        }
        self.check();
    }
}

fn run<S: BuildHasher + Clone>(seed: u64, hasher: &S, steps: usize, exercised: &mut Exercised) {
    let mut rng = Rng::new(seed);
    let shards = rng.pick(&[1_usize, 2, 4, 16]);
    let capacity = shards + usize::try_from(rng.below(40 * shards as u64)).expect("fits");
    let max_capacity = capacity * rng.pick(&[1, 1, 3]);
    let lifetime = rng.pick(&[Lifetime::Forever, Lifetime::ToLive, Lifetime::ToIdle]);
    let limited = rng.below(2) == 0;
    let keys = (capacity as u64 * rng.pick(&[1, 2, 4]) / 2).max(4);
    let heard = Heard::default();
    let mut builder = ShardedCacheBuilder::new(capacity)
        .max_capacity(max_capacity)
        .shards(shards)
        .record_stats()
        .hasher(hasher.clone())
        .weighter(ByWeight)
        .removal_listener(Listener(Arc::clone(&heard)));
    let max_weight = 2 * capacity as u64;
    if limited {
        builder = builder.max_weight(max_weight);
    }
    builder = match lifetime {
        Lifetime::Forever => builder,
        Lifetime::ToLive => builder.time_to_live(HOUR),
        Lifetime::ToIdle => builder.time_to_idle(HOUR),
    };
    let alive = VERSIONED_ALIVE.get();
    {
        let cache: Cache<S> = builder.build();
        let mut run = Run {
            cache: &cache,
            heard,
            model: Model {
                entries: HashMap::new(),
                now: 0,
                lifetime,
                next_version: 0,
                evictions: 0,
                expirations: 0,
                hits: 0,
                misses: 0,
            },
            rng,
            keys,
            heavy: if limited {
                max_weight
            } else {
                u64::from(MAX_WEIGHT)
            },
            exercised: Exercised::default(),
        };
        let (pairs, relocations) = (WINDOW_EVICTIONS.get(), RELOCATIONS.get());
        for step in 0..steps {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run.step()));
            if let Err(panic) = result {
                eprintln!(
                    "seed {seed}, step {step}: {shards} shards, capacity {capacity} of \
                     {max_capacity}, {lifetime:?}, weight limited {limited}, {keys} keys"
                );
                std::panic::resume_unwind(panic);
            }
        }
        exercised.evictions += run.model.evictions;
        exercised.expirations += run.model.expirations;
        exercised.refusals += run.exercised.refusals;
        exercised.loads_cached += run.exercised.loads_cached;
        exercised.replacements += run.exercised.replacements;
        exercised.resizes += run.exercised.resizes;
        exercised.pair_evictions += WINDOW_EVICTIONS.get() - pairs;
        exercised.relocations += RELOCATIONS.get() - relocations;
    }
    assert_eq!(
        VERSIONED_ALIVE.get(),
        alive,
        "seed {seed}: values leaked or dropped twice"
    );
}

const SEEDS: u64 = if cfg!(miri) { 2 } else { 300 };
const STEPS: usize = if cfg!(miri) { 150 } else { 2_000 };

/// Runs `SEEDS` seeds from `first` and checks that they evicted, expired,
/// refused, loaded, replaced, and resized, and that they relocated keys and
/// evicted from full bucket pairs as `relocates` and `fills_pairs` say.
fn run_seeds<S: BuildHasher + Clone>(first: u64, hasher: &S, relocates: bool, fills_pairs: bool) {
    let mut exercised = Exercised::default();
    for seed in first..first + SEEDS {
        run(seed, hasher, STEPS, &mut exercised);
    }
    let Exercised {
        evictions,
        expirations,
        refusals,
        loads_cached,
        replacements,
        resizes,
        pair_evictions,
        relocations,
    } = &exercised;
    assert!(
        [
            evictions,
            expirations,
            refusals,
            loads_cached,
            replacements,
            resizes
        ]
        .iter()
        .all(|count| **count > 0)
            && (*relocations > 0) == relocates
            && (*pair_evictions > 0) == fills_pairs,
        "{exercised:?}"
    );
}

#[test]
fn random_operations_match_the_model_the_listener_keeps() {
    // A fixed hasher, so a seed replays its run. Tables a fifth empty
    // rarely fill both of a key's buckets.
    run_seeds(
        0,
        &BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(),
        true,
        false,
    );
}

#[test]
fn random_operations_on_clustered_keys_match_the_model() {
    run_seeds(1_000, &Clustered, true, true);
}

#[test]
fn random_operations_on_one_bucket_pair_match_the_model() {
    // Every key shares both buckets, so none can move to make room.
    run_seeds(2_000, &Colliding, false, true);
}
