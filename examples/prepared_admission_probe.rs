//! Structural cost floor for prepared non-boundary admission keys.
//!
//! This fixture keeps the production serialized control-word publication
//! shape but isolates it from cache arena and capacity accounting. It compares
//! the current per-insert materialization, safe tail materialization, keys
//! prepared before the timed region, and a bounded 32-key staging pipeline
//! whose preparation is included in the measurement.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::hint::{black_box, spin_loop};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const BUCKET_SLOTS: usize = 8;
const TARGET_NUMERATOR: usize = 4;
const TARGET_DENOMINATOR: usize = 1;
const BYTE_LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const WRITING: u8 = 1;
#[derive(Clone, Copy)]
enum Mode {
    WholeSlice,
    TailMatch,
    PreparedBytes,
    PreparedWords,
    StagedBytes8,
    StagedBytes16,
    StagedBytes32,
    StagedWords8,
    StagedWords16,
    StagedWords32,
}

impl Mode {
    const ALL: [Self; 10] = [
        Self::WholeSlice,
        Self::TailMatch,
        Self::PreparedBytes,
        Self::PreparedWords,
        Self::StagedBytes8,
        Self::StagedBytes16,
        Self::StagedBytes32,
        Self::StagedWords8,
        Self::StagedWords16,
        Self::StagedWords32,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::WholeSlice => "inline_whole_slice",
            Self::TailMatch => "inline_tail_match",
            Self::PreparedBytes => "prepared_bytes_before_measurement",
            Self::PreparedWords => "prepared_words_before_measurement",
            Self::StagedBytes8 => "tail_match_staged_bytes_8_counted",
            Self::StagedBytes16 => "tail_match_staged_bytes_16_counted",
            Self::StagedBytes32 => "tail_match_staged_bytes_32_counted",
            Self::StagedWords8 => "tail_match_staged_words_8_counted",
            Self::StagedWords16 => "tail_match_staged_words_16_counted",
            Self::StagedWords32 => "tail_match_staged_words_32_counted",
        }
    }

    const fn staged_keys(self) -> usize {
        match self {
            Self::StagedBytes8 | Self::StagedWords8 => 8,
            Self::StagedBytes16 | Self::StagedWords16 => 16,
            Self::StagedBytes32 | Self::StagedWords32 => 32,
            Self::WholeSlice | Self::TailMatch | Self::PreparedBytes | Self::PreparedWords => 0,
        }
    }

    const fn is_prepared(self) -> bool {
        matches!(self, Self::PreparedBytes | Self::PreparedWords)
    }
}

trait EncodedWords<const WORDS: usize> {
    fn word(&self, index: usize) -> u64;
}

impl<const N: usize, const WORDS: usize> EncodedWords<WORDS> for [u8; N] {
    fn word(&self, index: usize) -> u64 {
        debug_assert_eq!(N, WORDS * 8);
        encoded_word(self, index)
    }
}

impl<const WORDS: usize> EncodedWords<WORDS> for [u64; WORDS] {
    fn word(&self, index: usize) -> u64 {
        self[index]
    }
}

struct Slot<const WORDS: usize> {
    words: [AtomicU64; WORDS],
}

struct Table<const N: usize, const WORDS: usize> {
    controls: Box<[AtomicU64]>,
    slots: Box<[Slot<WORDS>]>,
    buckets: usize,
}

impl<const N: usize, const WORDS: usize> Table<N, WORDS> {
    fn with_capacity(capacity: usize) -> Self {
        assert_eq!(N, WORDS * 8);
        let slots = capacity
            .saturating_mul(TARGET_NUMERATOR)
            .div_ceil(TARGET_DENOMINATOR);
        let buckets = slots.div_ceil(BUCKET_SLOTS).max(1);
        Self {
            controls: (0..buckets)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            slots: (0..buckets * BUCKET_SLOTS)
                .map(|_| Slot {
                    words: std::array::from_fn(|_| AtomicU64::new(0)),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            buckets,
        }
    }

    fn insert<K: EncodedWords<WORDS>>(&self, key: &K, hash: u64) -> bool {
        let tag = u8::try_from(hash % 254).expect("tag remainder fits u8") + 2;
        for bucket in bucket_choices(hash, self.buckets) {
            loop {
                let control = self.controls[bucket].load(Ordering::Acquire);
                if matching_bytes(control, WRITING) != 0 {
                    spin_loop();
                    continue;
                }
                let mut candidates = matching_bytes(control, tag);
                while let Some(offset) = take_offset(&mut candidates) {
                    let index = bucket * BUCKET_SLOTS + offset;
                    if self.key_matches(index, key) {
                        return false;
                    }
                }
                let Some(offset) = first_offset(control, 0) else {
                    break;
                };
                let shift = offset * u8::BITS as usize;
                let claimed = control | (u64::from(WRITING) << shift);
                if self.controls[bucket]
                    .compare_exchange(control, claimed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                }
                let index = bucket * BUCKET_SLOTS + offset;
                for word in 0..WORDS {
                    self.slots[index].words[word].store(key.word(word), Ordering::Relaxed);
                }
                let byte_mask = u64::from(u8::MAX) << shift;
                let published = (control & !byte_mask) | (u64::from(tag) << shift);
                self.controls[bucket].store(published, Ordering::Release);
                return true;
            }
        }
        panic!("four sparse bucket choices unexpectedly exhausted");
    }

    fn key_matches<K: EncodedWords<WORDS>>(&self, index: usize, key: &K) -> bool {
        (0..WORDS)
            .all(|word| self.slots[index].words[word].load(Ordering::Relaxed) == key.word(word))
    }
}

fn encode_whole_slice<const N: usize>(key: &[u8]) -> [u8; N] {
    let mut encoded = [0_u8; N];
    encoded[..key.len()].copy_from_slice(key);
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

fn encode_tail_match<const N: usize>(key: &[u8]) -> [u8; N] {
    let prefix = N - 8;
    let mut encoded = [0_u8; N];
    encoded[..prefix].copy_from_slice(&key[..prefix]);
    let tail = &key[prefix..];
    match tail.len() {
        0 => {}
        1 => encoded[prefix] = tail[0],
        2 => encoded[prefix..prefix + 2].copy_from_slice(tail),
        3 => encoded[prefix..prefix + 3].copy_from_slice(tail),
        4 => encoded[prefix..prefix + 4].copy_from_slice(tail),
        5 => encoded[prefix..prefix + 5].copy_from_slice(tail),
        6 => encoded[prefix..prefix + 6].copy_from_slice(tail),
        7 => encoded[prefix..prefix + 7].copy_from_slice(tail),
        _ => unreachable!("a short size-class tail is at most seven bytes"),
    }
    encoded[N - 1] = u8::try_from(key.len()).expect("probe key length fits u8");
    encoded
}

fn encoded_word<const N: usize>(key: &[u8; N], word: usize) -> u64 {
    let begin = word * 8;
    u64::from_ne_bytes(
        key[begin..begin + 8]
            .try_into()
            .expect("encoded key word is eight bytes"),
    )
}

fn encode_tail_words<const N: usize, const WORDS: usize>(key: &[u8]) -> [u64; WORDS] {
    debug_assert_eq!(N, WORDS * 8);
    let encoded = encode_tail_match::<N>(key);
    std::array::from_fn(|word| encoded_word(&encoded, word))
}

fn binary_key(value: usize, key_bytes: usize) -> Box<[u8]> {
    let mut key = vec![0_u8; key_bytes];
    let mut state = mix(value as u64);
    for chunk in key.chunks_mut(8) {
        state = mix(state);
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    key.into_boxed_slice()
}

fn measure<const N: usize, const WORDS: usize>(
    mode: Mode,
    keys: &Arc<Vec<Box<[u8]>>>,
    prepared_bytes: &Arc<Vec<[u8; N]>>,
    prepared_words: &Arc<Vec<[u64; WORDS]>>,
    hashes: &Arc<Vec<u64>>,
    threads: usize,
) -> f64 {
    let entries = keys.len();
    let table = Arc::new(Table::<N, WORDS>::with_capacity(entries));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for worker in 0..threads {
        let table = Arc::clone(&table);
        let keys = Arc::clone(keys);
        let prepared_bytes = Arc::clone(prepared_bytes);
        let prepared_words = Arc::clone(prepared_words);
        let hashes = Arc::clone(hashes);
        let barrier = Arc::clone(&barrier);
        let begin = entries * worker / threads;
        let end = entries * (worker + 1) / threads;
        workers.push(thread::spawn(move || {
            barrier.wait();
            let mut inserted = 0_usize;
            match mode {
                Mode::WholeSlice => {
                    for index in begin..end {
                        let encoded = encode_whole_slice::<N>(black_box(&keys[index]));
                        inserted += usize::from(table.insert(black_box(&encoded), hashes[index]));
                    }
                }
                Mode::TailMatch => {
                    for index in begin..end {
                        let encoded = encode_tail_match::<N>(black_box(&keys[index]));
                        inserted += usize::from(table.insert(black_box(&encoded), hashes[index]));
                    }
                }
                Mode::PreparedBytes => {
                    for index in begin..end {
                        inserted += usize::from(
                            table.insert(black_box(&prepared_bytes[index]), hashes[index]),
                        );
                    }
                }
                Mode::PreparedWords => {
                    for index in begin..end {
                        inserted += usize::from(
                            table.insert(black_box(&prepared_words[index]), hashes[index]),
                        );
                    }
                }
                Mode::StagedBytes8 => {
                    inserted =
                        insert_staged_bytes::<N, WORDS, 8>(&table, &keys, &hashes, begin, end);
                }
                Mode::StagedBytes16 => {
                    inserted =
                        insert_staged_bytes::<N, WORDS, 16>(&table, &keys, &hashes, begin, end);
                }
                Mode::StagedBytes32 => {
                    inserted =
                        insert_staged_bytes::<N, WORDS, 32>(&table, &keys, &hashes, begin, end);
                }
                Mode::StagedWords8 => {
                    inserted =
                        insert_staged_words::<N, WORDS, 8>(&table, &keys, &hashes, begin, end);
                }
                Mode::StagedWords16 => {
                    inserted =
                        insert_staged_words::<N, WORDS, 16>(&table, &keys, &hashes, begin, end);
                }
                Mode::StagedWords32 => {
                    inserted =
                        insert_staged_words::<N, WORDS, 32>(&table, &keys, &hashes, begin, end);
                }
            }
            inserted
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let inserted = workers
        .into_iter()
        .map(|worker| worker.join().expect("probe worker completes"))
        .sum::<usize>();
    let elapsed = started.elapsed();
    assert_eq!(inserted, entries);
    entries as f64 / elapsed.as_secs_f64() / 1_000_000.0
}

fn insert_staged_bytes<const N: usize, const WORDS: usize, const STAGE: usize>(
    table: &Table<N, WORDS>,
    keys: &[Box<[u8]>],
    hashes: &[u64],
    begin: usize,
    end: usize,
) -> usize {
    let mut inserted = 0;
    let mut cursor = begin;
    while cursor < end {
        let count = (end - cursor).min(STAGE);
        let staged: [[u8; N]; STAGE] = std::array::from_fn(|offset| {
            if offset < count {
                encode_tail_match::<N>(black_box(&keys[cursor + offset]))
            } else {
                [0_u8; N]
            }
        });
        for offset in 0..count {
            inserted +=
                usize::from(table.insert(black_box(&staged[offset]), hashes[cursor + offset]));
        }
        cursor += count;
    }
    inserted
}

fn insert_staged_words<const N: usize, const WORDS: usize, const STAGE: usize>(
    table: &Table<N, WORDS>,
    keys: &[Box<[u8]>],
    hashes: &[u64],
    begin: usize,
    end: usize,
) -> usize {
    let mut inserted = 0;
    let mut cursor = begin;
    while cursor < end {
        let count = (end - cursor).min(STAGE);
        let staged: [[u64; WORDS]; STAGE] = std::array::from_fn(|offset| {
            if offset < count {
                encode_tail_words::<N, WORDS>(black_box(&keys[cursor + offset]))
            } else {
                [0_u64; WORDS]
            }
        });
        for offset in 0..count {
            inserted +=
                usize::from(table.insert(black_box(&staged[offset]), hashes[cursor + offset]));
        }
        cursor += count;
    }
    inserted
}

fn percentile(mut values: Vec<f64>, numerator: usize, denominator: usize) -> f64 {
    values.sort_by(f64::total_cmp);
    let index = values.len().saturating_sub(1).saturating_mul(numerator) / denominator;
    values[index]
}

fn run<const N: usize, const WORDS: usize>(
    key_bytes: usize,
    entries: usize,
    threads: usize,
    samples: usize,
) {
    assert!((N - 8..N).contains(&key_bytes));
    let keys = Arc::new(
        (0..entries)
            .map(|index| binary_key(index, key_bytes))
            .collect::<Vec<_>>(),
    );
    let prepared_bytes = Arc::new(
        keys.iter()
            .map(|key| encode_whole_slice::<N>(key))
            .collect::<Vec<_>>(),
    );
    let prepared_words = Arc::new(
        keys.iter()
            .map(|key| encode_tail_words::<N, WORDS>(key))
            .collect::<Vec<_>>(),
    );
    for ((key, encoded), words) in keys
        .iter()
        .zip(prepared_bytes.iter())
        .zip(prepared_words.iter())
    {
        assert_eq!(encode_tail_match::<N>(key), *encoded);
        for (word, prepared_word) in words.iter().enumerate() {
            assert_eq!(*prepared_word, encoded_word(encoded, word));
        }
    }
    let hashes = Arc::new((0..entries).map(|index| mix(index as u64)).collect());
    let mut results = vec![Vec::with_capacity(samples); Mode::ALL.len()];
    for sample in 0..samples {
        for offset in 0..Mode::ALL.len() {
            let index = (sample + offset) % Mode::ALL.len();
            results[index].push(measure::<N, WORDS>(
                Mode::ALL[index],
                &keys,
                &prepared_bytes,
                &prepared_words,
                &hashes,
                threads,
            ));
        }
    }

    println!(
        "mode,key_bytes,class_bytes,entries,threads,samples,median_mops,p05_mops,prepared_bytes_per_pending_key,staged_stack_bytes_per_worker"
    );
    for (mode, values) in Mode::ALL.into_iter().zip(results) {
        let median = percentile(values.clone(), 1, 2);
        let p05 = percentile(values, 1, 20);
        let prepared_bytes = usize::from(mode.is_prepared()) * N;
        let staged_bytes = mode.staged_keys() * N;
        println!(
            "{},{key_bytes},{N},{entries},{threads},{samples},{median:.3},{p05:.3},{prepared_bytes},{staged_bytes}",
            mode.name()
        );
    }
}

fn argument(arguments: &mut impl Iterator<Item = String>, default: usize) -> usize {
    arguments.next().map_or(default, |value| {
        value.parse().expect("probe arguments are integers")
    })
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let key_bytes = argument(&mut arguments, 13);
    let entries = argument(&mut arguments, 1_000_000).max(1);
    let threads = argument(&mut arguments, 8).max(1);
    let samples = argument(&mut arguments, 13).max(3);
    match key_bytes {
        1..=7 => run::<8, 1>(key_bytes, entries, threads, samples),
        8..=15 => run::<16, 2>(key_bytes, entries, threads, samples),
        16..=23 => run::<24, 3>(key_bytes, entries, threads, samples),
        24..=31 => run::<32, 4>(key_bytes, entries, threads, samples),
        _ => panic!("probe accepts non-boundary widths from 1 through 31"),
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn start_index(hash: u64, buckets: usize) -> usize {
    usize::try_from((u128::from(hash) * buckets as u128) >> 64)
        .expect("reduced bucket index fits usize")
}

fn bucket_choices(hash: u64, buckets: usize) -> [usize; 4] {
    let primary = start_index(hash, buckets);
    let mut secondary = start_index(hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15, buckets);
    while secondary == primary {
        secondary = (secondary + 1) % buckets;
    }
    let mut tertiary = start_index(hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93, buckets);
    while tertiary == primary || tertiary == secondary {
        tertiary = (tertiary + 1) % buckets;
    }
    let mut fourth = start_index(hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f, buckets);
    while fourth == primary || fourth == secondary || fourth == tertiary {
        fourth = (fourth + 1) % buckets;
    }
    [primary, secondary, tertiary, fourth]
}

fn matching_bytes(control: u64, state: u8) -> u64 {
    let different = control ^ (u64::from(state) * BYTE_ONES);
    !((different & BYTE_LOW_BITS).wrapping_add(BYTE_LOW_BITS) | different | BYTE_LOW_BITS)
        & BYTE_HIGH_BITS
}

fn first_offset(control: u64, state: u8) -> Option<usize> {
    let matches = matching_bytes(control, state);
    (matches != 0).then(|| {
        usize::try_from(matches.trailing_zeros()).expect("control bit offset fits usize") / 8
    })
}

fn take_offset(matches: &mut u64) -> Option<usize> {
    if *matches == 0 {
        return None;
    }
    let offset =
        usize::try_from(matches.trailing_zeros()).expect("control bit offset fits usize") / 8;
    *matches &= *matches - 1;
    Some(offset)
}
