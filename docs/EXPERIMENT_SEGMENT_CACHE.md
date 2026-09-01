# Compact segment-cache experiment

Date: 2026-08-02

## Goal

Reduce the complete structural cost of the mixed 8/16/24/32/48-byte key and
64-byte value cache from 22.355 B/entry into the 12--16 B/entry range without
allowing fingerprint collisions to return incorrect values.

The mixed distribution averages 18.8 key bytes, so its exact logical payload is
82.8 B/entry. The current `DirectPackedCache` result is 105.155 requested
B/entry:

```text
105.155 - 64.0 - 18.8 = 22.355 B/entry
```

## Accepted 64-byte bucket

The accepted frozen-generation index uses exactly one 64-byte cache line per
eight physical slots:

```text
atomic control word:  8 x [access bit | 7-bit primary tag] =  8 bytes
packed references:    8 x [16-bit secondary | 40-bit location] = 56 bytes
                                                               --------
                                                               64 bytes
```

- Control tag zero is the empty marker. The 128 possible seven-bit hash values
  are mapped onto 127 nonzero tags.
- The remaining 16 fingerprint bits live beside the location. The combined
  fingerprint has about 127 x 65,536 = 8.32 million outcomes.
- Every fingerprint candidate still verifies the exact packed key, so hash
  collisions cannot return an incorrect value.
- One control-word load filters all eight slots. Only a primary-tag candidate
  reads a seven-byte packed reference.
- The access bit supports sampled CLOCK state without a separate LRU node.
- A 40-bit append-only location addresses 1 TiB. A mutable implementation can
  interpret it as a 16-bit segment ID plus a 24-bit offset: 65,536 reclaimable
  16 MiB segments.
- Packed references are immutable within one generation. Whole-generation
  publication and epoch retirement provide the future mutation/ABA boundary.

The original control layout put an extra eight-byte control word beside eight
full atomic `u64` references, producing a 72-byte bucket. It consumed 14.286
B/entry of TTL overhead and was slower on both hits and misses; it was rejected.

An independently atomic `u64` per slot remains available as a benchmark control.
It encodes one access bit, a 23-bit fingerprint, and a 40-bit location. It has
the same 64-byte bucket cost, but scanning a bucket requires more atomic loads.

## Record math

Records retain exact data once:

```text
varint(key length) | varint(value length) | optional u16 expiry | key | value
```

The measured fixture needs one byte for each length. At the dense seven-live-
entries-per-eight-slots target:

| Layout | Index | Lengths | Expiry | Predicted overhead |
| --- | ---: | ---: | ---: | ---: |
| No per-entry TTL | 9.143 | 2 | 0 | **11.143 B/entry** |
| Relative 16-bit TTL | 9.143 | 2 | 2 | **13.143 B/entry** |

At six live entries per bucket, the TTL index costs 10.667 B/entry and complete
overhead is 14.667 B/entry. This spends 1.524 B/entry for shorter probe chains.

The TTL representation has 65,534 finite ticks plus a never-expire marker.
One-second ticks cover about 18.2 hours; one-minute ticks cover about 45.5 days.
A production cache needs TTL-classed segments or an overflow path for longer
deadlines.

## Measured result

The following is the median of seven interleaved fresh-process samples with one
million resident entries, five million operations per hit/miss pass, 64-byte
values, mixed keys, and relative TTL. Higher Mops/s is better; lower bytes are
better.

| Backend | Requested B/entry | Structural B/entry | Read hit Mops/s | Read miss Mops/s |
| --- | ---: | ---: | ---: | ---: |
| Packed bucket, 7/8 | **95.943** | **13.143** | **17.291** | 13.817 |
| Packed bucket, 6/8 | 97.467 | 14.667 | 16.969 | 17.300 |
| Atomic words, 7/8 | **95.943** | **13.143** | 15.201 | 11.265 |
| `DirectPackedCache` | 105.155 | 22.355 | 12.189 | **19.382** |

Compared with `DirectPackedCache`, the dense packed bucket uses 8.76% less
total requested memory and 41.2% less structural memory. Its hit throughput is
41.9% higher, while pure misses are 28.7% slower. Using median latency, the
dense packed layout wins the simple hit/miss weighted read time above roughly a
46% hit rate. The 6/8 layout moves that crossover to roughly 21%, at the cost of
1.524 B/entry.

The 7/8 layout remains the density-first default. The 6/8 option is useful when
miss latency matters more; 7/8 overtakes it on weighted read latency at roughly
a 93% hit rate in this fixture.

The allocation count reported by the probe is cumulative and includes the one
million transient boxed input keys. Requested live bytes are the meaningful
retained-memory measurement.

With expiration disabled, the same packed 7/8 layout measured 93.943 requested
B/entry and 11.143 structural B/entry. This exactly matches the model: removing
the `u16` deadline saves two bytes per live record.

## External-number interpretation

This result reaches Dragonfly's published 6--16 B/item overhead range for the
frozen layout, but it does not claim the same complete feature set or workload.
Dragonfly's number is described here:

<https://www.dragonflydb.io/blog/how-does-dragonfly-cut-cost>

Carrot's often-repeated six-byte figure is not complete record overhead. Its
`SubCompactBlockIndexFormat` is a six-byte index entry pointing into a block;
the exact record still carries length encoding and block storage costs. The
relevant implementation is here:

<https://github.com/carrotdata/carrot-cache/blob/main/src/main/java/com/carrotdata/cache/index/SubCompactBlockIndexFormat.java>

Carrot's record writer is here:

<https://github.com/carrotdata/carrot-cache/blob/main/src/main/java/com/carrotdata/cache/io/BlockDataWriter.java>

The comparable PackedGen numbers are therefore 9.143 B/entry for the index and
11.143 B/entry without TTL or 13.143 B/entry with TTL for complete structural
overhead. Matching Carrot's six-byte index would require a block-level locator
and a scan/decompression tradeoff, not merely a smaller per-record header.

## Rejected experiments

| Experiment | Result | Decision |
| --- | --- | --- |
| Extra control word beside eight `u64` slots | 97.086 B/entry, 16.109 M hit/s, 12.346 M miss/s in its comparison run | Reject: more RAM and slower than the inline baseline |
| Earlier subtract-mask SWAR matching over the packed control word | Improved miss scanning, but reduced hit throughput; scalar won above about 55% hits | Superseded by the exact candidate-mask implementation measured in `MUTABLE_SEGMENT_CONCURRENCY.md` |
| Six rather than seven occupied slots | Faster misses but +1.524 B/entry | Keep as an explicit workload option, not density default |
| Separate preallocated eight-byte-key base index | 98.369 pristine B/entry; 108.044 after 100k updates | Reject: saves changed-key bytes but charges 2.16 MB before the first mutation |
| Separate adaptive base index | 96.464 pristine; 108.125 update; 101.725 delete B/base-entry | Superseded: low idle cost, but updated/deleted reads were only 8.802/8.946 Mops/s |
| One-word pointer plus embedded three-bit tag | 107.138 update and 100.738 delete B/base-entry | Reject: excellent RAM, but false tag matches reduced updated/deleted reads to 7.124/7.760 Mops/s |
| Skip frozen control validation for known base slots | 17.06 -> 15.82 updated-read Mops/s; 17.31 -> 16.05 deleted-read Mops/s | Reject: the apparent load reduction regressed the measured path |
| One atomic dirty-route bitfield | 15.01 -> 14.22 untouched-read Mops/s; 17.60 -> 15.84 deleted-read Mops/s | Reject on this ARM host |

## Accepted mutable delta

`MutableSegmentCache` now layers concurrent point mutation over the packed
generation:

- Base-key changes are indexed by their immutable physical slot, so the
  variable-length key is not copied into the delta.
- The accepted base-override table uses one atomic eight-byte handle per slot
  plus one control byte. The control byte provides a 254-way hash tag; every
  candidate is still verified against the exact base key. Prime bucket counts
  avoid the power-of-two capacity cliff while double hashing retains complete
  probe coverage.
- Live overrides lazily allocate a 32-bit base-record-location sidecar. It
  eliminates both an epoch-protected value dereference and a second packed-
  reference decode during verification. Tombstone-only deltas do not allocate
  this sidecar because their tagged handles already encode the base record
  location.
- A one-bit-per-physical-slot bitmap provides an exact unchanged-key fast path.
- Blocked two-bit membership filters need one atomic-word load per route and
  preserve the full exact-key verification contract.
- Truly new arbitrary-length keys use the adaptive atomic index. Live delta
  values use one epoch-protected allocation containing expiry/access metadata,
  a compact 32-bit length, and trailing bytes; this supports variable values up
  to four GiB, removes the separate arena slot plus boxed slice, and saves four
  requested bytes per delta value.
- Retired values up to 4 KiB enter bounded exact-size thread-local free lists.
  A 512 KiB shared exchange moves 128-block batches between writers when epoch
  reclamation and the next allocation occur on different threads. The exchange
  never rounds value sizes or grows without bound.
- Updates replace handles with CAS. Deletes publish tagged immediate handles,
  so successful deletion does not allocate an arena record. No global lock is
  taken by point reads, inserts, updates, or deletes.
- Exclusive two-pass compaction preserves remaining TTL and streams directly
  into the new packed generation without staging copied key/value payloads.
  Its eight-byte build words are compacted backward in place into one
  allocation with contiguous controls followed by seven-byte references; only
  the one-eighth-size control array is temporary.
- A pinned read guard snapshots base and delta time once, giving one request or
  batch a stable expiration view without a monotonic-clock read per hit.
- Saturation falls back to the arbitrary-length concurrent index; a focused
  test forces that path with a one-entry configured delta.
- The opt-in `OnlineMutableSegmentCache` redirects writer stripes into a
  successor delta, packs the stable predecessor while point traffic continues,
  and publishes the equivalent frozen base through two reader epochs. It adds
  fixed per-cache state rather than per-entry metadata. Detailed live-build
  results are in `MUTABLE_SEGMENT_CONCURRENCY.md`.

At a 100,000-entry base-change capacity, the packed override table rounds to
14,293 prime-counted eight-slot buckets:

```text
controls:          14,293 * 8 bytes     =   114,344 bytes
handles:           14,293 * 8 * 8 bytes =   914,752 bytes
                                               ---------
                                               1,029,096 bytes
                                        = 10.291 B/change
lazy live sidecar: 14,293 * 8 * 4 bytes =   457,376 bytes
```

That index is allocated lazily on the first base mutation. It stores no base
key bytes. Delete-only state stays at 10.291 B/change of configured capacity;
the live sidecar raises the table to 14.865 B/change only after a live update,
in exchange for avoiding a second protected pointer chase on updated reads.

## Mutable operation matrix

The single-thread results below are the original design-stage matrix. The
newer accepted implementation and the complete Papaya/Direct multicore matrix
are recorded in [`MUTABLE_SEGMENT_CONCURRENCY.md`](MUTABLE_SEGMENT_CONCURRENCY.md).

These are medians of five fresh processes on the same host. Each process starts
with one million mixed-length keys, 64-byte values, relative TTL, a 100,000-key
delta capacity, and either 100,000 writes or five million reads. Higher Mops/s
is better; lower B/base-entry is better. Insert memory is normalized to the
original one-million-entry population even though 100,000 live keys are added.

| Operation | `MutableSegmentCache` Mops/s | `DirectPackedCache` Mops/s | Packed segment result |
| --- | ---: | ---: | ---: |
| Pristine base hit | 15.765 | **17.624** | 10.5% slower |
| Pristine miss | 16.808 | **42.453** | 60.4% slower |
| Update existing base key | **8.442** | 5.956 | **41.7% faster** |
| Insert new key | **6.976** | 6.259 | **11.5% faster** |
| Delete existing base key | **9.960** | 8.494 | **17.3% faster** |
| Read updated base key | 16.458 | **21.956** | 25.0% slower |
| Read newly inserted key | **22.878** | 16.873 | **35.6% faster** |
| Read deleted key as miss | 17.600 | **46.770** | 62.4% slower |
| Read untouched base after 100k updates | 15.013 | **17.515** | 14.3% slower |
| Read base after 100k new inserts | 14.797 | **17.273** | 14.3% slower |

This historical matrix motivated the later work. It is superseded by the
multicore matrix in `MUTABLE_SEGMENT_CONCURRENCY.md`, where one-allocation
values and guard TTL snapshots materially change both the read and mixed paths.

| State | `MutableSegmentCache` B/base-entry | `DirectPackedCache` B/base-entry | RAM reduction |
| --- | ---: | ---: | ---: |
| Pristine | **96.595** | 107.605 | **10.23%** |
| 100k base updates | **107.400** | 115.885 | **7.32%** |
| 100k new inserts | **109.490** | 118.075 | **7.27%** |
| 100k base tombstones | **101.000** | 108.685 | **7.07%** |

A 90,000-operation mixed churn/compaction sample retained 107,706,872 bytes
before compaction and 96,612,296 afterward. The new packed generation was
95,942,912 bytes, the temporary raw build index was 9,142,912 bytes, and copied
payload staging remained zero. The conservative modeled peak was 212,792,696
bytes, about 1.98x the pre-compaction live bytes. Peak compaction memory is now
the largest density weakness.

Correctness coverage includes exact binary keys/values, expiry, update/delete/
reinsert, eight distinct writers, a contended same-base-key test, packed-index
saturation and fallback, and a 20,000-operation differential test against
`HashMap` with a mid-run compaction.

## Current contract and next proof

The mutable prototype now supports insertion, replacement, deletion, shared
lock-free reads, multi-writer CAS publication, immediate tombstones, TTL
preservation and guard snapshots, exclusive compaction, opt-in online
generation publication, and a multicore Papaya/Direct comparison. It is not
yet a complete production cache because the adaptive recommendation still
needs caller-owned background scheduling integration, long-run latency proof,
and lower peak memory.

The next stages are:

1. wire the adaptive compaction recommendation into caller-owned background
   scheduling and expand maintenance telemetry;
2. reduce the roughly 1.9x compaction peak;
3. add bounded sampled eviction and admission without per-entry LRU nodes;
4. reproduce the current one-, two-, four-, eight-, and sixteen-thread matrix
   on pinned Linux and add percentile latency;
5. optimize the untouched-base and tombstone read paths without adding copied
   key bytes back to base overrides.

Defaulting to online publication should wait until the remaining read-path,
latency, and compaction-peak gaps are explicit; the current result is promising
but not yet a blanket replacement for mature concurrent caches.

## Reproduce

```text
cargo test segment_cache --lib
cargo run --release --example segment_cache_probe -- 1000000 10000000 ttl packed
cargo run --release --example segment_cache_probe -- 1000000 10000000 ttl packed6
cargo run --release --example segment_cache_probe -- 1000000 10000000 ttl segment
cargo run --release --example segment_cache_probe -- 1000000 10000000 no-ttl packed
cargo run --release --example segment_cache_probe -- 1000000 10000000 ttl direct
cargo run --release --example memory_probe -- \
  direct-packed-cache-admission-mixed-value64 1000000
cargo run --release --example mutable_segment_cache_probe -- \
  segment read-updated-hit 1000000 5000000 100000
cargo run --release --example mutable_segment_cache_probe -- \
  direct read-updated-hit 1000000 5000000 100000
cargo run --release --example mutable_segment_cache_probe -- \
  segment compact 1000000 90000 100000
```
