# Mutable segment concurrency proof

Date: 2026-08-06

This document records the first complete multicore proof for
`MutableSegmentCache`. It is a development result, not a claim that the cache
already replaces Papaya or `DirectPackedCache` in every workload.

## Method

The probe is `examples/mutable_segment_concurrency_probe.rs`. Every data point
starts in a fresh process with:

- Apple M4 Max, 16 physical/logical cores, unpinned macOS threads;
- one million resident mixed 8/16/24/32/48-byte binary keys;
- 64-byte values and inline TTL/control metadata;
- synchronized worker start and completion barriers;
- precomputed keys, outside the measured cache allocation region;
- five samples for the final matrix, reported as the median;
- a Papaya control containing the same 64-byte value, weight, expiry, and
  access fields.

Higher Mops/s is better. Lower bytes are better. The Papaya read path loads its
control fields but does not perform expiration comparison or access-bit writes,
so it is a strong optimistic control. Threads are scheduled by macOS and are
not pinned; ranges are retained because some multicore results are noisy.

## Final 16-core operation matrix

The table uses the accepted implementation after packed frozen-control SWAR,
tagged immediate tombstones, prime override geometry, lazy live-location
sidecars, compact one-allocation delta values, bounded exact-size recycling,
per-guard TTL snapshots, and in-place frozen-index packing. Point-read rows
retain the longer accepted runs. The final update and mixed rows are medians of
five fresh-process samples; mixed rows use 50 million operations. The current
distinct insert/delete comparison uses 100,000 operations and a 100,000-record
delta capacity.

| Workload | Mutable segment | Direct packed | Papaya control | Winner |
| --- | ---: | ---: | ---: | --- |
| Pristine hit | **42.283** | 27.591 | 34.914 | Mutable segment |
| Pristine miss | 82.515 | **200.065** | 157.564 | Direct |
| Updated-base hit | 111.605 | 82.516 | **126.593** | Papaya |
| New-key hit | **147.206** | 69.667 | 126.544 | Mutable segment |
| Deleted-key miss | 98.695 | 133.004 | **153.163** | Papaya |
| Untouched hit after updates | 29.910 | 28.297 | **35.550** | Papaya |
| Repeated base update | **5.961** | 3.465 | 2.350 | Mutable segment |
| Distinct inserts | 5.631 | **5.853** | 4.690 | Direct (segment within 4%) |
| Distinct deletes | 9.641 | **15.000** | 12.589 | Direct |
| 95% read / 5% update | **65.309** | 44.330 | 44.022 | Mutable segment |
| 80% read / 20% update | **26.531** | 15.769 | 11.500 | Mutable segment |

This is not a universal throughput win. The mutable segment now leads the
tested high-contention update, pristine-hit, new-key-hit, and both mixed paths.
Distinct insertion is effectively tied with Direct in the current run while
using less retained memory.
Papaya remains the strongest updated and deleted point-read control, although
the segment is now ahead of Direct on updated hits. Direct remains strongest
for pure misses and distinct deletes. Unpinned multicore ranges are noisy—most
notably Direct's mixed runs—so small differences should not be treated as
universal.

A fresh five-process gate after selective segmentation kept Papaya in every
comparison. At 16 unpinned threads, online/Papaya medians were 62.592/63.516
Mops/s for pristine reads (-1.5%, effectively tied), 3.941/3.957 for distinct
inserts (-0.4%, tied), 4.593/2.369 for repeated updates (+93.9%), 124.338/
254.803 for misses (-51.2%), and 4.725/7.061 for distinct deletes (-33.1%).
The host was materially noisier than the preceding gate, in which online led
reads by 5.3% and inserts by 34.0%; the current result therefore does not claim
a small throughput rank. Retained RAM is stable and decisive: online/Papaya
used 103.261/140.494 B/base initially (-26.5%), 115.096/152.654 after inserts
(-24.6%), 113.361/140.550 after updates (-19.3%), and 104.292/128.371 after
deletes (-18.8%).

## Retained memory

The first table reports total retained bytes divided by the original one
million base entries so delta growth is visible directly.

| State | Mutable segment | Direct packed | Papaya control |
| --- | ---: | ---: | ---: |
| Pristine | **103.391 B/base** | 114.423 | 140.494 |
| After 100k base updates | **112.477** | 122.703 | 140.494 |
| After 100k new keys | **115.098** | 125.728 | 152.654 |
| After 100k deletes | **104.420** | 115.503 | 128.334 |
| After 50M operations at 95/5 | **113.237** | 123.204 | 140.536 |
| After 50M operations at 80/20 | **113.496** | 123.099 | 140.538 |
| After 5M repeated updates | **113.499** | 123.018 | 140.550 |

The pristine packed generation is 9.6% smaller than Direct and 26.4% smaller
than the Papaya control. The one-allocation delta removes the old arena-slot
high-water penalty: after sustained 95/5, 80/20, and repeated-update churn, the
segment remains about 7.7--8.1% smaller than Direct and about 19.2% smaller than
Papaya. Periodic compaction is still required to fold arbitrary new keys and
tombstones back into the densest packed representation.

For the one-million-distinct-insert test, there are two million live records at
the end. Mutable segment retains 222.12 MB, or about 111.06 B/live-entry,
below Direct's 113.62 B/live-entry and Papaya's 140.48. Throughput in the same
run was 6.217 Mops/s versus 6.190 Direct and 5.373 Papaya. Folding the
arbitrary-key delta into a packed generation still lowers the final
representation further.

After deleting the complete one-million-key base without compaction, prime
override geometry reduces mutable segment to 115.63 MB versus the prior 124.22
MB. Papaya can physically remove all records and is therefore much smaller in
that deliberately empty end state. Exact base shadowing retains the packed
override index until generation compaction.

## Compaction lifecycle

A separate single-thread fixture uses a slightly shorter mixed-key distribution,
so its absolute B/entry is not directly interchangeable with the matrix above.
It demonstrates the lifecycle:

| Stage | Retained bytes |
| --- | ---: |
| 1M packed base + 1M new-key delta | 207,350,560 |
| Compacted 2M-entry packed generation | 191,885,760 |
| Final packed bytes per live entry | **95.94** |
| Temporary build index | 2,285,720 |
| Conservative rebuild peak | 401,522,040 |

The full-copy compactor streams payload into the builder with zero
copied-payload staging. The raw eight-byte build table is compacted backward in
place into the final split control/reference layout, so only the one-eighth-size
control array is temporary. Its old and new payloads still coexist. Removing
the remaining 2.286 MB scratch alone barely changes that case, and the tested
zero-scratch index builder cut packing throughput by roughly 37%.

Online compaction now has a selective shared-payload path. Immutable predecessor
segments are ranked by retained dead bytes; clean segments are reused while
live records from dirtier segments are copied. Reuse has a hard 64 KiB dead-byte
budget and the finished generation is capped at two payload segments. Initial
payloads split only at valid record boundaries and target 96 MiB, so the common
one-million-entry generation remains flat while larger generations acquire
regions that later compactions can select independently. TTL epoch overflow,
unrepresentable locations, excessive fragmentation, or an unsuitable reuse
plan automatically falls back to a full-copy build.

The existing 40-bit record location is split into an eight-bit local segment
ID and a 32-bit offset, so the seven-byte packed index does not grow. Mutable
overrides identify base records by physical packed slot rather than byte
location; segmented locations use the high location bits and must never spill
into the larger generic override index. The common second segment pointer
remains inline, keeping point-read indirection bounded.

For one million existing records plus 100,000 admitted records, five-pair
fresh-process executable A/B medians were:

| Metric | Full copy | Shared payload | Result |
| --- | ---: | ---: | --- |
| Modeled peak | 212.024 MB | **128.024 MB** | **39.6% lower** |
| Post-compaction retained | 104.017 MB | 104.017 MB | Flat (+16 bytes) |
| Total compaction | 168.455 ms | **118.041 ms** | **29.9% faster** |
| Mixed post-compaction reads | 5.234 Mops/s | 5.234 Mops/s | Flat |

The replacement reused 1,000,000 records / 83.999964 MB, copied 8.400021 MB,
and allocated 18.457173 MB for its new index plus copied records. Peak modeling
therefore uses `newly_allocated_generation_bytes`, not the replacement's total
logical retained size. A second segment pointer is stored inline because the
two-segment case is the normal admission-compaction result; this recovered the
initial roughly 4% mixed-read regression seen with an out-of-line segment
directory.

Selective reuse is not restricted by the percentage of the whole table that
changed. The probe first admits 100,000 records to create a two-segment
1.1-million-entry base, then updates or deletes that complete localized segment
(9.1% of the base). Five fresh-process medians compare the same two-segment
predecessor with selective reuse enabled or disabled:

| Second compaction | Full copy | Selective reuse | Result |
| --- | ---: | ---: | --- |
| Localized 100k updates, modeled peak | 217.534 MB | **133.534 MB** | **38.6% lower** |
| Localized 100k updates, total time | 171.112 ms | **110.967 ms** | **35.1% faster** |
| Localized 100k deletes, modeled peak | 200.359 MB | **116.359 MB** | **41.9% lower** |
| Localized 100k deletes, total time | 160.761 ms | **104.308 ms** | **35.1% faster** |

The update rebuild reuses 1,000,000 records / 83.999964 MB and copies 100,000
records / 8.400021 MB. The delete rebuild reuses the same clean million records
and copies no record payload. Steady retained bytes remain 104.029 MB after the
updates and 94.688 MB after the deletes. Randomly distributed churn still
falls back to a flat payload, because keeping fragments of dirty segments would
amplify steady RAM.

Both the sizing and construction scans now use the base-change bitmap as an
exact negative proof. When no predecessor-layer new record can shadow the
frozen base, a live unmarked physical slot is known unchanged and bypasses key
hashing plus all mutable-index probes. Every direct mutation marks the slot
before publishing its value. Expired records and generations with possible
new-index shadowing conservatively retain the complete lookup path. This scan
proof is responsible for the larger absolute timing reduction: admission fell
from 286.104 to 139.928 ms, localized update from 269.433 to 134.866 ms,
localized delete from 254.449 to 124.231 ms, random-update full copy from
259.035 to 170.530 ms, and random-delete full copy from 251.651 to 161.921 ms.
Post-admission point reads remained flat at 5.245 versus 5.223 Mops/s.

Deleting the complete one-million-entry base and compacting leaves 1,406,976
bytes for the empty generation and pre-sized future-delta metadata.

## Accepted optimizations

| Change | Before | After | Decision |
| --- | ---: | ---: | --- |
| Exact word-at-a-time frozen control matching, pristine hit (1T) | 3.860 | 4.246 Mops/s | Keep, +10.0% |
| Exact word-at-a-time frozen control matching, miss (1T) | 12.734 | 17.209 Mops/s | Keep, +35.1% |
| Immediate tombstone, deleted read (1T) | 6.442 | 11.603 Mops/s | Keep, +80.1% |
| Immediate tombstone, delete (8T) | 6.419 | 9.633 Mops/s | Keep, +50.1% |
| Immediate tombstone, full-delete RAM | 156.353 | 124.219 B/base | Keep, -20.6% |
| Rotated batched cross-partition reuse, repeated update RAM (16T) | 157.019 | 134.663 B/base | Keep, -14.2% |
| Same reuse, 95/5 RAM (16T) | 132.815 | 122.339 B/base | Keep, -7.9% |
| Same reuse, 80/20 RAM (16T) | 186.170 | 138.975 B/base | Keep, -25.4% |
| Prime override buckets, full-delete RAM | 124.219 | 115.631 B/base | Keep, -6.9% |
| Lazy live-slot sidecar, updated hit (1T) | 6.362 | 7.295 Mops/s | Keep, +14.7% |
| Lazy sidecar, delete-only RAM | 115.631 | 115.631 B/base | Keep, no cost |
| One-allocation delta, 100k-update RAM | 111.025 | 109.986 B/base | Keep, -1.04 MB |
| One-allocation delta, 1M-new RAM | 242.330 | 226.191 MB | Keep, -16.14 MB |
| One-allocation delta, update (16T) | 3.472 | 4.388 Mops/s | Keep, +26.4% |
| One-allocation delta, 95/5 long run (16T) | 29.447 | 35.468 Mops/s | Keep, +20.4% |
| Guard TTL snapshot, updated hit (16T) | 42.433 | 65.600 Mops/s | Keep, +54.6% |
| Guard TTL snapshot, new-key hit (16T) | 74.978 | 100.513 Mops/s | Keep, +34.1% |
| Store direct record location in live sidecar, updated hit (1T) | 10.354 | 14.259 Mops/s | Keep, +37.7% |
| Same record-location sidecar, updated hit (16T A/B) | 52.011 | 62.221 Mops/s | Keep, +19.6% |
| Same record-location sidecar, 95/5 (1T) | 9.170 | 10.739 Mops/s | Keep, +17.1% |
| Encode record location in tombstone, deleted hit (1T) | 10.869 | 14.915 Mops/s | Keep, +37.2% |
| Compact 12-byte value header, 100k updated values | 112.877 | 112.477 B/base | Keep, -0.40 MB |
| Same compact header, 1M new values before compaction | 211.351 | 207.351 MB | Keep, -4.00 MB |
| Exact-size local recycler, repeated update (1T A/B) | 3.696 | 4.691 Mops/s | Keep, +26.9% |
| Same local recycler, 95/5 (16T A/B) | 52.390 | 68.800 Mops/s | Keep, +31.3% |
| 512 KiB cross-thread batch exchange, update (16T) | 6.509 | 10.035 Mops/s | Keep, +54.2% |
| Same exchange, 80/20 (16T) | 23.149 | 30.025 Mops/s | Keep, +29.7% |
| Backward in-place frozen-index packing, temporary index | 18.286 | 2.286 MB | Keep, -87.5% |
| Same in-place packing, modeled compaction peak | 417.522 | 401.522 MB | Keep, -16.00 MB |
| Same in-place packing, pristine/miss read gate (1T) | 6.612 / 17.886 | 6.587 / 17.903 Mops/s | Keep, flat |
| One-byte record-header fast path plus direct seven-byte reference decode, same-layout pristine hit (1T) | 4.751 | 5.063 Mops/s | Keep, +6.6% |
| Validated SWAR override lookup, updated hit (1T executable A/B) | 14.331 | 19.154 Mops/s | Keep, +33.7% |
| Same override lookup, deleted hit (1T executable A/B) | 16.245 | 24.692 Mops/s | Keep, +52.0% |
| Tombstone-only SWAR insertion, distinct delete (1T executable A/B) | 6.658 | 7.527 Mops/s | Keep, +13.1% |
| Same tombstone insertion, distinct delete (16T executable A/B) | 9.967 | 11.467 Mops/s | Keep, +15.0% |
| Inline common tombstone key span, deleted hit (1T executable A/B) | 23.542 | 26.742 Mops/s | Keep, +13.6% |
| Adaptive pinless first-base delete (1T executable A/B median) | 7.131 | 7.377 Mops/s | Keep, +3.5% |
| Same delete with 16 writer batches | 10.343 | 10.466 Mops/s | Keep, +1.2%; concurrent batches retain the pinned path |
| Lazy unused new-key filter, 1M delta budget | 95.741 | 94.692 MB | Keep, -1.049 MB before any new-key write |
| Same filter, empty post-compaction generation | 2.456 | 1.407 MB | Keep, -42.7% |
| Thin lazy-filter owner, scalar delete (10-run median) | 7.376 | 7.426 Mops/s | Keep, +0.7%; no write-path regression |
| Shared payload, 1M base + 100k admissions modeled peak | 212.024 | 128.024 MB | Keep, -39.6% |
| Same shared payload, final compaction time | 168.455 | 118.041 ms | Keep, +29.9% |
| Inline second segment, mixed post-compaction reads | 5.234 | 5.234 Mops/s | Keep, flat; removed initial ~4% regression |
| Selective clean-region reuse, localized 100k-update peak | 217.534 | 133.534 MB | Keep, -38.6% with 9.1% global churn |
| Same localized update, same-source compaction time | 171.112 | 110.967 ms | Keep, +35.1% |
| Selective clean-region reuse, localized 100k-delete peak | 200.359 | 116.359 MB | Keep, -41.9% |
| Same localized delete, same-source compaction time | 160.761 | 104.308 ms | Keep, +35.1% |
| Unchanged-live-slot scan proof, admission compaction | 286.104 | 139.928 ms | Keep, +51.1%; output bytes identical |
| Same scan proof, random update full-copy fallback | 259.035 | 170.530 ms | Keep, +34.2% |
| Same scan proof, random delete full-copy fallback | 251.651 | 161.921 ms | Keep, +35.7% |
| Same scan proof, post-admission reads | 5.245 | 5.223 Mops/s | Keep, flat (-0.4%) |
| Reuse exact frozen header size once per sizing scan, admission | 137.452 | 133.279 ms | Keep, +3.0%; delete sweeps flat-to-positive across reversed order |
| One packed-control load per physical bucket, admission | 139.109 | 120.132 ms | Keep, +13.6% |
| Same bucket traversal, localized update/delete | 136.807 / 122.482 | 111.940 / 103.497 ms | Keep, +18.2% / +15.5% |
| Same bucket traversal, random update/delete fallback | 171.242 / 165.566 | 150.863 / 146.487 ms | Keep, +11.9% / +11.5% |
| Same bucket traversal, post-admission reads (reversed seven-pair gate) | 5.245 | 5.247 Mops/s | Keep, flat |

The final reuse comparison varied with interleaving, but repeated five-sample
checks kept update throughput within about 1--2% of the old path and improved
some mixed traces. The memory reduction is the reliable reason to retain it.

The operation-phase allocation counters make the newer recycler result easier
to interpret. In the final five-million-update run the segment made a median
1.31 million allocator calls, versus 5.02 million for Direct and 10.31 million
for Papaya. At 95/5 it made 0.87 million calls for 2.5 million writes, versus
2.51 million Direct and 5.16 million Papaya. Lower allocation count does not
guarantee lower CPU by itself, but here the controlled recycler-on/off A/B also
improved throughput.

## Online generation publication

`OnlineMutableSegmentCache` is now an opt-in implementation beside the
unchanged exclusive-compaction baseline. Its rebuild protocol is lossless:

1. publish a successor whose logical base is the current generation;
2. redirect equal-key writers stripe by stripe, draining each predecessor
   stripe before the successor owns it;
3. pack the now-stable predecessor while reads and writes continue;
4. publish an exactly equivalent frozen base under the still-live successor
   delta, with no changed-key replay;
5. flip readers to a second epoch so new zero-copy guards continue immediately
   while pre-publication guards drain;
6. enable direct packed-base writes per stripe only at zero active writers.

The value arena is shared across generations, so guards can continue returning
borrowed byte slices without cloning. `ArcSwap` protects generation/base
publication, while the existing epoch collector protects mutable value
allocations. Failed packing leaves a correct predecessor-backed successor that
a later compaction can flatten.

The online wrapper adds about 712 fixed retained bytes in the current
one-million-entry fixture: 103.394 versus 103.393 B/base-entry. It does not add
per-entry metadata. The following final confirmation rows are one fresh process
each, not the five-sample final matrix above; they establish that the accepted
fast paths did not impose a universal online tax, but macOS scheduling remains
too noisy for small rank differences.

| Workload | Online segment | Exclusive segment | Papaya control | Operations |
| --- | ---: | ---: | ---: | ---: |
| Pristine hit | 35.158 | 39.002 | **44.036** | 50M |
| Repeated base update | **10.324** | 9.289 | 2.252 | 5M |
| Distinct new insert | 8.107 | **8.245** | 4.226 | 1M |
| Distinct delete | 6.427 | 6.317 | **7.582** | 1M |
| 95% read / 5% update | **59.228** | 52.586 | 33.694 | 50M |
| 80% read / 20% update | **32.406** | 24.952 | 9.895 | 50M |

Higher Mops/s is better. Online is within 2% of the baseline on million-key
insertion, leads both mixed rows and repeated updates in these confirmations,
and remains behind Papaya on the pure-read and physical-delete rows. The
million-key delete end state also illustrates a semantic RAM difference:
Papaya physically removes the now-empty population, whereas the segment keeps
compact tombstones until online compaction folds them away.

After the packed-record decoder and validated full-byte SWAR override lookup,
five fresh 16-thread processes per backend produced these targeted medians:

| Workload | Online segment | Papaya control | Online relative result |
| --- | ---: | ---: | ---: |
| Updated-key read | **155.950 Mops/s** | 153.542 Mops/s | +1.6% |
| Repeated base update | **7.868 Mops/s** | 2.542 Mops/s | 3.10x |
| Distinct delete | 9.919 Mops/s | **10.999 Mops/s** | -9.8% |
| Deleted-key read | 108.472 Mops/s | **152.871 Mops/s** | -29.0% |

The macOS multicore rows remain highly variable, but the same-executable 1T
controls isolate the implementation changes: updated override reads improved
33.7%, deleted reads 52.0%, and tombstone publication 13.1%. Lower RAM remains
the online segment's consistent advantage: 112.480 versus 140.494 B/base-entry
after 100k updates, and 104.423 versus 128.334 after 100k deletes.

The subsequent zero-byte tombstone-key-span encoding uses the 22 previously
unused immediate-handle bits for the common record-header offset and key
length. It preserves the record location for exact reinsertion identity and
falls back to header decoding for keys above 131,070 bytes or unusual headers.
Seven alternating one-thread processes put the improved deleted-key read at
26.098 Mops/s versus Papaya's 27.834 (-6.2%), while retaining 104.421 versus
128.334 B/base-entry (-18.6%).

Mutation-heavy callers can use `pin_writer_batch()`. It retains one generation
reservation but enters the value epoch separately for each mutation, avoiding
the recycler damage caused by one long value pin. Holding a read/value guard
over 4,096 pure updates was tested and rejected: allocator calls rose sharply
and throughput fell. Mixed request batches may use the read guard for both
reads and writes, but should keep it short because any zero-copy value guard
necessarily delays value reclamation.

An untouched frozen-base delete publishes an immediate tombstone and normally
does not dereference or retire an arena value. The online path now avoids the
value-epoch entry for that case. A writer batch takes this shortcut only while
it is the sole active batch in its read epoch; concurrent batches automatically
use the previous pinned path. Seven alternating processes kept the 16-thread
median flat-to-positive instead of the 5.1% regression from unconditional pin
elision. If a racing update wins publication first, the deleting writer
acquires a pin after its successful replacement, checks the displaced value,
and assumes sole responsibility for retiring that now-unreachable handle.

The corresponding final Papaya delete comparison remains workload-shaped. In
one seven-process batch, online PackedGen reached 7.690 Mops/s versus Papaya's
14.333 at one thread, but 10.558 versus 8.742 at 16 threads. Higher throughput
is better. The full-delete end state is not an equal memory comparison: Papaya
physically removes the million records and retained 37.8 MB, whereas the
uncompacted segment retained 115.6 MB including tombstones; adaptive compaction
reduces the empty segment to roughly 2.46 MB.

### Monotonic routing writes and sparse overflow misses

The delta's dirty flags and routing bits are monotonic. Publishing `true` or
performing an atomic OR on every mutation made otherwise independent writers
fight over state that was already set. Dirty flags now use a load-first,
release-once path; exact change and membership words use load-first OR. The
exact mutable index/control publication remains authoritative, so a stale
routing hint can only cause an exact fallback probe. Seven 16-thread processes
moved repeated update throughput from the preceding 12.898 to 14.969 Mops/s
(+16.1%); together with the dirty-flag change this is about +41% over the
original 10.607 Mops/s median, with no per-entry RAM cost.

Lower frozen-bucket occupancy remains available as a general speed/RAM knob.
At one million entries, 6/8 occupancy added 1.548 B/base entry and improved the
screened miss median about 54%; 5/8 added 3.714 B. A more targeted optional
filter gives a better density point. It is checked only after the first two
packed buckets are full and contains only keys actually displaced beyond that
prefix. One requested bit per base entry therefore costs exactly 125,000 bytes
at one million entries (+0.125 B/entry), while positive answers still use the
exact index and key comparison. The filter is rebuilt by both flat and
shared-payload compaction and has a focused no-false-negative test.

The final seven-process 16-thread gate used
`with_negative_filter_bits_per_entry(1)`. Values are medians; higher Mops/s is
better and lower B/entry is better.

| Workload | Online + sparse filter | Papaya control | Online relative result |
| --- | ---: | ---: | ---: |
| Pristine hit | 52.365 Mops/s | **55.901 Mops/s** | -6.3% |
| Miss | 197.425 Mops/s | **233.333 Mops/s** | -15.4% |
| Repeated base update | **14.771 Mops/s** | 2.408 Mops/s | +513% |
| Distinct new insert | **6.172 Mops/s** | 4.508 Mops/s | +36.9% |
| Distinct delete | 11.404 Mops/s | **14.003 Mops/s** | -18.6% |
| 95% read / 5% update | **55.694 Mops/s** | 43.754 Mops/s | +27.3% |

The base uses 103.386 versus 140.494 B/entry (-26.4%). After the measured
update, insert, and delete phases it uses 113.330/115.208/104.417 B/base entry
versus Papaya's 140.550/152.654/128.371 (-19.4%/-24.5%/-18.7%). The dense
zero-filter configuration remains the default because a very high-hit cache
may prefer its last 0.125 B/entry and shortest possible hit path; the sparse
filter is the balanced miss-heavy profile.

### Live compaction sample

`examples/online_segment_compaction_probe.rs` runs one online compaction under
continuous 95/5 traffic. With one million entries, 100,000 prepared updates, a
200,000-record delta budget, and 16 workers, three fresh processes produced:

| Metric | Median | Meaning |
| --- | ---: | --- |
| Writer redirect | 1.688 ms | Old read/write batches and writer stripes drain; point work routes to the successor |
| Background packed build | 704.785 ms | Old stable generation is scanned and packed while point traffic continues |
| Base publication / old-reader drain | 12.389 ms | New readers already use the second epoch; this is not a global read pause |
| Total compaction | 718.009 ms | End-to-end maintenance call |
| Point throughput during compaction | 19.927 Mops/s | Higher is better; includes memory-bandwidth contention with the builder |

With traffic disabled, the same fixture retained 104,128,316 bytes before
compaction and 94,687,948 afterward. The new packed base was 93,142,876 bytes,
the temporary build index was 1,142,864 bytes, and the conservative modeled
peak was 198,414,056 bytes. Live traffic naturally leaves new successor-delta
records after publication, so its post-compaction resident set depends on how
many writes occur during the build.

### Adaptive compaction recommendation

`compaction_threshold_records()`, `compaction_recommended()`, and
`compact_if_recommended()` now expose a size-independent maintenance policy.
The trigger is the lower of 75% of the configured delta budget and roughly
12.5% of the packed-base population, with a 1,024-record density floor and a
special case for an initially empty base. The recommendation uses unique
overlay records rather than raw operation count, so repeated hot-key updates
do not cause rebuild loops. `compact_if_recommended()` checks again under the
rebuild mutex and therefore cannot queue redundant compactions.

In the one-million-entry, one-million-delta fixture, 125,000 distinct deletes
hit the density trigger. Retained bytes fell from 104,978,756 before compaction
to 83,031,876 afterward, with exactly 875,000 live packed records. Deleting all
one million entries and compacting fell from 104,978,756 to 1,406,976 bytes,
reclaiming about 98.7% of the cache allocation. This is why the 114.581
B/base full-delete number is a pre-maintenance state rather than the
post-compaction memory floor.

Correctness tests now cover a held pre-publication guard concurrently with a
new post-publication guard, concurrent distinct writers across redirect,
repeated compactions under update/delete churn, retry-capable predecessor
layers, TTL preservation without deadline restart, repeated append-only
segment sharing, automatic full-copy fallback under update churn, and ordering
between pre-publication new-index writes and later segmented-base overrides.

## Rejected experiments from this pass

| Experiment | Result | Decision |
| --- | --- | --- |
| Earlier low-seven-bit word matching in the mutable override table | Updated hit 6.001 -> 4.829 Mops/s | Revert; superseded by validated full-byte candidates plus exact-byte verification |
| Non-exact subtract-based per-byte mask | Could flag adjacent bytes and caused invalid handle reads in a large preparation trace | Replace with exact low-seven-bit addition mask and exhaustive byte test |
| Segment-specific epoch batch of 64 | Pure-update RAM rose to 185.752 B/base and 95/5 throughput fell | Revert |
| One-at-a-time cross-partition stealing | RAM improved, but update fell to 3.450 and 95/5 to 22.598 Mops/s | Replace with batched transfer |
| Batched stealing without per-writer rotation | All writers selected the lowest partition and 95/5 remained 21.094 Mops/s | Replace with rotated availability mask |
| Override-first update with two table scans | Duplicate mutable probes outweighed the saved base work | Revert |
| Override-first direct-CAS update | Correct but no repeatable improvement outside noise | Revert |
| Lazy secondary stride | Regressed all measured override paths | Revert |
| Low-byte or high-byte shortcut fingerprint | Update fell from about 3.09 to 2.54--2.56 Mops/s | Revert; insufficient full-width mixing |
| Multiply-high fingerprint sharing primary reduction | Updated hit fell 10.642 -> 4.903 Mops/s | Revert; bucket/tag correlation destroys filtering |
| Minimum-capacity exact overflow reserve | Saved only 0.049 B/base and reduced updated-read headroom | Revert; adaptive reserve was already mostly lazy |
| Second membership bit from raw high hash bits | Flat at 1T; mixed gains did not repeat and new/untouched paths regressed | Revert; retain stronger independent mixing |
| Unconditional changed-base membership bypass | Changed-key hits improved 0.8--3.8%, but untouched hits fell 12.6% | Revert; cold-key cost is too high |
| Membership bypass retested after SWAR override lookup | Updated +7.9%, but deleted -3.2% and untouched -3.5% | Revert; SWAR narrows but does not remove the universal-read tradeoff |
| Guard-adaptive membership bypass after 64 samples | Changed-key hits improved 2--4%, but the 16T 95/5 mix fell about 6% | Revert; workload-local routing state is not robust enough |
| Sampled base-first routing in each online guard | Untouched reads +3.56%, but pristine -2.91% and updated reads -1.10% | Revert; not a universal cache improvement |
| SWAR insertion for live base updates | 1T improved, but 16T update fell 11.321 -> 10.322 Mops/s | Revert for live values; retain only the tombstone specialization |
| Key/TTL-only frozen mutation probe | Delete improved 1.8% at 1T and 5.8% at 16T, but long-run update fell 1.2% and 2.2% | Revert; duplicate probe machinery was not a universal mutation win |
| One-check unsafe common record decode | Pristine read -0.6%, update -1.4%, delete -5.2% | Revert; safe bounds checks were already optimized better than the added branch/code shape |
| Make both delta membership filters lazy | Saved 2.097 MB at a 1M budget, but updated reads fell 1.6% and deleted reads about 5% | Revert for base changes; retain laziness only for the unused new-key filter |
| Reuse one long-lived read guard for writes | 95/5 fell about 13% and 80/20 about 9% | Revert; concentrated retirement delayed reclamation |
| 256 KiB rather than 512 KiB shared exchange | Saved about 0.2 MB, but update and 80/20 each fell about 5% | Revert; 512 KiB is the better Pareto point |
| 256- rather than 128-block transfers | Update improved, but 80/20 fell about 6% | Revert; retain the balanced batch size |
| Interleave controls and references in each physical bucket | Pristine hits improved about 4%, but pure misses fell about 13% | Revert; contiguous controls are better for negative scans |
| Zero-temporary seven-byte build table | Initial build rose from about 90 to 214 ms and full compaction fell about 37% | Replace with backward in-place packing plus a 1/8-size control array |
| Out-of-line directory for the common second payload segment | Mixed reads after admission compaction were about 4% below the flat-generation control | Replace with an inline second-segment pointer; final six-run medians are equal |
| Permit 2% retained payload waste | Random 100k updates retained about 1.61 MB extra after compaction without a compaction-time gain | Revert; use a hard 64 KiB dead-byte budget |
| Automatic 64 MiB payload regions with up to four retained segments | Pristine-hit median fell about 5.5% | Revert; target 96 MiB and cap selective output at two segments |
| Special-case the one-segment planner and remove apparently redundant scan accounting | Seven interleaved admission medians were 311.191 versus 299.613 ms for the prior planner, 3.9% worse | Revert; fewer source operations did not improve the optimized executable |
| Full pre-index negative filter | Miss throughput improved sharply, but the extra random load cut hit/update-heavy rows by 30% or more in the broad matrix | Reject placement; probe packed buckets before consulting a filter |
| Filter every key after the primary bucket | At +0.125 B/entry, misses improved about 52%, but pristine hits and the 95/5 mix moved about -4.5%/-3.3% in the median screen | Supersede with the two-bucket, displaced-key-only filter |
| Power-of-two negative-filter sizing | Six requested bits rounded to 8.39 physical bits/entry | Replace with exact word count and multiply-high reduction; six bits now cost 0.75 B/entry |
| Reuse one read/value pin across a 4,096-delete batch | Nine-process median 11.391 versus 11.404 Mops/s for the existing writer batch | Revert; tombstone/index publication, not epoch entry, is the remaining delete cost |

## Correctness and validation

The focused suite contains segment-cache tests, including exact binary
round trips, expiry, update/delete/reinsert, eight distinct writers, same-key
contention, packed-index saturation fallback, compaction TTL preservation, and
a 20,000-operation differential trace against `HashMap`. A dedicated test
proves pinned TTL snapshot semantics. The exact SWAR mask is exhaustively
checked for all byte values. A four-thread allocator stress test forces more
than 65,000 variable-size retire/reuse operations. A 256-key race test checks
both legal linearizations when a pinless delete and update publish against the
same untouched base key. Eight concurrent writers also verify that lazy
new-key-filter initialization publishes one complete filter without losing a
record. The final validation passed 83 all-feature library
unit tests plus every integration target (one documented
long soak remains ignored by default), and all-target/all-feature Clippy with
warnings denied.

## Next production milestone

Online publication is implemented and stress-tested, but it remains opt-in
until the following gates are complete:

1. Linux pinned-core throughput and percentile-latency reproduction;
2. adaptive background scheduling before delta churn erases the density lead;
3. cancellation/backpressure and observability for long rebuilds;
4. add a maintenance time budget for rebuilds that compete with foreground
   memory bandwidth;
5. Miri/Loom-style focused validation of publication and retirement edges;
6. a longer randomized differential soak with repeated concurrent rebuilds.

## Reproduce

```text
cargo build --release --example mutable_segment_concurrency_probe
./target/release/examples/mutable_segment_concurrency_probe \
  segment read95-update5 1000000 20000000 16 100000
./target/release/examples/mutable_segment_concurrency_probe \
  direct read95-update5 1000000 20000000 16 100000
./target/release/examples/mutable_segment_concurrency_probe \
  papaya read95-update5 1000000 20000000 16 100000
./target/release/examples/mutable_segment_concurrency_probe \
  online read95-update5 1000000 50000000 16 100000 7 1

cargo run --release --example online_segment_compaction_probe -- \
  1000000 100000 16 200000 insert 20000000
cargo run --release --example online_segment_compaction_probe -- \
  1000000 100000 0 200000 localized-update 0
cargo run --release --example online_segment_compaction_probe -- \
  1000000 100000 0 200000 localized-delete 0

cargo run --release --example mutable_segment_cache_probe -- \
  segment compact-new 1000000 1000000 1000000
cargo run --release --example mutable_segment_cache_probe -- \
  segment compact-delete 1000000 1000000 1000000
```
