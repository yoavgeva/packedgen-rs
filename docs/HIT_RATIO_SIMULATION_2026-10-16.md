# Direct cache hit ratio: simulation, diagnosis, and policy fix

> **Update, October 17:** the real-trace gap below was not missing frequency
> admission. It came from the ghost filter's window, single access marks, a
> fixed victim window, and the default index overlay. See
> [HIT_RATIO_REAL_TRACES_2026-10-17.md](HIT_RATIO_REAL_TRACES_2026-10-17.md).

The direct cache's default policy missed about 25% more reads than QuickCache
on the bounded-cache turnover trace and fell below exact LRU on every skewed
trace. A deterministic simulator traced this to three causes. The fixes bring
the default within 0.1 point of QuickCache on turnover at 50,000 entries, and on
turnover and Zipf 0.9 at 262,144 entries, with no consistent throughput cost.
One pathological loop trace regressed.

The synthetic parity does not carry over to real workloads. On six ARC-paper
traces the new default is far better than the original policy: 1.5x to 4.6x
the hit rate wherever the original was below 30%. But it reaches QuickCache
in only 3 of 18 cache sizes. It is typically 1 to 4 points behind, and 9.7
points behind on DS1 at 4,000,000 entries.

These are simulator and screening results on an unisolated host. They don't
qualify a release.

## Method

`examples/hit_ratio_sim.rs` replays a fixed trace on one thread through:

- the direct cache, in every admission mode,
- QuickCache 0.7's `unsync` cache with a fixed hasher,
- exact LRU,
- Belady's OPT with bypass, which bounds every online policy,
- the reference models `model-clock`, `model-s3clock` and `model-s3fifo`.

Read misses fill through `insert_if_absent`, so admission applies as it would
in a look-aside cache. Writes are upserts. Every record has the same charge, so
each policy holds at most `ENTRIES` records. The packed sessions keep
`bounded_cache_probe`'s cadence: they refresh the guard every 512 requests and
call `maintain()` every 4,096 writes. Hit ratios count reads only.

The traces:

- `turnover`: the probe's one-worker trace, preload included. 95% of requests
  are reads, 80% of those go to a hot tenth of the keys, the rest are spread
  over twice the capacity, and 5% are writes of new keys never read again.
- `zipf:ALPHA`: reads over ten times the capacity.
- `scan-zipf:0.9`: like zipf 0.9, but every four capacities it reads half a
  capacity of keys that never return.
- `shift:0.9`: like zipf 0.9, but the popular keys change halfway through.
- `loop`: a cyclic scan over 1.25 times the capacity.

The simulator reproduces the probe's one-worker turnover hit rates:
PackedGen 87.48% (probe 87.36%) and QuickCache 89.89% (probe 89.97%). The gap
therefore lies in the eviction policy, not in concurrency. PackedGen also held
49,978 of its 50,000 slots on average, so lost capacity is not the cause
either.

Each cache seeds its index hasher randomly, which changes the order of the
eviction sweep. PackedGen results therefore vary by up to about 0.25% of reads
between identical replays at 256 entries, and much less at larger sizes. The
reference policies replay exactly.

## Diagnosis

1. **The default was CLOCK with one in sixteen hits marked.**
   `model-clock/sample=16` reproduces the default within about 0.3 point on
   every trace except the loop. Marking every hit lifts CLOCK to LRU level, and
   LRU is the most this design can reach. QuickCache's CLOCK-Pro-like policy
   with ghost history stays about 7 points higher on Zipf traces.
2. **Keys written once and never read displaced reused keys for a full sweep.**
   On turnover, scan writes occupy about a third of the non-hot slots under
   CLOCK. S3-FIFO's small FIFO plus ghost history removes them quickly.
   `model-s3clock` uses a one-bit CLOCK main area and a 2% small FIFO. It is
   within 0.9 point of QuickCache on turnover and level with it or ahead on the
   Zipf, scan and shift traces.
3. **Large caches evicted almost at random.** The proactive removal target is
   capacity / 1024, clamped to the 64-entry victim window. From 65,536 entries
   upward, each round evicted every candidate it sampled, marked or not. At
   262,144 entries with probation off, the default scored 86.17% on turnover
   and 60.05% on zipf 0.9, against exact LRU's 87.81% and 62.98%.

Several experiments changed nothing. Widening the victim window alone made no
difference while marks were sampled, because nearly every candidate was
unmarked. Calling `maintain()` every 65,536 writes instead of every 4,096 also
made no difference. Never calling it cost 3 to 7 points. Keeping a key's mark
when it leaves probation instead of clearing it made no difference either.

## Changes

- **Probation.** New keys wait in a FIFO sized at 2% of the population, set by
  `with_probation`. Eviction takes the oldest probationary key first. A key read
  since publication stays resident under the sweep. An unread key is evicted
  and its route hash is remembered in a two-generation ghost filter: 16 bits per
  remembered key, three bits set, about two bytes per expected entry. A
  remembered key that returns skips probation. Records are revalidated against
  the index when popped. The FIFO is capped near its share of the population, so
  the cache never keeps a second copy of every key. Small caches use fewer
  shards so that round-robin pops stay close to FIFO order.
- **Every hit marks.** Only the optional frequency sketch is still sampled.
- **Selective sweeps.** Each eviction round samples at least four candidates
  per removal. Repeated samples are merged by sorting instead of a quadratic
  scan.

Three new failing-first tests guard these: a one-shot scan of 4x capacity
(0 of 999 reused keys survived before, 944 to 983 across 1,000 runs after), a
single read during probation (evicted 15 times in 16 before), and a
131,072-entry sweep (14,097 of 65,536 read entries lost before, under 1%
after). The simulator gate now requires the default to beat exact LRU on four
traces and to miss at most 6% more than QuickCache on three.

## Results

Read hit rate in percent, 50,000 entries, default config. Turnover uses
1,000,000 requests; the other traces use 2,000,000.

| Trace | Original | Probation only | Final | QuickCache 0.7 | Exact LRU | OPT |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| turnover | 87.47 | 89.86 | **89.92** | 89.89 | 87.75 | 95.09 |
| zipf 0.7 | 32.69 | 38.91 | **41.15** | 42.87 | 35.09 | 59.30 |
| zipf 0.9 | 59.78 | 64.80 | **66.53** | 67.53 | 62.34 | 76.36 |
| zipf 1.1 | 85.75 | 87.34 | **87.94** | 88.09 | 86.91 | 90.78 |
| scan-zipf 0.9 | 49.62 | 54.81 | **56.51** | 57.65 | 51.72 | 66.54 |
| shift 0.9 | 59.24 | 60.98 | **62.47** | 63.01 | 61.83 | 73.87 |
| loop | 58.00 | 64.40 | **54.75** | 63.84 | 0.00 | 77.50 |

At 262,144 entries and 3,000,000 requests, the final default scores 89.92% on
turnover (QuickCache 89.85%) and 64.66% on zipf 0.9 (QuickCache 64.76%).

The loop regression is real. With selective sweeps, a cyclic scan just larger
than the cache behaves like CLOCK, which is LRU's worst case, instead of like
random eviction. `packed-tiered` keeps 67.73% on the loop.

## Real traces

The traces are six from the ARC paper: N. Megiddo and D. S. Modha, "ARC: A
Self-Tuning, Low Overhead Replacement Cache," FAST 2003. The copies come from
the `arc` directory of `github.com/moka-rs/cache-trace`; the trace files are
not committed here. Each `.lis` line `START COUNT ...` reads `COUNT`
consecutive blocks, and `arc:PATH` expands it the same way. The expanded
request counts match the paper's table exactly. On OLTP at the paper's cache
sizes, the simulator's exact LRU reproduces the paper's published LRU column:
32.83%, 42.47%, 53.65%, 60.70%, and 64.63%.

Read hit rate in percent. Original is the policy before this work, run with
the simulator at `c3e769d`. Final is the default after all the changes here.

| Trace | Entries | Original | Final | Tiered | QuickCache | LRU | OPT | Final vs QuickCache |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OLTP | 1,000 | 30.00 | 35.43 | 26.00 | 35.15 | 32.83 | 53.62 | +0.28 |
| OLTP | 5,000 | 48.87 | 52.94 | 46.94 | 54.29 | 53.65 | 68.27 | -1.36 |
| OLTP | 15,000 | 61.72 | 63.22 | 58.03 | 64.83 | 64.63 | 75.14 | -1.61 |
| P3 | 8,192 | 1.58 | 4.16 | 3.84 | 3.64 | 1.62 | 11.25 | +0.52 |
| P3 | 32,768 | 5.04 | 14.41 | 13.78 | 12.94 | 3.56 | 32.25 | +1.48 |
| P3 | 131,072 | 39.08 | 46.50 | 44.42 | 48.61 | 44.79 | 67.51 | -2.11 |
| S1 | 65,536 | 1.56 | 5.80 | 5.69 | 6.17 | 1.56 | 18.14 | -0.37 |
| S1 | 262,144 | 8.93 | 23.15 | 23.53 | 23.77 | 6.43 | 42.50 | -0.62 |
| S1 | 524,288 | 27.60 | 40.24 | 41.63 | 41.99 | 23.71 | 59.01 | -1.74 |
| P8 | 8,192 | 2.83 | 8.43 | 9.30 | 10.40 | 2.30 | 23.22 | -1.96 |
| P8 | 32,768 | 17.14 | 28.47 | 30.67 | 32.41 | 17.18 | 49.36 | -3.95 |
| P8 | 131,072 | 57.47 | 67.62 | 66.99 | 70.01 | 62.10 | 83.81 | -2.40 |
| S3 | 100,000 | 2.40 | 11.09 | 11.68 | 12.67 | 2.33 | 25.42 | -1.58 |
| S3 | 400,000 | 19.00 | 37.26 | 39.41 | 41.75 | 12.04 | 59.96 | -4.49 |
| S3 | 800,000 | 51.15 | 64.30 | 64.75 | 66.99 | 56.60 | 81.27 | -2.69 |
| DS1 | 1,000,000 | 4.90 | 13.31 | 13.58 | 14.86 | 3.09 | 20.19 | -1.55 |
| DS1 | 4,000,000 | 23.01 | 35.10 | 42.88 | 44.80 | 20.24 | 48.09 | -9.70 |
| DS1 | 8,000,000 | 57.43 | 67.68 | 67.69 | 69.22 | 43.04 | 74.93 | -1.54 |

The final default beats exact LRU everywhere except OLTP at 5,000 and 15,000
entries, where it trails by about 0.7 and 1.4 points. OLTP is a recency-heavy
database trace.

A model sweep on DS1, S3, P8, OLTP, and S1 shows that S3-FIFO-style probation
is at its ceiling on these traces. The implementation matches
`model-s3clock/small=2` on DS1 at 4M (35.10% vs 34.92%) and beats it on S3 and
S1. It trails the model by about 1.5 points on P8 and OLTP. A 10% small FIFO
or a ghost of twice the capacity loses 1.6 to 4.8 points on DS1, S3, P8, and
S1, and gains at most 1.1 points on OLTP. S3-FIFO's two-bit frequency gains at
most 1.6 points, on P8. Only frequency moves the large gaps:
`packed-tiered`, the existing opt-in frequency admission, lifts DS1 at 4M from
35.10% to 42.88%. But it costs 5 points on OLTP at 15,000 entries (58.03% vs
63.22%). Closing the rest needs frequency admission that switches itself off
on recency-heavy traffic, as Caffeine's adaptive W-TinyLFU window does.

## Memory

`tests/direct_cache_memory.rs` measures requested live bytes (`stats_alloc`,
not RSS) for 32-byte keys and `u64` values, with and without probation. Each
cache is measured once filled to capacity and once after inserting twice its
capacity:

| Entries | Filled, off | Filled, on | Probation, filled | After churn, off | After churn, on | Probation, churned |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 65,536 | 103.6 | 107.3 | +3.7 | 115.4 | 119.9 | +4.5 |
| 200,000 | 80.8 | 84.7 | +3.9 | 81.6 | 85.6 | +4.1 |
| 524,288 | 68.2 | 71.8 | +3.6 | 71.2 | 75.0 | +3.8 |

All values are bytes per entry. The ghost filter and the reserved record
buffers cost 2.4 B/entry before any insert, and a full FIFO's key copies add
about 0.8 B/entry. The remaining 0.4 to 1.3 B/entry is not accounted for by
the structure sizes and is probably growth slack in the key-byte buffers. The
cache's own footprint barely changes between filling and churning. The test
bounds the probation overhead at 6 B/entry.

Live bytes are `bytes_allocated - bytes_deallocated`. `stats_alloc` already
folds each reallocation's size change into those two counters. An earlier
version of this section also added `bytes_reallocated`, which counted every
reallocation twice. That error showed the cache growing from 117-151 to
184-373 B/entry under churn and probation saving memory after churn; neither
is real. The test now also checks that dropping the cache releases what it
measured.

The first implementation reserved a FIFO record cap of twice the target, grew
its buffers by doubling, and rounded the ghost filter up to a power of two.
All three were tightened.

## Throughput screen

The test used `bounded_cache_probe` with `direct-owned`, 64-byte values, and
50,000 entries. Each cell ran six alternating pairs on a loaded host (load
average about 6 to 10). The change shown is the paired median, final build
against the probation-only build:

| Workload | 1 worker | 2 workers |
| --- | ---: | ---: |
| cold fill | +2.8% | +8.9% |
| turnover | -2.0% | +2.8% |
| resident hit | +6.3% | -4.4% |
| replace | -0.6% | +0.6% |

Every range crossed zero except the one-worker cold-fill gain. The
probation-only build against the original was neutral to positive: turnover
gained +3.6% with one worker and +30.3% with two, because higher hit rates mean
fewer loads and inserts. At 262,144 entries, cold fill read -3.3% and -4.7%,
with ranges crossing zero. Cold fill only exercises probation pops, so this is
probably noise, but it should be rechecked on a quiet host. Turnover was +4.3%
and -0.6%, with its hit rate rising from 89.49% to 89.92%.

## Remaining gaps

- Zipf 0.7 to 0.9 and scan-zipf are still 1.0 to 1.7 points behind QuickCache
  at 50,000 entries. `packed-tiered` closes part of this but loses on `shift`.
- A cache whose writers never call `maintain()` samples its overlay
  pseudo-randomly. Sampling can then visit an entry twice in one pass, clearing
  its mark the first time and evicting it the second. The sweep test needs
  periodic maintenance to pass.
- The admission doorkeeper only gates `insert_if_absent`. Upserts bypass it,
  so the October probe, which fills misses by upserting, could never have
  measured any admission mode.
- On real ARC traces, the default trails QuickCache by 1 to 4 points in most
  configurations and by 9.7 points on DS1 at 4,000,000 entries. Adaptive
  frequency admission is the next design step.
- Larger traces, such as Twitter or Meta cache traces in a one-key-per-line
  format via `file:PATH`, and a quiet-host throughput qualification are still
  to do.

## Reproduce

```sh
cargo build --release --example hit_ratio_sim
target/release/examples/hit_ratio_sim turnover 50000 1000000 1
target/release/examples/hit_ratio_sim zipf:0.9 50000 2000000 1 packed quick lru opt
target/release/examples/hit_ratio_sim zipf:0.9 50000 2000000 1 packed/probation=0
zstd -d OLTP.lis.zst
target/release/examples/hit_ratio_sim arc:OLTP.lis 15000 0 1 packed quick lru opt
cargo test --example hit_ratio_sim
cargo test --release --test direct_cache_memory -- --nocapture
```
