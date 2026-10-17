# Direct cache hit ratio on real traces: ghost window, sweep, and overlay

After [the October 16 work](HIT_RATIO_SIMULATION_2026-10-16.md), the direct
cache matched QuickCache on synthetic traces but trailed it on the ARC paper's
real traces, by 9.7 points on DS1 at 4,000,000 entries. That write-up
attributed the gap to missing frequency admission. It was not. A model of
QuickCache that reproduces it cell by cell shows the gap came from four
mechanical differences, none of them frequency:

1. The ghost filter remembered evicted keys for up to a full capacity.
   QuickCache remembers them for half, and DS1 has a cliff between the two.
2. One access mark per entry. QuickCache counts up to two.
3. Each eviction inspected a fixed four candidates per removal and evicted
   read entries when those four were all read. A CLOCK hand keeps sweeping.
4. With the default configuration, a cache smaller than 65,536 entries kept a
   65,536-slot index overlay. Between rebuilds it filled with evicted keys'
   slots, so sampled eviction found few live candidates.

With all four fixed, the summed gap to QuickCache over 18 trace and size
cells fell from 49.5 to 15.4 points with the default configuration, an
average of 0.86 points per cell. The largest single gain is OLTP at 1,000
entries, from 26.20% to 35.33%. Small default caches also use far less
memory: a 1,000-entry cache fell from 3,334 to 250 requested bytes per entry.

These are simulator results on an unisolated host. They do not qualify a
release.

## Method

The traces, simulator, and QuickCache configuration are the ones described
in the October 16 write-up. The simulator now builds the packed cache with its
default overlay capacity, as an application would; it previously set the
overlay to the cache's capacity, which hid item 4.

`hit_ratio_sim` gained reference models (see `ClockModel::parse`):

- `model-quick`: QuickCache 0.7's policy with its defaults: a 3% cold FIFO, a
  ghost FIFO of half the capacity, and hot CLOCK marks counting to two.
- `model-tinylfu`: W-TinyLFU. A 1% window FIFO feeds a CLOCK main area; a key
  leaving the window unread duels the next main victim on a count-min sketch.
  `/climb=1` adds Caffeine's hill climber on the window size.
- Options on every model: `ghost=PCT` for a ghost FIFO, `ghostgens=N` and
  `ghostgenbps=BPS` for the packed cache's ring of filter generations,
  `maxf=N` for the mark limit, `smallbps=BPS`, and `window=N`, which bounds
  how many main keys one eviction inspects.

`model-quick` reproduces QuickCache within 0.6 points on 16 of the 18 cells
(OLTP at 1,000 entries reads 1.7 points high, S3 at 800,000 entries 1.2). Its
summed difference is +2.1 points. That made it possible to ablate QuickCache
one mechanism at a time.

Sums below are percentage points against QuickCache over the 18 cells;
negative is worse.

## Diagnosis

### Frequency was not the missing piece

| Model | Sum vs QuickCache |
| --- | ---: |
| `model-quick` | +2.11 |
| `model-quick/keep=0` (promotion forgets marks) | +2.36 |
| `model-quick/smallbps=200` (2% cold, as packed) | +3.53 |
| `model-quick/maxf=1` (one mark) | -2.95 |
| `model-quick/ghost=100` (ghost of a full capacity) | -9.55 |
| `model-tinylfu` | -16.90 |
| `model-tinylfu/climb=1` | -13.87 |
| `model-tinylfu/smallbps=200/ghost=100` | -37.36 |

W-TinyLFU wins DS1 at 4,000,000 entries (45.32%) but trails QuickCache by
8.6 points on P3 at 131,072 entries and by up to 3.3 on P8. Caffeine's hill
climber, which resizes the window once per ten capacities of requests,
recovers only 3 points in total. The ghost size is the big lever: with a
full-capacity ghost, the QuickCache model falls 10.2 points below QuickCache
on DS1 at 4,000,000 entries and 5.3 on S3 at 400,000.

### The ghost window has a cliff

With a 2% probation FIFO and one mark, as in the packed cache:

| Ghost | Remembers | Sum vs QuickCache |
| --- | --- | ---: |
| FIFO, 30% | 30% | -12.73 |
| FIFO, 40% | 40% | -4.97 |
| FIFO, 50% | 50% | -1.70 |
| FIFO, 65% | 65% | -3.68 |
| 2 generations x 50% (the October 16 filter) | 50-100% | -12.24 |
| 2 generations x 33% | 33-67% | -5.72 |
| 2 generations x 25% | 25-50% | -6.39 |
| 3 generations x 16.7% | 33-50% | -4.24 |
| 4 generations x 12.5% | 37.5-50% | -3.38 |
| 8 generations x 6.25% | 44-50% | -3.52 |
| 4 generations x 12.5%, two marks | 37.5-50% | +1.03 |
| FIFO 50%, two marks | 50% | +3.73 |

DS1 at 4,000,000 entries needs the ghost to stay at or below about half the
capacity, so a two-generation filter cannot both cover enough and stay under
the cliff. Four generations of an eighth each track a FIFO closely. Two marks
add about four points on top.

### A bounded sweep evicts read entries

The packed cache with the new ghost and two marks still trailed its model by 1
to 3 points per cell. Bounding the model's CLOCK hand to the packed cache's
four candidates per removal reproduced that:

| Cell | Unbounded | `window=4` | `window=16` | `window=64` | Packed cache, fixed window |
| --- | ---: | ---: | ---: | ---: | ---: |
| P3, 131,072 | 48.50 | 46.47 | 48.06 | 48.37 | 46.24 |
| S3, 100,000 | 12.59 | 11.56 | 12.36 | 12.57 | 11.25 |
| P8, 32,768 | 32.15 | 30.27 | 31.81 | 32.08 | 29.7-30.2 |
| P8, 8,192 | 10.29 | 7.89 | 9.62 | 10.08 | 8.2-9.3 |
| OLTP, 5,000 | 53.71 | 53.09 | 53.68 | 53.71 | 52.92 |

When most sampled entries are read, a fixed window of four per removal has too
few unread candidates and evicts read ones. Two marks make that more common.

### Ghost false positives

At 16 bits and three set bits per key, the four-generation filter matched
2% to 3% of keys it had never seen. On S3 at 100,000 entries only about 6,000
keys reached the main area by being read during probation, while about 290,000
arrived through the ghost filter (inferred from main-area evictions), roughly
2% of the 14 million keys checked. A false
positive lets a one-hit key skip probation, which costs most on scan-heavy
traces. A nearly exact filter recovered 0.5 to 1.0 points on S3 and P8.

### The default overlay

`CacheConfig` defaulted the index overlay to 65,536 slots. Slots of removed
keys are reclaimed only by a rebuild, which maintenance runs once three
quarters of the overlay's slots have been used. A 1,000-entry cache therefore
sampled mostly dead overlay slots between rebuilds:

| Cell | Overlay = capacity | Default overlay (65,536) |
| --- | ---: | ---: |
| OLTP, 1,000 | 35.30 | 26.13 |
| OLTP, 5,000 | 52.90 | 48.81 |
| OLTP, 15,000 | 63.44 | 61.90 |

The default left OLTP at 1,000 entries below exact LRU's 32.83%. On P3 at 131,072 entries the best
overlay is about the capacity: 32,768 slots give 44.81%, 65,536 give 45.35%,
131,072 give 46.34%, 262,144 give 46.30%, and 1,048,576 give 43.82%. Too
large leaves dead slots; too small rebuilds often, and each rebuild reshuffles
the base's sweep order.

## Changes

- **Ghost filter.** A ring of four generations, each holding an eighth of the
  expected entries, so evicted probationary keys are remembered for three
  eighths to half of the capacity. Each key sets five bits in one 64-bit word
  at 32 bits per key: about 0.2% false positives across the generations, and
  the same 2 bytes per expected entry as the old filter.
- **Two access marks.** A hit raises the first mark, a later hit the second;
  each sweep visit takes one. Once both are set a hit only loads, so hot keys
  stay read-shared. The second mark is the top bit of the entry's weight word,
  so a direct-cache entry's weight is now limited to 2 GiB - 1 (it was 4 GiB -
  1); larger weights return `CacheInsertError::WeightNotCompact`.
- **Sweep rounds.** An eviction batch that samples fewer unread candidates than
  it must remove samples another window, up to 16 rounds. Each round takes a
  mark from every sampled entry, so even a fully read cache converges.
- **Default overlay.** A `DirectPackedCache` with an entry limit and no
  explicit `with_overlay_capacity` sizes its overlay to that limit, between 64
  and 65,536. `PackedCache` and caches without an entry limit keep 65,536.

## Results

Read hit rate in percent, both builds with the default configuration. Before
is the October 16 policy (`perf/borrowed-direct-reads` at `400dba9`) run
through the same simulator.

| Trace | Entries | Before | After | QuickCache | LRU | OPT | After vs QuickCache |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OLTP | 1,000 | 26.20 | 35.33 | 35.15 | 32.83 | 53.62 | +0.18 |
| OLTP | 5,000 | 48.87 | 52.85 | 54.29 | 53.65 | 68.27 | -1.44 |
| OLTP | 15,000 | 61.34 | 63.49 | 64.83 | 64.63 | 75.14 | -1.34 |
| P3 | 8,192 | 4.21 | 4.03 | 3.64 | 1.62 | 11.25 | +0.39 |
| P3 | 32,768 | 14.72 | 12.64 | 12.94 | 3.56 | 32.25 | -0.29 |
| P3 | 131,072 | 44.98 | 45.33 | 48.61 | 44.79 | 67.51 | -3.28 |
| S1 | 65,536 | 5.80 | 5.87 | 6.17 | 1.56 | 18.14 | -0.31 |
| S1 | 262,144 | 23.12 | 23.11 | 23.77 | 6.43 | 42.50 | -0.66 |
| S1 | 524,288 | 39.75 | 41.60 | 41.99 | 23.71 | 59.01 | -0.39 |
| P8 | 8,192 | 8.78 | 9.49 | 10.40 | 2.30 | 23.22 | -0.91 |
| P8 | 32,768 | 28.89 | 30.88 | 32.41 | 17.18 | 49.36 | -1.54 |
| P8 | 131,072 | 67.18 | 69.50 | 70.01 | 62.10 | 83.81 | -0.52 |
| S3 | 100,000 | 11.08 | 11.55 | 12.67 | 2.33 | 25.42 | -1.11 |
| S3 | 400,000 | 37.04 | 40.00 | 41.75 | 12.04 | 59.96 | -1.76 |
| S3 | 800,000 | 63.57 | 67.01 | 66.99 | 56.60 | 81.27 | +0.02 |
| DS1 | 1,000,000 | 13.67 | 14.34 | 14.86 | 3.09 | 20.19 | -0.51 |
| DS1 | 4,000,000 | 38.05 | 43.87 | 44.80 | 20.24 | 48.09 | -0.93 |
| DS1 | 8,000,000 | 67.72 | 68.20 | 69.22 | 43.04 | 74.93 | -1.02 |

The summed gap to QuickCache is 49.54 points before and 15.43 after. The
after column reaches QuickCache in 3 cells, is within one point in 11, and
beats exact LRU in 16; it trails LRU only on OLTP at 5,000 and 15,000 entries.
Each cache seeds its index hasher randomly, which changes the sweep order;
three runs of P3 at 131,072 entries varied by 0.03 points.

Two cells lost ground. P3 at 32,768 entries fell from 14.72% to 12.64%,
still within 0.3 of QuickCache; the full-capacity ghost happened to suit it
(the one-mark model reads 17.96% with that ghost). P3 at 8,192 entries fell
0.18 points.

With the overlay set to the capacity, as the October 16 measurements did, the
same comparison reads -35.12 before and -13.78 after.

### Synthetic traces

50,000 entries, 1,000,000 operations, seed 1, default configuration:

| Trace | Before | After | QuickCache | LRU |
| --- | ---: | ---: | ---: | ---: |
| turnover | 89.90 | 89.95 | 89.89 | 87.75 |
| zipf 0.7 | 39.45 | 39.39 | 40.39 | 34.65 |
| zipf 0.9 | 64.83 | 64.73 | 65.31 | 61.84 |
| zipf 1.1 | 86.64 | 86.63 | 86.66 | 86.19 |
| scan-zipf 0.9 | 54.06 | 53.49 | 54.84 | 51.53 |
| shift 0.9 | 59.08 | 58.19 | 59.27 | 60.82 |
| loop | 54.52 | 61.62 | 63.09 | 0.00 |

The smaller ghost costs 0.6 points on `scan-zipf` and 0.9 on `shift`, as the
models predict. On `shift` recency wins: at 4,096 entries the packed cache
now reads 57.4%, below exact LRU's 57.7% and QuickCache's 58.8%, so the
simulator's gate no longer requires beating LRU there; it requires staying
within 6% of QuickCache's misses on every trace (measured 0.997 to 1.033).
The loop regression from October 16 is mostly gone.

### Ablation in the packed cache

Overlay set to the capacity, to isolate the policy changes:

| Build | Sum vs QuickCache |
| --- | ---: |
| Four-generation ghost at 16 bits, two marks | -23.91 |
| Same, one mark | -29.78 |
| Same, 16 candidates per removal | -17.26 |
| Same, one probation shard | -21.44 |
| Final ghost (32 bits, five set bits), two marks, sweep rounds | -13.78 |
| Final, but sampling only the base unless it is empty | -15.04 |

Things tried and not kept:

- Sampling only the base index (overlay entries are new to the main area, so
  this delays their first inspection, as a CLOCK hand would). It gained 1.2
  points on P3 at 131,072 entries but lost 2.0 on S3 at 800,000.
- Keeping marks when a key graduates from probation: neutral to -0.8.
- Marking keys that the ghost readmits: +1.2 on P3 at 131,072 entries but
  -0.7 on P8 at 32,768.
- One probation shard: +2.5 points in total, mostly DS1 at 8,000,000 entries,
  but every insert would take the same mutex.
- An overlay equal to the capacity for caches above 65,536 entries: about a
  point on P3 at 131,072 entries for roughly 35 more bytes per entry.

## Memory

Requested live bytes after inserting twice the capacity in 32-byte keys with
`u64` values (`stats_alloc`, not RSS), default configuration:

| Entries | Before B/entry | After B/entry |
| ---: | ---: | ---: |
| 1,000 | 3,333.8 | 249.5 |
| 4,096 | 823.1 | 107.7 |
| 16,384 | 222.2 | 127.3 |
| 65,536 | 119.7 | 119.6 |

The ghost filter stays at 2 bytes per expected entry, and
`tests/direct_cache_memory.rs` still bounds probation's overhead at 6 B/entry.

## Throughput screen

`bounded_cache_probe` with `direct-owned`, 64-byte values, 50,000 entries,
and an overlay of the capacity, so both builds have the same index. Each cell
ran six alternating pairs on a host where other work used about six of its 16
cores. The change is the paired median, after against before:

| Workload | 1 worker | 2 workers |
| --- | ---: | ---: |
| cold fill | +4.3% | +2.9% |
| turnover | -0.7% | +0.7% |
| resident hit | -5.9% | +1.1% |
| replace | -2.2% | +6.4% |

Every range crossed zero except the one-worker cold-fill gain (+2.4% to
+5.3%). Resident hits are where the second mark adds a write, so that cell was
rerun with twelve more pairs: median -1.4%, interquartile range -7.1% to
+1.9%, which this host cannot separate from zero. A hit writes at most twice
before both marks are set and only loads after that. Turnover's hit rate moved
from 89.91% to 89.93% with one worker. This screen does not qualify a release;
a quiet host should repeat it.

## Remaining gaps

- P3 at 131,072 entries trails QuickCache by 3.3 points. About one point is
  the 65,536-slot overlay cap. The rest is unexplained: with an overlay of
  the capacity the cache reads 46.34% against its model's 48.50%, although
  the model bounded to sixteen candidates per removal still reads 48.06%.
  The base's sweep order changing at each rebuild is one candidate.
- OLTP at 5,000 and 15,000 entries trails QuickCache by 1.4 and 1.3 points
  and exact LRU by 0.8 and 1.1. The model of the same policy reads 0.9 to 1.1
  points higher.
- S3 at 400,000 entries and P8 at 32,768 trail by 1.5 to 1.8 points.
- In an earlier build, the probation FIFO's sixteen round-robin shards cost
  about 2.5 points in total against one exact FIFO.

## Reproduce

```sh
cargo build --release --example hit_ratio_sim
SIM=target/release/examples/hit_ratio_sim
$SIM arc:PATH/DS1.lis 4000000 0 1 packed quick model-quick
$SIM arc:PATH/OLTP.lis 1000 0 1 packed packed/overlay=65536
$SIM file:PATH/P3.keys 131072 0 1 model-s3clock/smallbps=200/maxf=2/ghostgens=4/ghostgenbps=1250/window=4
cargo test --release --lib -- direct_ghost direct_sweep direct_default_overlay
cargo test --release --test direct_packed_cache rejects_an_entry_weight_above_two_gib
```
