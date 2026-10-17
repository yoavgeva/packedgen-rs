# Direct-arena pointer collection experiment — 2026-09-13

## Candidate

`drain_retired_epoch` in `src/cache_arena.rs` converts an owning
`Vec<RemovedDirectHandle>` into a `Vec<*mut DirectArenaEntry<V>>`. The original
`.iter().map(...).collect()` keeps the handle vector and a second pointer
vector alive at the same time. The candidate uses the owning `.into_iter()`.
On the supported 64-bit targets both elements are one pointer wide, so Rust's
owning collection path can reuse the handle allocation for the pointer output.
The Rust 1.88 implementation documents this cross-type in-place collection in
[`alloc::vec::in_place_collect`](https://github.com/rust-lang/rust/blob/1.88.0/library/alloc/src/vec/in_place_collect.rs).
Reuse is a tested compiler/library optimization, not a stable `Vec` API
guarantee; correctness does not depend on allocation identity. This is a cold
reclamation-path change; it does not add production unsafe code or alter
atomics, publication, or ordering.

## Failure-first evidence

The focused test covers empty, one-item, 17-item, and a batch larger than the
64-item publication batch, while checking output order, tag stripping, and
entry visibility.

With the test helper intentionally using the original `.iter()` conversion,
the native RED command was:

```sh
cargo test --locked --lib \
  cache_arena::tests::direct_arena_pointer_collection_reuses_handle_buffer \
  -- --exact --nocapture
```

It exited 101 with the expected reuse assertion failure: the output pointer
buffer had a different address from the handle buffer.

After changing only the helper to `.into_iter()`, the same command exited 0 on
native Rust 1.98.0/aarch64-apple-darwin. The pinned Rust 1.88 Linux fallback
also passed the focused test (image
`rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89`).
The Linux panic-reclamation filter also passed all four tests:

```sh
cargo test --locked --lib cache_arena::tests::panicking -- --nocapture
```

## Bound and B/C A/B screen

The attributable saving is temporary scratch only: at most roughly `8 * N`
bytes of pointer-vector capacity and its separate allocation for `N` retired
handles during conversion. It is not a per-entry, retained-RSS, or steady
state cache-memory claim. Empty collections remain allocation-free, and the
existing handle ownership/drop behavior remains covered by the panic tests.

An exploratory B/C screen was run after the correctness gates. B is the
capacity-corrected tree with the exact original `src/cache_arena.rs` from
`26d1d93`; C is the same tree with the owning `.into_iter()` candidate. All
other source, `Cargo.lock`, compiler, feature flags, probe settings, and
container limits were identical. The two sessions used seven fresh-process
pairs for each of six workloads at one and two workers; session 2 reversed the
initial B/C order. Direct and Papaya control runs were separate fresh
processes. `C/B` is the paired Direct throughput change, and higher Mops/s is
better. Values are percentages unless marked Mops/s or percentage points.

| workload | workers | S1 C/B median | S2 C/B median | combined C/B median (range) | Direct B/C median Mops/s | Papaya C/B median | Direct hit C-B pp; final-entry C-B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| read_hit | 1 | +8.0 | +0.3 | +1.1 (-23.9..+33.8) | 20.474 / 20.213 | +0.3 | n/a; 0 |
| read_hit | 2 | -18.0 | +2.8 | +0.2 (-49.8..+98.3) | 41.697 / 42.875 | -0.7 | n/a; 0 |
| read_miss | 1 | +2.3 | +1.8 | +2.0 (-5.2..+6.3) | 68.635 / 70.868 | +1.8 | n/a; 0 |
| read_miss | 2 | -3.8 | -8.6 | -6.2 (-49.4..+89.3) | 117.345 / 104.981 | +2.6 | n/a; 0 |
| insert_miss | 1 | -0.6 | -3.2 | -1.9 (-34.0..+42.3) | 4.754 / 4.804 | -0.3 | n/a; 0 |
| insert_miss | 2 | -4.1 | -1.2 | -2.7 (-23.4..+61.7) | 9.656 / 9.148 | +3.1 | n/a; 0 |
| replace_hit | 1 | +10.3 | -1.4 | +7.0 (-13.9..+25.7) | 3.527 / 3.604 | -6.9 | n/a; 0 |
| replace_hit | 2 | -29.9 | +4.3 | -5.6 (-60.7..+97.3) | 3.802 / 3.613 | -0.3 | n/a; 0 |
| remove_hit | 1 | +8.8 | -10.1 | -0.0 (-25.3..+19.7) | 4.666 / 4.622 | +0.9 | n/a; 0 |
| remove_hit | 2 | -10.1 | +4.7 | +0.5 (-28.5..+103.9) | 7.276 / 7.184 | +3.9 | n/a; 0 |
| cache_mix_95_pressure | 1 | +3.0 | +1.5 | +2.2 (-15.2..+32.1) | 10.860 / 10.960 | +2.7 | +0.011; -6.5 |
| cache_mix_95_pressure | 2 | -12.0 | +7.9 | +3.5 (-15.9..+58.4) | 17.244 / 16.213 | -5.1 | -0.008; 0 |

Across both sessions, Papaya control movement spans roughly `-11.717%` to
`+26.423%` by paired median, while Direct pair ranges are as wide as `-60.7%`
to `+103.9%`. This variability is consistent with scheduler/allocator noise
in the constrained shared container; its cause was not isolated. Same-sign
negative Direct session medians include `insert_miss` at one worker
(`-0.6%`, `-3.2%`), `insert_miss` at two workers (`-4.1%`, `-1.2%`), and
`read_miss` at two workers (`-3.8%`, `-8.6%`). The pooled medians also show
warning rows for `read_miss`/two workers (`-6.188%`) and `replace_hit`/two
workers (`-5.611%`), but these samples do not establish attribution or a
stable performance verdict. The screen does not establish a throughput win, a
no-regression result, or a retained-memory/RSS win. The pressure hit-rate
deltas are `+0.011` and `-0.008` percentage points, and final-entry
differences are small transient workload outcomes, not resident-memory
evidence.

The probe used `entries=50000`, `operations=1000000`, 32-byte keys, 64-byte
values, and a configured weight limit of 96 per entry. The nominal
`entries=50000` setting is not a hard cap for every path: `insert_miss` uses
capacity `operations + 4096`, seeds only `min(entries, 4096)`, and grows to
`1,004,096` final entries in both variants; `remove_hit` starts with
`1,000,000` seeded entries. Those rows therefore measure expanding
insert/delete scenarios, not a 50k bounded cold-fill/eviction workload, and
must not be used as bounded-residency claims.

### Reproduction and provenance

Each session produced 84 paired rows, 336 Direct/Papaya CSVs, and 336 stderr
files; both stderr scans were clean. Raw outputs and per-run metadata are
preserved under
[`target/memory-ab-20260913/results`](../target/memory-ab-20260913/results).
The runner is [`run_bc.sh`](../target/memory-ab-20260913/run_bc.sh), and the
summary logic is [`summarize_bc.sh`](../target/memory-ab-20260913/summarize_bc.sh).

The release builds used Rust 1.88.0 in image
`rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89`,
`--no-default-features`, `--release`, cpuset `0-1`, `--cpus=2`, and 4 GiB
memory. `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS` were empty. Both sessions
reused the same stable build directories but retained separate immutable
result directories. The resulting binary hashes were B
`705395cc2d2a8500eef0f291f9f55f38f303d80f50ed861e9a8afdc71f142cf8` and C
`29d858d01ba31f15de9d0a1821956c404793d2f6d0478bc5e8ecdd40209d286a`.

The snapshot source hashes are recorded in each session's
`source-hashes.tsv`: B arena `f00cd62a460d1a24a7ac0b1028fb9ba930a350c16e3549ffc8468cee096b9609`,
C arena `22d9e6bb4fb709fbc1b39dddf1ccfc96edb92965875663525de5a68e0bef842b`,
and common `src/cache.rs`
`c2ddfabce71499e8eb3833805e64386b3a838f35997b24af50842d36cd3a0ec5`.
`diff -rq --exclude=.git B C` reported only `src/cache_arena.rs`.
