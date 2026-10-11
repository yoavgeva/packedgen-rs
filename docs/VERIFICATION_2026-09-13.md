# Verification log — 2026-09-13

Repository: `/Users/yoavgea/repos/packedgen-rs`  
Branch: `codex/p2-churn-tail`  
Host/toolchain: Apple ARM, `rustc 1.98.0 (88d9e12ae 2026-08-18)`,
`cargo 1.98.0 (797e8a9bc 2026-08-05)`

## Latest status

The combined post-fix correctness pass ran in the pinned official
`rust:1.88-slim-bookworm` Linux ARM64 image (digest recorded below). The
selected default bounded gate passed **204 tests** (`145` library, `1`
allocation regression, `58` direct-cache integration). The final feature
package passed **376 tests** with **1 explicitly ignored** long soak (`149`
library, including the bounded Loom model; `76` direct-cache integration;
`2` direct-cache differential plus the ignored soak). The initial full feature
package run before the diagnostic ordering and Release/Acquire repair is
retained as historical RED: exit 101 after `289` passing tests, one failure,
and one ignored test. Its failure was
`concurrent_async_admission_never_crosses_the_configured_hard_limit` at
`tests/direct_packed_cache.rs:2372:5`, on the
`diagnostics.peak_weight <= ...` assertion; the captured output omitted
numeric operands. A later pre-fix enriched stress reproduced `peak_weight=283`
against `hard_limit_weight=282` at `tests/direct_packed_cache.rs:2385:5`.
The combined repair then passed 50/50 repeated feature stress attempts, so the
historical failure is not treated as current, and no UID causation is claimed.
The earlier macOS startup-limited allocation attempts remain historical and
unverified.

The original feature direct-cache failure is retained as a historical pre-fix
result. The repair uses wrapping delta arithmetic and wrapping public shard
folds; it does not add saturation or synchronization for those observations.
For diagnostic builds, removal debits diagnostics before releasing hard
capacity, and hard-counter release/rollback RMWs use `Release` while successful
bounded reservation CAS uses `Acquire`. This publishes the debit before an
admission that reuses released capacity; non-diagnostic builds remain
`Relaxed`. This follows the [Rust atomic ordering
documentation](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html).
Concurrent `len()` and bounded `weight()` results are weak, non-linearizable
snapshots that can be temporarily modulo-wrapped while publication
bookkeeping settles and converge after quiescence. They are not hard-capacity
reservation state.

The final strict Clippy runs passed for the root all-targets relevant-feature
lint (including its `packedgen-opthash` path dependency) and the `packedgen`
crate with `cache-production-diagnostics,prepared-keys`; the two pre-fix lint
failures are retained in the ledger below. The independent opthash package test
run passed **374** tests (the earlier `376` figure was a counting/
transcription discrepancy, not additional doctests). No performance, Miri, or
sanitizer qualification was run in this verification pass.

## Test change reviewed

`tests/direct_cache_allocations.rs` remains one test binary because
`stats_alloc` measures process-wide allocations. The retained test covers warm
tracked/untracked borrowed reads and `Arc` cloned reads, with a deep-copy
positive control.

A `prepared-keys`-gated extension was drafted and compiled during the feature
attempt, but was reverted before handoff because that feature-enabled
executable never reached test execution. Its proposed coverage is deferred:
bulk-load a frozen resident entry, prepare hit and miss handles, warm prepared
routes outside each `Region`, and then measure tracked/untracked prepared
hit/miss reads. A successful execution is required before making a
process-wide allocation claim for that path.

The dependency graph includes runtime Rayon through `ptr_hash 2.1.1` (its
manifest declares a normal `rayon` dependency; its source imports
`rayon::prelude::*` and builds pilots with `keys.par_iter()`). Therefore
one-time parallel-builder initialization or worker activity can affect
process-wide `stats_alloc` observations. Setup warm-up alone cannot establish
determinism, and the dynamic-library listing below cannot rule out statically
linked Rayon or background allocations.

## Command ledger

### Historical macOS default allocation attempt

```text
cargo test --locked --test direct_cache_allocations -- --nocapture
```

Cargo session `59559` spawned Cargo PID `78802` and rustc PID `78869` for the
integration target. After 2m45s, rustc remained at 0.0% CPU, no test executable
had been produced, and no `running 1 test` line appeared. The owned session was
stopped with Ctrl-C (exit 130). This attempt is **unverified**, not failed by
an allocation assertion.

The prior 2026-09-12 review records an earlier default allocation regression as
passed. That is historical evidence from the earlier run/tree; it must not be
confused with this stopped retry.

### Historical macOS feature-enabled allocation attempt

```text
cargo test --locked --features cache-production-diagnostics,prepared-keys --test direct_cache_allocations -- --nocapture
```

Cargo session `54455` compiled the feature-enabled library and test target,
reporting:

```text
Finished `test` profile [unoptimized + debuginfo] target(s) in 1m 53s
Running tests/direct_cache_allocations.rs (target/debug/deps/direct_cache_allocations-b8d23ad363ce8a03)
```

The executable (observed as PID `12596`) then stayed at 0.0% CPU before
printing `running 1 test`; it was stopped after roughly two minutes of startup
with Ctrl-C (session exit 130). No prepared allocation assertion result is
available, so this historical macOS feature-enabled run is **unverified**.

These two host attempts are retained for startup diagnosis only. They do not
override the later post-fix Linux allocation result in the final gates below.

### Pinned Linux container verification

The official image used for the isolated correctness fallback was:

```text
docker.io/library/rust:1.88-slim-bookworm
digest=sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89
```

Both runs used `--cpus=2 --memory=4g`, a read-only bind of the repository at
`/src`, a read/write bind of `target/linux-verification` at `/work`,
`--workdir /src`, host UID/GID mapping, `HOME=/work`,
`CARGO_HOME=/work/cargo`, and `CARGO_TARGET_DIR=/work/target`. The container
was ephemeral (`--rm`) and had no host credentials or Docker socket mounted.
These are compiler/correctness results, not performance evidence.

The first two commands below are historical pre-fix baselines. Their failure
and pass counts are retained to show the defect exposed by the feature run;
the post-fix focused and full gates follow them.

#### Historical pre-fix default command

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --lib --test direct_packed_cache --test direct_cache_allocations
```

Result: exit 0 on Rust 1.88. The library reported `141 passed; 0 failed`,
`direct_cache_allocations` reported `1 passed; 0 failed`, and
`direct_packed_cache` reported `58 passed; 0 failed`. The arena pointer-buffer
reuse test was green in the library suite.

#### Historical pre-fix feature command

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib --test direct_packed_cache --test direct_cache_allocations
```

Result: exit 101. The library reported `144 passed; 0 failed`, and
`direct_cache_allocations` reported `1 passed; 0 failed`. The direct-cache
suite reported `75 passed; 1 failed`; the failing test was
`concurrent_async_large_replacements_keep_exact_capacity`. Two scoped writer
threads panicked at `/src/src/cache.rs:7363` with `attempt to subtract with
overflow`, and the scoped test then panicked at
`tests/direct_packed_cache.rs:2470`. This is a runtime correctness failure
under the requested feature combination and must not be marked passed.

The prepared-read allocation extension was not present in either Linux test
binary; it remains deferred as described above.

#### Post-fix deterministic accounting regressions

Before the repair, the feature-enabled focused commands below were RED:

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib cache::tests::replacement_capacity_uses_the_published_delta_after_reordered_bookkeeping -- --exact --nocapture
```

Exit 101: `0 passed; 1 failed; 145 filtered out`; it panicked at
`src/cache.rs:7363:21` with `attempt to subtract with overflow`.

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib cache::tests::weight_pressure_transitions_follow_wrapped_reordered_deltas -- --exact --nocapture
```

Exit 101: `0 passed; 1 failed; 145 filtered out`; the final pressure
assertion observed `left: 0`, `right: 1` after a reordered remove-100/add-120
delta sequence.

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib cache::tests::capacity_observations_wrap_across_shard_sums -- --exact --nocapture
```

Exit 101: `0 passed; 1 failed; 147 filtered out`; the public shard
`Iterator::sum` panicked with `attempt to add with overflow`.

After the repair, the replacement, wrapped-pressure, all-24-order telescope,
and cross-shard-sum regressions each passed under the feature configuration
(one passed; respectively 146, 146, 146, and 147 filtered). The same four
focused regressions each passed under default features (one passed; the first
three had 143 filtered and the sum test had 144 filtered). The original
`concurrent_async_large_replacements_keep_exact_capacity` integration test
also passed under default features (`1 passed; 57 filtered`) and the feature
configuration (`1 passed; 75 filtered`).

#### Earlier post-arithmetic selected correctness gates

Default command:

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --lib --test direct_packed_cache --test direct_cache_allocations
```

Exit 0: `145` library tests, `1` allocation test, and `58` direct-cache
integration tests passed; no failures or ignored tests.

Feature command:

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib --test direct_packed_cache --test direct_cache_allocations
```

Exit 0: `148` library tests, `1` allocation test, and `76` direct-cache
integration tests passed; no failures or ignored tests.

#### Historical pre-memory-ordering full package runs

The full default package run passed with the same pinned image and container
flags:

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user 0:0 --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen
```

It exited 0: `346` tests passed and one explicitly ignored long soak was
reported across the package's executed binaries.

The diagnostic/prepared package run before the diagnostic ordering and
Release/Acquire repair was not green:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen --features cache-production-diagnostics,prepared-keys
```

It exited 101 after `289` tests passed, one failed, and one explicitly
ignored long soak. The failure was
`direct_packed_cache::concurrent_async_admission_never_crosses_the_configured_hard_limit`
at `tests/direct_packed_cache.rs:2372:5` on:

```text
assert!(diagnostics.peak_weight <= u64::try_from(HARD_LIMIT).unwrap())
```

The captured failure output did not include numeric assertion operands. A
normal host-UID/GID exact retry passed (`1 passed; 75 filtered out`), but this
did not explain the concurrency failure or erase the failed historical result.

#### Final combined-fix package result

The same full diagnostic/prepared package command was rerun after the
diagnostic-before-release reorder and feature-gated Release/Acquire
strengthening. It exited 0 with `376` tests passed and `1` explicitly ignored
long soak: `149` library tests (including the Loom model), `1` allocation test,
`2` direct-cache differential tests plus its ignored soak, `76` direct-cache
integration tests, and all other executed integration binaries passing.

The combined-fix enriched feature stress completed 50/50 exact attempts with
exit 0 (`1 passed; 75 filtered` per attempt). The pre-fix stress is retained
above: it stopped on attempt 8/50 at `peak_weight=283` versus
`hard_limit_weight=282`.

The independent opthash package run exited 0 with **374** passed tests. Its
reported per-binary counts were `78 + 12 + 106 + 11 + 110 + 1 + 10 + 46`;
the earlier `376` figure was a counting/transcription discrepancy, not two
additional doctests.

### Formatting

```text
cargo fmt --all -- --check
```

Final result: exit 0 after formatting the source changes. An earlier
pre-fix formatting check had reported a test wrapping difference and an
unrelated existing `src/cache_arena.rs:381` difference; that historical output
does not describe the final tree.

### Strict Clippy

The requested relevant configuration was run in the pinned image with the
official clippy component installed in the ephemeral container:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 sh -c 'rustup component add clippy && cargo clippy --locked --all-targets --features cache-production-diagnostics,prepared-keys -- -D warnings'
```

Historical pre-fix result: exit 101. Compilation reached the crate but failed
on the then-existing `packedgen-opthash` lint `clippy::useless_conversion` at
`crates/elastic-core/src/funnel.rs:354`.

The final scoped `packedgen`-only command was also run as requested:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 sh -c 'rustup component add clippy && cargo clippy --locked -p packedgen --no-deps --all-targets --features cache-production-diagnostics,prepared-keys -- -D warnings'
```

Historical pre-fix result: it exited 101 on `src/cache.rs:6545`
(`clippy::unused_self`) in the cfg stub. An earlier scoped attempt also
reported `src/cache_arena.rs:3627` (`clippy::ptr_as_ptr`), which belongs to the
new memory-agent pointer-buffer test and was corrected with `.cast()`.

After the narrow cfg-stub allow and redundant funnel `.into_iter()` removal,
the same root all-targets command (including its path dependency) and the
scoped root command both exited 0. The scoped command was:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 sh -c 'rustup component add clippy && cargo clippy --locked -p packedgen --no-deps --all-targets --features cache-production-diagnostics,prepared-keys -- -D warnings'
```

The root all-targets command above was rerun unchanged and also exited 0. The
pre-fix failures and the final passes are kept distinct; no unrelated
all-features lint gate is claimed.

Earlier all-features attempts were intentionally excluded from the final
gate: one lacked the clippy component, a component-installed retry hit the
project's `gxhash` AES/NEON guard, and a native-target retry reached
`tikv-jemalloc-sys` but the slim image lacked `make`. No broader dependency
installation was performed.

The assigned allocation test was formatted successfully with:

```text
rustfmt --edition 2024 tests/direct_cache_allocations.rs
```

The runtime-file formatting difference was not changed by this agent.

### Read-only startup diagnostics

The built feature test's dynamic dependencies were checked with:

```text
perl -e 'alarm 5; exec @ARGV' /usr/bin/otool -L target/debug/deps/direct_cache_allocations-b8d23ad363ce8a03
```

This returned successfully and listed only `libiconv.2.dylib` and
`libSystem.B.dylib`; this is not evidence that the statically linked runtime
contains no Rayon code or worker allocations. Bounded security assessments
were then attempted:

```text
perl -e 'alarm 5; exec @ARGV' /usr/bin/codesign --verify --verbose=4 --strict target/debug/deps/direct_cache_allocations-b8d23ad363ce8a03
perl -e 'alarm 5; exec @ARGV' /usr/sbin/spctl --assess --type execute --verbose=4 target/debug/deps/direct_cache_allocations-b8d23ad363ce8a03
```

Both exceeded the five-second alarm and exited 142 with no diagnostic output.
Targeted unified logging was responsive:

```text
perl -e 'alarm 8; exec @ARGV' /usr/bin/log show --last 10m --style compact --predicate 'process == "syspolicyd" AND (eventMessage CONTAINS[c] "packedgen" OR eventMessage CONTAINS[c] "direct_cache_allocations")'
```

Exit 0 with only the column header and no matching `syspolicyd` events. No OS
security setting was disabled or bypassed; no signatures were removed and no
dynamic-loader injection was used.

## Process ownership and limits

The historical host Cargo/rustc/test processes (`78802`, `78869`, and `12596`)
were terminated after their bounded startup observations. The unrelated
ferricstore application Cargo processes (`9627`, `9647`, and `14208`) were not
touched. All post-fix Linux containers were ephemeral and completed; no owned
Cargo, rustc, test, or container process remains active. The arena pointer
reuse test passed in both final library gates.

No Miri, sanitizer, benchmark, or runtime-performance qualification was run in
this verification pass.
