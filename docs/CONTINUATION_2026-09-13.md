# Verification continuation log — 2026-09-13

Repository: `/Users/yoavgea/repos/packedgen-rs`  
Branch: `codex/p2-churn-tail`

This continuation covers the two requested lint cleanups, the diagnostic
ordering and memory-ordering repair, and bounded Linux correctness checks. It
does not claim a performance result. The earlier diagnostic/prepared failure
is retained as historical RED; the combined-fix package run is recorded below.

## Latest status

The final combined fix passed the bounded Loom model, 50/50 repeated feature
stress attempts, the full feature package (`376 passed; 1 ignored`), and the
default bounded gate (`204 passed`). The pre-fix stress did not complete 50
attempts: it reproduced on attempt 8 with `peak_weight=283` against
`hard_limit_weight=282`. The original full-package failure remains historical
evidence of that race, not a current unresolved result. The separate opthash
package run passed `374` tests.

## Environment

Correctness and lint commands used the official ARM64 image
`rust:1.88-slim-bookworm`:

```text
digest=sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89
```

Container test/lint runs used `--cpus=2 --memory=4g`, an ephemeral `--rm`
container, repository bind-mounted read-only at `/src`,
`target/linux-verification` bind-mounted read/write at `/work`,
`--workdir /src`, `HOME=/work`, `CARGO_HOME=/work/cargo`, and
`CARGO_TARGET_DIR=/work/target`. No host credentials or Docker socket was
mounted. Root-owned containers were used only where installing the ephemeral
official clippy component was required; normal test retries used host UID/GID
mapping.

## Lint RED and GREEN

Before the fixes, the root all-targets relevant-feature lint (including its
`packedgen-opthash` path dependency) was RED:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 sh -c 'rustup component add clippy && cargo clippy --locked --all-targets --features cache-production-diagnostics,prepared-keys -- -D warnings'
```

It exited 101 on `crates/elastic-core/src/funnel.rs:354`,
`clippy::useless_conversion` for a redundant `.into_iter()`.

The pre-fix root-only lint was also RED:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 sh -c 'rustup component add clippy && cargo clippy --locked -p packedgen --no-deps --all-targets --features cache-production-diagnostics,prepared-keys -- -D warnings'
```

It exited 101 on `src/cache.rs:6545`, `clippy::unused_self`, in the
no-pressure-timing cfg stub.

The fixes were intentionally narrow: a justified `clippy::unused_self` allow
preserving the timed/no-timing method shape, and removal of only the redundant
funnel `.into_iter()`. Afterward, both exact commands above passed with exit 0.
The root lint included its `packedgen-opthash` path dependency; no unrelated
workspace-member lint gate was claimed and no other `elastic-core` behavior was
changed.

## Historical pre-memory-ordering package results

Default package command:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen
```

Exit 0. The library ran 145 tests successfully. All executed package
integration binaries passed; `direct_cache_differential` reported 2 passed
and 1 explicitly ignored long soak. The direct-cache integration binary
reported 58 passed.

Diagnostic/prepared package command before the Release/Acquire strengthening:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen --features cache-production-diagnostics,prepared-keys
```

This pre-strengthening run exited 0: `148` library tests and `76`
`direct_packed_cache` tests passed; `direct_cache_differential` reported `2`
passed and `1` explicitly ignored long soak. The earlier full-package RED
run (before the diagnostic ordering repair) is retained in the prior
verification log and below as historical evidence.

The pre-strengthening run did not include the new Loom model. Its earlier RED
failure was in
`direct_packed_cache::concurrent_async_admission_never_crosses_the_configured_hard_limit`
at `tests/direct_packed_cache.rs:2372:5` on:

```text
assert!(diagnostics.peak_weight <= u64::try_from(HARD_LIMIT).unwrap())
```

The captured full-package failure output did not print numeric assertion
operands. A later bounded pre-fix stress with enriched assertions reproduced
the same race at `tests/direct_packed_cache.rs:2385:5`: `peak_weight=283` and
`hard_limit_weight=282`, with `current_entries=281` and `current_weight=281`.
The failure was not attributed to UID differences.

The exact historical failing full-package command and image flags were:

```text
docker run --rm --cpus=2 --memory=4g --user 0:0 --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen --features cache-production-diagnostics,prepared-keys
```

## Final combined-fix results

The repair first debits diagnostics, then releases hard capacity. In
`cache-production-diagnostics` builds, hard-counter release/rollback RMWs use
`Release` and successful reservation CAS uses `Acquire`; non-diagnostic builds
retain `Relaxed`. This gives the intended debit → release → acquire → add
happens-before chain without changing the default ordering.

The bounded Loom model was run with:

```text
docker run --rm --cpus=2 --memory=4g --user "$(id -u):$(id -g)" --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --features cache-production-diagnostics,prepared-keys --lib cache::tests::hard_capacity_release_acquire_orders_diagnostic_reuse -- --exact --nocapture
```

It exited 0 with `1 passed; 148 filtered out`. The intentional old
Relaxed/Relaxed negative control emitted the expected caught assertion
(`left: 2`, `right: 1`); the actual Release/Acquire selectors passed the
explored schedules. This is a one-dimensional, two-atomic litmus rather than
a proof of the entire cache protocol, and Loom does not model every possible
Relaxed reordering ([Loom Relaxed-memory-ordering
documentation](https://docs.rs/loom/latest/loom/#relaxed-memory-ordering)).
The HB conclusion is separate static reasoning from the Release/Acquire chain.

The enriched feature stress command was repeated before and after the
combined fix, capped at 50 attempts/60 seconds with the same image flags and
exact test target:

```text
cargo test --locked -p packedgen --features cache-production-diagnostics,prepared-keys --test direct_packed_cache concurrent_async_admission_never_crosses_the_configured_hard_limit -- --exact --nocapture
```

The pre-fix loop stopped at attempt 8/50 when the enriched assertion reproduced
the stale diagnostic peak (`peak_weight=283`, `hard_limit_weight=282`); it did
not complete 50 attempts. After the combined fix, all 50/50 attempts passed
(`1 passed; 75 filtered` each), exit 0.

The final full feature package command above was rerun after the combined fix
and exited 0: `376` tests passed and `1` explicitly ignored long soak. The
library reported `149` passed (including the Loom model),
`direct_cache_allocations` `1` passed, `direct_cache_differential` `2` passed
and `1` ignored, and `direct_packed_cache` `76` passed; all other executed
integration binaries passed. The final default bounded command was:

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked --lib --test direct_packed_cache --test direct_cache_allocations
```

It exited 0 with `204` tests passed (`145` library, `1` allocation, `58`
direct-cache integration), no ignored tests.

## Opthash tests

```text
docker run --rm --cpus=2 --memory=4g --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs,dst=/src,readonly --mount type=bind,src=/Users/yoavgea/repos/packedgen-rs/target/linux-verification,dst=/work --workdir /src --user "$(id -u):$(id -g)" --env HOME=/work --env CARGO_HOME=/work/cargo --env CARGO_TARGET_DIR=/work/target rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 cargo test --locked -p packedgen-opthash
```

Exit 0: 374 tests passed across the reported unit, fixture, common, epoch,
parity, reserve, set, and prehashed binaries. This is the exact sum of the
per-binary counts in that run (`78 + 12 + 106 + 11 + 110 + 1 + 10 + 46`);
the earlier `376` figure was a counting/transcription discrepancy, not two
additional doctests. No failure was reported.

## Focused memory and formatting checks

The memory-agent pointer cast fix was validated with:

```text
cargo test --locked --lib cache_arena::tests::direct_arena_pointer_collection_reuses_handle_buffer -- --exact --nocapture
```

In the pinned image it exited 0 with 1 passed and 144 filtered. The final
format check after the documentation update was:

```text
cargo fmt --all -- --check
```

It exited 0.

No commit or push was performed. No benchmarks, Miri, or sanitizer runs were
performed in this verification pass. The memory agent separately owns its
release-build and benchmark activity.
