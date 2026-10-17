# Dependency security policy

Every release is scanned with `scripts/security_audit.sh`. Vulnerability,
unsoundness, yanked, and unreviewed unmaintained advisories fail the audit.

## Temporary PtrHash/fxhash exception

RustSec advisory
[`RUSTSEC-2025-0057`](https://rustsec.org/advisories/RUSTSEC-2025-0057.html)
marks `fxhash 0.2.1` as unmaintained; it does not report a vulnerability and
lists no patched version. `ptr_hash 2.1.1` currently declares that crate for
its public `FastIntHash` and `FxHash` aliases.

PackedGen supplies its own 128-bit `DigestHasher` to PtrHash and does not use
either alias. The audit script proves that there is exactly one normal
dependency path (`packedgen -> ptr_hash -> fxhash`) and fails if PackedGen
starts referring to the excepted API. Remove the exception as soon as PtrHash
makes the aliases optional, replaces the dependency, or a measured alternative
meets PackedGen's RAM and throughput gates.

The repository tracks its root `Cargo.lock` so tests, benchmarks, and audits can
resolve the workspace graph reproducibly with `--locked`. Published library
consumers resolve their own dependency graph and lockfile; this repository lock
does not override downstream resolution, while the exact dependency requirements
in `Cargo.toml` remain part of the published package.
