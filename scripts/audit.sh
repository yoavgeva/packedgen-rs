#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

budget_mib="${PACKEDGEN_AUDIT_BUDGET_MIB:-${ELASTICHASH_AUDIT_BUDGET_MIB:-64}}"
lookups="${PACKEDGEN_AUDIT_LOOKUPS:-${ELASTICHASH_AUDIT_LOOKUPS:-1000000}}"
entries="${PACKEDGEN_AUDIT_ENTRIES:-${ELASTICHASH_AUDIT_ENTRIES:-1000000}}"
samples="${PACKEDGEN_AUDIT_SAMPLES:-${ELASTICHASH_AUDIT_SAMPLES:-5}}"

cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo run --release --locked --example memory_probe -- packed-binary-6 "$entries"
cargo run --release --locked --example memory_probe -- frozen-binary "$entries"
cargo run --release --locked --example memory_probe -- hashbrown-binary "$entries"
cargo run --release --locked --example ram_budget_probe -- "$budget_mib" "$lookups" "$samples"
