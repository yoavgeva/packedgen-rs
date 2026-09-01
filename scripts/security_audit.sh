#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! command -v cargo-audit >/dev/null 2>&1; then
  printf 'cargo-audit is required; install it with cargo install cargo-audit --locked\n' >&2
  exit 2
fi

# PtrHash 2.x declares fxhash for public convenience aliases even when its
# caller supplies a custom KeyHasher, as PackedGen does. RustSec classifies the
# crate as unmaintained, not vulnerable, and publishes no patched fxhash
# version. Keep this narrow exception only while the dependency tree and source
# usage remain exactly constrained below.
if rg -n 'ptr_hash::hash::(FastIntHash|FxHash)|fxhash::' src tests examples benches; then
  printf 'PackedGen started using the excepted fxhash API directly\n' >&2
  exit 1
fi

tree="$(cargo tree -i fxhash --edges normal --prefix none)"
line_count="$(printf '%s\n' "$tree" | wc -l | tr -d ' ')"
line_one="$(printf '%s\n' "$tree" | sed -n '1p')"
line_two="$(printf '%s\n' "$tree" | sed -n '2p')"
line_three="$(printf '%s\n' "$tree" | sed -n '3p')"
if [[ "$line_count" != 3 \
  || "$line_one" != 'fxhash v0.2.1' \
  || "$line_two" != 'ptr_hash v2.0.1' \
  || "$line_three" != packedgen\ v0.1.0\ * ]]; then
  printf 'fxhash dependency path changed; review the RustSec exception:\n%s\n' "$tree" >&2
  exit 1
fi

cargo audit --deny warnings --ignore RUSTSEC-2025-0057
