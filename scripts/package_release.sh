#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# packedgen, the cache, depends on no workspace crate; the research crate,
# which does, is not published.
cargo package -p packedgen --allow-dirty
cargo package -p packedgen-opthash --allow-dirty
