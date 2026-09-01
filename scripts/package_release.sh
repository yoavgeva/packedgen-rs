#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# Cargo verifies published dependencies through a registry rather than their
# workspace paths. Before packedgen-opthash is uploaded, this command-line
# patch gives the root package verifier the exact archive that will be
# published first. The patch is not written into either package manifest.
cargo package -p packedgen-opthash --allow-dirty
cargo package -p packedgen --allow-dirty \
  --config 'patch.crates-io.packedgen-opthash.path="crates/elastic-core"'
