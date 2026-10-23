#!/usr/bin/env bash
# Run tests under Miri with strict provenance.
#
# Slow tests are gated with `#[cfg_attr(miri, ignore)]`.
#
# With MIRI_PARTITION set to a cargo-nextest partition, such as slice:1/3,
# this runs that share of the tests through nextest, an interpreter per
# test, several at once. Nextest runs no doctests, so the first share runs
# them.
#
# Extra args pass through.

set -ex

export MIRIFLAGS="${MIRIFLAGS:--Zmiri-strict-provenance}"

if [[ -n "${MIRI_PARTITION:-}" ]]; then
  cargo +nightly miri nextest run --no-fail-fast --partition "$MIRI_PARTITION" "$@"
  if [[ "$MIRI_PARTITION" == *:1/* ]]; then
    cargo +nightly miri test --doc "$@"
  fi
else
  cargo +nightly miri test "$@"
fi
