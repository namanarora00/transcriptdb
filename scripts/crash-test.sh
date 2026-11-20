#!/usr/bin/env bash
# Kill the writer with SIGKILL at random moments while it ingests (synthetic data),
# reopen after every kill, and check that every request it acknowledged is still
# there and rebuilds byte for byte. Exits non-zero if anything acknowledged is lost.
#   scripts/crash-test.sh [kills] [sync: full|fsync|os]
set -euo pipefail
cd "$(dirname "$0")/.."
# Homebrew's rustup is keg-only, so cargo may not be on PATH
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
KILLS="${1:-1000}"
SYNC="${2:-full}"
STORE="${STORE:-${TMPDIR:-/tmp}/transcriptdb-crash}"
cargo build --release --quiet
./target/release/tdb crash-test --store "$STORE" --kills "$KILLS" --sync "$SYNC" --seed "${SEED:-1}"
rm -rf "$STORE"
