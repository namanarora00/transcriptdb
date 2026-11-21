#!/usr/bin/env bash
# Ingest every Claude Code log on this machine, then check that every recorded
# request rebuilds byte for byte from the store (sha256 equal to Claude Code's own
# requestDigest), that every step matches a slow reference, and print sizes,
# storage runs and read times.
#
# Real logs are read at run time from ~/.claude/projects (or $LOGS). The store is
# written outside the repo ($STORE, default under $TMPDIR) and removed at the end
# unless KEEP=1. Nothing from the logs is printed except counts, sizes and times.
set -euo pipefail
cd "$(dirname "$0")/.."
# Homebrew's rustup is keg-only, so cargo may not be on PATH
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
LOGS="${LOGS:-$HOME/.claude/projects}"
STORE="${STORE:-${TMPDIR:-/tmp}/transcriptdb-check}"
cargo build --release --quiet
TDB=./target/release/tdb
rm -rf "$STORE"
echo "== ingest (logs: $LOGS, store: $STORE)"
$TDB ingest-claude --store "$STORE" --logs "$LOGS"
echo "== stats as flushed"
$TDB stats --store "$STORE"
echo "== merge into one segment, main line first"
$TDB compact --store "$STORE"
$TDB stats --store "$STORE"
echo "== check every recorded request against the store"
$TDB check-claude --store "$STORE" --logs "$LOGS"   # exits non-zero if any stored request rebuilds wrong
echo "== steps against the slow reference"
$TDB check-steps --store "$STORE"   # exits non-zero on any difference
echo "== reads"
$TDB bench --store "$STORE"
[ "${KEEP:-0}" = 1 ] || rm -rf "$STORE" "$STORE.check.txt"
echo "OK"
