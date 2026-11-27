#!/usr/bin/env bash
# Head-to-head against DuckDB (in this process, built with --features duckdb) on this
# machine's Claude Code logs, scaled up by copies. Prints counts, sizes and times only.
#   COPIES=30     copies of every session (each copy as sessions of its own, text marked)
#   STORE=DIR     use this store instead (left as it is)
#   NO_KEYS=1     DuckDB without primary keys (smaller, point reads scan)
set -euo pipefail
cd "$(dirname "$0")/.."
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
WORK="${WORK:-${TMPDIR:-/tmp}/transcriptdb-duck}"
cargo build --release --quiet --features duckdb
TDB=./target/release/tdb
mkdir -p "$WORK"
if [ -z "${STORE:-}" ]; then
  rm -rf "$WORK/one" "$WORK/store"
  "$TDB" ingest-claude --store "$WORK/one" --sync os >/dev/null
  "$TDB" scale --from "$WORK/one" --store "$WORK/store" --copies "${COPIES:-30}" --sync os >/dev/null
  "$TDB" rules --store "$WORK/store" --benchmark x >/dev/null
  "$TDB" compact --store "$WORK/store" >/dev/null
  STORE="$WORK/store"
fi
"$TDB" duck --store "$STORE" --duck "$WORK/h2h.duckdb" --reps "${REPS:-7}" --reads "${READS:-300}" ${NO_KEYS:+--no-keys x}
[ "${KEEP:-0}" = 1 ] || rm -rf "${WORK:?}"
