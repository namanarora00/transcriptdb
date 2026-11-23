#!/usr/bin/env bash
# Head-to-head against PostgreSQL on a sample of this machine's Claude Code logs.
# Needs PostgreSQL (Homebrew postgresql@18). Creates a throwaway cluster and store under
# $WORK (default $TMPDIR/transcriptdb-h2h), runs the comparison, then stops PostgreSQL and
# deletes both (KEEP=1 keeps them). Prints counts, sizes and times only.
# The store never keeps thinking or images, and PostgreSQL is loaded from the store, so both
# hold the same messages with those values blanked.
#   DROP=documents,other   blank those values too
#   H2H_ARGS="--split x"   extra arguments for `tdb h2h` (`--split x` prints where PostgreSQL's
#                          bytes are, `--sizes-only x` skips the queries)
set -euo pipefail
cd "$(dirname "$0")/.."
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@18/bin}"
WORK="${WORK:-${TMPDIR:-/tmp}/transcriptdb-h2h}"
FILES="${FILES:-200}"
PORT="${PORT:-5455}"
cargo build --release --quiet
rm -rf "$WORK"
mkdir -p "$WORK"
"$PGBIN/initdb" -D "$WORK/pg" --locale=C --encoding=UTF8 -U tdb --auth=trust >/dev/null
cat >> "$WORK/pg/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = $PORT
unix_socket_directories = ''
wal_level = minimal
max_wal_senders = 0
shared_buffers = 256MB
work_mem = 32MB
maintenance_work_mem = 128MB
effective_cache_size = 2GB
max_parallel_workers_per_gather = 4
jit = off
EOF
cleanup() {
  LC_ALL=en_US.UTF-8 "$PGBIN/pg_ctl" -D "$WORK/pg" -w stop >/dev/null 2>&1 || true
  [ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"
}
trap cleanup EXIT
# PostgreSQL on macOS refuses to start without a valid LC_ALL
LC_ALL=en_US.UTF-8 "$PGBIN/pg_ctl" -D "$WORK/pg" -l "$WORK/pg.log" -w start >/dev/null
"$PGBIN/psql" -h 127.0.0.1 -p "$PORT" -U tdb -d postgres -qc "CREATE DATABASE h2h"
./target/release/tdb ingest-claude --store "$WORK/store" --max-files "$FILES" --seed 7 --sync os ${DROP:+--drop "$DROP"}
./target/release/tdb compact --store "$WORK/store"
./target/release/tdb h2h --store "$WORK/store" --pg "127.0.0.1:$PORT" --reps 7 --reads 300 ${H2H_ARGS:-}
