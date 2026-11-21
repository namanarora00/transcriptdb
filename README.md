# TranscriptDB

A storage and query engine for agent transcripts: Kusto for agent traces. This repository is
the **storage core**, written in Rust: each session is stored once as an ordered tree of
messages, any request can be rebuilt byte for byte, and a crash loses nothing acknowledged.

On top of the stored messages it keeps **steps**: each tool call paired with its result, as
dictionary-coded columns with a one-byte shape code per step, and a **query language** in the
style of Kusto with a `match` stage for sequences of steps.

## What the storage core does

A request arrives as the exact body the model API received (the Anthropic Messages format),
plus a session id, an agent id (empty for the main agent) and a request id.

1. **Split** (`src/split.rs`). A byte-span JSON scanner (`src/json.rs`) finds the `messages`
   array, the `system` value and the `tools` value. Nothing is re-encoded, so key order and
   whitespace survive. What is left is a small *skeleton* (model, params) with holes.
2. **Cut out what moves.** Each `cache_control` member on a content block is cut out into a
   side note (offset + exact bytes), because the marker moves to the newest message on every
   call and would make the same message look new each time.
3. **Place in the session tree** (`src/db.rs`). The messages are walked down the session's tree
   by hash of their bytes. The shared start adds nothing; the first message that differs starts
   new nodes, on the end of the main line or as a fork (a side query, a rewind, a compaction).
   A request is then just its *tip* node: its messages are the path from the root.
4. **Opaque store.** In new messages, long base64 strings under `signature` or `data` (thinking
   signatures, images, documents) are cut out and kept as raw bytes (decoded when re-encoding
   gives the same text back, kept as text otherwise). Each is classified by where it sits, and
   `Options::keep` (`tdb --drop thinking,images,documents,other`) chooses which kinds to keep:
   a value not kept is stored as its kind, hash and size only, and requests that held it
   rebuild with an empty string in its place (thinking text is cut out too when thinking is
   dropped). Step result sizes still count the original values.
5. **Shared pieces.** Skeletons, system blocks, tool lists and marker texts are interned by
   hash across all sessions.
6. **Log.** Everything the request adds goes into one checksummed record in the write-ahead log
   (`src/wal.rs`). `Db::commit` writes and syncs it (`F_FULLFSYNC` on macOS by default); only
   then does the request count as saved. In-memory state only ever changes by applying a log
   record, live or during recovery, so recovery rebuilds exactly the same state.
7. **Flush** (`src/segment.rs`, `src/blob.rs`). Every 64 MB, the in-memory part becomes an
   immutable level-0 segment and a blob file. The segment holds a *part* of each session it
   touched: the new nodes laid out *main line first* (`src/layout.rs`) and cut into 64 KB zstd
   frames, plus the session's new requests and steps, encoded into compressed blocks. The blob
   file holds the new pieces and opaque values, with dense id ranges. The manifest
   (`src/manifest.rs`) is replaced atomically (write, sync, rename, sync the directory) and old
   log files are deleted.
8. **Merge.** Once four level-0 segments wait, they are merged with the level-1 segments whose
   session ranges they touch (and the last level-1 segment before brand-new sessions, while it
   is under 256 MB). Each session is rewritten whole, main line first over its whole tree, into
   level-1 segments split by size and partitioned by session id, so finished sessions settle
   and are not rewritten again. Blob files are never rewritten. `Db::compact` merges everything.
9. **Recovery** (`Db::open`). Reads the manifest, deletes files it does not list (a crash during
   a flush or merge), reads each file's catalog, replays the log after the checkpoint and cuts a
   torn last record.

## What stays in memory

Memory does not grow with the stored data; it is fixed parts plus one small catalog per file:

- **Catalogs.** Per segment: its session id range, counts, time range, the tool and field names
  its steps use, Bloom filters of session ids, session keys and request ids (14 bits per key,
  about 0.2% false positives), where its blocks are, and the first request id of each request
  table block. Per blob file: its id ranges, a Bloom filter of content hashes, and where each
  table block is. Opening a store reads the manifest and these catalogs, nothing else. This is
  the one part that grows with the data: about 2 bytes per stored request, mostly Bloom filters
  (3–5 MB for 1.4 million requests in the growth test).
- **Sessions taking writes.** A session's message tree index, pending tool calls and step tails
  stay in memory while it takes writes, and leave two flushes after its last write. A session
  that wakes up later is read back from its parts.
- **Unflushed data** (up to 64 MB), and **caches with fixed budgets** (256 MB in all by default)
  for sessions, steps, blocks, directories, request tables, blob tables and pieces.

A session is a self-contained unit: its messages, requests and steps (with its own dictionary of
input values; tool and field names are shared) live in its parts and are read on demand. A
request is addressed as (session, index in the session). Finding a session by key, a request by
id, or an existing piece or opaque value by hash asks the Bloom filters first and reads only the
files that may hold it.

"Main line first" weighs each node by the number of requests that pass through it and writes
the tree depth first, heaviest child first. The main line is then one run, and each side branch
is one more run right after the subtree it hangs from.

## Steps

`src/steps.rs`. A step is a tool call paired with its result. Pairing happens inside `apply`, the
same code path that log replay uses, so recovery rebuilds exactly the same steps:

- when a message is added, its `tool_use` blocks are remembered by id, and each `tool_result`
  block is paired with the call it answers, found on the result's own path through the tree (a
  cached call is only used if it sits on that path; otherwise the path is walked upwards);
- each session's steps are numbered in arrival order, and each step records `prev`, the last step
  before its turn on its own path. Calls made in one model turn share a `prev`, so "then" never
  orders them. Walking steps in number order and following `prev` visits every path of a forked
  session, with the shared start walked once: the shape the `match` stage needs;
- columns per step: tool, outcome, result size (opaque values counted at their text size), the
  time of the request that carried the result, call and result positions, and the call's
  top-level input fields. Values up to 256 bytes are dictionary-coded; longer ones are marked
  `LONG` (open the message);
- the shape code is `tool << 1 | failed`, tool codes 0..126 for the first 127 tools seen and 127
  for the rest; postings per tool and per field value form the field index.

Segments store steps with their own dictionaries; codes in memory are per process.

`tdb check-steps` compares the engine's steps with a slow reference that rebuilds every stored
request, parses the whole JSON and pairs results with calls inside that request, and checks every
`prev` by brute force.

`tdb serve --store DIR` serves local pages on 127.0.0.1: the sessions with their step strips, one
session as a timeline (tool, inputs, output size, failures, forks and same-turn calls), and one
step's call and result.

## Queries

`src/query/`. Kusto-style pipes over steps:

```
where time > ago(30d)
| match a: tool == "Bash" and failed
    then b: tool in ("Edit", "Write") and file_path contains "test"
    then c: tool == "Bash" and not failed
    where no (tool in ("Edit", "Write") and not file_path contains "test") between a and c
| extend took = c.time - a.time
| summarize n = count(), sessions = dcount(session), typical = avg(took) by a.tool
| top 10 by n
```

- Stages: `where`, `match`, `extend`, `project`, `summarize` (`count`, `countif`, `dcount`, `sum`,
  `min`, `max`, `avg`, `by` keys including `bin(time, 1d)`), `top`, `sort`, `take`, `count`.
- Step columns: `tool`, `failed`, `ok`, `time`, `out_bytes`, `step`, `session`, `agent`, and input
  fields as `input.file_path` or just `file_path`. After `match`, name the element: `a.time`.
- Operators: `== != < <= > >=`, `in (...)`, `not in`, `has`, `contains`, `startswith`, `endswith`,
  `matches` (glob), case-insensitive (`_cs` for exact case); `and or not`; `+ - * /` on numbers,
  times and durations (`7d`, `3h`, `10m`, `30s`, `250ms`); `ago()`, `now()`, `bin()`, `strlen()`,
  `tolower()`, `iff()`, `isnull()`.
- `match`: `then` = any later turn on the same path, `next` = the very next turn; `{m}`, `{m,n}`,
  `{m,}` = consecutive turns; `within N` turns from first to last; `no (P) between a and c`; one
  compared field (`a.file_path == c.file_path`) splits steps by value. Calls in one model turn are
  unordered. A match is reported at the turn that completes it, binding the latest earlier steps
  that allow it; matches do not overlap; each branch of a forked session is matched, its shared
  start once.

How a query runs: leading `where` stages and each element's test are compiled against the
dictionaries (a test over one column is evaluated once per distinct value; tests on tool and
outcome become a table over the 256 shape codes); sessions lacking a tool an element needs are
skipped; the matcher makes one pass per session over its turn tree, in parallel across sessions;
later stages work on the matched rows. Errors point at the query with a caret; unknown columns
and mismatched types are refused before running.

`tdb check-queries` generates random patterns from the store's own tools and compares the engine
with `src/query/reference.rs`, a brute-force evaluator that reads tools, inputs and outcomes from
the stored messages, lists every root-to-leaf path of each session's turn tree and tries every
binding. Planted bugs in the matcher (exclusions ignored, `next` treated as `then`, repeats
allowed to skip turns) each make it fail.

## Head-to-head against PostgreSQL

`scripts/h2h.sh` (needs Homebrew `postgresql@18`) builds a store from a sample of the local logs,
loads the same data into a throwaway PostgreSQL 18 cluster, runs the same 16 queries and 3 read
workloads on both, and compares answers. `src/h2h.rs` holds the queries; `src/pg.rs` is a small
client (wire protocol, simple queries, `COPY`).

PostgreSQL gets the usual tricks: every distinct message stored once by hash, requests as arrays of
message ids, a step table with indexes and the two input fields the queries use pulled out as
columns, and for patterns a materialized table of root-to-leaf turn paths, so window-function SQL
gives exact answers on forked sessions. Pattern queries are compared on the set of sessions with a
match (what SQL answers naturally; it equals "the engine reports at least one match"), aggregations
on their rows, reads on the bytes returned. Settings: shared_buffers 256 MB, work_mem 32 MB, up to 4
parallel workers, JIT off, C collation. PostgreSQL is reached over loopback TCP, so every query
pays a client round trip (about 0.1 ms) that the in-process engine does not; that matters for the
sub-millisecond rows below.

Measured 2026-11-21 (after the catalog and merge layout) on 200 log files: 156 sessions, 15,663
requests, 20,248 steps, 118 MB of distinct message text. Every answer matched (16 of 16 queries, 756
of 756 reads). Each query best of 7; a second run on the final file formats gave the same TranscriptDB times
within 0.15 ms. The earlier best-of-three run on the old layout had
the same picture; against it, one-thread times here are within about 20%, with the smallest queries
(under 0.1 ms) up to 50% slower from reading each session through the cache.

| | TranscriptDB | PostgreSQL |
|---|---|---|
| Disk (tables, indexes, TOAST) | 62.3 MB | 145.7 MB (2.34x) |

| Query | TDB, 1 thread | TDB, 8 threads | PostgreSQL | PG / TDB (1 thread) |
|---|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 0.61 ms | 0.27 ms | 192 ms | 316x |
| Bash fails, only test files edited, Bash passes | 0.62 ms | 0.28 ms | 351 ms | 571x |
| 3 Read/Grep turns in a row, then Edit | 0.18 ms | 0.12 ms | 12.8 ms | 70x |
| Bash fails, then passes the next turn | 0.40 ms | 0.18 ms | 11.6 ms | 29x |
| Edit, then Bash within 3 turns | 0.24 ms | 0.14 ms | 8.8 ms | 37x |
| Write a file, later Read the same file | 0.47 ms | 0.40 ms | 1.54 ms | 3.3x |
| 3 failing turns in a row | 0.35 ms | 0.17 ms | 11.1 ms | 32x |
| WebSearch, then WebFetch the next turn | 0.09 ms | 0.10 ms | 10.8 ms | 116x |
| Steps per tool, top 10 | 0.22 ms | 0.26 ms | 0.68 ms | 3.1x |
| Failure rate per tool | 0.25 ms | 0.28 ms | 1.74 ms | 6.9x |
| Bash steps and failures per week | 0.28 ms | 0.32 ms | 1.63 ms | 5.9x |
| Sessions with the most failed steps | 0.09 ms | 0.12 ms | 0.87 ms | 9.5x |
| Most edited files | 0.37 ms | 0.39 ms | 0.47 ms | 1.3x |
| Average output size per tool | 0.24 ms | 0.28 ms | 1.86 ms | 7.8x |
| Sessions using browser tools, per day | 0.10 ms | 0.12 ms | 2.55 ms | 27x |
| Commands running `git commit` | 13.6 ms | 4.1 ms | 18.1 ms | 1.3x |

| Read (median / p95 per item) | TranscriptDB | PostgreSQL |
|---|---|---|
| A request's messages in order | 0.35 / 1.5 ms | 1.04 / 3.6 ms |
| 20 steps with their call and result messages | 0.10 / 0.22 ms | 0.41 / 1.1 ms |
| A whole session's messages | 0.27 / 1.4 ms | 0.79 / 3.7 ms |

Every query is faster on one thread, so the design's bar ("each query faster") is met on this
sample; the closest are `git commit` and most edited files (1.3x each). The first run of the
comparison (before the fixes below) lost three: `command contains ...` over long commands (278 ms
against 17), the same-file pattern (7.3 ms against 1.7) and most edited files (0.72 against 0.46).
What changed:

- long input values (over 4 KB) keep their byte position in the call's stored message, so a test
  reads just those bytes (a frame or two) instead of rebuilding and parsing the whole message;
  values up to 4 KB (98% of shell commands) are dictionary-coded, with a hash-keyed index;
- a test over one input field is worked out per distinct value the first time a step reaches it;
  `contains` with a plain ASCII pattern runs on the raw JSON text and decodes only on a hit
  (a raw miss is a real miss when there are no `\u` escapes);
- `isnull`/`isnotnull` test presence without looking at values;
- `summarize` over step columns groups on dictionary codes (a dense array for one key), with flat
  accumulators; `top` and `sort | take` select the rows they keep instead of sorting all;
- a compared field (`a.file_path == b.file_path`) skips values that an element never has, and each
  scan starts at the first step that can begin a match;
- reads cache decompressed frames per thread and fetch a request's opaque values (signatures,
  images) with a few large reads instead of one each.

## Reading Claude Code's logs

`src/claude.rs` turns `~/.claude/projects/**/*.jsonl` into request bodies. Newer Claude Code
versions log each model call as an `api-request` line (params, the message list as `base`,
`keep` and `tail` hashes, the shape hash, cache-marker paths and `requestDigest`), plus
`api-request-blob` and `api-request-shape` lines. Rules found by checking against
`requestDigest` (the sha256 of the body with keys sorted):

- messages without a blob line come from the transcript in four forms: a user line as is, one
  assistant line, all assistant lines sharing a message id, and a run of user lines merged;
- a message whose content is a string is sent as one text block;
- some blob lines are hashed in that block form while logging the string form.

With these, the reader builds the canonical body and checks it against `requestDigest` before
the store sees it, so the rebuild check compares against Claude Code's own hash, not against
our reconstruction. Real logs are only read at run time; tests use synthetic data.

## Checks

```
scripts/test.sh            # unit + end-to-end tests on synthetic sessions
scripts/check-rebuild.sh   # ingest every local log, merge, rebuild every request, sizes, read times
scripts/crash-test.sh 1000 # SIGKILL the writer 1000 times at random moments
scripts/h2h.sh             # head-to-head against PostgreSQL (needs postgresql@18)
tdb growth --store DIR     # grow a synthetic store 1x..16x; open time, memory, reads, rewrites per round
```

`check-rebuild.sh` writes its store outside the repo (`$TMPDIR/transcriptdb-check`) and deletes it
at the end unless `KEEP=1`. Both it and the crash test exit non-zero on any failure.

## Measured (2026-11-21, every Claude Code log on one Mac, Apple Silicon, 10 cores)

| | Result |
|---|---|
| Request lines in the logs | 157,253 (126,886 distinct ids; resumed sessions repeat requests) |
| Rebuilt from the logs, equal to `requestDigest` | all but 20 (a message is missing from the log) |
| Ingest | 126,866 requests, 157.1 GB of bodies in 228 s; store write path 1.04 GB/s |
| Store size | 561.0 MB: frames 191.6 MB, blob files 330.7 MB (signatures 156.5 MB, images 156.3 MB, pieces 13.8 MB, tables), other segment bytes 38.8 MB; 280x smaller than the bodies |
| Store size with `--drop thinking,images` | 240.2 MB; 124,444 requests equal the log's body with those values blanked, the rest byte for byte |
| Open | 10–17 ms; 0.4 MB of catalogs for 17–20 files is all that is read |
| Storage runs per request | as ingested (with automatic merges): 95.7% one, max 4; after `compact`: 98.8% one, max 3 |
| Rebuild check | 126,866 of 126,866 byte for byte (126,890 on a later ingest); 157 GB rebuilt on 10 threads at 6.1–6.9 GB/s with other load on the machine (9.3 GB/s on a quieter run of the previous file format) |
| Rebuild one request (mean 1.23 MB), warm | median 0.43 ms, p95 1.9 ms, p99 3.5 ms |
| 40 messages in a row / whole session | median 0.11 ms / 0.31 ms |
| Steps | every step of 1,063 sessions; engine and reference agree on every distinct step; 0 wrong `prev`, 0 results without a call |
| Crash test | 1,000 SIGKILLs during writes, flushes and merges: 12,499 acknowledged, 0 lost, 0 wrong bytes, steps all match; recovery removed 1,944 files left by interrupted flushes and merges |
| Queries vs reference (2026-11-21) | 100 generated queries, 758 real sessions up to 120 steps: engine = brute force in every session (337,573 matches); 300 on synthetic: same |

### Growth (synthetic sessions, 1x to 16x)

`tdb growth --rounds 5 --base 2000 --flush-mb 16 --wake 0.1`: each round doubles the number of
synthetic sessions (45 requests each, 64 sessions at a time with their requests interleaved); 10%
of sessions pause after 30 requests and take the rest a round later, after their data was flushed
and merged. After each round a fresh process opens the store, does 2,000 random request rebuilds
(cold: the first 200; warm: the last 1,000) and one query over every step. Cache budget 256 MB.

| Sessions | Requests | Store | Open | Catalogs | Memory after open | Random read, cold / warm (median) | Query over all steps | Memory after the query | Bytes written / store | Ingest |
|---|---|---|---|---|---|---|---|---|---|---|
| 2,000 | 87 k | 109 MB | 15 ms | 0.3 MB | 3.0 MB | 0.07 / 0.06 ms | 0.03 s | 101 MB | 1.83x | 475 MB/s |
| 4,000 | 177 k | 222 MB | 16 ms | 0.6 MB | 3.4 MB | 0.08 / 0.06 ms | 0.06 s | 165 MB | 1.85x | 479 MB/s |
| 8,000 | 354 k | 445 MB | 15 ms | 1.1 MB | 4.5 MB | 0.10 / 0.06 ms | 0.13 s | 233 MB | 2.25x | 436 MB/s |
| 16,000 | 708 k | 892 MB | 10 ms | 2.3 MB | 5.8 MB | 0.12 / 0.08 ms | 0.43 s | 247 MB | 2.24x | 455 MB/s |
| 32,000 | 1.42 M | 1.78 GB | 14 ms | 4.5 MB | 8.3 MB | 0.47 / 0.33 ms | 1.06 s | 260 MB | 2.68x | 397 MB/s |

Open time stays flat, memory with a query running stays at the cache budget, ingest holds, and
merges wrote 2.7 bytes for every byte stored (flushes included). Catalogs grow with the data, about
3 bytes per request (Bloom filters). Random reads slow down once the sessions no longer fit the
cache: at 32,000 sessions most reads decode the session's part from disk (about 0.3–0.5 ms); that
is the cold cost, not a curve that keeps rising. The query reads every session, so it grows with
the data. An earlier version of this layout lost ingest speed with the number of blob files (121
MB/s at 32,000 sessions): each new value was looked up in every blob file's filter, and each false
positive loaded and sorted a whole table. Tables in small blocks and 14-bit filters fixed that.

Against the Python proof of concept (48-session sample): size per byte of distinct text 0.50 vs
0.51; rebuild median 0.41 vs 0.85 ms; replay 0.29 vs 0.41 ms. The proof of concept's
"11,718 of 11,718 byte for byte" was checked against its own reconstruction; the numbers here are
checked against Claude Code's digest.

The three patterns are the proof of concept's; it timed them at 5.9, 7.1 and 1.8 ms (Python,
288k steps from transcripts, flat step lists) and PostgreSQL window SQL at 146, 168 and 90 ms.
Different data and semantics (turn trees here), so the PostgreSQL comparison still has to be
run on the same steps.

Not measured yet: cold-disk reads, size against a zstd `--long` archive of the same requests (needs
~150 GB of scratch disk), torn log records from real power loss (SIGKILL cannot tear a `write`;
a unit test covers the path).

## Command line

```
tdb ingest-claude --store DIR [--logs DIR] [--max-files N --seed S] [--sync full|fsync|os]
tdb check-claude  --store DIR [--logs DIR]
tdb stats         --store DIR
tdb compact       --store DIR
tdb bench         --store DIR [--n 300]
tdb crash-test    --store DIR [--kills 1000] [--sync full|fsync|os]
tdb check-steps   --store DIR
tdb steps         --store DIR [--top 15]
tdb serve         --store DIR [--port 8377]
tdb synth         --store DIR [--sessions 20 --per 40]
tdb query         --store DIR --q 'QUERY' [--limit 30] [--threads N] [--repeat N]
tdb check-queries --store DIR [--n 300] [--max-steps 400] [--seed S]
tdb h2h           --store DIR [--pg 127.0.0.1:5455] [--no-load x] [--reps 5] [--reads 200]
```

## On disk

```
MANIFEST              live segment and blob files, id counters, checkpoint, first live log (crc32c)
wal-NNNNNN.log        [len][crc32c][record]...
seg-NNNNNN.tdb        frames per session part | blocks: groups of parts (messages + requests,
                      steps), request id table (2048 ids a block), session directory | catalog | footer
blob-NNNNNN.tdb       opaque values (raw) and pieces (zstd) | tables in blocks: by id (1024 a block),
                      hash index (2048 a block) | catalog | footer
LOCK                  held with flock while a process has the store open
```

## What's next

1. Derived columns (rules at write time, backfilled when a merge rewrites a session), then text
   search with an index so substring tests stop scanning every distinct value.
2. Queries that skip whole segments from their catalogs (time range, tool names) before reading
   any session.
3. Flat catalog memory: Bloom filters and block lists loaded on demand (partitioned, as RocksDB
   does) instead of held for every file; and one hash index for finding existing pieces and
   opaque values, merged like the segments, instead of one filter per blob file (blob files
   grow by one per flush, so per-file checks grow with the data).
4. Sessions that woke up keep extra parts until a merge reaches their segments; segments at the
   size cap are never merged again, so a periodic pass should fold those parts back.
5. A bigger head-to-head on the full logs, and a multi-terabyte run on an external disk.
