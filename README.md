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
7. **Flush** (`src/segment.rs`, `src/blob.rs`, `src/index.rs`). Every 64 MB (`--flush-mb`), the in-memory
   part becomes an immutable level-0 segment and an index run, and its values are appended to a blob
   file. The segment holds a *part* of each session it touched: the new nodes laid out *main
   line first* (`src/layout.rs`) and cut into 64 KB zstd frames, plus the session's new requests
   and steps, encoded into compressed blocks. The new pieces and opaque values go to the end of
   the last blob file (a new one once it passes 1 GB), grouped by session in the order requests
   read them. The index run holds what the flush added, findable by hash (pieces, opaque values,
   request ids, session keys) and by id (where each piece and opaque value is). The manifest
   (`src/manifest.rs`) is replaced atomically (write, sync, rename, sync the directory) and old
   log files are deleted.
8. **Merge.** Index runs merge first: four neighbours of one size tier at a time, up to 1 GB, so
   a lookup reads a few runs whatever the store's size. Then, once four level-0 segments wait,
   they are merged: each session's run of consecutive parts in them is rewritten as one part,
   main line first over that part of its tree, into level-1 segments split by size. Merged
   segments merge again four of one size tier at a time, up to 256 MB, so each byte is rewritten
   about once per tier. Blob files are never rewritten. `Db::compact` merges every segment.
   A session that takes writes again after its data reached full-size segments (a resumed
   conversation, or a long one between turns) would stay in parts spread over segments that
   never merge again. So a merge whose output reaches 256 MB, the last time that data moves,
   *folds* a session into one part, reading its other parts from wherever they are, once it has
   gone quiet (its newest part is in the merge and it took no writes in the last two flushes) or
   when it would otherwise be left in more than four parts (`max_parts`). Folding there writes a
   folded session once, at full size, instead of sending it up the size tiers again. The parts
   a fold replaces stay in their segments and are never read: on read, a part covered by
   another part is skipped. The manifest counts each segment's replaced parts, and a segment
   that is half replaced parts is rewritten without them.
9. **Recovery** (`Db::open`). Reads the manifest, deletes files it does not list (a crash during
   a flush or merge), cuts blob files back to the length the manifest gives them (a flush that
   appended and did not finish), reads each segment's and index run's catalog, replays the log
   after the checkpoint and cuts a torn last record.

## What stays in memory

Memory hardly grows with the stored data: fixed parts plus one small catalog per file, with
nothing per key or per block in it.

- **Catalogs.** Per segment: its session id range, counts, time range, the tool and field names
  its steps use, a Bloom filter of the sessions it holds (14 bits per session part), and the
  first session of each 64 KB directory block, plus, in the manifest, what folds replaced in
  it (six numbers, only for segments holding such parts). Per index run: counts, offsets, its piece and
  opaque id ranges, and the first key of each fence page (one per two million keys). Per blob
  file: its length. Opening a store reads the manifest and these catalogs, nothing else: 17.8 KB
  for the 9 files of every Claude Code log on this Mac, and 67 KB at 1.4 million synthetic
  requests (see Growth); the session filters are the one part that grows, 2 bytes per session part.
- **Sessions taking writes.** A session's message tree index, pending tool calls and step tails
  stay in memory while it takes writes, and leave two flushes after its last write. A session
  that wakes up later is read back from its parts.
- **Unflushed data** (up to 64 MB), and **caches with fixed budgets** (256 MB in all by default)
  for sessions, steps, segment blocks, directory blocks, index blocks and pieces.

A session is a self-contained unit: its messages, requests and steps (with its own dictionary of
input values; tool and field names are shared) live in its parts and are read on demand. A
request is addressed as (session, index in the session). Finding a session by key, a request by
id, or an existing piece or opaque value by hash asks the index: in each run, newest first, one
fence page says which key page could hold the key, that page's own Bloom filter (2048 keys, 14
bits each) says whether it may, and only then is the page read. Every part of a run has a fixed
width, so blocks are found by arithmetic. A hit on a request id or session key is checked against
the stored id, so a hash collision would stop with an error rather than mix two requests up.

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

Measured 2026-11-22 (on the layout with the index and appended blob files) on 200 log files: 146
sessions, 18,533 requests, 21,477 steps, 133 MB of distinct message text (the logs grew since the
2026-11-21 run, so the 200 files sampled differ). Every answer matched (16 of 16 queries, 746 of 746
reads). Each query best of 7. The samples differ, so this is not a like-for-like comparison with the
2026-11-21 run on the previous layout (156 sessions, 15,663 requests); most one-thread times are within
10% of it, the largest change being most edited files (0.54 against 0.37 ms).

| | TranscriptDB | PostgreSQL |
|---|---|---|
| Disk (tables, indexes, TOAST) | 67.5 MB | 168.4 MB (2.49x) |

| Query | TDB, 1 thread | TDB, 8 threads | PostgreSQL | PG / TDB (1 thread) |
|---|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 0.67 ms | 0.22 ms | 197 ms | 293x |
| Bash fails, only test files edited, Bash passes | 0.75 ms | 0.29 ms | 398 ms | 528x |
| 3 Read/Grep turns in a row, then Edit | 0.20 ms | 0.13 ms | 13.0 ms | 64x |
| Bash fails, then passes the next turn | 0.42 ms | 0.19 ms | 11.6 ms | 27x |
| Edit, then Bash within 3 turns | 0.26 ms | 0.13 ms | 8.8 ms | 34x |
| Write a file, later Read the same file | 0.37 ms | 0.38 ms | 1.82 ms | 4.9x |
| 3 failing turns in a row | 0.37 ms | 0.17 ms | 11.3 ms | 31x |
| WebSearch, then WebFetch the next turn | 0.07 ms | 0.08 ms | 10.9 ms | 161x |
| Steps per tool, top 10 | 0.23 ms | 0.26 ms | 0.80 ms | 3.5x |
| Failure rate per tool | 0.26 ms | 0.30 ms | 1.85 ms | 7.1x |
| Bash steps and failures per week | 0.29 ms | 0.33 ms | 1.75 ms | 6.0x |
| Sessions with the most failed steps | 0.10 ms | 0.11 ms | 0.99 ms | 10x |
| Most edited files | 0.54 ms | 0.53 ms | 0.70 ms | 1.3x |
| Average output size per tool | 0.25 ms | 0.28 ms | 1.96 ms | 7.9x |
| Sessions using browser tools, per day | 0.07 ms | 0.10 ms | 2.52 ms | 36x |
| Commands running `git commit` | 14.2 ms | 3.3 ms | 18.9 ms | 1.3x |

| Read (median / p95 per item) | TranscriptDB | PostgreSQL |
|---|---|---|
| A request's messages in order | 0.46 / 1.5 ms | 1.31 / 3.9 ms |
| 20 steps with their call and result messages | 0.10 / 0.23 ms | 0.42 / 1.4 ms |
| A whole session's messages | 0.31 / 1.8 ms | 0.92 / 8.5 ms |

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

## Measured (2026-11-22, every Claude Code log on one Mac, Apple Silicon, 10 cores)

| | Result |
|---|---|
| Request lines in the logs | 157,400 (127,033 distinct ids; resumed sessions repeat requests) |
| Rebuilt from the logs, equal to `requestDigest` | all but 20 (a message is missing from the log) |
| Ingest | 127,013 requests, 157.3 GB of bodies in 219 s; store write path 1.08 GB/s |
| Store size | 571.1 MB: frames 191.9 MB, blob files 335.1 MB (opaque values 321.3 MB, pieces 13.9 MB), index runs 8.6 MB, other segment bytes 35.5 MB; 275x smaller than the bodies (the previous layout: 569 MB on the same logs) |
| Store size with `--drop thinking,images` | 249.8 MB (index runs 8.8 MB, blob files 14.0 MB); 124,792 requests equal the log's body with those values blanked, the other 2,254 byte for byte, none different |
| Open | 4–11 ms; 17.8 KB of catalogs for 9 files (1 segment, 7 index runs, 1 blob file) is all that is read; 2.6 MB resident after open |
| Storage runs per request | as ingested (with automatic merges): 95.8% one, max 4; after `compact`: 98.8% one, max 3 |
| Rebuild check | 127,013 of 127,013 byte for byte; 157 GB rebuilt on 10 threads at 9.6 GB/s (the previous layout: 9.2–9.3 GB/s on a quiet machine, 6.1–7.5 GB/s while other work kept the load at 7–9) |
| Rebuild one request (mean 1.24 MB), warm | median 0.40 ms, p95 1.8 ms, p99 2.6 ms |
| 40 messages in a row / whole session | median 0.10 ms / 0.27 ms |
| Recheck with folding (2026-11-23) | the logs have grown to 127,477 requests: all byte for byte, steps match the reference in all 1,066 sessions; ingest 1.01 GB/s through the write path; store 575.4 MB; 7 merges during ingest, none reaching 256 MB, so nothing folded; rebuild 8.7 GB/s on 10 threads (load 3.5), one request median 0.39 ms |
| Steps | every step of 1,063 sessions; engine and reference agree on every distinct step; 0 wrong `prev`, 0 results without a call |
| Crash test (2026-11-23) | 1,000 SIGKILLs during writes, flushes, appends to blob files, merges of segments and index runs, folds and rewrites of half-replaced segments: 26,170 acknowledged, 0 lost, 0 wrong bytes, steps all match, request count equal to the requests listed; at least 77 folds and 2 rewrites finished between kills; recovery removed 1,108 files left by interrupted flushes and merges. (The writer now uses 128 KB segments and compacts on 3% of commits, where it used 1 MB and 50%: with those, nearly every child was killed mid-compaction and no merge reached full size, so nothing folded.) |
| Queries vs reference (2026-11-21) | 100 generated queries, 758 real sessions up to 120 steps: engine = brute force in every session (337,573 matches); 300 on synthetic: same |

### Growth (synthetic sessions, 1x to 16x)

`tdb growth --rounds 5 --base 2000 --flush-mb 16 --wake 0.1`: each round doubles the number of
synthetic sessions (45 requests each, 64 sessions at a time with their requests interleaved); 10%
of sessions pause after 30 requests and take the rest a round later, after their data was flushed
and merged. After each round a fresh process opens the store, does 2,000 random request rebuilds
(cold: the first 200; warm: the last 1,000) and one query over every step. Cache budget 256 MB.

| Sessions | Requests | Store | Open | Catalogs | Memory after open | Random read, cold / warm (median) | Query over all steps | Memory after the query | Index runs | Sessions in more than one part | Bytes written / store | Ingest |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2,000 | 87 k | 112 MB | 19 ms | 6.8 KB | 2.4 MB | 0.15 / 0.07 ms | 0.03 s | 101 MB | 3 | 125 | 1.90x | 488 MB/s |
| 4,000 | 177 k | 228 MB | 12 ms | 11.3 KB | 2.5 MB | 0.15 / 0.09 ms | 0.06 s | 163 MB | 6 | 297 | 1.91x | 479 MB/s |
| 8,000 | 354 k | 457 MB | 9 ms | 18.9 KB | 2.5 MB | 0.22 / 0.19 ms | 0.18 s | 235 MB | 9 | 122 | 2.30x | 438 MB/s |
| 16,000 | 708 k | 915 MB | 18 ms | 39.0 KB | 2.7 MB | 0.32 / 0.27 ms | 0.52 s | 259 MB | 14 | 743 | 2.32x | 427 MB/s |
| 32,000 | 1.42 M | 1.83 GB | 11 ms | 67.4 KB | 2.7 MB | 0.50 / 0.36 ms | 1.03 s | 278 MB | 20 | 180 | 2.75x | 388 MB/s |

Open time and memory after open stay flat (2.4–2.7 MB), memory with a query running stays near
the cache budget, ingest holds, and merges wrote 2.75 bytes for every byte stored (flushes
included). Catalogs grow only with the number of session parts, 2 bytes each for the segments'
session filters: 67 KB at 32,000 sessions, where the previous layout held 4.5 MB (3 bytes per
request, nearly all Bloom filters over request ids and content hashes, one set per file, and one
blob file per flush: 284 of them here, against 1 now). Index runs grow with the log of the index's
size, up to three waiting per size tier. On synthetic data the store is 2.7% bigger than before
(index entries have a fixed width and are not compressed; on the real logs, 0.3%).

Random reads slow down once the sessions no longer fit the cache: most reads then decode the
session's part from disk (about 0.3–0.5 ms); that is the cold cost, not a curve that keeps rising.
Read times move with whatever else the machine is doing (this run had a load of 2–3 from other
work): the old and new layouts run back to back on the first three rounds gave warm medians of
0.057 / 0.074 / 0.140 ms (old) and 0.056 / 0.059 / 0.091 ms (new). The query reads every session,
so it grows with the data.

**Folding (2026-11-23).** The table is from 2026-11-22; rerun with folding on, it gives the same
sizes, merges and bytes written, because at 256 MB segments this 1.83 GB store has only one or two
merges that reach full size and no session folds by the end. The sessions in more than one part are
the ones that woke in the latest rounds; their new parts have not reached a full-size merge yet. The
same run with 64 MB segments (`--seg-mb 64`) is in the state a store a few times bigger would be
in at 256 MB:

| 64 MB segments, 32,000 sessions | Folding off (`--max-parts 0`) | Folding on (default) |
|---|---|---|
| Sessions folded into one part | 0 | 401 |
| Sessions in more than one part at the end | 593 | 180 |
| Bytes written by merges | 2,645 MB | 2,667 MB (+0.8%) |
| Replaced parts left in segments | 0 | 6.6 MB (0.4% of the store) |
| Segments rewritten to drop them | 0 | 0 (none was close to half replaced) |

The 180 left in two parts with folding on are again sessions that woke in the last round, waiting
for their newest part to reach a full-size merge: folding waits on purpose, so a session still
taking writes is not rewritten over and over. Read times in the two runs are within the machine's
noise (warm medians at 32,000 sessions 0.34 and 0.37 ms, load 3–5). The worst case is the unit
test's tiny setup (96 KB segments, where nearly every merge reaches full size and sessions go quiet
after one flush): folding raised merge writes by 70% (10.8 to 18.3 MB), and sessions woken five
times ended in one part instead of five or six. Two costs remain: a replaced part still costs its
session a directory lookup on read (not a block read) until its segment is rewritten, and a session
whose newest part lands in a full-size merge while it is still taking writes (possible when a merge
of fresh data reaches full size) is not folded as quiet, so it keeps up to four parts.

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
MANIFEST              live segments (with what folds replaced in each), index runs (in id order)
                      and blob files (with the length each is durable at), id counters,
                      checkpoint, first live log (crc32c)
wal-NNNNNN.log        [len][crc32c][record]...
seg-NNNNNN.tdb        frames per session part | blocks: groups of parts (messages + requests,
                      steps) | session directory in 64 KB blocks, each entry saying where its
                      part's blocks are | catalog | footer
idx-NNNNNN.tdb        key pages (2048 sorted (kind, hash, value) keys + their Bloom filter, each
                      with a crc32c) | fence pages (first key of 1024 pages) | piece table and
                      opaque value table (1024 a block, by id) | catalog | footer
blob-NNNNNN.dat       pieces (zstd, one frame each) and opaque values (raw), appended per flush
LOCK                  held with flock while a process has the store open
```

## What's next

1. Derived columns (rules at write time, backfilled when a merge rewrites a session), then text
   search with an index so substring tests stop scanning every distinct value.
2. Queries that skip whole segments from their catalogs (time range, tool names) before reading
   any session.
3. Merges run inside the flush that triggers them; a background thread would keep writes going
   while a big merge runs (index merges are capped at 1 GB and segment merges at 256 MB).
4. A bigger head-to-head on the full logs, and a multi-terabyte run on an external disk (the
   index's filters are about 1.8 bytes per key and are read through the cache, so at that size
   ingest speed depends on how much of them the cache holds).
