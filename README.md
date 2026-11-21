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
   signatures, images) are cut out and kept as raw bytes in a separate file per segment
   (decoded when re-encoding gives the same text back, kept as text otherwise).
5. **Shared pieces.** Skeletons, system blocks, tool lists and marker texts are interned by
   hash across all sessions.
6. **Log.** Everything the request adds goes into one checksummed record in the write-ahead log
   (`src/wal.rs`). `Db::commit` writes and syncs it (`F_FULLFSYNC` on macOS by default); only
   then does the request count as saved. In-memory state only ever changes by applying a log
   record, live or during recovery, so recovery rebuilds exactly the same state.
7. **Flush** (`src/segment.rs`). Every 64 MB, the in-memory part becomes an immutable segment:
   per session, the new nodes laid out *main line first* (`src/layout.rs`) and cut into 64 KB
   zstd frames with an offset table; the pieces; the request records; an index. The manifest
   (`src/manifest.rs`) is replaced atomically (write, sync, rename, sync the directory) and old
   log files are deleted.
8. **Merge** (`Db::compact`). Rewrites all segments into one, each session in one stretch,
   stored main line first over its whole tree, so most requests become a single run of frames.
9. **Recovery** (`Db::open`). Reads the manifest, deletes files it does not list (a crash during
   a flush or merge), loads the segments, replays the log after the checkpoint and cuts a torn
   last record.

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
```

`check-rebuild.sh` writes its store outside the repo (`$TMPDIR/transcriptdb-check`) and deletes it
at the end unless `KEEP=1`. Both it and the crash test exit non-zero on any failure.

## Measured (2026-11-20, every Claude Code log on one Mac, Apple Silicon, 10 cores)

| | Result |
|---|---|
| Request lines in the logs | 153,504 (123,156 distinct ids; resumed sessions repeat requests) |
| Rebuilt from the logs, equal to `requestDigest` | 153,484; 20 not rebuildable (a message is missing from the log) |
| Ingest | 123,117 requests, 148.3 GB of bodies in 223 s; store write path ~1.0 GB/s |
| Store size | 511.8 MB: frames 186.8, opaque 294.1, pieces 12.2, index 18.8 (290x smaller than the bodies) |
| Message frames | 623.6 MB of distinct message bytes in 186.8 MB (3.3x) |
| Storage runs per request | as flushed: 85.6% one, max 3; after merge: 98.8% one, 1.2% two, max 3 |
| Rebuild check | 123,117 of 123,117 byte for byte; 148 GB rebuilt in 24 s on 10 threads |
| Rebuild one request (mean 1.28 MB), warm | median 0.41 ms, p95 1.9 ms, p99 6.0 ms |
| 40 messages in a row / whole session | median 0.11 ms / 0.29 ms |
| Crash test | 1,000 SIGKILLs, 13,059 acknowledged, 0 lost, 0 wrong bytes |
| Steps (2026-11-21, 125,905 requests) | 146,175 steps in 1,062 sessions; engine and reference agree on all 141,513 distinct steps; 0 wrong `prev`, 0 results without a call |
| Step columns | 134 tools, 192 input fields, 108,605 distinct short values; shape strings 146 KB; index +5 MB |
| Queries vs reference (2026-11-21) | 100 generated queries, 758 real sessions up to 120 steps: engine = brute force in every session (337,573 matches); 300 on synthetic: same |
| Bash, Edit, Bash, no Read between (146k steps) | 4.5 ms on 1 thread, 1.2 ms on 8; 1,576 matches in 33 sessions |
| Bash fails, only test files edited, Bash passes | 5.5 ms on 1 thread (1.8 ms of it planning), 2.8 ms on 8 |
| 3 Read/Grep in a row, then Edit | 1.3 ms on 1 thread, 0.4 ms on 8 |
| Bash steps per week with failures (`summarize ... by bin(time, 7d)`) | 16.6 ms, nearly all in row-by-row aggregation |

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
```

## On disk

```
MANIFEST              live segments, checkpoint, first live log file (crc32c)
wal-NNNNNN.log        [len][crc32c][record]...
seg-NNNNNN.tdb        frames per session chunk | pieces | zstd index | footer
seg-NNNNNN.opq        opaque values, raw
```

## What's next

1. The head-to-head against PostgreSQL on the same steps and messages (needs PostgreSQL
   installed locally).
2. Derived columns (rules at write time, backfilled on merge), then text search.
