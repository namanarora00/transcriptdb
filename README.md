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
  for sessions, steps (with each session's turn tree and steps by shape code), segment blocks,
  directory blocks, index blocks, pieces and decoded step-summary blocks (1/12 of the budget).

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
outcome become a table over the 256 shape codes, and an element's whole bit mask comes from one
lookup by shape code when tool and outcome decide every test); sessions lacking a tool an element
needs are skipped; the matcher makes one pass per session over its turn tree, in parallel across
sessions (on one thread it runs on the caller's thread); later stages work on the matched rows.
Errors point at the query with a caret; unknown columns and mismatched types are refused before
running. A session's turn tree and its steps grouped by shape code are worked out when its steps
are read and kept with them in the step cache (counted in its budget); a session taking writes
drops them when a step is added. Queries whose tests read no input values (tool, outcome,
session, time and size tests, compared fields) take sessions' steps from a second cache that
holds them without their values (about 13 KB per session on the real logs, against 80 KB with
them): on all the logs the steps with their values are 88 MB, more than the step cache's share
of the default budget, and every pattern query used to read every session again.

**Compared fields.** When every element of a pattern is compared (`a.file_path == b.file_path`),
only steps with the value can bind, so for each value the matcher visits just the turns holding
it, in tree order, and carries partial matches from one visited turn to the next one below it as
waiting through each turn between would: repeats must be done, the next element must allow any
later turn (not `next`), the `within` window must still be open at the last turn between, and no
exclusion the match is inside may hit there (counted per turn along each root path). Values are
found by dictionary code in an array, long values by text. With some elements not compared, each
value runs the full pass as before.

**Grouping by an input field** (`by file_path`, `dcount(command)`) groups by the decoded value:
`"a\/b"` with `"a/b"`, `1.0` with `1`, and `null` with steps that lack the field (`dcount` does
not count it). Until now the columnar path grouped by the raw JSON text, so a field holding `null`
made a second empty group; the brute-force check below found it.

### Step summaries

`src/stats.rs`, `src/query/summary.rs`. Each segment keeps summaries of the steps of its parts,
written with it (at a flush, or rebuilt by the merge that writes it) and never changed, so
recovery needs nothing beyond what it already does:

- **Part table**: per part, one row per (tool, outcome, UTC day): steps, sum, minimum and maximum
  of result sizes, earliest and latest time.
- **Field tables**: per input field. A field whose distinct values average under 256 bytes
  (paths, patterns, flags) keeps the segment's distinct values once each, in 64 KB blocks, and one
  row per (value, part, tool, outcome, day) with steps and summed result sizes; values with the
  same decoded value point at one group. A field of long text (commands, file contents, prompts;
  distinct values averaging 256 bytes or more) keeps no copy of its values: its table lists the
  steps that have it, each with where its value's text sits in the part's encoded step data, so
  reading it is a slice of a (cached) segment block. Values over 4 KB are listed by step either
  way and read from the message, as a scan reads them. Parts where a step gives a field twice are
  marked, and queries on that field read those parts from their steps.
- **Word index**: per field, the words of every string value (lower-cased runs of ASCII letters
  and digits and non-ASCII characters, 3 bytes or more), sorted, each with the values or listed
  steps that hold it. A `contains`, `has`, `startswith`, `endswith`, `==` or `in` test with a
  literal splits the pattern into words: a word inside the pattern must be a whole word of the
  value, the first may end one, the last may start one, a lone word may sit anywhere inside one
  (found with one substring search over the whole word list). Only values holding words that fit
  every such word are tested; values that are not strings are always tested. Case-sensitive tests
  use the index only for ASCII patterns, where case folding is per character.

`where ... | summarize ... by ...` is answered from these when every part of it can be: `where`
tests on tool, outcome, session, agent and at most one input field; keys tool, outcome, session,
agent, that field and `bin(time, N)` for a whole number of days; aggregates `count`, `countif`
(the same kinds of tests), `sum`/`avg` of `out_bytes`, `min`/`max` of `out_bytes` and `time`
(without a field), `dcount` of tool, session, agent or the field; and the stages after it must not
depend on the order of its rows (a `sort` or `top` on every key, or `count`): the scan returns
groups in first-seen order, the summaries in another. Tool and outcome tests are folded into a
leftover test per tool, so rows of tools that cannot pass are never looked at; field tests are
worked out once per distinct value; steps without the field are the part table less the field
table. A following `sort ... | take n` or `top n` picks its rows on the group counters (first
finding the n-th best value of an integer first key) before any row is made. Steps no summary
covers are read from their sessions as before: steps not flushed yet, parts in segments written
before summaries existed, and parts in a segment that a fold replaced (each segment holding such
parts works out once per manifest version which of its parts are live). Anything else (a `time`
test, `bin(time, 1h)`, two input fields, `sum(step)`) runs the scan.

Strings tests on a value's raw JSON text (`contains`, case-insensitive or not) now decide without
decoding whenever that gives the decoded answer: the pattern is ASCII with no character an escape
stands for (quote, backslash, control characters, and `/` when the text has `\/`), the text has
no `\u` escapes, and an occurrence counts only if it does not start right after an odd run of
backslashes; case-insensitive tests also need the text free of U+0130 and U+212A, the only two
characters whose lower case is ASCII (checked over every code point). Before, any text with an
escape or a non-ASCII character was decoded and lower-cased; and a pattern with `/` could miss a
value written with `\/`.

What the summaries cost (2026-11-23): 1.6 MB on the 200-file sample's store
(124.8 MB in all; postings 0.73 MB, words 0.34, rows and listed steps 0.24, values 0.25, part
tables 0.04), which main stored in 123.1 MB the same day: +1.2%. On every log: 8.1 MB of 585.5 MB
(1.4%). Ingest: the build before this branch and this one, run back to back on the same 200 files
(load 5–7), went through the write path at 769–817 MB/s and 870–875 MB/s, so no cost shows above
the noise; flushes write 1.3% more bytes; compacting that store takes about 0.3 s more (1.0 s
against 0.6–0.8 s), for splitting values into words and finding equal values at every flush and
merge. Memory: decoded summary blocks share 1/12 of the cache budget; steps without values 1/12
(taken from the step cache, now 1/6).

### Checks

`tdb check-queries` compares the engine with `src/query/reference.rs`, brute-force evaluators that
read tools, inputs and outcomes from the stored messages (no dictionaries, shape codes, summaries
or skipping):

- generated patterns over the store's own tools: every root-to-leaf path of each session's turn
  tree, every binding (`--n`, default 300);
- generated two-element patterns on a compared field, often with `next`, repeats, `within` and an
  exclusion: the sparse matcher's cases (`--eq`, default 150);
- generated `where ... | summarize ...` queries mixing tests the summaries answer with ones they
  do not (`time`, `out_bytes`, `strlen`, `matches`, `step`), keys including `file_path`, `command`
  and `bin(time, 1h)`, sorted, some with `take` or `top` (`--agg`, default 300). Each runs three
  ways (summaries, step scan, brute force evaluating every `where`, key and aggregate on every
  step) and all three must agree to the printed cell.

`tdb synth --rich x` makes synthetic sessions whose tool inputs exercise all of this: values over
4 KB, JSON escapes (`\"`, `\\`, `\/`, `\n`, `\t`), `\u` escapes, non-ASCII text including U+0130 and
U+212A, `null`s, numbers, arrays and a key given twice. `tests/query.rs` runs the checks on such
stores split over several segments with steps not yet flushed, after `compact`, on a store where
folds left replaced parts in older segments, and on sessions taking writes between queries.

`scripts/planted-bugs.sh` plants one bug at a time in a copy of the source and requires the
checks to fail. All 15 are caught: in the summaries, a day off by one in the part table, `countif`
ignoring the outcome, field rows counted as one step each, a word prefix taken as the whole word,
values that are not strings left untested, steps without the field left out, long values left
out, parts that folds replaced counted, steps not flushed yet left out, the `take n` cut one value
too tight, values grouped by their text; in the matcher, exclusions ignored across skipped turns,
turn trees kept after a step is added, and wrong bits in the per-shape masks; and in the raw-text
test, a hit right after an escaping backslash counted. (A 16th, the `within` window not tested
across skipped turns, was caught by nothing because it is not a bug: a match past its window
there is past it at the visited turn too, which tests it; the redundant test was dropped.)
Earlier, planted bugs in the matcher
(exclusions ignored, `next` treated as `then`, repeats allowed to skip turns) each made the
pattern check fail.

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

Measured 2026-11-23 on 200 log files: 156 sessions, 20,220 requests, 26,246 steps, 204.7 MB of
distinct message text. Every answer matched (16 of 16 queries, 756 of 756 reads). Each query best
of 7. The machine was at load 15–19 from other work during this run (PostgreSQL's times were about
1.4x lower at load 3, and the ratios held: a run at load 3 a few minutes earlier, before the last
change to `git commit`, gave 26x for most edited files and 29x for `git commit`). "Before" is this
branch's starting point (main at d2067f6) on the same sample earlier the same day, at load 3.4
(the logs grew by 153 steps between the runs).

| | TranscriptDB | PostgreSQL |
|---|---|---|
| Disk (tables, indexes, TOAST) | 124.8 MB, of which step summaries 1.6 MB | 250.1 MB (2.00x) |

| Query | TDB before, 1 thread | TDB, 1 thread | TDB, 8 threads | PostgreSQL | PG / TDB (1 thread) |
|---|---|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 0.83 ms | 0.901 ms | 0.430 ms | 665 ms | 738x |
| Bash fails, only test files edited, Bash passes | 0.78 ms | 0.970 ms | 0.654 ms | 839 ms | 865x |
| 3 Read/Grep turns in a row, then Edit | 0.27 ms | 0.235 ms | 0.174 ms | 28.5 ms | 121x |
| Bash fails, then passes the next turn | 0.48 ms | 0.480 ms | 0.255 ms | 25.8 ms | 54x |
| Edit, then Bash within 3 turns | 0.33 ms | 0.267 ms | 0.181 ms | 17.7 ms | 66x |
| Write a file, later Read the same file | 1.45 ms | 0.067 ms | 0.116 ms | 3.22 ms | 48x |
| 3 failing turns in a row | 0.42 ms | 0.409 ms | 0.199 ms | 22.1 ms | 54x |
| WebSearch, then WebFetch the next turn | 0.10 ms | 0.058 ms | 0.087 ms | 22.1 ms | 380x |
| Steps per tool, top 10 | 0.27 ms | 0.013 ms | 0.013 ms | 0.904 ms | 70x |
| Failure rate per tool | 0.32 ms | 0.025 ms | 0.025 ms | 2.19 ms | 86x |
| Bash steps and failures per week | 0.36 ms | 0.013 ms | 0.013 ms | 1.95 ms | 151x |
| Sessions with the most failed steps | 0.10 ms | 0.013 ms | 0.013 ms | 1.18 ms | 90x |
| Most edited files | 0.49 ms | 0.023 ms | 0.023 ms | 0.602 ms | 26x |
| Average output size per tool | 0.30 ms | 0.020 ms | 0.019 ms | 2.34 ms | 118x |
| Sessions using browser tools, per day | 0.14 ms | 0.020 ms | 0.020 ms | 3.56 ms | 179x |
| Commands running `git commit` | 18.6 ms | 0.224 ms | 0.219 ms | 23.5 ms | 105x |

| Read (median / p95 per item) | TranscriptDB | PostgreSQL |
|---|---|---|
| A request's messages in order | 0.53 / 6.5 ms | 3.14 / 15.6 ms |
| 20 steps with their call and result messages | 0.11 / 0.25 ms | 0.57 / 3.4 ms |
| A whole session's messages | 0.31 / 1.7 ms | 1.11 / 8.6 ms |

Every query is at least 20x faster than PostgreSQL on one thread on this sample. The closest is
most edited files (26x): its 970 groups are counted in an array from the `file_path` field table,
and about half of the 23 µs left is parsing (about 3 µs), planning (about 4) and building the 10
rows. The pattern queries that were already far ahead move with the machine's load (two runs a few
minutes apart at load 4 gave 0.67 and 0.75 ms for the first); their work is unchanged except that a
step's tests are one table lookup by shape code, one-thread runs no longer start a thread, and
sessions' steps come from the cache without values. `PG / TDB` uses unrounded times.

**All the logs.** The same comparison on every Claude Code log on this Mac (2026-11-23): 1,068
sessions, 127,902 requests, 148,265 steps, 1,059 MB of distinct message text; PostgreSQL 1,280.1 MB
on disk, TranscriptDB 585.5 MB (2.19x), of which step summaries 8.1 MB. Every answer matched (16 of
16, 900 of 900 reads). Best of 7, load 14 falling to 9. The last column is the first full run of
this branch, before steps without values were cached apart and before listed values were read from
their step blocks.

| Query | TDB, 1 thread | TDB, 8 threads | PostgreSQL | PG / TDB (1 thread) | first full run, TDB 1 thread |
|---|---|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 3.88 ms | 0.968 ms | 1,831 ms | 472x | 121 ms |
| Bash fails, only test files edited, Bash passes | 90.7 ms | 28.0 ms | 3,419 ms | 38x | 128 ms |
| 3 Read/Grep turns in a row, then Edit | 1.14 ms | 0.662 ms | 146 ms | 128x | 85.8 ms |
| Bash fails, then passes the next turn | 2.41 ms | 0.976 ms | 128 ms | 53x | 86.4 ms |
| Edit, then Bash within 3 turns | 1.46 ms | 0.700 ms | 85.1 ms | 58x | 86.4 ms |
| Write a file, later Read the same file | 0.406 ms | 0.598 ms | 15.0 ms | 37x | 94.4 ms (0.2x) |
| 3 failing turns in a row | 2.28 ms | 0.949 ms | 125 ms | 55x | 107 ms |
| WebSearch, then WebFetch the next turn | 0.326 ms | 0.503 ms | 126 ms | 385x | 127 ms |
| Steps per tool, top 10 | 0.037 ms | 0.037 ms | 4.78 ms | 131x | 0.04 ms |
| Failure rate per tool | 0.075 ms | 0.074 ms | 9.11 ms | 121x | 0.09 ms |
| Bash steps and failures per week | 0.055 ms | 0.054 ms | 16.3 ms | 298x | 0.06 ms |
| Sessions with the most failed steps | 0.050 ms | 0.050 ms | 11.2 ms | 222x | 0.06 ms |
| Most edited files | 0.109 ms | 0.107 ms | 4.43 ms | 41x | 0.12 ms |
| Average output size per tool | 0.056 ms | 0.055 ms | 9.06 ms | 161x | 0.06 ms |
| Sessions using browser tools, per day | 0.055 ms | 0.055 ms | 21.8 ms | 399x | 0.07 ms |
| Commands running `git commit` | 29.1 ms | 29.1 ms | 49.5 ms | 1.7x | 72.5 ms |

| Read (median / p95 per item) | TranscriptDB | PostgreSQL |
|---|---|---|
| A request's messages in order | 1.04 / 3.3 ms | 5.12 / 18.3 ms |
| 20 steps with their call and result messages | 0.46 / 1.1 ms | 2.01 / 4.2 ms |
| A whole session's messages | 0.69 / 2.5 ms | 2.76 / 9.4 ms |

At this size the cache budget decides. All sessions' steps with their values take 88 MB (74 MB of it
input values), more than the step cache's share of the 256 MB default: in the first full run every
pattern query read every session again (0 hits), 85–128 ms whatever the pattern, and the same-file
pattern lost to PostgreSQL. Patterns now take steps without values (13 KB a session, 14 MB for all),
which stay cached. Two queries still read values and so still miss:

- **`git commit` (1.7x):** its candidates (steps whose commands hold a word ending in `git` and one
  starting with `commit`) are confirmed on their text, read from their parts' step blocks. A large
  session's block is several MB, the blocks needed take more than the summaries' 22 MB cache, and
  each run decompresses about 300 of them; on the 200-file sample they stay cached (105x). Keeping
  a copy of long field values in the summaries (about 2% more storage on these logs; the first
  design here did that) or step blocks of bounded size would remove this.
- **Bash fails, only test files edited (38x):** `file_path contains "test"` needs the sessions'
  values; it stays far ahead because PostgreSQL's window query over 233,389 path rows takes 3.4 s.

What changed for the eight that were under 20x:

- **Aggregations** read the segments' step summaries instead of every step (see Step summaries):
  per tool, outcome and day for counts, failures, result sizes and sessions; the `file_path` field
  table for most edited files (only rows of the four edit tools are read); the word index for
  `command contains "git commit"` (only commands holding a word that ends in `git` and one that
  starts with `commit` are confirmed on their text, without decoding). Most edited files, the tightest, also
  picks its 10 rows on the counters before making them, finding the 10th-best count first.
- **Write a file, later Read the same file** (a compared field) visits for each path only the turns
  that hold it, with each session's turn tree and steps by shape code kept in the step cache, and
  values found by dictionary code.
- **Every query** costs less to start: one-thread queries run on the caller's thread instead of a
  scoped thread, and tests of `tool` against literals are worked out on the names without building
  a value per tool (an empty query, `where tool == "nothing" | count`, went from 42 to 19 µs on
  average).

What the summaries cost: 1.6 MB of this store (+1.2%), no measurable ingest cost, about 0.3 s more
to compact it (see Step summaries).

Earlier fixes (2026-11-21/22), from the first run of this comparison, which lost three queries:
long input values keep their place in the message so a test reads only those bytes; values up to
4 KB are dictionary-coded with a hash-keyed index; a test over one field is worked out once per
distinct value; `isnull`/`isnotnull` test presence only; `summarize` groups on dictionary codes;
`top` and `sort | take` select instead of sorting all; compared fields skip values an element never
has; reads cache frames per thread and fetch a request's opaque values with a few large reads.


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
scripts/planted-bugs.sh    # plant each query bug in a copy of the source; the checks must fail
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
| Crash test (2026-11-23) | 1,000 SIGKILLs during writes, flushes, appends to blob files, merges of segments and index runs, folds and rewrites of half-replaced segments: 26,170 acknowledged, 0 lost, 0 wrong bytes, steps all match, request count equal to the requests listed; at least 77 folds and 2 rewrites finished between kills; recovery removed 1,108 files left by interrupted flushes and merges. (The writer now uses 128 KB segments and compacts on 3% of commits, where it used 1 MB and 50%: with those, nearly every child was killed mid-compaction and no merge reached full size, so nothing folded.) Rerun with step summaries written at every flush and merge: 1,000 SIGKILLs, 21,827 acknowledged, 0 lost, 0 wrong bytes, steps all match, at least 48 folds and 1 rewrite. |
| Queries vs reference (2026-11-21) | 100 generated queries, 758 real sessions up to 120 steps: engine = brute force in every session (337,573 matches); 300 on synthetic: same |
| Queries vs reference (2026-11-23) | on the 200-file sample: 100 generated patterns and 100 on a compared field, 146 sessions up to 400 steps, 0 differ (112,035 matches); 300 and then 200 generated aggregations three ways (summaries, scan, brute force) on all 156 sessions, 0 differ (123 and 93 answered from summaries); on rich synthetic stores (split over segments, with steps not flushed, after `compact`, with a 1–2 MB cache): 0 differ in 400 compared-field patterns and 400 aggregations each |

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
tdb synth         --store DIR [--sessions 20 --per 40] [--rich x] [--no-flush x]
tdb query         --store DIR --q 'QUERY' [--limit 30] [--threads N] [--repeat N] [--summaries off]
tdb check-queries --store DIR [--n 300] [--eq 150] [--agg 300] [--max-steps 400] [--seed S]
tdb h2h           --store DIR [--pg 127.0.0.1:5455] [--no-load x] [--reps 5] [--reads 200]
```

## On disk

```
MANIFEST              live segments (with what folds replaced in each), index runs (in id order)
                      and blob files (with the length each is durable at), id counters,
                      checkpoint, first live log (crc32c)
wal-NNNNNN.log        [len][crc32c][record]...
seg-NNNNNN.tdb        frames per session part | blocks: groups of parts (messages + requests,
                      steps) | step summaries: per field values, rows, words, postings, then the
                      head (parts, part table, where each field's blocks are) | session directory
                      in 64 KB blocks, each entry saying where its part's blocks are | catalog
                      (points at the summary head; segments written before summaries existed still
                      open, but a store written now does not open in an older build) | footer
idx-NNNNNN.tdb        key pages (2048 sorted (kind, hash, value) keys + their Bloom filter, each
                      with a crc32c) | fence pages (first key of 1024 pages) | piece table and
                      opaque value table (1024 a block, by id) | catalog | footer
blob-NNNNNN.dat       pieces (zstd, one frame each) and opaque values (raw), appended per flush
LOCK                  held with flock while a process has the store open
```

## What's next

1. Derived columns (rules at write time, backfilled when a merge rewrites a session).
2. Summaries for `time` tests (the part table has each row's earliest and latest time, so a row
   wholly inside or outside a range could be counted without its steps) and for `bin(time, 1h)`;
   pattern queries that skip whole segments from their catalogs before reading any session.
3. The field tables keep short values a second time (the step parts keep each part's own
   dictionary): one dictionary per segment that the step parts point into would remove that copy.
4. Merges run inside the flush that triggers them; a background thread would keep writes going
   while a big merge runs (index merges are capped at 1 GB and segment merges at 256 MB).
5. A multi-terabyte run on an external disk (the index's filters are about 1.8 bytes per key and
   are read through the cache, so at that size ingest speed depends on how much of them the
   cache holds).
