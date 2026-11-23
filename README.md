# TranscriptDB

A storage and query engine for agent transcripts: Kusto for agent traces. This repository is
the **storage core**, written in Rust: each session is stored once as an ordered tree of
messages, any request can be rebuilt byte for byte except for what is never stored (thinking
and images, which come back as empty strings), and a crash loses nothing acknowledged.

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
   A request is then just its *tip* node: its messages are the path from the root. Hashes are
   not stored: a node's key is the hash of its message as stored, and a session that wakes up
   works out the keys of a node's children from their bytes the first time a walk passes it
   (messages that came with thinking or images are also found by the hash of the message as it
   came, for as long as the session stays in memory).
4. **Thinking and images are never stored; other opaque values are.** In new messages, thinking
   text, thinking signatures, redacted thinking and base64 images are replaced by empty strings
   (a cache marker after one moves back by what was removed), so a request that held them
   rebuilds with `""` in their place and its steps count result sizes as stored. Other long
   base64 strings under `signature` or `data` (documents, anything else) are cut out and kept as
   raw bytes (decoded when re-encoding gives the same text back, kept as text otherwise), unless
   `Options::keep` (`tdb --drop documents,other`) blanks them too.
5. **Shared pieces.** Skeletons, system blocks, tool lists and marker texts are interned by
   hash across all sessions. A new piece is compressed against whichever of the last 32 pieces
   of its kind (as a raw zstd dictionary) makes it smallest, when that at least halves it: tool
   lists and system prompts change a little at a time, so most pieces are stored as a few KB of
   difference. Chains are at most 32 pieces long, and a piece read once is cached.
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
   A merge whose output reaches 256 MB is the last time that data moves, so it, and
   `compact`, write at zstd level 19 with a dictionary stored once in each output segment:
   up to 1 MB trained on the merge's input (frames sampled evenly, about 100 times the
   dictionary's size), followed by up to 8 MB of the input's own messages (whole frames
   sampled evenly, about 1/12 of the input). Frames of many sessions are compressed at once on
   every core, sharing the prepared dictionary's tables; the session parts' step blocks use the
   dictionary too. Smaller merges, and merges of under 1.6 MB of messages, write at level 3
   like flushes. A frame compressed with a dictionary is decompressed into a buffer that holds
   the dictionary's bytes right before it, so zstd copies matches into the dictionary as
   matches into earlier output (about 25 against 41 µs per 64 KB frame through a dictionary
   kept apart). In those merges a *cold* session (its last request more than 7 days older
   than the newest request in the store, `--cold-days`) has its messages written in runs of up
   to 4 MB (`--cold-mb`; most sessions are one run) instead of 64 KB frames: about 17% fewer
   frame bytes, while reading one message of it decodes its whole run (up to about 1.5 ms).
   Recent sessions keep 64 KB frames, and steps keep their blocks either way.
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
  directory blocks, index blocks, pieces and decoded step-summary blocks (1/12 of the budget),
  plus the merged segments' dictionaries (32 MB, shared) and, per thread reading, decode
  buffers for the last segments with a dictionary it read (the dictionary plus 256 KB, or plus
  4 MB once it reads a cold run; up to 16 MB in all) and decompressed frames (8 MB).

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
- columns per step: tool, outcome, result size (as stored: kept opaque values at their text
  size, thinking and images as the empty strings that replace them), the time of the request
  that carried the result, call and result positions, and the call's top-level input fields.
  Values up to 4 KB are dictionary-coded; longer ones are marked `LONG` (open the message);
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
loads the same data into a throwaway PostgreSQL 18 cluster, runs the same 16 queries and 4 read
workloads on both, and compares answers. `src/h2h.rs` holds the queries; `src/pg.rs` is a small
client (wire protocol, simple queries, `COPY`).

PostgreSQL gets the usual tricks: every distinct message stored once by hash, requests as arrays of
message ids, a step table with indexes and the two input fields the queries use pulled out as
columns, and for patterns a materialized table of root-to-leaf turn paths, so window-function SQL
gives exact answers on forked sessions. It also holds what the store keeps to rebuild every request
byte for byte, deduplicated: skeletons (the body around messages, system prompt and tools, with its
hole positions), system prompts, tool lists and cache-marker texts once each, with tool lists and
system arrays split into elements stored once each (tool lists change one tool at a time); per
request its time (`timestamptz`), its metadata (query source and client version, normalized), its
cache-marker positions and, when they are not bare commas, its message separators. The fourth read
rebuilds whole requests from PostgreSQL alone (one query, then the same assembly as the store's)
and compares them with the store's rebuild. Pattern queries are compared on the set of sessions
with a match (what SQL answers naturally; it equals "the engine reports at least one match"),
aggregations on their rows, reads on the bytes returned. Settings: shared_buffers 256 MB, work_mem
32 MB, up to 4 parallel workers, JIT off, C collation. PostgreSQL is reached over loopback TCP, so
every query pays a client round trip (about 0.1 ms) that the in-process engine does not; that
matters for the sub-millisecond rows below.

Measured 2026-11-23 with thinking and images never stored (PostgreSQL is loaded from the store,
so it holds the same blanked messages) and cold sessions in runs, on 200 log files (seed 7, from
the 1,730 files that existed before 14:43 that day: `BORN_BEFORE=1795425000`): 156 sessions, 20,567
requests, 26,565 steps, 95.9 MB of distinct message text; 139 of the 156 sessions are cold (last
request more than 7 days before the newest). Every answer matched (16 of 16 queries, 1,056 of 1,056
reads, including 300 whole requests rebuilt from each side). Each query best of 7, at load 3–4.

| | TranscriptDB | PostgreSQL |
|---|---|---|
| Disk (tables, indexes, TOAST) | 18.0 MB | 130.7 MB (7.24x) |

| Query | TDB, 1 thread | TDB, 8 threads | PostgreSQL | PG / TDB (1 thread) |
|---|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 0.85 ms | 0.30 ms | 398 ms | 466x |
| Bash fails, only test files edited, Bash passes | 0.81 ms | 0.30 ms | 717 ms | 888x |
| 3 Read/Grep turns in a row, then Edit | 0.27 ms | 0.16 ms | 25.5 ms | 93x |
| Bash fails, then passes the next turn | 0.49 ms | 0.23 ms | 22.6 ms | 47x |
| Edit, then Bash within 3 turns | 0.34 ms | 0.18 ms | 17.3 ms | 51x |
| Write a file, later Read the same file | 1.47 ms | 1.27 ms | 3.15 ms | 2.1x |
| 3 failing turns in a row | 0.42 ms | 0.19 ms | 22.0 ms | 52x |
| WebSearch, then WebFetch the next turn | 0.10 ms | 0.10 ms | 21.4 ms | 218x |
| Steps per tool, top 10 | 0.32 ms | 0.36 ms | 0.89 ms | 2.8x |
| Failure rate per tool | 0.36 ms | 0.42 ms | 2.27 ms | 6.2x |
| Bash steps and failures per week | 0.40 ms | 0.44 ms | 1.98 ms | 5.0x |
| Sessions with the most failed steps | 0.10 ms | 0.15 ms | 1.15 ms | 11x |
| Most edited files | 0.51 ms | 0.48 ms | 0.63 ms | 1.2x |
| Average output size per tool | 0.35 ms | 0.40 ms | 2.42 ms | 6.9x |
| Sessions using browser tools, per day | 0.15 ms | 0.19 ms | 3.59 ms | 24x |
| Commands running `git commit` | 29.5 ms | 11.3 ms | 24.3 ms | **0.8x** |

| Read (median / p95 per item) | TranscriptDB | PostgreSQL |
|---|---|---|
| A request's messages in order | 0.43 / 1.6 ms | 1.03 / 2.8 ms |
| 20 steps with their call and result messages | 0.18 / 0.79 ms | 0.41 / 1.7 ms |
| A whole session's messages | 0.21 / 0.82 ms | 0.79 / 2.6 ms |
| A whole request rebuilt | 0.53 / 1.6 ms | 1.81 / 4.0 ms |

One query is now slower than PostgreSQL on one thread: `git commit` (29.5 against 24.3 ms; on 8
threads 11.3 ms). It tests `command`, and the 393 commands over 4 KB are read from the call's
message, which in a cold session means decoding a whole run (with 64 KB frames the query took
16.7 ms). Copying input values over 4 KB of cold sessions into their step parts would fix it for
0.49 MB (`command` only) or 1.42 MB (every field) on this sample (`examples/longs_exp.rs`); not
done. With every session in 64 KB frames (`--cold-mb 0`) the store is 20.1 MB and every query is
faster on one thread (closest: most edited files 1.3x, `git commit` 1.5x). Before PostgreSQL held
what rebuilding needs, it was 127.7 MB on the same data (126.9 MB on the earlier run): skeletons,
pieces and request metadata cost it 3.0 MB. Earlier runs, when the store kept every value:
2026-11-22 on another 200-file sample, 67.5 against 168.4 MB (2.49x), all answers equal and every
query faster; on 2026-11-23 before this work, 106.6 against 250.5 MB (2.35x). The first run of the
comparison (before the fixes below) lost three: `command contains ...` over long commands (278 ms
against 17), the same-file pattern (7.3 ms against 1.7) and most edited files (0.72 against 0.46).
What changed:


### Size against PostgreSQL, and how small it can get

The store never keeps thinking or images (decided 2026-11-23), so this is the one mode measured
here. Sample: the head-to-head's 200 files, pinned to the files that existed before 14:43 that
day (`BORN_BEFORE=1795425000`: the 200 are drawn by shuffling every log file, so one new file
draws a different sample). Sizes do not depend on machine load; times give it.

**Where the bytes are** on that sample (about 20,500 requests; 410f084 is the store before this
work, with `--drop thinking,images`; PostgreSQL then held what the queries need, now also what
rebuilding every request needs):

| | 410f084, `--drop thinking,images` | Now |
|---|---|---|
| **PostgreSQL** | **126.9 MB** | **130.7 MB** |
| messages: heap + TOAST + index | 33.7 + 33.1 + 1.1 | 34.2 + 33.2 + 1.1 |
| requests (arrays of message ids; now also skeleton, system, tools, time, metadata, cache markers) | 36.9 | 38.8 |
| steps, path_steps, nodes, sessions | 13.6, 4.8, 3.6, 0.1 | 13.5, 4.8, 3.6, 0.1 |
| tool and system elements, pieces, metadata values | | 1.0, 0.1, 0.1 |
| **TranscriptDB** | **25.9 MB (4.90x; 5.05x against the new PostgreSQL)** | **18.0 MB (7.24x)** |
| message frames | 18.9 | 12.9 (14.9 with every session in 64 KB frames) |
| zstd dictionary (in the segment's catalog bytes) | 0.4 | 1.8 |
| message/request blocks | 2.2 | 0.7 |
| step blocks | 2.4 | 1.8 |
| shared pieces | 0.5 | 0.2 |
| documents (kept opaque values) | 0.1 | 0.1 |
| index runs | 1.6 | 0.6 |

**The best possible size.** `tdb ceiling --out DIR` writes the store's distinct data as plain
streams: every distinct message once across all sessions (stored form, opaque values cut out),
opaque values as raw bytes by kind, the trees, the cut positions, request columns and the shared
pieces. Each stream is compressed with zstd -19 --long=31, zstd -22 --long=31 and xz -9e and the
smallest kept. Nothing in this is randomly accessible, and it holds no steps and no index
(PostgreSQL's step and path tables are 18.4 MB of the 126.9).

| Stream (with thinking and images blanked) | Raw | Best |
|---|---|---|
| messages | 94.2 MB | 11.5 MB (xz, 8.2x) |
| trees, cut positions, request ids, times and the rest | 3.1 | 0.6 |
| shared pieces | 12.9 | 0.1 |
| documents | 0.1 | 0.1 |
| **all** | | **12.3 MB** |

**Ceiling = PostgreSQL / best possible = 10.2x** against the PostgreSQL that held what the queries
need, 10.6x against the one that can also rebuild every request. 12x is out of reach on this data
even without random access. (When the store kept every value, the ceiling was 2.87x: signatures are random
and images already compressed, so they were 85% of the best possible size, while PostgreSQL held
them as base64, only about 1.4x bigger.)

**What changed**, each measured on its own (sample sizes from runs on the same files a few
requests apart; full logs: every local log, 1,730 files, about 128,400 requests; write path =
ingest throughput of the full logs, load given):

| Change | Size | Reads | Writes |
|---|---|---|---|
| Thinking and images blanked in the message at ingest: no opaque entry, index key or hole kept for them | sample 25.7 → 24.8 MB (index 1.5 → 0.6 MB); full logs 157.1 → 152.4 MB | not measured apart (the next row's back-to-back run compares against this) | 735 MB/s at load 6–8 with builds running alongside |
| Node hashes not stored: keys worked out from stored bytes when a woken session's walk needs them | sample 24.8 → 24.0 MB (message/request blocks 2.1 → 1.4 MB); full logs 152.4 → 147.5 MB | back to back at load 12: one request 0.28 against 0.29–0.30 ms, 40 messages 0.076 / 0.075 ms, a session 0.20 / 0.19–0.20 ms | 992 MB/s at load 5–6 |
| Request digests not stored (`tdb check-claude` checks against Claude Code's `requestDigest`) | sample 24.0 → 23.3 MB (blocks 1.4 → 0.7 MB); full logs 4.1 MB less (147.5 → 142.6 MB with the next row) | none | none (the digest came from the log) |
| Pieces against the best of the last 32 of their kind, chains under 32 (was 4 and 8) | 0.44 → 0.16 MB (`examples/pieces_exp.rs`); full logs 1.4 → 0.5 MB | a piece's first read decodes its chain (longest seen 22 on the sample), then it is cached | 1,001 MB/s at load 3–5 |
| Step blocks compressed with the segment's dictionary | 2.35 → 2.07 MB (`examples/steps_exp.rs`) | on block cache misses only; blocks over 4 frames decode without the per-thread buffer | none |
| Cold sessions (last request more than 7 days before the newest) in runs of up to 4 MB instead of 64 KB frames, in full-size merges and `compact`; step blocks unchanged | one ingest compacted both ways: sample 20.0 → 18.0 MB (frames 14.9 → 12.9; 139 of 156 sessions cold); full logs 125.7 → 112.9 MB (frames 103.9 → 91.1; 952 of 1,068 sessions) | back to back, sample at load 4 (full logs at load 7–8): one request 0.20 → 0.37 ms (0.26 → 0.39), 40 messages 0.06 → 0.26 ms (0.07–0.08 → 0.33), a whole session 0.14–0.18 → 0.13 ms (0.17 both). Queries testing input values over 4 KB, read from the call's message, decode whole runs: `git commit` 16.7 → 29.3 ms, Write `content contains` 16.6 → 27.0 ms (full logs 222 → 307, 194 → 271 ms); queries that read no values unchanged | `compact` of the sample 13.8 → 17.4 s, 518 → 911 MB resident; of the full logs 108 → 109 s at load 7, 540 MB → 1.24 GB resident (level 19 tables for 4 MB inputs on every thread) |
| The dictionary followed by 8 MB of the merge's own messages, sampled evenly | sample 22.7 → 19.9 MB (frames 18.8 → 14.9, dictionary 0.4 → 1.8, step blocks 2.1 → 1.8); full logs, with the row above, 142.6 → 125.4 MB (frames 119.5 → 103.7; 123.6 MB when each compressing thread copied the dictionary's tables instead of sharing them) | back to back on the full logs at load 3–4: one request 0.27 ms against 0.32 before this change (and 0.29–0.31 for 410f084), 40 messages 0.073 against 0.074 (0.078–0.085), a session 0.18–0.19 against 0.20–0.28 (0.20–0.21) | `compact` of the sample 22 → 55 s of CPU (14 s wall); of the full logs 68 s wall, 357 s of CPU, 643 MB resident (410f084: 42 s, 230 s, 1.18 GB); each thread reading a full-size segment holds a 9 MB decode buffer |

Correctness on the way: `tdb check-steps` passes on these stores (it failed on 852 steps in 11
sessions of a `--drop thinking,images` store, because steps counted result sizes with the
dropped values while the reference read blanked bodies); `tdb check-claude` now says how many
requests no longer equal `requestDigest` because something was blanked (full logs: 2,262 byte
for byte, 126,227 equal to the log's body with thinking and images blanked, none different).

How the frame codec was chosen (`examples/frames_exp.rs` on the sample's 101 MB of stored
messages, before the dictionary carried messages; sizes include the dictionary):

| 64 KB frames | Frames | Decode, per frame |
|---|---|---|
| level 3 (before) | 30.2 MB | 33 µs |
| level 19 | 27.2 MB | 36 µs |
| level 9 / 12 / 15 + 1 MB dictionary | 21.3 / 21.1 / 20.7 MB | 25 µs |
| level 19 + 1 MB dictionary | 19.6 MB | 26 µs |
| level 19 + 256 KB dictionary | 20.9 MB | 28 µs |
| whole sessions, level 19 + dictionary (no random access) | 15.7 MB | |

Adding messages to the dictionary (`examples/xsession_exp.rs`, 64 KB frames at level 19, frames +
dictionary): on the sample, 1 MB trained alone 19.14 MB; plus 1, 2, 4, 7.6 MB of evenly sampled
messages 18.82, 18.84, 18.22, 17.58 MB. On the full logs (645 MB of messages): 120.23 MB; plus 4,
8, 16 MB: 116.06, 112.58, 110.24 MB, decode 46, 43, 41, 49 µs per frame through a prepared
dictionary. zstd's trainer returns worse dictionaries when asked for more than 1 MB on this much
data; whole messages give it the long matches it cannot keep. 8 MB is where the gain per byte of
memory falls off. In the store a frame compressed with a dictionary is decompressed into a
buffer that holds the dictionary's bytes right before it: a dictionary kept apart made reads
17–20% slower (41 µs per frame: zstd copies each match into it with a `memmove` call). Level 19
costs 5 times the CPU of level 15 for a store 8% smaller (`compact` of the sample with the 9 MB
dictionary: 55 against 11 s of CPU, 20.0 against 21.8 MB; level 12: 22.2 MB, 7 s), so it is spent
once per byte, in merges into full-size segments and `compact` (`--merge-level 15` is the
cheaper choice). Compressing threads share the prepared dictionary's tables instead of copying
them (`compact` of the sample: 517 MB resident, 1.38 GB when copied).

Tried and not kept:

- Messages shared across sessions as references to the earlier copy: 3.1% of stored message
  bytes on the sample are in a session that came earlier (3.6% on the full logs, not the 7% first
  estimated), and leaving them out of the frames saves 0.24 of 18.8 MB (1.4 of 119.9 MB on the
  full logs), and 0.15 of 12.26 MB with whole-session runs (`examples/cold_exp.rs`): not worth
  references between parts that are self-contained by design. Putting them in the dictionary
  instead saves 0.08 MB; evenly sampled messages do far better (above).
- Whole sessions as one run, or runs of 1 MB: `examples/cold_exp.rs` on the sample, frames +
  dictionary 16.69 MB in 64 KB frames, 15.87 in 256 KB, 15.13 in 1 MB, 14.15 in 4 MB, 14.04 as
  whole sessions; reading one message decodes up to 0.1, 0.3, 0.6, 1.5 and 2.8 ms (the largest
  session is 10 MB; median 0.36 MB, p99 5.3 MB). 4 MB keeps nearly all of the gain with a bounded
  read, and 1 MB runs did not make the long-value queries faster (`git commit` 31 ms).
- Long Bash commands as positions in the call's message, like values over 4 KB already are:
  commands are 7.5 of the 10.7 MB of step input values, and values over 256 bytes kept as
  positions would take step blocks from 2.35 to 0.70 MB, but `command contains ...` would then
  decompress message frames, and `git commit` is already the closest query to PostgreSQL.
- Step blocks of 128–512 KB (2.31–2.24 MB against 2.35), and a dictionary trained on step parts
  (256 KB: 2.06 MB plus 0.10 MB for the dictionary) instead of the frames' one.
- 64-bit index keys (about a third of the 0.6 MB): every namespace (request ids, session keys,
  piece hashes) would need collision checks against stored content and duplicate keys.
- 256 KB frames: 6% smaller, 3 times the decode per random read (80 against 26 µs).
- zstd -19 on each image, lossless PNG or JPEG recompression: moot now that images are not
  stored (PNGs did not re-deflate byte for byte with zlib, so they would have needed a new crate).

**What is left** between the store and the best possible size: frames 12.9 against 11.5 MB (runs
per cold session and 64 KB frames for recent ones, with a 9 MB dictionary, against one archive
matching across everything), the dictionary 1.8 MB, step blocks 1.8 MB and the index 0.6 MB that
the best possible does not hold, message and request blocks 0.7 against 0.6 MB, pieces 0.2 against
0.1 MB. Step blocks, pieces and the index are at their floor for this codec: sorting each part's
step values by field and value saves 0.01 MB, 512 KB step blocks 0.03 MB, and the index is 27 bytes
per request id for random access by id.

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
scripts/h2h.sh             # head-to-head against PostgreSQL (needs postgresql@18); BORN_BEFORE=SECS pins
                           # the sample's files, H2H_ARGS="--split x" adds where the bytes are
scripts/planted-bugs.sh    # plant each query bug in a copy of the source; the checks must fail
tdb growth --store DIR     # grow a synthetic store 1x..16x; open time, memory, reads, rewrites per round
```

`check-rebuild.sh` writes its store outside the repo (`$TMPDIR/transcriptdb-check`) and deletes it
at the end unless `KEEP=1`. Both it and the crash test exit non-zero on any failure.

## Measured (2026-11-23, thinking and images never stored, every Claude Code log on one Mac)

Apple Silicon, 10 cores. Sizes compare stores built from the 1,730 log files that existed before
14:43 that day; the rebuild check ran on every log present when it ran.

| | Result |
|---|---|
| Request lines in the logs | 158,876 read (128,518 distinct ids; resumed sessions repeat requests); all rebuilt from the logs equal to `requestDigest` but 20 (a message is missing from the log) |
| Ingest | 128,489 requests, 158.8 GB of bodies in 235 s; write path 1,009 MB/s at load 3, 977 MB/s on a second run at load 7; 410f084 (`--drop thinking,images`) on the same files right between them: 1,030 MB/s |
| Store size | 112.9 MB after `compact` with cold sessions in runs (952 of 1,068 sessions, 563 of 646 MB of messages): frames 91.1 MB (7.09x), step blocks 11.6 MB, message/request blocks 4.3 MB, dictionary and catalogs 1.7 MB, index runs 3.5 MB, blob files 0.6 MB. The same ingest compacted with every session in 64 KB frames: 125.7 MB (frames 103.9). 410f084 with `--drop thinking,images` on the same files: 158.2 MB (frames 120.0, step blocks 14.0, message/request blocks 13.5, dictionary and catalogs 0.4, index runs 8.9, blob files 1.4). As ingested, before `compact`: 219 MB (no merge reached full size) |
| Rebuild check (`scripts/check-rebuild.sh`, every log, cold sessions in runs) | 128,674 requests in 1,070 sessions (a few log files newer than the 1,730): 2,264 byte for byte; 126,410 no longer equal to `requestDigest` because thinking or images were blanked, and equal to the log's body with them blanked; none different, no read errors; 96.8 GB rebuilt on 10 threads at 13.5 GB/s (17,880 requests/s). Steps: 148,984, engine and reference agree on every distinct step, 0 wrong `prev` or shape codes. Store 113.1 MB, 953 sessions cold; ingest 984 MB/s through the write path |
| Compact | the 645 MB of messages in 109 s, 470 s of CPU, 1.24 GB resident at load 7 (64 KB frames, same run: 108 s, 460 s, 540 MB; at load 3: 68 s, 357 s, 643 MB); 410f084: 42 s, 230 s, 1.18 GB |
| Reads (one store, back to back) | cold sessions in runs, load 7–8: one request median 0.39 ms (p99 1.8), 40 messages 0.33 ms (p99 1.7), a whole session 0.17 ms; the same data in 64 KB frames: 0.26 ms (p99 1.1), 0.07–0.08 ms, 0.18 ms; 410f084 at load 3–4: 0.29–0.31 ms (p99 1.2), 0.078–0.085 ms, 0.20–0.21 ms (p99 4.5–5.2) |
| Queries over input values over 4 KB (one thread, best of 7, load 7) | `git commit` 307 ms in cold runs against 222 ms in 64 KB frames; Write `content contains` 271 against 194 ms; a query that reads no values (Bash failures) 159 against 163 ms |
| Open | 8–10 ms |
| Crash test | 1,000 SIGKILLs: 19,854 acknowledged, 0 lost, 0 wrong bytes, steps all match; at least 153 folds and 3 rewrites of half-replaced segments finished between kills; 2,914 of 2,921 sessions in cold runs at the end; recovery removed 703 files left by interrupted flushes and merges. The writer flushes at 128 KB into 32 KB segments (was 384 and 128 KB; blanked synthetic requests store much less, and with the old sizes no merge reached full size, so nothing folded) and makes every session older than the newest request cold, in runs of 256 KB |

## Measured (2026-11-22, when every value was kept)

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
| Size work (2026-11-23) | 127,912 requests: store 482.9 MB after `compact` (frames 119.4 MB for 644.7 MB of messages, 5.40x; blob files 327.0 MB: opaque values 325.5 MB, pieces 1.5 MB; index runs 8.6 MB; other segment bytes 27.8 MB), was 575.4 MB; 559.6 MB as ingested (merges below full size write at level 3). All byte for byte; steps match the reference in all 1,068 sessions. Ingest 813 MB/s through the write path and `compact` 41.6 s, rebuild 8.0 GB/s on 10 threads, one request median 0.45 ms, all at load 10–26 from other work; back to back at load 1.7 on one store compacted both ways, one request 0.44 → 0.37 ms |
| Size work, `--drop thinking,images` | 157.1 MB (frames 118.9 MB, index runs 8.9 MB, blob files 1.5 MB), was 249.8 MB; 125,678 requests equal the log's body with those values blanked, the other 2,261 byte for byte, none different |
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

Not measured yet: cold-disk reads, size against a zstd `--long` archive of the raw request bodies
(needs ~150 GB of scratch disk; against an archive of the distinct data, see "Size against
PostgreSQL, and how small it can get"), torn log records from real power loss (SIGKILL cannot tear
a `write`; a unit test covers the path).

## Command line

```
tdb ingest-claude --store DIR [--logs DIR] [--max-files N --seed S] [--born-before SECS] [--sync full|fsync|os] [--drop documents,other]
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
tdb h2h           --store DIR [--pg 127.0.0.1:5455] [--no-load x] [--reps 5] [--reads 200] [--split x] [--sizes-only x]
tdb ceiling       --store DIR [--out DIR]   # where the store's bytes are; --out writes its distinct data as streams
                                            # ingest and compact take --merge-level 19 --dict-kb 1024
                                            # --dict-content-kb 8192 --zstd-threads N --cold-days 7 --cold-mb 4
```

## On disk

```
MANIFEST              live segments (with what folds replaced in each), index runs (in id order)
                      and blob files (with the length each is durable at), id counters,
                      checkpoint, first live log (crc32c)
wal-NNNNNN.log        [len][crc32c][record]...
seg-NNNNNN.tdb        frames per session part (64 KB, or runs of up to 4 MB for a cold session;
                      each part says which) | blocks: groups of parts (messages + requests,
                      steps; each part column by column, no hashes or digests) | step summaries:
                      per field values, rows, words, postings, then the head (parts, part table,
                      where each field's blocks are) | session directory in 64 KB blocks, each
                      entry saying where its part's blocks are | zstd dictionary of the frames
                      and step blocks (segments from full-size merges and compact: trained, then
                      the merge's own messages) | catalog (points at the dictionary and the
                      summary head) | footer
idx-NNNNNN.tdb        key pages (2048 sorted (kind, hash, value) keys + their Bloom filter, each
                      with a crc32c) | fence pages (first key of 1024 pages) | piece table and
                      opaque value table (1024 a block, by id) | catalog | footer
blob-NNNNNN.dat       pieces (zstd, one frame each, most against an earlier piece of their kind)
                      and opaque values other than thinking and images (raw), appended per flush
LOCK                  held with flock while a process has the store open
```

## What's next

1. Derived columns (rules at write time, backfilled when a merge rewrites a session), then text
   search over message text.
2. Summaries for `time` tests (the part table has each row's earliest and latest time, so a row
   wholly inside or outside a range could be counted without its steps) and for `bin(time, 1h)`;
   pattern queries that skip whole segments from their catalogs before reading any session.
3. The field tables keep short values a second time (the step parts keep each part's own
   dictionary): one dictionary per segment that the step parts point into would remove that copy.
4. Merges run inside the flush that triggers them; a background thread would keep writes going
   while a big merge runs (index merges are capped at 1 GB and segment merges at 256 MB). This
   matters more now that a merge into a full-size segment spends about 2.5 times the CPU it did
   (the 9 MB dictionary at level 19).
5. A bigger head-to-head on the full logs, and a multi-terabyte run on an external disk (the
   index's filters are about 1.8 bytes per key and are read through the cache, so at that size
   ingest speed depends on how much of them the cache holds).
