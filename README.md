# TranscriptDB

A storage and query engine for agent transcripts, written in Rust.

## What it is

- **Storage.** Each session is stored once, as an ordered tree of messages. Any request can be
  rebuilt byte for byte, except thinking and images, which are never stored and come back as
  empty strings. A crash loses nothing that was acknowledged.
- **Steps.** Each tool call is paired with its result and kept in columns.
- **Queries.** A Kusto-style pipe language with a `match` stage for sequences of steps.

## What it's for

Asking what agents did, and in what order, across many sessions. For example: a command failed,
only test files were edited, and then the command passed.

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

In SQL this takes window functions over a table of every path through every session. Here a
pattern is matched in one pass over each session's steps.

## Benchmarks against PostgreSQL

Both sides hold the same data: 1,730 Claude Code log files from one Mac, which is 1,068
sessions, 129,056 requests, 149,424 steps and 612.8 MB of distinct message text. Every answer
and every read was identical on both sides.

- **PostgreSQL 18** has a tuned schema: each distinct message stored once, a step table with
  indexes, a materialized table of turn paths for the sequence queries, and everything needed to
  rebuild whole requests. It may use up to 4 parallel workers.
- **TranscriptDB** runs on one thread.
- Times are the best of 7 with warm caches, measured on 2026-11-23 on an Apple M4 with 10 cores
  and 16 GB.

### Size on disk

| Data | TranscriptDB | PostgreSQL | Smaller by |
|---|---|---|---|
| Every log file | 121.6 MB | 771.0 MB | 6.34x |
| 200-file sample | 19.6 MB | 132.3 MB | 6.74x |

### Sequence queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 3.92 ms | 1,830 ms | 466x |
| Bash fails, only test files edited, Bash passes | 4.53 ms | 3,355 ms | 740x |
| 3 Read/Grep turns in a row, then Edit next | 1.12 ms | 139 ms | 125x |
| Bash fails, then Bash passes the next turn | 2.32 ms | 119 ms | 51x |
| Edit, then Bash within 3 turns | 1.42 ms | 82.4 ms | 58x |
| Write a file, later Read the same file | 0.281 ms | 14.6 ms | 52x |
| 3 failing turns in a row | 2.23 ms | 118 ms | 53x |
| WebSearch, then WebFetch the next turn | 0.312 ms | 115 ms | 368x |

### Other queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Steps per tool, top 10 | 0.036 ms | 4.79 ms | 132x |
| Failure rate per tool (100+ steps) | 0.073 ms | 9.08 ms | 124x |
| Bash steps and failures per week | 0.053 ms | 15.3 ms | 286x |
| Sessions with the most failed steps | 0.049 ms | 11.1 ms | 226x |
| Most edited files | 0.106 ms | 4.42 ms | 42x |
| Average output size per tool | 0.056 ms | 9.06 ms | 162x |
| Sessions using browser tools, per day | 0.056 ms | 23.4 ms | 415x |
| Commands running `git commit` | 1.46 ms | 135 ms | 92x |

On the 200-file sample (156 sessions, 26,818 steps) the sequence queries are 58x to 961x
faster and the other queries 26x to 178x.

### Reads

| Read (median / 95th percentile) | TranscriptDB | PostgreSQL | Faster at the median |
|---|---|---|---|
| A request's messages in order | 0.559 / 2.207 ms | 1.665 / 4.365 ms | 3.0x |
| 20 steps with their messages | 0.295 / 0.799 ms | 0.641 / 2.126 ms | 2.2x |
| A whole session's messages | 0.302 / 1.145 ms | 1.081 / 6.768 ms | 3.6x |
| A whole request rebuilt | 0.589 / 2.297 ms | 1.771 / 5.184 ms | 3.0x |

### Reading these numbers

- PostgreSQL is reached over loopback TCP, which adds about 0.1 ms to each query. That matters
  for the rows under a millisecond.
- The first run after opening the store is slower. On every log file, `git commit` takes 137 ms
  and the test-files sequence 188 ms. PostgreSQL's first runs were not timed.
- The data is one person's logs.

To run it yourself: `scripts/h2h.sh` (needs Homebrew `postgresql@18`).
