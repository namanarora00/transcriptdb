# TranscriptDB

A storage and query engine for agent transcripts, written in Rust.

## What it is

- **Storage.** Each session is stored once, as an ordered tree of messages. Any request can be
  rebuilt byte for byte, except thinking and images, which are never stored and come back as
  empty strings. A crash loses nothing that was acknowledged. Flushes and merges run on
  threads of their own, so a write never waits for one.
- **Steps.** Each tool call is paired with its result and kept in columns.
- **Text.** A word index over what the user wrote and what the assistant said, searched by
  word, phrase or regular expression. Tool outputs are stored but not searched.
- **Rule columns.** A rule names a fact about a step, such as the program a shell command
  runs or how many tests a run says failed. Its values are stored with the step and queried
  like any other column. A rule added later covers old steps too.
- **Queries.** A Kusto-style pipe language with a `match` stage for sequences of steps and a
  `messages` source for text.

## What it's for

Asking what agents did and said, and in what order, across many sessions. It is meant as a
cheap first pass over every session, so that a slower review only has to look at the sessions
a filter passes on.

A sequence of steps: a command failed, only test files were edited, and then the command
passed.

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

Text: sessions where the user said it still fails or asked to revert, and sessions where
the assistant reported failing tests.

```
messages
| where role == "user" and (text contains "still failing" or text has "revert")
| summarize n = count() by session

messages
| where role == "assistant" and text matches regex @"[0-9]+ tests? failed"
| summarize n = count() by session
```

Rule columns: two rules, and test runs that reported failures, by session.

```
.create column program      on tool == "Bash" = extract(command, @"^\s*(\S+)")
.create column tests_failed on program in ("pytest", "cargo") = toint(extract(output, @"(\d+) failed"))

where tests_failed > 0
| summarize runs = count() by session
```

## Benchmarks against PostgreSQL

Both sides hold the same data: 1,841 Claude Code log files from one Mac, which is 1,059
sessions, 129,730 requests, 149,226 steps, 76,809 text blocks and 617.1 MB of distinct
message text. Every answer and every read was identical on both sides.

- **PostgreSQL 18** has a tuned schema: each distinct message stored once, a step table with
  indexes, a materialized table of turn paths for the sequence queries, and everything needed to
  rebuild whole requests. For search it has a table of the same text with a word index and a
  trigram index, and a trigram index on commands. For the rule columns its step tables
  hold the same two columns, each with an index. It may use up to 4 parallel workers.
- **TranscriptDB** runs on one thread.
- Times are the best of 7 with warm caches, measured on 2026-11-25
  on an Apple M4 with 10 cores and 16 GB while a virtual machine was using several of the
  cores, so both sides are slower here than on an idle machine.

### Size on disk, with search on both sides

| Data | TranscriptDB | PostgreSQL | Smaller by |
|---|---|---|---|
| Every log file | 128.0 MB | 1,051.2 MB | 8.22x |
| 200-file sample | 26.0 MB | 235.4 MB | 9.05x |

### Sequence queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 4.09 ms | 1,838 ms | 450x |
| Bash fails, only test files edited, Bash passes | 4.78 ms | 3,389 ms | 709x |
| 3 Read/Grep turns in a row, then Edit next | 1.18 ms | 135 ms | 114x |
| Bash fails, then Bash passes the next turn | 2.39 ms | 122 ms | 51x |
| Edit, then Bash within 3 turns | 1.47 ms | 82.3 ms | 56x |
| Write a file, later Read the same file | 0.265 ms | 14.6 ms | 55x |
| 3 failing turns in a row | 2.29 ms | 118 ms | 52x |
| WebSearch, then WebFetch the next turn | 0.305 ms | 114 ms | 375x |

### Other queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Steps per tool, top 10 | 0.040 ms | 4.81 ms | 120x |
| Failure rate per tool (100+ steps) | 0.076 ms | 9.47 ms | 125x |
| Bash steps and failures per week | 0.055 ms | 14.2 ms | 261x |
| Sessions with the most failed steps | 0.049 ms | 8.51 ms | 173x |
| Most edited files | 0.112 ms | 4.43 ms | 40x |
| Average output size per tool | 0.058 ms | 9.59 ms | 166x |
| Sessions using browser tools, per day | 0.051 ms | 21.1 ms | 409x |

### Text search

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| User text with the word error, top sessions | 0.047 ms | 0.640 ms | 14x |
| Assistant text containing root cause | 0.108 ms | 82.3 ms | 764x |
| Text saying still failing or try again | 0.443 ms | 262 ms | 592x |
| Text matching root cause ... fix (regex) | 1.10 ms | 131 ms | 119x |
| Commands matching git push or pull ... origin (regex) | 1.88 ms | 11.7 ms | 6.2x |
| Commands running `git commit` | 1.12 ms | 13.0 ms | 12x |

### Rule columns

Two rules are defined on both sides: the program a shell command runs, and the number of
failed tests a test run reports.

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Shell steps per program, top 10 | 0.107 ms | 3.91 ms | 36x |
| Failure rate per program (50+ steps) | 0.284 ms | 9.08 ms | 32x |
| Test runs with failed tests, per week | 0.013 ms | 0.201 ms | 16x |
| Tests fail, then an edit, then a test run | 3.12 ms | 1,467 ms | 470x |

On the 200-file sample (151 sessions, 31,498 steps), times faster: sequence queries 72x to 1,107x, other queries 26x to 240x, text search 7.3x to 442x, rule columns 15x to 733x.

### Reads

Random items, each compared byte for byte.

| Read | TranscriptDB median / p99 | PostgreSQL median / p99 | Faster at the median / p99 |
|---|---|---|---|
| A request's messages in order | 0.664 / 3.300 ms | 1.319 / 4.878 ms | 2.0x / 1.5x |
| 20 steps with their messages | 0.236 / 1.261 ms | 0.608 / 2.152 ms | 2.6x / 1.7x |
| A whole session's messages | 0.238 / 1.739 ms | 0.836 / 7.707 ms | 3.5x / 4.4x |
| A whole request rebuilt | 0.608 / 3.047 ms | 1.894 / 5.427 ms | 3.1x / 1.8x |

Cold: each side's own cache emptied before every read (the store's caches, and
PostgreSQL's shared buffers with `pg_buffercache_evict_all()`). The operating system's file
cache stays warm for both.

| Read | TranscriptDB median / p99 | PostgreSQL median / p99 | Faster at the median / p99 |
|---|---|---|---|
| A request's messages in order | 0.973 / 5.935 ms | 1.160 / 4.599 ms | 1.2x / 0.8x |
| 20 steps with their messages | 0.415 / 1.503 ms | 0.473 / 1.560 ms | 1.1x / 1.0x |
| A whole session's messages | 0.422 / 1.787 ms | 0.861 / 9.158 ms | 2.0x / 5.1x |
| A whole request rebuilt | 1.140 / 5.182 ms | 2.096 / 5.790 ms | 1.8x / 1.1x |

### At 3 times the data

3 copies of every log file, each copy as sessions of its own: 3,177 sessions, 389,190 requests,
447,678 steps. TranscriptDB 359.3 MB, PostgreSQL 3,084.0 MB: 8.58x smaller. Every answer and read
was identical on both sides.

| Query | TranscriptDB 1x → 3x | PostgreSQL 1x → 3x | Faster by at 3x |
|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 4.09 → 12.34 ms | 1,838 → 5,430 ms | 440x |
| Bash fails, only test files edited, Bash passes | 4.78 → 13.84 ms | 3,389 → 10,012 ms | 723x |
| 3 Read/Grep turns in a row, then Edit next | 1.18 → 3.52 ms | 135 → 451 ms | 128x |
| Bash fails, then Bash passes the next turn | 2.39 → 7.12 ms | 122 → 403 ms | 57x |
| Edit, then Bash within 3 turns | 1.47 → 4.49 ms | 82.3 → 243 ms | 54x |
| Write a file, later Read the same file | 0.265 → 1.05 ms | 14.6 → 45.2 ms | 43x |
| 3 failing turns in a row | 2.29 → 6.99 ms | 118 → 384 ms | 55x |
| WebSearch, then WebFetch the next turn | 0.305 → 1.46 ms | 114 → 384 ms | 264x |
| Steps per tool, top 10 | 0.040 → 0.108 ms | 4.81 → 9.96 ms | 92x |
| Failure rate per tool (100+ steps) | 0.076 → 0.185 ms | 9.47 → 21.5 ms | 116x |
| Bash steps and failures per week | 0.055 → 0.150 ms | 14.2 → 53.5 ms | 356x |
| Sessions with the most failed steps | 0.049 → 0.142 ms | 8.51 → 17.4 ms | 123x |
| Most edited files | 0.112 → 0.232 ms | 4.43 → 12.7 ms | 55x |
| Average output size per tool | 0.058 → 0.136 ms | 9.59 → 20.8 ms | 152x |
| Sessions using browser tools, per day | 0.051 → 0.149 ms | 21.1 → 26.5 ms | 177x |
| User text with the word error, top sessions | 0.047 → 0.139 ms | 0.640 → 1.87 ms | 13x |
| Assistant text containing root cause | 0.108 → 0.267 ms | 82.3 → 245 ms | 917x |
| Text saying still failing or try again | 0.443 → 1.27 ms | 262 → 776 ms | 610x |
| Text matching root cause ... fix (regex) | 1.10 → 3.05 ms | 131 → 387 ms | 127x |
| Commands matching git push or pull ... origin (regex) | 1.88 → 5.35 ms | 11.7 → 34.3 ms | 6.4x |
| Commands running `git commit` | 1.12 → 3.40 ms | 13.0 → 38.8 ms | 11x |
| Shell steps per program, top 10 | 0.107 → 0.305 ms | 3.91 → 8.70 ms | 28x |
| Failure rate per program (50+ steps) | 0.284 → 0.615 ms | 9.08 → 19.9 ms | 32x |
| Test runs with failed tests, per week | 0.013 → 0.031 ms | 0.201 → 0.374 ms | 12x |
| Tests fail, then an edit, then a test run | 3.12 → 9.26 ms | 1,467 → 4,477 ms | 483x |

Reads at 3x:

| Read | TranscriptDB median / p99 | PostgreSQL median / p99 | Faster at the median / p99 |
|---|---|---|---|
| A request's messages in order | 0.817 / 3.636 ms | 4.178 / 12.341 ms | 5.1x / 3.4x |
| 20 steps with their messages | 0.452 / 2.455 ms | 1.288 / 5.558 ms | 2.8x / 2.3x |
| A whole session's messages | 0.428 / 2.043 ms | 2.104 / 12.181 ms | 4.9x / 6.0x |
| A whole request rebuilt | 0.754 / 3.677 ms | 3.124 / 10.675 ms | 4.1x / 2.9x |

Cold reads at 3x:

| Read | TranscriptDB median / p99 | PostgreSQL median / p99 | Faster at the median / p99 |
|---|---|---|---|
| A request's messages in order | 0.942 / 4.930 ms | 1.130 / 4.455 ms | 1.2x / 0.9x |
| 20 steps with their messages | 0.441 / 1.528 ms | 0.473 / 1.716 ms | 1.1x / 1.1x |
| A whole session's messages | 0.491 / 1.640 ms | 0.842 / 8.996 ms | 1.7x / 5.5x |
| A whole request rebuilt | 1.172 / 4.631 ms | 2.158 / 5.709 ms | 1.8x / 1.2x |

- At 10 times the data (1,297,300 requests) TranscriptDB was measured alone, because PostgreSQL
  would need about 10 GB, more than this disk had free. With every request written and synced
  on its own, writes took 4.7 ms at p99 while merges ran and 370 ms at worst; no write waited for
  a flush. The store took 1,182 MB and opened in 10 ms. 16 of 18 filters took 11 times their time
  on every log file. The two that read commands longer than 4 KB back from their messages took
  much longer (`rm -rf` 4.0 seconds). Reading one request took 1.75 ms at the median, against
  0.41 ms at 1x.
- The goal of 100 times the logs needs about 13 GB for the store and over 100 GB for PostgreSQL
  (estimates), more disk than this machine has.

### Reading these numbers

- PostgreSQL is reached over loopback TCP, which adds about 0.1 ms to each query. That matters
  for the rows under a millisecond.
- PostgreSQL's size includes its searchable copy of the text (143.1 MB on every log file).
  Leaving that copy out, the store is 7.09x smaller.
- The first run after opening the store is slower, because text has to be read before it is
  cached. On every log file: 29 ms for the word query, 92 ms for the phrase, 396 ms for the two
  phrases, 352 ms for the regular expression on text, 82 ms for the one on commands and 96 ms
  for `git commit`. PostgreSQL's first runs were not timed.
- Search goes through words of three letters or more. A pattern without one, such as `rm -rf`,
  reads every value: 40 ms on every log file. PostgreSQL's trigram index on commands does not
  have this limit.
- A rule added to a store that already holds steps is worked out as sessions are read until a
  merge stores its values: on every log file the first query on a new rule took 0.4 to 0.54
  seconds before a merge and about 0.1 ms after it.
- Cold reads are not 2x faster. Sessions that have gone quiet are compressed in runs of 4 MB,
  and a cold read decompresses the whole run it needs. Runs of 1 MB read faster but made the
  store 4.8% bigger.
- The data is one person's logs.

To run it yourself: `scripts/h2h.sh` (needs Homebrew `postgresql@18`).
