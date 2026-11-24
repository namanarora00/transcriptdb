# TranscriptDB

A storage and query engine for agent transcripts, written in Rust.

## What it is

- **Storage.** Each session is stored once, as an ordered tree of messages. Any request can be
  rebuilt byte for byte, except thinking and images, which are never stored and come back as
  empty strings. A crash loses nothing that was acknowledged.
- **Steps.** Each tool call is paired with its result and kept in columns.
- **Text.** A word index over what the user wrote and what the assistant said, searched by
  word, phrase or regular expression. Tool outputs are stored but not searched.
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

## Benchmarks against PostgreSQL

Both sides hold the same data: 1,730 Claude Code log files from one Mac, which is 1,068
sessions, 129,647 requests, 150,093 steps, 76,177 text blocks and 615.5 MB of distinct
message text. Every answer and every read was identical on both sides.

- **PostgreSQL 18** has a tuned schema: each distinct message stored once, a step table with
  indexes, a materialized table of turn paths for the sequence queries, and everything needed to
  rebuild whole requests. For search it has a table of the same text with a word index and a
  trigram index, and a trigram index on commands. It may use up to 4 parallel workers.
- **TranscriptDB** runs on one thread.
- Times are the best of 7 with warm caches, measured on 2026-11-24 on an Apple M4 with 10 cores
  and 16 GB while another job was using about six of the cores, so both sides are slower here
  than on an idle machine.

### Size on disk, with search on both sides

| Data | TranscriptDB | PostgreSQL | Smaller by |
|---|---|---|---|
| Every log file | 126.9 MB | 1,043.9 MB | 8.23x |
| 200-file sample | 20.8 MB | 182.8 MB | 8.78x |

### Sequence queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 5.42 ms | 2,807 ms | 518x |
| Bash fails, only test files edited, Bash passes | 5.81 ms | 5,073 ms | 873x |
| 3 Read/Grep turns in a row, then Edit next | 1.37 ms | 193 ms | 141x |
| Bash fails, then Bash passes the next turn | 2.80 ms | 174 ms | 62x |
| Edit, then Bash within 3 turns | 1.86 ms | 132 ms | 71x |
| Write a file, later Read the same file | 0.385 ms | 21.0 ms | 55x |
| 3 failing turns in a row | 2.90 ms | 169 ms | 58x |
| WebSearch, then WebFetch the next turn | 0.385 ms | 165 ms | 429x |

### Other queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Steps per tool, top 10 | 0.045 ms | 6.70 ms | 149x |
| Failure rate per tool (100+ steps) | 0.178 ms | 12.8 ms | 72x |
| Bash steps and failures per week | 0.135 ms | 19.4 ms | 144x |
| Sessions with the most failed steps | 0.079 ms | 12.2 ms | 155x |
| Most edited files | 0.132 ms | 6.74 ms | 51x |
| Average output size per tool | 0.150 ms | 13.5 ms | 90x |
| Sessions using browser tools, per day | 0.072 ms | 28.6 ms | 399x |

### Text search

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| User text with the word error, top sessions | 0.058 ms | 1.42 ms | 25x |
| Assistant text containing root cause | 1.27 ms | 110 ms | 87x |
| Text saying still failing or try again | 5.97 ms | 354 ms | 59x |
| Text matching root cause ... fix (regex) | 6.78 ms | 170 ms | 25x |
| Commands matching git push or pull ... origin (regex) | 2.21 ms | 16.0 ms | 7.2x |
| Commands running `git commit` | 1.25 ms | 16.5 ms | 13x |

On the 200-file sample (156 sessions, 26,915 steps) the sequence queries are 35x to 1,198x
faster, the other queries 40x to 160x and text search 10x to 518x.

### Reads

| Read (median / 95th percentile) | TranscriptDB | PostgreSQL | Faster at the median |
|---|---|---|---|
| A request's messages in order | 1.269 / 4.991 ms | 2.122 / 6.632 ms | 1.7x |
| 20 steps with their messages | 0.513 / 1.576 ms | 0.990 / 3.857 ms | 1.9x |
| A whole session's messages | 0.508 / 1.962 ms | 1.512 / 5.612 ms | 3.0x |
| A whole request rebuilt | 0.948 / 4.186 ms | 3.007 / 7.186 ms | 3.2x |

### Reading these numbers

- PostgreSQL is reached over loopback TCP, which adds about 0.1 ms to each query. That matters
  for the rows under a millisecond.
- PostgreSQL's size includes its searchable copy of the text (141.9 MB on every log file).
  Leaving that copy out, the store is 7.11x smaller.
- The first run after opening the store is slower, because text has to be read before it is
  cached. On every log file: 40 ms for the word query, 111 ms for the phrase, 521 ms for the two
  phrases, 489 ms for the regular expression on text, 136 ms for the one on commands and
  163 ms for `git commit`.
  PostgreSQL's first runs were not timed.
- Search goes through words of three letters or more. A pattern without one, such as
  `rm -rf`, reads every value (345 ms on every log file). A regular expression whose only
  plain text is a common word reads every text holding that word (`error\[E[0-9]+\]` took 446 ms).
- The data is one person's logs.

To run it yourself: `scripts/h2h.sh` (needs Homebrew `postgresql@18`).
