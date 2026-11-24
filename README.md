# TranscriptDB

A storage and query engine for agent transcripts, written in Rust.

## What it is

- **Storage.** Each session is stored once, as an ordered tree of messages. Any request can be
  rebuilt byte for byte, except thinking and images, which are never stored and come back as
  empty strings. A crash loses nothing that was acknowledged.
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

Both sides hold the same data: 1,730 Claude Code log files from one Mac, which is 1,068
sessions, 129,647 requests, 150,093 steps, 76,177 text blocks and 615.5 MB of distinct
message text. Every answer and every read was identical on both sides.

- **PostgreSQL 18** has a tuned schema: each distinct message stored once, a step table with
  indexes, a materialized table of turn paths for the sequence queries, and everything needed to
  rebuild whole requests. For search it has a table of the same text with a word index and a
  trigram index, and a trigram index on commands. For the rule columns its step tables
  hold the same two columns, each with an index. It may use up to 4 parallel workers.
- **TranscriptDB** runs on one thread.
- Times are the best of 7 with warm caches, measured on 2026-11-24 on an Apple M4 with 10 cores
  and 16 GB while another job was using about six of the cores, so both sides are slower here
  than on an idle machine.

### Size on disk, with search on both sides

| Data | TranscriptDB | PostgreSQL | Smaller by |
|---|---|---|---|
| Every log file | 127.1 MB | 1,046.8 MB | 8.23x |
| 200-file sample | 20.9 MB | 183.3 MB | 8.78x |

### Sequence queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Bash, then Edit, then Bash; no Read between | 5.68 ms | 3,042 ms | 535x |
| Bash fails, only test files edited, Bash passes | 6.78 ms | 5,522 ms | 815x |
| 3 Read/Grep turns in a row, then Edit next | 1.66 ms | 212 ms | 128x |
| Bash fails, then Bash passes the next turn | 3.39 ms | 195 ms | 57x |
| Edit, then Bash within 3 turns | 2.12 ms | 129 ms | 61x |
| Write a file, later Read the same file | 0.658 ms | 23.9 ms | 36x |
| 3 failing turns in a row | 3.30 ms | 191 ms | 58x |
| WebSearch, then WebFetch the next turn | 0.492 ms | 178 ms | 361x |

### Other queries

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Steps per tool, top 10 | 0.062 ms | 8.06 ms | 131x |
| Failure rate per tool (100+ steps) | 0.118 ms | 14.3 ms | 121x |
| Bash steps and failures per week | 0.090 ms | 22.4 ms | 248x |
| Sessions with the most failed steps | 0.131 ms | 15.8 ms | 120x |
| Most edited files | 0.163 ms | 6.46 ms | 40x |
| Average output size per tool | 0.168 ms | 13.5 ms | 80x |
| Sessions using browser tools, per day | 0.135 ms | 29.5 ms | 218x |

### Text search

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| User text with the word error, top sessions | 0.067 ms | 1.47 ms | 22x |
| Assistant text containing root cause | 1.47 ms | 105 ms | 71x |
| Text saying still failing or try again | 7.90 ms | 378 ms | 48x |
| Text matching root cause ... fix (regex) | 7.21 ms | 189 ms | 26x |
| Commands matching git push or pull ... origin (regex) | 2.47 ms | 16.8 ms | 6.8x |
| Commands running `git commit` | 1.40 ms | 17.5 ms | 13x |

### Rule columns

Two rules are defined on both sides: the program a shell command runs, and the number of
failed tests a test run reports.

| Query | TranscriptDB | PostgreSQL | Faster by |
|---|---|---|---|
| Shell steps per program, top 10 | 0.148 ms | 6.37 ms | 43x |
| Failure rate per program (50+ steps) | 0.400 ms | 12.0 ms | 30x |
| Test runs with failed tests, per week | 0.016 ms | 0.348 ms | 22x |
| Tests fail, then an edit, then a test run | 4.24 ms | 2,348 ms | 553x |

On the 200-file sample (156 sessions, 26,915 steps) the sequence queries are 39x to 683x
faster, the other queries 36x to 138x, text search 8.9x to 575x and the rule
columns 19x to 449x.

### Reads

| Read (median / 95th percentile) | TranscriptDB | PostgreSQL | Faster at the median |
|---|---|---|---|
| A request's messages in order | 1.266 / 4.810 ms | 2.174 / 5.729 ms | 1.7x |
| 20 steps with their messages | 0.469 / 1.492 ms | 0.964 / 2.308 ms | 2.1x |
| A whole session's messages | 0.582 / 1.989 ms | 1.570 / 6.206 ms | 2.7x |
| A whole request rebuilt | 0.986 / 3.896 ms | 3.177 / 6.721 ms | 3.2x |

### Reading these numbers

- PostgreSQL is reached over loopback TCP, which adds about 0.1 ms to each query. That matters
  for the rows under a millisecond.
- PostgreSQL's size includes its searchable copy of the text (141.9 MB on every log file).
  Leaving that copy out, the store is 7.12x smaller.
- The first run after opening the store is slower, because text has to be read before it is
  cached. On every log file: 40 ms for the word query, 111 ms for the phrase, 521 ms for the two
  phrases, 489 ms for the regular expression on text, 136 ms for the one on commands and
  163 ms for `git commit`.
  PostgreSQL's first runs were not timed.
- Search goes through words of three letters or more. A pattern without one, such as
  `rm -rf`, reads every value (345 ms on every log file). A regular expression whose only
  plain text is a common word reads every text holding that word (`error\[E[0-9]+\]` took 446 ms).
- A rule added to a store that already holds steps is worked out as sessions are read until
  a merge stores its values: on every log file the first query on the two rules here took 1.1 to
  1.3 seconds before a merge and 2 ms after it.
- The data is one person's logs.

To run it yourself: `scripts/h2h.sh` (needs Homebrew `postgresql@18`).
