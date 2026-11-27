# TranscriptDB

A storage and query engine for LLM agent transcripts, written in Rust.

Agents resend the whole conversation on every model call. TranscriptDB stores each session as a
tree of messages, keeps every message once, and indexes tool calls and text for querying.

## Features

- Lossless: requests and responses come back exactly as received (thinking blocks and images
  are dropped).
- Anthropic, OpenAI Chat Completions, OpenAI Responses and Gemini formats; other bodies stored as-is.
- Kusto-style query language with sequence matching over tool calls.
- Write-ahead log, background compaction, crash recovery.
- Deletion by session or age, with `vacuum`.
- HTTP server with per-project stores, group commit and snapshot reads.

## Quick start

```bash
cargo build --release
./target/release/tdb server --root /var/lib/tdb --token "$TDB_TOKEN"   # 127.0.0.1:8378
```

```bash
curl -X POST localhost:8378/v1/p/acme/put -H "Authorization: Bearer $TDB_TOKEN" \
  -d '[{"id":"evt_1","session":"trace_9","body":{...},"response":{...}}]'
```

Node client: [`clients/node`](clients/node).

## HTTP API

| Endpoint | |
|---|---|
| `POST /v1/p/{project}/put` | Store requests (returns once durable) |
| `GET /v1/p/{project}/requests/{id}` | Fetch a request |
| `POST /v1/p/{project}/requests/get` | Fetch requests in bulk |
| `GET /v1/p/{project}/sessions/{session}` | A session's requests in order |
| `POST /v1/p/{project}/query` | Run a query |
| `POST /v1/p/{project}/drop`, `/vacuum` | Delete sessions, purge from disk |

## Queries

Two sources: `steps` (one row per tool call and result) and `messages` (one row per text block).

```
match a: tool == "Bash" and failed
  then b: tool in ("Edit", "Write")
  then c: tool == "Bash" and not failed
| summarize n = count() by a.tool
```

Supports `where`, `match ... then/next/within`, `summarize`, `extend`, `project`, `top`, `order
by`; text tests `has`, `contains`, `matches regex`; and stored rule columns
(`.create column name on <filter> = <expr>`).

## Benchmarks

Identical results on every query and read. TranscriptDB single-threaded.

**vs PostgreSQL 18** (dataset A)

| | TranscriptDB | PostgreSQL | |
|:--|--:|--:|--:|
| Size | 128 MB | 1,051 MB | 8.2× |
| Sequence queries | 0.27 – 4.8 ms | 15 – 3,389 ms | 51 – 709× |
| Aggregations | 0.04 – 0.11 ms | 4.4 – 21 ms | 40 – 409× |
| Text search | 0.05 – 1.9 ms | 0.64 – 262 ms | 6 – 764× |
| Point reads, warm | 0.24 – 0.66 ms | 0.61 – 1.9 ms | 2 – 3.5× |
| Point reads, cold | 0.42 – 1.1 ms | 0.47 – 2.1 ms | 1.1 – 2× |

**vs DuckDB 2.0, 12 threads** (dataset B)

| | TranscriptDB | DuckDB | |
|:--|--:|--:|--:|
| Size | 6.6 MB | 32 MB | 4.9× |
| Size, DuckDB zstd (reads slow to 5 – 11 ms) | 6.6 MB | 13 MB | 2× |
| Sequence queries | 0.13 – 2.0 ms | 5.4 – 32 ms | 14 – 42× |
| Aggregations | 0.03 – 0.07 ms | 1.7 – 6.3 ms | 24 – 167× |
| Text search | 0.01 – 0.51 ms | 2.1 – 59 ms | 41 – 513× |
| Full scans | 4.2 – 17 ms | 33 – 76 ms | 4.4 – 8× |
| Point reads, warm | 0.19 – 0.36 ms | 2.1 – 8.2 ms | 6 – 23× |
| Point reads, cold | 0.38 – 0.70 ms | 2.5 – 8.3 ms | 4.7 – 12× |

**Raw request bodies in DuckDB** (dataset B, 3,078 MB, fetch one request by id)

| | Size | Warm | Cold |
|:--|--:|--:|--:|
| TranscriptDB | 6.6 MB | 0.17 ms | 0.73 ms |
| DuckDB | 113 MB | 50 ms | 52 ms |
| DuckDB, per-row zstd | 231 – 286 MB | 5.5 – 6.7 ms | 8.6 – 10 ms |
| DuckDB, uncompressed | 3,080 MB | 0.37 ms | 0.73 ms |

**Write throughput** (dataset C, fsync per commit)

| Clients | Requests/call | Requests/s | MB/s | Read p99 |
|--:|--:|--:|--:|--:|
| 1 | 1 | 230 | 154 | 0.1 ms |
| 12 | 1 | 739 | 498 | 0.8 ms |
| 12 | 8 | 2,342 | 1,562 | 1.1 ms |

Methodology:

- Data: real Claude Code sessions. A: 1,059 sessions, 129,730 requests. B: 24 sessions (3,377
  requests); queries and reads on 30 marked replicas, sizes on the originals. C: 943 requests,
  0.67 MB average.
- Hardware: Apple M4 (PostgreSQL), Apple M6 (DuckDB, throughput).
- Queries: best of 7. Reads: median of 300. Cold: each engine's caches emptied before every
  read, OS page cache warm.
- DuckDB got PostgreSQL's schema plus an app-built word index, batched lookups and rewritten
  queries. It supports full SQL; TranscriptDB's language does not.

Run them with `scripts/h2h.sh`, `scripts/duck.sh` and `tdb load`.

## Testing

```bash
scripts/test.sh           # unit and integration tests
scripts/crash-test.sh     # kill -9 at random points, verify recovery
scripts/planted-bugs.sh   # each planted bug must be caught
```
