# TranscriptDB

A storage and query engine for LLM agent transcripts, written in Rust.

Agent frameworks resend the full conversation with every model call, so a session of a few
hundred calls carries the same history hundreds of times. TranscriptDB stores each session as a
tree of messages and keeps every message once, then indexes tool calls and text for querying.

## Features

- Lossless storage: requests and responses are reconstructed exactly as received. Thinking
  blocks and images are dropped at ingest.
- Anthropic Messages, OpenAI Chat Completions, OpenAI Responses and Gemini request formats.
  Other bodies (embeddings, audio, non-JSON) are stored as-is.
- A Kusto-style query language over tool calls and message text, with sequence matching.
- Write-ahead log, background flushes and merges, crash recovery.
- Deletion by session or by age, with `vacuum` to purge deleted data from disk.
- An HTTP/JSON server with per-project stores, group commit and snapshot reads.

## Quick start

```bash
cargo build --release
./target/release/tdb server --root /var/lib/tdb --token "$TDB_TOKEN"   # listens on 127.0.0.1:8378
```

```bash
curl -X POST localhost:8378/v1/p/acme/put -H "Authorization: Bearer $TDB_TOKEN" \
  -d '[{"id":"evt_1","session":"trace_9","body":{"model":"gpt-4o","messages":[...]},"response":{...}}]'
```

Node client ([`clients/node`](clients/node), no dependencies):

```js
const tdb = new TranscriptDB({ url: 'http://127.0.0.1:8378', project: 'acme', token })
await tdb.put([{ id, session: traceId, time, meta, body, response }])
await tdb.get(id)
await tdb.session(traceId, { bodies: true })
await tdb.query('summarize n = count() by tool')
await tdb.dropBefore(new Date(Date.now() - 7 * 86_400_000))
```

## HTTP API

| Endpoint | Description |
|---|---|
| `POST /v1/p/{project}/put` | Store requests. Returns once they are durable. |
| `GET /v1/p/{project}/requests/{id}` | Fetch a request with its response and metadata. |
| `POST /v1/p/{project}/requests/get` | Fetch requests by id in bulk. |
| `GET /v1/p/{project}/sessions/{session}` | List a session's requests in time order. |
| `POST /v1/p/{project}/query` | Run a query. |
| `POST /v1/p/{project}/drop` | Delete sessions by key or by age. |
| `POST /v1/p/{project}/vacuum` | Purge deleted sessions from disk. |
| `GET /v1/p/{project}/stats` | Store statistics. |
| `DELETE /v1/p/{project}` | Delete a project. |

## Query language

Queries run over one of two sources:

- `steps` (default): one row per tool call and its result. Columns: `tool`, `failed`, `time`,
  `out_bytes`, `session`, `agent`, and each input field of the call.
- `messages`: one row per text block from the user or the model. Columns: `role`, `text`,
  `time`, `session`.

| | |
|---|---|
| Filtering | `where` with comparisons, `in`, `and`/`or`/`not`; `has`, `contains`, `startswith`, `endswith`, `matches regex` (case-insensitive; `_cs` variants are case-sensitive) |
| Sequences | `match a: ... then b: ...`, `next`, `within N`, repeats (`{n}`, `{n,}`), exclusions (`where no (...) between a and b`) |
| Aggregation | `summarize ... by` with `count`, `dcount`, `countif`, `sum`, `avg`, `min`, `max`; `extend`, `project`, `top`, `order by`, `take` |
| Time | `ago()`, `now()`, `bin()`, durations between matched steps |
| Functions | `extract`, `toint`, `tostring`, `tolower`, `strlen`, `iff`, `isnull` |
| Rule columns | `.create column name on <filter> = <expr>` defines a column computed per step and stored |

```
match a: tool == "Bash" and failed
  then b: tool in ("Edit", "Write")
  then c: tool == "Bash" and not failed
| summarize n = count() by a.tool
```

## Concurrency

The server uses one thread per connection. Request bodies are parsed and hashed on the
connection thread; a single writer thread per project applies writes in arrival order and
commits each batch with one fsync. Reads and queries run against snapshots and never block
writes.

## Benchmarks

All engines held the same data and returned identical results for every query and read.
TranscriptDB ran single-threaded.

**PostgreSQL 18** (tuned schema, parallel workers enabled), dataset A:

| | TranscriptDB | PostgreSQL | |
|:--|--:|--:|:--|
| Size on disk | 128 MB | 1,051 MB | 8.2× smaller |
| Sequence queries | 0.27 – 4.8 ms | 15 – 3,389 ms | 51 – 709× faster |
| Aggregations | 0.04 – 0.11 ms | 4.4 – 21 ms | 40 – 409× faster |
| Text search | 0.05 – 1.9 ms | 0.64 – 262 ms | 6 – 764× faster |
| Rule columns | 0.01 – 3.1 ms | 0.20 – 1,467 ms | 16 – 470× faster |
| Point reads, median | 0.24 – 0.66 ms | 0.61 – 1.9 ms | 2 – 3.5× faster |

**DuckDB 2.0** (embedded, 12 threads), dataset B. DuckDB was given the same schema as
PostgreSQL plus the optimizations that measured fastest: an inverted word index, denormalized
message ids, batched lookups, and rewritten window queries.

| | TranscriptDB | DuckDB | |
|:--|--:|--:|:--|
| Sequence queries | 0.13 – 2.0 ms | 5.4 – 32 ms | 14 – 42× faster |
| Aggregations | 0.03 – 0.07 ms | 1.7 – 6.3 ms | 24 – 167× faster |
| Text search | 0.01 – 0.51 ms | 2.1 – 59 ms | 41 – 513× faster |
| Rule columns | 0.01 – 1.5 ms | 2.1 – 27 ms | 18 – 671× faster |
| Full scans | 4.2 – 17 ms | 33 – 76 ms | 4.4 – 8× faster |
| Point reads, median | 0.14 – 0.30 ms | 1.8 – 6.9 ms | 6 – 24× faster |

Storing raw request bodies in DuckDB (one row per call, 3,078 MB of input):

| | Size | Fetch by id, median |
|:--|--:|--:|
| TranscriptDB | 6.6 MB | 0.17 ms |
| DuckDB, default compression | 113 MB | 49 ms |
| DuckDB, per-row zstd in the application | 231 – 841 MB | 0.7 – 6.5 ms |
| DuckDB, uncompressed | 3,080 MB | 0.36 ms |

**Write throughput**, requests averaging 0.7 MB, fsync on every commit:

| Clients | Requests per call | Requests/s | MB/s | Read p99 under load |
|--:|--:|--:|--:|--:|
| 1 | 1 | 230 | 154 | 0.1 ms |
| 12 | 1 | 739 | 498 | 0.8 ms |
| 12 | 8 | 2,342 | 1,562 | 1.1 ms |

### Methodology

- Dataset A: 1,059 Claude Code sessions, 129,730 requests, 617 MB of distinct message text.
- Dataset B: 24 Claude Code sessions (3,377 requests) replicated 30 times with unique ids and
  text markers: 720 sessions, 101,310 requests, 537 MB of distinct text. Sizes are reported on
  the unreplicated sessions, since replicas compress unusually well.
- Hardware: Apple M6, 12 cores, 32 GB RAM. Query times are the best of 7 runs; read times are
  over 300 random items with warm caches.
- Caveats: DuckDB pays roughly 1.7 ms of fixed overhead per query and supports full SQL, which
  TranscriptDB's language does not. DuckDB's full-text extension was not used. Forcing zstd on
  DuckDB's deduplicated tables shrinks them to 2× TranscriptDB's size at the cost of 5 – 11 ms
  median reads.

Scripts: `scripts/h2h.sh` (PostgreSQL), `scripts/duck.sh` (DuckDB), `tdb load` (throughput).

## Testing

```bash
scripts/test.sh           # unit and integration tests
scripts/crash-test.sh     # kills the writer at random points and verifies recovery
scripts/planted-bugs.sh   # mutation checks: each planted bug must be caught by a test
```
