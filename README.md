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

Every comparison checks that both engines return identical results for every query and every
read. TranscriptDB runs on one thread; the other engine uses the parallelism listed for it.
Ranges span the queries or read types in each row. Datasets, machines and measurement details
are under [Methodology](#methodology).

### PostgreSQL 18, dataset A

| | TranscriptDB | PostgreSQL | |
|:--|--:|--:|:--|
| Size on disk | 128 MB | 1,051 MB | 8.2× smaller |
| Sequence queries (8) | 0.27 – 4.8 ms | 15 – 3,389 ms | 51 – 709× faster |
| Aggregations (7) | 0.04 – 0.11 ms | 4.4 – 21 ms | 40 – 409× faster |
| Text search (6) | 0.05 – 1.9 ms | 0.64 – 262 ms | 6 – 764× faster |
| Rule columns (4) | 0.01 – 3.1 ms | 0.20 – 1,467 ms | 16 – 470× faster |
| Point reads (4 types), median, warm | 0.24 – 0.66 ms | 0.61 – 1.9 ms | 2 – 3.5× faster |
| Point reads (4 types), median, cold | 0.42 – 1.1 ms | 0.47 – 2.1 ms | 1.1 – 2× faster |

At 3× dataset A (each session replicated three times): 359 MB vs 3,084 MB on disk, and every
query 6 – 917× faster.

### DuckDB 2.0, dataset B

| | TranscriptDB | DuckDB | |
|:--|--:|--:|:--|
| Size on disk, unreplicated sessions | 6.6 MB | 32 MB | 4.9× smaller |
| Size on disk, unreplicated sessions, DuckDB with zstd forced | 6.6 MB | 13 MB | 2× smaller, but DuckDB point reads slow to 5 – 11 ms |
| Sequence queries (8) | 0.13 – 2.0 ms | 5.4 – 32 ms | 14 – 42× faster |
| Aggregations (7) | 0.03 – 0.07 ms | 1.7 – 6.3 ms | 24 – 167× faster |
| Text search (6) | 0.01 – 0.51 ms | 2.1 – 59 ms | 41 – 513× faster |
| Rule columns (4) | 0.01 – 1.5 ms | 2.1 – 27 ms | 18 – 671× faster |
| Queries no index can narrow (2) | 4.2 – 17 ms | 33 – 76 ms | 4.4 – 8× faster |
| Point reads (4 types), median, warm | 0.19 – 0.36 ms | 2.1 – 8.2 ms | 6 – 23× faster |
| Point reads (4 types), median, cold | 0.38 – 0.70 ms | 2.5 – 8.3 ms | 4.7 – 12× faster |

### DuckDB storing raw request bodies, dataset B unreplicated

One row per request, keyed by request id, holding the body as sent (3,377 requests, 3,078 MB).
Reads fetch one whole request by id.

| | Size | Fetch by id, median, warm | Fetch by id, median, cold |
|:--|--:|--:|--:|
| TranscriptDB | 6.6 MB | 0.17 ms | 0.73 ms |
| DuckDB, default compression | 113 MB | 50 ms | 52 ms |
| DuckDB, each body zstd-compressed by the application (levels 19 and 3) | 231 – 286 MB | 5.5 – 6.7 ms | 8.6 – 10 ms |
| DuckDB, each body zstd-compressed by the application, DuckDB compression off | 841 MB | 0.69 ms | 0.87 ms |
| DuckDB, uncompressed | 3,080 MB | 0.37 ms | 0.73 ms |

### Write throughput, dataset C

Each client writes its own sessions in order; every commit is fsynced. Two reader threads fetch
already-acknowledged requests meanwhile.

| Clients | Requests per call | Requests/s | Body MB/s | Read p99 under load |
|--:|--:|--:|--:|--:|
| 1, in process | 1 | 230 | 154 | 0.1 ms |
| 12, over HTTP | 1 | 739 | 498 | 0.8 ms |
| 12, over HTTP | 8 | 2,342 | 1,562 | 1.1 ms |

### Methodology

Datasets (all real Claude Code sessions):

- A: 1,059 sessions, 129,730 requests, 617 MB of distinct message text.
- B: 24 sessions, 3,377 requests, 3,078 MB of request bodies, 17 MB of distinct message text.
  Queries and reads ran on B replicated 30 times with unique ids and text markers (720
  sessions, 101,310 requests, 537 MB of distinct text). Sizes are for the unreplicated 24
  sessions, because replicas compress unusually well.
- C: 943 requests from 12 sessions, 631 MB of request bodies (0.67 MB on average).

Machines:

- PostgreSQL comparison: Apple M4, 10 cores, 16 GB RAM, measured 2026-11-25.
- DuckDB comparison and write throughput: Apple M6, 12 cores, 32 GB RAM, measured 2026-11-27.
- On both machines a virtual machine was running and using several cores during the runs.

Engines:

- PostgreSQL 18 over loopback TCP, up to 4 parallel workers, 256 MB of shared buffers. Schema:
  each distinct message stored once, a step table with indexes, a materialized table of turn
  paths for sequence queries, everything needed to rebuild requests, and for search a copy of
  the text with a full-text index plus trigram indexes on text and commands.
- DuckDB 2.0.1 embedded in the same process, 12 threads. The PostgreSQL schema plus what
  measured fastest for it: an inverted word table built by the application, message ids
  denormalized onto steps, batched `IN` lookups for reads, rewritten window queries, primary-key
  indexes and prepared statements. Its full-text extension was not used (it is downloaded at
  run time).

Measurement:

- Query times are the best of 7 runs. Read times are medians over 300 random items.
- Warm reads run with each engine's caches populated. Cold reads empty each engine's own
  caches before every read: TranscriptDB's caches; PostgreSQL's shared buffers
  (`pg_buffercache_evict_all()`); DuckDB closed and reopened, with its buffer pool checked
  empty. The operating system's page cache stays warm for both.
- DuckDB query times and read times come from separate runs; the machine was busier during the
  read run, which slowed both engines by about 15 – 20% relative to the query run.
- Size on disk: TranscriptDB's store files; PostgreSQL's tables, indexes and TOAST, including
  its search copy; DuckDB's database file after a checkpoint, including its text copy and word
  table.

Caveats:

- DuckDB spends about 1.7 ms per query before execution, and supports full SQL (joins, ad hoc
  queries), which TranscriptDB's language does not.
- Cold, at p99, PostgreSQL was faster on one read type (a request's messages, 0.8×).

Reproduce: `scripts/h2h.sh` (PostgreSQL), `scripts/duck.sh` (DuckDB), `tdb load` (throughput).

## Testing

```bash
scripts/test.sh           # unit and integration tests
scripts/crash-test.sh     # kills the writer at random points and verifies recovery
scripts/planted-bugs.sh   # mutation checks: each planted bug must be caught by a test
```
