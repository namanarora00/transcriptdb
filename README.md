# TranscriptDB

A storage and query engine for AI agent transcripts, in Rust. Every session is stored once as a
tree of messages, so a request that repeats the whole conversation only adds what is new.

- **Exact.** Any request (and the response it got) comes back byte for byte. Thinking and images
  are never stored.
- **Any model API.** Anthropic, OpenAI Chat Completions, OpenAI Responses and Gemini are split
  into messages, tool steps and text; anything else is kept whole.
- **Queries.** A Kusto-style language over tool steps (with a `match` stage for sequences), text
  search by word, phrase or regex, and rule columns computed from steps.
- **Durable.** Nothing acknowledged is lost in a crash. Flushes and merges run in the background.
- **Deletes.** Sessions go by key or by age; `vacuum` removes every trace from disk.
- **Server.** `tdb server`: HTTP and JSON, many clients, one store per project.

## Run it

```bash
cargo build --release
./target/release/tdb server --root /var/lib/tdb --token "$TDB_TOKEN"   # 127.0.0.1:8378
```

```bash
curl -X POST localhost:8378/v1/p/acme/put -H "Authorization: Bearer $TDB_TOKEN" \
  -d '[{"id":"evt_1","session":"trace_9","body":{"model":"gpt-4o","messages":[...]},"response":{...}}]'
```

From Node, with [`clients/node`](clients/node) (no dependencies):

```js
const tdb = new TranscriptDB({ url: 'http://127.0.0.1:8378', project: 'acme', token })
await tdb.put([{ id, session: traceId, time, meta, body, response }])
await tdb.get(id)                                   // body, response, meta
await tdb.session(traceId, { bodies: true })        // a run's calls in time order
await tdb.query('summarize n = count() by tool')
await tdb.dropBefore(new Date(Date.now() - 7 * 86_400_000)); await tdb.vacuum()
```

## API

| | |
|---|---|
| `POST /v1/p/{project}/put` | Store requests; answers once durable. Non-JSON bodies go as `body_text`. |
| `GET /v1/p/{project}/requests/{id}` | One request with body, response and metadata. |
| `POST /v1/p/{project}/requests/get` | Many by id. |
| `GET /v1/p/{project}/sessions/{session}` | A session's requests in time order (`?bodies=1`). |
| `POST /v1/p/{project}/query` | Run a query. |
| `POST /v1/p/{project}/drop`, `/vacuum` | Delete sessions (`{"sessions": [...]}` or `{"before": time}`), then purge. |
| `GET /v1/p/{project}/stats`, `GET /v1/projects`, `DELETE /v1/p/{project}`, `GET /health` | |

## Queries

A Kusto-style pipe language over two sources:

- **Steps** (the default): one row per tool call paired with its result. Columns are `tool`,
  `failed`, `time`, `out_bytes`, `session`, `agent` and every input field of the call.
- **`messages`**: one row per text block the user wrote or the model said, with `role`, `text`,
  `time` and `session`.

What you can do with them:

| | |
|---|---|
| Filter | `where` with `==`, `<`, `in`, `and`/`or`/`not`; text tests `has`, `contains`, `startswith`, `endswith`, `matches regex` (case-insensitive, `_cs` for exact case). |
| Sequences | `match` binds steps in order: `then` (any later turn), `next` (the very next turn), `within N` turns, repeats `{n}` / `{n,}`, and `where no (...) between a and b` to exclude steps in the gap. |
| Shape | `extend`, `project`, `summarize ... by` with `count`, `dcount`, `countif`, `sum`, `avg`, `min`, `max`, then `top`, `order by`, `take`. |
| Time | `ago(7d)`, `now()`, `bin(time, 1d)`; differences between bound steps (`c.time - a.time`). |
| Functions | `extract(regex)`, `toint`, `tostring`, `tolower`, `strlen`, `iff`, `isnull`. |
| Rule columns | `.create column name on <filter> = <expr>` adds a stored column computed from each step (its inputs or its output), queried like any other and filled in for old steps too. |

## How it handles many clients

- **A thread per connection**, capped by `--max-connections`.
- **Writes:** the connection's thread parses and hashes the body (most of the work). One writer
  thread per project applies everything queued in arrival order and syncs the whole group once.
  Requests to one session stay in order; different sessions never wait on each other.
- **Reads** run on snapshots of committed data, so queries never block writes, and a client always
  reads what it wrote.

## Numbers

**Against PostgreSQL 18** (tuned schema, 4 parallel workers) on 1,059 real Claude Code sessions,
129,730 requests. TranscriptDB on one thread.

| | TranscriptDB | PostgreSQL | |
|:--|--:|--:|:--|
| Size on disk | 128 MB | 1,051 MB | **8.2× smaller** |
| Sequence queries | 0.27 – 4.8 ms | 15 – 3,389 ms | **51 – 709× faster** |
| Aggregations | 0.04 – 0.11 ms | 4.4 – 21 ms | **40 – 409× faster** |
| Text search | 0.05 – 1.9 ms | 0.64 – 262 ms | **6 – 764× faster** |
| Rule columns | 0.01 – 3.1 ms | 0.20 – 1,467 ms | **16 – 470× faster** |
| Reads, median | 0.24 – 0.66 ms | 0.61 – 1.9 ms | **2 – 3.5× faster** |

The gaps hold at 3 times the data.

**Against DuckDB 2.0**, in the same process, on all 12 cores, with the same tables and indexes
PostgreSQL got. TranscriptDB on one thread. Queries and reads run on this Mac's 24 real sessions
copied 30 times (720 sessions, 101,310 requests, 537 MB of distinct text). Sizes are for the
24 real sessions alone.

| | TranscriptDB | DuckDB | |
|:--|--:|--:|:--|
| Size on disk | 6.6 MB | 29 – 32 MB | **4.4 – 4.8× smaller** |
| Size, each request body kept whole | 6.6 MB | 112 MB | **17× smaller** |
| Sequence queries | 0.15 – 2.1 ms | 5.1 – 37 ms | **15 – 35× faster** |
| Aggregations | 0.03 – 0.07 ms | 1.7 – 6.6 ms | **24 – 165× faster** |
| Text search | 0.01 – 0.50 ms | 5.9 – 59 ms | **82 – 533× faster** |
| Rule columns | 0.01 – 1.4 ms | 2.1 – 30 ms | **22 – 769× faster** |
| Scans its index can't narrow | 4.1 – 17 ms | 33 – 78 ms | **4.5 – 8× faster** |
| Reads, median | 0.15 – 0.31 ms | 1.8 – 6.7 ms | **7 – 40× faster** |

Every answer and read was identical. What favours whom:

- DuckDB was handed TranscriptDB's deduplication (each distinct message once). Storing each
  request whole, as calls arrive, it is the 17× row.
- Every DuckDB query pays about 1.7 ms before it starts. DuckDB is built for big scans and full
  SQL (joins, anything ad hoc), which TranscriptDB's language doesn't offer.
- The closest race is text with no word for the index to use (a digits regex, `rm -rf`): both
  sides scan.
- DuckDB had no full-text extension (it would be downloaded at run time); warm reads only.
- The 30 copies are near-duplicates. That helps TranscriptDB's compression (17× smaller at 30x),
  so sizes are given for the real data.

**Many clients at once**: real requests of about 0.7 MB each, every write fsynced, Apple M6.

| Clients | Requests per write | Requests/s | Body MB/s | Read p99 meanwhile |
|--:|--:|--:|--:|--:|
| 1 | 1 | 230 | 154 | 0.1 ms |
| 12 (HTTP) | 1 | 739 | 498 | 0.8 ms |
| 12 (HTTP) | 8 | 2,342 | 1,562 | 1.1 ms |

Reproduce with `scripts/h2h.sh` (needs `postgresql@18`), `scripts/duck.sh` (builds DuckDB from
source) and `tdb load --store DIR --via http`.

## Tests

`scripts/test.sh` (unit and end-to-end), `scripts/crash-test.sh` (SIGKILL at random points, then
checks nothing acknowledged was lost), `scripts/planted-bugs.sh` (each planted bug must be caught).
