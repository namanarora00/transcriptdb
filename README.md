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

```
match a: tool == "Bash" and failed
  then b: tool in ("Edit", "Write") and file_path contains "test"
  then c: tool == "Bash" and not failed
| summarize n = count(), sessions = dcount(session) by a.tool

messages | where role == "user" and text has "revert" | summarize n = count() by session

.create column program on tool == "Bash" = extract(command, @"^\s*(\S+)")
```

## How it handles many clients

- **A thread per connection**, capped by `--max-connections`.
- **Writes:** the connection's thread parses and hashes the body (most of the work). One writer
  thread per project applies everything queued in arrival order and syncs the whole group once.
  Requests to one session stay in order; different sessions never wait on each other.
- **Reads** run on snapshots of committed data, so queries never block writes, and a client always
  reads what it wrote.

## Numbers

Against PostgreSQL 18 with a tuned schema, on 1,059 real Claude Code sessions (129,730 requests):

| | TranscriptDB vs PostgreSQL |
|---|---|
| Size on disk | 128 MB vs 1,051 MB (8.2x smaller) |
| Sequence queries | 0.3–4.8 ms; 51–709x faster |
| Aggregations | under 0.12 ms; 40–409x faster |
| Text search | 0.05–1.9 ms; 6–764x faster |
| Reading a request | 0.6 ms median; 2–3.5x faster |

At 3 times the data the gaps hold. Many clients, real requests of about 0.7 MB each, every write
fsynced (Apple M6):

| | Requests/s | Body MB/s | Read p99 meanwhile |
|---|---|---|---|
| 1 client | 230 | 154 | 0.1 ms |
| 12 clients, HTTP | 739 | 498 | 0.8 ms |
| 12 clients, HTTP, 8 per put | 2,342 | 1,562 | 1.1 ms |

Reproduce with `scripts/h2h.sh` (needs `postgresql@18`) and `tdb load --store DIR --via http`.

## Tests

`scripts/test.sh` (unit and end-to-end), `scripts/crash-test.sh` (SIGKILL at random points, then
checks nothing acknowledged was lost), `scripts/planted-bugs.sh` (each planted bug must be caught).
