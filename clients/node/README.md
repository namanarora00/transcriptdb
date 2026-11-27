# @transcriptdb/client

A client for `tdb server` with no dependencies (it uses Node's global `fetch`, Node 18+). Every
method works on one project. See the server section of the main README for the API.

```js
import { TranscriptDB } from '@transcriptdb/client'

const tdb = new TranscriptDB({ url: 'http://127.0.0.1:8378', project: 'acme', token: process.env.TDB_TOKEN })

// bodies and responses as JSON values (stored as JSON.stringify gives them), or as exact
// text with bodyText / responseText
const results = await tdb.put([{ id: 'evt_1', session: 'trace_9', time: new Date(), meta: { feature: 'dash' },
                                 body: requestBody, response: responseBody }])
// [{ stored: '...' }] or [{ duplicate: '...' }] or [{ error: '...' }]

await tdb.get('evt_1')                        // { id, session, time, meta, body, response }
await tdb.getMany(['evt_1', 'evt_2'], { response: false })
await tdb.session('trace_9', { bodies: true }) // the trace's calls in time order
await tdb.query('summarize n = count() by tool')
await tdb.dropSessions(['trace_9'])
await tdb.dropBefore(new Date(Date.now() - 7 * 86_400_000))
await tdb.vacuum()
```

Errors from the server throw `TranscriptDBError` with its HTTP `status`; `get` returns `null`
and `session` returns `[]` for what is not stored.
