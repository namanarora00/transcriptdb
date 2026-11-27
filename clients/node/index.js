// Client for the TranscriptDB server (`tdb server`). No dependencies: it uses the global
// fetch (Node 18+). Every method talks to one project.

export class TranscriptDBError extends Error {
  constructor(status, message, body) {
    super(message);
    this.name = 'TranscriptDBError';
    this.status = status;
    this.body = body;
  }
}

/** Fields of a request envelope: a body as JSON (any value) or as exact text. */
function envelope(r) {
  if (r.id == null) throw new TypeError('a request needs an id');
  const e = { id: String(r.id) };
  if (r.session != null) e.session = String(r.session);
  if (r.agent != null) e.agent = String(r.agent);
  if (r.time != null) e.time = r.time instanceof Date ? r.time.toISOString() : String(r.time);
  if (r.meta !== undefined) e.meta = r.meta;
  if (r.bodyText != null) e.body_text = String(r.bodyText);
  else if (r.body !== undefined) e.body = r.body;
  else throw new TypeError(`request ${e.id} needs a body or bodyText`);
  if (r.responseText != null) e.response_text = String(r.responseText);
  else if (r.response !== undefined && r.response !== null) e.response = r.response;
  return e;
}

/** A request as the server returns it, with `body` / `response` parsed when they are JSON. */
function request(o) {
  if (o == null) return null;
  return {
    id: o.id,
    session: o.session,
    agent: o.agent,
    time: o.time,
    ref: o.ref,
    meta: o.meta ?? null,
    body: 'body' in o ? o.body : undefined,
    bodyText: o.body_text,
    response: 'response' in o ? o.response : undefined,
    responseText: o.response_text,
  };
}

export class TranscriptDB {
  /**
   * @param {object} opts
   * @param {string} opts.url   server address, e.g. http://127.0.0.1:8378
   * @param {string} opts.project
   * @param {string} [opts.token] sent as a bearer token
   * @param {number} [opts.timeoutMs] per call (default 30000)
   * @param {typeof fetch} [opts.fetch]
   */
  constructor({ url, project, token, timeoutMs = 30000, fetch: f } = {}) {
    if (!url || !project) throw new TypeError('url and project are required');
    this.url = url.replace(/\/+$/, '');
    this.project = project;
    this.token = token;
    this.timeoutMs = timeoutMs;
    this.fetch = f ?? globalThis.fetch;
    if (!this.fetch) throw new TypeError('no fetch available (Node 18+ has one)');
  }

  path(p) {
    return `${this.url}/v1/p/${encodeURIComponent(this.project)}${p}`;
  }

  async call(method, url, body, contentType = 'application/json') {
    const headers = {};
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers['content-type'] = contentType;
    const res = await this.fetch(url, {
      method,
      headers,
      body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    if (!res.ok) throw new TranscriptDBError(res.status, json?.error ?? `HTTP ${res.status}`, json ?? text);
    return json;
  }

  /**
   * Store requests; resolves once they are durable. One result per request, in order:
   * `{stored: ref}`, `{duplicate: ref}` (that id was stored before) or `{error: message}`.
   * @param {Array<{id: string, session?: string, agent?: string, time?: string|Date, meta?: any,
   *   body?: any, bodyText?: string, response?: any, responseText?: string}>} requests
   */
  async put(requests) {
    const out = await this.call('POST', this.path('/put'), JSON.stringify(requests.map(envelope)));
    return out.results;
  }

  /** One request by id, with its body and response; null if there is none. */
  async get(id, { body = true, response = true } = {}) {
    try {
      const q = `?body=${body ? 1 : 0}&response=${response ? 1 : 0}`;
      return request(await this.call('GET', this.path(`/requests/${encodeURIComponent(id)}${q}`)));
    } catch (e) {
      if (e instanceof TranscriptDBError && e.status === 404) return null;
      throw e;
    }
  }

  /** Many requests by id, in order; null for ids not stored. */
  async getMany(ids, { body = true, response = true } = {}) {
    const out = await this.call('POST', this.path('/requests/get'), { ids: ids.map(String), body, response });
    return out.requests.map(request);
  }

  /** A session's requests in time order (with bodies and responses when `bodies`). */
  async session(session, { agent = '', bodies = false } = {}) {
    try {
      const q = `?agent=${encodeURIComponent(agent)}&bodies=${bodies ? 1 : 0}`;
      const out = await this.call('GET', this.path(`/sessions/${encodeURIComponent(session)}${q}`));
      return out.requests.map(request);
    } catch (e) {
      if (e instanceof TranscriptDBError && e.status === 404) return [];
      throw e;
    }
  }

  /** Run a query: `{columns, rows, rowsTotal, ms}`; rows are arrays of values. */
  async query(q, { limit = 1000 } = {}) {
    const out = await this.call('POST', this.path('/query'), { q, limit });
    return { columns: out.columns, rows: out.rows, rowsTotal: out.rows_total, ms: out.ms };
  }

  /** Delete sessions, by key (`[{session, agent?}]` or session ids). */
  async dropSessions(sessions) {
    const list = sessions.map((s) => (typeof s === 'string' ? { session: s } : s));
    return this.call('POST', this.path('/drop'), { sessions: list });
  }

  /** Delete every session whose newest request is older than `before` (Date, ISO or ms). */
  async dropBefore(before) {
    const b = before instanceof Date ? before.toISOString() : String(before);
    return this.call('POST', this.path('/drop'), { before: b });
  }

  /** Remove deleted sessions from every file (rewrites the project's store). */
  async vacuum() {
    return this.call('POST', this.path('/vacuum'));
  }

  async stats() {
    return this.call('GET', this.path('/stats'));
  }

  /** Delete the whole project. */
  async deleteProject() {
    return this.call('DELETE', this.path(''));
  }

  async health() {
    return this.call('GET', `${this.url}/health`);
  }
}
