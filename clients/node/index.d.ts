export interface RequestIn {
  id: string;
  /** The conversation the request belongs to (a trace id); defaults to the id. */
  session?: string;
  agent?: string;
  time?: string | Date;
  meta?: unknown;
  /** The request body as JSON (stored as JSON.stringify gives it) ... */
  body?: unknown;
  /** ... or as exact text. */
  bodyText?: string;
  response?: unknown;
  responseText?: string;
}

export type PutResult = { stored: string } | { duplicate: string } | { error: string };

export interface RequestOut {
  id: string;
  session: string;
  agent: string;
  time: string;
  ref: string;
  meta: unknown;
  /** Present when the body is JSON. */
  body?: unknown;
  /** Present when the body is not JSON. */
  bodyText?: string;
  response?: unknown;
  responseText?: string;
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rowsTotal: number;
  ms: number;
}

export interface DropResult {
  sessions: number;
  requests: number;
  dead_bytes: number;
}

export declare class TranscriptDBError extends Error {
  status: number;
  body: unknown;
}

export declare class TranscriptDB {
  constructor(opts: { url: string; project: string; token?: string; timeoutMs?: number; fetch?: typeof fetch });
  put(requests: RequestIn[]): Promise<PutResult[]>;
  get(id: string, opts?: { body?: boolean; response?: boolean }): Promise<RequestOut | null>;
  getMany(ids: string[], opts?: { body?: boolean; response?: boolean }): Promise<(RequestOut | null)[]>;
  session(session: string, opts?: { agent?: string; bodies?: boolean }): Promise<RequestOut[]>;
  query(q: string, opts?: { limit?: number }): Promise<QueryResult>;
  dropSessions(sessions: (string | { session: string; agent?: string })[]): Promise<DropResult>;
  dropBefore(before: Date | string | number): Promise<DropResult>;
  vacuum(): Promise<Record<string, number>>;
  stats(): Promise<Record<string, number>>;
  deleteProject(): Promise<{ ok: true }>;
  health(): Promise<{ ok: true }>;
}
