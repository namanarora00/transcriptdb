//! The network server: many clients, many projects, one process.
//!
//! It speaks HTTP/1.1 with keep-alive and JSON, so any language's HTTP client is a
//! TranscriptDB client (a fetch-based Node client is in `clients/node`). The shape follows
//! database servers:
//!
//! - **Connections.** Each connection gets a thread of its own, as each PostgreSQL
//!   connection gets a process: a connection's requests run one after another, and many
//!   connections run at once. Past `max_connections` a new connection is answered 503 and
//!   closed; an idle connection is closed after `idle_timeout`.
//! - **Projects.** Each project is a store of its own (a directory under the root), opened
//!   on first use and kept open: one project's data never mixes with another's, deleting a
//!   project deletes its directory, and retention runs per project. Every store shares one
//!   cache budget (the buffer pool) and the process-wide cap on open files.
//! - **Writes** go through the store's pipeline ([`crate::store`]): the connection's thread
//!   splits and hashes the bodies it was sent, the store's one writing thread puts them and
//!   makes each group of puts from all connections durable with one sync, and the
//!   connection is answered once its puts are durable. Requests to different sessions never
//!   wait on each other beyond that; requests to one session are applied in arrival order.
//! - **Reads** run on the connection's thread against a snapshot, so a long query never
//!   holds up writes, and a client always reads what it was told is stored.
//! - **Security.** With a token set, every request but `/health` must carry
//!   `Authorization: Bearer <token>`; without one, the server only listens on loopback.
//! - **Shutdown.** SIGINT or SIGTERM stops new connections, lets the requests at work
//!   finish, and closes every store (everything acknowledged is already durable).
//!
//! The API, all JSON (bodies, responses and metadata are embedded as the JSON they are, or
//! as `body_text` / `response_text` strings when they are not JSON):
//!
//! | Method and path | Does |
//! |---|---|
//! | `GET /health` | `{"ok":true}` |
//! | `GET /v1/projects` | project names |
//! | `POST /v1/p/{project}/put` | store requests: `[{"id","session","agent"?,"time"?,"meta"?,"body" or "body_text","response" or "response_text"?}]`; answers `{"results":[{"stored":ref} or {"duplicate":ref} or {"error":msg}]}` once durable |
//! | `GET /v1/p/{project}/requests/{id}` | one request with its body, response and metadata |
//! | `POST /v1/p/{project}/requests/get` | many: `{"ids":[...],"body"?:bool,"response"?:bool}` |
//! | `GET /v1/p/{project}/sessions/{session}?agent=&bodies=1` | a session's requests in time order |
//! | `POST /v1/p/{project}/query` | run a query (the text, or `{"q","limit"?}`): `{"columns","rows","rows_total","ms"}` |
//! | `POST /v1/p/{project}/drop` | delete sessions: `{"sessions":[{"session","agent"?}]}` or `{"before": ISO time or ms}` |
//! | `POST /v1/p/{project}/vacuum` | remove deleted sessions from every file |
//! | `POST /v1/p/{project}/rules` | define a rule column (the `.create column` text); `DELETE /v1/p/{project}/rules/{name}` drops one |
//! | `GET /v1/p/{project}/stats` | sizes and counts |
//! | `DELETE /v1/p/{project}` | delete the project and its directory |

use crate::codec::Error;
use crate::db::{Db, Options, PutOutcome};
use crate::json;
use crate::query::value::Value;
use crate::store::{Request, Store, StoreOptions};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Directory holding one directory per project.
    pub root: PathBuf,
    pub bind: SocketAddr,
    /// Required as `Authorization: Bearer <token>` when set.
    pub token: Option<String>,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    /// Largest request body accepted.
    pub max_body: usize,
    /// Cache budget shared by every project's store.
    pub cache_bytes: usize,
    /// Threads one query may use.
    pub query_threads: usize,
    /// Store options for every project (`cache_bytes` and `cache_pool` are set from the
    /// server's).
    pub store: Options,
    pub pipeline: StoreOptions,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            root: PathBuf::from("data"),
            bind: "127.0.0.1:8378".parse().unwrap(),
            token: None,
            max_connections: 256,
            idle_timeout: Duration::from_secs(60),
            max_body: 256 << 20,
            cache_bytes: 1 << 30,
            query_threads: std::thread::available_parallelism().map_or(4, |n| n.get()).min(8),
            store: Options::default(),
            pipeline: StoreOptions::default(),
        }
    }
}

struct Shared {
    cfg: ServerConfig,
    stores: Mutex<HashMap<String, Arc<Store>>>,
    pool: Arc<crate::cache::Pool>,
    conns: AtomicUsize,
    stop: AtomicBool,
    started: Instant,
    requests: AtomicUsize,
}

/// A server running on threads of its own.
pub struct Running {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
    accept: Option<std::thread::JoinHandle<()>>,
}

impl Running {
    /// Stop taking connections, wait for those at work to finish (at most `wait`), and
    /// close every store.
    pub fn stop(mut self, wait: Duration) {
        self.shutdown(wait);
    }

    fn shutdown(&mut self, wait: Duration) {
        self.shared.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
        let t = Instant::now();
        while self.shared.conns.load(Ordering::SeqCst) > 0 && t.elapsed() < wait {
            std::thread::sleep(Duration::from_millis(10));
        }
        let stores: Vec<Arc<Store>> = self.shared.stores.lock().unwrap().drain().map(|x| x.1).collect();
        for s in stores {
            s.close();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if self.accept.is_some() {
            self.shutdown(Duration::from_secs(10));
        }
    }
}

/// Start serving on threads of its own.
pub fn start(cfg: ServerConfig) -> std::io::Result<Running> {
    if cfg.token.is_none() && !cfg.bind.ip().is_loopback() {
        return Err(std::io::Error::other("a server listening beyond loopback needs a token"));
    }
    std::fs::create_dir_all(&cfg.root)?;
    let listener = TcpListener::bind(cfg.bind)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let pool = crate::cache::Pool::new(cfg.cache_bytes);
    let shared = Arc::new(Shared { cfg, stores: Mutex::new(HashMap::new()), pool, conns: AtomicUsize::new(0), stop: AtomicBool::new(false), started: Instant::now(), requests: AtomicUsize::new(0) });
    let sh = shared.clone();
    let accept = std::thread::Builder::new().name("tdb-accept".into()).spawn(move || accept_loop(listener, sh))?;
    Ok(Running { addr, shared, accept: Some(accept) })
}

static SIGNALLED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    SIGNALLED.store(true, Ordering::SeqCst);
}

/// Serve until SIGINT or SIGTERM, then shut down cleanly.
pub fn serve(cfg: ServerConfig) -> std::io::Result<()> {
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
    }
    let running = start(cfg)?;
    eprintln!("tdb server listening on http://{}", running.addr);
    while !SIGNALLED.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("shutting down: finishing requests at work, closing stores");
    running.stop(Duration::from_secs(30));
    eprintln!("stopped");
    Ok(())
}

fn accept_loop(listener: TcpListener, sh: Arc<Shared>) {
    while !sh.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_nodelay(true);
                if sh.conns.load(Ordering::SeqCst) >= sh.cfg.max_connections {
                    let mut s = stream;
                    let _ = write_response(&mut s, 503, "application/json", br#"{"error":"too many connections"}"#, false);
                    continue;
                }
                sh.conns.fetch_add(1, Ordering::SeqCst);
                let sh2 = sh.clone();
                let spawned = std::thread::Builder::new().name("tdb-conn".into()).spawn(move || {
                    connection(stream, &sh2);
                    sh2.conns.fetch_sub(1, Ordering::SeqCst);
                });
                if spawned.is_err() {
                    sh.conns.fetch_sub(1, Ordering::SeqCst);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(5)),
            Err(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }
}

/// One parsed HTTP request.
struct HttpReq {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpReq {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|h| h.0.eq_ignore_ascii_case(name)).map(|h| h.1.as_str())
    }
    fn param(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|q| q.0 == name).map(|q| q.1.as_str())
    }
}

/// An answer: status, content type, body.
struct Answer(u16, &'static str, Vec<u8>);

fn ok_json(s: String) -> Answer {
    Answer(200, "application/json", s.into_bytes())
}

fn err(status: u16, msg: &str) -> Answer {
    Answer(status, "application/json", format!(r#"{{"error":{}}}"#, q(msg)).into_bytes())
}

fn connection(stream: TcpStream, sh: &Shared) {
    let mut w = match stream.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut r = BufReader::with_capacity(64 << 10, stream);
    loop {
        // wait for the next request in short slices, so a shutdown does not wait for idle
        // connections; then read it with the full timeout
        let _ = r.get_ref().set_read_timeout(Some(Duration::from_millis(100)));
        let idle = Instant::now();
        loop {
            if sh.stop.load(Ordering::SeqCst) {
                return;
            }
            match r.fill_buf() {
                Ok([]) => return,
                Ok(_) => break,
                Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
                    if idle.elapsed() >= sh.cfg.idle_timeout {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        let _ = r.get_ref().set_read_timeout(Some(sh.cfg.idle_timeout));
        let req = match read_request(&mut r, sh.cfg.max_body) {
            Ok(Some(req)) => req,
            Ok(None) => return,
            Err(Bad(status, msg)) => {
                let _ = write_response(&mut w, status, "application/json", format!(r#"{{"error":{}}}"#, q(&msg)).as_bytes(), false);
                return;
            }
        };
        sh.requests.fetch_add(1, Ordering::Relaxed);
        let keep = !req.header("connection").is_some_and(|c| c.eq_ignore_ascii_case("close"));
        let Answer(status, ct, body) = match authorized(&req, sh) {
            true => route(&req, sh).unwrap_or_else(|e| e),
            false => err(401, "missing or wrong token"),
        };
        if write_response(&mut w, status, ct, &body, keep).is_err() || !keep {
            return;
        }
    }
}

fn authorized(req: &HttpReq, sh: &Shared) -> bool {
    let Some(token) = &sh.cfg.token else { return true };
    if req.path == "/health" {
        return true;
    }
    let Some(h) = req.header("authorization") else { return false };
    let Some(given) = h.strip_prefix("Bearer ") else { return false };
    // in constant time
    let (a, b) = (given.as_bytes(), token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

struct Bad(u16, String);

fn read_request(r: &mut BufReader<TcpStream>, max_body: usize) -> Result<Option<HttpReq>, Bad> {
    let mut line = String::new();
    match r.read_line(&mut line) {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(_) => return Ok(None),
    }
    let line = line.trim_end();
    if line.is_empty() {
        return Ok(None);
    }
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Bad(400, "bad request line".into()));
    };
    let mut headers = Vec::new();
    let mut header_bytes = 0;
    loop {
        let mut h = String::new();
        match r.read_line(&mut h) {
            Ok(0) | Err(_) => return Ok(None),
            Ok(n) => header_bytes += n,
        }
        if header_bytes > 64 << 10 {
            return Err(Bad(431, "headers too large".into()));
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let get = |n: &str| headers.iter().find(|h: &&(String, String)| h.0.eq_ignore_ascii_case(n)).map(|h| h.1.clone());
    if get("transfer-encoding").is_some_and(|t| t.eq_ignore_ascii_case("chunked")) {
        return Err(Bad(411, "send a Content-Length".into()));
    }
    let len: usize = match get("content-length") {
        Some(v) => v.parse().map_err(|_| Bad(400, "bad Content-Length".into()))?,
        None => 0,
    };
    if len > max_body {
        return Err(Bad(413, format!("body over {max_body} bytes")));
    }
    if len > 0 && get("expect").is_some_and(|e| e.eq_ignore_ascii_case("100-continue")) {
        let _ = r.get_ref().write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).map_err(|_| Bad(400, "body shorter than its Content-Length".into()))?;
    if get("content-encoding").is_some_and(|e| e.eq_ignore_ascii_case("zstd")) {
        body = zstd::stream::decode_all(&body[..]).map_err(|_| Bad(400, "bad zstd body".into()))?;
        if body.len() > max_body {
            return Err(Bad(413, format!("body over {max_body} bytes")));
        }
    }
    let (path, qs) = target.split_once('?').unwrap_or((target, ""));
    let query = qs.split('&').filter(|x| !x.is_empty()).map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (pct_decode(k), pct_decode(v))
    });
    Ok(Some(HttpReq { method: method.to_string(), path: path.to_string(), query: query.collect(), headers, body }))
}

fn write_response(w: &mut TcpStream, status: u16, ct: &str, body: &[u8], keep: bool) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let head = format!("HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n", body.len(), if keep { "keep-alive" } else { "close" });
    w.write_all(head.as_bytes())?;
    w.write_all(body)?;
    w.flush()
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => match std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok()) {
                Some(v) => {
                    out.push(v);
                    i += 3;
                }
                None => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A JSON string literal.
pub fn q(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn valid_project(p: &str) -> bool {
    !p.is_empty() && p.len() <= 128 && !p.starts_with('.') && p.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
}

impl Shared {
    /// The project's store; with `create`, made if it does not exist.
    fn store(&self, project: &str, create: bool) -> Result<Arc<Store>, Answer> {
        if !valid_project(project) {
            return Err(err(400, "project names are 1-128 of A-Z a-z 0-9 _ - . and do not start with ."));
        }
        let mut g = self.stores.lock().unwrap();
        if let Some(s) = g.get(project) {
            return Ok(s.clone());
        }
        let dir = self.cfg.root.join(project);
        if !create && !dir.join("MANIFEST").exists() {
            return Err(err(404, "no such project"));
        }
        let opts = Options { cache_pool: Some(self.pool.clone()), cache_bytes: self.cfg.cache_bytes, ..self.cfg.store.clone() };
        let s = Arc::new(Store::open(&dir, opts, self.cfg.pipeline.clone()).map_err(|e| err(500, &format!("opening the project: {e}")))?);
        g.insert(project.to_string(), s.clone());
        Ok(s)
    }
}

fn route(req: &HttpReq, sh: &Shared) -> Result<Answer, Answer> {
    let segs: Vec<String> = req.path.split('/').filter(|s| !s.is_empty()).map(pct_decode).collect();
    let segs: Vec<&str> = segs.iter().map(|s| s.as_str()).collect();
    let m = req.method.as_str();
    match (m, segs.as_slice()) {
        ("GET", ["health"]) => Ok(ok_json(format!(r#"{{"ok":true,"uptime_s":{},"connections":{},"requests":{}}}"#, sh.started.elapsed().as_secs(), sh.conns.load(Ordering::Relaxed), sh.requests.load(Ordering::Relaxed)))),
        ("GET", ["v1", "projects"]) => {
            let mut names: Vec<String> = std::fs::read_dir(&sh.cfg.root).map_err(|e| err(500, &e.to_string()))?.filter_map(|e| e.ok()).filter(|e| e.path().join("MANIFEST").exists()).map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| valid_project(n)).collect();
            names.sort();
            Ok(ok_json(format!("[{}]", names.iter().map(|n| q(n)).collect::<Vec<_>>().join(","))))
        }
        ("POST", ["v1", "p", p, "put"]) => put(req, &*sh.store(p, true)?),
        ("GET", ["v1", "p", p, "requests", id]) => get_one(&*sh.store(p, false)?, id, req),
        ("POST", ["v1", "p", p, "requests", "get"]) => get_many(&*sh.store(p, false)?, req),
        ("GET", ["v1", "p", p, "sessions", session]) => session_reqs(&*sh.store(p, false)?, session, req),
        ("POST", ["v1", "p", p, "query"]) => query(&*sh.store(p, false)?, req, sh.cfg.query_threads),
        ("POST", ["v1", "p", p, "drop"]) => drop_sessions(&*sh.store(p, false)?, req),
        ("POST", ["v1", "p", p, "vacuum"]) => vacuum(&*sh.store(p, false)?),
        ("POST", ["v1", "p", p, "rules"]) => {
            let def = String::from_utf8_lossy(&req.body).into_owned();
            sh.store(p, false)?.exec(move |db| db.define_rule(&def)).map_err(|e| err(500, &e.to_string()))?.map_err(|e| err(400, &e.to_string()))?;
            Ok(ok_json(r#"{"ok":true}"#.into()))
        }
        ("DELETE", ["v1", "p", p, "rules", name]) => {
            let name = name.to_string();
            sh.store(p, false)?.exec(move |db| db.drop_rule(&name)).map_err(|e| err(500, &e.to_string()))?.map_err(|e| err(400, &e.to_string()))?;
            Ok(ok_json(r#"{"ok":true}"#.into()))
        }
        ("GET", ["v1", "p", p, "stats"]) => stats(&*sh.store(p, false)?),
        ("DELETE", ["v1", "p", p]) => delete_project(sh, p),
        (_, ["health"] | ["v1", ..]) => Err(err(405, "no such method for this path")),
        _ => Err(err(404, "no such path")),
    }
}

/// A string member, unescaped; numbers are taken as their text.
fn str_member(b: &[u8], ms: &[json::Member], name: &str) -> Option<String> {
    let m = json::find_member(b, ms, name)?;
    match b[m.value.start] {
        b'"' => Some(crate::text::unescape(json::inner(b, &m.value))),
        b'n' => None,
        _ => Some(String::from_utf8_lossy(&b[m.value.clone()]).into_owned()),
    }
}

/// A member as raw JSON bytes (`name`), or a string's content as bytes (`name_text`).
fn raw_member(b: &[u8], ms: &[json::Member], name: &str) -> Option<Vec<u8>> {
    if let Some(m) = json::find_member(b, ms, name)
        && &b[m.value.clone()] != b"null"
    {
        return Some(b[m.value.clone()].to_vec());
    }
    let t = json::find_member(b, ms, &format!("{name}_text"))?;
    (b[t.value.start] == b'"').then(|| crate::text::unescape(json::inner(b, &t.value)).into_bytes())
}

fn parse_doc(b: &[u8]) -> Result<json::Span, Answer> {
    let start = json::skip_ws(b, 0);
    let end = json::scan_value(b, start).map_err(|e| err(400, &format!("bad JSON: {e}")))?;
    if json::skip_ws(b, end) != b.len() {
        return Err(err(400, "bad JSON: trailing bytes"));
    }
    Ok(start..end)
}

fn put(req: &HttpReq, store: &Store) -> Result<Answer, Answer> {
    let b = &req.body;
    let doc = parse_doc(b)?;
    // a list of requests, or {"requests": [...]}
    let list = if b[doc.start] == b'[' {
        doc
    } else {
        let ms = json::members(b, &doc).map_err(|e| err(400, &e.to_string()))?;
        json::find_member(b, &ms, "requests").ok_or_else(|| err(400, "send a list of requests"))?.value.clone()
    };
    let (els, _) = json::elements(b, &list).map_err(|e| err(400, &e.to_string()))?;
    let mut reqs = Vec::with_capacity(els.len());
    let mut bad: Vec<Option<String>> = Vec::with_capacity(els.len());
    for el in &els {
        let one = (|| -> Result<Request, String> {
            if b[el.start] != b'{' {
                return Err("a request is an object".into());
            }
            let ms = json::members(b, el).map_err(|e| e.to_string())?;
            let id = str_member(b, &ms, "id").filter(|s| !s.is_empty()).ok_or("a request needs an id")?;
            let session = str_member(b, &ms, "session").filter(|s| !s.is_empty()).unwrap_or_else(|| id.clone());
            let body = raw_member(b, &ms, "body").ok_or("a request needs a body (or body_text)")?;
            Ok(Request {
                session,
                agent: str_member(b, &ms, "agent").unwrap_or_default(),
                time: str_member(b, &ms, "time").unwrap_or_default(),
                meta: raw_member(b, &ms, "meta").unwrap_or_default(),
                response: raw_member(b, &ms, "response").unwrap_or_default(),
                body,
                id,
            })
        })();
        match one {
            Ok(r) => {
                reqs.push(r);
                bad.push(None);
            }
            Err(e) => bad.push(Some(e)),
        }
    }
    let mut outs = store.put(reqs).into_iter();
    let mut parts = Vec::with_capacity(bad.len());
    for b in bad {
        parts.push(match b {
            Some(e) => format!(r#"{{"error":{}}}"#, q(&e)),
            None => match outs.next() {
                Some(Ok(PutOutcome::Stored(r))) => format!(r#"{{"stored":"{r:x}"}}"#),
                Some(Ok(PutOutcome::Duplicate(r))) => format!(r#"{{"duplicate":"{r:x}"}}"#),
                Some(Err(e)) => format!(r#"{{"error":{}}}"#, q(&e.to_string())),
                None => r#"{"error":"no answer"}"#.to_string(),
            },
        });
    }
    if let Some(why) = store.poisoned() {
        return Ok(Answer(503, "application/json", format!(r#"{{"error":{},"results":[{}]}}"#, q(&format!("the store takes no writes: {why}")), parts.join(",")).into_bytes()));
    }
    Ok(ok_json(format!(r#"{{"results":[{}]}}"#, parts.join(","))))
}

/// Bytes as a JSON member: as the JSON they are, else as a `_text` string.
fn embed(out: &mut String, name: &str, b: Option<&[u8]>) {
    match b {
        None => out.push_str(&format!(r#""{name}":null"#)),
        Some(b) => {
            let s = json::skip_ws(b, 0);
            let is_json = !b.is_empty() && json::scan_value(b, s).is_ok_and(|e| json::skip_ws(b, e) == b.len());
            if is_json && let Ok(t) = std::str::from_utf8(b) {
                out.push_str(&format!(r#""{name}":{t}"#));
            } else {
                out.push_str(&format!(r#""{name}_text":{}"#, q(&String::from_utf8_lossy(b))));
            }
        }
    }
}

/// One request as JSON.
fn request_json(db: &Db, ri: crate::db::ReqId, body: bool, response: bool) -> Result<String, Error> {
    let r = db.request(ri)?;
    let (s, _) = crate::db::req_parts(ri);
    let (session, agent) = db.session_key(s);
    let mut o = format!(r#"{{"id":{},"session":{},"agent":{},"time":{},"ref":"{ri:x}","#, q(&r.id), q(&session), q(&agent), q(&r.time));
    embed(&mut o, "meta", (!r.meta.is_empty()).then_some(&r.meta[..]));
    if body {
        o.push(',');
        embed(&mut o, "body", Some(&db.rebuild(ri)?));
    }
    if response {
        o.push(',');
        embed(&mut o, "response", db.response(ri)?.as_deref());
    }
    o.push('}');
    Ok(o)
}

fn flag(req: &HttpReq, name: &str, default: bool) -> bool {
    req.param(name).map_or(default, |v| matches!(v, "1" | "true" | "yes"))
}

fn get_one(store: &Store, id: &str, req: &HttpReq) -> Result<Answer, Answer> {
    let db = store.snapshot().map_err(|e| err(500, &e.to_string()))?;
    let ri = db.request_index(id).ok_or_else(|| err(404, "no such request"))?;
    if req.param("part") == Some("body") {
        return Ok(Answer(200, "application/octet-stream", db.rebuild(ri).map_err(|e| err(500, &e.to_string()))?));
    }
    Ok(ok_json(request_json(&db, ri, flag(req, "body", true), flag(req, "response", true)).map_err(|e| err(500, &e.to_string()))?))
}

fn get_many(store: &Store, req: &HttpReq) -> Result<Answer, Answer> {
    let b = &req.body;
    let doc = parse_doc(b)?;
    let ms = json::members(b, &doc).map_err(|e| err(400, &e.to_string()))?;
    let ids = json::find_member(b, &ms, "ids").ok_or_else(|| err(400, "send {\"ids\": [...]}"))?;
    let (els, _) = json::elements(b, &ids.value).map_err(|e| err(400, &e.to_string()))?;
    let want = |n: &str| json::find_member(b, &ms, n).is_none_or(|m| &b[m.value.clone()] != b"false");
    let (body, response) = (want("body"), want("response"));
    let db = store.snapshot().map_err(|e| err(500, &e.to_string()))?;
    let mut out = Vec::with_capacity(els.len());
    for el in els {
        let id = crate::text::unescape(json::inner(b, &el));
        out.push(match db.request_index(&id) {
            Some(ri) => request_json(&db, ri, body, response).map_err(|e| err(500, &e.to_string()))?,
            None => "null".into(),
        });
    }
    Ok(ok_json(format!(r#"{{"requests":[{}]}}"#, out.join(","))))
}

fn session_reqs(store: &Store, session: &str, req: &HttpReq) -> Result<Answer, Answer> {
    let db = store.snapshot().map_err(|e| err(500, &e.to_string()))?;
    let agent = req.param("agent").unwrap_or("");
    let s = db.session_index(session, agent).map_err(|e| err(500, &e.to_string()))?.ok_or_else(|| err(404, "no such session"))?;
    let core = db.core(s).map_err(|e| err(500, &e.to_string()))?;
    let mut order: Vec<(i64, &str, u32)> = core.requests().iter().enumerate().map(|(k, r)| (crate::steps::parse_time_ms(&r.time), &*r.id, k as u32)).collect();
    order.sort();
    let bodies = flag(req, "bodies", false);
    let mut out = Vec::with_capacity(order.len());
    for (_, _, k) in order {
        out.push(request_json(&db, crate::db::req_id(s, k), bodies, bodies).map_err(|e| err(500, &e.to_string()))?);
    }
    Ok(ok_json(format!(r#"{{"session":{},"agent":{},"requests":[{}]}}"#, q(session), q(agent), out.join(","))))
}

fn value_json(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_finite() => f.to_string(),
        Value::Float(_) => "null".into(),
        Value::Str(s) => q(s),
        Value::Time(_) => q(&v.to_string()),
        Value::Dur(d) => d.to_string(),
    }
}

fn query(store: &Store, req: &HttpReq, threads: usize) -> Result<Answer, Answer> {
    let b = &req.body;
    let (text, limit) = if b.get(json::skip_ws(b, 0)) == Some(&b'{') {
        let doc = parse_doc(b)?;
        let ms = json::members(b, &doc).map_err(|e| err(400, &e.to_string()))?;
        let text = str_member(b, &ms, "q").ok_or_else(|| err(400, "send {\"q\": \"...\"} or the query as text"))?;
        (text, str_member(b, &ms, "limit").and_then(|l| l.parse().ok()).unwrap_or(1000usize))
    } else {
        (String::from_utf8_lossy(b).into_owned(), req.param("limit").and_then(|l| l.parse().ok()).unwrap_or(1000usize))
    };
    let db = store.snapshot().map_err(|e| err(500, &e.to_string()))?;
    let t = Instant::now();
    let run = crate::query::run(&db, &text, threads).map_err(|e| err(400, &e.show(&text)))?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let (heads, rows) = crate::query::exec::render_values(&db, &run.table, limit).map_err(|e| err(500, &e.show(&text)))?;
    let rows: Vec<String> = rows.iter().map(|r| format!("[{}]", r.iter().map(value_json).collect::<Vec<_>>().join(","))).collect();
    Ok(ok_json(format!(r#"{{"columns":[{}],"rows":[{}],"rows_total":{},"ms":{ms:.3}}}"#, heads.iter().map(|h| q(h)).collect::<Vec<_>>().join(","), rows.join(","), run.table.rows.len())))
}

fn drop_sessions(store: &Store, req: &HttpReq) -> Result<Answer, Answer> {
    let b = &req.body;
    let doc = parse_doc(b)?;
    let ms = json::members(b, &doc).map_err(|e| err(400, &e.to_string()))?;
    let rep = if let Some(before) = str_member(b, &ms, "before") {
        let cutoff = match before.parse::<i64>() {
            Ok(n) => n,
            Err(_) => match crate::steps::parse_time_ms(&before) {
                0 => return Err(err(400, "before: an ISO time or ms since the epoch")),
                t => t,
            },
        };
        store.exec(move |db| db.drop_before(cutoff))
    } else if let Some(list) = json::find_member(b, &ms, "sessions") {
        let (els, _) = json::elements(b, &list.value).map_err(|e| err(400, &e.to_string()))?;
        let mut keys = Vec::with_capacity(els.len());
        for el in els {
            if b[el.start] == b'"' {
                keys.push((crate::text::unescape(json::inner(b, &el)), String::new()));
                continue;
            }
            let em = json::members(b, &el).map_err(|e| err(400, &e.to_string()))?;
            let s = str_member(b, &em, "session").ok_or_else(|| err(400, "each session needs a session id"))?;
            keys.push((s, str_member(b, &em, "agent").unwrap_or_default()));
        }
        store.exec(move |db| {
            let mut ss = Vec::new();
            for (s, a) in &keys {
                if let Some(x) = db.session_index(s, a)? {
                    ss.push(x);
                }
            }
            db.drop_sessions(&ss)
        })
    } else {
        return Err(err(400, "send {\"sessions\": [...]} or {\"before\": time}"));
    };
    let rep = rep.map_err(|e| err(500, &e.to_string()))?.map_err(|e| err(500, &e.to_string()))?;
    Ok(ok_json(format!(r#"{{"sessions":{},"requests":{},"dead_bytes":{}}}"#, rep.sessions, rep.requests, rep.dead_bytes)))
}

fn vacuum(store: &Store) -> Result<Answer, Answer> {
    let r = store.exec(|db| db.vacuum()).map_err(|e| err(500, &e.to_string()))?.map_err(|e| err(500, &e.to_string()))?;
    Ok(ok_json(format!(
        r#"{{"segment_bytes_before":{},"segment_bytes_after":{},"blob_bytes_before":{},"blob_bytes_after":{},"pieces_removed":{},"opaques_removed":{},"ms":{}}}"#,
        r.segment_bytes_before, r.segment_bytes_after, r.blob_bytes_before, r.blob_bytes_after, r.pieces_removed, r.opaques_removed, r.ms
    )))
}

fn stats(store: &Store) -> Result<Answer, Answer> {
    let db = store.snapshot().map_err(|e| err(500, &e.to_string()))?;
    let st = db.stats();
    Ok(ok_json(format!(
        r#"{{"sessions":{},"sessions_deleted":{},"requests":{},"steps":{},"store_bytes":{},"segment_bytes":{},"blob_bytes":{},"index_bytes":{},"unflushed_bytes":{},"segments":{}}}"#,
        db.session_count() as u64 - db.dropped_count(),
        db.dropped_count(),
        st.requests,
        st.steps,
        st.store_bytes(),
        st.seg_file_bytes,
        st.blob_file_bytes,
        st.index_file_bytes,
        st.mem_bytes,
        st.segments
    )))
}

fn delete_project(sh: &Shared, p: &str) -> Result<Answer, Answer> {
    if !valid_project(p) {
        return Err(err(400, "bad project name"));
    }
    let dir = sh.cfg.root.join(p);
    let open = sh.stores.lock().unwrap().remove(p);
    if let Some(s) = open {
        s.close();
    }
    if !dir.exists() {
        return Err(err(404, "no such project"));
    }
    crate::block::wait_removed();
    std::fs::remove_dir_all(&dir).map_err(|e| err(500, &format!("removing the project: {e}")))?;
    Ok(ok_json(r#"{"ok":true}"#.into()))
}
