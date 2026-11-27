//! The server over real sockets: the API end to end, many clients at once, the token, the
//! connection cap, and a restart.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use transcriptdb::server::{q, start, ServerConfig};
use transcriptdb::synth::{self, SynthRequest};
use transcriptdb::{json, Options, SyncMode};

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-srv-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig {
        root: root.to_path_buf(),
        bind: "127.0.0.1:0".parse().unwrap(),
        store: Options { sync: SyncMode::Os, flush_bytes: 256 << 10, merge_at: 2, seg_bytes: 1 << 20, ..Options::default() },
        cache_bytes: 64 << 20,
        ..ServerConfig::default()
    }
}

/// One keep-alive connection.
struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
    token: Option<String>,
}

impl Client {
    fn new(addr: SocketAddr, token: Option<&str>) -> Client {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        Client { r: BufReader::new(s.try_clone().unwrap()), w: s, token: token.map(String::from) }
    }

    fn call(&mut self, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let auth = self.token.as_ref().map(|t| format!("Authorization: Bearer {t}\r\n")).unwrap_or_default();
        let head = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Length: {}\r\n\r\n", body.len());
        self.w.write_all(head.as_bytes()).unwrap();
        self.w.write_all(body).unwrap();
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        let status: u16 = line.split(' ').nth(1).unwrap_or_else(|| panic!("bad status line {line:?}")).parse().unwrap();
        let mut len = 0;
        loop {
            let mut h = String::new();
            self.r.read_line(&mut h).unwrap();
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some(v) = h.strip_prefix("Content-Length: ") {
                len = v.parse().unwrap();
            }
        }
        let mut b = vec![0; len];
        self.r.read_exact(&mut b).unwrap();
        (status, b)
    }

    fn ok(&mut self, method: &str, path: &str, body: &[u8]) -> Vec<u8> {
        let (s, b) = self.call(method, path, body);
        assert_eq!(s, 200, "{method} {path}: {}", String::from_utf8_lossy(&b));
        b
    }
}

/// The raw JSON text of member `name` of the object `b`.
fn member(b: &[u8], name: &str) -> Vec<u8> {
    let ms = json::members(b, &(0..b.len())).unwrap();
    let m = json::find_member(b, &ms, name).unwrap_or_else(|| panic!("no {name} in {}", String::from_utf8_lossy(b)));
    b[m.value.clone()].to_vec()
}

fn elements(b: &[u8]) -> Vec<Vec<u8>> {
    json::elements(b, &(0..b.len())).unwrap().0.into_iter().map(|e| b[e].to_vec()).collect()
}

fn envelope(q: &SynthRequest, response: &str) -> String {
    format!(r#"{{"id":{},"session":{},"agent":{},"time":{},"meta":{{"trace":{}}},"body":{},"response":{response}}}"#, transcriptdb::server::q(&q.id), transcriptdb::server::q(&q.session), transcriptdb::server::q(&q.agent), transcriptdb::server::q(&q.time), transcriptdb::server::q(&q.session), String::from_utf8_lossy(&q.body))
}

/// What the store gives back: the body with thinking and images blanked, without the
/// whitespace around it (a body sent as JSON is the value, not the text around it; send
/// `body_text` to keep that too).
fn want(q: &SynthRequest) -> Vec<u8> {
    let b = transcriptdb::split::blank_dropped(&q.body, &Default::default()).unwrap();
    String::from_utf8(b).unwrap().trim().as_bytes().to_vec()
}

#[test]
fn the_api_end_to_end() {
    let root = tmpdir("api");
    let srv = start(ServerConfig { token: Some("s3cret".into()), ..config(&root) }).unwrap();
    let mut c = Client::new(srv.addr, Some("s3cret"));
    assert_eq!(c.ok("GET", "/health", b"")[..9], br#"{"ok":tru"#[..]);
    // the token is needed for everything else
    let mut anon = Client::new(srv.addr, None);
    assert_eq!(anon.call("GET", "/v1/projects", b"").0, 401);
    assert_eq!(anon.call("GET", "/health", b"").0, 200);
    let mut wrong = Client::new(srv.addr, Some("s3cres"));
    assert_eq!(wrong.call("GET", "/v1/projects", b"").0, 401);

    let reqs: Vec<SynthRequest> = (0..3).flat_map(|s| synth::session(4, s, 12)).collect();
    let mut batch = Vec::new();
    for (i, q) in reqs.iter().enumerate() {
        batch.push(envelope(q, &format!(r#"{{"id":"resp{i}","usage":{{"output_tokens":{i}}}}}"#)));
    }
    batch.push(r#"{"id":"emb-1","session":"embeddings","body_text":"not json at all","response":{"data":[1,2]}}"#.into());
    batch.push(r#"{"session":"no-id","body":{}}"#.into());
    let out = c.ok("POST", "/v1/p/acme/put", format!("[{}]", batch.join(",")).as_bytes());
    let results = elements(&member(&out, "results"));
    assert_eq!(results.len(), reqs.len() + 2);
    assert!(results[..reqs.len() + 1].iter().all(|r| r.starts_with(br#"{"stored""#)));
    assert!(results[reqs.len() + 1].starts_with(br#"{"error""#));
    // again: each one a duplicate
    let out = c.ok("POST", "/v1/p/acme/put", format!(r#"{{"requests":[{}]}}"#, batch[..3].join(",")).as_bytes());
    assert!(elements(&member(&out, "results")).iter().all(|r| r.starts_with(br#"{"duplicate""#)));

    // one request back, exactly
    let got = c.ok("GET", &format!("/v1/p/acme/requests/{}", reqs[5].id), b"");
    assert_eq!(member(&got, "body"), want(&reqs[5]));
    assert_eq!(member(&got, "response"), br#"{"id":"resp5","usage":{"output_tokens":5}}"#);
    assert_eq!(member(&got, "meta"), format!(r#"{{"trace":{}}}"#, q(&reqs[5].session)).into_bytes());
    let raw = c.ok("GET", &format!("/v1/p/acme/requests/{}?part=body", reqs[5].id), b"");
    assert_eq!(raw, want(&reqs[5]));
    let emb = c.ok("GET", "/v1/p/acme/requests/emb-1", b"");
    assert_eq!(member(&emb, "body_text"), br#""not json at all""#);
    assert_eq!(c.call("GET", "/v1/p/acme/requests/nope", b"").0, 404);
    assert_eq!(c.call("GET", "/v1/p/nosuch/requests/x", b"").0, 404);
    assert_eq!(c.call("POST", "/v1/p/..bad/put", b"[]").0, 400);

    // many at once, a missing one as null
    let ids = format!(r#"{{"ids":[{},"nope",{}],"response":false}}"#, q(&reqs[0].id), q(&reqs[20].id));
    let many = elements(&member(&c.ok("POST", "/v1/p/acme/requests/get", ids.as_bytes()), "requests"));
    assert_eq!(member(&many[0], "body"), want(&reqs[0]));
    assert_eq!(many[1], b"null");
    assert!(json::find_member(&many[2], &json::members(&many[2], &(0..many[2].len())).unwrap(), "response").is_none());

    // a session's requests in time order, with bodies
    let sess = c.ok("GET", &format!("/v1/p/acme/sessions/{}?bodies=1", reqs[12].session), b"");
    let rs = elements(&member(&sess, "requests"));
    assert_eq!(rs.len(), 12);
    for (r, q) in rs.iter().zip(&reqs[12..24]) {
        assert_eq!(member(r, "id"), self::q(&q.id).into_bytes());
        assert_eq!(member(r, "body"), want(q));
    }

    // a query, typed
    let res = c.ok("POST", "/v1/p/acme/query", br#"{"q":"summarize n = count() by session | order by session asc"}"#);
    assert_eq!(member(&res, "columns"), br#"["session","n"]"#);
    let rows = elements(&member(&res, "rows"));
    assert_eq!(rows.len(), 3);
    assert!(elements(&rows[0])[1].iter().all(|c| c.is_ascii_digit()), "counts are numbers");
    let (s, e) = c.call("POST", "/v1/p/acme/query", b"summarize n = count( by");
    assert_eq!(s, 400, "{}", String::from_utf8_lossy(&e));

    // stats, delete a session, vacuum
    let st = c.ok("GET", "/v1/p/acme/stats", b"");
    assert_eq!(member(&st, "sessions"), b"4");
    let d = c.ok("POST", "/v1/p/acme/drop", format!(r#"{{"sessions":[{{"session":{}}}]}}"#, q(&reqs[0].session)).as_bytes());
    assert_eq!(member(&d, "sessions"), b"1");
    assert_eq!(c.call("GET", &format!("/v1/p/acme/requests/{}", reqs[0].id), b"").0, 404);
    let v = c.ok("POST", "/v1/p/acme/vacuum", b"");
    assert!(!member(&v, "segment_bytes_after").is_empty());
    assert_eq!(member(&c.ok("GET", &format!("/v1/p/acme/requests/{}", reqs[13].id), b""), "body"), want(&reqs[13]));
    let d = c.ok("POST", "/v1/p/acme/drop", br#"{"before":"2100-01-01T00:00:00Z"}"#);
    assert_eq!(member(&d, "sessions"), b"3");

    // projects, and deleting one
    c.ok("POST", "/v1/p/other/put", format!("[{}]", batch[0]).as_bytes());
    assert_eq!(c.ok("GET", "/v1/projects", b""), br#"["acme","other"]"#);
    c.ok("DELETE", "/v1/p/other", b"");
    assert_eq!(c.ok("GET", "/v1/projects", b""), br#"["acme"]"#);
    assert!(!root.join("other").exists());
    srv.stop(Duration::from_secs(5));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn many_clients_at_once_and_a_restart() {
    let root = tmpdir("many");
    let srv = start(config(&root)).unwrap();
    let addr = srv.addr;
    let acked: Arc<Mutex<Vec<SynthRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let checked = Arc::new(AtomicUsize::new(0));
    let writers_left = Arc::new(AtomicUsize::new(12));
    let mut threads = Vec::new();
    // 12 writers, each on its own connection and sessions, in batches of 4
    for w in 0..12u64 {
        let (acked, left) = (acked.clone(), writers_left.clone());
        threads.push(std::thread::spawn(move || {
            let mut c = Client::new(addr, None);
            let reqs: Vec<SynthRequest> = (0..2).flat_map(|k| synth::session(8, w * 10 + k, 16)).collect();
            for chunk in reqs.chunks(4) {
                let body = format!("[{}]", chunk.iter().map(|q| envelope(q, "null")).collect::<Vec<_>>().join(","));
                let out = c.ok("POST", "/v1/p/load/put", body.as_bytes());
                assert!(elements(&member(&out, "results")).iter().all(|r| r.starts_with(br#"{"stored""#)), "{}", String::from_utf8_lossy(&out));
                acked.lock().unwrap().extend(chunk.iter().cloned());
            }
            left.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    // 3 readers: whatever was acknowledged is there
    for _ in 0..3 {
        let (acked, checked, left) = (acked.clone(), checked.clone(), writers_left.clone());
        threads.push(std::thread::spawn(move || {
            let mut c = Client::new(addr, None);
            while left.load(Ordering::SeqCst) > 0 {
                let seen: Vec<SynthRequest> = acked.lock().unwrap().iter().rev().take(3).cloned().collect();
                for q in seen {
                    let got = c.ok("GET", &format!("/v1/p/load/requests/{}", q.id), b"");
                    assert_eq!(member(&got, "body"), want(&q));
                    checked.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    assert!(checked.load(Ordering::Relaxed) > 0);
    let all = acked.lock().unwrap().clone();
    assert_eq!(all.len(), 12 * 32);
    let gs = transcriptdb::store::group_stats();
    assert!(gs.largest > 4, "puts from several connections shared a sync: {gs:?}");
    srv.stop(Duration::from_secs(5));

    // a restart finds everything
    let srv = start(config(&root)).unwrap();
    let mut c = Client::new(srv.addr, None);
    for q in all.iter().step_by(7) {
        assert_eq!(member(&c.ok("GET", &format!("/v1/p/load/requests/{}", q.id), b""), "body"), want(q));
    }
    let st = c.ok("GET", "/v1/p/load/stats", b"");
    assert_eq!(member(&st, "requests"), (12 * 32).to_string().into_bytes());
    srv.stop(Duration::from_secs(5));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn connections_past_the_cap_are_turned_away() {
    let root = tmpdir("cap");
    let srv = start(ServerConfig { max_connections: 3, ..config(&root) }).unwrap();
    let mut held: Vec<Client> = (0..3).map(|_| Client::new(srv.addr, None)).collect();
    for c in &mut held {
        c.ok("GET", "/health", b"");
    }
    let mut extra = Client::new(srv.addr, None);
    assert_eq!(extra.call("GET", "/health", b"").0, 503);
    drop(held);
    std::thread::sleep(Duration::from_millis(100));
    let mut again = Client::new(srv.addr, None);
    assert_eq!(again.call("GET", "/health", b"").0, 200);
    // a server beyond loopback without a token does not start
    assert!(start(ServerConfig { bind: "0.0.0.0:0".parse().unwrap(), ..config(&root) }).is_err());
    srv.stop(Duration::from_secs(5));
    let _ = std::fs::remove_dir_all(&root);
}
