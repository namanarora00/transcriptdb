//! Reading Claude Code's local logs (`~/.claude/projects/**/*.jsonl`) into request bodies.
//!
//! Newer Claude Code versions log every model call as an `api-request` line: the
//! request parameters, `base`/`keep`/`tail` (the message list as "the first `keep`
//! messages of request `base`, then these hashes"), the `shapeHash` of the system
//! blocks and tools, the cache-marker positions, and `requestDigest`, the sha256 of
//! the body with keys sorted. Messages are logged as `api-request-blob` lines, except
//! those that can be formed from the transcript's own user and assistant lines. A
//! message's hash is the sha256 of its JSON with keys sorted.
//!
//! What this module had to learn from the logs (checked against `requestDigest`):
//! * transcript messages come in four forms: a user line as is; one assistant line;
//!   all assistant lines that share a message id, blocks joined; and a run of
//!   consecutive user lines merged into one message, blocks joined;
//! * a message whose content is a string is sent as one text block,
//!   `[{"type":"text","text":...}]`; transcript messages are hashed in the string form,
//!   but some blob lines are hashed in the block form while logging the string form.
//!
//! The body built here is the canonical form (keys sorted, no whitespace), which is
//! the form `requestDigest` covers, so a match proves the body is exactly what
//! Claude Code sent, up to key order. Real logs are only ever read at run time.

use crate::codec::{hex, unhex32};
use crate::json::{self, Node, Span};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A JSON value being built, children already canonical.
enum V {
    Raw(Vec<u8>),
    Obj(Vec<(Vec<u8>, V)>),
    Arr(Vec<V>),
}

impl V {
    fn from_node(b: &[u8], n: &Node) -> V {
        match n {
            Node::Tok(s) => V::Raw(b[s.clone()].to_vec()),
            Node::Arr(items) => V::Arr(items.iter().map(|x| V::from_node(b, x)).collect()),
            Node::Obj(ms) => V::Obj(ms.iter().map(|(k, v)| (b[k.clone()].to_vec(), V::from_node(b, v))).collect()),
        }
    }

    fn write(&self, out: &mut Vec<u8>) {
        match self {
            V::Raw(r) => out.extend_from_slice(r),
            V::Arr(items) => {
                out.push(b'[');
                for (i, x) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    x.write(out);
                }
                out.push(b']');
            }
            V::Obj(ms) => {
                let mut idx: Vec<usize> = (0..ms.len()).collect();
                idx.sort_by(|&a, &b| cmp_key_tokens(&ms[a].0, &ms[b].0));
                out.push(b'{');
                for (i, &k) in idx.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    out.extend_from_slice(&ms[k].0);
                    out.push(b':');
                    ms[k].1.write(out);
                }
                out.push(b'}');
            }
        }
    }

    fn set(&mut self, key: &[u8], v: V) {
        if let V::Obj(ms) = self {
            if let Some(m) = ms.iter_mut().find(|m| m.0 == key) {
                m.1 = v;
            } else {
                ms.push((key.to_vec(), v));
            }
        }
    }
}

fn cmp_key_tokens(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let mut buf = Vec::with_capacity(a.len() + b.len());
    buf.extend_from_slice(a);
    buf.extend_from_slice(b);
    json::cmp_keys(&buf, &(0..a.len()), &(a.len()..a.len() + b.len()))
}

fn canon(b: &[u8], span: &Span) -> Option<Vec<u8>> {
    json::canonical(&b[span.clone()]).ok()
}

fn sha(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

fn str_of(b: &[u8], ms: &[json::Member], key: &str) -> Option<String> {
    let m = json::find_member(b, ms, key)?;
    if b[m.value.start] != b'"' {
        return None;
    }
    // ids, timestamps and hashes never carry escapes
    std::str::from_utf8(json::inner(b, &m.value)).ok().map(|s| s.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Problem {
    /// A message hash with no blob line and no matching transcript form.
    MissingMessage,
    MissingShape,
    MissingBase,
    /// A cache-marker path that does not fit the message list.
    BadMarker,
    BadLine,
}

#[derive(Debug, Clone)]
pub struct LogRequest {
    pub session: String,
    pub agent: String,
    pub id: String,
    pub time: String,
    pub meta: Vec<u8>,
    /// Claude Code's `requestDigest`, if present.
    pub digest: Option<[u8; 32]>,
    pub body: Result<Vec<u8>, Problem>,
}

impl LogRequest {
    pub fn verified(&self) -> bool {
        match (&self.body, &self.digest) {
            (Ok(b), Some(d)) => &sha(b) == d,
            _ => false,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct FileReport {
    pub requests: usize,
    pub blobs: usize,
    pub blob_hash_mismatch: usize,
    pub blob_hashed_as_blocks: usize,
    pub from_transcript: usize,
}

struct ReqLine {
    session: String,
    agent: String,
    id: String,
    time: String,
    meta: Vec<u8>,
    shape: Option<[u8; 32]>,
    params: Span,
    base: Option<String>,
    keep: usize,
    tail: Vec<[u8; 32]>,
    markers: Vec<(Vec<MarkerStep>, Vec<u8>)>,
    digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone)]
enum MarkerStep {
    Key(Vec<u8>),
    Index(usize),
}

fn parse_req(b: &[u8], line: &Span) -> Option<ReqLine> {
    let ms = json::members(b, line).ok()?;
    let mut tail = Vec::new();
    if let Some(t) = json::find_member(b, &ms, "tail") {
        for e in json::elements(b, &t.value).ok()?.0 {
            tail.push(unhex32(std::str::from_utf8(json::inner(b, &e)).ok()?)?);
        }
    }
    let keep = match json::find_member(b, &ms, "keep") {
        Some(m) => std::str::from_utf8(&b[m.value.clone()]).ok()?.parse().ok()?,
        None => 0,
    };
    let mut markers = Vec::new();
    if let Some(c) = json::find_member(b, &ms, "cacheControl") {
        for e in json::elements(b, &c.value).ok()?.0 {
            let em = json::members(b, &e).ok()?;
            let path = json::find_member(b, &em, "path")?;
            let value = json::find_member(b, &em, "value")?;
            let mut steps = Vec::new();
            for p in json::elements(b, &path.value).ok()?.0 {
                if b[p.start] == b'"' {
                    steps.push(MarkerStep::Key(b[p.clone()].to_vec()));
                } else {
                    steps.push(MarkerStep::Index(std::str::from_utf8(&b[p.clone()]).ok()?.parse().ok()?));
                }
            }
            markers.push((steps, canon(b, &value.value)?));
        }
    }
    let mut meta = Vec::from(&b"{"[..]);
    for (i, k) in ["querySource", "version"].iter().enumerate() {
        if let Some(m) = json::find_member(b, &ms, k) {
            if meta.len() > 1 {
                meta.push(b',');
            }
            meta.extend_from_slice(&b[m.key.clone()]);
            meta.push(b':');
            meta.extend_from_slice(&b[m.value.clone()]);
        }
        let _ = i;
    }
    meta.push(b'}');
    Some(ReqLine {
        session: str_of(b, &ms, "sessionId").unwrap_or_default(),
        agent: str_of(b, &ms, "agentId").unwrap_or_default(),
        id: str_of(b, &ms, "id")?,
        time: str_of(b, &ms, "timestamp").unwrap_or_default(),
        meta,
        shape: str_of(b, &ms, "shapeHash").and_then(|s| unhex32(&s)),
        params: json::find_member(b, &ms, "params").map(|m| m.value.clone()).unwrap_or(0..0),
        base: str_of(b, &ms, "base"),
        keep,
        tail,
        markers,
        digest: str_of(b, &ms, "requestDigest").and_then(|s| unhex32(&s)),
    })
}

/// Content blocks of a message object as canonical bytes (a string becomes one text block).
fn blocks_of(b: &[u8], msg: &Span) -> Option<Vec<Vec<u8>>> {
    let ms = json::members(b, msg).ok()?;
    let c = json::find_member(b, &ms, "content")?;
    if b[c.value.start] == b'"' {
        let mut t = Vec::from(&br#"{"text":"#[..]);
        t.extend_from_slice(&b[c.value.clone()]);
        t.extend_from_slice(br#","type":"text"}"#);
        return Some(vec![t]);
    }
    let (els, _) = json::elements(b, &c.value).ok()?;
    els.iter().map(|e| canon(b, e)).collect()
}

fn msg_from_blocks(role: &str, blocks: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::from(&br#"{"content":["#[..]);
    for (i, x) in blocks.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(x);
    }
    out.extend_from_slice(br#"],"role":""#);
    out.extend_from_slice(role.as_bytes());
    out.extend_from_slice(b"\"}");
    out
}

/// Turn string content into one text block and add cache markers to blocks.
fn send_form(canonical_msg: &[u8], markers: &[(usize, &[u8])]) -> Option<Vec<u8>> {
    let node = json::parse_all(canonical_msg).ok()?;
    let mut v = V::from_node(canonical_msg, &node);
    let V::Obj(ms) = &mut v else { return None };
    let content = ms.iter_mut().find(|m| m.0 == b"\"content\"")?;
    if let V::Raw(r) = &content.1
        && r.first() == Some(&b'"') {
            let s = r.clone();
            content.1 = V::Arr(vec![V::Obj(vec![(b"\"text\"".to_vec(), V::Raw(s)), (b"\"type\"".to_vec(), V::Raw(b"\"text\"".to_vec()))])]);
        }
    if let V::Arr(items) = &mut content.1 {
        for &(j, val) in markers {
            let blk = items.get_mut(j)?;
            blk.set(b"\"cache_control\"", V::Raw(val.to_vec()));
        }
    } else if !markers.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(canonical_msg.len() + 64);
    v.write(&mut out);
    Some(out)
}

pub struct LogFile {
    data: Vec<u8>,
    lines: Vec<Span>,
}

/// Every `*.jsonl` file under `root`, sorted.
pub fn log_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(p),
                Ok(t) if t.is_file() && p.extension().is_some_and(|x| x == "jsonl") => out.push(p),
                _ => {}
            }
        }
    }
    out.sort();
    out
}

impl LogFile {
    pub fn read(path: &Path) -> std::io::Result<LogFile> {
        let data = std::fs::read(path)?;
        let mut lines = Vec::new();
        let mut s = 0;
        for i in memchr::memchr_iter(b'\n', &data) {
            if i > s {
                lines.push(s..i);
            }
            s = i + 1;
        }
        if s < data.len() {
            lines.push(s..data.len());
        }
        Ok(LogFile { data, lines })
    }

    pub fn has_requests(&self) -> bool {
        memchr::memmem::find(&self.data, br#"{"type":"api-request""#).is_some()
    }

    /// Just the request ids and digests (for checking a store against the logs).
    pub fn request_digests(&self) -> Vec<(String, Option<[u8; 32]>)> {
        let b = &self.data;
        self.lines
            .iter()
            .filter(|l| b[(*l).clone()].starts_with(br#"{"type":"api-request""#))
            .filter_map(|l| {
                let ms = json::members(b, l).ok()?;
                Some((str_of(b, &ms, "id")?, str_of(b, &ms, "requestDigest").and_then(|s| unhex32(&s))))
            })
            .collect()
    }

    /// Build every recorded request in file order and hand each to `f`.
    pub fn requests(&self, mut f: impl FnMut(LogRequest)) -> FileReport {
        let b = &self.data;
        let mut rep = FileReport::default();
        let mut msgs: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        let mut shapes: HashMap<[u8; 32], Vec<(Vec<u8>, Span)>> = HashMap::new();
        let mut reqs: Vec<Option<ReqLine>> = Vec::new();
        // (is_user, message span, assistant message id)
        let mut conv: Vec<(bool, Span, Option<Vec<u8>>)> = Vec::new();
        for l in &self.lines {
            let line = &b[l.clone()];
            if line.starts_with(br#"{"type":"api-request-blob""#) {
                let Ok(ms) = json::members(b, l) else { continue };
                let (Some(h), Some(m)) = (str_of(b, &ms, "hash").and_then(|s| unhex32(&s)), json::find_member(b, &ms, "message")) else { continue };
                rep.blobs += 1;
                if let Some(c) = canon(b, &m.value) {
                    if sha(&c) == h {
                        msgs.insert(h, c);
                    } else if let Some(blocked) = send_form(&c, &[]).filter(|x| sha(x) == h) {
                        // some blobs are hashed after string content became a text block
                        rep.blob_hashed_as_blocks += 1;
                        msgs.insert(h, blocked);
                    } else {
                        // keep it anyway; the request digest decides
                        rep.blob_hash_mismatch += 1;
                        msgs.insert(h, c);
                    }
                }
            } else if line.starts_with(br#"{"type":"api-request-shape""#) {
                let Ok(ms) = json::members(b, l) else { continue };
                let (Some(h), Some(s)) = (str_of(b, &ms, "shapeHash").and_then(|s| unhex32(&s)), json::find_member(b, &ms, "shape")) else { continue };
                if let Ok(sm) = json::members(b, &s.value) {
                    shapes.insert(h, sm.into_iter().map(|m| (b[m.key.clone()].to_vec(), m.value)).collect());
                }
            } else if line.starts_with(br#"{"type":"api-request""#) {
                reqs.push(parse_req(b, l));
            } else if memchr::memmem::find(line, br#""message""#).is_some() {
                let Ok(ms) = json::members(b, l) else { continue };
                let Some(t) = json::find_member(b, &ms, "type") else { continue };
                let is_user = json::inner(b, &t.value) == b"user";
                if !is_user && json::inner(b, &t.value) != b"assistant" {
                    continue;
                }
                let Some(m) = json::find_member(b, &ms, "message") else { continue };
                if b[m.value.start] != b'{' {
                    continue;
                }
                let mid = if is_user {
                    None
                } else {
                    json::members(b, &m.value).ok().and_then(|mm| json::find_member(b, &mm, "id").map(|x| b[x.value.clone()].to_vec()))
                };
                conv.push((is_user, m.value.clone(), mid));
            }
        }

        // transcript forms, only for hashes no blob covers
        let need: HashSet<[u8; 32]> = reqs.iter().flatten().flat_map(|r| r.tail.iter()).filter(|h| !msgs.contains_key(*h)).copied().collect();
        if !need.is_empty() {
            let mut offer = |c: Vec<u8>, msgs: &mut HashMap<[u8; 32], Vec<u8>>| {
                let h = sha(&c);
                if need.contains(&h) && !msgs.contains_key(&h) {
                    msgs.insert(h, c);
                    rep.from_transcript += 1;
                }
            };
            let mut groups: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
            let mut gix: HashMap<Vec<u8>, usize> = HashMap::new();
            for (is_user, span, mid) in &conv {
                if *is_user {
                    if let Some(c) = canon(b, span) {
                        if memchr::memmem::find(&c, br#"{"content":""#) == Some(0)
                            && let Some(blocked) = send_form(&c, &[]) {
                                offer(blocked, &mut msgs);
                            }
                        offer(c, &mut msgs);
                    }
                } else if let Some(bl) = blocks_of(b, span) {
                    offer(msg_from_blocks("assistant", &bl), &mut msgs);
                    let k = mid.clone().unwrap_or_default();
                    let i = *gix.entry(k.clone()).or_insert_with(|| {
                        groups.push((k, Vec::new()));
                        groups.len() - 1
                    });
                    groups[i].1.extend(bl);
                }
            }
            for (_, bl) in &groups {
                offer(msg_from_blocks("assistant", bl), &mut msgs);
            }
            let mut i = 0;
            while i < conv.len() {
                if !conv[i].0 {
                    i += 1;
                    continue;
                }
                let mut j = i;
                while j < conv.len() && conv[j].0 {
                    j += 1;
                }
                for a in i..j {
                    let mut acc: Vec<Vec<u8>> = Vec::new();
                    for (k, item) in conv.iter().enumerate().take(j.min(a + 12)).skip(a) {
                        if let Some(bl) = blocks_of(b, &item.1) {
                            acc.extend(bl);
                        }
                        if k > a {
                            offer(msg_from_blocks("user", &acc), &mut msgs);
                        }
                    }
                }
                i = j;
            }
        }

        let mut lists: HashMap<String, Vec<[u8; 32]>> = HashMap::new();
        for r in reqs {
            rep.requests += 1;
            let Some(r) = r else {
                continue;
            };
            let list = match &r.base {
                None => Some(r.tail.clone()),
                Some(base) => lists.get(base).map(|bl| {
                    let mut v = bl[..r.keep.min(bl.len())].to_vec();
                    v.extend_from_slice(&r.tail);
                    v
                }),
            };
            let body = match &list {
                None => Err(Problem::MissingBase),
                Some(list) => build_body(b, &r, list, &msgs, &shapes),
            };
            if let Some(list) = list {
                lists.insert(r.id.clone(), list);
            }
            f(LogRequest { session: r.session, agent: r.agent, id: r.id, time: r.time, meta: r.meta, digest: r.digest, body });
        }
        rep
    }
}

fn build_body(
    b: &[u8],
    r: &ReqLine,
    list: &[[u8; 32]],
    msgs: &HashMap<[u8; 32], Vec<u8>>,
    shapes: &HashMap<[u8; 32], Vec<(Vec<u8>, Span)>>,
) -> Result<Vec<u8>, Problem> {
    let shape = match &r.shape {
        Some(h) => Some(shapes.get(h).ok_or(Problem::MissingShape)?),
        None => None,
    };
    // markers by target
    let mut msg_marks: HashMap<usize, Vec<(usize, &[u8])>> = HashMap::new();
    let mut top_marks: HashMap<Vec<u8>, Vec<(usize, &[u8])>> = HashMap::new();
    for (path, val) in &r.markers {
        let v = val.as_slice();
        match path.as_slice() {
            [MarkerStep::Key(k), MarkerStep::Index(i), MarkerStep::Key(c), MarkerStep::Index(j)] if k == b"\"messages\"" && c == b"\"content\"" => {
                msg_marks.entry(*i).or_default().push((*j, v));
            }
            [MarkerStep::Key(k), MarkerStep::Index(i)] => top_marks.entry(k.clone()).or_default().push((*i, v)),
            _ => return Err(Problem::BadMarker),
        }
    }
    if msg_marks.keys().any(|&i| i >= list.len()) {
        return Err(Problem::BadMarker);
    }
    let mut members: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if !r.params.is_empty() {
        for m in json::members(b, &r.params).map_err(|_| Problem::BadLine)? {
            members.push((b[m.key.clone()].to_vec(), canon(b, &m.value).ok_or(Problem::BadLine)?));
        }
    }
    let mut arr = Vec::with_capacity(list.iter().map(|h| msgs.get(h).map_or(0, |m| m.len() + 1)).sum::<usize>() + 2);
    arr.push(b'[');
    for (i, h) in list.iter().enumerate() {
        let m = msgs.get(h).ok_or(Problem::MissingMessage)?;
        if i > 0 {
            arr.push(b',');
        }
        let marks = msg_marks.get(&i).map(|v| v.as_slice()).unwrap_or(&[]);
        // string content or markers need the message rewritten; most messages are copied
        let needs = !marks.is_empty() || memchr::memmem::find(m, br#"{"content":""#) == Some(0);
        if needs {
            arr.extend_from_slice(&send_form(m, marks).ok_or(Problem::BadMarker)?);
        } else {
            arr.extend_from_slice(m);
        }
    }
    arr.push(b']');
    members.push((b"\"messages\"".to_vec(), arr));
    if let Some(shape) = shape {
        for (k, span) in shape.iter() {
            let marks = top_marks.remove(k).unwrap_or_default();
            let val = if marks.is_empty() {
                canon(b, span).ok_or(Problem::BadLine)?
            } else {
                let node = json::parse(b, span.start).map_err(|_| Problem::BadLine)?.0;
                let mut v = V::from_node(b, &node);
                let V::Arr(items) = &mut v else { return Err(Problem::BadMarker) };
                for (j, mv) in marks {
                    items.get_mut(j).ok_or(Problem::BadMarker)?.set(b"\"cache_control\"", V::Raw(mv.to_vec()));
                }
                let mut out = Vec::new();
                // children of a parsed node are not canonical yet: canonicalize the result
                v.write(&mut out);
                json::canonical(&out).map_err(|_| Problem::BadLine)?
            };
            members.push((k.clone(), val));
        }
    }
    if !top_marks.is_empty() {
        return Err(Problem::BadMarker);
    }
    members.sort_by(|a, c| cmp_key_tokens(&a.0, &c.0));
    let mut out = Vec::with_capacity(members.iter().map(|m| m.0.len() + m.1.len() + 2).sum::<usize>() + 2);
    out.push(b'{');
    for (i, (k, v)) in members.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(k);
        out.push(b':');
        out.extend_from_slice(v);
    }
    out.push(b'}');
    Ok(out)
}

pub fn digest_hex(d: &[u8; 32]) -> String {
    hex(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(s: String) -> String {
        s + "\n"
    }

    /// A tiny made-up log in Claude Code's format.
    #[test]
    fn builds_bodies_from_a_synthetic_log() {
        let user = r#"{"role":"user","content":"hello there"}"#;
        let u_canon = json::canonical(user.as_bytes()).unwrap();
        let u_hash = hex(&sha(&u_canon));
        let blob_msg = r#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}"#;
        let b_hash = hex(&sha(&json::canonical(blob_msg.as_bytes()).unwrap()));
        let asst1 = r#"{"id":"msg_1","role":"assistant","content":[{"type":"text","text":"let me look"}]}"#;
        let asst2 = r#"{"id":"msg_1","role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"path":"a"}}]}"#;
        let grouped = msg_from_blocks(
            "assistant",
            &[br#"{"text":"let me look","type":"text"}"#.to_vec(), br#"{"id":"t1","input":{"path":"a"},"name":"Read","type":"tool_use"}"#.to_vec()],
        );
        let a_hash = hex(&sha(&grouped));
        let shape = r#"{"system":[{"type":"text","text":"sys"}],"tools":[{"name":"Read"}]}"#;
        let s_hash = hex(&sha(&json::canonical(shape.as_bytes()).unwrap()));
        // the body Claude Code would have sent: string content as a block, markers in place
        let expect = format!(
            r#"{{"max_tokens":10,"messages":[{{"content":[{{"text":"hello there","type":"text"}}],"role":"user"}},{},{{"content":[{{"cache_control":{{"type":"ephemeral"}},"content":"ok","tool_use_id":"t1","type":"tool_result"}}],"role":"user"}}],"model":"m","system":[{{"cache_control":{{"type":"ephemeral"}},"text":"sys","type":"text"}}],"tools":[{{"name":"Read"}}]}}"#,
            String::from_utf8(grouped.clone()).unwrap()
        );
        let digest = hex(&sha(expect.as_bytes()));
        let mut log = String::new();
        log += &line(format!(r#"{{"type":"user","message":{user},"uuid":"1"}}"#));
        log += &line(format!(r#"{{"type":"api-request-shape","shapeHash":"{s_hash}","shape":{shape}}}"#));
        log += &line(format!(r#"{{"type":"api-request","sessionId":"s1","id":"r1","timestamp":"t","shapeHash":"{s_hash}","params":{{"model":"m","max_tokens":10}},"keep":0,"tail":["{u_hash}"],"cacheControl":[],"requestDigest":"x"}}"#));
        log += &line(format!(r#"{{"type":"assistant","message":{asst1}}}"#));
        log += &line(format!(r#"{{"type":"assistant","message":{asst2}}}"#));
        log += &line(format!(r#"{{"type":"api-request-blob","hash":"{b_hash}","message":{blob_msg}}}"#));
        log += &line(format!(
            r#"{{"type":"api-request","sessionId":"s1","id":"r2","timestamp":"t","shapeHash":"{s_hash}","params":{{"model":"m","max_tokens":10}},"base":"r1","keep":1,"tail":["{a_hash}","{b_hash}"],"cacheControl":[{{"path":["system",0],"value":{{"type":"ephemeral"}}}},{{"path":["messages",2,"content",0],"value":{{"type":"ephemeral"}}}}],"requestDigest":"{digest}"}}"#
        ));
        let dir = std::env::temp_dir().join(format!("tdb-claude-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("s1.jsonl");
        std::fs::write(&p, log).unwrap();
        let lf = LogFile::read(&p).unwrap();
        let mut got = Vec::new();
        lf.requests(|r| got.push(r));
        assert_eq!(got.len(), 2);
        assert!(got[0].body.is_ok());
        assert_eq!(String::from_utf8(got[1].body.clone().unwrap()).unwrap(), expect);
        assert!(got[1].verified());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
