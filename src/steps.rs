//! Steps: a tool call paired with its result, kept as dictionary-coded columns.
//!
//! A step's fields are the tool name, the call's input fields (top-level keys of
//! `input`), whether the result was an error, the result's size and the time the
//! result first reached the model. Each session's steps are numbered in arrival
//! order, and each step records `prev`: the last step *before its turn* on its own
//! path through the session tree. Tool calls made in one model turn share a `prev`,
//! so "then" never orders them, and a forked session (a side query, a rewind) is
//! still one array: walking steps in number order and following `prev` visits every
//! path, with the shared start of the tree walked once.
//!
//! The one-byte *shape code* of a step is `tool << 1 | failed`, with tool codes 0..126
//! for the first 127 tools seen and 127 for any other. A session's shape string is
//! the fast path for patterns over tool and outcome.

use crate::json;
use std::collections::HashMap;

/// An input value too long for the dictionary: queries must open the message.
pub const LONG: u32 = u32::MAX - 1;
/// Input values up to this many bytes (as JSON text) are dictionary-coded. 4 KB covers 98% of
/// shell commands in the measured logs (half are over 256 bytes).
pub const MAX_VALUE: usize = 4096;
pub const SHAPE_OTHER: u8 = 127;

/// Distinct values with dense codes. The index holds a 128-bit hash of each value, not a
/// second copy; a (never yet seen) hash collision falls back to an exact side table.
#[derive(Default, Debug, Clone)]
pub struct Dict {
    vals: Vec<Box<[u8]>>,
    ix: HashMap<u128, u32>,
    clash: HashMap<Box<[u8]>, u32>,
}

impl Dict {
    pub fn intern(&mut self, v: &[u8]) -> u32 {
        if let Some(c) = self.code(v) {
            return c;
        }
        let c = self.vals.len() as u32;
        self.vals.push(v.into());
        let h = xxhash_rust::xxh3::xxh3_128(v);
        if self.ix.contains_key(&h) {
            self.clash.insert(v.into(), c);
        } else {
            self.ix.insert(h, c);
        }
        c
    }
    pub fn get(&self, c: u32) -> &[u8] {
        &self.vals[c as usize]
    }
    pub fn code(&self, v: &[u8]) -> Option<u32> {
        match self.ix.get(&xxhash_rust::xxh3::xxh3_128(v)) {
            Some(&c) if *self.vals[c as usize] == *v => Some(c),
            Some(_) => self.clash.get(v).copied(),
            None => None,
        }
    }
    pub fn len(&self) -> usize {
        self.vals.len()
    }
    pub fn is_empty(&self) -> bool {
        self.vals.is_empty()
    }
    pub fn bytes(&self) -> usize {
        self.vals.iter().map(|v| v.len()).sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepRow {
    pub call_node: u32,
    pub result_node: u32,
    /// Last step before this step's turn on its path, or `NONE`.
    pub prev: u32,
    pub tool: u32,
    pub time_ms: i64,
    /// Size of the result content as sent (opaque values counted at their text size).
    pub out_bytes: u32,
    pub call_block: u16,
    pub result_block: u16,
    pub failed: bool,
}

/// A tool call found in a message.
#[derive(Debug, Clone)]
pub struct RawCall<'a> {
    pub block: u16,
    pub id: &'a [u8],
    pub name: &'a [u8],
    /// (key without quotes, value as JSON text)
    pub fields: Vec<(&'a [u8], &'a [u8])>,
}

/// A tool result found in a message.
#[derive(Debug, Clone)]
pub struct RawResult<'a> {
    pub block: u16,
    pub tool_use_id: &'a [u8],
    pub is_error: bool,
    /// Byte range of the `content` value in the message.
    pub content: json::Span,
}

/// Tool calls and tool results among a message's top-level content blocks.
pub fn scan_message(b: &[u8]) -> (Vec<RawCall<'_>>, Vec<RawResult<'_>>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    let f = memchr::memmem::find;
    if f(b, b"\"tool_use\"").is_none() && f(b, b"\"tool_result\"").is_none() {
        return (calls, results);
    }
    let Ok(ms) = json::members(b, &(0..b.len())) else { return (calls, results) };
    let Some(c) = json::find_member(b, &ms, "content") else { return (calls, results) };
    let Ok((els, _)) = json::elements(b, &c.value) else { return (calls, results) };
    for (k, el) in els.iter().enumerate() {
        if b[el.start] != b'{' {
            continue;
        }
        let Ok(bm) = json::members(b, el) else { continue };
        let Some(t) = json::find_member(b, &bm, "type") else { continue };
        let ty = json::inner(b, &t.value);
        let s = |name: &str| json::find_member(b, &bm, name).filter(|m| b[m.value.start] == b'"').map(|m| json::inner(b, &m.value));
        if ty == b"tool_use" {
            let (Some(id), Some(name)) = (s("id"), s("name")) else { continue };
            let mut fields = Vec::new();
            if let Some(inp) = json::find_member(b, &bm, "input")
                && b[inp.value.start] == b'{'
                    && let Ok(im) = json::members(b, &inp.value) {
                        for m in im {
                            fields.push((json::inner(b, &m.key), &b[m.value.clone()]));
                        }
                    }
            calls.push(RawCall { block: k as u16, id, name, fields });
        } else if ty == b"tool_result" {
            let Some(id) = s("tool_use_id") else { continue };
            let is_error = json::find_member(b, &bm, "is_error").is_some_and(|m| &b[m.value.clone()] == b"true");
            let content = json::find_member(b, &bm, "content").map(|m| m.value.clone()).unwrap_or(el.end..el.end);
            results.push(RawResult { block: k as u16, tool_use_id: id, is_error, content });
        }
    }
    (calls, results)
}

/// Milliseconds since the Unix epoch for `YYYY-MM-DDTHH:MM:SS[.fff]Z`, or a plain number
/// of seconds; 0 when it cannot be read.
pub fn parse_time_ms(s: &str) -> i64 {
    if let Ok(secs) = s.parse::<i64>() {
        return secs * 1000;
    }
    let b = s.as_bytes();
    let num = |a: usize, n: usize| -> Option<i64> { std::str::from_utf8(b.get(a..a + n)?).ok()?.parse().ok() };
    let (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(se)) = (num(0, 4), num(5, 2), num(8, 2), num(11, 2), num(14, 2), num(17, 2)) else {
        return 0;
    };
    let mut ms = 0;
    if b.get(19) == Some(&b'.') {
        let mut k = 20;
        let mut scale = 100;
        while k < b.len() && b[k].is_ascii_digit() {
            ms += (b[k] - b'0') as i64 * scale;
            scale /= 10;
            k += 1;
        }
    }
    // days from civil (Howard Hinnant)
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 24 + h) * 60 + mi) * 60_000 + se * 1000 + ms
}

/// A call remembered until its result arrives (or found again by walking up the tree).
#[derive(Debug, Clone)]
pub struct CallInfo {
    pub node: u32,
    pub block: u16,
    pub tool: u32,
    pub fields: Vec<(u32, u32)>,
}

/// One session's steps as columns.
#[derive(Debug, Default)]
pub struct SessionSteps {
    pub rows: Vec<StepRow>,
    /// Fields of step `i` are `fields[fstart[i]..fstart[i + 1]]`.
    pub fstart: Vec<u32>,
    /// (field id, value code or `LONG`)
    pub fields: Vec<(u32, u32)>,
    /// One shape code per step.
    pub shape: Vec<u8>,
    /// Per tree node: the last step completed at or above it on its path (`NONE` if none).
    pub tail: Vec<u32>,
    /// Steps `0..flushed` are in segments.
    pub flushed: u32,
    /// Calls seen since the store opened, by tool_use id.
    pub calls: HashMap<Box<[u8]>, CallInfo>,
    /// Bitset of tool codes present in the session.
    pub tool_set: Vec<u64>,
}

impl SessionSteps {
    pub fn has_tool(&self, t: u32) -> bool {
        self.tool_set.get((t / 64) as usize).is_some_and(|w| w >> (t % 64) & 1 == 1)
    }

    pub fn fields_of(&self, i: u32) -> &[(u32, u32)] {
        if self.fstart.is_empty() {
            return &[];
        }
        &self.fields[self.fstart[i as usize] as usize..self.fstart[i as usize + 1] as usize]
    }
}

/// Dictionaries shared by all sessions, plus postings (the field index).
#[derive(Debug, Default)]
pub struct StepDicts {
    pub tools: Dict,
    pub fields: Dict,
    /// Per field id: its values.
    pub values: Vec<Dict>,
    /// Per tool code: steps as `session << 32 | step`.
    pub tool_post: Vec<Vec<u64>>,
    /// Per field id, per value code: steps as `session << 32 | step`.
    pub value_post: Vec<HashMap<u32, Vec<u64>>>,
}

impl StepDicts {
    pub fn field(&mut self, name: &[u8]) -> u32 {
        let f = self.fields.intern(name);
        while self.values.len() <= f as usize {
            self.values.push(Dict::default());
            self.value_post.push(HashMap::new());
        }
        f
    }

    pub fn value(&mut self, field: u32, v: &[u8]) -> u32 {
        if v.len() > MAX_VALUE { LONG } else { self.values[field as usize].intern(v) }
    }

    pub fn call_info(&mut self, node: u32, block: u16, name: &[u8], fields: &[(Vec<u8>, Vec<u8>)]) -> CallInfo {
        let tool = self.tools.intern(name);
        let fields = fields
            .iter()
            .map(|(k, v)| {
                let f = self.field(k);
                (f, self.value(f, v))
            })
            .collect();
        CallInfo { node, block, tool, fields }
    }

    pub fn shape_code(tool: u32, failed: bool) -> u8 {
        let t = if tool < SHAPE_OTHER as u32 { tool as u8 } else { SHAPE_OTHER };
        t << 1 | failed as u8
    }

    /// Append a step to a session, keeping shape and postings in step.
    pub fn push(&mut self, session: u32, ss: &mut SessionSteps, row: StepRow, fields: &[(u32, u32)]) -> u32 {
        let pos = ss.rows.len() as u32;
        if ss.fstart.is_empty() {
            ss.fstart.push(0);
        }
        ss.rows.push(row);
        ss.fields.extend_from_slice(fields);
        ss.fstart.push(ss.fields.len() as u32);
        ss.shape.push(Self::shape_code(row.tool, row.failed));
        let w = (row.tool / 64) as usize;
        if ss.tool_set.len() <= w {
            ss.tool_set.resize(w + 1, 0);
        }
        ss.tool_set[w] |= 1 << (row.tool % 64);
        let key = (session as u64) << 32 | pos as u64;
        let t = row.tool as usize;
        if self.tool_post.len() <= t {
            self.tool_post.resize_with(t + 1, Vec::new);
        }
        self.tool_post[t].push(key);
        for &(f, v) in fields {
            if v != LONG {
                self.value_post[f as usize].entry(v).or_default().push(key);
            }
        }
        pos
    }
}

/// An owned copy of what [`scan_message`] finds, so the caller can drop the borrow.
pub struct Extracted {
    pub calls: Vec<(Vec<u8>, u16, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
    pub results: Vec<(u16, Vec<u8>, bool, json::Span)>,
}

pub fn extract(b: &[u8]) -> Extracted {
    let (c, r) = scan_message(b);
    Extracted {
        calls: c
            .into_iter()
            .map(|c| (c.id.to_vec(), c.block, c.name.to_vec(), c.fields.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()))
            .collect(),
        results: r.into_iter().map(|r| (r.block, r.tool_use_id.to_vec(), r.is_error, r.content)).collect(),
    }
}

/// One step as the slow reference sees it, for checking the engine's steps.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefStep {
    pub tool_use_id: Vec<u8>,
    pub tool: Vec<u8>,
    pub failed: bool,
    pub out_bytes: u32,
    /// (field, value or `None` when longer than `MAX_VALUE`), sorted by field.
    pub fields: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// The slow reference: every tool result in a full request body, paired with the call
/// it answers in an earlier message of the same request. Walks the parsed JSON.
pub fn reference_steps(body: &[u8]) -> Vec<RefStep> {
    let Ok(root) = json::parse_all(body) else { return Vec::new() };
    let json::Node::Obj(top) = &root else { return Vec::new() };
    let Some((_, json::Node::Arr(msgs))) = top.iter().find(|(k, _)| json::key_is(body, k, "messages")) else {
        return Vec::new();
    };
    fn get<'n>(body: &[u8], o: &'n [(json::Span, json::Node)], name: &str) -> Option<&'n json::Node> {
        o.iter().find(|(k, _)| json::key_is(body, k, name)).map(|(_, v)| v)
    }
    let tok = |n: Option<&json::Node>| match n {
        Some(json::Node::Tok(s)) if body[s.start] == b'"' => Some(json::inner(body, s).to_vec()),
        _ => None,
    };
    let mut calls: HashMap<Vec<u8>, (Vec<u8>, Vec<(Vec<u8>, Option<Vec<u8>>)>)> = HashMap::new();
    let mut out = Vec::new();
    for m in msgs {
        let json::Node::Obj(mo) = m else { continue };
        let Some(json::Node::Arr(blocks)) = get(body, mo, "content") else { continue };
        for blk in blocks {
            let json::Node::Obj(bo) = blk else { continue };
            match tok(get(body, bo, "type")).as_deref() {
                Some(b"tool_use") => {
                    let (Some(id), Some(name)) = (tok(get(body, bo, "id")), tok(get(body, bo, "name"))) else { continue };
                    let mut fields = Vec::new();
                    if let Some(json::Node::Obj(im)) = get(body, bo, "input") {
                        for (k, v) in im {
                            let mut text = Vec::new();
                            json::write_canonical(body, v, &mut text);
                            fields.push((json::inner(body, k).to_vec(), if text.len() <= MAX_VALUE { Some(text) } else { None }));
                        }
                    }
                    fields.sort();
                    calls.insert(id, (name, fields));
                }
                Some(b"tool_result") => {
                    let Some(id) = tok(get(body, bo, "tool_use_id")) else { continue };
                    let Some((name, fields)) = calls.get(&id) else { continue };
                    let failed = matches!(get(body, bo, "is_error"), Some(json::Node::Tok(s)) if &body[s.clone()] == b"true");
                    // compact length of the content; equal to the engine's raw length for compact bodies
                    let out_bytes = get(body, bo, "content").map(|c| {
                        let mut t = Vec::new();
                        json::write_canonical(body, c, &mut t);
                        t.len() as u32
                    });
                    out.push(RefStep { tool_use_id: id, tool: name.clone(), failed, out_bytes: out_bytes.unwrap_or(0), fields: fields.clone() });
                }
                _ => {}
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        assert_eq!(parse_time_ms("1970-01-01T00:00:00.000Z"), 0);
        assert_eq!(parse_time_ms("2000-03-01T00:00:01.5Z"), 951_868_801_500);
        assert_eq!(parse_time_ms("1763000007"), 1_763_000_007_000);
        assert_eq!(parse_time_ms("garbage"), 0);
    }

    #[test]
    fn scans_calls_and_results() {
        let a = br#"{"content":[{"text":"x","type":"text"},{"id":"t1","input":{"command":"ls","n":3},"name":"Bash","type":"tool_use"}],"role":"assistant"}"#;
        let (c, r) = scan_message(a);
        assert_eq!(c.len(), 1);
        assert!(r.is_empty());
        assert_eq!(c[0].block, 1);
        assert_eq!(c[0].name, b"Bash");
        assert_eq!(c[0].fields, vec![(&b"command"[..], &b"\"ls\""[..]), (&b"n"[..], &b"3"[..])]);
        let u = br#"{"content":[{"content":"ok","is_error":true,"tool_use_id":"t1","type":"tool_result"}],"role":"user"}"#;
        let (c, r) = scan_message(u);
        assert!(c.is_empty());
        assert_eq!(r[0].tool_use_id, b"t1");
        assert!(r[0].is_error);
        assert_eq!(&u[r[0].content.clone()], b"\"ok\"");
    }
}
