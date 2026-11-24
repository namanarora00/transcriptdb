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

/// Value codes at or above this mark a value too long for the dictionary: the code points
/// at an (offset, length) in [`SessionSteps::longs`], the value's place in the call's
/// stored message, so reading it touches only those bytes.
pub const LONG_BASE: u32 = 1 << 31;

#[inline]
pub fn is_long(v: u32) -> bool {
    v >= LONG_BASE
}
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
        if let std::collections::hash_map::Entry::Vacant(e) = self.ix.entry(h) {
            e.insert(c);
        } else {
            self.clash.insert(v.into(), c);
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

/// The text of the tool result in content block `block` of a message: the string itself,
/// or the text blocks of a list joined by newlines. `None` when the message has no such
/// result or the result has no text.
pub fn result_text(b: &[u8], block: u16) -> Option<String> {
    let (_, results) = scan_message(b);
    let c = &results.iter().find(|r| r.block == block)?.content;
    match b.get(c.start)? {
        b'"' => Some(crate::text::unescape(json::inner(b, c))),
        b'[' => {
            let (els, _) = json::elements(b, c).ok()?;
            let mut texts: Vec<String> = Vec::new();
            for el in els.iter().filter(|el| b[el.start] == b'{') {
                let Ok(ms) = json::members(b, el) else { continue };
                if !json::find_member(b, &ms, "type").is_some_and(|t| json::inner(b, &t.value) == b"text") {
                    continue;
                }
                if let Some(t) = json::find_member(b, &ms, "text").filter(|m| b[m.value.start] == b'"') {
                    texts.push(crate::text::unescape(json::inner(b, &t.value)));
                }
            }
            (!texts.is_empty()).then(|| texts.join("\n"))
        }
        _ => None,
    }
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

/// An input field's value: a dictionary code, or where a long value sits in the call's
/// stored message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldVal {
    Code(u32),
    Long { off: u32, len: u32 },
}

/// A call remembered until its result arrives (or found again by walking up the tree).
#[derive(Debug, Clone)]
pub struct CallInfo {
    pub node: u32,
    pub block: u16,
    pub tool: u32,
    pub fields: Vec<(u32, FieldVal)>,
}

/// An input field as found in a stored message: `val` is empty for long values.
#[derive(Debug, Clone)]
pub struct RawField {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub off: u32,
    pub len: u32,
}

/// One session's steps as columns. Tool and field codes are the store's (see
/// [`StepDicts`]); input values are coded per session, in `vals`.
#[derive(Debug, Default, Clone)]
pub struct SessionSteps {
    pub rows: Vec<StepRow>,
    /// Fields of step `i` are `fields[fstart[i]..fstart[i + 1]]`.
    pub fstart: Vec<u32>,
    /// (field id, value code, or `LONG_BASE + i` for `longs[i]`)
    pub fields: Vec<(u32, u32)>,
    /// (offset, length) of long values in their call's stored message.
    pub longs: Vec<(u32, u32)>,
    /// One shape code per step.
    pub shape: Vec<u8>,
    /// Per field id seen in this session: its values.
    pub vals: Vec<(u32, Dict)>,
    /// Per tree node: the last step completed at or above it on its path (`NONE` if none).
    /// Kept only while the session takes writes.
    pub tail: Vec<u32>,
    /// Steps `0..flushed` are in segments.
    pub flushed: u32,
    /// Calls seen since the session was loaded for writing, by tool_use id.
    pub calls: HashMap<Box<[u8]>, CallInfo>,
    /// Bitset of tool codes present in the session.
    pub tool_set: Vec<u64>,
    /// The turn tree of `rows`, worked out on first use and dropped when a step is added.
    pub turns: std::sync::OnceLock<std::sync::Arc<Turns>>,
    /// Step positions by shape code, likewise.
    pub by_shape: std::sync::OnceLock<std::sync::Arc<ByShape>>,
    /// A copy without the input values (`vals` empty; see [`SessionSteps::without_values`]):
    /// for queries whose tests read none, so the steps of many more sessions fit the cache.
    pub light: bool,
    /// Per field: how many values it has (kept in a copy without them).
    pub counts: Vec<(u32, u32)>,
    /// In a `light` copy: the fields whose values it does hold (see
    /// [`SessionSteps::with_fields`]), ascending.
    pub kept: Vec<u32>,
}

/// Step positions grouped by shape code: code `c`'s are `pos[start[c]..start[c + 1]]`, ascending.
#[derive(Debug, Clone, Default)]
pub struct ByShape {
    pub start: Vec<u32>,
    pub pos: Vec<u32>,
}

impl ByShape {
    pub fn new(shape: &[u8]) -> ByShape {
        let mut start = vec![0u32; 257];
        for &c in shape {
            start[c as usize + 1] += 1;
        }
        for c in 0..256 {
            start[c + 1] += start[c];
        }
        let mut fill = start.clone();
        let mut pos = vec![0u32; shape.len()];
        for (i, &c) in shape.iter().enumerate() {
            pos[fill[c as usize] as usize] = i as u32;
            fill[c as usize] += 1;
        }
        ByShape { start, pos }
    }
    pub fn of(&self, code: u8) -> &[u32] {
        &self.pos[self.start[code as usize] as usize..self.start[code as usize + 1] as usize]
    }
}

impl SessionSteps {
    /// Where long value `v` sits in its call's stored message.
    pub fn long_at(&self, v: u32) -> (u32, u32) {
        self.longs[(v - LONG_BASE) as usize]
    }

    pub fn has_tool(&self, t: u32) -> bool {
        self.tool_set.get((t / 64) as usize).is_some_and(|w| w >> (t % 64) & 1 == 1)
    }

    pub fn fields_of(&self, i: u32) -> &[(u32, u32)] {
        if self.fstart.is_empty() {
            return &[];
        }
        &self.fields[self.fstart[i as usize] as usize..self.fstart[i as usize + 1] as usize]
    }

    /// Values of field `f` in this session.
    pub fn field_dict(&self, f: u32) -> Option<&Dict> {
        assert!(!self.light || self.kept.contains(&f), "input values read from a copy of a session's steps without them");
        self.vals.iter().find(|x| x.0 == f).map(|x| &x.1)
    }

    /// A copy holding the values of fields `keep` only (ascending): what a query whose tests
    /// read just those fields needs. A session's commands and file contents are most of its
    /// steps' bytes, so many more such copies fit the cache than whole ones.
    pub fn with_fields(&self, keep: &[u32]) -> SessionSteps {
        let mut c = self.without_values();
        c.vals = self.vals.iter().filter(|(f, _)| keep.contains(f)).cloned().collect();
        c.kept = keep.to_vec();
        c
    }

    /// A copy without the input values: rows, field codes, shape codes, tool set, turns and
    /// steps by shape code, and each field's number of values.
    pub fn without_values(&self) -> SessionSteps {
        SessionSteps {
            rows: self.rows.clone(),
            fstart: self.fstart.clone(),
            fields: self.fields.clone(),
            longs: self.longs.clone(),
            shape: self.shape.clone(),
            flushed: self.flushed,
            tool_set: self.tool_set.clone(),
            turns: self.turns.clone(),
            by_shape: self.by_shape.clone(),
            light: true,
            counts: if self.light { self.counts.clone() } else { self.vals.iter().map(|(f, d)| (*f, d.len() as u32)).collect() },
            ..SessionSteps::default()
        }
    }

    /// The JSON text of value `v` of field `f` (a short value; see [`is_long`]).
    pub fn value(&self, f: u32, v: u32) -> &[u8] {
        self.field_dict(f).expect("field present in session").get(v)
    }

    pub fn value_code(&self, f: u32, text: &[u8]) -> Option<u32> {
        self.field_dict(f)?.code(text)
    }

    pub fn value_count(&self, f: u32) -> usize {
        if self.light && !self.kept.contains(&f) {
            return self.counts.iter().find(|x| x.0 == f).map_or(0, |x| x.1 as usize);
        }
        self.field_dict(f).map_or(0, |d| d.len())
    }

    pub fn intern_value(&mut self, f: u32, text: &[u8]) -> u32 {
        let i = match self.vals.iter().position(|x| x.0 == f) {
            Some(i) => i,
            None => {
                self.vals.push((f, Dict::default()));
                self.vals.len() - 1
            }
        };
        self.vals[i].1.intern(text)
    }

    /// The session's turns (worked out once).
    pub fn turns(&self) -> std::sync::Arc<Turns> {
        self.turns.get_or_init(|| std::sync::Arc::new(Turns::new(&self.rows))).clone()
    }

    /// Step positions by shape code (worked out once).
    pub fn by_shape(&self) -> std::sync::Arc<ByShape> {
        self.by_shape.get_or_init(|| std::sync::Arc::new(ByShape::new(&self.shape))).clone()
    }

    /// Append a step, keeping shape codes and the tool set in step.
    pub fn push(&mut self, row: StepRow, fields: &[(u32, FieldVal)]) -> u32 {
        self.turns.take();
        self.by_shape.take();
        let pos = self.rows.len() as u32;
        if self.fstart.is_empty() {
            self.fstart.push(0);
        }
        self.rows.push(row);
        for &(f, v) in fields {
            let code = match v {
                FieldVal::Code(c) => c,
                FieldVal::Long { off, len } => {
                    self.longs.push((off, len));
                    LONG_BASE + self.longs.len() as u32 - 1
                }
            };
            self.fields.push((f, code));
        }
        self.fstart.push(self.fields.len() as u32);
        self.shape.push(StepDicts::shape_code(row.tool, row.failed));
        let w = (row.tool / 64) as usize;
        if self.tool_set.len() <= w {
            self.tool_set.resize(w + 1, 0);
        }
        self.tool_set[w] |= 1 << (row.tool % 64);
        pos
    }

    /// Bytes this takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        self.turns.get().map_or(0, |t| t.of.len() * 4 + t.depth.len() * 16) + self.by_shape.get().map_or(0, |b| 1028 + b.pos.len() * 4) + self.counts.len() * 8 + self.rows.len() * 40 + self.fields.len() * 8 + self.fstart.len() * 4 + self.longs.len() * 8 + self.shape.len() + self.tail.len() * 4 + self.vals.iter().map(|(_, d)| d.bytes() + d.len() * 48).sum::<usize>() + self.calls.len() * 96 + 256
    }
}

/// A session's turns: steps that share `prev`, call message and result message form a turn;
/// a turn's parent is the turn of its `prev`. Kept with the session's steps once worked out.
#[derive(Debug, Clone, Default)]
pub struct Turns {
    /// Turn of each step.
    pub of: Vec<u32>,
    pub parent: Vec<u32>,
    pub depth: Vec<u32>,
    /// Preorder number of each turn and the last preorder number in its subtree.
    pub tin: Vec<u32>,
    pub tout: Vec<u32>,
}

impl Turns {
    pub fn new(rows: &[StepRow]) -> Turns {
        let n = rows.len();
        let mut t = Turns { of: Vec::with_capacity(n), parent: Vec::with_capacity(n), depth: Vec::with_capacity(n), tin: Vec::new(), tout: Vec::new() };
        let mut i = 0;
        while i < n {
            let r = rows[i];
            let mut j = i + 1;
            while j < n && rows[j].prev == r.prev && rows[j].call_node == r.call_node && rows[j].result_node == r.result_node {
                j += 1;
            }
            let g = t.depth.len() as u32;
            t.of.resize(j, g);
            let p = if r.prev == u32::MAX { u32::MAX } else { t.of[r.prev as usize] };
            t.depth.push(if p == u32::MAX { 1 } else { t.depth[p as usize] + 1 });
            t.parent.push(p);
            i = j;
        }
        // preorder numbers from subtree sizes (parents come before their children)
        let nt = t.depth.len();
        let mut size = vec![1u32; nt];
        for g in (0..nt).rev() {
            let p = t.parent[g];
            if p != u32::MAX {
                size[p as usize] += size[g];
            }
        }
        t.tin = vec![0; nt];
        let mut next = vec![0u32; nt];
        let mut clock = 0u32;
        for g in 0..nt {
            let p = t.parent[g];
            let at = if p == u32::MAX {
                let x = clock;
                clock += size[g];
                x
            } else {
                let x = next[p as usize];
                next[p as usize] += size[g];
                x
            };
            t.tin[g] = at;
            next[g] = at + 1;
        }
        t.tout = (0..nt).map(|g| t.tin[g] + size[g] - 1).collect();
        t
    }

    pub fn is_ancestor(&self, a: u32, b: u32) -> bool {
        self.tin[a as usize] <= self.tin[b as usize] && self.tin[b as usize] <= self.tout[a as usize]
    }
}

/// Tool and field names, shared by all sessions: there are few of them, and shared codes
/// make shape codes and tool tests the same in every session.
#[derive(Debug, Default, Clone)]
pub struct StepDicts {
    pub tools: Dict,
    pub fields: Dict,
}

impl StepDicts {
    pub fn field(&mut self, name: &[u8]) -> u32 {
        self.fields.intern(name)
    }

    /// Code a call's tool and fields; short values go into the session's dictionaries.
    pub fn call_info(&mut self, ss: &mut SessionSteps, node: u32, block: u16, name: &[u8], fields: &[RawField]) -> CallInfo {
        let tool = self.tools.intern(name);
        let fields = fields
            .iter()
            .map(|r| {
                let f = self.field(&r.key);
                let v = if r.len as usize > MAX_VALUE { FieldVal::Long { off: r.off, len: r.len } } else { FieldVal::Code(ss.intern_value(f, &r.val)) };
                (f, v)
            })
            .collect();
        CallInfo { node, block, tool, fields }
    }

    pub fn shape_code(tool: u32, failed: bool) -> u8 {
        let t = if tool < SHAPE_OTHER as u32 { tool as u8 } else { SHAPE_OTHER };
        t << 1 | failed as u8
    }
}

/// An owned copy of what [`scan_message`] finds, so the caller can drop the borrow.
pub struct Extracted {
    pub calls: Vec<(Vec<u8>, u16, Vec<u8>, Vec<RawField>)>,
    pub results: Vec<(u16, Vec<u8>, bool, json::Span)>,
}

pub fn extract(b: &[u8]) -> Extracted {
    let (c, r) = scan_message(b);
    let base = b.as_ptr() as usize;
    Extracted {
        calls: c
            .into_iter()
            .map(|c| {
                let fields = c
                    .fields
                    .into_iter()
                    .map(|(k, v)| RawField {
                        key: k.to_vec(),
                        val: if v.len() > MAX_VALUE { Vec::new() } else { v.to_vec() },
                        off: (v.as_ptr() as usize - base) as u32,
                        len: v.len() as u32,
                    })
                    .collect();
                (c.id.to_vec(), c.block, c.name.to_vec(), fields)
            })
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
        assert_eq!(result_text(u, 0).as_deref(), Some("ok"));
        assert_eq!(result_text(u, 1), None);
        let l = br#"{"content":[{"content":[{"text":"3 failed\n","type":"text"},{"source":{"data":""},"type":"image"},{"text":"done","type":"text"}],"tool_use_id":"t1","type":"tool_result"}],"role":"user"}"#;
        assert_eq!(result_text(l, 0).as_deref(), Some("3 failed\n\ndone"));
    }
}
