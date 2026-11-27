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
use std::borrow::Cow;
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
    /// (key without quotes, value as JSON text). Borrowed from the message when the input is
    /// a JSON object in it (Anthropic's `input`, Gemini's `args`); decoded when the input is
    /// a JSON string holding an object (OpenAI's `arguments`), whose values longer than
    /// [`MAX_VALUE`] are left out: they are not a range of the message to point at.
    pub fields: Vec<(Cow<'a, [u8]>, Cow<'a, [u8]>)>,
}

/// A tool result found in a message.
#[derive(Debug, Clone)]
pub struct RawResult<'a> {
    pub block: u16,
    pub tool_use_id: &'a [u8],
    pub is_error: bool,
    /// Byte range of the result's content in the message.
    pub content: json::Span,
}

/// Strings in a message that mean it may hold a tool call or result, in one of the shapes
/// [`scan_message`] reads.
const MARKERS: &[&[u8]] = &[b"\"tool_use\"", b"\"tool_result\"", b"\"tool_calls\"", b"\"tool_call_id\"", b"function_call", b"custom_tool_call", b"\"functionCall\"", b"\"functionResponse\""];

/// The members of an object input as fields, borrowed.
fn object_fields<'a>(b: &'a [u8], v: &json::Span) -> Vec<(Cow<'a, [u8]>, Cow<'a, [u8]>)> {
    if b.get(v.start) != Some(&b'{') {
        return Vec::new();
    }
    let Ok(im) = json::members(b, v) else { return Vec::new() };
    im.into_iter().map(|m| (Cow::Borrowed(json::inner(b, &m.key)), Cow::Borrowed(&b[m.value.clone()]))).collect()
}

/// The members of an input sent as a JSON string holding an object, decoded.
fn string_fields<'a>(b: &'a [u8], v: &json::Span) -> Vec<(Cow<'a, [u8]>, Cow<'a, [u8]>)> {
    if b.get(v.start) != Some(&b'"') {
        return object_fields(b, v);
    }
    let text = crate::text::unescape(json::inner(b, v)).into_bytes();
    let start = json::skip_ws(&text, 0);
    if text.get(start) != Some(&b'{') {
        return Vec::new();
    }
    let Ok(end) = json::scan_value(&text, start) else { return Vec::new() };
    let Ok(im) = json::members(&text, &(start..end)) else { return Vec::new() };
    im.into_iter().filter(|m| m.value.len() <= MAX_VALUE).map(|m| (Cow::Owned(json::inner(&text, &m.key).to_vec()), Cow::Owned(text[m.value.clone()].to_vec()))).collect()
}

/// Tool calls and tool results in a message, in any of the shapes model APIs use:
///
/// - Anthropic: `content` blocks of type `tool_use` (`id`, `name`, `input`) and
///   `tool_result` (`tool_use_id`, `content`, `is_error`); the block is its index.
/// - OpenAI Chat Completions: an assistant message's `tool_calls` (`id`,
///   `function.name`, `function.arguments` as a JSON string; the block is its index), and
///   a `tool` message (`tool_call_id`, `content`; block 0).
/// - OpenAI Responses: items of type `function_call` / `custom_tool_call` (`call_id`,
///   `name`, `arguments` or `input`) and `function_call_output` /
///   `custom_tool_call_output` (`call_id`, `output`); block 0.
/// - Gemini: `parts` holding `functionCall` (`name`, `args`, maybe `id`) and
///   `functionResponse` (`name`, `response`, maybe `id`); the block is the part's index. A
///   call without an id is known by its name, so a response pairs with the nearest call
///   of that name before it; a response holding `error` failed.
pub fn scan_message(b: &[u8]) -> (Vec<RawCall<'_>>, Vec<RawResult<'_>>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    if !MARKERS.iter().any(|m| memchr::memmem::find(b, m).is_some()) {
        return (calls, results);
    }
    let Ok(ms) = json::members(b, &(0..b.len())) else { return (calls, results) };
    let s_in = |o: &[json::Member], name: &str| json::find_member(b, o, name).filter(|m| b[m.value.start] == b'"').map(|m| json::inner(b, &m.value));
    let obj = |o: &[json::Member], name: &str| json::find_member(b, o, name).filter(|m| b[m.value.start] == b'{').and_then(|m| json::members(b, &m.value).ok());
    let arr = |o: &[json::Member], name: &str| json::find_member(b, o, name).and_then(|m| json::elements(b, &m.value).ok()).map(|x| x.0);

    // Anthropic content blocks
    if let Some(els) = arr(&ms, "content") {
        for (k, el) in els.iter().enumerate() {
            if b[el.start] != b'{' {
                continue;
            }
            let Ok(bm) = json::members(b, el) else { continue };
            let Some(ty) = s_in(&bm, "type") else { continue };
            if ty == b"tool_use" {
                let (Some(id), Some(name)) = (s_in(&bm, "id"), s_in(&bm, "name")) else { continue };
                let fields = json::find_member(b, &bm, "input").map(|m| object_fields(b, &m.value)).unwrap_or_default();
                calls.push(RawCall { block: k as u16, id, name, fields });
            } else if ty == b"tool_result" {
                let Some(id) = s_in(&bm, "tool_use_id") else { continue };
                let is_error = json::find_member(b, &bm, "is_error").is_some_and(|m| &b[m.value.clone()] == b"true");
                let content = json::find_member(b, &bm, "content").map(|m| m.value.clone()).unwrap_or(el.end..el.end);
                results.push(RawResult { block: k as u16, tool_use_id: id, is_error, content });
            }
        }
    }
    // OpenAI Chat Completions
    if let Some(els) = arr(&ms, "tool_calls") {
        for (k, el) in els.iter().enumerate() {
            if b[el.start] != b'{' {
                continue;
            }
            let Ok(cm) = json::members(b, el) else { continue };
            let (Some(id), Some(f)) = (s_in(&cm, "id"), obj(&cm, "function")) else { continue };
            let Some(name) = s_in(&f, "name") else { continue };
            let fields = json::find_member(b, &f, "arguments").map(|m| string_fields(b, &m.value)).unwrap_or_default();
            calls.push(RawCall { block: k as u16, id, name, fields });
        }
    }
    if let Some(id) = s_in(&ms, "tool_call_id") {
        let content = json::find_member(b, &ms, "content").map(|m| m.value.clone()).unwrap_or(b.len()..b.len());
        results.push(RawResult { block: 0, tool_use_id: id, is_error: false, content });
    }
    // OpenAI Responses items
    match s_in(&ms, "type") {
        Some(t @ (b"function_call" | b"custom_tool_call")) => {
            if let (Some(id), Some(name)) = (s_in(&ms, "call_id"), s_in(&ms, "name")) {
                let input = if t == b"function_call" { "arguments" } else { "input" };
                let fields = json::find_member(b, &ms, input).map(|m| string_fields(b, &m.value)).unwrap_or_default();
                calls.push(RawCall { block: 0, id, name, fields });
            }
        }
        Some(b"function_call_output" | b"custom_tool_call_output") => {
            if let Some(id) = s_in(&ms, "call_id") {
                let content = json::find_member(b, &ms, "output").map(|m| m.value.clone()).unwrap_or(b.len()..b.len());
                results.push(RawResult { block: 0, tool_use_id: id, is_error: false, content });
            }
        }
        _ => {}
    }
    // Gemini parts
    if let Some(els) = arr(&ms, "parts") {
        for (k, el) in els.iter().enumerate() {
            if b[el.start] != b'{' {
                continue;
            }
            let Ok(pm) = json::members(b, el) else { continue };
            if let Some(fc) = obj(&pm, "functionCall") {
                let Some(name) = s_in(&fc, "name") else { continue };
                let id = s_in(&fc, "id").unwrap_or(name);
                let fields = json::find_member(b, &fc, "args").map(|m| object_fields(b, &m.value)).unwrap_or_default();
                calls.push(RawCall { block: k as u16, id, name, fields });
            } else if let Some(fr) = json::find_member(b, &pm, "functionResponse").filter(|m| b[m.value.start] == b'{') {
                let Ok(frm) = json::members(b, &fr.value) else { continue };
                let Some(name) = s_in(&frm, "name") else { continue };
                let id = s_in(&frm, "id").unwrap_or(name);
                let resp = json::find_member(b, &frm, "response");
                let is_error = resp.as_ref().filter(|m| b[m.value.start] == b'{').and_then(|m| json::members(b, &m.value).ok()).is_some_and(|rm| json::find_member(b, &rm, "error").is_some());
                let content = resp.map(|m| m.value.clone()).unwrap_or(fr.value.clone());
                results.push(RawResult { block: k as u16, tool_use_id: id, is_error, content });
            }
        }
    }
    (calls, results)
}

/// The text of the tool result in block `block` of a message: a string itself, the text
/// blocks of a list joined by newlines, or any other JSON value as its text (Gemini's
/// `response` object). `None` when the message has no such result or the result has no text.
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
                if !json::find_member(b, &ms, "type").is_some_and(|t| matches!(json::inner(b, &t.value), b"text" | b"input_text" | b"output_text")) {
                    continue;
                }
                if let Some(t) = json::find_member(b, &ms, "text").filter(|m| b[m.value.start] == b'"') {
                    texts.push(crate::text::unescape(json::inner(b, &t.value)));
                }
            }
            (!texts.is_empty()).then(|| texts.join("\n"))
        }
        b'{' => Some(String::from_utf8_lossy(&b[c.clone()]).into_owned()),
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
                        // a decoded value is short and is not a range of the message
                        off: match &v {
                            Cow::Borrowed(x) => (x.as_ptr() as usize - base) as u32,
                            Cow::Owned(_) => 0,
                        },
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
/// it answers in an earlier message of the same request. Walks the parsed JSON, in each of
/// the shapes [`scan_message`] reads.
pub fn reference_steps(body: &[u8]) -> Vec<RefStep> {
    let Ok(root) = json::parse_all(body) else { return Vec::new() };
    let json::Node::Obj(top) = &root else { return Vec::new() };
    fn get<'n>(body: &[u8], o: &'n [(json::Span, json::Node)], name: &str) -> Option<&'n json::Node> {
        o.iter().find(|(k, _)| json::key_is(body, k, name)).map(|(_, v)| v)
    }
    let Some(msgs) = ["messages", "contents", "input"].iter().find_map(|k| match get(body, top, k) {
        Some(json::Node::Arr(a)) => Some(a),
        _ => None,
    }) else {
        return Vec::new();
    };
    let tok = |n: Option<&json::Node>| match n {
        Some(json::Node::Tok(s)) if body[s.start] == b'"' => Some(json::inner(body, s).to_vec()),
        _ => None,
    };
    let canon = |n: &json::Node| {
        let mut t = Vec::new();
        json::write_canonical(body, n, &mut t);
        t
    };
    // an object input: canonical values, `None` past MAX_VALUE
    let obj_fields = |n: Option<&json::Node>| {
        let mut fields = Vec::new();
        if let Some(json::Node::Obj(im)) = n {
            for (k, v) in im {
                let text = canon(v);
                fields.push((json::inner(body, k).to_vec(), if text.len() <= MAX_VALUE { Some(text) } else { None }));
            }
        }
        fields.sort();
        fields
    };
    // an input sent as a JSON string: its decoded values as written, long ones left out
    let str_fields = |n: Option<&json::Node>| {
        let mut fields = Vec::new();
        if let Some(json::Node::Tok(s)) = n
            && body[s.start] == b'"'
        {
            let text = crate::text::unescape(json::inner(body, s)).into_bytes();
            if let Ok(json::Node::Obj(im)) = json::parse_all(&text) {
                for (k, v) in im {
                    let raw = match &v {
                        json::Node::Tok(x) => text[x.clone()].to_vec(),
                        other => {
                            let mut t = Vec::new();
                            json::write_canonical(&text, other, &mut t);
                            t
                        }
                    };
                    if raw.len() <= MAX_VALUE {
                        fields.push((json::inner(&text, &k).to_vec(), Some(raw)));
                    }
                }
            }
        }
        fields.sort();
        fields
    };
    let out_len = |n: Option<&json::Node>| n.map_or(0, |c| canon(c).len() as u32);
    let mut calls: HashMap<Vec<u8>, (Vec<u8>, Vec<(Vec<u8>, Option<Vec<u8>>)>)> = HashMap::new();
    let mut out = Vec::new();
    let mut result = |calls: &HashMap<Vec<u8>, (Vec<u8>, Vec<(Vec<u8>, Option<Vec<u8>>)>)>, id: Vec<u8>, failed: bool, out_bytes: u32| {
        if let Some((name, fields)) = calls.get(&id) {
            out.push(RefStep { tool_use_id: id, tool: name.clone(), failed, out_bytes, fields: fields.clone() });
        }
    };
    for m in msgs {
        let json::Node::Obj(mo) = m else { continue };
        // Anthropic content blocks
        if let Some(json::Node::Arr(blocks)) = get(body, mo, "content") {
            for blk in blocks {
                let json::Node::Obj(bo) = blk else { continue };
                match tok(get(body, bo, "type")).as_deref() {
                    Some(b"tool_use") => {
                        let (Some(id), Some(name)) = (tok(get(body, bo, "id")), tok(get(body, bo, "name"))) else { continue };
                        calls.insert(id, (name, obj_fields(get(body, bo, "input"))));
                    }
                    Some(b"tool_result") => {
                        let Some(id) = tok(get(body, bo, "tool_use_id")) else { continue };
                        let failed = matches!(get(body, bo, "is_error"), Some(json::Node::Tok(s)) if &body[s.clone()] == b"true");
                        // compact length of the content; equal to the engine's raw length for compact bodies
                        result(&calls, id, failed, out_len(get(body, bo, "content")));
                    }
                    _ => {}
                }
            }
        }
        // OpenAI Chat Completions
        if let Some(json::Node::Arr(tcs)) = get(body, mo, "tool_calls") {
            for tc in tcs {
                let json::Node::Obj(co) = tc else { continue };
                let (Some(id), Some(json::Node::Obj(f))) = (tok(get(body, co, "id")), get(body, co, "function")) else { continue };
                let Some(name) = tok(get(body, f, "name")) else { continue };
                calls.insert(id, (name, str_fields(get(body, f, "arguments"))));
            }
        }
        if let Some(id) = tok(get(body, mo, "tool_call_id")) {
            result(&calls, id, false, out_len(get(body, mo, "content")));
        }
        // OpenAI Responses items
        match tok(get(body, mo, "type")).as_deref() {
            Some(t @ (b"function_call" | b"custom_tool_call")) => {
                if let (Some(id), Some(name)) = (tok(get(body, mo, "call_id")), tok(get(body, mo, "name"))) {
                    let input = if t == b"function_call" { "arguments" } else { "input" };
                    calls.insert(id, (name, str_fields(get(body, mo, input))));
                }
            }
            Some(b"function_call_output" | b"custom_tool_call_output") => {
                if let Some(id) = tok(get(body, mo, "call_id")) {
                    result(&calls, id, false, out_len(get(body, mo, "output")));
                }
            }
            _ => {}
        }
        // Gemini parts
        if let Some(json::Node::Arr(parts)) = get(body, mo, "parts") {
            for p in parts {
                let json::Node::Obj(po) = p else { continue };
                if let Some(json::Node::Obj(fc)) = get(body, po, "functionCall") {
                    let Some(name) = tok(get(body, fc, "name")) else { continue };
                    let id = tok(get(body, fc, "id")).unwrap_or_else(|| name.clone());
                    calls.insert(id, (name, obj_fields(get(body, fc, "args"))));
                } else if let Some(json::Node::Obj(fr)) = get(body, po, "functionResponse") {
                    let Some(name) = tok(get(body, fr, "name")) else { continue };
                    let id = tok(get(body, fr, "id")).unwrap_or_else(|| name.clone());
                    let resp = get(body, fr, "response");
                    let failed = matches!(resp, Some(json::Node::Obj(r)) if get(body, r, "error").is_some());
                    result(&calls, id, failed, out_len(resp));
                }
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
        assert_eq!(fields(&c[0]), vec![("command", "\"ls\""), ("n", "3")]);
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

    fn fields<'a>(c: &'a RawCall) -> Vec<(&'a str, &'a str)> {
        c.fields.iter().map(|(k, v)| (std::str::from_utf8(k).unwrap(), std::str::from_utf8(v).unwrap())).collect()
    }

    #[test]
    fn openai_chat_calls_and_results() {
        let a = br#"{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"run_shell","arguments":"{\"command\":\"ls -la\",\"n\":3}"}},{"id":"call_2","type":"function","function":{"name":"read","arguments":"{}"}}]}"#;
        let (c, r) = scan_message(a);
        assert!(r.is_empty());
        assert_eq!(c.len(), 2);
        assert_eq!((c[0].block, c[0].id, c[0].name), (0, &b"call_1"[..], &b"run_shell"[..]));
        assert_eq!(fields(&c[0]), vec![("command", "\"ls -la\""), ("n", "3")]);
        assert_eq!((c[1].block, c[1].name), (1, &b"read"[..]));
        // decoded values are not ranges of the message
        let ex = extract(a);
        assert_eq!(ex.calls[0].3[0].val, b"\"ls -la\"");
        let t = br#"{"role":"tool","tool_call_id":"call_1","content":"total 0"}"#;
        let (c, r) = scan_message(t);
        assert!(c.is_empty());
        assert_eq!((r[0].block, r[0].tool_use_id, r[0].is_error), (0, &b"call_1"[..], false));
        assert_eq!(result_text(t, 0).as_deref(), Some("total 0"));
    }

    #[test]
    fn responses_items() {
        let a = br#"{"type":"function_call","call_id":"fc_1","name":"get_weather","arguments":"{\"city\":\"Paris\"}","id":"x"}"#;
        let (c, _) = scan_message(a);
        assert_eq!((c[0].id, c[0].name), (&b"fc_1"[..], &b"get_weather"[..]));
        assert_eq!(fields(&c[0]), vec![("city", "\"Paris\"")]);
        let o = br#"{"type":"function_call_output","call_id":"fc_1","output":"sunny"}"#;
        let (_, r) = scan_message(o);
        assert_eq!(r[0].tool_use_id, b"fc_1");
        assert_eq!(result_text(o, 0).as_deref(), Some("sunny"));
    }

    #[test]
    fn gemini_parts() {
        let a = br#"{"role":"model","parts":[{"text":"checking"},{"functionCall":{"name":"lookup","args":{"q":"rust","k":2}}}]}"#;
        let (c, _) = scan_message(a);
        assert_eq!((c[0].block, c[0].id, c[0].name), (1, &b"lookup"[..], &b"lookup"[..]));
        assert_eq!(fields(&c[0]), vec![("q", "\"rust\""), ("k", "2")]);
        let u = br#"{"role":"user","parts":[{"functionResponse":{"name":"lookup","response":{"error":"timeout"}}}]}"#;
        let (_, r) = scan_message(u);
        assert_eq!((r[0].tool_use_id, r[0].is_error), (&b"lookup"[..], true));
        assert_eq!(result_text(u, 0).as_deref(), Some(r#"{"error":"timeout"}"#));
    }
}
