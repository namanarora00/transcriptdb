//! Step summaries kept in each segment, written with it and never changed.
//!
//! Queries of the form `where ... | summarize ... by ...` over tool, outcome, session, day
//! and one input field can be answered from these instead of from every step:
//!
//! * **Part table.** For each session part in the segment: one row per (tool, outcome, UTC
//!   day) with the number of steps, the sum, minimum and maximum of their result sizes, and
//!   their earliest and latest time.
//! * **Field tables.** For each input field: the segment's distinct short values (stored
//!   once however many parts use them), one row per (value, part, tool, outcome, day) with
//!   the number of steps and the sum of their result sizes, and every step whose value is
//!   too long for the dictionary, by where it is (its text stays in the message).
//! * **Word index.** Per field, the words of every string value (long ones included):
//!   lower-cased runs of ASCII letters and digits and non-ASCII characters, three bytes or
//!   longer, each with the values that hold it. A `contains`, `has`, `startswith`,
//!   `endswith` or `==` test with a literal then reads only the values whose words can
//!   hold the pattern, and confirms each on its text.
//!
//! Everything here is derived from the parts the segment holds, so a merge rebuilds it for
//! its output and recovery needs nothing: segments are written whole or not at all.

use crate::block::BlockRef;
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::segment::StepPart;
use crate::steps;
use std::collections::HashMap;

pub const DAY_MS: i64 = 86_400_000;
/// Words shorter than this are not indexed.
pub const MIN_WORD: usize = 3;
/// Raw bytes per block of values.
const VAL_CHUNK: usize = 64 * 1024;
const VERSION: u8 = 1;

/// Steps of one part with one tool, outcome and day.
#[derive(Debug, Clone, Copy, Default)]
pub struct PartRow {
    pub part: u32,
    /// The segment's tool code.
    pub tool: u32,
    pub failed: bool,
    pub day: i64,
    pub count: u32,
    pub sum_out: u64,
    pub min_out: u32,
    pub max_out: u32,
    pub min_time: i64,
    pub max_time: i64,
}

/// Steps of one part with one value of a field, one tool, outcome and day.
#[derive(Debug, Clone, Copy, Default)]
pub struct ValRow {
    pub part: u32,
    pub tool: u32,
    pub failed: bool,
    pub day: i64,
    pub count: u32,
    pub sum_out: u64,
}

/// A step whose value of the field is read from where it is stored: a long value (from
/// the message), or any value of a field listed by step (from the part's step data).
#[derive(Debug, Clone, Copy, Default)]
pub struct LongStep {
    pub part: u32,
    /// Step number in its session.
    pub pos: u32,
    pub tool: u32,
    pub failed: bool,
    pub day: i64,
    pub out_bytes: u32,
    /// For a dictionary value: where its text is in the part's encoded step data (see
    /// [`PartInfo::steps`]) and how long it is; `vlen` 0 for a long value.
    pub voff: u32,
    pub vlen: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PartInfo {
    pub session: u32,
    /// Index into [`StatsHead::sids`] and [`StatsHead::agents`].
    pub sid: u32,
    pub agent: u32,
    /// The part's steps in its session.
    pub first_row: u32,
    pub n_rows: u32,
    /// Where the part's encoded step data is: a segment block and the offset in it.
    pub steps: (BlockRef, u32),
}

/// Where one field's table is.
#[derive(Debug, Clone, Default)]
pub struct FieldDir {
    /// The segment's field code.
    pub field: u32,
    pub n_values: u32,
    pub n_longs: u32,
    /// (first value id, block) per block of values.
    pub chunks: Vec<(u32, BlockRef)>,
    pub rows: BlockRef,
    pub words: BlockRef,
    pub post: BlockRef,
}

/// The head of a segment's summaries: parts, the part table, and where each field's table is.
#[derive(Debug, Clone, Default)]
pub struct StatsHead {
    pub parts: Vec<PartInfo>,
    pub sids: Vec<String>,
    pub agents: Vec<String>,
    /// Rows of part `p` are `rows[row_start[p]..row_start[p + 1]]`.
    pub row_start: Vec<u32>,
    pub rows: Vec<PartRow>,
    pub fields: Vec<FieldDir>,
}

impl StatsHead {
    pub fn bytes(&self) -> usize {
        256 + self.parts.len() * 12 + self.rows.len() * std::mem::size_of::<PartRow>() + self.sids.iter().chain(&self.agents).map(|s| s.len() + 24).sum::<usize>() + self.fields.iter().map(|f| 96 + f.chunks.len() * 32).sum::<usize>()
    }

    pub fn field(&self, seg_field: u32) -> Option<&FieldDir> {
        self.fields.iter().find(|f| f.field == seg_field)
    }
}

/// One field's rows: rows of value `v` are `rows[start[v]..start[v + 1]]`.
#[derive(Debug, Clone, Default)]
pub struct FieldRows {
    pub start: Vec<u32>,
    pub rows: Vec<ValRow>,
    /// The value of each row, and the rows in tool order: rows with segment tool code `t`
    /// are `by_tool[tool_start[t]..tool_start[t + 1]]` (built when read).
    pub value: Vec<u32>,
    pub tool_start: Vec<u32>,
    pub by_tool: Vec<u32>,
    pub longs: Vec<LongStep>,
    /// Ids (as in postings) of values that are not JSON strings: the word index says
    /// nothing about them, so they are always tested.
    pub unindexed: Vec<u32>,
    /// The value `null`, if the field has it (`isnull` and `isnotnull` need nothing else).
    pub null_value: Option<u32>,
    /// Per value, the first value with the same decoded value (its group in `summarize ...
    /// by` and `dcount`; see `query::value::group_key`), or `u32::MAX` for `null`.
    pub group: Vec<u32>,
    /// Parts with a step that gives this field twice: tests disagree on which copy counts,
    /// so queries on the field read those parts from their steps.
    pub inexact: Vec<u32>,
}

impl FieldRows {
    pub fn bytes(&self) -> usize {
        64 + self.start.len() * 4 + self.rows.len() * (std::mem::size_of::<ValRow>() + 8) + self.tool_start.len() * 4 + self.longs.len() * std::mem::size_of::<LongStep>() + self.unindexed.len() * 4 + self.group.len() * 4
    }
}

/// Values `first..first + n` of a field, as JSON text.
#[derive(Debug, Clone, Default)]
pub struct ValChunk {
    pub first: u32,
    pub buf: Vec<u8>,
    pub off: Vec<u32>,
}

impl ValChunk {
    pub fn get(&self, v: u32) -> &[u8] {
        let i = (v - self.first) as usize;
        &self.buf[self.off[i] as usize..self.off[i + 1] as usize]
    }
    pub fn len(&self) -> usize {
        self.off.len() - 1
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The word list of a field, sorted, each word followed by a zero byte (so a substring
/// search runs over the whole list at once), with where each word's postings are.
#[derive(Debug, Clone, Default)]
pub struct Words {
    pub buf: Vec<u8>,
    /// Word `i` is `buf[start[i]..start[i + 1] - 1]`.
    pub start: Vec<u32>,
    /// Postings of word `i` are `post[post_off[i]..post_off[i + 1]]`.
    pub post_off: Vec<u32>,
}

impl Words {
    pub fn len(&self) -> usize {
        self.start.len().saturating_sub(1)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn word(&self, i: usize) -> &[u8] {
        &self.buf[self.start[i] as usize..self.start[i + 1] as usize - 1]
    }
    /// The word containing byte `o` of `buf`.
    fn at(&self, o: usize) -> usize {
        self.start.partition_point(|&s| s as usize <= o) - 1
    }
    /// Words for which `want` holds of `t`: equal, starting with it, ending with it, or containing it.
    pub fn find(&self, t: &[u8], how: WordMatch) -> Vec<usize> {
        let n = self.len();
        match how {
            WordMatch::Exact | WordMatch::Prefix => {
                let (mut a, mut b) = (0, n);
                while a < b {
                    let m = (a + b) / 2;
                    if self.word(m) < t {
                        a = m + 1;
                    } else {
                        b = m;
                    }
                }
                let mut out = Vec::new();
                for i in a..n {
                    let w = self.word(i);
                    if !w.starts_with(t) {
                        break;
                    }
                    if how == WordMatch::Prefix || w == t {
                        out.push(i);
                    }
                }
                out
            }
            WordMatch::Suffix | WordMatch::Contains => {
                let mut out: Vec<usize> = Vec::new();
                for o in memchr::memmem::find_iter(&self.buf, t) {
                    let i = self.at(o);
                    if how == WordMatch::Suffix && o + t.len() + 1 != self.start[i + 1] as usize {
                        continue;
                    }
                    if out.last() != Some(&i) {
                        out.push(i);
                    }
                }
                out
            }
        }
    }
    pub fn bytes(&self) -> usize {
        64 + self.buf.len() + self.start.len() * 4 + self.post_off.len() * 4
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WordMatch {
    Exact,
    Prefix,
    Suffix,
    Contains,
}

/// The ids (value ids, then `n_values + i` for long step `i`) in one posting list.
pub fn postings(post: &[u8], out: &mut Vec<u32>) -> Result<()> {
    let mut d = Dec::new(post);
    let mut id = 0u32;
    let mut first = true;
    while !d.done() {
        let x = d.var32()?;
        id = if first { x } else { id + x };
        first = false;
        out.push(id);
    }
    Ok(())
}

/// The text the word index sees for a value given as JSON: string values decoded (as the
/// string tests decode them) and lower-cased (as case-insensitive tests compare them).
/// `None` for anything that is not a string: such values are always tested directly.
pub fn index_text(raw: &[u8]) -> Option<String> {
    if raw.len() < 2 || raw[0] != b'"' || raw[raw.len() - 1] != b'"' {
        return None;
    }
    let body = &raw[1..raw.len() - 1];
    let s = if body.contains(&b'\\') { crate::query::value::json_unescape(body) } else { String::from_utf8_lossy(body).into_owned() };
    Some(lower(&s))
}

/// Lower case as the case-insensitive string tests do it.
pub fn lower(s: &str) -> String {
    if s.is_ascii() { s.to_ascii_lowercase() } else { s.to_lowercase() }
}

#[inline]
fn word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b >= 0x80
}

/// Each indexed word of lower-cased text.
pub fn words(text: &str, mut f: impl FnMut(&[u8])) {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if !word_byte(b[i]) {
            i += 1;
            continue;
        }
        let a = i;
        while i < b.len() && word_byte(b[i]) {
            i += 1;
        }
        if i - a >= MIN_WORD {
            f(&b[a..i]);
        }
    }
}

/// The words of a lower-cased pattern, each with how a word of a matching value must
/// relate to it: a word inside the pattern is a whole word of the value; the first may be
/// the end of one, the last the start of one, a lone one anywhere inside one. Words too
/// short to be indexed are left out.
pub fn pattern_words(p: &str) -> Vec<(Vec<u8>, WordMatch)> {
    let b = p.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if !word_byte(b[i]) {
            i += 1;
            continue;
        }
        let a = i;
        while i < b.len() && word_byte(b[i]) {
            i += 1;
        }
        let (open_left, open_right) = (a == 0, i == b.len());
        let how = match (open_left, open_right) {
            (false, false) => WordMatch::Exact,
            (false, true) => WordMatch::Prefix,
            (true, false) => WordMatch::Suffix,
            (true, true) => WordMatch::Contains,
        };
        if i - a >= MIN_WORD {
            out.push((b[a..i].to_vec(), how));
        }
    }
    out
}

// ---------------------------------------------------------------------------- writing

/// A field whose distinct values average at least this many bytes keeps no copy of them:
/// its table lists the steps that have it, and their values are read from the sessions'
/// own dictionaries (long text such as commands and file contents would otherwise be
/// stored twice).
pub const TEXT_MEAN: usize = 256;

/// One step's value of a field: a dictionary value id, or `LONG | i` for long value `i`.
const LONG: u32 = 1 << 31;

#[derive(Default)]
struct FieldBuild {
    vals: Vec<Vec<u8>>,
    ix: HashMap<Vec<u8>, u32>,
    /// Every step with the field (first copy of a key given twice).
    steps: Vec<(LongStep, u32)>,
    /// Words of each long value's text (`None`: not a string).
    long_words: Vec<Option<Vec<Vec<u8>>>>,
    inexact: Vec<u32>,
}

/// Collects the summaries of a segment as its parts are added.
#[derive(Default)]
pub struct StatsBuilder {
    parts: Vec<PartInfo>,
    sids: Vec<String>,
    sid_ix: HashMap<String, u32>,
    agents: Vec<String>,
    agent_ix: HashMap<String, u32>,
    row_start: Vec<u32>,
    rows: Vec<PartRow>,
    fields: HashMap<u32, FieldBuild>,
}

fn intern(v: &mut Vec<String>, ix: &mut HashMap<String, u32>, s: &str) -> u32 {
    if let Some(&i) = ix.get(s) {
        return i;
    }
    v.push(s.to_string());
    ix.insert(s.to_string(), v.len() as u32 - 1);
    v.len() as u32 - 1
}

fn word_list(t: &str) -> Vec<Vec<u8>> {
    let mut ws: Vec<Vec<u8>> = Vec::new();
    words(t, |w| ws.push(w.to_vec()));
    ws.sort_unstable();
    ws.dedup();
    ws
}

impl StatsBuilder {
    /// Add the next part (parts come in directory order). Tool and field codes in `sp` are
    /// the segment's; `long_text` is the JSON text of each of `sp.longs`.
    pub fn add_part(&mut self, session: u32, key: &(String, String), sp: &StepPart, long_text: &[Vec<u8>], value_at: &[u32]) -> Result<()> {
        if long_text.len() != sp.longs.len() || value_at.len() != sp.values.len() {
            return corrupt("step part without the text of its long values or where its values are");
        }
        let part = self.parts.len() as u32;
        let sid = intern(&mut self.sids, &mut self.sid_ix, &key.0);
        let agent = intern(&mut self.agents, &mut self.agent_ix, &key.1);
        self.row_start.push(self.rows.len() as u32);
        let mut a: HashMap<(u32, bool, i64), PartRow> = HashMap::new();
        let mut seen: Vec<u32> = Vec::new();
        for (i, (r, fs)) in sp.rows.iter().zip(&sp.fields).enumerate() {
            // times are stored clamped at zero; summarize what reads will see
            let t = r.time_ms.max(0);
            let day = t.div_euclid(DAY_MS);
            let e = a.entry((r.tool, r.failed, day)).or_insert(PartRow { part, tool: r.tool, failed: r.failed, day, min_out: u32::MAX, max_out: 0, min_time: i64::MAX, max_time: i64::MIN, ..PartRow::default() });
            e.count += 1;
            e.sum_out += r.out_bytes as u64;
            e.min_out = e.min_out.min(r.out_bytes);
            e.max_out = e.max_out.max(r.out_bytes);
            e.min_time = e.min_time.min(t);
            e.max_time = e.max_time.max(t);
            seen.clear();
            for &(f, v) in fs {
                let fb = self.fields.entry(f).or_default();
                if seen.contains(&f) {
                    if fb.inexact.last() != Some(&part) {
                        fb.inexact.push(part);
                    }
                    continue;
                }
                seen.push(f);
                let mut step = LongStep { part, pos: sp.first + i as u32, tool: r.tool, failed: r.failed, day, out_bytes: r.out_bytes, voff: 0, vlen: 0 };
                if steps::is_long(v) {
                    let li = (v - steps::LONG_BASE) as usize;
                    fb.long_words.push(index_text(&long_text[li]).map(|t| word_list(&t)));
                    fb.steps.push((step, LONG | (fb.long_words.len() as u32 - 1)));
                } else {
                    let text = &sp.values.get(v as usize).ok_or_else(|| crate::codec::Error::Corrupt("step names a missing value".into()))?.1;
                    step.voff = value_at[v as usize];
                    step.vlen = text.len() as u32;
                    let id = match fb.ix.get(text.as_slice()) {
                        Some(&id) => id,
                        None => {
                            fb.vals.push(text.clone());
                            fb.ix.insert(text.clone(), fb.vals.len() as u32 - 1);
                            fb.vals.len() as u32 - 1
                        }
                    };
                    fb.steps.push((step, id));
                }
            }
        }
        let mut v: Vec<PartRow> = a.into_values().collect();
        v.sort_by_key(|r| (r.tool, r.failed, r.day));
        self.rows.extend(v);
        self.parts.push(PartInfo { session, sid, agent, first_row: sp.first, n_rows: sp.rows.len() as u32, steps: (BlockRef::default(), 0) });
        Ok(())
    }

    /// Write the summaries with `put` (which writes one block and says where) and return
    /// where the head is.
    /// `steps` says where each part's encoded step data ended up (in directory order).
    pub fn finish(self, steps: &[crate::segment::PartRef], put: &mut dyn FnMut(&[u8]) -> Result<BlockRef>) -> Result<BlockRef> {
        if steps.len() != self.parts.len() {
            return corrupt("summaries for a different number of parts");
        }
        let mut dirs = Vec::new();
        let mut fields: Vec<(u32, FieldBuild)> = self.fields.into_iter().collect();
        fields.sort_by_key(|x| x.0);
        for (f, fb) in fields {
            let text_bytes: usize = fb.vals.iter().map(|v| v.len()).sum();
            let by_step = !fb.vals.is_empty() && text_bytes / fb.vals.len() >= TEXT_MEAN;
            // the values (none when the field is listed by step), then the steps read from
            // the sessions: long values, or every step when listed by step
            let vals: &[Vec<u8>] = if by_step { &[] } else { &fb.vals };
            let nv = vals.len() as u32;
            let listed: Vec<&(LongStep, u32)> = fb.steps.iter().filter(|(_, v)| by_step || v & LONG != 0).collect();
            let mut chunks = Vec::new();
            let mut i = 0usize;
            while i < vals.len() {
                let mut e = Enc::new();
                let first = i;
                let mut size = 0;
                while i < vals.len() && (i == first || size < VAL_CHUNK) {
                    size += vals[i].len() + 2;
                    i += 1;
                }
                e.var((i - first) as u64);
                for v in &vals[first..i] {
                    e.bytes(v);
                }
                chunks.push((first as u32, put(&e.buf)?));
            }
            // rows, by value: (value, part, tool, failed, day) -> (count, sum of result sizes)
            let mut agg: HashMap<(u32, u32, u32, bool, i64), (u32, u64)> = HashMap::new();
            if !by_step {
                for (st, v) in &fb.steps {
                    if v & LONG == 0 {
                        let e = agg.entry((*v, st.part, st.tool, st.failed, st.day)).or_insert((0, 0));
                        e.0 += 1;
                        e.1 += st.out_bytes as u64;
                    }
                }
            }
            let mut rows: Vec<((u32, u32, u32, bool, i64), (u32, u64))> = agg.into_iter().collect();
            rows.sort_unstable_by_key(|x| x.0);
            let mut e = Enc::new();
            e.var(nv as u64);
            let mut k = 0;
            for v in 0..nv {
                let a = k;
                while k < rows.len() && rows[k].0 .0 == v {
                    k += 1;
                }
                e.var((k - a) as u64);
            }
            e.var(rows.len() as u64);
            for ((_, part, tool, failed, day), (count, sum)) in &rows {
                e.var(*part as u64);
                e.var(*tool as u64);
                e.u8(*failed as u8);
                e.var(*day as u64);
                e.var(*count as u64);
                e.var(*sum);
            }
            e.var(listed.len() as u64);
            for (l, _) in &listed {
                e.var(l.part as u64);
                e.var(l.pos as u64);
                e.var(l.tool as u64);
                e.u8(l.failed as u8);
                e.var(l.day as u64);
                e.var(l.out_bytes as u64);
                e.var(l.voff as u64);
                e.var(l.vlen as u64);
            }
            // words of each value and listed step; ids are value ids, then nv + listed index
            let val_words: Vec<Option<Vec<Vec<u8>>>> = fb.vals.iter().map(|v| index_text(v).map(|t| word_list(&t))).collect();
            let words_of = |v: u32| -> &Option<Vec<Vec<u8>>> { if v & LONG != 0 { &fb.long_words[(v & !LONG) as usize] } else { &val_words[v as usize] } };
            let mut unindexed: Vec<u32> = (0..nv).filter(|&i| val_words[i as usize].is_none()).collect();
            unindexed.extend(listed.iter().enumerate().filter(|(_, (_, v))| words_of(*v).is_none()).map(|(i, _)| nv + i as u32));
            e.var(unindexed.len() as u64);
            for id in &unindexed {
                e.var(*id as u64);
            }
            e.var(if by_step { 0 } else { fb.ix.get(b"null".as_slice()).map_or(0, |&i| i as u64 + 1) });
            // values whose group is another value's (or none): (value, group + 1, or 0)
            let mut first: HashMap<Vec<u8>, u32> = HashMap::new();
            let mut odd: Vec<(u32, u32)> = Vec::new();
            for (i, v) in vals.iter().enumerate() {
                match crate::query::value::group_key(v) {
                    None => odd.push((i as u32, 0)),
                    Some(k) => {
                        let g = *first.entry(k).or_insert(i as u32);
                        if g != i as u32 {
                            odd.push((i as u32, g + 1));
                        }
                    }
                }
            }
            e.var(odd.len() as u64);
            for (v, g) in &odd {
                e.var(*v as u64);
                e.var(*g as u64);
            }
            e.var(fb.inexact.len() as u64);
            for p in &fb.inexact {
                e.var(*p as u64);
            }
            let rows_ref = put(&e.buf)?;
            // words: each with the ids holding it, ascending
            let mut post: HashMap<&[u8], Vec<u32>> = HashMap::new();
            for v in 0..nv {
                for w in val_words[v as usize].iter().flatten() {
                    post.entry(w).or_default().push(v);
                }
            }
            for (i, (_, v)) in listed.iter().enumerate() {
                for w in words_of(*v).iter().flatten() {
                    post.entry(w).or_default().push(nv + i as u32);
                }
            }
            let mut ws: Vec<(&[u8], Vec<u32>)> = post.into_iter().collect();
            ws.sort_unstable_by(|a, b| a.0.cmp(b.0));
            let mut we = Enc::new();
            let mut pe = Enc::new();
            we.var(ws.len() as u64);
            for (w, ids) in &ws {
                let at = pe.buf.len();
                let mut prev = 0u32;
                for (j, &id) in ids.iter().enumerate() {
                    pe.var(if j == 0 { id as u64 } else { (id - prev) as u64 });
                    prev = id;
                }
                we.bytes(w);
                we.var((pe.buf.len() - at) as u64);
            }
            let words_ref = put(&we.buf)?;
            let post_ref = put(&pe.buf)?;
            dirs.push(FieldDir { field: f, n_values: nv, n_longs: listed.len() as u32, chunks, rows: rows_ref, words: words_ref, post: post_ref });
        }
        let mut e = Enc::new();
        e.u8(VERSION);
        e.var(self.parts.len() as u64);
        for (p, st) in self.parts.iter().zip(steps) {
            e.var(p.session as u64);
            e.var(p.sid as u64);
            e.var(p.agent as u64);
            e.var(p.first_row as u64);
            e.var(p.n_rows as u64);
            st.0.encode(&mut e);
            e.var(st.1 as u64);
        }
        for list in [&self.sids, &self.agents] {
            e.var(list.len() as u64);
            for s in list {
                e.bytes(s.as_bytes());
            }
        }
        for p in 0..self.parts.len() {
            let end = self.row_start.get(p + 1).copied().unwrap_or(self.rows.len() as u32);
            e.var((end - self.row_start[p]) as u64);
        }
        for r in &self.rows {
            let base = r.day * DAY_MS;
            e.var(r.tool as u64);
            e.u8(r.failed as u8);
            e.var(r.day as u64);
            e.var(r.count as u64);
            e.var(r.sum_out);
            e.var(r.min_out as u64);
            e.var(r.max_out as u64);
            e.var((r.min_time - base) as u64);
            e.var((r.max_time - base) as u64);
        }
        e.var(dirs.len() as u64);
        for d in &dirs {
            e.var(d.field as u64);
            e.var(d.n_values as u64);
            e.var(d.n_longs as u64);
            e.var(d.chunks.len() as u64);
            for (first, r) in &d.chunks {
                e.var(*first as u64);
                r.encode(&mut e);
            }
            d.rows.encode(&mut e);
            d.words.encode(&mut e);
            d.post.encode(&mut e);
        }
        put(&e.buf)
    }
}

// ---------------------------------------------------------------------------- reading

pub fn decode_head(b: &[u8]) -> Result<StatsHead> {
    let mut d = Dec::new(b);
    if d.u8()? != VERSION {
        return corrupt("unknown summary version");
    }
    let np = d.var()? as usize;
    let mut h = StatsHead::default();
    for _ in 0..np {
        h.parts.push(PartInfo { session: d.var32()?, sid: d.var32()?, agent: d.var32()?, first_row: d.var32()?, n_rows: d.var32()?, steps: (BlockRef::decode(&mut d)?, d.var32()?) });
    }
    for list in [&mut h.sids, &mut h.agents] {
        for _ in 0..d.var()? {
            list.push(d.string()?);
        }
    }
    let mut counts = Vec::with_capacity(np);
    for _ in 0..np {
        counts.push(d.var32()?);
    }
    h.row_start.push(0);
    for (part, &n) in counts.iter().enumerate() {
        for _ in 0..n {
            let tool = d.var32()?;
            let failed = d.u8()? != 0;
            let day = d.var()? as i64;
            let base = day * DAY_MS;
            h.rows.push(PartRow {
                part: part as u32,
                tool,
                failed,
                day,
                count: d.var32()?,
                sum_out: d.var()?,
                min_out: d.var32()?,
                max_out: d.var32()?,
                min_time: base + d.var()? as i64,
                max_time: base + d.var()? as i64,
            });
        }
        h.row_start.push(h.rows.len() as u32);
    }
    for _ in 0..d.var()? {
        let mut f = FieldDir { field: d.var32()?, n_values: d.var32()?, n_longs: d.var32()?, ..FieldDir::default() };
        for _ in 0..d.var()? {
            f.chunks.push((d.var32()?, BlockRef::decode(&mut d)?));
        }
        f.rows = BlockRef::decode(&mut d)?;
        f.words = BlockRef::decode(&mut d)?;
        f.post = BlockRef::decode(&mut d)?;
        h.fields.push(f);
    }
    if !d.done() || h.parts.iter().any(|p| p.sid as usize >= h.sids.len() || p.agent as usize >= h.agents.len()) {
        return corrupt("bad summary head");
    }
    Ok(h)
}

pub fn decode_rows(b: &[u8], n_parts: usize) -> Result<FieldRows> {
    let mut d = Dec::new(b);
    let nv = d.var()? as usize;
    let mut start = Vec::with_capacity(nv + 1);
    start.push(0u32);
    for _ in 0..nv {
        let n = d.var32()?;
        start.push(start.last().unwrap() + n);
    }
    let nr = d.var()? as usize;
    if *start.last().unwrap() as usize != nr {
        return corrupt("bad summary rows");
    }
    let mut rows = Vec::with_capacity(nr);
    for _ in 0..nr {
        rows.push(ValRow { part: d.var32()?, tool: d.var32()?, failed: d.u8()? != 0, day: d.var()? as i64, count: d.var32()?, sum_out: d.var()? });
    }
    let nl = d.var()? as usize;
    let mut longs = Vec::with_capacity(nl);
    for _ in 0..nl {
        longs.push(LongStep { part: d.var32()?, pos: d.var32()?, tool: d.var32()?, failed: d.u8()? != 0, day: d.var()? as i64, out_bytes: d.var32()?, voff: d.var32()?, vlen: d.var32()? });
    }
    let nu = d.var()? as usize;
    let mut unindexed = Vec::with_capacity(nu);
    for _ in 0..nu {
        unindexed.push(d.var32()?);
    }
    let null_value = match d.var32()? {
        0 => None,
        n if (n as usize) <= nv => Some(n - 1),
        _ => return corrupt("bad summary rows"),
    };
    let mut group: Vec<u32> = (0..nv as u32).collect();
    for _ in 0..d.var()? {
        let (v, g) = (d.var32()? as usize, d.var32()?);
        if v >= nv || g as usize > nv {
            return corrupt("bad summary rows");
        }
        group[v] = if g == 0 { u32::MAX } else { g - 1 };
    }
    let mut inexact = Vec::new();
    for _ in 0..d.var()? {
        let p = d.var32()?;
        if p as usize >= n_parts {
            return corrupt("bad summary rows");
        }
        inexact.push(p);
    }
    if !d.done() || rows.iter().any(|r| r.part as usize >= n_parts) || longs.iter().any(|l| l.part as usize >= n_parts) || unindexed.iter().any(|&u| u as usize >= nv + nl) {
        return corrupt("bad summary rows");
    }
    let mut value = Vec::with_capacity(nr);
    for v in 0..nv {
        value.extend(std::iter::repeat_n(v as u32, (start[v + 1] - start[v]) as usize));
    }
    let nt = rows.iter().map(|r| r.tool as usize + 1).max().unwrap_or(0);
    let mut tool_start = vec![0u32; nt + 1];
    for r in &rows {
        tool_start[r.tool as usize + 1] += 1;
    }
    for t in 0..nt {
        tool_start[t + 1] += tool_start[t];
    }
    let mut fill = tool_start.clone();
    let mut by_tool = vec![0u32; nr];
    for (i, r) in rows.iter().enumerate() {
        by_tool[fill[r.tool as usize] as usize] = i as u32;
        fill[r.tool as usize] += 1;
    }
    Ok(FieldRows { start, rows, value, tool_start, by_tool, longs, unindexed, null_value, group, inexact })
}

pub fn decode_chunk(b: &[u8], first: u32) -> Result<ValChunk> {
    let mut d = Dec::new(b);
    let n = d.var()? as usize;
    let mut c = ValChunk { first, buf: Vec::with_capacity(b.len()), off: Vec::with_capacity(n + 1) };
    c.off.push(0);
    for _ in 0..n {
        c.buf.extend_from_slice(d.bytes()?);
        c.off.push(c.buf.len() as u32);
    }
    if !d.done() {
        return corrupt("bad summary values");
    }
    Ok(c)
}

pub fn decode_words(b: &[u8], post_len: usize) -> Result<Words> {
    let mut d = Dec::new(b);
    let n = d.var()? as usize;
    let mut w = Words { buf: Vec::with_capacity(b.len()), start: Vec::with_capacity(n + 1), post_off: Vec::with_capacity(n + 1) };
    w.post_off.push(0);
    for _ in 0..n {
        w.start.push(w.buf.len() as u32);
        w.buf.extend_from_slice(d.bytes()?);
        w.buf.push(0);
        let l = d.var32()?;
        w.post_off.push(w.post_off.last().unwrap() + l);
    }
    w.start.push(w.buf.len() as u32);
    if !d.done() || *w.post_off.last().unwrap() as usize != post_len {
        return corrupt("bad summary words");
    }
    Ok(w)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_words_say_how_values_must_hold_them() {
        assert_eq!(pattern_words("git commit"), vec![(b"git".to_vec(), WordMatch::Suffix), (b"commit".to_vec(), WordMatch::Prefix)]);
        assert_eq!(pattern_words("test"), vec![(b"test".to_vec(), WordMatch::Contains)]);
        assert_eq!(pattern_words(" src/main.rs"), vec![(b"src".to_vec(), WordMatch::Exact), (b"main".to_vec(), WordMatch::Exact)]);
        assert!(pattern_words("a b").is_empty());
        let mut v = Vec::new();
        words(&lower("Run CARGO test --all; été"), |w| v.push(String::from_utf8(w.to_vec()).unwrap()));
        assert_eq!(v, vec!["run", "cargo", "test", "all", "été"]);
    }

    #[test]
    fn word_lookups() {
        let mut e = Enc::new();
        let ws = ["cargo", "commit", "commits", "legit", "git"];
        let mut sorted = ws.to_vec();
        sorted.sort();
        e.var(sorted.len() as u64);
        for w in &sorted {
            e.bytes(w.as_bytes());
            e.var(1);
        }
        let w = decode_words(&e.buf, sorted.len()).unwrap();
        let names = |v: Vec<usize>| v.into_iter().map(|i| String::from_utf8(w.word(i).to_vec()).unwrap()).collect::<Vec<_>>();
        assert_eq!(names(w.find(b"git", WordMatch::Suffix)), vec!["git", "legit"]);
        assert_eq!(names(w.find(b"commit", WordMatch::Prefix)), vec!["commit", "commits"]);
        assert_eq!(names(w.find(b"commit", WordMatch::Exact)), vec!["commit"]);
        assert_eq!(names(w.find(b"it", WordMatch::Contains)), vec!["commit", "commits", "git", "legit"]);
    }
}
