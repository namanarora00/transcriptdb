//! The `messages` source: one row per text unit (a text block the user wrote or the
//! assistant said; see [`crate::text`]).
//!
//! ```text
//! messages
//! | where role == "assistant" and text contains "let me check"
//! | summarize n = count() by session
//! ```
//!
//! Columns: `role` ("user" or "assistant"), `text`, `chars` (bytes of text), `session`,
//! `agent`, `node` (the message's number in its session), `block`, `time`.
//!
//! The leading `where` stages run against each segment's word index
//! ([`crate::textindex`]): every test gives the units that surely pass and the units that
//! may, the tests are combined, and only the units left in doubt have their text read.
//! Messages not yet flushed are scanned.

use super::exec::{parallel, Kind, QStats, Row, Table};
use super::matcher::MAX_ELEMS;
use super::parse::{CmpOp, Expr, Query, Stage, StrOp};
use super::value::{eval, str_op, Ctx, Value};
use super::QError;
use crate::db::{Db, TextSource};
use crate::record::NONE;
use crate::textindex::{self, Bits, Fit, TextHead};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgCol {
    Role,
    Text,
    Chars,
    Session,
    Agent,
    Node,
    Block,
    Time,
}

pub const COLUMNS: &[&str] = &["role", "text", "chars", "session", "agent", "node", "block", "time"];

pub fn resolve(path: &[String]) -> Result<MsgCol, QError> {
    if let [x] = path {
        return Ok(match x.as_str() {
            "role" => MsgCol::Role,
            "text" => MsgCol::Text,
            "chars" => MsgCol::Chars,
            "session" => MsgCol::Session,
            "agent" => MsgCol::Agent,
            "node" => MsgCol::Node,
            "block" => MsgCol::Block,
            "time" => MsgCol::Time,
            _ => return Err(QError::new(format!("unknown column '{x}' (messages have: {})", COLUMNS.join(", ")))),
        });
    }
    Err(QError::new(format!("unknown column '{}' (messages have: {})", path.join("."), COLUMNS.join(", "))))
}

/// A text unit as a filter sees it. `text` is `None` when the filter reads no text.
#[derive(Debug, Clone, Copy)]
pub struct U<'a> {
    pub s: u32,
    pub node: u32,
    pub block: u16,
    pub user: bool,
    pub chars: u32,
    pub time: i64,
    pub text: Option<&'a str>,
    /// The text is known to pass `value::ascii_case_only`.
    pub plain: bool,
}

/// A row for a unit: everything but its text travels in the row.
fn row(u: &U) -> Row {
    let mut b = [NONE; MAX_ELEMS];
    b[0] = u.node;
    b[1] = u.block as u32;
    b[2] = u.user as u32;
    b[3] = u.chars;
    b[4] = u.time as u64 as u32;
    b[5] = (u.time as u64 >> 32) as u32;
    Row { session: u.s, steps: b, vals: Vec::new() }
}

fn of_row<'a>(r: &Row, text: Option<&'a str>) -> U<'a> {
    U { s: r.session, node: r.steps[0], block: r.steps[1] as u16, user: r.steps[2] == 1, chars: r.steps[3], time: (r.steps[4] as u64 | (r.steps[5] as u64) << 32) as i64, text, plain: false }
}

fn qe(e: crate::codec::Error) -> QError {
    QError::new(e.to_string())
}

fn value(db: &Db, u: &U, col: MsgCol) -> Result<Value, QError> {
    Ok(match col {
        MsgCol::Role => Value::Str(if u.user { "user" } else { "assistant" }.into()),
        MsgCol::Text => match u.text {
            Some(t) => Value::Str(t.into()),
            None => Value::Str(db.unit_texts(u.s, &[(u.node, u.block)]).map_err(qe)?[0].text.as_str().into()),
        },
        MsgCol::Chars => Value::Int(u.chars as i64),
        MsgCol::Session => Value::Str(db.session_key(u.s).0.into()),
        MsgCol::Agent => Value::Str(db.session_key(u.s).1.into()),
        MsgCol::Node => Value::Int(u.node as i64),
        MsgCol::Block => Value::Int(u.block as i64),
        MsgCol::Time => {
            if u.time != 0 {
                Value::Time(u.time)
            } else {
                Value::Null
            }
        }
    })
}

/// The value of a column for the unit a row points at.
pub fn row_value(db: &Db, r: &Row, path: &[String]) -> Result<Value, QError> {
    value(db, &of_row(r, None), resolve(path)?)
}

struct UnitCtx<'a> {
    db: &'a Db,
    u: &'a U<'a>,
    now: i64,
}

impl Ctx for UnitCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        value(self.db, self.u, resolve(path)?)
    }
    fn now(&self) -> i64 {
        self.now
    }
}

/// A filter over text units, with the tests on `text` and `role` taken out so they run on
/// the unit's text in place and against the word index.
#[derive(Debug, Clone)]
pub enum Mp {
    Const(bool),
    /// `role == "user"` (`true`) or `role == "assistant"`.
    Role(bool),
    /// `ascii`: the pattern is ASCII.
    Text { op: StrOp, pat: String, ascii: bool, cs: bool, neg: bool },
    Generic(Expr),
    And(Vec<Mp>),
    Or(Vec<Mp>),
    Not(Box<Mp>),
}

fn is_col(e: &Expr, name: &str) -> bool {
    matches!(e, Expr::Ref(p) if p.len() == 1 && p[0] == name)
}

fn lit_str(e: &Expr) -> Option<&str> {
    match e {
        Expr::Lit(Value::Str(s)) => Some(s),
        _ => None,
    }
}

fn refs_text(e: &Expr) -> bool {
    match e {
        Expr::Lit(_) => false,
        Expr::Ref(p) => p.len() == 1 && p[0] == "text",
        Expr::Not(x) => refs_text(x),
        Expr::And(v) | Expr::Or(v) => v.iter().any(refs_text),
        Expr::Cmp(_, a, b) | Expr::Ar(_, a, b) => refs_text(a) || refs_text(b),
        Expr::In { e, list, .. } => refs_text(e) || list.iter().any(refs_text),
        Expr::Str { e, pat, .. } => refs_text(e) || refs_text(pat),
        Expr::Call(_, args) => args.iter().any(refs_text),
    }
}

pub fn compile(e: &Expr) -> Mp {
    match e {
        Expr::And(v) => Mp::And(v.iter().map(compile).collect()),
        Expr::Or(v) => Mp::Or(v.iter().map(compile).collect()),
        Expr::Not(x) => Mp::Not(Box::new(compile(x))),
        Expr::Str { op, e: col, pat, neg, cs } if is_col(col, "text") && lit_str(pat).is_some() => Mp::Text { op: *op, pat: lit_str(pat).unwrap().to_string(), ascii: lit_str(pat).unwrap().is_ascii(), cs: *cs, neg: *neg },
        Expr::Cmp(op @ (CmpOp::Eq | CmpOp::Ne), a, b) => {
            let role = if is_col(a, "role") { lit_str(b) } else if is_col(b, "role") { lit_str(a) } else { None };
            match role {
                Some(r) if r == "user" || r == "assistant" => {
                    let m = Mp::Role(r == "user");
                    if *op == CmpOp::Eq { m } else { Mp::Not(Box::new(m)) }
                }
                Some(_) => Mp::Const(*op == CmpOp::Ne),
                None => Mp::Generic(e.clone()),
            }
        }
        _ => Mp::Generic(e.clone()),
    }
}

impl Mp {
    pub fn reads_text(&self) -> bool {
        match self {
            Mp::Const(_) | Mp::Role(_) => false,
            Mp::Text { .. } => true,
            Mp::Generic(e) => refs_text(e),
            Mp::And(v) | Mp::Or(v) => v.iter().any(|x| x.reads_text()),
            Mp::Not(x) => x.reads_text(),
        }
    }

    pub fn test(&self, db: &Db, u: &U, now: i64) -> Result<bool, QError> {
        Ok(match self {
            Mp::Const(b) => *b,
            Mp::Role(user) => u.user == *user,
            Mp::Text { op, pat, ascii, cs, neg } => {
                // an ASCII pattern on a text known to lower-case plainly: compared ignoring
                // ASCII case, without looking for the characters that would forbid it
                let test = |t: &str, plain: bool| {
                    if !*cs && *ascii && plain {
                        let (h, n) = (t.as_bytes(), pat.as_bytes());
                        match op {
                            StrOp::Contains => return super::value::ascii_ci_find(h, n).is_some(),
                            StrOp::StartsWith => return h.len() >= n.len() && h[..n.len()].eq_ignore_ascii_case(n),
                            StrOp::EndsWith => return h.len() >= n.len() && h[h.len() - n.len()..].eq_ignore_ascii_case(n),
                            StrOp::Has => return super::value::has_term_ascii_ci(t, pat),
                            StrOp::Matches => {}
                        }
                    }
                    str_op(*op, t, pat, *cs)
                };
                let hit = match u.text {
                    Some(t) => test(t, u.plain),
                    None => {
                        let t = db.unit_texts(u.s, &[(u.node, u.block)]).map_err(qe)?;
                        test(&t[0].text, t[0].plain_case)
                    }
                };
                hit != *neg
            }
            Mp::Generic(e) => eval(e, &UnitCtx { db, u, now })?.truthy(),
            Mp::And(v) => {
                for x in v {
                    if !x.test(db, u, now)? {
                        return Ok(false);
                    }
                }
                true
            }
            Mp::Or(v) => {
                for x in v {
                    if x.test(db, u, now)? {
                        return Ok(true);
                    }
                }
                false
            }
            Mp::Not(x) => !x.test(db, u, now)?,
        })
    }
}

/// Refuse unknown columns before anything runs.
pub fn check(e: &Expr) -> Result<(), QError> {
    match e {
        Expr::Lit(_) => Ok(()),
        Expr::Ref(p) => resolve(p).map(|_| ()),
        Expr::Not(x) => check(x),
        Expr::And(v) | Expr::Or(v) => v.iter().try_for_each(check),
        Expr::Cmp(_, a, b) | Expr::Ar(_, a, b) => check(a).and_then(|_| check(b)),
        Expr::In { e, list, .. } => check(e).and_then(|_| list.iter().try_for_each(check)),
        Expr::Str { e, pat, .. } => check(e).and_then(|_| check(pat)),
        Expr::Call(_, args) => args.iter().try_for_each(check),
    }
}

// ------------------------------------------------------------------ against the index

fn full(n: usize) -> Bits {
    let mut b = vec![u64::MAX; n.div_ceil(64)];
    if !n.is_multiple_of(64) {
        *b.last_mut().unwrap() = (1u64 << (n % 64)) - 1;
    }
    b
}

fn and(a: &mut Bits, b: &Bits) {
    a.iter_mut().zip(b).for_each(|(x, y)| *x &= *y);
}

fn or(a: &mut Bits, b: &Bits) {
    a.iter_mut().zip(b).for_each(|(x, y)| *x |= *y);
}

fn and_not(a: &mut Bits, b: &Bits) {
    a.iter_mut().zip(b).for_each(|(x, y)| *x &= !*y);
}

/// The units of a segment that surely pass a text test, and those that may.
fn text_atom(db: &Db, num: u32, h: &TextHead, op: StrOp, pat: &str, cs: bool) -> Result<(Bits, Bits), QError> {
    let n = h.units();
    let whole = match op {
        StrOp::Has => true,
        StrOp::Contains | StrOp::StartsWith | StrOp::EndsWith => false,
        StrOp::Matches => return Ok((textindex::bits(n), full(n))),
    };
    if pat.is_empty() {
        return Ok((full(n), textindex::bits(n)));
    }
    let low = pat.to_lowercase();
    let (runs, single) = textindex::pattern_runs(&low, whole);
    if runs.is_empty() {
        return Ok((textindex::bits(n), full(n)));
    }
    let mut cand: Option<Bits> = None;
    let mut open = false;
    let mut words = Vec::new();
    for (r, fit) in &runs {
        words.clear();
        h.find(r.as_bytes(), *fit, &mut words);
        let mut b = textindex::bits(n);
        for &w in &words {
            db.text_postings(num, h, w, &mut b).map_err(qe)?;
        }
        open |= matches!(fit, Fit::Suffix | Fit::Inside);
        cand = Some(match cand {
            None => b,
            Some(mut c) => {
                and(&mut c, &b);
                c
            }
        });
    }
    let mut cand = cand.unwrap();
    // a pattern that is one run of letters and digits: a unit with a fitting word matches
    // (a word cut short may hold it past its kept start; a text with a hole is read)
    if single && !cs && matches!(op, StrOp::Has | StrOp::Contains) {
        let mut maybe = h.holes.clone();
        if open {
            or(&mut maybe, &h.long);
        }
        and_not(&mut cand, &h.holes);
        and_not(&mut maybe, &cand);
        return Ok((cand, maybe));
    }
    if open {
        or(&mut cand, &h.long);
    }
    or(&mut cand, &h.holes);
    Ok((textindex::bits(n), cand))
}

/// (surely pass, may pass) for a filter over a segment's units.
fn plan(db: &Db, num: u32, h: &TextHead, mp: &Mp) -> Result<(Bits, Bits), QError> {
    let n = h.units();
    Ok(match mp {
        Mp::Const(true) => (full(n), textindex::bits(n)),
        Mp::Const(false) => (textindex::bits(n), textindex::bits(n)),
        Mp::Role(user) => {
            let mut d = full(n);
            if *user { and(&mut d, &h.user) } else { and_not(&mut d, &h.user) }
            (d, textindex::bits(n))
        }
        Mp::Text { op, pat, cs, neg, .. } => {
            let (d, m) = text_atom(db, num, h, *op, pat, *cs)?;
            if *neg {
                let mut nd = full(n);
                and_not(&mut nd, &d);
                and_not(&mut nd, &m);
                (nd, m)
            } else {
                (d, m)
            }
        }
        Mp::Generic(_) => (textindex::bits(n), full(n)),
        Mp::And(v) => {
            let (mut d, mut m) = (full(n), textindex::bits(n));
            for x in v {
                let (d2, m2) = plan(db, num, h, x)?;
                // what may still pass: possible on both sides
                let mut p = d.clone();
                or(&mut p, &m);
                let mut p2 = d2.clone();
                or(&mut p2, &m2);
                and(&mut p, &p2);
                and(&mut d, &d2);
                and_not(&mut p, &d);
                m = p;
            }
            (d, m)
        }
        Mp::Or(v) => {
            let (mut d, mut m) = (textindex::bits(n), textindex::bits(n));
            for x in v {
                let (d2, m2) = plan(db, num, h, x)?;
                or(&mut d, &d2);
                or(&mut m, &m2);
            }
            and_not(&mut m, &d);
            (d, m)
        }
        Mp::Not(x) => {
            let (d, m) = plan(db, num, h, x)?;
            let mut nd = full(n);
            and_not(&mut nd, &d);
            and_not(&mut nd, &m);
            (nd, m)
        }
    })
}

fn unit_of<'a>(h: &TextHead, i: u32, text: Option<&'a str>) -> U<'a> {
    let k = i as usize;
    U { s: h.parts[h.part_of[k] as usize].0, node: h.node[k], block: h.block[k], user: textindex::get(&h.user, i), chars: h.chars[k], time: h.time[k], text, plain: false }
}

fn scan_segment(db: &Db, num: u32, live: Option<&[bool]>, pre: &Mp, reads_text: bool, now: i64, tested: &AtomicUsize) -> Result<Vec<Row>, QError> {
    let h = db.text_head(num).map_err(qe)?;
    let (mut def, mut maybe) = plan(db, num, &h, pre)?;
    if let Some(live) = live {
        let mut dead = textindex::bits(h.units());
        for (p, &(_, first, n)) in h.parts.iter().enumerate() {
            if !live.get(p).copied().unwrap_or(false) {
                (first..first + n).for_each(|i| textindex::set(&mut dead, i));
            }
        }
        and_not(&mut def, &dead);
        and_not(&mut maybe, &dead);
    }
    let mut rows = Vec::new();
    textindex::each(&def, |i| rows.push(row(&unit_of(&h, i, None))));
    if textindex::any(&maybe) {
        // read the texts in doubt one session at a time
        let mut ids = Vec::new();
        textindex::each(&maybe, |i| ids.push(i));
        tested.fetch_add(ids.len(), Ordering::Relaxed);
        let mut a = 0;
        while a < ids.len() {
            let part = h.part_of[ids[a] as usize];
            let mut b = a;
            while b < ids.len() && h.part_of[ids[b] as usize] == part {
                b += 1;
            }
            let s = h.parts[part as usize].0;
            let texts = if reads_text {
                let wants: Vec<(u32, u16)> = ids[a..b].iter().map(|&i| (h.node[i as usize], h.block[i as usize])).collect();
                Some(db.unit_texts(s, &wants).map_err(qe)?)
            } else {
                None
            };
            for (k, &i) in ids[a..b].iter().enumerate() {
                let mut u = unit_of(&h, i, texts.as_ref().map(|t| t[k].text.as_str()));
                u.plain = texts.as_ref().is_some_and(|t| t[k].plain_case);
                if pre.test(db, &u, now)? {
                    rows.push(row(&u));
                }
            }
            a = b;
        }
    }
    Ok(rows)
}

fn scan_nodes(db: &Db, s: u32, start: u32, end: u32, pre: &Mp, now: i64, tested: &AtomicUsize) -> Result<Vec<Row>, QError> {
    let t = db.session_texts(s).map_err(qe)?;
    let mut rows = Vec::new();
    for (k, x) in t.units.iter().enumerate() {
        if x.node < start || x.node >= end {
            continue;
        }
        tested.fetch_add(1, Ordering::Relaxed);
        let u = U { s, node: x.node, block: x.block, user: x.user, chars: x.len, time: t.node_time.get(x.node as usize).copied().unwrap_or(0), text: Some(t.text_of(k)), plain: false };
        if pre.test(db, &u, now)? {
            rows.push(row(&u));
        }
    }
    Ok(rows)
}

thread_local! {
    static USE_INDEX: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Whether searches on this thread may use the word index (on by default). Checks turn it
/// off to compare against reading every text.
pub fn set_index(on: bool) {
    USE_INDEX.with(|c| c.set(on));
}

/// Run the leading `where` stages of a `messages` query. Returns the rows (in session,
/// node, block order) and how many stages were used.
pub fn scan(db: &Db, q: &Query, threads: usize, now: i64, st: &mut QStats) -> Result<(Table, usize), QError> {
    let mut i = 0;
    let mut parts = Vec::new();
    while let Some(Stage::Where(e)) = q.stages.get(i) {
        check(e)?;
        parts.push(compile(e));
        i += 1;
    }
    if q.stages[i..].iter().any(|s| matches!(s, Stage::Match(_))) {
        return Err(QError::new("match works on steps, not on messages"));
    }
    let pre = if parts.len() == 1 { parts.pop().unwrap() } else { Mp::And(parts) };
    let reads_text = pre.reads_text();
    let tested = AtomicUsize::new(0);
    let use_index = USE_INDEX.with(|c| c.get());
    let per = if use_index {
        let sources = db.text_sources().map_err(qe)?;
        // one source is searched on this thread: starting threads costs more than a lookup
        parallel(sources.len(), threads.min(sources.len()), |k| match &sources[k as usize] {
            TextSource::Seg { num, live } => scan_segment(db, *num, live.as_ref().map(|m| &m[..]), &pre, reads_text, now, &tested),
            TextSource::Nodes { session, start, end } => scan_nodes(db, *session, *start, *end, &pre, now, &tested),
        })?
    } else {
        parallel(db.session_count(), threads, |s| scan_nodes(db, s, 0, u32::MAX, &pre, now, &tested))?
    };
    let mut rows: Vec<Row> = per.into_iter().flat_map(|x| x.1).collect();
    rows.sort_unstable_by_key(|r| (r.session, r.steps[0], r.steps[1]));
    st.steps_scanned = tested.load(Ordering::Relaxed);
    st.matches = rows.len();
    Ok((Table { kind: Kind::Messages, cols: Vec::new(), rows }, i))
}

/// Whether `summarize` over unit rows can be done on the rows themselves: counts (and
/// distinct sessions) by session, agent or role.
pub fn fast_fits(t: &Table, aggs: &[(String, super::parse::AggFn, Option<Expr>)], by: &[(String, Expr)]) -> bool {
    use super::parse::AggFn;
    t.kind == Kind::Messages
        && t.cols.is_empty()
        && by.len() <= 2
        && by.iter().all(|(_, e)| is_col(e, "session") || is_col(e, "agent") || is_col(e, "role"))
        && aggs.iter().all(|(_, f, a)| match (f, a) {
            (AggFn::Count, _) => true,
            (AggFn::DCount, Some(e)) => is_col(e, "session"),
            _ => false,
        })
}

/// `summarize` by session, agent and role without evaluating each row: the same groups in
/// the same order (first seen) as the general path. A session here is its id as text: the
/// main agent and its side agents are stored apart and share one.
pub fn fast_summarize(db: &Db, t: &Table, aggs: &[(String, super::parse::AggFn, Option<Expr>)], by: &[(String, Expr)]) -> Table {
    use super::parse::AggFn;
    let by_session = by.iter().any(|(_, e)| is_col(e, "session"));
    let by_agent = by.iter().any(|(_, e)| is_col(e, "agent"));
    let by_role = by.iter().any(|(_, e)| is_col(e, "role"));
    let want_distinct = aggs.iter().any(|(_, f, _)| *f == AggFn::DCount);
    // rows come in stored-session order: count each run of rows with one stored session and
    // role first, then name the stored sessions and fold the runs into groups
    let mut runs: Vec<(usize, u32, u32, i64)> = Vec::new();
    for (i, r) in t.rows.iter().enumerate() {
        let role = if by_role { r.steps[2] } else { NONE };
        match runs.last_mut() {
            Some(x) if x.1 == r.session && x.2 == role => x.3 += 1,
            _ => runs.push((i, r.session, role, 1)),
        }
    }
    // the stored sessions of the runs, each with a number for its session id and one for
    // its agent name
    let mut stored: Vec<u32> = Vec::new();
    for &(_, s, _, _) in &runs {
        if stored.last() != Some(&s) {
            stored.push(s);
        }
    }
    let ids = db.key_ids(&stored).expect("read session");
    // per group: its key, a row that stands for it, its count, its distinct session ids
    let mut acc: Vec<((u32, u32, u32), usize, i64, Vec<u32>)> = Vec::new();
    let mut group_of: std::collections::BTreeMap<(u32, u32, u32), usize> = Default::default();
    let mut at = 0;
    for &(first, s, role, n) in &runs {
        while stored[at] != s {
            at += 1;
        }
        let (sid, agent) = ids[at];
        let k = (if by_session { sid } else { NONE }, if by_agent { agent } else { NONE }, role);
        let g = *group_of.entry(k).or_insert_with(|| {
            acc.push((k, first, 0, Vec::new()));
            acc.len() - 1
        });
        acc[g].2 += n;
        if want_distinct {
            acc[g].3.push(sid);
        }
    }
    if by.is_empty() && acc.is_empty() {
        acc.push(((NONE, NONE, NONE), usize::MAX, 0, Vec::new()));
    }
    let mut rows = Vec::with_capacity(acc.len());
    for (_, first, count, sessions) in &acc {
        let mut vals = Vec::with_capacity(by.len() + aggs.len());
        for (_, e) in by {
            let r = &t.rows[*first];
            vals.push(if is_col(e, "session") {
                Value::Str(db.session_key(r.session).0.into())
            } else if is_col(e, "agent") {
                Value::Str(db.session_key(r.session).1.into())
            } else {
                Value::Str(if r.steps[2] == 1 { "user" } else { "assistant" }.into())
            });
        }
        let distinct = if want_distinct {
            let mut d = sessions.clone();
            d.sort_unstable();
            d.dedup();
            d.len() as i64
        } else {
            0
        };
        for (_, f, _) in aggs {
            vals.push(Value::Int(if *f == AggFn::Count { *count } else { distinct }));
        }
        rows.push(Row { session: NONE, steps: [NONE; MAX_ELEMS], vals });
    }
    let cols = by.iter().map(|(n, _)| n.clone()).chain(aggs.iter().map(|(n, _, _)| n.clone())).collect();
    Table { kind: Kind::Plain, cols, rows }
}
