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

use super::exec::{parallel, Kind, QStats, Row, Table};
use super::matcher::MAX_ELEMS;
use super::parse::{CmpOp, Expr, Query, Stage, StrOp};
use super::value::{eval, str_op, Ctx, Value};
use super::QError;
use crate::db::Db;
use crate::record::NONE;
use crate::text::SessionTexts;
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

pub fn value(db: &Db, s: u32, st: &SessionTexts, u: usize, col: MsgCol) -> Value {
    let x = &st.units[u];
    match col {
        MsgCol::Role => Value::Str(if x.user { "user" } else { "assistant" }.into()),
        MsgCol::Text => Value::Str(st.text_of(u).into()),
        MsgCol::Chars => Value::Int(x.len as i64),
        MsgCol::Session => Value::Str(db.session_key(s).0.into()),
        MsgCol::Agent => Value::Str(db.session_key(s).1.into()),
        MsgCol::Node => Value::Int(x.node as i64),
        MsgCol::Block => Value::Int(x.block as i64),
        MsgCol::Time => match st.node_time.get(x.node as usize) {
            Some(&t) if t != 0 => Value::Time(t),
            _ => Value::Null,
        },
    }
}

/// The value of a column for the unit a row points at.
pub fn row_value(db: &Db, s: u32, u: u32, path: &[String]) -> Result<Value, QError> {
    let col = resolve(path)?;
    let st = db.session_texts(s).map_err(|e| QError::new(e.to_string()))?;
    Ok(value(db, s, &st, u as usize, col))
}

struct UnitCtx<'a> {
    db: &'a Db,
    s: u32,
    st: &'a SessionTexts,
    u: usize,
    now: i64,
}

impl Ctx for UnitCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        Ok(value(self.db, self.s, self.st, self.u, resolve(path)?))
    }
    fn now(&self) -> i64 {
        self.now
    }
}

/// A filter over text units, with the tests on `text` and `role` taken out so they run on
/// the unit's text in place.
#[derive(Debug, Clone)]
pub enum Mp {
    Const(bool),
    /// `role == "user"` (`true`) or `role == "assistant"`.
    Role(bool),
    Text { op: StrOp, pat: String, cs: bool, neg: bool },
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

pub fn compile(e: &Expr) -> Mp {
    match e {
        Expr::And(v) => Mp::And(v.iter().map(compile).collect()),
        Expr::Or(v) => Mp::Or(v.iter().map(compile).collect()),
        Expr::Not(x) => Mp::Not(Box::new(compile(x))),
        Expr::Str { op, e: col, pat, neg, cs } if is_col(col, "text") && lit_str(pat).is_some() => Mp::Text { op: *op, pat: lit_str(pat).unwrap().to_string(), cs: *cs, neg: *neg },
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
    pub fn test(&self, db: &Db, s: u32, st: &SessionTexts, u: usize, now: i64) -> Result<bool, QError> {
        Ok(match self {
            Mp::Const(b) => *b,
            Mp::Role(user) => st.units[u].user == *user,
            Mp::Text { op, pat, cs, neg } => str_op(*op, st.text_of(u), pat, *cs) != *neg,
            Mp::Generic(e) => eval(e, &UnitCtx { db, s, st, u, now })?.truthy(),
            Mp::And(v) => {
                for x in v {
                    if !x.test(db, s, st, u, now)? {
                        return Ok(false);
                    }
                }
                true
            }
            Mp::Or(v) => {
                for x in v {
                    if x.test(db, s, st, u, now)? {
                        return Ok(true);
                    }
                }
                false
            }
            Mp::Not(x) => !x.test(db, s, st, u, now)?,
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

/// Run the leading `where` stages of a `messages` query over every session's text units.
/// Returns the rows and how many stages were used.
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
    let scanned = AtomicUsize::new(0);
    let per = parallel(db.session_count(), threads, |s| {
        let t = db.session_texts(s).map_err(|e| QError::new(e.to_string()))?;
        scanned.fetch_add(t.units.len(), Ordering::Relaxed);
        let mut v = Vec::new();
        for u in 0..t.units.len() {
            if pre.test(db, s, &t, u, now)? {
                v.push(u as u32);
            }
        }
        Ok(v)
    })?;
    let mut rows = Vec::new();
    for (s, v) in per {
        rows.extend(v.into_iter().map(|u| {
            let mut b = [NONE; MAX_ELEMS];
            b[0] = u;
            Row { session: s, steps: b, vals: Vec::new() }
        }));
    }
    st.steps_scanned = scanned.load(Ordering::Relaxed);
    st.matches = rows.len();
    Ok((Table { kind: Kind::Messages, cols: Vec::new(), rows }, i))
}
