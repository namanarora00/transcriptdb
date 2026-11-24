//! Step predicates compiled against the store's dictionaries.
//!
//! An expression over one dictionary-coded column (the tool, one input field, the
//! session) is evaluated once per distinct value, giving a table indexed by code; a step
//! is then tested with one lookup. Tool and outcome tests also compile to a table over
//! the 256 shape codes. Anything else is evaluated per step.

use super::parse::Expr;
use super::value::{eval, Ctx, Value};
use super::QError;
use crate::db::Db;
use crate::record::NONE;
use crate::steps::{self, SessionSteps};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A column of a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepCol {
    Tool,
    Failed,
    Ok,
    Time,
    OutBytes,
    Step,
    Session,
    Agent,
    Field(String),
}

pub const BUILTINS: &[&str] = &["tool", "failed", "ok", "time", "out_bytes", "step", "session", "agent"];

pub fn resolve(path: &[String]) -> Result<StepCol, QError> {
    Ok(match path {
        [x] => match x.as_str() {
            "tool" => StepCol::Tool,
            "failed" => StepCol::Failed,
            "ok" => StepCol::Ok,
            "time" => StepCol::Time,
            "out_bytes" => StepCol::OutBytes,
            "step" => StepCol::Step,
            "session" => StepCol::Session,
            "agent" => StepCol::Agent,
            f => StepCol::Field(f.to_string()),
        },
        [i, f] if i == "input" => StepCol::Field(f.clone()),
        _ => return Err(QError::new(format!("unknown column '{}'", path.join(".")))),
    })
}

/// The raw JSON text of input field `name` of a step, read from the stored message (the
/// full message only if an opaque value sits inside that field).
pub fn read_input(db: &Db, s: u32, ss: &SessionSteps, pos: u32, name: &str) -> Result<Option<Vec<u8>>, QError> {
    db.read_input(s, ss, pos, name).map_err(|e| QError::new(e.to_string()))
}

/// The raw JSON text of long value `v` (a `LONG_BASE` code) of input field `name`: only its
/// bytes are read, unless an opaque value sits inside (then the whole message).
pub fn long_value(db: &Db, s: u32, ss: &SessionSteps, pos: u32, v: u32, name: &str) -> Result<Option<Vec<u8>>, QError> {
    db.long_value(s, ss, pos, v, name).map_err(|e| QError::new(e.to_string()))
}

/// The value of one step column.
pub fn step_value(db: &Db, s: u32, pos: u32, col: &StepCol) -> Result<Value, QError> {
    let qe = |e: crate::codec::Error| QError::new(e.to_string());
    // what is not a step's own needs no steps; what is not an input value needs none of them
    match col {
        StepCol::Session => return Ok(Value::Str(db.session_names(s).map_err(qe)?.0.as_ref().into())),
        StepCol::Agent => return Ok(Value::Str(db.session_names(s).map_err(qe)?.1.as_ref().into())),
        StepCol::Field(_) => {}
        _ => {
            let ss = db.light_steps(s).map_err(qe)?;
            let r = ss.rows[pos as usize];
            return Ok(match col {
                StepCol::Tool => Value::Str(String::from_utf8_lossy(db.step_dicts().tools.get(r.tool)).into()),
                StepCol::Failed => Value::Bool(r.failed),
                StepCol::Ok => Value::Bool(!r.failed),
                StepCol::Time => Value::Time(r.time_ms),
                StepCol::OutBytes => Value::Int(r.out_bytes as i64),
                _ => Value::Int(pos as i64),
            });
        }
    }
    let ss_arc = db.steps(s);
    let ss = &*ss_arc;
    let sd = db.step_dicts();
    Ok(match col {
        StepCol::Field(name) => {
            let Some(f) = sd.fields.code(name.as_bytes()) else { return Ok(Value::Null) };
            match ss.fields_of(pos).iter().find(|x| x.0 == f) {
                None => Value::Null,
                Some(&(_, v)) if steps::is_long(v) => match long_value(db, s, ss, pos, v, name)? {
                    Some(t) => Value::from_json(&t),
                    None => Value::Null,
                },
                Some(&(_, v)) => Value::from_json(ss.value(f, v)),
            }
        }
        _ => unreachable!("handled above"),
    })
}

/// Columns of one step, read through the dictionaries.
pub struct StepCtx<'a> {
    pub db: &'a Db,
    pub s: u32,
    pub pos: u32,
    pub now: i64,
}

impl Ctx for StepCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        step_value(self.db, self.s, self.pos, &resolve(path)?)
    }
    fn now(&self) -> i64 {
        self.now
    }
}

/// A context that answers one column with a fixed value (for evaluating over a dictionary).
struct OneCol<'a> {
    col: &'a StepCol,
    v: Value,
    now: i64,
}

impl Ctx for OneCol<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        let c = resolve(path)?;
        if &c == self.col { Ok(self.v.clone()) } else { Err(QError::new("internal: second column in a one-column predicate")) }
    }
    fn now(&self) -> i64 {
        self.now
    }
}

/// A shape-only context: tool and outcome.
struct ShapeCtx<'a> {
    tool: &'a str,
    failed: bool,
    now: i64,
}

impl Ctx for ShapeCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        Ok(match resolve(path)? {
            StepCol::Tool => Value::Str(self.tool.into()),
            StepCol::Failed => Value::Bool(self.failed),
            StepCol::Ok => Value::Bool(!self.failed),
            _ => return Err(QError::new("not a shape column")),
        })
    }
    fn now(&self) -> i64 {
        self.now
    }
}

#[derive(Debug, Clone)]
pub enum Sp {
    Const(bool),
    /// By tool code.
    Tool(Arc<[bool]>),
    /// By value of input field `f`: each session's value is tested the first time a step
    /// has it, and remembered for that session (see [`Memo`]); `missing` when the step lacks
    /// the field; long values are read (just their bytes) and tested with `expr`.
    Field { name: String, f: Option<u32>, uid: u64, missing: bool, expr: Arc<Expr> },
    /// `isnotnull(field)` (`want`) or `isnull(field)`.
    Present { f: Option<u32>, want: bool },
    /// By session (its id or agent), tested once per session.
    Session { uid: u64, col: StepCol, expr: Arc<Expr> },
    /// Evaluated per step.
    Generic(Arc<Expr>),
    And(Vec<Sp>),
    Or(Vec<Sp>),
    Not(Box<Sp>),
}

fn single_col(e: &Expr) -> Result<Option<StepCol>, QError> {
    let mut refs = Vec::new();
    e.refs(&mut refs);
    let mut col: Option<StepCol> = None;
    for r in refs {
        let c = resolve(r)?;
        match &col {
            None => col = Some(c),
            Some(x) if *x == c => {}
            Some(_) => return Ok(None),
        }
    }
    Ok(col)
}

thread_local! {
    /// Compiled one-column tests, so a test repeated in one query shares its memo. Cleared
    /// at the start of every query.
    static ATOMS: std::cell::RefCell<std::collections::HashMap<String, Sp>> = std::cell::RefCell::new(std::collections::HashMap::new());
    static MEMO: std::cell::RefCell<Memo> = std::cell::RefCell::new(Memo::default());
}

static NEXT_UID: AtomicU64 = AtomicU64::new(1);

/// Per thread, for the session being tested: one table per field test, indexed by the
/// session's value code (0 = not yet, 1 = false, 2 = true), and session tests' answers.
/// Each session has its own value codes, so the tables start over with each session.
#[derive(Default)]
struct Memo {
    session: u32,
    steps: usize,
    tables: Vec<(u64, Vec<u8>)>,
}

impl Memo {
    fn for_session(&mut self, s: u32, ss: &SessionSteps) {
        let id = ss as *const SessionSteps as usize;
        if self.session != s || self.steps != id {
            self.session = s;
            self.steps = id;
            self.tables.clear();
        }
    }
    fn table(&mut self, uid: u64, n: usize) -> &mut Vec<u8> {
        let i = match self.tables.iter().position(|t| t.0 == uid) {
            Some(i) => i,
            None => {
                self.tables.push((uid, vec![0; n]));
                self.tables.len() - 1
            }
        };
        &mut self.tables[i].1
    }
}

pub fn clear_cache() {
    ATOMS.with(|c| c.borrow_mut().clear());
}


pub fn compile(db: &Db, e: &Expr, now: i64) -> Result<Sp, QError> {
    Ok(match e {
        Expr::And(v) => Sp::And(v.iter().map(|x| compile(db, x, now)).collect::<Result<_, _>>()?),
        Expr::Or(v) => Sp::Or(v.iter().map(|x| compile(db, x, now)).collect::<Result<_, _>>()?),
        Expr::Not(x) => Sp::Not(Box::new(compile(db, x, now)?)),
        _ => {
            let key = format!("{e:?}");
            if let Some(sp) = ATOMS.with(|c| c.borrow().get(&key).cloned()) {
                return Ok(sp);
            }
            let sp = compile_atom(db, e, now)?;
            ATOMS.with(|c| c.borrow_mut().insert(key, sp.clone()));
            sp
        }
    })
}

fn compile_atom(db: &Db, e: &Expr, now: i64) -> Result<Sp, QError> {
    Ok({
        let mut refs = Vec::new();
        e.refs(&mut refs);
        if refs.is_empty() {
            struct NoCols(i64);
            impl Ctx for NoCols {
                fn col(&self, _: &[String]) -> Result<Value, QError> {
                    Err(QError::new("no columns here"))
                }
                fn now(&self) -> i64 {
                    self.0
                }
            }
            return Ok(Sp::Const(eval(e, &NoCols(now))?.truthy()));
        }
        let sd = db.step_dicts();
        match single_col(e)? {
            Some(StepCol::Tool) => {
                let col = StepCol::Tool;
                let t: Vec<bool> = match tool_name_test(e) {
                    // common forms tested on the names directly, without building values
                    Some(f) => (0..sd.tools.len() as u32).map(|c| f(&name(sd.tools.get(c)))).collect(),
                    None => (0..sd.tools.len() as u32)
                        .map(|c| eval(e, &OneCol { col: &col, v: Value::Str(String::from_utf8_lossy(sd.tools.get(c)).into()), now }).map(|v| v.truthy()))
                        .collect::<Result<_, _>>()?,
                };
                Sp::Tool(t.into())
            }
            Some(StepCol::Field(name)) => {
                let col = StepCol::Field(name.clone());
                let f = sd.fields.code(name.as_bytes());
                // isnull / isnotnull need no value at all
                if let Expr::Call(fun, args) = e
                    && (fun == "isnull" || fun == "isnotnull") && args.len() == 1 && matches!(args[0], Expr::Ref(_)) {
                        return Ok(Sp::Present { f, want: fun == "isnotnull" });
                    }
                let missing = eval(e, &OneCol { col: &col, v: Value::Null, now })?.truthy();
                Sp::Field { name, f, uid: NEXT_UID.fetch_add(1, Ordering::Relaxed), missing, expr: Arc::new(e.clone()) }
            }
            Some(c @ (StepCol::Session | StepCol::Agent)) => Sp::Session { uid: NEXT_UID.fetch_add(1, Ordering::Relaxed), col: c, expr: Arc::new(e.clone()) },
            _ => Sp::Generic(Arc::new(e.clone())),
        }
    })
}

/// A name as text (checked as UTF-8 first: much faster than a lossy conversion that has
/// nothing to replace).
fn name(b: &[u8]) -> std::borrow::Cow<'_, str> {
    match std::str::from_utf8(b) {
        Ok(s) => std::borrow::Cow::Borrowed(s),
        Err(_) => String::from_utf8_lossy(b),
    }
}

/// `tool == "x"`, `tool != "x"`, `tool in (...)` and string tests against a literal, as a
/// test on the name that agrees with evaluating the expression.
fn tool_name_test(e: &Expr) -> Option<Box<dyn Fn(&str) -> bool>> {
    use super::parse::CmpOp;
    let lit = |x: &Expr| match x {
        Expr::Lit(Value::Str(s)) => Some(s.to_string()),
        _ => None,
    };
    let is_ref = |x: &Expr| matches!(x, Expr::Ref(_));
    match e {
        Expr::Cmp(op @ (CmpOp::Eq | CmpOp::Ne), a, b) => {
            let l = if is_ref(a) { lit(b)? } else if is_ref(b) { lit(a)? } else { return None };
            let eq = *op == CmpOp::Eq;
            Some(Box::new(move |n| (n == l) == eq))
        }
        Expr::In { e: inner, list, neg } if is_ref(inner) => {
            let ls: Vec<String> = list.iter().map(lit).collect::<Option<_>>()?;
            let neg = *neg;
            Some(Box::new(move |n| ls.iter().any(|l| l == n) != neg))
        }
        Expr::Str { op, e: inner, pat, neg, cs } if is_ref(inner) => {
            let (op, p, neg, cs) = (*op, lit(pat)?, *neg, *cs);
            Some(Box::new(move |n| super::value::str_op(op, n, &p, cs) != neg))
        }
        _ => None,
    }
}

/// `contains` on the raw text of a JSON string (between the quotes), giving the answer
/// for the decoded text without decoding it, or `None` when that cannot be done.
///
/// The pattern must be ASCII with no character an escape stands for (quote, backslash,
/// control characters). Where the raw text holds it, starting at a character that is not
/// escaped (one not right after an odd run of backslashes) and not among the digits of a
/// `\u` escape, the decoded text holds it too: the search stops there, so a text is read
/// only as far as its first hit. With no such place the decoded text can still hold the
/// pattern through an escape (`\u`, or `\/` for a pattern with `/`) or, ignoring case,
/// through one of the two characters whose lower case is ASCII (U+0130 and U+212A): a
/// text with any of those is handed back.
pub fn raw_contains(body: &[u8], p: &[u8], cs: bool) -> Option<bool> {
    use memchr::memmem::find;
    if !p.iter().all(|&c| c.is_ascii() && c >= 0x20 && c != b'"' && c != b'\\') {
        return None;
    }
    let mut i = 0;
    loop {
        let at = if cs { find(&body[i..], p) } else { super::value::ascii_ci_find(&body[i..], p) };
        let Some(k) = at else { break };
        let a = i + k;
        let slashes = body[..a].iter().rev().take_while(|&&c| c == b'\\').count();
        if slashes % 2 == 0 {
            // a `\u` just before: the hit may start among its four digits
            return if find(&body[a.saturating_sub(5)..a], b"\\u").is_some() { None } else { Some(true) };
        }
        i = a + 1;
    }
    if find(body, b"\\u").is_some() || (p.contains(&b'/') && find(body, b"\\/").is_some()) {
        return None;
    }
    if !cs && !body.is_ascii() && (find(body, "\u{130}".as_bytes()).is_some() || find(body, "\u{212a}".as_bytes()).is_some()) {
        return None;
    }
    Some(false)
}

/// Test one value (raw JSON text) against a one-column expression, as evaluating it on the
/// decoded value does. String tests with a literal pattern run on the text itself, decoded
/// only if that cannot decide.
fn test_raw(e: &Expr, name: &str, raw: &[u8], now: i64) -> Result<bool, QError> {
    if let Expr::Str { op, e: inner, pat, neg, cs } = e
        && let (Expr::Ref(_), Expr::Lit(Value::Str(p))) = (inner.as_ref(), pat.as_ref())
        && raw.len() >= 2
        && raw[0] == b'"'
    {
        let body = &raw[1..raw.len() - 1];
        if *op == super::parse::StrOp::Contains
            && let Some(hit) = raw_contains(body, p.as_bytes(), *cs)
        {
            return Ok(hit != *neg);
        }
        let hit = if body.contains(&b'\\') {
            super::value::str_op(*op, &super::value::json_unescape(body), p, *cs)
        } else {
            match std::str::from_utf8(body) {
                Ok(t) => super::value::str_op(*op, t, p, *cs),
                Err(_) => super::value::str_op(*op, &String::from_utf8_lossy(body), p, *cs),
            }
        };
        return Ok(hit != *neg);
    }
    let col = StepCol::Field(name.to_string());
    Ok(eval(e, &OneCol { col: &col, v: Value::from_json(raw), now })?.truthy())
}

/// A step's value of the field an atom tests, as the summaries hand it over.
#[derive(Debug, Clone, Copy)]
pub enum FieldArg<'a> {
    Missing,
    /// A dictionary value (JSON text).
    Short(&'a [u8]),
    /// A long value's text (`None` if it could not be found in the call).
    Long(Option<&'a [u8]>),
}

/// The answer of a field atom (`Field` or `Present`) for one value, exactly as
/// [`Sp::test`] gives it for a step with that value.
pub fn field_atom(sp: &Sp, arg: FieldArg, now: i64) -> Result<bool, QError> {
    match sp {
        Sp::Field { name, missing, expr, .. } => match arg {
            FieldArg::Missing => Ok(*missing),
            FieldArg::Short(t) | FieldArg::Long(Some(t)) => test_raw(expr, name, t, now),
            FieldArg::Long(None) => {
                let col = StepCol::Field(name.clone());
                Ok(eval(expr, &OneCol { col: &col, v: Value::Null, now })?.truthy())
            }
        },
        Sp::Present { want, .. } => {
            let present = match arg {
                FieldArg::Missing => false,
                FieldArg::Short(t) => t != b"null",
                FieldArg::Long(_) => true,
            };
            Ok(present == *want)
        }
        _ => Err(QError::new("internal: not a field test")),
    }
}

/// The answer of a session atom for a session with this id and agent.
pub fn session_atom(sp: &Sp, sid: &str, agent: &str, now: i64) -> Result<bool, QError> {
    let Sp::Session { col, expr, .. } = sp else { return Err(QError::new("internal: not a session test")) };
    let v = if *col == StepCol::Session { sid } else { agent };
    Ok(eval(expr, &OneCol { col, v: Value::Str(v.into()), now })?.truthy())
}

/// A per-step expression decided by tool and outcome alone: its answer per tool code and
/// outcome, or `None` if it reads other columns.
pub fn shape_atom(db: &Db, e: &Expr, now: i64) -> Option<Vec<[bool; 2]>> {
    let mut refs = Vec::new();
    e.refs(&mut refs);
    let cols: Vec<StepCol> = refs.into_iter().map(|r| resolve(r).ok()).collect::<Option<_>>()?;
    if !cols.iter().all(|c| matches!(c, StepCol::Tool | StepCol::Failed | StepCol::Ok)) {
        return None;
    }
    let sd = db.step_dicts();
    let one = |tool: &str| -> Option<[bool; 2]> {
        let a = eval(e, &ShapeCtx { tool, failed: false, now }).ok()?.truthy();
        let b = eval(e, &ShapeCtx { tool, failed: true, now }).ok()?.truthy();
        Some([a, b])
    };
    if !cols.contains(&StepCol::Tool) {
        let x = one("")?;
        return Some(vec![x; sd.tools.len()]);
    }
    (0..sd.tools.len() as u32).map(|t| one(&String::from_utf8_lossy(sd.tools.get(t)))).collect()
}

impl Sp {
    /// Whether testing a step reads its input values (so it needs the session's steps with
    /// their values, not a copy without them).
    pub fn reads_values(&self) -> bool {
        match self {
            Sp::Const(_) | Sp::Tool(_) | Sp::Session { .. } => false,
            Sp::Field { .. } | Sp::Present { .. } => true,
            Sp::Generic(e) => {
                let mut refs = Vec::new();
                e.refs(&mut refs);
                refs.iter().any(|r| !matches!(resolve(r), Ok(StepCol::Tool | StepCol::Failed | StepCol::Ok | StepCol::Time | StepCol::OutBytes | StepCol::Step | StepCol::Session | StepCol::Agent)))
            }
            Sp::And(v) | Sp::Or(v) => v.iter().any(|x| x.reads_values()),
            Sp::Not(x) => x.reads_values(),
        }
    }

    /// The input fields whose values testing a step reads, added to `out` (store field
    /// codes); false when that cannot be said (a test evaluated per step reads the whole
    /// step).
    pub fn value_fields(&self, out: &mut Vec<u32>) -> bool {
        match self {
            Sp::Const(_) | Sp::Tool(_) | Sp::Session { .. } => true,
            Sp::Field { f, .. } | Sp::Present { f, .. } => {
                out.extend(f.iter());
                true
            }
            // one reading only a step's own columns (`failed`, `time`...) reads no values
            Sp::Generic(_) => !self.reads_values(),
            Sp::And(v) | Sp::Or(v) => v.iter().all(|x| x.value_fields(out)),
            Sp::Not(x) => x.value_fields(out),
        }
    }

    pub fn test(&self, db: &Db, s: u32, ss: &SessionSteps, pos: u32, now: i64) -> Result<bool, QError> {
        Ok(match self {
            Sp::Const(b) => *b,
            Sp::Tool(t) => t[ss.rows[pos as usize].tool as usize],
            Sp::Field { name, f, uid, missing, expr } => {
                let Some(f) = f else { return Ok(*missing) };
                match ss.fields_of(pos).iter().find(|x| x.0 == *f) {
                    None => *missing,
                    Some(&(_, v)) if steps::is_long(v) => {
                        let col = StepCol::Field(name.clone());
                        let val = match long_value(db, s, ss, pos, v, name)? {
                            Some(t) => Value::from_json(&t),
                            None => Value::Null,
                        };
                        eval(expr, &OneCol { col: &col, v: val, now })?.truthy()
                    }
                    Some(&(_, v)) => {
                        let known = MEMO.with(|m| {
                            let mut m = m.borrow_mut();
                            m.for_session(s, ss);
                            m.table(*uid, ss.value_count(*f))[v as usize]
                        });
                        match known {
                            1 => false,
                            2 => true,
                            _ => {
                                let b = test_raw(expr, name, ss.value(*f, v), now)?;
                                MEMO.with(|m| m.borrow_mut().table(*uid, ss.value_count(*f))[v as usize] = if b { 2 } else { 1 });
                                b
                            }
                        }
                    }
                }
            }
            Sp::Present { f, want } => {
                let present = f.is_some_and(|f| ss.fields_of(pos).iter().any(|x| x.0 == f && (steps::is_long(x.1) || ss.value(f, x.1) != b"null")));
                present == *want
            }
            Sp::Session { .. } => !self.session_false(db, s, ss, now)?,
            Sp::Generic(e) => eval(e, &StepCtx { db, s, pos, now })?.truthy(),
            Sp::And(v) => {
                for x in v {
                    if !x.test(db, s, ss, pos, now)? {
                        return Ok(false);
                    }
                }
                true
            }
            Sp::Or(v) => {
                for x in v {
                    if x.test(db, s, ss, pos, now)? {
                        return Ok(true);
                    }
                }
                false
            }
            Sp::Not(x) => !x.test(db, s, ss, pos, now)?,
        })
    }

    /// The answer from tool and outcome alone, or `None` if it needs other columns.
    fn shape_eval(&self, db: &Db, tool: u32, failed: bool, now: i64) -> Option<bool> {
        Some(match self {
            Sp::Const(b) => *b,
            Sp::Tool(t) => t[tool as usize],
            Sp::Generic(e) => {
                let name = String::from_utf8_lossy(db.step_dicts().tools.get(tool)).into_owned();
                eval(e, &ShapeCtx { tool: &name, failed, now }).ok()?.truthy()
            }
            Sp::And(v) => {
                let mut r = true;
                for x in v {
                    r &= x.shape_eval(db, tool, failed, now)?;
                }
                r
            }
            Sp::Or(v) => {
                let mut r = false;
                for x in v {
                    r |= x.shape_eval(db, tool, failed, now)?;
                }
                r
            }
            Sp::Not(x) => !x.shape_eval(db, tool, failed, now)?,
            Sp::Field { .. } | Sp::Present { .. } | Sp::Session { .. } => return None,
        })
    }

    /// A table over the 256 shape codes, when tool and outcome decide the predicate.
    /// Entries for the shared "other tool" code are `None` (test the step itself).
    pub fn shape_table(&self, db: &Db, now: i64) -> Option<[Option<bool>; 256]> {
        let mut t = [None; 256];
        let ntools = db.step_dicts().tools.len() as u32;
        for tool in 0..ntools.min(steps::SHAPE_OTHER as u32) {
            for failed in [false, true] {
                t[steps::StepDicts::shape_code(tool, failed) as usize] = Some(self.shape_eval(db, tool, failed, now)?);
            }
        }
        Some(t)
    }

    /// Tools a matching step must have, if the predicate pins them down.
    pub fn tool_set(&self) -> Option<Vec<bool>> {
        match self {
            Sp::Tool(t) => Some(t.to_vec()),
            Sp::Const(false) => Some(Vec::new()),
            Sp::And(v) => {
                let mut acc: Option<Vec<bool>> = None;
                for x in v {
                    if let Some(s) = x.tool_set() {
                        acc = Some(match acc {
                            None => s,
                            Some(a) => a.iter().enumerate().map(|(i, &b)| b && s.get(i).copied().unwrap_or(false)).collect(),
                        });
                    }
                }
                acc
            }
            Sp::Or(v) => {
                let mut acc: Vec<bool> = Vec::new();
                for x in v {
                    let s = x.tool_set()?;
                    if acc.len() < s.len() {
                        acc.resize(s.len(), false);
                    }
                    for (i, b) in s.into_iter().enumerate() {
                        acc[i] |= b;
                    }
                }
                Some(acc)
            }
            _ => None,
        }
    }

    /// True when no step of session `s` can pass (decided from session columns alone).
    pub fn session_false(&self, db: &Db, s: u32, ss: &SessionSteps, now: i64) -> Result<bool, QError> {
        Ok(match self {
            Sp::Const(b) => !*b,
            Sp::Session { uid, col, expr } => {
                let known = MEMO.with(|m| {
                    let mut m = m.borrow_mut();
                    m.for_session(s, ss);
                    m.table(*uid, 1)[0]
                });
                match known {
                    1 => true,
                    2 => false,
                    _ => {
                        let k = db.session_key(s);
                        let v = if *col == StepCol::Session { k.0 } else { k.1 };
                        let pass = eval(expr, &OneCol { col, v: Value::Str(v.as_str().into()), now })?.truthy();
                        MEMO.with(|m| m.borrow_mut().table(*uid, 1)[0] = if pass { 2 } else { 1 });
                        !pass
                    }
                }
            }
            Sp::And(v) => {
                for x in v {
                    if x.session_false(db, s, ss, now)? {
                        return Ok(true);
                    }
                }
                false
            }
            Sp::Or(v) => {
                for x in v {
                    if !x.session_false(db, s, ss, now)? {
                        return Ok(false);
                    }
                }
                true
            }
            _ => false,
        })
    }
}

/// A predicate ready to run over a session: a shape table when possible, else the tree.
pub struct Test {
    pub sp: Sp,
    pub shape: Option<[Option<bool>; 256]>,
    pub tools: Option<Vec<bool>>,
}

impl Test {
    pub fn new(db: &Db, sp: Sp, now: i64) -> Test {
        let shape = sp.shape_table(db, now);
        let tools = sp.tool_set();
        Test { sp, shape, tools }
    }

    #[inline]
    pub fn test(&self, db: &Db, s: u32, ss: &SessionSteps, pos: u32, now: i64) -> Result<bool, QError> {
        if let Some(t) = &self.shape
            && let Some(b) = t[ss.shape[pos as usize] as usize] {
                return Ok(b);
            }
        self.sp.test(db, s, ss, pos, now)
    }

    /// False when session `s` has none of the tools this test needs, or fails a session test.
    pub fn possible_in(&self, db: &Db, ss: &SessionSteps, s: u32, now: i64) -> Result<bool, QError> {
        let tools = match &self.tools {
            None => true,
            Some(t) => t.iter().enumerate().any(|(i, &b)| b && ss.has_tool(i as u32)),
        };
        Ok(tools && !self.sp.session_false(db, s, ss, now)?)
    }
}

pub fn and(a: &Sp, b: &Sp) -> Sp {
    match (a, b) {
        (Sp::Const(true), x) | (x, Sp::Const(true)) => x.clone(),
        _ => Sp::And(vec![a.clone(), b.clone()]),
    }
}

pub const NO_STEP: u32 = NONE;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse::parse;

    fn atom(q: &str) -> Expr {
        let Some(crate::query::parse::Stage::Where(e)) = parse(q).unwrap().stages.into_iter().next() else { panic!() };
        e
    }

    #[test]
    fn raw_contains_agrees_with_decoding() {
        // (raw text between the quotes, pattern, case-sensitive)
        let cases: &[(&str, &str, bool)] = &[
            ("cargo test", "test", false),
            ("cargo TEST", "test", false),
            ("cargo TEST", "test", true),
            (r"run \test", "test", false),
            (r"run \\test", "test", false),
            (r"run \\\test", "test", false),
            (r"a\/b", "a/b", false),
            (r"\u0067it commit", "git commit", false),
            ("echo \u{212a}elvin", "kelvin", false),
            ("echo \u{130}stanbul", "istanbul", false),
            ("café — ok", "ok", false),
            ("café — OK", "ok", false),
            (r#"say \"hi\""#, "hi", false),
            (r"line\nnext", "nnext", false),
            (r"line\nnext", "next", false),
            // a hit among the digits of an escape is not one; right after the escape it is
            (r"caf\u00e9dit", "9dit", false),
            (r"caf\u00e9dit", "e9d", false),
            (r"caf\u00e9dit", "dit", false),
            (r"caf\u00e9 git commit", "git commit", false),
            (r"a\\u0041b", "0041", false),
            (r"x\/y git commit", "git commit", false),
            ("\u{212a}elvin kelvin", "kelvin", false),
        ];
        for &(body, p, cs) in cases {
            let decoded = crate::query::value::json_unescape(body.as_bytes());
            let want = crate::query::value::str_op(crate::query::parse::StrOp::Contains, &decoded, p, cs);
            if let Some(got) = raw_contains(body.as_bytes(), p.as_bytes(), cs) {
                assert_eq!(got, want, "{body:?} contains {p:?} (cs {cs})");
            }
            let raw = format!("\"{body}\"");
            let e = atom(&format!("where command {} {p:?}", if cs { "contains_cs" } else { "contains" }));
            assert_eq!(test_raw(&e, "command", raw.as_bytes(), 0).unwrap(), want, "{body:?} contains {p:?} (cs {cs})");
        }
        // what a raw search cannot decide is handed back
        assert_eq!(raw_contains(br"\u0067it", b"git", false), None);
        assert_eq!(raw_contains(br"a\/b", b"a/b", false), None);
        assert_eq!(raw_contains(br"caf\u00e9dit", b"9dit", false), None);
        // a hit that stands clear of every escape is taken where it is found
        assert_eq!(raw_contains(br"caf\u00e9dit", b"dit", false), Some(true));
        assert_eq!(raw_contains(br"git commit \u00e9", b"git commit", false), Some(true));
        assert_eq!(raw_contains("kelvin \u{212a}".as_bytes(), b"kelvin", false), Some(true));
        assert_eq!(raw_contains("\u{212a}".as_bytes(), b"k", false), None);
        assert_eq!(raw_contains("\u{212a}".as_bytes(), b"k", true), Some(false));
    }

    #[test]
    fn raw_string_tests_agree_with_decoding() {
        let e = atom(r#"where command contains "test""#);
        let t = |raw: &[u8]| test_raw(&e, "command", raw, 0).unwrap();
        assert!(t(br#""cargo test --all""#));
        assert!(t(br#""cargo TEST""#));
        assert!(!t(br#""cargo tes""#));
        // the escape \t is not the letter t: no match after decoding
        assert!(!t(br#""run \test""#));
        // a \u escape can hide a match from the raw text
        assert!(t(br#""test""#));
        // non-ASCII text takes the decoding path
        assert!(t("\"\u{dc}nit test\"".as_bytes()));
        assert!(t(br#""line one\nthen test""#));
        let n = atom(r#"where not (command contains "test")"#);
        let Expr::Not(inner) = n else { panic!() };
        assert!(!test_raw(&inner, "command", br#""x""#, 0).unwrap());
    }
}
