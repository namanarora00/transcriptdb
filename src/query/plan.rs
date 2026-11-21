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
    let r = ss.rows[pos as usize];
    let find = |m: &[u8]| -> Option<(usize, usize)> {
        let (calls, _) = steps::scan_message(m);
        let c = calls.iter().find(|c| c.block == r.call_block)?;
        let (_, v) = c.fields.iter().find(|(k, _)| *k == name.as_bytes())?;
        let start = v.as_ptr() as usize - m.as_ptr() as usize;
        Some((start, start + v.len()))
    };
    let (stored, holes) = db.stored_message(s, r.call_node).map_err(|e| QError::new(e.to_string()))?;
    match find(&stored) {
        None => Ok(None),
        Some((a, b)) if !holes.iter().any(|&h| (h as usize) >= a && (h as usize) <= b) => Ok(Some(stored[a..b].to_vec())),
        Some(_) => {
            let m = db.message(s, r.call_node).map_err(|e| QError::new(e.to_string()))?;
            Ok(find(&m).map(|(a, b)| m[a..b].to_vec()))
        }
    }
}

/// The value of one step column.
pub fn step_value(db: &Db, s: u32, pos: u32, col: &StepCol) -> Result<Value, QError> {
    let ss = db.steps(s);
    let r = ss.rows[pos as usize];
    let sd = db.step_dicts();
    Ok(match col {
        StepCol::Tool => Value::Str(String::from_utf8_lossy(sd.tools.get(r.tool)).into()),
        StepCol::Failed => Value::Bool(r.failed),
        StepCol::Ok => Value::Bool(!r.failed),
        StepCol::Time => Value::Time(r.time_ms),
        StepCol::OutBytes => Value::Int(r.out_bytes as i64),
        StepCol::Step => Value::Int(pos as i64),
        StepCol::Session => Value::Str(db.session_key(s).0.as_str().into()),
        StepCol::Agent => Value::Str(db.session_key(s).1.as_str().into()),
        StepCol::Field(name) => {
            let Some(f) = sd.fields.code(name.as_bytes()) else { return Ok(Value::Null) };
            match ss.fields_of(pos).iter().find(|x| x.0 == f) {
                None => Value::Null,
                Some(&(_, v)) if v == steps::LONG => match read_input(db, s, ss, pos, name)? {
                    Some(t) => Value::from_json(&t),
                    None => Value::Null,
                },
                Some(&(_, v)) => Value::from_json(sd.values[f as usize].get(v)),
            }
        }
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
    /// By value code of input field `f`; `missing` when the step lacks the field; long values
    /// are read from the message and tested with `expr`.
    Field { name: String, f: Option<u32>, table: Arc<[bool]>, missing: bool, expr: Arc<Expr> },
    /// By session index.
    Session(Arc<[bool]>),
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
    /// Compiled one-column tests, so a test repeated in one query is evaluated over its
    /// dictionary once. Cleared at the start of every query.
    static ATOMS: std::cell::RefCell<std::collections::HashMap<String, Sp>> = std::cell::RefCell::new(std::collections::HashMap::new());
}

pub fn clear_cache() {
    ATOMS.with(|c| c.borrow_mut().clear());
}

thread_local! {
    static THREADS: std::cell::Cell<u32> = const { std::cell::Cell::new(1) };
}

/// How many threads planning may use (set per query).
pub fn set_threads(n: usize) {
    THREADS.with(|t| t.set(n.max(1) as u32));
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
                let t: Vec<bool> = (0..sd.tools.len() as u32)
                    .map(|c| eval(e, &OneCol { col: &col, v: Value::Str(String::from_utf8_lossy(sd.tools.get(c)).into()), now }).map(|v| v.truthy()))
                    .collect::<Result<_, _>>()?;
                Sp::Tool(t.into())
            }
            Some(StepCol::Field(name)) => {
                let col = StepCol::Field(name.clone());
                let f = sd.fields.code(name.as_bytes());
                let table: Vec<bool> = match f {
                    Some(f) => {
                        let d = &sd.values[f as usize];
                        let one = |c: u32| eval(e, &OneCol { col: &col, v: Value::from_json(d.get(c)), now }).map(|v| v.truthy());
                        let n = d.len() as u32;
                        let threads = THREADS.with(|t| t.get());
                        if n < 4096 || threads == 1 {
                            (0..n).map(one).collect::<Result<_, _>>()?
                        } else {
                            // large dictionaries (long text values): evaluate in parallel
                            let per = n.div_ceil(threads);
                            let parts: Vec<Result<Vec<bool>, QError>> = std::thread::scope(|sc| {
                                let hs: Vec<_> = (0..threads).map(|t| sc.spawn(move || (t * per..((t + 1) * per).min(n)).map(one).collect::<Result<Vec<bool>, QError>>())).collect();
                                hs.into_iter().map(|h| h.join().unwrap()).collect()
                            });
                            let mut all = Vec::with_capacity(n as usize);
                            for p in parts {
                                all.extend(p?);
                            }
                            all
                        }
                    }
                    None => Vec::new(),
                };
                let missing = eval(e, &OneCol { col: &col, v: Value::Null, now })?.truthy();
                Sp::Field { name, f, table: table.into(), missing, expr: Arc::new(e.clone()) }
            }
            Some(c @ (StepCol::Session | StepCol::Agent)) => {
                let t: Vec<bool> = (0..db.session_count() as u32)
                    .map(|s| {
                        let k = db.session_key(s);
                        let v = if c == StepCol::Session { &k.0 } else { &k.1 };
                        eval(e, &OneCol { col: &c, v: Value::Str(v.as_str().into()), now }).map(|v| v.truthy())
                    })
                    .collect::<Result<_, _>>()?;
                Sp::Session(t.into())
            }
            _ => Sp::Generic(Arc::new(e.clone())),
        }
    })
}

impl Sp {
    pub fn test(&self, db: &Db, s: u32, ss: &SessionSteps, pos: u32, now: i64) -> Result<bool, QError> {
        Ok(match self {
            Sp::Const(b) => *b,
            Sp::Tool(t) => t[ss.rows[pos as usize].tool as usize],
            Sp::Field { name, f, table, missing, expr } => {
                let Some(f) = f else { return Ok(*missing) };
                match ss.fields_of(pos).iter().find(|x| x.0 == *f) {
                    None => *missing,
                    Some(&(_, v)) if v == steps::LONG => {
                        let col = StepCol::Field(name.clone());
                        let val = match read_input(db, s, ss, pos, name)? {
                            Some(t) => Value::from_json(&t),
                            None => Value::Null,
                        };
                        eval(expr, &OneCol { col: &col, v: val, now })?.truthy()
                    }
                    Some(&(_, v)) => table[v as usize],
                }
            }
            Sp::Session(t) => t[s as usize],
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
            Sp::Field { .. } | Sp::Session(_) => return None,
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
    pub fn session_false(&self, s: u32) -> bool {
        match self {
            Sp::Const(b) => !*b,
            Sp::Session(t) => !t[s as usize],
            Sp::And(v) => v.iter().any(|x| x.session_false(s)),
            Sp::Or(v) => v.iter().all(|x| x.session_false(s)),
            _ => false,
        }
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

    /// False when session `s` has none of the tools this test needs.
    pub fn possible_in(&self, ss: &SessionSteps, s: u32) -> bool {
        if self.sp.session_false(s) {
            return false;
        }
        match &self.tools {
            None => true,
            Some(t) => t.iter().enumerate().any(|(i, &b)| b && ss.has_tool(i as u32)),
        }
    }
}

pub fn and(a: &Sp, b: &Sp) -> Sp {
    match (a, b) {
        (Sp::Const(true), x) | (x, Sp::Const(true)) => x.clone(),
        _ => Sp::And(vec![a.clone(), b.clone()]),
    }
}

pub const NO_STEP: u32 = NONE;
