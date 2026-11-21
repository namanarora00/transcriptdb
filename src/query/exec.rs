//! Running a parsed query: leading `where` stages become a compiled step filter, `match`
//! runs the matcher per session (in parallel), and later stages work on rows.

use super::matcher::{self, Match, Spec, MAX_ELEMS, UNBOUNDED};
use super::parse::{AggFn, Expr, Link, Pattern, Query, Stage};
use super::plan::{self, resolve, step_value, Sp, StepCol, Test};
use super::value::{eval, Ctx, Key, Value};
use super::QError;
use crate::db::Db;
use crate::record::NONE;
use crate::steps;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Steps,
    Matches(Vec<String>),
    Plain,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub session: u32,
    pub steps: [u32; MAX_ELEMS],
    pub vals: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub kind: Kind,
    pub cols: Vec<String>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Default)]
pub struct QStats {
    pub sessions: usize,
    pub sessions_skipped: usize,
    pub steps_scanned: usize,
    pub matches: usize,
    pub plan_ms: f64,
    pub scan_ms: f64,
    pub rest_ms: f64,
}

pub struct RowCtx<'a> {
    db: &'a Db,
    t: &'a Table,
    row: &'a Row,
    now: i64,
}

impl Ctx for RowCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        if path.len() == 1 {
            if let Some(i) = self.t.cols.iter().position(|c| *c == path[0]) {
                return Ok(self.row.vals.get(i).cloned().unwrap_or(Value::Null));
            }
        }
        match &self.t.kind {
            Kind::Plain => Err(QError::new(format!("unknown column '{}'", path.join(".")))),
            Kind::Steps => step_value(self.db, self.row.session, self.row.steps[0], &resolve(path)?),
            Kind::Matches(vars) => {
                if let Some(k) = vars.iter().position(|v| *v == path[0]) {
                    if path.len() == 1 {
                        return Ok(Value::Int(self.row.steps[k] as i64));
                    }
                    return step_value(self.db, self.row.session, self.row.steps[k], &resolve(&path[1..])?);
                }
                match path {
                    [x] if x == "session" || x == "agent" => step_value(self.db, self.row.session, self.row.steps[0], &resolve(path)?),
                    _ => Err(QError::new(format!("unknown column '{}' (name an element: {}.{})", path.join("."), vars.first().map(|s| s.as_str()).unwrap_or("a"), path.join(".")))),
                }
            }
        }
    }
    fn now(&self) -> i64 {
        self.now
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Check every column a step-level expression names: built-ins, or input fields the store has seen.
fn check_step_expr(db: &Db, e: &Expr) -> Result<(), QError> {
    let mut refs = Vec::new();
    e.refs(&mut refs);
    for r in refs {
        if let StepCol::Field(f) = resolve(r)? {
            if db.step_dicts().fields.code(f.as_bytes()).is_none() {
                let mut names: Vec<String> = plan::BUILTINS.iter().map(|s| s.to_string()).collect();
                let sd = db.step_dicts();
                for i in 0..sd.fields.len() as u32 {
                    names.push(String::from_utf8_lossy(sd.fields.get(i)).into_owned());
                }
                let close: Vec<&String> = names.iter().filter(|n| n.contains(&f[..f.len().min(3)])).take(6).collect();
                return Err(QError::new(format!("no step has a column or input field '{f}'{}", if close.is_empty() { String::new() } else { format!(" (similar: {})", close.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")) })));
            }
        }
    }
    type_check(e)
}

/// Comparisons between a built-in column and a literal of the wrong kind.
fn type_check(e: &Expr) -> Result<(), QError> {
    use super::parse::Expr as E;
    let kind = |x: &E| -> Option<&'static str> {
        match x {
            E::Ref(p) if p.len() == 1 => match p[0].as_str() {
                "failed" | "ok" => Some("bool"),
                "time" => Some("time"),
                "out_bytes" | "step" => Some("number"),
                "tool" | "session" | "agent" => Some("string"),
                _ => None,
            },
            E::Lit(v) if !matches!(v, Value::Null) => Some(v.type_name()),
            E::Call(f, _) if f == "ago" || f == "now" => Some("time"),
            _ => None,
        }
    };
    match e {
        E::Cmp(_, a, b) => {
            if let (Some(x), Some(y)) = (kind(a), kind(b)) {
                if x != y {
                    return Err(QError::new(format!("cannot compare {x} with {y}")));
                }
            }
            type_check(a)?;
            type_check(b)
        }
        E::And(v) | E::Or(v) => v.iter().try_for_each(type_check),
        E::Not(x) => type_check(x),
        _ => Ok(()),
    }
}

pub struct Compiled {
    pub spec: Spec,
    pub elems: Vec<Test>,
    pub excl: Vec<Test>,
    pub vars: Vec<String>,
    pub eq: Option<(String, Vec<usize>)>,
}

pub fn compile_pattern(db: &Db, p: &Pattern, pre: &Sp, now: i64) -> Result<Compiled, QError> {
    if p.elems.len() > MAX_ELEMS {
        return Err(QError::new("a match can have at most 8 elements"));
    }
    if p.excl.len() > 32 {
        return Err(QError::new("too many exclusions"));
    }
    let mut spec = Spec { n: p.elems.len(), min: [1; MAX_ELEMS], max: [1; MAX_ELEMS], next: [false; MAX_ELEMS], within: p.within, excl: Vec::new() };
    let mut elems = Vec::new();
    let mut vars = Vec::new();
    for (k, e) in p.elems.iter().enumerate() {
        check_step_expr(db, &e.pred)?;
        spec.min[k] = e.min;
        spec.max[k] = e.max.unwrap_or(UNBOUNDED);
        spec.next[k] = k > 0 && p.links[k] == Link::Next;
        elems.push(Test::new(db, plan::and(pre, &plan::compile(db, &e.pred, now)?), now));
        vars.push(e.name.clone().unwrap_or_else(|| format!("e{k}")));
    }
    let mut excl = Vec::new();
    for (x, i, j) in &p.excl {
        check_step_expr(db, x)?;
        spec.excl.push((*i, *j));
        excl.push(Test::new(db, plan::and(pre, &plan::compile(db, x, now)?), now));
    }
    let eq = match &p.eq {
        None => None,
        Some((path, ks)) => match resolve(path)? {
            StepCol::Field(f) => Some((f, ks.clone())),
            _ => return Err(QError::new("element comparisons work on input fields for now (such as a.file_path == c.file_path)")),
        },
    };
    Ok(Compiled { spec, elems, excl, vars, eq })
}

/// Partition key of a step for an equality constraint: the dictionary code, or the full
/// text for long values; `None` if the step lacks the field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EqKey {
    Code(u32),
    Raw(Vec<u8>),
}

pub fn eq_key(db: &Db, s: u32, pos: u32, field: &str) -> Result<Option<EqKey>, QError> {
    let ss = db.steps(s);
    let Some(f) = db.step_dicts().fields.code(field.as_bytes()) else { return Ok(None) };
    Ok(match ss.fields_of(pos).iter().find(|x| x.0 == f) {
        None => None,
        Some(&(_, v)) if v == steps::LONG => plan::read_input(db, s, ss, pos, field)?.map(EqKey::Raw),
        Some(&(_, v)) => Some(EqKey::Code(v)),
    })
}

/// Matches of a compiled pattern in one session.
pub fn session_matches(db: &Db, c: &Compiled, s: u32, now: i64, scanned: &AtomicUsize) -> Result<Option<Vec<Match>>, QError> {
    let ss = db.steps(s);
    if ss.rows.is_empty() || !c.elems.iter().all(|t| t.possible_in(ss, s)) {
        return Ok(None);
    }
    scanned.fetch_add(ss.rows.len(), Ordering::Relaxed);
    let mut masks = vec![0u64; ss.rows.len()];
    for (pos, m) in masks.iter_mut().enumerate() {
        let pos = pos as u32;
        for (k, t) in c.elems.iter().enumerate() {
            if t.test(db, s, ss, pos, now)? {
                *m |= 1 << k;
            }
        }
        for (x, t) in c.excl.iter().enumerate() {
            if t.test(db, s, ss, pos, now)? {
                *m |= 1 << (32 + x);
            }
        }
    }
    let mut out = Vec::new();
    match &c.eq {
        None => matcher::run(&c.spec, ss, &masks, &mut out),
        Some((field, ks)) => {
            let bits: u64 = ks.iter().fold(0, |a, &k| a | 1 << k);
            let mut keys: Vec<Option<EqKey>> = Vec::with_capacity(ss.rows.len());
            let mut distinct: Vec<EqKey> = Vec::new();
            for (pos, m) in masks.iter().enumerate() {
                let k = if m & bits != 0 { eq_key(db, s, pos as u32, field)? } else { None };
                if let Some(k) = &k {
                    if !distinct.contains(k) {
                        distinct.push(k.clone());
                    }
                }
                keys.push(k);
            }
            for v in distinct {
                let part: Vec<u64> = masks.iter().zip(&keys).map(|(m, k)| if k.as_ref() == Some(&v) { *m } else { m & !bits }).collect();
                matcher::run(&c.spec, ss, &part, &mut out);
            }
            out.sort();
            out.dedup();
        }
    }
    Ok(Some(out))
}

fn parallel<T: Send>(n: usize, threads: usize, f: impl Fn(u32) -> Result<T, QError> + Sync) -> Result<Vec<(u32, T)>, QError> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<(u32, T)>> = Mutex::new(Vec::new());
    let err: Mutex<Option<QError>> = Mutex::new(None);
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| {
                let mut local = Vec::new();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    match f(i as u32) {
                        Ok(v) => local.push((i as u32, v)),
                        Err(e) => {
                            *err.lock().unwrap() = Some(e);
                            break;
                        }
                    }
                }
                out.lock().unwrap().extend(local);
            });
        }
    });
    if let Some(e) = err.into_inner().unwrap() {
        return Err(e);
    }
    let mut v = out.into_inner().unwrap();
    v.sort_by_key(|x| x.0);
    Ok(v)
}

pub struct Run {
    pub table: Table,
    pub stats: QStats,
}

pub fn run(db: &Db, q: &Query, threads: usize) -> Result<Run, QError> {
    let now = now_ms();
    let t0 = Instant::now();
    let mut st = QStats { sessions: db.session_count(), ..QStats::default() };
    // leading where stages filter steps
    let mut i = 0;
    let mut pre = Sp::Const(true);
    while let Some(Stage::Where(e)) = q.stages.get(i) {
        check_step_expr(db, e)?;
        pre = plan::and(&pre, &plan::compile(db, e, now)?);
        i += 1;
    }
    let scanned = AtomicUsize::new(0);
    let mut table = match q.stages.get(i) {
        Some(Stage::Match(p)) => {
            i += 1;
            let c = compile_pattern(db, p, &pre, now)?;
            st.plan_ms = t0.elapsed().as_secs_f64() * 1e3;
            let t1 = Instant::now();
            let per = parallel(db.session_count(), threads, |s| session_matches(db, &c, s, now, &scanned))?;
            st.scan_ms = t1.elapsed().as_secs_f64() * 1e3;
            let mut rows = Vec::new();
            for (s, m) in per {
                match m {
                    None => st.sessions_skipped += 1,
                    Some(ms) => rows.extend(ms.into_iter().map(|m| Row { session: s, steps: m.binds, vals: Vec::new() })),
                }
            }
            st.matches = rows.len();
            Table { kind: Kind::Matches(c.vars), cols: Vec::new(), rows }
        }
        _ => {
            let test = Test::new(db, pre, now);
            st.plan_ms = t0.elapsed().as_secs_f64() * 1e3;
            let t1 = Instant::now();
            let per = parallel(db.session_count(), threads, |s| {
                let ss = db.steps(s);
                if !test.possible_in(ss, s) {
                    return Ok(None);
                }
                scanned.fetch_add(ss.rows.len(), Ordering::Relaxed);
                let mut v = Vec::new();
                for pos in 0..ss.rows.len() as u32 {
                    if test.test(db, s, ss, pos, now)? {
                        v.push(pos);
                    }
                }
                Ok(Some(v))
            })?;
            st.scan_ms = t1.elapsed().as_secs_f64() * 1e3;
            let mut rows = Vec::new();
            for (s, v) in per {
                match v {
                    None => st.sessions_skipped += 1,
                    Some(v) => rows.extend(v.into_iter().map(|p| {
                        let mut b = [NONE; MAX_ELEMS];
                        b[0] = p;
                        Row { session: s, steps: b, vals: Vec::new() }
                    })),
                }
            }
            Table { kind: Kind::Steps, cols: Vec::new(), rows }
        }
    };
    st.steps_scanned = scanned.load(Ordering::Relaxed);
    let t2 = Instant::now();
    for stage in &q.stages[i..] {
        table = apply_stage(db, table, stage, now)?;
    }
    st.rest_ms = t2.elapsed().as_secs_f64() * 1e3;
    Ok(Run { table, stats: st })
}

fn eval_row(db: &Db, t: &Table, row: &Row, e: &Expr, now: i64) -> Result<Value, QError> {
    eval(e, &RowCtx { db, t, row, now })
}

fn apply_stage(db: &Db, mut t: Table, stage: &Stage, now: i64) -> Result<Table, QError> {
    Ok(match stage {
        Stage::Where(e) => {
            let mut keep = Vec::with_capacity(t.rows.len());
            for r in &t.rows {
                keep.push(eval_row(db, &t, r, e, now)?.truthy());
            }
            let mut k = keep.into_iter();
            t.rows.retain(|_| k.next().unwrap());
            t
        }
        Stage::Match(_) => return Err(QError::new("match must come first (after any where stages)")),
        Stage::Extend(v) => {
            for (name, e) in v {
                let mut vals = Vec::with_capacity(t.rows.len());
                for r in &t.rows {
                    vals.push(eval_row(db, &t, r, e, now)?);
                }
                let idx = t.cols.iter().position(|c| c == name);
                for (r, v) in t.rows.iter_mut().zip(vals) {
                    match idx {
                        Some(i) => r.vals[i] = v,
                        None => r.vals.push(v),
                    }
                }
                if idx.is_none() {
                    t.cols.push(name.clone());
                }
            }
            t
        }
        Stage::Project(v) => {
            let mut rows = Vec::with_capacity(t.rows.len());
            for r in &t.rows {
                let vals = v.iter().map(|(_, e)| eval_row(db, &t, r, e, now)).collect::<Result<Vec<_>, _>>()?;
                rows.push(Row { session: r.session, steps: r.steps, vals });
            }
            Table { kind: Kind::Plain, cols: v.iter().map(|(n, _)| n.clone()).collect(), rows }
        }
        Stage::Summarize { aggs, by } => {
            struct Acc {
                key: Vec<Value>,
                count: Vec<i64>,
                sum: Vec<f64>,
                min: Vec<Option<Value>>,
                max: Vec<Option<Value>>,
                distinct: Vec<std::collections::HashSet<Key>>,
                all_int: Vec<bool>,
                all_dur: Vec<bool>,
            }
            let mut groups: HashMap<Vec<Key>, Acc> = HashMap::new();
            let mut order: Vec<Vec<Key>> = Vec::new();
            for r in &t.rows {
                let kv = by.iter().map(|(_, e)| eval_row(db, &t, r, e, now)).collect::<Result<Vec<_>, _>>()?;
                let kk: Vec<Key> = kv.iter().map(|v| v.key()).collect();
                let acc = groups.entry(kk.clone()).or_insert_with(|| {
                    order.push(kk.clone());
                    Acc {
                        key: kv.clone(),
                        count: vec![0; aggs.len()],
                        sum: vec![0.0; aggs.len()],
                        min: vec![None; aggs.len()],
                        max: vec![None; aggs.len()],
                        distinct: vec![Default::default(); aggs.len()],
                        all_int: vec![true; aggs.len()],
                        all_dur: vec![true; aggs.len()],
                    }
                });
                for (ai, (_, f, arg)) in aggs.iter().enumerate() {
                    let v = match arg {
                        Some(e) => eval_row(db, &t, r, e, now)?,
                        None => Value::Null,
                    };
                    match f {
                        AggFn::Count => acc.count[ai] += 1,
                        AggFn::CountIf => acc.count[ai] += v.truthy() as i64,
                        AggFn::DCount => {
                            if !matches!(v, Value::Null) {
                                acc.distinct[ai].insert(v.key());
                            }
                        }
                        AggFn::Sum | AggFn::Avg => match v {
                            Value::Int(x) => {
                                acc.sum[ai] += x as f64;
                                acc.count[ai] += 1;
                                acc.all_dur[ai] = false;
                            }
                            Value::Float(x) => {
                                acc.sum[ai] += x;
                                acc.count[ai] += 1;
                                acc.all_int[ai] = false;
                                acc.all_dur[ai] = false;
                            }
                            Value::Dur(x) => {
                                acc.sum[ai] += x as f64;
                                acc.count[ai] += 1;
                            }
                            _ => {}
                        },
                        AggFn::Min | AggFn::Max => {
                            if !matches!(v, Value::Null) {
                                let slot = if *f == AggFn::Min { &mut acc.min[ai] } else { &mut acc.max[ai] };
                                let replace = match slot {
                                    None => true,
                                    Some(cur) => {
                                        let o = v.order(cur);
                                        if *f == AggFn::Min { o.is_lt() } else { o.is_gt() }
                                    }
                                };
                                if replace {
                                    *slot = Some(v);
                                }
                            }
                        }
                    }
                }
            }
            if by.is_empty() && groups.is_empty() {
                order.push(Vec::new());
                groups.insert(
                    Vec::new(),
                    Acc { key: Vec::new(), count: vec![0; aggs.len()], sum: vec![0.0; aggs.len()], min: vec![None; aggs.len()], max: vec![None; aggs.len()], distinct: vec![Default::default(); aggs.len()], all_int: vec![true; aggs.len()], all_dur: vec![true; aggs.len()] },
                );
            }
            let mut rows = Vec::with_capacity(order.len());
            for k in order {
                let a = &groups[&k];
                let mut vals = a.key.clone();
                for (ai, (_, f, _)) in aggs.iter().enumerate() {
                    vals.push(match f {
                        AggFn::Count | AggFn::CountIf => Value::Int(a.count[ai]),
                        AggFn::DCount => Value::Int(a.distinct[ai].len() as i64),
                        AggFn::Sum if a.all_dur[ai] && a.count[ai] > 0 => Value::Dur(a.sum[ai] as i64),
                        AggFn::Avg if a.all_dur[ai] && a.count[ai] > 0 => Value::Dur((a.sum[ai] / a.count[ai] as f64) as i64),
                        AggFn::Sum => {
                            if a.all_int[ai] {
                                Value::Int(a.sum[ai] as i64)
                            } else {
                                Value::Float(a.sum[ai])
                            }
                        }
                        AggFn::Avg => {
                            if a.count[ai] == 0 {
                                Value::Null
                            } else {
                                Value::Float(a.sum[ai] / a.count[ai] as f64)
                            }
                        }
                        AggFn::Min => a.min[ai].clone().unwrap_or(Value::Null),
                        AggFn::Max => a.max[ai].clone().unwrap_or(Value::Null),
                    });
                }
                rows.push(Row { session: NONE, steps: [NONE; MAX_ELEMS], vals });
            }
            let cols = by.iter().map(|(n, _)| n.clone()).chain(aggs.iter().map(|(n, _, _)| n.clone())).collect();
            Table { kind: Kind::Plain, cols, rows }
        }
        Stage::Top { n, by, desc } => {
            let t = sort_rows(db, t, &[(by.clone(), *desc)], now)?;
            take(t, *n)
        }
        Stage::Sort(keys) => sort_rows(db, t, keys, now)?,
        Stage::Take(n) => take(t, *n),
        Stage::Count => Table { kind: Kind::Plain, cols: vec!["count".into()], rows: vec![Row { session: NONE, steps: [NONE; MAX_ELEMS], vals: vec![Value::Int(t.rows.len() as i64)] }] },
    })
}

fn take(mut t: Table, n: usize) -> Table {
    t.rows.truncate(n);
    t
}

fn sort_rows(db: &Db, mut t: Table, keys: &[(Expr, bool)], now: i64) -> Result<Table, QError> {
    let mut ks: Vec<(Vec<Value>, usize)> = Vec::with_capacity(t.rows.len());
    for (i, r) in t.rows.iter().enumerate() {
        ks.push((keys.iter().map(|(e, _)| eval_row(db, &t, r, e, now)).collect::<Result<_, _>>()?, i));
    }
    ks.sort_by(|a, b| {
        for (j, (_, desc)) in keys.iter().enumerate() {
            let o = a.0[j].order(&b.0[j]);
            let o = if *desc { o.reverse() } else { o };
            if o.is_ne() {
                return o;
            }
        }
        a.1.cmp(&b.1)
    });
    let mut rows: Vec<Option<Row>> = t.rows.into_iter().map(Some).collect();
    t.rows = ks.into_iter().map(|(_, i)| rows[i].take().unwrap()).collect();
    Ok(t)
}

/// Headers and cells for printing.
pub fn render(db: &Db, t: &Table, limit: usize) -> Result<(Vec<String>, Vec<Vec<String>>), QError> {
    let now = now_ms();
    let mut heads: Vec<String> = Vec::new();
    let mut exprs: Vec<Expr> = Vec::new();
    let r = |s: &str| Expr::Ref(s.split('.').map(|x| x.to_string()).collect());
    match &t.kind {
        Kind::Steps => {
            for c in ["session", "step", "tool", "failed", "time", "out_bytes"] {
                heads.push(c.into());
                exprs.push(r(c));
            }
        }
        Kind::Matches(vars) => {
            heads.push("session".into());
            exprs.push(r("session"));
            for v in vars {
                heads.push(v.clone());
                exprs.push(r(v));
            }
            let last = vars.last().unwrap();
            heads.push(format!("{last}.time"));
            exprs.push(r(&format!("{last}.time")));
        }
        Kind::Plain => {}
    }
    for c in &t.cols {
        heads.push(c.clone());
        exprs.push(Expr::Ref(vec![c.clone()]));
    }
    let mut cells = Vec::new();
    for row in t.rows.iter().take(limit) {
        let mut line = Vec::new();
        for e in &exprs {
            line.push(eval_row(db, t, row, e, now)?.to_string());
        }
        cells.push(line);
    }
    Ok((heads, cells))
}
