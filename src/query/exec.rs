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
    /// Segments whose step summaries answered the query (0: steps were scanned).
    pub summary_segments: usize,
    pub summarized: bool,
}

thread_local! {
    static SUMMARIES: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Whether queries on this thread may read step summaries (on by default). Checks turn it
/// off to compare against the step scan.
pub fn set_summaries(on: bool) {
    SUMMARIES.with(|c| c.set(on));
}

pub struct RowCtx<'a> {
    db: &'a Db,
    t: &'a Table,
    row: &'a Row,
    now: i64,
}

impl Ctx for RowCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        if path.len() == 1
            && let Some(i) = self.t.cols.iter().position(|c| *c == path[0]) {
                return Ok(self.row.vals.get(i).cloned().unwrap_or(Value::Null));
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

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Check every column a step-level expression names: built-ins, or input fields the store has seen.
fn check_step_expr(db: &Db, e: &Expr) -> Result<(), QError> {
    let mut refs = Vec::new();
    e.refs(&mut refs);
    for r in refs {
        if let StepCol::Field(f) = resolve(r)?
            && db.step_dicts().fields.code(f.as_bytes()).is_none() {
                let mut names: Vec<String> = plan::BUILTINS.iter().map(|s| s.to_string()).collect();
                let sd = db.step_dicts();
                for i in 0..sd.fields.len() as u32 {
                    names.push(String::from_utf8_lossy(sd.fields.get(i)).into_owned());
                }
                let close: Vec<&String> = names.iter().filter(|n| n.contains(&f[..f.len().min(3)])).take(6).collect();
                return Err(QError::new(format!("no step has a column or input field '{f}'{}", if close.is_empty() { String::new() } else { format!(" (similar: {})", close.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")) })));
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
            if let (Some(x), Some(y)) = (kind(a), kind(b))
                && x != y {
                    return Err(QError::new(format!("cannot compare {x} with {y}")));
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
    /// Per shape code, a step's whole mask when tool and outcome decide every test.
    pub shape_masks: Box<[Option<u64>; 256]>,
    /// Whether a test reads input values (else sessions' steps are read without them).
    pub reads_values: bool,
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
    let mut shape_masks = Box::new([None; 256]);
    for (code, slot) in shape_masks.iter_mut().enumerate() {
        let mut m = 0u64;
        let mut all = true;
        for (bit, t) in elems.iter().enumerate().chain(excl.iter().enumerate().map(|(x, t)| (32 + x, t))) {
            match t.shape.as_ref().and_then(|tab| tab[code]) {
                Some(true) => m |= 1 << bit,
                Some(false) => {}
                None => all = false,
            }
        }
        if all {
            *slot = Some(m);
        }
    }
    let reads_values = elems.iter().chain(&excl).any(|t| t.sp.reads_values());
    Ok(Compiled { spec, elems, excl, vars, eq, shape_masks, reads_values })
}

/// Partition key of a step for an equality constraint: the dictionary code, or the full
/// text for long values; `None` if the step lacks the field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EqKey {
    Code(u32),
    Raw(Vec<u8>),
}

pub fn eq_key(db: &Db, s: u32, pos: u32, field: &str) -> Result<Option<EqKey>, QError> {
    let ss_arc = db.steps(s);
    let ss = &*ss_arc;
    let Some(f) = db.step_dicts().fields.code(field.as_bytes()) else { return Ok(None) };
    Ok(match ss.fields_of(pos).iter().find(|x| x.0 == f) {
        None => None,
        Some(&(_, v)) if steps::is_long(v) => plan::long_value(db, s, ss, pos, v, field)?.map(EqKey::Raw),
        Some(&(_, v)) => Some(EqKey::Code(v)),
    })
}

/// Matches of a compiled pattern in one session.
pub fn session_matches(db: &Db, c: &Compiled, s: u32, now: i64, scanned: &AtomicUsize) -> Result<Option<Vec<Match>>, QError> {
    let ss_arc = if c.reads_values { db.try_steps(s) } else { db.light_steps(s) }.map_err(|e| QError::new(e.to_string()))?;
    let ss = &*ss_arc;
    if ss.rows.is_empty() {
        return Ok(None);
    }
    for t in &c.elems {
        if !t.possible_in(db, ss, s, now)? {
            return Ok(None);
        }
    }
    scanned.fetch_add(ss.rows.len(), Ordering::Relaxed);
    let mut sc = SCRATCH.with(|b| std::mem::take(&mut *b.borrow_mut()));
    // the masks are all zero between sessions: only the steps a session wrote are cleared
    let n = ss.rows.len();
    if sc.masks.len() < n {
        sc.masks.resize(n, 0);
    }
    let r = session_matches_in(db, c, s, ss, now, &mut sc);
    if c.eq.is_none() || r.is_err() {
        sc.masks[..n].fill(0);
    } else {
        let by = ss.by_shape();
        for code in 0..=255u8 {
            if c.shape_masks[code as usize] != Some(0) {
                for &p in by.of(code) {
                    sc.masks[p as usize] = 0;
                }
            }
        }
    }
    SCRATCH.with(|b| *b.borrow_mut() = sc);
    r
}

/// Working arrays of one session's matching, kept between sessions so each session does
/// not allocate its own (a session with few steps costs about as much to allocate for as
/// to match).
#[derive(Default)]
struct Scratch {
    /// Per-step masks.
    masks: Vec<u64>,
    active: Vec<u32>,
    runs: Vec<(u32, u32)>,
    merged: Vec<u32>,
    keys: Vec<(u32, u32)>,
    by_code: Vec<u32>,
    seen: Vec<u64>,
    gx: Vec<u64>,
    at: Vec<u32>,
    fill: Vec<u32>,
    flat: Vec<(u32, u64)>,
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

/// Put `active`, made of ascending runs `runs` (start, end), in ascending order.
fn merge_runs(active: &mut Vec<u32>, runs: &mut Vec<(u32, u32)>, merged: &mut Vec<u32>) {
    runs.retain(|r| r.0 < r.1);
    if runs.len() <= 1 {
        return;
    }
    if runs.len() > 8 {
        active.sort_unstable();
        return;
    }
    merged.clear();
    merged.reserve(active.len());
    loop {
        // the run whose next step comes first
        let mut best = usize::MAX;
        let mut low = u32::MAX;
        for (i, r) in runs.iter().enumerate() {
            if r.0 < r.1 && (best == usize::MAX || active[r.0 as usize] < low) {
                best = i;
                low = active[r.0 as usize];
            }
        }
        if best == usize::MAX {
            break;
        }
        merged.push(low);
        runs[best].0 += 1;
    }
    std::mem::swap(active, merged);
}

fn session_matches_in(db: &Db, c: &Compiled, s: u32, ss: &steps::SessionSteps, now: i64, sc: &mut Scratch) -> Result<Option<Vec<Match>>, QError> {
    let Scratch { masks, active, runs, merged, keys, by_code, seen, gx, at, fill, flat } = sc;
    let masks: &mut [u64] = &mut masks[..ss.rows.len()];
    // masks by shape code: whole groups of steps at once where tool and outcome decide every
    // test, step by step elsewhere; `active` gets the steps passing some element, as one
    // ascending run per shape code
    active.clear();
    runs.clear();
    if c.eq.is_none() {
        // every step's mask, in order
        for (pos, m) in masks.iter_mut().enumerate() {
            if let Some(x) = c.shape_masks[ss.shape[pos] as usize] {
                *m = x;
                continue;
            }
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
    }
    let by = ss.by_shape();
    for code in 0..=255u8 {
        if c.eq.is_none() {
            break;
        }
        let ps = by.of(code);
        if ps.is_empty() {
            continue;
        }
        match c.shape_masks[code as usize] {
            Some(0) => {}
            Some(x) => {
                for &p in ps {
                    masks[p as usize] = x;
                }
                if x as u32 != 0 {
                    let a = active.len() as u32;
                    active.extend_from_slice(ps);
                    runs.push((a, active.len() as u32));
                }
            }
            None => {
                let a = active.len() as u32;
                for &pos in ps {
                    let mut m = 0u64;
                    for (k, t) in c.elems.iter().enumerate() {
                        if t.test(db, s, ss, pos, now)? {
                            m |= 1 << k;
                        }
                    }
                    for (x, t) in c.excl.iter().enumerate() {
                        if t.test(db, s, ss, pos, now)? {
                            m |= 1 << (32 + x);
                        }
                    }
                    masks[pos as usize] = m;
                    if m as u32 != 0 {
                        active.push(pos);
                    }
                }
                runs.push((a, active.len() as u32));
            }
        }
    }
    merge_runs(active, runs, merged);
    let masks: &[u64] = masks;
    let mut out = Vec::new();
    match &c.eq {
        None => matcher::run(&c.spec, ss, masks, &mut out),
        Some((field, ks)) => {
            let bits: u64 = ks.iter().fold(0, |a, &k| a | 1 << k);
            let fcode = db.step_dicts().fields.code(field.as_bytes());
            // (step, value id) of the steps a compared element passes, with each distinct
            // value (dictionary codes by array, long values by text) and the compared
            // elements it has a step for
            keys.clear();
            by_code.clear();
            by_code.resize(fcode.map_or(0, |f| ss.value_count(f)), u32::MAX);
            let mut longs: HashMap<Vec<u8>, u32> = HashMap::new();
            seen.clear();
            for &pos in active.iter() {
                let m = masks[pos as usize];
                if let Some(f) = fcode
                    && m & bits != 0
                    && let Some(&(_, v)) = ss.fields_of(pos).iter().find(|x| x.0 == f)
                {
                    let n = seen.len() as u32;
                    let i = if steps::is_long(v) {
                        plan::long_value(db, s, ss, pos, v, field)?.map(|raw| *longs.entry(raw).or_insert(n))
                    } else {
                        let slot = &mut by_code[v as usize];
                        if *slot == u32::MAX {
                            *slot = n;
                        }
                        Some(*slot)
                    };
                    if let Some(i) = i {
                        if i == n {
                            seen.push(0);
                        }
                        seen[i as usize] |= m & bits;
                        keys.push((pos, i));
                    }
                }
            }
            if !seen.contains(&bits) {
                // no value is seen by every compared element
            } else if bits == (1u64 << c.spec.n) - 1 {
                // every element is compared: only steps with the value can bind, so each
                // value visits just its own turns
                let turns = ss.turns();
                let nt = turns.depth.len();
                gx.clear();
                gx.resize(nt, 0);
                if !c.spec.excl.is_empty() {
                    for (pos, m) in masks.iter().enumerate() {
                        gx[turns.of[pos] as usize] |= m >> 32;
                    }
                }
                let mut xs: Vec<Vec<u32>> = vec![vec![0; nt]; c.spec.excl.len()];
                for (x, cnt) in xs.iter_mut().enumerate() {
                    for g in 0..nt {
                        let up = if turns.parent[g] == NONE { 0 } else { cnt[turns.parent[g] as usize] };
                        cnt[g] = up + (gx[g] >> x & 1) as u32;
                    }
                }
                // steps of each value seen by every compared element, in one array by value
                at.clear();
                at.resize(seen.len() + 1, 0);
                for &(_, v) in keys.iter() {
                    if seen[v as usize] == bits {
                        at[v as usize + 1] += 1;
                    }
                }
                for v in 0..seen.len() {
                    at[v + 1] += at[v];
                }
                fill.clear();
                fill.extend_from_slice(at);
                flat.clear();
                flat.resize(at[seen.len()] as usize, (0u32, 0u64));
                for &(pos, v) in keys.iter() {
                    if seen[v as usize] == bits {
                        flat[fill[v as usize] as usize] = (pos, masks[pos as usize]);
                        fill[v as usize] += 1;
                    }
                }
                for v in 0..seen.len() {
                    matcher::run_sparse(&c.spec, &turns, gx, &xs, &flat[at[v] as usize..at[v + 1] as usize], &mut out);
                }
            } else {
                let mut key_of: Vec<u32> = vec![u32::MAX; masks.len()];
                for &(pos, v) in keys.iter() {
                    key_of[pos as usize] = v;
                }
                let mut part = vec![0u64; masks.len()];
                for (v, &have) in seen.iter().enumerate() {
                    if have != bits {
                        continue; // some compared element never sees this value
                    }
                    let mut start = usize::MAX;
                    for (pos, (m, k)) in masks.iter().zip(&key_of).enumerate() {
                        part[pos] = if *k == v as u32 { *m } else { m & !bits };
                        if start == usize::MAX && part[pos] & 1 == 1 {
                            start = pos;
                        }
                    }
                    if start != usize::MAX {
                        matcher::run_from(&c.spec, ss, &part, start, &mut out);
                    }
                }
            }
            out.sort();
            out.dedup();
        }
    }
    Ok(Some(out))
}

fn parallel<T: Send>(n: usize, threads: usize, f: impl Fn(u32) -> Result<T, QError> + Sync) -> Result<Vec<(u32, T)>, QError> {
    if threads <= 1 {
        // on the caller's thread: no thread to start, and per-thread caches stay warm
        return (0..n as u32).map(|i| f(i).map(|v| (i, v))).collect();
    }
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
    plan::clear_cache();
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
    // `where ... | summarize`: from the segments' step summaries when they can answer it
    let mut from_summaries: Option<Table> = None;
    if let Some(Stage::Summarize { aggs, by }) = q.stages.get(i)
        && SUMMARIES.with(|c| c.get())
    {
        st.plan_ms = t0.elapsed().as_secs_f64() * 1e3;
        let t1 = Instant::now();
        if let Some((t, used)) = super::summary::try_run(db, &pre, aggs, by, &q.stages[i + 1..], now, &mut st)? {
            st.summarized = true;
            from_summaries = Some(t);
            i += 1 + used;
        }
        st.scan_ms = t1.elapsed().as_secs_f64() * 1e3;
    }
    let mut table = match (from_summaries, q.stages.get(i)) {
        (Some(t), _) => t,
        (None, Some(Stage::Match(p))) => {
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
        (None, _) => {
            let reads_values = pre.reads_values();
            let test = Test::new(db, pre, now);
            st.plan_ms = t0.elapsed().as_secs_f64() * 1e3;
            let t1 = Instant::now();
            let per = parallel(db.session_count(), threads, |s| {
                let ss_arc = if reads_values { db.try_steps(s) } else { db.light_steps(s) }.map_err(|e| QError::new(e.to_string()))?;
                let ss = &*ss_arc;
                if !test.possible_in(db, ss, s, now)? {
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
    if !st.summarized {
        st.steps_scanned = scanned.load(Ordering::Relaxed);
    }
    let t2 = Instant::now();
    let rest = &q.stages[i..];
    let mut k = 0;
    while k < rest.len() {
        // `sort ... | take n` keeps only n rows: select them instead of sorting everything
        if let (Stage::Sort(keys), Some(Stage::Take(n))) = (&rest[k], rest.get(k + 1)) {
            table = sort_rows(db, table, keys, now, Some(*n))?;
            k += 2;
            continue;
        }
        table = apply_stage(db, table, &rest[k], now)?;
        k += 1;
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
        Stage::Summarize { aggs, by } if fast_summarize_fits(&t, aggs, by) => fast_summarize(db, &t, aggs, by, now)?,
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
        Stage::Top { n, by, desc } => sort_rows(db, t, &[(by.clone(), *desc)], now, Some(*n))?,
        Stage::Sort(keys) => sort_rows(db, t, keys, now, None)?,
        Stage::Take(n) => take(t, *n),
        Stage::Count => Table { kind: Kind::Plain, cols: vec!["count".into()], rows: vec![Row { session: NONE, steps: [NONE; MAX_ELEMS], vals: vec![Value::Int(t.rows.len() as i64)] }] },
    })
}

/// A fast non-cryptographic hasher for small integer keys (FxHash's mix).
#[derive(Default, Clone, Copy)]
struct Fx(u64);

impl std::hash::Hasher for Fx {
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut b = [0u8; 8];
            b[..c.len()].copy_from_slice(c);
            self.write_u64(u64::from_le_bytes(b));
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

type FxMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<Fx>>;

#[derive(Clone, Copy, PartialEq)]
enum KeyPart {
    Tool,
    Sid,
    Agent,
    Failed,
    Ok,
    Field(u32),
    MissingField,
    BinTime(i64),
}

#[derive(Clone, Copy, PartialEq)]
enum NumCol {
    OutBytes,
    Time,
    Step,
}

fn key_part(db: &Db, e: &Expr) -> Option<KeyPart> {
    match e {
        Expr::Ref(p) => Some(match resolve(p).ok()? {
            StepCol::Tool => KeyPart::Tool,
            StepCol::Session => KeyPart::Sid,
            StepCol::Agent => KeyPart::Agent,
            StepCol::Failed => KeyPart::Failed,
            StepCol::Ok => KeyPart::Ok,
            StepCol::Field(f) => match db.step_dicts().fields.code(f.as_bytes()) {
                Some(c) => KeyPart::Field(c),
                None => KeyPart::MissingField,
            },
            _ => return None,
        }),
        Expr::Call(f, args) if (f == "bin" || f == "floor") && args.len() == 2 => match (&args[0], &args[1]) {
            (Expr::Ref(p), Expr::Lit(Value::Dur(d))) if *d > 0 && resolve(p).ok()? == StepCol::Time => Some(KeyPart::BinTime(*d)),
            _ => None,
        },
        _ => None,
    }
}

fn num_col(e: &Expr) -> Option<NumCol> {
    match e {
        Expr::Ref(p) => match resolve(p).ok()? {
            StepCol::OutBytes => Some(NumCol::OutBytes),
            StepCol::Time => Some(NumCol::Time),
            StepCol::Step => Some(NumCol::Step),
            _ => None,
        },
        _ => None,
    }
}

fn fast_summarize_fits(t: &Table, aggs: &[(String, AggFn, Option<Expr>)], by: &[(String, Expr)]) -> bool {
    if t.kind != Kind::Steps || !t.cols.is_empty() || by.len() > 4 {
        return false;
    }
    let simple_key = |e: &Expr| matches!(e, Expr::Ref(_)) || matches!(e, Expr::Call(f, a) if (f == "bin" || f == "floor") && a.len() == 2 && matches!(&a[1], Expr::Lit(Value::Dur(_))) && matches!(&a[0], Expr::Ref(p) if p.len() == 1 && p[0] == "time"));
    by.iter().all(|(_, e)| simple_key(e) && !matches!(e, Expr::Ref(p) if matches!(resolve(p), Ok(StepCol::OutBytes | StepCol::Time | StepCol::Step) | Err(_))))
        && aggs.iter().all(|(_, f, a)| match (f, a) {
            (AggFn::Count, _) | (AggFn::CountIf, Some(_)) => true,
            (AggFn::Sum | AggFn::Avg, Some(e)) => matches!(num_col(e), Some(NumCol::OutBytes | NumCol::Step)),
            (AggFn::Min | AggFn::Max, Some(e)) => num_col(e).is_some(),
            (AggFn::DCount, Some(e)) => matches!(e, Expr::Ref(p) if matches!(resolve(p), Ok(StepCol::Tool | StepCol::Session | StepCol::Agent | StepCol::Field(_)))),
            _ => false,
        })
}

/// `summarize` over step rows on dictionary codes, without evaluating each row.
/// Same groups, same order (first seen) and same arithmetic as the general path.
fn fast_summarize(db: &Db, t: &Table, aggs: &[(String, AggFn, Option<Expr>)], by: &[(String, Expr)], now: i64) -> Result<Table, QError> {
    const NULL: u64 = u64::MAX;
    const LONG_TAG: u64 = 1 << 62;
    let sd = db.step_dicts();
    let parts: Vec<KeyPart> = by.iter().map(|(_, e)| key_part(db, e).unwrap()).collect();
    // session or agent strings, and field values, interned for this query: value codes
    // are per session, so a value's group id comes from its text
    let mut sid_ids: HashMap<String, u64> = HashMap::new();
    let mut agent_ids: HashMap<String, u64> = HashMap::new();
    let mut sid_names: Vec<String> = Vec::new();
    let mut agent_names: Vec<String> = Vec::new();
    let mut val_ids: FxMap<(u32, Vec<u8>), u64> = FxMap::default();
    let mut val_names: Vec<Vec<u8>> = Vec::new();
    let mut longs: Vec<Vec<u8>> = Vec::new();
    let mut long_ids: HashMap<Vec<u8>, u64> = HashMap::new();
    // per session being read: its (session, agent) ids and its value codes -> ids
    let mut sess_vals: FxMap<(u32, u32), u64> = FxMap::default();
    let mut field_key = |s: u32, ss: &crate::steps::SessionSteps, pos: u32, f: u32, sess_vals: &mut FxMap<(u32, u32), u64>| -> Result<u64, QError> {
        Ok(match ss.fields_of(pos).iter().find(|x| x.0 == f) {
            None => NULL,
            Some(&(_, v)) if crate::steps::is_long(v) => {
                let name = String::from_utf8_lossy(sd.fields.get(f)).into_owned();
                match plan::long_value(db, s, ss, pos, v, &name)? {
                    None => NULL,
                    Some(raw) => match super::value::group_key(&raw) {
                        None => NULL,
                        Some(k) => {
                            let n = long_ids.len() as u64;
                            *long_ids.entry(k).or_insert_with(|| {
                                longs.push(raw);
                                LONG_TAG | n
                            })
                        }
                    },
                }
            }
            Some(&(_, v)) => *sess_vals.entry((f, v)).or_insert_with(|| {
                // values group by what they decode to (see `group_key`)
                let text = ss.value(f, v);
                match super::value::group_key(text) {
                    None => NULL,
                    Some(k) => {
                        let n = val_names.len() as u64;
                        *val_ids.entry((f, k)).or_insert_with(|| {
                            val_names.push(text.to_vec());
                            n
                        })
                    }
                }
            }),
        })
    };
    let tests: Vec<Option<Test>> = aggs
        .iter()
        .map(|(_, f, a)| match (f, a) {
            (AggFn::CountIf, Some(e)) => plan::compile(db, e, now).map(|sp| Some(Test::new(db, sp, now))),
            _ => Ok(None),
        })
        .collect::<Result<_, _>>()?;
    let dkey: Vec<Option<KeyPart>> = aggs.iter().map(|(_, f, a)| if *f == AggFn::DCount { a.as_ref().and_then(|e| key_part(db, e)) } else { None }).collect();
    let na = aggs.len();
    // Groups in first-seen order. One dictionary-coded key is looked up by its code in a
    // dense array; anything else (several keys, long or missing values) in a hash map.
    let dense_len = match parts.as_slice() {
        [KeyPart::Tool] => sd.tools.len(),
        [KeyPart::Sid] => sid_names.len(),
        [KeyPart::Agent] => agent_names.len(),
        [KeyPart::Failed] | [KeyPart::Ok] => 2,
        _ => 0,
    };
    let mut dense: Vec<u32> = vec![u32::MAX; dense_len];
    let mut map: FxMap<[u64; 4], u32> = FxMap::default();
    // accumulators, flat: [group * na + agg]
    struct Accs {
        na: usize,
        keys: Vec<[u64; 4]>,
        count: Vec<i64>,
        sum: Vec<f64>,
        lo: Vec<Option<i64>>,
        hi: Vec<Option<i64>>,
        distinct: Vec<std::collections::HashSet<u64>>,
    }
    impl Accs {
        fn group(&mut self, key: [u64; 4]) -> u32 {
            self.keys.push(key);
            let n = self.na;
            self.count.resize(self.count.len() + n, 0);
            self.sum.resize(self.sum.len() + n, 0.0);
            self.lo.resize(self.lo.len() + n, None);
            self.hi.resize(self.hi.len() + n, None);
            self.distinct.resize_with(self.distinct.len() + n, Default::default);
            self.keys.len() as u32 - 1
        }
    }
    let mut acc = Accs { na, keys: Vec::new(), count: Vec::new(), sum: Vec::new(), lo: Vec::new(), hi: Vec::new(), distinct: Vec::new() };
    let nums: Vec<Option<NumCol>> = aggs.iter().map(|(_, _, a)| a.as_ref().and_then(num_col)).collect();
    let mut cur_s = u32::MAX;
    let mut cur: Option<std::sync::Arc<crate::steps::SessionSteps>> = None;
    let (mut sid_cur, mut agent_cur) = (0u64, 0u64);
    for r in &t.rows {
        let (s, pos) = (r.session, r.steps[0]);
        if s != cur_s {
            cur_s = s;
            cur = Some(db.try_steps(s).map_err(|e| QError::new(e.to_string()))?);
            sess_vals.clear();
            if parts.iter().chain(dkey.iter().flatten()).any(|p| matches!(p, KeyPart::Sid | KeyPart::Agent)) {
                let (sid, agent) = db.session_key(s);
                let n = sid_ids.len() as u64;
                sid_cur = *sid_ids.entry(sid.clone()).or_insert_with(|| {
                    sid_names.push(sid);
                    n
                });
                let n = agent_ids.len() as u64;
                agent_cur = *agent_ids.entry(agent.clone()).or_insert_with(|| {
                    agent_names.push(agent);
                    n
                });
            }
        }
        let ss: &crate::steps::SessionSteps = cur.as_ref().unwrap();
        let row = ss.rows[pos as usize];
        let mut key = [0u64; 4];
        for (i, p) in parts.iter().enumerate() {
            key[i] = match *p {
                KeyPart::Tool => row.tool as u64,
                KeyPart::Sid => sid_cur,
                KeyPart::Agent => agent_cur,
                KeyPart::Failed => row.failed as u64,
                KeyPart::Ok => !row.failed as u64,
                KeyPart::Field(f) => field_key(s, ss, pos, f, &mut sess_vals)?,
                KeyPart::MissingField => NULL,
                KeyPart::BinTime(d) => row.time_ms.div_euclid(d) as u64,
            };
        }
        let gi = if (key[0] as usize) < dense_len && key[0] & LONG_TAG == 0 {
            let slot = &mut dense[key[0] as usize];
            if *slot == u32::MAX {
                *slot = acc.group(key);
            }
            *slot
        } else {
            match map.get(&key) {
                Some(&g) => g,
                None => {
                    let g = acc.group(key);
                    map.insert(key, g);
                    g
                }
            }
        } as usize;
        let base = gi * na;
        for (ai, (_, f, _)) in aggs.iter().enumerate() {
            let num = || -> i64 {
                match nums[ai] {
                    Some(NumCol::OutBytes) => row.out_bytes as i64,
                    Some(NumCol::Time) => row.time_ms,
                    _ => pos as i64,
                }
            };
            let at = base + ai;
            match f {
                AggFn::Count => acc.count[at] += 1,
                AggFn::CountIf => acc.count[at] += tests[ai].as_ref().unwrap().test(db, s, ss, pos, now)? as i64,
                AggFn::Sum | AggFn::Avg => {
                    acc.sum[at] += num() as f64;
                    acc.count[at] += 1;
                }
                AggFn::Min => {
                    let v = num();
                    acc.lo[at] = Some(acc.lo[at].map_or(v, |m| m.min(v)));
                }
                AggFn::Max => {
                    let v = num();
                    acc.hi[at] = Some(acc.hi[at].map_or(v, |m| m.max(v)));
                }
                AggFn::DCount => {
                    let k = match dkey[ai] {
                        Some(KeyPart::Tool) => row.tool as u64,
                        Some(KeyPart::Sid) => sid_cur,
                        Some(KeyPart::Agent) => agent_cur,
                        Some(KeyPart::Field(fid)) => field_key(s, ss, pos, fid, &mut sess_vals)?,
                        _ => NULL,
                    };
                    if k != NULL {
                        acc.distinct[at].insert(k);
                    }
                }
            }
        }
    }
    if by.is_empty() && acc.keys.is_empty() {
        acc.group([0; 4]);
    }
    let key_value = |p: KeyPart, k: u64| -> Value {
        if k == NULL {
            return Value::Null;
        }
        match p {
            KeyPart::Tool => Value::Str(String::from_utf8_lossy(sd.tools.get(k as u32)).into()),
            KeyPart::Sid => Value::Str(sid_names[k as usize].as_str().into()),
            KeyPart::Agent => Value::Str(agent_names[k as usize].as_str().into()),
            KeyPart::Failed | KeyPart::Ok => Value::Bool(k == 1),
            KeyPart::Field(f) => {
                if k & LONG_TAG != 0 {
                    Value::from_json(&longs[(k & !LONG_TAG) as usize])
                } else {
                    let _ = f;
                    Value::from_json(&val_names[k as usize])
                }
            }
            KeyPart::MissingField => Value::Null,
            KeyPart::BinTime(d) => Value::Time(k as i64 * d),
        }
    };
    let mut rows = Vec::with_capacity(acc.keys.len());
    for (gi, key) in acc.keys.iter().enumerate() {
        let mut vals: Vec<Value> = parts.iter().enumerate().map(|(i, p)| key_value(*p, key[i])).collect();
        for (ai, (_, f, _)) in aggs.iter().enumerate() {
            let at = gi * na + ai;
            let is_time = nums[ai] == Some(NumCol::Time);
            vals.push(match f {
                AggFn::Count | AggFn::CountIf => Value::Int(acc.count[at]),
                AggFn::DCount => Value::Int(acc.distinct[at].len() as i64),
                AggFn::Sum => Value::Int(acc.sum[at] as i64),
                AggFn::Avg => {
                    if acc.count[at] == 0 {
                        Value::Null
                    } else {
                        Value::Float(acc.sum[at] / acc.count[at] as f64)
                    }
                }
                AggFn::Min | AggFn::Max => match if *f == AggFn::Min { acc.lo[at] } else { acc.hi[at] } {
                    None => Value::Null,
                    Some(v) if is_time => Value::Time(v),
                    Some(v) => Value::Int(v),
                },
            });
        }
        rows.push(Row { session: NONE, steps: [NONE; MAX_ELEMS], vals });
    }
    let cols = by.iter().map(|(n, _)| n.clone()).chain(aggs.iter().map(|(n, _, _)| n.clone())).collect();
    Ok(Table { kind: Kind::Plain, cols, rows })
}

fn take(mut t: Table, n: usize) -> Table {
    t.rows.truncate(n);
    t
}

/// Sort rows (stable: ties keep their order); with `limit`, only the first `limit` rows are
/// kept, chosen by partial selection instead of a full sort.
fn sort_rows(db: &Db, mut t: Table, keys: &[(Expr, bool)], now: i64, limit: Option<usize>) -> Result<Table, QError> {
    // keys that are plain columns are compared in place; others are evaluated once per row
    let direct: Vec<Option<usize>> = keys
        .iter()
        .map(|(e, _)| match e {
            Expr::Ref(p) if p.len() == 1 => t.cols.iter().position(|c| *c == p[0]),
            _ => None,
        })
        .collect();
    let mut evald: Vec<Vec<Value>> = Vec::new();
    if direct.iter().any(|d| d.is_none()) {
        evald.reserve(t.rows.len());
        for r in &t.rows {
            evald.push(keys.iter().zip(&direct).map(|((e, _), d)| if d.is_some() { Ok(Value::Null) } else { eval_row(db, &t, r, e, now) }).collect::<Result<_, _>>()?);
        }
    }
    let rows = &t.rows;
    let cmp = |a: &usize, b: &usize| {
        for (j, (_, desc)) in keys.iter().enumerate() {
            let (x, y) = match direct[j] {
                Some(c) => (&rows[*a].vals[c], &rows[*b].vals[c]),
                None => (&evald[*a][j], &evald[*b][j]),
            };
            let o = x.order(y);
            let o = if *desc { o.reverse() } else { o };
            if o.is_ne() {
                return o;
            }
        }
        a.cmp(b)
    };
    let mut idx: Vec<usize> = (0..rows.len()).collect();
    match limit {
        Some(n) if n < idx.len() => {
            if n == 0 {
                idx.clear();
            } else {
                idx.select_nth_unstable_by(n - 1, cmp);
                idx.truncate(n);
                idx.sort_unstable_by(cmp);
            }
        }
        _ => idx.sort_unstable_by(cmp),
    }
    let mut slots: Vec<Option<Row>> = std::mem::take(&mut t.rows).into_iter().map(Some).collect();
    t.rows = idx.into_iter().map(|i| slots[i].take().unwrap()).collect();
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
