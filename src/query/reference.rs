//! A slow, simple evaluator for `match`, used to check the planner and the matcher.
//!
//! It reads every step's tool, input and outcome from the stored messages (no
//! dictionaries, no shape codes, no session skipping), lists every root-to-leaf path of
//! the session's turn tree, and on each path tries every way to bind the pattern, keeping
//! the semantics written in `matcher.rs`. Exponential in the worst case; meant for tests
//! and samples.

use super::exec::{self, Compiled};
use super::matcher::{Match, MAX_ELEMS};
use super::parse::{Expr, Link, Pattern, Query, Stage};
use super::plan;
use super::value::{eval, Ctx, Value};
use super::QError;
use crate::db::Db;
use crate::record::NONE;
use crate::steps::{self, SessionSteps};
use std::collections::HashMap;

struct RawStep {
    tool: String,
    fields: Vec<(String, Vec<u8>)>,
    failed: bool,
}

fn block(m: &[u8], k: u16) -> Option<&[u8]> {
    let ms = crate::json::members(m, &(0..m.len())).ok()?;
    let c = crate::json::find_member(m, &ms, "content")?;
    let (els, _) = crate::json::elements(m, &c.value).ok()?;
    els.get(k as usize).map(|e| &m[e.clone()])
}

fn raw_steps(db: &Db, s: u32) -> Result<Vec<RawStep>, QError> {
    let ss_arc = db.steps(s);
    let ss = &*ss_arc;
    let mut cache: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut out = Vec::with_capacity(ss.rows.len());
    for r in &ss.rows {
        for n in [r.call_node, r.result_node] {
            if let std::collections::hash_map::Entry::Vacant(e) = cache.entry(n) {
                e.insert(db.message(s, n).map_err(|e| QError::new(e.to_string()))?);
            }
        }
        let call = block(&cache[&r.call_node], r.call_block).ok_or_else(|| QError::new("reference: call block missing"))?;
        let res = block(&cache[&r.result_node], r.result_block).ok_or_else(|| QError::new("reference: result block missing"))?;
        // the call block is a one-block message for scan purposes
        let mut wrapped = Vec::from(&br#"{"content":["#[..]);
        wrapped.extend_from_slice(call);
        wrapped.extend_from_slice(b"]}");
        let (calls, _) = steps::scan_message(&wrapped);
        let c = calls.first().ok_or_else(|| QError::new("reference: not a tool call"))?;
        let rm = crate::json::members(res, &(0..res.len())).map_err(|e| QError::new(e.to_string()))?;
        let failed = crate::json::find_member(res, &rm, "is_error").is_some_and(|m| &res[m.value.clone()] == b"true");
        out.push(RawStep {
            tool: String::from_utf8_lossy(c.name).into_owned(),
            fields: c.fields.iter().map(|(k, v)| (String::from_utf8_lossy(k).into_owned(), v.to_vec())).collect(),
            failed,
        });
    }
    Ok(out)
}

struct RawCtx<'a> {
    db: &'a Db,
    s: u32,
    pos: u32,
    st: &'a RawStep,
    ss: &'a SessionSteps,
    now: i64,
}

impl Ctx for RawCtx<'_> {
    fn col(&self, path: &[String]) -> Result<Value, QError> {
        let r = self.ss.rows[self.pos as usize];
        Ok(match plan::resolve(path)? {
            plan::StepCol::Tool => Value::Str(self.st.tool.as_str().into()),
            plan::StepCol::Failed => Value::Bool(self.st.failed),
            plan::StepCol::Ok => Value::Bool(!self.st.failed),
            plan::StepCol::Time => Value::Time(r.time_ms),
            plan::StepCol::OutBytes => Value::Int(r.out_bytes as i64),
            plan::StepCol::Step => Value::Int(self.pos as i64),
            plan::StepCol::Session => Value::Str(self.db.session_key(self.s).0.as_str().into()),
            plan::StepCol::Agent => Value::Str(self.db.session_key(self.s).1.as_str().into()),
            plan::StepCol::Field(f) => match self.st.fields.iter().find(|(k, _)| *k == f) {
                Some((_, v)) => Value::from_json(v),
                None => Value::Null,
            },
        })
    }
    fn now(&self) -> i64 {
        self.now
    }
}

struct Turn {
    steps: Vec<u32>,
}

/// Paths of the turn tree: each a list of turn indices from a root to a leaf.
fn turns_and_paths(ss: &SessionSteps) -> (Vec<Turn>, Vec<Vec<usize>>) {
    let rows = &ss.rows;
    let mut turns: Vec<Turn> = Vec::new();
    let mut inst_of = vec![0usize; rows.len()];
    let mut parent: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let r = rows[i];
        let mut j = i + 1;
        while j < rows.len() && rows[j].prev == r.prev && rows[j].call_node == r.call_node && rows[j].result_node == r.result_node {
            j += 1;
        }
        let g = turns.len();
        for p in inst_of.iter_mut().take(j).skip(i) {
            *p = g;
        }
        parent.push(if r.prev == NONE { usize::MAX } else { inst_of[r.prev as usize] });
        turns.push(Turn { steps: (i as u32..j as u32).collect() });
        i = j;
    }
    let mut kids: Vec<Vec<usize>> = vec![Vec::new(); turns.len()];
    let mut roots = Vec::new();
    for (g, &p) in parent.iter().enumerate() {
        if p == usize::MAX { roots.push(g) } else { kids[p].push(g) }
    }
    let mut paths = Vec::new();
    let mut stack: Vec<(usize, Vec<usize>)> = roots.into_iter().map(|r| (r, vec![r])).collect();
    while let Some((g, path)) = stack.pop() {
        if kids[g].is_empty() {
            paths.push(path);
        } else {
            for &k in &kids[g] {
                let mut p = path.clone();
                p.push(k);
                stack.push((k, p));
            }
        }
    }
    (turns, paths)
}

struct Brute<'a> {
    p: &'a Pattern,
    /// [elem][step] passes
    pass: Vec<Vec<bool>>,
    /// [excl][step] passes
    xpass: Vec<Vec<bool>>,
}

impl Brute<'_> {
    fn max_of(&self, k: usize) -> u32 {
        self.p.elems[k].max.unwrap_or(u32::MAX)
    }

    /// All assignments: runs[k] = (start depth, end depth) and binds[k], 1-based depths on `path`.
    #[allow(clippy::too_many_arguments)]
    fn search(&self, turns: &[Turn], path: &[usize], last_end: usize, k: usize, end: usize, bind: u32, runs: &mut Vec<(usize, usize)>, binds: &mut [u32; MAX_ELEMS], out: &mut Vec<(Vec<(usize, usize)>, [u32; MAX_ELEMS])>) {
        let el = &self.p.elems[k];
        let is_last = k + 1 == self.p.elems.len();
        let lens: Vec<u32> = if is_last { vec![el.min] } else { (el.min..=self.max_of(k).min(end as u32)).collect() };
        for r in lens {
            let r = r as usize;
            if r > end || end + 1 - r <= last_end {
                continue;
            }
            let start = end + 1 - r;
            // every turn of the run needs a passing step (the end turn's is `bind`)
            if !(start..end).all(|d| turns[path[d - 1]].steps.iter().any(|&s| self.pass[k][s as usize])) {
                continue;
            }
            runs[k] = (start, end);
            binds[k] = bind;
            if k == 0 {
                out.push((runs.clone(), *binds));
                continue;
            }
            let prev_ends: Vec<usize> = if self.p.links[k] == Link::Next { vec![start - 1] } else { (last_end + 1..start).collect() };
            for pe in prev_ends {
                if pe <= last_end || pe == 0 {
                    continue;
                }
                // bind of elem k-1 is the latest passing step in its end turn
                let Some(&b) = turns[path[pe - 1]].steps.iter().rev().find(|&&s| self.pass[k - 1][s as usize]) else { continue };
                self.search(turns, path, last_end, k - 1, pe, b, runs, binds, out);
            }
        }
    }

    fn path_matches(&self, turns: &[Turn], path: &[usize], out: &mut Vec<Match>) {
        let n = self.p.elems.len();
        let last = n - 1;
        let mut last_end = 0usize;
        for d in 1..=path.len() {
            let g = &turns[path[d - 1]];
            let mut best: Option<(usize, [u32; MAX_ELEMS])> = None;
            for &c in &g.steps {
                if !self.pass[last][c as usize] {
                    continue;
                }
                let mut runs = vec![(0, 0); n];
                let mut binds = [NONE; MAX_ELEMS];
                let mut found = Vec::new();
                self.search(turns, path, last_end, last, d, c, &mut runs, &mut binds, &mut found);
                for (runs, b) in found {
                    let start = runs[0].0;
                    if self.p.within.is_some_and(|w| d - start > w as usize) {
                        continue;
                    }
                    if !self.excl_ok(turns, path, &runs) {
                        continue;
                    }
                    if best.is_none_or(|x| (start, b) > x) {
                        best = Some((start, b));
                    }
                }
            }
            if let Some((_, b)) = best {
                out.push(Match { end: b[last], binds: b });
                last_end = d;
            }
        }
    }

    /// No turn strictly between the end of run i and the start of run j holds an excluded step.
    fn excl_ok(&self, turns: &[Turn], path: &[usize], runs: &[(usize, usize)]) -> bool {
        for (x, (_, i, j)) in self.p.excl.iter().enumerate() {
            for dd in runs[*i].1 + 1..runs[*j].0 {
                if turns[path[dd - 1]].steps.iter().any(|&s| self.xpass[x][s as usize]) {
                    return false;
                }
            }
        }
        true
    }
}

/// Matches by brute force for one session.
pub fn session_matches(db: &Db, p: &Pattern, pre: Option<&Expr>, s: u32, now: i64) -> Result<Vec<Match>, QError> {
    let ss_arc = db.steps(s);
    let ss = &*ss_arc;
    if ss.rows.is_empty() {
        return Ok(Vec::new());
    }
    let raw = raw_steps(db, s)?;
    let test = |e: &Expr, pos: usize| -> Result<bool, QError> {
        let ctx = RawCtx { db, s, pos: pos as u32, st: &raw[pos], ss, now };
        if let Some(pre) = pre
            && !eval(pre, &ctx)?.truthy() {
                return Ok(false);
            }
        Ok(eval(e, &ctx)?.truthy())
    };
    let mut pass = Vec::new();
    for el in &p.elems {
        pass.push((0..raw.len()).map(|i| test(&el.pred, i)).collect::<Result<Vec<_>, _>>()?);
    }
    let mut xpass = Vec::new();
    for (x, _, _) in &p.excl {
        xpass.push((0..raw.len()).map(|i| test(x, i)).collect::<Result<Vec<_>, _>>()?);
    }
    let (turns, paths) = turns_and_paths(ss);
    let mut out = Vec::new();
    let run_paths = |pass: Vec<Vec<bool>>, out: &mut Vec<Match>| {
        let b = Brute { p, pass, xpass: xpass.clone() };
        for path in &paths {
            b.path_matches(&turns, path, out);
        }
    };
    match &p.eq {
        None => run_paths(pass, &mut out),
        Some((path, ks)) => {
            let plan::StepCol::Field(f) = plan::resolve(path)? else { return Err(QError::new("reference: eq on a non-field")) };
            let key = |i: usize| raw[i].fields.iter().find(|(k, _)| *k == f).map(|(_, v)| v.clone());
            let mut distinct: Vec<Vec<u8>> = Vec::new();
            for i in 0..raw.len() {
                if ks.iter().any(|&k| pass[k][i])
                    && let Some(v) = key(i)
                        && !distinct.contains(&v) {
                            distinct.push(v);
                        }
            }
            for v in distinct {
                let mut part = pass.clone();
                for &k in ks {
                    for (i, b) in part[k].iter_mut().enumerate() {
                        *b = *b && key(i).as_deref() == Some(&v[..]);
                    }
                }
                run_paths(part, &mut out);
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Engine against reference for one query, over all sessions. Returns (engine matches,
/// sessions that differ).
pub fn compare(db: &Db, src: &str, max_steps: usize) -> Result<(usize, Vec<u32>), QError> {
    let q: Query = super::parse::parse(src)?;
    let now = 1_800_000_000_000;
    plan::clear_cache();
    let mut i = 0;
    let mut pre_exprs = Vec::new();
    while let Some(Stage::Where(e)) = q.stages.get(i) {
        pre_exprs.push(e.clone());
        i += 1;
    }
    let Some(Stage::Match(p)) = q.stages.get(i) else { return Err(QError::new("compare needs a match stage")) };
    let pre_expr = match pre_exprs.len() {
        0 => None,
        1 => Some(pre_exprs[0].clone()),
        _ => Some(Expr::And(pre_exprs.clone())),
    };
    let pre = match &pre_expr {
        None => plan::Sp::Const(true),
        Some(e) => plan::compile(db, e, now)?,
    };
    let c: Compiled = exec::compile_pattern(db, p, &pre, now)?;
    let scanned = std::sync::atomic::AtomicUsize::new(0);
    let mut total = 0;
    let mut bad = Vec::new();
    for s in 0..db.session_count() as u32 {
        if db.steps(s).rows.len() > max_steps {
            continue;
        }
        let eng = exec::session_matches(db, &c, s, now, &scanned)?.unwrap_or_default();
        let refm = session_matches(db, p, pre_expr.as_ref(), s, now)?;
        total += eng.len();
        if eng != refm {
            bad.push(s);
        }
    }
    Ok((total, bad))
}

/// A random `match` query over the store's own tools, for checking.
pub fn random_query(db: &Db, r: &mut crate::synth::Rng) -> String {
    let sd = db.step_dicts();
    let mut counts = vec![0usize; sd.tools.len()];
    for s in 0..db.session_count() as u32 {
        for r in &db.steps(s).rows {
            counts[r.tool as usize] += 1;
        }
    }
    let mut tools: Vec<(usize, u32)> = counts.into_iter().enumerate().map(|(t, n)| (n, t as u32)).collect();
    tools.sort_by(|a, b| b.cmp(a));
    let names: Vec<String> = tools.iter().take(6).map(|(_, t)| String::from_utf8_lossy(sd.tools.get(*t)).into_owned()).collect();
    if names.is_empty() {
        return "match e0: failed".into();
    }
    let tool = |r: &mut crate::synth::Rng| format!("{:?}", r.pick(&names));
    const SUBS: &[&str] = &["1", "2", "3", "test", "src", ".rs", ".md", "/"];
    let atom = |r: &mut crate::synth::Rng| -> String {
        match r.below(9) {
            0 | 1 => format!("tool == {}", tool(r)),
            2 => format!("tool in ({}, {})", tool(r), tool(r)),
            3 => "failed".into(),
            4 => "not failed".into(),
            5 => format!("file_path contains {:?}", r.pick(SUBS)),
            6 => format!("out_bytes > {}", r.pick(&[500, 2000, 5000])),
            7 => format!("command has {:?}", r.pick(&["test", "cargo", "git"])),
            _ => format!("tool != {}", tool(r)),
        }
    };
    let pred = |r: &mut crate::synth::Rng| -> String {
        match r.below(20) {
            0..=11 => atom(r),
            12..=16 => format!("{} and {}", atom(r), atom(r)),
            17 | 18 => format!("({} or {})", atom(r), atom(r)),
            _ => format!("not ({})", atom(r)),
        }
    };
    let n = 1 + r.below(3) as usize;
    let mut q = String::new();
    if r.chance(0.15) {
        q.push_str(if r.chance(0.5) { "where not failed | " } else { "where out_bytes > 100 | " });
    }
    q.push_str("match ");
    for k in 0..n {
        if k > 0 {
            q.push_str(if r.chance(0.3) { " next " } else { " then " });
        }
        q.push_str(&format!("e{k}: {}", pred(r)));
        match r.below(40) {
            0..=3 => q.push_str("{2}"),
            4..=6 => q.push_str("{1,2}"),
            7..=9 => q.push_str("{2,}"),
            _ => {}
        }
    }
    if r.chance(0.25) {
        q.push_str(&format!(" within {}", 2 + r.below(9)));
    }
    let mut cons = Vec::new();
    if n >= 2 && r.chance(0.3) {
        let i = r.below(n as u64 - 1) as usize;
        let j = i + 1 + r.below((n - i - 1) as u64) as usize;
        cons.push(format!("no ({}) between e{i} and e{j}", atom(r)));
    }
    if n >= 2 && r.chance(0.15) {
        cons.push(format!("e0.file_path == e{}.file_path", n - 1));
    }
    if !cons.is_empty() {
        q.push_str(" where ");
        q.push_str(&cons.join(" and "));
    }
    q
}

// ---------------------------------------------------------------------------- aggregations

/// Every session's steps as the reference reads them from the stored messages.
pub struct RawStore {
    sessions: Vec<(u32, Vec<RawStep>)>,
}

/// Read every step of every session from the stored messages (once, for many queries).
pub fn raw_store(db: &Db) -> Result<RawStore, QError> {
    let mut sessions = Vec::with_capacity(db.session_count());
    for s in 0..db.session_count() as u32 {
        sessions.push((s, raw_steps(db, s)?));
    }
    Ok(RawStore { sessions })
}

/// `where ... | summarize ... by ... [| sort ... [| take n]]` by brute force over steps read
/// from the messages: each `where` and each key and aggregate evaluated on every step, groups
/// by the keys' values. Returns the rows as printed cells; without a `sort`, rows come
/// ordered by their keys.
pub fn summarize(db: &Db, raw: &RawStore, src: &str, now: i64) -> Result<Vec<Vec<String>>, QError> {
    use super::value::Key;
    let q: Query = super::parse::parse(src)?;
    let mut i = 0;
    let mut pre = Vec::new();
    while let Some(Stage::Where(e)) = q.stages.get(i) {
        pre.push(e.clone());
        i += 1;
    }
    let Some(Stage::Summarize { aggs, by }) = q.stages.get(i) else { return Err(QError::new("reference: needs where stages then summarize")) };
    struct Acc {
        key: Vec<Value>,
        count: Vec<i64>,
        sum: Vec<f64>,
        all_int: Vec<bool>,
        best: Vec<Option<Value>>,
        set: Vec<std::collections::HashSet<Key>>,
    }
    let na = aggs.len();
    let mut groups: HashMap<Vec<Key>, Acc> = HashMap::new();
    for (s, steps) in &raw.sessions {
        let ss_arc = db.steps(*s);
        let ss = &*ss_arc;
        for (pos, st) in steps.iter().enumerate() {
            let ctx = RawCtx { db, s: *s, pos: pos as u32, st, ss, now };
            let mut pass = true;
            for e in &pre {
                if !eval(e, &ctx)?.truthy() {
                    pass = false;
                    break;
                }
            }
            if !pass {
                continue;
            }
            let kv = by.iter().map(|(_, e)| eval(e, &ctx)).collect::<Result<Vec<_>, _>>()?;
            let kk: Vec<Key> = kv.iter().map(|v| v.key()).collect();
            let acc = groups.entry(kk).or_insert_with(|| Acc { key: kv, count: vec![0; na], sum: vec![0.0; na], all_int: vec![true; na], best: vec![None; na], set: vec![Default::default(); na] });
            for (ai, (_, f, arg)) in aggs.iter().enumerate() {
                let v = match arg {
                    Some(e) => eval(e, &ctx)?,
                    None => Value::Null,
                };
                match f {
                    super::parse::AggFn::Count => acc.count[ai] += 1,
                    super::parse::AggFn::CountIf => acc.count[ai] += v.truthy() as i64,
                    super::parse::AggFn::Sum | super::parse::AggFn::Avg => match v {
                        Value::Int(x) => {
                            acc.sum[ai] += x as f64;
                            acc.count[ai] += 1;
                        }
                        Value::Float(x) => {
                            acc.sum[ai] += x;
                            acc.count[ai] += 1;
                            acc.all_int[ai] = false;
                        }
                        _ => {}
                    },
                    super::parse::AggFn::Min | super::parse::AggFn::Max => {
                        if !matches!(v, Value::Null) {
                            let better = match &acc.best[ai] {
                                None => true,
                                Some(b) => {
                                    let o = v.order(b);
                                    if *f == super::parse::AggFn::Min { o.is_lt() } else { o.is_gt() }
                                }
                            };
                            if better {
                                acc.best[ai] = Some(v);
                            }
                        }
                    }
                    super::parse::AggFn::DCount => {
                        if !matches!(v, Value::Null) {
                            acc.set[ai].insert(v.key());
                        }
                    }
                }
            }
        }
    }
    if by.is_empty() && groups.is_empty() {
        groups.insert(Vec::new(), Acc { key: Vec::new(), count: vec![0; na], sum: vec![0.0; na], all_int: vec![true; na], best: vec![None; na], set: vec![Default::default(); na] });
    }
    let cols: Vec<String> = by.iter().map(|(n, _)| n.clone()).chain(aggs.iter().map(|(n, _, _)| n.clone())).collect();
    let mut rows: Vec<Vec<Value>> = groups
        .into_values()
        .map(|a| {
            let mut vals = a.key.clone();
            for (ai, (_, f, _)) in aggs.iter().enumerate() {
                vals.push(match f {
                    super::parse::AggFn::Count | super::parse::AggFn::CountIf => Value::Int(a.count[ai]),
                    super::parse::AggFn::DCount => Value::Int(a.set[ai].len() as i64),
                    super::parse::AggFn::Sum if a.all_int[ai] => Value::Int(a.sum[ai] as i64),
                    super::parse::AggFn::Sum => Value::Float(a.sum[ai]),
                    super::parse::AggFn::Avg if a.count[ai] == 0 => Value::Null,
                    super::parse::AggFn::Avg => Value::Float(a.sum[ai] / a.count[ai] as f64),
                    super::parse::AggFn::Min | super::parse::AggFn::Max => a.best[ai].clone().unwrap_or(Value::Null),
                });
            }
            vals
        })
        .collect();
    // rows by their keys first (a canonical order), then the query's own order
    let nk = by.len();
    rows.sort_by(|a, b| {
        for k in 0..nk {
            let o = a[k].order(&b[k]);
            if o.is_ne() {
                return o;
            }
        }
        std::cmp::Ordering::Equal
    });
    let col = |e: &Expr| match e {
        Expr::Ref(p) if p.len() == 1 => cols.iter().position(|c| *c == p[0]),
        _ => None,
    };
    let mut take = None;
    for st in &q.stages[i + 1..] {
        match st {
            Stage::Sort(keys) => {
                let ks: Vec<(usize, bool)> = keys.iter().map(|(e, d)| col(e).map(|c| (c, *d))).collect::<Option<_>>().ok_or_else(|| QError::new("reference: sort on a column of the summary only"))?;
                rows.sort_by(|a, b| {
                    for &(c, desc) in &ks {
                        let o = a[c].order(&b[c]);
                        let o = if desc { o.reverse() } else { o };
                        if o.is_ne() {
                            return o;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            Stage::Top { n, by: e, desc } => {
                let c = col(e).ok_or_else(|| QError::new("reference: top on a column of the summary only"))?;
                rows.sort_by(|a, b| {
                    let o = a[c].order(&b[c]);
                    if *desc { o.reverse() } else { o }
                });
                take = Some(*n);
            }
            Stage::Take(n) => take = Some(take.map_or(*n, |t: usize| t.min(*n))),
            _ => return Err(QError::new("reference: only sort, top and take after summarize")),
        }
    }
    if let Some(n) = take {
        rows.truncate(n);
    }
    Ok(rows.into_iter().map(|r| r.into_iter().map(|v| v.to_string()).collect()).collect())
}

/// One aggregation query three ways: from the summaries, by scanning steps, and by brute
/// force. Returns whether the summaries answered it and whether all three agree.
pub fn compare_summarize(db: &Db, raw: &RawStore, src: &str) -> Result<(bool, bool), QError> {
    let run = |on: bool| -> Result<(Vec<Vec<String>>, bool), QError> {
        super::exec::set_summaries(on);
        let r = super::run(db, src, 1);
        super::exec::set_summaries(true);
        let r = r?;
        Ok((super::exec::render(db, &r.table, usize::MAX)?.1, r.stats.summarized))
    };
    let (fast, used) = run(true)?;
    let (scan, _) = run(false)?;
    let brute = summarize(db, raw, src, super::exec::now_ms())?;
    Ok((used, fast == scan && scan == brute))
}

/// A random `where ... | summarize ...` query over the store's own tools, mixing tests the
/// summaries can answer (tool, outcome, session, one input field) with ones they cannot.
pub fn random_summarize(db: &Db, r: &mut crate::synth::Rng) -> String {
    let sd = db.step_dicts();
    let mut counts = vec![0usize; sd.tools.len()];
    for s in 0..db.session_count() as u32 {
        for row in &db.steps(s).rows {
            counts[row.tool as usize] += 1;
        }
    }
    let mut tools: Vec<(usize, u32)> = counts.into_iter().enumerate().map(|(t, n)| (n, t as u32)).collect();
    tools.sort_by(|a, b| b.cmp(a));
    let names: Vec<String> = tools.iter().take(6).map(|(_, t)| String::from_utf8_lossy(sd.tools.get(*t)).into_owned()).collect();
    let names = if names.is_empty() { vec!["Bash".to_string()] } else { names };
    let sid = if db.session_count() > 0 { db.session_key(r.below(db.session_count() as u64) as u32).0 } else { String::new() };
    let sid_prefix: String = sid.chars().take(3).collect();
    let tool = |r: &mut crate::synth::Rng| format!("{:?}", r.pick(&names));
    let atom = |r: &mut crate::synth::Rng| -> String {
        match r.below(34) {
            0 | 1 => format!("tool == {}", tool(r)),
            2 => format!("tool in ({}, {})", tool(r), tool(r)),
            3 => format!("tool != {}", tool(r)),
            4 => r#"tool startswith "re""#.into(),
            5 => "failed".into(),
            6 => "not failed".into(),
            7 => r#"agent == """#.into(),
            8 => r#"agent != """#.into(),
            9 => format!("session startswith {sid_prefix:?}"),
            10 => format!("file_path contains {:?}", r.pick(&["src", "test", ".rs", "/repo", "ünï", "C:", "src/1"])),
            11 => format!("command contains {:?}", r.pick(&["git commit", "git", "commit", "cargo test", "\"hi\"", "/ slash", "kelvin", "istanbul", "café", "upper", "nline two", "ttab"])),
            12 => format!("command has {:?}", r.pick(&["git", "commit", "status"])),
            13 => format!("command contains_cs {:?}", r.pick(&["Git", "GIT", "git commit"])),
            14 => format!("command startswith {:?}", r.pick(&["git", "echo", "cat"])),
            15 => format!("command endswith {:?}", r.pick(&["upper", "status", "EOF"])),
            16 => r#"command == "git status""#.into(),
            17 => r#"command in ("ls", "git status", "Git Commit -m Upper")"#.into(),
            18 => format!("file_path == {:?}", format!("/repo/src/{}.rs", r.below(12))),
            19 => "isnull(file_path)".into(),
            20 => "isnotnull(command)".into(),
            21 => "isnull(command)".into(),
            22 => r#"content contains "fn main""#.into(),
            23 => r#"not (file_path contains "src")"#.into(),
            24 => format!("out_bytes > {}", r.pick(&[100, 300])),
            25 => "time > ago(100000d)".into(),
            26 => "strlen(command) > 12".into(),
            27 => r#"pattern contains "fn""#.into(),
            28 => r#"description contains "the""#.into(),
            29 => r#"not (command contains "commit")"#.into(),
            30 => r#"file_path not in ("/repo/src/1.rs", "/repo/src/2.rs")"#.into(),
            31 => r#"command matches "git*""#.into(),
            32 => format!("step < {}", r.pick(&[5, 40])),
            _ => "ok".into(),
        }
    };
    let pred = |r: &mut crate::synth::Rng| -> String {
        match r.below(10) {
            0..=4 => atom(r),
            5..=7 => format!("{} and {}", atom(r), atom(r)),
            8 => format!("({} or {})", atom(r), atom(r)),
            _ => format!("not ({})", atom(r)),
        }
    };
    let mut q = String::new();
    for _ in 0..r.below(3) {
        q.push_str(&format!("where {} | ", pred(r)));
    }
    const KEYS: &[&str] = &["tool", "failed", "ok", "session", "agent", "bin(time, 1d)", "bin(time, 7d)", "file_path", "command", "bin(time, 1h)", "content"];
    let nk = r.below(3) as usize;
    let mut keys: Vec<&str> = Vec::new();
    while keys.len() < nk {
        let k = *r.pick(KEYS);
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
    const AGGS: &[&str] = &["n = count()", "f = countif(failed)", "b = countif(tool == \"Bash\")", "c = countif(file_path contains \"src\")", "s = sum(out_bytes)", "a = avg(out_bytes)", "lo = min(time)", "hi = max(out_bytes)", "ds = dcount(session)", "df = dcount(file_path)", "dt = dcount(tool)", "da = dcount(agent)", "dc = dcount(command)"];
    let na = 1 + r.below(3) as usize;
    let mut aggs: Vec<&str> = Vec::new();
    while aggs.len() < na {
        let a = *r.pick(AGGS);
        if !aggs.contains(&a) {
            aggs.push(a);
        }
    }
    q.push_str(&format!("summarize {}", aggs.join(", ")));
    if !keys.is_empty() {
        let named: Vec<String> = keys.iter().enumerate().map(|(i, k)| format!("k{i} = {k}")).collect();
        q.push_str(&format!(" by {}", named.join(", ")));
        let dir = |r: &mut crate::synth::Rng| if r.chance(0.5) { "asc" } else { "desc" };
        let all: Vec<String> = (0..keys.len()).map(|i| format!("k{i} {}", dir(r))).collect();
        match r.below(4) {
            0 if keys.len() == 1 => q.push_str(&format!(" | top {} by k0 {}", 1 + r.below(5), dir(r))),
            1 => {
                let first = aggs[0].split(' ').next().unwrap();
                q.push_str(&format!(" | sort by {first} {}, {} | take {}", dir(r), all.join(", "), 1 + r.below(6)));
            }
            _ => q.push_str(&format!(" | sort by {}", all.join(", "))),
        }
    }
    q
}

/// A random two-element `match` whose elements must share a field's value, often with an
/// exclusion between them, a window, `next` or repeats: the cases where the matcher visits
/// only the turns holding each value.
pub fn random_eq_query(db: &Db, r: &mut crate::synth::Rng) -> String {
    let sd = db.step_dicts();
    let mut counts = vec![0usize; sd.tools.len()];
    for s in 0..db.session_count() as u32 {
        for row in &db.steps(s).rows {
            counts[row.tool as usize] += 1;
        }
    }
    let mut tools: Vec<(usize, u32)> = counts.into_iter().enumerate().map(|(t, n)| (n, t as u32)).collect();
    tools.sort_by(|a, b| b.cmp(a));
    let names: Vec<String> = tools.iter().take(6).map(|(_, t)| String::from_utf8_lossy(sd.tools.get(*t)).into_owned()).collect();
    let names = if names.is_empty() { vec!["Read".to_string()] } else { names };
    let tool = |r: &mut crate::synth::Rng| format!("{:?}", r.pick(&names));
    let atom = |r: &mut crate::synth::Rng| -> String {
        match r.below(7) {
            0 | 1 => format!("tool == {}", tool(r)),
            2 => format!("tool in ({}, {})", tool(r), tool(r)),
            3 => "failed".into(),
            4 => "not failed".into(),
            5 => format!("tool != {}", tool(r)),
            _ => format!("out_bytes > {}", r.pick(&[200, 1000])),
        }
    };
    let pred = |r: &mut crate::synth::Rng| if r.chance(0.7) { atom(r) } else { format!("{} and {}", atom(r), atom(r)) };
    let rep = match r.below(8) {
        0 => "{2}",
        1 => "{1,2}",
        _ => "",
    };
    let link = if r.chance(0.25) { "next" } else { "then" };
    let mut q = format!("match a: {}{rep} {link} b: {}", pred(r), pred(r));
    if r.chance(0.4) {
        q.push_str(&format!(" within {}", 1 + r.below(6)));
    }
    let mut cons = Vec::new();
    if r.chance(0.6) {
        cons.push(format!("no ({}) between a and b", atom(r)));
    }
    let f = if r.chance(0.8) { "file_path" } else { "command" };
    cons.push(format!("a.{f} == b.{f}"));
    q.push_str(&format!(" where {}", cons.join(" and ")));
    q
}
