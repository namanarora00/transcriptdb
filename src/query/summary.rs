//! `where ... | summarize ... by ...` answered from the segments' step summaries
//! (`crate::stats`) instead of from every step.
//!
//! Used when every part of the query can be read off the summaries:
//! * the `where` stages test tool, outcome, session and agent, and at most one input field
//!   (each test of it worked out once per distinct value in a segment, and only for the
//!   values the word index allows when a test of the field is a `contains`-like literal);
//! * `by` keys are tool, outcome, session, agent, that field, and `bin(time, N)` with N a
//!   whole number of days;
//! * aggregates are `count`, `countif` (with the same kinds of tests), `sum` and `avg` of
//!   `out_bytes`, `min` and `max` of `out_bytes` and `time` (without a field), and
//!   `dcount` of tool, session, agent or the field;
//! * the stages after it do not depend on the order `summarize` returns its rows in (a
//!   `sort` or `top` on every key, or `count`), since that order differs from a scan's.
//!
//! Steps not covered by summaries (not flushed yet, or in segments written before
//! summaries existed) are read from the sessions as usual, so the answer is always whole.
//! The answer is the one the step scan gives, row for row; `tdb check-queries` checks that.

use super::exec::{Kind, QStats, Row, Table};
use super::matcher::MAX_ELEMS;
use super::parse::{AggFn, CmpOp, Expr, Stage, StrOp};
use super::plan::{self, resolve, FieldArg, Sp, StepCol, Test};
use super::value::Value;
use super::QError;
use crate::db::{Db, StatBlock, StepSource};
use crate::record::NONE;
use crate::stats::{self, FieldDir, WordMatch, DAY_MS};
use crate::steps;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const NULL: u64 = u64::MAX;
/// Field group ids of values given by their text (long values, steps read from sessions)
/// when value ids of the one segment are used as group ids.
const TEXT_TAG: u64 = 1 << 62;

fn qe(e: crate::codec::Error) -> QError {
    QError::new(e.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum KeyK {
    Tool,
    Sid,
    Agent,
    Failed,
    Ok,
    Field,
    Null,
    /// `bin(time, d)`, `d` a whole number of days.
    Bin(i64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggK {
    Count,
    CountIf(usize),
    Sum,
    Avg,
    MinOut,
    MaxOut,
    MinTime,
    MaxTime,
    DTool,
    DSid,
    DAgent,
    DField,
    /// `dcount` of a field the store has never seen: always 0.
    DNone,
}

/// A predicate over (tool, outcome, session tests, field tests).
#[derive(Debug, Clone)]
enum P {
    Const(bool),
    Tool(Arc<[bool]>),
    Shape(usize),
    Sess(usize),
    Fld(usize),
    And(Vec<P>),
    Or(Vec<P>),
    Not(Box<P>),
}

struct Plan {
    keys: Vec<KeyK>,
    aggs: Vec<AggK>,
    pre: P,
    countifs: Vec<P>,
    shapes: Vec<Vec<[bool; 2]>>,
    satoms: Vec<Sp>,
    fatoms: Vec<Sp>,
    /// The one input field the query reads (the store's code) and its name.
    field: Option<(u32, String)>,
    /// Per `where` conjunct the word index can serve: alternatives, each a list of words.
    index: Vec<Vec<Vec<(Vec<u8>, WordMatch)>>>,
    needs_sid: bool,
    needs_agent: bool,
    /// One key and one `count()`.
    count_only: bool,
    /// Whether the field's values group rows or are counted.
    needs_fval: bool,
}

/// Words that a value passing this atom must hold (alternatives), when the index can say.
fn index_spec(sp: &Sp) -> Option<Vec<Vec<(Vec<u8>, WordMatch)>>> {
    let Sp::Field { expr, .. } = sp else { return None };
    // case-sensitive tests are only served for ASCII patterns: case folding is per
    // character there, so an exact occurrence is also one in the folded text
    let words = |p: &str, cs: bool| -> Option<Vec<(Vec<u8>, WordMatch)>> {
        if cs && !p.is_ascii() {
            return None;
        }
        let w = stats::pattern_words(&stats::lower(p));
        if w.is_empty() { None } else { Some(w) }
    };
    let is_ref = |e: &Expr| matches!(e, Expr::Ref(_));
    let lit = |e: &Expr| match e {
        Expr::Lit(Value::Str(s)) => Some(s.to_string()),
        _ => None,
    };
    match &**expr {
        Expr::Str { op, e, pat, neg: false, cs } if *op != StrOp::Matches && is_ref(e) => Some(vec![words(&lit(pat)?, *cs)?]),
        Expr::Cmp(CmpOp::Eq, a, b) if is_ref(a) => Some(vec![words(&lit(b)?, true)?]),
        Expr::Cmp(CmpOp::Eq, a, b) if is_ref(b) => Some(vec![words(&lit(a)?, true)?]),
        Expr::In { e, list, neg: false } if is_ref(e) => list.iter().map(|x| words(&lit(x)?, true)).collect(),
        _ => None,
    }
}

fn conjuncts<'a>(sp: &'a Sp, out: &mut Vec<&'a Sp>) {
    match sp {
        Sp::And(v) => v.iter().for_each(|x| conjuncts(x, out)),
        x => out.push(x),
    }
}

impl Plan {
    fn new(db: &Db, pre: &Sp, aggs: &[(String, AggFn, Option<Expr>)], by: &[(String, Expr)], now: i64) -> Result<Option<Plan>, QError> {
        let mut pl = Plan { keys: Vec::new(), aggs: Vec::new(), pre: P::Const(true), countifs: Vec::new(), shapes: Vec::new(), satoms: Vec::new(), fatoms: Vec::new(), field: None, index: Vec::new(), needs_sid: false, needs_agent: false, count_only: false, needs_fval: false };
        if by.len() > 4 {
            return Ok(None);
        }
        let sd = db.step_dicts();
        let mut fields: Vec<Option<u32>> = Vec::new();
        let field_of = |name: &str| sd.fields.code(name.as_bytes());
        for (_, e) in by {
            let k = match e {
                Expr::Ref(p) => match resolve(p) {
                    Ok(StepCol::Tool) => KeyK::Tool,
                    Ok(StepCol::Session) => KeyK::Sid,
                    Ok(StepCol::Agent) => KeyK::Agent,
                    Ok(StepCol::Failed) => KeyK::Failed,
                    Ok(StepCol::Ok) => KeyK::Ok,
                    Ok(StepCol::Field(f)) => match field_of(&f) {
                        Some(c) => {
                            fields.push(Some(c));
                            pl.field = Some((c, f));
                            KeyK::Field
                        }
                        None => KeyK::Null,
                    },
                    _ => return Ok(None),
                },
                Expr::Call(f, a) if (f == "bin" || f == "floor") && a.len() == 2 => match (&a[0], &a[1]) {
                    (Expr::Ref(p), Expr::Lit(Value::Dur(d))) if *d > 0 && d % DAY_MS == 0 && resolve(p).ok() == Some(StepCol::Time) => KeyK::Bin(*d),
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            };
            pl.keys.push(k);
        }
        for (_, f, a) in aggs {
            let col = |a: &Option<Expr>| match a {
                Some(Expr::Ref(p)) => resolve(p).ok(),
                _ => None,
            };
            let k = match (f, col(a)) {
                (AggFn::Count, _) => AggK::Count,
                (AggFn::CountIf, _) => {
                    let Some(e) = a else { return Ok(None) };
                    let sp = plan::compile(db, e, now)?;
                    let Some(p) = pl.tr(db, &sp, now, &mut fields) else { return Ok(None) };
                    pl.countifs.push(p);
                    AggK::CountIf(pl.countifs.len() - 1)
                }
                (AggFn::Sum, Some(StepCol::OutBytes)) => AggK::Sum,
                (AggFn::Avg, Some(StepCol::OutBytes)) => AggK::Avg,
                (AggFn::Min, Some(StepCol::OutBytes)) => AggK::MinOut,
                (AggFn::Max, Some(StepCol::OutBytes)) => AggK::MaxOut,
                (AggFn::Min, Some(StepCol::Time)) => AggK::MinTime,
                (AggFn::Max, Some(StepCol::Time)) => AggK::MaxTime,
                (AggFn::DCount, Some(StepCol::Tool)) => AggK::DTool,
                (AggFn::DCount, Some(StepCol::Session)) => AggK::DSid,
                (AggFn::DCount, Some(StepCol::Agent)) => AggK::DAgent,
                (AggFn::DCount, Some(StepCol::Field(f))) => match field_of(&f) {
                    Some(c) => {
                        fields.push(Some(c));
                        pl.field = Some((c, f));
                        AggK::DField
                    }
                    None => AggK::DNone,
                },
                _ => return Ok(None),
            };
            pl.aggs.push(k);
        }
        let Some(p) = pl.tr(db, pre, now, &mut fields) else { return Ok(None) };
        pl.pre = p;
        // one input field at most: the field tables hold one field each
        let mut fs: Vec<u32> = fields.into_iter().flatten().collect();
        fs.sort_unstable();
        fs.dedup();
        if fs.len() > 1 || pl.satoms.len() > 64 || pl.fatoms.len() > 64 {
            return Ok(None);
        }
        if let Some(&c) = fs.first()
            && pl.field.is_none()
        {
            pl.field = Some((c, String::from_utf8_lossy(sd.fields.get(c)).into_owned()));
        }
        if pl.field.is_some() && pl.aggs.iter().any(|a| matches!(a, AggK::MinOut | AggK::MaxOut | AggK::MinTime | AggK::MaxTime)) {
            return Ok(None);
        }
        pl.count_only = pl.keys.len() == 1 && pl.aggs == [AggK::Count];
        pl.needs_fval = pl.keys.contains(&KeyK::Field) || pl.aggs.contains(&AggK::DField);
        pl.needs_sid = pl.keys.contains(&KeyK::Sid) || pl.aggs.contains(&AggK::DSid);
        pl.needs_agent = pl.keys.contains(&KeyK::Agent) || pl.aggs.contains(&AggK::DAgent);
        let mut cs = Vec::new();
        conjuncts(pre, &mut cs);
        for c in cs {
            if let Some(spec) = index_spec(c) {
                pl.index.push(spec);
            }
        }
        Ok(Some(pl))
    }

    /// The predicate as tool, outcome, session and field tests; `None` if it reads anything else.
    fn tr(&mut self, db: &Db, sp: &Sp, now: i64, fields: &mut Vec<Option<u32>>) -> Option<P> {
        Some(match sp {
            Sp::Const(b) => P::Const(*b),
            Sp::Tool(t) => P::Tool(t.clone()),
            Sp::Generic(e) => {
                self.shapes.push(plan::shape_atom(db, e, now)?);
                P::Shape(self.shapes.len() - 1)
            }
            Sp::Session { .. } => {
                self.satoms.push(sp.clone());
                P::Sess(self.satoms.len() - 1)
            }
            Sp::Field { f: None, missing, .. } => P::Const(*missing),
            Sp::Present { f: None, want } => P::Const(!*want),
            Sp::Field { f: Some(c), .. } | Sp::Present { f: Some(c), .. } => {
                fields.push(Some(*c));
                self.fatoms.push(sp.clone());
                P::Fld(self.fatoms.len() - 1)
            }
            Sp::And(v) => P::And(v.iter().map(|x| self.tr(db, x, now, fields)).collect::<Option<_>>()?),
            Sp::Or(v) => P::Or(v.iter().map(|x| self.tr(db, x, now, fields)).collect::<Option<_>>()?),
            Sp::Not(x) => P::Not(Box::new(self.tr(db, x, now, fields)?)),
        })
    }

    #[inline]
    fn ev(&self, p: &P, tool: u32, failed: bool, sb: u64, fb: u64) -> bool {
        match p {
            P::Const(b) => *b,
            P::Tool(t) => t.get(tool as usize).copied().unwrap_or(false),
            P::Shape(i) => self.shapes[*i].get(tool as usize).is_some_and(|x| x[failed as usize]),
            P::Sess(i) => sb >> i & 1 == 1,
            P::Fld(i) => fb >> i & 1 == 1,
            P::And(v) => v.iter().all(|x| self.ev(x, tool, failed, sb, fb)),
            P::Or(v) => v.iter().any(|x| self.ev(x, tool, failed, sb, fb)),
            P::Not(x) => !self.ev(x, tool, failed, sb, fb),
        }
    }

    /// Like [`Plan::ev`] with the session tests unknown and field tests given: `None` when
    /// the answer depends on the session.
    fn ev3(&self, p: &P, tool: u32, failed: bool, fb: u64) -> Option<bool> {
        match p {
            P::Sess(_) => None,
            P::And(v) => {
                let mut any_none = false;
                for x in v {
                    match self.ev3(x, tool, failed, fb) {
                        Some(false) => return Some(false),
                        None => any_none = true,
                        Some(true) => {}
                    }
                }
                if any_none { None } else { Some(true) }
            }
            P::Or(v) => {
                let mut any_none = false;
                for x in v {
                    match self.ev3(x, tool, failed, fb) {
                        Some(true) => return Some(true),
                        None => any_none = true,
                        Some(false) => {}
                    }
                }
                if any_none { None } else { Some(false) }
            }
            P::Not(x) => self.ev3(x, tool, failed, fb).map(|b| !b),
            x => Some(self.ev(x, tool, failed, 0, fb)),
        }
    }

    /// `p` with its tool and outcome tests answered for one tool and outcome.
    fn simplify(&self, p: &P, tool: u32, failed: bool) -> P {
        match p {
            P::Const(_) | P::Sess(_) | P::Fld(_) => p.clone(),
            P::Tool(_) | P::Shape(_) => P::Const(self.ev(p, tool, failed, 0, 0)),
            P::And(v) | P::Or(v) => {
                let and = matches!(p, P::And(_));
                let mut out = Vec::new();
                for x in v {
                    match self.simplify(x, tool, failed) {
                        P::Const(b) if b != and => return P::Const(b),
                        P::Const(_) => {}
                        y => out.push(y),
                    }
                }
                match out.len() {
                    0 => P::Const(and),
                    1 => out.pop().unwrap(),
                    _ if and => P::And(out),
                    _ => P::Or(out),
                }
            }
            P::Not(x) => match self.simplify(x, tool, failed) {
                P::Const(b) => P::Const(!b),
                y => P::Not(Box::new(y)),
            },
        }
    }

    /// The predicate's answer from field tests alone (`None` if it also depends on tool,
    /// outcome or session).
    fn ev3_field_only(&self, p: &P, fb: u64) -> Option<bool> {
        match p {
            P::Const(b) => Some(*b),
            P::Fld(i) => Some(fb >> i & 1 == 1),
            P::Tool(_) | P::Shape(_) | P::Sess(_) => None,
            P::And(v) => {
                let mut any_none = false;
                for x in v {
                    match self.ev3_field_only(x, fb) {
                        Some(false) => return Some(false),
                        None => any_none = true,
                        Some(true) => {}
                    }
                }
                if any_none { None } else { Some(true) }
            }
            P::Or(v) => {
                let mut any_none = false;
                for x in v {
                    match self.ev3_field_only(x, fb) {
                        Some(true) => return Some(true),
                        None => any_none = true,
                        Some(false) => {}
                    }
                }
                if any_none { None } else { Some(false) }
            }
            P::Not(x) => self.ev3_field_only(x, fb).map(|b| !b),
        }
    }

    fn field_bits(&self, arg: FieldArg, now: i64) -> Result<u64, QError> {
        let mut b = 0u64;
        for (i, a) in self.fatoms.iter().enumerate() {
            if plan::field_atom(a, arg, now)? {
                b |= 1 << i;
            }
        }
        Ok(b)
    }

    fn sess_bits(&self, sid: &str, agent: &str, now: i64) -> Result<u64, QError> {
        let mut b = 0u64;
        for (i, a) in self.satoms.iter().enumerate() {
            if plan::session_atom(a, sid, agent, now)? {
                b |= 1 << i;
            }
        }
        Ok(b)
    }

    fn countif_bits(&self, tool: u32, failed: bool, sb: u64, fb: u64) -> u64 {
        let mut b = 0u64;
        for (i, p) in self.countifs.iter().enumerate() {
            if self.ev(p, tool, failed, sb, fb) {
                b |= 1 << i;
            }
        }
        b
    }
}

/// Whether the stages after `summarize` give the same answer whatever order its rows come in.
fn order_free(by: &[(String, Expr)], rest: &[Stage]) -> bool {
    if by.is_empty() {
        return true;
    }
    let mut keys: HashSet<String> = by.iter().map(|(n, _)| n.clone()).collect();
    if keys.len() != by.len() {
        return false;
    }
    let need = keys.len();
    for st in rest {
        match st {
            Stage::Where(_) => {}
            Stage::Extend(v) => {
                for (n, _) in v {
                    keys.remove(n);
                }
            }
            Stage::Project(v) => {
                keys = v.iter().filter(|(_, e)| matches!(e, Expr::Ref(p) if p.len() == 1 && keys.contains(&p[0]))).map(|(n, _)| n.clone()).collect();
            }
            Stage::Sort(ks) => {
                let named: HashSet<&String> = ks.iter().filter_map(|(e, _)| match e {
                    Expr::Ref(p) if p.len() == 1 => Some(&p[0]),
                    _ => None,
                }).collect();
                return keys.len() == need && keys.iter().all(|k| named.contains(k));
            }
            Stage::Top { by: Expr::Ref(p), .. } => return need == 1 && keys.len() == 1 && p.len() == 1 && keys.contains(&p[0]),
            Stage::Count => return true,
            _ => return false,
        }
    }
    false
}

/// What one summary row, long step or step adds.
#[derive(Clone, Copy)]
struct W {
    count: u32,
    sum: u64,
    min_out: u32,
    max_out: u32,
    min_t: i64,
    max_t: i64,
}

/// Accumulators per group, flat (`[group * na + aggregate]`). With one key that is a small
/// dense id (tool, outcome, a value id of the one segment), groups `0..dense` are those ids
/// and are laid out up front; other groups follow, found by key.
struct Accs {
    na: usize,
    /// Whether any aggregate needs sums, min/max, or distinct sets.
    sums: bool,
    minmax: bool,
    sets: bool,
    dense: usize,
    used: Vec<bool>,
    extra: Vec<[u64; 4]>,
    count: Vec<i64>,
    sum: Vec<u64>,
    lo: Vec<Option<i64>>,
    hi: Vec<Option<i64>>,
    distinct: Vec<HashSet<u64>>,
    map: HashMap<[u64; 4], u32>,
}

impl Accs {
    fn new(na: usize, sums: bool, minmax: bool, sets: bool, dense: usize) -> Accs {
        let mut a = Accs { na, sums, minmax, sets, dense, used: vec![false; dense], extra: Vec::new(), count: vec![0; dense * na], sum: if sums { vec![0; dense * na] } else { Vec::new() }, lo: Vec::new(), hi: Vec::new(), distinct: Vec::new(), map: HashMap::new() };
        if minmax {
            a.lo = vec![None; dense * na];
            a.hi = vec![None; dense * na];
        }
        if sets {
            a.distinct.resize_with(dense * na, Default::default);
        }
        a
    }
    #[inline]
    fn group(&mut self, key: [u64; 4]) -> usize {
        if (key[0] as usize) < self.dense {
            self.used[key[0] as usize] = true;
            return key[0] as usize;
        }
        if let Some(&g) = self.map.get(&key) {
            return g as usize;
        }
        let g = self.new_group(key);
        self.map.insert(key, g as u32);
        g
    }
    fn new_group(&mut self, key: [u64; 4]) -> usize {
        self.extra.push(key);
        let n = self.na;
        self.count.resize(self.count.len() + n, 0);
        if self.sums {
            self.sum.resize(self.sum.len() + n, 0);
        }
        if self.minmax {
            self.lo.resize(self.lo.len() + n, None);
            self.hi.resize(self.hi.len() + n, None);
        }
        if self.sets {
            self.distinct.resize_with(self.distinct.len() + n, Default::default);
        }
        self.dense + self.extra.len() - 1
    }
    fn key(&self, g: usize) -> [u64; 4] {
        if g < self.dense { [g as u64, 0, 0, 0] } else { self.extra[g - self.dense] }
    }
    /// Groups that got a step, dense ones first.
    fn groups(&self) -> Vec<usize> {
        (0..self.dense).filter(|&g| self.used[g]).chain(self.dense..self.dense + self.extra.len()).collect()
    }
}

/// Global ids for session ids, agents and field values met in several sources.
#[derive(Default)]
struct Names {
    ix: HashMap<String, u64>,
    names: Vec<String>,
}

impl Names {
    fn id(&mut self, s: &str) -> u64 {
        if let Some(&i) = self.ix.get(s) {
            return i;
        }
        self.names.push(s.to_string());
        self.ix.insert(s.to_string(), self.names.len() as u64 - 1);
        self.names.len() as u64 - 1
    }
}

/// Field value groups met as text: by group key, with the first value's text.
#[derive(Default)]
struct Texts {
    ix: HashMap<Vec<u8>, u64>,
    texts: Vec<Vec<u8>>,
}

impl Texts {
    fn id(&mut self, key: Vec<u8>, t: &[u8]) -> u64 {
        if let Some(&i) = self.ix.get(&key) {
            return i;
        }
        self.texts.push(t.to_vec());
        self.ix.insert(key, self.texts.len() as u64 - 1);
        self.texts.len() as u64 - 1
    }
}

struct Exec<'a> {
    db: &'a Db,
    pl: &'a Plan,
    aggs: &'a [(String, AggFn, Option<Expr>)],
    now: i64,
    acc: Accs,
    sids: Names,
    agents: Names,
    texts: Texts,
    /// When the field's values come from one segment only: its number, and value ids are group ids.
    single: Option<u32>,
    single_dir: Option<FieldDir>,
    test: Option<Test>,
    countif_tests: Vec<Test>,
    rows_read: usize,
}

impl Exec<'_> {
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn add(&mut self, tool: u32, failed: bool, bin_t: i64, sid: u64, agent: u64, fval: u64, cif: u64, w: W) {
        let pl = self.pl;
        // the common case: counts by one dense key
        if pl.count_only && (fval as usize) < self.acc.dense && pl.keys[0] == KeyK::Field {
            let g = fval as usize;
            self.acc.used[g] = true;
            self.acc.count[g] += w.count as i64;
            return;
        }
        let mut key = [0u64; 4];
        for (i, k) in pl.keys.iter().enumerate() {
            key[i] = match *k {
                KeyK::Tool => tool as u64,
                KeyK::Sid => sid,
                KeyK::Agent => agent,
                KeyK::Failed => failed as u64,
                KeyK::Ok => !failed as u64,
                KeyK::Field => fval,
                KeyK::Null => NULL,
                KeyK::Bin(d) => bin_t.div_euclid(d) as u64,
            };
        }
        let g = self.acc.group(key);
        let base = g * self.acc.na;
        for (ai, a) in pl.aggs.iter().enumerate() {
            let at = base + ai;
            match *a {
                AggK::Count => self.acc.count[at] += w.count as i64,
                AggK::CountIf(k) => {
                    if cif >> k & 1 == 1 {
                        self.acc.count[at] += w.count as i64
                    }
                }
                AggK::Sum | AggK::Avg => {
                    self.acc.sum[at] += w.sum;
                    self.acc.count[at] += w.count as i64;
                }
                AggK::MinOut => self.acc.lo[at] = Some(self.acc.lo[at].map_or(w.min_out as i64, |m| m.min(w.min_out as i64))),
                AggK::MaxOut => self.acc.hi[at] = Some(self.acc.hi[at].map_or(w.max_out as i64, |m| m.max(w.max_out as i64))),
                AggK::MinTime => self.acc.lo[at] = Some(self.acc.lo[at].map_or(w.min_t, |m| m.min(w.min_t))),
                AggK::MaxTime => self.acc.hi[at] = Some(self.acc.hi[at].map_or(w.max_t, |m| m.max(w.max_t))),
                AggK::DTool => {
                    self.acc.distinct[at].insert(tool as u64);
                }
                AggK::DSid => {
                    self.acc.distinct[at].insert(sid);
                }
                AggK::DAgent => {
                    self.acc.distinct[at].insert(agent);
                }
                AggK::DField => {
                    if fval != NULL {
                        self.acc.distinct[at].insert(fval);
                    }
                }
                AggK::DNone => {}
            }
        }
    }

    /// Group id of a value given by its text (`NULL` for `null`).
    fn text_id(&mut self, t: &[u8]) -> u64 {
        let Some(k) = super::value::group_key(t) else { return NULL };
        let i = self.texts.id(k, t);
        if self.single.is_some() { TEXT_TAG | i } else { i }
    }

    fn segment(&mut self, num: u32, live: Option<&Arc<Vec<bool>>>, pre: &Sp) -> Result<(), QError> {
        let db = self.db;
        let pl = self.pl;
        let head_b = db.stats_head(num).map_err(qe)?.ok_or_else(|| QError::new("internal: segment without summaries"))?;
        let head = head_b.head();
        let (tools, fields) = db.seg_codes(num).ok_or_else(|| QError::new("internal: segment codes missing"))?;
        let ntools = db.step_dicts().tools.len() as u32;
        let tool_of = |t: u32| tools.get(t as usize).copied().unwrap_or(ntools);
        // per segment tool code and outcome (of the tools the rows read here use): what is left
        // of the predicate, and whether it can pass at all (0 never, 1 always, 2 depends on
        // session or field)
        let residuals = |need: &[bool]| -> (Vec<[P; 2]>, Vec<[u8; 2]>) {
            let resid: Vec<[P; 2]> = (0..tools.len()).map(|t| {
                if !need[t] {
                    return [P::Const(false), P::Const(false)];
                }
                let st = tool_of(t as u32);
                [pl.simplify(&pl.pre, st, false), pl.simplify(&pl.pre, st, true)]
            }).collect();
            let state = resid.iter().map(|r| [&r[0], &r[1]].map(|p| match p {
                P::Const(false) => 0,
                P::Const(true) => 1,
                _ => 2,
            })).collect();
            (resid, state)
        };
        let sid_g: Vec<u64> = if pl.needs_sid { head.sids.iter().map(|s| self.sids.id(s)).collect() } else { Vec::new() };
        let agent_g: Vec<u64> = if pl.needs_agent { head.agents.iter().map(|s| self.agents.id(s)).collect() } else { Vec::new() };
        let mut sbits: Vec<u64> = Vec::with_capacity(head.parts.len());
        if !pl.satoms.is_empty() {
            let mut memo: HashMap<(u32, u32), u64> = HashMap::new();
            for p in &head.parts {
                let b = match memo.get(&(p.sid, p.agent)) {
                    Some(&b) => b,
                    None => {
                        let b = pl.sess_bits(&head.sids[p.sid as usize], &head.agents[p.agent as usize], self.now)?;
                        memo.insert((p.sid, p.agent), b);
                        b
                    }
                };
                sbits.push(b);
            }
        }
        let mut part_ok: Vec<bool> = (0..head.parts.len()).map(|p| live.is_none_or(|m| m.get(p).copied().unwrap_or(false))).collect();
        let by_part = pl.needs_sid || pl.needs_agent || !pl.satoms.is_empty();
        let pinfo = |p: u32| {
            if !by_part {
                return (0, 0, 0);
            }
            let pi = head.parts[p as usize];
            (
                if pl.needs_sid { sid_g[pi.sid as usize] } else { 0 },
                if pl.needs_agent { agent_g[pi.agent as usize] } else { 0 },
                if pl.satoms.is_empty() { 0 } else { sbits[p as usize] },
            )
        };
        let seg_field = pl.field.as_ref().and_then(|(f, _)| fields.iter().position(|x| x == f)).map(|x| x as u32);
        let fd = seg_field.and_then(|f| head.field(f));
        let missing = pl.field_bits(FieldArg::Missing, self.now)?;
        let mut need = vec![false; tools.len()];
        let Some(fd) = fd else {
            // no field, or no step here has it: the part table says everything
            for r in &head.rows {
                if let Some(x) = need.get_mut(r.tool as usize) {
                    *x = true;
                }
            }
            let (resid, state) = residuals(&need);
            for r in &head.rows {
                let s = state.get(r.tool as usize).map_or(0, |x| x[r.failed as usize]);
                if s == 0 || !part_ok[r.part as usize] {
                    continue;
                }
                let (sid, agent, sb) = pinfo(r.part);
                let tool = tool_of(r.tool);
                if s == 2 && !pl.ev(&resid[r.tool as usize][r.failed as usize], tool, r.failed, sb, missing) {
                    continue;
                }
                let cif = pl.countif_bits(tool, r.failed, sb, missing);
                self.add(tool, r.failed, r.day * DAY_MS, sid, agent, NULL, cif, W { count: r.count, sum: r.sum_out, min_out: r.min_out, max_out: r.max_out, min_t: r.min_time, max_t: r.max_time });
            }
            return Ok(());
        };
        let rows_b = db.stats_rows(num, head, fd).map_err(qe)?;
        let rows = rows_b.rows();
        let nv = fd.n_values;
        let nl = fd.n_longs;
        // parts with a step giving the field twice are read from their steps
        for &p in &rows.inexact {
            let pi = head.parts[p as usize];
            if part_ok[p as usize] && pi.n_rows > 0 {
                self.rows(pi.session, pi.first_row, pi.first_row + pi.n_rows, pre)?;
            }
            part_ok[p as usize] = false;
        }
        let live_p = |p: u32| part_ok[p as usize];
        // steps without the field count only if the predicate can pass for them
        let needs_missing = match pl.ev3_field_only(&pl.pre, missing) {
            Some(b) => b,
            None => (0..tools.len() as u32).any(|t| [false, true].iter().any(|&f| pl.ev3(&pl.pre, tool_of(t), f, missing) != Some(false))),
        };
        for t in 0..rows.tool_start.len().saturating_sub(1) {
            if rows.tool_start[t + 1] > rows.tool_start[t]
                && let Some(x) = need.get_mut(t)
            {
                *x = true;
            }
        }
        for l in &rows.longs {
            if let Some(x) = need.get_mut(l.tool as usize) {
                *x = true;
            }
        }
        if needs_missing {
            for r in &head.rows {
                if let Some(x) = need.get_mut(r.tool as usize) {
                    *x = true;
                }
            }
        }
        let (resid, state) = residuals(&need);
        let group_of = |v: u32| match rows.group[v as usize] {
            u32::MAX => NULL,
            g => g as u64,
        };
        // which values and long steps can pass, from the word index
        let cands: Option<Vec<u32>> = if pl.index.is_empty() {
            None
        } else {
            let (wb, pb) = db.stats_words(num, fd).map_err(qe)?;
            let (words, post) = (wb.words(), pb.post());
            let nb = (nv + nl) as usize;
            let mut acc: Vec<u64> = vec![u64::MAX; nb.div_ceil(64)];
            let mut ids = Vec::new();
            for conj in &pl.index {
                let mut cset = vec![0u64; acc.len()];
                for alt in conj {
                    let mut aset = vec![u64::MAX; acc.len()];
                    for (w, how) in alt {
                        let mut wset = vec![0u64; acc.len()];
                        for wi in words.find(w, *how) {
                            ids.clear();
                            let (a, b) = (words.post_off[wi] as usize, words.post_off[wi + 1] as usize);
                            stats::postings(&post[a..b], &mut ids).map_err(qe)?;
                            for &id in &ids {
                                if (id as usize) < nb {
                                    wset[id as usize / 64] |= 1 << (id % 64);
                                }
                            }
                        }
                        for (x, y) in aset.iter_mut().zip(&wset) {
                            *x &= y;
                        }
                    }
                    for (x, y) in cset.iter_mut().zip(&aset) {
                        *x |= y;
                    }
                }
                for (x, y) in acc.iter_mut().zip(&cset) {
                    *x &= y;
                }
            }
            for &id in &rows.unindexed {
                if (id as usize) < nb {
                    acc[id as usize / 64] |= 1 << (id % 64);
                }
            }
            let mut out = Vec::new();
            for (wi, &w) in acc.iter().enumerate() {
                let mut w = w;
                while w != 0 {
                    let b = w.trailing_zeros();
                    let id = wi as u32 * 64 + b;
                    if (id as usize) < nb {
                        out.push(id);
                    }
                    w &= w - 1;
                }
            }
            Some(out)
        };
        // short values: each value's rows, its tests worked out once (and its text read) only
        // if a row can pass on tool and outcome
        let visit = |this: &mut Self, v: u32, chunk: &mut Option<Arc<StatBlock>>| -> Result<(), QError> {
            let mut fb: Option<u64> = None;
            let mut fval = None;
            let mut cb: Option<Arc<StatBlock>> = None;
            for r in &rows.rows[rows.start[v as usize] as usize..rows.start[v as usize + 1] as usize] {
                let s = state.get(r.tool as usize).map_or(0, |x| x[r.failed as usize]);
                if s == 0 || !part_ok[r.part as usize] {
                    continue;
                }
                if cb.is_none() {
                    if chunk.as_ref().is_none_or(|c| {
                        let c = c.vals();
                        v < c.first || v >= c.first + c.len() as u32
                    }) {
                        *chunk = Some(db.stats_values(num, fd, v).map_err(qe)?);
                    }
                    cb = chunk.clone();
                }
                let text = cb.as_ref().unwrap().vals().get(v);
                let fbv = match fb {
                    Some(b) => b,
                    None => {
                        let b = pl.field_bits(FieldArg::Short(text), this.now)?;
                        fb = Some(b);
                        b
                    }
                };
                let (sid, agent, sb) = pinfo(r.part);
                let tool = tool_of(r.tool);
                if s == 2 && !pl.ev(&resid[r.tool as usize][r.failed as usize], tool, r.failed, sb, fbv) {
                    continue;
                }
                let fv = match fval {
                    Some(x) => x,
                    None => {
                        let x = if this.single == Some(num) { group_of(v) } else { this.text_id(text) };
                        fval = Some(x);
                        x
                    }
                };
                let cif = pl.countif_bits(tool, r.failed, sb, fbv);
                this.add(tool, r.failed, r.day * DAY_MS, sid, agent, fv, cif, W { count: r.count, sum: r.sum_out, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
            }
            Ok(())
        };
        let mut chunk: Option<Arc<StatBlock>> = None;
        let mut longs_to_visit: Vec<u32> = Vec::new();
        match &cands {
            Some(ids) => {
                for &id in ids {
                    if id < nv {
                        visit(self, id, &mut chunk)?;
                    } else {
                        longs_to_visit.push(id - nv);
                    }
                }
            }
            None => {
                // every value: the rows of each tool that can pass, in tool order, each
                // value's tests worked out the first time one of its rows can pass
                let mut known: Vec<bool> = vec![false; nv as usize];
                let mut bits: Vec<u64> = if pl.fatoms.is_empty() { Vec::new() } else { vec![0; nv as usize] };
                let mut blocks: Vec<Option<Arc<StatBlock>>> = vec![None; fd.chunks.len()];
                let single = self.single == Some(num);
                let present_only = !pl.fatoms.is_empty() && pl.fatoms.iter().all(|a| matches!(a, Sp::Present { .. }));
                for t in 0..rows.tool_start.len().saturating_sub(1) {
                    let Some(st) = state.get(t) else { continue };
                    if *st == [0, 0] {
                        continue;
                    }
                    let tool = tool_of(t as u32);
                    for &ri in &rows.by_tool[rows.tool_start[t] as usize..rows.tool_start[t + 1] as usize] {
                        let r = &rows.rows[ri as usize];
                        let s = st[r.failed as usize];
                        if s == 0 || !part_ok[r.part as usize] {
                            continue;
                        }
                        let v = rows.value[ri as usize];
                        // the value's text: for its tests (the first time) and, when values
                        // come from several sources, for its group
                        let mut text: &[u8] = &[];
                        let first = !known[v as usize];
                        if (first && !pl.fatoms.is_empty() && !present_only) || !single {
                            let ci = fd.chunks.partition_point(|c| c.0 <= v) - 1;
                            if blocks[ci].is_none() {
                                blocks[ci] = Some(db.stats_values(num, fd, v).map_err(qe)?);
                            }
                            text = blocks[ci].as_ref().unwrap().vals().get(v);
                        }
                        if first {
                            if present_only {
                                // `isnull` / `isnotnull` only: the one `null` value is known
                                let null = rows.null_value == Some(v);
                                bits[v as usize] = pl.field_bits(FieldArg::Short(if null { b"null" } else { b"\"\"" }), self.now)?;
                            } else if !pl.fatoms.is_empty() {
                                bits[v as usize] = pl.field_bits(FieldArg::Short(text), self.now)?;
                            }
                            known[v as usize] = true;
                        }
                        let fb = if pl.fatoms.is_empty() { 0 } else { bits[v as usize] };
                        let (sid, agent, sb) = pinfo(r.part);
                        if s == 2 && !pl.ev(&resid[t][r.failed as usize], tool, r.failed, sb, fb) {
                            continue;
                        }
                        let fv = if single { group_of(v) } else { self.text_id(text) };
                        let cif = pl.countif_bits(tool, r.failed, sb, fb);
                        self.add(tool, r.failed, r.day * DAY_MS, sid, agent, fv, cif, W { count: r.count, sum: r.sum_out, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
                    }
                }
                longs_to_visit.extend(0..nl);
            }
        }
        // steps whose value is read where it is stored: a dictionary value from the part's step
        // data (each value's tests worked out once per part), a long value from its message, as
        // a scan reads it (with the session's steps without their values)
        let (fcode, fname) = pl.field.clone().unwrap();
        let mut cur: Option<(u32, Arc<crate::steps::SessionSteps>)> = None;
        let mut block: Option<(u64, Arc<StatBlock>)> = None;
        let mut held: Option<(u32, Option<Arc<crate::steps::SessionSteps>>)> = None;
        let mut memo: HashMap<(u32, u32), (u64, u64)> = HashMap::new();
        for li in longs_to_visit {
            let l = rows.longs[li as usize];
            let s = state.get(l.tool as usize).map_or(0, |x| x[l.failed as usize]);
            if s == 0 || !live_p(l.part) {
                continue;
            }
            let pi = head.parts[l.part as usize];
            let (fb, fv) = if l.vlen > 0 {
                match memo.get(&(l.part, l.voff)) {
                    Some(&x) => x,
                    None => {
                        // from the session's steps when they are in memory, else from the
                        // part's step data in its segment block
                        if held.as_ref().is_none_or(|h| h.0 != pi.session) {
                            held = Some((pi.session, db.cached_steps(pi.session)));
                        }
                        let in_mem = held.as_ref().unwrap().1.clone();
                        let owned;
                        let text: &[u8] = match &in_mem {
                            Some(ss) => {
                                let Some(&(_, v)) = ss.fields_of(l.pos).iter().find(|x| x.0 == fcode) else { return Err(QError::new("internal: summary names a step without the field")) };
                                owned = ss.value(fcode, v).to_vec();
                                &owned
                            }
                            None => {
                                let (r, off) = pi.steps;
                                if block.as_ref().is_none_or(|b| b.0 != r.off) {
                                    block = Some((r.off, db.step_block(num, &r).map_err(qe)?));
                                }
                                let b = block.as_ref().unwrap().1.post();
                                let a = off as usize + l.voff as usize;
                                b.get(a..a + l.vlen as usize).ok_or_else(|| QError::new("internal: summary value past its block"))?
                            }
                        };
                        let fb = pl.field_bits(FieldArg::Short(text), self.now)?;
                        let fv = if pl.needs_fval { self.text_id(text) } else { NULL };
                        memo.insert((l.part, l.voff), (fb, fv));
                        (fb, fv)
                    }
                }
            } else {
                let session = pi.session;
                if cur.as_ref().is_none_or(|c| c.0 != session) {
                    cur = Some((session, db.light_steps(session).map_err(qe)?));
                }
                let ss = cur.as_ref().unwrap().1.clone();
                let Some(&(_, v)) = ss.fields_of(l.pos).iter().find(|x| x.0 == fcode) else { return Err(QError::new("internal: summary names a step without the field")) };
                if !steps::is_long(v) {
                    return Err(QError::new("internal: summary names a long value the step lacks"));
                }
                let text = plan::long_value(db, session, &ss, l.pos, v, &fname)?;
                let fb = pl.field_bits(FieldArg::Long(text.as_deref()), self.now)?;
                (fb, match &text {
                    Some(t) if pl.needs_fval => self.text_id(t),
                    _ => NULL,
                })
            };
            let (sid, agent, sb) = pinfo(l.part);
            let tool = tool_of(l.tool);
            if s == 2 && !pl.ev(&resid[l.tool as usize][l.failed as usize], tool, l.failed, sb, fb) {
                continue;
            }
            let cif = pl.countif_bits(tool, l.failed, sb, fb);
            self.add(tool, l.failed, l.day * DAY_MS, sid, agent, fv, cif, W { count: 1, sum: l.out_bytes as u64, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
        }
        // steps without the field: the part table less the field table
        if needs_missing {
            let mut have: HashMap<(u32, u32, bool, i64), (u32, u64)> = HashMap::new();
            for r in &rows.rows {
                let e = have.entry((r.part, r.tool, r.failed, r.day)).or_default();
                e.0 += r.count;
                e.1 += r.sum_out;
            }
            for l in &rows.longs {
                let e = have.entry((l.part, l.tool, l.failed, l.day)).or_default();
                e.0 += 1;
                e.1 += l.out_bytes as u64;
            }
            for r in &head.rows {
                if !live_p(r.part) {
                    continue;
                }
                let (c, sum) = have.get(&(r.part, r.tool, r.failed, r.day)).copied().unwrap_or((0, 0));
                if c > r.count || sum > r.sum_out {
                    return Err(QError::new("internal: field table counts more steps than the part table"));
                }
                if c == r.count {
                    continue;
                }
                let (sid, agent, sb) = pinfo(r.part);
                let tool = tool_of(r.tool);
                if !pl.ev(&pl.pre, tool, r.failed, sb, missing) {
                    continue;
                }
                let cif = pl.countif_bits(tool, r.failed, sb, missing);
                self.add(tool, r.failed, r.day * DAY_MS, sid, agent, NULL, cif, W { count: r.count - c, sum: r.sum_out - sum, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
            }
        }
        Ok(())
    }

    /// Steps `start..end` of session `s`, read from its steps as a scan reads them.
    fn rows(&mut self, s: u32, start: u32, end: u32, pre: &Sp) -> Result<(), QError> {
        let db = self.db;
        let pl = self.pl;
        if self.test.is_none() {
            self.test = Some(Test::new(db, pre.clone(), self.now));
            for (_, f, a) in self.aggs {
                if *f == AggFn::CountIf {
                    let e = a.as_ref().unwrap();
                    self.countif_tests.push(Test::new(db, plan::compile(db, e, self.now)?, self.now));
                }
            }
        }
        let ss = db.try_steps(s).map_err(qe)?;
        let test = self.test.take().unwrap();
        let res = (|| -> Result<(), QError> {
            if !test.possible_in(db, &ss, s, self.now)? {
                return Ok(());
            }
            let (sid, agent) = if pl.needs_sid || pl.needs_agent { db.session_key(s) } else { (String::new(), String::new()) };
            let sid = if pl.needs_sid { self.sids.id(&sid) } else { 0 };
            let agent = if pl.needs_agent { self.agents.id(&agent) } else { 0 };
            let field = pl.field.clone();
            for pos in start..end.min(ss.rows.len() as u32) {
                self.rows_read += 1;
                if !test.test(db, s, &ss, pos, self.now)? {
                    continue;
                }
                let r = ss.rows[pos as usize];
                let fval = match &field {
                    None => NULL,
                    Some((f, name)) => match ss.fields_of(pos).iter().find(|x| x.0 == *f) {
                        None => NULL,
                        Some(&(_, v)) if steps::is_long(v) => match plan::long_value(db, s, &ss, pos, v, name)? {
                            Some(t) => self.text_id(&t),
                            None => NULL,
                        },
                        Some(&(_, v)) => {
                            let t = ss.value(*f, v).to_vec();
                            self.text_id(&t)
                        }
                    },
                };
                let mut cif = 0u64;
                for (k, t) in self.countif_tests.iter().enumerate() {
                    if t.test(db, s, &ss, pos, self.now)? {
                        cif |= 1 << k;
                    }
                }
                self.add(r.tool, r.failed, r.time_ms, sid, agent, fval, cif, W { count: 1, sum: r.out_bytes as u64, min_out: r.out_bytes, max_out: r.out_bytes, min_t: r.time_ms, max_t: r.time_ms });
            }
            Ok(())
        })();
        self.test = Some(test);
        res
    }

    fn field_text(&self, id: u64) -> Result<Vec<u8>, QError> {
        if id & TEXT_TAG != 0 || self.single.is_none() {
            return Ok(self.texts.texts[(id & !TEXT_TAG) as usize].clone());
        }
        let fd = self.single_dir.as_ref().unwrap();
        let b = self.db.stats_values(self.single.unwrap(), fd, id as u32).map_err(qe)?;
        Ok(b.vals().get(id as u32).to_vec())
    }

    fn key_value(&self, k: KeyK, id: u64) -> Result<Value, QError> {
        if id == NULL {
            return Ok(Value::Null);
        }
        Ok(match k {
            KeyK::Tool => Value::Str(String::from_utf8_lossy(self.db.step_dicts().tools.get(id as u32)).into()),
            KeyK::Sid => Value::Str(self.sids.names[id as usize].as_str().into()),
            KeyK::Agent => Value::Str(self.agents.names[id as usize].as_str().into()),
            KeyK::Failed | KeyK::Ok => Value::Bool(id == 1),
            KeyK::Field => Value::from_json(&self.field_text(id)?),
            KeyK::Null => Value::Null,
            KeyK::Bin(d) => Value::Time(id as i64 * d),
        })
    }

    fn agg_value(&self, g: usize, ai: usize) -> Value {
        let at = g * self.acc.na + ai;
        match self.pl.aggs[ai] {
            AggK::Count | AggK::CountIf(_) => Value::Int(self.acc.count[at]),
            AggK::DTool | AggK::DSid | AggK::DAgent | AggK::DField => Value::Int(self.acc.distinct[at].len() as i64),
            AggK::DNone => Value::Int(0),
            AggK::Sum => Value::Int(self.acc.sum[at] as i64),
            AggK::Avg => {
                if self.acc.count[at] == 0 {
                    Value::Null
                } else {
                    Value::Float(self.acc.sum[at] as f64 / self.acc.count[at] as f64)
                }
            }
            AggK::MinOut => self.acc.lo[at].map_or(Value::Null, Value::Int),
            AggK::MaxOut => self.acc.hi[at].map_or(Value::Null, Value::Int),
            AggK::MinTime => self.acc.lo[at].map_or(Value::Null, Value::Time),
            AggK::MaxTime => self.acc.hi[at].map_or(Value::Null, Value::Time),
        }
    }

    /// Column `c` of group `g` when it is an integer aggregate (compared as `Value::order` would).
    fn int_value(&self, g: usize, c: usize) -> Option<i64> {
        let ai = c.checked_sub(self.pl.keys.len())?;
        let at = g * self.acc.na + ai;
        match self.pl.aggs[ai] {
            AggK::Count | AggK::CountIf(_) => Some(self.acc.count[at]),
            AggK::Sum => Some(self.acc.sum[at] as i64),
            AggK::DTool | AggK::DSid | AggK::DAgent | AggK::DField => Some(self.acc.distinct[at].len() as i64),
            AggK::DNone => Some(0),
            _ => None,
        }
    }

    fn row(&self, g: usize) -> Result<Row, QError> {
        let mut vals = Vec::with_capacity(self.pl.keys.len() + self.pl.aggs.len());
        for (i, k) in self.pl.keys.iter().enumerate() {
            vals.push(self.key_value(*k, self.acc.key(g)[i])?);
        }
        for ai in 0..self.pl.aggs.len() {
            vals.push(self.agg_value(g, ai));
        }
        Ok(Row { session: NONE, steps: [NONE; MAX_ELEMS], vals })
    }
}

/// Runs `summarize` (stage `aggs`, `by`, after leading `where` stages compiled into `pre`)
/// from the summaries, with `rest` the stages after it. Returns the table and how many of
/// `rest` it also did (a `sort` with `take`, or `top`, over the groups), or `None` when the
/// query is not one the summaries can answer.
#[allow(clippy::type_complexity)]
pub fn try_run(db: &Db, pre: &Sp, aggs: &[(String, AggFn, Option<Expr>)], by: &[(String, Expr)], rest: &[Stage], now: i64, st: &mut QStats) -> Result<Option<(Table, usize)>, QError> {
    if !order_free(by, rest) {
        return Ok(None);
    }
    let Some(pl) = Plan::new(db, pre, aggs, by, now)? else { return Ok(None) };
    let sources = db.step_sources().map_err(qe)?;
    // value ids are group ids when one segment holds every value of the field
    let mut single = None;
    let mut single_dir = None;
    if let Some((f, _)) = &pl.field {
        let mut segs_with = Vec::new();
        let mut other = false;
        for s in &sources {
            match s {
                StepSource::Seg { num, .. } => {
                    let h = db.stats_head(*num).map_err(qe)?.ok_or_else(|| QError::new("internal: segment without summaries"))?;
                    let sf = db.seg_codes(*num).and_then(|(_, fs)| fs.iter().position(|x| x == f));
                    if let Some(d) = sf.and_then(|sf| h.head().field(sf as u32)) {
                        segs_with.push((*num, d.clone()));
                        // values met as text (long ones, parts read from their steps) group
                        // by text, so then every value does
                        other |= d.n_longs > 0 || !db.stats_rows(*num, h.head(), d).map_err(qe)?.rows().inexact.is_empty();
                    }
                }
                StepSource::Rows { .. } => other = true,
            }
        }
        if segs_with.len() == 1 && !other {
            single = Some(segs_with[0].0);
            single_dir = Some(segs_with[0].1.clone());
        }
    }
    let na = pl.aggs.len();
    let dense_len = match pl.keys.as_slice() {
        [KeyK::Tool] => db.step_dicts().tools.len(),
        [KeyK::Failed] | [KeyK::Ok] => 2,
        [KeyK::Field] => single_dir.as_ref().map_or(0, |d| d.n_values as usize),
        _ => 0,
    };
    let minmax = pl.aggs.iter().any(|a| matches!(a, AggK::MinOut | AggK::MaxOut | AggK::MinTime | AggK::MaxTime));
    let sets = pl.aggs.iter().any(|a| matches!(a, AggK::DTool | AggK::DSid | AggK::DAgent | AggK::DField | AggK::DNone));
    let sums = pl.aggs.iter().any(|a| matches!(a, AggK::Sum | AggK::Avg));
    let acc = Accs::new(na, sums, minmax, sets, dense_len);
    let mut ex = Exec { db, pl: &pl, aggs, now, acc, sids: Names::default(), agents: Names::default(), texts: Texts::default(), single, single_dir, test: None, countif_tests: Vec::new(), rows_read: 0 };
    let mut segs = 0;
    for s in &sources {
        match s {
            StepSource::Seg { num, live } => {
                ex.segment(*num, live.as_ref(), pre)?;
                segs += 1;
            }
            StepSource::Rows { session, start, end } => ex.rows(*session, *start, *end, pre)?,
        }
    }
    st.summary_segments = segs;
    st.steps_scanned = ex.rows_read;
    let mut groups = ex.acc.groups();
    if pl.keys.is_empty() && groups.is_empty() {
        groups.push(ex.acc.new_group([0; 4]));
    }
    let cols: Vec<String> = by.iter().map(|(n, _)| n.clone()).chain(aggs.iter().map(|(n, _, _)| n.clone())).collect();
    // `sort ... | take n` or `top n` over the groups: pick the n rows before making them
    let sel: Option<(Vec<(usize, bool)>, usize, usize)> = match rest {
        [Stage::Sort(keys), Stage::Take(n), ..] => keys.iter().map(|(e, d)| col_index(&cols, e).map(|c| (c, *d))).collect::<Option<Vec<_>>>().map(|k| (k, *n, 2)),
        [Stage::Top { n, by, desc }, ..] => col_index(&cols, by).map(|c| (vec![(c, *desc)], *n, 1)),
        _ => None,
    };
    let ng = groups.len();
    if let Some((keys, n, used)) = sel
        && n <= 10_000
    {
        let nk = pl.keys.len();
        // with an integer first key, groups worse than the n-th best on it cannot be kept:
        // find that value first, and compare only the groups that reach it
        let mut cands: Vec<usize> = Vec::new();
        if let Some(&(c0, desc0)) = keys.first()
            && n > 0
            && n < ng
            && ex.int_value(groups[0], c0).is_some()
        {
            let pairs: Vec<(i64, usize)> = groups.iter().map(|&g| (ex.int_value(g, c0).unwrap(), g)).collect();
            let mut prim: Vec<i64> = pairs.iter().map(|p| p.0).collect();
            let t = if desc0 {
                *prim.select_nth_unstable_by(n - 1, |a, b| b.cmp(a)).1
            } else {
                *prim.select_nth_unstable(n - 1).1
            };
            cands = pairs.into_iter().filter(|&(x, _)| if desc0 { x >= t } else { x <= t }).map(|p| p.1).collect();
        } else {
            cands.clone_from(&groups);
        }
        // key values of the groups compared, made once each when first needed
        let mut cache: HashMap<(usize, usize), Value> = HashMap::new();
        let mut val = |ex: &Exec, g: usize, c: usize| -> Result<Value, QError> {
            if c < nk {
                if let Some(v) = cache.get(&(g, c)) {
                    return Ok(v.clone());
                }
                let v = ex.key_value(pl.keys[c], ex.acc.key(g)[c])?;
                cache.insert((g, c), v.clone());
                Ok(v)
            } else {
                Ok(ex.agg_value(g, c - nk))
            }
        };
        let mut cmp = |ex: &Exec, a: usize, b: usize| -> Result<Ordering, QError> {
            for &(c, desc) in &keys {
                let o = match (ex.int_value(a, c), ex.int_value(b, c)) {
                    (Some(x), Some(y)) => x.cmp(&y),
                    _ => val(ex, a, c)?.order(&val(ex, b, c)?),
                };
                let o = if desc { o.reverse() } else { o };
                if o.is_ne() {
                    return Ok(o);
                }
            }
            Ok(a.cmp(&b))
        };
        let mut best: Vec<usize> = Vec::with_capacity(n + 1);
        for g in cands {
            if n == 0 {
                break;
            }
            if best.len() == n && cmp(&ex, g, best[n - 1])? != Ordering::Less {
                continue;
            }
            // insert in order
            let (mut lo, mut hi) = (0, best.len());
            while lo < hi {
                let m = (lo + hi) / 2;
                if cmp(&ex, best[m], g)? == Ordering::Less {
                    lo = m + 1;
                } else {
                    hi = m;
                }
            }
            best.insert(lo, g);
            best.truncate(n);
        }
        let rows = best.into_iter().map(|g| ex.row(g)).collect::<Result<Vec<_>, _>>()?;
        return Ok(Some((Table { kind: Kind::Plain, cols, rows }, used)));
    }
    let rows = groups.iter().map(|&g| ex.row(g)).collect::<Result<Vec<_>, _>>()?;
    Ok(Some((Table { kind: Kind::Plain, cols, rows }, 0)))
}

fn col_index(cols: &[String], e: &Expr) -> Option<usize> {
    match e {
        Expr::Ref(p) if p.len() == 1 => cols.iter().position(|c| *c == p[0]),
        _ => None,
    }
}
