//! `tdb check-rules`: generated column rules give the same answers however their values
//! were worked out.
//!
//! Works on a copy of the store. Rules are generated from a seed and added a batch at a
//! time. For every rule a few queries are answered three times: right after the rule is
//! defined, when no segment holds its values and they are worked out as sessions are read;
//! after a merge has stored them, when the step summaries answer; and (with `write_time`)
//! on a second store that had every rule before its first request, filled by replaying
//! the first store's requests. The three sets of answers must be equal. Later rules read
//! earlier ones, so stored values feed values being worked out.
//!
//! Only counts leave this module: rule values are compared in memory.

use crate::codec::{Error, Result};
use crate::db::{Db, Options, Put};
use crate::query::{self, value::Value};
use crate::synth::Rng;
use std::path::Path;

#[derive(Debug, Default)]
pub struct Report {
    pub rules: usize,
    /// Rules that gave at least one step a value.
    pub with_values: usize,
    /// Rules that read the result text, a long input field, or another rule.
    pub read_output: usize,
    pub read_rules: usize,
    pub queries: usize,
    pub rows: u64,
    /// Rules with an answer that differs before and after the merge.
    pub differ: usize,
    /// Rules with an answer that differs on the store that had the rules from the start.
    pub differ_write_time: usize,
    pub write_time: bool,
    /// Rules whose values a query read from the summaries before any segment held them
    /// (must be 0), and rules the summaries did not answer for after the merge.
    pub summarized_early: usize,
    pub not_summarized: usize,
    /// (rule number, query number, stage) of the first differences.
    pub first: Vec<(usize, usize, &'static str)>,
}

fn bad(e: impl std::fmt::Display) -> Error {
    Error::BadRequest(e.to_string())
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let name = e.file_name();
        if name == "LOCK" {
            continue;
        }
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &to.join(&name))?;
        } else {
            std::fs::copy(e.path(), to.join(&name))?;
        }
    }
    Ok(())
}

const REGEXES: &[&str] = &[r"^\s*(\S+)", r"(\d+)", r"\.(\w+)$", r"^(\w+)", r"/([^/]+)$", r"(?i)(error|fail\w*|warn\w*)", r"--(\w[\w-]*)", r"^(.{0,20})", r"(\S+)\s*$", r"(?s)(\w+)\W+\w+\s*$", r"[A-Z]\w+"];
const WORDS: &[&str] = &["test", "error", "src", "git", "cargo", "npm", "py", ".rs", "/", "fail", " -", "main"];
const FIELDS: &[&str] = &["command", "file_path", "pattern", "description", "content", "old_string", "new_string", "url", "query", "prompt", "path"];

/// What a generated rule gives: decides which rules a later rule can build on.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Text,
    Number,
    Flag,
}

struct Gen<'a> {
    rng: Rng,
    fields: Vec<&'a str>,
    tools: Vec<String>,
    /// (name, kind) of the rules so far.
    made: Vec<(String, Kind)>,
    /// Whether rules read `out_bytes`. Not when a second store is filled by replaying
    /// requests: a result that held an image is smaller there, the image being blank.
    sizes: bool,
}

impl Gen<'_> {
    /// Text to work on: an input field, the result text, or an earlier text rule.
    fn text(&mut self) -> String {
        let earlier: Vec<&String> = self.made.iter().filter(|m| m.1 == Kind::Text).map(|m| &m.0).collect();
        match self.rng.below(10) {
            0..=2 => "output".to_string(),
            3 if !earlier.is_empty() => (*self.rng.pick(&earlier)).clone(),
            _ => self.rng.pick(&self.fields).to_string(),
        }
    }

    fn number(&mut self) -> String {
        let earlier: Vec<&String> = self.made.iter().filter(|m| m.1 == Kind::Number).map(|m| &m.0).collect();
        match self.rng.below(6) {
            0 if self.sizes => "out_bytes".to_string(),
            1 if !earlier.is_empty() => (*self.rng.pick(&earlier)).clone(),
            2 => format!(r#"toint(extract({}, @"(\d+)"))"#, self.text()),
            3 if self.sizes => format!("out_bytes / {}", 1 + self.rng.below(4096)),
            3 => format!("strlen({}) / {}", self.text(), 1 + self.rng.below(64)),
            _ => format!("strlen({})", self.text()),
        }
    }

    fn flag(&mut self) -> String {
        let earlier: Vec<&String> = self.made.iter().filter(|m| m.1 == Kind::Flag).map(|m| &m.0).collect();
        match self.rng.below(9) {
            0 => "failed".to_string(),
            1 => "not failed".to_string(),
            2 if !earlier.is_empty() => (*self.rng.pick(&earlier)).clone(),
            3 => format!("{} > {}", self.number(), self.rng.below(2000)),
            4 => format!(r#"tool == "{}""#, self.rng.pick(&self.tools)),
            5 => format!(r#"{} matches regex @"{}""#, self.text(), self.rng.pick(REGEXES)),
            6 => format!("isnull({})", self.text()),
            7 => format!(r#"tool in ("{}", "{}")"#, self.rng.pick(&self.tools), self.rng.pick(&self.tools)),
            _ => format!(r#"{} contains "{}""#, self.text(), self.rng.pick(WORDS)),
        }
    }

    fn rule(&mut self, n: usize) -> String {
        let name = format!("r{n}");
        let (kind, expr) = match self.rng.below(10) {
            0..=3 => {
                let (t, re) = (self.text(), *self.rng.pick(REGEXES));
                let e = format!(r#"extract({t}, @"{re}")"#);
                (Kind::Text, if self.rng.chance(0.3) { format!("tolower({e})") } else { e })
            }
            4..=6 => (Kind::Number, self.number()),
            7 => {
                let (c, a, b) = (self.flag(), self.text(), self.text());
                (Kind::Text, format!("iff({c}, {a}, {b})"))
            }
            _ => (Kind::Flag, self.flag()),
        };
        let on = if self.rng.chance(0.5) { format!(" on {}", self.flag()) } else { String::new() };
        self.made.push((name.clone(), kind));
        format!("{name}{on} = {expr}")
    }
}

/// A value written as the query language reads it; `None` for one it cannot spell.
fn literal(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) if s.len() <= 120 && !s.chars().any(|c| c.is_control()) => Some(format!("@\"{}\"", s.replace('"', "\"\""))),
        Value::Int(i) => Some(i.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// One query's rows, typed (`Str("3")` and `Int(3)` differ).
type Answer = Vec<Vec<String>>;

fn ask(db: &Db, q: &str) -> Result<(Answer, query::exec::QStats)> {
    let r = query::run(db, q, 1).map_err(|e| bad(format!("{e}: {q}")))?;
    Ok((r.table.rows.iter().map(|row| row.vals.iter().map(|v| format!("{v:?}")).collect()).collect(), r.stats))
}

/// The queries asked about a rule. The first lists every value with its count; the rest
/// are built around the commonest value it shows, so they are the same at every stage.
fn queries(db: &Db, rule: &str) -> Result<Vec<String>> {
    let first = format!("summarize n = count() by {rule} | sort by {rule} asc");
    let r = query::run(db, &first, 1).map_err(|e| bad(format!("{e}: {first}")))?;
    let mut qs = vec![first, format!("where isnotnull({rule}) | summarize n = count(), s = dcount(session), d = dcount({rule})"), format!("summarize n = count(), f = countif(failed) by tool, got = isnotnull({rule}) | sort by tool asc, got asc")];
    let mut best: Option<(i64, String, bool)> = None;
    for row in &r.table.rows {
        let n = match row.vals.get(1) {
            Some(Value::Int(n)) => *n,
            _ => 0,
        };
        if let Some(l) = literal(&row.vals[0])
            && best.as_ref().is_none_or(|b| n > b.0)
        {
            best = Some((n, l, matches!(row.vals[0], Value::Str(_))));
        }
    }
    if let Some((_, v, text)) = best {
        qs.push(format!("where {rule} == {v} | summarize n = count() by session | sort by session asc"));
        qs.push(format!("match a: {rule} == {v} then b: not failed | summarize n = count(), s = dcount(session)"));
        if text {
            qs.push(format!("where {rule} contains {v} | summarize n = count(), s = dcount(session)"));
        }
    }
    Ok(qs)
}

/// Copy the store at `store` to `work`, and run the check there with `n` rules.
pub fn run(store: &Path, work: &Path, opts: &Options, n: usize, seed: u64, batch: usize, write_time: bool) -> Result<Report> {
    let _ = std::fs::remove_dir_all(work);
    let (da, db_dir) = (work.join("a"), work.join("b"));
    copy_dir(store, &da)?;
    let mut a = Db::open(&da, opts.clone())?;
    if !a.rules().is_empty() {
        return Err(bad("the store already has rules: check a store without any"));
    }
    let sd = a.step_dicts();
    let names: Vec<String> = (0..sd.fields.len() as u32).map(|c| String::from_utf8_lossy(sd.fields.get(c)).into_owned()).collect();
    let fields: Vec<&str> = FIELDS.iter().copied().filter(|f| names.iter().any(|n| n == f)).collect();
    let tools: Vec<String> = (0..sd.tools.len() as u32).map(|c| String::from_utf8_lossy(sd.tools.get(c)).into_owned()).filter(|t| t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')).take(12).collect();
    if fields.is_empty() || tools.is_empty() {
        return Err(bad("the store has no steps to write rules about"));
    }
    let mut g = Gen { rng: Rng::new(seed), fields, tools, made: Vec::new(), sizes: !write_time };
    let defs: Vec<String> = (0..n).map(|k| g.rule(k)).collect();
    let mut rep = Report { rules: n, write_time, ..Report::default() };

    // the second store has every rule before its first request
    let b = if write_time {
        let mut b = Db::open(&db_dir, opts.clone())?;
        for d in &defs {
            b.define_rule(d)?;
        }
        let mut body = Vec::new();
        for (k, ri) in a.all_requests()?.into_iter().enumerate() {
            let r = a.request(ri)?;
            let (sid, agent) = a.session_key(r.session);
            body.clear();
            a.rebuild_into(ri, &mut body, &mut Default::default())?;
            b.put(&Put { session: &sid, agent: &agent, id: &r.id, time: &r.time, meta: &r.meta, body: &body })?;
            if k % 256 == 255 {
                b.commit()?;
            }
        }
        b.commit()?;
        Some(b)
    } else {
        None
    };

    let mut at = 0;
    while at < n {
        let end = (at + batch.max(1)).min(n);
        for d in &defs[at..end] {
            a.define_rule(d)?;
        }
        let info: Vec<(String, bool, bool)> = a.rules().rules[at..end].iter().map(|r| (r.name.clone(), r.reads_output, !r.deps.is_empty())).collect();
        // worked out as sessions are read
        let mut asked: Vec<(Vec<String>, Vec<Answer>)> = Vec::new();
        for (k, (name, _, _)) in info.iter().enumerate() {
            let qs = queries(&a, name)?;
            let mut ans = Vec::new();
            for (qi, q) in qs.iter().enumerate() {
                let (rows, st) = ask(&a, q)?;
                if qi == 0 && st.summary_segments > 0 {
                    rep.summarized_early += 1;
                    let _ = k;
                }
                ans.push(rows);
            }
            asked.push((qs, ans));
        }
        // stored by a merge
        a.compact()?;
        if a.rule_coverage()[at..end].iter().any(|&(have, of)| have != of) {
            return Err(bad("a merge left a segment without the rules' values"));
        }
        for (k, (name, reads_output, reads_rules)) in info.iter().enumerate() {
            let (qs, before) = &asked[k];
            let (mut differs, mut differs_wt) = (false, false);
            for (qi, q) in qs.iter().enumerate() {
                let (rows, st) = ask(&a, q)?;
                rep.queries += 1;
                rep.rows += rows.len() as u64;
                if qi == 0 && !(st.summarized && st.steps_scanned == 0) {
                    rep.not_summarized += 1;
                }
                if rows != before[qi] {
                    differs = true;
                    if rep.first.len() < 5 {
                        rep.first.push((at + k, qi, "after the merge"));
                    }
                }
                if let Some(b) = &b {
                    let (wt, _) = ask(b, q)?;
                    if wt != rows {
                        differs_wt = true;
                        if rep.first.len() < 5 {
                            rep.first.push((at + k, qi, "written under the rules"));
                        }
                    }
                }
            }
            rep.differ += differs as usize;
            rep.differ_write_time += differs_wt as usize;
            rep.read_output += *reads_output as usize;
            rep.read_rules += *reads_rules as usize;
            // a rule with a value shows more than the one row of steps without any
            if before[0].len() > 1 || before[0].first().is_some_and(|r| r[0] != "Null") {
                rep.with_values += 1;
            }
            let _ = name;
        }
        at = end;
    }
    drop(a);
    drop(b);
    let _ = std::fs::remove_dir_all(work);
    Ok(rep)
}
