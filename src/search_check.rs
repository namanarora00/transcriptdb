//! Check text search against `grep`.
//!
//! Regular expressions are drawn from the text too (two pieces of a line with anything
//! between, either of two pieces, a piece with a run of letters replaced by `[a-z]+`, the
//! start or the end of a line) and compared with `/usr/bin/grep -E`.
//!
//! Writes every searchable unit to a file, one per line (the user's and the assistant's
//! text blocks in one file, each of the most used input fields in its own), draws patterns
//! from the text itself, and for each compares the units a query finds with the lines
//! `/usr/bin/grep -F` finds. Half the patterns are matched exactly (`contains_cs`), half
//! ignoring ASCII case (`contains`, pattern in mixed case). Prints counts only: neither the
//! text nor the patterns leave the work directory, which is removed at the end.

use crate::db::Db;
use crate::query::exec;
use crate::query::parse::{Expr, Query, Source, Stage, StrOp};
use crate::query::value::Value;
use crate::steps;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::process::Command;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// One file of units: where each line's unit is, and the lines' text for drawing patterns.
struct Dump {
    name: String,
    /// `Some(field)` for an input field, `None` for message text.
    field: Option<String>,
    path: std::path::PathBuf,
    /// (session, node, block) of a text unit or (session, step, 0) per line.
    at: Vec<(u32, u32, u32)>,
    lines: Vec<Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct Report {
    pub files: Vec<(String, usize, u64)>,
    pub patterns: usize,
    pub exact: usize,
    pub hits: u64,
    pub differ: usize,
    /// (pattern number, file, hits by the engine, hits by grep) for the first differences.
    pub first: Vec<(usize, String, usize, usize)>,
    /// Regular expressions compared with `grep -E`.
    pub regexes: usize,
    pub regex_hits: u64,
    pub regex_differ: usize,
    pub regex_first: Vec<(usize, String, usize, usize)>,
    /// Filters over message text run with the word index and by reading every text.
    pub index_filters: usize,
    pub index_rows: u64,
    pub index_differ: usize,
    /// (filter number, kind, rows with the index, rows by reading) for the first differences.
    pub index_first: Vec<(usize, usize, usize, usize)>,
}

type R<T> = std::result::Result<T, String>;

fn line_bytes(t: &[u8]) -> Vec<u8> {
    t.iter().map(|&c| if c == b'\n' { 1 } else { c }).collect()
}

fn write_dump(d: &Dump) -> R<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(&d.path).map_err(|e| e.to_string())?);
    for l in &d.lines {
        f.write_all(l).and_then(|_| f.write_all(b"\n")).map_err(|e| e.to_string())?;
    }
    f.flush().map_err(|e| e.to_string())
}

/// A substring of a random line, 3 to 32 bytes, with no control characters.
fn draw(d: &Dump, rng: &mut Rng, ascii: bool) -> Option<Vec<u8>> {
    for _ in 0..200 {
        let l = &d.lines[rng.below(d.lines.len())];
        if l.len() < 3 {
            continue;
        }
        let want = 3 + rng.below(30);
        let mut a = rng.below(l.len());
        while a > 0 && (l[a] & 0xC0) == 0x80 {
            a -= 1;
        }
        let mut b = (a + want).min(l.len());
        while b < l.len() && (l[b] & 0xC0) == 0x80 {
            b += 1;
        }
        let p = &l[a..b];
        if p.len() < 3 || p.iter().any(|&c| c < 0x20 || c == 0x7f) || p.iter().all(|&c| c == b' ') || std::str::from_utf8(p).is_err() {
            continue;
        }
        if ascii && !(p.is_ascii() && p.iter().any(|c| c.is_ascii_alphabetic())) {
            continue;
        }
        return Some(p.to_vec());
    }
    None
}

/// `p` as a regular expression that matches exactly it, in both `grep -E` and the engine.
fn escape(p: &[u8], out: &mut Vec<u8>) {
    for &c in p {
        if b".^$*+?()[]{}|\\".contains(&c) {
            out.push(b'\\');
        }
        out.push(c);
    }
}

/// A piece of `l` of 3 to 10 bytes starting at or after `from`, as a byte range.
fn piece_of(l: &[u8], from: usize, rng: &mut Rng) -> Option<(usize, usize)> {
    if l.len() < from + 3 {
        return None;
    }
    let mut a = from + rng.below(l.len() - from - 2);
    while a < l.len() && (l[a] & 0xC0) == 0x80 {
        a += 1;
    }
    let mut b = (a + 3 + rng.below(8)).min(l.len());
    while b < l.len() && (l[b] & 0xC0) == 0x80 {
        b += 1;
    }
    let p = l.get(a..b)?;
    (p.len() >= 3 && !p.iter().any(|&c| c < 0x20 || c == 0x7f) && std::str::from_utf8(p).is_ok()).then_some((a, b))
}

/// A regular expression that matches at least the line it was drawn from.
fn draw_regex(d: &Dump, rng: &mut Rng, kind: usize) -> Option<Vec<u8>> {
    for _ in 0..200 {
        let l = &d.lines[rng.below(d.lines.len())];
        if l.len() < 8 {
            continue;
        }
        let mut out = Vec::new();
        match kind % 4 {
            0 => {
                let Some((a, b)) = piece_of(l, 0, rng) else { continue };
                let Some((c, e)) = piece_of(l, b, rng) else { continue };
                escape(&l[a..b], &mut out);
                out.extend_from_slice(b".*");
                escape(&l[c..e], &mut out);
            }
            1 => {
                let Some((a, b)) = piece_of(l, 0, rng) else { continue };
                let l2 = &d.lines[rng.below(d.lines.len())];
                let Some((c, e)) = piece_of(l2, 0, rng) else { continue };
                out.push(b'(');
                escape(&l[a..b], &mut out);
                out.push(b'|');
                escape(&l2[c..e], &mut out);
                out.push(b')');
            }
            2 => {
                // a run of lower-case letters with text on both sides becomes [a-z]+
                let start = rng.below(l.len());
                let Some(a) = (start..l.len()).find(|&i| l[i].is_ascii_lowercase() && (i == 0 || !l[i - 1].is_ascii_lowercase())) else { continue };
                let b = (a..l.len()).find(|&i| !l[i].is_ascii_lowercase()).unwrap_or(l.len());
                if a < 3 || b + 3 > l.len() {
                    continue;
                }
                let mut pa = a - 3;
                while pa > 0 && (l[pa] & 0xC0) == 0x80 {
                    pa -= 1;
                }
                let mut sb = b + 3;
                while sb < l.len() && (l[sb] & 0xC0) == 0x80 {
                    sb += 1;
                }
                let (pre, post) = (&l[pa..a], &l[b..sb]);
                if pre.iter().chain(post).any(|&c| c < 0x20 || c == 0x7f) || std::str::from_utf8(pre).is_err() || std::str::from_utf8(post).is_err() {
                    continue;
                }
                escape(pre, &mut out);
                out.extend_from_slice(b"[a-z]+");
                escape(post, &mut out);
            }
            _ => {
                let mut n = (4 + rng.below(8)).min(l.len());
                if rng.below(2) == 0 {
                    while n < l.len() && (l[n] & 0xC0) == 0x80 {
                        n += 1;
                    }
                    let p = &l[..n];
                    if p.iter().any(|&c| c < 0x20 || c == 0x7f) || std::str::from_utf8(p).is_err() {
                        continue;
                    }
                    out.push(b'^');
                    escape(p, &mut out);
                } else {
                    let mut a = l.len() - n;
                    while a > 0 && (l[a] & 0xC0) == 0x80 {
                        a -= 1;
                    }
                    let p = &l[a..];
                    if p.iter().any(|&c| c < 0x20 || c == 0x7f) || std::str::from_utf8(p).is_err() {
                        continue;
                    }
                    escape(p, &mut out);
                    out.push(b'$');
                }
            }
        }
        return Some(out);
    }
    None
}

fn grep(d: &Dump, pat: &[u8], cs: bool) -> R<Vec<u32>> {
    grep_how(d, pat, if cs { &["-F"] } else { &["-F", "-i"] })
}

fn grep_how(d: &Dump, pat: &[u8], how: &[&str]) -> R<Vec<u32>> {
    use std::os::unix::ffi::OsStrExt;
    let mut c = Command::new("/usr/bin/grep");
    c.env("LC_ALL", "C").arg("-a").arg("-n");
    for h in how {
        c.arg(h);
    }
    let out = c.arg("-e").arg(std::ffi::OsStr::from_bytes(pat)).arg(&d.path).output().map_err(|e| format!("running grep: {e}"))?;
    if !out.status.success() && out.status.code() != Some(1) {
        return Err(format!("grep failed with {:?}", out.status.code()));
    }
    let mut v = Vec::new();
    for line in out.stdout.split(|&c| c == b'\n') {
        let digits = line.iter().take_while(|c| c.is_ascii_digit()).count();
        if digits > 0 && line.get(digits) == Some(&b':') {
            v.push(std::str::from_utf8(&line[..digits]).unwrap().parse::<u32>().map_err(|e| e.to_string())? - 1);
        }
    }
    Ok(v)
}

fn engine(db: &Db, d: &Dump, line_of: &HashMap<(u32, u32, u32), u32>, pat: &[u8], cs: bool) -> R<Vec<u32>> {
    engine_op(db, d, line_of, StrOp::Contains, pat, cs)
}

fn engine_op(db: &Db, d: &Dump, line_of: &HashMap<(u32, u32, u32), u32>, op: StrOp, pat: &[u8], cs: bool) -> R<Vec<u32>> {
    let col = match &d.field {
        None => vec!["text".to_string()],
        Some(f) => vec!["input".to_string(), f.clone()],
    };
    let e = Expr::Str { op, e: Box::new(Expr::Ref(col)), pat: Box::new(Expr::Lit(Value::Str(String::from_utf8_lossy(pat).as_ref().into()))), neg: false, cs };
    let q = Query { source: if d.field.is_none() { Source::Messages } else { Source::Steps }, stages: vec![Stage::Where(e)] };
    let run = exec::run(db, &q, 1).map_err(|e| e.to_string())?;
    let mut v = Vec::with_capacity(run.table.rows.len());
    for r in &run.table.rows {
        let k = if d.field.is_none() { (r.session, r.steps[0], r.steps[1]) } else { (r.session, r.steps[0], 0) };
        match line_of.get(&k) {
            Some(&l) => v.push(l),
            None => return Err("the engine returned a unit that is not in the dump".into()),
        }
    }
    v.sort_unstable();
    Ok(v)
}

pub fn run(db: &Db, n_patterns: usize, seed: u64, work: &Path, top_fields: usize) -> R<Report> {
    std::fs::create_dir_all(work).map_err(|e| e.to_string())?;
    let e = |x: crate::codec::Error| x.to_string();
    let ns = db.session_count() as u32;
    // message text
    let mut msgs = Dump { name: "user and assistant text".into(), field: None, path: work.join("messages.txt"), at: Vec::new(), lines: Vec::new() };
    for s in 0..ns {
        let t = db.session_texts(s).map_err(e)?;
        for u in 0..t.units.len() {
            msgs.at.push((s, t.units[u].node, t.units[u].block as u32));
            msgs.lines.push(line_bytes(t.text_of(u).as_bytes()));
        }
    }
    // the most used input fields
    let sd = db.step_dicts();
    let mut uses: HashMap<u32, u64> = HashMap::new();
    for s in 0..ns {
        let ss = db.try_steps(s).map_err(e)?;
        for pos in 0..ss.rows.len() as u32 {
            for &(f, _) in ss.fields_of(pos) {
                *uses.entry(f).or_default() += 1;
            }
        }
    }
    let mut top: Vec<(u32, u64)> = uses.into_iter().collect();
    top.sort_by_key(|x| (std::cmp::Reverse(x.1), x.0));
    top.truncate(top_fields);
    let mut dumps = vec![msgs];
    for (k, &(f, _)) in top.iter().enumerate() {
        let name = String::from_utf8_lossy(sd.fields.get(f)).into_owned();
        dumps.push(Dump { name: format!("input field {}", k + 1), field: Some(name), path: work.join(format!("field-{k}.txt")), at: Vec::new(), lines: Vec::new() });
    }
    for s in 0..ns {
        let ss = db.try_steps(s).map_err(e)?;
        for pos in 0..ss.rows.len() as u32 {
            let mut seen: Vec<u32> = Vec::new();
            for &(f, v) in ss.fields_of(pos) {
                let Some(k) = top.iter().position(|x| x.0 == f) else { continue };
                // a key given twice: the first is the step's value
                if seen.contains(&f) {
                    continue;
                }
                seen.push(f);
                let d = &mut dumps[k + 1];
                let raw = if steps::is_long(v) { db.long_value(s, &ss, pos, v, d.field.as_deref().unwrap()).map_err(e)?.unwrap_or_default() } else { ss.value(f, v).to_vec() };
                let text = match Value::from_json(&raw) {
                    Value::Null => String::new(),
                    Value::Str(t) => t.to_string(),
                    other => other.to_string(),
                };
                d.at.push((s, pos, 0));
                d.lines.push(line_bytes(text.as_bytes()));
            }
        }
    }
    let mut rep = Report::default();
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let mut maps = Vec::new();
    for d in &dumps {
        write_dump(d)?;
        rep.files.push((d.name.clone(), d.lines.len(), d.lines.iter().map(|l| l.len() as u64).sum()));
        maps.push(d.at.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect::<HashMap<_, _>>());
    }
    // half the patterns on message text, the rest spread over the fields
    let mut seen: std::collections::HashSet<(usize, Vec<u8>, bool)> = Default::default();
    let mut k = 0;
    let mut tries = 0;
    while k < n_patterns && tries < n_patterns * 50 {
        tries += 1;
        let di = if k % 2 == 0 || dumps.len() == 1 { 0 } else { 1 + (k / 2) % (dumps.len() - 1) };
        let cs = (k / 2) % 2 == 0;
        // a field with nothing to draw from gives its turn to the message text
        let (di, drawn) = match if dumps[di].lines.is_empty() { None } else { draw(&dumps[di], &mut rng, !cs) } {
            Some(p) => (di, Some(p)),
            None if !dumps[0].lines.is_empty() => (0, draw(&dumps[0], &mut rng, !cs)),
            None => (di, None),
        };
        let d = &dumps[di];
        let Some(mut pat) = drawn else {
            k += 1;
            continue;
        };
        if !cs {
            for c in pat.iter_mut() {
                if rng.below(2) == 0 {
                    *c = if c.is_ascii_lowercase() { c.to_ascii_uppercase() } else { c.to_ascii_lowercase() };
                }
            }
        }
        if !seen.insert((di, pat.clone(), cs)) {
            continue;
        }
        let a = engine(db, d, &maps[di], &pat, cs)?;
        let b = grep(d, &pat, cs)?;
        rep.patterns += 1;
        rep.exact += cs as usize;
        rep.hits += b.len() as u64;
        if a != b {
            rep.differ += 1;
            if rep.first.len() < 10 {
                rep.first.push((rep.patterns, d.name.clone(), a.len(), b.len()));
            }
        }
        k += 1;
    }
    // regular expressions: half as many, spread the same way
    let mut seen_rx: std::collections::HashSet<(usize, Vec<u8>)> = Default::default();
    let (mut k, mut tries) = (0, 0);
    while k < n_patterns / 2 && tries < n_patterns * 25 {
        tries += 1;
        let di = if k % 2 == 0 || dumps.len() == 1 { 0 } else { 1 + (k / 2) % (dumps.len() - 1) };
        let (di, drawn) = match if dumps[di].lines.is_empty() { None } else { draw_regex(&dumps[di], &mut rng, k / 2) } {
            Some(p) => (di, Some(p)),
            None if !dumps[0].lines.is_empty() => (0, draw_regex(&dumps[0], &mut rng, k / 2)),
            None => (di, None),
        };
        let d = &dumps[di];
        let Some(rx) = drawn else {
            k += 1;
            continue;
        };
        if !seen_rx.insert((di, rx.clone())) {
            continue;
        }
        // the engine's `.` stops at a line break unless told otherwise; a unit is one line here
        let mut eng = b"(?s)".to_vec();
        eng.extend_from_slice(&rx);
        let a = engine_op(db, d, &maps[di], StrOp::Regex, &eng, true)?;
        let b = grep_how(d, &rx, &["-E"])?;
        rep.regexes += 1;
        rep.regex_hits += b.len() as u64;
        if a != b {
            rep.regex_differ += 1;
            if rep.regex_first.len() < 10 {
                rep.regex_first.push((rep.regexes, format!("{} (kind {})", d.name, (k / 2) % 4), a.len(), b.len()));
            }
        }
        k += 1;
    }
    index_check(db, &dumps[0], n_patterns, &mut rng, &mut rep)?;
    std::fs::remove_dir_all(work).map_err(|e| e.to_string())?;
    Ok(rep)
}

fn text_test(op: StrOp, pat: &[u8], cs: bool, neg: bool) -> Expr {
    Expr::Str { op, e: Box::new(Expr::Ref(vec!["text".into()])), pat: Box::new(Expr::Lit(Value::Str(String::from_utf8_lossy(pat).as_ref().into()))), neg, cs }
}

fn role_is(r: &str) -> Expr {
    Expr::Cmp(crate::query::parse::CmpOp::Eq, Box::new(Expr::Ref(vec!["role".into()])), Box::new(Expr::Lit(Value::Str(r.into()))))
}

/// A whole word (a run of letters and digits) of a random line, or several in a row.
fn draw_words(d: &Dump, rng: &mut Rng, n_words: usize) -> Option<Vec<u8>> {
    for _ in 0..200 {
        let l = &d.lines[rng.below(d.lines.len())];
        let Ok(t) = std::str::from_utf8(l) else { continue };
        let mut runs: Vec<(usize, usize)> = Vec::new();
        crate::textindex::runs(t, |a, r| runs.push((a, a + r.len())));
        if runs.len() < n_words {
            continue;
        }
        let k = rng.below(runs.len() - n_words + 1);
        let p = &l[runs[k].0..runs[k + n_words - 1].1];
        if p.len() >= 2 && p.len() <= 80 && !p.iter().any(|&c| c < 0x20 || c == 0x7f) {
            return Some(p.to_vec());
        }
    }
    None
}

/// Random filters over message text, each run with the word index and by reading every
/// text: the rows must be the same.
fn index_check(db: &Db, d: &Dump, n: usize, rng: &mut Rng, rep: &mut Report) -> R<()> {
    use crate::query::messages::set_index;
    if d.lines.is_empty() {
        return Ok(());
    }
    for k in 0..n {
        let kind = k % 12;
        let flip = |p: &mut Vec<u8>, rng: &mut Rng| {
            for c in p.iter_mut() {
                if rng.below(3) == 0 {
                    *c = if c.is_ascii_lowercase() { c.to_ascii_uppercase() } else { c.to_ascii_lowercase() };
                }
            }
        };
        let e = match kind {
            0 => draw_words(d, rng, 1).map(|mut w| {
                flip(&mut w, rng);
                text_test(StrOp::Has, &w, false, false)
            }),
            1 => draw(d, rng, false).map(|p| text_test(StrOp::Contains, &p, false, false)),
            2 => draw_words(d, rng, 1).map(|w| Expr::And(vec![role_is("user"), text_test(StrOp::Has, &w, false, false)])),
            3 => match (draw(d, rng, false), draw_words(d, rng, 2)) {
                (Some(a), Some(b)) => Some(Expr::Or(vec![text_test(StrOp::Contains, &a, false, false), text_test(StrOp::Contains, &b, false, false)])),
                _ => None,
            },
            4 => draw_words(d, rng, 1).map(|w| Expr::And(vec![text_test(StrOp::Has, &w, false, true), role_is("assistant")])),
            5 => draw_words(d, rng, 2).map(|w| text_test(StrOp::Has, &w, false, false)),
            6 => draw_words(d, rng, 1).map(|w| text_test(StrOp::Has, &w, true, false)),
            7 => draw(d, rng, true).map(|mut p| {
                flip(&mut p, rng);
                text_test(StrOp::Contains, &p, true, false)
            }),
            10 | 11 => draw_regex(d, rng, k).map(|rx| {
                let mut eng = if kind == 10 { b"(?s)".to_vec() } else { b"(?is)".to_vec() };
                eng.extend_from_slice(&rx);
                text_test(StrOp::Regex, &eng, true, false)
            }),
            8 => {
                // the start or the end of a text
                let l = &d.lines[rng.below(d.lines.len())];
                let want = 3 + rng.below(20);
                let mut cut = want.min(l.len());
                while cut < l.len() && (l[cut] & 0xC0) == 0x80 {
                    cut += 1;
                }
                let p = &l[..cut];
                if p.len() >= 3 && !p.iter().any(|&c| c < 0x20 || c == 0x7f) && std::str::from_utf8(p).is_ok() { Some(text_test(StrOp::StartsWith, p, false, false)) } else { None }
            }
            _ => draw_words(d, rng, 3).map(|w| Expr::Not(Box::new(Expr::Or(vec![text_test(StrOp::Contains, &w, false, false), role_is("user")])))),
        };
        let Some(e) = e else { continue };
        let q = Query { source: Source::Messages, stages: vec![Stage::Where(e)] };
        let keys = |on: bool| -> R<Vec<(u32, u32, u32)>> {
            set_index(on);
            let r = exec::run(db, &q, 1).map_err(|e| e.to_string());
            set_index(true);
            Ok(r?.table.rows.iter().map(|r| (r.session, r.steps[0], r.steps[1])).collect())
        };
        let (a, b) = (keys(true)?, keys(false)?);
        rep.index_filters += 1;
        rep.index_rows += b.len() as u64;
        // grouping the rows directly gives what grouping them through the general path does
        let grouped = |general: bool| -> R<Vec<Vec<String>>> {
            use crate::query::parse::AggFn;
            let col = |c: &str| Expr::Ref(vec![c.to_string()]);
            let by = match k % 3 {
                0 => vec![("session".to_string(), col("session"))],
                1 => vec![("role".to_string(), col("role")), ("agent".to_string(), col("agent"))],
                _ => Vec::new(),
            };
            let mut stages = q.stages.clone();
            if general {
                stages.push(Stage::Extend(vec![("one".into(), Expr::Lit(Value::Int(1)))]));
            }
            stages.push(Stage::Summarize { aggs: vec![("n".into(), AggFn::Count, None), ("s".into(), AggFn::DCount, Some(col("session")))], by });
            let r = exec::run(db, &Query { source: Source::Messages, stages }, 1).map_err(|e| e.to_string())?;
            Ok(r.table.rows.iter().map(|r| r.vals.iter().map(|v| v.to_string()).collect()).collect())
        };
        let same_groups = grouped(false)? == grouped(true)?;
        if a != b || !same_groups {
            rep.index_differ += 1;
            if rep.index_first.len() < 10 {
                rep.index_first.push((rep.index_filters, kind, a.len(), b.len()));
            }
        }
    }
    Ok(())
}
