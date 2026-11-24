//! What a regular expression needs a text to contain, for looking candidates up in the
//! word index: a tree of literals (lower-cased) that every matching text holds, ignoring
//! case. Conservative: whatever cannot be reduced to a literal asks for nothing.

use regex_syntax::hir::{Class, Hir, HirKind};

/// What a matching text must contain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Req {
    /// Nothing can be said.
    Any,
    /// This text, ignoring case (ASCII, lower-cased, not empty).
    Lit(String),
    And(Vec<Req>),
    Or(Vec<Req>),
}

/// The most texts a part of a pattern is spelled out into.
const MAX_SET: usize = 16;
const MAX_LEN: usize = 256;

/// The lower-case ASCII characters the members of a small class lower-case to (what
/// `(?i)k` or `[ab]` becomes), if every member has one.
fn class_chars(c: &Class) -> Option<Vec<char>> {
    let mut members: Vec<char> = Vec::new();
    match c {
        Class::Unicode(u) => {
            for r in u.ranges() {
                if r.end() as u32 - r.start() as u32 >= 4 {
                    return None;
                }
                members.extend(r.start()..=r.end());
            }
        }
        Class::Bytes(b) => {
            for r in b.ranges() {
                if r.end() - r.start() >= 4 || r.end() >= 0x80 {
                    return None;
                }
                members.extend((r.start()..=r.end()).map(|x| x as char));
            }
        }
    }
    if members.is_empty() || members.len() > 6 {
        return None;
    }
    let mut out: Vec<char> = Vec::new();
    for m in members {
        let mut l = m.to_lowercase();
        match (l.next(), l.next()) {
            (Some(x), None) if x.is_ascii() => {
                if !out.contains(&x) {
                    out.push(x);
                }
            }
            _ => return None,
        }
    }
    (out.len() <= 4).then_some(out)
}

/// Every text `h` can match, ignoring case and lower-cased, when those are few; an empty
/// string for what matches without taking any text (anchors, word boundaries).
fn set(h: &Hir) -> Option<Vec<String>> {
    let out = match h.kind() {
        HirKind::Empty | HirKind::Look(_) => vec![String::new()],
        // non-ASCII text is left out: lower-casing it can depend on what follows
        HirKind::Literal(l) => {
            if !l.0.is_ascii() {
                return None;
            }
            vec![String::from_utf8_lossy(&l.0).to_ascii_lowercase()]
        }
        HirKind::Class(c) => class_chars(c)?.into_iter().map(|c| c.to_string()).collect(),
        HirKind::Capture(c) => set(&c.sub)?,
        HirKind::Concat(v) => {
            let mut acc = vec![String::new()];
            for x in v {
                let next = set(x)?;
                if acc.len() * next.len() > MAX_SET {
                    return None;
                }
                acc = acc.iter().flat_map(|a| next.iter().map(move |n| format!("{a}{n}"))).collect();
            }
            acc
        }
        HirKind::Alternation(v) => {
            let mut acc: Vec<String> = Vec::new();
            for x in v {
                for t in set(x)? {
                    if !acc.contains(&t) {
                        acc.push(t);
                    }
                }
            }
            acc
        }
        HirKind::Repetition(r) if r.min == r.max.unwrap_or(u32::MAX) && r.min <= 8 => {
            let one = set(&r.sub)?;
            if one.len() != 1 {
                return None;
            }
            vec![one[0].repeat(r.min as usize)]
        }
        _ => return None,
    };
    (out.len() <= MAX_SET && out.iter().all(|t| t.len() <= MAX_LEN)).then_some(out)
}

fn lits(v: Vec<String>) -> Req {
    // a text that may be empty asks for nothing
    if v.iter().any(|t| t.is_empty()) {
        return Req::Any;
    }
    Req::Or(v.into_iter().map(Req::Lit).collect())
}

fn simplify(r: Req) -> Req {
    match r {
        Req::And(v) => {
            let mut out: Vec<Req> = Vec::new();
            for x in v.into_iter().map(simplify) {
                match x {
                    Req::Any => {}
                    Req::And(inner) => out.extend(inner),
                    x => out.push(x),
                }
            }
            match out.len() {
                0 => Req::Any,
                1 => out.pop().unwrap(),
                _ => Req::And(out),
            }
        }
        Req::Or(v) => {
            let mut out: Vec<Req> = Vec::new();
            for x in v.into_iter().map(simplify) {
                match x {
                    Req::Any => return Req::Any,
                    Req::Or(inner) => out.extend(inner),
                    x => out.push(x),
                }
            }
            match out.len() {
                0 => Req::Any,
                1 => out.pop().unwrap(),
                _ => Req::Or(out),
            }
        }
        Req::Lit(s) if s.is_empty() => Req::Any,
        x => x,
    }
}

fn req(h: &Hir) -> Req {
    if let Some(v) = set(h) {
        return lits(v);
    }
    match h.kind() {
        HirKind::Concat(v) => {
            // runs of parts with few texts join into whole literals
            let mut out: Vec<Req> = Vec::new();
            let mut run: Vec<String> = vec![String::new()];
            let flush = |run: &mut Vec<String>, out: &mut Vec<Req>| {
                let r = std::mem::replace(run, vec![String::new()]);
                if r != [String::new()] {
                    out.push(lits(r));
                }
            };
            for x in v {
                match set(x) {
                    Some(next) if run.len() * next.len() <= MAX_SET => {
                        run = run.iter().flat_map(|a| next.iter().map(move |n| format!("{a}{n}"))).collect();
                    }
                    Some(next) => {
                        flush(&mut run, &mut out);
                        run = next;
                    }
                    None => {
                        flush(&mut run, &mut out);
                        out.push(req(x));
                    }
                }
            }
            flush(&mut run, &mut out);
            Req::And(out)
        }
        HirKind::Alternation(v) => Req::Or(v.iter().map(req).collect()),
        HirKind::Repetition(r) if r.min >= 1 => req(&r.sub),
        HirKind::Capture(c) => req(&c.sub),
        _ => Req::Any,
    }
}

/// What every text matching `pattern` contains. `Any` for a pattern that does not parse
/// (the caller reports that).
pub fn required(pattern: &str) -> Req {
    match regex_syntax::parse(pattern) {
        Ok(h) => simplify(req(&h)),
        Err(_) => Req::Any,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(s: &str) -> Req {
        Req::Lit(s.into())
    }

    #[test]
    fn literals_and_structure() {
        assert_eq!(required("root cause"), lit("root cause"));
        assert_eq!(required("Root Cause"), lit("root cause"));
        // `(?i)s` also matches the long s, which does not lower-case to `s`: the text is cut there
        assert_eq!(required("(?i)Root Cause"), Req::And(vec![lit("root cau"), lit("e")]));
        assert_eq!(required("(?i)Timeout Error"), lit("timeout error"));
        assert_eq!(required("^error: "), lit("error: "));
        assert_eq!(required(r"error\s+E\d+"), Req::And(vec![lit("error"), lit("e")]));
        assert_eq!(required("git (push|pull) origin"), Req::Or(vec![lit("git push origin"), lit("git pull origin")]));
        assert_eq!(required("gr[ae]y box"), Req::Or(vec![lit("gray box"), lit("grey box")]));
        assert_eq!(required("(still failing|try again)"), Req::Or(vec![lit("still failing"), lit("try again")]));
        assert_eq!(required("foo.*bar"), Req::And(vec![lit("foo"), lit("bar")]));
        assert_eq!(required("(foo)+bar"), Req::And(vec![lit("foo"), lit("bar")]));
        assert_eq!(required("ab{3}c"), lit("abbbc"));
    }

    #[test]
    fn nothing_when_unsure() {
        assert_eq!(required(".*"), Req::Any);
        assert_eq!(required("(foo)?"), Req::Any);
        assert_eq!(required("foo|.*"), Req::Any);
        assert_eq!(required("[a-z]+"), Req::Any);
        assert_eq!(required(r"\d{4}-\d{2}"), lit("-"));
        // non-ASCII text asks for nothing; so does a letter whose other case is not ASCII
        assert_eq!(required("naïve"), Req::Any);
        assert_eq!(required("(?i)s"), Req::Any);
        assert_eq!(required("(?i)k"), lit("k"));
        assert_eq!(required("(unclosed"), Req::Any);
    }
}
