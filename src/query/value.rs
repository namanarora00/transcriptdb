//! Values and the expression evaluator used by every stage (and by the slow reference).

use super::parse::{ArOp, CmpOp, Expr, StrOp};
use super::QError;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc as Rc;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Rc<str>),
    /// Milliseconds since the Unix epoch.
    Time(i64),
    /// Milliseconds.
    Dur(i64),
}

impl PartialEq for Value {
    fn eq(&self, o: &Value) -> bool {
        self.key() == o.key()
    }
}

/// A hashable, totally ordered form of a value, for grouping and sorting.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Null,
    Bool(bool),
    Num(i64, u64),
    Str(Rc<str>),
    Time(i64),
    Dur(i64),
}

impl Value {
    pub fn key(&self) -> Key {
        match self {
            Value::Null => Key::Null,
            Value::Bool(b) => Key::Bool(*b),
            Value::Int(i) => Key::Num(*i, 0),
            Value::Float(f) => {
                if f.fract() == 0.0 && f.abs() < 9e15 {
                    Key::Num(*f as i64, 0)
                } else {
                    Key::Num(f.floor() as i64, ((f - f.floor()) * 1e15) as u64)
                }
            }
            Value::Str(s) => Key::Str(s.clone()),
            Value::Time(t) => Key::Time(*t),
            Value::Dur(d) => Key::Dur(*d),
        }
    }

    pub fn truthy(&self) -> bool {
        matches!(self, Value::Bool(true))
    }

    fn num(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) | Value::Float(_) => "number",
            Value::Str(_) => "string",
            Value::Time(_) => "time",
            Value::Dur(_) => "duration",
        }
    }

    /// A JSON value as text (from an input field) to a value: strings decoded, numbers parsed,
    /// anything else kept as its text.
    pub fn from_json(t: &[u8]) -> Value {
        match t.first() {
            None => Value::Null,
            Some(b'"') => {
                let inner = &t[1..t.len().saturating_sub(1)];
                match std::str::from_utf8(inner) {
                    Ok(plain) if !inner.contains(&b'\\') => Value::Str(plain.into()),
                    _ => Value::Str(json_unescape(inner).into()),
                }
            }
            Some(b't') if t == b"true" => Value::Bool(true),
            Some(b'f') if t == b"false" => Value::Bool(false),
            Some(b'n') if t == b"null" => Value::Null,
            Some(c) if c.is_ascii_digit() || *c == b'-' => {
                let s = String::from_utf8_lossy(t);
                if let Ok(i) = s.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::Str(s.into())
                }
            }
            _ => Value::Str(String::from_utf8_lossy(t).into()),
        }
    }

    /// Compare for sorting: nulls first, then by kind.
    pub fn order(&self, o: &Value) -> Ordering {
        match (self.num(), o.num()) {
            (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => self.key().cmp(&o.key()),
        }
    }
}

pub fn fmt_time(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil from days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z", rem / 3600, rem % 3600 / 60, rem % 60, ms.rem_euclid(1000))
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, ""),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x:.3}"),
            Value::Str(s) => write!(f, "{s}"),
            Value::Time(t) => write!(f, "{}", fmt_time(*t)),
            Value::Dur(d) => {
                let a = d.abs();
                if a < 1000 {
                    write!(f, "{d}ms")
                } else if a < 60_000 {
                    write!(f, "{:.1}s", *d as f64 / 1e3)
                } else if a < 3_600_000 {
                    write!(f, "{:.1}m", *d as f64 / 60e3)
                } else if a < 86_400_000 {
                    write!(f, "{:.1}h", *d as f64 / 3.6e6)
                } else {
                    write!(f, "{:.1}d", *d as f64 / 8.64e7)
                }
            }
        }
    }
}

pub fn json_unescape(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    let mut i = 0;
    let mut pending: Option<u16> = None; // a high surrogate waiting for its pair
    let flush_pending = |pending: &mut Option<u16>, out: &mut String| {
        if pending.take().is_some() {
            out.push('\u{fffd}');
        }
    };
    while i < b.len() {
        let next = memchr::memchr(b'\\', &b[i..]).map(|k| i + k).unwrap_or(b.len());
        if next > i {
            flush_pending(&mut pending, &mut out);
            out.push_str(&String::from_utf8_lossy(&b[i..next]));
        }
        if next + 1 >= b.len() {
            if next < b.len() {
                out.push('\\');
            }
            break;
        }
        let e = b[next + 1];
        i = next + 2;
        if e == b'u' && i + 4 <= b.len() {
            let u = std::str::from_utf8(&b[i..i + 4]).ok().and_then(|h| u16::from_str_radix(h, 16).ok()).unwrap_or(0xfffd);
            i += 4;
            match (pending.take(), u) {
                (Some(hi), 0xdc00..=0xdfff) => {
                    let c = 0x10000 + (((hi as u32) - 0xd800) << 10) + (u as u32 - 0xdc00);
                    out.push(char::from_u32(c).unwrap_or('\u{fffd}'));
                }
                (prev, 0xd800..=0xdbff) => {
                    if prev.is_some() {
                        out.push('\u{fffd}');
                    }
                    pending = Some(u);
                }
                (prev, _) => {
                    if prev.is_some() {
                        out.push('\u{fffd}');
                    }
                    out.push(char::from_u32(u as u32).unwrap_or('\u{fffd}'));
                }
            }
            continue;
        }
        flush_pending(&mut pending, &mut out);
        out.push(match e {
            b'n' => '\n',
            b't' => '\t',
            b'r' => '\r',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            other => other as char,
        });
    }
    flush_pending(&mut pending, &mut out);
    out
}

/// Glob match (`*`, `?`), on chars.
pub fn glob(pat: &[char], s: &[char]) -> bool {
    let (mut p, mut t) = (0, 0);
    let (mut star, mut mark) = (usize::MAX, 0);
    while t < s.len() {
        if p < pat.len() && (pat[p] == '?' || pat[p] == s[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == '*' {
            star = p;
            mark = t;
            p += 1;
        } else if star != usize::MAX {
            p = star + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == '*' {
        p += 1;
    }
    p == pat.len()
}

/// Kusto's `has`: the term appears as a whole word (alphanumeric runs).
fn has_term(s: &str, term: &str) -> bool {
    if term.is_empty() {
        return true;
    }
    let mut start = 0;
    while let Some(i) = s[start..].find(term) {
        let a = start + i;
        let b = a + term.len();
        let before = s[..a].chars().next_back().is_none_or(|c| !c.is_alphanumeric());
        let after = s[b..].chars().next().is_none_or(|c| !c.is_alphanumeric());
        if before && after {
            return true;
        }
        start = a + term.chars().next().map(|c| c.len_utf8()).unwrap_or(1);
    }
    false
}

/// Case-insensitive ASCII substring search without allocating: find candidates by the
/// first byte (either case), then compare the rest in place.
fn ascii_ci_contains(h: &[u8], n: &[u8]) -> bool {
    let Some(&first) = n.first() else { return true };
    if h.len() < n.len() {
        return false;
    }
    let (lo, up) = (first.to_ascii_lowercase(), first.to_ascii_uppercase());
    let last_start = h.len() - n.len();
    let mut i = 0;
    while i <= last_start {
        match memchr::memchr2(lo, up, &h[i..=last_start]) {
            None => return false,
            Some(k) => {
                let at = i + k;
                if h[at..at + n.len()].eq_ignore_ascii_case(n) {
                    return true;
                }
                i = at + 1;
            }
        }
    }
    false
}

pub fn str_op(op: StrOp, s: &str, pat: &str, cs: bool) -> bool {
    if !cs && s.is_ascii() && pat.is_ascii() {
        let (h, n) = (s.as_bytes(), pat.as_bytes());
        match op {
            StrOp::Contains => return ascii_ci_contains(h, n),
            StrOp::StartsWith => return h.len() >= n.len() && h[..n.len()].eq_ignore_ascii_case(n),
            StrOp::EndsWith => return h.len() >= n.len() && h[h.len() - n.len()..].eq_ignore_ascii_case(n),
            _ => {}
        }
    }
    let (s, pat) = if cs { (s.to_string(), pat.to_string()) } else { (s.to_lowercase(), pat.to_lowercase()) };
    match op {
        StrOp::Has => has_term(&s, &pat),
        StrOp::Contains => s.contains(&pat),
        StrOp::StartsWith => s.starts_with(&pat),
        StrOp::EndsWith => s.ends_with(&pat),
        StrOp::Matches => {
            let p: Vec<char> = pat.chars().collect();
            let t: Vec<char> = s.chars().collect();
            glob(&p, &t)
        }
    }
}

/// Where column values come from.
pub trait Ctx {
    fn col(&self, path: &[String]) -> Result<Value, QError>;
    fn now(&self) -> i64;
}

pub fn cmp(op: CmpOp, a: &Value, b: &Value) -> bool {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return match op {
            CmpOp::Eq => matches!((a, b), (Value::Null, Value::Null)),
            CmpOp::Ne => !matches!((a, b), (Value::Null, Value::Null)),
            _ => false,
        };
    }
    let ord = match (a, b) {
        (Value::Str(x), Value::Str(y)) => Some(x.cmp(y)),
        (Value::Time(x), Value::Time(y)) | (Value::Dur(x), Value::Dur(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => match (a.num(), b.num()) {
            (Some(x), Some(y)) => x.partial_cmp(&y),
            _ => None,
        },
    };
    match (op, ord) {
        (CmpOp::Eq, o) => o == Some(Ordering::Equal),
        (CmpOp::Ne, o) => o != Some(Ordering::Equal),
        (_, None) => false,
        (CmpOp::Lt, Some(o)) => o == Ordering::Less,
        (CmpOp::Le, Some(o)) => o != Ordering::Greater,
        (CmpOp::Gt, Some(o)) => o == Ordering::Greater,
        (CmpOp::Ge, Some(o)) => o != Ordering::Less,
    }
}

fn arith(op: ArOp, a: Value, b: Value) -> Result<Value, QError> {
    use Value::*;
    Ok(match (op, a, b) {
        (_, Null, _) | (_, _, Null) => Null,
        (ArOp::Add, Time(t), Dur(d)) | (ArOp::Add, Dur(d), Time(t)) => Time(t + d),
        (ArOp::Sub, Time(t), Dur(d)) => Time(t - d),
        (ArOp::Sub, Time(a), Time(b)) => Dur(a - b),
        (ArOp::Add, Dur(a), Dur(b)) => Dur(a + b),
        (ArOp::Sub, Dur(a), Dur(b)) => Dur(a - b),
        (ArOp::Div, Dur(a), Dur(b)) if b != 0 => Float(a as f64 / b as f64),
        (ArOp::Mul, Dur(a), Int(b)) | (ArOp::Mul, Int(b), Dur(a)) => Dur(a * b),
        (ArOp::Div, Dur(a), Int(b)) if b != 0 => Dur(a / b),
        (ArOp::Add, Str(a), Str(b)) => Str(format!("{a}{b}").into()),
        (op, Int(a), Int(b)) => match op {
            ArOp::Add => Int(a.wrapping_add(b)),
            ArOp::Sub => Int(a.wrapping_sub(b)),
            ArOp::Mul => Int(a.wrapping_mul(b)),
            ArOp::Div => {
                if b == 0 {
                    Null
                } else {
                    Float(a as f64 / b as f64)
                }
            }
        },
        (op, a, b) => match (a.num(), b.num()) {
            (Some(x), Some(y)) => Float(match op {
                ArOp::Add => x + y,
                ArOp::Sub => x - y,
                ArOp::Mul => x * y,
                ArOp::Div => {
                    if y == 0.0 {
                        return Ok(Null);
                    }
                    x / y
                }
            }),
            _ => return Err(QError::new(format!("cannot apply {op:?} to {} and {}", a.type_name(), b.type_name()))),
        },
    })
}

pub fn eval(e: &Expr, ctx: &dyn Ctx) -> Result<Value, QError> {
    Ok(match e {
        Expr::Lit(v) => v.clone(),
        Expr::Ref(p) => ctx.col(p)?,
        Expr::Not(x) => Value::Bool(!eval(x, ctx)?.truthy()),
        Expr::And(v) => {
            for x in v {
                if !eval(x, ctx)?.truthy() {
                    return Ok(Value::Bool(false));
                }
            }
            Value::Bool(true)
        }
        Expr::Or(v) => {
            for x in v {
                if eval(x, ctx)?.truthy() {
                    return Ok(Value::Bool(true));
                }
            }
            Value::Bool(false)
        }
        Expr::Cmp(op, a, b) => Value::Bool(cmp(*op, &eval(a, ctx)?, &eval(b, ctx)?)),
        Expr::In { e, list, neg } => {
            let v = eval(e, ctx)?;
            let mut hit = false;
            for x in list {
                if cmp(CmpOp::Eq, &v, &eval(x, ctx)?) {
                    hit = true;
                    break;
                }
            }
            Value::Bool(hit != *neg)
        }
        Expr::Str { op, e, pat, neg, cs } => {
            let v = eval(e, ctx)?;
            let p = eval(pat, ctx)?;
            let hit = match (v.as_str(), p.as_str()) {
                (Some(s), Some(pt)) => str_op(*op, s, pt, *cs),
                (None, Some(pt)) if !matches!(v, Value::Null) => str_op(*op, &v.to_string(), pt, *cs),
                _ => false,
            };
            Value::Bool(hit != *neg)
        }
        Expr::Ar(op, a, b) => arith(*op, eval(a, ctx)?, eval(b, ctx)?)?,
        Expr::Call(f, args) => {
            let a: Vec<Value> = args.iter().map(|x| eval(x, ctx)).collect::<Result<_, _>>()?;
            match (f.as_str(), a.as_slice()) {
                ("now", []) => Value::Time(ctx.now()),
                ("ago", [Value::Dur(d)]) => Value::Time(ctx.now() - d),
                ("bin" | "floor", [Value::Time(t), Value::Dur(d)]) if *d > 0 => Value::Time(t.div_euclid(*d) * d),
                ("bin" | "floor", [Value::Dur(t), Value::Dur(d)]) if *d > 0 => Value::Dur(t.div_euclid(*d) * d),
                ("bin" | "floor", [Value::Int(x), Value::Int(d)]) if *d > 0 => Value::Int(x.div_euclid(*d) * d),
                ("bin" | "floor", [x, y]) if x.num().is_some() && y.num().is_some_and(|d| d > 0.0) => {
                    let d = y.num().unwrap();
                    Value::Float((x.num().unwrap() / d).floor() * d)
                }
                ("bin" | "floor", [Value::Null, _]) => Value::Null,
                ("strlen", [Value::Str(s)]) => Value::Int(s.chars().count() as i64),
                ("strlen", [Value::Null]) => Value::Null,
                ("tolower", [Value::Str(s)]) => Value::Str(s.to_lowercase().into()),
                ("toupper", [Value::Str(s)]) => Value::Str(s.to_uppercase().into()),
                ("isnull", [v]) => Value::Bool(matches!(v, Value::Null)),
                ("isnotnull", [v]) => Value::Bool(!matches!(v, Value::Null)),
                ("tostring", [v]) => Value::Str(v.to_string().into()),
                ("iff" | "iif", [c, x, y]) => {
                    if c.truthy() {
                        x.clone()
                    } else {
                        y.clone()
                    }
                }
                _ => return Err(QError::new(format!("unknown function {f}() for ({})", a.iter().map(|v| v.type_name()).collect::<Vec<_>>().join(", ")))),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs_and_terms() {
        let g = |p: &str, s: &str| glob(&p.chars().collect::<Vec<_>>(), &s.chars().collect::<Vec<_>>());
        assert!(g("*test*", "src/tests/a.rs"));
        assert!(g("*.rs", "a.rs"));
        assert!(!g("*.rs", "a.rsx"));
        assert!(g("a?c", "abc"));
        assert!(has_term("cargo test --all", "test"));
        assert!(!has_term("cargo tests", "test"));
        assert!(str_op(StrOp::Contains, "Error: X", "error", false));
        assert!(str_op(StrOp::Contains, "run CARGO Test now", "cargo test", false));
        assert!(!str_op(StrOp::Contains, "cargo tes", "cargo test", false));
        assert!(str_op(StrOp::Contains, "x", "", false));
        assert!(!str_op(StrOp::Contains, "Error: X", "error", true));
    }

    #[test]
    fn json_values() {
        assert_eq!(Value::from_json(br#""a\nb\u00e9""#).as_str().unwrap(), "a\nb\u{e9}");
        assert!(matches!(Value::from_json(b"42"), Value::Int(42)));
        assert_eq!(json_unescape(br#"a\"b\\c\ud83d\ude80d\ud83de"#), "a\"b\\c\u{1f680}d\u{fffd}e");
        assert_eq!(json_unescape(br#"\u00e9t\u00e9"#), "\u{e9}t\u{e9}");
        assert!(matches!(Value::from_json(b"1.5e-3"), Value::Float(_)));
        assert_eq!(fmt_time(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(fmt_time(951_868_801_500), "2000-03-01T00:00:01.500Z");
    }
}
