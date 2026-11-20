//! A small JSON scanner that works on byte spans of the original input.
//!
//! Nothing is re-encoded: strings and numbers are copied exactly as they appear,
//! so a value written back out has the same bytes it came in with. The only
//! transformation offered is [`write_canonical`], which re-orders object keys
//! (sorted by UTF-16 code units, like JavaScript's default sort) and drops
//! whitespace, while still copying every string and number token verbatim.

use std::cmp::Ordering;
use std::ops::Range;

pub type Span = Range<usize>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonError {
    pub at: usize,
    pub what: &'static str,
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad JSON at byte {}: {}", self.at, self.what)
    }
}

impl std::error::Error for JsonError {}

pub type JResult<T> = Result<T, JsonError>;

fn err<T>(at: usize, what: &'static str) -> JResult<T> {
    Err(JsonError { at, what })
}

const MAX_DEPTH: usize = 2000;

#[inline]
pub fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// `b[i]` must be the opening quote. Returns the index just past the closing quote.
#[inline]
pub fn scan_string(b: &[u8], i: usize) -> JResult<usize> {
    debug_assert_eq!(b[i], b'"');
    let mut j = i + 1;
    loop {
        match memchr::memchr2(b'"', b'\\', &b[j..]) {
            None => return err(i, "unterminated string"),
            Some(k) => {
                let p = j + k;
                if b[p] == b'"' {
                    return Ok(p + 1);
                }
                // backslash: skip it and the escaped byte
                if p + 1 >= b.len() {
                    return err(p, "unterminated escape");
                }
                j = p + 2;
            }
        }
    }
}

#[inline]
fn scan_scalar(b: &[u8], i: usize) -> JResult<usize> {
    let mut j = i;
    while j < b.len() && matches!(b[j], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' | b'a'..=b'z') {
        j += 1;
    }
    if j == i {
        return err(i, "unexpected byte");
    }
    Ok(j)
}

/// Skip one value starting at `i` (no leading whitespace). Returns its end.
pub fn scan_value(b: &[u8], i: usize) -> JResult<usize> {
    scan_value_d(b, i, 0)
}

fn scan_value_d(b: &[u8], i: usize, depth: usize) -> JResult<usize> {
    if depth > MAX_DEPTH {
        return err(i, "too deep");
    }
    match b.get(i) {
        None => err(i, "unexpected end"),
        Some(b'"') => scan_string(b, i),
        Some(b'{') => {
            let mut j = skip_ws(b, i + 1);
            if b.get(j) == Some(&b'}') {
                return Ok(j + 1);
            }
            loop {
                if b.get(j) != Some(&b'"') {
                    return err(j, "expected key");
                }
                j = skip_ws(b, scan_string(b, j)?);
                if b.get(j) != Some(&b':') {
                    return err(j, "expected colon");
                }
                j = skip_ws(b, j + 1);
                j = skip_ws(b, scan_value_d(b, j, depth + 1)?);
                match b.get(j) {
                    Some(b',') => j = skip_ws(b, j + 1),
                    Some(b'}') => return Ok(j + 1),
                    _ => return err(j, "expected , or }"),
                }
            }
        }
        Some(b'[') => {
            let mut j = skip_ws(b, i + 1);
            if b.get(j) == Some(&b']') {
                return Ok(j + 1);
            }
            loop {
                j = skip_ws(b, scan_value_d(b, j, depth + 1)?);
                match b.get(j) {
                    Some(b',') => j = skip_ws(b, j + 1),
                    Some(b']') => return Ok(j + 1),
                    _ => return err(j, "expected , or ]"),
                }
            }
        }
        Some(_) => scan_scalar(b, i),
    }
}

/// One member of an object: the key token (with quotes) and the value span.
#[derive(Debug, Clone)]
pub struct Member {
    pub key: Span,
    pub value: Span,
}

/// Members of the object whose `{` is at `span.start`.
pub fn members(b: &[u8], span: &Span) -> JResult<Vec<Member>> {
    let i = span.start;
    if b.get(i) != Some(&b'{') {
        return err(i, "expected object");
    }
    let mut out = Vec::new();
    let mut j = skip_ws(b, i + 1);
    if b.get(j) == Some(&b'}') {
        return Ok(out);
    }
    loop {
        if b.get(j) != Some(&b'"') {
            return err(j, "expected key");
        }
        let ke = scan_string(b, j)?;
        let key = j..ke;
        j = skip_ws(b, ke);
        if b.get(j) != Some(&b':') {
            return err(j, "expected colon");
        }
        j = skip_ws(b, j + 1);
        let ve = scan_value(b, j)?;
        out.push(Member { key, value: j..ve });
        j = skip_ws(b, ve);
        match b.get(j) {
            Some(b',') => j = skip_ws(b, j + 1),
            Some(b'}') => return Ok(out),
            _ => return err(j, "expected , or }"),
        }
    }
}

/// Elements of the array whose `[` is at `span.start`, and the index of the closing `]`.
pub fn elements(b: &[u8], span: &Span) -> JResult<(Vec<Span>, usize)> {
    let i = span.start;
    if b.get(i) != Some(&b'[') {
        return err(i, "expected array");
    }
    let mut out = Vec::new();
    let mut j = skip_ws(b, i + 1);
    if b.get(j) == Some(&b']') {
        return Ok((out, j));
    }
    loop {
        let e = scan_value(b, j)?;
        out.push(j..e);
        j = skip_ws(b, e);
        match b.get(j) {
            Some(b',') => j = skip_ws(b, j + 1),
            Some(b']') => return Ok((out, j)),
            _ => return err(j, "expected , or ]"),
        }
    }
}

/// The raw bytes between the quotes of a key or string token.
#[inline]
pub fn inner<'a>(b: &'a [u8], tok: &Span) -> &'a [u8] {
    &b[tok.start + 1..tok.end - 1]
}

/// Whether the key token equals `name` (an unescaped ASCII name).
#[inline]
pub fn key_is(b: &[u8], key: &Span, name: &str) -> bool {
    inner(b, key) == name.as_bytes()
}

pub fn find_member<'m>(b: &[u8], ms: &'m [Member], name: &str) -> Option<&'m Member> {
    ms.iter().find(|m| key_is(b, &m.key, name))
}

/// A parsed value as a tree of spans into the source buffer.
#[derive(Debug, Clone)]
pub enum Node {
    Obj(Vec<(Span, Node)>),
    Arr(Vec<Node>),
    /// A string, number, `true`, `false` or `null`, copied as is.
    Tok(Span),
}

pub fn parse(b: &[u8], i: usize) -> JResult<(Node, usize)> {
    parse_d(b, i, 0)
}

fn parse_d(b: &[u8], i: usize, depth: usize) -> JResult<(Node, usize)> {
    if depth > MAX_DEPTH {
        return err(i, "too deep");
    }
    match b.get(i) {
        None => err(i, "unexpected end"),
        Some(b'"') => {
            let e = scan_string(b, i)?;
            Ok((Node::Tok(i..e), e))
        }
        Some(b'{') => {
            let mut out = Vec::new();
            let mut j = skip_ws(b, i + 1);
            if b.get(j) == Some(&b'}') {
                return Ok((Node::Obj(out), j + 1));
            }
            loop {
                if b.get(j) != Some(&b'"') {
                    return err(j, "expected key");
                }
                let ke = scan_string(b, j)?;
                let key = j..ke;
                j = skip_ws(b, ke);
                if b.get(j) != Some(&b':') {
                    return err(j, "expected colon");
                }
                j = skip_ws(b, j + 1);
                let (v, ve) = parse_d(b, j, depth + 1)?;
                out.push((key, v));
                j = skip_ws(b, ve);
                match b.get(j) {
                    Some(b',') => j = skip_ws(b, j + 1),
                    Some(b'}') => return Ok((Node::Obj(out), j + 1)),
                    _ => return err(j, "expected , or }"),
                }
            }
        }
        Some(b'[') => {
            let mut out = Vec::new();
            let mut j = skip_ws(b, i + 1);
            if b.get(j) == Some(&b']') {
                return Ok((Node::Arr(out), j + 1));
            }
            loop {
                let (v, ve) = parse_d(b, j, depth + 1)?;
                out.push(v);
                j = skip_ws(b, ve);
                match b.get(j) {
                    Some(b',') => j = skip_ws(b, j + 1),
                    Some(b']') => return Ok((Node::Arr(out), j + 1)),
                    _ => return err(j, "expected , or ]"),
                }
            }
        }
        Some(_) => {
            let e = scan_scalar(b, i)?;
            Ok((Node::Tok(i..e), e))
        }
    }
}

/// Parse a whole buffer holding exactly one value (surrounding whitespace allowed).
pub fn parse_all(b: &[u8]) -> JResult<Node> {
    let i = skip_ws(b, 0);
    let (n, e) = parse(b, i)?;
    if skip_ws(b, e) != b.len() {
        return err(e, "trailing bytes");
    }
    Ok(n)
}

/// Decode a key token to UTF-16 code units (only used for keys that are not plain ASCII).
fn key_units(raw: &[u8]) -> Vec<u16> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        let c = raw[i];
        if c == b'\\' && i + 1 < raw.len() {
            let e = raw[i + 1];
            i += 2;
            let u = match e {
                b'n' => 0x0a,
                b't' => 0x09,
                b'r' => 0x0d,
                b'b' => 0x08,
                b'f' => 0x0c,
                b'u' if i + 4 <= raw.len() => {
                    let h = std::str::from_utf8(&raw[i..i + 4]).ok().and_then(|s| u16::from_str_radix(s, 16).ok());
                    i += 4;
                    h.unwrap_or(0xfffd)
                }
                other => other as u16,
            };
            out.push(u);
        } else {
            // decode one UTF-8 sequence
            let len = match c {
                0x00..=0x7f => 1,
                0xc0..=0xdf => 2,
                0xe0..=0xef => 3,
                _ => 4,
            };
            let end = (i + len).min(raw.len());
            match std::str::from_utf8(&raw[i..end]) {
                Ok(s) => {
                    for ch in s.chars() {
                        let mut buf = [0u16; 2];
                        out.extend_from_slice(ch.encode_utf16(&mut buf));
                    }
                }
                Err(_) => out.push(0xfffd),
            }
            i = end;
        }
    }
    out
}

/// Compare two key tokens the way JavaScript's default sort compares the decoded strings.
pub fn cmp_keys(b: &[u8], x: &Span, y: &Span) -> Ordering {
    let a = inner(b, x);
    let c = inner(b, y);
    let plain = |s: &[u8]| s.iter().all(|&ch| ch < 0x80 && ch != b'\\');
    if plain(a) && plain(c) {
        a.cmp(c)
    } else {
        key_units(a).cmp(&key_units(c))
    }
}

/// Write `node` compactly with object keys sorted. Tokens are copied verbatim.
pub fn write_canonical(b: &[u8], node: &Node, out: &mut Vec<u8>) {
    match node {
        Node::Tok(s) => out.extend_from_slice(&b[s.clone()]),
        Node::Arr(items) => {
            out.push(b'[');
            for (k, it) in items.iter().enumerate() {
                if k > 0 {
                    out.push(b',');
                }
                write_canonical(b, it, out);
            }
            out.push(b']');
        }
        Node::Obj(ms) => {
            let mut idx: Vec<usize> = (0..ms.len()).collect();
            if !ms.windows(2).all(|w| cmp_keys(b, &w[0].0, &w[1].0) != Ordering::Greater) {
                idx.sort_by(|&p, &q| cmp_keys(b, &ms[p].0, &ms[q].0));
            }
            out.push(b'{');
            for (k, &p) in idx.iter().enumerate() {
                if k > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&b[ms[p].0.clone()]);
                out.push(b':');
                write_canonical(b, &ms[p].1, out);
            }
            out.push(b'}');
        }
    }
}

/// Canonical bytes of the single value in `b`.
pub fn canonical(b: &[u8]) -> JResult<Vec<u8>> {
    let n = parse_all(b)?;
    let mut out = Vec::with_capacity(b.len());
    write_canonical(b, &n, &mut out);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_and_sorts() {
        let src = br#" { "b" : [1, 2.5e3, "x\"y"], "a": {"d": null, "c": true}, "\u00e9": "z" } "#;
        let c = canonical(src).unwrap();
        assert_eq!(c, br#"{"a":{"c":true,"d":null},"b":[1,2.5e3,"x\"y"],"\u00e9":"z"}"#.to_vec());
    }

    #[test]
    fn members_and_elements() {
        let src = br#"{"messages":[{"a":1}, {"b":2}],"model":"m"}"#;
        let ms = members(src, &(0..src.len())).unwrap();
        assert_eq!(ms.len(), 2);
        let m = find_member(src, &ms, "messages").unwrap();
        let (els, close) = elements(src, &m.value).unwrap();
        assert_eq!(els.len(), 2);
        assert_eq!(&src[els[1].clone()], br#"{"b":2}"#);
        assert_eq!(src[close], b']');
    }

    #[test]
    fn lone_surrogate_escapes_survive() {
        let src = br#"{"t":"\ud800 and \\ and \u001f"}"#;
        assert_eq!(canonical(src).unwrap(), src.to_vec());
    }

    #[test]
    fn rejects_garbage() {
        assert!(canonical(b"{\"a\":}").is_err());
        assert!(canonical(b"[1,2").is_err());
        assert!(canonical(b"\"abc").is_err());
    }
}
