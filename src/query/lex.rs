//! Tokens of the query language.

use super::QError;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    /// A duration in milliseconds, such as `7d`, `3h`, `10m`, `30s`, `250ms`.
    Dur(i64),
    Pipe,
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Dot,
    Assign,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,
    End,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub tok: Tok,
    pub at: usize,
}

fn dur_unit(u: &str) -> Option<i64> {
    Some(match u {
        "ms" => 1,
        "s" => 1000,
        "m" | "min" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    })
}

pub fn lex(src: &str) -> Result<Vec<Token>, QError> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let at = i;
        let two = |x: u8| b.get(i + 1) == Some(&x);
        let (tok, len) = match c {
            b'|' => (Tok::Pipe, 1),
            b'(' => (Tok::LParen, 1),
            b')' => (Tok::RParen, 1),
            b'{' => (Tok::LBrace, 1),
            b'}' => (Tok::RBrace, 1),
            b',' => (Tok::Comma, 1),
            b':' => (Tok::Colon, 1),
            b'.' if !b.get(i + 1).is_some_and(|d| d.is_ascii_digit()) => (Tok::Dot, 1),
            b'+' => (Tok::Plus, 1),
            b'-' => (Tok::Minus, 1),
            b'*' => (Tok::Star, 1),
            b'/' => (Tok::Slash, 1),
            b'=' if two(b'=') => (Tok::Eq, 2),
            b'=' => (Tok::Assign, 1),
            b'!' if two(b'=') => (Tok::Ne, 2),
            b'<' if two(b'=') => (Tok::Le, 2),
            b'<' if two(b'>') => (Tok::Ne, 2),
            b'<' => (Tok::Lt, 1),
            b'>' if two(b'=') => (Tok::Ge, 2),
            b'>' => (Tok::Gt, 1),
            b'"' | b'\'' => {
                let q = c;
                let mut s = String::new();
                let mut j = i + 1;
                loop {
                    let Some(&d) = b.get(j) else { return Err(QError::at(at, "unterminated string")) };
                    if d == q {
                        break;
                    }
                    if d == b'\\' {
                        let e = *b.get(j + 1).ok_or_else(|| QError::at(j, "unterminated escape"))?;
                        s.push(match e {
                            b'n' => '\n',
                            b't' => '\t',
                            b'r' => '\r',
                            b'\\' => '\\',
                            b'"' => '"',
                            b'\'' => '\'',
                            _ => return Err(QError::at(j, "unknown escape")),
                        });
                        j += 2;
                        continue;
                    }
                    let ch = src[j..].chars().next().unwrap();
                    s.push(ch);
                    j += ch.len_utf8();
                }
                (Tok::Str(s), j + 1 - i)
            }
            b'0'..=b'9' | b'.' => {
                let mut j = i;
                while j < b.len() && (b[j].is_ascii_digit() || b[j] == b'.' || b[j] == b'_') {
                    j += 1;
                }
                let num: String = src[i..j].chars().filter(|&c| c != '_').collect();
                let mut k = j;
                while k < b.len() && b[k].is_ascii_alphabetic() {
                    k += 1;
                }
                let unit = &src[j..k];
                if !unit.is_empty() {
                    let mul = dur_unit(unit).ok_or_else(|| QError::at(j, format!("unknown unit '{unit}'")))?;
                    let v: f64 = num.parse().map_err(|_| QError::at(at, "bad number"))?;
                    (Tok::Dur((v * mul as f64).round() as i64), k - i)
                } else if num.contains('.') {
                    (Tok::Float(num.parse().map_err(|_| QError::at(at, "bad number"))?), j - i)
                } else {
                    (Tok::Int(num.parse().map_err(|_| QError::at(at, "bad number"))?), j - i)
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let mut j = i;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                (Tok::Ident(src[i..j].to_string()), j - i)
            }
            _ => return Err(QError::at(at, format!("unexpected character '{}'", src[i..].chars().next().unwrap()))),
        };
        out.push(Token { tok, at });
        i += len;
    }
    out.push(Token { tok: Tok::End, at: b.len() });
    Ok(out)
}
