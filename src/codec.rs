//! Binary encoding helpers (LEB128 varints, length-prefixed bytes) and the crate's error type.

use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Corrupt(String),
    Json(crate::json::JsonError),
    BadRequest(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Corrupt(s) => write!(f, "corrupt: {s}"),
            Error::Json(e) => write!(f, "{e}"),
            Error::BadRequest(s) => write!(f, "bad request: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<crate::json::JsonError> for Error {
    fn from(e: crate::json::JsonError) -> Self {
        Error::Json(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn corrupt<T>(s: impl Into<String>) -> Result<T> {
    Err(Error::Corrupt(s.into()))
}

#[derive(Default)]
pub struct Enc {
    pub buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Self {
        Enc { buf: Vec::new() }
    }
    #[inline]
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    #[inline]
    pub fn var(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    #[inline]
    pub fn bytes(&mut self, b: &[u8]) {
        self.var(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    #[inline]
    pub fn u128(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
}

pub struct Dec<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Dec<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Dec { b, i: 0 }
    }
    pub fn done(&self) -> bool {
        self.i >= self.b.len()
    }
    pub fn remaining(&self) -> usize {
        self.b.len() - self.i
    }
    pub fn rest(&mut self) -> &'a [u8] {
        let s = &self.b[self.i..];
        self.i = self.b.len();
        s
    }
    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        let v = *self.b.get(self.i).ok_or_else(|| Error::Corrupt("short read (u8)".into()))?;
        self.i += 1;
        Ok(v)
    }
    #[inline]
    pub fn var(&mut self) -> Result<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let byte = self.u8()?;
            if shift >= 64 {
                return corrupt("varint too long");
            }
            v |= ((byte & 0x7f) as u64) << shift;
            if byte < 0x80 {
                return Ok(v);
            }
            shift += 7;
        }
    }
    #[inline]
    pub fn var32(&mut self) -> Result<u32> {
        let v = self.var()?;
        u32::try_from(v).map_err(|_| Error::Corrupt("varint over u32".into()))
    }
    #[inline]
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.var()? as usize;
        self.raw(n)
    }
    #[inline]
    pub fn raw(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.b.len() - self.i < n {
            return corrupt("short read (bytes)");
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    #[inline]
    pub fn u128(&mut self) -> Result<u128> {
        let s = self.raw(16)?;
        Ok(u128::from_le_bytes(s.try_into().unwrap()))
    }
    pub fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| Error::Corrupt("string not utf-8".into()))
    }
}

pub fn hex(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(H[(x >> 4) as usize] as char);
        s.push(H[(x & 15) as usize] as char);
    }
    s
}

pub fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let b = s.as_bytes();
    for i in 0..32 {
        let v = |c: u8| -> Option<u8> {
            match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            }
        };
        out[i] = (v(b[2 * i])? << 4) | v(b[2 * i + 1])?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let mut e = Enc::new();
        e.var(0);
        e.var(300);
        e.var(u64::MAX);
        e.bytes(b"abc");
        e.u128(7);
        let mut d = Dec::new(&e.buf);
        assert_eq!(d.var().unwrap(), 0);
        assert_eq!(d.var().unwrap(), 300);
        assert_eq!(d.var().unwrap(), u64::MAX);
        assert_eq!(d.bytes().unwrap(), b"abc");
        assert_eq!(d.u128().unwrap(), 7);
        assert!(d.done());
        assert!(d.u8().is_err());
    }
}
