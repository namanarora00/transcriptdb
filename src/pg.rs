//! A minimal PostgreSQL client for the head-to-head: protocol 3.0 over TCP, trust
//! authentication, simple queries (rows as text) and `COPY ... FROM STDIN`.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

#[derive(Debug)]
pub struct PgError(pub String);

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "postgres: {}", self.0)
    }
}

impl std::error::Error for PgError {}

impl From<io::Error> for PgError {
    fn from(e: io::Error) -> Self {
        PgError(e.to_string())
    }
}

pub type Rows = Vec<Vec<Option<String>>>;

pub struct Pg {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
}

fn error_text(body: &[u8]) -> String {
    // fields: type byte, cstring; M = message, D = detail
    let mut out = String::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let t = body[i];
        let end = body[i + 1..].iter().position(|&b| b == 0).map(|p| i + 1 + p).unwrap_or(body.len());
        let s = String::from_utf8_lossy(&body[i + 1..end]);
        if t == b'M' || t == b'D' {
            if !out.is_empty() {
                out.push_str(": ");
            }
            out.push_str(&s);
        }
        i = end + 1;
    }
    out
}

impl Pg {
    pub fn connect(addr: &str, user: &str, db: &str) -> Result<Pg, PgError> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        let mut pg = Pg { r: BufReader::with_capacity(1 << 16, s.try_clone()?), w: BufWriter::with_capacity(1 << 16, s) };
        let mut body = Vec::new();
        body.extend_from_slice(&196_608u32.to_be_bytes());
        for (k, v) in [("user", user), ("database", db), ("client_encoding", "UTF8")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        pg.w.write_all(&((body.len() + 4) as u32).to_be_bytes())?;
        pg.w.write_all(&body)?;
        pg.w.flush()?;
        loop {
            let (t, b) = pg.read_msg()?;
            match t {
                b'R' => {
                    let code = u32::from_be_bytes(b[0..4].try_into().unwrap());
                    if code != 0 {
                        return Err(PgError(format!("server asked for authentication method {code}; this client needs trust")));
                    }
                }
                b'E' => return Err(PgError(error_text(&b))),
                b'Z' => return Ok(pg),
                _ => {}
            }
        }
    }

    fn read_msg(&mut self) -> Result<(u8, Vec<u8>), PgError> {
        let mut h = [0u8; 5];
        self.r.read_exact(&mut h)?;
        let len = u32::from_be_bytes(h[1..5].try_into().unwrap()) as usize;
        let mut b = vec![0u8; len - 4];
        self.r.read_exact(&mut b)?;
        Ok((h[0], b))
    }

    fn send(&mut self, t: u8, body: &[u8]) -> io::Result<()> {
        self.w.write_all(&[t])?;
        self.w.write_all(&((body.len() + 4) as u32).to_be_bytes())?;
        self.w.write_all(body)
    }

    /// Run one or more statements; returns the rows of the last statement that had any.
    pub fn query(&mut self, sql: &str) -> Result<Rows, PgError> {
        let mut q = sql.as_bytes().to_vec();
        q.push(0);
        self.send(b'Q', &q)?;
        self.w.flush()?;
        let mut rows = Vec::new();
        let mut cur: Rows = Vec::new();
        let mut err = None;
        loop {
            let (t, b) = self.read_msg()?;
            match t {
                b'T' => cur.clear(),
                b'D' => {
                    let n = u16::from_be_bytes([b[0], b[1]]) as usize;
                    let mut i = 2;
                    let mut row = Vec::with_capacity(n);
                    for _ in 0..n {
                        let l = i32::from_be_bytes(b[i..i + 4].try_into().unwrap());
                        i += 4;
                        if l < 0 {
                            row.push(None);
                        } else {
                            let l = l as usize;
                            row.push(Some(String::from_utf8_lossy(&b[i..i + l]).into_owned()));
                            i += l;
                        }
                    }
                    cur.push(row);
                }
                b'C' => {
                    if !cur.is_empty() {
                        rows = std::mem::take(&mut cur);
                    }
                }
                b'E' => err = Some(PgError(error_text(&b))),
                b'G' => {
                    // COPY FROM STDIN without data: refuse
                    self.send(b'f', b"no data\0")?;
                    self.w.flush()?;
                }
                b'Z' => break,
                _ => {}
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }

    pub fn exec(&mut self, sql: &str) -> Result<(), PgError> {
        self.query(sql).map(|_| ())
    }

    /// `COPY ... FROM STDIN` (text format); `fill` writes lines through the [`Copy`].
    pub fn copy_in(&mut self, sql: &str, fill: impl FnOnce(&mut Copy) -> Result<(), PgError>) -> Result<(), PgError> {
        let mut q = sql.as_bytes().to_vec();
        q.push(0);
        self.send(b'Q', &q)?;
        self.w.flush()?;
        loop {
            let (t, b) = self.read_msg()?;
            match t {
                b'G' => break,
                b'E' => {
                    let e = PgError(error_text(&b));
                    while self.read_msg()?.0 != b'Z' {}
                    return Err(e);
                }
                _ => {}
            }
        }
        let mut c = Copy { pg: self, buf: Vec::with_capacity(1 << 20), line: Vec::new() };
        let res = fill(&mut c);
        c.flush_buf()?;
        match &res {
            Ok(()) => self.send(b'c', &[])?,
            Err(e) => {
                let mut m = e.0.clone().into_bytes();
                m.push(0);
                self.send(b'f', &m)?;
            }
        }
        self.w.flush()?;
        let mut err = None;
        loop {
            let (t, b) = self.read_msg()?;
            match t {
                b'E' => err = Some(PgError(error_text(&b))),
                b'Z' => break,
                _ => {}
            }
        }
        res?;
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Writes rows for `COPY ... FROM STDIN` in text format.
pub struct Copy<'a> {
    pg: &'a mut Pg,
    buf: Vec<u8>,
    line: Vec<u8>,
}

impl Copy<'_> {
    fn flush_buf(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let b = std::mem::take(&mut self.buf);
            self.pg.send(b'd', &b)?;
            self.buf = b;
            self.buf.clear();
        }
        Ok(())
    }

    /// Add a field to the current line (`None` = NULL).
    pub fn field(&mut self, v: Option<&[u8]>) {
        if !self.line.is_empty() {
            self.line.push(b'\t');
        }
        match v {
            None => self.line.extend_from_slice(b"\\N"),
            Some(v) => {
                for &c in v {
                    match c {
                        b'\\' => self.line.extend_from_slice(b"\\\\"),
                        b'\n' => self.line.extend_from_slice(b"\\n"),
                        b'\r' => self.line.extend_from_slice(b"\\r"),
                        b'\t' => self.line.extend_from_slice(b"\\t"),
                        0 => {}
                        c => self.line.push(c),
                    }
                }
            }
        }
    }

    pub fn int(&mut self, v: i64) {
        self.field(Some(v.to_string().as_bytes()));
    }

    pub fn end_row(&mut self) -> io::Result<()> {
        self.line.push(b'\n');
        self.buf.extend_from_slice(&self.line);
        self.line.clear();
        if self.buf.len() >= 1 << 20 {
            self.flush_buf()?;
        }
        Ok(())
    }
}
