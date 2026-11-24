//! The query AST and a recursive-descent parser.
//!
//! ```text
//! query     := ['steps'] ('|' stage)*
//! stage     := 'where' expr
//!            | 'match' pattern
//!            | 'extend' name '=' expr {',' name '=' expr}
//!            | 'project' item {',' item}              item := name ['=' expr] | expr
//!            | 'summarize' agg {',' agg} ['by' key {',' key}]
//!            | 'top' INT 'by' expr ['asc'|'desc']
//!            | 'sort' 'by' expr ['asc'|'desc'] {',' expr ['asc'|'desc']}
//!            | 'take' INT | 'count'
//! pattern   := elem {('then'|'next') elem} ['within' INT ['steps'|'turns']]
//!              ['where' constraint {'and' constraint}]
//! elem      := [name ':'] expr ['{' INT [',' [INT]] '}']
//! constraint:= 'no' '(' expr ')' 'between' name 'and' name | name '.' field '==' name '.' field
//! expr      := or ; or := and {'or' and} ; and := not {'and' not} ; not := 'not' not | cmp
//! cmp       := add [op add | ['!'|'not'] 'in' '(' list ')' | strop add]
//! strop     := has | contains | startswith | endswith | matches   (case-insensitive; *_cs for exact case)
//! ```

use super::lex::{lex, Tok, Token};
use super::value::Value;
use super::QError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrOp {
    Has,
    Contains,
    StartsWith,
    EndsWith,
    /// Glob: `*` any run, `?` one character.
    Matches,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Lit(Value),
    /// `tool`, `input.file_path`, `a.time`, `a.input.command`
    Ref(Vec<String>),
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    In { e: Box<Expr>, list: Vec<Expr>, neg: bool },
    Str { op: StrOp, e: Box<Expr>, pat: Box<Expr>, neg: bool, cs: bool },
    Ar(ArOp, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}

impl Expr {
    /// Every column reference in the expression.
    pub fn refs<'a>(&'a self, out: &mut Vec<&'a [String]>) {
        match self {
            Expr::Lit(_) => {}
            Expr::Ref(r) => out.push(r),
            Expr::Not(e) => e.refs(out),
            Expr::And(v) | Expr::Or(v) => v.iter().for_each(|e| e.refs(out)),
            Expr::Cmp(_, a, b) | Expr::Ar(_, a, b) => {
                a.refs(out);
                b.refs(out);
            }
            Expr::In { e, list, .. } => {
                e.refs(out);
                list.iter().for_each(|x| x.refs(out));
            }
            Expr::Str { e, pat, .. } => {
                e.refs(out);
                pat.refs(out);
            }
            Expr::Call(_, args) => args.iter().for_each(|x| x.refs(out)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    /// Any later turn on the same path.
    Then,
    /// The immediately following turn.
    Next,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Elem {
    pub name: Option<String>,
    pub pred: Expr,
    pub min: u32,
    /// `None` = no upper bound.
    pub max: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub elems: Vec<Elem>,
    /// `links[k]` joins elem `k` to elem `k - 1`; `links[0]` is unused.
    pub links: Vec<Link>,
    pub within: Option<u32>,
    /// (excluded steps, from elem, to elem)
    pub excl: Vec<(Expr, usize, usize)>,
    /// Equal field across elems: (field path, elems)
    pub eq: Option<(Vec<String>, Vec<usize>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    DCount,
    Sum,
    Min,
    Max,
    Avg,
    CountIf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Where(Expr),
    Match(Pattern),
    Extend(Vec<(String, Expr)>),
    Project(Vec<(String, Expr)>),
    Summarize { aggs: Vec<(String, AggFn, Option<Expr>)>, by: Vec<(String, Expr)> },
    Top { n: usize, by: Expr, desc: bool },
    Sort(Vec<(Expr, bool)>),
    Take(usize),
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub source: Source,
    pub stages: Vec<Stage>,
}

/// What a query's rows are before its first stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Source {
    /// Tool calls paired with their results (the default).
    #[default]
    Steps,
    /// Text the user wrote and the assistant said, one row per text block.
    Messages,
}

const RESERVED: &[&str] = &[
    "and", "or", "not", "in", "has", "contains", "startswith", "endswith", "matches", "has_cs", "contains_cs", "startswith_cs", "endswith_cs",
    "matches_cs", "then", "next", "within", "where", "by", "between", "asc", "desc", "steps", "turns", "messages",
];

struct P {
    t: Vec<Token>,
    i: usize,
}

impl P {
    fn peek(&self) -> &Tok {
        &self.t[self.i].tok
    }
    fn peek2(&self) -> &Tok {
        &self.t[(self.i + 1).min(self.t.len() - 1)].tok
    }
    fn at(&self) -> usize {
        self.t[self.i].at
    }
    fn bump(&mut self) -> Tok {
        let t = self.t[self.i].tok.clone();
        if self.i + 1 < self.t.len() {
            self.i += 1;
        }
        t
    }
    fn err<T>(&self, m: impl Into<String>) -> Result<T, QError> {
        Err(QError::at(self.at(), m))
    }
    fn is_kw(&self, k: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(k))
    }
    fn kw(&mut self, k: &str) -> bool {
        if self.is_kw(k) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn expect_kw(&mut self, k: &str) -> Result<(), QError> {
        if self.kw(k) { Ok(()) } else { self.err(format!("expected '{k}'")) }
    }
    fn expect(&mut self, t: Tok, what: &str) -> Result<(), QError> {
        if *self.peek() == t {
            self.bump();
            Ok(())
        } else {
            self.err(format!("expected {what}"))
        }
    }
    fn ident(&mut self) -> Result<String, QError> {
        match self.peek().clone() {
            Tok::Ident(s) if !RESERVED.contains(&s.to_ascii_lowercase().as_str()) => {
                self.bump();
                Ok(s)
            }
            _ => self.err("expected a name"),
        }
    }
    fn int(&mut self) -> Result<i64, QError> {
        match self.bump() {
            Tok::Int(n) => Ok(n),
            _ => {
                self.i -= 1;
                self.err("expected a number")
            }
        }
    }

    fn query(&mut self) -> Result<Query, QError> {
        let mut stages = Vec::new();
        let source = if self.kw("messages") {
            Source::Messages
        } else {
            self.kw("steps");
            Source::Steps
        };
        loop {
            match self.peek() {
                Tok::End => break,
                Tok::Pipe => {
                    self.bump();
                    stages.push(self.stage()?);
                }
                _ if stages.is_empty() => stages.push(self.stage()?),
                _ => return self.err("expected '|' or the end of the query"),
            }
        }
        Ok(Query { source, stages })
    }

    fn stage(&mut self) -> Result<Stage, QError> {
        let name = match self.peek() {
            Tok::Ident(s) => s.to_ascii_lowercase(),
            _ => return self.err("expected a stage (where, match, extend, project, summarize, top, sort, take, count)"),
        };
        self.bump();
        Ok(match name.as_str() {
            "where" => Stage::Where(self.expr()?),
            "match" => Stage::Match(self.pattern()?),
            "extend" => Stage::Extend(self.assigns()?),
            "project" => {
                let mut v = Vec::new();
                loop {
                    let e = self.named_or_expr()?;
                    v.push(e);
                    if *self.peek() != Tok::Comma {
                        break;
                    }
                    self.bump();
                }
                Stage::Project(v)
            }
            "summarize" => {
                let mut aggs = Vec::new();
                loop {
                    let (mut name, e) = self.named_or_expr()?;
                    let Expr::Call(f, args) = e else { return self.err("summarize needs aggregates such as count(), dcount(x), sum(x)") };
                    let fun = match f.to_ascii_lowercase().as_str() {
                        "count" => AggFn::Count,
                        "dcount" => AggFn::DCount,
                        "sum" => AggFn::Sum,
                        "min" => AggFn::Min,
                        "max" => AggFn::Max,
                        "avg" => AggFn::Avg,
                        "countif" => AggFn::CountIf,
                        _ => return self.err(format!("unknown aggregate '{f}'")),
                    };
                    let arg = args.into_iter().next();
                    if fun != AggFn::Count && arg.is_none() {
                        return self.err(format!("{f}() needs an argument"));
                    }
                    if name.starts_with("Column") {
                        name = match fun {
                            AggFn::Count => "count_".into(),
                            _ => format!("{}_", f.to_ascii_lowercase()),
                        };
                    }
                    aggs.push((name, fun, arg));
                    if *self.peek() != Tok::Comma {
                        break;
                    }
                    self.bump();
                }
                let mut by = Vec::new();
                if self.kw("by") {
                    loop {
                        by.push(self.named_or_expr()?);
                        if *self.peek() != Tok::Comma {
                            break;
                        }
                        self.bump();
                    }
                }
                Stage::Summarize { aggs, by }
            }
            "top" => {
                let n = self.int()? as usize;
                self.expect_kw("by")?;
                let by = self.expr()?;
                let desc = !self.kw("asc");
                self.kw("desc");
                Stage::Top { n, by, desc }
            }
            "sort" | "order" => {
                self.expect_kw("by")?;
                let mut v = Vec::new();
                loop {
                    let e = self.expr()?;
                    let desc = !self.kw("asc");
                    self.kw("desc");
                    v.push((e, desc));
                    if *self.peek() != Tok::Comma {
                        break;
                    }
                    self.bump();
                }
                Stage::Sort(v)
            }
            "take" | "limit" => Stage::Take(self.int()? as usize),
            "count" => Stage::Count,
            other => return Err(QError::at(self.t[self.i - 1].at, format!("unknown stage '{other}'"))),
        })
    }

    fn assigns(&mut self) -> Result<Vec<(String, Expr)>, QError> {
        let mut v = Vec::new();
        loop {
            let n = match self.peek().clone() {
                Tok::Ident(n) if *self.peek2() == Tok::Assign => {
                    self.bump();
                    n
                }
                _ => return self.err("expected name = value"),
            };
            self.expect(Tok::Assign, "'='")?;
            v.push((n, self.expr()?));
            if *self.peek() != Tok::Comma {
                break;
            }
            self.bump();
        }
        Ok(v)
    }

    /// `name = expr` or `expr` (named after its last path part, or Column<i>).
    fn named_or_expr(&mut self) -> Result<(String, Expr), QError> {
        if let (Tok::Ident(n), Tok::Assign) = (self.peek().clone(), self.peek2().clone()) {
            // any word can name a column, reserved or not
            self.bump();
            self.bump();
            return Ok((n, self.expr()?));
        }
        let at = self.i;
        let e = self.expr()?;
        let name = match &e {
            Expr::Ref(p) => p.join("_"),
            Expr::Call(f, args) if f == "bin" => match args.first() {
                Some(Expr::Ref(p)) => p.join("_"),
                _ => format!("Column{at}"),
            },
            _ => format!("Column{at}"),
        };
        Ok((name, e))
    }

    fn pattern(&mut self) -> Result<Pattern, QError> {
        let mut elems = Vec::new();
        let mut links = vec![Link::Then];
        loop {
            let name = if matches!(self.peek(), Tok::Ident(_)) && *self.peek2() == Tok::Colon {
                let n = self.ident()?;
                self.bump();
                Some(n)
            } else {
                None
            };
            let pred = self.expr()?;
            let (mut min, mut max) = (1, Some(1));
            if *self.peek() == Tok::LBrace {
                self.bump();
                min = self.int()? as u32;
                max = Some(min);
                if *self.peek() == Tok::Comma {
                    self.bump();
                    max = if *self.peek() == Tok::RBrace { None } else { Some(self.int()? as u32) };
                }
                self.expect(Tok::RBrace, "'}'")?;
                if min == 0 || max.is_some_and(|m| m < min) {
                    return self.err("repeat counts must be {m}, {m,n} or {m,} with 1 <= m <= n");
                }
            }
            elems.push(Elem { name, pred, min, max });
            if self.kw("then") {
                links.push(Link::Then);
            } else if self.kw("next") {
                links.push(Link::Next);
            } else {
                break;
            }
        }
        if elems.len() > 8 {
            return self.err("a match can have at most 8 elements");
        }
        let mut within = None;
        if self.kw("within") {
            within = Some(self.int()? as u32);
            let _ = self.kw("steps") || self.kw("turns");
        }
        let idx = |p: &P, n: &str| -> Result<usize, QError> {
            elems.iter().position(|e| e.name.as_deref() == Some(n)).ok_or_else(|| QError::at(p.at(), format!("no element named '{n}'")))
        };
        let mut excl = Vec::new();
        let mut eq: Option<(Vec<String>, Vec<usize>)> = None;
        if self.kw("where") {
            loop {
                if self.kw("no") {
                    self.expect(Tok::LParen, "'('")?;
                    let e = self.expr()?;
                    self.expect(Tok::RParen, "')'")?;
                    self.expect_kw("between")?;
                    let a = self.ident()?;
                    let ai = idx(self, &a)?;
                    self.expect_kw("and")?;
                    let c = self.ident()?;
                    let ci = idx(self, &c)?;
                    if ci <= ai {
                        return self.err("'between a and c' needs a before c");
                    }
                    excl.push((e, ai, ci));
                } else {
                    let l = self.path()?;
                    self.expect(Tok::Eq, "'=='")?;
                    let r = self.path()?;
                    if l.len() < 2 || r.len() < 2 || l[1..] != r[1..] {
                        return self.err("compare the same field of two elements, such as a.file_path == c.file_path");
                    }
                    let (li, ri) = (idx(self, &l[0])?, idx(self, &r[0])?);
                    let field = l[1..].to_vec();
                    match &mut eq {
                        None => eq = Some((field, vec![li, ri])),
                        Some((f, v)) if *f == field => {
                            v.push(li);
                            v.push(ri);
                        }
                        Some(_) => return self.err("only one compared field per match for now"),
                    }
                }
                if !self.kw("and") {
                    break;
                }
            }
        }
        if let Some((_, v)) = &mut eq {
            v.sort_unstable();
            v.dedup();
        }
        Ok(Pattern { elems, links, within, excl, eq })
    }

    fn path(&mut self) -> Result<Vec<String>, QError> {
        let mut p = vec![self.ident()?];
        while *self.peek() == Tok::Dot {
            self.bump();
            p.push(self.ident()?);
        }
        Ok(p)
    }

    fn expr(&mut self) -> Result<Expr, QError> {
        let mut v = vec![self.and()?];
        while self.kw("or") {
            v.push(self.and()?);
        }
        Ok(if v.len() == 1 { v.pop().unwrap() } else { Expr::Or(v) })
    }

    fn and(&mut self) -> Result<Expr, QError> {
        let mut v = vec![self.not()?];
        while self.is_kw("and") {
            self.bump();
            v.push(self.not()?);
        }
        Ok(if v.len() == 1 { v.pop().unwrap() } else { Expr::And(v) })
    }

    fn not(&mut self) -> Result<Expr, QError> {
        if self.is_kw("not") && !matches!(self.peek2(), Tok::Ident(s) if s.eq_ignore_ascii_case("in")) {
            self.bump();
            return Ok(Expr::Not(Box::new(self.not()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Result<Expr, QError> {
        let l = self.add()?;
        let op = match self.peek() {
            Tok::Eq => Some(CmpOp::Eq),
            Tok::Ne => Some(CmpOp::Ne),
            Tok::Lt => Some(CmpOp::Lt),
            Tok::Le => Some(CmpOp::Le),
            Tok::Gt => Some(CmpOp::Gt),
            Tok::Ge => Some(CmpOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.bump();
            let r = self.add()?;
            return Ok(Expr::Cmp(op, Box::new(l), Box::new(r)));
        }
        // [not|!] in (...)
        let neg_in = (self.is_kw("not") && matches!(self.peek2(), Tok::Ident(s) if s.eq_ignore_ascii_case("in")))
            || (*self.peek() == Tok::Ident("!in".into()));
        if neg_in || self.is_kw("in") {
            if neg_in {
                self.bump();
            }
            self.expect_kw("in")?;
            self.expect(Tok::LParen, "'('")?;
            let mut list = Vec::new();
            loop {
                list.push(self.add()?);
                if *self.peek() != Tok::Comma {
                    break;
                }
                self.bump();
            }
            self.expect(Tok::RParen, "')'")?;
            return Ok(Expr::In { e: Box::new(l), list, neg: neg_in });
        }
        let word = match self.peek() {
            Tok::Ident(s) => s.to_ascii_lowercase(),
            _ => return Ok(l),
        };
        let (base, cs) = match word.strip_suffix("_cs") {
            Some(b) => (b.to_string(), true),
            None => (word.clone(), false),
        };
        let op = match base.as_str() {
            "has" => StrOp::Has,
            "contains" => StrOp::Contains,
            "startswith" => StrOp::StartsWith,
            "endswith" => StrOp::EndsWith,
            "matches" => StrOp::Matches,
            _ => return Ok(l),
        };
        self.bump();
        let pat = self.add()?;
        Ok(Expr::Str { op, e: Box::new(l), pat: Box::new(pat), neg: false, cs })
    }

    fn add(&mut self) -> Result<Expr, QError> {
        let mut l = self.mul()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArOp::Add,
                Tok::Minus => ArOp::Sub,
                _ => return Ok(l),
            };
            self.bump();
            let r = self.mul()?;
            l = Expr::Ar(op, Box::new(l), Box::new(r));
        }
    }

    fn mul(&mut self) -> Result<Expr, QError> {
        let mut l = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArOp::Mul,
                Tok::Slash => ArOp::Div,
                _ => return Ok(l),
            };
            self.bump();
            let r = self.unary()?;
            l = Expr::Ar(op, Box::new(l), Box::new(r));
        }
    }

    fn unary(&mut self) -> Result<Expr, QError> {
        if *self.peek() == Tok::Minus {
            self.bump();
            let e = self.unary()?;
            return Ok(match e {
                Expr::Lit(Value::Int(n)) => Expr::Lit(Value::Int(-n)),
                Expr::Lit(Value::Float(f)) => Expr::Lit(Value::Float(-f)),
                Expr::Lit(Value::Dur(d)) => Expr::Lit(Value::Dur(-d)),
                e => Expr::Ar(ArOp::Sub, Box::new(Expr::Lit(Value::Int(0))), Box::new(e)),
            });
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, QError> {
        match self.peek().clone() {
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Lit(Value::Str(s.into())))
            }
            Tok::Int(n) => {
                self.bump();
                Ok(Expr::Lit(Value::Int(n)))
            }
            Tok::Float(f) => {
                self.bump();
                Ok(Expr::Lit(Value::Float(f)))
            }
            Tok::Dur(d) => {
                self.bump();
                Ok(Expr::Lit(Value::Dur(d)))
            }
            Tok::LParen => {
                self.bump();
                let e = self.expr()?;
                self.expect(Tok::RParen, "')'")?;
                Ok(e)
            }
            Tok::Ident(s) => {
                let low = s.to_ascii_lowercase();
                match low.as_str() {
                    "true" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Bool(true)));
                    }
                    "false" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Bool(false)));
                    }
                    "null" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Null));
                    }
                    _ => {}
                }
                if *self.peek2() == Tok::LParen && !RESERVED.contains(&low.as_str()) {
                    self.bump();
                    self.bump();
                    let mut args = Vec::new();
                    if *self.peek() != Tok::RParen {
                        loop {
                            args.push(self.expr()?);
                            if *self.peek() != Tok::Comma {
                                break;
                            }
                            self.bump();
                        }
                    }
                    self.expect(Tok::RParen, "')'")?;
                    return Ok(Expr::Call(low, args));
                }
                Ok(Expr::Ref(self.path()?))
            }
            _ => self.err("expected a value, a column or '('"),
        }
    }
}

pub fn parse(src: &str) -> Result<Query, QError> {
    let mut p = P { t: lex(src)?, i: 0 };
    p.query()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_design_example() {
        let q = parse(
            r#"where time > ago(7d)
            | match a: tool == "Bash" and failed
                then b: tool == "Edit" and file_path matches "*test*"
                then c: tool == "Bash" and not failed
                where no (tool == "Edit" and not file_path matches "*test*") between a and c
            | summarize cheats = count() by session, bin(a.time, 1d)
            | top 10 by cheats"#,
        )
        .unwrap();
        assert_eq!(q.stages.len(), 4);
        let Stage::Match(p) = &q.stages[1] else { panic!() };
        assert_eq!(p.elems.len(), 3);
        assert_eq!(p.excl.len(), 1);
        assert_eq!(p.excl[0].1, 0);
        assert_eq!(p.excl[0].2, 2);
    }

    #[test]
    fn repeats_links_and_eq() {
        let q = parse(r#"match r: tool in ("Read", "Grep"){3,} next e: tool == "Edit" within 10 steps where r.file_path == e.file_path"#).unwrap();
        let Stage::Match(p) = &q.stages[0] else { panic!() };
        assert_eq!((p.elems[0].min, p.elems[0].max), (3, None));
        assert_eq!(p.links[1], Link::Next);
        assert_eq!(p.within, Some(10));
        assert_eq!(p.eq, Some((vec!["file_path".to_string()], vec![0, 1])));
    }

    #[test]
    fn reserved_words_can_name_columns() {
        let q = parse("summarize matches = count(), then = dcount(session) | extend by = 1").unwrap();
        assert_eq!(q.stages.len(), 2);
    }

    #[test]
    fn errors_point_somewhere() {
        assert!(parse("where tool ==").is_err());
        assert!(parse("match a: x then").is_err());
        assert!(parse("| frobnicate").is_err());
        assert!(parse("match a: x where no (y) between c and a").is_err());
    }
}
