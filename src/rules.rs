//! Column rules: named facts a user's rule works out from each step.
//!
//! ```text
//! .create column program      on tool == "Bash"             = extract(command, @"^\s*(\S+)")
//! .create column tests_failed on program in ("pytest", "npm") = toint(extract(output, @"(\d+) failed"))
//! ```
//!
//! A rule reads only the step itself: its tool, its outcome (`failed`, `ok`), the size of
//! its result (`out_bytes`), its input fields, the text of its result (`output`) and the
//! columns of rules defined before it. With `on TEST`, steps that fail the test have no
//! value. A value is a string (cut to [`DERIVED_MAX`] bytes), a number or a boolean; a
//! rule that gives null, or cannot be worked out for a step, leaves the step without one.
//!
//! A value is stored as one more field of its step, named like the rule, so it is coded,
//! indexed and queried like an input field (a rule's name hides an input field of the
//! same name). Each segment records the rules its steps' values came from and their
//! versions; a version is a hash of the rule's text and of the versions of the rules it
//! reads. Steps in a segment written without a rule, or with another version of it, get
//! their values worked out when a session's steps are read (`Db::read_steps`), by the
//! same code that stores them: at write time for new steps, and when a merge rewrites a
//! session for old ones.

use crate::codec::{corrupt, Dec, Enc, Result};
use crate::query::parse::{self, Expr};
use crate::query::value::{self, Ctx, Value};
use crate::query::QError;

/// Strings a rule gives are cut to this many bytes (at a character).
pub const DERIVED_MAX: usize = 512;
/// Part of every version: raised when what rules compute changes.
const SEMANTICS: u64 = 1;
const MAGIC: &[u8; 8] = b"TDBRULE1";
pub const MAX_RULES: usize = 256;

/// Columns of a step a rule can read besides input fields and other rules.
const STEP_COLS: &[&str] = &["tool", "failed", "ok", "out_bytes", "output"];
/// Columns that are not the step's own: a rule may not read them.
const NOT_STEP: &[&str] = &["time", "step", "session", "agent"];
const FUNCTIONS: &[(&str, usize, usize)] =
    &[("extract", 2, 3), ("toint", 1, 1), ("tolong", 1, 1), ("strlen", 1, 1), ("tolower", 1, 1), ("toupper", 1, 1), ("isnull", 1, 1), ("isnotnull", 1, 1), ("tostring", 1, 1), ("iff", 3, 3), ("iif", 3, 3), ("bin", 2, 2), ("floor", 2, 2)];

#[derive(Debug, Clone)]
pub struct Rule {
    pub name: String,
    /// The rule as written (trimmed): what is kept and what the version is a hash of.
    pub def: String,
    pub ver: u64,
    /// Rules it reads (positions in the set, all before its own).
    pub deps: Vec<usize>,
    /// Input fields it reads.
    pub inputs: Vec<String>,
    pub reads_output: bool,
}

/// The rules of a store, in the order they were defined. Shared by every thread: the
/// parsed rules are kept per thread (see [`RuleSet::parsed`]).
#[derive(Debug, Clone, Default)]
pub struct RuleSet {
    pub rules: Vec<Rule>,
    /// A hash of every rule's name and version.
    id: u64,
}

/// A rule's test and expression.
struct Parsed {
    on: Option<Expr>,
    expr: Expr,
}

thread_local! {
    /// The parsed rules of the set this thread last worked with.
    static PARSED: std::cell::RefCell<Option<(u64, std::rc::Rc<Vec<Parsed>>)>> = const { std::cell::RefCell::new(None) };
}

/// What a rule can read of a step.
pub trait StepView {
    fn tool(&self) -> Value;
    fn failed(&self) -> bool;
    fn out_bytes(&self) -> u32;
    /// The value of an input field; null when the step has none.
    fn input(&self, name: &str) -> Value;
    /// The text of the step's result.
    fn output(&self) -> Value;
}

struct RuleCtx<'a> {
    set: &'a RuleSet,
    upto: usize,
    view: &'a dyn StepView,
    vals: &'a [Value],
}

impl Ctx for RuleCtx<'_> {
    fn col(&self, path: &[String]) -> std::result::Result<Value, QError> {
        Ok(match path {
            [x] => match x.as_str() {
                "tool" => self.view.tool(),
                "failed" => Value::Bool(self.view.failed()),
                "ok" => Value::Bool(!self.view.failed()),
                "out_bytes" => Value::Int(self.view.out_bytes() as i64),
                "output" => self.view.output(),
                name => match self.set.rules[..self.upto].iter().position(|r| r.name == name) {
                    Some(i) => self.vals[i].clone(),
                    None => self.view.input(name),
                },
            },
            [i, f] if i == "input" => self.view.input(f),
            _ => return Err(QError::new(format!("a rule cannot read '{}'", path.join(".")))),
        })
    }
    fn now(&self) -> i64 {
        0
    }
}

/// JSON text of a string.
fn json_string(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for &c in s.as_bytes() {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\r' => out.extend_from_slice(b"\\r"),
            c if c < 0x20 => out.extend_from_slice(format!("\\u{c:04x}").as_bytes()),
            c => out.push(c),
        }
    }
    out.push(b'"');
}

/// A rule's value as it is stored (JSON text), `None` for no value. Reading it back with
/// [`Value::from_json`] gives what queries and later rules see.
pub fn stored(v: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match v {
        Value::Null => return None,
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Int(i) | Value::Time(i) | Value::Dur(i) => out.extend_from_slice(i.to_string().as_bytes()),
        Value::Float(f) => {
            if !f.is_finite() {
                return None;
            }
            out.extend_from_slice(f.to_string().as_bytes());
        }
        Value::Str(s) => {
            let mut end = s.len().min(DERIVED_MAX);
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            json_string(&s[..end], &mut out);
        }
    }
    Some(out)
}

fn check_expr(e: &Expr) -> std::result::Result<(), QError> {
    match e {
        Expr::Lit(_) | Expr::Ref(_) => Ok(()),
        Expr::Not(x) => check_expr(x),
        Expr::And(v) | Expr::Or(v) => v.iter().try_for_each(check_expr),
        Expr::Cmp(_, a, b) | Expr::Ar(_, a, b) => {
            check_expr(a)?;
            check_expr(b)
        }
        Expr::In { e, list, .. } => {
            check_expr(e)?;
            list.iter().try_for_each(check_expr)
        }
        Expr::Str { e, pat, .. } => {
            check_expr(e)?;
            check_expr(pat)
        }
        Expr::Call(f, args) => {
            let Some(&(_, lo, hi)) = FUNCTIONS.iter().find(|x| x.0 == f) else {
                return Err(QError::new(format!("a rule cannot call {f}()")));
            };
            if args.len() < lo || args.len() > hi {
                return Err(QError::new(format!("{f}() takes {} here", if lo == hi { format!("{lo} values") } else { format!("{lo} to {hi} values") })));
            }
            if f == "extract" {
                match &args[1] {
                    Expr::Lit(Value::Str(re)) => {
                        if regex::Regex::new(re).is_err() {
                            return Err(QError::new(format!("bad regular expression in extract(): {re}")));
                        }
                    }
                    _ => return Err(QError::new("extract() takes the text, then a regular expression written out")),
                }
            }
            args.iter().try_for_each(check_expr)
        }
    }
}

impl RuleSet {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn position(&self, name: &[u8]) -> Option<usize> {
        self.rules.iter().position(|r| r.name.as_bytes() == name)
    }

    /// (name, version) of every rule: what a segment records about its steps' values.
    pub fn versions(&self) -> Vec<(Vec<u8>, u64)> {
        self.rules.iter().map(|r| (r.name.clone().into_bytes(), r.ver)).collect()
    }

    /// The rules given by `defs`, in order: each may read only the ones before it.
    pub fn build(defs: &[String]) -> std::result::Result<RuleSet, QError> {
        if defs.len() > MAX_RULES {
            return Err(QError::new(format!("a store holds at most {MAX_RULES} rules")));
        }
        let mut set = RuleSet::default();
        for def in defs {
            let def = def.trim().to_string();
            let (name, on, expr) = parse::parse_rule(&def)?;
            if STEP_COLS.contains(&name.as_str()) || NOT_STEP.contains(&name.as_str()) || name == "input" {
                return Err(QError::new(format!("'{name}' is a column every step has; a rule needs another name")));
            }
            if set.position(name.as_bytes()).is_some() {
                return Err(QError::new(format!("rule '{name}' is defined twice")));
            }
            let mut refs = Vec::new();
            if let Some(on) = &on {
                check_expr(on)?;
                on.refs(&mut refs);
            }
            check_expr(&expr)?;
            expr.refs(&mut refs);
            let (mut deps, mut inputs, mut reads_output) = (Vec::new(), Vec::new(), false);
            for r in refs {
                match r {
                    [x] if x == "output" => reads_output = true,
                    [x] if STEP_COLS.contains(&x.as_str()) => {}
                    [x] if NOT_STEP.contains(&x.as_str()) => return Err(QError::new(format!("rule '{name}' reads '{x}': a rule reads only the step itself"))),
                    [x] if x == &name => return Err(QError::new(format!("rule '{name}' reads itself"))),
                    [x] => match set.position(x.as_bytes()) {
                        Some(i) => {
                            if !deps.contains(&i) {
                                deps.push(i);
                            }
                        }
                        None => {
                            if !inputs.contains(x) {
                                inputs.push(x.clone());
                            }
                        }
                    },
                    [i, f] if i == "input" => {
                        if !inputs.contains(f) {
                            inputs.push(f.clone());
                        }
                    }
                    other => return Err(QError::new(format!("rule '{name}' cannot read '{}'", other.join(".")))),
                }
            }
            deps.sort_unstable();
            let mut h = Vec::with_capacity(def.len() + 16 * (deps.len() + 1));
            h.extend_from_slice(&SEMANTICS.to_le_bytes());
            h.extend_from_slice(def.as_bytes());
            for &d in &deps {
                h.push(0);
                h.extend_from_slice(set.rules[d].name.as_bytes());
                h.extend_from_slice(&set.rules[d].ver.to_le_bytes());
            }
            let ver = xxhash_rust::xxh3::xxh3_64(&h);
            set.rules.push(Rule { name, def, ver, deps, inputs, reads_output });
        }
        let mut h = Vec::new();
        for r in &set.rules {
            h.extend_from_slice(r.name.as_bytes());
            h.push(0);
            h.extend_from_slice(&r.ver.to_le_bytes());
        }
        set.id = xxhash_rust::xxh3::xxh3_64(&h);
        // a rule's name hides the input field of that name, so no rule can read that field
        for r in &set.rules {
            if let Some(f) = r.inputs.iter().find(|f| set.position(f.as_bytes()).is_some()) {
                return Err(QError::new(format!("rule '{}' reads input field '{f}', which rule '{f}' hides; define '{f}' first to read the rule, or give it another name", r.name)));
            }
        }
        Ok(set)
    }

    /// The rules' tests and expressions, parsed once per thread (they hold values that
    /// cannot be shared between threads).
    fn parsed(&self) -> std::rc::Rc<Vec<Parsed>> {
        PARSED.with(|c| {
            if let Some((id, p)) = &*c.borrow()
                && *id == self.id
            {
                return p.clone();
            }
            let p: Vec<Parsed> = self
                .rules
                .iter()
                .map(|r| {
                    let (_, on, expr) = parse::parse_rule(&r.def).expect("a rule that parsed when it was defined");
                    Parsed { on, expr }
                })
                .collect();
            let p = std::rc::Rc::new(p);
            *c.borrow_mut() = Some((self.id, p.clone()));
            p
        })
    }

    fn defs(&self) -> Vec<String> {
        self.rules.iter().map(|r| r.def.clone()).collect()
    }

    /// The set with `def` added, or put in place of the rule of that name (the rules that
    /// read it then get new versions too).
    pub fn with(&self, def: &str) -> std::result::Result<RuleSet, QError> {
        let (name, _, _) = parse::parse_rule(def.trim())?;
        let mut defs = self.defs();
        match self.position(name.as_bytes()) {
            Some(i) => defs[i] = def.to_string(),
            None => defs.push(def.to_string()),
        }
        RuleSet::build(&defs)
    }

    /// The set without rule `name`; refused while another rule reads it.
    pub fn without(&self, name: &str) -> std::result::Result<RuleSet, QError> {
        let Some(i) = self.position(name.as_bytes()) else { return Err(QError::new(format!("no rule '{name}'"))) };
        if let Some(r) = self.rules.iter().find(|r| r.deps.contains(&i)) {
            return Err(QError::new(format!("rule '{}' reads '{name}'", r.name)));
        }
        let mut defs = self.defs();
        defs.remove(i);
        RuleSet::build(&defs)
    }

    /// The value of rule `i` for a step, as queries will see it, given those of the rules
    /// before it (`vals[..i]`).
    pub fn eval(&self, i: usize, view: &dyn StepView, vals: &[Value]) -> Value {
        let parsed = self.parsed();
        let r = &parsed[i];
        let ctx = RuleCtx { set: self, upto: i, view, vals };
        if let Some(on) = &r.on {
            match value::eval(on, &ctx) {
                Ok(v) if v.truthy() => {}
                _ => return Value::Null,
            }
        }
        match value::eval(&r.expr, &ctx).ok().as_ref().and_then(stored) {
            Some(t) => Value::from_json(&t),
            None => Value::Null,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.raw(MAGIC);
        e.var(self.rules.len() as u64);
        for r in &self.rules {
            e.bytes(r.def.as_bytes());
        }
        let crc = crc32c::crc32c(&e.buf);
        e.raw(&crc.to_le_bytes());
        e.buf
    }

    pub fn decode(b: &[u8]) -> Result<RuleSet> {
        if b.len() < MAGIC.len() + 4 || &b[..MAGIC.len()] != MAGIC {
            return corrupt("RULES is not a rules file");
        }
        let (body, crc) = b.split_at(b.len() - 4);
        if crc32c::crc32c(body).to_le_bytes() != crc {
            return corrupt("RULES fails its checksum");
        }
        let mut d = Dec::new(&body[MAGIC.len()..]);
        let mut defs = Vec::new();
        for _ in 0..d.var()? {
            defs.push(d.string()?);
        }
        if !d.done() {
            return corrupt("RULES has trailing bytes");
        }
        match RuleSet::build(&defs) {
            Ok(s) => Ok(s),
            Err(e) => corrupt(format!("RULES holds a rule that does not read: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Step {
        tool: &'static str,
        failed: bool,
        command: Option<&'static str>,
        output: Option<&'static str>,
    }

    impl StepView for Step {
        fn tool(&self) -> Value {
            Value::Str(self.tool.into())
        }
        fn failed(&self) -> bool {
            self.failed
        }
        fn out_bytes(&self) -> u32 {
            self.output.map_or(0, |o| o.len() as u32)
        }
        fn input(&self, name: &str) -> Value {
            match (name, self.command) {
                ("command", Some(c)) => Value::Str(c.into()),
                _ => Value::Null,
            }
        }
        fn output(&self) -> Value {
            self.output.map_or(Value::Null, |o| Value::Str(o.into()))
        }
    }

    fn all(set: &RuleSet, s: &Step) -> Vec<Value> {
        let mut vals = Vec::new();
        for i in 0..set.rules.len() {
            let v = set.eval(i, s, &vals);
            vals.push(v);
        }
        vals
    }

    fn design() -> RuleSet {
        RuleSet::build(&[
            r#".create column program on tool == "Bash" = extract(command, @"^\s*(\S+)")"#.to_string(),
            r#".create column tests_failed on program in ("pytest", "npm") = toint(extract(output, @"(\d+) failed"))"#.to_string(),
            r#"broken = failed or tests_failed > 0"#.to_string(),
        ])
        .unwrap()
    }

    #[test]
    fn the_design_rules() {
        let set = design();
        assert_eq!(set.rules[1].deps, vec![0]);
        assert_eq!(set.rules[2].deps, vec![1]);
        assert!(set.rules[1].reads_output && !set.rules[0].reads_output);
        assert_eq!(set.rules[0].inputs, vec!["command".to_string()]);
        let v = all(&set, &Step { tool: "Bash", failed: false, command: Some("  pytest -q tests/"), output: Some("3 failed, 10 passed") });
        assert_eq!(v, vec![Value::Str("pytest".into()), Value::Int(3), Value::Bool(true)]);
        let v = all(&set, &Step { tool: "Bash", failed: false, command: Some("ls -la"), output: Some("2 failed") });
        assert_eq!(v, vec![Value::Str("ls".into()), Value::Null, Value::Bool(false)]);
        let v = all(&set, &Step { tool: "Read", failed: true, command: None, output: None });
        assert_eq!(v, vec![Value::Null, Value::Null, Value::Bool(true)]);
    }

    #[test]
    fn versions_follow_what_a_rule_reads() {
        let a = design();
        let b = a.with(r#"program on tool == "Bash" = tolower(extract(command, @"^\s*(\S+)"))"#).unwrap();
        assert_eq!(b.rules.len(), 3);
        assert_ne!(a.rules[0].ver, b.rules[0].ver);
        // the rules that read it change with it
        assert_ne!(a.rules[1].ver, b.rules[1].ver);
        assert_ne!(a.rules[2].ver, b.rules[2].ver);
        let c = a.with("size = out_bytes / 1024").unwrap();
        assert_eq!(a.versions(), c.versions()[..3].to_vec());
        assert_eq!(RuleSet::decode(&c.encode()).unwrap().versions(), c.versions());
        assert!(c.without("program").is_err());
        assert_eq!(c.without("size").unwrap().versions(), a.versions());
    }

    #[test]
    fn what_is_refused() {
        let bad = |d: &str| RuleSet::build(&[d.to_string()]).is_err();
        assert!(bad("tool = 1"));
        assert!(bad("x = now()"));
        assert!(bad("x = time"));
        assert!(bad("x = x"));
        assert!(bad("x = a.tool"));
        assert!(bad(r#"x = extract(command, "(")"#));
        assert!(bad(r#"x = extract(command, command)"#));
        assert!(bad("x = "));
        // an input field a later rule would hide
        assert!(RuleSet::build(&["x = y".to_string(), "y = 1".to_string()]).is_err());
        assert!(RuleSet::build(&["y = 1".to_string(), "x = input.y".to_string()]).is_err());
        assert_eq!(RuleSet::build(&["y = 1".to_string(), "x = y + input.z".to_string()]).unwrap().rules[1].inputs, vec!["z".to_string()]);
        assert!(RuleSet::build(&["x = 1".to_string(), "x = 2".to_string()]).is_err());
    }

    #[test]
    fn stored_values() {
        assert_eq!(stored(&Value::Null), None);
        assert_eq!(stored(&Value::Str("a\"b\n".into())).unwrap(), br#""a\"b\n""#);
        assert_eq!(stored(&Value::Float(f64::NAN)), None);
        assert_eq!(Value::from_json(&stored(&Value::Float(2.0)).unwrap()), Value::Int(2));
        let long = "é".repeat(400);
        let Value::Str(cut) = Value::from_json(&stored(&Value::Str(long.as_str().into())).unwrap()) else { panic!() };
        assert_eq!(cut.len(), DERIVED_MAX);
        let ctl = "a\u{1}b";
        assert_eq!(Value::from_json(&stored(&Value::Str(ctl.into())).unwrap()), Value::Str(ctl.into()));
    }
}
