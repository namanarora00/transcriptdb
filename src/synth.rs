//! Synthetic agent sessions in the Anthropic Messages format, for tests and the crash test.
//!
//! Deterministic from a seed. Covers what real traffic does to the store: growing
//! contexts, a cache marker that moves to the newest message, side queries and
//! rewinds that fork the session tree, the same message twice in one request,
//! string and block content, thinking signatures and base64 images, escapes
//! (including lone surrogate escapes), non-ASCII text, subagents, and bodies with
//! whitespace.

use std::fmt::Write as _;

#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
}

pub struct SynthRequest {
    pub session: String,
    pub agent: String,
    pub id: String,
    pub time: String,
    pub body: Vec<u8>,
}

const WORDS: &[&str] = &[
    "the", "test", "fails", "because", "parser", "returns", "None", "when", "input", "is", "empty", "fix", "edit",
    "file", "src/lib.rs", "cargo", "build", "passed", "error[E0308]:", "mismatched", "types", "über", "naïve", "日本語",
    "emoji 🚀", "tab\there", "quote\"inside", "back\\slash", "line\nbreak", "ok", "done", "reading", "grep", "TODO",
];

const TOOLS: &[&str] = &["Bash", "Read", "Edit", "Grep", "Write", "Glob"];

/// JSON string literal for `s` (escapes like JavaScript's JSON.stringify).
pub fn jstr(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for ch in s.chars() {
        match ch {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            '\u{8}' => o.push_str("\\b"),
            '\u{c}' => o.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(o, "\\u{:04x}", c as u32);
            }
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn text(r: &mut Rng, words: usize) -> String {
    let mut s = String::new();
    for i in 0..words {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(r.pick(WORDS));
    }
    s
}

/// A JSON string literal, sometimes with a lone surrogate escape (as JavaScript writes them).
fn text_lit(r: &mut Rng, words: usize) -> String {
    let mut l = jstr(&text(r, words));
    if r.chance(0.03) {
        l.insert_str(l.len() - 1, "\\ud83d");
    }
    l
}

fn b64(r: &mut Rng, n: usize) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut raw = Vec::with_capacity(n);
    for _ in 0..n {
        raw.push(r.next() as u8);
    }
    let mut s = String::with_capacity(n * 4 / 3 + 4);
    for c in raw.chunks(3) {
        let v = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        s.push(A[(v >> 18) as usize & 63] as char);
        s.push(A[(v >> 12) as usize & 63] as char);
        s.push(if c.len() > 1 { A[(v >> 6) as usize & 63] as char } else { '=' });
        s.push(if c.len() > 2 { A[v as usize & 63] as char } else { '=' });
    }
    s
}

/// One message as its content blocks (each a JSON object without cache_control) and role.
#[derive(Clone)]
struct Msg {
    role: &'static str,
    /// `None` means string content.
    blocks: Option<Vec<String>>,
    string: String,
}

impl Msg {
    fn render(&self, marker: Option<&str>) -> String {
        match &self.blocks {
            None if marker.is_none() => format!(r#"{{"role":"{}","content":{}}}"#, self.role, self.string),
            None => {
                let m = marker.unwrap();
                format!(r#"{{"role":"{}","content":[{{"type":"text","text":{},"cache_control":{m}}}]}}"#, self.role, self.string)
            }
            Some(bs) => {
                let mut s = format!(r#"{{"role":"{}","content":["#, self.role);
                for (i, b) in bs.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    if i + 1 == bs.len()
                        && let Some(m) = marker {
                            s.push_str(&b[..b.len() - 1]);
                            let _ = write!(s, r#","cache_control":{m}}}"#);
                            continue;
                        }
                    s.push_str(b);
                }
                s.push_str("]}");
                s
            }
        }
    }
}

fn user_text(r: &mut Rng) -> Msg {
    if r.chance(0.5) {
        Msg { role: "user", blocks: None, string: { let n = 6 + r.below(30) as usize; text_lit(r, n) } }
    } else {
        let n = 1 + r.below(3) as usize;
        let bs = (0..n).map(|_| format!(r#"{{"type":"text","text":{}}}"#, { let n = 5 + r.below(40) as usize; text_lit(r, n) })).collect();
        Msg { role: "user", blocks: Some(bs), string: String::new() }
    }
}

fn assistant(r: &mut Rng, ids: &mut u64) -> (Msg, Vec<String>) {
    let mut bs = Vec::new();
    if r.chance(0.6) {
        bs.push(format!(r#"{{"type":"thinking","thinking":{},"signature":"{}"}}"#, { let n = 10 + r.below(60) as usize; text_lit(r, n) }, { let n = 200 + r.below(400) as usize; b64(r, n) }));
    }
    if r.chance(0.5) {
        bs.push(format!(r#"{{"type":"text","text":{}}}"#, { let n = 5 + r.below(30) as usize; text_lit(r, n) }));
    }
    let mut calls = Vec::new();
    let n = if r.chance(0.2) { 2 } else { 1 };
    for _ in 0..n {
        *ids += 1;
        let id = format!("toolu_{:016x}", *ids);
        let tool = *r.pick(TOOLS);
        let input = match tool {
            "Bash" => format!(r#"{{"command":{},"description":{}}}"#, jstr(&format!("cargo test {}", r.below(100))), text_lit(r, 4)),
            "Edit" => format!(r#"{{"file_path":"/repo/src/{}.rs","old_string":{},"new_string":{}}}"#, r.below(20), text_lit(r, 8), text_lit(r, 8)),
            _ => format!(r#"{{"file_path":"/repo/src/{}.rs","limit":{},"ratio":{}}}"#, r.below(20), r.below(500), "1.5e-3"),
        };
        bs.push(format!(r#"{{"type":"tool_use","id":"{id}","name":"{tool}","input":{input}}}"#));
        calls.push(id);
    }
    (Msg { role: "assistant", blocks: Some(bs), string: String::new() }, calls)
}

fn results(r: &mut Rng, calls: &[String]) -> Msg {
    let mut bs = Vec::new();
    for id in calls {
        let content = if r.chance(0.08) {
            format!(r#"[{{"type":"text","text":"screenshot"}},{{"type":"image","source":{{"type":"base64","media_type":"image/png","data":"{}"}}}}]"#, { let n = 2000 + r.below(6000) as usize; b64(r, n) })
        } else {
            { let n = 20 + r.below(400) as usize; text_lit(r, n) }
        };
        if r.chance(0.15) {
            bs.push(format!(r#"{{"type":"tool_result","tool_use_id":"{id}","content":{content},"is_error":true}}"#));
        } else {
            bs.push(format!(r#"{{"type":"tool_result","tool_use_id":"{id}","content":{content}}}"#));
        }
    }
    if r.chance(0.2) {
        bs.push(format!(r#"{{"type":"text","text":{}}}"#, jstr("<system-reminder>keep going</system-reminder>")));
    }
    Msg { role: "user", blocks: Some(bs), string: String::new() }
}

fn tools_json(r: &mut Rng, variant: u64) -> String {
    let mut s = String::from("[");
    for (i, t) in TOOLS.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(
            s,
            r#"{{"name":"{t}","description":{},"input_schema":{{"type":"object","properties":{{"file_path":{{"type":"string"}},"limit":{{"type":"integer","maximum":9007199254740991}}}},"required":["file_path"]}}}}"#,
            jstr(&format!("{t} tool, variant {variant}. {}", text(r, 40)))
        );
    }
    s.push(']');
    s
}

fn body(r: &mut Rng, model: &str, system: &str, tools: &str, msgs: &[Msg], pretty: bool) -> Vec<u8> {
    let marker = if r.chance(0.5) { r#"{"type":"ephemeral"}"# } else { r#"{"type":"ephemeral","ttl":"1h"}"# };
    let (sep, colon) = if pretty { (",\n  ", ": ") } else { (",", ":") };
    let mut s = String::new();
    let _ = write!(s, r#"{{"model"{colon}"{model}"{sep}"max_tokens"{colon}{}{sep}"messages"{colon}["#, 32000 + 1000 * r.below(3));
    for (i, m) in msgs.iter().enumerate() {
        if i > 0 {
            s.push_str(if pretty { ", " } else { "," });
        }
        let mk = if i + 1 == msgs.len() || (i + 3 == msgs.len() && r.chance(0.3)) { Some(marker) } else { None };
        s.push_str(&m.render(mk));
    }
    let _ = write!(s, r#"]{sep}"system"{colon}{system}{sep}"tools"{colon}{tools}{sep}"thinking"{colon}{{"type":"adaptive"}}}}"#);
    if pretty {
        s.push('\n');
    }
    s.into_bytes()
}

/// All requests of synthetic session `no`, in order.
pub fn session(seed: u64, no: u64, n_requests: usize) -> Vec<SynthRequest> {
    let mut r = Rng::new(seed.wrapping_mul(1_000_003).wrapping_add(no));
    let session = format!("{:08x}-synth-{no}", seed as u32);
    let agent = if r.chance(0.3) { format!("a{:016x}", r.next()) } else { String::new() };
    let model = *r.pick(&["model-large", "model-small"]);
    let system = format!(
        r#"[{{"type":"text","text":"You are an agent."}},{{"type":"text","text":{},"cache_control":{{"type":"ephemeral"}}}}]"#,
        jstr(&text(&mut r, 60))
    );
    let mut tools = tools_json(&mut r, 0);
    let mut msgs = vec![user_text(&mut r)];
    let mut ids = no << 24;
    let mut out = Vec::with_capacity(n_requests);
    let start = 1_763_000_000u64 + no * 10_000;
    while out.len() < n_requests {
        let k = out.len();
        let pretty = r.chance(0.04);
        // side query: same context plus one extra message, not continued
        if k > 2 && r.chance(0.08) {
            let mut side = msgs.clone();
            side.push(Msg { role: "user", blocks: None, string: jstr("Summarize the work so far in one line.") });
            out.push(req(&session, &agent, no, k, start, body(&mut r, model, &system, &tools, &side, pretty)));
            continue;
        }
        // rewind: drop the last few messages and go another way
        if msgs.len() > 6 && r.chance(0.04) {
            let keep = msgs.len() - 2 * (1 + r.below(2) as usize);
            msgs.truncate(keep.max(1));
            if msgs.last().map(|m| m.role) == Some("assistant") {
                msgs.push(user_text(&mut r));
            }
        }
        if k > 0 && r.chance(0.02) {
            tools = tools_json(&mut r, k as u64);
        }
        out.push(req(&session, &agent, no, k, start, body(&mut r, model, &system, &tools, &msgs, pretty)));
        // the model answers; tools run
        let (a, calls) = assistant(&mut r, &mut ids);
        msgs.push(a);
        if r.chance(0.05) {
            // the same short message twice in one context
            msgs.push(Msg { role: "user", blocks: None, string: jstr("continue") });
            msgs.push(Msg { role: "assistant", blocks: Some(vec![r#"{"type":"text","text":"ok"}"#.into()]), string: String::new() });
        }
        msgs.push(results(&mut r, &calls));
        if r.chance(0.1) {
            msgs.push(user_text(&mut r));
        }
    }
    out
}

fn req(session: &str, agent: &str, no: u64, k: usize, start: u64, body: Vec<u8>) -> SynthRequest {
    SynthRequest { session: session.into(), agent: agent.into(), id: format!("synth-{no}-{k}"), time: format!("{}", start + k as u64 * 7), body }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_are_valid_json_and_deterministic() {
        let a = session(1, 3, 40);
        let b = session(1, 3, 40);
        assert_eq!(a.len(), 40);
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.body, y.body);
            crate::json::parse_all(&x.body).unwrap();
        }
    }
}
