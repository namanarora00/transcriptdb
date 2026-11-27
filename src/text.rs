//! Searchable text: what the user wrote and what the assistant said.
//!
//! A *text unit* is one text block of a message: a `{"type":"text","text":"..."}` element
//! of its `content` array, or the whole `content` when that is a string. Tool calls and
//! tool results are not text units: a call's inputs are searched through its step's
//! fields, and tool outputs are stored but never searched.
//!
//! A session's units are numbered in node order, then block order.

use crate::json::{self, Span};
use crate::query::value::json_unescape;

/// A text block found in a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSpan {
    /// Index of the content block (0 when `content` is a string).
    pub block: u16,
    /// The string's bytes between its quotes, still JSON-escaped.
    pub span: Span,
}

/// The role of a message and its text blocks: user or model, in any of the shapes model
/// APIs use. `None` when the bytes are not a message from the user or the model (no role,
/// or a system, developer or tool message).
///
/// Text blocks are `content` as a string, or its elements of type `text` (Anthropic, OpenAI
/// Chat Completions), `input_text` or `output_text` (OpenAI Responses), each with a `text`
/// string; and Gemini's `parts` that hold a `text` string (thoughts left out). The model's
/// role is `assistant`, or `model` in Gemini.
pub fn scan_text(b: &[u8]) -> Option<(bool, Vec<TextSpan>)> {
    let ms = json::members(b, &(0..b.len())).ok()?;
    let role = json::find_member(b, &ms, "role").filter(|r| b[r.value.start] == b'"')?;
    let user = match json::inner(b, &role.value) {
        b"user" => true,
        b"assistant" | b"model" => false,
        _ => return None,
    };
    let mut out = Vec::new();
    let text_of = |bm: &[json::Member]| json::find_member(b, bm, "text").filter(|x| b[x.value.start] == b'"').map(|x| x.value.start + 1..x.value.end - 1);
    if let Some(c) = json::find_member(b, &ms, "content") {
        if b[c.value.start] == b'"' {
            out.push(TextSpan { block: 0, span: c.value.start + 1..c.value.end - 1 });
        } else if b[c.value.start] == b'['
            && let Ok((els, _)) = json::elements(b, &c.value)
        {
            for (k, el) in els.iter().enumerate() {
                if b[el.start] != b'{' {
                    continue;
                }
                let Ok(bm) = json::members(b, el) else { continue };
                let Some(t) = json::find_member(b, &bm, "type") else { continue };
                if b[t.value.start] != b'"' || !matches!(json::inner(b, &t.value), b"text" | b"input_text" | b"output_text") {
                    continue;
                }
                if let Some(span) = text_of(&bm) {
                    out.push(TextSpan { block: k as u16, span });
                }
            }
        }
    }
    if let Some(p) = json::find_member(b, &ms, "parts")
        && b[p.value.start] == b'['
        && let Ok((els, _)) = json::elements(b, &p.value)
    {
        for (k, el) in els.iter().enumerate() {
            if b[el.start] != b'{' {
                continue;
            }
            let Ok(pm) = json::members(b, el) else { continue };
            if json::find_member(b, &pm, "thought").is_some_and(|t| &b[t.value.clone()] == b"true") {
                continue;
            }
            if let Some(span) = text_of(&pm) {
                out.push(TextSpan { block: k as u16, span });
            }
        }
    }
    Some((user, out))
}

/// The text of a unit as the user or the model wrote it.
pub fn unescape(raw: &[u8]) -> String {
    if raw.contains(&b'\\') { json_unescape(raw) } else { String::from_utf8_lossy(raw).into_owned() }
}

/// One text unit of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unit {
    pub node: u32,
    pub block: u16,
    pub user: bool,
    /// Where its text starts in [`SessionTexts::text`].
    pub off: u32,
    pub len: u32,
}

/// Every text unit of a session with its text, in node order then block order.
#[derive(Debug, Default, Clone)]
pub struct SessionTexts {
    pub units: Vec<Unit>,
    pub text: String,
    /// Time of the request that first carried each node (ms since the epoch, 0 unknown),
    /// indexed by node.
    pub node_time: Vec<i64>,
}

impl SessionTexts {
    pub fn text_of(&self, u: usize) -> &str {
        let x = &self.units[u];
        &self.text[x.off as usize..(x.off + x.len) as usize]
    }

    /// Add the units of node `node`, whose message is `b`.
    pub fn push_message(&mut self, node: u32, b: &[u8]) {
        let Some((user, spans)) = scan_text(b) else { return };
        for t in spans {
            let off = self.text.len() as u32;
            self.text.push_str(&unescape(&b[t.span.clone()]));
            self.units.push(Unit { node, block: t.block, user, off, len: self.text.len() as u32 - off });
        }
    }

    pub fn bytes(&self) -> usize {
        self.units.len() * std::mem::size_of::<Unit>() + self.text.len() + self.node_time.len() * 8 + 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_text_blocks_only() {
        let m = br#"{"role":"assistant","content":[{"type":"text","text":"Let me \"check\"\nthat."},{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}},{"type":"text","text":"done"}]}"#;
        let (user, spans) = scan_text(m).unwrap();
        assert!(!user);
        assert_eq!(spans.iter().map(|s| s.block).collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(unescape(&m[spans[0].span.clone()]), "Let me \"check\"\nthat.");
        assert_eq!(unescape(&m[spans[1].span.clone()]), "done");
    }

    #[test]
    fn string_content_and_tool_results() {
        let m = br#"{"role":"user","content":"fix the build"}"#;
        let (user, spans) = scan_text(m).unwrap();
        assert!(user);
        assert_eq!(spans.len(), 1);
        assert_eq!(&m[spans[0].span.clone()], b"fix the build");
        let r = br#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"secret output"},{"type":"text","text":"and now?"}]}"#;
        let (_, spans) = scan_text(r).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].block, 1);
        assert_eq!(&r[spans[0].span.clone()], b"and now?");
    }

    #[test]
    fn other_apis() {
        let texts = |m: &[u8]| -> Option<(bool, Vec<String>)> { scan_text(m).map(|(u, sp)| (u, sp.iter().map(|s| unescape(&m[s.span.clone()])).collect())) };
        // OpenAI Chat Completions: system and tool messages are not text units
        assert_eq!(texts(br#"{"role":"system","content":"be terse"}"#), None);
        assert_eq!(texts(br#"{"role":"tool","tool_call_id":"c","content":"out"}"#), None);
        assert_eq!(texts(br#"{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"x"}}]}"#), Some((true, vec!["hi".into()])));
        assert_eq!(texts(br#"{"role":"assistant","content":null,"tool_calls":[]}"#), Some((false, vec![])));
        // OpenAI Responses
        assert_eq!(texts(br#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}"#), Some((false, vec!["done".into()])));
        assert_eq!(texts(br#"{"role":"user","content":[{"type":"input_text","text":"go"}]}"#), Some((true, vec!["go".into()])));
        assert_eq!(texts(br#"{"type":"function_call","call_id":"c","name":"f","arguments":"{}"}"#), None);
        // Gemini: model is the assistant; thoughts are left out
        assert_eq!(texts(br#"{"role":"model","parts":[{"text":"plan","thought":true},{"text":"answer"},{"functionCall":{"name":"f","args":{}}}]}"#), Some((false, vec!["answer".into()])));
        assert_eq!(texts(br#"{"role":"user","parts":[{"text":"why?"}]}"#), Some((true, vec!["why?".into()])));
    }

    #[test]
    fn session_texts_number_units_in_order() {
        let mut st = SessionTexts::default();
        st.push_message(0, br#"{"role":"user","content":"one"}"#);
        st.push_message(1, br#"{"role":"assistant","content":[{"type":"text","text":"two"},{"type":"text","text":"three"}]}"#);
        assert_eq!(st.units.len(), 3);
        assert_eq!((st.text_of(0), st.text_of(1), st.text_of(2)), ("one", "two", "three"));
        assert!(st.units[0].user && !st.units[1].user);
        assert_eq!(st.units[2], Unit { node: 1, block: 1, user: false, off: 6, len: 5 });
    }
}
