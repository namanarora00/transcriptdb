//! Splitting a model request body into the pieces the store keeps, and putting them back.
//!
//! The splitter reads the Anthropic Messages format: a JSON object with a `messages`
//! array, an optional `system` value and an optional `tools` array. Everything is
//! found by byte position and nothing is re-encoded, so [`assemble`] gives back the
//! exact input bytes, whatever their key order or whitespace.
//!
//! Three kinds of cuts are made:
//! * the `messages` array content, the `system` value and the `tools` value become
//!   holes in a *skeleton*, which holds the small rest of the body (model, params);
//! * every `cache_control` member on a content block (or a system/tool block) is cut
//!   out into a [`Note`], because the marker moves on every call and would otherwise
//!   make the same message look different from one request to the next;
//! * long base64 strings under `signature` or `data` keys (thinking signatures,
//!   images) are cut out of a message by [`cut_opaque`] into the opaque store.

use crate::codec::{corrupt, Dec, Enc, Error, Result};
use crate::json::{self, Member, Span};
use base64::Engine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HoleKind {
    Messages = 0,
    System = 1,
    Tools = 2,
}

impl HoleKind {
    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => HoleKind::Messages,
            1 => HoleKind::System,
            2 => HoleKind::Tools,
            _ => return corrupt("bad hole kind"),
        })
    }
}

/// Bytes cut out of a piece: insert `text` at `offset`, where `offset` counts in the
/// cut bytes. Notes are applied in order; several notes may share an offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub offset: u32,
    pub text: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    pub bytes: Vec<u8>,
    pub notes: Vec<Note>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skeleton {
    pub bytes: Vec<u8>,
    pub holes: Vec<(u32, HoleKind)>,
}

impl Skeleton {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.var(self.holes.len() as u64);
        for &(o, k) in &self.holes {
            e.var(o as u64);
            e.u8(k as u8);
        }
        e.raw(&self.bytes);
        e.buf
    }
    pub fn decode(b: &[u8]) -> Result<Skeleton> {
        let mut d = Dec::new(b);
        let n = d.var()? as usize;
        let mut holes = Vec::with_capacity(n);
        for _ in 0..n {
            let o = d.var32()?;
            let k = HoleKind::from_u8(d.u8()?)?;
            holes.push((o, k));
        }
        Ok(Skeleton { bytes: d.rest().to_vec(), holes })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    pub skeleton: Skeleton,
    pub messages: Vec<Piece>,
    /// The bytes around and between messages when they are not exactly `a,b,c`:
    /// `n + 1` gaps for `n` messages (leading, between each pair, trailing).
    pub layout: Option<Vec<Vec<u8>>>,
    pub system: Option<Piece>,
    pub tools: Option<Piece>,
}

const CACHE_KEY: &[u8] = b"\"cache_control\"";

/// The byte range to delete so that member `k` disappears from its object, with one
/// adjacent comma, leaving valid JSON.
fn member_cut(ms: &[Member], k: usize) -> Span {
    if k + 1 < ms.len() {
        ms[k].key.start..ms[k + 1].key.start
    } else if k > 0 {
        ms[k - 1].value.end..ms[k].value.end
    } else {
        ms[k].key.start..ms[k].value.end
    }
}

/// Ranges of `cache_control` members on the object elements of the array at `arr`.
fn cache_cuts_in_array(b: &[u8], arr: &Span, out: &mut Vec<Span>) -> Result<()> {
    if b.get(arr.start) != Some(&b'[') {
        return Ok(());
    }
    let (els, _) = json::elements(b, arr)?;
    for el in els {
        if b[el.start] != b'{' || memchr::memmem::find(&b[el.clone()], CACHE_KEY).is_none() {
            continue;
        }
        let ms = json::members(b, &el)?;
        if let Some(k) = ms.iter().position(|m| json::key_is(b, &m.key, "cache_control")) {
            out.push(member_cut(&ms, k));
        }
    }
    Ok(())
}

/// Remove `cuts` (sorted, non-overlapping, local to `b`) and describe them as notes.
fn apply_cuts(b: &[u8], cuts: &[Span]) -> Piece {
    if cuts.is_empty() {
        return Piece { bytes: b.to_vec(), notes: Vec::new() };
    }
    let mut bytes = Vec::with_capacity(b.len());
    let mut notes = Vec::with_capacity(cuts.len());
    let mut pos = 0;
    for c in cuts {
        bytes.extend_from_slice(&b[pos..c.start]);
        notes.push(Note { offset: bytes.len() as u32, text: b[c.clone()].to_vec() });
        pos = c.end;
    }
    bytes.extend_from_slice(&b[pos..]);
    Piece { bytes, notes }
}

/// Cut the cache markers out of one message (an object whose `content` is an array of blocks).
pub fn cut_message(m: &[u8]) -> Result<Piece> {
    if memchr::memmem::find(m, CACHE_KEY).is_none() || m.first() != Some(&b'{') {
        return Ok(Piece { bytes: m.to_vec(), notes: Vec::new() });
    }
    let ms = json::members(m, &(0..m.len()))?;
    let mut cuts = Vec::new();
    if let Some(c) = json::find_member(m, &ms, "content") {
        cache_cuts_in_array(m, &c.value, &mut cuts)?;
    }
    cuts.sort_by_key(|c| c.start);
    Ok(apply_cuts(m, &cuts))
}

/// Cut cache markers out of a `system` or `tools` value.
fn cut_blocks(v: &[u8]) -> Result<Piece> {
    let mut cuts = Vec::new();
    if memchr::memmem::find(v, CACHE_KEY).is_some() {
        cache_cuts_in_array(v, &(0..v.len()), &mut cuts)?;
    }
    cuts.sort_by_key(|c| c.start);
    Ok(apply_cuts(v, &cuts))
}

/// Split a request body. The body must be a JSON object.
pub fn split(body: &[u8]) -> Result<Split> {
    let start = json::skip_ws(body, 0);
    if body.get(start) != Some(&b'{') {
        return Err(Error::BadRequest("body is not a JSON object".into()));
    }
    let end = json::scan_value(body, start)?;
    if json::skip_ws(body, end) != body.len() {
        return Err(Error::BadRequest("trailing bytes after the body".into()));
    }
    let top = json::members(body, &(start..end))?;

    // (range removed from the body, hole kind)
    let mut cuts: Vec<(Span, HoleKind)> = Vec::new();
    let mut messages = Vec::new();
    let mut layout = None;
    let mut system = None;
    let mut tools = None;

    if let Some(m) = json::find_member(body, &top, "messages") {
        if body[m.value.start] == b'[' {
            let (els, close) = json::elements(body, &m.value)?;
            let interior = m.value.start + 1..close;
            // compact means exactly e0,e1,...,en with nothing else
            let mut compact = true;
            let mut pos = interior.start;
            for (k, e) in els.iter().enumerate() {
                let sep_ok = if k == 0 { e.start == pos } else { e.start == pos + 1 && body[pos] == b',' };
                if !sep_ok {
                    compact = false;
                    break;
                }
                pos = e.end;
            }
            if compact && pos != interior.end {
                compact = false;
            }
            if !compact {
                let mut gaps = Vec::with_capacity(els.len() + 1);
                let mut p = interior.start;
                for e in &els {
                    gaps.push(body[p..e.start].to_vec());
                    p = e.end;
                }
                gaps.push(body[p..interior.end].to_vec());
                layout = Some(gaps);
            }
            for e in &els {
                messages.push(cut_message(&body[e.clone()])?);
            }
            cuts.push((interior, HoleKind::Messages));
        }
    }
    if let Some(m) = json::find_member(body, &top, "system") {
        system = Some(cut_blocks(&body[m.value.clone()])?);
        cuts.push((m.value.clone(), HoleKind::System));
    }
    if let Some(m) = json::find_member(body, &top, "tools") {
        tools = Some(cut_blocks(&body[m.value.clone()])?);
        cuts.push((m.value.clone(), HoleKind::Tools));
    }
    cuts.sort_by_key(|c| c.0.start);

    let mut sk = Vec::with_capacity(1024);
    let mut holes = Vec::with_capacity(cuts.len());
    let mut pos = 0;
    for (r, k) in &cuts {
        sk.extend_from_slice(&body[pos..r.start]);
        holes.push((sk.len() as u32, *k));
        pos = r.end;
    }
    sk.extend_from_slice(&body[pos..]);
    Ok(Split { skeleton: Skeleton { bytes: sk, holes }, messages, layout, system, tools })
}

/// Append `cut` to `out` with `notes` put back.
pub fn apply_notes(cut: &[u8], notes: &[Note], out: &mut Vec<u8>) {
    let mut pos = 0usize;
    for n in notes {
        let o = n.offset as usize;
        out.extend_from_slice(&cut[pos..o]);
        out.extend_from_slice(&n.text);
        pos = o;
    }
    out.extend_from_slice(&cut[pos..]);
}

/// Write the body back. `fill` writes the content of each hole.
pub fn assemble(
    skel: &Skeleton,
    out: &mut Vec<u8>,
    mut fill: impl FnMut(HoleKind, &mut Vec<u8>) -> Result<()>,
) -> Result<()> {
    let mut pos = 0usize;
    for &(o, k) in &skel.holes {
        let o = o as usize;
        out.extend_from_slice(&skel.bytes[pos..o]);
        fill(k, out)?;
        pos = o;
    }
    out.extend_from_slice(&skel.bytes[pos..]);
    Ok(())
}

/// Rebuild a body from a [`Split`] (used by tests and by the writer's self-check).
pub fn join(s: &Split) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    assemble(&s.skeleton, &mut out, |k, out| {
        match k {
            HoleKind::Messages => {
                for (i, m) in s.messages.iter().enumerate() {
                    match &s.layout {
                        Some(g) => out.extend_from_slice(&g[i]),
                        None if i > 0 => out.push(b','),
                        None => {}
                    }
                    apply_notes(&m.bytes, &m.notes, out);
                }
                if let Some(g) = &s.layout {
                    out.extend_from_slice(&g[s.messages.len()]);
                }
            }
            HoleKind::System => {
                let p = s.system.as_ref().unwrap();
                apply_notes(&p.bytes, &p.notes, out);
            }
            HoleKind::Tools => {
                let p = s.tools.as_ref().unwrap();
                apply_notes(&p.bytes, &p.notes, out);
            }
        }
        Ok(())
    })?;
    Ok(out)
}

/// How an opaque value is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpaqueEnc {
    /// Decoded base64; re-encoding with the standard alphabet and padding gives the text back.
    Base64 = 0,
    /// The string content as it was.
    Text = 1,
}

#[derive(Debug, Clone)]
pub struct OpaqueHole {
    /// Offset in the cut message where the string content goes (between the quotes).
    pub offset: u32,
    pub enc: OpaqueEnc,
    pub payload: Vec<u8>,
}

const MIN_OPAQUE: usize = 64;

fn is_b64(s: &[u8]) -> bool {
    s.iter().all(|&c| c.is_ascii_alphanumeric() || c == b'+' || c == b'/' || c == b'=')
}

fn walk_opaque(b: &[u8], n: &json::Node, out: &mut Vec<Span>) {
    match n {
        json::Node::Obj(ms) => {
            for (k, v) in ms {
                if let json::Node::Tok(s) = v {
                    if b[s.start] == b'"'
                        && s.len() - 2 >= MIN_OPAQUE
                        && (json::key_is(b, k, "signature") || json::key_is(b, k, "data"))
                        && is_b64(json::inner(b, s))
                    {
                        out.push(s.start + 1..s.end - 1);
                        continue;
                    }
                }
                walk_opaque(b, v, out);
            }
        }
        json::Node::Arr(items) => {
            for it in items {
                walk_opaque(b, it, out);
            }
        }
        json::Node::Tok(_) => {}
    }
}

/// Cut thinking signatures and base64 images out of a message.
pub fn cut_opaque(m: &[u8]) -> Result<(Vec<u8>, Vec<OpaqueHole>)> {
    let f = memchr::memmem::find;
    if f(m, b"\"signature\"").is_none() && f(m, b"\"data\"").is_none() {
        return Ok((m.to_vec(), Vec::new()));
    }
    let node = json::parse_all(m)?;
    let mut spans = Vec::new();
    walk_opaque(m, &node, &mut spans);
    if spans.is_empty() {
        return Ok((m.to_vec(), Vec::new()));
    }
    spans.sort_by_key(|s| s.start);
    let eng = base64::engine::general_purpose::STANDARD;
    let mut bytes = Vec::with_capacity(m.len());
    let mut holes = Vec::with_capacity(spans.len());
    let mut pos = 0;
    for s in spans {
        bytes.extend_from_slice(&m[pos..s.start]);
        let text = &m[s.clone()];
        let (enc, payload) = match eng.decode(text) {
            Ok(raw) if eng.encode(&raw).as_bytes() == text => (OpaqueEnc::Base64, raw),
            _ => (OpaqueEnc::Text, text.to_vec()),
        };
        holes.push(OpaqueHole { offset: bytes.len() as u32, enc, payload });
        pos = s.end;
    }
    bytes.extend_from_slice(&m[pos..]);
    Ok((bytes, holes))
}

/// Write an opaque value back as the text it came from.
pub fn opaque_text(enc: OpaqueEnc, payload: &[u8], out: &mut Vec<u8>) {
    match enc {
        OpaqueEnc::Text => out.extend_from_slice(payload),
        OpaqueEnc::Base64 => {
            let eng = base64::engine::general_purpose::STANDARD;
            let start = out.len();
            out.resize(start + base64::encoded_len(payload.len(), true).unwrap(), 0);
            let n = eng.encode_slice(payload, &mut out[start..]).unwrap();
            out.truncate(start + n);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(body: &[u8]) -> Split {
        let s = split(body).unwrap();
        assert_eq!(join(&s).unwrap(), body.to_vec(), "roundtrip of {}", String::from_utf8_lossy(body));
        s
    }

    #[test]
    fn splits_compact_body() {
        let body = br#"{"max_tokens":5,"messages":[{"content":[{"cache_control":{"type":"ephemeral"},"text":"hi","type":"text"}],"role":"user"},{"content":"yo","role":"assistant"}],"model":"m","system":[{"cache_control":{"type":"ephemeral"},"text":"s","type":"text"}],"tools":[{"name":"t"}]}"#;
        let s = rt(body);
        assert_eq!(s.messages.len(), 2);
        assert_eq!(s.messages[0].bytes, br#"{"content":[{"text":"hi","type":"text"}],"role":"user"}"#.to_vec());
        assert_eq!(s.messages[0].notes.len(), 1);
        assert_eq!(s.system.as_ref().unwrap().bytes, br#"[{"text":"s","type":"text"}]"#.to_vec());
        assert!(s.layout.is_none());
        assert_eq!(s.skeleton.bytes, br#"{"max_tokens":5,"messages":[],"model":"m","system":,"tools":}"#.to_vec());
    }

    #[test]
    fn whitespace_and_marker_positions() {
        rt(br#"  { "messages" : [ {"role":"user", "content": [ {"type":"text","text":"a", "cache_control" : {"type":"ephemeral"} } ] } ,{"content":[{"cache_control":{}}]} ] , "model":"x" }  "#);
        rt(br#"{"messages":[]}"#);
        rt(br#"{"messages":[ ]}"#);
        rt(br#"{"model":"no messages"}"#);
        rt(br#"{"messages":"odd","system":"plain string"}"#);
    }

    #[test]
    fn opaque_cut_and_restore() {
        let sig = "QUJD".repeat(40);
        let img = "iVBORw0KGgo".repeat(10) + "=";
        let m = format!(r#"{{"content":[{{"signature":"{sig}","thinking":"x","type":"thinking"}},{{"source":{{"data":"{img}","media_type":"image/png","type":"base64"}},"type":"image"}},{{"data":"short","type":"t"}}],"role":"assistant"}}"#);
        let (cut, holes) = cut_opaque(m.as_bytes()).unwrap();
        assert_eq!(holes.len(), 2);
        assert_eq!(holes[0].enc, OpaqueEnc::Base64);
        assert_eq!(holes[1].enc, OpaqueEnc::Text); // not canonical base64 (bad padding)
        let mut out = Vec::new();
        let mut pos = 0;
        for h in &holes {
            out.extend_from_slice(&cut[pos..h.offset as usize]);
            opaque_text(h.enc, &h.payload, &mut out);
            pos = h.offset as usize;
        }
        out.extend_from_slice(&cut[pos..]);
        assert_eq!(out, m.as_bytes());
    }
}
