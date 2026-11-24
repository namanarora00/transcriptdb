//! Experiment: how much searchable text does a store hold, and what would indexing it cost?
//! Walks every stored message and splits its bytes into user text, assistant text, tool
//! inputs, tool outputs and the rest. For the searchable kinds (user text, assistant text,
//! tool inputs) it reports units, sizes, what a second compressed copy would cost, and the
//! size of word and trigram postings at several granularities. Sizes and counts only.
//!
//!   cargo run --release --example text_exp -- STORE

use std::collections::{HashMap, HashSet};
use transcriptdb::query::value::json_unescape;
use transcriptdb::{json, Db, Options, ReadStats};

#[derive(Default, Clone, Copy)]
struct Kind {
    units: u64,
    raw: u64,
    text: u64,
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn z19(b: &[u8]) -> usize {
    b.chunks(1 << 20).map(|c| zstd::bulk::compress(c, 19).unwrap().len()).sum()
}

fn grouped(texts: &[String], group: usize) -> usize {
    let mut c = zstd::bulk::Compressor::new(19).unwrap();
    let (mut total, mut buf) = (0, Vec::new());
    for t in texts {
        buf.extend_from_slice(t.as_bytes());
        if buf.len() >= group {
            total += c.compress(&buf).unwrap().len();
            buf.clear();
        }
    }
    if !buf.is_empty() {
        total += c.compress(&buf).unwrap().len();
    }
    total
}

fn word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b >= 0x80
}

/// Postings (key -> ascending ids) as delta varints, then zstd 19.
fn postings_size(p: &HashMap<u64, Vec<u32>>) -> (usize, usize, u64) {
    let mut keys: Vec<&u64> = p.keys().collect();
    keys.sort_unstable();
    let (mut buf, mut n) = (Vec::new(), 0u64);
    for k in keys {
        let v = &p[k];
        varint(v.len() as u64, &mut buf);
        let mut prev = 0;
        for &id in v {
            varint((id - prev) as u64, &mut buf);
            prev = id;
        }
        n += v.len() as u64;
    }
    (buf.len(), z19(&buf), n)
}

fn main() {
    let store = std::env::args().nth(1).expect("store dir");
    let db = Db::open(&store, Options::default()).expect("open");
    let (mut user, mut asst, mut input, mut output) = (Kind::default(), Kind::default(), Kind::default(), Kind::default());
    let mut total = 0u64;
    let mut texts: [Vec<String>; 3] = Default::default(); // user, assistant, inputs (lower-cased)
    let mut sess_of: [Vec<u32>; 3] = Default::default();
    let mut msg_of: [Vec<u32>; 3] = Default::default();
    let mut n_msgs = 0u32;
    for s in 0..db.session_count() as u32 {
        let ids: Vec<u32> = (0..db.session_len(s)).collect();
        let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).expect("read");
        for m in &msgs {
            total += m.len() as u64;
            n_msgs += 1;
            let b = &m[..];
            let Ok(ms) = json::members(b, &(0..b.len())) else { continue };
            let role = json::find_member(b, &ms, "role").map(|r| json::inner(b, &r.value).to_vec()).unwrap_or_default();
            let Some(c) = json::find_member(b, &ms, "content") else { continue };
            let is_user = role == b"user";
            let mut add = |k: usize, kind: &mut Kind, raw: &[u8], is_str: bool| {
                let t = if is_str { json_unescape(raw) } else { String::from_utf8_lossy(raw).into_owned() };
                kind.units += 1;
                kind.raw += raw.len() as u64;
                kind.text += t.len() as u64;
                texts[k].push(if t.is_ascii() { t.to_ascii_lowercase() } else { t.to_lowercase() });
                sess_of[k].push(s);
                msg_of[k].push(n_msgs - 1);
            };
            if b[c.value.start] == b'"' {
                let raw = json::inner(b, &c.value);
                if is_user { add(0, &mut user, raw, true) } else { add(1, &mut asst, raw, true) }
                continue;
            }
            let Ok((els, _)) = json::elements(b, &c.value) else { continue };
            for el in &els {
                if b[el.start] != b'{' {
                    continue;
                }
                let Ok(bm) = json::members(b, el) else { continue };
                let ty = json::find_member(b, &bm, "type").map(|t| json::inner(b, &t.value)).unwrap_or(b"");
                match ty {
                    b"text" => {
                        if let Some(t) = json::find_member(b, &bm, "text").filter(|t| b[t.value.start] == b'"') {
                            let raw = json::inner(b, &t.value);
                            if is_user { add(0, &mut user, raw, true) } else { add(1, &mut asst, raw, true) }
                        }
                    }
                    b"tool_use" => {
                        if let Some(inp) = json::find_member(b, &bm, "input").filter(|i| b[i.value.start] == b'{')
                            && let Ok(im) = json::members(b, &inp.value)
                        {
                            for f in im {
                                let v = &b[f.value.clone()];
                                if v.first() == Some(&b'"') {
                                    add(2, &mut input, json::inner(b, &f.value), true)
                                } else {
                                    add(2, &mut input, v, false)
                                }
                            }
                        }
                    }
                    b"tool_result" => {
                        if let Some(o) = json::find_member(b, &bm, "content") {
                            output.units += 1;
                            output.raw += o.value.len() as u64;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let mb = |v: u64| format!("{:.2} MB", v as f64 / 1e6);
    println!("{} sessions, {} messages, {} of stored message bytes", db.session_count(), n_msgs, mb(total));
    for (name, k) in [("user text", user), ("assistant text", asst), ("tool inputs", input)] {
        println!("  {name}: {} units, {} as JSON, {} as text", k.units, mb(k.raw), mb(k.text));
    }
    println!("  tool outputs: {} units, {} as JSON (not searched)", output.units, mb(output.raw));
    println!("  everything else (structure, ids, names): {}", mb(total - user.raw - asst.raw - input.raw - output.raw));

    for (k, name) in ["user text", "assistant text", "tool inputs"].iter().enumerate() {
        let t = &texts[k];
        let mut seen: HashSet<u128> = HashSet::new();
        let mut distinct: Vec<String> = Vec::new();
        let mut lens: Vec<usize> = Vec::with_capacity(t.len());
        for x in t {
            lens.push(x.len());
            if seen.insert(xxhash_rust::xxh3::xxh3_128(x.as_bytes())) {
                distinct.push(x.clone());
            }
        }
        lens.sort_unstable();
        let q = |p: f64| lens[((lens.len() - 1) as f64 * p) as usize];
        let dbytes: usize = distinct.iter().map(|x| x.len()).sum();
        println!("== {name}: {} units, {} distinct ({}); unit bytes p50 {} p95 {} p99 {} max {}; over 4 KB: {} units", t.len(), distinct.len(), mb(dbytes as u64), q(0.5), q(0.95), q(0.99), lens[lens.len() - 1], lens.iter().filter(|&&l| l > 4096).count());
        println!("   a second copy of the distinct text, zstd 19 in 64 KB groups: {}; in 1 MB groups: {}", mb(grouped(&distinct, 64 << 10) as u64), mb(grouped(&distinct, 1 << 20) as u64));

        // word postings by unit, by message and by session; trigram postings the same
        let mut words: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut word_bytes: HashMap<u64, usize> = HashMap::new();
        let mut by: [HashMap<u64, Vec<u32>>; 3] = Default::default(); // word by message, word by session, trigram by session
        let mut tri_unit: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut tri_msg: HashMap<u64, Vec<u32>> = HashMap::new();
        for (u, x) in t.iter().enumerate() {
            let b = x.as_bytes();
            let (mut i, mut ws): (usize, HashSet<u64>) = (0, HashSet::new());
            while i < b.len() {
                if !word_byte(b[i]) {
                    i += 1;
                    continue;
                }
                let a = i;
                while i < b.len() && word_byte(b[i]) {
                    i += 1;
                }
                if i - a >= 2 && i - a <= 64 {
                    let h = xxhash_rust::xxh3::xxh3_64(&b[a..i]);
                    word_bytes.entry(h).or_insert(i - a);
                    ws.insert(h);
                }
            }
            let mut tris: HashSet<u64> = HashSet::new();
            for w in b.windows(3) {
                tris.insert(w[0] as u64 | (w[1] as u64) << 8 | (w[2] as u64) << 16);
            }
            let push = |m: &mut HashMap<u64, Vec<u32>>, k: u64, id: u32| {
                let v = m.entry(k).or_default();
                if v.last() != Some(&id) {
                    v.push(id);
                }
            };
            for &w in &ws {
                push(&mut words, w, u as u32);
                push(&mut by[0], w, msg_of[k][u]);
                push(&mut by[1], w, sess_of[k][u]);
            }
            for &g in &tris {
                push(&mut tri_unit, g, u as u32);
                push(&mut tri_msg, g, msg_of[k][u]);
                push(&mut by[2], g, sess_of[k][u]);
            }
        }
        let dict: usize = word_bytes.values().map(|l| l + 1).sum();
        println!("   words: {} distinct, word list {} raw", words.len(), mb(dict as u64));
        for (label, p) in [("word -> unit", &words), ("word -> message", &by[0]), ("word -> session", &by[1]), ("trigram -> unit", &tri_unit), ("trigram -> message", &tri_msg), ("trigram -> session", &by[2])] {
            let (raw, z, n) = postings_size(p);
            println!("   {label}: {} keys, {n} postings, {} raw, {} zstd 19", p.len(), mb(raw as u64), mb(z as u64));
        }
        // a Bloom filter of trigrams per session at 10 bits per distinct trigram
        let mut per_sess: HashMap<u32, u64> = HashMap::new();
        for v in by[2].values() {
            for &s in v {
                *per_sess.entry(s).or_default() += 1;
            }
        }
        let bits: u64 = per_sess.values().map(|n| n * 10).sum();
        println!("   trigram Bloom filters, one per session at 10 bits per distinct trigram: {}", mb(bits / 8));
    }
}
