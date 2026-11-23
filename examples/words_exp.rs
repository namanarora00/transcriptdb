//! Experiment: what would the word index of the step summaries cost under other layouts?
//! For each input field: the steps that have it, their distinct texts, and the size of the
//! postings (delta varints, zstd 19) when ids are steps or distinct texts, with words held by
//! more than a share of the ids left out, and with long values' words left out; the word
//! list plain and front-coded. Sizes and counts only.
//!
//!   cargo run --release --example words_exp -- STORE [FIELD ...]

use std::collections::HashMap;
use transcriptdb::stats::{index_text, words};
use transcriptdb::{steps, Db, Options};

fn var(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn z(b: &[u8]) -> usize {
    zstd::bulk::compress(b, 19).unwrap().len()
}

/// (postings bytes, word list bytes, front-coded word list bytes, words kept) for texts given
/// as (id, word list), leaving out words held by more than `n_ids / cut` ids (cut 0: none).
fn index(texts: &[(u32, &Vec<Vec<u8>>)], n_ids: usize, cut: usize) -> (usize, usize, usize, usize) {
    let mut post: HashMap<&[u8], Vec<u32>> = HashMap::new();
    for (id, ws) in texts {
        for w in ws.iter() {
            post.entry(w).or_default().push(*id);
        }
    }
    let mut ws: Vec<(&[u8], Vec<u32>)> = post.into_iter().collect();
    ws.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let (mut pe, mut we, mut fe) = (Vec::new(), Vec::new(), Vec::new());
    let mut prev_w: &[u8] = b"";
    for (w, ids) in &mut ws {
        ids.sort_unstable();
        ids.dedup();
        let at = pe.len();
        if cut == 0 || ids.len() <= n_ids / cut {
            let mut prev = 0u32;
            for (j, &id) in ids.iter().enumerate() {
                var(&mut pe, if j == 0 { id as u64 } else { (id - prev) as u64 });
                prev = id;
            }
        }
        var(&mut we, w.len() as u64);
        we.extend_from_slice(w);
        var(&mut we, (pe.len() - at) as u64);
        let common = prev_w.iter().zip(w.iter()).take_while(|(a, b)| a == b).count();
        var(&mut fe, common as u64);
        var(&mut fe, (w.len() - common) as u64);
        fe.extend_from_slice(&w[common..]);
        var(&mut fe, (pe.len() - at) as u64);
        prev_w = w;
    }
    (z(&pe), z(&we), z(&fe), ws.len())
}

fn main() {
    let mut args = std::env::args().skip(1);
    let store = std::path::PathBuf::from(args.next().expect("store dir"));
    let only: Vec<String> = args.collect();
    let db = Db::open(&store, Options::default()).expect("open");
    let sd = db.step_dicts();
    // per field: every step's word list (None: not a string), whether long, and its text id
    struct F {
        steps: Vec<(u32, bool)>,
        texts: Vec<Vec<Vec<u8>>>,
        ix: HashMap<Vec<u8>, u32>,
        bytes: usize,
    }
    let mut fields: HashMap<u32, F> = HashMap::new();
    for s in 0..db.session_count() as u32 {
        let ss = db.steps(s);
        for (i, r) in ss.rows.iter().enumerate() {
            for &(f, v) in ss.fields_of(i as u32) {
                let long = steps::is_long(v);
                let text: Vec<u8> = if long {
                    let (off, len) = ss.long_at(v);
                    match db.stored_range(s, r.call_node, off, len).expect("read value") {
                        Some(b) => b,
                        None => continue,
                    }
                } else {
                    ss.value(f, v).to_vec()
                };
                let e = fields.entry(f).or_insert_with(|| F { steps: Vec::new(), texts: Vec::new(), ix: HashMap::new(), bytes: 0 });
                let id = match e.ix.get(&text) {
                    Some(&id) => id,
                    None => {
                        let mut ws: Vec<Vec<u8>> = Vec::new();
                        if let Some(t) = index_text(&text) {
                            words(&t, |w| ws.push(w.to_vec()));
                        }
                        ws.sort_unstable();
                        ws.dedup();
                        e.bytes += text.len();
                        e.texts.push(ws);
                        e.ix.insert(text, e.texts.len() as u32 - 1);
                        e.texts.len() as u32 - 1
                    }
                };
                e.steps.push((id, long));
            }
        }
    }
    let mut fs: Vec<(u32, F)> = fields.into_iter().collect();
    fs.sort_by_key(|x| std::cmp::Reverse(x.1.steps.len()));
    let mut sum = [0usize; 6];
    for (f, e) in &fs {
        let name = String::from_utf8_lossy(sd.fields.get(*f)).into_owned();
        if !only.is_empty() && !only.contains(&name) {
            continue;
        }
        let n_long = e.steps.iter().filter(|x| x.1).count();
        let mut long_text = vec![false; e.texts.len()];
        for (id, l) in &e.steps {
            long_text[*id as usize] |= *l;
        }
        let by_step: Vec<(u32, &Vec<Vec<u8>>)> = e.steps.iter().enumerate().map(|(i, (id, _))| (i as u32, &e.texts[*id as usize])).collect();
        let by_text: Vec<(u32, &Vec<Vec<u8>>)> = e.texts.iter().enumerate().map(|(i, ws)| (i as u32, ws)).collect();
        let short_only: Vec<(u32, &Vec<Vec<u8>>)> = by_text.iter().filter(|(i, _)| !long_text[*i as usize]).cloned().collect();
        let a = index(&by_step, by_step.len(), 0);
        let b = index(&by_text, by_text.len(), 0);
        let c = index(&by_text, by_text.len(), 16);
        let d = index(&by_text, by_text.len(), 64);
        let g = index(&short_only, by_text.len(), 16);
        println!(
            "{name}: {} steps, {} distinct texts ({:.2} MB), {} long steps | postings by step {} by text {} by text without words in over 1/16 {} over 1/64 {} same without long values {} | words {} ({} bytes) front-coded {} without long values {} ({} bytes front-coded)",
            e.steps.len(), e.texts.len(), e.bytes as f64 / 1e6, n_long, a.0, b.0, c.0, d.0, g.0, a.3, a.1, a.2, g.3, g.2
        );
        for (i, x) in [a.0, b.0, c.0, g.0, a.1, a.2].iter().enumerate() {
            sum[i] += x;
        }
    }
    println!("all fields: postings by step {} by text {} without words in over 1/16 {} and without long values {} | words {} front-coded {}", sum[0], sum[1], sum[2], sum[3], sum[4], sum[5]);
}
