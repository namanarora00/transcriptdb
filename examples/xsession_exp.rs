//! Experiment: what would sharing messages across sessions save? Each session's messages as
//! stored (in storage order) are compressed in 64 KB frames at level 19 with a 1 MB
//! dictionary trained the way a merge trains it (A); then with every message an earlier
//! session already has left out of the frames (B, as if it were a reference to the earlier
//! copy); then with all messages in the frames but the messages two or more sessions hold
//! appended to the trained dictionary, capped at MB megabytes (C). Prints sizes and decode
//! times only.
//!
//!   cargo run --release --example xsession_exp -- STORE [MB,MB,...]

use std::collections::HashMap;
use std::time::Instant;
use transcriptdb::{Db, Options, ReadStats};
use xxhash_rust::xxh3::xxh3_128;
use zstd::dict::{DecoderDictionary, EncoderDictionary};

const FRAME: usize = 64 << 10;

fn frames(stream: &[u8]) -> impl Iterator<Item = &[u8]> {
    stream.chunks(FRAME)
}

fn compress(streams: &[Vec<u8>], dict: &[u8]) -> (usize, Vec<Vec<u8>>) {
    let cd = EncoderDictionary::copy(dict, 19);
    let mut c = zstd::bulk::Compressor::with_prepared_dictionary(&cd).unwrap();
    let mut out = Vec::new();
    let mut total = 0;
    for s in streams {
        for f in frames(s) {
            let z = c.compress(f).unwrap();
            total += z.len();
            out.push(z);
        }
    }
    (total, out)
}

/// Seconds to decode every frame once, per frame.
fn decode_time(fs: &[Vec<u8>], dict: &[u8]) -> f64 {
    let dd = DecoderDictionary::copy(dict);
    let mut d = zstd::bulk::Decompressor::with_prepared_dictionary(&dd).unwrap();
    let t = Instant::now();
    for f in fs {
        std::hint::black_box(d.decompress(f, FRAME).unwrap());
    }
    t.elapsed().as_secs_f64() / fs.len().max(1) as f64
}

fn train(streams: &[Vec<u8>], size: usize) -> Vec<u8> {
    // samples spread over the input, about 100 times the dictionary in all (as Db::train_dict)
    let total: usize = streams.iter().map(|s| s.len()).sum();
    let want = (size * 100).min(total);
    let stride = (total / want.max(1)).max(1);
    let (mut buf, mut sizes) = (Vec::new(), Vec::new());
    let mut k = 0usize;
    for s in streams {
        for f in frames(s) {
            if k.is_multiple_of(stride) {
                buf.extend_from_slice(f);
                sizes.push(f.len());
            }
            k += 1;
        }
    }
    zstd::dict::from_continuous(&buf, &sizes, size).unwrap_or_default()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let store = args.next().expect("store dir");
    let caps: Vec<usize> = args.next().unwrap_or_else(|| "1,2,4,8".into()).split(',').map(|x| x.parse().unwrap()).collect();
    let db = Db::open(&store, Options::default()).expect("open");
    let n = db.session_count() as u32;
    // per session: (key, stored bytes) in storage order
    let mut sess: Vec<Vec<(u128, Vec<u8>)>> = Vec::with_capacity(n as usize);
    let mut holders: HashMap<u128, u32> = HashMap::new();
    for s in 0..n {
        let nodes = db.stored_nodes(s).expect("session");
        let ids: Vec<u32> = nodes.iter().map(|x| x.0).collect();
        let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).expect("messages");
        let mut v = Vec::with_capacity(nodes.len());
        let mut mine = std::collections::HashSet::new();
        for (nd, m) in nodes.into_iter().zip(msgs) {
            let k = xxh3_128(&m);
            if mine.insert(k) {
                *holders.entry(k).or_default() += 1;
            }
            v.push((k, nd.2));
        }
        sess.push(v);
    }
    let mut all: Vec<Vec<u8>> = Vec::new();
    let mut dedup: Vec<Vec<u8>> = Vec::new();
    let mut seen: std::collections::HashSet<u128> = Default::default();
    let mut shared: Vec<(u128, &[u8])> = Vec::new();
    let (mut dup_nodes, mut dup_bytes, mut nodes, mut bytes) = (0usize, 0usize, 0usize, 0usize);
    for v in &sess {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        let mut mine = std::collections::HashSet::new();
        for (k, m) in v {
            nodes += 1;
            bytes += m.len();
            a.extend_from_slice(m);
            if seen.contains(k) && !mine.contains(k) {
                dup_nodes += 1;
                dup_bytes += m.len();
            } else {
                if !seen.contains(k) && holders[k] >= 2 {
                    shared.push((*k, m));
                }
                b.extend_from_slice(m);
            }
            seen.insert(*k);
            mine.insert(*k);
        }
        all.push(a);
        dedup.push(b);
    }
    println!("{n} sessions, {nodes} stored messages, {:.2} MB; held by an earlier session too: {dup_nodes} messages, {:.2} MB ({:.1}%)", bytes as f64 / 1e6, dup_bytes as f64 / 1e6, dup_bytes as f64 * 100.0 / bytes as f64);
    let shared_bytes: usize = shared.iter().map(|x| x.1.len()).sum();
    println!("distinct messages two or more sessions hold: {}, {:.2} MB", shared.len(), shared_bytes as f64 / 1e6);
    let t = Instant::now();
    let dict = train(&all, 1 << 20);
    println!("dictionary {:.2} MB trained in {:.1}s", dict.len() as f64 / 1e6, t.elapsed().as_secs_f64());
    let dz = |d: &[u8]| zstd::bulk::compress(d, 19).unwrap().len();
    let (a, fa) = compress(&all, &dict);
    println!("A all messages:         frames {:.2} MB + dictionary {:.2} MB; decode {:.1} us/frame", a as f64 / 1e6, dz(&dict) as f64 / 1e6, decode_time(&fa, &dict) * 1e6);
    let (b, _) = compress(&dedup, &dict);
    println!("B shared ones left out: frames {:.2} MB + dictionary {:.2} MB + references (about {:.2} MB raw at 6 B each)", b as f64 / 1e6, dz(&dict) as f64 / 1e6, dup_nodes as f64 * 6.0 / 1e6);
    // largest first: biggest saving per dictionary byte is a message many sessions hold
    let mut by_gain: Vec<&(u128, &[u8])> = shared.iter().collect();
    by_gain.sort_by_key(|x| std::cmp::Reverse((holders[&x.0] as usize - 1) * x.1.len()));
    // control: as many bytes of messages only one session holds, spread over all of them
    let single: Vec<&[u8]> = sess.iter().flat_map(|v| v.iter()).filter(|(k, _)| holders[k] == 1).map(|(_, m)| &m[..]).collect();
    for cap in caps {
        let mut d2 = dict.clone();
        let mut extra = Vec::new();
        if cap == 0 {
            let step = (single.len() / 2000).max(1);
            for m in single.iter().step_by(step) {
                if extra.len() + m.len() <= shared_bytes {
                    extra.extend_from_slice(m);
                }
            }
            print!("(control, messages one session holds) ");
        }
        if cap >= 100 {
            // cap - 100 MB of messages sampled evenly over all sessions, whatever they hold
            let want = (cap - 100) << 20;
            let step = (bytes / want.max(1)).max(1);
            let (mut acc, mut next) = (0usize, 0usize);
            for v in &sess {
                for (_, m) in v {
                    if acc >= next && extra.len() + m.len() <= want {
                        extra.extend_from_slice(m);
                        next = acc + step.saturating_sub(1) * m.len().max(1);
                    }
                    acc += m.len();
                }
            }
            print!("(messages sampled evenly) ");
        }
        for (_, m) in by_gain.iter().filter(|_| cap > 0 && cap < 100) {
            if extra.len() + m.len() > cap << 20 {
                continue;
            }
            extra.extend_from_slice(m);
        }
        // content nearest the end of a dictionary is the cheapest to point at
        d2.extend_from_slice(&extra);
        let t = Instant::now();
        let (c, fc) = compress(&all, &d2);
        let ct = t.elapsed().as_secs_f64();
        println!(
            "C dictionary + {:.2} MB of shared messages: frames {:.2} MB + dictionary {:.2} MB = {:.2} MB (A: {:.2}); compress {ct:.1}s; decode {:.1} us/frame",
            extra.len() as f64 / 1e6,
            c as f64 / 1e6,
            dz(&d2) as f64 / 1e6,
            (c + dz(&d2)) as f64 / 1e6,
            (a + dz(&dict)) as f64 / 1e6,
            decode_time(&fc, &d2) * 1e6
        );
    }
}
