//! Experiment: what would it cost to copy step input values over 4 KB (now positions in the
//! call's message) into the step blocks? Reads every such value, groups them by session into
//! ~64 KB blocks like the writer does, and compresses them at level 19 with the segment's
//! dictionary: for `command` alone and for every field. Sizes only.
//!
//!   cargo run --release --example longs_exp -- STORE

use transcriptdb::segment::Segment;
use transcriptdb::{steps, Db, Options};

fn grouped(vals: &[Vec<u8>], dict: &[u8]) -> usize {
    let mut c = if dict.is_empty() { zstd::bulk::Compressor::new(19).unwrap() } else { zstd::bulk::Compressor::with_dictionary(19, dict).unwrap() };
    let (mut total, mut buf) = (0, Vec::new());
    for v in vals {
        buf.extend_from_slice(v);
        if buf.len() >= 64 << 10 {
            total += c.compress(&buf).unwrap().len();
            buf.clear();
        }
    }
    if !buf.is_empty() {
        total += c.compress(&buf).unwrap().len();
    }
    total
}

fn main() {
    let store = std::path::PathBuf::from(std::env::args().nth(1).expect("store dir"));
    let mut dict = Vec::new();
    for e in std::fs::read_dir(&store).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        if let Some(num) = name.strip_prefix("seg-").and_then(|x| x.strip_suffix(".tdb")).and_then(|x| x.parse::<u32>().ok()) {
            let seg = Segment::open(&store, num).unwrap();
            if let Some(r) = &seg.cat.dict {
                dict = seg.block(r).unwrap();
            }
        }
    }
    let db = Db::open(&store, Options::default()).expect("open");
    let sd = db.step_dicts();
    let (mut cmd, mut all) = (Vec::new(), Vec::new());
    for s in 0..db.session_count() as u32 {
        let ss = db.steps(s);
        for (i, r) in ss.rows.iter().enumerate() {
            for &(f, v) in ss.fields_of(i as u32) {
                if steps::is_long(v) {
                    let (off, len) = ss.long_at(v);
                    let Some(b) = db.stored_range(s, r.call_node, off, len).expect("read value") else { continue };
                    if sd.fields.get(f) == b"command" {
                        cmd.push(b.clone());
                    }
                    all.push(b);
                }
            }
        }
    }
    let raw = |v: &Vec<Vec<u8>>| v.iter().map(|x| x.len()).sum::<usize>() as f64 / 1e6;
    println!("values over 4 KB: {} ({:.2} MB raw), of which command {} ({:.2} MB)", all.len(), raw(&all), cmd.len(), raw(&cmd));
    println!("  copied into step blocks, level 19 with the segment's dictionary: command {:.2} MB; every field {:.2} MB", grouped(&cmd, &dict) as f64 / 1e6, grouped(&all, &dict) as f64 / 1e6);
}
