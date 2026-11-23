//! Experiment: where a store's step-summary bytes are, and what compressing them harder
//! would save. For every segment with summaries, adds up the blocks of each field's values,
//! rows, words and postings: raw, on disk, at level 19, and at level 19 with the segment's
//! dictionary. Sizes only.
//!
//!   cargo run --release --example stats_exp -- STORE

use transcriptdb::segment::Segment;
use transcriptdb::{Db, Options};

fn main() {
    let store = std::path::PathBuf::from(std::env::args().nth(1).expect("store dir"));
    let db = Db::open(&store, Options::default()).expect("open");
    // raw, disk, level 19, level 19 + dictionary
    let mut tot = [[0u64; 4]; 4];
    let mut per: Vec<(u64, String)> = Vec::new();
    for (num, ..) in db.segments() {
        let Some(h) = db.stats_head(num).expect("head") else { continue };
        let head = h.head();
        let seg = Segment::open(&store, num).expect("segment");
        let dict = seg.cat.dict.as_ref().map(|r| seg.block(r).expect("dictionary")).unwrap_or_default();
        let mut plain = zstd::bulk::Compressor::new(19).unwrap();
        let mut with = if dict.is_empty() { zstd::bulk::Compressor::new(19).unwrap() } else { zstd::bulk::Compressor::with_dictionary(19, &dict).unwrap() };
        for f in &head.fields {
            let mut k = [[0u64; 4]; 4];
            let mut add = |i: usize, r: &transcriptdb::block::BlockRef| {
                let raw = seg.block(r).expect("block");
                k[i][0] += r.rawlen as u64;
                k[i][1] += r.clen as u64;
                k[i][2] += plain.compress(&raw).unwrap().len() as u64;
                k[i][3] += with.compress(&raw).unwrap().len() as u64;
            };
            for (_, r) in &f.chunks {
                add(0, r);
            }
            add(1, &f.rows);
            add(2, &f.words);
            add(3, &f.post);
            for i in 0..4 {
                for j in 0..4 {
                    tot[i][j] += k[i][j];
                }
            }
            let disk: u64 = k.iter().map(|x| x[1]).sum();
            per.push((disk, format!("field {} values {} listed {}: values {}/{} rows {}/{} words {}/{} postings {}/{} (raw/disk bytes)", db.seg_field_name(num, f.field), f.n_values, f.n_longs, k[0][0], k[0][1], k[1][0], k[1][1], k[2][0], k[2][1], k[3][0], k[3][1])));
        }
    }
    for (i, n) in ["values", "rows", "words", "postings"].iter().enumerate() {
        let m = |x: u64| x as f64 / 1e6;
        println!("{n}: raw {:.2} MB, on disk {:.2}, level 19 {:.2}, level 19 with the dictionary {:.2}", m(tot[i][0]), m(tot[i][1]), m(tot[i][2]), m(tot[i][3]));
    }
    per.sort_by_key(|x| std::cmp::Reverse(x.0));
    for (_, line) in per.iter().take(8) {
        println!("{line}");
    }
}
