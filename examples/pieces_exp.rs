//! Experiment: how small can the shared pieces (skeletons, system prompts, tool lists,
//! markers) get with random access? Replays the flush's choice (each new piece compressed
//! against one of the last N pieces of its kind whose chain is shorter than D, when that at
//! least halves it) for several N and D, and prints the floor: each kind's pieces as one
//! stream (no random access). Sizes only.
//!
//!   cargo run --release --example pieces_exp -- STORE

use transcriptdb::blob::pack_piece;
use transcriptdb::{Db, Options};

fn main() {
    let store = std::env::args().nth(1).expect("store dir");
    let db = Db::open(&store, Options::default()).expect("open");
    let mut pieces: Vec<(u8, Vec<u8>)> = Vec::new();
    db.each_piece(|_, kind, b, _, _| pieces.push((kind, b.to_vec()))).expect("pieces");
    let raw: usize = pieces.iter().map(|p| p.1.len()).sum();
    println!("{} pieces, {:.2} MB raw", pieces.len(), raw as f64 / 1e6);
    for kind in 1..=4u8 {
        let s: Vec<u8> = pieces.iter().filter(|p| p.0 == kind).flat_map(|p| p.1.clone()).collect();
        if !s.is_empty() {
            let mut c = zstd::bulk::Compressor::new(19).unwrap();
            c.set_parameter(zstd::zstd_safe::CParameter::EnableLongDistanceMatching(true)).unwrap();
            c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(27)).unwrap();
            println!("  kind {kind}: {:.2} MB raw, as one stream {:.3} MB", s.len() as f64 / 1e6, c.compress(&s).unwrap().len() as f64 / 1e6);
        }
    }
    for (n, d) in [(4usize, 8u32), (8, 8), (16, 8), (16, 16), (32, 32), (64, 64), (100_000, 100_000)] {
        // per kind: recent (id, depth)
        let mut bases: Vec<Vec<(u32, u32)>> = vec![Vec::new(); 8];
        let (mut total, mut deltas, mut max_depth) = (0usize, 0usize, 0u32);
        for (i, (kind, b)) in pieces.iter().enumerate() {
            let ids = &mut bases[*kind as usize];
            let cand: Vec<(u32, &[u8])> = ids.iter().filter(|x| x.1 < d).map(|x| (x.0, &pieces[x.0 as usize].1[..])).collect();
            let (base, c) = pack_piece(b, &cand, 19).unwrap();
            total += c.len();
            let depth = if base == 0 { 0 } else { ids.iter().find(|x| x.0 == base - 1).map_or(d, |x| x.1 + 1) };
            deltas += (base != 0) as usize;
            max_depth = max_depth.max(depth);
            ids.push((i as u32, depth));
            if ids.len() > n {
                ids.remove(0);
            }
        }
        println!("last {n:>6} pieces, chains under {d:>6}: {:.3} MB ({deltas} against another piece, longest chain {max_depth})", total as f64 / 1e6);
    }
}
