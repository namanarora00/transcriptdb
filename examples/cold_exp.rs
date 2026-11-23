//! Experiment: what would compressing cold sessions in bigger runs save, and what would a
//! read then cost? Each session's messages as stored (storage order) are compressed at level
//! 19 with a dictionary made the way a merge makes it (1 MB trained, then 8 MB of the
//! messages sampled evenly), cut into runs of 64 KB, 256 KB, 1 MB, 4 MB, or one run per
//! session. Prints sizes, compress time, decode time per run and what reading one message
//! of a session would decode; also session sizes, and messages another session also holds
//! left out at each run size. Sizes and times only.
//!
//!   cargo run --release --example cold_exp -- STORE

use std::collections::{HashMap, HashSet};
use std::time::Instant;
use transcriptdb::{Db, Options, ReadStats};
use xxhash_rust::xxh3::xxh3_128;
use zstd::dict::{DecoderDictionary, EncoderDictionary};

fn dictionary(streams: &[Vec<u8>]) -> Vec<u8> {
    let raw: usize = streams.iter().map(|s| s.len()).sum();
    let size = (raw / 100).min(1 << 20);
    let content = (raw / 12).min(8 << 20);
    let stride = (raw / (size * 100)).max(1);
    let fstep = (raw / content).max(1);
    let (mut data, mut sizes, mut extra) = (Vec::new(), Vec::new(), Vec::new());
    let mut k = 0usize;
    for s in streams {
        for f in s.chunks(64 << 10) {
            if k.is_multiple_of(stride) {
                data.extend_from_slice(f);
                sizes.push(f.len());
            }
            if k.is_multiple_of(fstep) && extra.len() + f.len() <= content {
                extra.extend_from_slice(f);
            }
            k += 1;
        }
    }
    let mut d = zstd::dict::from_continuous(&data, &sizes, size).unwrap_or_default();
    d.extend_from_slice(&extra);
    d
}

fn q(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn main() {
    let store = std::env::args().nth(1).expect("store dir");
    let db = Db::open(&store, Options::default()).expect("open");
    let n = db.session_count() as u32;
    let (mut all, mut dedup) = (Vec::new(), Vec::new());
    let mut seen: HashSet<u128> = HashSet::new();
    for s in 0..n {
        let nodes = db.stored_nodes(s).expect("session");
        let ids: Vec<u32> = nodes.iter().map(|x| x.0).collect();
        let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).expect("messages");
        let (mut a, mut b) = (Vec::new(), Vec::new());
        let mut mine = HashSet::new();
        for (nd, m) in nodes.into_iter().zip(msgs) {
            let k = xxh3_128(&m);
            a.extend_from_slice(&nd.2);
            if !(seen.contains(&k) && !mine.contains(&k)) {
                b.extend_from_slice(&nd.2);
            }
            mine.insert(k);
            seen.insert(k);
        }
        all.push(a);
        dedup.push(b);
    }
    let raw: usize = all.iter().map(|s| s.len()).sum();
    let mut sz: Vec<f64> = all.iter().map(|s| s.len() as f64 / 1e6).collect();
    println!(
        "{n} sessions, {:.1} MB of stored messages; session size median {:.2} MB, p90 {:.2}, p99 {:.2}, largest {:.2}",
        raw as f64 / 1e6,
        q(&mut sz.clone(), 0.5),
        q(&mut sz.clone(), 0.9),
        q(&mut sz.clone(), 0.99),
        q(&mut sz, 1.0)
    );
    let t = Instant::now();
    let dict = dictionary(&all);
    let dz = zstd::bulk::compress(&dict, 19).unwrap().len();
    println!("dictionary {:.2} MB ({:.2} MB stored), made in {:.1}s", dict.len() as f64 / 1e6, dz as f64 / 1e6, t.elapsed().as_secs_f64());
    let cd = EncoderDictionary::copy(&dict, 19);
    let dd = DecoderDictionary::copy(&dict);
    let mut runs_by: HashMap<usize, usize> = HashMap::new();
    for (label, run) in [("64 KB", 64usize << 10), ("256 KB", 256 << 10), ("1 MB", 1 << 20), ("4 MB", 4 << 20), ("whole session", usize::MAX)] {
        let mut c = zstd::bulk::Compressor::with_prepared_dictionary(&cd).unwrap();
        c.set_parameter(zstd::zstd_safe::CParameter::ForceAttachDict(zstd::zstd_safe::DictAttachPref::ForceAttach)).unwrap();
        let t = Instant::now();
        let mut packed: Vec<(Vec<u8>, usize)> = Vec::new();
        for s in &all {
            for r in s.chunks(run.min(s.len().max(1))) {
                packed.push((c.compress(r).unwrap(), r.len()));
            }
        }
        let ct = t.elapsed().as_secs_f64();
        let total: usize = packed.iter().map(|p| p.0.len()).sum();
        runs_by.insert(run, total);
        // decode every run once; reading one message decodes the run (or runs) holding it
        let mut d = zstd::bulk::Decompressor::with_prepared_dictionary(&dd).unwrap();
        let mut per: Vec<f64> = Vec::with_capacity(packed.len());
        for (z, len) in &packed {
            let t = Instant::now();
            std::hint::black_box(d.decompress(z, *len).unwrap());
            per.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let sum: f64 = per.iter().sum();
        // what a read of a random message decodes, weighting runs by their raw size
        let mut w: Vec<(f64, usize)> = per.iter().zip(&packed).map(|(t, p)| (*t, p.1)).collect();
        w.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let wsum: usize = w.iter().map(|x| x.1).sum();
        let at = |p: f64| {
            let mut acc = 0usize;
            for (t, l) in &w {
                acc += l;
                if acc as f64 >= p * wsum as f64 {
                    return *t;
                }
            }
            w.last().map_or(0.0, |x| x.0)
        };
        println!(
            "{label:>13}: {:.2} MB + dictionary {:.2} = {:.2} MB; compress {ct:.1}s; decode {:.0} MB/s; a random message's run decodes in median {:.2} ms, p90 {:.2}, p99 {:.2}, max {:.1}",
            total as f64 / 1e6,
            dz as f64 / 1e6,
            (total + dz) as f64 / 1e6,
            raw as f64 / 1e6 / (sum / 1e3),
            at(0.5),
            at(0.9),
            at(0.99),
            w.last().map_or(0.0, |x| x.0)
        );
    }
    for (label, run) in [("64 KB", 64usize << 10), ("whole session", usize::MAX)] {
        let mut c = zstd::bulk::Compressor::with_prepared_dictionary(&cd).unwrap();
        c.set_parameter(zstd::zstd_safe::CParameter::ForceAttachDict(zstd::zstd_safe::DictAttachPref::ForceAttach)).unwrap();
        let total: usize = dedup.iter().flat_map(|s| s.chunks(run.min(s.len().max(1))).map(|r| c.compress(r).unwrap().len()).collect::<Vec<_>>()).sum();
        println!("{label:>13}, messages an earlier session holds left out: {:.2} MB (all: {:.2})", total as f64 / 1e6, runs_by[&run] as f64 / 1e6);
    }
}
