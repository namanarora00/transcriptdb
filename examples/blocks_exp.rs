//! Experiment: how small can segment blocks get? Re-encodes every part of every segment of
//! a store by rows (the encoding before 2026-11-23) and by columns (the one now used),
//! groups parts into ~64 KB blocks as the writer does, compresses at a level, with and
//! without the segment's frame dictionary, and prints sizes only.
//!
//!   cargo run --release --example blocks_exp -- STORE [level]

use transcriptdb::codec::Enc;
use transcriptdb::segment::{decode_core, decode_steps, CorePart, Segment, StepPart};

fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn steps_rows(p: &StepPart) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(p.first as u64);
    e.var(p.values.len() as u64);
    for (f, v) in &p.values {
        e.var(*f as u64);
        e.bytes(v);
    }
    e.var(p.rows.len() as u64);
    for (r, fs) in p.rows.iter().zip(&p.fields) {
        e.var(r.call_node as u64);
        e.var(r.call_block as u64);
        e.var(r.result_node as u64);
        e.var(r.result_block as u64);
        e.var(if r.prev == u32::MAX { 0 } else { r.prev as u64 + 1 });
        e.var(r.tool as u64);
        e.u8(r.failed as u8);
        e.var(r.out_bytes as u64);
        e.var(r.time_ms.max(0) as u64);
        e.var(fs.len() as u64);
        for &(f, v) in fs {
            e.var(f as u64);
            e.var(v as u64);
        }
    }
    e.buf
}

fn steps_cols(p: &StepPart) -> Vec<u8> {
    let mut cols: Vec<Enc> = (0..14).map(|_| Enc::new()).collect();
    cols[0].var(p.first as u64);
    cols[0].var(p.values.len() as u64);
    cols[0].var(p.rows.len() as u64);
    for (f, v) in &p.values {
        cols[1].var(*f as u64);
        cols[2].var(v.len() as u64);
        cols[3].raw(v);
    }
    let (mut pc, mut pt) = (0i64, 0i64);
    for (i, (r, fs)) in p.rows.iter().zip(&p.fields).enumerate() {
        cols[4].var(zz(r.call_node as i64 - pc));
        pc = r.call_node as i64;
        cols[5].var(r.call_block as u64);
        cols[6].var(zz(r.result_node as i64 - r.call_node as i64));
        cols[5].var(r.result_block as u64);
        cols[7].var(if r.prev == u32::MAX { 0 } else { (p.first as u64 + i as u64) - r.prev as u64 });
        cols[8].u8((r.tool as u8) << 1 | r.failed as u8);
        cols[9].var(r.out_bytes as u64);
        cols[10].var(zz(r.time_ms - pt));
        pt = r.time_ms;
        cols[11].var(fs.len() as u64);
        for &(f, v) in fs {
            cols[12].var(f as u64);
            cols[13].var(v as u64);
        }
    }
    cols.into_iter().flat_map(|c| c.buf).collect()
}

fn core_rows(p: &CorePart) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(p.raw_len);
    e.var(p.frames.len() as u64);
    for &(o, l) in &p.frames {
        e.var(o);
        e.var(l as u64);
    }
    e.var(p.first_node as u64);
    e.var(p.nodes.len() as u64);
    for n in &p.nodes {
        e.var(n.id as u64);
        e.var(if n.parent == u32::MAX { 0 } else { n.parent as u64 + 1 });

        e.var(n.off as u64);
        e.var(n.len as u64);
        e.var(n.holes.len() as u64);
        for &(o, id) in &n.holes {
            e.var(o as u64);
            e.var(id);
        }
    }
    e.var(p.first_req as u64);
    e.var(p.requests.len() as u64);
    for r in &p.requests {
        r.encode(&mut e);
    }
    e.buf
}

/// Columns; `off` left out (each node starts where the one before it in storage order ends),
/// frame offsets as lengths, ids and parents as differences, request fields in columns.
fn core_cols(p: &CorePart, with_hash: bool) -> Vec<u8> {
    let mut c: Vec<Enc> = (0..20).map(|_| Enc::new()).collect();
    c[0].var(p.raw_len);
    c[0].var(p.frames.len() as u64);
    c[0].var(p.first_node as u64);
    c[0].var(p.nodes.len() as u64);
    c[0].var(p.first_req as u64);
    c[0].var(p.requests.len() as u64);
    if let Some(f) = p.frames.first() {
        c[0].var(f.0);
    }
    for &(_, l) in &p.frames {
        c[1].var(l as u64);
    }
    let mut prev_id = p.first_node as i64 - 1;
    for n in &p.nodes {
        c[2].var(zz(n.id as i64 - prev_id - 1));
        prev_id = n.id as i64;
        c[3].var(if n.parent == u32::MAX { 0 } else { zz(n.id as i64 - n.parent as i64) + 1 });
        let _ = with_hash;
        c[5].var(n.len as u64);
        c[6].var(n.holes.len() as u64);
        let mut po = 0u32;
        for &(o, id) in &n.holes {
            c[7].var((o - po) as u64);
            po = o;
            c[8].var(id);
        }
    }
    let mut pt = 0i64;
    for r in &p.requests {
        c[9].bytes(r.id.as_bytes());
        let t = transcriptdb::steps::parse_time_ms(&r.time);
        c[10].var(zz(t - pt));
        pt = t;
        c[10].bytes(r.time.as_bytes().get(24..).unwrap_or(b""));
        c[11].bytes(&r.meta);
        c[12].var(zz(r.tip as i64 - prev_id));
        c[12].var(r.n_msgs as u64);
        c[13].var(r.skeleton as u64);
        c[13].var(r.system as u64);
        c[13].var(r.tools as u64);
        c[14].var(r.notes.len() as u64);
        for n in r.notes.iter() {
            c[14].var(n.target as u64);
            c[14].var(n.offset as u64);
            c[14].var(n.text as u64);
        }
        match &r.layout {
            None => c[16].u8(0),
            Some(g) => {
                c[16].u8(1);
                c[16].var(g.len() as u64);
                for x in g.iter() {
                    c[16].bytes(x);
                }
            }
        }
    }
    c.into_iter().flat_map(|e| e.buf).collect()
}

fn grouped(parts: &[Vec<u8>], level: i32) -> usize {
    grouped_dict(parts, level, &[])
}

fn grouped_dict(parts: &[Vec<u8>], level: i32, dict: &[u8]) -> usize {
    let mut c = if dict.is_empty() { zstd::bulk::Compressor::new(level).unwrap() } else { zstd::bulk::Compressor::with_dictionary(level, dict).unwrap() };
    let (mut total, mut buf) = (0, Vec::new());
    for p in parts {
        buf.extend_from_slice(p);
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
    let mut args = std::env::args().skip(1);
    let store = std::path::PathBuf::from(args.next().expect("store dir"));
    let level: i32 = args.next().map(|l| l.parse().unwrap()).unwrap_or(19);
    let (mut sr, mut sc, mut cr, mut cc, mut cch) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut hashes, mut digests, mut ids) = (0usize, 0usize, 0usize);
    let mut dict = Vec::new();
    for e in std::fs::read_dir(&store).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        let Some(num) = name.strip_prefix("seg-").and_then(|x| x.strip_suffix(".tdb")).and_then(|x| x.parse::<u32>().ok()) else { continue };
        let seg = Segment::open(&store, num).unwrap();
        if let Some(r) = &seg.cat.dict {
            dict = seg.block(r).unwrap();
        }
        for k in 0..seg.cat.dirs.len() {
            for d in seg.dir_block(k).unwrap() {
                let b = seg.block(&d.core.0).unwrap();
                let p = decode_core(&b[d.core.1 as usize..(d.core.1 + d.core.2) as usize]).unwrap();
                hashes += p.nodes.len() * 16;
                digests += p.requests.len() * 32;
                ids += p.requests.iter().map(|r| r.id.len()).sum::<usize>();
                cr.push(core_rows(&p));
                cc.push(core_cols(&p, true));
                cch.push(core_cols(&p, false));
                let b = seg.step_block(&d.steps.0).unwrap();
                let p = decode_steps(&b[d.steps.1 as usize..(d.steps.1 + d.steps.2) as usize], &seg.step_block(&d.vals.0).unwrap()[d.vals.1 as usize..(d.vals.1 + d.vals.2) as usize]).unwrap();
                sr.push(steps_rows(&p));
                sc.push(steps_cols(&p));
            }
        }
    }
    let raw = |v: &Vec<Vec<u8>>| v.iter().map(|x| x.len()).sum::<usize>() as f64 / 1e6;
    println!("parts: {} (level {level})", cr.len());
    println!("  messages and requests: rows {:.2} MB raw -> {:.2} MB; columns {:.2} -> {:.2} MB; columns without node hashes {:.2} -> {:.2} MB", raw(&cr), grouped(&cr, level) as f64 / 1e6, raw(&cc), grouped(&cc, level) as f64 / 1e6, raw(&cch), grouped(&cch, level) as f64 / 1e6);
    println!("    node hashes {:.2} MB, request digests {:.2} MB, request ids {:.2} MB raw", hashes as f64 / 1e6, digests as f64 / 1e6, ids as f64 / 1e6);
    println!("  steps: rows {:.2} MB raw -> {:.2} MB; columns {:.2} -> {:.2} MB", raw(&sr), grouped(&sr, level) as f64 / 1e6, raw(&sc), grouped(&sc, level) as f64 / 1e6);
    if !dict.is_empty() {
        println!("  with the segment's frame dictionary ({:.2} MB): messages and requests rows {:.2} MB, columns {:.2} MB; steps rows {:.2} MB, columns {:.2} MB", dict.len() as f64 / 1e6, grouped_dict(&cr, level, &dict) as f64 / 1e6, grouped_dict(&cc, level, &dict) as f64 / 1e6, grouped_dict(&sr, level, &dict) as f64 / 1e6, grouped_dict(&sc, level, &dict) as f64 / 1e6);
    }
}
