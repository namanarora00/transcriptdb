//! Experiment: what fills the step blocks, and what would they cost if input values over a
//! threshold were kept as positions in the call's stored message (as values over 4 KB are
//! now) instead of copies? Prints, per input field name, the values stored and their bytes,
//! then step blocks re-encoded at each threshold, grouped into ~64 KB blocks like the writer
//! does and compressed at level 19 with and without the segment's frame dictionary. Sizes only.
//!
//!   cargo run --release --example steps_exp -- STORE [T,T,...]

use std::collections::HashMap;
use transcriptdb::codec::Enc;
use transcriptdb::segment::{decode_steps, Segment, StepPart};
use transcriptdb::steps;

fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// The column encoding the writer uses, with values longer than `t` turned into positions.
fn encode(p: &StepPart, t: usize) -> Vec<u8> {
    let mut c: Vec<Enc> = (0..12).map(|_| Enc::new()).collect();
    let keep: Vec<bool> = p.values.iter().map(|(_, v)| v.len() <= t).collect();
    let mut code = vec![u32::MAX; p.values.len()];
    let mut n = 0u32;
    for (i, k) in keep.iter().enumerate() {
        if *k {
            code[i] = n;
            n += 1;
        }
    }
    c[0].var(p.first as u64);
    c[0].var(n as u64);
    c[0].var(p.rows.len() as u64);
    for (i, (f, v)) in p.values.iter().enumerate() {
        if keep[i] {
            c[1].var(*f as u64);
            c[2].bytes(v);
        }
    }
    let (mut pc, mut pt) = (0i64, 0i64);
    for (i, (r, fs)) in p.rows.iter().zip(&p.fields).enumerate() {
        c[3].var(zz(r.call_node as i64 - pc));
        pc = r.call_node as i64;
        c[4].var(zz(r.result_node as i64 - r.call_node as i64));
        c[5].var(r.call_block as u64);
        c[5].var(r.result_block as u64);
        c[6].var(if r.prev == u32::MAX { 0 } else { zz(p.first as i64 + i as i64 - r.prev as i64) + 1 });
        c[7].var(r.tool as u64);
        c[7].u8(r.failed as u8);
        c[8].var(r.out_bytes as u64);
        c[9].var(zz(r.time_ms.max(0) - pt));
        pt = r.time_ms.max(0);
        c[10].var(fs.len() as u64);
        for &(f, v) in fs {
            c[10].var(f as u64);
            let pos = if steps::is_long(v) {
                Some(p.longs[(v - steps::LONG_BASE) as usize])
            } else if !keep[v as usize] {
                // a position in the call message: offsets of a few hundred bytes are typical
                Some((300, p.values[v as usize].1.len() as u32))
            } else {
                None
            };
            match pos {
                Some((off, len)) => {
                    c[11].var(0);
                    c[11].var(off as u64);
                    c[11].var(len as u64);
                }
                None => c[11].var(code[v as usize] as u64 + 1),
            }
        }
    }
    let mut out = Enc::new();
    for col in &c {
        out.var(col.buf.len() as u64);
    }
    for col in c {
        out.raw(&col.buf);
    }
    out.buf
}

fn grouped(parts: &[Vec<u8>], level: i32, dict: &[u8]) -> usize {
    grouped_by(parts, level, dict, 64 << 10)
}

fn grouped_by(parts: &[Vec<u8>], level: i32, dict: &[u8], block: usize) -> usize {
    let mut c = if dict.is_empty() { zstd::bulk::Compressor::new(level).unwrap() } else { zstd::bulk::Compressor::with_dictionary(level, dict).unwrap() };
    let (mut total, mut buf) = (0, Vec::new());
    for p in parts {
        buf.extend_from_slice(p);
        if buf.len() >= block {
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
    let ts: Vec<usize> = args.next().unwrap_or_else(|| "4096,2048,1024,512,256,128".into()).split(',').map(|x| x.parse().unwrap()).collect();
    let mut parts: Vec<StepPart> = Vec::new();
    let mut dict = Vec::new();
    let mut by_field: HashMap<Vec<u8>, [u64; 6]> = HashMap::new(); // values, bytes, >256 B bytes, >1 KB bytes, long refs, long bytes
    for e in std::fs::read_dir(&store).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        let Some(num) = name.strip_prefix("seg-").and_then(|x| x.strip_suffix(".tdb")).and_then(|x| x.parse::<u32>().ok()) else { continue };
        let seg = Segment::open(&store, num).unwrap();
        if let Some(r) = &seg.cat.dict {
            dict = seg.block(r).unwrap();
        }
        for k in 0..seg.cat.dirs.len() {
            for d in seg.dir_block(k).unwrap() {
                let b = seg.step_block(&d.steps.0).unwrap();
                let p = decode_steps(&b[d.steps.1 as usize..(d.steps.1 + d.steps.2) as usize]).unwrap();
                for (f, v) in &p.values {
                    let e = by_field.entry(seg.cat.fields[*f as usize].clone()).or_default();
                    e[0] += 1;
                    e[1] += v.len() as u64;
                    e[2] += if v.len() > 256 { v.len() as u64 } else { 0 };
                    e[3] += if v.len() > 1024 { v.len() as u64 } else { 0 };
                }
                for fs in &p.fields {
                    for &(f, v) in fs {
                        if steps::is_long(v) {
                            let e = by_field.entry(seg.cat.fields[f as usize].clone()).or_default();
                            e[4] += 1;
                            e[5] += p.longs[(v - steps::LONG_BASE) as usize].1 as u64;
                        }
                    }
                }
                parts.push(p);
            }
        }
    }
    let mut v: Vec<_> = by_field.into_iter().collect();
    v.sort_by_key(|x| std::cmp::Reverse(x.1[1]));
    let total: u64 = v.iter().map(|x| x.1[1]).sum();
    println!("{} step parts, {} rows; stored input values {:.2} MB", parts.len(), parts.iter().map(|p| p.rows.len()).sum::<usize>(), total as f64 / 1e6);
    println!("  {:<22} {:>8} {:>9} {:>9} {:>9} {:>7} {:>9}", "field", "values", "MB", ">256B MB", ">1KB MB", "over 4K", "their MB");
    for (f, e) in v.iter().take(16) {
        println!("  {:<22} {:>8} {:>9.2} {:>9.2} {:>9.2} {:>7} {:>9.2}", String::from_utf8_lossy(f), e[0], e[1] as f64 / 1e6, e[2] as f64 / 1e6, e[3] as f64 / 1e6, e[4], e[5] as f64 / 1e6);
    }
    // the writer's blocks as they are, with other codecs
    let enc: Vec<Vec<u8>> = parts.iter().map(|p| encode(p, 4096)).collect();
    // each part's values sorted by (field, value), codes renumbered: similar values side by side
    let sorted: Vec<Vec<u8>> = parts
        .iter()
        .map(|p| {
            let mut order: Vec<usize> = (0..p.values.len()).collect();
            order.sort_by(|&a, &b| p.values[a].cmp(&p.values[b]));
            let mut new_code = vec![0u32; p.values.len()];
            for (n, &o) in order.iter().enumerate() {
                new_code[o] = n as u32;
            }
            let mut q = p.clone();
            q.values = order.iter().map(|&o| p.values[o].clone()).collect();
            for fs in q.fields.iter_mut() {
                for fv in fs.iter_mut() {
                    if !steps::is_long(fv.1) {
                        fv.1 = new_code[fv.1 as usize];
                    }
                }
            }
            encode(&q, 4096)
        })
        .collect();
    println!("values sorted by (field, value): {:.2} MB at level 19, {:.2} MB with the frame dictionary (as written: {:.2} / {:.2})", grouped(&sorted, 19, &[]) as f64 / 1e6, grouped(&sorted, 19, &dict) as f64 / 1e6, grouped(&enc, 19, &[]) as f64 / 1e6, grouped(&enc, 19, &dict) as f64 / 1e6);
    for kb in [128usize, 256, 512] {
        println!("blocks of {kb} KB: {:.2} MB at level 19, {:.2} MB with the frame dictionary", grouped_by(&enc, 19, &[], kb << 10) as f64 / 1e6, grouped_by(&enc, 19, &dict, kb << 10) as f64 / 1e6);
    }
    for kb in [64usize, 128, 256] {
        // a dictionary trained on step parts themselves, samples spread over all of them
        let all: Vec<u8> = enc.concat();
        let want = (kb << 10) * 100;
        let stride = (all.len() / want.max(1)).max(1);
        let (mut buf, mut sizes) = (Vec::new(), Vec::new());
        for (i, c) in all.chunks(4096).enumerate() {
            if i % stride == 0 {
                buf.extend_from_slice(c);
                sizes.push(c.len());
            }
        }
        let d = zstd::dict::from_continuous(&buf, &sizes, kb << 10).unwrap_or_default();
        let dz = zstd::bulk::compress(&d, 19).unwrap().len();
        println!("dictionary trained on step parts, {kb} KB ({:.2} MB stored): 64 KB blocks {:.2} MB, 256 KB blocks {:.2} MB", dz as f64 / 1e6, grouped(&enc, 19, &d) as f64 / 1e6, grouped_by(&enc, 19, &d, 256 << 10) as f64 / 1e6);
    }
    for t in ts {
        let enc: Vec<Vec<u8>> = parts.iter().map(|p| encode(p, t)).collect();
        let raw: usize = enc.iter().map(|x| x.len()).sum();
        println!(
            "values over {t:>4} B as positions: raw {:.2} MB -> {:.2} MB at level 19, {:.2} MB with the frame dictionary",
            raw as f64 / 1e6,
            grouped(&enc, 19, &[]) as f64 / 1e6,
            if dict.is_empty() { 0.0 } else { grouped(&enc, 19, &dict) as f64 / 1e6 }
        );
    }
}
