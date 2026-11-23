//! Experiment: how small can session message streams get with random access kept?
//! For each configuration (frame KB : zstd level : dictionary KB [: training samples as a
//! multiple of the dictionary, default 100]), cut every session's stream (stored messages in
//! storage order, as frames hold them) into frames, compress, and time decompressing every
//! frame: in order, in random order with caches made cold, and with a dictionary through a
//! prepared dictionary kept apart against one laid out right before the output. Prints sizes
//! and times only.
//!
//!   cargo run --release --example frames_exp -- STORE "64:3:0,64:19:0,64:19:1024"

use std::time::Instant;
use transcriptdb::{Db, Options};

fn main() {
    let mut args = std::env::args().skip(1);
    let store = args.next().expect("store dir");
    let configs = args.next().unwrap_or_else(|| "64:3:0".into());
    let db = Db::open(&store, Options::default()).expect("open");
    let t = Instant::now();
    let mut streams: Vec<Vec<u8>> = Vec::new();
    for s in 0..db.session_count() as u32 {
        let mut b = Vec::new();
        for n in db.stored_nodes(s).expect("session") {
            b.extend_from_slice(&n.3);
        }
        streams.push(b);
    }
    let total: usize = streams.iter().map(|s| s.len()).sum();
    println!("{} sessions, {:.1} MB of stored messages, read in {:.1}s", streams.len(), total as f64 / 1e6, t.elapsed().as_secs_f64());
    for c in configs.split(',') {
        let v: Vec<usize> = c.split(':').map(|x| x.parse().unwrap()).collect();
        let (fkb, level, dkb) = (v[0], v[1] as i32, v[2]);
        let mult = v.get(3).copied().unwrap_or(100);
        let fs = fkb << 10;
        let t = Instant::now();
        let dict = if dkb > 0 {
            // train on frame-sized samples spread over the data (about 100x the dictionary)
            let want = (dkb << 10) * mult;
            let step = (total / want.max(1)).max(1);
            let (mut data, mut sizes) = (Vec::new(), Vec::new());
            let mut k = 0usize;
            for s in &streams {
                for ch in s.chunks(fs) {
                    if k.is_multiple_of(step) {
                        data.extend_from_slice(ch);
                        sizes.push(ch.len());
                    }
                    k += 1;
                }
            }
            zstd::dict::from_continuous(&data, &sizes, dkb << 10).expect("train")
        } else {
            Vec::new()
        };
        let t_train = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let mut comp = if dict.is_empty() { zstd::bulk::Compressor::new(level).unwrap() } else { zstd::bulk::Compressor::with_dictionary(level, &dict).unwrap() };
        let mut frames: Vec<(Vec<u8>, usize)> = Vec::new();
        for s in &streams {
            for ch in s.chunks(fs) {
                frames.push((comp.compress(ch).unwrap(), ch.len()));
            }
        }
        let t_comp = t.elapsed().as_secs_f64();
        let csize: usize = frames.iter().map(|f| f.0.len()).sum::<usize>() + dict.len();
        let mut dec = if dict.is_empty() { zstd::bulk::Decompressor::new().unwrap() } else { zstd::bulk::Decompressor::with_dictionary(&dict).unwrap() };
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            for (f, n) in &frames {
                let d = dec.decompress(f, *n).unwrap();
                assert_eq!(d.len(), *n);
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        // random order, with 8 MB of other memory touched between frames (caches cold)
        let mut scrub = vec![0u8; 8 << 20];
        let mut order: Vec<usize> = (0..frames.len()).collect();
        let mut x = 12345u64;
        for i in (1..order.len()).rev() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            order.swap(i, (x % (i as u64 + 1)) as usize);
        }
        let mut cold = 0f64;
        for &i in order.iter().take(2000) {
            for b in scrub.iter_mut().step_by(64) {
                *b = b.wrapping_add(1);
            }
            let t = Instant::now();
            let d = dec.decompress(&frames[i].0, frames[i].1).unwrap();
            cold += t.elapsed().as_secs_f64();
            assert_eq!(d.len(), frames[i].1);
        }
        let cold_us = cold * 1e6 / order.len().min(2000) as f64;
        if !dict.is_empty() {
            let dd = zstd::zstd_safe::DDict::create(&dict);
            let mut dctx = zstd::zstd_safe::DCtx::create();
            let mut best2 = f64::MAX;
            let mut best = f64::MAX;
            for _ in 0..5 {
                let t = Instant::now();
                for (f, n) in &frames {
                    let mut out = Vec::with_capacity(*n);
                    dctx.decompress_using_ddict(&mut out, f, &dd).unwrap();
                    assert_eq!(out.len(), *n);
                }
                best2 = best2.min(t.elapsed().as_secs_f64());
                let t = Instant::now();
                for (f, n) in &frames {
                    let d = dec.decompress(f, *n).unwrap();
                    assert_eq!(d.len(), *n);
                }
                best = best.min(t.elapsed().as_secs_f64());
            }
            // decode right after the dictionary's bytes: matches into it are plain prefix matches
            let d = dict.len();
            let mut buf = vec![0u8; d + fs];
            buf[..d].copy_from_slice(&dict);
            let (pre, outb) = buf.split_at_mut(d);
            let ddr = zstd::zstd_safe::DDict::create_by_reference(pre);
            let mut best3 = f64::MAX;
            for _ in 0..5 {
                let t = Instant::now();
                for (f, n) in &frames {
                    let got = dctx.decompress_using_ddict(&mut outb[..*n], f, &ddr).unwrap();
                    assert_eq!(got, *n);
                }
                best3 = best3.min(t.elapsed().as_secs_f64());
            }
            println!("    contiguous after the dictionary: {:.1} us per frame", best3 * 1e6 / frames.len() as f64);
            println!("    with a prepared DDict: {:.1} us per frame (loaded into the context: {:.1})", best2 * 1e6 / frames.len() as f64, best * 1e6 / frames.len() as f64);
        }
        println!("    cold random frame: {cold_us:.1} us (checksum {})", scrub[0]);
        println!(
            "frame {fkb:>4} KB level {level:>2} dict {dkb:>5} KB: {:>7.2} MB ({:.2}x){}; compress {:.1}s ({:.0} MB/s){}; decompress {:.0} MB/s, {:.1} us per frame",
            csize as f64 / 1e6,
            total as f64 / csize as f64,
            if dict.is_empty() { String::new() } else { format!(" incl. dict {:.2} MB", dict.len() as f64 / 1e6) },
            t_comp,
            total as f64 / 1e6 / t_comp,
            if dkb > 0 { format!(", train {t_train:.1}s") } else { String::new() },
            total as f64 / 1e6 / best,
            best * 1e6 / frames.len() as f64
        );
    }
}
