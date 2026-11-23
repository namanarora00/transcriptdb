//! `tdb`: command-line tool for the TranscriptDB storage core.
//!
//!   tdb ingest-claude --store DIR [--logs DIR] [--max-files N] [--seed S] [--threads N] [--sync full|fsync|os]
//!                     [--drop documents,other]
//!   tdb check-claude  --store DIR [--logs DIR] [--threads N] [--drop ...as at ingest]
//!   tdb stats         --store DIR
//!   tdb compact       --store DIR
//!   tdb bench         --store DIR [--n 300] [--seed S]
//!   tdb crash-test    --store DIR [--kills 1000] [--seed S] [--sync full|fsync|os]
//!   tdb growth        --store DIR [--rounds 5] [--base 300] [--wake 0.1] [--cache-mb 256] [--flush-mb 64] [--seg-mb 256] [--max-parts 4]
//!   tdb ceiling       --store DIR [--out DIR]
//!   tdb h2h           --store DIR [--pg ADDR] [--split x] [--sizes-only x]
//!
//! Writing commands take --merge-level 19 --dict-kb 1024 --zstd-threads N (full-size merges
//! and compact: zstd level, dictionary size, compression threads).
//!
//! Thinking (text, signatures, redacted thinking) and images are never stored: they are
//! replaced by empty strings at ingest. `--drop` blanks documents and other base64 too.
//!
//! Real logs are read from `--logs` (default `~/.claude/projects`) at run time only.
//! Output is counts, sizes and times; no log content is printed.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use transcriptdb::claude::{self, LogFile, LogRequest, Problem};
use transcriptdb::db::sha256;
use transcriptdb::split::{self, Keep};
use transcriptdb::synth::{self, Rng};
use transcriptdb::{Db, Options, Put, PutOutcome, ReadStats, SyncMode};

struct Args {
    cmd: String,
    kv: HashMap<String, String>,
}

impl Args {
    fn parse() -> Args {
        let mut it = std::env::args().skip(1);
        let cmd = it.next().unwrap_or_default();
        let mut kv = HashMap::new();
        while let Some(k) = it.next() {
            let k = k.trim_start_matches("--").to_string();
            let v = it.next().unwrap_or_default();
            kv.insert(k, v);
        }
        Args { cmd, kv }
    }
    fn get(&self, k: &str) -> Option<&str> {
        self.kv.get(k).map(|s| s.as_str())
    }
    fn num(&self, k: &str, d: u64) -> u64 {
        self.get(k).map(|v| v.parse().expect("number")).unwrap_or(d)
    }
    fn store(&self) -> PathBuf {
        PathBuf::from(self.get("store").expect("--store DIR is required"))
    }
    fn logs(&self) -> PathBuf {
        match self.get("logs") {
            Some(p) => PathBuf::from(p),
            None => PathBuf::from(std::env::var("HOME").unwrap()).join(".claude/projects"),
        }
    }
    fn sync(&self) -> SyncMode {
        match self.get("sync").unwrap_or("full") {
            "full" => SyncMode::Full,
            "fsync" => SyncMode::Fsync,
            "os" => SyncMode::Os,
            o => panic!("unknown sync mode {o}"),
        }
    }
    fn threads(&self) -> usize {
        self.num("threads", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4) as u64) as usize
    }
    /// `--drop documents,other`: opaque values not to keep besides thinking and images.
    fn keep(&self) -> Keep {
        match self.get("drop") {
            None => Keep::default(),
            Some(l) => Keep::dropping(l).unwrap_or_else(|e| panic!("--drop: {e}")),
        }
    }
    fn opts(&self) -> Options {
        Options {
            sync: self.sync(),
            flush_bytes: (self.num("flush-mb", 64) as usize) << 20,
            level: self.num("level", 3) as i32,
            merge_level: self.num("merge-level", Options::default().merge_level as u64) as i32,
            dict_bytes: (self.num("dict-kb", (Options::default().dict_bytes >> 10) as u64) as usize) << 10,
            threads: self.num("zstd-threads", Options::default().threads as u64) as usize,
            frame_size: (self.num("frame-kb", 64) as usize) << 10,
            keep: self.keep(),
            cache_bytes: (self.num("cache-mb", 256) as usize) << 20,
            max_parts: self.num("max-parts", 4) as usize,
            seg_bytes: self.num("seg-mb", 256) << 20,
            ..Options::default()
        }
    }
}

fn mb(b: u64) -> String {
    format!("{:.1} MB", b as f64 / 1e6)
}

fn q(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn select_files(a: &Args) -> Vec<PathBuf> {
    let mut files = claude::log_files(&a.logs());
    if let Some(n) = a.get("max-files") {
        let mut r = Rng::new(a.num("seed", 7));
        for i in (1..files.len()).rev() {
            let j = r.below(i as u64 + 1) as usize;
            files.swap(i, j);
        }
        files.truncate(n.parse().unwrap());
        files.sort();
    }
    files
}

enum Item {
    Req(LogRequest),
    File(claude::FileReport),
}

/// Read log files on worker threads, in file order per worker.
fn read_logs(files: Vec<PathBuf>, threads: usize) -> mpsc::Receiver<Item> {
    let (tx, rx) = mpsc::sync_channel::<Item>(64);
    let files = Arc::new(files);
    let next = Arc::new(AtomicUsize::new(0));
    for _ in 0..threads.max(1) {
        let (tx, files, next) = (tx.clone(), files.clone(), next.clone());
        std::thread::spawn(move || {
            loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= files.len() {
                    break;
                }
                let Ok(lf) = LogFile::read(&files[i]) else { continue };
                if !lf.has_requests() {
                    continue;
                }
                let rep = lf.requests(|r| {
                    let _ = tx.send(Item::Req(r));
                });
                let _ = tx.send(Item::File(rep));
            }
        });
    }
    rx
}

fn ingest_claude(a: &Args) {
    let t0 = Instant::now();
    let files = select_files(a);
    let store_unverified = a.get("store-unverified").is_some();
    let mut db = Db::open(a.store(), a.opts()).expect("open store");
    println!("store opened in {:.2}s: {} requests already stored ({:?})", t0.elapsed().as_secs_f64(), db.request_count(), db.recovery);
    let rx = read_logs(files.clone(), a.threads().saturating_sub(1).max(1));
    let (mut seen, mut verified, mut mismatch, mut stored, mut dup, mut since_commit) = (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
    let mut no_digest = 0usize;
    let mut problems: HashMap<Problem, usize> = HashMap::new();
    let mut body_bytes = 0u64;
    let mut files_with_requests = 0usize;
    let mut blob_bad = 0usize;
    let mut put_time = Duration::ZERO;
    for item in rx {
        match item {
            Item::File(rep) => {
                files_with_requests += 1;
                blob_bad += rep.blob_hash_mismatch;
            }
            Item::Req(r) => {
                seen += 1;
                let ok = r.verified();
                match (&r.body, r.digest) {
                    (Err(p), _) => *problems.entry(*p).or_default() += 1,
                    (Ok(_), None) => no_digest += 1,
                    (Ok(_), Some(_)) if ok => verified += 1,
                    (Ok(_), Some(_)) => mismatch += 1,
                }
                let Ok(body) = &r.body else { continue };
                if !ok && !store_unverified {
                    continue;
                }
                let t = Instant::now();
                let out = db
                    .put(&Put { session: &r.session, agent: &r.agent, id: &r.id, time: &r.time, meta: &r.meta, body })
                    .unwrap_or_else(|e| panic!("put {}: {e}", r.id));
                match out {
                    PutOutcome::Stored(_) => {
                        stored += 1;
                        body_bytes += body.len() as u64;
                        since_commit += 1;
                    }
                    PutOutcome::Duplicate(_) => dup += 1,
                }
                if since_commit >= 256 {
                    db.commit().expect("commit");
                    since_commit = 0;
                }
                put_time += t.elapsed();
            }
        }
    }
    let t = Instant::now();
    db.commit().expect("commit");
    db.flush().expect("flush");
    put_time += t.elapsed();
    let secs = t0.elapsed().as_secs_f64();
    println!("log files: {} found, {} with recorded requests", files.len(), files_with_requests);
    println!("recorded requests read: {seen}");
    println!("  rebuilt from the log and equal to Claude Code's requestDigest: {verified}");
    println!("  rebuilt but different from requestDigest: {mismatch}");
    println!("  no requestDigest in the log: {no_digest}");
    for (p, n) in &problems {
        println!("  not rebuildable from the log ({p:?}): {n}");
    }
    if blob_bad > 0 {
        println!("  blob lines whose hash did not match their message: {blob_bad}");
    }
    println!("stored now: {stored} new, {dup} already present; total in store {}", db.request_count());
    println!(
        "ingest: {:.1}s total, {:.1}s in the store's write path; {} of request bodies in, {:.0} MB/s and {:.0} requests/s through the write path",
        secs,
        put_time.as_secs_f64(),
        mb(body_bytes),
        body_bytes as f64 / 1e6 / put_time.as_secs_f64().max(1e-9),
        stored as f64 / put_time.as_secs_f64().max(1e-9)
    );
    let ms = &db.merge_stats;
    println!(
        "merges during ingest: {}, writing {} for {} written by flushes; sessions folded {}, segments rewritten to drop replaced parts {}",
        ms.merges,
        mb(ms.bytes_out),
        mb(ms.flushed),
        ms.folded,
        ms.reclaims
    );
}

fn check_claude(a: &Args) {
    let t0 = Instant::now();
    let files = claude::log_files(&a.logs());
    let mut list: Vec<(String, Option<[u8; 32]>, usize)> = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        if let Ok(lf) = LogFile::read(f)
            && lf.has_requests() {
                list.extend(lf.request_digests().into_iter().map(|(i, d)| (i, d, fi)));
            }
    }
    // the same request can be logged in more than one file (resumed or forked sessions)
    let raw_lines = list.len();
    let mut seen_ids = HashSet::new();
    list.retain(|(id, _, _)| seen_ids.insert(id.clone()));
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let t_read = t0.elapsed().as_secs_f64();
    let ok = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let bytes = AtomicUsize::new(0);
    let missing: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    // rebuilds not equal to requestDigest: compared below with the log's body, blanked
    let differ: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let next = AtomicUsize::new(0);
    let t1 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..a.threads() {
            s.spawn(|| {
                let mut out = Vec::new();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= list.len() {
                        break;
                    }
                    let (id, d, _) = &list[i];
                    let Some(ri) = db.request_index(id) else {
                        missing.lock().unwrap().push(i);
                        continue;
                    };
                    out.clear();
                    if let Err(e) = db.rebuild_into(ri, &mut out, &mut ReadStats::default()) {
                        errors.fetch_add(1, Ordering::Relaxed);
                        failures.lock().unwrap().push(format!("{id}: {e}"));
                        continue;
                    }
                    bytes.fetch_add(out.len(), Ordering::Relaxed);
                    if d.is_some_and(|d| d == sha256(&out)) {
                        ok.fetch_add(1, Ordering::Relaxed);
                    } else {
                        differ.lock().unwrap().push(i);
                    }
                }
            });
        }
    });
    let secs = t1.elapsed().as_secs_f64();
    let missing = missing.into_inner().unwrap();
    let differ = differ.into_inner().unwrap();
    // build each such body from the log again, blank what the store never keeps (and what
    // `--drop` names), and compare with the rebuild
    let keep = a.keep();
    let exact_late = AtomicUsize::new(0);
    let blank_ok = AtomicUsize::new(0);
    let blank_bad = AtomicUsize::new(0);
    let t2 = Instant::now();
    if !differ.is_empty() {
        let want: HashMap<&str, usize> = differ.iter().map(|&i| (list[i].0.as_str(), i)).collect();
        let mut fset: Vec<usize> = differ.iter().map(|&i| list[i].2).collect();
        fset.sort_unstable();
        fset.dedup();
        let next = AtomicUsize::new(0);
        let done: Mutex<HashSet<usize>> = Mutex::new(HashSet::new());
        std::thread::scope(|s| {
            for _ in 0..a.threads() {
                s.spawn(|| {
                    loop {
                        let k = next.fetch_add(1, Ordering::Relaxed);
                        if k >= fset.len() {
                            break;
                        }
                        let Ok(lf) = LogFile::read(&files[fset[k]]) else { continue };
                        lf.requests(|r| {
                            let Some(&i) = want.get(r.id.as_str()) else { return };
                            let Ok(body) = &r.body else { return };
                            if !done.lock().unwrap().insert(i) {
                                return;
                            }
                            let ri = db.request_index(&list[i].0).unwrap();
                            match (split::blank_dropped(body, &keep), db.rebuild(ri)) {
                                // a log line without requestDigest: compared with its body
                                (_, Ok(got)) if list[i].1.is_none() && &got == body => _ = exact_late.fetch_add(1, Ordering::Relaxed),
                                (Ok(want), Ok(got)) if want == got && &want != body => _ = blank_ok.fetch_add(1, Ordering::Relaxed),
                                _ => {
                                    blank_bad.fetch_add(1, Ordering::Relaxed);
                                    failures.lock().unwrap().push(format!("{}: rebuild differs from the log's body, both as sent and with values not kept blanked", r.id));
                                }
                            }
                        });
                    }
                });
            }
        });
        let unseen = differ.len() - done.into_inner().unwrap().len();
        blank_bad.fetch_add(unseen, Ordering::Relaxed);
    }
    let stored_in_logs = list.len() - missing.len();
    println!("requests recorded in the logs now: {} distinct ids ({raw_lines} request lines; read in {t_read:.1}s)", list.len());
    println!("  in the store: {stored_in_logs}");
    println!("    rebuilt byte for byte (sha256 equal to Claude Code's requestDigest): {}", ok.load(Ordering::Relaxed) + exact_late.load(Ordering::Relaxed));
    println!(
        "    no longer equal to requestDigest because thinking or images (or --drop values) were blanked, and equal to the log's body with them blanked: {} ({:.1}s)",
        blank_ok.load(Ordering::Relaxed),
        t2.elapsed().as_secs_f64()
    );
    println!("    different from the log's body both as sent and blanked: {}", blank_bad.load(Ordering::Relaxed));
    println!("    read errors: {}", errors.load(Ordering::Relaxed));
    println!(
        "    {} rebuilt in {secs:.1}s on {} threads ({:.0} MB/s, {:.0} requests/s)",
        mb(bytes.load(Ordering::Relaxed) as u64),
        a.threads(),
        bytes.load(Ordering::Relaxed) as f64 / 1e6 / secs,
        stored_in_logs as f64 / secs
    );
    for f in failures.lock().unwrap().iter().take(20) {
        println!("    FAIL {f}");
    }
    let failed = errors.load(Ordering::Relaxed) + blank_bad.load(Ordering::Relaxed);
    println!("  not in the store: {}", missing.len());
    if !missing.is_empty() {
        // explain each one by building it from the log again
        let want: HashSet<&str> = missing.iter().map(|&i| list[i].0.as_str()).collect();
        let fset: HashSet<usize> = missing.iter().map(|&i| list[i].2).collect();
        let mut why: HashMap<String, usize> = HashMap::new();
        for fi in fset {
            if let Ok(lf) = LogFile::read(&files[fi]) {
                lf.requests(|r| {
                    if want.contains(r.id.as_str()) {
                        let k = match (&r.body, r.verified()) {
                            (Err(p), _) => format!("not rebuildable from the log ({p:?})"),
                            (Ok(_), false) => "rebuilt from the log but different from requestDigest, so not stored".into(),
                            (Ok(_), true) => "rebuildable and verified, but not ingested (outside the sample, or logged after the ingest ran)".into(),
                        };
                        *why.entry(k).or_default() += 1;
                    }
                });
            }
        }
        let mut w: Vec<_> = why.into_iter().collect();
        w.sort();
        for (k, n) in w {
            println!("    {k}: {n}");
        }
    }
    if failed > 0 {
        println!("FAILED: {failed} stored requests did not rebuild exactly");
        std::process::exit(1);
    }
}

fn stats(a: &Args) {
    let t = Instant::now();
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let t_open = t.elapsed().as_secs_f64();
    let st = db.stats();
    let mem_line = format!(
        "memory after open: catalogs {} for {} files, sessions taking writes {} ({}), unflushed {}",
        mb(st.catalog_bytes as u64),
        st.segments + st.runs + st.blobs,
        mb(st.active_bytes as u64),
        st.active_sessions,
        mb(st.mem_bytes as u64)
    );
    let mut runs: HashMap<u32, usize> = HashMap::new();
    let mut msgs = 0u64;
    let all = db.all_requests().expect("list requests");
    for &i in &all {
        *runs.entry(db.request_runs(i).unwrap()).or_default() += 1;
        msgs += db.request(i).unwrap().n_msgs as u64;
    }
    let n = all.len();
    let opaque_text: u64 = st.opaque_bytes * 4 / 3;
    let data = st.store_bytes();
    println!("sessions {}  requests {}  messages in requests {}  stored messages (tree nodes) {}", st.sessions, st.requests, msgs, st.nodes);
    println!("distinct message text: {} ({} message bytes + about {} of base64 opaque values)", mb(st.node_bytes + opaque_text), mb(st.node_bytes), mb(opaque_text));
    println!(
        "on disk: segments {} + index {} + blob files {} = {}; {} segment(s) ({} from flushes not merged yet), {} index run(s), {} blob file(s); log {}",
        mb(st.seg_file_bytes),
        mb(st.index_file_bytes),
        mb(st.blob_file_bytes),
        mb(data),
        st.segments,
        st.level0,
        st.runs,
        st.blobs,
        mb(st.wal_bytes)
    );
    println!("  message frames {}  ({:.2}x smaller than the {} they hold)", mb(st.frame_bytes), st.node_bytes as f64 / st.frame_bytes.max(1) as f64, mb(st.node_bytes));
    println!("  opaque values {} raw ({} values)", mb(st.opaque_bytes), st.opaques);
    println!("  shared pieces {} compressed from {} ({} pieces: skeletons, system, tools, markers)", mb(st.piece_bytes), mb(st.piece_raw_bytes), st.pieces);
    println!("  segment blocks, directories and catalogs {}; index runs {}", mb(st.index_bytes), mb(st.index_file_bytes));
    let mut sizes: Vec<u64> = db.segments().iter().map(|s| s.4).collect();
    sizes.sort_unstable();
    println!("  segments: {} from flushes, {} merged; sizes median {}, largest {}", st.level0, st.segments - st.level0, mb(sizes.get(sizes.len() / 2).copied().unwrap_or(0)), mb(sizes.last().copied().unwrap_or(0)));
    if st.dead_bytes > 0 {
        println!("  session parts that folds replaced, left in their segments until those are rewritten: {}", mb(st.dead_bytes));
    }
    let parts: Vec<usize> = (0..st.sessions as u32).map(|s| db.part_count(s).expect("session parts")).collect();
    println!("  sessions in more than one part: {} of {} (most parts {})", parts.iter().filter(|&&p| p > 1).count(), st.sessions, parts.iter().max().unwrap_or(&0));
    println!("  all data files: {} for {} of distinct text ({:.3} bytes per byte)", mb(data), mb(st.node_bytes + opaque_text), data as f64 / (st.node_bytes + opaque_text).max(1) as f64);
    if a.get("opaque").is_some() {
        let total = data.max(1) as f64;
        println!("  opaque values by kind (values, size, share of all data files):");
        for (kind, n, bytes) in db.opaque_kinds().expect("opaque kinds") {
            println!("    {:<20} {:>8}  {:>10}  {:>5.1}%", kind, n, mb(bytes), bytes as f64 * 100.0 / total);
        }
    }
    let mut rv: Vec<_> = runs.into_iter().collect();
    rv.sort();
    let tot = n.max(1) as f64;
    let line: Vec<String> = rv.iter().take(6).map(|(r, c)| format!("{r} run(s): {} ({:.1}%)", c, *c as f64 * 100.0 / tot)).collect();
    println!("storage runs per request: {}; max {}", line.join(", "), rv.last().map(|x| x.0).unwrap_or(0));
    println!("{mem_line}; opened in {t_open:.3}s");
    let st = db.stats();
    let c: Vec<String> = st.caches.iter().map(|(k, c)| format!("{k} {}", mb(c.bytes as u64))).collect();
    println!("caches after reading every request (budget {} in all): {}", mb(a.opts().cache_bytes as u64), c.join(", "));
}


fn compact(a: &Args) {
    let mut db = Db::open(a.store(), a.opts()).expect("open store");
    let t = Instant::now();
    db.compact().expect("compact");
    println!("merged into {} segment(s), each session whole and main line first, in {:.1}s", db.segment_count(), t.elapsed().as_secs_f64());
}

fn bench(a: &Args) {
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let n = a.num("n", 300) as usize;
    let mut r = Rng::new(a.num("seed", 5));
    let all = db.all_requests().expect("list requests");
    let total = all.len() as u64;
    let pick: Vec<u64> = (0..n).map(|_| all[r.below(total) as usize]).collect();
    let long: Vec<u64> = all.iter().copied().filter(|&i| db.request(i).unwrap().n_msgs >= 40).collect();
    let ranges: Vec<(u64, u32, usize)> = (0..n.min(long.len() * 4))
        .map(|_| {
            let ri = *r.pick(&long);
            let rec = db.request(ri).unwrap();
            let k = r.below(rec.n_msgs as u64 - 40 + 1) as usize;
            (ri, rec.session, k)
        })
        .collect();
    let sessions: Vec<u32> = (0..n.min(db.session_count())).map(|_| r.below(db.session_count() as u64) as u32).collect();
    for pass in 0..2 {
        let (mut t1, mut t2, mut t3) = (Vec::new(), Vec::new(), Vec::new());
        let (mut reads, mut bytes) = (Vec::new(), 0u64);
        let mut out = Vec::new();
        for &ri in &pick {
            out.clear();
            let mut st = ReadStats::default();
            let t = Instant::now();
            db.rebuild_into(ri, &mut out, &mut st).unwrap();
            t1.push(t.elapsed().as_secs_f64() * 1e3);
            reads.push(st.reads as f64);
            bytes += out.len() as u64;
        }
        for &(ri, s, k) in &ranges {
            let path = db.request_path(ri).unwrap();
            let t = Instant::now();
            db.read_messages(s, &path[k..k + 40], &mut ReadStats::default()).unwrap();
            t2.push(t.elapsed().as_secs_f64() * 1e3);
        }
        for &s in &sessions {
            let t = Instant::now();
            db.replay(s, &mut ReadStats::default()).unwrap();
            t3.push(t.elapsed().as_secs_f64() * 1e3);
        }
        if pass == 1 {
            println!("reads with a warm OS cache, {} samples each (pass 2 of 2):", n);
            println!(
                "  rebuild one request       median {:.3} ms  p95 {:.3}  p99 {:.3}   (mean request {:.0} KB; disk reads per rebuild median {:.0}, p99 {:.0})",
                q(&mut t1.clone(), 0.5),
                q(&mut t1.clone(), 0.95),
                q(&mut t1, 0.99),
                bytes as f64 / pick.len() as f64 / 1e3,
                q(&mut reads.clone(), 0.5),
                q(&mut reads, 0.99)
            );
            println!("  40 messages in a row      median {:.3} ms  p95 {:.3}  p99 {:.3}", q(&mut t2.clone(), 0.5), q(&mut t2.clone(), 0.95), q(&mut t2, 0.99));
            println!("  replay a whole session    median {:.3} ms  p95 {:.3}  p99 {:.3}", q(&mut t3.clone(), 0.5), q(&mut t3.clone(), 0.95), q(&mut t3, 0.99));
        }
    }
}

fn check_steps(a: &Args) {
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let t = Instant::now();
    let c = transcriptdb::check::check_steps(&db, a.threads()).expect("check steps");
    println!("sessions {}  engine steps {}  distinct (engine {} / reference {})", c.sessions, c.engine_steps, c.engine_keys, c.reference_keys);
    println!("  reference steps the engine lacks: {}", c.missing);
    println!("  engine steps the reference lacks: {}", c.extra);
    println!("  steps with a wrong prev: {}   wrong shape codes: {}", c.prev_wrong, c.shape_wrong);
    println!("  tool results with no call on their path: {}", db.stats().orphan_results);
    println!("  checked in {:.1}s", t.elapsed().as_secs_f64());
    if !c.ok() {
        println!("FAILED in {} session(s), first: {:?}", c.bad_sessions.len(), &c.bad_sessions[..c.bad_sessions.len().min(10)]);
        std::process::exit(1);
    }
}

/// Step counts per tool and outcome (tool names only; no inputs or outputs).
fn steps_summary(a: &Args) {
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let sd = db.step_dicts();
    let mut per_tool: HashMap<u32, (usize, usize)> = HashMap::new();
    let mut total = 0usize;
    let mut forked = 0usize;
    for s in 0..db.session_count() as u32 {
        let ss_arc = db.steps(s);
        let ss = &*ss_arc;
        total += ss.rows.len();
        for (i, r) in ss.rows.iter().enumerate() {
            let e = per_tool.entry(r.tool).or_default();
            e.0 += 1;
            e.1 += r.failed as usize;
            // a step whose prev is not simply an earlier turn's last step sits on a fork
            if i > 0 && r.prev != i as u32 - 1 && r.prev != ss.rows[i - 1].prev {
                forked += 1;
            }
        }
    }
    let shape_bytes: usize = (0..db.session_count() as u32).map(|s| db.steps(s).shape.len()).sum();
    println!("steps {total} in {} sessions; {} tools, {} input fields, {} short values (distinct per session)", db.session_count(), sd.tools.len(), sd.fields.len(), (0..db.session_count() as u32).map(|s| db.steps(s).vals.iter().map(|(_, d)| d.len()).sum::<usize>()).sum::<usize>());
    println!("shape strings: {} bytes in all; steps whose prev is not the step before (forks, parallel calls): {forked}", shape_bytes);
    let mut v: Vec<_> = per_tool.into_iter().collect();
    v.sort_by_key(|x| std::cmp::Reverse(x.1 .0));
    for (t, (n, f)) in v.iter().take(a.num("top", 15) as usize) {
        println!("  {:<28} {:>7} steps  {:>6} failed", String::from_utf8_lossy(sd.tools.get(*t)), n, f);
    }
}

fn print_table(heads: &[String], cells: &[Vec<String>]) {
    let clip = |s: &str| -> String {
        let one = s.replace('\n', " ");
        if one.chars().count() > 60 { format!("{}…", one.chars().take(59).collect::<String>()) } else { one }
    };
    let cells: Vec<Vec<String>> = cells.iter().map(|r| r.iter().map(|c| clip(c)).collect()).collect();
    let mut w: Vec<usize> = heads.iter().map(|h| h.chars().count()).collect();
    for r in &cells {
        for (i, c) in r.iter().enumerate() {
            w[i] = w[i].max(c.chars().count());
        }
    }
    let line = |r: &[String]| r.iter().enumerate().map(|(i, c)| format!("{c:<width$}", width = w[i])).collect::<Vec<_>>().join("  ");
    println!("{}", line(heads));
    println!("{}", w.iter().map(|n| "-".repeat(*n)).collect::<Vec<_>>().join("  "));
    for r in &cells {
        println!("{}", line(r));
    }
}

/// The head-to-head against PostgreSQL on the same store.
fn head_to_head(a: &Args) {
    use transcriptdb::h2h;
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let addr = a.get("pg").unwrap_or("127.0.0.1:5455").to_string();
    let mut pg = transcriptdb::pg::Pg::connect(&addr, a.get("pg-user").unwrap_or("tdb"), a.get("pg-db").unwrap_or("h2h")).expect("connect to postgres");
    if a.get("no-load").is_none() {
        println!("{}", h2h::load(&db, &mut pg).expect("load"));
    }
    let st = db.stats();
    let engine_bytes = st.store_bytes();
    let sizes = h2h::pg_sizes(&mut pg).expect("sizes");
    let pg_total: u64 = sizes.iter().map(|x| x.1).sum();
    println!("\n== size on disk");
    println!("  TranscriptDB store: {} (segments {}, index {}, blob files {})", mb(engine_bytes), mb(st.seg_file_bytes), mb(st.index_file_bytes), mb(st.blob_file_bytes));
    println!("  PostgreSQL tables with indexes and TOAST: {} ({})", mb(pg_total), sizes.iter().map(|(n, b)| format!("{n} {}", mb(*b))).collect::<Vec<_>>().join(", "));
    println!("  PostgreSQL / TranscriptDB: {:.2}x", pg_total as f64 / engine_bytes as f64);
    if a.get("split").is_some() {
        println!("\n== where PostgreSQL's bytes are");
        for l in h2h::pg_split(&mut pg).expect("split") {
            println!("  {l}");
        }
    }
    if a.get("sizes-only").is_some() {
        return;
    }
    let reps = a.num("reps", 5) as usize;
    let out = h2h::run_queries(&db, &mut pg, reps).expect("queries");
    println!("\n== queries over {} steps in {} sessions (best of {reps}; TranscriptDB on 1 thread and on 8)", (0..db.session_count() as u32).map(|s| db.steps(s).rows.len()).sum::<usize>(), db.session_count());
    println!("  {:<48} {:>6} {:>12} {:>9} {:>12} {:>9}  answer", "query", "same", "TDB 1 thread", "TDB 8", "PostgreSQL", "speedup");
    let mut all_same = true;
    for o in &out {
        all_same &= o.same;
        let ratio = o.pg_ms / o.engine_ms.max(1e-6);
        let sp = if ratio >= 10.0 { format!("{ratio:.0}x") } else { format!("{ratio:.1}x") };
        println!("  {:<48} {:>6} {:>9.2} ms {:>6.2} ms {:>9.2} ms {:>9}  {}", o.name, if o.same { "yes" } else { "NO" }, o.engine_ms, o.engine_ms_par, o.pg_ms, sp, o.detail);
    }
    let reads = h2h::run_reads(&db, &mut pg, a.num("reads", 200) as usize, a.num("seed", 5)).expect("reads");
    println!("\n== reads (median / p95 ms per item; PostgreSQL over loopback TCP)");
    for r in &reads {
        all_same &= r.same == r.total;
        println!("  {:<36} same {}/{}   TDB {:.3} / {:.3}   PostgreSQL {:.3} / {:.3}   {:.1}x at the median", r.name, r.same, r.total, r.engine.0, r.engine.1, r.pg.0, r.pg.1, r.pg.0 / r.engine.0.max(1e-9));
    }
    if !all_same {
        println!("\nFAILED: some answers differ");
        std::process::exit(1);
    }
}

/// Generated match queries: the engine against the slow reference, session by session.
fn check_queries(a: &Args) {
    use transcriptdb::query::reference;
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let n = a.num("n", 300);
    let max_steps = a.num("max-steps", 400) as usize;
    let mut r = Rng::new(a.num("seed", 3));
    let t = Instant::now();
    let (mut total, mut failed, mut with_matches) = (0usize, 0usize, 0usize);
    let checked_sessions = (0..db.session_count() as u32).filter(|&s| db.steps(s).rows.len() <= max_steps).count();
    for _ in 0..n {
        let q = reference::random_query(&db, &mut r);
        match reference::compare(&db, &q, max_steps) {
            Ok((m, bad)) => {
                total += m;
                with_matches += (m > 0) as usize;
                if !bad.is_empty() {
                    failed += 1;
                    if failed <= 10 {
                        println!("DIFF in {} session(s) (first {:?}): {q}", bad.len(), &bad[..bad.len().min(5)]);
                    }
                }
            }
            Err(e) => {
                failed += 1;
                println!("ERROR {e}: {q}");
            }
        }
    }
    println!(
        "{n} generated queries over {checked_sessions} sessions (of {}; sessions over {max_steps} steps left out): {failed} differ; {with_matches} queries had matches, {total} matches in all; {:.1}s",
        db.session_count(),
        t.elapsed().as_secs_f64()
    );
    if failed > 0 {
        std::process::exit(1);
    }
}

fn query(a: &Args) {
    let src = match (a.get("q"), a.get("file")) {
        (Some(q), _) => q.to_string(),
        (None, Some(f)) => std::fs::read_to_string(f).expect("read query file"),
        _ => {
            eprintln!("usage: tdb query --store DIR --q 'QUERY' [--limit 30] [--threads N] [--repeat N]");
            std::process::exit(2);
        }
    };
    let t0 = Instant::now();
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let open_s = t0.elapsed().as_secs_f64();
    let reps = a.num("repeat", 1).max(1);
    let mut best = f64::MAX;
    let mut run = None;
    for _ in 0..reps {
        let t = Instant::now();
        match transcriptdb::query::run(&db, &src, a.threads()) {
            Ok(r) => {
                best = best.min(t.elapsed().as_secs_f64() * 1e3);
                run = Some(r);
            }
            Err(e) => {
                eprintln!("error: {}", e.show(&src));
                std::process::exit(1);
            }
        }
    }
    let r = run.unwrap();
    let limit = a.num("limit", 30) as usize;
    let (heads, cells) = transcriptdb::query::exec::render(&db, &r.table, limit).expect("render");
    print_table(&heads, &cells);
    let st = &r.stats;
    println!(
        "\n{} rows{}  |  {:.2} ms (best of {reps}): plan {:.2}, scan {:.2}, rest {:.2}  |  {} sessions, {} skipped, {} steps scanned, {} matches  |  store opened in {:.1}s",
        r.table.rows.len(),
        if r.table.rows.len() > limit { format!(" (showing {limit})") } else { String::new() },
        best,
        st.plan_ms,
        st.scan_ms,
        st.rest_ms,
        st.sessions,
        st.sessions_skipped,
        st.steps_scanned,
        st.matches,
        open_s
    );
}

/// A store of synthetic sessions (for trying things without real data).
fn synth_store(a: &Args) {
    let mut db = Db::open(a.store(), a.opts()).expect("open store");
    let seed = a.num("seed", 1);
    let mut n = 0;
    for s in 0..a.num("sessions", 20) {
        for q in synth::session(seed, s, a.num("per", 40) as usize) {
            db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).expect("put");
            n += 1;
        }
        db.commit().expect("commit");
    }
    db.flush().expect("flush");
    println!("{n} synthetic requests in {} sessions", db.session_count());
}

/// Local web pages: sessions, a session's timeline, one step. Listens on 127.0.0.1 only.
fn serve(a: &Args) {
    use std::io::Read;
    use std::net::TcpListener;
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let port = a.num("port", 8377);
    let l = TcpListener::bind(("127.0.0.1", port as u16)).expect("bind");
    println!("serving {} on http://127.0.0.1:{port}/", a.store().display());
    for conn in l.incoming() {
        let Ok(mut c) = conn else { continue };
        let mut buf = [0u8; 4096];
        let n = c.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
        let parts: Vec<&str> = path.trim_matches('/').split('/').filter(|p| !p.is_empty()).collect();
        let nums: Vec<Option<u32>> = parts.iter().skip(1).map(|p| p.parse().ok()).collect();
        let (status, body) = match (parts.first().copied(), nums.as_slice()) {
            (None, _) => ("200 OK", transcriptdb::trace::index_page(&db)),
            (Some("s"), [Some(s)]) if (*s as usize) < db.session_count() => ("200 OK", transcriptdb::trace::session_page(&db, *s)),
            (Some("s"), [Some(s), Some(i)]) if (*s as usize) < db.session_count() && (*i as usize) < db.steps(*s).rows.len() => {
                match transcriptdb::trace::step_page(&db, *s, *i) {
                    Ok(p) => ("200 OK", p),
                    Err(e) => ("500 Internal Server Error", format!("error: {e}")),
                }
            }
            _ => ("404 Not Found", "not found".to_string()),
        };
        let head = format!("HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        let _ = c.write_all(head.as_bytes());
        let _ = c.write_all(body.as_bytes());
    }
}

// ---------------------------------------------------------------------- crash test

const CRASH_SESSIONS: u64 = 6;
const CRASH_PER: usize = 30;

fn crash_child(a: &Args) {
    let seed = a.num("seed", 1);
    let run = a.num("run", 0);
    // small flushes, frequent merges into small segments, sessions leaving memory after one
    // idle flush and tiny caches: kills land in every part of the write path; merges of over
    // 1.6 MB of messages train a dictionary (at a low level, to keep kills frequent)
    let opts = Options { sync: a.sync(), flush_bytes: 384 << 10, merge_at: 3, seg_bytes: 128 << 10, idle_flushes: 1, cache_bytes: 4 << 20, blob_bytes: 512 << 10, run_bytes: 256 << 10, merge_level: 3, threads: 2, ..Options::default() };
    let mut db = Db::open(a.store(), opts).expect("open store");
    {
        let mut l = std::io::stdout().lock();
        writeln!(l, "READY").unwrap();
        l.flush().unwrap();
    }
    let mut r = Rng::new(seed ^ run.wrapping_mul(0x51_7cc1_b727_220a));
    // sessions overlap between runs, so a session keeps growing across crashes
    let base = run * 3;
    let mut queues: Vec<Vec<synth::SynthRequest>> = (base..base + CRASH_SESSIONS).map(|s| synth::session(seed, s, CRASH_PER)).collect();
    let out = std::io::stdout();
    loop {
        let live: Vec<usize> = (0..queues.len()).filter(|&i| !queues[i].is_empty()).collect();
        if live.is_empty() {
            break;
        }
        let mut batch = Vec::new();
        for _ in 0..1 + r.below(6) {
            let live: Vec<usize> = (0..queues.len()).filter(|&i| !queues[i].is_empty()).collect();
            if live.is_empty() {
                break;
            }
            let i = *r.pick(&live);
            let q = queues[i].remove(0);
            let o = db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).expect("put");
            if matches!(o, PutOutcome::Stored(_)) {
                batch.push(q.id);
            }
        }
        db.commit().expect("commit");
        let ms = &db.merge_stats;
        let mut l = out.lock();
        if !batch.is_empty() {
            writeln!(l, "ACK {}", batch.join(" ")).unwrap();
        }
        // folds and rewrites that finished in this process so far
        writeln!(l, "MERGES {} {}", ms.folded, ms.reclaims).unwrap();
        l.flush().unwrap();
        drop(l);
        if db.segment_count() >= 6 && r.chance(0.03) {
            db.compact().expect("compact");
        }
    }
}

fn crash_test(a: &Args) {
    let dir = a.store();
    let kills = a.num("kills", 1000);
    let seed = a.num("seed", 1);
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().unwrap();
    let mut r = Rng::new(seed.wrapping_add(99));
    let mut acked: Vec<String> = Vec::new();
    let mut verified_upto = 0usize;
    let mut expected: HashMap<String, [u8; 32]> = HashMap::new();
    let mut have_session: HashSet<u64> = HashSet::new();
    let mut expect = |id: &str, expected: &mut HashMap<String, [u8; 32]>| -> [u8; 32] {
        let no: u64 = id.split('-').nth(1).unwrap().parse().unwrap();
        if have_session.insert(no) {
            for q in synth::session(seed, no, CRASH_PER) {
                expected.insert(q.id.clone(), sha256(&split::blank_dropped(&q.body, &Keep::default()).unwrap()));
            }
        }
        expected[id]
    };
    let (mut torn, mut replayed, mut orphans, mut lost, mut wrong, mut finished) = (0u64, 0usize, 0usize, 0usize, 0usize, 0u64);
    let (mut folded, mut reclaims) = (0u64, 0u64);
    let t0 = Instant::now();
    for k in 0..kills {
        let mut child = Command::new(&exe)
            .args(["crash-child", "--store", dir.to_str().unwrap(), "--seed", &seed.to_string(), "--run", &k.to_string(), "--sync", a.get("sync").unwrap_or("full")])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn child");
        let stdout = child.stdout.take().unwrap();
        let (ltx, lrx) = mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if ltx.send(line).is_err() {
                    break;
                }
            }
        });
        // one kill in ten lands any time after the start (during recovery too);
        // the rest land while the child is ingesting
        let mut lines = Vec::new();
        if r.chance(0.1) {
            std::thread::sleep(Duration::from_millis(r.below(40)));
        } else {
            while let Ok(l) = lrx.recv_timeout(Duration::from_secs(30)) {
                let ready = l == "READY";
                lines.push(l);
                if ready {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(r.below(150)));
        }
        let still_running = child.try_wait().unwrap().is_none();
        if still_running {
            child.kill().unwrap(); // SIGKILL
        } else {
            finished += 1;
        }
        child.wait().unwrap();
        reader.join().unwrap();
        lines.extend(lrx.try_iter());
        let mut last_merges = (0, 0);
        for l in lines {
            if let Some(rest) = l.strip_prefix("ACK ") {
                acked.extend(rest.split(' ').map(|s| s.to_string()));
            } else if let Some(rest) = l.strip_prefix("MERGES ") {
                let v: Vec<u64> = rest.split(' ').map(|x| x.parse().unwrap()).collect();
                last_merges = (v[0], v[1]);
            }
        }
        folded += last_merges.0;
        reclaims += last_merges.1;

        // reopen (recovery) and check
        let db = Db::open(&dir, Options { sync: SyncMode::Os, ..Options::default() }).expect("recovery failed");
        torn += db.recovery.torn_bytes;
        replayed += db.recovery.replayed;
        orphans += db.recovery.orphans_removed;
        let mut check = |id: &str, expected: &mut HashMap<String, [u8; 32]>| {
            let want = expect(id, expected);
            match db.request_index(id) {
                None => {
                    lost += 1;
                    eprintln!("LOST acknowledged request {id} after kill {k}");
                }
                Some(ri) => match db.rebuild(ri) {
                    Ok(b) if sha256(&b) == want => {}
                    _ => {
                        wrong += 1;
                        eprintln!("WRONG bytes for {id} after kill {k}");
                    }
                },
            }
        };
        for id in acked[verified_upto..].iter() {
            check(id, &mut expected);
        }
        verified_upto = acked.len();
        for _ in 0..50.min(acked.len()) {
            let id = acked[r.below(acked.len() as u64) as usize].clone();
            check(&id, &mut expected);
        }
        if (k + 1) % 100 == 0 {
            println!("  {} kills, {} acknowledged so far, lost {lost}, wrong {wrong}, {:.0}s", k + 1, acked.len(), t0.elapsed().as_secs_f64());
        }
    }
    // final full check: every acknowledged request, and every stored request against the generator
    let db = Db::open(&dir, Options { sync: SyncMode::Os, ..Options::default() }).expect("recovery failed");
    let mut stored_wrong = 0;
    let all = db.all_requests().expect("list requests");
    if all.len() != db.request_count() {
        eprintln!("request count {} but {} requests listed", db.request_count(), all.len());
        stored_wrong += 1;
    }
    for i in all {
        let id = db.request(i).expect("read request").id.to_string();
        let want = expect(&id, &mut expected);
        if db.rebuild(i).map(|b| sha256(&b) != want).unwrap_or(true) {
            stored_wrong += 1;
        }
    }
    let sc = transcriptdb::check::check_steps(&db, 4).expect("step check");
    let ack_set: HashSet<&String> = acked.iter().collect();
    let missing_final = ack_set.iter().filter(|id| db.request_index(id).is_none()).count();
    println!(
        "crash test: {kills} runs ({} killed with SIGKILL, {finished} finished first), {} requests acknowledged, {} stored",
        kills - finished,
        ack_set.len(),
        db.request_count()
    );
    println!("  lost after any kill: {lost}; wrong bytes after any kill: {wrong}");
    println!("  final check: acknowledged but missing {missing_final}; stored requests that rebuild wrong {stored_wrong}");
    println!("  steps after all kills: {} engine steps; missing {}, extra {}, wrong prev {}, wrong shape {}", sc.engine_steps, sc.missing, sc.extra, sc.prev_wrong, sc.shape_wrong);
    println!("  recovery saw: {torn} torn log bytes cut, {replayed} log records replayed, {orphans} orphan files removed");
    println!("  merges seen to finish before a kill: at least {folded} sessions folded, {reclaims} segments rewritten to drop replaced parts; replaced parts left {}", mb(db.stats().dead_bytes));
    println!("  {:.0}s", t0.elapsed().as_secs_f64());
    if lost + wrong + missing_final + stored_wrong > 0 || !sc.ok() {
        std::process::exit(1);
    }
}

fn main() {
    let a = Args::parse();
    match a.cmd.as_str() {
        "ingest-claude" => ingest_claude(&a),
        "check-claude" => check_claude(&a),
        "stats" => stats(&a),
        "compact" => compact(&a),
        "check-steps" => check_steps(&a),
        "serve" => serve(&a),
        "query" => query(&a),
        "check-queries" => check_queries(&a),
        "h2h" => head_to_head(&a),
        "synth" => synth_store(&a),
        "steps" => steps_summary(&a),
        "bench" => bench(&a),
        "crash-child" => crash_child(&a),
        "crash-test" => crash_test(&a),
        "growth" => growth(&a),
        "probe" => probe(&a),
        "ceiling" => ceiling(&a),
        _ => {
            eprintln!("usage: tdb <ingest-claude|check-claude|stats|compact|bench|crash-test|ceiling|h2h|...> --store DIR [options]");
            std::process::exit(2);
        }
    }
    let _ = Path::new(".");
}

/// Where a store's bytes are, and the store's distinct data written out as plain streams
/// (`--out DIR`) for an archiver to compress as hard as it can: messages once each across
/// all sessions (stored form, and with opaque values put back), opaque values raw by kind,
/// tree structure, request columns and shared pieces. Prints sizes only.
fn ceiling(a: &Args) {
    use transcriptdb::codec::Enc;
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let st = db.stats();
    let seg = db.segment_split().expect("segment split");
    println!("store {} = segments {} + index runs {} + blob files {}", mb(st.store_bytes()), mb(st.seg_file_bytes), mb(st.index_file_bytes), mb(st.blob_file_bytes));
    println!("  frames {} ({} of stored messages)", mb(seg[0]), mb(st.node_bytes));
    println!("  message/request blocks {} (raw {}), step blocks {} (raw {}), directory blocks {} (raw {}), catalogs and footers {}", mb(seg[1]), mb(seg[4]), mb(seg[2]), mb(seg[5]), mb(seg[3]), mb(seg[6]), mb(seg[7]));
    println!("  pieces {} (raw {}, {} pieces)", mb(st.piece_bytes), mb(st.piece_raw_bytes), st.pieces);
    for (k, n, b) in db.opaque_kinds().expect("opaque kinds") {
        println!("  opaque {k}: {n} values {} raw ({} as base64)", mb(b), mb(b.div_ceil(3) * 4));
    }
    println!("  blob file bytes other than pieces and opaque values {}", mb(st.blob_file_bytes - st.piece_bytes - st.opaque_bytes));
    let mut pk: std::collections::BTreeMap<u8, [u64; 4]> = Default::default();
    db.each_piece(|_, kind, raw, clen, base| {
        let e = pk.entry(kind).or_default();
        let w = if base == 0 { 0 } else { 2 };
        e[w] += 1;
        e[w + 1] += clen as u64;
        let _ = raw;
    })
    .expect("pieces");
    for (k, e) in pk {
        println!("  pieces of kind {k}: {} whole ({}), {} against an earlier piece ({})", e[0], mb(e[1]), e[2], mb(e[3]));
    }
    let Some(out) = a.get("out") else { return };
    let out = PathBuf::from(out);
    std::fs::create_dir_all(&out).expect("create --out");
    let create = |n: &str| std::io::BufWriter::new(std::fs::File::create(out.join(n)).expect("create stream file"));
    // opaque values raw, by kind
    let names = ["other", "signature", "image", "document", "redacted", "thinking"];
    let mut ofiles: Vec<_> = names.iter().map(|n| create(&format!("opq-{n}.bin"))).collect();
    let mut olens: Vec<Enc> = names.iter().map(|_| Enc::new()).collect();
    db.each_opaque(|_, kind, _, p| {
        let k = (kind as usize).min(names.len() - 1);
        ofiles[k].write_all(p).unwrap();
        olens[k].var(p.len() as u64);
    })
    .expect("opaque values");
    for (i, n) in names.iter().enumerate() {
        std::fs::write(out.join(format!("opq-{n}.len")), &olens[i].buf).unwrap();
    }
    let mut pieces = create("pieces.bin");
    let mut plens = Enc::new();
    db.each_piece(|_, kind, b, _, _| {
        pieces.write_all(b).unwrap();
        plens.u8(kind);
        plens.var(b.len() as u64);
    })
    .expect("pieces");
    std::fs::write(out.join("pieces.len"), &plens.buf).unwrap();
    // messages once each, in storage order, sessions in id order
    let (mut stored, mut full) = (create("msgs.bin"), create("full.bin"));
    let (mut tree, mut holes) = (Enc::new(), Enc::new());
    let (mut rid, mut rtime, mut rmeta, mut rrest, mut rlayout) = (Enc::new(), Enc::new(), Enc::new(), Enc::new(), Enc::new());
    let mut seen: HashMap<u128, u32> = HashMap::new();
    let (mut n_nodes, mut n_distinct, mut stored_bytes, mut full_bytes) = (0u64, 0u64, 0u64, 0u64);
    let mut last_opaque = 0u64;
    for s in 0..db.session_count() as u32 {
        let nodes = db.stored_nodes(s).expect("read session");
        let ids: Vec<u32> = nodes.iter().map(|n| n.0).collect();
        let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).expect("read messages");
        let mut pos_of = vec![0u32; nodes.len()];
        for (p, n) in nodes.iter().enumerate() {
            pos_of[n.0 as usize] = p as u32;
        }
        tree.var(nodes.len() as u64);
        for (p, (n, m)) in nodes.iter().zip(&msgs).enumerate() {
            n_nodes += 1;
            tree.var(if n.1 == u32::MAX { 0 } else { (p as u32 - pos_of[n.1 as usize]) as u64 });
            let key = xxhash_rust::xxh3::xxh3_128(m);
            match seen.get(&key) {
                Some(&g) => tree.var(g as u64 + 1),
                None => {
                    seen.insert(key, n_distinct as u32);
                    n_distinct += 1;
                    tree.var(0);
                    tree.var(n.2.len() as u64);
                    stored.write_all(&n.2).unwrap();
                    full.write_all(m).unwrap();
                    stored_bytes += n.2.len() as u64;
                    full_bytes += m.len() as u64;
                    holes.var(n.3.len() as u64);
                    let mut prev = 0u32;
                    for &(o, id) in &n.3 {
                        holes.var((o - prev) as u64);
                        prev = o;
                        holes.var(id.wrapping_sub(last_opaque).wrapping_add(1) & 0xffff_ffff);
                        last_opaque = id;
                    }
                }
            }
        }
        for ri in db.session_requests(s) {
            let r = db.request(ri).expect("request");
            rid.bytes(r.id.as_bytes());
            rtime.bytes(r.time.as_bytes());
            rmeta.bytes(&r.meta);
            rrest.var(if r.tip == u32::MAX { 0 } else { pos_of[r.tip as usize] as u64 + 1 });
            rrest.var(r.n_msgs as u64);
            for v in [r.skeleton, r.system, r.tools] {
                rrest.var(v as u64);
            }
            rrest.var(r.notes.len() as u64);
            for n in r.notes.iter() {
                rrest.var(n.target as u64);
                rrest.var(n.offset as u64);
                rrest.var(n.text as u64);
            }
            match &r.layout {
                None => rlayout.u8(0),
                Some(g) => {
                    rlayout.u8(1);
                    rlayout.var(g.len() as u64);
                    for x in g.iter() {
                        rlayout.bytes(x);
                    }
                }
            }
        }
    }
    for (n, e) in [("tree.bin", tree), ("holes.bin", holes), ("req-id.bin", rid), ("req-time.bin", rtime), ("req-meta.bin", rmeta), ("req-rest.bin", rrest), ("req-layout.bin", rlayout)] {
        std::fs::write(out.join(n), &e.buf).unwrap();
    }
    println!("wrote {}: {} nodes, {} distinct messages ({} stored form, {} with opaque values as text)", out.display(), n_nodes, n_distinct, mb(stored_bytes), mb(full_bytes));
}

/// Highest resident memory of this process so far, in bytes.
fn peak_rss() -> u64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let v = u.ru_maxrss as u64;
    if cfg!(target_os = "macos") { v } else { v * 1024 }
}

/// Open a store and use it a little, for `growth`: one line of numbers.
fn probe(a: &Args) {
    let t = Instant::now();
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let open_s = t.elapsed().as_secs_f64();
    let rss_open = peak_rss();
    let st = db.stats();
    let mut r = Rng::new(a.num("seed", 3));
    let mut lat = Vec::new();
    for _ in 0..a.num("n", 2000) {
        let s = r.below(db.session_count() as u64) as u32;
        let reqs = db.session_requests(s);
        let ri = reqs[r.below(reqs.len() as u64) as usize];
        let t = Instant::now();
        db.rebuild(ri).expect("rebuild");
        lat.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let rss_reads = peak_rss();
    // the first 200 reads find cold caches in a fresh process; the last half, warm ones
    let mut cold = lat[..lat.len().min(200)].to_vec();
    let mut warm = lat[lat.len() / 2..].to_vec();
    let t = Instant::now();
    let res = transcriptdb::query::run(&db, r#"where tool == "Bash" and failed | summarize n = count()"#, 1).expect("query");
    let q_s = t.elapsed().as_secs_f64();
    let rss_query = peak_rss();
    let hits = transcriptdb::query::exec::render(&db, &res.table, 1).map(|(_, c)| c.first().and_then(|r| r.first()).cloned().unwrap_or_default()).unwrap_or_default();
    println!(
        "PROBE {open_s:.4} {} {rss_open} {:.3} {:.3} {rss_reads} {q_s:.3} {rss_query} {} {} {} {hits} {:.3} {:.3} {}",
        st.catalog_bytes,
        q(&mut cold.clone(), 0.5),
        q(&mut cold, 0.99),
        st.segments,
        st.level0,
        st.blobs,
        q(&mut warm.clone(), 0.5),
        q(&mut warm, 0.99),
        st.runs
    );
}

/// Grow a store from 1x to 2^(rounds-1)x synthetic sessions and measure, after each round
/// and in a fresh process, what opening and using it costs. A share of sessions pause and
/// take more requests a round later, after their data has been flushed and merged.
fn growth(a: &Args) {
    let dir = a.store();
    let _ = std::fs::remove_dir_all(&dir);
    let rounds = a.num("rounds", 5);
    let base = a.num("base", 300);
    let wake: f64 = a.get("wake").map(|v| v.parse().expect("number")).unwrap_or(0.1);
    let seed = a.num("seed", 17);
    let opts = Options { sync: SyncMode::Os, ..a.opts() };
    let mut db = Db::open(&dir, opts.clone()).expect("open store");
    let mut r = Rng::new(seed);
    let mut next_no = 0u64;
    // sessions to wake next round (their last requests); generated again when needed, since
    // every synthetic request carries its whole history and a round's bodies run to gigabytes
    let mut waiting: Vec<u64> = Vec::new();
    let (mut raw, mut woken) = (0u64, 0u64);
    let mut acc = transcriptdb::db::MergeStats::default();
    let exe = std::env::current_exe().expect("own path");
    println!("round  sessions  requests   raw in    store  | open s  catalogs  RSS open  RSS reads | cold med   p99 | warm med   p99 ms | query s  RSS query | segs (L0) runs blobs | split (max) | rewritten | ingest MB/s");
    for round in 0..rounds {
        let target = base << round;
        let t = Instant::now();
        // (session number, wake-up?) for this round
        let mut work: Vec<(u64, bool)> = std::mem::take(&mut waiting).into_iter().map(|n| (n, true)).collect();
        woken += work.len() as u64;
        while next_no < target {
            work.push((next_no, false));
            if r.chance(wake) {
                waiting.push(next_no);
            }
            next_no += 1;
        }
        let pausing: std::collections::HashSet<u64> = waiting.iter().copied().collect();
        let mut round_raw = 0u64;
        // like real traffic: 64 sessions at a time, their requests interleaved
        for chunk in work.chunks(64) {
            let mut queues: Vec<Vec<synth::SynthRequest>> = chunk
                .iter()
                .map(|&(no, wake_up)| {
                    let mut reqs = synth::session(seed, no, 45);
                    if wake_up {
                        reqs.drain(..30);
                    } else if pausing.contains(&no) {
                        reqs.truncate(30);
                    }
                    reqs.reverse();
                    reqs
                })
                .collect();
            loop {
                let mut any = false;
                for s in queues.iter_mut() {
                    if let Some(q) = s.pop() {
                        db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).expect("put");
                        round_raw += q.body.len() as u64;
                        any = true;
                    }
                }
                db.commit().expect("commit");
                if !any {
                    break;
                }
            }
        }
        db.flush().expect("flush");
        raw += round_raw;
        let ingest_s = t.elapsed().as_secs_f64();
        let st = db.stats();
        acc.merges += st.merges.merges;
        acc.flushed += st.merges.flushed;
        acc.bytes_out += st.merges.bytes_out;
        acc.folded += st.merges.folded;
        acc.reclaims += st.merges.reclaims;
        let parts: Vec<usize> = (0..st.sessions as u32).map(|s| db.part_count(s).expect("session parts")).collect();
        let split = parts.iter().filter(|&&p| p > 1).count();
        let most = parts.iter().max().copied().unwrap_or(0);
        // the probe opens the store in its own process; one process at a time
        drop(db);
        let out = Command::new(&exe).args(["probe", "--store", dir.to_str().unwrap(), "--cache-mb", &a.num("cache-mb", 256).to_string()]).output().expect("run probe");
        let text = String::from_utf8_lossy(&out.stdout);
        let line = text.lines().find(|l| l.starts_with("PROBE ")).unwrap_or_else(|| panic!("probe failed: {}", String::from_utf8_lossy(&out.stderr)));
        let f: Vec<&str> = line.split_whitespace().skip(1).collect();
        db = Db::open(&dir, opts.clone()).expect("open store again");
        let store = st.store_bytes();
        let rewritten = (acc.flushed + acc.bytes_out) as f64 / store.max(1) as f64;
        let n = |i: usize| -> f64 { f[i].parse().unwrap_or(0.0) };
        println!(
            "{round:>5}  {:>8}  {:>8}  {:>8}  {:>7}  | {:>6.3}  {:>8}  {:>8}  {:>9} | {:>8.3} {:>5.3} | {:>8.3} {:>5.3}    | {:>7.3}  {:>9} | {:>4} ({}) {:>4} {:>5} | {:>5} ({:>2})  | {:>8.2}x | {:>6.0}",
            st.sessions,
            st.requests,
            mb(raw),
            mb(store),
            n(0),
            format!("{:.1} KB", n(1) / 1e3),
            mb(n(2) as u64),
            mb(n(5) as u64),
            n(3),
            n(4),
            n(12),
            n(13),
            n(6),
            mb(n(7) as u64),
            f[8],
            f[9],
            f[14],
            f[10],
            split,
            most,
            rewritten,
            round_raw as f64 / 1e6 / ingest_s
        );
    }
    let st = db.stats();
    println!(
        "sessions woken after their data was flushed and merged: {woken}; merges {}; bytes written by flushes {} and merges {} for a store of {}; sessions folded {}, segments rewritten to drop replaced parts {}, replaced parts left {}",
        acc.merges,
        mb(acc.flushed),
        mb(acc.bytes_out),
        mb(st.store_bytes()),
        acc.folded,
        acc.reclaims,
        mb(st.dead_bytes)
    );
}
