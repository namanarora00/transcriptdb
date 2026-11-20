//! `tdb`: command-line tool for the TranscriptDB storage core.
//!
//!   tdb ingest-claude --store DIR [--logs DIR] [--max-files N] [--seed S] [--threads N] [--sync full|fsync|os]
//!   tdb check-claude  --store DIR [--logs DIR] [--threads N]
//!   tdb stats         --store DIR
//!   tdb compact       --store DIR
//!   tdb bench         --store DIR [--n 300] [--seed S]
//!   tdb crash-test    --store DIR [--kills 1000] [--seed S] [--sync full|fsync|os]
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
    fn opts(&self) -> Options {
        Options { sync: self.sync(), flush_bytes: (self.num("flush-mb", 64) as usize) << 20, ..Options::default() }
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
                    .put(&Put { session: &r.session, agent: &r.agent, id: &r.id, time: &r.time, meta: &r.meta, body, digest: r.digest })
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
    let bad_log = AtomicUsize::new(0);
    let bad_stored = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let bytes = AtomicUsize::new(0);
    let missing: Mutex<Vec<usize>> = Mutex::new(Vec::new());
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
                    let h = sha256(&out);
                    let stored_ok = h == db.request(ri).digest;
                    let log_ok = d.is_none_or(|d| d == h);
                    if !stored_ok {
                        bad_stored.fetch_add(1, Ordering::Relaxed);
                    }
                    if !log_ok {
                        bad_log.fetch_add(1, Ordering::Relaxed);
                    }
                    if stored_ok && log_ok {
                        ok.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failures.lock().unwrap().push(format!("{id}: rebuilt bytes differ (stored digest ok: {stored_ok}, log digest ok: {log_ok})"));
                    }
                }
            });
        }
    });
    let secs = t1.elapsed().as_secs_f64();
    let missing = missing.into_inner().unwrap();
    let stored_in_logs = list.len() - missing.len();
    println!("requests recorded in the logs now: {} distinct ids ({raw_lines} request lines; read in {t_read:.1}s)", list.len());
    println!("  in the store: {stored_in_logs}");
    println!("    rebuilt byte for byte (sha256 equal to Claude Code's requestDigest and to the digest taken at ingest): {}", ok.load(Ordering::Relaxed));
    println!("    rebuilt but different from requestDigest: {}", bad_log.load(Ordering::Relaxed));
    println!("    rebuilt but different from the ingest digest: {}", bad_stored.load(Ordering::Relaxed));
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
    let failed = bad_log.load(Ordering::Relaxed) + bad_stored.load(Ordering::Relaxed) + errors.load(Ordering::Relaxed);
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
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let st = db.stats();
    let n = db.request_count() as u32;
    let mut runs: HashMap<u32, usize> = HashMap::new();
    let mut msgs = 0u64;
    for i in 0..n {
        *runs.entry(db.request_runs(i).unwrap()).or_default() += 1;
        msgs += db.request(i).n_msgs as u64;
    }
    let opaque_text: u64 = st.opaque_bytes * 4 / 3;
    let data = st.frame_bytes + st.piece_bytes + st.opq_file_bytes + st.index_bytes;
    println!("sessions {}  requests {}  messages in requests {}  stored messages (tree nodes) {}", st.sessions, st.requests, msgs, st.nodes);
    println!("distinct message text: {} ({} message bytes + about {} of base64 opaque values)", mb(st.node_bytes + opaque_text), mb(st.node_bytes), mb(opaque_text));
    println!("on disk: segments {} + opaque {} = {} across {} segment(s); log {}", mb(st.seg_file_bytes), mb(st.opq_file_bytes), mb(st.seg_file_bytes + st.opq_file_bytes), st.segments, mb(st.wal_bytes));
    println!("  message frames {}  ({:.2}x smaller than the {} they hold)", mb(st.frame_bytes), st.node_bytes as f64 / st.frame_bytes.max(1) as f64, mb(st.node_bytes));
    println!("  opaque values {} raw ({} values)", mb(st.opq_file_bytes), st.opaques);
    println!("  shared pieces {} compressed from {} ({} pieces: skeletons, system, tools, markers)", mb(st.piece_bytes), mb(st.piece_raw_bytes), st.pieces);
    println!("  index {}", mb(st.index_bytes));
    println!("  all data files: {} for {} of distinct text ({:.3} bytes per byte)", mb(data), mb(st.node_bytes + opaque_text), data as f64 / (st.node_bytes + opaque_text).max(1) as f64);
    let mut rv: Vec<_> = runs.into_iter().collect();
    rv.sort();
    let tot = n.max(1) as f64;
    let line: Vec<String> = rv.iter().take(6).map(|(r, c)| format!("{r} run(s): {} ({:.1}%)", c, *c as f64 * 100.0 / tot)).collect();
    println!("storage runs per request: {}; max {}", line.join(", "), rv.last().map(|x| x.0).unwrap_or(0));
}

fn compact(a: &Args) {
    let mut db = Db::open(a.store(), a.opts()).expect("open store");
    let t = Instant::now();
    db.compact().expect("compact");
    println!("merged into one segment in {:.1}s", t.elapsed().as_secs_f64());
}

fn bench(a: &Args) {
    let db = Db::open(a.store(), a.opts()).expect("open store");
    let n = a.num("n", 300) as usize;
    let mut r = Rng::new(a.num("seed", 5));
    let total = db.request_count() as u64;
    let pick: Vec<u32> = (0..n).map(|_| r.below(total) as u32).collect();
    let long: Vec<u32> = (0..total as u32).filter(|&i| db.request(i).n_msgs >= 40).collect();
    let ranges: Vec<(u32, u32, usize)> = (0..n.min(long.len() * 4))
        .map(|_| {
            let ri = *r.pick(&long);
            let k = r.below(db.request(ri).n_msgs as u64 - 40 + 1) as usize;
            (ri, db.request(ri).session, k)
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

// ---------------------------------------------------------------------- crash test

const CRASH_SESSIONS: u64 = 6;
const CRASH_PER: usize = 30;

fn crash_child(a: &Args) {
    let seed = a.num("seed", 1);
    let run = a.num("run", 0);
    let opts = Options { sync: a.sync(), flush_bytes: 384 << 10, ..Options::default() };
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
            let o = db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body, digest: None }).expect("put");
            if matches!(o, PutOutcome::Stored(_)) {
                batch.push(q.id);
            }
        }
        db.commit().expect("commit");
        if !batch.is_empty() {
            let mut l = out.lock();
            writeln!(l, "ACK {}", batch.join(" ")).unwrap();
            l.flush().unwrap();
        }
        if db.segment_count() >= 6 && r.chance(0.5) {
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
                expected.insert(q.id.clone(), sha256(&q.body));
            }
        }
        expected[id]
    };
    let (mut torn, mut replayed, mut orphans, mut lost, mut wrong, mut finished) = (0u64, 0usize, 0usize, 0usize, 0usize, 0u64);
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
        for l in lines {
            if let Some(rest) = l.strip_prefix("ACK ") {
                acked.extend(rest.split(' ').map(|s| s.to_string()));
            }
        }

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
    for i in 0..db.request_count() as u32 {
        let id = db.request(i).id.to_string();
        let want = expect(&id, &mut expected);
        if db.rebuild(i).map(|b| sha256(&b) != want).unwrap_or(true) {
            stored_wrong += 1;
        }
    }
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
    println!("  recovery saw: {torn} torn log bytes cut, {replayed} log records replayed, {orphans} orphan files removed");
    println!("  {:.0}s", t0.elapsed().as_secs_f64());
    if lost + wrong + missing_final + stored_wrong > 0 {
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
        "bench" => bench(&a),
        "crash-child" => crash_child(&a),
        "crash-test" => crash_test(&a),
        _ => {
            eprintln!("usage: tdb <ingest-claude|check-claude|stats|compact|bench|crash-test> --store DIR [options]");
            std::process::exit(2);
        }
    }
    let _ = Path::new(".");
}
