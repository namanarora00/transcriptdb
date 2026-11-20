//! End-to-end tests of the store on synthetic sessions (no real logs).

use std::path::PathBuf;
use transcriptdb::synth::{self, SynthRequest};
use transcriptdb::{Db, Options, Put, PutOutcome, SyncMode};

fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn opts(flush: usize) -> Options {
    Options { flush_bytes: flush, sync: SyncMode::Os, ..Options::default() }
}

/// Requests of several sessions, interleaved the way real traffic arrives.
fn traffic(seed: u64, sessions: u64, per: usize) -> Vec<SynthRequest> {
    let mut all: Vec<Vec<SynthRequest>> = (0..sessions).map(|s| synth::session(seed, s, per)).collect();
    let mut out = Vec::new();
    let mut r = synth::Rng::new(seed);
    while all.iter().any(|v| !v.is_empty()) {
        let live: Vec<usize> = (0..all.len()).filter(|&i| !all[i].is_empty()).collect();
        let i = *r.pick(&live);
        out.push(all[i].remove(0));
    }
    out
}

fn put(db: &mut Db, q: &SynthRequest) -> PutOutcome {
    db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body, digest: None }).unwrap()
}

fn check_all(db: &Db, reqs: &[SynthRequest]) {
    for q in reqs {
        let ri = db.request_index(&q.id).unwrap_or_else(|| panic!("{} missing", q.id));
        let got = db.rebuild_checked(ri).unwrap();
        assert!(got == q.body, "{} rebuilt differently", q.id);
    }
}

#[test]
fn rebuild_through_flush_reopen_and_compact() {
    let dir = tmpdir("cycle");
    let reqs = traffic(11, 12, 40);
    {
        let mut db = Db::open(&dir, opts(256 << 10)).unwrap();
        for (i, q) in reqs.iter().enumerate() {
            assert!(matches!(put(&mut db, q), PutOutcome::Stored(_)));
            if i % 7 == 0 {
                db.commit().unwrap();
            }
        }
        db.commit().unwrap();
        check_all(&db, &reqs);
        assert!(db.stats().segments > 1, "small flush size should have made several segments");
    }
    let mut db = Db::open(&dir, opts(256 << 10)).unwrap();
    assert!(db.recovery.replayed > 0 || db.recovery.segments > 0);
    check_all(&db, &reqs);
    let before: Vec<u32> = (0..db.request_count() as u32).map(|i| db.request_runs(i).unwrap()).collect();
    db.compact().unwrap();
    assert_eq!(db.stats().segments, 1);
    check_all(&db, &reqs);
    let after: Vec<u32> = (0..db.request_count() as u32).map(|i| db.request_runs(i).unwrap()).collect();
    let one = after.iter().filter(|&&r| r == 1).count();
    let max = *after.iter().max().unwrap();
    eprintln!(
        "runs: before mean {:.2}, after mean {:.2}, single-run {}/{} max {max}",
        before.iter().sum::<u32>() as f64 / before.len() as f64,
        after.iter().sum::<u32>() as f64 / after.len() as f64,
        one,
        after.len()
    );
    assert!(one * 10 >= after.len() * 8, "main line first should make most requests one run");
    drop(db);
    let db = Db::open(&dir, opts(256 << 10)).unwrap();
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn committed_puts_survive_without_flush() {
    let dir = tmpdir("wal");
    let reqs = traffic(5, 4, 25);
    {
        let mut db = Db::open(&dir, opts(1 << 30)).unwrap();
        for q in &reqs {
            put(&mut db, q);
        }
        db.commit().unwrap();
        assert_eq!(db.stats().segments, 0);
        // dropped without a flush, like a killed process
    }
    let db = Db::open(&dir, opts(1 << 30)).unwrap();
    assert_eq!(db.recovery.replayed, reqs.len());
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn torn_log_tail_and_orphans_are_cleaned() {
    use std::io::Write;
    let dir = tmpdir("torn");
    let reqs = traffic(9, 3, 20);
    {
        let mut db = Db::open(&dir, opts(1 << 30)).unwrap();
        for q in &reqs {
            put(&mut db, q);
        }
        db.commit().unwrap();
    }
    // a half-written record at the end of the newest log, and a stray segment file
    let mut logs: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|e| e == "log")).collect();
    logs.sort();
    let last = logs.iter().rev().find(|p| std::fs::metadata(p).unwrap().len() > 8).unwrap();
    std::fs::OpenOptions::new().append(true).open(last).unwrap().write_all(&[200, 1, 0, 0, 9, 9, 9]).unwrap();
    std::fs::write(dir.join("seg-000099.tdb"), b"junk").unwrap();
    let db = Db::open(&dir, opts(1 << 30)).unwrap();
    assert_eq!(db.recovery.torn_bytes, 7);
    assert_eq!(db.recovery.orphans_removed, 1);
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn duplicate_ids_are_stored_once() {
    let dir = tmpdir("dup");
    let reqs = synth::session(3, 1, 10);
    let mut db = Db::open(&dir, opts(1 << 30)).unwrap();
    for q in &reqs {
        put(&mut db, q);
    }
    assert!(matches!(put(&mut db, &reqs[4]), PutOutcome::Duplicate(4)));
    db.commit().unwrap();
    assert_eq!(db.request_count(), 10);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn messages_are_stored_once_per_position() {
    let dir = tmpdir("dedup");
    let reqs = synth::session(21, 2, 60);
    let mut db = Db::open(&dir, opts(1 << 30)).unwrap();
    for q in &reqs {
        put(&mut db, q);
    }
    db.commit().unwrap();
    let total_msgs: u64 = (0..db.request_count() as u32).map(|i| db.request(i).n_msgs as u64).sum();
    let st = db.stats();
    eprintln!("{} requests carry {total_msgs} messages; {} stored", db.request_count(), st.nodes);
    assert!((st.nodes as u64) * 5 < total_msgs);
    std::fs::remove_dir_all(&dir).unwrap();
}
