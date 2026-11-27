//! The store shared by many threads: writers on many sessions at once, writers racing on
//! one session, readers on snapshots while flushes and merges replace files.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use transcriptdb::split::{blank_dropped, Keep};
use transcriptdb::synth::{self, SynthRequest};
use transcriptdb::{Db, Options, PutOutcome, Request, Store, StoreOptions, SyncMode};

fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-conc-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Small flushes and merges, so they happen often while the threads work.
fn opts() -> Options {
    Options { flush_bytes: 64 << 10, merge_at: 2, seg_bytes: 256 << 10, sync: SyncMode::Os, idle_flushes: 1, cache_bytes: 8 << 20, ..Options::default() }
}

fn req(q: &SynthRequest) -> Request {
    Request { session: q.session.clone(), agent: q.agent.clone(), id: q.id.clone(), time: q.time.clone(), meta: br#"{"k":1}"#.to_vec(), body: q.body.clone(), response: Vec::new() }
}

fn want(q: &SynthRequest) -> Vec<u8> {
    blank_dropped(&q.body, &Keep::default()).unwrap()
}

fn check(db: &Db, q: &SynthRequest) {
    let ri = db.request_index(&q.id).unwrap_or_else(|| panic!("{} missing", q.id));
    assert_eq!(db.rebuild(ri).unwrap(), want(q), "{} rebuilt differently", q.id);
}

#[test]
fn many_writers_and_readers() {
    let dir = tmpdir("many");
    let store = Arc::new(Store::open(&dir, opts(), StoreOptions { prepare_threads: 2, ..StoreOptions::default() }).unwrap());
    // 8 writers on sessions of their own; 2 more racing on one shared session
    let own: Vec<Vec<SynthRequest>> = (0..8).map(|w| (0..3).flat_map(|k| synth::session(5, w * 10 + k, 30)).collect()).collect();
    let shared: Vec<SynthRequest> = synth::session(5, 999, 60);
    let acked: Arc<Mutex<Vec<SynthRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));

    // readers: every request acknowledged before a snapshot is taken is in it, and rebuilds
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (store, acked, done, reads) = (store.clone(), acked.clone(), done.clone(), reads.clone());
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let seen: Vec<SynthRequest> = acked.lock().unwrap().clone();
                    let snap = store.snapshot().unwrap();
                    for q in seen.iter().rev().take(20) {
                        check(&snap, q);
                    }
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let mut writers = Vec::new();
    for reqs in own {
        let (store, acked) = (store.clone(), acked.clone());
        writers.push(std::thread::spawn(move || {
            for chunk in reqs.chunks(3) {
                let rs = store.put(chunk.iter().map(req).collect());
                for (q, r) in chunk.iter().zip(rs) {
                    assert!(matches!(r.unwrap(), PutOutcome::Stored(_)), "{}", q.id);
                    acked.lock().unwrap().push(q.clone());
                }
            }
        }));
    }
    // the shared session: two threads put the same requests (duplicates must be caught
    // once each, whichever thread gets there first)
    let stored = Arc::new(AtomicUsize::new(0));
    for t in 0..2 {
        let (store, acked, stored, reqs) = (store.clone(), acked.clone(), stored.clone(), shared.clone());
        writers.push(std::thread::spawn(move || {
            for q in reqs.iter().skip(t) {
                match store.put(vec![req(q)]).pop().unwrap().unwrap() {
                    PutOutcome::Stored(_) => {
                        stored.fetch_add(1, Ordering::Relaxed);
                    }
                    PutOutcome::Duplicate(_) => {}
                }
                acked.lock().unwrap().push(q.clone());
            }
        }));
    }
    for w in writers {
        w.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    assert_eq!(stored.load(Ordering::Relaxed), shared.len(), "each request of the shared session is stored once");
    assert!(reads.load(Ordering::Relaxed) > 0);

    let all: Vec<SynthRequest> = acked.lock().unwrap().clone();
    let snap = store.snapshot().unwrap();
    for q in &all {
        check(&snap, q);
    }
    let gs = transcriptdb::store::group_stats();
    assert!(gs.groups > 0 && gs.requests >= all.len() as u64);
    drop(snap);
    store.close();
    drop(store);

    // everything acknowledged survives a reopen
    let db = Db::open(&dir, opts()).unwrap();
    for q in &all {
        check(&db, q);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_outlives_merges_that_replace_its_files() {
    let dir = tmpdir("old");
    let store = Store::open(&dir, opts(), StoreOptions::default()).unwrap();
    let first: Vec<SynthRequest> = (0..6).flat_map(|s| synth::session(9, s, 25)).collect();
    for c in first.chunks(10) {
        for r in store.put(c.iter().map(req).collect()) {
            r.unwrap();
        }
    }
    store.exec(|db| db.settle().unwrap()).unwrap();
    let old = store.snapshot().unwrap();
    let segs_before: Vec<u32> = old.segments().iter().map(|s| s.0).collect();
    assert!(!segs_before.is_empty());

    // more writes, flushes and merges, until none of the old snapshot's segments is live
    let more: Vec<SynthRequest> = (0..6).flat_map(|s| synth::session(9, s + 100, 25)).chain((0..6).flat_map(|s| synth::session(9, s, 40).into_iter().skip(25))).collect();
    for c in more.chunks(10) {
        for r in store.put(c.iter().map(req).collect()) {
            r.unwrap();
        }
    }
    store.exec(|db| db.compact().unwrap()).unwrap();
    let now = store.snapshot().unwrap();
    let segs_now: Vec<u32> = now.segments().iter().map(|s| s.0).collect();
    assert!(segs_before.iter().all(|n| !segs_now.contains(n)), "compaction replaced every old segment");

    // the old snapshot still reads everything it held, from files no longer in the store
    for q in &first {
        check(&old, q);
    }
    // and does not see what came after it
    assert!(old.request_index(&more[0].id).is_none());
    for q in first.iter().chain(&more) {
        check(&now, q);
    }
    // its files go once it does
    let path = |n: u32| dir.join(format!("seg-{n:06}.tdb"));
    let still: usize = segs_before.iter().filter(|&&n| path(n).exists()).count();
    drop(old);
    transcriptdb::block::wait_removed();
    let after: usize = segs_before.iter().filter(|&&n| path(n).exists()).count();
    assert_eq!(after, 0, "old segments deleted once the snapshot was dropped ({still} were kept for it)");
    drop(now);
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_snapshot_is_a_point_in_time() {
    let dir = tmpdir("pit");
    let store = Store::open(&dir, opts(), StoreOptions::default()).unwrap();
    let reqs = synth::session(3, 1, 40);
    for q in &reqs[..20] {
        store.put(vec![req(q)]).pop().unwrap().unwrap();
    }
    let at20 = store.snapshot().unwrap();
    for q in &reqs[20..] {
        store.put(vec![req(q)]).pop().unwrap().unwrap();
    }
    let s = at20.session_index(&reqs[0].session, &reqs[0].agent).unwrap().unwrap();
    assert_eq!(at20.session_requests(s).len(), 20);
    for q in &reqs[..20] {
        check(&at20, q);
    }
    assert!(at20.request_index(&reqs[20].id).is_none());
    let now = store.snapshot().unwrap();
    assert_eq!(now.session_requests(s).len(), 40);
    drop((at20, now));
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
}
