//! End-to-end tests of the store on synthetic sessions (no real logs).

use std::path::PathBuf;
use transcriptdb::split::{blank_dropped, Keep};
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
    db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).unwrap()
}

/// What the store gives back for a request: its body with thinking and images blanked.
fn want(q: &SynthRequest) -> Vec<u8> {
    blank_dropped(&q.body, &Keep::default()).unwrap()
}

fn check_all(db: &Db, reqs: &[SynthRequest]) {
    for q in reqs {
        let ri = db.request_index(&q.id).unwrap_or_else(|| panic!("{} missing", q.id));
        let got = db.rebuild(ri).unwrap();
        let w = want(q);
        if got != w {
            let d = got.iter().zip(&w).position(|(a, b)| a != b).unwrap_or(got.len().min(w.len()));
            panic!("{} (req {ri:x}) rebuilt differently: {} vs {} bytes, first difference at {d}: got {:?} want {:?}", q.id, got.len(), w.len(), String::from_utf8_lossy(&got[d.saturating_sub(40)..(d + 40).min(got.len())]), String::from_utf8_lossy(&w[d.saturating_sub(40)..(d + 40).min(w.len())]));
        }
    }
}

#[test]
fn rebuild_through_flush_reopen_and_compact() {
    let dir = tmpdir("cycle");
    let reqs = traffic(11, 12, 40);
    {
        let mut db = Db::open(&dir, opts(16 << 10)).unwrap();
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
    let mut db = Db::open(&dir, opts(16 << 10)).unwrap();
    assert!(db.recovery.replayed > 0 || db.recovery.segments > 0);
    check_all(&db, &reqs);
    let before: Vec<u32> = db.all_requests().unwrap().into_iter().map(|i| db.request_runs(i).unwrap()).collect();
    db.compact().unwrap();
    assert_eq!(db.stats().segments, 1);
    check_all(&db, &reqs);
    let after: Vec<u32> = db.all_requests().unwrap().into_iter().map(|i| db.request_runs(i).unwrap()).collect();
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
    let db = Db::open(&dir, opts(16 << 10)).unwrap();
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
    let total_msgs: u64 = db.all_requests().unwrap().into_iter().map(|i| db.request(i).unwrap().n_msgs as u64).sum();
    let st = db.stats();
    eprintln!("{} requests carry {total_msgs} messages; {} stored", db.request_count(), st.nodes);
    assert!((st.nodes as u64) * 5 < total_msgs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn steps_match_the_reference_and_survive_reopen_and_compact() {
    let dir = tmpdir("steps");
    let reqs = traffic(17, 10, 40);
    // dictionary codes are per process; compare steps decoded
    type Decoded = (u32, u32, u32, Vec<u8>, i64, u32, u16, u16, bool, Vec<(Vec<u8>, Vec<u8>)>);
    let snapshot = |db: &Db| -> Vec<Vec<Decoded>> {
        let sd = db.step_dicts();
        (0..db.session_count() as u32)
            .map(|s| {
                let ss = db.steps(s);
                ss.rows
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let f = ss
                            .fields_of(i as u32)
                            .iter()
                            .map(|&(f, v)| (sd.fields.get(f).to_vec(), if transcriptdb::steps::is_long(v) { format!("LONG {:?}", ss.long_at(v)).into_bytes() } else { ss.value(f, v).to_vec() }))
                            .collect();
                        (r.call_node, r.result_node, r.prev, sd.tools.get(r.tool).to_vec(), r.time_ms, r.out_bytes, r.call_block, r.result_block, r.failed, f)
                    })
                    .collect()
            })
            .collect()
    };
    let before;
    {
        let mut db = Db::open(&dir, opts(200 << 10)).unwrap();
        for q in &reqs {
            put(&mut db, q);
        }
        db.commit().unwrap();
        let c = transcriptdb::check::check_steps(&db, 4).unwrap();
        eprintln!("{c:?}");
        assert!(c.engine_steps > 100);
        assert!(c.ok(), "{c:?}");
        before = snapshot(&db);
    }
    let mut db = Db::open(&dir, opts(200 << 10)).unwrap();
    assert_eq!(snapshot(&db), before, "steps differ after reopen");
    assert!(transcriptdb::check::check_steps(&db, 4).unwrap().ok());
    db.compact().unwrap();
    drop(db);
    let db = Db::open(&dir, opts(200 << 10)).unwrap();
    assert_eq!(snapshot(&db), before, "steps differ after compact");
    assert!(transcriptdb::check::check_steps(&db, 4).unwrap().ok());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn thinking_and_images_are_never_kept() {
    let reqs = traffic(23, 6, 30);
    let dir = tmpdir("blanked");
    let mut db = Db::open(&dir, opts(128 << 10)).unwrap();
    for q in &reqs {
        put(&mut db, q);
    }
    db.commit().unwrap();
    let check = |db: &Db| {
        let mut held = 0;
        for q in &reqs {
            let got = db.rebuild(db.request_index(&q.id).unwrap()).unwrap();
            assert_eq!(got, want(q), "{}", q.id);
            held += (got.len() < q.body.len()) as usize;
        }
        assert!(held > reqs.len() / 2, "most synthetic requests hold thinking or images ({held})");
        assert_eq!((db.stats().opaques, db.stats().opaque_bytes), (0, 0), "synthetic traffic holds no documents");
        // steps come from the blanked messages, so they match the reference over rebuilds
        let c = transcriptdb::check::check_steps(db, 2).unwrap();
        assert!(c.ok(), "{c:?}");
    };
    check(&db);
    drop(db);
    let mut db = Db::open(&dir, opts(128 << 10)).unwrap();
    check(&db);
    db.compact().unwrap();
    check(&db);
    drop(db);
    std::fs::remove_dir_all(&dir).unwrap();

    // documents and other base64 are kept unless `--drop` names them; cache markers after
    // blanked values land where they were
    let pdf = "JVBERi0x".repeat(40);
    let img = "iVBORw0K".repeat(40);
    let sig = "QUJD".repeat(40);
    let m0 = format!(r#"{{"content":[{{"source":{{"data":"{pdf}","media_type":"application/pdf","type":"base64"}},"type":"document"}},{{"source":{{"data":"{img}","media_type":"image/png","type":"base64"}},"type":"image"}},{{"cache_control":{{"type":"ephemeral"}},"text":"read these","type":"text"}}],"role":"user"}}"#);
    let m0_plain = m0.replace(r#""cache_control":{"type":"ephemeral"},"#, "");
    let m1 = format!(r#"{{"content":[{{"signature":"{sig}","thinking":"plan","type":"thinking"}},{{"text":"done","type":"text","cache_control":{{"type":"ephemeral"}}}}],"role":"assistant"}}"#);
    let bodies = [format!(r#"{{"messages":[{m0}],"model":"m"}}"#), format!(r#"{{"messages":[{m0},{m1}],"model":"m"}}"#), format!(r#"{{"messages":[{m0_plain},{m1}],"model":"m"}}"#)];
    for (keep, kept) in [(Keep::default(), 1), (Keep::dropping("documents").unwrap(), 0)] {
        // a directory of its own each time: open file handles are shared in the process by path
        let dir = tmpdir(&format!("documents-{kept}"));
        let mut db = Db::open(&dir, Options { keep, ..opts(1 << 30) }).unwrap();
        for (i, b) in bodies.iter().enumerate() {
            db.put(&Put { session: "s", agent: "", id: &format!("r{i}"), time: "2026-11-23T10:00:00.000Z", meta: b"", body: b.as_bytes() }).unwrap();
        }
        let check = |db: &Db| {
            for (i, b) in bodies.iter().enumerate() {
                let got = db.rebuild(db.request_index(&format!("r{i}")).unwrap()).unwrap();
                assert_eq!(String::from_utf8(got).unwrap(), String::from_utf8(blank_dropped(b.as_bytes(), &keep).unwrap()).unwrap());
            }
            assert_eq!(db.stats().opaques, kept);
        };
        check(&db);
        db.commit().unwrap();
        db.compact().unwrap();
        check(&db);
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

/// Sessions that pause and wake up again after their data settled in merged segments,
/// with merges, sessions leaving memory and tiny caches all the time.
#[test]
fn merges_wakeups_and_small_caches_keep_everything_exact() {
    let dir = tmpdir("merges");
    // folding off, so woken sessions stay in several parts and reads join them
    let o = Options { flush_bytes: 48 << 10, sync: SyncMode::Os, merge_at: 3, seg_bytes: 160 << 10, idle_flushes: 1, cache_bytes: 192 << 10, max_parts: 0, ..Options::default() };
    let early: Vec<Vec<SynthRequest>> = (0..10).map(|s| synth::session(41, s, 36)).collect();
    let mut order: Vec<&SynthRequest> = Vec::new();
    for s in &early {
        order.extend(s[..18].iter());
    }
    // request ids from another seed repeat ("synth-<n>-<k>"): keep them apart
    let later: Vec<SynthRequest> = traffic(43, 12, 30).into_iter().map(|q| SynthRequest { id: format!("late-{}", q.id), ..q }).collect();
    order.extend(later.iter());
    for s in &early {
        order.extend(s[18..].iter());
    }
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for (i, q) in order.iter().enumerate() {
        put(&mut db, q);
        if i % 5 == 0 {
            db.commit().unwrap();
        }
    }
    db.commit().unwrap();
    let all: Vec<SynthRequest> = order.iter().map(|q| SynthRequest { session: q.session.clone(), agent: q.agent.clone(), id: q.id.clone(), time: q.time.clone(), body: q.body.clone() }).collect();
    let check = |db: &Db| {
        check_all(db, &all);
        let c = transcriptdb::check::check_steps(db, 2).unwrap();
        assert!(c.ok(), "{c:?}");
        for (name, c) in db.stats().caches {
            assert!(c.bytes <= c.budget, "{name} cache over budget");
        }
    };
    let st = db.stats();
    assert!(st.merges.merges >= 3, "{:?}", st.merges);
    assert!(db.segments().iter().filter(|s| s.1 == 1).count() >= 2, "{:?}", db.segments());
    // woken sessions find the messages they already have (node keys are not stored, but
    // worked out from the stored bytes): as many nodes as a store that never flushed
    let one_dir = tmpdir("merges-in-memory");
    let mut one = Db::open(&one_dir, opts(1 << 30)).unwrap();
    for q in &order {
        put(&mut one, q);
    }
    assert_eq!(st.nodes, one.stats().nodes, "woken sessions stored messages they already had");
    drop(one);
    std::fs::remove_dir_all(&one_dir).unwrap();
    // the paused sessions woke up after their first half was merged: they span parts
    let spread = (0..10).filter(|&s| db.part_count(s).unwrap() >= 2).count();
    assert!(spread >= 5, "only {spread} woken sessions span several parts");
    check(&db);
    drop(db);
    let mut db = Db::open(&dir, o.clone()).unwrap();
    check(&db);
    db.compact().unwrap();
    assert!(db.segments().iter().all(|s| s.1 == 1));
    assert!((0..db.session_count() as u32).all(|s| db.part_count(s).unwrap() == 1), "compaction leaves one part per session");
    check(&db);
    db.clear_caches();
    check(&db);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_store_opens_in_one_place_at_a_time() {
    let dir = tmpdir("lock");
    let db = Db::open(&dir, opts(1 << 20)).unwrap();
    assert!(Db::open(&dir, opts(1 << 20)).is_err(), "second open while the first holds the store");
    drop(db);
    Db::open(&dir, opts(1 << 20)).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn many_files_with_few_open_at_once() {
    let dir = tmpdir("files");
    let o = Options { flush_bytes: 24 << 10, sync: SyncMode::Os, merge_at: 50, open_files: 4, cache_bytes: 64 << 10, blob_bytes: 1 << 10, ..Options::default() };
    let reqs = traffic(51, 10, 30);
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for q in &reqs {
        put(&mut db, q);
        db.commit().unwrap();
    }
    db.flush().unwrap();
    let st = db.stats();
    // blob files hold only pieces now (synthetic traffic has no documents), so there are few
    assert!(st.segments > 20 && st.runs > 20 && st.blobs >= 3, "{} segments, {} index runs, {} blob files", st.segments, st.runs, st.blobs);
    check_all(&db, &reqs);
    drop(db);
    let db = Db::open(&dir, o).unwrap();
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn blob_files_fill_up_and_an_unfinished_append_is_cut() {
    use std::io::Write;
    let dir = tmpdir("blobfill");
    let o = Options { flush_bytes: 32 << 10, sync: SyncMode::Os, blob_bytes: 4 << 10, ..Options::default() };
    let reqs = traffic(61, 8, 30);
    let (first, rest) = reqs.split_at(reqs.len() / 2);
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for q in first {
        put(&mut db, q);
        db.commit().unwrap();
    }
    db.flush().unwrap();
    let st = db.stats();
    // each file grows past the cap once, then a new one starts: files follow bytes, not flushes
    assert!(st.blobs >= 2 && (st.blobs as u64) <= st.blob_file_bytes / (4 << 10) + 1, "{} blob files for {} bytes", st.blobs, st.blob_file_bytes);
    assert!(st.runs < 12, "{} index runs after {} flushes", st.runs, st.merges.merges);
    drop(db);
    // a flush that appended to the last blob file and crashed before the manifest
    let mut blobs: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|e| e == "dat")).collect();
    blobs.sort();
    std::fs::OpenOptions::new().append(true).open(blobs.last().unwrap()).unwrap().write_all(&[7u8; 1000]).unwrap();
    let mut db = Db::open(&dir, o.clone()).unwrap();
    assert_eq!(db.recovery.blob_tail_bytes, 1000);
    check_all(&db, first);
    for q in rest {
        put(&mut db, q);
        db.commit().unwrap();
    }
    // every request again: each is found by id through the index and stored once
    for q in &reqs {
        assert!(matches!(put(&mut db, q), PutOutcome::Duplicate(_)), "{} stored twice", q.id);
    }
    db.flush().unwrap();
    check_all(&db, &reqs);
    drop(db);
    let db = Db::open(&dir, o).unwrap();
    assert_eq!(db.recovery.blob_tail_bytes, 0);
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn index_runs_stay_few_through_many_small_flushes() {
    let dir = tmpdir("runs");
    let o = Options { flush_bytes: 24 << 10, sync: SyncMode::Os, merge_at: 3, seg_bytes: 64 << 10, run_bytes: 16 << 10, blob_bytes: 32 << 10, idle_flushes: 1, ..Options::default() };
    let reqs = traffic(71, 12, 30);
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for (i, q) in reqs.iter().enumerate() {
        put(&mut db, q);
        db.commit().unwrap();
        if i % 7 == 0 {
            db.flush().unwrap();
        }
        if i % 40 == 39 {
            db.compact().unwrap();
        }
    }
    db.flush().unwrap();
    let st = db.stats();
    // runs under the cap merge three at a time; the rest are full
    let full = st.index_file_bytes / (16 << 10);
    assert!((st.runs as u64) <= full + 2 * 3, "{} index runs for {} bytes of index", st.runs, st.index_file_bytes);
    check_all(&db, &reqs);
    drop(db);
    let db = Db::open(&dir, o).unwrap();
    check_all(&db, &reqs);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Sessions that take writes again and again while other traffic pushes their data into
/// full-size segments: merges into full-size segments fold them back into one part, and
/// segments mostly made of replaced parts are rewritten without them.
#[test]
fn woken_sessions_fold_back_into_one_part() {
    let dir = tmpdir("fold");
    let o = Options { flush_bytes: 24 << 10, sync: SyncMode::Os, merge_at: 3, seg_bytes: 96 << 10, idle_flushes: 1, cache_bytes: 256 << 10, max_parts: 3, ..Options::default() };
    let wakers: Vec<Vec<SynthRequest>> = (0..10).map(|s| synth::session(61, s, 40)).collect();
    // filler sessions from another seed: their ids repeat ("synth-<n>-<k>"), so keep them apart
    let filler: Vec<SynthRequest> = traffic(67, 60, 24).into_iter().map(|q| SynthRequest { id: format!("fill-{}", q.id), ..q }).collect();
    let mut order: Vec<&SynthRequest> = Vec::new();
    let mut fill = filler.chunks(filler.len() / 6);
    for round in 0..5 {
        for w in &wakers {
            order.extend(w[round * 8..round * 8 + 8].iter());
        }
        order.extend(fill.next().unwrap());
    }
    order.extend(fill.flatten());
    let all: Vec<&SynthRequest> = order.clone();
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for (i, q) in order.iter().enumerate() {
        put(&mut db, q);
        if i % 4 == 0 {
            db.commit().unwrap();
        }
    }
    db.flush().unwrap();
    let check = |db: &Db| {
        for q in &all {
            let ri = db.request_index(&q.id).unwrap_or_else(|| panic!("{} missing", q.id));
            assert_eq!(db.rebuild(ri).unwrap(), want(q), "{}", q.id);
        }
        assert_eq!(db.request_count(), all.len(), "requests in replaced parts are not counted");
        let c = transcriptdb::check::check_steps(db, 2).unwrap();
        assert!(c.ok(), "{c:?}");
    };
    let st = db.stats();
    let parts: Vec<usize> = (0..10).map(|s| db.part_count(s).unwrap()).collect();
    eprintln!("merges {:?}; waker parts {parts:?}; dead {} of {} segment bytes; segments {:?}", st.merges, st.dead_bytes, st.seg_file_bytes, db.segments());
    assert!(st.merges.folded >= 10, "{:?}", st.merges);
    assert!(st.merges.reclaims >= 1, "{:?}", st.merges);
    assert!(parts.iter().all(|&n| n <= 3), "a session left in more parts than max_parts: {parts:?}");
    assert!(parts.iter().filter(|&&n| n == 1).count() >= 8, "quiet sessions not folded back: {parts:?}");
    assert!(st.dead_bytes * 2 < st.seg_file_bytes);
    check(&db);
    drop(db);
    let mut db = Db::open(&dir, o.clone()).unwrap();
    let again = db.stats();
    assert_eq!((again.dead_bytes, again.nodes, again.steps, again.node_bytes), (st.dead_bytes, st.nodes, st.steps, st.node_bytes));
    check(&db);
    db.clear_caches();
    check(&db);
    db.compact().unwrap();
    let done = db.stats();
    assert_eq!(done.dead_bytes, 0);
    assert_eq!((done.nodes, done.steps, done.node_bytes), (st.nodes, st.steps, st.node_bytes), "totals with replaced parts left out match the totals after they are dropped");
    assert!((0..db.session_count() as u32).all(|s| db.part_count(s).unwrap() == 1));
    check(&db);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn merged_frames_use_a_trained_dictionary_and_pieces_chain() {
    // enough distinct message text for a dictionary (about 1% of it, at least 16 KB)
    let reqs = traffic(21, 90, 60);
    // 8 KB frames: step blocks (about 64 KB) are then bigger than the room for them after
    // the dictionary, and are decoded the other way
    // then with every session that ended before the newest request cold: messages in runs of
    // 8 frames, bigger than the room after the dictionary, which then grows
    for (frame_kb, cold) in [(64, false), (8, false), (8, true)] {
        merged_with_dictionary(&reqs, frame_kb, cold);
    }
}

fn merged_with_dictionary(reqs: &[SynthRequest], frame_kb: usize, cold: bool) {
    let dir = tmpdir(&format!("dict-{frame_kb}-{cold}"));
    // merges only in `compact`, at a low level (the level does not change the format)
    let (cold_after_ms, cold_run) = if cold { (1, 8 * (frame_kb << 10)) } else { (0, 0) };
    let o = Options { seg_bytes: 384 << 10, merge_at: 1 << 20, merge_level: 3, frame_size: frame_kb << 10, cold_after_ms, cold_run, ..opts(2 << 20) };
    let mut db = Db::open(&dir, o.clone()).unwrap();
    for (i, q) in reqs.iter().enumerate() {
        put(&mut db, q);
        if i % 50 == 0 {
            db.commit().unwrap();
        }
    }
    db.commit().unwrap();
    db.flush().unwrap();
    let flushed = db.stats();
    assert_eq!(flushed.dict_bytes, 0, "flushes write frames without a dictionary");
    assert!(flushed.node_bytes >= 2 << 20, "test data too small for a dictionary: {} bytes", flushed.node_bytes);
    db.compact().unwrap();
    let st = db.stats();
    assert!(st.segments >= 3, "small segments should give several merged segments");
    assert!(st.dict_bytes > 0, "merged segments should carry a dictionary");
    assert!(st.frame_bytes < flushed.frame_bytes, "dictionary frames {} should be smaller than flushed ones {}", st.frame_bytes, flushed.frame_bytes);
    assert!(st.piece_bytes * 3 < st.piece_raw_bytes, "pieces compressed against earlier ones");
    let (n_cold, _) = db.cold_sessions().unwrap();
    assert_eq!(n_cold > 0, cold, "{n_cold} sessions in cold runs");
    if cold {
        // sessions that end at the newest request's second are not cold
        assert!(n_cold * 4 >= db.session_count(), "only {n_cold} of {} sessions are cold", db.session_count());
    }
    check_all(&db, reqs);
    drop(db);
    // reopened, and read from several threads at once, each going back and forth between
    // more segments than it keeps dictionary buffers for; step blocks use the dictionary too
    let db = Db::open(&dir, o).unwrap();
    std::thread::scope(|sc| {
        for part in reqs.chunks(reqs.len().div_ceil(4)) {
            let db = &db;
            sc.spawn(move || check_all(db, part));
        }
    });
    let c = transcriptdb::check::check_steps(&db, 4).unwrap();
    assert!(c.ok() && c.engine_steps > 1000, "{c:?}");
    drop(db);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Merges run on their own thread while writes go on, are put in place at a later flush,
/// and leave the store as merges done on the spot do. A store closed while one is at work
/// reopens whole, with no file of the unfinished merge left behind.
#[test]
fn merges_run_while_writes_go_on() {
    let reqs = traffic(77, 14, 40);
    let small = |background: bool| Options { flush_bytes: 32 << 10, sync: SyncMode::Os, merge_at: 2, seg_bytes: 128 << 10, idle_flushes: 1, cache_bytes: 512 << 10, max_parts: 3, background_merges: background, ..Options::default() };
    let seg_files = |dir: &std::path::Path| std::fs::read_dir(dir).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("seg-")).count();

    // merges on their own thread
    let da = tmpdir("bgmerge");
    let mut a = Db::open(&da, small(true)).unwrap();
    let mut seen_merging = 0;
    for (i, q) in reqs.iter().enumerate() {
        put(&mut a, q);
        if i % 4 == 0 {
            a.commit().unwrap();
            seen_merging += a.merging() as usize;
            if i % 40 == 0 {
                // reads go on against the files in place while a merge writes new ones
                check_all(&a, &reqs[..=i]);
            }
        }
    }
    a.commit().unwrap();
    assert!(seen_merging > 0, "no merge was ever at work between two commits");
    check_all(&a, &reqs);
    a.settle().unwrap();
    assert!(!a.merging());
    assert!(a.merge_stats.merges > 0, "no merge was put in place");
    assert_eq!(seg_files(&da), a.segment_count(), "a segment file is not in the manifest");
    check_all(&a, &reqs);

    // the same writes with merges done on the spot: the same steps in every session
    let dbd = tmpdir("fgmerge");
    let mut b = Db::open(&dbd, small(false)).unwrap();
    for (i, q) in reqs.iter().enumerate() {
        put(&mut b, q);
        if i % 4 == 0 {
            b.commit().unwrap();
            assert!(!b.merging());
        }
    }
    b.settle().unwrap();
    check_all(&b, &reqs);
    assert_eq!(a.session_count(), b.session_count());
    let by_key = |db: &Db| -> std::collections::BTreeMap<(String, String), Vec<transcriptdb::steps::StepRow>> { (0..db.session_count() as u32).map(|s| (db.session_key(s), db.steps(s).rows.clone())).collect() };
    assert_eq!(by_key(&a), by_key(&b));
    drop(b);

    // closed while a merge is at work: nothing of it stays, and nothing is lost
    drop(a);
    let dc = tmpdir("dropmerge");
    let mut c = Db::open(&dc, small(true)).unwrap();
    let mut closed_merging = false;
    for (i, q) in reqs.iter().enumerate() {
        put(&mut c, q);
        if i % 4 == 0 {
            c.commit().unwrap();
            if c.merging() && i > reqs.len() / 2 {
                closed_merging = true;
                drop(c);
                c = Db::open(&dc, small(true)).unwrap();
                assert_eq!(seg_files(&dc), c.segment_count(), "a file of the unfinished merge was left behind");
                check_all(&c, &reqs[..=i]);
            }
        }
    }
    assert!(closed_merging);
    c.settle().unwrap();
    check_all(&c, &reqs);
    drop(c);
    for d in [da, dbd, dc] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Steps read without their values (the fast way, for sessions in one part) are what reading
/// every value and then dropping them gives: rows, field codes, value counts, shapes.
#[test]
fn light_steps_equal_full_steps_without_values() {
    let dir = tmpdir("light");
    let reqs = traffic(91, 12, 40);
    let mut db = Db::open(&dir, Options { background_merges: false, ..opts(48 << 10) }).unwrap();
    for (i, q) in reqs.iter().enumerate() {
        put(&mut db, q);
        if i % 50 == 0 {
            db.commit().unwrap();
            db.flush().unwrap();
        }
    }
    db.commit().unwrap();
    let same = |db: &Db, stage: &str| {
        let mut n = 0;
        for s in 0..db.session_count() as u32 {
            db.clear_caches();
            let light = db.light_steps(s).unwrap();
            db.clear_caches();
            let full = db.steps(s).without_values();
            // values per field, as queries ask for them (a session taking writes is not light)
            let nf = db.step_dicts().fields.len() as u32;
            let lc: Vec<usize> = (0..nf).map(|f| light.value_count(f)).collect();
            let fc: Vec<usize> = (0..nf).map(|f| full.value_count(f)).collect();
            assert_eq!((&light.rows, &light.fstart, &light.fields, &light.longs, &light.shape, lc), (&full.rows, &full.fstart, &full.fields, &full.longs, &full.shape, fc), "{stage}: session {s}");
            n += light.rows.len();
        }
        assert!(n > 0);
    };
    same(&db, "many parts");
    db.compact().unwrap();
    same(&db, "one part each");
    // a rule the merged segment was written without, then with
    db.define_rule(r#"program on tool == "Bash" = extract(command, @"^\s*(\S+)")"#).unwrap();
    same(&db, "a rule to work out");
    db.compact().unwrap();
    same(&db, "a rule stored");
    db.define_rule(r#"program on tool == "Bash" = extract(command, @"(\S+)\s*$")"#).unwrap();
    same(&db, "a rule's stored values out of date");
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
