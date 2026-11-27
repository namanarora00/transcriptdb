//! Deleting sessions: gone from reads, lookups and queries at once and for good, the key
//! free for a new session, removed from every file by a vacuum, and still readable from a
//! snapshot taken before.

use std::collections::BTreeSet;
use transcriptdb::query::{self, exec};
use transcriptdb::synth::{self, SynthRequest};
use transcriptdb::{Db, Options, Put, PutOutcome, SyncMode};

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-drop-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn opts() -> Options {
    Options { sync: SyncMode::Os, flush_bytes: 64 << 10, merge_at: 2, seg_bytes: 256 << 10, ..Options::default() }
}

/// Session `s`: synthetic requests, each with a response and a system prompt only it has.
fn session(s: u64, n: usize, t0: i64) -> Vec<(SynthRequest, Vec<u8>)> {
    synth::session(21, s, n)
        .into_iter()
        .enumerate()
        .map(|(k, mut q)| {
            // a system prompt of its own, so a vacuum has pieces to remove
            let body = String::from_utf8(q.body).unwrap();
            q.body = body.replacen("{", &format!(r#"{{"system":"secret prompt of session {s}","#), 1).into_bytes();
            q.time = format!("{}", (t0 + k as i64 * 60_000) / 1000);
            let resp = format!(r#"{{"id":"msg_{s}_{k}","content":[{{"type":"text","text":"reply {k} of {s}"}}]}}"#).into_bytes();
            (q, resp)
        })
        .collect()
}

fn put(db: &mut Db, q: &SynthRequest, resp: &[u8]) -> PutOutcome {
    db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body, response: resp }).unwrap()
}

fn want(q: &SynthRequest) -> Vec<u8> {
    transcriptdb::split::blank_dropped(&q.body, &Default::default()).unwrap()
}

fn live_ok(db: &Db, reqs: &[(SynthRequest, Vec<u8>)]) {
    for (q, r) in reqs {
        let ri = db.request_index(&q.id).unwrap_or_else(|| panic!("{} missing", q.id));
        assert_eq!(db.rebuild(ri).unwrap(), want(q));
        assert_eq!(db.response(ri).unwrap().as_deref(), Some(&r[..]));
    }
}

fn gone(db: &Db, reqs: &[(SynthRequest, Vec<u8>)]) {
    for (q, _) in reqs {
        assert!(db.request_index(&q.id).is_none(), "{} still found", q.id);
    }
    let (q, _) = &reqs[0];
    assert!(db.session_index(&q.session, &q.agent).unwrap().is_none());
}

fn count(db: &Db, q: &str) -> u64 {
    let r = query::run(db, q, 2).unwrap_or_else(|e| panic!("{}", e.show(q)));
    let cells = exec::render(db, &r.table, 10).unwrap().1;
    cells.first().map_or(0, |row| row[0].parse().unwrap())
}

#[test]
fn dropped_sessions_are_gone_everywhere() {
    let dir = tmpdir("gone");
    let day = 86_400_000i64;
    let t0 = 1_760_000_000_000i64;
    let sessions: Vec<Vec<(SynthRequest, Vec<u8>)>> = (0..8).map(|s| session(s, 30, t0 + s as i64 * day)).collect();
    let mut db = Db::open(&dir, opts()).unwrap();
    for (i, ss) in sessions.iter().enumerate() {
        for (q, r) in ss {
            put(&mut db, q, r);
        }
        if i % 3 == 0 {
            db.flush().unwrap();
        }
    }
    db.commit().unwrap();
    let steps_all = count(&db, "summarize n = count()");
    let per: Vec<u64> = (0..8u32).map(|s| db.steps(s).rows.len() as u64).collect();
    let text_all = count(&db, r#"messages | summarize n = count()"#);
    let text_per: Vec<u64> = (0..8u32).map(|s| db.session_texts(s).unwrap().units.len() as u64).collect();

    // 1 and 6 are dropped while in files, 7 while its last requests are still in memory
    let k = &sessions[1][0].0;
    let rep = db.drop_session_key(&k.session, &k.agent).unwrap();
    assert_eq!((rep.sessions, rep.requests), (1, 30));
    db.drop_sessions(&[6, 7]).unwrap();
    for s in [1usize, 6, 7] {
        gone(&db, &sessions[s]);
    }
    let live = [0usize, 2, 3, 4, 5];
    for s in live {
        live_ok(&db, &sessions[s]);
    }
    // queries see none of their steps or text, from summaries or from memory
    assert_eq!(count(&db, "summarize n = count()"), steps_all - per[1] - per[6] - per[7]);
    assert_eq!(count(&db, r#"messages | summarize n = count()"#), text_all - text_per[1] - text_per[6] - text_per[7]);
    assert_eq!(count(&db, r#"where session == "s-1" | summarize n = count()"#), 0);

    // the key starts over: the same requests again make a new session
    for (q, r) in &sessions[1] {
        assert!(matches!(put(&mut db, q, r), PutOutcome::Stored(_)));
    }
    db.commit().unwrap();
    live_ok(&db, &sessions[1]);
    let again = db.session_index(&k.session, &k.agent).unwrap().unwrap();
    assert!(again >= 8, "a new session number");
    db.drop_sessions(&[again]).unwrap();

    // and it lasts: flushed, merged and reopened, and again from the log after a crash
    db.settle().unwrap();
    for s in [1usize, 6, 7] {
        gone(&db, &sessions[s]);
    }
    drop(db);
    let mut db = Db::open(&dir, opts()).unwrap();
    for s in [1usize, 6, 7] {
        gone(&db, &sessions[s]);
    }
    for s in live {
        live_ok(&db, &sessions[s]);
    }
    let extra = session(50, 20, t0);
    for (q, r) in &extra {
        put(&mut db, q, r);
    }
    db.commit().unwrap();
    let s50 = db.session_index(&extra[0].0.session, "").unwrap().unwrap();
    db.drop_sessions(&[s50]).unwrap();
    // no flush: the dropped session's requests are only in the log, and are replayed
    drop(db);
    let db = Db::open(&dir, opts()).unwrap();
    gone(&db, &extra);
    for s in live {
        live_ok(&db, &sessions[s]);
    }
    assert_eq!(count(&db, "summarize n = count()"), live.iter().map(|&s| per[s]).sum::<u64>());
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_before_takes_whole_sessions_by_their_newest_request() {
    let dir = tmpdir("before");
    let day = 86_400_000i64;
    let t0 = 1_760_000_000_000i64;
    let mut db = Db::open(&dir, opts()).unwrap();
    // session s starts on day s; 30 requests a minute apart
    let sessions: Vec<Vec<(SynthRequest, Vec<u8>)>> = (0..6).map(|s| session(s, 30, t0 + s as i64 * day)).collect();
    for ss in &sessions {
        for (q, r) in ss {
            put(&mut db, q, r);
        }
    }
    db.commit().unwrap();
    db.settle().unwrap();
    // cutoff 10 minutes into day 3: sessions 0-2 are wholly older; 3 straddles it
    let rep = db.drop_before(t0 + 3 * day + 10 * 60_000).unwrap();
    assert_eq!(rep.sessions, 3);
    for ss in &sessions[..3] {
        gone(&db, ss);
    }
    for ss in &sessions[3..] {
        live_ok(&db, ss);
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vacuum_leaves_no_trace_and_old_snapshots_still_read() {
    let dir = tmpdir("vacuum");
    let t0 = 1_760_000_000_000i64;
    let mut db = Db::open(&dir, opts()).unwrap();
    let sessions: Vec<Vec<(SynthRequest, Vec<u8>)>> = (0..6).map(|s| session(s, 40, t0)).collect();
    for ss in &sessions {
        for (q, r) in ss {
            put(&mut db, q, r);
        }
    }
    db.commit().unwrap();
    db.settle().unwrap();
    let before = db.snapshot().unwrap();
    db.drop_sessions(&[0, 2, 4]).unwrap();
    // until a vacuum their bytes are still in segments, and their system prompts in blobs
    assert!(db.sessions_in_files().unwrap().contains(&0));
    let secret = |db: &Db, s: u32| {
        let mut n = 0;
        db.each_piece(|_, _, raw, _, _| n += String::from_utf8_lossy(raw).contains(&format!("secret prompt of session {s}\"")) as usize).unwrap();
        n
    };
    assert_eq!(secret(&db, 0), 1);

    let rep = db.vacuum().unwrap();
    assert!(rep.pieces_removed >= 3, "{rep:?}");
    assert!(rep.segment_bytes_after < rep.segment_bytes_before, "{rep:?}");
    let in_files = db.sessions_in_files().unwrap();
    assert_eq!(in_files, BTreeSet::from([1, 3, 5]), "only live sessions are in segments");
    for s in [0, 2, 4] {
        assert_eq!(secret(&db, s), 0, "session {s}'s system prompt is gone from the blobs");
        gone(&db, &sessions[s as usize]);
    }
    for s in [1, 3, 5] {
        assert_eq!(secret(&db, s), 1);
        live_ok(&db, &sessions[s as usize]);
    }
    // the snapshot from before reads everything it held, from the files it holds
    for ss in &sessions {
        live_ok(&before, ss);
    }
    // new writes after a vacuum: new pieces, old ones found again by hash
    let more = session(9, 20, t0);
    for (q, r) in &more {
        put(&mut db, q, r);
    }
    for (q, r) in &sessions[1] {
        assert!(matches!(put(&mut db, q, r), PutOutcome::Duplicate(_)));
    }
    db.commit().unwrap();
    db.settle().unwrap();
    live_ok(&db, &more);
    drop(db);
    // the old files go once the snapshot does
    let files = || std::fs::read_dir(&dir).unwrap().count();
    let with_snapshot = files();
    drop(before);
    transcriptdb::block::wait_removed();
    assert!(files() < with_snapshot);
    let db = Db::open(&dir, opts()).unwrap();
    for s in [1usize, 3, 5] {
        live_ok(&db, &sessions[s]);
    }
    live_ok(&db, &more);
    for s in [0usize, 2, 4] {
        gone(&db, &sessions[s]);
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
