//! Query language tests on small hand-built sessions (each rule pinned down) and on
//! synthetic traffic checked against the slow reference.

use transcriptdb::query::{self, exec::Kind};
use transcriptdb::synth::{self, Rng};
use transcriptdb::{Db, Options, Put, SyncMode};

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-qtest-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn opts() -> Options {
    Options { sync: SyncMode::Os, flush_bytes: 1 << 30, ..Options::default() }
}

/// One turn: parallel calls (tool, failed).
type Turn = Vec<(&'static str, bool)>;

/// Requests for a session whose context grows turn by turn; `tag` keeps tool ids apart.
fn session_bodies(turns: &[Turn], prefix: &[String], tag: &str) -> Vec<String> {
    let mut msgs: Vec<String> = if prefix.is_empty() { vec![r#"{"role":"user","content":"start"}"#.to_string()] } else { prefix.to_vec() };
    let mut out = vec![body(&msgs)];
    for (t, calls) in turns.iter().enumerate() {
        let uses: Vec<String> = calls.iter().enumerate().map(|(i, (tool, _))| format!(r#"{{"type":"tool_use","id":"{tag}{t}_{i}","name":"{tool}","input":{{"file_path":"/repo/{tool}.rs"}}}}"#)).collect();
        let res: Vec<String> = calls.iter().enumerate().map(|(i, (_, f))| format!(r#"{{"type":"tool_result","tool_use_id":"{tag}{t}_{i}","content":"out {t}","is_error":{f}}}"#)).collect();
        msgs.push(format!(r#"{{"role":"assistant","content":[{}]}}"#, uses.join(",")));
        msgs.push(format!(r#"{{"role":"user","content":[{}]}}"#, res.join(",")));
        out.push(body(&msgs));
    }
    out
}

fn body(msgs: &[String]) -> String {
    format!(r#"{{"model":"m","max_tokens":10,"messages":[{}]}}"#, msgs.join(","))
}

fn store(name: &str, sessions: &[Vec<String>]) -> (Db, std::path::PathBuf) {
    let dir = tmpdir(name);
    let mut db = Db::open(&dir, opts()).unwrap();
    let mut k = 0;
    for (s, bodies) in sessions.iter().enumerate() {
        for b in bodies {
            let id = format!("r{k}");
            let time = format!("{}", 1_700_000_000 + k);
            db.put(&Put { session: &format!("s{s}"), agent: "", id: &id, time: &time, meta: b"", body: b.as_bytes() }).unwrap();
            k += 1;
        }
    }
    db.commit().unwrap();
    (db, dir)
}

/// (session, binds) of every match.
fn matches(db: &Db, q: &str) -> Vec<(u32, Vec<u32>)> {
    let r = query::run(db, q, 2).unwrap_or_else(|e| panic!("{}", e.show(q)));
    let Kind::Matches(vars) = &r.table.kind else { panic!("not a match table") };
    r.table.rows.iter().map(|row| (row.session, row.steps[..vars.len()].to_vec())).collect()
}

const B: &str = "Bash";

#[test]
fn then_next_exclusion_and_parallel_calls() {
    let s0 = session_bodies(&[vec![(B, true)], vec![("Read", false)], vec![("Edit", false)], vec![(B, false)]], &[], "a");
    let s1 = session_bodies(&[vec![("Read", false), ("Grep", false)], vec![("Edit", false)]], &[], "b");
    let (db, dir) = store("rules", &[s0, s1]);
    // then: any later turn
    assert_eq!(matches(&db, r#"match a: tool == "Bash" and failed then c: tool == "Bash" and not failed"#), vec![(0, vec![0, 3])]);
    // an excluded step between them kills it
    assert!(matches(&db, r#"match a: failed then c: tool == "Bash" where no (tool == "Read") between a and c"#).is_empty());
    // excluded tool not between: fine
    assert_eq!(matches(&db, r#"match a: failed then c: tool == "Bash" where no (tool == "Grep") between a and c"#).len(), 1);
    // next: the very next turn only
    assert!(matches(&db, r#"match a: failed next c: tool == "Edit""#).is_empty());
    assert_eq!(matches(&db, r#"match a: tool == "Read" next c: tool == "Edit""#), vec![(0, vec![1, 2]), (1, vec![0, 2])]);
    // calls in one turn are not ordered
    assert!(matches(&db, r#"match a: tool == "Read" then b: tool == "Grep""#).is_empty());
    assert_eq!(matches(&db, r#"match a: tool == "Grep" next b: tool == "Edit""#), vec![(1, vec![1, 2])]);
    // within counts turns
    assert!(matches(&db, r#"match a: failed then c: tool == "Bash" within 2"#).is_empty());
    assert_eq!(matches(&db, r#"match a: failed then c: tool == "Bash" within 3"#).len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn repeats_and_no_overlap() {
    let s0 = session_bodies(&[vec![("Read", false)], vec![("Read", false)], vec![("Read", false)], vec![("Edit", false)]], &[], "a");
    let s1 = session_bodies(&[vec![(B, true)], vec![(B, true)], vec![(B, false)], vec![(B, false)]], &[], "b");
    let (db, dir) = store("repeat", &[s0, s1]);
    assert_eq!(matches(&db, r#"match r: tool == "Read"{3} next e: tool == "Edit""#), vec![(0, vec![2, 3])]);
    assert!(matches(&db, r#"match r: tool == "Read"{4} next e: tool == "Edit""#).is_empty());
    assert_eq!(matches(&db, r#"match r: tool == "Read"{2,} next e: tool == "Edit""#), vec![(0, vec![2, 3])]);
    // two fails then two passes: one match, binding the latest fail; no overlapping second match
    assert_eq!(matches(&db, r#"match a: failed then c: not failed"#), vec![(1, vec![1, 2])]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn forks_are_each_matched_with_shared_history_once() {
    // main line: Bash fails, Edit, Bash passes; a rewind after the failure: Read, Bash passes
    let main = session_bodies(&[vec![(B, true)], vec![("Edit", false)], vec![(B, false)]], &[], "m");
    let prefix: Vec<String> = {
        // the messages up to and including the first turn's result
        let mut v = vec![r#"{"role":"user","content":"start"}"#.to_string()];
        v.push(r#"{"role":"assistant","content":[{"type":"tool_use","id":"m0_0","name":"Bash","input":{"file_path":"/repo/Bash.rs"}}]}"#.into());
        v.push(r#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"m0_0","content":"out 0","is_error":true}]}"#.into());
        v
    };
    let side = session_bodies(&[vec![("Read", false)], vec![(B, false)]], &prefix, "r");
    let mut all = main;
    all.extend(side);
    let (db, dir) = store("fork", &[all]);
    let ss = db.steps(0);
    assert_eq!(ss.rows.len(), 5, "3 steps on the main line, 2 on the branch");
    let m = matches(&db, r#"match a: failed then c: tool == "Bash" and not failed"#);
    assert_eq!(m.len(), 2, "one match per branch: {m:?}");
    assert!(m.iter().all(|(_, b)| b[0] == 0), "both bind the shared failure");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn stages_after_match() {
    let s0 = session_bodies(&[vec![(B, true)], vec![(B, false)], vec![(B, true)], vec![(B, false)]], &[], "a");
    let (db, dir) = store("stages", &[s0]);
    let r = query::run(&db, r#"match a: failed then c: not failed | extend took = c.time - a.time | summarize n = count(), total = sum(took) by a.tool"#, 2).unwrap();
    let (h, cells) = query::exec::render(&db, &r.table, 10).unwrap();
    assert_eq!(h, vec!["a_tool", "n", "total"]);
    assert_eq!(cells, vec![vec!["Bash".to_string(), "2".into(), "2.0s".into()]]);
    let r = query::run(&db, r#"where tool == "Bash" | summarize n = count(), f = countif(failed) | extend rate = f * 1.0 / n"#, 2).unwrap();
    let (_, cells) = query::exec::render(&db, &r.table, 10).unwrap();
    assert_eq!(cells[0], vec!["4", "2", "0.500"]);
    assert!(query::run(&db, "where tool == 3", 2).is_err());
    assert!(query::run(&db, "match a: tool == \"Bash\" | where tool == \"x\"", 2).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn generated_queries_match_the_reference() {
    let dir = tmpdir("gen");
    let mut db = Db::open(&dir, opts()).unwrap();
    for s in 0..16 {
        for q in synth::session(5, s, 45) {
            db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).unwrap();
        }
    }
    db.commit().unwrap();
    let mut r = Rng::new(11);
    let mut with = 0;
    for _ in 0..120 {
        let q = query::reference::random_query(&db, &mut r);
        let (m, bad) = query::reference::compare(&db, &q, 1000).unwrap();
        assert!(bad.is_empty(), "engine and reference differ on: {q}");
        with += (m > 0) as usize;
    }
    assert!(with > 40, "too few generated queries matched anything ({with})");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn columnar_summarize_equals_the_general_path() {
    let dir = tmpdir("fastsum");
    let mut db = Db::open(&dir, opts()).unwrap();
    for s in 0..12 {
        for q in synth::session(8, s, 40) {
            db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).unwrap();
        }
    }
    db.commit().unwrap();
    let qs = [
        "summarize n = count() by tool | sort by n desc, tool asc",
        "summarize n = count(), f = countif(failed), a = avg(out_bytes), s = sum(out_bytes), lo = min(time), hi = max(time) by tool, failed",
        "where tool == \"Edit\" | summarize n = count(), d = dcount(session) by file_path, bin(time, 1h)",
        "summarize d = dcount(tool), e = dcount(file_path), x = max(out_bytes) by session, agent",
        "where tool == \"nothing\" | summarize n = count()",
        "summarize n = count() by command",
    ];
    for q in qs {
        let (h1, c1) = query::exec::render(&db, &query::run(&db, q, 2).unwrap().table, 1000).unwrap();
        let slow = q.replacen("summarize", "extend zz_ = 1 | summarize", 1);
        let slow = if slow.starts_with("extend") { format!("where true | {slow}") } else { slow };
        let (h2, c2) = query::exec::render(&db, &query::run(&db, &slow, 2).unwrap_or_else(|e| panic!("{}", e.show(&slow))).table, 1000).unwrap();
        assert_eq!(h1, h2, "{q}");
        assert_eq!(c1, c2, "{q}");
        assert!(!c1.is_empty(), "{q}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

fn put_req(db: &mut Db, q: &synth::SynthRequest) {
    db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).unwrap();
}

/// Requests of `sessions` rich synthetic sessions, interleaved.
fn rich_traffic(seed: u64, sessions: u64, per: usize) -> Vec<synth::SynthRequest> {
    let mut all: Vec<std::collections::VecDeque<synth::SynthRequest>> = (0..sessions).map(|s| synth::session_with(seed, s, per, true).into()).collect();
    let mut r = Rng::new(seed);
    let mut out = Vec::new();
    while all.iter().any(|v| !v.is_empty()) {
        let live: Vec<usize> = (0..all.len()).filter(|&i| !all[i].is_empty()).collect();
        let i = *r.pick(&live);
        out.push(all[i].pop_front().unwrap());
    }
    out
}

/// Random aggregations three ways (summaries, step scan, brute force from the messages);
/// returns how many the summaries answered.
fn check_aggregations(db: &Db, n: usize, seed: u64) -> usize {
    let raw = query::reference::raw_store(db).unwrap();
    let mut r = Rng::new(seed);
    let mut used = 0;
    for _ in 0..n {
        let q = query::reference::random_summarize(db, &mut r);
        let (u, same) = query::reference::compare_summarize(db, &raw, &q).unwrap_or_else(|e| panic!("{}: {q}", e.show(&q)));
        assert!(same, "summaries, scan and reference differ on: {q}");
        used += u as usize;
    }
    used
}

#[test]
fn aggregations_from_summaries_match_the_scan_and_the_reference() {
    let dir = tmpdir("aggsum");
    // small flushes: several segments, merged and not, and steps not flushed yet
    let o = Options { sync: SyncMode::Os, flush_bytes: 256 << 10, merge_at: 3, seg_bytes: 1 << 20, ..Options::default() };
    let mut db = Db::open(&dir, o.clone()).unwrap();
    let reqs = rich_traffic(21, 24, 50);
    for (i, q) in reqs.iter().enumerate() {
        put_req(&mut db, q);
        if i % 8 == 0 {
            db.commit().unwrap();
        }
    }
    db.commit().unwrap();
    let st = db.stats();
    assert!(st.segments >= 2 && st.active_sessions > 0 && st.summary_bytes > 0, "{} segments, {} sessions taking writes", st.segments, st.active_sessions);
    let used = check_aggregations(&db, 250, 3);
    assert!(used >= 80, "summaries answered only {used} of 250");
    // the hand-picked ones the benchmark runs, and a few edge cases
    for q in [
        r#"where tool in ("Edit", "Write", "MultiEdit", "NotebookEdit") and isnotnull(file_path) | summarize n = count() by file_path | sort by n desc, file_path asc | take 10"#,
        r#"where command contains "git commit" | summarize n = count(), s = dcount(session)"#,
        r#"where failed | summarize n = count() by session | sort by n desc, session asc | take 10"#,
        r#"summarize n = count(), df = dcount(file_path) by k = file_path | sort by k asc"#,
        r#"where command contains_cs "GIT COMMIT" | summarize n = count() by k = command | sort by k asc"#,
    ] {
        let raw = query::reference::raw_store(&db).unwrap();
        let (u, same) = query::reference::compare_summarize(&db, &raw, q).unwrap();
        assert!(u && same, "{q}");
    }
    // and after everything is flushed and merged into one segment
    db.compact().unwrap();
    let used = check_aggregations(&db, 120, 4);
    assert!(used >= 40, "summaries answered only {used} of 120");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Folds leave the parts they replace in older segments: the summaries of those segments
/// must leave them out.
#[test]
fn aggregations_skip_parts_that_folds_replaced() {
    let dir = tmpdir("aggfold");
    let o = Options { flush_bytes: 24 << 10, sync: SyncMode::Os, merge_at: 3, seg_bytes: 96 << 10, idle_flushes: 1, cache_bytes: 256 << 10, max_parts: 3, ..Options::default() };
    let wakers: Vec<Vec<synth::SynthRequest>> = (0..10).map(|s| synth::session_with(61, s, 40, true)).collect();
    let filler: Vec<synth::SynthRequest> = rich_traffic(67, 50, 24).into_iter().map(|q| synth::SynthRequest { id: format!("fill-{}", q.id), session: format!("fill-{}", q.session), ..q }).collect();
    let mut order: Vec<&synth::SynthRequest> = Vec::new();
    let mut fill = filler.chunks(filler.len() / 6);
    for round in 0..5 {
        for w in &wakers {
            order.extend(w[round * 8..round * 8 + 8].iter());
        }
        order.extend(fill.next().unwrap());
    }
    order.extend(fill.flatten());
    let mut db = Db::open(&dir, o).unwrap();
    for (i, q) in order.iter().enumerate() {
        put_req(&mut db, q);
        if i % 4 == 0 {
            db.commit().unwrap();
        }
    }
    db.flush().unwrap();
    let st = db.stats();
    assert!(st.merges.folded > 0 && st.dead_bytes > 0, "no parts replaced by folds: {:?}", st.merges);
    let used = check_aggregations(&db, 150, 9);
    assert!(used >= 40, "summaries answered only {used} of 150");
    // text search skips the replaced parts too
    let mut hits = 0;
    for q in [
        r#"messages | where text has "parser""#,
        r#"messages | where text contains "test fails" or text has "cargo""#,
        r#"messages | where role == "assistant" and not (text has "empty")"#,
        r#"messages"#,
    ] {
        let (a, b) = (message_rows(&db, q, true), message_rows(&db, q, false));
        assert_eq!(a, b, "index and scan differ on {q}");
        hits += b.len();
    }
    assert!(hits > 100, "only {hits} text units found");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Sessions taking writes get new steps between queries: their turn trees and shape lists
/// must follow.
#[test]
fn patterns_follow_sessions_taking_writes() {
    let dir = tmpdir("live");
    let mut db = Db::open(&dir, opts()).unwrap();
    let reqs = rich_traffic(33, 8, 40);
    let mut r = Rng::new(5);
    for (i, q) in reqs.iter().enumerate() {
        put_req(&mut db, q);
        if i % 37 == 36 {
            db.commit().unwrap();
            for _ in 0..4 {
                let q = query::reference::random_query(&db, &mut r);
                let (_, bad) = query::reference::compare(&db, &q, 1000).unwrap();
                assert!(bad.is_empty(), "engine and reference differ after {i} requests on: {q}");
            }
            let q = r#"match a: tool == "Write" then b: tool == "Read" where a.file_path == b.file_path"#;
            let (_, bad) = query::reference::compare(&db, q, 1000).unwrap();
            assert!(bad.is_empty(), "{q}");
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Rows of a `messages` query as (session, node, block), with the word index or by
/// reading every text.
fn message_rows(db: &Db, q: &str, index: bool) -> Vec<(u32, u32, u32)> {
    query::messages::set_index(index);
    let r = query::run(db, q, 2);
    query::messages::set_index(true);
    let r = r.unwrap_or_else(|e| panic!("{}", e.show(q)));
    assert_eq!(r.table.kind, Kind::Messages);
    r.table.rows.iter().map(|row| (row.session, row.steps[0], row.steps[1])).collect()
}

/// Text search gives the same rows from the word index as from reading every text, while
/// messages are still in memory, after flushes, across segments, after a merge and after
/// reopening.
#[test]
fn message_search_matches_a_scan_at_every_stage() {
    let dir = tmpdir("msgsearch");
    let mut db = Db::open(&dir, opts()).unwrap();
    let traffic = rich_traffic(21, 10, 30);
    let filters = [
        r#"messages | where text has "parser""#,
        r#"messages | where text has "PARSER" and role == "assistant""#,
        r#"messages | where text contains "test fails""#,
        r#"messages | where text contains "ils beca" or text has "cargo""#,
        r#"messages | where role == "user" and not (text has "empty")"#,
        r#"messages | where text contains "error[E0308]: mismatched""#,
        r#"messages | where text has "über" or text contains "naïve""#,
        r#"messages | where text contains "日本""#,
        r#"messages | where text contains_cs "None" and chars > 20"#,
        r#"messages | where text startswith "the" or text endswith "empty""#,
        r#"messages | where text has "keep going""#,
        r#"messages | where text has "no such word anywhere""#,
    ];
    let mut some = 0;
    let mut check = |db: &Db, stage: &str| {
        for q in &filters {
            let (a, b) = (message_rows(db, q, true), message_rows(db, q, false));
            assert_eq!(a, b, "{stage}: index and scan differ on {q}");
            some += !b.is_empty() as usize;
        }
    };
    let third = traffic.len() / 3;
    for (k, q) in traffic.iter().enumerate() {
        db.put(&Put { session: &q.session, agent: &q.agent, id: &q.id, time: &q.time, meta: b"", body: &q.body }).unwrap();
        if k + 1 == third {
            db.commit().unwrap();
            check(&db, "in memory");
            db.flush().unwrap();
            check(&db, "one segment");
        }
        if k + 1 == 2 * third {
            db.commit().unwrap();
            check(&db, "one segment and memory");
            db.flush().unwrap();
            check(&db, "two segments");
        }
    }
    db.commit().unwrap();
    check(&db, "two segments and memory");
    db.compact().unwrap();
    check(&db, "merged");
    drop(db);
    let db = Db::open(&dir, opts()).unwrap();
    check(&db, "reopened");
    assert!(some >= 40, "the filters should find text at most stages ({some})");
    // grouping the rows directly gives what the general path gives
    for by in ["session", "role", "agent, role"] {
        let fast = format!(r#"messages | where text has "parser" | summarize n = count(), s = dcount(session) by {by}"#);
        let slow = format!(r#"messages | where text has "parser" | extend one = 1 | summarize n = count(), s = dcount(session) by {by}"#);
        let rows = |q: &str| -> Vec<Vec<String>> { query::run(&db, q, 1).unwrap_or_else(|e| panic!("{}", e.show(q))).table.rows.iter().map(|r| r.vals.iter().map(|v| v.to_string()).collect()).collect() };
        assert_eq!(rows(&fast), rows(&slow), "grouping by {by}");
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `n` bytes with no word in them, ending in a space.
fn pad(n: usize) -> String {
    format!("{} ", ".".repeat(n - 1))
}

/// Phrases at piece boundaries, at the ends of long texts, inside longer words and next
/// to words too long for the index, and sessions that share a session id: the word index
/// finds exactly what reading every text finds.
#[test]
fn phrases_across_pieces_and_shared_session_ids() {
    let texts = [
        // "needle" starts in the first piece, "one" in the second
        format!("{}needle one {}", pad(1018), pad(500)),
        // both words, far apart
        format!("needle {} one", pad(3000)),
        // at the very end, and at the very start
        format!("{}needle one", pad(2000)),
        format!("needle one {}", pad(2000)),
        // not a whole word where the text is, but both words stand alone further on
        format!("{}xneedle one {}needle zzz one {}", pad(511), pad(577), pad(300)),
        // after a run of letters too long to be indexed whole
        format!("{}needle one", "a".repeat(200)),
        "Needle ONE".to_string(),
        "nothing here".to_string(),
    ];
    let dir = tmpdir("pieces");
    let mut db = Db::open(&dir, opts()).unwrap();
    let mut put = |db: &mut Db, session: &str, agent: &str, k: usize, text: &str| {
        let b = body(&[format!(r#"{{"role":"user","content":"{text}"}}"#)]);
        db.put(&Put { session, agent, id: &format!("r{k}"), time: &format!("{}", 1_700_000_000 + k), meta: b"", body: b.as_bytes() }).unwrap();
    };
    for (k, t) in texts.iter().enumerate() {
        put(&mut db, &format!("s{k}"), "", k, t);
    }
    // one session id stored three times: the main agent and two side agents
    put(&mut db, "shared", "", 100, "a needle for the main agent");
    put(&mut db, "shared", "side-1", 101, "a needle for a side agent");
    put(&mut db, "shared", "side-2", 102, "no such thing here");
    db.commit().unwrap();
    let check = |db: &Db, stage: &str| {
        for (q, want) in [
            (r#"messages | where text contains "needle one""#, 6),
            (r#"messages | where text has "needle one""#, 4),
            (r#"messages | where text contains "eedle on""#, 6),
            (r#"messages | where text has "needle""#, 8),
            (r#"messages | where text contains_cs "needle one""#, 5),
            (r#"messages | where not (text contains "needle one") and role == "user""#, 5),
            (r#"messages | where text endswith "needle one" or text startswith "needle one""#, 4),
        ] {
            let (a, b) = (message_rows(db, q, true), message_rows(db, q, false));
            assert_eq!(a, b, "{stage}: index and scan differ on {q}");
            assert_eq!(b.len(), want, "{stage}: {q}");
        }
        let rows = |q: &str| -> Vec<Vec<String>> { query::run(db, q, 1).unwrap_or_else(|e| panic!("{}", e.show(q))).table.rows.iter().map(|r| r.vals.iter().map(|v| v.to_string()).collect()).collect() };
        let fast = rows(r#"messages | where text has "needle" | summarize n = count(), s = dcount(session) by session | sort by session asc"#);
        let slow = rows(r#"messages | where text has "needle" | extend one = 1 | summarize n = count(), s = dcount(session) by session | sort by session asc"#);
        assert_eq!(fast, slow, "{stage}: grouping by session");
        assert!(fast.contains(&vec!["shared".to_string(), "2".to_string(), "1".to_string()]), "{stage}: the shared session id is one group");
        let all = rows(r#"messages | where text has "needle" | summarize n = count(), s = dcount(session)"#);
        assert_eq!(all, vec![vec!["8".to_string(), "7".to_string()]], "{stage}: distinct session ids");
    };
    check(&db, "in memory");
    db.flush().unwrap();
    check(&db, "flushed");
    db.compact().unwrap();
    check(&db, "merged");
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
