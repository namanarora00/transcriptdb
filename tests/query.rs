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
