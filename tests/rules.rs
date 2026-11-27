//! Column rules: the values a rule gives are the same whether they were worked out when a
//! step was written, when its session is read from a segment written without the rule, or
//! by the merge that stores them.

use transcriptdb::query;
use transcriptdb::{Db, Options, Put, SyncMode};

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-rtest-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn opts() -> Options {
    Options { sync: SyncMode::Os, flush_bytes: 1 << 30, ..Options::default() }
}

/// A step: tool, input object (JSON text), result content (JSON text), failed.
type Step = (&'static str, String, String, bool);

fn bash(cmd: &str, out: &str, failed: bool) -> Step {
    ("Bash", format!(r#"{{"command":{cmd:?}}}"#), format!("{out:?}"), failed)
}

/// Requests of a session whose context grows one step at a time.
fn bodies(steps: &[Step], tag: &str) -> Vec<String> {
    let body = |msgs: &[String]| format!(r#"{{"model":"m","max_tokens":10,"messages":[{}]}}"#, msgs.join(","));
    let mut msgs = vec![r#"{"role":"user","content":"start"}"#.to_string()];
    let mut out = vec![body(&msgs)];
    for (k, (tool, input, content, failed)) in steps.iter().enumerate() {
        msgs.push(format!(r#"{{"role":"assistant","content":[{{"type":"tool_use","id":"{tag}{k}","name":"{tool}","input":{input}}}]}}"#));
        msgs.push(format!(r#"{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"{tag}{k}","content":{content},"is_error":{failed}}}]}}"#));
        out.push(body(&msgs));
    }
    out
}

fn sessions() -> Vec<Vec<Step>> {
    let long = format!("echo {}", "a".repeat(6000));
    let edit = || ("Edit", r#"{"file_path":"/repo/src/lib.rs"}"#.to_string(), r#""ok""#.to_string(), false);
    vec![
        vec![
            bash("cargo test --all", "test result: FAILED. 7 passed; 3 failed; 0 ignored", true),
            edit(),
            bash("cargo test --all", "test result: ok. 10 passed; 0 failed", false),
            bash("  pytest -q tests/", "2 failed, 5 passed in 0.3s", false),
            bash(&long, "aaaa", false),
            bash("ls -la", "12 failed", false),
        ],
        vec![
            // a result given as a list of blocks, and an input field with a rule's name
            ("Bash", r#"{"command":"pytest tests/unit"}"#.to_string(), r#"[{"type":"text","text":"collected 9 items"},{"type":"text","text":"4 failed in 1s"}]"#.to_string(), false),
            ("Run", r#"{"program":"xyz","args":"-v"}"#.to_string(), r#""done""#.to_string(), false),
            edit(),
            bash("cargo build", "Compiling", false),
            bash("git status", "clean", false),
            bash("cargo test one", "test result: FAILED. 0 passed; 1 failed", true),
            ("Read", r#"{"file_path":"/repo/src/lib.rs"}"#.to_string(), r#""fn main() {}""#.to_string(), true),
        ],
    ]
}

fn extra() -> Vec<Step> {
    vec![bash("cargo test again", "test result: FAILED. 1 passed; 6 failed", true), ("Edit", r#"{"file_path":"/repo/a.rs"}"#.to_string(), r#""ok""#.to_string(), false), bash("cargo test again", "test result: ok. 7 passed; 0 failed", false)]
}

const RULES_V1: &[&str] = &[
    r#".create column program on tool == "Bash" = extract(command, @"^\s*(\S+)")"#,
    r#".create column tests_failed on program in ("cargo", "pytest") = toint(extract(output, @"(\d+) failed"))"#,
    "broken = failed or tests_failed > 0",
    "cmdlen = strlen(command)",
];
/// The same columns with `program` redefined (the second word of the command).
const PROGRAM_V2: &str = r#"program on tool == "Bash" = extract(command, @"^\s*\S+\s+(\S+)")"#;

const QUERIES: &[&str] = &[
    "summarize n = count() by program | sort by program asc",
    "where tests_failed > 0 | summarize n = count(), f = dcount(tests_failed) by session | sort by session asc",
    "where broken | count",
    r#"match a: program == "cargo" and broken then b: tool == "Edit" then c: program == "cargo" and not broken | summarize n = count()"#,
    "summarize n = count(), d = dcount(program) by broken | sort by broken asc",
    "where cmdlen > 4096 | count",
    "summarize n = count() by tests_failed | sort by tests_failed asc",
    "where isnotnull(cmdlen) | summarize n = count(), big = countif(cmdlen > 20) by tool | sort by tool asc",
    r#"where tool == "Run" and isnotnull(program) | count"#,
    r#"where program contains "car" | summarize n = count(), s = dcount(session)"#,
    "where isnotnull(tests_failed) | project tests_failed, broken, program | sort by tests_failed asc, program asc, broken asc",
];

fn put_all(db: &mut Db, name: &str, steps: &[Step], k: &mut usize) {
    for b in bodies(steps, name) {
        db.put(&Put { session: name, agent: "", id: &format!("r{k}"), time: &format!("{}", 1_700_000_000 + *k), meta: b"", body: b.as_bytes(), response: b"" }).unwrap();
        *k += 1;
    }
    db.commit().unwrap();
}

fn rows(db: &Db, q: &str) -> Vec<Vec<String>> {
    query::run(db, q, 1).unwrap_or_else(|e| panic!("{}", e.show(q))).table.rows.iter().map(|r| r.vals.iter().map(|v| v.to_string()).collect()).collect()
}

fn answers(db: &Db) -> Vec<Vec<Vec<String>>> {
    QUERIES.iter().map(|q| rows(db, q)).collect()
}

fn same(a: &[Vec<Vec<String>>], b: &[Vec<Vec<String>>], stage: &str) {
    for (k, q) in QUERIES.iter().enumerate() {
        assert_eq!(a[k], b[k], "{stage}: {q}");
    }
}

#[test]
fn rule_values_are_the_same_however_they_were_worked_out() {
    // the rules are there before the first step: every value is worked out at write time
    let da = tmpdir("write");
    let mut a = Db::open(&da, opts()).unwrap();
    for r in RULES_V1 {
        a.define_rule(r).unwrap();
    }
    let mut ka = 0;
    for (i, s) in sessions().iter().enumerate() {
        put_all(&mut a, &format!("s{i}"), s, &mut ka);
    }
    let want = answers(&a);
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    assert_eq!(want[0], vec![s(&["", "4"]), s(&["cargo", "4"]), s(&["echo", "1"]), s(&["git", "1"]), s(&["ls", "1"]), s(&["pytest", "2"])]);
    assert_eq!(want[1], vec![s(&["s0", "2", "2"]), s(&["s1", "2", "2"])]);
    // failed steps (two cargo runs, the Read) and the two pytest runs that say tests failed
    assert_eq!(want[2], vec![s(&["5"])]);
    assert_eq!(want[3], vec![s(&["1"])]);
    assert_eq!(want[5], vec![s(&["1"])]);
    // `ls` printed "12 failed" but is not a test run: no value
    assert_eq!(want[6], vec![s(&["", "8"]), s(&["0", "1"]), s(&["1", "1"]), s(&["2", "1"]), s(&["3", "1"]), s(&["4", "1"])]);
    // the input field `program` of the Run call is hidden by the rule
    assert_eq!(want[8], vec![s(&["0"])]);
    a.flush().unwrap();
    same(&answers(&a), &want, "write time, flushed");
    assert!(query::run(&a, QUERIES[0], 1).unwrap().stats.summarized, "a flushed segment written under the rules answers from its summaries");

    // no rules while the steps are written and flushed; then the rules arrive
    let db_dir = tmpdir("later");
    let mut b = Db::open(&db_dir, opts()).unwrap();
    let mut kb = 0;
    for (i, s) in sessions().iter().enumerate() {
        put_all(&mut b, &format!("s{i}"), s, &mut kb);
    }
    assert_eq!(rows(&b, QUERIES[8]), vec![s(&["1"])], "without the rule the input field shows");
    b.flush().unwrap();
    for r in RULES_V1 {
        b.define_rule(r).unwrap();
    }
    assert_eq!(b.rule_coverage(), vec![(0, 1); 4]);
    same(&answers(&b), &want, "worked out when read");
    let run = query::run(&b, QUERIES[0], 1).unwrap();
    assert!(run.stats.summary_segments == 0 && run.stats.steps_scanned > 0, "a segment written without the rule is read, not summarized");

    // new steps on both: written under the rules next to steps that are worked out when read
    put_all(&mut a, "s2", &extra(), &mut ka);
    put_all(&mut b, "s2", &extra(), &mut kb);
    let want2 = answers(&a);
    assert_ne!(want2[0], want[0]);
    same(&answers(&b), &want2, "old steps read, new steps written");
    b.flush().unwrap();
    same(&answers(&b), &want2, "two segments, one without the rules");
    assert_eq!(b.rule_coverage(), vec![(1, 2); 4]);

    // closed and opened again: the rules are kept
    drop(b);
    let mut b = Db::open(&db_dir, opts()).unwrap();
    assert_eq!(b.rules().rules.len(), 4);
    same(&answers(&b), &want2, "reopened");

    // a merge stores the values
    b.compact().unwrap();
    assert_eq!(b.rule_coverage(), vec![(1, 1); 4]);
    same(&answers(&b), &want2, "stored by a merge");
    let run = query::run(&b, QUERIES[0], 1).unwrap();
    assert!(run.stats.summarized && run.stats.steps_scanned == 0, "after the merge the summaries answer");
    a.compact().unwrap();
    same(&answers(&a), &want2, "write time, merged");

    // a rule redefined: its values and those of the rules that read it follow at once, and
    // equal those of a store that had the new rule from the start
    let dc = tmpdir("v2");
    let mut c = Db::open(&dc, opts()).unwrap();
    for r in [PROGRAM_V2, RULES_V1[1], RULES_V1[2], RULES_V1[3]] {
        c.define_rule(r).unwrap();
    }
    let mut kc = 0;
    for (i, s) in sessions().iter().enumerate() {
        put_all(&mut c, &format!("s{i}"), s, &mut kc);
    }
    put_all(&mut c, "s2", &extra(), &mut kc);
    let want3 = answers(&c);
    assert_ne!(want3[0], want2[0]);
    // nothing is a test run any more: the second word is never `cargo` or `pytest`
    assert_eq!(want3[6], vec![s(&["", "16"])]);
    b.define_rule(PROGRAM_V2).unwrap();
    // `cmdlen` does not read `program`: its stored values stay
    assert_eq!(b.rule_coverage(), vec![(0, 1), (0, 1), (0, 1), (1, 1)]);
    same(&answers(&b), &want3, "redefined, stored values of the old rule left out");
    assert!(query::run(&b, QUERIES[7], 1).unwrap().stats.summarized, "a column whose rule did not change is still summarized");
    b.compact().unwrap();
    assert_eq!(b.rule_coverage(), vec![(1, 1); 4]);
    same(&answers(&b), &want3, "redefined and stored again");

    // a rule dropped: its stored values are not read, before or after a merge
    b.drop_rule("cmdlen").unwrap();
    assert!(b.drop_rule("program").is_err(), "a rule another rule reads stays");
    assert_eq!(rows(&b, "where isnotnull(cmdlen) | count"), vec![s(&["0"])]);
    assert_eq!(rows(&b, "summarize n = count() by cmdlen | sort by cmdlen asc"), vec![s(&["", "16"])]);
    b.compact().unwrap();
    assert_eq!(rows(&b, "where isnotnull(cmdlen) | count"), vec![s(&["0"])]);
    assert_eq!(rows(&b, QUERIES[0]), want3[0]);

    // steps not flushed when the store closes are replayed under the rules
    let dd = tmpdir("replay");
    let mut d = Db::open(&dd, opts()).unwrap();
    for r in RULES_V1 {
        d.define_rule(r).unwrap();
    }
    let mut kd = 0;
    for (i, s) in sessions().iter().enumerate() {
        put_all(&mut d, &format!("s{i}"), s, &mut kd);
    }
    drop(d);
    let d = Db::open(&dd, opts()).unwrap();
    same(&answers(&d), &want, "replayed from the log");

    for (db, dir) in [(a, da), (b, db_dir), (c, dc), (d, dd)] {
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn rules_that_cannot_be_defined() {
    let dir = tmpdir("bad");
    let mut db = Db::open(&dir, opts()).unwrap();
    assert!(db.define_rule("tool = 1").is_err());
    assert!(db.define_rule("x = now()").is_err());
    assert!(db.define_rule("x = nothing(").is_err());
    db.define_rule("x = out_bytes > 3").unwrap();
    assert!(db.define_rule("y = input.x").is_err());
    assert!(db.drop_rule("z").is_err());
    assert_eq!(db.rules().rules.len(), 1);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
