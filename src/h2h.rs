//! Head-to-head against PostgreSQL on the same data.
//!
//! PostgreSQL gets the store's contents with the usual tricks: every distinct message stored
//! once (by hash), requests as arrays of message ids, a step table with indexes and the
//! two input fields the queries use as columns, and for patterns a materialized table of
//! root-to-leaf turn paths so window functions are exact on forked sessions. Pattern
//! queries are compared on the set of sessions with a match (what SQL answers naturally),
//! aggregations on their rows, reads on the bytes returned.

use crate::db::{sha256, Db, ReadStats};
use crate::pg::{Pg, PgError};
use crate::query::{self, exec::Kind, plan, value::Value};
use crate::record::NONE;
use crate::synth::Rng;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_128;

type R<T> = Result<T, String>;

fn pe(e: PgError) -> String {
    e.to_string()
}

/// Turns of a session: (turn of each step, depth of each turn, parent turn) and root-to-leaf paths.
pub fn turn_tree(db: &Db, s: u32) -> (Vec<u32>, Vec<u32>, Vec<Vec<u32>>) {
    let rows = &db.steps(s).rows;
    let mut turn_of = vec![0u32; rows.len()];
    let mut depth: Vec<u32> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let r = rows[i];
        let mut j = i + 1;
        while j < rows.len() && rows[j].prev == r.prev && rows[j].call_node == r.call_node && rows[j].result_node == r.result_node {
            j += 1;
        }
        let t = depth.len() as u32;
        for x in turn_of.iter_mut().take(j).skip(i) {
            *x = t;
        }
        let p = if r.prev == NONE { NONE } else { turn_of[r.prev as usize] };
        depth.push(if p == NONE { 1 } else { depth[p as usize] + 1 });
        parent.push(p);
        i = j;
    }
    let n = depth.len();
    let mut kids: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut roots = Vec::new();
    for t in 0..n {
        if parent[t] == NONE { roots.push(t as u32) } else { kids[parent[t] as usize].push(t as u32) }
    }
    let mut paths = Vec::new();
    let mut stack: Vec<Vec<u32>> = roots.into_iter().map(|r| vec![r]).collect();
    while let Some(p) = stack.pop() {
        let last = *p.last().unwrap() as usize;
        if kids[last].is_empty() {
            paths.push(p);
        } else {
            for &k in &kids[last] {
                let mut q = p.clone();
                q.push(k);
                stack.push(q);
            }
        }
    }
    (turn_of, depth, paths)
}

fn text_of(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.as_bytes().to_vec()),
        other => Some(other.to_string().into_bytes()),
    }
}

/// Load the store into PostgreSQL (tables dropped first).
pub fn load(db: &Db, pg: &mut Pg) -> R<String> {
    let t0 = Instant::now();
    pg.exec(
        "DROP TABLE IF EXISTS sessions, messages, nodes, requests, steps, path_steps CASCADE;
         BEGIN;
         CREATE TABLE sessions (id int PRIMARY KEY, sid text NOT NULL, agent text NOT NULL);
         CREATE TABLE messages (id int, body text NOT NULL);
         CREATE TABLE nodes (session int, node int, parent int, msg int);
         CREATE TABLE requests (id text, session int, msgs int[]);
         CREATE TABLE steps (session int, step int, turn int, depth int, prev int, call_node int, result_node int,
                             tool text, failed bool, out_bytes int, time_ms bigint, file_path text, command text);
         CREATE TABLE path_steps (session int, path int, depth int, step int, tool text, failed bool, file_path text);",
    )
    .map_err(pe)?;
    let ns = db.session_count() as u32;
    pg.copy_in("COPY sessions FROM STDIN", |c| {
        for s in 0..ns {
            let (sid, agent) = db.session_key(s);
            c.int(s as i64);
            c.field(Some(sid.as_bytes()));
            c.field(Some(agent.as_bytes()));
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    // messages once by content; nodes point at them
    let mut msg_ids: HashMap<u128, u32> = HashMap::new();
    let mut node_msg: Vec<Vec<u32>> = Vec::with_capacity(ns as usize);
    let mut distinct_bytes = 0u64;
    let mut err = None;
    pg.copy_in("COPY messages FROM STDIN", |c| {
        for s in 0..ns {
            let ids: Vec<u32> = (0..db.session_len(s)).collect();
            let msgs = match db.read_messages(s, &ids, &mut ReadStats::default()) {
                Ok(m) => m,
                Err(e) => {
                    err = Some(e.to_string());
                    return Ok(());
                }
            };
            let mut nm = Vec::with_capacity(msgs.len());
            for m in msgs {
                let h = xxh3_128(&m);
                let next = msg_ids.len() as u32;
                let id = *msg_ids.entry(h).or_insert(next);
                if id == next {
                    c.int(id as i64);
                    c.field(Some(&m));
                    c.end_row()?;
                    distinct_bytes += m.len() as u64;
                }
                nm.push(id);
            }
            node_msg.push(nm);
        }
        Ok(())
    })
    .map_err(pe)?;
    if let Some(e) = err {
        return Err(e);
    }
    pg.copy_in("COPY nodes FROM STDIN", |c| {
        for s in 0..ns {
            let parents = db.node_parents(s);
            for (n, &p) in parents.iter().enumerate() {
                c.int(s as i64);
                c.int(n as i64);
                if p == NONE { c.field(None) } else { c.int(p as i64) }
                c.int(node_msg[s as usize][n] as i64);
                c.end_row()?;
            }
        }
        Ok(())
    })
    .map_err(pe)?;
    pg.copy_in("COPY requests FROM STDIN", |c| {
        for ri in db.all_requests().map_err(|e| PgError(e.to_string()))? {
            let r = db.request(ri).map_err(|e| PgError(e.to_string()))?;
            let path = db.request_path(ri).map_err(|e| PgError(e.to_string()))?;
            let arr: Vec<String> = path.iter().map(|&n| node_msg[r.session as usize][n as usize].to_string()).collect();
            c.field(Some(r.id.as_bytes()));
            c.int(r.session as i64);
            c.field(Some(format!("{{{}}}", arr.join(",")).as_bytes()));
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    let sd = db.step_dicts();
    let mut fields_err = None;
    let mut steps_cache: Vec<(Vec<u32>, Vec<u32>, Vec<Vec<u32>>)> = Vec::with_capacity(ns as usize);
    let mut fp: Vec<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>> = Vec::with_capacity(ns as usize);
    for s in 0..ns {
        steps_cache.push(turn_tree(db, s));
        let mut v = Vec::new();
        for pos in 0..db.steps(s).rows.len() as u32 {
            let f = plan::step_value(db, s, pos, &plan::StepCol::Field("file_path".into()));
            let c = plan::step_value(db, s, pos, &plan::StepCol::Field("command".into()));
            match (f, c) {
                (Ok(f), Ok(c)) => v.push((text_of(f), text_of(c))),
                (Err(e), _) | (_, Err(e)) => fields_err = Some(e.to_string()),
            }
        }
        fp.push(v);
    }
    if let Some(e) = fields_err {
        return Err(e);
    }
    pg.copy_in("COPY steps FROM STDIN", |c| {
        for s in 0..ns {
            let ss_arc = db.steps(s);
            let ss = &*ss_arc;
            let (turn_of, depth, _) = &steps_cache[s as usize];
            for (pos, r) in ss.rows.iter().enumerate() {
                c.int(s as i64);
                c.int(pos as i64);
                c.int(turn_of[pos] as i64);
                c.int(depth[turn_of[pos] as usize] as i64);
                if r.prev == NONE { c.field(None) } else { c.int(r.prev as i64) }
                c.int(r.call_node as i64);
                c.int(r.result_node as i64);
                c.field(Some(sd.tools.get(r.tool)));
                c.field(Some(if r.failed { b"t" } else { b"f" }));
                c.int(r.out_bytes as i64);
                c.int(r.time_ms);
                let (f, cm) = &fp[s as usize][pos];
                c.field(f.as_deref());
                c.field(cm.as_deref());
                c.end_row()?;
            }
        }
        Ok(())
    })
    .map_err(pe)?;
    let mut path_rows = 0usize;
    pg.copy_in("COPY path_steps FROM STDIN", |c| {
        for s in 0..ns {
            let ss_arc = db.steps(s);
            let ss = &*ss_arc;
            let (turn_of, depth, paths) = &steps_cache[s as usize];
            let mut steps_of: HashMap<u32, Vec<u32>> = HashMap::new();
            for (pos, &t) in turn_of.iter().enumerate() {
                steps_of.entry(t).or_default().push(pos as u32);
            }
            for (pi, path) in paths.iter().enumerate() {
                for &t in path {
                    for &pos in &steps_of[&t] {
                        let r = ss.rows[pos as usize];
                        c.int(s as i64);
                        c.int(pi as i64);
                        c.int(depth[t as usize] as i64);
                        c.int(pos as i64);
                        c.field(Some(sd.tools.get(r.tool)));
                        c.field(Some(if r.failed { b"t" } else { b"f" }));
                        c.field(fp[s as usize][pos as usize].0.as_deref());
                        c.end_row()?;
                        path_rows += 1;
                    }
                }
            }
        }
        Ok(())
    })
    .map_err(pe)?;
    pg.exec(
        "ALTER TABLE messages ADD PRIMARY KEY (id);
         ALTER TABLE nodes ADD PRIMARY KEY (session, node);
         ALTER TABLE requests ADD PRIMARY KEY (id);
         ALTER TABLE steps ADD PRIMARY KEY (session, step);
         CREATE INDEX steps_tool ON steps (tool);
         CREATE INDEX path_steps_order ON path_steps (session, path, depth);
         COMMIT;",
    )
    .map_err(pe)?;
    pg.exec("VACUUM ANALYZE").map_err(pe)?;
    Ok(format!(
        "loaded {} sessions, {} distinct messages ({:.1} MB), {} requests, {} steps, {} path rows in {:.1}s",
        ns,
        msg_ids.len(),
        distinct_bytes as f64 / 1e6,
        db.request_count(),
        (0..ns).map(|s| db.steps(s).rows.len()).sum::<usize>(),
        path_rows,
        t0.elapsed().as_secs_f64()
    ))
}

pub struct QuerySpec {
    pub name: &'static str,
    pub engine: String,
    pub sql: String,
    /// Compare sets of sessions (pattern) or ordered rows (aggregation).
    pub sessions: bool,
}

const EDITS: &str = r#"("Edit", "Write", "MultiEdit", "NotebookEdit")"#;
const EDITS_SQL: &str = "('Edit','Write','MultiEdit','NotebookEdit')";

/// Window SQL for "a then b then c, nothing passing X strictly between a and c": for each b,
/// take the latest a before it and the earliest c after it (the tightest span).
fn three_step_sql(a: &str, b: &str, c: &str, x: &str) -> String {
    format!(
        "WITH w AS (SELECT session, ({b}) AS isb,
            max(depth) FILTER (WHERE {a}) OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pa,
            max(depth) FILTER (WHERE {x}) OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS px,
            min(depth) FILTER (WHERE {c}) OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS nc,
            min(depth) FILTER (WHERE {x}) OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS nx,
            bool_or({x}) OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS xhere
          FROM path_steps)
         SELECT DISTINCT session FROM w WHERE isb AND pa IS NOT NULL AND nc IS NOT NULL
           AND (px IS NULL OR px <= pa) AND NOT xhere AND (nx IS NULL OR nx >= nc)"
    )
}

/// Turn-level SQL: `cur` holds in a turn and `prev_k` held in the k turns right before it.
fn turn_seq_sql(cols: &[(&str, &str)], cond: &str) -> String {
    let aggs: Vec<String> = cols.iter().map(|(n, e)| format!("bool_or({e}) AS {n}")).collect();
    let lags: Vec<String> = cols.iter().flat_map(|(n, _)| (1..=3).map(move |k| format!("lag({n}, {k}) OVER p AS {n}{k}"))).collect();
    let plain: Vec<String> = cols.iter().map(|(n, _)| n.to_string()).collect();
    format!(
        "WITH t AS (SELECT session, path, depth, {} FROM path_steps GROUP BY session, path, depth),
         w AS (SELECT session, {}, {} FROM t WINDOW p AS (PARTITION BY session, path ORDER BY depth))
         SELECT DISTINCT session FROM w WHERE {cond}",
        aggs.join(", "),
        plain.join(", "),
        lags.join(", ")
    )
}

pub fn queries() -> Vec<QuerySpec> {
    let test = "coalesce(file_path, '') ILIKE '%test%'";
    vec![
        QuerySpec {
            name: "Bash, then Edit, then Bash; no Read between",
            engine: r#"match a: tool == "Bash" then b: tool == "Edit" then c: tool == "Bash" where no (tool == "Read") between a and c"#.into(),
            sql: three_step_sql("tool = 'Bash'", "tool = 'Edit'", "tool = 'Bash'", "tool = 'Read'"),
            sessions: true,
        },
        QuerySpec {
            name: "Bash fails, only test files edited, Bash passes",
            engine: format!(r#"match a: tool == "Bash" and failed then b: tool in {EDITS} and file_path contains "test" then c: tool == "Bash" and not failed where no (tool in {EDITS} and not file_path contains "test") between a and c"#),
            sql: three_step_sql("tool = 'Bash' AND failed", &format!("tool IN {EDITS_SQL} AND {test}"), "tool = 'Bash' AND NOT failed", &format!("tool IN {EDITS_SQL} AND NOT {test}")),
            sessions: true,
        },
        QuerySpec {
            name: "3 Read/Grep turns in a row, then Edit next",
            engine: r#"match r: tool in ("Read", "Grep"){3} next e: tool == "Edit""#.into(),
            sql: turn_seq_sql(&[("rg", "tool IN ('Read','Grep')"), ("ed", "tool = 'Edit'")], "ed AND rg1 AND rg2 AND rg3"),
            sessions: true,
        },
        QuerySpec {
            name: "Bash fails, then Bash passes the next turn",
            engine: r#"match a: tool == "Bash" and failed next b: tool == "Bash" and not failed"#.into(),
            sql: turn_seq_sql(&[("bf", "tool = 'Bash' AND failed"), ("bo", "tool = 'Bash' AND NOT failed")], "bo AND bf1"),
            sessions: true,
        },
        QuerySpec {
            name: "Edit, then Bash within 3 turns",
            engine: r#"match a: tool == "Edit" then b: tool == "Bash" within 3"#.into(),
            sql: "WITH w AS (SELECT session, tool,
                    max(depth) FILTER (WHERE tool = 'Edit') OVER (PARTITION BY session, path ORDER BY depth RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pe
                  FROM path_steps)
                 SELECT DISTINCT session FROM w WHERE tool = 'Bash' AND pe IS NOT NULL"
                .into(),
            sessions: true,
        },
        QuerySpec {
            name: "Write a file, later Read the same file",
            engine: r#"match a: tool == "Write" then b: tool == "Read" where a.file_path == b.file_path"#.into(),
            sql: "WITH w AS (SELECT session, tool,
                    max(depth) FILTER (WHERE tool = 'Write') OVER (PARTITION BY session, path, file_path ORDER BY depth RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pw
                  FROM path_steps WHERE file_path IS NOT NULL)
                 SELECT DISTINCT session FROM w WHERE tool = 'Read' AND pw IS NOT NULL"
                .into(),
            sessions: true,
        },
        QuerySpec {
            name: "3 failing turns in a row",
            engine: "match f: failed{3}".into(),
            sql: turn_seq_sql(&[("f", "failed")], "f AND f1 AND f2"),
            sessions: true,
        },
        QuerySpec {
            name: "WebSearch, then WebFetch the next turn",
            engine: r#"match a: tool == "WebSearch" next b: tool == "WebFetch""#.into(),
            sql: turn_seq_sql(&[("ws", "tool = 'WebSearch'"), ("wf", "tool = 'WebFetch'")], "wf AND ws1"),
            sessions: true,
        },
        QuerySpec {
            name: "Steps per tool, top 10",
            engine: "summarize n = count() by tool | sort by n desc, tool asc | take 10".into(),
            sql: "SELECT tool, count(*) AS n FROM steps GROUP BY tool ORDER BY n DESC, tool ASC LIMIT 10".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Failure rate per tool (100+ steps)",
            engine: "summarize n = count(), f = countif(failed) by tool | where n >= 100 | extend rate = f * 1.0 / n | project tool, n, f, rate | sort by tool asc".into(),
            sql: "SELECT tool, count(*), count(*) FILTER (WHERE failed), (count(*) FILTER (WHERE failed))::float8 / count(*) FROM steps GROUP BY tool HAVING count(*) >= 100 ORDER BY tool".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Bash steps and failures per week",
            engine: r#"where tool == "Bash" | summarize n = count(), f = countif(failed) by w = bin(time, 7d) | sort by w asc"#.into(),
            sql: "SELECT (time_ms / 604800000) * 604800000 AS w, count(*), count(*) FILTER (WHERE failed) FROM steps WHERE tool = 'Bash' GROUP BY w ORDER BY w".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Sessions with the most failed steps",
            engine: "where failed | summarize n = count() by session | sort by n desc, session asc | take 10".into(),
            sql: "SELECT se.sid, count(*) AS n FROM steps s JOIN sessions se ON se.id = s.session WHERE s.failed GROUP BY se.sid ORDER BY n DESC, se.sid ASC LIMIT 10".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Most edited files",
            engine: format!(r#"where tool in {EDITS} and isnotnull(file_path) | summarize n = count() by file_path | sort by n desc, file_path asc | take 10"#),
            sql: format!("SELECT file_path, count(*) AS n FROM steps WHERE tool IN {EDITS_SQL} AND file_path IS NOT NULL GROUP BY file_path ORDER BY n DESC, file_path ASC LIMIT 10"),
            sessions: false,
        },
        QuerySpec {
            name: "Average output size per tool",
            engine: "summarize a = avg(out_bytes), n = count() by tool | sort by tool asc".into(),
            sql: "SELECT tool, avg(out_bytes)::float8, count(*) FROM steps GROUP BY tool ORDER BY tool".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Sessions using browser tools, per day",
            engine: r#"where tool startswith "mcp__Claude_Browser" | summarize s = dcount(session) by d = bin(time, 1d) | sort by d asc"#.into(),
            sql: r"SELECT (s.time_ms / 86400000) * 86400000 AS d, count(DISTINCT se.sid) FROM steps s JOIN sessions se ON se.id = s.session WHERE s.tool ILIKE 'mcp\_\_claude\_browser%' GROUP BY d ORDER BY d".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Commands running git commit",
            engine: r#"where command contains "git commit" | summarize n = count(), s = dcount(session)"#.into(),
            sql: "SELECT count(*), count(DISTINCT se.sid) FROM steps s JOIN sessions se ON se.id = s.session WHERE s.command ILIKE '%git commit%'".into(),
            sessions: false,
        },
    ]
}

fn cell_eq(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0),
        _ => false,
    }
}

fn engine_cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Float(f) => format!("{f}"),
        Value::Time(t) | Value::Dur(t) => t.to_string(),
        Value::Bool(b) => if *b { "t".into() } else { "f".into() },
        other => other.to_string(),
    }
}

pub struct Outcome {
    pub name: String,
    pub same: bool,
    pub engine_ms: f64,
    pub engine_ms_par: f64,
    pub pg_ms: f64,
    pub detail: String,
}

fn best<T>(reps: usize, mut f: impl FnMut() -> R<T>) -> R<(T, f64)> {
    let mut out = None;
    let mut t = f64::MAX;
    for _ in 0..reps.max(1) {
        let a = Instant::now();
        let v = f()?;
        t = t.min(a.elapsed().as_secs_f64() * 1e3);
        out = Some(v);
    }
    Ok((out.unwrap(), t))
}

pub fn run_queries(db: &Db, pg: &mut Pg, reps: usize) -> R<Vec<Outcome>> {
    let mut out = Vec::new();
    for q in queries() {
        let run = |threads: usize| query::run(db, &q.engine, threads).map_err(|e| format!("{}: {}", q.name, e.show(&q.engine)));
        let (er, e1) = best(reps, || run(1))?;
        let (_, e8) = best(reps, || run(8))?;
        let (pr, pms) = best(reps, || pg.query(&q.sql).map_err(|e| format!("{}: {e}", q.name)))?;
        let (same, detail) = if q.sessions {
            let Kind::Matches(_) = er.table.kind else { return Err("pattern query did not return matches".into()) };
            let es: BTreeSet<u32> = er.table.rows.iter().map(|r| r.session).collect();
            let ps: BTreeSet<u32> = pr.iter().filter_map(|r| r[0].as_ref()?.parse().ok()).collect();
            (es == ps, format!("{} sessions", es.len()))
        } else {
            let erows: Vec<Vec<String>> = er.table.rows.iter().map(|r| r.vals.iter().map(engine_cell).collect()).collect();
            let prows: Vec<Vec<String>> = pr.iter().map(|r| r.iter().map(|c| c.clone().unwrap_or_default()).collect()).collect();
            let same = erows.len() == prows.len() && erows.iter().zip(&prows).all(|(a, b)| a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cell_eq(x, y)));
            (same, format!("{} rows", erows.len()))
        };
        out.push(Outcome { name: q.name.into(), same, engine_ms: e1, engine_ms_par: e8, pg_ms: pms, detail });
    }
    Ok(out)
}

fn quantile(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

pub struct ReadOutcome {
    pub name: String,
    pub same: usize,
    pub total: usize,
    pub engine: (f64, f64),
    pub pg: (f64, f64),
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Reads: a request's messages, 20 steps with their messages, a whole session. Median and p95 ms.
pub fn run_reads(db: &Db, pg: &mut Pg, n: usize, seed: u64) -> R<Vec<ReadOutcome>> {
    let mut r = Rng::new(seed);
    let mut res = Vec::new();
    let e = |x: crate::codec::Error| x.to_string();
    let all = db.all_requests().map_err(e)?;
    // 1. a request's messages, in order
    {
        let (mut te, mut tp, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n {
            let ri = all[r.below(all.len() as u64) as usize];
            let rec = db.request(ri).map_err(e)?;
            let a = Instant::now();
            let path = db.request_path(ri).map_err(e)?;
            let msgs = db.read_messages(rec.session, &path, &mut ReadStats::default()).map_err(e)?;
            let eb = msgs.join(&b","[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let sql = format!(
                "SELECT string_agg(m.body, ',' ORDER BY u.o) FROM requests r, unnest(r.msgs) WITH ORDINALITY AS u(id, o) JOIN messages m ON m.id = u.id WHERE r.id = {}",
                sql_str(&rec.id)
            );
            let a = Instant::now();
            let rows = pg.query(&sql).map_err(pe)?;
            tp.push(a.elapsed().as_secs_f64() * 1e3);
            let pb = rows.first().and_then(|x| x[0].clone()).unwrap_or_default();
            same += (sha256(&eb) == sha256(pb.as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "a request's messages in order".into(), same, total: n, engine: (quantile(&mut te.clone(), 0.5), quantile(&mut te, 0.95)), pg: (quantile(&mut tp.clone(), 0.5), quantile(&mut tp, 0.95)) });
    }
    // 2. 20 steps of a session with their call and result messages
    {
        let long: Vec<u32> = (0..db.session_count() as u32).filter(|&s| db.steps(s).rows.len() >= 20).collect();
        let (mut te, mut tp, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n {
            let s = *r.pick(&long);
            let ss_arc = db.steps(s);
            let ss = &*ss_arc;
            let k = r.below(ss.rows.len() as u64 - 19) as usize;
            let a = Instant::now();
            let mut ids = Vec::with_capacity(40);
            for row in &ss.rows[k..k + 20] {
                ids.push(row.call_node);
                ids.push(row.result_node);
            }
            let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).map_err(e)?;
            let eb = msgs.join(&b"|"[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let sql = format!(
                "SELECT cm.body, rm.body FROM steps s
                   JOIN nodes cn ON cn.session = s.session AND cn.node = s.call_node JOIN messages cm ON cm.id = cn.msg
                   JOIN nodes rn ON rn.session = s.session AND rn.node = s.result_node JOIN messages rm ON rm.id = rn.msg
                 WHERE s.session = {s} AND s.step >= {k} AND s.step < {} ORDER BY s.step",
                k + 20
            );
            let a = Instant::now();
            let rows = pg.query(&sql).map_err(pe)?;
            tp.push(a.elapsed().as_secs_f64() * 1e3);
            let pb: Vec<String> = rows.into_iter().flat_map(|x| x.into_iter().map(|c| c.unwrap_or_default())).collect();
            same += (sha256(&eb) == sha256(pb.join("|").as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "20 steps with their messages".into(), same, total: n, engine: (quantile(&mut te.clone(), 0.5), quantile(&mut te, 0.95)), pg: (quantile(&mut tp.clone(), 0.5), quantile(&mut tp, 0.95)) });
    }
    // 3. every message of a session
    {
        let (mut te, mut tp, mut same) = (Vec::new(), Vec::new(), 0);
        let m = n.min(db.session_count());
        for _ in 0..m {
            let s = r.below(db.session_count() as u64) as u32;
            let a = Instant::now();
            let ids: Vec<u32> = (0..db.session_len(s)).collect();
            let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).map_err(e)?;
            let eb = msgs.join(&b","[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let sql = format!("SELECT string_agg(m.body, ',' ORDER BY n.node) FROM nodes n JOIN messages m ON m.id = n.msg WHERE n.session = {s}");
            let a = Instant::now();
            let rows = pg.query(&sql).map_err(pe)?;
            tp.push(a.elapsed().as_secs_f64() * 1e3);
            let pb = rows.first().and_then(|x| x[0].clone()).unwrap_or_default();
            same += (sha256(&eb) == sha256(pb.as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "a whole session's messages".into(), same, total: m, engine: (quantile(&mut te.clone(), 0.5), quantile(&mut te, 0.95)), pg: (quantile(&mut tp.clone(), 0.5), quantile(&mut tp, 0.95)) });
    }
    Ok(res)
}

/// Where PostgreSQL's bytes are: per table its heap (with free space and visibility maps),
/// TOAST (with its index) and indexes; then how much of the message table the opaque values
/// cost, found by loading copies of it with each kind blanked (same rows, same order) and
/// comparing heap + TOAST. Returns printable lines.
pub fn pg_split(pg: &mut Pg) -> R<Vec<String>> {
    let mut out = Vec::new();
    let rows = pg
        .query(
            "SELECT c.relname, pg_relation_size(c.oid, 'main'), pg_relation_size(c.oid, 'fsm') + pg_relation_size(c.oid, 'vm'),
                    coalesce(pg_total_relation_size(nullif(c.reltoastrelid, 0)), 0), pg_indexes_size(c.oid)
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY pg_total_relation_size(c.oid) DESC",
        )
        .map_err(pe)?;
    let num = |v: &Option<String>| v.as_deref().unwrap_or("0").parse::<u64>().unwrap_or(0);
    let mb = |b: u64| format!("{:.1} MB", b as f64 / 1e6);
    for r in &rows {
        out.push(format!(
            "{:<11} heap {:>9}  maps {:>8}  TOAST {:>9}  indexes {:>9}",
            r[0].clone().unwrap_or_default(),
            mb(num(&r[1])),
            mb(num(&r[2])),
            mb(num(&r[3])),
            mb(num(&r[4]))
        ));
    }
    let r = pg
        .query("SELECT count(*), sum(octet_length(body)), sum(pg_column_size(body)), count(*) FILTER (WHERE pg_column_compression(body) IS NOT NULL) FROM messages")
        .map_err(pe)?;
    out.push(format!("messages: {} rows, {} of text, {} as stored (pg_column_size), {} rows compressed", num(&r[0][0]), mb(num(&r[0][1])), mb(num(&r[0][2])), num(&r[0][3])));
    // canonical messages have sorted keys, so an image's data comes right before its media type
    const B64: &str = "[A-Za-z0-9+/=]{64,}";
    let kinds = [
        ("thinking signatures", format!("\"signature\":\"{B64}\""), "\"signature\":\"\""),
        ("images", format!("\"data\":\"{B64}\"(?=,\"media_type\":\"image/)"), "\"data\":\"\""),
        ("other data (documents, redacted)", format!("\"data\":\"{B64}\"(?!,\"media_type\":\"image/)"), "\"data\":\"\""),
    ];
    let base = |pg: &mut Pg, t: &str| -> R<u64> {
        let r = pg.query(&format!("SELECT pg_relation_size('{t}', 'main') + coalesce(pg_total_relation_size(nullif((SELECT reltoastrelid FROM pg_class WHERE relname = '{t}'), 0)), 0)")).map_err(pe)?;
        Ok(num(&r[0][0]))
    };
    let full = base(pg, "messages")?;
    let mut all = String::from("body");
    for (i, (name, pat, rep)) in kinds.iter().enumerate() {
        let r = pg.query(&format!("SELECT count(*), coalesce(sum(length(m[1])), 0) FROM messages, regexp_matches(body, '({pat})', 'g') AS m")).map_err(pe)?;
        let (n, text) = (num(&r[0][0]), num(&r[0][1]));
        let t = format!("m_blank{i}");
        pg.exec(&format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} AS SELECT id, regexp_replace(body, '{pat}', '{rep}', 'g') AS body FROM messages ORDER BY id")).map_err(pe)?;
        let without = base(pg, &t)?;
        pg.exec(&format!("DROP TABLE {t}")).map_err(pe)?;
        all = format!("regexp_replace({all}, '{pat}', '{rep}', 'g')");
        out.push(format!("  {name}: {n} values, {} of base64 text; heap + TOAST {} with them, {} without: they cost {}", mb(text), mb(full), mb(without), mb(full.saturating_sub(without))));
    }
    pg.exec(&format!("DROP TABLE IF EXISTS m_blank; CREATE TABLE m_blank AS SELECT id, {all} AS body FROM messages ORDER BY id")).map_err(pe)?;
    let without = base(pg, "m_blank")?;
    pg.exec("DROP TABLE m_blank").map_err(pe)?;
    out.push(format!("  all of them blanked: heap + TOAST {}; opaque values cost {}", mb(without), mb(full.saturating_sub(without))));
    Ok(out)
}

/// PostgreSQL bytes per table (with indexes and TOAST).
pub fn pg_sizes(pg: &mut Pg) -> R<Vec<(String, u64)>> {
    let rows = pg
        .query(
            "SELECT c.relname, pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 2 DESC",
        )
        .map_err(pe)?;
    Ok(rows.into_iter().map(|r| (r[0].clone().unwrap_or_default(), r[1].as_deref().unwrap_or("0").parse().unwrap_or(0))).collect())
}
