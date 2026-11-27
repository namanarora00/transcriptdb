//! The head-to-head against DuckDB (`tdb duck`, built with `--features duckdb`).
//!
//! DuckDB gets what PostgreSQL gets in [`crate::h2h`]: the store's data decomposed into
//! tables (each distinct message once, a step table, turn paths for the sequence queries,
//! everything needed to rebuild whole requests), a lower-cased copy of the text with its
//! whole words for search, and the same queries in DuckDB's dialect ([`to_duck`]). Primary
//! keys give it ART indexes for the point reads. It runs in this process, as DuckDB is
//! meant to (no network between the two sides), on every core by default; it is also timed
//! on one thread. Every answer and every read is compared with the store's.
//!
//! Not given to DuckDB: its full-text extension (it would have to be downloaded at run
//! time; whole-word search scans a list of each text's words instead), and a way to empty
//! its buffer pool (so there are no cold reads).

use crate::db::{Db, ReadStats};
use crate::h2h::{self, Outcome, QuerySpec, ReadOutcome};
use crate::query::{self, exec::Kind, plan};
use crate::record::NONE;
use crate::split::Skeleton;
use crate::synth::Rng;
use duckdb::types::Value as DV;
use duckdb::{params, Connection};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_128;

type R<T> = Result<T, String>;

fn de(e: duckdb::Error) -> String {
    e.to_string()
}

/// A DuckDB value as PostgreSQL's text format would give it.
fn cell(v: DV) -> Option<String> {
    Some(match v {
        DV::Null => return None,
        DV::Boolean(b) => if b { "t" } else { "f" }.into(),
        DV::TinyInt(x) => x.to_string(),
        DV::SmallInt(x) => x.to_string(),
        DV::Int(x) => x.to_string(),
        DV::BigInt(x) => x.to_string(),
        DV::HugeInt(x) => x.to_string(),
        DV::UTinyInt(x) => x.to_string(),
        DV::USmallInt(x) => x.to_string(),
        DV::UInt(x) => x.to_string(),
        DV::UBigInt(x) => x.to_string(),
        DV::Float(x) => x.to_string(),
        DV::Double(x) => x.to_string(),
        DV::Text(s) => s,
        DV::List(xs) => format!("{{{}}}", xs.into_iter().map(|x| cell(x).unwrap_or_default()).collect::<Vec<_>>().join(",")),
        other => format!("{other:?}"),
    })
}

/// Every row of a query, as text cells.
pub fn rows(conn: &Connection, sql: &str) -> R<Vec<Vec<Option<String>>>> {
    rows_p(conn, sql, &[])
}

/// [`rows`] for a statement with parameters, prepared once and kept (reads run the same
/// statement many times, so DuckDB plans each once).
pub fn rows_p(conn: &Connection, sql: &str, p: &[&dyn duckdb::ToSql]) -> R<Vec<Vec<Option<String>>>> {
    let mut st = conn.prepare_cached(sql).map_err(de)?;
    let mut rs = st.query(p).map_err(de)?;
    let mut out = Vec::new();
    while let Some(r) = rs.next().map_err(de)? {
        let mut line = Vec::new();
        for i in 0.. {
            match r.get::<usize, DV>(i) {
                Ok(v) => line.push(cell(v)),
                Err(duckdb::Error::InvalidColumnIndex(_)) => break,
                Err(e) => return Err(e.to_string()),
            }
        }
        out.push(line);
    }
    Ok(out)
}

/// The PostgreSQL text of a query of [`h2h::queries`] in DuckDB's dialect: `/` on integers
/// is `//`, whole-word search tests the word list, `~` is `regexp_matches` with `.` matching
/// newlines as it does in PostgreSQL, and LIKE patterns with backslashes say it escapes.
pub fn to_duck(sql: &str) -> String {
    let div = regex::Regex::new(r"\(([a-z_.]+) / (\d+)\)").unwrap();
    let terms = regex::Regex::new(r"(\w+\.)?terms @@ '([^']+)'::tsquery").unwrap();
    let re = regex::Regex::new(r"([\w.]+) ~ '((?:[^']|'')*)'").unwrap();
    let like = regex::Regex::new(r"(I?LIKE '[^']*\\[^']*')").unwrap();
    let s = div.replace_all(sql, "($1 // $2)");
    let s = terms.replace_all(&s, "list_contains(${1}terms, '$2')");
    let s = re.replace_all(&s, "regexp_matches($1, '(?s)$2')");
    like.replace_all(&s, r"$1 ESCAPE '\'").into_owned()
}

const SCHEMA: &str = "
CREATE TABLE sessions (id INTEGER PRIMARY KEY, sid VARCHAR NOT NULL, agent VARCHAR NOT NULL);
CREATE TABLE messages (id INTEGER PRIMARY KEY, body VARCHAR NOT NULL);
CREATE TABLE nodes (session INTEGER, node INTEGER, parent INTEGER, msg INTEGER, PRIMARY KEY (session, node));
CREATE TABLE requests_s (id VARCHAR, session INTEGER, msgs VARCHAR, skeleton INTEGER, system INTEGER, tools INTEGER, time VARCHAR, meta INTEGER, notes VARCHAR, layout VARCHAR);
CREATE TABLE requests (id VARCHAR PRIMARY KEY, session INTEGER, msgs INTEGER[], skeleton INTEGER, system INTEGER, tools INTEGER, time VARCHAR, meta INTEGER, notes INTEGER[], layout VARCHAR);
CREATE TABLE metas (id INTEGER PRIMARY KEY, body VARCHAR);
CREATE TABLE pieces_s (id INTEGER, kind SMALLINT, body VARCHAR, holes VARCHAR, elements VARCHAR);
CREATE TABLE pieces (id INTEGER PRIMARY KEY, kind SMALLINT, body VARCHAR, holes INTEGER[], elements INTEGER[]);
CREATE TABLE blocks (id INTEGER PRIMARY KEY, body VARCHAR);
CREATE TABLE steps (session INTEGER, step INTEGER, turn INTEGER, depth INTEGER, prev INTEGER, call_node INTEGER, result_node INTEGER,
                    tool VARCHAR, failed BOOLEAN, out_bytes INTEGER, time_ms BIGINT, file_path VARCHAR, command VARCHAR,
                    program VARCHAR, tests_failed INTEGER, call_msg INTEGER, result_msg INTEGER, PRIMARY KEY (session, step));
CREATE TABLE path_steps (session INTEGER, path INTEGER, depth INTEGER, step INTEGER, tool VARCHAR, failed BOOLEAN, file_path VARCHAR, tests_failed INTEGER);
CREATE TABLE texts_s (session INTEGER, unit INTEGER, node INTEGER, role VARCHAR, time_ms BIGINT, lbody VARCHAR, terms VARCHAR);
CREATE TABLE texts (session INTEGER, unit INTEGER, node INTEGER, role VARCHAR, time_ms BIGINT, lbody VARCHAR, terms VARCHAR[], PRIMARY KEY (session, unit));
";

/// Lists arrive as comma-joined text and become `INTEGER[]` here.
const FINISH: &str = "
INSERT INTO requests SELECT id, session, CASE WHEN msgs = '' THEN []::INTEGER[] ELSE string_split(msgs, ',')::INTEGER[] END,
  skeleton, system, tools, time, meta, CASE WHEN notes IS NULL THEN NULL ELSE string_split(notes, ',')::INTEGER[] END, layout FROM requests_s;
INSERT INTO pieces SELECT id, kind, body, CASE WHEN holes IS NULL THEN NULL WHEN holes = '' THEN []::INTEGER[] ELSE string_split(holes, ',')::INTEGER[] END,
  CASE WHEN elements IS NULL THEN NULL ELSE string_split(elements, ',')::INTEGER[] END FROM pieces_s;
INSERT INTO texts SELECT session, unit, node, role, time_ms, lbody, CASE WHEN terms = '' THEN []::VARCHAR[] ELSE string_split(terms, ' ') END FROM texts_s;
DROP TABLE requests_s; DROP TABLE pieces_s; DROP TABLE texts_s;
-- an inverted index the application builds: each text's words, sorted by word so DuckDB's
-- zone maps skip all but the word asked for
CREATE TABLE words AS SELECT unnest(terms) AS word, session, unit, role FROM texts ORDER BY word, session, unit;
CHECKPOINT;
";

fn utf8(b: &[u8]) -> R<&str> {
    std::str::from_utf8(b).map_err(|_| "a value is not UTF-8".to_string())
}

/// Load the store into a new DuckDB database. `keys`: with primary keys (ART indexes, for
/// the point reads); without, smaller, and every read scans.
pub fn load(db: &Db, conn: &Connection, keys: bool) -> R<String> {
    let t0 = Instant::now();
    // (a forced compression, set by the caller, applies to what this writes)
    let schema = if keys { SCHEMA.to_string() } else { regex::Regex::new(r",? ?PRIMARY KEY( \([a-z, ]+\))?").unwrap().replace_all(SCHEMA, "").into_owned() };
    conn.execute_batch(&schema).map_err(de)?;
    let ns = db.session_count() as u32;
    let e = |x: crate::codec::Error| x.to_string();
    {
        let mut a = conn.appender("sessions").map_err(de)?;
        for s in 0..ns {
            let (sid, agent) = db.session_key(s);
            a.append_row(params![s as i32, sid, agent]).map_err(de)?;
        }
    }
    // messages once by content; nodes point at them
    let mut msg_ids: HashMap<u128, u32> = HashMap::new();
    let mut node_msg: Vec<Vec<u32>> = Vec::with_capacity(ns as usize);
    let mut distinct = 0u64;
    {
        let mut a = conn.appender("messages").map_err(de)?;
        for s in 0..ns {
            let ids: Vec<u32> = (0..db.session_len(s)).collect();
            let msgs = db.read_messages(s, &ids, &mut ReadStats::default()).map_err(e)?;
            let mut nm = Vec::with_capacity(msgs.len());
            for m in msgs {
                let next = msg_ids.len() as u32;
                let id = *msg_ids.entry(xxh3_128(&m)).or_insert(next);
                if id == next {
                    a.append_row(params![id as i32, utf8(&m)?]).map_err(de)?;
                    distinct += m.len() as u64;
                }
                nm.push(id);
            }
            node_msg.push(nm);
        }
    }
    {
        let mut a = conn.appender("nodes").map_err(de)?;
        for s in 0..ns {
            for (n, &p) in db.node_parents(s).iter().enumerate() {
                a.append_row(params![s as i32, n as i32, (p != NONE).then_some(p as i32), node_msg[s as usize][n] as i32]).map_err(de)?;
            }
        }
    }
    let mut meta_ids: HashMap<Vec<u8>, u32> = HashMap::new();
    {
        let mut a = conn.appender("requests_s").map_err(de)?;
        for ri in db.all_requests().map_err(e)? {
            let r = db.request(ri).map_err(e)?;
            let path = db.request_path(ri).map_err(e)?;
            let msgs: Vec<String> = path.iter().map(|&n| node_msg[r.session as usize][n as usize].to_string()).collect();
            let opt = |v: u32| (v != NONE).then_some(v as i32);
            let next = meta_ids.len() as u32;
            let meta = *meta_ids.entry(r.meta.to_vec()).or_insert(next);
            let notes: Vec<String> = r.notes.iter().flat_map(|n| [n.target, n.offset, n.text]).map(|x| x.to_string()).collect();
            let layout = r.layout.as_ref().map(|g| h2h::json_strings(g.iter().map(|x| &x[..])));
            a.append_row(params![
                &*r.id,
                r.session as i32,
                msgs.join(","),
                r.skeleton as i32,
                opt(r.system),
                opt(r.tools),
                r.time.contains('T').then_some(&*r.time),
                meta as i32,
                (!notes.is_empty()).then(|| notes.join(",")),
                layout.as_deref().map(utf8).transpose()?
            ])
            .map_err(de)?;
        }
    }
    {
        let mut a = conn.appender("metas").map_err(de)?;
        for (m, id) in &meta_ids {
            a.append_row(params![*id as i32, utf8(m)?]).map_err(de)?;
        }
    }
    // pieces once each; a tool list or system array that is exactly its elements joined by
    // commas is kept as element ids, each distinct element once (as for PostgreSQL)
    let mut pieces: Vec<(u32, u8, Vec<u8>)> = Vec::new();
    db.each_piece(|id, kind, b, _, _| pieces.push((id, kind, b.to_vec()))).map_err(e)?;
    let mut block_ids: HashMap<u128, u32> = HashMap::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    {
        let mut a = conn.appender("pieces_s").map_err(de)?;
        for (id, kind, b) in &pieces {
            if *kind == crate::record::PIECE_SKELETON {
                let sk = Skeleton::decode(b).map_err(e)?;
                let h: Vec<String> = sk.holes.iter().flat_map(|&(o, k)| [o, k as u32]).map(|x| x.to_string()).collect();
                a.append_row(params![*id as i32, *kind as i16, utf8(&sk.bytes)?, h.join(","), None::<String>]).map_err(de)?;
            } else if let Some(els) = h2h::array_elements(b).filter(|_| *kind == crate::record::PIECE_TOOLS || *kind == crate::record::PIECE_SYSTEM) {
                let ids: Vec<String> = els
                    .iter()
                    .map(|el| {
                        let next = blocks.len() as u32;
                        let i = *block_ids.entry(xxh3_128(el)).or_insert(next);
                        if i == next {
                            blocks.push(el.to_vec());
                        }
                        i.to_string()
                    })
                    .collect();
                a.append_row(params![*id as i32, *kind as i16, None::<String>, None::<String>, ids.join(",")]).map_err(de)?;
            } else {
                a.append_row(params![*id as i32, *kind as i16, utf8(b)?, None::<String>, None::<String>]).map_err(de)?;
            }
        }
    }
    {
        let mut a = conn.appender("blocks").map_err(de)?;
        for (i, b) in blocks.iter().enumerate() {
            a.append_row(params![i as i32, utf8(b)?]).map_err(de)?;
        }
    }
    // steps, with the fields the queries test, read from the store as the engine reads them
    let sd = db.step_dicts();
    let rules = h2h::has_rule_columns(db);
    let mut path_rows = 0usize;
    {
        let mut sa = conn.appender("steps").map_err(de)?;
        let mut pa = conn.appender("path_steps").map_err(de)?;
        for s in 0..ns {
            let ss = db.steps(s);
            let (turn_of, depth, paths) = h2h::turn_tree(db, s);
            let mut fp: Vec<[Option<String>; 4]> = Vec::with_capacity(ss.rows.len());
            for pos in 0..ss.rows.len() as u32 {
                let mut row: [Option<String>; 4] = Default::default();
                for (k, name) in ["file_path", "command", "program", "tests_failed"].into_iter().enumerate() {
                    if k >= 2 && !rules {
                        continue;
                    }
                    let v = plan::step_value(db, s, pos, &plan::StepCol::Field(name.into())).map_err(|e| e.to_string())?;
                    row[k] = h2h::text_of(v).map(|b| String::from_utf8_lossy(&b).into_owned());
                }
                fp.push(row);
            }
            for (pos, r) in ss.rows.iter().enumerate() {
                let f = &fp[pos];
                sa.append_row(params![
                    s as i32,
                    pos as i32,
                    turn_of[pos] as i32,
                    depth[turn_of[pos] as usize] as i32,
                    (r.prev != NONE).then_some(r.prev as i32),
                    r.call_node as i32,
                    r.result_node as i32,
                    utf8(sd.tools.get(r.tool))?,
                    r.failed,
                    r.out_bytes as i32,
                    r.time_ms,
                    f[0].as_deref(),
                    f[1].as_deref(),
                    f[2].as_deref(),
                    f[3].as_deref().and_then(|x| x.parse::<i32>().ok()),
                    node_msg[s as usize][r.call_node as usize] as i32,
                    node_msg[s as usize][r.result_node as usize] as i32
                ])
                .map_err(de)?;
            }
            let mut steps_of: HashMap<u32, Vec<u32>> = HashMap::new();
            for (pos, &t) in turn_of.iter().enumerate() {
                steps_of.entry(t).or_default().push(pos as u32);
            }
            for (pi, path) in paths.iter().enumerate() {
                for &t in path {
                    for &pos in &steps_of[&t] {
                        let r = ss.rows[pos as usize];
                        let f = &fp[pos as usize];
                        pa.append_row(params![s as i32, pi as i32, depth[t as usize] as i32, pos as i32, utf8(sd.tools.get(r.tool))?, r.failed, f[0].as_deref(), f[3].as_deref().and_then(|x| x.parse::<i32>().ok())]).map_err(de)?;
                        path_rows += 1;
                    }
                }
            }
        }
    }
    // text units: lower-cased text for substring search and its words for whole-word search
    {
        let mut a = conn.appender("texts_s").map_err(de)?;
        for s in 0..ns {
            let t = db.session_texts(s).map_err(e)?;
            for (u, x) in t.units.iter().enumerate() {
                let low = t.text_of(u).to_lowercase();
                let words: Vec<&str> = h2h::words(&low).into_iter().collect();
                a.append_row(params![s as i32, u as i32, x.node as i32, if x.user { "u" } else { "a" }, t.node_time[x.node as usize], low.as_str(), words.join(" ")]).map_err(de)?;
            }
        }
    }
    conn.execute_batch(FINISH).map_err(de)?;
    Ok(format!(
        "loaded {} sessions, {} distinct messages ({:.1} MB), {} requests, {} steps, {} path rows in {:.1}s",
        ns,
        msg_ids.len(),
        distinct as f64 / 1e6,
        db.request_count(),
        (0..ns).map(|s| db.steps(s).rows.len()).sum::<usize>(),
        path_rows,
        t0.elapsed().as_secs_f64()
    ))
}

/// Bytes DuckDB's tables take, whole and without the text and word tables (the search copy
/// and its index).
pub fn sizes(conn: &Connection, path: &Path) -> R<(u64, u64)> {
    conn.execute_batch("CHECKPOINT").map_err(de)?;
    let total = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    let bs: u64 = rows(conn, "SELECT block_size FROM pragma_database_size()")?.first().and_then(|r| r[0].as_ref()?.parse().ok()).unwrap_or(262_144);
    let mut search = 0u64;
    for t in ["texts", "words"] {
        search += rows(conn, &format!("SELECT count(DISTINCT block_id) FROM pragma_storage_info('{t}') WHERE persistent AND block_id >= 0"))?.first().and_then(|r| r[0].as_ref()?.parse().ok()).unwrap_or(0);
    }
    Ok((total, total.saturating_sub(search * bs)))
}

/// "a then b then c, nothing passing x strictly between a and c", in the form DuckDB runs
/// fastest of those tried: one row per turn, then the nearest a before and c after each b turn
/// with `last_value` / `first_value` ignoring nulls (about a quarter faster than
/// [`h2h`]'s filtered RANGE frames over steps, same answers).
fn three_step_duck(a: &str, b: &str, c: &str, x: &str) -> String {
    format!(
        "WITH t AS (SELECT session, path, depth, bool_or({a}) AS a, bool_or({b}) AS b, bool_or({c}) AS c, bool_or({x}) AS x FROM path_steps GROUP BY ALL),
         w AS (SELECT session, b, x,
           last_value(CASE WHEN a THEN depth END IGNORE NULLS) OVER pre AS pa, last_value(CASE WHEN x THEN depth END IGNORE NULLS) OVER pre AS px,
           first_value(CASE WHEN c THEN depth END IGNORE NULLS) OVER post AS nc, first_value(CASE WHEN x THEN depth END IGNORE NULLS) OVER post AS nx
           FROM t WINDOW pre AS (PARTITION BY session, path ORDER BY depth ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                         post AS (PARTITION BY session, path ORDER BY depth ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING))
         SELECT DISTINCT session FROM w WHERE b AND pa IS NOT NULL AND nc IS NOT NULL AND (px IS NULL OR px <= pa) AND NOT x AND (nx IS NULL OR nx >= nc)"
    )
}

/// DuckDB's SQL for a query: its own form where one was faster, else [`to_duck`] of the
/// PostgreSQL text.
pub fn duck_sql(q: &QuerySpec) -> String {
    let edits = "tool IN ('Edit','Write','MultiEdit','NotebookEdit')";
    let test = "coalesce(file_path, '') ILIKE '%test%'";
    match q.name {
        "Bash, then Edit, then Bash; no Read between" => three_step_duck("tool = 'Bash'", "tool = 'Edit'", "tool = 'Bash'", "tool = 'Read'"),
        "Bash fails, only test files edited, Bash passes" => three_step_duck("tool = 'Bash' AND failed", &format!("{edits} AND {test}"), "tool = 'Bash' AND NOT failed", &format!("{edits} AND NOT {test}")),
        "Tests fail, then an edit, then a test run" => three_step_duck("tests_failed > 0", edits, "tests_failed IS NOT NULL", "false"),
        // whole words from the word table instead of scanning every text's word list
        "User text with the word error, top sessions" => "SELECT se.sid, count(*) AS n FROM words w JOIN sessions se ON se.id = w.session WHERE w.role = 'u' AND w.word = 'error' GROUP BY se.sid ORDER BY n DESC, se.sid ASC LIMIT 10".into(),
        _ => to_duck(&q.sql),
    }
}

/// Queries the store's indexes cannot narrow, so it reads every value as DuckDB does: a
/// regular expression with no literal word in it, and a substring with no word of three
/// letters or more.
pub fn scan_queries() -> Vec<QuerySpec> {
    vec![
        QuerySpec {
            name: "Text with a run of 5+ digits (regex, no words)",
            engine: r#"messages | where text matches regex "[0-9]{5,}" | summarize n = count() by role | sort by role asc"#.into(),
            sql: "SELECT CASE role WHEN 'u' THEN 'user' ELSE 'assistant' END AS r, count(*) FROM texts WHERE regexp_matches(lbody, '[0-9]{5,}') GROUP BY r ORDER BY r".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Commands containing rm -rf (no 3-letter word)",
            engine: r#"where command contains "rm -rf" | summarize n = count(), s = dcount(session)"#.into(),
            sql: "SELECT count(*), count(DISTINCT se.sid) FROM steps s JOIN sessions se ON se.id = s.session WHERE s.command ILIKE '%rm -rf%'".into(),
            sessions: false,
        },
    ]
}

/// Every query of [`h2h::queries`] (and the rule-column ones when the store has the rules),
/// then [`scan_queries`], on the store at 1 and 8 threads and on DuckDB at `threads` (`ms`
/// per thread count).
pub fn run_queries(db: &Db, conn: &Connection, reps: usize, threads: &[usize]) -> R<Vec<(Outcome, Vec<f64>)>> {
    let mut out = Vec::new();
    let rule_qs = if h2h::has_rule_columns(db) { h2h::rule_queries() } else { Vec::new() };
    for q in h2h::queries().into_iter().chain(rule_qs).chain(scan_queries()) {
        let sql = duck_sql(&q);
        let QuerySpec { name, engine, sessions, .. } = q;
        let run = |t: usize| query::run(db, &engine, t).map_err(|e| format!("{name}: {}", e.show(&engine)));
        let (er, e1) = h2h::best(reps, || run(1))?;
        let (_, e8) = h2h::best(reps, || run(8))?;
        let mut dms = Vec::new();
        let mut dr = None;
        for &t in threads {
            conn.execute_batch(&format!("SET threads = {t}")).map_err(de)?;
            let (r, ms) = h2h::best(reps, || rows(conn, &sql).map_err(|e| format!("{name}: {e}\n{sql}")))?;
            dms.push(ms);
            dr = Some(r);
        }
        let dr = dr.unwrap_or_default();
        let (same, detail) = if sessions {
            let Kind::Matches(_) = er.table.kind else { return Err("pattern query did not return matches".into()) };
            let es: BTreeSet<u32> = er.table.rows.iter().map(|r| r.session).collect();
            let ds: BTreeSet<u32> = dr.iter().filter_map(|r| r[0].as_ref()?.parse().ok()).collect();
            (es == ds, format!("{} sessions", es.len()))
        } else {
            let erows: Vec<Vec<String>> = er.table.rows.iter().map(|r| r.vals.iter().map(h2h::engine_cell).collect()).collect();
            let drows: Vec<Vec<String>> = dr.iter().map(|r| r.iter().map(|c| c.clone().unwrap_or_default()).collect()).collect();
            let same = erows.len() == drows.len() && erows.iter().zip(&drows).all(|(a, b)| a.len() == b.len() && a.iter().zip(b).all(|(x, y)| h2h::cell_eq(x, y)));
            (same, format!("{} rows", erows.len()))
        };
        out.push((Outcome { name: name.into(), same, engine_ms: e1, engine_ms_par: e8, pg_ms: *dms.first().unwrap_or(&0.0), detail }, dms));
    }
    Ok(out)
}

/// A request rebuilt from DuckDB alone: its row, its messages in order, its pieces (array
/// pieces from their blocks), then the same assembly as the store's.
const Q_HEAD: &str = "SELECT skeleton, coalesce(system, -1), coalesce(tools, -1), notes, layout FROM requests WHERE id = ?";
const Q_PIECE: &str = "SELECT body, holes, elements IS NOT NULL FROM pieces WHERE id = ?";
const Q_BLOCKS: &str = "SELECT b.body FROM (SELECT unnest(elements) AS bid, generate_subscripts(elements, 1) AS k FROM pieces WHERE id = ?) u JOIN blocks b ON b.id = u.bid ORDER BY u.k";
const Q_REQ_IDS: &str = "SELECT msgs FROM requests WHERE id = ?";
const Q_REQ_MSGS: &str = "SELECT id, body FROM messages WHERE id IN (SELECT unnest(msgs) FROM requests WHERE id = ?)";
const Q_STEP_IDS: &str = "SELECT call_msg, result_msg FROM steps WHERE session = ? AND step >= ? AND step < ? ORDER BY step";
const Q_STEP_MSGS: &str = "SELECT id, body FROM messages WHERE id IN (SELECT unnest([call_msg, result_msg]) FROM steps WHERE session = ? AND step >= ? AND step < ?)";
// (one join was faster here than ids then IN: a session's messages are many)
const Q_SESSION: &str = "SELECT string_agg(m.body, ',' ORDER BY n.node) FROM nodes n JOIN messages m ON m.id = n.msg WHERE n.session = ?";
const READS: &[&str] = &[Q_HEAD, Q_PIECE, Q_BLOCKS, Q_REQ_IDS, Q_REQ_MSGS, Q_STEP_IDS, Q_STEP_MSGS, Q_SESSION];

/// Close the database and open it again with nothing in its buffer pool, with `stmts`
/// prepared (so a cold read pays for reading the data, not for planning).
fn reopen(conn: &mut Connection, path: &Path, stmts: &[&str]) -> R<()> {
    // the file allows one open instance: let go of this one first
    drop(std::mem::replace(conn, Connection::open_in_memory().map_err(de)?));
    *conn = Connection::open(path).map_err(de)?;
    for q in stmts {
        conn.prepare_cached(q).map_err(de)?;
    }
    Ok(())
}

/// DuckDB's buffer memory (bytes) right after [`reopen`], and after reading a request: shows
/// that a reopened database starts with nothing of the data in memory.
pub fn reopen_check(conn: &mut Connection, path: &Path, id: &str) -> R<(u64, u64)> {
    reopen(conn, path, READS)?;
    let mem = |c: &Connection| -> R<u64> { Ok(rows(c, "SELECT sum(memory_usage_bytes) FROM duckdb_memory()")?.first().and_then(|r| r[0].as_ref()?.parse().ok()).unwrap_or(0)) };
    let before = mem(conn)?;
    duck_rebuild(conn, id)?;
    Ok((before, mem(conn)?))
}

fn duck_rebuild(conn: &Connection, id: &str) -> R<Vec<u8>> {
    let head = rows_p(conn, Q_HEAD, &[&id])?;
    let h = head.first().ok_or("no such request")?;
    let ids: Vec<i64> = h[..3].iter().map(|c| c.as_deref().and_then(|x| x.parse().ok()).unwrap_or(-1)).collect();
    let msgs: Vec<(u64, String)> = request_messages(conn, id)?.into_iter().enumerate().map(|(k, m)| (k as u64, m)).collect();
    let notes: Vec<i64> = h[3].as_deref().map(|n| n.trim_matches(|c| c == '{' || c == '}').split(',').filter_map(|x| x.trim().parse().ok()).collect()).unwrap_or_default();
    let mut want: Vec<i64> = ids.iter().copied().filter(|&x| x >= 0).collect();
    want.extend(notes.chunks(3).filter_map(|c| c.get(2).copied()));
    want.sort_unstable();
    want.dedup();
    let mut pcs: HashMap<u32, (String, Option<String>)> = HashMap::new();
    for pid in want {
        let pid32 = pid as i32;
        let r = rows_p(conn, Q_PIECE, &[&pid32])?;
        let r = r.first().ok_or_else(|| format!("missing piece {pid}"))?;
        match &r[0] {
            Some(body) => {
                pcs.insert(pid as u32, (body.clone(), r[1].clone()));
            }
            None => {
                // an array piece: its blocks in order
                let parts = rows_p(conn, Q_BLOCKS, &[&pid32])?;
                let parts: Vec<String> = parts.into_iter().map(|x| x[0].clone().unwrap_or_default()).collect();
                pcs.insert(pid as u32, (format!("[{}]", parts.join(",")), None));
            }
        }
    }
    let notes_text = (!notes.is_empty()).then(|| format!("{{{}}}", notes.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")));
    h2h::assemble(msgs, &pcs, (format!("{},{},{}", ids[0], ids[1], ids[2]), notes_text, h[4].clone()))
}

/// Messages by id, fetched with one `IN` lookup (DuckDB's fastest of the forms tried), put in
/// the order `ids` gives.
fn messages_in_order(conn: &Connection, sql: &str, p: &[&dyn duckdb::ToSql], ids: &[i64]) -> R<Vec<String>> {
    let mut by: HashMap<i64, String> = HashMap::new();
    for r in rows_p(conn, sql, p)? {
        by.insert(r[0].as_deref().and_then(|x| x.parse().ok()).unwrap_or(-1), r[1].clone().unwrap_or_default());
    }
    ids.iter().map(|i| by.get(i).cloned().ok_or_else(|| format!("missing message {i}"))).collect()
}

fn list_ints(s: Option<&str>) -> Vec<i64> {
    s.map(|n| n.trim_matches(|c| c == '{' || c == '}').split(',').filter_map(|x| x.trim().parse().ok()).collect()).unwrap_or_default()
}

/// A request's messages in order.
fn request_messages(conn: &Connection, id: &str) -> R<Vec<String>> {
    let ids = list_ints(rows_p(conn, Q_REQ_IDS, &[&id])?.first().and_then(|r| r[0].as_deref()));
    messages_in_order(conn, Q_REQ_MSGS, &[&id], &ids)
}

/// The reads of [`h2h::run_reads`]: `n` random items each, compared byte for byte. With
/// `cold`, both sides start every item with nothing of the data in their own memory: the
/// store's caches are emptied (and its decoded dictionaries), and DuckDB is closed and opened
/// again, its statements prepared, before the timing. The operating system's file cache stays
/// warm for both.
pub fn run_reads(db: &Db, conn: &mut Connection, path: &Path, n: usize, seed: u64, cold: bool) -> R<Vec<ReadOutcome>> {
    let mut r = Rng::new(seed);
    let prep = |conn: &mut Connection| -> R<()> {
        if cold {
            db.clear_caches();
            reopen(conn, path, READS)?;
        }
        Ok(())
    };
    let e = |x: crate::codec::Error| x.to_string();
    let all = db.all_requests().map_err(e)?;
    let mut res = Vec::new();
    let sha = crate::db::sha256;
    {
        let (mut te, mut td, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n {
            let ri = all[r.below(all.len() as u64) as usize];
            let rec = db.request(ri).map_err(e)?;
            prep(conn)?;
            let a = Instant::now();
            let rp = db.request_path(ri).map_err(e)?;
            let eb = db.read_messages(rec.session, &rp, &mut ReadStats::default()).map_err(e)?.join(&b","[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let id: &str = &rec.id;
            let a = Instant::now();
            let db_ = request_messages(conn, id)?.join(",");
            td.push(a.elapsed().as_secs_f64() * 1e3);
            same += (sha(&eb) == sha(db_.as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "a request's messages in order".into(), same, total: n, engine: h2h::quantiles(&te), pg: Some(h2h::quantiles(&td)) });
    }
    {
        let long: Vec<u32> = (0..db.session_count() as u32).filter(|&s| db.steps(s).rows.len() >= 20).collect();
        let (mut te, mut td, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n.min(if long.is_empty() { 0 } else { n }) {
            let s = *r.pick(&long);
            let ss = db.steps(s);
            let k = r.below(ss.rows.len() as u64 - 19) as usize;
            prep(conn)?;
            let a = Instant::now();
            let ids: Vec<u32> = ss.rows[k..k + 20].iter().flat_map(|row| [row.call_node, row.result_node]).collect();
            let eb = db.read_messages(s, &ids, &mut ReadStats::default()).map_err(e)?.join(&b"|"[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let (s32, k0, k1) = (s as i32, k as i32, k as i32 + 20);
            let a = Instant::now();
            // the steps' message ids, then the messages by id
            let ids: Vec<i64> = rows_p(conn, Q_STEP_IDS, &[&s32, &k0, &k1])?
                .into_iter()
                .flat_map(|r| r.into_iter().map(|c| c.and_then(|x| x.parse().ok()).unwrap_or(-1)))
                .collect();
            let got = messages_in_order(conn, Q_STEP_MSGS, &[&s32, &k0, &k1], &ids)?;
            td.push(a.elapsed().as_secs_f64() * 1e3);
            same += (sha(&eb) == sha(got.join("|").as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "20 steps with their messages".into(), same, total: te.len(), engine: h2h::quantiles(&te), pg: Some(h2h::quantiles(&td)) });
    }
    {
        let (mut te, mut td, mut same) = (Vec::new(), Vec::new(), 0);
        let m = n.min(db.session_count());
        for _ in 0..m {
            let s = r.below(db.session_count() as u64) as u32;
            prep(conn)?;
            let a = Instant::now();
            let ids: Vec<u32> = (0..db.session_len(s)).collect();
            let eb = db.read_messages(s, &ids, &mut ReadStats::default()).map_err(e)?.join(&b","[..]);
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let s32 = s as i32;
            let a = Instant::now();
            let got = rows_p(conn, Q_SESSION, &[&s32])?
                .into_iter()
                .next()
                .and_then(|x| x.into_iter().next().flatten())
                .unwrap_or_default();
            td.push(a.elapsed().as_secs_f64() * 1e3);
            same += (sha(&eb) == sha(got.as_bytes())) as usize;
        }
        res.push(ReadOutcome { name: "a whole session's messages".into(), same, total: m, engine: h2h::quantiles(&te), pg: Some(h2h::quantiles(&td)) });
    }
    {
        let (mut te, mut td, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n {
            let ri = all[r.below(all.len() as u64) as usize];
            let id = db.request(ri).map_err(e)?.id;
            prep(conn)?;
            let a = Instant::now();
            let eb = db.rebuild(ri).map_err(e)?;
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let a = Instant::now();
            let got = duck_rebuild(conn, &id)?;
            td.push(a.elapsed().as_secs_f64() * 1e3);
            same += (eb == got) as usize;
        }
        res.push(ReadOutcome { name: "a whole request rebuilt".into(), same, total: n, engine: h2h::quantiles(&te), pg: Some(h2h::quantiles(&td)) });
    }
    Ok(res)
}

/// What [`raw_bodies`] measured.
pub struct RawOutcome {
    pub bytes_in: u64,
    pub size: u64,
    pub load_s: f64,
    /// A whole request by id, warm then cold (see [`run_reads`]): (median, p95, p99) ms for
    /// the store's rebuild and DuckDB's lookup, and how many were the same bytes.
    pub reads: ReadOutcome,
    pub cold: ReadOutcome,
}

/// DuckDB holding every request body whole, one row each, keyed by request id: what a system
/// storing calls as they arrive keeps (CostKey's `request_body`). `compression` forces one
/// of DuckDB's (`zstd`, `fsst`, ...) instead of letting it choose. Then `n` random requests
/// read back by id from both sides, compared byte for byte.
///
/// With `app_zstd`, the application compresses each body with zstd at that level and DuckDB
/// stores the bytes (a BLOB); a read decompresses the one body it fetched.
pub fn raw_bodies(db: &Db, path: &Path, compression: Option<&str>, app_zstd: Option<i32>, n: usize, seed: u64) -> R<RawOutcome> {
    let _ = std::fs::remove_file(path);
    let conn = Connection::open(path).map_err(de)?;
    if let Some(c) = compression {
        conn.execute_batch(&format!("SET force_compression = '{c}'")).map_err(de)?;
    }
    let ty = if app_zstd.is_some() { "BLOB" } else { "VARCHAR" };
    conn.execute_batch(&format!("CREATE TABLE calls (id VARCHAR PRIMARY KEY, session VARCHAR, time VARCHAR, body {ty})")).map_err(de)?;
    let t = Instant::now();
    let mut bytes_in = 0u64;
    let all = db.all_requests().map_err(|e| e.to_string())?;
    {
        let mut a = conn.appender("calls").map_err(de)?;
        for &ri in &all {
            let r = db.request(ri).map_err(|e| e.to_string())?;
            let b = db.rebuild(ri).map_err(|e| e.to_string())?;
            bytes_in += b.len() as u64;
            match app_zstd {
                Some(level) => {
                    let c = zstd::bulk::compress(&b, level).map_err(|e| e.to_string())?;
                    a.append_row(params![&*r.id, db.session_key(r.session).0, &*r.time, c]).map_err(de)?
                }
                None => a.append_row(params![&*r.id, db.session_key(r.session).0, &*r.time, utf8(&b)?]).map_err(de)?,
            }
        }
    }
    conn.execute_batch("CHECKPOINT").map_err(de)?;
    let load_s = t.elapsed().as_secs_f64();
    let size = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    let mut conn = conn;
    let q = "SELECT body FROM calls WHERE id = ?";
    let mut out = Vec::new();
    for cold in [false, true] {
    let mut r = Rng::new(seed);
    let (mut te, mut td, mut same) = (Vec::new(), Vec::new(), 0);
    for _ in 0..n {
        let ri = all[r.below(all.len() as u64) as usize];
        let id = db.request(ri).map_err(|e| e.to_string())?.id;
        if cold {
            db.clear_caches();
            reopen(&mut conn, path, &[q])?;
        }
        let a = Instant::now();
        let eb = db.rebuild(ri).map_err(|e| e.to_string())?;
        te.push(a.elapsed().as_secs_f64() * 1e3);
        let idr: &str = &id;
        let a = Instant::now();
        let got: Vec<u8> = match app_zstd {
            Some(_) => {
                let mut st = conn.prepare_cached(q).map_err(de)?;
                let c: Vec<u8> = st.query_row([idr], |r| r.get(0)).map_err(de)?;
                zstd::stream::decode_all(&c[..]).map_err(|e| e.to_string())?
            }
            None => rows_p(&conn, q, &[&idr])?.into_iter().next().and_then(|x| x.into_iter().next().flatten()).unwrap_or_default().into_bytes(),
        };
        td.push(a.elapsed().as_secs_f64() * 1e3);
        same += (eb == got) as usize;
    }
    out.push(ReadOutcome { name: if cold { "a whole request by id, cold" } else { "a whole request by id" }.into(), same, total: n, engine: h2h::quantiles(&te), pg: Some(h2h::quantiles(&td)) });
    }
    drop(conn);
    let _ = std::fs::remove_file(path);
    let cold = out.pop().unwrap();
    let reads = out.pop().unwrap();
    Ok(RawOutcome { bytes_in, size, load_s, reads, cold })
}

#[cfg(test)]
mod tests {
    use super::to_duck;

    #[test]
    fn dialect() {
        assert_eq!(to_duck("SELECT (time_ms / 604800000) * 604800000 AS w"), "SELECT (time_ms // 604800000) * 604800000 AS w");
        assert_eq!(to_duck("x::float8 / count(*)"), "x::float8 / count(*)");
        assert_eq!(to_duck("WHERE t.terms @@ 'error'::tsquery"), "WHERE list_contains(t.terms, 'error')");
        assert_eq!(to_duck("WHERE t.lbody ~ 'root cause.*fix'"), "WHERE regexp_matches(t.lbody, '(?s)root cause.*fix')");
        assert_eq!(to_duck(r"s.tool ILIKE 'mcp\_\_x%' GROUP"), r"s.tool ILIKE 'mcp\_\_x%' ESCAPE '\' GROUP");
        assert_eq!(to_duck("command ILIKE '%git commit%'"), "command ILIKE '%git commit%'");
    }
}
