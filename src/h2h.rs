//! Head-to-head against PostgreSQL on the same data.
//!
//! PostgreSQL gets the store's contents with the usual tricks: every distinct message stored
//! once (by hash), requests as arrays of message ids, a step table with indexes and the
//! two input fields the queries use as columns, and for patterns a materialized table of
//! root-to-leaf turn paths so window functions are exact on forked sessions. It also gets
//! what rebuilding every request byte for byte needs, as the store keeps it: skeletons,
//! system prompts, tool lists and cache-marker texts once each (tool lists and system arrays
//! split into elements stored once each), and per request its time, metadata, cache-marker
//! positions and message separators. Pattern queries are compared on the set of sessions
//! with a match (what SQL answers naturally), aggregations on their rows, reads on the bytes
//! returned, and whole requests rebuilt from either side are compared byte for byte.

use crate::db::{sha256, Db, ReadStats};
use crate::pg::{Pg, PgError};
use crate::query::{self, exec::Kind, plan, value::Value};
use crate::record::{NONE, TARGET_MSG0, TARGET_SYSTEM, TARGET_TOOLS};
use crate::split::{self, HoleKind, Skeleton};
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

/// The elements of a JSON array that is exactly `[` + elements joined by `,` + `]`.
fn array_elements(b: &[u8]) -> Option<Vec<&[u8]>> {
    let (spans, end) = crate::json::elements(b, &(0..b.len())).ok()?;
    if end + 1 != b.len() {
        return None;
    }
    let els: Vec<&[u8]> = spans.into_iter().map(|s| &b[s]).collect();
    let mut joined = vec![b'['];
    for (i, e) in els.iter().enumerate() {
        if i > 0 {
            joined.push(b',');
        }
        joined.extend_from_slice(e);
    }
    joined.push(b']');
    (joined == b && !els.is_empty()).then_some(els)
}

/// Strings as a JSON array (for the rare request whose messages are not separated by bare
/// commas).
fn json_strings<'a>(xs: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = vec![b'['];
    for (i, x) in xs.enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'"');
        for &c in x {
            match c {
                b'"' => out.extend_from_slice(b"\\\""),
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'\n' => out.extend_from_slice(b"\\n"),
                b'\r' => out.extend_from_slice(b"\\r"),
                b'\t' => out.extend_from_slice(b"\\t"),
                c => out.push(c),
            }
        }
        out.push(b'"');
    }
    out.push(b']');
    out
}

fn parse_json_strings(s: &str) -> Option<Vec<Vec<u8>>> {
    let b = s.as_bytes();
    let (spans, _) = crate::json::elements(b, &(0..b.len())).ok()?;
    spans.iter().map(|sp| (b[sp.start] == b'"').then(|| crate::query::value::json_unescape(crate::json::inner(b, sp)).into_bytes())).collect()
}

fn ints(s: &str) -> Vec<u32> {
    s.trim_matches(|c| c == '{' || c == '}').split(',').filter(|x| !x.is_empty()).filter_map(|x| x.parse().ok()).collect()
}

/// Insert cache-marker texts at their offsets.
fn with_notes(body: &[u8], notes: &[(u32, Vec<u8>)], out: &mut Vec<u8>) -> R<()> {
    let mut pos = 0usize;
    for (o, t) in notes {
        let o = *o as usize;
        if o < pos || o > body.len() {
            return Err("cache note out of range".into());
        }
        out.extend_from_slice(&body[pos..o]);
        out.extend_from_slice(t);
        pos = o;
    }
    out.extend_from_slice(&body[pos..]);
    Ok(())
}

/// Rebuild request `id` from PostgreSQL alone: one query for its messages, pieces, cache
/// markers and separators, then the same assembly as the store's.
pub fn pg_rebuild(pg: &mut Pg, id: &str) -> R<Vec<u8>> {
    let sql = format!(
        "WITH r AS (SELECT * FROM requests WHERE id = {}),
         p AS (SELECT p.id, p.kind, p.holes,
                      coalesce(p.body, '[' || (SELECT string_agg(b.body, ',' ORDER BY u.k) FROM unnest(p.elements) WITH ORDINALITY u(bid, k) JOIN blocks b ON b.id = u.bid) || ']') AS body
               FROM pieces p, r
               WHERE p.id IN (r.skeleton, r.system, r.tools)
                  OR p.id = ANY (SELECT r.notes[i] FROM generate_subscripts(r.notes, 1) i WHERE i % 3 = 0))
         SELECT 'm', u.k::text, m.body, NULL FROM r, unnest(r.msgs) WITH ORDINALITY u(mid, k) JOIN messages m ON m.id = u.mid
         UNION ALL SELECT 'p', p.id::text, p.body, p.holes::text FROM p
         UNION ALL SELECT 'r', concat_ws(',', r.skeleton, coalesce(r.system, -1), coalesce(r.tools, -1)), r.notes::text, r.layout FROM r",
        sql_str(id)
    );
    let rows = pg.query(&sql).map_err(pe)?;
    let (mut msgs, mut pcs, mut head) = (Vec::new(), HashMap::new(), None);
    for row in rows {
        let col = |i: usize| row[i].clone();
        match col(0).as_deref() {
            Some("m") => msgs.push((col(1).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0), col(2).unwrap_or_default())),
            Some("p") => {
                pcs.insert(col(1).and_then(|x| x.parse::<u32>().ok()).unwrap_or(u32::MAX), (col(2).unwrap_or_default(), col(3)));
            }
            Some("r") => head = Some((col(1).unwrap_or_default(), col(2), col(3))),
            _ => {}
        }
    }
    let (ids, notes, layout) = head.ok_or("no such request")?;
    msgs.sort_by_key(|m| m.0);
    let ids: Vec<i64> = ids.split(',').filter_map(|x| x.parse().ok()).collect();
    let piece = |i: i64| pcs.get(&(i as u32)).ok_or_else(|| format!("missing piece {i}"));
    let (skel_body, holes) = piece(ids[0])?;
    let h = ints(holes.as_deref().unwrap_or(""));
    let mut sk_holes = Vec::with_capacity(h.len() / 2);
    for p in h.chunks(2) {
        sk_holes.push((p[0], HoleKind::from_u8(p[1] as u8).map_err(|e| e.to_string())?));
    }
    let skel = Skeleton { bytes: skel_body.as_bytes().to_vec(), holes: sk_holes };
    let nt = ints(notes.as_deref().unwrap_or(""));
    let mut by_target: HashMap<u32, Vec<(u32, Vec<u8>)>> = HashMap::new();
    for t in nt.chunks(3) {
        by_target.entry(t[0]).or_default().push((t[1], piece(t[2] as i64)?.0.as_bytes().to_vec()));
    }
    let layout = match layout {
        Some(l) => Some(parse_json_strings(&l).ok_or("bad layout")?),
        None => None,
    };
    let empty = Vec::new();
    let mut out = Vec::new();
    let mut err = None;
    split::assemble(&skel, &mut out, |kind, out| {
        let r = (|| -> R<()> {
            match kind {
                HoleKind::Messages => {
                    for (i, (_, m)) in msgs.iter().enumerate() {
                        match &layout {
                            Some(g) => out.extend_from_slice(&g[i]),
                            None if i > 0 => out.push(b','),
                            None => {}
                        }
                        with_notes(m.as_bytes(), by_target.get(&(TARGET_MSG0 + i as u32)).unwrap_or(&empty), out)?;
                    }
                    if let Some(g) = &layout {
                        out.extend_from_slice(&g[msgs.len()]);
                    }
                }
                HoleKind::System => with_notes(piece(ids[1])?.0.as_bytes(), by_target.get(&TARGET_SYSTEM).unwrap_or(&empty), out)?,
                HoleKind::Tools => with_notes(piece(ids[2])?.0.as_bytes(), by_target.get(&TARGET_TOOLS).unwrap_or(&empty), out)?,
            }
            Ok(())
        })();
        if let Err(e) = r {
            err = Some(e);
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// The whole words of lower-cased text as a `tsvector` literal: each maximal run of
/// letters and digits, which is what the engine's `has` matches (runs over 255 bytes are
/// left out).
fn terms_vector(low: &str) -> String {
    let mut words: BTreeSet<&str> = BTreeSet::new();
    let mut start = None;
    for (i, ch) in low.char_indices() {
        match (ch.is_alphanumeric(), start) {
            (true, None) => start = Some(i),
            (false, Some(a)) => {
                if i - a <= 255 {
                    words.insert(&low[a..i]);
                }
                start = None;
            }
            _ => {}
        }
    }
    if let Some(a) = start
        && low.len() - a <= 255
    {
        words.insert(&low[a..]);
    }
    let mut out = String::new();
    for w in words {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push('\'');
        out.push_str(w);
        out.push('\'');
    }
    out
}

/// Load the store into PostgreSQL (tables dropped first).
pub fn load(db: &Db, pg: &mut Pg) -> R<String> {
    let t0 = Instant::now();
    pg.exec(
        "DROP TABLE IF EXISTS sessions, messages, nodes, requests, steps, path_steps, pieces, blocks, metas, texts CASCADE;
         CREATE EXTENSION IF NOT EXISTS pg_trgm;
         BEGIN;
         CREATE TABLE texts (session int, unit int, node int, role text, time_ms bigint, lbody text, terms tsvector);
         CREATE TABLE sessions (id int PRIMARY KEY, sid text NOT NULL, agent text NOT NULL);
         CREATE TABLE messages (id int, body text NOT NULL);
         CREATE TABLE nodes (session int, node int, parent int, msg int);
         CREATE TABLE requests (id text, session int, msgs int[], skeleton int, system int, tools int,
                                time timestamptz, meta int, notes int[], layout text);
         CREATE TABLE metas (id int, body text);
         CREATE TABLE pieces (id int, kind smallint, body text, holes int[], elements int[]);
         CREATE TABLE blocks (id int, body text);
         CREATE TABLE steps (session int, step int, turn int, depth int, prev int, call_node int, result_node int,
                             tool text, failed bool, out_bytes int, time_ms bigint, file_path text, command text,
                             program text, tests_failed int);
         CREATE TABLE path_steps (session int, path int, depth int, step int, tool text, failed bool, file_path text, tests_failed int);",
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
    // per-request metadata (query source, client version) takes few values: once each
    let mut meta_ids: HashMap<Vec<u8>, u32> = HashMap::new();
    pg.copy_in("COPY requests FROM STDIN", |c| {
        let opt = |v: u32| if v == NONE { None } else { Some(v.to_string()) };
        for ri in db.all_requests().map_err(|e| PgError(e.to_string()))? {
            let r = db.request(ri).map_err(|e| PgError(e.to_string()))?;
            let path = db.request_path(ri).map_err(|e| PgError(e.to_string()))?;
            let arr: Vec<String> = path.iter().map(|&n| node_msg[r.session as usize][n as usize].to_string()).collect();
            c.field(Some(r.id.as_bytes()));
            c.int(r.session as i64);
            c.field(Some(format!("{{{}}}", arr.join(",")).as_bytes()));
            c.int(r.skeleton as i64);
            c.field(opt(r.system).as_deref().map(str::as_bytes));
            c.field(opt(r.tools).as_deref().map(str::as_bytes));
            c.field(r.time.contains('T').then_some(r.time.as_bytes()));
            let next = meta_ids.len() as u32;
            c.int(*meta_ids.entry(r.meta.to_vec()).or_insert(next) as i64);
            let notes: Vec<String> = r.notes.iter().flat_map(|n| [n.target, n.offset, n.text]).map(|x| x.to_string()).collect();
            c.field(if notes.is_empty() { None } else { Some(format!("{{{}}}", notes.join(","))) }.as_deref().map(str::as_bytes));
            c.field(r.layout.as_ref().map(|g| json_strings(g.iter().map(|x| &x[..]))).as_deref());
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    pg.copy_in("COPY metas FROM STDIN", |c| {
        for (m, id) in &meta_ids {
            c.int(*id as i64);
            c.field(Some(m));
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    // pieces once each; a tool list or system array that is exactly its elements joined by
    // commas is kept as element ids, each distinct element once
    let mut pieces: Vec<(u32, u8, Vec<u8>)> = Vec::new();
    db.each_piece(|id, kind, b, _, _| pieces.push((id, kind, b.to_vec()))).map_err(|e| e.to_string())?;
    let mut block_ids: HashMap<u128, u32> = HashMap::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    pg.copy_in("COPY pieces FROM STDIN", |c| {
        for (id, kind, b) in &pieces {
            c.int(*id as i64);
            c.int(*kind as i64);
            if *kind == crate::record::PIECE_SKELETON {
                let sk = Skeleton::decode(b).map_err(|e| PgError(e.to_string()))?;
                c.field(Some(&sk.bytes));
                let h: Vec<String> = sk.holes.iter().flat_map(|&(o, k)| [o, k as u32]).map(|x| x.to_string()).collect();
                c.field(Some(format!("{{{}}}", h.join(",")).as_bytes()));
                c.field(None);
            } else if let Some(els) = array_elements(b).filter(|_| *kind == crate::record::PIECE_TOOLS || *kind == crate::record::PIECE_SYSTEM) {
                let ids: Vec<String> = els
                    .iter()
                    .map(|e| {
                        let next = blocks.len() as u32;
                        let id = *block_ids.entry(xxh3_128(e)).or_insert(next);
                        if id == next {
                            blocks.push(e.to_vec());
                        }
                        id.to_string()
                    })
                    .collect();
                c.field(None);
                c.field(None);
                c.field(Some(format!("{{{}}}", ids.join(",")).as_bytes()));
            } else {
                c.field(Some(b));
                c.field(None);
                c.field(None);
            }
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    pg.copy_in("COPY blocks FROM STDIN", |c| {
        for (i, b) in blocks.iter().enumerate() {
            c.int(i as i64);
            c.field(Some(b));
            c.end_row()?;
        }
        Ok(())
    })
    .map_err(pe)?;
    let sd = db.step_dicts();
    let mut fields_err = None;
    let mut steps_cache: Vec<(Vec<u32>, Vec<u32>, Vec<Vec<u32>>)> = Vec::with_capacity(ns as usize);
    // (file_path, command, program, tests_failed); the last two are the columns of the
    // rules of those names, taken from the store so both sides hold the same values
    let rules = has_rule_columns(db);
    let mut fp: Vec<Vec<[Option<Vec<u8>>; 4]>> = Vec::with_capacity(ns as usize);
    for s in 0..ns {
        steps_cache.push(turn_tree(db, s));
        let mut v = Vec::new();
        for pos in 0..db.steps(s).rows.len() as u32 {
            let mut row: [Option<Vec<u8>>; 4] = Default::default();
            for (k, name) in ["file_path", "command", "program", "tests_failed"].into_iter().enumerate() {
                if k >= 2 && !rules {
                    continue;
                }
                match plan::step_value(db, s, pos, &plan::StepCol::Field(name.into())) {
                    Ok(x) => row[k] = text_of(x),
                    Err(e) => fields_err = Some(e.to_string()),
                }
            }
            v.push(row);
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
                for x in &fp[s as usize][pos] {
                    c.field(x.as_deref());
                }
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
                        c.field(fp[s as usize][pos as usize][0].as_deref());
                        c.field(fp[s as usize][pos as usize][3].as_deref());
                        c.end_row()?;
                        path_rows += 1;
                    }
                }
            }
        }
        Ok(())
    })
    .map_err(pe)?;
    // text units: lower-cased text for substring search and its words for whole-word search
    let mut err = None;
    pg.copy_in("COPY texts FROM STDIN", |c| {
        for s in 0..ns {
            let t = match db.session_texts(s) {
                Ok(t) => t,
                Err(e) => {
                    err = Some(e.to_string());
                    return Ok(());
                }
            };
            for (u, x) in t.units.iter().enumerate() {
                let low = t.text_of(u).to_lowercase();
                c.int(s as i64);
                c.int(u as i64);
                c.int(x.node as i64);
                c.field(Some(if x.user { b"u" } else { b"a" }));
                c.int(t.node_time[x.node as usize]);
                c.field(Some(low.as_bytes()));
                c.field(Some(terms_vector(&low).as_bytes()));
                c.end_row()?;
            }
        }
        Ok(())
    })
    .map_err(pe)?;
    if let Some(e) = err {
        return Err(e);
    }
    pg.exec(
        "ALTER TABLE texts ADD PRIMARY KEY (session, unit);
         CREATE INDEX texts_trgm ON texts USING gin (lbody gin_trgm_ops);
         CREATE INDEX texts_terms ON texts USING gin (terms);
         CREATE INDEX steps_command_trgm ON steps USING gin (command gin_trgm_ops);
         ALTER TABLE messages ADD PRIMARY KEY (id);
         ALTER TABLE nodes ADD PRIMARY KEY (session, node);
         ALTER TABLE requests ADD PRIMARY KEY (id);
         ALTER TABLE pieces ADD PRIMARY KEY (id);
         ALTER TABLE blocks ADD PRIMARY KEY (id);
         ALTER TABLE metas ADD PRIMARY KEY (id);
         ALTER TABLE steps ADD PRIMARY KEY (session, step);
         CREATE INDEX steps_tool ON steps (tool);
         CREATE INDEX path_steps_order ON path_steps (session, path, depth);
         COMMIT;",
    )
    .map_err(pe)?;
    if rules {
        pg.exec("CREATE INDEX steps_program ON steps (program); CREATE INDEX steps_tests_failed ON steps (tests_failed)").map_err(pe)?;
    }
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

/// Whether the store has the rules the rule-column queries read ([`rule_queries`]).
pub fn has_rule_columns(db: &Db) -> bool {
    db.rules().position(b"program").is_some() && db.rules().position(b"tests_failed").is_some()
}

/// The rules [`rule_queries`] are about: the program a shell command runs (past a leading
/// `cd DIR &&` and variable assignments) and the number of failed tests a test run reports.
pub const RULES: &[&str] = &[
    r#"program on tool == "Bash" = extract(command, @"^\s*(?:cd\s+(?:""[^""]*""|'[^']*'|\S+)\s*(?:&&|;)\s*)?(?:[A-Za-z_][A-Za-z0-9_]*=\S*\s+)*(\S+)")"#,
    r#"tests_failed on program in ("cargo", "pytest", "npm", "npx", "pnpm", "yarn", "go", "make", "python", "python3") = toint(extract(output, @"(\d+) failed"))"#,
];

/// Queries over rule columns, for a store that has [`RULES`].
pub fn rule_queries() -> Vec<QuerySpec> {
    vec![
        QuerySpec {
            name: "Shell steps per program, top 10",
            engine: "where isnotnull(program) | summarize n = count() by program | sort by n desc, program asc | take 10".into(),
            sql: "SELECT program, count(*) AS n FROM steps WHERE program IS NOT NULL GROUP BY program ORDER BY n DESC, program ASC LIMIT 10".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Failure rate per program (50+ steps)",
            engine: "where isnotnull(program) | summarize n = count(), f = countif(failed) by program | where n >= 50 | extend rate = f * 1.0 / n | project program, n, f, rate | sort by program asc".into(),
            sql: "SELECT program, count(*), count(*) FILTER (WHERE failed), (count(*) FILTER (WHERE failed))::float8 / count(*) FROM steps WHERE program IS NOT NULL GROUP BY program HAVING count(*) >= 50 ORDER BY program".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Test runs with failed tests, per week",
            engine: "where tests_failed > 0 | summarize n = count(), s = dcount(session) by w = bin(time, 7d) | sort by w asc".into(),
            sql: "SELECT (s.time_ms / 604800000) * 604800000 AS w, count(*), count(DISTINCT se.sid) FROM steps s JOIN sessions se ON se.id = s.session WHERE s.tests_failed > 0 GROUP BY w ORDER BY w".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Tests fail, then an edit, then a test run",
            engine: format!("match a: tests_failed > 0 then b: tool in {EDITS} then c: isnotnull(tests_failed)"),
            sql: three_step_sql("tests_failed > 0", &format!("tool IN {EDITS_SQL}"), "tests_failed IS NOT NULL", "false"),
            sessions: true,
        },
    ]
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
            name: "User text with the word error, top sessions",
            engine: r#"messages | where role == "user" and text has "error" | summarize n = count() by session | sort by n desc, session asc | take 10"#.into(),
            sql: "SELECT se.sid, count(*) AS n FROM texts t JOIN sessions se ON se.id = t.session WHERE t.role = 'u' AND t.terms @@ 'error'::tsquery GROUP BY se.sid ORDER BY n DESC, se.sid ASC LIMIT 10".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Assistant text containing root cause",
            engine: r#"messages | where role == "assistant" and text contains "root cause" | summarize n = count(), s = dcount(session)"#.into(),
            sql: "SELECT count(*), count(DISTINCT se.sid) FROM texts t JOIN sessions se ON se.id = t.session WHERE t.role = 'a' AND t.lbody LIKE '%root cause%'".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Text saying still failing or try again",
            engine: r#"messages | where text contains "still failing" or text contains "try again" | summarize n = count() by role | sort by role asc"#.into(),
            sql: "SELECT CASE role WHEN 'u' THEN 'user' ELSE 'assistant' END AS r, count(*) FROM texts WHERE lbody LIKE '%still failing%' OR lbody LIKE '%try again%' GROUP BY r ORDER BY r".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Text matching root cause ... fix (regex)",
            engine: r#"messages | where text matches regex "(?is)root cause.*fix" | summarize n = count(), s = dcount(session)"#.into(),
            sql: "SELECT count(*), count(DISTINCT se.sid) FROM texts t JOIN sessions se ON se.id = t.session WHERE t.lbody ~ 'root cause.*fix'".into(),
            sessions: false,
        },
        QuerySpec {
            name: "Commands matching git push or pull ... origin (regex)",
            engine: r#"where command matches regex "(?s)git (push|pull) .*origin" | summarize n = count(), s = dcount(session)"#.into(),
            sql: "SELECT count(*), count(DISTINCT se.sid) FROM steps s JOIN sessions se ON se.id = s.session WHERE s.command ~ 'git (push|pull) .*origin'".into(),
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
    let rule_qs = if has_rule_columns(db) { rule_queries() } else { Vec::new() };
    for q in queries().into_iter().chain(rule_qs) {
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
    // 4. a whole request rebuilt byte for byte, as it was sent (less what is never stored)
    {
        let (mut te, mut tp, mut same) = (Vec::new(), Vec::new(), 0);
        for _ in 0..n {
            let ri = all[r.below(all.len() as u64) as usize];
            let id = db.request(ri).map_err(e)?.id;
            let a = Instant::now();
            let eb = db.rebuild(ri).map_err(e)?;
            te.push(a.elapsed().as_secs_f64() * 1e3);
            let a = Instant::now();
            let pb = pg_rebuild(pg, &id)?;
            tp.push(a.elapsed().as_secs_f64() * 1e3);
            same += (eb == pb) as usize;
        }
        res.push(ReadOutcome { name: "a whole request rebuilt".into(), same, total: n, engine: (quantile(&mut te.clone(), 0.5), quantile(&mut te, 0.95)), pg: (quantile(&mut tp.clone(), 0.5), quantile(&mut tp, 0.95)) });
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
