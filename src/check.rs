//! Checks of derived data against slow, simple references.

use crate::codec::Result;
use crate::db::Db;
use crate::record::NONE;
use crate::steps::{self, RefStep};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Debug, Default, Clone)]
pub struct StepCheck {
    pub sessions: usize,
    pub engine_steps: usize,
    /// Distinct steps the reference finds across all requests of each session.
    pub reference_keys: usize,
    pub engine_keys: usize,
    /// Reference steps the engine lacks, and engine steps the reference lacks.
    pub missing: usize,
    pub extra: usize,
    /// Steps whose `prev` is not the last step before their turn on their path.
    pub prev_wrong: usize,
    /// Shape codes that do not match tool and outcome.
    pub shape_wrong: usize,
    pub bad_sessions: Vec<u32>,
}

impl StepCheck {
    pub fn ok(&self) -> bool {
        self.missing == 0 && self.extra == 0 && self.prev_wrong == 0 && self.shape_wrong == 0
    }
}

fn engine_keys(db: &Db, s: u32) -> Result<(HashSet<RefStep>, usize, usize)> {
    let ss = db.steps(s);
    let sd = db.step_dicts();
    let mut keys = HashSet::new();
    let mut msg_cache: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut shape_wrong = 0;
    for (pos, r) in ss.rows.iter().enumerate() {
        let m = match msg_cache.get(&r.call_node) {
            Some(m) => m,
            None => {
                let m = db.message(s, r.call_node)?;
                msg_cache.entry(r.call_node).or_insert(m)
            }
        };
        let (calls, _) = steps::scan_message(m);
        let id = calls.iter().find(|c| c.block == r.call_block).map(|c| c.id.to_vec()).unwrap_or_default();
        let mut fields: Vec<(Vec<u8>, Option<Vec<u8>>)> = ss
            .fields_of(pos as u32)
            .iter()
            .map(|&(f, v)| (sd.fields.get(f).to_vec(), if steps::is_long(v) { None } else { Some(sd.values[f as usize].get(v).to_vec()) }))
            .collect();
        fields.sort();
        keys.insert(RefStep { tool_use_id: id, tool: sd.tools.get(r.tool).to_vec(), failed: r.failed, out_bytes: r.out_bytes, fields });
        if ss.shape[pos] != steps::StepDicts::shape_code(r.tool, r.failed) {
            shape_wrong += 1;
        }
    }
    // prev, by brute force: the last step whose result sits above the call on its path
    let mut last_at: HashMap<u32, u32> = HashMap::new();
    for (pos, r) in ss.rows.iter().enumerate() {
        last_at.insert(r.result_node, pos as u32);
    }
    let parents = db.node_parents(s);
    let mut prev_wrong = 0;
    for r in &ss.rows {
        let mut best = NONE;
        let mut n = r.call_node;
        while n != NONE {
            if let Some(&p) = last_at.get(&n) {
                // only steps that existed when this one arrived count; later ones at an
                // ancestor are impossible because ancestors come first
                best = if best == NONE { p } else { best.max(p) };
                break;
            }
            n = parents[n as usize];
        }
        if best != r.prev {
            prev_wrong += 1;
        }
    }
    Ok((keys, prev_wrong, shape_wrong))
}

/// Compare every session's steps with the slow reference over its rebuilt requests.
pub fn check_steps(db: &Db, threads: usize) -> Result<StepCheck> {
    let n = db.session_count();
    let out = Mutex::new(StepCheck { sessions: n, ..StepCheck::default() });
    let next = AtomicUsize::new(0);
    let err: Mutex<Option<crate::codec::Error>> = Mutex::new(None);
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| {
                loop {
                    let s = next.fetch_add(1, Ordering::Relaxed);
                    if s >= n {
                        break;
                    }
                    let s = s as u32;
                    let run = || -> Result<()> {
                        let mut reference = HashSet::new();
                        for ri in db.session_requests(s) {
                            for k in steps::reference_steps(&db.rebuild(ri)?) {
                                reference.insert(k);
                            }
                        }
                        let (eng, prev_wrong, shape_wrong) = engine_keys(db, s)?;
                        let missing = reference.difference(&eng).count();
                        let extra = eng.difference(&reference).count();
                        let mut o = out.lock().unwrap();
                        o.engine_steps += db.steps(s).rows.len();
                        o.reference_keys += reference.len();
                        o.engine_keys += eng.len();
                        o.missing += missing;
                        o.extra += extra;
                        o.prev_wrong += prev_wrong;
                        o.shape_wrong += shape_wrong;
                        if missing + extra + prev_wrong + shape_wrong > 0 {
                            o.bad_sessions.push(s);
                        }
                        Ok(())
                    };
                    if let Err(e) = run() {
                        *err.lock().unwrap() = Some(e);
                    }
                }
            });
        }
    });
    if let Some(e) = err.into_inner().unwrap() {
        return Err(e);
    }
    let mut o = out.into_inner().unwrap();
    o.bad_sessions.sort_unstable();
    Ok(o)
}
