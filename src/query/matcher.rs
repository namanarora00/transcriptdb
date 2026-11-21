//! The one-pass matcher for `match`.
//!
//! Input: per step a bit mask (bit `k` = the step passes element `k`, bit `32 + x` = it
//! passes exclusion `x`), and the session's steps with `prev`. Steps that share `prev`,
//! call message and result message form one *turn*; turns form a tree (a turn's parent
//! is the turn of its `prev`). The matcher walks turns in step order, computing for each
//! turn the set of partial matches ("threads") alive after it, from the set alive after
//! its parent. Shared history is processed once however many branches grow from it.
//!
//! Semantics (the slow reference in `reference.rs` implements the same by brute force):
//! * `then` = any later turn on the path; `next` = the very next turn. Calls in one turn
//!   are unordered: a turn can advance a thread by at most one element.
//! * `{m,n}` = that many consecutive turns.
//! * `within N`: the last turn is at most N turns after the first.
//! * `no (P) between a and c`: no turn strictly between them holds a step passing P.
//! * A match is reported at the turn that completes it, with the latest bindings that
//!   allow it (latest first element, then latest later ones). At most one match per turn,
//!   and matching restarts after it (matches do not overlap).
//!
//! One thread per (element, repeat count) suffices: a thread that reaches a state later
//! has a start and bindings no earlier than any older thread in that state, and every
//! constraint (window, exclusions) only gets easier with later bindings.

use crate::record::NONE;
use crate::steps::SessionSteps;

pub const MAX_ELEMS: usize = 8;
pub const UNBOUNDED: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct Spec {
    pub n: usize,
    pub min: [u32; MAX_ELEMS],
    pub max: [u32; MAX_ELEMS],
    /// `next[k]`: element `k` must come in the turn right after element `k - 1`.
    pub next: [bool; MAX_ELEMS],
    pub within: Option<u32>,
    /// (from elem, to elem); exclusion `x` is mask bit `32 + x`.
    pub excl: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Match {
    /// Step that completed the match.
    pub end: u32,
    /// Last step bound to each element.
    pub binds: [u32; MAX_ELEMS],
}

#[derive(Debug, Clone, Copy)]
struct Th {
    k: u8,
    count: u32,
    start: u32,
    last: u32,
    binds: [u32; MAX_ELEMS],
}

impl Th {
    fn key(&self, sp: &Spec) -> (u8, u32) {
        let k = self.k as usize;
        (self.k, if sp.max[k] == UNBOUNDED { self.count.min(sp.min[k]) } else { self.count })
    }
    fn rank(&self) -> (u32, [u32; MAX_ELEMS]) {
        let mut b = [0u32; MAX_ELEMS];
        b[..=self.k as usize].copy_from_slice(&self.binds[..=self.k as usize]);
        (self.start, b)
    }
}

fn put(sp: &Spec, set: &mut Vec<Th>, t: Th) {
    let key = t.key(sp);
    for x in set.iter_mut() {
        if x.key(sp) == key {
            if t.rank() > x.rank() {
                *x = t;
            }
            return;
        }
    }
    set.push(t);
}

/// Is a thread whose last matched element is `k` cut by an exclusion seen in this turn?
/// `here`: the thread matched element `k` in this very turn (then spans starting at `k`
/// do not cover this turn).
#[inline]
fn killed(sp: &Spec, k: usize, here: bool, gx: u64) -> bool {
    if gx == 0 {
        return false;
    }
    for (x, &(i, j)) in sp.excl.iter().enumerate() {
        if gx >> x & 1 == 1 && i <= k && k < j && !(here && i == k) {
            return true;
        }
    }
    false
}

/// Run the pattern over one session. Matches are appended in step order.
pub fn run(sp: &Spec, ss: &SessionSteps, masks: &[u64], out: &mut Vec<Match>) {
    let rows = &ss.rows;
    let n = rows.len();
    if n == 0 || sp.n == 0 {
        return;
    }
    let last_elem = sp.n - 1;
    let mut inst_of = vec![0u32; n];
    let mut depth: Vec<u32> = Vec::new();
    let mut range: Vec<(u32, u32)> = Vec::new();
    let mut pool: Vec<Th> = Vec::new();
    let mut before: Vec<Th> = Vec::new();
    let mut new: Vec<Th> = Vec::new();
    let mut i = 0;
    while i < n {
        let r = rows[i];
        let mut j = i + 1;
        while j < n && rows[j].prev == r.prev && rows[j].call_node == r.call_node && rows[j].result_node == r.result_node {
            j += 1;
        }
        let g = depth.len() as u32;
        for p in inst_of.iter_mut().take(j).skip(i) {
            *p = g;
        }
        before.clear();
        let pd = if r.prev == NONE {
            0
        } else {
            let pi = inst_of[r.prev as usize] as usize;
            let (o, l) = range[pi];
            before.extend_from_slice(&pool[o as usize..(o + l) as usize]);
            depth[pi]
        };
        let d = pd + 1;
        let gx = masks[i..j].iter().fold(0u64, |a, m| a | (m >> 32));
        new.clear();
        let mut done: Option<Th> = None;
        let within_ok = |start: u32| sp.within.is_none_or(|w| d - start <= w);

        // threads that wait through this turn
        for t in &before {
            let k = t.k as usize;
            if t.count < sp.min[k] || k == last_elem || sp.next[k + 1] || !within_ok(t.start) {
                continue;
            }
            if killed(sp, k, false, gx) {
                continue;
            }
            put(sp, &mut new, *t);
        }
        // threads that take a step of this turn
        for (pos, &m) in masks.iter().enumerate().take(j).skip(i) {
            let pos = pos as u32;
            for ti in 0..=before.len() {
                let init = ti == before.len();
                let mut cands: [Option<Th>; 2] = [None, None];
                if init {
                    if m & 1 == 1 {
                        let mut b = [NONE; MAX_ELEMS];
                        b[0] = pos;
                        cands[0] = Some(Th { k: 0, count: 1, start: d, last: d, binds: b });
                    }
                } else {
                    let t = &before[ti];
                    let k = t.k as usize;
                    // one more of the same element
                    if t.count < sp.max[k] && d == t.last + 1 && m >> k & 1 == 1 {
                        let mut u = *t;
                        u.count += 1;
                        u.last = d;
                        u.binds[k] = pos;
                        cands[0] = Some(u);
                    }
                    // the next element
                    if t.count >= sp.min[k] && k < last_elem && m >> (k + 1) & 1 == 1 && (!sp.next[k + 1] || d == t.last + 1) {
                        let mut u = *t;
                        u.k += 1;
                        u.count = 1;
                        u.last = d;
                        u.binds[k + 1] = pos;
                        cands[1] = Some(u);
                    }
                }
                for u in cands.into_iter().flatten() {
                    let k = u.k as usize;
                    if !within_ok(u.start) || killed(sp, k, true, gx) {
                        continue;
                    }
                    if k == last_elem && u.count == sp.min[k] {
                        let better = match &done {
                            None => true,
                            Some(x) => (u.start, u.binds) > (x.start, x.binds),
                        };
                        if better {
                            done = Some(u);
                        }
                    } else {
                        put(sp, &mut new, u);
                    }
                }
            }
        }
        if let Some(t) = done {
            let mut b = [NONE; MAX_ELEMS];
            b[..sp.n].copy_from_slice(&t.binds[..sp.n]);
            out.push(Match { end: t.binds[last_elem], binds: b });
            new.clear();
        }
        depth.push(d);
        range.push((pool.len() as u32, new.len() as u32));
        pool.extend_from_slice(&new);
        i = j;
    }
}
