//! Storage order for a session tree: "main line first".
//!
//! Each node is weighted by how many requests pass through it. A depth-first walk
//! that always takes the heaviest child first writes the main line as one run, and
//! every side branch as one run right after the subtree it hangs from. A request that
//! follows the main line is then one read; a request on a side branch is two.

use crate::record::NONE;

/// Order the nodes `first..first + n` of one session.
/// `parent(id)` gives the parent id (`NONE` for a root); parents outside the range
/// count as roots. `tips` lists the last node of each request (repeats allowed).
pub fn heavy_first(first: u32, n: u32, parent: impl Fn(u32) -> u32, tips: impl Iterator<Item = u32>) -> Vec<u32> {
    let n = n as usize;
    let local = |id: u32| -> Option<usize> {
        if id == NONE || id < first || (id - first) as usize >= n { None } else { Some((id - first) as usize) }
    };
    let mut w = vec![0u64; n];
    for t in tips {
        if let Some(i) = local(t) {
            w[i] += 1;
        }
    }
    let mut par = vec![usize::MAX; n];
    for i in 0..n {
        if let Some(p) = local(parent(first + i as u32)) {
            par[i] = p;
        }
    }
    // children always have larger ids than their parents
    for i in (0..n).rev() {
        if par[i] != usize::MAX {
            debug_assert!(par[i] < i);
            w[par[i]] += w[i];
        }
    }
    // children lists in CSR form
    let mut count = vec![0u32; n + 1];
    for i in 0..n {
        if par[i] != usize::MAX {
            count[par[i] + 1] += 1;
        }
    }
    for i in 0..n {
        count[i + 1] += count[i];
    }
    let mut fill = count.clone();
    let mut kids = vec![0u32; count[n] as usize];
    let mut roots = Vec::new();
    for i in 0..n {
        if par[i] == usize::MAX {
            roots.push(i as u32);
        } else {
            let p = par[i];
            kids[fill[p] as usize] = i as u32;
            fill[p] += 1;
        }
    }
    for p in 0..n {
        let s = &mut kids[count[p] as usize..count[p + 1] as usize];
        if s.len() > 1 {
            s.sort_by(|&a, &b| w[b as usize].cmp(&w[a as usize]).then(a.cmp(&b)));
        }
    }
    let mut out = Vec::with_capacity(n);
    let mut stack: Vec<u32> = roots.iter().rev().copied().collect();
    while let Some(i) = stack.pop() {
        out.push(first + i);
        let s = &kids[count[i as usize] as usize..count[i as usize + 1] as usize];
        stack.extend(s.iter().rev());
    }
    debug_assert_eq!(out.len(), n);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_line_is_one_run() {
        // 0-1-2-3-5-6 is the main line; 4 is a side query off 3; 7 a side query off 1.
        let parents = [NONE, 0, 1, 2, 3, 3, 5, 1];
        let tips = [1, 2, 3, 4, 5, 6, 6, 7];
        let order = heavy_first(0, 8, |i| parents[i as usize], tips.iter().copied());
        assert_eq!(order, vec![0, 1, 2, 3, 5, 6, 4, 7]);
    }

    #[test]
    fn partial_range() {
        // nodes 10..13, parent of 10 is outside (already stored)
        let order = heavy_first(10, 3, |i| [9, 10, 10][(i - 10) as usize], [11, 12, 12].into_iter());
        assert_eq!(order, vec![10, 12, 11]);
    }
}
