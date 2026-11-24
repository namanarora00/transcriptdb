//! The word index over message text, kept in each segment and written with it.
//!
//! For every text unit of the segment's parts (see [`crate::text`]): which message and
//! block it is, who wrote it, its length and time; and for every word, the units that
//! hold it. A *word* is a maximal run of letters and digits of the lower-cased text, three
//! bytes or longer; a run over [`MAX_WORD`] bytes is kept by its first bytes.
//!
//! A whole-word test (`has`) with one word is answered from the index alone. A substring
//! test (`contains`) reads the units whose words can hold the pattern and confirms each on
//! its text, except a pattern that is one run of letters and digits, which every unit with
//! a word containing it matches.
//!
//! Like the step summaries, everything here is derived from the parts the segment holds:
//! a merge rebuilds it for its output and recovery needs nothing.

use crate::block::BlockRef;
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::segment::CorePart;
use crate::text;
use std::collections::BTreeMap;

pub const MIN_WORD: usize = 3;
pub const MAX_WORD: usize = 64;
const VERSION: u8 = 1;
/// Raw bytes of postings per block.
const POST_CHUNK: usize = 256 * 1024;

/// Each maximal run of letters and digits in `low` (text already lower-cased).
pub fn runs(low: &str, mut f: impl FnMut(usize, &str)) {
    let mut start = None;
    for (i, ch) in low.char_indices() {
        match (ch.is_alphanumeric(), start) {
            (true, None) => start = Some(i),
            (false, Some(a)) => {
                f(a, &low[a..i]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(a) = start {
        f(a, &low[a..]);
    }
}

/// The key a run is indexed by: itself, or its first bytes when it is long.
pub fn key(run: &str) -> &str {
    if run.len() <= MAX_WORD {
        return run;
    }
    let mut n = MAX_WORD;
    while !run.is_char_boundary(n) {
        n -= 1;
    }
    &run[..n]
}

/// One unit of a part as the index needs it.
#[derive(Debug, Clone, Default)]
pub struct UnitBuild {
    pub node: u32,
    pub block: u16,
    pub user: bool,
    pub chars: u32,
    pub time: i64,
    /// An opaque value was cut out of its text: only its message says what it holds.
    pub hole: bool,
    /// It has a word longer than [`MAX_WORD`].
    pub long: bool,
    /// Its distinct words, as indexed.
    pub words: Vec<String>,
}

/// The text units of a part, in node order then block order, from its stored bytes.
pub fn part_units(core: &CorePart, stream: &[u8]) -> Vec<UnitBuild> {
    // a node's time is that of the first request of the part that carried it
    let (lo, n) = (core.first_node, core.nodes.len() as u32);
    let mut parent = vec![crate::record::NONE; n as usize];
    for x in &core.nodes {
        if x.id >= lo && x.id - lo < n {
            parent[(x.id - lo) as usize] = x.parent;
        }
    }
    let mut time = vec![0i64; n as usize];
    let mut seen = vec![false; n as usize];
    for r in &core.requests {
        let t = crate::steps::parse_time_ms(&r.time);
        let mut c = r.tip;
        while c >= lo && c - lo < n && !seen[(c - lo) as usize] {
            seen[(c - lo) as usize] = true;
            time[(c - lo) as usize] = t;
            c = parent[(c - lo) as usize];
        }
    }
    let mut out = Vec::new();
    for x in &core.nodes {
        let b = &stream[x.off as usize..(x.off + x.len) as usize];
        let Some((user, spans)) = text::scan_text(b) else { continue };
        for t in spans {
            let hole = x.holes.iter().any(|h| (h.0 as usize) >= t.span.start && (h.0 as usize) <= t.span.end);
            let s = text::unescape(&b[t.span.clone()]);
            let low = s.to_lowercase();
            let mut words: Vec<String> = Vec::new();
            let mut long = false;
            runs(&low, |_, r| {
                if r.len() >= MIN_WORD {
                    long |= r.len() > MAX_WORD;
                    words.push(key(r).to_string());
                }
            });
            words.sort_unstable();
            words.dedup();
            let idx = if x.id >= lo && x.id - lo < n { (x.id - lo) as usize } else { usize::MAX };
            out.push(UnitBuild { node: x.id, block: t.block, user, chars: s.len() as u32, time: time.get(idx).copied().unwrap_or(0), hole, long, words });
        }
    }
    out.sort_by_key(|u| (u.node, u.block));
    out
}

/// Collects a segment's text index as its parts are added (in directory order).
#[derive(Default)]
pub struct TextIndexBuilder {
    parts: Vec<(u32, u32)>,
    units: Vec<UnitBuild>,
    post: BTreeMap<String, Vec<u32>>,
}

fn zig(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzig(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

impl TextIndexBuilder {
    pub fn add_part(&mut self, session: u32, units: Vec<UnitBuild>) {
        self.parts.push((session, units.len() as u32));
        for mut u in units {
            let id = self.units.len() as u32;
            for w in std::mem::take(&mut u.words) {
                self.post.entry(w).or_default().push(id);
            }
            self.units.push(u);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.units.is_empty()
    }

    /// Write the index through `write` (one call per block); returns the head's block.
    pub fn finish(self, write: &mut dyn FnMut(&[u8]) -> Result<BlockRef>) -> Result<BlockRef> {
        // units
        let mut e = Enc::new();
        let (mut prev_node, mut prev_time) = (0u32, 0i64);
        let mut at = 0usize;
        for &(_, n) in &self.parts {
            for (k, u) in self.units[at..at + n as usize].iter().enumerate() {
                e.var(if k == 0 { u.node as u64 } else { (u.node - prev_node) as u64 });
                e.var(u.block as u64);
                e.u8(u.user as u8 | (u.hole as u8) << 1 | (u.long as u8) << 2);
                e.var(u.chars as u64);
                e.var(zig(u.time - prev_time));
                prev_node = u.node;
                prev_time = u.time;
            }
            at += n as usize;
        }
        let units_ref = write(&e.buf)?;
        // postings in blocks, words front-coded with the length of each list
        let mut words = Enc::new();
        let mut chunks: Vec<(u64, BlockRef)> = Vec::new();
        let (mut buf, mut start, mut prev) = (Vec::new(), 0u64, String::new());
        words.var(self.post.len() as u64);
        for (w, ids) in &self.post {
            let mut list = Enc::new();
            list.var(ids.len() as u64);
            let mut p = 0;
            for &id in ids {
                list.var((id - p) as u64);
                p = id;
            }
            let shared = w.bytes().zip(prev.bytes()).take_while(|(a, b)| a == b).count();
            words.var(shared as u64);
            words.bytes(&w.as_bytes()[shared..]);
            words.var(list.buf.len() as u64);
            prev.clone_from(w);
            buf.extend_from_slice(&list.buf);
            if buf.len() >= POST_CHUNK {
                chunks.push((start, write(&buf)?));
                start += buf.len() as u64;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            chunks.push((start, write(&buf)?));
        }
        let words_ref = write(&words.buf)?;
        let mut h = Enc::new();
        h.u8(VERSION);
        h.var(self.parts.len() as u64);
        for &(s, n) in &self.parts {
            h.var(s as u64);
            h.var(n as u64);
        }
        units_ref.encode(&mut h);
        words_ref.encode(&mut h);
        h.var(chunks.len() as u64);
        for (s, r) in &chunks {
            h.var(*s);
            r.encode(&mut h);
        }
        write(&h.buf)
    }
}

/// Where a segment's text index is, read from its head block.
#[derive(Debug, Clone, Default)]
pub struct TextDir {
    pub parts: Vec<(u32, u32)>,
    pub units: BlockRef,
    pub words: BlockRef,
    pub chunks: Vec<(u64, BlockRef)>,
}

pub fn decode_dir(b: &[u8]) -> Result<TextDir> {
    let mut d = Dec::new(b);
    if d.u8()? != VERSION {
        return corrupt("unknown text index version");
    }
    let mut t = TextDir::default();
    for _ in 0..d.var()? {
        t.parts.push((d.var32()?, d.var32()?));
    }
    t.units = BlockRef::decode(&mut d)?;
    t.words = BlockRef::decode(&mut d)?;
    for _ in 0..d.var()? {
        t.chunks.push((d.var()?, BlockRef::decode(&mut d)?));
    }
    Ok(t)
}

/// A set of units of one segment.
pub type Bits = Vec<u64>;

pub fn bits(n: usize) -> Bits {
    vec![0; n.div_ceil(64)]
}

#[inline]
pub fn set(b: &mut Bits, i: u32) {
    b[(i >> 6) as usize] |= 1 << (i & 63);
}

#[inline]
pub fn get(b: &Bits, i: u32) -> bool {
    b[(i >> 6) as usize] >> (i & 63) & 1 == 1
}

pub fn each(b: &Bits, mut f: impl FnMut(u32)) {
    for (k, &w) in b.iter().enumerate() {
        let mut w = w;
        while w != 0 {
            f((k as u32) << 6 | w.trailing_zeros());
            w &= w - 1;
        }
    }
}

pub fn any(b: &Bits) -> bool {
    b.iter().any(|&w| w != 0)
}

/// A segment's text index in memory: every unit and the word list (postings are read by
/// block when a word is looked up).
#[derive(Debug, Default)]
pub struct TextHead {
    /// (session, first unit, units) per part, in directory order.
    pub parts: Vec<(u32, u32, u32)>,
    pub part_of: Vec<u32>,
    pub node: Vec<u32>,
    pub block: Vec<u16>,
    pub chars: Vec<u32>,
    pub time: Vec<i64>,
    pub user: Bits,
    /// Units with an opaque value cut out of their text.
    pub holes: Bits,
    pub has_holes: bool,
    /// Units with a word longer than [`MAX_WORD`].
    pub long: Bits,
    words: Vec<u8>,
    word_at: Vec<u32>,
    /// Where each word's postings start in the concatenated postings (one past the end last).
    post_off: Vec<u64>,
    pub chunks: Vec<(u64, BlockRef)>,
}

/// How a pattern's run must relate to a word that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    Exact,
    Prefix,
    Suffix,
    Inside,
}

impl TextHead {
    pub fn decode(dir: &TextDir, units: &[u8], words: &[u8]) -> Result<TextHead> {
        let n: usize = dir.parts.iter().map(|p| p.1 as usize).sum();
        let mut h = TextHead { user: bits(n), holes: bits(n), long: bits(n), chunks: dir.chunks.clone(), ..TextHead::default() };
        let mut d = Dec::new(units);
        let (mut prev_node, mut prev_time) = (0u32, 0i64);
        for (pi, &(s, pn)) in dir.parts.iter().enumerate() {
            h.parts.push((s, h.node.len() as u32, pn));
            for k in 0..pn {
                let id = h.node.len() as u32;
                let dn = d.var32()?;
                let node = if k == 0 { dn } else { prev_node + dn };
                h.node.push(node);
                h.block.push(d.var32()? as u16);
                let f = d.u8()?;
                if f & 1 != 0 {
                    set(&mut h.user, id);
                }
                if f & 2 != 0 {
                    set(&mut h.holes, id);
                    h.has_holes = true;
                }
                if f & 4 != 0 {
                    set(&mut h.long, id);
                }
                h.chars.push(d.var32()?);
                let t = prev_time + unzig(d.var()?);
                h.time.push(t);
                h.part_of.push(pi as u32);
                prev_node = node;
                prev_time = t;
            }
        }
        let mut d = Dec::new(words);
        let nw = d.var()? as usize;
        h.word_at.reserve(nw + 1);
        h.post_off.reserve(nw + 1);
        let mut off = 0u64;
        let mut prev_start = 0usize;
        for _ in 0..nw {
            let shared = d.var()? as usize;
            let rest = d.bytes()?;
            let start = h.words.len();
            if shared > start - prev_start {
                return corrupt("bad shared prefix in the text index's words");
            }
            h.words.extend_from_within(prev_start..prev_start + shared);
            h.words.extend_from_slice(rest);
            h.word_at.push(start as u32);
            h.post_off.push(off);
            off += d.var()?;
            prev_start = start;
        }
        h.word_at.push(h.words.len() as u32);
        h.post_off.push(off);
        Ok(h)
    }

    pub fn units(&self) -> usize {
        self.node.len()
    }

    pub fn n_words(&self) -> usize {
        self.word_at.len() - 1
    }

    pub fn word(&self, i: usize) -> &[u8] {
        &self.words[self.word_at[i] as usize..self.word_at[i + 1] as usize]
    }

    pub fn bytes(&self) -> usize {
        256 + self.node.len() * 23 + self.words.len() + self.word_at.len() * 12 + self.parts.len() * 12 + self.user.len() * 24
    }

    /// The words a run of a pattern can sit in, by index.
    pub fn find(&self, run: &[u8], fit: Fit, out: &mut Vec<usize>) {
        let n = self.n_words();
        let lower = |k: &[u8]| {
            let (mut a, mut b) = (0, n);
            while a < b {
                let m = (a + b) / 2;
                if self.word(m) < k { a = m + 1 } else { b = m }
            }
            a
        };
        match fit {
            Fit::Exact => {
                let i = lower(run);
                if i < n && self.word(i) == run {
                    out.push(i);
                }
            }
            Fit::Prefix => {
                let mut i = lower(run);
                while i < n && self.word(i).starts_with(run) {
                    out.push(i);
                    i += 1;
                }
            }
            Fit::Suffix | Fit::Inside => {
                let f = memchr::memmem::Finder::new(run);
                // one pass over the word list; a hit that crosses two words is not one
                let mut from = 0;
                let mut w = 0;
                while let Some(k) = f.find(&self.words[from..]) {
                    let at = from + k;
                    while self.word_at[w + 1] as usize <= at {
                        w += 1;
                    }
                    let end = self.word_at[w + 1] as usize;
                    if if fit == Fit::Suffix { at + run.len() == end } else { at + run.len() <= end } {
                        out.push(w);
                        from = end;
                    } else {
                        from = at + 1;
                    }
                }
            }
        }
    }

    /// Which block of postings word `w` is in, and its byte range there.
    pub fn postings_at(&self, w: usize) -> (usize, usize, usize) {
        let (a, b) = (self.post_off[w], self.post_off[w + 1]);
        let c = self.chunks.partition_point(|x| x.0 <= a) - 1;
        ((c), (a - self.chunks[c].0) as usize, (b - self.chunks[c].0) as usize)
    }
}

/// Add the units of one posting list (as stored) to `out`.
pub fn add_postings(list: &[u8], out: &mut Bits) -> Result<()> {
    let mut d = Dec::new(list);
    let n = d.var()?;
    let mut id = 0u32;
    for _ in 0..n {
        id += d.var32()?;
        if (id >> 6) as usize >= out.len() {
            return corrupt("posting past the last unit");
        }
        set(out, id);
    }
    Ok(())
}

/// The runs of a lower-cased pattern that an index lookup can use, each with how a word
/// must hold it. `whole` (a `has` test): the pattern starts and ends at word boundaries.
/// The second value says whether the pattern is exactly one run, so that a unit matches if
/// and only if it has a fitting word (when none of its words is cut short).
pub fn pattern_runs(low: &str, whole: bool) -> (Vec<(String, Fit)>, bool) {
    let mut out = Vec::new();
    let mut n_runs = 0;
    let mut covered = 0;
    runs(low, |a, r| {
        n_runs += 1;
        covered += r.len();
        let open_left = !whole && a == 0;
        let open_right = !whole && a + r.len() == low.len();
        let fit = match (open_left, open_right) {
            (false, false) => Fit::Exact,
            (false, true) => Fit::Prefix,
            (true, false) => Fit::Suffix,
            (true, true) => Fit::Inside,
        };
        if r.len() < MIN_WORD {
            return;
        }
        if r.len() > MAX_WORD {
            // only its start can be looked up, and only where the run starts a word
            if matches!(fit, Fit::Exact | Fit::Prefix) {
                out.push((key(r).to_string(), Fit::Prefix));
            }
            return;
        }
        out.push((r.to_string(), fit));
    });
    // a word cut short keeps at least MAX_WORD - 3 bytes, so a shorter pattern is never one
    let single = n_runs == 1 && covered == low.len() && out.len() == 1 && low.len() + 4 <= MAX_WORD;
    (out, single)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(node: u32, block: u16, user: bool, text: &str) -> UnitBuild {
        let low = text.to_lowercase();
        let mut words = Vec::new();
        let mut long = false;
        runs(&low, |_, r| {
            if r.len() >= MIN_WORD {
                long |= r.len() > MAX_WORD;
                words.push(key(r).to_string());
            }
        });
        words.sort_unstable();
        words.dedup();
        UnitBuild { node, block, user, chars: text.len() as u32, time: 1000 + node as i64, hole: false, long, words }
    }

    fn build(parts: Vec<(u32, Vec<UnitBuild>)>) -> (TextHead, Vec<Vec<u8>>) {
        let mut b = TextIndexBuilder::default();
        for (s, u) in parts {
            b.add_part(s, u);
        }
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let head = b
            .finish(&mut |raw| {
                blocks.push(raw.to_vec());
                Ok(BlockRef { off: blocks.len() as u64 - 1, clen: raw.len() as u32, rawlen: raw.len() as u32, ..BlockRef::default() })
            })
            .unwrap();
        let dir = decode_dir(&blocks[head.off as usize]).unwrap();
        let h = TextHead::decode(&dir, &blocks[dir.units.off as usize], &blocks[dir.words.off as usize]).unwrap();
        (h, blocks)
    }

    fn units_of(h: &TextHead, blocks: &[Vec<u8>], run: &str, fit: Fit) -> Vec<u32> {
        let mut ws = Vec::new();
        h.find(run.as_bytes(), fit, &mut ws);
        let mut b = bits(h.units());
        for w in ws {
            let (c, a, e) = h.postings_at(w);
            add_postings(&blocks[h.chunks[c].1.off as usize][a..e], &mut b).unwrap();
        }
        let mut v = Vec::new();
        each(&b, |i| v.push(i));
        v
    }

    #[test]
    fn round_trip_and_lookups() {
        let (h, blocks) = build(vec![
            (3, vec![unit(0, 0, true, "Fix the commit hook"), unit(1, 0, false, "I will git-commit now"), unit(1, 2, false, "Done; committed.")]),
            (7, vec![unit(5, 0, true, "legit question about GIT")]),
        ]);
        assert_eq!(h.units(), 4);
        assert_eq!(h.parts, vec![(3, 0, 3), (7, 3, 1)]);
        assert_eq!(h.node, vec![0, 1, 1, 5]);
        assert_eq!(h.block, vec![0, 0, 2, 0]);
        assert_eq!(h.time, vec![1000, 1001, 1001, 1005]);
        assert!(get(&h.user, 0) && !get(&h.user, 1) && get(&h.user, 3));
        assert_eq!(units_of(&h, &blocks, "commit", Fit::Exact), vec![0, 1]);
        assert_eq!(units_of(&h, &blocks, "commit", Fit::Prefix), vec![0, 1, 2]);
        assert_eq!(units_of(&h, &blocks, "git", Fit::Suffix), vec![1, 3]);
        assert_eq!(units_of(&h, &blocks, "omm", Fit::Inside), vec![0, 1, 2]);
        assert_eq!(units_of(&h, &blocks, "nothing", Fit::Exact), Vec::<u32>::new());
    }

    #[test]
    fn pattern_runs_fit() {
        assert_eq!(pattern_runs("git commit", false), (vec![("git".into(), Fit::Suffix), ("commit".into(), Fit::Prefix)], false));
        assert_eq!(pattern_runs("commit", false), (vec![("commit".into(), Fit::Inside)], true));
        assert_eq!(pattern_runs("commit", true), (vec![("commit".into(), Fit::Exact)], true));
        assert_eq!(pattern_runs(" a root cause.", false), (vec![("root".into(), Fit::Exact), ("cause".into(), Fit::Exact)], false));
        assert_eq!(pattern_runs("to", true), (vec![], false));
        assert_eq!(pattern_runs("->", false), (vec![], false));
    }

    #[test]
    fn long_words_are_kept_by_their_start() {
        let long = "a".repeat(100);
        let (h, blocks) = build(vec![(0, vec![unit(0, 0, true, &format!("x {long} y")), unit(1, 0, true, "short")])]);
        assert!(get(&h.long, 0) && !get(&h.long, 1));
        assert_eq!(units_of(&h, &blocks, &"a".repeat(MAX_WORD), Fit::Prefix), vec![0]);
        let (r, single) = pattern_runs(&long, true);
        assert_eq!(r, vec![("a".repeat(MAX_WORD), Fit::Prefix)]);
        assert!(!single);
    }
}
