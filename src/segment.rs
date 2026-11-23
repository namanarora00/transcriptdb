//! Segment files: written once at a flush or a merge, never changed afterwards.
//!
//! A segment holds *parts* of sessions: for each session it touches, the messages
//! (tree nodes), requests and steps that session gained since its previous part, or a run
//! of its parts joined by a merge. Opening a segment reads only its catalog: the session id
//! range, counts, time range, the tool and field names its steps use, a Bloom filter of the
//! sessions it holds, and the first session of each directory block. Everything else is
//! read on demand; finding a request or a session by its id goes through the index.
//!
//! `seg-N.tdb`: magic; each part's messages as one stream cut into fixed-size zstd frames
//! (each with zstd's content checksum); blocks (zstd, crc32c), being groups of encoded
//! parts (message metadata and requests in one kind of group, steps in another) and the
//! session directory in blocks of about 64 KB, whose entries say where each part's blocks
//! are; in a merged segment, the zstd dictionary its frames were compressed with (trained
//! by the merge on its input); then the catalog and the footer.
//!
//! A frame compressed with a dictionary is decompressed into a buffer that holds the
//! dictionary's bytes right before it (one per thread for the last two segments it read),
//! so that zstd copies matches into the dictionary as matches into earlier output.

use crate::block::{file, handle, read_block, read_catalog, write_block, write_catalog, BlockRef};
use crate::bloom::{key_hash, Bloom};
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::record::ReqRec;
use crate::steps::{self, StepRow};
use crate::wal::{sync_file, SyncMode};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SEG_MAGIC: &[u8; 8] = b"TDBSEG08";
/// Raw bytes per group block of encoded parts, and per directory block.
const GROUP: usize = 64 * 1024;
/// Bloom filter bits per key: about 0.2% false positives.
const BLOOM_BITS: usize = 14;

pub fn seg_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("seg-{num:06}.tdb"))
}

#[derive(Debug, Clone)]
pub struct SegNode {
    pub id: u32,
    pub parent: u32,
    pub off: u32,
    pub len: u32,
    pub holes: Vec<(u32, u64)>,
}

/// One session's messages and requests in a segment.
#[derive(Debug, Clone, Default)]
pub struct CorePart {
    pub frames: Vec<(u64, u32)>,
    pub raw_len: u64,
    /// In storage order; ids are `first_node..first_node + nodes.len()` in some order.
    pub nodes: Vec<SegNode>,
    pub first_node: u32,
    pub first_req: u32,
    pub requests: Vec<ReqRec>,
}

/// One session's steps in a segment. Tool and field codes are the segment's; value codes
/// index `values`, the part's own dictionary.
#[derive(Debug, Clone, Default)]
pub struct StepPart {
    pub first: u32,
    pub rows: Vec<StepRow>,
    /// Per row: (field, value code or `steps::LONG_BASE + i` for `longs[i]`).
    pub fields: Vec<Vec<(u32, u32)>>,
    /// (field, value as JSON text)
    pub values: Vec<(u32, Vec<u8>)>,
    pub longs: Vec<(u32, u32)>,
}

/// Where a session's part sits: (block, offset in the decompressed block, length).
pub type PartRef = (BlockRef, u32, u32);

#[derive(Debug, Clone)]
pub struct DirEnt {
    pub session: u32,
    pub key: (String, String),
    pub first_node: u32,
    pub n_nodes: u32,
    pub first_req: u32,
    pub n_reqs: u32,
    pub first_row: u32,
    pub n_rows: u32,
    pub frame_bytes: u64,
    pub core: PartRef,
    pub steps: PartRef,
}

/// What every open store keeps in memory about a segment.
#[derive(Debug, Clone, Default)]
pub struct SegCat {
    /// 0 for a flush; 1 for a merge output, which holds whole sessions.
    pub level: u8,
    pub frame_size: u32,
    pub max_seq: u64,
    /// Lowest and highest session id in the segment.
    pub lo: u32,
    pub hi: u32,
    pub sessions: u32,
    pub requests: u64,
    pub nodes: u64,
    pub steps: u64,
    /// Earliest and latest request time (ms since the epoch; 0 when unknown).
    pub time_lo: i64,
    pub time_hi: i64,
    /// Tool and field names; step parts use their positions here as codes.
    pub tools: Vec<Vec<u8>>,
    pub fields: Vec<Vec<u8>>,
    /// Over the session ids with a part here.
    pub sess_bloom: Bloom,
    /// (first session, block) per directory block.
    pub dirs: Vec<(u32, BlockRef)>,
    pub frame_bytes: u64,
    pub node_bytes: u64,
    /// The zstd dictionary the frames were compressed with (merged segments), if any.
    pub dict: Option<BlockRef>,
}

impl SegCat {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u8(self.level);
        e.var(self.frame_size as u64);
        e.var(self.max_seq);
        e.var(self.lo as u64);
        e.var(self.hi as u64);
        e.var(self.sessions as u64);
        e.var(self.requests);
        e.var(self.nodes);
        e.var(self.steps);
        e.var(self.time_lo.max(0) as u64);
        e.var(self.time_hi.max(0) as u64);
        e.var(self.tools.len() as u64);
        for t in &self.tools {
            e.bytes(t);
        }
        e.var(self.fields.len() as u64);
        for f in &self.fields {
            e.bytes(f);
        }
        self.sess_bloom.encode(&mut e);
        e.var(self.dirs.len() as u64);
        for (s, r) in &self.dirs {
            e.var(*s as u64);
            r.encode(&mut e);
        }
        e.var(self.frame_bytes);
        e.var(self.node_bytes);
        match &self.dict {
            None => e.u8(0),
            Some(r) => {
                e.u8(1);
                r.encode(&mut e);
            }
        }
        e.buf
    }

    fn decode(b: &[u8]) -> Result<SegCat> {
        let mut d = Dec::new(b);
        let mut c = SegCat { level: d.u8()?, frame_size: d.var32()?, max_seq: d.var()?, lo: d.var32()?, hi: d.var32()?, sessions: d.var32()?, ..SegCat::default() };
        c.requests = d.var()?;
        c.nodes = d.var()?;
        c.steps = d.var()?;
        c.time_lo = d.var()? as i64;
        c.time_hi = d.var()? as i64;
        for _ in 0..d.var()? {
            c.tools.push(d.bytes()?.to_vec());
        }
        for _ in 0..d.var()? {
            c.fields.push(d.bytes()?.to_vec());
        }
        c.sess_bloom = Bloom::decode(&mut d)?;
        for _ in 0..d.var()? {
            c.dirs.push((d.var32()?, BlockRef::decode(&mut d)?));
        }
        c.frame_bytes = d.var()?;
        c.node_bytes = d.var()?;
        c.dict = match d.u8()? {
            0 => None,
            1 => Some(BlockRef::decode(&mut d)?),
            _ => return corrupt("bad segment catalog"),
        };
        if !d.done() || c.dirs.windows(2).any(|w| w[0].0 > w[1].0) || (c.sessions > 0) != !c.dirs.is_empty() {
            return corrupt("bad segment catalog");
        }
        Ok(c)
    }

    /// Directory blocks that may hold parts of session `s`.
    pub fn dir_blocks_for(&self, s: u32) -> std::ops::Range<usize> {
        let a = self.dirs.partition_point(|d| d.0 < s).saturating_sub(1);
        let b = self.dirs.partition_point(|d| d.0 <= s);
        a..b.max(a)
    }

    pub fn may_hold(&self, s: u32) -> bool {
        self.sessions > 0 && s >= self.lo && s <= self.hi && self.sess_bloom.may_contain(key_hash(&s.to_le_bytes()))
    }

    /// Bytes this catalog takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        256 + self.sess_bloom.bytes() + self.dirs.len() * 40 + self.tools.iter().chain(&self.fields).map(|x| x.len() + 24).sum::<usize>()
    }
}

pub fn key_bytes(s: &str, a: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(s.len() + a.len() + 1);
    k.extend_from_slice(s.as_bytes());
    k.push(0);
    k.extend_from_slice(a.as_bytes());
    k
}

// Session parts are encoded column by column (each column a length-prefixed run of
// values), with ids, offsets and times as differences from what precedes them: each column
// holds values of one kind, which zstd compresses much better than rows of mixed fields.

fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzz(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn opt_id(v: u32) -> u64 {
    if v == u32::MAX { 0 } else { v as u64 + 1 }
}

fn unopt_id(v: u64) -> Result<u32> {
    if v == 0 { Ok(u32::MAX) } else { u32::try_from(v - 1).map_err(|_| crate::codec::Error::Corrupt("id over u32".into())) }
}

fn put_columns(cols: Vec<Enc>, e: &mut Enc) {
    for c in cols {
        e.bytes(&c.buf);
    }
}

fn get_columns<'a>(d: &mut Dec<'a>, n: usize) -> Result<Vec<Dec<'a>>> {
    (0..n).map(|_| Ok(Dec::new(d.bytes()?))).collect()
}

fn i64_from(base: i64, delta: u64) -> Result<i64> {
    base.checked_add(unzz(delta)).ok_or_else(|| crate::codec::Error::Corrupt("delta out of range".into()))
}

fn u32_from(base: i64, delta: u64) -> Result<u32> {
    u32::try_from(i64_from(base, delta)?).map_err(|_| crate::codec::Error::Corrupt("value out of range".into()))
}

const CORE_COLS: usize = 14;

fn encode_core(p: &CorePart, e: &mut Enc) {
    let mut c: Vec<Enc> = (0..CORE_COLS).map(|_| Enc::new()).collect();
    c[0].var(p.raw_len);
    c[0].var(p.frames.len() as u64);
    c[0].var(p.first_node as u64);
    c[0].var(p.nodes.len() as u64);
    c[0].var(p.first_req as u64);
    c[0].var(p.requests.len() as u64);
    // frames: where each starts against where the one before it ended (0 when back to back)
    let mut end = 0i64;
    for &(o, l) in &p.frames {
        c[1].var(zz(o as i64 - end));
        c[2].var(l as u64);
        end = o as i64 + l as i64;
    }
    // nodes in storage order: id against the one before plus 1, parent against id, offset
    // against where the node before ended
    let (mut prev, mut at) = (p.first_node as i64 - 1, 0i64);
    let mut last_opaque = 0i64;
    for n in &p.nodes {
        c[3].var(zz(n.id as i64 - prev - 1));
        prev = n.id as i64;
        c[4].var(if n.parent == u32::MAX { 0 } else { zz(n.id as i64 - n.parent as i64) + 1 });
        c[5].var(zz(n.off as i64 - at));
        c[6].var(n.len as u64);
        at = n.off as i64 + n.len as i64;
        c[7].var(n.holes.len() as u64);
        let mut po = 0i64;
        for &(o, id) in &n.holes {
            c[8].var(zz(o as i64 - po));
            po = o as i64;
            c[8].var(zz(id as i64 - last_opaque));
            last_opaque = id as i64;
        }
    }
    let (mut tip, mut nm, mut ps, mut sess) = (0i64, 0i64, [0i64; 3], 0i64);
    for r in &p.requests {
        c[12].var(zz(r.session as i64 - sess));
        sess = r.session as i64;
        c[9].bytes(r.id.as_bytes());
        c[10].bytes(r.time.as_bytes());
        c[11].bytes(&r.meta);
        let t = opt_id(r.tip) as i64;
        c[12].var(zz(t - tip));
        tip = t;
        c[12].var(zz(r.n_msgs as i64 - nm));
        nm = r.n_msgs as i64;
        for (k, v) in [r.skeleton as u64, opt_id(r.system), opt_id(r.tools)].into_iter().enumerate() {
            c[12].var(zz(v as i64 - ps[k]));
            ps[k] = v as i64;
        }
        c[13].var(r.notes.len() as u64);
        for n in r.notes.iter() {
            c[13].var(n.target as u64);
            c[13].var(n.offset as u64);
            c[13].var(n.text as u64);
        }
        match &r.layout {
            None => c[13].u8(0),
            Some(g) => {
                c[13].u8(1);
                c[13].var(g.len() as u64);
                for x in g.iter() {
                    c[13].bytes(x);
                }
            }
        }
    }
    put_columns(c, e);
}

pub fn decode_core(b: &[u8]) -> Result<CorePart> {
    let mut d = Dec::new(b);
    let mut c = get_columns(&mut d, CORE_COLS)?;
    if !d.done() {
        return corrupt("trailing bytes in a session part");
    }
    let raw_len = c[0].var()?;
    let nf = c[0].var()? as usize;
    let first_node = c[0].var32()?;
    let nn = c[0].var()? as usize;
    let first_req = c[0].var32()?;
    let nr = c[0].var()? as usize;
    let mut frames = Vec::with_capacity(nf.min(1 << 20));
    let mut end = 0i64;
    for _ in 0..nf {
        let o = i64_from(end, c[1].var()?)?;
        let l = c[2].var32()?;
        frames.push((u64::try_from(o).map_err(|_| crate::codec::Error::Corrupt("bad frame offset".into()))?, l));
        end = o + l as i64;
    }
    let mut nodes = Vec::with_capacity(nn.min(1 << 20));
    let (mut prev, mut at) = (first_node as i64 - 1, 0i64);
    let mut last_opaque = 0i64;
    for _ in 0..nn {
        let id = u32_from(prev + 1, c[3].var()?)?;
        prev = id as i64;
        let pv = c[4].var()?;
        let parent = if pv == 0 { u32::MAX } else { u32_from(id as i64, zz(-unzz(pv - 1)))? };
        let off = u32_from(at, c[5].var()?)?;
        let len = c[6].var32()?;
        at = off as i64 + len as i64;
        let nh = c[7].var()? as usize;
        let mut holes = Vec::with_capacity(nh.min(1 << 16));
        let mut po = 0i64;
        for _ in 0..nh {
            let o = u32_from(po, c[8].var()?)?;
            po = o as i64;
            let id = i64_from(last_opaque, c[8].var()?)?;
            last_opaque = id;
            holes.push((o, u64::try_from(id).map_err(|_| crate::codec::Error::Corrupt("bad opaque id".into()))?));
        }
        nodes.push(SegNode { id, parent, off, len, holes });
    }
    let mut requests = Vec::with_capacity(nr.min(1 << 20));
    let (mut tip, mut nm, mut ps, mut sess) = (0i64, 0i64, [0i64; 3], 0i64);
    let utf8 = |b: &[u8]| -> Result<Box<str>> { Ok(std::str::from_utf8(b).map_err(|_| crate::codec::Error::Corrupt("string not utf-8".into()))?.into()) };
    for _ in 0..nr {
        sess = i64_from(sess, c[12].var()?)?;
        let id = utf8(c[9].bytes()?)?;
        let time = utf8(c[10].bytes()?)?;
        let meta = c[11].bytes()?.to_vec().into_boxed_slice();
        tip = i64_from(tip, c[12].var()?)?;
        nm = i64_from(nm, c[12].var()?)?;
        let mut v = [0u64; 3];
        for k in 0..3 {
            ps[k] = i64_from(ps[k], c[12].var()?)?;
            v[k] = u64::try_from(ps[k]).map_err(|_| crate::codec::Error::Corrupt("bad piece id".into()))?;
        }
        let nn = c[13].var()? as usize;
        let mut notes = Vec::with_capacity(nn.min(1 << 16));
        for _ in 0..nn {
            notes.push(crate::record::ReqNote { target: c[13].var32()?, offset: c[13].var32()?, text: c[13].var32()? });
        }
        let layout = match c[13].u8()? {
            0 => None,
            1 => {
                let n = c[13].var()? as usize;
                let mut g = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    g.push(c[13].bytes()?.to_vec().into_boxed_slice());
                }
                Some(g.into_boxed_slice())
            }
            _ => return corrupt("bad layout flag"),
        };
        let as_u32 = |x: i64| u32::try_from(x).map_err(|_| crate::codec::Error::Corrupt("value out of range".into()));
        requests.push(ReqRec {
            id,
            time,
            meta,
            session: as_u32(sess)?,
            tip: unopt_id(as_u32(tip)? as u64)?,
            n_msgs: as_u32(nm)?,
            skeleton: u32::try_from(v[0]).map_err(|_| crate::codec::Error::Corrupt("bad piece id".into()))?,
            system: unopt_id(v[1])?,
            tools: unopt_id(v[2])?,
            notes: notes.into_boxed_slice(),
            layout,
        });
    }
    if c.iter().any(|x| !x.done()) {
        return corrupt("trailing bytes in a session part");
    }
    Ok(CorePart { frames, raw_len, nodes, first_node, first_req, requests })
}

const STEP_COLS: usize = 12;

fn encode_steps(p: &StepPart, e: &mut Enc) {
    let mut c: Vec<Enc> = (0..STEP_COLS).map(|_| Enc::new()).collect();
    c[0].var(p.first as u64);
    c[0].var(p.values.len() as u64);
    c[0].var(p.rows.len() as u64);
    for (f, v) in &p.values {
        c[1].var(*f as u64);
        c[2].bytes(v);
    }
    let (mut pc, mut pt) = (0i64, 0i64);
    for (i, (r, fs)) in p.rows.iter().zip(&p.fields).enumerate() {
        c[3].var(zz(r.call_node as i64 - pc));
        pc = r.call_node as i64;
        c[4].var(zz(r.result_node as i64 - r.call_node as i64));
        c[5].var(r.call_block as u64);
        c[5].var(r.result_block as u64);
        c[6].var(if r.prev == u32::MAX { 0 } else { zz(p.first as i64 + i as i64 - r.prev as i64) + 1 });
        c[7].var(r.tool as u64);
        c[7].u8(r.failed as u8);
        c[8].var(r.out_bytes as u64);
        c[9].var(zz(r.time_ms.max(0) - pt));
        pt = r.time_ms.max(0);
        c[10].var(fs.len() as u64);
        for &(f, v) in fs {
            c[10].var(f as u64);
            if steps::is_long(v) {
                let (off, len) = p.longs[(v - steps::LONG_BASE) as usize];
                c[11].var(0);
                c[11].var(off as u64);
                c[11].var(len as u64);
            } else {
                c[11].var(v as u64 + 1);
            }
        }
    }
    put_columns(c, e);
}

pub fn decode_steps(b: &[u8]) -> Result<StepPart> {
    let mut d = Dec::new(b);
    let mut c = get_columns(&mut d, STEP_COLS)?;
    if !d.done() {
        return corrupt("trailing bytes in a step part");
    }
    let first = c[0].var32()?;
    let nv = c[0].var()? as usize;
    let n = c[0].var()? as usize;
    let mut values = Vec::with_capacity(nv.min(1 << 20));
    for _ in 0..nv {
        values.push((c[1].var32()?, c[2].bytes()?.to_vec()));
    }
    let mut rows = Vec::with_capacity(n.min(1 << 20));
    let mut fields = Vec::with_capacity(n.min(1 << 20));
    let mut longs = Vec::new();
    let (mut pc, mut pt) = (0i64, 0i64);
    for i in 0..n {
        let call_node = u32_from(pc, c[3].var()?)?;
        pc = call_node as i64;
        let result_node = u32_from(call_node as i64, c[4].var()?)?;
        let call_block = c[5].var32()? as u16;
        let result_block = c[5].var32()? as u16;
        let pv = c[6].var()?;
        let prev = if pv == 0 { u32::MAX } else { u32_from(first as i64 + i as i64, zz(-unzz(pv - 1)))? };
        let tool = c[7].var32()?;
        let failed = c[7].u8()? != 0;
        let out_bytes = c[8].var32()?;
        let time_ms = i64_from(pt, c[9].var()?)?;
        pt = time_ms;
        let nf = c[10].var()? as usize;
        let mut fs = Vec::with_capacity(nf.min(1 << 10));
        for _ in 0..nf {
            let f = c[10].var32()?;
            let v = c[11].var()?;
            if v == 0 {
                longs.push((c[11].var32()?, c[11].var32()?));
                fs.push((f, steps::LONG_BASE + longs.len() as u32 - 1));
            } else {
                let v = (v - 1) as u32;
                if v as usize >= values.len() {
                    return corrupt("step names a missing value");
                }
                fs.push((f, v));
            }
        }
        rows.push(StepRow { call_node, result_node, prev, tool, time_ms, out_bytes, call_block, result_block, failed });
        fields.push(fs);
    }
    if c.iter().any(|x| !x.done()) {
        return corrupt("trailing bytes in a step part");
    }
    Ok(StepPart { first, rows, fields, values, longs })
}

fn encode_dir(dir: &[DirEnt]) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(dir.len() as u64);
    for x in dir {
        e.var(x.session as u64);
        e.bytes(x.key.0.as_bytes());
        e.bytes(x.key.1.as_bytes());
        for v in [x.first_node, x.n_nodes, x.first_req, x.n_reqs, x.first_row, x.n_rows] {
            e.var(v as u64);
        }
        e.var(x.frame_bytes);
        for (b, o, l) in [x.core, x.steps] {
            b.encode(&mut e);
            e.var(o as u64);
            e.var(l as u64);
        }
    }
    e.buf
}

pub fn decode_dir(b: &[u8]) -> Result<Vec<DirEnt>> {
    let mut d = Dec::new(b);
    let n = d.var()? as usize;
    let mut out = Vec::with_capacity(n.min(1 << 22));
    for _ in 0..n {
        let session = d.var32()?;
        let key = (d.string()?, d.string()?);
        let mut v = [0u32; 6];
        for x in v.iter_mut() {
            *x = d.var32()?;
        }
        let frame_bytes = d.var()?;
        let core = (BlockRef::decode(&mut d)?, d.var32()?, d.var32()?);
        let steps = (BlockRef::decode(&mut d)?, d.var32()?, d.var32()?);
        out.push(DirEnt { session, key, first_node: v[0], n_nodes: v[1], first_req: v[2], n_reqs: v[3], first_row: v[4], n_rows: v[5], frame_bytes, core, steps });
    }
    if !d.done() || out.windows(2).any(|w| (w[0].session, w[0].first_req) >= (w[1].session, w[1].first_req)) {
        return corrupt("bad segment directory");
    }
    Ok(out)
}

/// How a segment's frames and blocks are compressed.
#[derive(Debug, Clone, Default)]
pub struct Codec {
    /// zstd level for frames.
    pub level: i32,
    /// zstd level for blocks (parts' metadata, steps, directories).
    pub block_level: i32,
    /// A trained zstd dictionary for the frames; empty for none.
    pub dict: Vec<u8>,
    /// Threads compressing a session's frames at once.
    pub threads: usize,
}

impl Codec {
    /// What flushes use: one thread, no dictionary.
    pub fn plain(level: i32) -> Codec {
        Codec { level, block_level: 3, dict: Vec::new(), threads: 1 }
    }
}

fn frame_compressor<'a>(level: i32, dict: Option<&'a zstd::dict::EncoderDictionary<'static>>) -> Result<zstd::bulk::Compressor<'a>> {
    use zstd::zstd_safe::CParameter;
    let mut c = match dict {
        None => zstd::bulk::Compressor::new(level)?,
        Some(d) => zstd::bulk::Compressor::with_prepared_dictionary(d)?,
    };
    c.set_parameter(CParameter::ChecksumFlag(true))?;
    if dict.is_some() {
        // the segment says which dictionary; the id would only repeat it in every frame
        c.set_parameter(CParameter::DictIdFlag(false))?;
    }
    Ok(c)
}

/// Compress `chunks` on up to `threads` threads (each takes the next chunk not yet taken),
/// each with its own compressor sharing one prepared dictionary; results in order.
fn compress_all(level: i32, dict: Option<&zstd::dict::EncoderDictionary<'static>>, threads: usize, chunks: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
    let n = threads.min(chunks.len()).max(1);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let work = || -> Result<Vec<(usize, Vec<u8>)>> {
        let mut c = frame_compressor(level, dict)?;
        let mut out = Vec::new();
        loop {
            let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if k >= chunks.len() {
                return Ok(out);
            }
            out.push((k, c.compress(chunks[k])?));
        }
    };
    let parts: Vec<Result<Vec<(usize, Vec<u8>)>>> = if n == 1 {
        vec![work()]
    } else {
        std::thread::scope(|sc| {
            let hs: Vec<_> = (0..n).map(|_| sc.spawn(work)).collect();
            hs.into_iter().map(|h| h.join().expect("frame compression thread")).collect()
        })
    };
    let mut done: Vec<Vec<u8>> = vec![Vec::new(); chunks.len()];
    for p in parts {
        for (k, c) in p? {
            done[k] = c;
        }
    }
    Ok(done)
}

/// A part whose frames wait to be compressed together with other parts' frames.
struct Pending {
    session: u32,
    key: (String, String),
    core: CorePart,
    stream: Vec<u8>,
    steps: StepPart,
}

/// Raw message bytes of parts waiting for their frames to be compressed (merges).
const PENDING_BYTES: usize = 32 << 20;

pub struct SegWriter {
    f: BufWriter<File>,
    pos: u64,
    /// For a flush: one thread, no dictionary.
    comp: Option<zstd::bulk::Compressor<'static>>,
    /// The codec's dictionary, prepared once for all threads.
    cdict: Option<zstd::dict::EncoderDictionary<'static>>,
    codec: Codec,
    frame: usize,
    cat: SegCat,
    tool_ix: HashMap<Vec<u8>, u32>,
    field_ix: HashMap<Vec<u8>, u32>,
    dir: Vec<DirEnt>,
    core_buf: Vec<u8>,
    core_waiting: Vec<usize>,
    steps_buf: Vec<u8>,
    steps_waiting: Vec<usize>,
    pending: Vec<Pending>,
    pending_raw: usize,
}

impl SegWriter {
    pub fn create(dir: &Path, num: u32, frame: usize, codec: Codec, seg_level: u8, max_seq: u64) -> Result<SegWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(seg_path(dir, num))?;
        let mut f = BufWriter::with_capacity(1 << 20, f);
        f.write_all(SEG_MAGIC)?;
        let plain = codec.dict.is_empty() && codec.threads <= 1;
        let comp = if plain { Some(frame_compressor(codec.level, None)?) } else { None };
        let cdict = if codec.dict.is_empty() { None } else { Some(zstd::dict::EncoderDictionary::copy(&codec.dict, codec.level)) };
        let cat = SegCat { level: seg_level, frame_size: frame as u32, max_seq, lo: u32::MAX, hi: 0, time_lo: i64::MAX, ..SegCat::default() };
        Ok(SegWriter {
            f,
            pos: 8,
            comp,
            cdict,
            codec,
            frame,
            cat,
            tool_ix: HashMap::new(),
            field_ix: HashMap::new(),
            dir: Vec::new(),
            core_buf: Vec::new(),
            core_waiting: Vec::new(),
            steps_buf: Vec::new(),
            steps_waiting: Vec::new(),
            pending: Vec::new(),
            pending_raw: 0,
        })
    }

    /// Write a session's message stream as frames; returns (offset, compressed length) per frame.
    pub fn frames(&mut self, stream: &[u8]) -> Result<Vec<(u64, u32)>> {
        let packed = match &mut self.comp {
            Some(c) => stream.chunks(self.frame).map(|ch| c.compress(ch)).collect::<std::io::Result<Vec<_>>>()?,
            None => compress_all(self.codec.level, self.cdict.as_ref(), self.codec.threads, &stream.chunks(self.frame).collect::<Vec<_>>())?,
        };
        self.write_frames(packed)
    }

    fn write_frames(&mut self, packed: Vec<Vec<u8>>) -> Result<Vec<(u64, u32)>> {
        let mut out = Vec::with_capacity(packed.len());
        for c in packed {
            self.f.write_all(&c)?;
            out.push((self.pos, c.len() as u32));
            self.pos += c.len() as u64;
            self.cat.frame_bytes += c.len() as u64;
        }
        Ok(out)
    }

    /// Add a part with its message stream not yet written: with several threads, the frames
    /// of many parts are compressed at once (most parts have only a few frames). The part's
    /// frames are filled in when written. Same order rules as [`SegWriter::add`].
    pub fn add_stream(&mut self, session: u32, key: (String, String), mut core: CorePart, stream: Vec<u8>, steps: StepPart) -> Result<()> {
        if self.comp.is_some() {
            core.frames = self.frames(&stream)?;
            return self.add(session, key, &core, &steps);
        }
        self.pending_raw += stream.len();
        self.pending.push(Pending { session, key, core, stream, steps });
        if self.pending_raw >= PENDING_BYTES {
            self.flush_pending()?;
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending);
        self.pending_raw = 0;
        let fs = self.frame;
        let chunks: Vec<&[u8]> = pending.iter().flat_map(|p| p.stream.chunks(fs)).collect();
        let mut packed = compress_all(self.codec.level, self.cdict.as_ref(), self.codec.threads, &chunks)?.into_iter();
        drop(chunks);
        for mut p in pending {
            let nf = p.stream.len().div_ceil(fs);
            let mine: Vec<Vec<u8>> = packed.by_ref().take(nf).collect();
            p.core.frames = self.write_frames(mine)?;
            self.add(p.session, p.key, &p.core, &p.steps)?;
        }
        Ok(())
    }

    pub fn tool_code(&mut self, name: &[u8]) -> u32 {
        if let Some(&c) = self.tool_ix.get(name) {
            return c;
        }
        self.cat.tools.push(name.to_vec());
        let c = self.cat.tools.len() as u32 - 1;
        self.tool_ix.insert(name.to_vec(), c);
        c
    }

    pub fn field_code(&mut self, name: &[u8]) -> u32 {
        if let Some(&c) = self.field_ix.get(name) {
            return c;
        }
        self.cat.fields.push(name.to_vec());
        let c = self.cat.fields.len() as u32 - 1;
        self.field_ix.insert(name.to_vec(), c);
        c
    }

    /// Compressed bytes written so far (frames and blocks), with a guess for parts waiting
    /// for their frames.
    pub fn written(&self) -> u64 {
        self.pos + (self.pending_raw / 4) as u64
    }

    fn seal(&mut self, steps: bool) -> Result<()> {
        let (buf, waiting) = if steps { (std::mem::take(&mut self.steps_buf), std::mem::take(&mut self.steps_waiting)) } else { (std::mem::take(&mut self.core_buf), std::mem::take(&mut self.core_waiting)) };
        if waiting.is_empty() {
            return Ok(());
        }
        let r = write_block(&mut self.f, &mut self.pos, &buf, self.codec.block_level)?;
        for i in waiting {
            if steps {
                self.dir[i].steps.0 = r;
            } else {
                self.dir[i].core.0 = r;
            }
        }
        Ok(())
    }

    /// Add one part of a session. Parts must come in order of (session, first request); the
    /// part's frames must already be written with [`SegWriter::frames`].
    pub fn add(&mut self, session: u32, key: (String, String), core: &CorePart, steps: &StepPart) -> Result<()> {
        if self.dir.last().is_some_and(|d| (d.session, d.first_req) >= (session, core.first_req)) {
            return corrupt("session parts added to a segment out of order");
        }
        let i = self.dir.len();
        let mut e = Enc::new();
        encode_core(core, &mut e);
        let core_ref = (BlockRef::default(), self.core_buf.len() as u32, e.buf.len() as u32);
        self.core_buf.extend_from_slice(&e.buf);
        self.core_waiting.push(i);
        let mut e = Enc::new();
        encode_steps(steps, &mut e);
        let steps_ref = (BlockRef::default(), self.steps_buf.len() as u32, e.buf.len() as u32);
        self.steps_buf.extend_from_slice(&e.buf);
        self.steps_waiting.push(i);
        for r in core.requests.iter() {
            let t = steps::parse_time_ms(&r.time);
            if t > 0 {
                self.cat.time_lo = self.cat.time_lo.min(t);
                self.cat.time_hi = self.cat.time_hi.max(t);
            }
        }
        let c = &mut self.cat;
        c.lo = c.lo.min(session);
        c.hi = c.hi.max(session);
        c.sessions += 1;
        c.requests += core.requests.len() as u64;
        c.nodes += core.nodes.len() as u64;
        c.steps += steps.rows.len() as u64;
        c.node_bytes += core.nodes.iter().map(|n| n.len as u64).sum::<u64>();
        let frame_bytes = core.frames.iter().map(|f| f.1 as u64).sum();
        self.dir.push(DirEnt {
            session,
            key,
            first_node: core.first_node,
            n_nodes: core.nodes.len() as u32,
            first_req: core.first_req,
            n_reqs: core.requests.len() as u32,
            first_row: steps.first,
            n_rows: steps.rows.len() as u32,
            frame_bytes,
            core: core_ref,
            steps: steps_ref,
        });
        if self.core_buf.len() >= GROUP {
            self.seal(false)?;
        }
        if self.steps_buf.len() >= GROUP {
            self.seal(true)?;
        }
        Ok(())
    }

    pub fn sessions(&self) -> usize {
        self.dir.len() + self.pending.len()
    }

    pub fn finish(mut self, mode: SyncMode) -> Result<SegCat> {
        self.flush_pending()?;
        self.seal(false)?;
        self.seal(true)?;
        // the directory in blocks of about GROUP bytes, each starting at a new entry
        let mut a = 0;
        while a < self.dir.len() {
            let mut b = a;
            let mut size = 0;
            while b < self.dir.len() && (b == a || size < GROUP) {
                size += 64 + self.dir[b].key.0.len() + self.dir[b].key.1.len();
                b += 1;
            }
            let r = write_block(&mut self.f, &mut self.pos, &encode_dir(&self.dir[a..b]), self.codec.block_level)?;
            self.cat.dirs.push((self.dir[a].session, r));
            a = b;
        }
        if !self.codec.dict.is_empty() {
            self.cat.dict = Some(write_block(&mut self.f, &mut self.pos, &self.codec.dict, self.codec.block_level)?);
        }
        let mut sb = Bloom::new(self.dir.len(), BLOOM_BITS);
        for d in &self.dir {
            sb.add(key_hash(&d.session.to_le_bytes()));
        }
        self.cat.sess_bloom = sb;
        if self.cat.time_lo == i64::MAX {
            self.cat.time_lo = 0;
        }
        if self.dir.is_empty() {
            self.cat.lo = 0;
        }
        let cat = self.cat.encode();
        write_catalog(&mut self.f, &mut self.pos, &cat, SEG_MAGIC)?;
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        sync_file(&f, mode)?;
        Ok(self.cat)
    }
}

pub struct Segment {
    pub num: u32,
    /// Unique in this process (frames are cached by it).
    uid: u64,
    path: PathBuf,
    pub cat: SegCat,
    pub file_bytes: u64,
}

thread_local! {
    static DCTX: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
    /// For frames compressed with a dictionary.
    static DDCTX: RefCell<Option<zstd::zstd_safe::DCtx<'static>>> = const { RefCell::new(None) };
    /// Recently decompressed frames, per thread: frames never change, so this is always safe.
    static FRAMES: RefCell<FrameCache> = RefCell::new(FrameCache::default());
}

const FRAME_CACHE: usize = 128;

#[derive(Default)]
struct FrameCache {
    map: HashMap<(u64, u64), Arc<Vec<u8>>>,
    order: std::collections::VecDeque<(u64, u64)>,
}

impl FrameCache {
    fn get(&self, k: &(u64, u64)) -> Option<Arc<Vec<u8>>> {
        self.map.get(k).cloned()
    }
    fn put(&mut self, k: (u64, u64), v: Arc<Vec<u8>>) {
        if self.map.insert(k, v).is_none() {
            self.order.push_back(k);
            while self.order.len() > FRAME_CACHE {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }
}

/// Segments' dictionaries (raw bytes), shared by all threads within a byte budget; one
/// dropped from it is read again from its segment.
struct DictCache {
    map: HashMap<u64, Arc<[u8]>>,
    order: std::collections::VecDeque<(u64, usize)>,
    bytes: usize,
}

const DICT_BUDGET: usize = 32 << 20;

static DICTS: std::sync::Mutex<Option<DictCache>> = std::sync::Mutex::new(None);

/// A dictionary's bytes with room for one frame right after them, and the dictionary made
/// from those bytes by reference. Decompressing into that room makes matches into the
/// dictionary ordinary matches into earlier output, which zstd copies much faster than
/// matches into a dictionary kept elsewhere (about 25 against 41 us per 64 KB frame).
struct DictBuf {
    uid: u64,
    ddict: std::mem::ManuallyDrop<zstd::zstd_safe::DDict<'static>>,
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    dlen: usize,
}

impl DictBuf {
    fn new(uid: u64, dict: &[u8], room: usize) -> DictBuf {
        let mut b = vec![0u8; dict.len() + room].into_boxed_slice();
        b[..dict.len()].copy_from_slice(dict);
        let len = b.len();
        let ptr = std::ptr::NonNull::new(Box::into_raw(b) as *mut u8).expect("box pointer");
        // SAFETY: the buffer lives until `drop`, which drops the dictionary first, and its
        // first `dict.len()` bytes are never written again.
        let pre: &'static [u8] = unsafe { std::slice::from_raw_parts(ptr.as_ptr(), dict.len()) };
        DictBuf { uid, ddict: std::mem::ManuallyDrop::new(zstd::zstd_safe::DDict::create_by_reference(pre)), ptr, len, dlen: dict.len() }
    }

    /// `src` decompressed into the room after the dictionary; it must give `cap` bytes.
    fn decompress(&mut self, dctx: &mut zstd::zstd_safe::DCtx<'static>, src: &[u8], cap: usize) -> Result<Vec<u8>> {
        if self.dlen + cap > self.len {
            return corrupt("frame larger than the frame size");
        }
        // SAFETY: inside the buffer, after the dictionary's bytes (which `ddict` reads).
        let room = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(self.dlen), cap) };
        let n = dctx.decompress_using_ddict(room, src, &self.ddict).map_err(|e| crate::codec::Error::Corrupt(format!("zstd: {}", zstd::zstd_safe::get_error_name(e))))?;
        if n != cap {
            return corrupt("frame has the wrong size");
        }
        Ok(room.to_vec())
    }
}

impl Drop for DictBuf {
    fn drop(&mut self) {
        // SAFETY: the dictionary goes first; then the buffer, made by `Box::into_raw` in `new`.
        unsafe {
            std::mem::ManuallyDrop::drop(&mut self.ddict);
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr.as_ptr(), self.len)));
        }
    }
}

/// Dictionary buffers per thread, most recently used last.
const DICT_BUFS: usize = 2;

thread_local! {
    static DICT_BUF: RefCell<Vec<DictBuf>> = const { RefCell::new(Vec::new()) };
}

pub fn decompress(src: &[u8], cap: usize) -> Result<Vec<u8>> {
    DCTX.with(|c| {
        let mut c = c.borrow_mut();
        if c.is_none() {
            *c = Some(zstd::bulk::Decompressor::new()?);
        }
        Ok(c.as_mut().unwrap().decompress(src, cap)?)
    })
}

impl Segment {
    pub fn open(dir: &Path, num: u32) -> Result<Segment> {
        let path = seg_path(dir, num);
        let f = file(&path)?;
        let (c, file_bytes) = read_catalog(&f, SEG_MAGIC, &format!("segment {num}"))?;
        let cat = SegCat::decode(&c)?;
        Ok(Segment { num, uid: crate::block::uid(), path, cat, file_bytes })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The dictionary this segment's frames were compressed with, if any.
    fn dict(&self) -> Result<Option<Arc<[u8]>>> {
        let Some(r) = &self.cat.dict else { return Ok(None) };
        if let Some(d) = DICTS.lock().unwrap().as_ref().and_then(|c| c.map.get(&self.uid).cloned()) {
            return Ok(Some(d));
        }
        let d: Arc<[u8]> = self.block(r)?.into();
        let mut g = DICTS.lock().unwrap();
        let c = g.get_or_insert_with(|| DictCache { map: HashMap::new(), order: Default::default(), bytes: 0 });
        if c.map.insert(self.uid, d.clone()).is_none() {
            c.order.push_back((self.uid, d.len()));
            c.bytes += d.len();
            while c.bytes > DICT_BUDGET && c.order.len() > 1 {
                let (u, n) = c.order.pop_front().unwrap();
                c.map.remove(&u);
                c.bytes -= n;
            }
        }
        Ok(Some(d))
    }

    /// Frame `src` of this segment decompressed with its dictionary.
    fn decompress_dict(&self, src: &[u8], cap: usize) -> Result<Vec<u8>> {
        DICT_BUF.with(|bufs| {
            let mut bufs = bufs.borrow_mut();
            match bufs.iter().position(|b| b.uid == self.uid) {
                Some(i) => {
                    let b = bufs.remove(i);
                    bufs.push(b);
                }
                None => {
                    let d = self.dict()?.ok_or_else(|| crate::codec::Error::Corrupt("segment has no dictionary".into()))?;
                    if bufs.len() >= DICT_BUFS {
                        bufs.remove(0);
                    }
                    bufs.push(DictBuf::new(self.uid, &d, self.cat.frame_size as usize));
                }
            }
            let b = bufs.last_mut().unwrap();
            DDCTX.with(|c| {
                let mut c = c.borrow_mut();
                b.decompress(c.get_or_insert_with(zstd::zstd_safe::DCtx::create), src, cap)
            })
        })
    }

    pub fn block(&self, r: &BlockRef) -> Result<Vec<u8>> {
        if r.off + r.clen as u64 > self.file_bytes {
            return corrupt(format!("segment {} has no block at {}", self.num, r.off));
        }
        read_block(&*handle(self.uid, &self.path)?, r, &format!("segment {}", self.num))
    }

    /// Directory block `i`.
    pub fn dir_block(&self, i: usize) -> Result<Vec<DirEnt>> {
        let (first, r) = self.cat.dirs.get(i).ok_or_else(|| crate::codec::Error::Corrupt(format!("segment {} has no directory block {i}", self.num)))?;
        let d = decode_dir(&self.block(r)?)?;
        if d.first().map(|x| x.session) != Some(*first) {
            return corrupt(format!("segment {}: directory block {i} does not start where the catalog says", self.num));
        }
        Ok(d)
    }

    /// Frames `first..=last` of a part's stream, decompressed: from the cache, or with one
    /// read for the run of frames that are not cached.
    pub fn frames(&self, frames: &[(u64, u32)], raw_len: u64, first: usize, last: usize) -> Result<Vec<Arc<Vec<u8>>>> {
        let mut out: Vec<Option<Arc<Vec<u8>>>> = (first..=last).map(|k| FRAMES.with(|c| c.borrow().get(&(self.uid, frames[k].0)))).collect();
        let missing: Vec<usize> = (first..=last).filter(|k| out[k - first].is_none()).collect();
        if let (Some(&lo), Some(&hi)) = (missing.first(), missing.last()) {
            let start = frames[lo].0;
            let end = frames[hi].0 + frames[hi].1 as u64;
            let mut buf = vec![0u8; (end - start) as usize];
            handle(self.uid, &self.path)?.read_exact_at(&mut buf, start)?;
            let fs = self.cat.frame_size as usize;
            let dict = self.cat.dict.is_some();
            for k in lo..=hi {
                if out[k - first].is_some() {
                    continue;
                }
                let (o, l) = frames[k];
                let s = (o - start) as usize;
                let cap = (raw_len as usize - k * fs).min(fs);
                let src = &buf[s..s + l as usize];
                let d = if dict { self.decompress_dict(src, cap)? } else { decompress(src, cap)? };
                if d.len() != cap {
                    return corrupt(format!("segment {} frame {k} has the wrong size", self.num));
                }
                let d = Arc::new(d);
                FRAMES.with(|c| c.borrow_mut().put((self.uid, o), d.clone()));
                out[k - first] = Some(d);
            }
        }
        Ok(out.into_iter().map(|x| x.unwrap()).collect())
    }
}
