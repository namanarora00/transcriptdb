//! Segment files: written once at a flush or a merge, never changed afterwards.
//!
//! A segment holds *parts* of sessions: for each session it touches, the messages
//! (tree nodes), requests and steps that session gained since its previous part, or all
//! of them after a merge. Opening a segment reads only its catalog: the session id range,
//! counts, time range, the tool and field names its steps use, Bloom filters of session
//! keys and request ids, and where its blocks are. Everything else is read on demand.
//!
//! `seg-N.tdb`: magic; each part's messages as one stream cut into fixed-size zstd frames
//! (each with zstd's content checksum); blocks (zstd, crc32c), being groups of encoded
//! parts (message metadata and requests in one kind of group, steps in another), the
//! request id table and the session directory; then the catalog and the footer.

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

const SEG_MAGIC: &[u8; 8] = b"TDBSEG04";
/// Raw bytes per group block of encoded parts.
const GROUP: usize = 64 * 1024;
/// Request ids per block of the request table.
const REQ_BLOCK: usize = 2048;
/// Bloom filter bits per key: about 0.2% false positives.
const BLOOM_BITS: usize = 14;

pub fn seg_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("seg-{num:06}.tdb"))
}

#[derive(Debug, Clone)]
pub struct SegNode {
    pub id: u32,
    pub parent: u32,
    pub hash: u128,
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
pub type PartRef = (u32, u32, u32);

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
    pub key_bloom: Bloom,
    pub req_bloom: Bloom,
    /// Over the session ids with a part here.
    pub sess_bloom: Bloom,
    pub blocks: Vec<BlockRef>,
    pub dir: u32,
    /// (first request id, block) per block of the request id table.
    pub reqtab: Vec<(Box<str>, u32)>,
    pub frame_bytes: u64,
    pub node_bytes: u64,
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
        self.key_bloom.encode(&mut e);
        self.req_bloom.encode(&mut e);
        self.sess_bloom.encode(&mut e);
        e.var(self.blocks.len() as u64);
        for b in &self.blocks {
            b.encode(&mut e);
        }
        e.var(self.dir as u64);
        e.var(self.reqtab.len() as u64);
        for (id, b) in &self.reqtab {
            e.bytes(id.as_bytes());
            e.var(*b as u64);
        }
        e.var(self.frame_bytes);
        e.var(self.node_bytes);
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
        c.key_bloom = Bloom::decode(&mut d)?;
        c.req_bloom = Bloom::decode(&mut d)?;
        c.sess_bloom = Bloom::decode(&mut d)?;
        for _ in 0..d.var()? {
            c.blocks.push(BlockRef::decode(&mut d)?);
        }
        c.dir = d.var32()?;
        for _ in 0..d.var()? {
            c.reqtab.push((d.string()?.into_boxed_str(), d.var32()?));
        }
        c.frame_bytes = d.var()?;
        c.node_bytes = d.var()?;
        if !d.done() || c.dir as usize >= c.blocks.len() || c.reqtab.iter().any(|x| x.1 as usize >= c.blocks.len()) {
            return corrupt("bad segment catalog");
        }
        Ok(c)
    }

    /// The request table block that would hold `id`.
    pub fn reqtab_block_for(&self, id: &str) -> Option<u32> {
        let i = self.reqtab.partition_point(|x| *x.0 <= *id);
        i.checked_sub(1).map(|i| self.reqtab[i].1)
    }

    pub fn may_hold(&self, s: u32) -> bool {
        self.sessions > 0 && s >= self.lo && s <= self.hi && self.sess_bloom.may_contain(key_hash(&s.to_le_bytes()))
    }

    /// Bytes this catalog takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        256 + self.key_bloom.bytes() + self.req_bloom.bytes() + self.sess_bloom.bytes() + self.blocks.len() * 24 + self.reqtab.iter().map(|x| x.0.len() + 32).sum::<usize>() + self.tools.iter().chain(&self.fields).map(|x| x.len() + 24).sum::<usize>()
    }
}

pub fn key_bytes(s: &str, a: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(s.len() + a.len() + 1);
    k.extend_from_slice(s.as_bytes());
    k.push(0);
    k.extend_from_slice(a.as_bytes());
    k
}

fn encode_core(p: &CorePart, e: &mut Enc) {
    e.var(p.raw_len);
    e.var(p.frames.len() as u64);
    for &(o, l) in &p.frames {
        e.var(o);
        e.var(l as u64);
    }
    e.var(p.first_node as u64);
    e.var(p.nodes.len() as u64);
    for n in &p.nodes {
        e.var(n.id as u64);
        e.var(if n.parent == u32::MAX { 0 } else { n.parent as u64 + 1 });
        e.u128(n.hash);
        e.var(n.off as u64);
        e.var(n.len as u64);
        e.var(n.holes.len() as u64);
        for &(o, id) in &n.holes {
            e.var(o as u64);
            e.var(id);
        }
    }
    e.var(p.first_req as u64);
    e.var(p.requests.len() as u64);
    for r in &p.requests {
        r.encode(e);
    }
}

pub fn decode_core(b: &[u8]) -> Result<CorePart> {
    let mut d = Dec::new(b);
    let raw_len = d.var()?;
    let nf = d.var()? as usize;
    let mut frames = Vec::with_capacity(nf.min(1 << 20));
    for _ in 0..nf {
        frames.push((d.var()?, d.var32()?));
    }
    let first_node = d.var32()?;
    let nn = d.var()? as usize;
    let mut nodes = Vec::with_capacity(nn.min(1 << 20));
    for _ in 0..nn {
        let id = d.var32()?;
        let p = d.var()?;
        let parent = if p == 0 { u32::MAX } else { (p - 1) as u32 };
        let hash = d.u128()?;
        let off = d.var32()?;
        let len = d.var32()?;
        let nh = d.var()? as usize;
        let mut holes = Vec::with_capacity(nh.min(1 << 16));
        for _ in 0..nh {
            holes.push((d.var32()?, d.var()?));
        }
        nodes.push(SegNode { id, parent, hash, off, len, holes });
    }
    let first_req = d.var32()?;
    let nr = d.var()? as usize;
    let mut requests = Vec::with_capacity(nr.min(1 << 20));
    for _ in 0..nr {
        requests.push(ReqRec::decode(&mut d)?);
    }
    if !d.done() {
        return corrupt("trailing bytes in a session part");
    }
    Ok(CorePart { frames, raw_len, nodes, first_node, first_req, requests })
}

fn encode_steps(p: &StepPart, e: &mut Enc) {
    e.var(p.first as u64);
    e.var(p.values.len() as u64);
    for (f, v) in &p.values {
        e.var(*f as u64);
        e.bytes(v);
    }
    e.var(p.rows.len() as u64);
    for (r, fs) in p.rows.iter().zip(&p.fields) {
        e.var(r.call_node as u64);
        e.var(r.call_block as u64);
        e.var(r.result_node as u64);
        e.var(r.result_block as u64);
        e.var(if r.prev == u32::MAX { 0 } else { r.prev as u64 + 1 });
        e.var(r.tool as u64);
        e.u8(r.failed as u8);
        e.var(r.out_bytes as u64);
        e.var(r.time_ms.max(0) as u64);
        e.var(fs.len() as u64);
        for &(f, v) in fs {
            e.var(f as u64);
            if steps::is_long(v) {
                let (off, len) = p.longs[(v - steps::LONG_BASE) as usize];
                e.var(0);
                e.var(off as u64);
                e.var(len as u64);
            } else {
                e.var(v as u64 + 1);
            }
        }
    }
}

pub fn decode_steps(b: &[u8]) -> Result<StepPart> {
    let mut d = Dec::new(b);
    let first = d.var32()?;
    let nv = d.var()? as usize;
    let mut values = Vec::with_capacity(nv.min(1 << 20));
    for _ in 0..nv {
        values.push((d.var32()?, d.bytes()?.to_vec()));
    }
    let n = d.var()? as usize;
    let mut rows = Vec::with_capacity(n.min(1 << 20));
    let mut fields = Vec::with_capacity(n.min(1 << 20));
    let mut longs = Vec::new();
    for _ in 0..n {
        let call_node = d.var32()?;
        let call_block = d.var32()? as u16;
        let result_node = d.var32()?;
        let result_block = d.var32()? as u16;
        let p = d.var()?;
        let prev = if p == 0 { u32::MAX } else { (p - 1) as u32 };
        let tool = d.var32()?;
        let failed = d.u8()? != 0;
        let out_bytes = d.var32()?;
        let time_ms = d.var()? as i64;
        let nf = d.var()? as usize;
        let mut fs = Vec::with_capacity(nf.min(1 << 10));
        for _ in 0..nf {
            let f = d.var32()?;
            let v = d.var()?;
            if v == 0 {
                longs.push((d.var32()?, d.var32()?));
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
    if !d.done() {
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
            e.var(b as u64);
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
        let core = (d.var32()?, d.var32()?, d.var32()?);
        let steps = (d.var32()?, d.var32()?, d.var32()?);
        out.push(DirEnt { session, key, first_node: v[0], n_nodes: v[1], first_req: v[2], n_reqs: v[3], first_row: v[4], n_rows: v[5], frame_bytes, core, steps });
    }
    if !d.done() || out.windows(2).any(|w| (w[0].session, w[0].first_req) >= (w[1].session, w[1].first_req)) {
        return corrupt("bad segment directory");
    }
    Ok(out)
}

/// (request id, session, index of the request in its session), sorted by id.
pub type ReqTab = Vec<(Box<str>, u32, u32)>;

pub fn decode_reqtab(b: &[u8]) -> Result<ReqTab> {
    let mut d = Dec::new(b);
    let n = d.var()? as usize;
    let mut out = Vec::with_capacity(n.min(1 << 22));
    for _ in 0..n {
        out.push((d.string()?.into_boxed_str(), d.var32()?, d.var32()?));
    }
    if !d.done() {
        return corrupt("bad request table");
    }
    Ok(out)
}

pub struct SegWriter {
    f: BufWriter<File>,
    pos: u64,
    comp: zstd::bulk::Compressor<'static>,
    frame: usize,
    cat: SegCat,
    tool_ix: HashMap<Vec<u8>, u32>,
    field_ix: HashMap<Vec<u8>, u32>,
    dir: Vec<DirEnt>,
    reqs: ReqTab,
    core_buf: Vec<u8>,
    core_waiting: Vec<usize>,
    steps_buf: Vec<u8>,
    steps_waiting: Vec<usize>,
}

impl SegWriter {
    pub fn create(dir: &Path, num: u32, frame: usize, level: i32, seg_level: u8, max_seq: u64) -> Result<SegWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(seg_path(dir, num))?;
        let mut f = BufWriter::with_capacity(1 << 20, f);
        f.write_all(SEG_MAGIC)?;
        let mut comp = zstd::bulk::Compressor::new(level)?;
        comp.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        let cat = SegCat { level: seg_level, frame_size: frame as u32, max_seq, lo: u32::MAX, hi: 0, time_lo: i64::MAX, ..SegCat::default() };
        Ok(SegWriter {
            f,
            pos: 8,
            comp,
            frame,
            cat,
            tool_ix: HashMap::new(),
            field_ix: HashMap::new(),
            dir: Vec::new(),
            reqs: Vec::new(),
            core_buf: Vec::new(),
            core_waiting: Vec::new(),
            steps_buf: Vec::new(),
            steps_waiting: Vec::new(),
        })
    }

    /// Write a session's message stream as frames; returns (offset, compressed length) per frame.
    pub fn frames(&mut self, stream: &[u8]) -> Result<Vec<(u64, u32)>> {
        let mut out = Vec::with_capacity(stream.len().div_ceil(self.frame));
        for part in stream.chunks(self.frame) {
            let c = self.comp.compress(part)?;
            self.f.write_all(&c)?;
            out.push((self.pos, c.len() as u32));
            self.pos += c.len() as u64;
            self.cat.frame_bytes += c.len() as u64;
        }
        Ok(out)
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

    /// Compressed bytes written so far (frames and blocks).
    pub fn written(&self) -> u64 {
        self.pos
    }

    fn seal(&mut self, steps: bool) -> Result<()> {
        let (buf, waiting) = if steps { (std::mem::take(&mut self.steps_buf), std::mem::take(&mut self.steps_waiting)) } else { (std::mem::take(&mut self.core_buf), std::mem::take(&mut self.core_waiting)) };
        if waiting.is_empty() {
            return Ok(());
        }
        let r = write_block(&mut self.f, &mut self.pos, &buf, 3)?;
        self.cat.blocks.push(r);
        let b = self.cat.blocks.len() as u32 - 1;
        for i in waiting {
            if steps {
                self.dir[i].steps.0 = b;
            } else {
                self.dir[i].core.0 = b;
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
        let core_ref = (0, self.core_buf.len() as u32, e.buf.len() as u32);
        self.core_buf.extend_from_slice(&e.buf);
        self.core_waiting.push(i);
        let mut e = Enc::new();
        encode_steps(steps, &mut e);
        let steps_ref = (0, self.steps_buf.len() as u32, e.buf.len() as u32);
        self.steps_buf.extend_from_slice(&e.buf);
        self.steps_waiting.push(i);
        for (k, r) in core.requests.iter().enumerate() {
            self.reqs.push((r.id.clone(), session, core.first_req + k as u32));
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
        self.dir.len()
    }

    pub fn finish(mut self, mode: SyncMode) -> Result<SegCat> {
        self.seal(false)?;
        self.seal(true)?;
        self.reqs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for c in self.reqs.chunks(REQ_BLOCK) {
            let mut e = Enc::new();
            e.var(c.len() as u64);
            for (id, s, k) in c {
                e.bytes(id.as_bytes());
                e.var(*s as u64);
                e.var(*k as u64);
            }
            let r = write_block(&mut self.f, &mut self.pos, &e.buf, 3)?;
            self.cat.blocks.push(r);
            self.cat.reqtab.push((c[0].0.clone(), self.cat.blocks.len() as u32 - 1));
        }
        let r = write_block(&mut self.f, &mut self.pos, &encode_dir(&self.dir), 3)?;
        self.cat.blocks.push(r);
        self.cat.dir = self.cat.blocks.len() as u32 - 1;
        let mut kb = Bloom::new(self.dir.len(), BLOOM_BITS);
        for d in &self.dir {
            kb.add(key_hash(&key_bytes(&d.key.0, &d.key.1)));
        }
        let mut rb = Bloom::new(self.reqs.len(), BLOOM_BITS);
        for (id, _, _) in &self.reqs {
            rb.add(key_hash(id.as_bytes()));
        }
        let mut sb = Bloom::new(self.dir.len(), BLOOM_BITS);
        for d in &self.dir {
            sb.add(key_hash(&d.session.to_le_bytes()));
        }
        self.cat.key_bloom = kb;
        self.cat.req_bloom = rb;
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

    pub fn block(&self, i: u32) -> Result<Vec<u8>> {
        let r = self.cat.blocks.get(i as usize).ok_or_else(|| crate::codec::Error::Corrupt(format!("segment {} has no block {i}", self.num)))?;
        read_block(&*handle(self.uid, &self.path)?, r, &format!("segment {}", self.num))
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
            for k in lo..=hi {
                if out[k - first].is_some() {
                    continue;
                }
                let (o, l) = frames[k];
                let s = (o - start) as usize;
                let cap = (raw_len as usize - k * fs).min(fs);
                let d = decompress(&buf[s..s + l as usize], cap)?;
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
