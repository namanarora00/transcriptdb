//! Segment files: written once at a flush or a merge, never changed afterwards.
//!
//! `seg-N.tdb` holds, in order: an 8-byte magic; per session chunk, that session's
//! node bytes as one stream cut into fixed-size zstd frames (each with zstd's own
//! content checksum); the shared pieces, each compressed on its own; then a
//! zstd-compressed index and a footer `[index_off u64][index_len u64][crc32c u32][magic]`.
//! `seg-N.opq` holds the opaque values (signatures, images) as raw bytes.

use crate::codec::{corrupt, Dec, Enc, Result};
use crate::record::ReqRec;
use crate::steps::StepRow;
use crate::wal::{sync_file, SyncMode};
use std::cell::RefCell;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const SEG_MAGIC: &[u8; 8] = b"TDBSEG01";
const OPQ_MAGIC: &[u8; 8] = b"TDBOPQ01";
const FOOTER: usize = 8 + 8 + 4 + 8;

pub fn seg_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("seg-{num:06}.tdb"))
}

pub fn opq_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("seg-{num:06}.opq"))
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

#[derive(Debug, Clone)]
pub struct SegChunk {
    pub session: u32,
    pub raw_len: u64,
    pub frames: Vec<(u64, u32)>,
    /// In storage order.
    pub nodes: Vec<SegNode>,
}

#[derive(Debug, Clone)]
pub struct SegPiece {
    pub id: u32,
    pub kind: u8,
    pub hash: u128,
    pub off: u64,
    pub clen: u32,
    pub rawlen: u32,
}

#[derive(Debug, Clone)]
pub struct SegOpaque {
    pub id: u64,
    pub enc: u8,
    pub hash: u128,
    pub off: u64,
    pub len: u32,
}

/// One session's steps in a segment. `tool` and field/value codes are local to the segment.
#[derive(Debug, Clone)]
pub struct SegSteps {
    pub session: u32,
    pub first: u32,
    pub rows: Vec<StepRow>,
    /// Per row: (local field, local value, or `steps::LONG_BASE + i` for `longs[i]`).
    pub fields: Vec<Vec<(u32, u32)>>,
    /// (offset, length) of long values in their call's stored message.
    pub longs: Vec<(u32, u32)>,
}

#[derive(Debug, Clone, Default)]
pub struct SegIndex {
    pub max_seq: u64,
    pub frame_size: u32,
    pub sessions: Vec<(u32, String, String)>,
    pub chunks: Vec<SegChunk>,
    pub pieces: Vec<SegPiece>,
    pub opaques: Vec<SegOpaque>,
    pub requests: Vec<ReqRec>,
    pub step_tools: Vec<Vec<u8>>,
    pub step_fields: Vec<Vec<u8>>,
    /// (local field, value text)
    pub step_values: Vec<(u32, Vec<u8>)>,
    pub steps: Vec<SegSteps>,
}

impl SegIndex {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.var(self.max_seq);
        e.var(self.frame_size as u64);
        e.var(self.sessions.len() as u64);
        for (i, s, a) in &self.sessions {
            e.var(*i as u64);
            e.bytes(s.as_bytes());
            e.bytes(a.as_bytes());
        }
        e.var(self.chunks.len() as u64);
        for c in &self.chunks {
            e.var(c.session as u64);
            e.var(c.raw_len);
            e.var(c.frames.len() as u64);
            for &(o, l) in &c.frames {
                e.var(o);
                e.var(l as u64);
            }
            e.var(c.nodes.len() as u64);
            for n in &c.nodes {
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
        }
        e.var(self.pieces.len() as u64);
        for p in &self.pieces {
            e.var(p.id as u64);
            e.u8(p.kind);
            e.u128(p.hash);
            e.var(p.off);
            e.var(p.clen as u64);
            e.var(p.rawlen as u64);
        }
        e.var(self.opaques.len() as u64);
        for o in &self.opaques {
            e.var(o.id);
            e.u8(o.enc);
            e.u128(o.hash);
            e.var(o.off);
            e.var(o.len as u64);
        }
        e.var(self.requests.len() as u64);
        for r in &self.requests {
            r.encode(&mut e);
        }
        e.var(self.step_tools.len() as u64);
        for t in &self.step_tools {
            e.bytes(t);
        }
        e.var(self.step_fields.len() as u64);
        for f in &self.step_fields {
            e.bytes(f);
        }
        e.var(self.step_values.len() as u64);
        for (f, v) in &self.step_values {
            e.var(*f as u64);
            e.bytes(v);
        }
        e.var(self.steps.len() as u64);
        for c in &self.steps {
            e.var(c.session as u64);
            e.var(c.first as u64);
            e.var(c.rows.len() as u64);
            for (r, fs) in c.rows.iter().zip(&c.fields) {
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
                    if crate::steps::is_long(v) {
                        let (off, len) = c.longs[(v - crate::steps::LONG_BASE) as usize];
                        e.var(0);
                        e.var(off as u64);
                        e.var(len as u64);
                    } else {
                        e.var(v as u64 + 1);
                    }
                }
            }
        }
        e.buf
    }

    fn decode(b: &[u8]) -> Result<SegIndex> {
        let mut d = Dec::new(b);
        let max_seq = d.var()?;
        let frame_size = d.var32()?;
        let ns = d.var()? as usize;
        let mut sessions = Vec::with_capacity(ns);
        for _ in 0..ns {
            sessions.push((d.var32()?, d.string()?, d.string()?));
        }
        let nc = d.var()? as usize;
        let mut chunks = Vec::with_capacity(nc);
        for _ in 0..nc {
            let session = d.var32()?;
            let raw_len = d.var()?;
            let nf = d.var()? as usize;
            let mut frames = Vec::with_capacity(nf);
            for _ in 0..nf {
                frames.push((d.var()?, d.var32()?));
            }
            let nn = d.var()? as usize;
            let mut nodes = Vec::with_capacity(nn);
            for _ in 0..nn {
                let id = d.var32()?;
                let p = d.var()?;
                let parent = if p == 0 { u32::MAX } else { (p - 1) as u32 };
                let hash = d.u128()?;
                let off = d.var32()?;
                let len = d.var32()?;
                let nh = d.var()? as usize;
                let mut holes = Vec::with_capacity(nh);
                for _ in 0..nh {
                    holes.push((d.var32()?, d.var()?));
                }
                nodes.push(SegNode { id, parent, hash, off, len, holes });
            }
            chunks.push(SegChunk { session, raw_len, frames, nodes });
        }
        let np = d.var()? as usize;
        let mut pieces = Vec::with_capacity(np);
        for _ in 0..np {
            pieces.push(SegPiece { id: d.var32()?, kind: d.u8()?, hash: d.u128()?, off: d.var()?, clen: d.var32()?, rawlen: d.var32()? });
        }
        let no = d.var()? as usize;
        let mut opaques = Vec::with_capacity(no);
        for _ in 0..no {
            opaques.push(SegOpaque { id: d.var()?, enc: d.u8()?, hash: d.u128()?, off: d.var()?, len: d.var32()? });
        }
        let nr = d.var()? as usize;
        let mut requests = Vec::with_capacity(nr);
        for _ in 0..nr {
            requests.push(ReqRec::decode(&mut d)?);
        }
        let n = d.var()? as usize;
        let mut step_tools = Vec::with_capacity(n);
        for _ in 0..n {
            step_tools.push(d.bytes()?.to_vec());
        }
        let n = d.var()? as usize;
        let mut step_fields = Vec::with_capacity(n);
        for _ in 0..n {
            step_fields.push(d.bytes()?.to_vec());
        }
        let n = d.var()? as usize;
        let mut step_values = Vec::with_capacity(n);
        for _ in 0..n {
            step_values.push((d.var32()?, d.bytes()?.to_vec()));
        }
        let n = d.var()? as usize;
        let mut steps = Vec::with_capacity(n);
        for _ in 0..n {
            let session = d.var32()?;
            let first = d.var32()?;
            let nrow = d.var()? as usize;
            let mut rows = Vec::with_capacity(nrow);
            let mut fields = Vec::with_capacity(nrow);
            let mut longs = Vec::new();
            for _ in 0..nrow {
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
                let mut fs = Vec::with_capacity(nf);
                for _ in 0..nf {
                    let f = d.var32()?;
                    let v = d.var()?;
                    if v == 0 {
                        longs.push((d.var32()?, d.var32()?));
                        fs.push((f, crate::steps::LONG_BASE + longs.len() as u32 - 1));
                    } else {
                        fs.push((f, (v - 1) as u32));
                    }
                }
                rows.push(StepRow { call_node, result_node, prev, tool, time_ms, out_bytes, call_block, result_block, failed });
                fields.push(fs);
            }
            steps.push(SegSteps { session, first, rows, fields, longs });
        }
        if !d.done() {
            return corrupt("trailing bytes in segment index");
        }
        Ok(SegIndex { max_seq, frame_size, sessions, chunks, pieces, opaques, requests, step_tools, step_fields, step_values, steps })
    }
}

pub struct SegWriter {
    f: BufWriter<File>,
    o: BufWriter<File>,
    pos: u64,
    opos: u64,
    comp: zstd::bulk::Compressor<'static>,
    frame: usize,
}

impl SegWriter {
    pub fn create(dir: &Path, num: u32, frame: usize, level: i32) -> Result<SegWriter> {
        let mk = |p: PathBuf| OpenOptions::new().write(true).create(true).truncate(true).open(p);
        let mut f = BufWriter::with_capacity(1 << 20, mk(seg_path(dir, num))?);
        let mut o = BufWriter::with_capacity(1 << 20, mk(opq_path(dir, num))?);
        f.write_all(SEG_MAGIC)?;
        o.write_all(OPQ_MAGIC)?;
        let mut comp = zstd::bulk::Compressor::new(level)?;
        comp.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        Ok(SegWriter { f, o, pos: 8, opos: 8, comp, frame })
    }

    /// Write a session's stream as frames; returns (offset, compressed length) per frame.
    pub fn frames(&mut self, stream: &[u8]) -> Result<Vec<(u64, u32)>> {
        let mut out = Vec::with_capacity(stream.len().div_ceil(self.frame));
        for part in stream.chunks(self.frame) {
            let c = self.comp.compress(part)?;
            self.f.write_all(&c)?;
            out.push((self.pos, c.len() as u32));
            self.pos += c.len() as u64;
        }
        Ok(out)
    }

    pub fn piece(&mut self, raw: &[u8]) -> Result<(u64, u32)> {
        let c = self.comp.compress(raw)?;
        self.piece_compressed(&c)
    }

    pub fn piece_compressed(&mut self, c: &[u8]) -> Result<(u64, u32)> {
        self.f.write_all(c)?;
        let at = self.pos;
        self.pos += c.len() as u64;
        Ok((at, c.len() as u32))
    }

    pub fn opaque(&mut self, payload: &[u8]) -> Result<(u64, u32)> {
        self.o.write_all(payload)?;
        let at = self.opos;
        self.opos += payload.len() as u64;
        Ok((at, payload.len() as u32))
    }

    pub fn finish(mut self, index: &SegIndex, mode: SyncMode) -> Result<()> {
        let raw = index.encode();
        let c = zstd::bulk::compress(&raw, 3)?;
        let at = self.pos;
        self.f.write_all(&c)?;
        self.f.write_all(&at.to_le_bytes())?;
        self.f.write_all(&(c.len() as u64).to_le_bytes())?;
        self.f.write_all(&crc32c::crc32c(&c).to_le_bytes())?;
        self.f.write_all(SEG_MAGIC)?;
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        let o = self.o.into_inner().map_err(|e| e.into_error())?;
        sync_file(&o, mode)?;
        sync_file(&f, mode)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub raw_len: u64,
    pub frames: Vec<(u64, u32)>,
}

pub struct Segment {
    pub num: u32,
    /// Unique in this process (frames are cached by it).
    uid: u64,
    f: File,
    o: File,
    pub frame_size: u32,
    pub chunks: Vec<ChunkInfo>,
    pub file_bytes: u64,
    pub opq_bytes: u64,
    pub index_bytes: u64,
}

thread_local! {
    static DCTX: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
    /// Recently decompressed frames, per thread: frames never change, so this is always safe.
    static FRAMES: RefCell<FrameCache> = RefCell::new(FrameCache::default());
}

static NEXT_UID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
const FRAME_CACHE: usize = 128;

#[derive(Default)]
struct FrameCache {
    map: std::collections::HashMap<(u64, u32, usize), Arc<Vec<u8>>>,
    order: std::collections::VecDeque<(u64, u32, usize)>,
}

impl FrameCache {
    fn get(&self, k: &(u64, u32, usize)) -> Option<Arc<Vec<u8>>> {
        self.map.get(k).cloned()
    }
    fn put(&mut self, k: (u64, u32, usize), v: Arc<Vec<u8>>) {
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
    pub fn open(dir: &Path, num: u32) -> Result<(Segment, SegIndex)> {
        let f = File::open(seg_path(dir, num))?;
        let o = File::open(opq_path(dir, num))?;
        let len = f.metadata()?.len();
        let opq_bytes = o.metadata()?.len();
        if len < (8 + FOOTER) as u64 {
            return corrupt(format!("segment {num} is too short"));
        }
        let mut head = [0u8; 8];
        f.read_exact_at(&mut head, 0)?;
        let mut foot = [0u8; FOOTER];
        f.read_exact_at(&mut foot, len - FOOTER as u64)?;
        if &head != SEG_MAGIC || &foot[20..] != SEG_MAGIC {
            return corrupt(format!("segment {num} has a bad magic"));
        }
        let at = u64::from_le_bytes(foot[0..8].try_into().unwrap());
        let clen = u64::from_le_bytes(foot[8..16].try_into().unwrap());
        let crc = u32::from_le_bytes(foot[16..20].try_into().unwrap());
        if at + clen + FOOTER as u64 != len {
            return corrupt(format!("segment {num} footer does not match its size"));
        }
        let mut c = vec![0u8; clen as usize];
        f.read_exact_at(&mut c, at)?;
        if crc32c::crc32c(&c) != crc {
            return corrupt(format!("segment {num} index checksum mismatch"));
        }
        let raw = zstd::stream::decode_all(&c[..])?;
        let index = SegIndex::decode(&raw)?;
        let chunks = index.chunks.iter().map(|c| ChunkInfo { raw_len: c.raw_len, frames: c.frames.clone() }).collect();
        let uid = NEXT_UID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let seg = Segment { num, uid, f, o, frame_size: index.frame_size, chunks, file_bytes: len, opq_bytes, index_bytes: clen };
        Ok((seg, index))
    }

    /// Frames `first..=last` of a chunk, decompressed: from the cache, or with one read for
    /// the run of frames that are not cached.
    pub fn frames(&self, chunk: u32, first: usize, last: usize) -> Result<Vec<Arc<Vec<u8>>>> {
        let mut out: Vec<Option<Arc<Vec<u8>>>> = (first..=last).map(|k| FRAMES.with(|c| c.borrow().get(&(self.uid, chunk, k)))).collect();
        let missing: Vec<usize> = (first..=last).filter(|k| out[k - first].is_none()).collect();
        if let (Some(&lo), Some(&hi)) = (missing.first(), missing.last()) {
            let ch = &self.chunks[chunk as usize];
            let start = ch.frames[lo].0;
            let end = ch.frames[hi].0 + ch.frames[hi].1 as u64;
            let mut buf = vec![0u8; (end - start) as usize];
            self.f.read_exact_at(&mut buf, start)?;
            let fs = self.frame_size as usize;
            for k in lo..=hi {
                if out[k - first].is_some() {
                    continue;
                }
                let (o, l) = ch.frames[k];
                let s = (o - start) as usize;
                let cap = (ch.raw_len as usize - k * fs).min(fs);
                let d = decompress(&buf[s..s + l as usize], cap)?;
                if d.len() != cap {
                    return corrupt(format!("segment {} frame {k} has the wrong size", self.num));
                }
                let d = Arc::new(d);
                FRAMES.with(|c| c.borrow_mut().put((self.uid, chunk, k), d.clone()));
                out[k - first] = Some(d);
            }
        }
        Ok(out.into_iter().map(|x| x.unwrap()).collect())
    }

    pub fn piece_raw(&self, off: u64, clen: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; clen as usize];
        self.f.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }

    pub fn piece(&self, off: u64, clen: u32, rawlen: u32) -> Result<Vec<u8>> {
        let c = self.piece_raw(off, clen)?;
        let d = decompress(&c, rawlen as usize)?;
        if d.len() != rawlen as usize {
            return corrupt("piece has the wrong size");
        }
        Ok(d)
    }

    pub fn opaque(&self, off: u64, len: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        self.o.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }
}
