//! The single-node store: write path, flush, merge, recovery and reads.
//!
//! Write path for one request: split the body ([`crate::split`]), walk the messages
//! down the session's tree comparing hashes, keep only the messages the session has
//! not sent before as new nodes, cut opaque values out of those, intern the skeleton,
//! system, tools and cache-marker pieces, then append one log record. The record is
//! durable once [`Db::commit`] returns; that is the moment a request counts as saved.
//!
//! Everything in memory that is not yet in a segment can be rebuilt from the log,
//! because the in-memory state only ever changes by applying a log record
//! ([`Db::apply`]), both live and during recovery.

use crate::codec::{corrupt, Error, Result};
use crate::layout;
use crate::manifest::Manifest;
use crate::record::*;
use crate::segment::{opq_path, seg_path, SegChunk, SegIndex, SegNode, SegOpaque, SegPiece, SegSteps, SegWriter, Segment};
use crate::steps::{self, SessionSteps, StepDicts, StepRow};
use crate::split::{self, HoleKind, OpaqueEnc, Skeleton};
use crate::wal::{self, SyncMode, WalWriter};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::{xxh3_128, xxh3_128_with_seed};

#[derive(Debug, Clone)]
pub struct Options {
    /// Uncompressed bytes per zstd frame in a session stream.
    pub frame_size: usize,
    /// Flush the in-memory part to a segment once it holds this many bytes.
    pub flush_bytes: usize,
    pub sync: SyncMode,
    pub level: i32,
    /// Check, before logging, that the split pieces join back to the exact body.
    pub verify_split: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options { frame_size: 64 * 1024, flush_bytes: 32 << 20, sync: SyncMode::Full, level: 3, verify_split: true }
    }
}

/// One request as it arrives: the exact body the model API received, plus who sent it.
#[derive(Debug, Clone, Copy)]
pub struct Put<'a> {
    pub session: &'a str,
    /// Empty for the main agent; a subagent id otherwise.
    pub agent: &'a str,
    pub id: &'a str,
    pub time: &'a str,
    pub meta: &'a [u8],
    pub body: &'a [u8],
    /// sha256 of `body`, if the caller already has it (saves hashing twice).
    pub digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Stored(u32),
    /// A request with this id is already stored; nothing was written.
    Duplicate(u32),
}

#[derive(Debug, Clone, Copy)]
enum Loc {
    Mem(u32),
    Seg { seg: u32, chunk: u32, off: u32 },
}

const UNSET: Loc = Loc::Mem(u32::MAX);

#[derive(Debug, Clone)]
struct NodeMeta {
    parent: u32,
    len: u32,
    hash: u128,
    loc: Loc,
    holes: Option<Box<[(u32, u64)]>>,
}

#[derive(Debug, Default)]
struct Session {
    key: (String, String),
    declared: bool,
    nodes: Vec<NodeMeta>,
    children: HashMap<(u32, u128), u32>,
    /// Nodes `0..flushed` are in segments.
    flushed: u32,
    steps: SessionSteps,
}

#[derive(Debug, Clone, Copy)]
enum PLoc {
    Mem(u32),
    Seg { seg: u32, off: u64, clen: u32, rawlen: u32 },
}

#[derive(Debug, Clone, Copy)]
struct PieceMeta {
    kind: u8,
    hash: u128,
    loc: PLoc,
}

#[derive(Debug, Clone, Copy)]
enum OLoc {
    Mem(u32),
    Seg { seg: u32, off: u64, len: u32 },
}

#[derive(Debug, Clone, Copy)]
struct OpaqueMeta {
    enc: u8,
    hash: u128,
    loc: OLoc,
}

#[derive(Default)]
struct Mem {
    nodes: Vec<Box<[u8]>>,
    pieces: Vec<Arc<[u8]>>,
    opaques: Vec<Box<[u8]>>,
    bytes: usize,
    first_request: usize,
    first_piece: usize,
    first_opaque: usize,
    touched: BTreeSet<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub segments: usize,
    pub wal_files: usize,
    pub replayed: usize,
    pub skipped: usize,
    pub torn_bytes: u64,
    pub orphans_removed: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ReadStats {
    /// Separate disk reads (one per run of adjacent frames).
    pub reads: u32,
    pub frames: u32,
    /// Runs of messages that sit next to each other in storage.
    pub runs: u32,
    pub opaque_reads: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub sessions: usize,
    pub nodes: usize,
    pub requests: usize,
    pub pieces: usize,
    pub opaques: usize,
    pub segments: usize,
    /// Uncompressed bytes of all stored messages (after cutting markers and opaque values).
    pub node_bytes: u64,
    /// Compressed frames holding those messages.
    pub frame_bytes: u64,
    pub piece_raw_bytes: u64,
    pub piece_bytes: u64,
    pub opaque_bytes: u64,
    pub index_bytes: u64,
    pub seg_file_bytes: u64,
    pub opq_file_bytes: u64,
    pub wal_bytes: u64,
    pub mem_bytes: usize,
    pub steps: usize,
    pub tools: usize,
    pub step_fields: usize,
    pub step_values: usize,
    pub orphan_results: usize,
}

pub struct Db {
    dir: PathBuf,
    opts: Options,
    manifest: Manifest,
    segs: HashMap<u32, Segment>,
    sessions: Vec<Session>,
    session_ix: HashMap<(String, String), u32>,
    pieces: Vec<PieceMeta>,
    piece_ix: HashMap<u128, u32>,
    opaques: Vec<OpaqueMeta>,
    opaque_ix: HashMap<u128, u64>,
    requests: Vec<ReqRec>,
    request_ix: HashMap<Box<str>, u32>,
    mem: Mem,
    wal: WalWriter,
    seq: u64,
    piece_cache: Mutex<HashMap<u32, Arc<[u8]>>>,
    skel_cache: Mutex<HashMap<u32, Arc<Skeleton>>>,
    sd: StepDicts,
    /// Tool results whose call could not be found on their path.
    orphan_results: usize,
    pub recovery: RecoveryReport,
}

fn piece_hash(kind: u8, b: &[u8]) -> u128 {
    xxh3_128_with_seed(b, kind as u64)
}

fn opaque_hash(enc: u8, b: &[u8]) -> u128 {
    xxh3_128_with_seed(b, 0x6f70_0000 | enc as u64)
}

pub fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

fn parse_num(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?.strip_suffix(suffix)?.parse().ok()
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let _ = fs::remove_file(dir.join("MANIFEST.tmp"));
        let manifest = match Manifest::load(&dir)? {
            Some(m) => m,
            None => {
                let m = Manifest { generation: 0, checkpoint: 0, wal_start: 1, next_seg: 1, segs: Vec::new() };
                m.store(&dir, opts.sync)?;
                m
            }
        };
        let mut rec = RecoveryReport::default();
        let live: BTreeSet<u32> = manifest.segs.iter().copied().collect();
        let mut wal_nums = Vec::new();
        for e in fs::read_dir(&dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            let seg = parse_num(&name, "seg-", ".tdb").or_else(|| parse_num(&name, "seg-", ".opq"));
            if let Some(n) = seg {
                if !live.contains(&(n as u32)) {
                    fs::remove_file(dir.join(&name))?;
                    rec.orphans_removed += 1;
                }
            } else if let Some(n) = parse_num(&name, "wal-", ".log") {
                if n < manifest.wal_start {
                    fs::remove_file(dir.join(&name))?;
                    rec.orphans_removed += 1;
                } else {
                    wal_nums.push(n);
                }
            }
        }
        wal_nums.sort_unstable();
        let new_wal = wal_nums.last().map(|n| n + 1).unwrap_or(manifest.wal_start).max(manifest.wal_start);
        let wal = WalWriter::create(&dir, new_wal, opts.sync)?;

        let mut db = Db {
            dir,
            opts,
            manifest,
            segs: HashMap::new(),
            sessions: Vec::new(),
            session_ix: HashMap::new(),
            pieces: Vec::new(),
            piece_ix: HashMap::new(),
            opaques: Vec::new(),
            opaque_ix: HashMap::new(),
            requests: Vec::new(),
            request_ix: HashMap::new(),
            mem: Mem::default(),
            wal,
            seq: 0,
            piece_cache: Mutex::new(HashMap::new()),
            skel_cache: Mutex::new(HashMap::new()),
            sd: StepDicts::default(),
            orphan_results: 0,
            recovery: RecoveryReport::default(),
        };

        let mut segs: Vec<u32> = db.manifest.segs.clone();
        segs.sort_unstable();
        for n in segs {
            let (seg, index) = Segment::open(&db.dir, n)?;
            db.load_segment(n, index)?;
            db.segs.insert(n, seg);
            rec.segments += 1;
        }
        db.finish_load()?;
        db.seq = db.manifest.checkpoint;

        let logs: Vec<(u64, wal::WalContents)> =
            wal_nums.iter().map(|&n| Ok((n, wal::read_wal(&wal::wal_path(&db.dir, n))?))).collect::<Result<_>>()?;
        for (k, (n, c)) in logs.iter().enumerate() {
            let (n, p) = (*n, wal::wal_path(&db.dir, *n));
            rec.wal_files += 1;
            for r in &c.records {
                let m = Mutation::decode(r)?;
                if m.seq <= db.manifest.checkpoint {
                    rec.skipped += 1;
                    continue;
                }
                db.apply(m)?;
                rec.replayed += 1;
            }
            if c.torn {
                // A torn tail is the last thing written before a crash. Newer log files
                // may exist (a crash during recovery creates one) but must be empty.
                if logs[k + 1..].iter().any(|(_, later)| !later.records.is_empty()) {
                    return corrupt(format!("log file {n} is damaged before its end"));
                }
                let len = fs::metadata(&p)?.len();
                rec.torn_bytes = len - c.valid_len;
                wal::truncate(&p, c.valid_len, db.opts.sync)?;
            }
        }
        db.recovery = rec;
        Ok(db)
    }

    fn session_slot(&mut self, idx: u32) -> &mut Session {
        let i = idx as usize;
        if self.sessions.len() <= i {
            self.sessions.resize_with(i + 1, Session::default);
        }
        &mut self.sessions[i]
    }

    fn load_segment(&mut self, num: u32, ix: SegIndex) -> Result<()> {
        for (i, s, a) in ix.sessions {
            let slot = self.session_slot(i);
            if slot.declared && slot.key != (s.clone(), a.clone()) {
                return corrupt("session declared twice with different keys");
            }
            slot.key = (s, a);
            slot.declared = true;
        }
        for (ci, c) in ix.chunks.into_iter().enumerate() {
            let sess = self.session_slot(c.session);
            for n in c.nodes {
                let i = n.id as usize;
                if sess.nodes.len() <= i {
                    sess.nodes.resize(i + 1, NodeMeta { parent: NONE, len: 0, hash: 0, loc: UNSET, holes: None });
                }
                sess.nodes[i] = NodeMeta {
                    parent: n.parent,
                    len: n.len,
                    hash: n.hash,
                    loc: Loc::Seg { seg: num, chunk: ci as u32, off: n.off },
                    holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) },
                };
            }
        }
        for p in ix.pieces {
            let i = p.id as usize;
            if self.pieces.len() <= i {
                self.pieces.resize(i + 1, PieceMeta { kind: 0, hash: 0, loc: PLoc::Mem(u32::MAX) });
            }
            self.pieces[i] = PieceMeta { kind: p.kind, hash: p.hash, loc: PLoc::Seg { seg: num, off: p.off, clen: p.clen, rawlen: p.rawlen } };
        }
        for o in ix.opaques {
            let i = o.id as usize;
            if self.opaques.len() <= i {
                self.opaques.resize(i + 1, OpaqueMeta { enc: 0, hash: 0, loc: OLoc::Mem(u32::MAX) });
            }
            self.opaques[i] = OpaqueMeta { enc: o.enc, hash: o.hash, loc: OLoc::Seg { seg: num, off: o.off, len: o.len } };
        }
        for r in ix.requests {
            let i = self.requests.len() as u32;
            if self.request_ix.insert(r.id.clone(), i).is_some() {
                return corrupt(format!("request {} stored twice", r.id));
            }
            self.requests.push(r);
        }
        // steps: map the segment's own codes to this store's dictionaries
        let tools: Vec<u32> = ix.step_tools.iter().map(|t| self.sd.tools.intern(t)).collect();
        let fields: Vec<u32> = ix.step_fields.iter().map(|f| self.sd.field(f)).collect();
        let mut values = Vec::with_capacity(ix.step_values.len());
        for (f, v) in &ix.step_values {
            let gf = *fields.get(*f as usize).ok_or_else(|| Error::Corrupt("step value names a missing field".into()))?;
            values.push(self.sd.value(gf, v));
        }
        for c in ix.steps {
            self.session_slot(c.session);
            let ss = &mut self.sessions[c.session as usize].steps;
            if c.first as usize != ss.rows.len() {
                return corrupt("steps out of order across segments");
            }
            let mut fs = Vec::new();
            for (mut r, rf) in c.rows.into_iter().zip(c.fields) {
                r.tool = *tools.get(r.tool as usize).ok_or_else(|| Error::Corrupt("step names a missing tool".into()))?;
                fs.clear();
                for (f, v) in rf {
                    let gf = *fields.get(f as usize).ok_or_else(|| Error::Corrupt("step names a missing field".into()))?;
                    let gv = if v == steps::LONG { steps::LONG } else { *values.get(v as usize).ok_or_else(|| Error::Corrupt("step names a missing value".into()))? };
                    fs.push((gf, gv));
                }
                self.sd.push(c.session, ss, r, &fs);
            }
        }
        Ok(())
    }

    fn finish_load(&mut self) -> Result<()> {
        for (si, s) in self.sessions.iter_mut().enumerate() {
            if !s.declared {
                return corrupt(format!("session {si} has no declaration"));
            }
            s.children.reserve(s.nodes.len());
            for (i, n) in s.nodes.iter().enumerate() {
                if matches!(n.loc, Loc::Mem(u32::MAX)) {
                    return corrupt(format!("session {si} is missing node {i}"));
                }
                s.children.insert((n.parent, n.hash), i as u32);
            }
            s.flushed = s.nodes.len() as u32;
            self.session_ix.insert(s.key.clone(), si as u32);
            // per node: last step at or above it on its path
            let mut last_at = vec![NONE; s.nodes.len()];
            for (pos, r) in s.steps.rows.iter().enumerate() {
                let n = r.result_node as usize;
                if n >= s.nodes.len() || r.call_node as usize >= s.nodes.len() {
                    return corrupt(format!("session {si} has a step past its nodes"));
                }
                last_at[n] = pos as u32;
            }
            s.steps.tail = Vec::with_capacity(s.nodes.len());
            for (i, n) in s.nodes.iter().enumerate() {
                let t = if last_at[i] != NONE {
                    last_at[i]
                } else if n.parent == NONE {
                    NONE
                } else {
                    s.steps.tail[n.parent as usize]
                };
                s.steps.tail.push(t);
            }
            s.steps.flushed = s.steps.rows.len() as u32;
        }
        for (i, p) in self.pieces.iter().enumerate() {
            if matches!(p.loc, PLoc::Mem(u32::MAX)) {
                return corrupt(format!("missing piece {i}"));
            }
            self.piece_ix.insert(p.hash, i as u32);
        }
        for (i, o) in self.opaques.iter().enumerate() {
            if matches!(o.loc, OLoc::Mem(u32::MAX)) {
                return corrupt(format!("missing opaque value {i}"));
            }
            self.opaque_ix.insert(o.hash, i as u64);
        }
        for r in &self.requests {
            let s = self.sessions.get(r.session as usize).ok_or_else(|| Error::Corrupt("request names a missing session".into()))?;
            if r.tip != NONE && r.tip as usize >= s.nodes.len() {
                return corrupt("request points past its session");
            }
        }
        self.mem = Mem {
            first_request: self.requests.len(),
            first_piece: self.pieces.len(),
            first_opaque: self.opaques.len(),
            ..Mem::default()
        };
        Ok(())
    }

    // ------------------------------------------------------------------ writes

    fn piece_id(&self, kind: u8, b: &[u8], m: &mut Mutation, pending: &mut HashMap<u128, u32>) -> u32 {
        let h = piece_hash(kind, b);
        if let Some(&i) = self.piece_ix.get(&h) {
            return i;
        }
        if let Some(&i) = pending.get(&h) {
            return i;
        }
        let id = (self.pieces.len() + m.pieces.len()) as u32;
        m.pieces.push((id, kind, h, b.to_vec()));
        pending.insert(h, id);
        id
    }

    fn opaque_id(&self, enc: OpaqueEnc, payload: Vec<u8>, m: &mut Mutation, pending: &mut HashMap<u128, u64>) -> u64 {
        let h = opaque_hash(enc as u8, &payload);
        if let Some(&i) = self.opaque_ix.get(&h) {
            return i;
        }
        if let Some(&i) = pending.get(&h) {
            return i;
        }
        let id = (self.opaques.len() + m.opaques.len()) as u64;
        m.opaques.push((id, enc as u8, h, payload));
        pending.insert(h, id);
        id
    }

    /// Add one request. It is visible to reads at once and durable after [`Db::commit`].
    pub fn put(&mut self, p: &Put) -> Result<PutOutcome> {
        if let Some(&i) = self.request_ix.get(p.id) {
            return Ok(PutOutcome::Duplicate(i));
        }
        let sp = split::split(p.body)?;
        if self.opts.verify_split && split::join(&sp)? != p.body {
            return Err(Error::BadRequest("split pieces do not join back to the body".into()));
        }
        let digest = p.digest.unwrap_or_else(|| sha256(p.body));
        let key = (p.session.to_string(), p.agent.to_string());
        let existing = self.session_ix.get(&key).copied();
        let sidx = existing.unwrap_or(self.sessions.len() as u32);
        let mut m = Mutation {
            seq: self.seq + 1,
            new_session: if existing.is_none() { Some((sidx, key.0, key.1)) } else { None },
            pieces: Vec::new(),
            opaques: Vec::new(),
            session: sidx,
            nodes: Vec::new(),
            req: ReqRec {
                id: p.id.into(),
                time: p.time.into(),
                meta: p.meta.into(),
                session: sidx,
                tip: NONE,
                n_msgs: sp.messages.len() as u32,
                skeleton: 0,
                system: NONE,
                tools: NONE,
                notes: Box::new([]),
                digest,
                layout: sp.layout.as_ref().map(|g| g.iter().map(|x| x.clone().into_boxed_slice()).collect()),
            },
        };
        let mut pp = HashMap::new();
        let mut po = HashMap::new();

        // walk the session tree
        let sess = existing.map(|i| &self.sessions[i as usize]);
        let mut next = sess.map(|s| s.nodes.len() as u32).unwrap_or(0);
        let mut parent = NONE;
        let mut creating = sess.is_none();
        for msg in &sp.messages {
            let h = xxh3_128(&msg.bytes);
            if !creating {
                if let Some(&c) = sess.unwrap().children.get(&(parent, h)) {
                    parent = c;
                    continue;
                }
                creating = true;
            }
            let (cut, holes) = split::cut_opaque(&msg.bytes)?;
            let mut hl = Vec::with_capacity(holes.len());
            for oh in holes {
                let oid = self.opaque_id(oh.enc, oh.payload, &mut m, &mut po);
                hl.push((oh.offset, oid));
            }
            m.nodes.push(NewNode { id: next, parent, hash: h, bytes: cut, holes: hl });
            parent = next;
            next += 1;
        }
        m.req.tip = parent;

        let mut notes = Vec::new();
        if let Some(sys) = &sp.system {
            m.req.system = self.piece_id(PIECE_SYSTEM, &sys.bytes, &mut m, &mut pp);
            for n in &sys.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp);
                notes.push(ReqNote { target: TARGET_SYSTEM, offset: n.offset, text: t });
            }
        }
        if let Some(tools) = &sp.tools {
            m.req.tools = self.piece_id(PIECE_TOOLS, &tools.bytes, &mut m, &mut pp);
            for n in &tools.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp);
                notes.push(ReqNote { target: TARGET_TOOLS, offset: n.offset, text: t });
            }
        }
        for (i, msg) in sp.messages.iter().enumerate() {
            for n in &msg.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp);
                notes.push(ReqNote { target: TARGET_MSG0 + i as u32, offset: n.offset, text: t });
            }
        }
        m.req.notes = notes.into_boxed_slice();
        m.req.skeleton = self.piece_id(PIECE_SKELETON, &sp.skeleton.encode(), &mut m, &mut pp);

        let idx = self.requests.len() as u32;
        self.wal.append(&m.encode());
        self.apply(m)?;
        Ok(PutOutcome::Stored(idx))
    }

    /// Change the in-memory state by one log record. The only way state changes.
    fn apply(&mut self, m: Mutation) -> Result<()> {
        if m.seq <= self.seq {
            return corrupt(format!("log record {} after {}", m.seq, self.seq));
        }
        if let Some((i, s, a)) = m.new_session {
            if i as usize != self.sessions.len() {
                return corrupt("new session out of order");
            }
            self.session_ix.insert((s.clone(), a.clone()), i);
            self.sessions.push(Session { key: (s, a), declared: true, ..Session::default() });
        }
        for (id, kind, h, b) in m.pieces {
            if id as usize != self.pieces.len() {
                return corrupt("piece out of order");
            }
            self.mem.bytes += b.len();
            self.pieces.push(PieceMeta { kind, hash: h, loc: PLoc::Mem(self.mem.pieces.len() as u32) });
            self.mem.pieces.push(b.into());
            self.piece_ix.insert(h, id);
        }
        for (id, enc, h, b) in m.opaques {
            if id as usize != self.opaques.len() {
                return corrupt("opaque value out of order");
            }
            self.mem.bytes += b.len();
            self.opaques.push(OpaqueMeta { enc, hash: h, loc: OLoc::Mem(self.mem.opaques.len() as u32) });
            self.mem.opaques.push(b.into_boxed_slice());
            self.opaque_ix.insert(h, id);
        }
        let time_ms = steps::parse_time_ms(&m.req.time);
        if m.session as usize >= self.sessions.len() {
            return corrupt("record names a missing session");
        }
        for n in m.nodes {
            let id = n.id;
            let sess = &mut self.sessions[m.session as usize];
            if n.id as usize != sess.nodes.len() {
                return corrupt("node out of order");
            }
            if n.parent != NONE && n.parent >= n.id {
                return corrupt("node parent after the node");
            }
            self.mem.bytes += n.bytes.len() + 64;
            sess.children.insert((n.parent, n.hash), n.id);
            sess.nodes.push(NodeMeta {
                parent: n.parent,
                len: n.bytes.len() as u32,
                hash: n.hash,
                loc: Loc::Mem(self.mem.nodes.len() as u32),
                holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) },
            });
            self.mem.nodes.push(n.bytes.into_boxed_slice());
            self.add_node_steps(m.session, id, time_ms)?;
        }
        let sess = &self.sessions[m.session as usize];
        if m.req.tip != NONE && m.req.tip as usize >= sess.nodes.len() {
            return corrupt("request tip past the session");
        }
        self.mem.touched.insert(m.session);
        self.mem.bytes += 256;
        let i = self.requests.len() as u32;
        self.request_ix.insert(m.req.id.clone(), i);
        self.requests.push(m.req);
        self.seq = m.seq;
        Ok(())
    }

    fn opaque_text_len(&self, id: u64) -> u64 {
        let Some(om) = self.opaques.get(id as usize) else { return 0 };
        let n = match om.loc {
            OLoc::Seg { len, .. } => len as u64,
            OLoc::Mem(mi) => self.mem.opaques[mi as usize].len() as u64,
        };
        if om.enc == 0 { n.div_ceil(3) * 4 } else { n }
    }

    /// Whether `anc` is on the path from the root to `from` (inclusive).
    fn on_path(&self, s: &Session, anc: u32, mut from: u32) -> bool {
        while from != NONE && from >= anc {
            if from == anc {
                return true;
            }
            from = s.nodes[from as usize].parent;
        }
        false
    }

    /// Walk up from `from` looking for the message that holds tool call `id`.
    #[allow(clippy::type_complexity)]
    fn find_call(&self, si: u32, mut from: u32, id: &[u8]) -> Result<Option<(u32, u16, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>> {
        let s = &self.sessions[si as usize];
        let mut cache = HashMap::new();
        let mut buf = Vec::new();
        let mut hops = 0;
        while from != NONE && hops < 4096 {
            let nm = &s.nodes[from as usize];
            buf.clear();
            self.stored_bytes(nm, &mut cache, &mut buf)?;
            let ex = steps::extract(&buf);
            if let Some(c) = ex.calls.into_iter().find(|c| c.0 == id) {
                return Ok(Some((from, c.1, c.2, c.3)));
            }
            from = nm.parent;
            hops += 1;
        }
        Ok(None)
    }

    /// Pair the tool results in node `id` with their calls, and remember its calls.
    fn add_node_steps(&mut self, si: u32, id: u32, time_ms: i64) -> Result<()> {
        let s = &self.sessions[si as usize];
        let nm = &s.nodes[id as usize];
        let Loc::Mem(mi) = nm.loc else { return corrupt("new node is not in memory") };
        let ex = steps::extract(&self.mem.nodes[mi as usize]);
        let parent = nm.parent;
        let holes: Vec<(u32, u64)> = nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default();
        let mut found = Vec::with_capacity(ex.results.len());
        for (block, tuid, failed, span) in ex.results {
            let s = &self.sessions[si as usize];
            let cached = s.steps.calls.get(&tuid[..]).filter(|c| self.on_path(s, c.node, parent)).cloned();
            let call = match cached {
                Some(c) => Some(c),
                None => match self.find_call(si, parent, &tuid)? {
                    Some((n, b, name, fields)) => {
                        let ci = self.sd.call_info(n, b, &name, &fields);
                        self.sessions[si as usize].steps.calls.insert(tuid.clone().into(), ci.clone());
                        Some(ci)
                    }
                    None => None,
                },
            };
            let Some(call) = call else {
                self.orphan_results += 1;
                continue;
            };
            let extra: u64 = holes.iter().filter(|h| (h.0 as usize) >= span.start && (h.0 as usize) <= span.end).map(|h| self.opaque_text_len(h.1)).sum();
            found.push((call, block, failed, (span.len() as u64 + extra).min(u32::MAX as u64) as u32));
        }
        for (tid, block, name, fields) in ex.calls {
            let ci = self.sd.call_info(id, block, &name, &fields);
            self.sessions[si as usize].steps.calls.insert(tid.into(), ci);
        }
        let sess = &mut self.sessions[si as usize];
        if sess.steps.tail.len() != id as usize {
            return corrupt("step tails out of step with nodes");
        }
        let mut tail = if parent == NONE { NONE } else { sess.steps.tail[parent as usize] };
        for (call, block, failed, out_bytes) in found {
            let prev = sess.steps.tail[call.node as usize];
            let row = StepRow { call_node: call.node, result_node: id, prev, tool: call.tool, time_ms, out_bytes, call_block: call.block, result_block: block, failed };
            tail = self.sd.push(si, &mut sess.steps, row, &call.fields);
        }
        sess.steps.tail.push(tail);
        Ok(())
    }

    /// The steps of sessions `which` from their given first step, with codes local to one segment.
    fn steps_for_segment(&self, which: &[(u32, u32)], ix: &mut SegIndex) {
        let mut tl: HashMap<u32, u32> = HashMap::new();
        let mut fl: HashMap<u32, u32> = HashMap::new();
        let mut vl: HashMap<(u32, u32), u32> = HashMap::new();
        for &(s, first) in which {
            let ss = &self.sessions[s as usize].steps;
            if first as usize >= ss.rows.len() {
                continue;
            }
            let mut rows = Vec::with_capacity(ss.rows.len() - first as usize);
            let mut fields = Vec::with_capacity(rows.capacity());
            for pos in first..ss.rows.len() as u32 {
                let mut r = ss.rows[pos as usize];
                r.tool = *tl.entry(r.tool).or_insert_with(|| {
                    ix.step_tools.push(self.sd.tools.get(r.tool).to_vec());
                    ix.step_tools.len() as u32 - 1
                });
                let mut fs = Vec::new();
                for &(f, v) in ss.fields_of(pos) {
                    let lf = *fl.entry(f).or_insert_with(|| {
                        ix.step_fields.push(self.sd.fields.get(f).to_vec());
                        ix.step_fields.len() as u32 - 1
                    });
                    let lv = if v == steps::LONG {
                        steps::LONG
                    } else {
                        *vl.entry((f, v)).or_insert_with(|| {
                            ix.step_values.push((lf, self.sd.values[f as usize].get(v).to_vec()));
                            ix.step_values.len() as u32 - 1
                        })
                    };
                    fs.push((lf, lv));
                }
                rows.push(r);
                fields.push(fs);
            }
            ix.steps.push(SegSteps { session: s, first, rows, fields });
        }
    }

    /// Make every `put` so far durable. After this returns they count as saved.
    pub fn commit(&mut self) -> Result<()> {
        self.wal.commit()?;
        if self.mem.bytes >= self.opts.flush_bytes {
            self.flush()?;
        }
        Ok(())
    }

    pub fn put_batch(&mut self, reqs: &[Put]) -> Result<Vec<PutOutcome>> {
        let mut out = Vec::with_capacity(reqs.len());
        for r in reqs {
            out.push(self.put(r)?);
        }
        self.commit()?;
        Ok(out)
    }

    fn sorted_sessions(&self, ids: impl Iterator<Item = u32>) -> Vec<u32> {
        let mut v: Vec<u32> = ids.collect();
        v.sort_by(|&a, &b| self.sessions[a as usize].key.cmp(&self.sessions[b as usize].key));
        v
    }

    /// Write everything in memory to a new segment and trim the log.
    pub fn flush(&mut self) -> Result<()> {
        self.wal.commit()?;
        if self.requests.len() == self.mem.first_request {
            return Ok(());
        }
        let new_wal = self.wal.num + 1;
        self.wal = WalWriter::create(&self.dir, new_wal, self.opts.sync)?;
        let num = self.manifest.next_seg;
        let mut w = SegWriter::create(&self.dir, num, self.opts.frame_size, self.opts.level)?;
        let mut ix = SegIndex { max_seq: self.seq, frame_size: self.opts.frame_size as u32, ..SegIndex::default() };

        let mut tips: HashMap<u32, Vec<u32>> = HashMap::new();
        for r in &self.requests[self.mem.first_request..] {
            tips.entry(r.session).or_default().push(r.tip);
        }
        for s in self.sorted_sessions(self.mem.touched.iter().copied()) {
            let sess = &self.sessions[s as usize];
            ix.sessions.push((s, sess.key.0.clone(), sess.key.1.clone()));
            let first = sess.flushed;
            let n = sess.nodes.len() as u32 - first;
            if n == 0 {
                continue;
            }
            let t = tips.get(&s).map(|v| v.as_slice()).unwrap_or(&[]);
            let order = layout::heavy_first(first, n, |id| sess.nodes[id as usize].parent, t.iter().copied());
            let mut stream = Vec::new();
            let mut nodes = Vec::with_capacity(order.len());
            for id in order {
                let nm = &sess.nodes[id as usize];
                let Loc::Mem(mi) = nm.loc else { return corrupt("unflushed node is not in memory") };
                nodes.push(SegNode {
                    id,
                    parent: nm.parent,
                    hash: nm.hash,
                    off: stream.len() as u32,
                    len: nm.len,
                    holes: nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default(),
                });
                stream.extend_from_slice(&self.mem.nodes[mi as usize]);
            }
            if stream.len() > u32::MAX as usize {
                return Err(Error::BadRequest("a session chunk is over 4 GiB".into()));
            }
            let frames = w.frames(&stream)?;
            ix.chunks.push(SegChunk { session: s, raw_len: stream.len() as u64, frames, nodes });
        }
        for id in self.mem.first_piece..self.pieces.len() {
            let pm = self.pieces[id];
            let PLoc::Mem(mi) = pm.loc else { return corrupt("unflushed piece is not in memory") };
            let raw = &self.mem.pieces[mi as usize];
            let (off, clen) = w.piece(raw)?;
            ix.pieces.push(SegPiece { id: id as u32, kind: pm.kind, hash: pm.hash, off, clen, rawlen: raw.len() as u32 });
        }
        for id in self.mem.first_opaque..self.opaques.len() {
            let om = self.opaques[id];
            let OLoc::Mem(mi) = om.loc else { return corrupt("unflushed opaque value is not in memory") };
            let (off, len) = w.opaque(&self.mem.opaques[mi as usize])?;
            ix.opaques.push(SegOpaque { id: id as u64, enc: om.enc, hash: om.hash, off, len });
        }
        ix.requests = self.requests[self.mem.first_request..].to_vec();
        let which: Vec<(u32, u32)> = self.sorted_sessions(self.mem.touched.iter().copied()).into_iter().map(|s| (s, self.sessions[s as usize].steps.flushed)).collect();
        self.steps_for_segment(&which, &mut ix);
        w.finish(&ix, self.opts.sync)?;

        let mut man = self.manifest.clone();
        man.generation += 1;
        man.checkpoint = self.seq;
        man.wal_start = new_wal;
        man.next_seg = num + 1;
        man.segs.push(num);
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;

        let (seg, _) = Segment::open(&self.dir, num)?;
        self.relocate(num, &ix);
        for &s in &self.mem.touched {
            let sess = &mut self.sessions[s as usize];
            sess.flushed = sess.nodes.len() as u32;
            sess.steps.flushed = sess.steps.rows.len() as u32;
        }
        self.segs.insert(num, seg);
        self.mem = Mem {
            first_request: self.requests.len(),
            first_piece: self.pieces.len(),
            first_opaque: self.opaques.len(),
            ..Mem::default()
        };
        self.remove_old_wal(new_wal)?;
        Ok(())
    }

    fn relocate(&mut self, num: u32, ix: &SegIndex) {
        for (ci, c) in ix.chunks.iter().enumerate() {
            let sess = &mut self.sessions[c.session as usize];
            for n in &c.nodes {
                sess.nodes[n.id as usize].loc = Loc::Seg { seg: num, chunk: ci as u32, off: n.off };
            }
        }
        for p in &ix.pieces {
            self.pieces[p.id as usize].loc = PLoc::Seg { seg: num, off: p.off, clen: p.clen, rawlen: p.rawlen };
        }
        for o in &ix.opaques {
            self.opaques[o.id as usize].loc = OLoc::Seg { seg: num, off: o.off, len: o.len };
        }
    }

    fn remove_old_wal(&self, keep_from: u64) -> Result<()> {
        for e in fs::read_dir(&self.dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if let Some(n) = parse_num(&name, "wal-", ".log")
                && n < keep_from {
                    fs::remove_file(self.dir.join(&name))?;
                }
        }
        Ok(())
    }

    /// Rewrite every segment into one, with each session stored main line first.
    pub fn compact(&mut self) -> Result<()> {
        self.flush()?;
        if self.manifest.segs.is_empty() {
            return Ok(());
        }
        let num = self.manifest.next_seg;
        let mut w = SegWriter::create(&self.dir, num, self.opts.frame_size, self.opts.level)?;
        let mut ix = SegIndex { max_seq: self.manifest.checkpoint, frame_size: self.opts.frame_size as u32, ..SegIndex::default() };
        let mut tips: Vec<Vec<u32>> = vec![Vec::new(); self.sessions.len()];
        for r in &self.requests {
            tips[r.session as usize].push(r.tip);
        }
        for s in self.sorted_sessions(0..self.sessions.len() as u32) {
            let sess = &self.sessions[s as usize];
            ix.sessions.push((s, sess.key.0.clone(), sess.key.1.clone()));
            let n = sess.nodes.len() as u32;
            if n == 0 {
                continue;
            }
            let order = layout::heavy_first(0, n, |id| sess.nodes[id as usize].parent, tips[s as usize].iter().copied());
            let mut cache: HashMap<(u32, u32, usize), Arc<Vec<u8>>> = HashMap::new();
            let mut stream = Vec::new();
            let mut nodes = Vec::with_capacity(order.len());
            for id in order {
                let nm = &sess.nodes[id as usize];
                nodes.push(SegNode {
                    id,
                    parent: nm.parent,
                    hash: nm.hash,
                    off: stream.len() as u32,
                    len: nm.len,
                    holes: nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default(),
                });
                self.stored_bytes(nm, &mut cache, &mut stream)?;
            }
            if stream.len() > u32::MAX as usize {
                return Err(Error::BadRequest("a session is over 4 GiB".into()));
            }
            let frames = w.frames(&stream)?;
            ix.chunks.push(SegChunk { session: s, raw_len: stream.len() as u64, frames, nodes });
        }
        for (id, pm) in self.pieces.iter().enumerate() {
            let PLoc::Seg { seg, off, clen, rawlen } = pm.loc else { return corrupt("piece still in memory after flush") };
            let raw = self.segs[&seg].piece_raw(off, clen)?;
            let (off, clen) = w.piece_compressed(&raw)?;
            ix.pieces.push(SegPiece { id: id as u32, kind: pm.kind, hash: pm.hash, off, clen, rawlen });
        }
        for (id, om) in self.opaques.iter().enumerate() {
            let OLoc::Seg { seg, off, len } = om.loc else { return corrupt("opaque value still in memory after flush") };
            let payload = self.segs[&seg].opaque(off, len)?;
            let (off, len) = w.opaque(&payload)?;
            ix.opaques.push(SegOpaque { id: id as u64, enc: om.enc, hash: om.hash, off, len });
        }
        ix.requests = self.requests.clone();
        let which: Vec<(u32, u32)> = self.sorted_sessions(0..self.sessions.len() as u32).into_iter().map(|s| (s, 0)).collect();
        self.steps_for_segment(&which, &mut ix);
        w.finish(&ix, self.opts.sync)?;

        let old: Vec<u32> = self.manifest.segs.clone();
        let mut man = self.manifest.clone();
        man.generation += 1;
        man.next_seg = num + 1;
        man.segs = vec![num];
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;

        let (seg, _) = Segment::open(&self.dir, num)?;
        self.relocate(num, &ix);
        self.segs.clear();
        self.segs.insert(num, seg);
        for n in old {
            let _ = fs::remove_file(seg_path(&self.dir, n));
            let _ = fs::remove_file(opq_path(&self.dir, n));
        }
        Ok(())
    }

    // ------------------------------------------------------------------ reads

    /// Append a node's stored bytes (opaque values still cut out).
    fn stored_bytes(&self, nm: &NodeMeta, cache: &mut HashMap<(u32, u32, usize), Arc<Vec<u8>>>, out: &mut Vec<u8>) -> Result<()> {
        match nm.loc {
            Loc::Mem(mi) => out.extend_from_slice(&self.mem.nodes[mi as usize]),
            Loc::Seg { seg, chunk, off } => {
                if nm.len == 0 {
                    return Ok(());
                }
                let sg = &self.segs[&seg];
                let fs = sg.frame_size as usize;
                let (a, b) = (off as usize, off as usize + nm.len as usize);
                let (f0, f1) = (a / fs, (b - 1) / fs);
                let missing: Vec<usize> = (f0..=f1).filter(|k| !cache.contains_key(&(seg, chunk, *k))).collect();
                if let (Some(&lo), Some(&hi)) = (missing.first(), missing.last()) {
                    for (k, d) in (lo..=hi).zip(sg.frames(chunk, lo, hi)?) {
                        cache.entry((seg, chunk, k)).or_insert(d);
                    }
                }
                for k in f0..=f1 {
                    let fr = &cache[&(seg, chunk, k)];
                    let s = a.max(k * fs) - k * fs;
                    let e = b.min((k + 1) * fs) - k * fs;
                    out.extend_from_slice(&fr[s..e]);
                }
            }
        }
        Ok(())
    }

    fn opaque_into(&self, id: u64, out: &mut Vec<u8>, st: &mut ReadStats) -> Result<()> {
        let om = self.opaques.get(id as usize).ok_or_else(|| Error::Corrupt("missing opaque value".into()))?;
        let enc = if om.enc == 0 { OpaqueEnc::Base64 } else { OpaqueEnc::Text };
        match om.loc {
            OLoc::Mem(mi) => split::opaque_text(enc, &self.mem.opaques[mi as usize], out),
            OLoc::Seg { seg, off, len } => {
                let p = self.segs[&seg].opaque(off, len)?;
                st.opaque_reads += 1;
                if opaque_hash(om.enc, &p) != om.hash {
                    return corrupt(format!("opaque value {id} fails its checksum"));
                }
                split::opaque_text(enc, &p, out);
            }
        }
        Ok(())
    }

    /// Read nodes of one session, in the order given, with opaque values put back.
    /// Each node's bytes go to `out`; `spans` gets their ranges.
    fn read_nodes(&self, s: &Session, ids: &[u32], out: &mut Vec<u8>, spans: &mut Vec<(usize, usize)>, st: &mut ReadStats) -> Result<()> {
        // which frames are needed, read with one call per run of adjacent frames
        let mut need: BTreeSet<(u32, u32, usize)> = BTreeSet::new();
        let mut prev: Option<(u32, u32, u32)> = None;
        for &id in ids {
            let nm = &s.nodes[id as usize];
            match nm.loc {
                Loc::Seg { seg, chunk, off } => {
                    let fs = self.segs[&seg].frame_size as usize;
                    if nm.len > 0 {
                        for k in off as usize / fs..=(off as usize + nm.len as usize - 1) / fs {
                            need.insert((seg, chunk, k));
                        }
                    }
                    if prev != Some((seg, chunk, off)) {
                        st.runs += 1;
                    }
                    prev = Some((seg, chunk, off + nm.len));
                }
                Loc::Mem(_) => {
                    st.runs += 1;
                    prev = None;
                }
            }
        }
        let mut cache: HashMap<(u32, u32, usize), Arc<Vec<u8>>> = HashMap::with_capacity(need.len());
        let v: Vec<_> = need.into_iter().collect();
        let mut i = 0;
        while i < v.len() {
            let (seg, chunk, k0) = v[i];
            let mut j = i;
            while j + 1 < v.len() && v[j + 1] == (seg, chunk, v[j].2 + 1) {
                j += 1;
            }
            let frames = self.segs[&seg].frames(chunk, k0, v[j].2)?;
            st.reads += 1;
            st.frames += frames.len() as u32;
            for (n, d) in frames.into_iter().enumerate() {
                cache.insert((seg, chunk, k0 + n), d);
            }
            i = j + 1;
        }
        // opaque values: read the ones in segments with as few reads as possible (a
        // session's values sit close together in its segment's opaque file)
        let mut wanted: Vec<(u32, u64, u32, u64)> = Vec::new(); // (seg, off, len, id)
        for &id in ids {
            if let Some(h) = &s.nodes[id as usize].holes {
                for &(_, oid) in h.iter() {
                    if let Some(OLoc::Seg { seg, off, len }) = self.opaques.get(oid as usize).map(|o| o.loc) {
                        wanted.push((seg, off, len, oid));
                    }
                }
            }
        }
        let mut fetched: HashMap<u64, Vec<u8>> = HashMap::with_capacity(wanted.len());
        if !wanted.is_empty() {
            wanted.sort_unstable();
            wanted.dedup();
            const GAP: u64 = 64 * 1024;
            let mut i = 0;
            while i < wanted.len() {
                let (seg, start, _, _) = wanted[i];
                let mut end = start + wanted[i].2 as u64;
                let mut j = i;
                while j + 1 < wanted.len() && wanted[j + 1].0 == seg && wanted[j + 1].1 <= end + GAP {
                    j += 1;
                    end = end.max(wanted[j].1 + wanted[j].2 as u64);
                }
                let buf = self.segs[&seg].opaque(start, (end - start) as u32)?;
                st.opaque_reads += 1;
                for w in &wanted[i..=j] {
                    let a = (w.1 - start) as usize;
                    fetched.insert(w.3, buf[a..a + w.2 as usize].to_vec());
                }
                i = j + 1;
            }
        }
        let mut tmp = Vec::new();
        for &id in ids {
            let nm = &s.nodes[id as usize];
            let start = out.len();
            match &nm.holes {
                None => self.stored_bytes(nm, &mut cache, out)?,
                Some(holes) => {
                    tmp.clear();
                    self.stored_bytes(nm, &mut cache, &mut tmp)?;
                    let mut pos = 0usize;
                    for &(o, oid) in holes.iter() {
                        out.extend_from_slice(&tmp[pos..o as usize]);
                        match fetched.get(&oid) {
                            Some(p) => {
                                let om = &self.opaques[oid as usize];
                                if opaque_hash(om.enc, p) != om.hash {
                                    return corrupt(format!("opaque value {oid} fails its checksum"));
                                }
                                split::opaque_text(if om.enc == 0 { OpaqueEnc::Base64 } else { OpaqueEnc::Text }, p, out);
                            }
                            None => self.opaque_into(oid, out, st)?,
                        }
                        pos = o as usize;
                    }
                    out.extend_from_slice(&tmp[pos..]);
                }
            }
            spans.push((start, out.len()));
        }
        Ok(())
    }

    fn piece(&self, id: u32) -> Result<Arc<[u8]>> {
        let pm = self.pieces.get(id as usize).ok_or_else(|| Error::Corrupt(format!("missing piece {id}")))?;
        match pm.loc {
            PLoc::Mem(mi) => Ok(self.mem.pieces[mi as usize].clone()),
            PLoc::Seg { seg, off, clen, rawlen } => {
                if let Some(p) = self.piece_cache.lock().unwrap().get(&id) {
                    return Ok(p.clone());
                }
                let p: Arc<[u8]> = self.segs[&seg].piece(off, clen, rawlen)?.into();
                self.piece_cache.lock().unwrap().insert(id, p.clone());
                Ok(p)
            }
        }
    }

    fn skeleton(&self, id: u32) -> Result<Arc<Skeleton>> {
        if let Some(s) = self.skel_cache.lock().unwrap().get(&id) {
            return Ok(s.clone());
        }
        let s = Arc::new(Skeleton::decode(&self.piece(id)?)?);
        self.skel_cache.lock().unwrap().insert(id, s.clone());
        Ok(s)
    }

    fn path(&self, s: &Session, tip: u32, n: u32) -> Result<Vec<u32>> {
        let mut v = Vec::with_capacity(n as usize);
        let mut c = tip;
        while c != NONE {
            v.push(c);
            if v.len() > n as usize {
                break;
            }
            c = s.nodes[c as usize].parent;
        }
        if v.len() != n as usize {
            return corrupt("request path has the wrong length");
        }
        v.reverse();
        Ok(v)
    }

    fn with_notes(&self, cut: &[u8], notes: &[ReqNote], out: &mut Vec<u8>) -> Result<()> {
        let mut pos = 0usize;
        for n in notes {
            let o = n.offset as usize;
            if o < pos || o > cut.len() {
                return corrupt("cache note out of range");
            }
            out.extend_from_slice(&cut[pos..o]);
            out.extend_from_slice(&self.piece(n.text)?);
            pos = o;
        }
        out.extend_from_slice(&cut[pos..]);
        Ok(())
    }

    /// Rebuild the exact body of request `ri`.
    pub fn rebuild_into(&self, ri: u32, out: &mut Vec<u8>, st: &mut ReadStats) -> Result<()> {
        let r = self.requests.get(ri as usize).ok_or_else(|| Error::BadRequest("no such request".into()))?;
        let s = &self.sessions[r.session as usize];
        let path = self.path(s, r.tip, r.n_msgs)?;
        let mut mbuf = Vec::new();
        let mut spans = Vec::with_capacity(path.len());
        self.read_nodes(s, &path, &mut mbuf, &mut spans, st)?;
        let skel = self.skeleton(r.skeleton)?;
        let notes = &r.notes[..];
        let of = |t: u32| -> &[ReqNote] {
            let a = notes.partition_point(|n| n.target < t);
            let b = notes.partition_point(|n| n.target <= t);
            &notes[a..b]
        };
        out.reserve(mbuf.len() + 4096);
        split::assemble(&skel, out, |k, out| {
            match k {
                HoleKind::Messages => {
                    for (i, &(a, b)) in spans.iter().enumerate() {
                        match &r.layout {
                            Some(g) => out.extend_from_slice(&g[i]),
                            None if i > 0 => out.push(b','),
                            None => {}
                        }
                        self.with_notes(&mbuf[a..b], of(TARGET_MSG0 + i as u32), out)?;
                    }
                    if let Some(g) = &r.layout {
                        out.extend_from_slice(&g[spans.len()]);
                    }
                }
                HoleKind::System => self.with_notes(&self.piece(r.system)?, of(TARGET_SYSTEM), out)?,
                HoleKind::Tools => self.with_notes(&self.piece(r.tools)?, of(TARGET_TOOLS), out)?,
            }
            Ok(())
        })
    }

    pub fn rebuild(&self, ri: u32) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.rebuild_into(ri, &mut out, &mut ReadStats::default())?;
        Ok(out)
    }

    /// Rebuild and check the result against the hash taken when the request arrived.
    pub fn rebuild_checked(&self, ri: u32) -> Result<Vec<u8>> {
        let out = self.rebuild(ri)?;
        if sha256(&out) != self.requests[ri as usize].digest {
            return corrupt(format!("request {} does not rebuild to its digest", self.requests[ri as usize].id));
        }
        Ok(out)
    }

    pub fn request_index(&self, id: &str) -> Option<u32> {
        self.request_ix.get(id).copied()
    }

    pub fn request(&self, ri: u32) -> &ReqRec {
        &self.requests[ri as usize]
    }

    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn session_key(&self, s: u32) -> &(String, String) {
        &self.sessions[s as usize].key
    }

    pub fn session_len(&self, s: u32) -> u32 {
        self.sessions[s as usize].nodes.len() as u32
    }

    /// Node ids of a request's messages, in order.
    pub fn request_path(&self, ri: u32) -> Result<Vec<u32>> {
        let r = &self.requests[ri as usize];
        self.path(&self.sessions[r.session as usize], r.tip, r.n_msgs)
    }

    /// Read some messages of a session (node ids), with opaque values put back.
    pub fn read_messages(&self, s: u32, ids: &[u32], st: &mut ReadStats) -> Result<Vec<Vec<u8>>> {
        let sess = &self.sessions[s as usize];
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(sess, ids, &mut buf, &mut spans, st)?;
        Ok(spans.into_iter().map(|(a, b)| buf[a..b].to_vec()).collect())
    }

    /// Every message of a session in storage order: the whole timeline in one pass.
    pub fn replay(&self, s: u32, st: &mut ReadStats) -> Result<Vec<u8>> {
        let sess = &self.sessions[s as usize];
        let mut ids: Vec<u32> = (0..sess.nodes.len() as u32).collect();
        ids.sort_by_key(|&i| match sess.nodes[i as usize].loc {
            Loc::Seg { seg, chunk, off } => (0u8, seg, chunk, off),
            Loc::Mem(m) => (1u8, 0, 0, m),
        });
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(sess, &ids, &mut buf, &mut spans, st)?;
        Ok(buf)
    }

    /// How many separate runs of storage a request's messages occupy.
    pub fn request_runs(&self, ri: u32) -> Result<u32> {
        let r = &self.requests[ri as usize];
        let s = &self.sessions[r.session as usize];
        let path = self.path(s, r.tip, r.n_msgs)?;
        let mut runs = 0;
        let mut prev: Option<(u32, u32, u32)> = None;
        for id in path {
            let nm = &s.nodes[id as usize];
            match nm.loc {
                Loc::Seg { seg, chunk, off } => {
                    if prev != Some((seg, chunk, off)) {
                        runs += 1;
                    }
                    prev = Some((seg, chunk, off + nm.len));
                }
                Loc::Mem(_) => {
                    runs += 1;
                    prev = None;
                }
            }
        }
        Ok(runs)
    }

    pub fn stats(&self) -> Stats {
        let mut st = Stats {
            sessions: self.sessions.len(),
            requests: self.requests.len(),
            pieces: self.pieces.len(),
            opaques: self.opaques.len(),
            segments: self.segs.len(),
            wal_bytes: self.wal.bytes,
            mem_bytes: self.mem.bytes,
            ..Stats::default()
        };
        st.tools = self.sd.tools.len();
        st.step_fields = self.sd.fields.len();
        st.step_values = self.sd.values.iter().map(|d| d.len()).sum();
        st.orphan_results = self.orphan_results;
        for s in &self.sessions {
            st.steps += s.steps.rows.len();
            st.nodes += s.nodes.len();
            st.node_bytes += s.nodes.iter().map(|n| n.len as u64).sum::<u64>();
        }
        for p in &self.pieces {
            match p.loc {
                PLoc::Seg { clen, rawlen, .. } => {
                    st.piece_bytes += clen as u64;
                    st.piece_raw_bytes += rawlen as u64;
                }
                PLoc::Mem(mi) => st.piece_raw_bytes += self.mem.pieces[mi as usize].len() as u64,
            }
        }
        for o in &self.opaques {
            st.opaque_bytes += match o.loc {
                OLoc::Seg { len, .. } => len as u64,
                OLoc::Mem(mi) => self.mem.opaques[mi as usize].len() as u64,
            };
        }
        for sg in self.segs.values() {
            st.frame_bytes += sg.chunks.iter().flat_map(|c| c.frames.iter()).map(|f| f.1 as u64).sum::<u64>();
            st.index_bytes += sg.index_bytes;
            st.seg_file_bytes += sg.file_bytes;
            st.opq_file_bytes += sg.opq_bytes;
        }
        st
    }

    /// A session's steps as columns.
    pub fn steps(&self, s: u32) -> &SessionSteps {
        &self.sessions[s as usize].steps
    }

    /// Tool, field and value dictionaries, and the field index.
    pub fn step_dicts(&self) -> &StepDicts {
        &self.sd
    }

    /// One message as stored: opaque values (signatures, images) still cut out, with the
    /// offsets where they belong. Cheaper than [`Db::message`] when they are not needed.
    pub fn stored_message(&self, s: u32, node: u32) -> Result<(Vec<u8>, Vec<u32>)> {
        let nm = &self.sessions[s as usize].nodes[node as usize];
        let mut out = Vec::with_capacity(nm.len as usize);
        self.stored_bytes(nm, &mut HashMap::new(), &mut out)?;
        Ok((out, nm.holes.as_deref().map(|h| h.iter().map(|x| x.0).collect()).unwrap_or_default()))
    }

    /// One message (tree node) of a session, with opaque values put back.
    pub fn message(&self, s: u32, node: u32) -> Result<Vec<u8>> {
        Ok(self.read_messages(s, &[node], &mut ReadStats::default())?.pop().unwrap_or_default())
    }

    /// Parent of every node of a session (`NONE` for roots).
    pub fn node_parents(&self, s: u32) -> Vec<u32> {
        self.sessions[s as usize].nodes.iter().map(|n| n.parent).collect()
    }

    /// Requests of one session, by index.
    pub fn session_requests(&self, s: u32) -> Vec<u32> {
        (0..self.requests.len() as u32).filter(|&i| self.requests[i as usize].session == s).collect()
    }

    pub fn segment_count(&self) -> usize {
        self.segs.len()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
