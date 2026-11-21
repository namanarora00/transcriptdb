//! The single-node store: write path, flush, merge, recovery and reads.
//!
//! Write path for one request: split the body ([`crate::split`]), walk the messages
//! down the session's tree comparing hashes, keep only the messages the session has
//! not sent before as new nodes, cut opaque values out of those, intern the skeleton,
//! system, tools and cache-marker pieces, then append one log record. The record is
//! durable once [`Db::commit`] returns; that is the moment a request counts as saved.
//!
//! Everything in memory that is not yet in a file can be rebuilt from the log, because
//! that state only ever changes by applying a log record ([`Db::apply`]), both live and
//! during recovery.
//!
//! What stays in memory does not grow with the data: the manifest, one small catalog per
//! file, the sessions taking writes, the unflushed data, and caches with fixed budgets.
//! A session is a self-contained unit (its messages, requests and steps) stored as parts
//! in segments; it is read from its parts on demand. Shared pieces and opaque values are
//! in blob files, found by id range, or by hash through each blob file's Bloom filter.
//! Flushes write level-0 segments; merges combine them with the level-1 segments whose
//! session ranges they touch, so finished sessions settle and are not rewritten again.

use crate::blob::{blob_path, find_in, Blob, BlobWriter, TBlock, HASHES, ID_BLOCK, OPAQUES, PIECES};
use crate::bloom::key_hash;
use crate::cache::{Cache, CacheStats};
use crate::codec::{corrupt, Error, Result};
use crate::layout;
use crate::manifest::Manifest;
use crate::record::*;
use crate::segment::{decode_core, decode_dir, decode_reqtab, decode_steps, key_bytes, seg_path, CorePart, DirEnt, ReqTab, SegNode, SegWriter, Segment, StepPart};
use crate::split::{self, HoleKind, Keep, OpaqueEnc, OpaqueHole, OpaqueKind, Skeleton};
use crate::steps::{self, FieldVal, SessionSteps, StepDicts, StepRow};
use crate::wal::{self, SyncMode, WalWriter};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    /// Which opaque values (thinking, images, documents) to keep. Values not kept are
    /// stored as kind, hash and size; their requests rebuild with `""` in their place.
    pub keep: Keep,
    /// Memory for sessions, blocks and tables read from files.
    pub cache_bytes: usize,
    /// Merge once this many flushed (level-0) segments are waiting.
    pub merge_at: usize,
    /// A merge starts a new output segment past this many bytes.
    pub seg_bytes: u64,
    /// Sessions that took no writes for this many flushes stop being kept in memory.
    pub idle_flushes: u64,
    /// Most segment and blob files kept open at once (process-wide).
    pub open_files: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            frame_size: 64 * 1024,
            flush_bytes: 32 << 20,
            sync: SyncMode::Full,
            level: 3,
            verify_split: true,
            keep: Keep::default(),
            cache_bytes: 256 << 20,
            merge_at: 4,
            seg_bytes: 256 << 20,
            idle_flushes: 2,
            open_files: 128,
        }
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

/// A stored request: its session in the high 32 bits, its index in the session below.
pub type ReqId = u64;

pub fn req_id(session: u32, k: u32) -> ReqId {
    (session as u64) << 32 | k as u64
}

pub fn req_parts(r: ReqId) -> (u32, u32) {
    ((r >> 32) as u32, r as u32)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Stored(ReqId),
    /// A request with this id is already stored; nothing was written.
    Duplicate(ReqId),
}

#[derive(Debug, Clone, Copy)]
enum Loc {
    Mem(u32),
    /// `chunk` indexes the session's chunks.
    Seg { chunk: u32, off: u32 },
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

/// One part's message stream in a segment.
#[derive(Debug, Clone)]
struct Chunk {
    seg: u32,
    frames: Arc<[(u64, u32)]>,
    raw_len: u64,
}

/// A session's messages and requests.
#[derive(Debug, Clone, Default)]
pub struct SessionCore {
    key: (String, String),
    nodes: Vec<NodeMeta>,
    chunks: Vec<Chunk>,
    requests: Vec<ReqRec>,
    flushed_nodes: u32,
    flushed_reqs: u32,
}

impl SessionCore {
    fn bytes(&self) -> usize {
        256 + self.nodes.len() * 56
            + self.nodes.iter().filter_map(|n| n.holes.as_ref()).map(|h| h.len() * 16).sum::<usize>()
            + self.chunks.iter().map(|c| c.frames.len() * 16 + 48).sum::<usize>()
            + self.requests.iter().map(|r| 160 + r.id.len() + r.time.len() + r.meta.len() + r.notes.len() * 12).sum::<usize>()
    }
}

/// A session taking writes.
struct Active {
    core: Arc<SessionCore>,
    steps: Arc<SessionSteps>,
    children: HashMap<(u32, u128), u32>,
    /// Flush count when it last took a write.
    last: u64,
}

#[derive(Debug, Clone, Copy)]
enum OLoc {
    Mem(u32),
    Blob { blob: u32, off: u64, len: u32 },
    /// Not kept: `len` is the length the payload had.
    Dropped { len: u32 },
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
    /// Pieces with ids from `Manifest::pieces` on.
    pieces: Vec<(u8, u128, Arc<[u8]>)>,
    /// Opaque values with ids from `Manifest::opaques` on; `None` payload = not kept.
    opaques: Vec<(u8, u8, u128, Option<Box<[u8]>>, u32)>,
    piece_ix: HashMap<u128, u32>,
    opaque_ix: HashMap<u128, u64>,
    req_ix: HashMap<Box<str>, ReqId>,
    bytes: usize,
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
pub struct MergeStats {
    pub merges: u64,
    /// File bytes read by merges and written by them.
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// File bytes written by flushes.
    pub flushed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub sessions: usize,
    pub nodes: usize,
    pub requests: usize,
    pub pieces: usize,
    pub opaques: usize,
    /// Opaque values stored as kind, hash and size only.
    pub opaques_dropped: usize,
    pub segments: usize,
    pub level0: usize,
    pub blobs: usize,
    /// Uncompressed bytes of all stored messages (after cutting markers and opaque values).
    pub node_bytes: u64,
    /// Compressed frames holding those messages.
    pub frame_bytes: u64,
    pub piece_raw_bytes: u64,
    pub piece_bytes: u64,
    pub opaque_bytes: u64,
    /// Segment bytes other than frames: blocks of parts, request tables, directories, catalogs.
    pub index_bytes: u64,
    pub seg_file_bytes: u64,
    pub blob_file_bytes: u64,
    pub wal_bytes: u64,
    pub mem_bytes: usize,
    pub steps: usize,
    pub tools: usize,
    pub step_fields: usize,
    pub orphan_results: usize,
    /// Memory held by file catalogs (always loaded), and by sessions taking writes.
    pub catalog_bytes: usize,
    pub active_sessions: usize,
    pub active_bytes: usize,
    pub caches: Vec<(&'static str, CacheStats)>,
    pub merges: MergeStats,
}

pub struct Db {
    dir: PathBuf,
    /// Held with an exclusive lock while the store is open.
    _lock: fs::File,
    opts: Options,
    manifest: Manifest,
    segs: BTreeMap<u32, Arc<Segment>>,
    /// Per segment: its tool and field codes mapped to the store's.
    seg_codes: HashMap<u32, (Vec<u32>, Vec<u32>)>,
    /// In file order, so piece and opaque id ranges ascend.
    blobs: Vec<Arc<Blob>>,
    dirs: Cache<u32, Vec<DirEnt>>,
    /// Request table blocks by (segment, block).
    reqtabs: Cache<(u32, u32), ReqTab>,
    blocks: Cache<(u32, u32), Vec<u8>>,
    /// Blob table blocks by (file, kind, index).
    tables: Cache<(u32, u8, u32), TBlock>,
    cores: Cache<u32, SessionCore>,
    stepc: Cache<u32, SessionSteps>,
    pieces: Cache<u32, Vec<u8>>,
    skels: Cache<u32, Skeleton>,
    active: HashMap<u32, Active>,
    key_ix: HashMap<(String, String), u32>,
    next_session: u32,
    mem: Mem,
    wal: WalWriter,
    seq: u64,
    sd: StepDicts,
    flushes: u64,
    /// Tool results whose call could not be found on their path.
    orphan_results: usize,
    pub recovery: RecoveryReport,
    pub merge_stats: MergeStats,
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

fn bad(s: impl Into<String>) -> Error {
    Error::Corrupt(s.into())
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = fs::OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("LOCK"))?;
        if unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(Error::BadRequest(format!("{} is open in another process", dir.display())));
        }
        let _ = fs::remove_file(dir.join("MANIFEST.tmp"));
        let manifest = match Manifest::load(&dir)? {
            Some(m) => m,
            None => {
                let m = Manifest { wal_start: 1, next_file: 1, ..Manifest::default() };
                m.store(&dir, opts.sync)?;
                m
            }
        };
        let mut rec = RecoveryReport::default();
        let live: BTreeSet<u32> = manifest.segs.iter().chain(&manifest.blobs).copied().collect();
        let mut wal_nums = Vec::new();
        for e in fs::read_dir(&dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            let file = parse_num(&name, "seg-", ".tdb").or_else(|| parse_num(&name, "blob-", ".tdb"));
            if let Some(n) = file {
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
        crate::block::set_open_files(opts.open_files);
        let cb = opts.cache_bytes;
        let mut db = Db {
            dir,
            _lock: lock,
            manifest,
            segs: BTreeMap::new(),
            seg_codes: HashMap::new(),
            blobs: Vec::new(),
            dirs: Cache::new(cb / 16),
            reqtabs: Cache::sharded(cb / 16, 16),
            blocks: Cache::sharded(cb / 8, 16),
            tables: Cache::sharded(cb / 16, 16),
            cores: Cache::new(cb / 4),
            stepc: Cache::new(cb / 4),
            pieces: Cache::sharded(cb / 16, 16),
            skels: Cache::sharded(cb / 32, 16),
            active: HashMap::new(),
            key_ix: HashMap::new(),
            next_session: 0,
            mem: Mem::default(),
            wal,
            seq: 0,
            sd: StepDicts::default(),
            flushes: 0,
            orphan_results: 0,
            recovery: RecoveryReport::default(),
            merge_stats: MergeStats::default(),
            opts,
        };
        let mut segs = db.manifest.segs.clone();
        segs.sort_unstable();
        for n in segs {
            db.add_segment(Segment::open(&db.dir, n)?);
            rec.segments += 1;
        }
        let mut blobs = db.manifest.blobs.clone();
        blobs.sort_unstable();
        for n in blobs {
            db.blobs.push(Arc::new(Blob::open(&db.dir, n)?));
        }
        db.check_blob_ranges()?;
        db.next_session = db.manifest.sessions;
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

    fn add_segment(&mut self, seg: Segment) {
        let tools = seg.cat.tools.iter().map(|t| self.sd.tools.intern(t)).collect();
        let fields = seg.cat.fields.iter().map(|f| self.sd.field(f)).collect();
        self.seg_codes.insert(seg.num, (tools, fields));
        self.segs.insert(seg.num, Arc::new(seg));
    }

    fn check_blob_ranges(&self) -> Result<()> {
        let (mut p, mut o) = (0u32, 0u64);
        for b in &self.blobs {
            if b.cat.piece_lo != p || b.cat.opaque_lo != o {
                return corrupt(format!("blob file {} does not continue the id ranges", b.num));
            }
            p += b.cat.n_pieces;
            o += b.cat.n_opaques;
        }
        if p != self.manifest.pieces || o != self.manifest.opaques {
            return corrupt("blob files do not cover the ids the manifest names");
        }
        Ok(())
    }

    fn next_piece(&self) -> u32 {
        self.manifest.pieces + self.mem.pieces.len() as u32
    }

    fn next_opaque(&self) -> u64 {
        self.manifest.opaques + self.mem.opaques.len() as u64
    }

    // ------------------------------------------------------------------ finding things in files

    fn dir_of(&self, seg: &Segment) -> Result<Arc<Vec<DirEnt>>> {
        if let Some(d) = self.dirs.get(&seg.num) {
            return Ok(d);
        }
        let d = decode_dir(&seg.block(seg.cat.dir)?)?;
        let size = d.iter().map(|x| 96 + x.key.0.len() + x.key.1.len()).sum();
        Ok(self.dirs.insert(seg.num, Arc::new(d), size))
    }

    fn block(&self, seg: &Segment, b: u32) -> Result<Arc<Vec<u8>>> {
        if let Some(x) = self.blocks.get(&(seg.num, b)) {
            return Ok(x);
        }
        let raw = seg.block(b)?;
        let n = raw.len();
        Ok(self.blocks.insert((seg.num, b), Arc::new(raw), n))
    }

    fn tblock(&self, blob: &Blob, kind: u8, i: usize) -> Result<Arc<TBlock>> {
        let k = (blob.num, kind, i as u32);
        if let Some(t) = self.tables.get(&k) {
            return Ok(t);
        }
        let t = blob.table_block(kind, i)?;
        let n = t.bytes();
        Ok(self.tables.insert(k, Arc::new(t), n))
    }

    /// Every part of session `s`, in order.
    fn parts(&self, s: u32) -> Result<Vec<(Arc<Segment>, DirEnt)>> {
        let mut out = Vec::new();
        for seg in self.segs.values() {
            if !seg.cat.may_hold(s) {
                continue;
            }
            let dir = self.dir_of(seg)?;
            let a = dir.partition_point(|d| d.session < s);
            for d in dir[a..].iter().take_while(|d| d.session == s) {
                out.push((seg.clone(), d.clone()));
            }
        }
        out.sort_by_key(|p| (p.1.first_req, p.1.first_node, p.1.first_row));
        Ok(out)
    }

    fn find_session_in_files(&self, key: &(String, String)) -> Result<Option<u32>> {
        let h = key_hash(&key_bytes(&key.0, &key.1));
        for seg in self.segs.values().rev() {
            if !seg.cat.key_bloom.may_contain(h) {
                continue;
            }
            if let Some(d) = self.dir_of(seg)?.iter().find(|d| d.key == *key) {
                return Ok(Some(d.session));
            }
        }
        Ok(None)
    }

    fn find_request_in_files(&self, id: &str) -> Result<Option<ReqId>> {
        let h = key_hash(id.as_bytes());
        for seg in self.segs.values().rev() {
            if !seg.cat.req_bloom.may_contain(h) {
                continue;
            }
            let Some(b) = seg.cat.reqtab_block_for(id) else { continue };
            let tab = match self.reqtabs.get(&(seg.num, b)) {
                Some(t) => t,
                None => {
                    let t = decode_reqtab(&seg.block(b)?)?;
                    let size = t.iter().map(|x| x.0.len() + 32).sum();
                    self.reqtabs.insert((seg.num, b), Arc::new(t), size)
                }
            };
            if let Ok(i) = tab.binary_search_by(|x| (*x.0).cmp(id)) {
                return Ok(Some(req_id(tab[i].1, tab[i].2)));
            }
        }
        Ok(None)
    }

    /// An existing piece or opaque value with this hash (tag 0 = piece, 1 = opaque).
    fn find_by_hash(&self, tag: u8, hash: u128) -> Result<Option<u64>> {
        let mem = if tag == 0 { self.mem.piece_ix.get(&hash).map(|&x| x as u64) } else { self.mem.opaque_ix.get(&hash).copied() };
        if mem.is_some() {
            return Ok(mem);
        }
        for b in self.blobs.iter().rev() {
            if !b.cat.bloom.may_contain(hash as u64) {
                continue;
            }
            let Some(i) = b.cat.hash_block_for(tag, hash) else { continue };
            if let TBlock::Hashes(v) = &*self.tblock(b, HASHES, i)?
                && let Some(id) = find_in(v, tag, hash)
            {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    fn blob_for_piece(&self, id: u32) -> Result<&Arc<Blob>> {
        let i = self.blobs.partition_point(|b| b.cat.piece_lo + b.cat.n_pieces <= id);
        self.blobs.get(i).filter(|b| b.has_piece(id)).ok_or_else(|| bad(format!("missing piece {id}")))
    }

    fn blob_for_opaque(&self, id: u64) -> Result<&Arc<Blob>> {
        let i = self.blobs.partition_point(|b| b.cat.opaque_lo + b.cat.n_opaques <= id);
        self.blobs.get(i).filter(|b| b.has_opaque(id)).ok_or_else(|| bad(format!("missing opaque value {id}")))
    }

    fn opaque_meta(&self, id: u64) -> Result<OpaqueMeta> {
        self.opaque_meta_in(id, &mut HashMap::new())
    }

    /// [`Db::opaque_meta`], keeping the table blocks it needs in `tables` for the next call.
    fn opaque_meta_in(&self, id: u64, tables: &mut HashMap<(u32, u32), Arc<TBlock>>) -> Result<OpaqueMeta> {
        if id >= self.manifest.opaques {
            let (enc, _, hash, payload, len) = self.mem.opaques.get((id - self.manifest.opaques) as usize).ok_or_else(|| bad(format!("missing opaque value {id}")))?;
            let loc = if payload.is_some() { OLoc::Mem((id - self.manifest.opaques) as u32) } else { OLoc::Dropped { len: *len } };
            return Ok(OpaqueMeta { enc: *enc, hash: *hash, loc });
        }
        let b = self.blob_for_opaque(id)?;
        let i = (id - b.cat.opaque_lo) as usize;
        let t = match tables.get(&(b.num, (i / ID_BLOCK) as u32)) {
            Some(t) => t.clone(),
            None => {
                let t = self.tblock(b, OPAQUES, i / ID_BLOCK)?;
                tables.insert((b.num, (i / ID_BLOCK) as u32), t.clone());
                t
            }
        };
        let TBlock::Opaques(v) = &*t else { return corrupt("blob block of the wrong kind") };
        let o = *v.get(i % ID_BLOCK).ok_or_else(|| bad(format!("missing opaque value {id}")))?;
        let loc = if o.dropped { OLoc::Dropped { len: o.len } } else { OLoc::Blob { blob: b.num, off: o.off, len: o.len } };
        Ok(OpaqueMeta { enc: o.enc, hash: o.hash, loc })
    }

    // ------------------------------------------------------------------ sessions

    fn load_core(&self, s: u32) -> Result<Arc<SessionCore>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.core.clone());
        }
        if let Some(c) = self.cores.get(&s) {
            return Ok(c);
        }
        let parts = self.parts(s)?;
        let Some(first) = parts.first() else { return Err(Error::BadRequest(format!("no session {s}"))) };
        let mut core = SessionCore { key: first.1.key.clone(), ..SessionCore::default() };
        for (seg, d) in &parts {
            let raw = self.block(seg, d.core.0)?;
            let (a, b) = (d.core.1 as usize, (d.core.1 + d.core.2) as usize);
            let p = decode_core(raw.get(a..b).ok_or_else(|| bad("session part past its block"))?)?;
            if p.first_node as usize != core.nodes.len() || p.first_req as usize != core.requests.len() {
                return corrupt(format!("session {s}: parts do not follow each other"));
            }
            let chunk = core.chunks.len() as u32;
            core.chunks.push(Chunk { seg: seg.num, frames: p.frames.into(), raw_len: p.raw_len });
            let end = core.nodes.len() + p.nodes.len();
            core.nodes.resize(end, NodeMeta { parent: NONE, len: 0, hash: 0, loc: UNSET, holes: None });
            for n in p.nodes {
                let slot = core.nodes.get_mut(n.id as usize).ok_or_else(|| bad(format!("session {s}: node {} outside its part", n.id)))?;
                *slot = NodeMeta { parent: n.parent, len: n.len, hash: n.hash, loc: Loc::Seg { chunk, off: n.off }, holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) } };
            }
            core.requests.extend(p.requests);
        }
        if core.nodes.iter().any(|n| matches!(n.loc, Loc::Mem(u32::MAX))) {
            return corrupt(format!("session {s} is missing nodes"));
        }
        core.flushed_nodes = core.nodes.len() as u32;
        core.flushed_reqs = core.requests.len() as u32;
        let size = core.bytes();
        Ok(self.cores.insert(s, Arc::new(core), size))
    }

    fn load_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.steps.clone());
        }
        if let Some(c) = self.stepc.get(&s) {
            return Ok(c);
        }
        let parts = self.parts(s)?;
        let mut ss = SessionSteps::default();
        let mut fs: Vec<(u32, FieldVal)> = Vec::new();
        for (seg, d) in &parts {
            let raw = self.block(seg, d.steps.0)?;
            let (a, b) = (d.steps.1 as usize, (d.steps.1 + d.steps.2) as usize);
            let p = decode_steps(raw.get(a..b).ok_or_else(|| bad("step part past its block"))?)?;
            if p.first as usize != ss.rows.len() {
                return corrupt(format!("session {s}: step parts do not follow each other"));
            }
            let (tools, fields) = &self.seg_codes[&seg.num];
            let gf = |f: u32| fields.get(f as usize).copied().ok_or_else(|| bad("step names a missing field"));
            let mut vmap = Vec::with_capacity(p.values.len());
            for (f, v) in &p.values {
                vmap.push(ss.intern_value(gf(*f)?, v));
            }
            for (mut r, rf) in p.rows.into_iter().zip(p.fields) {
                r.tool = *tools.get(r.tool as usize).ok_or_else(|| bad("step names a missing tool"))?;
                fs.clear();
                for (f, v) in rf {
                    let val = if steps::is_long(v) {
                        let (off, len) = p.longs[(v - steps::LONG_BASE) as usize];
                        FieldVal::Long { off, len }
                    } else {
                        FieldVal::Code(vmap[v as usize])
                    };
                    fs.push((gf(f)?, val));
                }
                ss.push(r, &fs);
            }
        }
        ss.flushed = ss.rows.len() as u32;
        let size = ss.bytes();
        Ok(self.stepc.insert(s, Arc::new(ss), size))
    }

    /// Load a session for writing (no-op if it already takes writes).
    fn activate(&mut self, s: u32) -> Result<()> {
        if let Some(a) = self.active.get_mut(&s) {
            a.last = self.flushes;
            return Ok(());
        }
        let core = self.load_core(s)?;
        let steps = self.load_steps(s)?;
        self.cores.remove(&s);
        self.stepc.remove(&s);
        let mut ss = Arc::unwrap_or_clone(steps);
        let mut children = HashMap::with_capacity(core.nodes.len());
        for (i, n) in core.nodes.iter().enumerate() {
            children.insert((n.parent, n.hash), i as u32);
        }
        // per node: last step at or above it on its path
        let mut last_at = vec![NONE; core.nodes.len()];
        for (pos, r) in ss.rows.iter().enumerate() {
            let n = r.result_node as usize;
            if n >= core.nodes.len() || r.call_node as usize >= core.nodes.len() {
                return corrupt(format!("session {s} has a step past its nodes"));
            }
            last_at[n] = pos as u32;
        }
        ss.tail = Vec::with_capacity(core.nodes.len());
        for (i, n) in core.nodes.iter().enumerate() {
            let t = if last_at[i] != NONE {
                last_at[i]
            } else if n.parent == NONE {
                NONE
            } else {
                ss.tail[n.parent as usize]
            };
            ss.tail.push(t);
        }
        ss.calls.clear();
        self.key_ix.insert(core.key.clone(), s);
        self.active.insert(s, Active { core, steps: Arc::new(ss), children, last: self.flushes });
        Ok(())
    }

    // ------------------------------------------------------------------ writes

    fn piece_id(&self, kind: u8, b: &[u8], m: &mut Mutation, pending: &mut HashMap<u128, u32>) -> Result<u32> {
        let h = piece_hash(kind, b);
        if let Some(&i) = pending.get(&h) {
            return Ok(i);
        }
        if let Some(i) = self.find_by_hash(0, h)? {
            return Ok(i as u32);
        }
        let id = self.next_piece() + m.pieces.len() as u32;
        m.pieces.push((id, kind, h, b.to_vec()));
        pending.insert(h, id);
        Ok(id)
    }

    fn opaque_dropped(&self, id: u64, m: &Mutation) -> Result<bool> {
        if id >= self.next_opaque() {
            return Ok(m.opaques.iter().any(|o| o.id == id && o.dropped.is_some()));
        }
        Ok(matches!(self.opaque_meta(id)?.loc, OLoc::Dropped { .. }))
    }

    fn opaque_id(&self, oh: OpaqueHole, m: &mut Mutation, pending: &mut HashMap<u128, u64>) -> Result<u64> {
        let keep = self.opts.keep.keeps(oh.kind);
        let h = opaque_hash(oh.enc as u8, &oh.payload);
        let found = match pending.get(&h) {
            Some(&i) => Some(i),
            None => self.find_by_hash(1, h)?,
        };
        if let Some(i) = found {
            // a copy already stored is reused, unless it was not kept and this one is
            if !keep || !self.opaque_dropped(i, m)? {
                return Ok(i);
            }
        }
        let id = self.next_opaque() + m.opaques.len() as u64;
        let (payload, dropped) = if keep { (oh.payload, None) } else { (Vec::new(), Some(oh.payload.len() as u32)) };
        m.opaques.push(NewOpaque { id, enc: oh.enc as u8, kind: oh.kind as u8, hash: h, payload, dropped });
        pending.insert(h, id);
        Ok(id)
    }

    /// Add one request. It is visible to reads at once and durable after [`Db::commit`].
    pub fn put(&mut self, p: &Put) -> Result<PutOutcome> {
        if let Some(&r) = self.mem.req_ix.get(p.id) {
            return Ok(PutOutcome::Duplicate(r));
        }
        if let Some(r) = self.find_request_in_files(p.id)? {
            return Ok(PutOutcome::Duplicate(r));
        }
        let sp = split::split(p.body)?;
        if self.opts.verify_split && split::join(&sp)? != p.body {
            return Err(Error::BadRequest("split pieces do not join back to the body".into()));
        }
        let digest = p.digest.unwrap_or_else(|| sha256(p.body));
        let key = (p.session.to_string(), p.agent.to_string());
        let existing = match self.key_ix.get(&key) {
            Some(&s) => Some(s),
            None => self.find_session_in_files(&key)?,
        };
        if let Some(s) = existing {
            self.activate(s)?;
        }
        let sidx = existing.unwrap_or(self.next_session);
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
        let act = existing.map(|i| &self.active[&i]);
        let mut next = act.map(|a| a.core.nodes.len() as u32).unwrap_or(0);
        let mut parent = NONE;
        let mut creating = act.is_none();
        for msg in &sp.messages {
            let h = xxh3_128(&msg.bytes);
            if !creating {
                if let Some(&c) = act.unwrap().children.get(&(parent, h)) {
                    parent = c;
                    continue;
                }
                creating = true;
            }
            let (cut, holes) = split::cut_opaque(&msg.bytes, &self.opts.keep)?;
            let mut hl = Vec::with_capacity(holes.len());
            for oh in holes {
                let off = oh.offset;
                let oid = self.opaque_id(oh, &mut m, &mut po)?;
                hl.push((off, oid));
            }
            m.nodes.push(NewNode { id: next, parent, hash: h, bytes: cut, holes: hl });
            parent = next;
            next += 1;
        }
        m.req.tip = parent;

        let mut notes = Vec::new();
        if let Some(sys) = &sp.system {
            m.req.system = self.piece_id(PIECE_SYSTEM, &sys.bytes, &mut m, &mut pp)?;
            for n in &sys.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp)?;
                notes.push(ReqNote { target: TARGET_SYSTEM, offset: n.offset, text: t });
            }
        }
        if let Some(tools) = &sp.tools {
            m.req.tools = self.piece_id(PIECE_TOOLS, &tools.bytes, &mut m, &mut pp)?;
            for n in &tools.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp)?;
                notes.push(ReqNote { target: TARGET_TOOLS, offset: n.offset, text: t });
            }
        }
        for (i, msg) in sp.messages.iter().enumerate() {
            for n in &msg.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp)?;
                notes.push(ReqNote { target: TARGET_MSG0 + i as u32, offset: n.offset, text: t });
            }
        }
        m.req.notes = notes.into_boxed_slice();
        m.req.skeleton = self.piece_id(PIECE_SKELETON, &sp.skeleton.encode(), &mut m, &mut pp)?;

        let k = act.map(|a| a.core.requests.len() as u32).unwrap_or(0);
        self.wal.append(&m.encode());
        self.apply(m)?;
        Ok(PutOutcome::Stored(req_id(sidx, k)))
    }

    /// Change the in-memory state by one log record. The only way state changes.
    fn apply(&mut self, m: Mutation) -> Result<()> {
        if m.seq <= self.seq {
            return corrupt(format!("log record {} after {}", m.seq, self.seq));
        }
        let s = m.session;
        if let Some((i, sk, ak)) = m.new_session {
            if i != self.next_session || i != s {
                return corrupt("new session out of order");
            }
            let core = SessionCore { key: (sk, ak), ..SessionCore::default() };
            self.key_ix.insert(core.key.clone(), i);
            self.active.insert(i, Active { core: Arc::new(core), steps: Arc::new(SessionSteps::default()), children: HashMap::new(), last: self.flushes });
            self.next_session += 1;
        } else {
            if s >= self.next_session {
                return corrupt("record names a missing session");
            }
            self.activate(s)?;
        }
        for (id, kind, h, b) in m.pieces {
            if id != self.next_piece() {
                return corrupt("piece out of order");
            }
            self.mem.bytes += b.len();
            self.mem.pieces.push((kind, h, b.into()));
            self.mem.piece_ix.insert(h, id);
        }
        for o in m.opaques {
            if o.id != self.next_opaque() {
                return corrupt("opaque value out of order");
            }
            let payload = match o.dropped {
                Some(_) => None,
                None => {
                    self.mem.bytes += o.payload.len();
                    Some(o.payload.into_boxed_slice())
                }
            };
            let len = o.dropped.unwrap_or(payload.as_ref().map_or(0, |p| p.len() as u32));
            self.mem.opaques.push((o.enc, o.kind, o.hash, payload, len));
            self.mem.opaque_ix.insert(o.hash, o.id);
        }
        let time_ms = steps::parse_time_ms(&m.req.time);
        for n in m.nodes {
            let id = n.id;
            let a = self.active.get_mut(&s).unwrap();
            let core = Arc::make_mut(&mut a.core);
            if n.id as usize != core.nodes.len() {
                return corrupt("node out of order");
            }
            if n.parent != NONE && n.parent >= n.id {
                return corrupt("node parent after the node");
            }
            a.children.insert((n.parent, n.hash), n.id);
            core.nodes.push(NodeMeta {
                parent: n.parent,
                len: n.bytes.len() as u32,
                hash: n.hash,
                loc: Loc::Mem(self.mem.nodes.len() as u32),
                holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) },
            });
            self.mem.bytes += n.bytes.len() + 64;
            self.mem.nodes.push(n.bytes.into_boxed_slice());
            self.add_node_steps(s, id, time_ms)?;
        }
        let a = self.active.get_mut(&s).unwrap();
        let core = Arc::make_mut(&mut a.core);
        if m.req.tip != NONE && m.req.tip as usize >= core.nodes.len() {
            return corrupt("request tip past the session");
        }
        let k = core.requests.len() as u32;
        self.mem.req_ix.insert(m.req.id.clone(), req_id(s, k));
        self.mem.bytes += 256;
        core.requests.push(m.req);
        a.last = self.flushes;
        self.mem.touched.insert(s);
        self.seq = m.seq;
        Ok(())
    }

    fn opaque_text_len(&self, id: u64) -> u64 {
        let Ok(om) = self.opaque_meta(id) else { return 0 };
        let n = match om.loc {
            OLoc::Blob { len, .. } | OLoc::Dropped { len } => len as u64,
            OLoc::Mem(mi) => self.mem.opaques[mi as usize].3.as_ref().map_or(0, |p| p.len() as u64),
        };
        if om.enc == 0 { n.div_ceil(3) * 4 } else { n }
    }

    /// Whether `anc` is on the path from the root to `from` (inclusive).
    fn on_path(core: &SessionCore, anc: u32, mut from: u32) -> bool {
        while from != NONE && from >= anc {
            if from == anc {
                return true;
            }
            from = core.nodes[from as usize].parent;
        }
        false
    }

    /// Walk up from `from` looking for the message that holds tool call `id`.
    #[allow(clippy::type_complexity)]
    fn find_call(&self, core: &SessionCore, mut from: u32, id: &[u8]) -> Result<Option<(u32, u16, Vec<u8>, Vec<steps::RawField>)>> {
        let mut cache = HashMap::new();
        let mut buf = Vec::new();
        let mut hops = 0;
        while from != NONE && hops < 4096 {
            let nm = &core.nodes[from as usize];
            buf.clear();
            self.stored_bytes(core, nm, &mut cache, &mut buf)?;
            let ex = steps::extract(&buf);
            if let Some(c) = ex.calls.into_iter().find(|c| c.0 == id) {
                return Ok(Some((from, c.1, c.2, c.3)));
            }
            from = nm.parent;
            hops += 1;
        }
        Ok(None)
    }

    /// Pair the tool results in node `id` of active session `si` with their calls, and
    /// remember its calls.
    fn add_node_steps(&mut self, si: u32, id: u32, time_ms: i64) -> Result<()> {
        let a = &self.active[&si];
        let core = a.core.clone();
        let nm = &core.nodes[id as usize];
        let Loc::Mem(mi) = nm.loc else { return corrupt("new node is not in memory") };
        let ex = steps::extract(&self.mem.nodes[mi as usize]);
        let parent = nm.parent;
        let holes: Vec<(u32, u64)> = nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default();
        let mut found = Vec::with_capacity(ex.results.len());
        for (block, tuid, failed, span) in ex.results {
            let cached = self.active[&si].steps.calls.get(&tuid[..]).filter(|c| Self::on_path(&core, c.node, parent)).cloned();
            let call = match cached {
                Some(c) => Some(c),
                None => match self.find_call(&core, parent, &tuid)? {
                    Some((n, b, name, fields)) => {
                        let a = self.active.get_mut(&si).unwrap();
                        let ss = Arc::make_mut(&mut a.steps);
                        let ci = self.sd.call_info(ss, n, b, &name, &fields);
                        ss.calls.insert(tuid.clone().into(), ci.clone());
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
        let a = self.active.get_mut(&si).unwrap();
        let ss = Arc::make_mut(&mut a.steps);
        for (tid, block, name, fields) in ex.calls {
            let ci = self.sd.call_info(ss, id, block, &name, &fields);
            ss.calls.insert(tid.into(), ci);
        }
        if ss.tail.len() != id as usize {
            return corrupt("step tails out of step with nodes");
        }
        let mut tail = if parent == NONE { NONE } else { ss.tail[parent as usize] };
        for (call, block, failed, out_bytes) in found {
            let prev = ss.tail[call.node as usize];
            let row = StepRow { call_node: call.node, result_node: id, prev, tool: call.tool, time_ms, out_bytes, call_block: call.block, result_block: block, failed };
            tail = ss.push(row, &call.fields);
        }
        ss.tail.push(tail);
        Ok(())
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

    /// Steps `rows` of a session with the segment's tool and field codes and a dictionary
    /// of their own values.
    fn step_part(&self, w: &mut SegWriter, ss: &SessionSteps, rows: std::ops::Range<u32>) -> StepPart {
        let mut vl: HashMap<(u32, u32), u32> = HashMap::new();
        let mut p = StepPart { first: rows.start, ..StepPart::default() };
        for pos in rows {
            let mut r = ss.rows[pos as usize];
            r.tool = w.tool_code(self.sd.tools.get(r.tool));
            let mut fs = Vec::new();
            for &(f, v) in ss.fields_of(pos) {
                let lf = w.field_code(self.sd.fields.get(f));
                let lv = if steps::is_long(v) {
                    p.longs.push(ss.long_at(v));
                    steps::LONG_BASE + p.longs.len() as u32 - 1
                } else {
                    *vl.entry((f, v)).or_insert_with(|| {
                        p.values.push((lf, ss.value(f, v).to_vec()));
                        p.values.len() as u32 - 1
                    })
                };
                fs.push((lf, lv));
            }
            p.rows.push(r);
            p.fields.push(fs);
        }
        p
    }

    /// Write nodes `nodes` and requests `reqs` of a session as one part, main line first.
    fn core_part(&self, w: &mut SegWriter, core: &SessionCore, nodes: std::ops::Range<u32>, reqs: std::ops::Range<u32>) -> Result<CorePart> {
        let (first, n) = (nodes.start, nodes.end - nodes.start);
        let first_req = reqs.start;
        let tips = core.requests[reqs.start as usize..reqs.end as usize].iter().map(|r| r.tip);
        let order = layout::heavy_first(first, n, |id| core.nodes[id as usize].parent, tips);
        let mut stream = Vec::new();
        let mut nodes = Vec::with_capacity(order.len());
        let mut cache = HashMap::new();
        for id in order {
            let nm = &core.nodes[id as usize];
            nodes.push(SegNode { id, parent: nm.parent, hash: nm.hash, off: stream.len() as u32, len: nm.len, holes: nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default() });
            self.stored_bytes(core, nm, &mut cache, &mut stream)?;
        }
        if stream.len() > u32::MAX as usize {
            return Err(Error::BadRequest("a session part is over 4 GiB".into()));
        }
        let frames = w.frames(&stream)?;
        Ok(CorePart { frames, raw_len: stream.len() as u64, nodes, first_node: first, first_req, requests: core.requests[reqs.start as usize..reqs.end as usize].to_vec() })
    }

    /// Write everything in memory to a new segment and blob file, and trim the log.
    pub fn flush(&mut self) -> Result<()> {
        self.wal.commit()?;
        if self.mem.touched.is_empty() && self.mem.pieces.is_empty() && self.mem.opaques.is_empty() {
            return Ok(());
        }
        let new_wal = self.wal.num + 1;
        self.wal = WalWriter::create(&self.dir, new_wal, self.opts.sync)?;
        let mut man = self.manifest.clone();
        let num = man.next_file;
        man.next_file += 1;
        let mut w = SegWriter::create(&self.dir, num, self.opts.frame_size, self.opts.level, 0, self.seq)?;
        let mut written: Vec<(u32, CorePart)> = Vec::new();
        for &s in &self.mem.touched {
            let a = &self.active[&s];
            let (core, ss) = (&a.core, &a.steps);
            let cp = self.core_part(&mut w, core, core.flushed_nodes..core.nodes.len() as u32, core.flushed_reqs..core.requests.len() as u32)?;
            let sp = self.step_part(&mut w, ss, ss.flushed..ss.rows.len() as u32);
            w.add(s, core.key.clone(), &cp, &sp)?;
            written.push((s, cp));
        }
        w.finish(self.opts.sync)?;
        man.segs.push(num);
        let mut blob_num = None;
        if !self.mem.pieces.is_empty() || !self.mem.opaques.is_empty() {
            let bn = man.next_file;
            man.next_file += 1;
            let mut bw = BlobWriter::create(&self.dir, bn, self.opts.level, self.manifest.pieces, self.manifest.opaques)?;
            for (kind, h, raw) in &self.mem.pieces {
                bw.piece(*kind, *h, raw)?;
            }
            // values in the order requests read them: by session, then by message
            let lo = self.manifest.opaques;
            let n = self.mem.opaques.len();
            let mut order = Vec::with_capacity(n);
            let mut placed = vec![false; n];
            for &s in &self.mem.touched {
                let core = &self.active[&s].core;
                for nm in &core.nodes[core.flushed_nodes as usize..] {
                    for &(_, oid) in nm.holes.iter().flat_map(|h| h.iter()) {
                        if oid >= lo && !std::mem::replace(&mut placed[(oid - lo) as usize], true) {
                            order.push((oid - lo) as usize);
                        }
                    }
                }
            }
            order.extend((0..n).filter(|&i| !placed[i]));
            bw.opaques(n);
            for i in order {
                let (enc, kind, h, payload, len) = &self.mem.opaques[i];
                match payload {
                    Some(p) => bw.opaque(i, *enc, *kind, *h, p)?,
                    None => bw.dropped(i, *enc, *kind, *h, *len),
                }
            }
            bw.finish(self.opts.sync)?;
            man.blobs.push(bn);
            blob_num = Some(bn);
        }
        man.generation += 1;
        man.checkpoint = self.seq;
        man.wal_start = new_wal;
        man.sessions = self.next_session;
        man.pieces = self.next_piece();
        man.opaques = self.next_opaque();
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;

        let seg = Segment::open(&self.dir, num)?;
        self.merge_stats.flushed += seg.file_bytes;
        self.add_segment(seg);
        if let Some(bn) = blob_num {
            let b = Blob::open(&self.dir, bn)?;
            self.merge_stats.flushed += b.file_bytes;
            self.blobs.push(Arc::new(b));
        }
        for (s, cp) in written {
            let a = self.active.get_mut(&s).unwrap();
            let core = Arc::make_mut(&mut a.core);
            let chunk = core.chunks.len() as u32;
            core.chunks.push(Chunk { seg: num, frames: cp.frames.into(), raw_len: cp.raw_len });
            for n in &cp.nodes {
                core.nodes[n.id as usize].loc = Loc::Seg { chunk, off: n.off };
            }
            core.flushed_nodes = core.nodes.len() as u32;
            core.flushed_reqs = core.requests.len() as u32;
            let ss = Arc::make_mut(&mut a.steps);
            ss.flushed = ss.rows.len() as u32;
        }
        self.mem = Mem::default();
        self.flushes += 1;
        // sessions that stopped taking writes leave memory (they are all in files now)
        let idle: Vec<u32> = self.active.iter().filter(|(_, a)| a.last + self.opts.idle_flushes <= self.flushes).map(|(&s, _)| s).collect();
        for s in idle {
            if let Some(a) = self.active.remove(&s) {
                self.key_ix.remove(&a.core.key);
            }
        }
        self.remove_old_wal(new_wal)?;
        self.merge_policy()
    }

    /// Once `merge_at` flushed segments wait, merge them; then merge any `merge_at` merged
    /// segments of one size tier (sizes in powers of 4 from 1 MB) under the size cap. Each
    /// byte is rewritten about once per tier, and finished data stops moving at the cap.
    fn merge_policy(&mut self) -> Result<()> {
        let l0: BTreeSet<u32> = self.segs.values().filter(|s| s.cat.level == 0).map(|s| s.num).collect();
        if l0.len() >= self.opts.merge_at {
            self.merge(l0)?;
        }
        let tier = |b: u64| {
            let (mut t, mut x) = (0u32, b >> 20);
            while x >= 4 {
                x /= 4;
                t += 1;
            }
            t
        };
        loop {
            let mut tiers: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for sg in self.segs.values().filter(|s| s.cat.level > 0 && s.file_bytes < self.opts.seg_bytes) {
                tiers.entry(tier(sg.file_bytes)).or_default().push(sg.num);
            }
            let Some((_, group)) = tiers.into_iter().find(|(_, v)| v.len() >= self.opts.merge_at) else { break };
            self.merge(group.into_iter().take(self.opts.merge_at).collect())?;
        }
        Ok(())
    }

    fn remove_old_wal(&self, keep_from: u64) -> Result<()> {
        for e in fs::read_dir(&self.dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if let Some(n) = parse_num(&name, "wal-", ".log")
                && n < keep_from
            {
                fs::remove_file(self.dir.join(&name))?;
            }
        }
        Ok(())
    }

    /// Rewrite segments `inputs` into merged segments split by size. For each session, each
    /// run of its consecutive parts that all sit in `inputs` becomes one part, main line
    /// first; parts elsewhere (a session that woke up after its data was merged) stay put.
    fn merge(&mut self, inputs: BTreeSet<u32>) -> Result<()> {
        if inputs.is_empty() {
            return Ok(());
        }
        let mut sessions = BTreeSet::new();
        let mut bytes_in = 0;
        for &n in &inputs {
            let seg = self.segs.get(&n).ok_or_else(|| bad(format!("merge of a missing segment {n}")))?.clone();
            bytes_in += seg.file_bytes;
            for d in self.dir_of(&seg)?.iter() {
                sessions.insert(d.session);
            }
        }
        let mut man = self.manifest.clone();
        let mut outputs: Vec<u32> = Vec::new();
        let mut w: Option<SegWriter> = None;
        for &s in &sessions {
            let parts = self.parts(s)?;
            let mut runs = Vec::new();
            let mut i = 0;
            while i < parts.len() {
                if !inputs.contains(&parts[i].0.num) {
                    i += 1;
                    continue;
                }
                let mut j = i + 1;
                while j < parts.len() && inputs.contains(&parts[j].0.num) {
                    j += 1;
                }
                runs.push((i, j));
                i = j;
            }
            if runs.is_empty() {
                continue;
            }
            if w.as_ref().is_some_and(|w| w.sessions() > 0 && w.written() >= self.opts.seg_bytes) {
                w.take().unwrap().finish(self.opts.sync)?;
            }
            if w.is_none() {
                let num = man.next_file;
                man.next_file += 1;
                outputs.push(num);
                w = Some(SegWriter::create(&self.dir, num, self.opts.frame_size, self.opts.level, 1, self.manifest.checkpoint)?);
            }
            let core = self.load_core(s)?;
            let ss = self.load_steps(s)?;
            let wr = w.as_mut().unwrap();
            for (a, b) in runs {
                let (pa, pb) = (&parts[a].1, &parts[b - 1].1);
                let cp = self.core_part(wr, &core, pa.first_node..pb.first_node + pb.n_nodes, pa.first_req..pb.first_req + pb.n_reqs)?;
                let sp = self.step_part(wr, &ss, pa.first_row..pb.first_row + pb.n_rows);
                wr.add(s, core.key.clone(), &cp, &sp)?;
            }
        }
        if let Some(w) = w {
            w.finish(self.opts.sync)?;
        }
        man.generation += 1;
        man.segs.retain(|n| !inputs.contains(n));
        man.segs.extend(&outputs);
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        // sessions that point at the old files are read again from the new ones
        self.active.retain(|_, a| a.core.flushed_nodes as usize != a.core.nodes.len() || a.core.flushed_reqs as usize != a.core.requests.len());
        if !self.active.is_empty() {
            return corrupt("merge with unflushed sessions");
        }
        self.key_ix.clear();
        for c in [&self.cores as &dyn Clear, &self.stepc, &self.dirs, &self.reqtabs, &self.blocks] {
            c.clear_all();
        }
        let mut bytes_out = 0;
        for &n in &outputs {
            let seg = Segment::open(&self.dir, n)?;
            bytes_out += seg.file_bytes;
            self.add_segment(seg);
        }
        for n in &inputs {
            self.segs.remove(n);
            self.seg_codes.remove(n);
            let p = seg_path(&self.dir, *n);
            crate::block::forget(&p);
            let _ = fs::remove_file(p);
        }
        self.merge_stats.merges += 1;
        self.merge_stats.bytes_in += bytes_in;
        self.merge_stats.bytes_out += bytes_out;
        Ok(())
    }

    /// Flush, then merge every segment into level 1: each session stored whole, main line first.
    pub fn compact(&mut self) -> Result<()> {
        self.flush()?;
        let all: BTreeSet<u32> = self.segs.keys().copied().collect();
        self.merge(all)
    }

    // ------------------------------------------------------------------ reads

    /// Append a node's stored bytes (opaque values still cut out).
    fn stored_bytes(&self, core: &SessionCore, nm: &NodeMeta, cache: &mut HashMap<(u32, usize), Arc<Vec<u8>>>, out: &mut Vec<u8>) -> Result<()> {
        match nm.loc {
            Loc::Mem(mi) => out.extend_from_slice(&self.mem.nodes[mi as usize]),
            Loc::Seg { chunk, off } => {
                if nm.len == 0 {
                    return Ok(());
                }
                let ch = &core.chunks[chunk as usize];
                let sg = self.segs.get(&ch.seg).ok_or_else(|| bad(format!("segment {} is gone", ch.seg)))?;
                let fs = sg.cat.frame_size as usize;
                let (a, b) = (off as usize, off as usize + nm.len as usize);
                let (f0, f1) = (a / fs, (b - 1) / fs);
                let missing: Vec<usize> = (f0..=f1).filter(|k| !cache.contains_key(&(chunk, *k))).collect();
                if let (Some(&lo), Some(&hi)) = (missing.first(), missing.last()) {
                    for (k, d) in (lo..=hi).zip(sg.frames(&ch.frames, ch.raw_len, lo, hi)?) {
                        cache.entry((chunk, k)).or_insert(d);
                    }
                }
                for k in f0..=f1 {
                    let fr = &cache[&(chunk, k)];
                    let s = a.max(k * fs) - k * fs;
                    let e = b.min((k + 1) * fs) - k * fs;
                    out.extend_from_slice(&fr[s..e]);
                }
            }
        }
        Ok(())
    }

    fn opaque_payload(&self, om: &OpaqueMeta, st: &mut ReadStats) -> Result<Option<Vec<u8>>> {
        Ok(match om.loc {
            OLoc::Dropped { .. } => None,
            OLoc::Mem(mi) => self.mem.opaques[mi as usize].3.as_ref().map(|p| p.to_vec()),
            OLoc::Blob { blob, off, len } => {
                let b = self.blobs.iter().find(|b| b.num == blob).ok_or_else(|| bad("blob file is gone"))?;
                st.opaque_reads += 1;
                Some(b.read(off, len)?)
            }
        })
    }

    /// Read nodes of one session, in the order given, with opaque values put back.
    /// Each node's bytes go to `out`; `spans` gets their ranges. Values not kept are left
    /// out; `drops` gets (index in `ids`, offset in the full message, text length) for each.
    fn read_nodes(&self, core: &SessionCore, ids: &[u32], out: &mut Vec<u8>, spans: &mut Vec<(usize, usize)>, drops: &mut Vec<(u32, u32, u32)>, st: &mut ReadStats) -> Result<()> {
        // which frames are needed, read with one call per run of adjacent frames
        let mut need: BTreeSet<(u32, usize)> = BTreeSet::new();
        let mut prev: Option<(u32, u32)> = None;
        for &id in ids {
            let nm = &core.nodes[id as usize];
            match nm.loc {
                Loc::Seg { chunk, off } => {
                    let fs = self.segs.get(&core.chunks[chunk as usize].seg).map_or(1, |s| s.cat.frame_size as usize);
                    if nm.len > 0 {
                        for k in off as usize / fs..=(off as usize + nm.len as usize - 1) / fs {
                            need.insert((chunk, k));
                        }
                    }
                    if prev != Some((chunk, off)) {
                        st.runs += 1;
                    }
                    prev = Some((chunk, off + nm.len));
                }
                Loc::Mem(_) => {
                    st.runs += 1;
                    prev = None;
                }
            }
        }
        let mut cache: HashMap<(u32, usize), Arc<Vec<u8>>> = HashMap::with_capacity(need.len());
        let v: Vec<_> = need.into_iter().collect();
        let mut i = 0;
        while i < v.len() {
            let (chunk, k0) = v[i];
            let mut j = i;
            while j + 1 < v.len() && v[j + 1] == (chunk, v[j].1 + 1) {
                j += 1;
            }
            let ch = &core.chunks[chunk as usize];
            let sg = self.segs.get(&ch.seg).ok_or_else(|| bad(format!("segment {} is gone", ch.seg)))?;
            let frames = sg.frames(&ch.frames, ch.raw_len, k0, v[j].1)?;
            st.reads += 1;
            st.frames += frames.len() as u32;
            for (n, d) in frames.into_iter().enumerate() {
                cache.insert((chunk, k0 + n), d);
            }
            i = j + 1;
        }
        // opaque values: read the ones in blob files with as few reads as possible (a
        // session's values sit close together in each blob file)
        let mut metas: HashMap<u64, OpaqueMeta> = HashMap::new();
        let mut tables = HashMap::new();
        let mut wanted: Vec<(u32, u64, u32, u64)> = Vec::new(); // (blob, off, len, id)
        for &id in ids {
            if let Some(h) = &core.nodes[id as usize].holes {
                for &(_, oid) in h.iter() {
                    if let std::collections::hash_map::Entry::Vacant(e) = metas.entry(oid) {
                        let om = self.opaque_meta_in(oid, &mut tables)?;
                        if let OLoc::Blob { blob, off, len } = om.loc {
                            wanted.push((blob, off, len, oid));
                        }
                        e.insert(om);
                    }
                }
            }
        }
        let mut fetched: HashMap<u64, Vec<u8>> = HashMap::with_capacity(wanted.len());
        if !wanted.is_empty() {
            wanted.sort_unstable();
            const GAP: u64 = 64 * 1024;
            let mut i = 0;
            while i < wanted.len() {
                let (blob, start, _, _) = wanted[i];
                let mut end = start + wanted[i].2 as u64;
                let mut j = i;
                while j + 1 < wanted.len() && wanted[j + 1].0 == blob && wanted[j + 1].1 <= end + GAP {
                    j += 1;
                    end = end.max(wanted[j].1 + wanted[j].2 as u64);
                }
                let b = self.blobs.iter().find(|b| b.num == blob).ok_or_else(|| bad("blob file is gone"))?;
                let buf = b.read(start, (end - start) as u32)?;
                st.opaque_reads += 1;
                for w in &wanted[i..=j] {
                    let a = (w.1 - start) as usize;
                    fetched.insert(w.3, buf[a..a + w.2 as usize].to_vec());
                }
                i = j + 1;
            }
        }
        let mut tmp = Vec::new();
        for (k, &id) in ids.iter().enumerate() {
            let nm = &core.nodes[id as usize];
            let start = out.len();
            match &nm.holes {
                None => self.stored_bytes(core, nm, &mut cache, out)?,
                Some(holes) => {
                    tmp.clear();
                    self.stored_bytes(core, nm, &mut cache, &mut tmp)?;
                    let mut pos = 0usize;
                    let mut gone = 0u32;
                    for &(o, oid) in holes.iter() {
                        out.extend_from_slice(&tmp[pos..o as usize]);
                        pos = o as usize;
                        let om = metas[&oid];
                        let enc = if om.enc == 0 { OpaqueEnc::Base64 } else { OpaqueEnc::Text };
                        let p = match fetched.remove(&oid) {
                            Some(p) => Some(p),
                            None => self.opaque_payload(&om, st)?,
                        };
                        match p {
                            None => {
                                let n = match om.loc {
                                    OLoc::Dropped { len } => len,
                                    _ => 0,
                                };
                                let len = if om.enc == 0 { n.div_ceil(3) * 4 } else { n };
                                drops.push((k as u32, (out.len() - start) as u32 + gone, len));
                                gone += len;
                            }
                            Some(p) => {
                                if opaque_hash(om.enc, &p) != om.hash {
                                    return corrupt(format!("opaque value {oid} fails its checksum"));
                                }
                                split::opaque_text(enc, &p, out);
                                fetched.insert(oid, p);
                            }
                        }
                    }
                    out.extend_from_slice(&tmp[pos..]);
                }
            }
            spans.push((start, out.len()));
        }
        Ok(())
    }

    fn piece(&self, id: u32) -> Result<Arc<Vec<u8>>> {
        if id >= self.manifest.pieces {
            let (_, _, b) = self.mem.pieces.get((id - self.manifest.pieces) as usize).ok_or_else(|| bad(format!("missing piece {id}")))?;
            return Ok(Arc::new(b.to_vec()));
        }
        if let Some(p) = self.pieces.get(&id) {
            return Ok(p);
        }
        let b = self.blob_for_piece(id)?;
        let i = (id - b.cat.piece_lo) as usize;
        let t = self.tblock(b, PIECES, i / ID_BLOCK)?;
        let TBlock::Pieces(v) = &*t else { return corrupt("blob block of the wrong kind") };
        let e = *v.get(i % ID_BLOCK).ok_or_else(|| bad(format!("missing piece {id}")))?;
        let c = b.read(e.off, e.clen)?;
        let d = crate::segment::decompress(&c, e.rawlen as usize)?;
        if d.len() != e.rawlen as usize {
            return corrupt("piece has the wrong size");
        }
        let n = d.len();
        Ok(self.pieces.insert(id, Arc::new(d), n))
    }

    fn skeleton(&self, id: u32) -> Result<Arc<Skeleton>> {
        if let Some(s) = self.skels.get(&id) {
            return Ok(s);
        }
        let p = self.piece(id)?;
        let n = p.len() + 64;
        Ok(self.skels.insert(id, Arc::new(Skeleton::decode(&p)?), n))
    }

    fn path(core: &SessionCore, tip: u32, n: u32) -> Result<Vec<u32>> {
        let mut v = Vec::with_capacity(n as usize);
        let mut c = tip;
        while c != NONE {
            v.push(c);
            if v.len() > n as usize {
                break;
            }
            c = core.nodes[c as usize].parent;
        }
        if v.len() != n as usize {
            return corrupt("request path has the wrong length");
        }
        v.reverse();
        Ok(v)
    }

    /// Put cache markers back into `cut`. Their offsets count in the full message; `drops`
    /// lists (offset, length) of values left out of `cut` before them.
    fn with_notes(&self, cut: &[u8], notes: &[ReqNote], drops: &[(u32, u32, u32)], out: &mut Vec<u8>) -> Result<()> {
        let mut pos = 0usize;
        for n in notes {
            let gone: u32 = drops.iter().filter(|d| d.1 < n.offset).map(|d| d.2).sum();
            let o = n.offset.checked_sub(gone).ok_or_else(|| bad("cache note inside a value not kept"))? as usize;
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

    fn core_req(&self, ri: ReqId) -> Result<(Arc<SessionCore>, u32)> {
        let (s, k) = req_parts(ri);
        if s >= self.next_session {
            return Err(Error::BadRequest("no such request".into()));
        }
        let core = self.load_core(s)?;
        if k as usize >= core.requests.len() {
            return Err(Error::BadRequest("no such request".into()));
        }
        Ok((core, k))
    }

    /// Rebuild the exact body of request `ri`.
    pub fn rebuild_into(&self, ri: ReqId, out: &mut Vec<u8>, st: &mut ReadStats) -> Result<()> {
        let (core, k) = self.core_req(ri)?;
        let r = &core.requests[k as usize];
        let path = Self::path(&core, r.tip, r.n_msgs)?;
        let mut mbuf = Vec::new();
        let mut spans = Vec::with_capacity(path.len());
        let mut drops = Vec::new();
        self.read_nodes(&core, &path, &mut mbuf, &mut spans, &mut drops, st)?;
        let skel = self.skeleton(r.skeleton)?;
        let notes = &r.notes[..];
        let of = |t: u32| -> &[ReqNote] {
            let a = notes.partition_point(|n| n.target < t);
            let b = notes.partition_point(|n| n.target <= t);
            &notes[a..b]
        };
        out.reserve(mbuf.len() + 4096);
        split::assemble(&skel, out, |kind, out| {
            match kind {
                HoleKind::Messages => {
                    for (i, &(a, b)) in spans.iter().enumerate() {
                        match &r.layout {
                            Some(g) => out.extend_from_slice(&g[i]),
                            None if i > 0 => out.push(b','),
                            None => {}
                        }
                        let d0 = drops.partition_point(|d| (d.0 as usize) < i);
                        let d1 = drops.partition_point(|d| (d.0 as usize) <= i);
                        self.with_notes(&mbuf[a..b], of(TARGET_MSG0 + i as u32), &drops[d0..d1], out)?;
                    }
                    if let Some(g) = &r.layout {
                        out.extend_from_slice(&g[spans.len()]);
                    }
                }
                HoleKind::System => self.with_notes(&self.piece(r.system)?, of(TARGET_SYSTEM), &[], out)?,
                HoleKind::Tools => self.with_notes(&self.piece(r.tools)?, of(TARGET_TOOLS), &[], out)?,
            }
            Ok(())
        })
    }

    pub fn rebuild(&self, ri: ReqId) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.rebuild_into(ri, &mut out, &mut ReadStats::default())?;
        Ok(out)
    }

    /// Rebuild and check the result against the hash taken when the request arrived.
    pub fn rebuild_checked(&self, ri: ReqId) -> Result<Vec<u8>> {
        if self.request_has_dropped(ri)? {
            return Err(Error::BadRequest("the request held values the store did not keep".into()));
        }
        let out = self.rebuild(ri)?;
        let r = self.request(ri)?;
        if sha256(&out) != r.digest {
            return corrupt(format!("request {} does not rebuild to its digest", r.id));
        }
        Ok(out)
    }

    pub fn request_index(&self, id: &str) -> Option<ReqId> {
        if let Some(&r) = self.mem.req_ix.get(id) {
            return Some(r);
        }
        self.find_request_in_files(id).ok().flatten()
    }

    pub fn request(&self, ri: ReqId) -> Result<ReqRec> {
        let (core, k) = self.core_req(ri)?;
        Ok(core.requests[k as usize].clone())
    }

    pub fn request_count(&self) -> usize {
        self.segs.values().map(|s| s.cat.requests as usize).sum::<usize>() + self.mem.req_ix.len()
    }

    /// Every stored request, session by session (reads every session's metadata).
    pub fn all_requests(&self) -> Result<Vec<ReqId>> {
        let mut out = Vec::with_capacity(self.request_count());
        for s in 0..self.next_session {
            let n = self.load_core(s)?.requests.len() as u32;
            out.extend((0..n).map(|k| req_id(s, k)));
        }
        Ok(out)
    }

    pub fn session_count(&self) -> usize {
        self.next_session as usize
    }

    /// The session's (session id, agent id). Panics if the session cannot be read.
    pub fn session_key(&self, s: u32) -> (String, String) {
        self.load_core(s).expect("read session").key.clone()
    }

    pub fn session_len(&self, s: u32) -> u32 {
        self.load_core(s).expect("read session").nodes.len() as u32
    }

    /// Node ids of a request's messages, in order.
    pub fn request_path(&self, ri: ReqId) -> Result<Vec<u32>> {
        let (core, k) = self.core_req(ri)?;
        let r = &core.requests[k as usize];
        Self::path(&core, r.tip, r.n_msgs)
    }

    /// Read some messages of a session (node ids), with opaque values put back.
    pub fn read_messages(&self, s: u32, ids: &[u32], st: &mut ReadStats) -> Result<Vec<Vec<u8>>> {
        let core = self.load_core(s)?;
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(&core, ids, &mut buf, &mut spans, &mut Vec::new(), st)?;
        Ok(spans.into_iter().map(|(a, b)| buf[a..b].to_vec()).collect())
    }

    /// Every message of a session in storage order: the whole timeline in one pass.
    pub fn replay(&self, s: u32, st: &mut ReadStats) -> Result<Vec<u8>> {
        let core = self.load_core(s)?;
        let mut ids: Vec<u32> = (0..core.nodes.len() as u32).collect();
        ids.sort_by_key(|&i| match core.nodes[i as usize].loc {
            Loc::Seg { chunk, off } => (0u8, chunk, off),
            Loc::Mem(m) => (1u8, 0, m),
        });
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(&core, &ids, &mut buf, &mut spans, &mut Vec::new(), st)?;
        Ok(buf)
    }

    /// How many separate runs of storage a request's messages occupy.
    pub fn request_runs(&self, ri: ReqId) -> Result<u32> {
        let (core, k) = self.core_req(ri)?;
        let r = &core.requests[k as usize];
        let path = Self::path(&core, r.tip, r.n_msgs)?;
        let mut runs = 0;
        let mut prev: Option<(u32, u32)> = None;
        for id in path {
            let nm = &core.nodes[id as usize];
            match nm.loc {
                Loc::Seg { chunk, off } => {
                    if prev != Some((chunk, off)) {
                        runs += 1;
                    }
                    prev = Some((chunk, off + nm.len));
                }
                Loc::Mem(_) => {
                    runs += 1;
                    prev = None;
                }
            }
        }
        Ok(runs)
    }

    /// Counts and sizes from the file catalogs and memory (reads no session data).
    pub fn stats(&self) -> Stats {
        let mut st = Stats {
            sessions: self.next_session as usize,
            requests: self.request_count(),
            pieces: self.next_piece() as usize,
            opaques: self.next_opaque() as usize,
            segments: self.segs.len(),
            level0: self.segs.values().filter(|s| s.cat.level == 0).count(),
            blobs: self.blobs.len(),
            wal_bytes: self.wal.bytes,
            mem_bytes: self.mem.bytes,
            tools: self.sd.tools.len(),
            step_fields: self.sd.fields.len(),
            orphan_results: self.orphan_results,
            active_sessions: self.active.len(),
            merges: self.merge_stats.clone(),
            ..Stats::default()
        };
        for sg in self.segs.values() {
            let c = &sg.cat;
            st.nodes += c.nodes as usize;
            st.steps += c.steps as usize;
            st.node_bytes += c.node_bytes;
            st.frame_bytes += c.frame_bytes;
            st.seg_file_bytes += sg.file_bytes;
            st.catalog_bytes += c.bytes();
        }
        st.index_bytes = st.seg_file_bytes - st.frame_bytes;
        for b in &self.blobs {
            st.blob_file_bytes += b.file_bytes;
            st.opaque_bytes += b.cat.opaque_bytes;
            st.piece_bytes += b.cat.piece_bytes;
            st.piece_raw_bytes += b.cat.piece_raw_bytes;
            st.opaques_dropped += b.cat.n_dropped as usize;
            st.catalog_bytes += b.cat.bytes();
        }
        st.nodes += self.mem.nodes.len();
        st.node_bytes += self.mem.nodes.iter().map(|n| n.len() as u64).sum::<u64>();
        st.piece_raw_bytes += self.mem.pieces.iter().map(|p| p.2.len() as u64).sum::<u64>();
        for (_, _, _, p, _) in &self.mem.opaques {
            match p {
                Some(p) => st.opaque_bytes += p.len() as u64,
                None => st.opaques_dropped += 1,
            }
        }
        for a in self.active.values() {
            st.steps += a.steps.rows.len() - a.steps.flushed as usize;
            st.active_bytes += a.core.bytes() + a.steps.bytes() + a.children.len() * 40;
        }
        st.caches = vec![
            ("sessions", self.cores.stats()),
            ("steps", self.stepc.stats()),
            ("blocks", self.blocks.stats()),
            ("directories", self.dirs.stats()),
            ("request tables", self.reqtabs.stats()),
            ("blob table blocks", self.tables.stats()),
            ("pieces", self.pieces.stats()),
        ];
        st
    }

    /// A session's steps as columns. Panics if the session cannot be read.
    pub fn steps(&self, s: u32) -> Arc<SessionSteps> {
        self.load_steps(s).expect("read session steps")
    }

    pub fn try_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        self.load_steps(s)
    }

    /// Tool and field names.
    pub fn step_dicts(&self) -> &StepDicts {
        &self.sd
    }

    /// One message as stored: opaque values (signatures, images) still cut out, with the
    /// offsets where they belong. Cheaper than [`Db::message`] when they are not needed.
    pub fn stored_message(&self, s: u32, node: u32) -> Result<(Vec<u8>, Vec<u32>)> {
        let core = self.load_core(s)?;
        let nm = &core.nodes[node as usize];
        let mut out = Vec::with_capacity(nm.len as usize);
        self.stored_bytes(&core, nm, &mut HashMap::new(), &mut out)?;
        Ok((out, nm.holes.as_deref().map(|h| h.iter().map(|x| x.0).collect()).unwrap_or_default()))
    }

    /// Bytes `off..off + len` of a node's stored form, reading only the frames that hold
    /// them; `None` if an opaque value sits inside that range (then read the message).
    pub fn stored_range(&self, s: u32, node: u32, off: u32, len: u32) -> Result<Option<Vec<u8>>> {
        let core = self.load_core(s)?;
        let nm = &core.nodes[node as usize];
        let (a, b) = (off as usize, off as usize + len as usize);
        if b > nm.len as usize {
            return corrupt("range past the end of a message");
        }
        if nm.holes.as_deref().is_some_and(|h| h.iter().any(|&(o, _)| (o as usize) >= a && (o as usize) <= b)) {
            return Ok(None);
        }
        match nm.loc {
            Loc::Mem(mi) => Ok(Some(self.mem.nodes[mi as usize][a..b].to_vec())),
            Loc::Seg { chunk, off: base } => {
                if len == 0 {
                    return Ok(Some(Vec::new()));
                }
                let ch = &core.chunks[chunk as usize];
                let sg = self.segs.get(&ch.seg).ok_or_else(|| bad("segment is gone"))?;
                let fs = sg.cat.frame_size as usize;
                let (ga, gb) = (base as usize + a, base as usize + b);
                let (f0, f1) = (ga / fs, (gb - 1) / fs);
                let frames = sg.frames(&ch.frames, ch.raw_len, f0, f1)?;
                let mut out = Vec::with_capacity(len as usize);
                for (i, fr) in frames.iter().enumerate() {
                    let k = f0 + i;
                    let s0 = ga.max(k * fs) - k * fs;
                    let e0 = gb.min((k + 1) * fs) - k * fs;
                    out.extend_from_slice(&fr[s0..e0]);
                }
                Ok(Some(out))
            }
        }
    }

    /// One message (tree node) of a session, with opaque values put back.
    pub fn message(&self, s: u32, node: u32) -> Result<Vec<u8>> {
        Ok(self.read_messages(s, &[node], &mut ReadStats::default())?.pop().unwrap_or_default())
    }

    /// Parent of every node of a session (`NONE` for roots).
    pub fn node_parents(&self, s: u32) -> Vec<u32> {
        self.load_core(s).expect("read session").nodes.iter().map(|n| n.parent).collect()
    }

    /// Requests of one session.
    pub fn session_requests(&self, s: u32) -> Vec<ReqId> {
        let n = self.load_core(s).expect("read session").requests.len() as u32;
        (0..n).map(|k| req_id(s, k)).collect()
    }

    /// Opaque values by kind: (kind, values kept, bytes kept, values not kept, bytes they had).
    /// Reads every blob table.
    pub fn opaque_kinds(&self) -> Result<Vec<(&'static str, u64, u64, u64, u64)>> {
        let mut out: Vec<(&'static str, u64, u64, u64, u64)> = Vec::new();
        let mut add = |kind: u8, kept: bool, len: u64| {
            let name = OpaqueKind::from_u8(kind).name();
            let i = match out.iter().position(|x| x.0 == name) {
                Some(i) => i,
                None => {
                    out.push((name, 0, 0, 0, 0));
                    out.len() - 1
                }
            };
            if kept {
                (out[i].1, out[i].2) = (out[i].1 + 1, out[i].2 + len);
            } else {
                (out[i].3, out[i].4) = (out[i].3 + 1, out[i].4 + len);
            }
        };
        for b in &self.blobs {
            for i in 0..b.cat.opaque_blocks.len() {
                if let TBlock::Opaques(v) = &*self.tblock(b, OPAQUES, i)? {
                    for o in v {
                        add(o.kind, !o.dropped, o.len as u64);
                    }
                }
            }
        }
        for (_, kind, _, p, len) in &self.mem.opaques {
            add(*kind, p.is_some(), *len as u64);
        }
        out.sort_by_key(|x| std::cmp::Reverse(x.2 + x.4));
        Ok(out)
    }

    /// Whether a request held values the store did not keep (so it cannot rebuild exactly).
    pub fn request_has_dropped(&self, ri: ReqId) -> Result<bool> {
        let (core, k) = self.core_req(ri)?;
        let r = &core.requests[k as usize];
        for id in Self::path(&core, r.tip, r.n_msgs)? {
            if let Some(h) = &core.nodes[id as usize].holes {
                for &(_, oid) in h.iter() {
                    if matches!(self.opaque_meta(oid)?.loc, OLoc::Dropped { .. }) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// How many parts session `s` is stored in.
    pub fn part_count(&self, s: u32) -> Result<usize> {
        Ok(self.parts(s)?.len())
    }

    pub fn segment_count(&self) -> usize {
        self.segs.len()
    }

    /// (segment number, level, lowest session, highest session, file bytes) per segment.
    pub fn segments(&self) -> Vec<(u32, u8, u32, u32, u64)> {
        self.segs.values().map(|s| (s.num, s.cat.level, s.cat.lo, s.cat.hi, s.file_bytes)).collect()
    }

    /// Drop everything read from files from the caches (sessions taking writes stay).
    pub fn clear_caches(&self) {
        for c in [&self.cores as &dyn Clear, &self.stepc, &self.dirs, &self.reqtabs, &self.blocks, &self.tables, &self.pieces, &self.skels] {
            c.clear_all();
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where a blob file lives (for tests that damage files).
    pub fn blob_file(&self, num: u32) -> PathBuf {
        blob_path(&self.dir, num)
    }
}

trait Clear {
    fn clear_all(&self);
}

impl<K: std::hash::Hash + Eq + Clone, V> Clear for Cache<K, V> {
    fn clear_all(&self) {
        self.clear();
    }
}
