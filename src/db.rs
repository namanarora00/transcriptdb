//! The single-node store: write path, flush, merge, recovery and reads.
//!
//! Write path for one request: split the body ([`crate::split`]), walk the messages
//! down the session's tree comparing hashes, keep only the messages the session has
//! not sent before as new nodes, blank thinking and images in those (they are never
//! stored) and cut the other opaque values out, intern the skeleton, system, tools and
//! cache-marker pieces, then append one log record. The record is durable once
//! [`Db::commit`] returns; that is the moment a request counts as saved. Nothing per
//! message or request is stored that the data itself gives back: node hashes are worked
//! out again from the stored bytes when a session wakes, and there is no request digest
//! (`tdb check-claude` checks rebuilds against Claude Code's own `requestDigest`).
//!
//! Everything in memory that is not yet in a file can be rebuilt from the log, because
//! that state only ever changes by applying a log record ([`Db::apply`]), both live and
//! during recovery.
//!
//! What stays in memory hardly grows with the data: the manifest, one small catalog per
//! segment and index run (nothing per key or per block), the sessions taking writes, the
//! unflushed data, and caches with fixed budgets. A session is a self-contained unit (its
//! messages, requests and steps) stored as parts in segments and read from its parts on
//! demand. Shared pieces and opaque values are appended to blob files. The index finds a
//! piece or opaque value by hash or by id, a request by its id and a session by its key;
//! each flush adds an index run and runs merge by size tiers, so a lookup reads a few runs.
//! Flushes write level-0 segments (zstd level 3); merges combine segments by size tiers up
//! to a cap, writing frames and blocks at a higher level (19) with a zstd dictionary trained
//! on what they merge, compressed on several threads. A
//! session that takes writes again after its data reached the cap is folded back into one
//! part by a later merge into full-size segments (`Db::merge`); the parts it replaces
//! stay in their segments, unread, until half of a segment is such parts and it is rewritten.

use crate::blob::{self, blob_path, BlobAppend};
use crate::block::BlockRef;
use crate::cache::{Cache, CacheStats};
use crate::index::{self, IxBlock, OpaqueEnt, PieceEnt, Run, RunWriter, IDS, K_OPAQUES, K_PIECES, NS_OPAQUE, NS_PIECE, NS_REQUEST, NS_SESSION};
use crate::codec::{corrupt, Error, Result};
use crate::layout;
use crate::manifest::{Dead, Manifest};
use crate::record::*;
use crate::segment::{decode_core, decode_steps, key_bytes, seg_path, Codec, CorePart, DirEnt, SegNode, SegWriter, Segment, StepPart};
use crate::split::{self, HoleKind, Keep, OpaqueEnc, OpaqueHole, OpaqueKind, Skeleton};
use crate::stats::{self, FieldDir, FieldRows, StatsHead, ValChunk, Words};
use crate::steps::{self, FieldVal, SessionSteps, StepDicts, StepRow};
use crate::wal::{self, SyncMode, WalWriter};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    /// Which opaque values to keep besides those never kept (thinking and images). Values
    /// not kept are replaced by `""` at ingest, and their requests rebuild that way.
    pub keep: Keep,
    /// Memory for sessions, blocks and tables read from files.
    pub cache_bytes: usize,
    /// Merge once this many flushed (level-0) segments are waiting.
    pub merge_at: usize,
    /// A merge starts a new output segment past this many bytes.
    pub seg_bytes: u64,
    /// Sessions that took no writes for this many flushes stop being kept in memory.
    pub idle_flushes: u64,
    /// Most segment, index and blob files kept open at once (process-wide).
    pub open_files: usize,
    /// A flush starts a new blob file once the last one has this many bytes.
    pub blob_bytes: u64,
    /// Index runs this big are not merged again.
    pub run_bytes: u64,
    /// A merge into full-size segments folds a session into one part when it has gone quiet
    /// or would otherwise be left in more parts than this; 0 turns folding off.
    pub max_parts: usize,
    /// zstd level for what merges into full-size segments (and `compact`) write; flushes and
    /// smaller merges use `level`. Also the level of new shared pieces.
    pub merge_level: i32,
    /// Most bytes of the zstd dictionary such a merge trains on its input and compresses its
    /// output's frames with (about 1% of the messages merged, up to this); 0 for none.
    pub dict_bytes: usize,
    /// Most bytes of the merge's own messages, sampled evenly, added after the trained
    /// dictionary (about 1/12 of the messages merged, up to this). Every thread reading a
    /// segment keeps a copy of its dictionary, so this costs memory per reading thread.
    pub dict_content: usize,
    /// A session whose last request is more than `cold_after_ms` older than the newest
    /// request in the store is cold: merges into full-size segments write its messages in
    /// runs of up to `cold_run` bytes instead of `frame_size` frames (about 17% smaller;
    /// reading one message then decodes up to a whole run). 0 for `cold_run`: never.
    pub cold_after_ms: i64,
    pub cold_run: usize,
    /// Threads compressing frames in merges.
    pub threads: usize,
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
            blob_bytes: 1 << 30,
            run_bytes: 1 << 30,
            max_parts: 4,
            merge_level: 19,
            dict_bytes: 1 << 20,
            dict_content: 8 << 20,
            cold_after_ms: 7 * 86_400_000,
            cold_run: 4 << 20,
            threads: std::thread::available_parallelism().map_or(4, |n| n.get()),
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
    loc: Loc,
    holes: Option<Box<[(u32, u64)]>>,
}

/// One part's message stream in a segment.
#[derive(Debug, Clone)]
struct Chunk {
    seg: u32,
    frames: Arc<[(u64, u32)]>,
    raw_len: u64,
    /// Raw bytes per frame.
    fs: u32,
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
    /// (parent, key) -> child. A node's key is the xxh3 of its message as stored (values
    /// not kept blanked, kept ones in place); messages that came with values since blanked
    /// are found by the hash of the message as it came too.
    children: HashMap<(u32, u128), u32>,
    /// Nodes read back from segments when the session woke, by parent: hashes are not
    /// stored, so their keys are worked out from their bytes the first time a walk passes
    /// their parent (which then goes in `keyed`).
    stored_kids: Vec<u32>,
    keyed: std::collections::HashSet<u32>,
    /// Flush count when it last took a write.
    last: u64,
}

impl Active {
    fn new(core: Arc<SessionCore>, steps: Arc<SessionSteps>, last: u64) -> Active {
        let mut stored_kids: Vec<u32> = (0..core.nodes.len() as u32).collect();
        stored_kids.sort_by_key(|&i| core.nodes[i as usize].parent);
        Active { core, steps, children: HashMap::new(), stored_kids, keyed: Default::default(), last }
    }

    /// Stored children of `parent` whose keys are not known yet.
    fn unkeyed(&self, parent: u32) -> &[u32] {
        if self.keyed.contains(&parent) {
            return &[];
        }
        let nodes = &self.core.nodes;
        let a = self.stored_kids.partition_point(|&i| nodes[i as usize].parent < parent);
        let b = self.stored_kids.partition_point(|&i| nodes[i as usize].parent <= parent);
        &self.stored_kids[a..b]
    }
}

#[derive(Debug, Clone, Copy)]
enum OLoc {
    Mem(u32),
    Blob { blob: u32, off: u64, len: u32 },
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
    /// Opaque values with ids from `Manifest::opaques` on: (enc, kind, hash, payload).
    opaques: Vec<(u8, u8, u128, Box<[u8]>)>,
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
    /// Bytes cut from blob files past the length the manifest gives them.
    pub blob_tail_bytes: u64,
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
    /// Sessions folded into one part, and segments rewritten to drop the parts folds replaced.
    pub folded: u64,
    pub reclaims: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub sessions: usize,
    pub nodes: usize,
    pub requests: usize,
    pub pieces: usize,
    pub opaques: usize,
    pub segments: usize,
    pub level0: usize,
    pub runs: usize,
    pub blobs: usize,
    /// Uncompressed bytes of all stored messages (after cutting markers and opaque values).
    pub node_bytes: u64,
    /// Compressed frames holding those messages.
    pub frame_bytes: u64,
    /// zstd dictionaries the merged segments' frames were compressed with.
    pub dict_bytes: u64,
    pub piece_raw_bytes: u64,
    pub piece_bytes: u64,
    pub opaque_bytes: u64,
    /// Segment bytes other than frames: blocks of parts, directories, catalogs.
    pub index_bytes: u64,
    pub seg_file_bytes: u64,
    pub index_file_bytes: u64,
    pub blob_file_bytes: u64,
    pub wal_bytes: u64,
    pub mem_bytes: usize,
    pub steps: usize,
    pub tools: usize,
    pub step_fields: usize,
    pub orphan_results: usize,
    /// Memory held by file catalogs (always loaded), and by sessions taking writes.
    pub catalog_bytes: usize,
    /// File bytes of session parts that folds replaced, not yet dropped by a rewrite.
    pub dead_bytes: u64,
    /// Segment bytes of the step summaries (part of `seg_file_bytes`).
    pub summary_bytes: u64,
    pub active_sessions: usize,
    pub active_bytes: usize,
    pub caches: Vec<(&'static str, CacheStats)>,
    pub merges: MergeStats,
}

impl Stats {
    /// Bytes of every file the store's data is in (segments, index runs, blob files).
    pub fn store_bytes(&self) -> u64 {
        self.seg_file_bytes + self.index_file_bytes + self.blob_file_bytes
    }
}

/// A blob file: an id for the open-file cache, its path, and the length made durable.
struct BlobFile {
    uid: u64,
    path: PathBuf,
    len: u64,
}

/// Where a query that reads summaries finds steps (see [`Db::step_sources`]).
#[derive(Debug, Clone)]
pub enum StepSource {
    /// A segment's summaries; with `live`, only the parts marked live count.
    Seg { num: u32, live: Option<Arc<Vec<bool>>> },
    /// Steps `start..end` of a session, read from its steps.
    Rows { session: u32, start: u32, end: u32 },
}

/// A decoded block of a segment's summaries.
pub enum StatBlock {
    Head(StatsHead),
    Rows(FieldRows),
    Vals(ValChunk),
    Words(Words),
    Post(Vec<u8>),
    /// A block of encoded step parts, read for the values of fields listed by step.
    Raw(Vec<u8>),
}

impl StatBlock {
    fn bytes(&self) -> usize {
        match self {
            StatBlock::Head(h) => h.bytes(),
            StatBlock::Rows(r) => r.bytes(),
            StatBlock::Vals(v) => 64 + v.buf.len() + v.off.len() * 4,
            StatBlock::Words(w) => w.bytes(),
            StatBlock::Post(p) | StatBlock::Raw(p) => 32 + p.len(),
        }
    }
    // A block is cached under its own offset, so it always has the kind it was read as.
    pub fn head(&self) -> &StatsHead {
        match self {
            StatBlock::Head(x) => x,
            _ => unreachable!("summary block of another kind"),
        }
    }
    pub fn rows(&self) -> &FieldRows {
        match self {
            StatBlock::Rows(x) => x,
            _ => unreachable!("summary block of another kind"),
        }
    }
    pub fn vals(&self) -> &ValChunk {
        match self {
            StatBlock::Vals(x) => x,
            _ => unreachable!("summary block of another kind"),
        }
    }
    pub fn words(&self) -> &Words {
        match self {
            StatBlock::Words(x) => x,
            _ => unreachable!("summary block of another kind"),
        }
    }
    pub fn post(&self) -> &[u8] {
        match self {
            StatBlock::Post(x) | StatBlock::Raw(x) => x,
            _ => unreachable!("summary block of another kind"),
        }
    }
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
    /// Index runs in id order (oldest first), so piece and opaque id ranges ascend.
    runs: Vec<Arc<Run>>,
    blob_files: BTreeMap<u32, BlobFile>,
    /// Directory blocks by (segment, block).
    dirs: Cache<(u32, u32), Vec<DirEnt>>,
    /// Segment blocks by (segment, offset).
    blocks: Cache<(u32, u64), Vec<u8>>,
    /// Index blocks by (run, kind, index).
    ix: Cache<(u32, u8, u32), IxBlock>,
    cores: Cache<u32, SessionCore>,
    stepc: Cache<u32, SessionSteps>,
    /// Sessions' steps without their input values, for queries that read none.
    lightc: Cache<u32, SessionSteps>,
    /// Sessions' steps with the values of only the fields queries tested.
    fieldc: Cache<u32, SessionSteps>,
    /// Texts of step values that summaries read from step blocks, by (segment, part,
    /// offset in the part): a search confirms each candidate on its text, and its
    /// candidates are spread over far more step blocks than fit a cache.
    textc: Cache<(u32, u32, u32), Vec<u8>>,
    pieces: Cache<u32, Vec<u8>>,
    /// Per piece kind, the last few pieces stored, with how many pieces reading each one
    /// needs first: new pieces of that kind are compressed against one of them when that pays.
    piece_bases: HashMap<u8, Vec<(u32, u8)>>,
    skels: Cache<u32, Skeleton>,
    /// Decoded summary blocks by (segment, offset).
    statc: Cache<(u32, u64), StatBlock>,
    /// Input values over `MAX_VALUE` that queries read, by (session, call message, offset
    /// in it): their text is only in the message, and in a cold session reading it decodes
    /// a whole run.
    longc: Cache<(u32, u32, u32), Option<Vec<u8>>>,
    /// Per segment holding parts that folds replaced: which of its parts are live, as of a
    /// manifest generation.
    live_masks: Mutex<HashMap<u32, (u64, Arc<Vec<bool>>)>>,
    active: HashMap<u32, Active>,
    /// Sessions that took writes in the last `idle_flushes` flushes, as of the last flush
    /// (merges after it drop them from `active`).
    busy: BTreeSet<u32>,
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

/// Messages a merge needs (bytes) to train a dictionary and write at `merge_level`.
const DICT_MIN_RAW: u64 = 1600 << 10;

/// Recent pieces per kind that a new piece may be compressed against.
const PIECE_BASES: usize = 32;
/// Most pieces that reading one piece may need first.
const PIECE_CHAIN: u8 = 32;

fn piece_hash(kind: u8, b: &[u8]) -> u128 {
    xxh3_128_with_seed(b, kind as u64)
}

fn opaque_hash(enc: u8, b: &[u8]) -> u128 {
    xxh3_128_with_seed(b, 0x6f70_0000 | enc as u64)
}

fn request_hash(id: &str) -> u128 {
    xxh3_128_with_seed(id.as_bytes(), 0x7265_7100)
}

fn session_hash(key: &(String, String)) -> u128 {
    xxh3_128_with_seed(&key_bytes(&key.0, &key.1), 0x7365_7300)
}

/// Whether part `k` covers every request, node and step row of part `p`.
fn covers(k: &DirEnt, p: &DirEnt) -> bool {
    let within = |a: u32, n: u32, b: u32, m: u32| a <= b && b + m <= a + n;
    within(k.first_req, k.n_reqs, p.first_req, p.n_reqs) && within(k.first_node, k.n_nodes, p.first_node, p.n_nodes) && within(k.first_row, k.n_rows, p.first_row, p.n_rows)
}

/// What a part counts for in its segment, for a fold that replaces it.
fn part_dead(d: &DirEnt, core: &SessionCore) -> Dead {
    let share = |r: &(BlockRef, u32, u32)| r.2 as u64 * r.0.clen as u64 / r.0.rawlen.max(1) as u64;
    let nodes = d.first_node as usize..(d.first_node + d.n_nodes) as usize;
    Dead {
        bytes: d.frame_bytes + share(&d.core) + share(&d.steps),
        requests: d.n_reqs as u64,
        nodes: d.n_nodes as u64,
        steps: d.n_rows as u64,
        node_bytes: core.nodes.get(nodes).map_or(0, |ns| ns.iter().map(|n| n.len as u64).sum()),
        frame_bytes: d.frame_bytes,
    }
}

/// Size tier: 0 under 4 << `shift` bytes, then one more for every factor of 4.
fn tier(b: u64, shift: u32) -> u32 {
    let (mut t, mut x) = (0u32, b >> shift);
    while x >= 4 {
        x /= 4;
        t += 1;
    }
    t
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
        let live: BTreeSet<u32> = manifest.segs.iter().chain(&manifest.runs).copied().chain(manifest.blobs.iter().map(|b| b.0)).collect();
        let mut wal_nums = Vec::new();
        for e in fs::read_dir(&dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            let file = parse_num(&name, "seg-", ".tdb").or_else(|| parse_num(&name, "idx-", ".tdb")).or_else(|| parse_num(&name, "blob-", ".dat"));
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
            runs: Vec::new(),
            blob_files: BTreeMap::new(),
            dirs: Cache::sharded(cb / 16, 16),
            blocks: Cache::sharded(cb / 8, 16),
            ix: Cache::sharded(cb / 8, 16),
            cores: Cache::new(cb * 3 / 16),
            stepc: Cache::new(cb / 8),
            lightc: Cache::new(cb / 16),
            fieldc: Cache::new(cb / 16),
            textc: Cache::sharded(cb / 32, 16),
            pieces: Cache::sharded(cb / 16, 16),
            piece_bases: HashMap::new(),
            skels: Cache::sharded(cb / 32, 16),
            statc: Cache::new(cb / 12),
            longc: Cache::sharded(cb / 32, 16),
            live_masks: Mutex::new(HashMap::new()),
            active: HashMap::new(),
            busy: BTreeSet::new(),
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
        for &(n, len) in &db.manifest.blobs {
            rec.blob_tail_bytes += blob::trim(&db.dir, n, len, db.opts.sync)?;
            db.blob_files.insert(n, BlobFile { uid: crate::block::uid(), path: blob_path(&db.dir, n), len });
        }
        for &n in &db.manifest.runs {
            db.runs.push(Arc::new(Run::open(&db.dir, n)?));
        }
        db.check_run_ranges()?;
        if let Some((n, _)) = db.manifest.dead.iter().find(|(n, _)| !db.segs.contains_key(n)) {
            return corrupt(format!("MANIFEST counts replaced parts in segment {n}, which is not live"));
        }
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

    fn check_run_ranges(&self) -> Result<()> {
        let (mut p, mut o) = (0u32, 0u64);
        for r in &self.runs {
            if r.cat.plo != p || r.cat.olo != o {
                return corrupt(format!("index run {} does not continue the id ranges", r.num));
            }
            p += r.cat.np;
            o += r.cat.no;
        }
        if p != self.manifest.pieces || o != self.manifest.opaques {
            return corrupt("index runs do not cover the ids the manifest names");
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

    fn dir_block(&self, seg: &Segment, k: usize) -> Result<Arc<Vec<DirEnt>>> {
        let key = (seg.num, k as u32);
        if let Some(d) = self.dirs.get(&key) {
            return Ok(d);
        }
        let d = seg.dir_block(k)?;
        let size = d.iter().map(|x| 160 + x.key.0.len() + x.key.1.len()).sum();
        Ok(self.dirs.insert(key, Arc::new(d), size))
    }

    fn block(&self, seg: &Segment, r: &BlockRef) -> Result<Arc<Vec<u8>>> {
        if let Some(x) = self.blocks.get(&(seg.num, r.off)) {
            return Ok(x);
        }
        let raw = seg.block(r)?;
        let n = raw.len();
        Ok(self.blocks.insert((seg.num, r.off), Arc::new(raw), n))
    }

    fn step_block(&self, seg: &Segment, r: &BlockRef) -> Result<Arc<Vec<u8>>> {
        if let Some(x) = self.blocks.get(&(seg.num, r.off)) {
            return Ok(x);
        }
        let raw = seg.step_block(r)?;
        let n = raw.len();
        Ok(self.blocks.insert((seg.num, r.off), Arc::new(raw), n))
    }

    fn ixb(&self, run: &Run, kind: u8, i: usize) -> Result<Arc<IxBlock>> {
        let k = (run.num, kind, i as u32);
        if let Some(t) = self.ix.get(&k) {
            return Ok(t);
        }
        let t = run.block(kind, i)?;
        let n = t.bytes();
        Ok(self.ix.insert(k, Arc::new(t), n))
    }

    /// Every live part of session `s`, in order.
    ///
    /// A part covers a range of the session's requests, nodes and step rows. Merges and folds
    /// only ever write a part that joins live parts that follow each other, so two parts
    /// either do not overlap or one covers the other; a covered part was replaced by a fold
    /// and still sits in an older segment until that segment is rewritten. Sorting by start,
    /// longest first, puts each covering part before the parts it replaced.
    fn parts(&self, s: u32) -> Result<Vec<(Arc<Segment>, DirEnt)>> {
        let mut out = Vec::new();
        for seg in self.segs.values() {
            if !seg.cat.may_hold(s) {
                continue;
            }
            for k in seg.cat.dir_blocks_for(s) {
                let dir = self.dir_block(seg, k)?;
                let a = dir.partition_point(|d| d.session < s);
                for d in dir[a..].iter().take_while(|d| d.session == s) {
                    out.push((seg.clone(), d.clone()));
                }
            }
        }
        let start = |d: &DirEnt| (d.first_req, d.first_node, d.first_row);
        let end = |d: &DirEnt| (d.first_req + d.n_reqs, d.first_node + d.n_nodes, d.first_row + d.n_rows);
        out.sort_by(|a, b| start(&a.1).cmp(&start(&b.1)).then(end(&b.1).cmp(&end(&a.1))).then(b.0.num.cmp(&a.0.num)));
        let mut live: Vec<(Arc<Segment>, DirEnt)> = Vec::with_capacity(out.len());
        for p in out {
            if live.last().is_some_and(|k| covers(&k.1, &p.1)) {
                continue;
            }
            live.push(p);
        }
        Ok(live)
    }

    /// Replaced parts counted over every live segment.
    fn dead_total(&self) -> Dead {
        let mut t = Dead::default();
        for (_, d) in &self.manifest.dead {
            t.add(d);
        }
        t
    }

    /// The newest value stored under (`ns`, `h`) in the index runs.
    fn index_get(&self, ns: u8, h: u128) -> Result<Option<u64>> {
        for run in self.runs.iter().rev() {
            if let Some(v) = index::lookup(run, (ns, h), |kind, i| self.ixb(run, kind, i))? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    fn find_session_in_files(&self, key: &(String, String)) -> Result<Option<u32>> {
        let Some(s) = self.index_get(NS_SESSION, session_hash(key))? else { return Ok(None) };
        let s = u32::try_from(s).map_err(|_| bad("index gives a bad session id"))?;
        if s >= self.next_session || self.load_core(s)?.key != *key {
            return corrupt("index points a session key at another session");
        }
        Ok(Some(s))
    }

    fn find_request_in_files(&self, id: &str) -> Result<Option<ReqId>> {
        let Some(r) = self.index_get(NS_REQUEST, request_hash(id))? else { return Ok(None) };
        let (core, k) = self.core_req(r)?;
        if *core.requests[k as usize].id != *id {
            return corrupt("index points a request id at another request");
        }
        Ok(Some(r))
    }

    /// An existing piece or opaque value with this hash (tag 0 = piece, 1 = opaque).
    fn find_by_hash(&self, tag: u8, hash: u128) -> Result<Option<u64>> {
        let mem = if tag == 0 { self.mem.piece_ix.get(&hash).map(|&x| x as u64) } else { self.mem.opaque_ix.get(&hash).copied() };
        if mem.is_some() {
            return Ok(mem);
        }
        self.index_get(if tag == 0 { NS_PIECE } else { NS_OPAQUE }, hash)
    }

    fn run_for_piece(&self, id: u32) -> Result<&Arc<Run>> {
        let i = self.runs.partition_point(|r| r.cat.plo + r.cat.np <= id);
        self.runs.get(i).filter(|r| r.cat.has_piece(id)).ok_or_else(|| bad(format!("missing piece {id}")))
    }

    fn run_for_opaque(&self, id: u64) -> Result<&Arc<Run>> {
        let i = self.runs.partition_point(|r| r.cat.olo + r.cat.no <= id);
        self.runs.get(i).filter(|r| r.cat.has_opaque(id)).ok_or_else(|| bad(format!("missing opaque value {id}")))
    }

    fn blob_read(&self, num: u32, off: u64, len: u32) -> Result<Vec<u8>> {
        let b = self.blob_files.get(&num).ok_or_else(|| bad(format!("blob file {num} is not live")))?;
        if off + len as u64 > b.len {
            return corrupt(format!("a value runs past the end of blob file {num}"));
        }
        blob::read(b.uid, &b.path, off, len)
    }

    fn opaque_meta(&self, id: u64) -> Result<OpaqueMeta> {
        self.opaque_meta_in(id, &mut HashMap::new())
    }

    /// [`Db::opaque_meta`], keeping the table blocks it needs in `tables` for the next call.
    fn opaque_meta_in(&self, id: u64, tables: &mut HashMap<(u32, u32), Arc<IxBlock>>) -> Result<OpaqueMeta> {
        if id >= self.manifest.opaques {
            let (enc, _, hash, _) = self.mem.opaques.get((id - self.manifest.opaques) as usize).ok_or_else(|| bad(format!("missing opaque value {id}")))?;
            return Ok(OpaqueMeta { enc: *enc, hash: *hash, loc: OLoc::Mem((id - self.manifest.opaques) as u32) });
        }
        let r = self.run_for_opaque(id)?;
        let i = (id - r.cat.olo) as usize;
        let key = (r.num, (i / IDS) as u32);
        let t = match tables.get(&key) {
            Some(t) => t.clone(),
            None => {
                let t = self.ixb(r, K_OPAQUES, i / IDS)?;
                tables.insert(key, t.clone());
                t
            }
        };
        let IxBlock::Opaques(v) = &*t else { return corrupt("index block of the wrong kind") };
        let o = *v.get(i % IDS).ok_or_else(|| bad(format!("missing opaque value {id}")))?;
        Ok(OpaqueMeta { enc: o.enc, hash: o.hash, loc: OLoc::Blob { blob: o.file, off: o.off, len: o.len } })
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
            let raw = self.block(seg, &d.core.0)?;
            let (a, b) = (d.core.1 as usize, (d.core.1 + d.core.2) as usize);
            let p = decode_core(raw.get(a..b).ok_or_else(|| bad("session part past its block"))?)?;
            if p.first_node as usize != core.nodes.len() || p.first_req as usize != core.requests.len() {
                return corrupt(format!("session {s}: parts do not follow each other"));
            }
            let chunk = core.chunks.len() as u32;
            core.chunks.push(Chunk { seg: seg.num, frames: p.frames.into(), raw_len: p.raw_len, fs: p.frame });
            let end = core.nodes.len() + p.nodes.len();
            core.nodes.resize(end, NodeMeta { parent: NONE, len: 0, loc: UNSET, holes: None });
            for n in p.nodes {
                let slot = core.nodes.get_mut(n.id as usize).ok_or_else(|| bad(format!("session {s}: node {} outside its part", n.id)))?;
                *slot = NodeMeta { parent: n.parent, len: n.len, loc: Loc::Seg { chunk, off: n.off }, holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) } };
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
        let ss = self.read_steps(s)?;
        let size = ss.bytes();
        Ok(self.stepc.insert(s, Arc::new(ss), size))
    }

    /// A session's steps without their input values (or with them, when they are at hand),
    /// cached on their own: queries whose tests read no values use this.
    fn load_light_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.steps.clone());
        }
        if let Some(c) = self.stepc.get(&s) {
            return Ok(c);
        }
        if let Some(c) = self.lightc.get(&s) {
            return Ok(c);
        }
        let ss = self.read_steps(s)?.without_values();
        let size = ss.bytes();
        Ok(self.lightc.insert(s, Arc::new(ss), size))
    }

    /// Decode a session's steps from its parts (with its turns and steps by shape code).
    fn read_steps(&self, s: u32) -> Result<SessionSteps> {
        let parts = self.parts(s)?;
        let mut ss = SessionSteps::default();
        let mut fs: Vec<(u32, FieldVal)> = Vec::new();
        for (seg, d) in &parts {
            let raw = self.step_block(seg, &d.steps.0)?;
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
        ss.turns();
        ss.by_shape();
        Ok(ss)
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
        self.lightc.remove(&s);
        self.fieldc.remove(&s);
        let mut ss = Arc::unwrap_or_clone(steps);
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
        self.active.insert(s, Active::new(core, Arc::new(ss), self.flushes));
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

    fn opaque_id(&self, oh: OpaqueHole, m: &mut Mutation, pending: &mut HashMap<u128, u64>) -> Result<u64> {
        let h = opaque_hash(oh.enc as u8, &oh.payload);
        let found = match pending.get(&h) {
            Some(&i) => Some(i),
            None => self.find_by_hash(1, h)?,
        };
        if let Some(i) = found {
            return Ok(i);
        }
        let id = self.next_opaque() + m.opaques.len() as u64;
        m.opaques.push(NewOpaque { id, enc: oh.enc as u8, kind: oh.kind as u8, hash: h, payload: oh.payload });
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
                layout: sp.layout.as_ref().map(|g| g.iter().map(|x| x.clone().into_boxed_slice()).collect()),
            },
        };
        let mut pp = HashMap::new();
        let mut po = HashMap::new();

        // walk the session tree: by the hash of each message as it came, else by its key (the
        // hash of the message as stored, values not kept blanked); keys learned on the way
        // are added to the session after the record is applied
        let act = existing.map(|i| &self.active[&i]);
        let mut next = act.map(|a| a.core.nodes.len() as u32).unwrap_or(0);
        let mut parent = NONE;
        let mut creating = act.is_none();
        let mut learned: HashMap<(u32, u128), u32> = HashMap::new();
        let mut keyed = Vec::new();
        // values not kept are blanked; cache markers after them move back by what was removed
        let mut blanked: Vec<Vec<crate::json::Span>> = vec![Vec::new(); sp.messages.len()];
        for (i, msg) in sp.messages.iter().enumerate() {
            let h = xxh3_128(&msg.bytes);
            if !creating {
                let a = act.unwrap();
                let find = |k: &(u32, u128), learned: &HashMap<(u32, u128), u32>| a.children.get(k).or_else(|| learned.get(k)).copied();
                if let Some(c) = find(&(parent, h), &learned) {
                    parent = c;
                    if !msg.notes.is_empty() {
                        blanked[i] = split::blanked_spans(&msg.bytes, &self.opts.keep)?;
                    }
                    continue;
                }
                let spans = split::blanked_spans(&msg.bytes, &self.opts.keep)?;
                let k = if spans.is_empty() { h } else { xxh3_128(&split::remove_spans(&msg.bytes, &spans)) };
                let mut found = if k == h { None } else { find(&(parent, k), &learned) };
                let kids = a.unkeyed(parent);
                if found.is_none() && !kids.is_empty() && !keyed.contains(&parent) {
                    let (mut buf, mut at) = (Vec::new(), Vec::with_capacity(kids.len()));
                    self.read_nodes(&a.core, kids, &mut buf, &mut at, &mut ReadStats::default())?;
                    for (&kid, &(x, y)) in kids.iter().zip(&at) {
                        learned.insert((parent, xxh3_128(&buf[x..y])), kid);
                    }
                    keyed.push(parent);
                    found = learned.get(&(parent, k)).copied();
                }
                if let Some(c) = found {
                    learned.insert((parent, h), c);
                    parent = c;
                    blanked[i] = spans;
                    continue;
                }
                creating = true;
            }
            let cut = split::cut_opaque(&msg.bytes, &self.opts.keep)?;
            let k = if cut.blanked.is_empty() { h } else { xxh3_128(&split::remove_spans(&msg.bytes, &cut.blanked)) };
            if k != h {
                learned.insert((parent, h), next);
            }
            let mut hl = Vec::with_capacity(cut.holes.len());
            for oh in cut.holes {
                let off = oh.offset;
                let oid = self.opaque_id(oh, &mut m, &mut po)?;
                hl.push((off, oid));
            }
            blanked[i] = cut.blanked;
            m.nodes.push(NewNode { id: next, parent, hash: k, bytes: cut.bytes, holes: hl });
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
                notes.push(ReqNote { target: TARGET_MSG0 + i as u32, offset: split::offset_after_blanking(n.offset, &blanked[i]), text: t });
            }
        }
        m.req.notes = notes.into_boxed_slice();
        m.req.skeleton = self.piece_id(PIECE_SKELETON, &sp.skeleton.encode(), &mut m, &mut pp)?;

        let k = act.map(|a| a.core.requests.len() as u32).unwrap_or(0);
        self.wal.append(&m.encode());
        self.apply(m)?;
        let a = self.active.get_mut(&sidx).unwrap();
        a.children.extend(learned);
        a.keyed.extend(keyed);
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
            self.active.insert(i, Active::new(Arc::new(core), Arc::new(SessionSteps::default()), self.flushes));
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
            self.mem.bytes += o.payload.len();
            self.mem.opaques.push((o.enc, o.kind, o.hash, o.payload.into_boxed_slice()));
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
            OLoc::Blob { len, .. } => len as u64,
            OLoc::Mem(mi) => self.mem.opaques[mi as usize].3.len() as u64,
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
    fn step_part(&self, w: &mut SegWriter, s: u32, ss: &SessionSteps, rows: std::ops::Range<u32>) -> Result<StepPart> {
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
                    // the summaries index long values' words, so their text is read here
                    let name = String::from_utf8_lossy(self.sd.fields.get(f)).into_owned();
                    p.long_text.push(self.read_long_value(s, ss, pos, v, &name)?.unwrap_or_default());
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
        Ok(p)
    }

    /// Write nodes `nodes` and requests `reqs` of a session as one part, main line first.
    /// A session part's metadata and its message stream (frames not written yet).
    fn core_part(&self, core: &SessionCore, nodes: std::ops::Range<u32>, reqs: std::ops::Range<u32>) -> Result<(CorePart, Vec<u8>)> {
        let (first, n) = (nodes.start, nodes.end - nodes.start);
        let first_req = reqs.start;
        let tips = core.requests[reqs.start as usize..reqs.end as usize].iter().map(|r| r.tip);
        let order = layout::heavy_first(first, n, |id| core.nodes[id as usize].parent, tips);
        let mut stream = Vec::new();
        let mut nodes = Vec::with_capacity(order.len());
        let mut cache = HashMap::new();
        for id in order {
            let nm = &core.nodes[id as usize];
            nodes.push(SegNode { id, parent: nm.parent, off: stream.len() as u32, len: nm.len, holes: nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default() });
            self.stored_bytes(core, nm, &mut cache, &mut stream)?;
        }
        if stream.len() > u32::MAX as usize {
            return Err(Error::BadRequest("a session part is over 4 GiB".into()));
        }
        Ok((CorePart { frames: Vec::new(), raw_len: stream.len() as u64, frame: self.opts.frame_size as u32, nodes, first_node: first, first_req, requests: core.requests[reqs.start as usize..reqs.end as usize].to_vec() }, stream))
    }

    /// Write everything in memory to files (a new segment and index run; values appended
    /// to a blob file), and trim the log.
    pub fn flush(&mut self) -> Result<()> {
        self.wal.commit()?;
        if self.mem.touched.is_empty() && self.mem.pieces.is_empty() && self.mem.opaques.is_empty() {
            return Ok(());
        }
        let new_wal = self.wal.num + 1;
        self.wal = WalWriter::create(&self.dir, new_wal, self.opts.sync)?;
        let mut man = self.manifest.clone();
        let mut flushed = 0u64;

        // values: appended to the last blob file, or a new one once that is full
        let lo = self.manifest.opaques;
        let n = self.mem.opaques.len();
        let mut pents = Vec::with_capacity(self.mem.pieces.len());
        let mut oents: Vec<OpaqueEnt> = self.mem.opaques.iter().map(|(enc, kind, h, p)| OpaqueEnt { enc: *enc, kind: *kind, hash: *h, file: 0, off: 0, len: p.len() as u32 }).collect();
        let mut blob_len = None;
        if !self.mem.pieces.is_empty() || !self.mem.opaques.is_empty() {
            let (bn, len) = match man.blobs.last() {
                Some(&(bn, len)) if len < self.opts.blob_bytes => (bn, len),
                _ => {
                    let bn = man.next_file;
                    man.next_file += 1;
                    man.blobs.push((bn, 0));
                    (bn, 0)
                }
            };
            let mut bw = BlobAppend::open(&self.dir, bn, len)?;
            let start = bw.pos();
            // system prompts and tool lists change a little at a time: each new piece is
            // compressed against a recent whole piece of its kind when that at least halves it
            let first = self.manifest.pieces;
            let mut bases = std::mem::take(&mut self.piece_bases);
            for (i, (kind, h, raw)) in self.mem.pieces.iter().enumerate() {
                let ids = bases.entry(*kind).or_default();
                let mut cand: Vec<(u32, Vec<u8>)> = Vec::with_capacity(ids.len());
                for &(id, depth) in ids.iter() {
                    if depth < PIECE_CHAIN {
                        cand.push((id, if id >= first { self.mem.pieces[(id - first) as usize].2.to_vec() } else { self.piece(id)?.to_vec() }));
                    }
                }
                let refs: Vec<(u32, &[u8])> = cand.iter().map(|(id, b)| (*id, &b[..])).collect();
                let (base, c) = blob::pack_piece(raw, &refs, self.opts.merge_level)?;
                let (off, clen) = bw.packed(&c)?;
                pents.push(PieceEnt { kind: *kind, hash: *h, file: bn, off, clen, rawlen: raw.len() as u32, base });
                let depth = if base == 0 { 0 } else { ids.iter().find(|x| x.0 == base - 1).map_or(PIECE_CHAIN, |x| x.1 + 1) };
                ids.push((first + i as u32, depth));
                if ids.len() > PIECE_BASES {
                    ids.remove(0);
                }
            }
            self.piece_bases = bases;
            // values in the order requests read them: by session, then by message
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
            for i in order {
                let off = bw.opaque(&self.mem.opaques[i].3)?;
                oents[i] = OpaqueEnt { file: bn, off, ..oents[i] };
            }
            let new_len = bw.finish(self.opts.sync)?;
            flushed += new_len - start;
            man.blobs.last_mut().unwrap().1 = new_len;
            blob_len = Some((bn, new_len));
        }

        // index: what this flush added, findable by hash; values by id
        let rn = man.next_file;
        man.next_file += 1;
        let mut keys: Vec<(u8, u128, u64)> = Vec::with_capacity(pents.len() + n + self.mem.req_ix.len());
        keys.extend(pents.iter().enumerate().map(|(i, p)| (NS_PIECE, p.hash, self.manifest.pieces as u64 + i as u64)));
        keys.extend(oents.iter().enumerate().map(|(i, o)| (NS_OPAQUE, o.hash, lo + i as u64)));
        keys.extend(self.mem.req_ix.iter().map(|(id, &r)| (NS_REQUEST, request_hash(id), r)));
        for s in self.manifest.sessions..self.next_session {
            let a = self.active.get(&s).ok_or_else(|| bad(format!("new session {s} is not in memory at a flush")))?;
            keys.push((NS_SESSION, session_hash(&a.core.key), s as u64));
        }
        keys.sort_unstable();
        keys.dedup();
        let mut rw = RunWriter::create(&self.dir, rn, self.manifest.pieces, lo)?;
        for (ns, h, v) in keys {
            rw.key(ns, h, v)?;
        }
        for e in &pents {
            rw.piece(e)?;
        }
        for e in &oents {
            rw.opaque(e)?;
        }
        flushed += rw.finish(self.opts.sync)?;
        man.runs.push(rn);

        // messages, requests and steps of every session that took writes
        let num = man.next_file;
        man.next_file += 1;
        let mut w = SegWriter::create(&self.dir, num, self.opts.frame_size, Codec::plain(self.opts.level), 0, self.seq)?;
        let mut written: Vec<(u32, CorePart)> = Vec::new();
        for &s in &self.mem.touched {
            let a = &self.active[&s];
            let (core, ss) = (&a.core, &a.steps);
            let (mut cp, stream) = self.core_part(core, core.flushed_nodes..core.nodes.len() as u32, core.flushed_reqs..core.requests.len() as u32)?;
            cp.frames = w.frames(&stream)?;
            let sp = self.step_part(&mut w, s, ss, ss.flushed..ss.rows.len() as u32)?;
            w.add(s, core.key.clone(), &cp, &sp)?;
            written.push((s, cp));
        }
        w.finish(self.opts.sync)?;
        man.segs.push(num);
        man.generation += 1;
        man.checkpoint = self.seq;
        man.wal_start = new_wal;
        man.sessions = self.next_session;
        man.pieces = self.next_piece();
        man.opaques = self.next_opaque();
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;

        let seg = Segment::open(&self.dir, num)?;
        flushed += seg.file_bytes;
        self.merge_stats.flushed += flushed;
        self.add_segment(seg);
        self.runs.push(Arc::new(Run::open(&self.dir, rn)?));
        if let Some((bn, len)) = blob_len {
            self.blob_files.entry(bn).or_insert_with(|| BlobFile { uid: crate::block::uid(), path: blob_path(&self.dir, bn), len: 0 }).len = len;
        }
        for (s, cp) in written {
            let a = self.active.get_mut(&s).unwrap();
            let core = Arc::make_mut(&mut a.core);
            let chunk = core.chunks.len() as u32;
            core.chunks.push(Chunk { seg: num, frames: cp.frames.into(), raw_len: cp.raw_len, fs: cp.frame });
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
        self.busy = self.active.keys().copied().collect();
        self.merge_policy()
    }

    /// Once `merge_at` flushed segments wait, merge them; then merge any `merge_at` merged
    /// segments of one size tier (sizes in powers of 4 from 1 MB) under the size cap. Each
    /// byte is rewritten about once per tier, and finished data stops moving at the cap.
    /// A segment whose parts were mostly replaced by folds is rewritten on its own.
    /// Index runs merge the same way, `merge_at` neighbours of one tier at a time.
    fn merge_policy(&mut self) -> Result<()> {
        // index runs first: they are small, and every lookup reads each one
        let w = self.opts.merge_at.max(2);
        loop {
            let open = |r: &Run| r.file_bytes < self.opts.run_bytes;
            let pick = (0..(self.runs.len() + 1).saturating_sub(w)).find(|&i| {
                // runs are small, so their tiers start at 256 KB
                let t = tier(self.runs[i].file_bytes, 16);
                self.runs[i..i + w].iter().all(|r| open(r) && tier(r.file_bytes, 16) == t)
            });
            let Some(i) = pick else { break };
            self.merge_runs(i, w)?;
        }
        let l0: BTreeSet<u32> = self.segs.values().filter(|s| s.cat.level == 0).map(|s| s.num).collect();
        if l0.len() >= self.opts.merge_at {
            self.merge(l0, true, false)?;
        }
        loop {
            let mut tiers: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for sg in self.segs.values().filter(|s| s.cat.level > 0 && s.file_bytes < self.opts.seg_bytes) {
                tiers.entry(tier(sg.file_bytes, 20)).or_default().push(sg.num);
            }
            if let Some((_, group)) = tiers.into_iter().find(|(_, v)| v.len() >= self.opts.merge_at) {
                self.merge(group.into_iter().take(self.opts.merge_at).collect(), true, false)?;
                continue;
            }
            // half or more of the segment is parts that folds replaced: rewrite it without them
            let gc = self.manifest.dead.iter().find(|(n, d)| self.segs.get(n).is_some_and(|sg| sg.cat.level > 0 && d.bytes.saturating_mul(2) >= sg.file_bytes)).map(|x| x.0);
            let Some(n) = gc else { break };
            self.merge(BTreeSet::from([n]), false, false)?;
            self.merge_stats.reclaims += 1;
        }
        Ok(())
    }

    /// Merge index runs `i..i + n` into one.
    fn merge_runs(&mut self, i: usize, n: usize) -> Result<()> {
        let inputs: Vec<Arc<Run>> = self.runs[i..i + n].to_vec();
        let nums: Vec<u32> = inputs.iter().map(|r| r.num).collect();
        let mut man = self.manifest.clone();
        let pos = man.runs.iter().position(|&r| r == nums[0]).ok_or_else(|| bad("index run missing from the manifest"))?;
        if man.runs.get(pos..pos + n) != Some(&nums[..]) {
            return corrupt("index runs out of step with the manifest");
        }
        let num = man.next_file;
        man.next_file += 1;
        let bytes_out = index::merge(&self.dir, num, &inputs, self.opts.sync)?;
        man.runs.splice(pos..pos + n, [num]);
        man.generation += 1;
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        self.runs.splice(i..i + n, [Arc::new(Run::open(&self.dir, num)?)]);
        for r in &inputs {
            crate::block::forget(r.path());
            let _ = fs::remove_file(r.path());
        }
        self.merge_stats.merges += 1;
        self.merge_stats.bytes_in += inputs.iter().map(|r| r.file_bytes).sum::<u64>();
        self.merge_stats.bytes_out += bytes_out;
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
    /// run of its consecutive live parts that all sit in `inputs` becomes one part, main line
    /// first; parts replaced by a fold are dropped.
    ///
    /// Parts elsewhere (a session that took writes again after its data was merged) stay put,
    /// except in a merge whose output reaches the size cap (the last time this data moves),
    /// when `fold` is set: a session there is folded into one part, all of it read from every
    /// segment, when it has gone quiet (its newest part is in `inputs` and it takes no writes)
    /// or would otherwise be left in more than `max_parts` parts. The parts outside `inputs`
    /// that it replaces are counted as dead in their segments, which are rewritten once
    /// half dead. Folding only there means a folded session is written once at full size
    /// rather than climbing the size tiers again, and a session still taking writes waits.
    ///
    /// The same merges, and every merge when `last` is set (`compact`) or when an input was
    /// already written that way, write at `merge_level` with a dictionary trained on the
    /// input: about 4 times the CPU of level 3, paid once per byte. Smaller merges, and any
    /// merge of under 1.6 MB of messages (too little for a dictionary), write as flushes do.
    fn merge(&mut self, inputs: BTreeSet<u32>, fold: bool, last: bool) -> Result<()> {
        if inputs.is_empty() {
            return Ok(());
        }
        let mut sessions = BTreeSet::new();
        let mut bytes_in = 0;
        for &n in &inputs {
            let seg = self.segs.get(&n).ok_or_else(|| bad(format!("merge of a missing segment {n}")))?.clone();
            bytes_in += seg.file_bytes;
            for k in 0..seg.cat.dirs.len() {
                for d in self.dir_block(&seg, k)?.iter() {
                    sessions.insert(d.session);
                }
            }
        }
        let full = bytes_in >= self.opts.seg_bytes;
        let fold = fold && full;
        // where data moves for the last time: cold sessions go in runs there
        let settled = last || full;
        let raw_in: u64 = inputs.iter().filter_map(|n| self.segs.get(n)).map(|s| s.cat.node_bytes).sum();
        let last = (last || full || inputs.iter().any(|n| self.segs.get(n).is_some_and(|s| s.cat.dict.is_some()))) && raw_in >= DICT_MIN_RAW;
        let codec = if last {
            Codec { level: self.opts.merge_level, block_level: self.opts.merge_level, dict: self.train_dict(&sessions, &inputs)?, threads: self.opts.threads }
        } else {
            Codec::plain(self.opts.level)
        };
        // cold sessions, in a merge into full-size segments: runs instead of frames
        let newest = self.segs.values().map(|s| s.cat.time_hi).max().unwrap_or(0);
        let cold_before = if settled && self.opts.cold_run > 0 && newest > 0 { newest - self.opts.cold_after_ms } else { i64::MIN };
        let mut man = self.manifest.clone();
        let mut outputs: Vec<u32> = Vec::new();
        let mut w: Option<SegWriter> = None;
        let mut dead: BTreeMap<u32, Dead> = BTreeMap::new();
        let (mut folded, mut fold_read) = (0u64, 0u64);
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
            let inside: usize = runs.iter().map(|(a, b)| b - a).sum();
            let outside = parts.len() - inside;
            let quiet = inputs.contains(&parts[parts.len() - 1].0.num) && !self.busy.contains(&s);
            let fold_s = fold && self.opts.max_parts > 0 && outside > 0 && (quiet || outside + runs.len() > self.opts.max_parts);
            if w.as_ref().is_some_and(|w| w.sessions() > 0 && w.written() >= self.opts.seg_bytes) {
                w.take().unwrap().finish(self.opts.sync)?;
            }
            if w.is_none() {
                let num = man.next_file;
                man.next_file += 1;
                outputs.push(num);
                w = Some(SegWriter::create(&self.dir, num, self.opts.frame_size, codec.clone(), 1, self.manifest.checkpoint)?);
            }
            let core = self.load_core(s)?;
            let ss = self.load_steps(s)?;
            if fold_s {
                for (seg, d) in parts.iter().filter(|p| !inputs.contains(&p.0.num)) {
                    let x = part_dead(d, &core);
                    fold_read += x.bytes;
                    dead.entry(seg.num).or_default().add(&x);
                }
                runs = vec![(0, parts.len())];
                folded += 1;
            }
            let last_ms = core.requests.last().map_or(0, |r| steps::parse_time_ms(&r.time));
            let cold = last_ms > 0 && last_ms < cold_before && !self.busy.contains(&s);
            let wr = w.as_mut().unwrap();
            for (a, b) in runs {
                let (pa, pb) = (&parts[a].1, &parts[b - 1].1);
                let (mut cp, stream) = self.core_part(&core, pa.first_node..pb.first_node + pb.n_nodes, pa.first_req..pb.first_req + pb.n_reqs)?;
                if cold {
                    cp.frame = self.opts.cold_run.max(self.opts.frame_size) as u32;
                }
                let sp = self.step_part(wr, s, &ss, pa.first_row..pb.first_row + pb.n_rows)?;
                wr.add_stream(s, core.key.clone(), cp, stream, sp)?;
            }
        }
        if let Some(w) = w {
            w.finish(self.opts.sync)?;
        }
        man.generation += 1;
        man.segs.retain(|n| !inputs.contains(n));
        man.segs.extend(&outputs);
        let mut all_dead: BTreeMap<u32, Dead> = man.dead.iter().filter(|(n, _)| !inputs.contains(n)).copied().collect();
        for (n, x) in dead {
            all_dead.entry(n).or_default().add(&x);
        }
        man.dead = all_dead.into_iter().collect();
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        // sessions that point at the old files are read again from the new ones
        self.active.retain(|_, a| a.core.flushed_nodes as usize != a.core.nodes.len() || a.core.flushed_reqs as usize != a.core.requests.len());
        if !self.active.is_empty() {
            return corrupt("merge with unflushed sessions");
        }
        self.key_ix.clear();
        for c in [&self.cores as &dyn Clear, &self.stepc, &self.lightc, &self.fieldc, &self.dirs, &self.blocks, &self.statc, &self.longc, &self.textc] {
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
        self.merge_stats.bytes_in += bytes_in + fold_read;
        self.merge_stats.bytes_out += bytes_out;
        self.merge_stats.folded += folded;
        Ok(())
    }

    /// The dictionary for a merge into a full-size segment: one trained on the input (zstd's
    /// trainer, about 1% of the messages, at most `dict_bytes`), then the input's own
    /// messages sampled evenly (about 1/12 of them, at most `dict_content`). The trainer
    /// keeps short common strings; whole messages give the long matches it cannot (on the
    /// full logs, 8 MB of them make frames and dictionary 6% smaller). Empty when the input
    /// is too small for one to pay (under about 1.6 MB of messages) or training fails.
    /// Frames are looked at in 64 KB pieces, so cold sessions' runs count like frames.
    fn train_dict(&self, sessions: &BTreeSet<u32>, inputs: &BTreeSet<u32>) -> Result<Vec<u8>> {
        const PIECE: usize = 64 << 10;
        let raw: u64 = inputs.iter().filter_map(|n| self.segs.get(n)).map(|s| s.cat.node_bytes).sum();
        let size = (raw / 100).min(self.opts.dict_bytes as u64) as usize;
        if raw < DICT_MIN_RAW || size == 0 {
            return Ok(Vec::new());
        }
        let content = (raw / 12).min(self.opts.dict_content as u64);
        let stride = (raw / (size as u64 * 100)).max(1);
        let (mut data, mut sizes, mut extra) = (Vec::new(), Vec::new(), Vec::new());
        let (mut k, mut at, mut next) = (0u64, 0u64, 0u64);
        for &s in sessions {
            let core = self.load_core(s)?;
            for ch in core.chunks.iter().filter(|c| inputs.contains(&c.seg)) {
                let sg = self.segs.get(&ch.seg).ok_or_else(|| bad("segment is gone"))?;
                let fs = ch.fs as usize;
                for f in 0..ch.frames.len() {
                    let len = (ch.raw_len as usize - f * fs).min(fs);
                    let mut fr: Option<Arc<Vec<u8>>> = None;
                    for p in 0..len.div_ceil(PIECE) {
                        let sample = k.is_multiple_of(stride);
                        // whole pieces of messages every raw / content bytes
                        let take = content > 0 && at >= next && (extra.len() as u64) < content;
                        if sample || take {
                            if fr.is_none() {
                                fr = Some(sg.frames(&ch.frames, ch.raw_len, fs, f, f)?.remove(0));
                            }
                            let fr = fr.as_ref().unwrap();
                            let piece = &fr[p * PIECE..((p + 1) * PIECE).min(len)];
                            if sample {
                                data.extend_from_slice(piece);
                                sizes.push(piece.len());
                            }
                            if take {
                                let n = piece.len().min((content as usize).saturating_sub(extra.len()));
                                extra.extend_from_slice(&piece[..n]);
                                next = at + (raw / content).max(1) * PIECE as u64;
                            }
                        }
                        at += PIECE as u64;
                        k += 1;
                    }
                }
            }
        }
        let mut dict = zstd::dict::from_continuous(&data, &sizes, size).unwrap_or_default();
        if !dict.is_empty() {
            // content nearest the end of a dictionary is the cheapest for a frame to point at
            dict.extend_from_slice(&extra);
        }
        Ok(dict)
    }

    /// Flush, then merge every segment into level 1: each session stored whole, main line first.
    pub fn compact(&mut self) -> Result<()> {
        self.flush()?;
        let all: BTreeSet<u32> = self.segs.keys().copied().collect();
        self.merge(all, false, true)
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
                let fs = ch.fs as usize;
                let (a, b) = (off as usize, off as usize + nm.len as usize);
                let (f0, f1) = (a / fs, (b - 1) / fs);
                let missing: Vec<usize> = (f0..=f1).filter(|k| !cache.contains_key(&(chunk, *k))).collect();
                if let (Some(&lo), Some(&hi)) = (missing.first(), missing.last()) {
                    for (k, d) in (lo..=hi).zip(sg.frames(&ch.frames, ch.raw_len, fs, lo, hi)?) {
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

    fn opaque_payload(&self, om: &OpaqueMeta, st: &mut ReadStats) -> Result<Vec<u8>> {
        Ok(match om.loc {
            OLoc::Mem(mi) => self.mem.opaques[mi as usize].3.to_vec(),
            OLoc::Blob { blob, off, len } => {
                st.opaque_reads += 1;
                self.blob_read(blob, off, len)?
            }
        })
    }

    /// Read nodes of one session, in the order given, with opaque values put back.
    /// Each node's bytes go to `out`; `spans` gets their ranges.
    fn read_nodes(&self, core: &SessionCore, ids: &[u32], out: &mut Vec<u8>, spans: &mut Vec<(usize, usize)>, st: &mut ReadStats) -> Result<()> {
        // which frames are needed, read with one call per run of adjacent frames
        let mut need: BTreeSet<(u32, usize)> = BTreeSet::new();
        let mut prev: Option<(u32, u32)> = None;
        for &id in ids {
            let nm = &core.nodes[id as usize];
            match nm.loc {
                Loc::Seg { chunk, off } => {
                    let fs = core.chunks[chunk as usize].fs as usize;
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
            let frames = sg.frames(&ch.frames, ch.raw_len, ch.fs as usize, k0, v[j].1)?;
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
                let buf = self.blob_read(blob, start, (end - start) as u32)?;
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
            let nm = &core.nodes[id as usize];
            let start = out.len();
            match &nm.holes {
                None => self.stored_bytes(core, nm, &mut cache, out)?,
                Some(holes) => {
                    tmp.clear();
                    self.stored_bytes(core, nm, &mut cache, &mut tmp)?;
                    let mut pos = 0usize;
                    for &(o, oid) in holes.iter() {
                        out.extend_from_slice(&tmp[pos..o as usize]);
                        pos = o as usize;
                        let om = metas[&oid];
                        let enc = if om.enc == 0 { OpaqueEnc::Base64 } else { OpaqueEnc::Text };
                        let p = match fetched.remove(&oid) {
                            Some(p) => p,
                            None => self.opaque_payload(&om, st)?,
                        };
                        if opaque_hash(om.enc, &p) != om.hash {
                            return corrupt(format!("opaque value {oid} fails its checksum"));
                        }
                        split::opaque_text(enc, &p, out);
                        fetched.insert(oid, p);
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
        let r = self.run_for_piece(id)?;
        let i = (id - r.cat.plo) as usize;
        let t = self.ixb(r, K_PIECES, i / IDS)?;
        let IxBlock::Pieces(v) = &*t else { return corrupt("index block of the wrong kind") };
        let e = *v.get(i % IDS).ok_or_else(|| bad(format!("missing piece {id}")))?;
        let c = self.blob_read(e.file, e.off, e.clen)?;
        let d = if e.base == 0 {
            crate::segment::decompress(&c, e.rawlen as usize)?
        } else {
            if e.base > id {
                return corrupt(format!("piece {id} is compressed against a later piece"));
            }
            blob::unpack(&c, Some(&self.piece(e.base - 1)?), e.rawlen as usize)?
        };
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

    /// Put cache markers back into a message, system prompt or tool list.
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
        self.read_nodes(&core, &path, &mut mbuf, &mut spans, st)?;
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

    pub fn rebuild(&self, ri: ReqId) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.rebuild_into(ri, &mut out, &mut ReadStats::default())?;
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
        (self.segs.values().map(|s| s.cat.requests).sum::<u64>() - self.dead_total().requests) as usize + self.mem.req_ix.len()
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
        self.read_nodes(&core, ids, &mut buf, &mut spans, st)?;
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
        self.read_nodes(&core, &ids, &mut buf, &mut spans, st)?;
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
            runs: self.runs.len(),
            blobs: self.blob_files.len(),
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
            st.dict_bytes += c.dict.map_or(0, |r| r.clen as u64);
            st.seg_file_bytes += sg.file_bytes;
            st.summary_bytes += c.stats_bytes;
            st.catalog_bytes += c.bytes();
        }
        let dead = self.dead_total();
        st.nodes -= dead.nodes as usize;
        st.steps -= dead.steps as usize;
        st.node_bytes -= dead.node_bytes;
        st.frame_bytes -= dead.frame_bytes;
        st.dead_bytes = dead.bytes;
        st.catalog_bytes += self.manifest.dead.len() * 56;
        st.index_bytes = st.seg_file_bytes.saturating_sub(st.frame_bytes + dead.bytes);
        for r in &self.runs {
            st.index_file_bytes += r.file_bytes;
            st.opaque_bytes += r.cat.opaque_bytes;
            st.piece_bytes += r.cat.piece_bytes;
            st.piece_raw_bytes += r.cat.piece_raw_bytes;
            st.catalog_bytes += r.cat.bytes();
        }
        for b in self.blob_files.values() {
            st.blob_file_bytes += b.len;
            st.catalog_bytes += 96 + b.path.as_os_str().len();
        }
        st.nodes += self.mem.nodes.len();
        st.node_bytes += self.mem.nodes.iter().map(|n| n.len() as u64).sum::<u64>();
        st.piece_raw_bytes += self.mem.pieces.iter().map(|p| p.2.len() as u64).sum::<u64>();
        st.opaque_bytes += self.mem.opaques.iter().map(|o| o.3.len() as u64).sum::<u64>();
        for a in self.active.values() {
            st.steps += a.steps.rows.len() - a.steps.flushed as usize;
            st.active_bytes += a.core.bytes() + a.steps.bytes() + a.children.len() * 40 + a.stored_kids.len() * 4 + a.keyed.len() * 8;
        }
        st.caches = vec![
            ("sessions", self.cores.stats()),
            ("steps", self.stepc.stats()),
            ("steps without values", self.lightc.stats()),
            ("steps with some fields' values", self.fieldc.stats()),
            ("step values read by summaries", self.textc.stats()),
            ("step summaries", self.statc.stats()),
            ("long input values", self.longc.stats()),
            ("blocks", self.blocks.stats()),
            ("directories", self.dirs.stats()),
            ("index blocks", self.ix.stats()),
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

    /// A session's steps with their values if they are in memory already (taking writes or
    /// cached), without reading anything.
    pub fn cached_steps(&self, s: u32) -> Option<Arc<SessionSteps>> {
        if let Some(a) = self.active.get(&s) {
            return Some(a.steps.clone());
        }
        self.stepc.get(&s)
    }

    /// A session's steps, possibly without their input values (`light`): for reading rows,
    /// field codes, turns and shapes only.
    pub fn light_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        self.load_light_steps(s)
    }

    /// A session's steps with at least the values of fields `fields` (ascending store
    /// field codes): the whole steps when they are at hand, else a copy holding only the
    /// fields queries have asked for so far.
    pub fn steps_with_fields(&self, s: u32, fields: &[u32]) -> Result<Arc<SessionSteps>> {
        if fields.is_empty() {
            return self.load_light_steps(s);
        }
        if let Some(a) = self.active.get(&s) {
            return Ok(a.steps.clone());
        }
        if let Some(c) = self.stepc.get(&s) {
            return Ok(c);
        }
        let mut keep = fields.to_vec();
        if let Some(c) = self.fieldc.get(&s) {
            if fields.iter().all(|f| c.kept.contains(f)) {
                return Ok(c);
            }
            keep.extend_from_slice(&c.kept);
            keep.sort_unstable();
            keep.dedup();
        }
        let ss = self.read_steps(s)?.with_fields(&keep);
        let size = ss.bytes();
        Ok(self.fieldc.insert(s, Arc::new(ss), size))
    }

    /// The text of a step value a summary points at: `vlen` bytes at `voff` in part
    /// `part`'s step data, which starts at `off` in block `r` of segment `num`. Kept after
    /// the first read.
    pub fn summary_text(&self, num: u32, part: u32, r: &BlockRef, off: u32, voff: u32, vlen: u32) -> Result<Arc<Vec<u8>>> {
        let key = (num, part, voff);
        if let Some(t) = self.textc.get(&key) {
            return Ok(t);
        }
        let b = self.stat_step_block(num, r)?;
        let a = off as usize + voff as usize;
        let t = b.post().get(a..a + vlen as usize).ok_or_else(|| bad("summary value past its block"))?.to_vec();
        let size = 64 + t.len();
        Ok(self.textc.insert(key, Arc::new(t), size))
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
                let fs = ch.fs as usize;
                let (ga, gb) = (base as usize + a, base as usize + b);
                let (f0, f1) = (ga / fs, (gb - 1) / fs);
                let frames = sg.frames(&ch.frames, ch.raw_len, fs, f0, f1)?;
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

    /// The raw JSON text of long input value `v` (a `LONG_BASE` code) of field `name` of
    /// step `pos`: just its bytes, unless an opaque value sits inside (then the message is
    /// rebuilt). `None` if the call no longer has the field.
    /// Kept in a cache after the first read: a message of a session never changes.
    pub fn long_value(&self, s: u32, ss: &SessionSteps, pos: u32, v: u32, name: &str) -> Result<Option<Vec<u8>>> {
        let key = (s, ss.rows[pos as usize].call_node, ss.long_at(v).0);
        if let Some(x) = self.longc.get(&key) {
            return Ok((*x).clone());
        }
        let x = self.read_long_value(s, ss, pos, v, name)?;
        let size = 64 + x.as_ref().map_or(0, |b| b.len());
        Ok((*self.longc.insert(key, Arc::new(x), size)).clone())
    }

    /// [`Db::long_value`] straight from the message (what writing a segment uses: it reads
    /// every long value once).
    fn read_long_value(&self, s: u32, ss: &SessionSteps, pos: u32, v: u32, name: &str) -> Result<Option<Vec<u8>>> {
        let (off, len) = ss.long_at(v);
        let r = ss.rows[pos as usize];
        match self.stored_range(s, r.call_node, off, len)? {
            Some(b) => Ok(Some(b)),
            None => self.read_input(s, ss, pos, name),
        }
    }

    /// The raw JSON text of input field `name` of a step, read from the stored message (the
    /// full message only if an opaque value sits inside that field).
    pub fn read_input(&self, s: u32, ss: &SessionSteps, pos: u32, name: &str) -> Result<Option<Vec<u8>>> {
        let r = ss.rows[pos as usize];
        let find = |m: &[u8]| -> Option<(usize, usize)> {
            let (calls, _) = steps::scan_message(m);
            let c = calls.iter().find(|c| c.block == r.call_block)?;
            let (_, v) = c.fields.iter().find(|(k, _)| *k == name.as_bytes())?;
            let start = v.as_ptr() as usize - m.as_ptr() as usize;
            Some((start, start + v.len()))
        };
        let (stored, holes) = self.stored_message(s, r.call_node)?;
        match find(&stored) {
            None => Ok(None),
            Some((a, b)) if !holes.iter().any(|&h| (h as usize) >= a && (h as usize) <= b) => Ok(Some(stored[a..b].to_vec())),
            Some(_) => {
                let m = self.message(s, r.call_node)?;
                Ok(find(&m).map(|(a, b)| m[a..b].to_vec()))
            }
        }
    }

    // ------------------------------------------------------------------ step summaries

    /// Where every step is, for queries that read summaries: each live segment with
    /// summaries as a whole (with which of its parts are live, when folds replaced some),
    /// and as row ranges of sessions the rest: parts in segments without summaries and
    /// steps not yet flushed. Every step of every session is in exactly one source.
    pub fn step_sources(&self) -> Result<Vec<StepSource>> {
        let mut out = Vec::new();
        for (&num, seg) in &self.segs {
            let live = if self.manifest.dead.iter().any(|(n, _)| *n == num) { Some(self.live_mask(seg)?) } else { None };
            if seg.cat.stats.is_some() {
                out.push(StepSource::Seg { num, live });
                continue;
            }
            let mut i = 0;
            for k in 0..seg.cat.dirs.len() {
                for d in self.dir_block(seg, k)?.iter() {
                    if live.as_ref().is_none_or(|m| m[i]) && d.n_rows > 0 {
                        out.push(StepSource::Rows { session: d.session, start: d.first_row, end: d.first_row + d.n_rows });
                    }
                    i += 1;
                }
            }
        }
        let mut act: Vec<(&u32, &Active)> = self.active.iter().collect();
        act.sort_by_key(|x| *x.0);
        for (&s, a) in act {
            if (a.steps.flushed as usize) < a.steps.rows.len() {
                out.push(StepSource::Rows { session: s, start: a.steps.flushed, end: a.steps.rows.len() as u32 });
            }
        }
        Ok(out)
    }

    /// Which parts of a segment are live (in directory order): a part a fold replaced is
    /// covered by a part of the same session in another segment.
    fn live_mask(&self, seg: &Arc<Segment>) -> Result<Arc<Vec<bool>>> {
        let generation = self.manifest.generation;
        if let Some((g, m)) = self.live_masks.lock().unwrap().get(&seg.num)
            && *g == generation
        {
            return Ok(m.clone());
        }
        let mut mask = Vec::new();
        let mut live_of: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
        for k in 0..seg.cat.dirs.len() {
            for d in self.dir_block(seg, k)?.iter() {
                if let std::collections::hash_map::Entry::Vacant(e) = live_of.entry(d.session) {
                    e.insert(self.parts(d.session)?.iter().filter(|p| p.0.num == seg.num).map(|p| (p.1.first_req, p.1.first_row)).collect());
                }
                mask.push(live_of[&d.session].contains(&(d.first_req, d.first_row)));
            }
        }
        let m = Arc::new(mask);
        self.live_masks.lock().unwrap().insert(seg.num, (generation, m.clone()));
        Ok(m)
    }

    fn stat_block(&self, num: u32, r: &BlockRef, decode: impl FnOnce(&[u8]) -> Result<StatBlock>) -> Result<Arc<StatBlock>> {
        let key = (num, r.off);
        if let Some(b) = self.statc.get(&key) {
            return Ok(b);
        }
        let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
        let b = decode(&seg.block(r)?)?;
        let size = b.bytes();
        Ok(self.statc.insert(key, Arc::new(b), size))
    }

    /// The head of a segment's summaries (`None` for a segment written without them).
    pub fn stats_head(&self, num: u32) -> Result<Option<Arc<StatBlock>>> {
        let Some(seg) = self.segs.get(&num) else { return corrupt(format!("segment {num} is gone")) };
        let Some(r) = seg.cat.stats else { return Ok(None) };
        Ok(Some(self.stat_block(num, &r, |b| Ok(StatBlock::Head(stats::decode_head(b)?)))?))
    }

    pub fn stats_rows(&self, num: u32, head: &StatsHead, f: &FieldDir) -> Result<Arc<StatBlock>> {
        let n = head.parts.len();
        self.stat_block(num, &f.rows, |b| Ok(StatBlock::Rows(stats::decode_rows(b, n)?)))
    }

    /// The block of values holding value `v` of a field.
    pub fn stats_values(&self, num: u32, f: &FieldDir, v: u32) -> Result<Arc<StatBlock>> {
        let i = f.chunks.partition_point(|c| c.0 <= v).checked_sub(1).ok_or_else(|| bad("summary value before the first block"))?;
        let (first, r) = f.chunks[i];
        let b = self.stat_block(num, &r, |b| Ok(StatBlock::Vals(stats::decode_chunk(b, first)?)))?;
        if (v - first) as usize >= b.vals().len() {
            return corrupt("summary value past its block");
        }
        Ok(b)
    }

    /// A field's word list and postings.
    pub fn stats_words(&self, num: u32, f: &FieldDir) -> Result<(Arc<StatBlock>, Arc<StatBlock>)> {
        let post = self.stat_block(num, &f.post, |b| Ok(StatBlock::Post(b.to_vec())))?;
        let n = post.post().len();
        let words = self.stat_block(num, &f.words, |b| Ok(StatBlock::Words(stats::decode_words(b, n)?)))?;
        Ok((words, post))
    }

    /// A block of encoded step parts, decompressed, kept with the summaries' blocks (whose
    /// cache is not split into shards, so a block of one large session still fits).
    pub fn stat_step_block(&self, num: u32, r: &BlockRef) -> Result<Arc<StatBlock>> {
        let key = (num, r.off);
        if let Some(b) = self.statc.get(&key) {
            return Ok(b);
        }
        let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
        // step blocks may be compressed with the segment's dictionary
        let b = StatBlock::Raw(seg.step_block(r)?);
        let size = b.bytes();
        Ok(self.statc.insert(key, Arc::new(b), size))
    }

    /// A segment's tool and field codes mapped to the store's.
    pub fn seg_codes(&self, num: u32) -> Option<&(Vec<u32>, Vec<u32>)> {
        self.seg_codes.get(&num)
    }

    /// The name of a segment's field code (for reports).
    pub fn seg_field_name(&self, num: u32, seg_field: u32) -> String {
        match self.seg_codes.get(&num).and_then(|c| c.1.get(seg_field as usize)) {
            Some(&f) => String::from_utf8_lossy(self.sd.fields.get(f)).into_owned(),
            None => format!("#{seg_field}"),
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

    /// Opaque values by kind: (kind, values, bytes). Reads every opaque value table.
    pub fn opaque_kinds(&self) -> Result<Vec<(&'static str, u64, u64)>> {
        let mut out: Vec<(&'static str, u64, u64)> = Vec::new();
        let mut add = |kind: u8, len: u64| {
            let name = OpaqueKind::from_u8(kind).name();
            let i = match out.iter().position(|x| x.0 == name) {
                Some(i) => i,
                None => {
                    out.push((name, 0, 0));
                    out.len() - 1
                }
            };
            (out[i].1, out[i].2) = (out[i].1 + 1, out[i].2 + len);
        };
        for r in &self.runs {
            for i in 0..(r.cat.no as usize).div_ceil(IDS) {
                if let IxBlock::Opaques(v) = r.block(K_OPAQUES, i)? {
                    for o in v {
                        add(o.kind, o.len as u64);
                    }
                }
            }
        }
        for (_, kind, _, p) in &self.mem.opaques {
            add(*kind, p.len() as u64);
        }
        out.sort_by_key(|x| std::cmp::Reverse(x.2));
        Ok(out)
    }

    /// Every opaque value in id order: (id, kind, enc, payload). For size measurements:
    /// reads every value.
    pub fn each_opaque(&self, mut f: impl FnMut(u64, u8, u8, &[u8])) -> Result<()> {
        for r in &self.runs {
            for i in 0..(r.cat.no as usize).div_ceil(IDS) {
                if let IxBlock::Opaques(v) = r.block(K_OPAQUES, i)? {
                    for (j, o) in v.iter().enumerate() {
                        let p = self.blob_read(o.file, o.off, o.len)?;
                        f(r.cat.olo + (i * IDS + j) as u64, o.kind, o.enc, &p);
                    }
                }
            }
        }
        for (k, (enc, kind, _, p)) in self.mem.opaques.iter().enumerate() {
            f(self.manifest.opaques + k as u64, *kind, *enc, p);
        }
        Ok(())
    }

    /// Segment bytes by what they hold: (frames, message/request blocks, step blocks,
    /// directory blocks, raw bytes of those three kinds of block, the rest: magic, catalogs
    /// and footers). For size measurements: reads every directory block.
    pub fn segment_split(&self) -> Result<[u64; 8]> {
        let mut t = [0u64; 8];
        for sg in self.segs.values() {
            t[0] += sg.cat.frame_bytes;
            let (mut core, mut steps) = (BTreeMap::new(), BTreeMap::new());
            for (k, (_, r)) in sg.cat.dirs.iter().enumerate() {
                t[3] += r.clen as u64;
                t[6] += r.rawlen as u64;
                for d in sg.dir_block(k)? {
                    core.insert(d.core.0.off, d.core.0);
                    steps.insert(d.steps.0.off, d.steps.0);
                }
            }
            for r in core.values() {
                t[1] += r.clen as u64;
                t[4] += r.rawlen as u64;
            }
            for r in steps.values() {
                t[2] += r.clen as u64;
                t[5] += r.rawlen as u64;
            }
            t[7] += sg.file_bytes;
        }
        t[7] -= t[0] + t[1] + t[2] + t[3];
        Ok(t)
    }

    /// Every shared piece in id order: (id, kind, raw bytes, stored bytes, id + 1 of the piece
    /// it is compressed against or 0). For size measurements.
    pub fn each_piece(&self, mut f: impl FnMut(u32, u8, &[u8], u32, u32)) -> Result<()> {
        for r in &self.runs {
            for i in 0..(r.cat.np as usize).div_ceil(IDS) {
                if let IxBlock::Pieces(v) = r.block(K_PIECES, i)? {
                    for (j, e) in v.iter().enumerate() {
                        let id = r.cat.plo + (i * IDS + j) as u32;
                        f(id, e.kind, &self.piece(id)?, e.clen, e.base);
                    }
                }
            }
        }
        for (k, (kind, _, b)) in self.mem.pieces.iter().enumerate() {
            f(self.manifest.pieces + k as u32, *kind, b, b.len() as u32, 0);
        }
        Ok(())
    }

    /// A session's messages as stored, in storage order: (node id, parent, stored bytes
    /// with opaque values cut out, (offset, opaque id) per cut value). For size
    /// measurements.
    #[allow(clippy::type_complexity)]
    pub fn stored_nodes(&self, s: u32) -> Result<Vec<(u32, u32, Vec<u8>, Vec<(u32, u64)>)>> {
        let core = self.load_core(s)?;
        let mut ids: Vec<u32> = (0..core.nodes.len() as u32).collect();
        ids.sort_by_key(|&i| match core.nodes[i as usize].loc {
            Loc::Seg { chunk, off } => (0u8, chunk, off),
            Loc::Mem(m) => (1u8, 0, m),
        });
        let mut cache = HashMap::new();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let nm = &core.nodes[id as usize];
            let mut b = Vec::with_capacity(nm.len as usize);
            self.stored_bytes(&core, nm, &mut cache, &mut b)?;
            out.push((id, nm.parent, b, nm.holes.as_deref().map(|h| h.to_vec()).unwrap_or_default()));
        }
        Ok(out)
    }

    /// Sessions with a part written in cold runs (bigger than its segment's frames), and the
    /// raw message bytes of those parts. Loads every session's metadata.
    pub fn cold_sessions(&self) -> Result<(usize, u64)> {
        let (mut n, mut bytes) = (0, 0);
        for s in 0..self.session_count() as u32 {
            let core = self.load_core(s)?;
            let cold: u64 = core.chunks.iter().filter(|c| self.segs.get(&c.seg).is_some_and(|g| c.fs > g.cat.frame_size)).map(|c| c.raw_len).sum();
            if cold > 0 {
                n += 1;
                bytes += cold;
            }
        }
        Ok((n, bytes))
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
        for c in [&self.cores as &dyn Clear, &self.stepc, &self.lightc, &self.dirs, &self.blocks, &self.ix, &self.pieces, &self.skels, &self.statc, &self.longc, &self.fieldc, &self.textc] {
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
