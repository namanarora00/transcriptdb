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
use crate::query::value::Value;
use crate::record::*;
use crate::rules::RuleSet;
use crate::segment::{decode_core, decode_steps, key_bytes, seg_path, Codec, CorePart, DirEnt, SegNode, SegWriter, Segment, StepPart};
use crate::split::{self, HoleKind, Keep, OpaqueEnc, OpaqueHole, OpaqueKind, Skeleton};
use crate::stats::{self, FieldDir, FieldRows, StatsHead, ValChunk, Words};
use crate::steps::{self, FieldVal, SessionSteps, StepDicts, StepRow};
use crate::wal::{self, SyncMode, WalWriter};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
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
    /// Merges of segments run on a thread of their own, reading a view of the files, and
    /// are put in place at the writer's next flush: a write never waits for one. Without
    /// it a flush that sets off a merge returns when the merge is done.
    pub background_merges: bool,
    /// When memory reaches `flush_bytes`, it is written out on a thread of its own while a
    /// new buffer takes the writes that follow (up to twice `flush_bytes` in all before a
    /// write waits). Without it the write that fills memory waits for the flush.
    pub background_flush: bool,
    /// A cache budget shared with other stores (a server's projects): when set, this
    /// store's caches use it instead of a budget of `cache_bytes` of their own.
    pub cache_pool: Option<Arc<crate::cache::Pool>>,
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
            background_merges: true,
            background_flush: true,
            cache_pool: None,
        }
    }
}

/// One request as it arrives: the exact body the model API received, plus who sent it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Put<'a> {
    pub session: &'a str,
    /// Empty for the main agent; a subagent id otherwise.
    pub agent: &'a str,
    pub id: &'a str,
    pub time: &'a str,
    pub meta: &'a [u8],
    pub body: &'a [u8],
    /// The response the model gave, kept exactly as it came (empty: none). Not split or
    /// read as messages; [`Db::response`] gives it back.
    pub response: &'a [u8],
}

/// A request body split and hashed: the part of a put that needs nothing from the store
/// (it is most of a put's work, since a body carries the whole conversation so far), so
/// threads taking requests do it at once before handing them to the one thread that writes.
pub struct Prepared {
    sp: split::Split,
    /// Each message's hash, as it came.
    hashes: Vec<u128>,
    skeleton: Vec<u8>,
    system_hash: u128,
    tools_hash: u128,
    skeleton_hash: u128,
}

impl Prepared {
    /// Split `body` and hash its parts; with `verify_split`, check the parts join back to it.
    pub fn new(opts: &Options, body: &[u8]) -> Result<Prepared> {
        let sp = split::split(body)?;
        if opts.verify_split && split::join(&sp)? != body {
            return Err(Error::BadRequest("split pieces do not join back to the body".into()));
        }
        let hashes = sp.messages.iter().map(|m| xxh3_128(&m.bytes)).collect();
        let skeleton = sp.skeleton.encode();
        let system_hash = sp.system.as_ref().map_or(0, |x| piece_hash(PIECE_SYSTEM, &x.bytes));
        let tools_hash = sp.tools.as_ref().map_or(0, |x| piece_hash(PIECE_TOOLS, &x.bytes));
        let skeleton_hash = piece_hash(PIECE_SKELETON, &skeleton);
        Ok(Prepared { sp, hashes, skeleton, system_hash, tools_hash, skeleton_hash })
    }
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
    /// (session id, agent id).
    pub fn key(&self) -> (&str, &str) {
        (&self.key.0, &self.key.1)
    }

    /// The session's requests, in the order they were stored.
    pub fn requests(&self) -> &[ReqRec] {
        &self.requests
    }
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
        // responses are never walked through
        let responses: std::collections::HashSet<u32> = core.requests.iter().map(|r| r.response).filter(|&r| r != NONE).collect();
        let mut stored_kids: Vec<u32> = (0..core.nodes.len() as u32).filter(|i| !responses.contains(i)).collect();
        stored_kids.sort_by_key(|&i| core.nodes[i as usize].parent);
        Active { core, steps, children: HashMap::new(), stored_kids, keyed: Default::default(), last }
    }

    /// Whether all of the session is in files.
    fn flushed(&self) -> bool {
        self.core.flushed_nodes as usize == self.core.nodes.len() && self.core.flushed_reqs as usize == self.core.requests.len() && self.steps.flushed as usize == self.steps.rows.len()
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
    /// Not flushed yet: the value's id.
    Mem(u64),
    Blob { blob: u32, off: u64, len: u32 },
}

#[derive(Debug, Clone, Copy)]
struct OpaqueMeta {
    enc: u8,
    hash: u128,
    loc: OLoc,
}

/// Unflushed data: messages, shared pieces and opaque values, and the requests that came
/// with them, numbered on from the ids already in files. The buffer taking writes is one;
/// a snapshot seals it, so the writer goes on in a new one while the snapshot reads the
/// sealed one (see [`Db::snapshot`]); a flush writes them all out as one.
#[derive(Default)]
struct Mem {
    /// `Loc::Mem(node_base + i)` is `nodes[i]`.
    node_base: u32,
    nodes: Vec<Arc<[u8]>>,
    /// Pieces with ids from `piece_base` on.
    piece_base: u32,
    pieces: Vec<(u8, u128, Arc<[u8]>)>,
    /// Opaque values with ids from `opaque_base` on: (enc, kind, hash, payload).
    opaque_base: u64,
    opaques: Vec<(u8, u8, u128, Arc<[u8]>)>,
    piece_ix: HashMap<u128, u32>,
    opaque_ix: HashMap<u128, u64>,
    req_ix: HashMap<Box<str>, ReqId>,
    bytes: usize,
    touched: BTreeSet<u32>,
}

impl Mem {
    fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.pieces.is_empty() && self.opaques.is_empty() && self.req_ix.is_empty() && self.touched.is_empty()
    }

    /// An empty buffer for the ids after this one's.
    fn next(&self) -> Mem {
        Mem {
            node_base: self.node_base + self.nodes.len() as u32,
            piece_base: self.piece_base + self.pieces.len() as u32,
            opaque_base: self.opaque_base + self.opaques.len() as u64,
            ..Mem::default()
        }
    }

    /// Buffers that follow each other (oldest first) as one; their contents are shared.
    fn join(ms: &[&Mem]) -> Result<Mem> {
        let mut out = Mem { node_base: ms[0].node_base, piece_base: ms[0].piece_base, opaque_base: ms[0].opaque_base, ..Mem::default() };
        for m in ms {
            if m.node_base != out.node_base + out.nodes.len() as u32 || m.piece_base != out.piece_base + out.pieces.len() as u32 || m.opaque_base != out.opaque_base + out.opaques.len() as u64 {
                return corrupt("unflushed buffers do not follow each other");
            }
            out.nodes.extend(m.nodes.iter().cloned());
            out.pieces.extend(m.pieces.iter().cloned());
            out.opaques.extend(m.opaques.iter().cloned());
            out.piece_ix.extend(m.piece_ix.iter().map(|(k, v)| (*k, *v)));
            out.opaque_ix.extend(m.opaque_ix.iter().map(|(k, v)| (*k, *v)));
            out.req_ix.extend(m.req_ix.iter().map(|(k, v)| (k.clone(), *v)));
            out.bytes += m.bytes;
            out.touched.extend(m.touched.iter().copied());
        }
        Ok(out)
    }
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
    /// Flushes run on their own thread, and the longest from start to being put in place.
    pub bg_flushes: u64,
    pub flush_ms_max: u64,
    /// Writes that waited for a flush at work (memory held two buffers' worth), and how long.
    pub flush_waits: u64,
    pub flush_wait_ms_max: u64,
    pub flush_wait_ms_total: u64,
    /// The longest each of these took on the thread taking writes: putting a flush in place
    /// (everything below included), merging index runs, putting a merge in place, and
    /// copying a session's tables for its first write after a flush started.
    pub install_ms_max: u64,
    pub run_merge_ms_max: u64,
    pub merge_install_ms_max: u64,
    pub copy_ms_max: u64,
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
    /// Shared with views and snapshots: the file is deleted, once doomed, when the last of
    /// them lets it go (see [`crate::block::Doom`]).
    keep: Arc<BlobKeep>,
}

impl BlobFile {
    fn new(path: PathBuf, len: u64) -> BlobFile {
        BlobFile { uid: crate::block::uid(), keep: Arc::new(BlobKeep { path: path.clone(), doom: Default::default() }), path, len }
    }
    fn share(&self) -> BlobFile {
        BlobFile { uid: self.uid, path: self.path.clone(), len: self.len, keep: self.keep.clone() }
    }
}

struct BlobKeep {
    path: PathBuf,
    doom: crate::block::Doom,
}

impl Drop for BlobKeep {
    fn drop(&mut self) {
        self.doom.drop_file(&self.path);
    }
}

/// Where a query that reads summaries finds steps (see [`Db::step_sources`]).
#[derive(Debug, Clone)]
pub enum StepSource {
    /// A segment's summaries; with `live`, only the parts marked live count.
    Seg { num: u32, live: Option<Arc<Vec<bool>>> },
    /// Steps `start..end` of a session, read from its steps.
    Rows { session: u32, start: u32, end: u32 },
}

/// A window of a unit's lower-cased text (see [`Db::piece_texts`]).
#[derive(Debug)]
pub struct PieceText {
    pub text: String,
    /// The unit's text goes on before the window, or after it.
    pub cut_left: bool,
    pub cut_right: bool,
}

/// A text unit read to confirm a match.
#[derive(Debug)]
pub struct UnitText {
    pub text: String,
    /// Lower-casing it changes only ASCII letters as far as an ASCII pattern can tell (see
    /// `query::value::ascii_case_only`), so such a pattern is compared ignoring ASCII case.
    pub plain_case: bool,
}

/// Numbers for session ids and agent names: stored sessions with the same session id (a
/// main agent and its side agents) share the first number.
#[derive(Default)]
struct KeyIds {
    of: Vec<Option<(u32, u32)>>,
    sids: HashMap<String, u32>,
    agents: HashMap<String, u32>,
    /// The names, by id.
    sid_names: Vec<Arc<str>>,
    agent_names: Vec<Arc<str>>,
}

/// Where a search finds text units (see [`Db::text_sources`]).
#[derive(Debug, Clone)]
pub enum TextSource {
    /// A segment's word index; with `live`, only the parts marked live count.
    Seg { num: u32, live: Option<Arc<Vec<bool>>> },
    /// Messages `start..end` of a session that no index covers yet, read from the session.
    Nodes { session: u32, start: u32, end: u32 },
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

/// Group ids for one field's values across segments: values equal as query values share
/// an id (see [`Db::field_groups`]).
#[derive(Debug, Default)]
pub struct FieldGroups {
    /// Group key (see [`crate::query::value::group_key`]) -> id.
    pub ix: HashMap<Vec<u8>, u32>,
    /// The first value met of each group (raw JSON text).
    pub texts: Vec<Vec<u8>>,
    /// Per segment: each value's group id (`u32::MAX`: null).
    pub of: HashMap<u32, Vec<u32>>,
}

/// What a merge read and wrote, before it is put in place (see [`Db::merge_build`]).
struct MergeBuild {
    inputs: BTreeSet<u32>,
    outputs: Vec<u32>,
    /// Parts in other segments that folds replaced.
    dead: BTreeMap<u32, Dead>,
    bytes_in: u64,
    fold_read: u64,
    folded: u64,
    /// A rewrite of one segment to drop its replaced parts.
    reclaim: bool,
}

/// A merge at work on its own thread.
struct BgMerge {
    /// (Behind a lock only so the store stays shareable between query threads.)
    rx: Mutex<std::sync::mpsc::Receiver<Result<MergeBuild>>>,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

/// What a flush must write (see [`Db::freeze`]).
struct FlushJob {
    frozen: Arc<Mem>,
    /// The sessions with data in `frozen`, as they were when it was frozen.
    sessions: Vec<(u32, Arc<SessionCore>, Arc<SessionSteps>)>,
    /// Sessions numbered below this exist (the flush indexes the new ones).
    sessions_hi: u32,
    checkpoint: u64,
    /// The log file the writes after the freeze went to.
    wal_start: u64,
    /// The first of the three file numbers set aside for it.
    first_file: u32,
    piece_bases: HashMap<u8, Vec<(u32, u8)>>,
}

/// One session's part written by a flush, and how far the session was flushed.
struct FlushedPart {
    session: u32,
    part: CorePart,
    nodes: u32,
    requests: u32,
    rows: u32,
}

/// What a flush wrote, before it is put in place (see [`Db::flush_install`]).
struct FlushBuild {
    seg: u32,
    run: u32,
    /// The blob file appended to and its new length.
    blob: Option<(u32, u64)>,
    new_blob: bool,
    blobs: Vec<(u32, u64)>,
    written: Vec<FlushedPart>,
    piece_bases: HashMap<u8, Vec<(u32, u8)>>,
    flushed: u64,
    sessions_hi: u32,
    checkpoint: u64,
    wal_start: u64,
    pieces: u32,
    opaques: u64,
}

/// A flush at work on its own thread.
struct BgFlush {
    rx: Mutex<std::sync::mpsc::Receiver<Result<FlushBuild>>>,
    handle: Option<std::thread::JoinHandle<()>>,
    started: std::time::Instant,
}

/// File numbers set aside for the outputs of a merge on its own thread.
const MERGE_FILES: u32 = 4096;

/// What reading steps from a segment does about rule columns (see [`Db::seg_rules`]).
#[derive(Debug, Default)]
struct SegRules {
    /// Per field code of the segment: its stored values are not what the rules give now
    /// (another version of the rule, a rule that is gone, or an input field a rule hides).
    drop: Vec<bool>,
    /// Rules to work out for the segment's steps, in order.
    todo: Vec<usize>,
}

/// A step as a rule reads it, while the step is read from a segment or being written.
struct RuleView<'a> {
    db: &'a Db,
    s: u32,
    ss: &'a SessionSteps,
    row: &'a StepRow,
    fields: &'a [(u32, FieldVal)],
    /// The message holding the step's result, when it is at hand (a step being written).
    result_msg: Option<&'a [u8]>,
    /// The result message last read, by node: steps of one turn share it.
    last: &'a RefCell<Option<(u32, Arc<Vec<u8>>)>>,
    err: RefCell<Option<Error>>,
}

impl crate::rules::StepView for RuleView<'_> {
    fn tool(&self) -> Value {
        Value::Str(String::from_utf8_lossy(self.db.sd.tools.get(self.row.tool)).into())
    }
    fn failed(&self) -> bool {
        self.row.failed
    }
    fn out_bytes(&self) -> u32 {
        self.row.out_bytes
    }
    fn input(&self, name: &str) -> Value {
        let Some(f) = self.db.sd.fields.code(name.as_bytes()) else { return Value::Null };
        match self.fields.iter().find(|x| x.0 == f) {
            None => Value::Null,
            Some((_, FieldVal::Code(c))) => Value::from_json(self.ss.value(f, *c)),
            Some((_, FieldVal::Long { off, len })) => match self.db.long_input(self.s, self.row, *off, *len, name) {
                Ok(Some(t)) => Value::from_json(&t),
                Ok(None) => Value::Null,
                Err(e) => {
                    *self.err.borrow_mut() = Some(e);
                    Value::Null
                }
            },
        }
    }
    fn output(&self) -> Value {
        let text = |m: &[u8]| steps::result_text(m, self.row.result_block).map_or(Value::Null, |t| Value::Str(t.into()));
        if let Some(m) = self.result_msg {
            return text(m);
        }
        if let Some((n, m)) = &*self.last.borrow()
            && *n == self.row.result_node
        {
            return text(m);
        }
        match self.db.stored_message(self.s, self.row.result_node) {
            Ok((m, _)) => {
                let v = text(&m);
                *self.last.borrow_mut() = Some((self.row.result_node, Arc::new(m)));
                v
            }
            Err(e) => {
                *self.err.borrow_mut() = Some(e);
                Value::Null
            }
        }
    }
}

/// Every cache of a store, behind one byte budget. The handle taking writes and every
/// snapshot taken from it share one `Caches`, the way a database server's buffer pool is
/// shared by all its connections. What a key names never changes: blocks of a file that is
/// never rewritten, a message, a piece; and a session's tables and steps read from files are
/// keyed by the manifest generation they were read at, so a snapshot reading an older
/// generation never hands its tables to a newer one.
pub(crate) struct Caches {
    /// The byte budget all the caches below share.
    pool: Arc<crate::cache::Pool>,
    /// Directory blocks by (segment, block).
    dirs: Cache<(u32, u32), Vec<DirEnt>>,
    /// Segment blocks by (segment, offset).
    blocks: Cache<(u32, u64), Vec<u8>>,
    /// Index blocks by (run, kind, index).
    ix: Cache<(u32, u8, u32), IxBlock>,
    cores: Cache<(u32, u64), SessionCore>,
    /// Session tables without their requests (see [`Db::load_node_map`]).
    nodemaps: Cache<(u32, u64), SessionCore>,
    stepc: Cache<(u32, u64), SessionSteps>,
    /// Sessions' steps without their input values, for queries that read none.
    lightc: Cache<(u32, u64), SessionSteps>,
    /// Sessions' steps with the values of only the fields queries tested.
    fieldc: Cache<(u32, u64), SessionSteps>,
    /// Sessions' text units (what the user wrote and the assistant said), read whole.
    msgtextc: Cache<(u32, u64), crate::text::SessionTexts>,
    /// Segments' word indexes over message text: units and word lists.
    texthead: Cache<u32, crate::textindex::TextHead>,
    /// Their postings, by (segment, block).
    textpost: Cache<(u32, u32), Vec<u8>>,
    /// Texts of units a search read to confirm a match, by (session, node, block).
    unitc: Cache<(u32, u32, u32), UnitText>,
    /// Windows of lower-cased unit text a search read to confirm a match, by (session,
    /// node, block, piece).
    piecec: Cache<(u32, u32, u32, u32), PieceText>,
    /// Texts of step values that summaries read from step blocks, by (segment, part,
    /// offset in the part): a search confirms each candidate on its text, and its
    /// candidates are spread over far more step blocks than fit a cache.
    textc: Cache<(u32, u32, u32), Vec<u8>>,
    pieces: Cache<u32, Vec<u8>>,
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
    /// Directory blocks, decoded once and kept until their segment goes: finding a
    /// session's parts reads them for every session, and a cache small next to the store
    /// would decode them over and over. About 250 bytes per part.
    dir_res: Mutex<HashMap<(u32, u32), Arc<Vec<DirEnt>>>>,
    /// Per field: its values' groups across segments, for the manifest generation and the
    /// segments it was built from.
    field_groups: Mutex<HashMap<u32, (u64, Vec<u32>, Arc<FieldGroups>)>>,
    /// Each segment's text index head (its word list and per-unit tables), decoded once and
    /// kept until the segment goes, for the same reason: every text query reads every head,
    /// and one bigger than its share of the cache would be decoded for every query. About
    /// 30 bytes per text unit.
    head_res: Mutex<HashMap<u32, Arc<crate::textindex::TextHead>>>,
    /// A number for each session id and each agent name seen so far, by stored session.
    key_ids: Mutex<KeyIds>,
}

impl Caches {
    /// Empty caches on one budget (`pool`): memory goes to what is being read now.
    fn new(pool: Arc<crate::cache::Pool>) -> Caches {
        Caches {
            pool: pool.clone(),
            dirs: Cache::pooled(&pool, 16),
            blocks: Cache::pooled(&pool, 16),
            ix: Cache::pooled(&pool, 16),
            cores: Cache::pooled(&pool, 1),
            nodemaps: Cache::pooled(&pool, 4),
            stepc: Cache::pooled(&pool, 1),
            lightc: Cache::pooled(&pool, 1),
            fieldc: Cache::pooled(&pool, 1),
            msgtextc: Cache::pooled(&pool, 1),
            texthead: Cache::pooled(&pool, 1),
            textpost: Cache::pooled(&pool, 16),
            unitc: Cache::pooled(&pool, 4),
            piecec: Cache::pooled(&pool, 4),
            textc: Cache::pooled(&pool, 16),
            pieces: Cache::pooled(&pool, 16),
            skels: Cache::pooled(&pool, 16),
            statc: Cache::pooled(&pool, 1),
            longc: Cache::pooled(&pool, 16),
            live_masks: Mutex::new(HashMap::new()),
            dir_res: Mutex::new(HashMap::new()),
            field_groups: Mutex::new(HashMap::new()),
            head_res: Mutex::new(HashMap::new()),
            key_ids: Mutex::new(KeyIds::default()),
        }
    }
}

pub struct Db {
    /// Caches, shared by the writing handle and the snapshots taken from it (see
    /// [`Caches`]).
    c: Arc<Caches>,
    dir: PathBuf,
    /// Held with an exclusive lock while the store is open (by the handle `open` returned;
    /// views and snapshots hold none).
    _lock: Option<fs::File>,
    opts: Options,
    manifest: Manifest,
    segs: BTreeMap<u32, Arc<Segment>>,
    /// Segments a merge replaced that sessions taking writes still read their flushed
    /// messages from (the same bytes are in the merge's output): kept open, out of the
    /// manifest, until no such session points at them, then deleted.
    retired: HashMap<u32, Arc<Segment>>,
    /// The handle `open` returned, which owns the store's files; views and snapshots do not.
    owner: bool,
    /// Per segment: its tool and field codes mapped to the store's.
    seg_codes: HashMap<u32, (Vec<u32>, Vec<u32>)>,
    /// Column rules (`rules.rs`) and the store's field code of each rule's column.
    rules: Arc<RuleSet>,
    rule_fields: Vec<u32>,
    /// Per segment: what reading its steps does about rule columns.
    seg_rules: Mutex<HashMap<u32, Arc<SegRules>>>,
    /// Index runs in id order (oldest first), so piece and opaque id ranges ascend.
    runs: Vec<Arc<Run>>,
    blob_files: BTreeMap<u32, BlobFile>,
    /// Per piece kind, the last few pieces stored, with how many pieces reading each one
    /// needs first: new pieces of that kind are compressed against one of them when that pays.
    piece_bases: HashMap<u8, Vec<(u32, u8)>>,
    active: HashMap<u32, Active>,
    /// Sessions that took writes in the last `idle_flushes` flushes, as of the last flush
    /// (merges after it drop them from `active`).
    busy: BTreeSet<u32>,
    key_ix: HashMap<(String, String), u32>,
    next_session: u32,
    mem: Mem,
    /// Buffers sealed by snapshots since the last flush started, oldest first: they follow
    /// each other and `mem` follows them. Neighbours are joined as they pile up, so there
    /// are only about log2 of the snapshots taken of them.
    sealed: Vec<Arc<Mem>>,
    /// The write buffer being flushed, while a flush writes it out (on its own thread, or
    /// in [`Db::flush`]); reads see it as unflushed data until the flush is put in place.
    frozen: Option<Arc<Mem>>,
    /// The flush at work on its own thread, if any.
    fbg: Option<BgFlush>,
    /// `None` in a view (see [`Db::view`]), which takes no writes.
    wal: Option<WalWriter>,
    /// The merge at work on its own thread, if any.
    bg: Option<BgMerge>,
    seq: u64,
    sd: StepDicts,
    flushes: u64,
    /// Tool results whose call could not be found on their path.
    orphan_results: usize,
    pub recovery: RecoveryReport,
    pub merge_stats: MergeStats,
}

/// Blob file number of a piece or opaque value [`Db::vacuum`] removed: no live request uses
/// it, and its table entry says where nothing is.
const GONE: u32 = u32::MAX;

/// What [`Db::drop_sessions`] and [`Db::vacuum`] did.
#[derive(Debug, Clone, Default)]
pub struct DropReport {
    pub sessions: u64,
    pub requests: u64,
    /// Segment bytes that hold deleted sessions, until merges or a vacuum rewrite them.
    pub dead_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct VacuumReport {
    pub segments_before: usize,
    pub segments_after: usize,
    pub segment_bytes_before: u64,
    pub segment_bytes_after: u64,
    pub blob_bytes_before: u64,
    pub blob_bytes_after: u64,
    pub pieces_removed: u64,
    pub opaques_removed: u64,
    pub ms: u64,
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
        bytes: d.frame_bytes + share(&d.core) + share(&d.steps) + share(&d.vals),
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

impl Drop for Db {
    /// A merge still at work on its own thread is told to stop and waited for: its files
    /// must not appear after the store is closed (they would be orphans, and their numbers
    /// could be handed out again by the next open).
    fn drop(&mut self) {
        // views and snapshots own no files
        if !self.owner {
            return;
        }
        // a flush at work finishes writing its files; they are not put in place (its data is
        // in the log, and the files are removed as orphans at the next open)
        if let Some(mut f) = self.fbg.take()
            && let Some(h) = f.handle.take()
        {
            let _ = h.join();
        }
        // segments kept only for sessions taking writes are not in the manifest: they go too
        // (once snapshots still reading them are dropped)
        for (_, seg) in std::mem::take(&mut self.retired) {
            seg.doom();
        }
        if let Some(mut bg) = self.bg.take() {
            bg.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(h) = bg.handle.take() {
                let _ = h.join();
            }
            // done before it could be stopped: its outputs were never put in place
            if let Ok(Ok(b)) = bg.rx.lock().unwrap().try_recv() {
                for n in b.outputs {
                    let p = seg_path(&self.dir, n);
                    crate::block::forget(&p);
                    let _ = fs::remove_file(p);
                }
            }
        }
    }
}

impl Db {
    /// A store's state with nothing loaded yet.
    fn blank(dir: PathBuf, lock: Option<fs::File>, opts: Options, manifest: Manifest, wal: Option<WalWriter>, cb: usize) -> Db {
        Db::with_caches(dir, lock, opts, manifest, wal, Arc::new(Caches::new(crate::cache::Pool::new(cb))))
    }

    /// [`Db::blank`] with caches it shares.
    fn with_caches(dir: PathBuf, lock: Option<fs::File>, opts: Options, manifest: Manifest, wal: Option<WalWriter>, c: Arc<Caches>) -> Db {
        Db {
            c,
            dir,
            _lock: lock,
            manifest,
            segs: BTreeMap::new(),
            retired: HashMap::new(),
            owner: false,
            seg_codes: HashMap::new(),
            rules: Arc::new(RuleSet::default()),
            rule_fields: Vec::new(),
            seg_rules: Mutex::new(HashMap::new()),
            runs: Vec::new(),
            blob_files: BTreeMap::new(),
            piece_bases: HashMap::new(),
            active: HashMap::new(),
            busy: BTreeSet::new(),
            key_ix: HashMap::new(),
            next_session: 0,
            mem: Mem::default(),
            sealed: Vec::new(),
            wal,
            bg: None,
            frozen: None,
            fbg: None,
            seq: 0,
            sd: StepDicts::default(),
            flushes: 0,
            orphan_results: 0,
            recovery: RecoveryReport::default(),
            merge_stats: MergeStats::default(),
            opts,
        }
    }

    /// A second, read-only handle on the store as its files are now, for a merge to read
    /// from on its own thread while this one goes on taking writes. Everything must be
    /// flushed. It shares the open segments and index runs, has caches of its own (a
    /// quarter of the size), and never writes the manifest or a log.
    fn view(&self) -> Result<Db> {
        // not locked: this handle's owner holds the lock
        let mut v = Db::blank(self.dir.clone(), None, self.opts.clone(), self.manifest.clone(), None, (self.opts.cache_bytes / 4).max(16 << 20));
        v.segs = self.segs.clone();
        v.seg_codes = self.seg_codes.clone();
        v.rules = self.rules.clone();
        v.rule_fields = self.rule_fields.clone();
        v.runs = self.runs.clone();
        v.blob_files = self.blob_files.iter().map(|(n, b)| (*n, b.share())).collect();
        v.busy = self.busy.clone();
        v.next_session = self.next_session;
        v.seq = self.seq;
        v.sd = self.sd.clone();
        Ok(v)
    }

    /// A read-only handle on the store as it is now, for reading on other threads while
    /// this handle goes on taking writes: every read API works on it and sees exactly the
    /// requests put before it was taken, however long it is kept.
    ///
    /// It costs about one pointer per session taking writes. It shares this handle's caches,
    /// its open files and its unflushed data: the buffer taking writes is sealed and shared
    /// ([`Db::seal`]), and a session table this handle changes later is copied first
    /// (`Arc::make_mut`), as a flush at work already needs. Files a merge replaces stay on
    /// disk until the last snapshot reading them is dropped.
    pub fn snapshot(&mut self) -> Result<Db> {
        self.seal()?;
        let mut v = Db::with_caches(self.dir.clone(), None, self.opts.clone(), self.manifest.clone(), None, self.c.clone());
        v.segs = self.segs.clone();
        v.retired = self.retired.clone();
        v.seg_codes = self.seg_codes.clone();
        v.rules = self.rules.clone();
        v.rule_fields = self.rule_fields.clone();
        v.seg_rules = Mutex::new(self.seg_rules.lock().unwrap().clone());
        v.runs = self.runs.clone();
        v.blob_files = self.blob_files.iter().map(|(n, b)| (*n, b.share())).collect();
        v.busy = self.busy.clone();
        v.key_ix = self.key_ix.clone();
        v.next_session = self.next_session;
        v.seq = self.seq;
        v.sd = self.sd.clone();
        v.flushes = self.flushes;
        v.frozen = self.frozen.clone();
        v.sealed = self.sealed.clone();
        v.mem = self.mem.next();
        v.active = self.active.iter().map(|(&s, a)| (s, Active { core: a.core.clone(), steps: a.steps.clone(), children: HashMap::new(), stored_kids: Vec::new(), keyed: Default::default(), last: a.last })).collect();
        v.orphan_results = self.orphan_results;
        v.recovery = self.recovery.clone();
        v.merge_stats = self.merge_stats.clone();
        Ok(v)
    }

    /// The log sequence number of the last request put (each put is one).
    pub fn seq(&self) -> u64 {
        self.seq
    }

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
            // (a file this process doomed may be deleted by its deleter thread meanwhile)
            let remove = |p: PathBuf| match fs::remove_file(p) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            };
            if let Some(n) = file {
                if !live.contains(&(n as u32)) {
                    remove(dir.join(&name))?;
                    rec.orphans_removed += 1;
                }
            } else if let Some(n) = parse_num(&name, "wal-", ".log") {
                if n < manifest.wal_start {
                    remove(dir.join(&name))?;
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
        let mut db = match opts.cache_pool.clone() {
            Some(pool) => Db::with_caches(dir, Some(lock), opts, manifest, Some(wal), Arc::new(Caches::new(pool))),
            None => Db::blank(dir, Some(lock), opts, manifest, Some(wal), cb),
        };
        db.owner = true;
        let _ = fs::remove_file(db.dir.join("RULES.tmp"));
        match fs::read(db.dir.join("RULES")) {
            Ok(b) => db.install_rules(RuleSet::decode(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let mut segs = db.manifest.segs.clone();
        segs.sort_unstable();
        for n in segs {
            db.add_segment(Segment::open(&db.dir, n)?);
            rec.segments += 1;
        }
        for &(n, len) in &db.manifest.blobs {
            rec.blob_tail_bytes += blob::trim(&db.dir, n, len, db.opts.sync)?;
            db.blob_files.insert(n, BlobFile::new(blob_path(&db.dir, n), len));
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
        db.mem.piece_base = db.manifest.pieces;
        db.mem.opaque_base = db.manifest.opaques;

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

    // ------------------------------------------------------------------ column rules

    pub fn rules(&self) -> &RuleSet {
        &self.rules
    }

    /// Per rule: how many segments with steps carry its values as today's rule gives them,
    /// and how many segments with steps there are.
    pub fn rule_coverage(&self) -> Vec<(usize, usize)> {
        let with_steps: Vec<&Arc<Segment>> = self.segs.values().filter(|s| s.cat.steps > 0).collect();
        self.rules.rules.iter().map(|r| (with_steps.iter().filter(|s| s.cat.rules.iter().any(|x| x.0 == r.name.as_bytes() && x.1 == r.ver)).count(), with_steps.len())).collect()
    }

    fn install_rules(&mut self, rs: RuleSet) {
        self.rule_fields = rs.rules.iter().map(|r| self.sd.field(r.name.as_bytes())).collect();
        self.rules = Arc::new(rs);
        self.seg_rules.lock().unwrap().clear();
    }

    /// Add a column rule, or put it in place of the rule of its name. Steps written from
    /// now on carry its values; older steps get them when their sessions are read, and
    /// keep them once a merge has rewritten them.
    pub fn define_rule(&mut self, def: &str) -> Result<()> {
        let rs = self.rules.with(def).map_err(|e| Error::BadRequest(e.to_string()))?;
        self.set_rules(rs)
    }

    /// Remove a column rule. Its stored values are no longer read, and go at the next merge.
    pub fn drop_rule(&mut self, name: &str) -> Result<()> {
        let rs = self.rules.without(name).map_err(|e| Error::BadRequest(e.to_string()))?;
        self.set_rules(rs)
    }

    fn set_rules(&mut self, rs: RuleSet) -> Result<()> {
        // steps still in memory go to a segment under the rules they were worked out with
        self.flush()?;
        let tmp = self.dir.join("RULES.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, &rs.encode())?;
            wal::sync_file(&f, self.opts.sync)?;
        }
        fs::rename(&tmp, self.dir.join("RULES"))?;
        wal::sync_dir(&self.dir, self.opts.sync)?;
        // sessions held in memory carry the old rules' values: they are read again when needed
        for (_, a) in self.active.drain() {
            self.key_ix.remove(&a.core.key);
        }
        self.install_rules(rs);
        self.clear_caches();
        Ok(())
    }

    /// What reading steps from a segment does about rule columns: which stored fields are
    /// left out and which rules are worked out. A segment written under today's rules needs
    /// neither.
    fn seg_rules(&self, seg: &Segment) -> Arc<SegRules> {
        if let Some(x) = self.seg_rules.lock().unwrap().get(&seg.num) {
            return x.clone();
        }
        let applied = |name: &[u8]| seg.cat.rules.iter().find(|r| r.0 == name).map(|r| r.1);
        let current = |name: &[u8]| self.rules.position(name).map(|i| self.rules.rules[i].ver);
        let drop = seg.cat.fields.iter().map(|f| applied(f) != current(f)).collect();
        let todo = self.rules.rules.iter().enumerate().filter(|(_, r)| applied(r.name.as_bytes()) != Some(r.ver)).map(|x| x.0).collect();
        let x = Arc::new(SegRules { drop, todo });
        self.seg_rules.lock().unwrap().insert(seg.num, x.clone());
        x
    }

    /// Whether a segment's stored values of store field `f` are not what queries see (see
    /// [`SegRules::drop`]), or the field is a rule's column the segment has no values of.
    fn seg_field_stale(&self, seg: &Segment, f: u32) -> bool {
        let name = self.sd.fields.get(f);
        seg.cat.rules.iter().find(|r| r.0 == name).map(|r| r.1) != self.rules.position(name).map(|i| self.rules.rules[i].ver)
    }

    /// The raw JSON text of a long input value of a step that is being read or written.
    fn long_input(&self, s: u32, row: &StepRow, off: u32, len: u32, name: &str) -> Result<Option<Vec<u8>>> {
        if let Some(b) = self.stored_range(s, row.call_node, off, len)? {
            return Ok(Some(b));
        }
        // an opaque value sits inside: the field is read from the whole message
        let m = self.message(s, row.call_node)?;
        let (calls, _) = steps::scan_message(&m);
        Ok(calls.iter().find(|c| c.block == row.call_block).and_then(|c| c.fields.iter().find(|(k, _)| *k == name.as_bytes())).map(|(_, v)| v.to_vec()))
    }

    /// The values rules `todo` give one step, as (field, JSON text), for the rules that give
    /// one. `fields` are the step's input fields and the values of the rules not in `todo`.
    #[allow(clippy::too_many_arguments)]
    fn rule_values(&self, s: u32, ss: &SessionSteps, row: &StepRow, fields: &[(u32, FieldVal)], todo: &[usize], result_msg: Option<&[u8]>, last: &RefCell<Option<(u32, Arc<Vec<u8>>)>>) -> Result<Vec<(u32, Vec<u8>)>> {
        let view = RuleView { db: self, s, ss, row, fields, result_msg, last, err: RefCell::new(None) };
        let mut vals: Vec<Value> = Vec::with_capacity(self.rules.rules.len());
        let mut out = Vec::new();
        let mut t = 0;
        for i in 0..self.rules.rules.len() {
            let f = self.rule_fields[i];
            if todo.get(t) == Some(&i) {
                t += 1;
                let v = self.rules.eval(i, &view, &vals);
                if let Some(text) = crate::rules::stored(&v) {
                    out.push((f, text));
                }
                vals.push(v);
            } else {
                vals.push(match fields.iter().find(|x| x.0 == f) {
                    Some((_, FieldVal::Code(c))) => Value::from_json(ss.value(f, *c)),
                    _ => Value::Null,
                });
            }
        }
        match view.err.take() {
            Some(e) => Err(e),
            None => Ok(out),
        }
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
        self.mem.piece_base + self.mem.pieces.len() as u32
    }

    fn next_opaque(&self) -> u64 {
        self.mem.opaque_base + self.mem.opaques.len() as u64
    }

    /// A segment by number for reading a session's messages through its chunks: a live one,
    /// or one a merge retired that a session taking writes still points at.
    fn seg_by_num(&self, n: u32) -> Option<&Arc<Segment>> {
        self.segs.get(&n).or_else(|| self.retired.get(&n))
    }

    /// Delete the retired segments no session taking writes points at any more (on a thread
    /// of its own: removing a large file can take a while).
    fn drop_retired(&mut self) {
        if self.retired.is_empty() {
            return;
        }
        // a session all in files that points at a retired segment leaves memory (it is read
        // again from the live segments when needed)
        let stale: Vec<u32> = self.active.iter().filter(|(_, a)| a.flushed() && a.core.chunks.iter().any(|c| self.retired.contains_key(&c.seg))).map(|(&s, _)| s).collect();
        for s in stale {
            if let Some(a) = self.active.remove(&s) {
                self.key_ix.remove(&a.core.key);
            }
        }
        let used: BTreeSet<u32> = self.active.values().flat_map(|a| a.core.chunks.iter().map(|c| c.seg)).collect();
        let gone: Vec<u32> = self.retired.keys().copied().filter(|n| !used.contains(n)).collect();
        for n in gone {
            if let Some(seg) = self.retired.remove(&n) {
                seg.doom();
            }
        }
    }

    /// Wait for the files being deleted to be gone (those no snapshot still reads).
    fn removed(&mut self) {
        crate::block::wait_removed();
    }

    /// Drop decoded directory blocks and text heads of segments that are gone. A snapshot
    /// still reading a gone segment may decode them again; the next merge drops those too.
    fn prune_segment_tables(&self) {
        let live = |n: &u32| self.segs.contains_key(n) || self.retired.contains_key(n);
        self.c.dir_res.lock().unwrap().retain(|k, _| live(&k.0));
        self.c.head_res.lock().unwrap().retain(|k, _| live(k));
    }

    /// The bytes of an unflushed message, `Loc::Mem(i)`: in the buffer taking writes, or in
    /// the one being flushed.
    fn mem_node(&self, i: u32) -> &[u8] {
        for m in self.mems() {
            if i >= m.node_base && i < m.node_base + m.nodes.len() as u32 {
                return &m.nodes[(i - m.node_base) as usize];
            }
        }
        panic!("unflushed message {i} is in no buffer")
    }

    /// Every unflushed buffer, newest first: the one taking writes, the sealed ones, the one
    /// being flushed.
    fn mems(&self) -> impl Iterator<Item = &Mem> {
        std::iter::once(&self.mem).chain(self.sealed.iter().rev().map(|m| &**m)).chain(self.frozen.as_deref())
    }

    /// [`Db::mems`], oldest first.
    fn mems_oldest_first(&self) -> impl Iterator<Item = &Mem> {
        self.frozen.as_deref().into_iter().chain(self.sealed.iter().map(|m| &**m)).chain(std::iter::once(&self.mem))
    }

    /// Bytes not yet written and not being written by a flush.
    fn pending_bytes(&self) -> usize {
        self.mem.bytes + self.sealed.iter().map(|m| m.bytes).sum::<usize>()
    }

    /// Seal the buffer taking writes (if it holds anything) so a snapshot can share it, and
    /// join the newest sealed buffers while the newer is at least half the older: each byte
    /// is joined about log2(snapshots) times, and lookups go through few buffers.
    fn seal(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let next = self.mem.next();
        self.sealed.push(Arc::new(std::mem::replace(&mut self.mem, next)));
        while self.sealed.len() >= 2 {
            let n = self.sealed.len();
            if self.sealed[n - 1].bytes * 2 < self.sealed[n - 2].bytes {
                break;
            }
            let j = Mem::join(&[&self.sealed[n - 2], &self.sealed[n - 1]])?;
            self.sealed.truncate(n - 2);
            self.sealed.push(Arc::new(j));
        }
        Ok(())
    }

    /// An unflushed piece by id (`None`: it is in the files).
    fn mem_piece(&self, id: u32) -> Option<&(u8, u128, Arc<[u8]>)> {
        for m in self.mems() {
            if id >= m.piece_base && id < m.piece_base + m.pieces.len() as u32 {
                return Some(&m.pieces[(id - m.piece_base) as usize]);
            }
        }
        None
    }

    /// An unflushed opaque value by id (`None`: it is in the files).
    fn mem_opaque(&self, id: u64) -> Option<&(u8, u8, u128, Arc<[u8]>)> {
        for m in self.mems() {
            if id >= m.opaque_base && id < m.opaque_base + m.opaques.len() as u64 {
                return Some(&m.opaques[(id - m.opaque_base) as usize]);
            }
        }
        None
    }

    /// The request with this id among unflushed ones.
    fn mem_request(&self, id: &str) -> Option<ReqId> {
        self.mems().find_map(|m| m.req_ix.get(id)).copied().filter(|&r| !self.manifest.is_dropped(req_parts(r).0))
    }

    // ------------------------------------------------------------------ finding things in files

    fn dir_block(&self, seg: &Segment, k: usize) -> Result<Arc<Vec<DirEnt>>> {
        let key = (seg.num, k as u32);
        if let Some(d) = self.c.dir_res.lock().unwrap().get(&key) {
            return Ok(d.clone());
        }
        let d = Arc::new(seg.dir_block(k)?);
        self.c.dir_res.lock().unwrap().insert(key, d.clone());
        Ok(d)
    }

    /// (bytes held by all caches, their shared budget)
    pub fn cache_use(&self) -> (usize, usize) {
        (self.c.pool.used(), self.c.pool.limit())
    }

    /// Group ids for store field `field`'s values across `segs` (each a segment and the
    /// field's table there): what grouping by the field needs when several segments hold its
    /// values. Built once, by reading each segment's values once, and kept until the
    /// segments change; `None` when there are too many values to keep.
    pub fn field_groups(&self, field: u32, segs: &[(u32, FieldDir)]) -> Result<Option<Arc<FieldGroups>>> {
        let nums: Vec<u32> = segs.iter().map(|s| s.0).collect();
        let generation = self.manifest.generation;
        if let Some((g, n, fg)) = self.c.field_groups.lock().unwrap().get(&field)
            && *g == generation
            && *n == nums
        {
            return Ok(Some(fg.clone()));
        }
        let total: usize = segs.iter().map(|s| s.1.n_values as usize).sum();
        if total > 4 << 20 {
            return Ok(None);
        }
        let mut fg = FieldGroups::default();
        for (num, fd) in segs {
            let mut ids = Vec::with_capacity(fd.n_values as usize);
            for &(first, _) in &fd.chunks {
                if first >= fd.n_values {
                    break;
                }
                let b = self.stats_values(*num, fd, first)?;
                let c = b.vals();
                for v in first..first + c.len() as u32 {
                    let t = c.get(v);
                    ids.push(match crate::query::value::group_key(t) {
                        None => u32::MAX,
                        Some(k) => match fg.ix.get(&k) {
                            Some(&id) => id,
                            None => {
                                let id = fg.texts.len() as u32;
                                fg.texts.push(t.to_vec());
                                fg.ix.insert(k, id);
                                id
                            }
                        },
                    });
                }
            }
            if ids.len() != fd.n_values as usize {
                return corrupt(format!("segment {num}: a field's value blocks hold {} values, not {}", ids.len(), fd.n_values));
            }
            fg.of.insert(*num, ids);
        }
        let fg = Arc::new(fg);
        self.c.field_groups.lock().unwrap().insert(field, (generation, nums, fg.clone()));
        Ok(Some(fg))
    }

    /// Bytes the decoded directory blocks take, roughly.
    pub fn directory_bytes(&self) -> usize {
        self.c.dir_res.lock().unwrap().values().flat_map(|d| d.iter()).map(|x| 160 + x.key.0.len() + x.key.1.len()).sum()
    }

    fn block(&self, seg: &Segment, r: &BlockRef) -> Result<Arc<Vec<u8>>> {
        if let Some(x) = self.c.blocks.get(&(seg.num, r.off)) {
            return Ok(x);
        }
        let raw = seg.block(r)?;
        let n = raw.len();
        Ok(self.c.blocks.insert((seg.num, r.off), Arc::new(raw), n))
    }

    fn step_block(&self, seg: &Segment, r: &BlockRef) -> Result<Arc<Vec<u8>>> {
        if let Some(x) = self.c.blocks.get(&(seg.num, r.off)) {
            return Ok(x);
        }
        let raw = seg.step_block(r)?;
        let n = raw.len();
        Ok(self.c.blocks.insert((seg.num, r.off), Arc::new(raw), n))
    }

    fn ixb(&self, run: &Run, kind: u8, i: usize) -> Result<Arc<IxBlock>> {
        let k = (run.num, kind, i as u32);
        if let Some(t) = self.c.ix.get(&k) {
            return Ok(t);
        }
        let t = run.block(kind, i)?;
        let n = t.bytes();
        Ok(self.c.ix.insert(k, Arc::new(t), n))
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
        if self.manifest.is_dropped(s) {
            // deleted: what is left of it in files is never read
            return Ok(out);
        }
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
        if self.manifest.is_dropped(s) {
            return Ok(None);
        }
        if s >= self.next_session || self.load_core(s)?.key != *key {
            return corrupt("index points a session key at another session");
        }
        Ok(Some(s))
    }

    fn find_request_in_files(&self, id: &str) -> Result<Option<ReqId>> {
        let Some(r) = self.index_get(NS_REQUEST, request_hash(id))? else { return Ok(None) };
        if self.manifest.is_dropped(req_parts(r).0) {
            return Ok(None);
        }
        let (core, k) = self.core_req(r)?;
        if *core.requests[k as usize].id != *id {
            return corrupt("index points a request id at another request");
        }
        Ok(Some(r))
    }

    /// An existing piece or opaque value with this hash (tag 0 = piece, 1 = opaque).
    fn find_by_hash(&self, tag: u8, hash: u128) -> Result<Option<u64>> {
        for m in self.mems() {
            let found = if tag == 0 { m.piece_ix.get(&hash).map(|&x| x as u64) } else { m.opaque_ix.get(&hash).copied() };
            if found.is_some() {
                return Ok(found);
            }
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
            let (enc, _, hash, _) = self.mem_opaque(id).ok_or_else(|| bad(format!("missing opaque value {id}")))?;
            return Ok(OpaqueMeta { enc: *enc, hash: *hash, loc: OLoc::Mem(id) });
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

    /// A session's key in the caches of tables read from files: with the manifest
    /// generation they were read at, since snapshots of other generations share the caches.
    fn skey(&self, s: u32) -> (u32, u64) {
        (s, self.manifest.generation)
    }

    fn load_core(&self, s: u32) -> Result<Arc<SessionCore>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.core.clone());
        }
        if let Some(c) = self.c.cores.get(&self.skey(s)) {
            return Ok(c);
        }
        let core = self.read_core(s, true)?;
        let size = core.bytes();
        Ok(self.c.cores.insert(self.skey(s), Arc::new(core), size))
    }

    /// A session's message table without its requests: where each message is, all reading
    /// message text needs. A long session's requests are most of its table, so many more of
    /// these fit the cache than whole tables when a search reads text from many sessions.
    fn load_node_map(&self, s: u32) -> Result<Arc<SessionCore>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.core.clone());
        }
        if let Some(c) = self.c.cores.get(&self.skey(s)) {
            return Ok(c);
        }
        if let Some(c) = self.c.nodemaps.get(&self.skey(s)) {
            return Ok(c);
        }
        let core = self.read_core(s, false)?;
        let size = core.bytes();
        Ok(self.c.nodemaps.insert(self.skey(s), Arc::new(core), size))
    }

    /// A session's table decoded from its parts (`requests`: with the request records).
    fn read_core(&self, s: u32, requests: bool) -> Result<SessionCore> {
        if self.manifest.is_dropped(s) {
            return Ok(SessionCore::default());
        }
        let parts = self.parts(s)?;
        let Some(first) = parts.first() else { return Err(Error::BadRequest(format!("no session {s}"))) };
        let mut core = SessionCore { key: first.1.key.clone(), ..SessionCore::default() };
        for (seg, d) in &parts {
            let raw = self.block(seg, &d.core.0)?;
            let (a, b) = (d.core.1 as usize, (d.core.1 + d.core.2) as usize);
            let p = decode_core(raw.get(a..b).ok_or_else(|| bad("session part past its block"))?)?;
            if p.first_node as usize != core.nodes.len() || (requests && p.first_req as usize != core.requests.len()) {
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
            if requests {
                core.requests.extend(p.requests);
            } else if p.first_req as usize != core.flushed_reqs as usize {
                return corrupt(format!("session {s}: parts do not follow each other"));
            } else {
                // only the count, so the next part's check holds
                core.flushed_reqs += p.requests.len() as u32;
            }
        }
        if core.nodes.iter().any(|n| matches!(n.loc, Loc::Mem(u32::MAX))) {
            return corrupt(format!("session {s} is missing nodes"));
        }
        core.flushed_nodes = core.nodes.len() as u32;
        if requests {
            core.flushed_reqs = core.requests.len() as u32;
        }
        Ok(core)
    }

    fn load_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.steps.clone());
        }
        if let Some(c) = self.c.stepc.get(&self.skey(s)) {
            return Ok(c);
        }
        let ss = self.read_steps(s)?;
        let size = ss.bytes();
        Ok(self.c.stepc.insert(self.skey(s), Arc::new(ss), size))
    }

    /// A session's steps without their input values (or with them, when they are at hand),
    /// cached on their own: queries whose tests read no values use this.
    fn load_light_steps(&self, s: u32) -> Result<Arc<SessionSteps>> {
        if let Some(a) = self.active.get(&s) {
            return Ok(a.steps.clone());
        }
        if let Some(c) = self.c.stepc.get(&self.skey(s)) {
            return Ok(c);
        }
        if let Some(c) = self.c.lightc.get(&self.skey(s)) {
            return Ok(c);
        }
        let ss = match self.read_light_steps(s)? {
            Some(ss) => ss,
            None => self.read_steps(s)?.without_values(),
        };
        let size = ss.bytes();
        Ok(self.c.lightc.insert(self.skey(s), Arc::new(ss), size))
    }

    /// A session's steps without their input values, read without reading the values:
    /// what [`Db::read_steps`] then [`SessionSteps::without_values`] give, codes and counts
    /// included. Only for a session in one part whose segment needs no rule worked out
    /// (value codes are then each field's values numbered in order); `None` otherwise.
    fn read_light_steps(&self, s: u32) -> Result<Option<SessionSteps>> {
        let parts = self.parts(s)?;
        let [(seg, d)] = &parts[..] else { return Ok(None) };
        let sr = self.seg_rules(seg);
        if !sr.todo.is_empty() {
            return Ok(None);
        }
        let dropped = |f: u32| sr.drop.get(f as usize).copied().unwrap_or(false);
        let raw = self.step_block(seg, &d.steps.0)?;
        let (a, b) = (d.steps.1 as usize, (d.steps.1 + d.steps.2) as usize);
        let p = crate::segment::decode_steps_light(raw.get(a..b).ok_or_else(|| bad("step part past its block"))?)?;
        if p.first != 0 {
            return corrupt(format!("session {s}: its only step part does not start at its first step"));
        }
        let (tools, fields) = &self.seg_codes[&seg.num];
        let gf = |f: u32| fields.get(f as usize).copied().ok_or_else(|| bad("step names a missing field"));
        // each value's code: its place among the values of its field (as interning gives)
        let mut next: HashMap<u32, u32> = HashMap::new();
        let mut code = Vec::with_capacity(p.value_fields.len());
        for &f in &p.value_fields {
            if dropped(f) {
                code.push(u32::MAX);
                continue;
            }
            let n = next.entry(gf(f)?).or_insert(0);
            code.push(*n);
            *n += 1;
        }
        let mut ss = SessionSteps::default();
        let mut fs: Vec<(u32, FieldVal)> = Vec::new();
        for (mut r, rf) in p.rows.into_iter().zip(p.fields) {
            r.tool = *tools.get(r.tool as usize).ok_or_else(|| bad("step names a missing tool"))?;
            fs.clear();
            for (f, v) in rf {
                if dropped(f) {
                    continue;
                }
                let val = if steps::is_long(v) {
                    let (off, len) = p.longs[(v - steps::LONG_BASE) as usize];
                    FieldVal::Long { off, len }
                } else {
                    FieldVal::Code(code[v as usize])
                };
                fs.push((gf(f)?, val));
            }
            ss.push(r, &fs);
        }
        ss.flushed = ss.rows.len() as u32;
        ss.turns();
        ss.by_shape();
        let mut ss = ss.without_values();
        ss.counts = next.into_iter().collect();
        ss.counts.sort_unstable();
        Ok(Some(ss))
    }

    /// Decode a session's steps from its parts (with its turns and steps by shape code).
    fn read_steps(&self, s: u32) -> Result<SessionSteps> {
        let parts = self.parts(s)?;
        let mut ss = SessionSteps::default();
        let mut fs: Vec<(u32, FieldVal)> = Vec::new();
        let last = RefCell::new(None);
        for (seg, d) in &parts {
            // rule columns: stored values that are not today's are left out, and the rules
            // the segment was written without are worked out, so every step read carries
            // exactly what today's rules give it
            let sr = self.seg_rules(seg);
            let dropped = |f: u32| sr.drop.get(f as usize).copied().unwrap_or(false);
            let raw = self.step_block(seg, &d.steps.0)?;
            let (a, b) = (d.steps.1 as usize, (d.steps.1 + d.steps.2) as usize);
            // a part without values has nothing to read in a value block
            let vraw = if d.vals.2 == 0 { None } else { Some(self.step_block(seg, &d.vals.0)?) };
            let vals: &[u8] = match &vraw {
                None => &[],
                Some(v) => v.get(d.vals.1 as usize..(d.vals.1 + d.vals.2) as usize).ok_or_else(|| bad("step values past their block"))?,
            };
            let p = decode_steps(raw.get(a..b).ok_or_else(|| bad("step part past its block"))?, vals)?;
            if p.first as usize != ss.rows.len() {
                return corrupt(format!("session {s}: step parts do not follow each other"));
            }
            let (tools, fields) = &self.seg_codes[&seg.num];
            let gf = |f: u32| fields.get(f as usize).copied().ok_or_else(|| bad("step names a missing field"));
            let mut vmap = Vec::with_capacity(p.values.len());
            for (f, v) in &p.values {
                vmap.push(if dropped(*f) { u32::MAX } else { ss.intern_value(gf(*f)?, v) });
            }
            for (mut r, rf) in p.rows.into_iter().zip(p.fields) {
                r.tool = *tools.get(r.tool as usize).ok_or_else(|| bad("step names a missing tool"))?;
                fs.clear();
                for (f, v) in rf {
                    if dropped(f) {
                        continue;
                    }
                    let val = if steps::is_long(v) {
                        let (off, len) = p.longs[(v - steps::LONG_BASE) as usize];
                        FieldVal::Long { off, len }
                    } else {
                        FieldVal::Code(vmap[v as usize])
                    };
                    fs.push((gf(f)?, val));
                }
                if !sr.todo.is_empty() {
                    for (f, t) in self.rule_values(s, &ss, &r, &fs, &sr.todo, None, &last)? {
                        fs.push((f, FieldVal::Code(ss.intern_value(f, &t))));
                    }
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
        self.c.cores.remove(&self.skey(s));
        self.c.nodemaps.remove(&self.skey(s));
        self.c.stepc.remove(&self.skey(s));
        self.c.lightc.remove(&self.skey(s));
        self.c.fieldc.remove(&self.skey(s));
        self.c.msgtextc.remove(&self.skey(s));
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
        self.piece_id_hashed(kind, b, piece_hash(kind, b), m, pending)
    }

    /// [`Db::piece_id`] with the piece's hash worked out already.
    fn piece_id_hashed(&self, kind: u8, b: &[u8], h: u128, m: &mut Mutation, pending: &mut HashMap<u128, u32>) -> Result<u32> {
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

    /// The stored session with this key, if any.
    pub fn session_index(&self, session: &str, agent: &str) -> Result<Option<u32>> {
        let key = (session.to_string(), agent.to_string());
        match self.key_ix.get(&key) {
            Some(&s) => Ok(Some(s)),
            None => self.find_session_in_files(&key),
        }
    }

    /// Read into the shared caches what putting `p` will read from files: the index blocks
    /// that find its id and session, and the session's tables. Done on a snapshot by the
    /// thread preparing a request, so the writing thread finds them in memory.
    pub fn warm(&self, p: &Put) -> Result<()> {
        if self.find_request(p.id)?.is_some() {
            return Ok(());
        }
        if let Some(s) = self.session_index(p.session, p.agent)?
            && !self.active.contains_key(&s)
        {
            self.load_core(s)?;
            self.load_steps(s)?;
        }
        Ok(())
    }

    /// The stored request with this id, if any (unflushed ones included).
    pub fn find_request(&self, id: &str) -> Result<Option<ReqId>> {
        if let Some(r) = self.mem_request(id) {
            return Ok(Some(r));
        }
        self.find_request_in_files(id)
    }

    /// Add one request. It is visible to reads at once and durable after [`Db::commit`].
    pub fn put(&mut self, p: &Put) -> Result<PutOutcome> {
        if let Some(r) = self.find_request(p.id)? {
            return Ok(PutOutcome::Duplicate(r));
        }
        let pr = Prepared::new(&self.opts, p.body)?;
        self.put_new(p, pr)
    }

    /// [`Db::put`] for a body already split and hashed by [`Prepared::new`], maybe on
    /// another thread: what is left is quick and reads nothing from the body again.
    pub fn put_prepared(&mut self, p: &Put, pr: Prepared) -> Result<PutOutcome> {
        if let Some(r) = self.find_request(p.id)? {
            return Ok(PutOutcome::Duplicate(r));
        }
        self.put_new(p, pr)
    }

    fn put_new(&mut self, p: &Put, pr: Prepared) -> Result<PutOutcome> {
        let Prepared { sp, hashes, skeleton, system_hash, tools_hash, skeleton_hash } = pr;
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
                response: NONE,
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
            let h = hashes[i];
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
        if !p.response.is_empty() {
            // a leaf under the request's last message, out of the tree's walks
            m.nodes.push(NewNode { id: next, parent, hash: 0, bytes: p.response.to_vec(), holes: Vec::new() });
            m.req.response = next;
        }

        let mut notes = Vec::new();
        if let Some(sys) = &sp.system {
            m.req.system = self.piece_id_hashed(PIECE_SYSTEM, &sys.bytes, system_hash, &mut m, &mut pp)?;
            for n in &sys.notes {
                let t = self.piece_id(PIECE_MARKER, &n.text, &mut m, &mut pp)?;
                notes.push(ReqNote { target: TARGET_SYSTEM, offset: n.offset, text: t });
            }
        }
        if let Some(tools) = &sp.tools {
            m.req.tools = self.piece_id_hashed(PIECE_TOOLS, &tools.bytes, tools_hash, &mut m, &mut pp)?;
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
        m.req.skeleton = self.piece_id_hashed(PIECE_SKELETON, &skeleton, skeleton_hash, &mut m, &mut pp)?;

        let k = act.map(|a| a.core.requests.len() as u32).unwrap_or(0);
        self.wal.as_mut().ok_or_else(|| bad("a view takes no writes"))?.append(&m.encode());
        // logged: state that fails to take it no longer matches the log
        self.apply(m).map_err(|e| Error::Corrupt(format!("a logged request could not be applied: {e}")))?;
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
        // a record for a session deleted since (only replayed after a crash): its shared
        // pieces and values are kept, since ids follow each other, and nothing else
        let gone = self.manifest.is_dropped(s);
        if let Some((i, sk, ak)) = m.new_session {
            if i != self.next_session || i != s {
                return corrupt("new session out of order");
            }
            if !gone {
                let core = SessionCore { key: (sk, ak), ..SessionCore::default() };
                self.key_ix.insert(core.key.clone(), i);
                self.active.insert(i, Active::new(Arc::new(core), Arc::new(SessionSteps::default()), self.flushes));
            }
            self.next_session += 1;
        } else {
            if s >= self.next_session {
                return corrupt("record names a missing session");
            }
            if !gone {
                self.activate(s)?;
            }
        }
        // a flush at work holds the session's tables as they were frozen: the first write
        // after that copies them (timed: a long session's tables are large)
        if let Some(a) = self.active.get_mut(&s)
            && (Arc::strong_count(&a.core) > 1 || Arc::strong_count(&a.steps) > 1)
        {
            let t = std::time::Instant::now();
            Arc::make_mut(&mut a.core);
            Arc::make_mut(&mut a.steps);
            self.merge_stats.copy_ms_max = self.merge_stats.copy_ms_max.max(t.elapsed().as_millis() as u64);
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
            self.mem.opaques.push((o.enc, o.kind, o.hash, o.payload.into()));
            self.mem.opaque_ix.insert(o.hash, o.id);
        }
        if gone {
            self.seq = m.seq;
            return Ok(());
        }
        let time_ms = steps::parse_time_ms(&m.req.time);
        let response = m.req.response;
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
            if id != response {
                a.children.insert((n.parent, n.hash), n.id);
            }
            core.nodes.push(NodeMeta {
                parent: n.parent,
                len: n.bytes.len() as u32,
                loc: Loc::Mem(self.mem.node_base + self.mem.nodes.len() as u32),
                holes: if n.holes.is_empty() { None } else { Some(n.holes.into_boxed_slice()) },
            });
            self.mem.bytes += n.bytes.len() + 64;
            self.mem.nodes.push(n.bytes.into());
            if id == response {
                // no steps: it holds no results, and its calls are the next request's
                let ss = Arc::make_mut(&mut self.active.get_mut(&s).unwrap().steps);
                if ss.tail.len() != id as usize {
                    return corrupt("step tails out of step with nodes");
                }
                let t = if n.parent == NONE { NONE } else { ss.tail[n.parent as usize] };
                ss.tail.push(t);
            } else {
                self.add_node_steps(s, id, time_ms)?;
            }
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
            OLoc::Mem(id) => self.mem_opaque(id).map_or(0, |o| o.3.len() as u64),
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
        let ex = steps::extract(self.mem_node(mi));
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
        {
            let a = self.active.get_mut(&si).unwrap();
            let ss = Arc::make_mut(&mut a.steps);
            for (tid, block, name, fields) in ex.calls {
                let ci = self.sd.call_info(ss, id, block, &name, &fields);
                ss.calls.insert(tid.into(), ci);
            }
            if ss.tail.len() != id as usize {
                return corrupt("step tails out of step with nodes");
            }
        }
        // the new steps, each with the values today's rules give it (an input field a rule
        // hides is left out)
        let mut rows: Vec<(StepRow, Vec<(u32, FieldVal)>, Vec<(u32, Vec<u8>)>)> = Vec::with_capacity(found.len());
        {
            let ss = &self.active[&si].steps;
            let all: Vec<usize> = (0..self.rules.rules.len()).collect();
            let last = RefCell::new(None);
            for (call, block, failed, out_bytes) in found {
                let prev = ss.tail[call.node as usize];
                let row = StepRow { call_node: call.node, result_node: id, prev, tool: call.tool, time_ms, out_bytes, call_block: call.block, result_block: block, failed };
                let mut fields = call.fields;
                let mut extra = Vec::new();
                if !all.is_empty() {
                    fields.retain(|x| !self.rule_fields.contains(&x.0));
                    extra = self.rule_values(si, ss, &row, &fields, &all, Some(self.mem_node(mi)), &last)?;
                }
                rows.push((row, fields, extra));
            }
        }
        let a = self.active.get_mut(&si).unwrap();
        let ss = Arc::make_mut(&mut a.steps);
        let mut tail = if parent == NONE { NONE } else { ss.tail[parent as usize] };
        for (row, mut fields, extra) in rows {
            for (f, t) in extra {
                fields.push((f, FieldVal::Code(ss.intern_value(f, &t))));
            }
            tail = ss.push(row, &fields);
        }
        ss.tail.push(tail);
        Ok(())
    }

    /// Make every `put` so far durable. After this returns they count as saved.
    pub fn commit(&mut self) -> Result<()> {
        self.wal.as_mut().ok_or_else(|| bad("a view takes no writes"))?.commit()?;
        if !self.opts.background_flush {
            if self.pending_bytes() >= self.opts.flush_bytes {
                self.flush()?;
            }
            return Ok(());
        }
        // a flush done on its own thread is put in place at the first commit after it
        if let Some(b) = self.flush_done(false)? {
            self.flush_install(b)?;
        }
        if self.pending_bytes() >= self.opts.flush_bytes {
            if self.fbg.is_some() && self.pending_bytes() >= 2 * self.opts.flush_bytes {
                // memory holds two buffers' worth: wait for the flush at work
                let t = std::time::Instant::now();
                if let Some(b) = self.flush_done(true)? {
                    self.flush_install(b)?;
                }
                let ms = t.elapsed().as_millis() as u64;
                self.merge_stats.flush_waits += 1;
                self.merge_stats.flush_wait_ms_max = self.merge_stats.flush_wait_ms_max.max(ms);
                self.merge_stats.flush_wait_ms_total += ms;
            }
            if self.fbg.is_none() {
                self.flush_start()?;
            }
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
    /// to a blob file) and trim the log. A flush already at work on its own thread is
    /// waited for and put in place first; when this returns, nothing is left in memory.
    pub fn flush(&mut self) -> Result<()> {
        self.wal.as_mut().ok_or_else(|| bad("a view takes no writes"))?.commit()?;
        if let Some(b) = self.flush_done(true)? {
            self.flush_install(b)?;
        }
        if self.mem.is_empty() && self.sealed.is_empty() {
            // nothing to write; a merge that finished on its own thread is still put in place
            return if self.bg.is_some() { self.merge_policy() } else { Ok(()) };
        }
        let job = self.freeze()?;
        let b = self.flush_view(&job)?.flush_build(job)?;
        self.flush_install(b)
    }

    /// Start flushing what is in memory on a thread of its own: the buffer is frozen (reads
    /// still see it) and a new one takes the writes that follow, so they never wait for the
    /// files to be written. Only one flush is at work at a time.
    fn flush_start(&mut self) -> Result<()> {
        let job = self.freeze()?;
        let view = self.flush_view(&job)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::Builder::new().name("tdb-flush".into()).spawn(move || {
            let _ = tx.send(view.flush_build(job));
        })?;
        self.fbg = Some(BgFlush { rx: Mutex::new(rx), handle: Some(handle), started: std::time::Instant::now() });
        Ok(())
    }

    /// The flush at work on its own thread, once it is done (`wait`: block until it is).
    fn flush_done(&mut self, wait: bool) -> Result<Option<FlushBuild>> {
        let Some(bg) = self.fbg.as_mut() else { return Ok(None) };
        let r = if wait {
            bg.rx.lock().unwrap().recv().unwrap_or_else(|_| Err(bad("the flush thread stopped without an answer")))
        } else {
            let got = bg.rx.lock().unwrap().try_recv();
            match got {
                Ok(r) => r,
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(None),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(bad("the flush thread stopped without an answer")),
            }
        };
        if let Some(mut bg) = self.fbg.take() {
            self.merge_stats.bg_flushes += 1;
            self.merge_stats.flush_ms_max = self.merge_stats.flush_ms_max.max(bg.started.elapsed().as_millis() as u64);
            if let Some(h) = bg.handle.take() {
                let _ = h.join();
            }
        }
        r.map(Some)
    }

    /// Hand the write buffer to a flush: freeze it, start a new log file and a new buffer for
    /// the writes that follow, and note what the flush must write.
    fn freeze(&mut self) -> Result<FlushJob> {
        if self.frozen.is_some() {
            return corrupt("a flush started while another is at work");
        }
        let w = self.wal.as_mut().ok_or_else(|| bad("a view takes no writes"))?;
        w.commit()?;
        let wal_start = w.num + 1;
        self.wal = Some(WalWriter::create(&self.dir, wal_start, self.opts.sync)?);
        let next = self.mem.next();
        let cur = std::mem::replace(&mut self.mem, next);
        let frozen = Arc::new(if self.sealed.is_empty() {
            cur
        } else {
            let mut all: Vec<&Mem> = self.sealed.iter().map(|m| &**m).collect();
            all.push(&cur);
            let j = Mem::join(&all)?;
            self.sealed.clear();
            j
        });
        // (a session deleted since it took writes is not written)
        let sessions: Vec<(u32, Arc<SessionCore>, Arc<SessionSteps>)> = frozen.touched.iter().filter_map(|&s| self.active.get(&s).map(|a| (s, a.core.clone(), a.steps.clone()))).collect();
        // file numbers for its segment, index run and maybe a new blob file
        let first_file = self.manifest.next_file;
        self.manifest.next_file += 3;
        self.frozen = Some(frozen.clone());
        Ok(FlushJob { frozen, sessions, sessions_hi: self.next_session, checkpoint: self.seq, wal_start, first_file, piece_bases: std::mem::take(&mut self.piece_bases) })
    }

    /// A read-only handle for a flush to read from: the files as they are, the frozen
    /// buffer, and the sessions it holds as they were when it was frozen.
    fn flush_view(&self, job: &FlushJob) -> Result<Db> {
        let mut v = Db::blank(self.dir.clone(), None, self.opts.clone(), self.manifest.clone(), None, (self.opts.cache_bytes / 8).max(8 << 20));
        v.segs = self.segs.clone();
        v.seg_codes = self.seg_codes.clone();
        v.rules = self.rules.clone();
        v.rule_fields = self.rule_fields.clone();
        v.runs = self.runs.clone();
        v.blob_files = self.blob_files.iter().map(|(n, b)| (*n, b.share())).collect();
        v.next_session = job.sessions_hi;
        v.seq = job.checkpoint;
        v.sd = self.sd.clone();
        v.frozen = Some(job.frozen.clone());
        v.mem = Mem { node_base: u32::MAX, piece_base: u32::MAX, opaque_base: u64::MAX, ..Mem::default() };
        for (s, core, steps) in &job.sessions {
            v.active.insert(*s, Active { core: core.clone(), steps: steps.clone(), children: HashMap::new(), stored_kids: Vec::new(), keyed: Default::default(), last: 0 });
        }
        Ok(v)
    }

    /// Write a frozen buffer's files (on a view: nothing of the store changes until
    /// [`Db::flush_install`]).
    fn flush_build(&self, job: FlushJob) -> Result<FlushBuild> {
        let mut piece_bases = job.piece_bases.clone();
        let src: &Mem = &job.frozen;
        let mut next_file = job.first_file;
        let mut take_file = || {
            let n = next_file;
            next_file += 1;
            n
        };
        let mut blobs = self.manifest.blobs.clone();
        let mut new_blob = false;
        let mut flushed = 0u64;

        // values: appended to the last blob file, or a new one once that is full
        let lo = src.opaque_base;
        let n = src.opaques.len();
        let mut pents = Vec::with_capacity(src.pieces.len());
        let mut oents: Vec<OpaqueEnt> = src.opaques.iter().map(|(enc, kind, h, p)| OpaqueEnt { enc: *enc, kind: *kind, hash: *h, file: 0, off: 0, len: p.len() as u32 }).collect();
        let mut blob_len = None;
        if !src.pieces.is_empty() || !src.opaques.is_empty() {
            let (bn, len) = match blobs.last() {
                Some(&(bn, len)) if len < self.opts.blob_bytes => (bn, len),
                _ => {
                    let bn = take_file();
                    blobs.push((bn, 0));
                    new_blob = true;
                    (bn, 0)
                }
            };
            let mut bw = BlobAppend::open(&self.dir, bn, len)?;
            let start = bw.pos();
            // system prompts and tool lists change a little at a time: each new piece is
            // compressed against a recent whole piece of its kind when that at least halves it
            let first = src.piece_base;
            let mut bases = std::mem::take(&mut piece_bases);
            for (i, (kind, h, raw)) in src.pieces.iter().enumerate() {
                let ids = bases.entry(*kind).or_default();
                let mut cand: Vec<(u32, Vec<u8>)> = Vec::with_capacity(ids.len());
                for &(id, depth) in ids.iter() {
                    if depth < PIECE_CHAIN {
                        cand.push((id, if id >= first { src.pieces[(id - first) as usize].2.to_vec() } else { self.piece(id)?.to_vec() }));
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
            piece_bases = bases;
            // values in the order requests read them: by session, then by message
            let mut order = Vec::with_capacity(n);
            let mut placed = vec![false; n];
            for &s in &src.touched {
                let Some(a) = self.active.get(&s) else { continue };
                let core = &a.core;
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
                let off = bw.opaque(&src.opaques[i].3)?;
                oents[i] = OpaqueEnt { file: bn, off, ..oents[i] };
            }
            let new_len = bw.finish(self.opts.sync)?;
            flushed += new_len - start;
            blobs.last_mut().unwrap().1 = new_len;
            blob_len = Some((bn, new_len));
        }

        // index: what this flush added, findable by hash; values by id
        let rn = take_file();
        let mut keys: Vec<(u8, u128, u64)> = Vec::with_capacity(pents.len() + n + src.req_ix.len());
        keys.extend(pents.iter().enumerate().map(|(i, p)| (NS_PIECE, p.hash, src.piece_base as u64 + i as u64)));
        keys.extend(oents.iter().enumerate().map(|(i, o)| (NS_OPAQUE, o.hash, lo + i as u64)));
        keys.extend(src.req_ix.iter().map(|(id, &r)| (NS_REQUEST, request_hash(id), r)));
        for s in self.manifest.sessions..job.sessions_hi {
            match self.active.get(&s) {
                Some(a) => keys.push((NS_SESSION, session_hash(&a.core.key), s as u64)),
                // deleted before its first flush
                None if self.manifest.is_dropped(s) => {}
                None => return corrupt(format!("new session {s} is not in memory at a flush")),
            }
        }
        keys.sort_unstable();
        keys.dedup();
        let mut rw = RunWriter::create(&self.dir, rn, src.piece_base, lo)?;
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

        // messages, requests and steps of every session that took writes
        let num = take_file();
        let mut w = SegWriter::create(&self.dir, num, self.opts.frame_size, Codec::plain(self.opts.level), 0, job.checkpoint)?;
        w.set_rules(self.rules.versions());
        let mut written: Vec<FlushedPart> = Vec::new();
        for &s in &src.touched {
            let Some(a) = self.active.get(&s) else { continue };
            let (core, ss) = (&a.core, &a.steps);
            let (mut cp, stream) = self.core_part(core, core.flushed_nodes..core.nodes.len() as u32, core.flushed_reqs..core.requests.len() as u32)?;
            cp.frames = w.frames(&stream)?;
            let sp = self.step_part(&mut w, s, ss, ss.flushed..ss.rows.len() as u32)?;
            let units = crate::textindex::part_units(&cp, &stream);
            w.add(s, core.key.clone(), &cp, &sp, units)?;
            written.push(FlushedPart { session: s, part: cp, nodes: core.nodes.len() as u32, requests: core.requests.len() as u32, rows: ss.rows.len() as u32 });
        }
        w.finish(self.opts.sync)?;
        Ok(FlushBuild {
            seg: num,
            run: rn,
            blob: blob_len,
            new_blob,
            blobs,
            written,
            piece_bases,
            flushed,
            sessions_hi: job.sessions_hi,
            checkpoint: job.checkpoint,
            wal_start: job.wal_start,
            pieces: src.pieces.len() as u32,
            opaques: src.opaques.len() as u64,
        })
    }

    /// Put a flush's files in place: the manifest first, then this handle's state.
    fn flush_install(&mut self, b: FlushBuild) -> Result<()> {
        let t = std::time::Instant::now();
        let r = self.flush_install_timed(b);
        self.merge_stats.install_ms_max = self.merge_stats.install_ms_max.max(t.elapsed().as_millis() as u64);
        r
    }

    fn flush_install_timed(&mut self, b: FlushBuild) -> Result<()> {
        let mut man = self.manifest.clone();
        man.blobs = b.blobs;
        man.runs.push(b.run);
        man.segs.push(b.seg);
        man.generation += 1;
        man.checkpoint = b.checkpoint;
        man.wal_start = b.wal_start;
        man.sessions = b.sessions_hi;
        man.pieces += b.pieces;
        man.opaques += b.opaques;
        // a flush that was at work when sessions were deleted wrote their parts
        let seg = Segment::open(&self.dir, b.seg)?;
        if let Some(x) = self.dropped_dead(&seg)? {
            let mut dead: BTreeMap<u32, Dead> = man.dead.iter().copied().collect();
            dead.entry(b.seg).or_default().add(&x);
            man.dead = dead.into_iter().collect();
        }
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        let _ = b.new_blob;

        self.merge_stats.flushed += b.flushed + seg.file_bytes;
        self.add_segment(seg);
        self.runs.push(Arc::new(Run::open(&self.dir, b.run)?));
        if let Some((bn, len)) = b.blob {
            self.blob_files.entry(bn).or_insert_with(|| BlobFile::new(blob_path(&self.dir, bn), 0)).len = len;
        }
        for w in b.written {
            let Some(a) = self.active.get_mut(&w.session) else {
                // deleted while the flush was at work
                if self.manifest.is_dropped(w.session) {
                    continue;
                }
                return corrupt(format!("session {} left memory before its flush was put in place", w.session));
            };
            let core = Arc::make_mut(&mut a.core);
            let chunk = core.chunks.len() as u32;
            core.chunks.push(Chunk { seg: b.seg, frames: w.part.frames.into(), raw_len: w.part.raw_len, fs: w.part.frame });
            for n in &w.part.nodes {
                core.nodes[n.id as usize].loc = Loc::Seg { chunk, off: n.off };
            }
            core.flushed_nodes = w.nodes;
            core.flushed_reqs = w.requests;
            Arc::make_mut(&mut a.steps).flushed = w.rows;
        }
        self.piece_bases = b.piece_bases;
        self.frozen = None;
        self.flushes += 1;
        // sessions that stopped taking writes leave memory once all of them is in files
        let idle: Vec<u32> = self.active.iter().filter(|(_, a)| a.last + self.opts.idle_flushes <= self.flushes && a.flushed()).map(|(&s, _)| s).collect();
        for s in idle {
            if let Some(a) = self.active.remove(&s) {
                self.key_ix.remove(&a.core.key);
            }
        }
        self.remove_old_wal(b.wal_start)?;
        self.drop_retired();
        self.busy = self.active.keys().copied().collect();
        self.merge_policy()
    }

    /// Once `merge_at` flushed segments wait, merge them; then merge any `merge_at` merged
    /// segments of one size tier (sizes in powers of 4 from 1 MB) under the size cap. Each
    /// byte is rewritten about once per tier, and finished data stops moving at the cap.
    /// A segment whose parts were mostly replaced by folds is rewritten on its own.
    /// Index runs merge the same way, `merge_at` neighbours of one tier at a time.
    fn merge_policy(&mut self) -> Result<()> {
        // files are replaced only while no flush is at work: a flush reads the files it saw
        if self.frozen.is_some() {
            return Ok(());
        }
        // a merge at work on its own thread: put it in place if it is done; until then
        // nothing else replaces files, since it reads them
        if self.bg.is_some() {
            match self.merge_done(false)? {
                Some(b) => self.merge_install(b)?,
                None => return Ok(()),
            }
        }
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
            let t = std::time::Instant::now();
            self.merge_runs(i, w)?;
            self.merge_stats.run_merge_ms_max = self.merge_stats.run_merge_ms_max.max(t.elapsed().as_millis() as u64);
        }
        loop {
            // the next merge of segments, if one is due: (inputs, fold, a rewrite to drop replaced parts)
            let l0: BTreeSet<u32> = self.segs.values().filter(|s| s.cat.level == 0).map(|s| s.num).collect();
            let mut tiers: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for sg in self.segs.values().filter(|s| s.cat.level > 0 && s.file_bytes < self.opts.seg_bytes) {
                tiers.entry(tier(sg.file_bytes, 20)).or_default().push(sg.num);
            }
            let next: Option<(BTreeSet<u32>, bool, bool)> = if l0.len() >= self.opts.merge_at {
                Some((l0, true, false))
            } else if let Some((_, group)) = tiers.into_iter().find(|(_, v)| v.len() >= self.opts.merge_at) {
                Some((group.into_iter().take(self.opts.merge_at).collect(), true, false))
            } else {
                // half or more of the segment is parts that folds replaced: rewrite it without them
                let gc = self.manifest.dead.iter().find(|(n, d)| self.segs.get(n).is_some_and(|sg| sg.cat.level > 0 && d.bytes.saturating_mul(2) >= sg.file_bytes)).map(|x| x.0);
                gc.map(|n| (BTreeSet::from([n]), false, true))
            };
            let Some((inputs, fold, reclaim)) = next else { break };
            if self.opts.background_merges {
                // one at a time; the next is looked for when this one is in place
                return self.merge_start(inputs, fold, reclaim);
            }
            self.merge(inputs, fold, false)?;
            self.merge_stats.reclaims += reclaim as u64;
        }
        Ok(())
    }

    /// Start a merge on a thread of its own, reading a view of the files as they are now.
    /// Its outputs take file numbers set aside here (and never handed out again).
    fn merge_start(&mut self, inputs: BTreeSet<u32>, fold: bool, reclaim: bool) -> Result<()> {
        let first = self.manifest.next_file;
        let mut man = self.manifest.clone();
        man.next_file += MERGE_FILES;
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        let view = self.view()?;
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();
        let stop = cancel.clone();
        let handle = std::thread::Builder::new().name("tdb-merge".into()).spawn(move || {
            crate::segment::set_low_priority();
            let r = view.merge_build(inputs, fold, false, first, MERGE_FILES, Some(&stop)).map(|mut b| {
                b.reclaim = reclaim;
                b
            });
            let _ = tx.send(r);
        })?;
        self.bg = Some(BgMerge { rx: Mutex::new(rx), cancel, handle: Some(handle) });
        Ok(())
    }

    /// The merge at work on its own thread, once it is done (`wait`: block until it is).
    fn merge_done(&mut self, wait: bool) -> Result<Option<MergeBuild>> {
        let Some(bg) = self.bg.as_mut() else { return Ok(None) };
        let r = if wait {
            bg.rx.lock().unwrap().recv().unwrap_or_else(|_| Err(bad("the merge thread stopped without an answer")))
        } else {
            let got = bg.rx.lock().unwrap().try_recv();
            match got {
                Ok(r) => r,
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(None),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(bad("the merge thread stopped without an answer")),
            }
        };
        if let Some(h) = self.bg.take().and_then(|mut bg| bg.handle.take()) {
            let _ = h.join();
        }
        r.map(Some)
    }

    /// Put in place a flush or merge that finished on its own thread, without waiting (and
    /// start the next merge if one is due): what a write would do, for a store taking none.
    /// Whether the store's files changed.
    pub fn maintain(&mut self) -> Result<bool> {
        let before = self.manifest.generation;
        if let Some(b) = self.flush_done(false)? {
            self.flush_install(b)?;
        } else if self.bg.is_some() {
            self.merge_policy()?;
        }
        Ok(self.manifest.generation != before)
    }

    // ------------------------------------------------------------------ deletes

    /// Every session with bytes in a segment, live, replaced or deleted (a deleted session
    /// is in this until merges or a vacuum rewrite its segments).
    pub fn sessions_in_files(&self) -> Result<BTreeSet<u32>> {
        let mut out = BTreeSet::new();
        for seg in self.segs.values() {
            for k in 0..seg.cat.dirs.len() {
                out.extend(self.dir_block(seg, k)?.iter().map(|d| d.session));
            }
        }
        Ok(out)
    }

    /// What a segment holds of deleted sessions (written by a flush or merge that was at
    /// work when they were deleted), to count as dead: queries then read its live mask.
    fn dropped_dead(&self, seg: &Segment) -> Result<Option<Dead>> {
        if self.manifest.dropped.is_empty() || !self.manifest.dropped.iter().any(|r| r.0 <= seg.cat.hi && seg.cat.lo < r.1) {
            return Ok(None);
        }
        let mut t = Dead::default();
        let mut any = false;
        for k in 0..seg.cat.dirs.len() {
            for d in seg.dir_block(k)?.iter().filter(|d| self.manifest.is_dropped(d.session)) {
                t.add(&part_dead(d, &SessionCore::default()));
                any = true;
            }
        }
        Ok(any.then_some(t))
    }

    /// Whether session `s` was deleted.
    pub fn is_dropped(&self, s: u32) -> bool {
        self.manifest.is_dropped(s)
    }

    /// How many sessions were deleted.
    pub fn dropped_count(&self) -> u64 {
        self.manifest.dropped_count()
    }

    /// Delete sessions. From when this returns nothing of them is read: their requests are
    /// not found, queries skip their steps and text, and a new request with a deleted
    /// session's key starts a new session. Durable at once (the manifest records it).
    ///
    /// Their bytes stay in their segments, counted as dead there, until merges rewrite those
    /// segments (once half of one is dead) or [`Db::vacuum`] removes every trace.
    pub fn drop_sessions(&mut self, sessions: &[u32]) -> Result<DropReport> {
        if self.wal.is_none() {
            return Err(bad("a view takes no writes"));
        }
        let mut man = self.manifest.clone();
        let mut dead: BTreeMap<u32, Dead> = man.dead.iter().copied().collect();
        let mut rep = DropReport::default();
        for &s in sessions {
            if s >= self.next_session || man.is_dropped(s) {
                continue;
            }
            let core = self.load_core(s)?;
            for (seg, d) in self.parts(s)? {
                let x = part_dead(&d, &core);
                rep.dead_bytes += x.bytes;
                dead.entry(seg.num).or_default().add(&x);
            }
            rep.sessions += 1;
            rep.requests += core.requests.len() as u64;
            man.drop_session(s);
        }
        if rep.sessions == 0 {
            return Ok(rep);
        }
        man.dead = dead.into_iter().filter(|(n, _)| self.segs.contains_key(n)).collect();
        man.generation += 1;
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        // out of memory; what they had unflushed stays in the buffer and is never written
        for &s in sessions {
            if self.manifest.is_dropped(s)
                && let Some(a) = self.active.remove(&s)
            {
                self.key_ix.remove(&a.core.key);
            }
        }
        let man = &self.manifest;
        self.busy.retain(|s| !man.is_dropped(*s));
        Ok(rep)
    }

    /// Delete the session with this key, if there is one.
    pub fn drop_session_key(&mut self, session: &str, agent: &str) -> Result<DropReport> {
        match self.session_index(session, agent)? {
            Some(s) => self.drop_sessions(&[s]),
            None => Ok(DropReport::default()),
        }
    }

    /// Delete every session whose newest request is older than `cutoff_ms` (ms since the
    /// epoch, by the requests' own times; a request whose time cannot be read counts as
    /// new). A session with any newer request keeps all of its requests until they are all
    /// older: a session's requests share their messages, so it goes as a whole.
    pub fn drop_before(&mut self, cutoff_ms: i64) -> Result<DropReport> {
        // the newest a session's requests can be, by the segments holding its parts; and
        // the sessions with a part where something is older than the cutoff
        let mut newest: HashMap<u32, i64> = HashMap::new();
        let mut cand: BTreeSet<u32> = BTreeSet::new();
        for seg in self.segs.values() {
            let old = seg.cat.time_lo < cutoff_ms;
            for k in 0..seg.cat.dirs.len() {
                for d in self.dir_block(seg, k)?.iter() {
                    let e = newest.entry(d.session).or_insert(i64::MIN);
                    *e = (*e).max(seg.cat.time_hi);
                    if old {
                        cand.insert(d.session);
                    }
                }
            }
        }
        cand.extend(self.active.keys().copied());
        let mut gone = Vec::new();
        for s in cand {
            if self.manifest.is_dropped(s) {
                continue;
            }
            // all of it in segments with nothing as new as the cutoff: no need to read it
            if !self.active.contains_key(&s) && newest.get(&s).is_some_and(|&t| t > 0 && t < cutoff_ms) {
                gone.push(s);
                continue;
            }
            let core = self.load_core(s)?;
            let times: Vec<i64> = core.requests.iter().map(|r| steps::parse_time_ms(&r.time)).collect();
            if !times.is_empty() && times.iter().all(|&t| t > 0 && t < cutoff_ms) {
                gone.push(s);
            }
        }
        self.drop_sessions(&gone)
    }

    /// Remove every trace of deleted sessions from the files. Segments are rewritten with
    /// live sessions only (as [`Db::compact`] does), blob files with only the shared pieces
    /// and opaque values live requests use, and the index from the live data alone; the old
    /// files are deleted once no snapshot reads them. Until a vacuum, deleted sessions are
    /// never read, but their bytes (and pieces nothing live uses any more, such as their
    /// system prompts) stay on disk.
    ///
    /// Reads and rewrites the whole store, on this thread.
    pub fn vacuum(&mut self) -> Result<VacuumReport> {
        let t0 = std::time::Instant::now();
        if self.wal.is_none() {
            return Err(bad("a view takes no writes"));
        }
        let mut rep = VacuumReport::default();
        self.settle()?;
        rep.segments_before = self.segs.len();
        rep.segment_bytes_before = self.segs.values().map(|s| s.file_bytes).sum();
        rep.blob_bytes_before = self.blob_files.values().map(|b| b.len).sum();
        let all: BTreeSet<u32> = self.segs.keys().copied().collect();
        if !all.is_empty() {
            self.merge(all, false, true)?;
        }
        self.drop_retired();

        // what live requests use, and every live key
        let (np, no) = (self.manifest.pieces as usize, self.manifest.opaques as usize);
        let mut live_p = vec![false; np];
        let mut live_o = vec![false; no];
        let mut keys: Vec<(u8, u128, u64)> = Vec::new();
        for s in 0..self.next_session {
            if self.manifest.is_dropped(s) {
                continue;
            }
            let core = self.load_core(s)?;
            keys.push((NS_SESSION, session_hash(&core.key), s as u64));
            for (k, r) in core.requests.iter().enumerate() {
                keys.push((NS_REQUEST, request_hash(&r.id), req_id(s, k as u32)));
                for id in [r.skeleton, r.system, r.tools].into_iter().chain(r.notes.iter().map(|n| n.text)) {
                    if id != NONE {
                        *live_p.get_mut(id as usize).ok_or_else(|| bad(format!("session {s} uses missing piece {id}")))? = true;
                    }
                }
            }
            for n in &core.nodes {
                for &(_, o) in n.holes.iter().flat_map(|h| h.iter()) {
                    *live_o.get_mut(o as usize).ok_or_else(|| bad(format!("session {s} uses missing opaque value {o}")))? = true;
                }
            }
        }

        // blob files with the live pieces and values, in id order; ids stay what they were
        let mut man = self.manifest.clone();
        let mut next_file = man.next_file;
        let mut blobs: Vec<(u32, u64)> = Vec::new();
        let mut cur: Option<(u32, BlobAppend)> = None;
        let (mut pents, mut oents) = (Vec::with_capacity(np), Vec::with_capacity(no));
        let mut bases: HashMap<u8, Vec<(u32, u8)>> = HashMap::new();
        let roll = |cur: &mut Option<(u32, BlobAppend)>, blobs: &mut Vec<(u32, u64)>, next_file: &mut u32| -> Result<()> {
            if cur.as_ref().is_some_and(|(_, w)| w.pos() < self.opts.blob_bytes) {
                return Ok(());
            }
            if let Some((bn, w)) = cur.take() {
                blobs.push((bn, w.finish(self.opts.sync)?));
            }
            let bn = *next_file;
            *next_file += 1;
            *cur = Some((bn, BlobAppend::open(&self.dir, bn, 0)?));
            Ok(())
        };
        for r in self.runs.clone() {
            for i in 0..(r.cat.np as usize).div_ceil(IDS) {
                let IxBlock::Pieces(v) = &*self.ixb(&r, K_PIECES, i)? else { return corrupt("index block of the wrong kind") };
                for (j, e) in v.iter().enumerate() {
                    let id = r.cat.plo + (i * IDS + j) as u32;
                    if e.file == GONE || !live_p[id as usize] {
                        rep.pieces_removed += (e.file != GONE) as u64;
                        pents.push(PieceEnt { kind: 0, hash: 0, file: GONE, off: 0, clen: 0, rawlen: 0, base: 0 });
                        continue;
                    }
                    let raw = self.piece(id)?;
                    // compressed against a recent live piece of its kind when that pays, as flushes do
                    let ids = bases.entry(e.kind).or_default();
                    let cand: Vec<(u32, Arc<Vec<u8>>)> = ids.iter().filter(|x| x.1 < PIECE_CHAIN).map(|&(b, _)| self.piece(b).map(|p| (b, p))).collect::<Result<_>>()?;
                    let refs: Vec<(u32, &[u8])> = cand.iter().map(|(b, p)| (*b, &p[..])).collect();
                    let (base, c) = blob::pack_piece(&raw, &refs, self.opts.merge_level)?;
                    roll(&mut cur, &mut blobs, &mut next_file)?;
                    let (bn, w) = cur.as_mut().unwrap();
                    let (off, clen) = w.packed(&c)?;
                    pents.push(PieceEnt { kind: e.kind, hash: e.hash, file: *bn, off, clen, rawlen: raw.len() as u32, base });
                    let depth = if base == 0 { 0 } else { ids.iter().find(|x| x.0 == base - 1).map_or(PIECE_CHAIN, |x| x.1 + 1) };
                    ids.push((id, depth));
                    if ids.len() > PIECE_BASES {
                        ids.remove(0);
                    }
                }
            }
        }
        for r in self.runs.clone() {
            for i in 0..(r.cat.no as usize).div_ceil(IDS) {
                let IxBlock::Opaques(v) = &*self.ixb(&r, K_OPAQUES, i)? else { return corrupt("index block of the wrong kind") };
                for (j, o) in v.iter().enumerate() {
                    let id = r.cat.olo + (i * IDS + j) as u64;
                    if o.file == GONE || !live_o[id as usize] {
                        rep.opaques_removed += (o.file != GONE) as u64;
                        oents.push(OpaqueEnt { enc: 0, kind: 0, hash: 0, file: GONE, off: 0, len: 0 });
                        continue;
                    }
                    let p = self.blob_read(o.file, o.off, o.len)?;
                    roll(&mut cur, &mut blobs, &mut next_file)?;
                    let (bn, w) = cur.as_mut().unwrap();
                    let off = w.opaque(&p)?;
                    oents.push(OpaqueEnt { file: *bn, off, ..*o });
                }
            }
        }
        if let Some((bn, w)) = cur.take() {
            blobs.push((bn, w.finish(self.opts.sync)?));
        }
        if pents.len() != np || oents.len() != no {
            return corrupt("index runs do not cover every piece and value");
        }

        // one index run of live keys, with the new tables
        for (i, e) in pents.iter().enumerate().filter(|x| x.1.file != GONE) {
            keys.push((NS_PIECE, e.hash, i as u64));
        }
        for (i, e) in oents.iter().enumerate().filter(|x| x.1.file != GONE) {
            keys.push((NS_OPAQUE, e.hash, i as u64));
        }
        keys.sort_unstable();
        keys.dedup();
        let rn = next_file;
        next_file += 1;
        let mut rw = RunWriter::create(&self.dir, rn, 0, 0)?;
        for (ns, h, v) in keys {
            rw.key(ns, h, v)?;
        }
        for e in &pents {
            rw.piece(e)?;
        }
        for e in &oents {
            rw.opaque(e)?;
        }
        rw.finish(self.opts.sync)?;

        man.next_file = next_file;
        man.runs = vec![rn];
        man.blobs = blobs;
        man.dead = Vec::new();
        man.generation += 1;
        man.store(&self.dir, self.opts.sync)?;
        let run = Arc::new(Run::open(&self.dir, rn)?);
        for r in std::mem::replace(&mut self.runs, vec![run]) {
            r.doom();
        }
        for (_, b) in std::mem::take(&mut self.blob_files) {
            b.keep.doom.set();
        }
        for &(bn, len) in &man.blobs {
            self.blob_files.insert(bn, BlobFile::new(blob_path(&self.dir, bn), len));
        }
        self.manifest = man;
        // new pieces are compressed against these from now on (ids that are gone are not)
        self.piece_bases = bases;
        self.check_run_ranges()?;
        rep.segments_after = self.segs.len();
        rep.segment_bytes_after = self.segs.values().map(|s| s.file_bytes).sum();
        rep.blob_bytes_after = self.blob_files.values().map(|b| b.len).sum();
        rep.ms = t0.elapsed().as_millis() as u64;
        Ok(rep)
    }

    /// Whether a flush is at work on its own thread.
    pub fn flushing(&self) -> bool {
        self.fbg.is_some()
    }

    /// Whether a merge is at work on its own thread.
    pub fn merging(&self) -> bool {
        self.bg.is_some()
    }

    /// Flush, then wait until no merge is at work and none is due.
    pub fn settle(&mut self) -> Result<()> {
        self.flush()?;
        while let Some(b) = self.merge_done(true)? {
            self.merge_install(b)?;
            self.merge_policy()?;
        }
        self.drop_retired();
        self.removed();
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
            r.doom();
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
        let first = self.manifest.next_file;
        let b = self.merge_build(inputs, fold, last, first, u32::MAX - first, None)?;
        self.manifest.next_file = first + b.outputs.len() as u32;
        self.merge_install(b)
    }

    /// The reading and writing of a merge (see [`Db::merge`]): the outputs are written as
    /// files numbered from `first` (at most `max_files` of them) and nothing else changes,
    /// so it can run on a view of the store while the store takes writes. Stops, removing
    /// what it wrote, when `cancel` is set.
    fn merge_build(&self, inputs: BTreeSet<u32>, fold: bool, last: bool, first: u32, max_files: u32, cancel: Option<&std::sync::atomic::AtomicBool>) -> Result<MergeBuild> {
        let mut outputs: Vec<u32> = Vec::new();
        let r = self.merge_write(&inputs, fold, last, first, max_files, cancel, &mut outputs);
        match r {
            Ok((dead, bytes_in, fold_read, folded)) => Ok(MergeBuild { inputs, outputs, dead, bytes_in, fold_read, folded, reclaim: false }),
            Err(e) => {
                for n in outputs {
                    let p = seg_path(&self.dir, n);
                    crate::block::forget(&p);
                    let _ = fs::remove_file(p);
                }
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_write(&self, inputs: &BTreeSet<u32>, fold: bool, last: bool, first: u32, max_files: u32, cancel: Option<&std::sync::atomic::AtomicBool>, outputs: &mut Vec<u32>) -> Result<(BTreeMap<u32, Dead>, u64, u64, u64)> {
        let mut sessions = BTreeSet::new();
        let mut bytes_in = 0;
        for &n in inputs {
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
            Codec { level: self.opts.merge_level, block_level: self.opts.merge_level, dict: self.train_dict(&sessions, inputs)?, threads: self.opts.threads }
        } else {
            Codec::plain(self.opts.level)
        };
        // cold sessions, in a merge into full-size segments: runs instead of frames
        let newest = self.segs.values().map(|s| s.cat.time_hi).max().unwrap_or(0);
        let cold_before = if settled && self.opts.cold_run > 0 && newest > 0 { newest - self.opts.cold_after_ms } else { i64::MIN };
        let mut w: Option<SegWriter> = None;
        let mut dead: BTreeMap<u32, Dead> = BTreeMap::new();
        let (mut folded, mut fold_read) = (0u64, 0u64);
        for &s in &sessions {
            if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
                return Err(bad("merge cancelled"));
            }
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
                if outputs.len() as u32 >= max_files {
                    return Err(bad("a merge ran out of the file numbers set aside for it"));
                }
                let num = first + outputs.len() as u32;
                outputs.push(num);
                w = Some(SegWriter::create(&self.dir, num, self.opts.frame_size, codec.clone(), 1, self.manifest.checkpoint)?);
                // steps come from `load_steps`, which gives every step today's rule values
                w.as_mut().unwrap().set_rules(self.rules.versions());
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
        Ok((dead, bytes_in, fold_read, folded))
    }

    /// Put a merge's outputs in place of its inputs: the manifest first, then this handle's
    /// own state. Every session must be flushed (the sessions held in memory point at the
    /// old files and are read again from the new ones), so this runs at the end of a flush.
    fn merge_install(&mut self, b: MergeBuild) -> Result<()> {
        let t = std::time::Instant::now();
        let r = self.merge_install_timed(b);
        self.merge_stats.merge_install_ms_max = self.merge_stats.merge_install_ms_max.max(t.elapsed().as_millis() as u64);
        r
    }

    fn merge_install_timed(&mut self, b: MergeBuild) -> Result<()> {
        let MergeBuild { inputs, outputs, dead, bytes_in, fold_read, folded, reclaim } = b;
        if let Some(n) = inputs.iter().find(|n| !self.segs.contains_key(n)) {
            return corrupt(format!("a merge read segment {n}, which is gone"));
        }
        let mut man = self.manifest.clone();
        man.generation += 1;
        man.segs.retain(|n| !inputs.contains(n));
        man.segs.extend(&outputs);
        let mut all_dead: BTreeMap<u32, Dead> = man.dead.iter().filter(|(n, _)| !inputs.contains(n)).copied().collect();
        for (n, x) in dead {
            all_dead.entry(n).or_default().add(&x);
        }
        // a merge that was at work when sessions were deleted wrote them out again
        let mut opened = Vec::with_capacity(outputs.len());
        for &n in &outputs {
            let seg = Segment::open(&self.dir, n)?;
            if let Some(x) = self.dropped_dead(&seg)? {
                all_dead.entry(n).or_default().add(&x);
            }
            opened.push(seg);
        }
        man.dead = all_dead.into_iter().collect();
        man.store(&self.dir, self.opts.sync)?;
        self.manifest = man;
        // sessions held in memory point at the old files: those all in files leave (and are
        // read again from the new ones when needed); the others keep what is not flushed yet
        // and get their flushed part read again below
        self.active.retain(|_, a| !a.flushed());
        let kept: Vec<u32> = self.active.keys().copied().collect();
        let active = &self.active;
        self.key_ix.retain(|_, s| active.contains_key(s));
        for c in [&self.c.cores as &dyn Clear, &self.c.nodemaps, &self.c.stepc, &self.c.lightc, &self.c.fieldc, &self.c.dirs, &self.c.blocks, &self.c.statc, &self.c.longc, &self.c.textc, &self.c.msgtextc] {
            c.clear_all();
        }
        let mut bytes_out = 0;
        for seg in opened {
            bytes_out += seg.file_bytes;
            self.add_segment(seg);
        }
        // sessions still taking writes keep reading their flushed messages from the inputs
        // (retired, deleted once none of them does); the other inputs go now
        let used: BTreeSet<u32> = kept.iter().flat_map(|s| self.active[s].core.chunks.iter().map(|c| c.seg)).collect();
        for n in &inputs {
            let seg = self.segs.remove(n);
            self.seg_codes.remove(n);
            if let Some(seg) = seg {
                if used.contains(n) {
                    self.retired.insert(*n, seg);
                } else {
                    // deleted once no snapshot reads it
                    seg.doom();
                }
            }
        }
        self.drop_retired();
        self.prune_segment_tables();
        self.merge_stats.merges += 1;
        self.merge_stats.bytes_in += bytes_in + fold_read;
        self.merge_stats.bytes_out += bytes_out;
        self.merge_stats.folded += folded;
        self.merge_stats.reclaims += reclaim as u64;
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
                let sg = self.seg_by_num(ch.seg).ok_or_else(|| bad("segment is gone"))?;
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
        self.settle()?;
        let all: BTreeSet<u32> = self.segs.keys().copied().collect();
        self.merge(all, false, true)
    }

    // ------------------------------------------------------------------ reads

    /// Append a node's stored bytes (opaque values still cut out).
    fn stored_bytes(&self, core: &SessionCore, nm: &NodeMeta, cache: &mut HashMap<(u32, usize), Arc<Vec<u8>>>, out: &mut Vec<u8>) -> Result<()> {
        match nm.loc {
            Loc::Mem(mi) => out.extend_from_slice(self.mem_node(mi)),
            Loc::Seg { chunk, off } => {
                if nm.len == 0 {
                    return Ok(());
                }
                let ch = &core.chunks[chunk as usize];
                let sg = self.seg_by_num(ch.seg).ok_or_else(|| bad(format!("segment {} is gone", ch.seg)))?;
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
            OLoc::Mem(id) => self.mem_opaque(id).ok_or_else(|| bad(format!("missing opaque value {id}")))?.3.to_vec(),
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
            let sg = self.seg_by_num(ch.seg).ok_or_else(|| bad(format!("segment {} is gone", ch.seg)))?;
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
            let (_, _, b) = self.mem_piece(id).ok_or_else(|| bad(format!("missing piece {id}")))?;
            return Ok(Arc::new(b.to_vec()));
        }
        if let Some(p) = self.c.pieces.get(&id) {
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
        Ok(self.c.pieces.insert(id, Arc::new(d), n))
    }

    fn skeleton(&self, id: u32) -> Result<Arc<Skeleton>> {
        if let Some(s) = self.c.skels.get(&id) {
            return Ok(s);
        }
        let p = self.piece(id)?;
        let n = p.len() + 64;
        Ok(self.c.skels.insert(id, Arc::new(Skeleton::decode(&p)?), n))
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
        if let Some(r) = self.mem_request(id) {
            return Some(r);
        }
        self.find_request_in_files(id).ok().flatten()
    }

    pub fn request(&self, ri: ReqId) -> Result<ReqRec> {
        let (core, k) = self.core_req(ri)?;
        Ok(core.requests[k as usize].clone())
    }

    pub fn request_count(&self) -> usize {
        (self.segs.values().map(|s| s.cat.requests).sum::<u64>() - self.dead_total().requests) as usize + self.mems().map(|m| m.req_ix.len()).sum::<usize>()
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

    /// For each stored session, a number for its session id and one for its agent name:
    /// equal numbers mean equal text. Worked out once per stored session.
    pub fn key_ids(&self, sessions: &[u32]) -> Result<Vec<(u32, u32)>> {
        let mut g = self.c.key_ids.lock().unwrap();
        let mut out = Vec::with_capacity(sessions.len());
        for &s in sessions {
            if g.of.len() <= s as usize {
                g.of.resize(s as usize + 1, None);
            }
            let ids = match g.of[s as usize] {
                Some(x) => x,
                None => {
                    let core = self.load_core(s)?;
                    let (ns, na) = (g.sids.len() as u32, g.agents.len() as u32);
                    let sid = *g.sids.entry(core.key.0.clone()).or_insert(ns);
                    if sid == ns {
                        g.sid_names.push(core.key.0.as_str().into());
                    }
                    let agent = *g.agents.entry(core.key.1.clone()).or_insert(na);
                    if agent == na {
                        g.agent_names.push(core.key.1.as_str().into());
                    }
                    g.of[s as usize] = Some((sid, agent));
                    (sid, agent)
                }
            };
            out.push(ids);
        }
        Ok(out)
    }

    /// A session's tree and requests (for [`SessionCore::key`] without copying it).
    pub fn core(&self, s: u32) -> Result<Arc<SessionCore>> {
        self.load_core(s)
    }

    /// The session's (session id, agent id). Panics if the session cannot be read.
    pub fn session_key(&self, s: u32) -> (String, String) {
        let (a, b) = self.session_names(s).expect("read session");
        (a.to_string(), b.to_string())
    }

    /// The session's session id and agent id, kept once looked up (a session's key never
    /// changes), so asking again does not read the session.
    pub fn session_names(&self, s: u32) -> Result<(Arc<str>, Arc<str>)> {
        {
            let g = self.c.key_ids.lock().unwrap();
            if let Some(Some((a, b))) = g.of.get(s as usize) {
                return Ok((g.sid_names[*a as usize].clone(), g.agent_names[*b as usize].clone()));
            }
        }
        let (a, b) = self.key_ids(&[s])?[0];
        let g = self.c.key_ids.lock().unwrap();
        Ok((g.sid_names[a as usize].clone(), g.agent_names[b as usize].clone()))
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
        let core = self.load_node_map(s)?;
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(&core, ids, &mut buf, &mut spans, st)?;
        Ok(spans.into_iter().map(|(a, b)| buf[a..b].to_vec()).collect())
    }

    /// Every message of a session in storage order: the whole timeline in one pass.
    pub fn replay(&self, s: u32, st: &mut ReadStats) -> Result<Vec<u8>> {
        let core = self.load_core(s)?;
        let responses: std::collections::HashSet<u32> = core.requests.iter().map(|r| r.response).filter(|&r| r != NONE).collect();
        let mut ids: Vec<u32> = (0..core.nodes.len() as u32).filter(|i| !responses.contains(i)).collect();
        ids.sort_by_key(|&i| match core.nodes[i as usize].loc {
            Loc::Seg { chunk, off } => (0u8, chunk, off),
            Loc::Mem(m) => (1u8, 0, m),
        });
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        self.read_nodes(&core, &ids, &mut buf, &mut spans, st)?;
        Ok(buf)
    }

    /// The response stored with a request, exactly as it came; `None` if it has none.
    pub fn response(&self, ri: ReqId) -> Result<Option<Vec<u8>>> {
        let (core, k) = self.core_req(ri)?;
        let r = core.requests[k as usize].response;
        if r == NONE {
            return Ok(None);
        }
        let (mut buf, mut spans) = (Vec::new(), Vec::new());
        self.read_nodes(&core, &[r], &mut buf, &mut spans, &mut ReadStats::default())?;
        Ok(Some(buf))
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
            wal_bytes: self.wal.as_ref().map_or(0, |w| w.bytes),
            mem_bytes: self.pending_bytes(),
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
        for m in self.mems() {
            st.nodes += m.nodes.len();
            st.node_bytes += m.nodes.iter().map(|n| n.len() as u64).sum::<u64>();
            st.piece_raw_bytes += m.pieces.iter().map(|p| p.2.len() as u64).sum::<u64>();
            st.opaque_bytes += m.opaques.iter().map(|o| o.3.len() as u64).sum::<u64>();
        }
        for a in self.active.values() {
            st.steps += a.steps.rows.len() - a.steps.flushed as usize;
            st.active_bytes += a.core.bytes() + a.steps.bytes() + a.children.len() * 40 + a.stored_kids.len() * 4 + a.keyed.len() * 8;
        }
        st.caches = vec![
            ("sessions", self.c.cores.stats()),
            ("steps", self.c.stepc.stats()),
            ("steps without values", self.c.lightc.stats()),
            ("steps with some fields' values", self.c.fieldc.stats()),
            ("sessions' text", self.c.msgtextc.stats()),
            ("text index heads", self.c.texthead.stats()),
            ("text index postings", self.c.textpost.stats()),
            ("unit texts", self.c.unitc.stats()),
            ("unit text windows", self.c.piecec.stats()),
            ("step values read by summaries", self.c.textc.stats()),
            ("step summaries", self.c.statc.stats()),
            ("long input values", self.c.longc.stats()),
            ("blocks", self.c.blocks.stats()),
            ("directories", self.c.dirs.stats()),
            ("index blocks", self.c.ix.stats()),
            ("pieces", self.c.pieces.stats()),
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
        self.c.stepc.get(&self.skey(s))
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
        if let Some(c) = self.c.stepc.get(&self.skey(s)) {
            return Ok(c);
        }
        let mut keep = fields.to_vec();
        if let Some(c) = self.c.fieldc.get(&self.skey(s)) {
            if fields.iter().all(|f| c.kept.contains(f)) {
                return Ok(c);
            }
            keep.extend_from_slice(&c.kept);
            keep.sort_unstable();
            keep.dedup();
        }
        let ss = self.read_steps(s)?.with_fields(&keep);
        let size = ss.bytes();
        Ok(self.c.fieldc.insert(self.skey(s), Arc::new(ss), size))
    }

    /// Where every text unit is: each segment with a word index as a whole (with which of
    /// its parts are live, when folds replaced some), and as node ranges the messages not
    /// yet flushed. Every unit is in exactly one source.
    pub fn text_sources(&self) -> Result<Vec<TextSource>> {
        let mut out = Vec::new();
        for (&num, seg) in &self.segs {
            if seg.cat.text.is_none() {
                // a segment without a word index has no text units
                continue;
            }
            // parts a fold replaced are searched where they were written again
            let replaced = self.manifest.dead.iter().any(|(n, _)| *n == num);
            let live = if replaced { Some(self.live_mask(seg)?) } else { None };
            out.push(TextSource::Seg { num, live });
        }
        let mut act: Vec<(&u32, &Active)> = self.active.iter().collect();
        act.sort_by_key(|x| *x.0);
        for (&s, a) in act {
            if (a.core.flushed_nodes as usize) < a.core.nodes.len() {
                out.push(TextSource::Nodes { session: s, start: a.core.flushed_nodes, end: a.core.nodes.len() as u32 });
            }
        }
        Ok(out)
    }

    /// A segment's word index over message text: its units and word list.
    pub fn text_head(&self, num: u32) -> Result<Arc<crate::textindex::TextHead>> {
        if let Some(h) = self.c.head_res.lock().unwrap().get(&num) {
            return Ok(h.clone());
        }
        let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
        let Some(r) = &seg.cat.text else { return Err(bad(format!("segment {num} has no text index"))) };
        let dir = crate::textindex::decode_dir(&seg.block(r)?)?;
        let h = Arc::new(crate::textindex::TextHead::decode(&dir, &seg.block(&dir.units)?, &seg.block(&dir.words)?)?);
        self.c.head_res.lock().unwrap().insert(num, h.clone());
        Ok(h)
    }

    /// Bytes the decoded text index heads take, roughly.
    pub fn text_head_bytes(&self) -> usize {
        self.c.head_res.lock().unwrap().values().map(|h| h.bytes()).sum()
    }

    /// Add the units holding word `word` of a segment's index to `out`.
    pub fn text_postings(&self, num: u32, head: &crate::textindex::TextHead, word: usize, out: &mut crate::textindex::Bits) -> Result<()> {
        let (c, a, b) = head.postings_at(word);
        let block = match self.c.textpost.get(&(num, c as u32)) {
            Some(x) => x,
            None => {
                let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
                let raw = seg.block(&head.chunks[c].1)?;
                let size = raw.len() + 64;
                self.c.textpost.insert((num, c as u32), Arc::new(raw), size)
            }
        };
        if b > block.len() {
            return corrupt("a word's postings run past their block");
        }
        crate::textindex::add_postings(&block[a..b], out)
    }

    /// The text of units of a session, each (node, block): from the cache, the rest read
    /// from their messages (one read for all of them).
    pub fn unit_texts(&self, s: u32, wants: &[(u32, u16)]) -> Result<Vec<Arc<UnitText>>> {
        let mut out: Vec<Option<Arc<UnitText>>> = wants.iter().map(|&(n, b)| self.c.unitc.get(&(s, n, b as u32))).collect();
        let mut missing: Vec<u32> = wants.iter().zip(&out).filter(|x| x.1.is_none()).map(|x| x.0.0).collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            let msgs = self.read_messages(s, &missing, &mut ReadStats::default())?;
            // kept here too: the cache may drop an entry before it is asked for again
            let mut read: HashMap<(u32, u16), Arc<UnitText>> = HashMap::new();
            for (&node, m) in missing.iter().zip(&msgs) {
                let Some((_, spans)) = crate::text::scan_text(m) else { continue };
                for t in spans {
                    let text = crate::text::unescape(&m[t.span.clone()]);
                    let plain_case = crate::query::value::ascii_case_only(&text);
                    let size = text.len() + 96;
                    read.insert((node, t.block), self.c.unitc.insert((s, node, t.block as u32), Arc::new(UnitText { text, plain_case }), size));
                }
            }
            for (o, w) in out.iter_mut().zip(wants) {
                if o.is_none() {
                    *o = read.get(w).cloned();
                }
            }
        }
        out.into_iter().map(|o| o.ok_or_else(|| bad("a text unit is not in its message".to_string()))).collect()
    }

    /// Windows of the lower-cased text of units of a session, each (node, block, piece):
    /// the piece with [`crate::textindex::WINDOW`] bytes on each side, so a pattern whose
    /// first word starts in the piece lies inside the window. From the cache, the rest
    /// read from their messages (one read for all of them).
    pub fn piece_texts(&self, s: u32, wants: &[(u32, u16, u32)]) -> Result<Vec<Arc<PieceText>>> {
        use crate::textindex::{PIECE, WINDOW};
        let mut out: Vec<Option<Arc<PieceText>>> = wants.iter().map(|&(n, b, p)| self.c.piecec.get(&(s, n, b as u32, p))).collect();
        let mut missing: Vec<u32> = wants.iter().zip(&out).filter(|x| x.1.is_none()).map(|x| x.0.0).collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            let msgs = self.read_messages(s, &missing, &mut ReadStats::default())?;
            for (&node, m) in missing.iter().zip(&msgs) {
                let Some((_, spans)) = crate::text::scan_text(m) else { continue };
                for t in spans {
                    let mut low: Option<String> = None;
                    for (o, &(n, b, p)) in out.iter_mut().zip(wants) {
                        if o.is_some() || n != node || b != t.block {
                            continue;
                        }
                        let low = low.get_or_insert_with(|| crate::text::unescape(&m[t.span.clone()]).to_lowercase());
                        let mut a = (p as usize * PIECE).saturating_sub(WINDOW).min(low.len());
                        while !low.is_char_boundary(a) {
                            a -= 1;
                        }
                        let mut e = ((p as usize + 1) * PIECE + WINDOW).min(low.len());
                        while !low.is_char_boundary(e) {
                            e += 1;
                        }
                        let w = PieceText { text: low[a..e].to_string(), cut_left: a > 0, cut_right: e < low.len() };
                        let size = w.text.len() + 96;
                        *o = Some(self.c.piecec.insert((s, n, b as u32, p), Arc::new(w), size));
                    }
                }
            }
        }
        out.into_iter().map(|o| o.ok_or_else(|| bad("a text unit is not in its message".to_string()))).collect()
    }

    /// Every text unit of a session (user and assistant text blocks) with its text, read
    /// from the session's messages. The slow way to search: it reads the whole session.
    pub fn session_texts(&self, s: u32) -> Result<Arc<crate::text::SessionTexts>> {
        let active = self.active.contains_key(&s);
        if !active && let Some(c) = self.c.msgtextc.get(&self.skey(s)) {
            return Ok(c);
        }
        let core = self.load_core(s)?;
        let responses: std::collections::HashSet<u32> = core.requests.iter().map(|r| r.response).filter(|&r| r != NONE).collect();
        let ids: Vec<u32> = (0..core.nodes.len() as u32).filter(|i| !responses.contains(i)).collect();
        let msgs = self.read_messages(s, &ids, &mut ReadStats::default())?;
        let mut st = crate::text::SessionTexts::default();
        for (id, m) in ids.iter().zip(&msgs) {
            st.push_message(*id, m);
        }
        // a node's time is that of the first request that carried it
        st.node_time = vec![0; core.nodes.len()];
        let mut seen = vec![false; core.nodes.len()];
        for r in core.requests.iter() {
            let t = steps::parse_time_ms(&r.time);
            let mut c = r.tip;
            while c != NONE && !seen[c as usize] {
                seen[c as usize] = true;
                st.node_time[c as usize] = t;
                c = core.nodes[c as usize].parent;
            }
        }
        let st = Arc::new(st);
        if active {
            return Ok(st);
        }
        let size = st.bytes();
        Ok(self.c.msgtextc.insert(self.skey(s), st, size))
    }

    /// The text of a step value a summary points at: `vlen` bytes at `voff` in part
    /// `part`'s step data, which starts at `off` in block `r` of segment `num`. Kept after
    /// the first read.
    pub fn summary_text(&self, num: u32, part: u32, r: &BlockRef, off: u32, voff: u32, vlen: u32) -> Result<Arc<Vec<u8>>> {
        let key = (num, part, voff);
        if let Some(t) = self.c.textc.get(&key) {
            return Ok(t);
        }
        let b = self.stat_step_block(num, r)?;
        let a = off as usize + voff as usize;
        let t = b.post().get(a..a + vlen as usize).ok_or_else(|| bad("summary value past its block"))?.to_vec();
        let size = 64 + t.len();
        Ok(self.c.textc.insert(key, Arc::new(t), size))
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
            Loc::Mem(mi) => Ok(Some(self.mem_node(mi)[a..b].to_vec())),
            Loc::Seg { chunk, off: base } => {
                if len == 0 {
                    return Ok(Some(Vec::new()));
                }
                let ch = &core.chunks[chunk as usize];
                let sg = self.seg_by_num(ch.seg).ok_or_else(|| bad("segment is gone"))?;
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
        if let Some(x) = self.c.longc.get(&key) {
            return Ok((*x).clone());
        }
        let x = self.read_long_value(s, ss, pos, v, name)?;
        let size = 64 + x.as_ref().map_or(0, |b| b.len());
        Ok((*self.c.longc.insert(key, Arc::new(x), size)).clone())
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
        enum Found {
            /// A range of the message.
            At(usize, usize),
            /// Decoded from a JSON string (see [`steps::RawCall::fields`]).
            Decoded(Vec<u8>),
        }
        let find = |m: &[u8]| -> Option<Found> {
            let (calls, _) = steps::scan_message(m);
            let c = calls.iter().find(|c| c.block == r.call_block)?;
            let (_, v) = c.fields.iter().find(|(k, _)| **k == *name.as_bytes())?;
            Some(match v {
                std::borrow::Cow::Borrowed(x) => {
                    let start = x.as_ptr() as usize - m.as_ptr() as usize;
                    Found::At(start, start + x.len())
                }
                std::borrow::Cow::Owned(x) => Found::Decoded(x.clone()),
            })
        };
        let (stored, holes) = self.stored_message(s, r.call_node)?;
        match find(&stored) {
            None => Ok(None),
            Some(Found::At(a, b)) if !holes.iter().any(|&h| (h as usize) >= a && (h as usize) <= b) => Ok(Some(stored[a..b].to_vec())),
            Some(Found::Decoded(v)) if holes.is_empty() => Ok(Some(v)),
            Some(_) => {
                // an opaque value sits inside: read the field from the whole message
                let m = self.message(s, r.call_node)?;
                Ok(find(&m).map(|f| match f {
                    Found::At(a, b) => m[a..b].to_vec(),
                    Found::Decoded(v) => v,
                }))
            }
        }
    }

    // ------------------------------------------------------------------ step summaries

    /// Where every step is, for queries that read summaries: each live segment with
    /// summaries as a whole (with which of its parts are live, when folds replaced some),
    /// and as row ranges of sessions the rest: parts in segments without summaries and
    /// steps not yet flushed. Every step of every session is in exactly one source.
    pub fn step_sources(&self) -> Result<Vec<StepSource>> {
        self.step_sources_for(None)
    }

    /// [`Db::step_sources`] for a query that reads store field `field`: a segment whose
    /// summaries do not hold what queries see of that field (a rule's column the segment
    /// was written without, or with another version of the rule) gives its steps as rows.
    pub fn step_sources_for(&self, field: Option<u32>) -> Result<Vec<StepSource>> {
        let mut out = Vec::new();
        for (&num, seg) in &self.segs {
            let live = if self.manifest.dead.iter().any(|(n, _)| *n == num) { Some(self.live_mask(seg)?) } else { None };
            if seg.cat.stats.is_some() && !field.is_some_and(|f| self.seg_field_stale(seg, f)) {
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
        if let Some((g, m)) = self.c.live_masks.lock().unwrap().get(&seg.num)
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
        self.c.live_masks.lock().unwrap().insert(seg.num, (generation, m.clone()));
        Ok(m)
    }

    fn stat_block(&self, num: u32, r: &BlockRef, decode: impl FnOnce(&[u8]) -> Result<StatBlock>) -> Result<Arc<StatBlock>> {
        let key = (num, r.off);
        if let Some(b) = self.c.statc.get(&key) {
            return Ok(b);
        }
        let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
        let b = decode(&seg.block(r)?)?;
        let size = b.bytes();
        Ok(self.c.statc.insert(key, Arc::new(b), size))
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
        if let Some(b) = self.c.statc.get(&key) {
            return Ok(b);
        }
        let seg = self.segs.get(&num).ok_or_else(|| bad(format!("segment {num} is gone")))?;
        // step blocks may be compressed with the segment's dictionary
        let b = StatBlock::Raw(seg.step_block(r)?);
        let size = b.bytes();
        Ok(self.c.statc.insert(key, Arc::new(b), size))
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
                    for o in v.iter().filter(|o| o.file != GONE) {
                        add(o.kind, o.len as u64);
                    }
                }
            }
        }
        for m in self.mems_oldest_first() {
            for (_, kind, _, p) in &m.opaques {
                add(*kind, p.len() as u64);
            }
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
                        if o.file == GONE {
                            continue;
                        }
                        let p = self.blob_read(o.file, o.off, o.len)?;
                        f(r.cat.olo + (i * IDS + j) as u64, o.kind, o.enc, &p);
                    }
                }
            }
        }
        for m in self.mems_oldest_first() {
            for (k, (enc, kind, _, p)) in m.opaques.iter().enumerate() {
                f(m.opaque_base + k as u64, *kind, *enc, p);
            }
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
                    if d.vals.2 > 0 {
                        steps.insert(d.vals.0.off, d.vals.0);
                    }
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
                        if e.file == GONE {
                            continue;
                        }
                        let id = r.cat.plo + (i * IDS + j) as u32;
                        f(id, e.kind, &self.piece(id)?, e.clen, e.base);
                    }
                }
            }
        }
        for m in self.mems_oldest_first() {
            for (k, (kind, _, b)) in m.pieces.iter().enumerate() {
                f(m.piece_base + k as u32, *kind, b, b.len() as u32, 0);
            }
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
        for c in [&self.c.cores as &dyn Clear, &self.c.nodemaps, &self.c.stepc, &self.c.lightc, &self.c.dirs, &self.c.blocks, &self.c.ix, &self.c.pieces, &self.c.skels, &self.c.statc, &self.c.longc, &self.c.fieldc, &self.c.textc, &self.c.msgtextc, &self.c.texthead, &self.c.textpost, &self.c.unitc, &self.c.piecec] {
            c.clear_all();
        }
        self.c.dir_res.lock().unwrap().clear();
        self.c.head_res.lock().unwrap().clear();
        crate::segment::forget_frames();
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

impl<K: std::hash::Hash + Eq + Clone + Send + 'static, V: Send + Sync + 'static> Clear for Cache<K, V> {
    fn clear_all(&self) {
        self.clear();
    }
}
