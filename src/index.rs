//! The index: one set of files to find any stored thing by hash, and any piece or opaque
//! value by id.
//!
//! Keys are (namespace, 128-bit hash) -> u64: pieces and opaque values by content hash (so
//! each is stored once), request ids (so each request is stored once) and session keys (to
//! find the session a request belongs to). The id tables say where each piece and opaque
//! value sits in a blob file.
//!
//! Every flush writes one run with what it added. Runs are merged like segments: adjacent
//! runs of one size tier, a few at a time, up to a size cap, so a lookup checks a handful of
//! runs whatever the store's size. Inside a run everything has a fixed width and is found
//! by arithmetic or through one small fence page; nothing per key or per block stays in
//! memory, and what a lookup reads goes through the store's caches.
//!
//! `idx-N.tdb`: magic; key pages (2048 sorted keys, then a Bloom filter over just those
//! keys, each part with a crc32c); fence pages (the first key of each of 1024 key pages);
//! the piece table and the opaque value table (1024 entries a block, by id, each block with
//! a crc32c); the catalog (counts, offsets, the first key of each fence page) and the footer.

use crate::block::{file, handle, read_catalog, write_catalog};
use crate::bloom::Bloom;
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_file, SyncMode};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"TDBIDX01";

pub const NS_PIECE: u8 = 0;
pub const NS_OPAQUE: u8 = 1;
pub const NS_REQUEST: u8 = 2;
pub const NS_SESSION: u8 = 3;

/// Keys per key page.
pub const PAGE: usize = 2048;
const KEY_W: usize = 25;
/// Key pages per fence page.
pub const FENCES: usize = 1024;
const FENCE_W: usize = 17;
/// Entries per block of the piece and opaque tables.
pub const IDS: usize = 1024;
const PIECE_W: usize = 37;
const OPAQUE_W: usize = 34;
/// Bloom filter bits per key: about 0.2% false positives.
const BLOOM_BITS: usize = 14;

pub type Key = (u8, u128);

pub fn idx_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("idx-{num:06}.tdb"))
}

fn filter_len(n: usize) -> usize {
    Bloom::new(n, BLOOM_BITS).bytes()
}

fn filter_k() -> u32 {
    Bloom::new(PAGE, BLOOM_BITS).k()
}

fn page_stride() -> u64 {
    (PAGE * KEY_W + 4 + filter_len(PAGE) + 4) as u64
}

/// The 64-bit hash a key's Bloom filter probes use.
pub fn filter_hash(k: Key) -> u64 {
    (k.1 as u64) ^ (k.0 as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceEnt {
    pub kind: u8,
    pub hash: u128,
    /// Blob file and where the compressed piece starts.
    pub file: u32,
    pub off: u64,
    pub clen: u32,
    pub rawlen: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpaqueEnt {
    pub enc: u8,
    pub kind: u8,
    /// Not kept: no payload, and `len` is the length it had.
    pub dropped: bool,
    pub hash: u128,
    pub file: u32,
    pub off: u64,
    pub len: u32,
}

impl PieceEnt {
    fn put(&self, b: &mut Vec<u8>) {
        b.push(self.kind);
        b.extend_from_slice(&self.hash.to_le_bytes());
        b.extend_from_slice(&self.file.to_le_bytes());
        b.extend_from_slice(&self.off.to_le_bytes());
        b.extend_from_slice(&self.clen.to_le_bytes());
        b.extend_from_slice(&self.rawlen.to_le_bytes());
    }
    fn get(b: &[u8]) -> PieceEnt {
        PieceEnt { kind: b[0], hash: u128_at(b, 1), file: u32_at(b, 17), off: u64_at(b, 21), clen: u32_at(b, 29), rawlen: u32_at(b, 33) }
    }
}

impl OpaqueEnt {
    fn put(&self, b: &mut Vec<u8>) {
        b.push(self.enc);
        b.push(self.kind | if self.dropped { 0x80 } else { 0 });
        b.extend_from_slice(&self.hash.to_le_bytes());
        b.extend_from_slice(&self.file.to_le_bytes());
        b.extend_from_slice(&self.off.to_le_bytes());
        b.extend_from_slice(&self.len.to_le_bytes());
    }
    fn get(b: &[u8]) -> OpaqueEnt {
        OpaqueEnt { enc: b[0], kind: b[1] & 0x7f, dropped: b[1] & 0x80 != 0, hash: u128_at(b, 2), file: u32_at(b, 18), off: u64_at(b, 22), len: u32_at(b, 30) }
    }
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}
fn u128_at(b: &[u8], i: usize) -> u128 {
    u128::from_le_bytes(b[i..i + 16].try_into().unwrap())
}

/// Kinds of block a run is read in, for caching by (run, kind, index).
pub const K_FENCE: u8 = 0;
pub const K_FILTER: u8 = 1;
pub const K_KEYS: u8 = 2;
pub const K_PIECES: u8 = 3;
pub const K_OPAQUES: u8 = 4;

#[derive(Debug)]
pub enum IxBlock {
    Fence(Vec<Key>),
    Filter(Bloom),
    Keys(Vec<(u8, u128, u64)>),
    Pieces(Vec<PieceEnt>),
    Opaques(Vec<OpaqueEnt>),
}

impl IxBlock {
    /// Bytes it takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        64 + match self {
            IxBlock::Fence(v) => v.len() * 32,
            IxBlock::Filter(b) => b.bytes(),
            IxBlock::Keys(v) => v.len() * 32,
            IxBlock::Pieces(v) => v.len() * 48,
            IxBlock::Opaques(v) => v.len() * 48,
        }
    }
}

/// What every open store keeps in memory about a run.
#[derive(Debug, Clone, Default)]
pub struct RunCat {
    pub n_keys: u64,
    /// Keys per namespace.
    pub ns: [u64; 4],
    pub keys_off: u64,
    pub fence_off: u64,
    pub piece_off: u64,
    pub opaque_off: u64,
    /// Piece ids `plo..plo + np` and opaque ids `olo..olo + no`.
    pub plo: u32,
    pub np: u32,
    pub olo: u64,
    pub no: u64,
    /// First key of each fence page.
    pub top: Vec<Key>,
    pub piece_bytes: u64,
    pub piece_raw_bytes: u64,
    pub opaque_bytes: u64,
    pub n_dropped: u64,
}

impl RunCat {
    pub fn pages(&self) -> usize {
        (self.n_keys as usize).div_ceil(PAGE)
    }

    fn page_n(&self, i: usize) -> usize {
        (self.n_keys as usize - i * PAGE).min(PAGE)
    }

    /// Bytes this catalog takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        192 + self.top.len() * 32
    }

    pub fn has_piece(&self, id: u32) -> bool {
        id >= self.plo && id - self.plo < self.np
    }

    pub fn has_opaque(&self, id: u64) -> bool {
        id >= self.olo && id - self.olo < self.no
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.var(self.n_keys);
        for n in self.ns {
            e.var(n);
        }
        for o in [self.keys_off, self.fence_off, self.piece_off, self.opaque_off] {
            e.var(o);
        }
        e.var(self.plo as u64);
        e.var(self.np as u64);
        e.var(self.olo);
        e.var(self.no);
        e.var(self.top.len() as u64);
        for &(ns, h) in &self.top {
            e.u8(ns);
            e.u128(h);
        }
        for v in [self.piece_bytes, self.piece_raw_bytes, self.opaque_bytes, self.n_dropped] {
            e.var(v);
        }
        e.buf
    }

    fn decode(b: &[u8]) -> Result<RunCat> {
        let mut d = Dec::new(b);
        let mut c = RunCat { n_keys: d.var()?, ..RunCat::default() };
        for n in c.ns.iter_mut() {
            *n = d.var()?;
        }
        c.keys_off = d.var()?;
        c.fence_off = d.var()?;
        c.piece_off = d.var()?;
        c.opaque_off = d.var()?;
        c.plo = d.var32()?;
        c.np = d.var32()?;
        c.olo = d.var()?;
        c.no = d.var()?;
        for _ in 0..d.var()? {
            c.top.push((d.u8()?, d.u128()?));
        }
        c.piece_bytes = d.var()?;
        c.piece_raw_bytes = d.var()?;
        c.opaque_bytes = d.var()?;
        c.n_dropped = d.var()?;
        if !d.done() || c.ns.iter().sum::<u64>() != c.n_keys || c.top.len() != c.pages().div_ceil(FENCES) {
            return corrupt("bad index catalog");
        }
        Ok(c)
    }
}

/// Writes one run: keys in ascending order, then the piece table, then the opaque table.
pub struct RunWriter {
    f: BufWriter<File>,
    pos: u64,
    cat: RunCat,
    page: Vec<(u8, u128, u64)>,
    fences: Vec<Key>,
    last: Option<(u8, u128, u64)>,
    /// 0 keys, 1 pieces, 2 opaque values.
    stage: u8,
    ids: Vec<u8>,
    ids_n: usize,
}

impl RunWriter {
    pub fn create(dir: &Path, num: u32, plo: u32, olo: u64) -> Result<RunWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(idx_path(dir, num))?;
        let mut f = BufWriter::with_capacity(1 << 20, f);
        f.write_all(MAGIC)?;
        let cat = RunCat { keys_off: 8, plo, olo, ..RunCat::default() };
        Ok(RunWriter { f, pos: 8, cat, page: Vec::with_capacity(PAGE), fences: Vec::new(), last: None, stage: 0, ids: Vec::new(), ids_n: 0 })
    }

    fn put(&mut self, b: &[u8]) -> Result<()> {
        self.f.write_all(b)?;
        self.pos += b.len() as u64;
        Ok(())
    }

    fn put_checked(&mut self, b: &[u8]) -> Result<()> {
        self.put(b)?;
        self.put(&crc32c::crc32c(b).to_le_bytes())
    }

    /// Add a key; keys must come in strictly ascending (namespace, hash, value) order.
    pub fn key(&mut self, ns: u8, h: u128, v: u64) -> Result<()> {
        if self.stage != 0 || ns > NS_SESSION {
            return corrupt("index key after the tables, or of an unknown kind");
        }
        if self.last.is_some_and(|l| l >= (ns, h, v)) {
            return corrupt("index keys out of order");
        }
        self.last = Some((ns, h, v));
        self.page.push((ns, h, v));
        self.cat.ns[ns as usize] += 1;
        self.cat.n_keys += 1;
        if self.page.len() == PAGE {
            self.write_page()?;
        }
        Ok(())
    }

    fn write_page(&mut self) -> Result<()> {
        let n = self.page.len();
        if n == 0 {
            return Ok(());
        }
        let mut b = Vec::with_capacity(n * KEY_W);
        let mut bloom = Bloom::new(n, BLOOM_BITS);
        for &(ns, h, v) in &self.page {
            b.push(ns);
            b.extend_from_slice(&h.to_le_bytes());
            b.extend_from_slice(&v.to_le_bytes());
            bloom.add(filter_hash((ns, h)));
        }
        self.put_checked(&b)?;
        self.put_checked(&bloom.raw())?;
        self.fences.push((self.page[0].0, self.page[0].1));
        self.page.clear();
        Ok(())
    }

    fn end_keys(&mut self) -> Result<()> {
        if self.stage > 0 {
            return Ok(());
        }
        self.write_page()?;
        self.cat.fence_off = self.pos;
        let fences = std::mem::take(&mut self.fences);
        for c in fences.chunks(FENCES) {
            let mut b = Vec::with_capacity(c.len() * FENCE_W);
            for &(ns, h) in c {
                b.push(ns);
                b.extend_from_slice(&h.to_le_bytes());
            }
            self.put_checked(&b)?;
            self.cat.top.push(c[0]);
        }
        self.stage = 1;
        self.cat.piece_off = self.pos;
        Ok(())
    }

    fn flush_ids(&mut self) -> Result<()> {
        if self.ids_n == 0 {
            return Ok(());
        }
        let b = std::mem::take(&mut self.ids);
        self.put_checked(&b)?;
        self.ids_n = 0;
        Ok(())
    }

    /// The next piece (id `plo + pieces so far`).
    pub fn piece(&mut self, e: &PieceEnt) -> Result<()> {
        self.end_keys()?;
        if self.stage != 1 {
            return corrupt("piece after the opaque values in an index run");
        }
        e.put(&mut self.ids);
        self.ids_n += 1;
        self.cat.np += 1;
        self.cat.piece_bytes += e.clen as u64;
        self.cat.piece_raw_bytes += e.rawlen as u64;
        if self.ids_n == IDS {
            self.flush_ids()?;
        }
        Ok(())
    }

    fn end_pieces(&mut self) -> Result<()> {
        self.end_keys()?;
        if self.stage > 1 {
            return Ok(());
        }
        self.flush_ids()?;
        self.stage = 2;
        self.cat.opaque_off = self.pos;
        Ok(())
    }

    /// The next opaque value (id `olo + values so far`).
    pub fn opaque(&mut self, e: &OpaqueEnt) -> Result<()> {
        self.end_pieces()?;
        e.put(&mut self.ids);
        self.ids_n += 1;
        self.cat.no += 1;
        if e.dropped {
            self.cat.n_dropped += 1;
        } else {
            self.cat.opaque_bytes += e.len as u64;
        }
        if self.ids_n == IDS {
            self.flush_ids()?;
        }
        Ok(())
    }

    /// Write the catalog and footer and make the file durable; returns its size.
    pub fn finish(mut self, mode: SyncMode) -> Result<u64> {
        self.end_pieces()?;
        self.flush_ids()?;
        let cat = self.cat.encode();
        write_catalog(&mut self.f, &mut self.pos, &cat, MAGIC)?;
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        sync_file(&f, mode)?;
        Ok(self.pos)
    }
}

pub struct Run {
    pub num: u32,
    uid: u64,
    path: PathBuf,
    pub cat: RunCat,
    pub file_bytes: u64,
}

impl Run {
    pub fn open(dir: &Path, num: u32) -> Result<Run> {
        let path = idx_path(dir, num);
        let f = file(&path)?;
        let (c, file_bytes) = read_catalog(&f, MAGIC, &format!("index run {num}"))?;
        let cat = RunCat::decode(&c)?;
        let run = Run { num, uid: crate::block::uid(), path, cat, file_bytes };
        let c = &run.cat;
        let ends = [
            (c.keys_off, run.pages_end()),
            (c.fence_off, c.fence_off + (c.pages().div_ceil(FENCES) * 4 + c.pages() * FENCE_W) as u64),
            (c.piece_off, c.piece_off + (c.np as u64).div_ceil(IDS as u64) * 4 + c.np as u64 * PIECE_W as u64),
            (c.opaque_off, c.opaque_off + c.no.div_ceil(IDS as u64) * 4 + c.no * OPAQUE_W as u64),
        ];
        if ends.windows(2).any(|w| w[0].1 != w[1].0) || ends[3].1 > file_bytes {
            return corrupt(format!("index run {num}: catalog does not match the file"));
        }
        Ok(run)
    }

    fn pages_end(&self) -> u64 {
        let c = &self.cat;
        match c.pages() {
            0 => c.keys_off,
            p => {
                let n = c.page_n(p - 1);
                c.keys_off + (p as u64 - 1) * page_stride() + (n * KEY_W + 8 + filter_len(n)) as u64
            }
        }
    }

    /// `len` bytes plus their crc32c at `off`, checked.
    fn checked(&self, off: u64, len: usize) -> Result<Vec<u8>> {
        let mut b = vec![0u8; len + 4];
        handle(self.uid, &self.path)?.read_exact_at(&mut b, off)?;
        let crc = u32::from_le_bytes(b[len..].try_into().unwrap());
        b.truncate(len);
        if crc32c::crc32c(&b) != crc {
            return corrupt(format!("index run {}: checksum mismatch", self.num));
        }
        Ok(b)
    }

    /// Block `i` of kind `kind` (`K_FENCE`, `K_FILTER`, `K_KEYS`, `K_PIECES`, `K_OPAQUES`).
    pub fn block(&self, kind: u8, i: usize) -> Result<IxBlock> {
        let c = &self.cat;
        let bad = || corrupt(format!("index run {} has no block {kind}/{i}", self.num));
        Ok(match kind {
            K_KEYS | K_FILTER => {
                if i >= c.pages() {
                    return bad();
                }
                let n = c.page_n(i);
                let off = c.keys_off + i as u64 * page_stride();
                if kind == K_KEYS {
                    let b = self.checked(off, n * KEY_W)?;
                    IxBlock::Keys(b.as_chunks::<KEY_W>().0.iter().map(|e| (e[0], u128_at(e, 1), u64_at(e, 17))).collect())
                } else {
                    IxBlock::Filter(Bloom::from_raw(&self.checked(off + (n * KEY_W + 4) as u64, filter_len(n))?, filter_k())?)
                }
            }
            K_FENCE => {
                if i >= c.top.len() {
                    return bad();
                }
                let n = (c.pages() - i * FENCES).min(FENCES);
                let b = self.checked(c.fence_off + (i * (FENCES * FENCE_W + 4)) as u64, n * FENCE_W)?;
                IxBlock::Fence(b.as_chunks::<FENCE_W>().0.iter().map(|e| (e[0], u128_at(e, 1))).collect())
            }
            K_PIECES => {
                if i as u64 >= (c.np as u64).div_ceil(IDS as u64) {
                    return bad();
                }
                let n = (c.np as usize - i * IDS).min(IDS);
                let b = self.checked(c.piece_off + (i * (IDS * PIECE_W + 4)) as u64, n * PIECE_W)?;
                IxBlock::Pieces(b.as_chunks::<PIECE_W>().0.iter().map(|e| PieceEnt::get(e)).collect())
            }
            K_OPAQUES => {
                if i as u64 >= c.no.div_ceil(IDS as u64) {
                    return bad();
                }
                let n = (c.no as usize - i * IDS).min(IDS);
                let b = self.checked(c.opaque_off + (i * (IDS * OPAQUE_W + 4)) as u64, n * OPAQUE_W)?;
                IxBlock::Opaques(b.as_chunks::<OPAQUE_W>().0.iter().map(|e| OpaqueEnt::get(e)).collect())
            }
            _ => return bad(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The value stored last under `k` in `run`, reading blocks through `get` (so the caller
/// can cache them): one fence page, one filter, and the key page only if the filter
/// says the key may be there.
pub fn lookup(run: &Run, k: Key, get: impl Fn(u8, usize) -> Result<Arc<IxBlock>>) -> Result<Option<u64>> {
    let j = run.cat.top.partition_point(|f| *f <= k);
    if j == 0 {
        return Ok(None);
    }
    let fb = get(K_FENCE, j - 1)?;
    let IxBlock::Fence(f) = &*fb else { return corrupt("index block of the wrong kind") };
    let p = f.partition_point(|x| *x <= k);
    if p == 0 {
        return Ok(None);
    }
    let page = (j - 1) * FENCES + p - 1;
    let fl = get(K_FILTER, page)?;
    let IxBlock::Filter(bloom) = &*fl else { return corrupt("index block of the wrong kind") };
    if !bloom.may_contain(filter_hash(k)) {
        return Ok(None);
    }
    let kb = get(K_KEYS, page)?;
    let IxBlock::Keys(v) = &*kb else { return corrupt("index block of the wrong kind") };
    let i = v.partition_point(|e| (e.0, e.1) <= k);
    Ok((i > 0 && (v[i - 1].0, v[i - 1].1) == k).then(|| v[i - 1].2))
}

/// Merge adjacent runs (in id order) into run `num`, reading each input once from start
/// to end; returns the new file's size.
pub fn merge(dir: &Path, num: u32, inputs: &[Arc<Run>], mode: SyncMode) -> Result<u64> {
    let Some(first) = inputs.first() else { return corrupt("index merge of nothing") };
    for w in inputs.windows(2) {
        let (a, b) = (&w[0].cat, &w[1].cat);
        if a.plo + a.np != b.plo || a.olo + a.no != b.olo {
            return corrupt("index merge of runs that are not adjacent");
        }
    }
    let mut out = RunWriter::create(dir, num, first.cat.plo, first.cat.olo)?;
    struct Cur {
        page: usize,
        v: Vec<(u8, u128, u64)>,
        i: usize,
    }
    let keys = |r: &Run, p: usize| -> Result<Vec<(u8, u128, u64)>> {
        match r.block(K_KEYS, p)? {
            IxBlock::Keys(v) => Ok(v),
            _ => corrupt("index block of the wrong kind"),
        }
    };
    let mut curs = Vec::with_capacity(inputs.len());
    let mut heap = BinaryHeap::new();
    for (k, r) in inputs.iter().enumerate() {
        if r.cat.pages() > 0 {
            let v = keys(r, 0)?;
            heap.push(Reverse((v[0], k)));
            curs.push(Cur { page: 0, v, i: 0 });
        } else {
            curs.push(Cur { page: 0, v: Vec::new(), i: 0 });
        }
    }
    let mut last = None;
    while let Some(Reverse((e, k))) = heap.pop() {
        if last != Some(e) {
            out.key(e.0, e.1, e.2)?;
            last = Some(e);
        }
        let c = &mut curs[k];
        c.i += 1;
        if c.i == c.v.len() {
            c.page += 1;
            if c.page == inputs[k].cat.pages() {
                continue;
            }
            c.v = keys(&inputs[k], c.page)?;
            c.i = 0;
        }
        heap.push(Reverse((c.v[c.i], k)));
    }
    for r in inputs {
        for b in 0..(r.cat.np as usize).div_ceil(IDS) {
            let IxBlock::Pieces(v) = r.block(K_PIECES, b)? else { return corrupt("index block of the wrong kind") };
            for e in &v {
                out.piece(e)?;
            }
        }
    }
    for r in inputs {
        for b in 0..(r.cat.no as usize).div_ceil(IDS) {
            let IxBlock::Opaques(v) = r.block(K_OPAQUES, b)? else { return corrupt("index block of the wrong kind") };
            for e in &v {
                out.opaque(e)?;
            }
        }
    }
    out.finish(mode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tdb-index-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn h(i: u64) -> u128 {
        xxhash_rust::xxh3::xxh3_128(&i.to_le_bytes())
    }

    fn get(run: &Run) -> impl Fn(u8, usize) -> Result<Arc<IxBlock>> + '_ {
        move |kind, i| Ok(Arc::new(run.block(kind, i)?))
    }

    /// Write a run of keys `range` (namespace by key) with `np` pieces and `no` opaques.
    fn write(dir: &Path, num: u32, range: std::ops::Range<u64>, plo: u32, np: u32, olo: u64, no: u64) -> Run {
        let mut keys: Vec<(u8, u128, u64)> = range.map(|i| ((i % 4) as u8, h(i), i)).collect();
        keys.sort_unstable();
        let mut w = RunWriter::create(dir, num, plo, olo).unwrap();
        for (ns, hh, v) in keys {
            w.key(ns, hh, v).unwrap();
        }
        for i in 0..np {
            let id = plo + i;
            w.piece(&PieceEnt { kind: 1, hash: h(id as u64), file: 7, off: id as u64 * 10, clen: 5, rawlen: 9 }).unwrap();
        }
        for i in 0..no {
            let id = olo + i;
            w.opaque(&OpaqueEnt { enc: 0, kind: 2, dropped: id.is_multiple_of(5), hash: h(id), file: 3, off: id * 3, len: 4 }).unwrap();
        }
        w.finish(SyncMode::Os).unwrap();
        Run::open(dir, num).unwrap()
    }

    #[test]
    fn lookups_merges_and_tables() {
        let dir = tmp("lookups");
        let a = Arc::new(write(&dir, 1, 0..5000, 0, 1500, 0, 2100));
        let b = Arc::new(write(&dir, 2, 5000..5003, 1500, 0, 2100, 7));
        let c = Arc::new(write(&dir, 3, 5003..9000, 1500, 3000, 2107, 0));
        for (r, range) in [(&a, 0..5000u64), (&b, 5000..5003), (&c, 5003..9000)] {
            for i in range.clone() {
                assert_eq!(lookup(r, ((i % 4) as u8, h(i)), get(r)).unwrap(), Some(i), "key {i}");
            }
            let misses = (100_000..110_000u64).filter(|&i| lookup(r, ((i % 4) as u8, h(i)), get(r)).unwrap().is_some()).count();
            assert_eq!(misses, 0);
        }
        merge(&dir, 4, &[a.clone(), b.clone(), c.clone()], SyncMode::Os).unwrap();
        let m = Run::open(&dir, 4).unwrap();
        assert_eq!(m.cat.n_keys, 9000);
        assert_eq!((m.cat.plo, m.cat.np, m.cat.olo, m.cat.no), (0, 4500, 0, 2107));
        for i in 0..9000u64 {
            assert_eq!(lookup(&m, ((i % 4) as u8, h(i)), get(&m)).unwrap(), Some(i));
        }
        for id in [0u32, 1023, 1024, 1499, 1500, 4499] {
            let IxBlock::Pieces(v) = m.block(K_PIECES, (id as usize) / IDS).unwrap() else { panic!() };
            assert_eq!(v[id as usize % IDS].off, id as u64 * 10);
        }
        for id in [0u64, 5, 2099, 2100, 2106] {
            let IxBlock::Opaques(v) = m.block(K_OPAQUES, (id as usize) / IDS).unwrap() else { panic!() };
            let e = v[id as usize % IDS];
            assert_eq!((e.off, e.dropped, e.hash), (id * 3, id.is_multiple_of(5), h(id)));
        }
        // merged adjacency is checked
        assert!(merge(&dir, 5, &[a.clone(), c.clone()], SyncMode::Os).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_newest_copy_of_a_key_wins_across_pages() {
        let dir = tmp("dups");
        // one hash stored twice in a run (a value not kept, then kept): the later id wins,
        // even when the two land on different pages
        let mut keys: BTreeMap<(u8, u128, u64), ()> = BTreeMap::new();
        for i in 0..(PAGE as u64 * 3) {
            keys.insert((NS_OPAQUE, h(i), i), ());
        }
        // the last key of the first page gets a newer copy, which starts the second page
        let dup = keys.keys().nth(PAGE - 1).unwrap().1;
        keys.insert((NS_OPAQUE, dup, 1 << 40), ());
        assert_eq!(keys.keys().nth(PAGE).unwrap().2, 1 << 40);
        let mut w = RunWriter::create(&dir, 1, 0, 0).unwrap();
        for k in keys.keys() {
            w.key(k.0, k.1, k.2).unwrap();
        }
        assert!(w.key(NS_OPAQUE, 0, 0).is_err(), "out of order");
        w.finish(SyncMode::Os).unwrap();
        let r = Run::open(&dir, 1).unwrap();
        assert_eq!(lookup(&r, (NS_OPAQUE, dup), get(&r)).unwrap(), Some(1 << 40));
        assert_eq!(lookup(&r, (NS_PIECE, dup), get(&r)).unwrap(), None);
        // a damaged page is caught
        let p = idx_path(&dir, 1);
        let mut b = std::fs::read(&p).unwrap();
        b[100] ^= 1;
        std::fs::write(&p, &b).unwrap();
        let r = Run::open(&dir, 1).unwrap();
        assert!(r.block(K_KEYS, 0).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
