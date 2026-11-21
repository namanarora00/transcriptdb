//! Blob files: the shared pieces (request skeletons, system prompts, tool lists, cache
//! markers) and the opaque values (thinking signatures, images) that arrived between two
//! flushes. Ids are dense and each file holds one contiguous range of piece ids and one of
//! opaque ids. Merges never rewrite blob files: they hold over half of the bytes and none
//! of the session structure.
//!
//! `blob-N.tdb`: magic, payloads (opaque values raw, pieces zstd-compressed one by one),
//! the tables in blocks (pieces and opaque values by id, 1024 to a block; a hash index
//! sorted by hash, 2048 to a block), then the catalog (id ranges, a Bloom filter of
//! hashes, where each block is, the first hash of each index block) and the footer. A
//! lookup reads one small block, never a whole table.

use crate::block::{file, handle, read_block, read_catalog, write_block, write_catalog, BlockRef};
use crate::bloom::Bloom;
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_file, SyncMode};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"TDBBLB02";
/// Table entries per id block, and per hash index block.
pub const ID_BLOCK: usize = 1024;
pub const HASH_BLOCK: usize = 2048;
/// Bloom filter bits per hash: about 0.2% false positives.
const BLOOM_BITS: usize = 14;

pub fn blob_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("blob-{num:06}.tdb"))
}

#[derive(Debug, Clone, Copy)]
pub struct PieceEnt {
    pub kind: u8,
    pub hash: u128,
    pub off: u64,
    pub clen: u32,
    pub rawlen: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct OpaqueEnt {
    pub enc: u8,
    pub kind: u8,
    /// Not kept: no payload, and `len` is the length it had.
    pub dropped: bool,
    pub hash: u128,
    pub off: u64,
    pub len: u32,
}

/// One block of a blob file's tables.
#[derive(Debug)]
pub enum TBlock {
    Pieces(Vec<PieceEnt>),
    Opaques(Vec<OpaqueEnt>),
    /// (hash, 0 = piece / 1 = opaque, id), sorted.
    Hashes(Vec<(u128, u8, u64)>),
}

impl TBlock {
    pub fn bytes(&self) -> usize {
        64 + match self {
            TBlock::Pieces(v) => v.len() * 48,
            TBlock::Opaques(v) => v.len() * 48,
            TBlock::Hashes(v) => v.len() * 32,
        }
    }
}

/// Block kinds, for caching blocks by (file, kind, index).
pub const PIECES: u8 = 0;
pub const OPAQUES: u8 = 1;
pub const HASHES: u8 = 2;

/// What every open store keeps in memory about a blob file.
#[derive(Debug, Clone)]
pub struct BlobCat {
    pub piece_lo: u32,
    pub n_pieces: u32,
    pub opaque_lo: u64,
    pub n_opaques: u64,
    /// Over the low 64 bits of every piece and opaque hash.
    pub bloom: Bloom,
    pub piece_blocks: Vec<BlockRef>,
    pub opaque_blocks: Vec<BlockRef>,
    /// (first hash, first tag, block) per hash index block.
    pub hash_blocks: Vec<(u128, u8, BlockRef)>,
    pub opaque_bytes: u64,
    pub piece_bytes: u64,
    pub piece_raw_bytes: u64,
    /// Opaque values not kept.
    pub n_dropped: u64,
}

impl BlobCat {
    /// Bytes this catalog takes in memory, roughly.
    pub fn bytes(&self) -> usize {
        128 + self.bloom.bytes() + (self.piece_blocks.len() + self.opaque_blocks.len()) * 24 + self.hash_blocks.len() * 48
    }

    /// The hash index block that would hold the last entry for (`hash`, `tag`).
    pub fn hash_block_for(&self, tag: u8, hash: u128) -> Option<usize> {
        let i = self.hash_blocks.partition_point(|b| (b.0, b.1) <= (hash, tag));
        i.checked_sub(1)
    }
}

/// The last id with (`hash`, `tag`) in a sorted hash block (several opaque copies can share
/// a hash: one not kept, one kept later; the last wins).
pub fn find_in(v: &[(u128, u8, u64)], tag: u8, hash: u128) -> Option<u64> {
    let i = v.partition_point(|x| (x.0, x.1) <= (hash, tag));
    (i > 0 && (v[i - 1].0, v[i - 1].1) == (hash, tag)).then(|| v[i - 1].2)
}

fn encode_pieces(v: &[PieceEnt]) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(v.len() as u64);
    for p in v {
        e.u8(p.kind);
        e.u128(p.hash);
        e.var(p.off);
        e.var(p.clen as u64);
        e.var(p.rawlen as u64);
    }
    e.buf
}

fn encode_opaques(v: &[OpaqueEnt]) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(v.len() as u64);
    for o in v {
        e.u8(o.enc);
        e.u8(o.kind | if o.dropped { 0x80 } else { 0 });
        e.u128(o.hash);
        e.var(o.off);
        e.var(o.len as u64);
    }
    e.buf
}

fn encode_hashes(v: &[(u128, u8, u64)]) -> Vec<u8> {
    let mut e = Enc::new();
    e.var(v.len() as u64);
    for &(h, t, id) in v {
        e.u128(h);
        e.u8(t);
        e.var(id);
    }
    e.buf
}

fn decode_block(kind: u8, b: &[u8]) -> Result<TBlock> {
    let mut d = Dec::new(b);
    let n = d.var()? as usize;
    if n > ID_BLOCK.max(HASH_BLOCK) {
        return corrupt("blob table block too long");
    }
    let t = match kind {
        PIECES => {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(PieceEnt { kind: d.u8()?, hash: d.u128()?, off: d.var()?, clen: d.var32()?, rawlen: d.var32()? });
            }
            TBlock::Pieces(v)
        }
        OPAQUES => {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let (enc, k) = (d.u8()?, d.u8()?);
                v.push(OpaqueEnt { enc, kind: k & 0x7f, dropped: k & 0x80 != 0, hash: d.u128()?, off: d.var()?, len: d.var32()? });
            }
            TBlock::Opaques(v)
        }
        _ => {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push((d.u128()?, d.u8()?, d.var()?));
            }
            TBlock::Hashes(v)
        }
    };
    if !d.done() {
        return corrupt("trailing bytes in a blob table block");
    }
    Ok(t)
}

pub struct BlobWriter {
    f: BufWriter<std::fs::File>,
    pos: u64,
    comp: zstd::bulk::Compressor<'static>,
    pieces: Vec<PieceEnt>,
    opaques: Vec<OpaqueEnt>,
    cat: BlobCat,
}

impl BlobWriter {
    pub fn create(dir: &Path, num: u32, level: i32, piece_lo: u32, opaque_lo: u64) -> Result<BlobWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(blob_path(dir, num))?;
        let mut f = BufWriter::with_capacity(1 << 20, f);
        f.write_all(MAGIC)?;
        let mut comp = zstd::bulk::Compressor::new(level)?;
        comp.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        let cat = BlobCat {
            piece_lo,
            n_pieces: 0,
            opaque_lo,
            n_opaques: 0,
            bloom: Bloom::default(),
            piece_blocks: Vec::new(),
            opaque_blocks: Vec::new(),
            hash_blocks: Vec::new(),
            opaque_bytes: 0,
            piece_bytes: 0,
            piece_raw_bytes: 0,
            n_dropped: 0,
        };
        Ok(BlobWriter { f, pos: 8, comp, pieces: Vec::new(), opaques: Vec::new(), cat })
    }

    pub fn piece(&mut self, kind: u8, hash: u128, raw: &[u8]) -> Result<()> {
        let c = self.comp.compress(raw)?;
        self.f.write_all(&c)?;
        self.pieces.push(PieceEnt { kind, hash, off: self.pos, clen: c.len() as u32, rawlen: raw.len() as u32 });
        self.pos += c.len() as u64;
        self.cat.piece_bytes += c.len() as u64;
        self.cat.piece_raw_bytes += raw.len() as u64;
        Ok(())
    }

    /// Make room for `n` opaque values; [`BlobWriter::opaque`] then fills them in any order,
    /// so values can sit in the order they are read rather than the order they arrived.
    pub fn opaques(&mut self, n: usize) {
        self.opaques = vec![OpaqueEnt { enc: 0, kind: 0, dropped: true, hash: 0, off: 0, len: 0 }; n];
    }

    /// Opaque value `i` of this file (id `opaque_lo + i`).
    pub fn opaque(&mut self, i: usize, enc: u8, kind: u8, hash: u128, payload: &[u8]) -> Result<()> {
        self.f.write_all(payload)?;
        self.opaques[i] = OpaqueEnt { enc, kind, dropped: false, hash, off: self.pos, len: payload.len() as u32 };
        self.pos += payload.len() as u64;
        self.cat.opaque_bytes += payload.len() as u64;
        Ok(())
    }

    pub fn dropped(&mut self, i: usize, enc: u8, kind: u8, hash: u128, len: u32) {
        self.opaques[i] = OpaqueEnt { enc, kind, dropped: true, hash, off: 0, len };
        self.cat.n_dropped += 1;
    }

    pub fn finish(mut self, mode: SyncMode) -> Result<()> {
        for c in self.pieces.chunks(ID_BLOCK) {
            let r = write_block(&mut self.f, &mut self.pos, &encode_pieces(c), 3)?;
            self.cat.piece_blocks.push(r);
        }
        for c in self.opaques.chunks(ID_BLOCK) {
            let r = write_block(&mut self.f, &mut self.pos, &encode_opaques(c), 3)?;
            self.cat.opaque_blocks.push(r);
        }
        let mut by_hash: Vec<(u128, u8, u64)> = self.pieces.iter().enumerate().map(|(i, p)| (p.hash, 0, self.cat.piece_lo as u64 + i as u64)).collect();
        by_hash.extend(self.opaques.iter().enumerate().map(|(i, o)| (o.hash, 1, self.cat.opaque_lo + i as u64)));
        by_hash.sort_unstable();
        for c in by_hash.chunks(HASH_BLOCK) {
            let r = write_block(&mut self.f, &mut self.pos, &encode_hashes(c), 3)?;
            self.cat.hash_blocks.push((c[0].0, c[0].1, r));
        }
        let mut bloom = Bloom::new(by_hash.len(), BLOOM_BITS);
        for x in &by_hash {
            bloom.add(x.0 as u64);
        }
        self.cat.bloom = bloom;
        self.cat.n_pieces = self.pieces.len() as u32;
        self.cat.n_opaques = self.opaques.len() as u64;
        let mut e = Enc::new();
        let c = &self.cat;
        e.var(c.piece_lo as u64);
        e.var(c.n_pieces as u64);
        e.var(c.opaque_lo);
        e.var(c.n_opaques);
        c.bloom.encode(&mut e);
        for list in [&c.piece_blocks, &c.opaque_blocks] {
            e.var(list.len() as u64);
            for r in list {
                r.encode(&mut e);
            }
        }
        e.var(c.hash_blocks.len() as u64);
        for (h, t, r) in &c.hash_blocks {
            e.u128(*h);
            e.u8(*t);
            r.encode(&mut e);
        }
        e.var(c.opaque_bytes);
        e.var(c.piece_bytes);
        e.var(c.piece_raw_bytes);
        e.var(c.n_dropped);
        write_catalog(&mut self.f, &mut self.pos, &e.buf, MAGIC)?;
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        sync_file(&f, mode)
    }
}

pub struct Blob {
    pub num: u32,
    uid: u64,
    path: PathBuf,
    pub cat: BlobCat,
    pub file_bytes: u64,
}

impl Blob {
    pub fn open(dir: &Path, num: u32) -> Result<Blob> {
        let path = blob_path(dir, num);
        let f = file(&path)?;
        let (c, file_bytes) = read_catalog(&f, MAGIC, &format!("blob file {num}"))?;
        let mut d = Dec::new(&c);
        let (piece_lo, n_pieces, opaque_lo, n_opaques) = (d.var32()?, d.var32()?, d.var()?, d.var()?);
        let bloom = Bloom::decode(&mut d)?;
        let mut lists = [Vec::new(), Vec::new()];
        for l in lists.iter_mut() {
            for _ in 0..d.var()? {
                l.push(BlockRef::decode(&mut d)?);
            }
        }
        let [piece_blocks, opaque_blocks] = lists;
        let mut hash_blocks = Vec::new();
        for _ in 0..d.var()? {
            hash_blocks.push((d.u128()?, d.u8()?, BlockRef::decode(&mut d)?));
        }
        let cat = BlobCat {
            piece_lo,
            n_pieces,
            opaque_lo,
            n_opaques,
            bloom,
            piece_blocks,
            opaque_blocks,
            hash_blocks,
            opaque_bytes: d.var()?,
            piece_bytes: d.var()?,
            piece_raw_bytes: d.var()?,
            n_dropped: d.var()?,
        };
        if !d.done() || cat.piece_blocks.len() != (cat.n_pieces as usize).div_ceil(ID_BLOCK) || cat.opaque_blocks.len() as u64 != cat.n_opaques.div_ceil(ID_BLOCK as u64) {
            return corrupt(format!("blob file {num}: bad catalog"));
        }
        Ok(Blob { num, uid: crate::block::uid(), path, cat, file_bytes })
    }

    /// Table block `i` of kind `kind` ([`PIECES`], [`OPAQUES`] or [`HASHES`]).
    pub fn table_block(&self, kind: u8, i: usize) -> Result<TBlock> {
        let r = match kind {
            PIECES => self.cat.piece_blocks.get(i),
            OPAQUES => self.cat.opaque_blocks.get(i),
            _ => self.cat.hash_blocks.get(i).map(|x| &x.2),
        }
        .ok_or_else(|| crate::codec::Error::Corrupt(format!("blob file {} has no table block {i}", self.num)))?;
        decode_block(kind, &read_block(&*handle(self.uid, &self.path)?, r, &format!("blob file {}", self.num))?)
    }

    pub fn read(&self, off: u64, len: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        handle(self.uid, &self.path)?.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }

    pub fn has_piece(&self, id: u32) -> bool {
        id >= self.cat.piece_lo && id - self.cat.piece_lo < self.cat.n_pieces
    }

    pub fn has_opaque(&self, id: u64) -> bool {
        id >= self.cat.opaque_lo && id - self.cat.opaque_lo < self.cat.n_opaques
    }
}
