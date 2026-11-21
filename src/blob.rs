//! Blob files: the shared pieces (request skeletons, system prompts, tool lists, cache
//! markers) and the opaque values (thinking signatures, images) that arrived between two
//! flushes. Ids are dense and each file holds one contiguous range of piece ids and one of
//! opaque ids. Merges never rewrite blob files: they hold over half of the bytes and none
//! of the session structure.
//!
//! `blob-N.tdb`: magic, payloads (opaque values raw, pieces zstd-compressed one by one),
//! the table block, then the catalog (id ranges, a Bloom filter of hashes, where the table
//! is) and the footer.

use crate::block::{file, handle, read_block, read_catalog, write_block, write_catalog, BlockRef};
use crate::bloom::Bloom;
use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_file, SyncMode};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"TDBBLB01";

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

/// What a blob file holds, loaded on demand.
#[derive(Debug, Default)]
pub struct BlobTable {
    pub piece_lo: u32,
    pub pieces: Vec<PieceEnt>,
    pub opaque_lo: u64,
    pub opaques: Vec<OpaqueEnt>,
    /// (hash, 0 = piece / 1 = opaque, id), sorted: finds an existing copy by content.
    pub by_hash: Vec<(u128, u8, u64)>,
}

impl BlobTable {
    pub fn bytes(&self) -> usize {
        self.pieces.len() * 48 + self.opaques.len() * 48 + self.by_hash.len() * 32
    }

    pub fn find(&self, tag: u8, hash: u128) -> Option<u64> {
        let i = self.by_hash.partition_point(|x| (x.0, x.1) < (hash, tag));
        // several opaque copies can share a hash (one not kept, one kept later): the last wins
        let mut found = None;
        for x in &self.by_hash[i..] {
            if (x.0, x.1) != (hash, tag) {
                break;
            }
            found = Some(x.2);
        }
        found
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.var(self.piece_lo as u64);
        e.var(self.pieces.len() as u64);
        for p in &self.pieces {
            e.u8(p.kind);
            e.u128(p.hash);
            e.var(p.off);
            e.var(p.clen as u64);
            e.var(p.rawlen as u64);
        }
        e.var(self.opaque_lo);
        e.var(self.opaques.len() as u64);
        for o in &self.opaques {
            e.u8(o.enc);
            e.u8(o.kind | if o.dropped { 0x80 } else { 0 });
            e.u128(o.hash);
            e.var(o.off);
            e.var(o.len as u64);
        }
        e.buf
    }

    fn decode(b: &[u8]) -> Result<BlobTable> {
        let mut d = Dec::new(b);
        let piece_lo = d.var32()?;
        let n = d.var()? as usize;
        let mut pieces = Vec::with_capacity(n.min(1 << 20));
        for _ in 0..n {
            pieces.push(PieceEnt { kind: d.u8()?, hash: d.u128()?, off: d.var()?, clen: d.var32()?, rawlen: d.var32()? });
        }
        let opaque_lo = d.var()?;
        let n = d.var()? as usize;
        let mut opaques = Vec::with_capacity(n.min(1 << 20));
        for _ in 0..n {
            let (enc, k) = (d.u8()?, d.u8()?);
            opaques.push(OpaqueEnt { enc, kind: k & 0x7f, dropped: k & 0x80 != 0, hash: d.u128()?, off: d.var()?, len: d.var32()? });
        }
        if !d.done() {
            return corrupt("trailing bytes in a blob table");
        }
        let mut by_hash: Vec<(u128, u8, u64)> = pieces.iter().enumerate().map(|(i, p)| (p.hash, 0, piece_lo as u64 + i as u64)).collect();
        by_hash.extend(opaques.iter().enumerate().map(|(i, o)| (o.hash, 1, opaque_lo + i as u64)));
        by_hash.sort_unstable();
        Ok(BlobTable { piece_lo, pieces, opaque_lo, opaques, by_hash })
    }
}

/// What every open store keeps in memory about a blob file.
#[derive(Debug, Clone)]
pub struct BlobCat {
    pub piece_lo: u32,
    pub n_pieces: u32,
    pub opaque_lo: u64,
    pub n_opaques: u64,
    /// Over the low 64 bits of every piece and opaque hash.
    pub bloom: Bloom,
    pub table: BlockRef,
    pub opaque_bytes: u64,
    pub piece_bytes: u64,
    pub piece_raw_bytes: u64,
    /// Opaque values not kept.
    pub n_dropped: u64,
}

pub struct BlobWriter {
    f: BufWriter<File>,
    pos: u64,
    comp: zstd::bulk::Compressor<'static>,
    t: BlobTable,
    cat: BlobCat,
}

impl BlobWriter {
    pub fn create(dir: &Path, num: u32, level: i32, piece_lo: u32, opaque_lo: u64) -> Result<BlobWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(blob_path(dir, num))?;
        let mut f = BufWriter::with_capacity(1 << 20, f);
        f.write_all(MAGIC)?;
        let mut comp = zstd::bulk::Compressor::new(level)?;
        comp.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        let t = BlobTable { piece_lo, opaque_lo, ..BlobTable::default() };
        let cat = BlobCat { piece_lo, n_pieces: 0, opaque_lo, n_opaques: 0, bloom: Bloom::default(), table: BlockRef::default(), opaque_bytes: 0, piece_bytes: 0, piece_raw_bytes: 0, n_dropped: 0 };
        Ok(BlobWriter { f, pos: 8, comp, t, cat })
    }

    pub fn piece(&mut self, kind: u8, hash: u128, raw: &[u8]) -> Result<()> {
        let c = self.comp.compress(raw)?;
        self.f.write_all(&c)?;
        self.t.pieces.push(PieceEnt { kind, hash, off: self.pos, clen: c.len() as u32, rawlen: raw.len() as u32 });
        self.pos += c.len() as u64;
        self.cat.piece_bytes += c.len() as u64;
        self.cat.piece_raw_bytes += raw.len() as u64;
        Ok(())
    }

    /// Make room for `n` opaque values; [`BlobWriter::opaque`] then fills them in any order,
    /// so values can sit in the order they are read rather than the order they arrived.
    pub fn opaques(&mut self, n: usize) {
        self.t.opaques = vec![OpaqueEnt { enc: 0, kind: 0, dropped: true, hash: 0, off: 0, len: 0 }; n];
    }

    /// Opaque value `i` of this file (id `opaque_lo + i`).
    pub fn opaque(&mut self, i: usize, enc: u8, kind: u8, hash: u128, payload: &[u8]) -> Result<()> {
        self.f.write_all(payload)?;
        self.t.opaques[i] = OpaqueEnt { enc, kind, dropped: false, hash, off: self.pos, len: payload.len() as u32 };
        self.pos += payload.len() as u64;
        self.cat.opaque_bytes += payload.len() as u64;
        Ok(())
    }

    pub fn dropped(&mut self, i: usize, enc: u8, kind: u8, hash: u128, len: u32) {
        self.t.opaques[i] = OpaqueEnt { enc, kind, dropped: true, hash, off: 0, len };
        self.cat.n_dropped += 1;
    }

    pub fn finish(mut self, mode: SyncMode) -> Result<()> {
        let raw = self.t.encode();
        self.cat.table = write_block(&mut self.f, &mut self.pos, &raw, 3)?;
        let mut bloom = Bloom::new(self.t.pieces.len() + self.t.opaques.len(), 10);
        for p in &self.t.pieces {
            bloom.add(p.hash as u64);
        }
        for o in &self.t.opaques {
            bloom.add(o.hash as u64);
        }
        self.cat.bloom = bloom;
        self.cat.n_pieces = self.t.pieces.len() as u32;
        self.cat.n_opaques = self.t.opaques.len() as u64;
        let mut e = Enc::new();
        let c = &self.cat;
        e.var(c.piece_lo as u64);
        e.var(c.n_pieces as u64);
        e.var(c.opaque_lo);
        e.var(c.n_opaques);
        c.bloom.encode(&mut e);
        c.table.encode(&mut e);
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
        let cat = BlobCat {
            piece_lo: d.var32()?,
            n_pieces: d.var32()?,
            opaque_lo: d.var()?,
            n_opaques: d.var()?,
            bloom: Bloom::decode(&mut d)?,
            table: BlockRef::decode(&mut d)?,
            opaque_bytes: d.var()?,
            piece_bytes: d.var()?,
            piece_raw_bytes: d.var()?,
            n_dropped: d.var()?,
        };
        Ok(Blob { num, uid: crate::block::uid(), path, cat, file_bytes })
    }

    pub fn table(&self) -> Result<BlobTable> {
        let t = BlobTable::decode(&read_block(&*handle(self.uid, &self.path)?, &self.cat.table, &format!("blob file {}", self.num))?)?;
        if t.pieces.len() as u32 != self.cat.n_pieces || t.opaques.len() as u64 != self.cat.n_opaques {
            return corrupt(format!("blob file {}: table does not match its catalog", self.num));
        }
        Ok(t)
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
