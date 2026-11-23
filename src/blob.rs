//! Blob files: the bytes of the shared pieces (request skeletons, system prompts, tool
//! lists, cache markers; zstd, one frame each, most of them compressed against an earlier
//! piece of their kind) and of the opaque values (thinking signatures, images; raw),
//! appended at each flush in the order requests read them.
//!
//! A file grows until it passes `Options::blob_bytes`; the next flush then starts a new one,
//! so the number of blob files follows the data's size, not the number of flushes. Nothing
//! in a blob file describes it: the index says where each value is and the manifest says
//! how long each file is. Bytes past that length come from a flush that did not finish and
//! are cut when the store opens. Merges never rewrite blob files: they hold over half of
//! the bytes and none of the session structure.
//!
//! `blob-N.dat`: magic, then payloads.

use crate::block::handle;
use crate::codec::{corrupt, Result};
use crate::wal::{sync_file, SyncMode};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"TDBBLB03";

pub fn blob_path(dir: &Path, num: u32) -> PathBuf {
    dir.join(format!("blob-{num:06}.dat"))
}

/// Appends one flush's payloads to a blob file.
pub struct BlobAppend {
    f: BufWriter<File>,
    pos: u64,
}

impl BlobAppend {
    /// Open blob file `num` to append at `len` (0 for a new file), cutting anything after.
    pub fn open(dir: &Path, num: u32, len: u64) -> Result<BlobAppend> {
        let path = blob_path(dir, num);
        let (f, pos) = if len == 0 {
            let mut f = OpenOptions::new().write(true).create(true).truncate(true).open(&path)?;
            f.write_all(MAGIC)?;
            (f, MAGIC.len() as u64)
        } else {
            let f = OpenOptions::new().append(true).open(&path)?;
            let have = f.metadata()?.len();
            if have < len {
                return corrupt(format!("blob file {num} is shorter than the manifest says"));
            }
            if have > len {
                f.set_len(len)?;
            }
            (f, len)
        };
        Ok(BlobAppend { f: BufWriter::with_capacity(1 << 20, f), pos })
    }

    /// Append a piece already compressed; returns (offset, length).
    pub fn packed(&mut self, c: &[u8]) -> Result<(u64, u32)> {
        self.f.write_all(c)?;
        let off = self.pos;
        self.pos += c.len() as u64;
        Ok((off, c.len() as u32))
    }

    /// Append an opaque value as it is; returns its offset.
    pub fn opaque(&mut self, payload: &[u8]) -> Result<u64> {
        self.f.write_all(payload)?;
        let off = self.pos;
        self.pos += payload.len() as u64;
        Ok(off)
    }

    /// Where the next byte goes.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Make what was appended durable; returns the file's new length.
    pub fn finish(self, mode: SyncMode) -> Result<u64> {
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        sync_file(&f, mode)?;
        Ok(self.pos)
    }
}

/// Check blob file `num` against the length the manifest gives it, and cut what a flush
/// that did not finish left after it; returns the bytes cut.
pub fn trim(dir: &Path, num: u32, len: u64, mode: SyncMode) -> Result<u64> {
    let path = blob_path(dir, num);
    let f = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return corrupt(format!("blob file {num} is missing")),
        Err(e) => return Err(e.into()),
    };
    let have = f.metadata()?.len();
    let mut head = [0u8; 8];
    if have < len || len < 8 || f.read_exact_at(&mut head, 0).is_err() || &head != MAGIC {
        return corrupt(format!("blob file {num} is shorter than the manifest says, or not a blob file of this version"));
    }
    if have > len {
        f.set_len(len)?;
        sync_file(&f, mode)?;
    }
    Ok(have - len)
}

fn zerr(e: usize) -> crate::codec::Error {
    crate::codec::Error::Corrupt(format!("zstd: {}", zstd::zstd_safe::get_error_name(e)))
}

/// `raw` compressed with zstd (with its checksum), against `base` as a raw-content
/// dictionary when given.
pub fn pack(raw: &[u8], base: Option<&[u8]>, level: i32) -> Result<Vec<u8>> {
    use zstd::zstd_safe::{CCtx, CParameter};
    let mut c = CCtx::create();
    c.set_parameter(CParameter::CompressionLevel(level)).map_err(zerr)?;
    c.set_parameter(CParameter::ChecksumFlag(true)).map_err(zerr)?;
    if let Some(b) = base {
        // the window must reach back over the whole base (readers accept up to 2^27)
        let need = (b.len() + raw.len()).next_power_of_two().trailing_zeros().clamp(10, 27);
        c.set_parameter(CParameter::WindowLog(need)).map_err(zerr)?;
        // long matches against the base, which a fast match finder's tables can lose
        c.set_parameter(CParameter::EnableLongDistanceMatching(true)).map_err(zerr)?;
        c.ref_prefix(b).map_err(zerr)?;
    }
    let mut out = Vec::with_capacity(zstd::zstd_safe::compress_bound(raw.len()));
    c.compress2(&mut out, raw).map_err(zerr)?;
    Ok(out)
}

/// [`pack`] undone: `cap` bytes expected.
pub fn unpack(c: &[u8], base: Option<&[u8]>, cap: usize) -> Result<Vec<u8>> {
    use zstd::zstd_safe::DCtx;
    let mut d = DCtx::create();
    if let Some(b) = base {
        d.ref_prefix(b).map_err(zerr)?;
    }
    let mut out = Vec::with_capacity(cap);
    d.decompress(&mut out, c).map_err(zerr)?;
    Ok(out)
}

/// Compress a new piece, against whichever of `bases` (earlier pieces of its kind, newest
/// last) makes it smallest when that at least halves it; returns (base id + 1 or 0,
/// compressed bytes).
pub fn pack_piece(raw: &[u8], bases: &[(u32, &[u8])], level: i32) -> Result<(u32, Vec<u8>)> {
    let alone = pack(raw, None, 3)?.len();
    let mut best: Option<(usize, u32, &[u8])> = None;
    for &(id, b) in bases {
        let n = pack(raw, Some(b), 3)?.len();
        if best.is_none_or(|x| n < x.0) {
            best = Some((n, id, b));
        }
    }
    match best {
        Some((n, id, b)) if n * 2 <= alone => Ok((id + 1, pack(raw, Some(b), level)?)),
        _ => Ok((0, pack(raw, None, level)?)),
    }
}

/// `len` bytes at `off` of a blob file opened through the shared file cache.
pub fn read(uid: u64, path: &Path, off: u64, len: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    handle(uid, path)?.read_exact_at(&mut buf, off)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pieces_pack_alone_or_against_an_earlier_one() {
        let a: Vec<u8> = (0..60_000u32).flat_map(|i| format!("tool {i} reads files; ").into_bytes()).collect();
        let mut b = a.clone();
        b[30_000..30_010].copy_from_slice(b"CHANGED!!!");
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let unrelated: Vec<u8> = (0..20_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        // nothing to compress against: whole
        let (base, c) = pack_piece(&a, &[], 19).unwrap();
        assert_eq!(base, 0);
        assert_eq!(unpack(&c, None, a.len()).unwrap(), a);
        // a near copy is stored against the most similar earlier piece
        let (base, c) = pack_piece(&b, &[(4, &unrelated), (7, &a)], 19).unwrap();
        assert_eq!(base, 8);
        assert!(c.len() < 200, "delta of a near copy is {} bytes", c.len());
        assert_eq!(unpack(&c, Some(&a), b.len()).unwrap(), b);
        assert!(unpack(&c, Some(&unrelated), b.len()).is_err() || unpack(&c, Some(&unrelated), b.len()).unwrap() != b);
        // a base that does not halve it is not used
        let (base, _) = pack_piece(&unrelated, &[(7, &a)], 19).unwrap();
        assert_eq!(base, 0);
    }
}
