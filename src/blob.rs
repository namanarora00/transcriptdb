//! Blob files: the bytes of the shared pieces (request skeletons, system prompts, tool
//! lists, cache markers; zstd, one frame each) and of the opaque values (thinking
//! signatures, images; raw), appended at each flush in the order requests read them.
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
    comp: zstd::bulk::Compressor<'static>,
}

impl BlobAppend {
    /// Open blob file `num` to append at `len` (0 for a new file), cutting anything after.
    pub fn open(dir: &Path, num: u32, len: u64, level: i32) -> Result<BlobAppend> {
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
        let mut comp = zstd::bulk::Compressor::new(level)?;
        comp.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        Ok(BlobAppend { f: BufWriter::with_capacity(1 << 20, f), pos, comp })
    }

    /// Append a piece, compressed; returns (offset, compressed length).
    pub fn piece(&mut self, raw: &[u8]) -> Result<(u64, u32)> {
        let c = self.comp.compress(raw)?;
        self.f.write_all(&c)?;
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

/// `len` bytes at `off` of a blob file opened through the shared file cache.
pub fn read(uid: u64, path: &Path, off: u64, len: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    handle(uid, path)?.read_exact_at(&mut buf, off)?;
    Ok(buf)
}
