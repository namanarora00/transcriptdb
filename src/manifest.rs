//! The manifest: which segment, index and blob files are live (and how long each blob
//! file is), the id counters they cover, and where log replay starts.
//!
//! It is replaced atomically: written to `MANIFEST.tmp`, made durable, renamed over
//! `MANIFEST`, and the directory made durable. Readers see the old list or the new
//! one, never half of either.

use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_dir, sync_file, SyncMode};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: &[u8; 8] = b"TDBMAN03";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    /// Every log record with `seq <= checkpoint` is already in a segment.
    pub checkpoint: u64,
    /// Log files numbered below this hold nothing that is not in a segment.
    pub wal_start: u64,
    /// Number for the next segment, index or blob file.
    pub next_file: u32,
    pub segs: Vec<u32>,
    /// Index runs in id order (oldest first).
    pub runs: Vec<u32>,
    /// Blob files in order, with the length each has been made durable at.
    pub blobs: Vec<(u32, u64)>,
    /// Ids below these are in files: sessions, pieces, opaque values.
    pub sessions: u32,
    pub pieces: u32,
    pub opaques: u64,
}

impl Manifest {
    pub fn load(dir: &Path) -> Result<Option<Manifest>> {
        let p = dir.join("MANIFEST");
        let data = match fs::read(&p) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if data.len() < 12 || &data[..8] != MAGIC {
            return corrupt("MANIFEST has a bad header");
        }
        let (body, crc) = data.split_at(data.len() - 4);
        if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return corrupt("MANIFEST checksum mismatch");
        }
        let mut d = Dec::new(&body[8..]);
        let generation = d.var()?;
        let checkpoint = d.var()?;
        let wal_start = d.var()?;
        let next_file = d.var32()?;
        let mut segs = Vec::new();
        for _ in 0..d.var()? {
            segs.push(d.var32()?);
        }
        let mut runs = Vec::new();
        for _ in 0..d.var()? {
            runs.push(d.var32()?);
        }
        let mut blobs = Vec::new();
        for _ in 0..d.var()? {
            blobs.push((d.var32()?, d.var()?));
        }
        let (sessions, pieces, opaques) = (d.var32()?, d.var32()?, d.var()?);
        if !d.done() {
            return corrupt("MANIFEST has trailing bytes");
        }
        Ok(Some(Manifest { generation, checkpoint, wal_start, next_file, segs, runs, blobs, sessions, pieces, opaques }))
    }

    pub fn store(&self, dir: &Path, mode: SyncMode) -> Result<()> {
        let mut e = Enc::new();
        e.raw(MAGIC);
        e.var(self.generation);
        e.var(self.checkpoint);
        e.var(self.wal_start);
        e.var(self.next_file as u64);
        e.var(self.segs.len() as u64);
        for &s in &self.segs {
            e.var(s as u64);
        }
        e.var(self.runs.len() as u64);
        for &r in &self.runs {
            e.var(r as u64);
        }
        e.var(self.blobs.len() as u64);
        for &(b, len) in &self.blobs {
            e.var(b as u64);
            e.var(len);
        }
        e.var(self.sessions as u64);
        e.var(self.pieces as u64);
        e.var(self.opaques);
        let crc = crc32c::crc32c(&e.buf);
        e.raw(&crc.to_le_bytes());
        let tmp = dir.join("MANIFEST.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&e.buf)?;
            sync_file(&f, mode)?;
        }
        fs::rename(&tmp, dir.join("MANIFEST"))?;
        sync_dir(dir, mode)
    }
}
