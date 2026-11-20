//! The manifest: which segment files are live and where log replay starts.
//!
//! It is replaced atomically: written to `MANIFEST.tmp`, made durable, renamed over
//! `MANIFEST`, and the directory made durable. Readers see the old list or the new
//! one, never half of either.

use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_dir, sync_file, SyncMode};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: &[u8; 8] = b"TDBMAN01";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    /// Every log record with `seq <= checkpoint` is already in a segment.
    pub checkpoint: u64,
    /// Log files numbered below this hold nothing that is not in a segment.
    pub wal_start: u64,
    pub next_seg: u32,
    pub segs: Vec<u32>,
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
        let next_seg = d.var32()?;
        let n = d.var()? as usize;
        let mut segs = Vec::with_capacity(n);
        for _ in 0..n {
            segs.push(d.var32()?);
        }
        Ok(Some(Manifest { generation, checkpoint, wal_start, next_seg, segs }))
    }

    pub fn store(&self, dir: &Path, mode: SyncMode) -> Result<()> {
        let mut e = Enc::new();
        e.raw(MAGIC);
        e.var(self.generation);
        e.var(self.checkpoint);
        e.var(self.wal_start);
        e.var(self.next_seg as u64);
        e.var(self.segs.len() as u64);
        for &s in &self.segs {
            e.var(s as u64);
        }
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
