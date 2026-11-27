//! The manifest: which segment, index and blob files are live (and how long each blob
//! file is), what each segment still holds that a fold replaced, the id counters the
//! files cover, and where log replay starts.
//!
//! It is replaced atomically: written to `MANIFEST.tmp`, made durable, renamed over
//! `MANIFEST`, and the directory made durable. Readers see the old list or the new
//! one, never half of either.

use crate::codec::{corrupt, Dec, Enc, Result};
use crate::wal::{sync_dir, sync_file, SyncMode};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: &[u8; 8] = b"TDBMAN05";

/// Session parts in a segment that a fold replaced: they stay in the file, unread, until
/// the segment is rewritten. Counted so the store's totals leave them out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Dead {
    /// File bytes the parts take, roughly (their frames and their share of blocks).
    pub bytes: u64,
    pub requests: u64,
    pub nodes: u64,
    pub steps: u64,
    pub node_bytes: u64,
    pub frame_bytes: u64,
}

impl Dead {
    pub fn add(&mut self, o: &Dead) {
        self.bytes += o.bytes;
        self.requests += o.requests;
        self.nodes += o.nodes;
        self.steps += o.steps;
        self.node_bytes += o.node_bytes;
        self.frame_bytes += o.frame_bytes;
    }
}

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
    /// Live segments holding replaced parts, by segment number.
    pub dead: Vec<(u32, Dead)>,
    /// Index runs in id order (oldest first).
    pub runs: Vec<u32>,
    /// Blob files in order, with the length each has been made durable at.
    pub blobs: Vec<(u32, u64)>,
    /// Ids below these are in files: sessions, pieces, opaque values.
    pub sessions: u32,
    pub pieces: u32,
    pub opaques: u64,
    /// Sessions deleted, as sorted ranges `[a, b)` that do not touch: nothing of them is
    /// read again, merges leave them out, and [`crate::Db::vacuum`] removes their last traces
    /// (and empties this).
    pub dropped: Vec<(u32, u32)>,
}

impl Manifest {
    pub fn is_dropped(&self, s: u32) -> bool {
        let i = self.dropped.partition_point(|r| r.1 <= s);
        self.dropped.get(i).is_some_and(|r| r.0 <= s)
    }

    /// Mark session `s` deleted.
    pub fn drop_session(&mut self, s: u32) {
        if self.is_dropped(s) {
            return;
        }
        let i = self.dropped.partition_point(|r| r.1 < s);
        // join the range ending at s and the one starting at s + 1, if there are such
        let joins_prev = i < self.dropped.len() && self.dropped[i].1 == s;
        let next = if joins_prev { i + 1 } else { i };
        let joins_next = next < self.dropped.len() && self.dropped[next].0 == s + 1;
        match (joins_prev, joins_next) {
            (true, true) => {
                self.dropped[i].1 = self.dropped[next].1;
                self.dropped.remove(next);
            }
            (true, false) => self.dropped[i].1 = s + 1,
            (false, true) => self.dropped[next].0 = s,
            (false, false) => self.dropped.insert(i, (s, s + 1)),
        }
    }

    /// How many sessions are deleted.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.iter().map(|r| (r.1 - r.0) as u64).sum()
    }

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
        let mut dead = Vec::new();
        for _ in 0..d.var()? {
            let n = d.var32()?;
            dead.push((n, Dead { bytes: d.var()?, requests: d.var()?, nodes: d.var()?, steps: d.var()?, node_bytes: d.var()?, frame_bytes: d.var()? }));
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
        let mut dropped = Vec::new();
        let mut at = 0u32;
        for _ in 0..d.var()? {
            let a = at.checked_add(d.var32()?).ok_or_else(|| crate::codec::Error::Corrupt("MANIFEST: bad deleted sessions".into()))?;
            let b = a.checked_add(d.var32()?).ok_or_else(|| crate::codec::Error::Corrupt("MANIFEST: bad deleted sessions".into()))?;
            dropped.push((a, b));
            at = b;
        }
        if !d.done() {
            return corrupt("MANIFEST has trailing bytes");
        }
        Ok(Some(Manifest { generation, checkpoint, wal_start, next_file, segs, dead, runs, blobs, sessions, pieces, opaques, dropped }))
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
        e.var(self.dead.len() as u64);
        for (n, x) in &self.dead {
            e.var(*n as u64);
            for v in [x.bytes, x.requests, x.nodes, x.steps, x.node_bytes, x.frame_bytes] {
                e.var(v);
            }
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
        e.var(self.dropped.len() as u64);
        let mut at = 0u32;
        for &(a, b) in &self.dropped {
            e.var((a - at) as u64);
            e.var((b - a) as u64);
            at = b;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropped_ranges_join() {
        let mut m = Manifest::default();
        for s in [5, 3, 4, 9, 1, 2, 7] {
            m.drop_session(s);
        }
        assert_eq!(m.dropped, vec![(1, 6), (7, 8), (9, 10)]);
        m.drop_session(8);
        assert_eq!(m.dropped, vec![(1, 6), (7, 10)]);
        m.drop_session(6);
        assert_eq!(m.dropped, vec![(1, 10)]);
        assert!(m.is_dropped(1) && m.is_dropped(9) && !m.is_dropped(0) && !m.is_dropped(10));
        assert_eq!(m.dropped_count(), 9);
    }
}
