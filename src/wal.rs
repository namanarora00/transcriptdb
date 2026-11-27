//! The write-ahead log: framed, checksummed records, made durable before "saved".
//!
//! File layout: an 8-byte magic, then records of `[len u32][crc32c u32][payload]`.
//! A crash can leave a torn last record; [`read_wal`] stops there and reports
//! where the valid prefix ends so the caller can cut the file back.

use crate::codec::{corrupt, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const MAGIC: &[u8; 8] = b"TDBWAL04";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// `F_FULLFSYNC` on macOS (`fsync` elsewhere): survives power loss.
    Full,
    /// Plain `fsync`.
    Fsync,
    /// Only `write`: survives the process being killed, not the machine losing power.
    Os,
}

pub fn sync_file(f: &File, mode: SyncMode) -> Result<()> {
    match mode {
        SyncMode::Full => f.sync_all()?,
        SyncMode::Fsync => {
            use std::os::fd::AsRawFd;
            if unsafe { libc::fsync(f.as_raw_fd()) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        SyncMode::Os => {}
    }
    Ok(())
}

pub fn sync_dir(dir: &Path, mode: SyncMode) -> Result<()> {
    if mode == SyncMode::Os {
        return Ok(());
    }
    let d = File::open(dir)?;
    sync_file(&d, mode)
}

pub fn wal_path(dir: &Path, num: u64) -> PathBuf {
    dir.join(format!("wal-{num:06}.log"))
}

pub struct WalWriter {
    file: File,
    pub num: u64,
    pending: Vec<u8>,
    mode: SyncMode,
    pub bytes: u64,
}

impl WalWriter {
    pub fn create(dir: &Path, num: u64, mode: SyncMode) -> Result<WalWriter> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(wal_path(dir, num))?;
        file.write_all(MAGIC)?;
        sync_file(&file, mode)?;
        sync_dir(dir, mode)?;
        Ok(WalWriter { file, num, pending: Vec::new(), mode, bytes: MAGIC.len() as u64 })
    }

    pub fn append(&mut self, payload: &[u8]) {
        self.pending.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.pending.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        self.pending.extend_from_slice(payload);
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Write everything appended so far and make it durable.
    pub fn commit(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.pending)?;
        self.bytes += self.pending.len() as u64;
        self.pending.clear();
        sync_file(&self.file, self.mode)
    }
}

pub struct WalContents {
    pub records: Vec<Vec<u8>>,
    /// Length of the valid prefix of the file.
    pub valid_len: u64,
    /// True if bytes after the valid prefix were ignored (a torn or bad last record).
    pub torn: bool,
}

pub fn read_wal(path: &Path) -> Result<WalContents> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    if data.len() < MAGIC.len() {
        // created but the magic never made it: an empty log
        return Ok(WalContents { records: Vec::new(), valid_len: 0, torn: !data.is_empty() });
    }
    if &data[..8] != MAGIC {
        return corrupt(format!("{} is not a write-ahead log", path.display()));
    }
    let mut i = 8usize;
    let mut records = Vec::new();
    while i < data.len() {
        if data.len() - i < 8 {
            break;
        }
        let len = u32::from_le_bytes(data[i..i + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[i + 4..i + 8].try_into().unwrap());
        if data.len() - i - 8 < len {
            break;
        }
        let payload = &data[i + 8..i + 8 + len];
        if crc32c::crc32c(payload) != crc {
            break;
        }
        records.push(payload.to_vec());
        i += 8 + len;
    }
    Ok(WalContents { records, valid_len: i as u64, torn: i < data.len() })
}

/// Cut a log back to its valid prefix.
pub fn truncate(path: &Path, len: u64, mode: SyncMode) -> Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    sync_file(&f, mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn torn_tail_is_dropped() {
        let dir = std::env::temp_dir().join(format!("tdb-wal-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut w = WalWriter::create(&dir, 1, SyncMode::Os).unwrap();
        w.append(b"one");
        w.append(b"two");
        w.commit().unwrap();
        drop(w);
        let p = wal_path(&dir, 1);
        // a torn third record
        let mut f = OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(&[9, 0, 0, 0, 1, 2]).unwrap();
        drop(f);
        let c = read_wal(&p).unwrap();
        assert_eq!(c.records, vec![b"one".to_vec(), b"two".to_vec()]);
        assert!(c.torn);
        truncate(&p, c.valid_len, SyncMode::Os).unwrap();
        let c2 = read_wal(&p).unwrap();
        assert!(!c2.torn);
        assert_eq!(c2.records.len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
