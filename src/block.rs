//! Compressed, checksummed blocks and the footer that points at a file's catalog. Shared
//! by segment files and blob files.

use crate::codec::{corrupt, Dec, Enc, Result};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockRef {
    pub off: u64,
    pub clen: u32,
    pub rawlen: u32,
    pub crc: u32,
}

impl BlockRef {
    pub fn encode(&self, e: &mut Enc) {
        e.var(self.off);
        e.var(self.clen as u64);
        e.var(self.rawlen as u64);
        e.raw(&self.crc.to_le_bytes());
    }
    pub fn decode(d: &mut Dec) -> Result<BlockRef> {
        Ok(BlockRef { off: d.var()?, clen: d.var32()?, rawlen: d.var32()?, crc: u32::from_le_bytes(d.raw(4)?.try_into().unwrap()) })
    }
}

/// Compress `raw` and write it at `*pos`.
pub fn write_block(w: &mut impl Write, pos: &mut u64, raw: &[u8], level: i32) -> Result<BlockRef> {
    write_packed(w, pos, &zstd::bulk::compress(raw, level)?, raw.len())
}

/// Write a block compressed by the caller (`rawlen` bytes once decompressed) at `*pos`.
pub fn write_packed(w: &mut impl Write, pos: &mut u64, c: &[u8], rawlen: usize) -> Result<BlockRef> {
    w.write_all(c)?;
    let r = BlockRef { off: *pos, clen: c.len() as u32, rawlen: rawlen as u32, crc: crc32c::crc32c(c) };
    *pos += c.len() as u64;
    Ok(r)
}

/// A block's compressed bytes, checksum checked.
pub fn read_packed(f: &File, r: &BlockRef, what: &str) -> Result<Vec<u8>> {
    let mut c = vec![0u8; r.clen as usize];
    f.read_exact_at(&mut c, r.off)?;
    if crc32c::crc32c(&c) != r.crc {
        return corrupt(format!("{what}: block checksum mismatch"));
    }
    Ok(c)
}

pub fn read_block(f: &File, r: &BlockRef, what: &str) -> Result<Vec<u8>> {
    let c = read_packed(f, r, what)?;
    let raw = crate::segment::decompress(&c, r.rawlen as usize)?;
    if raw.len() != r.rawlen as usize {
        return corrupt(format!("{what}: block has the wrong size"));
    }
    Ok(raw)
}

pub const FOOTER: usize = 8 + 8 + 4 + 8;

/// Write the catalog (uncompressed) and the footer that points at it.
pub fn write_catalog(w: &mut impl Write, pos: &mut u64, cat: &[u8], magic: &[u8; 8]) -> Result<()> {
    let at = *pos;
    w.write_all(cat)?;
    w.write_all(&at.to_le_bytes())?;
    w.write_all(&(cat.len() as u64).to_le_bytes())?;
    w.write_all(&crc32c::crc32c(cat).to_le_bytes())?;
    w.write_all(magic)?;
    *pos += cat.len() as u64 + FOOTER as u64;
    Ok(())
}

/// Check head and footer, and read the catalog.
pub fn read_catalog(f: &File, magic: &[u8; 8], what: &str) -> Result<(Vec<u8>, u64)> {
    let len = f.metadata()?.len();
    if len < (8 + FOOTER) as u64 {
        return corrupt(format!("{what} is too short"));
    }
    let mut head = [0u8; 8];
    f.read_exact_at(&mut head, 0)?;
    let mut foot = [0u8; FOOTER];
    f.read_exact_at(&mut foot, len - FOOTER as u64)?;
    if &head != magic || &foot[20..] != magic {
        return corrupt(format!("{what} has a bad magic (written by another version?)"));
    }
    let at = u64::from_le_bytes(foot[0..8].try_into().unwrap());
    let clen = u64::from_le_bytes(foot[8..16].try_into().unwrap());
    let crc = u32::from_le_bytes(foot[16..20].try_into().unwrap());
    if at + clen + FOOTER as u64 != len {
        return corrupt(format!("{what}: footer does not match the file size"));
    }
    let mut c = vec![0u8; clen as usize];
    f.read_exact_at(&mut c, at)?;
    if crc32c::crc32c(&c) != crc {
        return corrupt(format!("{what}: catalog checksum mismatch"));
    }
    Ok((c, len))
}

/// Open files, shared and capped: segment and blob files are opened on demand and the
/// least recently used are closed beyond the cap, so a store with thousands of files stays
/// under the process's file limit. Reads use `pread`, so one handle serves every thread.
struct Files {
    open: HashMap<PathBuf, (Arc<File>, u64)>,
    order: VecDeque<(PathBuf, u64)>,
    tick: u64,
    max: usize,
}

static FILES: Mutex<Option<Files>> = Mutex::new(None);

fn with_files<T>(f: impl FnOnce(&mut Files) -> T) -> T {
    let mut g = FILES.lock().unwrap();
    let files = g.get_or_insert_with(|| Files { open: HashMap::new(), order: VecDeque::new(), tick: 0, max: 128 });
    f(files)
}

/// The most files kept open at once (process-wide).
pub fn set_open_files(max: usize) {
    with_files(|f| f.max = max.max(4));
}

pub fn open_files() -> usize {
    with_files(|f| f.open.len())
}

pub fn file(path: &Path) -> std::io::Result<Arc<File>> {
    if let Some(h) = with_files(|f| {
        f.tick += 1;
        let t = f.tick;
        let h = f.open.get_mut(path).map(|e| {
            e.1 = t;
            e.0.clone()
        });
        if h.is_some() {
            f.order.push_back((path.to_path_buf(), t));
        }
        h
    }) {
        return Ok(h);
    }
    let h = Arc::new(File::open(path)?);
    with_files(|f| {
        f.tick += 1;
        let t = f.tick;
        f.open.insert(path.to_path_buf(), (h.clone(), t));
        f.order.push_back((path.to_path_buf(), t));
        while f.open.len() > f.max {
            let Some((p, t)) = f.order.pop_front() else { break };
            if f.open.get(&p).is_some_and(|e| e.1 == t) {
                f.open.remove(&p);
            }
        }
        if f.order.len() > 8 * f.max + 64 {
            let open = &f.open;
            f.order.retain(|(p, t)| open.get(p).is_some_and(|e| e.1 == *t));
        }
    });
    Ok(h)
}

/// Close a file before it is deleted.
pub fn forget(path: &Path) {
    with_files(|f| {
        f.open.remove(path);
    });
}

/// Files a store replaced, deleted on a thread of their own once nothing reads them.
///
/// A segment or index run a merge replaced may still be read by snapshots taken before the
/// merge was put in place (and files are reopened by path when their handle was closed to
/// stay under the cap), so the store only marks it doomed; the file goes when the last
/// handle on it is dropped ([`Doom`]).
struct Deleter {
    tx: std::sync::mpsc::Sender<PathBuf>,
}

static DELETER: Mutex<Option<Deleter>> = Mutex::new(None);
/// Files handed to the deleter and not yet gone.
static PENDING: (Mutex<usize>, std::sync::Condvar) = (Mutex::new(0), std::sync::Condvar::new());

/// Close `path` and delete it on the deleter's thread.
pub fn remove_later(path: PathBuf) {
    forget(&path);
    *PENDING.0.lock().unwrap() += 1;
    let mut g = DELETER.lock().unwrap();
    let d = g.get_or_insert_with(|| {
        let (tx, rx) = std::sync::mpsc::channel::<PathBuf>();
        let _ = std::thread::Builder::new().name("tdb-remove".into()).spawn(move || {
            for p in rx {
                let _ = std::fs::remove_file(&p);
                let mut n = PENDING.0.lock().unwrap();
                *n -= 1;
                if *n == 0 {
                    PENDING.1.notify_all();
                }
            }
        });
        Deleter { tx }
    });
    if let Err(e) = d.tx.send(path) {
        // the thread is gone: delete here
        let _ = std::fs::remove_file(e.0);
        *PENDING.0.lock().unwrap() -= 1;
    }
}

/// Wait until every file handed to [`remove_later`] is gone.
pub fn wait_removed() {
    let mut n = PENDING.0.lock().unwrap();
    while *n > 0 {
        n = PENDING.1.wait(n).unwrap();
    }
}

/// A file that is deleted when its owner is dropped, once marked: segments and index runs
/// hold one, and every handle on them is an `Arc` (the store's, and each snapshot's).
#[derive(Debug, Default)]
pub struct Doom(std::sync::atomic::AtomicBool);

impl Doom {
    pub fn set(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    /// Called from the owner's `drop`: delete `path` if marked.
    pub fn drop_file(&self, path: &Path) {
        if self.0.load(std::sync::atomic::Ordering::Acquire) {
            remove_later(path.to_path_buf());
        }
    }
}

thread_local! {
    /// Per thread: handles already looked up, held weakly so closing still works.
    static HANDLES: std::cell::RefCell<HashMap<u64, std::sync::Weak<File>>> = std::cell::RefCell::new(HashMap::new());
}

static NEXT_UID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A number unique in this process, for a file object.
pub fn uid() -> u64 {
    NEXT_UID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// [`file`] for the file object `uid`, without the shared lock while this thread's handle
/// is still open.
pub fn handle(uid: u64, path: &Path) -> std::io::Result<Arc<File>> {
    if let Some(f) = HANDLES.with(|h| h.borrow().get(&uid).and_then(|w| w.upgrade())) {
        return Ok(f);
    }
    let f = file(path)?;
    HANDLES.with(|h| {
        let mut h = h.borrow_mut();
        if h.len() > 4096 {
            h.retain(|_, w| w.strong_count() > 0);
        }
        h.insert(uid, Arc::downgrade(&f));
    });
    Ok(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_files_stay_under_the_cap() {
        let dir = std::env::temp_dir().join(format!("tdb-files-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        set_open_files(4);
        let paths: Vec<PathBuf> = (0..12).map(|i| dir.join(format!("f{i}"))).collect();
        for p in &paths {
            std::fs::write(p, b"x").unwrap();
        }
        for round in 0..3 {
            for (i, p) in paths.iter().enumerate() {
                let f = handle(1000 + i as u64, p).unwrap();
                let mut b = [0u8; 1];
                f.read_exact_at(&mut b, 0).unwrap();
                assert_eq!(&b, b"x", "round {round}");
                assert!(open_files() <= 4);
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
