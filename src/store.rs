//! A store shared by many threads: many writers, many readers, one thread that writes.
//!
//! [`Db`] takes writes through `&mut self` and reads through `&self`. `Store` puts one `Db`
//! on a thread of its own and lets any number of threads use it at once, the way a
//! database server serves its connections:
//!
//! - **Writes go through a pipeline.** The calling thread splits and hashes each body
//!   ([`Prepared::new`]): most of a put's work, since a body carries the whole conversation
//!   so far, and nothing in it needs the store, so callers on many threads do it at once.
//!   It also warms the caches the put will read (the session's tables, index blocks) from
//!   the latest snapshot. The prepared requests are queued for the writing thread, which
//!   takes everything queued at once as one group, puts each in arrival order (requests to
//!   one session are applied in the order they came; requests to different sessions never
//!   wait on each other beyond that), makes the whole group durable with one log write and
//!   one sync (group commit), and only then answers. While a group syncs, the next queues
//!   up, so the more writers there are, the more requests share each sync.
//! - **Reads go to snapshots.** [`Store::snapshot`] returns a read-only `Db`
//!   ([`Db::snapshot`]) of everything committed so far. A reader never waits for the
//!   writing thread except when the newest snapshot is older than the last commit and one
//!   must be taken; a long query holds its snapshot, not a lock, so writes go on.
//!   Snapshots are taken only after a commit, so a reader never sees a request that is not
//!   durable yet, and a request is answered only after a snapshot that holds it is
//!   published (when readers are about), so a client reads its own writes.
//! - **Everything else** that changes the store (rules, deletes, flushes) runs on the
//!   writing thread too, between groups ([`Store::exec`]).
//!
//! If making a group durable fails, the store stops taking writes: what is in memory may
//! no longer match the log. Reopening it recovers from the log.

use crate::codec::{Error, Result};
use crate::db::{Db, Options, Prepared, Put, PutOutcome};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// One request to store, owned (see [`Put`]).
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub session: String,
    pub agent: String,
    pub id: String,
    pub time: String,
    pub meta: Vec<u8>,
    pub body: Vec<u8>,
    /// The model's response, stored as it came (empty: none).
    pub response: Vec<u8>,
}

impl Request {
    fn put(&self) -> Put<'_> {
        Put { session: &self.session, agent: &self.agent, id: &self.id, time: &self.time, meta: &self.meta, body: &self.body, response: &self.response }
    }
}

type Reply<T> = Sender<T>;

enum Job {
    Puts(Vec<(Request, Prepared)>, Reply<Vec<Result<PutOutcome>>>),
    /// A reader wants a snapshot newer than the one published.
    Publish,
    Exec(Box<dyn FnOnce(&mut Db) + Send>),
    Stop,
}

/// A snapshot and the store version it was taken at.
struct Published {
    db: Arc<Db>,
    version: u64,
}

struct Shared {
    published: Mutex<Published>,
    /// Signalled when a new snapshot is published.
    fresh: Condvar,
    /// Bumped by the writing thread each time what a snapshot would hold changes.
    version: AtomicU64,
    /// When a reader last asked for a snapshot (ms since the store opened): while readers
    /// are about, the writing thread publishes after every change instead of on demand.
    last_read: AtomicU64,
    /// Why the store stopped taking writes, if it did.
    poisoned: Mutex<Option<String>>,
    stopping: AtomicBool,
    opened: Instant,
}

impl Shared {
    fn now_ms(&self) -> u64 {
        self.opened.elapsed().as_millis() as u64
    }
}

/// How the writing thread groups and paces its work.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Most body bytes put in one group (one sync) when more are queued.
    pub group_bytes: usize,
    /// How often the writing thread looks for finished flushes and merges when no writes
    /// come.
    pub tick: Duration,
    /// Threads preparing the requests of one call to [`Store::put`] (bodies are split on the
    /// calling thread and these).
    pub prepare_threads: usize,
}

impl Default for StoreOptions {
    fn default() -> Self {
        StoreOptions { group_bytes: 64 << 20, tick: Duration::from_millis(100), prepare_threads: 4 }
    }
}

pub struct Store {
    tx: Sender<Job>,
    shared: Arc<Shared>,
    opts: Options,
    sopts: StoreOptions,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// What the writing thread reports about its groups.
#[derive(Debug, Clone, Copy, Default)]
pub struct GroupStats {
    pub groups: u64,
    pub requests: u64,
    pub largest: u64,
    pub snapshots: u64,
}

static GROUP_STATS: Mutex<GroupStats> = Mutex::new(GroupStats { groups: 0, requests: 0, largest: 0, snapshots: 0 });

/// Totals over every store in this process.
pub fn group_stats() -> GroupStats {
    *GROUP_STATS.lock().unwrap()
}

impl Store {
    pub fn open(dir: impl AsRef<Path>, opts: Options, sopts: StoreOptions) -> Result<Store> {
        let mut db = Db::open(dir, opts.clone())?;
        let first = Arc::new(db.snapshot()?);
        let shared = Arc::new(Shared {
            published: Mutex::new(Published { db: first, version: 0 }),
            fresh: Condvar::new(),
            version: AtomicU64::new(0),
            last_read: AtomicU64::new(0),
            poisoned: Mutex::new(None),
            stopping: AtomicBool::new(false),
            opened: Instant::now(),
        });
        let (tx, rx) = channel();
        let sh = shared.clone();
        let so = sopts.clone();
        let thread = std::thread::Builder::new().name("tdb-writer".into()).spawn(move || writer(db, rx, sh, so))?;
        Ok(Store { tx, shared, opts, sopts, thread: Mutex::new(Some(thread)) })
    }

    /// Store `reqs` and answer once they are durable, one outcome per request in order. A
    /// request whose body cannot be split fails alone.
    pub fn put(&self, reqs: Vec<Request>) -> Vec<Result<PutOutcome>> {
        if let Some(why) = self.shared.poisoned.lock().unwrap().clone() {
            return reqs.iter().map(|_| Err(Error::BadRequest(format!("the store takes no writes: {why}")))).collect();
        }
        let n = reqs.len();
        let prepared = self.prepare(reqs);
        let mut out: Vec<Option<Result<PutOutcome>>> = (0..n).map(|_| None).collect();
        let mut send = Vec::with_capacity(n);
        let mut slots = Vec::with_capacity(n);
        for (i, (r, p)) in prepared.into_iter().enumerate() {
            match p {
                Ok(p) => {
                    send.push((r, p));
                    slots.push(i);
                }
                Err(e) => out[i] = Some(Err(e)),
            }
        }
        if !send.is_empty() {
            let (rtx, rrx) = channel();
            let got = match self.tx.send(Job::Puts(send, rtx)) {
                Ok(()) => rrx.recv().ok(),
                Err(_) => None,
            };
            match got {
                Some(rs) => {
                    for (i, r) in slots.into_iter().zip(rs) {
                        out[i] = Some(r);
                    }
                }
                None => {
                    for i in slots {
                        out[i] = Some(Err(Error::BadRequest("the store is closed".into())));
                    }
                }
            }
        }
        out.into_iter().map(|r| r.unwrap()).collect()
    }

    /// Split and hash each body, and warm the caches its put will read, on this thread and
    /// up to `prepare_threads` more.
    fn prepare(&self, reqs: Vec<Request>) -> Vec<(Request, Result<Prepared>)> {
        let snap = self.latest();
        let one = |r: Request| {
            let p = Prepared::new(&self.opts, &r.body);
            if p.is_ok() {
                // a failure here only means the writing thread reads what it needs itself
                let _ = snap.warm(&r.put());
            }
            (r, p)
        };
        let bytes: usize = reqs.iter().map(|r| r.body.len()).sum();
        let threads = self.sopts.prepare_threads.max(1).min(reqs.len());
        if threads <= 1 || bytes < 1 << 20 {
            return reqs.into_iter().map(one).collect();
        }
        // contiguous chunks keep the order
        let n = reqs.len();
        let per = n.div_ceil(threads);
        let mut chunks: Vec<Vec<Request>> = Vec::new();
        let mut it = reqs.into_iter();
        loop {
            let c: Vec<Request> = it.by_ref().take(per).collect();
            if c.is_empty() {
                break;
            }
            chunks.push(c);
        }
        std::thread::scope(|sc| {
            let hs: Vec<_> = chunks.into_iter().map(|c| sc.spawn(move || c.into_iter().map(one).collect::<Vec<_>>())).collect();
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
        })
    }

    /// The newest published snapshot, however old.
    fn latest(&self) -> Arc<Db> {
        self.shared.published.lock().unwrap().db.clone()
    }

    /// A read-only handle holding every request committed before this call (see
    /// [`Db::snapshot`]). Cheap when nothing changed since the last one.
    pub fn snapshot(&self) -> Result<Arc<Db>> {
        let sh = &self.shared;
        sh.last_read.store(sh.now_ms().max(1), Ordering::Relaxed);
        let want = sh.version.load(Ordering::Acquire);
        let mut g = sh.published.lock().unwrap();
        if g.version >= want {
            return Ok(g.db.clone());
        }
        if self.tx.send(Job::Publish).is_err() {
            return Ok(g.db.clone());
        }
        while g.version < want {
            if sh.stopping.load(Ordering::Relaxed) || sh.poisoned.lock().unwrap().is_some() {
                break;
            }
            let (ng, _) = sh.fresh.wait_timeout(g, Duration::from_millis(500)).unwrap();
            g = ng;
        }
        Ok(g.db.clone())
    }

    /// Run `f` on the writing thread, between groups, and wait for its answer: anything
    /// that changes the store besides puts. Whatever it changes is in the next snapshot.
    pub fn exec<R: Send + 'static>(&self, f: impl FnOnce(&mut Db) -> R + Send + 'static) -> Result<R> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Job::Exec(Box::new(move |db: &mut Db| {
                let _ = rtx.send(f(db));
            })))
            .map_err(|_| Error::BadRequest("the store is closed".into()))?;
        rrx.recv().map_err(|_| Error::BadRequest("the store's writing thread stopped".into()))
    }

    /// Why the store stopped taking writes, if it did.
    pub fn poisoned(&self) -> Option<String> {
        self.shared.poisoned.lock().unwrap().clone()
    }

    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// Stop the writing thread (what is committed stays; nothing is lost) and wait for it.
    pub fn close(&self) {
        self.shared.stopping.store(true, Ordering::Relaxed);
        let _ = self.tx.send(Job::Stop);
        if let Some(h) = self.thread.lock().unwrap().take() {
            let _ = h.join();
        }
        self.shared.fresh.notify_all();
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.close();
    }
}

/// The writing thread.
fn writer(mut db: Db, rx: Receiver<Job>, sh: Arc<Shared>, so: StoreOptions) {
    let mut queued: std::collections::VecDeque<Job> = Default::default();
    let mut stop = false;
    while !stop {
        // wait for work, looking after finished flushes and merges meanwhile
        if queued.is_empty() {
            match rx.recv_timeout(so.tick) {
                Ok(j) => queued.push_back(j),
                Err(RecvTimeoutError::Timeout) => {
                    maintain(&mut db, &sh);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        while let Ok(j) = rx.try_recv() {
            queued.push_back(j);
        }
        // one group: the puts queued ahead of the first job of another kind, up to a size
        let mut group: Vec<(Vec<(Request, Prepared)>, Reply<Vec<Result<PutOutcome>>>)> = Vec::new();
        let mut bytes = 0usize;
        let mut publish = false;
        while let Some(j) = queued.front() {
            match j {
                Job::Puts(items, _) => {
                    let b: usize = items.iter().map(|(r, _)| r.body.len() + r.response.len()).sum();
                    if !group.is_empty() && bytes + b > so.group_bytes {
                        break;
                    }
                    bytes += b;
                    let Some(Job::Puts(items, reply)) = queued.pop_front() else { unreachable!() };
                    group.push((items, reply));
                }
                Job::Publish => {
                    queued.pop_front();
                    publish = true;
                }
                Job::Exec(_) | Job::Stop => break,
            }
        }
        if !group.is_empty() {
            run_group(&mut db, &sh, group, publish);
            continue;
        }
        if publish {
            publish_now(&mut db, &sh);
            continue;
        }
        match queued.pop_front() {
            Some(Job::Exec(f)) => {
                f(&mut db);
                sh.version.fetch_add(1, Ordering::AcqRel);
                if readers_about(&sh) {
                    publish_now(&mut db, &sh);
                }
            }
            Some(Job::Stop) => stop = true,
            _ => {}
        }
    }
    // answer what is still queued
    for j in queued.drain(..).chain(rx.try_iter()) {
        if let Job::Puts(items, reply) = j {
            let _ = reply.send(items.iter().map(|_| Err(Error::BadRequest("the store is closed".into()))).collect());
        }
    }
    sh.stopping.store(true, Ordering::Relaxed);
    sh.fresh.notify_all();
}

fn readers_about(sh: &Shared) -> bool {
    let t = sh.last_read.load(Ordering::Relaxed);
    t > 0 && sh.now_ms().saturating_sub(t) < 1000
}

/// Put a group, commit it, publish, answer.
fn run_group(db: &mut Db, sh: &Shared, group: Vec<(Vec<(Request, Prepared)>, Reply<Vec<Result<PutOutcome>>>)>, publish: bool) {
    let poisoned = sh.poisoned.lock().unwrap().clone();
    let mut answers: Vec<(Reply<Vec<Result<PutOutcome>>>, Vec<Result<PutOutcome>>)> = Vec::with_capacity(group.len());
    let mut stored = 0u64;
    let mut fatal: Option<String> = poisoned.clone();
    for (items, reply) in group {
        let mut rs = Vec::with_capacity(items.len());
        for (r, p) in items {
            if let Some(why) = &fatal {
                rs.push(Err(Error::BadRequest(format!("the store takes no writes: {why}"))));
                continue;
            }
            let res = db.put_prepared(&r.put(), p);
            match &res {
                Ok(PutOutcome::Stored(_)) => stored += 1,
                Err(Error::Corrupt(m)) => fatal = Some(m.clone()),
                _ => {}
            }
            rs.push(res);
        }
        answers.push((reply, rs));
    }
    if fatal.is_none()
        && stored > 0
        && let Err(e) = db.commit()
    {
        fatal = Some(format!("making a group durable failed: {e}"));
    }
    if let Some(why) = &fatal {
        if poisoned.is_none() {
            *sh.poisoned.lock().unwrap() = Some(why.clone());
        }
        // nothing in this group may count as saved
        for (_, rs) in &mut answers {
            for r in rs.iter_mut() {
                if matches!(r, Ok(PutOutcome::Stored(_))) {
                    *r = Err(Error::BadRequest(format!("the store takes no writes: {why}")));
                }
            }
        }
    } else if stored > 0 {
        sh.version.fetch_add(1, Ordering::AcqRel);
        if publish || readers_about(sh) {
            publish_now(db, sh);
        }
    }
    let n: u64 = answers.iter().map(|a| a.1.len() as u64).sum();
    {
        let mut g = GROUP_STATS.lock().unwrap();
        g.groups += 1;
        g.requests += n;
        g.largest = g.largest.max(n);
    }
    for (reply, rs) in answers {
        let _ = reply.send(rs);
    }
}

/// Publish a snapshot of the store as it is now.
fn publish_now(db: &mut Db, sh: &Shared) {
    let version = sh.version.load(Ordering::Acquire);
    if sh.published.lock().unwrap().version >= version {
        sh.fresh.notify_all();
        return;
    }
    match db.snapshot() {
        Ok(s) => {
            *sh.published.lock().unwrap() = Published { db: Arc::new(s), version };
            GROUP_STATS.lock().unwrap().snapshots += 1;
        }
        Err(e) => {
            *sh.poisoned.lock().unwrap() = Some(format!("taking a snapshot failed: {e}"));
        }
    }
    sh.fresh.notify_all();
}

/// Put finished flushes and merges in place while no writes come.
fn maintain(db: &mut Db, sh: &Shared) {
    if sh.poisoned.lock().unwrap().is_some() {
        return;
    }
    match db.maintain() {
        Ok(true) => {
            sh.version.fetch_add(1, Ordering::AcqRel);
            if readers_about(sh) {
                publish_now(db, sh);
            }
        }
        Ok(false) => {}
        Err(e) => *sh.poisoned.lock().unwrap() = Some(format!("putting a merge in place failed: {e}")),
    }
}
