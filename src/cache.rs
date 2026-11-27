//! Caches with a byte budget. Eviction is CLOCK: entries used since the hand last passed
//! get a second chance, the rest are dropped oldest first. A cache can be split into
//! shards, each with its own lock, so many threads looking up small entries do not queue on
//! one lock.
//!
//! A cache either has a budget of its own, split evenly between its shards, or belongs to a
//! [`Pool`]: one budget shared by many caches, the way a database's buffer pool is shared by
//! all its tables and indexes. When an insert takes a pool over its budget, entries are
//! evicted from the cache used least recently first (by its own CLOCK), so memory goes to
//! whatever is being read now instead of sitting in fixed shares while another cache
//! thrashes.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

struct Entry<V> {
    v: Arc<V>,
    size: usize,
    used: bool,
}

struct Inner<K, V> {
    map: HashMap<K, Entry<V>>,
    order: VecDeque<K>,
    bytes: usize,
    hits: u64,
    misses: u64,
}

impl<K: Hash + Eq + Clone, V> Inner<K, V> {
    /// Advance the CLOCK hand over at most one round of entries: marked entries lose their
    /// mark, and the first unmarked one is dropped. Its size, or `None` when every entry was
    /// marked (all marks are now cleared) or the shard is empty.
    fn evict_one(&mut self) -> Option<usize> {
        for _ in 0..self.order.len() {
            let old = self.order.pop_front()?;
            match self.map.get_mut(&old) {
                Some(e) if e.used => {
                    e.used = false;
                    self.order.push_back(old);
                }
                Some(_) => {
                    let e = self.map.remove(&old).unwrap();
                    self.bytes -= e.size;
                    return Some(e.size);
                }
                None => {}
            }
        }
        None
    }
}

/// Something a [`Pool`] can take memory back from.
trait Evict: Send + Sync {
    /// Drop one entry; the bytes it freed, or `None` when there is nothing to drop.
    fn evict_one(&self) -> Option<usize>;
    /// When it was last used, on its pool's clock.
    fn last_used(&self) -> u64;
}

/// A byte budget shared by several caches.
pub struct Pool {
    limit: usize,
    total: AtomicUsize,
    members: Mutex<Vec<Weak<dyn Evict>>>,
    /// Advanced at every eviction pass; caches note it when used.
    clock: AtomicU64,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pool({} of {} bytes)", self.used(), self.limit)
    }
}

impl Pool {
    pub fn new(limit: usize) -> Arc<Pool> {
        Arc::new(Pool { limit, total: AtomicUsize::new(0), members: Mutex::new(Vec::new()), clock: AtomicU64::new(1) })
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Bytes held by all the pool's caches.
    pub fn used(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// Evict until the pool is within its budget (or nothing is left to evict): from the
    /// cache used least recently first, by its own CLOCK, then the next. Called with no
    /// cache lock held.
    fn make_room(&self) {
        if self.total.load(Ordering::Relaxed) <= self.limit {
            return;
        }
        self.clock.fetch_add(1, Ordering::Relaxed);
        let mut order: Vec<(u64, Arc<dyn Evict>)> = {
            let mut ms = self.members.lock().unwrap();
            ms.retain(|w| w.strong_count() > 0);
            ms.iter().filter_map(|w| w.upgrade()).map(|c| (c.last_used(), c)).collect()
        };
        order.sort_by_key(|x| x.0);
        for (_, c) in order {
            // a pass that only cleared marks is followed by one that drops
            let mut idle = 0;
            while self.total.load(Ordering::Relaxed) > self.limit && idle < 2 {
                match c.evict_one() {
                    Some(freed) => {
                        self.total.fetch_sub(freed, Ordering::Relaxed);
                        idle = 0;
                    }
                    None => idle += 1,
                }
            }
            if self.total.load(Ordering::Relaxed) <= self.limit {
                return;
            }
        }
    }
}

struct Shards<K, V> {
    shards: Vec<Mutex<Inner<K, V>>>,
    /// Which shard the pool takes from next.
    hand: AtomicUsize,
    /// When the cache was last used, on its pool's clock.
    last: AtomicU64,
}

impl<K: Hash + Eq + Clone + Send, V: Send + Sync> Evict for Shards<K, V> {
    fn evict_one(&self) -> Option<usize> {
        let n = self.shards.len();
        let start = self.hand.fetch_add(1, Ordering::Relaxed);
        for k in 0..n {
            if let Some(freed) = self.shards[(start + k) % n].lock().unwrap().evict_one() {
                return Some(freed);
            }
        }
        None
    }
    fn last_used(&self) -> u64 {
        self.last.load(Ordering::Relaxed)
    }
}

pub struct Cache<K, V> {
    inner: Arc<Shards<K, V>>,
    /// Per shard, for a cache with a budget of its own.
    budget: usize,
    pool: Option<Arc<Pool>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub budget: usize,
    pub hits: u64,
    pub misses: u64,
}

fn shards<K, V>(n: usize) -> Arc<Shards<K, V>> {
    Arc::new(Shards { shards: (0..n.max(1)).map(|_| Mutex::new(Inner { map: HashMap::new(), order: VecDeque::new(), bytes: 0, hits: 0, misses: 0 })).collect(), hand: AtomicUsize::new(0), last: AtomicU64::new(0) })
}

impl<K: Hash + Eq + Clone + Send + 'static, V: Send + Sync + 'static> Cache<K, V> {
    pub fn new(budget: usize) -> Self {
        Self::sharded(budget, 1)
    }

    /// `n` shards of `budget / n` bytes each; an entry bigger than a shard is not kept.
    pub fn sharded(budget: usize, n: usize) -> Self {
        let n = n.max(1);
        Cache { inner: shards(n), budget: budget / n, pool: None }
    }

    /// `n` shards drawing on `pool`'s budget; an entry bigger than an eighth of the pool is
    /// not kept, so one read cannot empty every other cache.
    pub fn pooled(pool: &Arc<Pool>, n: usize) -> Self {
        let c = Cache { inner: shards(n), budget: pool.limit / 8, pool: Some(pool.clone()) };
        let w: Weak<dyn Evict> = Arc::downgrade(&(c.inner.clone() as Arc<dyn Evict>));
        pool.members.lock().unwrap().push(w);
        c
    }

    fn shard(&self, k: &K) -> &Mutex<Inner<K, V>> {
        let s = &self.inner.shards;
        if s.len() == 1 {
            return &s[0];
        }
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut h);
        &s[(h.finish() % s.len() as u64) as usize]
    }

    fn touch(&self) {
        if let Some(p) = &self.pool {
            self.inner.last.store(p.clock.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        self.touch();
        let mut g = self.shard(k).lock().unwrap();
        match g.map.get_mut(k) {
            Some(e) => {
                e.used = true;
                let v = e.v.clone();
                g.hits += 1;
                Some(v)
            }
            None => {
                g.misses += 1;
                None
            }
        }
    }

    /// Insert (or replace) and return the cached value; an entry bigger than the cache can
    /// keep is returned without being kept.
    pub fn insert(&self, k: K, v: Arc<V>, size: usize) -> Arc<V> {
        if size > self.budget {
            return v;
        }
        self.touch();
        let mut g = self.shard(&k).lock().unwrap();
        let old = match g.map.insert(k.clone(), Entry { v: v.clone(), size, used: false }) {
            Some(old) => {
                g.bytes -= old.size;
                old.size
            }
            None => {
                g.order.push_back(k);
                0
            }
        };
        g.bytes += size;
        match &self.pool {
            Some(p) => {
                drop(g);
                p.total.fetch_add(size, Ordering::Relaxed);
                p.total.fetch_sub(old, Ordering::Relaxed);
                p.make_room();
            }
            None => {
                let mut idle = 0;
                while g.bytes > self.budget && idle < 2 {
                    // a round that only cleared marks is followed by one that drops
                    idle = if g.evict_one().is_some() { 0 } else { idle + 1 };
                }
            }
        }
        v
    }

    pub fn remove(&self, k: &K) {
        let mut g = self.shard(k).lock().unwrap();
        if let Some(e) = g.map.remove(k) {
            g.bytes -= e.size;
            if let Some(p) = &self.pool {
                p.total.fetch_sub(e.size, Ordering::Relaxed);
            }
        }
    }

    pub fn clear(&self) {
        for s in &self.inner.shards {
            let mut g = s.lock().unwrap();
            g.map.clear();
            g.order.clear();
            if let Some(p) = &self.pool {
                p.total.fetch_sub(g.bytes, Ordering::Relaxed);
            }
            g.bytes = 0;
        }
    }

    pub fn stats(&self) -> CacheStats {
        let budget = match &self.pool {
            Some(p) => p.limit,
            None => self.budget * self.inner.shards.len(),
        };
        let mut st = CacheStats { budget, ..CacheStats::default() };
        for s in &self.inner.shards {
            let g = s.lock().unwrap();
            st.entries += g.map.len();
            st.bytes += g.bytes;
            st.hits += g.hits;
            st.misses += g.misses;
        }
        st
    }
}

impl<K, V> Drop for Cache<K, V> {
    fn drop(&mut self) {
        // what the cache held no longer counts against its pool
        if let Some(p) = &self.pool {
            let held: usize = self.inner.shards.iter().map(|s| s.lock().map(|g| g.bytes).unwrap_or(0)).sum();
            p.total.fetch_sub(held.min(p.total.load(Ordering::Relaxed)), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_within_budget_and_keeps_used_entries() {
        let c: Cache<u32, Vec<u8>> = Cache::new(1000);
        for i in 0..10 {
            c.insert(i, Arc::new(vec![0; 100]), 100);
        }
        assert!(c.get(&0).is_some());
        c.insert(10, Arc::new(vec![0; 100]), 100);
        assert!(c.get(&0).is_some(), "used entry got a second chance");
        assert!(c.get(&1).is_none(), "oldest unused entry went first");
        assert!(c.stats().bytes <= 1000);
        c.insert(99, Arc::new(vec![0; 5000]), 5000);
        assert!(c.get(&99).is_none());
    }

    #[test]
    fn a_pool_gives_memory_to_the_cache_in_use() {
        let pool = Pool::new(10_000);
        let a: Cache<u32, Vec<u8>> = Cache::pooled(&pool, 2);
        let b: Cache<u32, Vec<u8>> = Cache::pooled(&pool, 4);
        // a fills the whole pool while b is idle
        for i in 0..100 {
            a.insert(i, Arc::new(vec![0; 100]), 100);
        }
        assert_eq!(a.stats().bytes, 10_000);
        // then b is used: it can grow well past a fixed half, taking from a
        for i in 0..90 {
            b.insert(i, Arc::new(vec![0; 100]), 100);
            for j in 0..=i {
                b.get(&j);
            }
        }
        assert!(pool.used() <= 10_000, "pool over budget: {}", pool.used());
        assert!(b.stats().bytes >= 8_000, "the cache in use got only {} bytes", b.stats().bytes);
        assert_eq!(a.stats().bytes + b.stats().bytes, pool.used());
        // an entry bigger than an eighth of the pool is not kept
        b.insert(1000, Arc::new(vec![0; 2000]), 2000);
        assert!(b.get(&1000).is_none());
        // removing and clearing give the bytes back
        a.clear();
        b.remove(&0);
        assert_eq!(pool.used(), b.stats().bytes);
        drop(b);
        assert_eq!(pool.used(), 0);
    }
}
