//! A shared cache with a byte budget. Eviction is CLOCK: entries used since the hand last
//! passed get a second chance, the rest are dropped oldest first. A cache can be split into
//! shards, each with its own lock and an equal share of the budget, so many threads looking
//! up small entries do not queue on one lock.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

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

pub struct Cache<K, V> {
    shards: Vec<Mutex<Inner<K, V>>>,
    /// Per shard.
    budget: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub budget: usize,
    pub hits: u64,
    pub misses: u64,
}

impl<K: Hash + Eq + Clone, V> Cache<K, V> {
    pub fn new(budget: usize) -> Self {
        Self::sharded(budget, 1)
    }

    /// `n` shards of `budget / n` bytes each; an entry bigger than a shard is not kept.
    pub fn sharded(budget: usize, n: usize) -> Self {
        let n = n.max(1);
        let shards = (0..n).map(|_| Mutex::new(Inner { map: HashMap::new(), order: VecDeque::new(), bytes: 0, hits: 0, misses: 0 })).collect();
        Cache { shards, budget: budget / n }
    }

    fn shard(&self, k: &K) -> &Mutex<Inner<K, V>> {
        if self.shards.len() == 1 {
            return &self.shards[0];
        }
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut h);
        &self.shards[(h.finish() % self.shards.len() as u64) as usize]
    }

    pub fn get(&self, k: &K) -> Option<Arc<V>> {
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

    /// Insert (or replace) and return the cached value; an entry bigger than the whole
    /// budget is returned without being kept.
    pub fn insert(&self, k: K, v: Arc<V>, size: usize) -> Arc<V> {
        if size > self.budget {
            return v;
        }
        let mut g = self.shard(&k).lock().unwrap();
        if let Some(old) = g.map.insert(k.clone(), Entry { v: v.clone(), size, used: false }) {
            g.bytes -= old.size;
        } else {
            g.order.push_back(k);
        }
        g.bytes += size;
        while g.bytes > self.budget {
            let Some(old) = g.order.pop_front() else { break };
            let second_chance = match g.map.get_mut(&old) {
                Some(e) if e.used => {
                    e.used = false;
                    true
                }
                Some(_) => false,
                None => continue,
            };
            if second_chance {
                g.order.push_back(old);
            } else if let Some(e) = g.map.remove(&old) {
                g.bytes -= e.size;
            }
        }
        v
    }

    pub fn remove(&self, k: &K) {
        let mut g = self.shard(k).lock().unwrap();
        if let Some(e) = g.map.remove(k) {
            g.bytes -= e.size;
        }
    }

    pub fn clear(&self) {
        for s in &self.shards {
            let mut g = s.lock().unwrap();
            g.map.clear();
            g.order.clear();
            g.bytes = 0;
        }
    }

    pub fn stats(&self) -> CacheStats {
        let mut st = CacheStats { budget: self.budget * self.shards.len(), ..CacheStats::default() };
        for s in &self.shards {
            let g = s.lock().unwrap();
            st.entries += g.map.len();
            st.bytes += g.bytes;
            st.hits += g.hits;
            st.misses += g.misses;
        }
        st
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
}
