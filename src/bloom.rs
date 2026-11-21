//! A Bloom filter: "certainly not here" or "maybe here" for a set of 64-bit hashes.
//! Ten bits per key gives about 1% false positives.

use crate::codec::{corrupt, Dec, Enc, Result};

#[derive(Debug, Clone, Default)]
pub struct Bloom {
    bits: Vec<u64>,
    k: u32,
}

impl Bloom {
    pub fn new(keys: usize, bits_per_key: usize) -> Bloom {
        let nbits = (keys * bits_per_key).max(64);
        let k = ((bits_per_key as f64 * 0.69).round() as u32).clamp(1, 30);
        Bloom { bits: vec![0; nbits.div_ceil(64)], k }
    }

    fn probes(&self, h: u64) -> impl Iterator<Item = usize> + '_ {
        let n = (self.bits.len() * 64) as u64;
        let h2 = h.rotate_left(32) | 1;
        (0..self.k as u64).map(move |i| (h.wrapping_add(i.wrapping_mul(h2)) % n) as usize)
    }

    pub fn add(&mut self, h: u64) {
        let idx: Vec<usize> = self.probes(h).collect();
        for i in idx {
            self.bits[i / 64] |= 1 << (i % 64);
        }
    }

    pub fn may_contain(&self, h: u64) -> bool {
        if self.bits.is_empty() {
            return false;
        }
        self.probes(h).all(|i| self.bits[i / 64] >> (i % 64) & 1 == 1)
    }

    pub fn bytes(&self) -> usize {
        self.bits.len() * 8
    }

    pub fn encode(&self, e: &mut Enc) {
        e.var(self.k as u64);
        e.var(self.bits.len() as u64);
        for w in &self.bits {
            e.raw(&w.to_le_bytes());
        }
    }

    pub fn decode(d: &mut Dec) -> Result<Bloom> {
        let k = d.var32()?;
        let n = d.var()? as usize;
        if n > d.remaining() / 8 {
            return corrupt("bloom filter longer than its block");
        }
        let mut bits = Vec::with_capacity(n);
        for _ in 0..n {
            bits.push(u64::from_le_bytes(d.raw(8)?.try_into().unwrap()));
        }
        Ok(Bloom { bits, k })
    }
}

pub fn key_hash(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_few_false_positives() {
        let mut b = Bloom::new(10_000, 10);
        for i in 0..10_000u64 {
            b.add(key_hash(&i.to_le_bytes()));
        }
        assert!((0..10_000u64).all(|i| b.may_contain(key_hash(&i.to_le_bytes()))));
        let fp = (10_000..110_000u64).filter(|i| b.may_contain(key_hash(&i.to_le_bytes()))).count();
        assert!(fp < 2_000, "{fp} false positives in 100k");
        let mut e = Enc::new();
        b.encode(&mut e);
        let c = Bloom::decode(&mut Dec::new(&e.buf)).unwrap();
        assert!((0..10_000u64).all(|i| c.may_contain(key_hash(&i.to_le_bytes()))));
    }
}
