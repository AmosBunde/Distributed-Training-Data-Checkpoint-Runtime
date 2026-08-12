//! Byte-budgeted LRU cache for whole objects.
//!
//! Values are `Bytes` (cheap to clone); the budget counts value bytes only.
//! An object larger than the whole budget is never cached (it would evict
//! everything for a single-use read).

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

struct Entry {
    data: Bytes,
    /// Monotonic access stamp; smallest = least recently used.
    stamp: u64,
}

struct Inner {
    map: HashMap<String, Entry>,
    used_bytes: u64,
    clock: u64,
}

pub struct LruByteCache {
    inner: Mutex<Inner>,
    budget_bytes: u64,
    evictions: AtomicU64,
}

impl LruByteCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                used_bytes: 0,
                clock: 0,
            }),
            budget_bytes,
            evictions: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let clock = inner.clock;
        inner.map.get_mut(key).map(|e| {
            e.stamp = clock;
            e.data.clone()
        })
    }

    pub fn contains(&self, key: &str) -> bool {
        self.inner.lock().unwrap().map.contains_key(key)
    }

    pub fn insert(&self, key: &str, data: Bytes) {
        let size = data.len() as u64;
        if size > self.budget_bytes {
            return; // never cache objects larger than the whole budget
        }
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let clock = inner.clock;

        if let Some(old) = inner.map.remove(key) {
            inner.used_bytes -= old.data.len() as u64;
        }

        // Evict LRU entries until the new object fits.
        while inner.used_bytes + size > self.budget_bytes {
            let lru_key = inner
                .map
                .iter()
                .min_by_key(|(_, e)| e.stamp)
                .map(|(k, _)| k.clone());
            match lru_key {
                Some(k) => {
                    if let Some(e) = inner.map.remove(&k) {
                        inner.used_bytes -= e.data.len() as u64;
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            }
        }

        inner.used_bytes += size;
        inner
            .map
            .insert(key.to_owned(), Entry { data, stamp: clock });
    }

    pub fn used_bytes(&self) -> u64 {
        self.inner.lock().unwrap().used_bytes
    }

    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_order_respects_access() {
        let cache = LruByteCache::new(100);
        cache.insert("a", Bytes::from(vec![0u8; 40]));
        cache.insert("b", Bytes::from(vec![0u8; 40]));
        cache.get("a"); // a becomes most recently used
        cache.insert("c", Bytes::from(vec![0u8; 40])); // evicts b

        assert!(cache.contains("a"));
        assert!(!cache.contains("b"));
        assert!(cache.contains("c"));
        assert_eq!(cache.used_bytes(), 80);
        assert_eq!(cache.evictions(), 1);
    }

    #[test]
    fn oversized_object_is_not_cached() {
        let cache = LruByteCache::new(10);
        cache.insert("big", Bytes::from(vec![0u8; 100]));
        assert!(!cache.contains("big"));
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn reinsert_replaces_and_adjusts_bytes() {
        let cache = LruByteCache::new(100);
        cache.insert("a", Bytes::from(vec![0u8; 60]));
        cache.insert("a", Bytes::from(vec![0u8; 20]));
        assert_eq!(cache.used_bytes(), 20);
    }
}
