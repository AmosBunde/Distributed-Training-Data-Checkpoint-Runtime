//! High-throughput read path for shard data.
//!
//! [`IoEngine`] fronts any [`Storage`] with:
//! - a byte-budgeted LRU cache (whole objects, keyed by storage key)
//! - a sequential prefetcher that warms the next N objects of a shard list
//! - hit/miss/eviction counters for the observability layer
//!
//! Single-flight de-duplication: concurrent reads of the same missing key
//! share one backend fetch instead of stampeding storage.

pub mod cache;

use bytes::Bytes;
use cache::LruByteCache;
use dtr_common::DtrError;
use dtr_storage::Storage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Snapshot of the engine's counters (for /metrics).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub bytes_read_backend: u64,
    pub bytes_served: u64,
}

pub struct IoEngine {
    storage: Arc<dyn Storage>,
    cache: LruByteCache,
    prefetch_depth: u32,
    hits: AtomicU64,
    misses: AtomicU64,
    backend_bytes: AtomicU64,
    served_bytes: AtomicU64,
    /// Keys currently being fetched, with a channel readers can wait on.
    inflight: Mutex<HashMap<String, broadcast::Sender<Result<Bytes, String>>>>,
}

impl IoEngine {
    pub fn new(storage: Arc<dyn Storage>, cache_bytes: u64, prefetch_depth: u32) -> Self {
        Self {
            storage,
            cache: LruByteCache::new(cache_bytes),
            prefetch_depth,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            backend_bytes: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Read a whole object through the cache.
    pub async fn read(&self, key: &str) -> Result<Bytes, DtrError> {
        if let Some(data) = self.cache.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.served_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(data);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let data = self.fetch_dedup(key).await?;
        self.served_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// Read a byte range. Served from the cached whole object when present;
    /// otherwise a direct range read (no caching — partial objects would
    /// poison whole-object cache semantics).
    pub async fn read_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, DtrError> {
        if let Some(data) = self.cache.get(key) {
            let end = (range.end as usize).min(data.len());
            let start = (range.start as usize).min(end);
            self.hits.fetch_add(1, Ordering::Relaxed);
            let slice = data.slice(start..end);
            self.served_bytes
                .fetch_add(slice.len() as u64, Ordering::Relaxed);
            return Ok(slice);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let data = self.storage.get_range(key, range).await?;
        self.backend_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.served_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// Warm the cache with the next `prefetch_depth` keys following `cursor`
    /// in `shard_keys`. Fire-and-forget: spawns background fetches and returns
    /// the number of fetches started.
    pub fn prefetch(self: &Arc<Self>, shard_keys: &[String], cursor: usize) -> usize {
        let mut started = 0;
        for key in shard_keys
            .iter()
            .skip(cursor + 1)
            .take(self.prefetch_depth as usize)
        {
            if self.cache.contains(key) {
                continue;
            }
            let engine = Arc::clone(self);
            let key = key.clone();
            tokio::spawn(async move {
                if let Err(e) = engine.fetch_dedup(&key).await {
                    tracing::debug!(key = %key, error = %e, "prefetch failed");
                }
            });
            started += 1;
        }
        started
    }

    pub fn stats(&self) -> IoStats {
        IoStats {
            cache_hits: self.hits.load(Ordering::Relaxed),
            cache_misses: self.misses.load(Ordering::Relaxed),
            evictions: self.cache.evictions(),
            bytes_read_backend: self.backend_bytes.load(Ordering::Relaxed),
            bytes_served: self.served_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 {
            0.0
        } else {
            h / (h + m)
        }
    }

    /// Fetch a key from the backend with single-flight de-duplication, then
    /// insert it into the cache.
    async fn fetch_dedup(&self, key: &str) -> Result<Bytes, DtrError> {
        // Fast path: someone may have populated the cache while we raced.
        if let Some(data) = self.cache.get(key) {
            return Ok(data);
        }

        let mut rx = {
            let mut inflight = self.inflight.lock().unwrap();
            if let Some(tx) = inflight.get(key) {
                // Another task is already fetching: subscribe and wait.
                Some(tx.subscribe())
            } else {
                let (tx, _) = broadcast::channel(1);
                inflight.insert(key.to_owned(), tx);
                None
            }
        };

        if let Some(rx) = rx.as_mut() {
            return match rx.recv().await {
                Ok(Ok(data)) => Ok(data),
                Ok(Err(msg)) => Err(DtrError::Internal(msg)),
                Err(_) => Err(DtrError::Internal(format!(
                    "in-flight fetch for {key} dropped"
                ))),
            };
        }

        // We are the fetching task.
        let result = self.storage.get(key).await;
        let broadcast_result = match &result {
            Ok(data) => {
                self.backend_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.cache.insert(key, data.clone());
                Ok(data.clone())
            }
            Err(e) => Err(e.to_string()),
        };
        if let Some(tx) = self.inflight.lock().unwrap().remove(key) {
            let _ = tx.send(broadcast_result);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use dtr_storage::{ObjectMeta, ObjectStorage, Storage};
    use std::sync::atomic::AtomicUsize;

    /// Wraps a real in-memory store, counting backend reads.
    struct CountingStorage {
        inner: ObjectStorage,
        gets: AtomicUsize,
    }

    impl CountingStorage {
        fn new() -> Self {
            Self {
                inner: ObjectStorage::in_memory(),
                gets: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Storage for CountingStorage {
        async fn get(&self, key: &str) -> Result<Bytes, DtrError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Bytes, DtrError> {
            self.inner.get_range(key, range).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<(), DtrError> {
            self.inner.put(key, data).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, DtrError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), DtrError> {
            self.inner.delete(key).await
        }
        async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, DtrError> {
            self.inner.head(key).await
        }
    }

    async fn seeded(keys: &[(&str, &[u8])]) -> Arc<CountingStorage> {
        let s = Arc::new(CountingStorage::new());
        for (k, v) in keys {
            s.put(k, Bytes::copy_from_slice(v)).await.unwrap();
        }
        s
    }

    #[tokio::test]
    async fn cache_hit_avoids_backend() {
        let storage = seeded(&[("shard-0", b"aaaa")]).await;
        let engine = IoEngine::new(storage.clone(), 1024, 0);

        assert_eq!(&engine.read("shard-0").await.unwrap()[..], b"aaaa");
        assert_eq!(&engine.read("shard-0").await.unwrap()[..], b"aaaa");
        assert_eq!(&engine.read("shard-0").await.unwrap()[..], b"aaaa");

        assert_eq!(storage.gets.load(Ordering::SeqCst), 1);
        let stats = engine.stats();
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.cache_misses, 1);
        assert!((engine.cache_hit_ratio() - 2.0 / 3.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn eviction_honors_byte_budget() {
        let storage = seeded(&[
            ("a", &[0u8; 40][..]),
            ("b", &[0u8; 40][..]),
            ("c", &[0u8; 40][..]),
        ])
        .await;
        // Budget fits two 40-byte objects.
        let engine = IoEngine::new(storage.clone(), 100, 0);

        engine.read("a").await.unwrap();
        engine.read("b").await.unwrap();
        engine.read("c").await.unwrap(); // evicts "a" (LRU)

        assert_eq!(engine.stats().evictions, 1);
        engine.read("b").await.unwrap(); // still cached
        assert_eq!(storage.gets.load(Ordering::SeqCst), 3);
        engine.read("a").await.unwrap(); // was evicted -> backend read
        assert_eq!(storage.gets.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn prefetch_warms_subsequent_reads() {
        let storage = seeded(&[
            ("s0", b"000"),
            ("s1", b"111"),
            ("s2", b"222"),
            ("s3", b"333"),
        ])
        .await;
        let engine = Arc::new(IoEngine::new(storage.clone(), 1024, 2));
        let keys: Vec<String> = ["s0", "s1", "s2", "s3"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        engine.read("s0").await.unwrap();
        let started = engine.prefetch(&keys, 0); // warms s1, s2
        assert_eq!(started, 2);

        // Wait for background fetches.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let before = storage.gets.load(Ordering::SeqCst);
        engine.read("s1").await.unwrap();
        engine.read("s2").await.unwrap();
        assert_eq!(storage.gets.load(Ordering::SeqCst), before); // pure hits

        let stats = engine.stats();
        assert_eq!(stats.cache_hits, 2);
    }

    #[tokio::test]
    async fn range_read_served_from_cached_object() {
        let storage = seeded(&[("s0", b"hello world")]).await;
        let engine = IoEngine::new(storage.clone(), 1024, 0);

        engine.read("s0").await.unwrap();
        let range = engine.read_range("s0", 6..11).await.unwrap();
        assert_eq!(&range[..], b"world");
        assert_eq!(storage.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_backend_fetch() {
        let storage = seeded(&[("s0", b"data")]).await;
        let engine = Arc::new(IoEngine::new(storage.clone(), 1024, 0));

        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let e = Arc::clone(&engine);
                tokio::spawn(async move { e.read("s0").await.unwrap() })
            })
            .collect();
        for t in tasks {
            assert_eq!(&t.await.unwrap()[..], b"data");
        }
        // Single-flight: all 16 concurrent readers share one backend GET.
        assert_eq!(storage.gets.load(Ordering::SeqCst), 1);
    }
}
