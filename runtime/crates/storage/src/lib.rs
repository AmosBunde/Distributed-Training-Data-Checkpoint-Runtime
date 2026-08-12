//! Storage abstraction for the DTR runtime.
//!
//! Everything above this crate (IO engine, checkpoint service) speaks
//! [`Storage`], a thin async facade over [`object_store`] that adds:
//!
//! - URI-scheme routing (`file://`, `s3://`) driven by [`RuntimeConfig`]
//! - byte-range reads for the shard read path
//! - a stable error mapping into `DtrError`
//!
//! `object_store` was chosen over per-cloud SDKs deliberately: one dependency
//! covers local FS, S3, MinIO, and (via feature flags later) GCS/Azure with
//! identical semantics. See docs/adr/0003-object-store-abstraction.md.

use async_trait::async_trait;
use bytes::Bytes;
use dtr_common::config::{RuntimeConfig, StorageBackendKind};
use dtr_common::DtrError;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use std::ops::Range;
use std::sync::Arc;

/// Metadata for one stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Key relative to the backend root, e.g. "checkpoints/run-001/manifests/step-10.json".
    pub key: String,
    pub size_bytes: u64,
}

/// Async storage facade used by the IO engine and checkpoint service.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Read a whole object.
    async fn get(&self, key: &str) -> Result<Bytes, DtrError>;

    /// Read a byte range of an object (half-open, in bytes).
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, DtrError>;

    /// Write a whole object (last write wins).
    async fn put(&self, key: &str, data: Bytes) -> Result<(), DtrError>;

    /// List all objects under a key prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, DtrError>;

    /// Delete one object. Deleting a missing object is not an error.
    async fn delete(&self, key: &str) -> Result<(), DtrError>;

    /// Object size, or None if it does not exist.
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, DtrError>;

    async fn exists(&self, key: &str) -> Result<bool, DtrError> {
        Ok(self.head(key).await?.is_some())
    }
}

/// The one concrete implementation: wraps any `object_store::ObjectStore`.
pub struct ObjectStorage {
    inner: Arc<dyn ObjectStore>,
    /// Human-readable backend description for error context ("fs:/mnt/dtr").
    label: String,
}

impl ObjectStorage {
    pub fn new(inner: Arc<dyn ObjectStore>, label: impl Into<String>) -> Self {
        Self {
            inner,
            label: label.into(),
        }
    }

    /// Build the backend selected by config.
    pub fn from_config(cfg: &RuntimeConfig) -> Result<Self, DtrError> {
        match cfg.storage.backend {
            StorageBackendKind::Fs => {
                let root = &cfg.storage.fs.root_dir;
                std::fs::create_dir_all(root).map_err(|e| DtrError::storage(root.clone(), e))?;
                let store = LocalFileSystem::new_with_prefix(root)
                    .map_err(|e| DtrError::storage(root.clone(), e))?;
                Ok(Self::new(Arc::new(store), format!("fs:{root}")))
            }
            StorageBackendKind::S3 => {
                let s3 = &cfg.storage.s3;
                let mut builder = object_store::aws::AmazonS3Builder::from_env();
                if let Some(endpoint) = &s3.endpoint {
                    builder = builder.with_endpoint(endpoint.clone());
                }
                if let Some(region) = &s3.region {
                    builder = builder.with_region(region.clone());
                }
                if let Some(key) = &s3.access_key_id {
                    builder = builder.with_access_key_id(key.clone());
                }
                if let Some(secret) = &s3.secret_access_key {
                    builder = builder.with_secret_access_key(secret.clone());
                }
                if s3.force_path_style {
                    builder = builder.with_virtual_hosted_style_request(false);
                }
                if s3.allow_http {
                    builder = builder.with_allow_http(true);
                }
                // object_store scopes a store to one bucket; the DTR convention
                // is a single bucket selected via env (S3_BUCKET).
                if let Ok(bucket) = std::env::var("S3_BUCKET") {
                    builder = builder.with_bucket_name(bucket);
                }
                let store = builder.build().map_err(|e| DtrError::storage("s3", e))?;
                let label = format!("s3:{}", s3.endpoint.clone().unwrap_or_else(|| "aws".into()));
                Ok(Self::new(Arc::new(store), label))
            }
        }
    }

    /// In-memory backend for tests.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemory::new()), "mem")
    }

    fn ctx(&self, key: &str) -> String {
        format!("{}/{key}", self.label)
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn get(&self, key: &str) -> Result<Bytes, DtrError> {
        let path = ObjPath::from(key);
        let res = self
            .inner
            .get(&path)
            .await
            .map_err(|e| DtrError::storage(self.ctx(key), e))?;
        res.bytes()
            .await
            .map_err(|e| DtrError::storage(self.ctx(key), e))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, DtrError> {
        let path = ObjPath::from(key);
        self.inner
            .get_range(&path, range.start as usize..range.end as usize)
            .await
            .map_err(|e| DtrError::storage(self.ctx(key), e))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), DtrError> {
        let path = ObjPath::from(key);
        self.inner
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map(|_| ())
            .map_err(|e| DtrError::storage(self.ctx(key), e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, DtrError> {
        let path = ObjPath::from(prefix);
        let metas: Vec<_> = self
            .inner
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(|e| DtrError::storage(self.ctx(prefix), e))?;
        Ok(metas
            .into_iter()
            .map(|m| ObjectMeta {
                key: m.location.to_string(),
                size_bytes: m.size as u64,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), DtrError> {
        let path = ObjPath::from(key);
        match self.inner.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(DtrError::storage(self.ctx(key), e)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, DtrError> {
        let path = ObjPath::from(key);
        match self.inner.head(&path).await {
            Ok(m) => Ok(Some(ObjectMeta {
                key: m.location.to_string(),
                size_bytes: m.size as u64,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(DtrError::storage(self.ctx(key), e)),
        }
    }
}

/// Strip a `file://` or `s3://bucket/` URI down to a backend-relative key.
///
/// The runtime config decides which backend is active; dataset/checkpoint URIs
/// from clients are resolved against it. `s3://bucket/a/b` -> `a/b` (the bucket
/// is part of backend construction), `file:///mnt/dtr/a/b` -> resolved against
/// the fs root by object_store itself, plain `a/b` passes through.
pub fn uri_to_key(uri: &str) -> Result<String, DtrError> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let mut parts = rest.splitn(2, '/');
        let _bucket = parts.next().unwrap_or_default();
        return Ok(parts
            .next()
            .unwrap_or_default()
            .trim_matches('/')
            .to_owned());
    }
    if let Some(rest) = uri.strip_prefix("file://") {
        return Ok(rest.trim_matches('/').to_owned());
    }
    if uri.contains("://") {
        return Err(DtrError::InvalidArgument(format!(
            "unsupported storage URI scheme: {uri}"
        )));
    }
    Ok(uri.trim_matches('/').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn uri_routing() {
        assert_eq!(uri_to_key("s3://bucket/a/b/").unwrap(), "a/b");
        assert_eq!(uri_to_key("file:///mnt/dtr/x").unwrap(), "mnt/dtr/x");
        assert_eq!(uri_to_key("plain/key").unwrap(), "plain/key");
        assert!(uri_to_key("gs://nope/x").is_err());
    }

    #[test]
    fn fs_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStorage::new(
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            "fs:test",
        );
        rt().block_on(async {
            store
                .put("data/shard-0000.bin", Bytes::from_static(b"hello world"))
                .await
                .unwrap();

            let all = store.get("data/shard-0000.bin").await.unwrap();
            assert_eq!(&all[..], b"hello world");

            let range = store.get_range("data/shard-0000.bin", 6..11).await.unwrap();
            assert_eq!(&range[..], b"world");

            let listed = store.list("data").await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].size_bytes, 11);

            let head = store.head("data/shard-0000.bin").await.unwrap().unwrap();
            assert_eq!(head.size_bytes, 11);
            assert!(store.exists("data/shard-0000.bin").await.unwrap());

            store.delete("data/shard-0000.bin").await.unwrap();
            assert!(!store.exists("data/shard-0000.bin").await.unwrap());
            // Deleting again is not an error.
            store.delete("data/shard-0000.bin").await.unwrap();
        });
    }

    #[test]
    fn missing_object_is_error_on_get_none_on_head() {
        let store = ObjectStorage::in_memory();
        rt().block_on(async {
            assert!(store.get("nope").await.is_err());
            assert!(store.head("nope").await.unwrap().is_none());
        });
    }

    #[test]
    fn list_scopes_to_prefix() {
        let store = ObjectStorage::in_memory();
        rt().block_on(async {
            store.put("a/1", Bytes::from_static(b"x")).await.unwrap();
            store.put("a/2", Bytes::from_static(b"y")).await.unwrap();
            store.put("b/1", Bytes::from_static(b"z")).await.unwrap();
            let listed = store.list("a").await.unwrap();
            assert_eq!(listed.len(), 2);
        });
    }
}
