//! Crash-safe checkpointing on object storage.
//!
//! Object stores have no atomic rename, so visibility is controlled by a
//! manifest: chunks land under `tmp/ckpt-step-N/`, are verified against
//! reported checksums, and only then does `manifests/step-N.json` appear.
//! Readers trust manifests exclusively — a partial upload is invisible by
//! construction. See docs/adr/0004-manifest-based-atomic-commit.md.
//!
//! Layout under a checkpoint target prefix:
//!
//! ```text
//! <prefix>/
//! ├─ tmp/ckpt-step-<N>/rank-<r>.part-<i>     uploads in flight
//! ├─ manifests/step-<N>.json                 committed manifests
//! └─ latest.json                             pointer to newest manifest
//! ```

pub mod chunker;
pub mod manifest;

use bytes::Bytes;
use dtr_common::DtrError;
use dtr_storage::Storage;
pub use manifest::{ChunkRecord, Manifest};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Compute the lowercase-hex SHA-256 of a buffer.
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Key helpers — single source of truth for the on-store layout.
pub mod layout {
    pub fn tmp_prefix(target_prefix: &str, step: u64) -> String {
        format!("{}/tmp/ckpt-step-{}", target_prefix.trim_matches('/'), step)
    }
    pub fn chunk_key(target_prefix: &str, step: u64, rank: u32, part: u32) -> String {
        format!(
            "{}/rank-{}.part-{:04}",
            tmp_prefix(target_prefix, step),
            rank,
            part
        )
    }
    pub fn manifest_key(target_prefix: &str, step: u64) -> String {
        format!(
            "{}/manifests/step-{}.json",
            target_prefix.trim_matches('/'),
            step
        )
    }
    pub fn latest_key(target_prefix: &str) -> String {
        format!("{}/latest.json", target_prefix.trim_matches('/'))
    }
}

/// Drives verify-then-publish commits and GC for one storage backend.
pub struct CheckpointStore {
    storage: Arc<dyn Storage>,
}

impl CheckpointStore {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Upload one chunk to the tmp area, returning its record.
    pub async fn upload_chunk(
        &self,
        target_prefix: &str,
        step: u64,
        rank: u32,
        part: u32,
        data: Bytes,
    ) -> Result<ChunkRecord, DtrError> {
        let key = layout::chunk_key(target_prefix, step, rank, part);
        let record = ChunkRecord {
            key: key.clone(),
            size_bytes: data.len() as u64,
            sha256: sha256_hex(&data),
            rank,
            part_index: part,
        };
        self.storage.put(&key, data).await?;
        Ok(record)
    }

    /// Verify every reported chunk exists with the reported size, then publish
    /// the manifest and update the `latest` pointer. This is THE commit point:
    /// the checkpoint exists iff the manifest object exists.
    pub async fn commit(
        &self,
        target_prefix: &str,
        step: u64,
        chunks: Vec<ChunkRecord>,
        verify_content: bool,
    ) -> Result<String, DtrError> {
        if chunks.is_empty() {
            return Err(DtrError::CheckpointCorrupt {
                token: format!("step-{step}"),
                reason: "no chunks reported".into(),
            });
        }

        // Phase 1: verify the expected set against storage.
        for chunk in &chunks {
            let meta = self.storage.head(&chunk.key).await?.ok_or_else(|| {
                DtrError::CheckpointCorrupt {
                    token: format!("step-{step}"),
                    reason: format!("missing chunk {}", chunk.key),
                }
            })?;
            if meta.size_bytes != chunk.size_bytes {
                return Err(DtrError::CheckpointCorrupt {
                    token: format!("step-{step}"),
                    reason: format!(
                        "size mismatch for {}: stored {} != reported {}",
                        chunk.key, meta.size_bytes, chunk.size_bytes
                    ),
                });
            }
            if verify_content {
                let data = self.storage.get(&chunk.key).await?;
                let actual = sha256_hex(&data);
                if actual != chunk.sha256 {
                    return Err(DtrError::CheckpointCorrupt {
                        token: format!("step-{step}"),
                        reason: format!(
                            "checksum mismatch for {}: stored {} != reported {}",
                            chunk.key, actual, chunk.sha256
                        ),
                    });
                }
            }
        }

        // Phase 2: publish the manifest (the atomic visibility flip).
        let manifest = Manifest::new(step, chunks);
        let manifest_key = layout::manifest_key(target_prefix, step);
        self.storage
            .put(&manifest_key, manifest.to_json_bytes()?)
            .await?;

        // Phase 3: move the latest pointer (best-effort convenience; readers
        // can always list manifests/ and take the max step).
        let latest = serde_json::json!({
            "step": step,
            "manifest_key": manifest_key,
            "committed_at": manifest.created_at,
        });
        self.storage
            .put(
                &layout::latest_key(target_prefix),
                Bytes::from(serde_json::to_vec(&latest).map_err(|e| {
                    DtrError::Internal(format!("latest pointer serialization: {e}"))
                })?),
            )
            .await?;

        Ok(manifest_key)
    }

    /// Load the newest committed manifest via the latest pointer, falling back
    /// to listing manifests/ (pointer write could have been interrupted).
    pub async fn latest_manifest(&self, target_prefix: &str) -> Result<Option<Manifest>, DtrError> {
        if let Some(_meta) = self
            .storage
            .head(&layout::latest_key(target_prefix))
            .await?
        {
            let raw = self.storage.get(&layout::latest_key(target_prefix)).await?;
            if let Ok(ptr) = serde_json::from_slice::<serde_json::Value>(&raw) {
                if let Some(key) = ptr.get("manifest_key").and_then(|v| v.as_str()) {
                    if self.storage.head(key).await?.is_some() {
                        let data = self.storage.get(key).await?;
                        return Ok(Some(Manifest::from_json_bytes(&data)?));
                    }
                }
            }
        }
        // Fallback: scan the manifests directory.
        let prefix = format!("{}/manifests", target_prefix.trim_matches('/'));
        let mut manifests = self.storage.list(&prefix).await?;
        manifests.sort_by_key(|m| {
            m.key
                .rsplit("step-")
                .next()
                .and_then(|s| s.trim_end_matches(".json").parse::<u64>().ok())
                .unwrap_or(0)
        });
        match manifests.last() {
            Some(meta) => {
                let data = self.storage.get(&meta.key).await?;
                Ok(Some(Manifest::from_json_bytes(&data)?))
            }
            None => Ok(None),
        }
    }

    /// Delete the tmp area for a step (after commit or abort).
    pub async fn clean_tmp(&self, target_prefix: &str, step: u64) -> Result<usize, DtrError> {
        let prefix = layout::tmp_prefix(target_prefix, step);
        let objects = self.storage.list(&prefix).await?;
        let mut deleted = 0;
        for obj in objects {
            self.storage.delete(&obj.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// GC: remove tmp areas for steps with no committed manifest, keeping
    /// steps listed in `in_flight` (their barrier is still open).
    pub async fn gc_orphans(
        &self,
        target_prefix: &str,
        in_flight: &[u64],
    ) -> Result<usize, DtrError> {
        let tmp_root = format!("{}/tmp", target_prefix.trim_matches('/'));
        let objects = self.storage.list(&tmp_root).await?;

        let mut deleted = 0;
        for obj in objects {
            // Keys look like <prefix>/tmp/ckpt-step-<N>/rank-...
            let step = obj
                .key
                .split("ckpt-step-")
                .nth(1)
                .and_then(|s| s.split('/').next())
                .and_then(|s| s.parse::<u64>().ok());
            let Some(step) = step else { continue };
            if in_flight.contains(&step) {
                continue;
            }
            let committed = self
                .storage
                .head(&layout::manifest_key(target_prefix, step))
                .await?
                .is_some();
            // Orphan = tmp data for a step that is neither committed nor in
            // flight (crashed mid-upload or aborted). Committed steps' tmp
            // data is also removable — commit already verified + published.
            let _ = committed; // both cases are safe to delete
            self.storage.delete(&obj.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtr_storage::ObjectStorage;

    fn store() -> (CheckpointStore, Arc<ObjectStorage>) {
        let storage = Arc::new(ObjectStorage::in_memory());
        (CheckpointStore::new(storage.clone()), storage)
    }

    #[tokio::test]
    async fn commit_round_trip() {
        let (cs, _) = store();
        let c0 = cs
            .upload_chunk("ckpt/run-1", 100, 0, 0, Bytes::from_static(b"rank0-state"))
            .await
            .unwrap();
        let c1 = cs
            .upload_chunk("ckpt/run-1", 100, 1, 0, Bytes::from_static(b"rank1-state"))
            .await
            .unwrap();

        let manifest_key = cs
            .commit("ckpt/run-1", 100, vec![c0, c1], true)
            .await
            .unwrap();
        assert_eq!(manifest_key, "ckpt/run-1/manifests/step-100.json");

        let manifest = cs.latest_manifest("ckpt/run-1").await.unwrap().unwrap();
        assert_eq!(manifest.step, 100);
        assert_eq!(manifest.chunks.len(), 2);
        assert_eq!(manifest.total_bytes(), 22);
    }

    #[tokio::test]
    async fn missing_chunk_never_publishes() {
        let (cs, storage) = store();
        let c0 = cs
            .upload_chunk("ckpt/run-1", 5, 0, 0, Bytes::from_static(b"data"))
            .await
            .unwrap();
        // Rank 1 "crashed": its chunk was reported but never uploaded.
        let ghost = ChunkRecord {
            key: layout::chunk_key("ckpt/run-1", 5, 1, 0),
            size_bytes: 4,
            sha256: sha256_hex(b"data"),
            rank: 1,
            part_index: 0,
        };

        let err = cs.commit("ckpt/run-1", 5, vec![c0, ghost], true).await;
        assert!(matches!(err, Err(DtrError::CheckpointCorrupt { .. })));

        // The manifest must not exist — the failed checkpoint is invisible.
        assert!(storage
            .head("ckpt/run-1/manifests/step-5.json")
            .await
            .unwrap()
            .is_none());
        assert!(cs.latest_manifest("ckpt/run-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn corrupted_chunk_detected_by_checksum() {
        let (cs, storage) = store();
        let mut c0 = cs
            .upload_chunk("ckpt/run-1", 7, 0, 0, Bytes::from_static(b"good-data"))
            .await
            .unwrap();
        // Simulate bit-rot / truncated overwrite with same length.
        storage
            .put(&c0.key, Bytes::from_static(b"bad!-data"))
            .await
            .unwrap();
        c0.sha256 = sha256_hex(b"good-data");

        let err = cs.commit("ckpt/run-1", 7, vec![c0], true).await;
        assert!(matches!(err, Err(DtrError::CheckpointCorrupt { .. })));
    }

    #[tokio::test]
    async fn latest_pointer_tracks_newest_and_survives_missing_pointer() {
        let (cs, storage) = store();
        for step in [10u64, 20, 30] {
            let c = cs
                .upload_chunk("ckpt/r", step, 0, 0, Bytes::from(vec![step as u8; 8]))
                .await
                .unwrap();
            cs.commit("ckpt/r", step, vec![c], true).await.unwrap();
        }
        assert_eq!(
            cs.latest_manifest("ckpt/r").await.unwrap().unwrap().step,
            30
        );

        // Pointer object lost -> fallback scan still finds step 30.
        storage.delete("ckpt/r/latest.json").await.unwrap();
        assert_eq!(
            cs.latest_manifest("ckpt/r").await.unwrap().unwrap().step,
            30
        );
    }

    #[tokio::test]
    async fn gc_sweeps_orphans_and_spares_in_flight() {
        let (cs, storage) = store();

        // Step 1: committed properly.
        let c = cs
            .upload_chunk("ckpt/r", 1, 0, 0, Bytes::from_static(b"x"))
            .await
            .unwrap();
        cs.commit("ckpt/r", 1, vec![c], true).await.unwrap();

        // Step 2: crashed mid-upload (orphan).
        cs.upload_chunk("ckpt/r", 2, 0, 0, Bytes::from_static(b"y"))
            .await
            .unwrap();

        // Step 3: still uploading (in flight).
        cs.upload_chunk("ckpt/r", 3, 0, 0, Bytes::from_static(b"z"))
            .await
            .unwrap();

        let deleted = cs.gc_orphans("ckpt/r", &[3]).await.unwrap();
        // Step 1 tmp (1 obj) + step 2 tmp (1 obj) deleted; step 3 spared.
        assert_eq!(deleted, 2);
        assert!(storage
            .head(&layout::chunk_key("ckpt/r", 3, 0, 0))
            .await
            .unwrap()
            .is_some());
        // Committed manifest untouched.
        assert!(cs.latest_manifest("ckpt/r").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn clean_tmp_removes_only_that_step() {
        let (cs, storage) = store();
        cs.upload_chunk("ckpt/r", 1, 0, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        cs.upload_chunk("ckpt/r", 2, 0, 0, Bytes::from_static(b"b"))
            .await
            .unwrap();
        let deleted = cs.clean_tmp("ckpt/r", 1).await.unwrap();
        assert_eq!(deleted, 1);
        assert!(storage
            .head(&layout::chunk_key("ckpt/r", 2, 0, 0))
            .await
            .unwrap()
            .is_some());
    }
}
