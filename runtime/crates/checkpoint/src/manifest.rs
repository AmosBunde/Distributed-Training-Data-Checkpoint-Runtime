//! Checkpoint manifest: the single source of truth for a committed checkpoint.

use dtr_common::DtrError;
use serde::{Deserialize, Serialize};

pub const MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRecord {
    /// Full storage key of the chunk (in the tmp area).
    pub key: String,
    pub size_bytes: u64,
    /// Lowercase hex SHA-256 of the chunk contents.
    pub sha256: String,
    pub rank: u32,
    pub part_index: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version for forward compatibility.
    pub format_version: u32,
    pub step: u64,
    pub chunks: Vec<ChunkRecord>,
    /// RFC 3339 commit timestamp.
    pub created_at: String,
}

impl Manifest {
    pub fn new(step: u64, mut chunks: Vec<ChunkRecord>) -> Self {
        // Deterministic order: by rank then part. Makes manifests diffable
        // and restore iteration order stable.
        chunks.sort_by_key(|c| (c.rank, c.part_index));
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            step,
            chunks,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.chunks.iter().map(|c| c.size_bytes).sum()
    }

    pub fn ranks(&self) -> Vec<u32> {
        let mut ranks: Vec<u32> = self.chunks.iter().map(|c| c.rank).collect();
        ranks.sort_unstable();
        ranks.dedup();
        ranks
    }

    pub fn to_json_bytes(&self) -> Result<bytes::Bytes, DtrError> {
        serde_json::to_vec_pretty(self)
            .map(bytes::Bytes::from)
            .map_err(|e| DtrError::Internal(format!("manifest serialization: {e}")))
    }

    pub fn from_json_bytes(data: &[u8]) -> Result<Self, DtrError> {
        serde_json::from_slice(data)
            .map_err(|e| DtrError::Internal(format!("manifest deserialization: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(rank: u32, part: u32) -> ChunkRecord {
        ChunkRecord {
            key: format!("tmp/ckpt-step-1/rank-{rank}.part-{part:04}"),
            size_bytes: 10,
            sha256: "ab".repeat(32),
            rank,
            part_index: part,
        }
    }

    #[test]
    fn manifest_orders_chunks_deterministically() {
        let m = Manifest::new(1, vec![chunk(1, 1), chunk(0, 1), chunk(1, 0), chunk(0, 0)]);
        let order: Vec<(u32, u32)> = m.chunks.iter().map(|c| (c.rank, c.part_index)).collect();
        assert_eq!(order, vec![(0, 0), (0, 1), (1, 0), (1, 1)]);
    }

    #[test]
    fn json_round_trip() {
        let m = Manifest::new(42, vec![chunk(0, 0), chunk(1, 0)]);
        let bytes = m.to_json_bytes().unwrap();
        let back = Manifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(m, back);
        assert_eq!(back.total_bytes(), 20);
        assert_eq!(back.ranks(), vec![0, 1]);
        assert_eq!(back.format_version, MANIFEST_FORMAT_VERSION);
    }
}
