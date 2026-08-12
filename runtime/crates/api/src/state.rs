//! Shared application state wired once in `runtime-server` and handed to all
//! three gRPC services.

use dtr_checkpoint::{CheckpointStore, ChunkRecord};
use dtr_common::config::RuntimeConfig;
use dtr_common::types::{CheckpointToken, TargetId, WorkerId};
use dtr_common::DtrError;
use dtr_coordinator::Coordinator;
use dtr_io_engine::IoEngine;
use dtr_storage::Storage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A registered checkpoint destination.
#[derive(Debug, Clone)]
pub struct TargetInfo {
    pub target_id: TargetId,
    /// Backend-relative key prefix (URI already routed via uri_to_key).
    pub prefix: String,
    pub chunk_bytes: u64,
    pub keep_last: u32,
}

pub struct AppState {
    pub cfg: RuntimeConfig,
    pub coordinator: Coordinator,
    pub storage: Arc<dyn Storage>,
    pub io: Arc<IoEngine>,
    pub ckpt: CheckpointStore,
    /// Registered checkpoint targets by id.
    targets: Mutex<HashMap<TargetId, TargetInfo>>,
    /// Chunk records reported per open checkpoint token.
    reported: Mutex<HashMap<CheckpointToken, Vec<ChunkRecord>>>,
}

impl AppState {
    pub fn new(cfg: RuntimeConfig, storage: Arc<dyn Storage>) -> Self {
        let coordinator = Coordinator::new(
            &cfg.coordinator,
            cfg.checkpoint.quorum,
            cfg.checkpoint.barrier_timeout_ms,
        );
        let io = Arc::new(IoEngine::new(
            storage.clone(),
            cfg.io.cache_bytes,
            cfg.io.prefetch_depth,
        ));
        let ckpt = CheckpointStore::new(storage.clone());
        Self {
            cfg,
            coordinator,
            storage,
            io,
            ckpt,
            targets: Mutex::new(HashMap::new()),
            reported: Mutex::new(HashMap::new()),
        }
    }

    /// Authenticate a (worker, token) pair against membership.
    pub fn auth(&self, worker_id: &str, token: &str) -> Result<WorkerId, DtrError> {
        let id = WorkerId::new(worker_id);
        self.coordinator.membership.authenticate(&id, token)?;
        Ok(id)
    }

    /// Register (or look up) a checkpoint target. Deterministic id per URI.
    pub fn register_target(
        &self,
        uri: &str,
        chunk_bytes: u64,
        keep_last: u32,
    ) -> Result<(TargetInfo, bool), DtrError> {
        let prefix = dtr_storage::uri_to_key(uri)?;
        let target_id = TargetId::new(format!("tgt-{:016x}", fnv(uri.as_bytes())));
        let mut targets = self.targets.lock().unwrap();
        let existed = targets.contains_key(&target_id);
        let info = targets
            .entry(target_id.clone())
            .or_insert_with(|| TargetInfo {
                target_id: target_id.clone(),
                prefix,
                chunk_bytes: if chunk_bytes == 0 {
                    self.cfg.checkpoint.default_chunk_bytes
                } else {
                    chunk_bytes
                },
                keep_last,
            })
            .clone();
        Ok((info, existed))
    }

    pub fn target(&self, target_id: &TargetId) -> Result<TargetInfo, DtrError> {
        self.targets
            .lock()
            .unwrap()
            .get(target_id)
            .cloned()
            .ok_or_else(|| DtrError::InvalidArgument(format!("unknown target {target_id}")))
    }

    pub fn record_chunks(&self, token: &CheckpointToken, chunks: Vec<ChunkRecord>) {
        self.reported
            .lock()
            .unwrap()
            .entry(token.clone())
            .or_default()
            .extend(chunks);
    }

    pub fn reported_chunks(&self, token: &CheckpointToken) -> Vec<ChunkRecord> {
        self.reported
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .unwrap_or_default()
    }

    pub fn drop_reported(&self, token: &CheckpointToken) {
        self.reported.lock().unwrap().remove(token);
    }

    /// Steps with an open (pending/ready) barrier — the GC spare list.
    pub fn in_flight_steps(&self) -> Vec<u64> {
        // Reported map keys are open tokens; resolve steps via barrier state.
        let tokens: Vec<CheckpointToken> = self.reported.lock().unwrap().keys().cloned().collect();
        tokens
            .iter()
            .filter_map(|t| self.coordinator.barriers.get(t).ok())
            .filter(|b| {
                matches!(
                    b.status,
                    dtr_coordinator::BarrierStatus::Pending | dtr_coordinator::BarrierStatus::Ready
                )
            })
            .map(|b| b.step)
            .collect()
    }
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}
