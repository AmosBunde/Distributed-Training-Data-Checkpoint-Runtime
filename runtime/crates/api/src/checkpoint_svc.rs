//! CheckpointService: barriers, chunk integrity reporting, commit/abort/recover.

use crate::pb::checkpoint::checkpoint_service_server::CheckpointService;
use crate::pb::checkpoint::commit_checkpoint_response::Status as CommitStatus;
use crate::pb::checkpoint::*;
use crate::{timestamp_after, to_status, AppState};
use dtr_checkpoint::ChunkRecord;
use dtr_common::types::{CheckpointToken, TargetId, WorkerId};
use dtr_common::DtrError;
use dtr_coordinator::BarrierStatus;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct CheckpointSvc {
    state: Arc<AppState>,
}

impl CheckpointSvc {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl CheckpointService for CheckpointSvc {
    async fn register_checkpoint_target(
        &self,
        request: Request<RegisterCheckpointTargetRequest>,
    ) -> Result<Response<RegisterCheckpointTargetResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("checkpoint spec is required"))?;
        if !spec.write_mode.is_empty() && spec.write_mode != "atomic" {
            return Err(Status::invalid_argument(format!(
                "unsupported write_mode '{}' (v1 supports: atomic)",
                spec.write_mode
            )));
        }
        let (info, already) = self
            .state
            .register_target(&spec.uri, spec.chunk_bytes, spec.keep_last)
            .map_err(to_status)?;
        tracing::info!(target_id = %info.target_id, uri = %spec.uri, "checkpoint target registered");
        Ok(Response::new(RegisterCheckpointTargetResponse {
            target_id: info.target_id.to_string(),
            already_registered: already,
        }))
    }

    async fn begin_checkpoint(
        &self,
        request: Request<BeginCheckpointRequest>,
    ) -> Result<Response<BeginCheckpointResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let target = self
            .state
            .target(&TargetId::new(&req.target_id))
            .map_err(to_status)?;

        let expected = self.state.coordinator.membership.expected_world_size();
        if expected == 0 {
            return Err(Status::failed_precondition(
                "no registered workers; register workers before checkpointing",
            ));
        }
        let barrier =
            self.state
                .coordinator
                .barriers
                .begin(target.target_id.clone(), req.step, expected);

        Ok(Response::new(BeginCheckpointResponse {
            checkpoint_token: barrier.token.to_string(),
            upload_prefix: dtr_checkpoint::layout::tmp_prefix(&target.prefix, req.step),
            expected_participants: barrier.expected,
            required_participants: barrier.required,
            barrier_deadline: Some(timestamp_after(
                self.state.cfg.checkpoint.barrier_timeout_ms,
            )),
        }))
    }

    async fn report_chunk(
        &self,
        request: Request<ReportChunkRequest>,
    ) -> Result<Response<ReportChunkResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let token = CheckpointToken::new(&req.checkpoint_token);

        let records: Vec<ChunkRecord> = req
            .chunks
            .iter()
            .map(|c| ChunkRecord {
                key: c.key.clone(),
                size_bytes: c.size_bytes,
                sha256: c.sha256.clone(),
                rank: c.rank,
                part_index: c.part_index,
            })
            .collect();
        self.state.record_chunks(&token, records);

        let mut completed = {
            let barrier = self
                .state
                .coordinator
                .barriers
                .get(&token)
                .map_err(to_status)?;
            barrier.completed_ranks.len() as u32
        };
        if req.rank_complete {
            let rank = self
                .state
                .coordinator
                .membership
                .get(&WorkerId::new(&req.worker_id))
                .map(|w| w.rank)
                .or_else(|| req.chunks.first().map(|c| c.rank))
                .ok_or_else(|| {
                    Status::invalid_argument("cannot resolve rank for completion report")
                })?;
            completed = self
                .state
                .coordinator
                .barriers
                .rank_complete(&token, rank)
                .map_err(to_status)?;
        }
        Ok(Response::new(ReportChunkResponse {
            completed_participants: completed,
        }))
    }

    async fn commit_checkpoint(
        &self,
        request: Request<CommitCheckpointRequest>,
    ) -> Result<Response<CommitCheckpointResponse>, Status> {
        let start = std::time::Instant::now();
        let res = self.commit_inner(request.into_inner()).await;
        self.state.metrics.observe_rpc(
            "CommitCheckpoint",
            &crate::code_of(&res),
            start.elapsed().as_secs_f64(),
        );
        res
    }

    async fn abort_checkpoint(
        &self,
        request: Request<AbortCheckpointRequest>,
    ) -> Result<Response<AbortCheckpointResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let token = CheckpointToken::new(&req.checkpoint_token);
        self.state
            .coordinator
            .barriers
            .abort(&token, req.reason)
            .map_err(to_status)?;
        self.state.drop_reported(&token);
        self.state.metrics.checkpoint_aborts_total.inc();
        Ok(Response::new(AbortCheckpointResponse {}))
    }

    async fn get_latest_manifest(
        &self,
        request: Request<GetLatestManifestRequest>,
    ) -> Result<Response<GetLatestManifestResponse>, Status> {
        let req = request.into_inner();
        let target = self
            .state
            .target(&TargetId::new(&req.target_id))
            .map_err(to_status)?;
        match self
            .state
            .ckpt
            .latest_manifest(&target.prefix)
            .await
            .map_err(to_status)?
        {
            Some(manifest) => Ok(Response::new(GetLatestManifestResponse {
                found: true,
                step: manifest.step,
                manifest_key: dtr_checkpoint::layout::manifest_key(&target.prefix, manifest.step),
                chunks: manifest
                    .chunks
                    .into_iter()
                    .map(|c| ManifestChunk {
                        key: c.key,
                        size_bytes: c.size_bytes,
                        sha256: c.sha256,
                        rank: c.rank,
                        part_index: c.part_index,
                    })
                    .collect(),
                committed_at: None,
            })),
            None => Ok(Response::new(GetLatestManifestResponse {
                found: false,
                step: 0,
                manifest_key: String::new(),
                chunks: vec![],
                committed_at: None,
            })),
        }
    }
}

impl CheckpointSvc {
    async fn commit_inner(
        &self,
        req: CommitCheckpointRequest,
    ) -> Result<Response<CommitCheckpointResponse>, Status> {
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let token = CheckpointToken::new(&req.checkpoint_token);
        let barrier = self
            .state
            .coordinator
            .barriers
            .get(&token)
            .map_err(to_status)?;

        match barrier.status {
            BarrierStatus::Pending => Ok(Response::new(CommitCheckpointResponse {
                status: CommitStatus::Pending as i32,
                manifest_key: String::new(),
            })),
            BarrierStatus::Aborted => Ok(Response::new(CommitCheckpointResponse {
                status: CommitStatus::Aborted as i32,
                manifest_key: String::new(),
            })),
            BarrierStatus::Committed => {
                // Idempotent re-commit: return the already-published manifest.
                let target = self.state.target(&barrier.target_id).map_err(to_status)?;
                Ok(Response::new(CommitCheckpointResponse {
                    status: CommitStatus::Committed as i32,
                    manifest_key: dtr_checkpoint::layout::manifest_key(
                        &target.prefix,
                        barrier.step,
                    ),
                }))
            }
            BarrierStatus::Ready => {
                let target = self.state.target(&barrier.target_id).map_err(to_status)?;
                let chunks = self.state.reported_chunks(&token);
                let total_bytes: u64 = chunks.iter().map(|c| c.size_bytes).sum();
                match self
                    .state
                    .ckpt
                    .commit(&target.prefix, barrier.step, chunks, true)
                    .await
                {
                    Ok(manifest_key) => {
                        self.state
                            .coordinator
                            .barriers
                            .mark_committed(&token)
                            .map_err(to_status)?;
                        self.state.drop_reported(&token);
                        self.state.metrics.checkpoint_commits_total.inc();
                        self.state
                            .metrics
                            .checkpoint_bytes_total
                            .inc_by(total_bytes);
                        tracing::info!(step = barrier.step, manifest = %manifest_key, bytes = total_bytes, "checkpoint committed");
                        Ok(Response::new(CommitCheckpointResponse {
                            status: CommitStatus::Committed as i32,
                            manifest_key,
                        }))
                    }
                    Err(e @ DtrError::CheckpointCorrupt { .. }) => {
                        tracing::error!(step = barrier.step, error = %e, "checkpoint verification failed; aborting");
                        self.state
                            .coordinator
                            .barriers
                            .abort(&token, e.to_string())
                            .map_err(to_status)?;
                        self.state.drop_reported(&token);
                        self.state.metrics.checkpoint_aborts_total.inc();
                        Ok(Response::new(CommitCheckpointResponse {
                            status: CommitStatus::Aborted as i32,
                            manifest_key: String::new(),
                        }))
                    }
                    Err(e) => Err(to_status(e)),
                }
            }
        }
    }
}
