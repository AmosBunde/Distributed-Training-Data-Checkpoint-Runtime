//! RuntimeService: membership, datasets, shard leases.

use crate::pb::runtime::runtime_service_server::RuntimeService;
use crate::pb::runtime::*;
use crate::{timestamp_after, to_status, AppState};
use dtr_common::types::{DatasetId, LeaseId, WorkerId};
use dtr_common::DtrError;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RuntimeSvc {
    state: Arc<AppState>,
}

impl RuntimeSvc {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl RuntimeService for RuntimeSvc {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        if req.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }
        let info = self.state.coordinator.membership.register(
            WorkerId::new(&req.worker_id),
            req.rank,
            req.world_size,
            req.labels,
        );
        tracing::info!(worker_id = %req.worker_id, rank = req.rank, "worker registered");
        Ok(Response::new(RegisterWorkerResponse {
            session_token: info.session_token,
            heartbeat_interval_ms: self.state.coordinator.membership.heartbeat_interval_ms(),
            missed_heartbeats_allowed: self
                .state
                .coordinator
                .membership
                .missed_heartbeats_allowed(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let result = self.state.coordinator.membership.heartbeat(
            &WorkerId::new(&req.worker_id),
            &req.session_token,
            req.current_step,
        );
        let must_reregister = match result {
            Ok(()) => false,
            // Evicted or restarted coordinator: instruct re-registration
            // instead of erroring — recovery is protocol-driven.
            Err(DtrError::UnknownWorker(_)) => true,
            Err(e) => return Err(to_status(e)),
        };
        Ok(Response::new(HeartbeatResponse {
            must_reregister,
            server_time: Some(timestamp_after(0)),
        }))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("dataset spec is required"))?;
        if spec.num_shards == 0 {
            return Err(Status::invalid_argument("num_shards must be > 0"));
        }
        let (dataset_id, already) = self
            .state
            .coordinator
            .leases
            .register_dataset(&spec.uri, spec.num_shards);
        tracing::info!(dataset_id = %dataset_id, uri = %spec.uri, shards = spec.num_shards, "dataset registered");
        Ok(Response::new(RegisterDatasetResponse {
            dataset_id: dataset_id.to_string(),
            already_registered: already,
        }))
    }

    async fn acquire_shard_lease(
        &self,
        request: Request<AcquireShardLeaseRequest>,
    ) -> Result<Response<AcquireShardLeaseResponse>, Status> {
        let req = request.into_inner();
        let worker = self
            .state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let lease = self
            .state
            .coordinator
            .leases
            .acquire(
                worker,
                &DatasetId::new(&req.dataset_id),
                req.epoch,
                req.requested_shards,
            )
            .map_err(to_status)?;
        let ttl = self.state.coordinator.leases.ttl_ms();
        Ok(Response::new(match lease {
            Some(lease) => AcquireShardLeaseResponse {
                lease_id: lease.lease_id.to_string(),
                shards: Some(ShardRange {
                    begin: lease.shards.begin,
                    end: lease.shards.end,
                }),
                lease_ttl_ms: ttl,
                expires_at: Some(timestamp_after(ttl)),
            },
            // Pool exhausted for this epoch: empty response by contract.
            None => AcquireShardLeaseResponse {
                lease_id: String::new(),
                shards: None,
                lease_ttl_ms: 0,
                expires_at: None,
            },
        }))
    }

    async fn renew_shard_lease(
        &self,
        request: Request<RenewShardLeaseRequest>,
    ) -> Result<Response<RenewShardLeaseResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        match self
            .state
            .coordinator
            .leases
            .renew(&LeaseId::new(&req.lease_id))
        {
            Ok(_) => Ok(Response::new(RenewShardLeaseResponse {
                expires_at: Some(timestamp_after(self.state.coordinator.leases.ttl_ms())),
                renewed: true,
            })),
            // Fencing signal, not an error: the worker must stop reading.
            Err(DtrError::LeaseExpired(_)) => Ok(Response::new(RenewShardLeaseResponse {
                expires_at: None,
                renewed: false,
            })),
            Err(e) => Err(to_status(e)),
        }
    }

    async fn release_shard_lease(
        &self,
        request: Request<ReleaseShardLeaseRequest>,
    ) -> Result<Response<ReleaseShardLeaseResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        match self
            .state
            .coordinator
            .leases
            .release(&LeaseId::new(&req.lease_id), req.completed)
        {
            // Releasing an already-expired lease is a no-op, not an error.
            Ok(()) | Err(DtrError::UnknownLease(_)) => {
                Ok(Response::new(ReleaseShardLeaseResponse {}))
            }
            Err(e) => Err(to_status(e)),
        }
    }
}
