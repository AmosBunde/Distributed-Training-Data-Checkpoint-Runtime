//! Control-plane state machine: membership, shard leases, checkpoint barriers.
//!
//! All state lives behind [`Coordinator`], an async-safe facade the gRPC layer
//! calls into. Time is injected via `tokio::time::Instant` so every expiry
//! path is deterministic under `tokio::time::pause()` in tests.

pub mod barrier;
pub mod lease;
pub mod membership;

use dtr_common::config::CoordinatorSection;
use dtr_common::types::QuorumPolicy;

pub use barrier::{BarrierManager, BarrierStatus, CheckpointBarrier};
pub use lease::{LeaseManager, ShardLease};
pub use membership::{Membership, WorkerInfo};

/// Aggregates the three control-plane managers behind one handle.
pub struct Coordinator {
    pub membership: Membership,
    pub leases: LeaseManager,
    pub barriers: BarrierManager,
}

impl Coordinator {
    pub fn new(cfg: &CoordinatorSection, quorum: QuorumPolicy, barrier_timeout_ms: u64) -> Self {
        Self {
            membership: Membership::new(cfg.heartbeat_interval_ms, cfg.missed_heartbeats_allowed),
            leases: LeaseManager::new(cfg.lease_ttl_ms, cfg.default_shards_per_lease),
            barriers: BarrierManager::new(quorum, barrier_timeout_ms),
        }
    }
}
