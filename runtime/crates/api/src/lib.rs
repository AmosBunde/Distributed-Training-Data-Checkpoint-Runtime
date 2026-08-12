//! gRPC service implementations for the DTR runtime.
//!
//! Three tonic services over one shared [`AppState`]:
//! - `RuntimeService` (runtime.proto): membership, datasets, shard leases
//! - `CheckpointService` (checkpoint.proto): barriers, integrity, commit
//! - `DataService` (data.proto): cached reads + proxied chunk uploads
//!
//! All domain errors (`DtrError`) map to stable gRPC status codes in
//! [`to_status`]; the services themselves contain orchestration only —
//! every rule lives in the coordinator/checkpoint/io crates.

pub mod state;

mod checkpoint_svc;
mod data_svc;
mod runtime_svc;

pub use checkpoint_svc::CheckpointSvc;
pub use data_svc::DataSvc;
pub use runtime_svc::RuntimeSvc;
pub use state::AppState;

pub mod pb {
    pub mod runtime {
        tonic::include_proto!("dtr.runtime.v1");
    }
    pub mod checkpoint {
        tonic::include_proto!("dtr.checkpoint.v1");
    }
    pub mod data {
        tonic::include_proto!("dtr.data.v1");
    }
}

use dtr_common::DtrError;
use tonic::Status;

/// Map the domain error taxonomy onto stable gRPC status codes.
pub fn to_status(err: DtrError) -> Status {
    match &err {
        DtrError::Config(_) | DtrError::Internal(_) => Status::internal(err.to_string()),
        DtrError::Storage { .. } => Status::unavailable(err.to_string()),
        DtrError::UnknownWorker(_) => Status::not_found(err.to_string()),
        DtrError::InvalidSession(_) => Status::unauthenticated(err.to_string()),
        DtrError::UnknownDataset(_)
        | DtrError::UnknownLease(_)
        | DtrError::UnknownCheckpoint(_) => Status::not_found(err.to_string()),
        DtrError::LeaseExpired(_) => Status::failed_precondition(err.to_string()),
        DtrError::CheckpointNotReady { .. } => Status::failed_precondition(err.to_string()),
        DtrError::CheckpointCorrupt { .. } => Status::data_loss(err.to_string()),
        DtrError::CheckpointAborted { .. } => Status::aborted(err.to_string()),
        DtrError::InvalidArgument(_) => Status::invalid_argument(err.to_string()),
    }
}

/// Convert a wall-clock duration-from-now into a protobuf Timestamp.
pub(crate) fn timestamp_after(ms_from_now: u64) -> prost_types::Timestamp {
    let t = std::time::SystemTime::now() + std::time::Duration::from_millis(ms_from_now);
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}
