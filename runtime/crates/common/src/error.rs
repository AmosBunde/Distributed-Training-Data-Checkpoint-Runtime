//! Error taxonomy shared across the runtime.
//!
//! Every crate maps its failures into `DtrError` at its public boundary so the
//! gRPC layer can translate them into stable status codes.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DtrError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("storage error at {uri}: {source}")]
    Storage {
        uri: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("unknown worker '{0}' (not registered or evicted)")]
    UnknownWorker(String),

    #[error("invalid session token for worker '{0}'")]
    InvalidSession(String),

    #[error("unknown dataset '{0}'")]
    UnknownDataset(String),

    #[error("unknown lease '{0}'")]
    UnknownLease(String),

    #[error("lease '{0}' expired and its shards were reassigned")]
    LeaseExpired(String),

    #[error("unknown checkpoint token '{0}'")]
    UnknownCheckpoint(String),

    #[error("checkpoint {token} cannot commit: {reason}")]
    CheckpointNotReady { token: String, reason: String },

    #[error("checkpoint {token} verification failed: {reason}")]
    CheckpointCorrupt { token: String, reason: String },

    #[error("checkpoint {token} aborted: {reason}")]
    CheckpointAborted { token: String, reason: String },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl DtrError {
    /// Storage helper that boxes any error with its URI context.
    pub fn storage(
        uri: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            uri: uri.into(),
            source: Box::new(source),
        }
    }
}

pub type Result<T> = std::result::Result<T, DtrError>;
