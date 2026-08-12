//! Typed runtime configuration, loaded from YAML with environment overrides.
//!
//! The same schema backs `config/local.yaml`, `config/eks.yaml`, etc. Every
//! field has a sane local-dev default so a minimal file boots the server.

use crate::types::QuorumPolicy;
use crate::DtrError;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub runtime: RuntimeSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub coordinator: CoordinatorSection,
    #[serde(default)]
    pub checkpoint: CheckpointSection,
    #[serde(default)]
    pub io: IoSection,
    #[serde(default)]
    pub observability: ObservabilitySection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSection {
    /// gRPC bind address, e.g. "0.0.0.0:50051".
    pub grpc_bind: String,
    /// Log level filter: trace|debug|info|warn|error.
    pub log_level: String,
    /// Emit logs as JSON (true in production, false for human-readable dev logs).
    pub log_json: bool,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            grpc_bind: "0.0.0.0:50051".into(),
            log_level: "info".into(),
            log_json: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    Fs,
    S3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSection {
    pub backend: StorageBackendKind,
    #[serde(default)]
    pub fs: FsStorageConfig,
    #[serde(default)]
    pub s3: S3StorageConfig,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Fs,
            fs: FsStorageConfig::default(),
            s3: S3StorageConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsStorageConfig {
    /// Root directory all `file://` URIs resolve under.
    pub root_dir: String,
}

impl Default for FsStorageConfig {
    fn default() -> Self {
        Self {
            root_dir: "/mnt/dtr".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct S3StorageConfig {
    /// Custom endpoint for S3-compatible stores (MinIO, GCS/Azure gateways).
    /// None = real AWS S3 resolved from the region.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Credentials resolve from the environment / workload identity when None.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Path-style addressing (required by MinIO).
    #[serde(default)]
    pub force_path_style: bool,
    #[serde(default)]
    pub allow_http: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorSection {
    pub heartbeat_interval_ms: u64,
    pub missed_heartbeats_allowed: u32,
    pub lease_ttl_ms: u64,
    /// Default shards granted per lease when the client does not ask for a count.
    pub default_shards_per_lease: u32,
}

impl Default for CoordinatorSection {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5_000,
            missed_heartbeats_allowed: 3,
            lease_ttl_ms: 60_000,
            default_shards_per_lease: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSection {
    pub quorum: QuorumPolicy,
    /// Barrier deadline: abort the checkpoint if quorum is not met in time.
    pub barrier_timeout_ms: u64,
    /// Default chunk size when the client spec omits it.
    pub default_chunk_bytes: u64,
    /// tmp/ directories older than this are GC-eligible.
    pub gc_orphan_after_ms: u64,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        Self {
            quorum: QuorumPolicy::AllRanks,
            barrier_timeout_ms: 600_000,
            default_chunk_bytes: 64 * 1024 * 1024,
            gc_orphan_after_ms: 3_600_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IoSection {
    /// In-memory read cache budget in bytes.
    pub cache_bytes: u64,
    /// How many objects ahead the prefetcher warms.
    pub prefetch_depth: u32,
}

impl Default for IoSection {
    fn default() -> Self {
        Self {
            cache_bytes: 4 * 1024 * 1024 * 1024,
            prefetch_depth: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySection {
    /// HTTP bind address for /metrics (Prometheus). None disables the endpoint.
    pub metrics_bind: Option<String>,
}

impl Default for ObservabilitySection {
    fn default() -> Self {
        Self {
            metrics_bind: Some("0.0.0.0:9090".into()),
        }
    }
}

impl RuntimeConfig {
    /// Load from a YAML file, then apply environment overrides.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DtrError> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            DtrError::Config(format!("cannot read {}: {e}", path.as_ref().display()))
        })?;
        let mut cfg: RuntimeConfig = serde_yaml::from_str(&raw)
            .map_err(|e| DtrError::Config(format!("invalid YAML: {e}")))?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Environment variables override file values so images stay generic:
    /// RUNTIME_BIND_ADDR, STORAGE_BACKEND, S3_ENDPOINT, S3_ACCESS_KEY,
    /// S3_SECRET_KEY, S3_REGION, METRICS_BIND_ADDR, LOG_LEVEL.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("RUNTIME_BIND_ADDR") {
            self.runtime.grpc_bind = v;
        }
        if let Ok(v) = std::env::var("LOG_LEVEL") {
            self.runtime.log_level = v;
        }
        if let Ok(v) = std::env::var("STORAGE_BACKEND") {
            match v.as_str() {
                "fs" => self.storage.backend = StorageBackendKind::Fs,
                "s3" => self.storage.backend = StorageBackendKind::S3,
                _ => {}
            }
        }
        if let Ok(v) = std::env::var("S3_ENDPOINT") {
            self.storage.s3.allow_http = v.starts_with("http://");
            self.storage.s3.endpoint = Some(v);
            self.storage.s3.force_path_style = true;
        }
        if let Ok(v) = std::env::var("S3_ACCESS_KEY") {
            self.storage.s3.access_key_id = Some(v);
        }
        if let Ok(v) = std::env::var("S3_SECRET_KEY") {
            self.storage.s3.secret_access_key = Some(v);
        }
        if let Ok(v) = std::env::var("S3_REGION") {
            self.storage.s3.region = Some(v);
        }
        if let Ok(v) = std::env::var("METRICS_BIND_ADDR") {
            self.observability.metrics_bind = Some(v);
        }
    }

    fn validate(&self) -> Result<(), DtrError> {
        if self.coordinator.lease_ttl_ms < self.coordinator.heartbeat_interval_ms {
            return Err(DtrError::Config(format!(
                "lease_ttl_ms ({}) must be >= heartbeat_interval_ms ({})",
                self.coordinator.lease_ttl_ms, self.coordinator.heartbeat_interval_ms
            )));
        }
        if let QuorumPolicy::Fraction(f) = self.checkpoint.quorum {
            if !(f > 0.0 && f <= 1.0) {
                return Err(DtrError::Config(format!(
                    "checkpoint quorum fraction must be in (0, 1], got {f}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(yaml: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        f
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let f = write_tmp("{}");
        let cfg = RuntimeConfig::load(f.path()).unwrap();
        assert_eq!(cfg.runtime.grpc_bind, "0.0.0.0:50051");
        assert_eq!(cfg.storage.backend, StorageBackendKind::Fs);
        assert_eq!(cfg.checkpoint.default_chunk_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn full_config_round_trips() {
        let f = write_tmp(
            r#"
runtime:
  grpc_bind: "0.0.0.0:50052"
  log_level: "debug"
  log_json: false
storage:
  backend: s3
  s3:
    endpoint: "http://minio:9000"
    region: "us-east-1"
    force_path_style: true
    allow_http: true
coordinator:
  heartbeat_interval_ms: 2000
  missed_heartbeats_allowed: 5
  lease_ttl_ms: 30000
  default_shards_per_lease: 4
checkpoint:
  quorum:
    policy: fraction
    value: 0.9
  barrier_timeout_ms: 120000
  default_chunk_bytes: 8388608
  gc_orphan_after_ms: 60000
io:
  cache_bytes: 1073741824
  prefetch_depth: 8
observability:
  metrics_bind: "0.0.0.0:9091"
"#,
        );
        let cfg = RuntimeConfig::load(f.path()).unwrap();
        assert_eq!(cfg.storage.backend, StorageBackendKind::S3);
        assert_eq!(cfg.checkpoint.quorum, QuorumPolicy::Fraction(0.9));
        assert_eq!(cfg.io.prefetch_depth, 8);
    }

    #[test]
    fn unknown_fields_rejected() {
        let f = write_tmp("runtime:\n  grpc_bind: \"x\"\n  typo_field: 1\n");
        assert!(RuntimeConfig::load(f.path()).is_err());
    }

    #[test]
    fn lease_shorter_than_heartbeat_rejected() {
        let f = write_tmp(
            "coordinator:\n  heartbeat_interval_ms: 10000\n  missed_heartbeats_allowed: 3\n  lease_ttl_ms: 5000\n  default_shards_per_lease: 1\n",
        );
        let err = RuntimeConfig::load(f.path()).unwrap_err();
        assert!(matches!(err, DtrError::Config(_)));
    }

    #[test]
    fn invalid_quorum_fraction_rejected() {
        let f = write_tmp(
            "checkpoint:\n  quorum:\n    policy: fraction\n    value: 1.5\n  barrier_timeout_ms: 1\n  default_chunk_bytes: 1\n  gc_orphan_after_ms: 1\n",
        );
        assert!(RuntimeConfig::load(f.path()).is_err());
    }
}
