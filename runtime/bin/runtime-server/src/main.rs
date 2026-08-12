//! DTR runtime server entrypoint.
//!
//! Boots from a YAML config (`--config path`, env overrides applied), serves
//! the three gRPC services plus grpc-health, runs the background sweeper
//! (membership eviction -> lease revocation, lease expiry, orphan GC), and
//! shuts down gracefully on SIGINT/SIGTERM.

use anyhow::Context;
use dtr_api::pb::checkpoint::checkpoint_service_server::CheckpointServiceServer;
use dtr_api::pb::data::data_service_server::DataServiceServer;
use dtr_api::pb::runtime::runtime_service_server::RuntimeServiceServer;
use dtr_api::{AppState, CheckpointSvc, DataSvc, RuntimeSvc};
use dtr_common::RuntimeConfig;
use dtr_storage::ObjectStorage;
use std::sync::Arc;
use std::time::Duration;

fn parse_config_path() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                if let Some(path) = args.next() {
                    return path;
                }
            }
            "--help" | "-h" => {
                println!("runtime-server --config <path/to/config.yaml>");
                std::process::exit(0);
            }
            _ => {}
        }
    }
    "config/local.yaml".to_string()
}

fn init_tracing(cfg: &RuntimeConfig) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(cfg.runtime.log_level.clone()));
    if cfg.runtime.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path();
    let cfg = RuntimeConfig::load(&config_path)
        .with_context(|| format!("loading config from {config_path}"))?;
    init_tracing(&cfg);
    tracing::info!(config = %config_path, "starting runtime-server");

    let storage = Arc::new(ObjectStorage::from_config(&cfg).context("building storage backend")?);
    let state = Arc::new(AppState::new(cfg.clone(), storage));

    // Background sweeper: evict dead workers, cascade into lease revocation,
    // expire due leases. Interval = one heartbeat period.
    let sweeper_state = Arc::clone(&state);
    let sweep_every = Duration::from_millis(cfg.coordinator.heartbeat_interval_ms);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_every);
        loop {
            interval.tick().await;
            let evicted = sweeper_state.coordinator.membership.sweep_expired();
            for worker in &evicted {
                let revoked = sweeper_state
                    .coordinator
                    .leases
                    .revoke_worker(&worker.worker_id);
                tracing::warn!(
                    worker_id = %worker.worker_id,
                    rank = worker.rank,
                    revoked_leases = revoked.len(),
                    "evicted worker after missed heartbeats"
                );
            }
            let expired = sweeper_state.coordinator.leases.expire_due_leases();
            if !expired.is_empty() {
                tracing::info!(
                    count = expired.len(),
                    "expired shard leases returned to pool"
                );
            }
        }
    });

    let addr: std::net::SocketAddr = cfg
        .runtime
        .grpc_bind
        .parse()
        .with_context(|| format!("invalid grpc_bind address {}", cfg.runtime.grpc_bind))?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<RuntimeServiceServer<RuntimeSvc>>()
        .await;
    health_reporter
        .set_serving::<CheckpointServiceServer<CheckpointSvc>>()
        .await;
    health_reporter
        .set_serving::<DataServiceServer<DataSvc>>()
        .await;

    tracing::info!(addr = %addr, "gRPC server listening");
    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(RuntimeServiceServer::new(RuntimeSvc::new(Arc::clone(
            &state,
        ))))
        .add_service(CheckpointServiceServer::new(CheckpointSvc::new(
            Arc::clone(&state),
        )))
        .add_service(DataServiceServer::new(DataSvc::new(Arc::clone(&state))))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("gRPC server failed")?;

    tracing::info!("runtime-server shut down cleanly");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
            _ = sigterm.recv() => tracing::info!("received SIGTERM, shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
