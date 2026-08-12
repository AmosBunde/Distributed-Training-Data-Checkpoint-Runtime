//! Prometheus metrics for the runtime.
//!
//! One registry, created at boot and shared by the gRPC services and the
//! /metrics HTTP endpoint. Metric names are stable API — dashboards and
//! alerts depend on them; change them only with a deprecation cycle.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,

    /// RPC latency by method, seconds.
    pub rpc_latency: HistogramVec,
    /// RPC outcomes by method and grpc code.
    pub rpc_total: IntCounterVec,

    pub workers_live: IntGauge,
    pub leases_active: IntGauge,
    pub lease_expirations_total: IntCounter,
    pub worker_evictions_total: IntCounter,

    pub checkpoint_commits_total: IntCounter,
    pub checkpoint_aborts_total: IntCounter,
    pub checkpoint_bytes_total: IntCounter,

    pub io_cache_hits_total: IntGauge,
    pub io_cache_misses_total: IntGauge,
    pub io_backend_read_bytes_total: IntGauge,
    pub io_served_bytes_total: IntGauge,
    pub io_cache_hit_ratio: prometheus::Gauge,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let rpc_latency = HistogramVec::new(
            HistogramOpts::new("dtr_rpc_latency_seconds", "gRPC request latency").buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["method"],
        )
        .unwrap();
        let rpc_total = IntCounterVec::new(
            Opts::new("dtr_rpc_total", "gRPC requests by method and status code"),
            &["method", "code"],
        )
        .unwrap();

        let workers_live = IntGauge::new("dtr_workers_live", "Registered live workers").unwrap();
        let leases_active = IntGauge::new("dtr_leases_active", "Active shard leases").unwrap();
        let lease_expirations_total = IntCounter::new(
            "dtr_lease_expirations_total",
            "Shard leases expired and returned to pool",
        )
        .unwrap();
        let worker_evictions_total = IntCounter::new(
            "dtr_worker_evictions_total",
            "Workers evicted after missed heartbeats",
        )
        .unwrap();

        let checkpoint_commits_total =
            IntCounter::new("dtr_checkpoint_commits_total", "Committed checkpoints").unwrap();
        let checkpoint_aborts_total =
            IntCounter::new("dtr_checkpoint_aborts_total", "Aborted checkpoints").unwrap();
        let checkpoint_bytes_total = IntCounter::new(
            "dtr_checkpoint_bytes_total",
            "Bytes of committed checkpoint chunks",
        )
        .unwrap();

        let io_cache_hits_total =
            IntGauge::new("dtr_io_cache_hits_total", "IO cache hits").unwrap();
        let io_cache_misses_total =
            IntGauge::new("dtr_io_cache_misses_total", "IO cache misses").unwrap();
        let io_backend_read_bytes_total = IntGauge::new(
            "dtr_io_backend_read_bytes_total",
            "Bytes read from the storage backend",
        )
        .unwrap();
        let io_served_bytes_total =
            IntGauge::new("dtr_io_served_bytes_total", "Bytes served to readers").unwrap();
        let io_cache_hit_ratio =
            prometheus::Gauge::new("dtr_io_cache_hit_ratio", "IO cache hit ratio (0..1)").unwrap();

        for c in [
            registry.register(Box::new(rpc_latency.clone())),
            registry.register(Box::new(rpc_total.clone())),
            registry.register(Box::new(workers_live.clone())),
            registry.register(Box::new(leases_active.clone())),
            registry.register(Box::new(lease_expirations_total.clone())),
            registry.register(Box::new(worker_evictions_total.clone())),
            registry.register(Box::new(checkpoint_commits_total.clone())),
            registry.register(Box::new(checkpoint_aborts_total.clone())),
            registry.register(Box::new(checkpoint_bytes_total.clone())),
            registry.register(Box::new(io_cache_hits_total.clone())),
            registry.register(Box::new(io_cache_misses_total.clone())),
            registry.register(Box::new(io_backend_read_bytes_total.clone())),
            registry.register(Box::new(io_served_bytes_total.clone())),
            registry.register(Box::new(io_cache_hit_ratio.clone())),
        ] {
            c.expect("metric registration");
        }

        Arc::new(Self {
            registry,
            rpc_latency,
            rpc_total,
            workers_live,
            leases_active,
            lease_expirations_total,
            worker_evictions_total,
            checkpoint_commits_total,
            checkpoint_aborts_total,
            checkpoint_bytes_total,
            io_cache_hits_total,
            io_cache_misses_total,
            io_backend_read_bytes_total,
            io_served_bytes_total,
            io_cache_hit_ratio,
        })
    }

    /// Refresh gauges sampled from runtime state (called by the exporter and
    /// the sweeper).
    pub fn sample_state(&self, state: &crate::AppState) {
        self.workers_live
            .set(state.coordinator.membership.live_count() as i64);
        self.leases_active
            .set(state.coordinator.leases.active_lease_count() as i64);
        let io = state.io.stats();
        self.io_cache_hits_total.set(io.cache_hits as i64);
        self.io_cache_misses_total.set(io.cache_misses as i64);
        self.io_backend_read_bytes_total
            .set(io.bytes_read_backend as i64);
        self.io_served_bytes_total.set(io.bytes_served as i64);
        self.io_cache_hit_ratio.set(state.io.cache_hit_ratio());
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("encode metrics");
        String::from_utf8(buf).expect("metrics are utf-8")
    }

    /// Observe one RPC: latency + outcome counter.
    pub fn observe_rpc(&self, method: &str, code: &str, seconds: f64) {
        self.rpc_latency
            .with_label_values(&[method])
            .observe(seconds);
        self.rpc_total.with_label_values(&[method, code]).inc();
    }
}

/// Serve `GET /metrics` on `bind` until the process exits.
pub async fn serve_metrics(
    bind: String,
    metrics: Arc<Metrics>,
    state: Arc<crate::AppState>,
) -> Result<(), std::io::Error> {
    use axum::{extract::State, routing::get, Router};

    async fn metrics_handler(
        State((metrics, state)): State<(Arc<Metrics>, Arc<crate::AppState>)>,
    ) -> String {
        metrics.sample_state(&state);
        metrics.render()
    }

    async fn health_handler() -> &'static str {
        "ok"
    }

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .with_state((metrics, state));

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(addr = %bind, "metrics endpoint listening");
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_and_render() {
        let m = Metrics::new();
        m.observe_rpc("AcquireShardLease", "ok", 0.012);
        m.observe_rpc("AcquireShardLease", "ok", 0.020);
        m.observe_rpc("CommitCheckpoint", "data_loss", 1.2);
        m.checkpoint_commits_total.inc();
        m.workers_live.set(8);

        let text = m.render();
        assert!(text.contains("dtr_rpc_latency_seconds_bucket"));
        assert!(text.contains("dtr_rpc_total{code=\"ok\",method=\"AcquireShardLease\"} 2"));
        assert!(text.contains("dtr_rpc_total{code=\"data_loss\",method=\"CommitCheckpoint\"} 1"));
        assert!(text.contains("dtr_checkpoint_commits_total 1"));
        assert!(text.contains("dtr_workers_live 8"));
    }

    #[test]
    fn duplicate_registry_creation_is_independent() {
        // Two instances must not collide (no global default registry usage).
        let a = Metrics::new();
        let b = Metrics::new();
        a.checkpoint_commits_total.inc();
        assert!(b.render().contains("dtr_checkpoint_commits_total 0"));
    }
}
