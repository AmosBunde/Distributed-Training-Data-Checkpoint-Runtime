# ADR 0006 — In-memory coordinator state for v1

**Status:** Accepted (2026-08)

## Context

Coordinator state (membership, lease table, barrier registry) could live in-memory,
in etcd, or in Postgres. Durable state buys coordinator-restart transparency at the
cost of an infrastructure dependency, consensus-write latency on the heartbeat/lease
hot path, and significant operational surface.

The README's design already treats the coordinator as pluggable
("persists metadata (etcd/Postgres optional, pluggable)").

## Decision

v1 keeps **all coordinator state in process memory** (std `Mutex`-guarded maps; time
via `tokio::time` for deterministic tests), and makes restart recovery **protocol
driven** instead of storage driven:

- Heartbeat from a worker the coordinator doesn't know returns
  `must_reregister=true`; the SDK re-registers automatically.
- Re-registration re-derives membership; datasets and checkpoint targets have
  **deterministic IDs** (FNV of the URI) so handles survive restarts.
- Committed checkpoints are already durable in storage — `GetLatestManifest` reads
  the manifest, not coordinator memory.
- In-flight leases and open barriers are lost on restart: leases lapse (workers
  acquire fresh ones), barriers re-open on the next `BeginCheckpoint`.

## Consequences

- ✅ Zero infrastructure dependencies; single-binary deploys; microsecond lease ops.
- ✅ Restart cost is bounded and automatic: one re-registration round + one aborted
  checkpoint attempt worst-case.
- ⚠️ A coordinator crash mid-barrier aborts that checkpoint (training continues to
  the next interval). Deployments needing barrier survival across coordinator
  restarts need the durable backend.
- 🔭 Follow-up: a `StateStore` trait with an etcd/Postgres implementation slots in
  behind `Coordinator` without touching the gRPC layer; `replicaCount > 1` in the
  Helm chart becomes true HA only after that lands.
