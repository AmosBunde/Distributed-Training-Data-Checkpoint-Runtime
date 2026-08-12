# ADR 0002 — gRPC (tonic / grpcio) as the transport

**Status:** Accepted (2026-08)

## Context

Workers and coordinator exchange three traffic shapes: high-frequency small control
RPCs (heartbeats, lease renewals), server-streamed bulk reads (shard frames), and
client-streamed bulk writes (checkpoint chunks). Candidates: REST/JSON over HTTP/1.1,
custom framing over raw HTTP/2, or gRPC.

## Context forces

- Thousands of workers ⇒ multiplexed long-lived connections, not per-request sockets.
- Bulk data ⇒ streaming with backpressure, not request/response buffering.
- Two implementation languages on day one ⇒ generated, versioned stubs.
- Kubernetes deployment ⇒ native health checking and load-balancer protocol hints.

## Decision

gRPC end to end:

- **Rust server:** `tonic` 0.12 + `prost`, protos compiled at build time
  ([ADR 0009](0009-proto-stub-strategy.md)); `tonic-health` provides
  `grpc.health.v1.Health`, which the Helm chart's readiness probe consumes natively.
- **Python client:** `grpcio` with a service-config retry policy (5 attempts,
  exponential backoff, `UNAVAILABLE` only) so coordinator restarts are invisible.
- **Packages are versioned** (`dtr.runtime.v1`, `dtr.checkpoint.v1`, `dtr.data.v1`);
  field numbers freeze at release; breaking changes mean a `v2` package side by side.
- **Error contract:** one `DtrError → tonic::Status` mapping (`to_status`) gives
  clients stable codes to key retry/fencing behavior on (`DATA_LOSS`,
  `FAILED_PRECONDITION`, `UNAUTHENTICATED`, `UNAVAILABLE`).

## Consequences

- ✅ One connection per worker carries heartbeats, leases, and streams concurrently.
- ✅ Kubernetes-native health/readiness; `appProtocol: grpc` on the Service.
- ✅ Client retry policy is declarative (service config), not hand-rolled loops.
- ⚠️ gRPC message-size ceilings require chunk streaming in frames (1 MiB) rather than
  single messages — the data plane API is shaped accordingly.
- ⚠️ Browser/debug tooling needs `grpcurl` instead of `curl` (documented in README).

Related: [checkpoint commit sequence](../diagrams/checkpoint-commit-sequence.html).
