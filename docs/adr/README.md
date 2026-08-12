# Architecture Decision Records

Load-bearing design decisions for the Distributed Training Data & Checkpoint Runtime.
Each record is immutable once accepted; changing a decision means writing a superseding
ADR that links back, never editing history (see `CONTRIBUTING.md`).

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-rust-core-python-api.md) | Rust core runtime, Python user-facing API | Accepted |
| [0002](0002-grpc-transport.md) | gRPC (tonic/grpcio) as the transport | Accepted |
| [0003](0003-object-store-abstraction.md) | `object_store` crate as the storage abstraction | Accepted |
| [0004](0004-manifest-based-atomic-commit.md) | Manifest-based atomic checkpoint commit | Accepted |
| [0005](0005-ttl-lease-shard-scheduling.md) | TTL lease-based shard scheduling with fencing | Accepted |
| [0006](0006-in-memory-coordinator-state.md) | In-memory coordinator state for v1 | Accepted |
| [0007](0007-quorum-barrier-policy.md) | Configurable quorum policy for checkpoint barriers | Accepted |
| [0008](0008-workload-identity.md) | Workload identity over static storage credentials | Accepted |
| [0009](0009-proto-stub-strategy.md) | Vendored protoc for Rust; committed stubs for Python | Accepted |

## Format

Each ADR carries: **Status**, **Context** (the forces at play), **Decision** (what we
chose and its shape in the code), and **Consequences** (what we gained, what we pay,
and the tracked follow-ups).

Diagrams referenced by these records live in [`../diagrams/`](../diagrams/) as
explorable standalone HTML (dark/light, pan/zoom, guided views) with SVG exports.
