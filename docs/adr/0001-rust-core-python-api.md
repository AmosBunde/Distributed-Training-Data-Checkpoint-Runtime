# ADR 0001 — Rust core runtime, Python user-facing API

**Status:** Accepted (2026-08)

## Context

The runtime sits on two hot paths with conflicting requirements:

- The **I/O and coordination path** (shard reads, checkpoint chunk handling, lease
  bookkeeping for hundreds–thousands of workers) needs predictable latency, real
  concurrency, and no GC pauses while multi-GB buffers move through memory.
- The **integration surface** must live where ML engineers live: inside PyTorch/HF/JAX
  training loops, which are Python.

A single-language solution fails one side or the other: pure Python cannot serve the
data plane at rate; pure Rust would force trainers through FFI or a CLI.

## Decision

Split by plane:

- **Rust** (`runtime/`): the `runtime-server` binary and five library crates
  (`common`, `storage`, `coordinator`, `io_engine`, `checkpoint`, `api`). Tokio async,
  `Bytes` for zero-copy buffer sharing, `thiserror` taxonomy at crate boundaries.
- **Python** (`python/`): the `dtr` package — `RuntimeClient`, spec dataclasses,
  transport config. Pure client; no business rules. Every rule (lease fencing, barrier
  quorum, commit verification) lives server-side so all client languages inherit it.

The wire contract between them is the proto set ([ADR 0002](0002-grpc-transport.md)),
never shared code.

## Consequences

- ✅ Data-plane throughput bounded by storage and network, not interpreter overhead.
- ✅ Training-loop integration is `pip install dtr` + ~10 lines.
- ✅ Protocol rules exist exactly once (server), tested once, inherited by any future
  client (Go, C++) for free.
- ⚠️ Two toolchains in CI (cargo + pip) — mitigated by the shared-gate design in
  `.github/workflows/ci.yml`.
- ⚠️ Cross-language drift risk — mitigated by integration tests that boot the real
  Rust server under pytest ([ADR 0009](0009-proto-stub-strategy.md)).

Related: [system architecture diagram](../diagrams/system-architecture.html).
