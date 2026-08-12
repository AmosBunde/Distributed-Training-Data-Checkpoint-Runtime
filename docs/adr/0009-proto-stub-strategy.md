# ADR 0009 — Vendored protoc for Rust; committed stubs for Python

**Status:** Accepted (2026-08)

## Context

Both sides of the wire are generated from `proto/*.proto`. The generation strategies
have different failure modes per ecosystem:

- Rust builds always run through cargo with a `build.rs` available; requiring a
  system `protoc` breaks fresh machines and slim CI images.
- Python consumers `pip install dtr`; requiring `grpcio-tools`/protoc at install time
  breaks the primary UX for no benefit.
- Checked-in generated code drifts silently unless something enforces regeneration.

## Decision

Asymmetric, per-ecosystem:

- **Rust: generate at build time, never check in.** `runtime/crates/api/build.rs`
  runs `tonic-build` with **`protoc-bin-vendored`** — `cargo build` works with zero
  system dependencies, and stubs can never drift because they don't persist.
- **Python: generate with `scripts/gen_protos.sh`, commit the stubs**
  (`python/src/dtr/_proto/`), with imports rewritten package-relative. `pip install`
  needs nothing but grpcio.
- **CI enforces the committed half:** the `protos` job regenerates and fails on any
  diff, with an actionable error pointing at the script.
- Cross-language compatibility is proven by integration tests that boot the real
  Rust server under pytest — the two independently-generated stub sets meet on a
  live socket in every CI run.

## Consequences

- ✅ `cargo build` and `pip install` both work on a bare machine.
- ✅ Stub drift is structurally impossible (Rust) or CI-fatal (Python).
- ⚠️ Python stub diffs appear in PRs that touch protos — by design; reviewers see the
  generated-surface change alongside the contract change.
- Related: [ADR 0002](0002-grpc-transport.md).
