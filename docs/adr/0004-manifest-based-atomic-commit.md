# ADR 0004 — Manifest-based atomic checkpoint commit

**Status:** Accepted (2026-08)

## Context

A checkpoint is written in parallel by every rank as many chunk objects. Object stores
have **no atomic rename and no multi-object transactions**; a crashed rank must never
leave a state that restores as a valid-looking but incomplete checkpoint. This is the
central correctness requirement of the whole system.

Rejected alternatives:
- *Copy-to-final-then-delete-tmp*: doubles write traffic for multi-TB checkpoints and
  is still non-atomic across objects.
- *Marker file after uploads*: a marker asserts nothing about which objects belong to
  the checkpoint or whether they are intact.

## Decision

Visibility is controlled by a **manifest object** — the same commit pattern Delta Lake
and Iceberg use for table transactions:

```
<prefix>/tmp/ckpt-step-N/rank-r.part-0000   uploads in flight (invisible)
<prefix>/manifests/step-N.json              THE commit point
<prefix>/latest.json                        best-effort convenience pointer
```

1. Ranks upload chunks to `tmp/` and report `(key, size, sha256, rank, part)` — the
   hash is computed by the **writer from its in-memory buffer**.
2. At quorum ([ADR 0007](0007-quorum-barrier-policy.md)), commit **verifies** every
   reported chunk in storage (existence + size always; full content re-hash by flag)
   and only then PUTs `manifests/step-N.json` — a single-object write, which S3
   guarantees read-after-write for.
3. `latest.json` moves best-effort; readers fall back to scanning `manifests/` for the
   max step, so pointer loss is harmless.
4. GC sweeps tmp areas for steps that are neither committed nor in an open barrier.

**Invariant: a checkpoint exists iff its manifest exists, and a manifest is only ever
written after every chunk it lists verified in storage.**

## Consequences

- ✅ Partial uploads are invisible by construction (test-pinned:
  `missing_chunk_never_publishes`).
- ✅ Same-length corruption is caught by the content re-hash
  (`corrupted_chunk_detected_by_checksum`).
- ⚠️ Committed chunks remain under `tmp/` (the manifest references them there); GC of
  committed steps assumes restore-before-GC-window. Promoting chunks to a `committed/`
  prefix at commit time is the recorded v2 hardening.
- ⚠️ Full content verification re-downloads every chunk — flag-gated per deployment
  (`verify_content`), defaulted on by the gRPC layer.

Related: [checkpoint commit sequence diagram](../diagrams/checkpoint-commit-sequence.html).
