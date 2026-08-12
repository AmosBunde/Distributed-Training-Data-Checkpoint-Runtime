# ADR 0003 — `object_store` crate as the storage abstraction

**Status:** Accepted (2026-08)

## Context

The runtime must read datasets and write checkpoints against: local disk/NVMe (dev,
distributed-FS mounts like Lustre/NFS/CephFS), AWS S3, MinIO (local stack), GCS, and
Azure Blob. Options considered:

1. Per-cloud SDKs (`aws-sdk-s3`, `google-cloud-storage`, `azure_storage_blobs`) behind
   a hand-written trait — three heavy dependency trees, three semantics to reconcile.
2. The [`object_store`](https://crates.io/crates/object_store) crate — one trait over
   fs/S3/GCS/Azure/in-memory, maintained under Apache Arrow, proven under
   DataFusion/Delta Lake.

## Decision

Option 2. `dtr-storage` wraps `object_store` behind the runtime's own `Storage` trait
(get / get_range / put / list / delete / head) with three deliberate semantics:

- **`delete` of a missing key is not an error** — GC and commit-cleanup may race.
- **`head` returns `Option`, `get` errors** — probing for manifests is a normal state;
  a missing shard mid-epoch is data loss.
- All failures wrap into `DtrError::Storage { uri, source }` with a
  `backend-label/key` context string.

v1 enables the `aws` feature only: real S3, MinIO, GCS S3-interop, and Blob-via-gateway
all speak that dialect. **Native GCS/Azure backends are a tracked follow-up** — they
are feature flags plus a builder arm in `ObjectStorage::from_config`, not a redesign;
`config/gke.yaml` and `config/aks.yaml` point here.

## Consequences

- ✅ One dependency, identical semantics across five backends, in-memory store for
  zero-I/O unit tests across every crate.
- ✅ Backend switch is config (`storage.backend`), not code.
- ⚠️ One store = one bucket (the crate's model): the runtime convention is a single
  bucket with `datasets/` and `checkpoints/` prefixes; multi-bucket needs a keyed
  registry of stores (deferred until a deployment needs it).
- ⚠️ No per-part multipart control; checkpoint chunking happens at our layer
  ([ADR 0004](0004-manifest-based-atomic-commit.md)), each chunk one PUT.
