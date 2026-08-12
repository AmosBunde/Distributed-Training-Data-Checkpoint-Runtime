# ADR 0008 — Workload identity over static storage credentials

**Status:** Accepted (2026-08)

## Context

The runtime holds the job's most valuable data (datasets, model checkpoints). Static
cloud keys baked into images or config files leak through image layers, logs, and
repo history, and rotate poorly. Every target cloud now offers pod-level identity
federation.

## Decision

Two credential rules, enforced by packaging:

1. **The runtime's storage credentials come from workload identity** wherever it
   runs on a cloud: IRSA on EKS, Workload Identity on GKE, Azure AD Workload Identity
   on AKS. The Helm chart carries this as ServiceAccount annotations injected at
   install time; the storage crate's `AmazonS3Builder::from_env()` picks up the
   ambient identity chain with no code path for baked keys. Explicit
   `access_key_id/secret_access_key` config exists **only** for the local MinIO stack
   and interop cases (GCS HMAC), delivered via secret refs, never in values files.
2. **Training workers need no storage credentials at all**: the data plane
   (`DataService`) proxies reads and checkpoint chunk uploads through the runtime, so
   bucket IAM is granted to exactly one principal — the runtime's service account.
   SAS tokens on Azure are explicitly avoided (`config/aks.yaml`).

Transport security follows the same layering: in-cluster mTLS via mesh or ingress
termination; the SDK's `TransportConfig` supports server-TLS and mTLS natively for
deployments that terminate in the runtime.

## Consequences

- ✅ No long-lived secrets in images, values files, or git history.
- ✅ Bucket access auditable to one principal per cluster.
- ✅ Blast radius of a compromised training pod excludes storage.
- ⚠️ The proxy data plane concentrates bandwidth at the runtime — deployments with
  trusted workers can grant them read-only IAM and bypass `DataService` (the
  control-plane contract works identically either way).
