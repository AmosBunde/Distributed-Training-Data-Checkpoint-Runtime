# Distributed Training Data & Checkpoint Runtime (DTR)

A distributed runtime that coordinates **data loading**, **crash-safe checkpointing**, and
**worker orchestration** for large-scale ML training jobs — hundreds to thousands of workers.

- **Core runtime:** Rust (tokio, tonic) — coordination, cached I/O, atomic checkpoint commits
- **User-facing API:** Python (`dtr` package) — ~10 lines to integrate a training loop
- **Transport:** gRPC (three versioned services)
- **Storage:** anything S3-compatible (AWS S3, MinIO, GCS interop, Blob gateway) or local/distributed FS
- **Ops:** Prometheus metrics, structured JSON logs, provisioned Grafana dashboard, Helm chart

![System architecture](docs/diagrams/system-architecture.svg)

> Interactive versions of every diagram (pan/zoom, dark/light, guided walkthroughs) live in
> [`docs/diagrams/`](docs/diagrams/README.md). Design decisions are recorded in
> [`docs/adr/`](docs/adr/README.md).

---

## Contents

- [What this does](#what-this-does)
- [Five-minute demo](#five-minute-demo)
- [How it works](#how-it-works)
  - [Shard leases](#shard-leases)
  - [The data plane](#the-data-plane)
  - [Atomic checkpoints](#atomic-checkpoints)
  - [Failure recovery](#failure-recovery)
- [Python API](#python-api)
- [Building from source](#building-from-source)
- [Running the test suites](#running-the-test-suites)
- [Configuration reference](#configuration-reference)
- [Deploying to Kubernetes](#deploying-to-kubernetes)
- [Observability](#observability)
- [Performance testing](#performance-testing)
- [Repository layout](#repository-layout)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing) · [License](#license)

---

## What this does

Training jobs waste GPUs on two problems this runtime owns:

1. **Data coordination** — which worker reads which dataset shards, with automatic
   reassignment when workers die. Workers acquire TTL-bound **leases** over contiguous
   shard ranges; a dead worker's shards flow back to the pool and the epoch completes
   without restarts. ([diagram](docs/diagrams/training-step-dataflow.svg), [ADR-0005](docs/adr/0005-ttl-lease-shard-scheduling.md))
2. **Crash-safe checkpoints** — parallel multi-rank uploads that are **atomic on object
   storage**: chunks land in a tmp area, are verified against writer-computed SHA-256,
   and become visible only when a manifest publishes. A partial upload can never be
   mistaken for a valid checkpoint. ([diagram](docs/diagrams/checkpoint-commit-sequence.svg), [ADR-0004](docs/adr/0004-manifest-based-atomic-commit.md))

Training workers need **no storage credentials**: reads and checkpoint uploads go through
the runtime's data plane (cached, prefetching), so bucket IAM is granted to exactly one
service account ([ADR-0008](docs/adr/0008-workload-identity.md)).

---

## Five-minute demo

Prereqs: **Docker** (with compose) and **Python 3.10+**. Nothing else — the stack builds
the Rust runtime inside Docker.

```bash
git clone https://github.com/AmosBunde/Distributed-Training-Data-Checkpoint-Runtime.git
cd Distributed-Training-Data-Checkpoint-Runtime

# 1. Bring up runtime + MinIO + Prometheus + Grafana, seed 16 toy shards
scripts/local_up.sh

# 2. Install the Python SDK
python3 -m venv .venv && source .venv/bin/activate
pip install -e python/

# 3. Run a simulated training job: leases shards, reads through the cached
#    data plane, commits atomic checkpoints every 8 steps
python examples/pytorch_ddp.py \
  --runtime-endpoint localhost:50051 \
  --dataset s3://dtr/datasets/toy --num-shards 16 \
  --checkpoint s3://dtr/checkpoints/run-001

# 4. Kill it mid-run (Ctrl-C) and run the same command again — it resumes
#    from the last *committed* manifest. That's the atomic-commit guarantee.

# 5. Look around
curl -s localhost:9090/metrics | grep dtr_     # runtime metrics
open http://localhost:3000                     # Grafana (admin/admin) → DTR dashboard
open http://localhost:9001                     # MinIO console (minioadmin/minioadmin)

# 6. Tear down (add --volumes to also delete stored data)
scripts/local_down.sh
```

All local ports bind to `127.0.0.1` only; the stack uses well-known dev credentials and is
**not** for real data (see the header of `docker/compose.yml`).

---

## How it works

### Shard leases

Each `(dataset, epoch)` owns a pool of shard indices. `AcquireShardLease` grants a
contiguous half-open range `[begin, end)` with a wall-clock TTL:

- **Renew** before expiry to keep reading. Renewal of a lapsed lease returns
  `renewed=false` — the *fencing signal*: those shards may already belong to another
  worker; stop reading and acquire a fresh lease.
- **Expiry** returns shards to the pool automatically — that is the whole fault-tolerance
  mechanism. No epoch restarts, no operator action.
- Contiguous ranges keep reads sequential, which is what the prefetcher optimizes for.

Config invariant (enforced at boot): `lease_ttl_ms ≥ heartbeat_interval_ms`, so a healthy
worker can't lose a lease between heartbeats.

Delivery is **at-least-once**: a worker that consumed shards but died before releasing
them causes re-reads. Jobs needing exactly-once must checkpoint consumption offsets with
model state ([ADR-0005](docs/adr/0005-ttl-lease-shard-scheduling.md)).

### The data plane

`DataService` serves workers that hold no storage credentials:

- `ListObjects(prefix)` — shard enumeration
- `ReadObject(key, offset, length, prefetch_hint)` — server-streamed 1 MiB frames through
  a **byte-budgeted LRU cache** with **single-flight dedup** (16 concurrent misses on one
  key = 1 backend GET) and background **prefetch** of hinted keys
- `UploadCheckpointChunk` — client-streamed chunk upload into the checkpoint tmp area;
  the response carries the server-computed SHA-256 for client cross-checking

Workers *with* storage IAM can bypass the data plane entirely; the control-plane
contracts work identically.

### Atomic checkpoints

The commit protocol ([sequence diagram](docs/diagrams/checkpoint-commit-sequence.svg)):

```text
<prefix>/tmp/ckpt-step-N/rank-r.part-0000   uploads in flight (invisible to readers)
<prefix>/manifests/step-N.json              ← THE commit point
<prefix>/latest.json                        best-effort pointer (fallback: scan manifests/)
```

1. `BeginCheckpoint(step)` opens (or idempotently joins) a barrier; all ranks get the same
   token, the tmp upload prefix, and the resolved quorum.
2. Ranks upload chunks and `ReportChunk` integrity metadata (key, size, SHA-256).
3. When the quorum is met, `CommitCheckpoint` **verifies every reported chunk in storage**
   (existence + size + content re-hash), then publishes the manifest — a single object
   PUT, which S3 guarantees read-after-write for. Verification failure auto-aborts the
   barrier with the reason.
4. Commit is client-polled (`PENDING / COMMITTED / ABORTED`); every rank converges on the
   same terminal state. Recovery reads `GetLatestManifest`.

**Invariant: a checkpoint exists iff its manifest exists, and a manifest is only written
after every chunk it lists verified in storage.**

Quorum is configurable per deployment: `all_ranks` (default — required for fully-sharded
state) or `fraction: 0.9` (sound only for redundantly-sharded state;
[ADR-0007](docs/adr/0007-quorum-barrier-policy.md)).

### Failure recovery

Every recovery arc is protocol-driven — no operator action
([lifecycle diagram](docs/diagrams/worker-lease-lifecycle.svg)):

| Failure | Detection | Recovery |
|---|---|---|
| Worker crash | missed heartbeats (deadline = interval × (missed+1)) | eviction → leases revoked → shards reflow to live workers |
| Worker restart | re-registration with same `worker_id` | session token rotates, fencing out any zombie process |
| Network partition | lease TTL lapses | shards reassigned; partitioned worker's renew gets `renewed=false` and it re-acquires |
| Straggler at barrier | barrier deadline | checkpoint aborts (costs one interval, never the job) — or commits without stragglers under fractional quorum |
| Crash mid-upload | commit verification | manifest never publishes; tmp data GC'd; restore uses the previous manifest |
| Coordinator restart | heartbeat gets `must_reregister` | SDK re-registers automatically; in-flight barriers abort, committed checkpoints unaffected ([ADR-0006](docs/adr/0006-in-memory-coordinator-state.md)) |

---

## Python API

```python
from dtr import RuntimeClient, DatasetSpec, CheckpointSpec

with RuntimeClient(
    "localhost:50051",
    worker_id="rank-0", rank=0, world_size=8,   # from RANK/WORLD_SIZE under torchrun
) as client:                                     # registers + starts heartbeat thread

    # -- datasets & leases ---------------------------------------------------
    dataset = client.register_dataset(DatasetSpec(
        uri="s3://dtr/datasets/imagenet-shards", num_shards=1024, shuffle=True,
    ))
    lease = client.acquire_shard_lease(dataset, epoch=0)     # ShardLease[begin, end)
    for shard in lease.shard_indices:
        data = client.read_object(f"datasets/imagenet-shards/shard-{shard:04d}.bin")
    client.release_shard_lease(lease, completed=True)

    # -- checkpoints ----------------------------------------------------------
    target = client.register_checkpoint(CheckpointSpec(
        uri="s3://dtr/checkpoints/run-001", chunk_bytes=64 * 1024 * 1024,
    ))
    ckpt = client.begin_checkpoint(target, step=1000)
    client.upload_state(ckpt, state_bytes)       # chunks, streams, verifies hashes
    manifest_key = client.commit_checkpoint(ckpt)  # polls until COMMITTED (or raises)

    # -- recovery -------------------------------------------------------------
    manifest = client.latest_manifest(target)    # None on a fresh run
```

What the client handles for you: background heartbeats at the server-dictated interval,
automatic re-registration after coordinator restarts, retries (exponential backoff on
`UNAVAILABLE` only), chunking + streaming + double-hash verification on upload, and
commit polling. TLS/mTLS via `TransportConfig` (`python/src/dtr/transport.py`).

Full framework integrations: [`examples/pytorch_ddp.py`](examples/pytorch_ddp.py)
(includes resume-from-manifest), [`examples/huggingface_trainer.py`](examples/huggingface_trainer.py)
(IterableDataset + TrainerCallback), [`examples/jax_pjit.py`](examples/jax_pjit.py).

---

## Building from source

Prereqs: **Rust stable** (via rustup) and **Python 3.10+**. No system protoc needed —
the build vendors it ([ADR-0009](docs/adr/0009-proto-stub-strategy.md)).

```bash
# Rust workspace: 6 crates + the server binary
cd runtime
cargo build                # debug; --release for deployment
cargo run --bin runtime-server -- --config ../config/local.yaml

# Python SDK with dev tools
pip install -e "python/[dev]"
```

Changed a `.proto`? Regenerate the committed Python stubs (Rust regenerates itself at
build time):

```bash
pip install grpcio-tools
bash scripts/gen_protos.sh     # CI fails if you forget to commit the result
```

## Running the test suites

```bash
# Rust: 50 unit tests (deterministic time via tokio::time::pause — no sleeps)
cd runtime && cargo test

# Quality gates (same commands CI runs)
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings

# Python: unit tests + integration tests that boot the real Rust server
cd .. && cargo build --manifest-path runtime/Cargo.toml   # integration tests need the binary
pytest -q python/tests
ruff check python/ && ruff format --check python/
```

The integration suite (`python/tests/test_integration.py`) starts `runtime-server` on a
random port with an fs backend and drives the full protocol over real gRPC — register →
lease → read → checkpoint → commit → recover — asserting byte-exact state reassembly.

---

## Configuration reference

The server boots from one YAML file (`--config path`, default `config/local.yaml`).
Unknown keys are **boot failures**, not silent ignores. Full schema with defaults:

```yaml
runtime:
  grpc_bind: "0.0.0.0:50051"
  log_level: "info"            # trace|debug|info|warn|error
  log_json: true               # false = human-readable dev logs

storage:
  backend: fs                  # fs | s3
  fs:
    root_dir: "/mnt/dtr"       # file:// URIs resolve under this root
  s3:
    endpoint: null             # null = real AWS S3; set for MinIO/gateways
    region: null
    access_key_id: null        # null = ambient identity (IRSA / workload identity)
    secret_access_key: null
    force_path_style: false    # required true for MinIO
    allow_http: false

coordinator:
  heartbeat_interval_ms: 5000
  missed_heartbeats_allowed: 3 # eviction after interval × (missed+1)
  lease_ttl_ms: 60000          # must be >= heartbeat_interval_ms (validated)
  default_shards_per_lease: 1

checkpoint:
  quorum:
    policy: all_ranks          # all_ranks | fraction
    # value: 0.9               # for fraction: (0, 1], ceil'd, min 1
  barrier_timeout_ms: 600000   # barrier aborts if quorum not met in time
  default_chunk_bytes: 67108864
  gc_orphan_after_ms: 3600000

io:
  cache_bytes: 4294967296      # in-memory LRU budget — keep pod memory above this
  prefetch_depth: 4

observability:
  metrics_bind: "0.0.0.0:9090" # null disables /metrics + /healthz
```

**Environment overrides** (win over the file; how the container image stays generic):

| Variable | Overrides |
|---|---|
| `RUNTIME_BIND_ADDR` | `runtime.grpc_bind` |
| `LOG_LEVEL` | `runtime.log_level` |
| `STORAGE_BACKEND` | `storage.backend` (`fs`/`s3`) |
| `S3_ENDPOINT` | `storage.s3.endpoint` (an `http://` value auto-enables path-style + allow_http) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` | corresponding `storage.s3.*` |
| `S3_BUCKET` | the bucket the store is scoped to (convention: one bucket, `datasets/` + `checkpoints/` prefixes) |
| `METRICS_BIND_ADDR` | `observability.metrics_bind` |

---

## Deploying to Kubernetes

The Helm chart deploys the runtime with gRPC-native probes, non-root security context,
and config-checksum-driven rollouts. Per-cloud values overlays encode the workload-identity
pattern — **no static keys anywhere** ([ADR-0008](docs/adr/0008-workload-identity.md)).

### AWS EKS + S3 (IRSA)

```bash
aws s3 mb s3://my-dtr-bucket

# IAM policy scoped to the bucket, bound to the runtime's ServiceAccount
eksctl create iamserviceaccount \
  --cluster my-cluster --namespace dtr --name dtr-runtime \
  --attach-policy-arn arn:aws:iam::<ACCOUNT>:policy/dtr-s3 --approve

kubectl create namespace dtr
helm upgrade --install dtr-runtime helm/dtr-runtime -n dtr -f config/eks.yaml \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<ACCOUNT>:role/dtr-s3 \
  --set env.S3_BUCKET=my-dtr-bucket
```

Training pods point at `dtr-runtime.dtr.svc.cluster.local:50051`:

```yaml
env:
  - name: DTR_ENDPOINT
    value: "dtr-runtime.dtr.svc.cluster.local:50051"
```

### GCP GKE (Workload Identity)

Uses the GCS S3-interop endpoint with HMAC keys in a secret; a native GCS backend is the
tracked follow-up in [ADR-0003](docs/adr/0003-object-store-abstraction.md).

```bash
kubectl -n dtr create secret generic dtr-gcs-hmac \
  --from-literal=S3_ACCESS_KEY=<hmac-id> --from-literal=S3_SECRET_KEY=<hmac-secret>
helm upgrade --install dtr-runtime helm/dtr-runtime -n dtr -f config/gke.yaml \
  --set-string serviceAccount.annotations."iam\.gke\.io/gcp-service-account"=dtr@<project>.iam.gserviceaccount.com \
  --set env.S3_BUCKET=my-dtr-bucket
```

### Azure AKS (AD Workload Identity)

Blob via an S3 gateway; SAS tokens are deliberately not supported in the profile:

```bash
helm upgrade --install dtr-runtime helm/dtr-runtime -n dtr -f config/aks.yaml \
  --set-string serviceAccount.annotations."azure\.workload\.identity/client-id"=<client-id> \
  --set config.storage.s3.endpoint=http://minio-gateway.dtr.svc:9000
```

### Sizing & HA notes

- Memory limit must exceed `io.cache_bytes` + ~1 GiB headroom (the cache is the dominant
  allocation; the values files encode this).
- `replicaCount > 1` gives fast failover, **not** shared state: coordinator state is
  in-memory in v1, and workers recover via the `must_reregister` protocol. True HA needs
  the durable-state follow-up in [ADR-0006](docs/adr/0006-in-memory-coordinator-state.md).
- TLS: terminate mTLS in a mesh/ingress, or in the runtime with the SDK's mTLS support.

---

## Observability

- **`GET :9090/metrics`** (Prometheus) — stable `dtr_*` names: RPC latency histograms +
  outcome counters by method, `dtr_workers_live`, `dtr_leases_active`,
  eviction/expiration/commit/abort counters, cache hit ratio and throughput series.
- **`GET :9090/healthz`** — liveness; readiness uses the native gRPC health service.
- **Logs** — structured JSON with `worker_id`/`rank`/`step`/`manifest` fields
  (`log_json: false` for dev).
- **Grafana** — the compose stack auto-provisions the DTR dashboard
  (`observability/grafana/dashboards/dtr-runtime.json`): p95 latency by method, RPC
  outcome rates, throughput, and a *failure signals* panel plotting evictions + lease
  expirations + checkpoint aborts together — the three curves that spike together when a
  node dies.

Debugging RPCs by hand:

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

---

## Performance testing

```bash
# Control-plane latency under concurrency (p50/p95/p99 per RPC family)
python scripts/load_test.py --endpoint localhost:50051 --workers 32 --shards 256 --checkpoint

# Data-plane throughput: sequential vs random × concurrency, cold vs warm cache
scripts/benchmark_io.sh localhost:50051 "1 4 16"    # results → benchmarks/results/
```

Profiling the Rust server: `perf record -F 99 -g -- target/release/runtime-server ...`,
`cargo flamegraph`, `heaptrack` — the release profile builds with thin LTO and single
codegen unit.

---

## Repository layout

```text
proto/                     gRPC contracts (runtime / checkpoint / data, all v1)
runtime/                   Rust workspace
├─ crates/common/          config (env overrides, validation), errors, domain types
├─ crates/storage/         Storage trait over object_store: fs + S3-compatible
├─ crates/coordinator/     membership, TTL leases, quorum barriers
├─ crates/io_engine/       LRU byte cache, prefetch, single-flight dedup
├─ crates/checkpoint/      chunking, SHA-256 manifests, atomic commit, GC
├─ crates/api/             tonic services, error→status mapping, /metrics
└─ bin/runtime-server/     the deployable binary
python/                    dtr SDK (client, specs, transport, committed stubs, tests)
docker/                    runtime + SDK images, local compose stack
helm/dtr-runtime/          Kubernetes chart
config/                    local server config + EKS/GKE/AKS Helm values overlays
observability/             Prometheus scrape config, Grafana provisioning + dashboard
scripts/                   gen_protos, local_up/down, load_test, benchmark_io
examples/                  PyTorch DDP, HF Trainer, JAX pjit integrations
docs/adr/                  Architecture Decision Records (9 accepted)
docs/diagrams/             interactive HTML + SVG architecture diagrams (+ specs)
.github/workflows/         CI quality gates + tag-triggered release pipeline
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Server exits at boot with `configuration error: unknown field` | typo'd YAML key (`deny_unknown_fields` is intentional) | check the key against the [reference](#configuration-reference) |
| `lease_ttl_ms (…) must be >= heartbeat_interval_ms` | invalid tunable combination | raise the TTL or lower the interval |
| SDK raises `UNAUTHENTICATED` | stale session token after worker restart | use the context manager / `connect()`; the client re-registers on `must_reregister` automatically |
| `renew_shard_lease` returns `False` | lease lapsed; shards reassigned (fencing) | stop reading that range, `acquire_shard_lease` again |
| `CommitCheckpoint` → `ABORTED` | barrier deadline passed, or chunk verification failed | check server logs for `checkpoint verification failed` (lists the exact chunk); watch `dtr_checkpoint_aborts_total` |
| Commit raises with `DATA_LOSS` | corrupted/missing chunk detected at verify | the checkpoint never published — restore path is unaffected; investigate the failing rank's upload |
| Slow reads / low `dtr_io_cache_hit_ratio` | cache budget below working set, or random access order | raise `io.cache_bytes`, raise `prefetch_depth`, keep leases contiguous |
| `connection refused` on 50051 | server not up / wrong bind | `curl :9090/healthz`, check `RUNTIME_BIND_ADDR`, k8s NetworkPolicies |
| Runtime can't reach MinIO in compose | bucket bootstrap failed | `docker compose -f docker/compose.yml logs minio-init` |
| CI fails `protos` job | edited a `.proto` without regenerating | `bash scripts/gen_protos.sh` and commit |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md): issue-linked branches, the exact quality-gate
commands CI enforces, stacked-PR conventions, and the ADR policy (decisions change by
superseding record, not by editing history).

## License

[Apache-2.0](LICENSE).
