# Distributed Training Data & Checkpoint Runtime

A high-performance distributed runtime that coordinates **data loading**, **checkpointing**, and **state persistence** across **hundreds to thousands of workers** for large-scale ML training jobs.

- **Core runtime:** Rust (I/O paths, concurrency, performance-critical code)
- **User-facing API & orchestration:** Python
- **Transport:** gRPC (recommended) or HTTP/2
- **Storage:** Distributed filesystem *or* object storage (S3-compatible)
- **Ops tooling:** Linux profiling tools + standard observability (Prometheus/OpenTelemetry)

---

## Contents

- [What this project does](#what-this-project-does)
- [Architecture overview](#architecture-overview)
- [Repository layout](#repository-layout)
- [Quick start (local)](#quick-start-local)
- [Local development setup](#local-development-setup)
- [Running the system locally](#running-the-system-locally)
- [Python API usage](#python-api-usage)
- [Checkpoint formats and consistency](#checkpoint-formats-and-consistency)
- [Cloud deployments](#cloud-deployments)
  - [Kubernetes (recommended)](#kubernetes-recommended)
  - [AWS EKS + S3](#aws-eks--s3)
  - [GCP GKE + GCS](#gcp-gke--gcs)
  - [Azure AKS + Blob](#azure-aks--blob)
- [Security and IAM](#security-and-iam)
- [Observability](#observability)
- [Performance & profiling](#performance--profiling)
- [Fault tolerance & recovery](#fault-tolerance--recovery)
- [CI/CD (GitHub)](#cicd-github)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## What this project does

This runtime provides a **simple Python API** to:

- register datasets and shard layouts
- coordinate distributed data access (shared data plane)
- write **fault-tolerant checkpoints** (atomic commit semantics)
- orchestrate worker membership, heartbeats, and recovery

A **Rust backend** handles:

- shared data access and high-throughput I/O
- async I/O and batching
- coordination and membership
- checkpoint writes and crash-safe commits
- recovery from worker/node failures under load

---

## Architecture overview

### High-level components

1. **Python Client SDK (`python/`)**
   - API used by training loops
   - dataset registration + shard assignment
   - checkpoint triggers and metadata publishing

2. **Runtime Coordinator (`runtime/`)**
   - gRPC control plane
   - worker membership + heartbeats
   - scheduling for shard leasing and checkpoint barriers
   - persists metadata (etcd/Postgres optional, pluggable)

3. **Data Plane / IO Engine (`runtime/`)**
   - high-throughput reads (local cache + prefetch)
   - supports:
     - local disk / NVMe cache
     - distributed FS (e.g., Lustre/NFS/CEPHFS)
     - object stores (S3 / MinIO / GCS / Azure Blob via S3 gateway)

4. **Checkpoint Service (`runtime/`)**
   - multi-part uploads / chunked writes
   - checksum + manifest
   - atomic commit: “write temp → verify → publish manifest”
   - garbage collection of orphan temp writes

5. **Observability (`observability/`)**
   - Prometheus metrics
   - OpenTelemetry traces (optional)
   - structured logging

### Data flow (training step)

- Worker requests **shard lease** → coordinator returns shard range and lease duration
- Worker reads shard via IO engine (local cache → remote store)
- At checkpoint barrier:
  - coordinator issues barrier token
  - workers upload state chunks + checksums
  - coordinator commits manifest once quorum/expected set is satisfied
  - failures → retry or reassign based on policy

---

## Repository layout

> This repo layout is a recommended starter structure. If you haven’t created files yet, use it as your blueprint.

```text
distributed-training-runtime/
├─ README.md
├─ LICENSE
├─ .gitignore
├─ .editorconfig
├─ .github/
│  └─ workflows/
│     ├─ ci.yml
│     └─ release.yml
├─ docker/
│  ├─ Dockerfile.runtime
│  ├─ Dockerfile.python
│  └─ compose.yml
├─ proto/
│  ├─ runtime.proto
│  └─ checkpoint.proto
├─ runtime/                          # Rust workspace
│  ├─ Cargo.toml
│  ├─ crates/
│  │  ├─ coordinator/                # membership, leases, barriers
│  │  ├─ io_engine/                  # async read/prefetch/cache
│  │  ├─ checkpoint/                 # chunking, manifests, GC
│  │  ├─ storage/                    # backends (fs, s3)
│  │  ├─ common/                     # config, errors, utils
│  │  └─ api/                        # gRPC server implementation
│  └─ bin/
│     └─ runtime-server/             # server entrypoint
├─ python/
│  ├─ pyproject.toml
│  ├─ src/
│  │  └─ dtr/                        # distributed training runtime client
│  │     ├─ __init__.py
│  │     ├─ client.py                # Python API
│  │     ├─ dataset.py               # dataset registration
│  │     ├─ checkpoint.py            # checkpoint helpers
│  │     └─ transport.py             # grpc/http2 selection
│  └─ tests/
├─ helm/
│  └─ dtr-runtime/                   # Kubernetes chart
│     ├─ Chart.yaml
│     ├─ values.yaml
│     └─ templates/
│        ├─ deployment.yaml
│        ├─ service.yaml
│        ├─ configmap.yaml
│        ├─ serviceaccount.yaml
│        └─ hpa.yaml
├─ config/
│  ├─ local.yaml
│  ├─ eks.yaml
│  ├─ gke.yaml
│  └─ aks.yaml
├─ scripts/
│  ├─ gen_protos.sh
│  ├─ local_up.sh
│  ├─ local_down.sh
│  ├─ load_test.py
│  └─ benchmark_io.sh
├─ docs/
│  ├─ architecture.md
│  ├─ checkpoints.md
│  ├─ storage_backends.md
│  ├─ k8s_deploy.md
│  └─ troubleshooting.md
└─ examples/
   ├─ pytorch_ddp.py
   ├─ huggingface_trainer.py
   └─ jax_pjit.py
```

---

## Quick start (local)

### Prereqs

- **Linux/macOS** (Linux recommended for profiling parity)
- **Rust** (stable toolchain) + `cargo`
- **Python 3.10+**
- **Docker** + **docker compose**
- Optional: `kubectl`, `helm`, `kind` (for local Kubernetes)

### One-command local demo (runtime + MinIO)

1) Start local services:

```bash
docker compose -f docker/compose.yml up -d
```

2) Install the Python client:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e python/
```

3) Run the example:

```bash
python examples/pytorch_ddp.py \
  --runtime-endpoint localhost:50051 \
  --dataset s3://datasets/imagenet-shards/ \
  --checkpoint s3://checkpoints/run-001/
```

4) View logs:

```bash
docker compose -f docker/compose.yml logs -f runtime
```

---

## Local development setup

### 1) Install Rust

Using rustup:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable
rustup component add clippy rustfmt
```

### 2) Install Python tooling

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e "python/[dev]"
```

Recommended dev extras:

- `pytest`, `ruff`, `mypy`
- `grpcio-tools` for Python stub generation

### 3) Generate protobufs

Rust and Python stubs should be generated from `proto/*.proto`.

```bash
bash scripts/gen_protos.sh
```

> Typical approach:
> - Rust: `tonic-build` or `prost-build`
> - Python: `grpcio-tools`

### 4) Build the runtime server (Rust)

```bash
cd runtime
cargo build --release
```

Run locally:

```bash
./target/release/runtime-server --config ../config/local.yaml
```

### 5) Run unit tests

Rust:

```bash
cd runtime
cargo test
```

Python:

```bash
pytest -q python/tests
```

---

## Running the system locally

### Option A: Docker Compose (recommended)

`docker/compose.yml` typically includes:

- runtime server
- MinIO (S3-compatible object store)
- optional Prometheus + Grafana

Common environment variables:

- `RUNTIME_BIND_ADDR=0.0.0.0:50051`
- `STORAGE_BACKEND=s3`
- `S3_ENDPOINT=http://minio:9000`
- `S3_ACCESS_KEY=minioadmin`
- `S3_SECRET_KEY=minioadmin`
- `S3_REGION=us-east-1`
- `CHECKPOINT_BUCKET=checkpoints`
- `DATASET_BUCKET=datasets`

Create buckets:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb -p local/datasets
mc mb -p local/checkpoints
```

Upload sample data:

```bash
mc cp --recursive ./data/sample_shards local/datasets/sample_shards
```

### Option B: Run everything on your machine (no Docker)

- Start runtime server (Rust)
- Use local FS backend:
  - datasets: `file:///mnt/datasets/...`
  - checkpoints: `file:///mnt/checkpoints/...`

Example config snippet:

```yaml
storage:
  backend: "fs"
  fs:
    root_dir: "/mnt/dtr"
runtime:
  grpc_bind: "0.0.0.0:50051"
  log_level: "info"
```

---

## Python API usage

### Minimal usage pattern

```python
from dtr import RuntimeClient, DatasetSpec, CheckpointSpec

client = RuntimeClient(endpoint="localhost:50051")

dataset = DatasetSpec(
    uri="s3://datasets/sample_shards/",
    shard_format="webdataset",          # example
    num_shards=1024,
    shuffle=True,
)

ckpt = CheckpointSpec(
    uri="s3://checkpoints/run-001/",
    write_mode="atomic",                # write temp → commit manifest
    chunk_bytes=64 * 1024 * 1024,       # 64MB chunks
)

client.register_dataset(dataset)
client.register_checkpoint(ckpt)

# During training
lease = client.acquire_shard_lease(worker_id="rank-0", epoch=0)
batch = client.read_next_batch(lease)

# On checkpoint
token = client.begin_checkpoint(step=1000)
client.upload_state(token, local_path="/tmp/state.bin")
client.commit_checkpoint(token)
```

### Recommended integration points

- **PyTorch DDP**: shard leasing aligns with rank/world-size
- **HF Trainer**: wrap dataloader + checkpoint callback
- **JAX**: barrier tokens map to `pmap`/`pjit` checkpoint points

See `examples/` for end-to-end integrations.

---

## Checkpoint formats and consistency

### Goals

- **Crash-safe**: partial uploads never appear as valid checkpoints
- **Recoverable**: manifest lists all chunks + checksums
- **Parallel**: workers upload in parallel without corrupting state

### Proposed on-disk / object-store layout

```text
checkpoints/run-001/
├─ tmp/
│  └─ ckpt-step-1000/
│     ├─ rank-0.part-0000
│     ├─ rank-1.part-0000
│     └─ ...
├─ manifests/
│  └─ step-1000.json
└─ latest -> manifests/step-1000.json   # optional pointer file/object
```

### Atomic commit pattern

1. Workers upload to `tmp/ckpt-step-<N>/...`
2. Coordinator verifies expected set + checksums
3. Coordinator writes `manifests/step-<N>.json`
4. (Optional) Updates `latest` pointer
5. Background GC cleans old tmp directories

This pattern works reliably on object storage where “rename” is not atomic.

---

## Cloud deployments

### Kubernetes (recommended)

This runtime fits best as a **Kubernetes service** with:

- one (or more) **runtime-coordinator** pods
- optional **read cache** sidecar or daemonset
- training jobs as separate workloads (PyTorchJob / Ray / plain pods)
- object store (S3/GCS/Blob) or distributed filesystem (EFS/Filestore/Azure Files/CEPHFS)

At a minimum you’ll deploy:

- `Deployment` for runtime server
- `Service` for gRPC endpoint
- `ConfigMap` for runtime config
- `ServiceAccount` + IAM binding (cloud specific)
- `HPA` (optional) for scaling coordinator

---

## AWS EKS + S3

### Overview

- **Compute:** EKS node groups (GPU or CPU)
- **Storage:** S3 for datasets/checkpoints
- **Identity:** IRSA (IAM Roles for Service Accounts) for secure S3 access
- **Networking:** NLB/ClusterIP depending on access pattern
- **Observability:** CloudWatch + Prometheus/Grafana optional

### 1) Create an EKS cluster (CLI)

You can use `eksctl` or Terraform. Example with `eksctl`:

```bash
eksctl create cluster \
  --name dtr-eks \
  --region us-east-1 \
  --nodegroup-name ng-default \
  --nodes 3 --nodes-min 3 --nodes-max 10 \
  --managed
```

### 2) Create S3 buckets

```bash
aws s3 mb s3://my-dtr-datasets
aws s3 mb s3://my-dtr-checkpoints
```

### 3) Configure IRSA for the runtime

Create IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:ListBucket","s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::my-dtr-datasets",
        "arn:aws:s3:::my-dtr-datasets/*",
        "arn:aws:s3:::my-dtr-checkpoints",
        "arn:aws:s3:::my-dtr-checkpoints/*"
      ]
    }
  ]
}
```

Attach it to a role and bind to the runtime service account. With `eksctl`:

```bash
eksctl create iamserviceaccount \
  --cluster dtr-eks \
  --name dtr-runtime \
  --namespace dtr \
  --attach-policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/dtr-s3 \
  --approve
```

### 4) Install with Helm

```bash
kubectl create namespace dtr

helm upgrade --install dtr-runtime helm/dtr-runtime \
  --namespace dtr \
  -f config/eks.yaml \
  --set storage.s3.bucketDatasets=my-dtr-datasets \
  --set storage.s3.bucketCheckpoints=my-dtr-checkpoints \
  --set serviceAccount.name=dtr-runtime
```

### 5) Run training jobs

Point your training job to the runtime service:

- Inside cluster: `dtr-runtime.dtr.svc.cluster.local:50051`
- Or via LoadBalancer/NLB if needed

Example env vars in your training pod:

```yaml
env:
  - name: DTR_ENDPOINT
    value: "dtr-runtime.dtr.svc.cluster.local:50051"
  - name: DTR_DATASET_URI
    value: "s3://my-dtr-datasets/imagenet/"
  - name: DTR_CHECKPOINT_URI
    value: "s3://my-dtr-checkpoints/run-001/"
```

---

## GCP GKE + GCS

### Notes

GCS is not S3, but you have common options:

- use a native GCS client in the Rust storage backend
- or route through an S3-compatible gateway (less ideal)

Recommended: implement **native GCS backend** in `runtime/crates/storage`.

### Workload Identity

Bind a Kubernetes service account to a Google service account with GCS permissions.

Steps (high level):

1. Create a Google service account with Storage Object Admin (scoped to bucket).
2. Enable Workload Identity on cluster/node pool.
3. Annotate KSA with GSA.
4. Use Helm values to set service account.

---

## Azure AKS + Blob

Preferred approach:

- Native Azure Blob backend in `storage` crate (recommended)
- Or S3-compat layer if your org already uses it

Identity options:

- Managed Identity for pods (Azure AD Workload Identity)
- SAS tokens (avoid if possible)

---

## Security and IAM

### Principles

- Don’t bake static cloud keys into images.
- Use workload identity (IRSA / Workload Identity / Managed Identity).
- Least privilege to dataset/checkpoint buckets and prefixes.
- Encrypt data at rest (SSE-S3/KMS) and in transit (TLS).

### TLS for gRPC

Options:

- Terminate TLS at an ingress (Envoy/NGINX) and use mTLS internally
- Or terminate in the runtime server directly

Recommended: **mTLS** between training pods and runtime inside the cluster.

---

## Observability

### Metrics

Expose `/metrics` for Prometheus:

- request latency (p50/p95/p99) for:
  - acquire lease
  - read batch
  - begin/commit checkpoint
- I/O throughput (MB/s) read/write
- cache hit ratio
- checkpoint success/failure counts
- retries, timeouts, backpressure signals

### Tracing

Use OpenTelemetry in Rust and Python:

- correlate a training step with:
  - shard lease
  - I/O reads
  - checkpoint barrier
  - manifest commit

### Logging

Structured logs (JSON) with fields:

- `worker_id`, `rank`, `epoch`, `step`
- `checkpoint_token`, `shard_id`
- `storage_backend`, `bucket`, `prefix`
- error codes and retry counts

---

## Performance & profiling

### Rust profiling

- CPU: `perf`, `flamegraph`, `pprof-rs`
- Memory: `heaptrack`, `jemalloc` profiling (optional)
- I/O: `iostat`, `blktrace`, `bpftrace`

Typical workflow:

```bash
# Run runtime-server with debug symbols
RUSTFLAGS="-g" cargo build

# Record perf
sudo perf record -F 99 -g -- ./target/debug/runtime-server --config config/local.yaml
sudo perf report
```

### Benchmarking

`scripts/benchmark_io.sh` should measure:

- sequential vs random reads
- prefetch depth
- chunk sizes
- concurrency level
- object store multipart thresholds

Keep a `benchmarks/` folder for captured results and configs.

---

## Fault tolerance & recovery

### Failure cases handled

- worker crash during checkpoint upload
- coordinator restart
- network partition (timeout + lease expiry)
- slow worker (straggler) at barrier
- partial object uploads

### Policies (configurable)

- **Lease expiry:** coordinator can reassign shards after timeout
- **Checkpoint quorum:** require all ranks or configurable quorum
- **Retry:** exponential backoff, bounded retries
- **Idempotency:** checkpoint upload uses content-addressed chunk ids

---

## CI/CD (GitHub)

### Recommended GitHub Actions workflow

- Rust:
  - `cargo fmt --check`
  - `cargo clippy -- -D warnings`
  - `cargo test`
- Python:
  - `ruff`, `mypy`, `pytest`
- Protobuf:
  - ensure generated code is up-to-date (fail if diff)
- Docker:
  - build runtime image
  - optional push on tag

Suggested release flow:

1. tag `vX.Y.Z`
2. build + push docker images
3. publish python package to internal index (or PyPI)
4. attach Helm chart version bump

---

## Troubleshooting

### gRPC connection errors

- Ensure Service exposes port 50051
- Check network policies
- Confirm TLS settings match client

### Slow reads

- Verify cache hit ratio
- Increase prefetch depth
- Check object store throttling
- Ensure worker nodes have enough bandwidth (ENA on AWS)

### Checkpoint stalls

- Inspect barrier logs for stragglers
- Increase checkpoint timeout
- Reduce chunk size if multipart overhead is high
- Confirm coordinator has stable backing store if enabled

---

## Contributing

1. Fork and create a feature branch
2. Add tests (Rust + Python where applicable)
3. Run formatting and lint:
   - `cargo fmt`
   - `cargo clippy`
   - `ruff format` / `ruff check`
4. Open a PR with:
   - problem statement
   - design notes
   - benchmarks if performance-sensitive

---

## License

Choose a license that matches your goals (Apache-2.0 is common for infrastructure projects).
