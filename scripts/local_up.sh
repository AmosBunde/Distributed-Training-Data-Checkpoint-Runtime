#!/usr/bin/env bash
# Bring up the local DTR stack (runtime + MinIO + Prometheus + Grafana),
# wait for health, and seed toy dataset shards.
#
# Usage: scripts/local_up.sh [--no-seed] [--shards N]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEED=1
NUM_SHARDS=16

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-seed) SEED=0; shift ;;
    --shards) NUM_SHARDS="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

echo ">> starting compose stack"
docker compose -f "$REPO_ROOT/docker/compose.yml" up -d --build

echo ">> waiting for runtime health"
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1; then
    echo "   runtime healthy"
    break
  fi
  sleep 2
done
curl -fsS http://127.0.0.1:9090/healthz >/dev/null || { echo "runtime did not become healthy" >&2; exit 1; }

if [[ "$SEED" == "1" ]]; then
  echo ">> seeding $NUM_SHARDS toy shards into s3://dtr/datasets/toy/"
  TMP=$(mktemp -d)
  trap 'rm -rf "$TMP"' EXIT
  for i in $(seq 0 $((NUM_SHARDS - 1))); do
    idx=$(printf '%04d' "$i")
    head -c 65536 /dev/urandom > "$TMP/shard-$idx.bin"
  done
  docker run --rm --network dtr_default \
    -v "$TMP":/seed:ro --entrypoint /bin/sh minio/mc:latest -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc cp --recursive /seed/ local/dtr/datasets/toy/
    "
fi

cat <<EOF

Local stack is up:
  gRPC        localhost:50051
  metrics     http://localhost:9090/metrics
  MinIO       http://localhost:9001   (minioadmin / minioadmin)
  Prometheus  http://localhost:9091
  Grafana     http://localhost:3000   (admin / admin)

Try:  python examples/pytorch_ddp.py --runtime-endpoint localhost:50051 \\
        --dataset s3://dtr/datasets/toy --num-shards $NUM_SHARDS \\
        --checkpoint s3://dtr/checkpoints/run-001
EOF
