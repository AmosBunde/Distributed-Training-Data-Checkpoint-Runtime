#!/usr/bin/env bash
# IO benchmark against a running DTR runtime's data plane.
#
# Reads the seeded toy shards (scripts/local_up.sh) through the runtime's
# cached data plane in sequential vs random order across a concurrency
# matrix, and reports MB/s. Two passes per cell show cold (backend) vs warm
# (cache) throughput. Results land in benchmarks/results/<timestamp>.txt.
#
# Usage: scripts/benchmark_io.sh [endpoint] [concurrency_list]
#   e.g. scripts/benchmark_io.sh localhost:50051 "1 4 16"
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENDPOINT="${1:-localhost:50051}"
CONCURRENCY="${2:-1 8}"
OUT_DIR="$REPO_ROOT/benchmarks/results"
mkdir -p "$OUT_DIR"
OUT="$OUT_DIR/io-$(date +%Y%m%d-%H%M%S).txt"

python3 - "$ENDPOINT" "$CONCURRENCY" <<'PY' | tee "$OUT"
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from dtr import RuntimeClient

endpoint, concurrency = sys.argv[1], sys.argv[2].split()

with RuntimeClient(endpoint, worker_id="bench-0", rank=0, world_size=1) as client:
    print(f"endpoint: {endpoint}")
    keys = [k for k, sz in client.list_objects("datasets/toy") if sz > 0]
    if not keys:
        sys.exit("no seeded objects under datasets/toy — run scripts/local_up.sh first")

    for conc in map(int, concurrency):
        for order in ("sequential", "random"):
            for temp in ("cold-ish", "warm"):
                ordered = list(keys)
                if order == "random":
                    random.shuffle(ordered)
                t0 = time.perf_counter()
                with ThreadPoolExecutor(max_workers=conc) as pool:
                    sizes = list(pool.map(lambda k: len(client.read_object(k)), ordered))
                dt = time.perf_counter() - t0
                mb = sum(sizes) / 1e6
                print(
                    f"objs={len(ordered):3d} order={order:10s} conc={conc:3d} "
                    f"pass={temp:8s} {mb:8.1f} MB in {dt:6.2f}s -> {mb / dt:8.1f} MB/s"
                )
PY

echo "results written to $OUT"
