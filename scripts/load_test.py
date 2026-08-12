#!/usr/bin/env python3
"""Load test: drive concurrent simulated workers against a running runtime.

Each simulated worker registers, leases shards across an epoch, optionally
reads objects through the data plane, and joins one shared checkpoint barrier.
Reports latency percentiles per RPC family.

Usage:
    python scripts/load_test.py --endpoint localhost:50051 \
        --workers 32 --shards 256 --checkpoint

Requires only the dtr SDK (pip install -e python/).
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time
from collections import defaultdict

from dtr import CheckpointSpec, DatasetSpec, RuntimeClient

LAT: dict[str, list[float]] = defaultdict(list)
LAT_LOCK = threading.Lock()


def timed(family: str, fn, *args, **kwargs):
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    dt = time.perf_counter() - t0
    with LAT_LOCK:
        LAT[family].append(dt)
    return out


def worker(idx: int, args, dataset_uri: str, barrier: threading.Barrier) -> None:
    with RuntimeClient(
        args.endpoint,
        worker_id=f"lt-{idx}",
        rank=idx,
        world_size=args.workers,
        heartbeat=True,
    ) as client:
        dataset = timed(
            "register_dataset",
            client.register_dataset,
            DatasetSpec(uri=dataset_uri, num_shards=args.shards),
        )
        consumed = 0
        while True:
            lease = timed(
                "acquire_lease",
                client.acquire_shard_lease,
                dataset,
                0,
                args.shards_per_lease,
            )
            if lease.exhausted:
                break
            consumed += len(lease)
            timed("renew_lease", client.renew_shard_lease, lease)
            client.release_shard_lease(lease, completed=True)

        if args.checkpoint:
            target = client.register_checkpoint(
                CheckpointSpec(uri="s3://dtr/checkpoints/loadtest", chunk_bytes=1 << 20)
            )
            barrier.wait(timeout=60)  # all workers hit the barrier together
            ckpt = client.begin_checkpoint(target, step=1)
            timed("upload_state", client.upload_state, ckpt, b"x" * args.state_bytes)
            timed("commit_poll", client.commit_checkpoint, ckpt, timeout_s=120)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="localhost:50051")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--shards", type=int, default=256)
    parser.add_argument("--shards-per-lease", type=int, default=4)
    parser.add_argument(
        "--checkpoint", action="store_true", help="also run a full-quorum checkpoint"
    )
    parser.add_argument("--state-bytes", type=int, default=1 << 20)
    args = parser.parse_args()

    dataset_uri = f"s3://dtr/datasets/loadtest-{int(time.time())}"
    barrier = threading.Barrier(args.workers)

    t0 = time.perf_counter()
    threads = [
        threading.Thread(
            target=worker, args=(i, args, dataset_uri, barrier), daemon=True
        )
        for i in range(args.workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=300)
    wall = time.perf_counter() - t0

    print(f"\n{args.workers} workers, {args.shards} shards, wall {wall:.2f}s")
    print(f"{'rpc family':<18}{'count':>8}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}")
    for family, samples in sorted(LAT.items()):
        qs = (
            statistics.quantiles(samples, n=100)
            if len(samples) >= 2
            else [samples[0]] * 99
        )
        print(
            f"{family:<18}{len(samples):>8}"
            f"{qs[49] * 1000:>10.2f}{qs[94] * 1000:>10.2f}{qs[98] * 1000:>10.2f}"
        )


if __name__ == "__main__":
    main()
