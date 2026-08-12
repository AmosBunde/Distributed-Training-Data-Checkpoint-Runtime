#!/usr/bin/env python3
"""PyTorch DDP integration example for the DTR runtime.

Each rank leases contiguous shard ranges from the coordinator, streams shard
bytes through the runtime's cached data plane, and checkpoints through the
atomic-commit barrier. Works without torch installed (falls back to a numpy-
free simulated training loop) so it can run against the compose stack as a
smoke test.

Usage (single process, against the local stack):

    python examples/pytorch_ddp.py \
        --runtime-endpoint localhost:50051 \
        --dataset s3://dtr/datasets/toy --num-shards 16 \
        --checkpoint s3://dtr/checkpoints/run-001 \
        --epochs 2 --checkpoint-every 8

Under torchrun, RANK/WORLD_SIZE env vars are picked up automatically:

    torchrun --nproc_per_node=4 examples/pytorch_ddp.py --runtime-endpoint ...
"""

from __future__ import annotations

import argparse
import logging
import os
import time

import grpc
from dtr import CheckpointSpec, DatasetSpec, RuntimeClient

try:  # torch is optional for this example
    import torch  # type: ignore

    HAVE_TORCH = True
except ImportError:
    HAVE_TORCH = False

log = logging.getLogger("pytorch_ddp_example")


def make_state(step: int, rank: int) -> bytes:
    """This rank's checkpoint payload. With torch: a real state_dict."""
    if HAVE_TORCH:
        import io

        model = torch.nn.Linear(64, 8)
        buf = io.BytesIO()
        torch.save({"step": step, "rank": rank, "model": model.state_dict()}, buf)
        return buf.getvalue()
    return f"step={step};rank={rank};".encode() * 1024


def train_step(batch: bytes) -> float:
    """Stand-in for forward/backward; returns a fake loss."""
    time.sleep(0.01)
    return 1.0 / (1 + len(batch) % 97)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-endpoint", default=os.environ.get("DTR_ENDPOINT", "localhost:50051")
    )
    parser.add_argument(
        "--dataset", default=os.environ.get("DTR_DATASET_URI", "s3://dtr/datasets/toy")
    )
    parser.add_argument(
        "--checkpoint",
        default=os.environ.get("DTR_CHECKPOINT_URI", "s3://dtr/checkpoints/run-001"),
    )
    parser.add_argument("--num-shards", type=int, default=16)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--shards-per-lease", type=int, default=2)
    parser.add_argument(
        "--checkpoint-every", type=int, default=8, help="steps between checkpoints"
    )
    args = parser.parse_args()

    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    logging.basicConfig(level=logging.INFO, format=f"[rank {rank}] %(message)s")

    with RuntimeClient(
        args.runtime_endpoint,
        worker_id=f"rank-{rank}",
        rank=rank,
        world_size=world_size,
        labels={"example": "pytorch_ddp", "host": os.uname().nodename},
    ) as client:
        dataset = client.register_dataset(
            DatasetSpec(
                uri=args.dataset, num_shards=args.num_shards, shard_format="webdataset"
            )
        )
        target = client.register_checkpoint(CheckpointSpec(uri=args.checkpoint))

        # Resume from the newest committed checkpoint, if any.
        manifest = client.latest_manifest(target)
        start_step = int(manifest["step"]) + 1 if manifest else 0
        if manifest:
            log.info("resuming after committed step %s", manifest["step"])

        step = start_step
        for epoch in range(args.epochs):
            while True:
                lease = client.acquire_shard_lease(
                    dataset, epoch, args.shards_per_lease
                )
                if lease.exhausted:
                    break  # epoch pool drained
                log.info(
                    "epoch %d: leased shards [%d, %d)", epoch, lease.begin, lease.end
                )

                shard_prefix = args.dataset.split("://", 1)[-1].split("/", 1)[-1]
                for shard in lease.shard_indices:
                    # Convention: shard files named shard-<idx>.bin under the prefix.
                    key = f"{shard_prefix}/shard-{shard:04d}.bin"
                    try:
                        batch = client.read_object(key)
                    except grpc.RpcError:
                        batch = b"synthetic" * 128  # tolerate missing toy data
                    loss = train_step(batch)
                    client.current_step = step
                    step += 1

                    if step % args.checkpoint_every == 0:
                        ckpt = client.begin_checkpoint(target, step)
                        client.upload_state(ckpt, make_state(step, rank))
                        manifest_key = client.commit_checkpoint(ckpt)
                        log.info(
                            "step %d: checkpoint committed -> %s (loss %.4f)",
                            step,
                            manifest_key,
                            loss,
                        )

                client.release_shard_lease(lease, completed=True)

        log.info("done at step %d", step)


if __name__ == "__main__":
    main()
