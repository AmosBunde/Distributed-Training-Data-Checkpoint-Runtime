#!/usr/bin/env python3
"""JAX pjit integration sketch for the DTR runtime.

Maps DTR concepts onto JAX multi-host training:
  - process_index / process_count -> worker rank / world size
  - shard leases -> per-host input pipelines
  - checkpoint barrier tokens -> the natural sync point after `jax.block_until_ready`

jax is optional; without it the script exercises lease + checkpoint flow with
simulated arrays as a smoke test against a live runtime.
"""

from __future__ import annotations

import logging
import os

from dtr import CheckpointSpec, DatasetSpec, RuntimeClient

try:
    import jax  # type: ignore

    RANK = jax.process_index()
    WORLD = jax.process_count()
except ImportError:
    jax = None
    RANK = int(os.environ.get("RANK", "0"))
    WORLD = int(os.environ.get("WORLD_SIZE", "1"))

log = logging.getLogger("jax_pjit_example")


def serialize_host_state(step: int) -> bytes:
    """This host's slice of the sharded train state. With jax: serialize the
    local shards of your pjit-partitioned state (e.g. via orbax or msgpack)."""
    if jax is not None:
        import numpy as np

        arr = np.full((256, 256), step, dtype=np.float32)
        return arr.tobytes()
    return f"host-{RANK}-state-step-{step}".encode() * 512


def main() -> None:
    logging.basicConfig(level=logging.INFO, format=f"[host {RANK}] %(message)s")
    endpoint = os.environ.get("DTR_ENDPOINT", "localhost:50051")

    with RuntimeClient(
        endpoint, worker_id=f"host-{RANK}", rank=RANK, world_size=WORLD
    ) as client:
        dataset = client.register_dataset(
            DatasetSpec(
                uri=os.environ.get("DTR_DATASET_URI", "s3://dtr/datasets/toy"),
                num_shards=8,
            )
        )
        target = client.register_checkpoint(
            CheckpointSpec(
                uri=os.environ.get("DTR_CHECKPOINT_URI", "s3://dtr/checkpoints/jax-run")
            )
        )

        step = 0
        lease = client.acquire_shard_lease(dataset, epoch=0)
        while not lease.exhausted:
            for _shard in lease.shard_indices:
                step += 1  # train_step(...) with pjit-sharded batch goes here
            client.release_shard_lease(lease, completed=True)
            lease = client.acquire_shard_lease(dataset, epoch=0)

        # Checkpoint at the pjit sync point: every host uploads its local
        # shards under the same barrier token; commit publishes one manifest.
        ckpt = client.begin_checkpoint(target, step)
        client.upload_state(ckpt, serialize_host_state(step))
        manifest_key = client.commit_checkpoint(ckpt)
        log.info("step %d committed -> %s", step, manifest_key)


if __name__ == "__main__":
    main()
