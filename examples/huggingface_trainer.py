#!/usr/bin/env python3
"""Hugging Face Trainer integration sketch for the DTR runtime.

Shows the two integration points:
  1. a DTR-backed IterableDataset that leases shards instead of enumerating
     files locally, and
  2. a TrainerCallback that drives the atomic checkpoint barrier on
     Trainer's `on_save` events.

transformers is optional — without it this file still runs the dataset half
against a live runtime as a smoke test.
"""

from __future__ import annotations

import logging
import os

from dtr import CheckpointSpec, DatasetSpec, RuntimeClient

log = logging.getLogger("hf_trainer_example")


class DtrShardIterable:
    """Iterates samples from DTR-leased shards. Wrap in torch's IterableDataset
    (or datasets.IterableDataset) for real training."""

    def __init__(self, client: RuntimeClient, dataset, epoch: int, prefix: str):
        self.client = client
        self.dataset = dataset
        self.epoch = epoch
        self.prefix = prefix

    def __iter__(self):
        while True:
            lease = self.client.acquire_shard_lease(self.dataset, self.epoch)
            if lease.exhausted:
                return
            keys = [f"{self.prefix}/shard-{i:04d}.bin" for i in lease.shard_indices]
            for i, key in enumerate(keys):
                # Hint the runtime about upcoming shards so the cache is warm.
                data = self.client.read_object(key, prefetch_hint=keys[i + 1 :])
                yield {"bytes": data, "shard_key": key}
            self.client.release_shard_lease(lease, completed=True)


try:
    from transformers import TrainerCallback  # type: ignore

    class DtrCheckpointCallback(TrainerCallback):
        """Uploads this rank's serialized state through the DTR barrier on save."""

        def __init__(self, client: RuntimeClient, target, serialize_fn):
            self.client = client
            self.target = target
            self.serialize_fn = serialize_fn

        def on_save(self, args, state, control, **kwargs):
            ckpt = self.client.begin_checkpoint(self.target, int(state.global_step))
            self.client.upload_state(ckpt, self.serialize_fn())
            manifest_key = self.client.commit_checkpoint(ckpt)
            log.info(
                "HF step %s checkpoint committed -> %s", state.global_step, manifest_key
            )

except ImportError:
    DtrCheckpointCallback = None  # transformers not installed


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    endpoint = os.environ.get("DTR_ENDPOINT", "localhost:50051")
    dataset_uri = os.environ.get("DTR_DATASET_URI", "s3://dtr/datasets/toy")
    ckpt_uri = os.environ.get("DTR_CHECKPOINT_URI", "s3://dtr/checkpoints/hf-run")
    rank = int(os.environ.get("RANK", "0"))
    world = int(os.environ.get("WORLD_SIZE", "1"))

    with RuntimeClient(
        endpoint, worker_id=f"rank-{rank}", rank=rank, world_size=world
    ) as client:
        dataset = client.register_dataset(DatasetSpec(uri=dataset_uri, num_shards=8))
        target = client.register_checkpoint(CheckpointSpec(uri=ckpt_uri))
        prefix = dataset_uri.split("://", 1)[-1].split("/", 1)[-1]

        n = 0
        for _sample in DtrShardIterable(client, dataset, epoch=0, prefix=prefix):
            n += 1
        log.info("consumed %d shard objects; target %s registered", n, target.target_id)
        if DtrCheckpointCallback is None:
            log.info("transformers not installed — callback half skipped")


if __name__ == "__main__":
    main()
