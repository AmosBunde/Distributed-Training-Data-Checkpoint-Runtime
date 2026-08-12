# dtr — Python client SDK

Client SDK for the Distributed Training Data & Checkpoint Runtime. See the
repository root README for the full manual.

```python
from dtr import RuntimeClient, DatasetSpec, CheckpointSpec

with RuntimeClient(endpoint="localhost:50051", worker_id="rank-0", rank=0, world_size=1) as client:
    dataset = client.register_dataset(DatasetSpec(uri="s3://datasets/shards/", num_shards=128))
    lease = client.acquire_shard_lease(dataset, epoch=0)

    target = client.register_checkpoint(CheckpointSpec(uri="s3://checkpoints/run-001/"))
    ckpt = client.begin_checkpoint(target, step=1000)
    client.upload_state(ckpt, b"...state bytes...")
    client.commit_checkpoint(ckpt)
```
