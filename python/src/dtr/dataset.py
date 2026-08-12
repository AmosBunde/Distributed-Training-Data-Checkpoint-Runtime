"""Dataset registration and shard-lease value types."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DatasetSpec:
    """A dataset of pre-sharded training data under one storage prefix."""

    uri: str
    num_shards: int
    shard_format: str = "webdataset"
    shuffle: bool = True
    shuffle_seed: int = 0

    def __post_init__(self) -> None:
        if self.num_shards <= 0:
            raise ValueError("num_shards must be > 0")
        if not self.uri:
            raise ValueError("uri must not be empty")


@dataclass(frozen=True)
class DatasetHandle:
    """Coordinator-assigned handle for a registered dataset."""

    dataset_id: str
    spec: DatasetSpec = field(compare=False, default=None)  # type: ignore[assignment]


@dataclass(frozen=True)
class ShardLease:
    """A TTL-bound grant of the half-open shard range [begin, end)."""

    lease_id: str
    dataset_id: str
    epoch: int
    begin: int
    end: int
    ttl_ms: int

    @property
    def shard_indices(self) -> range:
        return range(self.begin, self.end)

    def __len__(self) -> int:
        return self.end - self.begin

    @property
    def exhausted(self) -> bool:
        """True when the epoch pool had nothing left to grant."""
        return not self.lease_id
