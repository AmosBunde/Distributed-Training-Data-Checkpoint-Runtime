"""dtr — Python client SDK for the Distributed Training Data & Checkpoint Runtime."""

from dtr.checkpoint import CheckpointHandle, CheckpointSpec, CheckpointTarget, CommitStatus
from dtr.client import RuntimeClient
from dtr.dataset import DatasetHandle, DatasetSpec, ShardLease
from dtr.transport import TransportConfig

__all__ = [
    "CheckpointHandle",
    "CheckpointSpec",
    "CheckpointTarget",
    "CommitStatus",
    "DatasetHandle",
    "DatasetSpec",
    "RuntimeClient",
    "ShardLease",
    "TransportConfig",
]

__version__ = "0.1.0"
