"""Checkpoint value types and helpers."""

from __future__ import annotations

import enum
import hashlib
from dataclasses import dataclass


@dataclass
class CheckpointSpec:
    """A checkpoint destination under one storage prefix."""

    uri: str
    write_mode: str = "atomic"
    chunk_bytes: int = 64 * 1024 * 1024
    keep_last: int = 0

    def __post_init__(self) -> None:
        if not self.uri:
            raise ValueError("uri must not be empty")
        if self.write_mode != "atomic":
            raise ValueError("v1 supports only write_mode='atomic'")
        if self.chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be > 0")


@dataclass(frozen=True)
class CheckpointTarget:
    """Registered checkpoint destination."""

    target_id: str
    spec: CheckpointSpec


@dataclass(frozen=True)
class CheckpointHandle:
    """An open checkpoint barrier for one training step."""

    token: str
    target: CheckpointTarget
    step: int
    upload_prefix: str
    expected_participants: int
    required_participants: int


class CommitStatus(enum.Enum):
    PENDING = "pending"
    COMMITTED = "committed"
    ABORTED = "aborted"


def split_chunks(data: bytes, chunk_bytes: int) -> list[bytes]:
    """Split a state blob into chunk_bytes-sized pieces (last may be shorter)."""
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be > 0")
    return [data[i : i + chunk_bytes] for i in range(0, len(data), chunk_bytes)]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
