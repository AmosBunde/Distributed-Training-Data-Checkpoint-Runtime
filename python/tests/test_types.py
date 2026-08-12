"""Unit tests for SDK value types and helpers (no server needed)."""

import pytest

from dtr.checkpoint import CheckpointSpec, sha256_hex, split_chunks
from dtr.dataset import DatasetSpec, ShardLease


def test_dataset_spec_validation():
    with pytest.raises(ValueError):
        DatasetSpec(uri="", num_shards=4)
    with pytest.raises(ValueError):
        DatasetSpec(uri="s3://d/x", num_shards=0)
    spec = DatasetSpec(uri="s3://d/x", num_shards=8)
    assert spec.shuffle is True


def test_checkpoint_spec_validation():
    with pytest.raises(ValueError):
        CheckpointSpec(uri="s3://c/r", write_mode="overwrite")
    with pytest.raises(ValueError):
        CheckpointSpec(uri="s3://c/r", chunk_bytes=0)
    spec = CheckpointSpec(uri="s3://c/r")
    assert spec.chunk_bytes == 64 * 1024 * 1024


def test_shard_lease_helpers():
    lease = ShardLease(lease_id="l1", dataset_id="d", epoch=0, begin=4, end=8, ttl_ms=1000)
    assert len(lease) == 4
    assert list(lease.shard_indices) == [4, 5, 6, 7]
    assert not lease.exhausted

    empty = ShardLease(lease_id="", dataset_id="d", epoch=0, begin=0, end=0, ttl_ms=0)
    assert empty.exhausted
    assert len(empty) == 0


def test_split_chunks_round_trip():
    data = bytes(range(256)) * 40  # 10240 bytes
    chunks = split_chunks(data, 4096)
    assert [len(c) for c in chunks] == [4096, 4096, 2048]
    assert b"".join(chunks) == data
    with pytest.raises(ValueError):
        split_chunks(data, 0)


def test_sha256_hex_matches_known_vector():
    assert (
        sha256_hex(b"hello world")
        == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )
