"""End-to-end integration test against a real runtime-server.

Skipped automatically when the server binary is absent. Build it with:

    cd runtime && cargo build

The test boots the server on a random port with a filesystem backend in a
temp dir, then drives the full protocol: register -> dataset -> lease ->
renew -> data-plane read -> checkpoint begin/upload/commit -> recovery.
"""

from __future__ import annotations

import pathlib
import socket
import subprocess
import time

import pytest

from dtr import CheckpointSpec, DatasetSpec, RuntimeClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER_CANDIDATES = [
    REPO_ROOT / "runtime" / "target" / "debug" / "runtime-server",
    REPO_ROOT / "runtime" / "target" / "release" / "runtime-server",
]


def _server_binary() -> pathlib.Path | None:
    for p in SERVER_CANDIDATES:
        if p.exists():
            return p
    return None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server(tmp_path_factory: pytest.TempPathFactory):
    binary = _server_binary()
    if binary is None:
        pytest.skip("runtime-server binary not built (run: cd runtime && cargo build)")

    root = tmp_path_factory.mktemp("dtr")
    port = _free_port()
    config = root / "config.yaml"
    config.write_text(
        f"""
runtime:
  grpc_bind: "127.0.0.1:{port}"
  log_level: "warn"
  log_json: false
storage:
  backend: fs
  fs:
    root_dir: "{root / "data"}"
coordinator:
  heartbeat_interval_ms: 1000
  missed_heartbeats_allowed: 3
  lease_ttl_ms: 10000
  default_shards_per_lease: 2
"""
    )
    proc = subprocess.Popen([str(binary), "--config", str(config)])
    endpoint = f"127.0.0.1:{port}"
    # Wait for the port to accept connections.
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("runtime-server did not start")

    yield endpoint, root
    proc.terminate()
    proc.wait(timeout=10)


def test_full_protocol(server):
    endpoint, root = server
    # Seed a "shard" object directly in the fs backend.
    data_dir = root / "data" / "datasets" / "toy"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "shard-0000.bin").write_bytes(b"shard-zero-bytes")

    with RuntimeClient(endpoint, worker_id="rank-0", rank=0, world_size=1) as client:
        # Datasets + leases.
        dataset = client.register_dataset(DatasetSpec(uri="s3://bucket/datasets/toy", num_shards=4))
        lease = client.acquire_shard_lease(dataset, epoch=0)
        assert not lease.exhausted
        assert (lease.begin, lease.end) == (0, 2)  # default_shards_per_lease: 2
        assert client.renew_shard_lease(lease) is True

        second = client.acquire_shard_lease(dataset, epoch=0, requested_shards=2)
        assert (second.begin, second.end) == (2, 4)
        third = client.acquire_shard_lease(dataset, epoch=0)
        assert third.exhausted  # pool drained

        # Data plane read of the seeded object.
        listed = client.list_objects("datasets/toy")
        assert listed == [("datasets/toy/shard-0000.bin", 16)]
        assert client.read_object("datasets/toy/shard-0000.bin") == b"shard-zero-bytes"

        # Checkpoint round trip.
        target = client.register_checkpoint(
            CheckpointSpec(uri="s3://bucket/checkpoints/run-t", chunk_bytes=8)
        )
        ckpt = client.begin_checkpoint(target, step=100)
        assert ckpt.expected_participants == 1
        state = b"0123456789abcdef-final-state"  # 28 bytes -> 4 chunks of <=8
        client.upload_state(ckpt, state)
        manifest_key = client.commit_checkpoint(ckpt, timeout_s=30)
        assert manifest_key.endswith("manifests/step-100.json")

        # Recovery: latest manifest reflects the commit and reassembles.
        manifest = client.latest_manifest(target)
        assert manifest is not None
        assert manifest["step"] == 100
        chunks = sorted(manifest["chunks"], key=lambda c: c["part_index"])
        assert sum(c["size_bytes"] for c in chunks) == len(state)
        reassembled = b"".join(client.read_object(c["key"]) for c in chunks)
        assert reassembled == state

        # Idempotent re-commit returns the same manifest.
        assert client.commit_checkpoint(ckpt, timeout_s=5) == manifest_key


def test_lease_release_returns_shards(server):
    endpoint, _root = server
    with RuntimeClient(endpoint, worker_id="rank-1", rank=0, world_size=1) as client:
        dataset = client.register_dataset(
            DatasetSpec(uri="s3://bucket/datasets/toy2", num_shards=2)
        )
        lease = client.acquire_shard_lease(dataset, epoch=0, requested_shards=2)
        assert client.acquire_shard_lease(dataset, epoch=0).exhausted
        client.release_shard_lease(lease, completed=False)
        again = client.acquire_shard_lease(dataset, epoch=0, requested_shards=2)
        assert (again.begin, again.end) == (0, 2)
