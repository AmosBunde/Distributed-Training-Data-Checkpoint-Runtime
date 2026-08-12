"""RuntimeClient: the user-facing API for training loops.

Wraps the three gRPC services (runtime, checkpoint, data) behind one client
with session management, background heartbeats, and the checkpoint protocol
(begin -> upload -> report -> poll commit) implemented correctly so training
code stays a few lines.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from types import TracebackType

import grpc

from dtr._proto import (
    checkpoint_pb2,
    checkpoint_pb2_grpc,
    data_pb2,
    data_pb2_grpc,
    runtime_pb2,
    runtime_pb2_grpc,
)
from dtr.checkpoint import (
    CheckpointHandle,
    CheckpointSpec,
    CheckpointTarget,
    CommitStatus,
    sha256_hex,
    split_chunks,
)
from dtr.dataset import DatasetHandle, DatasetSpec, ShardLease
from dtr.transport import TransportConfig

logger = logging.getLogger("dtr")

_UPLOAD_FRAME_BYTES = 1024 * 1024


class RuntimeClient:
    """Client for one worker (rank) of a training job.

    Usage::

        with RuntimeClient("localhost:50051", worker_id="rank-0", rank=0, world_size=8) as c:
            ...

    The context manager registers the worker on entry and stops the heartbeat
    thread on exit. Outside a ``with`` block call :meth:`connect` / :meth:`close`.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        *,
        worker_id: str,
        rank: int = 0,
        world_size: int = 1,
        transport: TransportConfig | None = None,
        labels: dict[str, str] | None = None,
        heartbeat: bool = True,
    ) -> None:
        self.transport = transport or TransportConfig(endpoint=endpoint or "localhost:50051")
        if endpoint is not None:
            self.transport.endpoint = endpoint
        self.worker_id = worker_id
        self.rank = rank
        self.world_size = world_size
        self.labels = labels or {}
        self._heartbeat_enabled = heartbeat

        self._channel: grpc.Channel | None = None
        self._runtime: runtime_pb2_grpc.RuntimeServiceStub | None = None
        self._checkpoint: checkpoint_pb2_grpc.CheckpointServiceStub | None = None
        self._data: data_pb2_grpc.DataServiceStub | None = None

        self._session_token: str | None = None
        self._heartbeat_interval_s = 5.0
        self._hb_thread: threading.Thread | None = None
        self._hb_stop = threading.Event()
        self.current_step = 0

    # ------------------------------------------------------------------ setup

    def connect(self) -> RuntimeClient:
        self._channel = self.transport.channel()
        self._runtime = runtime_pb2_grpc.RuntimeServiceStub(self._channel)
        self._checkpoint = checkpoint_pb2_grpc.CheckpointServiceStub(self._channel)
        self._data = data_pb2_grpc.DataServiceStub(self._channel)
        self._register()
        if self._heartbeat_enabled:
            self._start_heartbeats()
        return self

    def close(self) -> None:
        self._hb_stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=5)
            self._hb_thread = None
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def __enter__(self) -> RuntimeClient:
        return self.connect()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _register(self) -> None:
        resp = self._stub_runtime.RegisterWorker(
            runtime_pb2.RegisterWorkerRequest(
                worker_id=self.worker_id,
                rank=self.rank,
                world_size=self.world_size,
                labels=self.labels,
            )
        )
        self._session_token = resp.session_token
        self._heartbeat_interval_s = max(resp.heartbeat_interval_ms / 1000.0, 0.5)
        logger.info(
            "registered worker %s (rank %d/%d), heartbeat every %.1fs",
            self.worker_id,
            self.rank,
            self.world_size,
            self._heartbeat_interval_s,
        )

    def _start_heartbeats(self) -> None:
        self._hb_stop.clear()

        def loop() -> None:
            while not self._hb_stop.wait(self._heartbeat_interval_s):
                try:
                    resp = self._stub_runtime.Heartbeat(
                        runtime_pb2.HeartbeatRequest(
                            worker_id=self.worker_id,
                            session_token=self._session_token or "",
                            current_step=self.current_step,
                        )
                    )
                    if resp.must_reregister:
                        logger.warning("coordinator requested re-registration; re-registering")
                        self._register()
                except grpc.RpcError as e:  # keep beating through transient failures
                    logger.warning("heartbeat failed: %s", e.code())

        self._hb_thread = threading.Thread(target=loop, name="dtr-heartbeat", daemon=True)
        self._hb_thread.start()

    # -------------------------------------------------------------- datasets

    def register_dataset(self, spec: DatasetSpec) -> DatasetHandle:
        resp = self._stub_runtime.RegisterDataset(
            runtime_pb2.RegisterDatasetRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                spec=runtime_pb2.DatasetSpec(
                    uri=spec.uri,
                    shard_format=spec.shard_format,
                    num_shards=spec.num_shards,
                    shuffle=spec.shuffle,
                    shuffle_seed=spec.shuffle_seed,
                ),
            )
        )
        return DatasetHandle(dataset_id=resp.dataset_id, spec=spec)

    def acquire_shard_lease(
        self, dataset: DatasetHandle, epoch: int, requested_shards: int = 0
    ) -> ShardLease:
        resp = self._stub_runtime.AcquireShardLease(
            runtime_pb2.AcquireShardLeaseRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                dataset_id=dataset.dataset_id,
                epoch=epoch,
                requested_shards=requested_shards,
            )
        )
        begin = resp.shards.begin if resp.HasField("shards") else 0
        end = resp.shards.end if resp.HasField("shards") else 0
        return ShardLease(
            lease_id=resp.lease_id,
            dataset_id=dataset.dataset_id,
            epoch=epoch,
            begin=begin,
            end=end,
            ttl_ms=resp.lease_ttl_ms,
        )

    def renew_shard_lease(self, lease: ShardLease) -> bool:
        """Extend the lease TTL. False means the lease lapsed and its shards
        may have been reassigned — stop reading them and acquire a new lease."""
        resp = self._stub_runtime.RenewShardLease(
            runtime_pb2.RenewShardLeaseRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                lease_id=lease.lease_id,
            )
        )
        return resp.renewed

    def release_shard_lease(self, lease: ShardLease, completed: bool = True) -> None:
        self._stub_runtime.ReleaseShardLease(
            runtime_pb2.ReleaseShardLeaseRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                lease_id=lease.lease_id,
                completed=completed,
            )
        )

    # ------------------------------------------------------------ data plane

    def list_objects(self, prefix: str) -> list[tuple[str, int]]:
        resp = self._stub_data.ListObjects(
            data_pb2.ListObjectsRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                prefix=prefix,
            )
        )
        return [(o.key, o.size_bytes) for o in resp.objects]

    def read_object(
        self,
        key: str,
        *,
        offset: int = 0,
        length: int = 0,
        prefetch_hint: list[str] | None = None,
    ) -> bytes:
        frames = self._stub_data.ReadObject(
            data_pb2.ReadObjectRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                key=key,
                offset=offset,
                length=length,
                prefetch_hint=prefetch_hint or [],
            )
        )
        return b"".join(frame.data for frame in frames)

    # ----------------------------------------------------------- checkpoints

    def register_checkpoint(self, spec: CheckpointSpec) -> CheckpointTarget:
        resp = self._stub_checkpoint.RegisterCheckpointTarget(
            checkpoint_pb2.RegisterCheckpointTargetRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                spec=checkpoint_pb2.CheckpointSpec(
                    uri=spec.uri,
                    write_mode=spec.write_mode,
                    chunk_bytes=spec.chunk_bytes,
                    keep_last=spec.keep_last,
                ),
            )
        )
        return CheckpointTarget(target_id=resp.target_id, spec=spec)

    def begin_checkpoint(self, target: CheckpointTarget, step: int) -> CheckpointHandle:
        resp = self._stub_checkpoint.BeginCheckpoint(
            checkpoint_pb2.BeginCheckpointRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                target_id=target.target_id,
                step=step,
            )
        )
        return CheckpointHandle(
            token=resp.checkpoint_token,
            target=target,
            step=step,
            upload_prefix=resp.upload_prefix,
            expected_participants=resp.expected_participants,
            required_participants=resp.required_participants,
        )

    def upload_state(self, ckpt: CheckpointHandle, data: bytes) -> int:
        """Upload this rank's state for the checkpoint: chunk, stream each
        chunk through the data plane, verify server hashes, report integrity
        metadata, and mark this rank complete. Returns bytes uploaded."""
        chunks = split_chunks(data, ckpt.target.spec.chunk_bytes)
        metas: list[checkpoint_pb2.ChunkMeta] = []
        for part_index, chunk in enumerate(chunks):
            local_sha = sha256_hex(chunk)
            resp = self._stub_data.UploadCheckpointChunk(
                self._upload_frames(ckpt.token, part_index, chunk)
            )
            if resp.sha256 != local_sha:
                raise RuntimeError(
                    f"chunk {part_index} hash mismatch: sent {local_sha}, server saw {resp.sha256}"
                )
            metas.append(
                checkpoint_pb2.ChunkMeta(
                    key=resp.key,
                    size_bytes=resp.size_bytes,
                    sha256=resp.sha256,
                    rank=self.rank,
                    part_index=part_index,
                )
            )
        self._stub_checkpoint.ReportChunk(
            checkpoint_pb2.ReportChunkRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                checkpoint_token=ckpt.token,
                chunks=metas,
                rank_complete=True,
            )
        )
        return len(data)

    def _upload_frames(
        self, token: str, part_index: int, chunk: bytes
    ) -> Iterator[data_pb2.UploadChunkRequest]:
        yield data_pb2.UploadChunkRequest(
            header=data_pb2.UploadChunkHeader(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                checkpoint_token=token,
                rank=self.rank,
                part_index=part_index,
            )
        )
        for i in range(0, len(chunk), _UPLOAD_FRAME_BYTES):
            yield data_pb2.UploadChunkRequest(data=chunk[i : i + _UPLOAD_FRAME_BYTES])

    def commit_checkpoint(
        self,
        ckpt: CheckpointHandle,
        *,
        poll_interval_s: float = 0.5,
        timeout_s: float = 600.0,
    ) -> str:
        """Poll CommitCheckpoint until COMMITTED (returns the manifest key)
        or ABORTED / timeout (raises)."""
        deadline = time.monotonic() + timeout_s
        while True:
            resp = self._stub_checkpoint.CommitCheckpoint(
                checkpoint_pb2.CommitCheckpointRequest(
                    worker_id=self.worker_id,
                    session_token=self._require_session(),
                    checkpoint_token=ckpt.token,
                )
            )
            status = checkpoint_pb2.CommitCheckpointResponse.Status.Name(resp.status)
            if status == "COMMITTED":
                return resp.manifest_key
            if status == "ABORTED":
                raise RuntimeError(f"checkpoint step {ckpt.step} aborted")
            if time.monotonic() > deadline:
                raise TimeoutError(f"checkpoint step {ckpt.step} did not commit in {timeout_s}s")
            time.sleep(poll_interval_s)

    def commit_status(self, ckpt: CheckpointHandle) -> CommitStatus:
        resp = self._stub_checkpoint.CommitCheckpoint(
            checkpoint_pb2.CommitCheckpointRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                checkpoint_token=ckpt.token,
            )
        )
        return CommitStatus(
            checkpoint_pb2.CommitCheckpointResponse.Status.Name(resp.status).lower()
        )

    def abort_checkpoint(self, ckpt: CheckpointHandle, reason: str = "") -> None:
        self._stub_checkpoint.AbortCheckpoint(
            checkpoint_pb2.AbortCheckpointRequest(
                worker_id=self.worker_id,
                session_token=self._require_session(),
                checkpoint_token=ckpt.token,
                reason=reason,
            )
        )

    def latest_manifest(self, target: CheckpointTarget) -> dict | None:
        """Recovery entry point: newest committed manifest, or None."""
        resp = self._stub_checkpoint.GetLatestManifest(
            checkpoint_pb2.GetLatestManifestRequest(target_id=target.target_id)
        )
        if not resp.found:
            return None
        return {
            "step": resp.step,
            "manifest_key": resp.manifest_key,
            "chunks": [
                {
                    "key": c.key,
                    "size_bytes": c.size_bytes,
                    "sha256": c.sha256,
                    "rank": c.rank,
                    "part_index": c.part_index,
                }
                for c in resp.chunks
            ],
        }

    # -------------------------------------------------------------- internal

    def _require_session(self) -> str:
        if self._session_token is None:
            raise RuntimeError("client is not connected; call connect() or use a with-block")
        return self._session_token

    @property
    def _stub_runtime(self) -> runtime_pb2_grpc.RuntimeServiceStub:
        if self._runtime is None:
            raise RuntimeError("client is not connected; call connect() or use a with-block")
        return self._runtime

    @property
    def _stub_checkpoint(self) -> checkpoint_pb2_grpc.CheckpointServiceStub:
        if self._checkpoint is None:
            raise RuntimeError("client is not connected; call connect() or use a with-block")
        return self._checkpoint

    @property
    def _stub_data(self) -> data_pb2_grpc.DataServiceStub:
        if self._data is None:
            raise RuntimeError("client is not connected; call connect() or use a with-block")
        return self._data
