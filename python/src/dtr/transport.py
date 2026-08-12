"""Channel construction: endpoint parsing, TLS/mTLS selection, retry policy."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import grpc

# gRPC service-config retry policy applied to idempotent control-plane RPCs.
# UNAVAILABLE covers coordinator restarts and transient network failures.
_RETRY_POLICY = {
    "methodConfig": [
        {
            "name": [
                {"service": "dtr.runtime.v1.RuntimeService"},
                {"service": "dtr.checkpoint.v1.CheckpointService"},
            ],
            "retryPolicy": {
                "maxAttempts": 5,
                "initialBackoff": "0.2s",
                "maxBackoff": "5s",
                "backoffMultiplier": 2.0,
                "retryableStatusCodes": ["UNAVAILABLE"],
            },
        }
    ]
}


@dataclass
class TransportConfig:
    """How to reach the runtime server.

    TLS modes:
      - plaintext (default, in-cluster with mTLS handled by a mesh, or local dev)
      - server TLS: set ``tls=True`` (and optionally ``root_ca``)
      - mutual TLS: also set ``client_cert`` and ``client_key``
    """

    endpoint: str = "localhost:50051"
    tls: bool = False
    root_ca: bytes | None = None
    client_cert: bytes | None = None
    client_key: bytes | None = None
    # Max message size: checkpoint chunk frames can be large.
    max_message_mb: int = 64
    options: list[tuple[str, str | int]] = field(default_factory=list)

    def channel(self) -> grpc.Channel:
        opts: list[tuple[str, str | int]] = [
            ("grpc.max_send_message_length", self.max_message_mb * 1024 * 1024),
            ("grpc.max_receive_message_length", self.max_message_mb * 1024 * 1024),
            ("grpc.service_config", json.dumps(_RETRY_POLICY)),
            ("grpc.enable_retries", 1),
            *self.options,
        ]
        if not self.tls:
            return grpc.insecure_channel(self.endpoint, options=opts)
        creds = grpc.ssl_channel_credentials(
            root_certificates=self.root_ca,
            private_key=self.client_key,
            certificate_chain=self.client_cert,
        )
        return grpc.secure_channel(self.endpoint, credentials=creds, options=opts)
