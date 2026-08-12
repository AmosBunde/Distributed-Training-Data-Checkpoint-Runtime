#!/usr/bin/env bash
# Generate gRPC stubs from proto/*.proto.
#
# Rust: nothing to do here — runtime/crates/api/build.rs compiles the protos at
#       cargo build time via tonic-build with a vendored protoc, so Rust stubs
#       are never checked in.
#
# Python: stubs ARE checked in (python/src/dtr/_proto/) so that installing the
#       SDK does not require grpcio-tools. Re-run this script whenever a .proto
#       changes and commit the result. CI fails if the committed stubs drift.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$REPO_ROOT/proto"
PY_OUT="$REPO_ROOT/python/src/dtr/_proto"

command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 1; }

python3 - <<'PY' || { echo "grpcio-tools missing: pip install grpcio-tools" >&2; exit 1; }
import grpc_tools.protoc  # noqa: F401
PY

mkdir -p "$PY_OUT"

python3 -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$PY_OUT" \
  --grpc_python_out="$PY_OUT" \
  "$PROTO_DIR"/runtime.proto \
  "$PROTO_DIR"/checkpoint.proto \
  "$PROTO_DIR"/data.proto

# grpc_tools emits absolute imports (import runtime_pb2); rewrite them to be
# package-relative so the stubs work inside dtr._proto.
sed -i -E 's/^import (runtime|checkpoint|data)_pb2/from . import \1_pb2/' \
  "$PY_OUT"/runtime_pb2_grpc.py "$PY_OUT"/checkpoint_pb2_grpc.py "$PY_OUT"/data_pb2_grpc.py

touch "$PY_OUT/__init__.py"

echo "Python stubs regenerated under $PY_OUT"
