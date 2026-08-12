#!/usr/bin/env bash
# Tear down the local DTR stack. Pass --volumes to also delete MinIO data.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTRA=()
if [[ "${1:-}" == "--volumes" ]]; then
  EXTRA+=(--volumes)
  echo ">> removing stack INCLUDING stored data"
else
  echo ">> removing stack (MinIO data kept; pass --volumes to delete)"
fi
docker compose -f "$REPO_ROOT/docker/compose.yml" down "${EXTRA[@]}"
