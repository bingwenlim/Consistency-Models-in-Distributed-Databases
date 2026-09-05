#!/usr/bin/env bash
# Tear down the cluster. By default keeps data volumes.
#   ./scripts/down.sh          # stop & remove containers, keep data
#   ./scripts/down.sh --wipe   # also delete data volumes (fresh start)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--wipe" ]; then
  echo "==> Stopping cluster and WIPING data volumes..."
  docker compose down -v
else
  echo "==> Stopping cluster (data volumes preserved)..."
  docker compose down
fi
echo "==> Done."
