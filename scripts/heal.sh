#!/usr/bin/env bash
# Reconnect (heal) a previously partitioned node back into the cluster.
#
# Usage: ./scripts/heal.sh mongo3
set -euo pipefail

NODE="${1:?Usage: heal.sh <mongoN>  (e.g. mongo3)}"

docker network connect mongo-cluster "$NODE"
echo "==> $NODE reconnected to mongo-cluster network."
echo "    It will re-sync with the replica set shortly."
