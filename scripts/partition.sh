#!/usr/bin/env bash
# Partition a node from the rest of the cluster by disconnecting it from the
# Docker network. The container keeps running but can't reach (or be reached by)
# the other nodes -- simulating a network partition.
#
# Usage: ./scripts/partition.sh mongo3
set -euo pipefail

NODE="${1:?Usage: partition.sh <mongoN>  (e.g. mongo3)}"

docker network disconnect mongo-cluster "$NODE"
echo "==> $NODE partitioned (disconnected from mongo-cluster network)."
echo "    It is now isolated. Reconnect with: ./scripts/heal.sh $NODE"
