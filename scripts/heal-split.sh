#!/usr/bin/env bash
# Heal a partition created by partition-split.sh: flush the iptables rules in
# every node's network namespace so all nodes can talk to each other again.
#
# Usage: ./scripts/heal-split.sh
set -euo pipefail

cd "$(dirname "$0")/.."

for n in mongo1 mongo2 mongo3 mongo4 mongo5; do
  docker run --rm --net "container:${n}" --cap-add NET_ADMIN nicolaka/netshoot \
    iptables -F >/dev/null 2>&1 || true
  echo "    flushed rules on $n"
done
echo "==> Partition healed. Nodes will re-sync; divergent (un-replicated) writes roll back."
