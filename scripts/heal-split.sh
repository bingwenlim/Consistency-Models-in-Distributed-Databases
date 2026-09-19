#!/usr/bin/env bash
# Heal a partition created by partition-split.sh: flush the iptables rules in
# every node's network namespace so all nodes can talk to each other again.
#
# Usage: ./scripts/heal-split.sh
set -euo pipefail

cd "$(dirname "$0")/.."

# Run a command in a container's network namespace, bounded by a timeout so a
# wedged Docker sidecar fails fast instead of hanging the whole experiment.
# Uses a plain `docker run` sidecar (the mongo image has no iptables) and a
# portable timeout (macOS has no coreutils `timeout`).
NETNS_TIMEOUT="${NETNS_TIMEOUT:-20}"
run_netns() {  # run_netns <container> <cmd...>
  local host="$1"; shift
  perl -e 'alarm shift; exec @ARGV or exit 127' "$NETNS_TIMEOUT" \
    docker run --rm --net "container:${host}" --cap-add NET_ADMIN \
    nicolaka/netshoot "$@" >/dev/null 2>&1 || return $?
}

for n in mongo1 mongo2 mongo3 mongo4 mongo5; do
  run_netns "$n" iptables -F || true
  echo "    flushed rules on $n"
done
echo "==> Partition healed. Nodes will re-sync; divergent (un-replicated) writes roll back."
