#!/usr/bin/env bash
# Create a TRUE two-sided network partition between two groups of nodes, while
# keeping every node reachable from the host (so the experiment client can still
# connect to any node by its published port).
#
# It injects iptables DROP rules into each container's network namespace (via a
# privileged helper container sharing that netns), blocking traffic ONLY between
# the two groups. Host<->container port mappings are unaffected (Docker NATs those
# on the host side), so `mongosh`/PyMongo from the host still reach every node.
#
# Group A defaults to "mongo1 mongo2" (minority, old primary side).
# Group B is everyone else       ("mongo3 mongo4 mongo5", majority, new primary).
#
# Usage: ./scripts/partition-split.sh [nodeA1 nodeA2 ...]
#   default: ./scripts/partition-split.sh mongo1 mongo2
# Heal with: ./scripts/heal-split.sh
set -euo pipefail

cd "$(dirname "$0")/.."

GROUP_A=("$@")
if [ ${#GROUP_A[@]} -eq 0 ]; then
  GROUP_A=(mongo1 mongo2)
fi

ALL=(mongo1 mongo2 mongo3 mongo4 mongo5)

# Build group B = ALL - GROUP_A
in_group_a() { local n="$1"; for a in "${GROUP_A[@]}"; do [ "$a" = "$n" ] && return 0; done; return 1; }
GROUP_B=()
for n in "${ALL[@]}"; do in_group_a "$n" || GROUP_B+=("$n"); done

# Resolve a node's IP on the mongo-cluster network (bash 3.2 compatible: no
# associative arrays).
node_ip() {
  docker network inspect mongo-cluster \
    --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}' \
    | awk -v n="$1" '$1==n {print $2}' | cut -d/ -f1
}

echo "==> Partitioning:"
echo "    Group A (minority / old primary): ${GROUP_A[*]}"
echo "    Group B (majority / new primary): ${GROUP_B[*]}"

# For every node in A, drop traffic to/from every node in B (and vice versa).
block_pair() {
  local host="$1" peerip="$2"
  docker run --rm --net "container:${host}" --cap-add NET_ADMIN nicolaka/netshoot \
    iptables -A INPUT  -s "$peerip" -j DROP >/dev/null 2>&1 || true
  docker run --rm --net "container:${host}" --cap-add NET_ADMIN nicolaka/netshoot \
    iptables -A OUTPUT -d "$peerip" -j DROP >/dev/null 2>&1 || true
}

for a in "${GROUP_A[@]}"; do
  a_ip="$(node_ip "$a")"
  for b in "${GROUP_B[@]}"; do
    b_ip="$(node_ip "$b")"
    block_pair "$a" "$b_ip"
    block_pair "$b" "$a_ip"
  done
done

echo "==> Partition active. Each node is still reachable from the host."
echo "    Heal with: ./scripts/heal-split.sh"
