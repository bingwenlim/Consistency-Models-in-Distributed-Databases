#!/usr/bin/env bash
# Single entrypoint for the consistency experiments.
#
# Usage:
#   ./run.sh read-your-writes <config> [--control]
#     configs: majority/majority | majority/w:1 | local/w:1 | local/majority
#     --control  (local/majority only) run the majority-read control -> UNAVAILABLE
#
# Ensures mongo1 is PRIMARY first, then runs the model. ALWAYS heals the partition
# and restores electionTimeoutMillis=10000 on exit (even on failure / Ctrl-C).
#
# More models (monotonic-reads, monotonic-writes, writes-follow-reads) slot in as
# experiments/models/<name>.py and a case below.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$HERE/../scripts"
MODEL="${1:?Usage: run.sh <model> <config> [--control]}"
shift

restore() {
  echo "==> [cleanup] healing partition + restoring electionTimeoutMillis=10000"
  "$SCRIPTS/heal-split.sh" >/dev/null 2>&1 || true
  local js='const c=rs.conf();c.settings.electionTimeoutMillis=10000;rs.reconfig(c);'
  for n in mongo1 mongo2 mongo3 mongo4 mongo5; do
    docker exec "$n" mongosh --quiet --eval "$js" >/dev/null 2>&1 && break || true
  done
}
trap restore EXIT

ensure_mongo1_primary() {
  echo "==> Ensuring mongo1 is PRIMARY before the experiment..."
  "$SCRIPTS/heal-split.sh" >/dev/null 2>&1 || true
  sleep 3
  for _ in $(seq 1 8); do
    CURP="$(docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().primary' 2>/dev/null || true)"
    [ "$CURP" = "mongo1:27017" ] && break
    if [ -n "$CURP" ] && [ "$CURP" != "null" ]; then
      NODE="$(echo "$CURP" | cut -d: -f1)"
      docker exec "$NODE" mongosh --quiet --eval 'try{rs.stepDown(120)}catch(e){}' >/dev/null 2>&1 || true
    fi
    sleep 3
  done
  CURP="$(docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().primary' 2>/dev/null || true)"
  if [ "$CURP" != "mongo1:27017" ]; then
    echo "    WARNING: mongo1 is not PRIMARY (is: ${CURP:-none}). Result may be inconclusive."
  else
    echo "    mongo1 is PRIMARY."
  fi
}

case "$MODEL" in
  read-your-writes|ryow)
    ensure_mongo1_primary
    uv run "$HERE/models/read_your_writes.py" --config "$@"
    ;;
  monotonic-writes|mw)
    ensure_mongo1_primary
    uv run "$HERE/models/monotonic_writes.py" --config "$@"
    ;;
  writes-follow-reads|wfr)
    ensure_mongo1_primary
    uv run "$HERE/models/writes_follow_reads.py" --config "$@"
    ;;
  *)
    echo "Unknown model: $MODEL" >&2
    echo "Available: read-your-writes, monotonic-writes, writes-follow-reads" >&2
    exit 1
    ;;
esac
