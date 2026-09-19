#!/usr/bin/env bash
# Repeated-trials harness (enhanced design).
#
# The per-config runs in run.sh each show ONE outcome. A single VIOLATED is enough
# to prove a guarantee does not hold, but it says nothing about how OFTEN the
# failure window is hit, and a single SAFE/INCONCLUSIVE can be bad luck. This
# wrapper runs a model x config N times and reports the verdict distribution --
# e.g. "VIOLATED 9/10" -- which is what belongs in the report's Results table.
#
# Usage:
#   ./trials.sh <model> <config> [N]      # default N=5
#   ./trials.sh monotonic-writes majority/w:1 10
#   ./trials.sh writes-follow-reads local/w:1 10
#
# Output: a tally to stdout and every raw run under trials-out/<model>-<config>-<i>.log
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MODEL="${1:?Usage: trials.sh <model> <config> [N]}"
CONFIG="${2:?Usage: trials.sh <model> <config> [N]}"
N="${3:-5}"
OUT="$HERE/trials-out"
mkdir -p "$OUT"
tag="$(echo "${MODEL}-${CONFIG}" | tr '/:' '__')"

declare -i violated=0 safe=0 held=0 unavailable=0 inconclusive=0 other=0
for i in $(seq 1 "$N"); do
  log="$OUT/${tag}-${i}.log"
  echo "==> trial $i/$N: $MODEL $CONFIG" >&2
  "$HERE/run.sh" "$MODEL" "$CONFIG" > "$log" 2>&1 || true
  v="$(grep -oE 'verdict:[[:space:]]+[A-Z]+' "$log" | head -1 | awk '{print $2}')"
  echo "    -> ${v:-NONE}" >&2
  case "$v" in
    VIOLATED)     violated+=1 ;;
    SAFE)         safe+=1 ;;
    HELD)         held+=1 ;;
    UNAVAILABLE)  unavailable+=1 ;;
    INCONCLUSIVE) inconclusive+=1 ;;
    *)            other+=1 ;;
  esac
  sleep 5   # let the cluster settle between trials
done

echo
echo "=== trials: $MODEL $CONFIG (N=$N) ==="
printf "  VIOLATED     %d/%d\n" "$violated" "$N"
printf "  SAFE         %d/%d\n" "$safe" "$N"
printf "  HELD         %d/%d\n" "$held" "$N"
printf "  UNAVAILABLE  %d/%d\n" "$unavailable" "$N"
printf "  INCONCLUSIVE %d/%d\n" "$inconclusive" "$N"
[ "$other" -gt 0 ] && printf "  (no verdict) %d/%d\n" "$other" "$N"
echo "  raw logs: $OUT/${tag}-*.log"
