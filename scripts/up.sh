#!/usr/bin/env bash
# Bring up the 5-node cluster and initialize the replica set.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Starting 5 MongoDB containers..."
docker compose up -d

echo "==> Waiting for mongo1 to accept connections..."
until docker exec mongo1 mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null 2>&1; do
  sleep 1
done

echo "==> Initializing replica set rs0..."
if docker exec mongo1 mongosh --quiet --eval 'rs.status().ok' >/dev/null 2>&1; then
  echo "    Replica set already initialized. Skipping."
else
  docker exec -i mongo1 mongosh --quiet < scripts/rs-init.js
fi

echo "==> Waiting for a PRIMARY to be elected..."
until [ "$(docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().primary ? "yes" : "no"' 2>/dev/null)" = "yes" ]; do
  sleep 1
done

echo "==> Ensuring mongo1 is PRIMARY..."
# mongo1 has the highest priority so it should win, but election timing on a
# fresh set can briefly seat another node. If mongo1 isn't primary, step down
# whoever is; mongo1's priority then wins the re-election.
for _ in $(seq 1 15); do
  PRIMARY="$(docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().primary' 2>/dev/null)"
  if [ "$PRIMARY" = "mongo1:27017" ]; then
    break
  fi
  echo "    Current PRIMARY is ${PRIMARY:-none}; stepping it down to favor mongo1..."
  # Find the current primary's container name from its host:port and step it down.
  CUR="$(echo "$PRIMARY" | cut -d: -f1)"
  if [ -n "$CUR" ]; then
    docker exec "$CUR" mongosh --quiet --eval 'try { rs.stepDown(60) } catch (e) {}' >/dev/null 2>&1 || true
  fi
  sleep 3
done

FINAL_PRIMARY="$(docker exec mongo1 mongosh --quiet --eval 'rs.isMaster().primary' 2>/dev/null)"
if [ "$FINAL_PRIMARY" != "mongo1:27017" ]; then
  echo "    WARNING: mongo1 is not PRIMARY (current: ${FINAL_PRIMARY:-none}). Continuing anyway."
else
  echo "    mongo1 is PRIMARY."
fi

echo "==> Cluster is up. Status:"
docker exec mongo1 mongosh --quiet --eval 'rs.status().members.forEach(m => print(m.name + " -> " + m.stateStr))'

echo "==> Setting electionTimeoutMillis to 5000ms for faster elections..."
docker exec mongo1 mongosh --quiet --eval 'const c=rs.conf(); c.settings.electionTimeoutMillis=5000; rs.reconfig(c)' >/dev/null 2>&1 || true

cat <<'EOF'

Connect from your host with:
  mongosh "mongodb://localhost:27017,localhost:27018,localhost:27019,localhost:27020,localhost:27021/?replicaSet=rs0"

Per-node host ports:
  mongo1 -> 27017   mongo2 -> 27018   mongo3 -> 27019
  mongo4 -> 27020   mongo5 -> 27021
EOF
