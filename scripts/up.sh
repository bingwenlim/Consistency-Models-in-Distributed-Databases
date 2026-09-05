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

echo "==> Cluster is up. Status:"
docker exec mongo1 mongosh --quiet --eval 'rs.status().members.forEach(m => print(m.name + " -> " + m.stateStr))'

cat <<'EOF'

Connect from your host with:
  mongosh "mongodb://localhost:27017,localhost:27018,localhost:27019,localhost:27020,localhost:27021/?replicaSet=rs0"

Per-node host ports:
  mongo1 -> 27017   mongo2 -> 27018   mongo3 -> 27019
  mongo4 -> 27020   mongo5 -> 27021
EOF
