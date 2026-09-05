#!/usr/bin/env bash
# Show current replica set state: who's primary, who's secondary, and lag.
set -euo pipefail

docker exec mongo1 mongosh --quiet --eval '
  const s = rs.status();
  print("Replica set: " + s.set);
  s.members.forEach(m => {
    print(
      m.name.padEnd(16) + " " +
      m.stateStr.padEnd(10) +
      " health=" + m.health +
      (m.optimeDate ? "  optime=" + m.optimeDate.toISOString() : "")
    );
  });
'
