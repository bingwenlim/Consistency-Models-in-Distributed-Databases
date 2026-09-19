"""Shared helpers for the consistency experiments.

Connections: the client talks to specific nodes by their published localhost
ports with directConnection=true. A replicaSet URI is NOT usable from the host --
the driver would discover members by their container hostnames (mongo1:27017,...)
which don't resolve on the host.

Topology: mongo1 priority 2 (PRIMARY), mongo3 priority 1 (failover), mongo2/4/5
priority 0. So a partition that isolates mongo1+mongo2 (minority) leaves mongo3 to
win on the mongo3+mongo4+mongo5 (majority) side.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Host-port map from docker-compose.yml.
NODES = {
    "mongo1": 27017,
    "mongo2": 27018,
    "mongo3": 27019,
    "mongo4": 27020,
    "mongo5": 27021,
}

DB = "consistency"
SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"

OLD_PRIMARY = "mongo1"        # priority 2; leads the minority side after partition
FAILOVER = "mongo3"           # priority 1; wins the majority side
MINORITY_SECONDARY = "mongo2" # rides with mongo1 on the minority side
MINORITY = ["mongo1", "mongo2"]


def direct(node: str, socket_ms: int = 5000) -> MongoClient:
    """Direct connection to one node by name (bypasses replica-set discovery)."""
    return MongoClient(
        f"mongodb://localhost:{NODES[node]}/?directConnection=true",
        serverSelectionTimeoutMS=3000,
        socketTimeoutMS=socket_ms,
    )


def run_script(script: str, *args: str) -> None:
    subprocess.run([str(SCRIPTS / script), *args], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def partition_minority() -> None:
    run_script("partition-split.sh", *MINORITY)


def heal() -> None:
    run_script("heal-split.sh")


def set_election_timeout(ms: int) -> None:
    """Set electionTimeoutMillis via one rs.reconfig. Tries each node so whoever
    is primary accepts it. Priority is left at baseline -- raising it does NOT
    speed elections (the full electionTimeoutMillis elapses before any vote).
    """
    js = f"const c=rs.conf(); c.settings.electionTimeoutMillis={ms}; rs.reconfig(c);"
    for node in NODES:
        r = subprocess.run(
            ["docker", "exec", node, "mongosh", "--quiet", "--eval", js],
            capture_output=True, text=True,
        )
        if r.returncode == 0 and "errmsg" not in (r.stdout + r.stderr):
            return


def wait_primary(node: str, timeout: int) -> bool:
    """Poll until `node` reports itself writable primary, or until timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = direct(node, socket_ms=2000)
            if c.admin.command("hello").get("isWritablePrimary"):
                c.close()
                return True
            c.close()
        except PyMongoError:
            pass
        time.sleep(2)
    return False


def print_state(label: str) -> None:
    print(f"    [state @ {label}]")
    for node in (OLD_PRIMARY, FAILOVER):
        try:
            c = direct(node)
            h = c.admin.command("hello")
            print(f"      {node}: reachable, isWritablePrimary={h.get('isWritablePrimary')}")
            c.close()
        except PyMongoError:
            print(f"      {node}: unreachable")
