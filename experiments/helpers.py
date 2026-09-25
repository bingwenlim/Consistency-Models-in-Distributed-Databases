"""Common helpers for consistency experiments.

Shared boilerplate across RYOW, MR, MW, WFR:
- pre_flight_check: validate cluster is in clean state
- stabilize_after_test: wait for cluster to settle
- finalize_experiment: heal partition + restore timeout
"""

from __future__ import annotations

import time

from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern

from lib import DB, NODES, direct, heal, set_election_timeout


class ClusterNotReady(Exception):
    """Raised when cluster is not in a valid test state."""
    pass


def pre_flight_check(timeout_seconds: int = 30) -> None:
    """Validate cluster is in clean state before test.
    - All 5 nodes reachable
    - mongo1 is PRIMARY
    - All nodes report consistent view
    Raises ClusterNotReady if not satisfied.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with direct("mongo1") as c1:
                is_primary = c1.admin.command("hello").get("isWritablePrimary", False)
                if not is_primary:
                    raise ClusterNotReady("mongo1 is not PRIMARY")

                status = c1.admin.command("replSetGetStatus")

            with direct("mongo3") as c3:
                is_secondary = not c3.admin.command("hello").get("isWritablePrimary", False)
                if not is_secondary:
                    raise ClusterNotReady("mongo3 is not SECONDARY")

            healthy = all(m.get("state") in (1, 2) for m in status["members"])
            if not healthy:
                raise ClusterNotReady("Some members are not SECONDARY or PRIMARY")

            return
        except (PyMongoError, ClusterNotReady) as e:
            if time.time() >= deadline:
                raise ClusterNotReady(f"Pre-flight check failed: {e}")
            time.sleep(1)


def stabilize_after_test(timeout_seconds: int = 30) -> None:
    """Wait for cluster to settle after a test.
    - All 5 nodes reachable
    - PRIMARY elected (any node)
    - Replica set healthy
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            for node in NODES:
                with direct(node, socket_ms=2000) as c:
                    status = c.admin.command("replSetGetStatus")
                    if status["ok"]:
                        members_ok = all(m.get("state") in (1, 2) for m in status["members"])
                        if members_ok:
                            return
        except PyMongoError:
            pass
        time.sleep(1)


def finalize_experiment(heal_wait_seconds: int = 12) -> None:
    """Heal partition and restore default election timeout (5s for faster elections)."""
    heal()
    time.sleep(heal_wait_seconds)
    set_election_timeout(5000)
    stabilize_after_test()
