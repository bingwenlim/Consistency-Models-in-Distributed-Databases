"""Monotonic-reads (MR) across all four readConcern x writeConcern configs.

MR: within a session, a read never returns an older value than an earlier read.
If Read 1 sees X=1, no later read in that session may return X=0.

Mechanism: divergent-read. Read 1 sees X=1 on the failover primary. Read 2,
carrying Read 1's causal tokens, targets a minority secondary whose clock has
been advanced past T1 but which never received X=1.

Expected verdicts:
  majority/majority -> NOT_VIOLATED  (majority read on minority times out, no stale data)
  majority/w:1      -> NOT_VIOLATED  (majority read on minority times out, no stale data)
  local/majority    -> VIOLATED      (local read gates on clock, returns X=0)
  local/w:1         -> VIOLATED      (local read gates on clock, returns X=0)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from uuid import uuid4

from pymongo.errors import ExecutionTimeout, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib import (  # noqa: E402
    DB, OLD_PRIMARY, FAILOVER, MINORITY_SECONDARY,
    direct, partition_minority, set_election_timeout, wait_primary,
)
from helpers import finalize_experiment

CONFIGS = {
    "majority/majority": ("majority", "majority"),
    "majority/w:1": ("majority", 1),
    "local/w:1": ("local", 1),
    "local/majority": ("local", "majority"),
}

ELECTION_TIMEOUT_MS = 120000
NEW_PRIMARY_WAIT_SECONDS = 175
READ_TIMEOUT_MS = 6000


class Inconclusive(Exception):
    pass


def wait_for_value(node: str, collection: str, key: str, value: int, seconds: int) -> bool:
    """Wait until a local read on a specific node sees a replicated document."""
    deadline = time.monotonic() + seconds
    with direct(node) as client:
        coll = client[DB].get_collection(collection, read_concern=ReadConcern("local"))
        while time.monotonic() < deadline:
            try:
                doc = coll.find_one({"k": key}, max_time_ms=2000)
                if doc is not None and doc.get("v") == value:
                    return True
            except PyMongoError:
                pass
            time.sleep(0.5)
    return False


def check_election_timeout(expected_ms: int) -> None:
    """Verify the replica set's election timeout is set to the expected value."""
    with direct(OLD_PRIMARY) as client:
        config = client.admin.command("replSetGetConfig")["config"]
    actual = config.get("settings", {}).get("electionTimeoutMillis", 10000)
    if actual != expected_ms:
        raise Inconclusive(f"electionTimeoutMillis is {actual}, expected {expected_ms}")


def monotonic_reads(read_concern: str, write_concern, config_label: str) -> str:
    """Divergent-read mechanism: Read 1 sees X=1 on failover, Read 2 targets stale minority."""
    key = f"mr-{uuid4().hex}"
    tick_key = f"mr-tick-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not reach its final read"

    try:
        set_election_timeout(ELECTION_TIMEOUT_MS)
        check_election_timeout(ELECTION_TIMEOUT_MS)
        time.sleep(3)

        with direct(OLD_PRIMARY) as client:
            coll = client[DB].get_collection(
                "mr", write_concern=WriteConcern(w="majority", wtimeout=10000)
            )
            coll.insert_one({"k": key, "v": 0})
        if not wait_for_value(MINORITY_SECONDARY, "mr", key, 0, 20):
            raise Inconclusive("baseline X=0 did not reach the minority secondary")
        print(f"==> baseline X=0 is present on {MINORITY_SECONDARY}", flush=True)

        partition_minority()
        print(f"==> waiting for {FAILOVER} to become PRIMARY", flush=True)
        if not wait_primary(FAILOVER, NEW_PRIMARY_WAIT_SECONDS):
            raise Inconclusive(f"{FAILOVER} was not elected within the timeout")

        with direct(FAILOVER, socket_ms=12000) as client:
            with client.start_session(causal_consistency=True) as session:
                writes = client[DB].get_collection(
                    "mr", write_concern=WriteConcern(w=write_concern, wtimeout=8000)
                )
                result = writes.update_one(
                    {"k": key}, {"$set": {"v": 1}}, session=session
                )
                if result.matched_count != 1:
                    raise Inconclusive("X=1 write did not match the baseline document")
                print(f"==> X=1 acknowledged on {FAILOVER} with w={write_concern}", flush=True)

                reads = client[DB].get_collection(
                    "mr", read_concern=ReadConcern(read_concern)
                )
                first = reads.find_one(
                    {"k": key}, session=session, max_time_ms=READ_TIMEOUT_MS
                )
                first_value = first.get("v") if first else None
                print(f"==> Read 1 on {FAILOVER}: X={first_value}", flush=True)
                if first_value != 1:
                    raise Inconclusive("first read did not observe X=1")

                operation_time = session.operation_time
                cluster_time = session.cluster_time
                if operation_time is None:
                    raise Inconclusive("first session has no causal operation time")

        # An unrelated write advances the minority side's clock beyond Read 1.
        time.sleep(1.2)
        with direct(OLD_PRIMARY) as client:
            client[DB].get_collection(
                "mr_ticks", write_concern=WriteConcern(w=1)
            ).insert_one({"k": tick_key, "v": 1})
        if not wait_for_value(MINORITY_SECONDARY, "mr_ticks", tick_key, 1, 12):
            raise Inconclusive("minority clock-advance write did not reach mongo2")
        print(f"==> clock-advance write replicated to {MINORITY_SECONDARY}", flush=True)

        with direct(MINORITY_SECONDARY, socket_ms=12000) as client:
            with client.start_session(causal_consistency=True) as session:
                if cluster_time is not None:
                    session.advance_cluster_time(cluster_time)
                session.advance_operation_time(operation_time)
                reads = client[DB].get_collection(
                    "mr", read_concern=ReadConcern(read_concern)
                )
                try:
                    second = reads.find_one(
                        {"k": key}, session=session, max_time_ms=READ_TIMEOUT_MS
                    )
                    second_value = second.get("v") if second else None
                    print(
                        f"==> Read 2 on {MINORITY_SECONDARY}: X={second_value}",
                        flush=True,
                    )
                    if second_value == 0:
                        verdict = "VIOLATED"
                        detail = "Read 2 returned X=0 after Read 1 returned X=1"
                    elif second_value == 1:
                        verdict = "NOT_VIOLATED"
                        detail = "Read 2 returned X=1"
                    else:
                        raise Inconclusive(f"Read 2 returned unexpected value {second_value}")
                except ExecutionTimeout:
                    verdict = "NOT_VIOLATED"
                    detail = "Read 2 timed out rather than returning stale data"

    except Inconclusive as exc:
        detail = str(exc)
    except (PyMongoError, OSError) as exc:
        detail = f"{type(exc).__name__}: {str(exc)[:60]}"
    finally:
        finalize_experiment()

    print()
    print(f"=== MR / divergent-read ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    return monotonic_reads(read_concern, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-reads experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    args = ap.parse_args()
    if run(args.config) == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
