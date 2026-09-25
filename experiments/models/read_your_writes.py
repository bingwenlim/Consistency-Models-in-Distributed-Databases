"""Read-your-writes (RYOW) across all four readConcern x writeConcern configs.

RYOW: after a client writes in a causally consistent session, a later read in
that session must return that write or newer -- never older.

Two mechanisms expose RYOW violations:
  - Rollback: w:1 writes ack on isolated minority, then roll back on heal
  - Divergent-read: local reads gate on clock, not data presence

Note: the rollback mechanism tests durable RYOW across recovery, not a literal
second read in the same live session. The write's session is closed and the
final check reads the recovered durable state after healing. A write the client
was told succeeded, then lost on rollback, is the RYOW violation -- consistent
with defining these experiments over durable causal histories.

Expected verdicts:
  majority/majority -> NOT_VIOLATED  (write refused on minority)
  majority/w:1      -> VIOLATED      (write rolls back)
  local/w:1         -> VIOLATED      (write rolls back)
  local/majority    -> VIOLATED      (divergent read returns stale data)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from uuid import uuid4

from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib import (  # noqa: E402
    DB, NODES, OLD_PRIMARY, FAILOVER, MINORITY_SECONDARY, MINORITY,
    direct, partition_minority, print_state, set_election_timeout, wait_primary,
)
from helpers import finalize_experiment

CONFIGS = {
    "majority/majority": ("majority", "majority"),
    "majority/w:1": ("majority", 1),
    "local/w:1": ("local", 1),
    "local/majority": ("local", "majority"),
}

DIVERGENT_ELECTION_TIMEOUT_MS = 120000
DIVERGENT_PRIMARY_WAIT = 175


class Inconclusive(Exception):
    pass


def wait_for_tick(node: str, tick: str, seconds: int) -> bool:
    """Wait until a local read on a node sees the clock-advance tick document."""
    deadline = time.monotonic() + seconds
    with direct(node) as client:
        coll = client[DB].get_collection("dummy", read_concern=ReadConcern("local"))
        while time.monotonic() < deadline:
            try:
                if coll.find_one({"tick": tick}, max_time_ms=2000) is not None:
                    return True
            except PyMongoError:
                pass
            time.sleep(0.5)
    return False


def rollback(write_concern, config_label: str) -> str:
    """Rollback mechanism: w:1 write acks on isolated minority, rolls back on heal."""
    wc_label = "majority" if write_concern == "majority" else f"w:{write_concern}"
    key = f"ryw-{wc_label}-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"
    healed = False

    try:
        set_election_timeout(30000)
        time.sleep(2)

        p = direct(OLD_PRIMARY)
        p[DB].get_collection("ryw", write_concern=WriteConcern(w="majority")).insert_one(
            {"k": key, "v": 0}
        )
        print(f"==> baseline: {key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        acked = False
        p = MongoClient(
            f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
            serverSelectionTimeoutMS=2000, socketTimeoutMS=3000,
        )
        try:
            with p.start_session(causal_consistency=True) as s:
                coll = p[DB].get_collection("ryw", write_concern=WriteConcern(w=write_concern, wtimeout=3000))
                try:
                    coll.update_one({"k": key}, {"$set": {"v": 1}}, session=s)
                    acked = True
                    print(f"==> {wc_label} write X=1 ACKED on {OLD_PRIMARY}", flush=True)
                except PyMongoError as e:
                    print(f"==> {wc_label} write X=1 REFUSED: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        print(f"==> waiting for {FAILOVER} to become PRIMARY", flush=True)
        if not wait_primary(FAILOVER, 60):
            raise Inconclusive(f"{FAILOVER} was not elected within the timeout")
        print_state("during partition")

        # Heal before the verdict read: the VIOLATED case depends on the doomed
        # write having rolled back in the recovered durable state.
        finalize_experiment()
        healed = True

        final = None
        for node in (OLD_PRIMARY, FAILOVER):
            try:
                c = direct(node)
                doc = c[DB].get_collection("ryw", read_concern=ReadConcern("majority")).find_one(
                    {"k": key}, max_time_ms=4000
                )
                final = doc["v"] if doc else None
                c.close()
                break
            except PyMongoError:
                continue

        if acked and final != 1:
            verdict = "VIOLATED"
            detail = "write rolled back"
        elif not acked:
            verdict = "NOT_VIOLATED"
            detail = "write refused"
        elif final == 1:
            verdict = "NOT_VIOLATED"
            detail = "write survived"

    except Inconclusive as e:
        detail = str(e)
    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"
    finally:
        if not healed:
            finalize_experiment()

    print()
    print(f"=== RYOW / rollback ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def divergent_read(read_concern: str, config_label: str) -> str:
    """Divergent-read mechanism: local reads gate on clock, not data presence."""
    key = f"ryw-div-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        print(f"==> raising electionTimeoutMillis to {DIVERGENT_ELECTION_TIMEOUT_MS}", flush=True)
        set_election_timeout(DIVERGENT_ELECTION_TIMEOUT_MS)
        time.sleep(3)

        p = direct(OLD_PRIMARY)
        p[DB].get_collection("ryw", write_concern=WriteConcern(w="majority")).insert_one(
            {"k": key, "v": 0}
        )
        print(f"==> baseline: {key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        print(f"==> waiting {DIVERGENT_PRIMARY_WAIT}s for {FAILOVER} to become PRIMARY", flush=True)
        if not wait_primary(FAILOVER, DIVERGENT_PRIMARY_WAIT):
            raise Inconclusive(f"{FAILOVER} was not elected within the timeout")

        op_time = cluster_time = None
        w = direct(FAILOVER)
        try:
            with w.start_session(causal_consistency=True) as ws:
                w[DB].get_collection("ryw", write_concern=WriteConcern(w="majority", wtimeout=8000)).update_one(
                    {"k": key}, {"$set": {"v": 1}}, session=ws
                )
                op_time = ws.operation_time
                cluster_time = ws.cluster_time
                print(f"==> Write 1: X=1 w:majority ACKED on {FAILOVER}; T1={op_time}", flush=True)
        except PyMongoError as e:
            raise Inconclusive(f"Write 1 failed: {str(e)[:60]}")
        finally:
            w.close()

        if op_time is None:
            raise Inconclusive("Write 1 session has no causal operation time")

        time.sleep(1)
        # An unrelated actor's w:1 write to the minority advances mongo2's clock
        # past T1 without shipping our data there, exposing the divergent read.
        tick = uuid4().hex
        d = direct(OLD_PRIMARY, socket_ms=3000)
        try:
            d[DB].get_collection("dummy", write_concern=WriteConcern(w=1)).insert_one(
                {"tick": tick}
            )
            print(f"==> dummy w:1 to {OLD_PRIMARY} (advances minority clock past T1)", flush=True)
        except PyMongoError as e:
            print(f"==> dummy clock-advance write failed: {str(e)[:60]}", flush=True)
            raise Inconclusive("clock-advance write to old primary failed")
        finally:
            d.close()

        if not wait_for_tick(MINORITY_SECONDARY, tick, 12):
            raise Inconclusive("clock-advance write did not reach the minority secondary")
        print(f"==> clock-advance write replicated to {MINORITY_SECONDARY}", flush=True)

        r = direct(MINORITY_SECONDARY, socket_ms=12000)
        try:
            with r.start_session(causal_consistency=True) as rsess:
                if cluster_time is not None:
                    rsess.advance_cluster_time(cluster_time)
                if op_time is not None:
                    rsess.advance_operation_time(op_time)
                try:
                    doc = r[DB].get_collection("ryw", read_concern=ReadConcern(read_concern)).find_one(
                        {"k": key}, session=rsess, max_time_ms=6000
                    )
                    val = doc["v"] if doc else None
                    print(f"==> Read 1: {read_concern} read on {MINORITY_SECONDARY}: X={val}", flush=True)
                    if val == 1:
                        verdict = "NOT_VIOLATED"
                        detail = "read reflected the write"
                    else:
                        verdict = "VIOLATED"
                        detail = f"read returned stale X={val}"
                except ExecutionTimeout:
                    verdict = "NOT_VIOLATED"
                    detail = f"{read_concern} read timed out rather than returning stale data"
                except PyMongoError as e:
                    verdict = "INCONCLUSIVE"
                    detail = f"causal read errored [{type(e).__name__}]: {str(e)[:60]}"
        finally:
            r.close()

    except Inconclusive as e:
        detail = str(e)
    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    finally:
        print("==> healing partition", flush=True)
        finalize_experiment()

    print()
    print(f"=== RYOW / divergent-read ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    if config == "local/majority":
        return divergent_read("local", config)
    elif write_concern == "majority":
        return rollback("majority", config)
    else:
        return rollback(1, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Read-your-writes experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    ap.add_argument("--control", action="store_true", help="run divergent-read with majority (control)")
    args = ap.parse_args()
    if args.config == "local/majority" and args.control:
        result = divergent_read("majority", "local/majority (control)")
    else:
        result = run(args.config)
    if result == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
