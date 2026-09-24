"""Monotonic-reads (MR) across all four readConcern x writeConcern configs.

MR: once a session has read a value, later reads must not return older state.

Two mechanisms expose MR violations:
  - Simple rollback: w:1 write (X) acks on minority, rolls back; w:majority write (Y) survives
  - Divergent-read: local reads gate on clock, not data presence

Expected verdicts:
  majority/majority -> HELD      (both writes refused or both survive)
  majority/w:1      -> VIOLATED  (X regresses, Y survives)
  local/w:1         -> VIOLATED  (X regresses, Y survives)
  local/majority    -> VIOLATED  (divergent read returns stale data)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

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

SIMPLE_STEPDOWN_WAIT = 40
DIVERGENT_ELECTION_TIMEOUT_MS = 120000
DIVERGENT_PRIMARY_WAIT = 175


def simple_rollback(write_concern, config_label: str) -> str:
    """Simple rollback: X (w:1, doomed) + Y (w:majority, survives). Check X regression."""
    x_key = f"mr-x-{int(time.time())}"
    y_key = f"mr-y-{int(time.time())}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        p = direct(OLD_PRIMARY)
        coll = p[DB].get_collection("mr", write_concern=WriteConcern(w="majority"))
        coll.insert_one({"k": x_key, "v": 0})
        coll.insert_one({"k": y_key, "v": 0})
        print(f"==> baseline: {x_key}=0, {y_key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        x_acked = False
        p = MongoClient(
            f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
            serverSelectionTimeoutMS=2000, socketTimeoutMS=3000,
        )
        try:
            with p.start_session(causal_consistency=True) as s:
                coll = p[DB].get_collection("mr", write_concern=WriteConcern(w=write_concern, wtimeout=3000))
                try:
                    coll.update_one({"k": x_key}, {"$set": {"v": 1}}, session=s)
                    x_acked = True
                    print(f"==> X=1 w:{write_concern} ACKED on {OLD_PRIMARY} (doomed)", flush=True)
                except PyMongoError as e:
                    print(f"==> X=1 w:{write_concern} REFUSED: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        print(f"==> waiting up to {SIMPLE_STEPDOWN_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        if not wait_primary(FAILOVER, SIMPLE_STEPDOWN_WAIT):
            print(f"==> {FAILOVER} not elected", flush=True)
        print_state("during partition")

        y_acked = False
        op_time = cluster_time = None
        w = direct(FAILOVER)
        try:
            with w.start_session(causal_consistency=True) as ws:
                coll = w[DB].get_collection("mr", write_concern=WriteConcern(w="majority", wtimeout=8000))
                try:
                    coll.update_one({"k": y_key}, {"$set": {"v": 2}}, session=ws)
                    y_acked = True
                    op_time = ws.operation_time
                    cluster_time = ws.cluster_time
                    print(f"==> Y=2 w:majority ACKED on {FAILOVER} (survives)", flush=True)
                except PyMongoError as e:
                    print(f"==> Y=2 w:majority REFUSED: {str(e)[:60]}", flush=True)
        finally:
            w.close()

        finalize_experiment()

        x_final = y_final = None
        for node in (FAILOVER, OLD_PRIMARY):
            try:
                c = direct(node)
                with c.start_session(causal_consistency=True) as rs:
                    if op_time is not None:
                        rs.advance_operation_time(op_time)
                    if cluster_time is not None:
                        rs.advance_cluster_time(cluster_time)
                    coll = c[DB].get_collection("mr", read_concern=ReadConcern("local"))
                    doc_x = coll.find_one({"k": x_key}, session=rs, max_time_ms=4000)
                    doc_y = coll.find_one({"k": y_key}, session=rs, max_time_ms=4000)
                    x_final = doc_x["v"] if doc_x else None
                    y_final = doc_y["v"] if doc_y else None
                c.close()
                break
            except PyMongoError:
                continue

        if y_acked and y_final == 2:
            if x_acked and x_final != 1:
                verdict = "VIOLATED"
                detail = "Y survived but X regressed"
            else:
                verdict = "HELD"
                detail = "consistent state"
        else:
            detail = "Y did not survive"

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    print()
    print(f"=== MR / simple-rollback ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def divergent_read(read_concern: str) -> str:
    """Divergent-read: local reads gate on clock, not data presence."""
    key = f"mr-div-{int(time.time())}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        print(f"==> raising electionTimeoutMillis to {DIVERGENT_ELECTION_TIMEOUT_MS}", flush=True)
        set_election_timeout(DIVERGENT_ELECTION_TIMEOUT_MS)
        time.sleep(3)

        p = direct(OLD_PRIMARY)
        p[DB].get_collection("mr", write_concern=WriteConcern(w="majority")).insert_one({"k": key, "v": 0})
        print(f"==> baseline: {key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        print(f"==> waiting {DIVERGENT_PRIMARY_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        if not wait_primary(FAILOVER, DIVERGENT_PRIMARY_WAIT):
            return "INCONCLUSIVE"

        op_time = cluster_time = None
        w = direct(FAILOVER)
        try:
            with w.start_session(causal_consistency=True) as ws:
                w[DB].get_collection("mr", write_concern=WriteConcern(w="majority", wtimeout=8000)).update_one(
                    {"k": key}, {"$set": {"v": 1}}, session=ws
                )
                op_time = ws.operation_time
                cluster_time = ws.cluster_time
                print(f"==> Write 1: X=1 w:majority ACKED on {FAILOVER}; T1={op_time}", flush=True)
        except PyMongoError as e:
            print(f"==> Write 1 failed: {str(e)[:60]}", flush=True)
            return "INCONCLUSIVE"
        finally:
            w.close()

        time.sleep(1)
        d = direct(OLD_PRIMARY, socket_ms=3000)
        try:
            d[DB].get_collection("dummy", write_concern=WriteConcern(w=1)).insert_one({"tick": int(time.time())})
            print(f"==> dummy w:1 (advances minority clock past T1)", flush=True)
        except PyMongoError:
            pass
        finally:
            d.close()

        time.sleep(2)

        r = direct(MINORITY_SECONDARY, socket_ms=12000)
        try:
            with r.start_session(causal_consistency=True) as rsess:
                if cluster_time is not None:
                    rsess.advance_cluster_time(cluster_time)
                if op_time is not None:
                    rsess.advance_operation_time(op_time)
                try:
                    doc = r[DB].get_collection("mr", read_concern=ReadConcern(read_concern)).find_one(
                        {"k": key}, session=rsess, max_time_ms=6000
                    )
                    val = doc["v"] if doc else None
                    print(f"==> Read 1: {read_concern} read: X={val}", flush=True)
                    verdict = "HELD" if val == 1 else "VIOLATED"
                    detail = "read reflected write" if val == 1 else f"read returned stale X={val}"
                except ExecutionTimeout:
                    verdict = "UNAVAILABLE"
                    detail = "read blocked on minority"
                except PyMongoError as e:
                    verdict = "UNAVAILABLE"
                    detail = f"read blocked [{type(e).__name__}]"
        finally:
            r.close()

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    finally:
        finalize_experiment()

    print()
    print(f"=== MR / divergent-read ===")
    print(f"  config:   local/{read_concern}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    if config == "local/majority":
        return divergent_read("local")
    else:
        return simple_rollback(write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-reads experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    ap.add_argument("--control", action="store_true", help="run divergent-read with majority (control)")
    args = ap.parse_args()
    if args.config == "local/majority" and args.control:
        divergent_read("majority")
    else:
        run(args.config)


if __name__ == "__main__":
    main()
