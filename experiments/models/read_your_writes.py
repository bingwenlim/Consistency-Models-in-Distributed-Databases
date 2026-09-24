"""Read-your-writes (RYOW) across all four readConcern x writeConcern configs.

RYOW: after a client writes in a causally consistent session, a later read in
that session must return that write or newer -- never older.

Two mechanisms expose RYOW violations:
  - Rollback: w:1 writes ack on isolated minority, then roll back on heal
  - Divergent-read: local reads gate on clock, not data presence

Expected verdicts:
  majority/majority -> SAFE      (write refused on minority)
  majority/w:1      -> VIOLATED  (write rolls back)
  local/w:1         -> VIOLATED  (write rolls back)
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

ROLLBACK_STEPDOWN_WAIT = 15
DIVERGENT_ELECTION_TIMEOUT_MS = 120000
DIVERGENT_PRIMARY_WAIT = 175


def rollback(write_concern, config_label: str) -> str:
    """Rollback mechanism: w:1 write acks on isolated minority, rolls back on heal."""
    wc_label = "majority" if write_concern == "majority" else f"w:{write_concern}"
    key = f"ryw-{wc_label}-{int(time.time())}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        p = direct(OLD_PRIMARY)
        p[DB].get_collection("ryw", write_concern=WriteConcern(w="majority")).insert_one(
            {"k": key, "v": 0}
        )
        print(f"==> baseline: {key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        acked = False
        session_saw_write = None
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
                if acked:
                    doc = p[DB].get_collection("ryw", read_concern=ReadConcern("local")).find_one(
                        {"k": key}, session=s
                    )
                    session_saw_write = doc["v"] if doc else None
                    print(f"==> session read-back: X={session_saw_write}", flush=True)
        finally:
            p.close()

        print(f"==> waiting {ROLLBACK_STEPDOWN_WAIT}s for majority to elect {FAILOVER}", flush=True)
        time.sleep(ROLLBACK_STEPDOWN_WAIT)
        print_state("during partition")

        finalize_experiment()

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

        if acked and session_saw_write == 1 and final != 1:
            verdict = "VIOLATED"
            detail = "write rolled back"
        elif not acked:
            verdict = "SAFE"
            detail = "write refused"
        elif final == 1:
            verdict = "HELD"
            detail = "write survived"

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    print()
    print(f"=== RYOW / rollback ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def divergent_read(read_concern: str) -> str:
    """Divergent-read mechanism: local reads gate on clock, not data presence."""
    key = f"ryw-div-{int(time.time())}"
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
            print(f"==> {FAILOVER} not elected; aborting", flush=True)
            return "INCONCLUSIVE"

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
            print(f"==> Write 1 failed: {str(e)[:60]}", flush=True)
            return "INCONCLUSIVE"
        finally:
            w.close()

        time.sleep(1)
        d = direct(OLD_PRIMARY, socket_ms=3000)
        try:
            d[DB].get_collection("dummy", write_concern=WriteConcern(w=1)).insert_one(
                {"tick": int(time.time())}
            )
            print(f"==> dummy w:1 to {OLD_PRIMARY} (advances minority clock past T1)", flush=True)
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
                    doc = r[DB].get_collection("ryw", read_concern=ReadConcern(read_concern)).find_one(
                        {"k": key}, session=rsess, max_time_ms=6000
                    )
                    val = doc["v"] if doc else None
                    print(f"==> Read 1: {read_concern} read on {MINORITY_SECONDARY}: X={val}", flush=True)
                    if val == 1:
                        verdict = "HELD"
                        detail = "read reflected the write"
                    else:
                        verdict = "VIOLATED"
                        detail = f"read returned stale X={val}"
                except ExecutionTimeout:
                    verdict = "UNAVAILABLE"
                    detail = f"{read_concern} read blocked on minority"
                except PyMongoError as e:
                    verdict = "UNAVAILABLE"
                    detail = f"{read_concern} read blocked [{type(e).__name__}]"
        finally:
            r.close()

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    finally:
        print("==> healing partition", flush=True)
        finalize_experiment()

    print()
    print(f"=== RYOW / divergent-read ===")
    print(f"  config:   local/{read_concern}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    if write_concern == "majority":
        return rollback("majority", config)
    elif config == "local/majority":
        return divergent_read("local")
    else:
        return rollback(1, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Read-your-writes experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    ap.add_argument("--control", action="store_true", help="run divergent-read with majority (control)")
    args = ap.parse_args()
    if args.config == "local/majority" and args.control:
        divergent_read("majority")
    else:
        run(args.config)


if __name__ == "__main__":
    main()
