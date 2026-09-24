"""Monotonic-reads (MR) across all four readConcern x writeConcern configs.

MR (Terry et al. 1994, "Session Guarantees for Weakly Consistent Replicated
Data"): once a session has read a value, later reads in that session must not
return an older state. MR is violated when a read returns a value that is later
contradicted by a newer read in the same causal session.

MongoDB uses two mechanisms to expose MR violations:

  SIMPLE ROLLBACK (writeConcern is the culprit) -- local/w:1 config:
    Write X=1 with w:1 to the isolated minority primary (doomed to rollback).
    Write Y=2 with w:majority to the majority side (survives).
    A causal read captures tokens from the Y=2 write, then reads on the minority
    secondary. If X regresses below 1 (or vanishes), MR is violated.

  DIVERGENT READ (readConcern is the culprit) -- local/majority config:
    Raise electionTimeoutMillis so the old primary stays writable. Partition;
    the majority side elects a new primary. Write X=1 w:majority to the new
    primary (capture T1). Advance the minority clock past T1 via unrelated w:1
    write. A causal `local` read on the minority secondary carrying T1 does NOT
    block (clock reached T1) but never received X=1 -> STALE -> VIOLATED.
    A `majority` read waits for majority commit point (minority can't advance
    it) -> UNAVAILABLE (holds).

Configs and expected verdicts:
    majority/majority -> SAFE        (majority writes do not roll back)
    majority/w:1      -> SAFE        (w:1 rollback doesn't affect MR if write concern is majority)
    local/w:1         -> SAFE        (w:1 write rolls back, but session tokens ensure consistency)
    local/majority    -> VIOLATED    (divergent read)

Run via ../run.sh monotonic-reads <config>, or directly:
    uv run models/monotonic_reads.py --config majority/majority
    uv run models/monotonic_reads.py --config majority/w:1
    uv run models/monotonic_reads.py --config local/w:1
    uv run models/monotonic_reads.py --config local/majority
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
    direct, heal, partition_minority, print_state, set_election_timeout, wait_primary,
)

# Simple rollback timing: w:1 write on minority primary before it steps down.
SIMPLE_STEPDOWN_WAIT = 15
SIMPLE_HEAL_WAIT = 12

# Divergent-read timing: at 120000, old primary stays writable while new primary
# is elected ~140-145s after partition -> wide, reliable window.
DIVERGENT_ELECTION_TIMEOUT_MS = 120000
DIVERGENT_PRIMARY_WAIT = 175
DIVERGENT_HEAL_WAIT = 10


# --------------------------------------------------------------------------- #
# Mechanism 1: simple rollback (writeConcern) -> local/w:1 config.            #
# --------------------------------------------------------------------------- #
def simple_rollback(write_concern, config_label: str) -> None:
    """Write X to minority (w:1, doomed), Y to majority (w:majority, survives).
    Read both on minority carrying causal tokens from Y write. If X regresses, MR violated.
    """
    x_key = f"mr-x-{int(time.time())}"
    y_key = f"mr-y-{int(time.time())}"

    # Baseline X=0, Y=0 durable.
    p = direct(OLD_PRIMARY)
    coll = p[DB].get_collection("mr", write_concern=WriteConcern(w="majority"))
    coll.insert_one({"k": x_key, "v": 0})
    coll.insert_one({"k": y_key, "v": 0})
    print(f"==> baseline written: {x_key}=0, {y_key}=0 (durable)", flush=True)
    p.close()

    print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
    partition_minority()

    # X=1 write on isolated minority (w:1, doomed to rollback).
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
                print(f"==> X=1 w:{write_concern} REFUSED on {OLD_PRIMARY}: {str(e)[:70]}", flush=True)
    finally:
        p.close()

    print(f"==> waiting {SIMPLE_STEPDOWN_WAIT}s for the majority side to elect {FAILOVER}", flush=True)
    time.sleep(SIMPLE_STEPDOWN_WAIT)
    print_state("during partition")

    # Y=2 write on majority primary (w:majority, survives). Capture causal tokens.
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
                print(f"==> Y=2 w:majority REFUSED on {FAILOVER}: {str(e)[:70]}", flush=True)
    finally:
        w.close()

    print("==> healing partition", flush=True)
    heal()
    time.sleep(SIMPLE_HEAL_WAIT)
    print_state("after heal")

    # Final read of X on minority secondary carrying causal tokens from Y write.
    # If X regressed below 1, MR is violated.
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

    print()
    print("=== Monotonic-reads / simple rollback ===")
    print(f"  config:          {config_label}")
    print(f"  X=1 acknowledged: {x_acked}")
    print(f"  Y=2 acknowledged: {y_acked}")
    print(f"  survived heal:    X={x_final}  Y={y_final}")
    if y_acked and y_final == 2:
        if x_acked and x_final != 1:
            print("  verdict:         VIOLATED (Y survived, but X regressed)")
        else:
            print("  verdict:         HELD (both writes or both rolled back)")
    else:
        print("  verdict:         INCONCLUSIVE (Y did not survive)")
    print()


# --------------------------------------------------------------------------- #
# Mechanism 2: divergent read (readConcern) -> local/majority config.        #
# --------------------------------------------------------------------------- #
def divergent_read(read_concern: str) -> None:
    """Local read gates on clock, not data. If clock advances past the causal
    token without the data being replicated, the read returns stale data.
    """
    x_key = f"mr-divergent-{int(time.time())}"
    verdict = detail = None
    dummy_ok = False

    try:
        print(f"==> raising electionTimeoutMillis to {DIVERGENT_ELECTION_TIMEOUT_MS} "
              f"(keeps {OLD_PRIMARY} writable on the minority side)", flush=True)
        set_election_timeout(DIVERGENT_ELECTION_TIMEOUT_MS)
        time.sleep(3)

        p = direct(OLD_PRIMARY)
        coll = p[DB].get_collection("mr", write_concern=WriteConcern(w="majority"))
        coll.insert_one({"k": x_key, "v": 0})
        print(f"==> baseline written: {x_key}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
        partition_minority()

        print(f"==> waiting up to {DIVERGENT_PRIMARY_WAIT}s for {FAILOVER} to become primary", flush=True)
        if not wait_primary(FAILOVER, DIVERGENT_PRIMARY_WAIT):
            print(f"==> {FAILOVER} not elected in time; aborting", flush=True)
            print("  verdict: INCONCLUSIVE (no new primary)")
            return

        # Write 1: X=1 w:majority on majority side. Capture causal token T1.
        op_time = cluster_time = None
        w = direct(FAILOVER)
        try:
            with w.start_session(causal_consistency=True) as ws:
                coll = w[DB].get_collection("mr", write_concern=WriteConcern(w="majority", wtimeout=8000))
                coll.update_one({"k": x_key}, {"$set": {"v": 1}}, session=ws)
                op_time = ws.operation_time
                cluster_time = ws.cluster_time
                print(f"==> Write 1: X=1 w:majority ACKED on {FAILOVER}; T1={op_time}", flush=True)
        except PyMongoError as e:
            print(f"==> Write 1 failed: {str(e)[:70]}; aborting", flush=True)
            print("  verdict: INCONCLUSIVE (majority write did not ack)")
            return
        finally:
            w.close()

        # Dummy w:1 to minority after Write 1: advances minority clusterTime past T1
        # and replicates to minority secondary. Lets the causal local read later skip
        # its afterClusterTime=T1 gate WITHOUT the secondary ever receiving Write 1.
        time.sleep(1)
        d = direct(OLD_PRIMARY, socket_ms=3000)
        try:
            d[DB].get_collection("dummy", write_concern=WriteConcern(w=1)).insert_one(
                {"tick": int(time.time())}
            )
            dummy_ok = True
            print(f"==> dummy w:1 to {OLD_PRIMARY} OK (advances minority clock past T1)", flush=True)
        except PyMongoError as e:
            print(f"==> dummy write failed: {str(e)[:60]}", flush=True)
        finally:
            d.close()

        time.sleep(2)

        # Read 1: causal read of X on minority secondary carrying T1.
        r = direct(MINORITY_SECONDARY, socket_ms=12000)
        try:
            with r.start_session(causal_consistency=True) as rsess:
                if cluster_time is not None:
                    rsess.advance_cluster_time(cluster_time)
                if op_time is not None:
                    rsess.advance_operation_time(op_time)
                try:
                    coll = r[DB].get_collection("mr", read_concern=ReadConcern(read_concern))
                    doc = coll.find_one({"k": x_key}, session=rsess, max_time_ms=6000)
                    val = doc["v"] if doc else None
                    print(f"==> Read 1: {read_concern} read on {MINORITY_SECONDARY}: X={val}", flush=True)
                    if val == 1:
                        verdict, detail = "HELD", "read reflected the write"
                    else:
                        verdict, detail = "VIOLATED", (
                            f"read returned stale X={val}; clock advanced past T1 but "
                            f"{MINORITY_SECONDARY} never received Write 1"
                        )
                except ExecutionTimeout:
                    verdict, detail = "UNAVAILABLE", (
                        f"{read_concern} read blocked: minority cannot advance majority commit point"
                    )
                except PyMongoError as e:
                    verdict, detail = "UNAVAILABLE", f"{read_concern} read blocked [{type(e).__name__}]"
        finally:
            r.close()
    finally:
        print("==> healing partition + restoring electionTimeoutMillis=10000", flush=True)
        heal()
        time.sleep(DIVERGENT_HEAL_WAIT)
        set_election_timeout(10000)

    print()
    print("=== Monotonic-reads / divergent read ===")
    print(f"  config:      local/majority")
    print(f"  dummy write: {'ok' if dummy_ok else 'FAILED'}")
    print(f"  verdict:     {verdict} ({detail})")
    print()


# --------------------------------------------------------------------------- #
# Dispatch: config -> mechanism.                                              #
# --------------------------------------------------------------------------- #
def run(config: str) -> None:
    if config == "majority/majority":
        simple_rollback("majority", config)
    elif config == "majority/w:1":
        simple_rollback(1, config)
    elif config == "local/w:1":
        simple_rollback(1, config)
    elif config == "local/majority":
        divergent_read("local")
    else:
        raise SystemExit(f"unknown config: {config}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-reads consistency experiment")
    ap.add_argument(
        "--config", required=True,
        choices=["majority/majority", "majority/w:1", "local/w:1", "local/majority"],
        help="readConcern/writeConcern combination to test",
    )
    ap.add_argument(
        "--control", action="store_true",
        help="for local/majority: run majority-read control (expect UNAVAILABLE)",
    )
    args = ap.parse_args()
    if args.config == "local/majority" and args.control:
        divergent_read("majority")
    else:
        run(args.config)


if __name__ == "__main__":
    main()
