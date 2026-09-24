"""Monotonic-writes (MW) across all four readConcern x writeConcern configs.

MW (Terry et al. 1994, "Session Guarantees for Weakly Consistent Replicated
Data"): if a session writes W1 and later writes W2, then every server that holds
W2 must also hold W1. Writes from one session are applied everywhere in the order
they were issued -- a later write is never visible while an earlier one is missing.

MongoDB has a single primary and one totally ordered oplog, so two writes can
never be *reordered*. The only way to expose W2 without W1 is to make W1 ROLL
BACK while W2 survives. That makes MW a writeConcern story, exactly like the
read-your-writes rollback: whether an acknowledged write is durable.

  MECHANISM (rollback; the writeConcern is the culprit):
    Partition mongo1+mongo2 (minority, mongo1 = old primary) from mongo3+4+5.
    W1 (k1=1) is written w:1 on the isolated mongo1 -> acknowledged, but doomed.
    The majority side elects mongo3; W2 (k2=1) is written on mongo3 -> durable.
    Heal: mongo1's un-replicated history (W1) is discarded, W2 survives.
    A reader on the surviving side now sees W2 but not W1 -> MW VIOLATED.

    With w:majority, W1 on the minority side can never reach a majority, so it is
    REFUSED -- never acknowledged, nothing to lose -> MW SAFE.

This matches MongoDB's causal-consistency guarantee table, where MW holds for the
two writeConcern:majority rows and is absent for the two w:1 rows -- readConcern
is irrelevant to it.

    majority/majority -> SAFE      (W1 refused on the minority side)
    local/majority    -> SAFE      (W1 refused; readConcern does not matter)
    majority/w:1      -> VIOLATED  (W1 rolled back, W2 survives)
    local/w:1         -> VIOLATED  (same rollback)

Run via ../run.sh monotonic-writes <config>, or directly:
    uv run models/monotonic_writes.py --config majority/w:1
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib import (  # noqa: E402
    DB, NODES, OLD_PRIMARY, FAILOVER, MINORITY,
    direct, heal, partition_minority, print_state, wait_primary,
)

FAILOVER_WAIT = 40
HEAL_WAIT = 12


def monotonic_writes(read_concern: str, write_concern, config_label: str) -> None:
    wc_label = "majority" if write_concern == "majority" else f"w:{write_concern}"
    key1 = f"mw-k1-{int(time.time())}"
    key2 = f"mw-k2-{int(time.time())}"

    # Baseline: k1=0, k2=0 durable, so there is a committed prior state.
    p = direct(OLD_PRIMARY)
    base = p[DB].get_collection("mw", write_concern=WriteConcern(w="majority"))
    base.insert_one({"k": key1, "v": 0})
    base.insert_one({"k": key2, "v": 0})
    print(f"==> baseline written: {key1}=0, {key2}=0 (durable)", flush=True)
    p.close()

    print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
    partition_minority()

    # W1: first write of the session, to the doomed minority primary.
    w1_acked = False
    p = MongoClient(
        f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
        serverSelectionTimeoutMS=2000, socketTimeoutMS=3000,
    )
    try:
        with p.start_session(causal_consistency=True) as s:
            coll = p[DB].get_collection(
                "mw", write_concern=WriteConcern(w=write_concern, wtimeout=3000)
            )
            try:
                coll.update_one({"k": key1}, {"$set": {"v": 1}}, session=s)
                w1_acked = True
                print(f"==> W1: {key1}=1 {wc_label} ACKED on {OLD_PRIMARY} (doomed)", flush=True)
            except PyMongoError as e:
                print(f"==> W1: {key1}=1 {wc_label} REFUSED on {OLD_PRIMARY}: {str(e)[:70]}", flush=True)
    finally:
        p.close()

    # Barrier: wait for the majority side to elect mongo3 before issuing W2.
    print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} to become primary", flush=True)
    elected = wait_primary(FAILOVER, FAILOVER_WAIT)
    print_state("during partition")

    # W2: second write of the session, to the surviving majority primary.
    w2_acked = False
    if elected:
        w = direct(FAILOVER, socket_ms=8000)
        try:
            coll = w[DB].get_collection(
                "mw", write_concern=WriteConcern(w=write_concern, wtimeout=8000)
            )
            coll.update_one({"k": key2}, {"$set": {"v": 1}})
            w2_acked = True
            print(f"==> W2: {key2}=1 {wc_label} ACKED on {FAILOVER} (survives)", flush=True)
        except PyMongoError as e:
            print(f"==> W2 failed on {FAILOVER}: {str(e)[:70]}", flush=True)
        finally:
            w.close()
    else:
        print(f"==> {FAILOVER} was not elected in time; cannot issue W2", flush=True)

    print("==> healing partition", flush=True)
    heal()
    time.sleep(HEAL_WAIT)
    print_state("after heal")

    # Final read of both keys from the surviving history.
    final1 = final2 = None
    for node in (FAILOVER, OLD_PRIMARY):
        try:
            c = direct(node)
            coll = c[DB].get_collection("mw", read_concern=ReadConcern(read_concern))
            d1 = coll.find_one({"k": key1}, max_time_ms=4000)
            d2 = coll.find_one({"k": key2}, max_time_ms=4000)
            final1 = d1["v"] if d1 else None
            final2 = d2["v"] if d2 else None
            c.close()
            break
        except PyMongoError:
            continue

    print()
    print("=== Monotonic-writes / rollback ===")
    print(f"  config:              {config_label}")
    print(f"  W1 acknowledged:     {w1_acked}   (k1)")
    print(f"  W2 acknowledged:     {w2_acked}   (k2)")
    print(f"  survived heal:       k1={final1}  k2={final2}")
    if w1_acked and w2_acked and final2 == 1 and final1 != 1:
        print("  verdict:             VIOLATED (W2 visible, W1 rolled back)")
    elif not w1_acked:
        print("  verdict:             SAFE (W1 refused — never falsely acknowledged)")
    elif final1 == 1 and final2 == 1:
        print("  verdict:             HELD (both writes survived, order intact)")
    else:
        print("  verdict:             INCONCLUSIVE (check the failover window)")
    print()


def run(config: str) -> None:
    rc, wc = config.split("/")
    write_concern = "majority" if wc == "majority" else 1
    monotonic_writes(rc, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-writes consistency experiment")
    ap.add_argument(
        "--config", required=True,
        choices=["majority/majority", "majority/w:1", "local/w:1", "local/majority"],
        help="readConcern/writeConcern combination to test",
    )
    args = ap.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
