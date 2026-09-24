"""Monotonic-writes (MW) across all four readConcern x writeConcern configs.

MW: writes issued in order within a session are applied in order everywhere.

Mechanism: rollback. w:1 writes ack on isolated minority, roll back on heal.
w:majority writes are refused on minority (never acked).

Expected verdicts:
  majority/majority -> SAFE      (W1 refused on minority)
  majority/w:1      -> VIOLATED  (W2 visible, W1 rolled back)
  local/w:1         -> VIOLATED  (W2 visible, W1 rolled back)
  local/majority    -> SAFE      (W1 refused on minority)
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
    direct, partition_minority, print_state, wait_primary,
)
from helpers import finalize_experiment

CONFIGS = {
    "majority/majority": ("majority", "majority"),
    "majority/w:1": ("majority", 1),
    "local/w:1": ("local", 1),
    "local/majority": ("local", "majority"),
}

FAILOVER_WAIT = 40


def monotonic_writes(read_concern: str, write_concern, config_label: str) -> str:
    """Rollback mechanism: W1 (w:1, doomed) on minority, W2 on majority. Check ordering."""
    k1 = f"mw-k1-{int(time.time())}"
    k2 = f"mw-k2-{int(time.time())}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        p = direct(OLD_PRIMARY)
        base = p[DB].get_collection("mw", write_concern=WriteConcern(w="majority"))
        base.insert_one({"k": k1, "v": 0})
        base.insert_one({"k": k2, "v": 0})
        print(f"==> baseline: {k1}=0, {k2}=0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY}", flush=True)
        partition_minority()

        w1_acked = False
        p = MongoClient(
            f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
            serverSelectionTimeoutMS=2000, socketTimeoutMS=3000,
        )
        try:
            with p.start_session(causal_consistency=True) as s:
                coll = p[DB].get_collection("mw", write_concern=WriteConcern(w=write_concern, wtimeout=3000))
                try:
                    coll.update_one({"k": k1}, {"$set": {"v": 1}}, session=s)
                    w1_acked = True
                    print(f"==> W1: {k1}=1 w:{write_concern} ACKED on {OLD_PRIMARY} (doomed)", flush=True)
                except PyMongoError as e:
                    print(f"==> W1 w:{write_concern} REFUSED: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        elected = wait_primary(FAILOVER, FAILOVER_WAIT)
        print_state("during partition")

        w2_acked = False
        if elected:
            w = direct(FAILOVER, socket_ms=8000)
            try:
                coll = w[DB].get_collection("mw", write_concern=WriteConcern(w=write_concern, wtimeout=8000))
                coll.update_one({"k": k2}, {"$set": {"v": 1}})
                w2_acked = True
                print(f"==> W2: {k2}=1 w:{write_concern} ACKED on {FAILOVER} (survives)", flush=True)
            except PyMongoError as e:
                print(f"==> W2 failed: {str(e)[:60]}", flush=True)
            finally:
                w.close()

        finalize_experiment()

        final1 = final2 = None
        for node in (FAILOVER, OLD_PRIMARY):
            try:
                c = direct(node)
                coll = c[DB].get_collection("mw", read_concern=ReadConcern(read_concern))
                d1 = coll.find_one({"k": k1}, max_time_ms=4000)
                d2 = coll.find_one({"k": k2}, max_time_ms=4000)
                final1 = d1["v"] if d1 else None
                final2 = d2["v"] if d2 else None
                c.close()
                break
            except PyMongoError:
                continue

        if w1_acked and w2_acked and final2 == 1 and final1 != 1:
            verdict = "VIOLATED"
            detail = "W2 visible, W1 rolled back"
        elif not w1_acked:
            verdict = "SAFE"
            detail = "W1 refused"
        elif final1 == 1 and final2 == 1:
            verdict = "HELD"
            detail = "both survived"

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    print()
    print(f"=== MW / rollback ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    rc, wc = config.split("/")
    write_concern = "majority" if wc == "majority" else 1
    return monotonic_writes(rc, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-writes experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    args = ap.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
