"""Monotonic-writes (MW) across all four readConcern x writeConcern configs.

MW: writes issued in order within a session are applied in order everywhere.

Mechanism: rollback. W1 (w:1) acks on the isolated minority; W2 is issued on the
failover primary in a session that carries W1's transferred causal timestamps.
On heal, W1 rolls back while W2 survives -- W2 without W1 violates MW. w:majority
writes are refused on minority (never acked), so no ordering can form.

Expected verdicts:
  majority/majority -> NOT_VIOLATED  (W1 refused on minority)
  majority/w:1      -> VIOLATED      (W2 visible, W1 rolled back)
  local/w:1         -> VIOLATED      (W2 visible, W1 rolled back)
  local/majority    -> NOT_VIOLATED  (W1 refused on minority)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from uuid import uuid4

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


class Inconclusive(Exception):
    pass


def monotonic_writes(read_concern: str, write_concern, config_label: str) -> str:
    """Rollback mechanism: W1 (w:1, doomed) on minority, W2 on majority. Check ordering."""
    k1 = f"mw-k1-{uuid4().hex}"
    k2 = f"mw-k2-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"
    healed = False

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
        op_time = cluster_time = None
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
                    op_time = s.operation_time
                    cluster_time = s.cluster_time
                    print(f"==> W1: {k1}=1 w:{write_concern} ACKED on {OLD_PRIMARY} (doomed); T1={op_time}", flush=True)
                except PyMongoError as e:
                    print(f"==> W1 w:{write_concern} REFUSED: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        elected = wait_primary(FAILOVER, FAILOVER_WAIT)
        print_state("during partition")

        w2_acked = False
        if w1_acked and not elected:
            raise Inconclusive(f"{FAILOVER} was not elected; cannot issue ordered W2")
        if elected:
            if w1_acked and op_time is None:
                raise Inconclusive("W1 session has no causal operation time")
            w = direct(FAILOVER, socket_ms=8000)
            try:
                # W2 is issued on the winning primary after transferring W1's
                # causal timestamps into this session.
                with w.start_session(causal_consistency=True) as s2:
                    if cluster_time is not None:
                        s2.advance_cluster_time(cluster_time)
                    if op_time is not None:
                        s2.advance_operation_time(op_time)
                    coll = w[DB].get_collection("mw", write_concern=WriteConcern(w=write_concern, wtimeout=8000))
                    coll.update_one({"k": k2}, {"$set": {"v": 1}}, session=s2)
                    w2_acked = True
                    print(f"==> W2: {k2}=1 w:{write_concern} ACKED on {FAILOVER} after W1 (survives)", flush=True)
            except PyMongoError as e:
                print(f"==> W2 failed: {str(e)[:60]}", flush=True)
            finally:
                w.close()

        # Heal before the verdict reads: the VIOLATED case depends on W1 having
        # rolled back in the recovered durable state.
        finalize_experiment()
        healed = True

        final1 = final2 = None
        for node in (FAILOVER, OLD_PRIMARY):
            try:
                c = direct(node)
                # Diagnostic read is fixed at majority regardless of the config
                # under test: we verify the recovered durable state, not the
                # visibility semantics of the read concern being exercised.
                coll = c[DB].get_collection("mw", read_concern=ReadConcern("majority"))
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
            verdict = "NOT_VIOLATED"
            detail = "W1 refused"
        elif final1 == 1 and final2 == 1:
            verdict = "NOT_VIOLATED"
            detail = "both survived"

    except Inconclusive as e:
        detail = str(e)
    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"
    finally:
        if not healed:
            finalize_experiment()

    print()
    print(f"=== MW / rollback ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    return monotonic_writes(read_concern, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monotonic-writes experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    args = ap.parse_args()
    if run(args.config) == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
