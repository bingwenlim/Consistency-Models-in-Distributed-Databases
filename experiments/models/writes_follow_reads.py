"""Writes-follow-reads (WFR) across all four readConcern x writeConcern configs.

WFR: a write issued after reading a value is ordered after that value.
If a session reads X and writes Y, every server holding Y must also hold X.

Mechanism: doomed-read. Session reads a value doomed to rollback, then writes based on it.

Expected verdicts:
  majority/majority -> SAFE      (read blocked, no dependent write)
  majority/w:1      -> SAFE      (read blocked, no dependent write)
  local/w:1         -> VIOLATED  (read doomed value, write survives rollback)
  local/majority    -> VIOLATED  (read doomed value, write survives rollback)
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
READ_TIMEOUT_MS = 5000


def writes_follow_reads(read_concern: str, write_concern, config_label: str) -> str:
    """Doomed-read mechanism: session reads doomed value, issues dependent write."""
    k1 = f"wfr-k1-{int(time.time())}"
    k2 = f"wfr-k2-{int(time.time())}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"

    try:
        p = direct(OLD_PRIMARY)
        base = p[DB].get_collection("wfr", write_concern=WriteConcern(w="majority"))
        base.insert_one({"k": k1, "v": 0})
        base.insert_one({"k": k2, "v": 0, "saw": None})
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
            coll = p[DB].get_collection("wfr", write_concern=WriteConcern(w=1, wtimeout=3000))
            coll.update_one({"k": k1}, {"$set": {"v": 1}})
            w1_acked = True
            print(f"==> W1: {k1}=1 w:1 ACKED on {OLD_PRIMARY} (doomed)", flush=True)
        except PyMongoError as e:
            print(f"==> W1 failed: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        read_value = None
        read_blocked = False
        r = MongoClient(
            f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
            serverSelectionTimeoutMS=2000, socketTimeoutMS=READ_TIMEOUT_MS + 2000,
        )
        try:
            with r.start_session(causal_consistency=True) as s:
                coll = r[DB].get_collection("wfr", read_concern=ReadConcern(read_concern))
                try:
                    doc = coll.find_one({"k": k1}, session=s, max_time_ms=READ_TIMEOUT_MS)
                    read_value = doc["v"] if doc else None
                    print(f"==> READ {k1}: {read_concern} read: {k1}={read_value}", flush=True)
                except (ExecutionTimeout, PyMongoError) as e:
                    read_blocked = True
                    print(f"==> READ {k1}: {read_concern} BLOCKED [{type(e).__name__}]", flush=True)
        finally:
            r.close()

        print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        elected = wait_primary(FAILOVER, FAILOVER_WAIT)
        print_state("during partition")

        w2_acked = False
        dependent = read_value == 1
        if dependent and elected:
            w = direct(FAILOVER, socket_ms=8000)
            try:
                coll = w[DB].get_collection("wfr", write_concern=WriteConcern(w=write_concern, wtimeout=8000))
                coll.update_one({"k": k2}, {"$set": {"v": 1, "saw": read_value}})
                w2_acked = True
                print(f"==> W2: {k2} written on {FAILOVER} (records saw={read_value})", flush=True)
            except PyMongoError as e:
                print(f"==> W2 failed: {str(e)[:60]}", flush=True)
            finally:
                w.close()
        elif not dependent:
            print(f"==> no dependent write (session never read {k1}=1)", flush=True)

        finalize_experiment()

        final1 = final2 = saw = None
        for node in (FAILOVER, OLD_PRIMARY):
            try:
                c = direct(node)
                coll = c[DB].get_collection("wfr", read_concern=ReadConcern("majority"))
                d1 = coll.find_one({"k": k1}, max_time_ms=4000)
                d2 = coll.find_one({"k": k2}, max_time_ms=4000)
                final1 = d1["v"] if d1 else None
                final2 = d2["v"] if d2 else None
                saw = d2.get("saw") if d2 else None
                c.close()
                break
            except PyMongoError:
                continue

        if w2_acked and saw == 1 and final2 == 1 and final1 != 1:
            verdict = "VIOLATED"
            detail = "W2 followed rolled-back read"
        elif read_blocked:
            verdict = "SAFE"
            detail = "read UNAVAILABLE"
        elif not dependent:
            verdict = "SAFE"
            detail = "no dependent write"
        elif final1 == 1:
            verdict = "HELD"
            detail = "read value survived"

    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"

    print()
    print(f"=== WFR / doomed-read ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    rc, wc = config.split("/")
    write_concern = "majority" if wc == "majority" else 1
    return writes_follow_reads(rc, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Writes-follow-reads experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    args = ap.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
