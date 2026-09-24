"""Writes-follow-reads (WFR) across all four readConcern x writeConcern configs.

WFR: a write issued after reading a value is ordered after that value.
If a session reads X and writes Y, every server holding Y must also hold X.

Mechanism: doomed-read. A session reads X=1 (a value doomed to roll back), then
issues a dependent write W2 on the winning primary in a session that carries the
read's transferred causal timestamps. On heal, X=1 rolls back while W2 survives --
a dependent write following a read that no server retains violates WFR.
A majority read never observes the doomed X=1 (it is not majority committed): it
either returns the committed X=0 or blocks, so no dependent write is issued.

Expected verdicts:
  majority/majority -> NOT_VIOLATED  (majority read sees X=0 or blocks, no dependent write)
  majority/w:1      -> NOT_VIOLATED  (majority read sees X=0 or blocks, no dependent write)
  local/w:1         -> VIOLATED      (read doomed value, write survives rollback)
  local/majority    -> VIOLATED      (read doomed value, write survives rollback)
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


class Inconclusive(Exception):
    pass


def writes_follow_reads(read_concern: str, write_concern, config_label: str) -> str:
    """Doomed-read mechanism: session reads doomed value, issues dependent write."""
    k1 = f"wfr-k1-{uuid4().hex}"
    k2 = f"wfr-k2-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not complete"
    healed = False

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
            # W1 is not one of the session's writes under test; it is a separate
            # actor's write that seeds a doomed value for our session to read.
            # It must be w:1 to ack on the isolated minority and later roll back.
            coll = p[DB].get_collection("wfr", write_concern=WriteConcern(w=1, wtimeout=3000))
            coll.update_one({"k": k1}, {"$set": {"v": 1}})
            w1_acked = True
            print(f"==> W1: {k1}=1 w:1 ACKED on {OLD_PRIMARY} (doomed)", flush=True)
        except PyMongoError as e:
            print(f"==> W1 failed: {str(e)[:60]}", flush=True)
        finally:
            p.close()

        # W1 is the doomed state the read is meant to observe. If it never
        # acked, the trial's precondition was not established -- that is a setup
        # failure (INCONCLUSIVE), distinct from a majority read legitimately not
        # seeing the uncommitted W1 (a negative control handled below).
        if not w1_acked:
            raise Inconclusive("could not establish doomed W1 on the minority")

        read_value = None
        read_blocked = False
        op_time = cluster_time = None
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
                    op_time = s.operation_time
                    cluster_time = s.cluster_time
                    print(f"==> READ {k1}: {read_concern} read: {k1}={read_value}", flush=True)
                except ExecutionTimeout:
                    read_blocked = True
                    print(f"==> READ {k1}: {read_concern} timed out (blocked on majority)", flush=True)
                except PyMongoError as e:
                    raise Inconclusive(f"read errored [{type(e).__name__}]: {str(e)[:60]}")
        finally:
            r.close()

        print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} PRIMARY", flush=True)
        elected = wait_primary(FAILOVER, FAILOVER_WAIT)
        print_state("during partition")

        w2_acked = False
        dependent = read_value == 1
        if dependent and not elected:
            raise Inconclusive(f"{FAILOVER} was not elected; cannot issue dependent W2")
        if dependent:
            if op_time is None:
                raise Inconclusive("read session has no causal operation time")
            w = direct(FAILOVER, socket_ms=8000)
            try:
                # W2 is issued on the winning primary after transferring the read
                # session's causal timestamps into this session.
                with w.start_session(causal_consistency=True) as s2:
                    if cluster_time is not None:
                        s2.advance_cluster_time(cluster_time)
                    s2.advance_operation_time(op_time)
                    coll = w[DB].get_collection("wfr", write_concern=WriteConcern(w=write_concern, wtimeout=8000))
                    coll.update_one({"k": k2}, {"$set": {"v": 1, "saw": read_value}}, session=s2)
                    w2_acked = True
                    print(f"==> W2: {k2} written on {FAILOVER} after READ {k1}=1 (records saw={read_value})", flush=True)
            except PyMongoError as e:
                print(f"==> W2 failed: {str(e)[:60]}", flush=True)
            finally:
                w.close()
        elif not dependent:
            print(f"==> no dependent write (session never read {k1}=1)", flush=True)

        # Heal before the verdict reads: the VIOLATED case depends on the doomed
        # X=1 having rolled back in the recovered durable state.
        finalize_experiment()
        healed = True

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
            verdict = "NOT_VIOLATED"
            detail = "read blocked, no dependent write"
        elif not dependent:
            verdict = "NOT_VIOLATED"
            detail = f"read returned {read_value}, no dependent write"
        elif final1 == 1:
            verdict = "NOT_VIOLATED"
            detail = "read value survived"

    except Inconclusive as e:
        detail = str(e)
    except (PyMongoError, OSError) as e:
        detail = f"{type(e).__name__}: {str(e)[:60]}"
    finally:
        if not healed:
            finalize_experiment()

    print()
    print(f"=== WFR / doomed-read ===")
    print(f"  config:   {config_label}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    return writes_follow_reads(read_concern, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Writes-follow-reads experiment")
    ap.add_argument("--config", required=True, choices=CONFIGS, help="readConcern/writeConcern")
    args = ap.parse_args()
    if run(args.config) == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
