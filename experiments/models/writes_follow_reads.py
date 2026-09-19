"""Writes-follow-reads (WFR) across all four readConcern x writeConcern configs.

WFR (Terry et al. 1994): if a session reads a value written by W1 and then issues
a write W2, then every server that holds W2 must also hold W1. A write is ordered
after every write the session has already observed.

The failure is subtler than monotonic-writes. It is not about W2's durability --
it is about whether the value the session READ before writing was itself durable.
If a session reads a write that later rolls back, and its dependent write W2
survives, then W2 exists while the W1 it was based on does not -> WFR VIOLATED.

Reading a doomed value is only possible with readConcern:local. readConcern:
majority returns only majority-committed data, which cannot roll back, so a write
that follows it is safely ordered. That makes WFR a readConcern story -- the
mirror image of monotonic-writes.

  MECHANISM (doomed read; the readConcern is the culprit):
    Partition mongo1+mongo2 (minority, mongo1 = old primary) from mongo3+4+5.
    W1 (k1=1) is written w:1 on the isolated mongo1 -> acknowledged, but doomed.
    The WFR session READS k1 on mongo1:
        readConcern local    -> returns k1=1 (the doomed value)
        readConcern majority -> BLOCKS: k1=1 is not majority-committed -> UNAVAILABLE
    Barrier: the majority side elects mongo3.
    The session writes W2 (k2 = "seen k1=1") on mongo3 -> durable, survives.
    Heal: mongo1 rolls back, k1=1 is discarded.
    Now k2 (which followed the read of k1=1) survives while k1=1 is gone -> VIOLATED.

    With readConcern:majority the session never reads the doomed value -- the read
    is UNAVAILABLE -- so it never issues the dependent write -> WFR SAFE.

This matches MongoDB's causal-consistency table, where WFR holds for the two
readConcern:majority rows and is absent for the two local rows -- writeConcern
is irrelevant to it.

    majority/majority -> SAFE      (doomed read blocks -> no dependent write)
    majority/w:1      -> SAFE      (doomed read blocks; writeConcern does not matter)
    local/w:1         -> VIOLATED  (reads doomed k1, writes surviving k2)
    local/majority    -> VIOLATED  (same doomed read)

Run via ../run.sh writes-follow-reads <config>, or directly:
    uv run models/writes_follow_reads.py --config local/w:1
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
    direct, heal, partition_minority, print_state, wait_primary,
)

FAILOVER_WAIT = 40
HEAL_WAIT = 12
READ_TIMEOUT_MS = 5000   # a majority read of a doomed value should hit this and abort


def writes_follow_reads(read_concern: str, write_concern, config_label: str) -> None:
    wc_label = "majority" if write_concern == "majority" else f"w:{write_concern}"
    key1 = f"wfr-k1-{int(time.time())}"
    key2 = f"wfr-k2-{int(time.time())}"

    # Baseline durable, so k1 has a committed prior state to roll back to.
    p = direct(OLD_PRIMARY)
    base = p[DB].get_collection("wfr", write_concern=WriteConcern(w="majority"))
    base.insert_one({"k": key1, "v": 0})
    base.insert_one({"k": key2, "v": 0, "saw": None})
    print(f"==> baseline written: {key1}=0, {key2}=0 (durable)", flush=True)
    p.close()

    print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
    partition_minority()

    # W1: a doomed w:1 write of k1 on the isolated minority primary.
    w1_acked = False
    p = MongoClient(
        f"mongodb://localhost:{NODES[OLD_PRIMARY]}/?directConnection=true",
        serverSelectionTimeoutMS=2000, socketTimeoutMS=3000,
    )
    try:
        coll = p[DB].get_collection("wfr", write_concern=WriteConcern(w=1, wtimeout=3000))
        coll.update_one({"k": key1}, {"$set": {"v": 1}})
        w1_acked = True
        print(f"==> W1: {key1}=1 w:1 ACKED on {OLD_PRIMARY} (doomed)", flush=True)
    except PyMongoError as e:
        print(f"==> W1 failed on {OLD_PRIMARY}: {str(e)[:70]}", flush=True)
    finally:
        p.close()

    # The WFR session READS k1 under the config's readConcern, then (only if the
    # read returns the value) issues the dependent write W2.
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
                doc = coll.find_one({"k": key1}, session=s, max_time_ms=READ_TIMEOUT_MS)
                read_value = doc["v"] if doc else None
                print(f"==> READ k1 with readConcern={read_concern} on {OLD_PRIMARY}: "
                      f"k1={read_value}", flush=True)
            except (ExecutionTimeout, PyMongoError) as e:
                read_blocked = True
                print(f"==> READ k1 with readConcern={read_concern} BLOCKED/UNAVAILABLE "
                      f"[{type(e).__name__}]", flush=True)
    finally:
        r.close()

    # Barrier: wait for the surviving side to elect a primary for the dependent write.
    print(f"==> waiting up to {FAILOVER_WAIT}s for {FAILOVER} to become primary", flush=True)
    elected = wait_primary(FAILOVER, FAILOVER_WAIT)
    print_state("during partition")

    # W2: the dependent write, issued ONLY if the session actually read k1=1.
    w2_acked = False
    dependent = read_value == 1
    if dependent and elected:
        w = direct(FAILOVER, socket_ms=8000)
        try:
            coll = w[DB].get_collection(
                "wfr", write_concern=WriteConcern(w=write_concern, wtimeout=8000)
            )
            coll.update_one({"k": key2}, {"$set": {"v": 1, "saw": read_value}})
            w2_acked = True
            print(f"==> W2: {key2} written on {FAILOVER} (records 'saw k1={read_value}')", flush=True)
        except PyMongoError as e:
            print(f"==> W2 failed on {FAILOVER}: {str(e)[:70]}", flush=True)
        finally:
            w.close()
    elif not dependent:
        print("==> no dependent write issued (session never read k1=1)", flush=True)

    print("==> healing partition", flush=True)
    heal()
    time.sleep(HEAL_WAIT)
    print_state("after heal")

    # Final: does k2 (the dependent write) survive while k1=1 is gone?
    final1 = final2 = saw = None
    for node in (FAILOVER, OLD_PRIMARY):
        try:
            c = direct(node)
            coll = c[DB].get_collection("wfr", read_concern=ReadConcern("majority"))
            d1 = coll.find_one({"k": key1}, max_time_ms=4000)
            d2 = coll.find_one({"k": key2}, max_time_ms=4000)
            final1 = d1["v"] if d1 else None
            final2 = d2["v"] if d2 else None
            saw = d2.get("saw") if d2 else None
            c.close()
            break
        except PyMongoError:
            continue

    print()
    print("=== Writes-follow-reads / doomed read ===")
    print(f"  config:              {config_label}")
    print(f"  W1 (k1) acked:       {w1_acked}")
    print(f"  session read k1:     {read_value}" + ("  (blocked)" if read_blocked else ""))
    print(f"  dependent W2 issued: {w2_acked}  (recorded saw={saw})")
    print(f"  survived heal:       k1={final1}  k2={final2}")
    if w2_acked and saw == 1 and final2 == 1 and final1 != 1:
        print("  verdict:             VIOLATED (W2 followed a read of k1=1 that rolled back)")
    elif read_blocked or not dependent:
        print("  verdict:             SAFE (doomed read was UNAVAILABLE — no dependent write)")
    elif final1 == 1:
        print("  verdict:             HELD (the read value survived)")
    else:
        print("  verdict:             INCONCLUSIVE (check the failover window)")
    print()


def run(config: str) -> None:
    rc, wc = config.split("/")
    write_concern = "majority" if wc == "majority" else 1
    writes_follow_reads(rc, write_concern, config)


def main() -> None:
    ap = argparse.ArgumentParser(description="Writes-follow-reads consistency experiment")
    ap.add_argument(
        "--config", required=True,
        choices=["majority/majority", "majority/w:1", "local/w:1", "local/majority"],
        help="readConcern/writeConcern combination to test",
    )
    args = ap.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
