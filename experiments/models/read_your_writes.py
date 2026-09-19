"""Read-your-writes (RYOW) across all four readConcern x writeConcern configs.

RYOW: after a client writes in a causally consistent session, a later read in
that session must return that write or newer -- never older.

Only majority/majority upholds RYOW (with data durability). The other three fail,
by two different real-event mechanisms (no failpoints):

  ROLLBACK (writeConcern is the culprit) -- covers the w:1 configs:
    Partition mongo1+mongo2 (minority, mongo1=old primary) from mongo3+4+5.
    A w:1 write acks on the isolated mongo1 and is read back, then is discarded
    when the partition heals (mongo3's majority history wins) -> VIOLATED.
    A w:majority write can't reach a majority on the minority side -> refused,
    never acked -> SAFE.

  DIVERGENT READ (readConcern is the culprit) -- covers local/majority:
    Raise electionTimeoutMillis so mongo1 stays writable. Partition; mongo3 is
    elected on the majority side. Write X=1 w:majority to mongo3 (capture T1). A
    real w:1 write to mongo1 advances the minority clusterTime PAST T1 and
    replicates to mongo2. A causal `local` read on mongo2 carrying T1 then does
    NOT block (clock reached T1) but mongo2 never got Write 1 -> STALE -> VIOLATED.
    A `majority` read waits for the majority commit point (the minority can never
    advance it) -> UNAVAILABLE (holds).

Configs and expected verdicts:
    majority/majority -> SAFE        (rollback path: majority write refused)
    majority/w:1      -> VIOLATED    (rollback)
    local/w:1         -> VIOLATED    (rollback)
    local/majority    -> VIOLATED    (divergent read); majority read control -> UNAVAILABLE

Run via ../run.sh read-your-writes <config>, or directly:
    uv run models/read_your_writes.py --config majority/majority
    uv run models/read_your_writes.py --config majority/w:1
    uv run models/read_your_writes.py --config local/w:1
    uv run models/read_your_writes.py --config local/majority
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

# Rollback timing: mongo1 accepts the w:1 write in the brief window before it
# steps down (default electionTimeoutMillis).
ROLLBACK_STEPDOWN_WAIT = 15
ROLLBACK_HEAL_WAIT = 12

# Divergent-read timing: at 120000, mongo1 stays writable (no stepdown) while
# mongo3 is elected ~140-145s after the partition -> a wide, reliable window.
DIVERGENT_ELECTION_TIMEOUT_MS = 120000
DIVERGENT_PRIMARY_WAIT = 175
DIVERGENT_HEAL_WAIT = 10


# --------------------------------------------------------------------------- #
# Mechanism 1: rollback (writeConcern is the culprit) -> w:1 configs.          #
# --------------------------------------------------------------------------- #
def rollback(write_concern, config_label: str) -> None:
    wc_label = "majority" if write_concern == "majority" else f"w:{write_concern}"
    key = f"rollback-{wc_label}-{int(time.time())}"

    # Baseline X=0 (durable), so there is a committed prior state to roll back to.
    p = direct(OLD_PRIMARY)
    p[DB].get_collection("ryw", write_concern=WriteConcern(w="majority")).insert_one(
        {"k": key, "v": 0}
    )
    print(f"==> baseline written: {key} = 0 (durable)", flush=True)
    p.close()

    print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
    partition_minority()

    session_saw_write = None
    acked = False
    # Short timeouts so a post-stepdown write fails fast instead of blocking.
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
                print(f"==> {wc_label} write X=1 REFUSED on {OLD_PRIMARY}: {str(e)[:70]}", flush=True)
            if acked:
                # Read own write back on the same node/session with readConcern:local
                # (a majority read here would block on the minority side).
                doc = p[DB].get_collection("ryw", read_concern=ReadConcern("local")).find_one(
                    {"k": key}, session=s
                )
                session_saw_write = doc["v"] if doc else None
                print(f"==> immediate causal read-back on {OLD_PRIMARY}: X={session_saw_write}", flush=True)
    finally:
        p.close()

    print(f"==> waiting {ROLLBACK_STEPDOWN_WAIT}s for the majority side to elect {FAILOVER}", flush=True)
    time.sleep(ROLLBACK_STEPDOWN_WAIT)
    print_state("during partition")

    print("==> healing partition", flush=True)
    heal()
    time.sleep(ROLLBACK_HEAL_WAIT)
    print_state("after heal")

    # Final read from the surviving history, bounded so it can't hang.
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

    print()
    print("=== RYOW / rollback ===")
    print(f"  config:              {config_label}")
    print(f"  write acknowledged:  {acked}")
    print(f"  read own write:      {session_saw_write}")
    print(f"  survived heal (X):   {final}")
    if acked and session_saw_write == 1 and final != 1:
        print("  verdict:             VIOLATED (acknowledged write rolled back)")
    elif not acked:
        print("  verdict:             SAFE (write refused — never falsely acknowledged)")
    elif final == 1:
        print("  verdict:             HELD (write survived)")
    else:
        print("  verdict:             INCONCLUSIVE (check timing / stepdown window)")
    print()


# --------------------------------------------------------------------------- #
# Mechanism 2: divergent read (readConcern is the culprit) -> local/majority.  #
# --------------------------------------------------------------------------- #
def divergent(read_concern: str) -> None:
    key = f"divergent-{read_concern}-{int(time.time())}"
    verdict = detail = None
    dummy_ok = False

    try:
        print(f"==> raising electionTimeoutMillis to {DIVERGENT_ELECTION_TIMEOUT_MS} "
              f"(keeps {OLD_PRIMARY}/P_old writable on the minority side)", flush=True)
        set_election_timeout(DIVERGENT_ELECTION_TIMEOUT_MS)
        time.sleep(3)

        p = direct(OLD_PRIMARY)
        p[DB].get_collection("ryw", write_concern=WriteConcern(w="majority")).insert_one(
            {"k": key, "v": 0}
        )
        print(f"==> baseline written: {key} = 0 (durable)", flush=True)
        p.close()

        print(f"==> partitioning {MINORITY} (minority) from the majority side", flush=True)
        partition_minority()

        print(f"==> waiting up to {DIVERGENT_PRIMARY_WAIT}s for {FAILOVER} to become primary "
              f"(lands ~140-145s; {OLD_PRIMARY} stays writable)", flush=True)
        if not wait_primary(FAILOVER, DIVERGENT_PRIMARY_WAIT):
            print(f"==> {FAILOVER} not elected in time; aborting", flush=True)
            print("  verdict: INCONCLUSIVE (no new primary)")
            return

        # Write 1: X=1, w:majority, on the winning side. Capture causal token T1.
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
            print(f"==> Write 1 failed: {str(e)[:70]}; aborting", flush=True)
            print("  verdict: INCONCLUSIVE (majority write did not ack)")
            return
        finally:
            w.close()

        # Dummy w:1 to mongo1 AFTER Write 1 (timestamp > T1): advances the minority
        # clusterTime past T1 and replicates to mongo2. This is what lets the later
        # causal local read on mongo2 clear its afterClusterTime=T1 gate WITHOUT
        # mongo2 ever receiving Write 1 -> stale read.
        time.sleep(1)
        d = direct(OLD_PRIMARY, socket_ms=3000)
        try:
            d[DB].get_collection("dummy", write_concern=WriteConcern(w=1)).insert_one(
                {"tick": int(time.time())}
            )
            dummy_ok = True
            print(f"==> dummy w:1 to {OLD_PRIMARY} OK (advances minority clock PAST T1)", flush=True)
        except PyMongoError as e:
            print(f"==> dummy write failed ({OLD_PRIMARY} unexpectedly not writable): {str(e)[:60]}", flush=True)
        finally:
            d.close()

        time.sleep(2)  # let the dummy replicate to the minority secondary

        # Read 1: causal read of X on the minority secondary, carrying T1.
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
                    print(f"==> Read 1: {read_concern} read on {MINORITY_SECONDARY} (minority): X={val}", flush=True)
                    if val == 1:
                        verdict, detail = "HELD", "read reflected the write"
                    else:
                        verdict, detail = "VIOLATED", (
                            f"read returned stale X={val}; clock advanced past T1 but "
                            f"{MINORITY_SECONDARY} never received Write 1"
                        )
                except ExecutionTimeout:
                    verdict, detail = "UNAVAILABLE", (
                        f"{read_concern} read blocked and hit maxTimeMS: the minority "
                        f"({MINORITY_SECONDARY}) cannot advance the majority commit point"
                    )
                except PyMongoError as e:
                    verdict, detail = "UNAVAILABLE", (
                        f"{read_concern} read blocked until the socket timed out "
                        f"[{type(e).__name__}]"
                    )
        finally:
            r.close()
    finally:
        print("==> healing partition + restoring electionTimeoutMillis=10000", flush=True)
        heal()
        time.sleep(DIVERGENT_HEAL_WAIT)
        set_election_timeout(10000)

    print()
    print("=== RYOW / divergent read ===")
    print(f"  config:        local/majority (read concern = {read_concern})")
    print(f"  dummy write:   {'ok' if dummy_ok else 'FAILED'}")
    print(f"  verdict:       {verdict} ({detail})")
    print()


# --------------------------------------------------------------------------- #
# Dispatch: config -> mechanism.                                              #
# --------------------------------------------------------------------------- #
def run(config: str) -> None:
    if config == "majority/majority":
        rollback("majority", config)    # majority write refused on minority side -> SAFE
    elif config == "majority/w:1":
        rollback(1, config)             # rollback -> VIOLATED
    elif config == "local/w:1":
        rollback(1, config)             # same rollback mechanism -> VIOLATED
    elif config == "local/majority":
        divergent("local")             # divergent read -> VIOLATED
    else:
        raise SystemExit(f"unknown config: {config}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Read-your-writes consistency experiment")
    ap.add_argument(
        "--config", required=True,
        choices=["majority/majority", "majority/w:1", "local/w:1", "local/majority"],
        help="readConcern/writeConcern combination to test",
    )
    ap.add_argument(
        "--control", action="store_true",
        help="for local/majority: run the majority-read control (expect UNAVAILABLE)",
    )
    args = ap.parse_args()
    if args.config == "local/majority" and args.control:
        divergent("majority")
    else:
        run(args.config)


if __name__ == "__main__":
    main()
