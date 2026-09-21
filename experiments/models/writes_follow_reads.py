"""Test whether a write follows the state observed by a preceding read."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4

from pymongo.errors import ExecutionTimeout, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from fault_support import (  # noqa: E402
    CONFIGS,
    Inconclusive,
    prepare_partition,
    restore_cluster,
    transfer_causal_time,
    wait_for_document,
)
from lib import DB, FAILOVER, OLD_PRIMARY, direct  # noqa: E402


SOURCE_COLLECTION = "wfr_source"
DEPENDENT_COLLECTION = "wfr_dependent"


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    key = f"wfr-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not reach its final verification"
    restored = False

    try:
        prepare_partition(SOURCE_COLLECTION, {"k": key, "v": 0})

        # This external w:1 write is visible locally but will lose to the
        # majority-side history when the partition heals.
        with direct(OLD_PRIMARY) as client:
            result = client[DB].get_collection(
                SOURCE_COLLECTION, write_concern=WriteConcern(w=1)
            ).update_one({"k": key}, {"$set": {"v": 1}})
            if result.matched_count != 1:
                raise Inconclusive("external write did not match the source document")
        print(f"==> external X=1 acknowledged on {OLD_PRIMARY} with w=1", flush=True)

        with direct(OLD_PRIMARY, socket_ms=10000) as old_client:
            with old_client.start_session(causal_consistency=True) as read_session:
                source = old_client[DB].get_collection(
                    SOURCE_COLLECTION, read_concern=ReadConcern(read_concern)
                )
                try:
                    first = source.find_one(
                        {"k": key}, session=read_session, max_time_ms=6000
                    )
                except ExecutionTimeout:
                    verdict = "UNAVAILABLE"
                    detail = "the first read timed out; no dependent write was issued"
                else:
                    observed = first.get("v") if first else None
                    print(f"==> Read 1 on {OLD_PRIMARY}: X={observed}", flush=True)
                    if observed not in (0, 1):
                        raise Inconclusive(f"first read returned unexpected value {observed}")
                    if read_concern == "local" and observed != 1:
                        raise Inconclusive("local read did not observe the external write")
                    if read_concern == "majority" and observed != 0:
                        raise Inconclusive("majority read unexpectedly observed X=1")

                    with direct(FAILOVER, socket_ms=12000) as new_client:
                        with new_client.start_session(causal_consistency=True) as write_session:
                            transfer_causal_time(read_session, write_session)
                            new_client[DB].get_collection(
                                DEPENDENT_COLLECTION,
                                write_concern=WriteConcern(w=write_concern, wtimeout=8000),
                            ).insert_one(
                                {"k": key, "observed": observed}, session=write_session
                            )
                            print(
                                f"==> dependent write acknowledged on {FAILOVER} "
                                f"with w={write_concern}; observed={observed}",
                                flush=True,
                            )

                    if wait_for_document(
                        FAILOVER, DEPENDENT_COLLECTION, {"k": key},
                        lambda doc: doc is not None and doc.get("observed") == observed,
                        seconds=20, read_concern="majority",
                    ) is None:
                        raise Inconclusive("dependent write did not become majority committed")

                    restore_cluster()
                    restored = True
                    with direct(FAILOVER) as client:
                        committed_source = client[DB].get_collection(
                            SOURCE_COLLECTION, read_concern=ReadConcern("majority")
                        ).find_one({"k": key}, max_time_ms=6000)
                        committed_dependent = client[DB].get_collection(
                            DEPENDENT_COLLECTION, read_concern=ReadConcern("majority")
                        ).find_one({"k": key}, max_time_ms=6000)
                    final_value = committed_source.get("v") if committed_source else None
                    final_observed = (
                        committed_dependent.get("observed")
                        if committed_dependent else None
                    )
                    print(
                        f"==> after heal: X={final_value}, "
                        f"dependent observed={final_observed}",
                        flush=True,
                    )
                    if final_value == 0 and final_observed == 1:
                        verdict = "VIOLATED"
                        detail = "dependent write survived, but the state it read rolled back"
                    elif final_value == 0 and final_observed == 0:
                        verdict = "HELD"
                        detail = "dependent write follows the committed state read in this trial"
                    else:
                        detail = (
                            f"unexpected final state: X={final_value}, "
                            f"dependent observed={final_observed}"
                        )
    except Inconclusive as exc:
        detail = str(exc)
    except (PyMongoError, OSError) as exc:
        detail = f"{type(exc).__name__}: {exc}"
    finally:
        if not restored:
            print("==> healing partition and restoring election timeout", flush=True)
            restore_cluster()

    print("\n=== Writes follow reads ===")
    print(f"  config:   {config}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def main() -> None:
    parser = argparse.ArgumentParser(description="Writes-follow-reads experiment")
    parser.add_argument("--config", required=True, choices=CONFIGS)
    args = parser.parse_args()
    if run(args.config) == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
