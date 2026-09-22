"""Test whether a later write is applied after an earlier session write."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4

from pymongo.errors import PyMongoError
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
from lib import DB, FAILOVER, MINORITY_SECONDARY, OLD_PRIMARY, direct  # noqa: E402


COLLECTION = "mw"


def append_event(client, key: str, event: str, write_concern, session) -> None:
    collection = client[DB].get_collection(
        COLLECTION,
        write_concern=WriteConcern(w=write_concern, wtimeout=8000),
    )
    result = collection.update_one(
        {"k": key}, {"$push": {"events": event}}, session=session
    )
    if result.matched_count != 1:
        raise Inconclusive(f"{event} did not match the baseline document")


def run(config: str) -> str:
    read_concern, write_concern = CONFIGS[config]
    key = f"mw-{uuid4().hex}"
    verdict = "INCONCLUSIVE"
    detail = "trial did not reach its final verification"
    restored = False

    try:
        prepare_partition(COLLECTION, {"k": key, "events": []})

        if write_concern == 1:
            # W1 can be acknowledged by the isolated old primary and later roll back.
            with direct(OLD_PRIMARY) as old_client:
                with old_client.start_session(causal_consistency=True) as first_session:
                    append_event(old_client, key, "W1", 1, first_session)
                    print(f"==> W1 acknowledged on {OLD_PRIMARY} with w=1", flush=True)
                    if wait_for_document(
                        MINORITY_SECONDARY, COLLECTION, {"k": key},
                        lambda doc: doc is not None and doc.get("events") == ["W1"],
                        seconds=12,
                    ) is None:
                        raise Inconclusive("W1 did not replicate within the minority")

                    with direct(FAILOVER) as new_client:
                        with new_client.start_session(causal_consistency=True) as second_session:
                            transfer_causal_time(first_session, second_session)
                            append_event(new_client, key, "W2", 1, second_session)
                            print(f"==> W2 acknowledged on {FAILOVER} with w=1", flush=True)
        else:
            # A majority write cannot complete on the isolated minority side.
            # Both acknowledged writes therefore use the elected majority primary.
            with direct(FAILOVER) as new_client:
                with new_client.start_session(causal_consistency=True) as session:
                    append_event(new_client, key, "W1", "majority", session)
                    print(f"==> W1 acknowledged on {FAILOVER} with w=majority", flush=True)
                    append_event(new_client, key, "W2", "majority", session)
                    print(f"==> W2 acknowledged on {FAILOVER} with w=majority", flush=True)

        if wait_for_document(
            FAILOVER, COLLECTION, {"k": key},
            lambda doc: doc is not None and "W2" in doc.get("events", []),
            seconds=20, read_concern="majority",
        ) is None:
            raise Inconclusive("W2 did not become majority committed")

        restore_cluster()
        restored = True
        with direct(FAILOVER) as client:
            doc = client[DB].get_collection(
                COLLECTION, read_concern=ReadConcern(read_concern)
            ).find_one({"k": key}, max_time_ms=6000)
        events = doc.get("events") if doc else None
        print(f"==> final {read_concern} read on {FAILOVER}: events={events}", flush=True)

        if events == ["W2"] and write_concern == 1:
            verdict = "VIOLATED"
            detail = "W2 survived, but the preceding acknowledged W1 rolled back"
        elif events == ["W1", "W2"]:
            verdict = "HELD"
            detail = "W2 was applied after W1 in this trial"
        else:
            detail = f"unexpected final event history: {events}"
    except Inconclusive as exc:
        detail = str(exc)
    except (PyMongoError, OSError) as exc:
        detail = f"{type(exc).__name__}: {exc}"
    finally:
        if not restored:
            print("==> healing partition and restoring election timeout", flush=True)
            restore_cluster()

    print("\n=== Monotonic writes ===")
    print(f"  config:   {config}")
    print(f"  verdict:  {verdict} ({detail})\n")
    return verdict


def main() -> None:
    parser = argparse.ArgumentParser(description="Monotonic-writes experiment")
    parser.add_argument("--config", required=True, choices=CONFIGS)
    args = parser.parse_args()
    if run(args.config) == "INCONCLUSIVE":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
