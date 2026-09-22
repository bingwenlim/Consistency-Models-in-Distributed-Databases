"""Shared setup for the write-ordering experiments."""

from __future__ import annotations

import time
from typing import Callable

from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from lib import (
    DB,
    FAILOVER,
    MINORITY_SECONDARY,
    OLD_PRIMARY,
    direct,
    heal,
    partition_minority,
    set_election_timeout,
    wait_primary,
)


CONFIGS = {
    "majority/majority": ("majority", "majority"),
    "majority/w:1": ("majority", 1),
    "local/majority": ("local", "majority"),
    "local/w:1": ("local", 1),
}
ELECTION_TIMEOUT_MS = 120000


class Inconclusive(Exception):
    pass


def wait_for_document(
    node: str,
    collection: str,
    query: dict,
    accept: Callable[[dict | None], bool],
    seconds: int = 20,
    read_concern: str = "local",
) -> dict | None:
    """Return the first matching document accepted by the caller, or None."""
    deadline = time.monotonic() + seconds
    with direct(node, socket_ms=5000) as client:
        coll = client[DB].get_collection(
            collection, read_concern=ReadConcern(read_concern)
        )
        while time.monotonic() < deadline:
            try:
                document = coll.find_one(query, max_time_ms=2500)
                if accept(document):
                    return document
            except PyMongoError:
                pass
            time.sleep(0.5)
    return None


def prepare_partition(collection: str, baseline: dict) -> None:
    """Commit a baseline on both sides, then elect the majority-side primary."""
    set_election_timeout(ELECTION_TIMEOUT_MS)
    with direct(OLD_PRIMARY) as client:
        config = client.admin.command("replSetGetConfig")["config"]
        actual = config.get("settings", {}).get("electionTimeoutMillis", 10000)
        if actual != ELECTION_TIMEOUT_MS:
            raise Inconclusive(f"election timeout is {actual}, expected {ELECTION_TIMEOUT_MS}")
        client[DB].get_collection(
            collection, write_concern=WriteConcern(w="majority", wtimeout=10000)
        ).insert_one(baseline)

    query = {"k": baseline["k"]}
    for node in (MINORITY_SECONDARY, FAILOVER):
        if wait_for_document(
            node, collection, query,
            lambda doc: doc is not None and doc.get("k") == baseline["k"],
        ) is None:
            raise Inconclusive(f"baseline did not replicate to {node}")

    time.sleep(3)
    print("==> baseline replicated to both partition sides", flush=True)
    partition_minority()
    print(f"==> waiting for {FAILOVER} to become PRIMARY", flush=True)
    if not wait_primary(FAILOVER, 175):
        raise Inconclusive(f"{FAILOVER} was not elected")


def transfer_causal_time(source_session, target_session) -> None:
    """Carry causal context between sessions bound to different MongoClients."""
    if source_session.operation_time is None:
        raise Inconclusive("source session has no operation time")
    if source_session.cluster_time is not None:
        target_session.advance_cluster_time(source_session.cluster_time)
    target_session.advance_operation_time(source_session.operation_time)


def restore_cluster() -> None:
    heal()
    time.sleep(10)
    set_election_timeout(10000)
