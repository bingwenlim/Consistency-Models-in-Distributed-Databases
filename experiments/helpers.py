"""Common helpers for consistency experiments.

Shared boilerplate across RYOW, MR, MW, WFR:
- write_baseline: durable baseline writes
- finalize_experiment: heal partition + restore timeout
"""

from __future__ import annotations

import time

from pymongo.write_concern import WriteConcern

from lib import DB, direct, heal, set_election_timeout


def write_baseline(node: str, collection: str, doc: dict) -> None:
    """Write a baseline document durably (w:majority) to a node."""
    with direct(node) as client:
        client[DB].get_collection(collection, write_concern=WriteConcern(w="majority")).insert_one(doc)


def finalize_experiment(heal_wait_seconds: int = 12) -> None:
    """Heal partition and restore default election timeout."""
    heal()
    time.sleep(heal_wait_seconds)
    set_election_timeout(10000)
