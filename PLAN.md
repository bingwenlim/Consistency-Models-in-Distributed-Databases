# Project Plan — Consistency Experiments

## Goal
Empirically test 4 client-centric consistency models on a 5-node MongoDB replica
set, each across 4 readConcern × writeConcern configs. Show which configs uphold
each guarantee and which violate it, using only real events (network partition +
real writes) — no failpoints.

The 4 models: read-your-writes, monotonic reads, monotonic writes, writes-follow-reads.
The 4 configs: readConcern {local, majority} × writeConcern {w:1, majority}.

## Cluster
5 nodes in Docker. Priorities: mongo1=2 (PRIMARY), mongo3=1 (failover), mongo2/4/5=0.
Client connects to specific nodes by host port with directConnection=true (a
replicaSet URI does not resolve container hostnames from the host).
Partition tool: scripts/partition-split.sh mongo1 mongo2 (minority) vs mongo3/4/5
(majority); nodes stay host-reachable, only inter-node traffic is blocked.

## Target file structure
```
experiments/
  lib.py                    shared: NODES, DB, connect, partition/reconfig helpers,
                            verdict classification, result-table printer
  models/
    read_your_writes.py     all 4 configs for RYOW
    monotonic_reads.py      (later)
    monotonic_writes.py     (later)
    writes_follow_reads.py  (later)
  run.sh                    entrypoint: ./run.sh <model> [config]
  reports/
    read-your-writes.md     steps + results per model
    ...
scripts/                    up/down/status/partition-split/heal-split/rs-init
```

## Read-your-writes: the 4 configs and mechanisms
| readConcern | writeConcern | verdict | mechanism |
|---|---|---|---|
| majority | majority | SAFE (control) | majority write refused on minority side; majority read waits, never stale |
| majority | w:1 | VIOLATED | w:1 write acked on old primary, rolled back on heal |
| local | w:1 | VIOLATED | same rollback (w:1 is the culprit) |
| local | majority | VIOLATED (if reproducible) | divergent read: causal local read gates on node CLOCK, not data presence |

## Status
- DONE: rollback mechanism (majority/w:1, local/w:1 → VIOLATED; majority/majority → SAFE). Verified.
- DONE: divergent_read (local/majority → VIOLATED; majority-read control → UNAVAILABLE). Verified.
- DONE: refactored into models/read_your_writes.py with shared lib.py helpers; single run.sh; report at reports/read-your-writes.md.
- PENDING: other 3 models (monotonic reads, monotonic writes, writes-follow-reads).

## Fault mechanisms (real events only, no failpoints)
- Rollback: partition puts old primary on minority side; w:1 write acks there and is
  discarded on heal.
- Divergent read: partition; majority write on winning side; a real w:1 write on the
  minority primary advances that side's clusterTime past the write's timestamp T1, so
  a causal local read on a minority secondary does not block (clock reached T1) yet
  returns stale data (it never received the write).
- electionTimeoutMillis is intentionally inflated to widen the (real but rare) timing
  window where the old primary is still writable while the new primary is elected.
