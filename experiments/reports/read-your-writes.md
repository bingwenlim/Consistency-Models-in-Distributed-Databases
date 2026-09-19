# Read-Your-Writes Consistency — Report

## 1. Hypothesis

Read-your-writes (RYOW): after a client writes a value in a causally consistent
session, a later read in that session must return that value or newer — never an
older state.

With data durability, only `readConcern:majority` + `writeConcern:majority`
guarantees RYOW. The other three configs violate it. We demonstrate each on a real
5-node MongoDB replica set using only real events (network partition + real
writes) — no failpoints.

## 2. Setup

- 5-node replica set. Priorities: mongo1=2 (PRIMARY), mongo3=1 (failover), mongo2/4/5=0.
- Client uses a causally consistent session throughout.
- Partition: mongo1+mongo2 (minority, mongo1 = old primary) vs mongo3+mongo4+mongo5
  (majority, mongo3 = new primary). All nodes stay reachable from the client; only
  inter-node traffic is cut.
- Verdicts: VIOLATED (read saw older/absent), SAFE / HELD (guarantee upheld),
  UNAVAILABLE (read/write refused — consistent but not available; not a failure).

Run: `./run.sh read-your-writes <config>` (configs below).

## 3. Results

| readConcern | writeConcern | verdict | mechanism |
|---|---|---|---|
| majority | majority | SAFE | majority write refused on minority side |
| majority | w:1 | VIOLATED | rollback |
| local | w:1 | VIOLATED | rollback |
| local | majority | VIOLATED | divergent read; `majority` read control → UNAVAILABLE |

**3 of 4 violate RYOW.** Only majority/majority holds.

## 4. The two write-concern failures (rollback)

Both `w:1` configs fail identically: a `w:1` write is acknowledged on mongo1 while
it is briefly isolated, read back successfully, then discarded when the partition
heals (mongo3's majority history wins).

Steps:
1. Baseline X=0 (durable, w:majority).
2. Partition mongo1+mongo2 off; mongo1 still briefly primary.
3. Write X=1 with `w:1` to mongo1; read it back (same causal session) → sees X=1.
4. Majority side elects mongo3. Heal.
5. Re-read X → X=0 (write is gone).

Captured (`./run.sh read-your-writes majority/w:1`):
```
==> baseline written: rollback-w:1-... = 0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> w:1 write X=1 ACKED on mongo1
==> immediate causal read-back on mongo1: X=1
==> waiting 15s for the majority side to elect mongo3
    [state @ during partition]
      mongo1: reachable, isWritablePrimary=False
      mongo3: reachable, isWritablePrimary=True
==> healing partition
=== RYOW / rollback ===
  config:              majority/w:1
  write acknowledged:  True
  read own write:      1
  survived heal (X):   0
  verdict:             VIOLATED (acknowledged write rolled back)
```

`local/w:1` uses the same path and yields the same VIOLATED result.

Why majority/majority is safe here (`./run.sh read-your-writes majority/majority`):
a `w:majority` write on the isolated minority side can never reach a majority, so
it is never acknowledged — there is no acknowledged write to lose.
```
==> majority write X=1 REFUSED on mongo1: ...timed out...
=== RYOW / rollback ===
  config:              majority/majority
  write acknowledged:  False
  verdict:             SAFE (write refused — never falsely acknowledged)
```

## 5. The read-concern failure: local/majority (divergent read)

Here the write is durable (`w:majority`, on the winning side). The question is
whether a `local` read can still miss it.

### 5a. Why it is normally CONSISTENT

In a causal session, every read — including `readConcern:local` — carries
`afterClusterTime = T1` (the write's timestamp). The node it reads from waits until
its own clock reaches T1 before answering. A minority node that never received the
write cannot advance its clock to T1 on its own, so the read BLOCKS (times out →
UNAVAILABLE) rather than returning stale data. The causal token protects even a
`local` read.

### 5b. How we made it INCONSISTENT

The gap: a causal `local` read waits for the node's CLOCK to reach T1 — it does NOT
verify the DATA written at T1 is present. So if the node's clock is advanced past
T1 by unrelated activity while the write itself never arrives, the read returns
immediately with stale data.

Steps:
1. Raise `electionTimeoutMillis` to 120000 so mongo1 (old primary) stays writable
   on the minority side (measured: at this value it does not step down; mongo3 is
   elected ~140-145s after the partition — a wide, reliable window).
2. Baseline X=0 (durable). Partition; wait for mongo3 to be elected.
3. Write 1: X=1 with `w:majority` to mongo3 (winning side). Capture T1.
4. A real `w:1` write to mongo1 (minority primary, unrelated key), AFTER Write 1 —
   its timestamp > T1, so it advances the minority clock past T1 and replicates to
   mongo2.
5. Read 1: `readConcern:local` on mongo2 (minority secondary), same causal session
   carrying T1. mongo2's clock is now ≥ T1 so it does NOT block, but it never
   received Write 1 → returns stale X=0.

> The clock advance in step 4 is a real, natural event: any write on the still-
> writable minority primary ticks its clock forward. We issue it explicitly only to
> make the (real but rare) timing deterministic; MongoDB is not modified. Inflating
> `electionTimeoutMillis` is done only to widen this rare window so it is reliably
> catchable — the underlying scenario occurs at the default timeout too, just rarely.

Captured (`./run.sh read-your-writes local/majority`):
```
==> raising electionTimeoutMillis to 120000 (keeps mongo1/P_old writable on the minority side)
==> baseline written: divergent-local-... = 0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> waiting up to 175s for mongo3 to become primary (lands ~140-145s; mongo1 stays writable)
==> Write 1: X=1 w:majority ACKED on mongo3; T1=Timestamp(...)
==> dummy w:1 to mongo1 OK (advances minority clock PAST T1)
==> Read 1: local read on mongo2 (minority): X=0
=== RYOW / divergent read ===
  config:        local/majority (read concern = local)
  verdict:       VIOLATED (read returned stale X=0; clock advanced past T1 but mongo2 never received Write 1)
```

### 5c. Control: the same setup with a majority read

`./run.sh read-your-writes local/majority --control` — identical setup but
`readConcern:majority`. The read waits for the majority commit point, which the
minority can never advance → UNAVAILABLE. It never returns stale:
```
=== RYOW / divergent read ===
  config:        local/majority (read concern = majority)
  verdict:       UNAVAILABLE (majority read blocked and hit maxTimeMS: the minority
                 (mongo2) cannot advance the majority commit point)
```

This is the whole distinction: `local` gates on the node clock (can be tricked);
`majority` gates on the majority commit point (cannot).

## 6. Conclusion

| readConcern | writeConcern | RYOW | why |
|---|---|---|---|
| majority | majority | ✅ holds | write refused / read waits — never stale |
| majority | w:1 | ❌ fails | acknowledged write rolls back |
| local | w:1 | ❌ fails | acknowledged write rolls back |
| local | majority | ❌ fails | causal local read gates on node clock, not data presence |

Write concern controls whether an acknowledged write is durable; read concern
controls whether a read gates on the node clock (`local`) or the majority commit
point (`majority`). RYOW under all conditions needs both — only majority/majority.
