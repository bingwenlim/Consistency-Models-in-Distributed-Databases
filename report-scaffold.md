# Report Scaffold

Two things live here:
1. **Report backbone** — the section-by-section outline for the full group report (PDF).
2. **Experiment 1 (Read-your-writes)** — a ready-to-paste write-up for the Experiment 1 section.

---

# 1. Report Backbone

> Fill each bullet. Read-your-writes (Experiment 1) is written out in §2 below; the
> other three models follow the same four-part structure.

### Introduction
- Chosen database: **MongoDB 7.0** (document store, replica-set replication).
- Deployment architecture: **5-node replica set `rs0`** in Docker Compose, one
  `mongo:7.0` container per node on a shared bridge network (`mongo-cluster`).
  Priorities `mongo1=2` (primary), `mongo3=1` (failover), `mongo2/4/5=0`.
- Software versions: MongoDB 7.0, mongosh (bundled), PyMongo ≥ 4.9, Python ≥ 3.11
  (via `uv`), Docker Compose.
- Installation / run procedure: `./scripts/up.sh` (start + init + force mongo1
  primary) → experiments via `experiments/run.sh` → `./scripts/down.sh` to stop.
- Client connection detail worth noting: connect per-node by host port with
  `directConnection=true` (a `replicaSet` URI can't resolve container hostnames from
  the host).

### The four configurations and their parameters
- The two swept knobs: **`writeConcern` ∈ {`1`, `majority`}** and **`readConcern` ∈
  {`local`, `majority`}** → 4 configs.
- Fixed knobs: **causal session always ON** (guarantees are defined over it);
  **target node chosen per experiment** (route the read at a node that can lag/diverge).
- Parameter meanings (cite MongoDB docs):
  - `writeConcern w:1` — acknowledged once the primary has it; not necessarily durable.
  - `writeConcern majority` — acknowledged only after a majority has it; durable.
  - `readConcern local` — returns the node's most recent data; may not be committed.
  - `readConcern majority` — returns only majority-committed data.
- Expected outcome per config, **based on the MongoDB causal-consistency docs**
  (the docs state RYOW-style guarantees hold only under `readConcern majority` +
  `writeConcern majority`):

  | readConcern | writeConcern | expected |
  |---|---|---|
  | majority | majority | guarantee holds |
  | majority | w:1 | can fail |
  | local | w:1 | can fail |
  | local | majority | can fail |

### Experiment 1 — Read-your-writes  *(written out in §2)*
- Consistency under test.
- Design + rationale.
- Results + explanation.
- Expectations vs. observations + limitations.

### Experiment 2 — Monotonic-reads  *(groupmate)*
- Consistency under test: once a session reads a value, later reads must not return
  an older state.
- Design + rationale.
- Results + explanation.
- Expectations vs. observations + limitations.

### Experiment 3 — Monotonic-writes  *(groupmate)*
- Consistency under test: a session's writes are applied in the order issued.
- Design + rationale.
- Results + explanation.
- Expectations vs. observations + limitations.

### Experiment 4 — Writes-follow-reads  *(groupmate)*
- Consistency under test: a write made after reading a value is ordered after it.
- Design + rationale.
- Results + explanation.
- Expectations vs. observations + limitations.

### References
- MongoDB Manual: causal consistency, read concern, write concern, replica-set
  elections/rollbacks.
- **AI usage disclosure**: state where AI assistance was used (e.g. drafting scripts,
  debugging the timing window, drafting the report) and that all results were
  produced and verified by running the experiments.

---

# 2. Experiment 1 — Read-Your-Writes

## 2.1 The consistency under test

Read-your-writes (RYOW): after a client writes a value inside a causally consistent
session, any later read in that same session must return that value **or newer** —
never an older or absent state.

**Prediction.** With data durability, only `readConcern:majority` +
`writeConcern:majority` upholds RYOW. The other three configs violate it. We
demonstrate each on the real 5-node replica set using only real events (network
partition + real writes) — no failpoints.

| readConcern | writeConcern | predicted | observed | mechanism |
|---|---|---|---|---|
| majority | majority | holds | **SAFE** | majority write refused on the minority side |
| majority | w:1 | fails | **VIOLATED** | rollback |
| local | w:1 | fails | **VIOLATED** | rollback |
| local | majority | fails | **VIOLATED** | divergent read (`majority`-read control → UNAVAILABLE) |

Three of four violate RYOW; only majority/majority holds. Two distinct real-event
mechanisms produce the failures — one driven by the write concern, one by the read
concern.

## 2.2 Design and rationale

**Setup.** 5-node replica set; priorities mongo1=2 (primary), mongo3=1 (failover),
mongo2/4/5=0. Every trial uses a causally consistent session. The partition splits the
cluster into **minority = mongo1+mongo2** (led by the old primary) and **majority =
mongo3+mongo4+mongo5** (mongo3 wins). All nodes stay reachable from the client; only
inter-node traffic is cut, so we can still read any node by its port.

We run **two experiments** because the three failing configs fail for two different
underlying reasons:

**Experiment 1A — rollback (the write concern is the culprit).** Covers both `w:1`
configs, and provides the `majority/majority` safe control.
1. Write baseline `X=0` durably (`w:majority`).
2. Partition the minority off; mongo1 is still briefly writable.
3. Write `X=1` with the config's write concern to mongo1, then read it back in the
   same session.
4. The majority side elects mongo3. Heal the partition.
5. Re-read `X`.

*Rationale:* a `w:1` write is acknowledged on the isolated mongo1 and read back
(RYOW momentarily appears to hold), but when the partition heals mongo3's majority
history wins and the write is **rolled back** — the acknowledged write vanishes, so a
later read returns the old value. A `w:majority` write on the minority side can never
reach a majority, so it is **never acknowledged** (refused) — there is no acknowledged
write to lose, which is exactly why majority/majority is safe.

**Experiment 1B — divergent read (the read concern is the culprit).** Covers
`local/majority`, where the write *is* durable and we test whether a `local` read can
still miss it.
1. Raise `electionTimeoutMillis` to 120000 so mongo1 stays writable on the minority
   side (widens a real but rare timing window so it is reliably catchable).
2. Baseline `X=0` durable. Partition. Wait for mongo3 to be elected on the majority side.
3. Write `X=1` with `w:majority` to mongo3 (winning side); capture its timestamp T1.
4. Issue a real `w:1` write to mongo1 (unrelated key) *after* T1 — its timestamp > T1,
   so it advances the minority side's cluster time past T1 and replicates to mongo2.
5. Read `X` with `readConcern:local` on mongo2, in the same causal session carrying T1.

*Rationale:* a causal read carries `afterClusterTime=T1` and the node waits until its
**clock** reaches T1 before answering — but it does **not** verify the data written at
T1 is present. Step 4 advances mongo2's clock past T1 with unrelated activity, so the
`local` read no longer blocks, yet mongo2 never received `X=1` → it returns stale
`X=0`. The clock advance is a real, natural event (any write on the still-writable
minority primary ticks the clock forward); we issue it explicitly only to make the
rare timing deterministic. **The control** re-runs the identical setup with
`readConcern:majority`: that read gates on the majority commit point, which the
minority can never advance, so it blocks → UNAVAILABLE (never stale). This isolates
the read concern as the cause.

## 2.3 Results and explanation

Run each with `./run.sh read-your-writes <config>` (`--control` for the majority-read
control).

**Rollback (`majority/w:1`, same for `local/w:1`):**
```
==> baseline written: rollback-w:1-... = 0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> w:1 write X=1 ACKED on mongo1
==> immediate causal read-back on mongo1: X=1
==> waiting 15s for the majority side to elect mongo3
==> healing partition
=== RYOW / rollback ===
  config:              majority/w:1
  write acknowledged:  True
  read own write:      1
  survived heal (X):   0
  verdict:             VIOLATED (acknowledged write rolled back)
```
The write was acknowledged and read back (X=1), but after the heal it is gone (X=0):
the acknowledged write was **rolled back** because it never reached a majority.

**Safe control (`majority/majority`):**
```
==> majority write X=1 REFUSED on mongo1: ...timed out...
=== RYOW / rollback ===
  config:              majority/majority
  write acknowledged:  False
  verdict:             SAFE (write refused — never falsely acknowledged)
```
The majority write on the minority side is never acknowledged, so there is no write to
lose.

**Divergent read (`local/majority`):**
```
==> raising electionTimeoutMillis to 120000
==> baseline written: divergent-local-... = 0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> waiting up to 175s for mongo3 to become primary
==> Write 1: X=1 w:majority ACKED on mongo3; T1=Timestamp(...)
==> dummy w:1 to mongo1 OK (advances minority clock PAST T1)
==> Read 1: local read on mongo2 (minority): X=0
=== RYOW / divergent read ===
  config:        local/majority (read concern = local)
  verdict:       VIOLATED (read returned stale X=0; clock advanced past T1
                 but mongo2 never received Write 1)
```
The durable write X=1 exists on the majority side, but the causal `local` read on
mongo2 returns stale X=0: its clock passed T1 (so it didn't block) yet it never
received the write.

**Divergent-read control (`local/majority --control`):**
```
=== RYOW / divergent read ===
  config:        local/majority (read concern = majority)
  verdict:       UNAVAILABLE (majority read blocked and hit maxTimeMS: the minority
                 (mongo2) cannot advance the majority commit point)
```
Same setup, `majority` read → blocks rather than returning stale. This is the whole
distinction: `local` gates on the node clock (can be tricked); `majority` gates on the
majority commit point (cannot).

**Summary:**

| readConcern | writeConcern | RYOW | why |
|---|---|---|---|
| majority | majority | holds | write refused / read waits — never stale |
| majority | w:1 | fails | acknowledged write rolls back |
| local | w:1 | fails | acknowledged write rolls back |
| local | majority | fails | causal local read gates on node clock, not data presence |

Write concern controls whether an acknowledged write is durable; read concern controls
whether a read gates on the node clock (`local`) or the majority commit point
(`majority`). RYOW under all conditions needs both — only majority/majority.

## 2.4 Execution flow (for auditing the code)

What `./run.sh read-your-writes <config>` actually does, in order:

1. **`experiments/run.sh`** (entrypoint) parses `<config>` and installs a
   `trap ... EXIT` cleanup handler — so no matter how the run ends (success, error,
   Ctrl-C) it will heal the partition and restore defaults.
2. **`ensure_mongo1_primary()`** (in `run.sh`) → calls **`scripts/heal-split.sh`** to
   clear any leftover partition, then steps down whoever is primary until **mongo1** is
   PRIMARY. This gives every config the same known starting state.
3. **`uv run models/read_your_writes.py --config <config>`** launches the experiment.
   The Python dispatches on the config:
   - `majority/majority`, `majority/w:1`, `local/w:1` → `rollback(...)`
   - `local/majority` → `divergent(...)` (add `--control` for the majority-read control)

   **rollback() path** (uses helpers from `lib.py`):
   - writes baseline `X=0` durably to mongo1;
   - **`partition_minority()`** → runs **`scripts/partition-split.sh mongo1 mongo2`**
     to split minority (mongo1+mongo2) from majority (mongo3+4+5);
   - writes `X=1` with the config's write concern to mongo1 and reads it back;
   - waits ~15s for mongo3 to be elected, prints state;
   - **`heal()`** → runs **`scripts/heal-split.sh`**; re-reads `X` and prints the verdict.

   **divergent() path** (uses helpers from `lib.py`):
   - **`set_election_timeout(120000)`** (via `docker exec ... mongosh rs.reconfig`) so
     mongo1 stays writable on the minority side;
   - writes baseline `X=0`; **`partition_minority()`** (→ `partition-split.sh`);
   - **`wait_primary("mongo3", 175)`** polls until mongo3 is elected;
   - writes `X=1` `w:majority` to mongo3 (captures T1); issues the dummy `w:1` write to
     mongo1 (advances the minority clock past T1); reads `X` on mongo2 and prints the verdict;
   - in a `finally`: **`heal()`** (→ `heal-split.sh`) and
     **`set_election_timeout(10000)`** to restore the default. (This is where the
     timeout is actually restored; the EXIT trap below only repeats it as a safety net.)
4. **`run.sh`'s EXIT trap** fires last as a redundant safety net: **`scripts/heal-split.sh`**
   again + a `mongosh rs.reconfig` re-setting `electionTimeoutMillis=10000` (in case the
   Python process died before its own `finally` ran). So the cluster is always left
   clean for the next run.

Scripts invoked, at a glance: `run.sh` → `heal-split.sh` (pre-clean) →
`partition-split.sh` (induce fault) → `heal-split.sh` (recover) → `heal-split.sh` +
`rs.reconfig` (EXIT cleanup). `up.sh`/`down.sh` are run manually to bring the cluster
up/down and are not called by `run.sh`.

## 2.5 Expectations vs. observations, and limitations

**Agreement.** Observations match the prediction from the MongoDB causal-consistency
docs exactly: only `readConcern:majority` + `writeConcern:majority` upholds RYOW; the
other three violate it. The two mechanisms also match theory — write-concern failures
are rollbacks, the read-concern failure is a divergent stale read.

**Limitations (stated honestly).**
- **Timing dependence of 1B.** The divergent-read failure exploits a real but rare
  race (old primary still writable while the new primary is elected). We inflate
  `electionTimeoutMillis` to widen it, but the run is still **~90% reliable** — it can
  occasionally return INCONCLUSIVE and need a re-run. The underlying scenario also
  occurs at the default timeout, just too rarely to catch on demand.
- **Deterministic priorities.** We fix mongo1 as primary and mongo3 as failover so the
  partition sides are predictable. This is a controlled simplification, not a
  restriction of the result.
- **Manufactured determinism.** The dummy `w:1` write in 1B forces a naturally-occurring
  clock advance to happen on cue; MongoDB itself is unmodified (no failpoints).
- **Single-key, single-client.** The experiments use one key and one client session to
  make the violation observable; they demonstrate the failure exists, not its frequency
  under real workloads.
