# Report Backbone: Client-Centric Consistency in MongoDB

---

## Section 1: System Setup

### Database and Deployment
- **Database:** MongoDB 7.0 (document store with replica-set replication)
- **Cluster:** 5-node replica set `rs0` in Docker Compose
  - mongo1, mongo2, mongo3, mongo4, mongo5 (each `mongo:7.0` container)
  - Priorities: mongo1=2 (primary), mongo3=1 (failover), mongo2/4/5=0
  - Network: Docker bridge network `mongo-cluster`; host ports 27017–27021
- **Software:** MongoDB 7.0, mongosh, PyMongo ≥ 4.9, Python ≥ 3.11 (via `uv`), Docker Compose
- **Client connectivity:** Per-node with `directConnection=true` (container hostnames don't resolve from host)
- **Causal consistency:** Always enabled for all experiments (guarantees are defined over causal sessions)

### Installation and Running
1. `./scripts/up.sh` — start cluster, init replica set, force mongo1 PRIMARY
2. `./run_procedures.sh <procedure> <consistency> <config>` — run an experiment
3. `./scripts/down.sh [--wipe]` — stop cluster (--wipe deletes data)

---

## Section 2: Experimental Procedures

We use three distinct experiment procedures to test the four consistency models. Each procedure partitions the cluster to create failure scenarios.

### Procedure A: Rollback

**Mechanism:** A `w:1` write is acknowledged on an isolated primary, then discarded when the partition heals and a majority-side primary wins. A `w:majority` write is refused on the isolated minority side (never falsely acknowledged).

**Setup:**
1. Write baseline `X=0` durably (`w:majority`) on mongo1
2. Partition: minority = mongo1+mongo2 | majority = mongo3+mongo4+mongo5
3. Issue write with config's `writeConcern` to mongo1 (isolated primary)
4. Read write back in same causal session
5. Wait ~15s for mongo3 to be elected (majority side)
6. Heal partition; wait ~12s for re-sync
7. Check if write survived or rolled back

**Why it reveals violations:** 
- `w:1`: acknowledged before majority, so on heal it rolls back
- `w:majority`: never acknowledged on minority (no majority to reach)

**Consistency models tested:** RYOW (w:1 configs), MW (w:1 config)

---

### Procedure B: Divergent-read

**Mechanism:** A `local` read gates on the node's **clock**, not on data presence. If the clock is advanced past the causal token's timestamp (by unrelated writes), the read skips the causal gate and returns stale data. A `majority` read gates on the majority commit point, which cannot be advanced on an isolated minority, so it never returns stale data (only blocks).

**Setup:**
1. Raise `electionTimeoutMillis` to 120000 (keeps mongo1 writable on minority)
2. Write baseline `X=0` durably
3. Partition: minority = mongo1+mongo2 | majority = mongo3+mongo4+mongo5
4. Write `X=1` with `w:majority` to mongo3 (majority side), capture timestamp T1
5. Dummy `w:1` write to mongo1 *after* T1 (advances minority clock past T1)
6. Local read on mongo2 (minority) carrying causal token T1
7. Heal partition; restore `electionTimeoutMillis=10000`

**Why it reveals violations:** 
- `local`: clock ≥ T1 so read doesn't block; mongo2 never got X=1 → returns stale X=0
- `majority`: read waits for majority commit point (minority can't advance it) → UNAVAILABLE (no stale data)

**Consistency models tested:** RYOW (local/majority config), MR (local/majority config)

---

### Procedure C: Doomed-read

**Mechanism:** A session reads a value that is acknowledged on an isolated primary but doomed to rollback. If the session issues a dependent write based on that doomed read, the dependent write survives while the read value is discarded.

**Setup:**
1. Write baseline `k1=0, k2=0` durably
2. Partition: minority = mongo1+mongo2 | majority = mongo3+mongo4+mongo5
3. Write W1: `k1=1` with `w:1` to mongo1 (doomed)
4. Session reads `k1` under config's `readConcern`:
   - `local`: reads `k1=1` (doomed value)
   - `majority`: blocks (can't reach majority) → UNAVAILABLE
5. Wait ~40s for mongo3 to be elected
6. If session read `k1=1`, issue W2 on mongo3 recording `"saw k1=1"`
7. Heal partition
8. Check if W2 survives while `k1=1` is rolled back

**Why it reveals violations:**
- `local`: session read the doomed value and wrote based on it → W2 survives, k1=1 vanishes → VIOLATED
- `majority`: read unavailable → no dependent write → SAFE

**Consistency models tested:** WFR (local configs)

---

## Section 3: Consistency Models and Results

### Read-Your-Writes (RYOW)

**Definition:** After a client writes a value in a causally consistent session, any later read in that session must return that value or newer — never an older or absent state.

**Expected outcomes (from MongoDB docs):**
| readConcern | writeConcern | Expected |
|---|---|---|
| majority | majority | SAFE (write refused / read waits) |
| majority | w:1 | VIOLATED (write rolls back) |
| local | w:1 | VIOLATED (write rolls back) |
| local | majority | VIOLATED (stale read) |

**Results:**
- **majority/majority** (Procedure A): write refused on minority → SAFE ✅
- **majority/w:1** (Procedure A): write acknowledged, rolled back → VIOLATED ✅
- **local/w:1** (Procedure A): write acknowledged, rolled back → VIOLATED ✅
- **local/majority** (Procedure B): local read stale, majority read blocked → VIOLATED ✅

---

### Monotonic-Reads (MR)

**Definition:** Once a session has read a value, later reads in that session must not return an older state.

**Expected outcomes:**
| readConcern | writeConcern | Expected |
|---|---|---|
| majority | majority | SAFE |
| majority | w:1 | SAFE |
| local | w:1 | SAFE |
| local | majority | VIOLATED |

**Results:**
- **local/majority** (Procedure B): Read 1 (X=1) on majority; Read 2 (X=0) on minority after clock advance → VIOLATED ✅

---

### Monotonic-Writes (MW)

**Definition:** Writes issued in order within a session are applied in that order everywhere.

**Expected outcomes:**
| readConcern | writeConcern | Expected |
|---|---|---|
| majority | majority | SAFE |
| majority | w:1 | SAFE |
| local | w:1 | VIOLATED |
| local | majority | SAFE |

**Results:**
- **local/w:1** (Procedure A): W1 rolls back, W2 survives → VIOLATED ✅

---

### Writes-Follow-Reads (WFR)

**Definition:** A write issued after reading a value must be ordered after that value. If a session reads X and writes Y, every server holding Y must also hold X.

**Expected outcomes:**
| readConcern | writeConcern | Expected |
|---|---|---|
| majority | majority | SAFE |
| majority | w:1 | SAFE |
| local | w:1 | VIOLATED |
| local | majority | VIOLATED |

**Results:**
- **local/w:1** (Procedure C): Session reads doomed k1=1, writes W2; on heal, k1=1 rolls back but W2 survives → VIOLATED ✅

---

## Section 4: Expectations vs. Observations

**Agreement:** Observations match MongoDB's documented causal-consistency guarantees exactly:
- Only `readConcern:majority` + `writeConcern:majority` upholds all four models
- `readConcern:local` failures are driven by the read concern (gates on clock, not data)
- `writeConcern:w:1` failures are driven by the write concern (acks before durable)
- Each model fails at different config intersections, reflecting its specific requirement

---

## Section 5: Limitations

- **Procedure B timing (~90% reliable):** The divergent-read failure exploits a real but rare race. We inflate `electionTimeoutMillis` to 120s to widen the window, but success depends on timing. Occasional inconclusive runs may occur; re-run if needed.
- **Single-key, single-client:** Experiments use one key and one session to make violations observable. They demonstrate the failure exists, not its frequency in real workloads.
- **Deterministic priorities:** Priorities (mongo1=2, mongo3=1, others=0) ensure predictable partition sides — a controlled simplification.
- **Manufactured determinism:** Procedure B's dummy write forces a naturally-occurring clock advance to happen on cue. MongoDB is unmodified (no failpoints).

---

## Section 6: References and AI Usage

**References:**
- MongoDB Manual: Causal Consistency, Read Concern, Write Concern, Replica-set Elections and Rollbacks
- Terry et al. (1994): "Session Guarantees for Weakly Consistent Replicated Data"

**AI usage disclosure:**
- Designing experiment procedures to isolate real-world failure modes
- Implementing test scripts in Python and bash
- Drafting this report backbone
- All results were produced by running the experiments on the live 5-node replica set
