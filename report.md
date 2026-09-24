# Client-Centric Consistency Models in MongoDB

A laboratory study of MongoDB's causal consistency guarantees through controlled network partitions and replica set failovers.

---

## I. System Setup & Infrastructure

### A. Cluster Topology

**Configuration:**
- MongoDB 7.0 replica set `rs0` with 5 nodes: mongo1, mongo2, mongo3, mongo4, mongo5
- Priorities: mongo1=2 (designated PRIMARY), mongo3=1 (failover), mongo2/4/5=0 (secondaries)
- Deployment: Docker Compose on a single host; containers connected via bridge network `mongo-cluster`
- Port mapping: mongo1→27017, mongo2→27018, mongo3→27019, mongo4→27020, mongo5→27021

**Why this topology?**
- Priorities ensure predictable partition outcomes: mongo3 (priority 1) always wins the majority side (3 nodes) after partition isolates mongo1+mongo2 (minority)
- Single host avoids real network latency; we inject failures deterministically via iptables
- 5 nodes (vs 3) provides both a writable minority and a quorum majority, enabling clearer failure modes

### B. Key Infrastructure Functions

#### `partition_minority()`
- **What it does:** Executes `/scripts/partition-split.sh`, which uses `docker exec` + `iptables` to drop all traffic between mongo1+mongo2 and mongo3+4+5
- **Effect on topology:** Creates two isolated networks:
  - Minority: mongo1 (PRIMARY, priority 2) + mongo2 (SECONDARY, priority 0)  
  - Majority: mongo3 (will become PRIMARY, priority 1) + mongo4, mongo5 (secondaries)
- **Why:** Simulates a network partition while preserving intra-group replication
- **Cleanup:** Reversed by `heal()`

#### `heal()`
- **What it does:** Executes `/scripts/heal-split.sh`, which flushes all iptables rules on all 5 nodes
- **Effect:** Restores connectivity; MongoDB discovers both partitions and merges histories
- **Behavior on heal:**
  - MongoDB recognizes one partition is majority (mongo3+4+5 with 3 nodes)
  - Majority partition's history becomes canonical
  - Minority partition's un-replicated writes (w:1) are **rolled back**
  - Minority nodes re-sync to majority's committed history
- **Why:** Simulates partition recovery in production; demonstrates durability vs. false acknowledgments

#### `set_election_timeout(ms)`
- **What it does:** Reconfigures `electionTimeoutMillis` on the replica set via `rs.reconfig()`
- **Parameters:**
  - Default (after heal): 5000ms (fast elections)
  - Divergent-read tests: 120000ms (slow elections)
- **Behavior:**
  - **5s timeout:** mongo1 steps down within ~5-10s after partition (no PRIMARY in minority after failover)
  - **120s timeout:** mongo1 **remains PRIMARY** on minority side during partition (no election triggered)
- **Why 120s for divergent-read?**  
  We need mongo1 to stay writable so we can issue clock-advancing writes to the minority. At 5s, mongo1 steps down too early; at 120s, we have a ~175s window before failover.  
  Trade-off: test takes longer but enables the clock-skew failure mode to manifest naturally.

#### `wait_primary(node, timeout_seconds) -> bool`
- **What it does:** Polls `node.admin.command('hello').isWritablePrimary` every 2s until true or timeout
- **Returns:** True if node becomes PRIMARY within timeout, False if timeout expires
- **Why:** Replaces hardcoded sleeps; ensures timing-sensitive operations (e.g., W2 write) happen on the actual PRIMARY
- **Example:** After partition, `wait_primary(mongo3, 40)` blocks until mongo3 is PRIMARY (usually ~10-20s), then proceeds immediately rather than sleeping 15s

#### `finalize_experiment(heal_wait_seconds=12)`
- **What it does:**
  1. Call `heal()` (flush iptables)
  2. Sleep `heal_wait_seconds` (default 12s) for re-sync to complete
  3. Call `set_election_timeout(5000)` (restore fast elections)
  4. Call `stabilize_after_test()` (wait for cluster to be fully healthy)
- **Why:** Deterministic cleanup between tests; ensures next test starts with a known good state

#### `stabilize_after_test(timeout_seconds=30)`
- **What it does:** Polls all 5 nodes every 1s until all report healthy (SECONDARY or PRIMARY states)
- **Why:** Some tests transition the cluster state quickly; ensures we don't start the next test mid-election or mid-sync

### C. Causal Consistency & Sessions

**Session model:**
- Every test uses `client.start_session(causal_consistency=True)`
- Causal sessions track two tokens: `operation_time` (write ordering) and `cluster_time` (logical clock)
- All reads and writes in the session are automatically ordered after prior operations

**Token advancement:**
- When a client reconnects (e.g., switches between nodes within same partition), the session carries its tokens forward
- We manually call `session.advance_operation_time(T1)` and `session.advance_cluster_time(T1)` before a read to simulate this
- Why manual advancement? It represents realistic client behavior: a session that connected to one node (captured T1), then reconnected to another node (e.g., due to timeout or load balancing)

**Why causal sessions?**
- Client-centric consistency models (RYOW, MR, MW, WFR) are **session-level** guarantees
- Tests must exercise session behavior to verify (or violate) these guarantees

### D. Collections & Schema

- **Collections:** `ryw` (RYOW tests), `mr` (MR tests), `mw` (MW tests), `wfr` (WFR tests)
- **Document schema:**
  ```json
  {"k": "unique-key-id", "v": 0, ...model-specific fields...}
  ```
- **Why single-key documents?** Simplifies test logic and makes failures unambiguous. Real workloads have thousands of keys; these tests demonstrate the failure **can** happen, not frequency.

### E. Timelines & Timeouts Summary

| Phase | Config | Timeout | Why |
|-------|--------|---------|-----|
| Rollback tests (RYOW/MR/MW w:1) | electionTimeoutMillis=5000ms | wait_primary(..., 40s) | mongo1 steps down quickly; we wait for mongo3 to win majority |
| Divergent-read tests (RYOW/MR local/majority) | electionTimeoutMillis=120000ms | wait_primary(..., 175s) | mongo1 stays PRIMARY on minority; we need time for clock to advance |
| General test timeout | any | 300s per model config | Covers longest test (divergent-read at ~210s) + buffer |
| Partition healing | any | 12s sleep | Re-sync window; un-replicated writes roll back |

---

## II. Read-Your-Writes (RYOW)

**Definition:**  
After a session writes a value, any later read in that session must return that value or a newer one—never older or absent.

### Expected Outcomes

| Config | Expected | Mechanism | Why |
|--------|----------|-----------|-----|
| majority/majority | SAFE | rollback | w:majority write refused on minority (no majority reachable); never acked → no rollback |
| majority/w:1 | VIOLATED | rollback | w:1 acks on isolated minority, rolls back on heal |
| local/w:1 | VIOLATED | rollback | same as above; readConcern is irrelevant to rollback |
| local/majority | VIOLATED | divergent-read | local read gates on clock, not data presence; stale read possible |

### Procedure A: Rollback Mechanism

[TODO: Complete based on template below]

**What we're simulating:**  
A w:1 write is acknowledged locally on an isolated PRIMARY, then discarded when the partition heals and the majority's history wins.

**Steps:**
1. Baseline write (X=0, w:majority)
2. Partition minority
3. Write under test (X=1, w:1 or w:majority), session reads it back
4. Wait for mongo3 election
5. Heal partition
6. Verify: final read of X

**Reasoning:**

**Results & Interpretation:**

**Limitations:**

---

### Procedure B: Divergent-Read Mechanism

[TODO: Complete based on template below]

**What we're simulating:**  
A `local` read gates on node clock (clusterTime ≥ afterClusterTime), not actual data presence. If clock advances past causal token without data replicating, stale read occurs.

**Steps:**
1. Raise electionTimeoutMillis to 120000ms
2. Baseline write (X=0, w:majority)
3. Partition
4. Wait for mongo3 election
5. Write 1: X=1 w:majority on mongo3 (capture T1)
6. Dummy write w:1 to mongo1 (advances minority clock past T1)
7. Read with causal tokens on mongo2 (local and majority controls)
8. Heal, restore timeout

**Reasoning:**

**Results & Interpretation:**

**Limitations:**

---

## III. Monotonic-Reads (MR)

**Definition:**  
Once a session has read a value, later reads in that session must not return an older state.

### Expected Outcomes

| Config | Expected | Mechanism | Why |
|--------|----------|-----------|-----|
| majority/majority | HELD | simple rollback | both writes refused or both survive; no regression |
| majority/w:1 | VIOLATED | simple rollback | X rolls back, Y survives → X regresses |
| local/w:1 | VIOLATED | simple rollback | same as above |
| local/majority | VIOLATED | divergent-read | local read returns stale X=0 after write X=1 on majority |

### Procedure A: Simple Rollback Mechanism

[TODO: Fill based on outline]

### Procedure B: Divergent-Read Mechanism

[TODO: Fill based on outline]

---

## IV. Monotonic-Writes (MW)

**Definition:**  
Writes issued in order within a session are applied in order everywhere. A later write never appears without an earlier write.

### Expected Outcomes

| Config | Expected | Mechanism | Why |
|--------|----------|-----------|-----|
| majority/majority | SAFE | rollback | W1 refused on minority (no majority) → never acked |
| majority/w:1 | VIOLATED | rollback | W1 acks, rolls back; W2 survives → W2 without W1 |
| local/w:1 | VIOLATED | rollback | same as above; readConcern does not matter |
| local/majority | SAFE | rollback | W1 refused on minority (no majority) → never acked |

### Procedure: Rollback Mechanism

[TODO: Fill based on outline]

---

## V. Writes-Follow-Reads (WFR)

**Definition:**  
A write issued after reading a value is ordered after that value. If a session reads X and then writes Y, every server holding Y must also hold X.

### Expected Outcomes

| Config | Expected | Mechanism | Why |
|--------|----------|-----------|-----|
| majority/majority | SAFE | doomed-read | session read blocked (X not majority-committed) → no dependent write |
| majority/w:1 | SAFE | doomed-read | same as above |
| local/w:1 | VIOLATED | doomed-read | session reads doomed X=1, writes W2 → W2 survives, X rolls back |
| local/majority | VIOLATED | doomed-read | same as above |

### Procedure: Doomed-Read Mechanism

[TODO: Fill based on outline]

---

## VI. Conclusions

[TODO: Fill with summary, agreement with MongoDB docs, limitations]

---

## Appendix: Running the Experiments

**Single test:**
```bash
cd experiments
uv run models/read_your_writes.py --config majority/w:1
```

**All 4 configs for one model:**
```bash
uv run run_experiments.py --model monotonic_reads
```

**All 16 tests (4 models × 4 configs):**
```bash
uv run run_experiments.py
```

**Expected runtime:**
- Single test: 90–210s (depending on mechanism)
- One model (4 tests): 300–600s
- All 16 tests: ~30 minutes

---

**Report generated:** [date]  
**Cluster:** MongoDB 7.0, 5-node replica set  
**Client:** PyMongo ≥4.9, Python ≥3.11  
**Methodology:** Controlled network partitions + failover simulation (no failpoints; only iptables + time)
