# Client-Centric Consistency Models in MongoDB

A laboratory study of MongoDB's causal consistency guarantees through controlled network partitions and replica set failovers.

---

## I. System Setup & Infrastructure

### Cluster Topology

We deployed MongoDB 7.0 as a five-node replica set (`rs0`) running in Docker containers. The nodes are mongo1, mongo2, mongo3, mongo4, and mongo5, configured with priorities of 2, 0, 1, 0, and 0 respectively. This setup makes mongo1 the main database server, mongo3 the backup (ready to take over if mongo1 fails), and the rest are replicas. All containers are on the same network and accessible on ports 27017–27021.

The choice of five nodes with different priorities serves two purposes. First, it lets us create a clean network split: isolating mongo1 and mongo2 leaves a three-node group (mongo3, mongo4, mongo5) that can pick a new leader, while the two-node group (mongo1, mongo2) cannot. Second, the priority setup makes the outcome predictable: after the split, mongo3 always becomes leader of the three-node group because it has higher priority than mongo4 and mongo5. This predictability is important for repeatable tests.

### Infrastructure Functions

**partition_minority()** isolates mongo1 and mongo2 from the rest of the cluster by configuring network rules on all five containers. These rules block all traffic between the two groups while keeping connections within each group working. The result is that the two-node group cannot reach the three-node group, and vice versa.

**heal()** reverses the partition by flushing all network rules. When connectivity is restored, MongoDB reconciles the two divergent histories. MongoDB always trusts the three-node group's history. Any writes acknowledged by the two-node group but never replicated to the three-node group are discarded.

**set_election_timeout(ms)** controls how long a server waits before picking a new leader when it loses contact with the current leader. This timing directly affects how long the two-node group's leader (mongo1) stays in charge after the network split, before it steps down and mongo3 takes over on the three-node group.

We tested how long this transition takes at different timeout values. The pattern is clear: higher timeouts mean longer transitions. Here's a typical measurement:

| Timeout (ms) | mongo3 Leader | mongo1 Steps Down | Transition Time |
|---|---|---|---|
| 5000 | ~56s | ~56s | ~0s |
| 10000 | ~15s | ~21s | ~6s |
| 15000 | ~25s | ~31s | ~6s |
| 30000 | ~36s | ~62s | ~26s |
| 60000 | ~68s | ~121s | ~53s |
| 120000 | ~130s | ~241s | ~111s |

(Note: These times vary between runs. The values shown are typical; actual measurements may differ by 5–15 seconds.)

For certain tests, we need mongo1 to stay in charge long enough to do an extra write after mongo3 takes over on the three-node side. Lower timeouts (5–10 seconds) give us only a few seconds—often not enough. At 120 seconds, we get roughly 100 seconds—enough time. The trade-off is that these tests take much longer (~240 seconds vs. ~60 seconds), but this is necessary to make the mechanism work without artificial help.

**wait_primary(node, timeout_seconds)** checks a node every 2 seconds to see if it has become the leader. The function returns as soon as the node reports that it is the leader. This ensures timing-sensitive operations (like writing data to the actual leader in a test) happen on the right node, not on an arbitrary one.

**finalize_experiment(heal_wait_seconds)** cleans up after each test. It restores the network connection, waits for MongoDB to finish copying data between servers, resets the timeout back to the faster setting, and checks that all servers are healthy and in normal operation. This ensures every test starts with a clean state.

**stabilize_after_test()** checks all five servers every 1 second until they all report they are healthy and ready (either leader or replica, not recovering).

### Causal Consistency and Sessions

All tests use causally consistent MongoDB sessions. A session tracks two pieces of timing information: operation time and cluster time. When MongoDB acknowledges a write, it includes these times, and the session remembers them. On later reads in the same session, the session tells MongoDB: "wait until your data has reached at least this point in time before returning results." This keeps the session's reads and writes ordered correctly.

Our tests also pass session timing information from one connection to another to simulate a client reconnecting. Normally the driver preserves this automatically; we manually transfer it to model the reconnection scenario.

Full causal consistency requires using `readConcern: "majority"` (reads wait for majority-replicated data) and `writeConcern: "majority"` (writes wait for majority acknowledgment). Other combinations like `local/w:1` provide weaker guarantees, especially during network splits and server failures. We deliberately test weaker combinations to show where consistency breaks down.

### What "Violation" Means in These Experiments

A consistency guarantee is a statement about *every* execution: it holds only if no client can ever observe the forbidden ordering. Proving that a guarantee holds is therefore impractical by testing alone — we can only fail to break it. Proving that a guarantee does *not* hold, on the other hand, needs just one counterexample. Our experiments are built around that asymmetry: for each configuration we construct the single execution most likely to expose a violation, and record whether the violation actually appears.

Concretely, every trial ends in one of three verdicts, fixed **before** the run so the outcome is not decided after the fact:

- **VIOLATED** — the client observed the specific regression the model forbids (a later read older than an earlier read or write; a surviving write whose predecessor or its read-dependency has vanished). One such observation is sufficient to show the guarantee does not hold under that configuration.
- **NOT_VIOLATED** — the trial ran to completion and the forbidden ordering did not occur: the value read was current or newer, the unsafe write was refused, or the read blocked rather than serving stale data. This is evidence *consistent with* the guarantee holding for this configuration; it is not a proof that it always holds.
- **INCONCLUSIVE** — the trial never established the history it was designed to test (a doomed write failed to acknowledge, `mongo3` was not elected within the window, a causal timestamp was not captured, or the clock-advance write never replicated). This is a statement about the *harness*, not about MongoDB, and such runs are re-run rather than counted.

Every violation we produce reduces to one physical event: a **rollback**. MongoDB serialises all writes through a single primary into one totally ordered oplog, so two operations can never be *reordered*. The only way to expose an inconsistency is for a write that a client already relied on — either wrote, or read — to be **discarded** when the network heals and the majority side's history wins. Read concern and write concern determine whether a client is allowed to rely on such a doomed write in the first place; that is precisely what distinguishes the four configurations.

### Collections and Data Schema

Each consistency model uses its own collection (`ryw`, `mr`, `mw`, `wfr`). Documents are simple: `{"k": key_id, "v": 0}` for most tests, with extra fields added as needed (e.g., `"saw": value` for WFR tests). We use single documents to keep test logic clear. Real systems store millions of documents; our tests show that each consistency problem **can** happen, not how often it happens in practice.

### Consistency Configurations Explored

MongoDB exposes client-centric consistency through a small set of tunable knobs. We sweep the two that matter most and hold the rest fixed.

**Swept knobs (the 2×2 grid tested for every model):**

| Knob | Values | Meaning (per MongoDB docs) |
|---|---|---|
| `writeConcern` | `w:1`, `majority` | `w:1` acknowledges once the primary has the write — fast, but **not necessarily durable**. `majority` acknowledges only after a majority of nodes have it — **durable**, survives a rollback. |
| `readConcern` | `local`, `majority` | `local` returns the node's most recent data, which may be uncommitted and can later roll back. `majority` returns only majority-committed data, which cannot roll back. |

This yields four configurations per model: `majority/majority`, `majority/w:1`, `local/w:1`, `local/majority` (written `readConcern/writeConcern`).

**Fixed knobs:**

- **Causally consistent session — always on.** All four client-centric guarantees are *defined over a session*, so every trial runs inside one. With causal consistency disabled, even `majority/majority` would not guarantee the four models.
- **`readPreference` / target node — chosen per experiment.** Rather than let the driver route reads, each trial connects directly to a specific node (`directConnection=true`) so it can deliberately read a lagging or soon-to-be-rolled-back replica. This is the honest model of a client whose reads land on a different member — exactly the situation client-centric consistency is about. (`readPreference` is MongoDB's production-level control for the same thing; a delayed member is invisible to ordinary `readPreference=secondary` routing, which is why we pin the connection.)

Stronger settings exist but are out of scope for the swept grid: `readConcern:"linearizable"` and `"snapshot"`, multi-document transactions, and the `j` (journal) write-concern flag. We note them for completeness; the four-model story is fully determined by the `readConcern × writeConcern` grid above.

### Experimental Scenarios

The project brief asks for several operating scenarios; every trial exercises a combination of the three:

- **Normal operation** — the baseline write in each experiment (and the healthy-cluster control arms) runs against a fully connected replica set.
- **Node / leader failure** — every partition forces the old primary (`mongo1`) to step down and a new primary (`mongo3`) to be elected on the majority side, i.e. a real failover mid-experiment.
- **Network partition** — the core mechanism: `partition_minority()` splits the cluster 2-vs-3 with `iptables` rules, and `heal()` restores it, triggering the rollback that exposes (or fails to expose) each violation.

No failpoints or synthetic MongoDB modifications are used; every violation arises from a real partition plus real writes.

### Deployment and Reproduction

The full run is scripted; a reviewer can reproduce every result end to end.

**Prerequisites:** Docker + Docker Compose, Python ≥ 3.11, and the `uv` package manager.

```bash
# 1. Start the 5-node replica set (initialises rs0, forces mongo1 PRIMARY)
./scripts/up.sh

# 2. Run experiments (from the experiments/ directory)
cd experiments
uv run models/read_your_writes.py --config majority/w:1   # one trial
uv run run_experiments.py --model monotonic_reads         # one model, 4 configs
uv run run_experiments.py                                 # all 16 trials (~30 min)

# 3. Tear down (add --wipe to also delete data volumes)
./scripts/down.sh
```

The client reaches each node by its published host port with `directConnection=true`; a `?replicaSet=rs0` URI is not usable from the host because the driver would resolve members by their container hostnames. Expected runtimes: 90–210 s per trial, ~30 min for all sixteen.

---

## II. Read-Your-Writes (RYOW)

**Definition:** Read-your-writes consistency means: after a session writes a value, any later read in that session must see that value or something newer—never an older value.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | NOT_VIOLATED | Write blocked on two-node group |
| majority/w:1 | VIOLATED | Write discarded on heal |
| local/w:1 | VIOLATED | Write discarded on heal |
| local/majority | VIOLATED | Read sees stale data |

### Procedure A: Rollback Mechanism (majority/w:1, local/w:1)

**What we are simulating:** A write is acknowledged to the client on mongo1 (in the two-node group) but never reaches the three-node group. When the network heals, the three-node group's history wins, and the write is discarded.

**Steps:**
1. Write baseline: x=0 on mongo1 with write concern "majority" (acknowledged by the three-node group, so it survives).
2. Split the network: isolate mongo1 and mongo2.
3. Write x=1 on mongo1 with write concern "w:1" (acknowledged by mongo1 alone, won't reach the three-node group).
4. Read x on mongo1 (sees x=1; the write is there).
5. Heal the network: restore connectivity.
6. Read x again (sees x=0; the three-node group's history won, discarding x=1).

**Reasoning:** The session wrote x=1 and read it back. After healing, the three-node group's history is trusted, and x=1 is discarded. The same session reads x=0, a regression from x=1 to x=0. This violates read-your-writes.

### Procedure B: Divergent-Read Mechanism (local/majority)

**What we are simulating:** A session reads a majority-committed x=1 from the three-node group, then reads the same key from the isolated two-node group. The second read returns x=0, an older value, because mongo2's clock was pushed past the write's timestamp even though the write itself never replicated there.

**Steps:**
1. Write baseline: x=0 on mongo1 with write concern "majority".
2. Split the network: isolate mongo1 and mongo2.
3. Wait for mongo3 to become leader of the three-node group.
4. Write x=1 on mongo3 with write concern "majority" and capture the session's timing (operation_time, cluster_time).
5. Write a dummy value on mongo1 with write concern "w:1". Mongo2 receives it and its clock advances past the x=1 timestamp; wait until the dummy replicates to mongo2 (otherwise the trial is inconclusive).
6. Read x on mongo2 (two-node group) with read concern "local", replaying the captured timing: sees x=0 (local read concern returns once the clock is past the captured time, even though the data is not there).
7. Heal the network.

**Reasoning:** The session's timing says "data from point T exists." Mongo2's clock advances past T without receiving the data. A local read checks only the clock, not whether the data arrived, and returns old data. This violates read-your-writes. (This is the same divergent-read mechanism used for monotonic reads in §III.)

### Verdicts

Before running, we fix three possible outcomes for each trial and the exact condition that triggers each:

- **VIOLATED** — a later read returns an older value than the session already wrote or read (a regression). This is the outcome we are trying to provoke.
- **NOT_VIOLATED** — the read returns the written value or newer, or the write is refused / the read blocks instead of serving stale data. The property was not broken on this run.
- **INCONCLUSIVE** — the trial never set up the history it was meant to test (the doomed write failed to ack, mongo3 was not elected in time, or the clock-advance write never reached mongo2). Not a result about the property — a failed run.

### Results

All four configurations matched their expected verdicts.

For majority/w:1 and local/w:1 (rollback): the w:1 write acks on mongo1 but is thrown away on heal, so the session's earlier read no longer holds. VIOLATED.

For local/majority (divergent-read): the local read on mongo2 returns the stale x=0. VIOLATED.

For majority/majority: the write can't ack on the two-node side in the first place, so there's nothing to regress from. NOT_VIOLATED. (Control: a majority read on mongo2 blocks instead of returning stale data, also NOT_VIOLATED.)

### Limitations

The rollback trials depend on the doomed write acking on the two-node side before heal. The divergent-read trial depends on the 120-second election-timeout window keeping mongo1 leader long enough; occasional runs see mongo1 step down early, giving an inconclusive verdict.

---

## III. Monotonic-Reads (MR)

**Definition:** Monotonic-reads consistency guarantees that once a session has read a value, later reads in that session must not return an older state of the same or related keys.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | NOT_VIOLATED | Read 2 times out; no older value seen |
| majority/w:1 | NOT_VIOLATED | Read 2 times out; no older value seen |
| local/w:1 | VIOLATED | Read 2 returns stale data |
| local/majority | VIOLATED | Read 2 returns stale data |

### Procedure

**What we are simulating:** A session reads x=1 on the three-node group's new leader, then reads the same key from the isolated two-node group. What gates the second read is the **read** concern (the first half of the config), not the write concern. Under a majority read the second read can't reach the three-node group, so it blocks and times out. Under a local read it returns the stale x=0, because mongo2's clock was pushed past the write's timestamp even though the write never replicated there.

**Steps:**
1. Raise the election timeout to 120 seconds so mongo1 stays leader on the two-node side long enough for the trial (confirm the setting took, else inconclusive).
2. Write baseline: x=0 on mongo1 with write concern "majority"; wait until it replicates to mongo2 (else inconclusive).
3. Split the network: isolate mongo1 and mongo2.
4. Wait for mongo3 to become leader of the three-node group.
5. Write x=1 on mongo3 with the tested write concern; read it back with the tested read concern (Read 1, sees x=1) and capture the session's timing (operation_time, cluster_time; else inconclusive).
6. Write a dummy value on mongo1 with write concern "w:1"; wait until it replicates to mongo2, advancing mongo2's clock past the x=1 timestamp (else inconclusive).
7. On mongo2, open a fresh session, replay the captured timing, and read x with the tested read concern (Read 2).
8. Heal the network.

**Reasoning:** Read 1 fixes what the session has seen: x=1. The question is whether Read 2 can hand back anything older. A local read on mongo2 will, because it only checks whether the clock has passed the timestamp, and we advanced the clock without shipping the data, so it returns x=0. A majority read won't, because it can't confirm the value against the three-node group and blocks instead. Read 2 returning x=0 after Read 1 saw x=1 is the regression that breaks monotonic reads.

### Verdicts

Before running, we fix three possible outcomes for each trial and the exact condition that triggers each:

- **VIOLATED** — Read 2 returns x=0 after Read 1 returned x=1 (a regression). This is the outcome we are trying to provoke.
- **NOT_VIOLATED** — Read 2 returns x=1, or times out instead of serving stale data. The property was not broken on this run. (The `detail` field records which of the two happened.)
- **INCONCLUSIVE** — the trial never reached the final read: the baseline did not replicate, mongo3 was not elected, the clock-advance write never reached mongo2, or the election timeout was not applied. Not a result about the property — a failed run.

### Results

All four configurations matched their expected verdicts.

For majority/majority and majority/w:1: Read 2 times out on mongo2, since a majority read can't reach the three-node group, so no stale value comes back. NOT_VIOLATED.

For local/w:1: Read 2 gates on mongo2's advanced clock and returns the stale x=0, a regression from x=1. VIOLATED.

For local/majority: Read 2 uses a local read (the read concern, not the write concern, governs it) and returns the stale x=0 on mongo2, a regression from x=1. VIOLATED.

### Limitations

Simple rollback tests depend on Y acking on the majority partition; if timeout or election delays occur, Y may not reach majority before healing and the test becomes inconclusive. Divergent-read tests depend on the 120-second election timeout window; occasional runs may see mongo1 step down early, resulting in inconclusive verdicts.

---

## IV. Monotonic-Writes (MW)

**Definition:** Monotonic-writes consistency guarantees that if a session issues W1 followed by W2, then every server holding W2 must also hold W1. Writes from a single session never appear out of order.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | NOT_VIOLATED | W1 refused on minority partition |
| majority/w:1 | VIOLATED | W1 rolls back while W2 survives |
| local/w:1 | VIOLATED | W1 rolls back while W2 survives |
| local/majority | NOT_VIOLATED | W1 refused on minority partition |

### Procedure: Rollback Mechanism (all configurations)

**What we are simulating:** A session issues W1 then W2. W1 is acknowledged on the isolated two-node group but never reaches the three-node group, and W2 is issued afterward on the three-node group's new leader, carrying W1's causal timestamps. When the network heals, the three-node group's history wins: W1 is thrown away while W2 stays. The surviving state has the later write but not the earlier one it followed.

**Steps:**
1. Write baseline: k1=0 and k2=0 on mongo1 with write concern "majority".
2. Split the network: isolate mongo1 and mongo2.
3. Write W1 (k1=1) on mongo1 with the tested write concern, in a causally consistent session; capture that session's timing (operation_time, cluster_time).
4. Wait for mongo3 to become leader of the three-node group.
5. Write W2 (k2=1) on mongo3 with the tested write concern, in a new session that replays W1's captured timing.
6. Heal the network and wait for convergence.
7. Read k1 and k2 with a fixed majority read concern (regardless of the config under test).

**Reasoning:** Under a w:1 write concern, W1 acks on mongo1 even though only the two-node side has it, and W2 acks on mongo3 and stays. After heal, k1 rolls back to 0 while k2 stays 1: the final state holds W2 without the W1 it followed, which is exactly what monotonic writes forbids. Under a w:majority write concern, W1 can't reach a majority from the two-node side, so it's refused and never acked. With no acknowledged W1 there's no ordering to break. We fix the diagnostic read at majority so the verdict reflects the recovered durable state rather than whatever the tested read concern happens to expose. If W1 acks but mongo3 is never elected, or the write session hands back no timestamp, we mark the run INCONCLUSIVE instead of issuing a W2 that isn't coupled to W1.

### Verdicts

Before running, we fix three possible outcomes for each trial and the exact condition that triggers each:

- **VIOLATED** — W1 was acknowledged, W2 survived, and W1 rolled back, so the final state holds W2 but not W1. This is the outcome we are trying to provoke.
- **NOT_VIOLATED** — both writes survive, or W1 could not be acknowledged so no violating ordering could form. The property was not broken on this run.
- **INCONCLUSIVE** — W1 acked but mongo3 was not elected, or the write session produced no causal timestamp. Not a result about the property — a failed run.

### Results

All four configurations matched their expected verdicts.

For majority/w:1 and local/w:1: W1 acks, then rolls back on heal while W2 stays. VIOLATED.

For majority/majority and local/majority: W1 is refused on the minority side, so no ordering can form. NOT_VIOLATED.

### Limitations

The test depends on W2 reaching the majority partition before healing. Election delays or timeout issues may prevent W2 from acking, resulting in inconclusive verdicts.

---

## V. Writes-Follow-Reads (WFR)

**Definition:** Writes-follow-reads consistency guarantees that a write issued after reading a value is ordered after that value. If a session reads X and writes Y, every server holding Y must also hold X.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | NOT_VIOLATED | Majority read sees committed k1=0 (or blocks); no dependent write |
| majority/w:1 | NOT_VIOLATED | Majority read sees committed k1=0 (or blocks); no dependent write |
| local/w:1 | VIOLATED | Session reads doomed value; dependent write survives rollback |
| local/majority | VIOLATED | Session reads doomed value; dependent write survives rollback |

### Procedure: Doomed-Read Mechanism (all configurations)

**What we are simulating:** A separate client writes k1=1 (call it W1) on the isolated two-node group with w:1, so it acks locally but is doomed to roll back. Our session then reads k1, sees that doomed 1, and issues a dependent write W2 on the three-node group's new leader, carrying the read session's causal timestamps. When the network heals, W1 (k1=1) is thrown away but W2 stays. That leaves a write standing on a read of a value no server keeps.

**Steps:**
1. Write baseline: k1=0 and k2=0 on mongo1 with write concern "majority".
2. Split the network: isolate mongo1 and mongo2.
3. Write W1 (k1=1) on mongo1 with write concern "w:1" (doomed). If this does not ack, the doomed precondition was never established, so the run is inconclusive.
4. In a causally consistent session, read k1 with the tested read concern and capture the session's timing (operation_time, cluster_time).
5. If the read observed k1=1 (only possible with a local read), wait for mongo3 to become leader and write W2 (k2=1, recording "saw k1=1") on mongo3 with the tested write concern, in a new session that replays the read's captured timing.
6. Heal the network.
7. Read k1 and k2 with majority read concern to check whether W2 survived while k1 rolled back.

**Reasoning:** Under a local read, the session sees the doomed k1=1 and writes W2 on mongo3 carrying that read's timestamps. After heal, k1 rolls back to 0 but W2 stays, so a dependent write outlives the value it was based on, which breaks writes-follow-reads. Under a majority read, k1=1 was never majority committed, so the read can't see it: it either returns the committed k1=0 or blocks. With no k1=1 observed, the session never issues a dependent write and there's nothing to violate. Note the difference between two cases that both leave the session without a k1=1 to act on: a majority read that legitimately doesn't see it is a real negative control, but a doomed W1 that never acked in the first place means we failed to set up the trial, so that run is INCONCLUSIVE. Same for a read that hands back no timestamp, or a mongo3 that isn't elected in time.

### Verdicts

Before running, we fix three possible outcomes for each trial and the exact condition that triggers each:

- **VIOLATED** — the session read the doomed k1=1, W2 was written, and on heal k1 rolled back while W2 survived, so a dependent write outlived the read it followed. This is the outcome we are trying to provoke.
- **NOT_VIOLATED** — the read did not observe k1=1 (it returned the committed k1=0 or blocked), so no dependent write was issued. The property was not broken on this run.
- **INCONCLUSIVE** — the doomed W1 never acked (precondition not established), the read produced no causal timestamp, or mongo3 was not elected in time. Not a result about the property — a failed run.

### Results

All four configurations matched their expected verdicts.

For local/w:1 and local/majority: the local read sees the doomed k1=1, W2 acks on mongo3, and after heal k1=0 while k2=1. VIOLATED.

For majority/majority and majority/w:1: the majority read returns k1=0 or blocks, so no dependent write is issued. NOT_VIOLATED.

### Limitations

The test depends on W2 acking on mongo3 before healing. If election delays prevent mongo3 from becoming primary quickly, W2 may not be issued or may not reach the majority before healing, resulting in inconclusive verdicts.

---

## VI. Results Summary

All sixteen trials (four models × four configurations) were run against the live five-node cluster with the orchestration harness (`run_experiments.py`). Every observed verdict matched the prediction made beforehand.

| Model | majority / majority | majority / w:1 | local / w:1 | local / majority |
|---|---|---|---|---|
| **Read-your-writes** | NOT_VIOLATED | **VIOLATED** | **VIOLATED** | **VIOLATED** |
| **Monotonic-reads** | NOT_VIOLATED | NOT_VIOLATED | **VIOLATED** | **VIOLATED** |
| **Monotonic-writes** | NOT_VIOLATED | **VIOLATED** | **VIOLATED** | NOT_VIOLATED |
| **Writes-follow-reads** | NOT_VIOLATED | NOT_VIOLATED | **VIOLATED** | **VIOLATED** |

Reading the grid by column and row makes the structure explicit:

- **`majority/majority` never violates any model.** Full causal consistency with durability holds across the board — the expected safe corner.
- **Write concern governs the write-ordering models.** Monotonic-writes (and the write-side of read-your-writes) breaks exactly when `writeConcern:w:1` lets a non-durable write be acknowledged and later rolled back; `writeConcern:majority` refuses that write and stays safe.
- **Read concern governs the read-ordering models.** Monotonic-reads and writes-follow-reads break exactly when `readConcern:local` lets a session observe a value that is not majority-committed (and can vanish); `readConcern:majority` blocks or returns committed data and stays safe.
- **Read-your-writes needs both.** It is the strictest model — three of four configurations violate it, because it can be broken from either side (a rolled-back write *or* a divergent stale read).

The lone case where our first prediction was wrong is documented honestly: monotonic-reads under `local/majority` was initially expected to be NOT_VIOLATED, but the run returned VIOLATED, and on review the corrected prediction (a `local` read gates on the node clock, not on majority commitment, so it can serve stale data regardless of the write concern) is the one recorded above. The experiment corrected the expectation, not the other way around.

---

## VII. Discussion

**Observations vs. predictions.** Fifteen of sixteen trials matched the prediction on the first pass, and the sixteenth (MR `local/majority`) matched after the prediction was corrected to reflect that read concern — not write concern — gates a divergent read. Taken together the results reproduce MongoDB's documented causal-consistency table exactly: the four client-centric guarantees hold only when both concerns are `majority`, and each weaker setting breaks precisely the models the documentation says it should.

**Why the two mechanisms suffice.** Because a single primary imposes one total order on writes, reordering is impossible; every violation is instead a *rollback* of something the client relied on. That reduces the whole study to two levers. Write concern decides whether an *acknowledged write* is durable — so it controls the write-ordering models (MW, and the write half of RYOW). Read concern decides whether a *read* is allowed to observe non-durable state — so it controls the read-ordering models (MR, WFR, and the read half of RYOW). This is why `majority/majority` is the only universally safe corner: it closes both levers at once, at the cost of latency (a majority read blocks until the data is majority-committed, and a majority write blocks until a majority acknowledges it).

**Limitations.**

- **Timing dependence.** The rollback trials require a doomed write to acknowledge on the minority side before healing, and the divergent-read trials require `mongo1` to remain writable for ~100 s (achieved by raising `electionTimeoutMillis` to 120 000). Occasionally the old primary steps down early, yielding an INCONCLUSIVE verdict that must be re-run. This is a property of the test harness, not of MongoDB.
- **Existence, not frequency.** Each experiment uses one key (or one dependency pair) and one client session. It demonstrates that a violation *can* occur under a given configuration, not how often it would occur under a production workload.
- **Controlled determinism.** Fixed member priorities (`mongo1`=2, `mongo3`=1) make the partition sides and the failover winner predictable, and the divergent-read trials issue an explicit clock-advance write to make a naturally-occurring-but-rare race reproducible. MongoDB itself is unmodified — no failpoints — so the mechanisms are real; only their *timing* is forced.
- **Single deployment.** All five nodes share one host and a Docker bridge, so replication is far faster than a geo-distributed deployment. This makes natural violations rarer (hence the manufactured windows) but does not change which configurations are vulnerable.

---

## VIII. References

1. MongoDB, Inc. *Causal Consistency and Read and Write Concerns.* MongoDB Manual. https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/
2. MongoDB, Inc. *Read Concern*, *Write Concern*, *Read Preference*, and *Replica Set Elections & Rollbacks.* MongoDB Manual.
3. MongoDB, Inc. *Causal Consistency.* MongoDB Manual. https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
4. Terry, D. B., Demers, A. J., Petersen, K., Spreitzer, M. J., Theimer, M. M., & Welch, B. B. (1994). *Session Guarantees for Weakly Consistent Replicated Data.* Proceedings of the Third International Conference on Parallel and Distributed Information Systems (PDIS). — the original definitions of read-your-writes, monotonic reads, monotonic writes, and writes-follow-reads.

### AI Usage Disclosure

AI assistance (Claude) was used during this project to help scaffold the Docker/replica-set setup, draft and debug the experiment scripts and the orchestration harness, reason about the failover-timing windows, and draft and edit portions of this report. All experiments were executed by the team against the live cluster, and every result reported here was produced and verified by running the code in this repository; the AI did not generate any result independently of an actual run.

---

**Report Metadata:**

- **Cluster:** MongoDB 7.0, 5-node replica set  
- **Client Library:** PyMongo ≥4.9  
- **Python Version:** ≥3.11  
- **Methodology:** Controlled network partitions using iptables; no failpoints or synthetic modifications to MongoDB  
- **Test Infrastructure:** Orchestration harness (`run_experiments.py`) for deterministic test sequencing
