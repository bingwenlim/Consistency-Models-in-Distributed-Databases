# Client-Centric Consistency Models in MongoDB

A laboratory study of MongoDB's causal consistency guarantees through controlled network partitions and replica set failovers.

---

## I. System Setup & Infrastructure

### Cluster Topology

We deployed MongoDB 7.0 as a five-node replica set (`rs0`) running in Docker containers. The nodes are mongo1, mongo2, mongo3, mongo4, and mongo5, configured with replica set priorities of 2, 0, 1, 0, and 0 respectively. This priority scheme designates mongo1 as the initial primary, mongo3 as the failover candidate with priority 1, and the remaining nodes as secondaries with no election eligibility. All containers run on the same Docker bridge network (`mongo-cluster`), with host-side access via published ports (27017–27021).

The choice of five nodes with asymmetric priorities serves two purposes. First, it allows us to create a mathematically clean partition: isolating mongo1 and mongo2 leaves a three-node majority (mongo3, mongo4, mongo5) that can elect a new primary, while the two-node minority (mongo1, mongo2) cannot reach quorum. Second, the priority configuration makes election outcomes predictable: after partition, mongo3 wins the majority side deterministically because of its higher priority relative to mongo4 and mongo5. This predictability is essential for test reproducibility.

### Infrastructure Functions

**partition_minority()** isolates mongo1 and mongo2 from the rest of the cluster by executing a script that configures iptables rules on all five containers. These rules drop all TCP traffic between the two groups while preserving connectivity within each group. The effect is a complete network partition: the minority partition cannot reach the majority, and vice versa.

**heal()** reverses the partition by flushing all iptables rules. When connectivity is restored, MongoDB detects the cluster is whole again and reconciles the two divergent histories. MongoDB always trusts the majority partition's history. Any writes acknowledged on the minority partition but never replicated to the majority are rolled back.

**set_election_timeout(ms)** reconfigures the replica set's `electionTimeoutMillis` parameter, which controls how long a node waits before triggering an election when it cannot reach the current primary. This parameter directly affects the timing dynamics during a network partition, particularly the window between when mongo3 (majority side) becomes PRIMARY and when mongo1 (minority side) steps down.

We measured election timing empirically at six different timeout values to understand this window. The pattern is clear: as the election timeout increases, the window widens significantly. The table below shows one representative measurement run:

| electionTimeoutMillis | mongo3 PRIMARY | mongo1 STEPDOWN | Window |
|---|---|---|---|
| 5000 | ~56s | ~56s | ~0s |
| 10000 | ~15s | ~21s | ~6s |
| 15000 | ~25s | ~31s | ~6s |
| 30000 | ~36s | ~62s | ~26s |
| 60000 | ~68s | ~121s | ~53s |
| 120000 | ~130s | ~241s | ~111s |

(Note: These timings vary between runs depending on system load and container overhead. The values shown represent a typical run; actual measurements may differ by 5–15 seconds.)

For divergent-read tests, we need mongo1 to remain PRIMARY on the isolated minority long enough to issue a clock-advancing write after mongo3 becomes PRIMARY on the majority side. At lower timeouts (5,000–10,000 milliseconds), the window is essentially nonexistent or only a few seconds wide—too narrow to reliably perform the clock-advance operation. At 120,000 milliseconds, the window expands to roughly 100–120 seconds, providing ample time to issue the necessary write. The trade-off is that divergent-read tests run significantly longer (~240 seconds vs. ~60 seconds for rollback tests), but this extended timeout is empirically necessary to make the clock-skew mechanism observable without artificial injection.

**wait_primary(node, timeout_seconds)** polls a node's `admin.command('hello')` response every 2 seconds, checking whether it reports itself as the writable primary. The function returns immediately once the node reports PRIMARY status. This ensures that timing-sensitive operations (such as writing W2 in a monotonic-writes test) occur on the actual elected primary, not on an arbitrary node.

**finalize_experiment(heal_wait_seconds)** performs cleanup after each test. It calls `heal()` to restore connectivity, sleeps for a configurable duration (default 12 seconds) to allow MongoDB's replication to complete, calls `set_election_timeout(5000)` to restore the fast election timeout, and calls `stabilize_after_test()` to poll until all nodes report a healthy state (either PRIMARY or SECONDARY). This sequence ensures every test starts with a known good cluster state.

**stabilize_after_test()** polls all five nodes every 1 second, waiting until all report themselves as either PRIMARY or SECONDARY (not RECOVERING, not STARTUP).

### Causal Consistency and Sessions

All reads and writes in our tests occur in causally consistent MongoDB sessions created with `client.start_session(causal_consistency=True)`. A client session tracks both an operation time and a cluster time. MongoDB returns these logical times with acknowledged operations, and the driver advances the session's causal state accordingly. For subsequent reads in a causally consistent session, the driver automatically supplies an `afterClusterTime` constraint. A replica-set member servicing such a read must wait until its oplog has reached that time before returning the result, preserving the session's causal ordering.

Our tests also transfer causal state between sessions to simulate a client carrying causal history across connections. Before issuing an operation from another session context, we advance its `operation_time` and `cluster_time` from the previous session. With a single session, the driver normally preserves this state automatically; the test manually transfers it to model a reconnection scenario.

Full durable causal-consistency guarantees require `readConcern: "majority"` and `writeConcern: "majority"`. Other read concern and write concern combinations provide weaker guarantees, particularly under network partitions and rollback scenarios. We deliberately test combinations like `local/w:1` and `majority/w:1` to expose the boundaries where weaker guarantees permit violations.

### Collections and Data Schema

Each of the four consistency models uses its own collection (`ryw`, `mr`, `mw`, `wfr`). Documents are simple: `{"k": unique_key_id, "v": 0}` for most tests, with model-specific fields as needed (e.g., `"saw": value` for WFR). We use single-key documents to keep test logic simple and failure modes unambiguous. Real workloads operate on thousands or millions of keys; our tests demonstrate that each consistency violation **can** occur, not how frequently it occurs in practice.

---

## II. Read-Your-Writes (RYOW)

**Definition:** Read-your-writes consistency guarantees that after a session writes a value, any later read in that session must return that value or a newer one—never an older or absent state.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | SAFE | Write refused on isolated minority |
| majority/w:1 | VIOLATED | Write rolls back on heal |
| local/w:1 | VIOLATED | Write rolls back on heal |
| local/majority | VIOLATED | Local read returns stale data |

### Procedure A: Rollback Mechanism (majority/w:1, local/w:1)

**What we are simulating:** We simulate the scenario where a write is acknowledged locally to a client on an isolated primary but has not yet reached a quorum. When the partition heals, the majority partition's committed history wins, and the write is discarded. This exposes applications that assume all acknowledged writes persist.

We begin by writing a baseline value (X=0) with w:majority to mongo1. This write requires acknowledgment from a majority of nodes (three out of five), so it is durable and will survive the partition. The baseline is essential because it allows us to later verify that the write-under-test rolled back: if the final read returns X=0, we know X=1 was discarded.

Next, we partition the cluster. The partition isolates mongo1 and mongo2 from mongo3, mongo4, and mongo5. At this point, mongo3 (with priority 1) becomes the primary of the majority side. We then open a causal consistency session on mongo1 (the isolated old primary) and write X=1 with the test's write concern. If write concern is w:1, this write is acknowledged immediately after reaching just mongo1 (one node), even though the other four nodes have no knowledge of it. If write concern is w:majority, the write cannot complete because mongo1 can only reach itself and mongo2 (two nodes), which is not a majority. The session then reads X=1 back immediately, confirming it was acknowledged.

We wait approximately 15 seconds (or more precisely, we use `wait_primary()` to detect when mongo3 becomes primary) to ensure the majority side has completed its election. During this time, mongo1's operational lifespan on the isolated minority is limited—after 15 seconds or so, it realizes it cannot reach a quorum and steps down.

Finally, we heal the partition by flushing the iptables rules. MongoDB detects connectivity is restored and immediately recognizes that three nodes (mongo3, mongo4, mongo5) form a quorum and that the canonical history is on the majority side. Mongo1 and mongo2 re-sync to this history. Any write acknowledged on mongo1 but not replicated to the majority—namely, X=1—is discarded. A final read of X from any surviving node will return X=0 (the baseline), confirming that X=1 rolled back.

**Reasoning:** The 15-second wait is calibrated to the default election timeout of 5,000 milliseconds. MongoDB will not step down a primary immediately upon discovering it has lost quorum; instead, it waits up to the election timeout to avoid thrashing during transient network glitches. At 5 seconds, the election timeout expires. Mongo1 may then step down, and mongo3 (already elected on the majority side) is now uncontested as the sole primary. We use 15 seconds to provide a buffer and to ensure that even if the election timing varies, mongo3 has become primary before we heal.

**Results and Interpretation:**

For the majority/majority configuration: The write is refused on the isolated minority because mongo1 cannot reach three nodes (including itself, it can only reach two). The client receives an error, the write is never acknowledged, and there is no rollback—simply a refusal to acknowledge an unsafe write. The verdict is SAFE because RYOW is satisfied: no acknowledged write is lost.

For the majority/w:1 configuration: The write is acknowledged on mongo1 (w:1 means write to 1 node), and the session reads it back. Upon heal, the write rolls back because it never reached the majority. The session observes X=1 in step 3 but X=0 in step 6, violating RYOW. The verdict is VIOLATED.

For the local/w:1 configuration: Identical to majority/w:1. The readConcern (local vs. majority) does not affect rollback behavior; only the writeConcern determines whether the write persists. The verdict is VIOLATED.

### Procedure B: Divergent-Read Mechanism (local/majority)

**What we are simulating:** We simulate a scenario where a local read gates on the node's logical clock (clusterTime) rather than on the actual presence of data. If the clock advances past the causal token's timestamp, the read proceeds without waiting for the data to arrive, returning a stale value. This exposes applications that rely on local reads for consistency.

We begin by raising the election timeout to 120,000 milliseconds. We write a baseline value (X=0) with w:majority, then partition the cluster. We wait for mongo3 to become the primary of the majority partition (using `wait_primary()`).

Next, we write X=1 to mongo3 with w:majority. Mongo3 is the primary of the majority partition, so this write is acknowledged and durable. We capture the causal tokens from this write: the operation_time (T1) and cluster_time. These tokens represent the logical point in the operation stream where X=1 was written.

Mongo2 (a secondary on the isolated minority) does not have X=1 because it is partitioned from mongo3. However, MongoDB's logical clock is separate from data replication. The clock ticks forward on every write, whether or not the write replicates. We now write a dummy value to mongo1 (the isolated primary) with w:1. This write has a timestamp after T1 and replicates to mongo2. When mongo2 receives this write, its local clock (clusterTime) advances past T1, even though it never received X=1.

We then read X on mongo2 using a causal session. We manually advance the session's tokens to T1 to model a client that connected to mongo3 earlier and captured T1, then reconnected to mongo2. A local read checks whether clusterTime ≥ afterClusterTime (the causal token's time). Since mongo2's clock is now past T1, the read proceeds immediately without waiting for data and returns X=0—the baseline. This is a stale read: we know X=1 exists on mongo3, but the local read returned X=0.

As a control, we run the same test with readConcern:majority. A majority read waits for the write to be majority-committed. Since mongo2 is on the isolated minority and cannot reach mongo3, the read blocks indefinitely and times out, returning UNAVAILABLE. The majority read correctly refused to return stale data.

Finally, we heal the partition and restore the election timeout to 5,000 milliseconds.

**Reasoning:** We use a 120-second election timeout so mongo1 stays primary on the minority partition long enough to issue the dummy write. At 5 seconds, mongo1 would step down before we could advance the clock. The dummy write to mongo1 is necessary because we need mongo2's clock to advance; writing to mongo2 directly would not trigger replication. By writing to mongo1 (the primary) with w:1, the write replicates to mongo2, and mongo2's clock advances.

The test transfers causal state between sessions to model a client carrying causal history across connections. With a single session, the driver normally preserves this state automatically; the test manually transfers it to a separate session.

**Results and Interpretation:**

For the local/majority configuration with local read: The read on mongo2 returns X=0 (stale). The session observed X=1 in step 4 (via the causal tokens and the write to mongo3) and X=0 in step 7 (via the stale local read on mongo2), violating RYOW. The verdict is VIOLATED.

For the local/majority configuration with majority read (control): The read blocks on mongo2 and times out. No stale data is returned; the read is UNAVAILABLE. The verdict is SAFE because we never return a stale value, and RYOW is upheld (though the session cannot read at all).

### Limitations

The divergent-read test's success depends on timing: mongo3 must be elected before mongo1 steps down, and the dummy write must propagate to mongo2 before the partition heals. At a 120-second election timeout, this timing window is wide enough (~140–175 seconds) that the test succeeds reliably. However, no guarantee exists that the window will remain adequate in all environments or that clock skew will manifest in every run. Occasional runs may see mongo1 step down early or mongo3 take longer than expected to be elected, resulting in INCONCLUSIVE verdicts. We mitigate this by using `wait_primary()` to actively detect when mongo3 is primary, rather than hardcoding a sleep duration.

Additionally, our tests use single keys and single sessions. Real workloads distribute operations across thousands of keys and many concurrent sessions. These tests demonstrate that each consistency violation **can** occur under specific conditions, not that it occurs frequently in practice.

---

## III. Monotonic-Reads (MR)

**Definition:** Monotonic-reads consistency guarantees that once a session has read a value, later reads in that session must not return an older state of the same or related keys.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | HELD | Both writes refused or both survive |
| majority/w:1 | VIOLATED | X rolls back while Y survives |
| local/w:1 | VIOLATED | X rolls back while Y survives |
| local/majority | VIOLATED | Local read returns stale data after majority write |

[TODO: Procedure A and B sections, following RYOW structure]

---

## IV. Monotonic-Writes (MW)

**Definition:** Monotonic-writes consistency guarantees that if a session issues W1 followed by W2, then every server holding W2 must also hold W1. Writes from a single session never appear out of order.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | SAFE | W1 refused on isolated minority |
| majority/w:1 | VIOLATED | W1 rolls back while W2 survives |
| local/w:1 | VIOLATED | W1 rolls back while W2 survives |
| local/majority | SAFE | W1 refused on isolated minority |

[TODO: Procedure section, following RYOW structure]

---

## V. Writes-Follow-Reads (WFR)

**Definition:** Writes-follow-reads consistency guarantees that if a session reads a value and then writes, the write is causally ordered after the read. Every server holding the write must also hold the value that was read.

### Expected Outcomes

| Configuration | Expected Verdict | Mechanism |
|---|---|---|
| majority/majority | SAFE | Session read blocked; no dependent write issued |
| majority/w:1 | SAFE | Session read blocked; no dependent write issued |
| local/w:1 | VIOLATED | Session reads doomed value; dependent write survives rollback |
| local/majority | VIOLATED | Session reads doomed value; dependent write survives rollback |

[TODO: Procedure section, following RYOW structure]

---

## VI. Conclusions

[TODO: Summary of findings, agreement with MongoDB documentation, final observations]

---

**Report Metadata:**

- **Cluster:** MongoDB 7.0, 5-node replica set  
- **Client Library:** PyMongo ≥4.9  
- **Python Version:** ≥3.11  
- **Methodology:** Controlled network partitions using iptables; no failpoints or synthetic modifications to MongoDB  
- **Test Infrastructure:** Orchestration harness (`run_experiments.py`) for deterministic test sequencing
