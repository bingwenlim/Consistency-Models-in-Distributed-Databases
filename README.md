# Consistency Models in Distributed Databases — MongoDB

A 5-node MongoDB replica set running in Docker, used to experiment with
**client-centric consistency**: read-your-writes, monotonic-reads,
monotonic-writes, and writes-follow-reads.

Each model is tested across the four `readConcern` × `writeConcern` configs, using
**only real events** — network partitions and real writes. No failpoints, no
modifications to MongoDB.

---

## 1. Prerequisites

- **Docker Desktop** installed and **running** (`docker info` should print without error).
- **uv** (Python package manager) for running the experiment scripts: `brew install uv`.
- **mongosh** — optional; you can use the copy *inside* the containers instead.

Everything below assumes you're in the project root:
```bash
cd ~/Consistency-Models-in-Distributed-Databases
```

---

## 2. What's in here

```
docker-compose.yml            5 mongod containers (mongo:7.0) on one Docker network
scripts/
  rs-init.js                  replica-set config (mongo1 priority 2 = primary, mongo3 = failover)
  up.sh                       start containers + init replica set + force mongo1 PRIMARY
  down.sh [--wipe]            stop the cluster (--wipe also deletes data)
  status.sh                   show who is PRIMARY / SECONDARY and replication lag
  partition-split.sh [nodes]  TRUE two-sided partition (default: mongo1+mongo2 | mongo3+4+5)
  heal-split.sh               remove the partition; nodes re-sync
experiments/
  lib.py                      shared helpers (connections, partition/reconfig, state print)
  models/read_your_writes.py  read-your-writes across all 4 configs
  run.sh                      entrypoint: ./run.sh read-your-writes <config> [--control]
  reports/read-your-writes.md steps + captured results for RYOW
```

**Node → host port map** (target a *specific* node from your host):

| Node   | Host port |
|--------|-----------|
| mongo1 | 27017     |
| mongo2 | 27018     |
| mongo3 | 27019     |
| mongo4 | 27020     |
| mongo5 | 27021     |

**Topology.** Priorities are `mongo1=2` (always PRIMARY), `mongo3=1` (the designated
failover), `mongo2/4/5=0` (never elected). So when the cluster is split into
`mongo1+mongo2` (minority, led by the old primary) vs `mongo3+mongo4+mongo5`
(majority), mongo3 deterministically wins the majority side.

---

## 3. Start the cluster

```bash
./scripts/up.sh
```

Pulls `mongo:7.0` (first run only), starts all 5 containers, initializes replica set
`rs0`, and forces mongo1 to PRIMARY. Check state at any time:
```bash
./scripts/status.sh      # expect 1 PRIMARY + 4 SECONDARY, all health=1
```

---

## 4. Connect and interact

The experiment client connects to **one specific node by its host port** with
`directConnection=true` — a `replicaSet=rs0` URI is not usable from the host because
the driver would try to reach members by their container hostnames (`mongo1:27017`,
…), which don't resolve outside the Docker network.

```bash
mongosh "mongodb://localhost:27019/?directConnection=true"   # talk to mongo3 directly
```

Or use the mongosh inside a container:
```bash
docker exec -it mongo1 mongosh
```

---

## 5. The consistency knobs

The experiment varies **two** knobs; the other two are fixed by protocol.

| Knob | Role | Values |
|------|------|--------|
| **writeConcern** `w` | **swept** | `1`, `"majority"` |
| **readConcern** | **swept** | `"local"`, `"majority"` |
| **Causal session** | **always ON** | the client-centric guarantees are *defined over* a causally consistent session |
| **readPreference / target node** | **chosen per experiment** | picks which node serves the read so it can lag or diverge |

**Why the causal session is always on.** The four client-centric guarantees only
apply *inside* a causally consistent session — it carries the write's operation time
(`afterClusterTime`) into the next read so the read can wait for that timestamp. With
the session off you'd be measuring raw replication luck, not the guarantee. We leave
it on for every trial and show that even so, 3 of the 4 configs still fail.

**Why the target node is a per-experiment choice.** It decides which node serves the
read. If reads always hit the primary, RYOW holds trivially. Each experiment
deliberately points the read at a node that may not have the write (a stale ex-primary
on the losing side of a partition). It is a lever, not a swept variable.

---

## 6. Running the experiments

```bash
./run.sh read-your-writes majority/majority     # SAFE   (write refused on minority)
./run.sh read-your-writes majority/w:1           # VIOLATED (rollback)
./run.sh read-your-writes local/w:1              # VIOLATED (rollback)
./run.sh read-your-writes local/majority         # VIOLATED (divergent read)
./run.sh read-your-writes local/majority --control  # UNAVAILABLE (majority-read control)
```
(run from the `experiments/` directory). `run.sh` forces mongo1 PRIMARY first, and
**always** heals the partition and restores `electionTimeoutMillis`/priorities on exit,
even on Ctrl-C. Full steps and captured output: [`experiments/reports/read-your-writes.md`](experiments/reports/read-your-writes.md).

The other three models (monotonic-reads, monotonic-writes, writes-follow-reads) slot
in as `experiments/models/<name>.py` plus a case in `run.sh`.

---

## 7. Faults: the network partition

`partition-split.sh` creates a **true two-sided partition** by injecting `iptables`
DROP rules into each container's network namespace (via a privileged helper container
sharing that netns). It blocks traffic **only between the two groups**; every node
stays reachable from the host, so the experiment client can still read any node by its
port. `heal-split.sh` flushes the rules and the nodes re-sync (un-replicated writes on
the minority side roll back).

```bash
./scripts/partition-split.sh            # default: mongo1+mongo2 | mongo3+mongo4+mongo5
./scripts/heal-split.sh
```

- Partition **2 nodes** off → the other 3 keep majority → the majority side stays
  writable and elects mongo3; the minority side becomes read-only.
- Partition the **current primary** onto the minority → after `electionTimeoutMillis`
  it steps down and mongo3 is elected on the majority side.

> **Safety vs. liveness.** A read that *blocks/times out*, or a majority write that is
> *refused*, is **consistent but unavailable** — the system declined to return or
> confirm a wrong value (score as UNAVAILABLE / SAFE, not a failure). A read that
> returns a *stale or absent* value is a genuine consistency **VIOLATION**.

---

## 8. Shut down

```bash
./scripts/down.sh          # stop containers, KEEP data
./scripts/down.sh --wipe   # stop AND delete all data for a clean slate
```

---

## 9. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `docker ... daemon not running` | Start Docker Desktop; wait for the whale icon, then re-run `up.sh`. |
| `up.sh` hangs on "waiting for PRIMARY" | Run `./scripts/status.sh`; give it ~30s. If stuck, `./scripts/down.sh --wipe` then `up.sh`. |
| Partition helper errors | First run pulls the `nicolaka/netshoot` image; ensure Docker has network access. |
| `local/majority` returns INCONCLUSIVE | Timing-window dependent (~90% reliable). Re-run; see the report's limitations. |
| Port already in use | Something else holds 27017–27021. Stop it, or edit the port mappings in `docker-compose.yml`. |
| Node stuck unreachable after heal | Wait for re-sync, or check you healed the right node name; `status.sh` shows current state. |

---

## 10. Optional: GUI inspection

MongoDB Compass (free) can connect to a single node with
`mongodb://localhost:27017/?directConnection=true` to browse data and view topology.
Handy for eyeballing state — but run the timed consistency experiments from the
CLI, where you control ordering precisely.
