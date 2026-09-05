# Consistency Models in Distributed Databases — MongoDB Manual

A 5-node MongoDB replica set running in Docker, used to experiment with
**client-centric consistency**: read-your-writes, monotonic-reads,
monotonic-writes, and writes-follow-reads.

---

## 1. Prerequisites

- **Docker Desktop** installed and **running** (whale icon in the menu bar).
  Verify: `docker info` should print without error.
- **mongosh** — you can either use the copy *inside* the containers (no install
  needed) or install it on your host for convenience:
  `brew install mongosh` (optional).

Everything below assumes you're in the project root:
```bash
cd ~/Consistency-Models-in-Distributed-Databases
```

---

## 2. What's in here

```
docker-compose.yml      5 mongod containers on one Docker network (mongo-cluster)
scripts/
  rs-init.js            replica-set config (5 members)
  up.sh                 start containers + initialize replica set + wait for PRIMARY
  status.sh             show who is PRIMARY / SECONDARY and replication lag
  partition.sh <node>   cut a node off the network (simulate a partition)
  heal.sh <node>        reconnect a partitioned node
  down.sh [--wipe]      stop the cluster (--wipe also deletes data)
```

**Node → host port map** (use these to target a *specific* node from your host):

| Node   | Host port |
|--------|-----------|
| mongo1 | 27017     |
| mongo2 | 27018     |
| mongo3 | 27019     |
| mongo4 | 27020     |
| mongo5 | 27021     |

---

## 3. Start the cluster

```bash
./scripts/up.sh
```

This pulls the `mongo:7.0` image (first run only), starts all 5 containers,
initializes replica set `rs0`, and waits until a PRIMARY is elected. When it
finishes you'll see each node's state printed.

Check state at any time:
```bash
./scripts/status.sh
```
Expect **one PRIMARY** and **four SECONDARY** nodes, all `health=1`.

---

## 4. Connect and interact (CLI)

**Connect to the whole replica set** (recommended — driver routes automatically):
```bash
mongosh "mongodb://localhost:27017,localhost:27018,localhost:27019,localhost:27020,localhost:27021/?replicaSet=rs0"
```

Don't have mongosh on your host? Use the one inside a container:
```bash
docker exec -it mongo1 mongosh
```

**Connect to ONE specific node** (bypasses routing — how you read from a chosen
secondary):
```bash
mongosh "mongodb://localhost:27019/"      # talks to mongo3 directly
```

Quick smoke test once connected:
```javascript
db.demo.insertOne({ hello: "world" })
db.demo.find()
```

---

## 5. The consistency knobs

These four settings are the entire experiment. Change them, observe the effect.

| Knob | Values | What it controls |
|------|--------|------------------|
| **writeConcern** `w` | `1`, `"majority"` | How many nodes must ack a write before it returns |
| **readConcern** | `"local"`, `"majority"`, `"linearizable"` | What durability/visibility a read guarantees |
| **readPreference** | `primary`, `secondary`, `nearest` | Which node serves the read |
| **Causal session** | on / off | Session that enforces the 4 client-centric models |

Example — a write that waits for a majority, then a majority read:
```javascript
db.demo.insertOne({ x: 1 }, { writeConcern: { w: "majority" } })
db.demo.find().readConcern("majority")
```

Read explicitly from a secondary (where stale reads live):
```javascript
// connect to one node, e.g. mongo3 on 27019, then:
db.getMongo().setReadPref("secondary")
db.demo.find()
```

---

## 6. Testing each consistency model

The general recipe: **do a write, then a read under some config, and check
whether the read reflects the write.** A causally consistent session should
preserve the guarantee; a naive secondary read with `w:1` often breaks it.

### 6a. Read-your-writes
You write a value, then you should be able to read it back.

**Should HOLD** (causal session):
```javascript
const s = db.getMongo().startSession({ causalConsistency: true })
const c = s.getDatabase("test").demo
c.insertOne({ k: "ryw", v: 42 }, { writeConcern: { w: "majority" } })
c.find({ k: "ryw" }).readPref("secondary").toArray()   // sees v:42
s.endSession()
```

**Can BREAK** (no session, write to primary, immediately read a secondary):
```javascript
// Terminal A: connect to the set, write with w:1
db.demo.insertOne({ k: "ryw2", v: 99 }, { writeConcern: { w: 1 } })
// Terminal B (fast!): connect directly to a secondary, e.g. 27019
db.demo.find({ k: "ryw2" })   // may return nothing yet -> violation
```
Widen the window with a partition (§7) if replication is too fast to catch.

### 6b. Monotonic-reads
Once you've seen a value, later reads must not show an *older* state.

Read a fresh secondary, then a lagging one:
```javascript
// 1. partition mongo3 so it falls behind:  ./scripts/partition.sh mongo3
// 2. write several updates via the set (they reach mongo1,2,4,5)
// 3. read from an up-to-date secondary (e.g. mongo2 :27018) -> sees new data
// 4. heal + immediately read mongo3 (:27019) before it catches up -> older data
```
A causal session pins reads forward in time and prevents the regression.

### 6c. Monotonic-writes
Your own writes are applied in the order you issued them. Issue an ordered
series inside one causal session and confirm order on replicas:
```javascript
const s = db.getMongo().startSession({ causalConsistency: true })
const c = s.getDatabase("test").seq
for (let i = 1; i <= 5; i++) c.insertOne({ step: i }, { writeConcern: { w: "majority" } })
c.find().sort({ step: 1 }).toArray()   // 1..5 in order everywhere
s.endSession()
```

### 6d. Writes-follow-reads
A write you make after reading a value must be ordered *after* that value.
Use a causal session so the read's cluster time is carried into the next write:
```javascript
const s = db.getMongo().startSession({ causalConsistency: true })
const c = s.getDatabase("test").wfr
c.findOne({ k: "base" })                       // read establishes a point in time
c.updateOne({ k: "base" }, { $set: { seen: true } }, { writeConcern: { w: "majority" } })
s.endSession()
```

> Tip: keep `./scripts/status.sh` open in a second terminal to watch `optime`
> lag between nodes — that lag is exactly what causes (and reveals) violations.

---

## 7. Simulating a network partition

Cut a node off, run operations, then heal it:
```bash
./scripts/partition.sh mongo3     # mongo3 is now isolated
./scripts/status.sh               # mongo3 shows health=0 / unreachable
# ...run reads/writes against the rest of the cluster...
./scripts/heal.sh mongo3          # reconnect; it re-syncs
```

Useful experiments:
- Partition **2 nodes** → remaining 3 still form a majority → cluster stays
  writable.
- Partition **3 nodes** → no majority → the lone side becomes read-only (no
  PRIMARY). Watch this with `status.sh`.
- Partition the **current PRIMARY** → observe a new PRIMARY get elected among
  the majority side.

---

## 8. Shut down

```bash
./scripts/down.sh          # stop containers, KEEP data (resume later with up.sh)
./scripts/down.sh --wipe   # stop AND delete all data for a clean slate
```

---

## 9. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `docker API ... daemon not running` | Start Docker Desktop; wait for the whale icon, then re-run `up.sh`. |
| `up.sh` hangs on "waiting for PRIMARY" | Run `./scripts/status.sh`; if members are `STARTUP`/unreachable, give it ~30s. If stuck, `./scripts/down.sh --wipe` then `up.sh`. |
| Can't reproduce a violation | Replication is fast locally. Use a partition (§7) to widen the staleness window, or read a specific secondary by its host port. |
| Port already in use | Something else holds 27017–27021. Stop it, or edit the port mappings in `docker-compose.yml`. |
| Node stuck unreachable after heal | Wait for re-sync, or check you healed the right node name; `status.sh` shows current state. |

---

## 10. Optional: GUI inspection

MongoDB Compass (free) can connect to
`mongodb://localhost:27017/?replicaSet=rs0` to browse data and view topology.
Handy for eyeballing state — but run the timed consistency experiments from the
CLI, where you control ordering precisely.