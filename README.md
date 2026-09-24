# Consistency Models in Distributed Databases

A laboratory study of MongoDB's causal consistency guarantees through controlled network partitions and replica set failovers.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python ≥ 3.11
- `uv` package manager

### Start the Cluster

```bash
./scripts/up.sh
```

This brings up the 5-node MongoDB replica set and initializes it with the correct priorities. The script waits for mongo1 to become PRIMARY before returning.

### Run Experiments

From the `experiments` directory:

```bash
cd experiments
```

**Run a single test:**
```bash
uv run models/read_your_writes.py --config majority/majority
```

**Run all 4 configs for one model:**
```bash
uv run run_experiments.py --model monotonic_reads
```

**Run all 16 tests (all 4 models × 4 configs):**
```bash
uv run run_experiments.py
```

**Expected runtimes:**
- Single test: 90–210 seconds (depending on mechanism)
- One model (4 tests): 300–600 seconds
- All 16 tests: ~30 minutes

### Stop the Cluster

```bash
./scripts/down.sh
```

To also wipe all data:
```bash
./scripts/down.sh --wipe
```

---

## Project Structure

```
.
├── docker-compose.yml              # 5-node MongoDB cluster definition
├── scripts/
│   ├── up.sh                       # Start and initialize cluster
│   ├── down.sh                     # Stop cluster
│   ├── rs-init.js                  # Replica set configuration
│   ├── partition-split.sh          # Create network partition
│   ├── heal-split.sh               # Heal network partition
│   └── ...
├── experiments/
│   ├── lib.py                      # Shared cluster utilities
│   ├── helpers.py                  # Common test helpers
│   ├── run_experiments.py          # Test orchestration harness
│   ├── models/
│   │   ├── read_your_writes.py
│   │   ├── monotonic_reads.py
│   │   ├── monotonic_writes.py
│   │   └── writes_follow_reads.py
│   └── pyproject.toml              # Python dependencies
├── report.md                        # Detailed experimental report
└── README.md                        # This file
```

---

## Cluster Topology

The cluster consists of 5 MongoDB nodes with the following replica set priorities:

- **mongo1** (port 27017): priority=2 (designated PRIMARY)
- **mongo2** (port 27018): priority=0 (SECONDARY)
- **mongo3** (port 27019): priority=1 (failover PRIMARY)
- **mongo4** (port 27020): priority=0 (SECONDARY)
- **mongo5** (port 27021): priority=0 (SECONDARY)

This configuration ensures that after a partition isolating mongo1 and mongo2, mongo3 becomes the PRIMARY of the majority side. See `report.md` Section I for detailed explanation.

---

## Experiment Overview

Four consistency models are tested across four read concern / write concern combinations:

1. **Read-Your-Writes (RYOW):** After a session writes, later reads in that session see the write or newer.
2. **Monotonic-Reads (MR):** Once a session reads a value, later reads do not return older state.
3. **Monotonic-Writes (MW):** Session writes appear in order everywhere; later writes never appear without earlier ones.
4. **Writes-Follow-Reads (WFR):** A write issued after reading a value is ordered after that value.

Configurations tested:
- `majority/majority`
- `majority/w:1`
- `local/w:1`
- `local/majority`

See `report.md` for detailed explanation of the setup, mechanisms, and results.

---

## Understanding the Results

Each test outputs a verdict and explanation. Verdicts are:

- **SAFE:** The consistency guarantee held.
- **VIOLATED:** The consistency guarantee was broken.
- **HELD:** Writes/reads remained consistent.
- **UNAVAILABLE:** The operation timed out or was unreachable (often correct behavior for majority reads on isolated partitions).
- **INCONCLUSIVE:** Timing or other factors prevented a conclusive result; re-run the test.

---

## Connection Details

To connect to a specific node from your host:

```bash
mongosh "mongodb://localhost:27017/?directConnection=true"   # mongo1
mongosh "mongodb://localhost:27019/?directConnection=true"   # mongo3
```

Or use mongosh inside a container:

```bash
docker exec -it mongo1 mongosh
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `docker ... daemon not running` | Start Docker Desktop; wait for the whale icon, then re-run `up.sh`. |
| `up.sh` hangs on "waiting for PRIMARY" | Run `./scripts/status.sh`; give it ~30s. If stuck, `./scripts/down.sh --wipe` then `up.sh`. |
| Partition helper errors | First run pulls the `nicolaka/netshoot` image; ensure Docker has network access. |
| `local/majority` returns INCONCLUSIVE | Timing-window dependent (~90% reliable). Re-run; see the report's limitations. |
| Port already in use | Something else holds 27017–27021. Stop it, or edit the port mappings in `docker-compose.yml`. |

---

## References

- MongoDB Manual: [Causal Consistency](https://docs.mongodb.com/manual/core/read-isolation-consistency-semantics/#causal-consistency)
- Terry et al. (1994): "Session Guarantees for Weakly Consistent Replicated Data"

