# Reproducing the experiments

Everything runs on one machine with Docker. Roughly 10 minutes end to end.

## 0. Prerequisites
- Docker Desktop running (`docker info` succeeds)
- `uv` (https://docs.astral.sh/uv/) — provides Python + PyMongo for the scripts
- First run pulls `mongo:7.0` and `nicolaka/netshoot` (a few minutes, once)

## 1. Start the 5-node replica set
```
./scripts/up.sh          # starts containers, initialises rs0, forces mongo1 PRIMARY
./scripts/status.sh      # expect 1 PRIMARY + 4 SECONDARY, all health=1
```

## 2. Run the main experiments
Each command runs one model under one `readConcern/writeConcern` config, induces a
real partition, prints a verdict, and always heals on exit. ~50 s each.

```
cd experiments

# Read-your-writes (Experiment 1)
./run.sh read-your-writes majority/w:1        # VIOLATED
./run.sh read-your-writes majority/majority   # SAFE

# Monotonic writes (Experiment 2) — writeConcern is the culprit
./run.sh monotonic-writes majority/w:1        # VIOLATED
./run.sh monotonic-writes majority/majority   # SAFE

# Writes-follow-reads (Experiment 3) — readConcern is the culprit
./run.sh writes-follow-reads local/w:1        # VIOLATED
./run.sh writes-follow-reads majority/w:1     # SAFE
```

Valid configs for every model: `majority/majority`, `majority/w:1`, `local/w:1`,
`local/majority`.

## 3. Monotonic reads (notebook)
```
uv run --with pymongo --with jupyterlab jupyter lab
# open experiments/notebooks/monotonic_reads.ipynb and run the cells top to bottom
```
The notebook has two parts: **basic** (partition + rollback) and **advanced**
(a delayed replica under normal operation, plus the causal-session fix that blocks
the stale read until it catches up).

## 4. Quantify a result (optional)
Run one config N times and report the verdict distribution instead of a single run:
```
cd experiments
./trials.sh monotonic-writes majority/w:1 10
```

## 5. Shut down
```
./scripts/down.sh          # stop, keep data
./scripts/down.sh --wipe   # stop and delete data volumes
```

## Repository layout
```
scripts/            up / down / status / partition-split / heal-split / rs-init
experiments/
  lib.py            shared helpers (connections, partition, wait_primary)
  run.sh            entrypoint: ./run.sh <model> <config>
  models/           read_your_writes.py, monotonic_writes.py, writes_follow_reads.py
  notebooks/        monotonic_reads.ipynb  (basic + advanced)
  reports/          per-model write-ups
  trials.sh         repeated-trials harness
REPORT.md           the full report (this submission)
REPRODUCE.md        this file
```

## Notes / troubleshooting
- The client reaches nodes by host port with `directConnection=true`; a
  `?replicaSet=rs0` URI does not resolve container hostnames from the host.
- If `docker run` for the partition helper hangs (Docker image pull under load),
  the split scripts now time out each call so a run fails fast instead of wedging;
  re-run after `./scripts/status.sh` shows the set healthy.
- A run that prints INCONCLUSIVE hit an unlucky failover window — just re-run it.
