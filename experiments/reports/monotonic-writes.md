# Monotonic-Writes Consistency — Report

## 1. Hypothesis

Monotonic-writes (MW): if a session issues W1 and then W2, every server that
holds W2 must also hold W1. A session's writes are applied everywhere in the
order they were issued.

MongoDB serialises all writes through a single primary into one totally ordered
oplog, so two writes cannot be *reordered*. The only way to expose W2 without W1
is for W1 to **roll back** while W2 survives. That makes MW a `writeConcern`
property: it holds exactly when an acknowledged write is durable.

Prediction (from the MongoDB causal-consistency docs, where the MW column is
ticked for the two `writeConcern:majority` rows and blank for the two `w:1`
rows):

| readConcern | writeConcern | predicted | mechanism |
|---|---|---|---|
| majority | majority | HOLDS | W1 refused on the minority side |
| local | majority | HOLDS | W1 refused (readConcern irrelevant) |
| majority | w:1 | VIOLATED | W1 rolls back, W2 survives |
| local | w:1 | VIOLATED | same rollback |

MW depends only on `writeConcern`; `readConcern` does not affect it.

## 2. Setup

Same 5-node replica set and partition as the read-your-writes experiment:
minority = mongo1+mongo2 (mongo1 = old primary), majority = mongo3+mongo4+mongo5
(mongo3 = new primary). All nodes stay reachable from the client; only inter-node
traffic is cut. Real events only — network partition + real writes, no failpoints.

Run: `./run.sh monotonic-writes <config>`.

## 3. Design and rationale

One causal session issues two writes to two keys:

1. Baseline k1=0, k2=0 durable (`w:majority`).
2. Partition the minority off; mongo1 is still briefly writable.
3. **W1**: write k1=1 with the config's write concern to mongo1.
4. **Barrier** — wait for the majority side to *elect* mongo3 (an observed event,
   not a fixed sleep).
5. **W2**: write k2=1 to mongo3 (the surviving primary).
6. Heal. Re-read k1 and k2 from the surviving history.

Rationale: with `w:1`, W1 is acknowledged on the doomed mongo1 and W2 lands
durably on mongo3. On heal, mongo1's un-replicated history is discarded — k1=1
vanishes while k2=1 remains, so a reader sees the *later* write without the
*earlier* one: MW violated. With `w:majority`, W1 on the minority side can never
reach a majority, so it is refused — never acknowledged, nothing to roll back,
and the ordering is preserved.

Why this tests MW specifically: the guarantee is precisely "no server has W2
without W1." We construct the one interleaving MongoDB permits that produces it —
a rollback that removes W1 but not W2 — and read both keys from the survivor.

## 4. Results

> Captured from `./run.sh monotonic-writes <config>`. <!-- RESULTS: paste run output -->

| readConcern | writeConcern | predicted | observed |
|---|---|---|---|
| majority | majority | HOLDS | <!-- SAFE --> |
| local | majority | HOLDS | <!-- SAFE --> |
| majority | w:1 | VIOLATED | <!-- VIOLATED --> |
| local | w:1 | VIOLATED | <!-- VIOLATED --> |

### VIOLATED — `majority/w:1` (and identically `local/w:1`)
```
<!-- PASTE: run.sh monotonic-writes majority/w:1 -->
```

### SAFE — `majority/majority` (and identically `local/majority`)
```
<!-- PASTE: run.sh monotonic-writes majority/majority -->
```

## 5. Expectations vs. observations, and limitations

**Agreement.** <!-- fill after runs: observations match the prediction; MW holds
iff writeConcern=majority. -->

**Limitations.**
- The violation depends on the failover window (W1 acked on the old primary
  before it steps down, W2 on the new primary). We wait for the election as an
  event rather than sleeping a guessed interval, which makes it reliable, but a
  run can still return INCONCLUSIVE if the window closes early — re-run.
- Deterministic priorities (mongo1 primary, mongo3 failover) fix the partition
  sides; a controlled simplification, not a restriction of the result.
- Single session, two keys, one violation shown — demonstrates the failure
  exists, not its frequency under load.
