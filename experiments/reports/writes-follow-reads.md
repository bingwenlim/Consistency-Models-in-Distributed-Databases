# Writes-Follow-Reads Consistency — Report

## 1. Hypothesis

Writes-follow-reads (WFR): if a session reads a value written by W1 and then
issues a write W2, every server that holds W2 must also hold W1. A write is
ordered after every write the session has already observed.

The failure is not about W2's durability but about whether the value the session
*read* was durable. If a session reads a write that later rolls back, and its
dependent write survives, then W2 exists while the W1 it depended on does not.
Reading a roll-back-able value requires `readConcern:local`; `readConcern:
majority` returns only majority-committed data, which cannot roll back. That
makes WFR a `readConcern` property — the mirror image of monotonic-writes.

Prediction (from the MongoDB causal-consistency docs, where the WFR column is
ticked for the two `readConcern:majority` rows and blank for the two `local`
rows):

| readConcern | writeConcern | predicted | mechanism |
|---|---|---|---|
| majority | majority | HOLDS | doomed read blocks → no dependent write |
| majority | w:1 | HOLDS | doomed read blocks (writeConcern irrelevant) |
| local | w:1 | VIOLATED | reads doomed k1, writes surviving k2 |
| local | majority | VIOLATED | same doomed read |

WFR depends only on `readConcern`; `writeConcern` does not affect it.

## 2. Setup

Same 5-node replica set and partition as the other experiments. Real events only.

Run: `./run.sh writes-follow-reads <config>`.

## 3. Design and rationale

1. Baseline k1=0, k2=0 durable.
2. Partition the minority off.
3. **W1**: a doomed `w:1` write of k1=1 on the isolated mongo1 (acknowledged, but
   never replicated to the majority).
4. The WFR session **reads k1** under the config's read concern:
   - `local` → returns the doomed k1=1;
   - `majority` → blocks and times out (k1=1 is not majority-committed) →
     UNAVAILABLE.
5. **Barrier** — wait for mongo3 to be elected.
6. **W2** (dependent): only if the session actually read k1=1, write k2 recording
   "saw k1=1" to mongo3 (survives).
7. Heal. Check whether k2 survives while k1=1 is gone.

Rationale: with `local`, the session reads a value that is about to disappear and
then commits a write that depends on it; after the rollback, k2 (the follow-on
write) exists but the k1=1 it followed does not — WFR violated. With `majority`,
the read of the non-durable value is UNAVAILABLE, so the session never issues the
dependent write, and there is nothing to violate.

Why this tests WFR specifically: the dependent write records exactly which value
it followed (`saw`), so the checker can confirm W2 was ordered after a read of
k1=1 and that that k1=1 no longer exists — the precise WFR violation.

## 4. Results

> Captured from `./run.sh writes-follow-reads <config>`. <!-- RESULTS: paste run output -->

| readConcern | writeConcern | predicted | observed |
|---|---|---|---|
| majority | majority | HOLDS | <!-- SAFE --> |
| majority | w:1 | HOLDS | <!-- SAFE --> |
| local | w:1 | VIOLATED | <!-- VIOLATED --> |
| local | majority | VIOLATED | <!-- VIOLATED --> |

### VIOLATED — `local/w:1` (and identically `local/majority`)
```
<!-- PASTE: run.sh writes-follow-reads local/w:1 -->
```

### SAFE — `majority/w:1` (and identically `majority/majority`)
```
<!-- PASTE: run.sh writes-follow-reads majority/w:1 -->
```

## 5. Expectations vs. observations, and limitations

**Agreement.** <!-- fill after runs: observations match the prediction; WFR holds
iff readConcern=majority. -->

**Limitations.**
- Same failover-window timing dependence as monotonic-writes; the barrier waits
  for the election rather than sleeping, but an early-closing window can yield
  INCONCLUSIVE — re-run.
- The `majority`-read SAFE arm demonstrates the guarantee by UNAVAILABILITY (the
  read blocks). That is the correct CAP-consistent behaviour, not a workaround.
- Single session, one dependency, one violation shown.
