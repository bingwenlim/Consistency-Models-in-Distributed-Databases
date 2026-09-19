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
   - `majority` → returns the committed baseline k1=0 (the doomed k1=1 is not
     majority-committed, so a majority read never sees it).
5. **Barrier** — wait for mongo3 to be elected.
6. **W2** (dependent): only if the session actually read k1=1, write k2 recording
   "saw k1=1" to mongo3 (survives).
7. Heal. Check whether k2 survives while k1=1 is gone.

Rationale: with `local`, the session reads a value that is about to disappear and
then commits a write that depends on it; after the rollback, k2 (the follow-on
write) exists but the k1=1 it followed does not — WFR violated. With `majority`,
the read returns only majority-committed data (the baseline k1=0), so the session
never observes the doomed k1=1 and never issues the dependent write — there is
nothing to violate.

Why this tests WFR specifically: the dependent write records exactly which value
it followed (`saw`), so the checker can confirm W2 was ordered after a read of
k1=1 and that that k1=1 no longer exists — the precise WFR violation.

## 4. Results

> Captured from `./run.sh writes-follow-reads <config>`. <!-- RESULTS: paste run output -->

| readConcern | writeConcern | predicted | observed |
|---|---|---|---|
| majority | majority | HOLDS | **SAFE** ✓ |
| majority | w:1 | HOLDS | **SAFE** ✓ |
| local | w:1 | VIOLATED | **VIOLATED** ✓ |
| local | majority | VIOLATED | **VIOLATED** ✓ |

All four observations match the prediction.

### VIOLATED — `local/w:1` (identical result for `local/majority`)
```
==> baseline written: wfr-k1=0, wfr-k2=0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> W1: wfr-k1=1 w:1 ACKED on mongo1 (doomed)
==> READ k1 with readConcern=local on mongo1: k1=1
==> waiting up to 40s for mongo3 to become primary
==> W2: wfr-k2 written on mongo3 (records 'saw k1=1')
==> healing partition
=== Writes-follow-reads / doomed read ===
  config:              local/w:1
  W1 (k1) acked:       True
  session read k1:     1
  dependent W2 issued: True  (recorded saw=1)
  survived heal:       k1=0  k2=1
  verdict:             VIOLATED (W2 followed a read of k1=1 that rolled back)
```
The session read k1=1 with `readConcern:local`, then wrote a dependent k2 that
records `saw=1`. After the heal, k2 survives but the k1=1 it followed is gone —
a write ordered after a read whose value no longer exists.

### SAFE — `majority/w:1` (identical result for `majority/majority`)
```
==> baseline written: wfr-k1=0, wfr-k2=0 (durable)
==> partitioning ['mongo1', 'mongo2'] (minority) from the majority side
==> W1: wfr-k1=1 w:1 ACKED on mongo1 (doomed)
==> READ k1 with readConcern=majority on mongo1: k1=0
==> waiting up to 40s for mongo3 to become primary
==> no dependent write issued (session never read k1=1)
==> healing partition
=== Writes-follow-reads / doomed read ===
  config:              majority/w:1
  W1 (k1) acked:       True
  session read k1:     0
  dependent W2 issued: False  (recorded saw=None)
  survived heal:       k1=0  k2=0
  verdict:             SAFE (majority read returned committed data, not the doomed value — no dependent write)
```
The `readConcern:majority` read returned the committed baseline k1=0, never the
doomed k1=1, so the session issued no dependent write. Nothing can be left
ordered after a value that rolled back.

## 5. Expectations vs. observations, and limitations

**Agreement.** All four observations match the prediction exactly. WFR holds iff
`readConcern:majority`; `writeConcern` has no effect on it. The failure needs a
`local` read of a value that then rolls back; a `majority` read only ever returns
committed data, so it never depends on a doomed write.

**Limitations.**
- Same failover-window timing dependence as monotonic-writes; the barrier waits
  for the election rather than sleeping, but an early-closing window can yield
  INCONCLUSIVE — re-run.
- The `majority`-read SAFE arm holds because the read returns committed data (the
  baseline), so no dependent write is ever issued. (If there were no committed
  baseline, a `majority` read would instead block/UNAVAILABLE — the same
  guarantee, expressed as unavailability.)
- Single session, one dependency, one violation shown.
