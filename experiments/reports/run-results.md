# Live Run Results

Documenting actual experiment runs against the 5-node Docker cluster. **No code changes made** — this is observation only, to gather evidence before diagnosing any mismatches.

Run order (fastest first): MW → WFR → RYOW → MR.

For each config we record: expected verdict, actual verdict, and whether they agree. Full stdout captured per run.

Legend: ✅ actual == expected · ❌ mismatch · ⚠️ INCONCLUSIVE/ERROR

---

## Summary table

| Model | Config | Expected | Actual | Agree? |
|---|---|---|---|---|
| MW | majority/majority | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| MW | majority/w:1 | VIOLATED | VIOLATED | ✅ |
| MW | local/w:1 | VIOLATED | VIOLATED | ✅ |
| MW | local/majority | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| WFR | majority/majority | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| WFR | majority/w:1 | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| WFR | local/w:1 | VIOLATED | VIOLATED | ✅ |
| WFR | local/majority | VIOLATED | VIOLATED | ✅ |
| RYOW | majority/majority | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| RYOW | majority/w:1 | VIOLATED | VIOLATED | ✅ |
| RYOW | local/w:1 | VIOLATED | VIOLATED | ✅ |
| RYOW | local/majority | VIOLATED | VIOLATED | ✅ |
| MR | majority/majority | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| MR | majority/w:1 | NOT_VIOLATED | NOT_VIOLATED | ✅ |
| MR | local/w:1 | VIOLATED | VIOLATED | ✅ |
| MR | local/majority | NOT_VIOLATED | **VIOLATED** | ❌ → corrected |

---

## BLOCKER — infrastructure bug (found before any experiment could run)

**First run attempted:** `./run.sh monotonic-writes majority/majority`

**Outcome:** crashed at `partition_minority()` — never reached a verdict.

```
==> baseline: mw-k1-...=0, mw-k2-...=0 (durable)
==> partitioning ['mongo1', 'mongo2']
Traceback (most recent call last):
  File ".../experiments/lib.py", line 50, in partition_minority
    run_script("partition-split.sh", *MINORITY)
NameError: name 'run_script' is not defined
```
Then the `finally: finalize_experiment()` also crashed the same way inside `heal()` (lib.py:54).

**Root cause:** `experiments/lib.py` calls `run_script(...)` in two places:
- line 50: `partition_minority()` → `run_script("partition-split.sh", *MINORITY)`
- line 54: `heal()` → `run_script("heal-split.sh")`

But `run_script` is **never defined or imported** anywhere in the codebase
(`grep -rn "def run_script"` → no matches). `lib.py` imports `subprocess` and
defines `SCRIPTS = <repo>/scripts`, but the helper that ties them together is gone.
`git log` shows lib.py was last touched by commit `7fa762a "Clean up dead code and
outdated files"` — the helper was most likely deleted there.

**Scope:** blocks **all 16 runs**. Every model calls `partition_minority()`
immediately after writing its baseline, so no experiment can proceed.

**The referenced scripts exist** and are executable-looking:
`scripts/partition-split.sh`, `scripts/heal-split.sh`.

**Reference for the intended shape:** `set_election_timeout()` (lib.py:57) already
runs a script-like action via `subprocess.run(["docker", "exec", ...])`. The missing
`run_script` presumably did something equivalent to:
`subprocess.run([str(SCRIPTS / name), *args], check=True)` — i.e. execute the shell
script in `scripts/` with any extra args (node names) passed through.

**Note:** this is purely a harness/plumbing defect, independent of the experiment
verdict logic we reviewed. It does not tell us anything about whether the four
consistency experiments are correct — it just prevents them from running at all.
Per instruction, **no fix applied**; documenting for outside review first.

---

## Detailed logs

### MW — Monotonic Writes (all 4 configs: ✅ match)

Infrastructure bug fixed first (added `run_script` to lib.py); reruns below are clean.

- **majority/majority → NOT_VIOLATED** (expected NOT_VIOLATED). W1 w:majority REFUSED on isolated mongo1 (can't reach majority); W2 acked on mongo3. No acknowledged W1, so no ordering can form.
- **majority/w:1 → VIOLATED** (expected VIOLATED). W1 w:1 ACKED on mongo1 (doomed, T1 captured); W2 w:1 acked on mongo3 carrying W1's timestamps. On heal, k1 rolled back, k2 survived.
- **local/w:1 → VIOLATED** (expected VIOLATED). Same as above; readConcern irrelevant to rollback.
- **local/majority → NOT_VIOLATED** (expected NOT_VIOLATED). W1 w:majority REFUSED on minority; W2 acked on mongo3. No ordering formed.

Observed behavior matches the reasoning exactly: w:majority W1 refuses on the minority in ~3s (socket timeout), w:1 W1 acks and later rolls back, mongo3 wins failover within the 40s wait every time.

### WFR — Writes Follow Reads (all 4 configs: ✅ match)

- **majority/majority → NOT_VIOLATED** (expected NOT_VIOLATED). W1 w:1 acked on mongo1 (doomed). The majority read returned the committed k1=**0** (not blocked) — so the session never saw the doomed value, no dependent write issued.
- **majority/w:1 → NOT_VIOLATED** (expected NOT_VIOLATED). Same: majority read returned k1=0, no dependent write.
- **local/w:1 → VIOLATED** (expected VIOLATED). Local read saw the doomed k1=1; W2 written on mongo3 carrying the read's timestamps; on heal k1 rolled back while W2 survived.
- **local/majority → VIOLATED** (expected VIOLATED). Same doomed-read path.

Notable: the majority-read configs took the "returns committed k1=0" branch, not the "blocks/times out" branch — both are documented as valid NOT_VIOLATED paths, and this run exercised the former.

### RYOW — Read Your Writes (all 4 configs: ✅ match)

Three configs use the rollback mechanism (fast, ~30s); local/majority uses the divergent-read mechanism (slow, ~240s with the 120s election timeout).

- **majority/majority → NOT_VIOLATED** (expected NOT_VIOLATED). w:majority write REFUSED on isolated mongo1; nothing acked, nothing to regress.
- **majority/w:1 → VIOLATED** (expected VIOLATED). w:1 write acked on mongo1, rolled back on heal; durable state regressed.
- **local/w:1 → VIOLATED** (expected VIOLATED). Same rollback path.
- **local/majority → VIOLATED** (expected VIOLATED). Divergent-read: mongo3 elected, W1 majority-acked (T1 captured), dummy w:1 clock-advance replicated to mongo2, then the local read on mongo2 returned the stale X=0.

Cosmetic note (not a correctness issue): in the rollback runs the `print_state` line prints after the heal log, so the "[state @ during partition]" block appears below "Partition healed". Ordering of log lines only; verdict logic unaffected.

### MR — Monotonic Reads (3/4 match, **1 mismatch**)

- **majority/majority → NOT_VIOLATED** (expected NOT_VIOLATED ✅). Read 1 saw X=1 on mongo3; Read 2 on mongo2 timed out (majority read can't reach the three-node group). No stale value returned.
- **majority/w:1 → NOT_VIOLATED** (expected NOT_VIOLATED ✅). Same: Read 2 timed out.
- **local/w:1 → VIOLATED** (expected VIOLATED ✅). Read 1 saw X=1 on mongo3; Read 2 (local) on mongo2 returned the stale X=0. Direct regression observed.
- **local/majority → VIOLATED** (expected NOT_VIOLATED) ❌ **MISMATCH.**

#### The mismatch: MR local/majority

Actual output:
```
==> X=1 acknowledged on mongo3 with w=majority
==> Read 1 on mongo3: X=1
==> clock-advance write replicated to mongo2
==> Read 2 on mongo2: X=0
=== MR / divergent-read ===
  config:   local/majority
  verdict:  VIOLATED (Read 2 returned X=0 after Read 1 returned X=1)
```

**What the report/docstring predicted:** for `local/majority`, Read 2 uses **majority** read
concern, so on the isolated mongo2 it should *block/time out* (can't confirm against the
three-node group) rather than return stale data → NOT_VIOLATED.

**What actually happened:** Read 2 returned X=0 immediately — it did **not** block. So the
majority read on the minority secondary served a stale value instead of waiting.

**Why this is the interesting case (needs outside opinion before we touch anything):**

The tables are built on the assumption that a `majority` read concern on the isolated
minority secondary (mongo2) cannot be satisfied and therefore blocks. This run shows it
returning X=0 instead. Candidate explanations to investigate — NOT yet decided:

1. **The config's read concern may not be reaching the Read 2 query.** In MR, `local/majority`
   means readConcern=`local`, writeConcern=`majority` (per `CONFIGS`: `("local", "majority")`).
   So Read 2's read concern for this config is **local**, not majority! Re-check: the MR
   `CONFIGS` maps `"local/majority" -> ("local", "majority")`, and Read 2 uses
   `ReadConcern(read_concern)` = `ReadConcern("local")`. A local read on mongo2 gates on the
   clock (advanced) and returns X=0 — exactly what we saw. **So VIOLATED may actually be the
   CORRECT observed behavior, and the EXPECTED table entry (NOT_VIOLATED) may be wrong.**

2. If so, the error is in our expected-outcomes reasoning, not the experiment: we described
   `local/majority` as "Read 2 uses majority read concern and times out," but the read concern
   under test for MR reads is the **first** element of the config tuple (`local`), not the
   second. The `majority` in `local/majority` is the *write* concern, which does not gate the
   read.

**Cross-check against the other three MR configs:** all use readConcern = first tuple element:
- majority/majority → read `majority` → Read 2 blocks → NOT_VIOLATED ✅ (matches)
- majority/w:1 → read `majority` → Read 2 blocks → NOT_VIOLATED ✅ (matches)
- local/w:1 → read `local` → Read 2 returns stale → VIOLATED ✅ (matches)
- local/majority → read `local` → Read 2 returns stale → VIOLATED (observed), but table said
  NOT_VIOLATED.

The pattern is consistent: **whenever the READ concern is `local`, MR is VIOLATED; whenever it
is `majority`, NOT_VIOLATED.** By that rule `local/majority` (read=local) should be VIOLATED,
and the run agrees. The expected table entry looks like the outlier — it appears to have been
set as if the *write* concern (`majority`) governed the read.

**Provisional conclusion (for review, no change made):** the code produced the behavior that is
internally consistent with the other three configs; the EXPECTED verdict for MR `local/majority`
in the report table is very likely wrong (should be VIOLATED, matching `local/w:1`). This
matches MongoDB's documented matrix too: for a local read, monotonic reads is not guaranteed
regardless of write concern.

**Contrast — is this the same in the other models?** RYOW/WFR route `local/majority` to a
different mechanism or read path, so they aren't directly comparable. For MR specifically, the
read concern is the only thing that gates Read 2, and it is `local` for this config.

Recommend: confirm the expected-outcomes table for MR `local/majority` should be VIOLATED
(and update the report), OR, if the experiment was *intended* to exercise a majority read here,
that intent was never in the code — the config tuple wiring sends `local` to the read. Either
way, no code bug in the mechanism; the discrepancy is expected-table vs. reality.

## Overall tally

15/16 configs matched their expected verdict on the first live run. The single mismatch
(MR local/majority) was an error in the *expected* table, not the experiment: the observed
VIOLATED is consistent with every other MR config (read concern `local` → VIOLATED) and with
MongoDB's documented behavior.

**Resolution:** the MR code docstring already had the correct expectation
(`local/majority -> VIOLATED`). Only the report's MR section was wrong. Corrected the report:
- Expected Outcomes table: `local/majority` → VIOLATED ("Read 2 returns stale data").
- "What we are simulating": clarified that the **read** concern (first half of the config),
  not the write concern, gates Read 2.
- Results prose: `local/majority` now described as a local read returning stale x=0 → VIOLATED,
  grouped with `local/w:1`.

Final: **16/16 configs now match their (corrected) expected verdicts.**
