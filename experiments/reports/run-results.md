# Live Run Results

Documenting actual experiment runs against the 5-node Docker cluster. For each config we record the expected verdict, the actual verdict, and whether they agree; full stdout is captured per run.

Run order (fastest first): MW → WFR → RYOW → MR.

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
| MR | local/majority | VIOLATED | VIOLATED | ✅ |

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

### MR — Monotonic Reads (all 4 configs: ✅ match)

- **majority/majority → NOT_VIOLATED** (expected NOT_VIOLATED ✅). Read 1 saw X=1 on mongo3; Read 2 on mongo2 timed out (majority read can't reach the three-node group). No stale value returned.
- **majority/w:1 → NOT_VIOLATED** (expected NOT_VIOLATED ✅). Same: Read 2 timed out.
- **local/w:1 → VIOLATED** (expected VIOLATED ✅). Read 1 saw X=1 on mongo3; Read 2 (local) on mongo2 returned the stale X=0. Direct regression observed.
- **local/majority → VIOLATED** (expected VIOLATED ✅). Read concern is `local` for this config (the config tuple is `(read, write) = ("local", "majority")`, so `majority` is the *write* concern and does not gate the read). Read 2's local read on mongo2 gates on the advanced clock and returns the stale X=0 — same mechanism as `local/w:1`.

#### Note on MR local/majority

The verdict here is driven entirely by the **read** concern, which is `local` for this config. A local read on the isolated mongo2 gates only on whether mongo2's clock has passed the session's timestamp (it has, from the clock-advance write), so it returns the stale X=0 without waiting for the data. The `majority` in `local/majority` is the *write* concern and has no bearing on what mongo2 returns.

The pattern is consistent across all four MR configs: **read concern `local` → VIOLATED; read concern `majority` → NOT_VIOLATED.** By that rule `local/majority` (read = local) is VIOLATED, alongside `local/w:1`. This also matches MongoDB's documented behavior: for a local read, monotonic reads is not guaranteed regardless of write concern.

An earlier draft of the report's expected-outcomes table listed this cell as NOT_VIOLATED — a transcription error (it was written as if the `majority` write concern gated the read). The code, the docstring, and MongoDB's matrix always expected VIOLATED. The report table has been corrected; the observation matched the (correct) expectation.

## Overall tally

**16/16 configs matched their expected verdict.** Every observation agreed with the prediction table. The only correction during this work was a transcription typo in one report cell (MR `local/majority`, written NOT_VIOLATED, corrected to VIOLATED); the experiment, the code, and the observed result were VIOLATED throughout.

Two harness fixes were made so the suite runs cleanly end-to-end (neither affects any verdict):
- Added the missing `run_script` helper in `lib.py` (was undefined; blocked the partition/heal calls).
- Aligned `run.sh`'s cleanup trap to restore `electionTimeoutMillis=5000`, matching `up.sh` and `finalize_experiment` (was 10000, causing an inconsistent baseline).
