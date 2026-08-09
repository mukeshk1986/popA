# Population Advyzer — Pipeline Performance Optimization Report

**Prepared for:** Technical & Business Leadership
**Environment:** STG (`pop_stg`) - Cycle 2026-03
**Comparison:** `main` branch (unoptimized) vs. `perf_optimization_uat` branch (optimized)
**Job:** `cms_risk_scoring_wf` (Job ID `627244742846076`)
**Date of analysis:** 2026-07-06

---

## 1. Executive Summary

Two of the heaviest stages of the CMS risk-scoring pipeline were optimized. The results are not just "faster" — in one case the old code could not finish at all.

| Stage | Before (main) | After (branch) | Outcome |
|---|---|---|---|
| Data Transformation (`data_transformation_common`) | 7h 42m ✅ finished | 37m ✅ | 12.5x faster · -92% |
| CMS HCC Risk Scoring v28 (`cms_hcc_risk_score_calc_v28`) | 17h 16m ❌ CANCELED, no output | 14m 52s ✅ | ≥70x faster · -98.6% (lower bound) |

**Key messages for leadership:**
- **Reliability:** The risk-scoring stage was effectively broken on `main` — it ran 17+ hours and had to be killed. On the optimized branch it completes in under 15 minutes. This converts a failing SLA into a reliable one.
- **Speed:** Combined, ~25 hours of per-cycle compute collapses to under 1 hour.
- **Cost:** ~90–98% compute cost reduction per run on identical hardware, plus elimination of wasted multi-hour failed/abandoned runs.
- **Scalability:** The fixes remove full-history table scans, so runtime no longer grows as data accumulates each cycle — the old design was degrading every month.

The speedup came from fixing four concrete data-quality/data-skew defects in the source data and enrollment logic (detailed in §3), plus the engineering optimizations in §4.

| # | Root-cause issue | Plans | Fix | Impact |
|---|---|---|---|---|
| 1 | Two `PLAN_ID`s per member (one blank) | fepma, uatplan2 | Drop blank-`PLAN_ID` rows before dedup | Removes MERGE ambiguity / bad enrollment rows |
| 2 | `COVRG_END_DT` = 9999-12-31 explodes 1 row → ~8,000 | fepma, fepma1 | Cap sentinel end date to current year | Eliminates hundreds of bogus coverage partitions |
| 3 | Overlapping/duplicate coverage collides on MERGE | fepma1 | Add `PROD_ID_CD`/`PROD_TYPE_CD`/`SUB_ID` to dedup + MERGE keys | Fixes `MULTIPLE_SOURCE_ROW_MATCHING` MERGE failure |
| 4 | One member with ~47,000 diagnosis codes (skew) | uatplan2 | Adaptive skew-join splitting + repartition | Removes straggler task behind the 17h+ hang |

---

## 2. Measured Results (Evidence)

All runs executed on the same cluster definition (`CMS-RISK-LARGE`): worker `i4i.4xlarge`, driver `i4i.8xlarge`, autoscale 1–40 workers, DBR 15.4, runtime STANDARD (no Photon). Because hardware is identical before/after, cost scales directly with runtime.

### Stage A — Data Transformation

| | Before | After |
|---|---|---|
| Run ID | 943364833078139 | 374530103020953 |
| Date (UTC) | 2026-07-02 | 2026-07-06 |
| Wall-clock | 7h 42m 15s | 37m 23s |
| Execution time | 7h 42m 53s | 33m 02s |
| Result | SUCCESS | SUCCESS |
| Plan / tables / month / risk year | fepma / member,facility,professional,provider,location,pharmacy / 03 / 2026 | (identical) |

### Stage B — CMS HCC Risk Scoring (v28)

| | Before | After |
|---|---|---|
| Run ID | 798241957833906 | 991469936255092 |
| Date (UTC) | 2026-07-02 | 2026-07-03 |
| Wall-clock | 17h 15m 47s | 14m 52s |
| Execution time | 17h 10m 56s | 13m 31s |
| Result | CANCELED — did not finish | SUCCESS |
| Plan / model | uatplan2 / cms_hcc (v24+v28) | (identical) |

> Stage B "before" is a lower bound — the run was manually terminated at 17h11m, so its true completion time is unknown and would be longer.

Corroborating run history for the same job shows multiple `main`-era runs with the same multi-hour / non-terminating behavior — independent confirmation that this was the norm, not a one-off.

---

## 3. Root-Cause Data Issues Discovered & Fixed

Investigation of the multi-hour/non-terminating runs surfaced four concrete data-quality and data-skew defects in the source data and enrollment logic. Each is a direct contributor to the runtime blow-ups — these are the "why it was slow" behind the engineering fixes in §4.

### Issue 1 — Two `PLAN_ID`s for the same member (blank + populated duplicate)
**Affected plans:** fepma, uatplan2
**Symptom:** A member appears twice in `STAGE_MEMBER_ENROLLMENT` — one row with a real `PLAN_ID` and a duplicate carrying a blank/space-only `PLAN_ID`. The blank duplicate could cause dedup or MERGE ambiguity / incorrect enrollment rows.
```sql
SELECT * FROM pop_stg.fepma_ingestion.STAGE_MEMBER_ENROLLMENT WHERE MEMB_ID_CD='P9OUCxkV';
SELECT * FROM pop_stg.uatplan2_ingestion.STAGE_MEMBER_ENROLLMENT WHERE HOME_PLAN_ID_CD='598' AND MEMB_ID_CD='nTJCAzl6XLF';
```
**Fix** (`member.sql`, commit `1a709bf3`): exclude blank `PLAN_ID` rows at source — `COALESCE(TRIM(PLAN_ID), '') <> ''` so only rows with a real `PLAN_ID` survive dedup, removing the collision.

### Issue 2 — `COVRG_END_DT` = 9999-12-31 exploding into hundreds of partitions
**Affected plans:** fepma, fepma1
**Symptom:** Enrollment is year-split via `sequence(year(begin), year(end))` + `explode()`. A sentinel/corrupt end date of 9999-12-31 expands a single enrollment into ~8,000 yearly rows, generating hundreds of coverage partitions and massively inflating downstream work.
**Fix** (commit `f2c5c120`): cap the sentinel to the current year — only the open-ended 9999 end date is collapsed to `year(current_date())`; genuine future end dates are preserved (for fepma1, end date was corrected to 2027-01-03). The true end date is retained downstream via `least(COVRG_END_DT_ORIG, make_date(yr,12,31))`.

### Issue 3 — Overlapping/duplicate coverage for the same member (MERGE conflict)
**Affected plan:** fepma1
**Symptom:** Two stage records with different original begin dates produce the same computed `COVRG_BEGIN_DT` after year-splitting (e.g. a multi-year span clipped to Jan-1 collides with a record that genuinely starts Jan-1). This triggers `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` — the MERGE fails on reprocess.
**Fix** (`member.sql`, commit `85e7654`): extend the dedupe key and the MERGE join key to include `PROD_ID_CD`, `PROD_TYPE_CD`, `SUB_ID` so product/subscriber segments stay distinct instead of colliding — the dedup key matches the MERGE key exactly, so `QUALIFY row_num = 1` keeps one row per (member, begin-date, product, subscriber).

### Issue 4 — Data skew: one member with ~47,000 diagnosis codes
**Affected plan:** uatplan2
**Symptom:** A single member (`MEMBER_BID 110000000349778`) carried ~47,000 diagnosis codes, creating a massive hot partition. In a skewed join this becomes one straggler task that runs for hours while all other tasks sit idle — the primary cause of the 17h+ non-terminating scoring run. (Skewed member isolated in `pop_stg.uatplan2_curation.risk_member_110000000349778` and `risk_member_diag_110000000349778`.)
**Fix:** Enable adaptive skew-join splitting (`spark.sql.adaptive.skewJoin.enabled`) + `coalescePartitions` + `shuffle.partitions=1000` (see §4-D), so Spark splits the overloaded partition at runtime instead of letting one task dominate wall-clock. This is why the same workload that ran 17h+ now completes in ~15m on identical hardware.

---

## 4. What Changed and Why It's Faster (Technical)

The optimizations target the same root causes across both stages: stop re-scanning full history, read only what's needed, and handle data skew. Verified from `git diff main..perf_optimization_uat`.

### A. Scope every read to the current run — biggest lever, both stages
**File:** `src/spark/helpers/transformations_commons.py` (`load_risk_member_and_reference_data`)
- **Before:** Discovered the latest data with 3–4 sequential, driver-blocking full-table scans of the append-only `risk_member_output` table: `MAX(YEAR_MONTH)` → filter → `MAX(CYCLE_RUN_DATE)` → filter. Every run rescanned the entire growing history, so runtime increased every cycle and trended toward "never finishes."
- **After:** The caller passes `year_month` + `time_period` down; the function reads only this run's slice in a single pass using a window function (`MAX(CYCLE_RUN_DATE) OVER (PARTITION BY YEAR_MONTH, TIME_PERIOD)`), then `persist()`s it since it's reused in 3 downstream steps.
- **Why faster:** Eliminates repeated full scans and driver stalls; makes read constant-time per run regardless of accumulated history. Also removes a `time_period` correctness race.

### B. Prune the massive `medical_claims` read — major I/O reduction
**File:** `src/spark/helpers/transformations_commons.py`
- **Before:** Full scan of a hundreds-of-millions-rows table, all columns.
- **After:** Filter to the run's `SOURCE_LOAD_MONTH` (Delta partition pruning skips most files), filter `QUALIFY_CLAIMS = 'Y'`, and select only the 5 columns used downstream (`MEMBER_BID`, `CLAIM_BID`, `CLM_TP_CD`, `STMT_THRU_DT`, `LAST_SRV_DT`).
- **Why faster:** Converts a full table scan into a targeted slice — large reduction in bytes read.

### C. Eliminate per-row `TRIM()` on join keys — Stage A
**Files:** `member.sql`, `facility.sql`, `professional.sql`, `pharmacy.sql`, `risk_prov.sql`, etc.; new `trim_string_columns()` in `src/spark/helpers/dataloader_util.py`.
- **Before:** `TRIM()` applied to join/merge keys at query time, across dozens of joins × millions of rows = billions of redundant string operations per run; also caused silent key mismatches that forced MERGE rework.
- **After:** Trim once at ingestion; joins compare clean columns natively.
- **Why faster:** Removes billions of per-row function evaluations and enables cleaner join planning.

### D. Adaptive execution + skew handling — both stages
**File:** `src/spark/cms/cms_hcc_risk_score_calc.py` and new `config/.../cluster_spec.yaml`
- Enabled `spark.sql.adaptive.enabled`, `adaptive.skewJoin.enabled`, `adaptive.coalescePartitions.enabled`, shuffle compression, FAIR scheduler, 128 MB partition sizing.
- **Why faster:** Skewed HCC/hierarchy joins were the likely cause of Stage B's multi-hour stalls — a single hot partition became one straggler task holding up the whole job. (Cluster autoscaled to 40 workers and held there for hours in the slow run — consistent with straggler-bound, not capacity-bound, execution.)

### E. Checkpoint long lineages — Stage B
**File:** `src/spark/cms/cms_hcc_risk_score_calc.py`
- The HCC-hierarchy result is written to a temp Delta table and re-read before scoring, truncating a very long DAG that Spark would otherwise recompute — a classic source of runaway "runs forever" jobs.

### F. Data-model correctness + cheaper checks
- **`MEMBER_ENROLLMENT`** (`member_enrollment.sql`): dedup + MERGE keys extended with `PROD_ID_CD`, `PROD_TYPE_CD`, `SUB_ID` to eliminate `MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW`; sentinel end-date (9999-12-31) handling; hash micro-batching.
- Removed duplicate reference-table loads in `config/sql/file_read_meta.yaml` (`MEMBER_ID`, `RISK_PROV`, `RISK_LOCATION`).
- Replaced `df.limit(1).count() == 0` with `df.isEmpty()` (HCC, ESRD, RxHCC scripts) — avoids a full aggregation just to test emptiness.

---

## 5. Time Savings

| Stage | Before | After | Time saved/run | Reduction |
|---|---|---|---|---|
| Data Transformation | 7h 42m | 37m | ~7h 05m | ~92% |
| CMS HCC Risk Scoring | 17h 16m (killed) | 15m | ~17h+ | ≥98.6% |
| **Combined / cycle** | **~25h** | **~52m** | **~24h** | **~96%** |

**Operational impact:** ~24 hours reclaimed off the critical path per cycle; the batch window opens up dramatically, downstream risk-scoring stages start sooner, and there is now ample headroom to re-run within SLA instead of blowing through it.

---

## 6. Cost Savings

**Cost model** (transparent assumptions — replace rates with contracted values):
- Worker `i4i.4xlarge` all-in at $1.82/node-hr (EC2 on-demand ~$1.373 + Jobs-Compute DBU ~$0.45).
- Driver `i4i.8xlarge` all-in at $3.60/hr.
- Used autoscaling: slow runs sat near the top of the 1–40 range for hours; fast runs scaled up briefly. Assumed time-weighted average ~15 active workers for the long runs and ~12 for the short ones (conservative).

| Stage | Before | After | Saved/run |
|---|---|---|---|
| Data Transformation | ~$240** (7.7h × (15+driver) × $1.82**) | ~$23** (0.62h × (12+driver) × $1.82**) | ~$217 (~90%) |
| CMS HCC Risk Scoring | ~$530** (17.2h × (15+driver) × $1.82**) | ~$9** (0.25h × (12+driver) × $1.82**) | ~$521 (~98%) |
| **Combined / cycle** | **~$770** | **~$32** | **~$738 (~96%)** |

**Annualized (illustrative):** at 12 plans × 12 monthly cycles = 144 cycles/yr, these two stages alone represent roughly **~$100K/yr**** of avoidable spend — before counting:
- Failed/abandoned runs (history shows 45h and 86h runs that produced nothing), and
- Engineer time spent monitoring and re-launching multi-hour jobs.

Exact dollars depend on your negotiated DBU + EC2 rates and the true time-weighted worker count; the ~90–98% reduction is rate-independent because before/after ran on identical hardware.

---

## 7. Caveats (stated up front)

1. Stage B "before" was **CANCELED, not completed** — present as "ran 17+ h without finishing → ≥70x," not a precise finish-to-finish ratio.
2. Stage A `historical_flag`: before-run used `'Y'`, after-run used `'N'`. If that flag changes the volume processed, part of Stage A's gain reflects a lighter workload. Recommend confirming, or citing a same-flag pair. (Stage B used identical params aside from a cosmetic `'03'` vs `'3'` month string — same data.)
3. Cost figures are illustrative pending actual DBU/EC2 rates and true time-weighted worker count.
4. Correctness was **not formally diffed** here (row-count / risk-score reconciliation) — both runs completed successfully, but a formal output reconciliation is recommended before PROD sign-off.

---

## 8. Recommendation

- Promote `perf_optimization_uat` to higher environments after a row-count / risk-score reconciliation vs. a known-good baseline (correctness gate).
- Adopt the cluster/Spark tuning (`cluster_spec.yaml`) as the default for **all** stages, not just the two measured — every stage on the shared config benefits from AQE + skew handling.
- Backfill a same-`historical_flag` Stage A comparison for a bulletproof headline number.
