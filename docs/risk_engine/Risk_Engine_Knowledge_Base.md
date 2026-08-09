# Risk Engine Knowledge Base

## Table of Contents
1. [SEDITS (Sex/Age Edits)](#sedits-sexage-edits)
2. [HCC-1 Exclusion Marker](#hcc-1-exclusion-marker)
3. [CHRONIC_HCC for Method 4](#chronic_hcc-for-method-4)
4. [Bug Fixes Applied](#bug-fixes-applied)
   - [Fix 1: CHRONIC_HCC NULL for Method 4](#fix-1-chronic_hcc-null-for-method-4-2026-05-10)
   - [Fix 2: FREQUENCY Calculation Bug](#fix-2-frequency-calculation-bug-in-member_persistent_cc-2026-05-03)
   - [Fix 3: DURATION Calculation](#fix-3-duration-calculation-in-member_persistent_cc-2026-05-03)
   - [Fix 4: Missing Claim Date Filter (2024 Claims in RISK_YEAR 2023)](#fix-4-missing-claim-date-filter-2024-claims-in-risk_year-2023-2026-05-02)
   - [Change 5: Chronic Condition Flag Updates](#change-5-chronic-condition-flag-updates-ref_chronic_conditioncsv-2026-05-09)
   - [Change 6: Confidence Factor Override for On-Prem Parity](#change-6-confidence-factor-override-for-on-prem-parity-ref_method_prior_yearcsv-2026-05-09)

---

## SEDITS (Sex/Age Edits)

### Overview

**SEDITS = Sex Edits + Age Edits** — a CMS (Centers for Medicare & Medicaid Services) validation rule that determines whether a diagnosis code is clinically valid for a specific patient based on their **sex** and **age**.

Some diagnosis codes are only clinically appropriate for certain demographics:
- Certain conditions only occur in females (e.g., pregnancy-related)
- Certain conditions only occur in males (e.g., sex-linked genetic disorders)
- Certain conditions only occur at specific ages (e.g., perinatal conditions in newborns)

### Purpose in CMS HCC Risk Adjustment

CMS uses SEDITS to either:
1. **Reassign** a diagnosis to a different HCC (valid diagnosis but needs mapping adjustment based on demographics)
2. **Exclude** a diagnosis from HCC mapping entirely (clinically invalid for that patient's demographics)

### Configuration

**File:** `config/constants/ma_ra_model_constants.yaml`

| SEDITS Group | Diagnosis Codes | Clinical Description |
|--------------|-----------------|----------------------|
| **SEDITS_DIAG1** | D66, D67 | Hemophilia (sex-linked conditions, primarily affects males) |
| **SEDITS_DIAG2** | J410-J449, J982, J983, J4481, J4489 | Chronic respiratory diseases (COPD, emphysema, chronic bronchitis) |
| **SEDITS_DIAG3** | C50xxx (all breast cancer codes) | Malignant neoplasm of breast |
| **SEDITS_DIAG4** | P04x, P27x, P93x, P96x | Perinatal conditions (conditions originating in the newborn period) |
| **SEDITS_DIAG5** | F3481 | Premenstrual dysphoric disorder |

### Implementation

**File:** `src/spark/cms/cms_hcc_transformations.py`

#### Version V28 Rules

| Rule | Condition | HCC Assignment | Business Logic |
|------|-----------|----------------|----------------|
| 1 | Female + SEDITS_DIAG1 (D66, D67) | **HCC 112** | Reassign hemophilia to HCC 112 for females |
| 2 | Age < 18 + SEDITS_DIAG2 (COPD codes) | **-1 (Exclude)** | COPD is clinically invalid for children under 18 |
| 3 | Age < 50 + SEDITS_DIAG3 (Breast cancer) | **HCC 22** | Reassign breast cancer to HCC 22 for patients under 50 |
| 4 | Age >= 2 + SEDITS_DIAG4 (Perinatal codes) | **-1 (Exclude)** | Perinatal conditions invalid for patients aged 2 or older |

#### Version V24 Rules

| Rule | Condition | HCC Assignment | Business Logic |
|------|-----------|----------------|----------------|
| 1 | Female + SEDITS_DIAG1 | **HCC 48** | Reassign hemophilia to HCC 48 for females |
| 2 | Age < 18 + SEDITS_DIAG2 | **HCC 112** | Reassign COPD to HCC 112 for children |
| 3 | (Age < 6 OR Age > 18) + SEDITS_DIAG5 | **-1 (Exclude)** | PMDD invalid outside ages 6-18 |

### Code Reference

```python
# cms_hcc_transformations.py - age_sex_sedits function
version_rules = {
    "v28": [
        {
            "condition": (
                (F.col("AGE_YR") >= 2)
                & (F.col("DIAG_CD").isin(*sedits_diag_codes["SEDITS_DIAG4"]))
            ),
            "value": -1,  # Exclusion marker
        },
        # ... other rules
    ]
}
```

---

## HCC-1 Exclusion Marker

### What is HCC-1?

`HCC-1` (or `-1`) is a **sentinel/exclusion marker** in the CMS HCC risk scoring process. It indicates that a diagnosis code should **NOT** contribute to HCC scoring for a specific patient due to SEDITS validation failure.

### Why HCC-1 Appears

When SEDITS rules determine a diagnosis is clinically invalid for a patient's demographics, the HCC mapping is set to `-1` instead of a valid HCC number.

**Example:**
- **Diagnosis:** P279 (Chronic respiratory disease originating in perinatal period)
- **Patient Age:** 25 years old
- **SEDITS Rule:** Age >= 2 + SEDITS_DIAG4 → Exclude
- **Result:** `CMS-HCC-MODEL-CATEGORY-V28 = -1`

### Data Flow

1. **ICD-HCC Mapping:** P279 normally maps to HCC 213
2. **SEDITS Override:** Patient age >= 2, so SEDITS rule overrides to `-1`
3. **Stored in `risk_member_output`:**
   ```json
   DIAG_CD_TO_HCC_JSON_LIST: "{'DIAG_CD':'P279','CMS-HCC-MODEL-CATEGORY-V28':-1}"
   HCC_TO_SCORE_JSON_LIST: "{'HCC_COEFF':'CND_HCC-1','HCC_SCORE':0.0}"
   ```
4. **Final in `risk_member_hcc`:** `RISK_TYPE_DETAIL_ID = 'HCC-1'`

### Intentional Behavior

This is **intentional and correct behavior**. The `-1` / `HCC-1` value:

1. **Preserves audit trail** — shows the diagnosis was evaluated but excluded
2. **Indicates SEDITS exclusion** — distinguishes from NULL (unmapped) diagnoses
3. **Has zero score impact** — `HCC_SCORE = 0.0` ensures no risk score contribution
4. **Should be filtered in downstream** — consuming applications should filter out `HCC-1` records

### Query to Identify HCC-1 Records

```sql
SELECT * 
FROM pop_stg.{plan}_curation.risk_member_hcc 
WHERE RISK_TYPE_DETAIL_ID = 'HCC-1'
```

### Downstream Handling

When using `risk_member_hcc` data for reporting or analytics:

```sql
-- Exclude SEDITS exclusion markers
SELECT * 
FROM risk_member_hcc 
WHERE RISK_TYPE_ID = 'HCC' 
  AND RISK_TYPE_DETAIL_ID NOT IN ('HCC-1', 'HCC--1')
  AND RISK_TYPE_DETAIL_ID LIKE 'HCC%'
```

---

## CHRONIC_HCC for Method 4

### Overview

`CHRONIC_HCC` is a flag indicating whether an HCC is a chronic (persistent) condition. For Method 4 (Persistent HCC) in gap suspecting, this value should default to `'Y'` since persistent HCCs are chronic by definition.

### Issue Identified

Method 4 data from `member_persistent_cc` was not including `CHRONIC_HCC` column when inserting to `suspected_gaps` and `risk_member_hcc` tables, resulting in NULL values.

### Root Cause

The `method4_integration_with_suspected_gaps` function in `gap_suspecting_helper.py` did not include `CHRONIC_HCC` in its select statement. The downstream guard at line 1633-1635 only added the column if missing, but didn't handle the case where other methods had the column (with values) and Method 4 had NULL.

### Fix Applied

**File:** `src/spark/gp_suspecting/gap_suspecting_helper.py` (lines 1633-1642)

```python
# Ensure CHRONIC_HCC exists (may not exist for Method 4 data)
if "CHRONIC_HCC" not in df_suspected_gaps_all.columns:
    df_suspected_gaps_all = df_suspected_gaps_all.withColumn("CHRONIC_HCC", F.lit(None).cast("string"))

# Default CHRONIC_HCC to 'Y' for Method 4 if null (persistent HCCs are chronic by definition)
df_suspected_gaps_all = df_suspected_gaps_all.withColumn(
    "CHRONIC_HCC",
    F.when((F.col("METHOD_ID") == 4) & F.col("CHRONIC_HCC").isNull(), F.lit("Y"))
     .otherwise(F.col("CHRONIC_HCC"))
)
```

### Business Logic

| Method | CHRONIC_HCC Source | Default if NULL |
|--------|-------------------|-----------------|
| 1, 2, 10 | `ref_chronic_condition` lookup | Based on CC_ID chronic flag |
| 4 | `member_persistent_cc` table | **'Y'** (persistent = chronic by definition) |

---

## Bug Fixes Applied

### Fix 1: CHRONIC_HCC NULL for Method 4 (2026-05-10)

#### Problem Statement
When running gap suspecting with Method 4 (Persistent HCC), the `CHRONIC_HCC` column in `suspected_gaps` and `risk_member_hcc` tables was NULL instead of 'Y'.

#### Impact
- **Tables Affected:** `suspected_gaps`, `risk_member_hcc`
- **Column:** `CHRONIC_HCC`
- **Expected Value:** `'Y'` for Method 4
- **Actual Value:** `NULL`

#### Root Cause Analysis

1. **Method 4 Function Missing Column:** The `method4_integration_with_suspected_gaps` function (lines 2172-2320) in `gap_suspecting_helper.py` did not include `CHRONIC_HCC` in its `to_insert` DataFrame select statement.

2. **Union Behavior:** When Method 4 runs together with other methods (1, 2, 10):
   - Methods 1/2/10 include `CHRONIC_HCC` from `ref_chronic_condition` lookup
   - Union with `allowMissingColumns=True` adds the column
   - Method 4 rows get `NULL` for `CHRONIC_HCC`

3. **Guard Not Sufficient:** The existing guard at line 1633-1635 only checked if column was missing:
   ```python
   if "CHRONIC_HCC" not in df_suspected_gaps_all.columns:
       df_suspected_gaps_all = df_suspected_gaps_all.withColumn("CHRONIC_HCC", F.lit(None).cast("string"))
   ```
   This didn't handle the case where column exists but has NULL values for Method 4.

#### Solution Applied

**File:** `src/spark/gp_suspecting/gap_suspecting_helper.py`  
**Lines:** 1633-1642

**Before:**
```python
# Ensure CHRONIC_HCC exists (may not exist for Method 4 data)
if "CHRONIC_HCC" not in df_suspected_gaps_all.columns:
    df_suspected_gaps_all = df_suspected_gaps_all.withColumn("CHRONIC_HCC", F.lit(None).cast("string"))
```

**After:**
```python
# Ensure CHRONIC_HCC exists (may not exist for Method 4 data)
if "CHRONIC_HCC" not in df_suspected_gaps_all.columns:
    df_suspected_gaps_all = df_suspected_gaps_all.withColumn("CHRONIC_HCC", F.lit(None).cast("string"))

# Default CHRONIC_HCC to 'Y' for Method 4 if null (persistent HCCs are chronic by definition)
df_suspected_gaps_all = df_suspected_gaps_all.withColumn(
    "CHRONIC_HCC",
    F.when((F.col("METHOD_ID") == 4) & F.col("CHRONIC_HCC").isNull(), F.lit("Y"))
     .otherwise(F.col("CHRONIC_HCC"))
)
```

#### Why This Fix Works

1. **Handles Both Scenarios:**
   - Method 4 runs alone: Column added with NULL, then set to 'Y'
   - Method 4 runs with other methods: Column exists from union, NULL values for Method 4 set to 'Y'

2. **Preserves Other Methods:** The `.otherwise(F.col("CHRONIC_HCC"))` ensures Methods 1/2/10 keep their original values from `ref_chronic_condition`.

3. **Business Logic Correct:** Persistent HCCs (Method 4) are chronic by definition, so defaulting to 'Y' is semantically correct.

#### Validation Query

```sql
-- Verify CHRONIC_HCC is 'Y' for Method 4
SELECT METHOD_ID, CHRONIC_HCC, COUNT(*) as cnt
FROM pop_stg.{plan}_curation.risk_member_hcc
WHERE METHOD_ID = 4
GROUP BY METHOD_ID, CHRONIC_HCC;

-- Expected: METHOD_ID=4, CHRONIC_HCC='Y', cnt > 0
```

#### Downstream Impact

| Consumer | Impact |
|----------|--------|
| `cms_persistence_hcc.py` line 131 | Positive - Method 4 rows now included in filter `CHRONIC_HCC == 'y'` |
| Gap Suspecting reports | Positive - Correct chronic flag displayed |
| Risk scoring downstream | No change - CHRONIC_HCC is informational |

---

### Fix 2: FREQUENCY Calculation Bug in member_persistent_cc (2026-05-03)

#### Problem Statement
For members with multiple claims for the same HCC condition, the `FREQUENCY` column in `member_persistent_cc` was incorrectly showing **1** for all records, regardless of actual claim count.

#### Example
| CC_CODE | RISK_YEAR | FREQUENCY (Actual) | FREQUENCY (Expected) |
|---------|-----------|--------------------|--------------------|
| HCC127 | 2025 | 1 | 6 |
| HCC198 | 2025 | 1 | 5 |
| HCC213 | 2025 | 1 | 24 |
| HCC253 | 2025 | 1 | 3 |

#### Impact
- **CONFIDENCE_FACTOR Miscalculation:** Confidence factor depends on FREQUENCY via join condition `FREQUENCY >= DX_FREQUENCY_MIN AND FREQUENCY <= DX_FREQUENCY_MAX`
- **Gap Prioritization:** High-frequency (high confidence) gaps incorrectly appear as low-priority
- **RAF Impact:** Potential revenue impact is underestimated

#### Root Cause Analysis

**File:** `src/spark/cms/member_persistent_hcc.py`  
**Function:** `build_df_raw_prior_year_filter()` (lines 32-42)

**Problematic Code:**
```python
# Deduplication happened BEFORE frequency calculation
window_latest_batch_by_year = (
    Window
    .partitionBy("RISK_YEAR", "SAS_MODEL_VERSION", "MEMBER_BID", "RISK_TYPE_DETAIL_ID")
    .orderBy(F.col("CREATED_DATE").desc(), F.col("CLAIM_BID").desc())
)

df_hcc_prior_years_latest_batch = (
    df_hcc_prior_years_raw
    .withColumn("rnk", F.row_number().over(window_latest_batch_by_year))
    .filter(F.col("rnk") == 1)  # <-- BUG: Reduces each HCC to 1 row BEFORE frequency count
    .drop("rnk")
)
```

**Data Flow (Buggy):**
```
risk_member_hcc (raw)           After Deduplication          FREQUENCY Calculation
┌────────────────────┐          ┌────────────────────┐       ┌────────────────────┐
│ HCC127 | CLAIM 1001│          │ HCC127 | CLAIM 1006│       │ HCC127 | FREQ = 1  │
│ HCC127 | CLAIM 1002│  ──────► │ (only 1 row!)      │ ────► │ (should be 6!)     │
│ HCC127 | CLAIM 1003│          └────────────────────┘       └────────────────────┘
│ HCC127 | CLAIM 1004│          
│ HCC127 | CLAIM 1005│          
│ HCC127 | CLAIM 1006│          
└────────────────────┘ (6 rows)
```

#### Solution Applied

**Approach:** Calculate FREQUENCY on raw data BEFORE deduplication, then carry the result through.

```python
# === FIX: Calculate FREQUENCY on raw data BEFORE deduplication ===
partition_keys = ["RISK_YEAR", "SAS_MODEL_VERSION", "MEMBER_BID", "RISK_TYPE_DETAIL_ID", "HOME_PLAN_ID_CD"]

# Count distinct claims per HCC using marker approach
window_by_claim = Window.partitionBy(*partition_keys, "CLAIM_BID").orderBy(F.lit(1))
window_by_member_hcc = Window.partitionBy(*partition_keys)

df_with_frequency = (
    df_hcc_prior_years_raw
    .withColumn("claim_marker", F.row_number().over(window_by_claim))
    .withColumn(
        "FREQUENCY",
        F.sum(F.when(F.col("claim_marker") == 1, 1).otherwise(0))
        .over(window_by_member_hcc)
    )
    .drop("claim_marker")
)

# NOW deduplicate - FREQUENCY is already calculated correctly
df_hcc_prior_years_latest_batch = (
    df_with_frequency
    .withColumn("rnk", F.row_number().over(window_latest_batch_by_year))
    .filter(F.col("rnk") == 1)
    .drop("rnk")
)
```

**Data Flow (Fixed):**
```
risk_member_hcc (raw)           FREQUENCY Calculated         After Deduplication
┌────────────────────┐          ┌────────────────────┐       ┌────────────────────┐
│ HCC127 | CLAIM 1001│          │ HCC127 | FREQ = 6  │       │ HCC127 | FREQ = 6  │
│ HCC127 | CLAIM 1002│  ──────► │ HCC127 | FREQ = 6  │ ────► │ (correct value!)   │
│ HCC127 | CLAIM 1003│          │ HCC127 | FREQ = 6  │       └────────────────────┘
│ HCC127 | CLAIM 1004│          │ HCC127 | FREQ = 6  │
│ HCC127 | CLAIM 1005│          │ HCC127 | FREQ = 6  │
│ HCC127 | CLAIM 1006│          │ HCC127 | FREQ = 6  │
└────────────────────┘          └────────────────────┘
```

#### Technical Note: Why Not countDistinct()?

Spark window functions don't support `countDistinct()` directly. The marker-based approach is used:
1. Partition by HCC + CLAIM_BID
2. Assign row_number = 1 to first occurrence of each CLAIM_BID
3. Sum the markers (count of distinct claims)

#### Validation Query

```sql
-- Verify FREQUENCY matches actual claim counts
WITH actual_counts AS (
    SELECT MEMBER_BID, RISK_TYPE_DETAIL_ID as CC_CODE, 
           COUNT(DISTINCT CLAIM_BID) as expected_frequency
    FROM risk_member_hcc
    WHERE DX_SOURCE = 'claim'
    GROUP BY MEMBER_BID, RISK_TYPE_DETAIL_ID
),
persistent_data AS (
    SELECT MEMBER_BID, CC_CODE, FREQUENCY as actual_frequency
    FROM member_persistent_cc
    WHERE SUSPECT_STATUS = 'Open Suspect'
)
SELECT a.*, p.actual_frequency,
       CASE WHEN a.expected_frequency = p.actual_frequency THEN 'PASS' ELSE 'FAIL' END as test_result
FROM actual_counts a
JOIN persistent_data p ON a.MEMBER_BID = p.MEMBER_BID AND a.CC_CODE = p.CC_CODE
WHERE a.expected_frequency != p.actual_frequency;

-- Expected: No rows returned (all frequencies match)
```

---

### Fix 3: DURATION Calculation in member_persistent_cc (2026-05-03)

#### Overview

`DURATION` represents the number of days since the latest claim for an HCC condition. It's used along with FREQUENCY to determine CONFIDENCE_FACTOR via the `ref_method_frequency` lookup.

#### Calculation Logic

**File:** `src/spark/cms/member_persistent_hcc.py`  
**Function:** `compute_prior_year_confidence_factor_without_dropping()` (lines 567-575)

```python
# DURATION = days since the claim date
df_with_duration = (
    df_raw_prior_year_filter
    .withColumn(
        "DURATION",
        F.abs(
            F.datediff(
                F.lit(as_of_date_value),    # Current/reference date
                F.col("CLAIM_DATE")          # Latest claim date for this HCC
            )
        )
    )
)
```

#### How DURATION is Used

DURATION and FREQUENCY together determine CONFIDENCE_FACTOR via join:

```python
df_with_confidence = (
    df_with_duration.alias("s")
    .join(
        df_dx_weights.alias("t"),  # ref_method_frequency table
        (
            (F.col("s.DURATION") >= (F.col("t.LAST_DX_OCCURANCE") - F.lit(29))) &
            (F.col("s.DURATION") <= F.col("t.LAST_DX_OCCURANCE")) &
            (F.col("s.FREQUENCY") >= F.col("t.DX_FREQUENCY_MIN")) &
            (F.col("s.FREQUENCY") <= F.col("t.DX_FREQUENCY_MAX"))
        ),
        how="left"
    )
    .withColumn("CONFIDENCE_FACTOR", F.col("t.PERCENT_WEIGHT"))
)
```

#### Reference Table: ref_method_frequency

| LAST_DX_OCCURANCE | DX_FREQUENCY_MIN | DX_FREQUENCY_MAX | PERCENT_WEIGHT |
|-------------------|------------------|------------------|----------------|
| 30 | 1 | 2 | 0.50 |
| 30 | 3 | 5 | 0.65 |
| 30 | 6+ | 999 | 0.80 |
| 90 | 1 | 2 | 0.40 |
| 90 | 3 | 5 | 0.55 |
| ... | ... | ... | ... |

#### Business Logic

- **Lower DURATION (recent claim)** + **Higher FREQUENCY** = **Higher CONFIDENCE_FACTOR**
- Example: Claim 15 days ago with 6 occurrences → High confidence gap
- Example: Claim 300 days ago with 1 occurrence → Low confidence gap

---

### Fix 4: Missing Claim Date Filter (2024 Claims in RISK_YEAR 2023) (2026-05-02)

#### Problem Statement

Claims from 2024 and 2025 were incorrectly appearing in `risk_member_hcc` table with `RISK_YEAR = 2023`. 

**Example:**
- CLAIM_BID: `3300000452183771`
- FIRST_SERV_DT: **2024-09-26** (service date in 2024)
- RISK_YEAR: **2023** (incorrect!)

51 records had RISK_YEAR=2023 but service dates ranging from 2024-01-16 to 2025-09-29.

#### Impact

- **Incorrect Risk Scores:** Claims from wrong years contribute to risk calculations
- **Data Integrity:** RISK_YEAR doesn't reflect actual claim dates
- **Downstream Analysis:** Gap suspecting and persistence calculations based on wrong year assignments

#### Root Cause Analysis

**File:** `src/spark/data_prep/ma_model_input_data.py`

**Problem 1: Claims loaded WITHOUT service date filter (lines 205-209)**
```python
# Only filters by SOURCE_LOAD_MONTH, NOT by service date
medical_claims = read_table(
    spark, silver_schema, "MEDICAL_CLAIMS", None,
    filter_condition=[
        col("SOURCE_LOAD_MONTH") == year_month,  # Processing month, NOT service date!
        lower(col("QUALIFY_CLAIMS")) == "y"
    ],
)
```

**Problem 2: TIME_PERIOD assigned as literal to ALL claims (line 294)**
```python
# Sets same TIME_PERIOD for ALL claims regardless of their actual service dates
risk_member_diag_df = (
    medical_claims_final_df.withColumn("TIME_PERIOD", lit(time_period))
    ...
)
```

**Problem 3: RISK_YEAR derived from TIME_PERIOD, not claim dates**

The downstream `process_risk_member_hcc` function derives RISK_YEAR from TIME_PERIOD, not from actual claim service dates (FIRST_SERV_DT).

#### Data Flow (Buggy)

```
Run for risk_year=2023, TIME_PERIOD=5
         │
         ▼
Load ALL claims with SOURCE_LOAD_MONTH=2025_12
┌─────────────────────────────────────────┐
│ CLAIM_BID | FIRST_SERV_DT | ...         │
│ 1001      | 2023-03-15    |             │  ← Correct for 2023
│ 1002      | 2024-01-16    |             │  ← Should NOT be in 2023!
│ 1003      | 2024-09-26    |             │  ← Should NOT be in 2023!
│ 1004      | 2025-02-10    |             │  ← Should NOT be in 2023!
└─────────────────────────────────────────┘
         │
         ▼
Assign TIME_PERIOD=5 to ALL claims
         │
         ▼
risk_member_hcc: ALL claims get RISK_YEAR=2023  ← BUG!
```

#### TIME_PERIOD Reference

| TIME_PERIOD | BEGIN_DATE | END_DATE | TYPE | Expected RISK_YEAR |
|-------------|------------|----------|------|-------------------|
| 5 | 2023-01-01 | 2023-12-31 | C | 2023 |
| 7 | 2024-01-01 | 2024-12-31 | C | 2024 |
| 9 | 2025-01-01 | 2025-12-31 | C | 2025 |

#### Solution Applied

**Add service date filter when loading claims:**

```python
# Get time period date range
time_period_dates = read_table(
    spark, config_schema, "time_periods", ["TIME_PERIOD", "BEGIN_DATE", "END_DATE"],
    filter_condition=[col("TIME_PERIOD") == time_period]
).first()

tp_begin_date = time_period_dates["BEGIN_DATE"]
tp_end_date = time_period_dates["END_DATE"]

# Filter claims by service date
medical_claims = read_table(
    spark, silver_schema, "MEDICAL_CLAIMS", None,
    filter_condition=[
        col("SOURCE_LOAD_MONTH") == year_month,
        lower(col("QUALIFY_CLAIMS")) == "y",
        col("FIRST_SERV_DT") >= tp_begin_date,  # ADD: Filter by service date
        col("FIRST_SERV_DT") <= tp_end_date      # ADD: Filter by service date
    ],
)
```

#### Data Flow (Fixed)

```
Run for risk_year=2023, TIME_PERIOD=5 (2023-01-01 to 2023-12-31)
         │
         ▼
Load claims with SOURCE_LOAD_MONTH=2025_12 AND FIRST_SERV_DT in 2023
┌─────────────────────────────────────────┐
│ CLAIM_BID | FIRST_SERV_DT | ...         │
│ 1001      | 2023-03-15    |             │  ← Only 2023 claims included
└─────────────────────────────────────────┘
         │
         ▼
Assign TIME_PERIOD=5 (correctly now)
         │
         ▼
risk_member_hcc: RISK_YEAR=2023 with only 2023 claims  ✓
```

#### Validation Query

```sql
-- Verify FIRST_SERV_DT range matches TIME_PERIOD date range
SELECT 
    rmd.TIME_PERIOD,
    tp.BEGIN_DATE as expected_min,
    tp.END_DATE as expected_max,
    MIN(rmd.FIRST_SERV_DT) as actual_min,
    MAX(rmd.FIRST_SERV_DT) as actual_max,
    CASE 
        WHEN MIN(rmd.FIRST_SERV_DT) >= tp.BEGIN_DATE 
         AND MAX(rmd.FIRST_SERV_DT) <= tp.END_DATE 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as test_result
FROM risk_member_diag rmd
JOIN time_periods tp ON rmd.TIME_PERIOD = tp.TIME_PERIOD
GROUP BY rmd.TIME_PERIOD, tp.BEGIN_DATE, tp.END_DATE;

-- Expected: All rows show 'PASS'
```

#### Code Locations

| File | Line | Issue |
|------|------|-------|
| `src/spark/data_prep/ma_model_input_data.py` | 205-209 | Claims loaded without service date filter |
| `src/spark/data_prep/ma_model_input_data.py` | 294 | TIME_PERIOD assigned as literal to all claims |
| `src/spark/helpers/transformations_commons.py` | 2438 | RISK_YEAR derived from TIME_PERIOD, not claim dates |

#### Deployment Notes

After implementing the fix:
1. Re-run `ma_model_input_data.py` for each TIME_PERIOD to correct the data
2. Re-run `cms_hcc_risk_score_calc.py` to regenerate `risk_member_hcc` with correct RISK_YEAR values
3. Verify data integrity with validation queries

---

### Change 5: Chronic Condition Flag Updates (ref_chronic_condition.csv) (2026-05-09)

#### Overview

Updated the `CHRONIC` flag for multiple HCC codes in `ref_chronic_condition.csv` based on clinical review feedback from **Venu** and **Sarah**.

**PR Reference:** [#370](https://github.com/bhi-emids/EMIDS-Population-Advyzer/pull/370)

#### Business Context

The `CHRONIC` flag (column value `1` or `0`) determines whether an HCC is considered a chronic (persistent) condition. This affects:
- Method 4 (Persistent HCC) gap identification
- Long-term condition tracking
- Gap prioritization for chronic disease management

#### Changes Made

**File:** `src/sql/data/ref_chronic_condition.csv`

**HCCs Changed from CHRONIC=1 to CHRONIC=0 (No longer chronic):**

| Model Version | CC_CODE | CC_DESCRIPTION | Previous | New |
|---------------|---------|----------------|----------|-----|
| MA V2425.86.P1 (v24) | CC017 | Diabetes with Acute Complications | 1 | **0** |
| MA V2425.86.P1 (v24) | CC033 | Intestinal Obstruction/Perforation | 1 | **0** |
| MA V2425.86.P1 (v24) | CC039 | Bone/Joint/Muscle Infections/Necrosis | 1 | **0** |
| MA V2425.86.P1 (v24) | CC087 | Unstable Angina and Other Acute Ischemic Heart Disease | 1 | **0** |
| MA V2425.86.P1 (v24) | CC100 | Ischemic or Unspecified Stroke | 1 | **0** |
| MA V2425.86.P1 (v24) | CC166 | Severe Head Injury | 1 | **0** |
| MA V2425.86.P1 (v24) | CC169 | Vertebral Fractures without Spinal Cord Injury | 1 | **0** |
| MA V2425.86.P1 (v24) | CC173 | Traumatic Amputations and Complications | 1 | **0** |
| MA V2825.115.T1 (v28) | CC135 | Drug Use with Psychotic Complications | 1 | **0** |
| MA V2825.115.T1 (v28) | CC136 | Alcohol Use with Psychotic Complications | 1 | **0** |
| MA V2825.115.T1 (v28) | CC002 | Septicemia, Sepsis, SIRS/Shock | 1 | **0** |
| MA V2825.115.T1 (v28) | CC211 | Respirator Dependence/Tracheostomy Status | 1 | **0** |
| MA V2825.115.T1 (v28) | CC213 | Cardio-Respiratory Failure and Shock | 1 | **0** |
| MA V2825.115.T1 (v28) | CC225 | Acute Heart Failure (Excludes Acute on Chronic) | 1 | **0** |
| MA V2825.115.T1 (v28) | CC229 | Unstable Angina and Other Acute Ischemic Heart Disease | 1 | **0** |
| MA V2825.115.T1 (v28) | CC248 | Intracranial Hemorrhage | 1 | **0** |
| MA V2825.115.T1 (v28) | CC249 | Ischemic or Unspecified Stroke | 1 | **0** |
| MA V2825.115.T1 (v28) | CC267 | Deep Vein Thrombosis and Pulmonary Embolism | 1 | **0** |
| MA V2825.115.T1 (v28) | CC298 | Severe Diabetic Eye Disease, Retinal Vein Occlusion | 1 | **0** |
| MA V2825.115.T1 (v28) | CC036 | Diabetes with Severe Acute Complications | 1 | **0** |
| MA V2825.115.T1 (v28) | CC385 | Severe Skin Burn | 1 | **0** |
| MA V2825.115.T1 (v28) | CC399 | Major Head Injury without Loss of Consciousness | 1 | **0** |
| MA V2825.115.T1 (v28) | CC401 | Vertebral Fractures without Spinal Cord Injury | 1 | **0** |
| MA V2825.115.T1 (v28) | CC405 | Traumatic Amputations and Complications | 1 | **0** |
| MA V2825.115.T1 (v28) | CC068 | Cholangitis and Obstruction of Bile Duct | 1 | **0** |
| MA V2825.115.T1 (v28) | CC078 | Intestinal Obstruction/Perforation | 1 | **0** |
| MA V2825.115.T1 (v28) | CC092 | Bone/Joint/Muscle/Severe Soft Tissue Infections | 1 | **0** |

#### Rationale

These conditions are **acute events** rather than chronic conditions:
- Acute complications (diabetes acute, heart failure acute)
- Traumatic injuries (head injury, fractures, amputations)
- Acute infections (sepsis, bone infections)
- Acute cardiovascular events (stroke, DVT/PE, unstable angina)

Marking these as non-chronic prevents them from being flagged as persistent gaps in Method 4 processing.

#### Additional Fix: Description Typo

Fixed apostrophe in condition description:
- **Before:** "Parkinsons and Huntingtons Diseases"
- **After:** "Parkinson's and Huntington's Diseases"

---

### Change 6: Confidence Factor Override for On-Prem Parity (ref_method_prior_year.csv) (2026-05-09)

#### Overview

Updated `ref_method_prior_year.csv` to set CONFIDENCE_FACTOR (PERCENT_WEIGHT) to **99.9%** for all DURATION values ≤360 days, based on feedback from **Venu** and **Sarah** to accommodate on-prem override logic.

**PR Reference:** [#370](https://github.com/bhi-emids/EMIDS-Population-Advyzer/pull/370)

#### Business Context

The on-prem Population Advyzer system uses a different confidence factor calculation that essentially treats all gaps within the first year (≤360 days) as high confidence. To maintain parity between cloud and on-prem outputs during the transition period, the cloud reference data was updated to match.

#### Changes Made

**File:** `src/sql/data/ref_method_prior_year.csv`

**Before (Variable confidence based on DURATION and FREQUENCY):**

| LAST_DX_OCCURANCE | DX_FREQUENCY_MIN | DX_FREQUENCY_MAX | PERCENT_WEIGHT |
|-------------------|------------------|------------------|----------------|
| 30 | 1 | 3 | 0.996 |
| 60 | 1 | 3 | 0.899 |
| 90 | 1 | 3 | 0.842 |
| 120 | 1 | 3 | 0.774 |
| 180 | 1 | 3 | 0.750 |
| 360 | 1 | 3 | 0.750 |

**After (Flat 99.9% for all DURATION ≤360):**

| LAST_DX_OCCURANCE | DX_FREQUENCY_MIN | DX_FREQUENCY_MAX | PERCENT_WEIGHT |
|-------------------|------------------|------------------|----------------|
| 30 | 1 | 3 | **0.999** |
| 60 | 1 | 3 | **0.999** |
| 90 | 1 | 3 | **0.999** |
| 120 | 1 | 3 | **0.999** |
| 180 | 1 | 3 | **0.999** |
| 360 | 1 | 3 | **0.999** |

#### Scope of Change

- **DURATION 30-360 days:** All PERCENT_WEIGHT values changed to 0.999
- **DURATION >360 days:** Original graduated values retained (0.446 to 0.996 based on frequency)

#### Impact

| Aspect | Impact |
|--------|--------|
| Method 4 Confidence Factor | All gaps within 1 year now have 99.9% confidence |
| Gap Prioritization | Less differentiation between recent and older gaps within first year |
| On-Prem Parity | Cloud output now matches on-prem behavior |
| Gaps >360 days | Still use graduated confidence based on frequency |

#### Future Consideration

Once cloud becomes the primary system, this reference data may be reverted to the original graduated confidence factors for more nuanced gap prioritization.

---

## Related Files

| File | Purpose |
|------|---------|
| `config/constants/ma_ra_model_constants.yaml` | SEDITS diagnosis code lists |
| `src/spark/cms/cms_hcc_transformations.py` | SEDITS rule implementation |
| `src/spark/cms/cms_hcc_esrd_transformations.py` | SEDITS rules for ESRD model |
| `src/spark/helpers/transformations_commons.py` | HCC processing and risk_member_hcc population |
| `src/spark/gp_suspecting/gap_suspecting_helper.py` | Gap suspecting method implementations |
| `src/sql/data/icd_hcc_mapping.csv` | ICD to HCC mapping reference data |

---

## Revision History

| Date | Author | Description |
|------|--------|-------------|
| 2026-05-10 | Mukesh Kumar | Initial documentation - SEDITS, HCC-1, CHRONIC_HCC Method 4 |
| 2026-05-10 | Mukesh Kumar | Added FREQUENCY and DURATION calculation bug documentation |
| 2026-05-10 | Mukesh Kumar | Added Missing Claim Date Filter (2024 claims in RISK_YEAR 2023) bug |
| 2026-05-10 | Mukesh Kumar | Added Reference Data changes (PR #370) - Chronic flags and Confidence factors |
