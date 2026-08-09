# Bug Investigation Report: Incorrect FREQUENCY Calculation in member_persistent_cc

## Summary
| Attribute | Value |
|-----------|-------|
| **Bug ID** | FREQ-001 |
| **Status** | Root cause identified |
| **Severity** | High |
| **Date Investigated** | 2026-05-03 |
| **Affected Table** | `member_persistent_cc` |
| **Affected Module** | Method 4 (Persistent CC Gap Suspecting) |

---

## Symptoms

For member `MEMBER_BID = 1100000001218227`, the `FREQUENCY` column in `member_persistent_cc` shows **1** for all "Open Suspect" HCCs, even when the member has multiple distinct claims for those conditions.

### Evidence from member_persistent_cc

| CC_CODE | RISK_YEAR | FREQUENCY (Actual) | FREQUENCY (Expected) |
|---------|-----------|--------------------|--------------------|
| HCC127 | 2025 | 1 | 6 |
| HCC198 | 2025 | 1 | 5 |
| HCC213 | 2025 | 1 | 24 |
| HCC253 | 2025 | 1 | 3 |
| HCC155 | 2025 | 1 | 1 |
| HCC002 | 2025 | 1 | 1 |
| HCC038 | 2025 | 1 | 1 |

### Verification Query (Actual Claim Counts)

```sql
WITH claim_diag AS (
    SELECT DISTINCT mc.CLAIM_BID, mc.MEMBER_BID, mc.FIRST_SERV_DT, pd.DIAG_CD
    FROM medical_claims mc
    JOIN professional_diag pd ON mc.CLAIM_BID = pd.PROFESSIONAL_BID
    WHERE mc.MEMBER_BID = 1100000001218227
    UNION
    SELECT DISTINCT mc.CLAIM_BID, mc.MEMBER_BID, mc.FIRST_SERV_DT, fd.DIAG_CD
    FROM medical_claims mc
    JOIN facility_diag fd ON mc.CLAIM_BID = fd.FACILITY_BID
    WHERE mc.MEMBER_BID = 1100000001218227
),
hcc_mapped AS (
    SELECT cd.*, CONCAT('HCC', LPAD(CAST(h.`CMS-HCC-MODEL-CATEGORY-V28` AS STRING), 3, '0')) as cc_code
    FROM claim_diag cd
    JOIN icd_hcc_mapping h ON cd.DIAG_CD = h.DIAGNOSISCODE
    WHERE h.`CMS-HCC-MODEL-CATEGORY-V28` IS NOT NULL
)
SELECT cc_code, COUNT(DISTINCT CLAIM_BID) as distinct_claim_count
FROM hcc_mapped
WHERE cc_code IN ('HCC127', 'HCC155', 'HCC198', 'HCC253', 'HCC002', 'HCC038', 'HCC213')
GROUP BY cc_code;
```

**Result:**
| cc_code | distinct_claim_count |
|---------|---------------------|
| HCC002 | 1 |
| HCC038 | 1 |
| HCC127 | **6** |
| HCC155 | 1 |
| HCC198 | **5** |
| HCC213 | **24** |
| HCC253 | **3** |

---

## Expected Behavior

FREQUENCY should represent the count of **distinct CLAIM_BIDs** for each (MEMBER_BID, CC_CODE, RISK_YEAR, SAS_MODEL_VERSION) combination across all claims data, not just from a single deduplicated row.

---

## Root Cause

### Problem Location
**File:** `src/spark/cms/member_persistent_hcc.py`  
**Function:** `build_df_raw_prior_year_filter()` (lines 32-42)

### Problematic Code

```python
# Lines 32-42 in member_persistent_hcc.py
window_latest_batch_by_year = (
    Window
    .partitionBy("RISK_YEAR", "SAS_MODEL_VERSION", "MEMBER_BID", "RISK_TYPE_DETAIL_ID")
    .orderBy(F.col("CREATED_DATE").desc(), F.col("CLAIM_BID").desc())
)

df_hcc_prior_years_latest_batch = (
    df_hcc_prior_years_raw
    .withColumn("rnk", F.row_number().over(window_latest_batch_by_year))
    .filter(F.col("rnk") == 1)  # <-- BUG: Keeps only 1 row per HCC
    .drop("rnk")
)
```

### Why This Causes the Bug

1. **Before deduplication:** Raw data contains multiple rows per HCC (one per claim)
   - Example: HCC127 has 6 distinct claims = 6 rows

2. **Deduplication happens too early:** The `row_number() + filter(rnk == 1)` reduces each HCC to exactly 1 row
   - Example: HCC127 reduced from 6 rows to 1 row

3. **FREQUENCY calculation receives deduplicated data:** When `compute_prior_year_confidence_factor_without_dropping()` runs:
   ```python
   # Lines 570-586 - FREQUENCY calculation
   df_with_frequency = (
       df_raw_prior_year_filter  # <-- Already deduplicated! Only 1 row per HCC
       .withColumn("claim_marker", F.row_number().over(window_by_claim))
       .withColumn(
           "FREQUENCY",
           F.sum(F.when(F.col("claim_marker") == 1, F.lit(1)).otherwise(F.lit(0)))
           .over(window_by_member_hcc)
       )
   )
   ```
   - Since input has only 1 row per HCC, `countDistinct(CLAIM_BID)` always returns 1

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CURRENT (BUGGY) FLOW                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  risk_member_hcc (raw)                                                   │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │ MEMBER_BID | CC_CODE | CLAIM_BID | CLAIM_DATE | ...      │           │
│  │ 123        | HCC127  | 1001      | 2023-01-18 |          │           │
│  │ 123        | HCC127  | 1002      | 2023-05-20 |          │           │
│  │ 123        | HCC127  | 1003      | 2023-07-28 |          │  6 rows   │
│  │ 123        | HCC127  | 1004      | 2023-08-15 |          │           │
│  │ 123        | HCC127  | 1005      | 2023-11-01 |          │           │
│  │ 123        | HCC127  | 1006      | 2024-01-16 |          │           │
│  └──────────────────────────────────────────────────────────┘           │
│                              │                                           │
│                              ▼                                           │
│  build_df_raw_prior_year_filter() - DEDUPLICATION (TOO EARLY!)          │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │ row_number().over(partition).filter(rnk == 1)            │           │
│  └──────────────────────────────────────────────────────────┘           │
│                              │                                           │
│                              ▼                                           │
│  Deduplicated DataFrame                                                  │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │ MEMBER_BID | CC_CODE | CLAIM_BID | CLAIM_DATE | ...      │           │
│  │ 123        | HCC127  | 1006      | 2024-01-16 |          │  1 row!   │
│  └──────────────────────────────────────────────────────────┘           │
│                              │                                           │
│                              ▼                                           │
│  compute_prior_year_confidence_factor_without_dropping()                 │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │ FREQUENCY = countDistinct(CLAIM_BID) = 1  ← WRONG!       │           │
│  │ (Should be 6)                                             │           │
│  └──────────────────────────────────────────────────────────┘           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Impact Analysis

### Affected Calculations

1. **FREQUENCY:** Always returns 1 instead of actual distinct claim count
2. **CONFIDENCE_FACTOR:** Incorrectly calculated because it depends on FREQUENCY
   - Join condition: `FREQUENCY >= DX_FREQUENCY_MIN AND FREQUENCY <= DX_FREQUENCY_MAX`
   - With FREQUENCY=1, many valid confidence factor matches are missed

### Business Impact

- **Understated confidence scores:** Members with high claim frequency for chronic conditions get lower confidence factors
- **Incorrect gap prioritization:** Gaps that should be high-priority (many claims = high confidence) appear as low-priority
- **RAF impact miscalculation:** Potential revenue impact is underestimated

---

## Proposed Fix

### Option 1: Calculate FREQUENCY Before Deduplication (Recommended)

Move the FREQUENCY calculation to happen on raw data, then carry the result through deduplication.

```python
def build_df_raw_prior_year_filter(
    df_raw_data: DataFrame,
    previous_years: list,
    final_cols: list,
    account_name: str
) -> DataFrame:
    """
    Filter raw data to previous years, calculate FREQUENCY on full data,
    then deduplicate while preserving the calculated FREQUENCY.
    """
    try:
        current_ts = F.current_timestamp()

        # Raw HCC records narrowed to the requested prior risk years only
        df_hcc_prior_years_raw = df_raw_data.filter(F.col("RISK_YEAR").isin(previous_years))

        # === FIX: Calculate FREQUENCY on raw data BEFORE deduplication ===
        partition_keys = ["RISK_YEAR", "SAS_MODEL_VERSION", "MEMBER_BID", "RISK_TYPE_DETAIL_ID", "HOME_PLAN_ID_CD"]
        
        # Count distinct claims per HCC
        df_with_frequency = (
            df_hcc_prior_years_raw
            .withColumn(
                "FREQUENCY",
                F.count(F.col("CLAIM_BID")).over(
                    Window.partitionBy(*partition_keys)
                )
            )
        )
        
        # Get max CLAIM_DATE for DURATION calculation
        df_with_duration_base = (
            df_with_frequency
            .withColumn(
                "MAX_CLAIM_DATE",
                F.max("CLAIM_DATE").over(Window.partitionBy(*partition_keys))
            )
        )

        # NOW deduplicate - but FREQUENCY and MAX_CLAIM_DATE are already calculated
        window_latest_batch_by_year = (
            Window
            .partitionBy("RISK_YEAR", "SAS_MODEL_VERSION", "MEMBER_BID", "RISK_TYPE_DETAIL_ID")
            .orderBy(F.col("CREATED_DATE").desc(), F.col("CLAIM_BID").desc())
        )

        df_hcc_prior_years_latest_batch = (
            df_with_duration_base
            .withColumn("rnk", F.row_number().over(window_latest_batch_by_year))
            .filter(F.col("rnk") == 1)
            .drop("rnk")
        )

        # Continue with existing schema mapping...
        df_prior_year_persistence_schema = (
            df_hcc_prior_years_latest_batch
            .withColumnRenamed("RISK_TYPE_DETAIL_ID", "CC_CODE")
            .withColumnRenamed("RISK_TYPE_DETAIL_DESC", "CC_DESCRIPTION")
            .withColumn("PERSISTENT_STATUS", F.lit(None).cast("string"))
            .withColumn("PERSISTENT_STATUS_REASON", F.lit(None).cast("string"))
            .withColumn("SUSPECT_STATUS", F.lit(None).cast("string"))
            # FREQUENCY is already calculated correctly
            .withColumn("DURATION", F.lit(None).cast("int"))  # Will be calculated in next step
            .withColumn("CONFIDENCE_FACTOR", F.lit(None).cast("decimal(7,6)"))
            .withColumn("ACTIVE_IND", F.lit("Y"))
            .withColumn("CREATED_BY", F.lit(account_name))
            .withColumn("CREATED_DATE", current_ts)
            .withColumn("UPDATED_BY", F.lit(account_name))
            .withColumn("UPDATED_DATE", current_ts)
            .select(*final_cols)
        )

        return df_prior_year_persistence_schema

    except Exception as e:
        logger.error(f"Error in build_df_raw_prior_year_filter: {e}")
        raise Exception(f"Failed in build_df_raw_prior_year_filter: {e}")
```

### Option 2: Pass Raw Data to compute_prior_year_confidence_factor

Modify the pipeline to pass raw (non-deduplicated) data to `compute_prior_year_confidence_factor_without_dropping()`, then deduplicate after FREQUENCY is calculated.

---

## Test Plan

### Unit Test

```python
def test_frequency_calculation_multiple_claims():
    """Verify FREQUENCY correctly counts distinct claims per HCC."""
    # Setup: Create test data with 3 distinct claims for same HCC
    test_data = [
        (123, "HCC127", 1001, "2023-01-18"),
        (123, "HCC127", 1002, "2023-05-20"),
        (123, "HCC127", 1003, "2023-07-28"),
    ]
    df = spark.createDataFrame(test_data, ["MEMBER_BID", "CC_CODE", "CLAIM_BID", "CLAIM_DATE"])
    
    # Execute: Run frequency calculation
    result = calculate_frequency(df)
    
    # Assert: FREQUENCY should be 3
    assert result.filter(F.col("CC_CODE") == "HCC127").first()["FREQUENCY"] == 3
```

### Integration Test

```sql
-- After fix, verify FREQUENCY matches actual claim counts
WITH actual_counts AS (
    SELECT 
        MEMBER_BID, CC_CODE, 
        COUNT(DISTINCT CLAIM_BID) as expected_frequency
    FROM risk_member_hcc
    WHERE DX_SOURCE = 'claim'
    GROUP BY MEMBER_BID, CC_CODE
),
persistent_data AS (
    SELECT MEMBER_BID, CC_CODE, FREQUENCY as actual_frequency
    FROM member_persistent_cc
    WHERE SUSPECT_STATUS = 'Open Suspect'
)
SELECT 
    a.MEMBER_BID, a.CC_CODE, 
    a.expected_frequency, 
    p.actual_frequency,
    CASE WHEN a.expected_frequency = p.actual_frequency THEN 'PASS' ELSE 'FAIL' END as test_result
FROM actual_counts a
JOIN persistent_data p ON a.MEMBER_BID = p.MEMBER_BID AND a.CC_CODE = p.CC_CODE
WHERE a.expected_frequency != p.actual_frequency;

-- Expected: No rows returned (all frequencies match)
```

---

## Code Locations

| File | Line | Issue |
|------|------|-------|
| `src/spark/cms/member_persistent_hcc.py` | 32-42 | Deduplication happens before FREQUENCY calculation |
| `src/spark/cms/member_persistent_hcc.py` | 570-586 | FREQUENCY calculation receives already-deduplicated data |

---

## Deployment Notes

After implementing the fix:

1. **Backfill Required:** Re-run `member_persistent_hcc` pipeline for all affected plans/risk years
2. **Downstream Impact:** `suspected_gaps` and `risk_member_hcc` tables will need regeneration for Method 4 gaps
3. **Validation:** Run integration test query above to verify fix

---

## Change History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-05-03 | Data Architecture Team | Initial investigation and documentation |
