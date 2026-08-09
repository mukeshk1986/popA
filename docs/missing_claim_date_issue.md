# Bug Investigation Report: 2024 Claims Appearing in RISK_YEAR 2023

## Summary
- **Bug ID:** 2024 claim appearing in RISK_YEAR 2023
- **Status:** Root cause identified
- **Severity:** High
- **Date Investigated:** 2026-05-02

## Symptoms
A claim with CLAIM_BID `3300000452183771` and MEMBER_BID `1100000001218227` has:
- FIRST_SERV_DT = **2024-09-26** (service date in 2024)
- But appears in `risk_member_hcc` table with **RISK_YEAR = 2023**

Multiple claims from 2024 and 2025 are incorrectly appearing in RISK_YEAR 2023.

## Expected Behavior
Claims should only be assigned to a RISK_YEAR that corresponds to their actual service date (FIRST_SERV_DT). A claim from September 2024 should never appear in RISK_YEAR 2023.

## Root Cause

**The issue is in the `ma_model_input_data.py` notebook at lines 205-209 and 294.**

### Problem 1: Claims loading does NOT filter by service date

Only filters by `SOURCE_LOAD_MONTH`, not by actual service date:

```python
# Line 205-209: Only filters by SOURCE_LOAD_MONTH, NOT by service date
medical_claims = read_table(
    spark, silver_schema, "MEDICAL_CLAIMS", None,
    filter_condition=[
        col("SOURCE_LOAD_MONTH") == year_month,  # This is the processing month, NOT service date
        lower(col("QUALIFY_CLAIMS")) == "y"
    ],
)
```

### Problem 2: TIME_PERIOD is assigned as a literal to ALL claims

Regardless of their actual service dates:

```python
# Line 294: Sets same TIME_PERIOD for ALL claims
risk_member_diag_df = (
    medical_claims_final_df.withColumn("TIME_PERIOD", lit(time_period))
    ...
)
```

### Problem 3: RISK_YEAR derived from TIME_PERIOD, not claim dates

The downstream `process_risk_member_hcc` function derives RISK_YEAR from TIME_PERIOD, not from the actual claim service dates (FIRST_SERV_DT).

### Why This Happens

1. When running for `risk_year=2023`, the notebook finds `TIME_PERIOD=5` (calendar year 2023)
2. It loads ALL claims that have `SOURCE_LOAD_MONTH=2025_12` (the latest load)
3. It assigns `TIME_PERIOD=5` to ALL those claims, including claims from 2024 and 2025
4. The downstream `process_risk_member_hcc` assigns `RISK_YEAR=2023` to all these records

## Evidence

### Data Analysis

```sql
-- risk_member_diag shows ALL claims for this member have TIME_PERIOD=9, 
-- regardless of their service dates ranging from 2023 to 2025
SELECT TIME_PERIOD, MIN(FIRST_SERV_DT) as min_dos, MAX(FIRST_SERV_DT) as max_dos
FROM pop_stg.uatplan2_curation.risk_member_diag 
WHERE RISK_MEMBER_ID = 1100000001218227
GROUP BY TIME_PERIOD;

-- Result: TIME_PERIOD=9, min_dos=2023-01-04, max_dos=2025-12-12
```

### Time Period Reference

| TIME_PERIOD | BEGIN_DATE | END_DATE | TYPE | Expected RISK_YEAR |
|-------------|------------|----------|------|-------------------|
| 5 | 2023-01-01 | 2023-12-31 | C | 2023 |
| 7 | 2024-01-01 | 2024-12-31 | C | 2024 |
| 9 | 2025-01-01 | 2025-12-31 | C | 2025 |

### Affected Records

51 records in `risk_member_hcc` have RISK_YEAR=2023 but service dates from 2024-01-16 to 2025-09-29.

## Code Locations

| File | Line | Issue |
|------|------|-------|
| `src/spark/data_prep/ma_model_input_data.py` | 205-209 | Claims loaded without service date filter |
| `src/spark/data_prep/ma_model_input_data.py` | 294 | TIME_PERIOD assigned as literal to all claims |
| `src/spark/helpers/transformations_commons.py` | 2438 | RISK_YEAR derived from TIME_PERIOD, not claim dates |

## Proposed Fix

### Option 1: Filter claims by service date in ma_model_input_data.py (Recommended)

Add a filter to ensure only claims within the time period's date range are processed:

```python
# After getting time_period, also get the date range
time_period_dates = read_table(
    spark, config_schema, "time_periods", ["TIME_PERIOD", "BEGIN_DATE", "END_DATE"],
    filter_condition=[col("TIME_PERIOD") == time_period]
).first()

tp_begin_date = time_period_dates["BEGIN_DATE"]
tp_end_date = time_period_dates["END_DATE"]

# Add date filter when loading medical_claims
medical_claims = read_table(
    spark, silver_schema, "MEDICAL_CLAIMS", None,
    filter_condition=[
        col("SOURCE_LOAD_MONTH") == year_month,
        lower(col("QUALIFY_CLAIMS")) == "y",
        col("FIRST_SERV_DT") >= tp_begin_date,  # ADD: Filter by service date
        col("FIRST_SERV_DT") <= tp_end_date      # ADD: Filter by service date
    ],
    is_table_mandatory=False
)
```

### Option 2: Filter risk_member_diag_df by service date

Before writing to risk_member_diag:

```python
risk_member_diag_df = (
    medical_claims_final_df
    .filter(
        (col("FIRST_SERV_DT") >= tp_begin_date) &
        (col("FIRST_SERV_DT") <= tp_end_date)
    )
    .withColumn("TIME_PERIOD", lit(time_period))
    ...
)
```

## Impact Analysis

- **Risk:** Medium - The fix involves modifying the data pipeline
- **Affected Components:** `ma_model_input_data.py`, potentially downstream `risk_member_hcc` processing
- **Regression Risk:** Low - The fix adds a filter that should only exclude records that shouldn't be there

## Test Plan

- [ ] Unit test: Verify claims outside time period date range are filtered out
- [ ] Integration test: Run full pipeline for TIME_PERIOD 5 (2023) and verify no 2024+ claims appear
- [ ] Data validation query:
  ```sql
  -- Verify FIRST_SERV_DT range matches TIME_PERIOD date range
  SELECT 
      rmd.TIME_PERIOD,
      tp.BEGIN_DATE as expected_min,
      tp.END_DATE as expected_max,
      MIN(rmd.FIRST_SERV_DT) as actual_min,
      MAX(rmd.FIRST_SERV_DT) as actual_max
  FROM risk_member_diag rmd
  JOIN time_periods tp ON rmd.TIME_PERIOD = tp.TIME_PERIOD
  GROUP BY rmd.TIME_PERIOD, tp.BEGIN_DATE, tp.END_DATE
  ```

## Deployment Notes

After implementing the fix:
1. Re-run `ma_model_input_data.py` for each TIME_PERIOD to correct the data
2. Re-run `cms_hcc_risk_score_calc.py` to regenerate `risk_member_hcc` with correct RISK_YEAR values
3. Verify data integrity with validation queries
