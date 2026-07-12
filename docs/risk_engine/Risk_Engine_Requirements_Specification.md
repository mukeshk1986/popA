# Risk Engine - Detailed Requirements Specification

**Document Version:** 1.0  
**Reverse Engineered from:** Population Advyzer Codebase  
**Date:** 2026-05-02  
**Classification:** Technical Requirements Document

---

## 1. Executive Summary

### 1.1 Purpose
The Risk Engine is a comprehensive Medicare Advantage (MA) Risk Adjustment calculation system designed to compute CMS-HCC (Hierarchical Condition Category) risk scores for health plan members. It supports multiple risk models and versions to calculate expected healthcare costs for Medicare Advantage populations.

### 1.2 Business Context
- **Domain:** Medicare Advantage Risk Adjustment (MA-RA)
- **Regulatory Framework:** CMS (Centers for Medicare & Medicaid Services) Risk Adjustment Model
- **Primary Use Cases:** 
  - CMS payment reconciliation
  - Plan financial forecasting
  - Member risk stratification
  - Gap closure analytics

---

## 2. Functional Requirements

### 2.1 Supported Risk Models

The system SHALL support the following CMS risk adjustment models:

| Model ID | Model Name | Description | Supported Versions |
|----------|------------|-------------|-------------------|
| CMS-HCC | CMS Hierarchical Condition Category | Part C (Medical) risk adjustment | v22, v24, v28 |
| CMS-RxHCC | CMS Prescription Drug HCC | Part D (Pharmacy) risk adjustment | v05, v08 |
| CMS-HCC-ESRD | CMS HCC End-Stage Renal Disease | ESRD population risk adjustment | v21, v24 |

### 2.2 Input Parameters

The system SHALL accept the following runtime parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | String | Yes | Risk model identifier (cms_hcc, rx_hcc, cms_hcc_esrd) |
| `version` | String | Yes | Model version (v22, v24, v28, v05, v08, v21) |
| `time_period` | Integer | Yes | Scoring period identifier (1, 2, etc.) |
| `home_plan_id` | String | Yes | Health plan identifier |
| `contract` | String | Yes | CMS contract number |
| `plan_name` | String | Yes | Plan name for schema resolution |
| `source_load_month` | String | Yes | Month of data load (01-12) |
| `source_load_year` | String | Yes | Year of data load (current year - 3 to current year) |
| `incl_supplemental_mmr` | Enum | Yes | Include supplemental MMR data (Y/N) |
| `incl_pseudo_claim` | Enum | No | Include pseudo claims (Y/N, default N) |
| `env` | Enum | Yes | Environment (DEV, QA, UAT, PROD) |

### 2.3 Risk Score Calculation Pipeline

#### 2.3.1 Data Loading Phase
**REQ-LOAD-001:** The system SHALL load member enrollment data filtered by:
- Plan ID and Contract
- Source load month/year
- Coverage period overlap with time period dates

**REQ-LOAD-002:** The system SHALL load member diagnosis data filtered by:
- Plan ID and Contract
- Time period date range (service dates within coverage period)
- Pseudo claim exclusion rules based on `incl_pseudo_claim` parameter

**REQ-LOAD-003:** The system SHALL load the following reference tables:
- `coefficient_scores` - Model coefficients for scoring
- `hierarchy_config` - HCC hierarchy suppression rules
- `interaction_coefficients` - Disease interaction rules
- `adjustment_factors` - Normalization and coding pattern adjustments
- `icd_hcc_mapping` - ICD-10 to HCC mapping
- `community_model_rules` - Segment assignment rules
- `time_periods` - Time period definitions
- `version_weightage_risk_year` - Version blending weights

#### 2.3.2 Member Age Calculation
**REQ-AGE-001:** Member age SHALL be calculated as of February 1st of the payment year:
- For Calendar year periods (TIME_PERIOD_TYPE_CD = 'C'): Payment year = BEGIN_DATE year + 1
- For Fiscal year periods (TIME_PERIOD_TYPE_CD = 'F'): Payment year = END_DATE year + 1

**REQ-AGE-002:** Age calculation formula:
```
AGE_YR = FLOOR(MONTHS_BETWEEN(reference_date, MEMB_BRTH_DT) / 12)
```

#### 2.3.3 Original Disability Status
**REQ-ORIGDS-001:** The system SHALL compute disability indicators:
- `DISABL = 1` if AGE_YR < 65 AND OREC != "0", else 0
- `ORIGDS = 1` if OREC = "1" AND DISABL = 0, else 0

#### 2.3.4 Community Model Assignment
**REQ-MODEL-001:** The system SHALL assign community model segments based on configurable rules:
- Rules are evaluated in priority order (SNPNE_ prefixed first, then NE_)
- Each rule contains a SQL condition expression and segment assignment
- Default segment = "Unknown" if no rule matches

**REQ-MODEL-002:** Community Model Segments include:
- **CMS-HCC v24/v28:**
  - CNA (Community Non-Aged)
  - CFA (Community Full Benefit Dual Aged)
  - CPA (Community Partial Benefit Dual Aged)
  - INS (Institutional)
  - NE_ (New Enrollee variants)
  - SNPNE_ (Special Needs Plan New Enrollee)

- **CMS-HCC-ESRD:**
  - DI (Dialysis)
  - GC (Graft Community)
  - GI (Graft Institutional)
  - DNE/GNE (Dialysis/Graft New Enrollee)

- **RxHCC:**
  - Rx_NE_NoLo, Rx_NE_Lo, Rx_NE_LTI (New Enrollee variants)

#### 2.3.5 Sex/Age Edit (SEDIT) Rules
**REQ-SEDIT-001:** The system SHALL apply sex and age-based diagnosis edits per model version:

**v28 Rules:**
| Condition | Result HCC |
|-----------|------------|
| Gender = Female AND DIAG_CD in SEDITS_DIAG1 (D66, D67 - Hemophilia) | HCC 112 |
| Age < 18 AND DIAG_CD in SEDITS_DIAG2 (J41x, J42, J43x, J44x - COPD) | Exclude (-1) |
| Age < 50 AND DIAG_CD in SEDITS_DIAG3 (C50xxx - Breast Cancer) | HCC 22 |
| Age >= 2 AND DIAG_CD in SEDITS_DIAG4 (P04x, P27x - Perinatal) | Exclude (-1) |

**v24 Rules:**
| Condition | Result HCC |
|-----------|------------|
| Gender = Female AND DIAG_CD in SEDITS_DIAG1 | HCC 48 |
| Age < 18 AND DIAG_CD in SEDITS_DIAG2 | HCC 112 |
| (Age < 6 OR Age > 18) AND DIAG_CD in SEDITS_DIAG5 (F3481) | Exclude (-1) |

#### 2.3.6 ICD-to-HCC Mapping
**REQ-MAP-001:** The system SHALL map ICD-10 diagnosis codes to HCC categories using the `icd_hcc_mapping` reference table.

**REQ-MAP-002:** Mapping SHALL use version-specific columns:
- v24: `CMS-HCC-MODEL-CATEGORY-V24`
- v28: `CMS-HCC-MODEL-CATEGORY-V28`
- ESRD v21: `CMS-HCC-ESRD-MODEL-CATEGORY-V21`
- ESRD v24: `CMS-HCC-ESRD-MODEL-CATEGORY-V24`
- RxHCC v05: `RXHCC-MODEL-CATEGORY-V05`
- RxHCC v08: `RXHCC-MODEL-CATEGORY-V08`

**REQ-MAP-003:** The system SHALL aggregate unique HCC codes per member after applying SEDIT rules.

#### 2.3.7 HCC Hierarchy Suppression
**REQ-HIER-001:** The system SHALL apply hierarchy rules to suppress child HCCs when parent HCCs are present.

**REQ-HIER-002:** Hierarchy rules are stored in `hierarchy_config` table with:
- PARENT: Parent HCC code (retained)
- CHILD: Comma-separated list of child HCC codes (suppressed when parent exists)

**REQ-HIER-003:** Example hierarchies:
- HCC 17 (Diabetes with Acute Complications) suppresses HCC 18, 19
- HCC 8 (Metastatic Cancer) suppresses HCC 9, 10, 11, 12
- HCC 85 (CHF) suppresses HCC 86, 87, 88

#### 2.3.8 Score Assignment
**REQ-SCORE-001:** The system SHALL calculate the following score components:

| Score Component | Description | Source |
|-----------------|-------------|--------|
| DEMOG_SCORE | Demographic score based on age, gender, and community model | coefficient_scores.SCORE where NAME = {community_model}_{gender}{age} |
| HCC_SCORE | Sum of individual HCC scores | coefficient_scores.SCORE where NAME = {community_model}_HCC{code} |
| ORIGDS_SCORE | Original disability adjustment | coefficient_scores.SCORE where NAME = {community_model}_OriginallyDisabled_{Male/Female} |
| HCC_COUNT_PAYMENT_SCORE | Payment HCC count adjustment | coefficient_scores.SCORE where NAME matches HCC count pattern |
| INTERACTION_SCORE | Disease interaction adjustments | interaction_coefficients |

**REQ-SCORE-002:** Demographic coefficient key format:
- New Enrollee models: `{community_model}{gender}{age}` (e.g., "NE_NMCAID_NORIGDIS_NEM65")
- Standard models: `{community_model}_{gender}{age}` (e.g., "CNA_NMCAID_NORIGDIS_M65")

**REQ-SCORE-003:** HCC coefficient key format: `{community_model}_HCC{code}` (e.g., "CNA_NMCAID_NORIGDIS_HCC85")

#### 2.3.9 Interaction Rules
**REQ-INT-001:** The system SHALL apply disease interaction rules when members have specific HCC combinations.

**REQ-INT-002:** Interaction rule structure:
- `INTERACTION_GROUP_PRIMARY`: Comma-separated list of primary HCCs
- `INTERACTION_GROUP_SECONDARY`: Comma-separated list of secondary HCCs
- `SCORE`: Additional score if member has at least one HCC from each group

**REQ-INT-003:** Total interaction score = SUM of all matching interaction rule scores.

#### 2.3.10 Final Score Calculation
**REQ-CALC-001:** Raw Risk Score calculation:
```
RAW_RISK_SCORE = DEMOG_SCORE + HCC_SCORE + HCC_COUNT_PAYMENT_SCORE + INTERACTION_SCORE + ORIGDS_SCORE
```

**REQ-CALC-002:** Normalized Risk Score calculation:
```
NORMALIZED_RISK_SCORE = RAW_RISK_SCORE / NORMALIZATION_FACTOR
```

**REQ-CALC-003:** Payment Risk Score calculation:
```
PAYMENT_RISK_SCORE = NORMALIZED_RISK_SCORE * (1 - CODING_PATTERN_ADJUSTMENT)
```

**REQ-CALC-004:** Weighted Risk Score calculation:
```
WEIGHTED_RISK_SCORE = PAYMENT_RISK_SCORE * WEIGHTAGE
```

#### 2.3.11 Blended Risk Score
**REQ-BLEND-001:** When multiple model versions are in effect (transition years), the system SHALL calculate:
```
BLENDED_RISK_SCORE = SUM(WEIGHTED_RISK_SCORE) across all applicable versions
```

### 2.4 Output Tables

#### 2.4.1 risk_member_output
Primary output table containing member-level risk scores.

| Column | Type | Description |
|--------|------|-------------|
| RISK_MEMBER_ID | BIGINT | Unique member identifier |
| HOME_PLAN_ID_CD | VARCHAR(3) | Plan identifier |
| MEMB_ID_CD | VARCHAR(22) | Member ID |
| MEMBER_AGE | INT | Member age as of Feb 1 payment year |
| MEMBER_GENDER | VARCHAR(1) | M/F |
| RISK_MODEL_TYPE | VARCHAR(20) | cms_hcc, rx_hcc, cms_hcc_esrd |
| RISK_MODEL_VERSION | VARCHAR(4) | v22, v24, v28, v05, v08, v21 |
| RISK_MODEL_YEAR | INT | SAS model year (e.g., 2025) |
| RISK_MODEL_SEGMENT | VARCHAR(25) | Community model segment |
| DEMOG_SCORE | DOUBLE | Demographic score |
| HCC_SCORE | DOUBLE | Total HCC score |
| INTERACTION_SCORE | DOUBLE | Interaction score |
| ORIGDS_SCORE | DOUBLE | Original disability score |
| HCC_COUNT_PAYMENT_SCORE | DOUBLE | HCC count payment score |
| RISK_SCORE_RAW | DOUBLE | Raw (unadjusted) risk score |
| RISK_SCORE_NORMALIZED | DOUBLE | Normalized risk score |
| RISK_SCORE_PAYMENT_HCC | DOUBLE | Payment-adjusted risk score |
| WEIGHTED_RISK_SCORE | DOUBLE | Version-weighted risk score |
| WEIGHTAGE | DOUBLE | Version weight factor |
| DIAG_CD_TO_HCC_JSON_LIST | ARRAY<STRING> | JSON mapping of diagnosis to HCC |
| HCC_TO_SCORE_JSON_LIST | ARRAY<STRING> | JSON mapping of HCC to score |
| SAS_MODEL_VERSION | VARCHAR(50) | CMS SAS model version identifier |

#### 2.4.2 risk_member_hcc
Detailed HCC-level output per member.

| Column | Type | Description |
|--------|------|-------------|
| RISK_MEMBER_ID | BIGINT | Member identifier |
| HCC_CODE | VARCHAR(10) | HCC code |
| HCC_SCORE | DOUBLE | Individual HCC score |
| CLAIM_TYPE | VARCHAR(20) | Source claim type |
| CLAIM_BID | BIGINT | Claim identifier |
| CLAIM_DATE | DATE | Service date |
| DIAG_CD | VARCHAR(8) | Source diagnosis code |
| CHRONIC | TINYINT | Chronic condition flag |

#### 2.4.3 risk_member_hcc_summary
Summary of HCC assignments per member.

---

## 3. Reference Data Requirements

### 3.1 ICD-to-HCC Mapping
**REQ-REF-001:** The system SHALL maintain ICD-10 to HCC mapping for all supported model versions.

**REQ-REF-002:** Mapping SHALL be version-year specific to support payment year transitions.

### 3.2 Coefficient Scores
**REQ-REF-003:** Coefficient scores SHALL be maintained by:
- Model (cms_hcc, rx_hcc, cms_hcc_esrd)
- Version (v22, v24, v28, etc.)
- Coefficient type (demographic, HCC, interaction, ORIGDS)

### 3.3 Hierarchy Configuration
**REQ-REF-004:** Hierarchy rules SHALL specify parent-child HCC relationships per model/version.

### 3.4 Adjustment Factors
**REQ-REF-005:** The system SHALL maintain:
- Normalization factors per model/version
- Coding pattern adjustments per model/version

### 3.5 Time Periods
**REQ-REF-006:** Time periods SHALL define:
- Begin and end dates for claim filtering
- Time period type (Calendar 'C' or Fiscal 'F')

### 3.6 Version Weightage
**REQ-REF-007:** Version weightage SHALL support blending during CMS transition years (e.g., 67% v24 / 33% v28).

---

## 4. Non-Functional Requirements

### 4.1 Performance
**REQ-PERF-001:** The system SHALL process 1 million member records within 30 minutes on standard Databricks cluster.

**REQ-PERF-002:** Reference tables SHALL be broadcast joined for optimal performance.

**REQ-PERF-003:** Large DataFrames SHALL be cached/persisted at appropriate pipeline stages.

### 4.2 Scalability
**REQ-SCALE-001:** The system SHALL support horizontal scaling through Spark partitioning.

**REQ-SCALE-002:** Data SHALL be partitioned by SOURCE_LOAD_MONTH for efficient querying.

### 4.3 Data Quality
**REQ-QUAL-001:** The system SHALL validate that time_period parameter maps to a valid entry in time_periods table.

**REQ-QUAL-002:** The system SHALL validate that joined DataFrames are not empty at critical stages.

**REQ-QUAL-003:** All score calculations SHALL be rounded to 3 decimal places.

### 4.4 Auditability
**REQ-AUDIT-001:** Output records SHALL include:
- CREATED_BY (executing user)
- CREATED_DATE (execution timestamp)
- CYCLE_RUN_DATE (scoring run timestamp)

**REQ-AUDIT-002:** The system SHALL preserve:
- DIAG_CD_TO_HCC_JSON_LIST - Traceability from diagnosis to HCC
- HCC_TO_SCORE_JSON_LIST - Traceability from HCC to score

### 4.5 Security
**REQ-SEC-001:** The system SHALL NOT log any PII/PHI in application logs.

**REQ-SEC-002:** Credentials SHALL be accessed via `dbutils.secrets`.

### 4.6 Compliance
**REQ-COMP-001:** Risk score calculations SHALL align with CMS published SAS algorithms.

**REQ-COMP-002:** SAS_MODEL_VERSION SHALL be captured for regulatory traceability.

---

## 5. Data Architecture

### 5.1 Storage Architecture (Medallion)
```
Bronze Layer (Ingestion):
├── member_demographics
├── medical_claims
├── pharmacy_claims
└── medicare_mao_004

Silver Layer (Transformation):
├── risk_member (curated enrollment)
├── risk_member_diag (curated diagnoses)
└── medicare_mao (cleaned MAO data)

Gold Layer (Curation):
├── risk_member_output (final scores)
├── risk_member_hcc (HCC details)
├── risk_member_hcc_summary
└── risk_member_output_blended (view)
```

### 5.2 Catalog Structure
```
pop_{env}/
├── {plan_name}_ingestion/    # Raw data
├── {plan_name}_transformation/ # Cleaned data
├── {plan_name}_curation/     # Risk scores
└── ma_reference/             # Reference tables
```

---

## 6. Integration Points

### 6.1 Upstream Dependencies
- Member enrollment data (MMR files)
- Medical claims (facility, professional)
- Pharmacy claims
- Medicare MAO 004 files (CMS diagnosis feedback)
- Supplemental diagnosis sources

### 6.2 Downstream Consumers
- MA Dashboard (risk visualization)
- Gap Suspecting Module (HCC opportunity identification)
- Financial forecasting systems
- CMS submission workflows

---

## 7. Error Handling

### 7.1 Validation Errors
- Invalid time_period: Raise exception with descriptive message
- Empty DataFrame after critical join: Raise exception
- Invalid model/version combination: Raise exception

### 7.2 Processing Errors
- All exceptions SHALL be logged with full stack trace
- Pipeline SHALL fail-fast on critical errors
- Execution time SHALL be logged on completion

---

## 8. Configuration Management

### 8.1 Environment Configuration
Location: `config/environments/{env}/values.yaml`
- catalog: Target Unity Catalog
- config_schema: Reference data schema
- target_table: Output table name
- notebook_time_out: Execution timeout

### 8.2 Model Constants
Location: `config/constants/ma_ra_model_constants.yaml`
- SEDITS_DIAG_CODES: Sex/age edit diagnosis lists
- DEMOG_MODELS: Demographic model segments
- DNE_GNE_MODELS: New enrollee model segments

### 8.3 Column Mappings
Location: `config/sql/file_read_meta.yaml`
- RISK_MEMBER_COLUMNS: Member table columns
- RISK_MEMBER_DIAG_COLUMNS: Diagnosis table columns
- RISK_MEMBER_OUTPUT_COLUMNS: Output table columns

---

## 9. Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-05-02 | Reverse Engineered | Initial requirements from codebase analysis |

---

## Appendix A: HCC Category Reference

| Category ID | Category Name | Example HCCs |
|-------------|---------------|--------------|
| 2 | Cardiovascular | 85, 86, 87, 88, 96 |
| 3 | Gastrointestinal | 21, 33, 35 |
| 4 | Renal | 134, 135, 136, 137, 138, 139, 140, 141 |
| 5 | Musculoskeletal | 39, 40, 170 |
| 6 | Neurological | 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99, 100, 103, 104 |
| 7 | Skin | 157, 158, 159, 160, 162 |
| 8 | Hematological | 46, 48 |
| 9 | Respiratory | 110, 111, 112 |
| 10 | Metabolic/Endocrine | 17, 18, 19, 23, 34 |
| 11 | Immune | 1, 47 |
| 12 | Infectious | 2, 6, 114, 115 |
| 13 | Psychiatric | 51, 52, 54, 55, 57, 58 |
| 14 | Neoplasm | 8, 9, 10, 11, 12 |

---

## Appendix B: Sample Calculation

**Member Profile:**
- Age: 72 (as of Feb 1, 2025)
- Gender: Male
- OREC: 0 (Age-in)
- Medicaid: No
- Institutional: No
- HCCs: 85 (CHF), 19 (Diabetes w/o Complication)

**Model Assignment:** CNA_NMCAID_NORIGDIS (Community Non-Dual Aged, Non-Medicaid, No Original Disability)

**Score Calculation (v28 example):**
1. DEMOG_SCORE = Coefficient for CNA_NMCAID_NORIGDIS_M72 = 0.298
2. HCC 85 Score = Coefficient for CNA_NMCAID_NORIGDIS_HCC85 = 0.368
3. HCC 19 Score = Coefficient for CNA_NMCAID_NORIGDIS_HCC19 = 0.104
4. HCC_SCORE = 0.368 + 0.104 = 0.472
5. ORIGDS_SCORE = 0 (ORIGDS = 0)
6. INTERACTION_SCORE = 0 (no qualifying interactions)
7. HCC_COUNT_PAYMENT_SCORE = 0 (only 2 HCCs)
8. **RAW_RISK_SCORE = 0.298 + 0.472 + 0 + 0 + 0 = 0.770**
9. NORMALIZED_RISK_SCORE = 0.770 / 1.015 = 0.759
10. PAYMENT_RISK_SCORE = 0.759 * (1 - 0.058) = 0.715
11. WEIGHTED_RISK_SCORE = 0.715 * 0.67 = 0.479 (if v28 weight = 67%)
