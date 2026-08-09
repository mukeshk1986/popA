# Risk Engine - High Level Solution Architecture

## Executive Summary

The Risk Engine is a comprehensive CMS HCC (Hierarchical Condition Category) risk scoring system that calculates member-level risk scores based on demographic data, clinical diagnoses, and pharmaceutical profiles. It processes 400M-500M+ records monthly to assign accurate risk adjustments for quality and financial planning purposes.

**Key Metrics:**
- **Processing Volume**: 400M-500M+ draft records per plan
- **Processing Time**: 30-90 minutes per plan
- **Accuracy**: High through CMS HCC v24/v28 and RxHCC models
- **Financial Impact**: $3,000-$8,000 per HCC per member captured

---

## Architecture Overview

### System Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Scoring Models Layer                      │
│  (CMS HCC v24/v28, RxHCC v05/v08, ESRD v21/v24)            │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                  Transformation Layer                        │
│  (ICD→HCC Mapping, Hierarchy, Scoring, Interactions)        │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                  Curation Layer                              │
│  (Deduplication, BID Keying, Partitioning)                  │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                  Ingestion Layer                             │
│  (18 Stage Tables: Claims, Enrollment, Pharmacy, etc.)      │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                  Data Sources                                │
│  (S3 Inbox, Health Plan EDI Files)                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### 1. Ingestion Layer

**Purpose**: Receive and stage raw data from health plan sources

**Data Sources (18 Stage Tables)**:
- **Claims Data**: Medical, Pharmacy, Dental, Vision
- **Enrollment Data**: Member demographics, plan assignments, coverage dates
- **Provider Data**: Network providers, facilities, professional credentials
- **Supplemental Data**: Lab results, biometric data, authorizations

**Key Tables**:
- `stage_member`: Member demographics and enrollment status
- `stage_claims_medical`: Primary source for diagnoses and procedures
- `stage_claims_pharmacy`: Pharmaceutical claims with drug codes
- `stage_facility_header`: Facility and visit information
- `stage_professional`: Professional provider details
- `stage_lab`: Laboratory test results and values

**Validation**: Great Expectations framework with null checks, uniqueness constraints, and referential integrity

---

### 2. Transformation Layer

**Purpose**: Apply CMS HCC logic, qualification rules, and business transformations

**Key Transformations**:

#### A. Risk Member Table
- **Source**: stage_member + enrollment history
- **Purpose**: Standardized member reference with active coverage periods
- **Key Columns**: 
  - `MEMBER_ID`, `MEMB_PLAN_ID`, `BID` (Business ID surrogate key)
  - `ENROLLMENT_MONTH_BEGIN`, `ENROLLMENT_MONTH_END`
  - `ACTIVE_FLAG`, `METHOD_ID`

#### B. Risk Member Diagnosis Table
- **Source**: stage_claims_medical + CMS qualifying rules
- **Purpose**: ICD codes mapped to HCC categories with qualifier validation
- **Key Columns**:
  - `MEMBER_ID`, `ICD_CODE`, `ICD_DESCRIPTION`
  - `HCC_CODE`, `HCC_DESCRIPTION`
  - `QUALIFYING_PROCEDURE`, `CONDITION_ONSET_DATE`
  - `VALIDATION_STATUS`, `FLAG_EXCLUDED`

**CMS Qualifying Rules**:
- Primary diagnosis (position 1) prioritized
- Procedure-based qualifiers (e.g., diabetes requires glucose testing)
- Time-based qualification windows (service dates within coverage period)
- Redundancy exclusions (duplicate ICD codes within visit)

---

### 3. 13-Step HCC Scoring Pipeline

The core risk scoring algorithm processes member diagnoses through a structured pipeline:

```
Step 1: Load Risk Member Data
   ↓ Retrieve members with active coverage and enrollment dates
   ↓
Step 2: Enrich with Diagnosis History
   ↓ Link qualifying diagnoses to members, apply time-based filters
   ↓
Step 3: Apply Disability Edits
   ↓ Identify institutional/community risk populations
   ↓
Step 4: Calculate Age/Sex/SEDITS
   ↓ Apply demographic factors, Sex/Age Edits per model version
   ↓
Step 5: Select Community vs. Institutional Model
   ↓ Route to appropriate CMS HCC model (v24/v28)
   ↓
Step 6: ICD → HCC Mapping
   ↓ Convert ICD-9/ICD-10 to HCC category codes
   ↓
Step 7: Apply Hierarchy Rules
   ↓ Suppress lower-severity codes when parent codes present
   ↓
Step 8: Score Assignment
   ↓ Assign CMS risk coefficient for each HCC
   ↓
Step 9: Apply Interaction Rules
   ↓ Calculate comorbid condition cost multipliers
   ↓
Step 10: Calculate HCC Count Payment
   ↓ Factor disease burden from multiple HCC categories
   ↓
Step 11: Normalize Scores
   ↓ Adjust for regional variations and benchmarks
   ↓
Step 12: Calculate Weighted Score
   ↓ Final risk score = base + demographic + HCC + interactions
   ↓
Step 13: Output Risk Scores
   ↓ Persist member-level and HCC-detail results
```

**Key Algorithm Components**:

#### HCC Hierarchy Rules
- **Parent-Child Relationships**: 100+ hierarchy rules prevent double-counting
- **Example**: HCC 19 (Diabetes with complications) suppresses HCC 18 (Diabetes without complications)
- **Logic**: If parent HCC present, child HCC codes are excluded from scoring

#### SEDITS (Sex/Age Edits)
- **Purpose**: Validate HCC appropriateness by demographic
- **Example**: HCC-1 (HIV) with age < 18 → EXCLUDED (unless flagged as exception)
- **Version-Specific**: Rules vary by model version (v24 vs v28)

#### Interaction Multipliers
- **Purpose**: Capture cost impact of comorbid conditions
- **Example**: Diabetes + Chronic Kidney Disease = 1.15x multiplier
- **Combinations**: 20+ documented interaction rules

---

### 4. Curation Layer

**Purpose**: Deduplicate and optimize data for analytics

**Key Operations**:

#### BID (Business ID) Surrogate Key
- **Problem**: Members may have multiple IDs across sub-plans
- **Solution**: Create unified `BID` across all member identifiers
- **Benefit**: Prevents duplicate risk scoring across sub-plans

#### Partitioning Strategy
```
Partitioned by: SOURCE_LOAD_MONTH, HOME_PLAN_ID_CD, METHOD_ID
Benefits:
- Improves query performance by 70-80%
- Enables incremental processing
- Supports multi-plan scenarios
```

#### Deduplication Rules
- **Member Level**: Keep most recent enrollment record
- **Diagnosis Level**: Keep most recent diagnosis for ICD code + member combination
- **Coverage Gaps**: Handle mid-year terminations and re-enrollments

---

## Data Flow

### Member Risk Score Calculation Flow

```
Health Plan EDI Files (S3)
        ↓
┌──────────────────────────────┐
│  Ingestion Layer             │
│  - Parse EDI formats         │
│  - Validate data integrity   │
│  - Stage in Databricks       │
└──────────────────────────────┘
        ↓
┌──────────────────────────────┐
│  Standardization             │
│  - Apply data quality rules  │
│  - Join enrollment data      │
│  - Create risk_member table  │
└──────────────────────────────┘
        ↓
┌──────────────────────────────┐
│  Diagnosis Qualification     │
│  - Map ICD → HCC             │
│  - Apply procedure rules     │
│  - Validate time windows     │
│  - Create risk_member_diag   │
└──────────────────────────────┘
        ↓
┌──────────────────────────────┐
│  Risk Scoring                │
│  - Apply 13-step pipeline    │
│  - Execute hierarchy rules   │
│  - Calculate interactions    │
│  - Generate final scores     │
└──────────────────────────────┘
        ↓
┌──────────────────────────────┐
│  Output Tables               │
│  - risk_score (member-level) │
│  - risk_member_hcc (detail)  │
│  - Persistence & History     │
└──────────────────────────────┘
        ↓
Downstream Systems
(Gap Suspecting, Financial Planning, CMS Submission)
```

---

## Output Data Model

### Primary Output Tables

#### `risk_score` (Member-Level Aggregation)
```sql
- MEMBER_ID, BID, HOME_PLAN_ID_CD
- RISK_SCORE (Final calculated score)
- DEMOGRAPHIC_SCORE
- HCC_COUNT_PAYMENT
- INTERACTION_SCORE
- RISK_MODEL (v24, v28, RxHCC, ESRD)
- CALCULATION_METHOD
- EFFECTIVE_DATE, LOAD_MONTH
```

#### `risk_member_hcc` (HCC-Level Detail)
```sql
- MEMBER_ID, BID, HOME_PLAN_ID_CD
- HCC_CODE, HCC_DESCRIPTION
- ICD_CODES (list of qualifying ICD codes)
- HCC_RISK_COEFFICIENT
- HIERARCHY_FLAG (1 if suppressed, 0 if included)
- SOURCE_CLAIM_DATE
- CALCULATION_DATE
```

#### `member_persistence_hcc` (3-Year History)
```sql
- MEMBER_ID, BID
- HCC_CODE
- YEAR_1, YEAR_2, YEAR_3 (Presence flags)
- PERSISTENCE_FLAG (Same HCC in multiple years)
- CARRYFORWARD_ELIGIBLE (For chronic conditions)
```

---

## Key Innovations & Best Practices

### 1. BID Surrogate Key Strategy
- **Problem**: Health plans use multiple member IDs (SSN, MRN, health plan ID)
- **Solution**: Create unified Business ID (BID) across all identifiers
- **Impact**: Eliminates duplicate risk scoring, improves accuracy by 15-20%

### 2. Partition-Pruning Optimization
```sql
Partitioned by (SOURCE_LOAD_MONTH, HOME_PLAN_ID_CD, METHOD_ID)
Benefits:
- Query performance: 70-80% faster for common queries
- Incremental processing: Only process new month's data
- Multi-tenant isolation: Each plan completely separated
```

### 3. Hierarchy Rule Automation
- 100+ parent-child relationships encoded as lookup table
- Automatic suppression during scoring step
- Validation: Compare output HCC counts to expected ranges

### 4. SEDITS Validation Framework
- **Version-Specific Rules**: Different rules for CMS HCC v24 vs v28
- **Demographic Validation**: Age/sex appropriateness for each HCC
- **Exception Handling**: Flag medical exceptions (e.g., juvenile diabetes)

---

## Quality Assurance & Validation

### Data Quality Checks
- **Completeness**: No nulls in key scoring columns
- **Uniqueness**: Member-BID relationship is 1:1
- **Referential Integrity**: HCC codes must exist in CMS mapping table
- **Range Validation**: Risk scores between 0.5 and 3.5 (typical range)
- **Consistency**: Member risk score = sum of HCC scores + demographic

### Output Validation
```sql
-- Member-level totals
SELECT COUNT(*), AVG(RISK_SCORE), STDDEV(RISK_SCORE)
FROM risk_score
WHERE LOAD_MONTH = CURRENT_MONTH;

-- HCC distribution validation
SELECT HCC_CODE, COUNT(*), AVG(HCC_RISK_COEFFICIENT)
FROM risk_member_hcc
WHERE HIERARCHY_FLAG = 0;

-- Year-over-year comparison
SELECT HOME_PLAN_ID_CD, COUNT(*), AVG(RISK_SCORE)
FROM risk_score
GROUP BY HOME_PLAN_ID_CD, EXTRACT(YEAR FROM LOAD_DATE)
```

---

## Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Processing Volume** | 400M-500M+ records | Draft records including duplicates |
| **Processing Time** | 30-90 minutes | Full plan per run |
| **Cluster Size** | 6-10 workers | POP-ADVYZER-LARGE |
| **Memory Usage** | 64-128 GB total | Depends on plan size |
| **Output Size** | 1-5 GB | Parquet compressed |
| **Computation Cost** | $50-150/plan/month | AWS Databricks pricing |
| **Accuracy Rate** | 95-98% | Validated against CMS reference |

---

## Known Issues & Resolutions (May-June 2026)

### Issue 1: NULL CHRONIC_HCC for Method 4
- **Symptom**: Some persistent conditions missing chronic flag
- **Root Cause**: Missing JOIN condition for chronic lookup
- **Fix**: Default to 'Y' post-union for method 4 records
- **Status**: RESOLVED (v2.1)

### Issue 2: FREQUENCY Miscalculation
- **Symptom**: Confidence scores incorrect for multi-method gaps
- **Root Cause**: Frequency calculated after deduplication (lost method diversity)
- **Fix**: Calculate frequency on raw data before union/dedup
- **Status**: RESOLVED (v2.2)

### Issue 3: Claim Date Filter Missing
- **Symptom**: Out-of-period claims affecting scores
- **Root Cause**: No service date validation against TIME_PERIOD
- **Fix**: Add WHERE clause filtering by claim.SERVICE_DATE BETWEEN enrollment dates
- **Status**: RESOLVED (v2.3)

### Issue 4: Chronic Condition Flag Updates
- **Symptom**: Inaccurate chronic vs acute classification
- **Change**: 25 HCCs updated (diabetes complications, sepsis, etc.)
- **Impact**: +2-3% accuracy improvement in risk calculation
- **Status**: RESOLVED (v2.4)

---

## Dependencies & Integration Points

### Upstream Dependencies
- **S3 Inbox**: Health plan EDI files (claims, enrollment, pharmacy)
- **Databricks Unity Catalog**: Central data repository
- **AWS IAM**: Authentication & authorization
- **Apache Airflow**: Workflow orchestration

### Downstream Dependencies
- **Gap Suspecting Engine**: Uses risk_score to identify improvement opportunities
- **Persistence Tracking**: Analyzes HCC trends over time
- **CMS Submission**: Population rates for HEDIS/quality measures
- **Financial Forecasting**: Revenue projection based on risk scores
- **Clinical Programs**: Member stratification for outreach

---

## Operational Considerations

### Scheduling
- **Frequency**: Monthly processing aligned with plan EDI cycles
- **Trigger**: When complete EDI files received in S3 inbox
- **SLA**: Results available within 24 hours of data receipt

### Monitoring
- **Alerting**: Pipeline failures, data quality issues, performance degradation
- **Dashboards**: Member count trends, score distribution, HCC frequency
- **Audit Logs**: All score changes tracked with timestamp and operator

### Maintenance
- **CMS Model Updates**: Incorporate v28 and new model versions annually
- **Hierarchy Rule Updates**: Quarterly review of parent-child relationships
- **Performance Tuning**: Ongoing optimization of partition strategy
- **Data Retention**: 3-year rolling history maintained for persistence analysis

---

## Security & Compliance

- **PII Protection**: Member IDs redacted in non-production environments
- **HIPAA Compliance**: Encryption at rest and in transit
- **Audit Trail**: All data access logged with user identification
- **Access Control**: Role-based access (Admin, Analyst, Viewer)
- **Data Governance**: Data lineage tracking from source to output

---

## Conclusion

The Risk Engine is a mission-critical system that accurately calculates member-level risk scores for quality and financial planning. By leveraging CMS HCC models, sophisticated hierarchy rules, and comprehensive data validation, it enables health plans to identify high-risk members and allocate resources effectively. Continuous monitoring, validation, and refinement ensure sustained accuracy and operational excellence.

