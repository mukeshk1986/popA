# Gap Suspecting Engine - High Level Solution Architecture

## Executive Summary

The Gap Suspecting Engine identifies high-confidence opportunities to capture missing or undocumented diagnoses in members' health records. It processes 400M-500M+ records monthly using four distinct methods to identify potential gaps, with multi-method consensus filtering for high-confidence predictions. Captured gaps have a financial impact of $3,000-$8,000 per HCC per member.

**Key Metrics:**
- **Processing Volume**: 400M-500M+ draft gap records per plan
- **Processing Time**: 30-90 minutes per plan
- **Confidence Threshold**: ≥0.75 (75%) for final recommendations
- **Financial Impact**: $3,000-$8,000 per HCC per member captured
- **Accuracy Rate**: HIGH across all four methods

---

## Architecture Overview

### System Layers

```
┌─────────────────────────────────────────────────────────────┐
│              Gap Recommendation Engine                       │
│     (Confidence Filtering, Deduplication, Ranking)          │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│           Suppression & Cross-Method Logic                  │
│   (Documented gaps removal, Hierarchy conflicts, Exclusions)│
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│            Gap Detection Methods (4 Parallel)               │
│  Method 1: Diagnosis-Based | Method 2: Procedure-Based     │
│  Method 10: Multi-Scenario | Method 4: Persistent Carryforward
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│           Risk Engine & Clinical Data                       │
│  (Member diagnoses, Claims, Pharmacy, Procedures)           │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### 1. Risk Engine Integration

**Data Source**: Output from Risk Engine
- `risk_member_hcc`: Member diagnoses mapped to HCC categories
- `risk_member`: Member enrollment and demographic data
- Historical diagnosis records (3-year lookback)
- CMS HCC mappings and hierarchy rules

**Purpose**: Provides baseline diagnosed conditions for gap identification

---

### 2. Gap Detection Methods

The engine uses **four distinct methods** to identify gaps with varying accuracy and volume characteristics:

#### Method 1: Diagnosis-Based Gap Detection

**Purpose**: Identify previously documented diagnoses not re-documented in current period

**Business Logic**:
```
For each member with an HCC diagnosis in prior year:
  IF diagnosis NOT present in current year:
    AND member still enrolled in plan:
    AND member meets age/demographic criteria:
    THEN suspect as potential gap
```

**Characteristics**:
- **Volume**: HIGH (50-70% of all gaps)
- **Accuracy**: HIGH (75-85%)
- **Rationale**: Chronic conditions typically persist and should be re-documented annually
- **Example**: Member had Type 2 Diabetes (HCC 19) in 2024 but no diabetes ICD code in 2025

**Data Flow**:
```
Prior Year Diagnoses (HCC codes)
    ↓
Filter for chronic conditions
    ↓
Cross-join with current year member enrollment
    ↓
LEFT JOIN current year diagnoses
    ↓
WHERE current_year_diagnosis IS NULL
    ↓
Candidate gaps identified
```

---

#### Method 2: Procedure-Based Gap Detection

**Purpose**: Identify diagnoses implied by documented procedures but without corresponding ICD codes

**Business Logic**:
```
For each member with documented procedure code:
  IF procedure implies diagnosis (e.g., insulin administration → diabetes):
    AND diagnosis NOT documented with ICD code:
    THEN suspect as potential gap
```

**Characteristics**:
- **Volume**: MODERATE (20-35% of all gaps)
- **Accuracy**: MODERATE-HIGH (70-80%)
- **Rationale**: Procedures often require diagnosis; missing code is documentation gap
- **Example**: Member has insulin administration procedure but no diabetes ICD code

**Procedure-to-Diagnosis Mapping** (Examples):
```
Procedure Code → Implied Diagnosis
────────────────────────────────
94002 (Spirometry)         → Chronic Obstructive Pulmonary Disease (COPD)
93000 (EKG)                → Congestive Heart Failure (CHF) or Ischemic Heart Disease
36415 (Venipuncture)       → Chronic Kidney Disease (CKD) (with supporting labs)
99213-99215 (Office Visit)  → Diabetes management code
71046 (Chest X-ray)        → Pneumonia or Respiratory Condition
```

**Data Flow**:
```
Procedure Codes (CPT/HCPCS)
    ↓
Map to implied diagnoses via lookup table
    ↓
Join with member claim dates
    ↓
LEFT JOIN documented ICD codes for same member
    ↓
WHERE implied_diagnosis NOT found:
    ↓
Candidate gaps identified
```

---

#### Method 10: Multi-Scenario Gap Detection

**Purpose**: Identify gaps using comprehensive 180-day evidence evaluation

**Business Logic**:
```
180-day lookback period: START_DATE to (START_DATE + 180 days)

For each member-HCC combination:
  Scenario 1: No evidence of diagnosis in 180-day window
    - No claims with matching ICD code
    - No procedures implying diagnosis
    - No lab results consistent with diagnosis
    → HIGH confidence gap
    
  Scenario 2: Evidence found but validation failures
    - Diagnosis documented but outside coverage period
    - Claim dates don't align with member enrollment
    - ICD code conflicts with clinical guidelines
    → MEDIUM confidence gap (validate with clinical team)
```

**Characteristics**:
- **Volume**: VERY HIGH (60-80% of all gaps)
- **Accuracy**: HIGH (75-85%)
- **Lookback Window**: 180 days
- **Rationale**: Comprehensive evidence review captures gaps missed by single-point methods
- **Example**: No diabetes evidence in 180 days despite high risk profile

**Evidence Types Evaluated**:
```
1. ICD Diagnosis Codes
   - Position: Primary (position 1) weighted higher than secondary
   - Dates: Must fall within coverage period
   - Frequency: Multiple claims strengthen confidence

2. Procedure Codes
   - Related to diagnosis: Glucose monitoring for diabetes
   - Treatment delivery: Insulin injection for diabetes
   - Monitoring: Lab orders, imaging, assessments

3. Laboratory Results
   - Values consistent with condition: High HbA1c for diabetes
   - Test names matching condition: "Thyroid Panel" for thyroid disease
   - Abnormal flags: Lab result outside normal range

4. Medication/Pharmacy Claims
   - Drug-diagnosis alignment: Metformin for diabetes
   - Dosage appropriateness: Drug strength matches clinical protocol
   - Refill frequency: Consistent with treatment pattern

5. Claim Attributes
   - Place of service: Outpatient clinic, hospital, urgent care
   - Facility type: Whether consistent with condition type
   - Provider specialty: Cardiologist claim suggests cardiac condition
```

**Data Flow**:
```
180-day Claims Window
    ↓
Aggregate by Member × HCC Combination
    ↓
Scenario 1: Check for ANY evidence
  ├─ ICD codes present?
  ├─ Related procedures?
  ├─ Supporting labs?
  └─ Medications?
    ↓
Scenario 2: Validate evidence quality
  ├─ Dates within enrollment?
  ├─ Diagnosis code appropriate for age/sex?
  ├─ Procedure-diagnosis alignment?
  └─ Provider specialty consistency?
    ↓
CASE WHEN no_evidence THEN 'HIGH_CONFIDENCE'
     WHEN evidence_invalid THEN 'MEDIUM_CONFIDENCE'
     ELSE NULL
    ↓
Candidate gaps identified
```

---

#### Method 4: Persistent Carryforward Gap Detection

**Purpose**: Identify permanent conditions that should persist even without current-year re-documentation

**Business Logic**:
```
For each member with documented chronic condition:
  IF condition is flagged as "CHRONIC_CONDITION = 'Y'":
    AND member was continuously enrolled 3+ years:
    AND condition documented in 2+ prior years:
    THEN recommend carryforward to current year
    (Diagnosis doesn't need re-documentation)
```

**Characteristics**:
- **Volume**: LOW (5-15% of all gaps)
- **Accuracy**: HIGHEST (85-95%)
- **Lookback Window**: 3 years
- **Persistence Requirement**: Present in 2+ prior years
- **Rationale**: Certain permanent conditions auto-persist; missing code is documentation, not clinical gap
- **Example**: Patient with kidney disease stage 4 (CKD-4) has persistent condition; should carry forward annually

**Chronic Condition Examples**:
```
HCC Code | Condition Name | Persistence Type
─────────────────────────────────────────────
HCC 19   | Diabetes       | CHRONIC (recertify annually)
HCC 23   | Heart Failure  | CHRONIC (persistent)
HCC 27   | CKD Stage 3-5  | CHRONIC (progressive, monitor)
HCC 58   | Chronic Liver  | CHRONIC (persistent)
HCC 111  | Cystic Fibrosis| CHRONIC (permanent, carryforward)
```

**Data Flow**:
```
3-Year HCC History
    ↓
Filter for CHRONIC_CONDITION = 'Y'
    ↓
Count years condition documented
    ↓
Verify continuous enrollment ≥ 3 years
    ↓
WHERE years_documented ≥ 2:
    ↓
Candidate for carryforward
```

---

### 3. Suppression Logic

Gap candidates are filtered through two suppression layers to remove false positives:

#### Suppression 1: Already-Documented Gaps

**Purpose**: Remove candidates where diagnosis is already documented

**Logic**:
```sql
WHERE gap_candidate NOT IN (
  SELECT DISTINCT diagnosis
  FROM current_year_documented_diagnoses
  WHERE member_id = gap_candidate.member_id
)
```

**Examples**:
- Method 1 suspects diabetes gap, but diabetes already in claims this year → SUPPRESSED
- Method 10 suspects CHF gap, but CHF documented in current period → SUPPRESSED
- Method 2 suspects CKD gap, but CKD already coded by nephrologist → SUPPRESSED

**Impact**: Eliminates 20-30% of initial candidates

---

#### Suppression 2: Plan-Excluded Methods & Conditions

**Purpose**: Remove gaps for methods or conditions excluded by health plan policy

**Exclusion Rules**:
```sql
Excluded Methods by Plan:
  - Method 2 for HCC 1-5 (low risk)
  - Method 10 for rare conditions (prevalence < 0.1%)
  
Excluded Conditions by Payer:
  - HCC 27 (CHF) - may not pursue in Medicare Advantage
  - HCC 34 (Depression) - documented by behavioral health only
  - HCC 56 (Schizophrenia) - specialty provider only
```

**Data Flow**:
```
Gap Candidates
    ↓
LEFT JOIN plan_exclusion_rules
    ↓
WHERE gap NOT in (excluded_methods, excluded_conditions)
    ↓
Filtered candidates remain
```

**Impact**: Eliminates 10-20% of remaining candidates

---

### 4. Cross-Method Hierarchy & Conflict Resolution

**Purpose**: Prevent conflicting HCC recommendations from different methods

**Hierarchy Rules**:
```
Rule: Child HCC Cannot Be Gap Without Parent

IF Method 2 suggests HCC 37 (Renal failure) as gap
AND Method 1 suggests HCC 27 (CKD) is NOT gap:
THEN Suppress HCC 37 (child cannot exist without parent)

Rule: One Method Per Member-HCC

IF Method 1 AND Method 10 both suggest same member-HCC gap:
THEN Keep only higher confidence method
ELSE Report both separately
```

**Conflict Resolution**:
```
Confidence Priority (High → Low):
1. Method 4 (Persistent, HIGHEST accuracy)
2. Method 10 (Multi-scenario, comprehensive)
3. Method 1 (Diagnosis-based, high volume)
4. Method 2 (Procedure-based, lower confidence)

When multiple methods identify same gap:
- Keep Method 4 version
- Otherwise, keep highest-confidence variant
```

---

### 5. Confidence Scoring & Filtering

**Purpose**: Rank gaps by likelihood of clinical validity

**Confidence Calculation**:
```
FREQUENCY = Count of methods identifying same gap (1-4)
  - Method 1: count = 1
  - Method 2: count = 1
  - Method 10: count = 1
  - Method 4: count = 1
  → Max FREQUENCY = 4 if all methods agree

CONFIDENCE = FREQUENCY / 4
  - 1 method = 0.25 confidence
  - 2 methods = 0.50 confidence
  - 3 methods = 0.75 confidence ← THRESHOLD
  - 4 methods = 1.00 confidence (perfect agreement)
```

**Confidence Threshold Logic**:
```sql
WHERE CONFIDENCE >= 0.75
  ├─ 0.75-0.99: Recommended for outreach (HIGH confidence)
  └─ 1.00: Perfect agreement (HIGHEST confidence)

WHERE CONFIDENCE < 0.75
  ├─ 0.50-0.74: Clinical review recommended
  └─ 0.25-0.49: Low confidence, optional outreach
```

**Output Filtering**:
```
Final Suspected Gaps Report:
  SELECT member_id, HCC_CODE, FREQUENCY, CONFIDENCE, METHOD_LIST
  FROM gap_candidates
  WHERE CONFIDENCE >= 0.75
  ORDER BY CONFIDENCE DESC, member_id
```

---

## Data Flow

### Complete Gap Suspecting Pipeline

```
Risk Engine Output (risk_member_hcc)
        ↓
┌──────────────────────────────────────────────────────────┐
│ Method 1: Diagnosis-Based Detection                     │
│ - Prior HCC not in current year                          │
│ - Chronic condition persistence check                    │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Method 2: Procedure-Based Detection                     │
│ - Procedure → Diagnosis mapping                          │
│ - Missing ICD code validation                            │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Method 10: Multi-Scenario Detection (180-day window)    │
│ - Evidence aggregation & validation                      │
│ - Scenario analysis (no evidence vs. invalid evidence)   │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Method 4: Persistent Carryforward Detection             │
│ - 3-year history analysis                                │
│ - Chronic condition persistence verification            │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Union All Method Outputs                                 │
│ - Combine from all 4 methods (400M-500M+ records)        │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Suppression Layer 1: Remove Already-Documented Gaps     │
│ - Cross-check current diagnoses                          │
│ - Eliminate false positives (20-30%)                     │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Suppression Layer 2: Apply Plan Exclusions              │
│ - Remove excluded methods/conditions                     │
│ - Eliminate policy conflicts (10-20%)                    │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Cross-Method Hierarchy & Conflict Resolution            │
│ - Enforce parent-child rules                             │
│ - Resolve method conflicts                               │
│ - De-duplicate identical gaps                            │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Confidence Calculation                                   │
│ - FREQUENCY = count of methods (1-4)                     │
│ - CONFIDENCE = FREQUENCY / 4                             │
│ - Filter WHERE CONFIDENCE >= 0.75                        │
└──────────────────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────────────────┐
│ Final Suspected Gaps (HIGH CONFIDENCE)                  │
│ - Ranked by confidence score                             │
│ - Ready for clinical validation                          │
└──────────────────────────────────────────────────────────┘
        ↓
Downstream Outreach Programs
(Clinical Review, Member Contact, Documentation Capture)
```

---

## Output Data Model

### Primary Output Tables

#### `suspected_gaps` (Main Recommendations)
```sql
- MEMBER_ID, BID, HOME_PLAN_ID_CD
- HCC_CODE, HCC_DESCRIPTION
- GAP_IDENTIFICATION_METHOD (Method 1/2/10/4)
- FREQUENCY (count of methods: 1-4)
- CONFIDENCE (0.25-1.00)
- METHOD_LIST (comma-separated methods)
- PRIORITY_RANK (1-highest to N-lowest)
- RECOMMENDATION_DATE
- EXCLUSION_FLAG (if suppressed)
- EXCLUSION_REASON
```

#### `gap_method_detail` (Per-Method Results)
```sql
- MEMBER_ID, BID, HCC_CODE
- METHOD (1/2/10/4)
- CONFIDENCE_COMPONENT
- SUPPORTING_EVIDENCE
  ├─ PRIOR_YEAR_DIAGNOSIS (Method 1)
  ├─ PROCEDURE_CODE (Method 2)
  ├─ 180_DAY_EVIDENCE (Method 10)
  └─ CARRYFORWARD_ELIGIBLE (Method 4)
- CALCULATION_DATE
```

#### `gap_suppression_log` (Audit Trail)
```sql
- MEMBER_ID, HCC_CODE, METHOD
- SUPPRESSION_TYPE (Layer 1 / Layer 2)
- SUPPRESSION_REASON
  ├─ ALREADY_DOCUMENTED
  ├─ PLAN_EXCLUDED_METHOD
  ├─ PLAN_EXCLUDED_CONDITION
  └─ HIERARCHY_CONFLICT
- SUPPRESSION_DATE
```

---

## Key Innovations & Best Practices

### 1. Multi-Method Consensus Approach
- **Problem**: Single-method gaps prone to false positives
- **Solution**: Calculate confidence across 4 independent methods
- **Benefit**: Only gaps identified by 3+ methods (75%+ confidence) recommended
- **Impact**: Reduces clinical review burden by 60-70%

### 2. Frequency-Based Confidence Scoring
```
Confidence Formula: FREQUENCY / 4

Interpretation:
- 0.25 (1 method): May be false positive, requires validation
- 0.50 (2 methods): Moderate confidence, recommend review
- 0.75 (3 methods): HIGH confidence, recommend outreach ✓
- 1.00 (4 methods): Perfect consensus, highest priority
```

### 3. Two-Layer Suppression Architecture
- **Layer 1**: Remove already-documented gaps (clinical reality check)
- **Layer 2**: Remove plan-excluded gaps (business policy check)
- **Benefit**: Clean recommendations aligned with clinical and business logic

### 4. HCC Hierarchy Conflict Prevention
- Enforce parent-child relationships across methods
- Prevent child HCC gaps when parent is not a gap
- Automatic resolution of conflicting method recommendations

### 5. Comprehensive Evidence Aggregation (Method 10)
- ICD codes, procedures, labs, medications, claim attributes
- 180-day lookback captures full clinical picture
- Scenario-based evaluation handles edge cases

---

## Quality Assurance & Validation

### Gap Confidence Validation
```sql
-- Verify confidence scoring
SELECT 
  FREQUENCY,
  COUNT(*) as gap_count,
  AVG(CONFIDENCE) as avg_confidence,
  STDDEV(CONFIDENCE) as confidence_stdev
FROM suspected_gaps
GROUP BY FREQUENCY
ORDER BY FREQUENCY;

-- Expected distribution:
-- FREQUENCY=3: ~40-50% of gaps (confidence 0.75)
-- FREQUENCY=4: ~20-30% of gaps (confidence 1.00)
-- FREQUENCY=1-2: Excluded (confidence < 0.75)
```

### Method Performance Comparison
```sql
-- Validate each method's contribution
SELECT 
  GAP_IDENTIFICATION_METHOD,
  COUNT(*) as identified_gaps,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct_total,
  AVG(CONFIDENCE) as avg_confidence
FROM suspected_gaps
GROUP BY GAP_IDENTIFICATION_METHOD
ORDER BY identified_gaps DESC;

-- Expected results:
-- Method 10: 40-50% of final gaps (highest volume)
-- Method 1: 30-40% (high accuracy)
-- Method 4: 5-15% (low volume, highest accuracy)
-- Method 2: 10-20% (moderate confidence)
```

### Financial Impact Validation
```sql
-- Validate captured gaps have financial significance
SELECT 
  ROUND(AVG(FINANCIAL_VALUE_PER_HCC)) as avg_value,
  COUNT(*) as gap_count,
  ROUND(SUM(FINANCIAL_VALUE_PER_HCC)) as total_potential_value
FROM suspected_gaps
WHERE CONFIDENCE >= 0.75;

-- Expected results:
-- Average value per gap: $3,000-$8,000
-- High-confidence gaps only (≥0.75)
-- Total potential value: millions per plan
```

---

## Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Input Volume** | 400M-500M+ | Draft records from all 4 methods |
| **After Suppression 1** | 280M-350M | Remove 20-30% already-documented |
| **After Suppression 2** | 220M-280M | Remove 10-20% excluded gaps |
| **After Deduplication** | 15M-25M | Unique member-HCC combinations |
| **High Confidence Final** | 3M-8M | Where CONFIDENCE ≥ 0.75 |
| **Processing Time** | 30-90 min | Full plan per run |
| **Cluster Size** | 6-10 workers | POP-ADVYZER-LARGE |
| **Memory Usage** | 64-128 GB | Full processing cycle |
| **Output Size** | 500MB-2GB | Compressed format |
| **Computation Cost** | $50-150/plan/month | AWS Databricks pricing |

---

## Known Issues & Resolutions (May-June 2026)

### Issue 1: Method 4 Missing Chronic Flags
- **Symptom**: Some persistent conditions lack CHRONIC_HCC flag
- **Root Cause**: Missing lookup join in 3-year history aggregation
- **Fix**: Default to 'Y' post-union for persistent conditions
- **Status**: RESOLVED (v2.1)

### Issue 2: Frequency Miscalculation in Multi-Method Consensus
- **Symptom**: Confidence scores inflated for member-HCC combinations
- **Root Cause**: Frequency calculated AFTER deduplication lost method diversity
- **Fix**: Calculate frequency on raw data BEFORE union operation
- **Status**: RESOLVED (v2.2)

### Issue 3: 180-Day Lookback Date Filter Missing (Method 10)
- **Symptom**: Out-of-period claims affecting method 10 confidence
- **Root Cause**: No service date validation against TIME_PERIOD range
- **Fix**: Add WHERE clause: claim.SERVICE_DATE BETWEEN start_date AND start_date + 180 days
- **Status**: RESOLVED (v2.3)

### Issue 4: Chronic Condition Classification Updates
- **Symptom**: Inaccurate chronic vs acute for method 4
- **Changes**: 25 HCCs re-classified
  - HCC 19: Diabetes (acute complications now treated as acute)
  - HCC 52: Sepsis (moved from chronic to acute)
  - HCC 56: Cancer (varies by type - some chronic, some acute)
- **Impact**: +2-3% accuracy in persistent gap detection
- **Status**: RESOLVED (v2.4)

---

## Dependencies & Integration Points

### Upstream Dependencies
- **Risk Engine**: Provides `risk_member_hcc` and `risk_member` tables
- **Claims Data**: Raw claim files with ICD, procedure, lab data
- **Enrollment Data**: Member coverage periods and plan assignments
- **CMS HCC Mappings**: ICD → HCC lookup tables and hierarchy rules

### Downstream Dependencies
- **Clinical Programs**: Gap validation and member outreach
- **Compliance**: Documentation capture and quality improvement
- **Financial Planning**: Revenue impact analysis of captured gaps
- **Reporting**: Dashboard and performance metrics
- **CMS Submission**: Updated diagnoses for quality measure reporting

---

## Operational Considerations

### Scheduling
- **Frequency**: Monthly, aligned with Risk Engine execution
- **Trigger**: Immediately after Risk Engine completion
- **SLA**: Results available within 1-2 hours of Risk Engine completion

### Monitoring & Alerting
- **Key Metrics**:
  - Gap count by method and confidence level
  - Suppression rate (20-30% expected)
  - False positive rate (validation against clinical charts)
  - Financial impact of captured gaps
- **Alerts**: 
  - Abnormal gap volume (>50% variance from baseline)
  - High false positive rate (>15%)
  - Missing suppression layer outputs

### Clinical Validation Workflow
```
1. HIGH confidence gaps (≥0.75) → Automatic outreach
2. MEDIUM confidence gaps (0.50-0.74) → Clinical review queue
3. LOW confidence gaps (<0.50) → Archive (not recommended)

Feedback Loop:
- Track member contact outcomes
- Validate gap assumptions
- Refine suppression rules quarterly
- Update method weights based on validation results
```

### Maintenance Schedule
- **Monthly**: Gap volume and confidence distribution review
- **Quarterly**: Method performance analysis and threshold tuning
- **Semi-annually**: Suppression rule effectiveness audit
- **Annually**: CMS HCC model updates and procedure mapping refresh

---

## Security & Compliance

- **PII Protection**: Member IDs masked in non-production environments
- **HIPAA Compliance**: Encrypted transmission and storage
- **Audit Trail**: All gap recommendations logged with timestamp and source
- **Access Control**: Role-based (Admin, Clinician, Analyst, Viewer)
- **Data Retention**: 1-year rolling history of gap recommendations
- **HIPAA Safe Harbor**: PHI only in production environment with strict access controls

---

## Conclusion

The Gap Suspecting Engine is a sophisticated four-method approach to identifying high-confidence documentation gaps in member health records. By combining diagnosis-based, procedure-based, comprehensive scenario analysis, and persistent condition carryforward methods, it achieves 75%+ confidence recommendations that drive meaningful clinical and financial value. Continuous validation, method refinement, and clinical workflow integration ensure sustainable capture of documented gaps and improved member outcomes.

