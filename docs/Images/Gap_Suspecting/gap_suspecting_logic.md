# Gap Suspecting - High Level Workflow & Data Flow Diagram

## Document Information
- **Created:** 2026-06-03
- **Author:** Mukesh Kumar
- **Version:** 1.0

---

## Table of Contents
1. [Overview](#overview)
2. [Method Workflows](#method-workflows)
3. [Post-Processing](#post-processing)
4. [Data Flow Diagram with Filters & Checks](#data-flow-diagram-with-filters--checks)
5. [Summary Tables](#summary-tables)

---

## Overview

The Gap Suspecting pipeline identifies potential healthcare coding gaps (HCCs that should be captured but aren't) using 4 different methods. All methods ultimately feed into `suspected_gaps` table and then to `risk_member_hcc` for risk scoring.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MAIN ORCHESTRATION FLOW                              │
│  call_gp_suspecting_main()                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Loop through requested methods (1, 2, 10, 4)                            │
│  2. Union all method outputs                                                │
│  3. Apply Cross-Method Hierarchy Suppression                                │
│  4. Insert to risk_member_hcc (final output)                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Method Workflows

### Method 1: Pharmacy-Based Gap Suspecting

**Purpose:** Identify gaps based on pharmacy/drug claims that suggest a diagnosis

```
┌──────────────────────────────────────────────────────────────────┐
│  METHOD 1 - PHARMACY CLAIMS                                      │
├──────────────────────────────────────────────────────────────────┤
│  INPUT:                                                          │
│    • Pharmacy claims (NDC codes)                                 │
│    • ref_method_metadata (METHOD_ID=1)                           │
│                                                                  │
│  LOGIC:                                                          │
│    1. Get pharmacy claims for risk year                          │
│    2. Match NDC codes against ref_method_metadata                │
│    3. Join with CC code mapping to get HCC codes                 │
│    4. Create draft gaps                                          │
│                                                                  │
│  OUTPUT: Draft gaps where drug indicates potential diagnosis     │
│                                                                  │
│  EXAMPLE:                                                        │
│    Member taking insulin (NDC) → Suspect Diabetes HCC            │
└──────────────────────────────────────────────────────────────────┘
```

---

### Method 2: Diagnosis-Based Gap Suspecting

**Purpose:** Identify gaps based on diagnosis codes in medical claims

```
┌──────────────────────────────────────────────────────────────────┐
│  METHOD 2 - DIAGNOSIS CODES                                      │
├──────────────────────────────────────────────────────────────────┤
│  INPUT:                                                          │
│    • Medical claims (Professional + Facility)                    │
│    • ref_method_metadata (METHOD_ID=2)                           │
│                                                                  │
│  LOGIC:                                                          │
│    1. Get all medical claims for risk year                       │
│    2. Match diagnosis codes against ref_method_metadata          │
│    3. Join with CC code mapping to get HCC codes                 │
│    4. Create draft gaps                                          │
│                                                                  │
│  OUTPUT: Draft gaps where diagnosis suggests related HCC         │
│                                                                  │
│  EXAMPLE:                                                        │
│    Member with ICD code for complication → Suspect parent HCC    │
└──────────────────────────────────────────────────────────────────┘
```

---

### Method 10: CPT/HCPCS Procedure-Based Gap Suspecting

**Purpose:** Identify gaps based on procedures (CPT/HCPCS codes) with exclusion logic

```
┌──────────────────────────────────────────────────────────────────┐
│  METHOD 10 - CPT/HCPCS PROCEDURES + EXCLUSIONS                   │
├──────────────────────────────────────────────────────────────────┤
│  INPUT:                                                          │
│    • Medical claims (Professional + Facility)                    │
│    • ref_method_metadata (METHOD_ID=10)                          │
│    • ref_method_metadata_codegroups (for exclusion expansion)    │
│                                                                  │
│  LOGIC - 3 PARALLEL PATHS:                                       │
│                                                                  │
│  A) INCLUSION PATH:                                              │
│     1. Match CPT/HCPCS codes against method metadata             │
│     2. Create draft gaps for matching procedures                 │
│                                                                  │
│  B) SCENARIO 1 (With Modifier Requirements):                     │
│     1. Find claims with trigger CPT code                         │
│     2. Check for required modifier in same claim                 │
│     3. Apply 180-day exclusion lookback                          │
│     4. Gap = Has modifier BUT no exclusion diagnosis             │
│                                                                  │
│  C) SCENARIO 2 (Without Modifier Requirements):                  │
│     1. Find claims with trigger CPT code                         │
│     2. Apply 180-day exclusion lookback                          │
│     3. Gap = Trigger code BUT no exclusion diagnosis             │
│                                                                  │
│  OUTPUT: Union of Inclusion + Scenario1 + Scenario2              │
│                                                                  │
│  EXAMPLE:                                                        │
│    CPT 99292 (Critical Care) + No cancer diagnosis in 180 days   │
│    → Suspect Sepsis/Shock HCC                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

### Method 4: Persistent HCC-Based Gap Suspecting

**Purpose:** Identify gaps based on chronic conditions that should recur annually

```
┌──────────────────────────────────────────────────────────────────┐
│  METHOD 4 - PERSISTENT/CHRONIC HCCs                              │
├──────────────────────────────────────────────────────────────────┤
│  INPUT:                                                          │
│    • member_persistent_cc table (pre-computed by CMS pipeline)   │
│    • Contains historical chronic conditions per member           │
│                                                                  │
│  LOGIC:                                                          │
│    1. Filter member_persistent_cc for current cycle              │
│    2. Filter for suspect statuses:                               │
│       - "Open Suspect" (no current year claim, expect one)       │
│       - "Closed Suspect" (has current year claim, confirmed)     │
│       - "Open Non Suspect" (suppressed by parent HCC)            │
│    3. Get latest row per (MEMBER_BID, CC_CODE, STATUS)           │
│    4. Map to suspected_gaps schema                               │
│    5. Set CREATE_GAP = 'Y' only for "Open Suspect"               │
│                                                                  │
│  KEY FIELDS:                                                     │
│    • CHRONIC_HCC = 'Y' (all Method 4 records are chronic)        │
│    • CONFIDENCE_FACTOR from persistence scoring                  │
│    • FREQUENCY from historical occurrence count                  │
│                                                                  │
│  OUTPUT: Suspected gaps for chronic conditions                   │
│                                                                  │
│  EXAMPLE:                                                        │
│    Member had Diabetes HCC in 2023, 2024 → Suspect for 2025      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Post-Processing

### Common Post-Processing After All Methods

```
┌──────────────────────────────────────────────────────────────────┐
│  COMMON POST-PROCESSING AFTER ALL METHODS                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. SUPPRESSION LOGIC 1 (Hierarchy within method):               │
│     - Apply HCC hierarchy (child suppressed by parent)           │
│     - e.g., CC019 suppressed by CC017 (both cancer HCCs)         │
│     - Sets SUPPRESSION1_YN = 'Y' for suppressed records          │
│                                                                  │
│  2. SUPPRESSION LOGIC 2 (Against member's existing HCCs):        │
│     - Check if member already has parent HCC from claims         │
│     - Sets SUPPRESSION2_YN = 'Y' if parent exists                │
│                                                                  │
│  3. GAP STATUS DETERMINATION:                                    │
│     - "Open Suspect" → CREATE_GAP = 'Y' (actionable gap)         │
│     - "Closed Suspect" → CREATE_GAP = 'N' (confirmed by claim)   │
│     - "Open Non Suspect" → CREATE_GAP = 'N' (suppressed)         │
│                                                                  │
│  4. SAVE TO suspected_gaps TABLE                                 │
│     - Includes frequency and confidence factor                   │
│                                                                  │
│  5. UNION ALL METHODS                                            │
│                                                                  │
│  6. CROSS-METHOD HIERARCHY SUPPRESSION:                          │
│     - Check if same HCC found by multiple methods                │
│     - Apply hierarchy across methods                             │
│     - Suppressed records → risk_member_hcc_suppressed (audit)    │
│     - Active records → risk_member_hcc                           │
│                                                                  │
│  7. INSERT TO risk_member_hcc:                                   │
│     - Filter by COMBINED_CONFIDENCE_FACTOR >= threshold          │
│     - OR GAP_STATUS = 'Open Non Suspect'                         │
│     - Join with risk_member_output for member attributes         │
│     - Calculate coefficient scores                               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Diagram with Filters & Checks

### Stage 1: Input Data

```
══════════════════════════════════════════════════════════════════════════════
                              STAGE 1: INPUT DATA
══════════════════════════════════════════════════════════════════════════════

    ┌───────────────┐   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
    │   Pharmacy    │   │  Professional │   │   Facility    │   │    Member     │
    │    Claims     │   │    Claims     │   │    Claims     │   │ Persistent CC │
    └───────┬───────┘   └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
            │                   │                   │                   │
    ┌───────▼───────┐   ┌───────▼───────────────────▼───────┐   ┌───────▼───────┐
    │   FILTERS:    │   │          FILTERS:                 │   │   FILTERS:    │
    │ • RISK_YEAR   │   │ • RISK_YEAR range                 │   │ • CYCLE_RUN   │
    │   range       │   │ • QUALIFY_CLAIMS = 'Y'            │   │ • RISK_YEAR   │
    │               │   │ • CLM_PMT_STS_CD = 'P' (Paid)     │   │   range       │
    │               │   │                                   │   │ • SUSPECT_    │
    │               │   │                                   │   │   STATUS IN   │
    │               │   │                                   │   │   (Open/Closed│
    │               │   │                                   │   │    Suspect,   │
    │               │   │                                   │   │   Open Non    │
    │               │   │                                   │   │    Suspect)   │
    └───────────────┘   └───────────────────────────────────┘   └───────────────┘

REFERENCE TABLES:
  • ref_method_metadata
  • ref_method_metadata_codegroups
  • ref_hierarchy
  • ref_cc_code_mapping
  • ref_method_frequency
```

---

### Stage 2: Method-Specific Processing

#### Method 1: Pharmacy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  METHOD 1: PHARMACY                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐     ┌────────────────┐     ┌────────────────┐           │
│  │ Pharmacy Claims│────▶│ JOIN with      │────▶│ Draft Gaps M1  │           │
│  │                │     │ ref_method_    │     │                │           │
│  │                │     │ metadata +     │     │                │           │
│  │                │     │ cc_code_mapping│     │                │           │
│  └────────────────┘     └────────────────┘     └────────────────┘           │
│                                                                             │
│  FILTERS & CHECKS:                                                          │
│  ✓ METHOD_ID = 1                                                            │
│  ✓ CLAIM_CD_TYPE = 'NDC' (National Drug Code)                               │
│  ✓ NDC code matches ref_method_metadata.CLAIM_CD                            │
│  ✓ CC_ID mapped from ref_cc_code_mapping                                    │
│  ✓ GAP_STATUS = 'Create Gap' for valid matches                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Method 2: Diagnosis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  METHOD 2: DIAGNOSIS                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐     ┌────────────────┐     ┌────────────────┐           │
│  │ Medical Claims │────▶│ JOIN with      │────▶│ Draft Gaps M2  │           │
│  │ (Prof+Facility)│     │ ref_method_    │     │                │           │
│  │                │     │ metadata +     │     │                │           │
│  │                │     │ cc_code_mapping│     │                │           │
│  └────────────────┘     └────────────────┘     └────────────────┘           │
│                                                                             │
│  FILTERS & CHECKS:                                                          │
│  ✓ METHOD_ID = 2                                                            │
│  ✓ CLAIM_CD_TYPE = 'DIAGNOSIS'                                              │
│  ✓ DIAG_CD matches ref_method_metadata.CLAIM_CD                             │
│  ✓ CC_ID mapped from ref_cc_code_mapping                                    │
│  ✓ GAP_STATUS = 'Create Gap' for valid matches                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Method 10: CPT/HCPCS with Exclusions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  METHOD 10: CPT/HCPCS WITH EXCLUSIONS                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  THREE PARALLEL PATHS:                                                      │
│                                                                             │
│  PATH A: INCLUSION         PATH B: SCENARIO 1      PATH C: SCENARIO 2       │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      │
│  │ CPT/HCPCS Match │      │ CPT + Modifier  │      │ CPT (No Modifier│      │
│  │ (Simple Match)  │      │ + 180-day Excl  │      │ Req) + 180-day  │      │
│  │                 │      │ Lookback        │      │ Excl Lookback   │      │
│  └────────┬────────┘      └────────┬────────┘      └────────┬────────┘      │
│           │                        │                        │               │
│           └────────────────────────┴────────────────────────┘               │
│                                    │                                        │
│                                    ▼                                        │
│                           ┌─────────────────┐                               │
│                           │ UNION ALL PATHS │                               │
│                           │ Draft Gaps M10  │                               │
│                           └─────────────────┘                               │
│                                                                             │
│  FILTERS & CHECKS:                                                          │
│  ✓ METHOD_ID = 10                                                           │
│  ✓ CLAIM_CD_TYPE IN ('CPT-HCPCS', 'ICD-PROC')                               │
│  ✓ CPT/HCPCS code matches ref_method_metadata.CLAIM_CD                      │
│                                                                             │
│  SCENARIO 1 (CLAIM_CD_MODIFIER_TYPE != 'NA'):                               │
│    ✓ Same-claim modifier validation (CLAIM_CD_MODIFIER group in same claim) │
│    ✓ 180-day lookback: NO exclusion diagnosis (EXCLUSION_1, EXCLUSION_2)    │
│    ✓ DATEDIFF(claim_date, historical_claim_date) BETWEEN 0 AND 180          │
│                                                                             │
│  SCENARIO 2 (CLAIM_CD_MODIFIER_TYPE = 'NA'):                                │
│    ✓ No modifier required                                                   │
│    ✓ 180-day lookback: NO exclusion diagnosis (EXCLUSION_1, EXCLUSION_2)    │
│                                                                             │
│  ✓ CHRONIC_HCC derived from CHRONIC column (1 → 'Y', else → 'N')            │
│  ✓ GAP_STATUS = 'Create Gap' for records passing exclusion checks           │
│  ✓ GAP_STATUS = 'Exclusion' for records failing exclusion checks            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Method 4: Persistent HCC

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  METHOD 4: PERSISTENT HCC                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐     ┌────────────────┐     ┌────────────────┐           │
│  │member_persistent│────▶│ Filter by      │────▶│ Map to         │           │
│  │_cc             │     │ Cycle + Status │     │ suspected_gaps │           │
│  │                │     │ + Latest Row   │     │ Schema         │           │
│  └────────────────┘     └────────────────┘     └────────────────┘           │
│                                                                             │
│  FILTERS & CHECKS:                                                          │
│  ✓ METHOD_ID = 4 (set as constant)                                          │
│  ✓ CYCLE_RUN = current source_load_year_month                               │
│  ✓ RISK_YEAR BETWEEN (risk_year - no_of_year + 1) AND risk_year             │
│  ✓ SUSPECT_STATUS IN ('Open Suspect', 'Closed Suspect', 'Open Non Suspect') │
│                                                                             │
│  WINDOW FUNCTION (Latest Row):                                              │
│    ✓ PARTITION BY: RISK_YEAR, SAS_MODEL_VERSION, MEMBER_BID, CC_CODE,       │
│                    SUSPECT_STATUS, CYCLE_RUN                                │
│    ✓ ORDER BY: CREATED_DATE DESC                                            │
│    ✓ ROW_NUMBER() = 1                                                       │
│                                                                             │
│  MAPPINGS:                                                                  │
│    ✓ CLAIM_CD_TYPE = 'DIAGNOSIS' (constant)                                 │
│    ✓ CLAIM_CD = 'CLAIM_CODE' (constant)                                     │
│    ✓ CHRONIC_HCC = 'Y' (all Method 4 are chronic by definition)             │
│    ✓ SUPPRESSION1_YN: 'Y' if Open Non Suspect, 'N' otherwise                │
│    ✓ CREATE_GAP: 'Y' if Open Suspect, 'N' otherwise                         │
│    ✓ CONFIDENCE_FACTOR from source (persistence scoring)                    │
│    ✓ FREQUENCY from source (historical occurrence count)                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Stage 3: Draft Gaps Consolidation

```
══════════════════════════════════════════════════════════════════════════════
                      STAGE 3: DRAFT GAPS CONSOLIDATION
══════════════════════════════════════════════════════════════════════════════

                ┌─────────────────────────────────────────────┐
                │                draft_gaps                   │
                │              (gap_curation)                 │
                ├─────────────────────────────────────────────┤
                │  FILTER APPLIED:                            │
                │  ✓ GAP_STATUS = 'Create Gap' (valid gaps)   │
                │  ✓ .distinct() to remove duplicates         │
                │                                             │
                │  CHECK:                                     │
                │  ✓ MODEL_VERSION extraction: v24 or v28     │
                │  ✓ Filter out MODEL_VERSION = 'v' (invalid) │
                └─────────────────────────────────────────────┘
```

---

### Stage 4: Suppression Processing

```
══════════════════════════════════════════════════════════════════════════════
                      STAGE 4: SUPPRESSION PROCESSING
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│  SUPPRESSION 1: HCC Hierarchy (Within Method)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐              ┌────────────────┐              ┌─────────┐│
│  │  Draft Gaps    │─────────────▶│ JOIN with      │─────────────▶│ Gaps w/ ││
│  │                │              │ ref_hierarchy  │              │ SUPP1_YN││
│  └────────────────┘              └────────────────┘              └─────────┘│
│                                                                             │
│  LOGIC & CHECKS:                                                            │
│  ✓ Join gap CC_CODE with ref_hierarchy.CHILD_CC                             │
│  ✓ Check if PARENT_CC exists in same member's gap set                       │
│  ✓ If parent exists → SUPPRESSION1_YN = 'Y', GAP_STATUS = 'Open Non Suspect'│
│  ✓ If no parent → SUPPRESSION1_YN = 'N'                                     │
│                                                                             │
│  EXAMPLE:                                                                   │
│    CC019 (Other Cancer) is CHILD of CC017 (Metastatic Cancer)               │
│    If member has both CC017 and CC019 gaps → CC019 suppressed               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SUPPRESSION 2: Against Member's Existing HCCs (From Claims)                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐     ┌────────────────┐     ┌────────────────┐           │
│  │ Gaps with      │────▶│ JOIN with      │────▶│ Gaps with      │           │
│  │ SUPPRESSION1_YN│     │ member_cc +    │     │ SUPPRESSION2_YN│           │
│  │                │     │ ref_hierarchy  │     │                │           │
│  └────────────────┘     └────────────────┘     └────────────────┘           │
│                                                                             │
│  LOGIC & CHECKS:                                                            │
│  ✓ Get member's existing HCCs from risk_member_output (DX_SOURCE = 'claim') │
│  ✓ Join gap CC_CODE with ref_hierarchy.CHILD_CC                             │
│  ✓ Check if PARENT_CC exists in member's existing HCCs                      │
│  ✓ If parent exists in claims → SUPPRESSION2_YN = 'Y'                       │
│  ✓ If no parent in claims → SUPPRESSION2_YN = 'N'                           │
│                                                                             │
│  EXAMPLE:                                                                   │
│    Member has CC017 from actual claim                                       │
│    Gap suspected for CC019 → Suppressed by existing CC017                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                ┌─────────────────────────────────────────────┐
                │          draft_gaps_with_details            │
                │              (gap_curation)                 │
                └─────────────────────────────────────────────┘
```

---

### Stage 5: Gap Status Determination

```
══════════════════════════════════════════════════════════════════════════════
                      STAGE 5: GAP STATUS DETERMINATION
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│  CREATE_GAP STATUS LOGIC                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐     │
│  │   OPEN SUSPECT     │  │  CLOSED SUSPECT    │  │ OPEN NON SUSPECT   │     │
│  ├────────────────────┤  ├────────────────────┤  ├────────────────────┤     │
│  │ SUPPRESSION1_YN='N'│  │ MATCHING_YN = 'Y'  │  │ SUPPRESSION1_YN='Y'│     │
│  │ SUPPRESSION2_YN='N'│  │ (Confirmed by      │  │ (Suppressed by     │     │
│  │ MATCHING_YN = 'N'  │  │  current year clm) │  │  parent HCC)       │     │
│  │ (No current yr clm)│  │                    │  │                    │     │
│  ├────────────────────┤  ├────────────────────┤  ├────────────────────┤     │
│  │ CREATE_GAP = 'Y'   │  │ CREATE_GAP = 'N'   │  │ CREATE_GAP = 'N'   │     │
│  │ (ACTIONABLE)       │  │ (CONFIRMED)        │  │ (SUPPRESSED)       │     │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘     │
│                                                                             │
│  CHECKS:                                                                    │
│  ✓ SUPPRESSION1_YN='N' AND SUPPRESSION2_YN='N' AND MATCHING_YN='N'          │
│    → Open Suspect                                                           │
│  ✓ MATCHING_YN = 'Y' → Closed Suspect (has confirming claim in risk year)   │
│  ✓ SUPPRESSION1_YN='Y' OR SUPPRESSION2_YN='Y' → Open Non Suspect            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Stage 6: Suspected Gaps Table

```
══════════════════════════════════════════════════════════════════════════════
                      STAGE 6: SUSPECTED GAPS TABLE
══════════════════════════════════════════════════════════════════════════════

                ┌─────────────────────────────────────────────┐
                │              suspected_gaps                 │
                │              (gap_curation)                 │
                ├─────────────────────────────────────────────┤
                │                                             │
                │  FREQUENCY CALCULATION:                     │
                │  ✓ Count of (MEMBER_BID, CC_CODE)           │
                │    occurrences across methods               │
                │  ✓ Higher frequency = more evidence         │
                │                                             │
                │  CONFIDENCE_FACTOR:                         │
                │  ✓ From ref_method_frequency or             │
                │    persistence scoring                      │
                │  ✓ PERCENT_WEIGHT from method metadata      │
                │                                             │
                │  CC_CODE NORMALIZATION:                     │
                │  ✓ Format: 'CC' + LPAD(number, 3, '0')      │
                │  ✓ Example: CC001, CC017, CC329             │
                │                                             │
                └─────────────────────────────────────────────┘
```

---

### Stage 7: Cross-Method Union & Suppression

```
══════════════════════════════════════════════════════════════════════════════
                   STAGE 7: CROSS-METHOD UNION & SUPPRESSION
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│  UNION ALL METHODS                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐                      │
│  │Method 1 │ + │Method 2 │ + │Method 10│ + │Method 4 │                      │
│  └─────────┘   └─────────┘   └─────────┘   └─────────┘                      │
│                          │                                                  │
│                          ▼                                                  │
│              UNION BY NAME (allowMissingColumns=True)                       │
│                                                                             │
│  CHECK:                                                                     │
│  ✓ All DataFrames aligned by column name                                    │
│  ✓ Missing columns filled with NULL                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CROSS-METHOD HIERARCHY SUPPRESSION                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 1: SEPARATE BY GAP_STATUS                                             │
│  ✓ df_open_non_suspect = GAP_STATUS = 'Open Non Suspect'                    │
│    (BYPASS cross-method suppression)                                        │
│  ✓ df_open_suspect = GAP_STATUS != 'Open Non Suspect'                       │
│    (APPLY cross-method suppression)                                         │
│                                                                             │
│  STEP 2: APPLY CROSS-METHOD SUPPRESSION (Only on Open Suspect)              │
│  ✓ Join with ref_hierarchy on CC_CODE = CHILD_CC                            │
│  ✓ Check if PARENT_CC exists for SAME member from ANY method                │
│  ✓ If parent exists → CROSS_SUPPRESSION_YN = 'Y'                            │
│  ✓ If no parent → CROSS_SUPPRESSION_YN = 'N'                                │
│                                                                             │
│  EXAMPLE:                                                                   │
│    Method 4 finds CC017 (parent), Method 10 finds CC019 (child)             │
│    → CC019 from Method 10 is cross-suppressed by CC017 from Method 4        │
│                                                                             │
│  STEP 3: SPLIT RESULTS                                                      │
│  ┌─────────────────────────────┐     ┌─────────────────────────────┐        │
│  │     ACTIVE RECORDS          │     │    SUPPRESSED RECORDS       │        │
│  │ CROSS_SUPPRESSION_YN = 'N'  │     │ CROSS_SUPPRESSION_YN = 'Y'  │        │
│  │ + Open Non Suspect (bypass) │     │                             │        │
│  │                             │     │                             │        │
│  │ → Goes to risk_member_hcc   │     │ → Goes to risk_member_hcc_  │        │
│  │                             │     │   suppressed (AUDIT)        │        │
│  └─────────────────────────────┘     └─────────────────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Stage 8: Confidence Threshold & Enrichment

```
══════════════════════════════════════════════════════════════════════════════
                  STAGE 8: CONFIDENCE THRESHOLD & ENRICHMENT
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│  CONFIDENCE FACTOR CALCULATION                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 1: METHOD_CONFIDENCE_FACTOR (per method)                              │
│  ✓ GROUP BY: RISK_YEAR, MODEL_VERSION, MEMBER_BID, CC_CODE, METHOD_ID       │
│  ✓ METHOD_CONFIDENCE_FACTOR = MAX(CONFIDENCE_FACTOR) per group              │
│                                                                             │
│  STEP 2: COMBINED_CONFIDENCE_FACTOR (across methods)                        │
│  ✓ GROUP BY: RISK_YEAR, MODEL_VERSION, MEMBER_BID, CC_CODE                  │
│  ✓ COMBINED = 1 - PRODUCT(1 - METHOD_CONFIDENCE_FACTOR)                     │
│                                                                             │
│  EXAMPLE:                                                                   │
│    Method 4 confidence = 0.75, Method 10 confidence = 0.55                  │
│    Combined = 1 - (1-0.75) * (1-0.55) = 1 - 0.25 * 0.45 = 0.8875            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  THRESHOLD FILTER                                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  FILTER CONDITION:                                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                                                                       │  │
│  │   (COMBINED_CONFIDENCE_FACTOR >= threshold_percent)                   │  │
│  │                        OR                                             │  │
│  │   (GAP_STATUS = 'Open Non Suspect')                                   │  │
│  │                                                                       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  DEFAULT THRESHOLD: 0.75 (75%)                                              │
│                                                                             │
│  NOTE: Open Non Suspect records have NULL CONFIDENCE_FACTOR                 │
│        The OR condition ensures they are included                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  RISK SCORING ENRICHMENT                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  JOIN 1: risk_member_output (Member Attributes)                             │
│  ✓ JOIN ON: HOME_PLAN_ID_CD, SAS_MODEL_VERSION, MODEL_VERSION,              │
│             MEMBER_BID, RISK_YEAR                                           │
│  ✓ GET: RISK_MODEL_TYPE, RISK_MODEL_SEGMENT, MEMBER_GENDER, MEMBER_AGE      │
│                                                                             │
│  JOIN 2: coefficient_scores (HCC Coefficients)                              │
│  ✓ BUILD: EXPECTED_CC_CODE = RISK_MODEL_SEGMENT + '_HCC' + CC_NUM           │
│    Example: CNA_HCC017 (Community Non-Dual Aged, HCC 17)                    │
│  ✓ JOIN ON: MODEL, VERSION, NAME (normalized/trimmed)                       │
│  ✓ GET: SCORE (raw coefficient)                                             │
│                                                                             │
│  JOIN 3: adjustment_factors (Normalization)                                 │
│  ✓ JOIN ON: MODEL_VERSION, RISK_MODEL_TYPE                                  │
│  ✓ GET: NORMALIZATION_FACTOR, CODING_PATTERN_ADJUSTMENT                     │
│  ✓ CALCULATE: ADJ_COEFF_SCORE = (SCORE / NORMALIZATION_FACTOR)              │
│               * (1 - CODING_PATTERN_ADJUSTMENT)                             │
│                                                                             │
│  FINAL MAPPINGS:                                                            │
│  ✓ RISK_TYPE_ID = 'HCC'                                                     │
│  ✓ RISK_TYPE_DETAIL_ID = 'H' + CC_CODE (e.g., HCC017)                       │
│  ✓ SUPPRESSION_IND: 'R' if not suppressed, 'S' if suppressed                │
│  ✓ DX_SOURCE = 'suspected_gap'                                              │
│  ✓ CLAIM_TYPE: 'facility-inpatient', 'facility-outpatient',                 │
│                'professional', 'pharmacy'                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Stage 9: Final Output

```
══════════════════════════════════════════════════════════════════════════════
                          STAGE 9: FINAL OUTPUT
══════════════════════════════════════════════════════════════════════════════

                ┌─────────────────────────────────────────────┐
                │              risk_member_hcc                │
                │                (curation)                   │
                ├─────────────────────────────────────────────┤
                │                                             │
                │  KEY OUTPUT COLUMNS:                        │
                │  • MEMBER_BID                               │
                │  • HOME_PLAN_ID_CD                          │
                │  • RISK_MODEL_YEAR, RISK_MODEL_TYPE,        │
                │    RISK_MODEL_VERSION                       │
                │  • RISK_MODEL_SEGMENT                       │
                │  • SAS_MODEL_VERSION                        │
                │  • MEMBER_GENDER, MEMBER_AGE                │
                │  • RISK_TYPE_ID = 'HCC'                     │
                │  • RISK_TYPE_DETAIL_ID (e.g., HCC017)       │
                │  • RISK_TYPE_DETAIL_DESC                    │
                │  • SUPPRESSION_IND ('R'=Active, 'S'=Supp)   │
                │  • METHOD_ID (1, 2, 4, 10)                  │
                │  • ADJ_COEFF_SCORE, COEFF_SCORE             │
                │  • METHOD_CONFIDENCE_FACTOR                 │
                │  • COMBINED_CONFIDENCE_FACTOR               │
                │  • CHRONIC_HCC ('Y'/'N')                    │
                │  • DX_SOURCE = 'suspected_gap'              │
                │  • GAP_STATUS                               │
                │  • CLAIM_TYPE, CLAIM_BID, CLAIM_DATE,       │
                │    DIAG_CD                                  │
                │  • RISK_YEAR                                │
                │  • CYCLE_RUN                                │
                │  • CREATED_DATE, CREATED_BY,                │
                │    UPDATED_DATE, UPDATED_BY                 │
                │                                             │
                │  FINAL CHECKS:                              │
                │  ✓ .distinct() to remove duplicates         │
                │  ✓ All data types cast to match schema      │
                │  ✓ INSERT INTO via SQL statement            │
                │                                             │
                └─────────────────────────────────────────────┘
```

---

## Summary Tables

### Method Summary

| Method | Source Data | Trigger | Key Logic | Output |
|--------|-------------|---------|-----------|--------|
| **1** | Pharmacy Claims | NDC Codes | Drug → Diagnosis mapping | Gaps from drug usage |
| **2** | Medical Claims | ICD Diagnosis | Diagnosis → HCC mapping | Gaps from related diagnoses |
| **10** | Medical Claims | CPT/HCPCS | Procedure + Exclusion rules (180-day lookback) | Gaps from procedures w/o exclusions |
| **4** | member_persistent_cc | Historical HCCs | Chronic condition recurrence | Gaps from persistent conditions |

---

### Key Tables

| Table | Schema | Purpose |
|-------|--------|---------|
| `draft_gaps` | gap_curation | Raw gap candidates from each method |
| `draft_gaps_with_details` | gap_curation | Gaps with suppression logic applied |
| `suspected_gaps` | gap_curation | Final gaps with frequency/confidence |
| `risk_member_hcc` | curation | Active gaps for risk scoring |
| `risk_member_hcc_suppressed` | gap_curation | Audit table for cross-method suppressed gaps |

---

### Key Filters & Checks by Stage

| Stage | Key Filters | Key Checks |
|-------|-------------|------------|
| **1. Input** | RISK_YEAR range, CYCLE_RUN, CLM_PMT_STS_CD='P', QUALIFY_CLAIMS='Y' | Valid date ranges, paid claims only |
| **2. Method Processing** | METHOD_ID specific, CLAIM_CD_TYPE match, 180-day exclusion window (M10) | Code matching, modifier validation, exclusion lookback |
| **3. Draft Gaps** | GAP_STATUS = 'Create Gap', MODEL_VERSION != 'v' | Valid model version extraction |
| **4. Suppression 1** | CHILD_CC in ref_hierarchy | Parent exists in same gap set |
| **5. Suppression 2** | CHILD_CC in ref_hierarchy | Parent exists in member's claims |
| **6. Gap Status** | SUPPRESSION1/2_YN, MATCHING_YN | Correct status assignment |
| **7. Cross-Method** | GAP_STATUS separation, CROSS_SUPPRESSION_YN | Parent exists across methods |
| **8. Threshold** | COMBINED_CONFIDENCE >= threshold OR Open Non Suspect | Confidence calculation, schema joins |
| **9. Final** | .distinct() | Data type casting, schema alignment |

---

### Simplified Flow Summary

```
  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
  │ Method 1 │   │ Method 2 │   │ Method 10│   │ Method 4 │
  │ Pharmacy │   │ Diagnosis│   │ CPT/Excl │   │Persistent│
  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘
       │              │              │              │
       └──────────────┴──────┬───────┴──────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │  draft_gaps    │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │  Suppression   │
                    │  Logic 1 & 2   │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │ suspected_gaps │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │  Cross-Method  │
                    │  Suppression   │
                    └───────┬────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
              ▼                           ▼
     ┌────────────────┐          ┌────────────────┐
     │ risk_member_hcc│          │ risk_member_   │
     │   (ACTIVE)     │          │ hcc_suppressed │
     │                │          │   (AUDIT)      │
     └────────────────┘          └────────────────┘
```

---

## Related Documents

- [Gap Suspecting ER Diagram](gap_suspecting_er_diagram.md)
- [Gap Suspecting Requirements Specification](Gap_Suspecting_Requirements_Specification.md)
- [Risk Engine Technical Documentation](risk_engine_technical_documentation.md)
