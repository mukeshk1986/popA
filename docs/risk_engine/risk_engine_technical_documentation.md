# Risk Engine Technical Documentation

## Medicare Advantage CMS Risk Adjustment Platform

**Version:** 1.0  
**Last Updated:** May 2026  
**Author:** Data Architecture Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Overview](#2-system-overview)
3. [Architecture](#3-architecture)
4. [Process Flow](#4-process-flow)
5. [Data Flow Diagrams](#5-data-flow-diagrams)
6. [Data Model](#6-data-model)
7. [Pipeline Stages](#7-pipeline-stages)
8. [Configuration](#8-configuration)
9. [Scoring Algorithm](#9-scoring-algorithm)
10. [Key Components](#10-key-components)

---

## 1. Executive Summary

The Risk Engine is a PySpark-based data processing platform that calculates CMS Risk Adjustment scores for Medicare Advantage populations. It processes member demographics, enrollment data, and medical claims to generate Hierarchical Condition Category (HCC) scores used for CMS payments.

### Supported Models

| Model | Description | Use Case |
|-------|-------------|----------|
| CMS HCC | General population risk scoring | Standard MA members |
| CMS HCC ESRD | End-Stage Renal Disease specific | ESRD population |
| RX HCC | Pharmacy/medication-based scoring | Part D risk adjustment |

### Technology Stack

- **Processing:** PySpark on Databricks
- **Storage:** Delta Lake (Bronze/Silver/Gold architecture)
- **Cloud:** AWS S3
- **Orchestration:** Databricks Workflows
- **Catalog:** Unity Catalog

---

## 2. System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           RISK ENGINE PLATFORM                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │   BRONZE     │───▶│    SILVER    │───▶│     GOLD     │───▶│  REPORTS  │ │
│  │   (Raw)      │    │ (Transform)  │    │  (Curated)   │    │           │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      REFERENCE DATA (ma_reference)                   │   │
│  │  coefficient_scores | hierarchy_config | interaction_coefficients   │   │
│  │  adjustment_factors | icd_hcc_mapping | community_model_rules       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Capabilities

- Multi-model risk scoring (CMS HCC, ESRD, RxHCC)
- Multi-version support (v24, v28, v05, v08, v21)
- Configurable time periods (Calendar/Fiscal)
- Full audit trail with JSON tracking
- Incremental processing with UPSERT operations

---

## 3. Architecture

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATABRICKS WORKSPACE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        ORCHESTRATION LAYER                           │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │   │
│  │  │ CMS HCC Calc    │  │ CMS ESRD Calc   │  │ RX HCC Calc     │     │   │
│  │  │ (Notebook)      │  │ (Notebook)      │  │ (Notebook)      │     │   │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘     │   │
│  └───────────┼────────────────────┼────────────────────┼───────────────┘   │
│              │                    │                    │                    │
│  ┌───────────▼────────────────────▼────────────────────▼───────────────┐   │
│  │                     TRANSFORMATION LAYER                             │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │              transformations_commons.py                      │   │   │
│  │  │  - load_and_prepare_data()     - assign_scores()            │   │   │
│  │  │  - enrich_claims_with_member() - apply_hcc_hierarchy()      │   │   │
│  │  │  - map_icd_to_hcc()            - calculate_final_scores()   │   │   │
│  │  │  - process_risk_member_hcc()   - normalization_scores()     │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │   │
│  │  │ cms_hcc_        │  │ cms_hcc_esrd_   │  │ cms_rxhcc_      │     │   │
│  │  │ transformations │  │ transformations │  │ transformations │     │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         DATA LAYER                                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │  SILVER     │  │    GOLD     │  │  REFERENCE  │                 │   │
│  │  │  (Source)   │  │  (Output)   │  │  (Config)   │                 │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Schema Organization

| Schema Pattern | Purpose | Example |
|----------------|---------|---------|
| `{plan}_ingestion` | Raw data landing | `anthem_ingestion` |
| `{plan}_transformation` | Silver layer | `anthem_transformation` |
| `{plan}_curation` | Gold layer outputs | `anthem_curation` |
| `ma_reference` | Reference/config tables | `ma_reference` |

---

## 4. Process Flow

### End-to-End Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RISK SCORING PIPELINE                                │
└─────────────────────────────────────────────────────────────────────────────┘

     PHASE 1: DATA PREPARATION
     ─────────────────────────
     ┌─────────────┐
     │   member    │──┐
     └─────────────┘  │
     ┌─────────────┐  │   ┌─────────────────┐      ┌─────────────────┐
     │   member    │──┼──▶│  risk_member    │─────▶│  risk_member    │
     │ enrollment  │  │   │  (data prep)    │      │  (table)        │
     └─────────────┘  │   └─────────────────┘      └─────────────────┘
     ┌─────────────┐  │
     │     mmr     │──┘
     └─────────────┘

     ┌─────────────┐
     │   medical   │──┐
     │   claims    │  │
     └─────────────┘  │   ┌─────────────────┐      ┌─────────────────┐
     ┌─────────────┐  │   │  risk_member    │      │  risk_member    │
     │ professional│──┼──▶│  _diag          │─────▶│  _diag (table)  │
     │    diag     │  │   │  (data prep)    │      │                 │
     └─────────────┘  │   └─────────────────┘      └─────────────────┘
     ┌─────────────┐  │
     │  facility   │──┘
     │    diag     │
     └─────────────┘


     PHASE 2: RISK SCORE CALCULATION
     ────────────────────────────────
     ┌─────────────────┐
     │  risk_member    │──┐
     └─────────────────┘  │
                          │   ┌─────────────────────────────────────────┐
     ┌─────────────────┐  │   │         13-STAGE PIPELINE               │
     │  risk_member    │──┼──▶│  1. Load & Validate                     │
     │  _diag          │  │   │  2. Enrich with Demographics            │
     └─────────────────┘  │   │  3. Disability Determination            │
                          │   │  4. Age/Sex/SEDITS Assignment           │
     ┌─────────────────┐  │   │  5. Community Model Assignment          │
     │   REFERENCE     │──┘   │  6. ICD to HCC Mapping                  │
     │   TABLES        │      │  7. HCC Hierarchy Rules                 │
     └─────────────────┘      │  8. Score Assignment                    │
                              │  9. Interaction Rules                   │
                              │  10. Raw Score Calculation              │
                              │  11. Normalization                      │
                              │  12. Weighted Risk Score                │
                              │  13. SAS Model Versioning               │
                              └─────────────────────────────────────────┘
                                                │
                                                ▼
     PHASE 3: OUTPUT
     ───────────────
                              ┌─────────────────────────────────────────┐
                              │           OUTPUT TABLES                  │
                              │  ┌─────────────────────────────────┐    │
                              │  │     risk_member_output          │    │
                              │  │  (Final scores per member)      │    │
                              │  └─────────────────────────────────┘    │
                              │  ┌─────────────────────────────────┐    │
                              │  │     risk_member_hcc             │    │
                              │  │  (Detailed HCC records)         │    │
                              │  └─────────────────────────────────┘    │
                              │  ┌─────────────────────────────────┐    │
                              │  │     risk_member_hcc_summary     │    │
                              │  │  (De-duplicated summary)        │    │
                              │  └─────────────────────────────────┘    │
                              └─────────────────────────────────────────┘
```

---

## 5. Data Flow Diagrams

### Level 0: Context Diagram

```
                              ┌─────────────────────┐
                              │                     │
      Member Data ───────────▶│                     │──────────▶ Risk Scores
      Claims Data ───────────▶│    RISK ENGINE      │──────────▶ HCC Details
      Reference Data ────────▶│                     │──────────▶ Audit Reports
                              │                     │
                              └─────────────────────┘
```

### Level 1: System Context

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RISK ENGINE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────────┐   │
│  │  1.0     │     │     2.0      │     │   3.0    │     │     4.0      │   │
│  │  DATA    │────▶│    RISK      │────▶│  HCC     │────▶│   OUTPUT     │   │
│  │  PREP    │     │   SCORING    │     │ PROCESS  │     │   WRITER     │   │
│  └──────────┘     └──────────────┘     └──────────┘     └──────────────┘   │
│       ▲                  ▲                  ▲                  │            │
│       │                  │                  │                  ▼            │
│  ┌────┴──────────────────┴──────────────────┴────┐    ┌──────────────┐    │
│  │              REFERENCE DATA                    │    │   DELTA      │    │
│  │  time_periods | coefficient_scores | hierarchy │    │   TABLES     │    │
│  └────────────────────────────────────────────────┘    └──────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Level 2: Process Decomposition

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          2.0 RISK SCORING                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    2.1      │    │    2.2      │    │    2.3      │    │    2.4      │  │
│  │  ENRICH    │───▶│  DISABILITY │───▶│  COMMUNITY  │───▶│   ICD TO    │  │
│  │  CLAIMS    │    │  DETERMINE  │    │   MODEL     │    │    HCC      │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                                                        │          │
│         ▼                                                        ▼          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    2.8      │◀───│    2.7      │◀───│    2.6      │◀───│    2.5      │  │
│  │   FINAL    │    │ INTERACTION │    │   ASSIGN    │    │  HIERARCHY  │  │
│  │   SCORE    │    │    RULES    │    │   SCORES    │    │    RULES    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                                                                    │
│         ▼                                                                    │
│  ┌─────────────┐    ┌─────────────┐                                         │
│  │    2.9      │───▶│    2.10     │                                         │
│  │ NORMALIZE   │    │  WEIGHTED   │                                         │
│  └─────────────┘    └─────────────┘                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Data Model

### Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA MODEL - ERD                                   │
└─────────────────────────────────────────────────────────────────────────────┘

                         SOURCE TABLES (SILVER)
    ┌────────────────┐                           ┌────────────────┐
    │     member     │                           │ medical_claims │
    ├────────────────┤                           ├────────────────┤
    │ PK: MEMBER_BID │◀──────────────────────────│FK: MEMBER_BID  │
    │    MEMB_ID_CD  │                           │ PK: CLAIM_BID  │
    │    MEMB_BRTH_DT│                           │    CLM_TP_CD   │
    │    MEMB_GENDER │                           │    FIRST_SERV  │
    │    HOME_PLAN_ID│                           │    LAST_SERV   │
    └────────────────┘                           └───────┬────────┘
           │                                             │
           │                                             │
           ▼                                             ▼
    ┌────────────────┐                           ┌────────────────┐
    │member_enrollment                           │ professional   │
    ├────────────────┤                           │ _diag          │
    │FK: MEMBER_BID  │                           ├────────────────┤
    │    COVRG_BEGIN │                           │FK: CLAIM_BID   │
    │    COVRG_END   │                           │    DIAG_CD     │
    │    PROD_TYPE   │                           │    DIAG_TYPE   │
    └────────────────┘                           └────────────────┘


                         CURATED TABLES (GOLD)
    ┌────────────────────────────────────────────────────────────────────┐
    │                         risk_member                                 │
    ├────────────────────────────────────────────────────────────────────┤
    │ PK: RISK_MEMBER_ID (bigint)                                        │
    │     HOME_PLAN_ID_CD, MEMB_ID_CD, BHI_MEMB_ID, DEID_MEM_ID          │
    │     PERSON_ID_CD, ISSUER_CONTRACT_ID, MBRS_STATE                   │
    │     MEMB_BRTH_DT, MEMB_GENDER_CD, MEMB_DEATH_DT                    │
    │     PLAN_ID, POP_ID, COVRG_BEGIN_DT, COVRG_END_DT                  │
    │     ACCT_CD, GRP_CD, SUBGRP_CD, PROD_ID_CD, PROD_TYPE_CD           │
    │     PHRMCY_BNFT_IND, MED_BNFT_IND, METAL                           │
    │     RISK_MODEL, OREC, ESRD_STATUS, LIS_STATUS_PART_D               │
    │     INSTITUTIONAL_STATUS_PART_C/D, MEDICAID_STATUS, CSNP_STATUS    │
    │     MEDICARE_NEWENROLLEE_STATUS, AGE_GROUP, MODEL_SEGMENT_PART_C/D │
    │     PLAN_NAME, TIME_PERIOD, SOURCE_LOAD_MONTH                      │
    │     CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE             │
    └────────────────────────────────────────────────────────────────────┘
           │
           │ 1:N
           ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                       risk_member_diag                              │
    ├────────────────────────────────────────────────────────────────────┤
    │ FK: RISK_MEMBER_ID (bigint)                                        │
    │     HOME_PLAN_ID_CD, MEMB_ID_CD                                    │
    │ PK: CLAIM_BID (bigint), CLM_ID_CD                                  │
    │     ADJ_SEQ_NUM, CLM_TP_CD (1=inpatient, 2=outpatient, 3=prof)    │
    │     SUPPLEMENTAL_DIAG_FLAG, TP_OF_BILL_CD                          │
    │     FIRST_SERV_DT, LAST_SERV_DT                                    │
    │     PROD_ID_CD, DIAG_TYPE_CD, DIAG_NUM                             │
    │     DIAG_CD (varchar 8), ICD_VER (10/9)                            │
    │     DX_SOURCE (claim/supplemental), PSEUDO_CLAIM_TYPE              │
    │     SOURCE_LOAD_MONTH, PLAN_NAME, TIME_PERIOD                      │
    │     CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE             │
    └────────────────────────────────────────────────────────────────────┘
           │
           │ Processing
           ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                      risk_member_output                             │
    ├────────────────────────────────────────────────────────────────────┤
    │ PK: RISK_MEMBER_ID, TIME_PERIOD, YEAR_MONTH, CYCLE_RUN_DATE        │
    │     HOME_PLAN_ID_CD, ISSUER_CONTRACT_ID                            │
    │     RISK_MODEL_TYPE, RISK_MODEL_VERSION, RISK_MODEL_YEAR           │
    │     RISK_MODEL_SEGMENT, SAS_MODEL_VERSION                          │
    │     MEMBER_GENDER, MEMBER_AGE                                      │
    │     ──── SCORE COLUMNS ────                                        │
    │     DEMOG_SCORE, ORIGDS_SCORE, HCC_SCORE                           │
    │     INTERACTION_SCORE, HCC_COUNT_PAYMENT_SCORE                     │
    │     RISK_SCORE_RAW, RISK_SCORE_NORMALIZED                          │
    │     RISK_SCORE_PAYMENT_HCC, WEIGHTED_RISK_SCORE                    │
    │     ──── JSON DETAIL COLUMNS ────                                  │
    │     DIAG_CD_TO_HCC_JSON_LIST (array of ICD→HCC mappings)          │
    │     HCC_TO_SCORE_JSON_LIST (array of HCC→score mappings)          │
    │     YEAR_MONTH, CYCLE_RUN_DATE                                     │
    │     CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE             │
    └────────────────────────────────────────────────────────────────────┘
           │
           │ 1:N
           ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                        risk_member_hcc                              │
    ├────────────────────────────────────────────────────────────────────┤
    │ FK: MEMBER_BID (bigint)                                            │
    │     HOME_PLAN_ID_CD, RISK_MODEL_YEAR, RISK_MODEL_TYPE              │
    │     RISK_MODEL_VERSION, RISK_MODEL_SEGMENT, SAS_MODEL_VERSION      │
    │     MEMBER_GENDER, MEMBER_AGE                                      │
    │     ──── RISK TYPE COLUMNS ────                                    │
    │     RISK_TYPE_ID (HCC/MEMBER_AGE/DISABILITY/DISEASE INTERACTION)  │
    │     RISK_TYPE_DETAIL_ID (HCC code or demographic key)              │
    │     RISK_TYPE_DETAIL_DESC                                          │
    │     SUPPRESSION_IND (Y/N/R - suppressed, not suppressed, retained) │
    │     ──── SCORE COLUMNS ────                                        │
    │     ADJ_COEFF_SCORE, COEFF_SCORE                                   │
    │     METHOD_CONFIDENCE_FACTOR, COMBINED_CONFIDENCE_FACTOR           │
    │     ──── CLAIM COLUMNS ────                                        │
    │     CLAIM_BID, CLAIM_DATE, CLAIM_TYPE                              │
    │     DIAG_CD, DX_SOURCE                                             │
    │     ──── STATUS COLUMNS ────                                       │
    │     CHRONIC_HCC (Y/N), GAP_STATUS, METHOD_ID                       │
    │     HRA, HOME_HRA, RISK_YEAR, ACTIVE_IND                           │
    │     CYCLE_RUN                                                       │
    │     CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE             │
    └────────────────────────────────────────────────────────────────────┘


                        REFERENCE TABLES (CONFIG)
    ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
    │ time_periods   │    │coefficient_    │    │ hierarchy_     │
    ├────────────────┤    │scores          │    │ config         │
    │PK: TIME_PERIOD │    ├────────────────┤    ├────────────────┤
    │    BEGIN_DATE  │    │PK: MODEL       │    │PK: MODEL       │
    │    END_DATE    │    │    VERSION     │    │    VERSION     │
    │    TYPE_CD(C/F)│    │    NAME        │    │    PARENT      │
    └────────────────┘    │    SCORE       │    │    CHILD       │
                          │    LABEL       │    └────────────────┘
                          └────────────────┘

    ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
    │ interaction_   │    │ adjustment_    │    │ icd_hcc_       │
    │ coefficients   │    │ factors        │    │ mapping        │
    ├────────────────┤    ├────────────────┤    ├────────────────┤
    │PK: MODEL       │    │PK: MODEL       │    │PK:DIAGNOSIS    │
    │    VERSION     │    │    VERSION     │    │   CODE         │
    │    HCC_CODE_1  │    │    FACTOR      │    │   CMS-HCC-V24  │
    │    HCC_CODE_2  │    └────────────────┘    │   CMS-HCC-V28  │
    │    SCORE       │                          │   RX-HCC-V05   │
    └────────────────┘                          └────────────────┘

    ┌────────────────┐    ┌────────────────┐
    │ community_     │    │ version_       │
    │ model_rules    │    │ weightage_     │
    ├────────────────┤    │ risk_year      │
    │PK: MODEL       │    ├────────────────┤
    │    VERSION     │    │PK: MODEL       │
    │    CONDITION   │    │    VERSION     │
    │    SEGMENT     │    │    TIME_PERIOD │
    │    PRIORITY    │    │    WEIGHTAGE   │
    └────────────────┘    └────────────────┘
```

### Table Relationships Summary

| Parent Table | Child Table | Relationship | Join Key |
|--------------|-------------|--------------|----------|
| risk_member | risk_member_diag | 1:N | RISK_MEMBER_ID |
| risk_member | risk_member_output | 1:N | RISK_MEMBER_ID |
| risk_member_output | risk_member_hcc | 1:N | MEMBER_BID + TIME_PERIOD |
| time_periods | risk_member | 1:N | TIME_PERIOD |
| coefficient_scores | risk_member_output | N:1 | MODEL + VERSION + NAME |
| hierarchy_config | (processing) | - | MODEL + VERSION |
| icd_hcc_mapping | (processing) | - | DIAGNOSISCODE |

---

## 7. Pipeline Stages

### Stage 1-2: Data Loading & Enrichment

**Function:** `load_and_prepare_data()`, `enrich_claims_with_member_data()`

```
Input:  risk_member, risk_member_diag, time_periods
Output: Enriched claims DataFrame with member demographics

Process:
1. Load risk_member filtered by plan_id, contract, SOURCE_LOAD_MONTH
2. Load risk_member_diag filtered by same criteria
3. Calculate member age based on reference date (Feb 1 of payment year)
4. Join member demographics with diagnosis records
```

### Stage 3: Disability Determination

**Function:** `get_original_disability()`

```
Input:  Enriched claims DataFrame
Output: DataFrame with DISABL and ORIGDS flags

Logic:
- DISABL = 1 if (age < 65 AND OREC != "0") else 0
- ORIGDS = 1 if (OREC == "1" AND DISABL == 0) else 0
```

### Stage 4: Age/Sex/SEDITS Assignment

**Function:** `age_sex_sedits()`

```
Input:  DataFrame with disability flags
Output: DataFrame with SEDITS overrides applied

Process:
- Apply diagnosis-specific overrides based on version rules
- Example: Female + diagnosis D66/D67 → CC 112 (v28) or 48 (v24)
```

### Stage 5: Community Model Assignment

**Function:** `assign_community_model()`

```
Input:  DataFrame with SEDITS applied
Output: DataFrame with COMMUNITY_MODEL segment assigned

Segment Types:
- NE_*: New Enrollee segments
- SNPNE_*: CSNP New Enrollee segments
- Continuing enrollee segments (no prefix)
```

### Stage 6: ICD to HCC Mapping

**Function:** `map_icd_to_hcc()`

```
Input:  DataFrame with community model, icd_hcc_mapping table
Output: DataFrame with HCC codes assigned to diagnoses

Process:
1. Join diagnosis codes with ICD-HCC mapping table
2. Map column varies by model version
3. Aggregate unique HCC codes per member
4. Track ICD→HCC mappings in JSON format
```

### Stage 7: HCC Hierarchy Rules

**Function:** `apply_hcc_hierarchy_rules()`

```
Input:  DataFrame with HCC codes, hierarchy_config table
Output: DataFrame with suppressed child HCCs removed

Process:
- Apply parent-child suppression rules
- Remove child HCC codes when parent HCC is present
- Mark suppression indicator (SUPPRESSION_IND)
```

### Stage 8: Score Assignment

**Function:** `assign_scores()`

```
Input:  DataFrame with final HCCs, coefficient_scores table
Output: DataFrame with all score components

Score Types:
- DEMOG_SCORE: {COMMUNITY_MODEL}_{GENDER}{AGE}
- HCC_SCORE: {COMMUNITY_MODEL}_HCC_{HCC_CODE}
- ORIGDS_SCORE: {COMMUNITY_MODEL}_OriginallyDisabled_{GENDER}
- HCC_COUNT_PAYMENT: HCC_COUNT_PAYMENT_{N}
```

### Stage 9: Interaction Rules

**Function:** `add_interaction_rules_and_calculate_score()`

```
Input:  DataFrame with scores, interaction_coefficients table
Output: DataFrame with INTERACTION_SCORE added

Process:
- Identify qualifying HCC pairs for each member
- Apply interaction scoring rules
- Calculate cumulative interaction score
```

### Stage 10: Raw Score Calculation

**Function:** `calculate_final_scores()`

```
Input:  DataFrame with all score components
Output: DataFrame with RAW_RISK_SCORE

Formula:
RAW_RISK_SCORE = DEMOG_SCORE + HCC_SCORE + ORIGDS_SCORE 
               + INTERACTION_SCORE + HCC_COUNT_PAYMENT_SCORE
```

### Stage 11: Normalization

**Function:** `normalization_scores()`

```
Input:  DataFrame with raw scores, adjustment_factors table
Output: DataFrame with NORMALIZED_RISK_SCORE

Formula:
NORMALIZED_RISK_SCORE = RAW_RISK_SCORE * normalization_factor
```

### Stage 12: Weighted Risk Score

**Function:** `weighted_risk_score()`

```
Input:  DataFrame with normalized scores, version_weightage_risk_year table
Output: DataFrame with WEIGHTED_RISK_SCORE

Formula:
WEIGHTED_RISK_SCORE = NORMALIZED_RISK_SCORE * weightage
```

### Stage 13: Output & SAS Model Versioning

**Function:** `get_sas_model_version()`

```
Input:  DataFrame with weighted scores, ref_sas_model table
Output: Final DataFrame with SAS_MODEL_VERSION

Process:
1. Lookup SAS model version
2. Add audit columns (CREATED_BY, CREATED_DATE, etc.)
3. Write to risk_member_output table
```

---

## 8. Configuration

### Environment Configuration

**Location:** `config/environments/{env}/values.yaml`

```yaml
catalog: pop_{env}
config_schema: ma_reference
notebook_time_out: 3600
target_score_table_mode: append
```

### Column Mappings

**Location:** `config/sql/file_read_meta.yaml`

```yaml
RISK_MEMBER_COLUMNS:
  - RISK_MEMBER_ID
  - HOME_PLAN_ID_CD
  - MEMB_ID_CD
  # ... additional columns

RISK_MEMBER_DIAG_COLUMNS:
  - RISK_MEMBER_ID
  - CLAIM_BID
  - DIAG_CD
  # ... additional columns
```

### Model Constants

**Location:** `config/constants/ma_ra_model_constants.yaml`

```yaml
SEDITS_RULES:
  - version: v28
    condition: "gender = 'F' AND diag_cd IN ('D66', 'D67')"
    hcc_override: 112
```

---

## 9. Scoring Algorithm

### Algorithm Summary

```
For each member in risk population:

  1. RETRIEVE all diagnosis codes from claims during time period
  
  2. MAP each ICD code to HCC using icd_hcc_mapping
     - Join key: DIAGNOSISCODE
     - HCC column varies by model version
  
  3. APPLY SEDITS rules (age/gender/diagnosis overrides)
     - Specific to model version
  
  4. APPLY HCC hierarchy
     - Remove child codes if parent exists
     - Mark suppression indicator
  
  5. RETRIEVE coefficient scores:
     - Demographic: {COMMUNITY_MODEL}_{GENDER}{AGE}
     - HCC: {COMMUNITY_MODEL}_HCC_{HCC_CODE}
     - Originally Disabled: {COMMUNITY_MODEL}_OriginallyDisabled_{GENDER}
  
  6. AGGREGATE scores by member
     - Sum all HCC scores
     - Count HCCs for payment adjustment
  
  7. APPLY interaction rules (if applicable)
     - Check HCC pairs against interaction_coefficients
  
  8. CALCULATE total raw score:
     RAW = DEMOG + HCC + ORIGDS + INTERACTION + HCC_COUNT_PAYMENT
  
  9. APPLY normalization:
     NORMALIZED = RAW * adjustment_factor
  
  10. APPLY version weightage:
      WEIGHTED = NORMALIZED * weightage
  
  11. OUTPUT all score components with audit metadata
```

### Coefficient Key Conventions

| Score Type | Key Pattern | Example |
|------------|-------------|---------|
| Demographic | `{MODEL}_{GENDER}{AGE}` | `CNA_M65` |
| HCC | `{MODEL}_HCC{CODE}` | `CNA_HCC019` |
| Originally Disabled | `{MODEL}_OriginallyDisabled_{GENDER}` | `CNA_OriginallyDisabled_Male` |
| HCC Count Payment | `{MODEL}_D{COUNT}` | `CNA_D10P` |

---

## 10. Key Components

### File Structure

```
src/spark/
├── cms/
│   ├── cms_hcc_risk_score_calc.py       # Main CMS HCC orchestration
│   ├── cms_hcc_esrd_risk_score_calc.py  # ESRD orchestration
│   ├── cms_rxhcc_risk_score_calc.py     # RX HCC orchestration
│   ├── cms_hcc_transformations.py       # CMS HCC specific transforms
│   ├── cms_hcc_esrd_transformations.py  # ESRD specific transforms
│   └── cms_rxhcc_transformations.py     # RX HCC specific transforms
├── helpers/
│   ├── transformations_commons.py       # Core transformation functions (40+)
│   ├── databricks_util.py               # Delta/Spark utilities
│   ├── config_util.py                   # Configuration readers
│   └── logger_util.py                   # Logging utilities
├── data_prep/
│   ├── ma_model_input_data.py           # Data preparation pipeline
│   └── ma_model_input_rules.py          # Data prep business rules
└── gp_suspecting/
    └── gap_suspecting_helper.py         # Gap suspecting utilities
```

### Core Functions Reference

| Function | Location | Purpose |
|----------|----------|---------|
| `load_and_prepare_data` | transformations_commons.py | Load all input/config tables |
| `enrich_claims_with_member_data` | transformations_commons.py | Member-claim enrichment |
| `get_original_disability` | transformations_commons.py | Disability flag calculation |
| `age_sex_sedits` | cms_hcc_transformations.py | Age/sex/SEDITS overrides |
| `assign_community_model` | transformations_commons.py | Segment assignment |
| `map_icd_to_hcc` | transformations_commons.py | ICD to HCC mapping |
| `apply_hcc_hierarchy_rules` | transformations_commons.py | Hierarchy suppression |
| `assign_scores` | cms_hcc_transformations.py | Score retrieval |
| `add_interaction_rules_and_calculate_score` | transformations_commons.py | Interaction scoring |
| `calculate_final_scores` | transformations_commons.py | Raw score calculation |
| `normalization_scores` | transformations_commons.py | Apply normalization |
| `weighted_risk_score` | transformations_commons.py | Apply weightage |
| `process_risk_member_hcc` | transformations_commons.py | HCC detail processing |

### Execution Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `plan_name` | Plan identifier | `anthem`, `bcbsla` |
| `env` | Environment | `DEV`, `QA`, `STG`, `PROD` |
| `model` | Risk model type | `cms_hcc`, `cms_hcc_esrd`, `rx_hcc` |
| `version` | Model version | `v24`, `v28`, `v05` |
| `time_period` | Processing period | `5` (Calendar 2023) |
| `year_month` | Source load month | `2025_12` |
| `plan_id` | Plan ID filter | `10` |
| `contract` | Contract filter | `77514` |

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| HCC | Hierarchical Condition Category |
| CMS | Centers for Medicare & Medicaid Services |
| MA | Medicare Advantage |
| OREC | Original Reason for Entitlement Code |
| ESRD | End-Stage Renal Disease |
| SEDITS | Sex-based Edits |
| ORIGDS | Originally Disabled Status |
| RAF | Risk Adjustment Factor |
| CSNP | Chronic Special Needs Plan |

---

## Appendix B: Time Period Reference

| TIME_PERIOD | BEGIN_DATE | END_DATE | TYPE | RISK_YEAR |
|-------------|------------|----------|------|-----------|
| 1 | 2021-01-01 | 2021-12-31 | C | 2021 |
| 2 | 2021-07-01 | 2022-06-30 | F | 2022 |
| 3 | 2022-01-01 | 2022-12-31 | C | 2022 |
| 4 | 2022-07-01 | 2023-06-30 | F | 2023 |
| 5 | 2023-01-01 | 2023-12-31 | C | 2023 |
| 6 | 2023-07-01 | 2024-06-30 | F | 2024 |
| 7 | 2024-01-01 | 2024-12-31 | C | 2024 |
| 8 | 2024-07-01 | 2025-06-30 | F | 2025 |
| 9 | 2025-01-01 | 2025-12-31 | C | 2025 |

**Type:** C = Calendar Year, F = Fiscal Year

---

*End of Document*
