# Cloud PopA – Guiding Principles

## Table of Contents

1. Overview of Project – 3
2. Data Preparation (Ingestion & Transformation) – 3
   - Loading data to STAGE tables – 4
   - Moving data from Stage to Transformation tables – 5
3. CMS Claim Qualification Logic – 6
   - Medical Claims for PopA – 8
   - Provider Visit Type Identification (E&M Visits) – 9
   - HRA identification (including IN-HOME) – 9
4. CMS Supplemental files (MMR, MAO-004 & MOR) – 10
   - MBI mapping to Member – 10
   - MMR – 10
   - MAO-004 – 10
   - MOR – 10
   - Pseudo Claims (Based on MAO-004 data) – 10
5. CMS Risk Scoring – 16
   - risk_member – 16
   - risk_member_diag – 16
   - risk_member_hcc – 16
   - risk_member_output – 16
   - Parameters for running Risk Scoring Module – 16
6. GAP Suspecting – 16
   - Methodologies and logic – 16
     - Method 1: On-Rx (Drug Indicators) Methodology – 16
     - Method 2: NOS/NEC Codes (Not Otherwise Specified (NOS) or Not Elsewhere Classified (NEC)) Methodology – 17

---

## Overview of Project

As part of the Population Advyzer modernization initiative, we are designing advanced Risk Adjustment scoring models to optimize revenue accuracy and compliance for Medicare Advantage and Affordable Care Act (ACA) populations.

Aligned with CMS regulatory guidelines, these models will empower payers to forecast risk more precisely, enhance reimbursement outcomes, and drive data-driven strategies for population health management.

### Value Outcomes

By modernizing the Population Advyzer Risk Scoring Engine for Medicare Advantage on cloud-native architecture, we will unlock scalable, on-demand processing power to handle growing Medicare Advantage population data volumes and generate related Client Data extracts.

- This foundational shift ensures faster, cost-efficient model iterations while aligning with enterprise cloud-first objectives.
- Modular Design will accelerate time-to-market for any version or rules updates from CMS by ensuring rapid adaptation to regulatory shifts.

---

## Data Preparation (Ingestion & Transformation)

For Cloud PopA (CMS MA – Risk Adjustment module, GAP Suspecting module), data topics needed are classified into 4 broad categories: Member, Claims, Supplemental & Reference data. These details are gathered through the files below:

**Member (Demographics & Enrollment information)**
1. Member
2. Member enrollment

**Claims (Claims, Provider & other related information)**
3. Facility header
4. Facility detail
5. Facility Diag
6. Professional
7. Professional Diag
8. Pharmacy
9. Provider
10. Designated Provider
11. Location
12. Lab
13. Health Events
14. Quality Events
15. Plan Specialty Reference

**Supplemental Files – CMS**
16. Monthly Membership Report (MMR)
17. Medicare Advantage Organization (MAO-004) Report
18. Model Output Report (MOR)

**Reference Tables** – see "Reference Tables" section

- Plan files: For field definition and mapping guidelines, refer to *"Population Advyzer File Layouts v3.6-RK.xlsx"*. For file layout / column order for loading, refer to the *Mapping STG* file.
- Supplemental Files: For field definition and layout, refer to the CMS specifications documents.

### Loading data to STAGE tables

- All fields in Stage tables will be stored AS-IS with STRING as data type.
- All Stage tables are append only; do not delete records, as this is required for history maintenance with audit fields.

**Step 1** – All data files shall be provided by plans in the designated AWS inbox bucket location as defined for that plan, except for supplemental files, which shall be shared via Databricks Deltashare. All files will be monthly incremental data files.

**Step 2** – The ingestion job shall read the files from this location. If it is a valid file, then it shall process it & move it to the ingestion volume `src` folder (path TBD); invalid files will be moved to a separate folder (TBD with Mukesh/Shyamal) for audit & logging purposes.

**Step 3** – All valid files shall be read from the ingestion volume `src` folder (path TBD) and will be loaded to stage tables in the ingestion schema. These files will then be moved to the archive location post successful processing.

### Moving data from Stage to Transformation tables

**Document link for Validation rules for each file** (TBD)

If Stage data is loaded successfully and meets the thresholds for monthly run processing, then only should it trigger the Data Transformation step.

**Step 1** – Create ID tables as mentioned below to assign unique IDs for each record and to connect data topics of the same category.

Example – FACILITY_ID is used to connect Facility header, Facility detail, and Facility diag tables.

**ID Tables:**

1. **member_id**
   MEMBER_BID logic = Create a new BID for each distinct combination of PERSON_ID_CD & HOME_PLAN_ID_CD from stage_member table. Also, get MEMB_ID_CD to attach to each Member_BID record.
   Use Prefix **'11'** (Refer Appendix 1)

2. **facility_id**
   FACILITY_BID logic = Create a new FACILITY_BID for each distinct combination of CLM_ID_CD & HOME_PLAN_ID_CD from stage_facility_header table.
   Use Prefix **'22'** (Refer Appendix 1)

3. **professional_id**
   PROFESSIONAL_BID logic = Create a new PROFESSIONAL_BID for each distinct combination of CLM_ID_CD & HOME_PLAN_ID_CD from stage_professional table.
   Use Prefix **'33'** (Refer Appendix 1)

4. **pharmacy_id**
   PHARMACY_BID logic = Create a new PHARMACY_BID for each distinct combination of CLM_ID_CD & HOME_PLAN_ID_CD from stage_pharmacy table.
   Use Prefix **'44'** (Refer Appendix 1)

5. **mao_id** (This is used for creating distinct MAO member records)
   SUPPLEMENTAL_BID logic = Create a new SUPPLEMENTAL_BID for each distinct combination of MBI, encounter ICN, encounter type, encounter submission date, serv_type, DOS_From, DOS_Thru, source load month from stage_medicare_mao table.

6. **risk_prov** (This is the Master provider table)
   PROVIDER_BID logic = Create a new PROVIDER_BID for each distinct combination of TBD from stage tables.
   Use Prefix **'66'** (Refer Appendix 1)

7. **risk_location** (This is the Master location table)
   LOCATION_BID logic = Create a new LOCATION_BID for each distinct combination of TBD from stage tables.
   Use Prefix **'77'** (Refer Appendix 1)

**Step 2** – By appending IDs created for each data topic with stage data, load records to transformation tables (permanent tables). During this process, ensure all data validation rules are checked and adhered to.

**Step 3** – See 'TBD' document for detailed table-level rules & logic.

**Step 4** – All errors and unprocessed records should be logged in an error table for audit and re-processing purposes.

---

## CMS Claim Qualification Logic

> **IMPORTANT NOTE:** CLAIMS ARE MARKED QUALIFIED OR UNQUALIFIED AT CLAIM LEVEL, AND NOT AT CLAIM LINE LEVEL.

Note: Member Coverage – should have valid coverage during date(s) of service (DOS).

**Step 1** – Get all MA Facility (Inpatient & Outpatient) claims.
Claim type = Inpatient or Outpatient; Pop_id = 05

**Step 1b** – Segregate Pseudo Claims from this dataset by applying this condition (TBD).

**Step 2** – Apply Inpatient filters – Stage 1 (This is at Claim Header level – FACILITY_HEADER table)

Conditions:
- Claim Type (CLM_TP_CD) = Inpatient (1)
- Claim Payment status (CLM_PMT_STS_CD) = Partially/Fully Paid (P)
- Type of Bill code (TP_OF_BILL_CD) = starts with 11 or 41
- MA_PROV_TYPE = HI (for future enhancement)

Output: Mark all inpatient claims that pass filter condition as 'Qualified' and mark all inpatient claims that do NOT pass filter condition as 'Unqualified'.

**Step 3** – Apply Outpatient filters – Stage 1 (This is at Claim Header level – FACILITY_HEADER table)

Conditions:
- Claim Type (CLM_TP_CD) = Outpatient (2)
- Allowed amount (ALLWD_AMT) > 0
- Claim Payment status (CLM_PMT_STS_CD) = Partially/Fully Paid (P)
- Type of Bill code (TP_OF_BILL_CD) = starts with 13 or 85
- MA_PROV_TYPE = HO (for future enhancement)

Output: Mark all outpatient claims that do NOT pass filter condition as 'Unqualified'. Select all outpatient claims that pass filter condition to Stage 2 filtering.

**Step 4** – Apply Outpatient filters – Stage 2 (CPT/HCPCS validation – This is at Claim Line level – FACILITY_DETAIL table)

Condition: At least one CPT/HCPCS code from facility detail lines for a given claim (CLM_ID_CD) should match with the valid CPT/HCPCS code list from `ref_risk_proc_qualifying` (CMS eligible & within the effective and expiration date).

Output: Mark all remaining outpatient claims from Stage 1 that pass filter condition as 'Qualified', and mark all remaining outpatient claims from Stage 1 that do NOT pass filter condition as 'Unqualified – CPT Invalid'.

**Step 5** – Get all MA Professional claims.

**Step 6** – Apply Professional filters – Stage 1 (This is at Claim Header level – PROFESSIONAL table)

Conditions:
- Claim Payment status (CLM_PMT_STS_CD) = Partially/Fully Paid (P)
- MA_PROV_TYPE = PH (for future enhancement)

Output: Mark all Professional claims that do NOT pass filter condition as 'Unqualified'. Select all Professional claims that pass filter condition to Stage 2 filtering.

**Step 7** – Apply Professional filters – Stage 2 (CPT validation)

Condition: At least one CPT/HCPCS code from Professional claim lines for a given claim (CLM_ID_CD) should match with the valid CPT/HCPCS code list from `ref_risk_proc_qualifying` (CMS eligible & within the effective and expiration date).

Output: Mark all Professional claims from Stage 2 that passed filter condition as 'Qualified', and mark all remaining Professional claims from Stage 2 that do NOT pass filter condition as 'Unqualified – CPT Invalid'.

**Step 8** – Union all qualified & unqualified Facility & Professional claims into one new table, and add the below additional fields:
- Risk Year = Year from Statement Thru Date
- Program = CMS (where Pop_id = 05)
- Qualified = Y (all qualified claims from Step 2, 4 & 7); N (all Unqualified or Unqualified – CPT Invalid claims from Step 2, 3, 4, 6 & 7)
- Invalid CPT = Y (all Unqualified – CPT Invalid claims from Step 4 & 7), else Blank

### Medical Claims for PopA

This table will hold all the claims from Facility, Professional, and Pseudo claims (MAO-004 based).

### Provider Visit Type Identification (E&M Visits)

### HRA identification (including In-Home)

- Is Encounter ICN in the MAO file the same as CLM_ID_CD from claims, that we can use to match MAO to claims data? **No**, this is not the same — use the logic below:
  - Member ID, Claim Type (Inpatient, Outpatient, Professional), and DOS_THRU (from MAO) should fall between First & Last Claim Dates (from Claims).
  - For Facility claims (Inpatient, Outpatient) → First Claim Date is `STMT_FROM_DT` & Last Claim Date is `STMT_THRU_DT`.
  - For Professional claims → First Claim Date is `SERV_FROM_DT` & Last Claim Date is `SERV_END_DT`.
  - For PopA purposes, use the statement through date on the facility header (stage_facility_header – `STMT_THRU_DT`), and the service end date on the professional (stage_professional – `SERV_END_DT`) claims.

**Step 5** – Create curated data (risk_member, risk_member_diag, risk_member_hcc)

**Document rules for creating above curated data from transformation tables.**

1. For **risk_member_diag** table:
   a. Bring data from Pseudo claim tables (Supp. Facility & Supp Professional) to risk_member_diag based on Member_BID match. For matching, use `DE_ID` as MBI and find the Member_BID to match. Map as many columns as possible that are similar (to be discussed further).
   b. Add HCC_Version columns (HCC_V24, HCC_V28, RXHCC_V05, RXHCC_V08, ESRD_V21 & ESRD_V24) and bring the matching CC code from ref table (`icd_hcc_mapping`) for each DIAG code. This is similar to how the Diag-to-CC array is constructed in risk_member_output.

2. All Risk_member & medical claims tables:
   a. Should have a Member_BID column, where we currently have Risk_BID or Risk_member_ID column, to be consistent.

**Step 6** – Apply risk scoring logic to generate risk score in detail, with month-over-month tracking capabilities (populate data in `risk_member_output` table).

---

## CMS Supplemental files (MMR, MAO-004 & MOR)

- For each Home Plan ID, there should be a mapping with Contract ID. This will link the supplemental file association to Home Plan ID and will help in connecting members & their IDs to MBI. Create a reference table (`ref_home_plan_contract`) for this purpose.

### MBI mapping to Member

- MBI should be mapped to Member_BID to maintain data consistency across systems and to map it to CMS supplemental files.
- The `DEID_MEM_ID` field in the Member table will house the MBI value that comes from the Plan (example – BCBS LA). However, some plans may not provide MBI in the `DEID_MEM_ID` field, and in those cases, a crosswalk file is obtained from Plans (containing MBI, MEMB_ID_CD, HOME_PLAN_PROD_ID & HOME_PLAN_ID_CD) that should be used to map to MBI, and then used to connect to CMS supplemental files data.

### MMR
For layout & guidelines, see: `mapd-PCUG_MMR.pdf`

### MAO-004
For layout & guidelines, see: `mapd-PCUG_MAO-004.pdf`

### MOR
For layout & guidelines, see: `mapd-PCUG_MOR.pdf`

### Pseudo Claims (Based on MAO-004 data)

The business purpose is to ensure accurate risk adjustment by incorporating additional diagnosis information provided by CMS in the MAO-004 file, which directly impacts the risk score calculation.

1. MAO-004 file is ingested into `stage_medicare_mao`.
2. From `stage_medicare_mao`:
   - Dedup (MBI, encounter ICN, encounter type, encounter submission date, serv_type, DOS_From, DOS_Thru, source load month) and create a Supplemental BID, inserting into a table (possibly named `mao_claim`).
   - Unpivot and create MAO records with the above-created Supplemental BID, inserting into a table (possibly named `mao_claim_diag`).
   - Match the `mao_claim` (where Allowed/Disallowed flag = 'A') with the medical claim (using claim matching criteria).
     - If matched, check DIAG match from `mao_claim_diag` against `facility_diag`/`professional_diag`.
       - If `mao_claim_diag` has any unmatched DIAGs, create a pseudo claim (possibly `Supplemental_claim`) & pseudo claim diagnoses (possibly `Supplemental_Claim_Diag`) for unmatched DIAGs whose diag flag = 'A'.
     - If unmatched, create a pseudo claim (possibly `Supplemental_claim`) & pseudo claim diagnoses (possibly `Supplemental_Claim_Diag`) for all DIAGs whose diag flag = 'A'.
   - Add this `Supplemental_claim` table data into Medical Claims with the qualified claim indicator set to qualified.
     - Ensure to add a Pseudo Claim Type field and populate values based on the table below.

**MAO Matching criteria → Pseudo Claim Encounter Type mapping:**

| MAO Matching Criteria | Pseudo Claim Encounter Type |
|---|---|
| If it matches to an actual claim (based on above criteria) but has additional diagnoses from MAO | Visit – Provider |
| If it doesn't match to an actual claim (based on above criteria) and has diagnoses from MAO | Visit – Home Assessment |

- Add these `Supplemental_claim_diag` details to `risk_member_diag`, with diagnosis source as "Pseudo claim" and the category (Chart Review or Visit) associated with it.

- Criteria to connect MAO data to actual claims – Member ID, Claim Type (Inpatient, Outpatient, Professional), and DOS_THRU (from MAO) should fall between First & Last Claim Dates (Service From Date & Service End Date from Claims respectively):

```sql
ON c.RISK_BID = p.RISK_BID
AND c.CLAIM_PROVIDER_TYPE = p.MA_PROVIDER_TYPE
AND c.FIRST_CLAIM_DATE <= p.DOS_THRU
AND c.LAST_CLAIM_DATE >= p.DOS_THRU
```

**Example — Claim / MAO matching scenarios:**

| Claim Type | Claim ID | Member ID | MAO Encounter Type | Matching Criteria | Pseudo Claim Type | Notes |
|---|---|---|---|---|---|---|
| Actual | 12345 | 124 | — | — | — | — |
| Pseudo | P55114 | 124 | If it matches to an actual claim (based on above criteria) but has additional (not on actual claim) diagnoses | Chart Review-based | DIAG codes that are additional | If this chart review-based pseudo claim comes through in future MAO as a Replacement... (remainder TBD – source table cropped) |

---

## CMS Risk Scoring

Create a curated Member & Claim dataset for RAF scoring.

- Final Notice
- Rate co-efficients by Version
- ICD mappings for each model version
- Valid CPT/HCPCS codes for each year
- Regression Model & definitions

**Curated / output tables:**
- `risk_member`
- `risk_member_diag`
- `risk_member_hcc`
- `risk_member_output`

### Parameters for running Risk Scoring Module

---

## GAP Suspecting

### Methodologies and logic

#### Method 1: On-Rx (Drug Indicators) Methodology

This methodology identifies members who have filled prescriptions for drugs that are strong indicators of specific clinical conditions, but who do not have a corresponding diagnosis for that condition in their medical claims. The goal is to find potential gaps in documentation of care.

**Logic:**
- Use the Method Metadata table for NDC drug codes to Condition Categories (CCs) mapping.
- For each member, compare the list of conditions indicated by their Rx claims to the list of conditions from their medical claims.
- Identify members who have a Rx claim for a condition, but no diagnosis (medical) claim for that condition in the current year.
- Create opportunities for these missing CCs for that member.

**Example:** Member A has a Metformin prescription (indicating diabetes) but no diabetes diagnosis in their 2025 medical claims. Member A is flagged as an opportunity for diabetes documentation.

#### Method 2: NOS/NEC Codes (Not Otherwise Specified (NOS) or Not Elsewhere Classified (NEC)) Methodology

This methodology identifies members with non-specific diagnosis codes (NOS/NEC), or codes that are only three or four digits long, when a more specific code exists and is required for risk scoring. The goal is to find cases where a more specific diagnosis code could be used, potentially improving documentation and risk adjustment.

**Logic:**
- For NOS/NEC codes, the Method Metadata table has mapping to all possible CCs based on code family and distribution of specific codes, including low-specificity codes (3/4-digit Dx codes to the CC of their 5-digit counterparts).
- Identify members who have only non-specific or low-specificity codes for a condition (CC) in medical claims in the current year, and don't have that condition identified in member HCCs for the current year.
- Create opportunities for these missing CCs for that member.

**Example:** Member A has only the C839 dx code in medical claims and no specific Lymphoma (CC021)-related full DX codes like C8391 or C8392, etc. This is flagged as an opportunity to use a more specific code.

#### Additional Suspecting Logic (Persistence-based)

- Identify members with several months of claims from the above claim date.
- If that member doesn't have that condition identified in member HCCs for the current year, create opportunities for these missing CCs for that member — built based on the BCBSA document *(Medicare Advantage Methodologies Association, Nov 2025 working doc)*.
- Use reference table `ref_chronic_condition` for the Chronic CC list.
- Flag members who had a chronic condition in the prior 2 years but not in the current year, and assign a confidence level based on frequency and recency of prior diagnoses, based on the `ref_method_prior_year` reference table.

**Example:** Member A had diabetes diagnoses in 2022 and 2023, but not in 2024. Member A is flagged as an opportunity to create a GAP.

**Prior Year Claims Processing**

- To call an HCC an "Actual Persistent" in 2025 Risk Year for a member, it should be a chronic HCC, and that chronic HCC (or a lower severity HCC) should have been identified in Risk Year 2025. Persistent = based on prior years (last 2 years).
- To call an HCC an "Open Persistent" in 2025 Risk Year for a member, it should be a chronic HCC, and that chronic HCC (or a lower severity HCC) should have been... *(remainder TBD – source cropped)*

**Persistent Risk Examples for Risk Year 2025 (Member Persistent CC):**

| Member BID | Risk Year | Model Version | CC Code | Claim Date | Dx Claim Type | Persistent Status | Suspect Status |
|---|---|---|---|---|---|---|---|
| 1101 | 2024 | V28 | CC058 | 4/1/2024 | Claim – Facility IP | NA | — |
| 1101 | 2024 | V28 | CC058 | 10/1/2024 | Claim – Facility IP | NA | — |
| 1101 | 2025 | V28 | CC058 | — | — | Open Persistent on 01/01/2025 | Open Suspect |
| 1101 | 2025 | V28 | CC059 | 2/1/2025 | Claim – Facility IP | Open NS Persistent | Open Non-Suspect |

### Creating suspected GAPs

Mapping for union of Rx & medical draft gaps (draft gaps from the Medical draft/Telacr method draft gaps table):

| Field | Source |
|---|---|
| METHOD_ID | AS IS from method table |
| CLAIM_CD_TYPE | AS IS from method table |
| CLAIM_CD | AS IS from method table |
| CC_ID | AS IS from method table |
| PERCENT_WEIGHT | AS IS from method table |
| CC_CODE | AS IS from method table |
| CC_DESCRIPTION | AS IS from method table |
| SAS_MODEL_YEAR | AS IS from method table |
| SAS_MODEL_VERSION | AS IS from method table |
| RISK_YEAR | AS IS from method table |
| CLAIM_BID | — |
| MEMBER_BID | — |
| SOURCE_LOAD_MONTH | — |
| CLM_LN_NUM | — |
| CPT_AND_HCPCS_CD | — |
| DIAG_CD | blank |
| Qualify claims | blank / final |
| NDC Code (PROD_SERV_ID_CD) | — |
| *(additional blank column — TBD)* | — |

Get method metadata where Exclusion Type 1 is not 'NA' & Program is 'CMS' — this will give all the rows for exclusions applicable for CMS MA methods.

### Parameters for running GAP Suspecting Module

**Reference Tables**

**1. `ref_home_plan_contract`** — This is a 3-digit unique ID for each Home Plan.

| Column | Description |
|---|---|
| Home Plan ID | — |
| Home Plan Description | Description of the home plan |
| Contract ID | Alpha-numeric ID, typically an N-character-long code (Example — H1234) |
| Contract Start Date | Start date of contract |
| Contract End Date | End date of contract |
| Is_active | Indicator (Y/N) |
| MBI Xwalk required | Indicator (Y/N). Indicates if a crosswalk table needs to be used to connect Member ID with MBI for supplemental file reading |

**2. Claim Type**

| CLM_TP_CD | Claim_Type | Claim_Type_Desc | MA_PROV_Type | MA_PROV_Type_Desc |
|---|---|---|---|---|
| 1 | I | Inpatient – Facility | HI | Hospital Inpatient |
| 2 | O | Outpatient – Facility | HO | Hospital Outpatient |
| 3 | V | Professional | PH | Physician |
| 4 | R | Pharmacy | — | Pharmacy |
| 5 | PSEUDO | Supp MAO-004 | — | — |

**3. Visit type (to HCPCS)**

**4. TOB to POS**

**5. Method Metadata**

### MA Dashboard Pipeline
- Table 1 – Member
- Table 2 – Member HCC

---

## Appendix

### Appendix 1: BID Prefixes

| Data Topic | BID Prefix | Notes |
|---|---|---|
| Member | 11 | Table = member_id (column is MEMBER_BID) |
| Facility | 22 | Table = facility_id (column is FACILITY_BID) |

*(Note: table appears to continue with Professional (33), Pharmacy (44), and other data topics per the ID Tables list above; source page was cropped in the scan.)*

---

*Note: This document is a transcription of a scanned PDF. Several tables and fields in the original were marked "TBD" or were partially illegible/cropped in the source scan (particularly pages 12–13 and the tail end of the Appendix); these are marked accordingly above.*
