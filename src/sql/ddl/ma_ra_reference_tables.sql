-- MA RA Reference Tables DDL

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.time_periods (
    TIME_PERIOD BIGINT,
    BEGIN_DATE DATE,
    END_DATE DATE,
    TIME_PERIOD_TYPE_CD STRING,
    CREATED_DATE TIMESTAMP,
    CREATED_BY STRING,
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY STRING
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_model (
    MODEL_ID SMALLINT NOT NULL,
    MODEL_NAME STRING NOT NULL,
    PROGRAM_MODEL VARCHAR(),
    PROGRAM_VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_variable (
    VARIABLE_ID INT NOT NULL,
    MODEL_ID SMALLINT NOT NULL,
    RAR_MODEL_VERSION_ID SMALLINT,
    VARIABLE_CODE VARCHAR(),
    VARIABLE_DESC VARCHAR(),
    MIN_AGE_LAST SMALLINT,
    MAX_AGE_LAST SMALLINT,
    GENDER VARCHAR(),
    HCC_FLAG VARCHAR(),
    ELIG_FLAG VARCHAR(),
    DEMO_FLAG VARCHAR(),
    AGESENDER_FLAG VARCHAR(),
    INCLUDE_IN_COUNTS VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_variable_detail (
    VARIABLE_ID INT NOT NULL,
    MODEL_ID SMALLINT NOT NULL,
    HCC_ID INT,
    RAR_MODEL_VERSION_ID SMALLINT,
    TYPE_ID TINYINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_method_metadata (
    METHOD_ID SMALLINT NOT NULL,
    PROGRAM_ID INT,
    PROGRAM_MODEL VARCHAR(),
    PROG_TYPE VARCHAR(),
    ICD_VER VARCHAR(),
    CLAIM_CD_TYPE VARCHAR(),
    CLAIM_CD VARCHAR(),
    CLAIM_CD_MODIFIER_TYPE VARCHAR(),
    CLAIM_CD_MODIFIER VARCHAR(),
    OJ_ID INT,
    SUBCO BIGINT,
    GENDER VARCHAR(),
    EXCLUSION_TYPE_1 VARCHAR(),
    EXCLUSION_1 VARCHAR(),
    EXCLUSION_TYPE_2 VARCHAR(),
    EXCLUSION_2 VARCHAR(),
    AGE_FIRST_START SMALLINT,
    AGE_FIRST_END SMALLINT,
    AGE_LAST_STAFF SMALLINT,
    AGE_LAST_END SMALLINT,
    MEDICAL_CONDITION_ID INT,
    QUALIFIED_CLAIM SMALLINT,
    BUNDLED_CHECK SMALLINT,
    EFFECTIVE_DATE DATE,
    EXPIRATION_DATE DATE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg')
PARTITIONED BY (METHOD_ID, PROGRAM);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.version_weightage_risk_year (
    MODEL_PERIOD,
    VERSION STRING,
    RAR_VERSION BIGINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.adjustment_factors (
    VERSION STRING,
    NORMALIZATION_FACTOR DOUBLE,
    CODING_PATTERN_ADJUSTMENT DOUBLE,
    DEMOGRAPHIC_FACTOR DOUBLE,
    TIME_PERIOD INT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.coefficient_scores (
    VERSION STRING,
    MODEL STRING,
    NAME STRING,
    COEFFICIENT DOUBLE,
    SCORE DOUBLE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.community_model_rules (
    MODEL STRING,
    VERSION STRING,
    CONDITION STRING,
    SNCO_MCP STRING,
    SNCO_SUPPLEMENTAL_AMR STRING,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.hierarchy_config (
    VERSION STRING,
    PARENT INT,
    CHILD STRING,
    HIERARCHY_NUM INT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

-- DDL for ${catalog}.${schema_reference}.icd_hcc_mapping
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.icd_hcc_mapping (
    DIAGNOSISCODE STRING,
    DESCRIPTION STRING,
    OM8_HCC_ERRD_MODEL_CATEGORY_V21 INT,
    OM8_HCC_ERRD_MODEL_CATEGORY_V24 INT,
    OM8_HCC_MODEL_CATEGORY_V22 INT,
    OM8_HCC_MODEL_CATEGORY_V24 INT,
    OM8_HCC_MODEL_CATEGORY_V28 INT,
    RARCC_MODEL_CATEGORY_V05 INT,
    RARCC_MODEL_CATEGORY_V03 INT,
    OM8_HCC_ERRD_MODEL_CATEGORY_V21_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    OM8_HCC_ERRD_MODEL_CATEGORY_V24_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    OM8_HCC_MODEL_CATEGORY_V22_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    OM8_HCC_MODEL_CATEGORY_V24_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    OM8_HCC_MODEL_CATEGORY_V28_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    RARCC_MODEL_CATEGORY_V05_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    RARCC_MODEL_CATEGORY_V03_FOR_2025_PAYMENT_YEAR VARCHAR(::),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.interaction_coefficients (
    MODEL STRING,
    VERSION STRING,
    INTERACTION_GROUP_PRIMARY STRING,
    INTERACTION_GROUP_SECONDARY STRING,
    SCORE DOUBLE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.procedure_codes (
    HCPCS_CPT_CODE STRING,
    DESCRIPTION STRING,
    EFFECTIVE_DATE DATE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY STRING,
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY STRING
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_proc_qualifying (
    CPT_AND_HCPCS_CD STRING,
    EFFECTIVE_DATE DATE,
    MA_EFFECTIVE_DATE DATE,
    OM8_ELIGIBILITY_FLAG STRING,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_hcc (
    OJ_ID INT,
    PROGRAM VARCHAR(),
    OJ_CODE VARCHAR(),
    OJ_DESCRIPTION VARCHAR(),
    CATEGORY_ID SMALLINT,
    Notes VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_subco (
    SUBCO BIGINT,
    SUBCO_DESCRIPTION VARCHAR(::),
    CATEGORY_ID SMALLINT,
    OUTWORKSSTART INT,
    OUTWORKSEND INT,
    CHRONIC TINYINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_saa_model (
    RAR_MODEL_VERSION_ID SMALLINT,
    RAR_MODEL_VERSION VARCHAR(::),
    RAR_MODEL_YEAR SMALLINT,
    MODEL_BENEFIT TINYINT,
    PROGRAM_MODEL VARCHAR(),
    VERSION_CODE VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_visit_type_rhcc (
    VISIT_TYPE_ID SMALLINT,
    VISIT_TYPE_DESC VARCHAR(::),
    RHCC_CD VARCHAR(),
    RHCC_CD_DESC VARCHAR(),
    HCPCS_CD VARCHAR(),
    HCPCS_CD_END_DT DATE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_place_of_service_hob (
    TYPE_OF_BILL_CD VARCHAR(),
    PLACE_OF_SERVICE VARCHAR(),
    POS_DESC VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_chronic_condition (
    RAR_MODEL_YEAR INT,
    RAR_MODEL_VERSION VARCHAR(),
    MODEL_VERSION VARCHAR(),
    CC_ID INT,
    CC_CODE VARCHAR(),
    CC_DESCRIPTION VARCHAR(),
    CHRONIC TINYINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_bid_mapping (
    PREFIX VARCHAR(),
    STARTING_SERIES VARCHAR(),
    CLAIM_TYPE VARCHAR(),
    CLAIM_SOURCE VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_mor_health_event_confidence (
    CHRONIC TINYINT,
    CURRENT_YEAR TINYINT,
    PRIOR_YEAR TINYINT,
    RECORD_YEAR_PRIOR INT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_home_plan (
    HOME_PLAN_ID_CD VARCHAR() NOT NULL,
    HOME_PLAN_DESCRIPTION VARCHAR(::),
    HOLDING_COMPANY_CODE VARCHAR(),
    HOLDING_COMPANY_DESCRIPTION VARCHAR(::),
    IS_ACTIVE_PLAN BOOLEAN NOT NULL,
    IS_SUPPLEMENTAL_PLAN BOOLEAN NOT NULL,
    EFFECTIVE_DATE DATE,
    EXPIRATION_DATE DATE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_home_plan_contract (
    HOME_PLAN_ID_CD VARCHAR() NOT NULL,
    HOME_PLAN_DESCRIPTION VARCHAR(::),
    CONTRACT_ID VARCHAR() NOT NULL,
    CONTRACT_DESCRIPTION VARCHAR(::),
    CONTRACT_START_DATE DATE,
    CONTRACT_END_DATE DATE,
    IS_ACTIVE BOOLEAN NOT NULL,
    HSI_VMARK_REQUIRED BOOLEAN NOT NULL,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_claim_type (
    CLAIM_TP_CD VARCHAR(),
    CLAIM_TYPE_CHG() NOT NULL,
    CLAIM_TYPE_DESC VARCHAR(::),
    MA_PROC_TYPE VARCHAR(::),
    CLAIM_CD BIGINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_method_frequency (
    METHOD_ID SMALLINT NOT NULL,
    PROGRAM VARCHAR() NOT NULL,
    PROGRAM_MODEL VARCHAR(),
    CLAIM_CD_TYPE VARCHAR(),
    CLAIM_CD VARCHAR(),
    CLAIM_CD_MODIFIER_TYPE VARCHAR() ,
    CLAIM_CD_MODIFIER VARCHAR(),
    SUBCO BIGINT,
    FREQUENCY INT,
    PERCENT_WEIGHT DECIMAL(, ) ,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_method_metadata_codegroups (
    RISK_CD INT,
    CLAIM_CD_TYPE VARCHAR() NOT NULL,
    CLAIM_CD VARCHAR() NOT NULL,
    CODE VARCHAR() NOT NULL,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg')
PARTITIONED BY (ICD_VER);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_csf (
    OBR_INDICATOR VARCHAR() NOT NULL,
    OBR_DESCRIPTION VARCHAR(),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_metal (
    METAL_ID SMALLINT NOT NULL,
    METAL VARCHAR(),
    METAL_DESCRIPTION VARCHAR() ,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_method_prior_year (
    LAST_DK_COVOYANCE INT NOT NULL,
    DK_FREQUENCY_MIN INT NOT NULL,
    DK_FREQUENCY_MAX INT,
    PERCENT_WEIGHT DECIMAL(, ) NOT NULL,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg')
PARTITIONED BY (LAST_DK_COVOYANCE);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_risk_type_detail (
    RAR_MODEL_VERSION VARCHAR(),
    RISK_TYPE_ID VARCHAR(),
    RISK_TYPE_DESC VARCHAR(),
    RISK_TYPE_DETAIL_ID VARCHAR(),
    RISK_TYPE_DETAIL_DESC VARCHAR(),
    NOTES VARCHAR(::),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_method (
    METHOD_ID SMALLINT NOT NULL,
    METHOD_DESCRIPTION VARCHAR(::),
    PROGRAM VARCHAR(),
    NOTES VARCHAR(::),
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_interaction_sources (
    RISK_MODEL_TYPE STRING,
    RISK_MODEL_VERSION STRING,
    RAR_MODEL_SEGMENT_STRING,
    MODEL_SEGMENT_STRING,
    RISK_MODEL_SEGMENT_TYPE STRING,
    INTERACTION_TYPE STRING,
    INTERACTION_NAME STRING,
    INTERACTION_DESC STRING,
    INTERACTION_MOC1 STRING,
    INTERACTION_MOC2 STRING,
    SCORE DOUBLE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY STRING,
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY STRING
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg',
    'tag.usage_status' = 'not_used_in_code',
    'tag.purpose' = 'reference_future_use');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_interaction_scores (
    RISK_MODEL_TYPE STRING,
    RISK_MODEL_VERSION STRING,
    RAR_MODEL_SEGMENT STRING,
    RISK_MODEL_SEGMENT STRING,
    INTERACTION_TYPE STRING,
    INTERACTION_NAME STRING,
    INTERACTION_DESC STRING,
    INTERACTION_MOC1 STRING,
    INTERACTION_MOC2 STRING,
    SCORE DOUBLE,
    CREATED_DATE TIMESTAMP,
    CREATED_BY STRING,
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY STRING
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_reference}.ref_diag_chronic_condition (
    RAR_MODEL_YEAR INT,
    RAR_MODEL_VERSION VARCHAR(),
    OJ_CODE VARCHAR(),
    OJ_DESCRIPTION VARCHAR(::),
    DIAG_CODE VARCHAR(),
    DIAG_DESCRIPTION VARCHAR(::),
    EFFECTIVE_DATE DATE,
    EXPIRATION DATE,
    CHRONIC TINYINT,
    CREATED_DATE TIMESTAMP,
    CREATED_BY VARCHAR(::),
    UPDATED_DATE TIMESTAMP,
    UPDATED_BY VARCHAR(::)
)
USING DELTA
WITH SERDEPROPERTIES(
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompat2' = 'true',
    'delta.universalFormat.enabledFormat' = 'iceberg');
