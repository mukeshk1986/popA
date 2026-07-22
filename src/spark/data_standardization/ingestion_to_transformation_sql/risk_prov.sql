-- Risk Provider Data Standardization SQL
-- Transforms provider-based risk data from multiple source systems
-- Consolidates and standardizes provider information across all source systems

WITH source_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(BILL_PROV_ID_CD, '')) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_FACILITY_HEADER
    WHERE WHERE TRIM(CATALOG(BILL_PROV_ID_CD, '')) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV target
USING source_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

WITH source_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(PROVIDER_ID) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PROFESSIONAL
    WHERE WHERE TRIM(PROVIDER_ID) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV target
USING source_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

WITH source_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(ORDERING_PROV_ID, '')) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_LAB
    WHERE WHERE TRIM(CATALOG(ORDERING_PROV_ID, '')) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV target
USING source_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

WITH source_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(EVENT_PROV_ID, '')) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_QUALITY_EVENTS
    WHERE WHERE TRIM(CATALOG(EVENT_PROV_ID, '')) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV target
USING source_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

-- BILL_PROV_ID_CD
WITH source_bill_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(BILL_PROV_ID_CD) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PHARMACY
    WHERE WHERE TRIM(BILL_PROV_ID_CD) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV AS target
USING source_bill_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

WITH source_prescriber_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(PRESCRIBER_PROV_ID, '')) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PHARMACY
    WHERE WHERE TRIM(CATALOG(PRESCRIBER_PROV_ID, '')) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV AS target
USING source_prescriber_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

WITH source_pharmacy_providers AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(PHARMACY_ID, '')) AS PROV_ID_CD,
        CURRENT_TIMESTAMP AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PHARMACY
    WHERE WHERE TRIM(CATALOG(PHARMACY_ID, '')) != '' -- Exclude empty providers
)
MERGE INTO [catalog].[gap_schema_transformation].RISK_PROV AS target
USING source_pharmacy_providers AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.PROV_ID_CD = source.PROV_ID_CD
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, PROV_ID_CD, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.PROV_ID_CD, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');
