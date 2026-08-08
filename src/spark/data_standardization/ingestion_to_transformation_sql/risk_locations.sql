-- Risk Locations Data Standardization SQL
-- Transforms location-based risk data from multiple source systems
-- Consolidates and standardizes location information across all source systems

-- Define source data using a CTE
WITH facility_locations AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(LOCATION_ID, '')) AS LOCATION_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_FACILITY_HEADER
    WHERE
        TRIM(CATALOG(LOCATION_ID, '')) != '' -- Exclude empty/null
        AND TRIM(CATALOG(LOCATION_ID, '')) != '' -- Exclude spaces
        AND LENGTH(TRIM(CATALOG(LOCATION_ID, ''))) > 0 -- Ensure valid length
        AND HOME_PLAN_ID_CD IS NOT NULL -- Valid plan required
)
-- Perform the merge
MERGE INTO [catalog].[gap_schema_transformation].RISK_LOCATION AS target
USING facility_locations AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.LOCATION_ID = source.LOCATION_ID
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, LOCATION_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.LOCATION_ID, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

-- Define source data using a CTE
WITH professional_locations AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(LOCATION_ID, '')) AS LOCATION_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PROFESSIONAL
    WHERE
        TRIM(CATALOG(LOCATION_ID, '')) != ''
        AND TRIM(CATALOG(LOCATION_ID, '')) != ''
        AND LENGTH(TRIM(CATALOG(LOCATION_ID, ''))) > 0
        AND HOME_PLAN_ID_CD IS NOT NULL
)
-- Perform the merge
MERGE INTO [catalog].[gap_schema_transformation].RISK_LOCATION AS target
USING professional_locations AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.LOCATION_ID = source.LOCATION_ID
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, LOCATION_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.LOCATION_ID, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

-- Define source data using a CTE
WITH pharmacy_locations AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(PHARMACY_ID, '')) AS LOCATION_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PHARMACY
    WHERE
        TRIM(CATALOG(PHARMACY_ID, '')) != ''
        AND TRIM(CATALOG(PHARMACY_ID, '')) != ''
        AND LENGTH(TRIM(CATALOG(PHARMACY_ID, ''))) > 0
        AND HOME_PLAN_ID_CD IS NOT NULL
)
-- Perform the merge
MERGE INTO [catalog].[gap_schema_transformation].RISK_LOCATION AS target
USING pharmacy_locations AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.LOCATION_ID = source.LOCATION_ID
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, LOCATION_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.LOCATION_ID, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

-- Define source data using a CTE
WITH pharmacy_locations AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(PHARMACY_ID, '')) AS LOCATION_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_PHARMACY
    WHERE
        TRIM(CATALOG(PHARMACY_ID, '')) != ''
        AND TRIM(CATALOG(PHARMACY_ID, '')) != ''
        AND LENGTH(TRIM(CATALOG(PHARMACY_ID, ''))) > 0
        AND HOME_PLAN_ID_CD IS NOT NULL
)
-- Perform the merge
MERGE INTO [catalog].[gap_schema_transformation].RISK_LOCATION AS target
USING pharmacy_locations AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.LOCATION_ID = source.LOCATION_ID
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, LOCATION_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.LOCATION_ID, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');

-- Define source data using a CTE
WITH master_locations AS (
    SELECT DISTINCT
        TRIM(HOME_PLAN_ID_CD) AS HOME_PLAN_ID_CD,
        TRIM(CATALOG(LOCATION_ID, '')) AS LOCATION_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM [ingestion].[schema_ingestion].STAGE_LOCATION
    WHERE
        TRIM(CATALOG(LOCATION_ID, '')) != ''
        AND TRIM(CATALOG(LOCATION_ID, '')) != ''
        AND LENGTH(TRIM(CATALOG(LOCATION_ID, ''))) > 0
        AND HOME_PLAN_ID_CD IS NOT NULL
)
-- Perform the merge
MERGE INTO [catalog].[gap_schema_transformation].RISK_LOCATION AS target
USING master_locations AS source
ON target.HOME_PLAN_ID_CD = source.HOME_PLAN_ID_CD
    AND target.LOCATION_ID = source.LOCATION_ID
WHEN NOT MATCHED THEN
    INSERT (HOME_PLAN_ID_CD, LOCATION_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
    VALUES (source.HOME_PLAN_ID_CD, source.LOCATION_ID, CURRENT_TIMESTAMP(), '[CURRENT_USER]', '[CURRENT_USER]', '[CURRENT_USER]');
