-- =====================================================================================
-- Databricks Community / Free Edition variant (Unity Catalog managed storage, NO S3).
-- Managed volume: no EXTERNAL keyword and no LOCATION clause, so Unity Catalog stores
-- the files in managed storage. Files are still accessed at:
--   /Volumes/${catalog}/${schema_ingestion}/ingestion/
-- Enterprise/S3 form was:
--   CREATE EXTERNAL VOLUME IF NOT EXISTS ${catalog}.${schema_ingestion}.ingestion
--     LOCATION 's3://bhi-${env_bucket}-datalake-${schema_plan_name}bronze-us-east-1/${schema_ingestion}/ingestion';
-- =====================================================================================

-- Volume creation for rest all plans(non anthem & planwise)------------------------
CREATE VOLUME IF NOT EXISTS ${catalog}.${schema_ingestion}.ingestion;
