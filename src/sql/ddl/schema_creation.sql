-- =====================================================================================
-- Databricks Community / Free Edition variant (Unity Catalog managed storage, NO S3).
-- Managed schemas: the `MANAGED LOCATION 's3://...'` clause is omitted, so Unity Catalog
-- places each schema in the catalog's managed storage automatically.
-- Enterprise/S3 form was:
--   CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema} managed location
--     's3://bhi-${env_bucket}-datalake-${schema_plan_name}<bronze|silver|gold>-us-east-1/${schema}';
-- NOTE: Free Edition may only allow schemas inside the pre-provisioned managed catalog
--       (e.g. `workspace`); set ${catalog} accordingly. Legacy Community Edition
--       (community.cloud.databricks.com) has no Unity Catalog and cannot run this.
-- =====================================================================================

--all schema needed for plan onboarding and reference tables
CREATE SCHEMA IF NOT EXISTS ${catalog}.${gap_schema_curation};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_ingestion};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_transformation};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_curation};

-- Supplemental output isolation: segregated curation and gap-curation schemas for supplemental risk-scoring runs.
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_curation_supp};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${gap_schema_curation_supp};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_monitoring};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_reference};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${ma_dashboard_reference_schema};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${ma_dashboard_schema};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_ref_schema};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_stage_schema};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_work_schema};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_result_schema};
