--all schema needed for plan onboarding and reference tables
CREATE SCHEMA IF NOT EXISTS ${catalog}.${gap_schema_curation} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${gap_schema_curation}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_ingestion} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}bronze-us-east-1/${schema_ingestion}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_transformation} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}silver-us-east-1/${schema_transformation}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_curation} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${schema_curation}';

-- Supplemental output isolation: segregated curation and gap-curation schemas for supplemental risk-scoring runs.
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_curation_supp} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${schema_curation_supp}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${gap_schema_curation_supp} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${gap_schema_curation_supp}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_monitoring} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}bronze-us-east-1/${schema_monitoring}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_reference} managed location 's3://bhi-${env_bucket}-datalake-silver-us-east-1/${schema_reference}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${ma_dashboard_reference_schema} managed location 's3://bhi-${env_bucket}-datalake-silver-us-east-1/${ma_dashboard_reference_schema}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${ma_dashboard_schema} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${ma_dashboard_schema}';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_ref_schema} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}silver-us-east-1/${sam_ref_schema}';
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_stage_schema} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}silver-us-east-1/${sam_stage_schema}';
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_work_schema} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${sam_work_schema}';
CREATE SCHEMA IF NOT EXISTS ${catalog}.${sam_result_schema} managed location 's3://bhi-${env_bucket}-datalake-${schema_plan_name}gold-us-east-1/${sam_result_schema}';
