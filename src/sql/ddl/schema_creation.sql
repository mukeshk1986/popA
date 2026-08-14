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
