# Databricks notebook source
from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.databricks_util import get_plan_name, get_path_plan_name
from src.spark.helpers.generic_util import ingestion_folder_check

logger = get_logger()

dbutils.widgets.dropdown("env", "DEV", ["DEV", "QA", "SIG", "PROD"])
dbutils.widgets.text("plan_name", "")
dbutils.widgets.text("plan_onboarding_reference_schema", "")
dbutils.widgets.text("plan_onboarding_schema_list", "")

# COMMAND ----------

plan_name = dbutils.widgets.get("plan_name").lower()
env = dbutils.widgets.get("env").lower()
env_bucket = "pop-"+env
catalog = "main"
schema_list_raw = dbutils.widgets.get("plan_onboarding_schema_list").strip()
schema_list = [schema.strip().lower() for schema in schema_list_raw.split(",") if schema.strip()]

reference_schema_raw = dbutils.widgets.get("plan_onboarding_reference_schema").strip()
reference_schema = reference_schema_raw.upper()

# reference_schema is plan-name independent and supports canonical aliases.
REFERENCE_SCHEMA_ALIASES = {
    "MA": "ma",
    "ACA": "aca",
    "MA_DASHBOARD": "ma_dashboard"
}

if reference_schema and reference_schema not in REFERENCE_SCHEMA_ALIASES:
    raise ValueError("reference_schema must be one of: ACA, MA, MA_DASHBOARD")

# COMMON schemas: shared across all plans, NEVER prefixed with plan_name.
COMMON_REFERENCE_SCHEMAS = {
    "ma": "ma_reference",
    "aca": "aca_reference",
    "ma_dashboard": "ma_dashboard_reference"
}

reference_schema_key = REFERENCE_SCHEMA_ALIASES.get(reference_schema, "")
ref_schema = COMMON_REFERENCE_SCHEMAS.get(reference_schema_key, "")

# PLAN-SPECIFIC schemas require a plan_name (non-anthec uses no prefix and is
# resolved later); COMMON reference schemas do not.
if schema_list and not plan_name:
    raise ValueError("plan_name widget cannot be empty when schema_list is provided.")

if not schema_list and not reference_schema_key:
    raise ValueError("Either schema_list or reference_schema must be provided.")

# Ensure the common reference schema selection is always part of schema_list processing.
if ref_schema and ref_schema not in schema_list:
    schema_list.append(reference_schema_key)

# COMMAND ----------

# DBTITLE 1,cell 1
v_plan_name = get_plan_name(plan_name)
v_schema_plan_name = get_path_plan_name(plan_name)

# Supplemental output isolation: segregated schemas provisioned for supplemental
# risk-scoring runs (must stay in sync with get_curation_schema / get_gap_curation_schema).
schema_curation_supp = v_plan_name+"curation_supp"
gap_schema_curation_supp = v_plan_name+"gap_curation_supp"
effective_plan_name = v_plan_name if plan_name else "non_anthec"
v_schema_plan_name = get_path_plan_name(effective_plan_name)

# PLAN-SPECIFIC schemas: <plan_prefix>+<base>, where plan_prefix is "" for non_anthec.
# The _supp bases provision segregated supplemental risk-scoring output and must
# stay in sync with get_curation_schema / get_gap_curation_schema.
PLAN_SCHEMA_BASES = [
    "transformation", "curation", "ingestion", "monitoring", "gap_curation",
    "curation_supp", "gap_curation_supp",
    "sam_ref", "sam_stage", "sam_work", "sam_result"
]

plan_schemas = {base: v_plan_name + base for base in PLAN_SCHEMA_BASES}

schema_transformation = plan_schemas["transformation"]
schema_curation = plan_schemas["curation"]
schema_ingestion = plan_schemas["ingestion"]
schema_monitoring = plan_schemas["monitoring"]
gap_schema_curation = plan_schemas["gap_curation"]
schema_curation_supp = plan_schemas["curation_supp"]
gap_schema_curation_supp = plan_schemas["gap_curation_supp"]
sam_ref_schema = plan_schemas["sam_ref"]
sam_stage_schema = plan_schemas["sam_stage"]
sam_work_schema = plan_schemas["sam_work"]
sam_result_schema = plan_schemas["sam_result"]

# COMMAND ----------

volume_name = "ingestion"
scr_files = "src_files"
archive = "archive"
volume_schema = v_plan_name+"ingestion"

if plan_name:
    # Get volume path from SQL for plan-specific ingestion volume.
    volume_row = spark.sql(f"""
    SELECT volume_name, volume_schema, volume_catalog
    FROM system.information_schema.volumes
    WHERE volume_name = '{volume_name}' and volume_catalog = '{catalog}' and volume_schema = '{volume_schema}'
    """).first()

    if not volume_row or not volume_row.storage_location:
        raise ValueError(
            f"Ingestion volume not found for catalog={catalog}, schema={volume_schema}, volume={volume_name}."
        )

    volume_path = volume_row.storage_location
    logger.info(f"Volume Path: {volume_path}")

    # Construct folder path
    src_file_path = f"{volume_path}/{scr_files}"
    archive_file_path = f"{volume_path}/{archive}"

    # Ensure both folders exist
    ingestion_folder_check(dbutils, src_file_path, logger)
    ingestion_folder_check(dbutils, archive_file_path, logger)
else:
    logger.info(
        "Skipping ingestion volume folder setup because plan_name is empty."
    )
