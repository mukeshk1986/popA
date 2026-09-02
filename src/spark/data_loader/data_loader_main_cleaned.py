# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Data Loader - Ingestion Launcher
# MAGIC
# MAGIC **Widgets (parameters)**
# MAGIC - `env`: Config environment (DEV, QA, STG, PROD)
# MAGIC - `plan_name`: Run label; also used to resolve plan-specific schemas and volumes
# MAGIC - `ingestion_tables`: Comma-separated list of files to process (blank = all configured files)
# MAGIC - `incl_supplemental_src`: Comma-separated list of contract IDs used to filter supplemental (MMR / MAO_004) records (blank = all contracts)
# MAGIC - `supp_source_load_month` / `supp_source_load_year`: Source load period used to filter supplemental Delta Share records

# COMMAND ----------

import sys
sys.dont_write_bytecode = True

repo_root = "/Workspace/Repos/DEV/popA"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from datetime import datetime

current_year = datetime.now().year

dbutils.widgets.dropdown("env", "DEV", ["DEV", "QA", "STG", "PROD"], "Environment")
dbutils.widgets.text("plan_name", "", "Plan Name")
dbutils.widgets.text("ingestion_tables", "", "Comma-separated list of tables to process")
dbutils.widgets.text("incl_supplemental_src", "", "Supplemental Contract IDs (blank = all)")
dbutils.widgets.dropdown("supp_source_load_month", "01", [f"{i:02d}" for i in range(1, 13)], "Supplemental Source Load Month")
dbutils.widgets.text("supp_source_load_year", str(current_year), "Supplemental Source Load Year")

# COMMAND ----------

import time
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType, TimestampType
)
from pyspark.sql.functions import col, year, month

from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.databricks_util import get_plan_name, get_path_plan_name, load_csv, write_table
from src.spark.helpers.generic_util import ingestion_folder_check

logger = get_logger()

# COMMAND ----------

# DBTITLE 1,Parse widgets and load configuration
env = dbutils.widgets.get("env").lower().strip()
plan_name = dbutils.widgets.get("plan_name").lower().strip()
ingestion_tables = dbutils.widgets.get("ingestion_tables").strip()
incl_supplemental_src = dbutils.widgets.get("incl_supplemental_src").strip()
supp_source_load_month = dbutils.widgets.get("supp_source_load_month").strip()
supp_source_load_year = dbutils.widgets.get("supp_source_load_year").strip()
source_load_month = f"{supp_source_load_year}-{supp_source_load_month}"
supplemental_contracts = [c.strip() for c in incl_supplemental_src.split(",") if c.strip()]

start_time = time.time()
logger.info(f"Starting data loader for env={env}, plan_name={plan_name}, source_load_month={source_load_month}")

data_loader_config = get_config_yaml("../../../config/constants/data_loader_config.yaml")
expected_schema_config = get_config_yaml("../../../config/constants/expected_schema.yaml")
env_config = get_config_yaml("../../../config/environments/" + env + "/values.yaml")

catalog = env_config["catalog"]
delta_share_catalog = env_config["delta_share_catalog"]
delta_share_schema = data_loader_config["delta_share_config"]["schema"]

v_plan_name = get_plan_name(plan_name)
v_path_plan_name = get_path_plan_name(plan_name)

target_schema = data_loader_config["source_schema"].format(plan_name=v_plan_name)
src_file_dir = data_loader_config["src_file_dir"].format(env=env, plan_name=v_path_plan_name)
ingestion_file_dir = data_loader_config["ingestion_file_dir"].format(env=env, plan_name=v_plan_name)
archive_dir = data_loader_config["archive_dir"].format(env=env, plan_name=v_plan_name)

file_names = data_loader_config["file_names"]
supplemental_file_names = set(data_loader_config["supplemental_file_names"])
delta_table_mappings = data_loader_config["delta_table_mappings"]

logger.info(
    f"catalog: {catalog}; target_schema: {target_schema}; "
    f"src_file_dir: {src_file_dir}; ingestion_file_dir: {ingestion_file_dir}; archive_dir: {archive_dir}"
)

spark.sql(f"USE CATALOG {catalog}")
ingestion_folder_check(dbutils, ingestion_file_dir, logger)
ingestion_folder_check(dbutils, archive_dir, logger)

# COMMAND ----------

# DBTITLE 1,Resolve tables to process
selected_tables = [t.strip().lower() for t in ingestion_tables.split(",") if t.strip()] if ingestion_tables else list(file_names)
non_supplemental_tables = [t for t in selected_tables if t not in supplemental_file_names]
supplemental_tables = [t for t in selected_tables if t in supplemental_file_names]

logger.info(f"Non-supplemental tables selected: {non_supplemental_tables}")
logger.info(f"Supplemental tables selected: {supplemental_tables}")

processed_tables = set()
file_failures = []

# COMMAND ----------

# DBTITLE 1,Build expected schema from config
_SCHEMA_TYPE_NAMESPACE = {
    "StructType": StructType,
    "StructField": StructField,
    "StringType": StringType,
    "LongType": LongType,
    "IntegerType": IntegerType,
    "BooleanType": BooleanType,
    "TimestampType": TimestampType,
}


def build_expected_schema(schema_config: dict, table_name: str) -> StructType:
    """Builds a StructType from the StructField expressions defined in expected_schema.yaml."""
    table_entry = schema_config.get("tables", {}).get(table_name)
    if not table_entry:
        raise KeyError(f"No expected schema configured for table '{table_name}'.")
    field_exprs = table_entry["schema"]["StructType"]
    fields = [eval(expr, {"__builtins__": {}}, _SCHEMA_TYPE_NAMESPACE) for expr in field_exprs]
    return StructType(fields)


def organize_files_by_table(dbutils, src_dir: str, target_dir: str, selected_tables: list, logger):
    """
    Organizes files from source directory (BHI inbox) into table-specific subdirectories.
    Finds matching files for each table, creates subdirectories, and moves files there.

    Args:
        dbutils: Databricks utilities
        src_dir: Source directory (BHI inbox)
        target_dir: Target directory (src_files base)
        selected_tables: List of table names to process
        logger: Logger instance

    Returns:
        dict: Mapping of table_name to file path in organized directory
    """
    organized_files = {}

    try:
        src_files = dbutils.fs.ls(src_dir)
    except Exception as list_err:
        logger.error(f"Failed to list source directory [{src_dir}]: {list_err}")
        return organized_files

    # Sort tables by name length (longest first) to match more specific names before shorter ones
    # This prevents "member" from matching files intended for "member_enrollment"
    sorted_tables = sorted(selected_tables, key=lambda t: len(t), reverse=True)

    for table_name in sorted_tables:
        token = table_name.upper()
        matching_files = [f for f in src_files if f.name.upper().split(".")[0].startswith(token)]

        if not matching_files:
            logger.warning(f"No files found in [{src_dir}] for table [{table_name}]")
            continue

        latest_file = max(matching_files, key=lambda f: f.modificationTime)
        table_subdir = f"{target_dir}/{table_name}"

        try:
            dbutils.fs.mkdirs(table_subdir)
            logger.info(f"Created subdirectory: {table_subdir}")
        except Exception as mkdir_err:
            logger.warning(f"Subdirectory may already exist for {table_name}: {mkdir_err}")

        try:
            target_path = f"{table_subdir}/{latest_file.name}"
            dbutils.fs.mv(latest_file.path, target_path, recurse=True)
            organized_files[table_name] = target_path
            logger.info(f"Organized {table_name}: moved {latest_file.name} to {table_subdir}/")
        except Exception as move_err:
            logger.error(f"Failed to move file for {table_name}: {move_err}")

    return organized_files


def match_inbox_file(all_files, table_name: str):
    """Finds the inbox file for a table name, matching on filename prefix (most recently modified wins)."""
    token = table_name.upper()
    matches = [f for f in all_files if f.name.upper().split(".")[0].startswith(token)]
    if not matches:
        return None
    return max(matches, key=lambda f: f.modificationTime)

# COMMAND ----------

# DBTITLE 1,Organize files from inbox into table-specific subdirectories
logger.info(f"Step 1: Organizing files from source inbox to table-specific subdirectories")
logger.info(f"Source inbox: {src_file_dir}")
logger.info(f"Target organized directory: {ingestion_file_dir}")

organized_files = organize_files_by_table(dbutils, src_file_dir, ingestion_file_dir, non_supplemental_tables, logger)

if not organized_files:
    logger.warning("No files were organized. Proceeding with empty table list.")
    non_supplemental_tables = []
else:
    logger.info(f"Successfully organized {len(organized_files)} files for tables: {list(organized_files.keys())}")

# COMMAND ----------

# DBTITLE 1,Load non-supplemental files from organized subdirectories
for table_name in non_supplemental_tables:
    if table_name not in organized_files:
        logger.error(f"{table_name}: No file found after organization step")
        file_failures.append((table_name, "File organization error", f"File not organized for table"))
        continue

    file_path = organized_files[table_name]
    logger.info(f"Processing table: {table_name}; file: {file_path}")

    try:
        expected_schema = build_expected_schema(expected_schema_config, table_name)
        df_tbl = load_csv(spark, file_path, expected_schema, delimiter="|", header=False)
        stage_table_name = f"stage_{table_name}"
        write_table(df_tbl, spark, target_schema, stage_table_name, mode="overwrite")
        processed_tables.add(stage_table_name)
        logger.info(f"{table_name}: Successfully loaded to {target_schema}.{stage_table_name}")
    except Exception as load_err:
        logger.error(f"{table_name}: Failed to process file; {load_err}")
        file_failures.append((table_name, "Load error", str(load_err)))
        continue

    try:
        table_subdir = f"{ingestion_file_dir}/{table_name}"
        files_in_subdir = dbutils.fs.ls(table_subdir)
        for file_obj in files_in_subdir:
            if file_obj.name == file_path.split("/")[-1]:
                archive_file_path = f"{archive_dir}/{table_name}/{file_obj.name}"
                dbutils.fs.mkdirs(f"{archive_dir}/{table_name}")
                dbutils.fs.mv(file_obj.path, archive_file_path, recurse=True)
                logger.info(f"{table_name}: Archived {file_obj.name}")
    except Exception as archive_err:
        logger.error(f"{table_name}: Loaded successfully but failed to archive file; {archive_err}")

# COMMAND ----------

# DBTITLE 1,Load supplemental tables from Delta Share
for table_name in supplemental_tables:
    mapping = delta_table_mappings.get(table_name)
    if not mapping:
        logger.error(f"{table_name}: No Delta Share mapping configured")
        file_failures.append((table_name, "Missing mapping", "No Delta Share mapping configured"))
        continue

    for sub_table in mapping["tables"]:
        sub_table_name = sub_table["name"]
        source_table = sub_table["source_table"]
        column_mapping = sub_table["column_mapping"]
        full_source_table = f"{delta_share_catalog}.{delta_share_schema}.{source_table}"

        logger.info(f"Processing supplemental table: {sub_table_name}; source: {full_source_table}")
        try:
            df = spark.table(full_source_table)
            select_exprs = [col(src_col).alias(tgt_col) for src_col, tgt_col in column_mapping.items()]
            df = df.select(*select_exprs)

            if supplemental_contracts:
                contract_col = "CONTRACT_ID" if "CONTRACT_ID" in df.columns else (
                    "CONTRACT_NUMBER" if "CONTRACT_NUMBER" in df.columns else None
                )
                if contract_col:
                    df = df.filter(col(contract_col).isin(supplemental_contracts))

            date_col = "REPORT_DATE" if "REPORT_DATE" in df.columns else (
                "RUN_DATE" if "RUN_DATE" in df.columns else None
            )
            if date_col:
                df = df.filter(
                    (year(col(date_col)) == int(supp_source_load_year))
                    & (month(col(date_col)) == int(supp_source_load_month))
                )

            stage_sub_table_name = f"stage_{sub_table_name}"
            write_table(df, spark, target_schema, stage_sub_table_name, mode="overwrite")
            processed_tables.add(stage_sub_table_name)
        except Exception as supp_err:
            logger.error(f"{sub_table_name}: Failed to process supplemental table; {supp_err}")
            file_failures.append((sub_table_name, "Supplemental load error", str(supp_err)))
            continue

# COMMAND ----------

# DBTITLE 1,Load summary
logger.info("Data Load Summary")
logger.info(f"Successful tables loaded: {len(processed_tables)}")
logger.info(f"Loaded tables: {sorted(processed_tables)}")

end_time = time.time()
duration = end_time - start_time
logger.info(f"Processing completed in {duration:.2f} seconds")

if file_failures:
    logger.error(f"Failed tables: {file_failures}")
    raise Exception(f"Table processing failed for {len(file_failures)} table(s)")
else:
    logger.info("No table processing failures.")
