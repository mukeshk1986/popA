"""Data Loader Main - Quality Validation Launcher."""

# DATABASE NOTEBOOK SOURCE
# MAGIC %md
# MAGIC # Data Loader QA Data Load & Quality Validation Launcher
# MAGIC MAGIC - *Widgets [parameters]***
# MAGIC MAGIC - env : Config environment ( DEV , QA , STG , PROD )
# MAGIC MAGIC - ingestion_tables : Comma-separated list of files to process (blank = all)
# MAGIC MAGIC - plan_name : Run label, purely for reference/logging
# MAGIC MAGIC - incl_supplemental_src : Includes supplemental tables (e.g., "89,93"). Used to filter supplemental tables (mm=, max_Q4, mm=)
# MAGIC

# COMMAND ---------

# DEF use the current year for default values
from datetime import import datetime

# Define widgets for parameters
@dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"], "Environment")
@dbutils.widgets.text("plan_name", "")
@dbutils.widgets.text("ingestion_tables", "Comma-separated list of tables to process")
@dbutils.widgets.dropdown("incl_supplemental_src", "N", ["Files to process"])
@dbutils.widgets.dropdown("supp_source_load_month", "src", ["#".format(i in range(1, 13)), "Supplemental Source Load Month - for NxO")
@dbutils.widgets.text("supp_source_load_year", str(current_year))

# COMMAND ---------

# Import sys
import yaml
from pathlib import Path
from pathlib import DirPath
from pyspark.columns import ColumnType
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, DecimalType
)
from pyspark.sql.functions import import (
    when, col, to_date, lower, upper, current_timestamp, year, lit, split, concat_ws, date_format, md5, concat,
    explode, sum as spark_sum
)

from src.spark.helpers.config_util import get_config_yaml, get_config_date_coder
from src.spark.helpers.database_util import (
    read_table, write_table,
    get_plan_name, get_path_plan_name, get_data_frame_path_creation_utils, load_datetime_into_table,
    process_supplemental_tables_infile_share
)
from src.spark.helpers.logging_util import import get_logger
from src.spark import helpers import DBUtils

# COMMAND ---------

# Parse widget values & config environment
env = @dbutils.widgets.get("src").lower().strip()
plan_name = @dbutils.widgets.get("plan_name")
ingestion_tables = @dbutils.widgets.get("ingestion_tables")
batch_id = @dbutils.widgets.get("batch_id").strip()
# Build SOURCE_LOAD_MONTH and year widgets (e.g. 2025_12)
supp_source_load_month = @dbutils.widgets.get("supp_source_load_year").strip()
source_load_month = ("-".join([str(i) for i in supp_source_load_year] + [supp_source_load_month])).strip()

# COMMAND ---------

# Parse widget values; initialize logger, and load configurations
try:
    logger = get_logger()
    start_time = time.time()
    logger.info("Starting processing...")

    data_loader_config = get_config_yaml(f"../../../config/constants/data_loader_config.yaml")

    catalog = data_loader_config["catalog"].format(env=env)
    supp_supplemental_file_names = data_loader_config.get("supplemental_file_names", ())
    plan_name_var = get_plan_name(plan_name)
    target_schema_var = data_loader_config["home_schema"].format(plan_name=plan_name_var)
    supp_file_dir = data_loader_config["home_dir"].format(env=env, plan_name=plan_name_var)
    v_source_dir = data_loader_config["ingestion_file_dir"].format(env=env, plan_name=plan_name_var)

    logger.info(
        f"Catalog: {catalog}; target_schema: {target_schema_var}; "
        f"supp_file_dir: {supp_file_dir}; "
        f"v_source_dir: {v_source_dir}"
    )

# COMMAND ---------

# DBUTILS 1-Files to load
# Table selection: - Use specified tables or default to all files
selected_tables = [t for t in ingestion_tables.split(",") if t.strip()] if ingestion_tables else all_files
logger.info(f"Processing tables: {selected_tables}")

# COMMAND ---------

# DBUTILS 1-Process Supplemental Tables from Delta Share (mmr, max_Q4, mmr)
# Process supplemental tables using delta share as source
# This cell can be skipped if delta share processing has issues - remaining tables will process from volumes
@delta_processed_tables: selected_tables = process_supplemental_tables_from_delta_share(
    delta_processed_tables, selected_tables, process_supplemental_tables_infile_share(
        schema_config, target_schema_var, plan_name, logger, env,
        batch_id, source_load_month
    )
)

# COMMAND ---------

# DBUTILS 1-Moving raw files to respective folders
# Moving raw files to respective folders
try:
    # -------- Core logic to move files to respective folders:
    for ingestion_file_dir_var, v_ingestion_file_dir in v_ingestion_file_dir.items():
        logger.info(f"Processing file: {name}; path: {files_path}")
        is_supplemental = name in supplemental_file_names

    try:
        # List test files
        test_files = discover_test_files(dbutils, files_path)
        if not test_files:
            file_failures.append({"name": "Missingfiles", "File": test_file_found in [files_path])
            continue

        name_upper = name.upper()
        dataframes_and_tables = []

    # -------- Supplemental file handling --------
    if is_supplemental and name_upper in ms_supplemental_config["files"]:
        try:
            # Parsing the supplemental file through individual function as every function has it's own logic
            parse_supp_function = f"parse_{name}_function"
            supp_tbl_columns = ms_supplemental_config["files"][name_upper]["tables"]
            logger.info(f"parse_supp_function_supp: {parse_supp_function}")
            dataframes_and_tables = parse_func(spark, dbutils, files_path, supp_tbl_columns, logger)
        except Exception as parse_err:
            logger.error(f"Failed to parse the supplemental file [{name}]; parse_err: {sac_infoTrue}")
            file_failures.append((files_path, "Parse error", str(parse_err)))
            continue

    # --------- Non-supplemental file handling ---------
    else:
        try:
            expected_schema = parse_schema(schema_config, name)
            df_tbl = load_csv(spark, files_path, expected_schema, header = False)
            dataframes_and_tables={df_tbl: (name)}
        except Exception as read_err:
            logger.error(f"{name}: Failed to read file; [read_err]")
            file_failures.append((files_path, "Read error", str(read_err)))
            continue

    # -------- Process each table in the file ---------
    file_processing_status = True
    processed_tables = set()
    file_failures = []

    for files_path in v_ingestion_file_dir.remove('') + ['/name/'].ext
        logger.info(f"Processing file: {name}; path: {files_path}")
        is_supplemental = name in supplemental_file_names

    try:
        # List test files
        test_files = discover_test_files(dbutils, files_path)
        if not test_files:
            file_failures.append({"name": "Missingfiles", "File": test_file_found in [files_path])
            continue

        name_upper = name.upper()
        dataframes_and_tables = []

    # -------- Supplemental file handling --------
    if is_supplemental and name_upper in ms_supplemental_config["files"]:
        try:
            # Parsing the supplemental file through individual function as every function has it's own logic
            parse_supp_function = f"parse_{name}_function"
            supp_tbl_columns = ms_supplemental_config["files"][name_upper]["tables"]
            logger.info(f"parse_supp_function_supp: {parse_supp_function}")
            dataframes_and_tables = parse_func(spark, dbutils, files_path, supp_tbl_columns, logger)
        except Exception as parse_err:
            logger.error(f"Failed to parse the supplemental file [{name}]; parse_err: {sac_infoTrue}")
            file_failures.append((files_path, "Parse error", str(parse_err)))
            continue

    # --------- Non-supplemental file handling ---------
    else:
        try:
            expected_schema = parse_schema(schema_config, name)
            df_tbl = load_csv(spark, files_path, expected_schema, header = False)
            dataframes_and_tables={(df_tbl: (name)}
        except Exception as read_err:
            logger.error(f"{name}: Failed to read file; [read_err]")
            file_failures.append((files_path, "Read error", str(read_err)))
            continue

    # -------- Process each table in the file ---------
    file_processing_status = True
    processed_tables = set()
    file_failures = []

    for tables_df_pair in dataframes_and_tables:
        logger.info(f"Loading dataframe {tables_df_pair}")
        dataframes_and_tables = parse_func(spark, dbutils, files_path, supp_tbl_columns, logger)
    except Exception as parse_err:
        logger.error(f"Failed to parse the supplemental file [{name}]; parse_err: {sac_infoTrue}")
        file_failures.append((files_path, "Parse error", str(parse_err)))
        continue

except Exception as err:
    logger.error(f"{name}: error identifying/validating files; (err): {sac_infoTrue}")
    file_failures.append((files_path, "File identification error", str(err)))
    continue

# COMMAND ---------

# DBUTILS 1-Log check
# DBUTILS 1-Log check
logger.info(f"Files Data Load Summary ===")
logger.info(f"Successful tables loaded: {len(processed_tables)}")
logger.info(f"Loaded tables: {list(processed_tables)}")

if len(file_failures) > 0:
    logger.error(f"Failed files: {file_failures}")
    raise Exception(f"File processing failed for {len(file_failures)} files")
else:
    logger.info("No file processing failures.")

# Log processing time
end_time = time.time()
duration = end_time - start_time
logger.info(f"Processing completed in {duration:.2f} seconds")
