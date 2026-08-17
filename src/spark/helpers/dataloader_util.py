"""Dataloader Utilities - File discovery, parsing, and Delta/volume loading helpers."""

import re
import uuid
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import PurePosixPath
from urllib.parse import urlparse

import boto3
from databricks.sdk.runtime import spark
from pyspark.sql import Window, DataFrame, functions as F
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, regexp_replace,
    substring, to_date, trim, when, row_number
)
from pyspark.sql.types import DateType, StringType, StructField, StructType  # noqa: F401 - used by eval() in parse_schema()

from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger

logger = get_logger("Dataloader_util", "Logger initialized")
schema_config = get_config_yaml("/Workspace/Repos/DEV/popA/config/constants/expected_schema.yaml")


def trim_string_columns(df):
    """
    TRIM leading/trailing whitespace on every STRING column of the DataFrame.

    Source files were found to carry stray spaces on some join/merge keys
    (e.g. CONTRACT_ID, ICN, MEMB_ID_CD), which silently break the downstream
    transformation MERGE joins. Rather than maintaining a hand-curated column
    list (which drifts whenever a column is added/renamed), this cleans all
    string columns generically from the DataFrame's own schema. Non-string
    columns (timestamps, numerics) are left untouched.
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    return df


def get_dict_subset(data_dict: dict, keys=None) -> dict:
    """
    Return a subset of a dict for the specified keys.
    If keys is None, return the original dict.
    If keys is a string, convert to a list.
    """
    try:
        # If keys is None, return the original dict
        if keys is None:
            return data_dict
        # If keys is a string, convert to a list
        if isinstance(keys, str):
            keys = [keys]
        # Uses dict comprehension to create a new dict with the specified keys
        return {k: data_dict[k] for k in keys if k in data_dict}
    except KeyError as e:
        raise Exception(f"Key not found in dict: {e}")
    except Exception as e:
        raise Exception(f"Failed to get dict subset: {e}")


def load_rules_from_file(spark, rules_path: str) -> dict:
    """
    Load data quality rules from a YAML file.
    Returns a dict mapping table names to their expectations.
    """
    try:
        # Open the YAML file and load its contents
        with open(rules_path, "r") as f:
            # Load the YAML file into a Python object
            master = yaml.safe_load(f)
        # Use dict comprehension to create a dict mapping table names to their expectations
        return {
            entry["table"]: entry.get("expectations", [])
            for entry in master.get("tables", [])
            if "table" in entry
        }
    except FileNotFoundError as e:
        raise Exception(f"Rules file not found: {rules_path}") from e
    except yaml.YAMLError as e:
        raise Exception(f"Failed to parse YAML rules file: {e}") from e
    except Exception as e:
        raise Exception(f"Failed to load rules from file: {e}") from e


def add_audit_columns(spark, df, account_name, logger):
    try:
        # Dynamically add audit columns to the DataFrame
        result = (
            df.withColumn("CREATED_DATE", current_timestamp())
              .withColumn("CREATED_BY", lit(account_name))
              .withColumn("UPDATED_DATE", lit(None).cast(DateType()))
              .withColumn("UPDATED_BY", lit(None).cast(StringType()))
              .withColumn("VALIDATED_TIMESTAMP", current_timestamp())
        )
        return result
    except Exception as e:
        if logger:
            logger.error(f"Failed to add audit columns: {e}", exc_info=True)
        raise Exception(f"Failed to add audit columns: {e}")


def load_config() -> dict:
    """Loads the default data loader configuration."""
    return get_config_yaml("../../../config/constants/data_loader_config.yaml")


def _execute_parallel_s3_transfers(dbutils, base_path, transfer_tasks, logger, max_workers=8) -> set:
    """
    Moves (source_path, target_path, filename) tuples in parallel via dbutils.fs,
    using a thread pool so many small file moves complete faster than issuing
    them one at a time.

    Returns:
        set: Filenames that were moved successfully.
    """
    moved_files = set()

    def _move_one(task):
        source_path, target_path, filename = task
        target_dir = target_path.rsplit("/", 1)[0]
        try:
            dbutils.fs.mkdirs(target_dir)
            dbutils.fs.mv(source_path, target_path)
            return filename, True, None
        except Exception as move_err:
            return filename, False, str(move_err)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_move_one, task): task for task in transfer_tasks}
        for future in as_completed(futures):
            filename, success, error = future.result()
            if success:
                logger.info(f"Moved {filename} to target location")
                moved_files.add(filename)
            else:
                logger.error(f"Failed to move {filename} from {base_path}: {error}")

    return moved_files


def archive_all_files(dbutils, src_dir: str, archive_dir: str, logger) -> list:
    """
    Archives all files under src_dir into archive_dir using date extracted from filename.
    Uses parallel boto3 transfers for performance.
    Supports:
      - member_09212025.txt   -> MMDDYYYY
      - P.R12345.PTDMODD.D250901.T143028 -> DYYMMDD (dynamic day)
    Returns:
        List of archived file paths.
    """
    src_dir = src_dir.rstrip('/')
    archive_dir = archive_dir.rstrip('/')

    # Precompile regex for performance
    re_std = re.compile(r'(\d{8})(?=\.\w+$)')      # MMDDYYYY
    re_special = re.compile(r'D(\d{6,8})')          # DYYMMDD

    # List all files
    try:
        all_files = [f for f in dbutils.fs.ls(src_dir) if not f.isDir()]
    except Exception as e:
        logger.warning(f"Error listing files in {src_dir}: {e}")
        return []

    if not all_files:
        logger.info(f"No files to archive in {src_dir}")
        return []

    # Build transfer tasks with date-based destination paths
    transfer_tasks = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for f in all_files:
        file_path = f.path
        filename = file_path.split("/")[-1]

        # Extract date from filename
        match_std = re_std.search(filename)
        match_special = re_special.search(filename)

        if match_std:
            mm, dd, yyyy = match_std.group(1)[:2], match_std.group(1)[2:4], match_std.group(1)[4:]
        elif match_special:
            dstr = match_special.group(1)
            yy, mm, dd = dstr[:2], dstr[2:4], dstr[4:6]
            yyyy = f"20{yy}"
        else:
            now = datetime.now()
            yyyy, mm, dd = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

        # Build destination path with timestamp in filename
        dest_dir = f"{archive_dir}/{yyyy}/{mm}/{dd}"
        base, ext = (filename.rsplit(".", 1) + [""])[:2]
        new_filename = f"{base}_{timestamp}.{ext}" if ext else f"{base}_{timestamp}"
        dest_file = f"{dest_dir}/{new_filename}"

        transfer_tasks.append((file_path, dest_file, filename))

    logger.info(f"Archiving {len(transfer_tasks)} files from {src_dir}")

    # Execute parallel transfers
    moved_files = _execute_parallel_s3_transfers(dbutils, src_dir, transfer_tasks, logger)

    # Return full destination paths for archived files
    archived = [t[1] for t in transfer_tasks if t[2] in moved_files]
    return archived


def apply_date_conversions(
    spark, df: DataFrame, table_name: str = None,
    patterns: list = ["yyyy-MM-dd", "MM/dd/yyyy", "yyyyMMdd", "yyyy-MMM-dd", "M/d/yyyy"]
) -> DataFrame:
    """
    Applies date conversions to columns ending with '_DT' in the given DataFrame.
    """
    try:
        # Identify columns requiring date conversion
        date_cols = [c for c in df.columns if c.upper().endswith('_DT')]

        # Apply date conversions to each identified column
        for c in date_cols:
            parsed = to_date(col(c), patterns[0])
            for patt in patterns[1:]:
                # Attempt conversion with subsequent patterns if initial conversion fails
                parsed = when(parsed.isNotNull(), parsed).otherwise(to_date(col(c), patt))
            df = df.withColumn(c, parsed)
        return df
    except Exception as e:
        raise Exception(f"Failed to apply date conversions: {e}")


def map_df_to_schema(spark, df, table_schema):
    try:
        # Build a new select statement that matches target schema by column name
        select_expr = []
        df_cols = set(df.columns)
        for field in table_schema.fields:
            if field.name in df_cols:
                select_expr.append(col(field.name).cast(field.dataType).alias(field.name))
            else:
                # If column is missing, add as null of the right type
                select_expr.append(lit(None).cast(field.dataType).alias(field.name))
        return df.select(*select_expr)
    except Exception as e:
        raise Exception(f"Failed to map DataFrame to schema: {e}")


def parse_fixed_width(spark, df, field_positions):
    """
    Parse a fixed-width formatted DataFrame into separate columns.
    This function replaces '*' with a single space to maintain a constant record length,
    extracts fields based on their positions, trims the extracted fields, and converts
    date fields to DateType.

    Args:
        df (DataFrame): The input DataFrame containing fixed-width formatted data.
        field_positions (list): A list of dictionaries containing field information.
            Each dictionary should have the following keys:
            - name (str): The name of the field.
            - start (int): The starting position of the field.
            - end (int): The ending position of the field.
            - type (str, optional): The type of the field (e.g., DATE).

    Returns:
        DataFrame: The parsed DataFrame with extracted fields.
    """
    try:
        # Replace '*' with a single space to maintain a constant record length
        df = df.withColumn("fw_line", regexp_replace("value", r"\*", " "))

        # Iterate over each field position and extract the corresponding field
        for field in field_positions:
            # Extract field information
            name = field["name"]
            start = int(field["start"]) + 1
            length = int(field["end"]) - int(field["start"])

            # Extract the field from the fixed-width line
            expr = trim(substring("fw_line", start, length))

            # Add the extracted field to the DataFrame
            df = df.withColumn(name, expr)

        # Drop temporary columns and keep all other columns
        keep_cols = [c for c in df.columns if c not in ("value", "fw_line")]
        return df.select(*keep_cols)
    except Exception as e:
        raise Exception(f"Failed to parse fixed-width file: {e}")


def add_source_load_month_df(spark, df: DataFrame) -> DataFrame:
    """
    Enriches input DataFrame by extracting SOURCE_LOAD_MONTH from filenames.
    Supports filenames like:
      1. member_09212025.txt                  (MMDDYYYY)
      2. P.R12345.PTDMODD.D25091501.T143028   (D + YYMMDDxx)

    Args:
        df: Input DataFrame to enrich
    Returns:
        DataFrame: Enriched DataFrame with SOURCE_LOAD_MONTH column
    Raises:
        Exception: If enrichment fails
    """
    try:
        # Step 1: Add file path column
        df = df.withColumn("full_path", input_file_name())

        # Step 2: Define regex patterns for date extraction
        standard_date_regex = r"(\d{8})(?=\.txt$)"   # Captures 8 digits before .txt
        special_date_regex = r"D(\d{6,8})"            # Captures 6-8 digits after 'D'

        # Step 3: Extract date strings using regex patterns
        df = (df
              .withColumn("STANDARD_DATE_STR", F.regexp_extract("full_path", standard_date_regex, 1))
              .withColumn("SPECIAL_DATE_STR", F.regexp_extract("full_path", special_date_regex, 1))
              )

        # Step 4: Parse standard date format (MMDDYYYY -> YYYY_MM)
        df = df.withColumn(
            "STANDARD_SOURCE_MONTH",
            F.when(F.col("STANDARD_DATE_STR") != "",
                   F.date_format(F.to_date("STANDARD_DATE_STR", "MMddyyyy"), "yyyy_MM")
                   )
        )

        # Step 5: Parse special date format (YYMMDD -> YYYY_MM)
        df = df.withColumn(
            "SPECIAL_SOURCE_MONTH",
            F.when(
                F.col("SPECIAL_DATE_STR") != "",
                F.concat(
                    F.lit("20"),                                    # Add century prefix '20'
                    F.substring(F.col("SPECIAL_DATE_STR"), 1, 2),    # Extract year 'YY' (positions 1-2)
                    F.lit("_"),                                     # Add separator
                    F.substring(F.col("SPECIAL_DATE_STR"), 3, 2)     # Extract month 'MM' (positions 3-4)
                )
            ).otherwise(F.lit(None))
        )

        # Step 6: Create final SOURCE_LOAD_MONTH column with fallback logic
        df = df.withColumn(
            "SOURCE_LOAD_MONTH",
            F.coalesce("STANDARD_SOURCE_MONTH", "SPECIAL_SOURCE_MONTH")
        )

        # Step 7: Clean up temporary columns
        columns_to_drop = [
            "full_path",
            "STANDARD_DATE_STR",
            "SPECIAL_DATE_STR",
            "STANDARD_SOURCE_MONTH",
            "SPECIAL_SOURCE_MONTH"
        ]
        return df.drop(*columns_to_drop)

    except Exception as e:
        raise Exception(f"Failed to enrich file DataFrame: {e}")


def parse_mao_004_files(spark, dbutils, files_path, mao_004_schema, logger):
    """
    Parse MAO-004 files into separate DataFrames for header, detail, and trailer lines.

    Args:
        spark (SparkSession): The Spark session.
        files_path (str): The path to the MAO-004 files.
        mao_004_schema (dict): The schema for the MAO-004 files.

    Returns:
        list: A list of tuples containing the parsed DataFrames and their corresponding types.

    Raises:
        Exception: If an error occurs during parsing.
    """
    try:
        file_id = str(uuid.uuid4())
        logger.info(f"Processing file_id {file_id}")
        # Read MAO-004 files into a DataFrame, preserving the file path
        raw_df = spark.read.text(files_path).withColumn("full_path", input_file_name()).withColumn("FID", lit(file_id))

        # Filter lines based on their type (header, detail, trailer)
        header_lines = raw_df.filter(col("value").substr(1, 1) == "0")
        detail_lines = raw_df.filter(col("value").substr(1, 1) == "1")
        trailer_lines = raw_df.filter(col("value").substr(1, 1) == "9")

        # Parse fixed-width lines for each type and return as a list of tuples
        return [
            (parse_fixed_width(spark, header_lines, mao_004_schema["medicare_mao_header"]["columns"]), "medicare_mao_header"),
            (parse_fixed_width(spark, detail_lines, mao_004_schema["medicare_mao"]["columns"]), "medicare_mao"),
            (parse_fixed_width(spark, trailer_lines, mao_004_schema["medicare_mao_trailer"]["columns"]), "medicare_mao_trailer"),
        ]
    except Exception as e:
        raise Exception(f"Failed to parse MAO-004 files: {e}")


def parse_mmr_files(spark, dbutils, files_path, mmr_schema, logger):
    """
    Parse MMR files into a DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        files_path (str): The path to the MMR files.
        mmr_schema (dict): The schema for the MMR files.

    Returns:
        list: A list of tuples containing the parsed DataFrame and its type.

    Raises:
        Exception: If an error occurs during parsing.
    """
    try:
        # Read the MMR files into a DataFrame, preserving the file path for future reference
        mmr_raw_df = spark.read.text(files_path).withColumn("full_path", input_file_name())

        # Parse the fixed-width lines based on the provided schema
        mmr_df = parse_fixed_width(spark, mmr_raw_df, mmr_schema["mmr"]["columns"])

        # Return the parsed DataFrame along with its type
        return [(mmr_df, "mmr")]
    except Exception as e:
        raise Exception(f"Failed to parse MMR files: {e}")


def parse_mor_files(spark, dbutils, files_path, mor_schema, logger):
    """
    Parse MOR files (Part C & Part D) dynamically based on record type.

    Args:
        spark (SparkSession): Active Spark session
        files_path (str): Path to MOR files
        mor_schema (dict): Schema layout loaded from YAML
    Returns:
        list: List of tuples (DataFrame, table_name)
    """
    results = []
    try:
        files_path = files_path.replace("*.txt", "")
        logger.info(f"Processing MOR files in {files_path}")
        files = [f.path for f in dbutils.fs.ls(files_path) if f.name.endswith(".txt")]
        if not files:
            logger.info(f"No MOR files found in {files_path}")
            return []

        for file_path in files:
            file_name = file_path.split("/")[-1]
            logger.info(f"Processing file: {file_name}")
            # Determine part type (PartC / PartD)
            if "HCCMODD" in file_name.upper():
                part_type = "partC"
            elif "PTDMODD" in file_name.upper():
                part_type = "partD"
            else:
                logger.info(f"Unknown part type for {file_name}. Skipping.")
                continue

            try:
                # make more descriptive file id logic
                file_id = str(uuid.uuid4())
                # Read file as text
                df_raw = spark.read.text(file_path).withColumn("file_name", F.input_file_name()).withColumn("FID", F.lit(file_id))
                df_raw = df_raw.withColumn("record_type", F.col("value").substr(1, 1))
                # Unique record types
                record_types = [r["record_type"] for r in df_raw.select("record_type").distinct().collect()]
                # Iterate over each record type
                for record_type in record_types:
                    parsed = False
                    for table_name, table_info in mor_schema[part_type].items():
                        if str(table_info["identifier"]) == str(record_type):
                            logger.info(f"Parsing record type {record_type} -> {table_name}")
                            logger.info(f"Record Type Identifier: {table_info['identifier']}, Record Type: {record_type}")
                            logger.info(f"Columns: {table_info['columns']}")
                            # Filter relevant lines
                            filtered = df_raw.filter(F.col("record_type") == record_type)
                            # Parse using fixed width layout
                            parsed_df = parse_fixed_width(spark, filtered, table_info["columns"])
                            # Add metadata
                            parsed_df = parsed_df.withColumn("source_file", F.col("file_name"))
                            parsed_df = parsed_df.withColumn("record_type", F.lit(record_type))
                            # Append to result list
                            results.append((parsed_df, table_name))
                            parsed = True
                            break
                    if not parsed:
                        logger.info(f"No schema found for record type {record_type} in {part_type}. Skipping...")
            except Exception as e:
                logger.info(f"Error parsing {file_name}: {e}")
                continue

    except Exception as e:
        logger.info(f"Overall MOR parsing failed: {e}")

    return results


def resolve_ingestion_paths(config: dict, env: str, plan_name_var: str, base_schema: str, target_schema_var: str) -> tuple:
    """
    Derives the ingestion volume path and archive volume path from config,
    rewriting the schema segment when a monthly schema is in use.

    Returns:
        (ingestion_file_dir, archive_dir) with the correct monthly schema segment.
    """
    ingestion_file_dir = config["ingestion_file_dir"].format(env=env, plan_name=plan_name_var)
    archive_dir = config["archive_dir"].format(env=env, plan_name=plan_name_var)
    if target_schema_var != base_schema:
        ingestion_file_dir = ingestion_file_dir.replace(f"/{base_schema}/", f"/{target_schema_var}/", 1)
        archive_dir = archive_dir.replace(f"/{base_schema}/", f"/{target_schema_var}/", 1)
    return ingestion_file_dir, archive_dir


def discover_text_files(dbutils, text_path):
    """
    Discover text files in a given directory.

    Args:
        dbutils (DBUtils): The DBUtils object.
        text_path (str): The path to the directory.

    Returns:
        list: A list of text file paths.

    Raises:
        Exception: If an error occurs during discovery.
    """
    try:
        # Get the parent directory of the provided text path
        parent_dir = str(PurePosixPath(text_path).parent)

        # List all files in the parent directory using DBUtils
        all_files = dbutils.fs.ls(parent_dir)

        # Filter the list of files to only include text files
        text_files = [f.path for f in all_files if f.path.lower().endswith('.txt')]

        # Return the list of text file paths
        return text_files

    except Exception as e:
        raise Exception(f"Failed to discover text files: {e}")


def write_table(df, spark, schema, table_name, mode="append"):
    """
    Writes a DataFrame to an existing Delta table using insertInto().
    Assumes the table exists and matches the DataFrame schema.
    """
    table_full = f"{schema}.{table_name}"
    try:
        df.write.format("delta").mode(mode).insertInto(table_full)
        logger.info(f"Data successfully written to table {table_full} (mode={mode}).")
    except Exception as e:
        logger.error(f"Error writing data to table {table_full}: {e}")
        raise Exception(f"Error writing data to table {table_full}: {e}")


def move_raw_file_dbutils(dbutils, source_volume_path, target_volume_path, logger, filenames, config=None):
    """
    Moves files from source_volume_path to target_volume_path using parallel boto3 transfers.
    Much faster than sequential dbutils.fs.cp for multiple files.

    Args:
        dbutils: Databricks dbutils
        source_volume_path: Source volume path
        target_volume_path: Target volume path
        logger: Logger instance
        filenames: List of file types to move
        config: Optional config dict
    """
    logger.info(f"Moving files from {source_volume_path} to {target_volume_path}")
    if config is None:
        config = load_config()

    source_volume_path = source_volume_path.rstrip('/')
    target_volume_path = target_volume_path.rstrip('/')

    # Step 1: Resolve standardized names for provided filenames
    renamed_filenames = []
    if filenames:
        for fname in filenames:
            if fname in config['file_types']:
                renamed_filenames.append(config['file_types'][fname]['standardized_name'])
            else:
                renamed_filenames.append(fname)

    # Step 2: Capture directory listing once and reuse
    try:
        all_files_info = [f for f in dbutils.fs.ls(source_volume_path) if not f.isDir()]
        all_filenames = [f.name for f in all_files_info]
    except Exception as e:
        logger.warning(f"Error listing files in {source_volume_path}: {e}")
        return []

    files_to_move = []
    if renamed_filenames:
        for fname in renamed_filenames:
            special_pattern = None
            # Check if file matches any special file config
            for key, special_cfg in config['special_files'].items():
                if fname == key:
                    special_pattern = special_cfg['pattern']
                    break
            if special_pattern:
                # Match from cached listing
                matching_files = [fn for fn in all_filenames
                                   if any(pat in fn.upper() for pat in special_pattern)]
            else:
                # Precompile regex once per fname
                compiled_pattern = re.compile(rf"{fname}_\d{{8}}\.txt$", re.IGNORECASE)
                matching_files = [fn for fn in all_filenames if compiled_pattern.match(fn)]
            files_to_move.extend(matching_files)
    else:
        # No specific filenames provided: include all .txt and special files
        for filename in all_filenames:
            filename_upper = filename.upper()
            filename_lower = filename.lower()
            if any(
                any(pat in filename_upper for pat in special_cfg['pattern'])
                for special_cfg in config['special_files'].values()
            ) or filename_lower.endswith(".txt"):
                files_to_move.append(filename)

    # Step 3: Exit early if nothing found
    if not files_to_move:
        logger.warning(f"No files found in {source_volume_path}")
        return []

    logger.info(f"Files to move: {files_to_move}")

    # Step 4: Build list of (source, target) pairs with folder routing
    transfer_tasks = []
    for f in files_to_move:
        # Extract base name
        if "_" in f and re.match(r".*_\d{8}\.txt$", f, re.IGNORECASE):
            base_name = "_".join(f.split("_")[:-1]).upper()
        else:
            base_name = f.split(".")[0].upper()

        # Determine target folder
        folder_name = None
        for file_type, cfg in config['file_types'].items():
            if cfg['standardized_name'] == base_name:
                folder_name = file_type
                break

        # Special file folder
        if not folder_name:
            for key, special_cfg in config['special_files'].items():
                if any(pat in f.upper() for pat in special_cfg['pattern']):
                    folder_name = key
                    break

        if not folder_name:
            folder_name = base_name.lower()

        source_file = f"{source_volume_path}/{f}"
        target_file = f"{target_volume_path}/{folder_name}/{f}"
        transfer_tasks.append((source_file, target_file, f))

    # Step 5: Execute parallel S3 transfers
    return _execute_parallel_s3_transfers(dbutils, source_volume_path, transfer_tasks, logger)


def parse_schema(schema_config, name):
    """
    Parse the schema string from the schema configuration.

    Args:
        schema_config (dict): The schema configuration dictionary.
        name (str): The name of the table.

    Returns:
        StructType: The parsed schema.

    Raises:
        Exception: If an error occurs during schema parsing.
    """
    schema_str = schema_config["tables"][name]["schema"]
    try:
        expected_schema = eval(schema_str)
        return expected_schema
    except Exception as schema_err:
        logger.error(f"[{name}] Failed to parse schema: {schema_err}", exc_info=True)
        raise Exception(f"Failed to parse schema: {schema_err}")


def log_and_raise_failures(file_failures, dq_failures, dq_failed, logger):
    """
    Logs pipeline failures and raises an exception if needed.
    """
    logger.error("--- PIPELINE FAILURE SUMMARY ---")

    # Log file failures
    for name_or_file, stage, reason in file_failures:
        logger.error(f"[{stage}] {name_or_file} | Reason: {reason}")

    # Log DQ failures
    for tbl, ts_or_reason in dq_failures:
        logger.error(f"[DQFailure] {tbl} | Timestamp/Reason: {ts_or_reason}")

    # If DQ failed, log a special message
    if dq_failed:
        logger.error("Pipeline failed due to data quality errors. See dq_summary for details.")

    # Raise one aggregated exception
    raise Exception(
        f"Pipeline completed with errors: "
        f"{len(file_failures)} file(s) failed, "
        f"{len(dq_failures)} table(s) failed DQ. Check logs for details."
    )


def load_dataframe_into_table(spark, files_path, dataframes_and_tables, target_schema_var, name, logger,
                               processed_tables, file_failures, file_processing_status, data_source="volume",
                               source_load_month=""):
    """
    Loads dataframes into the provided schema variables.

    Args:
        spark: SparkSession                    files_path: Source file path or description for logging
        dataframes_and_tables: List of (DataFrame, table_name) tuples to process    target_schema_var: Target schema name for stage tables
        name: Name of the data type being processed             logger: Logger instance
        processed_tables: Set to track successfully processed tables (modified in-place)
        file_failures: List to track failures as (path, stage, reason) tuples (modified in-place)
        file_processing_status: Boolean flag indicating overall processing status
        data_source: Source type - 'volume' (file-based) or 'delta_table' (Delta source)
        source_load_month: When set and name=="mao_004", overrides SOURCE_LOAD_MONTH on the MAO
            tables (detail/header/trailer) with this value. MMR and all other tables keep deriving
            SOURCE_LOAD_MONTH from DATA_CYCLE_ID.
    Returns:
        tuple: (processed_tables set, file_failures list, file_processing_status boolean)
    """
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    for df_tbl, tbl in dataframes_and_tables:
        try:
            # Renaming column for member table
            if tbl.upper() == "MEMBER" and "DT_OF_DTH" in df_tbl.columns:
                df_tbl = df_tbl.withColumnRenamed("DT_OF_DTH", "MEMB_DEATH_DT")
            # Calculating and adding SOURCE_LOAD_MONTH
            if data_source == "volume":
                # Use existing function that extracts from filename
                enriched_df = add_source_load_month_df(spark, df_tbl)
            elif name == "mao_004" and source_load_month:
                # MAO: stamp SOURCE_LOAD_MONTH from the parameter, overriding
                # DATA_CYCLE_ID. Applies to stage_medicare_mao and its header/trailer tables.
                # DATA_CYCLE_ID / HOME_PLAN_ID / SUBM_BID are kept as-is from Delta Share.
                logger.info(f"Stamping SOURCE_LOAD_MONTH='{source_load_month}' on stage_{tbl} (MAO override)")
                enriched_df = df_tbl.withColumn("SOURCE_LOAD_MONTH", F.lit(source_load_month))
            else:
                # MMR and other Delta tables: transform DATA_CYCLE_ID from "202512" to "2025_12"
                enriched_df = df_tbl.withColumn(
                    "SOURCE_LOAD_MONTH",
                    F.concat(
                        F.substring(col("DATA_CYCLE_ID"), 1, 4),   # Year (first 4 chars)
                        F.lit("_"),                                # Underscore separator
                        F.substring(col("DATA_CYCLE_ID"), 5, 2)    # Month (last 2 chars)
                    )
                )
            # PROCESS_IND_YN tracks downstream processing; stamp 'N' on load for the tables
            # that carry it (stage_mmr, stage_medicare_mao). Updated to 'Y' once processed.
            if data_source == "delta_table" and tbl in ("mmr", "medicare_mao"):
                enriched_df = enriched_df.withColumn("PROCESS_IND_YN", F.lit("N"))
            # Adding audit columns
            df_with_audit = add_audit_columns(spark, enriched_df, account_name, logger)
            # Trim whitespace on all string columns before landing in stage_*.
            # Source files carry stray spaces on some keys (e.g. CONTRACT_ID, ICN,
            # MEMB_ID_CD); cleaning them here keeps the downstream MERGE joins correct
            # without TRIM()-ing at query time, and needs no per-column maintenance.
            df_with_audit = trim_string_columns(df_with_audit)
            # Validating and aligning the schema before writing to delta table
            full_table_name = f"{target_schema_var}.stage_{tbl}"
            delta_schema = spark.table(full_table_name).schema
            valid_df_aligned = map_df_to_schema(spark, df_with_audit, delta_schema)

            # Writing the valid data into target
            logger.info(f"Writing the valid data into target stage_{tbl}")
            write_table(valid_df_aligned, spark, target_schema_var, f"stage_{tbl}", "append")
            logger.info(f"Table stage_{tbl} loaded successfully")

            processed_tables.add(tbl)

        except Exception as table_err:
            logger.error(f"Error processing table {tbl} from file {name}: {table_err}", exc_info=True)
            file_failures.append((files_path, "Table processing error", str(table_err)))
            file_processing_status = False
            continue

    return processed_tables, file_failures, file_processing_status


def _build_table_path(catalog, schema, table_path):
    """
    Build full Delta table path from catalog, schema, and table name.

    Args:
        catalog: Catalog name
        schema: Schema name
        table_path: Table path (can include schema as prefix)

    Returns:
        Full table path as "catalog.schema.table"
    """
    return f"{catalog}.{schema}.{table_path}"


def _load_and_union_tables(spark, catalog, schema, table_def, schema_config, supplemental_file_names, logger,
                            source_tables_processed, batch_ids=None):
    """
    Load one or more Delta tables and optionally union them.

    Args:
        spark: SparkSession                         catalog: Delta catalog
        schema: Delta schema                         table_def: Table definition with 'name', optional 'union', 'source_table' or 'source_tables'
        schema_config: Expected schema configuration  supplemental_file_names: List of supplemental file names
        logger: Logger instance                       source_tables_processed: List to track processed source tables
        batch_ids: List of SUBM_BID values for filtering
    Returns:
        tuple: (dataframe, target_name) or (None, None) if no data loaded
    """
    target_name = table_def["name"]
    column_mapping = table_def.get("column_mapping", {})
    is_union = table_def.get("union", False)

    # Get source table list - supports single and multiple tables
    if "source_tables" in table_def:
        source_table_list = table_def["source_tables"]
    elif "source_table" in table_def:
        source_table_list = [table_def["source_table"]]
    else:
        # Default: assume table name matches target name
        source_table_list = [target_name]

    if is_union:
        logger.info(f"Processing UNION of {len(source_table_list)} sources for {target_name}")

    combined_df = None
    tables_loaded = []

    for source_table_path in source_table_list:
        # Build full table path
        source_table = _build_table_path(catalog, schema, source_table_path)

        # Check if table exists - skip non-existent tables for both single and union sources
        if not spark.catalog.tableExists(source_table):
            if is_union:
                logger.info(f"Table {source_table} not found, skipping (union mode)")
            else:
                logger.error(f"Table {source_table} not found")
            continue

        # Load the source table
        try:
            logger.info(f"Loading {source_table}")
            df = _load_single_delta_table(spark=spark, source_table=source_table, target_name=target_name,
                                           schema_config=schema_config, supplemental_file_names=supplemental_file_names,
                                           logger=logger, column_mapping=column_mapping, batch_ids=batch_ids)
            # Combine dataframes
            if combined_df is None:
                combined_df = df
            elif is_union:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            else:
                # Multiple source tables without union flag - shouldn't happen but handle gracefully
                logger.warning(f"Multiple source tables found but union=false for {target_name}")
                combined_df = df  # Use only the latest one

            tables_loaded.append(source_table)
            source_tables_processed.append(source_table)

        except Exception as e:
            logger.error(f"Failed to load {source_table}: {e}")
            if not is_union:
                raise

    if combined_df is not None:
        logger.info(f"Successfully loaded {target_name} from {len(tables_loaded)} table(s)")
        return combined_df, target_name
    else:
        logger.warning(f"No data loaded for {target_name} - all sources missing")
        return None, None


def load_from_delta_source(spark, name, catalog, schema, config, schema_config, supplemental_file_names, logger, batch_ids=None):
    """
    Load data from Delta table(s) and prepare for ingestion.
    Handles both single tables and multi-table supplemental sources.

    Args:
        spark: SparkSession
        name: Name of the data type (e.g., 'member', 'mao_004')
        catalog: Delta catalog (e.g., 'bhi_medadvsupp_data_deploy_01')
        schema: Delta schema (e.g., 'atomic')
        config: Data loader configuration dictionary
        schema_config: Expected schema configuration
        supplemental_file_names: List of supplemental file names
        logger: Logger instance
        batch_ids: List of SUBM_BID values for filtering

    Returns:
        tuple: (list of (DataFrame, table_name) tuples, list of source tables processed)
    """
    dataframes_and_tables = []
    source_tables_processed = []
    try:
        # Get Delta table mappings from config
        delta_mappings = config.get("delta_table_mappings", {})

        if name not in delta_mappings:
            # Strict validation: all tables must be explicitly configured
            raise Exception(
                f"No Delta mapping configuration found for '{name}'. "
                f"Please add it to delta_table_mappings in data_loader_config.yaml"
            )

        if "tables" not in delta_mappings[name]:
            raise Exception(
                f"Invalid configuration for '{name}': missing 'tables' array. "
                f"Please ensure configuration follows the standard structure."
            )

        # Process tables array (handles both single and multi-table configurations)
        logger.info(f"Processing configuration for: {name}")

        # Get schema from config if specified, otherwise use provided schema
        config_schema = delta_mappings[name].get("schema", schema)

        for table_def in delta_mappings[name]["tables"]:
            # Use unified load function that handles both single and union tables
            loaded_df, target_name = _load_and_union_tables(
                spark, catalog, config_schema, table_def, schema_config,
                supplemental_file_names, logger, source_tables_processed,
                batch_ids
            )
            if loaded_df is not None:
                dataframes_and_tables.append((loaded_df, target_name))

        if not dataframes_and_tables:
            raise Exception(f"No Delta tables loaded for '{name}'")

        logger.info(f"Successfully loaded {len(dataframes_and_tables)} table(s) for {name}")
        return dataframes_and_tables, source_tables_processed

    except Exception as e:
        error_msg = f"Failed to load Delta source for {name}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def _load_single_delta_table(spark, source_table, target_name, schema_config, supplemental_file_names, logger,
                              column_mapping=None, batch_ids=None):
    """
    Helper function to load a single Delta table with schema validation and column mapping.

    Args:
        spark: SparkSession
        source_table: Full path to Delta table
        target_name: Name for the target table
        schema_config: Expected schema configuration
        supplemental_file_names: List of supplemental file names
        logger: Logger instance
        column_mapping: Dictionary mapping source columns to target column names
        batch_ids: List of SUBM_BID values for filtering

    Returns:
        DataFrame: The loaded and validated DataFrame
    """
    try:
        # Check if table exists
        if not spark.catalog.tableExists(source_table):
            raise Exception(f"Delta table {source_table} does not exist")

        # Read the Delta table
        df = spark.read.table(source_table)
        initial_count = df.count()
        logger.info(f"Loaded {initial_count} rows from {source_table}")

        # Apply SUBM_BID filter if batch_ids provided
        if batch_ids:
            if 'SUBM_BID' in df.columns:
                # commented code is for golden records logic of supplemental data
                # if target_name == "medicare_mao":
                #     df = df.filter(
                #         (col('VOID_FLAG') == 0) &
                #         (col('DATA_ANOMALY_FLAG') == 0) &
                #         (col('DELETE_FLAG') == '0') &
                #         col('SRVC_TYP').isin(['I', 'O', 'P'])
                #     )
                # elif target_name == "mmr":
                #     df = df.filter((col('DELETE_FLAG') == '0'))
                #     window_spec = Window.partitionBy("MEDCR_BNFCRY_ID", "PAYMENT_YEAR", "DELETE_FLAG").orderBy(col("CRT_TS").desc())  # or asc()
                #     df = (df.withColumn("row_num", row_number().over(window_spec))
                #             .filter(col("row_num") == 1).drop("row_num"))
                # else:
                df = df.filter(col('SUBM_BID').isin(batch_ids))
                filtered_count = df.count()
                logger.info(f"Applied SUBM_BID filter: {initial_count} -> {filtered_count} rows (filtered to {batch_ids})")
            else:
                logger.warning(f"SUBM_BID column not found in {source_table}. Loading all data.")

        # Apply column mapping if provided
        if column_mapping:
            # Rename columns based on mapping
            for src_col, tgt_col in column_mapping.items():
                if src_col in df.columns:
                    # MMR: PAYMENT_YEAR is YYYY format, append "12" to make YYYYMM for DATA_CYCLE_ID
                    # commented code is for golden records logic of supplemental data
                    # if target_name == "mmr" and src_col == "PAYMENT_YEAR" and tgt_col == "DATA_CYCLE_ID":
                    #     df = df.withColumn(tgt_col, F.concat(col(src_col), F.lit("12"))).drop(src_col)
                    # else:
                    df = df.withColumnRenamed(src_col, tgt_col)
            logger.info(f"Applied column mapping for {target_name}: {len(df.columns)} columns")

        # Apply schema mapping for non-supplemental tables only
        is_supplemental = any(supp in target_name.lower() for supp in ['mao', 'mmr', 'mor'])
        if not is_supplemental:
            try:
                expected_schema = parse_schema(schema_config, target_name)
                df = map_df_to_schema(spark, df, expected_schema)
                logger.info(f"Applied schema mapping for {target_name}")
            except Exception as e:
                logger.warning(f"Could not apply schema mapping for {target_name}: {e}")

        # Apply date conversions if needed
        df = apply_date_conversions(spark, df, target_name)

        return df
    except Exception as e:
        error_msg = f"Failed to load Delta table {source_table}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
