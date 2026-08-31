"""Databricks Utilities - Comprehensive helper functions for Spark/Databricks operations."""

import ast
import boto3
from delta.tables import DeltaTable
from src.spark.helpers.logger_util import get_logger
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import row_number, lit, to_date, col, year, concat_ws, col, lower, coalesce
from typing import Union, List, Optional, Tuple
from pyspark.sql.types import StructType

logger = get_logger()


def get_path_plan_name(plan_name: str) -> str:
    """
    Returns the schema prefix based on the plan_name.
    If the plan name is 'non_anthem', returns an empty string."""  
    return "" if plan_name =="non_anthem" else  plan_name+"-"

def get_plan_name(plan_name: str) -> str:
    """
    Returns the schema prefix based on the plan_name.
    If the plan name is 'non_anthem', returns an empty string."""  
    return "" if plan_name =="non_anthem" else  plan_name+"_"

def get_curation_schema(plan_name: str, incl_supplemental_mmr: str, incl_pseudo_claim: str = "N") -> str:
    """Resolves the curation schema for a scoring run, segregating supplemental from non-supplemental output.

    Non-supplemental runs write to (plan_name)_curation (unchanged behavior).
    Supplemental runs write to (plan_name)_curation_supp so the two run types are physically isolated.
    Supplemental has two feeds - MMR supplemental (incl_supplemental_mmr) and MAO pseudo-claims (incl_pseudo_claim) -
    and a run is treated as supplemental when EITHER flag is "Y".
    This is the single decision point for supplemental vs non-supplemental output routing.
    For non_anthem the prefix is empty, yielding "curation" or "curation_supp".

    Args:
        plan_name (str): The plan name (e.g. "uatplan1", "non_anthem").
        incl_supplemental_mmr (str): MMR supplemental run flag, "Y" for supplemental.
        incl_pseudo_claim (str): MAO pseudo-claim run flag, "Y" for supplemental.
            Defaults to "N" so callers that do not segregate on pseudo-claims keep their existing behavior.

    Returns:
        str: The resolved curation schema name.
    """
    v_plan_name = get_plan_name(plan_name)
    is_supp = (str(incl_supplemental_mmr).upper() == "Y" or str(incl_pseudo_claim).upper() == "Y")
    suffix = "curation_supp" if is_supp else "curation"
    return v_plan_name + suffix

def get_gap_curation_schema(plan_name: str, incl_supplemental_mmr: str) -> str:
    """Resolves the gap curation schema for a scoring/gap run, mirroring get_curation_schema for supplemental segregation.

    Non-supplemental runs use (plan_name)_gap_curation (unchanged).
    Supplemental runs use (plan_name)_gap_curation_supp.

    Args:
        plan_name (str): The plan name (e.g. "uatplan1", "non_anthem").
        incl_supplemental_mmr (str): Run type flag, "Y" for supplemental.

    Returns:
        str: The resolved gap curation schema name.
    """
    v_plan_name = get_plan_name(plan_name)
    suffix = "gap_curation_supp" if str(incl_supplemental_mmr).upper() == "Y" else "gap_curation"
    return v_plan_name + suffix


def get_dict_value(dict, key):
    """Returns values for the input key of a string format dict."""
    return str.literal_eval(dict).get(key, None)

def dedupe(array):
    """Removed duplicates and returns sorted de-duplicated input values."""
    return sorted(set(array)) if array is not None else None

def load_csv(
    spark,
    path: str,
    schema: StructType,
    delimiter: str = ",",
    header: bool = True
):
    """
    Loads a CSV file into a Spark DataFrame using a predefined schema.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the .csv file.
        schema (StructType): The schema to enforce.
        delimiter (str, optional): Field delimiter. Defaults to ','.
        header (bool, optional): Whether the first row is a header. Defaults to True.

    Returns:
        DataFrame: The loaded CSV as a Spark DataFrame.

    Raises:
        Exception: If error loading CSV file [path].
    """
    try:
        df = (
            spark.read.format("csv")
            .option("header", header)
            .option("delimiter", delimiter)
            .schema(schema)
            .load(path)
        )
        # Force schema validation on Spark Connect (lazy evaluation)
        df.schema
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file [path]: {e}")
        raise Exception(f"Failed to load CSV file [path]: {e}")

def load_txt(
    spark,
    path: str,
    schema: StructType,
    delimiter: str = "|",
    header: bool = True
):
    """
    Loads a text file into a Spark DataFrame using a predefined schema.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the .txt file.
        schema (StructType): The schema to apply to the data.
        delimiter (str, optional): The delimiter used in the .txt file. Defaults to "|".
        header (bool, optional): Whether the first row is a header row. Defaults to True.

    Returns:
        DataFrame: The loaded text data as a Spark DataFrame.

    Raises:
        Exception: If error loading text file [path].
    """
    try:
        df = (
            spark.read.format("csv")
            .option("header", header)
            .option("delimiter", delimiter)
            .schema(schema)
            .load(path)
        )
        # Force schema validation on Spark Connect (lazy evaluation)
        df.schema
        return df
    except Exception as e:
        logger.error(f"Error loading text file [path]: {e}")
        raise Exception(f"Failed to load text file [path]: {e}")

def read_table(
    spark,
    schema: str,
    table_name: str,
    columns: Optional[List[str]] = None,
    filter_condition: Union[Column, List[Column], None] = None,
    is_table_mandatory: bool = True,
) -> DataFrame:
    """
    Reads selected columns from a table in a given schema, applies an optional filter,
    and raises an exception if the result is empty.

    Args:
        spark (SparkSession): The Spark session.
        schema (str): The schema name.
        table_name (str): The table name.
        columns: Optional[List[str]] = None.
        filter_condition (Column or list of Columns, optional): A PySpark Column expression or list of expressions to filter the DataFrame.
                If is_table_mandatory: bool = True, raises an exception if the table is empty after applying filters.

    Returns:
        DataFrame: The (optionally filtered) table data as a Spark DataFrame.

    Raises:
        Exception: If the resulting DataFrame is empty or table doesn't exist.
    """
    full_table_name = f"[{schema}].[{table_name}]"
    try:
        # Check if table exists (SQL-based, works on Serverless)
        try:
            spark.sql(f"DESCRIBE TABLE {full_table_name}")
        except Exception:
            raise Exception(f"Table [{full_table_name}] does not exist.")

        # Read the table
        df = spark.read.table(full_table_name)

        # Check if table is empty before filtering
        if is_table_mandatory and df.limit(1).count() == 0:
            raise Exception(f"Table [{full_table_name}] is empty.")

        # Select columns if provided
        if columns:
            # Cache df.columns to avoid repeated Analyze RPCs on Spark Connect
            df_columns = set(df.columns)
            missing_cols = [col_name for col_name in columns if col_name not in df_columns]
            if missing_cols:
                raise Exception(f"Columns {missing_cols} not found in table [{full_table_name}].")
            df = df.select(*columns)

        # Apply filter condition if provided
        if filter_condition:
            if isinstance(filter_condition, list):
                from functools import reduce
                combined_condition = reduce(lambda a, b: a & b, filter_condition)
                df = df.filter(combined_condition)
            else:
                df = df.filter(filter_condition)

        # Check if filtered DataFrame is empty
        if is_table_mandatory and df.limit(1).count() == 0:
            raise Exception(f"Table [{full_table_name}] is empty after applying filters.")

        return df

    except Exception as e:
        logger.error(f"Error loading table [{full_table_name}]: {e}")
        raise Exception(f"Failed to read table [{full_table_name}]: {e}") from e

def write_table(
    df, spark, schema, table_name, mode, partition_by: Optional[List[str]] = None
) -> None:
    """
    Writes a DataFrame to a Delta table in the specified schema, with optional partitioning.

    Args:
        df (DataFrame): The DataFrame to write.
        spark (SparkSession): The Spark session.
        schema (str): The schema name.
        table_name (str): The table name.
        mode (str): The write mode.
        partition_by (List[str], optional): List of column names to partition by.

    Returns:
        None

    Raises:
        Exception: If error writing data to table [schema].[table_name].
    """
    try:
        # Create the schema if it doesn't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        if partition_by:
            df.write.format("delta").mode(mode).option("delta.enableChangeDataFeed", "true").partitionBy(partition_by).saveAsTable(f"[{schema}].[{table_name}]")
        else:
            df.write.format("delta").mode(mode).saveAsTable(f"[{schema}].[{table_name}]")

        logger.info(f"Data successfully written to table [{schema}].[{table_name}]")
    except Exception as e:
        logger.error(f"Error writing data to table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to write data to table [{schema}].[{table_name}]: {e}") from e

def update_table(spark, schema: str, table_name: str, condition: str, set_values: dict) -> None:
    """
    Updates a table in a given schema.

    Args:
        spark (SparkSession): The Spark session.
        schema (str): The schema name.
        table_name (str): The table name.
        condition (str): The update condition.
        set_values (dict): The values to set.

    Returns:
        None
    """
    try:
        delta_table = DeltaTable.forPath(spark, f"[{schema}].[{table_name}]")
        delta_table.update(condition=condition, set=set_values)

        logger.info(f"Data successfully updated in table [{schema}].[{table_name}]")
    except Exception as e:
        logger.error(f"Error during update on table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to update data in table [{schema}].[{table_name}]: {e}") from e

def delete_table(spark, schema: str, table_name: str, condition: str) -> None:
    """
    Deletes data from a table in a given schema.

    Args:
        spark (SparkSession): The Spark session.
        schema (str): The schema name.
        table_name (str): The table name.
        condition (str): The delete condition.

    Returns:
        None
    """
    try:
        delta_table = DeltaTable.forPath(spark, f"[{schema}].[{table_name}]")
        delta_table.delete(condition=condition)

        logger.info(f"Data successfully deleted from table [{schema}].[{table_name}]")
    except Exception as e:
        logger.error(f"Error deleting data from table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to delete data from table [{schema}].[{table_name}]: {e}") from e

def merge_table(
    spark,
    source_df: DataFrame,
    schema: str,
    table_name: str,
    when_matched_update_set: dict,
    when_not_matched_insert_values: dict,
) -> None:
    """
    Merges a DataFrame into a table in a given schema.

    Args:
        spark (SparkSession): The Spark session.
        source_df (DataFrame): The source DataFrame.
        schema (str): The schema name.
        table_name (str): The table name.
        when_matched_update_set (dict): The values to update when matched.
        when_not_matched_insert_values (dict): The values to insert when not matched.

    Returns:
        None
    """
    try:
        full_table_path = f"[{schema}].[{table_name}]"
        delta_table = DeltaTable.forPath(spark, full_table_path)

        delta_table.alias("target").merge(
            source_df.alias("source"), "target.id = source.id"
        ).whenMatched().update(set=when_matched_update_set) \
         .whenNotMatched().insert(values=when_not_matched_insert_values) \
         .execute()

        logger.info(f"Upsert completed on table [{full_table_path}]")
    except Exception as e:
        logger.error(f"Error during merge on table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to merge into table [{schema}].[{table_name}]: {e}") from e

def clean_up(df: DataFrame):
    """Cache management placeholder - Serverless compute handles caching automatically.
    
    Note: df.unpersist() and df.rdd are not supported on Serverless compute.
    Caching is managed automatically by the platform.
    """
    logger.info("Cache management is automatic on Serverless compute")
    pass

def create_merge_condition(merge_keys):
    """
    Create merge condition string from merge keys

    Args:
        merge_keys (list): List of column names to use as merge keys

    Returns:
        str: Merge condition string in format "target.col1 = source.col1 AND target.col2 = source.col2"
    """
    merge_conditions = [f"target.[{col}] = source.[{col}]" for col in merge_keys]
    return " AND ".join(merge_conditions)

def upsert_catalog(spark, catalog: str, schema: str, table_name: str, merge_condition: str, new_data_df: DataFrame) -> None:
    """
    Performs an upsert (update if exists, insert if not) on a Delta table.

    Args:
        spark (SparkSession): The Spark session.
        catalog (str): The catalog name.
        schema (str): The schema name.
        table_name (str): The table name.
        merge_condition (str): The condition to match existing records.
        new_data_df (DataFrame): The new data to upsert.

    Returns:
        None
    """
    try:
        full_table_path = f"[{catalog}].[{schema}].[{table_name}]"
        delta_table = DeltaTable.forPath(spark, full_table_path)

        delta_table.alias("target").merge(
            new_data_df.alias("source"),
            merge_condition
        ).whenMatched().update(
            set={}
        ).whenNotMatched().insert(
            values={}
        ).execute()

        logger.info(f"Upsert completed on table [{full_table_path}]")
    except Exception as e:
        logger.error(f"Error during upsert on table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to perform upsert on table [{schema}].[{table_name}]: {e}")

def create_dataframe(spark, schema: StructType, data: list):
    """
    Create a Spark DataFrame using the provided schema and data.

    Parameters:
        spark (SparkSession): The Spark session object
        schema (StructType): StructType defining the schema of the DataFrame
        data (list): List of tuples containing the data

    Returns:
        DataFrame: DataFrame created using the schema and data, or None if an error occurs
    """
    try:
        df = spark.createDataFrame(data, schema=schema)
        return df
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        raise Exception(f"Failed to create DataFrame: {e}")

def create_or_upsert_data_table(
    spark,
    df,
    table_name: str,
    schema_name: str = "default",
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    merge_condition: Optional[str] = None
):
    """
    Checks if a table exists in the specified schema. If not, creates it using the provided DataFrame.
    If it exists, perform an upsert using the provided merge condition.

    Parameters:
        spark (SparkSession): The Spark session object
        df (DataFrame): DataFrame to be saved or upserted
        table_name (str): Name of the table to be created/upserted
        schema_name (str): Name of the schema
        mode (str): Save mode ("overwrite", "append", etc.)
        partition_by (Optional[List[str]]): Optional list of columns to partition by
        merge_condition (Optional[str]): Merge condition string for upsert
    """
    full_table_name = f"[{schema_name}].[{table_name}]"
    try:
        # Check if table exists (SQL-based, works on Serverless)
        table_exists = False
        try:
            spark.sql(f"DESCRIBE TABLE {full_table_name}")
            table_exists = True
        except Exception:
            pass
        
        if table_exists:
            logger.info(f"Table '{full_table_name}' already exists.")
            if merge_condition:
                upsert_catalog(spark, schema_name, table_name, merge_condition, df)
            else:
                logger.info("Merge condition not provided. Skipping upsert.")
        else:
            write_table(df, spark, schema_name, table_name, mode, partition_by)
    except Exception as e:
        logger.error(f"Error while checking or creating/upserting table '{full_table_name}': {e}")
        raise Exception(f"Failed to check or create/upsert table '{full_table_name}': {e}")

def upsert_delta_table(
    spark,
    df,
    table_name: str,
    schema_name: str = "default",
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    merge_condition: Optional[str] = None
):
    """
    Performs an upsert (update if exists, insert if not) on a Delta table.

    Parameters:
        spark (SparkSession): The Spark session object
        df (DataFrame): DataFrame to be saved or upserted
        table_name (str): Name of the table to be created/upserted
        schema_name (str): Name of the schema
        mode (str): Save mode ("overwrite", "append", etc.)
        partition_by (Optional[List[str]]): Optional list of columns to partition by
        merge_condition (Optional[str]): Merge condition string for upsert

    Returns:
        None
    """
    try:
        full_table_path = f"[{schema_name}].[{table_name}]"
        delta_table = DeltaTable.forPath(spark, full_table_path)

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatched().update(set={}) \
         .whenNotMatched().insert(values={}) \
         .execute()

        logger.info(f"Upsert completed on table [{full_table_path}]")
    except Exception as e:
        logger.error(f"Error during upsert on table [{schema_name}].[{table_name}]: {e}")
        raise Exception(f"Failed to perform upsert on table [{schema_name}].[{table_name}]: {e}")

def upsert_delta_update_columns(spark, new_data_df, table_name: str, schema: str, merge_condition: str) -> None:
    """
    Performs an upsert (update if exists, insert if not) on a Delta table:
    - Updates all columns except created_by and created_date
    - Inserts all columns when not matched.
    """
    try:
        full_table_path = f"[{schema}].[{table_name}]"
        delta_table = DeltaTable.forPath(spark, full_table_path)

        # Build update set: exclude created_by and created_date
        # Cache columns to avoid repeated Analyze RPCs on Spark Connect
        df_columns = new_data_df.columns
        update_set = {
            col_name: f"source.[{col_name}]"
            for col_name in df_columns
            if col_name.lower() not in ["created_by", "created_date"]
        }

        # Perform merge
        delta_table.alias("target").merge(
            new_data_df.alias("source"),
            merge_condition
        ).whenMatched().update(set=update_set) \
         .whenNotMatched().insert(values={}) \
         .execute()

        logger.info(f"Upsert completed on table [{full_table_path}]")
    except Exception as e:
        logger.error(f"Error during upsert on table [{schema}].[{table_name}]: {e}")
        raise Exception(f"Failed to perform upsert on table [{schema}].[{table_name}]: {e}")

# ================================================
# Catalog Utilities (consolidated from catalog_utili.py)
# ================================================

# Valid environments
VALID_ENVIRONMENTS = ['dev', 'qa', 'stg', 'prod']

# Default schema for MA Dashboard
DEFAULT_SCHEMA = 'ma_dashboard'

# Catalog naming pattern
CATALOG_PREFIX = 'ppa_'

def validate_environment(env: str) -> bool:
    """
    Validate that the environment is valid.

    Args:
        env: Environment string (dev/qa/stg/prod)

    Returns:
        bool: True if valid

    Raises:
        ValueError: If environment is invalid
    """
    env_lower = env.lower().strip()
    if env_lower not in VALID_ENVIRONMENTS:
        raise ValueError(
            f"Must be one of: {', '.join(sorted(VALID_ENVIRONMENTS))}"
        )
    return True

def get_catalog_name(env: str, prefix: str = CATALOG_PREFIX) -> str:
    """
    Get the Unity Catalog name for a given environment.

    Args:
        env: Environment string (dev/qa/stg/prod)
        prefix: Catalog prefix (default: 'ppa_')

    Returns:
        str: Full catalog name (e.g., 'ppa_dev', 'ppa_prod')

    Raises:
        ValueError: If environment is invalid
    """
    validate_environment(env)
    return f"{prefix}{env.lower()}"

def get_full_table_name(env: str, table: str,
                        schema: str = DEFAULT_SCHEMA,
                        prefix: str = CATALOG_PREFIX) -> str:
    """
    Build a fully qualified table name for Unity Catalog.

    Args:
        env: Environment string (dev/qa/stg/prod)
        table: Table name
        schema: Schema name (default: 'ma_dashboard')
        prefix: Catalog prefix (default: 'ppa_')

    Returns:
        str: Fully qualified table name (catalog.schema.table)
    """
    catalog = get_catalog_name(env, prefix)
    return f"{catalog}.{schema}.{table}"

def parse_full_table_name(full_name: str) -> Tuple[str, str, str]:
    """
    Parse a fully qualified table name into components.

    Args:
        full_name: Fully qualified table name (catalog.schema.table)

    Returns:
        Tuple[str, str, str]: (catalog, schema, table)

    Raises:
        ValueError: If name doesn't have 3 parts
    """
    parts = full_name.split('.')
    if len(parts) != 3:
        raise ValueError(
            f"Invalid table name '{full_name}'. "
            f"Expected format: catalog.schema.table"
        )
    return tuple(parts)

def get_run_month_table_name(base_name: str, run_month: str) -> str:
    """
    Append run month suffix to a base table name.

    Args:
        base_name: Base table name (e.g., 'MEMBER_LEVEL')
        run_month: Run month in YYYYMM format (e.g., '202312')

    Returns:
        str: Table name with run month suffix
    """
    base_name = base_name.rstrip('_')
    return f"{base_name}_{run_month}"

def get_environment_from_catalog(catalog_name: str,
                                  prefix: str = CATALOG_PREFIX) -> str:
    """
    Extract environment from catalog name.

    Args:
        catalog_name: Full catalog name (e.g., 'ppa_dev')
        prefix: Catalog prefix (default: 'ppa_')

    Returns:
        str: Environment string (e.g., 'dev')
    """
    if not catalog_name.startswith(prefix):
        raise ValueError(f"Catalog '{catalog_name}' doesn't start with expected prefix '{prefix}'")

    return catalog_name[len(prefix):]

# ================================================
# Table Utilities (consolidated from table_utili.py)
# ================================================

def table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    """
    Check if a table exists in Unity Catalog.

    Args:
        spark: SparkSession
        catalog: The Unity Catalog name
        schema: The schema/database name
        table: The table name

    Returns:
        bool: True if exists, False otherwise
    """
    full_table_name = f"[{catalog}].[{schema}].[{table}]"
    try:
        # Use SQL-based check (works on Serverless, spark.catalog not supported)
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception as e:
        logger.debug(f"Table {full_table_name} does not exist: {str(e)}")
        return False

def drop_table_if_exists(spark, catalog: str, schema: str, table: str) -> bool:
    """
    Drop a table if it exists in Unity Catalog.

    Args:
        spark: SparkSession
        catalog: The catalog name
        schema: The schema/database name
        table: The table name

    Returns:
        bool: True if table was dropped, False if it didn't exist
    """
    full_table_name = f"[{catalog}].[{schema}].[{table}]"

    if table_exists(spark, catalog, schema, table):
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        logger.info(f"Dropped table: {full_table_name}")
        return True
    return False

def truncate_table(spark, catalog: str, schema: str, table: str) -> bool:
    """
    Truncate a Delta table if it exists.

    Args:
        spark: SparkSession
        catalog: The Unity Catalog name
        schema: The schema/database name
        table: The table name

    Returns:
        bool: True if table was truncated, False if it didn't exist
    """
    full_table_name = f"[{catalog}].[{schema}].[{table}]"

    if table_exists(spark, catalog, schema, table):
        spark.sql(f"TRUNCATE TABLE {full_table_name}")
        logger.info(f"Truncated table: {full_table_name}")
        return True
    return False

def get_table_row_count(spark, catalog: str, schema: str, table: str) -> int:
    """
    Get the row count of a table.

    Args:
        spark: SparkSession
        catalog: The Unity Catalog name
        schema: The schema/database name
        table: Table name

    Returns:
        int: Number of rows, or -1 if table doesn't exist
    """
    full_table_name = f"[{catalog}].[{schema}].[{table}]"

    if not table_exists(spark, catalog, schema, table):
        logger.warning(f"Table {full_table_name} doesn't exist")
        return -1

    result = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()
    return result[0]['cnt']

def create_table_from_view(spark, catalog: str, schema: str, table: str,
                           view_name: str,
                           partition_by: list = None,
                           enable_cdf: bool = True) -> None:
    """
    Create a Delta table from a temporary view with standard configurations.

    Args:
        spark: SparkSession
        catalog: The Unity Catalog name
        schema: The schema/database name
        table: The table name
        view_name: The source temp view name
        partition_by: Optional list of columns to partition by
        enable_cdf: Enable Change Data Feed (default: True)
    """
    full_table_name = f"[{catalog}].[{schema}].[{table}]"

    partition_clause = ""
    if partition_by:
        partition_clause = f"PARTITIONED BY ({', '.join(partition_by)})"

    tbl_props_clause = ""
    if enable_cdf:
        tbl_props_clause = f"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

    drop_table_if_exists(spark, catalog, schema, table)
    spark.sql(f"CREATE TABLE {full_table_name} AS (SELECT * FROM {view_name})")
    logger.info(f"Created table: {full_table_name}")

def drop_temp_views(spark, view_names: List[str]) -> None:
    """
    Drop multiple temporary views.

    Args:
        spark: SparkSession
        view_names: List of temp view names to drop
    """
    for view_name in view_names:
        try:
            spark.catalog.dropTempView(view_name)
            logger.info(f"Dropped temp view: {view_name}")
        except Exception as e:
            logger.warning(f"Could not drop {view_name}: {str(e)}")
