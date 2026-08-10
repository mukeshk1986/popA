from src.spark.helpers.logger_util import get_logger
import re
from pathlib import Path
import pandas as pd
from pyspark.sql.functions import current_timestamp, current_user

logger = get_logger()

def load_ref_data(spark, ref_data_files, load_tables, catalog, ref_schema, process_all) -> dict:
    """
    Load reference data from CSV files into ref tables.

    When specific tables are requested ("process_all=False"), every name in
    "load_tables" must have a matching CSV file. A "ValueError" is raised
    listing any requested tables that have no corresponding CSV.
    """
    logger.info("loading reference data from CSV files into reference tables...")

    if not process_all and load_tables:
        available_csv_names = {f.stem for f in ref_data_files}
        missing = [t for t in load_tables if t not in available_csv_names]
        if missing:
            raise ValueError(
                f"Requested tables have no matching CSV files in the data "
                f"{sorted(available_csv_names)}"
            )

    load_summary = {}
    # Process each CSV file in ref_data_files:
    for file_path in ref_data_files:
        table_name = file_path.stem
        if process_all or table_name in load_tables:
            # Call the load function
            result = load_csv_to_ref_table(
                spark=spark,
                file_path=file_path,
                catalog=catalog,
                schema=ref_schema,
                logger=logger,
            )
            load_summary[table_name] = result
    return load_summary


def load_csv_to_ref_table(spark, file_path: Path, catalog: str, schema: str, logger) -> dict:
    """
    Load a CSV file into a Delta table with schema validation and column alignment.

    Reads the target table's schema, validates that the CSV has all required
    non-audit columns, casts each column to the target type, and selects
    columns (in target-table order so that "insertInto" (which resolves by
    position) writes data into the correct columns.

    Args:
        spark: SparkSession
        file_path: Path to CSV file
        catalog: Catalog name
        schema: Schema name
        logger: Logger instance

    Returns:
        dict: Load summary with csv_records, loaded_records, and status
    """
    table_name = file_path.stem
    full_table = f"{catalog}.{schema}.{table_name}"
    AUDIT_COLUMNS = ("CREATED_DATE", "CREATED_BY", "UPDATED_DATE", "UPDATED_BY")
    try:
        logger.info(f"\nProcessing table: {table_name}")

        # --- Check table must exist ---
        if not spark.catalog.tableExists(full_table):
            raise ValueError(
                f"Target table '{full_table}' does not exist. "
                "Run DDL setup before loading reference data."
            )

        # --- Read target schema ---
        target_schema = spark.table(full_table).schema
        data_columns = [
            f for f in target_schema
            if f.name.upper() not in AUDIT_COLUMNS
        ]

        # --- Read CSV ---
        read_csv_df = pd.read_csv(
            file_path, keep_default_na=False, na_values=[""], low_memory=False,
            dtype=str, skipinitialspace=True
        )

        # Strip any leading/trailing whitespace from column names
        read_csv_df.columns = read_csv_df.columns.str.strip()
        record_count = len(read_csv_df)
        logger.info(f"  Records in CSV: {record_count},")

        # --- Schema alignment ---
        # Case-insensitive map: lowercase CSV name -> actual CSV name
        csv_col_map = {c.lower(): c for c in read_csv_df.columns}

        # Check for missing columns (CSV must have every non-audit target column)
        missing = []
        for f in data_columns:
            if f.name.lower() not in csv_col_map:
                missing.append(f.name)
        if missing:
            raise ValueError(
                f"CSV '{file_path.name}' is missing columns required by "
                f"{full_table}': {missing}"
            )

        # Warn about extra columns not in the target table
        target_col_lower = {f.name.lower() for f in target_schema}
        extras = [c for c in read_csv_df.columns if c.lower() not in target_col_lower]
        if extras:
            logger.warning(
                f"  CSV has extra columns not in target table (ignored): {extras}"
            )

        # --- Build Spark DataFrame ---
        csv_df = spark.createDataFrame(read_csv_df)

        # Select columns in target-table order and cast to target types
        from pyspark.sql.functions import col

        select_exprs = []
        for field in data_columns:
            csv_col_name = csv_col_map[field.name.lower()]
            select_exprs.append(
                col(f"{csv_col_name}").cast(field.dataType).alias(field.name)
            )

        csv_df = csv_df.select(select_exprs)

        # Append audit columns in target-table order
        csv_df = (
            csv_df.withColumn("CREATED_DATE", current_timestamp())
            .withColumn("CREATED_BY", current_user())
            .withColumn("UPDATED_DATE", current_timestamp())
            .withColumn("UPDATED_BY", current_user())
        )

        # --- Write (positionally safe) ---
        csv_df.write.mode("overwrite").insertInto(full_table)

        logger.info(f"  Successfully loaded to: {full_table}")

        # --- Verify load ---
        verify_count = (
            spark.sql(f"SELECT COUNT(*) as count FROM {full_table}")
            .collect()[0]["count"]
        )

        logger.info(f"  Records loaded: {verify_count},")

        # --- Post-write null check for non-nullable data columns ---
        non_nullable = [f.name for f in data_columns if not f.nullable]
        if non_nullable:
            from pyspark.sql.functions import sum as sum_func, when, lit
            null_check_exprs = [
                sum_func(when(col(c).isNull(), lit(1)).otherwise(lit(0))).alias(c)
                for c in non_nullable
            ]
            null_counts = spark.sql(f"SELECT {', '.join(null_check_exprs)} FROM {full_table}").collect()[0]
            bad_cols = [c for c in non_nullable if null_counts[c] > 0]
            if bad_cols:
                logger.warning(
                    f"  Unexpected NULLs in NOT-NULL columns after load "
                    f"(possible type-cast failures): {bad_cols}"
                )

        return {
            "csv_records": record_count,
            "loaded_records": verify_count,
            "status": "SUCCESS" if record_count == verify_count else "WARNING",
        }

    except Exception as e:
        logger.error(f"  Error processing {file_path.name}: {str(e)}")
        return {
            "csv_records": "ERROR",
            "loaded_records": "ERROR",
            "status": "FAILED",
            "error": str(e),
        }


def truncate_tables(spark, catalog_name, schema, tables):
    """
    Truncates the specified tables in the given catalog and schema.

    Args:
        spark: SparkSession
        catalog_name (str): The catalog name.
        schema (str): The schema name.
        tables (list): List of table names to truncate.
    """
    logger.info(f"Truncating tables in catalog: {catalog_name}, schema: {schema}, tables: {tables}")

    spark.sql(f"USE CATALOG {catalog_name}")
    for table in tables:
        full_table_name = f"{catalog_name}.{schema}.{table}"
        sql = f"TRUNCATE TABLE {full_table_name}"
        logger.info(f"Executing: {sql}")
        spark.sql(sql)


def drop_schema_tables(spark, catalog_name, drop_schema, tables):
    """
    Drops the specified tables in the given catalog and schema.

    Args:
        spark: SparkSession
        catalog_name (str): The catalog name.
        drop_schema (str): The schema name.
        tables (list): List of table names to drop.
    """
    spark.sql(f"USE CATALOG {catalog_name}")

    if catalog_name != "" and drop_schema != "":
        logger.info(f"{catalog_name}---{drop_schema}")
        spark.sql(f"USE CATALOG {catalog_name}")
        schema_list = [s.strip() for s in drop_schema.split(",")]
        logger.info(f"schema_list : {schema_list}")

        for schema in schema_list:
            spark.sql(f"USE SCHEMA {schema}")
            for table in tables:
                spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema}.{table}")
                logger.info(f"Dropped table: {catalog_name}.{schema}.{table}")


def config_plan_setup(spark, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ma_dashboard_ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list):
    """
    Config plan setup.

    schema_list drives which DDL groups run. Callers pass the list of schema
    names for every plan-specific group and excludes the shared reference / ma_dashboard
    schemas. schema_curation_supp / gap_schema_curation_supp are the supplemental-run
    curation and gap-curation schemas (e.g. {plan_name}_curation_supp,
    {plan_name}_gap_curation_supp). They are provisioned alongside the base
    schemas so supplemental risk-scoring output is physically segregated.
    """
    ddl_files = []
    if schema_list is None:
        schema_list = []
    logger.info("Executing config plan setup")
    ddl_groups = {
            "schema_creation": ["schema_creation"],
            "ingestion": ["ingestion_tables"],
            "monitoring": ["monitoring_tables"],
            "transformation_curation": ["transformation_curation_tables"],
            "reference": ["ma_ra_reference_tables"],
            "ma_dashboard": ["ma_dashboard_ref_tables"]
        }
    reference_schema_values = {"ma": "ma_reference", "aca": "aca_reference"}
    ma_dashboard_schema_values = {"ma_dashboard": "ma_dashboard_reference"}

    if not schema_list:
        raise ValueError("schema_list cannot be empty. Provide at least one schema name.")

    
    invalid_schema_names = []
    all_ddl_files = [ddl for ddl_list in ddl_groups.values() for ddl in ddl_list]

    for schema_name in schema_list:
        schema_key = schema_name.strip().lower()
        if schema_key in ddl_groups:
            for ddl in ddl_groups[schema_key]:
                if ddl not in ddl_files:
                    ddl_files.append(ddl)
        elif schema_key in reference_schema_values:
            for ddl in ddl_groups["reference"]:
                if ddl not in ddl_files:
                    ddl_files.append(ddl)
        elif schema_key in ma_dashboard_schema_values:
            for ddl in ddl_groups["ma_dashboard"]:
                if ddl not in ddl_files:
                    ddl_files.append(ddl)
        else:
            invalid_schema_names.append(schema_name)

    if "ma_ra_reference_tables" in ddl_files and not ref_schema:
        raise ValueError(
            "reference_schema is required when reference schemas are included in schema_list. "
            "Set reference_schema to MA or ACA."
        )

    if invalid_schema_names:
        raise ValueError(
            f"Invalid schema names in schema_list: {invalid_schema_names}. "
            f"Allowed DDL groups: {list(ddl_groups.keys())}, "
            f"Allowed reference schema values: {sorted(list(set(reference_schema_values.keys()) | set(ma_dashboard_schema_values.keys())))}."
        )

    logger.info(f"schema_list : {schema_list}")
    logger.info(f"ddl_files : {ddl_files}")

    for ddl_file_path in ddl_files:
        logger.info(f"****** Execution Started : {ddl_file_path}")
        run_ddl(spark, ddl_file_path, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ma_dashboard_ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list)


def run_ddl(spark, sql_file_path: str, catalog: str, ref_schema: str, gap_schema_curation: str, schema_curation: str, schema_transformation: str, env_bucket: str, schema_ingestion: str, schema_monitoring: str, v_schema_plan_name: str, ma_dashboard_ref_schema: str, sam_ref_schema: str, sam_stage_schema: str, sam_work_schema: str, sam_result_schema: str, schema_curation_supp: str, gap_schema_curation_supp: str, schema_list: str = ""):
    """
    Reads a SQL file and renders it with the provided catalog and schema.

    Args:
        spark (SparkSession): Spark session.
        sql_file_path (str): Path to the SQL file.
        catalog (str): Catalog name.
        schema (str): Schema name.

    Returns:
        str: Rendered SQL.
    """
    print('*********************')
    print(sql_file_path)
    with open(sql_file_path) as f:
        sql_template = f.read()

    sql_rendered = sql_template.replace("${catalog}", catalog).replace("${gap_schema_curation_supp}", gap_schema_curation_supp).replace("${gap_schema_curation}", gap_schema_curation).replace("${schema_curation}", schema_curation).replace("${schema_transformation}", schema_transformation).replace("${env_bucket}", env_bucket).replace("${schema_ingestion}", schema_ingestion).replace("${schema_plan_name}", v_schema_plan_name).replace("${schema_monitoring}", schema_monitoring).replace("${ma_dashboard_reference_schema}", ma_dashboard_ref_schema).replace("${sam_work_schema}", sam_work_schema).replace("${sam_result_schema}", sam_result_schema).replace("${sam_stage_schema}", sam_stage_schema).replace("${sam_ref_schema}", sam_ref_schema).replace("${schema_curation_supp}", schema_curation_supp).replace("${schema_list}", schema_list)

    statements = [stat.strip() for stat in re.split(r";\s*\n", sql_rendered) if stat.strip()]

    try:
        for stat in statements:
            logger.info(f"Executing statement in generic_util: {stat}")
            spark.sql(stat)
    except Exception as e:
        raise Exception(f"Failed to create the tables: {str(e)}")


def ingestion_folder_check(dbutils, folder_path, logger):
    """
    Ensure folder exists, create if it doesn't.

    Args:
        folder_path (str): Path to folder
        logger: Logger instance

    Returns:
        bool: True if folder exists or was created successfully
    """
    try:
        dbutils.fs.ls(folder_path)
        logger.info(f"✓ Folder already exists: {folder_path}")
        return True
    except Exception as e:
        try:
            dbutils.fs.mkdirs(folder_path)
            logger.info(f"✓ Folder created: {folder_path}")
            return True
        except Exception as create_error:
            logger.error(f"✗ Failed to create folder {folder_path}: {create_error}")
            raise Exception(f"Failed to create folder {folder_path}: {create_error}")
