# Databricks notebook source
# INGESTION TO TRANSFORMATION - Data Standardization and Transformation Pipeline

# COMMAND ----------

# Define Notebook Widgets for user input
dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"], "Environment")
dbutils.widgets.text("transformation_tables", "member", "Transformation Tables (comma-separated)")
dbutils.widgets.dropdown("historical_flags", "Y", ["Y", "N"], "Include Historical Data")

from datetime import datetime

# Get the current year for dropdown ranges
current_year = datetime.now().year
three_years_ago = current_year - 3

# Dynamic month dropdown (01-12)
dbutils.widgets.dropdown("source_load_month", "01",
                         [f"{i:02d}" for i in range(1, 13)],
                         "Source Load Month")

# Dynamic year dropdown (last 3 years to current)
dbutils.widgets.dropdown("source_load_year", str(current_year),
                         [str(i) for i in range(three_years_ago, current_year + 1)],
                         "Source Load Year")

# COMMAND ----------

# Import required libraries and utilities
from datetime import datetime
import yaml
import time
import sys

# Add project root to path for imports
sys.path.insert(0, '/Workspace/Repos/DEV/popA')

from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.generic_util import execute_sql_file
from src.spark.data_standardization.data_standardization_helpers import execute_transformation_script

# Initialize logger
logger = get_logger("ingestion_to_transformation", "Notebook started")

# COMMAND ----------

try:
    # Record start time for performance tracking
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("Starting Data Standardization and Transformation Pipeline")
    logger.info("=" * 80)

    # Get widget values from user input
    environment = dbutils.widgets.get("src").lower()
    v_transformation_tables = dbutils.widgets.get("transformation_tables").lower()
    selected_month = dbutils.widgets.get("source_load_month")
    selected_year = dbutils.widgets.get("source_load_year")
    historical_flag = dbutils.widgets.get("historical_flags")

    logger.info(f"📋 Parameters:")
    logger.info(f"   Environment: {environment}")
    logger.info(f"   Transformation Tables: {v_transformation_tables}")
    logger.info(f"   Source Load Month: {selected_month}")
    logger.info(f"   Source Load Year: {selected_year}")
    logger.info(f"   Historical Flags: {historical_flag}")

    # Get current user for audit trail
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    logger.info(f"   Current User: {account_name}")

except Exception as e:
    logger.error(f"❌ Error reading widget parameters: {str(e)}")
    raise Exception(f"Failed to read notebook parameters: {str(e)}")

# COMMAND ----------

try:
    # Load configuration from YAML files
    logger.info("\n📂 Loading configuration files...")

    config = get_config_yaml("config/constants/data_loader_config.yaml")
    col_map_config = get_config_yaml("config/sql/file_read_meta.yaml")

    # Extract configuration values
    catalog = config.get("catalog", "bhi_popA")
    schema_ingestion = config.get("schema_ingestion", "ingestion")
    gap_schema_transformation = config.get("gap_schema_transformation", "transformation")
    reference_schema = config.get("reference_schema", "reference")

    logger.info(f"✅ Configuration loaded:")
    logger.info(f"   Catalog: {catalog}")
    logger.info(f"   Ingestion Schema: {schema_ingestion}")
    logger.info(f"   Transformation Schema: {gap_schema_transformation}")
    logger.info(f"   Reference Schema: {reference_schema}")

    # Set default catalog
    spark.sql(f"USE CATALOG {catalog}")
    logger.info(f"✅ Using catalog: {catalog}")

except Exception as e:
    logger.error(f"❌ Error loading configuration: {str(e)}")
    raise Exception(f"Failed to load configuration: {str(e)}")

# COMMAND ----------

try:
    # Prepare variable replacements for SQL templates
    logger.info("\n🔄 Preparing SQL variable replacements...")

    year_month = f"{selected_year}_{selected_month}"

    replacements = {
        "${catalog}": catalog,
        "${schema_ingestion}": schema_ingestion,
        "${gap_schema_transformation}": gap_schema_transformation,
        "${schema_reference}": reference_schema,
        "${CURRENT_USER}": account_name,
        "${SOURCE_LOAD_MONTH}": year_month,
        "${CURRENT_TIMESTAMP}": datetime.now().isoformat()
    }

    logger.info(f"✅ Replacements configured:")
    for key, value in replacements.items():
        logger.info(f"   {key} → {value}")

except Exception as e:
    logger.error(f"❌ Error preparing replacements: {str(e)}")
    raise Exception(f"Failed to prepare SQL replacements: {str(e)}")

# COMMAND ----------

try:
    # Parse transformation tables list and determine which SQL scripts to execute
    logger.info("\n📋 Determining transformation scripts to execute...")

    if not v_transformation_tables or v_transformation_tables.strip() == "":
        logger.warning("⚠️  No transformation tables specified. Exiting.")
        dbutils.notebook.exit("No transformation tables specified")

    # Split comma-separated values and clean up
    selected_tables = [t.strip() for t in v_transformation_tables.split(",")]

    # Map table names to SQL script files
    sql_scripts = {}
    for table in selected_tables:
        script_name = f"{table}.sql"
        sql_scripts[table] = script_name

    logger.info(f"✅ Transformation scripts to execute:")
    for table, script in sql_scripts.items():
        logger.info(f"   - {table} → {script}")

except Exception as e:
    logger.error(f"❌ Error determining transformation scripts: {str(e)}")
    raise Exception(f"Failed to determine transformation scripts: {str(e)}")

# COMMAND ----------

try:
    # Define SQL directory path
    sql_directory = "src/spark/data_standardization/ingestion_to_transformation_sql"

    logger.info("\n⚡ EXECUTING SQL TRANSFORMATIONS")
    logger.info("=" * 80)

    # Execute each transformation script
    for table_name, script_name in sql_scripts.items():
        try:
            logger.info(f"\n🔄 Processing: {table_name}")
            logger.info(f"   Script: {script_name}")

            # Execute the transformation script
            execute_transformation_script(
                script_name=script_name,
                replacement_map=replacements,
                catalog=catalog,
                schema_ingestion=schema_ingestion,
                schema_transformation=gap_schema_transformation,
                schema_reference=reference_schema,
                sql_directory=sql_directory,
                historical_flag=historical_flag,
                logger=logger
            )

            logger.info(f"✅ {table_name} transformation completed successfully")

        except Exception as table_error:
            logger.error(f"❌ Error processing {table_name}: {str(table_error)}")
            raise Exception(f"Failed to transform {table_name}: {str(table_error)}")

except Exception as e:
    logger.error(f"❌ Error during SQL execution: {str(e)}")
    raise Exception(f"SQL transformation failed: {str(e)}")

# COMMAND ----------

try:
    # Validation: Check if target tables have data
    logger.info("\n📊 VALIDATION: Checking target tables")
    logger.info("=" * 80)

    for table_name in selected_tables:
        try:
            table_full_name = f"{catalog}.{gap_schema_transformation}.{table_name}"

            # Check row count
            count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_full_name}").collect()
            row_count = count_result[0]["cnt"]

            logger.info(f"✅ {table_name}: {row_count:,} rows")

        except Exception as val_error:
            logger.warning(f"⚠️  Could not validate {table_name}: {str(val_error)}")

except Exception as e:
    logger.warning(f"⚠️  Validation check failed: {str(e)}")

# COMMAND ----------

# Final summary and cleanup
try:
    end_time = time.time()
    duration = round(end_time - start_time, 2)

    logger.info("\n" + "=" * 80)
    logger.info("DATA STANDARDIZATION PIPELINE COMPLETED")
    logger.info("=" * 80)
    logger.info(f"✅ Total Duration: {duration} seconds")
    logger.info(f"📅 Completion Time: {datetime.now().isoformat()}")
    logger.info("=" * 80)

    # Return success
    dbutils.notebook.exit("✅ Transformation pipeline completed successfully")

except Exception as e:
    logger.error(f"❌ Error in final summary: {str(e)}")
    dbutils.notebook.exit(f"❌ Pipeline failed: {str(e)}")

# COMMAND ----------

# OPTIONAL: Data Quality Validation Section
# Uncomment to enable additional validation checks

def validate_member_data(catalog, schema, logger):
    """Validate member transformation data quality"""
    logger.info("\n🔍 Running Member Data Quality Checks...")

    checks = [
        {
            "name": "Null Primary Keys",
            "sql": f"SELECT COUNT(*) FROM {catalog}.{schema}.member WHERE memb_id_cd IS NULL OR home_plan_id_cd IS NULL"
        },
        {
            "name": "Invalid Date Ranges",
            "sql": f"SELECT COUNT(*) FROM {catalog}.{schema}.member_enrollment WHERE covrg_begin_dt > covrg_end_dt"
        },
        {
            "name": "Duplicate Records",
            "sql": f"SELECT memb_id_cd, COUNT(*) as cnt FROM {catalog}.{schema}.member GROUP BY memb_id_cd HAVING COUNT(*) > 1"
        }
    ]

    for check in checks:
        try:
            result = spark.sql(check["sql"]).collect()
            count = result[0][0] if result else 0
            status = "✅" if count == 0 else "⚠️"
            logger.info(f"{status} {check['name']}: {count}")
        except Exception as e:
            logger.warning(f"⚠️  {check['name']} check failed: {str(e)}")

# Uncomment to run validation:
# validate_member_data(catalog, gap_schema_transformation, logger)
