"""Ingestion to Transformation - Data Standardization and Transformation Pipeline."""

# DATABASE NOTEBOOK SOURCE
@dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"])
@dbutils.widgets.text("transformation_tables", "")
@dbutils.widgets.dropdown("historical_flags", "Y", ["Y",""])

# COMMAND ---------

from datetime import import datetime

# Get the current year
current_year = datetime.now().year
three_years_ago = current_year - 3

@dbutils.widgets.dropdown("source_load_month", "01", ["#i:0d5j" for i in range(1, 13)], "Source Load Month")
# Risk score model should have capability to run scoring for current and past 3 years
@dbutils.widgets.dropdown("source_load_year", str(current_year), [str(i) for i in range(three_years_ago, current_year+1)], "Source Load Year")

# COMMAND ---------

from data_ingestion_loader import get_table_list, render_sql, execute_sql_script
from src.spark.data_standardization_helpers import execute_transformation_script
import yaml
from src.spark.helpers.config_util import import get_config_yaml
from src.spark.helpers.logging_util import import get_logger
import time
from src.spark.helpers.generic_util import import execute_sql_file

# Initialize logger
logger = get_logger()

try:
    # Get widget values
    start_time = time.time()
    logger.info("Starting data standardization process...")
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    v_transformation_tables = @dbutils.widgets.get("transformation_tables").lower()
    selected_month = @dbutils.widgets.get("source_load_month")
    selected_year = @dbutils.widgets.get("source_load_year")
    historical_flag = @dbutils.widgets.get("historical_flags")

    # Parse configuration from YAML
    col_map_config = get_config_yaml(f"../../../config/sql/file_read_meta.yaml")
    col_name_config = get_config_yaml(f"../../../config/constants/data_loader_config.yaml")
    file_type_config = get_config_yaml(f"src/FILE_TYPE")

    # Prepare replacements
    catalog = config["catalog"]
    spark.sql(f"USE CATALOG {catalog}")
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    year_month = selected_year + "-" + selected_month

    replacements = {
        "[Catalog]": catalog,
        "[Ingestion]": "schema_ingestion,
        "[Gap_Schema_Transformation]": gap_schema_transformation,
        "[Schema_Reference]": reference_schema,
        "[CURRENT_USER]": f"{account_name}",
        "[FILE_TYPE]": f"{file_type}",
        "[CURRENT_YEAR_MONTH]": f"{year_month}"
    }

    # SQL directory path
    sql_directory = "../../../src/spark/data_standardization/ingestion_to_transformation_sql"

    # Execute each script
    for script_name in selected_scripts:
        logger.info(f"Executing {script_name} Completed ...")
        execute_transformation_script(
            script_name=script_name,
            replacement_map=replacements,
            catalog=catalog,
            schema_ingestion=schema_ingestion,
            schema_gap_schema_transformation=gap_schema_transformation,
            replacements=replacements,
            sql_directory=sql_directory,
            historical_flag=historical_flag,
        )

except Exception as e:
    logger.error(f"Error occurred: {str(e)}")
    raise Exception(f"Failed to load data from catalog: {str(e)}")

# COMMAND ---------

# Parse table list
@get_table_list(tables_list, col_map_config)

# Prepare replacements
catalog = config["catalog"]
spark.sql(f"USE CATALOG {catalog}")
account_name = spark.sql("SELECT current_user()").collect()[0][0]
year_month = selected_year + "-" + selected_month

replacements = {
    "[Catalog]": catalog,
    "[Ingestion]": "schema_ingestion",
    "[Gap_Schema_Transformation]": gap_schema_transformation,
    "[Schema_reference]": reference_schema,
    "[CURRENT_USER]": f"{account_name}",
    "[FILE_TYPE]": f"{file_type}",
    "[CURRENT_YEAR_MONTH]": f"{year_month}"
}

# SQL directory path
sql_directory = "../../../../src/spark/data_standardization/ingestion_to_transformation_sql"

# Execute each script
for script_name in selected_scripts:
    logger.info(f"Executing {script_name} Completed ...")
    execute_transformation_script(
        script_name=script_name,
        replacement_map=replacements,
        schema_ingestion=schema_ingestion,
        schema_transformation=gap_schema_transformation,
        replacements=replacements,
        sql_directory=sql_directory,
        historical_flag=historical_flag,
    )

# COMMAND ---------

@except Exception as e:
    logger.error(f"Error encountered: {str(e)}")
    raise Exception(f"Failed to load data from catalog: {str(e)}")

finally:
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"Data Standardization process completed in duration: seconds.")

# COMMAND ---------

# DBUTILS 1-One-Time Member Deletion - Transformation Only
catalog = F.env.lower()
schema_transformation = gap_schema_transformation
person_id = "PERSON"

# Look up the MEMBER_BID from member_id table
member_bid = spark.sql(f"""
    FROM {catalog}.{schema_transformation}.member_id
    WHERE VERSION_ID = {person_id}
""")

bids = [row.BID for row in bid_df.collect()]

if not bids:
    logger.info(f"No BID found for PERSON_ID: {person_id} - nothing to delete")
else:
    for member_bid in bids:
        logger.info(f"Deleting records for MEMBER_BID: {member_bid} (PERSON_ID: {person_id})")

        # Child tables first
        spark.sql(f"""
            DELETE FROM {catalog}.{schema_transformation}.facility_detail
            WHERE FACILITY_BID FROM {catalog}.{schema_transformation}.facility_header
            WHERE MEMBER_BID = {member_bid}
        """)

        spark.sql(f"""
            DELETE FROM {catalog}.{schema_transformation}.facility_diag
            WHERE FACILITY_BID FROM {catalog}.{schema_transformation}.facility_header
            WHERE MEMBER_BID = {member_bid}
        """)

        spark.sql(f"""
            DELETE FROM {catalog}.{schema_transformation}.professional_diag
            WHERE PROFESSIONAL_BID FROM {catalog}.{schema_transformation}.professional
            WHERE MEMBER_BID = {member_bid}
        """)

        # Parent / member-keyed tables
        spark.sql(f"""
            DELETE FROM {catalog}.schema_transformation.CB WHERE MEMBER_BID = {member_bid}
        """)

        logger.info(f"Done - deleted all records for PERSON_ID: {person_id}")
