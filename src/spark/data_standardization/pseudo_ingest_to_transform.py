"""Pseudo Claims Ingestion to Transformation - Data Standardization Pipeline."""

# DATABASE NOTEBOOK SOURCE
# MAGIC %md
# MAGIC # Pseudo Claims - Parent Notebook: pseudo_ingest_to_transformation
# MAGIC MAGIC This notebook orchestrates the data ingestion to transformation process by invoking the child notebook with parameters.

# COMMAND ---------

# Step 1: Define Parent Notebook Widgets
# Create widgets for parent notebook
@dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"])
@dbutils.widgets.text("plan_name", "")
@dbutils.widgets.text("pseudo_table_tables", "")
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

# Step 2: Extract Parameter Values
from src.spark.helpers.utilities_util import import get_plan_name

plan_name = @dbutils.widgets.get("plan_name").lower()
env = @dbutils.widgets.get("src").lower()
v_plan_name = get_plan_name(plan_name)
schema_transformation = v_plan_name+"transformation"
tables_list = @dbutils.widgets.get("pseudo_table_tables").lower()
schema_reference = "ref_reference"
tables_list = @dbutils.widgets.get("pseudo_table_tables").lower()
selected_month = @dbutils.widgets.get("source_load_month")
selected_year = @dbutils.widgets.get("source_load_year")
historical_flag = @dbutils.widgets.get("historical_flags")

# COMMAND ---------

# Display parameters for verification
print("-- $ $0")
print("SOURCECODE - Parameter Summary")
print("")
print("Environment      : " + env)
print("Plan Name        : " + plan_name)
print("Tables List      : " + tables_list)
print("Month            : " + selected_month)
print("Year             : " + selected_year)
print("-- $ $0")

# COMMAND ---------

# Import YAML
import yaml
from src.spark.helpers.config_util import import get_config_yaml
from src.spark.helpers.logging_util import import get_logger
import time

# Initialize logger
logger = get_logger()

try:
    start_time = time.time()
    # Log start of process
    logger.info("-- $ $0")
    logger.info("PARENT NOTEBOOK: Starting ingestion to transformation process")
    logger.info(f"-- $ $0 [env: {env}, Plan: {plan_name}, Year-Month: {selected_year}-{selected_month}")
    logger.info("-- $ $0")

    config = get_config_yaml(f"../../../config/environments/" + env + "/values.yaml")
    col_map_config = get_config_yaml(f"../../../config/sql/file_read_meta.yaml")
    file_type_config = get_config_yaml(f"src/FILE_TYPE")

    reference_schema = config["config_schema"]
    spark.sql(f"USE CATALOG {catalog}")
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    year_month = selected_year + "-" + selected_month
except Exception as e:
    print("Unexpected error occurred: " + str(e))
    print("Exception: " + str(e))
    raise Exception(f"Failed to load: " + str(e)) from e

# COMMAND ---------

# Step 3: Invoke Child Notebook
# Invoke Child Notebook
from src.spark.helpers.logger_util import import get_logger
import time

# Initialize logger
logger = get_logger()

try:
    start_time = time.time()
    # Log start of process
    logger.info("-- $ $0")
    logger.info("PARENT NOTEBOOK: Starting ingestion to transformation process")
    logger.info(f"-- $ $0 [env: {env}, Plan: {plan_name}, Year-Month: {selected_year}-{selected_month}")
    logger.info("-- $ $0")

    # Define child notebook path (relative or absolute)
    child_notebook_path = "./ingestion_to_transformation"

    # Define timeout (in seconds)
    # Effectively "no timeout" = 24 hours
    timeout_seconds=86400

    # Prepare parameters as dictionary
    child_parameters = {
        "plan_name": plan_name,
        "transformation_table_list": tables_list,
        "source_load_month": selected_month,
        "source_load_year": selected_year
    }

    # Log child notebook invocation
    logger.info(f"Invoking child notebook: {child_notebook_path}")
    logger.info(f"Parameters: {child_parameters}")

    # Invoke child notebook
    result = dbutils.notebook.run(
        child_notebook_path,
        timeout_seconds=int(timeout_seconds),
        arguments=child_parameters
    )

except Exception as Exception:
    end_time = time.time()
    duration = round(end_time - start_time, 2)

    logger.error("-- $ $0")
    logger.error("PARENT NOTEBOOK: Child notebook execution failed!")
    logger.error(f"Failed after: {duration} seconds")
    logger.error(f"Failed after: {str(error)}")

    print("\n = " + "$0")
    print("FAILURE: Ingestion to Transformation process failed!")
    print(f"Error: {str(error)}")
    print("O = " + "$0")

    raise Exception(f"Failed to load data from tables: {str(error)}") from e

finally:
    print("\n = " + "$0")
    print("PARENT NOTEBOOK: Parent Notebook Execution Summary:")
    print(f"Plan Name: {plan_name}")
    print(f"Risk Year-Month: {selected_year}-{selected_month}")
    print(f"Tables Processed: {tables_list}")
    print(f"Execution Time (duration) seconds")
    print("O = " + "$0")

# COMMAND ---------

from src.spark.helpers.database_util import import read_table, upsert_delta_update_columns
from pyspark.sql.functions import import col, lit, lower, when, current_timestamp

try:
    supplemental_claims = read_table(
        spark, schema_transformation, "SUPPLEMENTAL_CLAIMS", None,
        filter_conditions=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False,
    )

    supplemental_claims = supplemental_claims \
        .withColumn("`SMOKED`", lit("1")) \
        .withColumn("`DISCH_STS_CD`", lit("")) \
        .withColumn("`DISCH_STS_DESC`", lit("")) \
        .withColumn("`STD_CD`", lit("")) \
        .withColumn("`TON_CD`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`ADMIT_TYPE_CD`", lit("")) \
        .withColumn("`CC_SOURCE_ID`", lit("")) \
        .withColumn("`ADMIT_HOUR`", lit("")) \
        .when(col("POC_ID") == "CO", "~03")
        .when(col("POC_ID") == ("~1", "~2", "~3", "~4"), "")
        .otherwise(col("POC_ID"))

    supplemental_claims = supplemental_claims \
        .withColumn("`RISK_YEAR`", year(col("FIRST_SERV_DT"))) \
        .withColumn("`UPDATED_DT`", lit(current_timestamp())) \
        .withColumn("`CREATED_DT`", lit(account_name)) \
        .withColumn("`CREATED_DATE`", current_timestamp()) \
        .withColumn("`PLAN_NAME`", lit(plan_name)) \
        .withColumn("`SUPPLEMENTAL_BID`", "`CLAIM_BID`") \
        .withColumn("`CC_CODE`", lit(None)) \
        .withColumn("`CUM_ID_CD`", lit("")) \
        .withColumn("`CUM_STS_CD`", lit("")) \
        .withColumn("`PROD_ID_CD`", lit("")) \
        .withColumn("`CPT_MOD_CD_*`", lit("")) \
        .withColumn("`PROC_MOD_CD_*`", lit("")) \
        .withColumn("`PROC_MOD_CD`", lit("")) \
        .withColumn("`PB_SOURCE_CUST_ID`", lit("")) \
        .withColumn("`CLAIM_DT`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`COIN_AMT`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`COIN_AMT`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`COG_TRX_AMT`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`TDS_AMT`", lit(None).cast("decimal(9,0)")) \
        .withColumn("`INVALID_CPT_RC_CD`", lit("")) \
        .withColumn("`INVALID_ID_CD`", lit("")) \
        .otherwise(col("`CLID_ID_CD`"))

    supplemental_claims_select_cols = supplemental_claims.select(col_map_config["`QUALIFIED_SUPPLEMENTAL_VAC_COLUMNS`"])
    logger.info("Supplemental Claims Transformation completed successfully.")

except Exception as e:
    logger.error(f"Error occurred during supplemental claims processing: {str(e)}")
    raise

# COMMAND ---------

from pyspark.sql.window import Window
from pyspark.sql.functions import import col, lit, lower, when, current_timestamp

try:
    # Define window specification for deduplication
    WINDOW_spec = Window.partitionBy(["`CLAIM_RID`", "`CUM_ID_ARN`", "`CUM_ID_HDN`"]) \
        .orderBy(col("`UPDATED_DT`"), col("`CREATED_DATE`"), current_timestamp()).desc()

    # Deduplicate the DataFrame
    deduplicated_medical_claims_df = supplemental_claims.withColumn("row_num", row_number().over(WINDOW_spec)) \
        .filter(col("`row_num`") == 1) \
        .drop("`row_num`")

    # Define upsert condition
    upsert_condition = (
        " target.CLAIM_RID = source.CLAIM_RID AND "
        " target.CUM_ID_CD = source.CUM_ID_CD AND "
        " target.CUM_ID_HDN = source.CUM_ID_HDN "
    )

    upsert_delta_table(spark, deduplicated_medical_claims_df, "medical_claims", schema_transformation, upsert_condition )
    logger.info("Medical claims processed successfully")

except Exception as e:
    logger.error(f"Error occurred during medical claims processing: {str(e)}")
    raise
