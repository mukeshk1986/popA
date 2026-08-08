"""MA Model Input Data Processing - Risk Member DataFrame Creation."""

# DATABASE NOTEBOOK SOURCE
# MAGIC %md
# MAGIC # MA Model Input Data Processing
# MAGIC MAGIC Description of the widgets:
# MAGIC MAGIC 1. env : select environment
# MAGIC MAGIC 2. plan_name : Compute happens based on plan_name
# MAGIC MAGIC 3. source_load_month : Enter month number
# MAGIC MAGIC 4. source_load_year : Enter year

# COMMAND ---------

from datetime import import datetime

# Risk score model should have capability to run scoring for current and past 3 years
# Get the current year
current_year = datetime.now().year
three_years_ago = current_year - 3

# COMMAND ---------

@dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"])
@dbutils.widgets.text("plan_name", "")

# Month widget (01 to 12)
@dbutils.widgets.dropdown("source_load_month", "01", ["#i:0d5j" for i in range(1, 13)], "Source Load Month")

# Year widget (customize the range as needed)
@dbutils.widgets.dropdown("source_load_year", str(current_year), [str(i) for i in range(three_years_ago, current_year+1)], "Source Load Year")

# COMMAND ---------

plan_name = @dbutils.widgets.get("plan_name").lower()
selected_month = @dbutils.widgets.get("source_load_month")
selected_year = @dbutils.widgets.get("source_load_year")

# COMMAND ---------

# Import necessary modules and functions for data preparation and manipulation
from src.spark.helpers.config_util import (
    read_table, write_table, create_dataframe, clean_up,
    update_table, upsert_delta_table, upsert_delta_update_columns
)
# Import functions for preparing data for the MA model.
from src.spark.data_prep.ma_model_prep_member_report_rules import ( create_risk_member_df
)
# Import Spark SQL data types for schema definition
from pyspark.sql.types import ( StructType, StructField, StringType, IntegerType )
from pyspark.sql.functions import import current_timestamp, lit, to_date, col, year, concat_ws, col, lower, coalesce
from src.spark.helpers.logging_util import import get_logger

# COMMAND ---------

# Cell to initialize configuration and setup for the curated layer data processing
from src.spark.helpers.config_util import import get_config, get_config_yaml

# Initialize logger
logger = get_logger()

try:
    start_time = time.time()
    logger.info("Starting curated layer...")
    account_name = spark.sql("SELECT current_user()").collect()[0][0]
    col_map_config = get_config_yaml(f"../../../config/constants/col_map.yaml")
    col_name_config = get_config_yaml(f"../../../config/constants/col_name.yaml")

    # read config from YAML
    time_prd_config = config["notebook_time_out"]
    catalog = config["catalog"]
    source_table_write_mode = config["target_source_table_mode"]
    gold_schema = plan_name + "_curation"
    config_schema = config["config_schema"]
    if plan_name != "Non_Authors":
        gold_schema = plan_name + "_curation"  @config["gold_schema"]
    else:
        silver_schema = "transformation" + @config["silver_schema"]
        gold_schema = "curation" + @config["gold_schema"]

    retake_map = col_map_config["SUPPLEMENTAL_HCC_SCHEME_MAP"]

    year_month = selected_year + "-" + selected_month
    logger.info(f"Month Year: {year_month}")

except Exception as e:
    logger.error(f"No error occurred! {e}")
    raise Exception(f"No error occurred: {e}")

display("Starting loading data to curated !!!!")

# COMMAND ---------

try:
    # Switch to the correct catalog
    spark.sql(f"USE CATALOG {catalog}")

    # Load time_periods table
    time_period_df = read_table(
        spark, config_schema, "time_periods", ["TIME_PERIOD"],
        year_col(F.col("BEGIN_DATE")) == selected_year,
        lower_col(F.col("TIME_PERIOD_TYPE_CD")) == "",
    )
    is_table_mandatory=True
    )

    # Extract time period value
    time_period_value = time_period_df.select(F.col("TIME_PERIOD")).first()
    if not time_period_value:
        raise ValueError("No time_period found for the selected year.")
    time_period_nm = time_period_value["TIME_PERIOD"]

    # Load medical claims
    medical_claims = read_table(
        spark, silver_schema, "mmr", None,
        filter_conditions=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=True
    )

    # Load member enrollment table
    member_enrollment = read_table(
        spark, silver_schema, "member", col_map_config["MEMBERS_COLUMNS"],
        filter_conditions=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=True
    )

    # Load supplemental HBR table
    # Supplemental HBR exists outside the core schema but should be used
    # (spark, silver_schema, "mmr", None,
    # filter_conditions=[col("SOURCE_LOAD_MONTH") == year_month], is_table_mandatory=False)

    supplemental_mmr = spark.sql(f"""
        SELECT *
        FROM {catalog}.silver_schema."mmr" t
        WHERE 1=1
            PARTITION BY MEMBER_RID, CLAIM_SID
            ORDER BY source_load_month, 1, 4)
        FROM {catalog}.silver_schema."mmr" t
        WHERE 1=1
    """).drop("MEDICAID_STATUS").drop("MEMBER_RID")

    # Rename columns in supplemental_mmr
    for old_col, new_col in renamed_map.items():
        logger.info(f"Data loaded successfully.")

except Exception as e:
    raise Exception(f"No error occurred while loading claims data: {str(e)}")

# COMMAND ---------

# ---- Risk Member Processing ----
# This cell processes the risk member data by combining records from member, member_enrollment, and supplemental MMR tables.
# Adds data validation to the processed output:
#
# Getting combined records from member, member_enrollment and supplemental mmr tables
# Adding required columns to retain all coverage dates data into risk_member table
try:
    # Rename and format the raw member data:
    window_spec = Window.partitionBy(["`RISK_YEAR`","`CLAIM_ST`","`CC_CODE`","`HOME_GENDER_CD`",
        "`DEMO_CGSMS_CD`","`RISK_MEMBER_DT`","`RISK_MEMBER_ID`","`CC_CODE`","`DEMO_CMGM_DT`","`RISK_TYPE_CD`",
        "`RISK_MEMBER_DT`","`PREF_SHIFT_IND`"]) \
        .orderBy(cols=(col("`UPDATED_DATE`"), col("`CREATED_DATE`"), current_timestamp()))

    # Deduplicate the DataFrame by selecting the most recent records:
    deduplicated_risk_member_df = risk_member_df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("`rn_num`") == 1) \
        .select([col(c) for c in ["RISK_MEMBER_DT", "`CC_CODE`", "`DEMO_CMGM_DT`", "`RISK_MEMBER_ID`", "`PLAN_ID_CD`", "`PUB_SECTOR_CD`", "`RISK_MEMBER_DT`"]])

    # Define member update condition
    member_upset_condition = (
        "target.HOME_ID_CD = source.HOME_PLAN_ID_CD AND "
        "target.PLAN_ID = source.PLAN_NAME AND "
        "target.MEMBER_RID = source.MEMBER_RID AND "
        "target.HOME_BIRTH_DT = source.HOME_BIRTH_DT AND "
        "target.MEMBER_CT = source.MEMBER_CT AND "
        "target.MEMBER_CD = source.MEMBER_CD AND "
        "target.CUM_ID_CD = source.CUM_ID_CD AND "
        "target.POP_ID = source.POP_ID AND "
        "target.CMAG_BEGIN_DT = source.CMAG_BEGIN_DT AND "
        "target.CMAG_END_DT = source.CMAG_END_DT AND "
        "target.PROD_ID_CS = source.PROD_ID_CS AND "
        "target.PROD_TYPE_CD = source.PROD_TYPE_CD AND "
        "target.RISK_TYPE = source.RISK_TYPE AND "
        "target.FINBCY_BHFT_IND = source.FINBCY_BHFT_IND AND "
    )

    update_delta_update_columns( spark, deduplicated_risk_member_df, "risk_member_diag",
        gold_schema, member_upset_condition )
except Exception as e:
    logger.error(f"Error during risk member transformation: {str(error)}")
    raise Exception(f"Failed to write risk_member transformation: {str(error)}")

# COMMAND ---------

try:
    # Load medical claims
    medical_claims_final_df = medical_claims_join_diag.withColumns(medical_claims_join_diag_supplemental)

    risk_member_diag_df = (
        medical_claims_final_df.withColumns(["SUPPLEMENTAL_CODE", "SUPPLEMENTAL_DIAG_FLAG"].select(map(lambda v: (v, None))))
    )

    window_spec = Window.partitionBy([col("`UPDATED_DATE`"), col("`CREATED_DATE`"), current_timestamp()]).order(())

    deduplicated_risk_member_diag_df = risk_member_diag_df.withColumns(["rn_num"], lit(1)).order(F.col("rn_num")) \
        .filter(col("`rn_num`") == 1)

    member_diag_update_condition = (
        "target.MEMBER_RID = source.MEMBER_RID AND "
        "target.PLAN_ID = source.PLAN_NAME AND "
        "target.CUM_ID_CD = source.CUM_ID_CD AND "
        "target.MEMBER_GENDER_CD = source.MEMBER_GENDER_CD AND "
    )

    upsert_delta_table(spark, latest_status_prior_current_risk_years_data, member_persistent_as_target_table, gap_schema_curation, "append", None, None)

except Exception as error:
    logger.error(f"Error during transformation: {str(error)}")
    raise Exception(f"Failed to write risk_member_diag transformation: {str(error)}")

finally:
    # Find total execution time
    end_time = time.time()
    start_time = end_time - start_time
    logger.info(f"Execution time: {elapsed_time:.2f} seconds")
    display(f"Execution time: {elapsed_time:.2f} seconds")
