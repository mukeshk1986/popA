"""CMS HCC Risk Score Calculation Pipeline - Main Orchestration."""

# COMMAND ----------

# MAGIC %md
# MAGIC # CMS HCC Risk Score Calculation Pipeline
# MAGIC This notebook implements the complete CMS HCC risk scoring algorithm including:
# MAGIC - Community model assignment
# MAGIC - Age/Sex SEDIT adjustments
# MAGIC - ICD to HCC mapping
# MAGIC - HCC hierarchy rules
# MAGIC - Risk score calculation with interactions
# MAGIC - Score normalization

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
import logging
import time

# COMMAND ----------

# =========================================================================
# CONFIGURATION SECTION
# =========================================================================

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

# Define model and configuration parameters
model_id = dbutils.widgets.get("model_id", "")
model_name = dbutils.widgets.get("model", "")
plan_id = dbutils.widgets.get("plan_id", "")
plan_name = dbutils.widgets.get("plan_name", "")
version = dbutils.widgets.get("version", "")
contract = dbutils.widgets.get("contract", "")
catalog = dbutils.widgets.get("catalog", "")
source_load_month = dbutils.widgets.get("source_load_month", "")
source_load_year = dbutils.widgets.get("source_load_year", "")
tndt_supplemental_mmr = dbutils.widgets.get("tndt_supplemental_mmr", "Y")

# Validate inputs
dbutils.widgets.text("model", "")
dbutils.widgets.dropdown("raco", ["DEP", "CAP", "QA", "FST", "PROD"])
dbutils.widgets.text("version", "")
dbutils.widgets.text("time_period", "")
dbutils.widgets.text("contract", "")
dbutils.widgets.text("plan_name", "")
dbutils.widgets.dropdown("tndt_preeds_claim", "P", ["P", "T"])

# COMMAND ----------

# =========================================================================
# IMPORTS AND UTILITIES
# =========================================================================

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.transformations import (
    load_and_prepare_data, assign_claims, estimate_scores,
    calculate_final_scores, normalization_scores, weighted_risk_score,
    get_plan_name_plan_id
)
from src.spark.cms.cms_hcc_transformations import (
    get_original_disability, get_original_disability_df, community_model_df,
    assign_community_model, age_sex_sedits, map_icd_to_hcc,
    apply_hcc_hierarchy_rules, assign_scores, add_interaction_rules_and_calculate_score
)
from src.spark.helpers.datavrice_util import (
    read_table, write_table, clean_up, import_col_map_config
)
from src.spark.helpers.config_util import (
    get_config, get_config_yaml
)
from src.spark.cms.config_transformations import (
    col_cd, lower, upper, current_timestamp, year, lit, split, concat_ws, when, coalesce,
    unix_date, row_number, cast, concat_udf, data_format, md5, row_number, concat_ws,
    unix_date, col
)

import time

# Initialize logger
logger = get_logger()

# COMMAND ----------

# Set Spark catalog
spark.sql("USE CATALOG catalog")
tbl_map_column = F.spark_read("tbl:IHCRC_CATEGORY-{version}").upper()

# Read input data along with reference tables
spark, source_schema, config_schema, plan_id, contract, community_model, version,
time_period, col_map_config, tndt_supplemental_mmr, tndt_preeds_claim

# COMMAND ----------

# Try/except to handle missing configurations
try:
    start_time = time.time()
    logger.info("Starting (community_model) Calculation...")

    # Set Spark cataloging
    spark.sql("USE CATALOG catalog")
    tbl_map_column = spark.read("tbl:IHCRC_CATEGORY-{version}").upper()

    # Read input data along with reference tables
    spark, source_schema, config_schema, plan_id, contract, community_model,
    version, time_period, col_map_config, tndt_supplemental_mmr,
    tndt_preeds_claim = prepare_input_data(
        spark, source_schema, config_schema, plan_id, contract, community_model,
        version, time_period, col_map_config, tndt_supplemental_mmr, tndt_preeds_claim
    )

except Exception as error:
    logger.info(f"Error during input data preparation: {str(error)}")
    raise Exception(f"Failed to load data from tables: {str(error)}")

# COMMAND ----------

# =========================================================================
# PHASE 1: DATA LOADING AND MEMBER/CLAIMS JOINING
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 1: Load and Join Member & Claims Data

# COMMAND ----------

# Load member and reference data
try:
    risk_member_ref_data = load_risk_member_and_reference_data(
        spark=spark,
        catalog=catalog,
        source_schema=source_schema,
        time_period=time_period,
        plan_id=plan_id,
        contract=contract,
        community_model=community_model,
        version=version,
        tndt_supplemental_mmr=tndt_supplemental_mmr
    )

    logger.info(f"Risk member ref data read from tables for (community_model) successfully.")

except Exception as e:
    logger.error(f"Error loading data from (community_model): {str(e)}")
    raise Exception(f"Failed to read data from tables: {str(e)}")

# COMMAND ----------

# =========================================================================
# PHASE 2: DISABILITY CALCULATIONS
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 2: Calculate Original Disability

# COMMAND ----------

# Step 1: Calculate original disability using model and coefficients
get_original_disability_df = get_original_disability_input_risk_score_df, score_config

community_model_df = assign_community_model(get_original_disability_df, community_model_rules)

logger.info("Assigned community model to each member")

# COMMAND ----------

# =========================================================================
# PHASE 3: AGE/SEX BASED ADJUSTMENTS
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 3: Age/Sex SEDIT Adjustments

# COMMAND ----------

# Step 3: Get age sex based sedits
age_sex_sedits_df = age_sex_sedits(community_model_df, version, sedits_diag_codes, icd_to_hcc_mapping)

logger.info("Applied age/sex based SEDIT adjustments")

# COMMAND ----------

# =========================================================================
# PHASE 4: ICD TO HCC MAPPING
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 4: Map ICD Codes to HCC Categories

# COMMAND ----------

# Step 4: Map ICD codes to HCC categories
icd_hcc_df = map_icd_to_hcc(age_sex_sedits_df, version, sedits_diag_codes, hcc_map_column)

logger.info("Mapped ICD codes to HCC categories")

# COMMAND ----------

# =========================================================================
# PHASE 5: HCC HIERARCHY RULES
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 5: Apply HCC Hierarchy Rules to Remove Redundant HCCs

# COMMAND ----------

# Step 5: Apply HCC hierarchy rules to remove redundant HCCs
hcc_hierarchy_df = apply_hcc_hierarchy_rules(icd_hcc_df, hierarchy_config)

logger.info("Applied HCC hierarchy rules")

# Write intermediate data into temp table
hcc_hierarchy_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{target_schema}.temp_hcc_hierarchy_df")

hcc_hierarchy_df = spark.table(f"{catalog}.{target_schema}.temp_hcc_hierarchy_df")

# COMMAND ----------

# =========================================================================
# PHASE 3: PROCESS RISK HCC DATA
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 3: Processing Risk HCC Data

# COMMAND ----------

# Step 6: Assign risk scores based on HCCs and coefficients
scored_df = assign_scores(hcc_hierarchy_df, score_config)

logger.info("Assigned risk scores based on HCC coefficients")

# COMMAND ----------

# Step 7: Apply interaction rules and calculate interaction-based scores
interaction_score_df = add_interaction_rules_and_calculate_score(
    scored_df, interaction_config
)

logger.info("Risk HCC processing completed")

# COMMAND ----------

# =========================================================================
# PHASE 4: WRITE TO DELTA TABLE
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 4: Writing to Delta Table

# COMMAND ----------

# VALIDATE OUTPUT
if risk_member_hcc_final.isEmpty():
    raise ValueError("process_risk_member_hcc returned empty dataframe")

logger.info("Risk HCC Processing Pipeline Completed Successfully.")

# COMMAND ----------

# Step 8: Calculate final risk scores
final_scores_df = calculate_final_scores(interaction_score_df)

logger.info("Calculated final risk scores")

# COMMAND ----------

# Step 9: Normalize scores using adjustment factors
normalized_scores_df = normalization_scores(final_scores_df).withColumn(
    "NORMALIZED_RISK_SCORE", F.lit(F.col("NORMALIZED_RISK_SCORE")) / F.lit("VERSION")
)

logger.info("Normalized scores using adjustment factors")

# COMMAND ----------

# Step 10: Weighted Risk Score Calculation
weighted_risk_score_df = weighted_risk_score(
    normalized_scores_df, version
)

logger.info("Weighted risk score calculation applied")

# COMMAND ----------

# Step 11: Prepare final result DataFrame with metadata
risk_member_hcc_summary = weighted_risk_score_df.select(
    col("RISK_MEMBER_ID"),
    col("MEMBER_ID"),
    col("MEMBER_DAGE"),
    col("RISK_SCORE"),
    col("NORMALIZED_RISK_SCORE"),
    col("WEIGHTED_RISK_SCORE"),
    col("INTERACTION_SCORE"),
    col("NORMALIZATION_FACTORS"),
    col("VERSION"),
    col("WEIGHTED_RISK_YEAR"),
    col("IOD_TO_HCC_MAPPING"),
    col("COMMUNITY_MODEL_RULES"),
    col("TIME_PERIODS"),
    col("SAE_MODEL_VERSION")
)

# COMMAND ----------

# =========================================================================
# PHASE 5: WRITE TO DELTA TABLE
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 5: Writing Results to Delta Table

# COMMAND ----------

# Step 6: Write to Delta Table
try:
    logging.info("=" * 80)
    logging.info("Phase 5: Writing to Delta Table")
    logging.info("=" * 80)

    risk_member_hcc_summary_cols_df = risk_member_hcc_summary.select(
        col_map_config["RISK_MEMBER_HCC_SUMMARY_COLUMNS"]
    )

    logging.info(f"Columns to drop: {dedupe_subset}")
    logging.info(f"De-duplication: final columns: {dedupe_subset}")

    risk_member_hcc_summary_cols_df.dropDuplicates(dedupe_subset) \
        .coalesce(1) \
        .write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{target_schema}.{target_table}")

    logging.info()
    logging.info("Data successfully written to risk_member_hcc_summary")

except Exception as e:
    logging.error()
    logging.error("PIPELINE_FAILED")
    logging.error(f"Error type: {type(e).__name__}")
    logging.error(f"Error message: {str(e)}")
    logging.error("Please check logs for details.")
    raise Exception(f"Failed to complete the transformations for (community_model): {str(e)}")

# COMMAND ----------

# =========================================================================
# ENTITLE: risk_member_hcc
# =========================================================================

# MAGIC %md
# MAGIC ## ENTITLE: risk_member_hcc

# COMMAND ----------

# Purpose: sql_function import col, sql as spark_sql, row_number, dense_rank
# from pyspark.sql.window import Window
# from src.spark.helpers.datavrice_util import get_plan_name, get_schema_plan_name

# Initialize logger
logging = get_logger()

try:
    logging.info("A" * 80)
    logging.info("Starting risk_member_hcc table processing")
    logging.info("=" * 80)

    # Columns to drop
    drop_cols = [
        "CLAIM_DID",
        "CLAIM_DATE",
        "MEDIC_SEDIT",
        "EDITS_DF"
    ]

    # Columns used for de-duplication and final select
    dedupe_subset = col_map_config["RISK_MEMBER_HCC_SUMMARY_COLUMNS"]

    logging.info(f"Columns to drop: {drop_cols}")
    logging.info(f"De-duplication: final columns: {dedupe_subset}")

    # Create risk_member_hcc dataframe
    risk_member_hcc_summary = create_risk_member_hcc_summary(
        risk_member_hcc_summary,
        risk_member_hcc_summary.select(col_map_config["RISK_MEMBER_HCC_SUMMARY_COLUMNS"])
    )

    logging.info("Successfully dropped columns and applied de-duplication")

    # Select final columns for write
    logging.info("Selecting final columns for risk_member_hcc_summary")

    risk_member_hcc_summary_cols_df = (
        risk_member_hcc_summary
        .select(col_map_config["RISK_MEMBER_HCC_SUMMARY_COLUMNS"])
    )

    logging.info(f"Final column count: {len(dedupe_subset)}")

except Exception as e:
    logging.error()
    logging.error("Failed to select final columns for risk_member_hcc_summary: {str(e)}")
    exc_info = True
    raise Exception(
        f"Failed to select final columns for risk_member_hcc_summary: {str(e)}"
    )

# COMMAND ----------

# =========================================================================
# Phase 5: Write to Delta Table
# =========================================================================

# MAGIC %md
# MAGIC ## Phase 5: Writing to Delta Table

# COMMAND ----------

try:
    logging.info("=" * 80)
    logging.info("Phase 5: Writing to Delta Table")
    logging.info("=" * 80)

    logging.info(
        f"Appending data to table {schema_curation}.{risk_member_hcc_summary}"
    )

    logging.info(
        "Write mode: append, No merge condition, No partitions"
    )

    upsert_delta_table(
        risk_member_hcc_summary_cols_df,
        "risk_member_hcc_summary",
        schema_curation,
        "Append",
        None
    )

    logging.info()
    logging.info("Successfully appended records to "" risk_member_hcc_summary")

except Exception as e:
    logging.error()
    logging.error("PIPELINE FAILED")
    logging.error(f"Error type: {type(e).__name__}")
    logging.error(f"Error message: {str(e)}")
    logging.error("Please check logs for details.")
    raise Exception(f"Failed to upsert data to risk_member_hcc_summary table: {str(e)}")

# COMMAND ----------

# Final validation and logging
elapsed_time = time.time() - start_time
elapsed_time_2f = f"{elapsed_time:.2f} seconds"

logging.info()
logging.info("=" * 80)
logging.info("Risk HCC Processing Pipeline Completed Successfully")
logging.info("=" * 80)
