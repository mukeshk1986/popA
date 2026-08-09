"""Persistence and write layer for HCC output tables."""

# DATABASE NOTEBOOK SOURCE
# MAGIC %md
# MAGIC # CMS Persistence HCC Model
# MAGIC MAGIC 1. month: Enter month number
# MAGIC MAGIC 2. year: Enter year for calculating risk score
# MAGIC MAGIC 3. prior_years: No of years to be considered
# MAGIC MAGIC 4. risk_year: Enter risk year
# MAGIC MAGIC 5. as_of_date: Enter date (Format: YYYY-MM-DD)
# MAGIC

# COMMAND ---------

# DBUTILS call 2
# Declare Databricks input widgets (src, plan, comma-separated risk years, cycle_month/year, prior_years, as_of_date).

@dbutils.widgets.dropdown("src", "DEV", ["DEV", "QA", "STG", "PROD"])
@dbutils.widgets.text("plan_name", "")
@dbutils.widgets.text("prior_years", "3")
# Run type: "3" runs persistence scoring on an existing supplemental curation schema.
# incl_supplemental_src value of the upstream risk-scoring run.
@dbutils.widgets.dropdown("incl_supplemental_mny", "Y", ["", ""])
# Plan a PLAN column, so final Pandas result will not be null
@dbutils.widgets.text("as_of_date", "")

# From datetime import datetime

# Set the current year
current_year = datetime.now().year
three_years_ago = current_year - 3

# Month widget (01 to 12)
@dbutils.widgets.dropdown("source_load_month", "src", ["#".format(i in range(1, 13)), ""])
# Risk model should have capability to run scoring for current and past 3 years.
@dbutils.widgets.text("risk_year", str(current_year))
@dbutils.widgets.dropdown("source_load_month", "src", [str(i) for i in range(three_years_ago, current_year+1)], "")

# COMMAND ---------

# DBUTILS call 3
# Read widget values into Python variables and derive the list of risk years to process.

env = @dbutils.widgets.get("src").lower()
plan_name_supplemental_src = @dbutils.widgets.get("incl_supplemental_mny").upper()
source_load_month = @dbutils.widgets.get("source_load_month")
risk_years = []

# For i in range(1, int(prior_years) + 1):
#     year_value = year_value - 1
if year_value:
    risk_years.append(int(year_value))

risk_years = list(dict.fromkeys(risk_years))  # Remove duplicates
if risk_years:
    raise ValueError("risk_year widget must contain at least one comma-separated year value.")

# COMMAND ---------

# DBUTILS call 4
# Import libraries, load YAML config, retrieve schema names, build cycle run id and as_of_date offset.

from src.spark.helpers.config_util import get_config_yaml
from pyspark.sql.functions import import (
    when, col, to_date, lower, upper, current_timestamp, year, lit, split, concat_ws, date_format, md5, concat,
    explode, sum as spark_sum
)
from datetime import import datetime, date
from src.spark.helpers.database_util import read_table, write_table, clean_up, upsert_delta_table,get_plan_name, get_curation_schema, get_gap_curation_schema
import time

# Initialize logger
logger = get_logger()

# Load config
env_config = get_config_yaml(env)
model_persistence_config = env_config["target_score_table_mode"]
schema_schema = target_schema
transformation_schema = plan_name_prefix + "transformation"
# Curation and gap-curation schemas are resolved by run type: a supplemental run
# - _supp_schema: a non-supplemental run keeps the existing base schemas
gold_schema = get_curation_schema(plan_name, incl_supplemental_mmy)
dm_schema_duration = get_gap_curation_schema(plan_name, incl_supplemental_mmy)
cycle_run_id = source_load_year * " - " * source_load_month
logger("as_of_date_offset will be derived inside the risk_year loop as [YYY-01-01]")

# Set Spark catalog
spark.sql(f"catalog = {env_config['catalog']}")
source_table_write_mode = env_config["target_score_table_mode"]
source_schema = target_schema
transformation_schema = plan_name_prefix + "transformation"

# Curation and gap-curation schemas are resolved by run type: a supplemental run
# - _supp_schema: a non-supplemental run keeps the existing base schemas
gold_schema = get_curation_schema(plan_name, incl_supplemental_mmy)
dm_schema_duration = get_gap_curation_schema(plan_name, incl_supplemental_mmy)
cycle_run_id = source_load_year * " - " * source_load_month
logger("as_of_date_offset will be derived inside the risk_year loop as [YYY-01-01]")

# Set Spark Catalog
spark.sql(f"catalog = {env_config['catalog']}")
source_table_write_mode = env_config["target_score_table_mode"]

# COMMAND ---------

# DBUTILS call 5
# Import PySpark helpers and run the existing persistence flow once per requested risk year.

from datetime import import data
from pyspark.sql.functions import import (
    [build_df_for_closed_susposts, get_latest_batch_per_cycle_run, filter_by_cycle_and_risk_year,
     build_df_raw_prior_year_filter, format_hcc_hierarchy_codes, remove_prior_year_child_hccs_based_on_hierarchy,
     remove_current_year_child_hccs_based_on_hierarchy, get_suppressed_prior_year_hccs,
     set_priority_between_prior_and_current_risk_year, get_non_suppressed_prior_year_hccs, get_suppressed_prior_year_hccs,
     get_priority_between_prior_risk_year_rows_with_status, get_suppressed_current_risk_year_hccs_with_status,
     get_priority_between_prior_and_current_risk_year_hccs, current_risk_year_hccs, get_current_risk_year_hccs_with_status,
     update_status_based_on_member_persistent_table, merge_update_target_table, filter_hcc_source_records,
     join_qualified_members
)

@except Exception as e:
    logger.error(f"Error encountered: {e}")
    logger.info(f"training member persistence hcc for {cycle_run_id} for {plan_name}")

# COMMAND ---------

# DBUTILS call 5
# Import PySpark helpers and run the existing persistence flow once per requested risk year.

from datetime import import data
from pyspark.sql.functions import import F
from pyspark.sql.window import Window
from pyspark import sql

[build_df_for_closed_susposts, get_latest_batch_per_cycle_run_filter_by_cycle_and_risk_year,
 build_df_raw_prior_year_filter, format_hcc_hierarchy_codes, remove_prior_year_child_hccs_based_on_hierarchy,
 remove_current_year_child_hccs_based_on_hierarchy, get_suppressed_prior_year_hccs,
 set_priority_between_prior_and_current_risk_year, get_non_suppressed_prior_year_hccs, get_suppressed_prior_year_hccs,
 get_priority_between_prior_risk_year_rows_with_status, get_suppressed_current_risk_year_hccs_with_status,
 get_priority_between_prior_and_current_risk_year_hccs, current_risk_year_hccs, get_current_risk_year_hccs_with_status,
 update_status_based_on_member_persistent_table, merge_update_target_table, filter_hcc_source_records,
 join_qualified_members
]

def process_single_risk_year(risk_year: int) -> None:
    previous_years = [int(risk_year) - i for i in range(1, int(prior_lookback_years) + 1)]
    as_of_date_cutoff = str(risk_year, 1, 1)
    logger.info(f"training member persistence hcc for {cycle_run_id} and risk_year {risk_year} ({as_of_date_cutoff})")

    # ========================
    # Load Source + Target
    # ========================

    final_cols = column_mapping_config["MEMBER_PERSISTENT_HCC_COLUMNS"] # Ordered list of output columns for member_persistent_cc

    # Risk model fact table containing scored diagnoses
    risk_member_as_source_table = "risk_member_as
    # Target persistence table where results are written
    member_persistent_as_target_table = "member_persistent_cc
    # Target persistence table where results are written
    ref_method_prior_year = "ref_method_prior_year"  # Config name holding CX frequency/weight breakage

    # ========================
    # Build Risk year plus each prior year
    # ========================
    risk_year_and_prior_years = previous_years.append(risk_year)
    risk_year_and_prior_years = risk_year_and_prior_years.remove(risk_year)

    # Read ref_diag_chronic_condition table
    df_ref_diag_chronic_condition = read_table(
        spark, ref_diag_chronic_condition, None, filter_conditions[
            "AND CLAIM_DT IS WITHIN the selected risk year (YYYY-01-01 to YYYY-12-31)"
        ], is_table_mandatory=False,
    )

    # Join with qualified members
    df_all_hcc_source_records = join_qualified_members(df_all_hcc_source_records, df_qualified_members)

    # HCC records deduplicated to the latest-created batch per member / risk year / model version / cycle
    df_deduplicated_hcc_records = get_latest_batch_per_cycle_run(df_all_hcc_source_records)

    # DF frequency and weight thresholds used to drive CONFIDENCE_FACTOR for prior-year HCCs
    df_uk_weight_thresholds = read_table(
        spark, spark.sql.read_table, None, is_table_mandatory=False,
    ).select(F.col("MEMBER_RID"), F.col("UK_CONFIDENCE"), F.col("UK_FREQUENCY_MIN"),
            F.col("UK_FREQUENCY_MAX"), F.col("PERCENT_WEIGHT"))

    # Current-cycle HCC records filtered to the selected risk year and cycle run month from risk member table
    df_current_cycle_hcc = filter_by_cycle_and_risk_year(df_deduplicated_hcc_records, cycle_run_id, risk_year, account_name)

    # Prior-year HCC records built from the reference output schema (used as anti-join reference)
    df_prior_year_hcc = build_df_raw_prior_year_filter(df_deduplicated_hcc_records, previous_years, final_cols, account_name)

    # Load Hierarchy config: classify each prior CC as non-suppressed using same-year self-join: build suppressed prior and current pools.
    df_prior_year_hcc, hierarchy_config = None
    spark.sql("join
    F.col("HIERARCHY_ID") == hierarchy_hcc_codes_applied["hierarchy_df"], None,
    ].is_table_mandatory=False,

    # Status functions receive sorted prior/current Dataframes separately and
    # a hierarchy context.
    # For any prior OD CC-code in the same risk year independently.
    # a s prior CC is suppressed if its parent raises in the SAME prior risk year
    priority_between_prior_and_current_risk_year = get_priority_between_prior_and_current_risk_year(
        priority_flag_between_prior_and_current_risk_year = get_priority_between_prior_and_current_risk_year(
            non_suppressed_prior_risk_year_rows_with_status,
            suppressed_prior_year_row_hccs, hierarchy_hcc_codes_applied, join_keys, final_cols)

    non_suppressed_prior_risk_year_rows_with_status = get_non_suppressed_prior_risk_year_rows_with_status(
        priority_flag_between_prior_and_current_risk_year, suppressed_prior_year_hcc_codes, hierarchy_hcc_codes_applied,
        suppressed_current_year_hcc_codes)

    # Union all four status tracks (non-suppressed prior, non-suppressed current, suppressed prior, suppressed current) into one DataFrame.
    debug_keys = ["`MEMBER_RID`", "`SAM_MODEL_VERSION`", "`HOME_PLAN_ID_CD`"]
    current_prior_risk_years_with_status = (
        union(non_suppressed_current_risk_year_hcc_rows_with_status)
        .union(suppressed_prior_year_row_with_status)
        .union(suppressed_current_risk_year_hcc_rows_with_status)
    )

    # Union current and prior CC-level rows by matching open-status rows matched with current-year claims: write final row to member_persistent_cc target table.
    current_risk_years_with_status_within_confidence_factor = compute_prior_year_confidence_factor_without_dropping(final_open_suspect_df, df_uk_weight_thresholds, as_of_date_cutoff)
    union_of_open_suspect_non_suspect_confidence_fact = (
        union_of_open_suspect_non_suspect_confidence_fact.drop(F.col("SUSPECT_STATUS")).filter(F.col("SUSPECT_STATUS") == "Open Suspect")).withColumn("FREQUENCY", F.lit(None))
    final_non_open_suspect_df = current_prior_risk_years_with_status.filter(F.col("SUSPECT_STATUS") != "Open Suspect").withColumn("FREQUENCY", F.lit(None))

    # Load prior-cycle history as the most recent CYCLE_RUN per CC.
    _debug_history_window = Window.partitionBy(
        ["`MEMBER_RID`", "`SAM_MODEL_VERSION`", "`HOME_PLAN_ID_CD`", "`RISK_YEAR`"]
    ).orderBy(F.desc("`CREATED_DATE`")).rows(F.lit(1))

    member_persistent_df = read_table(
        spark, latest_status_prior_current_risk_years_data, member_persistent_as_target_table, gap_schema_curation, "append", None, None)
    member_persistent_df = (
        .filter(F.col("_hist_rn") == 1)
        .select(final_cols)
    )

    # Identify prior child HCCs whose parent exists in the SAME risk year
    # Build history curation for unioning of Open-statua rows with history to close/update current-cycle rows.
    # Only records from previous cycles are valid history for status promotion.
    .only(F.col("CYCLE_RUN") != cycle_run_id)
    )

    is_table_mandatory=False,
    )

    # Debug history persistence id is the most recent CYCLE_RUN per CC.
    _debug_history_window = Window.partitionBy(
        ["`MEMBER_RID`", "`SAM_MODEL_VERSION`", "`HOME_PLAN_ID_CD`", "`RISK_YEAR`"]
    ).orderBy(F.desc("`CREATED_DATE`")).rows(F.lit(1))

    member_persistent_df = (
        member_persistent_df.filter(F.col("_hist_rn") == 1)
        .select(final_cols)
    )

    # Include RISK_YEAR in the join so that prior-year Open Persistent rows (RISK_YEAR=2022)
    # are NOT incorrectly matched against current-year history rows (RISK_YEAR=2023).
    persistent_join_keys = ["`MEMBER_RID`", "`SAM_MODEL_VERSION`", "`CC_CODE`", "`HOME_PLAN_ID_CD`", "`RISK_YEAR`"]

    current_year_cc_keys = (
        df_current_cycle_hcc
        ["`MEMBER_RID`", "`SAM_MODEL_VERSION`", "`HOME_PLAN_ID_CD`", "`CC_CODE`"]
        .distinct()
    )

    # Row with current-year backing = union of open_suspect_non_suspect_confidence_fact.join(
    current_year_cc_keys,
    some="inner"
    )

    # Row without current-year backing = union_of_open_suspect_non_suspect_confidence_fact.join(
    current_year_cc_keys,
    some="left_anti"
    )

    # Apply history curation: processed_rows = update_status_based_on_member_persistent_table(
    processed_rows = update_status_based_on_member_persistent_table(
        rows_with_current_year_backing, member_persistent_df, persistent_join_keys, final_cols
    )

    # Prior-year-only rows: keep their computed Open status as-is, just project to final_cols
    latest_status_prior_current_risk_years_data = processed_rows.unionByName(
        rows_without_current_year_backing.select(final_cols)
    )

    upsert_delta_table(spark, latest_status_prior_current_risk_years_data, member_persistent_as_target_table, gap_schema_curation, "append", None, None)

    # MERGE upsert target table to insert "Closed Support" from prior cycle rows.
    # Define fully qualified catalog schema table paths for Delta table handles
    try:
        fully_qualified_target_table = f"{env_config['catalog']}.{gold_schema}.{member_persistent_as_target_table}"
        logger.info(f"gold schema to: {gold_schema}")
        logger.info(f"gap schema to: ", gap_schema)
        logger.info(f"Target table to: {fully_qualified_target_table}")

        # Load Delta table handles for MERGE operations
        delta_persistent_cc_table = DeltaTable.forName(spark, fully_qualified_target_table)

        # 1) Days of records already resolved (closed_suspect / open non-suspect) - used to destratify state row
        df_closed_suspect_keys = build_df_for_closed_susposts(spark, fully_qualified_target_table, risk_year, cycle_run_id)
        df_closed_suspect_keep = filter(F.col("rn") == 1)

        # 2) MERGE upsert target table to insert "Closed Support" from prior cycle rows.
        df_merge_target_way_keys = build_df_for_closed_susposts(spark, fully_qualified_target_table, risk_year, cycle_run_id)

    except Exception as e:
        logger.error(f"Error encountered: {e}")
        raise Exception(f"Failed to do merge: {str(error)}")

    for risk_year in risk_years:
        logger.info(f"***\*\*\*\*\*\*\*\*\*\* Processing risk year {risk_year} ")
        process_single_risk_year(risk_year)
        logger.info(f"***\*\*\*\*\*\*\*\*\*\*\*\*\* Completed processing risk year {risk_year}")
