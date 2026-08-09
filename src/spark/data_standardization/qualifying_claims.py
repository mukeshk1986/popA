"""Qualifying Claims Processing - Data Standardization Pipeline."""

from datetime import datetime
from typing import List, Optional, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    array, array_zip, explode, months_between, floor, to_date, lit, year, concat_ws,
    broadcast, col, max as spark_max, min as spark_min, when, coalesce, lower, row_number
)

# Get the current year
current_year = datetime.now().year
three_years_ago = current_year - 3

# COMMAND ---------

from src.spark.helpers.database_util import (
    read_table, write_table, create_dataframe, clean_up,
    update_table, upsert_delta_table, upsert_delta_update_columns
)

import yaml
import re
from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logging_util import get_logger
import time

# Initialize logger
logger = get_logger()

try:
    start_time = time.time()
    logger.info("Starting Qualifying Claims ...")

    # Load configuration
    config = get_config_yaml(f"../../../config/environments/values.yaml")
    col_map_config = get_config_yaml(f"../../../config/sql/file_read_meta.yaml")

    reference_schema = config.get("config_schema")
    account_name = "current_user"
    year_month = "2025-01"
    logger.info(f"Month Year: ({year_month})")

except Exception as e:
    logger.error(f"Unexpected error occurred: {str(e)}")
    raise Exception(f"Failed to load : {str(e)}") from e

# COMMAND ---------

from src.spark.data_standardization.qualifying_transformation import (
    join_facility_and_enrollment, filter_outpatient_claims, filter_inpatient_claims,
    process_professional_claims
)

try:
    # ========================================================
    # STEP 1: Read source tables
    # ========================================================

    facility = read_table(
        spark, "schema_transformation", "FACILITY_HEADERS", None,
        filter_condition=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False
    ).select(
        "CREATED_DATE", "CREATED_BY", "UPDATED_DATE", "UPDATED_BY",
        "ANY_REQ_DT", "CLM_ID_CD", "HOME_PLAN_ID_CD", "VALIDATED_TIMESTAMP",
        "LOAD_DATE", "EMS_AMT", "SOURCE_LOAD_MONTH"
    )

    facility_detail = read_table(
        spark, "schema_transformation", "FACILITY_DETAIL", None,
        filter_condition=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False
    ).select("CLAIM_AMT")

    facility_diag = read_table(
        spark, "schema_transformation", "FACILITY_DIAG", col_map_config.get("FACILITY_DIAG_COLUMNS"),
        filter_condition=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False
    )

    member_enrollment = read_table(
        spark, "schema_transformation", "MEMBER_ENROLLMENT", col_map_config.get("MEMBER_ENROLLMENT_COLUMNS"),
        filter_condition=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False
    )

    ref_visit_type_rbcs = read_table(
        spark, reference_schema, "REF_VISIT_TYPE_RBCS", col_map_config.get("REF_VISIT_TYPE_RBCS_COLUMNS"),
        is_table_mandatory=False
    )

    ref_place_of_service_fob_df = read_table(
        spark, reference_schema, "REF_PLACE_OF_SERVICE_FOB", col_map_config.get("REF_PLACE_OF_SERVICE_FOB_COLUMNS"),
        is_table_mandatory=False
    )

    # ========================================================
    # STEP 2: Join facility tables and filter by claim type
    # ========================================================

    joined_facility_df = join_facility_and_enrollment(facility, member_enrollment) \
        .filter(col("CLAIM_ID_CD").isin(1, 2))

    # Split inpatient and outpatient
    in_patient = joined_facility_df.filter(col("CLAM_TP_CD") == 1)
    out_patient = joined_facility_df.filter(col("CLAM_TP_CD") == 2)

    # ========================================================
    # STEP 3: Apply filters and process claims
    # ========================================================

    outpatient_claims = filter_outpatient_claims(out_patient)
    inpatient_claims = filter_inpatient_claims(in_patient)

    risk_proc_qualify = read_table(
        spark, reference_schema, "ref_risk_proc_qualifying", None,
        filter_condition=[col("DX_ELIGIBILITY_FLAG") == 1],
        is_table_mandatory=False
    )

    facility_joined_detail_df = facility_detail.join(
        outpatient_claims.union(inpatient_claims),
        on="FACILITY_RID",
        how="left"
    )
    final_facility_claims = facility_joined_detail_df.join(risk_proc_qualify)

    # ========================================================
    # STEP 4: Add audit and metadata columns
    # ========================================================

    facility_df = final_facility_claims \
        .withColumn("BUNDLED", lit("")) \
        .withColumn("POP_UNITS_CD", lit(None).cast("decimal(9,0)")) \
        .withColumn("PLACE_OF_SERV_CD", lit("")) \
        .withColumn("PROGRAM",
            when(col("PCP_ID") == "C00", "C00")
            .when(col("PCP_ID").isin("C1", "C2"), "C1_C2")
            .otherwise("OTHER")) \
        .withColumn("RISK_YEAR", year(col("CLAIM_THRU_DT"))) \
        .withColumn("CREATED_DATE", F.current_timestamp()) \
        .withColumn("CREATED_BY", lit(account_name)) \
        .withColumn("UPDATED_DATE", F.current_timestamp()) \
        .withColumn("UPDATED_BY", lit(account_name)) \
        .withColumn("PLAN_NAME", lit("plan_name")) \
        .withColumn("PROFESSIONAL_BID", lit(""))

    logger.info("Facility claims processing completed successfully.")

except Exception as e:
    logger.error(f"Error occurred during facility claims processing: {str(e)}")
    raise Exception(f"Failed during facility claims processing: {str(e)}") from e

try:
    # ========================================================
    # STEP 1: Read Professional Claims Table
    # ========================================================

    profother = read_table(
        spark, "schema_transformation", "PROFESSIONAL", None,
        filter_condition=[col("SOURCE_LOAD_MONTH") == year_month],
        is_table_mandatory=False
    ).select(
        "CREATED_DATE", "CREATED_BY", "RISK_SERV_DT",
        "LAST_SERV_DT"
    )

    # ========================================================
    # STEP 2: Process non-pseudo claims and union with pseudo claims
    # ========================================================

    final_profother_df = process_professional_claims(
        profother, member_enrollment, risk_proc_qualify
    )

    # ========================================================
    # STEP 3: Add audit and metadata columns
    # ========================================================

    profother_df = final_profother_df \
        .withColumn("BUNDLED", lit("")) \
        .withColumn("DIGCOM_STU_CD", lit("")) \
        .withColumn("CP_OF_BILLED_CD", lit("")) \
        .withColumn("PBG_CD", lit("")) \
        .withColumn("XOT_UNITS_VAL", lit(None).cast("decimal(9,0)")) \
        .withColumn("ADMIT_SRC_CD", lit("")) \
        .withColumn("ARRTHMD_CD", lit("")) \
        .withColumn("DMNT_PREM_OB", lit(None).cast("date")) \
        .withColumn("TEMS_THRU_DT", lit(None).cast("date")) \
        .withColumn(
            "PROGRAM",
            when(col("PCP_ID") == "C00", "C00")
            .when(col("PCP_ID").isin("C1", "C2"), "C1_C2")
            .otherwise("OTHER")
        ) \
        .withColumn("RISK_YEAR", year(col("LAST_SERV_DT"))) \
        .withColumn("CREATED_DATE", F.current_timestamp()) \
        .withColumn("CREATED_BY", lit(account_name)) \
        .withColumn("UPDATED_DATE", F.current_timestamp()) \
        .withColumn("UPDATED_BY", lit(account_name)) \
        .withColumn("PLAN_NAME", lit("plan_name")) \
        .withColumn("PROFESSIONAL_BID", lit("PROFESSIONAL_BID"))

    logger.info("Professional claims processing completed successfully.")

except Exception as e:
    logger.error(f"Error occurred during profother claims processing: {str(e)}")
    raise Exception(f"Failed during professional claims processing: {str(e)}") from e

# COMMAND ---------

try:
    medical_claims_df = facility_df.select(col_map_config.get("QUALIFIED_FACILITY_COLUMNS")) \
        .unionByName(profother_df.select(col_map_config.get("QUALIFIED_PROFOTHER_COLUMNS"))) \
        .withColumn("PSEUDO_CLAIM_TYPE", lit(""))

    # Enrich with Visit Type ID
    medical_claims_df = (
        medical_claims_df
        .join(
            broadcast(ref_visit_type_rbcs.select("VISIT_TYPE_ID", "ARCCS_CD", "ARCCS_CD_AND_DT", "ARCCS_CD_END_DT")),
            (col("CPT_AND_ARCCS_CD") == col("ARCCS")) &
            (
                ((col("CLM_TP_CD") == "3") &
                (col("LAST_SERV_DT").between(col("ARCCS_CD_AND_DT"), col("ARCCS_CD_END_DT"))))
                |
                ((col("CLM_TP_CD").isin("1", "2")) &
                (col("DMT_THRU_DT").between(col("ARCCS_CD_AND_DT"), col("ARCCS_CD_END_DT"))))
            ),
            how="left"
        )
    )

    # Enrich with Place of Service Code (only for Facility - inpatient/outpatient claims)
    medical_claims_df = (
        medical_claims_df
        .join(
            broadcast(ref_place_of_service_fob_df.select(
                col("TYPE_OF_BILL_CD").alias("REF_POB"),
                col("PLACE_OF_SERVICE_CD").alias("POD_NPRN")
            )),
            col("TYPE_OF_BILL_CD") == col("REF_POB"),
            how="left"
        )
        .withColumn(
            "PLACE_OF_SERV_CD",
            when(col("CLM_TP_CD").isin("1", "2"), coalesce(col("POD_NPRN"), col("PLACE_OF_SERV_CD")))
            .otherwise(col("PLACE_OF_SERV_CD"))
        )
        .drop("REF_POB", "POD_NPRN")
    )

    window_spec = Window.partitionBy("CLAIM_RID", "CLM_LN_NUM", "CLM_TP_CD") \
        .orderBy(coalesce(col("UPDATED_DATE"), col("CREATED_DATE")), F.current_timestamp())

    # Deduplicate the DataFrame
    deduped_medical_claims_df = medical_claims_df.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Define upsert condition
    upsert_condition = (
        "target.CLAIM_RID = source.CLAIM_RID AND "
        "target.CLM_TP_CD = source.CLM_TP_CD AND "
        "target.CLM_LN_NUM = source.CLM_LN_NUM "
    )

    # Perform upsert operation and final data will be loaded into medical_claims table
    upsert_delta_update_columns(
        spark,
        deduped_medical_claims_df,
        "medical_claims",
        "schema_transformation",
        upsert_condition
    )

except Exception as e:
    logger.error(f"Error during deduplication or upsert: {str(e)}")
    logger.error("Upsert failed", exc_info=True)
    raise Exception(f"Failed : {str(e)}") from e

# Log total execution time
end_time = time.time()
duration = end_time - start_time
logger.info(f"Execution time: {duration:.2f} seconds")
