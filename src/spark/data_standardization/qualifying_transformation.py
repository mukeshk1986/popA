"""Qualifying Transformation - Data Standardization and Transformation Pipeline."""

import sys
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import col, when, lit, year
from pyspark.sql import DataFrame

logging = get_logger()

def join_facility_df(facility_df, member_enrollment):
    """
    Join facility and facility_detail tables on FACIL_HOM_BID.
    Dedup the joined dataframe on logs at error if the join fails.
    """
    try:
        joined_df = facility_df\
            .join(member_enrollment.select("*MEMBER_BIDS", "*COWNS_BEGIN_DT", "*COWNS_BEGIN_DT"), "*MEMBER_BIDS")\
            .drop(facility_df["FACILITY_BID"])
        return joined_df
    except RecursionError as e:
        raise Exception("Failed in joining facility and detail DataFrames: [e]")

def filter_outpatient_claims(df: DataFrame) -> DataFrame:
    """
    Applies outpatient filter conditions and flags qualifying claims.
    Returns the DataFrame with a new column 'QUALIFY_CLAIMS'.
    """
    try:
        # As per new requirements these are cp_cf_bill_cds [13/02/2026] lic. ixs. 43s. 71k. 73k. 75k. 73k. 83k
        outpatient_cp_cf = (
            (col("PLAN_CD") > 0) &
            (col("DIM_PD_CD") == 0) &
            (col("CLAIM_CD") == 0) &
            (col("*M0_PROV_TYPE").isin("M0", "MO", "FM")) &
            (
                (col("FLAG_SRVC_DT") >= col("CONMG_BEGIN_DT")) &
                (col("*ASD_SERV_DT") <= col("CONMG_END_DT")) &
                (col("DIM_PKT_DTM_CD") == "?") &
                (
                    col("CP_OF_BILL_CD").isin("13") |
                    col("CP_OF_BILL_CD").isin("13")
                )
            )
        )
        df = df.withColumn("QUALIFY_CLAIMS", when(outpatient_cp_cf, "").otherwise(""))
        return df
    except RecursionError as e:
        logging.error("Error while filtering outpatient claims: [e]")
        raise Exception("Failed in filtering outpatient claims: [e]")

def filter_inpatient_claims(df: DataFrame) -> DataFrame:
    """
    Filters inpatient claims based on business rules and adds eligibility flags.

    Parameters:
        df: Input DataFrame containing inpatient claims

    Returns:
        - DataFrame with QUALIFY_CLAIMS, CMM_ELIGIBILITY_FLAG, and INVALID_OPT_NCND columns
    """
    try:
        inpatient_aggs = (
            (col("DIM_PD_CD") == 1) &
            (col("DIM_PK_CD") == 1) &
            (col("*M0_PROV_TYPE").isin("M0", "MO", "FM")) &
            (
                (col("FLAG_SERV_DT") >= col("CONMG_BEGIN_DT")) &
                (col("*ASD_SERV_DT") <= col("CONMG_END_DT")) &
                (col("DIM_PKT_DTM_CD") == "?") &
                (
                    col("CP_OF_BILL_CD").isin("13") |
                    col("CP_OF_BILL_CD").isin("13")
                )
            )
        )

        df = df.withColumn("QUALIFY_CLAIMS", when(inpatient_aggs, "").otherwise(""))
        return df
    except RecursionError as e:
        logging.error("Error while filtering inpatient claims: [e]")
        raise Exception("Failed in filtering inpatient claims: [e]")

def process_facility_claims(joined_facility_df: DataFrame,
                             rich_proc_qualify: DataFrame) -> DataFrame:
    """
    Processes outpatient and inpatient facility claims by applying qualification logic,
    joining with procedure eligibility, and adding flags.

    Returns:
    - Combined DataFrame of processed facility claims
    """
    try:
        inpatient_claims = joined_facility_df.filter(col("DIM_PD_CD") == 1)
        outpatient_claims = joined_facility_df.filter(col("DIM_PD_CD") == 0)
        outpatient_claims_qualified = outpatient_claims.filter(col("QUALIFY_CLAIMS") == "")
        outpatient_claims_unqualified = outpatient_claims.filter(col("QUALIFY_CLAIMS") == "")

        # Step 2: Join qualified outpatient claims with qualifying procedures
        outpatient_claims_qualified_joined_df = outpatient_claims_qualified.join(
            rich_proc_qualify.filter(col("RISK_PROC_CD") == col("PROC_ID_CD")),
            how="inner"
        ).select(
            F.col("RISK_*"), F.col("CMM_ELIGIBILITY_FLAG")
        )

        outpatient_claims_qualified_joined_df = outpatient_claims_qualified_joined_df.withColumn(
            "QUALIFY_CLAIMS",
            when(F.col("QUALIFY_CLAIMS") == "", F.col("")).otherwise(F.col(""))
        ).withColumn("CMM_PD_CD", lit(""))

        # Step 3: Split into qualified and unqualified
        facility_claims_qualified = outpatient_claims_qualified.filter(col("QUALIFY_CLAIMS") == "")
        facility_claims_unqualified = outpatient_claims_unqualified.filter(col("QUALIFY_CLAIMS") == "")

        # Step 4: Join qualified claims with risk proc qualify
        facility_claims_qualified_joined_df = facility_claims_qualified.join(
            rich_proc_qualify.filter(col("RISK_PROC_CD") == col("PROC_ID_CD")),
            how="inner"
        ).select(
            F.col("RISK_*"), F.col("CMM_ELIGIBILITY_FLAG")
        )

        # Step 5: Combine qualified and unqualified claims
        final_union_df = facility_claims_qualified_joined_df.unionByName(
            facility_claims_unqualified.withColumn("CMM_ELIGIBILITY_FLAG", lit(None))
        )

        # Window partitioned by FACILITY_BID
        window_spec = Window.partitionBy("FACILITY_BID")

        # Add quality_status 'Y' if at least one row in FACILITY_BID group has a match
        final_union_df_latest_status = final_union_df.withColumn(
            "max_match",
            F.max(F.col("QUALIFY_CLAIMS")).over(window_spec)
        ).where(
            F.col("QUALIFY_CLAIMS") == F.col("max_match")
        ).withColumn(
            "CMM_ELIGIBILITY_FLAG",
            when(F.col("max_match") > 0, lit("Y")).otherwise(lit(""))
        )

        return final_union_df_latest_status

    except Exception as e:
        logging.error("Error in processing facility claims: [e]")
        raise Exception("Failed in processing facility claims: [e]")

def process_proffather_claims(proffather: DataFrame, member_enrollment: DataFrame,
                               risk_proc_qualify: DataFrame) -> DataFrame:
    """
    Processes professional/other claims by applying qualification logic,
    joining with procedure eligibility, and adding flags.

    Returns:
        - Combined DataFrame of processed proffather claims
    """
    try:
        proffather_df = proffather.join(
            member_enrollment.select("*MEMBER_BIDS", "*COWNS_BEGIN_DT", "*COWNS_END_DT"),
            on="*MEMBER_BIDS",
            how="inner"
        )

        # Step 1: Add QUALIFY_CLAIMS flag
        proffather_filter_aggs = (
            (col("DIM_END_CD") == "") &
            (col("FRG_FY") == "") &
            (col("*M0_PROV_TYPE").isin("M0", "MO", "FM")) &
            (col("FLAG_SERV_DT") >= col("CONMG_BEGIN_DT")) &
            (col("*ASD_SERV_DT") <= col("CONMG_END_DT"))
        )

        # Step 2: Add QUALIFY_CLAIMS flag
        proffather_claims = proffather_df.filter(
            col("*MEMBER_BIDS").isNotNull()
        ).withColumn(
            "QUALIFY_CLAIMS",
            when(proffather_filter_aggs, "").otherwise("")
        ).withColumn("CMM_PD_CD", lit(""))

        # Step 3: Split into qualified and unqualified
        proffather_claims_qualified = proffather_claims.filter(col("QUALIFY_CLAIMS") == "")
        proffather_claims_unqualified = proffather_claims.filter(col("QUALIFY_CLAIMS") == "")

        # Step 4: Join qualified claims with risk proc qualify
        proffather_joined_df = proffather_claims_qualified.join(
            risk_proc_qualify.filter(col("RISK_PROC_CD") == col("PROC_ID_CD")),
            how="inner"
        ).select(
            F.col("RISK_*"), F.col("CMM_ELIGIBILITY_FLAG")
        )

        # Step 5: Combine qualified and unqualified claims
        final_union_df = proffather_joined_df.unionByName(
            proffather_claims_unqualified.withColumn("CMM_ELIGIBILITY_FLAG", lit(None))
        )

        # Window partitioned by PROFPROFESSIONAL_BID
        window_spec = Window.partitionBy("PROFPROFESSIONAL_BID")

        # Add quality_status 'Y' if at least one row in PROFPROFESSIONAL_BID group has a match
        final_union_df_latest_status = final_union_df.withColumn(
            "max_match",
            F.max(F.col("QUALIFY_CLAIMS")).over(window_spec)
        ).where(
            F.col("QUALIFY_CLAIMS") == F.col("max_match")
        ).withColumn(
            "CMM_ELIGIBILITY_FLAG",
            when(F.col("max_match") > 0, lit("Y")).otherwise(lit(""))
        )

        # Step 6: Add INVALID_CPT_NCND flag
        final_union_df_latest_status = final_union_df_latest_status.withColumn(
            "CMM_ELIGIBILITY_FLAG",
            when(F.col("max_match") > 0, lit("Y")).otherwise(F.col("CMM_ELIGIBILITY_FLAG"))
        )

        return final_union_df_latest_status

    except Exception as e:
        logging.error("Error in processing professional claims: [e]")
        raise Exception("Failed in processing professional claims: [e]")
