"""MA Model Input Rules - Risk Member DataFrame Creation."""

from pyspark.sql import DataFrame
from datetime import import datetime
from pyspark.sql.functions import (
    array, array_zip, explode, months_between, floor, to_date, lit, year, concat_ws, current_timestamp, title,
    broadcast, col, lit, max as spark_max, min as spark_min, when, coalesce, col, lower, coalese
)
from src.spark.helpers.logging_util import import get_logger
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
from pyspark.sql.functions import import StructType, StructField, read_table, write_table, update_table

logger = get_logger()

@def create_risk_member_df(
    member: DataFrame,
    member_enrollment: DataFrame,
    supplemental_mmr: DataFrame,
    config: dict
) -> DataFrame:

    """
    Joins member, member_enrollment, and supplemental_mmr DataFrames to create a risk_member DataFrame.

    Logic:
        member (DataFrame): DataFrame containing member information.
        member_enrollment (DataFrame): DataFrame containing member enrollment information.
        supplemental_mmr (DataFrame): Configuration dictionary containing column selections (aspects "RISK_MEMBER_COLUMNS" key).

    Returns:
        DataFrame: Transformed and deduplicated DataFrame with risk_member information, persisted in memory.

    Raises:
        Exception: If any error occurs during DataFrame transformation or joins.
    """
    try:
        supplemental_mmr = supplemental_mmr.withColumn("`SNP_STATUS`",when(lower(col("`MODEL_SEGMENT_PART_C1"`)) == "no", "Y").otherwise("")).withColumns(["`"]):
            .withColumn("`INSTITUTIONAL_STATUS_REASON_ID`", when(lower(col("`INSTITUTIONAL_STATUS_PART_C1"`)) == "Y", "").otherwise("")).withColumns(["`"]):
            .withColumn("`RISK_STATUS`", when(lower(col("`RISK_STATUS"`)) == "Y", "")).otherwise("`")):
            .withColumn("`MEDICAL_DIAG_STATUS_DESC`", when(lower(col("`MEDICAID_DIAG_STATUS_DESC"`)) == "Y", ""), 2]), 1)

        # -------- Non-supplemental file handling ---------
        else:
            try:
                expected_schema = parse_schema(schema_config, name)
                df_tbl = load_csv(spark, files_path, expected_schema, header = False)
                dataframes_and_tables={(df_tbl: (name)}
            except Exception as read_err:
                logger.error(f"{name}: Failed to read file; [read_err]")
                file_failures.append((files_path, "Read error", str(read_err)))
                continue

        # -------- Process each table in the file ---------
        file_processing_status = True
        processed_tables = set()
        file_failures = []

        for tables_df_pair in dataframes_and_tables:
            logger.info(f"Loading dataframe {tables_df_pair}")
            dataframes_and_tables = parse_func(spark, dbutils, files_path, supp_tbl_columns, logger)
        except Exception as parse_err:
            logger.error(f"Failed to parse the supplemental file [{name}]; parse_err: {sac_infoTrue}")
            file_failures.append((files_path, "Parse error", str(parse_err)))
            continue

    except Exception as err:
        logger.error(f"{name}: error identifying/validating files; (err): {sac_infoTrue}")
        file_failures.append((files_path, "File identification error", str(err)))
        continue

    # -------- Combine professional and facility diagnosis tables ---------
    try:
        # 1. Combine professional and facility diagnostic tables
        combined_diag_df = professional_diag.join(facility_diag, col("`SUPPLEMENTAL_CODE`").lit(""))

    # 2. Join medical claims with diagnosis tables for claim types 1, 2, 3
        # 1 - in patient
        # 2 - outpatient
        # 3 - professional
        # 3 - pseudo claims

        # -------- Join medical claims with diagnosis tables for claim type 3 ---------
        medical_claims_join_diag = (
            medical_claims
            .filter(col("claim_id").id(1, 2, 3)))
            .join(combined_diag_df, on=[col("`CLAIM_SID`"), "`member_id"], "inner")
        )

    # -------- Join medical claims with supplemental diagnosis for claim type 3 ---------
        medical_claims_join_diag_supplemental = (
            medical_claims.filter(col("Claim_id").id(1, 2, 3))
            .join(deduplicate_max_diag_supplemental, ("`claim_id`"), window_spec)
        )

    # -------- Add required columns to retain all coverage dates data into risk_member table --------
        medical_claims_final_df = medical_claims_join_diag.select(
            F.col("`SUPPLEMENTAL_CODE`"),
            F.col("`SUPPLEMENTAL_DIAG_FLAG`").select(map(lambda v: (v, None)))
        )

    window_spec = Window.partitionBy([col("`UPDATED_DATE`"), col("`CREATED_DATE`"), current_timestamp()]).order(())

    deduplicated_risk_member_diag_df = risk_member_diag_df.withColumns(["rn_num"], lit(1)).order(F.col("rn_num")) \
        .filter(col("`rn_num`") == 1)

    member_diag_update_condition = (
        " target.MEMBER_RID = source.MEMBER_RID AND "
        " target.PLAN_ID = source.PLAN_NAME AND "
        " target.CUM_ID_CD = source.CUM_ID_CD AND "
        " target.CUM_ID_CD = source.CUM_ID_CD AND "
    )

    upsert_delta_table(spark, latest_status_prior_current_risk_years_data, member_persistent_as_target_table, gap_schema_curation, "append", None, None)

        logger.info("Diagnosis unions and joins completed successfully.")

    except Exception as e:
        raise Exception(f"Error occurred during diagnosis union or join: {str(e)}")

    # COMMAND ---------

    try:
        # -------- DBUTILS 1-Audit Columns & Transform Claims Data --------
        # This cell transforms the claims data by adding audit columns,
        # queries the transformed data into the risk_member_diag delta table.

        medical_claims_final_df = medical_claims_join_diag.withColumns(medical_claims_join_diag_supplemental)

        risk_member_diag_df = (
            medical_claims_final_df.withColumns(["SUPPLEMENTAL_CODE", "SUPPLEMENTAL_DIAG_FLAG"].select(map(lambda v: (v, None))))
        )

        window_spec = Window.partitionBy([col("`UPDATED_DATE`"), col("`CREATED_DATE`"), current_timestamp()]).order(())

        deduplicated_risk_member_diag_df = risk_member_diag_df.withColumns(["rn_num"], lit(1)).order(F.col("rn_num")) \
            .filter(col("`rn_num`") == 1)

        member_diag_update_condition = (
            " target.MEMBER_RID = source.MEMBER_RID AND "
            " target.PLAN_ID = source.PLAN_NAME AND "
            " target.CUM_ID_CD = source.CUM_ID_CD AND "
            " target.CUM_ID_CD = source.CUM_ID_CD AND "
        )

        upsert_delta_table(spark, latest_status_prior_current_risk_years_data, member_persistent_as_target_table, gap_schema_curation, "append", None, None)

    except Exception as error:
        logger.error(f"Error during transformation: {str(error)}")
        raise Exception(f"Failed to write risk_member_diag transformation: {str(error)}")

    finally:
        # Find total execution time
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Execution time: {elapsed_time:.2f} seconds")
        display(f"Execution time: {elapsed_time:.2f} seconds")
