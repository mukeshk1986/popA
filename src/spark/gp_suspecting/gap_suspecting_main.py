from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark.gp_suspecting.gap_suspecting_helper import (
    apply_suppression_logic,
    build_claims_base,
    build_final_output,
    enrich_with_member_details,
    identify_method_4_candidates,
    identify_method_candidates,
    load_method_metadata,
    save_delta_table,
)

logger = logging.getLogger(__name__)


def run_gap_suspecting(
    spark: SparkSession,
    config: Dict[str, Any],
    method_ids: Optional[Sequence[int]] = None,
) -> Dict[str, DataFrame]:
    """Run the gap suspecting workflow end to end for the configured risk year and load month.

    Executes the complete gap suspecting pipeline:
    1. Loads and filters method metadata
    2. Builds claims base from source tables
    3. Identifies gap candidates for each method
    4. Applies suppression logic
    5. Enriches with member and diagnosis details
    6. Produces final output for reporting

    Args:
        spark (SparkSession): The Spark session.
        config (Dict[str, Any]): Configuration dictionary containing:
            - risk_year (str): The target risk year
            - source_load_month (str): The load month for filtering
            - cycle_run (str): The cycle run identifier
            - method_metadata_table (str): Source metadata table
            - method_codegroup_table (str, optional): Code group reference table
            - claim_source_tables (List[str]): List of claim source tables
            - member_table (str): Member demographics table
            - diagnosis_table (str): Diagnosis details table
            - persistence_table (str): Persistence/historical table
            - current_hcc_table (str): Current year HCC table
            - target_schema (str): Target schema for output tables
        method_ids (Sequence[int], optional): Specific method IDs to run (1, 2, 4, 10).
            Defaults to None (runs all methods).

    Returns:
        Dict[str, DataFrame]: Dictionary containing three DataFrames:
            - draft_gaps: Raw identified gaps without enrichment
            - draft_gaps_with_details: Gaps enriched with member and diagnosis data
            - suspected_gaps: Final output filtered for non-suppressed gaps

    Raises:
        ValueError: If required configuration parameters are missing.
        Exception: If any step of the workflow fails.
    """
    try:
        logger.info("=" * 80)
        logger.info("Starting gap suspecting workflow")
        logger.info("=" * 80)

        risk_year = config.get("risk_year")
        source_load_month = config.get("source_load_month")
        cycle_run = config.get("cycle_run")

        if not risk_year or not source_load_month:
            raise ValueError("risk_year and source_load_month are required in config")

        logger.info(f"Configuration: risk_year={risk_year}, load_month={source_load_month}, cycle_run={cycle_run}")

        metadata_table = config.get("method_metadata_table", "ref_method_metadata")
        codegroup_table = config.get("method_codegroup_table")
        claims_tables = config.get(
            "claim_source_tables",
            ["risk_member_diag", "risk_member_hcc", "member_persistence_hcc"],
        )

        logger.info("STEP 1: Loading method metadata")
        method_metadata = load_method_metadata(
            spark=spark,
            metadata_table=metadata_table,
            codegroup_table=codegroup_table,
            method_ids=method_ids,
        )

        logger.info("STEP 2: Building claims base")
        claims_df = build_claims_base(
            spark=spark,
            source_tables=claims_tables,
            risk_year=risk_year,
            source_load_month=source_load_month,
        )

        logger.info("STEP 3: Identifying gap candidates for each method")
        candidate_frames: list[DataFrame] = []
        methods_to_run = method_ids or [1, 2, 4, 10]
        logger.info(f"Running methods: {list(methods_to_run)}")

        for current_method_id in methods_to_run:
            if current_method_id == 4:
                logger.info(f"Processing method {current_method_id} (Persistent Condition Carryforward)")
                persistence_df = spark.table(config.get("persistence_table", "member_persistence_hcc"))
                current_year_df = spark.table(config.get("current_hcc_table", "risk_member_hcc"))
                candidate_frames.append(
                    identify_method_4_candidates(
                        persistence_df=persistence_df,
                        current_year_df=current_year_df,
                        risk_year=risk_year,
                        source_load_month=source_load_month,
                        cycle_run=cycle_run,
                    )
                )
            else:
                logger.info(f"Processing method {current_method_id}")
                candidate_frames.append(
                    identify_method_candidates(
                        claims_df=claims_df,
                        method_metadata=method_metadata,
                        method_id=current_method_id,
                        risk_year=risk_year,
                        source_load_month=source_load_month,
                        cycle_run=cycle_run,
                    )
                )

        logger.info(f"Combining {len(candidate_frames)} candidate frames")
        if len(candidate_frames) == 0:
            raise RuntimeError("No candidate frames were generated")

        draft_gaps = (
            F.coalesce(*candidate_frames) if len(candidate_frames) > 1 else candidate_frames[0]
        )

        logger.info("STEP 4: Applying suppression logic")
        draft_gaps = apply_suppression_logic(draft_gaps)

        logger.info("STEP 5: Enriching with member and diagnosis details")
        member_df = spark.table(config.get("member_table", "risk_member"))
        diag_df = spark.table(config.get("diagnosis_table", "risk_member_diag"))
        draft_details = enrich_with_member_details(draft_gaps, member_df=member_df, diag_df=diag_df)

        logger.info("STEP 6: Building final output")
        final_output = build_final_output(
            draft_gaps=draft_gaps,
            draft_details=draft_details,
            risk_year=risk_year,
            source_load_month=source_load_month,
            cycle_run=cycle_run,
        )

        logger.info("STEP 7: Persisting results to Delta tables")
        target_schema = config.get("target_schema", "pop_advyzer")
        draft_gaps_table = f"{target_schema}.draft_gaps"
        draft_details_table = f"{target_schema}.draft_gaps_with_details"
        suspected_gaps_table = f"{target_schema}.suspected_gaps"

        save_delta_table(
            draft_gaps,
            draft_gaps_table,
            partition_cols=["SOURCE_LOAD_MONTH", "HOME_PLAN_ID_CD", "METHOD_ID"],
        )
        save_delta_table(
            draft_details,
            draft_details_table,
            partition_cols=["SOURCE_LOAD_MONTH", "HOME_PLAN_ID_CD", "METHOD_ID"],
        )
        save_delta_table(
            final_output,
            suspected_gaps_table,
            partition_cols=["SOURCE_LOAD_MONTH", "HOME_PLAN_ID_CD", "METHOD_ID"],
        )

        logger.info("=" * 80)
        logger.info("Gap suspecting workflow completed successfully")
        logger.info(f"Final output: {final_output.count()} suspected gaps")
        logger.info("=" * 80)

        return {
            "draft_gaps": draft_gaps,
            "draft_gaps_with_details": draft_details,
            "suspected_gaps": final_output,
        }

    except Exception as e:
        logger.error(f"Error running gap suspecting workflow: {e}")
        raise RuntimeError(f"Gap suspecting workflow failed: {e}") from e


def main() -> None:
    """CLI-oriented entry point for the gap suspecting workflow.

    Initializes the Spark session, loads configuration from environment variables,
    and executes the gap suspecting workflow. All configuration values default to
    sensible values if environment variables are not set.

    Environment Variables:
        RISK_YEAR (str): The target risk year. Defaults to "2025".
        SOURCE_LOAD_MONTH (str): The source load month. Defaults to "2025_01".
        CYCLE_RUN (str): The cycle run identifier. Defaults to "manual".
        METHOD_METADATA_TABLE (str): Metadata reference table. Defaults to "ref_method_metadata".
        METHOD_CODEGROUP_TABLE (str, optional): Code group reference table. No default.
        CLAIM_SOURCE_TABLES (str): Comma-separated claim source tables.
            Defaults to "risk_member_diag".
        MEMBER_TABLE (str): Member demographics table. Defaults to "risk_member".
        DIAGNOSIS_TABLE (str): Diagnosis details table. Defaults to "risk_member_diag".
        PERSISTENCE_TABLE (str): Persistence/historical table. Defaults to "member_persistence_hcc".
        CURRENT_HCC_TABLE (str): Current year HCC table. Defaults to "risk_member_hcc".
        TARGET_SCHEMA (str): Target schema for output tables. Defaults to "pop_advyzer".

    Returns:
        None

    Raises:
        Exception: If the workflow execution fails.
    """
    spark = None
    try:
        logger.info("Initializing Spark session for gap suspecting workflow")
        spark = SparkSession.builder.appName("gap-suspecting").getOrCreate()
        logger.info("Spark session initialized successfully")

        logger.info("Loading configuration from environment variables")
        config = {
            "risk_year": os.getenv("RISK_YEAR", "2025"),
            "source_load_month": os.getenv("SOURCE_LOAD_MONTH", "2025_01"),
            "cycle_run": os.getenv("CYCLE_RUN", "manual"),
            "method_metadata_table": os.getenv("METHOD_METADATA_TABLE", "ref_method_metadata"),
            "method_codegroup_table": os.getenv("METHOD_CODEGROUP_TABLE"),
            "claim_source_tables": os.getenv("CLAIM_SOURCE_TABLES", "risk_member_diag").split(","),
            "member_table": os.getenv("MEMBER_TABLE", "risk_member"),
            "diagnosis_table": os.getenv("DIAGNOSIS_TABLE", "risk_member_diag"),
            "persistence_table": os.getenv("PERSISTENCE_TABLE", "member_persistence_hcc"),
            "current_hcc_table": os.getenv("CURRENT_HCC_TABLE", "risk_member_hcc"),
            "target_schema": os.getenv("TARGET_SCHEMA", "pop_advyzer"),
        }

        logger.info("Configuration loaded:")
        for key, value in config.items():
            if value and key not in ["method_codegroup_table"]:
                logger.info(f"  {key}: {value}")

        logger.info("Starting gap suspecting workflow with methods: [1, 2, 4, 10]")
        results = run_gap_suspecting(spark=spark, config=config, method_ids=[1, 2, 4, 10])

        logger.info(f"Workflow completed successfully")
        logger.info(f"Draft gaps: {results['draft_gaps'].count()} records")
        logger.info(f"Draft gaps with details: {results['draft_gaps_with_details'].count()} records")
        logger.info(f"Suspected gaps: {results['suspected_gaps'].count()} records")

    except Exception as e:
        logger.error(f"Error in main workflow: {e}")
        raise RuntimeError(f"Workflow failed: {e}") from e
    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    main()
