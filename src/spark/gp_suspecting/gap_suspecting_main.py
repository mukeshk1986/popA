from __future__ import annotations

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


def run_gap_suspecting(
    spark: SparkSession,
    config: Dict[str, Any],
    method_ids: Optional[Sequence[int]] = None,
) -> Dict[str, DataFrame]:
    """Run the gap suspecting workflow end to end for the configured risk year and load month."""
    risk_year = config.get("risk_year")
    source_load_month = config.get("source_load_month")
    cycle_run = config.get("cycle_run")

    metadata_table = config.get("method_metadata_table", "ref_method_metadata")
    codegroup_table = config.get("method_codegroup_table")
    claims_tables = config.get(
        "claim_source_tables",
        ["risk_member_diag", "risk_member_hcc", "member_persistence_hcc"],
    )

    method_metadata = load_method_metadata(
        spark=spark,
        metadata_table=metadata_table,
        codegroup_table=codegroup_table,
        method_ids=method_ids,
    )

    claims_df = build_claims_base(
        spark=spark,
        source_tables=claims_tables,
        risk_year=risk_year,
        source_load_month=source_load_month,
    )

    candidate_frames: list[DataFrame] = []
    for current_method_id in method_ids or [1, 2, 4, 10]:
        if current_method_id == 4:
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

    draft_gaps = (
        F.coalesce(*candidate_frames) if len(candidate_frames) > 1 else candidate_frames[0]
    )
    draft_gaps = apply_suppression_logic(draft_gaps)

    member_df = spark.table(config.get("member_table", "risk_member"))
    diag_df = spark.table(config.get("diagnosis_table", "risk_member_diag"))
    draft_details = enrich_with_member_details(draft_gaps, member_df=member_df, diag_df=diag_df)

    final_output = build_final_output(
        draft_gaps=draft_gaps,
        draft_details=draft_details,
        risk_year=risk_year,
        source_load_month=source_load_month,
        cycle_run=cycle_run,
    )

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

    return {
        "draft_gaps": draft_gaps,
        "draft_gaps_with_details": draft_details,
        "suspected_gaps": final_output,
    }


def main() -> None:
    """CLI-oriented entry point for the workflow."""
    import os

    spark = SparkSession.builder.appName("gap-suspecting").getOrCreate()
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
    run_gap_suspecting(spark=spark, config=config, method_ids=[1, 2, 4, 10])
    spark.stop()


if __name__ == "__main__":
    main()
