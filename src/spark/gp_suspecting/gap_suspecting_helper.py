from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

METHOD_LABELS = {
    1: "Diagnosis-Based Gap Identification",
    2: "Procedure-Based Gap Identification",
    4: "Persistent Condition Carryforward",
    10: "Multi-Step Exclusion Logic",
}


def ensure_columns(df: DataFrame, columns: Iterable[str], default_value: Any = None) -> DataFrame:
    """Ensure a DataFrame contains the requested columns, adding them as nulls when missing."""
    for column in columns:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(default_value))
    return df


def load_method_metadata(
    spark: SparkSession,
    metadata_table: str,
    codegroup_table: Optional[str] = None,
    method_ids: Optional[Sequence[int]] = None,
) -> DataFrame:
    """Load and filter the method metadata reference table for inclusion-only rules."""
    metadata = spark.table(metadata_table)

    if method_ids:
        metadata = metadata.filter(F.col("METHOD_ID").isin(list(method_ids)))

    metadata = metadata.filter(
        (F.col("EXCLUSION_TYPE_1").isNull() | (F.col("EXCLUSION_TYPE_1") == "NA"))
        & (F.col("EXCLUSION_TYPE_2").isNull() | (F.col("EXCLUSION_TYPE_2") == "NA"))
    )

    if codegroup_table:
        codegroups = spark.table(codegroup_table)
        metadata = metadata.join(codegroups, on=["METHOD_ID", "HCC_CODE"], how="left")

    return metadata


def build_claims_base(
    spark: SparkSession,
    source_tables: Sequence[str],
    risk_year: str,
    source_load_month: Optional[str] = None,
) -> DataFrame:
    """Combine the input claim sources into a single claims base for gap suspecting."""
    frames: list[DataFrame] = []
    for table_name in source_tables:
        df = spark.table(table_name)
        if source_load_month:
            df = df.filter(F.col("SOURCE_LOAD_MONTH") == source_load_month)
        if risk_year:
            df = df.filter(F.col("RISK_YEAR") == risk_year)
        frames.append(df)

    if not frames:
        raise ValueError("No claim source tables were provided.")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)

    return ensure_columns(
        combined,
        [
            "BID",
            "MEMBER_ID",
            "HOME_PLAN_ID_CD",
            "CONTRACT_ID",
            "HCC_CODE",
            "HCC_LABEL",
            "METHOD_ID",
            "METHOD_NAME",
            "DIAG_CD",
            "LAST_DOS",
            "RISK_YEAR",
            "SOURCE_LOAD_MONTH",
            "CYCLE_RUN",
        ],
    )


def identify_method_candidates(
    claims_df: DataFrame,
    method_metadata: DataFrame,
    method_id: int,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Generate draft candidate gaps for the specified gap method."""
    if method_id == 1:
        return (
            claims_df.join(
                method_metadata.filter(F.col("METHOD_ID") == method_id),
                on=["DIAG_CD"],
                how="inner",
            )
            .select(
                F.col("BID").alias("BID"),
                F.col("MEMBER_ID"),
                F.col("HOME_PLAN_ID_CD"),
                F.col("CONTRACT_ID"),
                F.col("HCC_CODE"),
                F.col("HCC_LABEL"),
                F.lit(method_id).alias("METHOD_ID"),
                F.lit(METHOD_LABELS[method_id]).alias("METHOD_NAME"),
                F.col("DIAG_CD"),
                F.col("LAST_DOS"),
                F.lit(risk_year).alias("RISK_YEAR"),
                F.lit(source_load_month).alias("SOURCE_LOAD_MONTH"),
                F.lit(cycle_run).alias("CYCLE_RUN"),
            )
        )

    if method_id == 2:
        return (
            claims_df.join(
                method_metadata.filter(F.col("METHOD_ID") == method_id),
                on=["DIAG_CD"],
                how="inner",
            )
            .select(
                F.col("BID").alias("BID"),
                F.col("MEMBER_ID"),
                F.col("HOME_PLAN_ID_CD"),
                F.col("CONTRACT_ID"),
                F.col("HCC_CODE"),
                F.col("HCC_LABEL"),
                F.lit(method_id).alias("METHOD_ID"),
                F.lit(METHOD_LABELS[method_id]).alias("METHOD_NAME"),
                F.col("DIAG_CD"),
                F.col("LAST_DOS"),
                F.lit(risk_year).alias("RISK_YEAR"),
                F.lit(source_load_month).alias("SOURCE_LOAD_MONTH"),
                F.lit(cycle_run).alias("CYCLE_RUN"),
            )
        )

    if method_id == 10:
        return (
            claims_df.join(
                method_metadata.filter(F.col("METHOD_ID") == method_id),
                on=["DIAG_CD"],
                how="inner",
            )
            .select(
                F.col("BID").alias("BID"),
                F.col("MEMBER_ID"),
                F.col("HOME_PLAN_ID_CD"),
                F.col("CONTRACT_ID"),
                F.col("HCC_CODE"),
                F.col("HCC_LABEL"),
                F.lit(method_id).alias("METHOD_ID"),
                F.lit(METHOD_LABELS[method_id]).alias("METHOD_NAME"),
                F.col("DIAG_CD"),
                F.col("LAST_DOS"),
                F.lit(risk_year).alias("RISK_YEAR"),
                F.lit(source_load_month).alias("SOURCE_LOAD_MONTH"),
                F.lit(cycle_run).alias("CYCLE_RUN"),
            )
        )

    raise ValueError(f"Unsupported gap method: {method_id}")


def identify_method_4_candidates(
    persistence_df: DataFrame,
    current_year_df: DataFrame,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Identify method 4 carry-forward gaps where the condition persisted historically but not this year."""
    current_year = ensure_columns(
        current_year_df,
        ["MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS"],
    )
    persistence = ensure_columns(
        persistence_df,
        ["MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS"],
    )

    prior_year_conditions = (
        persistence.filter(F.col("RISK_YEAR") != risk_year)
        .select("MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS")
        .distinct()
    )

    current_year_conditions = (
        current_year.filter(F.col("RISK_YEAR") == risk_year)
        .select("MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS")
        .distinct()
    )

    return (
        prior_year_conditions.join(
            current_year_conditions,
            on=["MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE"],
            how="leftanti",
        )
        .withColumn("METHOD_ID", F.lit(4))
        .withColumn("METHOD_NAME", F.lit(METHOD_LABELS[4]))
        .withColumn("RISK_YEAR", F.lit(risk_year))
        .withColumn("SOURCE_LOAD_MONTH", F.lit(source_load_month))
        .withColumn("CYCLE_RUN", F.lit(cycle_run))
        .select(
            F.col("MEMBER_ID"),
            F.col("HOME_PLAN_ID_CD"),
            F.col("CONTRACT_ID"),
            F.col("HCC_CODE"),
            F.col("HCC_LABEL"),
            F.col("METHOD_ID"),
            F.col("METHOD_NAME"),
            F.col("DIAG_CD"),
            F.col("LAST_DOS"),
            F.col("RISK_YEAR"),
            F.col("SOURCE_LOAD_MONTH"),
            F.col("CYCLE_RUN"),
        )
        .withColumn("BID", F.concat(F.col("MEMBER_ID"), F.lit("_"), F.col("HCC_CODE"), F.lit("_"), F.col("METHOD_ID")))
    )


def apply_suppression_logic(draft_gaps: DataFrame) -> DataFrame:
    """Apply the documented suppression steps: documentation and plan exclusion flags."""
    return (
        draft_gaps.withColumn("IS_SUPPRESSED_1", F.lit("N"))
        .withColumn("IS_SUPPRESSED_2", F.lit("N"))
        .withColumn("FREQUENCY_COUNT", F.lit(1))
    )


def enrich_with_member_details(
    draft_gaps: DataFrame,
    member_df: DataFrame,
    diag_df: Optional[DataFrame] = None,
) -> DataFrame:
    """Attach member and diagnosis context to the draft gap records."""
    base = draft_gaps
    if member_df is not None:
        member_df = ensure_columns(
            member_df,
            ["MEMBER_ID", "MEMBER_FIRST_NAME", "MEMBER_LAST_NAME", "DATE_OF_BIRTH", "GENDER", "AGE"],
        )
        base = base.join(member_df, on=["MEMBER_ID"], how="left")

    if diag_df is not None:
        diag_df = ensure_columns(
            diag_df,
            ["MEMBER_ID", "DIAG_CD", "DIAG_DESCRIPTION", "PROVIDER_NPI", "PROVIDER_NAME", "FACILITY_TYPE_CD", "CLAIM_TYPE"],
        )
        base = base.join(diag_df, on=["MEMBER_ID", "DIAG_CD"], how="left")

    return base


def save_delta_table(df: DataFrame, target_table: str, partition_cols: Optional[Sequence[str]] = None) -> None:
    """Persist a DataFrame as a Delta table using overwrite semantics."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.option("overwriteSchema", "true").saveAsTable(target_table)


def build_final_output(
    draft_gaps: DataFrame,
    draft_details: DataFrame,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Create the final suspected gap output table from the suppressed draft data."""
    return (
        draft_details.filter(F.col("IS_SUPPRESSED_1") == "N")
        .filter(F.col("IS_SUPPRESSED_2") == "N")
        .withColumn("RISK_YEAR", F.lit(risk_year))
        .withColumn("SOURCE_LOAD_MONTH", F.lit(source_load_month))
        .withColumn("CYCLE_RUN", F.lit(cycle_run))
        .select(
            "BID",
            "MEMBER_ID",
            "HOME_PLAN_ID_CD",
            "MEMBER_FIRST_NAME",
            "MEMBER_LAST_NAME",
            "DATE_OF_BIRTH",
            "GENDER",
            "AGE",
            "HCC_CODE",
            "HCC_LABEL",
            "METHOD_ID",
            "METHOD_NAME",
            "DIAG_CD",
            "DIAG_DESCRIPTION",
            "LAST_DOS",
            "PROVIDER_NPI",
            "PROVIDER_NAME",
            "FACILITY_TYPE_CD",
            "CLAIM_TYPE",
            "IS_SUPPRESSED_1",
            "IS_SUPPRESSED_2",
            "FREQUENCY_COUNT",
            "RISK_YEAR",
            "SOURCE_LOAD_MONTH",
            "CYCLE_RUN",
        )
    )
