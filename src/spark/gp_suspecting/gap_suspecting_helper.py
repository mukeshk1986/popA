from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

METHOD_LABELS = {
    1: "Diagnosis-Based Gap Identification",
    2: "Procedure-Based Gap Identification",
    4: "Persistent Condition Carryforward",
    10: "Multi-Step Exclusion Logic",
}


def ensure_columns(df: DataFrame, columns: Iterable[str], default_value: Any = None) -> DataFrame:
    """Ensure a DataFrame contains the requested columns, adding them as nulls when missing.

    Args:
        df (DataFrame): The input DataFrame to check.
        columns (Iterable[str]): Column names to ensure exist in the DataFrame.
        default_value (Any, optional): Default value to use for missing columns. Defaults to None.

    Returns:
        DataFrame: DataFrame with all requested columns present.

    Raises:
        ValueError: If the input DataFrame is None.
    """
    try:
        if df is None:
            raise ValueError("Input DataFrame cannot be None")

        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Adding missing columns to DataFrame: {missing_cols}")
            for column in missing_cols:
                df = df.withColumn(column, F.lit(default_value))

        return df
    except Exception as e:
        logger.error(f"Error ensuring columns in DataFrame: {e}")
        raise RuntimeError(f"Failed to ensure columns: {e}") from e


def load_method_metadata(
    spark: SparkSession,
    metadata_table: str,
    codegroup_table: Optional[str] = None,
    method_ids: Optional[Sequence[int]] = None,
) -> DataFrame:
    """Load and filter the method metadata reference table for inclusion-only rules.

    This function loads method metadata from a reference table, applies optional filtering
    by method IDs, and joins with a code group table if provided.

    Args:
        spark (SparkSession): The Spark session.
        metadata_table (str): The name of the metadata reference table to load.
        codegroup_table (str, optional): The name of the code group reference table to join.
            Defaults to None.
        method_ids (Sequence[int], optional): List of method IDs to filter by.
            If None, all methods are included. Defaults to None.

    Returns:
        DataFrame: Filtered metadata DataFrame with optional code group information.

    Raises:
        Exception: If the metadata table cannot be loaded.
        Exception: If the code group table cannot be loaded (when provided).
    """
    try:
        logger.info(f"Loading method metadata from table: {metadata_table}")
        metadata = spark.table(metadata_table)

        if metadata.count() == 0:
            logger.warning(f"Metadata table '{metadata_table}' is empty")

        if method_ids:
            logger.info(f"Filtering metadata for method IDs: {list(method_ids)}")
            metadata = metadata.filter(F.col("METHOD_ID").isin(list(method_ids)))

        logger.info("Filtering for inclusion-only rules (no exclusion types)")
        metadata = metadata.filter(
            (F.col("EXCLUSION_TYPE_1").isNull() | (F.col("EXCLUSION_TYPE_1") == "NA"))
            & (F.col("EXCLUSION_TYPE_2").isNull() | (F.col("EXCLUSION_TYPE_2") == "NA"))
        )

        if codegroup_table:
            logger.info(f"Joining with code group table: {codegroup_table}")
            codegroups = spark.table(codegroup_table)
            metadata = metadata.join(codegroups, on=["METHOD_ID", "HCC_CODE"], how="left")

        logger.info(f"Successfully loaded metadata with {metadata.count()} records")
        return metadata
    except Exception as e:
        logger.error(f"Error loading method metadata from {metadata_table}: {e}")
        raise RuntimeError(f"Failed to load method metadata: {e}") from e


def build_claims_base(
    spark: SparkSession,
    source_tables: Sequence[str],
    risk_year: str,
    source_load_month: Optional[str] = None,
) -> DataFrame:
    """Combine the input claim sources into a single claims base for gap suspecting.

    Loads multiple claim source tables, filters by risk year and source load month,
    and combines them into a single DataFrame with standardized columns.

    Args:
        spark (SparkSession): The Spark session.
        source_tables (Sequence[str]): List of claim source table names to combine.
        risk_year (str): The risk year to filter on.
        source_load_month (str, optional): The source load month to filter on.
            Defaults to None (no month filter).

    Returns:
        DataFrame: Combined claims DataFrame with all standard columns present.

    Raises:
        ValueError: If no claim source tables are provided.
        Exception: If any source table cannot be loaded.
    """
    try:
        if not source_tables:
            raise ValueError("No claim source tables were provided")

        logger.info(f"Building claims base from tables: {list(source_tables)}")
        frames: list[DataFrame] = []
        for table_name in source_tables:
            try:
                logger.info(f"Loading claim source table: {table_name}")
                df = spark.table(table_name)
                initial_count = df.count()
                logger.info(f"Loaded {initial_count} records from {table_name}")

                if source_load_month:
                    df = df.filter(F.col("SOURCE_LOAD_MONTH") == source_load_month)
                    filtered_count = df.count()
                    logger.info(f"Filtered to {filtered_count} records for load month {source_load_month}")

                if risk_year:
                    df = df.filter(F.col("RISK_YEAR") == risk_year)
                    filtered_count = df.count()
                    logger.info(f"Filtered to {filtered_count} records for risk year {risk_year}")

                frames.append(df)
            except Exception as e:
                logger.error(f"Failed to load claim source table {table_name}: {e}")
                raise

        if not frames:
            raise ValueError("No claim source tables could be loaded")

        logger.info(f"Combining {len(frames)} claim source tables")
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame, allowMissingColumns=True)

        logger.info(f"Combined claims base has {combined.count()} records")

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
    except Exception as e:
        logger.error(f"Error building claims base: {e}")
        raise RuntimeError(f"Failed to build claims base: {e}") from e


def identify_method_candidates(
    claims_df: DataFrame,
    method_metadata: DataFrame,
    method_id: int,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Generate draft candidate gaps for the specified gap method.

    Identifies potential gap candidates by joining claims data with method metadata
    based on diagnosis codes and produces a standardized output DataFrame.

    Args:
        claims_df (DataFrame): The combined claims DataFrame.
        method_metadata (DataFrame): The filtered method metadata DataFrame.
        method_id (int): The gap method ID (1, 2, 4, or 10).
        risk_year (str): The risk year for this analysis.
        source_load_month (str): The source load month for this analysis.
        cycle_run (str): The cycle run identifier.

    Returns:
        DataFrame: Draft gap candidates with method-specific dimensions.

    Raises:
        ValueError: If the method_id is not supported (must be 1, 2, 4, or 10).
        Exception: If the join operation fails.
    """
    try:
        if method_id not in [1, 2, 4, 10]:
            raise ValueError(f"Unsupported gap method: {method_id}. Supported methods are: 1, 2, 4, 10")

        logger.info(f"Identifying candidates for method {method_id} ({METHOD_LABELS.get(method_id, 'Unknown')})")

        method_filter = method_metadata.filter(F.col("METHOD_ID") == method_id)
        if method_filter.count() == 0:
            logger.warning(f"No metadata found for method {method_id}")
            return claims_df.limit(0)

        candidates = (
            claims_df.join(
                method_filter,
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

        candidate_count = candidates.count()
        logger.info(f"Identified {candidate_count} candidates for method {method_id}")
        return candidates

    except Exception as e:
        logger.error(f"Error identifying method {method_id} candidates: {e}")
        raise RuntimeError(f"Failed to identify method candidates: {e}") from e


def identify_method_4_candidates(
    persistence_df: DataFrame,
    current_year_df: DataFrame,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Identify method 4 carry-forward gaps where the condition persisted historically but not this year.

    Compares persistent historical conditions against current year HCC records to identify
    conditions that were previously documented but are missing from the current year.

    Args:
        persistence_df (DataFrame): Historical persistence DataFrame with prior year conditions.
        current_year_df (DataFrame): Current year HCC DataFrame.
        risk_year (str): The current risk year.
        source_load_month (str): The source load month for this analysis.
        cycle_run (str): The cycle run identifier.

    Returns:
        DataFrame: Method 4 gap candidates with carry-forward logic applied.

    Raises:
        Exception: If the comparison operation fails.
    """
    try:
        logger.info("Identifying method 4 (Persistent Condition Carryforward) candidates")

        current_year = ensure_columns(
            current_year_df,
            ["MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS"],
        )
        persistence = ensure_columns(
            persistence_df,
            ["MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS"],
        )

        logger.info("Extracting prior year conditions from persistence data")
        prior_year_conditions = (
            persistence.filter(F.col("RISK_YEAR") != risk_year)
            .select("MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS")
            .distinct()
        )
        prior_count = prior_year_conditions.count()
        logger.info(f"Found {prior_count} prior year conditions")

        logger.info("Extracting current year conditions")
        current_year_conditions = (
            current_year.filter(F.col("RISK_YEAR") == risk_year)
            .select("MEMBER_ID", "HOME_PLAN_ID_CD", "CONTRACT_ID", "HCC_CODE", "HCC_LABEL", "DIAG_CD", "LAST_DOS")
            .distinct()
        )
        current_count = current_year_conditions.count()
        logger.info(f"Found {current_count} current year conditions")

        candidates = (
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

        candidate_count = candidates.count()
        logger.info(f"Identified {candidate_count} method 4 candidates (carry-forward gaps)")
        return candidates

    except Exception as e:
        logger.error(f"Error identifying method 4 candidates: {e}")
        raise RuntimeError(f"Failed to identify method 4 candidates: {e}") from e


def apply_suppression_logic(draft_gaps: DataFrame) -> DataFrame:
    """Apply the documented suppression steps: documentation and plan exclusion flags.

    Initializes suppression flags and frequency counters for draft gaps. These flags
    will be updated by downstream processing to identify excluded or suppressed gaps.

    Args:
        draft_gaps (DataFrame): The draft gaps DataFrame.

    Returns:
        DataFrame: Draft gaps with suppression flags and frequency counts initialized.

    Raises:
        ValueError: If the input DataFrame is None or empty.
    """
    try:
        if draft_gaps is None:
            raise ValueError("Draft gaps DataFrame cannot be None")

        draft_count = draft_gaps.count()
        if draft_count == 0:
            logger.warning("Draft gaps DataFrame is empty")
            return draft_gaps

        logger.info(f"Applying suppression logic to {draft_count} draft gaps")
        result = (
            draft_gaps.withColumn("IS_SUPPRESSED_1", F.lit("N"))
            .withColumn("IS_SUPPRESSED_2", F.lit("N"))
            .withColumn("FREQUENCY_COUNT", F.lit(1))
        )
        logger.info("Suppression flags initialized")
        return result

    except Exception as e:
        logger.error(f"Error applying suppression logic: {e}")
        raise RuntimeError(f"Failed to apply suppression logic: {e}") from e


def enrich_with_member_details(
    draft_gaps: DataFrame,
    member_df: DataFrame,
    diag_df: Optional[DataFrame] = None,
) -> DataFrame:
    """Attach member and diagnosis context to the draft gap records.

    Enriches draft gap records by joining with member demographics and diagnosis details
    to provide complete context for gap analysis and outreach.

    Args:
        draft_gaps (DataFrame): The draft gaps DataFrame.
        member_df (DataFrame): The member demographics DataFrame.
        diag_df (DataFrame, optional): The diagnosis details DataFrame. Defaults to None.

    Returns:
        DataFrame: Draft gaps enriched with member and diagnosis details.

    Raises:
        ValueError: If draft_gaps or member_df are None.
        Exception: If join operations fail.
    """
    try:
        if draft_gaps is None:
            raise ValueError("Draft gaps DataFrame cannot be None")
        if member_df is None:
            raise ValueError("Member DataFrame cannot be None")

        logger.info("Enriching draft gaps with member details")
        base = draft_gaps

        if member_df is not None:
            member_df = ensure_columns(
                member_df,
                ["MEMBER_ID", "MEMBER_FIRST_NAME", "MEMBER_LAST_NAME", "DATE_OF_BIRTH", "GENDER", "AGE"],
            )
            base = base.join(member_df, on=["MEMBER_ID"], how="left")
            logger.info(f"Joined member data to {base.count()} records")

        if diag_df is not None:
            logger.info("Enriching draft gaps with diagnosis details")
            diag_df = ensure_columns(
                diag_df,
                ["MEMBER_ID", "DIAG_CD", "DIAG_DESCRIPTION", "PROVIDER_NPI", "PROVIDER_NAME", "FACILITY_TYPE_CD", "CLAIM_TYPE"],
            )
            base = base.join(diag_df, on=["MEMBER_ID", "DIAG_CD"], how="left")
            logger.info(f"Joined diagnosis data to {base.count()} records")

        logger.info("Gap enrichment completed successfully")
        return base

    except Exception as e:
        logger.error(f"Error enriching draft gaps with member details: {e}")
        raise RuntimeError(f"Failed to enrich draft gaps: {e}") from e


def save_delta_table(df: DataFrame, target_table: str, partition_cols: Optional[Sequence[str]] = None) -> None:
    """Persist a DataFrame as a Delta table using overwrite semantics.

    Writes a DataFrame to a Delta table with optional partitioning, allowing schema evolution.
    Uses overwrite mode to replace previous data.

    Args:
        df (DataFrame): The DataFrame to persist.
        target_table (str): The fully qualified table name (schema.table).
        partition_cols (Sequence[str], optional): List of column names to partition by.
            Defaults to None (no partitioning).

    Returns:
        None

    Raises:
        ValueError: If the DataFrame is None or the target table name is invalid.
        Exception: If the write operation fails.
    """
    try:
        if df is None:
            raise ValueError("DataFrame cannot be None")
        if not target_table:
            raise ValueError("Target table name cannot be empty")

        record_count = df.count()
        logger.info(f"Saving {record_count} records to Delta table: {target_table}")

        if record_count == 0:
            logger.warning(f"DataFrame for table {target_table} is empty")

        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            logger.info(f"Partitioning table by: {list(partition_cols)}")
            writer = writer.partitionBy(*partition_cols)

        writer.option("overwriteSchema", "true").saveAsTable(target_table)
        logger.info(f"Successfully saved {record_count} records to {target_table}")

    except Exception as e:
        logger.error(f"Error saving DataFrame to Delta table {target_table}: {e}")
        raise RuntimeError(f"Failed to save table {target_table}: {e}") from e


def build_final_output(
    draft_gaps: DataFrame,
    draft_details: DataFrame,
    risk_year: str,
    source_load_month: str,
    cycle_run: str,
) -> DataFrame:
    """Create the final suspected gap output table from the suppressed draft data.

    Filters draft gaps to include only non-suppressed records and formats the final
    output table with all required dimensions and measures for gap reporting.

    Args:
        draft_gaps (DataFrame): The draft gaps with suppression flags.
        draft_details (DataFrame): The enriched draft gaps with member and diagnosis details.
        risk_year (str): The risk year for this analysis.
        source_load_month (str): The source load month for this analysis.
        cycle_run (str): The cycle run identifier.

    Returns:
        DataFrame: Final output DataFrame ready for reporting and outreach.

    Raises:
        ValueError: If draft_details is None.
        Exception: If the filtering or selection operations fail.
    """
    try:
        if draft_details is None:
            raise ValueError("Draft details DataFrame cannot be None")

        logger.info(f"Building final output for risk_year={risk_year}, load_month={source_load_month}")

        unsuppressed_gaps = (
            draft_details.filter(F.col("IS_SUPPRESSED_1") == "N")
            .filter(F.col("IS_SUPPRESSED_2") == "N")
        )
        unsuppressed_count = unsuppressed_gaps.count()
        logger.info(f"Found {unsuppressed_count} unsuppressed gaps out of {draft_details.count()} total draft gaps")

        final_output = (
            unsuppressed_gaps
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

        final_count = final_output.count()
        logger.info(f"Final output contains {final_count} suspected gaps")
        return final_output

    except Exception as e:
        logger.error(f"Error building final output: {e}")
        raise RuntimeError(f"Failed to build final output: {e}") from e
