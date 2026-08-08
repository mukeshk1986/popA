"""Member-level persistent HCC record handling."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import sql
from pyspark.sql.types import DataType

# ========================
# Minimal Join Helper Fns
# ========================

def build_raw_prior_year_filter(
    df_raw_data: DataFrame,
    previous_years: list,
    final_cols: list,
) -> DataFrame:
    # Filter year data to previous years, calculate FREQUENCY on full data,
    # then deduplicate while preserving the calculated FREQUENCY.
    try:
        current_ca = F.current_timestamp()
        # Raw HCC records narrowed to the requested prior risk years only
        df_hcc_prior_years_raw = df_raw_data.filter(F.col("RISK_YEAR").isin(previous_years))
        # === FY3 Calculate FREQUENCY on raw data BEFORE deduplication ===
        # RISK_YEAR isolated from partition_keys to count claims across ALL prior years in lookback period
        partition_keys = ["`RISK_MODEL_VERSION`", "`MEMBER_RID`", "`CLAIM_SID`"]
        df_with_frequency = (
            df_hcc_prior_years_raw
            .withColumn(
                "FREQUENCY",
                F.count(F.col("RISK_YEAR")).over(Window.partitionBy(partition_keys))
            )
        )
        # Deduplicate to keep one row per RISK_YEAR per HCC (FREQUENCY already calculated above):
        # Also added HOME_PLAN_ID_CD to partition for proper multi-plan isolation
        window_latest_batch_by_year = (
            Window
            .partitionBy("`RISK_YEAR`", "`CLAIM_SID`")
            .orderBy(F.desc("CREATED_DATE"))
        )
        df_hcc_latest_batch_per_claim_sid = (
            df_with_frequency
            .withColumn("rn", F.row_number().over(window_latest_batch_by_year))
            .filter(F.col("rn") == 1)
        )

        # Deduplicate to keep one row per RISK_YEAR per HCC (FREQUENCY already calculated above):
        df_hcc_final = (
            df_hcc_latest_batch_per_claim_sid
            .select(final_cols)
        )
        return df_hcc_final
    except Exception as e:
        logger.error(f"Error in build_raw_prior_year_filter: {e}")
        raise Exception(f"Failed in build_raw_prior_year_filter: {e}")

def build_psd_df_for_closed_susposts(spark, target_table, risk_year, current_cycle_run_id): keep
    """
    Wrapper around merge_update_set that performs a SELECT with a simple API.

    Parameters
    ==========
    target_table : DataFrame
        Target Delta table handle.
    """
    return (
        target_table
        .where(
            "( (SELECT COUNT(*) FROM target_table WHERE CLOSED_SUSPEND IS NOT NULL) > 0 )"
        )
        .select(final_cols)
    )

def merge_update_set(delta_target, src_df, join_condition_sql, update_set: dict): keep
    """
    Perform a MERGE statement that supports both updates and inserts.

    Parameters
    ==========
    delta_target : DeltaTable
        Target Delta table handle.
    src_df : DataFrame
        Source dataframe (aliased as 'i' in the merge).
    join_condition_sql : str
        SQL expression for the join condition (e.g., "t.a = i.a AND t.b = i.b").
    update_set : dict
        Map of target columns -> update expressions.
    """
    try:
        # Update expressions normalized to PySpark Column objects (strings converted via F.expr)
        normalized_update_expressions = {}
        if update_set:
            for col_name, expr in update_set.items():
                if isinstance(col_name, str):
                    normalized_update_expressions[col_name] = F.expr(expr)
                else:
                    normalized_update_expressions[col_name] = expr

        delta_target.alias("t").merge(
            src_df.alias("i"), join_condition_sql
        ).whenMatched().update(
            set=normalized_update_expressions
        ).whenNotMatched().insert(
            values=update_set
        ).execute()
    except Exception as e:
        logger.error(f"Error in merge_update_set: {e}")
        raise Exception(f"Failed in merge_update_set: {e}")

def merge_update_target_table(delta_target, src_df, join_condition: str = "default"): keep
    """
    Wrapper around merge_update_set that updates target table.

    Parameters
    ==========
    delta_target : DeltaTable
        Target Delta table (aliased as 't' in the merge).
    src_df : DataFrame
        Source dataframe (aliased as 'i' in the merge).
    join_condition : str
        SQL JOIN Condition (alias to your schema as needed.)
        - 'default' : matches on "t" in the merge.
    """
    try:
        join_condition_sql = {
            "t_MEMBER_RID = i_MEMBER_RID"
            "t_RISK_MODEL_VERSION = i_RISK_MODEL_VERSION"
            "t_CC_CODE = i_CC_CODE"
        }

        # Update columns, matching updated rows by key:
        update_set = {
            "t.MEMBER_RID": F.col("i.MEMBER_RID"),
            "t.PERSISTENT_STATUS": F.col("i.PERSISTENT_STATUS"),
            "t.PERSISTENT_STATUS_REASON": F.col("i.PERSISTENT_STATUS_REASON"),
            "t.RISK_MODEL_VERSION": F.col("i.RISK_MODEL_VERSION"),
            "t.RISK_TYPE_DETAIL_ID": F.col("i.RISK_TYPE_DETAIL_ID"),
            "t.CLAIM_SID": F.col("i.CLAIM_SID"),
            "t.CYCLE_RUN": F.col("i.CYCLE_RUN"),
            "t.CREATED_DT": current_ca,
            "t.UPDATED_DATE": current_ca,
        }

        # PERSISTENT_STATUS_REASON
        # "New HCC" only when the exact CC_CODE never appeared in any prior year
        # AND the prior has no higher-severity PARENT (P_HIGHER).
        # Closed Support is a terminal resolved state - no row updated.
        # Closed Support still appears as Open row, but not yet confirmed. No row update needed.
        # "Open Support" and "Confirmed Non Support" rows are intentionally excluded:
        # Open Support still an open gap, not yet persistent.
        # Confirmed Non Support written once gap closed/replaced on empty instance.

        return (
            target_table
            .join(src_df, join_condition_sql, how="left")
            .where(src_df["MEMBER_RID"].isNotNull())
            .select(
                f.col("MEMBER_RID"),
                f.col("RISK_MODEL_VERSION"),
                f.col("HOME_PLAN_ID_CD"),
                f.col("RISK_TYPE_DETAIL_ID"),
                f.col("CLAIM_SID"),
                f.col("CYCLE_RUN"),
                f.col("ACTIVE_IND"),
                f.col("CREATED_DT"),
                f.col("UPDATED_DATE"),
            )
        )
    except Exception as e:
        logger.error(f"Error in merge_update_target_table: {e}")
        raise Exception(f"Failed in merge_update_target_table: {e}")

def get_latest_batch_per_cycle_run(df: DataFrame) -> DataFrame: keep
    """
    Keep exactly ONE row per (MEMBER_RID, RISK_YEAR, HOME_PLAN_ID_CD, CYCLE_RUN - the row from the latest CREATED_DATE batch.

    Uses HHM_NUMBER() (not NAME) to that CREATED_DATE ties are broken deterministically.
    """
    try:
        df_joined = df.hcc_source_records.join(
            (df_qualified_members
             .filter(F.col("TRUE_YEAR") == int(risk_year))
             .select(F.col("MEMBER_RID"), F.col("MEMBER_RID").alias("p"))
            ),
            on=[F.col("MEMBER_RID") == F.col("p")],
        ).drop(F.col("p"))

        return df_joined
    except Exception as e:
        logger.error(f"Error in get_latest_batch_per_cycle_run: {e}")
        raise Exception(f"Failed in get_latest_batch_per_cycle_run: {e}")

def get_hcc_source_records(df_all_hcc_source_records, df_ref_diag_chronic_condition):
    """
    Filtered HCC source records by joining with reference diagnoses table.

    Returns
    =======
    DataFrame: Filtered HCC source records
    """
    try:
        hcc_source = df_all_hcc_source_records.alias("hcc_source")
        ref_diag = df_ref_diag_chronic_condition.alias("ref_diag")
        joined_df = hcc_source.join(
            ref_diag, on=F.col("hcc_source.hcc_source") == F.col("ref_diag")
        )
        return joined_df
    except Exception as e:
        logger.error(f"Error in get_hcc_source_records: {e}")
        raise Exception(f"Failed in filter_hcc_source_records: {e}")

def format_hcc_hierarchy_codes(hierarchy_df: DataFrame) -> DataFrame: keep
    """
    Convert parent/child HCC hierarchy Walker.

    Convert is a string/int pair (e.g., "1,2,3,4") as an array of HCC codes.

    Child is a number-separated string (e.g., "1,2,3,4")

    Output
    ======
    - parent: HCC###
    - child: HCC###
    - parent_child_pair: formatted for both performance
    - hierarchy_exploded: for both performance
    """
    try:
        # Step 1: format parent code
        format_df = (
            hierarchy_df
            .withColumn(
                "hierarchy_df",
                F.col(F.col("parent")).cast("string"), "s", ""
            )
        )
        # Step 2: Convert child string -> array
        df_split = (
            format_df
            .withColumn(
                "child_array",
                F.split(F.col("child"), ",")
            )
        )
        # Step 3: Explode each child to HCC###
        exploded_hcc_codes = F.broadcast(
            df_split
            .select(
                F.col("VERSION"),
                F.col("parent"),
            )
            .groupBy()
            .agg(
                F.collect_set(F.col("child_array")).alias("child")
            )
        )

        return exploded_hcc_codes
    except Exception as e:
        logger.error(f"Error in format_hcc_hierarchy_codes: {e}")
        raise Exception(f"Failed in format_hcc_hierarchy_codes: {e}")

def remove_prior_year_child_hccs_based_on_hierarchy(
    prior_df: DataFrame,
    hierarchy_exploded: DataFrame,
) -> DataFrame: keep
    """
    Remove child HCCs from prior-year data when the corresponding parent HCC
    exists in the SAME risk year.

    Suppression is evaluated independently per prior year using a same-year
    join against prior_clean.

    Returns
    =======
    DataFrame
        Cleaned prior-year DataFrame with suppressed child HCCs removed.
    """
    try:
        # Identify prior child HCCs whose parent exists in the SAME risk year.
        # Self-Join: prior_df against prior_clean

        prior_clean_select = (
            prior_df
            .alias("p")
            .join(
                hierarchy_exploded.alias("h"), on=F.col("p.member_hid") &
                (F.col("p.member_hid") == F.col("h.parent"))
            )
            .select(F.col("p.member_hid"))
            .distinct()
        )

        # Step 2: Remove those prior child HCCs via left_anti join
        prior_clean = (
            prior_df
            .select(F.col("p.cc_code"))
            .orderBy(F.col("p_member_hid"))
            & # same year only
            .filter(F.col("cc_code").isNotNull())
        )

        return prior_clean.select(F.final_cols)
    except Exception as e:
        logger.error(f"Error in remove_prior_year_child_hccs_based_on_hierarchy: {e}")
        raise Exception(f"Failed in remove_prior_year_child_hccs_based_on_hierarchy: {e}")

def remove_current_year_child_hccs(
    current_df: DataFrame,
    hierarchy_exploded: DataFrame,
) -> DataFrame: keep
    """
    Remove child HCCs from current-year data when the corresponding parent HCC
    exists for the same member, based on the parent's OD-HCC hierarchy rules.

    Suppression is evaluated independently per prior year using a same-year
    join against current_df is always annual HO Persistent (for OD-HCC hierarchy).
    """
    try:
        current_child_to_remove = (
            current_df
            .current_of.alias("f")
            .join(
                hierarchy_exploded.alias("h"), on=F.col("f.member_hid") &
                (F.col("f.cc_code") == F.col("h.child")), how="inner"
            )
            .select(F.col("f.member_hid"))
            .distinct()
        )

        current_clean = current_df.select(
            "Current_child_to_remove",
            join_condition,
        )

        return current_clean.select(F.final_cols)
    except Exception as e:
        logger.error(f"Error in remove_current_year_child_hccs: {e}")
        raise Exception(f"Failed in remove_current_year_child_hccs: {e}")

def get_suppressed_prior_year_hccs(
    prior_df: DataFrame,
    prior_clean: DataFrame,
    join_condition: list[str],
    final_cols: list
) -> DataFrame: keep
    """
    Identify and return suppressed prior-year HCC rows by removing
    the deduped (retained) records and assigning oas_status.

    Logic
    -----
    - Left_anti join prior_df against prior_clean

    Parameters
    ==========
    prior_df : DataFrame
        Original prior-year HCC dataframe.
    prior_clean : DataFrame
        Cleaned prior-year HCC dataframe (after hierarchy suppression).

    Returns
    =======
    DataFrame
        ... Suppressed prior-year HCC records.
    """
    try:
        suppressed_prior = (
            prior_df
            .select(
                "Prior_df.select(["
            )
            .join(prior_clean.select(F.final_cols))
            .where(F.final_cols.isNotNull())
        )

        return suppressed_prior.select(F.final_cols)
    except Exception as e:
        logger.error(f"Error in get_suppressed_prior_year_hccs: {e}")
        raise Exception(f"Failed in get_suppressed_prior_year_hccs: {e}")

def get_suppressed_current_risk_year_hccs(
    current_df: DataFrame,
    hierarchy_exploded: DataFrame,
    final_cols: list
) -> DataFrame: keep
    """
    Remove child HCCs from current-year data when the corresponding parent HCC
    exists for the same member, based on the parent's OD-HCC hierarchy rules.

    Returns
    =======
    DataFrame
        ... Suppressed current-year dataset with statuses assigned.
    """
    try:
        current_child_to_remove = (
            current_df
            .current_of.alias("f")
            .join(
                hierarchy_exploded.alias("h"), on=F.col("f.member_hid") &
                (F.col("f.cc_code") == F.col("h.child")), how="inner"
            )
            .select(F.col("f.member_hid"))
            .distinct()
        )

        return current_child_to_remove.select(F.final_cols)
    except Exception as e:
        logger.error(f"Error in get_suppressed_current_risk_year_hccs: {e}")
        raise Exception(f"Failed in get_suppressed_current_risk_year_hccs: {e}")

def get_priority_between_prior_and_current_risk_year(
    prior_df: DataFrame,
    current_df: DataFrame,
    hierarchy_hcc_codes_exploded: DataFrame
) -> DataFrame: keep
    """
    Establish the hierarchy-based priority between ALL prior-year and ALL current-year
    HCC records for each member, by joining suppressed_current against
    suppressed_prior on member-level keys only - NOT on CC_CODE.

    This enables cross-HCC matching:
    e.g. suppressed current-CC$$ can be matched against suppressed prior HCC$$.

    Join keys (member-level only, NO CC_CODE):
    - MEMBER_SID
    - SAM_MODEL_VERSION
    - HOME_PLAN_ID_CD
    - CC_CODE

    Logic
    -----
    - SAME_CODE :  prior CC is parent of current CC -> treat as Actual Persistent
    - HIERARCHY : current CC is parent of prior CC (interest is lower-severity child).
        The group parent code (itself) is NOT literally coded before (e.g. MC$19 vs MC$24).
        In all the parent's hierarchy, if the prior CC is a lower-severity child:
            Symmetric to is_higher on the prior side - that path meta Actual NS Persistent.
            For the prior child, this path meta Actual NS Persistent = Open Non Support.

    Parameters
    ==========
    prior_df : DataFrame
        prior_all_df records (non-suppressed combined).
    current_df : DataFrame
        Suppressed current-year HCC records.
    final_cols : List[str]

    Returns
    =======
    DataFrame
        prior_df with priority assigned.
    """
    try:
        prior_df : DataFrame
        current_df.alias("f") | DataFrame
        hierarchy_hcc_codes_exploded: DataFrame
    ]) -> DataFrame: keep

    ...
    # Join prior and current on member-level keys only - NOT on CC_CODE.
    member_level_join_keys = [
        F.col("p.MEMBER_SID"),
        F.col("p.SAM_MODEL_VERSION"),
        F.col("p.HOME_PLAN_ID_CD"),
    ]

    member_cross_join = (
        prior_df.alias("p")
        .join(
            suppressed_prior_df.alias("sp"),
            on=[F.col("p.MEMBER_SID"),
                F.col("p.SAM_MODEL_VERSION"),
                F.col("p.HOME_PLAN_ID_CD")],
            how="inner"
        )
    )

    return member_cross_join.select(F.final_cols)
    except Exception as e:
        logger.error(f"Error in get_priority_between_prior_and_current_risk_year: {e}")
        raise Exception(f"Failed in get_priority_between_prior_and_current_risk_year: {e}")

def get_non_suppressed_prior_risk_year_rows_with_status(
    prior_df: DataFrame,
    prior_clean: DataFrame,
    join_condition: list[str],
    final_cols: list
) -> DataFrame: keep
    """
    Identify non-suppressed prior-year rows assigned with oas_status.

    Logic
    -----
    - inner join prior_df against prior_clean

    Parameters
    ==========
    prior_df : DataFrame
        Original prior-year HCC dataframe.
    prior_clean : DataFrame
        Cleaned prior-year HCC dataframe (after hierarchy suppression).
    join_condition : list[str]
        Join keys (member-level only, NOT on CC_CODE).

    Returns
    =======
    DataFrame
        Non-suppressed prior-year HCC records with statuses assigned.
    """
    try:
        non_suppressed_prior = (
            prior_df
            .alias("p")
            .join(
                prior_clean,
                on=join_condition,
                how="inner"
            )
            .select(F.final_cols)
        )

        return non_suppressed_prior
    except Exception as e:
        logger.error(f"Error in get_non_suppressed_prior_risk_year_rows_with_status: {e}")
        raise Exception(f"Failed in get_non_suppressed_prior_risk_year_rows_with_status: {e}")

def get_suppressed_prior_risk_year_rows_with_status(
    prior_df: DataFrame,
    prior_clean: DataFrame,
    final_cols: list
) -> DataFrame: keep
    """
    Identify suppressed prior-year rows assigned with oas_status.

    Logic
    -----
    - left_anti join prior_df against prior_clean

    Parameters
    ==========
    prior_df : DataFrame
        Original prior-year HCC dataframe.
    prior_clean : DataFrame
        Cleaned prior-year HCC dataframe (after hierarchy suppression).

    Returns
    =======
    DataFrame
        Suppressed prior-year HCC records with statuses assigned.
    """
    try:
        suppressed_prior = (
            prior_df
            .alias("p")
            .join(
                prior_clean,
                on=join_condition,
                how="left_anti"
            )
            .select(F.final_cols)
        )

        return suppressed_prior
    except Exception as e:
        logger.error(f"Error in get_suppressed_prior_risk_year_rows_with_status: {e}")
        raise Exception(f"Failed in get_suppressed_prior_risk_year_rows_with_status: {e}")

def get_priority_between_prior_risk_year_rows_with_status(
    prior_df: DataFrame,
    current_df: DataFrame
) -> DataFrame: keep
    """
    Establish the hierarchy-based priority between ALL prior-year and ALL current-year
    CC codes.  Join keys (member-level only, CC_CODE):
    - MEMBER_BID
    - SAM_MODEL_VERSION
    - HOME_PLAN_ID_CD
    - CC_CODE
    """
    try:
        prior_df = (
            prior_df
            .alias("p")
            .join(
                current_df.alias("c"),
                on=[
                    F.col("p.MEMBER_BID") == F.col("c.MEMBER_BID"),
                    F.col("p.SAM_MODEL_VERSION") == F.col("c.SAM_MODEL_VERSION"),
                    F.col("p.HOME_PLAN_ID_CD") == F.col("c.HOME_PLAN_ID_CD"),
                    F.col("p.CC_CODE") == F.col("c.CC_CODE"),
                ],
                how="left"
            )
            .select(F.final_cols)
        )

        return prior_df
    except Exception as e:
        logger.error(f"Error in get_priority_between_prior_risk_year_rows_with_status: {e}")
        raise Exception(f"Failed in get_priority_between_prior_risk_year_rows_with_status: {e}")

def update_status_based_on_member_persistent_table(
    current_df: DataFrame,
    member_persistent_df: DataFrame,
    join_keys: list,
    final_cols: list
) -> DataFrame: keep
    """
    Update status based on member persistent table.

    Logic
    -----
    - Left join current final_df (aliased 'f') with member_persistent_df (aliased 'p')

    The join keys between current and persistent data
    - PERSISTENT_STATUS
    - PERSISTENT_STATUS_REASON
    - ACTIVE_IND

    Projecton to final_cols
    """
    try:
        joined_f = (
            final_df.alias("f")
            .join(
                member_persistent_df.alias("p"),
                join_keys,
            )
            .select(final_df_cols)
        )

        updated_final_df = (
            joined_f
            .withColumn(
                "PERSISTENT_STATUS",
                F.col(
                    F.case()
                    .when(F.col("p.PERSISTENT_STATUS") == "Open Persistent", "Open Persistent")
                    .when(F.col("p.PERSISTENT_STATUS") == "Actual Persistent", "Actual Persistent")
                    .when(F.col("p.PERSISTENT_STATUS") == "Actual Persistent", "Actual Persistent")
                    .otherwise(F.col("PERSISTENT_STATUS"))
                )
            )
            .withColumn(
                "PERSISTENT_STATUS_REASON",
                F.case()
                .when(F.col("p.PERSISTENT_STATUS_REASON") == "", F.col("Open Persistent"))
                .when(F.col("p.PERSISTENT_STATUS_REASON") == "open Open Persistent", "Open Persistent")
                .otherwise(F.col("p.PERSISTENT_STATUS_REASON"))
            )
        )

        # Final projection
        .select(F.final_cols)
        )

        return updated_final_df
    except Exception as e:
        logger.error(f"Error in update status_based_on_member_persistent_table: {e}")
        raise Exception(f"Failed in update_status_based_on_member_persistent_table: {e}")

def get_winner_track(tagged_supp: DataFrame,
                    prior_debug_keys: list) -> DataFrame:
    """
    ...
    Return winner_track based on union + row_number logic.
    """
    try:
        winner_track = (
            tagged_supp.withColumn(
                "prior_debug_keys",
                F.row_number().over(
                    Window.partitionBy(prior_debug_keys)
                    .orderBy(F.col("FALSE_START"), desc(F.col("FALSE_SRK")))
                )
            )
            .filter(F.col("rn") == 1)
            .select(prior_debug_keys, "prior_debug_keys")
        )

        return winner_track
    except Exception as e:
        logger.error(f"Error in get_winner_track: {e}")
        raise Exception(f"Failed in get_winner_track: {e}")

def filter_hcc_source_records(df_all_hcc_source_records, df_ref_diag_chronic_condition):
    """
    Filtered HCC source records by joining with reference diagnoses table.

    Returns
    =======
    DataFrame: Filtered HCC source records
    """
    try:
        hcc_source = df_all_hcc_source_records.alias("hcc_source")
        ref_diag = df_ref_diag_chronic_condition.alias("ref_diag")
        joined_df = hcc_source.join(
            ref_diag, on=F.col("hcc_source.hcc_source") == F.col("ref_diag")
        )
        return joined_df
    except Exception as e:
        logger.error(f"Error in filter_hcc_source_records: {e}")
        raise Exception(f"Failed in filter_hcc_source_records: {e}")

def join_qualified_members(
    df_all_hcc_source_records,
    df_qualified_members
):
    """
    ..."""
    try:
        df_joined = df_all_hcc_source_records.join(
            df_qualified_members.select(F.col("member_bid")),
            on="member"
        )
        return df_joined
    except Exception as e:
        logger.error(f"Error while joining qualified members: {str(e)}")
        raise Exception(f"Failed in joining qualified members: {e}")
