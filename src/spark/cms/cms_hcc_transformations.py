"""CMS HCC Transformation Functions and Utilities."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURATION FUNCTIONS
# =========================================================================

def get_original_disability(input_risk_score_df: DataFrame, score_config: DataFrame) -> DataFrame:
    """
    Calculate original disability using model and coefficients.

    Args:
        input_risk_score_df: Input DataFrame with member data.
        score_config: Configuration DataFrame with model coefficients.

    Returns:
        DataFrame with original disability calculation.
    """
    try:
        original_disability_df = input_risk_score_df.join(
            score_config,
            on="COMMUNITY_MODEL",
            how="left"
        ).select("*")

        logger.info("Successfully calculated original disability")
        return original_disability_df

    except Exception as e:
        logger.error(f"Error in get_original_disability: {str(e)}")
        raise Exception(f"Failed to calculate original disability: {str(e)}")


def get_original_disability_input_risk_score_df(df: DataFrame) -> DataFrame:
    """Prepare input risk score dataframe with required columns."""
    return df.select("*")


def assign_community_model(df: DataFrame, community_model_rules: dict) -> DataFrame:
    """
    Assign community model to each member based on demographics.

    Args:
        df: Input DataFrame with member demographic data.
        community_model_rules: Dictionary containing community model assignment rules.

    Returns:
        DataFrame with assigned community model.
    """
    try:
        community_model_df = df.withColumn(
            "COMMUNITY_MODEL",
            F.when(
                (F.col("MEMBER_GENDER_CD") == "F"),
                "COMMUNITY_MODEL_F"
            ).when(
                (F.col("MEMBER_GENDER_CD") == "M"),
                "COMMUNITY_MODEL_M"
            ).otherwise("COMMUNITY_MODEL_DEFAULT")
        )

        logger.info("Successfully assigned community model to members")
        return community_model_df

    except Exception as e:
        logger.error(f"Error in assign_community_model: {str(e)}")
        raise Exception(f"Failed to assign community model: {str(e)}")


# =========================================================================
# AGE/SEX SEDIT FUNCTIONS
# =========================================================================

def age_sex_sedits(df: DataFrame, version: str, sedits_diag_codes: dict, icd_to_hcc_df: DataFrame) -> DataFrame:
    """
    Apply CC assignment logic based on the specified version.

    Applies CC assignment logic based on the specified version (v24 or v28) to the input DataFrame.

    Args:
        df (DataFrame): Input DataFrame containing member demographic and diagnosis code data.
        version (str): Version of the CC assignment logic to apply (v24 or v28).
        sedits_diag_codes (dict): Dictionary containing CC codes for different conditions.
        icd_to_hcc_df (DataFrame): ICD to HCC mapping DataFrame.

    Returns:
        DataFrame: A new DataFrame with an additional column 'CMS-HCC-Model-Category-[version]'
                   containing the assigned CC values.

    Raises:
        ValueError: If the specified version is not supported or invalid.
    """
    try:
        version_rules = {
            "V24": [
                {
                    "condition": (F.col("MEMBER_GENDER_CD") == "F"),
                    "value": 112,
                },
                {
                    "condition": ((F.col("AGE_YR") < 18) & (F.col("DIAG_CD").isin(sedits_diag_codes.get("SEDITS_DIAG1", [])))),
                    "value": -1,
                },
                {
                    "condition": ((F.col("AGE_YR") >= 50) & (F.col("DIAG_CD").isin(sedits_diag_codes.get("SEDITS_DIAG3", [])))),
                    "value": 21,
                },
                {
                    "condition": ((F.col("AGE_YR") >= 2) & (F.col("DIAG_CD").isin(sedits_diag_codes.get("SEDITS_DIAG4", [])))),
                    "value": -1,
                },
            ],
            "V28": [
                {
                    "condition": (F.col("MEMBER_GENDER_CD") == "F"),
                    "value": 45,
                },
                {
                    "condition": ((F.col("AGE_YR") < 18) & (F.col("DIAG_CD").isin(sedits_diag_codes.get("SEDITS_DIAG2", [])))),
                    "value": 112,
                },
                {
                    "condition": ((F.col("AGE_YR") < 6) & (F.col("DIAG_CD").isin(sedits_diag_codes.get("SEDITS_DIAG3", [])))),
                    "value": -1,
                },
            ],
        }

        if version not in version_rules:
            raise ValueError(f"Invalid version specified. Use 'V24' or 'V28'.")

        column_name = f"CMS_HCC_Model_Category_{version}"
        result_df = df

        for rule in version_rules[version]:
            condition = rule["condition"]
            value = rule["value"]
            result_df = result_df.withColumn(
                column_name,
                F.when(condition, F.lit(value)).otherwise(F.col(column_name))
            )

        logger.info(f"Successfully applied age/sex SEDIT logic for version {version}")
        return result_df

    except Exception as e:
        logger.error(f"Error applying CC assignment logic for version {version}: {str(e)}")
        raise Exception(f"Failed to apply CC assignment logic for version {version}: {str(e)}")


# =========================================================================
# ICD TO HCC MAPPING FUNCTIONS
# =========================================================================

def map_icd_to_hcc(df: DataFrame, version: str, sedits_diag_codes: dict, hcc_map_column: str) -> DataFrame:
    """
    Map ICD codes to HCC categories.

    Args:
        df: Input DataFrame with ICD codes.
        version: Model version for HCC mapping.
        sedits_diag_codes: Dictionary of SEDIT diagnosis codes.
        hcc_map_column: Name of the column containing HCC mapping.

    Returns:
        DataFrame with HCC categories assigned.
    """
    try:
        hcc_mapped_df = df.withColumn(
            "HCC_CODE",
            F.coalesce(
                F.col(hcc_map_column),
                F.lit("UNMAPPED")
            )
        )

        logger.info("Successfully mapped ICD codes to HCC categories")
        return hcc_mapped_df

    except Exception as e:
        logger.error(f"Error in map_icd_to_hcc: {str(e)}")
        raise Exception(f"Failed to map ICD codes to HCC: {str(e)}")


# =========================================================================
# HCC HIERARCHY FUNCTIONS
# =========================================================================

def apply_hcc_hierarchy_rules(df: DataFrame, hierarchy_config: dict) -> DataFrame:
    """
    Apply HCC hierarchy rules to remove redundant HCCs.

    Args:
        df: Input DataFrame with HCC codes.
        hierarchy_config: Dictionary containing hierarchy rules.

    Returns:
        DataFrame with hierarchy rules applied.
    """
    try:
        hierarchy_df = df

        for rule in hierarchy_config.get("rules", []):
            condition = rule.get("condition")
            exclude_hcc = rule.get("exclude_hcc")

            if condition and exclude_hcc:
                hierarchy_df = hierarchy_df.filter(
                    ~((F.col(condition.get("column", "")) == condition.get("value", "")) &
                      (F.col("HCC_CODE").isin(exclude_hcc)))
                )

        logger.info("Successfully applied HCC hierarchy rules")
        return hierarchy_df

    except Exception as e:
        logger.error(f"Error in apply_hcc_hierarchy_rules: {str(e)}")
        raise Exception(f"Failed to apply HCC hierarchy rules: {str(e)}")


# =========================================================================
# DEMOGRAPHIC AND HCC COEFFICIENT FUNCTIONS
# =========================================================================

def create_demog_and_hcc_coeff(df: DataFrame) -> DataFrame:
    """
    Create demographic and HCC coefficient keys.

    Creates demographic and HCC coefficient keys for score assignment.

    Args:
        df (DataFrame): DataFrame with member demographic and HCC data.

    Returns:
        DataFrame: DataFrame with 'DEMOG_COEFF' and 'HCC_COEFF' columns.
    """
    try:
        df = df.withColumn(
            "DEMOG_COEFF",
            F.when(
                F.col("COMMUNITY_MODEL").startswith("NA_"),
                F.lower(F.col("COMMUNITY_MODEL"))
            ).when(
                F.col("COMMUNITY_MODEL").startswith("RANGE_"),
                F.concat(F.col("COMMUNITY_MODEL"), F.lit("_"), F.col("MEMB_GENDER_CD"), F.col("AGE_YR"))
            ).otherwise(
                F.col("COMMUNITY_MODEL")
            )
        )

        return df.withColumn(
            "HCC_COEFF",
            F.concat(F.col("COMMUNITY_MODEL"), F.lit("_HCC"), F.col("HCC_CODE"))
        )

    except Exception as e:
        logger.error(f"Error in create_demog_and_hcc_coeff: {str(e)}")
        raise Exception(f"Failed to run create_demog_and_hcc_coeff: {str(e)}")


def add_origsds_score(df: DataFrame, coefficient: DataFrame) -> DataFrame:
    """
    Add originally disabled score based on gender and community model.

    Args:
        df (DataFrame): Input DataFrame.
        coefficient (DataFrame): Coefficient DataFrame with score mappings.

    Returns:
        DataFrame: DataFrame with 'ORIGSDS_SCORE' column added.
    """
    try:
        df = df.withColumn(
            "ORIGSDS_COEFF",
            F.when(
                (F.col("ORIGSDS") == 1),
                F.concat(
                    F.col("COMMUNITY_MODEL"),
                    F.lit("_ORIGSDS"),
                    F.col("MEMB_GENDER_CD"),
                    F.col("AGE_YR")
                ),
            ).otherwise(
                F.col("COMMUNITY_MODEL")
            )
        )

        result_df = df.join(
            coefficient.select("ORIGSDS_COEFF", "score"),
            on="ORIGSDS_COEFF",
            how="left"
        ).withColumnRenamed("score", "ORIGSDS_SCORE")

        return result_df

    except Exception as e:
        logger.error(f"Error in add_origsds_score: {str(e)}")
        raise Exception(f"Failed to run add_origsds_score: {str(e)}")


# =========================================================================
# SCORE ASSIGNMENT FUNCTIONS
# =========================================================================

def assign_scores(hcc_hierarchy_df: DataFrame, coefficient: DataFrame) -> DataFrame:
    """
    Assign demographic, HCC, and HCC count-based payment scores to each member.

    This function:
    - Creates demographic and HCC coefficient keys.
    - Joins with the coefficient table to assign demographic and HCC scores.
    - Aggregates scores at the member level.
    - Adds payment code and joins to assign HCC count-based payment scores.

    Args:
        hcc_hierarchy_df (DataFrame): Input DataFrame with member data and HCC codes.
        coefficient (DataFrame): Coefficient DataFrame containing coefficients.

    Returns:
        DataFrame: Final DataFrame with all calculated scores and HCC codes, including:
        - 'DEMOG_SCORE': Demographic score.
        - 'HCC_SCORE': HCC score.
        - 'ORIGSDS_SCORE': Original disability score.
        - 'HCC_CODES': Set of HCC codes.
        - 'HCC_COUNT_PAYMENT_SCORE': Payment score based on HCC count.

    Raises:
        Exception: If any error occurs during score assignment or aggregation.
    """
    try:
        explode_df = hcc_hierarchy_df.select("*")
        demog_hcc_df = create_demog_and_hcc_coeff(explode_df)

        demog_score_df = demog_hcc_df.join(
            coefficient.select("DEMOG_COEFF", "score"),
            on="DEMOG_COEFF",
            how="left"
        ).withColumnRenamed("score", "DEMOG_SCORE")

        hcc_score_df = demog_score_df.join(
            coefficient.select("HCC_COEFF", "score"),
            on="HCC_COEFF",
            how="left"
        ).withColumnRenamed("score", "HCC_SCORE")

        origsds_score_df = add_origsds_score(hcc_score_df, coefficient)

        logger.info("Completed assign_scores successfully.")
        return origsds_score_df

    except Exception as e:
        logger.error(f"Error in assign_scores: {str(e)}")
        raise Exception(f"Failed to run assign_scores: {str(e)}")


# =========================================================================
# INTERACTION SCORE FUNCTIONS
# =========================================================================

def add_interaction_rules_and_calculate_score(df: DataFrame, interaction_config: dict) -> DataFrame:
    """
    Apply interaction rules and calculate interaction-based scores.

    Assigns demographic, HCC, and HCC count-based payment scores to each member.

    Args:
        df (DataFrame): Input DataFrame with member data and HCC codes.
        interaction_config (dict): Configuration dictionary for interaction rules.

    Returns:
        DataFrame: Final DataFrame with all calculated scores and HCC codes.

    Raises:
        Exception: If any error occurs during score assignment or aggregation.
    """
    try:
        interaction_score_df = df

        for interaction_rule in interaction_config.get("rules", []):
            hcc1 = interaction_rule.get("hcc1")
            hcc2 = interaction_rule.get("hcc2")
            score_adjustment = interaction_rule.get("score_adjustment", 0)

            if hcc1 and hcc2:
                interaction_score_df = interaction_score_df.withColumn(
                    "INTERACTION_SCORE",
                    F.when(
                        (F.col("HCC_CODES").contains(hcc1)) & (F.col("HCC_CODES").contains(hcc2)),
                        F.col("HCC_SCORE") + F.lit(score_adjustment)
                    ).otherwise(F.col("HCC_SCORE"))
                )

        logger.info("Completed add_interaction_rules_and_calculate_score successfully.")
        return interaction_score_df

    except Exception as e:
        logger.error(f"Error in add_interaction_rules_and_calculate_score: {str(e)}")
        raise Exception(f"Failed to run add_interaction_rules_and_calculate_score: {str(e)}")


# =========================================================================
# FINAL SCORE CALCULATION FUNCTIONS
# =========================================================================

def calculate_final_scores(df: DataFrame) -> DataFrame:
    """
    Calculate final risk scores.

    Args:
        df: Input DataFrame with all component scores.

    Returns:
        DataFrame with final calculated risk scores.
    """
    try:
        final_scores_df = df.withColumn(
            "FINAL_RISK_SCORE",
            (F.col("DEMOG_SCORE") if "DEMOG_SCORE" in df.columns else F.lit(0)) +
            (F.col("HCC_SCORE") if "HCC_SCORE" in df.columns else F.lit(0)) +
            (F.col("INTERACTION_SCORE") if "INTERACTION_SCORE" in df.columns else F.lit(0))
        )

        logger.info("Successfully calculated final risk scores")
        return final_scores_df

    except Exception as e:
        logger.error(f"Error in calculate_final_scores: {str(e)}")
        raise Exception(f"Failed to calculate final scores: {str(e)}")


def normalization_scores(df: DataFrame) -> DataFrame:
    """
    Normalize scores using adjustment factors.

    Args:
        df: Input DataFrame with scores to normalize.

    Returns:
        DataFrame with normalized scores.
    """
    try:
        normalized_df = df.withColumn(
            "NORMALIZED_RISK_SCORE",
            F.col("FINAL_RISK_SCORE") *
            F.coalesce(F.col("NORMALIZATION_FACTOR"), F.lit(1.0))
        )

        logger.info("Successfully normalized scores")
        return normalized_df

    except Exception as e:
        logger.error(f"Error in normalization_scores: {str(e)}")
        raise Exception(f"Failed to normalize scores: {str(e)}")


def weighted_risk_score(df: DataFrame, version: str) -> DataFrame:
    """
    Apply weighted risk score calculation.

    Args:
        df: Input DataFrame with normalized scores.
        version: Model version for weighting.

    Returns:
        DataFrame with weighted risk scores.
    """
    try:
        weighted_df = df.withColumn(
            "WEIGHTED_RISK_SCORE",
            F.col("NORMALIZED_RISK_SCORE") *
            F.coalesce(F.col("WEIGHT_FACTOR"), F.lit(1.0))
        )

        logger.info("Successfully calculated weighted risk scores")
        return weighted_df

    except Exception as e:
        logger.error(f"Error in weighted_risk_score: {str(e)}")
        raise Exception(f"Failed to calculate weighted risk scores: {str(e)}")


# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def create_risk_member_hcc_summary(df: DataFrame, config_columns: list) -> DataFrame:
    """
    Create summary table with selected columns.

    Args:
        df: Input DataFrame.
        config_columns: List of columns to select.

    Returns:
        DataFrame with selected columns.
    """
    try:
        return df.select(*config_columns)
    except Exception as e:
        logger.error(f"Error in create_risk_member_hcc_summary: {str(e)}")
        raise Exception(f"Failed to create risk member HCC summary: {str(e)}")


def get_plan_name_plan_id(plan_id: str) -> tuple:
    """
    Get plan name from plan ID.

    Args:
        plan_id: Plan identifier.

    Returns:
        Tuple of (plan_name, plan_id).
    """
    plan_mapping = {
        "001": "Medicare Advantage",
        "002": "Medicare FFS",
        "003": "Medicaid"
    }
    return (plan_mapping.get(plan_id, "Unknown"), plan_id)
