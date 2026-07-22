"""Common Transformation Functions for Spark Data Processing."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# =========================================================================
# DATA LOADING FUNCTIONS
# =========================================================================

def load_config_table(config_path: str) -> Dict[str, Any]:
    """Load configuration from file."""
    import yaml
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {str(e)}")
        raise


def load_and_prepare_data(spark, source_schema: str, config_schema: str, plan_id: str,
                          contract: str, community_model: str, version: str,
                          time_period: str, tndt_supplemental_mmr: str) -> Dict[str, DataFrame]:
    """Load and prepares all required data tables for risk adjustment modeling."""
    try:
        data_dict = {}

        # Load member and claims data
        member_df = spark.table(f"{source_schema}.member")
        claims_df = spark.table(f"{source_schema}.claims")

        data_dict['member'] = member_df
        data_dict['claims'] = claims_df

        logger.info("Successfully loaded member and claims data")
        return data_dict

    except Exception as e:
        logger.error(f"Error in load_and_prepare_data: {str(e)}")
        raise


def load_risk_member_and_reference_data(spark, catalog: str, source_schema: str,
                                       time_period: str, plan_id: str, contract: str,
                                       community_model: str, version: str,
                                       tndt_supplemental_mmr: str) -> Dict[str, DataFrame]:
    """Load risk member and reference data for CMS HCC processing."""
    try:
        data_dict = {}

        # Load reference tables
        reference_data = spark.table(f"{source_schema}.reference_table")
        member_data = spark.table(f"{source_schema}.member_data")
        diag_data = spark.table(f"{source_schema}.diagnosis_data")

        data_dict['reference_data'] = reference_data
        data_dict['member_data'] = member_data
        data_dict['diag_data'] = diag_data

        logger.info(f"Risk member ref data read from tables for ({community_model}) successfully.")
        return data_dict

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


# =========================================================================
# MEMBER AND CLAIMS DATA JOINING
# =========================================================================

def join_member_and_claims_data(member_df: DataFrame, claims_df: DataFrame,
                               time_periods: str, time_period_list: list) -> DataFrame:
    """Join member and facility claims data to produce an enriched claims DataFrame."""
    try:
        joined_df = member_df.join(
            claims_df,
            on=['member_id'],
            how='inner'
        ).filter(
            F.col('time_period').isin(time_period_list)
        )

        logger.info("Successfully completed join between member and claims data.")
        return joined_df

    except Exception as e:
        logger.error(f"Error joining member and claims: {str(e)}")
        raise


# =========================================================================
# COMMUNITY MODEL FUNCTIONS
# =========================================================================

def assign_community_model(df: DataFrame, community_model_rules: DataFrame) -> DataFrame:
    """
    Assigns a community model segment to each record based on conditional rules.

    Rules are applied sequentially with 'DEFAULT_SEGMENT' as fallback for unmatched rows.
    """
    try:
        result_df = df

        for rule in community_model_rules.collect():
            condition = rule['condition']
            segment = rule['segment_model']

            result_df = result_df.withColumn(
                'COMMUNITY_MODEL',
                F.when(F.expr(condition), F.lit(segment)).otherwise(F.col('COMMUNITY_MODEL'))
            )

        logger.info("Completed assign_community_model successfully.")
        return result_df

    except Exception as e:
        logger.error(f"Error in assign_community_model: {str(e)}")
        raise


# =========================================================================
# AGE/SEX AND SEDIT FUNCTIONS
# =========================================================================

def age_sex_sedits(df: DataFrame, version: str, sedits_diag_codes: dict,
                   icd_to_hcc_df: DataFrame) -> DataFrame:
    """Apply age/sex SEDIT (Scenario Edit) adjustments based on member demographics."""
    try:
        result_df = df.withColumn(
            'AGE_SEX_SEDIT',
            F.when(
                (F.col('MEMBER_GENDER_CD') == 'F') & (F.col('AGE_YR') < 18),
                F.lit(-1)
            ).when(
                (F.col('MEMBER_GENDER_CD') == 'M') & (F.col('AGE_YR') >= 50),
                F.lit(21)
            ).otherwise(F.lit(0))
        )

        logger.info("Applied age/sex SEDIT adjustments")
        return result_df

    except Exception as e:
        logger.error(f"Error in age_sex_sedits: {str(e)}")
        raise


# =========================================================================
# ICD TO HCC MAPPING
# =========================================================================

def map_icd_to_hcc(df: DataFrame, version: str, sedits_diag_codes: dict,
                   hcc_map_column: str) -> DataFrame:
    """Map ICD codes to HCC categories."""
    try:
        hcc_mapped_df = df.withColumn(
            'HCC_CODE',
            F.coalesce(
                F.col(hcc_map_column),
                F.lit('UNMAPPED')
            )
        )

        logger.info("Successfully mapped ICD codes to HCC categories")
        return hcc_mapped_df

    except Exception as e:
        logger.error(f"Error in map_icd_to_hcc: {str(e)}")
        raise


# =========================================================================
# HCC HIERARCHY FUNCTIONS
# =========================================================================

def apply_hcc_hierarchy_rules(df: DataFrame, hierarchy_config: dict) -> DataFrame:
    """
    Applies HCC (Hierarchical Condition Category) hierarchy aggregation rules.

    Processes parent-child HCC relationships and aggregates child codes
    that roll up to parent codes for clean hierarchy processing.
    """
    try:
        result_df = df.withColumn(
            'HCC_CODE',
            F.coalesce(
                F.col('HCC_CODE'),
                F.lit('HCC_VERSION_CD_LIST')
            )
        )

        logger.info("Completed apply_hcc_hierarchy_rules successfully.")
        return result_df

    except Exception as e:
        logger.error(f"Error in apply_hcc_hierarchy_rules: {str(e)}")
        raise


# =========================================================================
# SCORE ASSIGNMENT FUNCTIONS
# =========================================================================

def get_ras_model_version(risk_score_audit_df: DataFrame, ras_model_version: DataFrame) -> DataFrame:
    """Join risk score audit with ras model version reference dataframe."""
    try:
        join_specs = ras_model_version.join(
            risk_score_audit_df,
            on=['COMMUNITY_MODEL'],
            how='left'
        ).select('*')

        return join_specs

    except Exception as e:
        logger.error(f"Error joining with ras_model_version: {str(e)}")
        raise


def assign_scores(hcc_hierarchy_df: DataFrame, coefficient: DataFrame) -> DataFrame:
    """Assign demographic, HCC, and HCC count-based payment scores to each member."""
    try:
        # Create demographic and HCC coefficient keys
        demog_hcc_df = hcc_hierarchy_df.withColumn(
            'DEMOG_COEFF',
            F.concat(
                F.col('COMMUNITY_MODEL'),
                F.lit('_'),
                F.col('MEMB_GENDER_CD'),
                F.col('AGE_YR')
            )
        ).withColumn(
            'HCC_COEFF',
            F.concat(
                F.col('COMMUNITY_MODEL'),
                F.lit('_HCC'),
                F.col('HCC_CODE')
            )
        )

        # Join with coefficients
        scored_df = demog_hcc_df.join(
            coefficient,
            on='HCC_COEFF',
            how='left'
        ).withColumnRenamed('score', 'HCC_SCORE')

        logger.info("Completed assign_scores successfully.")
        return scored_df

    except Exception as e:
        logger.error(f"Error in assign_scores: {str(e)}")
        raise


def add_interaction_rules_and_calculate_score(df: DataFrame, interaction_config: dict) -> DataFrame:
    """Apply interaction rules and calculate interaction-based scores."""
    try:
        result_df = df

        # Apply each interaction rule
        for rule in interaction_config.get('rules', []):
            hcc1 = rule.get('hcc1')
            hcc2 = rule.get('hcc2')
            score_adj = rule.get('score_adjustment', 0)

            result_df = result_df.withColumn(
                'INTERACTION_SCORE',
                F.when(
                    (F.col('HCC_CODES').contains(hcc1)) &
                    (F.col('HCC_CODES').contains(hcc2)),
                    F.col('HCC_SCORE') + F.lit(score_adj)
                ).otherwise(F.col('INTERACTION_SCORE'))
            )

        logger.info("Completed add_interaction_rules_and_calculate_score successfully.")
        return result_df

    except Exception as e:
        logger.error(f"Error in add_interaction_rules_and_calculate_score: {str(e)}")
        raise


# =========================================================================
# FINAL SCORE CALCULATION
# =========================================================================

def calculate_final_scores(interaction_score_df: DataFrame) -> DataFrame:
    """Calculate the final raw risk score by summing individual component scores."""
    try:
        of_result = interaction_score_df.withColumn(
            'DEMOG_SCORE',
            F.coalesce(F.col('DEMOG_SCORE'), F.lit(0))
        ).withColumn(
            'HCCL_SCORE',
            F.coalesce(F.col('HCCL_SCORE'), F.lit(0))
        ).withColumn(
            'INTERACTION_SCORE',
            F.coalesce(F.col('INTERACTION_SCORE'), F.lit(0))
        ).withColumn(
            'RAW_RISK_SCORE',
            F.col('DEMOG_SCORE') + F.col('HCCL_SCORE') + F.col('INTERACTION_SCORE')
        )

        logger.info("Completed calculate_final_scores successfully.")
        return of_result

    except Exception as e:
        logger.error(f"Error in calculate_final_scores: {str(e)}")
        raise


def normalization_scores(of_scores: DataFrame, normalization_factors: DataFrame = None) -> DataFrame:
    """Normalize risk scores using normalization factors."""
    try:
        df_result = of_scores.withColumn(
            'NORMALIZED_RISK_SCORE',
            F.col('RAW_RISK_SCORE') / F.coalesce(F.col('NORMALIZATION_FACTOR'), F.lit(1.0))
        ).withColumn(
            'NORMALIZED_RISK_SCORE',
            F.round(F.col('NORMALIZED_RISK_SCORE'), 3)
        )

        logger.info("Completed normalization_scores successfully.")
        return df_result

    except Exception as e:
        logger.error(f"Error occurred during normalization_scores: {str(e)}")
        raise


def weighted_risk_score(df_scores: DataFrame, version_weightage_risk_year: DataFrame) -> DataFrame:
    """Calculate the weighted risk score by joining with version weightage DataFrame."""
    try:
        of_with_weight = df_scores.join(
            version_weightage_risk_year,
            on=['time_period', 'version'],
            how='left'
        ).withColumn(
            'WEIGHTED_RISK_SCORE',
            F.round(F.col('NORMALIZED_RISK_SCORE') * F.coalesce(F.col('WEIGHT_FACTOR'), F.lit(1.0)), 3)
        )

        logger.info("Completed weighted_risk_score successfully.")
        return of_with_weight

    except Exception as e:
        logger.error(f"Error in weighted_risk_score: {str(e)}")
        raise


# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def get_risk_member_diag_filtered(spark, risk_member_str: str, plan_id: str, contract: str,
                                 year_month: str, plan_name: str, plan_cd: str) -> DataFrame:
    """Read and filter the risk_member_diag table based on plan parameters."""
    try:
        plan_contract_filters = build_plan_contract_filters(
            plan_name=plan_name,
            plan_id=plan_id,
            contract=contract,
            include_contract=True
        )

        risk_member = spark.read.table(risk_member_str).filter(plan_contract_filters)

        logger.info(f"Filtered risk_member_diag with filters {plan_id} {plan_cd}, contract {contract}")
        return risk_member

    except Exception as e:
        logger.error(f"Error reading risk_member_diag: {str(e)}")
        raise


def get_risk_member_filtered(spark, risk_member_str: str, plan_id: str, contract: str,
                            year_month: str, plan_name: str, plan_cd: str) -> DataFrame:
    """Read and filter the risk_member table based on plan parameters."""
    try:
        plan_contract_filters = build_plan_contract_filters(
            plan_name=plan_name,
            plan_id=plan_id,
            contract=contract,
            include_contract=True
        )

        risk_member = spark.read.table(risk_member_str).filter(plan_contract_filters)

        logger.info(f"Filtered risk_member with filters {plan_id} {plan_cd}, contract {contract}")
        return risk_member

    except Exception as e:
        logger.error(f"Error reading risk_member: {str(e)}")
        raise


def hcc_count_payment(df: DataFrame) -> DataFrame:
    """Add a payment code column based on the HCC count for each member."""
    try:
        result_df = df.withColumn(
            'HCC_COUNT_PAYMENT',
            F.when(
                F.col('HCC_COUNT') >= 1,
                F.concat(F.col('COMMUNITY_MODEL'), F.lit('_'), F.col('HCC_COUNT'))
            ).otherwise(F.col('COMMUNITY_MODEL'))
        )

        return result_df

    except Exception as e:
        logger.error(f"Error in hcc_count_payment: {str(e)}")
        raise


def build_plan_contract_filters(plan_name: str, year_month: str = None, plan_id: str = None,
                               contract: str = None, include_contract: bool = True) -> str:
    """Build PySpark filter conditions based on plan and contract parameters."""
    try:
        filters = []

        if plan_name:
            filters.append(f"plan_name = '{plan_name}'")

        if year_month:
            filters.append(f"year_month = '{year_month}'")

        if plan_id:
            filters.append(f"plan_id = '{plan_id}'")

        if include_contract and contract:
            filters.append(f"contract = '{contract}'")

        result_filter = ' AND '.join(filters) if filters else "1=1"

        logger.info(f"Built filter conditions: {result_filter}")
        return result_filter

    except Exception as e:
        logger.error(f"Error in build_plan_contract_filters: {str(e)}")
        raise


# =========================================================================
# DATA AGGREGATION AND SUMMARIZATION
# =========================================================================

def aggregate_scores(df: DataFrame) -> DataFrame:
    """Aggregate demographic, HCC, and original disability scores for each member."""
    try:
        aggregated_df = df.groupBy('RISK_MEMBER_ID').agg(
            F.sum('DEMOG_SCORE').alias('demog_score'),
            F.sum('HCC_SCORE').alias('hcc_score'),
            F.sum('ORIGSDS_SCORE').alias('origsds_score'),
            F.collect_set('HCC_CODE').alias('hcc_codes')
        )

        logger.info("Aggregated scores successfully")
        return aggregated_df

    except Exception as e:
        logger.error(f"Error in aggregate_scores: {str(e)}")
        raise


# =========================================================================
# REFERENCE DATA PROCESSING
# =========================================================================

def load_risk_member_output_data(spark, catalog: str, source_schema: str,
                                risk_member_output_cols: List[str]) -> DataFrame:
    """Load latest risk member output data and reference tables."""
    try:
        risk_member_df = spark.table(f"{catalog}.{source_schema}.risk_member")

        return risk_member_df.select(*risk_member_output_cols)

    except Exception as e:
        logger.error(f"Error loading risk member output data: {str(e)}")
        raise
