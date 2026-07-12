# Databricks notebook source
# MAGIC %md
# MAGIC #<--------Gap Suspecting----------------->
# MAGIC #### Gap_Suspecting parameter:
# MAGIC #### {    "env": "qa",	"risk_year": 2025,	"claim_duration_years": 1, Note: number of calender years of claims data to be consider(1:risk year, 2: risk_year+ 1 prior year)
# MAGIC ####   "plan_name": "perftest",
# MAGIC ####    "reference_schema": "MA",	
# MAGIC #### "model": "CMS_MA", #default CMS_MA(pop_id-05),  in future we will addd HHS_ACA
# MAGIC #### "gap_methods": "1", #default is 1, we run all the three 1,2,3 with comma separated
# MAGIC ####"generate_extract": "NO" #default is NO , in future we need to enable "YES", when we build the extract functionality
# MAGIC ####}
# MAGIC

# COMMAND ----------

# DBTITLE 1,current year
from datetime import datetime
# Risk score model should have capability to run scoring for current and past 3 years
# Get the current year
current_year = datetime.now().year

# COMMAND ----------

# DBTITLE 1,Configuration widget Parameters
dbutils.widgets.dropdown("env", "DEV", ["DEV", "QA","STG","PROD"])
# Create widget with current year as default
dbutils.widgets.text("risk_year", str(current_year), "risk_year")
dbutils.widgets.text("claim_duration_years", "1")
dbutils.widgets.text("plan_name","")
dbutils.widgets.text("model","CMS_MA")
dbutils.widgets.dropdown("gap_methods","1",["1","2","10","4"])
dbutils.widgets.text("generate_extract","NO")
dbutils.widgets.dropdown("reference_schema", "MA",["MA","ACA"])
dbutils.widgets.dropdown("source_load_month", "01", [f"{i:02d}" for i in range(1, 13)], "")
dbutils.widgets.text("source_load_year", str(current_year), "")
dbutils.widgets.text("Threshold", ".75")

# COMMAND ----------

# DBTITLE 1,Import Libraries and Helper Functions
from src.spark.helpers.databricks_util import get_plan_name
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, year, regexp_extract, concat, current_timestamp
from src.spark.helpers.config_util import get_config_yaml

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get parameters
plan_name = dbutils.widgets.get("plan_name").lower()
env = dbutils.widgets.get("env").lower()
method_id = dbutils.widgets.get("gap_methods").strip()
risk_year = dbutils.widgets.get("risk_year").strip()
no_of_year = dbutils.widgets.get("claim_duration_years").strip()
source_month = dbutils.widgets.get("source_load_month").strip()
source_year = dbutils.widgets.get("source_load_year").strip()
threshold = dbutils.widgets.get("Threshold").strip()
source_load_year_month = source_year+"_"+source_month
env_bucket = "pop-"+env
catalog = "pop_"+env
ma_reference = dbutils.widgets.get("reference_schema").strip().lower()+"_reference"
conf_file_read_meta = get_config_yaml(f"../../../config/sql/file_read_meta.yaml")

# COMMAND ----------

# DBTITLE 1,Configuration Setup
# Set schema names
v_plan_name = get_plan_name(plan_name)
schema = v_plan_name+"transformation"
schema_curation = v_plan_name+"curation"
schema_ingestion = v_plan_name+"ingestion"
gap_schema_curation = v_plan_name+"gap_curation"
# Get current user account name

# COMMAND ----------

# DBTITLE 1,Import Gap Suspecting helper functions

from src.spark.helpers.logger_util import get_logger
from src.spark.gp_suspecting.gap_suspecting_helper import *
# Initialize logger
logger = get_logger()

from pyspark.sql.functions import current_timestamp, lit
logger.info(f"Environment: {env}")
logger.info(f"Catalog: {catalog}")
logger.info(f"Schema: {schema}")
logger.info(f"Gap Schema: {gap_schema_curation}")
logger.info(f"method_id: {method_id}")
logger.info(f"risk_year: {risk_year}")
logger.info(f"no_of_year: {no_of_year}")
logger.info(f"source_month: {source_month}")
logger.info(f"source_year: {source_year}")
logger.info(f"source_load_year_month: {source_load_year_month}")
logger.info(f"threshold: {threshold}")
# Get Databricks current user
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method Exclusion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Multi-Step Gap Suspecting with Exclusions - Scenario 1 & 2
# MAGIC Gap Suspecting Logic: Multi-Step Exclusion-Based Gap Identification
# MAGIC Overview
# MAGIC This process identifies potential healthcare gaps by analyzing claims data against CMS method metadata with exclusion rules. The logic handles two distinct scenarios based on the presence of claim code modifiers, implementing a multi-step validation approach to determine valid gap records.
# MAGIC
# MAGIC Business Context
# MAGIC Gap suspecting identifies members with specific procedures or diagnoses that may indicate a healthcare condition, but applies exclusion rules to avoid false positives. The exclusion logic checks for:
# MAGIC
# MAGIC Modifier codes in the same claim (Scenario 1 only)
# MAGIC Exclusion diagnosis codes within a 180-day lookback period
# MAGIC Only claims that pass all validation steps without meeting exclusion criteria are flagged as gaps.
# MAGIC
# MAGIC Scenario 1: Claims with Modifier Code Requirements
# MAGIC Applies When: CLAIM_CD_MODIFIER_TYPE != 'NA'
# MAGIC
# MAGIC Example Configuration:
# MAGIC
# MAGIC METHOD_ID: 10, CC_ID: -2
# MAGIC Trigger Code: CPT-HCPCS = '61517'
# MAGIC Required Modifier Group: 'CHEMO-DRUG' (CPT-HCPCS GROUP)
# MAGIC Exclusion Groups: 'NON_CMS_CANCER', 'CHEMOTHERAPY' (DIAGNOSIS_GROUP)
# MAGIC Processing Steps:
# MAGIC
# MAGIC S1 - Initial Claim Identification:
# MAGIC
# MAGIC Identify claims containing the CPT-HCPCS code specified in CLAIM_CD from ref_method_metadata
# MAGIC Filter: CLAIM_CD_TYPE = 'CPT-HCPCS' and code matches medical claims
# MAGIC S2 - Same-Claim Modifier Validation:
# MAGIC
# MAGIC Expand CLAIM_CD_MODIFIER group codes using ref_method_metadata_codegroups
# MAGIC Check if any modifier code from the group exists within the same claim
# MAGIC This validates that the procedure was performed with the required context/methodology
# MAGIC Only claims with matching modifiers proceed to S3
# MAGIC S3 - 180-Day Exclusion Check:
# MAGIC
# MAGIC Expand exclusion diagnosis groups (EXCLUSION_1, EXCLUSION_2) using ref_method_metadata_codegroups
# MAGIC Search member's claims history for 180 days prior to and including the claim date
# MAGIC Check if any exclusion diagnosis codes are present
# MAGIC Records with matching exclusion codes are filtered out
# MAGIC Gap Creation Logic:
# MAGIC
# MAGIC S2 Claims - S3 Exclusions = Valid Gaps
# MAGIC Only create gaps for claims that have required modifiers (S2) but lack exclusion diagnoses (S3)
# MAGIC Scenario 2: Claims without Modifier Requirements
# MAGIC Applies When: CLAIM_CD_MODIFIER_TYPE = 'NA'
# MAGIC
# MAGIC Example Configuration:
# MAGIC
# MAGIC METHOD_ID: 10, CC_ID: -2
# MAGIC Trigger Code: CPT-HCPCS = '58346'
# MAGIC Modifier Requirement: None (NA)
# MAGIC Exclusion Groups: 'NON_CMS_CANCER', 'RADIATION' (DIAGNOSIS_GROUP)
# MAGIC Processing Steps:
# MAGIC
# MAGIC S1 - Initial Claim Identification:
# MAGIC
# MAGIC Identify claims containing the CPT-HCPCS code specified in CLAIM_CD from ref_method_metadata
# MAGIC Filter: CLAIM_CD_TYPE = 'CPT-HCPCS' and code matches medical claims
# MAGIC No modifier validation required
# MAGIC S2 - 180-Day Exclusion Check:
# MAGIC
# MAGIC Expand exclusion diagnosis groups (EXCLUSION_1, EXCLUSION_2) using ref_method_metadata_codegroups
# MAGIC Search member's claims history for 180 days prior to and including the claim date
# MAGIC Check if any exclusion diagnosis codes are present
# MAGIC Records with matching exclusion codes are filtered out
# MAGIC Gap Creation Logic:
# MAGIC
# MAGIC S1 Claims - S2 Exclusions = Valid Gaps
# MAGIC Create gaps for all matching claims (S1) that lack exclusion diagnoses (S2)
# MAGIC Key Reference Tables
# MAGIC ref_method_metadata
# MAGIC
# MAGIC Defines gap identification rules with trigger codes and exclusion criteria
# MAGIC Filters: PROGRAM = 'CMS', METHOD_ID IN (1, 2, 10), EXCLUSION_TYPE_1 != 'NA'
# MAGIC ref_method_metadata_codegroups
# MAGIC
# MAGIC Maps code group names to individual codes
# MAGIC Used for expanding both modifier groups and exclusion diagnosis groups
# MAGIC medical_claims_all
# MAGIC
# MAGIC Combined facility and professional claims with qualification indicators
# MAGIC Source for all claim matching and historical lookback
# MAGIC Technical Implementation Notes
# MAGIC 180-Day Lookback Window:
# MAGIC
# MAGIC Calculated using DATEDIFF(current_claim_date, historical_claim_date)
# MAGIC Range: 0 to 180 days (inclusive)
# MAGIC Applied at member level (MEMBER_BID)
# MAGIC Same-Claim Validation (Scenario 1):
# MAGIC
# MAGIC Joins on CLAIM_BID and CLM_TP_CD to ensure same claim context
# MAGIC Checks for modifier codes within the same claim line items
# MAGIC Gap Status Indicators:
# MAGIC
# MAGIC GAP_STATUS: "Gap"
# MAGIC Exclustion_Status: "Exclusion"
# MAGIC GAP_REASON: Scenario-specific description
# MAGIC Final Output:
# MAGIC
# MAGIC Union of Scenario 1 and Scenario 2 results
# MAGIC Distinct gaps by: CLAIM_BID, MEMBER_BID, METHOD_ID, CC_ID

# COMMAND ----------

# DBTITLE 1,Method 1,2 10 and 4 logic
from src.spark.gp_suspecting.gap_suspecting_helper import call_gp_suspecting_main
method_list = method_id.split(",")
save_to_table = True #enable all table to be saved when True
threshold_percent = float(threshold)
risk_member_hcc_summary_columns = conf_file_read_meta["RISK_MEMBER_HCC_SUMMARY_COLUMNS"]
call_gp_suspecting_main(spark, env, catalog, schema, schema_curation, schema_ingestion,
                    gap_schema_curation, ma_reference, method_list, risk_year, no_of_year,
                    plan_name, current_user, source_load_year_month, logger, save_to_table,threshold_percent, risk_member_hcc_summary_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ##---------------------------End of Gap Suspecting---------------------
