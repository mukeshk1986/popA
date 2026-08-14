# Databricks notebook source
import sys
sys.dont_write_bytecode = True

repo_root = "/Workspace/Repos/DEV/popA"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.databricks_util import get_plan_name, get_path_plan_name
from src.spark.helpers.generic_util import ingestion_folder_check

logger = get_logger()

dbutils.widgets.dropdown("env", "DEV", ["DEV", "QA", "STG", "PROD"])
dbutils.widgets.text("plan_name", "")
dbutils.widgets.text("plan_onboarding_reference_schema", "")
dbutils.widgets.text("plan_onboarding_schema_list", "")

# COMMAND ----------

plan_name = dbutils.widgets.get("plan_name").lower()
env = dbutils.widgets.get("env").lower()
env_bucket = "pop-"+env
catalog = "pop_"+env
schema_list_raw = dbutils.widgets.get("plan_onboarding_schema_list").strip()
schema_list = [schema.strip().lower() for schema in schema_list_raw.split(",") if schema.strip()]

reference_schema_raw = dbutils.widgets.get("plan_onboarding_reference_schema").strip()
reference_schema = reference_schema_raw.upper()

# reference_schema is plan-name independent and supports canonical aliases.
REFERENCE_SCHEMA_ALIASES = {
    "MA": "ma",
    "ACA": "aca",
    "MA_DASHBOARD": "ma_dashboard"
}

if reference_schema and reference_schema not in REFERENCE_SCHEMA_ALIASES:
    raise ValueError("reference_schema must be one of: ACA, MA, MA_DASHBOARD")

# COMMON schemas: shared across all plans, NEVER prefixed with plan_name.
COMMON_REFERENCE_SCHEMAS = {
    "ma": "ma_reference",
    "aca": "aca_reference",
    "ma_dashboard": "ma_dashboard_reference"
}

reference_schema_key = REFERENCE_SCHEMA_ALIASES.get(reference_schema, "")
ref_schema = COMMON_REFERENCE_SCHEMAS.get(reference_schema_key, "")

# PLAN-SPECIFIC schemas require a plan_name (non-anthec uses no prefix and is
# resolved later); COMMON reference schemas do not.
if schema_list and not plan_name:
    raise ValueError("plan_name widget cannot be empty when schema_list is provided.")

if not schema_list and not reference_schema:
    raise ValueError("Either schema_list or reference_schema must be provided.")

# Ensure the common reference schema selection is always part of schema_list processing.
if ref_schema and ref_schema not in schema_list:
    schema_list.append(reference_schema_key)

# COMMAND ----------

# DBTITLE 1,cell 1
v_plan_name = get_plan_name(plan_name)
v_schema_plan_name = get_path_plan_name(plan_name)

# Supplemental output isolation: segregated schemas provisioned for supplemental
# risk-scoring runs (must stay in sync with get_curation_schema / get_gap_curation_schema).
schema_curation_supp = v_plan_name+"curation_supp"
gap_schema_curation_supp = v_plan_name+"gap_curation_supp"
effective_plan_name = v_plan_name if plan_name else "non_anthem"
v_schema_plan_name = get_path_plan_name(effective_plan_name)

# PLAN-SPECIFIC schemas: <plan_prefix>+<base>, where plan_prefix is "" for non_anthec.
# The _supp bases provision segregated supplemental risk-scoring output and must
# stay in sync with get_curation_schema / get_gap_curation_schema.
PLAN_SCHEMA_BASES = [
    "transformation", "curation", "ingestion", "monitoring", "gap_curation",
    "curation_supp", "gap_curation_supp",
    "sam_ref", "sam_stage", "sam_work", "sam_result"
]

plan_schemas = {base: v_plan_name + base for base in PLAN_SCHEMA_BASES}

schema_transformation = plan_schemas["transformation"]
schema_curation = plan_schemas["curation"]
schema_ingestion = plan_schemas["ingestion"]
schema_monitoring = plan_schemas["monitoring"]
gap_schema_curation = plan_schemas["gap_curation"]
schema_curation_supp = plan_schemas["curation_supp"]
gap_schema_curation_supp = plan_schemas["gap_curation_supp"]
sam_ref_schema = plan_schemas["sam_ref"]
sam_stage_schema = plan_schemas["sam_stage"]
sam_work_schema = plan_schemas["sam_work"]
sam_result_schema = plan_schemas["sam_result"]

# COMMAND ----------

# DBTITLE 1,Running the run_ddl function
import importlib
import sys
import os

# Reload generic_util to pick up fixes
if 'src.spark.helpers.generic_util' in sys.modules:
    importlib.reload(sys.modules['src.spark.helpers.generic_util'])

from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger
from src.spark.helpers.generic_util import ingestion_folder_check, run_ddl

config = get_config_yaml("../../../config/environments/"+env+"/values.yaml")
logger = get_logger()
catalog = config["catalog"]

# Workaround: config_plan_setup has a bug where it doesn't construct full DDL file paths
# Build the paths manually and call run_ddl directly
repo_root = "/Workspace/Repos/DEV/popA"
ddl_base_path = os.path.join(repo_root, "src/sql/ddl")

ddl_groups = {
    "schema_creation": ["schema_creation"],
    "ingestion": ["ingestion_tables"],
    "monitoring": ["monitoring_tables"],
    "transformation_curation": ["transformation_curation_tables"],
    "reference": ["ma_ra_reference_tables"],
    "ma_dashboard": ["ma_dashboard_ref_tables"]
}

reference_schema_values = {"ma": "ma_reference", "aca": "aca_reference"}
ma_dashboard_schema_values = {"ma_dashboard": "ma_dashboard_reference"}
print(reference_schema_values[reference_schema_raw])

ddl_files = []
for schema_name in schema_list:
    schema_key = schema_name.strip().lower()
    if schema_key in ddl_groups:
        for ddl in ddl_groups[schema_key]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)
    elif schema_key in reference_schema_values:
        for ddl in ddl_groups["reference"]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)
    elif schema_key in ma_dashboard_schema_values:
        for ddl in ddl_groups["ma_dashboard"]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)

logger.info(f"schema_list : {schema_list}")
logger.info(f"ddl_files : {ddl_files}")

# Convert schema_list to string (run_ddl expects str, not list)
schema_list_str = ",".join(schema_list) if isinstance(schema_list, list) else schema_list

# Wrapper to fix SQL syntax issues in DDL files
def run_ddl_fixed(spark, sql_file_path, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ma_dashboard_ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list):
    import re
    with open(sql_file_path) as f:
        sql_template = f.read()
    
    sql_rendered = sql_template.replace("${catalog}", catalog).replace("${gap_schema_curation_supp}", gap_schema_curation_supp).replace("${gap_schema_curation}", gap_schema_curation).replace("${schema_curation}", schema_curation).replace("${schema_transformation}", schema_transformation).replace("${env_bucket}", env_bucket).replace("${schema_ingestion}", schema_ingestion).replace("${schema_plan_name}", v_schema_plan_name).replace("${schema_monitoring}", schema_monitoring).replace("${ma_dashboard_reference_schema}", ma_dashboard_ref_schema).replace("${schema_reference}", reference_schema_values[reference_schema_raw]).replace("${sam_work_schema}", sam_work_schema).replace("${sam_result_schema}", sam_result_schema).replace("${sam_stage_schema}", sam_stage_schema).replace("${sam_ref_schema}", sam_ref_schema).replace("${schema_curation_supp}", schema_curation_supp).replace("${schema_list}", schema_list).replace("${catalog}", catalog)
    
    # Fix VARCHAR() syntax error - replace with STRING
    sql_rendered = re.sub(r'VARCHAR\(\s*\)', 'STRING', sql_rendered)
    
    # Fix SQL syntax: swap TBLPROPERTIES and PARTITIONED BY when they're in wrong order
    def find_balanced_parens(text, start_pos):
        """Find closing paren for the opening paren at start_pos"""
        if text[start_pos] != '(':
            return -1
        depth = 0
        for i in range(start_pos, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return i
        return -1
    
    # Find and swap USING DELTA TBLPROPERTIES(...) PARTITIONED BY(...)
    # Process all occurrences in the file
    offset = 0
    while True:
        pattern = re.compile(r'USING\s+DELTA\s+TBLPROPERTIES\s*\(', re.IGNORECASE)
        match = pattern.search(sql_rendered, offset)
        
        if not match:
            break
            
        tblprops_start = match.end() - 1  # Position of '(' after TBLPROPERTIES
        tblprops_end = find_balanced_parens(sql_rendered, tblprops_start)
        
        if tblprops_end == -1:
            offset = match.end()
            continue
        
        # Extract TBLPROPERTIES clause
        tblprops_clause = sql_rendered[match.start():tblprops_end+1]
        
        # Look for PARTITIONED BY after TBLPROPERTIES (with or without space)
        remaining = sql_rendered[tblprops_end+1:]
        part_match = re.match(r'\s*(PARTITIONED\s+BY\s*\([^)]+\))', remaining, re.IGNORECASE)
        
        if part_match:
            partitioned_clause = part_match.group(1)
            # Reconstruct with swapped order
            before = sql_rendered[:match.start()]
            after = sql_rendered[tblprops_end+1+part_match.end():]
            sql_rendered = before + "USING DELTA " + partitioned_clause + " " + tblprops_clause[len("USING DELTA "):] + after
            offset = len(before) + len("USING DELTA ") + len(partitioned_clause) + 1 + len(tblprops_clause) - len("USING DELTA ")
        else:
            offset = match.end()
    
    statements = [stat.strip() for stat in re.split(r";\s*\n", sql_rendered) if stat.strip()]
    
    print(f"DEBUG: Total {len(statements)} statements to execute")
    for i, stat in enumerate(statements, 1):
        try:
            print(f"\nDEBUG: Executing statement {i}/{len(statements)}...")
            spark.sql(stat)
            print(f"SUCCESS: Statement {i} completed")
        except Exception as e:
            print(f"\nERROR: Statement {i} failed with error: {e}")
            print(f"\nFailing statement:\n{stat}")
            raise

for ddl_file in ddl_files:
    ddl_file_path = os.path.join(ddl_base_path, ddl_file + ".sql")
    logger.info(f"****** Execution Started : {ddl_file_path}")
    run_ddl_fixed(spark, ddl_file_path, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list_str)

# COMMAND ----------

# DBTITLE 1, Execute DDL to Create Tables
import os
import re

logger.info("Starting table creation with DDL execution...")

repo_root = "/Workspace/Repos/DEV/popA"
ddl_base_path = os.path.join(repo_root, "src/sql/ddl")

ddl_groups = {
    "schema_creation": ["schema_creation"],
    "ingestion": ["ingestion_tables"],
    "monitoring": ["monitoring_tables"],
    "transformation_curation": ["transformation_curation_tables"],
    "reference": ["ma_ra_reference_tables"],
    "ma_dashboard": ["ma_dashboard_ref_tables"]
}

reference_schema_values = {"ma": "ma_reference", "aca": "aca_reference"}
ma_dashboard_schema_values = {"ma_dashboard": "ma_dashboard_reference"}

ddl_files = []
for schema_name in schema_list:
    schema_key = schema_name.strip().lower()
    if schema_key in ddl_groups:
        for ddl in ddl_groups[schema_key]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)
    elif schema_key in reference_schema_values:
        for ddl in ddl_groups["reference"]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)
    elif schema_key in ma_dashboard_schema_values:
        for ddl in ddl_groups["ma_dashboard"]:
            if ddl not in ddl_files:
                ddl_files.append(ddl)

logger.info(f"schema_list : {schema_list}")
logger.info(f"ddl_files : {ddl_files}")

schema_list_str = ",".join(schema_list) if isinstance(schema_list, list) else schema_list

def run_ddl_fixed(spark, sql_file_path, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ma_dashboard_ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list, reference_schema_value):
    with open(sql_file_path) as f:
        sql_template = f.read()
    
    sql_rendered = sql_template.replace("${catalog}", catalog).replace("${gap_schema_curation_supp}", gap_schema_curation_supp).replace("${gap_schema_curation}", gap_schema_curation).replace("${schema_curation}", schema_curation).replace("${schema_transformation}", schema_transformation).replace("${env_bucket}", env_bucket).replace("${schema_ingestion}", schema_ingestion).replace("${schema_plan_name}", v_schema_plan_name).replace("${schema_monitoring}", schema_monitoring).replace("${ma_dashboard_reference_schema}", ma_dashboard_ref_schema).replace("${schema_reference}", reference_schema_value).replace("${sam_work_schema}", sam_work_schema).replace("${sam_result_schema}", sam_result_schema).replace("${sam_stage_schema}", sam_stage_schema).replace("${sam_ref_schema}", sam_ref_schema).replace("${schema_curation_supp}", schema_curation_supp).replace("${schema_list}", schema_list)
    
    sql_rendered = re.sub(r'VARCHAR\(\s*\)', 'STRING', sql_rendered)
    
    def find_balanced_parens(text, start_pos):
        if text[start_pos] != '(':
            return -1
        depth = 0
        for i in range(start_pos, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return i
        return -1
    
    offset = 0
    while True:
        pattern = re.compile(r'USING\s+DELTA\s+TBLPROPERTIES\s*\(', re.IGNORECASE)
        match = pattern.search(sql_rendered, offset)
        
        if not match:
            break
            
        tblprops_start = match.end() - 1
        tblprops_end = find_balanced_parens(sql_rendered, tblprops_start)
        
        if tblprops_end == -1:
            offset = match.end()
            continue
        
        tblprops_clause = sql_rendered[match.start():tblprops_end+1]
        
        remaining = sql_rendered[tblprops_end+1:]
        part_match = re.match(r'\s*(PARTITIONED\s+BY\s*\([^)]+\))', remaining, re.IGNORECASE)
        
        if part_match:
            partitioned_clause = part_match.group(1)
            before = sql_rendered[:match.start()]
            after = sql_rendered[tblprops_end+1+part_match.end():]
            sql_rendered = before + "USING DELTA " + partitioned_clause + " " + tblprops_clause[len("USING DELTA "):] + after
            offset = len(before) + len("USING DELTA ") + len(partitioned_clause) + 1 + len(tblprops_clause) - len("USING DELTA ")
        else:
            offset = match.end()
    
    statements = [stat.strip() for stat in re.split(r";\s*\n", sql_rendered) if stat.strip()]
    
    for i, stat in enumerate(statements, 1):
        try:
            spark.sql(stat)
        except Exception as e:
            print(f"\nERROR: Statement {i} failed with error: {e}")
            print(f"\nFailing statement:\n{stat}")
            raise

for ddl_file in ddl_files:
    ddl_file_path = os.path.join(ddl_base_path, ddl_file + ".sql")
    logger.info(f"****** Execution Started : {ddl_file_path}")
    run_ddl_fixed(spark, ddl_file_path, catalog, ref_schema, gap_schema_curation, schema_curation, schema_transformation, env_bucket, schema_ingestion, schema_monitoring, v_schema_plan_name, ref_schema, sam_ref_schema, sam_stage_schema, sam_work_schema, sam_result_schema, schema_curation_supp, gap_schema_curation_supp, schema_list_str, reference_schema_values.get(reference_schema_raw, ref_schema))

logger.info("✓ Table creation completed successfully!")

# COMMAND ----------

volume_name = "ingestion"
scr_files = "src_files"
archive = "archive"
volume_schema = v_plan_name+"ingestion"

if plan_name:
    # Get volume path from SQL for plan-specific ingestion volume.
    volume_row = spark.sql(f"""
    SELECT volume_name, volume_schema, volume_catalog
    FROM system.information_schema.volumes
    WHERE volume_name = '{volume_name}' and volume_catalog = '{catalog}' and volume_schema = '{volume_schema}'
    """).first()

    if not volume_row or not volume_row.storage_location:
        raise ValueError(
            f"Ingestion volume not found for catalog={catalog}, schema={volume_schema}, volume={volume_name}."
        )

    volume_path = volume_row.storage_location
    logger.info(f"Volume Path: {volume_path}")

    # Construct folder path
    src_file_path = f"{volume_path}/{scr_files}"
    archive_file_path = f"{volume_path}/{archive}"

    # Ensure both folders exist
    ingestion_folder_check(dbutils, src_file_path, logger)
    ingestion_folder_check(dbutils, archive_file_path, logger)
else:
    logger.info(
        "Skipping ingestion volume folder setup because plan_name is empty."
    )
