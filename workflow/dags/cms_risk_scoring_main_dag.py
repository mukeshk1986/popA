from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
 
# -----------------------------------------------------------------------------
# Global functions 
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Global variables
# -----------------------------------------------------------------------------
global_version_dag_name_list = []
global_version_list = []
global_airflow_parameter = {}
# Predefined HCC versions - STATIC list for task creation
HCC_MODEL_VERSIONS = ["v24", "v28", "v30"]  # Add more versions as needed
 
# -----------------------------------------------------------------------------
# Helper function to create TriggerDagRunOperator for model DAGs.
# -----------------------------------------------------------------------------
def create_model_trigger(task_id, trigger_dag_id, param_key, param_fields, static_overrides=None):
    """
    Dynamically creates a TriggerDagRunOperator for model DAGs.
    Args:
        task_id (str): Unique task ID.
        trigger_dag_id (str): The DAG ID to trigger.
        param_key (str): The key in dag_run.conf containing parameters.
        param_fields (list): List of fields to pull from dag_run.conf.
        static_overrides (dict): Dict of fixed values (e.g., {"version": "v24"}).
    """
    conf_lines = []
    
    # Pull from dag_run.conf
    for field in param_fields:
        conf_lines.append(f'"{field}": "{{{{ dag_run.conf[\'{param_key}\'][\'{field}\'] }}}}"')
    # Apply static overrides (like forcing version = v24)
    if static_overrides:
        for key, val in static_overrides.items():
            conf_lines.append(f'"{key}": "{val}"')
    conf_str = "{\n    " + ",\n    ".join(conf_lines) + "\n}"
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        conf=conf_str,
        wait_for_completion=True,
        reset_dag_run=True
    )
 
def dynamic_hcc_dag_name_generation(airflow_parameter, model_name):
    global global_version_dag_name_list, global_version_list, global_airflow_parameter
 
    version_value = airflow_parameter['version']
 
    # Support both string "v24,v28" and list ["v24", "v28"]
    if isinstance(version_value, str):
        version_list = version_value.split(",")
    elif isinstance(version_value, list):
        version_list = [str(v) for v in version_value]
    else:
        raise TypeError(f"Unsupported type for version: {type(version_value)}")
 
    global_airflow_parameter = airflow_parameter
    global_version_list = version_list
    global_version_dag_name_list = []
 
    version_dag_name_list = []
    for v in version_list:
        version_dag_name = f"{model_name}_{v}"
        version_dag_name_list.append(version_dag_name)
        global_version_dag_name_list.append({'task_id': version_dag_name, 'version': v})
 
    return version_dag_name_list
# -----------------------------------------------------------------------------
# Function to determine which model branch to execute
# -----------------------------------------------------------------------------
def choose_model_branch(**context) -> List[str]:
    """
    Determine which model branches to execute based on DAG configuration.
    
    This function returns task IDs for STATICALLY DEFINED tasks, not dynamic ones.
    """
    try:
        dag_run = context.get('dag_run')
        if not dag_run:
            raise DAGConfigurationError("DAG run context is missing")
        
        conf = dag_run.conf or {}
        branches = []
        
        # Check for RxHCC parameters
        if conf.get("rx_params"):
            branches.append("trigger_cms_rxhcc_risk_scoring")
            logging.info("Added RxHCC model to execution branches")
        
        # Check for HCC parameters - STATIC task selection
        if conf.get("hcc_params"):
            logging.info("Processing HCC parameters...")
            hcc_params = conf.get("hcc_params")
            version_string = hcc_params.get('version', '')
            
            if version_string:
                # Parse requested versions
                requested_versions = [v.strip() for v in version_string.split(',') if v.strip()]
                
                # Add only the STATICALLY DEFINED tasks that match requested versions
                for version in requested_versions:
                    if version in HCC_MODEL_VERSIONS:
                        task_id = f"trigger_cms_hcc_risk_scoring_{version}"
                        branches.append(task_id)
                        logging.info(f"Added HCC model {version} to execution branches")
                    else:
                        logging.warning(f"Requested HCC version {version} not supported. Available: {HCC_MODEL_VERSIONS}")
            else:
                logging.warning("HCC parameters provided but no version specified")
        
        # Check for ESRD parameters
        if conf.get("esrd_params"):
            branches.append("trigger_cms_esrd_risk_scoring")
            logging.info("Added ESRD model to execution branches")
        
        # Default branch if no models are selected
        if not branches:
            branches.append("no_model_to_trigger")
            logging.info("No models selected, using default branch")
        
        logging.info(f"Final execution branches: {branches}")
        return branches
        
    except Exception as e:
        logging.error(f"Error in choose_model_branch: {str(e)}")
        raise DAGConfigurationError(f"Branch selection failed: {str(e)}")
 
 
# -----------------------------------------------------------------------------
# Default arguments for the DAG
# -----------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}
 
# -----------------------------------------------------------------------------
# DAG Definition
# -----------------------------------------------------------------------------
 
with DAG(
    dag_id='cms_risk_scoring_main_dag',
    default_args=default_args,
    description='Main orchestration DAG for CMS risk scoring pipeline',
    schedule_interval=None,  # Triggered manually or by external systems
    catchup=False,
    max_active_runs=1,
    params={"my_param": "default_values"},
    tags=['parent', 'selective']
) as dag:
 
    # -------------------------------------------------------------------------
    # d+ Curation
    # -------------------------------------------------------------------------
    trigger_data_ingestion = TriggerDagRunOperator(
        task_id='trigger_data_ingestion',
        trigger_dag_id='data_ingestion',
        conf="""
        {
            "raw_schema": "{{ dag_run.conf['ingestion_params']['raw_schema'] }}",
            "external_file_dir": "{{ dag_run.conf['ingestion_params']['external_file_dir'] }}",
            "target_schema": "{{ dag_run.conf['ingestion_params']['target_schema'] }}",
            "plan_name": "{{ dag_run.conf['ingestion_params']['plan_name'] }}",
            "tables": "{{ dag_run.conf['ingestion_params']['tables'] }}"
        }
        """,
        wait_for_completion=True,
        reset_dag_run=True
    )
    trigger_data_curation = TriggerDagRunOperator(
        task_id='trigger_data_curation',
        trigger_dag_id='data_curation',
        conf="""
        {
            "plan_name": "{{ dag_run.conf['curation_params']['plan_name'] }}",
            "month": "{{ dag_run.conf['curation_params']['month'] }}",
            "risk_year": "{{ dag_run.conf['curation_params']['risk_year'] }}"
        }
        """,
        wait_for_completion=True,
        reset_dag_run=True
    )
    branch_models = BranchPythonOperator(
        task_id='branch_models',
        python_callable=choose_model_branch,
        provide_context=True
    )
    print("-----------branch_models----------")
    print(branch_models)
    
    
 
    # -------------------------------------------------------------------------
    # Define fields common across models
    # -------------------------------------------------------------------------
    model_fields = [
        "model", "plan_id", "contract", "time_period",
        "plan_name", "run_mode", "incl_pseudo_claim",
        "month", "risk_year"
    ]
 
    
 
    # -------------------------------------------------------------------------
    # Model Triggers (DRY via helper)
    # -------------------------------------------------------------------------
    # RXHCC
    trigger_cms_rxhcc_risk_scoring = create_model_trigger(
        "trigger_cms_rxhcc_risk_scoring", "cms_rxhcc_risk_scoring", "rx_params", model_fields + ["version"]
    )
    # ESRD
    trigger_cms_esrd_risk_scoring = create_model_trigger(
        "trigger_cms_esrd_risk_scoring", "cms_esrd_risk_scoring", "esrd_params", model_fields + ["version"]
    )
    
    # -------------------------------------------------------------------------
    # Dynamicly Create triggers  -- this need to change on param key : like hcc,
    # -------------------------------------------------------------------------
 
 
    # HCC Model Triggers - STATIC creation for all supported versions
    hcc_triggers = {}
    for version in HCC_MODEL_VERSIONS:
        task_id = f"trigger_cms_hcc_risk_scoring_{version}"
        hcc_triggers[version] = create_model_trigger(
        task_id=task_id,
        trigger_dag_id='cms_hcc_risk_scoring',
        param_key='hcc_params',
        param_fields=model_fields + ['version'],
        static_overrides={'version': version}  # Override version for each task
    )

    
    # Dummy branch if nothing is selected
    no_model_to_trigger = DummyOperator(task_id='no_model_to_trigger')
 
    # Task Dependencies - STATIC definition
    # Sequential execution: Ingestion -> Curation -> Model Branching
    trigger_data_ingestion >> trigger_data_curation >> branch_models
    
    # Branch to individual models - ALL STATICALLY DEFINED
    branch_models >> [
        trigger_cms_rxhcc_risk_scoring,
        trigger_cms_esrd_risk_scoring,
        no_model_to_trigger
    ]
    
    # Branch to HCC models - STATIC dependencies
    for version, hcc_trigger in hcc_triggers.items():
        branch_models >> hcc_trigger
