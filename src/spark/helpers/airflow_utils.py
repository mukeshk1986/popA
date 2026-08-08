"""Airflow Utilities for Databricks Orchestration."""

import os
import json
import yaml
import logging
import smtplib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from boto3 import Session
from botocore.session import Session as BotoCoreSession
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURATION LOADING FUNCTIONS
# =========================================================================

def load_values_config():
    """Load values configuration from YAML file."""
    config_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(config_dir, 'config', 'environments', 'values.yaml')

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"values.yaml file not found in {config_dir}")

    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def load_param_values_config():
    """Load parameter values configuration from YAML file."""
    config_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(config_dir, 'config', 'environments', 'param_values.yaml')

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"param_values.yaml file not found in {config_dir}")

    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def load_cluster_config_file(cluster_file_name: str, **kwargs) -> Dict:
    """
    Load cluster configuration from file.

    Args:
        cluster_file_name: Name of the cluster configuration file.
        **kwargs: Additional keyword arguments including dag_run.

    Returns:
        Dictionary containing cluster configuration.
    """
    try:
        values_conf = load_values_config()
        trigger_config = kwargs.get('dag_run').conf or {}
        config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'environments')
        env = values_conf.get('env', 'dev').lower()

        cluster_file_path = os.path.join(config_dir, cluster_file_name)

        logger.info(f"Loading cluster config from {cluster_file_path}")

        with open(cluster_file_path, 'r') as f:
            config = yaml.safe_load(f)

        if 'custom_tags' not in config or not config['custom_tags']:
            config['custom_tags'] = {}

            if 'aws_attributes' in config:
                config['custom_tags']['service_profile_arn'] = values_conf.get('arn', '')
                config['custom_tags']['env'] = values_conf.get('env', 'dev')
                config['custom_tags']['team'] = config['custom_tags'].get('team', 'default')
                config['custom_tags']['billingcode'] = config['custom_tags'].get('billingcode', 'env')

        logger.info(f"Successfully loaded cluster configuration: {config}")

    except FileNotFoundError as e:
        logger.error(f"Failed to load cluster configuration file: {str(e)}")
        raise

    return config


# =========================================================================
# EMAIL AND NOTIFICATION FUNCTIONS
# =========================================================================

def send_email(dag_id: str, task, message: str, emails=None, smtp_server=None, sender_email=None):
    """
    Send email alert with task and DAG information.

    Args:
        dag_id: DAG ID for the alert.
        task: Task instance key string.
        message: Email message body.
        emails: List of recipient emails.
        smtp_server: SMTP server configuration.
        sender_email: Sender email address.
    """
    try:
        if smtp_server is None:
            smtp_server = Variable.get('smtp_server', 'default_smtp_server')

        emails = emails or Variable.get('email_alerts', {}).get('default_recipient_email', [])
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(emails)
        msg['Subject'] = f"Airflow Alert: DAG {dag_id} - Task {task}"

        msg.attach(MIMEText(message, 'plain'))

        with smtplib.SMTP(smtp_server) as smtp:
            smtp.starttls()
            smtp.sendmail(sender_email, emails, msg.as_string())

    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")


def failure_callback(context):
    """
    Handle task failure with DAG ID and task details.

    Args:
        context: Airflow task context.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = f"""
    Airflow DAG Failure Alert

    DAG ID: {dag_id}
    Task ID: {task_id}
    Execution Date: {execution_date}
    Log URL: {log_url}
    """

    send_email(dag_id, task_id, message)


def success_callback(context):
    """
    Handle successful task execution.

    Args:
        context: Airflow task context.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    message = f"""
    Airflow DAG Success Alert

    DAG ID: {dag_id}
    Task ID: {task_id}
    Execution successfully completed.
    """

    send_email(dag_id, task_id, message)


# =========================================================================
# CLUSTER AND NOTEBOOK CONFIGURATION FUNCTIONS
# =========================================================================

def get_cluster_config(notebook: str, **kwargs) -> Dict:
    """
    Get cluster configuration from notebook configuration.

    Args:
        notebook: Notebook name.
        **kwargs: Additional arguments including dag_run.

    Returns:
        Cluster configuration dictionary.
    """
    try:
        trigger_config = kwargs.get('dag_run').conf or {}
        cluster_config = trigger_config.get('cluster_config', {})

        if 'validate' in notebook:
            validate_stage_name = kwargs.get('validate_stage_name')

        for cluster in cluster_config:
            if cluster.get('notebook') == notebook:
                cluster_config_yaml = cluster.get('cluster_config_file')

        logger.info(f"Using cluster configuration file: {cluster_config_yaml} for notebook: {notebook}")

        return load_cluster_config_file(cluster_config_yaml, **kwargs)

    except Exception as e:
        logger.error(f"Failed to load cluster configuration: {str(e)}")
        raise


def notebook_dict(**kwargs) -> Dict:
    """
    Create notebook dictionary with configuration.

    Args:
        **kwargs: Notebook parameters including notebook_task_name.

    Returns:
        Notebook configuration dictionary.
    """
    try:
        values_conf = load_param_values_config()
        notebook_task_name = kwargs.get('notebook_task_name')

        if not notebook_task_name:
            logger.error("Notebook task name is missing.")
            raise ValueError("Notebook task name is required.")

        base_params = kwargs.get('base_params', {})
        notebook_path = f"/Workspace/Notebooks/{notebook_task_name}"

        notebook_dict = {
            "notebook_path": notebook_path,
            "base_parameters": base_params
        }

        logger.info(f"Generated notebook dictionary: {notebook_dict}")
        return notebook_dict

    except Exception as e:
        logger.error(f"Error creating notebook dictionary: {str(e)}")
        raise


def run_notebook(notebook_task_name: str, **kwargs):
    """
    Execute Databricks notebook task.

    Args:
        notebook_task_name: Name of the notebook task to execute.
        **kwargs: Additional task parameters.
    """
    try:
        logger.info(f"Running notebook: {notebook_task_name}")

        notebook_task_cluster_spec = get_cluster_config(notebook_task_name, **kwargs)
        notebook_task_dict = notebook_dict(notebook_task_name=notebook_task_name, **kwargs)

        logger.info(f"Cluster spec: {json.dumps(notebook_task_cluster_spec, indent=4)}")
        logger.info(f"Notebook dict: {json.dumps(notebook_task_dict, indent=4)}")

        values_conf = load_values_config()
        databricks_conn_id = values_conf.get('databricks_conn_id', 'databricks_default')

        notebook_task.execute(kwargs)

    except Exception as e:
        logger.error(f"Failed to run notebook: {str(e)}")
        raise


# =========================================================================
# ACL AND PERMISSION FUNCTIONS
# =========================================================================

def get_acls(**kwargs) -> Dict:
    """
    Retrieve access control list for notebook runs.

    Args:
        **kwargs: Additional arguments.

    Returns:
        Access control list dictionary.
    """
    try:
        access_control_list = []
        values_conf = kwargs.get('values_conf') or load_values_config()
        groups = values_conf.get('airflow', {}).get('databricks_users_groups', [])

        for group in groups:
            permission = {
                'group_name': group,
                'permission_level': 'CAN_VIEW'
            }
            access_control_list.append(permission)

        logger.info(f"Access control list: {access_control_list}")
        return access_control_list

    except Exception as e:
        logger.error(f"Error retrieving ACLs: {str(e)}")
        raise


def add_acls(run_page_url: str, **kwargs):
    """
    Add permissions to notebook run.

    Args:
        run_page_url: URL of the notebook run page.
        **kwargs: Additional arguments including permissions.
    """
    try:
        if not run_page_url:
            logger.warning("run_page_url is None. Skipping permission setting.")
            return

        job_id = run_page_url.split("/")[-3]
        logger.info(f"Adding permissions to job_id: {job_id}")

        values_conf = kwargs.get('values_conf') or load_values_config()
        databricks_instance = values_conf.get('airflow', {}).get('databricks_instance')

        permissions_api = f"https://{databricks_instance}/api/2.0/permissions/jobs/{job_id}"
        acls = get_acls(**kwargs)

        logger.info(f"Adding ACLs to {permissions_api}")

    except Exception as e:
        logger.error(f"Exception in applying acls: {str(e)}")
        raise


def add_permissions(notebook_task_name: str, **kwargs):
    """
    Set permissions for Databricks notebook job.

    Args:
        notebook_task_name: Name of the notebook task.
        **kwargs: Additional arguments including task instance.
    """
    logger.info(f"Setting permissions for notebook task: {notebook_task_name}")

    ti = kwargs.get('ti')
    run_page_url = None

    if ti:
        run_page_url = ti.xcom_pull(key='run_page_url', task_id=notebook_task_name)

        if run_page_url:
            logger.info(f"Adding permissions for run_page_url: {run_page_url}")
            add_acls(run_page_url, **kwargs)
        else:
            logger.warning(f"run_page_url not found in XCom for task: {notebook_task_name}")
    else:
        logger.warning("Task instance not found in context")


def get_db_token(aws_region: str, secret_name: str) -> str:
    """
    Retrieve Databricks API token from AWS Secrets Manager.

    Args:
        aws_region: AWS region name.
        secret_name: Name of the secret in AWS Secrets Manager.

    Returns:
        Databricks API token string.
    """
    try:
        session = Session()
        secretsmanager = session.client(service_name='secretsmanager', region_name=aws_region)

        secret = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(secret.get('SecretString'))
        db_token = secret_dict.get('databricks_apikey', {}).get('value')

        return f"Bearer {db_token}"

    except Exception as e:
        logger.error(f"Failed to retrieve Databricks token: {str(e)}")
        raise


# =========================================================================
# UTILITY FUNCTIONS FOR PLAN NAMES AND PATHS
# =========================================================================

def get_schema_plan_name(plan_name: str, incl_supplemental_mmr: str = "") -> str:
    """
    Get schema prefix based on plan name.

    Non-supplemental runs use [plan_name]_gap_curation (unchanged).
    Supplemental runs use [plan_name]_gap_curation_supp.

    Args:
        plan_name: The plan name (e.g., "anthem", "non_anthem").
        incl_supplemental_mmr: Run type flag, "T" for supplemental.

    Returns:
        The resolved gap curation schema name.
    """
    v_plan_name = get_plan_name(plan_name)
    suffix = "_gap_curation_supp" if incl_supplemental_mmr == "T" else "_gap_curation"
    return v_plan_name + suffix


def get_path_plan_name(plan_name: str) -> str:
    """
    Get path prefix based on plan name.

    If the plan name is "non_anthem", returns an empty string.
    Otherwise returns the path segment based on plan name.
    """
    v_plan_name = get_plan_name(plan_name)
    return "" if plan_name == "non_anthem" else plan_name + "_"


def get_plan_name(plan_name: str) -> str:
    """
    Get plan name based on plan name.

    Returns the schema prefix based on the plan name.
    If the plan name is "non_anthem", returns an empty string.
    Otherwise returns the plan name as a string.
    """
    return "" if plan_name == "non_anthem" else plan_name


def resolve_plan_ids_to_descriptions(
    spark,
    member_level_table: str,
    bhi_home_plan_ids: str,
) -> str:
    """
    Resolve plan IDs to HOME_PLAN_DESCRIPTION values via the MEMBER_LEVEL table.

    Parameters
    ----------
    spark:
        Active Spark session.
    member_level_table:
        Fully qualified path to the MEMBER_LEVEL table.
    bhi_home_plan_ids:
        Comma-separated plan IDs (e.g., "370.043", "370.044").

    Returns
    -------
    str
        SQL-formatted quoted string for IN clauses
        (e.g., "'Humana Plan','Anthem Plan'"), or "" if no scoping.
    """
    if not bhi_home_plan_ids or not bhi_home_plan_ids.strip():
        return ""

    plan_ids = [p.strip() for p in bhi_home_plan_ids.split(",") if p.strip()]

    if not plan_ids:
        return ""

    ida_csv = ", ".join([f"'{pid}'" for pid in plan_ids])
    sql = f"""
    SELECT DISTINCT MA_HOME_PLAN_DESCRIPTION
    FROM {member_level_table}
    WHERE HOME_PLAN_ID IN ({ida_csv})
    AND HOME_PLAN_ID IS NOT NULL
    """

    try:
        rows = spark.sql(sql).collect()
        descriptions = [row["MA_HOME_PLAN_DESCRIPTION"] for row in rows]

        if not descriptions:
            logger.warning(
                f"No MA_HOME_PLAN_DESCRIPTION found for plan IDs {bhi_home_plan_ids}"
            )
            return ""

        return ", ".join([f"'{desc}'" for desc in descriptions])

    except Exception as e:
        logger.error(f"Error resolving plan IDs: {str(e)}")
        raise


# =========================================================================
# DELTA TABLE OPERATIONS
# =========================================================================

def merge_to_table(
    spark,
    source_df,
    full_table_path: str,
    merge_condition: str,
    source_scope_condition: str = None,
    exclude_update_columns: List = None,
) -> None:
    """
    Perform an atomic MERGE (upsert + scoped delete) on a Delta table.

    Behavior:
    - WHEN MATCHED - UPDATE all columns except 'exclude_update_columns'.
    - WHEN NOT MATCHED - INSERT all columns.
    - WHEN NOT MATCHED BY SOURCE (+ 'source_scope_condition') - DELETE stale rows.

    If the table does not yet exist - fails back to df.write.saveAsTable().

    Args:
        spark: Active SparkSession.
        source_df: DataFrame with new/updated records.
        full_table_path: Fully-qualified path [catalog.schema.table].
        merge_condition: SQL expression for the join (e.g., "s.id = t.id").
        source_scope_condition: SQL condition for DELETE scope.
        exclude_update_columns: Column names to preserve on UPDATE.
    """
    try:
        from pyspark.sql import functions as F

        if exclude_update_columns is None:
            exclude_update_columns = ["CREATED_DATE", "CREATED_BY"]

        source_cols_lower = {c.lower() for c in source_df.columns}
        audit_defaults = {
            "CREATED_DATE": F.current_timestamp(),
            "CREATED_BY": F.current_user(),
            "UPDATED_DATE": F.current_timestamp(),
            "UPDATED_BY": F.current_user()
        }

        for col_name, f_expr in audit_defaults.items():
            if col_name.lower() not in source_cols_lower:
                source_df = source_df.withColumn(col_name, f_expr)

        source_df.createOrReplaceTempView("source_data")

        merge_sql = f"""
        MERGE INTO {full_table_path} t
        USING source_data s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        if source_scope_condition:
            merge_sql += f"WHEN NOT MATCHED BY SOURCE AND {source_scope_condition} THEN DELETE"

        spark.sql(merge_sql)
        logger.info(f"Successfully merged data to {full_table_path}")

    except Exception as e:
        logger.error(f"Error during merge operation: {str(e)}")
        raise
