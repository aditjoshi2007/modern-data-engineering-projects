"""
Airflow DAG: Full Load Daily Refresh (AWS DMS → S3 → Redshift)

This DAG orchestrates a full reload of selected tables from AWS DMS
into Amazon Redshift using a manifest-driven COPY strategy.

Key responsibilities:
- Trigger AWS DMS reload tasks
- Wait for DMS task completion
- Execute Redshift staging + raw-current load logic
- Send success notifications

This DAG is designed to be:
✅ Idempotent
✅ Restart-safe
✅ Environment-aware (dev/tst/mdl/prd)
"""

import json
from datetime import timedelta
from typing import TYPE_CHECKING

import airflow
import pendulum

from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor
from airflow.utils.task_group import TaskGroup

# Core ingestion logic is intentionally isolated
from full_load_cdc_staging_serial import main

if TYPE_CHECKING:
    pass


# -------------------------------------------------------------------
# DAG Configuration (Environment-Aware)
# -------------------------------------------------------------------

local_tz = pendulum.timezone("America/Chicago")

curr_env = Variable.get("environment")
email_content = Variable.get("success_email_html_template")
email_subject = Variable.get("success_email_subject")

# DMS task ARNs are stored as a JSON list in Airflow Variables
dms_task_arn = json.loads(Variable.get("dms_reload_task_arn"))

dag_name = "full_load_daily_refresh"

alert_email_list = ["ops@email.com"]

# Account resolution by environment
account_no = json.loads(Variable.get("account_no"))
acct = account_no[curr_env]

# Environment-specific overrides
if curr_env in ["tst", "mdl"]:
    alert_email_list = ["ops@email.com"]
elif curr_env == "prd":
    alert_email_list = ["ops@email.com"]


DEFAULT_ARGS = {
    "owner": "owner_name",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "email": alert_email_list,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------

@dag(
    dag_name,
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="30 05 * * *",
    max_active_runs=1,
    tags=["dms", "redshift"],
)
def full_load_daily_refresh():
    """
    Orchestrates:
    1. DMS full reload
    2. Redshift staging + raw-current load
    3. Success notification
    """

    def call_main(**kwargs):
        """
        Wrapper around the core ingestion script.
        This allows the script to be reused outside Airflow.
        """
        config_file = kwargs["config"]
        env = kwargs["env"]
        manifest_file = kwargs["manifest"]

        main(config_file, env, manifest_file)

    # -------------------------------------------------------------------
    # Task Groups
    # -------------------------------------------------------------------

    @task_group
    def trigger_dms_task(task_arn: list):
        """
        Starts an AWS DMS replication task in reload-target mode.
        """
        return DmsStartTaskOperator(
            task_id="reload_dms_task",
            replication_task_arn=task_arn,
            start_replication_task_type="reload-target",
        )

    @task_group
    def wait_for_dms_task_completion(task_arn: list):
        """
        Blocks downstream execution until the DMS task completes.
        """
        return DmsTaskCompletedSensor(
            task_id="wait_for_completion",
            replication_task_arn=task_arn,
        )

    # -------------------------------------------------------------------
    # Core Tasks
    # -------------------------------------------------------------------

    trigger_full_load_cdc = PythonOperator(
        task_id="trigger_full_load_cdc",
        python_callable=call_main,
        op_kwargs={
            "config": "<s3_bucket>@<s3_prefix_json_config>",
            "env": curr_env,
            "manifest": "<s3_bucket>@<s3_prefix_manifest_file>/",
        },
        provide_context=True,
    )

    send_email = EmailOperator(
        mime_charset="utf-8",
        task_id="send_email",
        to=alert_email_list,
        subject=email_subject,
        html_content=email_content,
    )

    # -------------------------------------------------------------------
    # Dynamic Task Mapping
    # -------------------------------------------------------------------

    trigger_dms_task_obj = trigger_dms_task.partial().expand(
        task_arn=dms_task_arn
    )

    wait_for_dms_task_completion_obj = (
        wait_for_dms_task_completion.partial().expand(
            task_arn=dms_task_arn
        )
    )

    # Execution order
    chain(
        trigger_dms_task_obj,
        wait_for_dms_task_completion_obj,
        trigger_full_load_cdc,
        send_email,
    )


dag = full_load_daily_refresh()