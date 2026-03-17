"""
# 🚀 EMR Batch Load — Redshift/Sources → Spark on EMR → Curated

## Overview
This DAG provisions an **Amazon EMR** cluster on demand, submits **Spark ingestion steps** (defined by Airflow Variables), then **monitors cluster and step status** to ensure reliable, end‑to‑end ingestion. It is designed for **repeatable batch loads**, clear **observability**, and **safe defaults** suitable for open‑source/portfolio sharing.

---

## 🗓 Schedule & Concurrency
- **Schedule:** `None` (manually triggered or triggered by upstream DAGs)
- **Max Active Runs:** `1` (prevents overlapping clusters)
- **Owner:** `owner_name`

---

## 🧩 How It Works (High‑Level)
```text
Airflow
  └── EmrCreateJobFlowOperator → creates EMR with computed Steps
        └─▶ wait_for_cluster_ready (poll describe_cluster)
              └─▶ wait_for_steps_complete (poll describe_step for each step)
                    └─▶ send_success_email
"""
import copy
import json
import time as t
from datetime import timedelta

import airflow
import boto3
import botocore
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from botocore.config import Config as boto3_config

# Set up static variables for DAG
local_tz=pendulum.timezone("America/Chicago")
curr_env=Variable.get('environment')
email_content=Variable.get('success_email_html_template')
email_subject=Variable.get('success_email_subject')
dag_name='emr_batch_load'
alert_email_list=['']

# Set up schedule interval of the DAG and email list for DAG alerts

if curr_env == 'tst' or curr_env == 'mdl':
    # schedule_interval = '* * * * *'
    alert_email_list=['']
elif curr_env == 'prd':
    alert_email_list=['']

DEFAULT_ARGS={
    'owner': 'owner_name',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': alert_email_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Get Airflow metadata variables for the DAG

job_flw_override=Variable.get('JOB_FLOW_OVERRIDES')
job_list=Variable.get('jobs_list')
jobs_list=json.loads(job_list)


def get_job_flow(jobs_list, job_flw_override):
    """
    Dynamically creates job flow

    Parameters
    ----------
    RTS_jobs_list: dict

    job_flw_override: json

    Returns
    -------
    job_flow
    """

    steps=[]

    # Convert the job_flw_override to JSON type
    job_flw=json.loads(job_flw_override)

    # Loop through all the dict items of jobs_list
    for job_name, config_file_all in jobs_list.items():
        # Retrieve the spark steps from function get_spark_steps
        spark_step=get_spark_steps(''.join(config_file_all), 'Load_Ingestion', job_name)
        # Append all job flows to empty list job_flow
        steps.append(spark_step)

    # Create a copy of job_flw_override dict, so we don't overwrite the original job_flw_override dict
    flow=copy.copy(job_flw)
    # Retrieve the spark steps from function get_spark_steps
    flow['Steps']=steps
    flow['Name']='BATCH-LOAD'
    return flow


def get_spark_steps(table, task, job_name):
    """
    Creates spark steps for each job flow

    Parameters
    ----------
    table: string

    task: string

    job_name: string

    Returns
    -------
    spark_steps: json
    """

    act_no=Variable.get('account_no')
    account_no=json.loads(act_no)
    Ingestion_list=Variable.get('jobs_Ingestion_list')
    jobs_Ingestion_list=json.loads(Ingestion_list)
    ENV=Variable.get('environment')
    ACCT=account_no[ENV]
    INJ_File=jobs_Ingestion_list[job_name]

    if task == 'Load_Ingestion':
        script=f's3://<s3_bucket>/<s3_prefix>'
        config_bucket=f'<bucket_name>'
        config_key=f'<s3_prefix_config_file>'
        name='data_load_{}_{}'.format(job_name, table.split(".")[0])
        py_file_zip=f's3://<s3_bucket>/<s3_prefix_zip_file>'
        load_type='batch'
    arg_list=Variable.get('args_list')
    args_list=list(eval(arg_list))
    args_list.extend(
        ['--py-files', py_file_zip, script, '--config_bucket', config_bucket, '--config_key', config_key, '--load_type',
         load_type])
    spark_steps={
        'Name': name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': args_list,
        },
    }
    return spark_steps


# Generate multiple self termination EMRs for each configs
@dag(
    dag_name,
    description=__doc__.partition(".")[0],  # first sentence only for hover‑tooltip
    doc_md=__doc__,                         # full Markdown shows in UI
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *",
    max_active_runs=1,                      # avoid overlapping runs due to stateful S3 staging
    tags=['emr']
)
def create_emr_daily_data_load():
    # Step to create EMR Cluster
    cluster_creator=EmrCreateJobFlowOperator(
        task_id=f'create_emr',
        job_flow_overrides=get_job_flow(jobs_list, job_flw_override)
    )

    @task(task_id=f"emr_sensor")
    def emr_sensor(**kwargs):
        config=boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )

        task_instance=kwargs['task_instance']

        clusterId=task_instance.xcom_pull(task_ids='create_emr', key='return_value')

        terminate_flag=False

        print("Waiting for EMR Cluster to be up Cluster ID => " + clusterId)

        while terminate_flag == False:
            client=boto3.client('emr', region_name='<aws_region>', config=config)
            try:
                clusterDataList=client.describe_cluster(
                    ClusterId=clusterId
                )
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'ThrottlingException':
                    t.sleep(60)
                    continue
                else:
                    raise error

            cluster_dataArray=json.dumps(clusterDataList, indent=4, sort_keys=True,
                                         default=str)  # https://stackoverflow.com/a/36142844/3299397
            cluster_data=json.loads(cluster_dataArray)

            response=cluster_data["Cluster"]["Status"]["State"]

            print(f"*************Cluster status is: {response}")

            if response == "STARTING":
                # EMR is being created
                t.sleep(60)

            if response == "BOOTSTRAPPING":
                # EMR is being created
                t.sleep(60)

            if response == "RUNNING":
                # EMR is ready
                terminate_flag=True

            if response == "WAITING":
                # EMR is ready
                terminate_flag=True

            if response == "TERMINATING":
                # EMR creation failed
                raise Exception('EMR failed with TERMINATING')

            if response == "TERMINATED":
                # EMR creation failed
                raise Exception('EMR failed with TERMINATED')

            if response == "TERMINATED_WITH_ERRORS":
                # EMR creation failed
                raise Exception('EMR failed with TERMINATED_WITH_ERRORS')

    @task
    def get_spark_steps_status(*op_args, **kwargs):
        """
        Retrieve spark step_id for each job flow

        Parameters
        ----------
        task_id: string

        Returns
        -------
        step_ids: list
        """

        step_ids=[]
        emr_client=boto3.client("emr", "<aws_region>")
        task_instance=kwargs['task_instance']

        cluster_id=task_instance.xcom_pull(task_ids='create_emr', key='return_value')
        res=emr_client.list_steps(ClusterId=cluster_id)

        for i in res['Steps']:
            step_ids.append(i['Id'])

        emr_steps_flag=False
        while emr_steps_flag == False:
            status=[]
            for step in step_ids:
                try:
                    step_status=emr_client.describe_step(
                        ClusterId=cluster_id,
                        StepId=step
                    )
                except:
                    t.sleep(60)
                    continue
                status.append(step_status['Step']['Status']['State'])

            print(f"************* Steps status list is : {str(status)}")

            if all(x == "COMPLETED" for x in status) and len(status) == len(step_ids):
                emr_steps_flag=True
            if "CANCEL_PENDING" in status or "CANCELLED" in status or "FAILED" in status or "INTERRUPTED" in status:
                raise Exception(f'Task failures found in EMR cluster : {cluster_id}')

    send_email=EmailOperator(
        mime_charset='utf-8',
        task_id='send_email',
        to=alert_email_list,
        subject=email_subject,
        html_content=email_content
    )

    # Subsequent executions of all of the above steps
    cluster_creator >> emr_sensor() >> get_spark_steps_status() >> send_email


dag=create_emr_daily_data_load()