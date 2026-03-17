"""This DAG is utilized for starting DMS Replication task

Set below Airflow Variables for DAG:

start_task_type : STRING (default=’start-replication’) (‘start-replication’|’resume-processing’|’reload-target’)

start_task_args : DICT (DMS task start arguments, example - {"CdcStartTime":"2024-06-13T20:27:00"} )

dms_reload_task_arn : LIST (Comma Seperated list of Replication Task ARNs [""] )

"""

# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator
from datetime import timedelta, datetime
import json

################


curr_env=Variable.get('environment')
dms_task_arn=Variable.get('dms_reload_task_arn')
dms_task_arn_list = json.loads(dms_task_arn)
start_task_type=Variable.get('start_task_type')
start_task_args=Variable.get('start_task_args')
dag_name='start_dms_replication_task'

alert_email_list=['ops@email.com']

# Set up schedule interval of the DAG and email list for DAG alerts
schedule_interval=None

if curr_env == 'tst' or curr_env == 'mdl':
    alert_email_list=['ops@email.com']
elif curr_env == 'prd':
    alert_email_list=['ops@email.com']

DEFAULT_ARGS={
    'owner': 'owner_name',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2022, 3, 10),
    'email': alert_email_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Generate Dag
with DAG(dag_id=dag_name, description=__doc__.partition(".")[0], doc_md=__doc__, default_args=DEFAULT_ARGS, schedule_interval=schedule_interval, max_active_runs=1,
         tags=['dms'], catchup=False) as dag:
    # Dummy step to start the DAG
    begin_step=DummyOperator(task_id=f'Start_Task')

    for arn in dms_task_arn_list:
        reload_dms_task = DmsStartTaskOperator(
            task_id=f'reload_dms_task_{dms_task_arn_list.index(arn)}',
            replication_task_arn=arn,
            start_replication_task_type=start_task_type,
            start_task_kwargs=json.loads(start_task_args)
        )
        
        begin_step >> reload_dms_task