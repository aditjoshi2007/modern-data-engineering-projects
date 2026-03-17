import json
from datetime import timedelta

import airflow
import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

# Set up static variables for DAG
local_tz=pendulum.timezone("America/Chicago")
curr_env=Variable.get('environment')
s3_bucket=Variable.get('s3_purge_bucket')
s3_prefix=Variable.get('s3_purge_prefix_list')
s3_prefix_list=json.loads(s3_prefix)
dag_name='s3_clean_up'
schedule_interval=None

# Set up schedule interval of the DAG and email list for DAG alerts

if curr_env == 'tst' or curr_env == 'mdl':
    alert_email_list=['ops@email.com']
elif curr_env == 'prd':
    alert_email_list=['ops@email.com']

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

@dag(dag_name, default_args=DEFAULT_ARGS, schedule_interval=None, max_active_runs=1, tags=['s3_cleanup'],
     render_template_as_native_obj=True)
def s3_clean_up():
    for idx, value in enumerate(s3_prefix_list):
        list_s3_keys=S3ListOperator(
            # bucket = s3_bucket_name
            # prefix = "s3_prefix/202412"
            task_id=f"list_s3_keys_{idx}",
            bucket=s3_bucket,
            prefix=value
        )

        s3_cleanup=S3DeleteObjectsOperator(
            task_id=f"s3_cleanup_{idx}",
            bucket=s3_bucket,
            keys=f"{{{{ task_instance.xcom_pull(task_ids='list_s3_keys_{idx}', key='return_value') }}}}"
        )

        list_s3_keys >> s3_cleanup


dag=s3_clean_up()