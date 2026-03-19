
"""
Trigger Redshift Stored Procedures DAG

This DAG orchestrates execution of Redshift stored procedures for the
"redshift_layer" using process-driven batch control tables.

High-level responsibilities:
- Determine batch date (regular vs ad-hoc)
- Fetch ordered stored procedure execution groups
- Enforce dependency checks between procedures
- Execute SPs with retry / deadlock handling
- Optionally VACUUM tables post-load
- Persist run metadata to S3
- Send SNS notifications on success/failure

All environment-specific configuration is externalized via Airflow Variables.
"""

import copy
import json
# from msilib.schema import tables
import time
from datetime import timedelta
from itertools import groupby
from datetime import timedelta, datetime
import boto3
import traceback
from botocore.config import Config as boto3_config
from psycopg2 import errors
from typing import TYPE_CHECKING

import airflow
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.email import EmailOperator

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

# ---------------------------------------------------------------------
# DAG Metadata & Global Configuration
# ---------------------------------------------------------------------
DOC_MD = """
# 🚀 Trigger Redshift Stored Procedures

This DAG controls **batch execution of Redshift stored procedures** using
an process-driven dependency model.

## Swimlanes
- **Orchestration:** Apache Airflow
- **Compute:** Amazon Redshift (stored procedures)
- **Storage:** Amazon S3 (run metadata)

## Cost Model
- **Persistent:** Amazon Redshift
- **Ephemeral:** Airflow tasks

## Key Capabilities
- Dependency-aware SP execution
- Deadlock-safe retries
- process table integration
- SNS alerts + S3 run manifests
"""

# Set up static variables for DAG
local_tz=pendulum.timezone("America/Chicago")
curr_env="env"
sleep_time="seconds"
sleep_iteration="iter_count"
vacuum="True/False"
account_no="account_no"
adhoc_run=Variable.get('adhoc_run')
dag_name='trigger_redshift_sps'
alert_email_list=['ops@email.com']
aws_region="aws_region"
sns_arn=f"<sns_arn>"
schedule_interval=None

# Loading datapipeline_monitor_json into the DAG and copying it into another vairble to avoid overwriting
json1 = Variable.get("redshift_layer_json")
json2 = json.loads(json1)
redshift_layer_info_json = copy.copy(json2)


# Set up schedule interval of the DAG and email list for DAG alerts

if curr_env == 'tst' or curr_env == 'mdl':
    # schedule_interval = '0 13 * * *'
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


@dag(dag_name, doc_md=DOC_MD, default_args=DEFAULT_ARGS, schedule_interval=None, max_active_runs=1, tags=["redshift", "stored-procedures"])
def trigger_redshift_sps():
    def dependency_check(dependency_check_stmt):
        """
        Queries Postgres and returns a cursor to the results.
        """
        postgres=PostgresHook(postgres_conn_id="redshift-connection")
        conn=postgres.get_conn()
        cur=conn.cursor()

        cur.execute(dependency_check_stmt)
        my_cursor=conn.cursor("records")
        result=my_cursor.fetchall()
        conn.close()
        return result[0][0]

    def send_notification(aws_region, sns_arn, custom_message, curr_env):
        """Send operational notification via SNS."""
        config=boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )
        client=boto3.client('sns', region_name=aws_region, config=config)
        response=client.publish(
            TopicArn=f"<topic_arn>",
            Message=custom_message,
            Subject="Airflow Subject"
        )
        return response

    def write_to_s3(status, process_date, json_data):
        try:
            config=boto3_config(
                retries={
                    'max_attempts': 30,
                    'mode': 'standard'
                }
            )
            s3_client=boto3.client('s3', config=config)
            json_str=json.dumps(json_data, indent=2, default=str)
            json_body_bytes=json_str.encode('utf-8')
            file_name='<file_name>'
            curated_bucket=f'<s3_curated_bucket>'
            curated_path=f'<s3_curated_prefix>'
            s3_client.put_object(Body=json_body_bytes, Bucket=curated_bucket, Key=curated_path)
            return curated_bucket, curated_path, file_name
        except:
            send_notification(aws_region, sns_arn,
                              f'An error occurred while writing file to S3 , please check DAG\n, {traceback.format_exc()}',
                              curr_env)
            print(f'Error occurred:\n', traceback.format_exc())

    @task
    def get_sp_list(adhoc_run, **kwargs):
        """
        Queries Postgres and returns a cursor to the results.
        """

        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)

        sp_list=[]
        sp_groups=[]
        uniquekeys=[]

        postgres=PostgresHook(postgres_conn_id="redshift-connection")
        conn=postgres.get_conn()
        cur=conn.cursor()
        cur.execute(sp_stmt)
        my_cursor=conn.cursor("records")
        result=my_cursor.fetchall()
        result.sort(key=lambda x: (x[4], x[7]))

        print(f"*****************Cursor is executing: {status_insert}")
        cur.execute(status_insert)
        conn.commit()
        conn.close()

        for i in result:
            y=list(i)
            y[3]=i[3].strftime('%Y-%m-%d')
            i=tuple(y)
            sp_list.append(i)

        # iterate over to get a list of dicts
        # details_dicts = [group for key, group in groupby(sp_list, lambda x: x[4])]

        for key, group in groupby(sp_list, lambda x: x[4]):
            sp_groups.append(list(group))
            uniquekeys.append(key)

        try:
            return sp_groups
        except:
            print(f'redshift_layer_info_json \n ============\n {info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1) 

    @task
    def get_batch(sp_groups, **kwargs):
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)
        try:
            return sp_groups
        except:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            cur=conn.cursor()
            cur.execute(select_stmt)

            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]
            print(f'info_json \n ============\n {info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1)

    @task
    def trigger_sp_batch(sp_batch_list, **kwargs):
        
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)
        
        try:
            for i in sp_batch_list:
                trigger_sp_condition=False
                if i[5] != 'COMPLETED':
                    process_dict=json.loads(i[8])
                    if adhoc_run != "none":
                        postgres=PostgresHook(postgres_conn_id="redshift-connection")
                        conn=postgres.get_conn()
                        conn.autocommit=True
                        cur=conn.cursor()
                        print(f"cursor object: {cur}")
                        sp_stmt=i[2].replace('batch_dt', f"'{adhoc_run}'")
                        print(f"Cursor is executing SP: {sp_stmt}")
                        cur.execute(sp_stmt)
                        notice=conn.notices
                        for msg in notice:
                            print(msg.strip())
                            if 'EXCEPTION' in msg:
                                raise Exception(f"Found Exception: {msg}")
                        conn.commit()
                    else:
                        postgres=PostgresHook(postgres_conn_id="redshift-connection")
                        conn=postgres.get_conn()
                        conn.autocommit=True
                        cur=conn.cursor()
                        print(f"cursor object: {cur}")
                        max_tries=0
                        sp_stmt=i[2].replace('batch_dt', f"'{i[3]}'")
                        if i[9] is not None:
                            dependency_check_stmt="CALL <dependency_check_sp>"
                            print(f"Dependency call statement is: {dependency_check_stmt}")
                        else:
                            dependency_check_stmt="None"
                            print(f"Dependency call statement is: {dependency_check_stmt}")
                        if process_dict["dependency_check"].upper() == "TRUE" and dependency_check_stmt != "None":
                            while not trigger_sp_condition:
                                dependency_check_result=dependency_check(dependency_check_stmt)
                                print(f"*****************Dependency check result is : {str(dependency_check_result)}")
                                if str(dependency_check_result) == '0':
                                    print(f"Cursor is executing SP: {sp_stmt}")
                                    execute_condition=False
                                    execution_counter=0
                                    while execute_condition == False:
                                        try:
                                            cur.execute(sp_stmt)
                                            execute_condition=True
                                        except errors.UndefinedTable as e:
                                            if 'relation "redshift_layer' in e:
                                                print(
                                                    f"*******************SP encountered deadlock. Current try is : {execution_counter}")
                                                time.sleep(60)
                                                execution_counter=execution_counter + 1
                                                continue
                                            else:
                                                raise e
                                        except Exception as err:
                                            # psycopg2 extensions.Diagnostics object attribute
                                            print("\n*******************extensions.Diagnostics:", err.diag)

                                            # print the pgcode and pgerror exceptions
                                            print("*******************pgerror:", err.pgerror)
                                            print("*******************pgcode:", err.pgcode, "\n")
                                    notice=conn.notices
                                    for msg in notice:
                                        print(msg.strip())
                                        if 'EXCEPTION' in msg:
                                            raise Exception(f"Found Exception: {msg}")
                                    conn.commit()
                                    if vacuum == 'True':
                                        vacuum_stmt=f'vacuum redshift_layer.{i[1]}'
                                        cur.execute(vacuum_stmt)
                                    cur.close()
                                    conn.close()
                                    trigger_sp_condition=True
                                else:
                                    print(
                                        f"Sleeping for now, dependent job is yet to run or is running. Current try is----------{str(max_tries)}")
                                    time.sleep(int(sleep_time))
                                    max_tries=max_tries + 1
                                    if max_tries == int(sleep_iteration):
                                        msg=f"Max tries reached. Dependent job has not run successfully yet. Failing this dag"
                                        raise Exception(f"Found Exception: {msg}")
                                        trigger_sp_condition=True
                        else:
                            d_check=str(process_dict["dependency_check"])
                            print(f"Dependency check is {d_check}. Cursor is executing SP: {sp_stmt}")
                            execute_condition=False
                            execution_counter=0
                            while execute_condition == False:
                                try:
                                    cur.execute(sp_stmt)
                                    execute_condition=True
                                except errors.UndefinedTable as e:
                                    if 'relation "redshift_layer' in e:
                                        print(
                                            f"*******************SP encountered deadlock. Current try is : {execution_counter}")
                                        time.sleep(60)
                                        execution_counter=execution_counter + 1
                                        continue
                                    else:
                                        raise e
                                except Exception as err:
                                    # psycopg2 extensions.Diagnostics object attribute
                                    print("\n*******************extensions.Diagnostics:", err.diag)

                                    # print the pgcode and pgerror exceptions
                                    print("*******************pgerror:", err.pgerror)
                                    print("*******************pgcode:", err.pgcode, "\n")
                            notice=conn.notices
                            for msg in notice:
                                print(msg.strip())
                                if 'EXCEPTION' in msg:
                                    raise Exception(f"Found Exception: {msg}")
                            conn.commit()
                            if vacuum == 'True':
                                vacuum_stmt=f'vacuum redshift_layer.{i[1]}'
                                cur.execute(vacuum_stmt)
                            cur.close()
                            conn.close()
        except:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            cur=conn.cursor()
            cur.execute(select_stmt)
            fetch_batch_date=[doc for doc in cur]
            print(f'info_json \n ============\n {info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1)   


    @task
    def get_table_list(adhoc_run,**kwargs):
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)
        try:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            cur=conn.cursor()
            cur.execute(select_stmt)

            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]

            cur.execute(sp_stmt)
            my_cursor=conn.cursor("records")
            result=my_cursor.fetchall()
            tables_run = []
            for i in result: 
                if i[5] == 'COMPLETED':
                    tables_run.append(i[1])
            
            cur.execute(select_stmt)
            fetch_batch_date=[doc for doc in cur]
            
            print(f'info_json \n ============\n {info_json} \n============')
            return info_json

        except:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            cur=conn.cursor()
            cur.execute(select_stmt)
            
            print(f'info_json \n ============\n {info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1)

    @task(task_id="send_sns_write_to_s3")
    def send_sns_write_to_s3(**kwargs):  
        # send_sns(**kwargs):
        
    group_results=get_batch.partial().expand(sp_groups=get_sp_list(adhoc_run))
    trigger_sp_batch.partial().expand(sp_batch_list=group_results) >> get_table_list(adhoc_run) >> send_sns_write_to_s3()


dag=trigger_redshift_sps()
