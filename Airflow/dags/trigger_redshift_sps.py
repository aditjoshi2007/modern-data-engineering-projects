
"""
Trigger Redshift Stored Procedures DAG

This DAG orchestrates execution of Redshift stored procedures for the
"redshift_layer" using audit-driven batch control tables.

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
an audit-driven dependency model.

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
- Audit table integration
- SNS alerts + S3 run manifests
"""

# Set up static variables for DAG
local_tz=pendulum.timezone("America/Chicago")
curr_env=Variable.get('environment')
sleep_time=Variable.get('sleep_time')
sleep_iteration=Variable.get('sleep_iteration')
get_batch_run=Variable.get('ret_get_batch_run', default_var='get_batch_run')
vacuum=Variable.get('vacuum')
act_no=Variable.get('account_no')
all_accounts=json.loads(act_no)
account_no=all_accounts[curr_env]
email_content=Variable.get('success_email_html_template')
email_subject=Variable.get('success_email_subject')
adhoc_run=Variable.get('adhoc_run')
dag_name='trigger_redshift_sps'
alert_email_list=['ops@email.com']
aws_region='us-east-1'
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
            Subject="Airflow: " + dag_name + "; Env: " + curr_env + " - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            file_name=f'{process_date}_{status}_{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}'
            curated_bucket=f'<s3_curated_bucket>'
            curated_path=f'<s3_curated_prefix>/{dag_name}/load_date={process_date}/{file_name}.json'
            s3_client.put_object(Body=json_body_bytes, Bucket=curated_bucket, Key=curated_path)
            print(f'process_date: {process_date} \nfile_name: {file_name} \ncurated_bucket: {curated_bucket} \ncurated_path: {curated_path}')
            print(f'written to s3: \n {json_data} \n Bucket: {curated_bucket} \n Path: {curated_path}')
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

        select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
        cur.execute(select_stmt)

        fetch_batch_date=[doc for doc in cur]
        batch_date=fetch_batch_date[0][0]

        if adhoc_run != "none":
            sp_stmt="CALL <audit_schema>." + get_batch_run + "('batch_dt','redshift_layer','adhoc','records');".replace(
                'batch_dt', adhoc_run)
        else:
            sp_stmt="CALL schema_name." + get_batch_run + "('batch_dt','redshift_layer','regular','records');".replace(
                'batch_dt', batch_date.strftime('%Y-%m-%d'))
        cur.execute(sp_stmt)
        my_cursor=conn.cursor("records")
        result=my_cursor.fetchall()
        result.sort(key=lambda x: (x[4], x[7]))

        audit_status_insert="insert into <audit_schema>.batch_run (prcs_nme,prcs_id,status, batch_dt, end_dttm) select 'redshift_layer', (select prcs_id from <audit_schema>.prcs where prcs_nme ='redshift_layer'), 'Running', cast('audit_batch_dt' as date), null;".replace(
            'audit_batch_dt', batch_date.strftime('%Y-%m-%d'))
        print(f"*****************Cursor is executing: {audit_status_insert}")
        cur.execute(audit_status_insert)
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
            process_date=batch_date.strftime('%Y-%m-%d')
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["ProcessDate"] = process_date
            redshift_layer_info_json["JobName"]=dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {redshift_layer_info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, redshift_layer_info_json)
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

            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)

            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]
            process_date=batch_date.strftime('%Y-%m-%d')
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["ProcessDate"] = process_date
            redshift_layer_info_json["JobName"]=dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {redshift_layer_info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, redshift_layer_info_json)
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
                    audit_dict=json.loads(i[8])
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
                            dependency_check_stmt="CALL <audit_schema>.<dependency_check_sp>('{\"prcs_job_id\": \"pid\", \"layer\": \"redshift_layer\"}', 'batch_dt','records');".replace(
                                'batch_dt', i[3]).replace('pid', i[9])
                            print(f"Dependency call statement is: {dependency_check_stmt}")
                        else:
                            dependency_check_stmt="None"
                            print(f"Dependency call statement is: {dependency_check_stmt}")
                        if audit_dict["dependency_check"].upper() == "TRUE" and dependency_check_stmt != "None":
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
                                            if 'relation "redshift_layer_pjel' in e:
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
                            d_check=str(audit_dict["dependency_check"])
                            print(f"Dependency check is {d_check}. Cursor is executing SP: {sp_stmt}")
                            execute_condition=False
                            execution_counter=0
                            while execute_condition == False:
                                try:
                                    cur.execute(sp_stmt)
                                    execute_condition=True
                                except errors.UndefinedTable as e:
                                    if 'relation "redshift_layer_pjel' in e:
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
            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)
            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]
            process_date=batch_date.strftime('%Y-%m-%d')
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["ProcessDate"] = process_date
            redshift_layer_info_json["JobName"]=dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {redshift_layer_info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, redshift_layer_info_json)
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

            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)

            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]

            if adhoc_run != "none":
                sp_stmt="CALL <audit_schema>." + get_batch_run + "('batch_dt','redshift_layer','adhoc','records');".replace(
                    'batch_dt', adhoc_run)
            else:
                sp_stmt="CALL <audit_schema>." + get_batch_run + "('batch_dt','redshift_layer','regular','records');".replace(
                    'batch_dt', batch_date.strftime('%Y-%m-%d'))
            cur.execute(sp_stmt)
            my_cursor=conn.cursor("records")
            result=my_cursor.fetchall()
            tables_run = []
            for i in result: 
                if i[5] == 'COMPLETED':
                    tables_run.append(i[1])
            
            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)
            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]
            process_date=batch_date.strftime('%Y-%m-%d')
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["ProcessDate"] = process_date
            redshift_layer_info_json["JobName"]=dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Success"
            redshift_layer_info_json["FailureReason"]=""
            redshift_layer_info_json["Tables"] = tables_run
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            return redshift_layer_info_json

        except:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            cur=conn.cursor()
            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)
            fetch_batch_date=[doc for doc in cur]
            batch_date=fetch_batch_date[0][0]
            process_date=batch_date.strftime('%Y-%m-%d')
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["ProcessDate"] = process_date
            redshift_layer_info_json["JobName"] = dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {redshift_layer_info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, redshift_layer_info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1)

    @task
    def set_config_date(**kwargs):
        """
        Queries Postgres and returns a cursor to the results.
        """
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)
        try:
            postgres=PostgresHook(postgres_conn_id="redshift-connection")
            conn=postgres.get_conn()
            conn.autocommit=True
            cur=conn.cursor()

            select_stmt="select batch_date from <audit_schema>.<batch_date_table> where addi_info like '%redshift_layer%'"
            cur.execute(select_stmt)

            fetch_batch_date=[doc for doc in cur]
            date_str_new=(fetch_batch_date[0][0] + timedelta(1)).strftime('%Y-%m-%d')

            batch_date=fetch_batch_date[0][0]
            audit_status_update="update <audit_schema>.<batch_run> set status = 'Completed', end_dttm= getdate() where run_id = (select max(run_id) as run_id from <audit_schema>.<batch_run> where prcs_nme='redshift_layer' and batch_dt ='audit_batch_dt' and status = 'Running');".replace(
                'audit_batch_dt', batch_date.strftime('%Y-%m-%d'))
            print(f"*****************Cursor is executing: {audit_status_update}")
            cur.execute(audit_status_update)

            if adhoc_run != "none":
                update_stmt='''update <audit_schema>.<batch_date_table> set addi_info = replace(replace(addi_info,'adhoc','regular'),JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt'),''), batch_date= JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt')::date where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='redshift_layer';'''
            else:
                update_stmt='''update <audit_schema>.<batch_date_table> set batch_date ='batch_dt' where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='redshift_layer';'''
            cur.execute(update_stmt.replace('batch_dt', date_str_new))
            conn.commit()
            conn.close()

            return f"***** Updated batch_date insert statement : {update_stmt.replace('batch_dt', date_str_new)}"
        except:
            get_table_list_result=ti.xcom_pull(task_ids='get_table_list', key='return_value')
            redshift_layer_info_json=json.loads(get_table_list_result)
            process_date = redshift_layer_info_json["ProcessDate"]
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["JobName"]=dag_name
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                              f'An error occurred at task: get_table_category \n {redshift_layer_info_json} '
                              f'\nPlease check DAG; Below is the error: \n, {traceback.format_exc}', curr_env)
            file_name=write_to_s3('failed',  process_date, redshift_layer_info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1) 

    @task(task_id="send_sns_write_to_s3")
    def send_sns_write_to_s3(**kwargs):  # send_sns(**kwargs):
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        json1=Variable.get("redshift_layer_json")
        json2=json.loads(json1)
        redshift_layer_info_json=copy.copy(json2)

        try:
            set_config_date_result=ti.xcom_pull(task_ids='get_table_list', key='return_value')
            redshift_layer_info_json=json.loads(set_config_date_result)
            process_date = redshift_layer_info_json["ProcessDate"]
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Success"
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn, f'{redshift_layer_info_json}', curr_env)
            file_name=write_to_s3('success', process_date, redshift_layer_info_json)
            return redshift_layer_info_json, file_name
        except:
            set_config_date_result=ti.xcom_pull(task_ids='get_table_list', key='return_value')
            redshift_layer_info_json=json.loads(set_config_date_result)
            end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            process_date = redshift_layer_info_json["ProcessDate"]
            redshift_layer_info_json["JobEndDateTime"]=end_time
            redshift_layer_info_json["Status"]="Failed"
            redshift_layer_info_json["FailureReason"]=traceback.format_exc()
            redshift_layer_info_json=json.dumps(redshift_layer_info_json, indent=2)
            print(f'redshift_layer_info_json \n ============\n {redshift_layer_info_json} \n============')
            send_notification(aws_region, sns_arn,
                            f'An error occurred at task: get_sp_list \n {redshift_layer_info_json} \nPlease check DAG; Below is the error: \n, {traceback.format_exc()}',
                              curr_env)
            file_name=write_to_s3('failed', process_date, redshift_layer_info_json)
            print("file_name: ", {file_name})
            print(f'Error occurred:\n', traceback.format_exc())
            exit(1)

    group_results=get_batch.partial().expand(sp_groups=get_sp_list(adhoc_run))
    trigger_sp_batch.partial().expand(sp_batch_list=group_results) >> get_table_list(adhoc_run) >> set_config_date() >> send_sns_write_to_s3()


dag=trigger_redshift_sps()
