"""
# WP Initial Load – Rawcurrent Refresh DAG

## 📌 Overview
This DAG performs an **initial (full) load and CDC refresh** of tables into the **`rawcurrent` schema** in Amazon Redshift.  
It orchestrates the following high‑level steps:

1. Reads full-load and CDC files from the **landing S3 bucket**
2. Creates **manifest files** (JSON / Parquet‑style) required by Redshift COPY
3. Loads data into **staging**
4. Refreshes the **rawcurrent** schema
5. Updates **audit tables** and batch dates
6. Sends **success notifications**

This DAG is **manually triggered only** and supports **schema‑level and table‑level reloads**.

---

## 🚀 How to Trigger the DAG
Trigger this DAG using **“Trigger DAG w/ Config”** in the Airflow UI.

### ✅ Required Configuration Format
```json
{
  "table_list": [
    {
      "table_name": "table1",
      "schema_name": "all"
    },
    {
      "table_name": "table2",
      "schema_name": "schema1,schema2"
    },
    {
      "table_name": "table3",
      "schema_name": "schema3"
    }
  ]
}
"""

from __future__ import annotations

import json
from datetime import timedelta, datetime
from itertools import groupby
from typing import TYPE_CHECKING

import airflow
import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.email import EmailOperator

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

# -------------------------------------------------------------------
# Environment configuration (Airflow Variables)
# -------------------------------------------------------------------
local_tz = pendulum.timezone("America/Chicago")

curr_env = Variable.get("environment")
account_map = json.loads(Variable.get("account_no"))
account_no = account_map[curr_env]

get_batch_run = Variable.get("get_batch_run", default_var="get_batch_run")
vacuum = Variable.get("vacuum")
adhoc_run = Variable.get("adhoc_run")

email_content = Variable.get("success_email_html_template")
email_subject = Variable.get("success_email_subject")

dag_name = "wp_initload_rawcurrent"
schedule_interval = None

# Alert email routing by environment
alert_email_list = ["ops@email.com"]

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


@dag(
    dag_name,
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    params={
        "table_list": Param(
            [{"table_name": "tbl_name", "schema_name": "all"}],
            type="array",
            title="Table List",
            description="List of tables and schemas to reload"
        )
    },
    tags=['S3', 'Redshift',  'InitialLoad', 'CDC', 'DMS', 'Attunity'],
)
def initload_rawcurrent():
    def get_file_fullLoad(s3_bucket, subfolders_address, cdc_subfolders_address):
        """
        Reads landing S3 bucket and returns:
        - Full-load file paths + sizes
        - CDC file paths + sizes
        """
        s3_client=boto3.resource('s3')
        src_bucket=s3_client.Bucket(s3_bucket)
        fullload_files_list=[]
        fullload_content=[]
        cdc_files_list=[]
        cdc_content=[]

        for bucket_object_summary in src_bucket.objects.filter(Prefix=subfolders_address[1:]):
            file_name=bucket_object_summary.key
            #        print(file_name)
            if "LOAD" in file_name or "SEGMENT" in file_name:
                fullload_files_list.append('s3://' + s3_bucket + '/' + file_name)
                fullload_content.append(bucket_object_summary.size)

        # CDC discovery
        if "dms_v2" in subfolders_address:
            cdc_bucket_prefix = subfolders_address[1:]
        else:
            cdc_bucket_prefix = cdc_subfolders_address[1:]

        print(f"DMS landing s3 bucket is : {src_bucket}")
        print(f"DMS landing s3 fullload prefix is : {subfolders_address[1:]}")
        print(f"DMS landing s3 cdc prefix is : {cdc_bucket_prefix}")

        for cdc_bucket_object_summary in src_bucket.objects.filter(Prefix=cdc_bucket_prefix):
            cdc_file_name=cdc_bucket_object_summary.key
            #print(file_name)
            if not 'LOAD' in cdc_bucket_object_summary.key.split('/')[-1]:
                cdc_files_list.append('s3://' + s3_bucket + '/' + cdc_file_name)
                cdc_content.append(cdc_bucket_object_summary.size)

        return fullload_files_list, fullload_content, cdc_files_list, cdc_content

    def get_manifest_json(s3_bucket, subfolders_address):
        s3_client=boto3.resource('s3')
        src_bucket=s3_client.Bucket(s3_bucket)
        json_gz_file=[]
        json_file=[]
        for bucket_object_summary in src_bucket.objects.filter(Prefix=subfolders_address[1:]):
            file_name=bucket_object_summary.key
            if "json.gz" in file_name:
                json_gz_file.append('s3://' + s3_bucket + '/' + file_name)
            elif "json" in file_name:
                json_file.append('s3://' + s3_bucket + '/' + file_name)

        return json_gz_file, json_file

    def manifest_fulload_json_gz(manifest_bucket, manifest_key, table_name, schema_name, fullload_files_list):
        manifest_full_key=str(manifest_key).replace('cdc', 'full') + f"{schema_name}_{table_name}.manifest"

        ###now preparing for full load
        if len(fullload_files_list) > 0:
            manifest_full_table={}
            manifest_full_table["entries"]=[]
            for i in range(len(fullload_files_list)):
                new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True)
                manifest_full_table["entries"].append(new_item_full)
            new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
            # Upload JSON String to an S3 Object
            client=boto3.client('s3')
            client.put_object(
                Bucket=manifest_bucket,
                Key=manifest_full_key,
                Body=new_data_full
            )
            print(f"Fullload file {schema_name}_{table_name}.manifest has been created for initial load")

    def manifest_fulload_json(manifest_bucket, manifest_key, table_name, schema_name, fullload_files_list):
        manifest_full_key=str(manifest_key).replace('cdc', 'full') + f"{schema_name}_{table_name}.manifest"

        ###now preparing for full load
        if len(fullload_files_list) > 0:
            manifest_full_table={}
            manifest_full_table["entries"]=[]
            for i in range(len(fullload_files_list)):
                new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True)
                manifest_full_table["entries"].append(new_item_full)
            new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
            # Upload JSON String to an S3 Object
            client=boto3.client('s3')
            client.put_object(
                Bucket=manifest_bucket,
                Key=manifest_full_key,
                Body=new_data_full
            )
            print(f"Fullload file {schema_name}_{table_name}.manifest has been created for initial load")

    def manifest_cdc_json(manifest_bucket, manifest_key, table_name, schema_name, cdc_files_list):
        manifest_cdc_key=str(manifest_key) + f"{schema_name}_{table_name}.manifest"

        ###now preparing for full load
        if len(cdc_files_list) > 0:
            manifest_cdc_table={}
            manifest_cdc_table["entries"]=[]
            for i in range(len(cdc_files_list)):
                new_item_cdc=dict(url=str(cdc_files_list[i]), mandatory=True)
                manifest_cdc_table["entries"].append(new_item_cdc)
            new_data_cdc=json.dumps(manifest_cdc_table, indent=2, default=str)
            # Upload JSON String to an S3 Object
            client=boto3.client('s3')
            client.put_object(
                Bucket=manifest_bucket,
                Key=manifest_cdc_key,
                Body=new_data_cdc
            )
            print(f"CDC file {schema_name}_{table_name}.manifest has been created for initial load")

    def manifest_cdc_json_gz(manifest_bucket, manifest_key, table_name, schema_name, cdc_files_list):
        manifest_cdc_key=str(manifest_key) + f"{schema_name}_{table_name}.manifest"

        ###now preparing for full load
        if len(cdc_files_list) > 0:
            manifest_cdc_table={}
            manifest_cdc_table["entries"]=[]
            for i in range(len(cdc_files_list)):
                new_item_cdc=dict(url=str(cdc_files_list[i]), mandatory=True)
                manifest_cdc_table["entries"].append(new_item_cdc)
            new_data_cdc=json.dumps(manifest_cdc_table, indent=2, default=str)
            # Upload JSON String to an S3 Object
            client=boto3.client('s3')
            client.put_object(
                Bucket=manifest_bucket,
                Key=manifest_cdc_key,
                Body=new_data_cdc
            )
            print(f"CDC file {schema_name}_{table_name}.manifest has been created for initial load")

    def manifest_fullload_parquet(fullload_files_list, fullload_content, manifest_bucket, manifest_full_key):
        ##preparing manifest for fullload
        manifest_full_table={}
        manifest_full_table["entries"]=[]
        for i in range(len(fullload_files_list)):
            new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True,
                               meta=dict(content_length=fullload_content[i]))
            manifest_full_table["entries"].append(new_item_full)
        new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_full_key,
            Body=new_data_full
        )
        print(f"Fullload file {manifest_full_key} has been created for initial load")

    def manifest_cdc_parquet(cdc_files_list, cdc_content, manifest_bucket, manifest_cdc_key):
        ##preparing manifest for cdc
        manifest_cdc_table={}
        manifest_cdc_table["entries"]=[]
        for i in range(len(cdc_files_list)):
            new_item=dict(url=str(cdc_files_list[i]), mandatory=True, meta=dict(content_length=cdc_content[i]))
            manifest_cdc_table["entries"].append(new_item)
        new_data=json.dumps(manifest_cdc_table, indent=2, default=str)
        #            print(new_data)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_cdc_key,
            Body=new_data
        )
        print(f"CDC file {manifest_cdc_key} has been created for initial load")

    
    def create_audit_entry(
        stg_schema_name,
        rc_schema_name,
        table_name,
        prcs_job_id,
        sch_table_name,
        batch_dte,
        data_flow_no,
    ):
        """
        Creates START and END audit entries when no data
        is available for processing.
        """
        postgres=PostgresHook(postgres_conn_id="redshift-warehouse")
        conn=postgres.get_conn()
        conn.autocommit=True
        cur=conn.cursor()
        print(f"cursor object: {cur}")
        print(f"{stg_schema_name}.{sch_table_name} --> Rc_Cnt:0, hence coming out with end,rc_cnt=0")
        excn_id=datetime.now(local_tz).strftime("%Y%m%d%H%M%S")
        start_addi_info=f'{{"tbl":"{sch_table_name}", "msg":"load initiated"}}'
        end_addi_info=f'{{"tbl":"{stg_schema_name}.{sch_table_name}", "rc_cnt":"0","msg":"landing table has 0 rec"}}'
        stg_qry_exec_list=[]
        qry_1="CREATE TEMP TABLE IF NOT EXISTS prcs_job_excn_log_t (id bigint identity(1,1) ENCODE delta,prcs_job_excn_log_id bigint ENCODE zstd,prcs_job_id integer ENCODE zstd,status character varying(30) ENCODE zstd,batch_dt date ENCODE raw,rec_cren_dttm timestamp without time zone DEFAULT getdate() ENCODE raw,additional_info character varying(65535) DEFAULT '{}'::character varying ENCODE zstd ) SORTKEY (id, batch_dt, rec_cren_dttm );"
        stg_qry_exec_list.append(qry_1)
        qry_2=f"INSERT INTO prcs_job_excn_log_t (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, additional_info) VALUES ('{excn_id}','{prcs_job_id}','START','{batch_dte}','{start_addi_info}');"
        stg_qry_exec_list.append(qry_2)
        qry_3=f"INSERT INTO prcs_job_excn_log_t (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, additional_info) VALUES ('{excn_id}','{prcs_job_id}','END','{batch_dte}','{end_addi_info}');"
        stg_qry_exec_list.append(qry_3)
        qry_4=f"INSERT INTO <audit_schema>.{stg_schema_name}_pjel (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, rec_cren_dttm, additional_info) (select prcs_job_excn_log_id, prcs_job_id, status, batch_dt, rec_cren_dttm, additional_info from prcs_job_excn_log_t order by id);"
        stg_qry_exec_list.append(qry_4)
        for qry in stg_qry_exec_list:
            cur.execute(qry)

        rc_prcs_job_id_qry=f"select prcs_job_id from <audit_schema>.prcs_job where prcs_id=<prcs_id> and data_flow_no = {data_flow_no};"
        cur.execute(rc_prcs_job_id_qry)
        rc_prcs_job_id=cur.fetchall()

        
        # Temp audit table pattern
        # Ensures atomic logging across staging and rawcurrent
        if rc_prcs_job_id is None:
            print("rc_prcs_job_id is None. Failing the Dag now")
            exit(1)
        else:
            rc_qry_exec_list=[]
            rc_end_addi_info=f'{{"tbl":"{rc_schema_name}.{table_name}", "rc_cnt":"0","msg":"landing table has 0 rec"}}'
            qry_5="truncate table prcs_job_excn_log_t;"
            rc_qry_exec_list.append(qry_5)
            qry_6=f"INSERT INTO prcs_job_excn_log_t (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, additional_info) VALUES ('{excn_id}','{prcs_job_id}','START','{batch_dte}','{start_addi_info}');"
            rc_qry_exec_list.append(qry_6)
            qry_7=f"INSERT INTO prcs_job_excn_log_t (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, additional_info) VALUES ('{excn_id}','{prcs_job_id}','END','{batch_dte}','{rc_end_addi_info}');"
            rc_qry_exec_list.append(qry_7)
            qry_8=f"INSERT INTO <audit_schema>.{rc_schema_name}_pjel (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, rec_cren_dttm, additional_info) (select prcs_job_excn_log_id, prcs_job_id, status, batch_dt, rec_cren_dttm, additional_info from prcs_job_excn_log_t order by id);"
            rc_qry_exec_list.append(qry_8)
            for qry in rc_qry_exec_list:
                cur.execute(qry)
        notice=conn.notices
        for msg in notice:
            print(msg.strip())
            if 'EXCEPTION' in msg:
                raise Exception(f"Found Exception: {msg}")
        conn.commit()
        cur.close()
        conn.close()

    @task(task_id="get_table_list")
    def get_table_list(**kwargs) -> list[str]:
        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run
        if "table_list" not in dag_run.conf:
            print("Ooops, no table names given, was no UI used to trigger?")
            return []
        return dag_run.conf["table_list"]

    @task
    def get_sp_list(table_list):
        """
        Queries Postgres and returns a cursor to the results.
        """
        sp_list=[]
        sp_groups=[]
        uniquekeys=[]
        all_schema_tables=[]
        non_all_schema_tables=[]
        schema_tables=[]

        for i in table_list:
            if i['schema_name'] == 'all':
                all_schema_tables.append(i['table_name'].strip().lower())
            else:
                non_all_schema_tables.append(i)

        for i in non_all_schema_tables:
            if len(i['schema_name'].split(',')) > 1:
                for j in i['schema_name'].split(','):
                    schema_tables.append(j.strip().lower() + "_" + i["table_name"].strip().lower())
            else:
                schema_tables.append(i["schema_name"].strip().lower() + "_" + i["table_name"].strip().lower())

        postgres=PostgresHook(postgres_conn_id="redshift-warehouse")
        conn=postgres.get_conn()
        cur=conn.cursor()
        # tbl_list = context["params"]["table_list"]
        # tbl_list=str(table_list)[1:-1]
        print(f"DAG has received this list of tables to reload: {all_schema_tables} and {schema_tables}")

        if len(schema_tables) > 0 and len(all_schema_tables) > 0:
            all_schema_tables_tbl_list=str(all_schema_tables)[1:-1]
            schema_tables_tbl_list=str(schema_tables)[1:-1]
            sp_stmt=f"select * from (select pj.prcs_job_id,substring(prcs_job_name,position('_' in prcs_job_name)+1) as table_name,'call staging.sp_landing_to_staging_full_data_sync(batch_dt, full_file_cnt, cdc_file_cnt);' as procedure_call,cast(c.batch_date as date) as batch_dt,prcs_job_exec_seq_no,'NOTSTARTED' as status,0 as record_count,data_flow_no,additional_info from <audit_schema>.prcs_job pj join (select batch_date from <audit_schema>.<batch_date> where JSON_EXTRACT_PATH_TEXT(addi_info, 'layer')='staging') c on 1=1 where prcs_id=<prcs_id> and substring(prcs_job_name,position('_' in prcs_job_name)+1) in ({all_schema_tables_tbl_list}) union all select pj.prcs_job_id,substring(prcs_job_name,position('_' in prcs_job_name)+1) as table_name,'call staging.sp_landing_to_staging_full_data_sync(batch_dt);' as procedure_call,cast(c.batch_date as date) as batch_dt,prcs_job_exec_seq_no,'NOTSTARTED' as status,0 as record_count,data_flow_no,additional_info from <audit_schema>.prcs_job pj join (select batch_date from <audit_schema>.<batch_date> where JSON_EXTRACT_PATH_TEXT(addi_info, 'layer')='staging') c on 1=1 where prcs_id=<prcs_id> and prcs_job_name in({schema_tables_tbl_list})) order by prcs_job_exec_seq_no, data_flow_no;"
        elif len(schema_tables) == 0 and len(all_schema_tables) > 0:
            tbl_list=str(all_schema_tables)[1:-1]
            sp_stmt=f"select * from (select pj.prcs_job_id,substring(prcs_job_name,position('_' in prcs_job_name)+1) as table_name,'call staging.sp_landing_to_staging_full_data_sync(batch_dt, full_file_cnt, cdc_file_cnt);' as procedure_call,cast(c.batch_date as date) as batch_dt,prcs_job_exec_seq_no,'NOTSTARTED' as status,0 as record_count,data_flow_no,additional_info from <audit_schema>.prcs_job pj join (select batch_date from <audit_schema>.<batch_date> where JSON_EXTRACT_PATH_TEXT(addi_info, 'layer')='staging') c on 1=1 where prcs_id=<prcs_id> and substring(prcs_job_name,position('_' in prcs_job_name)+1) in ({tbl_list}));"
        elif len(schema_tables) > 0 and len(all_schema_tables) == 0:
            tbl_list=str(schema_tables)[1:-1]
            sp_stmt=f"select * from (select pj.prcs_job_id,substring(prcs_job_name,position('_' in prcs_job_name)+1) as table_name,'call staging.sp_landing_to_staging_full_data_sync(batch_dt, full_file_cnt, cdc_file_cnt);' as procedure_call,cast(c.batch_date as date) as batch_dt,prcs_job_exec_seq_no,'NOTSTARTED' as status,0 as record_count,data_flow_no,additional_info from <audit_schema>.prcs_job pj join (select batch_date from <audit_schema>.<batch_date> where JSON_EXTRACT_PATH_TEXT(addi_info, 'layer')='staging') c on 1=1 where prcs_id=<prcs_id> and prcs_job_name in({tbl_list})) order by prcs_job_exec_seq_no, data_flow_no;"
        else:
            print("No tables found to reload. Please re-trigger DAG with valid json object")
            exit(1)

        print(f"SQL is {sp_stmt}")

        cur.execute(sp_stmt)
        # my_cursor=conn.cursor("records")
        result=cur.fetchall()
        result.sort(key=lambda x: (x[4], x[7]))
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

        return sp_groups

    @task
    def get_batch(sp_groups):
        return sp_groups

    @task
    def trigger_sp_batch(sp_batch_list):
        for i in sp_batch_list:
            if i[5] != 'COMPLETED':
                audit_dict=json.loads(i[8])
                schema_name=audit_dict["table_schema"]
                table_name=audit_dict["table_name"]
                bucket=audit_dict["bucket"]
                if '/dms_v4' in audit_dict["key"] and '<schema_name>' not in audit_dict["key"]:
                    split_key=audit_dict["key"].split('/')
                    split_key[-2] = split_key[-2] + '__FL'
                    key='/'.join(split_key)
                else:
                    key=audit_dict["key"]
                rc_schema_name=audit_dict["rc_schema_name"]
                stg_schema_name=audit_dict["stg_schema_name"]
                full_load_file_cnt=0
                cdc_file_cnt=0

                load_date=str(i[3])
                manifest_bucket=f"<s3_bucket_name>"
                manifest_key=f"dc/manifest/{table_name}/cdc/"

                if '/dms_v4' in key:
                    service="dms"
                    split_key=audit_dict["key"].split('/')
                    split_key[-2]=split_key[-2] + '__CT'
                    delta_key='/'.join(split_key)
                elif '/dms' in key.lower():
                    service="dms"
                    delta_key=key.replace("fullload", "cdc")
                elif '/attunity' in key.lower():
                    service="attunity"
                    delta_key=str(key)[:-1] + "__ct/"

                if service == "attunity":
                    ##processing full load data to staging
                    fullload_json_gz, fullload_json=get_manifest_json(bucket, key)
                    ##processing fullload json.gz files.
                    if len(fullload_json_gz) == 0 and len(fullload_json) == 0:
                        print(f"No files to process for {schema_name}.{table_name}")
                        create_audit_entry(stg_schema_name, rc_schema_name, table_name, i[0], i[1], i[3], i[7])
                        continue

                    if len(fullload_json_gz) > 0:
                        print(f"File count for {schema_name}.{table_name} is {str(len(fullload_json_gz))}")
                        full_load_file_cnt=len(fullload_json_gz)
                        manifest_fulload_json_gz(manifest_bucket, manifest_key, table_name, schema_name,
                                                 fullload_json_gz)
                        print(f"Manifest file for {schema_name}.{table_name} has been loaded in s3")

                    ##processing fullload json files.
                    if len(fullload_json) > 0:
                        print(f"File count for {schema_name}.{table_name} is {str(len(fullload_json))}")
                        full_load_file_cnt=len(fullload_json_gz)
                        manifest_fulload_json(manifest_bucket, manifest_key, table_name, schema_name, fullload_json)
                        print(f"Manifest file for {schema_name}.{table_name} has been loaded in s3")

                    ##processing delta load to staging
                    cdc_json_gz, cdc_json=get_manifest_json(bucket, delta_key)

                    if len(cdc_json_gz) > 0:
                        print(f"File count for {schema_name}.{table_name} is {str(len(cdc_json_gz))}")
                        cdc_file_cnt=len(cdc_json_gz)
                        manifest_cdc_json_gz(manifest_bucket, manifest_key, table_name, schema_name, cdc_json_gz)
                        print(f"Manifest file for {schema_name}.{table_name} has been loaded in s3")

                    ##processing  cdc json files
                    if len(cdc_json) > 0:
                        print(f"File count for {schema_name}.{table_name} is {str(len(cdc_json))}")
                        cdc_file_cnt=len(cdc_json)
                        manifest_cdc_json(manifest_bucket, manifest_key, table_name, schema_name, cdc_json)
                        print(f"Manifest file for {schema_name}.{table_name} has been loaded in s3")
                elif service == "dms":
                    fullload_files_list, fullload_content, cdc_files_list, cdc_content=get_file_fullLoad(bucket, key,
                                                                                                         delta_key)
                    manifest_cdc_key=str(manifest_key) + f"{schema_name}_{table_name}.manifest"
                    manifest_full_key=str(manifest_key).replace('cdc', 'full') + f"{schema_name}_{table_name}.manifest"

                    ##preparing manifest for initial load
                    if len(fullload_files_list) > 0:
                        print(
                            f"File count for initial load of {schema_name}.{table_name} is {str(len(fullload_files_list))}")
                        full_load_file_cnt=len(fullload_files_list)
                        manifest_fullload_parquet(fullload_files_list, fullload_content, manifest_bucket,
                                                  manifest_full_key)
                        print(f"Manifest file for initial load of {schema_name}.{table_name} has been loaded in s3")
                    else:
                        print(f"No files to process for initial load of {schema_name}.{table_name}")
                        create_audit_entry(stg_schema_name, rc_schema_name, table_name, i[0], i[1], i[3], i[7])
                        continue

                    ##preparing manifest for cdc
                    if len(cdc_files_list) > 0:
                        print(f"File count for {schema_name}.{table_name} is {str(len(cdc_files_list))}")
                        cdc_file_cnt=len(cdc_files_list)
                        manifest_cdc_parquet(cdc_files_list, cdc_content, manifest_bucket, manifest_cdc_key)
                        print(f"Manifest file for {schema_name}.{table_name} has been loaded in s3")

                postgres=PostgresHook(postgres_conn_id="redshift-warehouse")
                conn=postgres.get_conn()
                conn.autocommit=True
                cur=conn.cursor()
                print(f"cursor object: {cur}")
                if cdc_file_cnt > 0:
                    sp_stmt=i[2].replace('batch_dt', str(i[0])).replace('full_file_cnt',
                                                                        str(full_load_file_cnt)).replace('cdc_file_cnt',
                                                                                                         str(cdc_file_cnt))
                else:
                    sp_stmt=i[2].replace('batch_dt', str(i[0])).replace('full_file_cnt',
                                                                        str(full_load_file_cnt)).replace('cdc_file_cnt',
                                                                                                         '0')
                print(f"Cursor is executing SP: {sp_stmt}")
                cur.execute(sp_stmt)
                notice=conn.notices
                for msg in notice:
                    print(msg.strip())
                    if 'EXCEPTION' in msg:
                        raise Exception(f"Found Exception: {msg}")
                conn.commit()
                if vacuum == 'True':
                    vacuum_stmt=f'vacuum rawcurrent.{i[1]}'
                    cur.execute(vacuum_stmt)
                cur.close()
                conn.close()

    @task
    def set_config_date(**kwargs):
        """
        Queries Postgres and returns a cursor to the results.
        """

        ti: TaskInstance=kwargs["ti"]
        dag_run: DagRun=ti.dag_run

        # Check if specific source table is present; skip incrementing the <batch_date> date
        table_list=ti.xcom_pull(task_ids='get_table_list', key='return_value')
        for i in table_list:
            if i['schema_name'] == '<schema_name>':
                raise AirflowSkipException

        postgres=PostgresHook(postgres_conn_id="redshift-warehouse")
        conn=postgres.get_conn()
        cur=conn.cursor()

        select_stmt="select batch_date from <audit_schema>.<batch_date> where addi_info like '%staging%'"
        cur.execute(select_stmt)

        fetch_batch_date=[doc for doc in cur]
        date_str_new=(fetch_batch_date[0][0] + timedelta(1)).strftime('%Y-%m-%d')

        if adhoc_run != "none":
            # rawcurrent_update_stmt='''update <audit_schema>.<batch_date> set addi_info = replace(replace(addi_info,'adhoc','regular'),JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt'),''), batch_date= JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt')::date where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='rawcurrent';'''
            staging_update_stmt='''update <audit_schema>.<batch_date> set addi_info = replace(replace(addi_info,'adhoc','regular'),JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt'),''), batch_date= JSON_EXTRACT_PATH_TEXT(addi_info,'reset_dt')::date where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='staging';'''
        else:
            # rawcurrent_update_stmt='''update <audit_schema>.<batch_date> set batch_date ='batch_dt' where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='rawcurrent';'''
            staging_update_stmt='''update <audit_schema>.<batch_date> set batch_date ='batch_dt' where JSON_EXTRACT_PATH_TEXT(addi_info,'layer')='staging';'''
        # cur.execute(rawcurrent_update_stmt.replace('batch_dt', date_str_new))
        cur.execute(staging_update_stmt.replace('batch_dt', date_str_new))
        conn.commit()
        conn.close()

        return (f"***** Updated batch_date i"
                f"nsert statement : {staging_update_stmt.replace('batch_dt', date_str_new)}")

    send_email=EmailOperator(
        mime_charset='utf-8',
        task_id='send_email',
        to=alert_email_list,
        subject=email_subject,
        html_content=email_content
    )

    group_results=get_batch.partial().expand(sp_groups=get_sp_list(get_table_list()))
    trigger_sp_batch.partial().expand(sp_batch_list=group_results) >> set_config_date() >> send_email


dag=initload_rawcurrent()