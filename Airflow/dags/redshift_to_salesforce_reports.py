"""
# 📊 Redshift to Salesforce Reports DAG

## 🌟 Overview
The **Redshift to Salesforce Reports** DAG orchestrates the **controlled export of reporting data from Amazon Redshift to Salesforce**.  
It supports **batch‑driven execution**, **config‑based unloads to S3**, and **optional AppFlow triggers** to push data into Salesforce.

This DAG is scheduled to trigger based on completion of daily ingestion cycle and designed for:
- Report publishing
- Downstream Salesforce integrations
- Controlled re‑runs and failover scenarios

---

## 🧠 What This DAG Does
For each configured table/job, the DAG:

1. Reads **process configuration** from Airflow Variables and audit tables
2. Validates unload and AppFlow configuration
3. Groups jobs by execution sequence
4. Unloads data from **Redshift → S3**
5. Optionally triggers **AWS AppFlow → Salesforce**
6. Logs execution status at every stage
7. Updates batch metadata
8. Sends completion notifications

---

## 🚀 How to Trigger the DAG
This DAG is scheduled to **trigger based** on completion of daily ingestion cycle.

Trigger manually via:
**Airflow UI → Trigger DAG**

No runtime config is required in the UI—execution is fully driven by **Airflow Variables and audit tables**.

---
"""
import re
import airflow
import logging
import json
import boto3
import pendulum
from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from itertools import groupby
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.operators.appflow import AppflowRunOperator
from airflow.operators.python import get_current_context

# =================================== #
# 🔧 ADMIN VARIABLE CONFIG FORMAT
# =================================== #
# Variable Name: redshift_to_salesforce_config
#
# {
#   "prcs_nme": "process_name",          # Required: Process name from <audit_schema>.<prcs_table>
#   "table_name": ""                     # Optional: Specific table name to unload(e.g., "table1, table2")
#	"batch_dt": "2025-07-15",            # Optional: Batch date to log execution (defaults to sysdate if not given)
#	"run_seq_start": 0,                  # Optional: Sequence number to start from in prcs_job
#	"run_seq_end": 5                     # Optional: Sequence number to end at
# }
#
#
# The actual config for each table comes from '<audit_schema>.<prcs_job_table>.additional_info' JSON:
# {
#	"table_name": "table1",		         			# ✅ Required: Table name to be unloaded
#	"src_schema_name": "<rawcurrent_schema>",       			# ✅ Required: Source schema name in Redshift
#   "unload_to_s3": "true",                             # Optional: To enable and disable unloads in case of failovers
#   "unload": {
#       "bucket": "my-bucket",                			# ✅ Required: S3 bucket name
#       "key": "folder/prefix/",              			# ✅ Required: S3 path prefix (slashes will be normalized)
#       "breakup_threshold": "4",                       # Optional: Control number of records to write to the file
#       "file_prefix": "export_",             			# Optional: Custom file prefix for output (default is table_name)
#       "file_extension": "csv",              			# Optional: File extension (should match data_format)
#       "data_format": "csv",      			  			# ✅ Required: Either 'csv' or 'parquet'
#       "delimiter": ",",          			  			# Optional: (CSV only): Delimited between fields (e.g, ',', '|')
#		"header": true,                       			# Optional: (CSV only): Whether to include column headers
#		"addquotes": true,                    			# Optional: (CSV only): Wrap each field in double quotes
#       "parallel": "OFF",         			  			# Optional: 'ON' (default) or 'OFF' - OFF generates a single file
#       "iam_role_arn": arn:aws:iam::xxx:role/my-role,  # ✅ Required: Redshift unload role
#		"compression": "gzip",                          # Optional: Compression format ('gzip' for CSV, 'snappy' for Parquet)
#		"manifest": false                               # Optional: Whether to generate manifest file
#   },
#   "query": "SELECT * FROM <rawcurrent_schema>.table1"     # ✅ Required: Query to run for unloading
#   "custom_headers": ["Col1", "Col2", "Col3"]          # Optional: Override column headers in csv
# },
#    "trigger_appflow": "true",                         # Optional: To enable and disable Appflow trigger in case of failovers
#    "appflow": {
#        "flow_name": "",                               # ✅ Required: Name of the related Appflow
#        "description": "run appflow wx"                # Optional: Appflow description
#    }
# }

# Set up static variables for DAG
local_tz=pendulum.timezone("America/Chicago")
curr_env=Variable.get('environment')
get_batch_run=Variable.get('get_batch_run', default_var='get_batch_run')
email_content=Variable.get('success_email_html_template')
email_subject=Variable.get('success_email_subject')
alert_email_list=['ops@email.com']
schedule_interval=None

if curr_env == 'tst' or curr_env == 'mdl':
    # schedule_interval = '0 13 * * *'
    alert_email_list=['ops@email.com']

elif curr_env == 'prd':
    alert_email_list=['ops@email.com']

DEFAULT_ARGS = {
    'owner': 'owner_name',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': days_ago(1),
    'email': alert_email_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='redshift_to_salesforce_reports',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['Redshift', 'Salesforce', 'Reports']
)
def redshift_to_salesforce_reports():

    @task()
    def get_config():
        config = json.loads(Variable.get("redshift_to_salesforce_config"))
        prcs_nme = config["prcs_nme"]
        table_filter = [tbl.strip().lower() for tbl in config.get("table_name", "").split(",") if tbl.strip()] or []

        pg_hook = PostgresHook(postgres_conn_id='redshift-warehouse')
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        # Get batch date from batch_date_table
        cur.execute("SELECT batch_date, addi_info FROM <audit_schema>.<batch_date_table>")
        batch_dt = None
        for row in cur.fetchall():
            addi_info = json.loads(row[1])
            if addi_info.get("layer") == prcs_nme:
                batch_dt = row[0]
                break
        if not batch_dt:
            raise Exception(f"No matching layer found in config_data for {prcs_nme}")

        # Get prcs_id from <audit_schema>.<prcs_table>
        cur.execute("SELECT prcs_id FROM <audit_schema>.<prcs_table> WHERE prcs_nme = %s", (prcs_nme,))
        result = cur.fetchone()
        if not result:
            raise Exception(f"prcs_id not found for {prcs_nme}")
        prcs_id = result[0]
        logging.info(f"Executing job query with prcs_id={prcs_id}, batch_dt={batch_dt}")
        # Use the get_batch_run_p SP to get job metadata??
        cur.execute( f" SELECT pj.<prcs_job_table>_id,pj.<prcs_table>_tbl_nme,pj.additional_info,pj.<prcs_job_table>_exec_seq_no,pj.data_flow_no FROM <audit_schema>.<prcs_job_table> pj WHERE pj.is_active = 'Y' AND pj.<prcs_table>_id = {prcs_id} AND NOT EXISTS (SELECT 1 FROM <audit_schema>.<redshift_to_salesforce_pjel> pjel WHERE pj.<prcs_job_table>_id  = pjel.<prcs_job_table>_id AND pjel.status = 'END' AND pjel.additional_info LIKE '%Completed%' AND batch_dt = '{batch_dt}')")
        jobs = cur.fetchall()
        logging.info(f"Number of jobs fetch: {len(jobs)}")
        for i, row in enumerate(jobs):
            logging.info(f"Row {i} length: {len(row)} | Content: {row}")

        filtered_jobs = []
        for row in jobs:
            if len(row) < 5:
                logging.warning(f"Skipping row with insufficient columns: {row}")
                continue
            try:
                if not table_filter or (len(row) > 1 and row[1].strip().lower() in table_filter):
                    filtered_jobs.append({
                        "prcs_job_id": row[0],
                        "prcs_tbl_nme": row[1],
                        "additional_info": row[2],
                        "exec_seq": row[3],
                        "data_flow_no": row[4],
                        "excn_log_id": int(batch_dt.strftime('%Y%m%d%H%M%S')),
                        "prcs_id": prcs_id,
                        "batch_dt": batch_dt,
                        "prcs_nme": prcs_nme
                    })
            except IndexError as e:
                logging.error(f"IndexError while processing row: {row} | Error: {e}")

        cur.close()
        conn.close()
        return filtered_jobs

    @task()
    def validate_config(job_list):
        logging.info("\n====== Config Validation Summary ======")
        for job in job_list:
            try:
                info = json.loads(job["additional_info"])
                unload_cfg = info.get("unload", {})
                tbl_name = info.get("table_name", "UNKNOWN")
                prcs_job_id = job.get("prcs_job_id", "N/A")
                exec_seq = job.get("exec_seq", "N/A")

                logging.info(f"\n Validating Job ID:  {prcs_job_id} | Table: {tbl_name} | Group: {exec_seq}")

                required_keys = ["bucket", "key", "data_format", "iam_role_arn", "query"]
                for k in required_keys:
                    if k not in unload_cfg:
                        raise ValueError(f"Missing required unload key: {k} for table {info.get('table_name')}")

                fmt = unload_cfg.get("data_format", "csv")
                if fmt == "csv":
                    pass
                elif fmt == "parquet":
                    for forbidden in ["delimiter", "addquotes", "header"]:
                        if unload_cfg.get(forbidden) is not None:
                            logging.warning(f"'{forbidden}' is ignored for Parquet format for table {info.get('table_name')}")
                else:
                    raise ValueError(f"Invalid data_format: {fmt} for table {info.get('table_name')}")
            except Exception as e:
                raise AirflowException(f"Validation failed for job {job['prcs_job_id']}: {e}")
        logging.info("==========================================================================\n")

    @task()
    def log_batch_start(job_list):
        if not job_list:
            return
        row = job_list[0]
        pg_hook = PostgresHook(postgres_conn_id='redshift-warehouse')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO <audit_schema>.batch_run (prcs_nme, prcs_id, status, batch_dt, start_dttm)
            VALUES (%s, %s, %s, %s, getdate())
        """, (row['prcs_nme'], row['prcs_id'], 'Running', row['batch_dt']))

    @task()
    def group_jobs(job_list):
        job_list.sort(key=lambda x: (x["exec_seq"], x.get("data_flow_no", 0)))
        groups = []
        logging.info("Total active jobs received for grouping: %d", len(job_list))
        logging.info(f"\n================= Grouping Jobs by prcs_job_seq_no and data_flow_no =================")

        for key, group in groupby(job_list, lambda x: x["exec_seq"]):
            group_list = list(group)
            logging.info(f"\n▶️️️ Group: prcs_job_seq_no = {key}")
            for job in group_list:
                prcs_job_id = job["prcs_job_id"]
                data_flow_no = job["data_flow_no"]
                try:
                    info = json.loads(job["additional_info"])
                    tbl_name = info.get("table_name", "<unknown>")
                except Exception:
                    tbl_name = "<parser_error"
                logging.info(f"  - PRCS_JOB_ID {prcs_job_id} | Data Flow No {data_flow_no} | Table: {tbl_name}")
            groups.append(group_list)
        logging.info("✅ Finished grouping jobs.\n")
        return groups

    @task()
    def log_job_group(job_group):
        tables = []
        for job in job_group:
            job_id = job['prcs_job_id']
            tbl_name = json.loads(job['additional_info']).get('table_name', 'UNKNOWN')
            tables.append(f"{tbl_name} (JOB ID: {job_id}")
        logging.info("\n====== Executing Mapped Task Group ======")
        logging.info("Mapped Task includes the following jobs:")
        for tbl in tables:
            logging.info(f" - {tbl_name}")

    @task()
    def execute_job_group(job_group):
        pg_hook = PostgresHook(postgres_conn_id='redshift-warehouse')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cur = conn.cursor()

        s3_client = boto3.client('s3')

        logging.info(f"\n♦️ Starting execution of job group with {len(job_group)} jobs(s)")
        ###excn_log_seq_no = datetime.now().strftime('%Y%m%d%H%M%S')   # Generate a timestamp

        for job in job_group:
            excn_log_id = datetime.now().strftime('%Y%m%d%H%M%S')  # Unique per job
            conn.notices.clear()                                   # Clear Redshift notices before each job
            job_id = job["prcs_job_id"]
            info = json.loads(job["additional_info"])
            tbl = info.get("table_name")
            schema = info.get("src_schema_name", "unknown")
            full_tbl = f"{schema}.{tbl}"
            batch_dt = job["batch_dt"]
            ###excn_log_id  = excn_log_seq_no
            exec_seq = job.get("exec_seq", "N/A")
            data_flow_no = job.get("data_flow_no", "N/A")
            logging.info(f"EXCN LOG ID: {excn_log_id}")

            logging.info(f"\n▶️ Exec Seq: {exec_seq}) | PRCS JOB ID: {job_id} | Data Flow No: {data_flow_no} | Table: {full_tbl}")

            def log(status_val, msg):
                logging.info(f"🔷 Log Status Val: {status_val} | Msg: {msg}")
                cur.execute("""
                    INSERT INTO <audit_schema>.<redshift_to_salesforce_pjel>
                    (prcs_job_excn_log_id, prcs_job_id, status, batch_dt, additional_info)
                    VALUES (%s, %s, %s, %s, %s)
                """, (excn_log_id, job_id, status_val, batch_dt, json.dumps({"tbl": full_tbl, "msg": msg})))

            try:
                log("START", "Process Initiated")

                if info.get("unload_to_s3", "false").lower() == "true":
                    logging.info(f"✅ unload_to_s3 is set to true for table : {full_tbl}")
                    unload_cfg = info.get("unload", {})
                    if isinstance(unload_cfg, str):
                        unload_cfg = json.loads(unload_cfg)

                    bucket = unload_cfg.get("bucket")
                    raw_prefix = unload_cfg.get("key")
                    prefix = re.sub(r'//+', '/', raw_prefix.strip('/')) + '/' if raw_prefix else ''
                    logging.info(f"🧹 Checking S3 bucket: {bucket}")
                    logging.info(f"🧹 Using prefix: {prefix}")

                    if bucket and prefix:
                        list_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                        contents = list_response.get('Contents', [])

                        logging.info(f"🔍 Found {len(contents)} file(s) under s3://{bucket}/{prefix}")
                        if not contents:
                            logging.info(f"👎 No files found to delete under: s3://{bucket}/{prefix}")
                        else:
                            to_delete = [obj['Key'] for obj in contents if 'Key' in obj]
                            logging.info(f"🗃️ Files to delete: {to_delete}")
                            for key in to_delete:
                                logging.info(f"🗑️ Deleting file(s): {key}")
                                s3_client.delete_object(Bucket=bucket, Key=key)

                    logging.info(f"📦 Calling unload_to_s3_sp for prcs_job_id: {job_id}")
                    sp_schema = info.get("src_schema_name", "public")
                    cur.execute(f"CALL {sp_schema}.unload_to_s3_sp({job_id})")
                    logging.info(f"📦 CALL {sp_schema}.unload_to_s3_sp({job_id}) is being processed for prcs_job_id: {job_id}")

                    # Fetch row count from Redshift notice
                    notices = conn.notices
                    logging.info(f"📝 Redshift Notices: {notices}")

                    # Newest notice
                    row_count = None
                    for note in notices[::-1]:
                        # logging.info(f"📝 Redshift Notice: {note.strip()}")
                        match = re.search(r"Unload Completed: (\d+) rows", note)
                        logging.info(f"Match: {match}")
                        if match:
                            row_count = int(match.group(1))
                            break

                    row_count_str = str(row_count) if row_count is not None else "0"
                    log("UNLOAD_TO_S3", f"Unload Completed. Row Count: {row_count_str}")
                else:
                    logging.info(f"⚠️ unload_to_s3 is false - skipping unload for table: {full_tbl}")
                    log("UNLOAD_SKIPPED", "unload_to_s3 is false")
                    logging.info(f"🔕 Unload not executed for table: {full_tbl}")

                if info.get("trigger_appflow", "false").lower() == "true":
                    logging.info(f"trigger_appflow is true")
                    flow = info.get("appflow", {}).get("flow_name")
                    logging.info(f"Appflow Name: {flow} ")
                    if not flow:
                        raise Exception("Missing flow_name in appflow config")
                    logging.info(f"Triggering Appflow '{flow}' for table: {full_tbl}")
                    context = get_current_context()
                    trigger_task = AppflowRunOperator(
                        task_id=f"trigger_appflow_{flow}",
                        flow_name=flow,
                        wait_for_completion=True,
                        poll_interval=120
                    )
                    trigger_task.execute(context=context)
                    #client = boto3.client("appflow")
                    #client.start_flow(flowName=flow)
                    log("TRIGGER_APPFLOW", f"Appflow '{flow}' triggered")
                else:
                    logging.info(f"⚠️ trigger_appflow is false - skipping Appflow for table: {full_tbl}")
                    log("APPLFOW_SKIPPED", "trigger_appflow is false")
                    logging.info(f"🔕 Appflow not triggered for table: {full_tbl}")

                log("END", "Process Completed")

            except Exception as e:
                log("FAILED", str(e))
                logging.error(f"❌ Execution failed for job_id: {job_id} - {str(e)}")
                exit(1)

        cur.close()
        conn.close()

    @task()
    def update_batch_date(job_list):
        if not job_list:
            return
        prcs_nme = job_list[0]["prcs_nme"]
        pg_hook = PostgresHook(postgres_conn_id='redshift-warehouse')
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE <audit_schema>.<batch_date_table>
            SET batch_date = batch_date + 1
            WHERE json_extract_path_text(addi_info,'layer') = %s
        """, (prcs_nme,))
        cur.execute("""
            UPDATE <audit_schema>.batch_run
            SET status = 'Completed', end_dttm = getdate()
            WHERE prcs_nme = %s AND status = 'Running'
        """, (prcs_nme,))
        conn.commit()
        cur.close()
        conn.close()

    send_email = EmailOperator(
        mime_charset='utf-8',
        task_id='send_email',
        to=alert_email_list,
        subject=email_subject,
        html_content=email_content
    )

    config = get_config()
    validate_config(config)
    log_start = log_batch_start(config)
    grouped = group_jobs(config)
    log_job_group.expand(job_group=grouped) >> execute_job_group.expand(job_group=grouped) >> update_batch_date(config) >> send_email

redshift_to_salesforce_reports_dag = redshift_to_salesforce_reports()