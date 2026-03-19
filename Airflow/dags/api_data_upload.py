"""# 📊 Reports Processing DAG

## Overview
The ** Reports DAG** is responsible for extracting reports from **Amazon Redshift**, staging them in **Amazon S3**, and securely delivering them to the **API**.  
This DAG ensures accurate, specific report generation, validation, and delivery with robust logging and operational safeguards.

---

## 🗓 Schedule
- **Cron:** `* * * * *`
- **Frequency:** Monthly/Daily/Hourly
- **Max Active Runs:** 1

---

## 🌍 Environments Supported
- `dev`
- `tst`
- `mdl`
- `prd`

Environment‑specific configurations (S3 buckets, IAM roles, API endpoints, credentials) are dynamically resolved using **Airflow Variables** and **Secrets Manager**.

---

## 🧱 High‑Level Workflow

```text
Redshift
   ↓
Unload CSV per report
   ↓
S3 Bucket
   ↓
Temp S3 Bucket
   ↓
REST API
"""
# ──────────────────────────────────────────────────────────────────────────────
# Standard library imports
# ──────────────────────────────────────────────────────────────────────────────
import csv
import glob
import json
import logging
import os
import time
from collections import Counter
from datetime import timedelta, datetime, timezone
from typing import List, TYPE_CHECKING
import botocore.exceptions

# ──────────────────────────────────────────────────────────────────────────────
# Third‑party / Airflow & AWS imports
# ──────────────────────────────────────────────────────────────────────────────
import airflow
import boto3
import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from dateutil.relativedelta import relativedelta

from get_secrets import get_secret  # helper that fetches secrets (e.g., from AWS Secrets Manager)

if TYPE_CHECKING:
    # Imported only for type hints during development, not at runtime
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

# ──────────────────────────────────────────────────────────────────────────────
# Global/static configuration resolved from Airflow variables & constants
# NOTE: All placeholders like <s3_bucket_name> should be replaced per environment.
# ──────────────────────────────────────────────────────────────────────────────

# Local timezone for display/logging (avoid mixing tz-naive & tz-aware datetimes)
local_tz = pendulum.timezone("America/Chicago")

# Airflow Variable that drives environment‑specific config (dev/tst/mdl/prd)
curr_env = Variable.get('environment')

# For monthly reports (e.g., "08_2025" for previous month). Adjust if your cadence changes.
curr_mm_yyyy = datetime.strftime(datetime.today() - relativedelta(months=1), '%m_%Y')

# Account number or AWS account alias—replace placeholder and ensure it maps per env if needed.
acct = '<account_no>'

# Mapping of report_name -> SQL query template stored in Airflow Variables
reports_query = Variable.get('reports_query')
query_dict = json.loads(reports_query)

# API config (token endpoints, client creds, file upload endpoints, params, etc.)
reports_api = Variable.get('reports_api_info')
reports_api_dict = json.loads(reports_api)

# S3 temp (staging) bucket and prefix, used because Airflow workers may not access RAW directly
tmp_bucket = '<s3_bucket_name>'
tmp_key = '<s3_bucket_prefix>'  # e.g., 'dag/reports/tmp/'

# S3 RAW bucket/prefix where Redshift UNLOAD writes the per‑report CSVs
raw_bucket = '<s3_bucket_name>'
raw_key = '<s3_bucket_prefix>'  # e.g., 'domain/reports/report_name/report_name_Report_dd_yyyy'

# Local directory where workers can access/mount CSVs, must be aligned with your deployment
directory_path = '<local_dir_path>'

# IAM role used by Redshift to UNLOAD to S3
iam_role = '<iam_role_arn>'

# DAG display name
dag_name = 'app_reports'

# ──────────────────────────────────────────────────────────────────────────────
# Alerting / Email routing
# Adjust email recipients based on environment to reduce noise in non‑prod
# ──────────────────────────────────────────────────────────────────────────────
if curr_env == 'dev' or curr_env == 'tst' or curr_env == 'mdl':
    # schedule_interval = '0 13 * * *'
    alert_email_list = ['']  # Add relevant non‑prod distro lists here
elif curr_env == 'prd':
    alert_email_list = ['']  # Add prod distro (e.g., ops/on‑call/partner support)

# ──────────────────────────────────────────────────────────────────────────────
# Airflow default args: retries, ownership, email policy, etc.
# start_date is in UTC to avoid DST issues
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'owner_name',
    'depends_on_past': False,         # Do not wait on previous dag run's task state
    'wait_for_downstream': False,     # Do not wait for downstream DAGs
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'email': alert_email_list,
    'email_on_failure': True,         # Notify on task failures
    'email_on_retry': False,          # Optional: avoid noise
    'retries': 0,                     # Change to >0 for transient failures in extract/stage
    'retry_delay': timedelta(minutes=1)
}

# ──────────────────────────────────────────────────────────────────────────────
# Task: Helper funtion for a retryable S3 consistency waiter
# - Wait until all given S3 objects exist (HEAD 200) or timeout is reached
# ──────────────────────────────────────────────────────────────────────────────
def wait_for_s3_objects(
    s3_client,
    bucket: str,
    keys: List[str],
    timeout_seconds: int = 300,
    delay_seconds: int = 5,
) -> None:
    """
    Uses the native boto3 S3 waiter 'object_exists' per key. This provides robust,
    retryable consistency handling vs. arbitrary sleeps, especially useful after
    cross-account copies, CRR, or when listings may lag.

    Args:
        s3_client: boto3 S3 client
        bucket (str): Destination S3 bucket
        keys (List[str]): Object keys to verify
        timeout_seconds (int): Total time to wait for each object
        delay_seconds (int): Delay between waiter polls

    Raises:
        AirflowException: If any object fails to appear within the timeout window
    """
    if not keys:
        logging.info("wait_for_s3_objects: no keys provided; nothing to wait for.")
        return

    # Compute waiter attempts based on desired timeout/backoff
    max_attempts = max(1, (timeout_seconds + delay_seconds - 1) // delay_seconds)
    waiter = s3_client.get_waiter('object_exists')

    logging.info(
        f"⏳ Waiting for {len(keys)} object(s) in s3://{bucket} "
        f"(Delay={delay_seconds}s, MaxAttempts={max_attempts})"
    )

    failures = []
    for key in keys:
        try:
            waiter.wait(
                Bucket=bucket,
                Key=key,
                WaiterConfig={"Delay": delay_seconds, "MaxAttempts": max_attempts},
            )
            logging.info(f"✅ Object is now available: s3://{bucket}/{key}")
        except botocore.exceptions.WaiterError as e:
            logging.error(f"❌ Timed out waiting for: s3://{bucket}/{key} — {e}")
            failures.append(key)

    if failures:
        raise AirflowException(
            f"One or more objects did not appear within the timeout: {failures}"
        )

# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# - description and doc_md pull from the module docstring above (renders in UI)
# - schedule here is every minute (*) — replace with production cron when ready
# - tags helpful for filtering in Airflow UI
# ──────────────────────────────────────────────────────────────────────────────
@dag(
    dag_name,
    description=__doc__.partition(".")[0],  # first sentence only for hover‑tooltip
    doc_md=__doc__,                         # full Markdown shows in UI
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *",
    max_active_runs=1,                      # avoid overlapping runs due to stateful S3 staging
    tags=['reports']
)
def reports():
    # ──────────────────────────────────────────────────────────────────────────
    # Task: get_csv_file
    # - Runs Redshift queries per report_name (from query_dict)
    # - Splits output by accountId and UNLOADs to S3 RAW
    # - Skips if the dataset is empty
    # - Returns list of accountIds discovered (for observability)
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def get_csv_file(report_name: str):
        """
        Queries Postgres and Unloads Report CSV files to S3 bucket.

        Args:
            report_name (str): The Report name to identify the view in redshift.

        Returns:
            list[str]: List of accountIds processed for this report_name.
        """

        report_ids = []
        # PostgresHook points to Redshift (configured via Airflow connection 'redshift-connection')
        postgres = PostgresHook(postgres_conn_id="redshift-connection")
        conn = postgres.get_conn()
        conn.autocommit = True  # Redshift UNLOAD requires autocommit enabled
        cur = conn.cursor()

        # Define S3 path for UNLOAD target.
        # raw_key template must contain 'report_name' and 'dd_yyyy' placeholders to be replaced.
        key = raw_key.replace('report_name', report_name).replace('dd_yyyy', curr_mm_yyyy)
        s3_path = f's3://{raw_bucket}/{key}'

        # Fetch SQL query for this report_name and configured max file size (MB) for Redshift UNLOAD
        query = query_dict[report_name]
        max_size = reports_api_dict["max_size_mb"]

        # Build a COUNT(*) version of the query to avoid scanning full dataset during unload if empty.
        rc_check_qry = query.lower().split('from')
        rc_check_qry[0] = 'select count(*) from'
        record_count_statement = ''.join(rc_check_qry)

        cur.execute(record_count_statement)
        record_count_result = cur.fetchall()
        rc_result = [doc for doc in record_count_result]
        logging.info(f"🔷 Total records found for {report_name} : {str(rc_result[0][0])}")

        # Extract unique account ids so that each CSV is firm/report‑specific.
        list_report_ids_qry = query.lower().split('from')
        list_report_ids_qry[0] = 'select distinct accountid as report_ids from'
        report_ids_statement = ''.join(list_report_ids_qry)
        cur.execute(report_ids_statement)
        report_firm_ids_result = cur.fetchall()

        for i in report_firm_ids_result:
            if i[0] is not None:
                report_ids.append(i[0])

        # If data exists, UNLOAD per accountId (non‑parallel to control file naming)
        if rc_result[0][0] > 0:
            for report_id in report_ids:
                unload_qry = query + f" where accountid = '{report_id}'"
                # NOTE:
                # - PARALLEL off ensures single file per report_id (easier downstream handling)
                # - ALLOWOVERWRITE helps idempotency on re‑runs
                # - MAXFILESIZE keeps multipart splitting under control if needed
                unload_cmd = (
                    f"unload($${unload_qry}$$) "
                    f"to '{s3_path}_{report_id}_' "
                    f"IAM_ROLE '{iam_role}' "
                    f"csv header PARALLEL off ALLOWOVERWRITE "
                    f"MAXFILESIZE {max_size} MB;"
                )
                logging.info(f"📝 Unload command for {report_name} is {unload_cmd}")
                logging.info(f"📝 Executing unload command for {report_name}")
                cur.execute(unload_cmd)
        else:
            logging.info(f"👎 No data found for {report_name} in Redshift")

        # Always close DB connection
        conn.commit()
        conn.close()
        return report_ids

    # ──────────────────────────────────────────────────────────────────────────
    # Task: delete_csv_files
    # - Cleans temp S3 prefix before copying fresh files
    # - Prevents mixing stale artifacts with current run
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def delete_csv_files():
        """
        Deletes S3 objects from temp location which were created previously.

        Returns:
            None
        """
        s3_client = boto3.client('s3')
        objects_to_delete = []

        # Use paginator for safety with large prefixes
        paginator = s3_client.get_paginator('list_objects_v2')
        deletion_iterator = paginator.paginate(Bucket=tmp_bucket, Prefix=tmp_key)

        for page in deletion_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            logging.info(f"🔎 Found {len(objects_to_delete)} objects with prefix '{tmp_key}' to delete")
            # S3 DeleteObjects API supports up to 1000 per request
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                try:
                    response = s3_client.delete_objects(
                        Bucket=tmp_bucket,
                        Delete={
                            'Objects': batch,
                            'Quiet': False  # Set True to suppress per‑object results
                        }
                    )
                    logging.info(f"✅ Successfully deleted {len(batch)} objects")
                    if 'Errors' in response:
                        logging.info(f"❌ Errors encountered during deletion: {response['Errors']}")
                except Exception as e:
                    logging.error(f"❌ Error deleting objects: {e}")
        else:
            logging.info(f"👎 No objects found with prefix {tmp_key} in bucket {tmp_bucket}")

    # ──────────────────────────────────────────────────────────────────────────
    # Task: copy_csv_files
    # - Copies today's RAW S3 objects to the temp S3 prefix so workers/API can access them
    # - Normalizes file names: removes Redshift part suffixes, maintains sequence where needed
    # - Sleeps to allow S3 eventual consistency/replication to settle
    # ──────────────────────────────────────────────────────────────────────────
    @task
	def copy_csv_files(report_name: str):
		"""
		Copies S3 objects from RAW to TMP for files created today, normalizes names,
		and waits (retryably) for all copied objects to be available in TMP.
	
		Args:
			report_name (str): The report key used to locate RAW S3 objects.
	
		Returns:
			None
		"""
		s3_client = boto3.client('s3')
		today = datetime.now(timezone.utc).date()
	
		# Build a paging iterator once and collect today's RAW objects
		paginator = s3_client.get_paginator('list_objects_v2')
		raw_prefix = '/'.join(raw_key.split("/")[:3]).replace('report_name', report_name)
		logging.info(f"💡 Checking RAW S3: bucket={raw_bucket}, prefix={raw_prefix}")
	
		raw_objects_today = []
		for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
			if 'Contents' not in page:
				continue
			for obj in page['Contents']:
				if obj['LastModified'].date() == today:
					raw_objects_today.append(obj['Key'])
	
		if not raw_objects_today:
			logging.info(f"👎 No RAW objects found for {report_name} created today; nothing to copy.")
			return
	
		# Detect duplicates (multipart) by comparing base prefix (strip final _part)
		base_key_without_part = ['_'.join(k.split('_')[:-1]) for k in raw_objects_today]
		element_counts = Counter(base_key_without_part)
		duplicates = {base for base, count in element_counts.items() if count > 1}
	
		logging.info(f"🔷 Found {len(raw_objects_today)} RAW object(s) for {report_name} created today.")
		if duplicates:
			logging.info(f"ℹ️ Multipart groups detected: {len(duplicates)}")
	
		# Build mapping RAW -> TMP destination keys and collect the list we must wait on
		expected_tmp_keys = []
	
		for obj_key in raw_objects_today:
			# Normalize destination filename:
			#  - If multipart, preserve an integer suffix (strip leading zeros)
			#  - If single-part, strip the suffix entirely
			if '_'.join(obj_key.split('_')[:-1]) in duplicates:
				parts = obj_key.split('_')
				# E.g., *_000 → keep "0"; otherwise strip leading zeros
				part_suffix = parts[-1]
				if all(c == '0' for c in part_suffix):
					normalized_name = obj_key[:-3] + obj_key[-1] + '.csv'
				else:
					normalized_name = obj_key[:-3] + part_suffix.lstrip('0') + '.csv'
			else:
				normalized_name = '_'.join(obj_key.split('_')[:-1]) + '.csv'
	
			destination_key = os.path.join(tmp_key, os.path.basename(normalized_name))
			expected_tmp_keys.append(destination_key)
	
			copy_source = {'Bucket': raw_bucket, 'Key': obj_key}
			logging.info(
				f"📦 Copying RAW → TMP :: {copy_source['Bucket']}/{copy_source['Key']}  →  "
				f"{tmp_bucket}/{destination_key}"
			)
			s3_client.copy_object(
				CopySource=copy_source,
				Bucket=tmp_bucket,
				Key=destination_key
			)
	
		# ✅ Replace sleep() with a robust waiter that verifies object existence
		wait_for_s3_objects(
			s3_client=s3_client,
			bucket=tmp_bucket,
			keys=expected_tmp_keys,
			timeout_seconds=300,   # tune per environment/volume
			delay_seconds=5        # backoff between checks
		)
	
		logging.info(f"✅ All {len(expected_tmp_keys)} object(s) are present in TMP: s3://{tmp_bucket}/{tmp_key}")

    # ──────────────────────────────────────────────────────────────────────────
    # Task: get_auth_token
    # - Fetches OAuth token for downstream API uploads
    # - Credentials are sourced via Secrets Manager (through get_secret)
    # - Token is returned and used in XCom by downstream task
    # ──────────────────────────────────────────────────────────────────────────
    @task
    def get_auth_token():
        """
        Generate Auth Token for uploading the Report CSV files through API call.

        Returns:
            Dict: Auth token response from auth token API call.
        """
        try:
            # NOTE: conf file holds the secret name per env; the secret value is fetched via get_secret()
            with open(f"<path_to_config_file>", "r") as f:
                data = json.loads(f.read())
            logging.info(f"✅ Received service account credentials from config file")

            # 'env' must be resolvable in scope; if not, replace with curr_env
            secretname = '<secret_name>'
            api_url = '<api_url>'

            # OAuth client credentials & user credentials (as applicable)
            data = {
                "client_id": "<client_id>",
                "client_secret": "<client_secret>",
                "username": "<username>",
                "password": "<pwd>",  # do NOT log this
                "grant_type": "<type>",
            }

            logging.info(f"📞 Calling Auth Token API URL : {api_url} 🌍")
            # verify=False is used here; ideally install CA bundle and set verify=True in prod
            response = requests.post(api_url, data=data, verify=False)
            if response.status_code == 200:
                logging.info(f"📩 Successfully received API Authorization Token")
                return response.json()
            else:
                logging.info(f"❌ Failed to fetch auth token. Status code : {response.status_code}")
                logging.info(f"❌ Error : {response.text}")
        except Exception as e:
            # Raising ensures task failure is captured by Airflow and triggers retries/alerts
            logging.error(f"❌ Error occured during Auth Token API call : {e}")
            raise

    # ──────────────────────────────────────────────────────────────────────────
    # Task: read_csv_and_upload
    # - Validates that temp S3 files == local files (by name set) to detect partial copies
    # - Extracts required metadata from the CSV (name, id) for the API
    # - Uploads files with attributes to the target REST endpoint
    # - Retries handle transient API failures
    # ──────────────────────────────────────────────────────────────────────────
    @task(retries=5, retry_delay=pendulum.duration(seconds=60))
    def read_csv_and_upload(**kwargs):
        """
        Fetch Auth Token from get_auth_token task and upload the Report CSV files through API call.

        Returns:
            None
        """

        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run  # useful for correlating logs with run_id/execution_date

        s3_client = boto3.client('s3')
        s3_file_list = []
        file_list = []
        desired_extension = '.csv'

        # Enumerate temp S3 bucket contents to build expected file set
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=tmp_bucket, Prefix=tmp_key)

        logging.info(f"💡 Checking S3 bucket: {tmp_bucket}")
        logging.info(f"💡 Using prefix: {tmp_key}")

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Normalize to base filename without extension for comparison
                    s3_file_list.append(str(obj['Key'].split('/')[-1].replace(desired_extension, '')))

        # Discover local CSVs under directory_path (Airflow worker accessible path)
        full_path_pattern = directory_path + '*' + desired_extension
        matching_csv_files = glob.glob(full_path_pattern)

        for file_path in matching_csv_files:
            try:
                # We only compare names; sizes could be added if desired
                file_list.append(file_path.split('/')[-1].replace(desired_extension, ''))
            except FileNotFoundError:
                logging.info(f"⚠️ Warning: File '{file_path}' not found (might have been deleted). Skipping.")
            except PermissionError:
                logging.info(f"⚠️ Warning: Permission denied to access '{file_path}'. Skipping.")

        # Pull OAuth token from prior task (XCom)
        fetch_auth_token = ti.xcom_pull(task_ids='get_auth_token', key='return_value')
        auth_token = fetch_auth_token["access_token"]

        # Clean/unique sets for comparison
        f_list = list(filter(lambda x: x.strip(), file_list))
        files_list = list(set(f_list))
        s3_list = list(filter(lambda x: x.strip(), s3_file_list))

        logging.info(f"💡 Local dir file list: {str(file_list)}")
        logging.info(f"💡 S3 file list: {str(s3_list)}")

        # Strict set equality check: if mismatch, fail fast to avoid incorrect uploads
        if Counter(files_list) != Counter(s3_list):
            raise AirflowException(f"❌ File list is corrupted, please re-run the task")
        else:
            for file in files_list[:]:
                report_name = ''
                report_id = ''
                try:
                    # API static parameters template
                    params = reports_api_dict["params"]
                    # Replace '_' with '-' for API friendly report date format
                    report_dt = curr_mm_yyyy.replace('_', '-')
                    # Derive reportType from file naming convention (first three underscore‑delimited tokens)
                    reportType = ''.join(file.split('_')[0:3])
                    logging.info(f"🔔 Processing Report name: {file} and reportType: {reportType}")

                    file_name = f"{file}.csv"
                    full_path = directory_path + file_name

                    # Extract metadata from the CSV content
                    # Assumes:
                    #  - row[1] contains a display/name field
                    #  - last column contains accountId (adjust indexes if schema differs)
                    with open(full_path, 'r', newline='') as csvfile:
                        csv_reader = csv.reader(csvfile)
                        # next(csv_reader)  # uncomment if header exists and should be skipped
                        for idx, row in enumerate(csv_reader):
                            if idx > 0:
                                if row[1]:
                                    # Remove non‑alnum chars to keep API payload clean
                                    report_name = ''.join([char for char in row[1] if char.isalnum()])
                                    report_id = row[-1]
                                    break

                    logging.info(f"🔔 report Name: {report_name}")
                    logging.info(f"🔔 report ID: {report_id}")

                    # Compose multipart/form‑data for file upload
                    # The server expects:
                    #  - 'files' as the binary upload
                    #  - 'Name' (from params) and 'fileAttributes' JSON with required metadata
                    files = {
                        'files': open(full_path, 'rb'),
                        'Name': (None, params['Name']),
                        'fileAttributes': (
                            None,
                            '{"account_id":"acct_id","report_name":"report_name","report_type":"report_type","report_date":"report_date"}'
                            .replace("acct_id", report_id)
                            .replace('report_name', report_name)
                            .replace('report_type', reportType)
                            .replace('report_date', report_dt)
                        ),
                    }

                    logging.info(f"📩 Parameters received for API Call : {files['Name']}")
                    logging.info(f"📩 Parameters received for API Call : {files['fileAttributes'][1]}")

                    # Resolve API endpoint per environment
                    api_url = reports_api_dict[f"report_api_{env}"]
                    headers = {
                        "Authorization": f"Bearer {auth_token}"
                    }

                    logging.info(f"📞 Calling Report API URL : {api_url} 🌍")

                    # verify=False for now; strongly recommended to set verify=True with proper certs in prod
                    response = requests.post(api_url, headers=headers, files=files, verify=False)
                    if response.status_code == 200:
                        logging.info("🚀 Report CSV file sent successfully.")
                        print("API Response:", response.json())
                        files_list.remove(file)  # remove successfully processed file to track residuals
                    else:
                        logging.info(f"❌ Failed to send Report CSV file. Status code: {response.status_code}")
                        print("API Response:", response.json())
                        # Raise to trigger Airflow retry/failure handling
                        raise AirflowException(f"❌ Error occured during API call: {str(response.text)}")
                except Exception as e:
                    # Fail the task to ensure visibility and prevent partial ingestion
                    raise AirflowException(f"❌ Error occured during API call: {e}")

        logging.info(f"📢 File List after processing each Reports : {str(files_list)} ")

    # ──────────────────────────────────────────────────────────────────────────
    # Task graph (TaskFlow API)
    # - Map get_csv_file & copy_csv_files over all report names in query_dict
    # - Ensure cleanup → copy → auth → upload sequencing
    # - Using >> for explicit ordering
    # ──────────────────────────────────────────────────────────────────────────
    get_csv_file.partial().expand(report_name=list(query_dict.keys())) \
        >> delete_csv_files() \
        >> copy_csv_files.partial().expand(report_name=list(query_dict.keys())) \
        >> get_auth_token() \
        >> read_csv_and_upload()


# Instantiate the DAG object for Airflow to discover
dag = reports()