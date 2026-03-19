
# EMR Batch Load DAG (Airflow)

A production‑ready Airflow DAG that creates an **EMR** cluster, runs **Spark** ingestion steps, and
monitors cluster & step completion with robust retry and clear failure semantics.

## Features
- Parameterized via **Airflow Variables** (no secrets in code)
- Testable helper functions (`poll_*`, `build_*`) decoupled from Airflow operators
- Clean logging & explicit terminal state handling
- Works well for GitHub portfolios (safe placeholders, no creds)

## Project Layout
```
.
├── dags/
│   └── create_emr_dag_polished.py
└── tests/
    ├── test_dag_loads.py
    └── test_emr_helpers.py
```

## Airflow Variables (examples)
Set these in the Airflow UI or CLI. Values below are safe templates; replace with your own.

```json
// jobs_list: map of job_name -> list of table/config tokens
{
  "ibm_db2_source_data": ["ibm_db2_source_data_tbls_config"],
  "sql_source_data_part2": ["sql_source_data_tbls_config"]
}
```

```json
// JOB_FLOW_OVERRIDES: base EMR cluster configuration (trimmed example)
{
  "ReleaseLabel": "emr-6.10.0",
  "Applications": [{"Name":"Hadoop"},{"Name":"Hive"},{"Name":"Spark"}],
  "Instances": {
    "Ec2SubnetId": "subnet-<id>",
    "KeepJobFlowAliveWhenNoSteps": false
  },
  "Steps": [],
  "VisibleToAllUsers": true,
  "Name": "dummy"
}
```

```json
// jobs_Ingestion_list: optional mapping job -> ingestion script
{
  "ibm_db2_source_data": "ibm_db2_source_data_tbls_script.py",
  "sql_source_data_part2": "sql_source_data_tbls_script.py"
}
```

Additional optional variables (with safe defaults):
- `spark_entrypoint_s3` (default `s3://<s3_bucket>/<s3_prefix>/entry.py`)
- `spark_pyfiles_s3` (default `s3://<s3_bucket>/<s3_prefix>/deps.zip`)
- `config_bucket` (default `<bucket_name>`)
- `config_key` (default `<s3_prefix_config_file>`)
- `load_type` (default `batch`)
- `args_list` (default `['spark-submit','--deploy-mode','cluster']`)
- `cluster_name` (default `BATCH-LOAD`)
- `aws_region` (default `<aws_region>`) 
- `success_email_html_template`, `success_email_subject`
- `alert_email_list` (JSON list of recipients)

## Running unit tests
Tests require Python 3.9+ and `pytest`.

- `test_dag_loads.py` validates DAG imports & topology (skips gracefully if Airflow is not installed).
- `test_emr_helpers.py` unit‑tests the helper polling functions with mocked EMR clients (no AWS needed).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install pytest
pytest -q
```

## Architecture

### EMR Batch Load DAG
diagrams/png/emr_batch_load_dag.png

## Notes
- Keep AWS creds & secrets out of code. Reference them via Airflow Connections/Variables.
- For production, consider adding `EmrJobFlowSensor`/`EmrStepSensor` or AWS waiters if preferred.
- The helper polling functions emulate robust behavior with explicit failure paths.

## 🔗 License

MIT License, attribution appreciated.

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.