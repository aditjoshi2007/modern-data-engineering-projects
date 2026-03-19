
# 📊 Reports Processing DAG (Airflow) — Redshift → S3 → API

Production-ready Airflow DAG for extracting report data from **Amazon Redshift**, staging in **Amazon S3**, and securely delivering files to a downstream **REST API**. This repo highlights strong engineering practices such as:

- Robust **S3 consistency handling** with a retryable waiter (no arbitrary sleeps)
- Clear **task boundaries** and **observability** (structured logging, validation)
- Secure secret handling (no secrets in code; integrate with Secrets Manager)
- Environment-driven configuration (via Airflow Variables)

---

## 🧱 Architecture

```text
Redshift
   ↓
UNLOAD CSV per report
   ↓
S3 RAW bucket (partitioned by report & date)
   ↓             (copied & normalized)
S3 TMP bucket  ————————————————→  AMA/Partner REST API
```

### Key Components
- **`get_csv_file`** — UNLOAD firm/report-specific CSVs from Redshift to S3 RAW.
- **`delete_csv_files`** — Clean TMP S3 prefix before each run.
- **`copy_csv_files`** — Copy *today's* RAW files to TMP and **wait** for consistency (no sleeps).
- **`get_auth_token`** — Acquire OAuth token (secret via AWS Secrets Manager).
- **`read_csv_and_upload`** — Validate file lists, extract metadata, and upload via REST API.

---

## 🚀 Why a Retryable S3 Waiter?

S3 listings and cross-region replications can be **eventually consistent**. Instead of guessing with `time.sleep()`, we use a **native `object_exists` waiter** that polls S3 until each expected object returns **HTTP 200**. This reduces flakiness and shrinks overall latency by not sleeping longer than needed.

The waiter is implemented in [`app_reports/s3_waiter.py`](app_reports/s3_waiter.py) and tested with `moto`.

---

## 📁 Repo Layout

```text
app_reports/
  ├── __init__.py
  └── s3_waiter.py            # Reusable, Airflow-agnostic S3 waiter
tests/
  └── test_s3_waiter.py       # Pytest unit tests with moto
README.md
```

> Your DAG file(s) can live under `dags/` per your Airflow deployment; import and use the `wait_for_s3_objects` helper in your task(s).

---

## 🔧 Setup (Local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install boto3 moto pytest
```

> Airflow is **not required** to run the tests; the helper provides a fallback `AirflowException` definition.

---

## 🧪 Running Tests

```bash
pytest -q
```

The test suite includes:

1. **Success case** — waiter passes when objects already exist
2. **Timeout case** — waiter raises when objects never appear

You can add an additional test to validate a **delayed appearance** (e.g., object is created by another process after a short delay) if desired.

---

## 🔒 Security & Secrets

- Do **not** hardcode credentials in code or variables.
- Use **AWS Secrets Manager** (or your org's vault) for auth secrets.
- Keep TLS verification `verify=True` in production for API calls.

---

## 🧰 Airflow Variables (Example)

These are typically set via the Airflow UI or CLI (values below are illustrative only):

```json
// Variable: reports_query
{
  "Report_A": "select * from schema.view_report_a",
  "Report_B": "select * from schema.view_report_b"
}
```

```json
// Variable: reports_api_info
{
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "uname_dev": "svc-dev@example.com",
  "uname_prd": "svc-prd@example.com",
  "grant_type": "password",
  "max_size_mb": 512,
  "token_api_dev": "https://auth.dev.example.com/oauth/token",
  "token_api_prd": "https://auth.prod.example.com/oauth/token",
  "report_api_dev": "https://api.dev.example.com/files",
  "report_api_prd": "https://api.prod.example.com/files",
  "params": { "Name": "PartnerName" }
}
```

---

## 🧩 Using the Waiter in Your DAG

```python
from app_reports.s3_waiter import wait_for_s3_objects

# inside your copy task after computing destination keys
wait_for_s3_objects(
    s3_client=s3_client,
    bucket=tmp_bucket,
    keys=expected_tmp_keys,
    timeout_seconds=300,
    delay_seconds=5,
)
```

---

## Architecture

### Reports Processing DAG
diagrams/png/reports_processing_dag.png

## 📝 Notes

- For us-east-1, you can create S3 buckets without a `CreateBucketConfiguration`. For other regions, supply a `LocationConstraint`.
- The waiter leverages S3 **HEAD** requests via boto3's built-in waiter and is robust across common eventual-consistency scenarios.

---

## 🔗 License

MIT License, attribution appreciated.

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.