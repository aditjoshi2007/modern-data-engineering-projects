# Redshift to Salesforce Reports Airflow DAG

## 📊 Overview
The **Redshift to Salesforce Reports** DAG is triggered after execution of daily ingestion cycle in Airflow. This pipeline exports reporting data from **Amazon Redshift** to **Salesforce**.  
It supports controlled batch execution, configurable unloads to **Amazon S3**, and optional **AWS AppFlow** triggers for Salesforce ingestion.

This DAG is designed for **process‑driven, repeatable, and failover‑safe reporting workflows**.

---

## 🧠 What This DAG Does
For each configured job, the DAG:

1. Reads execution configuration from **Airflow Variables** and **process tables**
2. Validates unload and AppFlow configuration
3. Groups jobs by execution sequence
4. Unloads data from **Redshift → S3**
5. Optionally triggers **AWS AppFlow → Salesforce**
6. Writes detailed process logs (START / END / FAILED)
7. Updates batch dates
8. Sends success notifications

---

## 🚀 How to Trigger the DAG
This DAG **does not run on a schedule**.

Trigger it manually from the **Airflow UI → Trigger DAG**.

> ✅ No runtime JSON is required in the UI.  
> Execution is fully driven by Airflow Variables and process metadata.

---

## ⚙️ Airflow Variable Configuration

### Variable Name
```
redshift_to_salesforce_config
```

### Example
```json
{
  "prcs_nme": "salesforce_reports",
  "table_name": "orders,customers",
  "batch_dt": "2025-07-15",
  "run_seq_start": 0,
  "run_seq_end": 5
}
```

### Example
```json
{
  "table_name": "orders",
  "src_schema_name": "rawcurrent",
  "unload_to_s3": "true",
  "unload": {
    "bucket": "reporting-bucket",
    "key": "salesforce/orders/",
    "data_format": "csv",
    "delimiter": ",",
    "header": true,
    "parallel": "OFF",
    "iam_role_arn": "arn:aws:iam::xxx:role/redshift-unload-role"
  },
  "query": "SELECT * FROM rawcurrent.orders",
  "trigger_appflow": "true",
  "appflow": {
    "flow_name": "orders_to_salesforce"
  }
}
```

---

## 🔄 DAG Task Flow

```text
get_config
    ↓
validate_config
    ↓
log_batch_start
    ↓
group_jobs
    ↓
[Mapped Execution]
 ├─ log_job_group
 └─ execute_job_group
    ↓
update_batch_date
    ↓
send_email
```

---

## 📦 Unload Capabilities

| Feature | Supported |
|------|-----------|
| CSV unload | ✅ |
| Parquet unload | ✅ |
| Header control | ✅ (CSV) |
| Compression | ✅ |
| Parallel OFF (single file) | ✅ |
| Automatic S3 cleanup | ✅ |

Row counts are extracted directly from **Redshift UNLOAD notices** and logged to process tables.

---

## 🔁 Salesforce Integration (Optional)
If enabled per job:
- Triggers **AWS AppFlow**
- Waits for completion
- Logs execution outcome

This step can be safely disabled during **failovers or reprocessing**.

---

## 🧾 process & Batch Tracking

The DAG writes detailed execution metadata

Tracked attributes include:
- Execution status
- Batch date
- Table name
- Row counts
- Error messages

---

## 📧 Notifications
- Success email is sent after full DAG completion
- Failure alerts are handled by Airflow defaults
- Email distribution is environment-aware

---

## ⚠️ Important Notes
- Manual execution only (`schedule_interval = None`)
- `max_active_runs = 1` ensures serialized execution
- Any job failure fails the DAG
- Unload and AppFlow steps are independently configurable

---

## 🏷️ DAG Metadata

| Attribute | Value                            |
|---------|----------------------------------|
| DAG ID | redshift_to_salesforce_reports   |
| Schedule | Airflow Dependency-Driven/Manual |
| Tags | Redshift, Salesforce, Reports    |
| Catchup | Disabled                         |

---

## 📞 Support
**Data Engineering – Analytics & Reporting Team**

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.
