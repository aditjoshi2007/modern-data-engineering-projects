# WP Initial Load – Rawcurrent Airflow DAG

## 📌 Overview
**Initial Load – Rawcurrent** is a manually triggered Apache Airflow DAG that performs a **controlled full refresh and CDC (Change Data Capture) load** of selected tables into the **`rawcurrent` schema** in Amazon Redshift.

The DAG is designed for **initial loads, reprocessing, and schema/table‑level reloads** by:
- Reading landing data from **Amazon S3**
- Generating **Redshift-compatible manifest files**
- Loading data into **staging**
- Refreshing **rawcurrent**
- Updating **audit & batch metadata**
- Sending **success notifications**

---

## 🧱 Architecture at a Glance

```
S3 Landing
   │
   ├── Full Load Files
   ├── CDC Files
   │
   ▼
Manifest Generation (S3)
   │
   ▼
Staging Schema (Redshift)
   │
   ▼
Rawcurrent Schema (Redshift)
   │
   ▼
Audit Tables + Batch Date Update
   │
   ▼
Email Notification
```

---

## 🚀 How to Trigger the DAG

This DAG **does not run on a schedule**.  
It must be triggered manually via **Airflow UI → Trigger DAG w/ Config**.

---

## ✅ Trigger Configuration

### Required JSON Payload
```json
{
  "table_list": [
    { "table_name": "table1", "schema_name": "all" },
    { "table_name": "table2", "schema_name": "schema1,schema2" },
    { "table_name": "table3", "schema_name": "schema3" }
  ]
}
```

---

## 🔍 Reload Modes Supported

| Scenario | Configuration |
|--------|---------------|
| Reload table across all schemas | `"schema_name": "all"` |
| Reload single schema | `"schema_name": "schema1"` |
| Reload multiple schemas | `"schema_name": "schema1,schema2"` |

---

## 🗂️ Source System Support

| Source System | File Type | Manifest Type |
|--------------|-----------|---------------|
| AWS DMS | Parquet | Content-length aware manifest |
| Attunity | JSON / JSON.GZ | Standard Redshift JSON manifest |

---

## 🔄 DAG Task Flow

```
get_table_list
      ↓
get_sp_list
      ↓
Batch Grouping
      ↓
trigger_sp_batch
      ↓
set_config_date
      ↓
send_email
```

---

## ⚙️ Required Airflow Variables

- `environment`
- `account_no`
- `vacuum`
- `adhoc_run`
- `success_email_subject`
- `success_email_html_template`

---

## 🔐 Connections Required

- `redshift-connection`

---

## ⚠️ Important Notes

- Manual execution only (`schedule_interval = None`)
- `max_active_runs = 1`
- DAG fails on missing S3 data or stored procedure errors
- Optional VACUUM runs when enabled

---

## 📞 Support
Data Engineering – Operations Team

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.