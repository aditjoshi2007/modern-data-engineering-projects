
# 📊 Redshift to Postgres Reporting Pipeline

> **A production‑grade Spark pipeline that publishes analytical data from Amazon Redshift to a Postgres reporting database**, supporting both **Full Load** and **CDC (Change Data Capture)** patterns with S3‑backed state management and automated cleanup.

---

## 🌟 Project Overview

This module demonstrates a **real‑world reporting pipeline** commonly used in enterprise data platforms, where:

- Amazon Redshift acts as the analytical source
- Amazon S3 is used for CDC state persistence
- Postgres serves as the downstream reporting / serving layer
- Spark handles scalable data movement and transformation

The pipeline is **config‑driven**, secure, and operationally robust.

---

## 🧭 High‑Level Architecture

```
Amazon Redshift
        │
        ▼
Apache Spark (EMR / Spark Runtime)
        │
        ├── Full Load
        ├── CDC (Delta Detection)
        │
        ▼
Amazon S3 (Curated / CDC State)
        │
        ▼
Postgres Reporting Database
        │
        ▼
BI / Dashboards / Downstream Apps
```

---

## 🧱 Key Files

```
├── publish_redshift_to_postgres_reports.py
│   # Spark job to publish Redshift data into Postgres
│
├── report_details.json
│   # Metadata configuration for source, target, and CDC behavior
```

---

## 🚀 Key Features

✅ Full load and CDC execution modes  
✅ Spark‑based Redshift ingestion (community connector)  
✅ Secure credential management via AWS Secrets Manager  
✅ Delta detection using S3‑persisted snapshots  
✅ Postgres staging + stored procedure upserts  
✅ Automated delete handling  
✅ S3 lifecycle cleanup (keeps last 2 partitions)  
✅ Production‑grade logging and error handling  

---

## ⚙️ Configuration‑Driven Design (`report_details.json`)

All runtime behavior is controlled via JSON configuration:

- Redshift source table
- Postgres main & staging tables
- Primary keys for CDC
- Column‑level handling (string cleanup, selects)
- S3 prefixes for CDC persistence
- Stored procedure name for upserts

This enables **new report onboarding without code changes**.

---

## 🔄 Processing Modes

### 🔹 Full Load

1. Read full dataset from Redshift
2. Clean UTF‑8 null characters
3. Write directly to Postgres main table
4. Persist snapshot to S3
5. Cleanup old S3 partitions

Used for:
- Initial backfills
- Table resets

---

### 🔹 CDC Load

1. Read latest Redshift snapshot
2. Persist current snapshot to S3
3. Load previous snapshot from S3
4. Identify deltas via DataFrame subtraction
5. Write deltas to Postgres staging table
6. Delete missing records from main table
7. Execute Postgres stored procedure for upsert
8. Cleanup S3 state

Used for:
- Daily incremental refresh
- Low‑latency reporting updates

---

## 🗂️ Data Flow & State Management

| Layer | Purpose |
|------|--------|
| Redshift | Analytical source of truth |
| S3 | CDC state & historical snapshots |
| Postgres Staging | Incremental delta writes |
| Postgres Main | Serving/reporting layer |

---

## 🔐 Security Best Practices

- No hard‑coded credentials
- Secrets retrieved via AWS Secrets Manager
- IAM‑based Redshift access
- Controlled JDBC parallelism

---

## ▶️ How to Run

```bash
spark-submit publish_redshift_to_postgres_reports.py   --config_bucket <config-bucket>   --config_key <path/to/report_details>   --load_type fullload | cdc
```

---

## 💰 Operational Optimizations

- Repartitioned JDBC writes to limit DB connections
- Incremental CDC instead of full reloads
- Automatic S3 cleanup to control storage costs
- Spark persistence only where required

---

## 🧠 What This Demonstrates

✅ Real enterprise reporting pipelines  
✅ Redshift → Postgres data publishing  
✅ CDC without native log‑based replication  
✅ Spark performance tuning & JDBC patterns  
✅ Strong operational and data hygiene practices

---

## 🧱 Tech Stack

AWS | Spark | Redshift | Postgres | S3

---

## 🚀 Future Enhancements

- Glue Data Catalog integration
- Delta Lake CDC state
- Airflow orchestration
- Data quality framework
- Schema drift detection

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.