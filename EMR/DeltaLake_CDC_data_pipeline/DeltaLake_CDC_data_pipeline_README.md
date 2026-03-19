
# 🏗️ Enterprise-Grade EMR Data Ingestion Framework

> **A production-scale, metadata‑driven data ingestion platform built on AWS EMR & PySpark**, designed to handle **Initial Loads, CDC, and Daily Full Loads** with full observability, auditability, and cost‑optimized execution.

---

## 🌟 Why This Project Matters

This project demonstrates **real-world data engineering at scale**, not toy examples.

It mirrors how ingestion platforms are built in **large enterprises**, including:

- Ephemeral EMR clusters
- Parallel Spark job execution
- CDC snapshot management
- Metadata‑driven ingestion
- Audit logging & job status tracking
- Automated failure handling & notifications

---

## 🧭 High-Level Architecture

```
Orchestrator (EC2 / CLI)
        │
        ▼
inflate_emr_ingestion.py
        │
        ▼
ingestion_single_table_v6.py
        │
        ▼
ingestion_baseclass.py
        │
        ▼
Amazon S3 (Landing → RAW → RAW_CURRENT)
```

---

## 🚀 Key Features

- Metadata‑driven ingestion (zero code per table)
- Supports Initial Load, CDC, and Daily Full Load
- Parallel table ingestion on EMR
- SHA‑hash–based change detection
- RAW & RAW_CURRENT snapshot management
- Built‑in data sanity checks
- Audit logging & job status tracking
- SNS notifications on failures
- Automatic EMR cluster termination

---

## 🧱 Repository Structure

```
EMR/
│
├── inflate_emr_ingestion.py
├── ingestion_single_table_v6.py
├── ingestion_baseclass.py
└── README.md
```

---

## 🧠 Component Overview

### EMR Orchestration
Handles EMR cluster creation, Spark step submission, monitoring, and termination.

### Table-Level Ingestion
Executes ingestion logic for a single table using metadata configuration.

### Core Ingestion Engine
Provides reusable Spark logic for schema enforcement, CDC merge, and S3 writes.

---

## 🗂️ Data Layers

| Layer | Description |
|------|-------------|
| Landing | Immutable source data |
| RAW | Full historical data |
| RAW_CURRENT | Latest snapshot per primary key |

---

## ✅ Data Quality & Audit

- Landing → RAW validation
- RAW → RAW_CURRENT validation
- Audit logs per execution
- Job status tracking (COMPLETED / FAILED / PARTIAL)

---

## ▶️ Example Run

```bash
python inflate_emr_ingestion.py   --env dev   --run_mode CDC   --acct 123456789   --emr_name ingestion   --cluster_size large   --config_file s3-bucket@path/config.json
```

---

## 🧱 Tech Stack

AWS | EMR | RDS | S3 | Glue

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.