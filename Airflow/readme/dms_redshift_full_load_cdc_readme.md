# AWS DMS → S3 → Redshift Full Load & CDC Pipeline

## Overview
This repository showcases a **production‑grade data ingestion pipeline** orchestrated with **Apache Airflow** and implemented in **Python**, leveraging **AWS DMS, S3, and Amazon Redshift**.

The project demonstrates:
- Full load and CDC ingestion patterns
- Manifest‑driven COPY operations into Redshift
- Robust orchestration using Airflow DAGs
- Modular, testable Python design suitable for enterprise data platforms

This code is shared to highlight real‑world data engineering practices.

---

## Architecture

**Flow**:
1. AWS DMS performs full load / CDC into S3
2. Airflow orchestrates reloads and monitors DMS tasks
3. Python loader script generates manifests dynamically
4. Redshift Data API executes COPY and merge logic

See `/diagrams/aws_dms_redshift_pipeline.drawio` for a visual representation.

---

## Repository Structure

```
.
├── dags/
│   └── dms_redshift_full_load.py
├── loaders/
│   └── full_load_cdc_staging_serial.py
├── tests/
│   ├── test_manifest_generation.py
│   └── test_copy_sql_generation.py
├── diagrams/
│   └── aws_dms_redshift_pipeline.png
└── dms_redshift_full_load_cdc_readme.md
```

---

## Key Highlights

✅ Production Airflow DAG patterns (TaskGroups, Sensors, dynamic mapping)
✅ AWS‑native Redshift Data API (no JDBC dependency)
✅ Manifest‑based scalable ingestion
✅ Supports DMS, AppFlow, Attunity sources
✅ Unit‑testable SQL and manifest generation logic

---

## How to Run Tests

```bash
pip install pytest
pytest tests/
```

---

## Disclaimer

This code has been **sanitized** to remove proprietary identifiers while preserving architectural and technical depth.
