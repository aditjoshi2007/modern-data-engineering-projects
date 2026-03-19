
# Trigger Redshift Stored Procedures (Airflow)

Production-grade Apache Airflow DAG for orchestrating **dependency-aware
Redshift stored procedure execution**.

## Architecture
- **Orchestration:** Apache Airflow
- **Compute:** Amazon Redshift
- **Storage:** Amazon S3 (run manifests)

## Features
- process-driven batch control
- Dependency checks between procedures
- Deadlock-safe retries
- SNS alerts + S3 run metadata

## Cost Model
- **Persistent:** Redshift
- **Ephemeral:** Airflow workers

## Testing
```bash
pytest -q
```
## Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes.