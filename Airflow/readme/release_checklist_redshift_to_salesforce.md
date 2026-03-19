# ✅ Release Checklist – Redshift to Salesforce Reports DAG

## Pre‑Merge
- [ ] DAG imports without errors
- [ ] Pytest suite passes
- [ ] README updated
- [ ] Draw.io diagram updated

## Configuration Validation
- [ ] Airflow Variable `redshift_to_salesforce_config` validated
- [ ] process table job configs reviewed
- [ ] IAM role permissions verified

## Non‑Prod Validation
- [ ] DAG run in TST/QA
- [ ] S3 unload verified
- [ ] Row counts validated
- [ ] AppFlow trigger tested (if enabled)
- [ ] process tables updated correctly

## Prod Readiness
- [ ] Backfill / re‑run plan approved
- [ ] Rollback plan documented
- [ ] Ops notified

## Post‑Deployment
- [ ] First prod run monitored
- [ ] Salesforce reports validated
- [ ] Batch date increment verified
