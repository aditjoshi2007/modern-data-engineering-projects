
# ☁️ Salesforce Data Integration using AWS AppFlow

> **An enterprise‑grade Salesforce ingestion and publishing framework built on AWS AppFlow**, enabling **full‑load, reverse‑load, and controlled triggering** of Salesforce data flows with strong governance, security, and operational visibility.

---

## 🌟 Project Overview

This project demonstrates a **real‑world Salesforce data integration pattern** commonly used in large enterprises where:

- Salesforce is either the **source** or **destination** system
- AWS AppFlow is used for managed, scalable data movement
- Amazon S3 acts as the landing / staging layer
- CSV‑driven metadata enables dynamic field mapping
- Automation handles flow creation, updates, and execution

The framework eliminates manual AppFlow configuration and enables **repeatable, environment‑aware deployments**.

---

## 🧭 High‑Level Architecture

```
Salesforce
   ▲   │
   │   ▼
AWS AppFlow (On‑Demand)
   │
   ▼
Amazon S3 (Landing / Error Buckets)
   │
   ▼
Downstream Data Platform
```

Supports **bi‑directional flows**:
- Salesforce ➜ S3 (Full Load)
- Salesforce ➜ S3 (Delta Load)
- S3 ➜ Salesforce (Reverse Load)

---

## 🧱 Key Files

```
├── create_flow_fullload.py
│   # Creates & triggers Salesforce → S3 full‑load AppFlows
│
├── create_flow_fullload_reverse.py
│   # Creates & triggers S3 → Salesforce reverse AppFlows
│
├── trigger-appflow.py
│   # Triggers AppFlows, monitors execution, sends notifications
```

---

## 🚀 Key Capabilities

✅ Automated AppFlow creation & updates  
✅ Salesforce → S3 full‑load and delta-load ingestion  
✅ S3 → Salesforce reverse publishing  
✅ CSV‑driven field mapping (no hard‑coding)  
✅ KMS‑encrypted AppFlow executions  
✅ Environment‑aware deployments (dev/tst/mdl/prd)  
✅ Execution monitoring & retries  
✅ Rejected record detection  
✅ SNS‑based success & failure notifications  

---

## 🔄 Processing Patterns

### 🔹 Salesforce ➜ S3 (Full Load/Delta load)

Implemented via **`create_flow_(fullload/deltaload).py`**:

1. Reads table and field mappings from S3‑hosted CSV
2. Creates or updates AppFlow dynamically
3. Uses Salesforce connector with deleted record support
4. Writes output to S3 in structured prefixes
5. Triggers the flow immediately after creation

Used for:
- Initial Salesforce backfills
- Periodic full refreshes
- Compliance & archival use cases

---

### 🔹 S3 ➜ Salesforce (Reverse Load)

Implemented via **`create_flow_fullload_reverse.py`**:

1. Reads CSV files from S3
2. Maps source → destination fields dynamically
3. Uses Salesforce external IDs for upserts
4. Captures rejected records in error buckets
5. Supports INSERT / UPSERT operations

Used for:
- Data enrichment back‑publishing
- Master data synchronization
- Corrective updates to Salesforce

---

### 🔹 Flow Triggering & Monitoring

Implemented via **`trigger-appflow.py`**:

- Starts flows programmatically
- Polls execution status (InProgress / Success / Error)
- Detects rejected records in S3
- Sends SNS notifications for:
  - Success
  - Partial success (rejected records)
  - Failure

---

## ⚙️ Configuration‑Driven Design

All flows are controlled via **S3‑hosted JSON + CSV metadata**:

- Salesforce object names
- S3 bucket & prefixes
- External IDs
- Write operation types
- Field‑level mappings
- Error handling paths

👉 New Salesforce objects can be onboarded **without code changes**.

---

## 🔐 Security & Governance

- AWS KMS encryption for AppFlow
- IAM‑based access to Salesforce & S3
- No hard‑coded secrets
- Environment‑specific connector profiles
- Controlled error isolation via reject buckets

---

## ▶️ How to Run

### Create Salesforce → S3 Full Load Flow
```bash
python create_flow_fullload.py   --config_file <s3-bucket>@<config-path>   --environment dev
```

### Create S3 → Salesforce Reverse Flow
```bash
python create_flow_fullload_reverse.py   --config_file <s3-bucket>@<config-path>   --environment dev
```

### Trigger an Existing AppFlow
```bash
python trigger-appflow.py   --flow_name <flow-name>   --environment dev   --config_file <s3-bucket>@<config-path>
```

---

## 🧠 What This Project Demonstrates

✅ Real enterprise Salesforce integrations  
✅ Managed ingestion using AWS AppFlow  
✅ Metadata‑driven flow automation  
✅ Secure bi‑directional data movement  
✅ Operational excellence with monitoring & alerts

---

## 🧱 Tech Stack

AWS | AppFlow | Salesforce | S3 | SNS

---

## 🚀 Future Enhancements

- Step Functions orchestration
- Schema drift detection
- Centralized flow catalog & dashboard

## ⚠️ Disclaimer

- This project is a personal learning project and doest not contain any proprietary or confidential information from my employer.
- All code, architecture, and data models are independently created and simplified for demonstration purposes. 