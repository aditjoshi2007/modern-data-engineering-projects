
# 📄 Outbound Financial Detail View (Portfolio Example)

## Overview
This project showcases a **fund-level outbound financial detail view** that
bridges internal plan and fund data with Salesforce financial summaries.

The view demonstrates advanced warehouse modeling patterns such as:
- Fund-level enrichment
- Asset-class-driven ordering
- Snapshot-aligned balances
- Cross-system Salesforce integration

---

## View Purpose
The view produces a **detailed, consumption-ready dataset** that:
- Exposes fund attributes and balances per contract
- Aligns balances to the latest available snapshot
- Enriches data with Salesforce contract and financial summary context
- Filters out inactive or irrelevant funds

---

## Design Highlights

### ✅ Dimensional Integrity
Only active plan and fund records are selected using:
- `is_current = 1`
- `valid_to IS NULL`

### ✅ Snapshot Consistency
All balances and dates are aligned to the **latest effective snapshot**
from plan-fund metrics, ensuring financial accuracy.

### ✅ Deterministic Ordering
A CASE-based sequence number provides consistent ordering of funds
based on asset class, supporting downstream UI and reporting needs.

### ✅ Salesforce Enrichment
The view integrates:
- Salesforce contract records
- Latest Salesforce financial summaries

This ensures outbound datasets are immediately usable by CRM systems.

### ✅ Business Filtering
Funds are included only if they:
- Have non-zero balances, OR
- Are active and explicitly displayable

---

## Files

```
├── vw_reporting_part4_mapping.sql   # Fully commented SQL view
├── vw_reporting_part4_mapping.drawio  # Architecture diagram
└── vw_reporting_part4_mapping_README.md                         # This document
```

---

## What This Demonstrates

✅ Advanced SQL modeling
✅ Financial domain expertise
✅ Snapshot-based analytics
✅ Salesforce system integration
✅ Production-grade data design

---

**Author:** Adit Joshi  
Senior Data Engineer
