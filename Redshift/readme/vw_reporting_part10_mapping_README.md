
# 📄 Outbound Contract Loan Type View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that exposes
**outstanding loan balances by loan type**, enriched with Salesforce
contract context and employer participation metrics.

The view demonstrates advanced SQL techniques including UNPIVOT,
snapshot alignment, and window-function-based aggregation.

---

## View Purpose
The view provides a **loan-type-granular analytics dataset** that:
- Breaks down outstanding loan balances by loan category
- Uses the latest effective snapshot for consistency
- Derives participating employer counts by loan type
- Aligns contract data with Salesforce CRM objects

---

## Design Highlights

### ✅ UNPIVOT-Based Normalization
Multiple loan balance columns are normalized into a single
`loan_type / loan_balance_amount` structure using `UNPIVOT`.

### ✅ Snapshot-Based Metric Selection
All balances are sourced from the **latest effective and load date** to
ensure consistent reporting across downstream consumers.

### ✅ Employer Participation Metrics
`DENSE_RANK()` and conditional aggregation are used to compute
participating employer counts per loan type.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
standard enterprise Salesforce integration patterns.

---

## Files

```
├── vw_reporting_part10_mapping.sql   # Fully commented SQL view
├── vw_reporting_part10_mapping.drawio  # Architecture diagram
└── vw_reporting_part10_mapping_README.md                           # This document
```

---

## What This Demonstrates

✅ Advanced SQL transformations (UNPIVOT)
✅ Snapshot-based analytics modeling
✅ Window functions & aggregation
✅ Financial domain expertise
✅ Salesforce system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
