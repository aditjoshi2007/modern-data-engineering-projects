
# 📄 Outbound Contract Loans & Balances View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that normalizes
multiple loan and balance metrics into a single, analytics-ready dataset
for Salesforce and downstream consumers.

The view demonstrates how complex financial metrics can be unified while
respecting employer (PEP) hierarchies.

---

## View Purpose
The view provides a **standardized loans & balances dataset** that:
- Exposes loan balances and key account balances
- Uses the latest snapshot for consistency
- Normalizes heterogeneous metrics into a single structure
- Correctly rolls up employer (PEP) plans
- Aligns contracts with Salesforce CRM objects

---

## Design Highlights

### ✅ Snapshot-Based Metric Selection
All metrics are sourced from the **latest effective and load date**, ensuring
consistent and reproducible reporting.

### ✅ Metric Normalization via UNION ALL
Different financial metrics (loans, forfeiture, expense, advance balances)
are normalized into a common structure using `UNION ALL`.

### ✅ PEP (Plan Group) Handling
The design prevents double-counting by:
- Excluding non-master plans
- Rolling balances up to the plan group master

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, a standard
enterprise Salesforce integration pattern.

---

## Files

```
├── vw_reporting_part9_mapping.sql   # Fully commented SQL view
├── vw_reporting_part9_mapping.drawio  # Architecture diagram
└── vw_reporting_part9_mapping_README.md                                    # This document
```

---

## What This Demonstrates

✅ Advanced SQL modeling
✅ Financial metric normalization
✅ Hierarchical rollup strategies
✅ Snapshot-based analytics design
✅ Salesforce system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
