
# 📄 Outbound Contract Monthly Contribution View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that exposes
**monthly participant contribution amounts** aligned to calendar months
and enriched with Salesforce contract metadata.

The view demonstrates time-series modeling, snapshot alignment, and
zero-fill techniques commonly required in enterprise financial reporting.

---

## View Purpose
The view provides a **monthly contribution time-series dataset** that:
- Reports contributions at a contract/month grain
- Uses the latest effective snapshot for consistency
- Represents months with no activity using zero values
- Produces ISO-8601 timestamps for downstream systems
- Aligns contribution data with Salesforce CRM contracts

---

## Design Highlights

### ✅ Snapshot-Based Metric Selection
All contribution metrics are sourced from the **most recent effective
and load date**, ensuring reproducible reporting.

### ✅ Calendarized Monthly Modeling
`DATE_TRUNC('month', ...)` is used to normalize month boundaries and
ensure consistent aggregation across contracts.

### ✅ Zero-Fill Logic
A LEFT JOIN ensures that months without contribution activity are still
represented with a value of zero, supporting accurate trend analysis.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
enterprise Salesforce integration standards.

---

## Files

```
├── vw_reporting_part13_mapping.sql   # Fully commented SQL view
├── vw_reporting_part13_mapping.drawio  # Architecture diagram
└── vw_reporting_part13_mapping_README.md                                      # This document
```

---

## What This Demonstrates

✅ Time-series financial modeling
✅ Snapshot-based analytics design
✅ Zero-fill and calendarization techniques
✅ CRM-aligned outbound SQL

---

**Author:** Adit Joshi  
Senior Data Engineer
