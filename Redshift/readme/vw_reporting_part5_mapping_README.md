
# 📄 Outbound Financial Detail Contribution View (Portfolio Example)

## Overview
This project showcases an **asset-class-level outbound contribution view**
designed to expose participant contribution activity to downstream systems
such as Salesforce and analytics platforms.

The view demonstrates how contribution data is aggregated, filtered, and
aligned with CRM systems in enterprise data platforms.

---

## View Purpose
The view provides a **clean, contribution-focused dataset** that:
- Aggregates participant contributions by asset class
- Uses only the latest available snapshot for consistency
- Filters out zero-value contribution records
- Aligns contribution data with Salesforce contracts and summaries

---

## Design Highlights

### ✅ Snapshot-Based Aggregation
Only the **most recent effective and load date** is used to aggregate
contributions, ensuring accurate and current reporting.

### ✅ Meaningful Aggregation Level
Contributions are grouped by **asset class**, enabling:
- Portfolio allocation analysis
- CRM-level reporting
- Simplified downstream consumption

### ✅ Data Noise Reduction
A `HAVING` clause removes zero-value contribution groups, reducing
unnecessary records in outbound feeds.

### ✅ Salesforce Enrichment
The view integrates:
- Salesforce contract metadata
- Latest Salesforce financial summaries

This allows contribution data to be immediately usable by CRM systems.

---

## Files

```
├── vw_reporting_part5_mapping.sql   # Fully commented SQL view
├── vw_reporting_part5_mapping.drawio  # Architecture diagram
└── vw_reporting_part5_mapping_README.md                                      # This document
```

---

## What This Demonstrates

✅ Contribution analytics modeling
✅ Snapshot-based aggregation
✅ Salesforce system integration
✅ Enterprise outbound data design

---

**Author:** Adit Joshi  
Senior Data Engineer
