
# 📄 Outbound Contract Contribution Source View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound contribution view** that
breaks down **year-to-date contributions by source type** and enriches the
results with Salesforce contract context.

The view demonstrates clean snapshot-based modeling and reference data
integration commonly used in enterprise financial platforms.

---

## View Purpose
The view provides a **source-granular contribution dataset** that:
- Exposes YTD contribution amounts by contribution source
- Uses the latest effective snapshot for consistency
- Resolves source names via reference mappings
- Aligns contribution data with Salesforce CRM contracts

---

## Design Highlights

### ✅ Snapshot-Based Metric Selection
Contribution metrics are sourced exclusively from the **most recent effective
and load date**, ensuring accurate and reproducible reporting.

### ✅ Reference Data Enrichment
A left join to contribution source reference data resolves clean,
user-friendly source names while preserving fallback logic.

### ✅ Clear Metric Semantics
The view explicitly models **year-to-date (YTD)** participant contributions
at the source level, supporting payroll, rollover, and employer analysis.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
enterprise Salesforce integration standards.

---

## Files

```
├── vw_reporting_part12_mapping.sql   # Fully commented SQL view
├── vw_reporting_part12_mapping.drawio  # Architecture diagram
└── vw_reporting_part12_mapping_README.md                                     # This document
```

---

## What This Demonstrates

✅ Snapshot-based analytics design
✅ Reference data integration
✅ Financial domain modeling (contributions)
✅ CRM-aligned outbound SQL

---

**Author:** Adit Joshi  
Senior Data Engineer
