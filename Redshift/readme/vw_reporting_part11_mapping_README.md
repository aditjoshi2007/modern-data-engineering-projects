
# 📄 Outbound Contract Investment Services View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that reports
adoption and balances for **investment services and features** such as
managed advice, portfolio services, and target date offerings.

The view demonstrates how feature-level metrics are normalized and
aggregated for CRM and analytics consumption.

---

## View Purpose
The view provides a **service-feature-granular dataset** that:
- Exposes active and termed participant counts
- Exposes balances associated with each service
- Uses the latest snapshot for consistency
- Derives participating employer counts
- Enriches records with Salesforce contract metadata

---

## Design Highlights

### ✅ Feature Normalization via UNION ALL
Each investment service is projected into a common structure using
`UNION ALL`, simplifying downstream analytics.

### ✅ Snapshot-Based Consistency
All metrics are aligned to the **latest effective and load date** to
ensure reproducible reporting.

### ✅ Employer Participation Metrics
`DENSE_RANK()` and conditional aggregation derive participating employer
counts while respecting parent/child plan relationships.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
enterprise Salesforce integration standards.

---

## Files

```
├── vw_reporting_part11_mapping.sql   # Fully commented SQL view
├── vw_reporting_part11_mapping.drawio  # Architecture diagram
└── vw_reporting_part11_mapping_README.md                                     # This document
```

---

## What This Demonstrates

✅ Feature-level analytics modeling
✅ Snapshot-based financial reporting
✅ Complex UNION-based normalization
✅ Window functions & conditional aggregation
✅ Salesforce system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
