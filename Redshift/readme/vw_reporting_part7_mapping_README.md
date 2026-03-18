
# 📄 Outbound Contract Withdrawal Type View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound withdrawal metrics view**
that breaks down withdrawal activity by withdrawal type and enriches it with
Salesforce contract context.

The view demonstrates advanced SQL patterns such as snapshot alignment,
window functions, and conditional aggregation.

---

## View Purpose
The view provides a **detailed withdrawal analytics dataset** that:
- Exposes withdrawal counts and amounts by type
- Uses only the latest effective snapshot for consistency
- Derives participating employer counts across contract groups
- Aligns withdrawal data with Salesforce CRM objects

---

## Design Highlights

### ✅ Snapshot-Based Metric Selection
All withdrawal metrics are sourced from the **latest effective and load date**,
ensuring consistent and reproducible reporting.

### ✅ Window Functions for Grouping
`DENSE_RANK()` is used to group contracts under the same employer hierarchy,
enabling group-level participation analysis.

### ✅ Conditional Aggregation Logic
Participating employer counts are derived using conditional flags, while
respecting parent/child contract relationships.

### ✅ Salesforce Integration
The view enriches each record with Salesforce contract metadata using
**prefix + suffix matching**, a standard enterprise integration pattern.

---

## Files

```
├── vw_reporting_part7_mapping.sql   # Fully commented SQL view
├── vw_reporting_part7_mapping.drawio  # Architecture diagram
└── vw_reporting_part7_mapping_README.md                                 # This document
```

---

## What This Demonstrates

✅ Advanced SQL analytics
✅ Window functions & ranking
✅ Snapshot-based modeling
✅ Conditional aggregation strategies
✅ Salesforce system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
