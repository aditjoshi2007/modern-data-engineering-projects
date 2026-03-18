
# 📄 Outbound Financial Summary View (Portfolio Example)

## Overview
This project showcases an **enterprise-grade outbound financial summary view**
designed to expose contract-level financial metrics to downstream systems
such as Salesforce and reporting layers.

The view demonstrates how data engineers design **stable, consumption-layer
interfaces** even when upstream metric availability evolves over time.

---

## View Purpose
The view provides a **single, contract-aligned financial snapshot** that:
- Filters to active and supported plan types
- Uses the most recent summary snapshot for consistency
- Aligns internal contract identifiers with Salesforce records
- Maintains schema stability for downstream consumers

---

## Design Highlights

### ✅ Current Record Handling
Only active plan records are included using:
- `is_current = 1`
- `valid_to IS NULL`

This ensures historical plan records do not leak into outbound feeds.

### ✅ Effective-Date Snapshot Control
A subquery guarantees that all outbound metrics are aligned to the
**latest available effective date**, which is critical for financial reporting.

### ✅ Business Rule Exclusions
Certain plan types are intentionally excluded from outbound processing:
- Managed DB plans
- Defined benefit plans

This enforces downstream contractual and reporting constraints.

### ✅ Schema Stabilization Pattern
Constant placeholder values are used for selected metrics to:
- Preserve schema contracts
- Enable parallel downstream development
- Allow incremental rollout of upstream calculations

### ✅ Salesforce Integration
Contracts are joined using **prefix + suffix matching**, a common
enterprise integration pattern between data warehouses and Salesforce.

---

## Files

```
├── vw_reporting_part3_mapping.sql   # Fully commented SQL view
├── vw_reporting_part3_mapping.drawio  # Architecture diagram
└── vw_reporting_part3_mapping_README.md                          # This document
```

---

## What This Demonstrates

✅ Consumption-layer SQL design
✅ Financial reporting patterns
✅ Snapshot-based modeling
✅ Schema evolution strategies
✅ Salesforce system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
