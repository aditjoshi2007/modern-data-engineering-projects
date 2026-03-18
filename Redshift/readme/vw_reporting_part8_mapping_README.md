
# 📄 Outbound Contract New Loans MTD View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that exposes
**month-to-date (MTD) new loan balances** for downstream systems such as
Salesforce and analytics platforms.

The view demonstrates a clean, snapshot-based design commonly used in
enterprise financial reporting.

---

## View Purpose
The view provides a **standardized MTD loan dataset** that:
- Exposes new loan balances at the contract level
- Uses only the latest effective snapshot for consistency
- Aligns internal contract identifiers with Salesforce records
- Applies standardized domain and transaction labels

---

## Design Highlights

### ✅ Snapshot-Based Metric Selection
MTD loan metrics are sourced exclusively from the **most recent effective
and load date**, ensuring current and reproducible reporting.

### ✅ Clear Metric Semantics
The metric represents **month-to-date new loan balances**, distinct from:
- Outstanding loan balances
- Year-to-date loan metrics

This clarity simplifies downstream analytics.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, a standard
enterprise Salesforce integration pattern.

### ✅ Standardized Outbound Labels
Static `data_domain` and `transaction_type` values enable:
- Consistent downstream filtering
- Unified reporting across outbound datasets

---

## Files

```
├── vw_reporting_part8_mapping.sql   # Fully commented SQL view
├── vw_reporting_part8_mapping.drawio  # Architecture diagram
└── vw_reporting_part8_mapping_README.md                               # This document
```

---

## What This Demonstrates

✅ Snapshot-based analytics design
✅ Financial metrics modeling
✅ CRM-aligned data structures
✅ Clean outbound SQL patterns

---

**Author:** Adit Joshi  
Senior Data Engineer
