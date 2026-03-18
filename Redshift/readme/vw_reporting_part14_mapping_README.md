
# 📄 Outbound Contract Balance View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound balance view** that
reports **plan balances by money source**, enriched with Salesforce
contract metadata.

The view demonstrates enterprise-grade balance modeling, reference data
integration, and plan group (PEP) handling.

---

## View Purpose
The view provides a **money-source-level balance dataset** that:
- Exposes aggregated plan balances by contribution source
- Uses the latest effective snapshot for consistency
- Normalizes source names via reference data
- Prevents double-counting through PEP-aware logic
- Aligns balances with Salesforce CRM contracts

---

## Design Highlights

### ✅ Snapshot-Based Balance Selection
Balances are sourced from the **most recent effective and load date**,
ensuring accurate and reproducible reporting.

### ✅ Source-Level Balance Modeling
Balances are grouped by **plan money source**, supporting detailed
asset allocation and funding analysis.

### ✅ Reference Data Enrichment
Contribution source reference data is used to produce clean,
human-readable source names with defensive defaults.

### ✅ PEP (Plan Group) Handling
The design safely handles employer plan groups by ensuring balances are
reported at the correct master contract level.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
enterprise Salesforce integration standards.

---

## Files

```
├── vw_reporting_part14_mapping.sql   # Fully commented SQL view
├── vw_reporting_part14_mapping.drawio  # Architecture diagram
└── vw_reporting_part14_mapping_README.md                          # This document
```

---

## What This Demonstrates

✅ Balance and asset modeling
✅ Snapshot-based analytics design
✅ Reference data integration
✅ Hierarchical plan group handling
✅ CRM-aligned outbound SQL

---

**Author:** Adit Joshi  
Senior Data Engineer
