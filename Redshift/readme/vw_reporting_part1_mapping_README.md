
# 📄 Outbound Source Mapping View (Portfolio Example)

## Overview
This project showcases an **enterprise-grade consumption-layer SQL view** designed to
bridge internal contribution-source data with Salesforce contract context.

The view demonstrates real-world data warehouse design patterns such as:
- Effective-dated snapshot selection
- Current-record dimensional modeling
- String normalization for reporting
- Stable joins against raw-current ingestion tables
- Salesforce integration patterns

---

## View Purpose
The view provides a **clean, analytics-ready dataset** that:
- Exposes contribution source names in a user-friendly format
- Aligns internal contract identifiers with Salesforce contracts
- Guarantees consistency by always using the latest metric snapshot

---

## Design Highlights

### ✅ Current Record Handling
Only active dimensional records are selected using:
- `is_current = 1`
- `valid_to IS NULL`

This prevents historical noise from impacting downstream analytics.

### ✅ Effective-Date Control
A subquery ensures **only the latest effective date** is used, which is
critical for reproducible reporting and financial reconciliation.

### ✅ String Normalization
Multi-part source names are trimmed and concatenated to avoid:
- Trailing spaces
- Inconsistent display values

### ✅ Salesforce Integration
Contracts are joined using **prefix + suffix matching**, a common enterprise
pattern when integrating legacy warehouse keys with Salesforce IDs.

---

## Files

```
├── vw_reporting_part1_mapping.sql   # Fully commented SQL view
├── vw_reporting_part1_mapping.drawio  # Architecture diagram
└── vw_reporting_part1_mapping_README.md                        # This document
```

---

## What This Demonstrates

✅ Advanced SQL design
✅ Consumption-layer modeling
✅ Cross-system integration
✅ Production-safe anonymization

---

**Author:** Adit Joshi  
Senior Data Engineer
