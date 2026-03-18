
# 📄 Outbound Fund Mapping View (Portfolio Example)

## Overview
This project demonstrates an **enterprise-grade fund-level outbound view** used
for downstream consumption and Salesforce alignment.

The view highlights advanced warehouse patterns including:
- Fund and plan dimensional modeling
- Effective-dated metric snapshots
- Business-rule-driven filtering
- Safe handling of sparse metric data
- Salesforce contract integration

---

## View Purpose
The view produces a **clean, curated list of funds** that are relevant for
external consumers by:
- Including only active or financially relevant funds
- Aligning internal contracts with Salesforce records
- Ensuring consistency via latest snapshot logic

---

## Design Highlights

### ✅ Current Record Handling
Only active plan and fund records are selected using:
- `is_current = 1`
- `valid_to IS NULL`

### ✅ Effective-Date Control
A subquery guarantees only the **most recent fund snapshot** is used,
which is critical for financial accuracy.

### ✅ Business Rule Filtering
Funds are included if they:
- Have a non-zero balance, OR
- Are explicitly flagged as ACTIVE and DISPLAY

This prevents clutter from inactive or irrelevant funds.

### ✅ Salesforce Integration
Contracts are joined using **prefix + suffix matching**, a standard
pattern when integrating legacy warehouse keys with Salesforce IDs.

---

## Files

```
├── vw_reporting_part2_mapping.sql     # Fully commented SQL view
├── vw_reporting_part2_mapping.drawio    # Architecture diagram
└── vw_reporting_part2_mapping_README.md                       # This document
```

---

## What This Demonstrates

✅ Advanced SQL & data modeling
✅ Financial data filtering logic
✅ Snapshot-based analytics
✅ Cross-system integration

---

**Author:** Adit Joshi  
Senior Data Engineer
