
# 📄 Outbound Contract Balance by Fund View (Portfolio Example)

## Overview
This project showcases a **contract-level outbound view** that exposes
**plan balances at the investment fund level**, enriched with Salesforce
contract metadata.

The view demonstrates fund-level balance modeling, snapshot alignment,
and dimensional enrichment used in enterprise retirement platforms.

---

## View Purpose
The view provides a **fund-granular balance dataset** that:
- Exposes balances by individual investment fund
- Uses the latest effective snapshot for consistency
- Preserves active/displayable funds even when balances are zero
- Provides rich fund descriptors for asset allocation analysis
- Aligns fund balances with Salesforce CRM contracts

---

## Design Highlights

### ✅ Snapshot-Based Balance Selection
Fund balances are sourced from the **most recent effective and load date**,
ensuring accurate and reproducible reporting.

### ✅ Fund-Level Asset Modeling
Balances are reported at the **fund level**, enabling:
- Asset class exposure analysis
- Fund lineup and utilization insights
- Downstream portfolio reporting

### ✅ Noise Reduction with Business Rules
Zero-balance funds are excluded unless the fund is:
- Active, and
- Explicitly marked for display

This preserves meaningful options without inflating record counts.

### ✅ Salesforce Integration
Contracts are enriched using **prefix + suffix matching**, following
enterprise Salesforce integration standards.

---

## Files

```
├── vw_reporting_part15_mapping.sql   # Fully commented SQL view
├── vw_reporting_part15_mapping.drawio  # Architecture diagram
└── vw_reporting_part15_mapping_README.md                              # This document
```

---

## What This Demonstrates

✅ Fund-level balance modeling
✅ Snapshot-based analytics design
✅ Dimensional enrichment (fund attributes)
✅ CRM-aligned outbound SQL

---

**Author:** Adit Joshi  
Senior Data Engineer
