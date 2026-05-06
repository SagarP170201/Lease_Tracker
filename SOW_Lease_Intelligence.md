# Statement of Work (SOW)

## Lease Intelligence Platform — Powered by Snowflake

**Prepared for:** Apparel Group  
**Prepared by:** Snowflake Solutions Engineering  
**Date:** May 6, 2026  
**Version:** 1.0

---

## 1. Executive Summary

This SOW covers the design, development, and deployment of an AI-powered **Lease Intelligence Platform** built entirely on Snowflake. The platform automates lease agreement ingestion, extracts 30+ structured fields using Cortex AI, enables human-in-the-loop validation, and provides real-time portfolio analytics — eliminating manual data entry and spreadsheet-based lease tracking across 100+ retail locations.

---

## 2. Business Objectives

| # | Objective |
|---|-----------|
| 1 | Eliminate manual lease data entry (currently 2-3 hours per agreement) |
| 2 | Centralize lease portfolio data across all brands, regions, and malls |
| 3 | Provide real-time visibility into monthly outgo, renewals, and escalations |
| 4 | Enable proactive alerts for expiring leases, lock-in periods, and escalation dates |
| 5 | Generate cash flow projections incorporating escalation clauses |
| 6 | Reduce renewal-related revenue leakage by 15-20% through timely alerts |

---

## 3. Solution Architecture

```
                        ┌─────────────────────────────────┐
                        │   Streamlit in Snowflake (SiS)  │
                        │   Lease Intelligence Dashboard  │
                        └──────────────┬──────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
   ┌────▼────┐                 ┌───────▼───────┐             ┌───────▼───────┐
   │ Upload  │                 │ Review &      │             │ Analytics &   │
   │ & Ingest│                 │ Approve       │             │ Alerts        │
   └────┬────┘                 └───────────────┘             └───────────────┘
        │
        ▼
  ┌───────────┐    Stream    ┌──────────────────┐    Stream    ┌───────────────┐
  │ Snowflake ├─────────────►│ Task 1:          ├─────────────►│ Task 2:       │
  │ Stage     │              │ AI_PARSE_DOCUMENT│              │ AI_EXTRACT    │
  │(LEASE_DOCS)│              │ (LAYOUT mode)    │              │ (30+ fields)  │
  └───────────┘              └──────────────────┘              └───────┬───────┘
                                                                       │
                                                                       ▼
                             ┌──────────────────┐              ┌───────────────┐
                             │ LEASE_TRACKER    │◄─── Approve ─┤ LEASE_STAGING │
                             │ (Golden Record) │              │(PENDING_REVIEW)│
                             └──────────────────┘              └───────────────┘
```

---

## 4. Scope of Work

### 4.1 Data Ingestion Pipeline

| Deliverable | Description |
|-------------|-------------|
| Internal Stage | `LEASE_DOCS` stage with directory table enabled for lease PDF storage |
| Stream 1 | `LEASE_DOCS_STREAM` — detects new file uploads to stage |
| Task 1 | `PARSE_LEASE_DOCS_TASK` — runs AI_PARSE_DOCUMENT (LAYOUT mode) on new PDFs, stores full parsed text in `PARSED_DOCS` |
| Stream 2 | `PARSED_DOCS_STREAM` — detects new parsed documents |
| Task 2 | `EXTRACT_LEASE_FIELDS_TASK` — runs AI_EXTRACT to extract 30+ structured lease fields into `LEASE_STAGING` |
| Schedule | Tasks run on a 5-minute cadence for near-real-time processing |

### 4.2 Data Model

**LEASE_TRACKER (Golden Record) — 35+ columns:**

| Category | Fields |
|----------|--------|
| Property | STORE_NAME, MALL_NAME, CITY, STATE, REGION, CARPET_AREA_SQFT |
| Parties | LESSOR_NAME, LESSEE_NAME, BRAND |
| Tenure | LEASE_START_DATE, LEASE_END_DATE, LEASE_TENURE_MONTHS, LOCK_IN_PERIOD_MONTHS, LOCK_IN_EXPIRY_DATE, NOTICE_PERIOD_DAYS, RENT_FREE_PERIOD_MONTHS |
| Rent | RENT_MODEL, FIXED_RENT_MONTHLY, REVENUE_SHARE_PCT, MINIMUM_GUARANTEE_MONTHLY, RATE_PER_SQFT |
| Escalation | ESCALATION_PCT, ESCALATION_FREQUENCY_MONTHS, NEXT_ESCALATION_DATE |
| Charges | CAM_MONTHLY, HVAC_MONTHLY, MARKETING_CONTRIBUTION_MONTHLY, SECURITY_DEPOSIT, STAMP_DUTY, REGISTRATION_COST, CAPEX_REIMBURSEMENT |
| Payment | PAYMENT_DUE_DAY, TOTAL_MONTHLY_OUTGO |
| Renewal | RENEWAL_OPTION, RENEWAL_NOTICE_DAYS, RENEWAL_DATE |
| Status | STATUS (ACTIVE / EXPIRING / EXPIRED), APPROVED_BY, APPROVED_AT |

### 4.3 Streamlit Application (6 Pages)

| Page | Functionality |
|------|---------------|
| **Portfolio Overview** | KPI metrics (total leases, monthly outgo, area, deposits), charts by brand/region/rent model/status, full lease table |
| **Upload & Extract** | PDF upload to Snowflake stage, pipeline status monitoring, stage contents, parsed document listing, architecture diagram |
| **Review & Approve** | Human-in-the-loop validation of AI-extracted fields, inline editing of 30+ fields organized by section, approve/reject/re-extract actions |
| **Cash Flow** | Monthly cash flow projection (6-36 months), escalation-aware calculations, brand-wise breakdown, stacked area chart |
| **Alerts & Renewals** | Lease expiry alerts (<90d critical, <180d warning), lock-in expiry tracking, upcoming escalation alerts, full renewal calendar |
| **Regional Analytics** | Region/city/state/mall level analytics, rate per sq ft benchmarking, deposit analysis, multi-select filtering |

### 4.4 AI/ML Components (Cortex AI)

| Function | Usage |
|----------|-------|
| AI_PARSE_DOCUMENT | Layout-aware PDF parsing preserving table structures (supports 60+ page agreements) |
| AI_EXTRACT | Structured field extraction with 30+ response format keys, handles Indian lease terminology (CAM, HVAC, lock-in, fit-out period) |

---

## 5. Snowflake Products & Features Used

| Product/Feature | Purpose |
|-----------------|---------|
| Snowflake Cortex AI (AI_PARSE_DOCUMENT) | Document intelligence — layout-aware PDF parsing |
| Snowflake Cortex AI (AI_EXTRACT) | Structured data extraction from unstructured text |
| Streams | Change data capture on stage and tables |
| Tasks | Scheduled/chained pipeline orchestration |
| Streamlit in Snowflake (SiS) | Interactive dashboard — no external hosting needed |
| Internal Stages (Directory Tables) | Secure document storage with file metadata |
| Snowpark (Python) | Server-side session for data access |

---

## 6. Deliverables & Timeline

| Phase | Deliverable | Duration |
|-------|-------------|----------|
| **Phase 1: Foundation** | Database, schema, stage, table DDL, seed data (30 leases) | 1 week |
| **Phase 2: AI Pipeline** | Streams, tasks, AI_PARSE_DOCUMENT + AI_EXTRACT pipeline | 1 week |
| **Phase 3: Application** | 6-page Streamlit app with all analytics pages | 2 weeks |
| **Phase 4: Testing & Refinement** | Real document testing, field accuracy tuning, edge case handling | 1 week |
| **Phase 5: Deployment & Handoff** | SiS deployment, documentation, knowledge transfer | 1 week |
| **Total** | | **6 weeks** |

---

## 7. Assumptions & Dependencies

1. Customer provides access to a Snowflake Enterprise (or higher) account with Cortex AI enabled
2. Lease agreements are in PDF format (scanned or digital)
3. Documents are primarily in English with Indian commercial lease terminology
4. Customer has a warehouse available for task execution (recommended: MEDIUM)
5. Initial seed data (30+ active leases) will be provided by the customer for validation
6. Cortex AI functions (AI_PARSE_DOCUMENT, AI_EXTRACT) are GA in the customer's region

---

## 8. Success Criteria

| Metric | Target |
|--------|--------|
| Document Processing Accuracy | >90% field extraction accuracy on standard lease agreements |
| Processing Time | <5 minutes from PDF upload to Review & Approve |
| Portfolio Coverage | 100% of active leases tracked in system |
| Renewal Alert Coverage | 100% of leases with end dates generate timely alerts |
| User Adoption | Lease team using platform as primary tracking tool within 30 days |

---

## 9. Out of Scope (Future Enhancements)

| Enhancement | Description |
|-------------|-------------|
| Cortex Search | Natural language search across parsed lease text |
| Cortex Analyst / Semantic View | NL querying ("What's my total outgo in Mumbai?") |
| Email/Slack Alerts | Automated notifications for critical renewals |
| Multi-language Support | Arabic/Hindi lease document processing |
| OCR Enhancement | Handling poor-quality scanned documents |
| Integration | ERP/SAP integration for payment reconciliation |

---

## 10. Pricing Considerations

| Component | Billing Model |
|-----------|---------------|
| Cortex AI (AI_PARSE_DOCUMENT, AI_EXTRACT) | Per-token / per-page (serverless) |
| Tasks (scheduled execution) | Serverless compute credits |
| Streamlit in Snowflake | Warehouse credits during active usage |
| Storage (PDFs + tables) | Standard Snowflake storage rates |

*Note: Detailed credit estimates depend on document volume and warehouse size. A MEDIUM warehouse running tasks for ~30 documents/month is estimated at <50 credits/month.*

---

## 11. Repository & Artifacts

| Artifact | Location |
|----------|----------|
| SiS Application Code | [GitHub - Lease_Tracker](https://github.com/SagarP170201/Lease_Tracker) — `streamlit_app.py` |
| Local Development Script | [GitHub - Lease_Tracker](https://github.com/SagarP170201/Lease_Tracker) — `lease_tracker_local.py` |
| Deployed SiS App | `SI_EVENTS_HOL.LEASE_INTELLIGENCE.LEASE_INTELLIGENCE_APP` |
| Database/Schema | `SI_EVENTS_HOL.LEASE_INTELLIGENCE` |

---

## 12. Acceptance

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Customer Sponsor | | | |
| Snowflake SE | Sagar Pawar | | |
| Project Manager | | | |

---

*This SOW is based on the production-deployed Lease Intelligence Platform currently running in Snowflake account SFSEAPAC-SPAWAR_AWSEAST1.*
