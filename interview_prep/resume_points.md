# Resume Points — Work Experience Section

> **Role:** Data Engineer at Accenture (Feb 2024 – Present)

Below are **8 variation batches** of resume bullet points. Each batch is self-contained. **Recommendation: Pick 2–3 points from any ONE batch** (not 2–3 batches — 2–3 individual points). Your work experience section should have ~5–6 total bullet points, and this automation should be 2–3 of those.

---

## Batch 1 (Recommended — Balanced Technical + Impact)

- **Designed and developed an end-to-end ADF and Databricks pipeline monitoring automation** using Python Azure Functions, Azure SQL, and REST APIs to track 150+ ADF pipelines and 45+ Databricks jobs across 30 data products — reducing ~25 hours/week of manual monitoring effort for the data engineering team.

- **Built an intelligent failure alerting system** with Azure Logic Apps (Adaptive Card notifications to Microsoft Teams), automated ServiceNow incident creation with smart deduplication logic (one incident per pipeline per day), and a self-healing mechanism that auto-detects successful manual re-triggers and corrects the dashboard status in real time.

- **Engineered the reporting data layer** using SQL views with schedule-aware monitoring logic, midnight-crossing schedule handling, IST timezone conversion, and a tiered long-running job detection algorithm — powering a real-time Power BI operations dashboard with complete visibility into pipeline health and failure trends.

---

## Batch 2 (Impact-Heavy — Best for Senior Roles)

- **Architected a serverless monitoring platform on Azure** (Functions, Logic Apps, SQL Server) that automates end-to-end observability for 150+ ADF pipelines and 45+ Databricks jobs — eliminating ~25 hours/week of manual status checking, failure triage, and incident creation across 30 data products.

- **Implemented automated ITSM integration** with ServiceNow REST API featuring dynamic incident routing based on data product ownership, configurable incident creation policies, and intelligent deduplication — reducing mean time to incident creation from 15+ minutes to under 30 seconds.

- **Developed a self-healing monitoring mechanism** that detects manually re-triggered jobs post-failure and auto-corrects the reporting layer, combined with real-time Adaptive Card alerts to Microsoft Teams for immediate failure visibility across the team.

---

## Batch 3 (Technical Depth — Best for Technical Interviews)

- **Built a multi-platform pipeline monitoring system** using Python Azure Functions (timer-triggered every 5 minutes), integrating with ADF Management SDK and Databricks REST API 2.1 to poll, track, and log execution status for 200+ daily scheduled job executions across 30 data products into an Azure SQL database.

- **Developed a config-driven alerting and incident management framework** with SQL-based scheduling views (supporting comma-separated multi-schedule entries and midnight-crossing logic), automated ServiceNow incident creation via REST API with per-pipeline deduplication, and Azure Logic App workflows delivering Adaptive Card alerts to Microsoft Teams.

- **Designed the SQL reporting layer** with views computing IST-converted timestamps, dynamic long-running job flags (tiered thresholds: 50%/30%/15% buffer based on job duration), and a self-healing recheck mechanism that auto-updates failure status when manual re-triggers succeed — powering a real-time Power BI operations dashboard.

---

## Batch 4 (Concise — 2 Points Only)

- **Designed and developed an automated ADF and Databricks pipeline monitoring platform** using Python Azure Functions, Azure SQL, and REST APIs — tracking 150+ ADF pipelines and 45+ Databricks jobs in real time, with automated failure alerting via Microsoft Teams, ServiceNow incident creation, and a SQL-powered reporting layer for the Power BI operations dashboard; reduced ~25 hours/week of manual monitoring effort.

- **Built an intelligent failure management system** featuring schedule-aware monitoring with midnight-crossing handling, smart incident deduplication (one per pipeline per day), configurable alerting policies, and a self-healing mechanism that auto-detects successful re-triggers — achieving near-zero manual intervention for pipeline health tracking.

---

## Batch 5 (Action-Verb Driven — ATS Optimized)

- **Automated** real-time monitoring of 150+ ADF pipelines and 45+ Databricks jobs by developing Python Azure Functions that poll platform APIs every 5 minutes, log execution metadata to Azure SQL, and trigger Adaptive Card alerts to Microsoft Teams via Logic Apps — **reducing** 25+ hours/week of manual monitoring across 30 data products.

- **Engineered** an automated incident management workflow integrating ServiceNow REST API with intelligent deduplication (one incident per pipeline per day), dynamic POC-based routing from a config-driven data product registry, and configurable creation policies — **decreasing** mean time to incident creation from 15 minutes to under 30 seconds.

- **Developed** a self-healing monitoring mechanism and SQL-based reporting layer with views computing schedule-aware status tracking, tiered long-running job detection, and automatic failure-to-success corrections when manual re-triggers are detected — **powering** the team's real-time Power BI operations dashboard.

---

## Batch 6 (Data Engineering Focused — Pipeline + SQL Heavy)

- **Built an automated pipeline monitoring system** for 150+ ADF pipelines and 45+ Databricks jobs using Python Azure Functions, REST APIs (ADF Management SDK, Databricks API 2.1), and Azure SQL stored procedures with MERGE-based upsert logic — enabling real-time execution tracking across 30 data products with zero manual intervention.

- **Designed and implemented 6 SQL views and 2 stored procedures** powering a schedule-aware monitoring engine with midnight-crossing logic, epoch-to-datetime conversion for Databricks timestamps, tiered long-running job detection, and IST timezone conversion — serving as the reporting data layer for the team's Power BI operations dashboard.

---

## Batch 7 (Problem → Solution → Impact Format)

- **Identified that manual monitoring of 200+ daily pipeline executions consumed ~25 hours/week** across the data engineering team, and designed an automated solution using Python Azure Functions, Azure SQL, and Logic Apps — delivering real-time failure detection, automated ServiceNow incident creation with smart deduplication, and Adaptive Card alerts to Microsoft Teams within seconds of failure.

- **Solved the stale dashboard problem** where manually re-triggered successful jobs still showed as FAILURE by building a self-healing recheck mechanism that polls APIs every 15 minutes, detects successful re-triggers, and auto-corrects the SQL database — restoring team confidence in the real-time Power BI operations dashboard.

---

## Batch 8 (Shortest — Single Power Point)

- **Designed and built an end-to-end ADF and Databricks pipeline monitoring automation** using Python Azure Functions, Azure SQL, REST APIs, Logic Apps, and ServiceNow integration — automating real-time failure detection, alerting, and incident creation for 150+ pipelines and 45+ jobs across 30 data products, reducing ~25 hours/week of manual monitoring and enabling a real-time Power BI operations dashboard.

---

## How the ~25 Hours/Week Estimate is Calculated

| Manual Task (Before Automation) | Estimated Daily Effort (across team) | Weekly (5 days) |
|-------------------------------|----------------------|-------------------|
| Checking 200 pipeline/job statuses manually (2-3 min each, split across team members) | ~3–4 hours | ~15–20 hours |
| Sending failure notifications to the team | ~20–30 min | ~2–2.5 hours |
| Creating ServiceNow incidents manually | ~15–20 min | ~1.5–2 hours |
| Compiling daily status updates | ~20–30 min | ~2–2.5 hours |
| **Total** | **~4–5 hours/day** | **~20–27 hours/week** |

> **Tip:** In interviews, say *"approximately 25 hours per week"* or *"roughly 100 hours per month"* — both are realistic and compelling.

---

## Why Real Numbers Are Better Than Inflated Ones

Your actual scale is already impressive:
- **150+ ADF pipelines** across 20 data products
- **45+ Databricks jobs** across 10 data products
- **200+ scheduled executions daily** (many pipelines run multiple times)
- **30 data products** monitored simultaneously
- **25 team members** benefiting from the automation

If asked "Can your system handle more?", answer: *"Absolutely — the architecture is config-driven. Adding a new pipeline or job is a single INSERT statement in the master table. There are no code changes required, making it horizontally scalable."*

---

## Important Notes

### Power BI Dashboard
You did NOT build the Power BI dashboard itself — you designed and built the **SQL views and stored procedures** (the data layer) that the dashboard connects to via DirectQuery. Always say: *"I built the reporting data layer — the SQL views that power the dashboard"* not *"I built the dashboard."*

### ServiceNow Incidents
Frame incidents as routing to the **appropriate team**, not YOUR team. Say: *"The automation creates incidents and routes them to the designated owner based on the data product config table."*

### Recommendation: How Many Points for This Project
- **2 points** if you have 5+ other bullet points to show breadth
- **3 points** if this is your strongest work (recommended — it IS your strongest)
- **Never more than 3** — it looks like you only did one thing at Accenture
