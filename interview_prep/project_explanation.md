# Detailed Project Explanation

This document provides a comprehensive, in-depth explanation of the entire Job Monitoring Automation project — every component, every decision, and every technical detail. Use this as your study material before interviews.

---

## The Problem

In a large-scale data engineering environment, we have:
- **150+ Azure Data Factory (ADF) pipelines** across 20 data products
- **45+ Databricks jobs** across 10 data products
- These pipelines collectively execute **200+ scheduled runs every single day**
- A team of **25 engineers** manages this ecosystem

**Before this automation, the daily workflow looked like this:**

1. **Morning check (distributed across team, ~3–4 hours total):** Multiple team members manually open ADF Studio and Databricks UI, navigate to each pipeline/job, check if it ran, check if it succeeded or failed.
2. **Failure triage (~30 min/day):** When a failure is found, the engineer copies the error message, opens ServiceNow, fills out the incident form, assigns it to the correct person, then messages the team on Teams.
3. **Status reporting (~20 min/day):** Someone compiles a spreadsheet or email summarizing which pipelines succeeded, which failed, which haven't started.
4. **Re-trigger follow-up (ongoing):** After re-triggering a failed pipeline, someone must remember to go back later and check if it succeeded, then update the spreadsheet/dashboard manually.

**Total manual effort across the team: ~4–5 hours/day (~25 hours/week)**

This was clearly unsustainable and error-prone. Pipelines were being missed, incidents were being created late, and the team had no real-time visibility.

---

## The Solution

I designed and built a **fully automated, serverless monitoring platform** that:

1. **Polls** every ADF pipeline and Databricks job automatically every 5 minutes
2. **Logs** the status of every run to a central database
3. **Alerts** the team instantly on any failure via Microsoft Teams
4. **Creates incidents** in ServiceNow automatically (with smart deduplication)
5. **Self-heals** by detecting successful manual re-triggers and auto-correcting the database
6. **Powers a dashboard** — SQL views and stored procedures serve as the data layer for a Power BI dashboard providing real-time visibility

**After automation: Zero hours of manual monitoring required.**

---

## Component Deep Dive

### Component 1: ADF Pipeline Monitor (`adf_jobs.py`)

**What it does:** Runs every 5 minutes as an Azure Function timer trigger. Checks if any ADF pipelines need monitoring right now, calls the ADF API, and updates the database.

**How it works — step by step:**

#### Step 1: Determine Which Pipelines to Check
The function queries a SQL view called `vw_ADFJobSchedules`. This view is the brain of the scheduling logic. It returns two types of records:

**Type A: "NOT STARTED" jobs**
- These are pipelines that are *supposed* to have started by now but haven't been logged yet
- The view checks the `ADFJobsMaster` table (where we register all pipelines we want to track)
- For each pipeline, it looks at the `Schedule` column (e.g., `'09:00,14:00,22:00'`) and uses `STRING_SPLIT` to break it into individual schedule times
- For each individual schedule time, it checks: *"Is the current UTC time within a 180-minute window starting from this schedule time?"*
- If yes, AND this specific (pipeline + factory + date + schedule) combo hasn't been logged yet, the view returns it as `Status = 'NOT STARTED'`

**Type B: "RUNNING" jobs**
- These are pipelines that we've already logged today and they were last recorded as RUNNING or QUEUED
- We need to re-check these to see if they've completed (successfully or with failure)

**Why 180 minutes for ADF?** ADF pipelines can have variable trigger delays — a pipeline scheduled at 09:00 might actually start at 09:15 or later depending on trigger conditions and concurrency limits. The 180-minute window ensures we catch late-starting pipelines.

**Midnight crossing logic:** Consider a pipeline scheduled at 22:00. The 180-minute window extends to 01:00 the next day. The view handles this with a CASE statement: if the window-end time wraps past midnight, it checks `current_time >= schedule_time OR current_time <= window_end`.

#### Step 2: Call the ADF API
For each pipeline returned by the view:

- **If NOT STARTED:** Call `pipeline_runs.query_by_factory()` to search for runs of this pipeline in the last 2 days. Filter to runs that started AFTER the schedule time (to avoid logging yesterday's run under today's schedule).
- **If RUNNING/QUEUED:** Call `pipeline_runs.get(run_id)` to get the current status of the specific run we already know about.

#### Step 3: Update the Database
Call the `UpdateADFJobRuns` stored procedure. This uses a `MERGE` statement:
- If the `JobRunId` already exists → UPDATE the status and end time
- If it's new → INSERT the full record

If the status is `Failed`, also INSERT into `failureLogs` (with a check to prevent duplicate failure entries).

#### Step 4: Handle Failures (Incident + Alert)
If the pipeline has failed, the `create_servicenow_incident` function orchestrates the response:

1. **Check the `incident_creation` flag** in `ADFJobsMaster`. Some pipelines are configured with `incident_creation = 0` because they're known to fail intermittently (e.g., retry-dependent ETL). For these, we skip incident creation but STILL send a Teams alert.

2. **Check for existing incidents today** in `incident_log`. This implements one-incident-per-pipeline-per-day. If an incident already exists, the Teams alert shows "Incident already created today - INC6113674" so the team knows the issue is already tracked.

3. **Look up data product config** from `DataProductConfig` table. This table maps each data product to:
   - POC name (point of contact)
   - Assignee ID (for ServiceNow assignment)
   - CMDB CI (configuration item for ServiceNow)
   This makes incident routing completely config-driven — no code changes needed when ownership changes.

4. **Create ServiceNow incident** via REST API POST. The incident includes the pipeline name, error message, run URL, data product, and is automatically assigned to the correct person.

5. **Send Teams alert** via Azure Logic App HTTP POST. The Logic App renders an Adaptive Card with all failure details and a "View Pipeline Run" button linking directly to the ADF monitoring page.

---

### Component 2: Databricks Job Monitor (`databricks_jobs.py`)

**What it does:** Same concept as the ADF monitor, but tailored for Databricks jobs.

**Key differences from ADF:**

1. **API:** Uses Databricks REST API 2.1 instead of ADF Management SDK
   - `GET /api/2.1/jobs/runs/list?job_id={id}&limit=1` for NOT STARTED jobs
   - `GET /api/2.1/jobs/runs/get?run_id={id}` for RUNNING jobs

2. **Schedule window:** 30 minutes instead of 180 minutes. Databricks jobs start almost instantly on schedule, so a shorter window prevents stale matches.

3. **Status mapping:** Databricks uses a two-level status system:
   - `life_cycle_state`: The execution phase (RUNNING, PENDING, TERMINATED, etc.)
   - `result_state`: The outcome (only when TERMINATED) — SUCCESS, FAILED, TIMEDOUT, CANCELED
   
   I built a mapping layer in both Python and SQL to normalize these to our four standard statuses.

4. **Error extraction:** For multi-task Databricks jobs, a single task failure causes the whole job to fail. I extract errors from individual task results (up to 3 failed tasks) so the alert message shows exactly which task failed and why.

5. **No ServiceNow incidents:** ADF pipelines create ServiceNow incidents because they orchestrate the end-to-end flow. Since many ADF pipelines call Databricks jobs via Execute Pipeline → REST API, the ADF failure already captures the Databricks failure. Creating incidents for both would be duplicate.

6. **Alert deduplication:** Uses `databricks_alert_log` table to track which run_ids have already been alerted. This prevents duplicate Teams notifications when the function polls the same failed run multiple times.

---

### Component 3: Recheck Failed Jobs (`recheck_failed_jobs.py`)

**What it does:** Runs every 15 minutes. Finds jobs that failed today and checks if they've been manually re-triggered and succeeded.

**Why this exists:** When a pipeline fails, the dashboard shows FAILURE. If someone manually re-triggers it and it succeeds, the dashboard would STILL show FAILURE until the next monitoring cycle — which might not happen if the original schedule window has passed. This caused confusion: "Is this pipeline still broken or was it fixed?"

**How it works:**

1. **Query failed ADF pipelines:**
   ```sql
   SELECT ... FROM jobRuns jr
   JOIN ADFJobsMaster jm ON jr.JobId = jm.PipelineName
   WHERE jr.Status = 'FAILURE' AND jr.AsOfDate = today
   ```

2. **For each failure, search for successful re-triggers:**
   - Call the ADF API for runs of this pipeline after the failure time
   - Filter for `status = 'Succeeded'` AND `run_id != failed_run_id`
   - **Critical check:** Ensure the successful run_id is NOT already in jobRuns. If it is, it's a scheduled run (not a manual re-trigger), and we shouldn't use it to overwrite the failure.

3. **Update the database:**
   - Change the jobRuns status from FAILURE to SUCCESS
   - Replace the run_id, start time, end time, and run URL with the re-trigger's details
   - Delete the entry from failureLogs

4. **Repeat for Databricks** with the same logic using the Databricks REST API.

---

### Component 4: SQL Database Schema

The database is the backbone of the system. Here's what each object does and why it exists:

#### Tables

| Table | Why It Exists |
|-------|--------------|
| `ADFJobsMaster` | Pipeline registration. When you want to monitor a new pipeline, INSERT a row here. No code changes needed. Contains schedule, data product, and the `incident_creation` flag. |
| `jobsMaster` | Same as above but for Databricks jobs. Contains workspace ID and job ID. |
| `DataProductConfig` | Decouples incident routing from code. When a data product's POC changes, update this table — no code deployment needed. |
| `jobRuns` | The core tracking table. One row per (pipeline + schedule + date). Updated via MERGE to handle both new runs and status changes. |
| `JobRunsHistory` | Historical archive. Current-day data lives in `jobRuns`; at end of day, it moves to history. Used for 7-day trend reporting. |
| `failureLogs` | Active failures only. When a job fails, it's logged here. When it's re-triggered successfully, the recheck function DELETES it. This keeps the failure view clean. |
| `incident_log` | Tracks which incidents have been created today for deduplication. |
| `databricks_alert_log` | Tracks which Databricks run_ids have been alerted to prevent duplicate Teams notifications. |
| `jobStatus` | Dimension table with the four standard statuses. Used by Power BI for slicer filters. |

#### Views

| View | Why It Exists |
|------|--------------|
| `vw_ADFJobSchedules` | Drives the ADF monitoring logic. The Python code doesn't decide which pipelines to check — the VIEW decides. This means scheduling logic changes require only a SQL view update, not a Python redeployment. |
| `vw_DatabricksJobSchedules` | Same for Databricks. |
| `VwRptJobsMaster` | Power BI needs a single unified source for all jobs. This UNION ALLs Databricks and ADF master tables. Also computes a NotStartedFlag. |
| `VwRptJobsRuns` | Power BI can't display UTC times to Indian users. This view converts UTC to IST (UTC + 5:30) and computes the long-running flag with tiered thresholds. |
| `VwRptJobsFailureLogs` | Combines live failures with historical failures for the failure details tab. |
| `VwRptJobsStatus` | Simple lookup for Power BI slicer buttons. |

#### Stored Procedures

| Procedure | Why It Exists |
|-----------|--------------|
| `UpdateADFJobRuns` | Encapsulates the INSERT/UPDATE logic. Uses MERGE for atomic upsert. Handles status translation (ADF's "Succeeded" → our "SUCCESS"). |
| `UpdateDatabricksJobRuns` | Same, but also converts Databricks epoch millisecond timestamps to DATETIME2 using `DATEADD(SECOND, epoch/1000, '1970-01-01')`. |

---

### Component 5: Alerting System

#### ADF Alerts (Teams + ServiceNow)
The ADF pipeline failure alert flow sends THREE different types of Teams messages:

1. **New Incident:** Shows the ServiceNow incident number (e.g., `INC6113674`)
2. **Existing Incident:** Shows "Incident already created today - INC6113674" — this happens when a pipeline with multiple schedules fails more than once in a day
3. **Incident Disabled:** Shows "Incident creation is disabled" — this is for pipelines where we've deliberately turned off incident creation but still want visibility

**Why we disable incident creation for some pipelines:**
Some pipelines fail intermittently due to transient issues (API rate limits, temporary network issues) and are designed to succeed on the next scheduled run. Creating a ServiceNow incident for every transient failure creates noise. We disable incidents for these but keep Teams alerts so the team is aware.

#### Databricks Alerts (Teams Only)
Databricks failures only generate Teams alerts (no ServiceNow incidents) because:
- ADF pipelines orchestrate Databricks jobs via Execute Pipeline activity
- When a Databricks job fails, the parent ADF pipeline also fails
- The ADF failure creates the incident, preventing duplicate tickets

---

### Component 6: SQL Reporting Layer (for Power BI Dashboard)

> **Important:** I built the SQL reporting views and stored procedures — the data layer. The Power BI dashboard itself was built by another team member. The dashboard connects to my views via **DirectQuery** for real-time data.

The 4 reporting views I designed serve as the complete data source:
- **VwRptJobsMaster**: Unifies ADF and Databricks into a single source, computes NotStartedFlag
- **VwRptJobsRuns**: Converts UTC to IST, calculates durations, computes long-running flags
- **VwRptJobsFailureLogs**: Combines live failures with 7-day historical failures
- **VwRptJobsStatus**: Status dimension for Power BI slicer filters

**Long-running detection thresholds (computed in VwRptJobsRuns):**
| Estimated Duration | Alert Threshold |
|---|---|
| < 2 hours | 150% of estimated (50% buffer) |
| 2–4 hours | 130% of estimated (30% buffer) |
| > 4 hours | 115% of estimated (15% buffer) |

The reason for tiered thresholds: Short jobs have more variability (a 10-minute job might take 15 minutes some days), so they get a bigger buffer. Long jobs are more consistent, so even a 15% overrun is worth flagging.

---

## Design Decisions & Trade-offs

### Why Azure Functions, not Databricks/ADF for monitoring?
- **ADF can't monitor itself** — circular dependency
- **Databricks is overkill** — this is lightweight API polling, not data processing
- **Azure Functions** are serverless, cost-effective ($0 for consumption plan at this scale), and guarantee execution via timer triggers

### Why SQL Server, not Cosmos DB or blob storage?
- Power BI works best with SQL via DirectQuery
- MERGE statements provide atomic upsert operations
- Views allow complex scheduling logic without Python changes
- Stored procedures encapsulate status translation and timestamp conversion

### Why Logic Apps for alerting, not direct Teams API?
- Logic Apps provide visual workflow design for Adaptive Cards
- Easy to modify alert format without code changes
- Built-in Teams connector handles authentication automatically
- Retry policies are handled by the Logic App runtime

### Why not use Azure Monitor / Application Insights alerts?
- Azure Monitor can alert on ADF pipeline failures, but it can't:
  - Track schedules ("this pipeline should have started by now")
  - Consolidate across ADF and Databricks into one dashboard
  - Create ServiceNow incidents with dynamic routing
  - Implement self-healing (recheck mechanism)
  - Provide config-driven, per-pipeline alerting policies
