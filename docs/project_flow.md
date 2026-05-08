# Project Flow — Detailed Execution Walkthrough

This document explains exactly how each component works, step by step.

---

## 1. ADF Pipeline Monitoring Flow

### Step 1: Timer Fires (Every 5 Minutes)
The Azure Function `adf_jobs.py` is triggered by a CRON-based timer trigger. On each execution:

### Step 2: Query Scheduling View
```sql
SELECT ... FROM [jobmonitoring].[vw_ADFJobSchedules]
```

The view returns pipelines in two categories:

**Category A — NOT STARTED Jobs:**
- The view checks `ADFJobsMaster` for all pipelines where `IsTracking = 'Y'`
- For each pipeline, it splits the `Schedule` column by comma (supports multiple schedules like `09:00,14:00,22:00`)
- For each individual schedule, it checks:
  - Is the current UTC time within a **180-minute window** starting from the schedule time?
  - Has this specific (pipeline + instance + date + schedule) combination already been logged in `jobRuns`?
- If both conditions are met → returned as `Status = 'NOT STARTED'`, `RunId = '0'`

**Category B — RUNNING Jobs:**
- Checks `jobRuns` for entries with `Status IN ('RUNNING', 'InProgress', 'QUEUED')`
- Only includes entries from today or yesterday (if started after 18:00 UTC — handles overnight jobs)
- Returns them with their actual `RunId` for status re-checking

### Step 3: Call ADF Management API
For each job returned by the view:

**If NOT STARTED:**
```python
adf_client.pipeline_runs.query_by_factory(
    factory_name=...,
    filter_parameters={
        "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [pipeline_name]}],
        "lastUpdatedAfter": ...,  # 2 days ago
        "lastUpdatedBefore": ...  # now
    }
)
```
- Queries all runs for this pipeline in the last 2 days
- Filters to only runs that started **after** the schedule time (prevents logging yesterday's run under today's schedule)
- Handles midnight crossing (if schedule is 22:00 and it's now 02:30, checks yesterday's 22:00)

**If RUNNING/QUEUED:**
```python
adf_client.pipeline_runs.get(resource_group, data_factory, run_id)
```
- Direct lookup of the specific run by ID to get updated status

### Step 4: Update Database
Calls the `UpdateADFJobRuns` stored procedure which uses a `MERGE` statement:
- **If JobRunId exists** → UPDATE status, end time, and parameters
- **If JobRunId is new** → INSERT full record with start time, status, run URL, etc.
- **If status is Failed** → Also INSERT into `failureLogs` (with duplicate check)

### Step 5: Handle Failures
If the pipeline status is `Failed`:

1. **Check incident_creation flag** in `ADFJobsMaster`
   - If disabled → Send Teams alert with label "Incident creation is disabled" → STOP

2. **Check for existing incident today** in `incident_log`
   - If exists → Send Teams alert with label "Incident already created today - INC123456" → STOP

3. **Create ServiceNow incident** via REST API
   - Dynamic POC assignment from `DataProductConfig` table
   - Log incident number to `incident_log` for deduplication
   - Send Teams alert with the new incident number

4. **Send Teams Alert** via Logic App (HTTP POST)
   - Always fires, regardless of incident creation status
   - Renders as an Adaptive Card with pipeline details and direct run link

---

## 2. Databricks Job Monitoring Flow

### Step 1: Timer Fires (Every 5 Minutes)
The Azure Function `databricks_jobs.py` is triggered.

### Step 2: Query Scheduling View
```sql
SELECT ... FROM [jobmonitoring].[vw_DatabricksJobSchedules]
```
- Similar logic to ADF but with a **30-minute window** (vs 180 min)
- Databricks jobs typically start within seconds of their scheduled time, so the shorter window is sufficient

### Step 3: Call Databricks REST API

**If NOT STARTED:**
```
GET https://{workspace_id}.azuredatabricks.net/api/2.1/jobs/runs/list?job_id={id}&limit=1
```
- Fetches the single most recent run for this job
- Validates against the schedule time (same midnight-crossing logic as ADF)

**If RUNNING/QUEUED/PENDING/TERMINATING:**
```
GET https://{workspace_id}.azuredatabricks.net/api/2.1/jobs/runs/get?run_id={id}
```
- Direct lookup of the specific run

### Step 4: Status Mapping
Databricks uses a two-level status system (unlike ADF's single status):
```
life_cycle_state → result_state (only when TERMINATED)
```

| life_cycle_state | result_state | Mapped Status |
|-----------------|-------------|--------------|
| RUNNING | — | RUNNING |
| PENDING | — | QUEUED |
| QUEUED | — | QUEUED |
| TERMINATING | — | RUNNING |
| TERMINATED | SUCCESS | SUCCESS |
| TERMINATED | FAILED | FAILED |
| TERMINATED | TIMEDOUT | FAILED |
| TERMINATED | CANCELED | CANCELED |
| INTERNAL_ERROR | — | FAILED |
| SKIPPED | — | SKIPPED |

### Step 5: Error Message Extraction
For failed jobs, errors are extracted in priority order:
1. `status.state_message` (primary)
2. `status.termination_details.message` (fallback)
3. Individual task errors for multi-task jobs (up to first 3 tasks)

### Step 6: Update Database + Alert
- Calls `UpdateDatabricksJobRuns` stored procedure (handles epoch → datetime conversion)
- On failure: checks `databricks_alert_log` for deduplication, then sends Teams alert via Logic App

---

## 3. Recheck Failed Jobs Flow

### Purpose
When a job fails, the dashboard shows FAILURE. If someone manually re-triggers the job and it succeeds, the dashboard should auto-correct. This function handles that.

### Step 1: Timer Fires (Every 15 Minutes)

### Step 2: Query Failed Jobs
```sql
SELECT ... FROM jobRuns jr
INNER JOIN ADFJobsMaster jm ON jr.JobId = jm.PipelineName
WHERE jr.Status = 'FAILURE' AND jr.AsOfDate = CAST(GETUTCDATE() AS DATE)
```

### Step 3: Search for Successful Re-triggers

**For ADF:**
- Query ADF API for runs after the failure time
- Filter: `status == 'Succeeded'` AND `run_id != failed_run_id` AND `start_time > failure_time`
- Extra check: Ensure the run_id is NOT already in `jobRuns` (to avoid matching scheduled runs)

**For Databricks:**
- Query Databricks API: `GET /api/2.1/jobs/runs/list?job_id={id}&limit=10`
- Same filtering logic as ADF

### Step 4: Update Records
If a successful re-trigger is found:
```sql
UPDATE jobRuns SET Status = 'SUCCESS', JobRunId = ?, StartTime = ?, EndTime = ? WHERE JobRunId = ?
DELETE FROM failureLogs WHERE JobRunId = ?
```

---

## 4. Alert Message Flow

### ADF Pipeline Failure Alert
```
adf_jobs.py → create_servicenow_incident() → notify_teams_logic_app()
                     │                                    │
                     ▼                                    ▼
              ServiceNow REST API              Logic App HTTP POST
              (creates incident)               (sends Teams card)
```

The Teams alert shows three possible states in the `Incident` field:
1. **`INC6113674`** — New incident created
2. **`Incident already created today - INC6113674`** — Duplicate prevented
3. **`Incident creation is disabled`** — Pipeline configured to skip incidents

### Databricks Job Failure Alert
```
databricks_jobs.py → databaseUpdater() → notify_databricks_failure()
                                                    │
                                                    ▼
                                          Logic App HTTP POST
                                          (sends Teams card)
```

No ServiceNow incident for Databricks (ADF orchestrates Databricks via Execute Pipeline activity, so the ADF failure captures the incident).

---

## 5. Power BI Dashboard Flow

```
SQL Views (DirectQuery) → Power BI Dashboard
```

| View | Dashboard Section |
|------|------------------|
| `VwRptJobsMaster` | Job listing, NotStartedFlag, platform filters |
| `VwRptJobsRuns` | Run details table, duration, long-running flags |
| `VwRptJobsFailureLogs` | Failure details tab, error messages |
| `VwRptJobsStatus` | Status slicer filters |

All times are converted from UTC to IST (UTC + 5:30) in the views for the dashboard display.
