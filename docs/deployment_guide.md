# Deployment Guide

## Prerequisites

- Azure subscription with the following services provisioned:
  - Azure Functions (Python 3.9+)
  - Azure SQL Database
  - Azure Logic Apps (2 workflows — one for ADF alerts, one for Databricks alerts)
  - Power BI Pro or Premium license
- Service Principal with appropriate ADF and Databricks access
- ServiceNow API access (for incident creation)

---

## Step 1: Database Setup

### 1.1 Create Schema
```sql
CREATE SCHEMA jobmonitoring;
```

### 1.2 Create Tables

#### ADFJobsMaster (ADF Pipeline Registration)
```sql
CREATE TABLE [jobmonitoring].[ADFJobsMaster] (
    PipelineName NVARCHAR(512) NOT NULL,
    DataFactory NVARCHAR(512) NOT NULL,
    SubscriptionId NVARCHAR(100),
    TenantId NVARCHAR(100),
    CIID NVARCHAR(100),
    ClientSecret NVARCHAR(512),
    ResourceGroup NVARCHAR(512),
    DataProduct NVARCHAR(256),
    Schedule NVARCHAR(512),          -- Comma-separated UTC times: '09:00,14:00,22:00'
    JobType NVARCHAR(50) DEFAULT 'ADF',
    IsTracking CHAR(1) DEFAULT 'Y',  -- Y = active monitoring, N = paused
    JobURL NVARCHAR(2000),
    JobName NVARCHAR(512),
    EstimatedDuration INT,           -- Expected duration in minutes
    incident_creation INT DEFAULT 1  -- 1 = enabled, 0 = disabled
);
```

#### jobsMaster (Databricks Job Registration)
```sql
CREATE TABLE [jobmonitoring].[jobsMaster] (
    JobId NVARCHAR(512) NOT NULL,
    WorkspaceId NVARCHAR(512) NOT NULL,
    DataProduct NVARCHAR(256),
    Schedule NVARCHAR(512),
    JobType NVARCHAR(50) DEFAULT 'Databricks',
    IsTracking CHAR(1) DEFAULT 'Y',
    JobURL NVARCHAR(2000),
    JobName NVARCHAR(512),
    EstimatedDuration INT
);
```

#### DataProductConfig (Product Metadata)
```sql
CREATE TABLE [jobmonitoring].[DataProductConfig] (
    DataProduct NVARCHAR(256) PRIMARY KEY,
    POC NVARCHAR(256),               -- Point of Contact name
    POC_MUDID NVARCHAR(100),         -- Assignee ID for ServiceNow
    CMDB_CI NVARCHAR(512)            -- Configuration Item for ServiceNow
);
```

#### jobRuns (Current Day Runs)
```sql
CREATE TABLE [jobmonitoring].[jobRuns] (
    JobId NVARCHAR(512),
    JobRunId NVARCHAR(512) PRIMARY KEY,
    Status NVARCHAR(100),
    StartTime DATETIME2(3),
    EndTime DATETIME2(3),
    CreatedDatetime DATETIME2(3),
    ModifiedDatetime DATETIME2(3),
    AsOfDate DATE,
    RunPageURL NVARCHAR(4000),
    InstanceName NVARCHAR(512),
    JobParameters NVARCHAR(MAX),
    Schedule NVARCHAR(100)
);
```

#### Other Tables
```sql
CREATE TABLE [jobmonitoring].[JobRunsHistory] (
    -- Same schema as jobRuns
    JobId NVARCHAR(512), JobRunId NVARCHAR(512), Status NVARCHAR(100),
    StartTime DATETIME2(3), EndTime DATETIME2(3), ErrorMessage NVARCHAR(MAX),
    CreatedDatetime DATETIME2(3), ModifiedDatetime DATETIME2(3),
    AsOfDate DATE, RunPageURL NVARCHAR(4000), InstanceName NVARCHAR(512)
);

CREATE TABLE [jobmonitoring].[failureLogs] (
    JobId NVARCHAR(512), JobRunId NVARCHAR(512), ErrorCode NVARCHAR(512),
    ErrorMessage NVARCHAR(MAX), StartTime DATETIME2(3), EndTime DATETIME2(3),
    FailedTime DATETIME2(3), CreatedDatetime DATETIME2(3), ModifiedDatetime DATETIME2(3),
    AsOfDate DATE, RunPageURL NVARCHAR(4000), InstanceName NVARCHAR(512),
    Schedule NVARCHAR(100), JobParameters NVARCHAR(MAX)
);

CREATE TABLE [jobmonitoring].[incident_log] (
    instance NVARCHAR(512), pipeline NVARCHAR(512),
    incident NVARCHAR(100), created_dt DATETIME2(3)
);

CREATE TABLE [jobmonitoring].[databricks_alert_log] (
    job_id NVARCHAR(512), run_id NVARCHAR(512), workspace_id NVARCHAR(512),
    created_dt DATETIME2(3) DEFAULT GETUTCDATE()
);

CREATE TABLE [jobmonitoring].[jobStatus] (
    Status NVARCHAR(50), StatusDesc NVARCHAR(100)
);

INSERT INTO [jobmonitoring].[jobStatus] VALUES
('SUCCESS', 'Success'), ('FAILURE', 'Failure'),
('RUNNING', 'Running'), ('NOT STARTED', 'Not Started');
```

### 1.3 Deploy Views and Stored Procedures
Run the SQL scripts in order:
1. `sql/views/vw_adf_job_schedules.sql`
2. `sql/views/vw_databricks_job_schedules.sql`
3. `sql/views/vw_rpt_jobs_master.sql`
4. `sql/views/vw_rpt_jobs_runs.sql`
5. `sql/views/vw_rpt_jobs_failure_logs.sql`
6. `sql/views/vw_rpt_jobs_status.sql`
7. `sql/stored_procedures/usp_update_adf_job_runs.sql`
8. `sql/stored_procedures/usp_update_databricks_job_runs.sql`

---

## Step 2: Azure Functions Setup

### 2.1 Create Function App
- Runtime: Python 3.9+
- Plan: Consumption or Premium (depending on execution frequency needs)
- Region: Same as your SQL Server and ADF instances

### 2.2 Configure Environment Variables

| Variable | Description | Example |
|----------|------------|---------|
| `SQLSERVER_SERVER` | SQL Server hostname | `your-server.database.windows.net` |
| `SQLSERVER_DATABASE` | Database name | `monitoring-db` |
| `SQLSERVER_USERNAME` | SQL username | `admin@domain.com` |
| `SQLSERVER_PASSWORD` | SQL password | `***` |
| `adf_client_id` | Service Principal Client ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `adf_client_secret` | Service Principal Secret | `***` |
| `db_api_token` | Databricks PAT token | `dapi...` |
| `LOGIC_APP_ENDPOINT_PRD` | ADF alert Logic App URL | `https://prod-xx.logic.azure.com/...` |
| `DB_LOGIC_APP_ENDPOINT` | Databricks alert Logic App URL | `https://prod-xx.logic.azure.com/...` |
| `apikey` | ServiceNow API key | `***` |
| `SERVICENOW_URL` | ServiceNow incident API URL | `https://api.enterprise.com/...` |
| `SERVICENOW_CALLER_ID` | ServiceNow caller ID | `automation_svc_account` |

### 2.3 Deploy Functions
Deploy the three functions with their respective timer triggers:

| Function | CRON Expression | Schedule |
|----------|----------------|----------|
| `adf_monitor` | `0 */5 * * * *` | Every 5 minutes |
| `databricks_monitor` | `0 */5 * * * *` | Every 5 minutes |
| `recheck_monitor` | `0 */15 * * * *` | Every 15 minutes |

---

## Step 3: Logic App Configuration

### 3.1 ADF Alert Logic App
1. Create a Logic App with HTTP trigger
2. Parse the incoming JSON payload:
   ```json
   {
     "incidentNumber": "INC123456",
     "source": "ADF",
     "adfName": "factory-name",
     "pipelineName": "pipeline-name",
     "errorMessage": "...",
     "dataProduct": "Product A",
     "pocName": "Anantha Sai Jinde",
     "runUrl": "https://adf.azure.com/..."
   }
   ```
3. Post an Adaptive Card to the designated Teams channel

### 3.2 Databricks Alert Logic App
1. Create a Logic App with HTTP trigger
2. Parse the incoming JSON payload:
   ```json
   {
     "dataProduct": "Product B",
     "jobName": "etl-daily-load",
     "jobId": "123456",
     "failureReason": "...",
     "runUrl": "https://adb-xxx.azuredatabricks.net/..."
   }
   ```
3. Post an Adaptive Card to the designated Teams channel

---

## Step 4: Power BI Dashboard

1. Open Power BI Desktop
2. Connect to Azure SQL Database using **DirectQuery** mode
3. Add the four dashboard views as data sources:
   - `jobmonitoring.VwRptJobsMaster`
   - `jobmonitoring.VwRptJobsRuns`
   - `jobmonitoring.VwRptJobsFailureLogs`
   - `jobmonitoring.VwRptJobsStatus`
4. Build the dashboard with the following components:
   - Status KPI cards (Running, Completed, Not Started, Failed)
   - Platform toggle (ADF / Databricks)
   - Data Product slicer
   - Job Status slicer
   - Long Running Jobs panel
   - Detail table with run page URL links
5. Publish to Power BI Service
6. Configure scheduled refresh (or rely on DirectQuery for real-time)

---

## Step 5: Register Jobs for Monitoring

### Add an ADF Pipeline
```sql
INSERT INTO [jobmonitoring].[ADFJobsMaster] (
    PipelineName, DataFactory, SubscriptionId, TenantId,
    ResourceGroup, DataProduct, Schedule, IsTracking, incident_creation
) VALUES (
    'pl_daily_etl', 'adf-production', 'sub-id-here', 'tenant-id-here',
    'rg-data-prod', 'Sales Analytics', '09:00,21:00', 'Y', 1
);
```

### Add a Databricks Job
```sql
INSERT INTO [jobmonitoring].[jobsMaster] (
    JobId, WorkspaceId, DataProduct, Schedule, IsTracking
) VALUES (
    '123456', 'adb-workspace-id', 'Sales Analytics', '09:30', 'Y'
);
```

### Add Data Product Config (for incident routing)
```sql
INSERT INTO [jobmonitoring].[DataProductConfig] (
    DataProduct, POC, POC_MUDID, CMDB_CI
) VALUES (
    'Sales Analytics', 'Anantha Sai Jinde', 'aj123456',
    'Enterprise Data Analytics - Prod'
);
```
