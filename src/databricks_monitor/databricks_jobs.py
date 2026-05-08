"""
Databricks Job Monitor — Azure Function
========================================
Timer-triggered Azure Function that monitors Databricks job executions.

Workflow:
    1. Queries the SQL view [vw_DatabricksJobSchedules] for jobs due to run
    2. Calls the Databricks REST API (2.1) to get real-time job run status
    3. Updates the SQL database via the [UpdateDatabricksJobRuns] stored procedure
    4. On failure: sends a Teams alert via Logic App with deduplication
       (one alert per run_id, logged in databricks_alert_log)

Runs every 5 minutes via Azure Function Timer Trigger.
"""

import pyodbc
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date, timezone
import logging
import os
import azure.functions as func

from shared.db_utils import get_db_connection

# --- Configuration & Globals ---
# Databricks API Token (single token for all workspaces)
DB_API_TOKEN = os.environ.get("db_api_token")

# Databricks Logic App endpoint for Teams failure alerts
DB_LOGIC_APP_ENDPOINT = os.environ.get("DB_LOGIC_APP_ENDPOINT")

# --- Main Function ---
def main(mytimer: func.TimerRequest) -> None:
    """Azure Function entry point triggered by a timer."""
    start_time = datetime.now(timezone.utc)
    logging.info(f"--- Databricks monitor timer trigger executed at {start_time} UTC ---")

    try:
        # Get the list of jobs needing attention from the SQL view
        job_list_df = get_scheduled_jobs()

        if job_list_df.empty:
            logging.info("--- No Databricks jobs returned by the view in this run cycle. Exiting. ---")
        else:
            logging.info(f"--- Processing {len(job_list_df)} Databricks job(s) found by the view. ---")

            for index, row in job_list_df.iterrows():
                job_id_from_view = row.get('jobid', 'N/A')
                logging.info(f"--- Processing job index: {index} (JobId: {job_id_from_view}) ---")
                try:
                    job_id = row['jobid']
                    workspace_id = row['workspaceid']
                    data_product = str(row['dataproduct'])
                    status_from_view = row['status']         # 'NOT STARTED' or 'RUNNING'
                    run_id_from_view = row['runid']           # '0' or actual run ID
                    schedule_from_view = row['schedule']      # e.g. '09:00' or ''

                    # Log extracted details
                    logging.info(f"  Job ID: {job_id}")
                    logging.info(f"  Workspace ID: {workspace_id}")
                    logging.info(f"  Data Product: {data_product}")
                    logging.info(f"  Status from View: {status_from_view}")
                    logging.info(f"  Run ID from View: {run_id_from_view}")
                    logging.info(f"  Schedule from View: {schedule_from_view}")

                    # Query the Databricks API for the actual current status
                    job_details, error_message = get_job_status(
                        workspace_id, job_id,
                        status_from_view, run_id_from_view, schedule_from_view
                    )

                    # If get_job_status didn't find a valid/current run, skip to the next job
                    if job_details == 'job not found':
                        logging.info(f"--- Job details not found or no valid run for JobId {job_id}. Skipping database update. ---")
                        continue
                    else:
                        # If valid details were found, update the database via the stored proc
                        databaseUpdater(job_details, workspace_id, schedule_from_view, error_message, data_product)

                except KeyError as key_err:
                    logging.error(f"--- Error processing job at index {index}: Missing expected column '{key_err}' in view data. Skipping. ---")
                    continue
                except Exception as loop_error:
                    logging.error(f"--- Error processing job at index {index} (JobId: {row.get('jobid', 'N/A')}): {str(loop_error)} ---")
                    continue  # Continue to the next job

    except Exception as e:
        logging.error(f"--- A critical error occurred in the main function: {str(e)} ---")

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    logging.info(f"--- Main function finished at {end_time} UTC. Duration: {duration}. ---")


# --- Helper Functions ---
def _get_run_status_object(run_data: dict) -> dict:
    """Return the Databricks run status object, falling back to the deprecated state object.

    Databricks API versions differ in their response format:
    - Newer API (2.1+): uses 'status' with life_cycle_state
    - Older/deprecated: uses 'state' with life_cycle_state
    This helper normalizes both formats.
    """
    status_obj = run_data.get('status')
    if isinstance(status_obj, dict) and status_obj.get('life_cycle_state'):
        return status_obj

    state_obj = run_data.get('state')
    if isinstance(state_obj, dict):
        return state_obj

    return status_obj if isinstance(status_obj, dict) else {}


def _normalize_job_parameters(job_parameters) -> dict:
    """Normalize Databricks job parameters into a dictionary for serialization.

    Handles various parameter formats returned by the Databricks API:
    - dict: returned as-is
    - list of dicts: converted using name/key/value patterns
    - other: wrapped in a simple dict
    """
    if job_parameters is None:
        return {}

    if isinstance(job_parameters, dict):
        return job_parameters

    if isinstance(job_parameters, list):
        normalized_parameters = {}
        for index, item in enumerate(job_parameters):
            if isinstance(item, dict):
                key = (
                    item.get('name')
                    or item.get('key')
                    or item.get('parameter_key')
                    or item.get('parameterKey')
                    or f'param_{index}'
                )

                value = item.get('value')
                if value is None:
                    value = item.get('default')

                if value is None and len(item) == 1:
                    value = next(iter(item.values()))

                normalized_parameters[str(key)] = value
            else:
                normalized_parameters[f'param_{index}'] = item

        return normalized_parameters

    return {'value': job_parameters}


def get_scheduled_jobs() -> pd.DataFrame:
    """Retrieves Databricks jobs scheduled to run from the SQL View."""
    conn = None
    try:
        conn = get_db_connection()
        query = "SELECT JobId AS jobid, WorkspaceId AS workspaceid, DataProduct AS dataproduct, Status AS status, RunId AS runid, Schedule AS schedule FROM [jobmonitoring].[vw_DatabricksJobSchedules]"
        jobs_df = pd.read_sql(query, conn)
        logging.info(f"--- get_scheduled_jobs: Found {len(jobs_df)} Databricks jobs in the view. ---")
        return jobs_df
    except Exception as ex:
        logging.error(f"Failed to retrieve scheduled Databricks jobs: {str(ex)}")
        return pd.DataFrame()  # Return empty dataframe on error
    finally:
        if conn:
            conn.close()


def get_job_status(workspace_id: str, job_id: str, status: str,
                   run_id: str, schedule_time_str: str) -> tuple:
    """Calls the Databricks REST API to get the current status of a job run.
    
    Args:
        workspace_id: Databricks workspace ID (domain)
        job_id: Databricks job ID
        status: From view: 'NOT STARTED' or active states (RUNNING, PENDING, QUEUED, TERMINATING)
        run_id: From view: '0' for NOT STARTED, actual run_id for active states
        schedule_time_str: Expected schedule time (e.g. '09:00') or empty string
    
    Returns:
        (run_dict, error_message): run dict from API or 'job not found' string, and error details if failed
        
    Note: Databricks timestamps are epoch milliseconds; SQL stored proc converts them to DATETIME2.
    """
    error_message = ''
    databricks_instance = f'https://{workspace_id}.azuredatabricks.net'

    if not DB_API_TOKEN:
        logging.error("--- Databricks API token (db_api_token) is not configured. Skipping. ---")
        return 'job not found', ''

    headers = {
        "Authorization": f"Bearer {DB_API_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        if status == 'NOT STARTED':
            logging.info(f"--- get_job_status: Status is 'NOT STARTED' for JobId {job_id}. Querying Databricks API... ---")

            # Using Jobs API 2.1 - queries the most recent run for this job
            # This is appropriate for checking if a scheduled job has started
            # API 2.1 endpoint: /api/2.1/jobs/runs/list
            # limit=1 gives us the single most recent run for this job.
            url = f"{databricks_instance}/api/2.1/jobs/runs/list?job_id={job_id}&limit=1"
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(f"--- Databricks API error for JobId {job_id}: Status {response.status_code}, Response: {response.text[:200]} ---")
                return 'job not found', ''

            response_data = response.json()
            runs = response_data.get('runs', [])

            if not runs:
                logging.info(f"--- No runs found for JobId {job_id}. ---")
                return 'job not found', ''

            # Extract the single latest run from the runs array
            latest_run = runs[0]

            # --- Schedule Validation ---
            # For jobs with multiple schedules (e.g. 09:00, 14:00), we must verify
            # the latest run belongs to the CURRENT schedule, not a previous one.
            # Without this, we could log the 09:00 run under the 14:00 schedule.
            if schedule_time_str and schedule_time_str.strip():
                try:
                    schedule_time_obj = datetime.strptime(schedule_time_str.strip(), '%H:%M').time()
                    now_utc = datetime.now(timezone.utc)
                    schedule_datetime_utc = datetime.combine(now_utc.date(), schedule_time_obj)

                    # Handle midnight crossing: if schedule (e.g. 22:00) is in the future
                    # relative to now (e.g. 02:30), the run belongs to yesterday.
                    if schedule_datetime_utc > now_utc.replace(tzinfo=None):
                        schedule_datetime_utc = schedule_datetime_utc - timedelta(days=1)

                    # Check if the latest run started AFTER the schedule time
                    run_start_epoch_ms = latest_run.get('start_time', 0)
                    if run_start_epoch_ms:
                        run_start_utc = datetime.fromtimestamp(run_start_epoch_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                        logging.info(f"  Schedule target: {schedule_datetime_utc}, Run started: {run_start_utc}")

                        if run_start_utc < schedule_datetime_utc:
                            # Latest run is from a previous schedule; this schedule hasn't started yet
                            logging.info(f"--- Latest run for JobId {job_id} is from before schedule time. Job not started for this schedule. ---")
                            return 'job not found', ''

                except ValueError:
                    # If schedule can't be parsed, accept the latest run as-is
                    logging.warning(f"--- Could not parse schedule '{schedule_time_str}'. Accepting latest run. ---")

            job_result = latest_run
            logging.info(f"--- Found run for JobId {job_id}: RunId {job_result.get('run_id')} ---")

        elif status in ('RUNNING', 'QUEUED', 'PENDING', 'TERMINATING'):
            # For active runs, we already have the run_id from the view (jobRuns table).
            # Just call runs/get to fetch the current state of that specific run.
            logging.info(f"--- get_job_status: Status is '{status}' for RunId {run_id}. Fetching from Databricks... ---")
            url = f"{databricks_instance}/api/2.1/jobs/runs/get?run_id={run_id}"
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(f"--- Failed to get RunId {run_id}: Status {response.status_code}, Response: {response.text[:200]} ---")
                return 'job not found', ''

            job_result = response.json()

        else:
            logging.warning(f"--- Unexpected status '{status}' from view for JobId {job_id}. Skipping. ---")
            return 'job not found', ''

        # --- Extract error message if failed ---
        # Use the same fallback helper that databaseUpdater uses (status → state)
        status_obj = _get_run_status_object(job_result)
        life_cycle_state = status_obj.get('life_cycle_state', '')
        result_state = status_obj.get('result_state', '')

        # Capture error details for failed or timed-out runs
        if result_state in ('FAILED', 'TIMEDOUT') or life_cycle_state == 'INTERNAL_ERROR':
            # Log raw status objects for debugging
            logging.info(f"  Error extraction — status field: {job_result.get('status')}")
            logging.info(f"  Error extraction — state field: {job_result.get('state')}")

            # Try state_message first (current format)
            error_message = status_obj.get('state_message', '')

            # Fallback: termination_details.message
            if not error_message:
                termination_details = status_obj.get('termination_details', {})
                error_message = termination_details.get('message', '')

            # Fallback: For multi-task jobs, check individual task errors
            if not error_message:
                tasks = job_result.get('tasks', [])
                task_errors = []
                for task in tasks:
                    task_status = _get_run_status_object(task)
                    task_result_state = task_status.get('result_state', '')
                    if task_result_state in ('FAILED', 'TIMEDOUT'):
                        task_error = task_status.get('state_message', '')
                        if not task_error:
                            task_term = task_status.get('termination_details', {})
                            task_error = task_term.get('message', '')
                        if task_error:
                            task_key = task.get('task_key', 'unknown_task')
                            task_errors.append(f"{task_key}: {task_error}")
                if task_errors:
                    error_message = "; ".join(task_errors[:3])  # Limit to first 3 task errors

            if not error_message:
                logging.warning(f"  No error message found in any field for JobId {job_result.get('job_id')}. Raw keys: {list(job_result.keys())}")
                error_message = 'No failure message available'

            error_message = error_message.replace("'", "''")

        return job_result, error_message

    except requests.exceptions.RequestException as req_err:
        logging.error(f"--- Network error calling Databricks API for JobId {job_id}: {str(req_err)} ---")
        return 'job not found', ''
    except Exception as ex:
        logging.error(f"--- Unexpected error in get_job_status for JobId {job_id}: {str(ex)} ---")
        return 'job not found', ''


def databaseUpdater(json_data: dict, workspace_id: str, schedule: str,
                    error_message_from_api: str, data_product: str = '') -> None:
    """Parses the Databricks run dict and updates the SQL database via stored proc.

    Expects a single Databricks run dict (from either runs/list or runs/get).
    Uses parameterized queries to prevent SQL injection.
    Also handles failure alerting with deduplication via databricks_alert_log.
    """
    conn = None
    cursor = None

    try:
        # --- Extract fields from the Databricks run dict ---
        # Both runs/list (single element) and runs/get return the same run structure.
        run_data = json_data

        JobId = str(run_data.get('job_id', ''))
        JobRunId = str(run_data.get('run_id', ''))
        RunPageURL = str(run_data.get('run_page_url', ''))

        # --- Determine Status from Databricks two-level status object ---
        # Databricks API uses:
        #   - life_cycle_state: current execution phase (RUNNING, PENDING, QUEUED, TERMINATING, TERMINATED, INTERNAL_ERROR, SKIPPED)
        #   - result_state (if TERMINATED): how it ended (SUCCESS, FAILED, TIMEDOUT, CANCELED)
        # This is different from ADF which has a single status field.
        # NOTE: Databricks may return either 'status' or the older deprecated 'state' object.
        status_obj = _get_run_status_object(run_data)
        life_cycle_state = status_obj.get('life_cycle_state', '')

        if life_cycle_state in ('RUNNING', 'PENDING', 'QUEUED', 'TERMINATING'):
            # Active states — stored proc will normalize these to standard values
            Status = life_cycle_state
        elif life_cycle_state == 'TERMINATED':
            # Terminal state — check result_state to determine outcome
            Status = status_obj.get('result_state', 'UNKNOWN')  # SUCCESS, FAILED, TIMEDOUT, CANCELED
        elif life_cycle_state == 'INTERNAL_ERROR':
            # Databricks internal error — treat as failure
            Status = 'FAILED'
        elif life_cycle_state == 'SKIPPED':
            # Job was skipped (e.g., due to condition)
            Status = 'SKIPPED'
        else:
            # Unexpected state — log for debugging
            Status = 'UNKNOWN'
            logging.warning(f"Unexpected life_cycle_state '{life_cycle_state}' for JobId {JobId}")

        # --- Timestamps ---
        # Databricks returns epoch milliseconds (e.g. 1714567890000).
        # Pass as strings — the stored proc converts them with DATEADD(SECOND, .../1000, '1970-01-01').
        StartTime = str(run_data.get('start_time', '0'))
        EndTime = str(run_data.get('end_time', '0'))

        # --- Error details ---
        error_code = ''
        if Status in ('FAILED', 'TIMEDOUT'):
            termination_details = status_obj.get('termination_details', {})
            error_code = str(termination_details.get('code', ''))

        # --- Job Parameters (with sensitive data masking) ---
        # Mask potentially sensitive parameter values (passwords, secrets, API keys)
        job_parameters = _normalize_job_parameters(run_data.get('job_parameters', {}))
        
        # Mask sensitive keys to avoid logging secrets
        sensitive_keys = ['password', 'secret', 'token', 'api_key', 'apikey', 'auth', 'credential']
        masked_parameters = {}
        for key, value in job_parameters.items():
            if any(sensitive_key in key.lower() for sensitive_key in sensitive_keys):
                masked_parameters[key] = '***MASKED***'
            else:
                masked_parameters[key] = value
        
        job_parameter_str = json.dumps(masked_parameters).replace("'", "''") if masked_parameters else ''

        Instance = workspace_id
        job_schedule = schedule if schedule else ''

        if not JobId:
            logging.warning("Skipped SQL update because JobId was missing.")
            return

        # --- Execute Stored Procedure ---
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            EXEC [jobmonitoring].[UpdateDatabricksJobRuns]
                @JobId = ?, @JobRunId = ?, @Status = ?,
                @StartTime = ?, @EndTime = ?, @RunPageURL = ?,
                @ErrorCode = ?, @ErrorMessage = ?, @Instance = ?,
                @Schedule = ?, @JobParameters = ?;
        """
        params = (
            JobId, JobRunId, Status,
            StartTime, EndTime, RunPageURL,
            error_code, error_message_from_api if error_message_from_api else None,
            Instance, job_schedule, job_parameter_str
        )

        logging.info(f"--- Executing SPROC for JobId: {JobId}, RunId: {JobRunId}, Status: {Status} ---")
        cursor.execute(query, params)
        conn.commit()
        logging.info(f"--- SPROC commit successful for RunId: {JobRunId}. ---")

        # --- Send Teams Alert on Failure ---
        if Status in ('FAILED', 'TIMEDOUT'):
            try:
                # Deduplication: Check if alert already sent for this run_id
                cursor.execute(
                    "SELECT 1 FROM [jobmonitoring].[databricks_alert_log] WHERE run_id = ?",
                    JobRunId
                )
                if not cursor.fetchone():
                    # Get job name from the run data
                    job_name = str(run_data.get('run_name', '')) or f'Job {JobId}'

                    notify_databricks_failure(
                        data_product=data_product,
                        job_name=job_name,
                        job_id=JobId,
                        error_message=error_message_from_api if error_message_from_api else 'No failure message available',
                        run_page_url=RunPageURL
                    )

                    # Log alert to prevent duplicates
                    cursor.execute(
                        "INSERT INTO [jobmonitoring].[databricks_alert_log] (job_id, run_id, workspace_id) VALUES (?, ?, ?)",
                        JobId, JobRunId, workspace_id
                    )
                    conn.commit()
                    logging.info(f"--- Alert sent and logged for JobId: {JobId}, RunId: {JobRunId} ---")
                else:
                    logging.info(f"--- Alert already sent for RunId: {JobRunId}. Skipping. ---")
            except Exception as alert_err:
                logging.error(f"--- Failed to send/log Databricks alert for RunId {JobRunId}: {str(alert_err)} ---")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0] if ex.args else 'N/A'
        logging.error(f"--- SQL ERROR: SQLSTATE={sqlstate}, Details: {str(ex)} ---")

    except Exception as e:
        logging.error(f"--- NON-SQL ERROR in databaseUpdater: {str(e)} ---")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def notify_databricks_failure(data_product: str, job_name: str, job_id: str,
                              error_message: str, run_page_url: str) -> None:
    """Sends a Databricks failure alert to Microsoft Teams via Azure Logic App.

    The Logic App renders an Adaptive Card showing data product, job name,
    job ID, failure reason, and a direct link to the Databricks job run.
    """
    try:
        if not DB_LOGIC_APP_ENDPOINT:
            logging.warning("--- DB_LOGIC_APP_ENDPOINT not configured. Skipping Databricks alert. ---")
            return

        payload = {
            "dataProduct": data_product if data_product else "Unknown",
            "jobName": job_name,
            "jobId": job_id,
            "failureReason": error_message,
            "runUrl": run_page_url
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(DB_LOGIC_APP_ENDPOINT, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"--- Databricks alert sent successfully for JobId {job_id} ---")

    except requests.exceptions.RequestException as ex:
        logging.error(f"--- Failed to send Databricks alert for JobId {job_id}: {str(ex)} ---")
    except Exception as ex:
        logging.error(f"--- Unexpected error sending Databricks alert for JobId {job_id}: {str(ex)} ---")
