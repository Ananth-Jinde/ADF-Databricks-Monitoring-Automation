"""
Recheck Failed Jobs — Azure Function (ADF + Databricks)
=======================================================
Timer-triggered Azure Function that rechecks failed pipelines and jobs.

When a pipeline/job fails, the dashboard shows it as FAILURE. If someone
manually re-triggers the job and it succeeds, this function automatically
detects the successful re-trigger and updates the dashboard to reflect
the corrected status.

Workflow:
    1. Query jobRuns table for all entries with Status = 'FAILURE' today
    2. For each failure, call the respective API (ADF or Databricks)
       to check if a new successful run exists after the failure time
    3. If a successful re-trigger is found:
       - Update jobRuns status to 'SUCCESS' with new run details
       - Delete the entry from failureLogs
    4. Dashboard auto-refreshes to show the corrected status

Covers both:
    - ADF pipelines (via ADF Management API)
    - Databricks jobs (via Databricks REST API)

Runs every 15 minutes via Azure Function Timer Trigger.
"""

import pyodbc
import requests
import json
import os
import logging
from datetime import datetime, timedelta, timezone
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
import azure.functions as func

from shared.db_utils import get_db_connection

# --- Configuration (same env vars as existing functions) ---
ADF_CLIENT_ID = os.environ.get("adf_client_id")
ADF_CLIENT_SECRET = os.environ.get("adf_client_secret")
DB_API_TOKEN = os.environ.get("db_api_token")


def _get_run_status_object(run_data: dict) -> dict:
    """Return the Databricks run status object, falling back to the deprecated state object.

    Shared helper for extracting status from Databricks API responses which may
    use either 'status' (newer) or 'state' (deprecated) field names.
    """
    status_obj = run_data.get('status')
    if isinstance(status_obj, dict) and status_obj.get('life_cycle_state'):
        return status_obj

    state_obj = run_data.get('state')
    if isinstance(state_obj, dict):
        return state_obj

    return status_obj if isinstance(status_obj, dict) else {}


def main(mytimer: func.TimerRequest) -> None:
    """Azure Function entry point. Rechecks failed ADF pipelines and Databricks jobs."""
    start_time = datetime.now(timezone.utc)
    logging.info(f"--- Recheck Failed Jobs: Started at {start_time} UTC ---")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # ============================================================
        # SECTION 1: RECHECK FAILED ADF PIPELINES
        # ============================================================
        logging.info("--- Recheck: === Checking ADF Pipelines === ---")
        adf_updated = recheck_failed_adf(cursor, conn)

        # ============================================================
        # SECTION 2: RECHECK FAILED DATABRICKS JOBS
        # ============================================================
        logging.info("--- Recheck: === Checking Databricks Jobs === ---")
        db_updated = recheck_failed_databricks(cursor, conn)

        total_updated = adf_updated + db_updated
        logging.info(f"--- Recheck: Completed. Total updated: {total_updated} (ADF: {adf_updated}, Databricks: {db_updated}). ---")

    except Exception as ex:
        logging.error(f"--- Recheck: Critical error — {str(ex)} ---")
    finally:
        if conn:
            conn.close()

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    logging.info(f"--- Recheck Failed Jobs: Finished at {end_time} UTC. Duration: {duration}. ---")


# ============================================================
# ADF RECHECK
# ============================================================
def recheck_failed_adf(cursor, conn) -> int:
    """Rechecks failed ADF pipelines by querying the ADF API for successful re-triggers.

    Returns:
        int: Number of pipelines updated from FAILURE to SUCCESS.
    """

    failed_query = """
        SELECT
            jr.JobId,
            jr.JobRunId,
            jr.StartTime,
            jr.EndTime,
            jr.Schedule,
            jr.InstanceName,
            jm.SubscriptionId,
            jm.TenantId,
            jm.ResourceGroup,
            jm.DataFactory
        FROM [jobmonitoring].[jobRuns] jr WITH (NOLOCK)
        INNER JOIN [jobmonitoring].[ADFJobsMaster] jm WITH (NOLOCK)
            ON jr.JobId = jm.PipelineName AND jr.InstanceName = jm.DataFactory
        WHERE jr.Status = 'FAILURE'
          AND jr.AsOfDate = CAST(GETUTCDATE() AS DATE)
          AND jm.IsTracking = 'Y'
          AND jm.JobType = 'ADF'
    """
    cursor.execute(failed_query)
    failed_jobs = cursor.fetchall()

    if not failed_jobs:
        logging.info("--- Recheck ADF: No failed ADF pipelines found today. ---")
        return 0

    logging.info(f"--- Recheck ADF: Found {len(failed_jobs)} failed pipeline(s) to recheck. ---")
    updated_count = 0

    for job in failed_jobs:
        pipeline_name = job[0]
        failed_run_id = job[1]
        failed_start_time = job[2]
        failed_end_time = job[3]
        schedule = job[4]
        instance_name = job[5]
        subscription_id = job[6]
        tenant_id = job[7]
        resource_group = job[8]
        data_factory = job[9]

        try:
            logging.info(f"--- Recheck ADF: Checking '{pipeline_name}' (Factory: {data_factory}, Schedule: {schedule}) ---")

            # Create ADF client
            credentials = ClientSecretCredential(
                client_id=ADF_CLIENT_ID,
                client_secret=ADF_CLIENT_SECRET,
                tenant_id=tenant_id
            )
            adf_client = DataFactoryManagementClient(credentials, subscription_id)

            # Search from failure time to now
            search_after = failed_end_time if failed_end_time else failed_start_time
            if search_after is None:
                logging.warning(f"--- Recheck ADF: No start/end time for '{pipeline_name}'. Skipping. ---")
                continue

            now_utc = datetime.now(timezone.utc)
            last_updated_after = search_after.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
            last_updated_before = now_utc.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'

            runs = adf_client.pipeline_runs.query_by_factory(
                factory_name=data_factory,
                resource_group_name=resource_group,
                filter_parameters={
                    "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [pipeline_name]}],
                    "lastUpdatedAfter": last_updated_after,
                    "lastUpdatedBefore": last_updated_before
                }
            )

            if not runs.value:
                logging.info(f"--- Recheck ADF: No new runs found for '{pipeline_name}' after failure. ---")
                continue

            # Find a successful re-trigger (Succeeded, not in jobRuns)
            retrigger_run = None
            candidate_runs = sorted(
                [r for r in runs.value if r.status == "Succeeded" and r.run_id != failed_run_id and r.run_start],
                key=lambda x: x.run_start
            )

            for run in candidate_runs:
                run_start_naive = run.run_start.replace(tzinfo=None) if run.run_start.tzinfo else run.run_start
                search_after_naive = search_after.replace(tzinfo=None) if hasattr(search_after, 'tzinfo') and search_after.tzinfo else search_after

                if run_start_naive < search_after_naive:
                    continue

                # Key check: If this run_id is already in jobRuns, it's a scheduled run → skip
                cursor.execute("SELECT 1 FROM [jobmonitoring].[jobRuns] WHERE JobRunId = ?", run.run_id)
                if cursor.fetchone():
                    logging.info(f"  RunId {run.run_id} already in jobRuns (scheduled run). Skipping.")
                    continue

                retrigger_run = run
                logging.info(f"  Found re-trigger! RunId: {run.run_id}, Start: {run.run_start}")
                break

            if not retrigger_run:
                logging.info(f"--- Recheck ADF: No successful re-trigger found for '{pipeline_name}'. ---")
                continue

            # Update jobRuns and delete from failureLogs
            new_run_id = retrigger_run.run_id
            new_start_time = retrigger_run.run_start.strftime('%Y-%m-%d %H:%M:%S.') + f"{retrigger_run.run_start.microsecond // 1000:03d}" if retrigger_run.run_start else None
            new_end_time = retrigger_run.run_end.strftime('%Y-%m-%d %H:%M:%S.') + f"{retrigger_run.run_end.microsecond // 1000:03d}" if retrigger_run.run_end else None

            factory_resource_id = (
                f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
                f"/providers/Microsoft.DataFactory/factories/{data_factory}"
            )
            new_run_page_url = f"https://adf.azure.com/monitoring/pipelineruns/{new_run_id}?factory={factory_resource_id}"

            update_query = """
                UPDATE [jobmonitoring].[jobRuns]
                SET Status = 'SUCCESS',
                    JobRunId = ?,
                    StartTime = ?,
                    EndTime = ?,
                    RunPageURL = ?,
                    ModifiedDatetime = GETUTCDATE()
                WHERE JobRunId = ?
            """
            cursor.execute(update_query, new_run_id, new_start_time, new_end_time, new_run_page_url, failed_run_id)

            delete_query = "DELETE FROM [jobmonitoring].[failureLogs] WHERE JobRunId = ?"
            cursor.execute(delete_query, failed_run_id)

            conn.commit()
            updated_count += 1
            logging.info(f"--- Recheck ADF: ✅ Updated '{pipeline_name}' (Schedule: {schedule}) from FAILURE → SUCCESS. New RunId: {new_run_id} ---")

        except Exception as job_error:
            logging.error(f"--- Recheck ADF: Error processing '{pipeline_name}': {str(job_error)} ---")
            continue

    return updated_count


# ============================================================
# DATABRICKS RECHECK
# ============================================================
def recheck_failed_databricks(cursor, conn) -> int:
    """Rechecks failed Databricks jobs by querying the Databricks REST API for successful re-triggers.

    Returns:
        int: Number of jobs updated from FAILURE to SUCCESS.
    """

    if not DB_API_TOKEN:
        logging.warning("--- Recheck Databricks: API token (db_api_token) not configured. Skipping. ---")
        return 0

    failed_query = """
        SELECT
            jr.JobId,
            jr.JobRunId,
            jr.StartTime,
            jr.EndTime,
            jr.Schedule,
            jr.InstanceName
        FROM [jobmonitoring].[jobRuns] jr WITH (NOLOCK)
        INNER JOIN [jobmonitoring].[jobsMaster] jm WITH (NOLOCK)
            ON jr.JobId = jm.JobId AND jr.InstanceName = jm.WorkspaceId
        WHERE jr.Status = 'FAILURE'
          AND jr.AsOfDate = CAST(GETUTCDATE() AS DATE)
          AND jm.IsTracking = 'Y'
          AND jm.JobType = 'Databricks'
    """
    cursor.execute(failed_query)
    failed_jobs = cursor.fetchall()

    if not failed_jobs:
        logging.info("--- Recheck Databricks: No failed Databricks jobs found today. ---")
        return 0

    logging.info(f"--- Recheck Databricks: Found {len(failed_jobs)} failed job(s) to recheck. ---")
    updated_count = 0

    headers = {
        "Authorization": f"Bearer {DB_API_TOKEN}",
        "Content-Type": "application/json"
    }

    for job in failed_jobs:
        job_id = job[0]          # This is the Databricks job_id (e.g. "123456")
        failed_run_id = job[1]   # Databricks run_id stored in jobRuns
        failed_start_time = job[2]
        failed_end_time = job[3]
        schedule = job[4]
        workspace_id = job[5]    # e.g. "adb-1234567890.12"

        try:
            logging.info(f"--- Recheck Databricks: Checking JobId '{job_id}' (Workspace: {workspace_id}, Schedule: {schedule}) ---")

            databricks_instance = f"https://{workspace_id}.azuredatabricks.net"

            # Determine search-after time (use failure end time, fallback to start time)
            search_after = failed_end_time if failed_end_time else failed_start_time
            if search_after is None:
                logging.warning(f"--- Recheck Databricks: No start/end time for JobId '{job_id}'. Skipping. ---")
                continue

            # Convert search_after to epoch milliseconds for Databricks API comparison
            if hasattr(search_after, 'tzinfo') and search_after.tzinfo:
                search_after_epoch_ms = int(search_after.timestamp() * 1000)
            else:
                search_after_epoch_ms = int(search_after.replace(tzinfo=timezone.utc).timestamp() * 1000)

            # Query Databricks API for recent runs of this job
            url = f"{databricks_instance}/api/2.1/jobs/runs/list?job_id={job_id}&limit=10"
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(f"--- Recheck Databricks: API error for JobId {job_id}: Status {response.status_code} ---")
                continue

            response_data = response.json()
            runs = response_data.get('runs', [])

            if not runs:
                logging.info(f"--- Recheck Databricks: No runs found for JobId '{job_id}'. ---")
                continue

            # Find a successful re-trigger
            retrigger_run = None

            for run in runs:
                run_id_str = str(run.get('run_id', ''))

                # Skip the original failed run
                if run_id_str == str(failed_run_id):
                    continue

                # Check if this run succeeded
                # Use shared helper for status extraction
                status_obj = _get_run_status_object(run)
                life_cycle_state = status_obj.get('life_cycle_state', '')
                result_state = status_obj.get('result_state', '')

                if life_cycle_state != 'TERMINATED' or result_state != 'SUCCESS':
                    continue

                # Check if this run started AFTER the failure
                run_start_epoch_ms = run.get('start_time', 0)
                if run_start_epoch_ms < search_after_epoch_ms:
                    continue

                # Key check: If this run_id is already in jobRuns, it's a scheduled run → skip
                cursor.execute("SELECT 1 FROM [jobmonitoring].[jobRuns] WHERE JobRunId = ?", run_id_str)
                if cursor.fetchone():
                    logging.info(f"  RunId {run_id_str} already in jobRuns (scheduled run). Skipping.")
                    continue

                # Found a successful re-trigger!
                retrigger_run = run
                logging.info(f"  Found re-trigger! RunId: {run_id_str}, Start: {datetime.fromtimestamp(run_start_epoch_ms / 1000, tz=timezone.utc)}")
                break

            if not retrigger_run:
                logging.info(f"--- Recheck Databricks: No successful re-trigger found for JobId '{job_id}'. ---")
                continue

            # Extract details from the re-triggered run
            new_run_id = str(retrigger_run.get('run_id', ''))
            new_run_page_url = str(retrigger_run.get('run_page_url', ''))

            # Convert epoch ms timestamps to DATETIME2-compatible strings
            new_start_epoch_ms = retrigger_run.get('start_time', 0)
            new_end_epoch_ms = retrigger_run.get('end_time', 0)

            new_start_time = None
            new_end_time = None
            if new_start_epoch_ms and new_start_epoch_ms > 0:
                new_start_time = datetime.fromtimestamp(new_start_epoch_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') + f"{new_start_epoch_ms % 1000:03d}"
            if new_end_epoch_ms and new_end_epoch_ms > 0:
                new_end_time = datetime.fromtimestamp(new_end_epoch_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') + f"{new_end_epoch_ms % 1000:03d}"

            # Update jobRuns
            update_query = """
                UPDATE [jobmonitoring].[jobRuns]
                SET Status = 'SUCCESS',
                    JobRunId = ?,
                    StartTime = ?,
                    EndTime = ?,
                    RunPageURL = ?,
                    ModifiedDatetime = GETUTCDATE()
                WHERE JobRunId = ?
            """
            cursor.execute(update_query, new_run_id, new_start_time, new_end_time, new_run_page_url, str(failed_run_id))

            # Delete from failureLogs
            delete_query = "DELETE FROM [jobmonitoring].[failureLogs] WHERE JobRunId = ?"
            cursor.execute(delete_query, str(failed_run_id))

            conn.commit()
            updated_count += 1
            logging.info(f"--- Recheck Databricks: ✅ Updated JobId '{job_id}' (Schedule: {schedule}) from FAILURE → SUCCESS. New RunId: {new_run_id} ---")

        except Exception as job_error:
            logging.error(f"--- Recheck Databricks: Error processing JobId '{job_id}': {str(job_error)} ---")
            continue

    return updated_count
