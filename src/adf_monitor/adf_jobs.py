"""
ADF Pipeline Monitor — Azure Function
======================================
Timer-triggered Azure Function that monitors Azure Data Factory pipeline executions.

Workflow:
    1. Queries the SQL view [vw_ADFJobSchedules] for pipelines due to run
    2. Calls the ADF Management API to get real-time pipeline status
    3. Updates the SQL database via the [UpdateADFJobRuns] stored procedure
    4. On failure: creates a ServiceNow incident (with deduplication) and
       sends a Teams alert via Logic App (Adaptive Card format)

Runs every 5 minutes via Azure Function Timer Trigger.
"""

# %%
import pyodbc
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date, timezone, time
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import os
import logging
import azure.functions as func

from shared.db_utils import get_db_connection

# --- Configuration & Globals ---
ADF_CLIENT_ID = os.environ.get("adf_client_id")
ADF_CLIENT_SECRET = os.environ.get("adf_client_secret")
LOGIC_APP_URL = os.environ.get("LOGIC_APP_ENDPOINT_PRD")
SERVICENOW_APIKEY = os.environ.get("apikey")
SERVICENOW_URL = os.environ.get("SERVICENOW_URL", "https://api.enterprise.com/itsm/servicenow/create/incident")


# --- Main Function ---
def main(mytimer: func.TimerRequest) -> None:
    """Azure Function entry point triggered by a timer."""
    start_time = datetime.now(timezone.utc)
    logging.info(f"--- Python timer trigger function executed at {start_time} UTC ---")

    try:
        # Get the list of jobs needing attention from the SQL view
        job_list_df = get_scheduled_jobs()

        if job_list_df.empty:
             logging.info("--- No jobs returned by the view in this run cycle. Exiting. ---")
        else:
            logging.info(f"--- Processing {len(job_list_df)} job(s) found by the view. ---")
            # Iterate through each job identified by the view

            for index, row in job_list_df.iterrows():
                pipeline_name_from_view = row.get('PipelineName', 'N/A')
                logging.info(f"--- Processing job index: {index} (Pipeline: {pipeline_name_from_view}) ---")
                try:
                    Resource_group = row['Resourcegroup']
                    Status_from_view = row['Status'] # Status from the view ('NOT STARTED' or 'RUNNING')
                    Data_factory = row['Datafactory']
                    PipelineName = row['PipelineName']
                    runid_from_view = row['runid'] # RunId from view ('0' or actual ID)
                    Subscription_Id = row['SubscriptionId']
                    Tenant_id = row['Tenantid']
                    schedule_from_view = row['schedule'] # Specific schedule time (e.g., '09:00') or ''

                    # Log extracted details
                    logging.info(f"  Resource Group: {Resource_group}")
                    logging.info(f"  Status from View: {Status_from_view}")
                    logging.info(f"  Data Factory: {Data_factory}")
                    logging.info(f"  Pipeline Name: {PipelineName}")
                    logging.info(f"  Run ID from View: {runid_from_view}")
                    logging.info(f"  Subscription ID: {Subscription_Id}")
                    logging.info(f"  Tenant ID: {Tenant_id}")
                    logging.info(f"  Schedule from View: {schedule_from_view}")

                    # Call get_job_status to query the ADF API for the actual current status
                    # Pass the schedule_from_view to handle the 'NOT STARTED' condition
                    job_details_obj, error_message_from_api = get_job_status(
                        Subscription_Id, ADF_CLIENT_ID, ADF_CLIENT_SECRET, Tenant_id,
                        Resource_group, Status_from_view, Data_factory, PipelineName, runid_from_view, schedule_from_view
                    )

                    # If get_job_status didn't find a valid/current run, skip to the next job
                    if job_details_obj == 'pipeline not found':
                        logging.info(f"--- Job details not found or invalid run identified by get_job_status for {PipelineName}. Skipping database update for this cycle. ---")
                        continue
                    else:
                        # If valid details were found, update the database via the stored proc
                        databaseUpdater(job_details_obj, error_message_from_api, schedule_from_view, Data_factory, Resource_group, Subscription_Id)

                except KeyError as key_err:
                     logging.error(f"--- Error processing job at index {index}: Missing expected column '{key_err}' in view data. Skipping job. ---")
                     continue
                except Exception as loop_error:
                     logging.error(f"--- Error processing job at index {index} (Pipeline: {row.get('PipelineName', 'N/A')}): {str(loop_error)} ---")
                     continue # Continue to the next job

    except Exception as e:
        # Catch errors occurring outside the main loop (e.g., in get_scheduled_jobs or DB connection)
        logging.error(f"--- An critical error occurred in the main function execution: {str(e)} ---")


    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    logging.info(f"--- Main function finished processing at {end_time} UTC. Duration: {duration}. ---")

# --- Helper Functions ---
def get_scheduled_jobs() -> pd.DataFrame:
    """Retrieves jobs scheduled to run from the SQL View."""
    conn = None
    try:
        conn = get_db_connection()
        query = "SELECT Resourcegroup, Status, Datafactory, PipelineName, runid, SubscriptionId, Tenantid, schedule FROM [jobmonitoring].[vw_ADFJobSchedules]"
        jobs_df = pd.read_sql(query, conn)
        logging.info(f"--- get_scheduled_jobs: Found {len(jobs_df)} jobs in the view. ---")
        return jobs_df
    except Exception as ex:
        logging.error(f"Failed to retrieve scheduled jobs: {str(ex)}")
        return pd.DataFrame() # Return empty dataframe on error
    finally:
        if conn:
            conn.close()

def get_job_status(Subscription_Id: str, client_id: str, client_secret: str, Tenant_id: str,
                   Resource_group: str, Status: str, Data_factory: str, PipelineName: str,
                   runid: str, schedule_time_str: str) -> tuple:
    """Queries ADF Management API to get the real-time status of a pipeline run.

    For 'NOT STARTED' pipelines: queries by factory for runs matching the schedule window.
    For 'RUNNING'/'QUEUED' pipelines: fetches the specific run by run_id.

    Returns:
        tuple: (pipeline_run_object or 'pipeline not found', error_message_string)
    """
    error_message = ''
    lastUpdatedBefore = str(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z')
    lastUpdatedAfter = str((datetime.now(timezone.utc) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z')

    try:
        credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=Tenant_id)
        adf_client = DataFactoryManagementClient(credentials, Subscription_Id)
       
        job_result = 'pipeline not found'

        if Status == 'NOT STARTED':
            logging.info(f"--- get_job_status: Status is 'NOT STARTED' for {PipelineName}. Querying ADF API... ---")
            runs = adf_client.pipeline_runs.query_by_factory(
                factory_name=Data_factory,
                resource_group_name=Resource_group,
                filter_parameters={
                    "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [PipelineName]}],
                    "lastUpdatedAfter": lastUpdatedAfter,
                    "lastUpdatedBefore": lastUpdatedBefore
                }
            )

            if not runs.value:
                job_result = 'pipeline not found'
            else:
                try:
                    schedule_time_obj = datetime.strptime(schedule_time_str.strip(), '%H:%M').time()
                   
                    now_utc = datetime.now(timezone.utc)
                    today_utc = now_utc.date()
                    schedule_datetime_utc = datetime.combine(today_utc, schedule_time_obj)

                    # If Target (e.g. 22:00) is in the future compared to Now (e.g. 02:30),
                    # it means the run happened Yesterday.
                    if schedule_datetime_utc > now_utc.replace(tzinfo=None):
                        schedule_datetime_utc = schedule_datetime_utc - timedelta(days=1)
                        logging.info(f"Target is in future. Checking Yesterday's run: {schedule_datetime_utc}")

                    logging.info(f"--- Validating {PipelineName} against target: {schedule_datetime_utc} ---")

                    # Filter: Only accept runs that started AFTER the calculated schedule
                    valid_runs = [
                        r for r in runs.value
                        if r.run_start and r.run_start.replace(tzinfo=None) >= schedule_datetime_utc
                    ]

                    if valid_runs:
                        # Grab the latest valid run
                        job_result = sorted(valid_runs, key=lambda x: x.last_updated, reverse=True)[0]
                        logging.info(f"--- Found valid run: {job_result.run_id} ---")
                    else:
                         job_result = 'pipeline not found'

                except ValueError:
                    logging.warning(f"--- Complex schedule '{schedule_time_str}'. Using Fallback (Latest Run). ---")
                    job_result = sorted(runs.value, key=lambda x: x.last_updated, reverse=True)[0]

        elif Status == 'RUNNING' or Status == 'QUEUED':
            logging.info(f"--- get_job_status: Status is '{Status}' for {runid}. Checking details... ---")
            try:
                job_result = adf_client.pipeline_runs.get(Resource_group, Data_factory, runid)
            except Exception as e:
                logging.error(f"--- Failed to get status for run_id {runid}: {str(e)} ---")
                job_result = 'pipeline not found'

        if job_result != 'pipeline not found':
            if job_result.status == "Failed":
                error_message = getattr(job_result, "message", "No failure message available")
                if error_message is None: error_message = "Error message not provided by ADF."
                error_message = error_message.replace("'", "''")
            return job_result, error_message
        else:
            return 'pipeline not found', ''

    except Exception as ex:
        logging.error(f"An unexpected error occurred in get_job_status for {PipelineName}: {str(ex)}")
        return 'pipeline not found', ''
   
def databaseUpdater(jobs, error_message: str, schedule: str, Data_factory: str,
                    Resource_group: str, subscription_id: str) -> None:
    """Updates the SQL database with pipeline run details and handles incident creation on failure.

    Calls the UpdateADFJobRuns stored procedure to upsert run records.
    On failure, attempts to create a ServiceNow incident before the DB update.
    """
    pipeline_name = jobs.pipeline_name
    JobRunId = jobs.run_id
    status = jobs.status

    # Convert datetime objects to clean ISO strings suitable for SQL DATETIME2
    StartTime = jobs.run_start.isoformat(timespec='milliseconds') if jobs.run_start else None
    EndTime = jobs.run_end.isoformat(timespec='milliseconds') if jobs.run_end else None

    factory_resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{Resource_group}/providers/Microsoft.DataFactory/factories/{Data_factory}"
    RunPageURL = f"https://adf.azure.com/monitoring/pipelineruns/{jobs.run_id}?factory={factory_resource_id}"
   
    Instance = Data_factory
    job_schedule = schedule if schedule else '' # Use schedule if available, else empty string

    job_parameter_dict = getattr(jobs, "parameters", {})
    if job_parameter_dict is None:
        job_parameter_dict = {}
    job_parameter = json.dumps(job_parameter_dict).replace("'", "''")

    incident_number = None

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
 
        if status == 'Failed':
            check_query = "SELECT 1 FROM [jobmonitoring].[jobRuns] WHERE JobRunId = ? AND Status = 'FAILURE'"
            cursor.execute(check_query, JobRunId)
            already_failed = cursor.fetchone()
 
            if already_failed:
                logging.info(f"RunId {JobRunId} already logged as Failed for {pipeline_name}. Skipping incident creation and alert.")
            else:
                try:
                    incident_number = create_servicenow_incident(
                        Instance,
                        pipeline_name,
                        error_message.replace("''", "'"),
                        urgency="3",
                        impact="3",
                        run_url=RunPageURL
                    )
                except Exception as e:
                    logging.error(f"[ERROR] ServiceNow incident creation failed for {pipeline_name}: {e}")

        # Use parameter binding for safety and correctness
        query = """
            EXEC [jobmonitoring].[UpdateADFJobRuns]
                @PipelineName = ?, @JobRunId = ?, @Status = ?,
                @StartTime = ?, @EndTime = ?, @RunPageURL = ?,
                @JobParameters = ?, @ErrorMessage = ?, @ADFInstance = ?,
                @Schedule = ?;
        """
        params = (
            pipeline_name, JobRunId, status,
            StartTime, EndTime, RunPageURL,
            job_parameter, error_message if error_message else None, Instance,
            job_schedule
        )

        logging.info(f"--- Executing SPROC for RunId: {JobRunId} with Status: {status} ---")
        logging.info(f"Params: {params}")

        cursor.execute(query, params)
        conn.commit()
        logging.info(f"--- SPROC commit successful for RunId: {JobRunId}. ---")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"--- SQL ERROR CAUGHT FOR RunId: {JobRunId}! ---")
        logging.error(f"SQLSTATE: {sqlstate}")
        logging.error(f"Error Details: {str(ex)}")

    except Exception as e:
        logging.error(f"--- A NON-SQL PYTHON ERROR occurred in databaseUpdater for RunId {JobRunId}: {str(e)} ---")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_servicenow_incident(data_factory: str, Pipeline_name: str, error_message: str,
                               urgency: str = "3", impact: str = "3", run_url: str = "") -> str:
    """Creates a ServiceNow incident for pipeline failure and triggers Teams alert.

    Implements smart deduplication: only one incident per pipeline per day.
    Even when incident creation is disabled for a pipeline, Teams alerts are
    still sent so the team has visibility into every failure.

    Flow:
        1. Check if incident creation is enabled in ADFJobsMaster
        2. Look up data product config (POC, CMDB CI) for dynamic routing
        3. If disabled → send Teams alert only (with "Incident creation disabled" label)
        4. If duplicate today → send Teams alert with existing incident number
        5. If new → create incident via ServiceNow API, log it, send Teams alert

    Returns:
        str or None: Incident number (e.g., 'INC6113674') if created, None otherwise.
    """
    conn = None
    cursor = None
   
    # Default Values
    assignment_group = "CloudOps-DataEngineering-L2"
    assigned_to_id = ""  # Will hold the assignee ID
    poc_name_display = "Anantha Sai Jinde"
    # Default CI (Fallback only, usually overwritten by DB lookup)
    cmdb_ci_value = "Enterprise Data & Analytics Operations - Prod"

    urgency_map = {
        "1": "1 - high",
        "2": "2 - medium",
        "3": "3 - low"
    }
    impact_map = {
        "1": "1 - high",
        "2": "2 - medium",
        "3": "3 - low"
    }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        today_local = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()  # IST date

        # ----------------------------------------------------------------------------------------------
        # 1. Check if incident creation is enabled in ADF JobsMaster. Get Pipeline Config & DataProduct.
        # ----------------------------------------------------------------------------------------------
        config_query = "SELECT incident_creation, DataProduct FROM jobmonitoring.ADFJobsMaster WHERE DataFactory = ? AND PipelineName = ?"
        cursor.execute(config_query, data_factory, Pipeline_name)
        config_result = cursor.fetchone()

        # If pipeline not found in ADFJobsMaster at all, skip.
        if not config_result:
            logging.info(f"Pipeline {Pipeline_name} not found in ADFJobsMaster. Skipping.")
            return None

        incident_creation_flag = config_result[0]
        data_product = config_result[1]

        # ---------------------------------------------------------
        # 2. DYNAMIC LOOKUP: GET ASSIGNEE ID AND CMDB_CI
        # ---------------------------------------------------------
        # Moved BEFORE incident_creation check so we have POC info
        # for Teams alerts even when incident creation is disabled.
        try:
            lookup_query = "SELECT POC_MUDID, CMDB_CI, POC FROM [jobmonitoring].[DataProductConfig] WHERE DataProduct = ?"
            cursor.execute(lookup_query, data_product)
            product_config = cursor.fetchone()

            if product_config:
                assigned_to_id = product_config[0]
                cmdb_ci_value = product_config[1]
                # Check if POC name is not null
                if product_config[2]:
                    poc_name_display = product_config[2]
                logging.info(f"Config Found for {data_product}: AssigneeId={assigned_to_id}, CI={cmdb_ci_value}, POC={poc_name_display}")
            else:
                # If DataProduct not found, use 'Default' row
                cursor.execute("SELECT POC_MUDID, CMDB_CI, POC FROM [jobmonitoring].[DataProductConfig] WHERE DataProduct = 'Default'")
                default_config = cursor.fetchone()
                if default_config:
                    assigned_to_id = default_config[0]
                    cmdb_ci_value = default_config[1]
                    if default_config[2]:
                         poc_name_display = default_config[2]
                    logging.warning(f"No config found for '{data_product}'. Used Default: AssigneeId={assigned_to_id}, CI={cmdb_ci_value}, POC={poc_name_display}")

        except Exception as lookup_ex:
            logging.error(f"Error looking up DataProductConfig: {str(lookup_ex)}. Proceeding with hardcoded defaults.")

        # ---------------------------------------------------------
        # 3. If incident creation is disabled, still send Teams alert
        # ---------------------------------------------------------
        if incident_creation_flag != 1:
            logging.info(f"Incident creation disabled for {data_factory} - {Pipeline_name}. Sending Teams alert only.")
            notify_teams_logic_app(
                "Incident creation is disabled",
                data_factory,
                Pipeline_name,
                error_message,
                data_product,
                poc_name_display,
                run_url,
                source="ADF"
            )
            return None

        # ---------------------------------------------------------
        # 4. CHECK FOR DUPLICATE INCIDENTS (one per pipeline per day)
        # ---------------------------------------------------------
        check_query = "SELECT incident FROM jobmonitoring.incident_log WHERE instance = ? AND pipeline = ? AND CAST(DATEADD(minute, 330, created_dt) AS DATE) = ?"
        cursor.execute(check_query, data_factory, Pipeline_name, today_local)
        existing_incident = cursor.fetchone()

        # If an incident already exists for today, send alert with existing incident number
        if existing_incident:
            existing_inc = existing_incident[0]
            logging.info(f"Incident {existing_inc} already logged today for {data_factory} - {Pipeline_name}. Sending Teams alert.")
            notify_teams_logic_app(
                f"Incident already created today - {existing_inc}",
                data_factory,
                Pipeline_name,
                error_message,
                data_product,
                poc_name_display,
                run_url,
                source="ADF"
            )
            return None

        # ---------------------------------------------------------
        # 5. CREATE INCIDENT VIA SERVICENOW REST API
        # ---------------------------------------------------------
        if not SERVICENOW_APIKEY or not SERVICENOW_URL:
            logging.error("ServiceNow API Key or URL not configured.")
            return None

        headers = {
            "Content-Type": "application/json", "Accept": "application/json", "apikey": SERVICENOW_APIKEY
        }
        final_urgency = urgency_map.get(str(urgency), "3 - low")
        final_impact = impact_map.get(str(impact), "3 - low")

        today_str = today_local.strftime("%d-%b-%Y")

        payload = {
            "caller_id": os.environ.get("SERVICENOW_CALLER_ID", "automation_svc_account"),
            "short_description": f"{data_product} | {Pipeline_name} has failed on {today_str}. Please take action.",
            "description": (
                f"Data Product: {data_product}\n"
                f"Pipeline: {Pipeline_name}\n"
                f"ADF Instance: {data_factory}\n"
                f"Failed On: {today_str}\n\n"
                f"Error Message:\n{error_message}\n\n"
                f"Run URL: {run_url}"
            ),
            "contact_type": "Chat", "category":"Software", "subcategory": "Applications",
            "business_service": "Data Engineering Operations-Service",
           
            # --- DYNAMIC FIELDS ---------------
            "cmdb_ci": cmdb_ci_value,          
            "assignment_group": assignment_group,
            "assigned_to": assigned_to_id,      
            # ----------------------------------
           
            "work_notes": "job-monitoring - Created by Automation",
            "urgency": final_urgency,
            "impact": final_impact
        }

        # If assigned_to_id is still empty (failed lookup + failed default),
        # remove the key so ServiceNow doesn't reject the payload.
        if not assigned_to_id:
            del payload['assigned_to']

        logging.info(f"Attempting to create ServiceNow incident for {data_factory} - {Pipeline_name}...")
        response = requests.post(SERVICENOW_URL, headers=headers, data=json.dumps(payload), timeout=30)

        if response.status_code == 201:
            incident_data = response.json().get("result", {})
            incident_number = incident_data.get("number")
            logging.info(f'ServiceNow Incident Created Successfully! Incident Number: {incident_number}')

            if incident_number:
                # Send Teams alert with the new incident number
                notify_teams_logic_app(
                    incident_number,
                    data_factory,
                    Pipeline_name,
                    error_message,
                    data_product,  
                    poc_name_display,
                    run_url,        
                    source="ADF"
                )
               
                # Log to DB for deduplication
                insert_query = "INSERT INTO jobmonitoring.incident_log (instance, pipeline, incident, created_dt) VALUES (?, ?, ?, GETUTCDATE())"
                cursor.execute(insert_query, data_factory, Pipeline_name, incident_number)
                conn.commit()
                logging.info(f"Logged incident {incident_number} to incident_log table.")
                return incident_number
            else:
                logging.warning("Incident number not found in ServiceNow API response although status was 201.")
                return None
        else:
            # Log failure details if API call was unsuccessful
            logging.error(f"Failed to create ServiceNow incident. Status: {response.status_code}, Response: {response.text}")
            return None

    except requests.exceptions.RequestException as api_ex:
         logging.error(f"ServiceNow API request failed for {Pipeline_name}: {str(api_ex)}")
         return None
    except pyodbc.Error as db_ex:
         logging.error(f"Database error during incident check/log for {Pipeline_name}: {str(db_ex)}")
         return None
    except Exception as ex:
        logging.error(f"An unexpected error occurred in create_servicenow_incident for {Pipeline_name}: {str(ex)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def notify_teams_logic_app(incident_number: str, adf_name: str, pipeline_name: str,
                           error_message: str, data_product: str, poc_name: str,
                           run_url: str, source: str = "ADF") -> None:
    """Sends a failure notification to Microsoft Teams via Azure Logic App.

    The Logic App renders an Adaptive Card with incident details, data product info,
    error message, and a direct link to the pipeline run for quick investigation.
    """
    try:
        if not LOGIC_APP_URL:
            logging.error("Logic App URL not configured.")
            return
       
        payload = {
            "incidentNumber": incident_number,
            "source": source,
            "adfName": adf_name,
            "pipelineName": pipeline_name,
            "errorMessage": error_message,
            "dataProduct": data_product,
            "pocName": poc_name,        
            "runUrl": run_url            
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(LOGIC_APP_URL, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully triggered Logic App for incident {incident_number}")

    except requests.exceptions.RequestException as ex:
        logging.error(f"Failed to trigger Logic App for incident {incident_number} due to a network or HTTP error: {str(ex)}")
    except Exception as ex:
        logging.error(f"An unexpected error occurred while triggering Logic App for incident {incident_number}: {str(ex)}")
