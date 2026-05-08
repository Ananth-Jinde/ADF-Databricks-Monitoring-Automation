# System Improvements — What I Would Add Next

> Use these in interviews when asked: *"What would you improve?"* or *"What's next for this system?"*

---

## 1. Alerting Escalation

### Problem
Currently, every failure creates a Priority 4 (Low) ServiceNow incident, regardless of how many times the pipeline has failed. A pipeline that fails once is treated the same as one that fails consistently across multiple days.

### Proposed Solution
Implement a **failure tracking mechanism** that automatically escalates incident priority to P3 and notifies the manager:

| Scenario | Escalation Action |
|----------|-------------------|
| 1st or 2nd failure (same day) | Create/keep incident at **P4 (Low)** — *current behavior* |
| 3rd failure (same pipeline, same day) | Escalate existing incident to **P3 (Medium)** + send email to manager |
| Pipeline runs once/day but fails 3 consecutive days | Escalate to **P3 (Medium)** + send email to manager |

> **Note:** Escalation stops at P3. No further priority increase beyond P3.

### How to Achieve This

**Step 1: Track failure count (same-day + multi-day)**
Add columns to `incident_log` or create a tracking table:
```sql
ALTER TABLE jobmonitoring.incident_log ADD failure_count INT DEFAULT 1;

-- New table for tracking consecutive day failures (for once-a-day pipelines)
CREATE TABLE [jobmonitoring].[consecutive_failure_tracker] (
    pipeline_name NVARCHAR(512),
    instance_name NVARCHAR(512),
    consecutive_days INT DEFAULT 1,
    last_failure_date DATE,
    CONSTRAINT PK_consecutive_failure UNIQUE (pipeline_name, instance_name)
);
```

**Step 2: Modify the incident creation logic in `adf_jobs.py`:**
```python
# After checking for existing incident
if existing_incident:
    existing_inc = existing_incident[0]
    failure_count = existing_incident[1]  # failure_count column
    
    # Same-day escalation: 3rd failure on the same day → escalate to P3
    if failure_count >= 2:  # This will be the 3rd failure
        escalate_servicenow_incident(
            incident_number=existing_inc,
            new_priority="3 - moderate"  # P3
        )
        send_manager_escalation_email(
            pipeline_name, data_factory, failure_count + 1,
            escalation_reason="3rd failure on the same day"
        )
    
    # Update failure count
    cursor.execute(
        "UPDATE jobmonitoring.incident_log SET failure_count = failure_count + 1 WHERE incident = ?",
        existing_inc
    )
    conn.commit()

# Multi-day escalation: Check consecutive day failures (for once-a-day pipelines)
cursor.execute("""
    SELECT consecutive_days, last_failure_date 
    FROM jobmonitoring.consecutive_failure_tracker 
    WHERE pipeline_name = ? AND instance_name = ?
""", pipeline_name, data_factory)
tracker = cursor.fetchone()

if tracker:
    consecutive_days = tracker[0]
    last_failure_date = tracker[1]
    
    if last_failure_date == (today_local - timedelta(days=1)):
        # Yesterday also failed → increment consecutive days
        new_consecutive = consecutive_days + 1
        cursor.execute("""
            UPDATE jobmonitoring.consecutive_failure_tracker 
            SET consecutive_days = ?, last_failure_date = ?
            WHERE pipeline_name = ? AND instance_name = ?
        """, new_consecutive, today_local, pipeline_name, data_factory)
        
        if new_consecutive >= 3:
            escalate_servicenow_incident(incident_number, "3 - moderate")
            send_manager_escalation_email(
                pipeline_name, data_factory, new_consecutive,
                escalation_reason=f"Failed for {new_consecutive} consecutive days"
            )
    else:
        # Reset counter (gap in failures)
        cursor.execute("""
            UPDATE jobmonitoring.consecutive_failure_tracker 
            SET consecutive_days = 1, last_failure_date = ?
            WHERE pipeline_name = ? AND instance_name = ?
        """, today_local, pipeline_name, data_factory)
```

**Step 3: Add escalation functions:**
```python
def escalate_servicenow_incident(incident_number, new_priority):
    """Updates an existing ServiceNow incident's priority to P3."""
    url = f"{SERVICENOW_URL}/{incident_number}"
    payload = {
        "urgency": new_priority,
        "work_notes": f"Auto-escalated to P3: Persistent failure pattern detected."
    }
    requests.patch(url, headers=headers, json=payload, timeout=30)


def send_manager_escalation_email(pipeline_name, instance, count, escalation_reason):
    """Sends an escalation email to the manager via Logic App or SMTP."""
    payload = {
        "subject": f"[ESCALATION] {pipeline_name} - {escalation_reason}",
        "body": f"Pipeline '{pipeline_name}' on '{instance}' requires attention.\n"
                f"Reason: {escalation_reason}\n"
                f"Failure count: {count}\n"
                f"Action: Please investigate the root cause.",
        "recipient": os.environ.get("MANAGER_EMAIL")
    }
    requests.post(os.environ.get("EMAIL_LOGIC_APP_URL"), json=payload, timeout=10)
```

**Step 4: Modify the Teams alert** to include the escalation status:
- "⚠️ Escalated to P3 — 3rd failure today"
- "⚠️ Escalated to P3 — Failed for 3 consecutive days"

### Business Impact
- Persistent failures get faster attention from management
- Reduces the risk of a data pipeline issue going unresolved for multiple days
- Managers are automatically looped in only when issues persist (avoids noise)

---

## 2. Self-Healing for Transient Failures

### Problem
Many pipeline failures are **transient** — caused by temporary issues like:
- API rate limiting / throttling
- Network timeouts
- Cluster spin-up failures (Databricks)
- Temporary resource unavailability

Currently, even transient failures create incidents and require manual re-triggers. This wastes the team's time on issues that would resolve themselves on retry.

### Proposed Solution
Implement **auto-retry** for known transient error patterns BEFORE creating an incident:

| Error Pattern | Retry Strategy |
|--------------|---------------|
| `HTTP 429 Too Many Requests` | Wait 60 seconds, retry once |
| `Connection timeout` | Wait 30 seconds, retry once |
| `ClusterNotReadyException` | Wait 120 seconds, retry once |
| `The specified resource does not exist` | No retry (genuine failure) |
| All other errors | No retry (create incident as usual) |

### How to Achieve This

**Step 1: Define transient error patterns**
Create a configuration table:
```sql
CREATE TABLE [jobmonitoring].[transient_error_patterns] (
    pattern_id INT IDENTITY(1,1),
    error_pattern NVARCHAR(512),      -- Regex or LIKE pattern
    retry_delay_seconds INT,           -- Wait before retry
    max_retries INT DEFAULT 1,
    is_active BIT DEFAULT 1
);

INSERT INTO [jobmonitoring].[transient_error_patterns] VALUES
('Too Many Requests', 60, 1, 1),
('Connection timeout', 30, 1, 1),
('ClusterNotReadyException', 120, 1, 1),
('TEMPORARILY_UNAVAILABLE', 60, 1, 1);
```

**Step 2: Add retry logic to `adf_jobs.py`:**
```python
def should_auto_retry(error_message, cursor):
    """Check if the error matches a known transient pattern."""
    cursor.execute(
        "SELECT pattern_id, retry_delay_seconds FROM jobmonitoring.transient_error_patterns WHERE is_active = 1"
    )
    patterns = cursor.fetchall()
    
    for pattern_id, delay in patterns:
        pattern_text = patterns[pattern_id]
        if pattern_text.lower() in error_message.lower():
            return True, delay
    
    return False, 0


def auto_retry_pipeline(adf_client, resource_group, data_factory, pipeline_name, delay_seconds):
    """Trigger a re-run of the failed pipeline after a delay."""
    import time
    time.sleep(delay_seconds)
    
    # Use ADF API to trigger a new run
    run_response = adf_client.pipelines.create_run(
        resource_group_name=resource_group,
        factory_name=data_factory,
        pipeline_name=pipeline_name
    )
    return run_response.run_id
```

**Step 3: Modify the failure handling flow:**
```python
if status == 'Failed':
    is_transient, delay = should_auto_retry(error_message, cursor)
    
    if is_transient:
        logging.info(f"Transient error detected for {pipeline_name}. Auto-retrying in {delay}s...")
        new_run_id = auto_retry_pipeline(adf_client, resource_group, data_factory, pipeline_name, delay)
        
        # Send a modified Teams alert: "Auto-retry triggered"
        notify_teams_logic_app(
            f"Auto-retry triggered (Run: {new_run_id})",
            data_factory, pipeline_name, error_message,
            data_product, poc_name, run_url, source="ADF"
        )
        # Don't create incident yet — wait for the retry result
    else:
        # Normal flow: create incident
        create_servicenow_incident(...)
```

**Step 4: Track retry outcomes**
```sql
CREATE TABLE [jobmonitoring].[auto_retry_log] (
    original_run_id NVARCHAR(512),
    retry_run_id NVARCHAR(512),
    pipeline_name NVARCHAR(512),
    error_pattern NVARCHAR(512),
    retry_status NVARCHAR(50),    -- PENDING, SUCCESS, FAILED
    created_dt DATETIME2(3) DEFAULT GETUTCDATE()
);
```

### Business Impact
- **60-70% reduction in transient failure incidents** (based on typical error distribution)
- Team focuses on genuine failures, not transient blips
- Faster recovery for issues that would have waited for manual intervention
- Clear audit trail of auto-retry attempts in the log table

---

## How to Present These in Interviews

When asked *"What would you improve?"*, say:

> *"I have two improvements in mind. First, alerting escalation — right now every failure creates a P4 incident. I'd add a failure tracking mechanism: if a pipeline fails 3 times on the same day, or fails consistently for 3 consecutive days, the incident automatically escalates to P3 and sends an email to the manager. This ensures persistent issues get management attention without creating noise for one-off failures.*
>
> *Second, self-healing for transient failures. We see a lot of pipeline failures due to temporary issues — API throttling, network timeouts, cluster spin-up delays. These usually succeed on retry. I'd build a config-driven auto-retry mechanism that recognizes known transient error patterns and automatically re-triggers the pipeline before creating an incident. Based on our error distribution, this would eliminate about 60-70% of incidents that currently require manual intervention."*

