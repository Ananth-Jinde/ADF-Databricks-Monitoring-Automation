# Interview Walkthrough — How to Explain This Project

## How to Position Your Team & Role

> **Never say:** "I work in an L2 support team" or "I'm in DataOps support" or "I'm in the development team" or "I'm in L3"
>
> **Always say:** "I work in the **Data Engineering team** responsible for building, deploying, and ensuring the operational reliability of the data pipeline infrastructure."

**Why this works:**
- It's truthful — DataOps IS data engineering (the operational side)
- You built automation tools — that's engineering work
- It avoids the support/development dichotomy entirely

When asked "What does your team do?", say:
> *"Our team manages the end-to-end data pipeline ecosystem — we build, deploy, and maintain 150+ data pipelines across ADF and Databricks for 30 data products. I specifically took ownership of building the monitoring and automation layer to eliminate manual operational overhead."*

### If Asked "Why does your team create ServiceNow incidents?"
> *"The incidents aren't created for my team — the automation routes them to the appropriate team based on data product ownership via a config table. Each data product has a designated owner in our DataProductConfig table, and incidents get auto-assigned accordingly. I built the automation system itself — the incident creation, routing, and deduplication logic."*

**This is bulletproof because:**
- You're the BUILDER of the tool, not the receiver of tickets
- Incidents go to "the designated owner" (vague but honest)
- Shows you understand ITSM workflow without positioning yourself in support

### If Asked "Who built the Power BI dashboard?"
> *"The Power BI dashboard was built by another team member. My role was designing and building the entire data layer that powers it — the SQL views, stored procedures, scheduling logic, and the reporting views with IST conversion, long-running detection, and failure trending. The dashboard connects to my views via DirectQuery."*

---

## The 2-Minute Version (Elevator Pitch)

Use this when asked: *"Tell me about a project you worked on"* or *"Describe your current role."*

> *"At Accenture, I work as a Data Engineer managing a large-scale data pipeline ecosystem with 150+ ADF pipelines and 45+ Databricks jobs across 30 data products. One of the key initiatives I led was building an end-to-end monitoring automation platform from scratch.*
>
> *Before this automation, team members collectively spent about 25 hours a week manually checking pipeline statuses, creating incident tickets, and notifying stakeholders about failures. I designed and built a system using Python Azure Functions that polls the ADF and Databricks APIs every 5 minutes, detects failures in real time, automatically creates ServiceNow incidents and routes them to the right team, and pushes Adaptive Card alerts to Microsoft Teams — all within seconds of a failure occurring.*
>
> *The system also has a self-healing capability where if someone manually re-triggers a failed job and it succeeds, the database auto-corrects without any manual intervention. This reduced approximately 25 hours per week of manual effort and gave the team complete real-time visibility through SQL views that power a Power BI dashboard."*

---

## The 5-Minute Version (Standard Interview)

Use this when they want more depth. Start with the 2-minute version, then add:

> *"Let me walk you through the technical architecture. The system has three Azure Functions running as timer triggers:*
>
> *The first function is the ADF Monitor — it runs every 5 minutes, queries a SQL scheduling view that determines which pipelines need checking right now based on their configured schedule times, then calls the ADF Management SDK to get the actual pipeline status, and writes the results to our SQL database through a MERGE-based stored procedure.*
>
> *The scheduling view itself has some interesting logic — it handles pipelines with multiple schedules per day using STRING_SPLIT, it handles midnight-crossing schedules where a 22:00 pipeline's monitoring window extends past midnight, and it deduplicates at the individual schedule level so the same pipeline isn't logged twice for the same schedule.*
>
> *The second function does the same for Databricks, but uses the Databricks REST API 2.1. One key difference is that Databricks uses a two-level status system — life_cycle_state and result_state — so I had to build a mapping layer to normalize these into our standard statuses.*
>
> *For failure handling, we have a smart incident management system. When an ADF pipeline fails, the code checks three things: Is incident creation enabled for this pipeline? Has an incident already been created today? If both pass, it creates a ServiceNow incident via REST API with dynamic POC routing based on the data product config table, and the incident gets routed to the appropriate team. But regardless of whether an incident is created, a Teams alert always goes out — because we want everyone to know about every failure immediately.*
>
> *The third function is the Recheck Monitor — it runs every 15 minutes and solves a real operational problem. If a pipeline fails at 9 AM and someone manually re-triggers it at 9:30, our database would still show FAILURE until the next day. The recheck function queries the API for any successful runs after the failure time and, if found, updates the status to SUCCESS and cleans up the failure logs.*
>
> *Finally, I also built the entire SQL reporting layer — 6 views and 2 stored procedures — that serves as the data source for the team's Power BI dashboard. These views handle IST timezone conversion, compute long-running job flags with tiered thresholds, and provide unified reporting across both ADF and Databricks platforms."*

---

## The Deep-Dive Version (Technical Rounds)

When asked to go deeper on specific areas, use these talking points:

### On the Scheduling Logic
> *"The scheduling view uses a sliding window approach. For ADF, it's a 180-minute window; for Databricks, it's 30 minutes. The reason for the difference is that ADF pipelines can have variable trigger delays, while Databricks jobs typically start within seconds of their cron schedule. The view uses CROSS APPLY with STRING_SPLIT to handle comma-separated multi-schedule entries, and it includes midnight-crossing logic using a CASE statement that checks whether the window-end time wraps past midnight."*

### On Incident Deduplication
> *"We implement one-incident-per-pipeline-per-day using an incident_log table. When a pipeline fails, we first check if an incident already exists for that pipeline-instance-date combination. If so, the Teams alert includes the existing incident number so the team knows not to create a duplicate. We also have a configurable flag per pipeline — for pipelines that we know fail intermittently (like retry-dependent pipelines), we disable incident creation but still send alerts."*

### On the Self-Healing Mechanism
> *"The recheck function finds all jobRuns entries with Status = FAILURE for today, then queries the respective API for runs that started AFTER the failure time. It has an important safeguard — it checks that the successful run isn't already in our jobRuns table, because that would mean it's a scheduled run, not a manual re-trigger. When it finds a genuine re-trigger, it updates the jobRuns record, changes the status to SUCCESS, and deletes the failureLogs entry so the dashboard immediately reflects the correction."*

### On Databricks Status Mapping
> *"Databricks has a more complex status model than ADF. ADF gives you a single status like 'Succeeded' or 'Failed'. Databricks uses a two-level system: life_cycle_state tells you the execution phase (RUNNING, TERMINATED, PENDING, etc.), and result_state (only present when TERMINATED) tells you the outcome (SUCCESS, FAILED, TIMEDOUT). I built a mapping layer in both the Python code and the SQL stored procedure to normalize these into our four standard statuses."*

---

## Common Interview Questions & Answers

### Q: "What was the biggest challenge in building this?"
> *"The scheduling logic was the most complex part. We have pipelines with multiple schedules — like one that runs at 9:00, 14:00, and 22:00. I needed each schedule to be tracked independently, so I couldn't just check 'has this pipeline run today' — I had to check 'has this pipeline run today FOR THIS SPECIFIC SCHEDULE.' The midnight-crossing logic added another layer of complexity because a 22:00 schedule's monitoring window extends into the next day."*

### Q: "How does your system handle high availability?"
> *"The Azure Functions run on a consumption plan with automatic scaling. If one execution takes longer than 5 minutes, the next timer trigger still fires independently. The SQL operations use parameterized queries and MERGE statements with proper error handling, so even if the API is slow, we don't lose data. Each function also has comprehensive try/catch blocks with continue statements, so one failed pipeline doesn't block the monitoring of others."*

### Q: "What would you improve about this system?"
> *"Two things I'd add: First, alerting escalation — right now every failure creates a P4 incident. I'd add a failure counter so that if a pipeline fails 3 times in a day or fails consistently across 3 consecutive days, the incident gets escalated to P3 and the manager is notified via email. This ensures persistent issues get faster attention.*
>
> *Second, self-healing for transient failures — for known intermittent errors like timeouts or throttling, auto-retry the job through the API before creating an incident. This would eliminate about 60-70% of incidents caused by temporary issues."*

### Q: "How do you add a new pipeline to monitoring?"
> *"It's completely config-driven. Adding a new pipeline is a single INSERT statement into the ADFJobsMaster or jobsMaster table — no code changes, no redeployment. You just specify the pipeline name, factory/workspace, schedule, and data product. The system picks it up automatically on the next monitoring cycle."*

### Q: "Why Azure Functions instead of Databricks Jobs or ADF itself for monitoring?"
> *"We considered several options. ADF can't easily monitor itself — you'd create circular dependencies. Databricks would work but adds unnecessary compute cost for what is essentially lightweight API polling. Azure Functions on a consumption plan are perfect: they run every 5 minutes, complete in under 30 seconds, cost almost nothing, and scale automatically. Plus, the timer trigger guarantees execution even if the monitored systems are down."*

### Q: "Tell me about a time you solved a production issue."
> *"We had a situation where a pipeline would fail at 9 AM, someone would manually re-trigger it at 9:30, and it would succeed — but the dashboard showed FAILURE all day, causing confusion. The team kept asking 'Is this still broken?' That's when I built the recheck mechanism. It runs every 15 minutes, detects successful re-triggers, and auto-corrects the database. It was a small addition architecturally, but it had a huge impact on team confidence in the dashboard's accuracy."*

### Q: "How do you handle secrets and security?"
> *"All secrets — database passwords, API tokens, Service Principal credentials — are stored as environment variables in the Azure Function App Settings, which are encrypted at rest. Nothing is hardcoded. The database connection uses Active Directory Password authentication, and all API calls use token-based or Service Principal auth. Job parameters with sensitive keys (passwords, tokens) are masked before logging."*

### Q: "Did you build the dashboard?"
> *"The Power BI dashboard was built by another team member. My contribution was the entire backend — I designed and built the SQL views and stored procedures that the dashboard queries. I created 6 reporting views that handle IST timezone conversion, compute long-running job flags with tiered thresholds, unify ADF and Databricks data into a single source, and provide 7-day failure trending. The dashboard connects to my views via DirectQuery for real-time data."*
