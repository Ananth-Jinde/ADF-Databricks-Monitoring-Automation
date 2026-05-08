/* ============================================================
   View: [jobmonitoring].[VwRptJobsRuns]
   Purpose: Power BI Dashboard source — all job runs with:
            - UTC to IST time conversion (UTC + 5:30)
            - Duration calculation (minutes)
            - Long Running Flag with tiered thresholds:
              * Short jobs (<2h): flagged if exceeds 150% of estimated
              * Medium jobs (2-4h): flagged if exceeds 130% of estimated
              * Long jobs (>4h): flagged if exceeds 115% of estimated
   
   Combines live data (jobRuns) with historical data (JobRunsHistory)
   for 7-day reporting window.
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[VwRptJobsRuns]
AS

-- === Section 1: LIVE Data ===
SELECT
    TRIM(JR.JobId) AS JobId,
    JR.JobRunId,
    JR.Status,

    CAST(DATEADD(minute, 330, JR.StartTime) AS DATE) AS ReportDate,
    DATEADD(minute, 330, JR.StartTime) AS [Start Time in IST],
    DATEADD(minute, 330, JR.EndTime) AS [End Time in IST],
    CAST(DATEADD(minute, 330, TRY_CAST(JR.Schedule AS TIME)) AS TIME) AS [Schedule in IST],

    -- Duration: for running jobs, calculate against current time
    CASE
        WHEN JR.Status IN ('RUNNING', 'InProgress', 'QUEUED') OR JR.EndTime IS NULL
        THEN DATEDIFF(MINUTE, JR.StartTime, GETUTCDATE())
        ELSE DATEDIFF(MINUTE, JR.StartTime, JR.EndTime)
    END AS [Duration in Mins],

    JR.RunPageURL,
    JR.AsOfDate,

    -- Long Running Flag (tiered thresholds based on estimated duration)
    CASE
        WHEN JR.Status IN ('RUNNING', 'InProgress', 'QUEUED') OR JR.EndTime IS NULL THEN
            CASE
                WHEN JM.EstimatedDuration IS NULL THEN 'N'
                WHEN JM.EstimatedDuration < 120
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.5) < DATEDIFF(MINUTE, JR.StartTime, GETUTCDATE()) THEN 'Y'
                WHEN JM.EstimatedDuration BETWEEN 120 AND 240
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.3) < DATEDIFF(MINUTE, JR.StartTime, GETUTCDATE()) THEN 'Y'
                WHEN JM.EstimatedDuration > 240
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.15) < DATEDIFF(MINUTE, JR.StartTime, GETUTCDATE()) THEN 'Y'
                ELSE 'N'
            END
        ELSE
            CASE
                WHEN JM.EstimatedDuration IS NULL OR JR.EndTime IS NULL THEN 'N'
                WHEN JM.EstimatedDuration < 120
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.5) < DATEDIFF(MINUTE, JR.StartTime, JR.EndTime) THEN 'Y'
                WHEN JM.EstimatedDuration BETWEEN 120 AND 240
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.3) < DATEDIFF(MINUTE, JR.StartTime, JR.EndTime) THEN 'Y'
                WHEN JM.EstimatedDuration > 240
                     AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.15) < DATEDIFF(MINUTE, JR.StartTime, JR.EndTime) THEN 'Y'
                ELSE 'N'
            END
    END AS LongRunningFlag

FROM [jobmonitoring].[jobRuns] JR WITH (NOLOCK)
LEFT JOIN [jobmonitoring].[VwRptJobsMaster] AS JM WITH (NOLOCK) ON JR.JobId = TRIM(JM.JobId)

UNION ALL

-- === Section 2: HISTORICAL Data (7-day window) ===
SELECT
    TRIM(JR_Hist.JobId),
    JR_Hist.JobRunId,
    JR_Hist.Status,

    CAST(DATEADD(minute, 330, JR_Hist.StartTime) AS DATE) AS ReportDate,
    DATEADD(minute, 330, JR_Hist.StartTime) AS [Start Time in IST],
    DATEADD(minute, 330, JR_Hist.EndTime) AS [End Time in IST],
    CAST(NULL AS TIME) AS [Schedule in IST],

    DATEDIFF(MINUTE, JR_Hist.StartTime, JR_Hist.EndTime) AS [Duration in Mins],

    JR_Hist.RunPageURL,
    JR_Hist.AsOfDate,

    -- Long Running Flag for historical data
    CASE
        WHEN JM.EstimatedDuration IS NULL OR JR_Hist.EndTime IS NULL THEN 'N'
        WHEN JM.EstimatedDuration < 120
             AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.5) < DATEDIFF(MINUTE, JR_Hist.StartTime, JR_Hist.EndTime) THEN 'Y'
        WHEN JM.EstimatedDuration BETWEEN 120 AND 240
             AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.3) < DATEDIFF(MINUTE, JR_Hist.StartTime, JR_Hist.EndTime) THEN 'Y'
        WHEN JM.EstimatedDuration > 240
             AND JM.EstimatedDuration + (JM.EstimatedDuration * 0.15) < DATEDIFF(MINUTE, JR_Hist.StartTime, JR_Hist.EndTime) THEN 'Y'
        ELSE 'N'
    END AS LongRunningFlag

FROM [jobmonitoring].[JobRunsHistory] JR_Hist WITH (NOLOCK)
LEFT JOIN [jobmonitoring].[VwRptJobsMaster] AS JM WITH (NOLOCK) ON JR_Hist.JobId = TRIM(JM.JobId)
WHERE JR_Hist.AsOfDate >= CAST(DATEADD(day, -7, GETUTCDATE()) AS DATE);

GO
