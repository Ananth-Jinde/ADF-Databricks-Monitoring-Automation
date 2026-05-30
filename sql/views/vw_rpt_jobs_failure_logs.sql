/* ============================================================
   View: [jobmonitoring].[VwRptJobsFailureLogs]
   Purpose: Power BI Dashboard source — failure details with IST
            time conversion. Combines live failures (failureLogs)
             with historical failures (JobRunsHistory) for
            failure trending and analysis.
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[VwRptJobsFailureLogs]
AS

-- === Section 1: LIVE Failures ===
SELECT
    TRIM(JobId) AS JobId,
    JobRunId,
    ErrorMessage,
    CAST(DATEADD(minute, 330, StartTime) AS DATE) AS ReportDate,
    DATEADD(minute, 330, StartTime) AS [Start Time in IST],
    DATEADD(minute, 330, EndTime) AS [End Time in IST],
    DATEADD(minute, 330, FailedTime) AS [Failed Time in IST],
    Schedule AS [Schedule in IST],

    CASE
        WHEN CAST(EndTime AS DATE) = '1970-01-01' OR EndTime IS NULL
        THEN -1
        ELSE DATEDIFF(MINUTE, StartTime, EndTime)
    END AS [Duration in Mins],

    RunPageURL,
    AsOfDate

FROM [jobmonitoring].[failureLogs] WITH (NOLOCK)

UNION ALL

-- === Section 2: HISTORICAL Failures ===
SELECT
    TRIM(JobId),
    JobRunId,
    ErrorMessage,
    CAST(DATEADD(minute, 330, StartTime) AS DATE) AS ReportDate,
    DATEADD(minute, 330, StartTime) AS [Start Time in IST],
    DATEADD(minute, 330, EndTime) AS [End Time in IST],
    DATEADD(minute, 330, EndTime) AS [Failed Time in IST],
    CAST(NULL AS NVARCHAR(100)) AS [Schedule in IST],

    DATEDIFF(MINUTE, StartTime, EndTime) AS [Duration in Mins],

    RunPageURL,
    AsOfDate

FROM [jobmonitoring].[JobRunsHistory] WITH (NOLOCK)
WHERE Status IN ('FAILURE', 'FAILED', 'Failed')
  AND AsOfDate >= CAST(DATEADD(day, -7, GETUTCDATE()) AS DATE);

GO
