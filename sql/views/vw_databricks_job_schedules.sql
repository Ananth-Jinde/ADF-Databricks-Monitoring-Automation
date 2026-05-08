/* ============================================================
   View: [jobmonitoring].[vw_DatabricksJobSchedules]
   Purpose: Determines which Databricks jobs need to be polled in the current cycle.
   
   Section 1 (UNION top): Finds jobs whose schedule falls within a 30-minute
   window and haven't been logged yet today — returns them as 'NOT STARTED'.
   
   Section 2 (UNION bottom): Finds jobs already logged today that are still
   in an active state (RUNNING/QUEUED) — returns them for status re-check.
   
   Note: Databricks uses a 30-minute window (vs ADF's 180-minute) because
   Databricks jobs typically start within seconds of their scheduled time.
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[vw_DatabricksJobSchedules]
AS

-- Section 1: Find jobs scheduled within the last 30 minutes
-- that haven't been logged for this specific schedule today.
SELECT
    JM.JobId,
    JM.WorkspaceId,
    JM.DataProduct,
    'NOT STARTED' AS Status,
    '0' AS RunId,
    LTRIM(RTRIM(spl.Value)) AS Schedule

FROM
    [jobmonitoring].[jobsMaster] AS JM WITH (NOLOCK)

CROSS APPLY
    STRING_SPLIT(JM.Schedule, ',') AS spl

WHERE
    JM.JobType = 'Databricks'
    AND JM.IsTracking = 'Y'

    -- Timing Check: Is the current UTC time within the 30-minute window
    -- starting from the scheduled UTC time?
    -- Example: If schedule is 09:00, this is TRUE between 09:00 and 09:30 UTC.
    AND (
        CASE
            -- Case A: Standard Window (e.g. 14:00 to 14:30). Does NOT cross midnight.
            WHEN TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME) <= DATEADD(minute, 30, TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME))
            THEN
                CASE
                    WHEN CAST(GETUTCDATE() AS TIME) BETWEEN TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME)
                         AND DATEADD(minute, 30, TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME))
                    THEN 1
                    ELSE 0
                END

            -- Case B: Midnight Crossing (e.g. 23:45 to 00:15). Crosses midnight.
            ELSE
                CASE
                    WHEN CAST(GETUTCDATE() AS TIME) >= TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME)
                      OR CAST(GETUTCDATE() AS TIME) <= CAST(DATEADD(minute, 30, TRY_CAST(LTRIM(RTRIM(spl.Value)) AS TIME)) AS TIME)
                    THEN 1
                    ELSE 0
                END
        END
    ) = 1

    -- Duplicate Check: Ensure NO run has already been logged for this
    -- specific job, instance, date, AND schedule time.
    AND NOT EXISTS (
        SELECT 1
        FROM [jobmonitoring].[jobRuns] JR WITH (NOLOCK)
        WHERE JR.JobId = JM.JobId
          AND JR.InstanceName = JM.WorkspaceId
          AND CAST(JR.AsOfDate AS DATE) = CAST(GETUTCDATE() AS DATE)
          AND JR.Schedule = LTRIM(RTRIM(spl.Value))
    )

UNION ALL

-- Section 2: Find jobs already logged today that are currently
-- in an active state — need re-checking for completion.
SELECT
    JM.JobId,
    JM.WorkspaceId,
    JM.DataProduct,
    JR.Status,
    JR.JobRunId AS RunId,
    '' AS Schedule

FROM
    [jobmonitoring].[jobsMaster] AS JM WITH (NOLOCK)

LEFT JOIN
    [jobmonitoring].[jobRuns] AS JR WITH (NOLOCK)
    ON JM.JobId = JR.JobId AND JM.WorkspaceId = JR.InstanceName

WHERE
    JM.JobType = 'Databricks'
    AND JM.IsTracking = 'Y'
    AND JR.Status IN ('RUNNING', 'QUEUED')
    AND (
        -- Track any job logged TODAY
        CAST(JR.AsOfDate AS DATE) = CAST(GETUTCDATE() AS DATE)

        -- OR: Track jobs logged YESTERDAY if they started after 18:00 UTC
        OR (
            CAST(JR.AsOfDate AS DATE) = CAST(DATEADD(day, -1, GETUTCDATE()) AS DATE)
            AND TRY_CAST(JR.StartTime AS TIME) >= '18:00:00'
        )
    );

GO
