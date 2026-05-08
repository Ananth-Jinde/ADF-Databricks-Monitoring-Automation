/* ============================================================
   View: [jobmonitoring].[vw_ADFJobSchedules]
   Purpose: Determines which ADF pipelines need to be polled in the current cycle.
   
   Section 1 (UNION top): Finds pipelines whose schedule falls within a 180-minute
   window and haven't been logged yet today — returns them as 'NOT STARTED'.
   
   Section 2 (UNION bottom): Finds pipelines already logged today that are still
   in an active state (RUNNING/QUEUED) — returns them for status re-check.
   
   Includes midnight-crossing logic for schedules like 22:00 → 01:00.
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[vw_ADFJobSchedules]
AS

-- Section 1: Find jobs scheduled within the last 180 minutes
-- that haven't been logged for this specific schedule today.
SELECT
    JM.SubscriptionId,
    JM.CIID,
    JM.ClientSecret,
    JM.TenantId,
    JM.ResourceGroup,
    JM.DataFactory,
    JM.PipelineName,
    JM.DataProduct,
    'NOT STARTED' AS Status,
    '0' AS RunId,
    spl.Value AS Schedule

FROM
    [jobmonitoring].ADFJobsMaster AS JM WITH (NOLOCK)

CROSS APPLY
    STRING_SPLIT(JM.Schedule, ',') AS spl

WHERE
    JM.JobType = 'ADF'
    AND JM.IsTracking = 'Y'

    -- Timing Check: Is the current UTC time within the 180-minute window
    -- starting from the scheduled UTC time?
    -- Example: If schedule is 09:00, this is TRUE between 09:00 and 12:00 UTC.
    AND (
        CASE
            -- Case A: Standard Window (e.g. 14:00 to 17:00). Does NOT cross midnight.
            WHEN TRY_CAST(spl.Value AS TIME) <= DATEADD(minute, 180, TRY_CAST(spl.Value AS TIME))
            THEN
                CASE
                    WHEN CAST(GETUTCDATE() AS TIME) BETWEEN TRY_CAST(spl.Value AS TIME)
                         AND DATEADD(minute, 180, TRY_CAST(spl.Value AS TIME))
                    THEN 1
                    ELSE 0
                END

            -- Case B: Midnight Crossing (e.g. 22:00 to 01:00). Crosses midnight.
            -- Check if Current Time is >= 22:00 OR <= 01:00
            ELSE
                CASE
                    WHEN CAST(GETUTCDATE() AS TIME) >= TRY_CAST(spl.Value AS TIME)
                      OR CAST(GETUTCDATE() AS TIME) <= CAST(DATEADD(minute, 180, TRY_CAST(spl.Value AS TIME)) AS TIME)
                    THEN 1
                    ELSE 0
                END
        END
    ) = 1

    -- Duplicate Check: Ensure NO run has already been logged for this
    -- specific pipeline, instance, date, AND schedule time.
    AND NOT EXISTS (
        SELECT 1
        FROM [jobmonitoring].[jobRuns] JR WITH (NOLOCK)
        WHERE JR.JobId = JM.PipelineName
          AND JR.InstanceName = JM.DataFactory
          AND CAST(JR.AsOfDate AS DATE) = CAST(GETUTCDATE() AS DATE)
          AND JR.Schedule = spl.Value
    )

UNION ALL

-- Section 2: Find jobs that are already logged today and currently
-- in a running state — need re-checking for completion.
SELECT
    JM.SubscriptionId,
    JM.CIID,
    JM.ClientSecret,
    JM.TenantId,
    JM.ResourceGroup,
    JM.DataFactory,
    JM.PipelineName,
    JM.DataProduct,
    JR.Status,
    JR.JobRunId,
    '' AS Schedule

FROM
    [jobmonitoring].ADFJobsMaster AS JM WITH (NOLOCK)

LEFT JOIN
    [jobmonitoring].[jobRuns] AS JR WITH (NOLOCK)
    ON JM.PipelineName = JR.JobId AND JM.DataFactory = JR.InstanceName

WHERE
    JM.JobType = 'ADF'
    AND JM.IsTracking = 'Y'
    AND JR.Status IN ('RUNNING', 'InProgress', 'QUEUED')
    AND (
        -- Track any job logged TODAY
        CAST(JR.AsOfDate AS DATE) = CAST(GETUTCDATE() AS DATE)

        -- OR: Track jobs logged YESTERDAY if they started after 18:00 UTC
        -- (handles overnight-running pipelines)
        OR (
            CAST(JR.AsOfDate AS DATE) = CAST(DATEADD(day, -1, GETUTCDATE()) AS DATE)
            AND TRY_CAST(JR.StartTime AS TIME) >= '18:00:00'
        )
    );

GO
