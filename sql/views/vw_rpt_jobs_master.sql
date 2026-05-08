/* ============================================================
   View: [jobmonitoring].[VwRptJobsMaster]
   Purpose: Power BI Dashboard source — unified master view combining
            both ADF pipelines and Databricks jobs into a single flat table.
   
   Also computes a NotStartedFlag by checking if a job has any
   runs logged for the current day (including overnight jobs).
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[VwRptJobsMaster]
AS

SELECT
    JobId,
    JobPlatform,
    InstanceName,
    JobURL,
    JobName,
    DataProduct,
    IsTracking,
    Schedule,

    -- Determine if the job has started today
    CASE
        WHEN TRIM(JobId) IN (
            SELECT DISTINCT JobId
            FROM [jobmonitoring].[jobRuns] WITH (NOLOCK)
            WHERE AsOfDate = CAST(GETUTCDATE() AS DATE)
               OR (AsOfDate = CAST(DATEADD(day, -1, GETUTCDATE()) AS DATE)
                   AND CAST(StartTime AS TIME) >= '18:30:00')
        ) THEN 'N'  -- Job Found (Started)
        ELSE 'Y'    -- Job Not Found (Not Started)
    END AS NotStartedFlag,

    EstimatedDuration

FROM
(
    -- Databricks jobs
    SELECT
        TRIM(JobId) AS JobId,
        JobType AS JobPlatform,
        WorkspaceId AS InstanceName,
        Schedule,
        JobURL,
        JobName AS JobName,
        DataProduct,
        IsTracking,
        TRY_CONVERT(int, EstimatedDuration) AS EstimatedDuration
    FROM [jobmonitoring].[jobsMaster] WITH (NOLOCK)

    UNION ALL

    -- ADF pipelines
    SELECT
        TRIM(PipelineName) AS JobId,
        JobType AS JobPlatform,
        DataFactory AS InstanceName,
        Schedule,
        JobURL,
        PipelineName AS JobName,
        DataProduct,
        IsTracking,
        TRY_CONVERT(int, EstimatedDuration) AS EstimatedDuration
    FROM [jobmonitoring].[ADFJobsMaster] WITH (NOLOCK)
) AS A;

GO
