/* ============================================================
   Stored Procedure: [jobmonitoring].[UpdateDatabricksJobRuns]
   Purpose: Upserts Databricks job run records into the jobRuns table
            using a MERGE statement (INSERT if new, UPDATE if exists).
            
   Handles epoch millisecond to DATETIME2 conversion for Databricks
   timestamps (e.g., 1714567890000 → 2024-05-01 12:31:30.000).
   
   Also logs failures into the failureLogs table with duplicate
   prevention (one failure record per unique JobRunId).
   
   Status Translation:
     Databricks Status → Normalized Status
     SUCCESS           → SUCCESS
     FAILED            → FAILURE
     TIMEDOUT          → FAILURE
     CANCELED          → CANCELED
     RUNNING           → RUNNING
     PENDING           → QUEUED
     QUEUED            → QUEUED
     TERMINATING       → RUNNING
     SKIPPED           → SKIPPED
     INTERNAL_ERROR    → FAILURE
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [jobmonitoring].[UpdateDatabricksJobRuns]
    @JobId NVARCHAR(512),
    @JobRunId NVARCHAR(512),
    @Status NVARCHAR(100),
    @StartTime NVARCHAR(100),
    @EndTime NVARCHAR(100),
    @RunPageURL NVARCHAR(4000),
    @ErrorCode NVARCHAR(512),
    @ErrorMessage NVARCHAR(4000),
    @Instance NVARCHAR(512),
    @Schedule NVARCHAR(100),
    @JobParameters NVARCHAR(MAX)
AS
BEGIN

    SET NOCOUNT ON;

    -- 1. Translate Databricks status to normalized values
    DECLARE @TranslatedStatus NVARCHAR(100);

    SET @TranslatedStatus = CASE
                                WHEN @Status = 'SUCCESS' THEN 'SUCCESS'
                                WHEN @Status = 'FAILED' THEN 'FAILURE'
                                WHEN @Status = 'TIMEDOUT' THEN 'FAILURE'
                                WHEN @Status = 'CANCELED' THEN 'CANCELED'
                                WHEN @Status = 'RUNNING' THEN 'RUNNING'
                                WHEN @Status = 'PENDING' THEN 'QUEUED'
                                WHEN @Status = 'QUEUED' THEN 'QUEUED'
                                WHEN @Status = 'TERMINATING' THEN 'RUNNING'
                                WHEN @Status = 'SKIPPED' THEN 'SKIPPED'
                                WHEN @Status = 'INTERNAL_ERROR' THEN 'FAILURE'
                                ELSE UPPER(@Status)
                            END;

    -- 2. Convert epoch millisecond timestamps to DATETIME2
    --    Databricks returns start_time/end_time as epoch ms (e.g. 1712345678000)
    DECLARE @ConvertedStartTime DATETIME2(3);
    DECLARE @ConvertedEndTime DATETIME2(3);

    SET @ConvertedStartTime = CASE
                                  WHEN TRY_CAST(@StartTime AS BIGINT) IS NOT NULL
                                       AND TRY_CAST(@StartTime AS BIGINT) > 0
                                  THEN DATEADD(MILLISECOND,
                                               TRY_CAST(@StartTime AS BIGINT) % 1000,
                                               DATEADD(SECOND,
                                                       TRY_CAST(@StartTime AS BIGINT) / 1000,
                                                       '1970-01-01'))
                                  ELSE NULL
                              END;

    SET @ConvertedEndTime = CASE
                                WHEN TRY_CAST(@EndTime AS BIGINT) IS NOT NULL
                                     AND TRY_CAST(@EndTime AS BIGINT) > 0
                                THEN DATEADD(MILLISECOND,
                                             TRY_CAST(@EndTime AS BIGINT) % 1000,
                                             DATEADD(SECOND,
                                                     TRY_CAST(@EndTime AS BIGINT) / 1000,
                                                     '1970-01-01'))
                                ELSE NULL
                            END;

    -- 3. MERGE (Upsert) into jobRuns
    MERGE INTO [jobmonitoring].[jobRuns] AS Target

    USING (SELECT
              @JobRunId AS JobRunId,
              @JobId AS JobId,
              @TranslatedStatus AS Status,
              @ConvertedStartTime AS StartTime,
              @ConvertedEndTime AS EndTime,
              @RunPageURL AS RunPageURL,
              @JobParameters AS JobParameters,
              @Instance AS InstanceName,
              @Schedule AS Schedule
          ) AS Source

    ON (Target.JobRunId = Source.JobRunId)

    -- If JobRunId already exists, UPDATE status and timestamps
    WHEN MATCHED THEN
        UPDATE SET
            Target.Status = Source.Status,
            Target.EndTime = Source.EndTime,
            Target.ModifiedDatetime = GETUTCDATE(),
            Target.JobParameters = Source.JobParameters

    -- If JobRunId does not exist, INSERT new record
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            JobId, JobRunId, Status, StartTime, EndTime,
            CreatedDatetime, ModifiedDatetime, AsOfDate,
            RunPageURL, InstanceName, JobParameters, Schedule
        )
        VALUES (
            Source.JobId, Source.JobRunId, Source.Status,
            Source.StartTime, Source.EndTime,
            GETUTCDATE(), GETUTCDATE(), CAST(GETUTCDATE() AS DATE),
            Source.RunPageURL, Source.InstanceName,
            Source.JobParameters, Source.Schedule
        );

    -- 4. Log failures (with duplicate prevention)
    IF (@TranslatedStatus = 'FAILURE')
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM [jobmonitoring].[failureLogs] WHERE JobRunId = @JobRunId)
        BEGIN
            INSERT INTO [jobmonitoring].[failureLogs] (
                JobId, JobRunId, ErrorCode, ErrorMessage, StartTime, EndTime,
                FailedTime, CreatedDatetime, ModifiedDatetime, AsOfDate,
                RunPageURL, InstanceName, Schedule, JobParameters
            )
            VALUES (
                @JobId, @JobRunId, @ErrorCode, @ErrorMessage,
                @ConvertedStartTime,
                @ConvertedEndTime,
                GETUTCDATE(), GETUTCDATE(), GETUTCDATE(), CAST(GETUTCDATE() AS DATE),
                @RunPageURL, @Instance, @Schedule, @JobParameters
            );
        END
    END

END
GO
