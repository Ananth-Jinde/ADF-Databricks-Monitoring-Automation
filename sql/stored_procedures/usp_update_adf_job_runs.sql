/* ============================================================
   Stored Procedure: [jobmonitoring].[UpdateADFJobRuns]
   Purpose: Upserts ADF pipeline run records into the jobRuns table
            using a MERGE statement (INSERT if new, UPDATE if exists).
            
   Also logs failures into the failureLogs table with duplicate
   prevention (one failure record per unique JobRunId).
   
   Status Translation:
     ADF Status    → Normalized Status
     Succeeded     → SUCCESS
     Failed        → FAILURE
     InProgress    → RUNNING
     In progress   → RUNNING
     Other         → UPPER(original)
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [jobmonitoring].[UpdateADFJobRuns]
    @PipelineName NVARCHAR(512),
    @JobRunId NVARCHAR(512),
    @Status NVARCHAR(100),
    @StartTime NVARCHAR(100),
    @EndTime NVARCHAR(100),
    @RunPageURL NVARCHAR(4000),
    @JobParameters NVARCHAR(MAX),
    @ErrorMessage NVARCHAR(MAX),
    @ADFInstance NVARCHAR(512),
    @Schedule NVARCHAR(100)
AS
BEGIN

    SET NOCOUNT ON;

    -- 1. Translate ADF status to normalized values
    DECLARE @TranslatedStatus NVARCHAR(100);
    DECLARE @ConvertedStartTime DATETIME2(3);
    DECLARE @ConvertedEndTime DATETIME2(3);

    SET @TranslatedStatus = CASE
                                WHEN @Status = 'Succeeded' THEN 'SUCCESS'
                                WHEN @Status = 'Failed' THEN 'FAILURE'
                                WHEN @Status = 'InProgress' THEN 'RUNNING'
                                WHEN @Status = 'In progress' THEN 'RUNNING'
                                ELSE UPPER(@Status)
                            END;

    -- 2. Convert ISO timestamp strings to DATETIME2
    SET @ConvertedStartTime = TRY_CAST(@StartTime AS DATETIME2(3));
    SET @ConvertedEndTime = TRY_CAST(@EndTime AS DATETIME2(3));

    -- 3. MERGE (Upsert) into jobRuns
    MERGE INTO [jobmonitoring].[jobRuns] AS Target

    USING (SELECT
              @JobRunId AS JobRunId,
              @PipelineName AS JobId,
              @TranslatedStatus AS Status,
              @ConvertedStartTime AS StartTime,
              @ConvertedEndTime AS EndTime,
              @RunPageURL AS RunPageURL_Part,
              @JobParameters AS JobParameters,
              @ADFInstance AS InstanceName,
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
            CONCAT(
                'https://adf.azure.com/en/monitoring/pipelineruns/', Source.JobRunId,
                '?factory=/subscriptions/',
                SUBSTRING(Source.RunPageURL_Part, 15, 36),
                '/resourceGroups/',
                SUBSTRING(Source.RunPageURL_Part,
                    CHARINDEX('/resourceGroups/', Source.RunPageURL_Part) + 16,
                    CHARINDEX('/providers/', Source.RunPageURL_Part) - (CHARINDEX('/resourceGroups/', Source.RunPageURL_Part) + 16)),
                '/providers/Microsoft.DataFactory/factories/', Source.InstanceName
            ),
            Source.InstanceName, Source.JobParameters, Source.Schedule
        );

    -- 4. Log failures (with duplicate prevention)
    IF (@Status = 'Failed')
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM [jobmonitoring].[failureLogs] WHERE JobRunId = @JobRunId)
        BEGIN
            INSERT INTO [jobmonitoring].[failureLogs] (
                JobId, JobRunId, ErrorCode, ErrorMessage, StartTime, EndTime,
                FailedTime, CreatedDatetime, ModifiedDatetime, AsOfDate,
                RunPageURL, InstanceName, Schedule, JobParameters
            )
            VALUES (
                @PipelineName, @JobRunId, '', @ErrorMessage,
                @ConvertedStartTime,
                @ConvertedEndTime,
                GETUTCDATE(), GETUTCDATE(), GETUTCDATE(), CAST(GETUTCDATE() AS DATE),
                CONCAT(
                    'https://adf.azure.com/en/monitoring/pipelineruns/', @JobRunId,
                    '?factory=/subscriptions/',
                    SUBSTRING(@RunPageURL, 15, 36),
                    '/resourceGroups/',
                    SUBSTRING(@RunPageURL,
                        CHARINDEX('/resourceGroups/', @RunPageURL) + 16,
                        CHARINDEX('/providers/', @RunPageURL) - (CHARINDEX('/resourceGroups/', @RunPageURL) + 16)),
                    '/providers/Microsoft.DataFactory/factories/', @ADFInstance
                ),
                @ADFInstance, @Schedule, @JobParameters
            );
        END
    END

END
GO
