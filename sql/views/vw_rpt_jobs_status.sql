/* ============================================================
   View: [jobmonitoring].[VwRptJobsStatus]
   Purpose: Power BI Dashboard source — status dimension lookup table.
            Provides the four standard status values used for
            Power BI slicer filters on the dashboard.
            
   Statuses: SUCCESS, FAILURE, RUNNING, NOT STARTED
   ============================================================ */

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER VIEW [jobmonitoring].[VwRptJobsStatus]
AS

SELECT
    [Status],
    [StatusDesc]
FROM [jobmonitoring].[jobStatus];

GO
