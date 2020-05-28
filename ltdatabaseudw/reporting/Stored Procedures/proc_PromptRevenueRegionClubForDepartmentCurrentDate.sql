CREATE PROC [reporting].[proc_PromptRevenueRegionClubForDepartmentCurrentDate] @DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

 IF 1=0 BEGIN
       SET FMTONLY OFF
     END


DECLARE @StartCalendarDate DATETIME,
        @EndCalendarDate DATETIME

SET @StartCalendarDate = DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0) 
SET @EndCalendarDate = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)

exec [reporting].[proc_PromptRevenueRegionClubForDepartmentDate] @StartCalendarDate, @EndCalendarDate, @DepartmentMinDimReportingHierarchyKeyList, @DivisionList, @SubDivisionList

END

