CREATE PROC [reporting].[proc_PromptRevenueReportingDepartmentHierarchyForDate] @StartDate [DateTime],@EndDate [DateTime] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END



------ Sample Execution
----- Exec reporting.proc_PromptRevenueReportingDepartmentHierarchyForDate '6/1/2019','1/1/1901'
------

DECLARE @StartCalendarDate DATETIME
DECLARE @StartCalendarDateDimDateKey VARCHAR(32)
DECLARE @EndCalendarDate DATETIME
DECLARE @EndCalendarDateDimDateKey VARCHAR(32)  


SELECT @StartCalendarDate = CASE WHEN Cast(@StartDate AS Date) = 'Jan 1, 1901' 
                                      THEN Cast(@EndDate AS Date)         
                                 WHEN Cast(@StartDate AS Date) = 'Jan 1, 1900' 
								      THEN month_starting_date
                                 ELSE Cast(@StartDate AS Date) END,
       @EndCalendarDate = CASE WHEN Cast(@EndDate AS Date)  = 'Jan 1, 1901' 
	                              THEN Cast(@StartDate AS Date)  
                               WHEN Cast(@EndDate AS Date) = 'Jan 1, 1900' 
							      THEN calendar_date
                               ELSE Cast(@EndDate AS Date) END
  FROM [marketing].[v_dim_date]
 WHERE calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate()-1,101),101)

 SET @StartCalendarDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartCalendarDate)
 SET @EndCalendarDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndCalendarDate)


IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

SELECT reporting_region_type AS RegionType,
       reporting_division AS DivisionName,
       reporting_sub_division AS SubdivisionName,
       reporting_department AS DepartmentName,
       reporting_product_group AS ProductGroupName,
       1 AS ProductGroupSortOrder,
       dim_reporting_hierarchy_key AS DimReportingHierarchyKey
  INTO #DimReportingHierarchy
  FROM [marketing].[v_dim_reporting_hierarchy_history]
 WHERE effective_dim_date_key <= @EndCalendarDateDimDateKey
   AND expiration_dim_date_key > @StartCalendarDateDimDateKey
   AND dim_reporting_hierarchy_key > '0'


IF OBJECT_ID('tempdb.dbo.#DepartmentMinKeys', 'U') IS NOT NULL
  DROP TABLE #DepartmentMinKeys;

SELECT DivisionName,
       SubdivisionName,
       DepartmentName,
       MIN(DimReportingHierarchyKey) MinKey
  INTO #DepartmentMinKeys
  FROM #DimReportingHierarchy
 GROUP BY DivisionName,
          SubdivisionName,
          DepartmentName

SELECT #DimReportingHierarchy.RegionType,
       #DimReportingHierarchy.DivisionName,
       #DimReportingHierarchy.SubdivisionName,
       #DimReportingHierarchy.DepartmentName,
       #DimReportingHierarchy.ProductGroupName,
       #DimReportingHierarchy.ProductGroupSortOrder,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       Cast(#DepartmentMinKeys.MinKey as Varchar(50)) DepartmentMinDimReportingHierarchyKey
  FROM #DimReportingHierarchy
  JOIN #DepartmentMinKeys
    ON #DimReportingHierarchy.DivisionName = #DepartmentMinKeys.DivisionName
   AND #DimReportingHierarchy.SubdivisionName = #DepartmentMinKeys.SubdivisionName
   AND #DimReportingHierarchy.DepartmentName = #DepartmentMinKeys.DepartmentName

END

