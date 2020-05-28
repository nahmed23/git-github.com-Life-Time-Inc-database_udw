CREATE PROC [reporting].[procCognos_RealTimeSalesByReportingDepartment_ReportingHierarchy_Today] @DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END
 
DECLARE @StartDate Datetime
, @EndDate Datetime

SET @StartDate = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)  -- Returns Today's date
SET @EndDate = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)  -- Returns Today's date

DECLARE @HeaderDateRange Varchar(110)
SET @HeaderDateRange = Replace(Substring(convert(varchar, @StartDate, 100),1,6)+', '+Substring(convert(varchar, @StartDate, 100),8,4),'  ',' ')

-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
						  from map_utc_time_zone_conversion
						  where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


------- Create Hierarchy temp table to return selected group names 
------  Extra steps needed to return the DepartmentyMinDimReportingHierarchyKey     
Exec [reporting].[proc_DimReportingHierarchy] @DivisionList,@SubdivisionList,@DepartmentMinDimreportingHierarchyKeyList,'N/A'
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy_Prelim', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy_Prelim; 

SELECT DimReportingHierarchyKey,  
       DivisionName,    
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType
INTO #DimReportingHierarchy_Prelim   
FROM #OuterOutputTable
 

IF OBJECT_ID('tempdb.dbo.#DepartmentGrouping', 'U') IS NOT NULL
  DROP TABLE #DepartmentGrouping; 

 SELECT MIN(DimReportingHierarchyKey) AS DepartmentMinDimReportingHierarchyKey,
        DivisionName,    
        SubdivisionName,
        DepartmentName
 INTO #DepartmentGrouping
 FROM #DimReportingHierarchy_Prelim
 GROUP BY DivisionName,    
        SubdivisionName,
        DepartmentName

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

SELECT Prelim.DimReportingHierarchyKey,  
       DeptGroup.DepartmentMinDimReportingHierarchyKey,
       Prelim.DivisionName,    
       Prelim.SubdivisionName,
       Prelim.DepartmentName,
	   Prelim.ProductGroupName,
	   1 ProductGroupSortOrder,
	   Prelim.RegionType
INTO #DimReportingHierarchy
FROM #DimReportingHierarchy_Prelim Prelim
 JOIN #DepartmentGrouping DeptGroup
   ON Prelim.DivisionName = DeptGroup.DivisionName
   AND Prelim.SubdivisionName = DeptGroup.SubdivisionName
   AND Prelim.DepartmentName = DeptGroup.DepartmentName

DECLARE @StartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = DimDate.[dim_date_key]
FROM [marketing].[v_dim_date] DimDate
WHERE DimDate.[calendar_date] = @StartDate

SELECT Hierarchy.DepartmentName AS RevenueReportingDepartmentName
	, @HeaderDateRange AS HeaderDateRange  
	, ProductGroupSortOrder AS SortOrder 
	, @ReportRunDateTime AS ReportRunDateTime
	, 0.00 AS GoalAmount
	, '' AS RevenueReportingDepartmentNameCommaList -- Calculate in report 
	, 0 AS ClubPriorYearActual
	, 0 AS RegionPriorYearActual
	, 0 AS StatusPriorYearActual
	, 0 AS ReportPriorYearActual
	, 0 AS SSSGClubPromptYearActual
	, '' AS HeaderDivisionList -- Calculate in report 
	, '' AS HeaderSubdivisionList -- Calculate in report 
	, @DepartmentMinDimreportingHierarchyKeyList DepartmentMinDimReportingHierarchyKeyList 
	, MagentoProduct.product_id
	, MagentoProduct.product_name
	, MagentoProduct.sku
FROM marketing.v_dim_magento_product MagentoProduct
JOIN #DimReportingHierarchy Hierarchy
     on MagentoProduct.dim_reporting_hierarchy_key = Hierarchy.DimReportingHierarchyKey


DROP TABLE #DimReportingHierarchy_Prelim
DROP TABLE #DepartmentGrouping
DROP TABLE #DimReportingHierarchy


END

