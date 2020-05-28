CREATE PROC [reporting].[proc_PromptOperationsReportingDepartmentHierarchyActive] AS
BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

--- Sample Execution
-- Exec [reporting].[proc_PromptOperationsReportingDepartmentHierarchyActive]
---
 
-- Create Hierarchy temp table to return all groups      

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchy; 

SELECT reporting_region_type AS RegionType,
       reporting_division AS DivisionName,
       reporting_sub_division AS SubdivisionName,
       reporting_department AS DepartmentName,
       reporting_product_group AS ProductGroupName,
       dim_reporting_hierarchy_key AS DimReportingHierarchyKey
  INTO #DimReportingHierarchy
  FROM [marketing].[v_dim_reporting_hierarchy]
 WHERE dim_reporting_hierarchy_key not in('-997','-998','-999')


IF OBJECT_ID('tempdb.dbo.#DepartmentMinKeys', 'U') IS NOT NULL DROP TABLE #DepartmentMinKeys; 

SELECT DivisionName,SubdivisionName,DepartmentName,MIN(DimReportingHierarchyKey) MinKey
INTO #DepartmentMinKeys
FROM #DimReportingHierarchy
GROUP BY DivisionName,SubdivisionName,DepartmentName

-- Final result set
SELECT DISTINCT
       #DimReportingHierarchy.RegionType,
       #DimReportingHierarchy.DivisionName,
       #DimReportingHierarchy.SubdivisionName,
       #DimReportingHierarchy.DepartmentName,
       #DimReportingHierarchy.ProductGroupName,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       #DepartmentMinKeys.MinKey DepartmentMinDimReportingHierarchyKey
  FROM #DimReportingHierarchy
  JOIN #DepartmentMinKeys
    ON #DimReportingHierarchy.DivisionName = #DepartmentMinKeys.DivisionName
   AND #DimReportingHierarchy.SubdivisionName = #DepartmentMinKeys.SubdivisionName
   AND #DimReportingHierarchy.DepartmentName = #DepartmentMinKeys.DepartmentName
Order by 1,2,3,4,5

DROP TABLE #DimReportingHierarchy
DROP TABLE #DepartmentMinKeys

END

