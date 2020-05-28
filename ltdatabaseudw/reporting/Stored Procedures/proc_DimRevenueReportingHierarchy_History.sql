CREATE PROC [reporting].[proc_DimRevenueReportingHierarchy_History] @DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@StartMonthStartingDimDateKey [VARCHAR](32),@EndMonthEndingDimDateKey [VARCHAR](32) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

----- This stored procedure is created to replicate the functionality of the LTFDM_Operations function
----- "fnRevenueDimReportingHierarchy";  This is to be called by a reporting stored procedure
----- which needs a "History" instance of the reporting hierarchy



----- Execution Sample
--- Exec [reporting].[proc_DimRevenueReportingHierarchy_History] 'All Divisions','All Subdivisions','All Departments','N/A','20181001','20181031'
-----


DECLARE @StartMonthEndingDimDateKey VARCHAR(32)
DECLARE @EndingMonthEndingDimDateKey VARCHAR(32)
SET @StartMonthEndingDimDateKey = (SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartMonthStartingDimDateKey)
SET @EndingMonthEndingDimDateKey = (SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndMonthEndingDimDateKey)

SET @DivisionList = CASE WHEN @DivisionList = 'N/A' 
                         THEN 'All Divisions' 
						 ELSE @DivisionList END
SET @SubdivisionList = CASE WHEN @SubdivisionList = 'N/A' 
                            THEN 'All Subdivisions' 
							ELSE @SubdivisionList END
SET @DepartmentMinDimReportingHierarchyKeyList = CASE WHEN @DepartmentMinDimReportingHierarchyKeyList = 'N/A' 
                                                      THEN 'All Departments' 
													  ELSE @DepartmentMinDimReportingHierarchyKeyList END
SET @DimReportingHierarchyKeyList = CASE WHEN @DimReportingHierarchyKeyList= 'N/A' 
                                         THEN 'All Product Groups' 
										 ELSE @DimReportingHierarchyKeyList  END


------ The "Header..." columns previously returned by the LTFDM function will not be created with this stored procedure
------ These strings can be more efficiently processed individually, as needed, within the reporting stored procedure
------  or within Cognos itself, based on the prompted values

----- Using performance enhancement replacement code replacing exec [marketing].[proc_parse_pipe_list] with
----- JOIN (SELECT @pipe_list pl) pl  ON '|'+pl.pl+'|' like '%|'+Hier.reporting_division+'|%' OR '|'+pl.pl+'|' like '%|All Divisions|%' 
----- in each temp table creation, as suggested in e-mail from Brian D. dated 4/18/19 3:15 PM

 ----- Create Division Temp table

IF OBJECT_ID('tempdb.dbo.#Divisions', 'U') IS NOT NULL
  DROP TABLE #Divisions; 

DECLARE @pipe_list varchar(8000) 
SET @pipe_list = @DivisionList 

SELECT DISTINCT Hier.reporting_division AS DivisionName
INTO #Divisions
FROM [marketing].[v_dim_reporting_hierarchy_history] Hier
JOIN (SELECT @pipe_list pl) pl
  ON '|'+pl.pl+'|' like '%|'+Hier.reporting_division+'|%'
    OR '|'+pl.pl+'|' like '%|All Divisions|%'
WHERE Hier.effective_dim_date_key <= @EndingMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthEndingDimDateKey


 ----- Create Subdivision Temp table

IF OBJECT_ID('tempdb.dbo.#Subdivisions', 'U') IS NOT NULL
  DROP TABLE #Subdivisions;

SET @pipe_list = @SubdivisionList 

SELECT DISTINCT  Hier.reporting_sub_division AS SubdivisionName
INTO #Subdivisions
FROM [marketing].[v_dim_reporting_hierarchy_history] Hier
JOIN (SELECT @pipe_list pl) pl
  ON '|'+pl.pl+'|' like '%|'+Hier.reporting_sub_division+'|%'
    OR '|'+pl.pl+'|' like '%|All Subdivisions|%'
WHERE Hier.effective_dim_date_key <= @EndingMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthEndingDimDateKey

 ----- Create Department Temp table

IF OBJECT_ID('tempdb.dbo.#Departments', 'U') IS NOT NULL
  DROP TABLE #Departments;

 SET @pipe_list = @DepartmentMinDimReportingHierarchyKeyList 

SELECT DISTINCT  Hier.reporting_department AS reporting_department
INTO #Departments
FROM [marketing].[v_dim_reporting_hierarchy_history] Hier
JOIN (SELECT @pipe_list pl) pl
  ON '|'+pl.pl+'|' like '%|'+Hier.dim_reporting_hierarchy_key+'|%'
    OR '|'+pl.pl+'|' like '%|All Departments|%'
WHERE Hier.effective_dim_date_key <= @EndingMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthEndingDimDateKey




 ----- Create Product Group Temp table

IF OBJECT_ID('tempdb.dbo.#HierarchyKey', 'U') IS NOT NULL
  DROP TABLE #HierarchyKey;

 SET @pipe_list = @DimReportingHierarchyKeyList 

SELECT DISTINCT Hier.dim_reporting_hierarchy_key AS DimReportingHierarchyKey
INTO #HierarchyKey
FROM [marketing].[v_dim_reporting_hierarchy_history] Hier
JOIN (SELECT @pipe_list pl) pl
  ON '|'+pl.pl+'|' like '%|'+Hier.dim_reporting_hierarchy_key+'|%'
    OR '|'+pl.pl+'|' like '%|All Product Groups|%'
WHERE Hier.effective_dim_date_key <= @EndingMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthEndingDimDateKey



 ----- Create the preliminary output Temp table
IF OBJECT_ID('tempdb.dbo.#OutputTable', 'U') IS NOT NULL
  DROP TABLE #OutputTable;

SELECT 
       DimReportingHierarchy.reporting_division DivisionName,
       DimReportingHierarchy.reporting_sub_division SubdivisionName,
       DimReportingHierarchy.reporting_department DepartmentName,
	   DimReportingHierarchy.reporting_product_group ProductGroupName,
	   DimReportingHierarchy.dim_reporting_hierarchy_key DimReportingHierarchyKey,
       DimReportingHierarchy.reporting_region_type RegionType
 INTO #OutputTable
  FROM [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
  JOIN #Divisions AS DivisionList
    ON DimReportingHierarchy.reporting_division = DivisionList.DivisionName
    OR DivisionList.DivisionName = 'All Divisions'
  JOIN #Subdivisions AS SubdivisionList
    ON DimReportingHierarchy.reporting_sub_division = SubdivisionList.SubdivisionName
    OR SubdivisionList.SubdivisionName = 'All Subdivisions'
  JOIN #Departments AS DepartmentList
    ON DimReportingHierarchy.reporting_department = DepartmentList.reporting_department
  JOIN #HierarchyKey ProductGroupHierarchyKeyList
    ON DimReportingHierarchy.dim_reporting_hierarchy_key = ProductGroupHierarchyKeyList.DimReportingHierarchyKey
	  OR ProductGroupHierarchyKeyList.DimReportingHierarchyKey = 'All Product Groups'
  WHERE DimReportingHierarchy.effective_dim_date_key  <= @EndingMonthEndingDimDateKey    
     AND DimReportingHierarchy.expiration_dim_date_key >= @StartMonthEndingDimDateKey
	 AND DimReportingHierarchy.Dim_Reporting_Hierarchy_Key not in('-997', '-998', '-999')
	 AND DimReportingHierarchy.reporting_product_group <> ''
	
  GROUP BY DimReportingHierarchy.reporting_division,
       DimReportingHierarchy.reporting_sub_division,
       DimReportingHierarchy.reporting_department,
	   DimReportingHierarchy.reporting_product_group,
	   DimReportingHierarchy.dim_reporting_hierarchy_key,
       DimReportingHierarchy.reporting_region_type


DECLARE @ReportRegionType VARCHAR(50)
SET @ReportRegionType = (Select CASE WHEN COUNT(DISTINCT RegionType) = 1 
                                     THEN MIN(RegionType) 
			                         ELSE 'MMS Region' 
			                         END
                             FROM #OutputTable)




 ----- Note from Brian D. 10/26/18
 ----- In answer to the question of how to set up a stored procedure to replace the functions used in LTFDM.  In this case, replacing "fnRevenueDimReportingHierarchy" with an "Active" version stored procedure
 ----- The syntax between DW and on-prem is quite different.  
 ----- What I’ve been doing is storing the results of a stored procedure in a temp table and just not dropping it.  
 ----- There’s a change in scoping in azure compared to on-premise, and temp tables end up somewhere between # and ## tables on premise.
 ----- Meaning, any temp table created within a called stored procedure is available to the calling code.  
 ----- So if you adjust proc_DimReportingHierarchy to insert into a temp table instead of whatever returns results, you’ll have your temp table outside of the procedure call.
 ----- If it messes with any prompting pages or anything else that calls the procedure, you’ll either need a 2nd version with different code, or an additional parameter passed in that toggles either a returned result set or temp table creation.




 ----- Create the preliminary output Temp table
IF OBJECT_ID('tempdb.dbo.#OuterOutputTable', 'U') IS NOT NULL
  DROP TABLE #OuterOutputTable;

SELECT DimReportingHierarchyKey,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   @ReportRegionType AS ReportRegionType
 INTO #OuterOutputTable     ------- Match name in outer query to call this temp table
FROM #OutputTable



END
