CREATE PROC [reporting].[proc_PromptRevenueRegionClubForDepartmentDate] @StartDate [DATETIME],@EndDate [DATETIME],@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
--- Exec [reporting].[proc_PromptRevenueRegionClubForDepartmentDate] '6/1/2019','6/30/2019','All Departments','Activities','Aquatics'
---


SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                       THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0) 
			 WHEN @StartDate = 'Jan 1, 1901' 
                       THEN @EndDate 
			 ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                     THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101) 
		      WHEN @EndDate = 'Jan 1, 1901' 
                     THEN @StartDate 
		       ELSE @EndDate END

DECLARE @StartMonthStartingDimDateKey INT
DECLARE @EndMonthEndingDimDateKey INT

SET @StartMonthStartingDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @EndMonthEndingDimDateKey = (SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndDate)



 ----- Create Divisions Temp table
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
WHERE Hier.effective_dim_date_key <= @EndMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthStartingDimDateKey

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
WHERE Hier.effective_dim_date_key <= @EndMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthStartingDimDateKey

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
WHERE Hier.effective_dim_date_key <= @EndMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthStartingDimDateKey


  ----- Create Region Type temp table   based on hierarchies selected
IF OBJECT_ID('tempdb.dbo.#RegionTypes', 'U') IS NOT NULL
  DROP TABLE #RegionTypes; 

SELECT 
       DimReportingHierarchy.reporting_region_type RegionType
 INTO #RegionTypes
  FROM [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
  JOIN #Divisions AS DivisionList
    ON DimReportingHierarchy.reporting_division = DivisionList.DivisionName
    OR DivisionList.DivisionName = 'All Divisions'
  JOIN #Subdivisions AS SubdivisionList
    ON DimReportingHierarchy.reporting_sub_division = SubdivisionList.SubdivisionName
    OR SubdivisionList.SubdivisionName = 'All Subdivisions'
  JOIN #Departments AS DepartmentList
    ON DimReportingHierarchy.reporting_department = DepartmentList.reporting_department
  WHERE DimReportingHierarchy.effective_dim_date_key  <= @EndMonthEndingDimDateKey   
     AND DimReportingHierarchy.expiration_dim_date_key >= @StartMonthStartingDimDateKey
  GROUP BY  DimReportingHierarchy.reporting_region_type

 ---- Set variable to return just one region type
DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT CASE WHEN COUNT(*) = 1 THEN MIN(RegionType) ELSE 'MMS Region' END FROM #RegionTypes)



SELECT CASE WHEN @RegionType = 'PT RCL Area' THEN PTRCLAreaDescription.description
            WHEN @RegionType = 'Member Activities Region' THEN MARegionDescription.description
            WHEN @RegionType = 'MMS Region' THEN MMSRegionDescription.description 
			END   ReportingRegionName
       ,DimClub.club_code +' - '+ DimClub.club_name  AS ClubCodeDashClubName
	   ,DimClub.dim_club_key as DimClubKey    ------- New Name		  
	   ,CASE WHEN DimClub.club_open_dim_date_key > @EndMonthEndingDimDateKey
	        THEN 'Presale'
			ELSE 'Open'
			END  ClubStatusDescription
	   ,DimClub.club_id AS MMSClubID 
	   ,DimClub.club_code AS ClubCode
	   ,DimClub.club_name AS ClubName
       ,DimClub.local_currency_code AS LocalCurrencyCode
	   ,DimClub.gl_club_id AS GLClubID
	   --,DimClub.club_type
	   --,DimClub.club_close_dim_date_key
	   --,DimClub.club_open_dim_date_key
	   --,DimClub.created_dim_date_key
	   --,DimClub.open_dim_date_key
	   ,ClubOpenDate.calendar_date AS ClubOpenDate
	   ,ClubCloseDate.calendar_date AS ClubCloseDate
  FROM [marketing].[v_dim_club] DimClub
    JOIN [marketing].[v_dim_description] PTRCLAreaDescription
	  ON DimClub.pt_rcl_area_dim_description_key = PTRCLAreaDescription.dim_description_key
    JOIN [marketing].[v_dim_description] MARegionDescription
	  ON DimClub.member_activities_region_dim_description_key = MARegionDescription.dim_description_key
    JOIN [marketing].[v_dim_description] MMSRegionDescription
	  ON DimClub.region_dim_description_key = MMSRegionDescription.dim_description_key
    LEFT JOIN [marketing].[v_dim_date] ClubOpenDate
	  ON DimClub.club_open_dim_date_key = ClubOpenDate.dim_date_key
     LEFT JOIN [marketing].[v_dim_date] ClubCloseDate
	  ON DimClub.club_close_dim_date_key = ClubCloseDate.dim_date_key
 WHERE DimClub.club_id IS NOT NULL
   AND DimClub.club_id NOT IN (-1,99,100)
   AND DimClub.club_id < 900
   AND DimClub.club_type = 'Club'
   AND (DimClub.club_close_dim_date_key <= '-997' 
         OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)

DROP TABLE #RegionTypes
DROP TABLE #Departments
DROP TABLE #Subdivisions
DROP TABLE #Divisions

END
