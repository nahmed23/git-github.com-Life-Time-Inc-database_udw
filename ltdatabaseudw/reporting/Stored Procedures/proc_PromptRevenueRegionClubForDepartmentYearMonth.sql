CREATE PROC [reporting].[proc_PromptRevenueRegionClubForDepartmentYearMonth] @StartFourDigitYearDashTwoDigitMonth [VARCHAR](22),@EndFourDigitYearDashTwoDigitMonth [VARCHAR](22),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

	IF 1=0 BEGIN
		SET FMTONLY OFF
	END
--DECLARE @StartFourDigitYearDashTwoDigitMonth VARCHAR(22) = '2018-02'
--DECLARE @EndFourDigitYearDashTwoDigitMonth VARCHAR(22) = '2018-02'
--DECLARE  @DepartmentMinDimReportingHierarchyKeyList VARCHAR(8000) = 'All Departments'
--DECLARE @DivisionList VARCHAR(8000) = 'Personal Training'
--DECLARE @SubdivisionList VARCHAR(8000) =  'Nutrition, Metabolism & Weight Mgmt'

----- Sample Execution
--- Exec [reporting].[proc_PromptRevenueRegionClubForDepartmentYearMonth] '2019-02','2019-02','All Departments','Personal Training','Nutrition, Metabolism & Weight Mgmt'
-----


  
-- Set the @StartFourDigitYearDashTwoDigitMonth variable
SET @StartFourDigitYearDashTwoDigitMonth =(SELECT CASE WHEN @StartFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Current Quarter' THEN Quarters.QuarterStart
                                                   ELSE @StartFourDigitYearDashTwoDigitMonth END
	FROM [marketing].[v_dim_date] CurrentMonthDimDate
      JOIN [marketing].[v_dim_date] NextMonthDimDate
        ON CurrentMonthDimDate.Next_Month_Starting_Dim_Date_Key = NextMonthDimDate.Dim_Date_Key
      JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate
        ON NextMonthDimDate.Next_Month_Starting_Dim_Date_Key = MonthAfterNextMonthDimDate.Dim_Date_Key
      JOIN (SELECT Year, Quarter_Number, MIN(Four_Digit_Year_Dash_Two_Digit_Month) QuarterStart, MAX(Four_Digit_Year_Dash_Two_Digit_Month) QuarterEnd
             FROM [marketing].[v_dim_date]
             GROUP BY Year, Quarter_Number) Quarters
        ON CurrentMonthDimDate.Quarter_Number = Quarters.Quarter_Number
         AND CurrentMonthDimDate.Year = Quarters.Year
     WHERE CurrentMonthDimDate.Calendar_Date = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))


 -- Set the @EndFourDigitYearDashTwoDigitMonth variable
 SET @EndFourDigitYearDashTwoDigitMonth = (SELECT CASE WHEN @EndFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Current Quarter' THEN Quarters.QuarterEnd
                                                 ELSE @EndFourDigitYearDashTwoDigitMonth END
     FROM [marketing].[v_dim_date] CurrentMonthDimDate
      JOIN [marketing].[v_dim_date] NextMonthDimDate
        ON CurrentMonthDimDate.Next_Month_Starting_Dim_Date_Key = NextMonthDimDate.Dim_Date_Key
      JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate
        ON NextMonthDimDate.Next_Month_Starting_Dim_Date_Key = MonthAfterNextMonthDimDate.Dim_Date_Key
      JOIN (SELECT Year, Quarter_Number, MIN(Four_Digit_Year_Dash_Two_Digit_Month) QuarterStart, MAX(Four_Digit_Year_Dash_Two_Digit_Month) QuarterEnd
              FROM [marketing].[v_dim_date]
              GROUP BY Year, Quarter_Number) Quarters
        ON CurrentMonthDimDate.Quarter_Number = Quarters.Quarter_Number
        AND CurrentMonthDimDate.Year = Quarters.Year
     WHERE CurrentMonthDimDate.Calendar_Date = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))

DECLARE @StartMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
        @EndDateCalendarMonthEndingDate DATETIME

SELECT @StartMonthStartingDimDateKey = Month_Starting_Dim_Date_Key
  FROM [marketing].[v_dim_date]
 WHERE Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth
   AND Day_Number_In_Month = 1

SELECT @EndMonthEndingDimDateKey = Month_Ending_Dim_Date_Key,
       @EndDateCalendarMonthEndingDate = Month_Ending_Date
  FROM [marketing].[v_dim_date]
 WHERE Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth
   AND Last_Day_In_Month_Flag = 'Y'



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
  FROM [marketing].[v_dim_club] DimClub
    JOIN [marketing].[v_dim_description] PTRCLAreaDescription
	  ON DimClub.pt_rcl_area_dim_description_key = PTRCLAreaDescription.dim_description_key
    JOIN [marketing].[v_dim_description] MARegionDescription
	  ON DimClub.member_activities_region_dim_description_key = MARegionDescription.dim_description_key
    JOIN [marketing].[v_dim_description] MMSRegionDescription
	  ON DimClub.region_dim_description_key = MMSRegionDescription.dim_description_key
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
