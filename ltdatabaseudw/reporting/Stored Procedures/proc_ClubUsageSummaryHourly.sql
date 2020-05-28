CREATE PROC [reporting].[proc_ClubUsageSummaryHourly] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME],@ReportDimLocationKeyList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



 ------ JIRA : REP-5875
 ------ Execution Sample: EXEC [reporting].[proc_ClubUsageSummaryHourly] '2014/01/01','2014/10/01','151'

--set needed datetime variables
DECLARE @ReportRunDateTime Datetime
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ----- UDW in UTC time


DECLARE @BeginDimDateKey INT
DECLARE @EndDimDateKey INT
DECLARE @HeaderDateRange VARCHAR(33)

SELECT @BeginDimDateKey = ReportBeginDimDate.dim_date_key,
       @EndDimDateKey = ReportEndDimDate.dim_date_key,
       @HeaderDateRange = ReportBeginDimDate.standard_date_name + ' through ' + ReportEndDimDate.standard_date_name
  FROM [marketing].[v_dim_date] ReportBeginDimDate
 CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
 WHERE ReportBeginDimDate.calendar_date = @ReportBeginDate
   AND ReportEndDimDate.calendar_date = @ReportEndDate


  IF OBJECT_ID('tempdb.dbo.#NumberOfDays', 'U') IS NOT NULL
  DROP TABLE #NumberOfDays; 
  IF OBJECT_ID('tempdb.dbo.#Locations', 'U') IS NOT NULL
  DROP TABLE #Locations; 
  IF OBJECT_ID('tempdb.dbo.#ClubUsage', 'U') IS NOT NULL
  DROP TABLE #ClubUsage; 
  IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 

CREATE TABLE #NumberOfDays (DayNumberInCalendarWeek INT, NumberOfDays DECIMAL(10,1))
INSERT INTO #NumberOfDays 
SELECT vDimDate.day_number_in_week , COUNT(*) AS NumberOfDays
  FROM [marketing].[v_dim_date] vDimDate 
 WHERE vDimDate.dim_date_key between @BeginDimDateKey and @EndDimDateKey
 GROUP BY vDimDate.day_number_in_week


 
DECLARE @list_table VARCHAR(8000)
SET @list_table = 'Locations'

EXEC marketing.proc_parse_pipe_list @ReportDimLocationKeyList,@list_table




 CREATE TABLE #ClubUsage (
       RegionName Varchar(50),
       ClubName VARCHAR(50),
       HourGroup VARCHAR(13),
       DimLocationKey INT,
       RegionClubGroupHeader VARCHAR(100),
       SundayTotal DECIMAL(10,1),
       MondayTotal DECIMAL(10,1),
       TuesdayTotal DECIMAL(10,1),
       WednesdayTotal DECIMAL(10,1),
       ThursdayTotal DECIMAL(10,1),
       FridayTotal DECIMAL(10,1),
       SaturdayTotal DECIMAL(10,1),
       PeriodTotal DECIMAL(10,1))

	   INSERT INTO #ClubUsage (
       RegionName,
       ClubName,
       HourGroup,
       DimLocationKey,
       RegionClubGroupHeader,
       SundayTotal,
       MondayTotal,
       TuesdayTotal,
       WednesdayTotal,
       ThursdayTotal,
       FridayTotal,
       SaturdayTotal,
       PeriodTotal)
		SELECT 
			   MMSRegion.description AS RegionName,
			   DimLocation.club_name AS ClubName, 
			   DimTime.display_24_hour_group AS HourGroup,
			   DimLocation.club_id AS DimLocationKey,
			   MMSRegion.description + ' Region - ' + DimLocation.club_name AS RegionClubGroupHeader,
			   SUM(CASE DimDate.day_number_in_week WHEN 1 THEN 1 ELSE 0 END) AS SundayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 2 THEN 1 ELSE 0 END) AS MondayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 3 THEN 1 ELSE 0 END) AS TuesdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 4 THEN 1 ELSE 0 END) AS WednesdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 5 THEN 1 ELSE 0 END) AS ThursdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 6 THEN 1 ELSE 0 END) AS FridayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 7 THEN 1 ELSE 0 END) AS SaturdayTotal,
			   COUNT(*) as PeriodTotal
		  FROM marketing.v_fact_mms_member_usage FactMemberUsage
		  JOIN marketing.v_dim_club DimLocation
			ON FactMemberUsage.dim_club_key = DimLocation.dim_club_key
		  JOIN marketing.v_dim_description MMSRegion
		    ON DimLocation.region_dim_description_key  = MMSRegion.dim_description_key
		  JOIN marketing.v_dim_date DimDate
			ON Cast(FactMemberUsage.check_in_dim_date_time AS Date) = DimDate.calendar_date			 
		  JOIN marketing.v_dim_time DimTime
		    ON cast(DATEPART(hour, FactMemberUsage.check_in_dim_date_time) as varchar) = DimTime.hour and cast(DATEPART(minute, FactMemberUsage.check_in_dim_date_time) as varchar) = DimTime.minute
		  JOIN #Locations #Locations
			ON DimLocation.club_id = #Locations.item
		WHERE DimDate.dim_date_key BETWEEN @BeginDimDateKey AND @EndDimDateKey
		 GROUP BY
			   MMSRegion.description,
			   DimLocation.club_name,
			   DimTime.display_24_hour_group,
			   DimLocation.club_id,
			   MMSRegion.description + ' Region - ' + DimLocation.club_name
		UNION ALL
		SELECT MMSRegion.description AS RegionName,
			   DimLocation.club_name AS ClubName, 
			   DimTime.display_24_hour_group AS HourGroup,
			   DimLocation.club_id AS DimLocationKey,
			   MMSRegion.description + ' Region - ' + DimLocation.club_name AS RegionClubGroupHeader,
			   SUM(CASE DimDate.day_number_in_week WHEN 1 THEN 1 ELSE 0 END) AS SundayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 2 THEN 1 ELSE 0 END) AS MondayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 3 THEN 1 ELSE 0 END) AS TuesdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 4 THEN 1 ELSE 0 END) AS WednesdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 5 THEN 1 ELSE 0 END) AS ThursdayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 6 THEN 1 ELSE 0 END) AS FridayTotal,
			   SUM(CASE DimDate.day_number_in_week WHEN 7 THEN 1 ELSE 0 END) AS SaturdayTotal,
			   COUNT(*) as PeriodTotal
		  FROM marketing.v_fact_mms_guest_club_usage FactGuestClubUsage
		  JOIN marketing.v_dim_club DimLocation
			ON FactGuestClubUsage.dim_club_key = DimLocation.dim_club_key
		  JOIN marketing.v_dim_description MMSRegion
		    ON DimLocation.region_dim_description_key  = MMSRegion.dim_description_key
		  JOIN marketing.v_dim_date DimDate
			ON FactGuestClubUsage.check_in_dim_date_key = DimDate.dim_date_key	
		  JOIN marketing.v_dim_time DimTime
			ON FactGuestClubUsage.check_in_dim_time_key = DimTime.dim_time_key
		  JOIN #Locations #Locations
			ON DimLocation.club_id = #Locations.item
		WHERE DimDate.dim_date_key BETWEEN @BeginDimDateKey AND @EndDimDateKey
		 GROUP BY 
			   MMSRegion.description,
			   DimLocation.club_name,
			   DimTime.display_24_hour_group,
			   DimLocation.club_id,
			   MMSRegion.description + ' Region - ' + DimLocation.club_name


	   SELECT RegionName,
		   ClubName,
		   HourGroup,
		   DimLocationKey,
		   RegionClubGroupHeader,
		   CAST(SUM(SundayTotal) AS INT) AS SundayTotal, 
		   CAST(SUM(SundayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 1) AS DECIMAL(10,1)) AS SundayAverage,
		   CAST(SUM(MondayTotal) AS INT) AS MondayTotal, 
		   CAST(SUM(MondayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 2) AS DECIMAL(10,1)) AS MondayAverage,
		   CAST(SUM(TuesdayTotal) AS INT) AS TuesdayTotal, 
		   CAST(SUM(TuesdayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 3) AS DECIMAL(10,1)) AS TuesdayAverage,
		   CAST(SUM(WednesdayTotal) AS INT) AS WednesdayTotal, 
		   CAST(SUM(WednesdayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 4) AS DECIMAL(10,1)) AS WednesdayAverage,
		   CAST(SUM(ThursdayTotal) AS INT) AS ThursdayTotal, 
		   CAST(SUM(ThursdayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 5) AS DECIMAL(10,1)) AS ThursdayAverage,
		   CAST(SUM(FridayTotal) AS INT) AS FridayTotal, 
		   CAST(SUM(FridayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 6) AS DECIMAL(10,1)) AS FridayAverage,
		   CAST(SUM(SaturdayTotal) AS INT) AS SaturdayTotal, 
		   CAST(SUM(SaturdayTotal) / (select NumberOfDays from #NumberOfDays where DayNumberInCalendarWeek = 7) AS DECIMAL(10,1)) AS SaturdayAverage,
		   CAST(SUM(PeriodTotal) AS INT) AS PeriodTotal
		INTO #Results
		FROM #ClubUsage
		GROUP BY 
		   RegionName,
		   ClubName,
		   HourGroup,
		   DimLocationKey,
		   RegionClubGroupHeader

SELECT RegionName,
       ClubName,
       HourGroup,
       DimLocationKey,
       RegionClubGroupHeader,
       @BeginDimDateKey ReportBeginDimDateKey,
       @EndDimDateKey ReportEndDimDateKey,
       SundayTotal, 
       SundayAverage,
       MondayTotal, 
       MondayAverage,
       TuesdayTotal, 
       TuesdayAverage,
       WednesdayTotal, 
       WednesdayAverage,
       ThursdayTotal, 
       ThursdayAverage,
       FridayTotal, 
       FridayAverage,
       SaturdayTotal, 
       SaturdayAverage,
       PeriodTotal,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       CAST(NULL AS VARCHAR(70)) HeaderEmptyResultSet
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) RegionName,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(13)) HourGroup,
       CAST(NULL AS INT) DimLocationKey,
       CAST(NULL AS VARCHAR(100)) RegionClubGroupHeader,
       @BeginDimDateKey ReportBeginDimDateKey,
       @EndDimDateKey ReportEndDimDateKey,
       CAST(NULL AS INT) SundayTotal, 
       CAST(NULL AS DECIMAL(10,1)) SundayAverage,
       CAST(NULL AS INT) MondayTotal, 
       CAST(NULL AS DECIMAL(10,1)) MondayAverage,
       CAST(NULL AS INT) TuesdayTotal, 
       CAST(NULL AS DECIMAL(10,1)) TuesdayAverage,
       CAST(NULL AS INT) WednesdayTotal, 
       CAST(NULL AS DECIMAL(10,1)) WednesdayAverage,
       CAST(NULL AS INT) ThursdayTotal, 
       CAST(NULL AS DECIMAL(10,1)) ThursdayAverage,
       CAST(NULL AS INT) FridayTotal, 
       CAST(NULL AS DECIMAL(10,1)) FridayAverage,
       CAST(NULL AS INT) SaturdayTotal, 
       CAST(NULL AS DECIMAL(10,1)) SaturdayAverage,
       CAST(NULL AS INT) PeriodTotal,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResultSet
 WHERE (SELECT COUNT(*) FROM #Results) = 0
 ORDER BY RegionName,
          ClubName,
          HourGroup

  DROP TABLE #NumberOfDays
  DROP TABLE #Locations
  DROP TABLE #ClubUsage
  DROP TABLE #Results

END
