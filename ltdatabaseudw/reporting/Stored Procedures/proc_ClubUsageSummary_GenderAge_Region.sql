CREATE PROC [reporting].[proc_ClubUsageSummary_GenderAge_Region] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME],@ReportMMSRegionNameList [VARCHAR](1000) AS
BEGIN

   SET XACT_ABORT ON
   SET NOCOUNT ON

   IF 1=0 BEGIN
   SET FMTONLY OFF
   END

--DECLARE @ReportBeginDate  DATETIME = '3/1/2018'
--DECLARE @ReportEndDate DATETIME = '3/2/2018'
--DECLARE @ReportMMSRegionNameList VARCHAR(1000) = 'Hall-MN-East'
----------------------------------------------------------
DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = CAST(DATEADD(HH,-5,GETDATE()) AS nvarchar(30))

DECLARE @list_table VARCHAR(1000)
SET @list_table = 'region_list'

EXEC marketing.proc_parse_pipe_list @ReportMMSRegionNameList, @list_table

IF OBJECT_ID('tempdb.dbo.#Regions', 'U') IS NOT NULL
  DROP TABLE #Regions; 

Select Distinct Item MMSRegionName
INTO #Regions
FROM #region_list

IF OBJECT_ID('tempdb.dbo.#Dates', 'U') IS NOT NULL
  DROP TABLE #Dates; 
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 

CREATE TABLE #Dates (
		DimDateKey char(32))
INSERT INTO #Dates SELECT dim_date_key From marketing.v_dim_date WHERE calendar_date BETWEEN
       CONVERT(DATETIME, CONVERT(VARCHAR(10), @ReportBeginDate, 101) , 101)
       AND CONVERT(DATETIME, CONVERT(VARCHAR(10), @ReportEndDate, 101) , 101)
	   
DECLARE @BeginDimDateKey INT,
        @EndDimDateKey INT,
        @HeaderDateRange VARCHAR(33)
SELECT @BeginDimDateKey = ReportBeginDimDate.Dim_Date_Key,
       @EndDimDateKey = ReportEndDimDate.Dim_Date_Key,
       @HeaderDateRange = ReportBeginDimDate.standard_date_name + ' through ' + ReportEndDimDate.standard_date_name
  FROM marketing.v_Dim_Date ReportBeginDimDate
  CROSS JOIN marketing.v_Dim_Date ReportEndDimDate
 WHERE ReportBeginDimDate.Calendar_Date = @ReportBeginDate
   AND ReportEndDimDate.Calendar_Date = @ReportEndDate
  
SELECT CAST(region.description AS VARCHAR(50)) as RegionName,
       CAST(CASE 
            WHEN DimTime.member_usage_targeted_segment_period = '05:00 - 09:00'
                 THEN '5:00 AM - 9:00 AM'
            WHEN DimTime.member_usage_targeted_segment_period = '11:00 - 13:00'
                 THEN '11:00 AM - 1:00 PM'
            WHEN DimTime.member_usage_targeted_segment_period = '16:00 - 20:00'
                 THEN '4:00 PM - 8:00 PM'
                 ELSE 'Other'
       END AS VARCHAR(20)) AS Display12HourTargetPeriod,
       CAST(DimTime.member_usage_targeted_segment_period AS VARCHAR(18)) AS MemberUsageTargetedSegmentPeriod,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'M' 
                AND FactMemberUsage.Member_Age_Years < 19
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS MaleUnder19,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'M' 
                AND (FactMemberUsage.Member_Age_Years >= 19 AND FactMemberUsage.Member_Age_Years < 26)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Male19_25,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'M' 
                AND (FactMemberUsage.Member_Age_Years >= 26 AND FactMemberUsage.Member_Age_Years < 41)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Male26_40,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'M' 
                AND (FactMemberUsage.Member_Age_Years >= 41 AND FactMemberUsage.Member_Age_Years < 56)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Male41_55,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'M' 
                AND ISNULL(FactMemberUsage.Member_Age_Years,118) >= 56 
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Male56AndOlder,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'F' 
                AND FactMemberUsage.Member_Age_Years < 19
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS FemaleUnder19,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'F' 
                AND (FactMemberUsage.Member_Age_Years >= 19 AND FactMemberUsage.Member_Age_Years < 26)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Female19_25,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'F' 
                AND (FactMemberUsage.Member_Age_Years >= 26 AND FactMemberUsage.Member_Age_Years < 41)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Female26_40,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'F' 
                AND (FactMemberUsage.Member_Age_Years >= 41 AND FactMemberUsage.Member_Age_Years < 56)
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Female41_55,
       CAST(SUM(CASE 
                WHEN FactMemberUsage.Gender_Abbreviation = 'F' 
                AND ISNULL(FactMemberUsage.Member_Age_Years,118) >= 56 
                     THEN 1 
                     ELSE 0 
       END) AS INT) AS Female56AndOlder
  INTO #Results
  FROM marketing.v_fact_mms_member_usage FactMemberUsage
  JOIN marketing.v_dim_club DimLocation
    ON FactMemberUsage.dim_club_key = DimLocation.dim_club_key
  JOIN marketing.v_Dim_Date DimDate
    ON convert(date,FactMemberUsage.check_in_dim_date_time)= DimDate.calendar_date and DimDate.calendar_date BETWEEN @ReportBeginDate AND @ReportEndDate
  JOIN marketing.v_Dim_Time DimTime
    ON CONVERT(VARCHAR(5), FactMemberUsage.Check_in_dim_date_time, 108) = DimTime.display_24_hour_time
  JOIN #Dates #Dates
    ON DimDate.Dim_Date_Key = #Dates.DimDateKey
  JOIN marketing.v_dim_description region
    ON DimLocation.region_dim_description_key = region.dim_description_key
  JOIN #Regions #Regions
    ON region.description = #Regions.MMSRegionName

 GROUP BY region.description,
       DimTime.member_usage_targeted_segment_period

SELECT RegionName,
       Display12HourTargetPeriod,
       @BeginDimDateKey ReportBeginDimDateKey,
       @EndDimDateKey ReportEndDimDateKey,
       @ReportBeginDate ReportBeginDate,
       @ReportEndDate ReportEndDate,
       MemberUsageTargetedSegmentPeriod,
       MaleUnder19,
       Male19_25,
       Male26_40,
       Male41_55,
       Male56AndOlder,
       FemaleUnder19,
       Female19_25,
       Female26_40,
       Female41_55,
       Female56AndOlder,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       CAST(NULL AS VARCHAR(70)) HeaderEmptyResultSet
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) RegionName,
       CAST(NULL AS VARCHAR(20)) Display12HourTargetPeriod,
       @BeginDimDateKey ReportBeginDimDateKey,
       @EndDimDateKey ReportEndDimDateKey,
       @ReportBeginDate ReportBeginDate,
       @ReportEndDate ReportEndDate,
       CAST(NULL AS VARCHAR(18)) MemberUsageTargetedSegmentPeriod,
       CAST(NULL AS INT) MaleUnder19,
       CAST(NULL AS INT) Male19_25,
       CAST(NULL AS INT) Male26_40,
       CAST(NULL AS INT) Male41_55,
       CAST(NULL AS INT) Male56AndOlder,
       CAST(NULL AS INT) FemaleUnder19,
       CAST(NULL AS INT) Female19_25,
       CAST(NULL AS INT) Female26_40,
       CAST(NULL AS INT) Female41_55,
       CAST(NULL AS INT) Female56AndOlder,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResultSet
 WHERE (SELECT COUNT(*) FROM #Results) = 0
 ORDER BY RegionName,
          MemberUsageTargetedSegmentPeriod

DROP TABLE #Regions
DROP TABLE #Dates

END


