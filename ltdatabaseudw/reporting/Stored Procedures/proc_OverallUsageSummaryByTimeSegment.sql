CREATE PROC [reporting].[proc_OverallUsageSummaryByTimeSegment] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME],@paramMMSClubIDList [VARCHAR](1000),@paramTimeSegment [VARCHAR](20) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

--- This SP returns usage for selected clubs and time frame from v_fact_mms_member_usage, v_fact_mms_guest_club_usage & v_fact_mms_child_center_usage
--- Execution Sample:  Exec [reporting].[proc_OverallUsageSummaryByTimeSegment]  '6/1/2018', '6/9/2018', '2|151', '1/2 Hour'
DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = CAST(DATEADD(HH,-5,GETDATE()) AS nvarchar(30)) --UDW on UTC, subtracting 5 hours and formatting for report output

DECLARE @ReportStartDateKey INT,
        @ReportEndDateKey INT,
        @HeaderDateRange VARCHAR(50)
SELECT @ReportStartDateKey = ReportBeginDimDate.dim_date_key,
       @ReportEndDateKey = ReportEndDimDate.dim_date_key,
       @HeaderDateRange = ReportBeginDimDate.full_date_description + ' through ' + ReportEndDimDate.full_date_description
  FROM [marketing].[v_dim_date] ReportBeginDimDate
  CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
 WHERE ReportBeginDimDate.calendar_date = @ReportBeginDate
   AND ReportEndDimDate.calendar_date = @ReportEndDate

IF OBJECT_ID('tempdb.dbo.#ReportingDates', 'U') IS NOT NULL DROP TABLE #ReportingDates; 
IF OBJECT_ID('tempdb.dbo.#ClubKeys', 'U') IS NOT NULL DROP TABLE #ClubKeys; 
IF OBJECT_ID('tempdb.dbo.#club_list', 'U') IS NOT NULL DROP TABLE #club_list; 
IF OBJECT_ID('tempdb.dbo.#Usage', 'U') IS NOT NULL DROP TABLE #Usage; 
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL DROP TABLE #Results; 

CREATE TABLE #ReportingDates (DimDateKey INT)
INSERT INTO #ReportingDates SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key BETWEEN @ReportStartDateKey AND @ReportEndDateKey

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @paramMMSClubIDList,@list_table

SELECT DimClub.dim_club_key 
 INTO #ClubKeys
 FROM #club_list MMSClubIDList
 JOIN [marketing].[v_dim_club] DimClub
 ON MMSClubIDList.Item = DimClub.club_id

CREATE TABLE #Usage (
       MMSRegionName Varchar(100),
       SalesAreaName Varchar(100),
       ClubCode Varchar(30),
       ClubName Varchar(100),
       MemberUsageCount Numeric(12,0),
       GuestVisitCount Numeric(12,0),
       ChildCenterUsageCount Numeric(12,0),
       UsageTimeSegment Varchar(30),
       UsageDate Varchar(20),
       UsageDateSort INT, 
       UsageDay Varchar(20),
       UsageMonth Varchar(20),
       UsageYear Numeric(4,0),
       Hour Integer,
       HourSegment Integer)

INSERT INTO #Usage
-- MEMBER Usage Fact: [marketing].[v_fact_mms_member_usage] 
SELECT dim_description_region.description MMSRegionName,  
       dim_description_salesarea.description SalesAreaName,
       DimLocation.club_code,
       DimLocation.club_name,
       Count(FactMemberUsage.fact_mms_member_usage_key) as MemberUsageCount,
       NULL as GuestVisitCount,
       NULL as ChildCenterUsageCount,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group
       END UsageTimeSegment,
	   DimDate.full_date_description as UsageDate,
       DimDate.dim_date_key as UsageDateSort,
       DimDate.day_of_week_name as UsageDay,
       DimDate.month_name as UsageMonth,
       DimDate.year as UsageYear,
       DimTime.hour as Hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter
       END HourSegment
FROM [marketing].[v_fact_mms_member_usage] FactMemberUsage
  JOIN #ClubKeys #ClubKeys 
    ON #ClubKeys.dim_club_key = FactMemberUsage.dim_club_key
  JOIN [marketing].[v_dim_club] DimLocation 
    ON #ClubKeys.dim_club_key  = DimLocation.dim_club_key 
  JOIN [marketing].[v_dim_date] DimDate 
	ON CAST(FactMemberUsage.[check_in_dim_date_time] as Date) = CAST(DimDate.[calendar_date]  as Date) 
  JOIN #ReportingDates Dates
    ON Dates.DimDateKey = DimDate.dim_date_key
  JOIN [marketing].[v_dim_description] dim_description_region
    ON dim_description_region.dim_description_key = DimLocation.region_dim_description_key
  JOIN [marketing].[v_dim_description] dim_description_salesarea
    ON dim_description_salesarea.dim_description_key = DimLocation.sales_area_dim_description_key
  JOIN [marketing].[v_dim_time] DimTime 
    ON DimTime.display_24_hour_time = (CASE WHEN DATEPART(HH,FactMemberUsage.[check_in_dim_date_time]) < 10   -- parse and format HH:MM from check_in_dim_date_time
									  THEN '0'+CAST(DATEPART(HH,FactMemberUsage.[check_in_dim_date_time]) AS CHAR(1))
									  ELSE CAST(DATEPART(HH,FactMemberUsage.[check_in_dim_date_time]) AS CHAR(2)) END 
									  +':'+
									  CASE WHEN DATEPART(MI,FactMemberUsage.[check_in_dim_date_time]) < 10 
									  THEN '0'+CAST(DATEPART(MI,FactMemberUsage.[check_in_dim_date_time]) AS CHAR(1))
									  ELSE CAST(DATEPART(MI,FactMemberUsage.[check_in_dim_date_time]) AS CHAR(2)) END)
  GROUP BY dim_description_region.description,
       dim_description_salesarea.description,
       DimLocation.club_code,
       DimLocation.club_name,
	   CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group END,
       DimDate.full_date_description,
       DimDate.dim_date_key,
       DimDate.day_of_week_name,
       DimDate.month_name,
       DimDate.year,
       DimTime.hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter END

UNION

-- GUEST MEMBER Usage Fact: [marketing].[v_fact_mms_guest_club_usage] 
       SELECT dim_description_region.description MMSRegionName,
       dim_description_salesarea.description SalesAreaName,
       DimLocation.club_code,
       DimLocation.club_name,
       NULL as MemberUsageCount,
       Count(GuestMemberUsage.fact_mms_guest_club_usage_key) as GuestVisitCount,
       NULL as ChildCenterUsageCount,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group
       END UsageTimeSegment,
	   DimDate.full_date_description as UsageDate,
       DimDate.dim_date_key as UsageDateSort,
       DimDate.day_of_week_name as UsageDay,
       DimDate.month_name as UsageMonth,
       DimDate.year as UsageYear,
       DimTime.hour as Hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter
       END HourSegment
FROM [marketing].[v_fact_mms_guest_club_usage] GuestMemberUsage
  JOIN #ClubKeys #ClubKeys 
    ON #ClubKeys.dim_club_key = GuestMemberUsage.dim_club_key
  JOIN [marketing].[v_dim_club] DimLocation 
    ON #ClubKeys.dim_club_key  = DimLocation.dim_club_key 
  JOIN [marketing].[v_dim_date] DimDate 
	ON GuestMemberUsage.check_in_dim_date_key = DimDate.dim_date_key
  JOIN #ReportingDates Dates
    ON Dates.DimDateKey = DimDate.dim_date_key
  JOIN [marketing].[v_dim_description] dim_description_region
    ON dim_description_region.dim_description_key = DimLocation.region_dim_description_key
  JOIN [marketing].[v_dim_description] dim_description_salesarea
    ON dim_description_salesarea.dim_description_key = DimLocation.sales_area_dim_description_key
  JOIN [marketing].[v_dim_time] DimTime 
    ON DimTime.dim_time_key = GuestMemberUsage.check_in_dim_time_key
  GROUP BY dim_description_region.description,
       dim_description_salesarea.description,
       DimLocation.club_code,
       DimLocation.club_name,
	   CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group END,
       DimDate.full_date_description,
       DimDate.dim_date_key,
       DimDate.day_of_week_name,
       DimDate.month_name,
       DimDate.year,
       DimTime.hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter END

UNION

-- CHILD CENTER Usage Fact: [marketing].[v_fact_mms_child_center_usage]
       SELECT dim_description_region.description MMSRegionName,
       dim_description_salesarea.description SalesAreaName,
       DimLocation.club_code,
       DimLocation.club_name,
       NULL as MemberUsageCount,
       NULL as GuestVisitCount,
       Count(ChildCenterUsage.fact_mms_child_center_usage_key) as ChildCenterUsageCount,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group
       END UsageTimeSegment,
	   DimDate.full_date_description as UsageDate,
       DimDate.dim_date_key as UsageDateSort,
       DimDate.day_of_week_name as UsageDay,
       DimDate.month_name as UsageMonth,
       DimDate.year as UsageYear,
       DimTime.hour as Hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter
       END HourSegment
FROM [marketing].[v_fact_mms_child_center_usage] ChildCenterUsage
  JOIN #ClubKeys #ClubKeys 
    ON #ClubKeys.dim_club_key = ChildCenterUsage.dim_club_key
  JOIN [marketing].[v_dim_club] DimLocation 
    ON #ClubKeys.dim_club_key  = DimLocation.dim_club_key 
  JOIN [marketing].[v_dim_date] DimDate 
	ON ChildCenterUsage.check_in_dim_date_key = DimDate.dim_date_key
  JOIN #ReportingDates Dates
    ON Dates.DimDateKey = DimDate.dim_date_key
  JOIN [marketing].[v_dim_description] dim_description_region
    ON dim_description_region.dim_description_key = DimLocation.region_dim_description_key
  JOIN [marketing].[v_dim_description] dim_description_salesarea
    ON dim_description_salesarea.dim_description_key = DimLocation.sales_area_dim_description_key
  JOIN [marketing].[v_dim_time] DimTime 
    ON DimTime.dim_time_key = ChildCenterUsage.check_in_dim_time_key
  GROUP BY dim_description_region.description,
       dim_description_salesarea.description,
       DimLocation.club_code,
       DimLocation.club_name,
	   CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.display_12_hour_group
                              WHEN '1/2 Hour' THEN DimTime.display_12_hour_half_group
                              WHEN '1/4 Hour' THEN DimTime.display_12_hour_quarter_group END,
       DimDate.full_date_description,
       DimDate.dim_date_key,
       DimDate.day_of_week_name,
       DimDate.month_name,
       DimDate.year,
       DimTime.hour,
       CASE @paramTimeSegment WHEN '1 Hour' THEN DimTime.hour
                              WHEN '1/2 Hour' THEN DimTime.half_hour
                              WHEN '1/4 Hour' THEN DimTime.hour_quarter END

-- FINAL Result Set:
SELECT MMSRegionName as RegionName, 
       SalesAreaName, 
       ClubCode, 
       ClubName, 
       sum(MemberUsageCount) as MemberUsageCount, 
       sum(GuestVisitCount) as GuestVisitCount, 
       sum(ChildCenterUsageCount) as ChildCenterUsageCount,
       UsageTimeSegment, 
       UsageDate, 
       UsageDateSort,
       UsageDay, 
       UsageMonth, 
       UsageYear, 
       Hour, 
       HourSegment
INTO #Results
FROM #Usage
GROUP BY MMSRegionName, 
       SalesAreaName, 
       ClubCode, 
       ClubName, 
       UsageTimeSegment, 
       UsageDate, 
       UsageDateSort,
       UsageDay, 
       UsageMonth, 
       UsageYear, 
       Hour, 
       HourSegment

SELECT CAST(RegionName AS VARCHAR(100)) RegionName, 
       CAST(SalesAreaName AS VARCHAR(100)) SalesAreaName, 
       CAST(ClubCode AS VARCHAR(30)) ClubCode, 
       CAST(ClubName AS VARCHAR(100)) ClubName, 
       CAST(MemberUsageCount AS INT) MemberUsageCount, 
       CAST(GuestVisitCount AS INT) GuestVisitCount, 
       CAST(ChildCenterUsageCount AS INT) ChildCenterUsageCount,
       CAST(UsageTimeSegment AS VARCHAR(30)) UsageTimeSegment, 
       CAST(UsageDate AS VARCHAR(20)) UsageDate, 
       CAST(UsageDateSort AS INT) UsageDateSort,
       CAST(UsageDay AS VARCHAR(20)) UsageDay, 
       CAST(UsageMonth AS VARCHAR(20)) UsageMonth, 
       CAST(UsageYear AS INT) UsageYear, 
       CAST(Hour AS INT) Hour, 
       CAST(HourSegment AS INT) HourSegment,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       CAST(NULL AS VARCHAR(70)) HeaderEmptyResultSet
  FROM #Results
  WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(100)) RegionName, 
       CAST(NULL AS VARCHAR(100)) SalesAreaName, 
       CAST(NULL AS VARCHAR(30)) ClubCode, 
       CAST(NULL AS VARCHAR(100)) ClubName, 
       CAST(NULL AS INT) MemberUsageCount, 
       CAST(NULL AS INT) GuestVisitCount, 
       CAST(NULL AS INT) ChildCenterUsageCount,
       CAST(NULL AS VARCHAR(30)) UsageTimeSegment, 
       CAST(NULL AS VARCHAR(20)) UsageDate, 
       CAST(NULL AS INT) UsageDateSort,
       CAST(NULL AS VARCHAR(20)) UsageDay, 
       CAST(NULL AS VARCHAR(20)) UsageMonth, 
       CAST(NULL AS INT) UsageYear, 
       CAST(NULL AS INT) Hour, 
       CAST(NULL AS INT) HourSegment,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResultSet
 WHERE (SELECT COUNT(*) FROM #Results) = 0
 ORDER BY RegionName, 
          ClubName, 
          UsageDateSort, 
          Hour, 
          HourSegment

DROP TABLE #ReportingDates
DROP TABLE #ClubKeys
DROP TABLE #club_list
DROP TABLE #Usage
DROP TABLE #Results
   
END

