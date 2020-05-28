CREATE PROC [reporting].[proc_DelinquentUsageSummaryByClub] @ReportBeginDate [DateTime],@ReportEndDate [DateTime],@ReportDimLocationKeyList [varchar](4000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
------ JIRA : REP-5880
------ Sample Execution
------ Exec [reporting].[proc_DelinquentUsageSummaryByClub] '05/01/2017','06/01/2017','146'
------

DECLARE @ReportRunDateTime VARCHAR(21)

SELECT @ReportRunDateTime = getdate()

DECLARE @ReportStartDimDateKey INT,
        @ReportEndDimDateKey INT,
        @HeaderDateRange VARCHAR(50)

SET @ReportStartDimDateKey = (SELECT dim_date_key FROM [marketing].v_dim_date WHERE calendar_date = @ReportBeginDate)
SET @ReportEndDimDateKey = (SELECT dim_date_key FROM [marketing].v_dim_date WHERE calendar_date = @ReportEndDate)

SELECT @HeaderDateRange = ReportStartDimDate.full_date_description + ' through ' + ReportEndDimDate.full_date_description
  FROM [marketing].v_dim_date ReportStartDimDate
  JOIN [marketing].v_dim_date ReportEndDimDate ON ReportEndDimDate.dim_date_key = @ReportEndDimDateKey
 WHERE ReportStartDimDate.dim_date_key = @ReportStartDimDateKey

DECLARE @list_table VARCHAR(500)
SET @list_table = 'clublist'

EXEC marketing.proc_parse_pipe_list @ReportDimLocationKeyList,@list_table


IF OBJECT_ID('tempdb.dbo.#FactMemberUsage', 'U') IS NOT NULL
  DROP TABLE #FactMemberUsage; 
IF OBJECT_ID('tempdb.dbo.#DelinquentCheckInCount', 'U') IS NOT NULL
  DROP TABLE #DelinquentCheckInCount; 
IF OBJECT_ID('tempdb.dbo.#MembersCheckInMoreThanOnce', 'U') IS NOT NULL
  DROP TABLE #MembersCheckInMoreThanOnce;
IF OBJECT_ID('tempdb.dbo.#MultipleCheckInPerDayByClubCount', 'U') IS NOT NULL
  DROP TABLE #MultipleCheckInPerDayByClubCount;

SELECT FactMemberUsage.dim_club_key as DimLocationKey,
       FactMemberUsage.dim_mms_checkin_member_key as CheckInDimCustomerKey,
       dt.dim_date_key as CheckInDimDateKey,
       FactMemberUsage.delinquent_checkin_flag as DelinquentCheckInFlag
  INTO #FactMemberUsage
  FROM marketing.v_fact_mms_member_usage FactMemberUsage
  JOIN marketing.v_dim_club club on FactMemberUsage.dim_club_key = club.dim_club_key
  JOIN #clublist ON club.club_id = #clublist.Item
  JOIN marketing.v_dim_date dt on dt.calendar_date =  CONVERT(VARCHAR, FactMemberUsage.check_in_dim_date_time,101)
 WHERE dt.dim_date_key >= @ReportStartDimDateKey
   AND dt.dim_date_key <= @ReportEndDimDateKey

SELECT #FactMemberUsage.DimLocationKey,
       COUNT(*) DelinquentCheckInCountByClub
  INTO #DelinquentCheckInCount
  FROM #FactMemberUsage
  JOIN marketing.v_dim_mms_member CheckInDimCustomer ON #FactMemberUsage.CheckInDimCustomerKey = CheckInDimCustomer.dim_mms_member_key
  JOIN marketing.v_dim_description mem_type on CheckInDimCustomer.member_type_dim_description_key = mem_type.dim_description_key
 WHERE #FactMemberUsage.DelinquentCheckInFlag = 'Y'
   AND mem_type.description in ('Primary','Partner')
 GROUP BY #FactMemberUsage.DimLocationKey

 SELECT DimLocationKey,
       CheckInDimCustomerKey,
       CheckInDimDateKey,
       COUNT(*) SameDayCheckInCount
  INTO #MembersCheckInMoreThanOnce
  FROM #FactMemberUsage
 GROUP BY DimLocationKey,
          CheckInDimCustomerKey,
          CheckInDimDateKey
HAVING COUNT(*) > 1

SELECT DimLocationKey,
       COUNT(Distinct CheckInDimCustomerKey) MembersWithMultipleDailyCheckIns
  INTO #MultipleCheckInPerDayByClubCount
  FROM #MembersCheckInMoreThanOnce
 GROUP BY DimLocationKey

SELECT MMSRegion.description as MMSRegionName,
       DimLocation.club_name as ClubName,
       DimLocation.club_id as DimLocationKey,
       #DelinquentCheckInCount.DelinquentCheckInCountByClub,
       #MultipleCheckInPerDayByClubCount.MembersWithMultipleDailyCheckIns,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime
  FROM #DelinquentCheckInCount
  FULL OUTER JOIN #MultipleCheckInPerDayByClubCount
    ON #DelinquentCheckInCount.DimLocationKey = #MultipleCheckInPerDayByClubCount.DimLocationKey
  JOIN marketing.v_dim_club DimLocation
    ON ISNULL(#DelinquentCheckInCount.DimLocationKey, #MultipleCheckInPerDayByClubCount.DimLocationKey) = DimLocation.dim_club_key
  JOIN marketing.v_dim_description MMSRegion on DimLocation.region_dim_description_key  = MMSRegion.dim_description_key
 ORDER BY MMSRegion.description, DimLocation.club_name

 DROP TABLE #FactMemberUsage

END
