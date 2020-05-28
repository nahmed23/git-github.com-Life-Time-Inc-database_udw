CREATE PROC [reporting].[proc_ClubUsageSummary_UniqueMembershipCount] @ReportBeginDate [DateTime],@ReportEndDate [DateTime],@ReportMMSClubIDList [varchar](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ 
------ Sample Execution
------ Exec [reporting].[proc_ClubUsageSummary_UniqueMembershipCount] '01/01/2018','01/31/2018','151'
------


DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ----- UDW in UTC time


DECLARE @HeaderDateRange VARCHAR(120)
SELECT @HeaderDateRange = ReportBeginDimDate.full_date_description + ' through ' + ReportEndDimDate.full_date_description
  FROM marketing.v_dim_date ReportBeginDimDate
 CROSS JOIN marketing.v_dim_date ReportEndDimDate
 WHERE ReportBeginDimDate.calendar_date = @ReportBeginDate
   AND ReportEndDimDate.calendar_date = @ReportEndDate


---- Create Region count temp table
IF OBJECT_ID('tempdb.dbo.#RegionCount', 'U') IS NOT NULL
  DROP TABLE #RegionCount; 

DECLARE @list_table VARCHAR(500)
SET @list_table = 'clublist'

EXEC marketing.proc_parse_pipe_list @ReportMMSCLubIDList,@list_table

CREATE TABLE #RegionCount (
       MMSRegionName VARCHAR(50),
       UniqueMembershipCount_Region INT)

INSERT INTO #RegionCount
SELECT DimMMSRegion.description MMSRegionName,
       COUNT(DISTINCT(FactMemberUsage.dim_mms_membership_key))
  FROM marketing.v_fact_mms_member_usage FactMemberUsage
  JOIN marketing.v_dim_club DimLocationSUBQUERY
	ON FactMemberUsage.dim_club_key = DimLocationSUBQUERY.dim_club_key       
  JOIN marketing.v_dim_description DimMMSRegion
	ON DimLocationSUBQUERY.region_dim_description_key = DimMMSRegion.dim_description_key 
  JOIN marketing.v_dim_date dt
    ON convert(date,FactMemberUsage.checkin_date_time)= dt.calendar_date 
	  AND dt.calendar_date BETWEEN @ReportBeginDate AND @ReportEndDate
  JOIN #clublist loc
    ON DimLocationSUBQUERY.club_id = loc.Item
	
 GROUP BY DimMMSRegion.description


 ---- Create Results temp table
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 

 SELECT CAST(DimMMSRegion.description AS VARCHAR(50)) AS RegionName,
       CAST(DimLocation.club_name AS VARCHAR(50)) AS ClubName,
       CAST(DimLocation.club_code AS VARCHAR(18)) AS ClubCode,
       CAST(COUNT(FactMemberUsage.fact_mms_member_usage_key) AS INT) AS TotalCheckInCount,
       CAST(COUNT(DISTINCT(FactMemberUsage.dim_mms_membership_key)) AS INT) AS UniqueMembershipCount_Club,
       CAST(rc.UniqueMembershipCount_Region AS INT) AS UniqueMembershipCount_Region
  INTO #Results
  FROM marketing.v_fact_mms_member_usage FactMemberUsage
  JOIN marketing.v_dim_club DimLocation
	ON FactMemberUsage.dim_club_key = DimLocation.dim_club_key
  JOIN marketing.v_dim_description DimMMSRegion
	ON DimLocation.region_dim_description_key = DimMMSRegion.dim_description_key
  JOIN marketing.v_dim_date dt
    ON convert(date,FactMemberUsage.checkin_date_time)= dt.calendar_date 
	  AND (dt.calendar_date BETWEEN @ReportBeginDate AND @ReportEndDate)
  JOIN #clublist loc
    ON DimLocation.club_id = loc.Item 
  JOIN #RegionCount rc
    ON DimMMSRegion.description = rc.MMSRegionName
  JOIN marketing.v_dim_date ReportBeginDimDate
    ON @ReportBeginDate = ReportBeginDimDate.calendar_date
  JOIN marketing.v_dim_date ReportEndDimDate
    ON @ReportEndDate = ReportEndDimDate.calendar_date
 GROUP BY DimMMSRegion.description,
       DimLocation.club_name,
       DimLocation.club_code,
       rc.UniqueMembershipCount_Region

SELECT RegionName,
       ClubName,
       ClubCode,
       TotalCheckInCount,
       UniqueMembershipCount_Club,
       UniqueMembershipCount_Region,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       CAST(NULL AS VARCHAR(70)) HeaderEmptyResultSet
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) RegionName,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(18)) ClubCode,
       CAST(NULL AS INT) TotalCheckInCount,
       CAST(NULL AS INT) UniqueMembershipCount_Club,
       CAST(NULL AS INT) UniqueMembershipCount_Region,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResultSet
 WHERE (SELECT COUNT(*) FROM #Results) = 0
 ORDER BY RegionName,
          ClubName,
          ClubCode


DROP TABLE #RegionCount
DROP TABLE #Results

END
