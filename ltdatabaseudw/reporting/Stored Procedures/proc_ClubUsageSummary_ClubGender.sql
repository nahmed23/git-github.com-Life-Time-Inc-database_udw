CREATE PROC [reporting].[proc_ClubUsageSummary_ClubGender] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME],@ReportDimLocationKeyList [VARCHAR](8000),@MembershipTypeList [VARCHAR](8000),@MembershipStatusList [VARCHAR](8000) AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON
--DECLARE  @ReportBeginDate  DATETIME = '10/1/2018'
--DECLARE  @ReportEndDate DATETIME = '10/31/2018'
--DECLARE  @ReportDimLocationKeyList VARCHAR(8000) = '151'
--DECLARE  @MembershipStatusList VARCHAR(8000) = '< Ignore this prompt >'
--DECLARE  @MembershipTypeList VARCHAR(8000) = '< Ignore this prompt >'
----------------------------------------------------
DECLARE  @BeginDimDateKey CHAR(32)
DECLARE	 @EndDimDateKey CHAR(32)
SET @BeginDimDateKey = (SELECT dim_date_key FROM marketing.v_dim_date  WHERE calendar_date = @ReportBeginDate)
SET @EndDimDateKey = (SELECT dim_date_key FROM marketing.v_dim_date  WHERE calendar_date = @ReportEndDate)

DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = CONVERT(VARCHAR(21), GETDATE(), 100)

DECLARE @list_table VARCHAR(1000)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @ReportDimLocationKeyList, @list_table

SELECT DISTINCT DimClub.dim_club_key, dimclub.club_name, Dimclub.club_id
INTO #Locations
FROM marketing.v_dim_club DimClub
JOIN #club_list club_list
  ON club_list.Item = DimClub.club_id

DECLARE @HeaderDateRange VARCHAR(33)
SELECT @HeaderDateRange = BeginDimDate.standard_date_name + ' through ' + EndDimDate.standard_date_name
  FROM marketing.v_Dim_Date BeginDimDate
 CROSS JOIN marketing.v_Dim_Date EndDimDate
 WHERE BeginDimDate.Calendar_Date = @ReportBeginDate
   AND EndDimDate.Calendar_Date = @ReportEndDate

EXEC marketing.proc_parse_pipe_list @MembershipStatusList, @list_table = 'membership_status'
Select Item MembershipStatus 
INTO #MembershipStatusList
FROM #membership_status

DECLARE @HeaderMembershipStatusList VARCHAR(4000)
SET @HeaderMembershipStatusList = CASE WHEN '< Ignore this prompt >' IN (SELECT MembershipStatus FROM #MembershipStatusList) THEN 'All Membership Statuses'
                                       ELSE REPLACE(@MembershipStatusList,'|',', ') END

DECLARE @HeaderMembershipTypeList VARCHAR(8000)
SET @HeaderMembershipTypeList = CASE WHEN '< Ignore this prompt >' IN (@MembershipTypeList) THEN 'All Membership Types'
								ELSE REPLACE(@MembershipTypeList,'|',', ') END

DECLARE @list_table_membership VARCHAR(100)
SET @list_table_membership = 'membership_type_dim_product'

EXEC marketing.proc_operations_membership_type_list @MembershipTypeList, @list_table_membership
SELECT dim_mms_product_key as DimProductKey   
INTO #IncludeMembershipTypeDimProduct
FROM #membership_membership_type_dim_product

DECLARE @CorporateMembershipFlag CHAR(1)
SET @CorporateMembershipFlag = CASE WHEN (@MembershipTypeList) like '%Corporate Memberships%' THEN 'Y' ELSE 'N' END


SELECT
  FactMemberUsage.dim_club_key DimLocationKey,
  COUNT(*) TotalCheckins,
  SUM(CASE WHEN FactMemberUsage.gender_abbreviation = 'F'
				THEN 1
			ELSE 0 END) as FemaleCheckIns,
  SUM(CASE WHEN FactMemberUsage.gender_abbreviation = 'M'
				THEN 1
			ELSE 0 END) as MaleCheckIns,
  SUM(CASE WHEN FactMemberUsage.gender_abbreviation = 'U'
				THEN 1
			ELSE 0 END) as UndefinedCheckIns,
  COUNT(DISTINCT FactmemberUsage.dim_mms_membership_key) UniqueMembershipCount,
  COUNT(Distinct CASE WHEN FactMemberUsage.gender_abbreviation = 'F'
						THEN FactmemberUsage.dim_mms_checkin_member_key
					ELSE NULL END) as UniqueFemaleCount,
  COUNT(Distinct CASE WHEN FactMemberUsage.gender_abbreviation = 'M'
						THEN FactmemberUsage.dim_mms_checkin_member_key
					ELSE NULL END) as UniqueMaleCount,
  COUNT(Distinct CASE WHEN FactMemberUsage.gender_abbreviation = 'U'
						THEN FactmemberUsage.dim_mms_checkin_member_key
					ELSE NULL END) as UniqueUndefinedCount,
  SUM(CASE WHEN FactMemberusage.department_dim_mms_description_key > 3 Then 1 Else 0 END) LTHealthCheckIns
INTO #FactMemberUsageSummary
FROM marketing.v_fact_mms_member_usage FactMemberUsage
JOIN marketing.v_dim_date CheckInDimDate
  ON convert(date,FactMemberUsage.check_in_dim_date_time)= CheckInDimDate.calendar_date and CheckInDimDate.calendar_date BETWEEN @ReportBeginDate AND @ReportEndDate
JOIN #Locations Locations
  ON Locations.dim_club_key = FactMemberUsage.dim_club_key
JOIN marketing.v_dim_mms_membership FactMembership
  ON FactMemberUsage.dim_mms_membership_key = FactMembership.dim_mms_membership_key
JOIN marketing.v_dim_mms_membership_history MembershipHistory
  ON MembershipHistory.[dim_mms_membership_key] = FactMembership.dim_mms_membership_key
   AND MembershipHistory.[effective_date_time] <= @ReportEndDate
   AND MembershipHistory.[expiration_date_time] > @ReportEndDate

WHERE FactMemberUsage.check_in_dim_date_time >= @ReportBeginDate
  AND FactMemberUsage.check_in_dim_date_time <= @ReportEndDate + 1
  AND (FactMembership.dim_mms_membership_type_key IN (SELECT DimproductKey FROM #IncludeMembershipTypeDimProduct)
		OR (MembershipHistory.[corporate_membership_flag] = 'Y' and @CorporateMembershipFlag = 'Y'))    
  AND (Factmembership.[membership_status] IN (SELECT MembershipStatus FROM #MembershipStatusList)
		OR '< Ignore this prompt >' IN (SELECT MembershipStatus FROM #MembershipStatusList))
GROUP BY FactMemberUsage.dim_club_key

SELECT FactGuestClubUsage.dim_club_key DimLocationKey,
       Count(FactGuestClubUsage.fact_mms_guest_club_usage_key) GuestCheckInCount
  INTO #GuestCheckIns
  FROM marketing.v_fact_mms_guest_club_usage FactGuestClubUsage
  JOIN #Locations
    ON FactGuestClubUsage.dim_club_key = #Locations.dim_club_key
 WHERE FactGuestClubUsage.check_in_dim_date_key >= @BeginDimDateKey
   AND FactGuestClubUsage.check_in_dim_date_key <= @EndDimDateKey
 GROUP BY FactGuestClubUsage.dim_club_key

SELECT DimLocation.dim_club_key DimLocationKey,
	   dim_description.description AS RegionName,
       DimLocation.club_name ClubName,
       DimLocation.Club_code ClubCode,
       DimLocation.club_id MMSClubID,
       DimLocation.workday_region WorkdayRegion,
       #FactMemberUsageSummary.TotalCheckIns,
       #FactMemberUsageSummary.FemaleCheckIns,
       #FactMemberUsageSummary.MaleCheckIns,
       #FactMemberUsageSummary.UndefinedCheckIns,
       #FactMemberUsageSummary.UniqueFemaleCount,
       #FactMemberUsageSummary.UniqueMaleCount,
       #FactMemberUsageSummary.UniqueUndefinedCount,
       #FactMemberUsageSummary.UniqueMembershipCount,
       #GuestCheckIns.GuestCheckInCount GuestCheckIns,
       #FactMemberUsageSummary.LTHealthCheckIns
  INTO #Results
  FROM #FactMemberUsageSummary
  JOIN marketing.v_dim_club DimLocation
    ON #FactMemberUsageSummary.DimLocationKey = DimLocation.dim_club_key
  JOIN marketing.v_dim_description dim_description
    ON DimLocation.region_dim_description_key = dim_description.dim_description_key
  LEFT JOIN #GuestCheckIns
    ON #FactMemberUsageSummary.DimLocationKey = #GuestCheckIns.DimLocationKey

SELECT DimLocationKey,
       RegionName,
       ClubName,
       ClubCode,
       TotalCheckIns,
       FemaleCheckIns,
       MaleCheckIns,
       UndefinedCheckIns,
       UniqueFemaleCount,
       UniqueMaleCount,
       UniqueUndefinedCount,
       UniqueMembershipCount,
       GuestCheckIns,
       @ReportBeginDate ReportBeginDate,
       @ReportEndDate ReportEndDate,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       CAST(NULL AS VARCHAR(71)) HeaderEmptyResultSet,
       LTHealthCheckIns,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       @HeaderMembershipStatusList HeaderMembershipStatusList,
       MMSClubID,
       WorkdayRegion
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(40)) DimLocationKey,
       CAST(NULL AS VARCHAR(50)) RegionName,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(18)) ClubCode,
       CAST(NULL AS INT) TotalCheckIns,
       CAST(NULL AS INT) FemaleCheckIns,
       CAST(NULL AS INT) MaleCheckIns,
       CAST(NULL AS INT) UndefinedCheckIns,
       CAST(NULL AS INT) UniqueFemaleCount,
       CAST(NULL AS INT) UniqueMaleCount,
       CAST(NULL AS INT) UniqueUndefinedCount,
       CAST(NULL AS INT) UniqueMembershipCount,
       CAST(NULL AS INT) GuestCheckIns,
       @ReportBeginDate ReportBeginDate,
       @ReportEndDate ReportEndDate,
       @HeaderDateRange HeaderDateRange,
       @ReportRunDateTime ReportRunDateTime,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResultSet,
       CAST(NULL AS INT) LTHealthCheckIns,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       @HeaderMembershipStatusList HeaderMembershipStatusList,
       CAST(NULL AS VARCHAR(40)) MMSClubID,
       CAST(NULL AS VARCHAR(4)) WorkdayRegion
 WHERE (SELECT COUNT(*) FROM #Results) = 0
 ORDER BY RegionName,
          ClubName


 DROP TABLE #Locations
 DROP TABLE #MembershipStatusList
 DROP TABLE #IncludeMembershipTypeDimProduct
 DROP TABLE #FactMemberUsageSummary
 DROP TABLE #GuestCheckIns
 DROP TABLE #Results

 END