CREATE PROC [reporting].[proc_ChildCenterUsageCheckInDetail] @ReportBeginDate [DATETIME],@ReportBeginTime [VARCHAR](15),@ReportEndDate [DATETIME],@ReportEndTime [VARCHAR](15),@ReportMMSClubIDList [VARCHAR](1000),@ReportMemberID [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



------ Sample Execution
------ Exec [reporting].[proc_ChildCenterUsageCheckInDetail] '1/1/2012','10:00 AM','12/1/2012','10:00 AM','151|8',0
------

DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()),100),18,2),'  ',' ')

DECLARE @ReportBeginDateTime VARCHAR(21),
        @ReportEndDateTime VARCHAR(21),
        @StartDimDateKey INT,
        @StartDimTimeKey INT,
        @EndDimDateKey INT,
        @EndDimTimeKey INT 
SELECT @ReportBeginDateTime = ReportBeginDimDate.standard_date_name + ' ' + ReportBeginDimTime.display_12_hour_time,
       @ReportEndDateTime = ReportEndDimDate.standard_date_name + ' ' + ReportEndDimTime.display_12_hour_time,
       @StartDimDateKey = ReportBeginDimDate.dim_date_key,
       @StartDimTimeKey = ReportBeginDimTime.dim_time_key,
       @EndDimDateKey = ReportEndDimDate.dim_date_key,
       @EndDimTimeKey = ReportEndDimTime.dim_time_key
  FROM [marketing].[v_dim_date] ReportBeginDimDate
 CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
 CROSS JOIN [marketing].[v_dim_time] ReportBeginDimTime
 CROSS JOIN [marketing].[v_dim_time] ReportEndDimTime
 WHERE ReportBeginDimDate.calendar_date = @ReportBeginDate
   AND ReportEndDimDate.calendar_date = @ReportEndDate
   AND ReportBeginDimTime.Hour = DatePart(HH,Convert(Datetime,@ReportBeginTime))
   AND ReportBeginDimTime.Minute = DatePart(MI,Convert(Datetime,@ReportBeginTime))
   AND ReportEndDimTime.Hour = DatePart(HH,Convert(Datetime,@ReportEndTime))
   AND ReportEndDimTime.Minute = DatePart(MI,Convert(Datetime,@ReportEndTime))


IF OBJECT_ID('tempdb.dbo.#Locations', 'U') IS NOT NULL
  DROP TABLE #Locations; 

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @ReportMMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimDescription.description AS MMSRegion
  INTO #Locations
  FROM #club_list MMSClubIDList
  JOIN [marketing].[v_dim_club] DimClub
    ON MMSClubIDList.Item = DimClub.club_id
  JOIN [marketing].[v_dim_description] DimDescription
   ON DimDescription.dim_description_key = DimClub.region_dim_description_key 
 WHERE DimClub.club_type = 'Club'  
 
  

DECLARE @MembershipID INT
SET @MembershipID = CASE WHEN @ReportMemberID = 0 
                          THEN 0 
						  ELSE (SELECT membership_id FROM [marketing].[v_dim_mms_member] WHERE member_id = @ReportMemberID) END

IF OBJECT_ID('tempdb.dbo.#FactChildCenterUsage', 'U') IS NOT NULL
  DROP TABLE #FactChildCenterUsage; 

SELECT FactChildCenterUsage.fact_mms_child_center_usage_key AS FactChildCenterUsageKey,
       FactChildCenterUsage.check_in_dim_date_key AS CheckInDimDateKey,
       FactChildCenterUsage.check_in_dim_time_key AS CheckInDimTimeKey,
       FactChildCenterUsage.check_out_dim_date_key AS CheckOutDimDateKey,
       FactChildCenterUsage.check_out_dim_time_key AS CheckOutDimTimeKey,
       FactChildCenterUsage.dim_club_key AS DimClubKey,
       FactChildCenterUsage.child_dim_mms_member_key AS ChildDimMemberKey,
       --FactChildCenterUsage.primary_dim_member_key AS PrimaryDimMemberKey,    ------- Comment out in QA/Prod - UDW-7540
	   FactChildCenterUsage.primary_dim_mms_member_key AS PrimaryDimMemberKey,   ------- Comment out in DEV   - UDW-7540
       FactChildCenterUsage.check_in_dim_mms_member_key AS CheckInDimMemberKey,
       FactChildCenterUsage.check_out_dim_mms_member_key AS CheckOutDimMemberKey,
       FactChildCenterUsage.child_age_years AS ChildAgeYears,
       FactChildCenterUsage.child_age_months AS ChildAgeMonths,
       FactChildCenterUsage.length_of_stay_display AS LengthOfStayDisplay,
       FactChildCenterUsage.kids_play_check_in_count AS KidsPlayCheckInCount
  INTO #FactChildCenterUsage
  FROM [marketing].[v_fact_mms_child_center_usage] FactChildCenterUsage
  JOIN #Locations
    ON FactChildCenterUsage.dim_club_key = #Locations.dim_club_key
 WHERE(FactChildCenterUsage.check_in_dim_date_key = @StartDimDateKey AND FactChildCenterUsage.check_in_dim_date_key < @EndDimDateKey AND FactChildCenterUsage.check_in_dim_time_key >= @StartDimTimeKey)
    OR (FactChildCenterUsage.check_in_dim_date_key > @StartDimDateKey AND FactChildCenterUsage.check_in_dim_date_key = @EndDimDateKey AND FactChildCenterUsage.check_in_dim_time_key <= @EndDimTimeKey )
    OR (FactChildCenterUsage.check_in_dim_date_key > @StartDimDateKey  AND FactChildCenterUsage.check_in_dim_date_key < @EndDimDateKey)
    OR (FactChildCenterUsage.check_in_dim_date_key = @StartDimDateKey AND FactChildCenterUsage.check_in_dim_date_key = @EndDimDateKey AND FactChildCenterUsage.check_in_dim_time_key >= @StartDimTimeKey AND FactChildCenterUsage.check_in_dim_time_key <= @EndDimTimeKey) 


IF OBJECT_ID('tempdb.dbo.#FirstCheckInDateByMember', 'U') IS NOT NULL
  DROP TABLE #FirstCheckInDateByMember; 
  	
SELECT ChildDimMemberKey, 
       Min(CheckInDimDateKey) AS FirstCheckInDimDateKey
  INTO #FirstCheckInDateByMember
  FROM #FactChildCenterUsage
GROUP BY ChildDimMemberKey -- return one record for each child member

IF OBJECT_ID('tempdb.dbo.#FirstCheckInByMember', 'U') IS NOT NULL
  DROP TABLE #FirstCheckInByMember; 

SELECT #FactChildCenterUsage.ChildDimMemberKey, 
       Min(#FactChildCenterUsage.CheckInDimDateKey) AS FirstCheckInDimDateKey, 
       Min(#FactChildCenterUsage.CheckInDimTimeKey) AS FirstCheckInDimTimeKey
  INTO #FirstCheckInByMember
  FROM #FactChildCenterUsage
  JOIN #FirstCheckInDateByMember FirstCheckInDate  
    ON #FactChildCenterUsage.ChildDimMemberKey = FirstCheckInDate.ChildDimMemberKey  
   AND #FactChildCenterUsage.CheckInDimDateKey = FirstCheckInDate.FirstCheckInDimDateKey
GROUP BY #FactChildCenterUsage.ChildDimMemberKey -- return one record for each child member

IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;

SELECT DimLocation.MMSRegion AS MMSRegionName, 
       DimLocation.club_code AS ClubCode,
       PrimaryDimMember.member_id AS PrimaryMemberID,
       PrimaryDimMember.first_name AS PrimaryFirstName,
       PrimaryDimMember.last_name AS PrimaryLastName,
       ChildDimMember.member_id ChildMemberID,
       ChildDimMember.first_name ChildFirstName,
       ChildDimMember.last_name ChildLastName,
       #FactChildCenterUsage.ChildAgeYears,
       #FactChildCenterUsage.ChildAgeMonths,
       CAST(ChildAgeYears AS VARCHAR(2))+' yrs. '+ CAST(ChildAgeMonths - (ChildAgeYears * 12) AS VARCHAR(2))+' mos.'  JuniorMemberAge,
       #FactChildCenterUsage.LengthOfStayDisplay,
       CheckInDimDate.standard_date_name AS CheckInDate, -----
       CheckInDimTime.display_12_hour_time AS CheckInTime,
       CheckInDimMember.first_name AS CheckInByFirstName,
       CheckInDimMember.last_name AS CheckInByLastName,
       CheckOutDimMember.first_name AS CheckOutByFirstName,
       CheckOutDimMember.last_name AS CheckOutByLastName,
       IsNull(CheckOutDimDate.standard_date_name,CheckInDimDate.standard_date_name) AS CheckOutDate,
       IsNull(CheckOutDimTime.display_12_hour_time, '11:59 PM') AS CheckOutTime,
       FirstCheckInByMember.FirstCheckInDimDateKey,
       FirstCheckInByMember.FirstCheckInDimTimeKey,
       #FactChildCenterUsage.CheckInDimDateKey,
       #FactChildCenterUsage.CheckInDimTimeKey,
       #FactChildCenterUsage.KidsPlayCheckInCount AS KidsPlayCheckIns
  INTO #Results
  FROM #FactChildCenterUsage
  JOIN #Locations DimLocation    
    ON #FactChildCenterUsage.DimClubKey = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_mms_member] PrimaryDimMember 
    ON #FactChildCenterUsage.PrimaryDimMemberKey = PrimaryDimMember.dim_mms_member_key
   AND (PrimaryDimMember.membership_id = @MembershipID OR @MembershipID = 0)
  JOIN [marketing].[v_dim_mms_member] ChildDimMember
    ON #FactChildCenterUsage.ChildDimMemberKey = ChildDimMember.dim_mms_member_key
  JOIN [marketing].[v_dim_date] CheckInDimDate 
    ON #FactChildCenterUsage.CheckInDimDateKey = CheckInDimDate.dim_date_key
  JOIN [marketing].[v_dim_time] CheckInDimTime 
    ON #FactChildCenterUsage.CheckInDimTimeKey = CheckInDimTime.dim_time_key
  JOIN [marketing].[v_dim_mms_member] CheckInDimMember
    ON #FactChildCenterUsage.CheckInDimMemberKey = CheckInDimMember.dim_mms_member_key
  JOIN [marketing].[v_dim_mms_member] CheckOutDimMember
    ON #FactChildCenterUsage.CheckOutDimMemberKey = CheckOutDimMember.dim_mms_member_key
  JOIN [marketing].[v_dim_date] CheckOutDimDate 
    ON #FactChildCenterUsage.CheckOutDimDateKey = CheckOutDimDate.dim_date_key
  JOIN [marketing].[v_dim_time] CheckOutDimTime 
    ON #FactChildCenterUsage.CheckOutDimTimeKey = CheckOutDimTime.dim_time_key
  JOIN #FirstCheckInByMember FirstCheckInByMember 
    ON #FactChildCenterUsage.ChildDimMemberKey = FirstCheckInByMember.ChildDimMemberKey



SELECT MMSRegionName, 
       ClubCode,
       PrimaryMemberID,
       PrimaryFirstName,
       PrimaryLastName,
       ChildMemberID,
       ChildFirstName,
       ChildLastName,
       ChildAgeYears,
       ChildAgeMonths,
       JuniorMemberAge,
       LengthOfStayDisplay,
       CheckInDate, -----
       CheckInTime,
       CheckInByFirstName,
       CheckInByLastName,
       CheckOutByFirstName,
       CheckOutByLastName,
       CheckOutDate,
       CheckOutTime,
       FirstCheckInDimDateKey,
       FirstCheckInDimTimeKey,
       @ReportBeginDateTime ReportStartDateTime,
       @ReportEndDateTime ReportEndDateTime,
       CheckInDimDateKey,
       CheckInDimTimeKey,
       CASE @ReportMemberID WHEN 0 THEN 'All Junior Members for this date range'
                            ELSE 'All Junior Member Usage for the membership with MemberID '+cast(@ReportMemberID AS VARCHAR)
       END AS MemberSelectionSubHeader,
       @ReportRunDateTime ReportRunDateTime,
       CAST(NULL AS VARCHAR(71)) HeaderEmptyResultSet,
       KidsPlayCheckIns
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) MMSRegionName, 
       CAST(NULL AS VARCHAR(18)) ClubCode,
       CAST(NULL AS INT) PrimaryMemberID,
       CAST(NULL AS VARCHAR(50)) PrimaryFirstName,
       CAST(NULL AS VARCHAR(80)) PrimaryLastName,
       CAST(NULL AS INT) ChildMemberID,
       CAST(NULL AS VARCHAR(50)) ChildFirstName,
       CAST(NULL AS VARCHAR(80)) ChildLastName,
       CAST(NULL AS INT) ChildAgeYears,
       CAST(NULL AS INT) ChildAgeMonths,
       CAST(NULL AS VARCHAR(15)) JuniorMemberAge,
       CAST(NULL AS VARCHAR(16)) LengthOfStayDisplay,
       CAST(NULL AS VARCHAR(12)) CheckInDate,
       CAST(NULL AS VARCHAR(8)) CheckInTime,
       CAST(NULL AS VARCHAR(50)) CheckInByFirstName,
       CAST(NULL AS VARCHAR(80)) CheckInByLastName,
       CAST(NULL AS VARCHAR(50)) CheckOutByFirstName,
       CAST(NULL AS VARCHAR(80)) CheckOutByLastName,
       CAST(NULL AS VARCHAR(12)) CheckOutDate,
       CAST(NULL AS VARCHAR(8)) CheckOutTime,
       CAST(NULL AS INT) FirstCheckInDimDateKey,
       CAST(NULL AS INT) FirstCheckInDimTimeKey,
       @ReportBeginDateTime ReportStartDateTime,
       @ReportEndDateTime ReportEndDateTime,
       CAST(NULL AS INT) CheckInDimDateKey,
       CAST(NULL AS INT) CheckInDimTimeKey,
       CASE @ReportMemberID WHEN 0 THEN 'All Junior Members for this date range'
                            ELSE 'All Junior Member Usage for the membership with MemberID '+cast(@ReportMemberID AS VARCHAR)
       END AS MemberSelectionSubHeader,
       @ReportRunDateTime ReportRunDateTime,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResultSet,
       CAST(NULL AS INT) KidsPlayCheckIns
 WHERE (SELECT COUNT(*) FROM #Results) = 0
ORDER BY 
    MMSRegionName, 
    ClubCode,
    FirstCheckInDimDateKey,
    FirstCheckInDimTimeKey,
    ChildLastName,
    ChildFirstName,
    CheckInDimDateKey,
    CheckInDimTimeKey

DROP TABLE #Locations
DROP TABLE #FactChildCenterUsage
DROP TABLE #FirstCheckInByMember
DROP TABLE #FirstCheckInDateByMember 
DROP TABLE #Results


END
