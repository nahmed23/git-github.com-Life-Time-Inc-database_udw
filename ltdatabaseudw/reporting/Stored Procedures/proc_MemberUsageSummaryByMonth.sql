CREATE PROC [reporting].[proc_MemberUsageSummaryByMonth] @BeginningYearMonth [VARCHAR](7),@EndingYearMonth [VARCHAR](7),@MMSClubIDList [VARCHAR](8000),@CheckInGroupList [VARCHAR](8000),@MembershipTypeList [VARCHAR](8000),@MembershipStatusList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



------ Execution Sample
-------- exec [reporting].[proc_MemberUsageSummaryByMonth] '2017-10','2017-12','151|8|10','< Ignore this prompt >','Employee Memberships','Active'
------

 ---- set needed datetime variables
DECLARE @ReportRunDateTime Datetime
DECLARE @StartDimDateKey INT
DECLARE @EndDimDateKey INT
DECLARE @EndDate DATETIME
DECLARE @HistoricalStartDate Datetime


SET @ReportRunDateTime = GetDate()
SET @StartDimDateKey = (SELECT MIN(month_starting_dim_date_key) FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @BeginningYearMonth)
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @EndingYearMonth AND last_day_in_month_flag = 'Y')
SET @EndDate = (SELECT calendar_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @HistoricalStartDate = DATEADD(DD,-364,@EndDate)



  ----- Create club temp table

IF OBJECT_ID('tempdb.dbo.#Locations', 'U') IS NOT NULL
  DROP TABLE #Locations; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key, DimClub.club_id, DimClub.club_name,
       DimClub.club_code,
	   Region.Description AS MMSRegion,
       @StartDimDateKey AS StartDimDateKey,
	   @EndDimDateKey AS EndDimDateKey,
	   @EndDate AS EndDate, 
	   @HistoricalStartDate AS HistoricalStartDate
  INTO #Locations
  FROM #club_list MMSClubIDList
  JOIN [marketing].[v_dim_club] DimClub
    ON MMSClubIDList.Item = DimClub.club_id
  JOIN [marketing].[v_dim_description] Region
    ON DimClub.region_dim_description_key = Region.dim_description_key


	-----  Set up as a replacement to the LTFDM "STUFF()" functionality to create comma separated string of values

	----- to create @HeaderClubList
IF OBJECT_ID('tempdb.dbo.#ClubSort', 'U') IS NOT NULL
  DROP TABLE #ClubSort; 

SELECT club_name,
ROW_NUMBER() OVER(ORDER BY club_name ASC ) ClubNameSort
  INTO #ClubSort
  FROM #Locations

  DECLARE @MaxClubSort INT
  SET @MaxClubSort = (SELECT MAX(ClubNameSort) FROM #ClubSort)

DECLARE @HeaderClubList VARCHAR(1000)
DECLARE @Sort INT
SET @HeaderClubList = ''
SET @Sort = 1
WHILE @Sort <= @MaxClubSort
BEGIN 
  SET @HeaderClubList = @HeaderClubList + (CASE WHEN @Sort = 1 
                                                THEN (Select club_name FROM #ClubSort WHERE ClubNameSort = @Sort)
												ELSE ', ' + (Select club_name FROM #ClubSort WHERE ClubNameSort = @Sort)
												END);
  SET @Sort = @Sort+1;
END


	----- to create @HeaderRegionList
IF OBJECT_ID('tempdb.dbo.#RegionSort', 'U') IS NOT NULL
  DROP TABLE #RegionSort; 

SELECT MMSRegion,
ROW_NUMBER() OVER(ORDER BY MMSRegion ASC ) RegionNameSort
  INTO #RegionSort
  FROM #Locations
  GROUP BY MMSRegion

  DECLARE @MaxRegionSort INT
  SET @MaxRegionSort = (SELECT MAX(RegionNameSort) FROM #RegionSort)

DECLARE @HeaderRegionList VARCHAR(1000)
--DECLARE @Sort INT  ---- Already declared in prior script
SET @HeaderRegionList = ''
SET @Sort = 1
WHILE @Sort <= @MaxRegionSort
BEGIN 
  SET @HeaderRegionList = @HeaderRegionList + (CASE WHEN @Sort = 1 
                                                THEN (Select MMSRegion FROM #RegionSort WHERE RegionNameSort = @Sort)
												ELSE ', ' + (Select MMSRegion FROM #RegionSort WHERE RegionNameSort = @Sort)
												END);
  SET @Sort = @Sort+1;
END



----- Create #MembershipStatus temp table
IF OBJECT_ID('tempdb.dbo.#MembershipStatus', 'U') IS NOT NULL
  DROP TABLE #MembershipStatus; 

SET @list_table = 'status_list'

  EXEC marketing.proc_parse_pipe_list @MembershipStatusList,@list_table

	
SELECT StatusList.Item AS MembershipStatusDescription,
      dim_description_id,
      dim_description_key
  INTO #MembershipStatus
  FROM #status_list  StatusList
   LEFT JOIN [marketing].[v_dim_description] DimDescription
     ON StatusList.Item = DimDescription.description
	 AND DimDescription.source_object = 'r_mms_val_membership_status'






DECLARE @HeaderMembershipStatusList VARCHAR(8000)
SET @HeaderMembershipStatusList = (CASE WHEN @MembershipStatusList ='< Ignore this prompt >' 
                                        THEN 'All Membership Statuses'
                                       ELSE REPLACE(@MembershipStatusList,'|',', ') END)



----- Create checkin Group temp table
IF OBJECT_ID('tempdb.dbo.#CheckInGroup', 'U') IS NOT NULL
  DROP TABLE #CheckInGroup; 

SET @list_table = 'CheckInGroup_list'

  EXEC marketing.proc_parse_pipe_list @CheckInGroupList,@list_table
	
SELECT CheckinGroupList.Item AS CheckInGroup
  INTO #CheckInGroup
  FROM #CheckInGroup_list  CheckinGroupList

DECLARE @HeaderCheckInGroupList VARCHAR(8000)
SET @HeaderCheckInGroupList = (CASE WHEN @CheckInGroupList = '< Ignore this prompt >' 
                                     THEN 'All Check-In Groups'
                                    ELSE REPLACE(@CheckInGroupList,'|',',') END)





 ---- Create a membership type table
 IF OBJECT_ID('tempdb.dbo.#CheckInGroupDimProduct', 'U') IS NOT NULL
  DROP TABLE #CheckInGroupDimProduct; 

SELECT DISTINCT MembershipType.dim_mms_product_key,
       CheckInGroup.CheckInGroup,
	   MembershipProduct.product_description              
  INTO #CheckInGroupDimProduct
  FROM [marketing].[v_dim_mms_membership_type] MembershipType
  JOIN #CheckInGroup CheckInGroup
    ON MembershipType.check_in_group_description = CheckInGroup.CheckInGroup
    OR CheckInGroup.CheckInGroup = '< Ignore this prompt >'
  JOIN [marketing].[v_dim_mms_product] MembershipProduct
    ON MembershipType.dim_mms_product_key = MembershipProduct.dim_mms_product_key



	

DECLARE @HeaderMembershipTypeList VARCHAR(8000)
SET @HeaderMembershipTypeList = (CASE WHEN @MembershipTypeList = '< Ignore this prompt >'  
                                          THEN 'All Membership Types'
                                     ELSE REPLACE(@MembershipTypeList,'|',',') 
									 END)


  ----- Create temp table holding product keys for selected membership type groups	
   IF OBJECT_ID('tempdb.dbo.#IncludeMembershipTypeDimProduct', 'U') IS NOT NULL
  DROP TABLE #IncludeMembershipTypeDimProduct; 
  								 
SET @list_table = 'membership_type_dim_product'

  EXEC marketing.proc_operations_membership_type_list @MembershipTypeList,@list_table
	
SELECT dim_mms_product_key
  INTO #IncludeMembershipTypeDimProduct
  FROM #membership_membership_type_dim_product




DECLARE @CorporateMembershipFlag CHAR(1)
SET @CorporateMembershipFlag = CASE WHEN @MembershipTypeList like '%Corporate Memberships%' THEN 'Y' ELSE 'N' END

  ----- Create temp table holding the list of members	
   IF OBJECT_ID('tempdb.dbo.#Members', 'U') IS NOT NULL
  DROP TABLE #Members; 

SELECT DimMember.dim_mms_member_key,
       DimMember.member_id AS MemberID,
       DimMember.customer_name_last_first AS MemberName,
       CASE WHEN DimMember.member_active_flag = 'Y' THEN 'Active'
            ELSE 'Inactive' END MemberStatus,
       CASE WHEN DimMember.gender_abbreviation = 'M' THEN 'Male'
            WHEN DimMember.gender_abbreviation = 'F' THEN 'Female'
            ELSE 'Undefined' END Gender,
       DimMember.join_date,
       MemberType.description AS MemberType,
       DimMember.membership_id AS MembershipID,
       Locations.dim_club_key,
	   Locations.club_name,
	   MembershipType.product_description AS MembershipTypeProductDescription,
	   MembershipStatusDescription.description AS MembershipStatus,
	   Cast(DimMMSMembership.created_date_time AS Date) MembershipCreatedDate,
	   MembershipType.attribute_membership_status_summary_group_description AS MembershipStatusSummaryTypeGroup,
       MembershipType.check_in_group_description,
	   MembershipType.family_status_description
  INTO #Members
  FROM [marketing].[v_dim_mms_membership_history] DimMMSMembership
  JOIN #Locations Locations
    ON DimMMSMembership.home_dim_club_key = Locations.dim_club_key
  JOIN [marketing].[v_dim_mms_member_history] DimMember
    ON DimMMSMembership.dim_mms_membership_key = DimMember.dim_mms_membership_key
   AND DimMember.effective_date_time <= @EndDate
   AND DimMember.expiration_date_time > @EndDate
  JOIN [marketing].[v_dim_description] MemberType
    ON DimMember.member_type_dim_description_key = MemberType.dim_description_key
  JOIN [marketing].[v_dim_mms_membership_type] MembershipType
	ON   DimMMSMembership.dim_mms_membership_type_key = MembershipType.dim_mms_membership_type_key
  JOIN [marketing].[v_dim_description] MembershipStatusDescription
    ON DimMMSMembership.membership_status_dim_description_key = MembershipStatusDescription.dim_description_key
  JOIN #MembershipStatus MembershipStatus
    ON MembershipStatusDescription.dim_description_key = IsNull(MembershipStatus.dim_description_key,'0')
	 OR MembershipStatus.MembershipStatusDescription = '< Ignore this prompt >'
 WHERE DimMMSMembership.effective_date_time <= @EndDate
   AND DimMMSMembership.expiration_date_time > @EndDate
   AND MembershipType.dim_mms_product_key IN (SELECT dim_mms_product_key FROM #CheckInGroupDimProduct)
   AND (MembershipType.dim_mms_product_key IN (SELECT dim_mms_product_key FROM #IncludeMembershipTypeDimProduct)   
        OR (DimMMSMembership.corporate_membership_flag = 'Y' AND @CorporateMembershipFlag = 'Y'))      ----------- FactMembership.CorporateMembershipFlag hardcoded to "U" pending completion of Defect #UDW-7189
GROUP BY DimMember.dim_mms_member_key,
       DimMember.member_id,
       DimMember.customer_name_last_first,
       CASE WHEN DimMember.member_active_flag = 'Y' THEN 'Active'
            ELSE 'Inactive' END,
       CASE WHEN DimMember.gender_abbreviation = 'M' THEN 'Male'
            WHEN DimMember.gender_abbreviation = 'F' THEN 'Female'
            ELSE 'Undefined' END,
       DimMember.join_date,
       MemberType.description,
       DimMember.membership_id,
       Locations.dim_club_key,
	   Locations.club_name,
	   MembershipType.product_description,
	   MembershipStatusDescription.description,
	   Cast(DimMMSMembership.created_date_time AS Date),
	   MembershipType.attribute_membership_status_summary_group_description,
       MembershipType.check_in_group_description,
	   MembershipType.family_status_description




  ---- return all usage for the returned members
  IF OBJECT_ID('tempdb.dbo.#Usage', 'U') IS NOT NULL
  DROP TABLE #Usage; 

SELECT Members.dim_mms_member_key,
       Members.MemberID,
       Cast(FactMemberUsage.check_in_dim_date_time AS Date) CheckInDate
  INTO #Usage
  FROM [marketing].[v_fact_mms_member_usage] FactMemberUsage
  JOIN #Members  Members
    ON FactMemberUsage.dim_mms_checkin_member_key = Members.dim_mms_member_key
 WHERE FactMemberUsage.check_in_dim_date_time >= @HistoricalStartDate
   AND FactMemberUsage.check_in_dim_date_time < DateAdd(DD,1,@EndDate)



   
  IF OBJECT_ID('tempdb.dbo.#MonthUsage', 'U') IS NOT NULL
  DROP TABLE #MonthUsage; 

SELECT #Usage.dim_mms_member_key,
       CheckInDimDate.four_digit_year_dash_two_digit_month + ' Check-Ins' CheckInYearMonthLabel,
       COUNT(#Usage.CheckInDate) CheckInCount
  INTO #MonthUsage
  FROM #Usage
  JOIN [marketing].[v_dim_date] CheckInDimDate
    ON CheckInDimDate.calendar_date = #Usage.CheckInDate
 WHERE CheckInDimDate.dim_date_key >= @StartDimDateKey
   AND CheckInDimDate.dim_date_key <= @EndDimDateKey
 GROUP BY #Usage.dim_mms_member_key,
          CheckInDimDate.four_digit_year_dash_two_digit_month

  IF OBJECT_ID('tempdb.dbo.#HistoricalUsage', 'U') IS NOT NULL
  DROP TABLE #HistoricalUsage; 

SELECT #Usage.dim_mms_member_key,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-13,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast14Days,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-29,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast30Days,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-59,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast60Days,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-89,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast90Days,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-179,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast180Days,
       SUM(CASE WHEN #Usage.CheckInDate >= DATEADD(DD,-364,@EndDate) THEN 1 ELSE 0 END) TotalCheckInsLast365Days
  INTO #HistoricalUsage
  FROM #Usage
 GROUP BY #Usage.dim_mms_member_key


 
  IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;

SELECT #Members.club_name MembershipHomeClub,
       #Members.MembershipID,
       #Members.check_in_group_description AS CheckInGroup,
       #Members.MembershipTypeProductDescription MembershipType,
       #Members.MembershipStatusSummaryTypeGroup,
       CASE WHEN #Members.family_status_description = 'Single Membership Type' THEN 'Single'
            WHEN #Members.family_status_description = 'Couple Membership Type' THEN 'Couple'
            WHEN #Members.family_status_description = 'Family Membership Type' THEN 'Family' END MembershipFamilyStatus,
       #Members.MembershipStatus,
       #Members.MembershipCreatedDate,
       #Members.MemberID,
       #Members.MemberName,
       #Members.MemberType,
       #Members.MemberStatus,
       #Members.Gender,
       #Members.join_date MemberJoinDate,
       #HistoricalUsage.TotalCheckInsLast14Days,
       #HistoricalUsage.TotalCheckInsLast30Days,
       #HistoricalUsage.TotalCheckInsLast60Days,
       #HistoricalUsage.TotalCheckInsLast90Days,
       #HistoricalUsage.TotalCheckInsLast180Days,
       #HistoricalUsage.TotalCheckInsLast365Days,
       #MonthUsage.CheckInYearMonthLabel,
       #MonthUsage.CheckInCount
  INTO #Results
  FROM #MonthUsage
  JOIN #Members
    ON #MonthUsage.dim_mms_member_key = #Members.dim_mms_member_key
  JOIN #HistoricalUsage
    ON #MonthUsage.dim_mms_member_key = #HistoricalUsage.dim_mms_member_key



SELECT MembershipHomeClub,
       MembershipID,
       CheckInGroup,
       MembershipType,
       MembershipStatusSummaryTypeGroup,
       MembershipFamilyStatus,
       MembershipStatus,
       MembershipCreatedDate,
       MemberID,
       MemberName,
       MemberType,
       MemberStatus,
       Gender,
       MemberJoinDate,
       TotalCheckInsLast14Days,
       TotalCheckInsLast30Days,
       TotalCheckInsLast60Days,
       TotalCheckInsLast90Days,
       TotalCheckInsLast180Days,
       TotalCheckInsLast365Days,
       CheckInYearMonthLabel,
       CheckInCount,
       @BeginningYearMonth + ' through ' + @EndingYearMonth HeaderYearMonthRange,
       @HeaderCheckInGroupList HeaderCheckInGroupList,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       @HeaderMembershipStatusList HeaderMembershipStatusList,
       @HeaderRegionList HeaderRegionList,   
       @HeaderClubList HeaderClubList,       
       CAST('' as Varchar(70)) HeaderEmptyResult,
       @ReportRunDateTime ReportRunDateTime
  FROM #Results
UNION ALL
SELECT Cast(NULL AS VARCHAR(50)) MembershipHomeClub,
       NULL MembershipID,
       Cast(NULL AS VARCHAR(50)) CheckInGroup,
       Cast(NULL AS VARCHAR(50)) MembershipType,
       Cast(NULL AS VARCHAR(50)) MembershipStatusSummaryTypeGroup,
       Cast(NULL AS VARCHAR(6)) MembershipFamilyStatus,
       Cast(NULL AS VARCHAR(50)) MembershipStatus,
       Cast(NULL AS VARCHAR(12)) MembershipCreatedDate,
       NULL MemberID,
       Cast(NULL AS VARCHAR(132)) MemberName,
       CAST(NULL as Varchar(50)) MemberType,
       CAST(NULL as VARCHAR(8)) MemberStatus,
       Cast(NULL AS VARCHAR(9)) Gender,
       CAST(NULL as Varchar(12)) MemberJoinDate,
       NULL TotalCheckInsLast14Days,
       NULL TotalCheckInsLast30Days,
       NULL TotalCheckInsLast60Days,
       NULL TotalCheckInsLast90Days,
       NULL TotalCheckInsLast180Days,
       NULL TotalCheckInsLast365Days,
       Cast(NULL AS VARCHAR(17)) CheckInYearMonthLabel,
       NULL CheckInCount,
       @BeginningYearMonth + ' through ' + @EndingYearMonth HeaderYearMonthRange,
       @HeaderCheckInGroupList HeaderCheckInGroupList,
       @HeaderMembershipTypeList HeaderMembershipTypeList,
       @HeaderMembershipStatusList HeaderMembershipStatusList,
       @HeaderRegionList HeaderRegionList,   
       @HeaderClubList HeaderClubList,       
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResult,
       @ReportRunDateTime ReportRunDateTime
 WHERE (SELECT COUNT(*) FROM #Results) = 0
order by MembershipID, CheckInYearMonthLabel

DROP TABLE #ClubSort
DROP TABLE #RegionSort
DROP TABLE #Locations
DROP TABLE #MembershipStatus
DROP TABLE #CheckInGroup
DROP TABLE #CheckInGroupDimProduct
DROP TABLE #IncludeMembershipTypeDimProduct
DROP TABLE #Members
DROP TABLE #Usage
DROP TABLE #MonthUsage
DROP TABLE #HistoricalUsage
DROP TABLE #Results




END
