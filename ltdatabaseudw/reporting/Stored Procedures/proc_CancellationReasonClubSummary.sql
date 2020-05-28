CREATE PROC [reporting].[proc_CancellationReasonClubSummary] @InputCancellationReportType [VARCHAR](50),@InputBeginningDate [DATETIME],@InputEndingDate [DATETIME],@DimLocationKeyList [VARCHAR](4000),@MembershipTypeList [VARCHAR](8000) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

--- This SP returns counts for Membership Cancellation reasons for selected clubs and time frame from marketing.v_dim_mms_membership_history 
--- Execution Sample:  Exec [reporting].[proc_CancellationReasonClubSummary] 'Cancellation Requested Date', '8/1/2018', '8/15/2018', '52', 'All Memberships - Excluding Founders|Corporate Memberships|Employee Memberships'

DECLARE @HeaderDateRange VARCHAR(100),
        @BeginDimDateKey INT,
        @EndDimDateKey INT,
        @EndFourDigitYearDashTwoDigitMonth CHAR(7),
        @EndMonthYear VARCHAR(20),
        @EndStandardDateDescription VARCHAR(18)
		
SELECT @HeaderDateRange = ReportBeginDimDate.full_date_description + ' through ' + ReportEndDimDate.full_date_description
     , @BeginDimDateKey = ReportBeginDimDate.dim_date_key
     , @EndDimDateKey = ReportEndDimDate.dim_date_key
	 , @EndFourDigitYearDashTwoDigitMonth = ReportEndDimDate.four_digit_year_dash_two_digit_month
	 , @EndMonthYear = ReportEndDimDate.month_name_year
	 , @EndStandardDateDescription = ReportEndDimDate.full_date_description
FROM [marketing].[v_dim_date] ReportBeginDimDate
  CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
WHERE ReportBeginDimDate.calendar_date = @InputBeginningDate
AND ReportEndDimDate.calendar_date = @InputEndingDate

DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = CAST(DATEADD(HH,-5,GETDATE()) AS nvarchar(30)) --UDW on UTC, subtracting 5 hours and formatting for report output

DECLARE @CancellationFlag CHAR(1)
SELECT @CancellationFlag = CASE WHEN @InputCancellationReportType = 'Cancellation Requested Date' THEN 'Y' ELSE 'N' END

DECLARE @ExpiredFlag CHAR(1)
SELECT @ExpiredFlag = CASE WHEN @InputCancellationReportType = 'Termination Date' THEN 'Y' ELSE 'N' END

--Delete temp tables if they exist
IF OBJECT_ID('tempdb.dbo.#ClubKeys', 'U') IS NOT NULL DROP TABLE #ClubKeys; 
IF OBJECT_ID('tempdb.dbo.#club_list', 'U') IS NOT NULL DROP TABLE #club_list; 
IF OBJECT_ID('tempdb.dbo.#IncludeMembershipTypeDimProduct', 'U') IS NOT NULL DROP TABLE #IncludeMembershipTypeDimProduct; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @DimLocationKeyList,@list_table

SELECT DimClub.dim_club_key 
 INTO #ClubKeys
 FROM #club_list ClubIDList
 JOIN [marketing].[v_dim_club] DimClub
 ON ClubIDList.Item = DimClub.club_id

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
				              
SELECT dim_description_region.description RegionName
	   , DimLocation.club_name ClubName
	   , DimLocation.club_id ClubID
	   , DimDescription.dim_description_id DimDescriptionID
	   , DimDescription.Description CancellationReason
	   , SUM(CASE WHEN DimDate.four_digit_year_two_digit_month_two_digit_day = @EndDimDateKey THEN 1 ELSE 0 END) CountForEndDate
       , SUM(CASE WHEN DimDate.four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth THEN 1 ELSE 0 END) CountForEndingMonth
       , COUNT(*) CountForPeriod
	   , @EndStandardDateDescription EndDateDescription
       , @EndMonthYear EndMonthYear
       , @HeaderDateRange HeaderDateRange
       , @ReportRunDateTime ReportRunDateTime
       , @HeaderMembershipTypeList HeaderMembershipTypeList
  FROM marketing.v_dim_mms_membership_history FactMembership
  JOIN marketing.v_dim_club DimLocation
    ON FactMembership.club_id  = DimLocation.club_id 
  JOIN marketing.v_dim_description dim_description_region
    ON dim_description_region.dim_description_key = DimLocation.region_dim_description_key
  JOIN marketing.v_dim_description DimDescription
    ON FactMembership.termination_reason_dim_description_key = DimDescription.dim_description_key
  JOIN #ClubKeys 
    ON #ClubKeys.dim_club_key = DimLocation.dim_club_key
  JOIN marketing.v_dim_date DimDate 
    ON DimDate.calendar_date = CASE WHEN @CancellationFlag = 'Y' THEN FactMembership.membership_cancellation_request_date
                                    WHEN @ExpiredFlag = 'Y' THEN FactMembership.membership_expiration_date END
  LEFT JOIN marketing.v_dim_mms_member DimMMSMember
    ON FactMembership.membership_id = DimMMSMember.membership_id
	   AND DimMMSMember.val_member_type_id = 1 -- Primary Member
  LEFT JOIN marketing.v_dim_mms_company DimCompany
    ON FactMembership.dim_mms_company_key = DimCompany.dim_mms_company_key
  JOIN marketing.v_dim_mms_membership_type DimMembershipType 
      ON FactMembership.dim_mms_membership_type_key = DimMembershipType.dim_mms_membership_type_key 
 WHERE FactMembership.effective_date_time <= @InputEndingDate
   AND FactMembership.expiration_date_time > @InputEndingDate
   AND DimDate.four_digit_year_two_digit_month_two_digit_day BETWEEN CAST(@BeginDimDateKey AS CHAR(8)) AND CAST(@EndDimDateKey AS CHAR(8))
   AND (DimMembershipType.dim_mms_product_key IN (SELECT DimProductKey FROM #IncludeMembershipTypeDimProduct)
        OR (FactMembership.corporate_membership_flag = 'Y' AND @CorporateMembershipFlag = 'Y'))
 GROUP BY dim_description_region.description 
	   , DimLocation.club_name 
	   , DimLocation.club_id 
	   , DimDescription.dim_description_id 
	   , DimDescription.Description 
 ORDER BY RegionName, ClubName, CancellationReason


DROP TABLE #ClubKeys
DROP TABLE #club_list
DROP TABLE #IncludeMembershipTypeDimProduct

 
END

