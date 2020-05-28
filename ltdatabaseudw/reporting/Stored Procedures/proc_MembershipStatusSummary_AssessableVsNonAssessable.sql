CREATE PROC [reporting].[proc_MembershipStatusSummary_AssessableVsNonAssessable] @ReportYearMonth [CHAR](7),@MMSClubIDList [VARCHAR](8000),@MembershipTypeList [VARCHAR](8000) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

--- This SP returns counts Assessable, NonAssessable and Total Memberships
--- Execution Sample:  Exec proc_MembershipStatusSummary_AssessableVsNonAssessable '2018-10','1|4|6|52|151','< Ignore this prompt >'
DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = CAST(DATEADD(HH,-5,GETDATE()) AS nvarchar(30)) --UDW on UTC, subtracting 5 hours and formatting for report output

DECLARE @ReportEndDate DATETIME,
        @ReportStartDimDateKey INT,
        @ReportYear INT
SELECT @ReportEndDate = [month_ending_date]
       , @ReportStartDimDateKey = [month_starting_dim_date_key]
	   , @ReportYear = [year]
FROM [marketing].[v_dim_date] 
WHERE [four_digit_year_dash_two_digit_month] = @ReportYearMonth
AND [day_number_in_month] = 1

DECLARE @HeaderMembershipTypeList VARCHAR(8000)
SET @HeaderMembershipTypeList = CASE WHEN (@MembershipTypeList) LIKE '%< Ignore this prompt >%' THEN 'All Membership Types'
								ELSE REPLACE(@MembershipTypeList,'|',', ') END

DECLARE @list_table_membership VARCHAR(100)
SET @list_table_membership = 'membership_type_dim_product'
IF OBJECT_ID('tempdb.dbo.#IncludeMembershipTypeDimProduct', 'U') IS NOT NULL DROP TABLE #IncludeMembershipTypeDimProduct; 

EXEC marketing.proc_operations_membership_type_list @MembershipTypeList, @list_table_membership
SELECT dim_mms_product_key as DimProductKey   
INTO #IncludeMembershipTypeDimProduct
FROM #membership_membership_type_dim_product

DECLARE @CorporateMembershipFlag CHAR(1)
SET @CorporateMembershipFlag = CASE WHEN (@MembershipTypeList) like '%Corporate Memberships%' THEN 'Y' ELSE 'N' END


IF OBJECT_ID('tempdb.dbo.#DimLocation', 'U') IS NOT NULL DROP TABLE #DimLocation; 
IF OBJECT_ID('tempdb.dbo.#club_list', 'U') IS NOT NULL DROP TABLE #club_list; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'
EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table

SELECT DimClub.dim_club_key
, dim_description_region.description MMSRegionName
, DimClub.club_name
, DimClub.local_currency_code
, DimClub.club_code
, DimClub.club_id
 INTO #DimLocation
 FROM #club_list MMSClubIDList
	JOIN [marketing].[v_dim_club] DimClub 
		ON MMSClubIDList.Item = DimClub.club_id
    JOIN [marketing].[v_dim_description] dim_description_region
		ON dim_description_region.dim_description_key = DimClub.region_dim_description_key

DECLARE @ReportingCurrencyCode VARCHAR(15)
SELECT @ReportingCurrencyCode = CASE WHEN COUNT(*) = 1 THEN MIN(LocalCurrencyCodes.local_currency_code) ELSE 'USD' END
FROM (SELECT DISTINCT local_currency_code FROM #DimLocation) LocalCurrencyCodes


IF OBJECT_ID('tempdb.dbo.#ExchangeRate', 'U') IS NOT NULL DROP TABLE #ExchangeRate; 

SELECT DimPlanExchangeRate.[from_currency_code]
, DimPlanExchangeRate.[to_currency_code]
, DimPlanExchangeRate.[exchange_rate] 
INTO #ExchangeRate
FROM [marketing].[v_dim_exchange_rate] DimPlanExchangeRate
  JOIN #DimLocation
    ON #DimLocation.local_currency_code = DimPlanExchangeRate.[from_currency_code]
where DimPlanExchangeRate.[exchange_rate_type_description] = 'Monthly Average Exchange Rate'
	   AND @ReportingCurrencyCode = DimPlanExchangeRate.[to_currency_code]
       AND @ReportEndDate = DimPlanExchangeRate.[effective_date]

SELECT #DimLocation.MMSRegionName MMSRegion
	   , #DimLocation.MMSRegionName + ' - ' + #DimLocation.club_name MMSRegionDashClubName
	   , CASE WHEN LEN(MembershipType.attribute_membership_status_summary_group_description) = 0 
			THEN 'Obsolete Membership Status Summary Type Group'
			ELSE MembershipType.attribute_membership_status_summary_group_description END AS MembershipStatusSummaryTypeGroup
	   , MembershipType.membership_type MembershipType
	   , FactMembership.current_price * ExchangeRate.[exchange_rate] DuesPrice
	   , SUM(CASE WHEN DimDescription.description = 'Active' THEN 1 ELSE 0 END) AssessableMembershipsActive
	   , SUM(CASE WHEN DimDescription.description = 'Pending Termination' THEN 1 ELSE 0 END) AssessableMembershipsPendingTermination
	   , SUM(CASE WHEN DimDescription.description = 'Non-Paid' THEN 1 ELSE 0 END) AssessableMembershipsNonPaidActive
	   , SUM(CASE WHEN DimDescription.description in ('Active','Pending Termination','Non-Paid') THEN 1 ELSE 0 END) AssessableMembershipsTotal
	   , FactMembership.current_price * ExchangeRate.[exchange_rate] * SUM(CASE WHEN DimDescription.description in ('Active','Pending Termination','Non-Paid') THEN 1 ELSE 0 END) AssessableMembershipsEstimatedDues
	   , SUM(CASE WHEN DimDescription.description = 'Suspended' THEN 1 ELSE 0 END) NonAssessableMembershipsSuspended
	   , SUM(CASE WHEN DimDescription.description = 'Late Activation' THEN 1 ELSE 0 END) NonAssessableMembershipsLateActivation
	   , SUM(CASE WHEN DimDescription.description = 'Non-Paid, Late Activation' THEN 1 ELSE 0 END) NonAssessableMembershipsNonPaidLateActivation
	   , SUM(CASE WHEN DimDescription.description <> 'Terminated' THEN 1 ELSE 0 END) MembershipTotal
	   , @ReportRunDateTime ReportRunDateTime
	   , @ReportingCurrencyCode ReportingCurrencyCode
	   , @HeaderMembershipTypeList HeaderMembershipTypeList
  FROM [marketing].[v_dim_mms_membership_history] FactMembership
  JOIN #DimLocation
    ON FactMembership.club_id = #DimLocation.club_id
  JOIN [marketing].[v_dim_description] DimDescription
    ON DimDescription.dim_description_key = FactMembership.membership_status_dim_description_key
  LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipType
      ON MembershipType.membership_type_id = FactMembership.membership_type_id
  JOIN #ExchangeRate ExchangeRate
	on #DimLocation.local_currency_code = ExchangeRate.[from_currency_code]
    AND @ReportingCurrencyCode = ExchangeRate.[to_currency_code]
 WHERE Cast(FactMembership.[effective_date_time] as date) <= @ReportEndDate
   AND Cast(FactMembership.[expiration_date_time] as date) > @ReportEndDate
   AND (FactMembership.[membership_expiration_date] > DATEADD(DD, 1, DATEADD(M, -1, @ReportEndDate)) -- Membership Expiration > First of the Month
       OR FactMembership.[membership_expiration_date] IS NULL)
   AND (FactMembership.[dim_mms_membership_type_key] IN (SELECT DimProductKey FROM #IncludeMembershipTypeDimProduct)
       OR (FactMembership.[corporate_membership_flag] = 'Y' AND @CorporateMembershipFlag = 'Y'))
 GROUP BY #DimLocation.MMSRegionName 
	   , #DimLocation.MMSRegionName + ' - ' + #DimLocation.club_name 
	   , CASE WHEN LEN(MembershipType.attribute_membership_status_summary_group_description) = 0 
			THEN 'Obsolete Membership Status Summary Type Group'
			ELSE MembershipType.attribute_membership_status_summary_group_description END
	   , MembershipType.membership_type 
	   , FactMembership.current_price
	   , ExchangeRate.[exchange_rate]
ORDER BY MMSRegionDashClubName, MembershipStatusSummaryTypeGroup, MembershipType, 5

DROP TABLE #IncludeMembershipTypeDimProduct 
DROP TABLE #DimLocation
DROP TABLE #club_list
DROP TABLE #ExchangeRate

END

