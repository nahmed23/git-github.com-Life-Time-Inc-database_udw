CREATE PROC [reporting].[proc_ProductAllocationRulesSetup] @DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@ProductStatusList [VARCHAR](1000),@StartDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END



----- Sample Executions
 ----- Exec [reporting].[proc_ProductAllocationRulesSetup] 'All Departments', 'Active|Inactive', '1/12/2019' 
 
DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @ReportingYearStartingFourDigitYearDashTwoDigitMonth VARCHAR(7)
DECLARE @ReportingYearEndingFourDigitYearDashTwoDigitMonth VARCHAR(7)
DECLARE @DimDateKey INT
DECLARE @HeaderReportDate VARCHAR(12)

SELECT @ReportingYearStartingFourDigitYearDashTwoDigitMonth = MIN(StartEndMonthYear.four_digit_year_dash_two_digit_month),
       @ReportingYearEndingFourDigitYearDashTwoDigitMonth = MAX(StartEndMonthYear.four_digit_year_dash_two_digit_month),
       @DimDateKey = CurrentYear.dim_date_key,
       @HeaderReportDate = CurrentYear.standard_date_name
FROM [marketing].[v_dim_date] CurrentYear  
 JOIN [marketing].[v_dim_date] StartEndMonthYear
   ON CurrentYear.year = StartEndMonthYear.year
WHERE CurrentYear.calendar_date = Cast(@StartDate as Date)
GROUP BY CurrentYear.dim_date_key,CurrentYear.standard_date_name



-- Create Product Status temp table to return selected product statuses (#StatusList) -- 
IF OBJECT_ID('tempdb.dbo.#StatusList', 'U') IS NOT NULL DROP TABLE #StatusList; 

----- Create Status temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'status_list'

EXEC marketing.proc_parse_pipe_list @ProductStatusList,@list_table
	
SELECT item
  INTO #StatusList
FROM #status_list 

----- Create Hierarchy temp table to return selected group names      
Exec [reporting].[proc_DimReportingHierarchy_History] 'All Divisions','All Subdivisions',@DepartmentMinDimReportingHierarchyKeyList,'All Product Groups',@DimDateKey,@DimDateKey

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable

IF OBJECT_ID('tempdb.dbo.#PostingMonthList', 'U') IS NOT NULL DROP TABLE #PostingMonthList; 

SELECT DimDate.[month_number_in_year] PostingMonthID,
       DimDate.[four_digit_year_dash_two_digit_month] PostingMonth, 
       DimDate.[four_digit_year_dash_two_digit_month] + ' Ratio' PostingMonthName
INTO #PostingMonthList
FROM [marketing].[v_dim_date] DimDate
WHERE DimDate.[four_digit_year_dash_two_digit_month] >= @ReportingYearStartingFourDigitYearDashTwoDigitMonth
  AND DimDate.[four_digit_year_dash_two_digit_month] <= @ReportingYearEndingFourDigitYearDashTwoDigitMonth
  AND DimDate.[day_number_in_month] = 1

DECLARE @PostingMonth1RatioName VARCHAR(100),
        @PostingMonth2RatioName VARCHAR(100),
        @PostingMonth3RatioName VARCHAR(100),
        @PostingMonth4RatioName VARCHAR(100),
        @PostingMonth5RatioName VARCHAR(100),
        @PostingMonth6RatioName VARCHAR(100),
        @PostingMonth7RatioName VARCHAR(100),
        @PostingMonth8RatioName VARCHAR(100),
        @PostingMonth9RatioName VARCHAR(100),
        @PostingMonth10RatioName VARCHAR(100),
        @PostingMonth11RatioName VARCHAR(100),
        @PostingMonth12RatioName VARCHAR(100)

SELECT @PostingMonth1RatioName = MAX(CASE WHEN PostingMonthID = 1 THEN PostingMonthName ELSE NULL END),
       @PostingMonth2RatioName = MAX(CASE WHEN PostingMonthID = 2 THEN PostingMonthName ELSE NULL END),
       @PostingMonth3RatioName = MAX(CASE WHEN PostingMonthID = 3 THEN PostingMonthName ELSE NULL END),
       @PostingMonth4RatioName = MAX(CASE WHEN PostingMonthID = 4 THEN PostingMonthName ELSE NULL END),
       @PostingMonth5RatioName = MAX(CASE WHEN PostingMonthID = 5 THEN PostingMonthName ELSE NULL END),
       @PostingMonth6RatioName = MAX(CASE WHEN PostingMonthID = 6 THEN PostingMonthName ELSE NULL END),
       @PostingMonth7RatioName = MAX(CASE WHEN PostingMonthID = 7 THEN PostingMonthName ELSE NULL END),
       @PostingMonth8RatioName = MAX(CASE WHEN PostingMonthID = 8 THEN PostingMonthName ELSE NULL END),
       @PostingMonth9RatioName = MAX(CASE WHEN PostingMonthID = 9 THEN PostingMonthName ELSE NULL END),
       @PostingMonth10RatioName = MAX(CASE WHEN PostingMonthID = 10 THEN PostingMonthName ELSE NULL END),
       @PostingMonth11RatioName = MAX(CASE WHEN PostingMonthID = 11 THEN PostingMonthName ELSE NULL END),
       @PostingMonth12RatioName = MAX(CASE WHEN PostingMonthID = 12 THEN PostingMonthName ELSE NULL END)       
FROM #PostingMonthList


DECLARE @PriorYear INT,
        @CurrentYear INT,
        @NextYear INT

SELECT @CurrentYear = DimDate.year
 FROM [marketing].[v_dim_date]  DimDate
WHERE DimDate.[calendar_date] = CAST(@StartDate as datetime)

DECLARE         
        @CurrentYearEarliestTransactionName VARCHAR(75),
        @CurrentYearLatestTransactionName VARCHAR(75),
        @CurrentYearLateTransactionName  VARCHAR(75)

SET @CurrentYearEarliestTransactionName = CONVERT(VARCHAR,@CurrentYear) + ' Revenue Allocation Earliest Transaction Month'
SET @CurrentYearLatestTransactionName = CONVERT(VARCHAR,@CurrentYear) + ' Revenue Allocation Latest Transaction Month'
SET @CurrentYearLateTransactionName  = CONVERT(VARCHAR,@CurrentYear) + ' Revenue Allocation Late Transaction Month'

-- For Magento, no Allocation Rules for magento products in v_dim_magento_product

SELECT Dimproduct.dim_mms_product_key DimProductKey, 
       DimProduct.allocation_rule RevenueAllocationRuleName,
       DimRevenueAllocationRule.dim_club_key DimLocationKey,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 1 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth1Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 2 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth2Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 3 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth3Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 4 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth4Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 5 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth5Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 6 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth6Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 7 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth7Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 8 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth8Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 9 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth9Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 10 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth10Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 11 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth11Ratio,
       SUM(CASE WHEN #PostingMonthList.PostingMonthID = 12 THEN DimRevenueAllocationRule.Ratio ELSE 0 END) AS PostingMonth12Ratio       
  INTO #RevenueAllocationRatio
  FROM [marketing].[v_dim_mms_product] DimProduct
  JOIN [marketing].[v_dim_revenue_allocation_rule] DimRevenueAllocationRule
    ON DimProduct.[allocation_rule] = DimRevenueAllocationRule.[revenue_allocation_rule_name]
  JOIN #StatusList
    ON DimProduct.[product_status] = #StatusList.item
  JOIN #DimReportingHierarchy
    ON DimProduct.[dim_reporting_hierarchy_key] = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #PostingMonthList
    ON DimRevenueAllocationRule.[revenue_posting_month_four_digit_year_dash_two_digit_month] = #PostingMonthList.PostingMonth
 WHERE DimRevenueAllocationRule.[revenue_posting_month_four_digit_year_dash_two_digit_month] >= @ReportingYearStartingFourDigitYearDashTwoDigitMonth
   AND DimRevenueAllocationRule.[revenue_posting_month_four_digit_year_dash_two_digit_month] <= @ReportingYearEndingFourDigitYearDashTwoDigitMonth
 GROUP BY Dimproduct.dim_mms_product_key, DimProduct.allocation_rule, DimRevenueAllocationRule.dim_club_key

-- For Magento, no Allocation Rules for magento products in v_dim_magento_product

SELECT Dimproduct.dim_mms_product_key, DimProduct.allocation_rule, DimRevenueAllocationRule.dim_club_key,
       Max(DimRevenueAllocationRule.[one_off_rule_flag]) OneOffRuleFlag, 
	   MIN(DimRevenueAllocationRule.[earliest_transaction_dim_date_key]) RuleStartDimDateKey,
       MIN(CASE WHEN DimRevenueAllocationRule.[revenue_from_late_transaction_flag] = 'N'     
                 AND CONVERT(INT,RIGHT(DimRevenueAllocationRule.[revenue_allocation_rule_set],4))=@CurrentYear
                     THEN DimRevenueAllocationRule.[earliest_transaction_dim_date_key]
                ELSE 99981231 END) CurrentYearEarliestTranDimDateKey,
       MAX(CASE WHEN DimRevenueAllocationRule.[revenue_from_late_transaction_flag] = 'N' 
                 AND CONVERT(INT,RIGHT(DimRevenueAllocationRule.[revenue_allocation_rule_set],4))=@CurrentYear
                     THEN DimRevenueAllocationRule.[latest_transaction_dim_date_key]
                ELSE 19000101 END) CurrentYearLatestTranDimDateKey,
       MAX(CASE WHEN DimRevenueAllocationRule.[revenue_from_late_transaction_flag] = 'Y' 
                 AND CONVERT(INT,RIGHT(DimRevenueAllocationRule.[revenue_allocation_rule_set],4))=@CurrentYear
                     THEN DimRevenueAllocationRule.[latest_transaction_dim_date_key]
                ELSE 19000101 END) CurrentYearLateTranDimDateKey
  INTO #RevenueAllocationDate
  FROM [marketing].[v_dim_mms_product] DimProduct
  JOIN [marketing].[v_dim_revenue_allocation_rule]  DimRevenueAllocationRule
    ON DimProduct.[allocation_rule] = DimRevenueAllocationRule.[revenue_allocation_rule_name]
  JOIN [marketing].[v_dim_club] DImLocation
    ON DimRevenueAllocationRule.dim_club_key = DimLocation.dim_club_key
  JOIN #StatusList
    ON DimProduct.[product_status] = #StatusList.item
  JOIN #DimReportingHierarchy
    ON DimProduct.[dim_reporting_hierarchy_key] = #DimReportingHierarchy.DimReportingHierarchyKey
WHERE CONVERT(INT,RIGHT(DimRevenueAllocationRule.[revenue_allocation_rule_set],4)) IN (@CurrentYear) --(@PriorYear,@CurrentYear,@NextYear)
GROUP BY Dimproduct.dim_mms_product_key, DimProduct.allocation_rule, DimRevenueAllocationRule.dim_club_key


--Result set
SELECT DimProduct.[department_description] MMSDepartment,
       DimReportingHierarchy.[reporting_department] ReportingDepartment,
       CASE WHEN (DimLocation.dim_club_key in ('-997','-998','-999') OR DimLocation.club_id = -1) THEN NULL ELSE DimLocation.[club_id] END MMSClubID,
       CASE WHEN (DimLocation.dim_club_key in ('-997','-998','-999') OR DimLocation.club_id = -1) THEN NULL ELSE DimLocation.[club_code] END ClubCode,
       CASE WHEN (DimLocation.dim_club_key in ('-997','-998','-999') OR DimLocation.club_id = -1) THEN NULL ELSE DimLocation.[workday_region] END WorkdayRegion,
       CASE WHEN (DimLocation.dim_club_key in ('-997','-998','-999') OR DimLocation.club_id = -1) THEN NULL ELSE DimLocation.[club_name] END ClubName,
       Dimproduct.[reporting_product_group] RevenueProductGroupDescription,
       DimProduct.[allocation_rule] RevenueAllocationRule,
       #RevenueAllocationDate.OneOffRuleFlag,
       CONVERT(VARCHAR(10), RuleStartDimDate.[calendar_date], 120) RuleStartDate,
       DimProduct.[product_id] ProductID,
       DimProduct.[product_description] ProductDescription,
       DimProduct.[product_status] ProductStatus,
       DimProduct.[reporting_product_group_gl_account] RevenueProductGroupGLAccount,
       DimProduct.[revenue_product_group_refund_gl_account] RevenueProductGroupRefundGLAccount,
       DimProduct.[revenue_product_group_discount_gl_account] RevenueProductGroupDiscountGLAccount,
       @PostingMonth1RatioName PostingMonth1RatioName,
       #RevenueAllocationRatio.PostingMonth1Ratio,
       @PostingMonth2RatioName PostingMonth2RatioName,
       #RevenueAllocationRatio.PostingMonth2Ratio,
       @PostingMonth3RatioName PostingMonth3RatioName,
       #RevenueAllocationRatio.PostingMonth3Ratio,
       @PostingMonth4RatioName PostingMonth4RatioName,
       #RevenueAllocationRatio.PostingMonth4Ratio,
       @PostingMonth5RatioName PostingMonth5RatioName,
       #RevenueAllocationRatio.PostingMonth5Ratio,
       @PostingMonth6RatioName PostingMonth6RatioName,
       #RevenueAllocationRatio.PostingMonth6Ratio,
       @PostingMonth7RatioName PostingMonth7RatioName,
       #RevenueAllocationRatio.PostingMonth7Ratio,
       @PostingMonth8RatioName PostingMonth8RatioName,
       #RevenueAllocationRatio.PostingMonth8Ratio,
       @PostingMonth9RatioName PostingMonth9RatioName,
       #RevenueAllocationRatio.PostingMonth9Ratio,
       @PostingMonth10RatioName PostingMonth10RatioName,
       #RevenueAllocationRatio.PostingMonth10Ratio,
       @PostingMonth11RatioName PostingMonth11RatioName,
       #RevenueAllocationRatio.PostingMonth11Ratio,
       @PostingMonth12RatioName PostingMonth12RatioName,
       #RevenueAllocationRatio.PostingMonth12Ratio,       
       DimProduct.[gl_account_number] ProductGLAccount,
       DimProduct.[gl_department_code] ProductGLDepartmentCode,
       DimProduct.[gl_product_code] ProductGLProductCode,
       DimProduct.[refund_gl_account_number] ProductRefundGLAccount,
       DimProduct.[discount_gl_account] ProductDiscountGLAccount,
       @CurrentYearEarliestTransactionName CurrentYearEarliestTransactionName,
       CASE WHEN CurrentYearEarliestTranDimDate.dim_date_key = 99991231 THEN NULL
            ELSE CurrentYearEarliestTranDimDate.[four_digit_year_dash_two_digit_month] 
       END CurrentYearRevenueAllocationPostingEarliestMonth,
       @CurrentYearLatestTransactionName CurrentYearLatestTransactionName,
       CASE WHEN CurrentYearLatestTranDimDate.dim_date_key = 19000101  THEN NULL
            ELSE CurrentYearLatestTranDimDate.[four_digit_year_dash_two_digit_month] 
       END CurrentYearRevenueAllocationPostingLatestMonth,
       @CurrentYearLateTransactionName CurrentYearLateTransactionName ,
       CASE WHEN CurrentYearLateTranDimDate.dim_date_key = 19000101  THEN NULL
            ELSE CurrentYearLateTranDimDate.[four_digit_year_dash_two_digit_month] 
       END CurrentYearRevenueAllocationPostingLateTransactionMonth,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderReportDate HeaderReportDate,
       DimReportingHierarchy.[reporting_division] Division,
       DimReportingHierarchy.[reporting_sub_division] Subdivision,
       NULL AS HeaderDivisionList, ---@HeaderDivisionList HeaderDivisionList,             ----- to be created within Cognos using parameter values - need to retain placeholder
       NULL AS HeaderSubdivisionList, ---@HeaderSubdivisionList HeaderSubdivisionList,    ----- to be created within Cognos using parameter values - need to retain placeholder
       NULL AS HeaderDepartmentList, ---@HeaderDepartmentList HeaderDepartmentList,       ----- to be created within Cognos using parameter values - need to retain placeholder
       REPLACE(@ProductStatusList,'|',', ') HeaderProductStatusList,
       DimProduct.[workday_revenue_product_group_account] WorkdayRevenueProductGroupAccount,
       DimProduct.[deferred_revenue_flag] DeferredRevenueFlag,
       DimProduct.[workday_revenue_product_group_refund_gl_account] WorkdayRevenueProductGroupRefundGLAccount,
       DimProduct.[workday_revenue_product_group_discount_gl_account] WorkdayRevenueProductGroupDiscountGLAccount,
       DimProduct.[workday_account] WorkdayAccount,
       DimProduct.[workday_cost_center] WorkdayCostCenter,
       DimProduct.[workday_offering] WorkdayOffering,
       DimProduct.[workday_refund_gl_account] WorkdayRefundGLAccount,
       DimProduct.[workday_discount_gl_account] WorkdayDiscountGLAccount
  FROM [marketing].[v_dim_mms_product]  DimProduct
  JOIN [marketing].[v_dim_reporting_hierarchy] DimReportingHierarchy
    ON DimReportingHierarchy.[dim_reporting_hierarchy_key] = DimProduct.[dim_reporting_hierarchy_key]
  JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.[dim_reporting_hierarchy_key]
  JOIN #RevenueAllocationRatio
    ON #RevenueAllocationRatio.DimProductKey = DimProduct.[dim_mms_product_key]
  JOIN #RevenueAllocationDate
    ON #RevenueAllocationDate.dim_mms_product_key = #RevenueAllocationRatio.DimProductKey
       AND #RevenueAllocationDate.allocation_rule = #RevenueAllocationRatio.RevenueAllocationRuleName
       AND #RevenueAllocationDate.[dim_club_key] = #RevenueAllocationRatio.DimLocationKey 
  JOIN [marketing].[v_dim_club] DimLocation
    ON #RevenueAllocationRatio.DimLocationKey = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_date] CurrentYearEarliestTranDimDate
    ON #RevenueAllocationDate.CurrentYearEarliestTranDimDateKey = CurrentYearEarliestTranDimDate.[dim_date_key]
  JOIN [marketing].[v_dim_date] CurrentYearLatestTranDimDate
    ON #RevenueAllocationDate.CurrentYearLatestTranDimDateKey = CurrentYearLatestTranDimDate.[dim_date_key]
  JOIN [marketing].[v_dim_date] CurrentYearLateTranDimDate
    ON #RevenueAllocationDate.CurrentYearLateTranDimDateKey = CurrentYearLateTranDimDate.[dim_date_key]
  JOIN [marketing].[v_dim_date] RuleStartDimDate
    ON #RevenueAllocationDate.RuleStartDimDateKey = RuleStartDimDate.[dim_date_key]
 ORDER BY DimReportingHierarchy.[reporting_department] 
 ,DimLocation.[club_name] 
 ,DimProduct.[product_description] 

DROP TABLE #StatusList
DROP TABLE #DimReportingHierarchy
DROP TABLE #PostingMonthList
DROP TABLE #RevenueAllocationDate
DROP TABLE #RevenueAllocationRatio


END
