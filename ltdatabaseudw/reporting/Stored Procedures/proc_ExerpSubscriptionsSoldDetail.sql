CREATE PROC [reporting].[proc_ExerpSubscriptionsSoldDetail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](8000),@MMSClubIDList [VARCHAR](8000),@DivisionName [VARCHAR](255) AS 
BEGIN 
SET XACT_ABORT ON 
SET NOCOUNT ON 

IF 1=0 BEGIN 
       SET FMTONLY OFF 
     END 
--DECLARE @StartDate [DATETIME] = '2/1/2020' 
--DECLARE @EndDate [DATETIME] = '2/29/2020' 
--DECLARE @RegionList [VARCHAR](8000) = 'All Regions' 
--DECLARE @MMSClubIDList [VARCHAR](8000) = '238' 
--DECLARE @DivisionName [VARCHAR](255) = 'Personal Training' 

SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0) ELSE @StartDate END 
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) ELSE @EndDate END 
DECLARE @StartReportDateDimDateKey INT 
DECLARE @EndReportDateDimDateKey INT 
SET @StartReportDateDimDateKey = (Select [dim_date_key] from [marketing].[v_dim_date] where [calendar_date] = @StartDate) 
SET @EndReportDateDimDateKey = (Select [dim_date_key] from [marketing].[v_dim_date] where [calendar_date] = @EndDate) 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time 
DECLARE @BeginDimDateKey INT, 
        @EndDimDateKey INT, 
        @EndMonthEndingDate DATETIME, 
        @HeaderDateRange VARCHAR(33) 
SELECT @BeginDimDateKey = StartDimDate.[dim_date_key], 
       @EndDimDateKey = EndDimDate.[dim_date_key], 
       @EndMonthEndingDate = EndDimDate.[month_ending_date], 
       @HeaderDateRange = StartDimDate.[standard_date_name] + ' through ' + EndDimDate.[standard_date_name] 
  FROM [marketing].[v_dim_date] StartDimDate 
 CROSS JOIN [marketing].[v_dim_date] EndDimDate 
 WHERE StartDimDate.[calendar_date] = @StartDate 
   AND EndDimDate.[calendar_date] = @EndDate 

IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #Clubs; 
-- Create club temp table 
DECLARE @list_table VARCHAR(100) 
SET @list_table = 'club_list' 
EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table 
SELECT DimClub.dim_club_key AS DimClubKey, 
  DimClub.club_name AS ClubName, 
      MMSRegion.description AS MMSRegionName, 
  DimClub.local_currency_code 
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub 
  JOIN #club_list ClubKeyList 
    ON ClubKeyList.Item = DimClub.club_id 
  OR ClubKeyList.Item = -1 
 JOIN [marketing].[v_dim_description]  MMSRegion 
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
WHERE DimClub.club_id Not In (-1,99,100) 
  AND DimClub.club_id < 900 
  AND DimClub.club_type = 'Club' 
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @BeginDimDateKey)   
GROUP BY DimClub.dim_club_key, DimClub.club_name, MMSRegion.description,DimClub.local_currency_code 
IF OBJECT_ID('tempdb.dbo.#DimLocationKeyList', 'U') IS NOT NULL DROP TABLE #DimLocationKeyList; 
-- Create region temp table 
SET @list_table = 'region_list' 
EXEC marketing.proc_parse_pipe_list @RegionList,@list_table 
SELECT Clubs.DimClubKey, 
  Clubs.ClubName, 
  WDRegionClub.[workday_region] WorkdayRegion, 
      Clubs.MMSRegionName, 
  Clubs.local_currency_code 
  INTO #DimLocationKeyList 
  FROM #Clubs Clubs 
  JOIN #region_list RegionList 
        ON Clubs.MMSRegionName = RegionList.Item 
    OR @RegionList like '%All Regions%' 
  JOIN [marketing].[v_dim_club] WDRegionClub 
        ON Clubs.DimClubKey = WDRegionClub.[dim_club_key] 
SELECT DISTINCT DimProduct.[dim_mms_product_key] DimProductKey 
  , DimProduct.[dim_reporting_hierarchy_key] DimReportingHierarchyKey 
  , DimProduct.[workday_cost_center] WorkdayCostCenter 
  , DimProduct.[workday_offering] WorkdayOffering 
  , DimProduct.[product_description] ProductDescription 
  , DimProduct.[department_description] MMSDepartmentDescription 
  , DimProduct.[payroll_standard_group_description] StandardProductCommissionCode 
       , DimProduct.[payroll_lt_bucks_group_description] LTBucksProductCommissionCode 
  , DimProduct.[reporting_division] DivisionName 
  , DimProduct.[reporting_sub_division] SubdivisionName 
  , DimProduct.[reporting_department] DepartmentName 
  , DimProduct.[lt_buck_cost_percent] LTBuckCostPercent 
  , DimExerpProduct.dim_exerp_product_key 
INTO #DimProduct 
  FROM [marketing].[v_dim_mms_product] DimProduct   
  JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy 
    ON DimProduct.[dim_reporting_hierarchy_key] = DimReportingHierarchy.[dim_reporting_hierarchy_key] 
  LEFT JOIN [marketing].[v_dim_exerp_product] DimExerpProduct     
    ON DimProduct.dim_mms_product_key = DimExerpProduct.dim_mms_product_key 
 WHERE DimProduct.[reporting_division] = @DivisionName 
 
SELECT Subscriptions.subscription_period_id, 
       DimLocation.MMSRegionName, 
       DimLocation.ClubName, 
  MMSTranItem.post_dim_date_key, 
  DimLocation.local_currency_code, 
  Subscriptions.price_per_booking, 
  Subscriptions.number_of_bookings, 
  #DimProduct.MMSDepartmentDescription, 
  SalesChannelDimDescription.description, 
  #DimProduct.WorkdayCostCenter, 
  #DimProduct.WorkdayOffering, 
  #DimProduct.ProductDescription, 
  #DimProduct.DivisionName Division, 
  #DimProduct.SubdivisionName Subdivision, 
  #DimProduct.DepartmentName ReportingDepartment, 
  DimLocation.WorkdayRegion, 
  PrimarySalesDimEmployee.employee_name, 
  #DimProduct.StandardProductCommissionCode, 
  #DimProduct.LTBucksProductCommissionCode, 
  DimMMSMember.member_id, 
  DimMMSMember.customer_name, 
  @HeaderDateRange HeaderDateRange, 
  @ReportRunDateTime AS ReportRunDateTime, 
  IsNull(#DimProduct.LTBuckCostPercent,0) *.01 AS ProductLTBucksCostPercent, 
  IsNull(Subscriptions.lt_bucks_amount,0) AS LTBucksPaymentForPackage, 
  Subscriptions.lt_bucks_amount 
FROM (Select SubscriptionPeriod.dim_exerp_subscription_period_key, 
       SubscriptionPeriod.dim_club_key, 
  SubscriptionPeriod.dim_exerp_product_key, 
       SubscriptionPeriod.fact_exerp_transaction_log_key, 
  SubscriptionPeriod.from_dim_date_key, 
       SubscriptionPeriod.lt_bucks_amount, 
  SubscriptionPeriod.net_amount, 
  SubscriptionPeriod.number_of_bookings, 
  SubscriptionPeriod.price_per_booking, 
  SubscriptionPeriod.price_per_booking_less_lt_bucks, 
  SubscriptionPeriod.subscription_period_id, 
  SubscriptionPeriod.to_dim_date_key, 
  TranLog.external_id, 
  TranLog.entry_dim_date_key 
   from [marketing].[v_dim_exerp_subscription_period] SubscriptionPeriod 
        JOIN [marketing].[v_fact_exerp_transaction_log] TranLog 
     ON TranLog.fact_exerp_transaction_log_key = SubscriptionPeriod.fact_exerp_transaction_log_key) Subscriptions 
JOIN #DimLocationKeyList DimLocation 
      On DimLocation.DimClubKey = Subscriptions.dim_club_key 
JOIN #DimProduct 
      ON Subscriptions.dim_exerp_product_key = #DimProduct.dim_exerp_product_key 
LEFT JOIN [marketing].[v_fact_mms_transaction_item] MMSTranItem 
  ON Subscriptions.external_id = MMSTranItem.external_item_id 
  AND MMSTranItem.transaction_source = 'Exerp' 
LEFT JOIN [marketing].[v_dim_description] SalesChannelDimDescription 
      ON MMSTranItem.sales_channel_dim_description_key = SalesChannelDimDescription.dim_description_key 
LEFT JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee 
      ON MMSTranItem.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key 
LEFT JOIN [marketing].[v_dim_mms_member] DimMMSMember 
      ON MMSTranItem.dim_mms_member_key = DimMMSMember.dim_mms_member_key 
WHERE MMSTranItem.post_dim_date_key >= @StartReportDateDimDateKey 
AND MMSTranItem.post_dim_date_key <= @EndReportDateDimDateKey 
AND MMSTranItem.voided_flag = 'N' 
Order by Subscriptions.from_dim_date_key 


DROP TABLE #Clubs 
DROP TABLE #DimLocationKeyList 
DROP TABLE #DimProduct 


END
