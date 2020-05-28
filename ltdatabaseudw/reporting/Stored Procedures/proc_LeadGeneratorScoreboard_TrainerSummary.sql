CREATE PROC [reporting].[proc_LeadGeneratorScoreboard_TrainerSummary] @MemberJoinStartDate [DATETIME],@MemberJoinEndDate [DATETIME],@Increment [INT],@AsOfDate [DATETIME],@MMSClubIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

 IF 1=0 BEGIN
       SET FMTONLY OFF
     END

 ---- Execution sample
 ---- exec proc_LeadGeneratorScoreboard_TrainerSummary '1/1/2012','1/31/2012',30,'5/1/2015','52'
 ---- exec proc_LeadGeneratorScoreboard_TrainerSummary '1/1/2012','1/31/2012',30,'5/1/2015','52'
-- declare @MemberJoinStartDate DATETIME
-- declare @MemberJoinEndDate DATETIME
-- declare @Increment INT
-- declare	@AsOfDate DATETIME
-- declare @MMSClubIDList VARCHAR(8000)
-- set @MemberJoinStartDate='1/1/2019'
-- set @MemberJoinEndDate='2/1/2019'
-- set @Increment=30
-- set @AsOfDate='5/1/2019'
-- set @MMSClubIDList='52'



IF OBJECT_ID('tempdb.dbo.#DimLocationKeyList', 'U') IS NOT NULL
  DROP TABLE #DimLocationKeyList;

IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL
  DROP TABLE #Clubs;

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #DimLocationKeyList--Clubs
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list MMSClubIDList
    ON MMSClubIDList.Item = DimClub.club_id
	  OR MMSClubIDList.Item = -1
JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_type = 'Club'



SET @MemberJoinStartDate = CASE WHEN @MemberJoinStartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
					  WHEN @MemberJoinStartDate = 'Dec 30, 1899'
					  THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
					  ELSE @MemberJoinStartDate END
SET @MemberJoinEndDate = CASE WHEN @MemberJoinEndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101)   ----- returns yesterday's date
					ELSE @MemberJoinEndDate END
SET @AsOfDate = CASE WHEN @AsOfDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @AsOfDate END

DECLARE @HeaderDateRange VARCHAR(33), @ReportRunDateTime VARCHAR(21) 
DECLARE @HeaderAsOfDate VARCHAR(33)

SET @ReportRunDateTime = Replace(Substring(convert(varchar,getdate(),100),1,6)+', '+Substring(convert(varchar,GETDATE(),100),8,10)+' '+Substring(convert(varchar,getdate(),100),18,2),'  ',' ')
SET @HeaderDateRange = convert(varchar(12), @MemberJoinStartDate, 107) + ' and ' + convert(varchar(12), @MemberJoinEndDate, 107)
SET @HeaderAsOfDate = convert(varchar(12), @AsOfDate, 107)

DECLARE @StartDimDateKey INT,
        @EndDimDateKey INT,
        @AsOfDateKey INT,
        @today datetime

SET @StartDimDateKey = (SELECT dim_date_key  FROM marketing.v_Dim_date WHERE calendar_date = @MemberJoinStartDate)--calendardate
SET @EndDimDateKey = (SELECT dim_date_key  FROM marketing.v_Dim_date WHERE calendar_date = @MemberJoinEndDate)
SET @AsOfDateKey = (SELECT dim_date_key  FROM marketing.v_Dim_date WHERE calendar_date = @AsOfDate)

SET @Increment = CONVERT(INT,replace(cast(@Increment as varchar), '.0', ''))
--  Need to explicitly remove decimal and zero and then convert to an INT before Cognos will accept this value

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
DROP TABLE #DimReportingHierarchy;

 SELECT DISTINCT DimReportingHierarchy.dim_reporting_hierarchy_key
  INTO #DimReportingHierarchy
  FROM marketing.v_Dim_Reporting_Hierarchy_history BridgeDimReportingHierarchy
  JOIN marketing.v_Dim_Reporting_Hierarchy_history DimReportingHierarchy
    ON BridgeDimReportingHierarchy.reporting_division = DimReportingHierarchy.reporting_division--divisionname
   AND BridgeDimReportingHierarchy.reporting_sub_division = DimReportingHierarchy.reporting_sub_division--subdivisionname
   AND BridgeDimReportingHierarchy.reporting_department = DimReportingHierarchy.reporting_department--DepartmentName
  JOIN (SELECT DISTINCT month_ending_dim_date_key--MonthEndingDimDateKey
          FROM marketing.v_Dim_date--vDimDate
         WHERE dim_date_key >= @StartDimDateKey
           AND dim_date_key <= @EndDimDateKey) MonthEndingDimDateKeys
    ON BridgeDimReportingHierarchy.Effective_Dim_Date_Key <= MonthEndingDimDateKeys.month_ending_dim_date_key
   AND BridgeDimReportingHierarchy.Expiration_Dim_Date_Key > MonthEndingDimDateKeys.month_ending_dim_date_key
   AND DimReportingHierarchy.Effective_Dim_Date_Key <= MonthEndingDimDateKeys.month_ending_dim_date_key
   AND DimReportingHierarchy.Expiration_Dim_Date_Key > MonthEndingDimDateKeys.month_ending_dim_date_key
 WHERE IsNull(BridgeDimReportingHierarchy.dim_reporting_hierarchy_key,'-998') not in('-997', '-998','-999')
   AND BridgeDimReportingHierarchy.reporting_division IN ('Personal Training','PT Division')
 

IF OBJECT_ID('tempdb.dbo.#Generators', 'U') IS NOT NULL
  DROP TABLE #Generators;
  
SELECT 'MyLTBucks' GeneratorDescription, 1 GeneratorSortOrder
  INTO #Generators
 UNION ALL
SELECT 'myHealthScore',2
 UNION ALL
SELECT 'FitPoint',3

IF OBJECT_ID('tempdb.dbo.#NewMembers', 'U') IS NOT NULL
DROP TABLE #NewMembers;
  
SELECT DimCustomer.dim_mms_member_key,--DimCustomerKey
       FactMembership.home_dim_club_key,----dim_club_key(DimLocationKey) 
       DimCustomer.join_date_key,
	   JoinDimDate.four_digit_year_two_digit_month_two_digit_day MemberJoinMonth,--FourDigitYearDashTwoDigitMonth MemberJoinMonth,
       CASE WHEN IncrementDimDate.Dim_Date_Key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.Dim_Date_Key
            END IncrementEndDimDateKey
  INTO #NewMembers
  FROM marketing.v_dim_mms_member DimCustomer--vDimCustomerActive=v_dim_mms_member--
  JOIN marketing.v_Dim_date JoinDimDate
    ON DimCustomer.join_date_key = JoinDimDate.dim_date_key
  JOIN marketing.v_dim_mms_membership_history FactMembership
    ON DimCustomer.membership_id = FactMembership.membership_id
	AND case when datepart(year,JoinDimDate.month_ending_date)= 9999 then 0
		 when  FactMembership.effective_date_time <= DateAdd(minute,1439,JoinDimDate.month_ending_date) --CalendarMonthEndingDate
         AND FactMembership.expiration_date_time > DateAdd(minute,1439,JoinDimDate.month_ending_date) then 1 END = 1
  
  JOIN #DimLocationKeyList MembershipDimLocation
    ON FactMembership.home_dim_club_key = MembershipDimLocation.DimClubKey
  -- JOIN marketing.v_dim_mms_product DimProduct--vDimProductActive
    -- ON FactMembership.DimProductKey = DimProduct.dim_mms_product_key-----------------------------------------FactMembership.DimProductKey ?
   -- AND ISNULL(Factmembership.SalesReportingCategoryDescription,DimProduct.MembershipTypeDSSRGroupDescription) <> 'DSSR_Other'------------?
   
     JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON MembershipType.dim_mms_membership_type_key = FactMembership.dim_mms_membership_type_key
	AND MembershipType.attribute_dssr_group_description <> 'DSSR_Other'	
  JOIN marketing.v_Dim_date IncrementDimDate
    ON case when datepart(year,IncrementDimDate.calendar_date)= 9999 then IncrementDimDate.calendar_date-1 else
  DATEADD(dd,@Increment,JoinDimDate.calendar_date) END = IncrementDimDate.calendar_date
 WHERE DimCustomer.join_date_key >= @StartDimDateKey
   AND DimCustomer.join_date_key <= @EndDimDateKey
   AND DimCustomer.description_member <> 'Junior'-- MemberTypeDescription
   AND DimCustomer.join_date_key <= @AsOfDateKey
 Group By DimCustomer.dim_mms_member_key,                     
       FactMembership.home_dim_club_key,
       DimCustomer.join_date_key,
	   JoinDimDate.four_digit_year_two_digit_month_two_digit_day,
       CASE WHEN IncrementDimDate.dim_date_key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.dim_date_key
            END 
 
DECLARE @MyLTBucksDimEmployeeKey varchar(32)--INT 	?
SET @MyLTBucksDimEmployeeKey = (SELECT dim_employee_key FROM marketing.v_dim_employee WHERE employee_id = -5)

DECLARE @AbsoluteStart INT, @AbsoluteEnd INT
SELECT @AbsoluteStart = MIN(join_date_key),
       @AbsoluteEnd = MAX(IncrementEndDimDateKey)
  FROM #NewMembers

IF OBJECT_ID('tempdb.dbo.#ServiceDetail', 'U') IS NOT NULL
  DROP TABLE #ServiceDetail;
  
SELECT #NewMembers.dim_mms_member_key,
       #NewMembers.join_date_key,
       #NewMembers.IncrementEndDimDateKey,
	   CASE WHEN DimProduct.connectivity_primary_lead_generator_flag = 'Y' THEN 'FitPoint'
	        WHEN DimProduct.connectivity_lead_generator_flag = 'Y' THEN 'myHealthScore'
            END GeneratorDescription,
       FactPackageSession.fact_mms_package_session_key,--FactPackageSessionKey
       FactPackageSession.created_dim_date_key,--CreatedDimDateKey
       CreatedDimDate.calendar_date Created_Date,
	   DimEmployee.dim_employee_key  DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.Employee_ID DeliveringTeamMemberEmployeeID,
	   DimEmployee.Employee_Name DeliveringTeamMemberName,
        CASE WHEN FourteenDayDimDate.Dim_Date_Key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE FourteenDayDimDate.Dim_Date_Key
            END FourteenDayDimDateKey,
       CASE WHEN FourteenDayDimDate.Calendar_Date > @AsOfDate
            THEN @AsOfDate
            ELSE FourteenDayDimDate.Calendar_Date
            END FourteenDayDate
  INTO #ServiceDetail
  FROM marketing.v_fact_mms_package_session FactPackageSession
  JOIN marketing.v_Dim_Date CreatedDimDate
    ON FactPackageSession.created_dim_date_key = CreatedDimDate.Dim_Date_Key
  JOIN marketing.v_Dim_Date FourteenDayDimDate
    ON DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.Calendar_Date
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.dim_mms_member_key
  JOIN marketing.v_dim_mms_product_history DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN marketing.v_dim_employee DimEmployee
    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
 WHERE FactPackageSession.voided_flag = 'N'
   AND FactPackageSession.created_dim_date_key >= @AbsoluteStart
   AND FactPackageSession.created_dim_date_key <= @AbsoluteEnd
   AND (UPPER(DimProduct.connectivity_primary_lead_generator_flag) = 'Y' OR UPPER(DimProduct.connectivity_lead_generator_flag) = 'Y') 
 
 
INSERT INTO #ServiceDetail
SELECT #NewMembers.dim_mms_member_key,
       #NewMembers.join_date_key,
       #NewMembers.IncrementEndDimDateKey,
       'myLTBucks' GeneratorDescription,
       FactPackageSession.fact_mms_package_session_key,
       FactPackageSession.created_dim_date_key,
       CreatedDimDate.Calendar_Date Created_Date,
	   DimEmployee.dim_employee_key  DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.Employee_ID DeliveringTeamMemberEmployeeID,
	   DimEmployee.Employee_Name DeliveringTeamMemberName,
        CASE WHEN FourteenDayDimDate.Dim_Date_Key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE FourteenDayDimDate.Dim_Date_Key
            END FourteenDayDimDateKey,
       CASE WHEN FourteenDayDimDate.Calendar_Date > @AsOfDate
            THEN @AsOfDate
            ELSE FourteenDayDimDate.Calendar_Date
            END FourteenDayDate
  FROM marketing.v_fact_mms_package_session FactPackageSession
  JOIN marketing.v_Dim_Date CreatedDimDate
    ON FactPackageSession.created_dim_date_key = CreatedDimDate.Dim_Date_Key
  JOIN marketing.v_Dim_Date FourteenDayDimDate
    ON DATEADD(dd,14,CreatedDimDate.Calendar_Date) = FourteenDayDimDate.Calendar_Date
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.dim_mms_member_key---DimCustomerKey
  JOIN marketing.v_dim_mms_product_history DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN marketing.v_dim_employee DimEmployee
    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
 WHERE FactPackageSession.voided_flag = 'N'
   AND FactPackageSession.created_dim_date_key >= @AbsoluteStart
   AND FactPackageSession.created_dim_date_key <= @AbsoluteEnd
   AND FactPackageSession.package_entered_dim_employee_key = @MyLTBucksDimEmployeeKey
   AND UPPER(DimProduct.connectivity_primary_lead_generator_flag) = 'N'
   AND UPPER(DimProduct.connectivity_lead_generator_flag) = 'N'

DELETE 
  FROM #ServiceDetail 
 WHERE created_dim_date_key NOT BETWEEN join_date_key AND IncrementEndDimDateKey


IF OBJECT_ID('tempdb.dbo.#ServiceConnection', 'U') IS NOT NULL
  DROP TABLE #ServiceConnection;
  
--The ranking is necessary later on when the valid #SalesConnections start getting assigned to #Service connections for the case where a 
--Generator has more than one session on the same day.
SELECT dim_mms_member_key,
       join_date_key,
       GeneratorDescription,
       created_dim_date_key,
       Created_Date,
	   DeliveringTeamMemberDimEmployeeKey,
	   DeliveringTeamMemberEmployeeID,
	   DeliveringTeamMemberName,
       FourteenDayDimDateKey,
       FourteenDayDate,
       RANK() OVER(PARTITION BY dim_mms_member_key, GeneratorDescription, created_dim_date_key
                       ORDER BY fact_mms_package_session_key) SessionRanking
  INTO #ServiceConnection
  FROM #ServiceDetail


----The following code looks for the members who had a Lead Gen service, but another member 
 ---- made a "14 day sale" purchase on their behalf -  a package product for them to use.
 ---- In the past, since the Lead Gen service member did not make the purchase, they were not recognized with a "14 day sale" 
 ----  

 ------- Find all the packages that have been created since the beginning of the selected period through the "As of Date"
 ------- for members who have had Lead Gen Sessions

IF OBJECT_ID('tempdb.dbo.#PurchasedPackagesForSessionMembers', 'U') IS NOT NULL
  DROP TABLE #PurchasedPackagesForSessionMembers;
  

Select FactPackage.dim_mms_member_key,
       FactPackage.mms_tran_id,
	   FactPackage.tran_item_id,
	   FactPackage.created_dim_date_key
INTO #PurchasedPackagesForSessionMembers
   FROM marketing.v_fact_mms_package FactPackage
	   JOIN #ServiceConnection ServiceConnection
		  ON FactPackage.dim_mms_member_key = ServiceConnection.dim_mms_member_key
   WHERE FactPackage.created_dim_date_key >= @StartDimDateKey
	   AND FactPackage.created_dim_date_key <= @AsOfDateKey
	   AND FactPackage.transaction_void_flag = 'N'
	   AND FactPackage.price_per_session <> 0
   GROUP BY FactPackage.dim_mms_member_key,
       FactPackage.mms_tran_id,
	   FactPackage.tran_item_id,
	   FactPackage.created_dim_date_key


------- Which of these packages were not purchased by the user member
IF OBJECT_ID('tempdb.dbo.#PackagesNotPurchasedBySessionMembers', 'U') IS NOT NULL
  DROP TABLE #PackagesNotPurchasedBySessionMembers;
  
Select SessionMembers.dim_mms_member_key PackageDimCustomerKey,
       FactSalesTransaction.dim_mms_member_key PurchaserDimCustomerKey,
	   FactSalesTransaction.tran_item_id,
	   FactSalesTransaction.mms_tran_id
	 INTO #PackagesNotPurchasedBySessionMembers   
FROM #PurchasedPackagesForSessionMembers SessionMembers
 JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction 
    ON SessionMembers.mms_tran_id = FactSalesTransaction.mms_tran_id
	AND SessionMembers.tran_item_id = FactSalesTransaction.tran_item_id
 WHERE SessionMembers.dim_mms_member_key <> FactSalesTransaction.dim_mms_member_key


----  Create a temp table for just these exceptions - Lead Gen Sessions followed by package purchases, where the package was purchased by another, to be used by the Lead Gen Session member.
----
----  To avoid duplication, we will filter these transactions off of the 2nd unioned query

IF OBJECT_ID('tempdb.dbo.#SalesDetail', 'U') IS NOT NULL  
DROP TABLE #SalesDetail

----- only those sales where the sold package was not purchased by the service user
SELECT #ServiceConnection.dim_mms_member_key,
       #ServiceConnection.GeneratorDescription,
       #ServiceConnection.Created_Date,
       #ServiceConnection.created_dim_date_key,
       FactSalesTransaction.post_dim_date_key,
       FactSalesTransaction.post_dim_time_key,
       FactSalesTransaction.mms_tran_id ,
	   FactSalesTransaction.primary_sales_dim_employee_key,
       PostDimDate.calendar_date as PostDate,
       DATEDIFF(DD,#ServiceConnection.Created_Date,PostDimDate.calendar_date) DaysDiff,
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) RefundDaysDiff,
	   FactSalesTransaction.sales_dollar_amount,
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount
  INTO #SalesDetail     
  FROM #ServiceConnection
  JOIN #PackagesNotPurchasedBySessionMembers                     
    ON #ServiceConnection.dim_mms_member_key = #PackagesNotPurchasedBySessionMembers.PackageDimCustomerKey
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction ------------                            
    ON #PackagesNotPurchasedBySessionMembers.tran_item_id = FactSalesTransaction.tran_item_id   ------- primary filtering join for this query
  JOIN marketing.v_Dim_Date PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
  JOIN marketing.v_dim_mms_product_history DimProduct
    ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > PostDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key   ----- being sure to join on the same transaction Item level
  LEFT JOIN [marketing].[v_dim_date]  RefundPostDimDate
    ON FactSalesTransactionAutomatedRefund.refund_post_dim_date_key = RefundPostDimDate.dim_date_key
   AND RefundPostDimDate.calendar_date <= DATEADD(dd,30,PostDimDate.calendar_date)
 WHERE FactSalesTransaction.post_dim_date_key >= #ServiceConnection.created_dim_date_key        ------- nothing purchased outside the 14 day window
   AND FactSalesTransaction.post_dim_date_key <= #ServiceConnection.FourteenDayDimDateKey
   AND FactSalesTransaction.voided_flag = 'N'
   AND FactSalesTransaction.transaction_edited_flag = 'N'
   AND FactSalesTransaction.reversal_flag = 'N'
   AND FactSalesTransaction.refund_flag = 'N'
   AND FactSalesTransaction.sales_dollar_amount > 0
   AND (FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key IS NULL
        OR RefundPostDimDate.dim_date_key IS NOT NULL)
 GROUP BY #ServiceConnection.dim_mms_member_key,
          #ServiceConnection.GeneratorDescription,
          #ServiceConnection.Created_Date,
          #ServiceConnection.created_dim_date_key,
          FactSalesTransaction.fact_mms_sales_transaction_item_key,    ------ note this is at the lowest item level
          FactSalesTransaction.post_dim_date_key,
          FactSalesTransaction.post_dim_time_key,
          FactSalesTransaction.mms_tran_id,
		  FactSalesTransaction.primary_sales_dim_employee_key,
          PostDimDate.calendar_date,
          FactSalesTransaction.sales_dollar_amount,
          DATEDIFF(DD,#ServiceConnection.Created_Date,PostDimDate.Calendar_Date),
		  DATEDIFF(DD,PostDimDate.Calendar_Date,IsNull(RefundPostDimDate.Calendar_Date,PostDimDate.Calendar_Date))

UNION ALL


SELECT #ServiceConnection.dim_mms_member_key,
       #ServiceConnection.GeneratorDescription,
       #ServiceConnection.Created_Date,
       #ServiceConnection.created_dim_date_key,
       FactSalesTransaction.post_dim_date_key,
       FactSalesTransaction.post_dim_time_key,
       FactSalesTransaction.mms_tran_id,
	   FactSalesTransaction.primary_sales_dim_employee_key,
       PostDimDate.calendar_date PostDate,
       DATEDIFF(DD,#ServiceConnection.Created_Date,PostDimDate.calendar_date) DaysDiff,
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) RefundDaysDiff,
	   FactSalesTransaction.sales_dollar_amount,
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount
  FROM #ServiceConnection
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
    ON #ServiceConnection.dim_mms_member_key = FactSalesTransaction.dim_mms_member_key
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.Month_Ending_Date
   AND DimProduct.expiration_date_time > PostDimDate.Month_Ending_Date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
  --JOIN [marketing].[v_dim_plan_exchange_rate] USDDimPlanExchangeRate											------ ignore this - we are not doing any rate conversions
  --  ON FactSalesTransaction.usd_dim_plan_exchange_rate_key = USDDimPlanExchangeRate.dim_plan_exchange_rate_key 
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key   ----- being sure to join on the same transaction Item level
  LEFT JOIN [marketing].[v_dim_date] RefundPostDimDate
    ON FactSalesTransactionAutomatedRefund.refund_post_dim_date_key = RefundPostDimDate.dim_date_key
   AND RefundPostDimDate.calendar_date <= DATEADD(dd,30,PostDimDate.calendar_date)   ----- This finds refunds happening within the 1st 30 days
  LEFT JOIN #PackagesNotPurchasedBySessionMembers
    ON FactSalesTransaction.tran_item_id = #PackagesNotPurchasedBySessionMembers.tran_item_id
 WHERE FactSalesTransaction.post_dim_date_key >= #ServiceConnection.created_dim_date_key
   AND FactSalesTransaction.post_dim_date_key <= #ServiceConnection.FourteenDayDimDateKey
   AND FactSalesTransaction.voided_flag = 'N'
   AND FactSalesTransaction.transaction_edited_flag = 'N'
   AND FactSalesTransaction.reversal_flag = 'N'
   AND FactSalesTransaction.refund_flag = 'N'
   AND FactSalesTransaction.sales_dollar_amount > 0
   AND IsNull(#PackagesNotPurchasedBySessionMembers.mms_tran_id,0) = 0    ------ prevents the counting of a transaction in both queries in this union
   AND (FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key IS NULL
        OR RefundPostDimDate.dim_date_key IS NOT NULL)      -------- faulty logic --- this is meant to ignore refunds happening after 30 days, but it is not --- see mms_tran_id 397069039
 GROUP BY #ServiceConnection.dim_mms_member_key,
          #ServiceConnection.GeneratorDescription,
          #ServiceConnection.Created_Date,
          #ServiceConnection.created_dim_date_key,
          FactSalesTransaction.fact_mms_sales_transaction_item_key,
          FactSalesTransaction.post_dim_date_key,
          FactSalesTransaction.post_dim_time_key,
          FactSalesTransaction.mms_tran_id,
		  FactSalesTransaction.primary_sales_dim_employee_key,
          PostDimDate.calendar_date,
          FactSalesTransaction.sales_dollar_amount,
          DATEDIFF(DD,#ServiceConnection.Created_Date,PostDimDate.calendar_date),
		  DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date))

IF OBJECT_ID('tempdb.dbo.#SalesSummary', 'U') IS NOT NULL  
DROP TABLE #SalesSummary

--Sum to transaction level
SELECT dim_mms_member_key,
       GeneratorDescription,
       Created_Date,
       created_dim_date_key,
       post_dim_date_key,
       post_dim_time_key,
       mms_tran_id,
	   primary_sales_dim_employee_key,
       PostDate,
       DaysDiff,
       SUM(CASE WHEN RefundDaysDiff <= 30
	            THEN sales_dollar_amount + RelatedRefundDollarAmount     ------ Change from subtract to add because refunds are stored as negative amounts in UDW
				ELSE sales_dollar_amount
				END) TotalAmount 
  INTO #SalesSummary  
  FROM #SalesDetail
 -----WHERE TotalAmount > 0
 GROUP BY dim_mms_member_key,
          GeneratorDescription,
          Created_Date,
          created_dim_date_key,
          post_dim_date_key,
          post_dim_time_key,
          mms_tran_id,
		  primary_sales_dim_employee_key,
          PostDate,
          DaysDiff

IF OBJECT_ID('tempdb.dbo.#TransactionRanking', 'U') IS NOT NULL  
DROP TABLE #TransactionRanking
--Assign a sale to at most one lead generator service (unless there are more than one service in a day)
--RANK: A transaction will only ever be associated with one generator.  Rank 1 picks the correct one based on the GeneratorSortOrder (See #GenerationRanking WHERE)
SELECT SalesSummary1.dim_mms_member_key,
       SalesSummary1.GeneratorDescription,
       SalesSummary1.Created_Date,
       SalesSummary1.created_dim_date_key,
       SalesSummary1.post_dim_date_key,
       SalesSummary1.post_dim_time_key,
       SalesSummary1.mms_tran_id,
	   SalesSummary1.primary_sales_dim_employee_key,
       SalesSummary1.PostDate,
       SalesSummary1.DaysDiff,
       SalesSummary1.TotalAmount,
       RANK() OVER (PARTITION BY dim_mms_member_key, SalesSummary1.mms_tran_id
                        ORDER BY GeneratorSortOrder) TransactionRank
  INTO #TransactionRanking   
  FROM #SalesSummary SalesSummary1
  JOIN (SELECT mms_tran_id, MIN(DaysDiff) DaysDiff FROM #SalesSummary GROUP BY mms_tran_id) MinTran
    ON SalesSummary1.mms_tran_id = MinTran.mms_tran_id
   AND SalesSummary1.DaysDiff = MinTran.DaysDiff
  JOIN #Generators
    ON SalesSummary1.GeneratorDescription = #Generators.GeneratorDescription
  Where SalesSummary1.TotalAmount > 0

IF OBJECT_ID('tempdb.dbo.#GeneratorRanking', 'U') IS NOT NULL  
DROP TABLE #GeneratorRanking
--RANK: A generator can have more than one valid associated sale.  Rank 1 picks which to use based on the PostDate and Time (See #SalesConnection WHERE)
SELECT dim_mms_member_key,
       GeneratorDescription,
       Created_Date,
       created_dim_date_key,
       post_dim_date_key,
       post_dim_time_key,
       mms_tran_id,
	   primary_sales_dim_employee_key,
       PostDate,
       DaysDiff,
       TransactionRank,
       TotalAmount,
       RANK() OVER (PARTITION BY dim_mms_member_key, GeneratorDescription
                        ORDER BY post_dim_date_key, post_dim_time_key, mms_tran_id, TransactionRank) GeneratorRank 
  INTO #GeneratorRanking  
  FROM #TransactionRanking  
 WHERE TransactionRank = 1

IF OBJECT_ID('tempdb.dbo.#SalesConnection', 'U') IS NOT NULL  
DROP TABLE #SalesConnection

SELECT dim_mms_member_key,
       GeneratorDescription,
       created_dim_date_key,
       mms_tran_id,
	   primary_sales_dim_employee_key,
       post_dim_date_key,
       post_dim_time_key,
       TotalAmount,
       TransactionRank
  INTO #SalesConnection   
  FROM #GeneratorRanking
 WHERE GeneratorRank = 1   



IF OBJECT_ID('tempdb.dbo.#ConnectionSummary', 'U') IS NOT NULL  
DROP TABLE #ConnectionSummary

--Connect valid #SalesConnections to #ServiceConnection.
SELECT #ServiceConnection.dim_mms_member_key,
       #ServiceConnection.GeneratorDescription,
	   #ServiceConnection.DeliveringTeamMemberDimEmployeeKey,
	   #ServiceConnection.DeliveringTeamMemberEmployeeID,
	   #ServiceConnection.DeliveringTeamMemberName,
	   #SalesConnection.primary_sales_dim_employee_key,
       MIN(DATEDIFF(DD,JoinDimDate.Calendar_Date,CreatedDimDate.Calendar_Date)) DaysToConnect,
       SUM(#SalesConnection.TotalAmount) TotalAmount
  INTO #ConnectionSummary  
  FROM #ServiceConnection
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON #ServiceConnection.join_date_key = JoinDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] CreatedDimDate
    ON #ServiceConnection.created_dim_date_key = CreatedDimDate.dim_date_key
  LEFT JOIN #SalesConnection
    ON #ServiceConnection.dim_mms_member_key = #SalesConnection.dim_mms_member_key
   AND #ServiceConnection.GeneratorDescription = #SalesConnection.GeneratorDescription
   AND #ServiceConnection.created_dim_date_key = #SalesConnection.created_dim_date_key
 WHERE #ServiceConnection.SessionRanking = 1
 GROUP BY #ServiceConnection.dim_mms_member_key,
          #ServiceConnection.GeneratorDescription,
		  #ServiceConnection.DeliveringTeamMemberDimEmployeeKey,
		  #ServiceConnection.DeliveringTeamMemberEmployeeID,
	      #ServiceConnection.DeliveringTeamMemberName,
		  #SalesConnection.primary_sales_dim_employee_key
   

IF OBJECT_ID('tempdb.dbo.#TotalNewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #TotalNewMembersByTrainer


 ---- Gather total distinct members per delivering team member
 Select #NewMembers.home_dim_club_key as dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126) as MemberJoinMonth,
 IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,-1) as DeliveringTeamMemberID, Count(Distinct(#NewMembers.dim_mms_member_key))  as NumberOfMembers
 INTO #TotalNewMembersByTrainer
 From #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.dim_mms_member_key= #ConnectionSummary.dim_mms_member_key
	Group By #NewMembers.home_dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126),IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,-1)


IF OBJECT_ID('tempdb.dbo.#MoveItNewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #MoveItNewMembersByTrainer

 ---- Gather total "Move it" distinct members per delivering team member
Select #NewMembers.home_dim_club_key as dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126) as MemberJoinMonth,
IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) as DeliveringTeamMemberID, Count(Distinct(#NewMembers.dim_mms_member_key))  as NumberOfMembers
 INTO #MoveItNewMembersByTrainer
 From #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.dim_mms_member_key= #ConnectionSummary.dim_mms_member_key
 Where #ConnectionSummary.GeneratorDescription= 'FitPoint'
	Group By #NewMembers.home_dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126),IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)

IF OBJECT_ID('tempdb.dbo.#myHealthScoreNewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #myHealthScoreNewMembersByTrainer
	 ---- Gather total "myHealthScore" distinct members per delivering team member
Select #NewMembers.home_dim_club_key as dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126) as MemberJoinMonth,
IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) as DeliveringTeamMemberID, Count(Distinct(#NewMembers.dim_mms_member_key))  as NumberOfMembers
 INTO #myHealthScoreNewMembersByTrainer
 From #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.dim_mms_member_key= #ConnectionSummary.dim_mms_member_key
 Where #ConnectionSummary.GeneratorDescription= 'myHealthScore'
	Group By #NewMembers.home_dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126),IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)

IF OBJECT_ID('tempdb.dbo.#myLTBuck$NewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #myLTBuck$NewMembersByTrainer
  ---- Gather total "myLTBuck$" distinct members per delivering team member
Select #NewMembers.home_dim_club_key as dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126) as MemberJoinMonth,
IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) as DeliveringTeamMemberID, Count(Distinct(#NewMembers.dim_mms_member_key))  as NumberOfMembers
 INTO #myLTBuck$NewMembersByTrainer
 From #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.dim_mms_member_key= #ConnectionSummary.dim_mms_member_key
 Where #ConnectionSummary.GeneratorDescription= 'myLTBucks'
	Group By #NewMembers.home_dim_club_key,CONVERT(char(7),cast( #NewMembers.MemberJoinMonth as datetime),126),IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)


IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL  
DROP TABLE #Results
--Result set!
SELECT DimLocation.dim_club_key,
       DimDescriptionPTCRLArea.description as Region,       
       CASE WHEN CHARINDEX('-',DimDescriptionRegion.description)>0 THEN LEFT(DimDescriptionRegion.description,CHARINDEX('-',DimDescriptionRegion.description)-1) ELSE '' END AS VP,
       DimDescriptionRegion.description as MMSRegionName,
       DimLocation.club_name as Club,
       DimLocation.club_code as ClubCode,
       JoinDimDate.four_digit_year_dash_two_digit_month as MemberJoinMonth,
	   CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0')='0'  
	   			THEN -1
				WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0')
			     AND IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0') <> '0'
				THEN 0
				ELSE #ConnectionSummary.DeliveringTeamMemberEmployeeID
				END DeliveringTeamMemberEmployeeID,
	   CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0')='0'  
	   			THEN ' No Lead Generator Session'
			WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0')
			     AND IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0') <> '0'
			    THEN ' Delivering Team Member Not Sale Team Member'
				ELSE #ConnectionSummary.DeliveringTeamMemberName
				END DeliveringTeamMemberName,
       
       SUM(CASE WHEN GeneratorDescription = 'FitPoint' 
	             AND #ConnectionSummary.TotalAmount IS NOT NULL 
				THEN 1 ELSE 0 END) FitPointFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'FitPoint' 
	            THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) FitPointTotalSales,
       CASE WHEN SUM(CASE WHEN GeneratorDescription = 'FitPoint' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN GeneratorDescription = 'FitPoint' THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN GeneratorDescription = 'FitPoint' THEN 1 ELSE 0 END) END FitPointAverageDaysToConnect,
       SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
	              AND #ConnectionSummary.TotalAmount IS NOT NULL 
				  THEN 1 ELSE 0 END) myHealthScoreFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
	            THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) myHealthScoreTotalSales,
       CASE WHEN SUM(CASE WHEN GeneratorDescription = 'myHealthScore' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
			              THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
						                                                               THEN 1 ELSE 0 END) 
						   END myHealthScoreAverageDaysToConnect,
       SUM(CASE WHEN GeneratorDescription = 'MyLTBucks' 
	              AND #ConnectionSummary.TotalAmount IS NOT NULL 
				  THEN 1 ELSE 0 END) MyLTBucksFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'MyLTBucks' 
				  THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) MyLTBucksTotalSales,
       CASE WHEN SUM(CASE WHEN GeneratorDescription = 'MyLTBucks' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN GeneratorDescription = 'MyLTBucks' THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN GeneratorDescription = 'MyLTBucks' THEN 1 ELSE 0 END) END MyLTBucksAverageDaysToConnect
  INTO #Results
  FROM #NewMembers  
  JOIN [marketing].[v_dim_club] DimLocation
    ON #NewMembers.home_dim_club_key = DimLocation.dim_club_key
  LEFT JOIN [marketing].[v_dim_description] DimDescriptionRegion
	ON DimDescriptionRegion.dim_description_key = DimLocation.region_dim_description_key
  LEFT JOIN [marketing].[v_dim_description] DimDescriptionPTCRLArea
	ON DimDescriptionPTCRLArea.dim_description_key = DimLocation.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON #NewMembers.join_date_key = JoinDimDate.dim_date_key
  LEFT JOIN #ConnectionSummary
    ON #NewMembers.dim_mms_member_key = #ConnectionSummary.dim_mms_member_key
 GROUP BY DimLocation.dim_club_key,
          DimDescriptionPTCRLArea.description,       
		  CASE WHEN CHARINDEX('-',DimDescriptionRegion.description)>0 THEN LEFT(DimDescriptionRegion.description,CHARINDEX('-',DimDescriptionRegion.description)-1) ELSE '' END,
	      DimDescriptionRegion.description,
          DimLocation.club_name,
          DimLocation.club_code,
          JoinDimDate.four_digit_year_dash_two_digit_month,
		  CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0')='0'  
	   			THEN -1
				WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0')
			     AND IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0') <> '0'
				THEN 0
				ELSE #ConnectionSummary.DeliveringTeamMemberEmployeeID
				END,
	      CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0')='0'  
	   			THEN ' No Lead Generator Session'
			WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0')
			     AND IsNull(#ConnectionSummary.primary_sales_dim_employee_key,'0') <> '0'
			    THEN ' Delivering Team Member Not Sale Team Member'
				ELSE #ConnectionSummary.DeliveringTeamMemberName
				END

SELECT 
#Results.dim_club_key,
Region,
VP,
MMSRegionName,
Club,
ClubCode,
CASE When DeliveringTeamMemberEmployeeID = -1
     THEN ''
	 ELSE DeliveringTeamMemberEmployeeID
	 END DeliveringTeamMemberEmployeeID,
DeliveringTeamMemberName,
ERA.role_name AS EmployeeRole,	
#Results.MemberJoinMonth,	
IsNull(#Total.NumberOfMembers,0)	AS TrainingSolutions_NumberOfMembers,   ------ actually not limited to "Training Solutions"
IsNull(#MoveIt.NumberOfMembers,0)	AS TrainingSolutions_NumberOfConnections,

CASE WHEN IsNull(#Total.NumberOfMembers,0) <>0 THEN CAST(IsNull(#MoveIt.NumberOfMembers,0)*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS TrainingSolutions_ConnectivityPercent,
FitPointFourteenDaySalesCount AS TrainingSolutions_SalesWithin14Days,
CASE WHEN IsNull(#MoveIt.NumberOfMembers,0) <>0 THEN CAST(FitPointFourteenDaySalesCount*1.00/#MoveIt.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END	AS TrainingSolutions_ClosingPercent,

FitPointTotalSales	AS TrainingSolutions_Revenue,
CASE WHEN FitPointFourteenDaySalesCount <> 0 THEN CAST(FitPointTotalSales/FitPointFourteenDaySalesCount AS INT) ELSE 0 END AS TrainingSolutions_AvgRevenue_Sale,
CASE WHEN IsNull(#Total.NumberOfMembers,0) <> 0 THEN CAST(FitPointFourteenDaySalesCount*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS TrainingSolutions_PenetrationPercent,

FitPointAverageDaysToConnect AS TrainingSolutions_AvgDaysToConnect,
	
IsNull(#myHealthScore.NumberOfMembers,0)	AS myHealthScore_NumberOfConnections,

CASE WHEN IsNull(#Total.NumberOfMembers,0) <>0 THEN CAST(IsNull(#myHealthScore.NumberOfMembers,0)*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_ConnectivityPercent,
CASE WHEN IsNull(#MoveIt.NumberOfMembers,0) <> 0 THEN CAST(IsNull(#myHealthScore.NumberOfMembers,0)*1.00/#MoveIt.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_PercentOfTSWhoDidMHS,
myHealthScoreFourteenDaySalesCount AS myHealthScore_SalesWithin14Days,
CASE WHEN IsNull(#myHealthScore.NumberOfMembers,0) <> 0 THEN CAST(myHealthScoreFourteenDaySalesCount*1.00/#myHealthScore.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_ClosingPercent,
myHealthScoreTotalSales AS myHealthScore_Revenue,
CASE WHEN myHealthScoreFourteenDaySalesCount <> 0 THEN myHealthScoreTotalSales*1.00/myHealthScoreFourteenDaySalesCount ELSE 0 END AS myHealthScore_AvgRevenue_Sale,
CASE WHEN IsNull(#Total.NumberOfMembers,0)<>0 THEN  CAST(myHealthScoreFourteenDaySalesCount*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_PenetrationPercent,
myHealthScoreAverageDaysToConnect AS myHealthScore_AvgDaysToConnect,
	
IsNull(#myLTBucks.NumberOfMembers,0) AS myLTBucks_NumberOfConnections,
CASE WHEN IsNull(#Total.NumberOfMembers,0)<>0 THEN CAST(IsNull(#myLTBucks.NumberOfMembers,0)*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_ConnectivityPercent,
CASE WHEN IsNull(#MoveIt.NumberOfMembers,0) <> 0 THEN CAST(IsNull(#myLTBucks.NumberOfMembers,0) *1.00/#MoveIt.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_PercentOfTSWhoDidLTB,
MyLTBucksFourteenDaySalesCount AS myLTBucks_SalesWithin14Days,
CASE WHEN IsNull(#myLTBucks.NumberOfMembers,0) <>0 THEN CAST(MyLTBucksFourteenDaySalesCount*1.00/#myLTBucks.NumberOfMembers  AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_ClosingPercent,
MyLTBucksTotalSales	AS myLTBucks_Revenue,
CASE WHEN MyLTBucksFourteenDaySalesCount<>0 THEN MyLTBucksTotalSales*1.00/MyLTBucksFourteenDaySalesCount ELSE 0 END AS myLTBucks_AvgRevenue_Sale,
CASE WHEN IsNull(#Total.NumberOfMembers,0)<>0 THEN CAST(MyLTBucksFourteenDaySalesCount*1.00/#Total.NumberOfMembers AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_PenetrationPercent,
MyLTBucksAverageDaysToConnect  AS myLTBucks_AvgDaysToConnect,
@ReportRunDateTime  AS ReportRunDateTime,
Cast(@Increment AS Varchar(3)) AS HeaderMemberConnectionDays,
@HeaderDateRange AS HeaderDateRange,
@HeaderAsOfDate AS HeaderAsOfDate

 
FROM #Results
LEFT JOIN #TotalNewMembersByTrainer #Total
ON #Results.DeliveringTeamMemberEmployeeID = #Total.DeliveringTeamMemberID
   AND #Results.dim_club_key = #Total.dim_club_key
   AND #Results.MemberJoinMonth = #Total.MemberJoinMonth
LEFT JOIN #MoveItNewMembersByTrainer #MoveIt
ON #Results.DeliveringTeamMemberEmployeeID = #MoveIt.DeliveringTeamMemberID
   AND #Results.dim_club_key = #MoveIt.dim_club_key
   AND #Results.MemberJoinMonth = #MoveIt.MemberJoinMonth
LEFT JOIN #myHealthScoreNewMembersByTrainer #myHealthScore
ON #Results.DeliveringTeamMemberEmployeeID = #myHealthScore.DeliveringTeamMemberID
   AND #Results.dim_club_key = #myHealthScore.dim_club_key
   AND #Results.MemberJoinMonth = #myHealthScore.MemberJoinMonth
LEFT JOIN #myLTBuck$NewMembersByTrainer  #myLTBucks
ON #Results.DeliveringTeamMemberEmployeeID = #myLTBucks.DeliveringTeamMemberID
   AND #Results.dim_club_key = #myLTBucks.dim_club_key
   AND #Results.MemberJoinMonth = #myLTBucks.MemberJoinMonth
LEFT JOIN [marketing].[v_dim_employee] E
    ON E.employee_id = #Results.DeliveringTeamMemberEmployeeID
JOIN [marketing].[v_dim_employee_bridge_dim_employee_role] EBERA
    ON EBERA.dim_employee_key = E.dim_employee_key
JOIN [marketing].[v_dim_employee_role] ERA
    ON ERA.Dim_Employee_Role_Key = EBERA.Dim_Employee_Role_Key 

WHERE --E.active_status = '1'   And 
EBERA.primary_employee_role_flag = 'Y'

 
END


