CREATE PROC [reporting].[proc_LeadGeneratorScoreboard] @MemberJoinStartDate [DATETIME],@MemberJoinEndDate [DATETIME],@Increment [INT],@AsOfDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

 IF 1=0 BEGIN
       SET FMTONLY OFF
     END


---- Sample execution
---- exec [reporting].proc_LeadGeneratorScoreboard '7/1/2019','7/31/2019','2','8/15/2019'
--DECLARE 
--	@MemberJoinStartDate [DATETIME] = '1/1/2019',
--	@MemberJoinEndDate [DATETIME] = '2/1/2019',
--	@Increment [INT] = '30',
--	@AsOfDate [DATETIME]  = '2/1/2019'
	

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

SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @MemberJoinStartDate)
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @MemberJoinEndDate)
SET @AsOfDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @AsOfDate)

SET @Increment = CONVERT(INT,replace(cast(@Increment as varchar), '.0', ''))
--  Need to explicitly remove decimal and zero and then convert to an INT before Cognos will accept this value

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL  
DROP TABLE #DimReportingHierarchy


 SELECT DISTINCT DimReportingHierarchy.dim_reporting_hierarchy_key
  INTO #DimReportingHierarchy
  FROM [marketing].v_dim_reporting_hierarchy_history BridgeDimReportingHierarchy
  JOIN [marketing].v_dim_reporting_hierarchy_history DimReportingHierarchy
    ON BridgeDimReportingHierarchy.reporting_division = DimReportingHierarchy.reporting_division
   AND BridgeDimReportingHierarchy.reporting_sub_division = DimReportingHierarchy.reporting_sub_division
   AND BridgeDimReportingHierarchy.reporting_department = DimReportingHierarchy.reporting_department
  JOIN ( SELECT DISTINCT month_ending_dim_date_key
          FROM [marketing].[v_dim_date]
         WHERE dim_date_key >= @StartDimDateKey
           AND dim_date_key <= @EndDimDateKey ) MonthEndingDimDateKeys
    ON BridgeDimReportingHierarchy.effective_dim_date_key <= MonthEndingDimDateKeys.month_ending_dim_date_key
   AND BridgeDimReportingHierarchy.expiration_dim_date_key > MonthEndingDimDateKeys.month_ending_dim_date_key
   AND DimReportingHierarchy.effective_dim_date_key <= MonthEndingDimDateKeys.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > MonthEndingDimDateKeys.month_ending_dim_date_key
 WHERE ISNULL(BridgeDimReportingHierarchy.dim_reporting_hierarchy_key,'-998') not in('-997', '-998','-999')  
    AND BridgeDimReportingHierarchy.reporting_division IN ('Personal Training','PT Division')


IF OBJECT_ID('tempdb.dbo.#Generators', 'U') IS NOT NULL  
DROP TABLE #Generators
   
SELECT 'MyLTBucks' GeneratorDescription, 1 GeneratorSortOrder
  INTO #Generators
 UNION ALL
SELECT 'myHealthScore',2
 UNION ALL
SELECT 'On Boarding',3

/* START - Code to get the List of Members from BOSS and Exerp who have a PT Onboarding session scheduled 
in past 20 Months going upto 2 months in future*/
IF OBJECT_ID('tempdb.dbo.#PTOnboarding_BOSS', 'U') IS NOT NULL  
DROP TABLE #PTOnboarding_BOSS
IF OBJECT_ID('tempdb.dbo.#PTOnboarding_EXERP', 'U') IS NOT NULL  
DROP TABLE #PTOnboarding_EXERP	
IF OBJECT_ID('tempdb.dbo.#PTOnboarding', 'U') IS NOT NULL  
DROP TABLE #PTOnboarding	

/* BOSS */
select 
--res.reservation_id,
mem.[customer_name_last_first],
mem.[member_id],
mem.[membership_id],
min(rmd.[meeting_dim_date_key]) min_obs_date
INTO #PTOnboarding_BOSS
from 
[marketing].[v_dim_boss_reservation] res
JOIN [marketing].[v_dim_boss_reservation_meeting_date] rmd on res.dim_boss_reservation_key = rmd.dim_boss_reservation_key
JOIN [marketing].[v_fact_boss_daily_roster] fdr on rmd.dim_boss_reservation_key = fdr.dim_boss_reservation_key and rmd.[meeting_dim_date_key] = fdr.[meeting_dim_date_key]
JOIN [marketing].[v_dim_boss_product] p on res.dim_boss_product_key = p.dim_boss_product_key and p.upc_code = '701592826589' 
JOIN [marketing].[v_dim_mms_member] mem on mem.dim_mms_member_key = fdr.dim_mms_member_key
JOIN [marketing].[v_dim_date] dt on rmd.[meeting_dim_date_key] = dt.dim_date_key
where 
dt.[calendar_date] between cast(DateAdd(mm, -20, GetDate()) as date) And cast(DateAdd(mm, +2, GetDate())  as date)
group by mem.[customer_name_last_first],
mem.[member_id],
mem.[membership_id]

/* EXERP*/

select 
	mem.[customer_name_last_first],
	mem.[member_id],
	mem.[membership_id],
	min(bo.[start_dim_date_key]) min_obs_date 
INTO #PTOnboarding_EXERP
from [marketing].[v_dim_exerp_booking] bo
JOIN [marketing].[v_dim_exerp_activity] a ON bo.dim_exerp_activity_key = a.dim_exerp_activity_key and a.external_id =  '701592826589' 
JOIN [marketing].[v_fact_exerp_participation] p on bo.dim_exerp_booking_key = p.dim_exerp_booking_key
JOIN [marketing].[v_dim_mms_member] mem on mem.dim_mms_member_key = p.dim_mms_member_key
JOIN [marketing].[v_dim_date] dt on bo.[start_dim_date_key] = dt.dim_date_key
where
dt.[calendar_date] between cast(DateAdd(mm, -20, GetDate()) as date) And cast(DateAdd(mm, +2, GetDate())  as date)
group by mem.[customer_name_last_first],
mem.[member_id],
mem.[membership_id]

select * 
INTO #PTOnboarding
from #PTOnboarding_BOSS
UNION ALL
select * from #PTOnboarding_EXERP
/* END - Code to get the List of Members from BOSS and Exerp who have a PT Onboarding session scheduled 
in past 20 Months going upto 2 months in future - END*/

IF OBJECT_ID('tempdb.dbo.#NewMembers', 'U') IS NOT NULL  
DROP TABLE #NewMembers

SELECT DimMMSMember.dim_mms_member_key as DimCustomerKey,
       DimMMSMember.member_id as MemberID,
	   DimMMSMember.membership_id as MembershipID,
	   DimMMSMember.First_Name as FirstName,
	   DimMMSMember.Last_Name as LastName,
	   FactMembership.home_dim_club_key as DimLocationKey,
       FactMembership.original_sales_dim_employee_key as OriginalSalesDimEmployeeKey,
       DimMMSMember.join_date_key as JoinDimDateKey,
	   JoinDimDate.calendar_date as MemberJoinDate,
       CASE WHEN IncrementDimDate.Dim_Date_Key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.Dim_Date_Key
            END as IncrementEndDimDateKey,
	   CASE WHEN IsNull(ptob.customer_name_last_first,'null') = 'null'
	        THEN 0 
			ELSE 1 END as ScheduledOnboarding
  INTO #NewMembers
  FROM [marketing].[v_dim_mms_member] DimMMSMember
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON DimMMSMember.join_date_key = JoinDimDate.Dim_Date_Key
  JOIN [marketing].[v_dim_mms_membership_history] FactMembership
    ON DimMMSMember.membership_id = FactMembership.membership_id
   --AND FactMembership.effective_date_time <= JoinDimDate.month_ending_date
   AND CONVERT(VARCHAR, FactMembership.effective_date_time,101) <= JoinDimDate.month_ending_date
   AND CONVERT(VARCHAR, FactMembership.expiration_date_time,101) > JoinDimDate.month_ending_date
 JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON MembershipType.dim_mms_membership_type_key = FactMembership.dim_mms_membership_type_key
	AND MembershipType.attribute_dssr_group_description <> 'DSSR_Other'
  JOIN [marketing].[v_dim_date] IncrementDimDate
    ---ON DATEADD(dd,@Increment,JoinDimDate.calendar_date) = IncrementDimDate.calendar_date
	---AND datepart(year,JoinDimDate.calendar_date) != 9999
	ON case when datepart(year,JoinDimDate.calendar_date)= 9999 then 0
		 when DATEADD(dd,@Increment,JoinDimDate.calendar_date) = IncrementDimDate.calendar_date then 1 else 0 END = 1
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimMMSMember.member_type_dim_description_key
  LEFT JOIN #PTOnboarding ptob on DimMMSMember.member_id = ptob.member_id
 WHERE DimMMSMember.join_date_key >= @StartDimDateKey
   AND DimMMSMember.join_date_key <= @EndDimDateKey
   AND MemberActivitiesRegion.description <> 'Junior'
   AND DimMMSMember.join_date_key <= @AsOfDateKey
 Group By DimMMSMember.dim_mms_member_key,
       DimMMSMember.member_id,
	   DimMMSMember.membership_id,
	   DimMMSMember.First_Name,
	   DimMMSMember.Last_Name,
	   FactMembership.home_dim_club_key ,
       FactMembership.original_sales_dim_employee_key,
       DimMMSMember.join_date_key,
	   JoinDimDate.calendar_date,
       CASE WHEN IncrementDimDate.Dim_Date_Key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.Dim_Date_Key
			END,
	   CASE WHEN IsNull(ptob.customer_name_last_first,'null') = 'null'
	        THEN 0 
			ELSE 1 END	

	
IF OBJECT_ID('tempdb.dbo.#NewMemberAccountManager', 'U') IS NOT NULL  
DROP TABLE #NewMemberAccountManager			
			
			
 ---- To create a smaller temp table rather than joining to the large DimEmployee table in a larger query
Select NewMembers.DimCustomerKey,
       AccountManagerDimEmployee.employee_id as AccountManagerEmployeeID,
       AccountManagerDimEmployee.employee_name as AccountManagerName
 INTO #NewMemberAccountManager
From #NewMembers NewMembers
 JOIN [marketing].[v_dim_employee] AccountManagerDimEmployee
   ON NewMembers.OriginalSalesDimEmployeeKey = AccountManagerDimEmployee.dim_employee_key
   

DECLARE @MyLTBucksDimEmployeeKey VARCHAR(8000)
SET @MyLTBucksDimEmployeeKey = (SELECT dim_employee_key FROM  [marketing].[v_dim_employee] WHERE Employee_ID = -5) 

DECLARE @AbsoluteStart INT, @AbsoluteEnd INT
SELECT @AbsoluteStart = MIN(JoinDimDateKey),
       @AbsoluteEnd = MAX(IncrementEndDimDateKey)
  FROM #NewMembers


IF OBJECT_ID('tempdb.dbo.#ServiceDetail', 'U') IS NOT NULL  
DROP TABLE #ServiceDetail	
  
  
SELECT #NewMembers.DimCustomerKey,
       #NewMembers.MembershipID,
       #NewMembers.JoinDimDateKey,
       #NewMembers.IncrementEndDimDateKey,
	   CASE WHEN DimProduct.connectivity_primary_lead_generator_flag = 'Y' THEN 'On Boarding' 
	        WHEN DimProduct.connectivity_lead_generator_flag = 'Y' THEN 'myHealthScore' 
            END GeneratorDescription,
		DimProduct.product_description as SessionProduct,                  
       FactPackageSession.fact_mms_package_session_key,
       FactPackageSession.created_dim_date_key,
       CreatedDimDate.calendar_date CreatedDate,
	   DimEmployee.dim_employee_key DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.employee_id DeliveringTeamMemberEmployeeID,
	   DimEmployee.employee_name DeliveringTeamMemberName,
	   CASE WHEN FourteenDayDimDate.dim_date_key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE FourteenDayDimDate.dim_date_key
            END FourteenDayDimDateKey,
       CASE WHEN FourteenDayDimDate.calendar_date > @AsOfDate
            THEN @AsOfDate
            ELSE FourteenDayDimDate.calendar_date
            END FourteenDayDate
  INTO #ServiceDetail
  FROM [marketing].[v_fact_mms_package_session] FactPackageSession
  JOIN [marketing].[v_dim_date] CreatedDimDate
    ON FactPackageSession.created_dim_date_key = CreatedDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] FourteenDayDimDate
    ---ON DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date
	ON case when datepart(year,CreatedDimDate.calendar_date)= 9999 then 0
	 when DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date then 1 else 0 END = 1
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.DimCustomerKey
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
 WHERE FactPackageSession.voided_flag = 'N'
   AND FactPackageSession.created_dim_date_key >= @AbsoluteStart
   AND FactPackageSession.created_dim_date_key <= @AbsoluteEnd
   AND (UPPER(DimProduct.connectivity_primary_lead_generator_flag) = 'Y' OR UPPER(DimProduct.connectivity_lead_generator_flag) = 'Y')  
  
INSERT INTO #ServiceDetail

SELECT #NewMembers.DimCustomerKey,
       #NewMembers.MembershipID,
       #NewMembers.JoinDimDateKey,
       #NewMembers.IncrementEndDimDateKey,
       'myLTBucks' GeneratorDescription,
	   DimProduct.product_description as SessionProduct,
       FactPackageSession.fact_mms_package_session_key,
       FactPackageSession.created_dim_date_key,
       CreatedDimDate.calendar_date CreatedDate,
	   DimEmployee.dim_employee_key DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.employee_id DeliveringTeamMemberEmployeeID,
	   DimEmployee.employee_name DeliveringTeamMemberName,
	   CASE WHEN FourteenDayDimDate.dim_date_key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE FourteenDayDimDate.dim_date_key
            END FourteenDayDimDateKey,
       CASE WHEN FourteenDayDimDate.calendar_date > @AsOfDate
            THEN @AsOfDate
            ELSE FourteenDayDimDate.calendar_date
            END FourteenDayDate
  FROM [marketing].[v_fact_mms_package_session] FactPackageSession
  JOIN [marketing].[v_dim_date] CreatedDimDate
    ON FactPackageSession.created_dim_date_key = CreatedDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] FourteenDayDimDate
    ---ON DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date
	ON case when datepart(year,CreatedDimDate.calendar_date)= 9999 then 0
	when DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date then 1 else 0 END = 1
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.DimCustomerKey
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN [marketing].[v_dim_employee] DimEmployee
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
 WHERE created_dim_date_key NOT BETWEEN JoinDimDateKey AND IncrementEndDimDateKey


--The ranking is necessary later on when the valid #SalesConnections start getting assigned to #Service connections for the case where a 
--Generator has more than one session on the same day.


IF OBJECT_ID('tempdb.dbo.#ServiceConnection', 'U') IS NOT NULL  
DROP TABLE #ServiceConnection


SELECT DimCustomerKey,
      JoinDimDateKey,
	  GeneratorDescription,
	  SessionProduct,
	  created_dim_date_key,
	  CreatedDate,
	  DeliveringTeamMemberDimEmployeeKey,
	  DeliveringTeamMemberEmployeeID,
	  DeliveringTeamMemberName,
      FourteenDayDimDateKey,
      FourteenDayDate, 
      RANK() OVER(PARTITION BY DimCustomerKey, GeneratorDescription, created_dim_date_key
                       ORDER BY fact_mms_package_session_key) SessionRanking
  INTO #ServiceConnection
  FROM #ServiceDetail


----The following code looks for the members who had a Lead Gen service, but another member 
 ---- made a "14 day sale" purchase on their behalf -  a package product for them to use.
 ---- In the past, since the Lead Gen service member did not make the purchase, they were not recognized with a "14 day sale" 

 ------- Find all the packages that have been created since the beginning of the selected period through the "As of Date"
 ------- for members who have had Lead Gen Sessions
 
IF OBJECT_ID('tempdb.dbo.#PurchasedPackagesForSessionMembers', 'U') IS NOT NULL  
DROP TABLE #PurchasedPackagesForSessionMembers


Select FactPackage.dim_mms_member_key,
       FactPackage.mms_tran_id,
	   FactPackage.tran_item_id,
	   FactPackage.created_dim_date_key
 INTO #PurchasedPackagesForSessionMembers
   FROM [marketing].[v_fact_mms_package] FactPackage
	   JOIN #ServiceConnection ServiceConnection
		  ON FactPackage.dim_mms_member_key = ServiceConnection.DimCustomerKey
   WHERE FactPackage.created_dim_date_key >= @StartDimDateKey
	   AND FactPackage.created_dim_date_key <= @AsOfDateKey
	   AND FactPackage.Transaction_Void_Flag = 'N'
	   AND FactPackage.Price_Per_Session <> 0
   GROUP BY FactPackage.dim_mms_member_key,
       FactPackage.mms_tran_id,
	   FactPackage.tran_item_id,
	   FactPackage.created_dim_date_key


------- Which of these packages were not purchased by the user member
IF OBJECT_ID('tempdb.dbo.#PackagesNotPurchasedBySessionMembers', 'U') IS NOT NULL  
DROP TABLE #PackagesNotPurchasedBySessionMembers

Select SessionMembers.dim_mms_member_key as PackageDimCustomerKey,
       FactSalesTransaction.dim_mms_member_key as PurchaserDimCustomerKey,
	   FactSalesTransaction.tran_item_id as TranItemID,
	   FactSalesTransaction.mms_tran_id as TranID 
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
SELECT #ServiceConnection.DimCustomerKey,
       #ServiceConnection.GeneratorDescription ,
       #ServiceConnection.CreatedDate,
       #ServiceConnection.created_dim_date_key as CreatedDimDateKey,
	   #ServiceConnection.SessionProduct,
	   FactSalesTransaction.post_dim_date_key as PostDimDateKey,
       FactSalesTransaction.post_dim_time_key as PostDimTimeKey,
       FactSalesTransaction.mms_tran_id as TranID,
	   FactSalesTransaction.tran_item_id as TranItemId,
       FactSalesTransaction.fact_mms_sales_transaction_key as FactSalesTransactionKey,
       FactSalesTransaction.dim_mms_product_key as DimProductKey,
       FactSalesTransaction.primary_sales_dim_employee_key as PrimarySalesDimEmployeeKey,
       PostDimDate.calendar_date as PostDate,
       DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date) as DaysDiff,
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) as RefundDaysDiff,
	   FactSalesTransaction.sales_dollar_amount as SalesDollarAmount,
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount
  INTO #SalesDetail
  FROM #ServiceConnection
  JOIN #PackagesNotPurchasedBySessionMembers                     
    ON #ServiceConnection.DimCustomerKey = #PackagesNotPurchasedBySessionMembers.PackageDimCustomerKey
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction                             
    ON #PackagesNotPurchasedBySessionMembers.TranItemID = FactSalesTransaction.tran_item_id   ------- primary filtering join for this query
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > PostDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key
  LEFT JOIN [marketing].[v_dim_date] RefundPostDimDate
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
 GROUP BY #ServiceConnection.DimCustomerKey,
	   #ServiceConnection.GeneratorDescription,
       #ServiceConnection.CreatedDate,
       #ServiceConnection.created_dim_date_key,
	   #ServiceConnection.SessionProduct,
	   FactSalesTransaction.post_dim_date_key,
       FactSalesTransaction.post_dim_time_key,
       FactSalesTransaction.mms_tran_id,
	   FactSalesTransaction.tran_item_id,
       FactSalesTransaction.fact_mms_sales_transaction_key,
       FactSalesTransaction.dim_mms_product_key,
       FactSalesTransaction.primary_sales_dim_employee_key,
       PostDimDate.calendar_date,
	   FactSalesTransaction.sales_dollar_amount,
       DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date),
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date))
	
UNION ALL

SELECT #ServiceConnection.DimCustomerKey,
       #ServiceConnection.GeneratorDescription,
       #ServiceConnection.CreatedDate,
       #ServiceConnection.created_dim_date_key as CreatedDimDateKey,
	   #ServiceConnection.SessionProduct,
	   FactSalesTransaction.post_dim_date_key as PostDimDateKey,
       FactSalesTransaction.post_dim_time_key as PostDimTimeKey,
       FactSalesTransaction.mms_tran_id as TranID,
	   FactSalesTransaction.tran_item_id as TranItemId,
       FactSalesTransaction.fact_mms_sales_transaction_key as FactSalesTransactionKey,
       FactSalesTransaction.dim_mms_product_key as DimProductKey,
       FactSalesTransaction.primary_sales_dim_employee_key as PrimarySalesDimEmployeeKey,
       PostDimDate.calendar_date as PostDate,
       DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date) as DaysDiff,
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) as RefundDaysDiff,
	   FactSalesTransaction.sales_dollar_amount as SalesDollarAmount,
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount
  FROM #ServiceConnection
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
    ON #ServiceConnection.DimCustomerKey = FactSalesTransaction.dim_mms_member_key
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > PostDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.dim_reporting_hierarchy_key
  JOIN [marketing].[v_dim_plan_exchange_rate] USDDimPlanExchangeRate
    ON FactSalesTransaction.usd_dim_plan_exchange_rate_key = USDDimPlanExchangeRate.dim_plan_exchange_rate_key
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key
  LEFT JOIN [marketing].[v_dim_date] RefundPostDimDate
    ON FactSalesTransactionAutomatedRefund.refund_post_dim_date_key = RefundPostDimDate.dim_date_key
   --AND RefundPostDimDate.calendar_date <= DATEADD(day,30,PostDimDate.calendar_date)
   AND dateadd(day,-30,RefundPostDimDate.calendar_date) <= PostDimDate.calendar_date
  LEFT JOIN #PackagesNotPurchasedBySessionMembers
    ON FactSalesTransaction.tran_item_id = #PackagesNotPurchasedBySessionMembers.TranItemID

 WHERE FactSalesTransaction.post_dim_date_key >= #ServiceConnection.created_dim_date_key
   AND FactSalesTransaction.post_dim_date_key <= #ServiceConnection.FourteenDayDimDateKey
   AND FactSalesTransaction.voided_flag = 'N'
   AND FactSalesTransaction.transaction_edited_flag = 'N'  
   AND FactSalesTransaction.reversal_flag = 'N'
   AND FactSalesTransaction.refund_flag = 'N'
   AND FactSalesTransaction.sales_dollar_amount > 0
   AND IsNull(#PackagesNotPurchasedBySessionMembers.TranID,0) = 0    ------ prevents the counting of a transaction in both queries in this union
   AND (FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key IS NULL
        OR RefundPostDimDate.dim_date_key IS NOT NULL)
		
 GROUP BY #ServiceConnection.DimCustomerKey,
          #ServiceConnection.GeneratorDescription,
          #ServiceConnection.CreatedDate,
          #ServiceConnection.created_dim_date_key,
		  FactSalesTransaction.post_dim_time_key,
		  #ServiceConnection.SessionProduct,		  
          FactSalesTransaction.fact_mms_sales_transaction_key,
          FactSalesTransaction.post_dim_date_key,
          FactSalesTransaction.mms_tran_id,
		  FactSalesTransaction.tran_item_id,
          FactSalesTransaction.dim_mms_product_key,
          FactSalesTransaction.primary_sales_dim_employee_key,
          PostDimDate.calendar_date,
          FactSalesTransaction.sales_dollar_amount,
         DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date),
	     DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date))

			   
  --Sum to transaction level
IF OBJECT_ID('tempdb.dbo.#SalesSummary', 'U') IS NOT NULL  
DROP TABLE #SalesSummary 

SELECT DimCustomerKey,
       GeneratorDescription,
       CreatedDate,
       CreatedDimDateKey,
       PostDimDateKey,
       PostDimTimeKey,
       TranID,
       PostDate,
       DaysDiff,
       SUM(CASE WHEN RefundDaysDiff <= 30
	            THEN SalesDollarAmount - abs(RelatedRefundDollarAmount)
				ELSE SalesDollarAmount
				END) TotalAmount
  INTO #SalesSummary
  FROM #SalesDetail
 GROUP BY DimCustomerKey,
          GeneratorDescription,
          CreatedDate,
          CreatedDimDateKey,
          PostDimDateKey,
          PostDimTimeKey,
          TranID,
          PostDate,
          DaysDiff

 --Assign a sale to at most one lead generator service (unless there are more than one service in a day)
 --RANK: A transaction will only ever be associated with one generator.  Rank 1 picks the correct one based on the GeneratorSortOrder (See #GenerationRanking WHERE)
IF OBJECT_ID('tempdb.dbo.#TransactionRanking', 'U') IS NOT NULL  
DROP TABLE #TransactionRanking 
 
SELECT SalesSummary1.DimCustomerKey,
       SalesSummary1.GeneratorDescription,
       SalesSummary1.CreatedDate,
       SalesSummary1.CreatedDimDateKey,
       SalesSummary1.PostDimDateKey,
       SalesSummary1.PostDimTimeKey,
       SalesSummary1.TranID,
       SalesSummary1.PostDate,
       SalesSummary1.DaysDiff,
       SalesSummary1.TotalAmount,
       RANK() OVER (PARTITION BY DimCustomerKey, SalesSummary1.TranID
                        ORDER BY #Generators.GeneratorSortOrder) TransactionRank
 INTO #TransactionRanking
  FROM #SalesSummary SalesSummary1
  JOIN (SELECT TranID, MIN(DaysDiff) DaysDiff FROM #SalesSummary GROUP BY TranID) MinTran
    ON SalesSummary1.TranID = MinTran.TranID
   AND SalesSummary1.DaysDiff = MinTran.DaysDiff
  JOIN #Generators
    ON SalesSummary1.GeneratorDescription = #Generators.GeneratorDescription
	Where SalesSummary1.TotalAmount > 0

--RANK: A generator can have more than one valid associated sale.  Rank 1 picks which to use based on the PostDate and Time (See #SalesConnection WHERE)
IF OBJECT_ID('tempdb.dbo.#GeneratorRanking', 'U') IS NOT NULL  
DROP TABLE #GeneratorRanking 

SELECT DimCustomerKey,
       GeneratorDescription,
       CreatedDate,
       CreatedDimDateKey,
       PostDimDateKey,
       PostDimTimeKey,
       TranID,
       PostDate,
       DaysDiff,
       TransactionRank,
       TotalAmount,
       RANK() OVER (PARTITION BY DimCustomerKey, GeneratorDescription
                        ORDER BY PostDimDateKey, PostDimTimeKey, TranID, TransactionRank) GeneratorRank 
 INTO #GeneratorRanking
  FROM #TransactionRanking
 WHERE TransactionRank = 1

IF OBJECT_ID('tempdb.dbo.#SalesConnection', 'U') IS NOT NULL  
DROP TABLE #SalesConnection  

SELECT DimCustomerKey,
       GeneratorDescription,
       CreatedDimDateKey,
       TranID,
       PostDimDateKey,
       PostDimTimeKey,
	   DaysDiff,
       TotalAmount,
       TransactionRank
  INTO #SalesConnection
  FROM #GeneratorRanking
 WHERE GeneratorRank = 1

--Connect valid #SalesConnections to #ServiceConnection.
IF OBJECT_ID('tempdb.dbo.#ConnectionSummary', 'U') IS NOT NULL  
DROP TABLE #ConnectionSummary 

SELECT #ServiceConnection.DimCustomerKey,
       #ServiceConnection.GeneratorDescription,
	   MIN(DATEDIFF(DD,JoinDimDate.Calendar_Date,CreatedDimDate.Calendar_Date)) DaysToConnect,
       SUM(#SalesConnection.TotalAmount) TotalAmount
  INTO #ConnectionSummary
  FROM #ServiceConnection
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON #ServiceConnection.JoinDimDateKey = JoinDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] CreatedDimDate
    ON #ServiceConnection.created_dim_date_key = CreatedDimDate.dim_date_key
  LEFT JOIN #SalesConnection
    ON #ServiceConnection.DimCustomerKey = #SalesConnection.DimCustomerKey
   AND #ServiceConnection.GeneratorDescription = #SalesConnection.GeneratorDescription
   AND #ServiceConnection.created_dim_date_key = #SalesConnection.CreatedDimDateKey
  LEFT JOIN [marketing].[v_dim_employee]  DimEmployee 
    ON DimEmployee.employee_id = #ServiceConnection.DeliveringTeamMemberEmployeeID
	AND DimEmployee.employee_active_flag = 'Y' 
  LEFT JOIN [marketing].[v_dim_employee_bridge_dim_employee_role] EmployeeBridge
    ON EmployeeBridge.dim_employee_key = DimEmployee.dim_employee_key
     And EmployeeBridge.primary_employee_role_flag = 'Y'
  LEFT JOIN [marketing].[v_dim_employee_role] EmployeeRole 
    ON EmployeeRole.dim_employee_role_key = EmployeeBridge.dim_employee_role_key
 WHERE #ServiceConnection.SessionRanking = 1
 GROUP BY #ServiceConnection.DimCustomerKey,
          #ServiceConnection.GeneratorDescription
		 

		

IF OBJECT_ID('tempdb.dbo.#DetailDataSet', 'U') IS NOT NULL  
DROP TABLE #DetailDataSet		  
--Result set!
SELECT distinct DimLocation.dim_club_key as DimLocationKey,
       DimDescriptionPTCRLArea.description as Region,       
       CASE WHEN CHARINDEX('-',DimDescriptionRegion.description)>0 THEN LEFT(DimDescriptionRegion.description,CHARINDEX('-',DimDescriptionRegion.description)-1) ELSE '' END AS VP,
       DimDescriptionRegion.description as MMSRegionName,
       DimLocation.club_name as Club,
       DimLocation.club_code as ClubCode,
       JoinDimDate.four_digit_year_dash_two_digit_month as MemberJoinMonth,
	   count(distinct #NewMembers.DimCustomerKey) NewMemberCount,
	   sum(#NewMembers.ScheduledOnboarding) OnboardingScheduled,
	   SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' THEN 1 ELSE 0 END) FitPointConnections,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' AND #ConnectionSummary.TotalAmount IS NOT NULL THEN 1 ELSE 0 END) FitPointFourteenDaySalesCount,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) FitPointTotalSales,
       CASE WHEN SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'On Boarding' THEN 1 ELSE 0 END) END FitPointAverageDaysToConnect,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' THEN 1 ELSE 0 END) myHealthScoreConnections,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' AND #ConnectionSummary.TotalAmount IS NOT NULL THEN 1 ELSE 0 END) myHealthScoreFourteenDaySalesCount,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) myHealthScoreTotalSales,
       CASE WHEN SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'myHealthScore' THEN 1 ELSE 0 END) END myHealthScoreAverageDaysToConnect,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' THEN 1 ELSE 0 END) MyLTBucksConnections,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' AND #ConnectionSummary.TotalAmount IS NOT NULL THEN 1 ELSE 0 END) MyLTBucksFourteenDaySalesCount,
       SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' THEN ISNULL(#ConnectionSummary.TotalAmount,0) ELSE 0 END) MyLTBucksTotalSales,
       CASE WHEN SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' THEN #ConnectionSummary.DaysToConnect ELSE 0 END) / SUM(CASE WHEN #ConnectionSummary.GeneratorDescription = 'MyLTBucks' THEN 1 ELSE 0 END) END MyLTBucksAverageDaysToConnect
  INTO #DetailDataSet
  FROM #NewMembers  
  JOIN [marketing].[v_dim_club] DimLocation
    ON #NewMembers.DimLocationKey = DimLocation.dim_club_key
  LEFT JOIN [marketing].[v_dim_description] DimDescriptionRegion
	ON DimDescriptionRegion.dim_description_key = DimLocation.region_dim_description_key
  LEFT JOIN [marketing].[v_dim_description] DimDescriptionPTCRLArea
	ON DimDescriptionPTCRLArea.dim_description_key = DimLocation.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON #NewMembers.JoinDimDateKey = JoinDimDate.dim_date_key
  LEFT JOIN #ConnectionSummary
    ON #NewMembers.DimCustomerKey = #ConnectionSummary.DimCustomerKey
 
  GROUP BY  DimLocation.dim_club_key,
	   DimDescriptionPTCRLArea.description,       
       CASE WHEN CHARINDEX('-',DimDescriptionRegion.description)>0 THEN LEFT(DimDescriptionRegion.description,CHARINDEX('-',DimDescriptionRegion.description)-1) ELSE '' END,
	   DimDescriptionRegion.description,
       DimLocation.club_name,
       DimLocation.club_code,
       JoinDimDate.four_digit_year_dash_two_digit_month

SELECT 
DimLocationKey,
Region,
VP,
MMSRegionName,
Club,
ClubCode,	
MemberJoinMonth,	
NewMemberCount	AS TrainingSolutions_NumberOfMembers,
OnboardingScheduled as OnboardingsScheduled,
CASE WHEN NewMemberCount <> 0 THEN CAST(OnboardingScheduled*1.00 / NewMemberCount as Decimal(8,6)) ELSE 0 END as OnboardingScheduledPercent,
FitPointConnections	AS TrainingSolutions_NumberOfConnections,
CASE WHEN NewMemberCount <>0 THEN CAST(FitPointConnections*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS TrainingSolutions_ConnectivityPercent,
FitPointFourteenDaySalesCount AS TrainingSolutions_SalesWithin14Days,
CASE WHEN FitPointConnections <>0 THEN CAST(FitPointFourteenDaySalesCount*1.00/FitPointConnections AS DECIMAL(8,6)) ELSE 0 END	AS TrainingSolutions_ClosingPercent,
FitPointTotalSales	AS TrainingSolutions_Revenue,
CASE WHEN FitPointFourteenDaySalesCount <> 0 THEN CAST(FitPointTotalSales/FitPointFourteenDaySalesCount AS INT) ELSE 0 END AS TrainingSolutions_AvgRevenue_Sale,
CASE WHEN NewMemberCount <> 0 THEN CAST(FitPointFourteenDaySalesCount*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS TrainingSolutions_PenetrationPercent,
FitPointAverageDaysToConnect AS TrainingSolutions_AvgDaysToConnect,
myHealthScoreConnections AS myHealthScore_NumberOfConnections,
CASE WHEN NewMemberCount <>0 THEN CAST(myHealthScoreConnections*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_ConnectivityPercent,
CASE WHEN FitPointConnections <> 0 THEN CAST(myHealthScoreConnections*1.00/FitPointConnections AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_PercentOfTSWhoDidMHS,
myHealthScoreFourteenDaySalesCount AS myHealthScore_SalesWithin14Days,
CASE WHEN myHealthScoreConnections <> 0 THEN CAST(myHealthScoreFourteenDaySalesCount*1.00/myHealthScoreConnections AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_ClosingPercent,
myHealthScoreTotalSales AS myHealthScore_Revenue,
CASE WHEN myHealthScoreFourteenDaySalesCount <> 0 THEN myHealthScoreTotalSales*1.00/myHealthScoreFourteenDaySalesCount ELSE 0 END AS myHealthScore_AvgRevenue_Sale,
CASE WHEN NewMemberCount<>0 THEN  CAST(myHealthScoreFourteenDaySalesCount*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS myHealthScore_PenetrationPercent,
myHealthScoreAverageDaysToConnect AS myHealthScore_AvgDaysToConnect,
MyLTBucksConnections AS myLTBucks_NumberOfConnections,
CASE WHEN NewMemberCount<>0 THEN CAST(MyLTBucksConnections*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_ConnectivityPercent,
CASE WHEN FitPointConnections <> 0 THEN CAST(MyLTBucksConnections*1.00/FitPointConnections AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_PercentOfTSWhoDidLTB,
MyLTBucksFourteenDaySalesCount AS myLTBucks_SalesWithin14Days,
CASE WHEN MyLTBucksConnections<>0 THEN CAST(MyLTBucksFourteenDaySalesCount*1.00/MyLTBucksConnections AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_ClosingPercent,
MyLTBucksTotalSales	AS myLTBucks_Revenue,
CASE WHEN MyLTBucksFourteenDaySalesCount<>0 THEN MyLTBucksTotalSales*1.00/MyLTBucksFourteenDaySalesCount ELSE 0 END AS myLTBucks_AvgRevenue_Sale,
CASE WHEN NewMemberCount<>0 THEN CAST(MyLTBucksFourteenDaySalesCount*1.00/NewMemberCount AS DECIMAL(8,6)) ELSE 0 END AS myLTBucks_PenetrationPercent,
MyLTBucksAverageDaysToConnect  AS myLTBucks_AvgDaysToConnect,
@ReportRunDateTime  AS ReportRunDateTime,
Cast(@Increment AS Varchar(3)) AS HeaderMemberConnectionDays,
@HeaderDateRange AS HeaderDateRange,
@HeaderAsOfDate AS HeaderAsOfDate
FROM #DetailDataSet


END

