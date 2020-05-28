CREATE PROC [reporting].[proc_UDW_FactPTDSSRLeadGeneratorEmployeeSummary] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



----  Daily data - append to a database summary table
----  @ReportDate is automated to set report to yesterday's date
----  Table will hold daily data for 13 months;  only EOM data for prior months



DECLARE @ReportDate DATETIME 
SET @ReportDate = '1/1/1900'

DECLARE @AsOfDate DATETIME
SET @AsOfDate = CASE WHEN @ReportDate = '1/1/1900'
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END

DECLARE @ReportDateLastDayInMonthIndicator 	VARCHAR(1)
DECLARE @Increment INT
DECLARE @ReportMonthStartDate DATETIME
DECLARE @ReportMonthStartDateDimDateKey VARCHAR(32)
DECLARE @ReportMonthEndDateDimDateKey VARCHAR(32)
DECLARE @MemberJoinStartDate DATETIME
DECLARE @MemberJoinEndDate DATETIME
DECLARE @FirstOf13MonthsPriorDimDateKey VARCHAR(32)


DECLARE @AsOfDateKey VARCHAR(32)
DECLARE @StartDimDateKey VARCHAR(32)
DECLARE @EndDimDateKey VARCHAR(32)


DECLARE @MyLTBucksDimEmployeeKey VARCHAR(32)


DECLARE @AbsoluteStart VARCHAR(32)
DECLARE @AbsoluteEnd VARCHAR(32)

  ------ Create 30 day connection increment data set
SET @Increment  = 30
SET @ReportDateLastDayInMonthIndicator = (SELECT last_day_in_month_flag FROM [marketing].[v_dim_date] WHERE dim_date_key = @AsOfDateKey)
SET @ReportMonthStartDate = DATEADD(MONTH,DATEDIFF(MONTH,0,@AsOfDate),0)    ----- returns 1st of Report day's month
SET @ReportMonthStartDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] where calendar_date = @ReportMonthStartDate)
SET @ReportMonthEndDateDimDateKey = (SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] where calendar_date = @ReportMonthStartDate)
SET @MemberJoinStartDate = DATEADD(Month,-1,(DATEADD(MONTH,(DATEDIFF(MONTH,0,@AsOfDate)),0)))    ----- returns 1st of Report day's prior month (30 day Connection Increment)
SET @MemberJoinEndDate = CONVERT(DATETIME,CONVERT(VARCHAR,@AsOfDate,101),101)   ----- returns Report day's date
SET @FirstOf13MonthsPriorDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-13,@ReportMonthStartDate))


SET @StartDimDateKey = (SELECT dim_date_key  FROM [marketing].[v_dim_date] WHERE calendar_date = @MemberJoinStartDate)  ---Changed from vDimdate to DimDate
SET @EndDimDateKey = (SELECT dim_date_key  FROM [marketing].[v_dim_date] WHERE calendar_date = @MemberJoinEndDate)
SET @AsOfDateKey = (SELECT dim_date_key  FROM [marketing].[v_dim_date] WHERE calendar_date = @AsOfDate)

--  Need to explicitly remove decimal and zero and then convert to an INT before Cognos will accept this value
SET @Increment = CONVERT(INT,replace(cast(@Increment as varchar), '.0', ''))



 ----  Over the course of time, business terminology has changed from "FitPoint" to "Training Solutions" to "Move It" and now to "On Boarding"


DECLARE @HeaderDateRange VARCHAR(33)
DECLARE @ReportRunDateTime VARCHAR(21) 
DECLARE @HeaderAsOfDate VARCHAR(33)

SET @ReportRunDateTime = Replace(Substring(convert(varchar,getdate(),100),1,6)+', '+Substring(convert(varchar,GETDATE(),100),8,10)+' '+Substring(convert(varchar,getdate(),100),18,2),'  ',' ')
SET @HeaderDateRange = convert(varchar(12), @MemberJoinStartDate, 107) + ' and ' + convert(varchar(12), @MemberJoinEndDate, 107)
SET @HeaderAsOfDate = convert(varchar(12), @AsOfDate, 107)


IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL  
DROP TABLE #DimReportingHierarchy


 SELECT DISTINCT DimReportingHierarchy.dim_reporting_hierarchy_key AS DimReportingHierarchyKey
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

SELECT 'myLT Bucks' GeneratorDescription, 1 GeneratorSortOrder
  INTO #Generators
 UNION ALL
SELECT 'myHealthScore',2
 UNION ALL
SELECT 'On Boarding',3  


IF OBJECT_ID('tempdb.dbo.#NewMembers', 'U') IS NOT NULL  
DROP TABLE #NewMembers

SELECT DimMember.dim_mms_member_key AS DimMemberKey,  
       MembershipDimClub.dim_club_key AS DimClubKey,  
       DimMember.join_date_key AS JoinDimDateKey,
	   JoinDimDate.four_digit_year_dash_two_digit_month AS MemberJoinMonth,
       CASE WHEN IncrementDimDate.dim_date_key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.dim_date_key
            END IncrementEndDimDateKey
  INTO #NewMembers   
  FROM [marketing].[v_dim_mms_member] DimMember   
  JOIN [marketing].[v_dim_date] JoinDimDate
    ON DimMember.join_date_key = JoinDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_membership_history] FactMembership
    ON DimMember.membership_id = FactMembership.membership_id
   AND CONVERT(DATE,CONVERT(VARCHAR,FactMembership.effective_date_time,101)) <= JoinDimDate.month_ending_date
   AND CONVERT(DATE,CONVERT(VARCHAR,FactMembership.expiration_date_time,101)) > JoinDimDate.month_ending_date
  JOIN [marketing].[v_dim_club] MembershipDimClub   
    ON FactMembership.club_id = MembershipDimClub.club_id
  JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON MembershipType.dim_mms_membership_type_key = FactMembership.dim_mms_membership_type_key
	AND MembershipType.attribute_dssr_group_description <> 'DSSR_Other'
  JOIN [marketing].[v_dim_date] IncrementDimDate
    ON DATEADD(dd,@Increment,JoinDimDate.calendar_date) = IncrementDimDate.calendar_date
  JOIN [marketing].[v_dim_description]  MemberTypeDescription
   ON MemberTypeDescription.dim_description_key = DimMember.member_type_dim_description_key  
 WHERE DimMember.join_date_key >= @StartDimDateKey
   AND DimMember.join_date_key <= @EndDimDateKey
   AND MemberTypeDescription.description <> 'Junior'
   AND DimMember.join_date_key <= @AsOfDateKey
 GROUP BY DimMember.dim_mms_member_key,
       MembershipDimClub.dim_club_key,  
       DimMember.join_date_key,
	   JoinDimDate.four_digit_year_dash_two_digit_month,
       CASE WHEN IncrementDimDate.dim_date_key > @AsOfDateKey
            THEN @AsOfDateKey
            ELSE IncrementDimDate.dim_date_key
            END



SET @MyLTBucksDimEmployeeKey = (SELECT dim_employee_key FROM [marketing].[v_dim_employee] WHERE employee_id = -5)


SELECT @AbsoluteStart = MIN(JoinDimDateKey),
       @AbsoluteEnd = MAX(IncrementEndDimDateKey)
  FROM #NewMembers


IF OBJECT_ID('tempdb.dbo.#ServiceDetail', 'U') IS NOT NULL  
DROP TABLE #ServiceDetail 

 ---- first to insert sessions for lead gen products

SELECT #NewMembers.DimMemberKey,  
       #NewMembers.JoinDimDateKey,
       #NewMembers.IncrementEndDimDateKey,
	   CASE WHEN DimProduct.connectivity_primary_lead_generator_flag = 'Y' THEN 'On Boarding'  
	        WHEN DimProduct.connectivity_lead_generator_flag = 'Y' THEN 'myHealthScore'
            END GeneratorDescription,
       FactPackageSession.fact_mms_package_session_key AS FactPackageSessionKey,
       FactPackageSession.created_dim_date_key AS CreatedDimDateKey,
       CreatedDimDate.calendar_date AS CreatedDate,
	   DimEmployee.dim_employee_key AS DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.employee_id AS DeliveringTeamMemberEmployeeID,
	   DimEmployee.employee_name AS DeliveringTeamMemberName,
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
    ON DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.DimMemberKey
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey 
  JOIN [marketing].[v_dim_employee]  DimEmployee
    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key

 WHERE FactPackageSession.voided_flag = 'N'
   AND FactPackageSession.created_dim_date_key >= @AbsoluteStart
   AND FactPackageSession.created_dim_date_key <= @AbsoluteEnd
   AND (UPPER(DimProduct.connectivity_primary_lead_generator_flag) = 'Y' 
        OR UPPER(DimProduct.connectivity_lead_generator_flag) = 'Y') 
 
 ---- then to insert sessions for purchases through the lt bucks loyalty program that are not lead gen products

INSERT INTO #ServiceDetail  
SELECT #NewMembers.DimMemberKey,
       #NewMembers.JoinDimDateKey,
       #NewMembers.IncrementEndDimDateKey,
       'myLT Bucks' GeneratorDescription,
       FactPackageSession.fact_mms_package_session_key AS FactPackageSessionKey,
       FactPackageSession.created_dim_date_key AS CreatedDimDateKey,
       CreatedDimDate.calendar_date AS CreatedDate,
	   DimEmployee.dim_employee_key AS DeliveringTeamMemberDimEmployeeKey,
	   DimEmployee.employee_id AS DeliveringTeamMemberEmployeeID,
	   DimEmployee.employee_name AS DeliveringTeamMemberName,
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
    ON DATEADD(dd,14,CreatedDimDate.calendar_date) = FourteenDayDimDate.calendar_date
  JOIN #NewMembers
    ON FactPackageSession.dim_mms_member_key = #NewMembers.DimMemberKey
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= CreatedDimDate.month_ending_date
   AND DimProduct.expiration_date_time > CreatedDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey 
  JOIN [marketing].[v_dim_employee]  DimEmployee
    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key

 WHERE FactPackageSession.voided_flag = 'N'
   AND FactPackageSession.created_dim_date_key >= @AbsoluteStart
   AND FactPackageSession.created_dim_date_key <= @AbsoluteEnd
   AND FactPackageSession.package_entered_dim_employee_key = @MyLTBucksDimEmployeeKey
   AND UPPER(DimProduct.connectivity_primary_lead_generator_flag) = 'N'
   AND UPPER(DimProduct.connectivity_lead_generator_flag) = 'N'

    

DELETE 
  FROM #ServiceDetail 
 WHERE CreatedDimDateKey NOT BETWEEN JoinDimDateKey AND IncrementEndDimDateKey

 

 IF OBJECT_ID('tempdb.dbo.#ServiceConnection', 'U') IS NOT NULL  
DROP TABLE #ServiceConnection 

--The ranking is necessary later on when the valid #SalesConnections start getting assigned to #Service connections for the case where a 
--Generator has more than one session on the same day.
SELECT DimMemberKey,
       JoinDimDateKey,
       GeneratorDescription,
       CreatedDimDateKey,
       CreatedDate,
	   DeliveringTeamMemberDimEmployeeKey,
	   DeliveringTeamMemberEmployeeID,
	   DeliveringTeamMemberName,
       FourteenDayDimDateKey,
       FourteenDayDate,
       RANK() OVER(PARTITION BY DimMemberKey, GeneratorDescription, CreatedDimDateKey
                       ORDER BY FactPackageSessionKey) SessionRanking
  INTO #ServiceConnection  
  FROM #ServiceDetail

 IF OBJECT_ID('tempdb.dbo.#SalesDetail', 'U') IS NOT NULL  
DROP TABLE #SalesDetail 

--All PT sales items within 14 days for every service in #ServiceConnection
SELECT #ServiceConnection.DimMemberKey,
       #ServiceConnection.GeneratorDescription,
       #ServiceConnection.CreatedDate,
       #ServiceConnection.CreatedDimDateKey,
       FactSalesTransaction.post_dim_date_key AS PostDimDateKey,
       FactSalesTransaction.post_dim_time_key AS PostDimTimeKey,
       FactSalesTransaction.mms_tran_id AS TranID,
	   FactSalesTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
       PostDimDate.calendar_date AS PostDate,
       DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date) DaysDiff,
	   DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) RefundDaysDiff,
	   FactSalesTransaction.sales_dollar_amount AS SalesDollarAmount, 
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount
  INTO #SalesDetail  
  FROM #ServiceConnection
  JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
    ON #ServiceConnection.DimMemberKey = FactSalesTransaction.dim_mms_member_key
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product_history]  DimProduct
    ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > PostDimDate.month_ending_date
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key
  LEFT JOIN [marketing].[v_dim_date]  RefundPostDimDate
    ON FactSalesTransactionAutomatedRefund.refund_post_dim_date_key = RefundPostDimDate.dim_date_key
	--   AND RefundPostDimDate.calendar_date <= DATEADD(dd,30,PostDimDate.calendar_date)    ----- throwing error - eliminating limit because the same 30 day filter is applied in next temp table
 WHERE FactSalesTransaction.post_dim_date_key >= #ServiceConnection.CreatedDimDateKey
   AND FactSalesTransaction.post_dim_date_key <= #ServiceConnection.FourteenDayDimDateKey
   AND FactSalesTransaction.voided_flag = 'N'
   AND FactSalesTransaction.transaction_edited_flag = 'N'
   AND FactSalesTransaction.reversal_flag = 'N'
   AND FactSalesTransaction.refund_flag = 'N'
   AND FactSalesTransaction.sales_dollar_amount > 0
   AND (FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key IS NULL
        OR RefundPostDimDate.dim_date_key IS NOT NULL)
 GROUP BY #ServiceConnection.DimMemberKey,
          #ServiceConnection.GeneratorDescription,
          #ServiceConnection.CreatedDate,
          #ServiceConnection.CreatedDimDateKey,
          FactSalesTransaction.fact_mms_sales_transaction_item_key,
          FactSalesTransaction.post_dim_date_key,
          FactSalesTransaction.post_dim_time_key,
          FactSalesTransaction.mms_tran_id,
		  FactSalesTransaction.primary_sales_dim_employee_key,
          PostDimDate.calendar_date,
          FactSalesTransaction.sales_dollar_amount,
          DATEDIFF(DD,#ServiceConnection.CreatedDate,PostDimDate.calendar_date),
		  DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date))

 IF OBJECT_ID('tempdb.dbo.#SalesSummary', 'U') IS NOT NULL  
DROP TABLE #SalesSummary 

--Sum to transaction level
SELECT DimMemberKey,
       GeneratorDescription,
       CreatedDate,
       CreatedDimDateKey,
       PostDimDateKey,
       PostDimTimeKey,
       TranID,
	   PrimarySalesDimEmployeeKey,
       PostDate,
       DaysDiff,
       SUM(CASE WHEN RefundDaysDiff <= 30
	            THEN SalesDollarAmount + RelatedRefundDollarAmount    ----- Refund amounts are stored as negative amounts in UDW
				ELSE SalesDollarAmount
				END) TotalAmount 
  INTO #SalesSummary
  FROM #SalesDetail   

 -----WHERE TotalAmount > 0
 GROUP BY DimMemberKey,
          GeneratorDescription,
          CreatedDate,
          CreatedDimDateKey,
          PostDimDateKey,
          PostDimTimeKey,
          TranID,
		  PrimarySalesDimEmployeeKey,
          PostDate,
          DaysDiff



 IF OBJECT_ID('tempdb.dbo.#TransactionRanking', 'U') IS NOT NULL  
DROP TABLE #TransactionRanking 

--Assign a sale to at most one lead generator service (unless there are more than one service in a day)
--RANK: A transaction will only ever be associated with one generator.  Rank 1 picks the correct one based on the GeneratorSortOrder (See #GenerationRanking WHERE)
SELECT SalesSummary1.DimMemberKey,
       SalesSummary1.GeneratorDescription,
       SalesSummary1.CreatedDate,
       SalesSummary1.CreatedDimDateKey,
       SalesSummary1.PostDimDateKey,
       SalesSummary1.PostDimTimeKey,
       SalesSummary1.TranID,
	   SalesSummary1.PrimarySalesDimEmployeeKey,
       SalesSummary1.PostDate,
       SalesSummary1.DaysDiff,
       SalesSummary1.TotalAmount,
       RANK() OVER (PARTITION BY DimMemberKey, SalesSummary1.TranID
                        ORDER BY GeneratorSortOrder) TransactionRank
  INTO #TransactionRanking   
  FROM #SalesSummary SalesSummary1
  JOIN (SELECT TranID, MIN(DaysDiff) DaysDiff FROM #SalesSummary GROUP BY TranID) MinTran
    ON SalesSummary1.TranID = MinTran.TranID
   AND SalesSummary1.DaysDiff = MinTran.DaysDiff
  JOIN #Generators
    ON SalesSummary1.GeneratorDescription = #Generators.GeneratorDescription
  WHERE SalesSummary1.TotalAmount > 0

 IF OBJECT_ID('tempdb.dbo.#GeneratorRanking', 'U') IS NOT NULL  
DROP TABLE #GeneratorRanking 

--RANK: A generator can have more than one valid associated sale.  Rank 1 picks which to use based on the PostDate and Time (See #SalesConnection WHERE)
SELECT DimMemberKey,
       GeneratorDescription,
       CreatedDate,
       CreatedDimDateKey,
       PostDimDateKey,
       PostDimTimeKey,
       TranID,
	   PrimarySalesDimEmployeeKey,
       PostDate,
       DaysDiff,
       TransactionRank,
       TotalAmount,
       RANK() OVER (PARTITION BY DimMemberKey, GeneratorDescription
                        ORDER BY PostDimDateKey, PostDimTimeKey, TranID, TransactionRank) GeneratorRank 
  INTO #GeneratorRanking   
  FROM #TransactionRanking
 WHERE TransactionRank = 1

 IF OBJECT_ID('tempdb.dbo.#SalesConnection', 'U') IS NOT NULL  
DROP TABLE #SalesConnection 

SELECT DimMemberKey,
       GeneratorDescription,
       CreatedDimDateKey,
       TranID,
	   PrimarySalesDimEmployeeKey,
       PostDimDateKey,
       PostDimTimeKey,
       TotalAmount,
       TransactionRank
  INTO #SalesConnection   
  FROM #GeneratorRanking
 WHERE GeneratorRank = 1


 IF OBJECT_ID('tempdb.dbo.#ConnectionSummary_Total', 'U') IS NOT NULL  
DROP TABLE #ConnectionSummary_Total
 
  --Connect valid #SalesConnections to #ServiceConnection.

SELECT #ServiceConnection.DimMemberKey,
       #ServiceConnection.GeneratorDescription,
	   #ServiceConnection.DeliveringTeamMemberDimEmployeeKey,
	   #ServiceConnection.DeliveringTeamMemberEmployeeID,
	   #ServiceConnection.DeliveringTeamMemberName,
	   #SalesConnection.PrimarySalesDimEmployeeKey,
       SUM(#SalesConnection.TotalAmount) TotalAmount,
       MIN(CreatedDimDate.calendar_date) as SessionDate,
	   MIN(#ServiceConnection.CreatedDimDateKey) as SessionDimDateKey,
	   MIN(IsNull(#SalesConnection.PostDimDateKey,21000101)) as SaleDateDimDateKey

  INTO #ConnectionSummary_Total   
  FROM #ServiceConnection
  JOIN [marketing].[v_dim_date] CreatedDimDate
    ON #ServiceConnection.CreatedDimDateKey = CreatedDimDate.dim_date_key
  LEFT JOIN #SalesConnection
    ON #ServiceConnection.DimMemberKey = #SalesConnection.DimMemberKey
   AND #ServiceConnection.GeneratorDescription = #SalesConnection.GeneratorDescription
   AND #ServiceConnection.CreatedDimDateKey = #SalesConnection.CreatedDimDateKey
 WHERE #ServiceConnection.SessionRanking = 1
 GROUP BY #ServiceConnection.DimMemberKey,
          #ServiceConnection.GeneratorDescription,
		  #ServiceConnection.DeliveringTeamMemberDimEmployeeKey,
		  #ServiceConnection.DeliveringTeamMemberEmployeeID,
	      #ServiceConnection.DeliveringTeamMemberName,
		  #SalesConnection.PrimarySalesDimEmployeeKey
   

IF OBJECT_ID('tempdb.dbo.#ConnectionSummary', 'U') IS NOT NULL  
DROP TABLE #ConnectionSummary

    ----- Filter off all but PT DSSR MTD related connections/sales
SELECT DimMemberKey,
       GeneratorDescription,
	   DeliveringTeamMemberDimEmployeeKey,
	   DeliveringTeamMemberEmployeeID,
	   DeliveringTeamMemberName,  
	   PrimarySalesDimEmployeeKey,
	   TotalAmount,
	   SessionDate, 
	   SessionDimDateKey
INTO #ConnectionSummary   
FROM #ConnectionSummary_Total AS ConnectionSummary_Total
  LEFT JOIN [marketing].[v_dim_date] SaleDate
   ON ConnectionSummary_Total.SaleDateDimDateKey = SaleDate.dim_date_key
WHERE ConnectionSummary_Total.SessionDate >= @ReportMonthStartDate  
  OR (CASE WHEN ConnectionSummary_Total.SaleDateDimDateKey = 21000101
           THEN '1/1/1900'
		   ELSE SaleDate.calendar_date
		   END) >= @ReportMonthStartDate

 

IF OBJECT_ID('tempdb.dbo.#MoveItNewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #MoveItNewMembersByTrainer

 ---- Gather total "On Boarding" distinct members per delivering team member
SELECT #NewMembers.DimClubKey,
	   IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) AS DeliveringTeamMemberID, 
	   COUNT(DISTINCT(#NewMembers.DimMemberKey))  AS NumberOfMembers
 INTO #MoveItNewMembersByTrainer   
 FROM #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.DimMemberKey = #ConnectionSummary.DimMemberKey
 WHERE #ConnectionSummary.GeneratorDescription= 'On Boarding'  
	GROUP BY #NewMembers.DimClubKey,
			 IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)

IF OBJECT_ID('tempdb.dbo.#myHealthScoreNewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #myHealthScoreNewMembersByTrainer

	 ---- Gather total "myHealthScore" distinct members per delivering team member
SELECT #NewMembers.DimClubKey,
	   IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) AS DeliveringTeamMemberID, 
	   COUNT(DISTINCT(#NewMembers.DimMemberKey))  AS NumberOfMembers
 INTO #myHealthScoreNewMembersByTrainer   
 FROM #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.DimMemberKey = #ConnectionSummary.DimMemberKey
 WHERE #ConnectionSummary.GeneratorDescription= 'myHealthScore'
	GROUP BY #NewMembers.DimClubKey,
			 IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)

IF OBJECT_ID('tempdb.dbo.#myLTBuck$NewMembersByTrainer', 'U') IS NOT NULL  
DROP TABLE #myLTBuck$NewMembersByTrainer

  ---- Gather total "myLTBuck$" distinct members per delivering team member
SELECT #NewMembers.DimClubKey,
	   IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0) AS DeliveringTeamMemberID, 
	   COUNT(DISTINCT(#NewMembers.DimMemberKey))  AS NumberOfMembers
 INTO #myLTBuck$NewMembersByTrainer  
 FROM #NewMembers
   LEFT JOIN #ConnectionSummary
    ON #NewMembers.DimMemberKey = #ConnectionSummary.DimMemberKey
 WHERE #ConnectionSummary.GeneratorDescription= 'myLT Bucks'
	GROUP BY #NewMembers.DimClubKey,
	IsNull(#ConnectionSummary.DeliveringTeamMemberEmployeeID,0)



IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL  
DROP TABLE #Results

--Result set!
SELECT DimLocation.dim_club_key AS DimClubKey,
       DimLocation.club_name AS Club,
       DimLocation.club_code AS ClubCode,
	   CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0')= '0'  
	   			THEN -1
				WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0')
			     AND IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0') <> '0'
				THEN 0
				ELSE #ConnectionSummary.DeliveringTeamMemberEmployeeID
				END DeliveringTeamMemberEmployeeID,
	   CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0') = '0'  
	   			THEN ' No Lead Generator Session'
			WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0')
			     AND IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0') <> '0'
			    THEN ' Delivering Team Member Not Sale Team Member'
				ELSE #ConnectionSummary.DeliveringTeamMemberName
				END DeliveringTeamMemberName,     
       SUM(CASE WHEN GeneratorDescription = 'On Boarding' 
	             AND #ConnectionSummary.TotalAmount IS NOT NULL 
				THEN 1 ELSE 0 END) FitPointFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'On Boarding'  
	            THEN IsNull(#ConnectionSummary.TotalAmount,0) ELSE 0 END) FitPointTotalSales,
       SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
	              AND #ConnectionSummary.TotalAmount IS NOT NULL 
				  THEN 1 ELSE 0 END) myHealthScoreFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'myHealthScore' 
	            THEN IsNull(#ConnectionSummary.TotalAmount,0) ELSE 0 END) myHealthScoreTotalSales,
       SUM(CASE WHEN GeneratorDescription = 'myLT Bucks' 
	              AND #ConnectionSummary.TotalAmount IS NOT NULL 
				  THEN 1 ELSE 0 END) MyLTBucksFourteenDaySalesCount,
       SUM(CASE WHEN GeneratorDescription = 'myLT Bucks' 
				  THEN IsNull(#ConnectionSummary.TotalAmount,0) ELSE 0 END) MyLTBucksTotalSales

  INTO #Results     ------ This is the "#Results" from the LTFDW query proc_LTFDW_FactPTDSSRLeadGeneratorEmployeeSummary   
  FROM #NewMembers  
  JOIN [marketing].[v_dim_club] DimLocation
    ON #NewMembers.DimClubKey = DimLocation.dim_club_key
  LEFT JOIN #ConnectionSummary
    ON #NewMembers.DimMemberKey = #ConnectionSummary.DimMemberKey
 GROUP BY DimLocation.dim_club_key,
          DimLocation.club_name,
          DimLocation.club_code,
		  CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0') = '0'  
	   			THEN -1
				WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0')
			     AND IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0') <> '0'
				THEN 0
				ELSE #ConnectionSummary.DeliveringTeamMemberEmployeeID
				END,
	      CASE WHEN IsNull(#ConnectionSummary.DeliveringTeamMemberDimEmployeeKey,'0') = '0'  
	   			THEN ' No Lead Generator Session'
			WHEN #ConnectionSummary.DeliveringTeamMemberDimEmployeeKey <> IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0')
			     AND IsNull(#ConnectionSummary.PrimarySalesDimEmployeeKey,'0') <> '0'
			    THEN ' Delivering Team Member Not Sale Team Member'
				ELSE #ConnectionSummary.DeliveringTeamMemberName
				END

 ----- pulling in the CompSession logic which has previously been processed and stored within the Sandbox using 
 -----     the BOSS proc "proc_CompSessionDetail_PriorMonthToDate"
 -----     and then aggregated in the LTFDM_Operations proc "proc_PTTrainer_DSSR_LeadGeneratorEmployeeSummary"
 ----- and now pulling in comp product clip cards from Exerp
 
IF OBJECT_ID('tempdb.dbo.#CompSessionDetail', 'U') IS NOT NULL  
DROP TABLE #CompSessionDetail

SELECT Member.member_id AS MemberID,
       Member.dim_mms_member_key AS DimMemberKey,
       Club.club_id AS MMSClubID,
       Employee.employee_id as TrainerEmployeeID,
	   Employee.dim_employee_key AS TrainerDimEmployeeKey,
	   Employee.employee_name AS TrainerName,
	   DimDate.calendar_date AS CompSessionDate,
	   DimDate.dim_date_key AS CompSessionDateDimDateKey,
	   CASE WHEN DATEPART(year,DimDate.calendar_date)= 9999 
            THEN 0
            ELSE DATEADD(dd,14,DimDate.calendar_date) 
	        END  SaleClose14DayEndDate 
INTO #CompSessionDetail   
FROM [marketing].[v_dim_boss_reservation] Reservation
 JOIN [marketing].[v_fact_boss_daily_roster] DailyRoster
   ON DailyRoster.dim_boss_reservation_key = Reservation.dim_boss_reservation_key 
 JOIN [marketing].[v_dim_mms_member] Member
   ON DailyRoster.dim_mms_member_key = Member.dim_mms_member_key
 JOIN [marketing].[v_dim_club] Club 
   ON Reservation.dim_club_key = Club.dim_club_key
 JOIN [marketing].[v_dim_employee] Employee 
   ON Reservation.dim_employee_key = Employee.dim_employee_key
 JOIN [marketing].[v_dim_date] DimDate 
   ON DailyRoster.player_start_dim_date_key = DimDate.dim_date_key
WHERE DailyRoster.paid = 'F'    ------ Comp Session Indicator
  AND DimDate.calendar_date >= @ReportMonthStartDate  
  AND DimDate.calendar_date <= @AsOfDate
  AND Member.member_id is not null
GROUP BY Member.member_id,
  Member.dim_mms_member_key,
  Club.club_id,
  Employee.employee_id,
  Employee.dim_employee_key,
  Employee.employee_name,
  DimDate.calendar_date,
  DimDate.dim_date_key,
  CASE WHEN DATEPART(year,DimDate.calendar_date)= 9999 
     THEN 0
     ELSE DATEADD(dd,14,DimDate.calendar_date) 
	 END

UNION

SELECT Member.member_id AS MemberID,
       Member.dim_mms_member_key AS DimMemberKey,
       Club.club_id AS MMSClubID,
       Employee.employee_id as TrainerEmployeeID,
	   Employee.dim_employee_key AS TrainerDimEmployeeKey,
	   Employee.employee_name AS TrainerName,
	   DimDate.calendar_date AS CompSessionDate,
	   DimDate.dim_date_key AS CompSessionDateDimDateKey,
	   CASE WHEN DATEPART(year,DimDate.calendar_date)= 9999 
            THEN 0
            ELSE DATEADD(dd,14,DimDate.calendar_date) 
	        END  SaleClose14DayEndDate 
FROM [marketing].[v_dim_exerp_product_product_group] CompProducts
  JOIN [marketing].[v_fact_exerp_clipcard_usage] Clipcards
    ON CompProducts.dim_exerp_product_key = Clipcards.dim_exerp_product_key
   JOIN [marketing].[v_dim_mms_member] Member
    ON Clipcards.dim_mms_member_key = Member.dim_mms_member_key
   JOIN [marketing].[v_dim_employee] Employee 
    ON Clipcards.delivered_dim_employee_key = Employee.dim_employee_key
   JOIN [marketing].[v_dim_club] Club 
    ON Employee.dim_club_key = Club.dim_club_key
   JOIN [marketing].[v_dim_date] DimDate 
    ON Clipcards.usage_dim_date_key = DimDate.dim_date_key
WHERE CompProducts.product_group_name = 'Comp Products'    ------ these 2 limits define a comp product in Exerp
  AND CompProducts.primary_product_group_flag = 'Y'		   ------
  AND DimDate.calendar_date >= @ReportMonthStartDate    
  AND DimDate.calendar_date <= @AsOfDate  
  AND Member.member_id is not null
  AND Clipcards.cancelled_flag = 'N'
  AND Clipcards.clipcard_blocked_flag = 'N'
  AND Clipcards.clipcard_cancelled_flag = 'N'

 ---- To find distinct Comp session members
 IF OBJECT_ID('tempdb.dbo.#CompSessionMembers', 'U') IS NOT NULL  
DROP TABLE #CompSessionMembers

SELECT #CompSessionDetail.MemberID, 
       #CompSessionDetail.DimMemberKey
  INTO #CompSessionMembers
 FROM #CompSessionDetail 
  GROUP BY #CompSessionDetail.MemberID, 
        #CompSessionDetail.DimMemberKey

----- To find purchases by the comp session members 
----- pulling all sales since the 1st of the report month 

 IF OBJECT_ID('tempdb.dbo.#SalesDetail', 'U') IS NOT NULL  
DROP TABLE #SalesDetail

SELECT SessionMembers.MemberID,
       SessionMembers.DimMemberKey,
	   FactSalesTransaction.post_dim_date_key AS PostDimDateKey,
	   FactSalesTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
	   FactSalesTransaction.mms_tran_id AS TranID,
       FactSalesTransaction.sales_dollar_amount AS SalesDollarAmount,
	   SUM(ISNULL(FactSalesTransactionAutomatedRefund.refund_dollar_amount,0)) RelatedRefundDollarAmount,
       DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date)) RefundDaysDiff
INTO #SalesDetail    
FROM #CompSessionMembers SessionMembers  
 JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
   ON SessionMembers.DimMemberKey = FactSalesTransaction.dim_mms_member_key
 JOIN [marketing].[v_dim_date]  PostDimDate
    ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
 JOIN [marketing].[v_dim_mms_product_history] MMSProduct 
    ON FactSalesTransaction.dim_mms_product_key = MMSProduct.dim_mms_product_key
     AND MMSProduct.effective_date_time <= PostDimDate.month_ending_date
     AND MMSProduct.expiration_date_time > PostDimDate.month_ending_date
 JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
   ON MMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
     AND DimReportingHierarchy.effective_dim_date_key <= @ReportMonthEndDateDimDateKey 
	 AND  DimReportingHierarchy.expiration_dim_date_key > @ReportMonthEndDateDimDateKey
 LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key
 LEFT JOIN [marketing].[v_dim_date] RefundPostDimDate
    ON FactSalesTransactionAutomatedRefund.refund_post_dim_date_key = RefundPostDimDate.dim_date_key
   AND RefundPostDimDate.calendar_date <= DATEADD(dd,30,PostDimDate.calendar_date)
 WHERE FactSalesTransaction.primary_sales_dim_employee_key not in('-997','-998', '-999')
   AND DimReportingHierarchy.reporting_division = 'Personal Training'
   AND FactSalesTransaction.voided_flag = 'N'
   AND FactSalesTransaction.refund_flag = 'N'
   AND FactSalesTransaction.post_dim_date_key >= @ReportMonthStartDateDimDateKey
   AND FactSalesTransaction.post_dim_date_key <= @AsOfDateKey
   AND FactSalesTransaction.sales_dollar_amount > 0
   AND (FactSalesTransactionAutomatedRefund.original_fact_sales_transaction_item_key IS NULL
        OR RefundPostDimDate.dim_date_key IS NOT NULL)
 GROUP BY SessionMembers.MemberID,
       SessionMembers.DimMemberKey,
	   FactSalesTransaction.post_dim_date_key,
	   FactSalesTransaction.primary_sales_dim_employee_key,
	   FactSalesTransaction.mms_tran_id,
       FactSalesTransaction.sales_dollar_amount,
       DATEDIFF(DD,PostDimDate.calendar_date,IsNull(RefundPostDimDate.calendar_date,PostDimDate.calendar_date))



--Sum comp session related sales to transaction level
 IF OBJECT_ID('tempdb.dbo.#SalesSummary_CompSessions', 'U') IS NOT NULL  
DROP TABLE #SalesSummary_CompSessions

SELECT #SalesDetail.DimMemberKey,
       #SalesDetail.MemberID,
       #SalesDetail.PostDimDateKey,
	   SaleDate.calendar_date AS PostDimDate,
       #SalesDetail.TranID,
	   #SalesDetail.PrimarySalesDimEmployeeKey,
       SUM(CASE WHEN #SalesDetail.RefundDaysDiff <= 30
	            THEN #SalesDetail.SalesDollarAmount + #SalesDetail.RelatedRefundDollarAmount    ----- Refund amounts are stored as negative amounts in UDW
				ELSE #SalesDetail.SalesDollarAmount
				END) TotalAmount 
  INTO #SalesSummary_CompSessions
  FROM #SalesDetail 
   JOIN [marketing].[v_dim_date] SaleDate
     ON #SalesDetail.PostDimDateKey = SaleDate.dim_date_key
 GROUP BY #SalesDetail.DimMemberKey,
       #SalesDetail.MemberID,
       #SalesDetail.PostDimDateKey,
	   SaleDate.calendar_date,
       #SalesDetail.TranID,
	   #SalesDetail.PrimarySalesDimEmployeeKey

--Sum comp session detail
 IF OBJECT_ID('tempdb.dbo.#ConnectionSummary_Total', 'U') IS NOT NULL  
DROP TABLE #ConnectionSummary_Total

SELECT SessionDetail.MemberID,
       SessionDetail.MMSClubID,
       SessionDetail.TrainerEmployeeID,
	   SessionDetail.TrainerDimEmployeeKey,
	   SessionDetail.TrainerName,
	   SalesSummary.TotalAmount,
       MAX(SessionDetail.CompSessionDate) as SessionDate,
	   MAX(SessionDetail.CompSessionDateDimDateKey) as SessionDimDateKey,
	   MIN(IsNull(SalesSummary.PostDimDateKey,21000101)) as SaleDateDimDateKey,
	   SalesSummary.TranID
  INTO #ConnectionSummary_Total
 FROM #CompSessionDetail SessionDetail
  LEFT JOIN #SalesSummary_CompSessions SalesSummary
   ON SessionDetail.MemberID = SalesSummary.MemberID
   AND SessionDetail.TrainerDimEmployeeKey = SalesSummary.PrimarySalesDimEmployeeKey
   AND SessionDetail.CompSessionDateDimDateKey <= SalesSummary.PostDimDateKey
   AND SessionDetail.SaleClose14DayEndDate >= SalesSummary.PostDimDate
   GROUP BY SessionDetail.MemberID,
       SessionDetail.MMSClubID,
       SessionDetail.TrainerEmployeeID,
	   SessionDetail.TrainerDimEmployeeKey,
	   SessionDetail.TrainerName,
	   SalesSummary.TotalAmount,
	   SalesSummary.TranID


 IF OBJECT_ID('tempdb.dbo.#CompSessionSaleDetail', 'U') IS NOT NULL  
DROP TABLE #CompSessionSaleDetail

Select MemberID,
       MMSClubID,
       TrainerEmployeeID,
	   TrainerDimEmployeeKey,
	   TrainerName,
	   SUM(IsNull(TotalAmount,0)) as TotalAmount,
	   SessionDate,
	   SessionDimDateKey,
	   1 AS CompSession
 INTO #CompSessionSaleDetail
FROM #ConnectionSummary_Total
 GROUP By MemberID,
          TrainerEmployeeID,
          MMSClubID,
	   TrainerDimEmployeeKey,
	   TrainerName,
	   SessionDate,
	   SessionDimDateKey

 IF OBJECT_ID('tempdb.dbo.#CompSessionEmployeeSummary', 'U') IS NOT NULL  
DROP TABLE #CompSessionEmployeeSummary

SELECT CompSessionSaleDetail.MMSClubID,
       DimClub.dim_club_key AS DimClubKey,
       CompSessionSaleDetail.TrainerEmployeeID,
	   CompSessionSaleDetail.TrainerDimEmployeeKey,
	   CompSessionSaleDetail.TrainerName,
	   'Comp Session' AS RowLabel,
	   '4' AS RowLabelSortOrder,
	   SUM(IsNull(CompSessionSaleDetail.TotalAmount,0)) AS CompSessionSalesTotal,
	   SUM(CASE WHEN CompSessionSaleDetail.TotalAmount > 0 THEN 1 ELSE 0 END) AS CompSessionSalesCount,
	   SUM(CompSessionSaleDetail.CompSession) AS CompSessionsDelivered
  INTO #CompSessionEmployeeSummary
FROM #CompSessionSaleDetail AS CompSessionSaleDetail
 JOIN [marketing].[v_dim_club] AS DimClub
   ON CompSessionSaleDetail.MMSClubID = DimClub.club_id
   GROUP BY CompSessionSaleDetail.MMSClubID,
       DimClub.dim_club_key,
       CompSessionSaleDetail.TrainerEmployeeID,
	   CompSessionSaleDetail.TrainerDimEmployeeKey,
	   CompSessionSaleDetail.TrainerName




IF OBJECT_ID('tempdb.dbo.#FactPTDSSRLeadGeneratorEmployeeSummary', 'U') IS NOT NULL  
DROP TABLE #FactPTDSSRLeadGeneratorEmployeeSummary

CREATE TABLE #FactPTDSSRLeadGeneratorEmployeeSummary 
(
DimClubKey VARCHAR(32),
DeliveringTeammemberEmployeeID int,
DeliveringTeamMemberName varchar(200),
RowLabel varchar(100),
RowLabelSortOrder int,
NumberOfConnections int,
SalesWithin14DaysCount int,
ClosingPercent decimal(8,6),
Revenue numeric(38,2),
AvgRevenue_Sale numeric(38,6),
AvgRevenue_Connection numeric(38,6),
ReportRunDateTime varchar(21),
HeaderMemberconnectionDays int,
HeaderDateRange varchar(100),
ReportDate varchar(33),
ReportDateDimDateKey int
)


INSERT INTO #FactPTDSSRLeadGeneratorEmployeeSummary   

SELECT 
#Results.DimClubKey,
CASE When #Results.DeliveringTeamMemberEmployeeID = -1
     THEN ''
	 ELSE #Results.DeliveringTeamMemberEmployeeID
	 END DeliveringTeamMemberEmployeeID,
#Results.DeliveringTeamMemberName,	
'On Boarding' AS RowLabel,
1 AS RowLabelSortOrder,
IsNull(#MoveIt.NumberOfMembers,0) AS NumberOfConnections,
#Results.FitPointFourteenDaySalesCount AS SalesWithin14DaysCount,
CASE WHEN IsNull(#MoveIt.NumberOfMembers,0) <>0 
     THEN CAST(#Results.FitPointFourteenDaySalesCount*1.00/#MoveIt.NumberOfMembers AS DECIMAL(8,6)) 
	 ELSE 0 END	AS ClosingPercent,
#Results.FitPointTotalSales	AS Revenue,
CASE WHEN #Results.FitPointFourteenDaySalesCount <> 0 
     THEN CAST(#Results.FitPointTotalSales/#Results.FitPointFourteenDaySalesCount AS INT) 
	 ELSE 0 END AS AvgRevenue_Sale,
CASE WHEN IsNull(#MoveIt.NumberOfMembers,0) <> 0 
     THEN CAST(#Results.FitPointTotalSales/#MoveIt.NumberOfMembers AS INT) 
	 ELSE 0 END AS AvgRevenue_Connection,
@ReportRunDateTime  AS ReportRunDateTime,
@Increment AS HeaderMemberConnectionDays,
@HeaderDateRange AS HeaderDateRange,
@HeaderAsOfDate AS ReportDate,
@AsOfDateKey as ReportDateDimDateKey
FROM #Results
LEFT JOIN #MoveItNewMembersByTrainer #MoveIt
ON #Results.DeliveringTeamMemberEmployeeID = #MoveIt.DeliveringTeamMemberID
   AND #Results.DimClubKey = #MoveIt.DimClubKey

UNION


SELECT 
#Results.DimClubKey,
CASE When #Results.DeliveringTeamMemberEmployeeID = -1
     THEN ''
	 ELSE #Results.DeliveringTeamMemberEmployeeID
	 END DeliveringTeamMemberEmployeeID,
#Results.DeliveringTeamMemberName,	
'myHealthScore' AS RowLabel,
2 AS RowLabelSortOrder,
IsNull(#myHealthScore.NumberOfMembers,0) AS NumberOfConnections,
#Results.myHealthScoreFourteenDaySalesCount AS SalesWithin14DaysCount,
CASE WHEN IsNull(#myHealthScore.NumberOfMembers,0) <> 0 
     THEN CAST(#Results.myHealthScoreFourteenDaySalesCount*1.00/#myHealthScore.NumberOfMembers AS DECIMAL(8,6)) 
	 ELSE 0 END AS ClosingPercent,
#Results.myHealthScoreTotalSales AS Revenue,
CASE WHEN #Results.myHealthScoreFourteenDaySalesCount <> 0 
     THEN #Results.myHealthScoreTotalSales*1.00/#Results.myHealthScoreFourteenDaySalesCount 
	 ELSE 0 END AS AvgRevenue_Sale,
CASE WHEN IsNull(#myHealthScore.NumberOfMembers,0) <> 0 
     THEN #Results.myHealthScoreTotalSales*1.00/#myHealthScore.NumberOfMembers
	 ELSE 0 END AS AvgRevenue_Connection,
@ReportRunDateTime  AS ReportRunDateTime,
@Increment AS HeaderMemberConnectionDays,
@HeaderDateRange AS HeaderDateRange,
@HeaderAsOfDate AS ReportDate,
@AsOfDateKey as ReportDateDimDateKey

FROM #Results
LEFT JOIN #myHealthScoreNewMembersByTrainer #myHealthScore
ON #Results.DeliveringTeamMemberEmployeeID = #myHealthScore.DeliveringTeamMemberID
   AND #Results.DimClubKey = #myHealthScore.DimClubKey

UNION

SELECT 
#Results.DimClubKey,
CASE When #Results.DeliveringTeamMemberEmployeeID = -1
     THEN ''
	 ELSE #Results.DeliveringTeamMemberEmployeeID
	 END DeliveringTeamMemberEmployeeID,
#Results.DeliveringTeamMemberName,	
'myLT Bucks' AS RowLabel,
3 AS RowLabelSortOrder,
IsNull(#myLTBucks.NumberOfMembers,0) AS NumberOfConnections,
#Results.MyLTBucksFourteenDaySalesCount AS SalesWithin14DaysCount,
CASE WHEN IsNull(#myLTBucks.NumberOfMembers,0) <>0 
     THEN CAST(#Results.MyLTBucksFourteenDaySalesCount*1.00/#myLTBucks.NumberOfMembers  AS DECIMAL(8,6)) 
	 ELSE 0 END AS ClosingPercent,
#Results.MyLTBucksTotalSales AS Revenue,
CASE WHEN #Results.MyLTBucksFourteenDaySalesCount<>0 
     THEN #Results.MyLTBucksTotalSales*1.00/#Results.MyLTBucksFourteenDaySalesCount 
	 ELSE 0 END AS AvgRevenue_Sale,
CASE WHEN IsNull(#myLTBucks.NumberOfMembers,0)<>0 
     THEN #Results.MyLTBucksTotalSales*1.00/#myLTBucks.NumberOfMembers 
	 ELSE 0 END AS AvgRevenue_Connection,
@ReportRunDateTime  AS ReportRunDateTime,
@Increment AS HeaderMemberConnectionDays,
@HeaderDateRange AS HeaderDateRange,
@HeaderAsOfDate AS ReportDate,
@AsOfDateKey as ReportDateDimDateKey

FROM #Results
LEFT JOIN #myLTBuck$NewMembersByTrainer  #myLTBucks
ON #Results.DeliveringTeamMemberEmployeeID = #myLTBucks.DeliveringTeamMemberID
   AND #Results.DimClubKey = #myLTBucks.DimClubKey

   UNION

SELECT DimClubKey,
       TrainerEmployeeID AS DeliveringTeamMemberEmployeeID,
	   TrainerName AS DeliveringTeamMemberName,
	   'Comp Session' AS RowLabel,
	   '4' AS RowLabelSortOrder,
	   CompSessionsDelivered AS NumberOfConnections,
	   CompSessionSalesCount AS SalesWithin14DaysCount,
	   CASE WHEN Convert(Decimal(5,2),CompSessionSalesCount) = 0
	        THEN 0
			WHEN Convert(Decimal(5,2),CompSessionsDelivered) = 0
			THEN 0
			ELSE (Convert(Decimal(5,2),CompSessionSalesCount) / Convert(Decimal(5,2),CompSessionsDelivered))
			END ClosingPercent,
	   CompSessionSalesTotal AS Revenue,
	   CASE WHEN CompSessionSalesTotal = 0
	        THEN 0
			WHEN CompSessionSalesCount = 0
			THEN 0
			ELSE (CompSessionSalesTotal/CompSessionSalesCount) 
			END AvgRevenue_Sale,
       CASE WHEN CompSessionSalesTotal = 0
	        THEN 0
			WHEN CompSessionsDelivered = 0
			THEN 0
			ELSE (CompSessionSalesTotal/CompSessionsDelivered) 
			END AvgRevenue_Connection,
	   @ReportRunDateTime AS ReportRunDateTime,
       @Increment  AS HeaderMemberConnectionDays,
       @HeaderDateRange AS HeaderDateRange,
       @HeaderAsOfDate AS ReportDate,
	   @AsOfDateKey as ReportDateDimDateKey
FROM #CompSessionEmployeeSummary  



 ------   Delete records for 14 months prior except for the final day's records for each month
  DELETE dbo.fact_ptdssr_lead_generator_employee_summary
  WHERE report_date_dim_date_key < @FirstOf13MonthsPriorDimDateKey
    AND report_date_last_day_in_month_indicator = 'N'

 
 INSERT INTO dbo.fact_ptdssr_lead_generator_employee_summary (
 avg_revenue_connection,
 avg_revenue_sale,
 closing_percent,
 delivering_team_member_employee_id,
 delivering_team_member_name,
 dim_club_key,
 header_date_range,
 header_member_connection_days,
 number_of_connections,
 report_date,
 report_date_dim_date_key,
 report_date_last_day_in_month_indicator,
 report_run_date_time,
 revenue,
 row_label,
 row_label_sort_order,
 sales_within_14_days_count,
 dv_load_date_time,		-- need to include all dv_columns in stored procedure
 dv_load_end_date_time,	-- need to include all dv_columns in stored procedure
 dv_batch_id,			-- need to include all dv_columns in stored procedure
 dv_inserted_date_time,	-- need to include all dv_columns in stored procedure
 dv_insert_user			-- need to include all dv_columns in stored procedure
 )

 SELECT 
 AvgRevenue_Connection,
 AvgRevenue_Sale,
 ClosingPercent,
 DeliveringTeamMemberEmployeeID,
 DeliveringTeamMemberName,
 DimClubKey,
 HeaderDateRange,
 HeaderMemberConnectionDays,
 NumberOfConnections,
 ReportDate,
 ReportDateDimDateKey,
 @ReportDateLastDayInMonthIndicator,
 ReportRunDateTime,
 Revenue,
 RowLabel,
 RowLabelSortOrder,
 SalesWithin14DaysCount,
 getdate(),												--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
 convert(datetime, '99991231', 112),					--this value would be same for all the stored procedure
 '-1',													--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
 getdate(),												--this value would be same for all the stored procedure
 suser_sname()											--this value would be same for all the stored procedure

  
 FROM #FactPTDSSRLeadGeneratorEmployeeSummary


END
