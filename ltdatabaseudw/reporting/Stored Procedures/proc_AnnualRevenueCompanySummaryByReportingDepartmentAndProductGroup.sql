CREATE PROC [reporting].[proc_AnnualRevenueCompanySummaryByReportingDepartmentAndProductGroup] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@SalesSourceList [VARCHAR](4000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON


 IF 1=0 BEGIN
       SET FMTONLY OFF
     END

 ----- Execution Sample
 --- exec [reporting].[proc_AnnualRevenueCompanySummaryByReportingDepartmentAndProductGroup] '2019-12','All Departments','MMS|Hybris|Cafe'
 -----
 ----- This stored procedure is used by Report ID 119 - Annual Revenue Company Summary

 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time



DECLARE @SSSGGrandOpeningDeadlineDate DATETIME,
        @SSSGGrandOpeningDeadlineDateKey INT,
        @StartMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
        @EndMonthEndingDate DATETIME,
        @PromptMonthNumberInYear INT,
        @PriorYearStartMonthStartingDimDateKey INT,
        @PriorYearEndMonthEndingDimDateKey INT,
        @PromptYear INT,
        @PromptMonth VARCHAR(30),
        @PriorYear INT
SELECT @SSSGGrandOpeningDeadlineDate = PriorYearPromptDimDate.calendar_date,
       @SSSGGrandOpeningDeadlineDateKey = PriorYearPromptDimDate.dim_date_key,
       @StartMonthStartingDimDateKey = FirstOfYearDimDate.month_starting_dim_date_key,
       @EndMonthEndingDimDateKey = PromptDimDate.month_ending_dim_date_key,
       @EndMonthEndingDate = PromptDimDate.month_ending_date,
       @PromptMonthNumberInYear = PromptDimDate.month_number_in_year,
       @PriorYearStartMonthStartingDimDateKey = PriorFirstOfYearDimDate.month_starting_dim_date_key,
       @PriorYearEndMonthEndingDimDateKey = PriorYearPromptDimDate.month_ending_dim_date_key,
       @PromptYear = PromptDimDate.year,
       @PromptMonth = PromptDimDate.month_name,
       @PriorYear = PriorYearPromptDimDate.year
  FROM [marketing].[v_dim_date] PromptDimDate
  JOIN [marketing].[v_dim_date] FirstOfYearDimDate
    ON PromptDimDate.year = FirstOfYearDimDate.year
   AND FirstOfYearDimDate.month_number_in_year = 1
   AND FirstOfYearDimDate.day_number_in_month = 1
  JOIN [marketing].[v_dim_date] PriorYearPromptDimDate
    ON PromptDimDate.year - 1 =  PriorYearPromptDimDate.year
   AND PromptDimDate.month_number_in_year = PriorYearPromptDimDate.month_number_in_year
   AND PromptDimDate.day_number_in_month = PriorYearPromptDimDate.day_number_in_month
  JOIN [marketing].[v_dim_date] PriorFirstOfYearDimDate
    ON FirstOfYearDimDate.year - 1 = PriorFirstOfYearDimDate.year
   AND FirstOfYearDimDate.month_number_in_year = PriorFirstOfYearDimDate.month_number_in_year
   AND FirstOfYearDimDate.day_number_in_month = PriorFirstOfYearDimDate.day_number_in_month
 WHERE PromptDimDate.four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth
   AND PromptDimDate.day_number_in_month = 1

 ----- Create Sales Source temp table   
IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL
  DROP TABLE #SalesSourceList;   

DECLARE @list_table VARCHAR(100)
SET @list_table = 'sales_source_list'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT DISTINCT sales_sourceList.Item SalesSource  
  INTO #SalesSourceList
  FROM #sales_source_list  sales_sourceList

DECLARE @SalesSourceCommaList Varchar(4000)
SET @SalesSourceCommaList = Replace(@SalesSourceList,'|',', ')


------- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_history] 'N/A','N/A',@DepartmentMinDimReportingHierarchyKeyList,'N/A',@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy_Prelim', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy_Prelim; 

 SELECT DimReportingHierarchyKey,  
       --DepartmentMinDimReportingHierarchyKey,
       DivisionName,    
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType,
	   CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
		END PTDeferredRevenueProductGroupFlag
 INTO #DimReportingHierarchy_Prelim   
 FROM #OuterOutputTable

IF OBJECT_ID('tempdb.dbo.#DepartmentGrouping', 'U') IS NOT NULL
  DROP TABLE #DepartmentGrouping; 

 SELECT MIN(DimReportingHierarchyKey) AS DepartmentMinDimReportingHierarchyKey,
        DivisionName,    
        SubdivisionName,
        DepartmentName
 INTO #DepartmentGrouping
 FROM #DimReportingHierarchy_Prelim
 GROUP BY DivisionName,    
        SubdivisionName,
        DepartmentName

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

SELECT Prelim.DimReportingHierarchyKey,  
       DeptGroup.DepartmentMinDimReportingHierarchyKey,
       Prelim.DivisionName,    
       Prelim.SubdivisionName,
       Prelim.DepartmentName,
	   Prelim.ProductGroupName,
	   Prelim.RegionType,
	   Prelim.ReportRegionType,
	   Prelim.PTDeferredRevenueProductGroupFlag
INTO #DimReportingHierarchy
FROM #DimReportingHierarchy_Prelim  Prelim
 JOIN #DepartmentGrouping  DeptGroup
   ON Prelim.DivisionName = DeptGroup.DivisionName
   AND Prelim.SubdivisionName = DeptGroup.SubdivisionName
   AND Prelim.DepartmentName = DeptGroup.DepartmentName




  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) FROM [marketing].[v_dim_date] WHERE year = @PromptYear)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(dim_date_key) FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth)

DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (SELECT month_number_in_year
                    FROM [marketing].[v_dim_date] 
				   WHERE dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (SELECT prior_month_starting_dim_date_key
			        FROM [marketing].[v_dim_date] 
			        WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
            ELSE (SELECT month_starting_dim_date_key
                    FROM [marketing].[v_dim_date]  
				   WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date] 
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (Select month_number_in_year
                    FROM [marketing].[v_dim_date] 
				   WHERE dim_date_key = @EndOfReportRangeDimDateKey) in (1,4,7,10)
			THEN (SELECT prior_month_ending_dim_date_key
			        FROM [marketing].[v_dim_date]
			        WHERE dim_date_key = @EndOfReportRangeDimDateKey)
            ELSE (SELECT month_ending_dim_date_key
                    FROM [marketing].[v_dim_date]
				   WHERE dim_date_key = @EndOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT PriorYearDimDate.month_ending_dim_date_key
                                                                   FROM [marketing].[v_dim_date] DimDate
                                                                   JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                     ON DimDate.year - 1 = PriorYearDimDate.year
                                                                    AND DimDate.month_number_in_year = PriorYearDimDate.month_number_in_year
                                                                    -----AND DimDate.DayNumberInCalendarMonth = PriorYearDimDate.DayNumberInCalendarMonth  --- changed due to leap year
                                                                  WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueEndMonthEndDimDateKey
                                                                    AND PriorYearDimDate.last_day_in_month_flag = 'Y')



IF OBJECT_ID('tempdb.dbo.#PromptYearRevenueDetail', 'U') IS NOT NULL
  DROP TABLE #PromptYearRevenueDetail; 

--Revenue calcs
SELECT FactAllocatedRevenue.allocated_dim_club_key AS DimClubKey,
       DimClub.club_open_dim_date_key,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentMinDimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartment,
       #DimReportingHierarchy.ProductGroupName AS RevenueProductGroup,
       RevenuePostingMonthDimDate.month_number_in_year AS RevenueMonth,
       RevenuePostingMonthDimDate.year AS RevenueYear,
       FactAllocatedRevenue.allocated_amount AS RevenueAmount
  INTO #PromptYearRevenueDetail
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue
  JOIN [marketing].[v_dim_club] DimClub
    ON FactAllocatedRevenue.allocated_dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_date] RevenuePostingMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenuePostingMonthDimDate.dim_date_key
  JOIN #DimReportingHierarchy
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #SalesSourceList
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.SalesSource 
 WHERE FactAllocatedRevenue.transaction_dim_date_key <= @EndMonthEndingDimDateKey
   AND RevenuePostingMonthDimDate.year = @PromptYear
   AND (FactAllocatedRevenue.sales_source in('MMS','Cafe')
        OR (FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento') AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N'))   --- deferral handling only needed for e-comm transactions

   
UNION ALL

SELECT FactAllocatedRevenue.allocated_dim_club_key AS DimClubKey,
       DimClub.club_open_dim_date_key,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentMinDimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartment,
       #DimReportingHierarchy.ProductGroupName AS RevenueProductGroup,
	   CASE WHEN RevenuePostingMonthDimDate.month_number_in_year in(1,4,7,10)
	        THEN RevenuePostingMonthDimDate.month_number_in_year +1
			ELSE RevenuePostingMonthDimDate.month_number_in_year 
		END RevenueMonth,
       RevenuePostingMonthDimDate.year AS RevenueYear,
       FactAllocatedRevenue.allocated_amount AS RevenueAmount

  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue   
  JOIN [marketing].[v_dim_club] DimClub
    ON FactAllocatedRevenue.allocated_dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_date] RevenuePostingMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenuePostingMonthDimDate.dim_date_key
  JOIN #DimReportingHierarchy
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #SalesSourceList
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.SalesSource 
 WHERE FactAllocatedRevenue.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey
   AND RevenuePostingMonthDimDate.year = @PromptYear
   AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'
   AND FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento')



IF OBJECT_ID('tempdb.dbo.#ProductGroupRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupRevenueSummary; 

SELECT DimReportingHierarchyKey,
       DepartmentMinDimReportingHierarchyKey,
       RevenueReportingDepartment, 
       RevenueProductGroup,
       RevenueMonth,
       Sum(RevenueAmount) MonthAmount
  INTO #ProductGroupRevenueSummary
  FROM #PromptYearRevenueDetail
 WHERE RevenueYear = @PromptYear
 GROUP BY DimReportingHierarchyKey,
          DepartmentMinDimReportingHierarchyKey,
          RevenueReportingDepartment, 
          RevenueProductGroup,
          RevenueMonth

IF OBJECT_ID('tempdb.dbo.#DepartmentRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentRevenueSummary; 

SELECT DepartmentMinDimReportingHierarchyKey,
       RevenueReportingDepartment, 
       RevenueMonth,
       Sum(RevenueAmount) MonthAmount
  INTO #DepartmentRevenueSummary  
  FROM #PromptYearRevenueDetail
 WHERE RevenueYear = @PromptYear
 GROUP BY DepartmentMinDimReportingHierarchyKey,
          RevenueReportingDepartment, 
          RevenueMonth

IF OBJECT_ID('tempdb.dbo.#PriorYearRevenueDetail', 'U') IS NOT NULL
  DROP TABLE #PriorYearRevenueDetail; 

--Revenue calcs
SELECT FactAllocatedRevenue.allocated_dim_club_key AS DimClubKey,
       DimClub.club_open_dim_date_key,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentMinDimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartment,
       #DimReportingHierarchy.ProductGroupName AS RevenueProductGroup,
       RevenuePostingMonthDimDate.month_number_in_year AS RevenueMonth,
       RevenuePostingMonthDimDate.year AS RevenueYear,
       FactAllocatedRevenue.allocated_amount AS RevenueAmount
  INTO #PriorYearRevenueDetail
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue
  JOIN [marketing].[v_dim_club] DimClub
    ON FactAllocatedRevenue.allocated_dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_date] RevenuePostingMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenuePostingMonthDimDate.dim_date_key
  JOIN #DimReportingHierarchy
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #SalesSourceList
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.SalesSource 
 WHERE FactAllocatedRevenue.transaction_dim_date_key <= @PriorYearEndMonthEndingDimDateKey
   AND RevenuePostingMonthDimDate.year = @PriorYear
   AND (FactAllocatedRevenue.sales_source in('MMS','Cafe')
        OR (FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento') AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N'))   --- deferral handling only needed for e-comm transactions

   
UNION ALL

SELECT FactAllocatedRevenue.allocated_dim_club_key AS DimClubKey,
       DimClub.club_open_dim_date_key,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentMinDimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartment,
       #DimReportingHierarchy.ProductGroupName AS RevenueProductGroup,
	   CASE WHEN RevenuePostingMonthDimDate.month_number_in_year in(1,4,7,10)
	        THEN RevenuePostingMonthDimDate.month_number_in_year +1
			ELSE RevenuePostingMonthDimDate.month_number_in_year 
		END RevenueMonth,
       RevenuePostingMonthDimDate.year AS RevenueYear,
       FactAllocatedRevenue.allocated_amount AS RevenueAmount

  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue   
  JOIN [marketing].[v_dim_club] DimClub
    ON FactAllocatedRevenue.allocated_dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_date] RevenuePostingMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenuePostingMonthDimDate.dim_date_key
   JOIN #DimReportingHierarchy
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
   JOIN #SalesSourceList
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.SalesSource 
 WHERE FactAllocatedRevenue.transaction_dim_date_key <= @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey 
   AND RevenuePostingMonthDimDate.year = @PriorYear
   AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'
   AND FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento')

IF OBJECT_ID('tempdb.dbo.#ProductGroupSSSGPriorRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupSSSGPriorRevenueSummary; 

--SSSG calcs
SELECT #PriorYearRevenueDetail.DimReportingHierarchyKey,
       #PriorYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
       #PriorYearRevenueDetail.RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #ProductGroupSSSGPriorRevenueSummary
  FROM #PriorYearRevenueDetail

 WHERE #PriorYearRevenueDetail.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDateKey
 GROUP BY #PriorYearRevenueDetail.DimReportingHierarchyKey,
          #PriorYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
          #PriorYearRevenueDetail.RevenueMonth

IF OBJECT_ID('tempdb.dbo.#ProductGroupSSSGPromptRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupSSSGPromptRevenueSummary; 

SELECT #PromptYearRevenueDetail.DimReportingHierarchyKey,
       #PromptYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
       #PromptYearRevenueDetail.RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #ProductGroupSSSGPromptRevenueSummary
  FROM #PromptYearRevenueDetail
 WHERE #PromptYearRevenueDetail.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDateKey
 GROUP BY #PromptYearRevenueDetail.DimReportingHierarchyKey,
          #PromptYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
          #PromptYearRevenueDetail.RevenueMonth

IF OBJECT_ID('tempdb.dbo.#DepartmentSSSGPriorRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentSSSGPriorRevenueSummary; 

SELECT #PriorYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
       #PriorYearRevenueDetail.RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #DepartmentSSSGPriorRevenueSummary
  FROM #PriorYearRevenueDetail
 WHERE #PriorYearRevenueDetail.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDateKey
 GROUP BY #PriorYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
          #PriorYearRevenueDetail.RevenueMonth

IF OBJECT_ID('tempdb.dbo.#DepartmentSSSGPromptRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentSSSGPromptRevenueSummary; 

SELECT #PromptYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
       #PromptYearRevenueDetail.RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #DepartmentSSSGPromptRevenueSummary    
  FROM #PromptYearRevenueDetail
 WHERE #PromptYearRevenueDetail.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDateKey
 GROUP BY #PromptYearRevenueDetail.DepartmentMinDimReportingHierarchyKey,
          #PromptYearRevenueDetail.RevenueMonth

--YOY calcs

IF OBJECT_ID('tempdb.dbo.#ProductGroupYOYSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupYOYSummary; 

SELECT DimReportingHierarchyKey,
       DepartmentMinDimReportingHierarchyKey,
       RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #ProductGroupYOYSummary
  FROM #PriorYearRevenueDetail
 GROUP BY DimReportingHierarchyKey,
          DepartmentMinDimReportingHierarchyKey,
          RevenueMonth

IF OBJECT_ID('tempdb.dbo.#DepartmentYOYSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentYOYSummary; 

SELECT DepartmentMinDimReportingHierarchyKey,
       RevenueMonth,
       SUM(RevenueAmount) ActualAmount
  INTO #DepartmentYOYSummary
  FROM #PriorYearRevenueDetail
 GROUP BY DepartmentMinDimReportingHierarchyKey,
          RevenueMonth   

IF OBJECT_ID('tempdb.dbo.#GoalDetail', 'U') IS NOT NULL
  DROP TABLE #GoalDetail;

--Goal calcs
SELECT #DimReportingHierarchy.DimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentMinDimReportingHierarchyKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartment,
       #DimReportingHierarchy.ProductGroupName AS RevenueProductGroup,
       GoalEffectiveDimDate.month_number_in_year AS GoalMonthNumber,
       FactGoal.goal_dollar_amount AS GoalDollarAmount
  INTO #GoalDetail
  FROM [marketing].[v_fact_revenue_goal] FactGoal
  JOIN #DimReportingHierarchy
    ON FactGoal.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN [marketing].[v_dim_date] GoalEffectiveDimDate
    ON FactGoal.goal_effective_dim_date_key = GoalEffectiveDimDate.dim_date_key
 WHERE GoalEffectiveDimDate.year = @PromptYear

IF OBJECT_ID('tempdb.dbo.#ProductGroupGoalSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupGoalSummary;

SELECT DimReportingHierarchyKey,
       DepartmentMinDimReportingHierarchyKey,
       RevenueReportingDepartment, 
       RevenueProductGroup,
       GoalMonthNumber,
       SUM(GoalDollarAmount) MonthGoal
  INTO #ProductGroupGoalSummary
  FROM #GoalDetail
 GROUP BY DimReportingHierarchyKey,
          DepartmentMinDimReportingHierarchyKey,
          RevenueReportingDepartment, 
          RevenueProductGroup,
          GoalMonthNumber

IF OBJECT_ID('tempdb.dbo.#DepartmentGoalSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentGoalSummary;

SELECT DepartmentMinDimReportingHierarchyKey,
       RevenueReportingDepartment,
       GoalMonthNumber,
       SUM(GoalDollarAmount) MonthGoal
  INTO #DepartmentGoalSummary
  FROM #GoalDetail
 GROUP BY DepartmentMinDimReportingHierarchyKey,
          RevenueReportingDepartment, 
          GoalMonthNumber       

IF OBJECT_ID('tempdb.dbo.#ProductGroupDetailSummary', 'U') IS NOT NULL
  DROP TABLE #ProductGroupDetailSummary;

--Put it all together
SELECT ISNULL(#ProductGroupGoalSummary.RevenueReportingDepartment,#ProductGroupRevenueSummary.RevenueReportingDepartment) RevenueReportingDepartment,
       ISNULL(#ProductGroupGoalSummary.RevenueProductGroup,#ProductGroupRevenueSummary.RevenueProductGroup) RevenueProductGroup,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 1 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) JanuaryActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 2 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) FebruaryActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 3 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) MarchActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 4 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) AprilActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 5 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) MayActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 6 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) JuneActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 7 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) JulyActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 8 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) AugustActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 9 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) SeptemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 10 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) OctoberActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 11 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) NovemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth = 12 THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) DecemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #ProductGroupRevenueSummary.MonthAmount ELSE 0 END),0) ThroughMonthActual,
       IsNull(Sum(#ProductGroupRevenueSummary.MonthAmount),0) AnnualActual,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 1 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) JanuaryGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 2 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) FebruaryGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 3 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) MarchGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 4 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) AprilGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 5 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) MayGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 6 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) JuneGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 7 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) JulyGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 8 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) AugustGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 9 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) SeptemberGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 10 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) OctoberGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 11 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) NovemberGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber = 12 THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) DecemberGoal,
       Sum(CASE WHEN #ProductGroupGoalSummary.GoalMonthNumber <= @PromptMonthNumberInYear THEN #ProductGroupGoalSummary.MonthGoal ELSE 0 END) ThroughMonthGoal,
       Sum(#ProductGroupGoalSummary.MonthGoal) AnnualGoal,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 1 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJanuaryPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 2 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGFebruaryPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 3 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMarchPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 4 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAprilPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 5 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMayPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 6 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJunePriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 7 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJulyPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 8 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAugustPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 9 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGSeptemberPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 10 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGOctoberPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 11 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGNovemberPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth = 12 THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGDecemberPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPriorRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #ProductGroupSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGThroughMonthPriorActual,
       IsNull(Sum(#ProductGroupSSSGPriorRevenueSummary.ActualAmount),0) SSSGAnnualPriorActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 1 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJanuaryPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 2 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGFebruaryPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 3 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMarchPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 4 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAprilPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 5 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMayPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 6 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJunePromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 7 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJulyPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 8 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAugustPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 9 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGSeptemberPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 10 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGOctoberPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 11 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGNovemberPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth = 12 THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGDecemberPromptActual,
       IsNull(Sum(CASE WHEN #ProductGroupSSSGPromptRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #ProductGroupSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGThroughMonthPromptActual,
       IsNull(Sum(#ProductGroupSSSGPromptRevenueSummary.ActualAmount),0) SSSGAnnualPromptActual,        
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 1 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYJanuaryActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 2 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYFebruaryActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 3 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYMarchActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 4 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYAprilActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 5 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYMayActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 6 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYJuneActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 7 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYJulyActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 8 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYAugustActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 9 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYSeptemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 10 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYOctoberActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 11 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYNovemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth = 12 THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYDecemberActual,
       IsNull(Sum(CASE WHEN #ProductGroupYOYSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #ProductGroupYOYSummary.ActualAmount ELSE 0 END),0) YOYThroughMonthActual,
       IsNull(Sum(#ProductGroupYOYSummary.ActualAmount),0) YOYAnnualActual
  INTO #ProductGroupDetailSummary      
  FROM #ProductGroupGoalSummary
  FULL OUTER JOIN #ProductGroupRevenueSummary
    ON #ProductGroupGoalSummary.DimReportingHierarchyKey = #ProductGroupRevenueSummary.DimReportingHierarchyKey
   AND #ProductGroupGoalSummary.GoalMonthNumber = #ProductGroupRevenueSummary.RevenueMonth
  LEFT JOIN #ProductGroupSSSGPriorRevenueSummary
    ON ISNULL(#ProductGroupGoalSummary.DimReportingHierarchyKey,#ProductGroupRevenueSummary.DimReportingHierarchyKey) = #ProductGroupSSSGPriorRevenueSummary.DimReportingHierarchyKey
   AND ISNULL(#ProductGroupGoalSummary.GoalMonthNumber,#ProductGroupRevenueSummary.RevenueMonth) = #ProductGroupSSSGPriorRevenueSummary.RevenueMonth
  LEFT JOIN #ProductGroupSSSGPromptRevenueSummary
    ON ISNULL(#ProductGroupGoalSummary.DimReportingHierarchyKey,#ProductGroupRevenueSummary.DimReportingHierarchyKey) = #ProductGroupSSSGPromptRevenueSummary.DimReportingHierarchyKey
   AND ISNULL(#ProductGroupGoalSummary.GoalMonthNumber,#ProductGroupRevenueSummary.RevenueMonth) = #ProductGroupSSSGPromptRevenueSummary.RevenueMonth                                                  
  LEFT JOIN #ProductGroupYOYSummary
    ON ISNULL(#ProductGroupGoalSummary.DimReportingHierarchyKey,#ProductGroupRevenueSummary.DimReportingHierarchyKey) = #ProductGroupYOYSummary.DimReportingHierarchyKey
   AND ISNULL(#ProductGroupGoalSummary.GoalMonthNumber,#ProductGroupRevenueSummary.RevenueMonth) = #ProductGroupYOYSummary.RevenueMonth
 GROUP BY ISNULL(#ProductGroupGoalSummary.RevenueReportingDepartment,#ProductGroupRevenueSummary.RevenueReportingDepartment),
          ISNULL(#ProductGroupGoalSummary.RevenueProductGroup,#ProductGroupRevenueSummary.RevenueProductGroup)    

IF OBJECT_ID('tempdb.dbo.#DepartmentDetailSummary', 'U') IS NOT NULL
  DROP TABLE #DepartmentDetailSummary;

SELECT ISNULL(#DepartmentGoalSummary.RevenueReportingDepartment,#DepartmentRevenueSummary.RevenueReportingDepartment) RevenueReportingDepartment,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 1 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) JanuaryActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 2 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) FebruaryActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 3 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) MarchActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 4 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) AprilActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 5 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) MayActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 6 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) JuneActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 7 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) JulyActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 8 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) AugustActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 9 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) SeptemberActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 10 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) OctoberActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 11 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) NovemberActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth = 12 THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) DecemberActual,
       IsNull(Sum(CASE WHEN #DepartmentRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #DepartmentRevenueSummary.MonthAmount ELSE 0 END),0) ThroughMonthActual,
       IsNull(Sum(#DepartmentRevenueSummary.MonthAmount),0) AnnualActual,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 1 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) JanuaryGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 2 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) FebruaryGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 3 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) MarchGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 4 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) AprilGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 5 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) MayGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 6 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) JuneGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 7 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) JulyGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 8 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) AugustGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 9 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) SeptemberGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 10 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) OctoberGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 11 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) NovemberGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber = 12 THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) DecemberGoal,
       Sum(CASE WHEN #DepartmentGoalSummary.GoalMonthNumber <= @PromptMonthNumberInYear THEN #DepartmentGoalSummary.MonthGoal ELSE 0 END) ThroughMonthGoal,
       Sum(#DepartmentGoalSummary.MonthGoal) AnnualGoal,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 1 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJanuaryPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 2 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGFebruaryPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 3 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMarchPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 4 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAprilPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 5 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMayPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 6 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJunePriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 7 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJulyPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 8 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAugustPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 9 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGSeptemberPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 10 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGOctoberPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 11 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGNovemberPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth = 12 THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGDecemberPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPriorRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #DepartmentSSSGPriorRevenueSummary.ActualAmount ELSE 0 END),0) SSSGThroughMonthPriorActual,
       IsNull(Sum(#DepartmentSSSGPriorRevenueSummary.ActualAmount),0) SSSGAnnualPriorActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 1 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJanuaryPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 2 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGFebruaryPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 3 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMarchPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 4 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAprilPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 5 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGMayPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 6 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJunePromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 7 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGJulyPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 8 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGAugustPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 9 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGSeptemberPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 10 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGOctoberPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 11 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGNovemberPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth = 12 THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGDecemberPromptActual,
       IsNull(Sum(CASE WHEN #DepartmentSSSGPromptRevenueSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #DepartmentSSSGPromptRevenueSummary.ActualAmount ELSE 0 END),0) SSSGThroughMonthPromptActual,
       IsNull(Sum(#DepartmentSSSGPromptRevenueSummary.ActualAmount),0) SSSGAnnualPromptActual,  
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 1 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYJanuaryActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 2 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYFebruaryActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 3 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYMarchActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 4 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYAprilActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 5 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYMayActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 6 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYJuneActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 7 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYJulyActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 8 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYAugustActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 9 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYSeptemberActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 10 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYOctoberActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 11 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYNovemberActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth = 12 THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYDecemberActual,
       IsNull(Sum(CASE WHEN #DepartmentYOYSummary.RevenueMonth <= @PromptMonthNumberInYear THEN #DepartmentYOYSummary.ActualAmount ELSE 0 END),0) YOYThroughMonthActual,
       IsNull(Sum(#DepartmentYOYSummary.ActualAmount),0) YOYAnnualActual
  INTO #DepartmentDetailSummary
  FROM #DepartmentGoalSummary
  FULL OUTER JOIN #DepartmentRevenueSummary
    ON #DepartmentGoalSummary.DepartmentMinDimReportingHierarchyKey = #DepartmentRevenueSummary.DepartmentMinDimReportingHierarchyKey
   AND #DepartmentGoalSummary.GoalMonthNumber = #DepartmentRevenueSummary.RevenueMonth
  LEFT JOIN #DepartmentSSSGPriorRevenueSummary
    ON ISNULL(#DepartmentGoalSummary.DepartmentMinDimReportingHierarchyKey,#DepartmentRevenueSummary.DepartmentMinDimReportingHierarchyKey) = #DepartmentSSSGPriorRevenueSummary.DepartmentMinDimReportingHierarchyKey
   AND ISNULL(#DepartmentGoalSummary.GoalMonthNumber,#DepartmentRevenueSummary.RevenueMonth) = #DepartmentSSSGPriorRevenueSummary.RevenueMonth
  LEFT JOIN #DepartmentSSSGPromptRevenueSummary
    ON ISNULL(#DepartmentGoalSummary.DepartmentMinDimReportingHierarchyKey,#DepartmentRevenueSummary.DepartmentMinDimReportingHierarchyKey)= #DepartmentSSSGPromptRevenueSummary.DepartmentMinDimReportingHierarchyKey 
   AND ISNULL(#DepartmentGoalSummary.GoalMonthNumber,#DepartmentRevenueSummary.RevenueMonth) = #DepartmentSSSGPromptRevenueSummary.RevenueMonth
  LEFT JOIN #DepartmentYOYSummary
    ON ISNULL(#DepartmentGoalSummary.DepartmentMinDimReportingHierarchyKey,#DepartmentRevenueSummary.DepartmentMinDimReportingHierarchyKey) = #DepartmentYOYSummary.DepartmentMinDimReportingHierarchyKey
   AND ISNULL(#DepartmentGoalSummary.GoalMonthNumber,#DepartmentRevenueSummary.RevenueMonth) = #DepartmentYOYSummary.RevenueMonth
 GROUP BY ISNULL(#DepartmentGoalSummary.RevenueReportingDepartment,#DepartmentRevenueSummary.RevenueReportingDepartment)     

--Result set         
SELECT 'Actual Revenue:' CategoryActual,
       Cast(JanuaryActual as Decimal(10,0)) JanuaryActual,
       Cast(FebruaryActual as Decimal(10,0)) FebruaryActual,
       Cast(MarchActual as Decimal(10,0)) MarchActual,
       Cast(AprilActual as Decimal(10,0)) AprilActual,
       Cast(MayActual as Decimal(10,0)) MayActual,
       Cast(JuneActual as Decimal(10,0)) JuneActual,
       Cast(JulyActual as Decimal(10,0)) JulyActual,
       Cast(AugustActual as Decimal(10,0)) AugustActual,
       Cast(SeptemberActual as Decimal(10,0)) SeptemberActual,
       Cast(OctoberActual as Decimal(10,0)) OctoberActual,
       Cast(NovemberActual as Decimal(10,0)) NovemberActual,
       Cast(DecemberActual as Decimal(10,0)) DecemberActual,
       Cast(AnnualActual as Decimal(10,0)) AnnualTotalActual,
       Cast(ThroughMonthActual as Decimal(10,0)) ThroughMonthActual,
       @PromptYear PromptYear,
       @PromptMonth PromptMonth,
       'Revenue Goal:' CategoryGoal,
       Cast(JanuaryGoal as Decimal(10,0)) JanuaryGoal,
       Cast(FebruaryGoal as Decimal(10,0)) FebruaryGoal,
       Cast(MarchGoal as Decimal(10,0)) MarchGoal,
       Cast(AprilGoal as Decimal(10,0)) AprilGoal,
       Cast(MayGoal as Decimal(10,0)) MayGoal,
       Cast(JuneGoal as Decimal(10,0)) JuneGoal,
       Cast(JulyGoal as Decimal(10,0)) JulyGoal,
       Cast(AugustGoal as Decimal(10,0)) AugustGoal,
       Cast(SeptemberGoal as Decimal(10,0)) SeptemberGoal,
       Cast(OctoberGoal as Decimal(10,0)) OctoberGoal,
       Cast(NovemberGoal as Decimal(10,0)) NovemberGoal,
       Cast(DecemberGoal as Decimal(10,0)) DecemberGoal,
       Cast(AnnualGoal as Decimal(10,0)) AnnualTotalGoal,
       Cast(ThroughMonthGoal as Decimal(10,0)) ThroughMonthGoal,
       '% of Goal:' CategoryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN JanuaryGoal = 0 THEN 0 ELSE JanuaryActual/JanuaryGoal END as DECIMAL(11,1)) as Varchar) + '%' JanuaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN FebruaryGoal = 0 THEN 0 ELSE FebruaryActual/FebruaryGoal END as DECIMAL(11,1)) as Varchar) + '%' FebruaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN MarchGoal = 0 THEN 0 ELSE MarchActual/MarchGoal END as DECIMAL(11,1)) as Varchar) + '%' MarchPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AprilGoal = 0 THEN 0 ELSE AprilActual/AprilGoal END as DECIMAL(11,1)) as Varchar) + '%' AprilPercentOfGoal,
       Cast(Cast(100 * CASE WHEN MayGoal = 0 THEN 0 ELSE MayActual/MayGoal END as DECIMAL(11,1)) as Varchar) + '%' MayPercentOfGoal,
       Cast(Cast(100 * CASE WHEN JuneGoal = 0 THEN 0 ELSE JuneActual/JuneGoal END as DECIMAL(11,1)) as Varchar) + '%' JunePercentOfGoal,
       Cast(Cast(100 * CASE WHEN JulyGoal = 0 THEN 0 ELSE JulyActual/JulyGoal END as DECIMAL(11,1)) as Varchar) + '%' JulyPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AugustGoal = 0 THEN 0 ELSE AugustActual/AugustGoal END as DECIMAL(11,1)) as Varchar) + '%' AugustPercentOfGoal,
       Cast(Cast(100 * CASE WHEN SeptemberGoal = 0 THEN 0 ELSE SeptemberActual/SeptemberGoal END as DECIMAL(11,1)) as Varchar) + '%' SeptemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN OctoberGoal = 0 THEN 0 ELSE OctoberActual/OctoberGoal END as DECIMAL(11,1)) as Varchar) + '%' OctoberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN NovemberGoal = 0 THEN 0 ELSE NovemberActual/NovemberGoal END as DECIMAL(11,1)) as Varchar) + '%' NovemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN DecemberGoal = 0 THEN 0 ELSE DecemberActual/DecemberGoal END as DECIMAL(11,1)) as Varchar) + '%' DecemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AnnualGoal = 0 THEN 0 ELSE AnnualActual/AnnualGoal END as DECIMAL(11,1)) as Varchar) + '%' AnnualTotalPercentOfGoal,
       Cast(Cast(100 * CASE WHEN ThroughMonthGoal = 0 THEN 0 ELSE ThroughMonthActual/ThroughMonthGoal END as DECIMAL(11,1)) as Varchar) + '%' ThroughMonthPercentOfGoal,       
       'SSSG %:' CategorySSSG,
       Cast(CAST(100 * CASE WHEN SSSGJanuaryPriorActual = 0 THEN NULL ELSE (SSSGJanuaryPromptActual - SSSGJanuaryPriorActual)/SSSGJanuaryPriorActual End as Decimal(11,1)) as Varchar) + '%' JanuarySSSG,
       Cast(CAST(100 * CASE WHEN SSSGFebruaryPriorActual = 0 THEN NULL ELSE (SSSGFebruaryPromptActual - SSSGFebruaryPriorActual)/SSSGFebruaryPriorActual End as Decimal(11,1)) as Varchar) + '%' FebruarySSSG,
       Cast(CAST(100 * CASE WHEN SSSGMarchPriorActual = 0 THEN NULL ELSE (SSSGMarchPromptActual - SSSGMarchPriorActual)/SSSGMarchPriorActual End as Decimal(11,1)) as Varchar) + '%' MarchSSSG,
       Cast(CAST(100 * CASE WHEN SSSGAprilPriorActual = 0 THEN NULL ELSE (SSSGAprilPromptActual - SSSGAprilPriorActual)/SSSGAprilPriorActual End as Decimal(11,1)) as Varchar) + '%' AprilSSSG,
       Cast(CAST(100 * CASE WHEN SSSGMayPriorActual = 0 THEN NULL ELSE (SSSGMayPromptActual - SSSGMayPriorActual)/SSSGMayPriorActual End as Decimal(11,1)) as Varchar) + '%' MaySSSG,
       Cast(CAST(100 * CASE WHEN SSSGJunePriorActual = 0 THEN NULL ELSE (SSSGJunePromptActual - SSSGJunePriorActual)/SSSGJunePriorActual End as Decimal(11,1)) as Varchar) + '%' JuneSSSG,
       Cast(CAST(100 * CASE WHEN SSSGJulyPriorActual = 0 THEN NULL ELSE (SSSGJulyPromptActual - SSSGJulyPriorActual)/SSSGJulyPriorActual End as Decimal(11,1)) as Varchar) + '%' JulySSSG,
       Cast(CAST(100 * CASE WHEN SSSGAugustPriorActual = 0 THEN NULL ELSE (SSSGAugustPromptActual - SSSGAugustPriorActual)/SSSGAugustPriorActual End as Decimal(11,1)) as Varchar) + '%' AugustSSSG,
       Cast(CAST(100 * CASE WHEN SSSGSeptemberPriorActual = 0 THEN NULL ELSE (SSSGSeptemberPromptActual - SSSGSeptemberPriorActual)/SSSGSeptemberPriorActual End as Decimal(11,1)) as Varchar) + '%' SeptemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGOctoberPriorActual = 0 THEN NULL ELSE (SSSGOctoberPromptActual - SSSGOctoberPriorActual)/SSSGOctoberPriorActual End as Decimal(11,1)) as Varchar) + '%' OctoberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGNovemberPriorActual = 0 THEN NULL ELSE (SSSGNovemberPromptActual - SSSGNovemberPriorActual)/SSSGNovemberPriorActual End as Decimal(11,1)) as Varchar) + '%' NovemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGDecemberPriorActual = 0 THEN NULL ELSE (SSSGDecemberPromptActual - SSSGDecemberPriorActual)/SSSGDecemberPriorActual End as Decimal(11,1)) as Varchar) + '%' DecemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGAnnualPriorActual = 0 THEN NULL ELSE (SSSGAnnualPromptActual - SSSGAnnualPriorActual)/SSSGAnnualPriorActual End as Decimal(11,1)) as Varchar) + '%' AnnualTotalSSSG,
       Cast(CAST(100 * CASE WHEN SSSGThroughMonthPriorActual = 0 THEN NULL ELSE (SSSGThroughMonthPromptActual - SSSGThroughMonthPriorActual)/SSSGThroughMonthPriorActual End as Decimal(11,1)) as Varchar) + '%' ThroughMonthSSSG,
       'YOY %:' CategoryYOY,
       Cast(CAST(100 * CASE WHEN YOYJanuaryActual = 0 THEN NULL ELSE (JanuaryActual - YOYJanuaryActual)/YOYJanuaryActual End as Decimal(11,1)) as Varchar) + '%' JanuaryYOY,
       Cast(CAST(100 * CASE WHEN YOYFebruaryActual = 0 THEN NULL ELSE (FebruaryActual - YOYFebruaryActual)/YOYFebruaryActual End as Decimal(11,1)) as Varchar) + '%' FebruaryYOY,
       Cast(CAST(100 * CASE WHEN YOYMarchActual = 0 THEN NULL ELSE (MarchActual - YOYMarchActual)/YOYMarchActual End as Decimal(11,1)) as Varchar) + '%' MarchYOY,
       Cast(CAST(100 * CASE WHEN YOYAprilActual = 0 THEN NULL ELSE (AprilActual - YOYAprilActual)/YOYAprilActual End as Decimal(11,1)) as Varchar) + '%' AprilYOY,
       Cast(CAST(100 * CASE WHEN YOYMayActual = 0 THEN NULL ELSE (MayActual - YOYMayActual)/YOYMayActual End as Decimal(11,1)) as Varchar) + '%' MayYOY,
       Cast(CAST(100 * CASE WHEN YOYJuneActual = 0 THEN NULL ELSE (JuneActual - YOYJuneActual)/YOYJuneActual End as Decimal(11,1)) as Varchar) + '%' JuneYOY,
       Cast(CAST(100 * CASE WHEN YOYJulyActual = 0 THEN NULL ELSE (JulyActual - YOYJulyActual)/YOYJulyActual End as Decimal(11,1)) as Varchar) + '%' JulyYOY,
       Cast(CAST(100 * CASE WHEN YOYAugustActual = 0 THEN NULL ELSE (AugustActual - YOYAugustActual)/YOYAugustActual End as Decimal(11,1)) as Varchar) + '%' AugustYOY,
       Cast(CAST(100 * CASE WHEN YOYSeptemberActual = 0 THEN NULL ELSE (SeptemberActual - YOYSeptemberActual)/YOYSeptemberActual End as Decimal(11,1)) as Varchar) + '%' SeptemberYOY,
       Cast(CAST(100 * CASE WHEN YOYOctoberActual = 0 THEN NULL ELSE (OctoberActual - YOYOctoberActual)/YOYOctoberActual End as Decimal(11,1)) as Varchar) + '%' OctoberYOY,
       Cast(CAST(100 * CASE WHEN YOYNovemberActual = 0 THEN NULL ELSE (NovemberActual - YOYNovemberActual)/YOYNovemberActual End as Decimal(11,1)) as Varchar) + '%' NovemberYOY,
       Cast(CAST(100 * CASE WHEN YOYDecemberActual = 0 THEN NULL ELSE (DecemberActual - YOYDecemberActual)/YOYDecemberActual End as Decimal(11,1)) as Varchar) + '%' DecemberYOY,
       Cast(CAST(100 * CASE WHEN YOYAnnualActual = 0 THEN NULL ELSE (AnnualActual - YOYAnnualActual)/YOYAnnualActual End as Decimal(11,1)) as Varchar) + '%' AnnualYOY,
       Cast(CAST(100 * CASE WHEN YOYThroughMonthActual = 0 THEN NULL ELSE (ThroughMonthActual - YOYThroughMonthActual)/YOYThroughMonthActual End as Decimal(11,1)) as Varchar) + '%' ThroughMonthYOY,       
       NULL  AS RevenueReportingDepartmentNameCommaList,   ------- @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,  must be created in Cognos  
       @SalesSourceCommaList SalesSourceCommaList,
       @ReportRunDateTime ReportRunDateTime,
       RevenueProductGroup RowLabel,
       'Report-'+RevenueReportingDepartment+'-'+RevenueProductGroup ReportSort,
       'N' TotalBorderFlag,
       NULL  AS HeaderDivisionList,  ------ @HeaderDivisionList  Must create this in Cognos
       NULL  AS HeaderSubdivisionList ------  @HeaderSubdivisionList Must create this in Cognos
FROM #ProductGroupDetailSummary         

UNION ALL

SELECT 'Actual Revenue:' CategoryActual,
       Cast(JanuaryActual as Decimal(10,0)) JanuaryActual,
       Cast(FebruaryActual as Decimal(10,0)) FebruaryActual,
       Cast(MarchActual as Decimal(10,0)) MarchActual,
       Cast(AprilActual as Decimal(10,0)) AprilActual,
       Cast(MayActual as Decimal(10,0)) MayActual,
       Cast(JuneActual as Decimal(10,0)) JuneActual,
       Cast(JulyActual as Decimal(10,0)) JulyActual,
       Cast(AugustActual as Decimal(10,0)) AugustActual,
       Cast(SeptemberActual as Decimal(10,0)) SeptemberActual,
       Cast(OctoberActual as Decimal(10,0)) OctoberActual,
       Cast(NovemberActual as Decimal(10,0)) NovemberActual,
       Cast(DecemberActual as Decimal(10,0)) DecemberActual,
       Cast(AnnualActual as Decimal(10,0)) AnnualTotalActual,
       Cast(ThroughMonthActual as Decimal(10,0)) ThroughMonthActual,
       @PromptYear PromptYear,
       @PromptMonth PromptMonth,
       'Revenue Goal:' CategoryGoal,
       Cast(JanuaryGoal as Decimal(10,0)) JanuaryGoal,
       Cast(FebruaryGoal as Decimal(10,0)) FebruaryGoal,
       Cast(MarchGoal as Decimal(10,0)) MarchGoal,
       Cast(AprilGoal as Decimal(10,0)) AprilGoal,
       Cast(MayGoal as Decimal(10,0)) MayGoal,
       Cast(JuneGoal as Decimal(10,0)) JuneGoal,
       Cast(JulyGoal as Decimal(10,0)) JulyGoal,
       Cast(AugustGoal as Decimal(10,0)) AugustGoal,
       Cast(SeptemberGoal as Decimal(10,0)) SeptemberGoal,
       Cast(OctoberGoal as Decimal(10,0)) OctoberGoal,
       Cast(NovemberGoal as Decimal(10,0)) NovemberGoal,
       Cast(DecemberGoal as Decimal(10,0)) DecemberGoal,
       Cast(AnnualGoal as Decimal(10,0)) AnnualTotalGoal,
       Cast(ThroughMonthGoal as Decimal(10,0)) ThroughMonthGoal,
       '% of Goal:' CategoryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN JanuaryGoal = 0 THEN 0 ELSE JanuaryActual/JanuaryGoal END as DECIMAL(11,1)) as Varchar) + '%' JanuaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN FebruaryGoal = 0 THEN 0 ELSE FebruaryActual/FebruaryGoal END as DECIMAL(11,1)) as Varchar) + '%' FebruaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN MarchGoal = 0 THEN 0 ELSE MarchActual/MarchGoal END as DECIMAL(11,1)) as Varchar) + '%' MarchPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AprilGoal = 0 THEN 0 ELSE AprilActual/AprilGoal END as DECIMAL(11,1)) as Varchar) + '%' AprilPercentOfGoal,
       Cast(Cast(100 * CASE WHEN MayGoal = 0 THEN 0 ELSE MayActual/MayGoal END as DECIMAL(11,1)) as Varchar) + '%' MayPercentOfGoal,
       Cast(Cast(100 * CASE WHEN JuneGoal = 0 THEN 0 ELSE JuneActual/JuneGoal END as DECIMAL(11,1)) as Varchar) + '%' JunePercentOfGoal,
       Cast(Cast(100 * CASE WHEN JulyGoal = 0 THEN 0 ELSE JulyActual/JulyGoal END as DECIMAL(11,1)) as Varchar) + '%' JulyPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AugustGoal = 0 THEN 0 ELSE AugustActual/AugustGoal END as DECIMAL(11,1)) as Varchar) + '%' AugustPercentOfGoal,
       Cast(Cast(100 * CASE WHEN SeptemberGoal = 0 THEN 0 ELSE SeptemberActual/SeptemberGoal END as DECIMAL(11,1)) as Varchar) + '%' SeptemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN OctoberGoal = 0 THEN 0 ELSE OctoberActual/OctoberGoal END as DECIMAL(11,1)) as Varchar) + '%' OctoberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN NovemberGoal = 0 THEN 0 ELSE NovemberActual/NovemberGoal END as DECIMAL(11,1)) as Varchar) + '%' NovemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN DecemberGoal = 0 THEN 0 ELSE DecemberActual/DecemberGoal END as DECIMAL(11,1)) as Varchar) + '%' DecemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN AnnualGoal = 0 THEN 0 ELSE AnnualActual/AnnualGoal END as DECIMAL(11,1)) as Varchar) + '%' AnnualTotalPercentOfGoal,
       Cast(Cast(100 * CASE WHEN ThroughMonthGoal = 0 THEN 0 ELSE ThroughMonthActual/ThroughMonthGoal END as DECIMAL(11,1)) as Varchar) + '%' ThroughMonthPercentOfGoal,       
       'SSSG %:' CategorySSSG,
       Cast(CAST(100 * CASE WHEN SSSGJanuaryPriorActual = 0 THEN NULL ELSE (SSSGJanuaryPromptActual - SSSGJanuaryPriorActual)/SSSGJanuaryPriorActual End as Decimal(11,1)) as Varchar) + '%' JanuarySSSG,
       Cast(CAST(100 * CASE WHEN SSSGFebruaryPriorActual = 0 THEN NULL ELSE (SSSGFebruaryPromptActual - SSSGFebruaryPriorActual)/SSSGFebruaryPriorActual End as Decimal(11,1)) as Varchar) + '%' FebruarySSSG,
       Cast(CAST(100 * CASE WHEN SSSGMarchPriorActual = 0 THEN NULL ELSE (SSSGMarchPromptActual - SSSGMarchPriorActual)/SSSGMarchPriorActual End as Decimal(11,1)) as Varchar) + '%' MarchSSSG,
       Cast(CAST(100 * CASE WHEN SSSGAprilPriorActual = 0 THEN NULL ELSE (SSSGAprilPromptActual - SSSGAprilPriorActual)/SSSGAprilPriorActual End as Decimal(11,1)) as Varchar) + '%' AprilSSSG,
       Cast(CAST(100 * CASE WHEN SSSGMayPriorActual = 0 THEN NULL ELSE (SSSGMayPromptActual - SSSGMayPriorActual)/SSSGMayPriorActual End as Decimal(11,1)) as Varchar) + '%' MaySSSG,
       Cast(CAST(100 * CASE WHEN SSSGJunePriorActual = 0 THEN NULL ELSE (SSSGJunePromptActual - SSSGJunePriorActual)/SSSGJunePriorActual End as Decimal(11,1)) as Varchar) + '%' JuneSSSG,
       Cast(CAST(100 * CASE WHEN SSSGJulyPriorActual = 0 THEN NULL ELSE (SSSGJulyPromptActual - SSSGJulyPriorActual)/SSSGJulyPriorActual End as Decimal(11,1)) as Varchar) + '%' JulySSSG,
       Cast(CAST(100 * CASE WHEN SSSGAugustPriorActual = 0 THEN NULL ELSE (SSSGAugustPromptActual - SSSGAugustPriorActual)/SSSGAugustPriorActual End as Decimal(11,1)) as Varchar) + '%' AugustSSSG,
       Cast(CAST(100 * CASE WHEN SSSGSeptemberPriorActual = 0 THEN NULL ELSE (SSSGSeptemberPromptActual - SSSGSeptemberPriorActual)/SSSGSeptemberPriorActual End as Decimal(11,1)) as Varchar) + '%' SeptemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGOctoberPriorActual = 0 THEN NULL ELSE (SSSGOctoberPromptActual - SSSGOctoberPriorActual)/SSSGOctoberPriorActual End as Decimal(11,1)) as Varchar) + '%' OctoberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGNovemberPriorActual = 0 THEN NULL ELSE (SSSGNovemberPromptActual - SSSGNovemberPriorActual)/SSSGNovemberPriorActual End as Decimal(11,1)) as Varchar) + '%' NovemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGDecemberPriorActual = 0 THEN NULL ELSE (SSSGDecemberPromptActual - SSSGDecemberPriorActual)/SSSGDecemberPriorActual End as Decimal(11,1)) as Varchar) + '%' DecemberSSSG,
       Cast(CAST(100 * CASE WHEN SSSGAnnualPriorActual = 0 THEN NULL ELSE (SSSGAnnualPromptActual - SSSGAnnualPriorActual)/SSSGAnnualPriorActual End as Decimal(11,1)) as Varchar) + '%' AnnualTotalSSSG,
       Cast(CAST(100 * CASE WHEN SSSGThroughMonthPriorActual = 0 THEN NULL ELSE (SSSGThroughMonthPromptActual - SSSGThroughMonthPriorActual)/SSSGThroughMonthPriorActual End as Decimal(11,1)) as Varchar) + '%' ThroughMonthSSSG,
       'YOY %:' CategoryYOY,
       Cast(CAST(100 * CASE WHEN YOYJanuaryActual = 0 THEN NULL ELSE (JanuaryActual - YOYJanuaryActual)/YOYJanuaryActual End as Decimal(11,1)) as Varchar) + '%' JanuaryYOY,
       Cast(CAST(100 * CASE WHEN YOYFebruaryActual = 0 THEN NULL ELSE (FebruaryActual - YOYFebruaryActual)/YOYFebruaryActual End as Decimal(11,1)) as Varchar) + '%' FebruaryYOY,
       Cast(CAST(100 * CASE WHEN YOYMarchActual = 0 THEN NULL ELSE (MarchActual - YOYMarchActual)/YOYMarchActual End as Decimal(11,1)) as Varchar) + '%' MarchYOY,
       Cast(CAST(100 * CASE WHEN YOYAprilActual = 0 THEN NULL ELSE (AprilActual - YOYAprilActual)/YOYAprilActual End as Decimal(11,1)) as Varchar) + '%' AprilYOY,
       Cast(CAST(100 * CASE WHEN YOYMayActual = 0 THEN NULL ELSE (MayActual - YOYMayActual)/YOYMayActual End as Decimal(11,1)) as Varchar) + '%' MayYOY,
       Cast(CAST(100 * CASE WHEN YOYJuneActual = 0 THEN NULL ELSE (JuneActual - YOYJuneActual)/YOYJuneActual End as Decimal(11,1)) as Varchar) + '%' JuneYOY,
       Cast(CAST(100 * CASE WHEN YOYJulyActual = 0 THEN NULL ELSE (JulyActual - YOYJulyActual)/YOYJulyActual End as Decimal(11,1)) as Varchar) + '%' JulyYOY,
       Cast(CAST(100 * CASE WHEN YOYAugustActual = 0 THEN NULL ELSE (AugustActual - YOYAugustActual)/YOYAugustActual End as Decimal(11,1)) as Varchar) + '%' AugustYOY,
       Cast(CAST(100 * CASE WHEN YOYSeptemberActual = 0 THEN NULL ELSE (SeptemberActual - YOYSeptemberActual)/YOYSeptemberActual End as Decimal(11,1)) as Varchar) + '%' SeptemberYOY,
       Cast(CAST(100 * CASE WHEN YOYOctoberActual = 0 THEN NULL ELSE (OctoberActual - YOYOctoberActual)/YOYOctoberActual End as Decimal(11,1)) as Varchar) + '%' OctoberYOY,
       Cast(CAST(100 * CASE WHEN YOYNovemberActual = 0 THEN NULL ELSE (NovemberActual - YOYNovemberActual)/YOYNovemberActual End as Decimal(11,1)) as Varchar) + '%' NovemberYOY,
       Cast(CAST(100 * CASE WHEN YOYDecemberActual = 0 THEN NULL ELSE (DecemberActual - YOYDecemberActual)/YOYDecemberActual End as Decimal(11,1)) as Varchar) + '%' DecemberYOY,
       Cast(CAST(100 * CASE WHEN YOYAnnualActual = 0 THEN NULL ELSE (AnnualActual - YOYAnnualActual)/YOYAnnualActual End as Decimal(11,1)) as Varchar) + '%' AnnualYOY,
       Cast(CAST(100 * CASE WHEN YOYThroughMonthActual = 0 THEN NULL ELSE (ThroughMonthActual - YOYThroughMonthActual)/YOYThroughMonthActual End as Decimal(11,1)) as Varchar) + '%' ThroughMonthYOY,       
       NULL  AS RevenueReportingDepartmentNameCommaList,   ------- @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,  must be created in Cognos  
       @SalesSourceCommaList SalesSourceCommaList,
       @ReportRunDateTime ReportRunDateTime,
       RevenueReportingDepartment + ' Totals:' RowLabel,
       'Report-'+RevenueReportingDepartment+'-zzz' ReportSort,
       'Y' TotalBorderFlag,
       NULL  AS HeaderDivisionList,  ------ @HeaderDivisionList  Must create this in Cognos
       NULL  AS HeaderSubdivisionList ------  @HeaderSubdivisionList Must create this in Cognos
FROM #DepartmentDetailSummary         

UNION ALL

SELECT NULL CategoryActual,
       NULL JanuaryActual,
       NULL FebruaryActual,
       NULL MarchActual,
       NULL AprilActual,
       NULL MayActual,
       NULL JuneActual,
       NULL JulyActual,
       NULL AugustActual,
       NULL SeptemberActual,
       NULL OctoberActual,
       NULL NovemberActual,
       NULL DecemberActual,
       NULL AnnualTotalActual,
       NULL ThroughMonthActual,
       @PromptYear PromptYear,
       @PromptMonth PromptMonth,
       NULL CategoryGoal,
       NULL JanuaryGoal,
       NULL FebruaryGoal,
       NULL MarchGoal,
       NULL AprilGoal,
       NULL MayGoal,
       NULL JuneGoal,
       NULL JulyGoal,
       NULL AugustGoal,
       NULL SeptemberGoal,
       NULL OctoberGoal,
       NULL NovemberGoal,
       NULL DecemberGoal,
       NULL AnnualTotalGoal,
       NULL ThroughMonthGoal,
       NULL CategoryPercentOfGoal,
       NULL JanuaryPercentOfGoal,
       NULL FebruaryPercentOfGoal,
       NULL MarchPercentOfGoal,
       NULL AprilPercentOfGoal,
       NULL MayPercentOfGoal,
       NULL JunePercentOfGoal,
       NULL JulyPercentOfGoal,
       NULL AugustPercentOfGoal,
       NULL SeptemberPercentOfGoal,
       NULL OctoberPercentOfGoal,
       NULL NovemberPercentOfGoal,
       NULL DecemberPercentOfGoal,
       NULL AnnualTotalPercentOfGoal,
       NULL ThroughMonthPercentOfGoal,
       NULL CategorySSSG,
       NULL JanuarySSSG,
       NULL FebruarySSSG,
       NULL MarchSSSG,
       NULL AprilSSSG,
       NULL MaySSSG,
       NULL JuneSSSG,
       NULL JulySSSG,
       NULL AugustSSSG,
       NULL SeptemberSSSG,
       NULL OctoberSSSG,
       NULL NovemberSSSG,
       NULL DecemberSSSG,
       NULL AnnualTotalSSSG,
       NULL ThroughMonthSSSG,
       NULL CategoryYOY,
       NULL JanuaryYOY,
       NULL FebruaryYOY,
       NULL MarchYOY,
       NULL AprilYOY,
       NULL MayYOY,
       NULL JuneYOY,
       NULL JulyYOY,
       NULL AugustYOY,
       NULL SeptemberYOY,
       NULL OctoberYOY,
       NULL NovemberYOY,
       NULL DecemberYOY,
       NULL AnnualTotalYOY,
       NULL ThroughMonthYOY,
       NULL RevenueReportingDepartmentNameCommaList,   ------- @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,  must be created in Cognos
       @SalesSourceCommaList SalesSourceCommaList,
       @ReportRunDateTime ReportRunDateTime,
       RevenueReportingDepartment RowLabel,
       'Report-'+RevenueReportingDepartment+'-000' ReportSort,
       'N' TotalBorderFlag,
       NULL  AS HeaderDivisionList,  ------ @HeaderDivisionList  Must create this in Cognos
       NULL  AS HeaderSubdivisionList ------  @HeaderSubdivisionList Must create this in Cognos
FROM #DepartmentDetailSummary       
GROUP BY RevenueReportingDepartment

UNION ALL

SELECT 'Actual Revenue:' CategoryActual,
       Cast(Sum(JanuaryActual) as Decimal(10,0)) JanuaryActual,
       Cast(Sum(FebruaryActual) as Decimal(10,0)) FebruaryActual,
       Cast(Sum(MarchActual) as Decimal(10,0)) MarchActual,
       Cast(Sum(AprilActual) as Decimal(10,0)) AprilActual,
       Cast(Sum(MayActual) as Decimal(10,0)) MayActual,
       Cast(Sum(JuneActual) as Decimal(10,0)) JuneActual,
       Cast(Sum(JulyActual) as Decimal(10,0)) JulyActual,
       Cast(Sum(AugustActual) as Decimal(10,0)) AugustActual,
       Cast(Sum(SeptemberActual) as Decimal(10,0)) SeptemberActual,
       Cast(Sum(OctoberActual) as Decimal(10,0)) OctoberActual,
       Cast(Sum(NovemberActual) as Decimal(10,0)) NovemberActual,
       Cast(Sum(DecemberActual) as Decimal(10,0)) DecemberActual,
       Cast(Sum(AnnualActual) as Decimal(10,0)) AnnualTotalActual,
       Cast(Sum(ThroughMonthActual) as Decimal(10,0)) ThroughMonthActual,
       @PromptYear PromptYear,
       @PromptMonth PromptMonth,
       'Revenue Goal:' CategoryGoal,
       Cast(Sum(JanuaryGoal) as Decimal(10,0)) JanuaryGoal,
       Cast(Sum(FebruaryGoal) as Decimal(10,0)) FebruaryGoal,
       Cast(Sum(MarchGoal) as Decimal(10,0)) MarchGoal,
       Cast(Sum(AprilGoal) as Decimal(10,0)) AprilGoal,
       Cast(Sum(MayGoal) as Decimal(10,0)) MayGoal,
       Cast(Sum(JuneGoal) as Decimal(10,0)) JuneGoal,
       Cast(Sum(JulyGoal) as Decimal(10,0)) JulyGoal,
       Cast(Sum(AugustGoal) as Decimal(10,0)) AugustGoal,
       Cast(Sum(SeptemberGoal) as Decimal(10,0)) SeptemberGoal,
       Cast(Sum(OctoberGoal) as Decimal(10,0)) OctoberGoal,
       Cast(Sum(NovemberGoal) as Decimal(10,0)) NovemberGoal,
       Cast(Sum(DecemberGoal) as Decimal(10,0)) DecemberGoal,
       Cast(Sum(AnnualGoal) as Decimal(10,0)) AnnualTotalGoal,
       Cast(Sum(ThroughMonthGoal) as Decimal(10,0)) ThroughMonthGoal,
       '% of Goal:' CategoryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(JanuaryGoal) = 0 THEN 0 ELSE Sum(JanuaryActual)/Sum(JanuaryGoal) END as DECIMAL(11,1)) as Varchar) + '%' JanuaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(FebruaryGoal) = 0 THEN 0 ELSE Sum(FebruaryActual)/Sum(FebruaryGoal) END as DECIMAL(11,1)) as Varchar) + '%' FebruaryPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(MarchGoal) = 0 THEN 0 ELSE Sum(MarchActual)/Sum(MarchGoal) END as DECIMAL(11,1)) as Varchar) + '%' MarchPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(AprilGoal) = 0 THEN 0 ELSE Sum(AprilActual)/Sum(AprilGoal) END as DECIMAL(11,1)) as Varchar) + '%' AprilPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(MayGoal) = 0 THEN 0 ELSE Sum(MayActual)/Sum(MayGoal) END as DECIMAL(11,1)) as Varchar) + '%' MayPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(JuneGoal) = 0 THEN 0 ELSE Sum(JuneActual)/Sum(JuneGoal) END as DECIMAL(11,1)) as Varchar) + '%' JunePercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(JulyGoal) = 0 THEN 0 ELSE Sum(JulyActual)/Sum(JulyGoal) END as DECIMAL(11,1)) as Varchar) + '%' JulyPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(AugustGoal) = 0 THEN 0 ELSE Sum(AugustActual)/Sum(AugustGoal) END as DECIMAL(11,1)) as Varchar) + '%' AugustPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(SeptemberGoal) = 0 THEN 0 ELSE Sum(SeptemberActual)/Sum(SeptemberGoal) END as DECIMAL(11,1)) as Varchar) + '%' SeptemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(OctoberGoal) = 0 THEN 0 ELSE Sum(OctoberActual)/Sum(OctoberGoal) END as DECIMAL(11,1)) as Varchar) + '%' OctoberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(NovemberGoal) = 0 THEN 0 ELSE Sum(NovemberActual)/Sum(NovemberGoal) END as DECIMAL(11,1)) as Varchar) + '%' NovemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(DecemberGoal) = 0 THEN 0 ELSE Sum(DecemberActual)/Sum(DecemberGoal) END as DECIMAL(11,1)) as Varchar) + '%' DecemberPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(AnnualGoal) = 0 THEN 0 ELSE Sum(AnnualActual)/Sum(AnnualGoal) END as DECIMAL(11,1)) as Varchar) + '%' AnnualTotalPercentOfGoal,
       Cast(Cast(100 * CASE WHEN Sum(ThroughMonthGoal) = 0 THEN 0 ELSE Sum(ThroughMonthActual)/Sum(ThroughMonthGoal) END as DECIMAL(11,1)) as Varchar) + '%' ThroughMonthPercentOfGoal,       
       'SSSG %:' CategorySSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGJanuaryPriorActual) = 0 THEN NULL ELSE (Sum(SSSGJanuaryPromptActual) - Sum(SSSGJanuaryPriorActual))/Sum(SSSGJanuaryPriorActual) End as Decimal(11,1)) as Varchar) + '%' JanuarySSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGFebruaryPriorActual) = 0 THEN NULL ELSE (Sum(SSSGFebruaryPromptActual) - Sum(SSSGFebruaryPriorActual))/Sum(SSSGFebruaryPriorActual) End as Decimal(11,1)) as Varchar) + '%' FebruarySSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGMarchPriorActual) = 0 THEN NULL ELSE (Sum(SSSGMarchPromptActual) - Sum(SSSGMarchPriorActual))/Sum(SSSGMarchPriorActual) End as Decimal(11,1)) as Varchar) + '%' MarchSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGAprilPriorActual) = 0 THEN NULL ELSE (Sum(SSSGAprilPromptActual) - Sum(SSSGAprilPriorActual))/Sum(SSSGAprilPriorActual) End as Decimal(11,1)) as Varchar) + '%' AprilSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGMayPriorActual) = 0 THEN NULL ELSE (Sum(SSSGMayPromptActual) - Sum(SSSGMayPriorActual))/Sum(SSSGMayPriorActual) End as Decimal(11,1)) as Varchar) + '%' MaySSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGJunePriorActual) = 0 THEN NULL ELSE (Sum(SSSGJunePromptActual) - Sum(SSSGJunePriorActual))/Sum(SSSGJunePriorActual) End as Decimal(11,1)) as Varchar) + '%' JuneSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGJulyPriorActual) = 0 THEN NULL ELSE (Sum(SSSGJulyPromptActual) - Sum(SSSGJulyPriorActual))/Sum(SSSGJulyPriorActual) End as Decimal(11,1)) as Varchar) + '%' JulySSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGAugustPriorActual) = 0 THEN NULL ELSE (Sum(SSSGAugustPromptActual) - Sum(SSSGAugustPriorActual))/Sum(SSSGAugustPriorActual) End as Decimal(11,1)) as Varchar) + '%' AugustSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGSeptemberPriorActual) = 0 THEN NULL ELSE (Sum(SSSGSeptemberPromptActual) - Sum(SSSGSeptemberPriorActual))/Sum(SSSGSeptemberPriorActual) End as Decimal(11,1)) as Varchar) + '%' SeptemberSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGOctoberPriorActual) = 0 THEN NULL ELSE (Sum(SSSGOctoberPromptActual) - Sum(SSSGOctoberPriorActual))/Sum(SSSGOctoberPriorActual) End as Decimal(11,1)) as Varchar) + '%' OctoberSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGNovemberPriorActual) = 0 THEN NULL ELSE (Sum(SSSGNovemberPromptActual) - Sum(SSSGNovemberPriorActual))/Sum(SSSGNovemberPriorActual) End as Decimal(11,1)) as Varchar) + '%' NovemberSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGDecemberPriorActual) = 0 THEN NULL ELSE (Sum(SSSGDecemberPromptActual) - Sum(SSSGDecemberPriorActual))/Sum(SSSGDecemberPriorActual) End as Decimal(11,1)) as Varchar) + '%' DecemberSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGAnnualPriorActual) = 0 THEN NULL ELSE (Sum(SSSGAnnualPromptActual) - Sum(SSSGAnnualPriorActual))/Sum(SSSGAnnualPriorActual) End as Decimal(11,1)) as Varchar) + '%' AnnualTotalSSSG,
       Cast(CAST(100 * CASE WHEN Sum(SSSGThroughMonthPriorActual) = 0 THEN NULL ELSE (Sum(SSSGThroughMonthPromptActual) - Sum(SSSGThroughMonthPriorActual))/Sum(SSSGThroughMonthPriorActual) End as Decimal(11,1)) as Varchar) + '%' ThroughMonthSSSG,
       'YOY %:' CategoryYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYJanuaryActual) = 0 THEN NULL ELSE (Sum(JanuaryActual) - Sum(YOYJanuaryActual))/Sum(YOYJanuaryActual) End as Decimal(11,1)) as Varchar) + '%' JanuaryYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYFebruaryActual) = 0 THEN NULL ELSE (Sum(FebruaryActual) - Sum(YOYFebruaryActual))/Sum(YOYFebruaryActual) End as Decimal(11,1)) as Varchar) + '%' FebruaryYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYMarchActual) = 0 THEN NULL ELSE (Sum(MarchActual) - Sum(YOYMarchActual))/Sum(YOYMarchActual) End as Decimal(11,1)) as Varchar) + '%' MarchYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYAprilActual) = 0 THEN NULL ELSE (Sum(AprilActual) - Sum(YOYAprilActual))/Sum(YOYAprilActual) End as Decimal(11,1)) as Varchar) + '%' AprilYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYMayActual) = 0 THEN NULL ELSE (Sum(MayActual) - Sum(YOYMayActual))/Sum(YOYMayActual) End as Decimal(11,1)) as Varchar) + '%' MayYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYJuneActual) = 0 THEN NULL ELSE (Sum(JuneActual) - Sum(YOYJuneActual))/Sum(YOYJuneActual) End as Decimal(11,1)) as Varchar) + '%' JuneYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYJulyActual) = 0 THEN NULL ELSE (Sum(JulyActual) - Sum(YOYJulyActual))/Sum(YOYJulyActual) End as Decimal(11,1)) as Varchar) + '%' JulyYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYAugustActual) = 0 THEN NULL ELSE (Sum(AugustActual) - Sum(YOYAugustActual))/Sum(YOYAugustActual) End as Decimal(11,1)) as Varchar) + '%' AugustYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYSeptemberActual) = 0 THEN NULL ELSE (Sum(SeptemberActual) - Sum(YOYSeptemberActual))/Sum(YOYSeptemberActual) End as Decimal(11,1)) as Varchar) + '%' SeptemberYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYOctoberActual) = 0 THEN NULL ELSE (Sum(OctoberActual) - Sum(YOYOctoberActual))/Sum(YOYOctoberActual) End as Decimal(11,1)) as Varchar) + '%' OctoberYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYNovemberActual) = 0 THEN NULL ELSE (Sum(NovemberActual) - Sum(YOYNovemberActual))/Sum(YOYNovemberActual) End as Decimal(11,1)) as Varchar) + '%' NovemberYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYDecemberActual) = 0 THEN NULL ELSE (Sum(DecemberActual) - Sum(YOYDecemberActual))/Sum(YOYDecemberActual) End as Decimal(11,1)) as Varchar) + '%' DecemberYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYAnnualActual) = 0 THEN NULL ELSE (Sum(AnnualActual) - Sum(YOYAnnualActual))/Sum(YOYAnnualActual) End as Decimal(11,1)) as Varchar) + '%' AnnualYOY,
       Cast(CAST(100 * CASE WHEN Sum(YOYThroughMonthActual) = 0 THEN NULL ELSE (Sum(ThroughMonthActual) - Sum(YOYThroughMonthActual))/Sum(YOYThroughMonthActual) End as Decimal(11,1)) as Varchar) + '%' ThroughMonthYOY,       
       NULL  AS RevenueReportingDepartmentNameCommaList,   ------- @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,  must be created in Cognos
       @SalesSourceCommaList SalesSourceCommaList,
       @ReportRunDateTime ReportRunDateTime,
       'Report Totals:' RowLabel,
       'Report-zzz-zzz' ReportSort,
       'Y' TotalBorderFlag,
       NULL  AS HeaderDivisionList,  ------ @HeaderDivisionList  Must create this in Cognos
       NULL  AS HeaderSubdivisionList ------  @HeaderSubdivisionList Must create this in Cognos
FROM #DepartmentDetailSummary         
ORDER BY ReportSort

DROP TABLE #SalesSourceList
DROP TABLE #DimReportingHierarchy
DROP TABLE #PromptYearRevenueDetail
DROP TABLE #ProductGroupRevenueSummary
DROP TABLE #DepartmentRevenueSummary
DROP TABLE #PriorYearRevenueDetail
DROP TABLE #ProductGroupSSSGPriorRevenueSummary
DROP TABLE #ProductGroupSSSGPromptRevenueSummary
DROP TABLE #DepartmentSSSGPriorRevenueSummary
DROP TABLE #DepartmentSSSGPromptRevenueSummary
DROP TABLE #ProductGroupYOYSummary
DROP TABLE #DepartmentYOYSummary
DROP TABLE #GoalDetail
DROP TABLE #ProductGroupGoalSummary
DROP TABLE #DepartmentGoalSummary
DROP TABLE #ProductGroupDetailSummary
DROP TABLE #DepartmentDetailSummary


END
