CREATE PROC [reporting].[proc_RevenueSummaryByClubAndTeamMember] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@EndFourDigitYearDashTwoDigitMonth [CHAR](7),@DimClubidList [VARCHAR](4000),@SalesSourceList [VARCHAR](4000),@CommissionTypeList [VARCHAR](4000),@DimReportingHierarchyKeyList [VARCHAR](8000),@TotalReportingHierarchyKeyCount [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON


IF 1=0 BEGIN
       SET FMTONLY OFF
     END


--DECLARE
--   @StartFourDigitYearDashTwoDigitMonth [CHAR](7) = '2019-01',
--   @EndFourDigitYearDashTwoDigitMonth [CHAR](7) = '2019-03',
--   @DimClubidList [VARCHAR](4000) = '-1',
--   @SalesSourceList [VARCHAR](4000) = 'Cafe',
--   @CommissionTypeList [VARCHAR](4000) = 'Commissioned|Non-Commissioned',
--   @DimReportingHierarchyKeyList [VARCHAR](8000) = 'N/A',
--   @TotalReportingHierarchyKeyCount [INT] = 1



-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings
DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)
+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)
+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') 
get_date_varchar from map_utc_time_zone_conversion
where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


DECLARE @StartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key
FROM [marketing].[v_dim_date] DimDate
WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth
  AND DimDate.Day_Number_In_Month = 1

DECLARE @EndMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT
SELECT @EndMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key,
       @EndMonthEndingDimDateKey = DimDate.month_ending_dim_date_key
FROM [marketing].[v_dim_date] DimDate
WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth
  AND DimDate.Day_Number_In_Month = 1
  


DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) FROM [marketing].[v_dim_date] where Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(dim_date_key) FROM [marketing].[v_dim_date] where Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth)

DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (Select Prior_Month_Starting_Dim_Date_Key
			        FROM [marketing].[v_dim_date] 
			        WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
            ELSE (Select Month_Starting_Dim_Date_Key
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @EndOfReportRangeDimDateKey) in (1,4,7,10)
			THEN (Select Prior_Month_Ending_Dim_Date_Key
			        FROM [marketing].[v_dim_date] 
			        WHERE dim_date_key = @EndOfReportRangeDimDateKey)
            ELSE (Select month_ending_dim_date_key
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @EndOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record

	  ----- Create Sales Source temp table   
IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL
  DROP TABLE #SalesSourceList;   

DECLARE @list_table VARCHAR(100)
SET @list_table = 'sales_source_list'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT DISTINCT SalesSourceList.Item Sales_Source  
  INTO #SalesSourceList
  FROM #sales_source_list SalesSourceList
  
DECLARE @SalesSourceCommaList Varchar(4000)
SET @SalesSourceCommaList = Replace(@SalesSourceList,'|',', ')

IF OBJECT_ID('tempdb.dbo.#DimClubidList', 'U') IS NOT NULL
  DROP TABLE #DimClubidList;   


SET @list_table = 'Dim_Club_List'

EXEC marketing.proc_parse_pipe_list @DimClubidList,@list_table

SELECT DISTINCT DimClubidList.Item Club_id  
  INTO #DimClubidList
  FROM #Dim_Club_list DimClubidList
   
IF OBJECT_ID('tempdb.dbo.#CommissionTypeList', 'U') IS NOT NULL
  DROP TABLE #CommissionTypeList;   

SET @list_table = 'Commission_Type_List'

EXEC marketing.proc_parse_pipe_list @CommissionTypeList,@list_table

SELECT DISTINCT CommissionTypeList.Item CommissionTypeList,
 CASE WHEN CommissionTypeList.item = 'Commissioned' THEN 'Y' ELSE 'N' END CommissionedSalesTransactionFlag
  INTO #CommissionTypeList
  FROM #Commission_Type_List  CommissionTypeList
  
DECLARE @CommissionTypeCommaList VARCHAR(4000)
SET @CommissionTypeCommaList = REPLACE(@CommissionTypeList,'|',', ')

Exec [reporting].[proc_DimReportingHierarchy_history] 'N/A','N/A','N/A',@DimReportingHierarchyKeyList,@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
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

	   

	   



	DECLARE @RevenueProductGroupNameCommaList VARCHAR(8000)
	DECLARE @RegionType VARCHAR(50)

	SET @RevenueProductGroupNameCommaList = (SELECT CASE WHEN COUNT(DISTINCT DimReportingHierarchyKey) >= @TotalReportingHierarchyKeyCount 
															  THEN 'All Product Groups'
															WHEN COUNT(DISTINCT DimReportingHierarchyKey) = 1 
															  THEN MIN(ProductGroupName)
															ELSE 'Multiple Product Groups' END
												  FROM #DimReportingHierarchy)
	SET @RegionType = (SELECT MIN(ReportRegionType) FROM #DimReportingHierarchy)


IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL  
  DROP TABLE #DimLocationInfo;

SELECT DimClub.dim_club_key AS DimClubKey,      ----- new name
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
	   DimClub.club_code ClubCode,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   CASE WHEN @RegionType = 'PT RCL Area' 
             THEN PTRCLRegion.description
           WHEN @RegionType = 'Member Activities Region' 
             THEN MemberActivitiesRegion.description
           WHEN @RegionTYpe = 'MMS Region' 
             THEN MMSRegion.description  
		   END  Region,
	   CASE WHEN club_open_dim_date_key <= @EndMonthEndingDimDateKey
	       THEN 'Open'
		   ELSE 'Presale'
		   END ClubStatus,
       DimClub.club_open_dim_date_key
 INTO #DimLocationInfo                                   
  FROM [marketing].[v_dim_club] DimClub
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
 JOIN #DimClubidList DimClubidList
 ON DimClub.club_id=DimClubidList.Club_id
 or DimClubidList.club_id=-1
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key in('-997','-998','-999') OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   CASE WHEN @RegionType = 'PT RCL Area' 
             THEN PTRCLRegion.description
           WHEN @RegionType = 'Member Activities Region' 
             THEN MemberActivitiesRegion.description
           WHEN @RegionTYpe = 'MMS Region' 
             THEN MMSRegion.description  
		   END,
	   CASE WHEN club_open_dim_date_key <= @EndMonthEndingDimDateKey
	       THEN 'Open'
		   ELSE 'Presale'
		   END,
       DimClub.club_open_dim_date_key

		
IF OBJECT_ID('tempdb.dbo.#TeamMemberSummary', 'U') IS NOT NULL
  DROP TABLE #TeamMemberSummary;

SELECT CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
                    THEN NULL
               ELSE DimEmployee.employee_name_last_first
			   END TeamMember,
       FactAllocatedRevenue.allocated_amount AS TeamMemberActualAmount,
       #DimLocationInfo.DimClubKey,
       DimEmployee.Dim_Employee_Key,
       DimEmployee.First_Name TeamMemberFirstName,
       DimEmployee.Last_Name TeamMemberLastName,
       0 AS SNSAmount,
	   #DimLocationInfo.club_id,
	   #DimReportingHierarchy.DivisionName,    
		#DimReportingHierarchy.SubdivisionName,
		#DimReportingHierarchy.DepartmentName,
		#DimReportingHierarchy.ProductGroupName,
		FactAllocatedRevenue.dim_reporting_hierarchy_key
  INTO #TeamMemberSummary  
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue
  JOIN [marketing].[v_dim_date] RevenueMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
  JOIN #DimReportingHierarchy   
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #DimLocationInfo  
    ON FactAllocatedRevenue.allocated_dim_club_key = #DimLocationInfo.DimClubKey
  JOIN [marketing].[v_dim_employee] DimEmployee
     ON FactAllocatedRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key
  JOIN #SalesSourceList  
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.Sales_Source 
 WHERE (FactAllocatedRevenue.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
          AND FactAllocatedRevenue.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey)
   AND (FactAllocatedRevenue.sales_source in('MMS','Cafe')
       OR (FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento') 
	        AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N'))  --- deferral handling only needed for e-comm transactions

  
UNION ALL

SELECT CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
                    THEN NULL
               ELSE DimEmployee.employee_name_last_first
			   END TeamMember,
       FactAllocatedRevenue.allocated_amount AS TeamMemberActualAmount,
       #DimLocationInfo.DimClubKey,
       DimEmployee.Dim_Employee_Key,
       DimEmployee.First_Name TeamMemberFirstName,
       DimEmployee.Last_Name TeamMemberLastName,
       0 AS SNSAmount,
	   #DimLocationInfo.club_id,
	   	   #DimReportingHierarchy.DivisionName,    
		#DimReportingHierarchy.SubdivisionName,
		#DimReportingHierarchy.DepartmentName,
		#DimReportingHierarchy.ProductGroupName,
		FactAllocatedRevenue.dim_reporting_hierarchy_key
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue  
  JOIN #DimLocationInfo
     ON FactAllocatedRevenue.allocated_dim_club_key = #DimLocationInfo.DimClubKey 
  JOIN [marketing].[v_dim_employee] DimEmployee
     ON FactAllocatedRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_date] RevenueMonthDimDate
    ON FactAllocatedRevenue.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
  JOIN #DimReportingHierarchy
    ON FactAllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #SalesSourceList
    ON FactAllocatedRevenue.sales_source = #SalesSourceList.Sales_Source 
 WHERE (FactAllocatedRevenue.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedRevenue.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey)
   AND #DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'
   AND FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento')
 

IF OBJECT_ID('tempdb.dbo.#RevenueSummary', 'U') IS NOT NULL
  DROP TABLE #RevenueSummary;
  
SELECT TeamMember,
       SUM(TeamMemberActualAmount) TeamMemberActualAmount,
       DimClubKey,
       Dim_Employee_Key,
       TeamMemberFirstName,
       TeamMemberLastName,
       SUM(SNSAmount) SNSAmount,
	   club_id
INTO #RevenueSummary
FROM #TeamMemberSummary
GROUP BY TeamMember,
         DimClubKey,
         Dim_Employee_Key,
         TeamMemberFirstName,
         TeamMemberLastName,
		 club_id

IF OBJECT_ID('tempdb.dbo.#ClubGoal', 'U') IS NOT NULL
  DROP TABLE #ClubGoal;

SELECT #DimLocationInfo.DimClubKey,
       #DimLocationInfo.Region,
       SUM(FactGoal.goal_dollar_amount) ClubGoalAmount,
	   #DimLocationInfo.club_id
  INTO #ClubGoal
  FROM [marketing].[v_fact_revenue_goal] FactGoal
  JOIN #DimReportingHierarchy
    ON FactGoal.Dim_Reporting_Hierarchy_Key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #DimLocationInfo 
    ON FactGoal.Dim_Club_Key = #DimLocationInfo.DimClubKey
 WHERE FactGoal.Goal_Effective_Dim_Date_Key >= @StartMonthStartingDimDateKey
   AND FactGoal.Goal_Effective_Dim_Date_Key <= @EndMonthStartingDimDateKey
 GROUP BY #DimLocationInfo.DimClubKey,
          #DimLocationInfo.Region,
		  #DimLocationInfo.club_id

IF OBJECT_ID('tempdb.dbo.#RegionGoal', 'U') IS NOT NULL
  DROP TABLE #RegionGoal;
  
SELECT Region,
       SUM(ClubGoalAmount) RegionGoalAmount
  INTO #RegionGoal
  FROM #ClubGoal
 GROUP BY Region

IF OBJECT_ID('tempdb.dbo.#ReportGoal', 'U') IS NOT NULL
  DROP TABLE #ReportGoal;
  
SELECT SUM(ClubGoalAmount) ReportGoalAmount
  INTO #ReportGoal
  FROM #ClubGoal

IF OBJECT_ID('tempdb.dbo.#GoalSummary', 'U') IS NOT NULL
  DROP TABLE #GoalSummary;
  
SELECT #ClubGoal.DimClubKey,
       #ClubGoal.ClubGoalAmount,
       #RegionGoal.RegionGoalAmount,
       #ReportGoal.ReportGoalAmount,
	   #ClubGoal.club_id
  INTO #GoalSummary
  FROM #ClubGoal
  JOIN #RegionGoal 
    ON #ClubGoal.Region = #RegionGoal.Region
  CROSS JOIN #ReportGoal
  
SELECT #DimLocationInfo.Region,
       ISNULL(#GoalSummary.RegionGoalAmount,0) RegionGoalAmount,
	   CASE WHEN #DimLocationInfo.club_id = 13 THEN '100' ELSE #DimLocationInfo.ClubCode END Club_Code,
       ISNULL(#GoalSummary.ClubGoalAmount,0) ClubGoalAmount,
       #RevenueSummary.TeamMember,
       ISNULL(#RevenueSummary.TeamMemberActualAmount,0) TeamMemberActualAmount,
       ISNULL(#GoalSummary.ReportGoalAmount,0) ReportGoalAmount,
       @ReportRunDateTime ReportRunDateTime,
       NULL RevenueReportingDepartmentNameCommaList,    --@RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,    ------ must build in Cognos
       @RevenueProductGroupNameCommaList As RevenueProductGroupNameCommaList,
       #DimLocationInfo.LocalCurrencyCode CurrencyCode,
       @SalesSourceCommaList SalesSourceCommaList,
       @CommissionTypeCommaList CommissionTypeCommaList,
       #RevenueSummary.TeamMemberLastName,
       #RevenueSummary.TeamMemberFirstName,
       CASE WHEN #GoalSummary.DimClubKey IS NULL THEN #RevenueSummary.DimClubKey ELSE #GoalSummary.DimClubKey END DimClubKey,
       #RevenueSummary.Dim_Employee_Key,
       0 AS SNSAmount,
       NULL HeaderDivisionList, -- @HeaderDivisionList HeaderDivisionList,
       NULL HeaderSubdivisionList, --@HeaderSubdivisionList HeaderSubdivisionList
	   CASE WHEN #GoalSummary.club_id IS NULL THEN #RevenueSummary.club_id ELSE #GoalSummary.club_id END MMSClubID 
	   FROM #RevenueSummary
  FULL OUTER JOIN #GoalSummary 
    ON #RevenueSummary.DimClubKey = #GoalSummary.DimClubKey
  JOIN #DimLocationInfo 
    ON #DimLocationInfo.DimClubKey = CASE WHEN #GoalSummary.DimClubKey IS NULL THEN #RevenueSummary.DimClubKey
                                              ELSE #GoalSummary.DimClubKey END
 WHERE ISNULL(#RevenueSummary.TeamMemberActualAmount,0) <> 0
    OR ISNULL(#GoalSummary.ClubGoalAmount,0) <> 0
 ORDER BY Region, Club_Code, TeamMemberLastName, TeamMemberFirstName

DROP TABLE #DimReportingHierarchy
DROP TABLE #SalesSourceList
DROP TABLE #CommissionTypeList
DROP TABLE #DimLocationInfo
DROP TABLE #TeamMemberSummary
DROP TABLE #RevenueSummary
DROP TABLE #ClubGoal
DROP TABLE #RegionGoal
DROP TABLE #ReportGoal
DROP TABLE #GoalSummary



END

