CREATE PROC [reporting].[proc_PTTrainer_DSSR_OldAndNewBusiness] AS   
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

----- This script is hardcoded to return data for yesterday's date as well as for all clubs
----- This script is used by Informatica to populate the MNCODB23.Sandbox table "rep.PTDSSR_OldandNewBusiness"

DECLARE  @ReportDate Datetime = '1/1/1900'
DECLARE    @RegionList VARCHAR(8000) = 'All Regions'
DECLARE    @DimClubIDList VARCHAR(8000) = '-1'  

DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

SET @ReportDate = CASE WHEN @ReportDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END

DECLARE @ReportDateDimDateKey VARCHAR(32)
DECLARE @ReportDateFirstOfMonthDate DateTime
DECLARE @ReportDateFirstOfMonthDimDateKey VARCHAR(32)

SET @ReportDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)
SET @ReportDateFirstOfMonthDate = (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)
SET @ReportDateFirstOfMonthDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)



 ----- When All Regions and All Clubs are selection options ( PT RCLArea only is used for this set of reports)
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

DECLARE @list_table VARCHAR(100)
  ----- Create club temp table
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @DimClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code AS ClubCode,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   PTRCLRegion.description AS PTRCLRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id
	  OR ClubKeyList.Item = -1
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @ReportDateDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   PTRCLRegion.description

IF OBJECT_ID('tempdb.dbo.#DimClubKeyList', 'U') IS NOT NULL
  DROP TABLE #DimClubKeyList;

  ----- Create Region temp table
SET @list_table = 'region_list'

  EXEC marketing.proc_parse_pipe_list @RegionList,@list_table
	
SELECT DimClub.DimClubKey,      ------ name change
       DimClub.PTRCLRegion AS Region,
       DimClub.ClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.LocalCurrencyCode,
	   DimClub.ClubCode
  INTO #DimClubKeyList     
  FROM #Clubs DimClub     
  JOIN #region_list RegionList 
   ON RegionList.Item =  DimClub.PTRCLRegion 
     OR RegionList.Item = 'All Regions'
 GROUP BY DimClub.DimClubKey, 
          DimClub.PTRCLRegion,
       DimClub.ClubName,
	   DimClub.club_id,
	   DimClub.gl_club_id,
	   DimClub.LocalCurrencyCode,
	   DimClub.ClubCode




 ----- pull in club budget amounts

 IF OBJECT_ID('tempdb.dbo.#ClubBusinessSubTypeBudget', 'U') IS NOT NULL
  DROP TABLE #ClubBusinessSubTypeBudget;

SELECT DimClub.MMSClubID AS ClubID,
       DimClub.ClubCode,
       CASE When DimGoalLineItem.Description = 'EFT'
         THEN 'EFT Amount'
		 WHEN DimGoalLineItem.Description = 'Non-EFT'
		 THEN 'Non-EFT Amount'
		 ELSE DimGoalLineItem.Description
		END BusinessSubType,
		FactGoal.goal_dollar_amount AS BudgetAmount

INTO #ClubBusinessSubTypeBudget     
FROM #DimClubKeyList DimClub
  JOIN [marketing].[v_fact_goal] FactGoal                              
    ON FactGoal.dim_club_key = DimClub.DimClubKey
  JOIN [marketing].[v_dim_goal_line_item] DimGoalLineItem
    ON FactGoal.dim_goal_line_item_key = DimGoalLineItem.dim_goal_line_item_key   
WHERE FactGoal.goal_effective_dim_date_key = @ReportDateFirstOfMonthDimDateKey    
AND DimGoalLineItem.category_description = 'PT Old And New Business'



 ------- Pull in data from the UDW table for just the report date

IF OBJECT_ID('tempdb.dbo.#SummaryTableDataPrelim', 'U') IS NOT NULL
  DROP TABLE #SummaryTableDataPrelim;

SELECT SummaryTable.report_date_dim_date_key AS ReportDateDimDateKey,
       SummaryTable.mms_club_id AS RevenueMMSClubID,
	   SummaryTable.employee_id AS PrimarySalesEmployeeID,
	   SummaryTable.business_type AS BusinessType,
	   SummaryTable.business_sub_type AS BusinessSubType,
	   SummaryTable.month_to_date_revenue_item_amount AS MTDRevenueItemAmount,
	   SummaryTable.report_date_item_amount AS Today_ItemAmount,
	   SummaryTable.forecast_amount AS ForecastAmount
INTO #SummaryTableDataPrelim      
FROM [dbo].[fact_ptdssr_old_and_new_business_employee_summary]  SummaryTable  

WHERE SummaryTable.report_date_dim_date_key = @ReportDateDimDateKey 



 ------ To return distinct attribute columns with their total amounts
IF OBJECT_ID('tempdb.dbo.#SummaryTableData', 'U') IS NOT NULL
  DROP TABLE #SummaryTableData;

SELECT ReportDateDimDateKey,
       RevenueMMSClubID,
	   PrimarySalesEmployeeID,
	   BusinessType,
	   BusinessSubType,
	   SUM(MTDRevenueItemAmount) AS MTDRevenueItemAmount,
	   SUM(Today_ItemAmount) AS Today_ItemAmount,
	   SUM(ForecastAmount) AS ForecastAmount
INTO #SummaryTableData     
FROM #SummaryTableDataPrelim
GROUP BY ReportDateDimDateKey,
       RevenueMMSClubID,
	   PrimarySalesEmployeeID,
	   BusinessType,
	   BusinessSubType


  ----- Collect all BusinessType and Subtype sets and setting a unique identifier for each BusinessSubType for later use for assigning budget
IF OBJECT_ID('tempdb.dbo.#TypeOptions', 'U') IS NOT NULL
  DROP TABLE #TypeOptions;

SELECT ROW_NUMBER() OVER(PARTITION BY BusinessSubType ORDER BY BusinessType,BusinessSubType) AS RowNumber,
	   BusinessType,
	   BusinessSubType
INTO #TypeOptions   
FROM #SummaryTableData
GROUP BY BusinessType,
	     BusinessSubType


IF OBJECT_ID('tempdb.dbo.#EmployeeIDList', 'U') IS NOT NULL
  DROP TABLE #EmployeeIDList;

Select PrimarySalesEmployeeID,RevenueMMSClubID
INTO #EmployeeIDList     
FROM #SummaryTableData
Group By PrimarySalesEmployeeID,RevenueMMSClubID

IF OBJECT_ID('tempdb.dbo.#AllEmployeeBusinessTypeOptions', 'U') IS NOT NULL
  DROP TABLE #AllEmployeeBusinessTypeOptions;

Select PrimarySalesEmployeeID,
	   RevenueMMSClubID,
	   RowNumber,
	   BusinessType,
	   BusinessSubType
INTO #AllEmployeeBusinessTypeOptions        
FROM #EmployeeIDList
CROSS JOIN #TypeOptions

  ----- to get a list of all values at the employee level
  IF OBJECT_ID('tempdb.dbo.#EmployeeDetail', 'U') IS NOT NULL
  DROP TABLE #EmployeeDetail;

SELECT @ReportDateDimDateKey AS ReportDateDimDateKey,
	   #AllOptions.RevenueMMSClubID,
	   #AllOptions.PrimarySalesEmployeeID,
	   #AllOptions.RowNumber,
	   #AllOptions.BusinessType,
	   #AllOptions.BusinessSubType,
	   IsNull(SummaryTableData.MTDRevenueItemAmount,0) AS MTDRevenueItemAmount,
	   IsNull(SummaryTableData.Today_ItemAmount,0) AS Today_ItemAmount,
	   IsNull(SummaryTableData.ForecastAmount,0) AS ForecastAmount,
	   @ReportDate AS ReportDate,
	   DimLocation.Region AS PersonalTrainingRegionalCategoryLeadAreaName,
	   DimLocation.ClubName,
	   DimLocation.MMSClubID,
	   DimLocation.ClubCode,
	   #AllOptions.PrimarySalesEmployeeID AS EmployeeID,
	   DimEmployee.first_name AS FirstName,
	   DimEmployee.last_name AS LastName,
CASE WHEN #AllOptions.BusinessSubType = 'Products'
     THEN 'Total Products'
	 WHEN #AllOptions.BusinessType = 'New Business'
	 THEN 'Total New Business'
	 ELSE 'Total Old Business'
	 END ReportGrouping,
	 @ReportRunDateTime as ReportRunDateTime
INTO #EmployeeDetail            
FROM #AllEmployeeBusinessTypeOptions #AllOptions
LEFT JOIN #SummaryTableData SummaryTableData
   ON #AllOptions.PrimarySalesEmployeeID = SummaryTableData.PrimarySalesEmployeeID
    AND #AllOptions.BusinessType = SummaryTableData.BusinessType
    AND #AllOptions.BusinessSubType = SummaryTableData.BusinessSubType
    AND #AllOptions.RevenueMMSClubID = SummaryTableData.RevenueMMSClubID
 JOIN #DimClubKeyList DimLocation
  ON #AllOptions.RevenueMMSClubID = DimLocation.MMSClubID
 LEFT JOIN [marketing].[v_dim_employee] DimEmployee        ------- This needs to be a left join or else transactions with employee key -998 drop off because the employee id is null
  ON #AllOptions.PrimarySalesEmployeeID = DimEmployee.employee_id

  

  ----- to get a list of all values at the Club level so as to not duplicate club level budget values
    IF OBJECT_ID('tempdb.dbo.#ClubSummary', 'U') IS NOT NULL
  DROP TABLE #ClubSummary;

  SELECT ReportDateDimDateKey,
	   RevenueMMSClubID,
	   -95 AS PrimarySalesEmployeeID,
	   RowNumber,
	   NULL AS ProductCategory,
	   BusinessType,
	   BusinessSubType,
	   SUM(MTDRevenueItemAmount) AS MTDRevenueItemAmount,
	   SUM(Today_ItemAmount) AS Today_ItemAmount,
	   SUM(ForecastAmount) AS ForecastAmount,
	   ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName,
	   ClubName,
	   MMSClubID,
	   ClubCode,
	   -95 AS EmployeeID,
	   'Entire Club' AS FirstName,
	   ' ' AS LastName,
       ReportGrouping,
	   ReportRunDateTime
INTO #ClubSummary   
FROM #EmployeeDetail
 GROUP BY ReportDateDimDateKey,
	   RevenueMMSClubID,
	   RowNumber,
	   BusinessType,
	   BusinessSubType,
	   ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName,
	   ClubName,
	   MMSClubID,
	   ClubCode,
       ReportGrouping,
	   ReportRunDateTime


----- to union Employee, club, Area and Company level records into a single output

SELECT ReportDateDimDateKey,
	   RevenueMMSClubID,
	   PrimarySalesEmployeeID,
	   NULL  AS ProductCategory,
	   BusinessType,
	   BusinessSubType,
	   MTDRevenueItemAmount,
	   Today_ItemAmount,
	   ForecastAmount,
	   ReportDate,
	   PersonalTrainingRegionalCategoryLeadAreaName,
	   ClubName,
	   MMSClubID,
	   ClubCode,
	   EmployeeID,
	   FirstName,
	   LastName,
       ReportGrouping,
	   ReportRunDateTime,
	   0 AS BudgetAmount
FROM #EmployeeDetail

UNION ALL

SELECT ClubSummary.ReportDateDimDateKey,
	   ClubSummary.RevenueMMSClubID,
	   ClubSummary.PrimarySalesEmployeeID,    ------ -95
	   NULL AS ProductCategory,
	   ClubSummary.BusinessType,
	   ClubSummary.BusinessSubType,
	   ClubSummary.MTDRevenueItemAmount,
	   ClubSummary.Today_ItemAmount,
	   ClubSummary.ForecastAmount,
	   ClubSummary.ReportDate,
	   ClubSummary.PersonalTrainingRegionalCategoryLeadAreaName,
	   ClubSummary.ClubName,
	   ClubSummary.MMSClubID,
	   ClubSummary.ClubCode,
	   ClubSummary.EmployeeID,    ----- -95
	   ClubSummary.FirstName,   -------- 'Entire Club'
	   ClubSummary.LastName,   ------- ' '
       ClubSummary.ReportGrouping,
	   ClubSummary.ReportRunDateTime,
	   IsNull(Budget.BudgetAmount,0) BudgetAmount
FROM #ClubSummary ClubSummary
 LEFT JOIN #ClubBusinessSubTypeBudget  Budget
   ON ClubSUmmary.MMSClubID = Budget.ClubID
   AND ClubSUmmary.BusinessSubType = Budget.BusinessSubType
   AND ClubSummary.RowNumber = 1

UNION ALL

SELECT ClubSummary.ReportDateDimDateKey,
	   -1 AS RevenueMMSClubID,
	   -96 AS PrimarySalesEmployeeID,
	   NULL AS ProductCategory,
	   ClubSummary.BusinessType,
	   ClubSummary.BusinessSubType,
	   SUM(ClubSummary.MTDRevenueItemAmount) AS MTDRevenueItemAmount,
	   SUM(ClubSummary.Today_ItemAmount) AS Today_ItemAmount,
	   SUM(ClubSummary.ForecastAmount) AS ForecastAmount,
	   ClubSummary.ReportDate,
	   ClubSummary.PersonalTrainingRegionalCategoryLeadAreaName,
	   ' Entire Area -'+' '+ClubSummary.PersonalTrainingRegionalCategoryleadAreaName AS ClubName,
	   -1 AS MMSClubID,
	   'ALL' AS ClubCode,
	   -96 AS EmployeeID,
	   'Entire Area' AS FirstName,
	   ' ' AS LastName,
       ClubSummary.ReportGrouping,
	   ClubSummary.ReportRunDateTime,
	   SUM(IsNull(Budget.BudgetAmount,0)) AS BudgetAmount
FROM #ClubSummary ClubSummary
 LEFT JOIN #ClubBusinessSubTypeBudget  Budget
   ON ClubSummary.MMSClubID = Budget.ClubID
   AND ClubSummary.BusinessSubType = Budget.BusinessSubType
   AND ClubSummary.RowNumber = 1
 GROUP BY ClubSummary.ReportDateDimDateKey,
	   ClubSummary.BusinessType,
	   ClubSummary.BusinessSubType,
	   ClubSummary.ReportDate,
	   ClubSummary.PersonalTrainingRegionalCategoryLeadAreaName,
       ClubSummary.ReportGrouping,
	   ClubSummary.ReportRunDateTime

UNION ALL

SELECT ClubSummary.ReportDateDimDateKey,
	   -1 AS RevenueMMSClubID,
	   -97 AS PrimarySalesEmployeeID,
	   NULL AS ProductCategory,
	   ClubSummary.BusinessType,
	   ClubSummary.BusinessSubType,
	   SUM(ClubSummary.MTDRevenueItemAmount) AS MTDRevenueItemAmount,
	   SUM(ClubSummary.Today_ItemAmount) AS Today_ItemAmount,
	   SUM(ClubSummary.ForecastAmount) AS ForecastAmount,
	   ClubSummary.ReportDate,
	   'Entire Company' AS PersonalTrainingRegionalCategoryLeadAreaName,
	   '  Entire Company' AS ClubName,
	   -1 AS MMSClubID,
	   'ALL' AS ClubCode,
	   -97 AS EmployeeID,
	   'Entire Company' AS FirstName,
	   ' ' AS LastName,
       ClubSummary.ReportGrouping,
	   ClubSummary.ReportRunDateTime,
	   SUM(IsNull(Budget.BudgetAmount,0)) AS BudgetAmount
FROM #ClubSummary ClubSummary
 LEFT JOIN #ClubBusinessSubTypeBudget  Budget
   ON ClubSummary.MMSClubID = Budget.ClubID
   AND ClubSummary.BusinessSubType = Budget.BusinessSubType
   AND ClubSummary.RowNumber = 1
 GROUP BY ClubSummary.ReportDateDimDateKey,
	   ClubSummary.BusinessType,
	   ClubSummary.BusinessSubType,
	   ClubSummary.ReportDate,
       ClubSummary.ReportGrouping,
	   ClubSummary.ReportRunDateTime


DROP TABLE #ClubBusinessSubTypeBudget
DROP TABLE #SummaryTableDataPrelim
DROP TABLE #SummaryTableData
DROP TABLE #DimClubKeyList
DROP TABLE #EmployeeIDList
DROP TABLE #AllEmployeeBusinessTypeOptions
DROP TABLE #EmployeeDetail
DROP TABLE #ClubSummary



END
