CREATE PROC [reporting].[proc_PTTrainer_DSSR_RevenueAndServiceEmployeeSummary] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
    END


 ------
 --- Used by Informatica to populate the sandbox table “rep.RevenueandServiceEmployeeSummary”
 ------


DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = Replace(Substring(convert(varchar,getdate(),100),1,6)+', '+Substring(convert(varchar,GETDATE(),100),8,10)+' '+Substring(convert(varchar,getdate(),100),18,2),'  ',' ')

DECLARE @ReportDate Datetime = '1/1/1900'
SET @ReportDate = CASE WHEN @ReportDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END


DECLARE @ReportDateDimDateKey VARCHAR(32)
DECLARE @ReportDateIsLastDayInMonthIndicator Varchar(1)
DECLARE @FirstOf13MonthsPriorDimDateKey VARCHAR(32)
SET @ReportDateDimDateKey = (Select dim_date_key from [marketing].[v_dim_date] where calendar_date = @ReportDate)
SET @ReportDateIsLastDayInMonthIndicator = (Select last_day_in_month_flag from [marketing].[v_dim_date] where calendar_date = @ReportDate)
SET @FirstOf13MonthsPriorDimDateKey = (Select dim_date_key from [marketing].[v_dim_date] where calendar_date = DATEADD(m,-13, @ReportDate))


IF OBJECT_ID('tempdb.dbo.#SummaryTableDataPrelim', 'U') IS NOT NULL
  DROP TABLE #SummaryTableDataPrelim;

SELECT SummaryTable.report_date_dim_date_key AS ReportDateDimDateKey,
DimClub.club_id AS RevenueMMSClubID,
DimEmployee.employee_id AS PrimarySalesEmployeeID,
DimEmployee.first_name AS FirstName,
DimEmployee.last_name AS LastName,
SummaryTable.ptdssr_category AS PTDSSRCategory,
SummaryTable.ptdssr_row_label AS PTDSSRRowLabel,
Categories.CategoryDisplayOrder,
SummaryTable.reporting_data_source AS ReportingDataSource,
SummaryTable.month_to_date_item_amount AS MonthToDateItemAmount,
SummaryTable.report_date_item_amount AS ReportDateItemAmount,
SummaryTable.forecast_amount AS ForecastAmount,
SummaryTable.one_on_one_product_grouping AS OneOnOneProductGrouping,
SummaryTable.month_to_date_session_sales_count AS MonthToDate_SessionSalesCount,
SummaryTable.month_to_date_session_sales_amount AS MonthToDate_SessionSalesAmount,
SummaryTable.month_to_date_session_delivered_count AS MonthToDate_SessionDeliveredCount,
SummaryTable.month_to_date_session_delivered_amount AS MonthToDate_SessionDeliveredAmount,
SummaryTable.month_to_date_session_adjustment_count AS MonthToDate_SessionAdjustmentCount,
SummaryTable.month_to_date_session_adjustment_amount AS MonthToDate_SessionAdjustmentAmount,
SummaryTable.report_date_session_outstanding_count AS ReportDate_SessionOutstandingCount,
SummaryTable.report_date_session_outstanding_amount AS ReportDate_SessionOutstandingAmount,
SummaryTable.club_goal AS ClubGoal,
SummaryTable.report_date_session_sales_count AS ReportDate_SessionSalesCount,
SummaryTable.report_date_session_sales_amount AS ReportDate_SessionSalesAmount,
SummaryTable.report_date_session_delivered_count AS ReportDate_SessionDeliveredCount,
SummaryTable.report_date_session_delivered_amount AS ReportDate_SessionDeliveredAmount,
SummaryTable.report_date_session_adjustment_count AS ReportDate_SessionAdjustmentCount,
SummaryTable.report_date_session_adjustment_amount AS ReportDate_SessionAdjustmentAmount,
SummaryTable.dim_reporting_hierarchy_key AS DimReportingHierarchyKey
INTO #SummaryTableDataPrelim 
FROM [dbo].[fact_ptdssr_revenue_and_service_employee_summary]  SummaryTable
JOIN [marketing].[v_dim_club] DimClub
 ON SummaryTable.dim_club_key = DimClub.dim_club_key
JOIN [marketing].[v_dim_employee] DimEmployee
  ON SummaryTable.dim_employee_key = DimEmployee.dim_employee_key
JOIN [reporting].[v_PTDSSR_MoveIt_KnowIt_NourishIt] Categories
  ON SummaryTable.dim_reporting_hierarchy_key = Categories.dim_reporting_hierarchy_key
   AND Categories.effective_dim_date_key <= @ReportDateDimDateKey
   AND Categories.expiration_dim_date_key > @ReportDateDimDateKey
   AND Categories.ActiveFlag = 'Y'
WHERE SummaryTable.report_date_dim_date_key = @ReportDateDimDateKey 


IF OBJECT_ID('tempdb.dbo.#SummaryTableData', 'U') IS NOT NULL
  DROP TABLE #SummaryTableData;

SELECT ReportDateDimDateKey,
RevenueMMSClubID,
PrimarySalesEmployeeID,
FirstName,
LastName,
PTDSSRCategory,
PTDSSRRowLabel,
CategoryDisplayOrder,
ReportingDataSource,
Sum(MonthToDateItemAmount) AS MonthToDateItemAmount,
Sum(ReportDateItemAmount) AS ReportDateItemAmount,
Sum(ForecastAmount) AS ForecastAmount,
OneOnOneProductGrouping,
Sum(MonthToDate_SessionSalesCount) AS MonthToDate_SessionSalesCount,
Sum(MonthToDate_SessionSalesAmount) AS MonthToDate_SessionSalesAmount,
Sum(MonthToDate_SessionDeliveredCount) AS MonthToDate_SessionDeliveredCount,
Sum(MonthToDate_SessionDeliveredAmount) AS MonthToDate_SessionDeliveredAmount,
Sum(MonthToDate_SessionAdjustmentCount) AS MonthToDate_SessionAdjustmentCount,
Sum(MonthToDate_SessionAdjustmentAmount) AS MonthToDate_SessionAdjustmentAmount,
Sum(ReportDate_SessionOutstandingCount) AS ReportDate_SessionOutstandingCount,
Sum(ReportDate_SessionOutstandingAmount) AS ReportDate_SessionOutstandingAmount,
Sum(ClubGoal) AS ClubGoal,
Sum(ReportDate_SessionSalesCount) AS ReportDate_SessionSalesCount,
Sum(ReportDate_SessionSalesAmount) AS ReportDate_SessionSalesAmount,
Sum(ReportDate_SessionDeliveredCount) AS ReportDate_SessionDeliveredCount,
Sum(ReportDate_SessionDeliveredAmount) AS ReportDate_SessionDeliveredAmount,
Sum(ReportDate_SessionAdjustmentCount) AS ReportDate_SessionAdjustmentCount,
Sum(ReportDate_SessionAdjustmentAmount) AS ReportDate_SessionAdjustmentAmount,
DimReportingHierarchyKey
INTO #SummaryTableData 
FROM #SummaryTableDataPrelim 
GROUP BY ReportDateDimDateKey,
RevenueMMSClubID,
PrimarySalesEmployeeID,
FirstName,
LastName,
PTDSSRCategory,
PTDSSRRowLabel,
CategoryDisplayOrder,
ReportingDataSource,
OneOnOneProductGrouping,
DimReportingHierarchyKey


  ------ Goal is at the DimReportingHierarchyKey level by Club.
  ------ To summarize the goal at the Club/Category/Row Label to join in later

IF OBJECT_ID('tempdb.dbo.#GoalPreGrouping', 'U') IS NOT NULL
  DROP TABLE #GoalPreGrouping;

SELECT RevenueMMSClubID,
       DimReportingHierarchyKey,
	   PTDSSRCategory,
       PTDSSRRowLabel,
       Max(IsNull(ClubGoal,0)) AS ClubGoal 
INTO #GoalPreGrouping
  FROM #SummaryTableData  
 GROUP BY   RevenueMMSClubID,
            DimReportingHierarchyKey, 
		    PTDSSRCategory,
            PTDSSRRowLabel

IF OBJECT_ID('tempdb.dbo.#ClubCategoryRowLabelGoal', 'U') IS NOT NULL
  DROP TABLE #ClubCategoryRowLabelGoal;

Select RevenueMMSClubID,
       PTDSSRCategory,
       PTDSSRRowLabel,
	   SUM(ClubGoal) AS ClubGoal
INTO #ClubCategoryRowLabelGoal
  FROM #GoalPreGrouping
  GROUP BY RevenueMMSClubID,
           PTDSSRCategory,
           PTDSSRRowLabel




 ----- To get a list of all possible category and row labels
IF OBJECT_ID('tempdb.dbo.#CategoryAndTypeOptions', 'U') IS NOT NULL
  DROP TABLE #CategoryAndTypeOptions;

SELECT PTDSSRCategory,
       PTDSSRRowLabel,
       ReportingDataSource,
	   CategoryDisplayOrder,
	   OneOnOneProductGrouping
INTO #CategoryAndTypeOptions
FROM #SummaryTableData
GROUP BY PTDSSRCategory,
         PTDSSRRowLabel,
         ReportingDataSource,
		 CategoryDisplayOrder,
		 OneOnOneProductGrouping


 ------ To get a list of all possible EmployeeIDs within each club
 IF OBJECT_ID('tempdb.dbo.#EmployeeIDList', 'U') IS NOT NULL
  DROP TABLE #EmployeeIDList;

SELECT PrimarySalesEmployeeID,
       RevenueMMSClubID,
	   FirstName,
       LastName
INTO #EmployeeIDList
FROM #SummaryTableData
GROUP BY PrimarySalesEmployeeID,RevenueMMSClubID,FirstName,LastName

 ------ To assign all possible row labels to each employee
  IF OBJECT_ID('tempdb.dbo.#AllEmployeeCategoryTypeOptions', 'U') IS NOT NULL
  DROP TABLE #AllEmployeeCategoryTypeOptions;

SELECT PrimarySalesEmployeeID,
RevenueMMSClubID,
FirstName,
LastName,
PTDSSRCategory,
PTDSSRRowLabel,
ReportingDataSource,
CategoryDisplayOrder,
OneOnOneProductGrouping
INTO #AllEmployeeCategoryTypeOptions
FROM #EmployeeIDList
CROSS JOIN #CategoryAndTypeOptions


  ------ to place the entire club under a single "employee" so that a full club's data set can be returned at the employee level
  IF OBJECT_ID('tempdb.dbo.#ClubTotals', 'U') IS NOT NULL
  DROP TABLE #ClubTotals;

  SELECT @ReportDateDimDateKey AS ReportDateDimDateKey,
	#AllOptions.RevenueMMSClubID,
	-95 AS PrimarySalesEmployeeID,
	#AllOptions.PTDSSRCategory,
	#AllOptions.CategoryDisplayOrder,
	#AllOptions.PTDSSRRowLabel,
	#AllOptions.ReportingDataSource,
	#AllOptions.OneOnOneProductGrouping,
	SUM(IsNull(SummaryTableData.MonthToDateItemAmount,0)) AS MTDRevenueItemAmount,
	SUM(IsNull(SummaryTableData.ReportDateItemAmount,0)) AS Today_ItemAmount,
	SUM(IsNull(SummaryTableData.ForecastAmount,0)) AS ForecastAmount,
	0 AS ReportDateDeliveredSessionPrice,
	0 AS MonthToDateDeliveredSessionPrice,
	@ReportDate AS ReportDate,
	PTRCLArea.description AS PersonalTrainingRegionalCategoryLeadAreaName,
	DimClub.club_name AS ClubName,
	DimClub.club_id AS MMSClubID,
	DimClub.club_code AS ClubCode,
	-95 AS EmployeeID,
	'Entire Club' AS FirstName,
	' ' AS LastName,
	@ReportRunDateTime AS ReportRunDateTime,
	SUM(IsNull(SummaryTableData.MonthToDate_SessionSalesCount,0)) AS MonthToDate_SessionSalesCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionSalesAmount,0)) AS MonthToDate_SessionSalesAmount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionDeliveredCount,0)) AS MonthToDate_SessionDeliveredCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionDeliveredAmount,0)) AS MonthToDate_SessionDeliveredAmount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionAdjustmentCount,0)) AS MonthToDate_SessionAdjustmentCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionAdjustmentAmount,0)) AS MonthToDate_SessionAdjustmentAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionOutstandingCount,0)) AS ReportDate_SessionOutstandingCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionOutstandingAmount,0)) AS ReportDate_SessionOutstandingAmount,
    MAX(IsNull(Goal.ClubGoal,0)) AS ClubCategoryRowLabelGoal,
	@ReportDateIsLastDayInMonthIndicator AS ReportDateIsLastDayInMonthIndicator,
	SUM(IsNull(SummaryTableData.ReportDate_SessionSalesCount,0)) AS ReportDate_SessionSalesCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionSalesAmount,0)) AS ReportDate_SessionSalesAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionDeliveredCount,0)) AS ReportDate_SessionDeliveredCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionDeliveredAmount,0)) AS ReportDate_SessionDeliveredAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionAdjustmentCount,0)) AS ReportDate_SessionAdjustmentCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionAdjustmentAmount,0)) AS ReportDate_SessionAdjustmentAmount,
	DimClub.club_code +'-'+ #AllOptions.PTDSSRCategory +'-'+ #AllOptions.PTDSSRRowLabel AS GoalGrouping
INTO #ClubTotals
	FROM #AllEmployeeCategoryTypeOptions #AllOptions
	LEFT JOIN #SummaryTableData SummaryTableData
      ON #AllOptions.PrimarySalesEmployeeID = SummaryTableData.PrimarySalesEmployeeID
       AND #AllOptions.PTDSSRCategory = SummaryTableData.PTDSSRCategory
       AND #AllOptions.PTDSSRRowLabel= SummaryTableData.PTDSSRRowLabel
       AND #AllOptions.ReportingDataSource= SummaryTableData.ReportingDataSource
	   AND #AllOptions.RevenueMMSClubID = SummaryTableData.RevenueMMSClubID
	   AND #AllOptions.OneOnOneProductGrouping = SummaryTableData.OneOnOneProductGrouping
	JOIN [marketing].[v_dim_club] DimClub
      ON #AllOptions.RevenueMMSClubID = DimClub.club_id
	JOIN [marketing].[v_dim_description] PTRCLArea
	  ON DimClub.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
	LEFT JOIN #ClubCategoryRowLabelGoal Goal
	  ON #AllOptions.RevenueMMSClubID = Goal.RevenueMMSClubID
	    AND #AllOptions.PTDSSRCategory = Goal.PTDSSRCategory
		AND #AllOptions.PTDSSRRowLabel = Goal.PTDSSRRowLabel
    GROUP BY 	#AllOptions.RevenueMMSClubID,
	#AllOptions.PTDSSRCategory,
	#AllOptions.CategoryDisplayOrder,
	#AllOptions.PTDSSRRowLabel,
	#AllOptions.ReportingDataSource,
	#AllOptions.OneOnOneProductGrouping,
    PTRCLArea.description,
	DimClub.club_name,
	DimClub.club_id,
	DimClub.club_code

 ----- set ranking on this Club file so that duplicate row label goal values can be identified

  IF OBJECT_ID('tempdb.dbo.#RankedClubTotals', 'U') IS NOT NULL
  DROP TABLE #RankedClubTotals;

 SELECT ReportDateDimDateKey,
	RevenueMMSClubID,
	PrimarySalesEmployeeID,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
	MTDRevenueItemAmount,
	Today_ItemAmount,
	ForecastAmount,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	PersonalTrainingRegionalCategoryLeadAreaName,
	ClubName,
	MMSClubID,
	ClubCode,
	EmployeeID,
	FirstName,
	LastName,
	ReportRunDateTime,
	MonthToDate_SessionSalesCount,
    MonthToDate_SessionSalesAmount,
    MonthToDate_SessionDeliveredCount,
    MonthToDate_SessionDeliveredAmount,
    MonthToDate_SessionAdjustmentCount,
    MonthToDate_SessionAdjustmentAmount,
    ReportDate_SessionOutstandingCount,
    ReportDate_SessionOutstandingAmount,
    ClubCategoryRowLabelGoal,
	ReportDateIsLastDayInMonthIndicator,
	ReportDate_SessionSalesCount,
    ReportDate_SessionSalesAmount,
    ReportDate_SessionDeliveredCount,
    ReportDate_SessionDeliveredAmount,
    ReportDate_SessionAdjustmentCount,
    ReportDate_SessionAdjustmentAmount,
	GoalGrouping,
	RANK() OVER(PARTITION BY GoalGrouping														----- grouping to be ranked
                       ORDER BY ReportingDataSource,OneOnOneProductGrouping) GoalGroupRanking   ----- values which make these group items different
INTO #RankedClubTotals
	FROM #ClubTotals



SELECT @ReportDateDimDateKey AS ReportDateDimDateKey,
	#AllOptions.RevenueMMSClubID,
	#AllOptions.PrimarySalesEmployeeID,
	#AllOptions.PTDSSRCategory,
	#AllOptions.CategoryDisplayOrder,
	#AllOptions.PTDSSRRowLabel,
	#AllOptions.ReportingDataSource,
	#AllOptions.OneOnOneProductGrouping,
	SUM(IsNull(SummaryTableData.MonthToDateItemAmount,0)) AS MTDRevenueItemAmount,
	SUM(IsNull(SummaryTableData.ReportDateItemAmount,0)) AS Today_ItemAmount,
	SUM(IsNull(SummaryTableData.ForecastAmount,0)) AS ForecastAmount,
	0 AS ReportDateDeliveredSessionPrice,
	0 AS MonthToDateDeliveredSessionPrice,
	@ReportDate AS ReportDate,
	PTRCLArea.Description AS PersonalTrainingRegionalCategoryLeadAreaName,
	DimClub.club_name AS ClubName,
	DimClub.club_id AS MMSClubID,
	DimClub.club_code AS ClubCode,
	#AllOptions.PrimarySalesEmployeeID AS EmployeeID,
	#AllOptions.FirstName,
	#AllOptions.LastName,
	@ReportRunDateTime AS ReportRunDateTime,
	SUM(IsNull(SummaryTableData.MonthToDate_SessionSalesCount,0)) AS MonthToDate_SessionSalesCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionSalesAmount,0)) AS MonthToDate_SessionSalesAmount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionDeliveredCount,0)) AS MonthToDate_SessionDeliveredCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionDeliveredAmount,0)) AS MonthToDate_SessionDeliveredAmount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionAdjustmentCount,0)) AS MonthToDate_SessionAdjustmentCount,
    SUM(IsNull(SummaryTableData.MonthToDate_SessionAdjustmentAmount,0)) AS MonthToDate_SessionAdjustmentAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionOutstandingCount,0)) AS ReportDate_SessionOutstandingCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionOutstandingAmount,0)) AS ReportDate_SessionOutstandingAmount,
    0 AS ClubCategoryRowLabelGoal,
	@ReportDateIsLastDayInMonthIndicator AS ReportDateIsLastDayInMonthIndicator,
    SUM(IsNull(SummaryTableData.ReportDate_SessionSalesCount,0)) AS ReportDate_SessionSalesCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionSalesAmount,0)) AS ReportDate_SessionSalesAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionDeliveredCount,0)) AS ReportDate_SessionDeliveredCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionDeliveredAmount,0)) AS ReportDate_SessionDeliveredAmount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionAdjustmentCount,0)) AS ReportDate_SessionAdjustmentCount,
    SUM(IsNull(SummaryTableData.ReportDate_SessionAdjustmentAmount,0)) AS ReportDate_SessionAdjustmentAmount
FROM #AllEmployeeCategoryTypeOptions #AllOptions
	LEFT JOIN #SummaryTableData SummaryTableData
      ON #AllOptions.PrimarySalesEmployeeID = SummaryTableData.PrimarySalesEmployeeID
       AND #AllOptions.PTDSSRCategory = SummaryTableData.PTDSSRCategory
       AND #AllOptions.PTDSSRRowLabel= SummaryTableData.PTDSSRRowLabel
       AND #AllOptions.ReportingDataSource= SummaryTableData.ReportingDataSource
	   AND #AllOptions.RevenueMMSClubID = SummaryTableData.RevenueMMSClubID
	   AND #AllOptions.OneOnOneProductGrouping = SummaryTableData.OneOnOneProductGrouping
    JOIN [marketing].[v_dim_club] DimClub
      ON #AllOptions.RevenueMMSClubID = DimClub.club_id
	JOIN [marketing].[v_dim_description] PTRCLArea
	  ON DimClub.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
	LEFT JOIN #ClubCategoryRowLabelGoal Goal
	  ON #AllOptions.RevenueMMSClubID = Goal.RevenueMMSClubID
	    AND #AllOptions.PTDSSRCategory = Goal.PTDSSRCategory
		AND #AllOptions.PTDSSRRowLabel = Goal.PTDSSRRowLabel
    GROUP BY 	#AllOptions.RevenueMMSClubID,
	#AllOptions.PrimarySalesEmployeeID,
	#AllOptions.PTDSSRCategory,
	#AllOptions.CategoryDisplayOrder,
	#AllOptions.PTDSSRRowLabel,
	#AllOptions.ReportingDataSource,
	#AllOptions.OneOnOneProductGrouping,
	PTRCLArea.Description,
	DimClub.club_name,
	DimClub.club_id,
	DimClub.club_code,
	#AllOptions.PrimarySalesEmployeeID,
	#AllOptions.FirstName,
	#AllOptions.LastName

UNION All 

------ to return the entire club under a single "employee" so that a full club's data set can be returned at the employee level
SELECT ReportDateDimDateKey,
	RevenueMMSClubID,
	PrimarySalesEmployeeID,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
	MTDRevenueItemAmount,
	Today_ItemAmount,
	ForecastAmount,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	PersonalTrainingRegionalCategoryLeadAreaName,
	ClubName,
	MMSClubID,
	ClubCode,
	EmployeeID,
	FirstName,
	LastName,
	ReportRunDateTime,
	MonthToDate_SessionSalesCount,
    MonthToDate_SessionSalesAmount,
    MonthToDate_SessionDeliveredCount,
    MonthToDate_SessionDeliveredAmount,
    MonthToDate_SessionAdjustmentCount,
    MonthToDate_SessionAdjustmentAmount,
    ReportDate_SessionOutstandingCount,
    ReportDate_SessionOutstandingAmount,
    CASE WHEN GoalGroupRanking = 1
	     THEN ClubCategoryRowLabelGoal
		 ELSE 0
		 END ClubCategoryRowLabelGoal,
	ReportDateIsLastDayInMonthIndicator,
	ReportDate_SessionSalesCount,
    ReportDate_SessionSalesAmount,
    ReportDate_SessionDeliveredCount,
    ReportDate_SessionDeliveredAmount,
    ReportDate_SessionAdjustmentCount,
    ReportDate_SessionAdjustmentAmount
FROM #RankedClubTotals





UNION All 

  ------ to place the entire Area under a single "employee" so that a full area's data set can be returned at the employee level
SELECT ReportDateDimDateKey,
	-1 AS RevenueMMSClubID,
	-96 AS PrimarySalesEmployeeID,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
    SUM(IsNull(MTDRevenueItemAmount,0)) AS MTDRevenueItemAmount,
	SUM(IsNull(Today_ItemAmount,0)) AS Today_ItemAmount,
	SUM(IsNull(ForecastAmount,0)) AS ForecastAmount,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	PersonalTrainingRegionalCategoryLeadAreaName,
	' Entire Area -'+' '+PersonalTrainingRegionalCategoryleadAreaName AS ClubName,
	-1 AS MMSClubID,
	'ALL' AS ClubCode,
	-96 AS EmployeeID,
	'Entire Area' AS FirstName,
	' ' AS LastName,
	ReportRunDateTime,
	SUM(MonthToDate_SessionSalesCount) AS MonthToDate_SessionSalesCount,
    SUM(MonthToDate_SessionSalesAmount) AS MonthToDate_SessionSalesAmount,
    SUM(MonthToDate_SessionDeliveredCount) AS MonthToDate_SessionDeliveredCount,
    SUM(MonthToDate_SessionDeliveredAmount) AS MonthToDate_SessionDeliveredAmount,
    SUM(MonthToDate_SessionAdjustmentCount) AS MonthToDate_SessionAdjustmentCount,
    SUM(MonthToDate_SessionAdjustmentAmount) AS MonthToDate_SessionAdjustmentAmount,
    SUM(ReportDate_SessionOutstandingCount) AS ReportDate_SessionOutstandingCount,
    SUM(ReportDate_SessionOutstandingAmount) AS ReportDate_SessionOutstandingAmount,
    SUM(CASE WHEN GoalGroupRanking = 1
	     THEN ClubCategoryRowLabelGoal
		 ELSE 0
		 END) ClubCategoryRowLabelGoal,
	ReportDateIsLastDayInMonthIndicator,
	SUM(ReportDate_SessionSalesCount) AS ReportDate_SessionSalesCount,
    SUM(ReportDate_SessionSalesAmount) AS ReportDate_SessionSalesAmount,
    SUM(ReportDate_SessionDeliveredCount) AS ReportDate_SessionDeliveredCount,
    SUM(ReportDate_SessionDeliveredAmount) AS ReportDate_SessionDeliveredAmount,
    SUM(ReportDate_SessionAdjustmentCount) AS ReportDate_SessionAdjustmentCount,
    SUM(ReportDate_SessionAdjustmentAmount) AS ReportDate_SessionAdjustmentAmount
FROM #RankedClubTotals
GROUP BY ReportDateDimDateKey,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	PersonalTrainingRegionalCategoryLeadAreaName,
	ReportRunDateTime,
	ReportDateIsLastDayInMonthIndicator



UNION All 

  ------ to place the entire Company under a single "employee" so that a full Company's data set can be returned at the employee level

SELECT ReportDateDimDateKey,
	-1 AS RevenueMMSClubID,
	-97 AS PrimarySalesEmployeeID,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
    SUM(IsNull(MTDRevenueItemAmount,0)) AS MTDRevenueItemAmount,
	SUM(IsNull(Today_ItemAmount,0)) AS Today_ItemAmount,
	SUM(IsNull(ForecastAmount,0)) AS ForecastAmount,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	'Entire Company' AS PersonalTrainingRegionalCategoryLeadAreaName,
	'  Entire Company' AS ClubName,
	-1 AS MMSClubID,
	'ALL' AS ClubCode,
	-97 AS EmployeeID,
	'Entire Company' AS FirstName,
	' ' AS LastName,
	ReportRunDateTime,
	SUM(MonthToDate_SessionSalesCount) AS MonthToDate_SessionSalesCount,
    SUM(MonthToDate_SessionSalesAmount) AS MonthToDate_SessionSalesAmount,
    SUM(MonthToDate_SessionDeliveredCount) AS MonthToDate_SessionDeliveredCount,
    SUM(MonthToDate_SessionDeliveredAmount) AS MonthToDate_SessionDeliveredAmount,
    SUM(MonthToDate_SessionAdjustmentCount) AS MonthToDate_SessionAdjustmentCount,
    SUM(MonthToDate_SessionAdjustmentAmount) AS MonthToDate_SessionAdjustmentAmount,
    SUM(ReportDate_SessionOutstandingCount) AS ReportDate_SessionOutstandingCount,
    SUM(ReportDate_SessionOutstandingAmount) AS ReportDate_SessionOutstandingAmount,
    SUM(CASE WHEN GoalGroupRanking = 1
	     THEN ClubCategoryRowLabelGoal
		 ELSE 0
		 END) ClubCategoryRowLabelGoal,
	ReportDateIsLastDayInMonthIndicator,
	SUM(ReportDate_SessionSalesCount) AS ReportDate_SessionSalesCount,
    SUM(ReportDate_SessionSalesAmount) AS ReportDate_SessionSalesAmount,
    SUM(ReportDate_SessionDeliveredCount) AS ReportDate_SessionDeliveredCount,
    SUM(ReportDate_SessionDeliveredAmount) AS ReportDate_SessionDeliveredAmount,
    SUM(ReportDate_SessionAdjustmentCount) AS ReportDate_SessionAdjustmentCount,
    SUM(ReportDate_SessionAdjustmentAmount) AS ReportDate_SessionAdjustmentAmount
FROM #RankedClubTotals
GROUP BY ReportDateDimDateKey,
	PTDSSRCategory,
	CategoryDisplayOrder,
	PTDSSRRowLabel,
	ReportingDataSource,
	OneOnOneProductGrouping,
	ReportDateDeliveredSessionPrice,
	MonthToDateDeliveredSessionPrice,
	ReportDate,
	ReportRunDateTime,
	ReportDateIsLastDayInMonthIndicator


	 ORDER BY 	RevenueMMSClubID,PrimarySalesEmployeeID,PTDSSRCategory,PTDSSRRowLabel


DROP TABLE #SummaryTableData
DROP TABLE #SummaryTableDataPrelim
DROP TABLE #CategoryAndTypeOptions
DROP TABLE #EmployeeIDList
DROP TABLE #AllEmployeeCategoryTypeOptions
DROP TABLE #GoalPreGrouping
DROP TABLE #ClubCategoryRowLabelGoal
DROP TABLE #ClubTotals
DROP TABLE #RankedClubTotals


END




