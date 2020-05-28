CREATE PROC [reporting].[proc_PTTrainer_DSSR_ClientRetentionSummary] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 

 ------
 --- Used by Informatica to populate the sandbox table “rep.ClientRetentionSummary”
 ------

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @ReportDate [DATETIME] = '1/1/1900'
SET @ReportDate = CASE WHEN  @ReportDate = '1/1/1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END

--------------------------------------------------------------------------3 Month Retention Period analysis -------------------------------------------------------
DECLARE @RetentionPeriod varchar(15) = '3 months' -- 3 6 9 12


IF OBJECT_ID('tempdb.dbo.#dates', 'U') IS NOT NULL
	DROP TABLE #dates;

SELECT Month_Starting_Dim_Date_Key,
       Dim_Date_Key,
       Four_Digit_Year_Dash_Two_Digit_Month,
       Calendar_Date
  INTO #Dates
 FROM [marketing].[v_dim_date] 
 WHERE Dim_Date_Key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END 
						  
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#Detail', 'U') IS NOT NULL
	DROP TABLE #Detail;

SELECT RetentionDetail.delivered_month_starting_dim_date_key AS MonthStartingDimDateKey, 
       RetentionDetail.delivered_dim_employee_key AS DeliveredDimEmployeeKey, 
       RetentionDetail.dim_member_key AS DimMemberKey,      ------- New Name
       RetentionDetail.delivered_price AS DeliveredPrice,
       RetentionDetail.delivered_four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth
  INTO #Detail     
FROM [dbo].[fact_ptdssr_client_retention_detail] RetentionDetail
  JOIN #Dates dd																	--Come back to correct the sandbox db. 
    ON RetentionDetail.delivered_date_dim_date_key = dd.Dim_Date_Key
  WHERE one_on_one_pt_product_flag = 'Y'
 


/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession;

  ----- find count of sessions per customer per month
 SELECT DimMemberKey,
        MonthStartingDimDateKey,
        COUNT(*) AS CustomerMonthSessionCount
INTO #CustomerMonthSession      
 FROM #Detail
 GROUP BY DimMemberKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth;
  ---- find the first session month
SELECT d1.DimMemberKey, 
       MAX(d1.MonthStartingDimDateKey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth    
 FROM #CustomerMonthSession d1
 JOIN [marketing].[v_Dim_Date] DeliveryMonth 
   ON d1.MonthStartingDimDateKey = DeliveryMonth.dim_date_key
 LEFT JOIN #CustomerMonthSession d2 
   ON d1.DimMemberKey = d2.DimMemberKey 
   AND DeliveryMonth.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimMemberKey is null
GROUP BY d1.DimMemberKey

-- to count months since the member's first session
IF OBJECT_ID('tempdb.dbo.#CustomerTenure', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure;

SELECT #CustomerMaxMonth.DimMemberKey,
       datediff(month,DeliveryMonth.calendar_date, ReportDate.month_starting_date)+1 AS Tenure --plus 1 to include the current month
INTO #CustomerTenure  
 FROM #CustomerMaxMonth
 JOIN [marketing].[v_dim_date] DeliveryMonth  
   ON #CustomerMaxMonth.MaxMonthStartingDimDateKey = DeliveryMonth.Dim_Date_Key
 JOIN [marketing].[v_dim_date] ReportDate 
   ON ReportDate.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



--create list of customers for report period(month)
IF OBJECT_ID('tempdb.dbo.#CurrentMonthSummary', 'U') IS NOT NULL
	DROP TABLE #CurrentMonthSummary;

SELECT #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       SUM(#Detail.DeliveredPrice) DeliveredPrice,
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey,
	   COUNT(#Detail.MonthStartingDimDateKey) AS ReportMonthSessionCount
INTO #CurrentMonthSummary    
  FROM #Detail
  JOIN #CustomerTenure
    ON #Detail.DimMemberKey = #CustomerTenure.DimMemberKey
 WHERE #Detail.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #Dates WHERE Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey



--create retention period session summary for current Month customers
IF OBJECT_ID('tempdb.dbo.#SessionSummary', 'U') IS NOT NULL
	DROP TABLE #SessionSummary;

SELECT MAX(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(MonthStartingDimDateKey) SessionCount,
       SUM(DeliveredPrice) SessionAmount,
       MAX(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #SessionSummary   
  FROM #Detail 
 WHERE DimMemberKey in (SELECT distinct DimMemberKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey




  --- Collect full retention Period client count for employees who delivered sessions in the current period
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients;

SELECT #Detail.DeliveredDimEmployeeKey,
       COUNT(distinct #Detail.DimMemberKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients 
  FROM #Detail
 WHERE #Detail.DeliveredDimEmployeeKey in (SELECT distinct DeliveredDimEmployeeKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#TenureData', 'U') IS NOT NULL
	DROP TABLE #TenureData;

SELECT #CurrentMonthSummary.DeliveredDimEmployeeKey,
       SUM(#CurrentMonthSummary.Tenure) AS CustomerTenure, 
       COUNT(#CurrentMonthSummary.DimMemberKey) AS CustomerCount,
       CAST(SUM(#CurrentMonthSummary.Tenure) AS decimal (12,2)) / CAST(COUNT(#CurrentMonthSummary.DimMemberKey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#CurrentMonthSummary.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#CurrentMonthSummary.MonthStartingDimDateKey) AS MonthStartingDimDateKey
INTO #TenureData   
  FROM #CurrentMonthSummary
  JOIN #RetentionPeriodClients
    ON #CurrentMonthSummary.DeliveredDimEmployeeKey = #RetentionPeriodClients.DeliveredDimEmployeeKey
 GROUP BY #CurrentMonthSummary.DeliveredDimEmployeeKey




IF OBJECT_ID('tempdb.dbo.#3MonthData', 'U') IS NOT NULL
	DROP TABLE #3MonthData;

SELECT 
	   PTRCLArea.description AS RegionName,
	   DimClub.club_id AS MMSClubID,
       DimClub.club_name As ClubName,         
       DimClub.club_code As ClubCode,
       DimClub.dim_club_key As DimClubKey, 
       DimEmployee.first_name As FirstName, 
       DimEmployee.last_name As LastName,  
       DimEmployee.employee_id As EmployeeID,
       #TenureData.CustomerCount AS CurrentClients, 
       (#SessionSummary.SessionCount / #TenureData.CustomerCount) AS AverageClientSessions,
       cast(#SessionSummary.sessionamount AS decimal(12,2)) / cast(#SessionSummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData.AverageTenure AS AverageTenure_Months,
       #TenureData.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData.CustomerCount)/Convert(decimal(12,1),#TenureData.RetentionPeriodClientCount)) AS ClientRetentionRate,
       @ReportRunDateTime AS ReportRunDateTime,
       #SessionSummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,         
       #SessionSummary.DeliveredDimEmployeeKey,           
       @ReportDate AS HeaderReportDate,
	   #SessionSummary.SessionCount,
	   #SessionSummary.SessionAmount,
	   #TenureData.CustomerTenure
INTO #3MonthData     
  FROM #TenureData
  JOIN #SessionSummary 
    ON #TenureData.DeliveredDimEmployeeKey = #SessionSummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_dim_employee] DimEmployee 
    ON #TenureData.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] As DimClub 
    ON DimEmployee.dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_description] PTRCLArea
	ON PTRCLArea.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
 WHERE #SessionSummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))

 

DROP TABLE #Dates
DROP TABLE #Detail
DROP TABLE #CustomerMonthSession 
DROP TABLE #CustomerMaxMonth
DROP TABLE #CustomerTenure
DROP TABLE #CurrentMonthSummary
DROP TABLE #SessionSummary
DROP TABLE #RetentionPeriodClients
DROP TABLE #TenureData


-------------------------------------------------------------------------------6 month data retention period analysis ---------------------------------------

SET @RetentionPeriod = '6 months' -- 3 6 9 12


IF OBJECT_ID('tempdb.dbo.#dates', 'U') IS NOT NULL
	DROP TABLE #dates;

SELECT Month_Starting_Dim_Date_Key,
       Dim_Date_Key,
       Four_Digit_Year_Dash_Two_Digit_Month,
       Calendar_Date
  INTO #Dates
 FROM [marketing].[v_dim_date] 
 WHERE Dim_Date_Key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END 
						  
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#Detail', 'U') IS NOT NULL
	DROP TABLE #Detail;

SELECT RetentionDetail.delivered_month_starting_dim_date_key AS MonthStartingDimDateKey, 
       RetentionDetail.delivered_dim_employee_key AS DeliveredDimEmployeeKey, 
       RetentionDetail.dim_member_key AS DimMemberKey,      ------- New Name
       RetentionDetail.delivered_price AS DeliveredPrice,
       RetentionDetail.delivered_four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth
  INTO #Detail     
FROM [dbo].[fact_ptdssr_client_retention_detail] RetentionDetail

  JOIN #Dates dd																	
    ON RetentionDetail.delivered_date_dim_date_key = dd.Dim_Date_Key
  WHERE one_on_one_pt_product_flag = 'Y'



/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession;

  ----- find count of sessions per customer per month
 SELECT DimMemberKey,
        MonthStartingDimDateKey,
        COUNT(*) AS CustomerMonthSessionCount
INTO #CustomerMonthSession      
 FROM #Detail
 GROUP BY DimMemberKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth;
  ---- find the first session month
SELECT d1.DimMemberKey, 
       MAX(d1.MonthStartingDimDateKey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth    
 FROM #CustomerMonthSession d1
 JOIN [marketing].[v_Dim_Date] DeliveryMonth --vdimdate dd 
   ON d1.MonthStartingDimDateKey = DeliveryMonth.dim_date_key
 LEFT JOIN #CustomerMonthSession d2 
   ON d1.DimMemberKey = d2.DimMemberKey 
   AND DeliveryMonth.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimMemberKey is null
GROUP BY d1.DimMemberKey

-- to count months since the member's first session
IF OBJECT_ID('tempdb.dbo.#CustomerTenure', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure;

SELECT #CustomerMaxMonth.DimMemberKey,
       datediff(month,DeliveryMonth.calendar_date, ReportDate.month_starting_date)+1 AS Tenure --plus 1 to include the current month
INTO #CustomerTenure  
 FROM #CustomerMaxMonth
 JOIN [marketing].[v_dim_date] DeliveryMonth  
   ON #CustomerMaxMonth.MaxMonthStartingDimDateKey = DeliveryMonth.Dim_Date_Key
 JOIN [marketing].[v_dim_date] ReportDate 
   ON ReportDate.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



--create list of customers for report period(month)
IF OBJECT_ID('tempdb.dbo.#CurrentMonthSummary', 'U') IS NOT NULL
	DROP TABLE #CurrentMonthSummary;

SELECT #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       SUM(#Detail.DeliveredPrice) DeliveredPrice,
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey,
	   COUNT(#Detail.MonthStartingDimDateKey) AS ReportMonthSessionCount
INTO #CurrentMonthSummary    
  FROM #Detail
  JOIN #CustomerTenure
    ON #Detail.DimMemberKey = #CustomerTenure.DimMemberKey
 WHERE #Detail.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #Dates WHERE Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey



--create retention period session summary for current Month customers
IF OBJECT_ID('tempdb.dbo.#SessionSummary', 'U') IS NOT NULL
	DROP TABLE #SessionSummary;

SELECT MAX(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(MonthStartingDimDateKey) SessionCount,
       SUM(DeliveredPrice) SessionAmount,
       MAX(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #SessionSummary   
  FROM #Detail 
 WHERE DimMemberKey in (SELECT distinct DimMemberKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey




  --- Collect full retention Period client count for employees who delivered sessions in the current period
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients;

SELECT #Detail.DeliveredDimEmployeeKey,
       COUNT(distinct #Detail.DimMemberKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients 
  FROM #Detail
 WHERE #Detail.DeliveredDimEmployeeKey in (SELECT distinct DeliveredDimEmployeeKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#TenureData', 'U') IS NOT NULL
	DROP TABLE #TenureData;

SELECT #CurrentMonthSummary.DeliveredDimEmployeeKey,
       SUM(#CurrentMonthSummary.Tenure) AS CustomerTenure, 
       COUNT(#CurrentMonthSummary.DimMemberKey) AS CustomerCount,
       CAST(SUM(#CurrentMonthSummary.Tenure) AS decimal (12,2)) / CAST(COUNT(#CurrentMonthSummary.DimMemberKey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#CurrentMonthSummary.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#CurrentMonthSummary.MonthStartingDimDateKey) AS MonthStartingDimDateKey
INTO #TenureData   
  FROM #CurrentMonthSummary
  JOIN #RetentionPeriodClients
    ON #CurrentMonthSummary.DeliveredDimEmployeeKey = #RetentionPeriodClients.DeliveredDimEmployeeKey
 GROUP BY #CurrentMonthSummary.DeliveredDimEmployeeKey




IF OBJECT_ID('tempdb.dbo.#6MonthData', 'U') IS NOT NULL
	DROP TABLE #6MonthData;

SELECT  
	   PTRCLArea.description AS RegionName,
	   DimClub.club_id AS MMSClubID,
       DimClub.club_name As ClubName,         
       DimClub.club_code As ClubCode,
       DimClub.dim_club_key As DimClubKey, 
       DimEmployee.first_name As FirstName, 
       DimEmployee.last_name As LastName,  
       DimEmployee.employee_id As EmployeeID,
       #TenureData.CustomerCount AS CurrentClients, 
       (#SessionSummary.SessionCount / #TenureData.CustomerCount) AS AverageClientSessions,
       cast(#SessionSummary.sessionamount AS decimal(12,2)) / cast(#SessionSummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData.AverageTenure AS AverageTenure_Months,
       #TenureData.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData.CustomerCount)/Convert(decimal(12,1),#TenureData.RetentionPeriodClientCount)) AS ClientRetentionRate,
       @ReportRunDateTime AS ReportRunDateTime,
       #SessionSummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,         
       #SessionSummary.DeliveredDimEmployeeKey,           
       @ReportDate AS HeaderReportDate,
	   #SessionSummary.SessionCount,
	   #SessionSummary.SessionAmount,
	   #TenureData.CustomerTenure
INTO #6MonthData     
  FROM #TenureData
  JOIN #SessionSummary 
    ON #TenureData.DeliveredDimEmployeeKey = #SessionSummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_dim_employee] DimEmployee 
    ON #TenureData.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] As DimClub 
    ON DimEmployee.dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_description] PTRCLArea
	ON PTRCLArea.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
 WHERE #SessionSummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 



DROP TABLE #Dates
DROP TABLE #Detail
DROP TABLE #CustomerMonthSession 
DROP TABLE #CustomerMaxMonth
DROP TABLE #CustomerTenure
DROP TABLE #CurrentMonthSummary
DROP TABLE #SessionSummary
DROP TABLE #RetentionPeriodClients
DROP TABLE #TenureData

----------------------------------------------------------------------9 Months Retention Period Analysis ----------------------------------------------------------------

SET @RetentionPeriod = '9 months' -- 3 6 9 12


IF OBJECT_ID('tempdb.dbo.#dates', 'U') IS NOT NULL
	DROP TABLE #dates;

SELECT Month_Starting_Dim_Date_Key,
       Dim_Date_Key,
       Four_Digit_Year_Dash_Two_Digit_Month,
       Calendar_Date
  INTO #Dates
 FROM [marketing].[v_dim_date] 
 WHERE Dim_Date_Key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END 
						  
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#Detail', 'U') IS NOT NULL
	DROP TABLE #Detail;

SELECT RetentionDetail.delivered_month_starting_dim_date_key AS MonthStartingDimDateKey, 
       RetentionDetail.delivered_dim_employee_key AS DeliveredDimEmployeeKey, 
       RetentionDetail.dim_member_key AS DimMemberKey,      ------- New Name
       RetentionDetail.delivered_price AS DeliveredPrice,
       RetentionDetail.delivered_four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth
  INTO #Detail     
FROM [dbo].[fact_ptdssr_client_retention_detail] RetentionDetail
  JOIN #Dates dd																	 
    ON RetentionDetail.delivered_date_dim_date_key = dd.Dim_Date_Key
  WHERE one_on_one_pt_product_flag = 'Y'


/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession;

  ----- find count of sessions per customer per month
 SELECT DimMemberKey,
        MonthStartingDimDateKey,
        COUNT(*) AS CustomerMonthSessionCount
INTO #CustomerMonthSession      
 FROM #Detail
 GROUP BY DimMemberKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth;
  ---- find the first session month
SELECT d1.DimMemberKey, 
       MAX(d1.MonthStartingDimDateKey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth    
 FROM #CustomerMonthSession d1
 JOIN [marketing].[v_Dim_Date] DeliveryMonth 
   ON d1.MonthStartingDimDateKey = DeliveryMonth.dim_date_key
 LEFT JOIN #CustomerMonthSession d2 
   ON d1.DimMemberKey = d2.DimMemberKey 
   AND DeliveryMonth.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimMemberKey is null
GROUP BY d1.DimMemberKey

-- to count months since the member's first session
IF OBJECT_ID('tempdb.dbo.#CustomerTenure', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure;

SELECT #CustomerMaxMonth.DimMemberKey,
       datediff(month,DeliveryMonth.calendar_date, ReportDate.month_starting_date)+1 AS Tenure --plus 1 to include the current month
INTO #CustomerTenure  
 FROM #CustomerMaxMonth
 JOIN [marketing].[v_dim_date] DeliveryMonth  
   ON #CustomerMaxMonth.MaxMonthStartingDimDateKey = DeliveryMonth.Dim_Date_Key
 JOIN [marketing].[v_dim_date] ReportDate 
   ON ReportDate.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



--create list of customers for report period(month)
IF OBJECT_ID('tempdb.dbo.#CurrentMonthSummary', 'U') IS NOT NULL
	DROP TABLE #CurrentMonthSummary;

SELECT #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       SUM(#Detail.DeliveredPrice) DeliveredPrice,
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey,
	   COUNT(#Detail.MonthStartingDimDateKey) AS ReportMonthSessionCount
INTO #CurrentMonthSummary    
  FROM #Detail
  JOIN #CustomerTenure
    ON #Detail.DimMemberKey = #CustomerTenure.DimMemberKey
 WHERE #Detail.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #Dates WHERE Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey



--create retention period session summary for current Month customers
IF OBJECT_ID('tempdb.dbo.#SessionSummary', 'U') IS NOT NULL
	DROP TABLE #SessionSummary;

SELECT MAX(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(MonthStartingDimDateKey) SessionCount,
       SUM(DeliveredPrice) SessionAmount,
       MAX(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #SessionSummary   
  FROM #Detail 
 WHERE DimMemberKey in (SELECT distinct DimMemberKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey




  --- Collect full retention Period client count for employees who delivered sessions in the current period
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients;

SELECT #Detail.DeliveredDimEmployeeKey,
       COUNT(distinct #Detail.DimMemberKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients 
  FROM #Detail
 WHERE #Detail.DeliveredDimEmployeeKey in (SELECT distinct DeliveredDimEmployeeKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#TenureData', 'U') IS NOT NULL
	DROP TABLE #TenureData;

SELECT #CurrentMonthSummary.DeliveredDimEmployeeKey,
       SUM(#CurrentMonthSummary.Tenure) AS CustomerTenure, 
       COUNT(#CurrentMonthSummary.DimMemberKey) AS CustomerCount,
       CAST(SUM(#CurrentMonthSummary.Tenure) AS decimal (12,2)) / CAST(COUNT(#CurrentMonthSummary.DimMemberKey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#CurrentMonthSummary.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#CurrentMonthSummary.MonthStartingDimDateKey) AS MonthStartingDimDateKey
INTO #TenureData   
  FROM #CurrentMonthSummary
  JOIN #RetentionPeriodClients
    ON #CurrentMonthSummary.DeliveredDimEmployeeKey = #RetentionPeriodClients.DeliveredDimEmployeeKey
 GROUP BY #CurrentMonthSummary.DeliveredDimEmployeeKey




IF OBJECT_ID('tempdb.dbo.#9MonthData', 'U') IS NOT NULL
	DROP TABLE #9MonthData;

SELECT 
	   PTRCLArea.description AS RegionName,
	   DimClub.club_id AS MMSClubID,
       DimClub.club_name As ClubName,         
       DimClub.club_code As ClubCode,
       DimClub.dim_club_key As DimClubKey, 
       DimEmployee.first_name As FirstName, 
       DimEmployee.last_name As LastName,  
       DimEmployee.employee_id As EmployeeID,
       #TenureData.CustomerCount AS CurrentClients, 
       (#SessionSummary.SessionCount / #TenureData.CustomerCount) AS AverageClientSessions,
       cast(#SessionSummary.sessionamount AS decimal(12,2)) / cast(#SessionSummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData.AverageTenure AS AverageTenure_Months,
       #TenureData.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData.CustomerCount)/Convert(decimal(12,1),#TenureData.RetentionPeriodClientCount)) AS ClientRetentionRate,
       @ReportRunDateTime AS ReportRunDateTime,
       #SessionSummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,        
       #SessionSummary.DeliveredDimEmployeeKey,            
       @ReportDate AS HeaderReportDate,
	   #SessionSummary.SessionCount,
	   #SessionSummary.SessionAmount,
	   #TenureData.CustomerTenure
INTO #9MonthData     
  FROM #TenureData
  JOIN #SessionSummary 
    ON #TenureData.DeliveredDimEmployeeKey = #SessionSummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_dim_employee] DimEmployee 
    ON #TenureData.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] As DimClub 
    ON DimEmployee.dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_description] PTRCLArea
	ON PTRCLArea.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
 WHERE #SessionSummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 



DROP TABLE #Dates
DROP TABLE #Detail
DROP TABLE #CustomerMonthSession 
DROP TABLE #CustomerMaxMonth
DROP TABLE #CustomerTenure
DROP TABLE #CurrentMonthSummary
DROP TABLE #SessionSummary
DROP TABLE #RetentionPeriodClients
DROP TABLE #TenureData

---------------------------------------------------------------------- 12 Months Retention Period Analysis ----------------------------------------------------------------
 
SET @RetentionPeriod = '12 months' -- 3 6 9 12

IF OBJECT_ID('tempdb.dbo.#dates', 'U') IS NOT NULL
	DROP TABLE #dates;

SELECT Month_Starting_Dim_Date_Key,
       Dim_Date_Key,
       Four_Digit_Year_Dash_Two_Digit_Month,
       Calendar_Date
  INTO #Dates
 FROM [marketing].[v_dim_date] 
 WHERE Dim_Date_Key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END 
						  
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#Detail', 'U') IS NOT NULL
	DROP TABLE #Detail;

SELECT RetentionDetail.delivered_month_starting_dim_date_key AS MonthStartingDimDateKey, 
       RetentionDetail.delivered_dim_employee_key AS DeliveredDimEmployeeKey, 
       RetentionDetail.dim_member_key AS DimMemberKey,      ------- New Name
       RetentionDetail.delivered_price AS DeliveredPrice,
       RetentionDetail.delivered_four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth
  INTO #Detail     
FROM [dbo].[fact_ptdssr_client_retention_detail] RetentionDetail
  JOIN #Dates dd																
    ON RetentionDetail.delivered_date_dim_date_key = dd.Dim_Date_Key
  WHERE one_on_one_pt_product_flag = 'Y'



/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession;

  ----- find count of sessions per customer per month
 SELECT DimMemberKey,
        MonthStartingDimDateKey,
        COUNT(*) AS CustomerMonthSessionCount
INTO #CustomerMonthSession      
 FROM #Detail
 GROUP BY DimMemberKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth;
  ---- find the first session month
SELECT d1.DimMemberKey, 
       MAX(d1.MonthStartingDimDateKey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth    
 FROM #CustomerMonthSession d1
 JOIN [marketing].[v_Dim_Date] DeliveryMonth  
   ON d1.MonthStartingDimDateKey = DeliveryMonth.dim_date_key
 LEFT JOIN #CustomerMonthSession d2 
   ON d1.DimMemberKey = d2.DimMemberKey 
   AND DeliveryMonth.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimMemberKey is null
GROUP BY d1.DimMemberKey

-- to count months since the member's first session
IF OBJECT_ID('tempdb.dbo.#CustomerTenure', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure;

SELECT #CustomerMaxMonth.DimMemberKey,
       datediff(month,DeliveryMonth.calendar_date, ReportDate.month_starting_date)+1 AS Tenure --plus 1 to include the current month
INTO #CustomerTenure  
 FROM #CustomerMaxMonth
 JOIN [marketing].[v_dim_date] DeliveryMonth  
   ON #CustomerMaxMonth.MaxMonthStartingDimDateKey = DeliveryMonth.Dim_Date_Key
 JOIN [marketing].[v_dim_date] ReportDate 
   ON ReportDate.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



--create list of customers for report period(month)
IF OBJECT_ID('tempdb.dbo.#CurrentMonthSummary', 'U') IS NOT NULL
	DROP TABLE #CurrentMonthSummary;

SELECT #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       SUM(#Detail.DeliveredPrice) DeliveredPrice,
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey,
	   COUNT(#Detail.MonthStartingDimDateKey) AS ReportMonthSessionCount
INTO #CurrentMonthSummary    
  FROM #Detail
  JOIN #CustomerTenure
    ON #Detail.DimMemberKey = #CustomerTenure.DimMemberKey
 WHERE #Detail.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #Dates WHERE Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #Detail.DeliveredDimEmployeeKey, 
       #Detail.DimMemberKey, 
       #CustomerTenure.Tenure,
       #Detail.FourDigitYearDashTwoDigitMonth,
       #Detail.MonthStartingDimDateKey



--create retention period session summary for current Month customers
IF OBJECT_ID('tempdb.dbo.#SessionSummary', 'U') IS NOT NULL
	DROP TABLE #SessionSummary;

SELECT MAX(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(MonthStartingDimDateKey) SessionCount,
       SUM(DeliveredPrice) SessionAmount,
       MAX(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #SessionSummary   
  FROM #Detail 
 WHERE DimMemberKey in (SELECT distinct DimMemberKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey




  --- Collect full retention Period client count for employees who delivered sessions in the current period
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients;

SELECT #Detail.DeliveredDimEmployeeKey,
       COUNT(distinct #Detail.DimMemberKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients 
  FROM #Detail
 WHERE #Detail.DeliveredDimEmployeeKey in (SELECT distinct DeliveredDimEmployeeKey FROM #CurrentMonthSummary)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#TenureData', 'U') IS NOT NULL
	DROP TABLE #TenureData;

SELECT #CurrentMonthSummary.DeliveredDimEmployeeKey,
       SUM(#CurrentMonthSummary.Tenure) AS CustomerTenure, 
       COUNT(#CurrentMonthSummary.DimMemberKey) AS CustomerCount,
       CAST(SUM(#CurrentMonthSummary.Tenure) AS decimal (12,2)) / CAST(COUNT(#CurrentMonthSummary.DimMemberKey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#CurrentMonthSummary.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#CurrentMonthSummary.MonthStartingDimDateKey) AS MonthStartingDimDateKey
INTO #TenureData   
  FROM #CurrentMonthSummary
  JOIN #RetentionPeriodClients
    ON #CurrentMonthSummary.DeliveredDimEmployeeKey = #RetentionPeriodClients.DeliveredDimEmployeeKey
 GROUP BY #CurrentMonthSummary.DeliveredDimEmployeeKey




IF OBJECT_ID('tempdb.dbo.#12MonthData', 'U') IS NOT NULL
	DROP TABLE #12MonthData;

SELECT 
	   PTRCLArea.description AS RegionName,
	   DimClub.club_id AS MMSClubID,
       DimClub.club_name As ClubName,         
       DimClub.club_code As ClubCode,
       DimClub.dim_club_key As DimClubKey, 
       DimEmployee.first_name As FirstName, 
       DimEmployee.last_name As LastName,  
       DimEmployee.employee_id As EmployeeID,
       #TenureData.CustomerCount AS CurrentClients, 
       (#SessionSummary.SessionCount / #TenureData.CustomerCount) AS AverageClientSessions,
       cast(#SessionSummary.sessionamount AS decimal(12,2)) / cast(#SessionSummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData.AverageTenure AS AverageTenure_Months,
       #TenureData.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData.CustomerCount)/Convert(decimal(12,1),#TenureData.RetentionPeriodClientCount)) AS ClientRetentionRate,
       @ReportRunDateTime AS ReportRunDateTime,
       #SessionSummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,       
       #SessionSummary.DeliveredDimEmployeeKey,          
       @ReportDate AS HeaderReportDate,
	   #SessionSummary.SessionCount,
	   #SessionSummary.SessionAmount,
	   #TenureData.CustomerTenure
INTO #12MonthData     
  FROM #TenureData
  JOIN #SessionSummary 
    ON #TenureData.DeliveredDimEmployeeKey = #SessionSummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_dim_employee] DimEmployee 
    ON #TenureData.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] As DimClub 
    ON DimEmployee.dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_description] PTRCLArea
	ON PTRCLArea.dim_description_key = DimClub.pt_rcl_area_dim_description_key 
 WHERE #SessionSummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))




DROP TABLE #Dates
DROP TABLE #Detail
DROP TABLE #CustomerMonthSession 
DROP TABLE #CustomerMaxMonth
DROP TABLE #CustomerTenure
DROP TABLE #CurrentMonthSummary
DROP TABLE #SessionSummary
DROP TABLE #RetentionPeriodClients
DROP TABLE #TenureData



IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
	DROP TABLE #Results;

   ---- Union all 4 result sets

    SELECT RegionName, 
	   MMSClubID,
       ClubName,         
       ClubCode,
       DimClubKey, 
       FirstName, 
       LastName,  
       EmployeeID,
       CurrentClients, 
       AverageClientSessions, 
       AverageSessionPrice,--- avg session price of retained clients
       AverageTenure_Months,
       RetentionPeriodClientCount, 
       ClientRetentionRate,
       ReportRunDateTime,
       HeaderReportMonth,
       HeaderRetentionPeriod,
       DeliveredDimEmployeeKey AS DimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  INTO #Results
  FROM #3MonthData

   UNION 

   SELECT RegionName, 
       MMSClubID,
       ClubName,         
       ClubCode,
       DimClubKey, 
       FirstName, 
       LastName,  
       EmployeeID,
       CurrentClients, 
       AverageClientSessions, 
       AverageSessionPrice,--- avg session price of retained clients
       AverageTenure_Months,
       RetentionPeriodClientCount, 
       ClientRetentionRate,
       ReportRunDateTime,
       HeaderReportMonth,
       HeaderRetentionPeriod,
       DeliveredDimEmployeeKey  AS DimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  FROM #6MonthData

   UNION 

   SELECT RegionName,
       MMSClubID, 
       ClubName,         
       ClubCode,
       DimClubKey, 
       FirstName, 
       LastName,  
       EmployeeID,
       CurrentClients, 
       AverageClientSessions, 
       AverageSessionPrice,--- avg session price of retained clients
       AverageTenure_Months,
       RetentionPeriodClientCount, 
       ClientRetentionRate,
       ReportRunDateTime,
       HeaderReportMonth,
       HeaderRetentionPeriod,
       DeliveredDimEmployeeKey  AS DimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  FROM #9MonthData

   UNION

      SELECT RegionName, 
	   MMSClubID,
       ClubName,         
       ClubCode,
       DimClubKey, 
       FirstName, 
       LastName,  
       EmployeeID,
       CurrentClients, 
       AverageClientSessions, 
       AverageSessionPrice,--- avg session price of retained clients
       AverageTenure_Months,
       RetentionPeriodClientCount, 
       ClientRetentionRate,
       ReportRunDateTime,
       HeaderReportMonth,
       HeaderRetentionPeriod,
       DeliveredDimEmployeeKey  AS DimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
   FROM #12MonthData

----- to union Employee, club, Area and Company level records into a single output

 SELECT RegionName,
		ClubName,
		ClubCode,
		MMSClubID,
		DimClubKey  AS DimLocationKey,
		EmployeeID,
		FirstName,
		LastName,
		CurrentClients,
		AverageClientSessions,
		AverageSessionPrice,
		AverageTenure_Months,
		RetentionPeriodClientCount,
		ClientRetentionRate,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		DimEmployeeKey,
		HeaderReportDate,
		SessionCount,
		SessionAmount,
		CustomerTenure
FROM #Results

UNION ALL

 SELECT RegionName,
		ClubName,
		ClubCode,
		MMSClubID,
		DimClubKey,
		-95 AS EmployeeID,
		'Entire Club' AS FirstName,
		' ' AS LastName,
		SUM(CurrentClients) AS CurrentClients,
		CASE WHEN SUM(IsNull(SessionCount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionCount)/SUM(CurrentClients)) 
			 END AverageClientSessions,
		CASE WHEN SUM(IsNull(SessionAmount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(SessionCount,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionAmount)/SUM(SessionCount)) 
			 END AverageSessionPrice,
		CASE WHEN SUM(IsNull(CustomerTenure,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE(SUM(CustomerTenure)/SUM(CurrentClients)) 
			 END AverageTenure_Months,
		SUM(RetentionPeriodClientCount) AS RetentionPeriodClientCount,		
		CASE WHEN SUM(IsNull(CurrentClients,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(RetentionPeriodClientCount,0)) = 0
			 THEN 0
			 ELSE (SUM(CurrentClients)/SUM(RetentionPeriodClientCount))
			 END ClientRetentionRate,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		'0' AS DimEmployeeKey,
		HeaderReportDate,
		SUM(SessionCount) AS SessionCount,
		SUM(SessionAmount) AS SessionAmount,
		SUM(CustomerTenure) AS CustomerTenure
FROM #Results
GROUP BY RegionName,
		ClubName,
		ClubCode,
		MMSClubID,
		DimClubKey,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		HeaderReportDate

UNION ALL

 SELECT RegionName,
		' Entire Area -'+' '+RegionName AS ClubName,
		'All' AS ClubCode,
		-1 AS MMSClubID,
		'0' AS DimClubKey,
		-96 AS EmployeeID,
		'Entire Area' AS FirstName,
		' ' AS LastName,
		SUM(CurrentClients) AS CurrentClients,
		CASE WHEN SUM(IsNull(SessionCount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionCount)/SUM(CurrentClients)) 
			 END AverageClientSessions,
		CASE WHEN SUM(IsNull(SessionAmount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(SessionCount,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionAmount)/SUM(SessionCount)) 
			 END AverageSessionPrice,
		CASE WHEN SUM(IsNull(CustomerTenure,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE(SUM(CustomerTenure)/SUM(CurrentClients)) 
			 END AverageTenure_Months,
		SUM(RetentionPeriodClientCount) AS RetentionPeriodClientCount,		
		CASE WHEN SUM(IsNull(CurrentClients,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(RetentionPeriodClientCount,0)) = 0
			 THEN 0
			 ELSE (SUM(CurrentClients)/SUM(RetentionPeriodClientCount))
			 END ClientRetentionRate,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		'0' AS DimEmployeeKey,
		HeaderReportDate,
		SUM(SessionCount) AS SessionCount,
		SUM(SessionAmount) AS SessionAmount,
		SUM(CustomerTenure) AS CustomerTenure
FROM #Results
GROUP BY RegionName,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		HeaderReportDate


UNION ALL

 SELECT 'Entire Company' AS RegionName,
		'  Entire Company' AS ClubName,
		'All' AS ClubCode,
		-1 AS MMSClubID,
		'0' AS DimClubKey,
		-97 AS EmployeeID,
		'Entire Company' AS FirstName,
		' ' AS LastName,
		SUM(CurrentClients) AS CurrentClients,
		CASE WHEN SUM(IsNull(SessionCount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionCount)/SUM(CurrentClients)) 
			 END AverageClientSessions,
		CASE WHEN SUM(IsNull(SessionAmount,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(SessionCount,0)) = 0
			 THEN 0
			 ELSE (SUM(SessionAmount)/SUM(SessionCount)) 
			 END AverageSessionPrice,
		CASE WHEN SUM(IsNull(CustomerTenure,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(CurrentClients,0)) = 0
			 THEN 0
			 ELSE(SUM(CustomerTenure)/SUM(CurrentClients)) 
			 END AverageTenure_Months,
		SUM(RetentionPeriodClientCount) AS RetentionPeriodClientCount,		
		CASE WHEN SUM(IsNull(CurrentClients,0)) = 0
		     THEN 0
			 WHEN SUM(ISNull(RetentionPeriodClientCount,0)) = 0
			 THEN 0
			 ELSE (SUM(CurrentClients)/SUM(RetentionPeriodClientCount))
			 END ClientRetentionRate,
		HeaderReportMonth,
		HeaderRetentionPeriod,
		'0' AS DimEmployeeKey,
		HeaderReportDate,
		SUM(SessionCount) AS SessionCount,
		SUM(SessionAmount) AS SessionAmount,
		SUM(CustomerTenure) AS CustomerTenure
FROM #Results
GROUP BY 
		HeaderReportMonth,
		HeaderRetentionPeriod,
		HeaderReportDate



 DROP TABLE #Results


END
