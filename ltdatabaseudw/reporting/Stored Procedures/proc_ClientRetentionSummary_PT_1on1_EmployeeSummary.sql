CREATE PROC [reporting].[proc_ClientRetentionSummary_PT_1on1_EmployeeSummary] @ReportDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON
----------------------------------parameter to test it out. Remove it once done development
--DECLARE @ReportDate DATETIME = '11/01/2019'

 ------
 --- Used by Alteryx to populate the new sandbox table “rep.FactPTDSSRClientRetentionEmployeeSummary”
 ------




SET @ReportDate = CASE WHEN  @ReportDate = 'Jan 1, 1900' 
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
  INTO #dates
 FROM [marketing].[v_dim_date] 
 WHERE Dim_Date_Key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE Calendar_Date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END 
						  
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tembdb.dbo.#detail', 'U') IS NOT NULL
	DROP TABLE #detail;

SELECT fps.[fact_mms_package_session_key] FactPackageSessionKey,
       dd.month_starting_dim_date_key MonthStartingDimDateKey, 
       fps.[delivered_dim_employee_key] DeliveredDimEmployeeKey, 
       fps.[dim_mms_member_key] DimCustomerKey,
       fps.[delivered_session_price] DeliveredSessionPrice,
       dd.four_digit_year_dash_two_digit_month FourDigitYearDashTwoDigitMonth
  INTO #detail
FROM [marketing].[v_fact_mms_package_session] fps --vFactPackageSession fps
  JOIN [marketing].[v_dim_date] DeliveredDimDate --vDimDate DeliveredDimDate
    ON fps.delivered_dim_date_key = DeliveredDimDate.Dim_Date_key
  JOIN #dates dd																	--Come back to correct the sandbox db. 
    ON DeliveredDimDate.[month_starting_dim_date_key] = dd.Dim_Date_Key
WHERE fps.[fact_mms_package_dim_product_key] IN (SELECT [DimProductKey] FROM [reporting].[v_PTDSSR_OneOnOneProduct])

AND fps.[delivered_session_price] > 0


/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession;
  ----- find count of sessions per customer per month
 SELECT DimCustomerKey,
       MonthStartingDimDateKey,
       COUNT(*) customermonthsessioncount
INTO #CustomerMonthSession
 FROM #detail
 GROUP BY DimCustomerKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth;
  ---- find the most recent session month
SELECT d1.dimcustomerkey, MAX(d1.Monthstartingdimdatekey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth
 FROM #CustomerMonthSession d1
 JOIN [marketing].[v_Dim_Date] dd --vdimdate dd 
   ON d1.monthstartingdimdatekey = dd.dim_date_key
 LEFT JOIN #CustomerMonthSession d2 
   ON d1.dimcustomerkey = d2.dimcustomerkey 
   AND dd.prior_month_starting_dim_date_key = d2.monthstartingdimdatekey
WHERE d2.DimCustomerKey is null
GROUP BY d1.dimcustomerkey

--cont
IF OBJECT_ID('tempdb.dbo.#CustomerTenure', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure;

SELECT #customerMaxMonth.DimCustomerKey,
       datediff(month,dd.calendar_date, td.month_starting_date)+1 tenure --plus 1 to include the current month
INTO #CustomerTenure
 FROM #CustomerMaxMonth
 JOIN [marketing].[v_dim_date] dd --vdimdate dd 
   ON #CustomerMaxMonth.MaxMonthStartingDimDateKey = dd.Dim_Date_Key
 JOIN [marketing].[v_dim_date] td --vdimdate td 
   ON td.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)





--create list of customers for report period(month)
IF OBJECT_ID('tempdb.dbo.#currentmonthsummary', 'U') IS NOT NULL
	DROP TABLE #currentmonthsummary;

SELECT #detail.delivereddimemployeekey, 
       #detail.DimCustomerKey, 
       SUM(#detail.DeliveredSessionPrice) DeliveredSessionPrice,
       #CustomerTenure.tenure,
       #detail.FourDigitYearDashTwoDigitMonth,
       #detail.MonthStartingDimDateKey,
	   COUNT(#detail.FactPackageSessionKey) AS ReportMonthSessionCount
INTO #currentmonthsummary
  FROM #detail
  JOIN #CustomerTenure
    ON #detail.dimcustomerkey = #CustomerTenure.DimCustomerKey
 WHERE #detail.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #dates WHERE Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #detail.delivereddimemployeekey, 
       #detail.DimCustomerKey, 
       #CustomerTenure.tenure,
       #detail.FourDigitYearDashTwoDigitMonth,
       #detail.MonthStartingDimDateKey



--create retention period session summary for current Month customers
IF OBJECT_ID('tempdb.dbo.#sessionsummary', 'U') IS NOT NULL
	DROP TABLE #sessionsummary;

SELECT MAX(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(FactPackageSessionKey) SessionCount,
       SUM(DeliveredSessionPrice) SessionAmount,
       MAX(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #sessionsummary
  FROM #detail fps
 WHERE dimcustomerkey in (SELECT distinct DimCustomerKey FROM #currentmonthsummary)
 GROUP BY DeliveredDimEmployeeKey




  --- Collect full retention Period client count for employees who delivered sessions in the current period
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients;

SELECT #detail.DeliveredDimEmployeeKey,
       COUNT(distinct #detail.DimCustomerKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients
  FROM #detail
 WHERE #detail.DeliveredDimEmployeeKey in (SELECT distinct delivereddimemployeekey FROM #currentmonthsummary)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#tenuredata', 'U') IS NOT NULL
	DROP TABLE #tenuredata;

SELECT #currentmonthsummary.DeliveredDimEmployeeKey,
       SUM(#currentmonthsummary.tenure) AS CustomerTenure, 
       COUNT(#currentmonthsummary.DimCustomerKey) AS CustomerCount,
       cast(SUM(#currentmonthsummary.tenure) AS decimal (12,2)) / cast(count(#currentmonthsummary.dimcustomerkey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#currentmonthsummary.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#currentmonthsummary.monthstartingdimdatekey) AS monthstartingdimdatekey
INTO #tenuredata
  FROM #currentmonthsummary
  JOIN #RetentionPeriodClients
    ON #currentmonthsummary.DeliveredDimEmployeeKey = #RetentionPeriodClients.DeliveredDimEmployeeKey
 GROUP BY #currentmonthsummary.DeliveredDimEmployeeKey




IF OBJECT_ID('tempdb.dbo.#3MonthData', 'U') IS NOT NULL
	DROP TABLE #3MonthData;

SELECT --DimLocation.PersonalTrainingRegionalCategoryLeadAreaName AS RegionName, ----Figure it out the right column 
	   region.description AS RegionName,
       DimLocation.Club_Name As ClubName,         
       DimLocation.Club_Code As ClubCode,
       DimLocation.Dim_Club_Key As DimLocationKey, 
       DimEmployee.first_name As FirstName, 
       DimEmployee.last_name As LastName,  
       DimEmployee.employee_id As EmployeeID,
       #TenureData.CustomerCount AS CurrentClients, 
       (sessionsummary.SessionCount / #tenuredata.CustomerCount) AS AverageClientSessions,
       cast(sessionsummary.sessionamount AS decimal(12,2)) / cast(sessionsummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData.AverageTenure AS AverageTenure_Months,
       #TenureData.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData.CustomerCount)/Convert(decimal(12,1),#TenureData.RetentionPeriodClientCount)) AS ClientRetentionRate,
       cast(getdate() AS datetime) AS ReportRunDateTime,
       sessionsummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,        ---Come back to verify 
       sessionsummary.DeliveredDimEmployeeKey,           ---Come back to verify 
       @ReportDate AS HeaderReportDate,
	   sessionsummary.SessionCount,
	   sessionsummary.SessionAmount,
	   #TenureData.CustomerTenure
INTO #3MonthData
  FROM #TenureData
  JOIN #sessionsummary sessionsummary
    ON #TenureData.DeliveredDimEmployeeKey = sessionsummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_Dim_Employee] DimEmployee --vDimEmployeeActive DimEmployee
    ON #tenuredata.DeliveredDimEmployeeKey = DimEmployee.Dim_Employee_Key
  JOIN [marketing].[v_Dim_Club] As DimLocation --vDimLocationActive AS DimLocation
    ON DimEmployee.Dim_Club_Key = DimLocation.Dim_Club_key
  JOIN marketing.v_dim_description region
	ON region.dim_description_key = DimLocation.[pt_rcl_area_dim_description_key] --region_dim_description_key
 WHERE sessionsummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 --ORDER BY sessionsummary.DeliveredDimEmployeeKey,region.description, DimLocation.club_name, dimemployee.employee_id

 --Order by clause is invalid in views
 

DROP TABLE #sessionsummary
DROP TABLE #currentmonthsummary
DROP TABLE #CustomerTenure
DROP TABLE #CustomerMaxMonth
DROP TABLE #CustomerMonthSession
DROP TABLE #dates
DROP TABLE #detail
DROP TABLE #RetentionPeriodClients
DROP TABLE #tenuredata
-------------------------------------------------------------------------------6 month data retention period analysis ---------------------------------------


IF OBJECT_ID('tempdb.dbo.#dates6', 'U') IS NOT NULL
	DROP TABLE #dates6;

SET @RetentionPeriod = '6 months' -- 3 6 9 12


SELECT d.month_starting_dim_date_key,
       d.dim_date_key,
       d.four_digit_year_dash_two_digit_month,
       d.calendar_date 
  INTO #dates6
  FROM [marketing].[v_dim_date] d  --vdimdate
 WHERE d.dim_date_key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END
   AND Calendar_Date <= convert(datetime,convert(varchar,@ReportDate,110),110)
-- ORDER BY 1



IF OBJECT_ID('tempdb.dbo.#detail6', 'U') IS NOT NULL
	DROP TABLE #detail6;

SELECT fps.fact_mms_package_session_key FactPackageSessionKey,
       dd.month_starting_dim_date_key MonthStartingDimDateKey, 
       fps.delivered_dim_employee_key DeliveredDimEmployeeKey, 
       fps.dim_mms_member_key DimCustomerKey,
       fps.delivered_session_price DeliveredSessionPrice,
       dd.four_digit_year_dash_two_digit_month FourDigitYearDashTwoDigitMonth
 INTO #detail6
  FROM [marketing].[v_fact_mms_package_session] fps  
  JOIN [marketing].[v_dim_date] DeliveredDimDate 
    ON fps.delivered_dim_date_key = DeliveredDimDate.dim_date_key
  JOIN #dates6 dd 
    ON DeliveredDimDate.month_starting_dim_date_key = dd.dim_date_key
WHERE fps.fact_mms_package_dim_product_key IN (SELECT [DimProductKey] FROM [reporting].[v_PTDSSR_OneOnOneProduct])

AND fps.delivered_session_price > 0


/***************    find customer tenure    ***************/

IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession6', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession6; 

SELECT DimCustomerKey,
       MonthStartingDimDateKey,
       COUNT(*) customermonthsessioncount
INTO #CustomerMonthSession6
FROM #detail6
GROUP BY DimCustomerKey, MonthStartingDimDateKey



IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth6', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth6;

SELECT d1.dimcustomerkey, 
    MAX(d1.Monthstartingdimdatekey) MaxMonthStartingDimDateKey
INTO  #CustomerMaxMonth6
FROM #CustomerMonthSession6 d1
   JOIN [marketing].[v_dim_date] dd 
     ON d1.monthstartingdimdatekey = dd.dim_date_key
LEFT JOIN #CustomerMonthSession6 d2 
     ON d1.DimCustomerKey = d2.DimCustomerKey 
	  AND dd.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimCustomerKey is null
GROUP BY d1.dimcustomerkey


IF OBJECT_ID('tempdb.dbo.#CustomerTenure6', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure6;

SELECT #CustomerMaxMonth6.DimCustomerKey,
       datediff(month,dd.calendar_date, td.month_starting_date)+1 tenure --plus 1 to include the current month
INTO #CustomerTenure6
FROM #CustomerMaxMonth6
  JOIN [Marketing].[v_dim_date] dd 
    ON #CustomerMaxMonth6.MaxMonthStartingDimDateKey = dd.dim_date_key
  JOIN [marketing].[v_dim_date] td 
    ON td.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#currencymonthsummary6', 'U') IS NOT NULL
	DROP TABLE #currentmonthsummary6;

--create list of customers for report period(month)

SELECT #detail6.delivereddimemployeekey, 
       #detail6.DimCustomerKey, 
       SUM(#detail6.DeliveredSessionPrice) AS DeliveredSessionPrice,
       #CustomerTenure6.tenure,
       #detail6.FourDigitYearDashTwoDigitMonth,
       #detail6.MonthStartingDimDateKey
INTO #currentmonthsummary6
  FROM #detail6
  JOIN #CustomerTenure6
    ON #detail6.dimcustomerkey = #CustomerTenure6.DimCustomerKey
 WHERE #detail6.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #dates6 WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #detail6.delivereddimemployeekey, 
       #detail6.DimCustomerKey, 
       #CustomerTenure6.tenure,
       #detail6.FourDigitYearDashTwoDigitMonth,
       #detail6.MonthStartingDimDateKey

IF OBJECT_ID('tempdb.dbo.#sessionsummary6', 'U') IS NOT NULL
	DROP TABLE #sessionsummary6;

--create retention period session summary for report month customers 
SELECT Max(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(FactPackageSessionKey) SessionCount,
       SUM(DeliveredSessionPrice) SessionAmount,
       Max(FourDigitYearDashTwoDigitMonth) MaxFourDigitYearDashTwoDigitMonth
  INTO #sessionsummary6
  FROM #detail6 fps
 WHERE dimcustomerkey IN(SELECT distinct DimCustomerKey FROM #currentmonthsummary6)
 GROUP BY DeliveredDimEmployeeKey       




IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients6', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients6;

  --- Collect full retention Period client count for employees who delivered sessions in the report month

SELECT #detail6.DeliveredDimEmployeeKey,
       COUNT(distinct #detail6.DimCustomerKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients6
  FROM #detail6
 WHERE #detail6.DeliveredDimEmployeeKey IN(SELECT distinct delivereddimemployeekey FROM #currentmonthsummary6)
 GROUP BY DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#tenuredata6', 'U') IS NOT NULL
	DROP TABLE #tenuredata6;

SELECT #currentmonthsummary6.DeliveredDimEmployeeKey,
       SUM(#currentmonthsummary6.tenure) AS CustomerTenure, 
       COUNT(#currentmonthsummary6.DimCustomerKey) AS CustomerCount,
       CAST(SUM(#currentmonthsummary6.tenure) AS decimal (12,2)) / CAST(COUNT(#currentmonthsummary6.dimcustomerkey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients6.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#currentmonthsummary6.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#currentmonthsummary6.monthstartingdimdatekey) AS monthstartingdimdatekey
  INTO #tenuredata6
  FROM #currentmonthsummary6
  JOIN #RetentionPeriodClients6
    ON #currentmonthsummary6.DeliveredDimEmployeeKey = #RetentionPeriodClients6.DeliveredDimEmployeeKey
 GROUP BY #currentmonthsummary6.DeliveredDimEmployeeKey

 /*
 JOIN the vDimDescription IN udw TO GET the PersonalTrainingRegionalCategoryLeadAreaName AS RegionName
 SELECT TOP(1000) * FROM marketing.v_dim_club WHERE LIKE '%SE Greening%'
 SELECT * FROM marketing.v_dim_description WHERE description LIKE '%Canada%'
 SELECT description FROM marketing.v_dim_description

 */
 --************************JOING vDIMDESCRIPTION TO GET THE REGIONNAME TO DIMCLUB
IF OBJECT_ID('tempdb.dbo.#6MonthData', 'U') IS NOT NULL
	DROP TABLE #6MonthData;

SELECT --DimLocation.   PersonalTrainingRegionalCategoryLeadAreaName AS RegionName, 
	   region.description AS RegionName,
       DimLocation.club_name ClubName,         
       DimLocation.club_code   ClubCode,
       DimLocation.dim_club_key DimLocationKey, 
       DimEmployee.first_name FirstName, 
       DimEmployee.last_name LastName,  
       DimEmployee.employee_id EmployeeID,
       #TenureData6.CustomerCount AS CurrentClients, 
       (sessionsummary.SessionCount / #tenuredata6.CustomerCount) AS AverageClientSessions, --
       cast(sessionsummary.sessionamount AS decimal(12,2)) / cast(sessionsummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData6.AverageTenure AS AverageTenure_Months,
       #TenureData6.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData6.CustomerCount)/Convert(decimal(12,1),#TenureData6.RetentionPeriodClientCount)) AS ClientRetentionRate,
       cast(getdate() AS datetime) AS ReportRunDateTime,
       sessionsummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,
       sessionsummary.DeliveredDimEmployeeKey,
       @ReportDate  AS HeaderReportDate,
	   sessionsummary.SessionCount,
	   sessionsummary.SessionAmount,
	   #TenureData6.CustomerTenure
   INTO #6MonthData
  FROM #tenuredata6
  JOIN #sessionsummary6 sessionsummary
    ON #tenuredata6.DeliveredDimEmployeeKey = sessionsummary.DeliveredDimEmployeeKey
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON #tenuredata6.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [Marketing].[v_dim_club] AS DimLocation
    ON DimEmployee.dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] region
	ON region.dim_description_key = DimLocation.[pt_rcl_area_dim_description_key] --region_dim_description_key
 WHERE sessionsummary.MaxMonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #dates6 WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 --ORDER BY RegionName, ClubName, dimemployee.EmployeeID
 --Note: The Order By clause is invalid in views - unless TOP or FOR XML is also applied 



DROP TABLE #sessionsummary6
DROP TABLE #currentmonthsummary6
DROP TABLE #CustomerTenure6
DROP TABLE #CustomerMaxMonth6
DROP TABLE #CustomerMonthSession6
DROP TABLE #dates6
DROP TABLE #detail6
DROP TABLE #RetentionPeriodClients6
DROP TABLE #tenuredata6

----------------------------------------------------------------------9 Months Retention Period Analysis ----------------------------------------------------------------

IF OBJECT_ID('tempdb.dbo.#dates9', 'U') IS NOT NULL
	DROP TABLE #dates9;

SET @RetentionPeriod = '9 months' -- 3 6 9 12


SELECT month_starting_dim_date_key,
       dim_date_key,
       four_digit_year_dash_two_digit_month,
       calendar_date
 INTO  #dates9
 FROM [Marketing].[v_dim_date]  
 WHERE dim_date_key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM [Marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM [Marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM [Marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM [Marketing].[v_dim_date] WHERE calendar_date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END
   AND calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110)
 --ORDER BY 1

  --**********************************************************************************Sandbox Refernce db 
IF OBJECT_ID('tempdb.dbo.#detail9', 'U') IS NOT NULL
	DROP TABLE #detail9;

SELECT fps.fact_mms_package_session_key FactPackageSessionKey,
       dd.month_starting_dim_date_key MonthStartingDimDateKey, 
       fps.delivered_dim_employee_key DeliveredDimEmployeeKey, 
       fps.dim_mms_member_key DimCustomerKey,
       fps.delivered_session_price DeliveredSessionPrice,
       dd.four_digit_year_dash_two_digit_month FourDigitYearDashTwoDigitMonth
  INTO #detail9
  FROM [Marketing].[v_fact_mms_package_session] fps
  JOIN [marketing] .[v_dim_date] DeliveredDimDate
    ON fps.delivered_dim_date_key = DeliveredDimDate.dim_date_key
  JOIN #dates9 dd 
    ON DeliveredDimDate.month_starting_dim_date_key = dd.dim_date_key
WHERE fps.fact_mms_package_dim_product_key IN (SELECT [DimProductKey] FROM [reporting].[v_PTDSSR_OneOnOneProduct])

AND fps.delivered_session_price > 0


/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession9', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession9;

SELECT DimCustomerKey,
       MonthStartingDimDateKey,
       COUNT(*) customermonthsessioncount
INTO #CustomerMonthSession9
FROM #detail9
GROUP BY DimCustomerKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth9', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth9;

SELECT d1.dimcustomerkey, 
       MAX(d1.Monthstartingdimdatekey) MaxMonthStartingDimDateKey
INTO  #CustomerMaxMonth9
 FROM #CustomerMonthSession9 d1
 JOIN marketing.v_dim_date dd 
   ON d1.monthstartingdimdatekey = dd.month_starting_dim_date_key
 LEFT JOIN #CustomerMonthSession9 d2 
   ON d1.dimcustomerkey = d2.dimcustomerkey AND dd.prior_month_starting_dim_date_key = d2.MonthStartingDimDateKey
WHERE d2.DimCustomerKey is null
GROUP BY d1.dimcustomerkey


IF OBJECT_ID('tempdb.dbo.#CustomerTenure9', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure9;

SELECT #customerMaxMonth9.DimCustomerKey,
       datediff(month,dd.calendar_date, td.month_starting_date)+1 tenure --plus 1 to include the current month
INTO #CustomerTenure9
FROM #CustomerMaxMonth9
 JOIN marketing.v_dim_date dd 
   ON #CustomerMaxMonth9.MaxMonthStartingDimDateKey = dd.dim_date_key
 JOIN marketing.v_dim_date td 
   ON td.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#currentmonthsummary9', 'U') IS NOT NULL
	DROP TABLE #currentmonthsummary9;

--create list of customers for report period(month)

SELECT #detail9.delivereddimemployeekey, 
       #detail9.DimCustomerKey, 
       SUM(#detail9.DeliveredSessionPrice) AS DeliveredSessionPrice,
       #CustomerTenure9.tenure,
       #detail9.FourDigitYearDashTwoDigitMonth,
       #detail9.MonthStartingDimDateKey
INTO #currentmonthsummary9
  FROM #detail9
  JOIN #CustomerTenure9
    ON #detail9.dimcustomerkey = #CustomerTenure9.DimCustomerKey
 WHERE #detail9.MonthStartingDimDateKey = (SELECT MAX(month_starting_dim_date_key) FROM #dates9 WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #detail9.delivereddimemployeekey, 
       #detail9.DimCustomerKey, 
       #CustomerTenure9.tenure,
       #detail9.FourDigitYearDashTwoDigitMonth,
       #detail9.MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#sessionsummary9', 'U') IS NOT NULL
	DROP TABLE #sessionsummary9;

--create retention period session summary for report month customers 

SELECT Max(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(FactPackageSessionKey) SessionCount,
       SUM(DeliveredSessionPrice) SessionAmount,
       Max(FourDigitYearDashTwoDigitMonth) AS MaxFourDigitYearDashTwoDigitMonth
  INTO #sessionsummary9
  FROM #detail9 fps
 WHERE dimcustomerkey in (SELECT distinct DimCustomerKey FROM #currentmonthsummary9)
 GROUP BY DeliveredDimEmployeeKey


IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients9', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients9;

  --- Collect full retention Period client count for employees who delivered sessions in the report month

SELECT #detail9.DeliveredDimEmployeeKey,
       COUNT(distinct #detail9.DimCustomerKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients9
  FROM #detail9
 WHERE #detail9.DeliveredDimEmployeeKey in (SELECT distinct delivereddimemployeekey FROM #currentmonthsummary9)
 GROUP BY DeliveredDimEmployeeKey


IF OBJECT_ID('tempdb.dbo.#tenuredata9', 'U') IS NOT NULL
	DROP TABLE #tenuredata9;

SELECT #currentmonthsummary9.DeliveredDimEmployeeKey,
       SUM(#currentmonthsummary9.tenure) AS CustomerTenure, 
       COUNT(#currentmonthsummary9.DimCustomerKey) AS CustomerCount,
       cast(SUM(#currentmonthsummary9.tenure) AS decimal (12,2)) / cast(count(#currentmonthsummary9.dimcustomerkey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients9.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#currentmonthsummary9.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#currentmonthsummary9.monthstartingdimdatekey) AS monthstartingdimdatekey
  INTO #tenuredata9
  FROM #currentmonthsummary9
  JOIN #RetentionPeriodClients9
    ON #currentmonthsummary9.DeliveredDimEmployeeKey = #RetentionPeriodClients9.DeliveredDimEmployeeKey
 GROUP BY #currentmonthsummary9.DeliveredDimEmployeeKey

 ----******************************************************************************JOIN DIMDESCRIPTION TO REGIONNAME
IF OBJECT_ID('tempdb.dbo.#9MonthData', 'U') IS NOT NULL
	DROP TABLE #9MonthData;

SELECT --DimLocation.PersonalTrainingRegionalCategoryLeadAreaName AS RegionName, ----------------Comeback to fix 
	   region.description AS RegionName,
       DimLocation.club_name ClubName,         
       DimLocation.club_code ClubCode,
       DimLocation.dim_club_key DimLocationKey, 
       DimEmployee.first_name FirstName, 
       DimEmployee.last_name LastName,  
       DimEmployee.employee_id EmployeeID,
       #tenuredata9.CustomerCount AS CurrentClients, 
       (sessionsummary.SessionCount / #tenuredata9.CustomerCount) AS AverageClientSessions, --
       cast(sessionsummary.SessionAmount AS decimal(12,2)) / cast(sessionsummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData9.AverageTenure AS AverageTenure_Months,
       #TenureData9.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData9.CustomerCount)/Convert(decimal(12,1),#TenureData9.RetentionPeriodClientCount)) AS ClientRetentionRate,
       cast(getdate() AS datetime) AS ReportRunDateTime,
       sessionsummary.MaxMonthStartingDimDateKey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,
       sessionsummary.DeliveredDimEmployeeKey,
       @ReportDate  AS HeaderReportDate,
	   sessionsummary.SessionCount,
	   sessionsummary.SessionAmount,
	   #TenureData9.CustomerTenure
   INTO #9MonthData
  FROM #TenureData9
  JOIN #sessionsummary9 sessionsummary
    ON #TenureData9.DeliveredDimEmployeeKey = sessionsummary.DeliveredDimEmployeeKey
  JOIN marketing.v_dim_employee DimEmployee 
    ON #tenuredata9.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN marketing.v_dim_club AS DimLocation
    ON DimEmployee.dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] region
	ON region.dim_description_key = DimLocation.[pt_rcl_area_dim_description_key]--region_dim_description_key
 WHERE sessionsummary.MaxMonthStartingDimDateKey = (select MAX(month_starting_dim_date_key) FROM #dates9 WHERE calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110))
 --ORDER BY RegionName, ClubName, dimemployee.EmployeeID




DROP TABLE #sessionsummary9
DROP TABLE #currentmonthsummary9
DROP TABLE #CustomerTenure9
DROP TABLE #CustomerMaxMonth9
DROP TABLE #CustomerMonthSession9
DROP TABLE #dates9
DROP TABLE #detail9
DROP TABLE #RetentionPeriodClients9
DROP TABLE #tenuredata9

--continue from here. 

IF OBJECT_ID('temdb.dbo.#dates12', 'U') IS NOT NULL
	DROP TABLE #dates12;

 SET @RetentionPeriod = '12 months' -- 3 6 9 12


SELECT month_starting_dim_date_key MonthStartingDimDateKey,
       dim_date_key DimDateKey,
       four_digit_year_dash_two_digit_month FourDigitYearDashTwoDigitMonth,
       calendar_date CalendarDate
  INTO #dates12
  FROM marketing.v_dim_date
 WHERE dim_date_key >= CASE WHEN  @RetentionPeriod = '3 Months' THEN (SELECT month_starting_dim_date_key FROM marketing.v_dim_date WHERE calendar_date = cast(DateAdd(Month,-2,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '6 Months' THEN (SELECT month_starting_dim_date_key FROM marketing.v_dim_date WHERE calendar_date = cast(DateAdd(Month,-5,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '9 Months' THEN (SELECT month_starting_dim_date_key FROM marketing.v_dim_date WHERE calendar_date = cast(DateAdd(Month,-8,@ReportDate) AS date))
                          WHEN  @RetentionPeriod = '12 Months' THEN (SELECT month_starting_dim_date_key FROM marketing.v_dim_date WHERE calendar_date = cast(DateAdd(Month,-11,@ReportDate) AS date))
                          ELSE null END
   AND calendar_date <= convert(datetime,convert(varchar,@ReportDate,110),110)
 --ORDER BY 1

 --*******************************************************************************Sandbox being used here 
IF OBJECT_ID('tempdb.dbo.#detail12', 'U') IS NOT NULL
	DROP TABLE #detail12;
SELECT fps.fact_mms_package_session_key FactPackageSessionKey,
       dd.MonthStartingDimDateKey, 
       fps.delivered_dim_employee_key DeliveredDimEmployeeKey, 
       fps.dim_mms_member_key DimCustomerKey,
       fps.delivered_session_price DeliveredSessionPrice,
       dd.FourDigitYearDashTwoDigitMonth
  INTO #detail12
  FROM marketing.v_fact_mms_package_session fps
  JOIN marketing.v_dim_date DeliveredDimDate
    ON fps.delivered_dim_date_key = DeliveredDimDate.dim_date_key
  JOIN #dates12 dd 
    ON DeliveredDimDate.month_starting_dim_date_key = dd.DimDateKey
 WHERE fps.fact_mms_package_dim_product_key IN (SELECT [DimProductKey] FROM [reporting].[v_PTDSSR_OneOnOneProduct])
 
AND fps.delivered_session_price > 0


/***************    find customer tenure    ***************/
IF OBJECT_ID('tempdb.dbo.#CustomerMonthSession12', 'U') IS NOT NULL
	DROP TABLE #CustomerMonthSession12;

SELECT DimCustomerKey,
       MonthStartingDimDateKey,
       COUNT(*) customermonthsessioncount
INTO #CustomerMonthSession12
 FROM #detail12
 GROUP BY DimCustomerKey, MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#CustomerMaxMonth12', 'U') IS NOT NULL
	DROP TABLE #CustomerMaxMonth12;

SELECT d1.dimcustomerkey, 
   MAX(d1.Monthstartingdimdatekey) MaxMonthStartingDimDateKey
INTO #CustomerMaxMonth12
 FROM #CustomerMonthSession12 d1
 JOIN marketing.v_dim_date dd ON d1.monthstartingdimdatekey = dd.dim_date_key
 LEFT JOIN #CustomerMonthSession12 d2 ON d1.dimcustomerkey = d2.dimcustomerkey AND dd.prior_month_starting_dim_date_key = d2.monthstartingdimdatekey
 WHERE d2.DimCustomerKey is null
 GROUP BY d1.dimcustomerkey


IF OBJECT_ID('tempdb.dbo.#CustomerTenure12', 'U') IS NOT NULL
	DROP TABLE #CustomerTenure12;

SELECT #customerMaxMonth12.DimCustomerKey,
       datediff(month,dd.calendar_date, td.month_starting_date)+1 tenure --plus 1 to include the current month
INTO #CustomerTenure12
 FROM #CustomerMaxMonth12
 JOIN marketing.v_dim_date dd 
   ON #CustomerMaxMonth12.MaxMonthStartingDimDateKey = dd.dim_date_key
 JOIN marketing.v_dim_date td 
   ON td.calendar_date = convert(datetime,convert(varchar,@ReportDate,110),110)



IF OBJECT_ID('tempdb.dbo.#currentmonthsummary12', 'U') IS NOT NULL
	DROP TABLE #currentmonthsummary12;
--create list of customers for report period(month)

SELECT #detail12.delivereddimemployeekey, 
       #detail12.DimCustomerKey, 
       SUM(#detail12.DeliveredSessionPrice) DeliveredSessionPrice,
       #CustomerTenure12.tenure,
       #detail12.FourDigitYearDashTwoDigitMonth,
       #detail12.MonthStartingDimDateKey
INTO #currentmonthsummary12
  FROM #detail12
  JOIN #CustomerTenure12
    ON #detail12.dimcustomerkey = #CustomerTenure12.DimCustomerKey
 WHERE #detail12.MonthStartingDimDateKey = (SELECT MAX(monthstartingdimdatekey) FROM #dates12 WHERE CalendarDate <= convert(datetime,convert(varchar,@ReportDate,110),110))
 GROUP BY #detail12.delivereddimemployeekey, 
       #detail12.DimCustomerKey, 
       #CustomerTenure12.tenure,
       #detail12.FourDigitYearDashTwoDigitMonth,
       #detail12.MonthStartingDimDateKey


IF OBJECT_ID('tempdb.dbo.#sessionsummary12', 'U') IS NOT NULL
	DROP TABLE #sessionsummary12;
--create retention period session summary for report month customers 
SELECT Max(MonthStartingDimDateKey) AS MaxMonthStartingDimDateKey, 
       DeliveredDimEmployeeKey, 
       COUNT(FactPackageSessionKey) SessionCount,
       SUM(DeliveredSessionPrice) SessionAmount,
       Max(FourDigitYearDashTwoDigitMonth) AS FourDigitYearDashTwoDigitMonth
  INTO #sessionsummary12
  FROM #detail12 fps
 WHERE dimcustomerkey in (SELECT distinct DimCustomerKey FROM #currentmonthsummary12)
 GROUP BY  DeliveredDimEmployeeKey



  --- Collect full retention Period client count for employees who delivered sessions in the report month
IF OBJECT_ID('tempdb.dbo.#RetentionPeriodClients12', 'U') IS NOT NULL
	DROP TABLE #RetentionPeriodClients12;

SELECT #detail12.DeliveredDimEmployeeKey,
       COUNT(distinct #detail12.DimCustomerKey) AS RetentionPeriodClientCount
  INTO #RetentionPeriodClients12
  FROM #detail12
 WHERE #detail12.DeliveredDimEmployeeKey in (SELECT distinct delivereddimemployeekey FROM #currentmonthsummary12)
 GROUP BY DeliveredDimEmployeeKey

	

IF OBJECT_ID('tempdb.dbo.#tenuredata12', 'U') IS NOT NULL
	DROP TABLE #tenuredata12;

SELECT #currentmonthsummary12.DeliveredDimEmployeeKey,
       SUM(#currentmonthsummary12.tenure) AS CustomerTenure, 
       COUNT(#currentmonthsummary12.DimCustomerKey) AS CustomerCount,
       cast(sum(#currentmonthsummary12.tenure) AS decimal (12,2)) / cast(count(#currentmonthsummary12.dimcustomerkey) AS decimal(12,2)) AS AverageTenure,
       MAX(#RetentionPeriodClients12.RetentionPeriodClientCount) RetentionPeriodClientCount,
       MAX(#currentmonthsummary12.FourDigitYearDashTwoDigitMonth) FourDigitYearDashTwoDigitMonth,
       MAX(#currentmonthsummary12.monthstartingdimdatekey) AS monthstartingdimdatekey
  INTO #tenuredata12
  FROM #currentmonthsummary12
  JOIN #RetentionPeriodClients12
    ON #currentmonthsummary12.DeliveredDimEmployeeKey = #RetentionPeriodClients12.DeliveredDimEmployeeKey
 GROUP BY #currentmonthsummary12.DeliveredDimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#12MonthData', 'U') IS NOT NULL
	DROP TABLE #12MonthData;

SELECT --DimLocation.PersonalTrainingRegionalCategoryLeadAreaName AS RegionName,  -------Fix it by joining the description table 
	   Region.description AS RegionName,
       DimLocation.club_name ClubName,         
       DimLocation.club_code ClubCode,
       DimLocation.dim_club_key DimLocationKey, 
       DimEmployee.first_name FirstName, 
       DimEmployee.last_name LastName,  
       DimEmployee.employee_id EmployeeID,
       #TenureData12.CustomerCount AS CurrentClients, 
       (sessionsummary.SessionCount / #tenuredata12.CustomerCount) AS AverageClientSessions, --
       cast(sessionsummary.sessionamount AS decimal(12,2)) / cast(sessionsummary.SessionCount AS decimal(12,2)) AS AverageSessionPrice,--- avg session price of retained clients
       #TenureData12.AverageTenure AS AverageTenure_Months,
       #TenureData12.RetentionPeriodClientCount, 
      (Convert(decimal(12,1),#TenureData12.CustomerCount)/Convert(decimal(12,1),#TenureData12.RetentionPeriodClientCount)) AS ClientRetentionRate,
       cast(getdate() AS datetime) AS ReportRunDateTime,
       sessionsummary.Maxmonthstartingdimdatekey AS HeaderReportMonth,
       @RetentionPeriod AS HeaderRetentionPeriod,
       sessionsummary.DeliveredDimEmployeeKey,
       @ReportDate AS HeaderReportDate,
	   sessionsummary.SessionCount,
	   sessionsummary.SessionAmount,
	   #TenureData12.CustomerTenure
   INTO #12MonthData
  FROM #TenureData12
  JOIN #sessionsummary12 sessionsummary
    ON #TenureData12.DeliveredDimEmployeeKey = sessionsummary.DeliveredDimEmployeeKey
  JOIN marketing.v_dim_employee DimEmployee
    ON #tenuredata12.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN marketing.v_dim_club AS DimLocation
    ON DimEmployee.dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] region
	ON region.dim_description_key = DimLocation.[pt_rcl_area_dim_description_key] --region_dim_description_key
 WHERE sessionsummary.MaxMonthStartingDimDateKey = (SELECT MAX(monthstartingdimdatekey) FROM #dates12 WHERE CalendarDate <= convert(datetime,convert(varchar,@ReportDate,110),110))
 --ORDER BY RegionName, ClubName, dimemployee.EmployeeID


   ---- Union all 4 result sets

    SELECT RegionName, 
       ClubName,         
       ClubCode,
       DimLocationKey, 
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
       DeliveredDimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  FROM #3MonthData

   UNION 

   SELECT RegionName, 
       ClubName,         
       ClubCode,
       DimLocationKey, 
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
       DeliveredDimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  FROM #6MonthData

   UNION 

   SELECT RegionName, 
       ClubName,         
       ClubCode,
       DimLocationKey, 
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
       DeliveredDimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
  FROM #9MonthData

   UNION

      SELECT RegionName, 
       ClubName,         
       ClubCode,
       DimLocationKey, 
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
       DeliveredDimEmployeeKey,
       HeaderReportDate,
	   SessionCount,
	   SessionAmount,
	   CustomerTenure
   FROM #12MonthData




DROP TABLE #sessionsummary12
DROP TABLE #currentmonthsummary12
DROP TABLE #CustomerTenure12
DROP TABLE #CustomerMaxMonth12
DROP TABLE #CustomerMonthSession12
DROP TABLE #dates12
DROP TABLE #detail12
DROP TABLE #RetentionPeriodClients12
DROP TABLE #tenuredata12
DROP TABLE #3MonthData
DROP TABLE #6MonthData
DROP TABLE #9MonthData
DROP TABLE #12MonthData

END


