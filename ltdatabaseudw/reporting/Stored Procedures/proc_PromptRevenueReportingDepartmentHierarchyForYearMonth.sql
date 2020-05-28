CREATE PROC [reporting].[proc_PromptRevenueReportingDepartmentHierarchyForYearMonth] @StartFourDigitYearDashTwoDigitMonth [VARCHAR](22),@EndFourDigitYearDashTwoDigitMonth [VARCHAR](22) AS
BEGIN
 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END



--- Sample Execution
-- EXEC [reporting].[proc_PromptRevenueReportingDepartmentHierarchyForYearMonth] '2019-01','2019-06'
---


-- Set the @StartFourDigitYearDashTwoDigitMonth variable
SET @StartFourDigitYearDashTwoDigitMonth = (SELECT CASE WHEN @StartFourDigitYearDashTwoDigitMonth = 'NULL' THEN @EndFourDigitYearDashTwoDigitMonth
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.four_digit_year_dash_two_digit_month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.four_digit_year_dash_two_digit_month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.four_digit_year_dash_two_digit_month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Current Quarter' THEN Quarters.QuarterStart
                                                   ELSE @StartFourDigitYearDashTwoDigitMonth END
  FROM [marketing].[v_dim_date] CurrentMonthDimDate
  JOIN [marketing].[v_dim_date] NextMonthDimDate ON CurrentMonthDimDate.next_month_starting_dim_date_key = NextMonthDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate ON NextMonthDimDate.next_month_starting_dim_date_key = MonthAfterNextMonthDimDate.dim_date_key
  JOIN (SELECT [year] CalendarYear, [quarter_number] CalendarQuarterNumber, MIN(four_digit_year_dash_two_digit_month) QuarterStart, MAX(four_digit_year_dash_two_digit_month) QuarterEnd
		FROM [marketing].[v_dim_date] 
		GROUP BY [year], [quarter_number]) Quarters
    ON CurrentMonthDimDate.[quarter_number] = Quarters.CalendarQuarterNumber
   AND CurrentMonthDimDate.[year] = Quarters.CalendarYear
 WHERE CurrentMonthDimDate.[calendar_date] = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))

 -- Set the @EndFourDigitYearDashTwoDigitMonth variable
SET @EndFourDigitYearDashTwoDigitMonth = (SELECT CASE WHEN @EndFourDigitYearDashTwoDigitMonth = 'NULL' THEN @StartFourDigitYearDashTwoDigitMonth
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.four_digit_year_dash_two_digit_month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.four_digit_year_dash_two_digit_month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.four_digit_year_dash_two_digit_month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Current Quarter' THEN Quarters.QuarterEnd
                                                 ELSE @EndFourDigitYearDashTwoDigitMonth END
  FROM [marketing].[v_dim_date] CurrentMonthDimDate
  JOIN [marketing].[v_dim_date] NextMonthDimDate ON CurrentMonthDimDate.next_month_starting_dim_date_key = NextMonthDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate ON NextMonthDimDate.next_month_starting_dim_date_key = MonthAfterNextMonthDimDate.dim_date_key
  JOIN (SELECT [year] CalendarYear, [quarter_number] CalendarQuarterNumber, MIN(four_digit_year_dash_two_digit_month) QuarterStart, MAX(four_digit_year_dash_two_digit_month) QuarterEnd
		FROM [marketing].[v_dim_date] 
		GROUP BY [year], [quarter_number]) Quarters
    ON CurrentMonthDimDate.[quarter_number] = Quarters.CalendarQuarterNumber
   AND CurrentMonthDimDate.[year] = Quarters.CalendarYear
 WHERE CurrentMonthDimDate.[calendar_date] = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))


DECLARE @StartMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT

SET @StartMonthStartingDimDateKey = (SELECT month_starting_dim_date_key
  FROM [marketing].[v_dim_date] 
 WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth
   AND day_number_in_month = 1)

SET @EndMonthEndingDimDateKey = (SELECT month_ending_dim_date_key
  FROM [marketing].[v_dim_date] 
 WHERE four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth
   AND last_day_in_month_flag = 'Y')


-- Create Hierarchy temp table for the selected dates


IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchy; 

SELECT reporting_region_type AS RegionType,
       reporting_division AS DivisionName,
       reporting_sub_division AS SubdivisionName,
       reporting_department AS DepartmentName,
       reporting_product_group AS ProductGroupName,
       dim_reporting_hierarchy_key AS DimReportingHierarchyKey
  INTO #DimReportingHierarchy
  FROM [marketing].[v_dim_reporting_hierarchy_history]
 WHERE effective_dim_date_key <= @EndMonthEndingDimDateKey
   AND expiration_dim_date_key > @StartMonthStartingDimDateKey
   AND dim_reporting_hierarchy_key > '0'


---- find the minimun dimreportinghierarchykey for each department
IF OBJECT_ID('tempdb.dbo.#DepartmentMinKeys', 'U') IS NOT NULL DROP TABLE #DepartmentMinKeys; 


SELECT DivisionName,
SubdivisionName,
DepartmentName,
MIN(DimReportingHierarchyKey) MinKey
INTO #DepartmentMinKeys
FROM #DimReportingHierarchy
GROUP BY DivisionName,SubdivisionName,DepartmentName

-- Final result set
SELECT DISTINCT
       #DimReportingHierarchy.RegionType,
       #DimReportingHierarchy.DivisionName,
       #DimReportingHierarchy.SubdivisionName,
       #DimReportingHierarchy.DepartmentName,
       #DimReportingHierarchy.ProductGroupName,
       #DimReportingHierarchy.DimReportingHierarchyKey,
       Cast(#DepartmentMinKeys.MinKey as Varchar(50)) DepartmentMinDimReportingHierarchyKey
  FROM #DimReportingHierarchy
  JOIN #DepartmentMinKeys
    ON #DimReportingHierarchy.DivisionName = #DepartmentMinKeys.DivisionName
   AND #DimReportingHierarchy.SubdivisionName = #DepartmentMinKeys.SubdivisionName
   AND #DimReportingHierarchy.DepartmentName = #DepartmentMinKeys.DepartmentName

DROP TABLE #DimReportingHierarchy
DROP TABLE #DepartmentMinKeys

END

