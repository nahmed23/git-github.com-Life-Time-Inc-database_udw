CREATE PROC [reporting].[proc_PromptRevenueYearMonth] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
------  Exec [reporting].[proc_PromptRevenueYearMonth] 
-----

DECLARE @ThisMonthStartingDimDateKey INT,
        @NextMonthStartingDimDateKey INT,
        @LastCompletedFourDigitYearDashTwoDigitMonth CHAR(7),
        @MonthAfterNextMonthStartingDimDateKey INT
SELECT @ThisMonthStartingDimDateKey = TodayDimDate.month_starting_dim_date_key,
       @NextMonthStartingDimDateKey = TodayDimDate.next_month_starting_dim_date_key,
       @LastCompletedFourDigitYearDashTwoDigitMonth = PriorMonthDimDate.four_digit_year_dash_two_digit_month,
       @MonthAfterNextMonthStartingDimDateKey = NextMonthDimDate.next_month_starting_dim_date_key
  FROM [marketing].[v_dim_date] TodayDimDate
  JOIN [marketing].[v_dim_date] PriorMonthDimDate
    ON TodayDimDate.prior_month_starting_dim_date_key = PriorMonthDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] NextMonthDimDate
    ON TodayDimDate.next_month_starting_dim_date_key = NextMonthDimDate.dim_date_key
 WHERE TodayDimDate.calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate(),101),101)

DECLARE @YesterdayFourDigitYearDashTwoDigitMonth CHAR(7),
        @TwoDaysAgoFourDigitYearDashTwoDigitMonth CHAR(7),
		@YesterdayCalendarDate Datetime,
		@TwoDaysAgoCalendarDate Datetime
SELECT @YesterdayFourDigitYearDashTwoDigitMonth = four_digit_year_dash_two_digit_month,
       @YesterdayCalendarDate = calendar_date 
  FROM [marketing].[v_dim_date] 
 WHERE calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate()-1,101),101)

SELECT @TwoDaysAgoFourDigitYearDashTwoDigitMonth = four_digit_year_dash_two_digit_month,
       @TwoDaysAgoCalendarDate = calendar_date
  FROM [marketing].[v_dim_date] 
 WHERE calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate()-2,101),101)


IF OBJECT_ID('tempdb.dbo.#Month2007ThroughLastMonthNextYear', 'U') IS NOT NULL
  DROP TABLE #Month2007ThroughLastMonthNextYear;

SELECT dim_date_key, 
       four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,
       Convert(Varchar,year) + '-' + quarter_name AS  FourDigitYearDashCalendarQuarterName
  INTO #Month2007ThroughLastMonthNextYear
  FROM [marketing].[v_dim_date] 
  WHERE last_day_in_month_flag = 'Y'
   AND year >= 2007
   AND year <= year(getdate()) +1


IF OBJECT_ID('tempdb.dbo.#Quarters', 'U') IS NOT NULL
  DROP TABLE #Quarters;

SELECT FourDigitYearDashCalendarQuarterName,
       MIN(FourDigitYearDashTwoDigitMonth) StartOfQuarterFourDigitYearDashTwoDigitMonth,
       MAX(FourDigitYearDashTwoDigitMonth) EndOfQuarterFourDigitYearDashTwoDigitMonth
  INTO #Quarters
  FROM #Month2007ThroughLastMonthNextYear
  GROUP BY FourDigitYearDashCalendarQuarterName


IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;

SELECT CASE WHEN DimDate.month_starting_dim_date_key = @ThisMonthStartingDimDateKey THEN 'Current Month'
            WHEN DimDate.month_starting_dim_date_key = @NextMonthStartingDimDateKey THEN 'Next Month'
            WHEN DimDate.month_starting_dim_date_key = @MonthAfterNextMonthStartingDimDateKey THEN 'Month after Next Month'
            ELSE '' END PromptDescription,
       MonthList.FourDigitYearDashTwoDigitMonth,
       MonthList.FourDigitYearDashCalendarQuarterName,
       @YesterdayCalendarDate AS YesterdayCalendarDate,
       @TwoDaysAgoCalendarDate AS TwoDaysAgoCalendarDate,
       JanuaryDimDate.four_digit_year_dash_two_digit_month AS JanuaryOfReportingYear
  INTO #Results
  FROM #Month2007ThroughLastMonthNextYear MonthList
  JOIN [marketing].[v_dim_date]  DimDate
    ON MonthList.dim_date_key = DimDate.dim_date_key
  JOIN [marketing].[v_dim_date] JanuaryDimDate
    ON DimDate.year = JanuaryDimDate.year
   AND JanuaryDimDate.month_number_in_year = 1
   AND JanuaryDimDate.day_number_in_month = 1



SELECT #Results.FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth AS ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month AS JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth AS LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month AS YesterdayFourDigitYearDashTwoDigitMonth,
       #Results.JanuaryOfReportingYear,
       #Results.FourDigitYearDashCalendarQuarterName,
       #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth,
       #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth,
       4 SortOrder
  FROM #Results
  JOIN [marketing].[v_dim_date]  YesterdayDimDate
    ON #Results.YesterdayCalendarDate = YesterdayDimDate.calendar_date
  JOIN [marketing].[v_dim_date] JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1
  JOIN #Quarters
    ON #Results.FourDigitYearDashTwoDigitMonth >= #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth
   AND #Results.FourDigitYearDashTwoDigitMonth <= #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth
     
UNION ALL

SELECT #Results.PromptDescription AS FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth AS  ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month AS JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth AS LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month AS YesterdayFourDigitYearDashTwoDigitMonth,
       #Results.JanuaryOfReportingYear,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Results.FourDigitYearDashCalendarQuarterName END FourDigitYearDashCalendarQuarterName,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth END StartOfQuarterFourDigitYearDashTwoDigitMonth,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth END EndOfQuarterFourDigitYearDashTwoDigitMonth,
       1 SortOrder
  FROM #Results
  JOIN [marketing].[v_dim_date]  YesterdayDimDate
    ON #Results.TwoDaysAgoCalendarDate = YesterdayDimDate.calendar_date
  JOIN [marketing].[v_dim_date]  JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1
  JOIN #Quarters
    ON #Results.FourDigitYearDashTwoDigitMonth >= #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth
   AND #Results.FourDigitYearDashTwoDigitMonth <= #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth
 WHERE #Results.PromptDescription = 'Current Month'

UNION ALL

SELECT #Results.PromptDescription AS FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth AS ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month AS JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth AS LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month AS YesterdayFourDigitYearDashTwoDigitMonth,
       #Results.JanuaryOfReportingYear,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Results.FourDigitYearDashCalendarQuarterName END FourDigitYearDashCalendarQuarterName,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth END StartOfQuarterFourDigitYearDashTwoDigitMonth,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth END EndOfQuarterFourDigitYearDashTwoDigitMonth,
       2 SortOrder
  FROM #Results
  JOIN [marketing].[v_dim_date] YesterdayDimDate
    ON #Results.YesterdayCalendarDate = YesterdayDimDate.calendar_date
  JOIN [marketing].[v_dim_date] JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1 
  JOIN #Quarters
    ON #Results.FourDigitYearDashTwoDigitMonth >= #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth
   AND #Results.FourDigitYearDashTwoDigitMonth <= #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth
 WHERE #Results.PromptDescription = 'Next Month'

UNION ALL

SELECT #Results.PromptDescription AS FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth AS ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month AS JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth AS LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month AS YesterdayFourDigitYearDashTwoDigitMonth,
       #Results.JanuaryOfReportingYear,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Results.FourDigitYearDashCalendarQuarterName END FourDigitYearDashCalendarQuarterName,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth END StartOfQuarterFourDigitYearDashTwoDigitMonth,
       CASE WHEN #Results.FourDigitYearDashCalendarQuarterName = CONVERT(VARCHAR,YesterdayDimDate.year) + '-' + YesterdayDimDate.quarter_name
                 THEN 'Current Quarter'
            ELSE #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth END EndOfQuarterFourDigitYearDashTwoDigitMonth,
       3 SortOrder
  FROM #Results
  JOIN [marketing].[v_dim_date] YesterdayDimDate
    ON #Results.YesterdayCalendarDate = YesterdayDimDate.calendar_date
  JOIN [marketing].[v_dim_date] JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1 
  JOIN #Quarters
    ON #Results.FourDigitYearDashTwoDigitMonth >= #Quarters.StartOfQuarterFourDigitYearDashTwoDigitMonth
   AND #Results.FourDigitYearDashTwoDigitMonth <= #Quarters.EndOfQuarterFourDigitYearDashTwoDigitMonth
 WHERE #Results.PromptDescription = 'Month after Next Month'
 ORDER BY SortOrder, FourDigitYearDashTwoDigitMonth DESC

DROP TABLE #Results


END
