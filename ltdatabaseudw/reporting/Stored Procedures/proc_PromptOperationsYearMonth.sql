CREATE PROC [reporting].[proc_PromptOperationsYearMonth] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ JIRA : REP-5969
------ Sample Execution
------ Exec [reporting].[proc_PromptOperationsYearMonth]

DECLARE @ThisMonthStartingDimDateKey INT,
        @NextMonthStartingDimDateKey INT,
        @LastCompletedFourDigitYearDashTwoDigitMonth CHAR(7)

SELECT @ThisMonthStartingDimDateKey = TodayDimDate.Month_Starting_Dim_Date_Key,
       @NextMonthStartingDimDateKey = TodayDimDate.Next_Month_Starting_Dim_Date_Key,
       @LastCompletedFourDigitYearDashTwoDigitMonth = PriorMonthDimDate.four_digit_year_dash_two_digit_month
  FROM marketing.v_dim_date TodayDimDate
  JOIN marketing.v_dim_date PriorMonthDimDate
    ON TodayDimDate.Prior_Month_Starting_Dim_Date_Key = PriorMonthDimDate.dim_date_key
 WHERE TodayDimDate.Calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate(),101),101)

 DECLARE @YesterdayFourDigitYearDashTwoDigitMonth CHAR(7),
        @TwoDaysAgoFourDigitYearDashTwoDigitMonth CHAR(7)
SELECT @YesterdayFourDigitYearDashTwoDigitMonth = four_digit_year_dash_two_digit_month
  FROM marketing.v_dim_date
 WHERE Calendar_Date = CONVERT(Datetime,Convert(Varchar,GetDate()-1,101),101)

SELECT @TwoDaysAgoFourDigitYearDashTwoDigitMonth = four_digit_year_dash_two_digit_month
  FROM marketing.v_dim_date
 WHERE Calendar_Date = CONVERT(Datetime,Convert(Varchar,GetDate()-2,101),101)

 IF OBJECT_ID('tempdb.dbo.#MonthJune2007ThroughLastMonthNextYear', 'U') IS NOT NULL
  DROP TABLE #MonthJune2007ThroughLastMonthNextYear; 

CREATE TABLE #MonthJune2007ThroughLastMonthNextYear (
       DimDateKey INT,
	   CalendarMonthEndingDate DATETIME,
	   FourDigitYearDashTwoDigitMonth CHAR(7),
	   FourDigitYearDashCalendarQuarterName VARCHAR(34))

INSERT INTO #MonthJune2007ThroughLastMonthNextYear
SELECT DimDate.Dim_Date_Key,
       DimDate.month_ending_date,
       DimDate.four_digit_year_dash_two_digit_month,
       Convert(Varchar,DimDate.year) + '-' + DimDate.quarter_name 
  FROM marketing.v_dim_date DimDate
 WHERE DimDate.last_day_in_month_flag = 'Y'
   AND DimDate.dim_date_key > 20070601
   AND DimDate.dim_date_key <= (SELECT LastMonthNextYearDimDate.dim_date_key
                                FROM marketing.v_dim_date LastMonthNextYearDimDate
                               WHERE LastMonthnextYearDiMDate.month_number_in_year = 12
                                 AND LastMonthNextYearDimDate.day_number_in_month = 31
                                 AND LastMonthNextYearDimDate.year = Year(GetDate())+1)



SELECT CASE WHEN DimDate.Month_Starting_Dim_Date_Key = @ThisMonthStartingDimDateKey THEN 'Current Month'
            WHEN DimDate.Month_Starting_Dim_Date_Key = @NextMonthStartingDimDateKey THEN 'Next Month'
            ELSE '' END PromptDescription,
       MonthList.FourDigitYearDashTwoDigitMonth,
       MonthList.FourDigitYearDashCalendarQuarterName,
       Convert(Datetime,Convert(Varchar,GetDate()-1,101),101) YesterdayCalendarDate,
       Convert(Datetime,Convert(Varchar,GetDate()-2,101),101) TwoDaysAgoCalendarDate
  INTO #Results
  FROM #MonthJune2007ThroughLastMonthNextYear MonthList
  JOIN marketing.v_dim_date DimDate
    ON MonthList.DimDateKey = DimDate.dim_date_key

SELECT #Results.FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month YesterdayFourDigitYearDashTwoDigitMonth
  FROM #Results
  JOIN marketing.v_dim_date YesterdayDimDate
    ON #Results.YesterdayCalendarDate = YesterdayDimDate.Calendar_Date
  JOIN marketing.v_dim_date JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.Year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1   
     
UNION ALL

SELECT #Results.PromptDescription FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month YesterdayFourDigitYearDashTwoDigitMonth
  FROM #Results
  JOIN marketing.v_dim_date YesterdayDimDate
    ON #Results.TwoDaysAgoCalendarDate = YesterdayDimDate.Calendar_Date
  JOIN marketing.v_dim_date JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.Year = JanuaryTwoYearsPriorToYesterdayDimDate.Year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1
 WHERE #Results.PromptDescription = 'Current Month'

UNION ALL

SELECT #Results.PromptDescription FourDigitYearDashTwoDigitMonth,
       #Results.FourDigitYearDashTwoDigitMonth ReportingFourDigitYearDashTwoDigitMonth,
       JanuaryTwoYearsPriorToYesterdayDimDate.four_digit_year_dash_two_digit_month JanuaryTwoYearsPriorToYesterday,
       @LastCompletedFourDigitYearDashTwoDigitMonth LastCompletedFourDigitYearDashTwoDigitMonth,
       YesterdayDimDate.four_digit_year_dash_two_digit_month YesterdayFourDigitYearDashTwoDigitMonth
  FROM #Results
  JOIN marketing.v_dim_date YesterdayDimDate
    ON #Results.YesterdayCalendarDate = YesterdayDimDate.Calendar_Date
  JOIN marketing.v_dim_date JanuaryTwoYearsPriorToYesterdayDimDate
    ON YesterdayDimDate.year = JanuaryTwoYearsPriorToYesterdayDimDate.year + 2
   AND JanuaryTwoYearsPriorToYesterdayDimDate.month_number_in_year = 1
   AND JanuaryTwoYearsPriorToYesterdayDimDate.day_number_in_month = 1 
 WHERE #Results.PromptDescription = 'Next Month'
 ORDER BY FourDigitYearDashTwoDigitMonth DESC

 DROP TABLE #Results
 DROP TABLE #MonthJune2007ThroughLastMonthNextYear

END
