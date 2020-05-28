CREATE PROC [reporting].[proc_PromptUsageYearMonth] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ JIRA : REP-5986
------ Sample Execution
------ Exec [reporting].[proc_PromptUsageYearMonth]


DECLARE @TodayDimDateKey INT,
        @YesterdayDimDateKey INT,
        @LastMonthDimDateKey INT
SET @TodayDimDateKey = (SELECT dim_date_key FROM marketing.v_dim_date WHERE calendar_date = CONVERT(Datetime,Convert(Varchar,GetDate(),101),101))
SET @YesterdayDimDateKey = (SELECT prior_day_dim_date_key FROM marketing.v_dim_date WHERE dim_date_key = @TodayDimDateKey)
SET @LastMonthDimDateKey = (SELECT prior_month_starting_dim_date_key FROM marketing.v_dim_date WHERE dim_date_key = @TodayDimDateKey)

SELECT DimDate.four_digit_year_dash_two_digit_month,
       YesterdayDimDate.four_digit_year_dash_two_digit_month YesterdayFourDigitYearDashTwoDigitMonth,
       PriorMonthDimDate.four_digit_year_dash_two_digit_month LastCompletedFourDigitYearDashTwoDigitMonth
  FROM marketing.v_dim_date DimDate
  JOIN marketing.v_dim_date YesterdayDimDate
    ON YesterdayDimDate.dim_date_key = @YesterdayDimDateKey
  JOIN marketing.v_dim_date PriorMonthDimDate
    ON PriorMonthDimDate.dim_date_key = @LastMonthDimDateKey
 WHERE DimDate.dim_date_key >= 20000701
   AND DimDate.dim_date_key <= @TodayDimDateKey
   AND DimDate.day_number_in_month = 1
ORDER BY DimDate.four_digit_year_dash_two_digit_month DESC


END
