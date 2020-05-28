CREATE PROC [reporting].[proc_PromptForecastAssessmentDates] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


select 
four_digit_year_dash_two_digit_month as YearMonth
, standard_date_name as AssessmentDate
, calendar_date as AssessmentDate_DateValue
, case when day_number_in_month = 1 then 1 else 0 end as FirstOfMonthFlag

  FROM [marketing].[v_dim_date]
  where year <= year(getdate()) + 1
  and calendar_date > getdate()
  and day_number_in_month <=28

END
